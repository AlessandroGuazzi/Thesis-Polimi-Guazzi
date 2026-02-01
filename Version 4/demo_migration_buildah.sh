#!/bin/bash

# ==============================================================================
# SPACE MISSION MIGRATION V3.3 - POST-RESTORE HOOK
# Fix: Automatizza il 'wake_up' eseguendolo live sul container ripristinato
# ==============================================================================

if [ "$#" -ne 2 ]; then
    echo "Uso: $0 <nodo-sorgente> <nodo-destinazione>"
    echo "Esempio: $0 minikube-m02 minikube-m03"
    exit 1
fi

SOURCE_NODE=$1
DEST_NODE=$2
POD_LABEL="app=space-mission"
TRANSIT_DIR="/tmp/checkpoint_transit"
RESTORED_IMAGE="localhost/space-mission:restored"

# Colori
GREEN='\033[0;32m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${CYAN}🚀 INIZIO MIGRAZIONE AUTOMATICA: $SOURCE_NODE -> $DEST_NODE${NC}"

# 1. TROVA IL POD
POD_NAME=$(kubectl get pod -l $POD_LABEL --field-selector spec.nodeName=$SOURCE_NODE -o jsonpath="{.items[0].metadata.name}")
if [ -z "$POD_NAME" ]; then
    echo -e "${RED}❌ Nessun pod trovato sul nodo $SOURCE_NODE${NC}"
    exit 1
fi
echo "📍 Pod Target: $POD_NAME"

# 2. TRIGGER CHECKPOINT
echo -e "${CYAN}--- FASE 1: CHECKPOINT ---${NC}"

# Attiviamo Flight Mode
echo "✈️  Attivazione Modalità Aereo..."
kubectl exec $POD_NAME -- touch /tmp/prepare_jump 2>/dev/null || true
echo "⏳ Attesa disconnessione client (5s)..."
sleep 5

# Checkpoint via API
PROXY_PORT=8001
kubectl proxy --port=$PROXY_PORT >/dev/null 2>&1 &
PROXY_PID=$!
sleep 2

API_URL="http://127.0.0.1:$PROXY_PORT/api/v1/nodes/$SOURCE_NODE/proxy/checkpoint/default/$POD_NAME/main-app"
echo "📸 Richiesta snapshot..."

for i in {1..5}; do
    RESPONSE=$(curl -X POST -s "$API_URL")
    if [[ "$RESPONSE" == *"/var/lib/kubelet/checkpoints"* ]]; then
        REMOTE_PATH=$(echo "$RESPONSE" | sed -e 's/.*"items":\["//' -e 's/"].*//')
        ARCHIVE_NAME=$(basename "$REMOTE_PATH")
        echo -e "${GREEN}✅ Checkpoint creato: $ARCHIVE_NAME${NC}"
        break
    else
        echo "⚠️  Retry checkpoint ($i/5)..."
        sleep 1
    fi
done

if [ -z "$REMOTE_PATH" ]; then
    echo -e "${RED}❌ Checkpoint fallito.${NC}"
    kill $PROXY_PID
    exit 1
fi

kill $PROXY_PID

# 3. TRASFERIMENTO
echo -e "${CYAN}--- FASE 2: TRASFERIMENTO & COSTRUZIONE ---${NC}"
mkdir -p $TRANSIT_DIR
rm -rf $TRANSIT_DIR/*

echo "🔓 Prelievo archivio..."
minikube ssh -n $SOURCE_NODE "sudo cp $REMOTE_PATH /tmp/$ARCHIVE_NAME && sudo chmod 644 /tmp/$ARCHIVE_NAME"
minikube cp "$SOURCE_NODE:/tmp/$ARCHIVE_NAME" "$TRANSIT_DIR/$ARCHIVE_NAME"
minikube ssh -n $SOURCE_NODE "sudo rm /tmp/$ARCHIVE_NAME"

if [ ! -f "$TRANSIT_DIR/$ARCHIVE_NAME" ]; then
    echo -e "${RED}❌ Errore critico: File non scaricato.${NC}"
    exit 1
fi

echo "⬆️  Caricamento su $DEST_NODE..."
minikube cp "$TRANSIT_DIR/$ARCHIVE_NAME" "$DEST_NODE:/tmp/checkpoint.tar"

# 4. BUILDAH (Struttura Base)
echo "🔨 Costruzione immagine su $DEST_NODE..."

BUILD_SCRIPT="
set -e
sudo buildah rm restoration-lab 2>/dev/null || true
sudo buildah from --name restoration-lab scratch
sudo buildah add restoration-lab /tmp/checkpoint.tar /

# === PREPARAZIONE FILESYSTEM ===
# Creiamo solo le cartelle necessarie. Il file 'landed' lo creeremo LIVE dopo il restore.
MNT=\$(sudo buildah mount restoration-lab)
sudo mkdir -p \"\$MNT/tmp\"
# Rimuoviamo il vecchio segnale di salto se presente
sudo rm -f \"\$MNT/tmp/prepare_jump\"
sudo buildah unmount restoration-lab
# ===============================

sudo buildah config --annotation \"io.kubernetes.cri-o.annotations.checkpoint.name=main-app\" restoration-lab
sudo buildah commit restoration-lab $RESTORED_IMAGE
sudo buildah rm restoration-lab
sudo rm /tmp/checkpoint.tar
"

minikube ssh -n $DEST_NODE "$BUILD_SCRIPT"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Immagine pronta.${NC}"
else
    echo -e "${RED}❌ Errore build.${NC}"
    exit 1
fi

# 5. RESTORE & WAKE UP
echo -e "${CYAN}--- FASE 3: SWITCHOVER & WAKE UP (CLEAN TURBO) ---${NC}"

# A. PREPARAZIONE CONFIGURAZIONE
# Aggiungiamo "terminationGracePeriodSeconds: 0" alla patch.
# Questo dice a K8s: "Quando spegni questo pod, fallo ISTANTANEAMENTE", senza creare zombie.
PATCH_JSON=$(cat <<EOF
{
  "spec": {
    "template": {
      "spec": {
        "terminationGracePeriodSeconds": 0,
        "nodeSelector": {
          "kubernetes.io/hostname": "$DEST_NODE"
        },
        "containers": [{
          "name": "main-app",
          "image": "$RESTORED_IMAGE"
        }]
      }
    }
  }
}
EOF
)

# B. APPLICAZIONE PATCH
echo "⚡ Applicazione Switchover..."
# Applichiamo la patch. Kubernetes vedrà il cambio nodo e creerà il nuovo pod.
# Vedrà anche terminationGracePeriodSeconds: 0 e ucciderà il vecchio pod istantaneamente.
kubectl patch deployment space-mission --type='strategic' -p "$PATCH_JSON"

# C. PULIZIA VECCHIO POD (Opzionale ma utile per velocità)
# Usiamo delete SENZA --force. Questo rispetta il protocollo Kubelet ed evita gli zombie.
# Avendo impostato grace-period: 0 nella patch sopra, sarà comunque istantaneo.
kubectl delete pod $POD_NAME --wait=false 2>/dev/null &

echo "⏳ Attesa nuovo Pod..."
# Loop di controllo
for i in {1..20}; do
    NEW_POD=$(kubectl get pod -l $POD_LABEL --field-selector spec.nodeName=$DEST_NODE -o jsonpath="{.items[0].metadata.name}")
    # Controlliamo che il nuovo pod esista e non sia quello vecchio (che sta morendo)
    if [ ! -z "$NEW_POD" ] && [ "$NEW_POD" != "$POD_NAME" ]; then
        echo "   Pod rilevato: $NEW_POD"
        break
    fi
    sleep 0.5
done

if [ -z "$NEW_POD" ]; then echo "❌ Errore avvio pod"; exit 1; fi

# D. WAKE UP IMMEDIATO (ROBUST)
echo "   Attesa ContainerRunning..."

# 1. Primo check: Aspettiamo che Kubernetes segni il container come avviato
until kubectl get pod $NEW_POD -o jsonpath='{.status.containerStatuses[0].state.running.startedAt}' 2>/dev/null | grep -q "20"; do
    sleep 0.1
done

echo -e "${GREEN}🔔 WAKE UP!${NC}"

# 2. Secondo check: Tentiamo l'EXEC finché il container non è VERAMENTE pronto
MAX_RETRIES=20
COUNT=0
until kubectl exec $NEW_POD -- touch /tmp/landed 2>/dev/null; do
    echo "   ...connessione exec in corso..."
    sleep 0.2
    ((COUNT++))
    if [ $COUNT -ge $MAX_RETRIES ]; then
        echo -e "${RED}❌ Timeout Wake Up!${NC}"
        exit 1
    fi
done

echo -e "${CYAN}🎉 MIGRAZIONE COMPLETATA!${NC}"