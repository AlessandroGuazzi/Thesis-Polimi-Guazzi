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
echo -e "${CYAN}--- FASE 3: SWITCHOVER & WAKE UP (SMART LOCK) ---${NC}"

# A. PREPARAZIONE CONFIGURAZIONE
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
kubectl patch deployment space-mission --type='strategic' -p "$PATCH_JSON"

# C. PULIZIA VECCHIO POD (Per accelerare)
kubectl delete pod $POD_NAME --wait=false 2>/dev/null &

echo "⏳ Ricerca nuovo Pod ATTIVO su $DEST_NODE..."

# VARIABILE TARGET DINAMICA
TARGET_POD=""
MAX_WAIT=60
count=0

# LOOP INTELLIGENTE:
# Non ci accontentiamo di "un pod qualsiasi". Cerchiamo specificamente un pod che:
# 1. Sia sul nodo di destinazione ($DEST_NODE)
# 2. Abbia lo status.phase = 'Running' (evita Terminating, ContainerCreating, Pending)
while [ $count -lt $MAX_WAIT ]; do

    # Chiediamo a K8s: Dammi il nome del pod su QUESTO nodo che è GIA' in stato RUNNING
    TARGET_POD=$(kubectl get pod -l $POD_LABEL --field-selector spec.nodeName=$DEST_NODE,status.phase=Running -o jsonpath="{.items[0].metadata.name}" 2>/dev/null)

    if [ ! -z "$TARGET_POD" ]; then
        # Verifica doppia: controlliamo che abbia anche il timestamp di avvio (è pronto davvero)
        IS_STARTED=$(kubectl get pod $TARGET_POD -o jsonpath='{.status.containerStatuses[0].state.running.startedAt}' 2>/dev/null)

        if [[ "$IS_STARTED" == *"20"* ]]; then
            echo -e "${GREEN}   ✅ Agganciato Pod Operativo: $TARGET_POD${NC}"
            break
        fi
    fi

    # Feedback visivo (sovrascrive la riga per non spammare)
    echo -ne "   ...scansione orbita $DEST_NODE ($count/${MAX_WAIT}s)...\r"
    sleep 1
    ((count++))

    # Se il target è vuoto, resetta la variabile per sicurezza
    TARGET_POD=""
done
echo "" # A capo dopo il loop

if [ -z "$TARGET_POD" ]; then
    echo -e "${RED}❌ Timeout: Nessun pod Running trovato su $DEST_NODE.${NC}"
    echo "Stato attuale del cluster:"
    kubectl get pods -o wide
    exit 1
fi

# D. WAKE UP (Usando il pod trovato dinamicamente)
echo "🔔 WAKE UP (Invio segnale a $TARGET_POD)..."

# Tentiamo l'EXEC con pazienza
MAX_RETRIES=30
COUNT=0
until kubectl exec $TARGET_POD -- touch /tmp/landed 2>/dev/null; do
    echo "   ...connessione neurale in corso ($COUNT/$MAX_RETRIES)..."
    sleep 0.5
    ((COUNT++))
    if [ $COUNT -ge $MAX_RETRIES ]; then
        echo -e "${RED}❌ Timeout Wake Up! Il container c'è ma non risponde.${NC}"
        exit 1
    fi
done

echo -e "${CYAN}🎉 MIGRAZIONE COMPLETATA!${NC}"