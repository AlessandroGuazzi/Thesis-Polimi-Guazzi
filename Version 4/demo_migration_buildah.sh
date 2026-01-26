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
echo -e "${CYAN}--- FASE 3: SWITCHOVER & WAKE UP ---${NC}"

echo "🛑 1. Spegnimento vecchio deployment..."
kubectl scale deployment space-mission --replicas=0
kubectl wait --for=delete pod/$POD_NAME --timeout=60s 2>/dev/null || sleep 2

echo "📌 2. Vincolo sul nodo destinazione ($DEST_NODE)..."
kubectl patch deployment space-mission --type='json' -p="[{\"op\": \"add\", \"path\": \"/spec/template/spec/nodeSelector/kubernetes.io~1hostname\", \"value\": \"$DEST_NODE\"}]"

echo "🖼️  3. Impostazione immagine..."
kubectl set image deployment/space-mission main-app=$RESTORED_IMAGE

echo "🚀 4. Avvio nuovo Pod..."
kubectl scale deployment space-mission --replicas=1

echo "⏳ Attesa disponibilità nuovo Pod..."
# Aspettiamo che Kubernetes assegni il nome e che il container sia creato
sleep 5
NEW_POD=$(kubectl get pod -l $POD_LABEL -o jsonpath="{.items[0].metadata.name}")

if [ -z "$NEW_POD" ]; then
     echo "⚠️ Pod non trovato subito, attendo..."
     sleep 5
     NEW_POD=$(kubectl get pod -l $POD_LABEL -o jsonpath="{.items[0].metadata.name}")
fi

echo "   Pod identificato: $NEW_POD"
echo "   Attesa stato 'Running'..."
kubectl wait --for=condition=Ready pod/$NEW_POD --timeout=60s

# === POST-RESTORE HOOK (AUTOMATIC WAKE UP) ===
echo -e "${GREEN}🔔 INVIO SEGNALE DI ATTERRAGGIO AUTOMATICO...${NC}"
kubectl exec $NEW_POD -- touch /tmp/landed
echo -e "${GREEN}✅ Satellite svegliato con successo!${NC}"
# ==============================================

echo -e "${CYAN}🎉 MIGRAZIONE COMPLETATA!${NC}"