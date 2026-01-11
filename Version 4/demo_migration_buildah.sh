#!/bin/bash

# ==============================================================================
# SPACE MISSION MIGRATION - BUILDAH STRATEGY (V2 - PERMISSION FIX)
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

echo -e "${CYAN}🚀 INIZIO MIGRAZIONE ROBUSTA (BUILDAH): $SOURCE_NODE -> $DEST_NODE${NC}"

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

# Loop di tentativi
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

# 3. TRASFERIMENTO (CON FIX PERMESSI)
echo -e "${CYAN}--- FASE 2: TRASFERIMENTO & COSTRUZIONE IMMAGINE ---${NC}"
mkdir -p $TRANSIT_DIR
rm -rf $TRANSIT_DIR/*

# === FIX PERMESSI: Copia in /tmp e chmod ===
echo "🔓 Sblocco permessi file sul nodo sorgente..."
minikube ssh -n $SOURCE_NODE "sudo cp $REMOTE_PATH /tmp/$ARCHIVE_NAME && sudo chmod 644 /tmp/$ARCHIVE_NAME"

echo "⬇️  Scaricamento da $SOURCE_NODE..."
# Scarichiamo dalla cartella /tmp dove abbiamo permessi di lettura
minikube cp "$SOURCE_NODE:/tmp/$ARCHIVE_NAME" "$TRANSIT_DIR/$ARCHIVE_NAME"

# Pulizia sul nodo sorgente
minikube ssh -n $SOURCE_NODE "sudo rm /tmp/$ARCHIVE_NAME"

if [ ! -f "$TRANSIT_DIR/$ARCHIVE_NAME" ]; then
    echo -e "${RED}❌ Errore critico: File non scaricato.${NC}"
    exit 1
fi

echo "⬆️  Caricamento su $DEST_NODE..."
# Copiamo il file in una cartella temporanea sul nodo destinazione
minikube cp "$TRANSIT_DIR/$ARCHIVE_NAME" "$DEST_NODE:/tmp/checkpoint.tar"

# 4. BUILDAH MAGIC (Sul nodo destinazione)
echo "🔨 Costruzione immagine ripristinata su $DEST_NODE..."

BUILD_SCRIPT="
set -e
# 1. Pulizia
sudo buildah rm restoration-lab 2>/dev/null || true

# 2. Creiamo un container 'scratch' (vuoto)
sudo buildah from --name restoration-lab scratch

# 3. Aggiungiamo il checkpoint alla root
sudo buildah add restoration-lab /tmp/checkpoint.tar /

# 4. Configuriamo l'annotazione magica per CRI-O
sudo buildah config --annotation \"io.kubernetes.cri-o.annotations.checkpoint.name=main-app\" restoration-lab

# 5. Creiamo l'immagine finale
sudo buildah commit restoration-lab $RESTORED_IMAGE

# 6. Pulizia
sudo buildah rm restoration-lab
sudo rm /tmp/checkpoint.tar
"

minikube ssh -n $DEST_NODE "$BUILD_SCRIPT"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Immagine $RESTORED_IMAGE costruita con successo!${NC}"
else
    echo -e "${RED}❌ Errore durante la build dell'immagine.${NC}"
    exit 1
fi

# 5. RESTORE
echo -e "${CYAN}--- FASE 3: RICONFIGURAZIONE ORBITALE ---${NC}"

echo "🔧 Patching Deployment..."
# Patchamo per usare la nuova immagine
kubectl patch deployment space-mission --patch "{\"spec\": {\"template\": {\"spec\": {\"containers\": [{\"name\": \"main-app\", \"image\": \"$RESTORED_IMAGE\"}]}}}}"

echo "💀 Cancellazione vecchio Pod (Force)..."
kubectl delete pod $POD_NAME --grace-period=0 --force >/dev/null 2>&1

echo -e "${GREEN}🎉 MIGRAZIONE COMPLETATA!${NC}"
echo "Il nuovo pod userà l'immagine '$RESTORED_IMAGE' che contiene la memoria RAM."