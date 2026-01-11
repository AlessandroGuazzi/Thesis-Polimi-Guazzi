#!/bin/bash

# ==============================================================================
# SPACE CLOUD V2.0 - PROTOCOLLO DI MIGRAZIONE INTER-SATELLITARE
# Sposta un Pod attivo dal Satellite Sorgente al Satellite Destinazione
# preservando la memoria RAM tramite CRIU.
# ==============================================================================

SOURCE_NODE=$1
DEST_NODE=$2

# Colori per output
GREEN='\033[0;32m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

# 1. VALIDAZIONE INPUT
if [ -z "$SOURCE_NODE" ] || [ -z "$DEST_NODE" ]; then
    echo -e "${RED}❌ Errore: Sintassi non valida.${NC}"
    echo "   Uso: ./demo_migration.sh <nodo_sorgente> <nodo_destinazione>"
    echo "   Es:  ./demo_migration.sh minikube-m02 minikube-m03"
    exit 1
fi

echo -e "${CYAN}🚀 INIZIO MIGRAZIONE: $SOURCE_NODE -> $DEST_NODE${NC}"

# 2. IDENTIFICAZIONE POD
# Cerchiamo il nome esatto del pod che sta girando sul nodo sorgente
# Filtriamo per label 'app=space-mission' e fieldSelector 'spec.nodeName'
POD_NAME=$(kubectl get pods --field-selector spec.nodeName=$SOURCE_NODE -l app=space-mission -o jsonpath="{.items[0].metadata.name}")

if [ -z "$POD_NAME" ]; then
    echo -e "${RED}❌ Nessun satellite attivo trovato su $SOURCE_NODE!${NC}"
    echo "   (Verifica che il pod stia girando lì con 'kubectl get pods -o wide')"
    exit 1
fi
echo "📍 Agganciato Pod Target: $POD_NAME"

# 3. CHECKPOINT (API TRIGGER)
# Questa parte è identica al tuo vecchio script, ma dinamica
echo -e "${CYAN}--- FASE 1: CONGELAMENTO MEMORIA (CHECKPOINT) ---${NC}"

# Avvio proxy temporaneo
PROXY_PORT=8001
kubectl proxy --port=$PROXY_PORT >/dev/null 2>&1 &
PROXY_PID=$!
sleep 2

# === NOVITÀ: ATTIVAZIONE FLIGHT MODE ===
echo "✈️  Attivazione Modalità Aereo (Chiusura Socket)..."
# Usiamo kubectl exec per chiamare l'API dall'interno (più sicuro del port-forward)
kubectl exec $POD_NAME -- curl -s http://localhost:80/api/prepare_jump
echo "" # A capo
sleep 10 # Diamo tempo di chiudere eventuali connessioni appese
# =======================================

# Chiamata API al Kubelet del nodo sorgente
# NOTA: Usiamo 'space-mission' come nome container, verifica il tuo yaml se diverso (es. main-app)
CONTAINER_NAME="main-app"
API_URL="http://127.0.0.1:$PROXY_PORT/api/v1/nodes/$SOURCE_NODE/proxy/checkpoint/default/$POD_NAME/$CONTAINER_NAME"

REMOTE_PATH=""
MAX_RETRIES=5

for ((i=1; i<=MAX_RETRIES; i++)); do
    echo "📸 Tentativo $i/$MAX_RETRIES: Richiesta snapshot..."
    RESPONSE=$(curl -X POST -s "$API_URL")

    # Controlliamo se la risposta contiene il percorso atteso
    if [[ "$RESPONSE" == *"/var/lib/kubelet/checkpoints"* ]]; then
        # Estrazione percorso file pulito
        REMOTE_PATH=$(echo "$RESPONSE" | sed -e 's/.*"items":\["//' -e 's/"].*//')
        echo -e "${GREEN}✅ Successo! Checkpoint creato: $REMOTE_PATH${NC}"
        break
    else
        echo "⚠️  Tentativo fallito (Socket occupato?). Riprovo tra 1s..."
        # Se siamo all'ultimo tentativo, stampiamo l'errore e usciamo
        if [ $i -eq $MAX_RETRIES ]; then
            echo -e "${RED}❌ Errore Checkpoint dopo $MAX_RETRIES tentativi: $RESPONSE${NC}"
            kill $PROXY_PID
            exit 1
        fi
        sleep 1
    fi
done

# Pulizia Proxy
kill $PROXY_PID

# Estrazione percorso file dal JSON
REMOTE_PATH=$(echo "$RESPONSE" | sed -e 's/.*"items":\["//' -e 's/"].*//')

# Verifica successo
if [[ "$REMOTE_PATH" != *"/var/lib/kubelet/checkpoints"* ]]; then
    echo -e "${RED}❌ Errore Checkpoint API: $RESPONSE${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Checkpoint creato su $SOURCE_NODE: $REMOTE_PATH${NC}"

# 4. TRASFERIMENTO DATI (Downlink -> Uplink)
echo -e "${CYAN}--- FASE 2: TRASFERIMENTO DATI ---${NC}"

# A. Downlink: Scarichiamo il file dal container del nodo sorgente al tuo PC
echo "⬇️  Scaricamento da $SOURCE_NODE..."
docker cp $SOURCE_NODE:$REMOTE_PATH ./checkpoint_transit.tar

# B. Uplink: Carichiamo il file dal tuo PC al container del nodo destinazione
# Lo rinominiamo 'checkpoint.tar' così l'entrypoint.sh lo riconosce subito
echo "⬆️  Caricamento su $DEST_NODE..."

# Assicuriamoci che la cartella esista (comando ssh nel docker container)
docker exec $DEST_NODE mkdir -p /var/lib/kubelet/checkpoints

# Copia del file
docker cp ./checkpoint_transit.tar $DEST_NODE:/var/lib/kubelet/checkpoints/checkpoint.tar

# Pulizia locale
rm ./checkpoint_transit.tar

echo -e "${GREEN}✅ Payload trasferito con successo.${NC}"

# 5. SWITCHOVER (Cambio Rotta)
echo -e "${CYAN}--- FASE 3: RICONFIGURAZIONE ORBITALE ---${NC}"

# Patchiamo il Deployment per dire: "D'ora in poi, usa SOLO il nodo di destinazione"
# Questo aggiorna il NodeSelector
echo "🔧 Aggiornamento Deployment..."
kubectl patch deployment space-mission -p "{\"spec\": {\"template\": {\"spec\": {\"nodeSelector\": {\"type\": \"satellite\", \"kubernetes.io/hostname\": \"$DEST_NODE\"}}}}}"

echo "💀 Terminazione vecchio Pod..."
# Cancellando il pod vecchio, Kubernetes ne crea uno nuovo immediatamente.
# Grazie alla patch, andrà sul DEST_NODE.
# Grazie alla copia file, troverà il checkpoint.tar e farà il restore.
kubectl delete pod $POD_NAME --grace-period=0 --force > /dev/null 2>&1

echo -e "${GREEN}🎉 MIGRAZIONE COMPLETATA!${NC}"
echo "   Il nuovo pod si sta avviando su $DEST_NODE recuperando lo stato."