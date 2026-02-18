#!/bin/bash

# ==============================================================================
# SPACE CLOUD V5 - SIDECAR MIGRATION MECHANISM
# Target: Sposta SOLO il container 'state-sidecar' preservando la RAM.
# Il container 'payload-phoenix' viene distrutto e ricreato stateless.
# ==============================================================================

# --- VALIDAZIONE ARGOMENTI ---
if [ "$#" -ne 2 ]; then
    echo "Uso: $0 <nodo-sorgente> <nodo-destinazione>"
    echo "Esempio: $0 minikube-m02 minikube-m03"
    exit 1
fi

SOURCE_NODE=$1
DEST_NODE=$2

# Configurazioni Tattiche
POD_LABEL="app=space-mission"
TARGET_CONTAINER="sidecar-guardian"  # IL CONTAINER DA MIGRARE (CRIU Target)
IGNORED_CONTAINER="payload-phoenix"  # IL CONTAINER DA RICREARE (Stateless)

TRANSIT_DIR="/tmp/checkpoint_transit"
RESTORED_IMG="localhost/space-sidecar:restored"
ORIGINAL_PAYLOAD_IMG="localhost/space-workload:latest"

# Colori
GREEN='\033[0;32m'
CYAN='\033[0;36m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${CYAN}🚀 INIZIO MIGRAZIONE CHIRURGICA: $SOURCE_NODE -> $DEST_NODE${NC}"

# 1. LOCALIZZAZIONE OBIETTIVO
echo -e "${YELLOW}[1/5] Scansione Orbitale (Ricerca Pod)...${NC}"
POD_NAME=$(kubectl get pod -l $POD_LABEL --field-selector spec.nodeName=$SOURCE_NODE -o jsonpath="{.items[0].metadata.name}")

if [ -z "$POD_NAME" ]; then
    echo -e "${RED}❌ ERRORE: Nessun pod attivo trovato su $SOURCE_NODE${NC}"
    exit 1
fi
echo "   📍 Agganciato Pod: $POD_NAME"

# 2. CONGELAMENTO (CHECKPOINT)
echo -e "${YELLOW}[2/5] Congelamento Memoria (CRIU)...${NC}"

# A. Attiva Flight Mode (Solo sul Sidecar)
echo "   ✈️  Attivazione Protocollo Phoenix..."
kubectl exec $POD_NAME -c $TARGET_CONTAINER -- touch /tmp/prepare_jump 2>/dev/null || true
echo "   ⏳ Attesa spegnimento connessioni (3s)..."
sleep 3

# B. Proxy API Locale
PROXY_PORT=8001
kubectl proxy --port=$PROXY_PORT >/dev/null 2>&1 &
PROXY_PID=$!
sleep 2

# C. Richiesta Checkpoint (SOLO PER IL SIDECAR)
# Nota: L'URL punta specificamente a /.../sidecar-guardian
API_URL="http://127.0.0.1:$PROXY_PORT/api/v1/nodes/$SOURCE_NODE/proxy/checkpoint/default/$POD_NAME/$TARGET_CONTAINER"
echo "   📸 Richiesta Snapshot RAM..."

CHECKPOINT_PATH=""
for i in {1..5}; do
    RESPONSE=$(curl -X POST -s "$API_URL")

    if [[ "$RESPONSE" == *"/var/lib/kubelet/checkpoints"* ]]; then
        # Estrazione path grezzo dal JSON response
        RAW_PATH=$(echo "$RESPONSE" | grep -o '/var/lib/kubelet/checkpoints/[^"]*')
        CHECKPOINT_PATH=$(echo "$RAW_PATH" | tr -d '[:space:]') # Pulisce whitespace

        ARCHIVE_NAME=$(basename "$CHECKPOINT_PATH")
        echo -e "${GREEN}   ✅ Checkpoint Creato: $ARCHIVE_NAME${NC}"
        break
    else
        echo "      ⚠️  Retry checkpoint ($i/5)..."
        sleep 1
    fi
done

# Cleanup Proxy
kill $PROXY_PID

if [ -z "$CHECKPOINT_PATH" ]; then
    echo -e "${RED}❌ ERRORE: Checkpoint fallito. Risposta API: $RESPONSE${NC}"
    exit 1
fi

# 3. TRASFERIMENTO DATI
echo -e "${YELLOW}[3/5] Trasferimento Neurale (Copia Dati)...${NC}"
mkdir -p $TRANSIT_DIR
rm -rf $TRANSIT_DIR/*

# A. Scarica da Sorgente
echo -ne "   ⬇️  Download da $SOURCE_NODE... \r"
minikube ssh -n $SOURCE_NODE "sudo cp $CHECKPOINT_PATH /tmp/$ARCHIVE_NAME && sudo chmod 644 /tmp/$ARCHIVE_NAME"
minikube cp "$SOURCE_NODE:/tmp/$ARCHIVE_NAME" "$TRANSIT_DIR/checkpoint.tar"
minikube ssh -n $SOURCE_NODE "sudo rm /tmp/$ARCHIVE_NAME"

# Verifica integrità
if [ ! -f "$TRANSIT_DIR/checkpoint.tar" ]; then
    echo -e "\n${RED}❌ ERRORE: File checkpoint non ricevuto.${NC}"
    exit 1
fi
echo -e "   ⬇️  Download completato.       "

# B. Carica su Destinazione
echo -ne "   ⬆️  Upload su $DEST_NODE...   \r"
minikube cp "$TRANSIT_DIR/checkpoint.tar" "$DEST_NODE:/tmp/checkpoint.tar"
echo -e "   ⬆️  Upload completato.       "


# 4. RICOSTRUZIONE IMMAGINE (Buildah)
echo -e "${YELLOW}[4/5] Ricostruzione Biologica (Buildah)...${NC}"

# Script remoto per costruire l'immagine 'restored' del Sidecar
BUILD_SCRIPT="
set -e
# Pulizia
sudo buildah rm restoration-lab 2>/dev/null || true

# Crea container scratch
sudo buildah from --name restoration-lab scratch

# Inietta il checkpoint (Memoria RAM + Filesystem differenziale)
sudo buildah add restoration-lab /tmp/checkpoint.tar /

# === PREPARAZIONE AMBIENTE ===
# Rimuove il trigger di salto per evitare loop al riavvio
MNT=\$(sudo buildah mount restoration-lab)
sudo rm -f \"\$MNT/tmp/prepare_jump\"
sudo buildah unmount restoration-lab
# =============================

# Configura annotazione per dire a CRI-O: 'Questa è una memoria congelata'
# Importante: Il nome annotazione deve matchare il nome container nel pod (sidecar-guardian)
sudo buildah config --annotation \"io.kubernetes.cri-o.annotations.checkpoint.name=$TARGET_CONTAINER\" restoration-lab

# Commit immagine finale
sudo buildah commit restoration-lab $RESTORED_IMG

# Cleanup
sudo buildah rm restoration-lab
sudo rm /tmp/checkpoint.tar
"

echo "   🔨 Compilazione immagine su $DEST_NODE..."
minikube ssh -n $DEST_NODE "$BUILD_SCRIPT"

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ ERRORE: Buildah fallito su nodo destinazione.${NC}"
    exit 1
fi
echo -e "${GREEN}   ✅ Immagine $RESTORED_IMG pronta.${NC}"


# 5. RESTORE & WAKE UP
echo -e "${YELLOW}[5/5] Switchover & Wake Up...${NC}"

# A. COSTRUZIONE PATCH JSON
# Questa patch dice a Kubernetes:
# 1. Sposta il pod su DEST_NODE
# 2. Usa l'immagine 'restored' per il Sidecar
# 3. Usa l'immagine 'originale' per il Payload (riavvio pulito)
PATCH_JSON=$(cat <<EOF
{
  "spec": {
    "template": {
      "spec": {
        "terminationGracePeriodSeconds": 0,
        "nodeSelector": {
          "type": "satellite",
          "kubernetes.io/hostname": "$DEST_NODE"
        },
        "containers": [
          {
            "name": "$TARGET_CONTAINER",
            "image": "$RESTORED_IMG"
          },
          {
            "name": "$IGNORED_CONTAINER",
            "image": "$ORIGINAL_PAYLOAD_IMG"
          }
        ]
      }
    }
  }
}
EOF
)

# B. APPLICAZIONE PATCH (Strategia Delete-Then-Create simulata)
echo "   📉 Terminazione vecchio Pod..."
kubectl scale deployment space-mission --replicas=0 >/dev/null
sleep 2

echo "   ⚡ Applicazione Patch Orbitale..."
# Nota: --type='strategic' fonde le configurazioni, sovrasrivendo le immagini
kubectl patch deployment space-mission --type='strategic' -p "$PATCH_JSON" >/dev/null

echo "   📈 Riavvio Deployment..."
kubectl scale deployment space-mission --replicas=1 >/dev/null

# C. ATTESA RIEMERSIONE
echo "   ⏳ Ricerca nuovo Pod su $DEST_NODE..."
NEW_POD=""
MAX_WAIT=60
count=0

while [ $count -lt $MAX_WAIT ]; do
    # Cerca pod Running sul nuovo nodo
    NEW_POD=$(kubectl get pod -l $POD_LABEL --field-selector spec.nodeName=$DEST_NODE,status.phase=Running -o jsonpath="{.items[0].metadata.name}" 2>/dev/null)

    if [ ! -z "$NEW_POD" ]; then
        echo -e "${GREEN}   ✅ Pod Agganciato: $NEW_POD${NC}"
        break
    fi

    echo -ne "      ...scansione ($count/${MAX_WAIT}s)...\r"
    sleep 1
    ((count++))
done
echo ""

if [ -z "$NEW_POD" ]; then
    echo -e "${RED}❌ TIMEOUT: Il pod non è ripartito.${NC}"
    kubectl get pods -l $POD_LABEL
    exit 1
fi

# D. WAKE UP (Sveglia il Sidecar)
echo "   🔔 Invio Segnale 'Wake Up' al Sidecar..."

# Loop di retry per connessione (il pod è Running ma magari il network non è pronto)
until kubectl exec $NEW_POD -c $TARGET_CONTAINER -- touch /tmp/landed 2>/dev/null; do
    echo "      ...bussando alla porta ($count)..."
    sleep 1
    ((count++))
    if [ $count -gt 20 ]; then
        echo -e "${RED}❌ ERRORE: Impossibile contattare il Sidecar.${NC}"
        exit 1
    fi
done

echo -e "${CYAN}🎉 MIGRAZIONE COMPLETATA CON SUCCESSO!${NC}"
echo "   - Sidecar: Memoria Ripristinata (Stateful)"
echo "   - Payload: Riavviato (Stateless)"