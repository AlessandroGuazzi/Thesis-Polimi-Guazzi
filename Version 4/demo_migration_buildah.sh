#!/bin/bash

# ==============================================================================
# SPACE MISSION MIGRATION V3.3 - POST-RESTORE HOOK
# Fix: Automatizza il 'wake_up' eseguendolo live sul container ripristinato
# ==============================================================================

# --- VALIDAZIONE ARGOMENTI ---
# Controlla che siano stati passati esattamente 2 parametri (source e dest node)
if [ "$#" -ne 2 ]; then
    echo "Uso: $0 <nodo-sorgente> <nodo-destinazione>"
    echo "Esempio: $0 minikube-m02 minikube-m03"
    exit 1
fi

# Salva parametri in variabili leggibili
SOURCE_NODE=$1
DEST_NODE=$2
POD_LABEL="app=space-mission"          # label Kubernetes per trovare il pod
TRANSIT_DIR="/tmp/checkpoint_transit"  # cartella locale temporanea per il checkpoint
RESTORED_IMAGE="localhost/space-mission:restored"  # nome immagine ricostruita

# --- COLORI PER OUTPUT ---
# Codici ANSI per rendere l’output più leggibile
GREEN='\033[0;32m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m' # reset colore

# Messaggio iniziale migrazione
echo -e "${CYAN}🚀 INIZIO MIGRAZIONE AUTOMATICA: $SOURCE_NODE -> $DEST_NODE${NC}"

# 1. TROVA IL POD
# Cerca il pod dell’app sul nodo sorgente usando label + field-selector
POD_NAME=$(kubectl get pod -l $POD_LABEL --field-selector spec.nodeName=$SOURCE_NODE -o jsonpath="{.items[0].metadata.name}")

# Se non trova nulla, abortisce
if [ -z "$POD_NAME" ]; then
    echo -e "${RED}❌ Nessun pod trovato sul nodo $SOURCE_NODE${NC}"
    exit 1
fi
echo "📍 Pod Target: $POD_NAME"

# 2. TRIGGER CHECKPOINT
echo -e "${CYAN}--- FASE 1: CHECKPOINT ---${NC}"

# Attiva la “flight mode” nel container creando un file trigger
echo "✈️  Attivazione Modalità Aereo..."
kubectl exec $POD_NAME -- touch /tmp/prepare_jump 2>/dev/null || true
echo "⏳ Attesa disconnessione client (5s)..."
sleep 5

# Avvia kubectl proxy locale per chiamare le API di checkpoint del nodo
PROXY_PORT=8001
kubectl proxy --port=$PROXY_PORT >/dev/null 2>&1 &
PROXY_PID=$!  # salva PID per chiuderlo dopo
sleep 2

# URL API checkpoint CRI-O/container runtime sul nodo sorgente
API_URL="http://127.0.0.1:$PROXY_PORT/api/v1/nodes/$SOURCE_NODE/proxy/checkpoint/default/$POD_NAME/main-app"
echo "📸 Richiesta snapshot..."

# Tenta la creazione del checkpoint fino a 5 volte
for i in {1..5}; do
    RESPONSE=$(curl -X POST -s "$API_URL")
    # Se la risposta contiene il path del checkpoint, estrai nome archivio
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

# Se non abbiamo ottenuto un path valido → errore
if [ -z "$REMOTE_PATH" ]; then
    echo -e "${RED}❌ Checkpoint fallito.${NC}"
    kill $PROXY_PID
    exit 1
fi

# Chiude il proxy locale
kill $PROXY_PID

# 3. TRASFERIMENTO
echo -e "${CYAN}--- FASE 2: TRASFERIMENTO & COSTRUZIONE ---${NC}"
# Prepara directory di transito pulita
mkdir -p $TRANSIT_DIR
rm -rf $TRANSIT_DIR/*

# Copia l’archivio checkpoint dal nodo sorgente a locale
echo "🔓 Prelievo archivio..."
minikube ssh -n $SOURCE_NODE "sudo cp $REMOTE_PATH /tmp/$ARCHIVE_NAME && sudo chmod 644 /tmp/$ARCHIVE_NAME"
minikube cp "$SOURCE_NODE:/tmp/$ARCHIVE_NAME" "$TRANSIT_DIR/$ARCHIVE_NAME"
minikube ssh -n $SOURCE_NODE "sudo rm /tmp/$ARCHIVE_NAME"

# Verifica che il file sia stato copiato correttamente
if [ ! -f "$TRANSIT_DIR/$ARCHIVE_NAME" ]; then
    echo -e "${RED}❌ Errore critico: File non scaricato.${NC}"
    exit 1
fi

# Copia il checkpoint sul nodo destinazione
echo "⬆️  Caricamento su $DEST_NODE..."
minikube cp "$TRANSIT_DIR/$ARCHIVE_NAME" "$DEST_NODE:/tmp/checkpoint.tar"

# 4. BUILDAH (Struttura Base)
# Costruisce una nuova immagine container a partire dal checkpoint
echo "🔨 Costruzione immagine su $DEST_NODE..."

# Script eseguito via SSH sul nodo destinazione
BUILD_SCRIPT="
set -e
# Pulisce eventuali container buildah precedenti
sudo buildah rm restoration-lab 2>/dev/null || true
# Crea container buildah vuoto (scratch)
sudo buildah from --name restoration-lab scratch
# Aggiunge il filesystem del checkpoint
sudo buildah add restoration-lab /tmp/checkpoint.tar /

# === PREPARAZIONE FILESYSTEM ===
# Monta filesystem immagine per piccole correzioni
MNT=\$(sudo buildah mount restoration-lab)
sudo mkdir -p \"\$MNT/tmp\"
# Rimuove eventuale vecchio trigger di salto
sudo rm -f \"\$MNT/tmp/prepare_jump\"
sudo buildah unmount restoration-lab
# ===============================

# Aggiunge annotation richiesta dal runtime per restore checkpoint
sudo buildah config --annotation \"io.kubernetes.cri-o.annotations.checkpoint.name=main-app\" restoration-lab
# Commit immagine finale
sudo buildah commit restoration-lab $RESTORED_IMAGE
# Cleanup container buildah
sudo buildah rm restoration-lab
sudo rm /tmp/checkpoint.tar
"

# Esegue lo script di build sul nodo destinazione
minikube ssh -n $DEST_NODE "$BUILD_SCRIPT"

# Controlla esito build
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Immagine pronta.${NC}"
else
    echo -e "${RED}❌ Errore build.${NC}"
    exit 1
fi

# 5. RESTORE & WAKE UP
echo -e "${CYAN}--- FASE 3: SWITCHOVER & WAKE UP (SMART LOCK) ---${NC}"

# A. PREPARAZIONE CONFIGURAZIONE
# JSON patch per forzare il deployment sul nodo destinazione con la nuova immagine
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
# Applica la patch al deployment per fare lo switchover
echo "⚡ Applicazione Switchover..."
kubectl patch deployment space-mission --type='strategic' -p "$PATCH_JSON"

# C. PULIZIA VECCHIO POD
# Cancella il vecchio pod senza attendere (più veloce)
kubectl delete pod $POD_NAME --wait=false 2>/dev/null &

echo "⏳ Ricerca nuovo Pod ATTIVO su $DEST_NODE..."

# Variabili controllo ricerca pod ripristinato
TARGET_POD=""
MAX_WAIT=60
count=0

# Loop di attesa intelligente del nuovo pod Running sul nodo destinazione
while [ $count -lt $MAX_WAIT ]; do

    # Cerca pod con label corretta, sul nodo giusto e già Running
    TARGET_POD=$(kubectl get pod -l $POD_LABEL --field-selector spec.nodeName=$DEST_NODE,status.phase=Running -o jsonpath="{.items[0].metadata.name}" 2>/dev/null)

    if [ ! -z "$TARGET_POD" ]; then
        # Verifica che il container sia realmente partito (campo startedAt presente)
        IS_STARTED=$(kubectl get pod $TARGET_POD -o jsonpath='{.status.containerStatuses[0].state.running.startedAt}' 2>/dev/null)

        if [[ "$IS_STARTED" == *"20"* ]]; then
            echo -e "${GREEN}   ✅ Agganciato Pod Operativo: $TARGET_POD${NC}"
            break
        fi
    fi

    # Output di progresso su una sola riga
    echo -ne "   ...scansione orbita $DEST_NODE ($count/${MAX_WAIT}s)...\r"
    sleep 1
    ((count++))

    # Reset variabile per sicurezza
    TARGET_POD=""
done
echo ""

# Se non trova pod valido entro timeout → errore
if [ -z "$TARGET_POD" ]; then
    echo -e "${RED}❌ Timeout: Nessun pod Running trovato su $DEST_NODE.${NC}"
    echo "Stato attuale del cluster:"
    kubectl get pods -o wide
    exit 1
fi

# D. WAKE UP
# Invia segnale di “atterraggio” al container ripristinato
echo "🔔 WAKE UP (Invio segnale a $TARGET_POD)..."

# Tenta exec ripetuti finché il container risponde
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

# Messaggio finale successo
echo -e "${CYAN}🎉 MIGRAZIONE COMPLETATA!${NC}"
