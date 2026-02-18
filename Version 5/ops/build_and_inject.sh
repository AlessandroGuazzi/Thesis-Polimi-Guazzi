#!/bin/bash

# ==============================================================================
# SPACE CLOUD V5 - DUAL VECTOR INJECTOR
# Compila e inietta le immagini 'Guardian' e 'Payload' direttamente nei nodi.
# Bypass del Registry per ambienti Edge/Air-gapped.
# ==============================================================================

# --- CONFIGURAZIONE ---
# Nodi bersaglio (Satelliti)
NODES=("minikube-m02" "minikube-m03" "minikube-m04")

# Configurazioni Immagini
IMG_GUARDIAN="localhost/space-sidecar:latest"
IMG_PAYLOAD="localhost/space-workload:latest"

# Percorsi contesti di build (Relativi alla root del progetto)
PATH_GUARDIAN="./src/state-sidecar"
PATH_PAYLOAD="./src/training-workload"

# Nomi file temporanei
TAR_GUARDIAN="sidecar-guardian.tar"
TAR_PAYLOAD="payload-phoenix.tar"

# Colori output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Check esecuzione dalla root
if [ ! -d "$PATH_GUARDIAN" ]; then
    echo -e "${RED}❌ ERRORE: Esegui lo script dalla root del progetto!${NC}"
    echo "   Esempio: ./ops/build_and_inject.sh"
    exit 1
fi

echo -e "${BLUE}>>> 🚀 INIZIALIZZAZIONE PROCEDURA DI LANCIO V5 <<<${NC}"

# ==============================================================================
# FASE 1: BUILD & PACKAGE (Locale)
# ==============================================================================
echo -e "\n${YELLOW}[1/3] Compilazione Moduli (Docker Build)...${NC}"

# --- A. Build Guardian ---
echo -e "   🛡️  Costruzione GUARDIAN (Sidecar)..."
docker build -t $IMG_GUARDIAN -f "$PATH_GUARDIAN/Dockerfile.sidecar" "$PATH_GUARDIAN" >/dev/null
if [ $? -ne 0 ]; then echo -e "${RED}❌ Build Guardian Fallita${NC}"; exit 1; fi

echo -e "   💾 Salvataggio archivio Guardian..."
docker save $IMG_GUARDIAN -o $TAR_GUARDIAN

# --- B. Build Payload ---
echo -e "   🔥 Costruzione PAYLOAD (Workload)..."
docker build -t $IMG_PAYLOAD -f "$PATH_PAYLOAD/Dockerfile.workload" "$PATH_PAYLOAD" >/dev/null
if [ $? -ne 0 ]; then echo -e "${RED}❌ Build Payload Fallita${NC}"; exit 1; fi

echo -e "   💾 Salvataggio archivio Payload..."
docker save $IMG_PAYLOAD -o $TAR_PAYLOAD

echo -e "${GREEN}✅ Artefatti pronti al lancio.${NC}"

# ==============================================================================
# FASE 2: TRASFERIMENTO & INGESTIONE (Remoto)
# ==============================================================================
echo -e "\n${YELLOW}[2/3] Iniezione sui Satelliti...${NC}"

for NODE in "${NODES[@]}"; do
    echo -e "   📡 Uplink verso nodo: ${BLUE}$NODE${NC}"

    # --- Upload ---
    echo -ne "      ⬆️  Upload archivi... \r"
    minikube cp $TAR_GUARDIAN $NODE:/tmp/$TAR_GUARDIAN
    minikube cp $TAR_PAYLOAD $NODE:/tmp/$TAR_PAYLOAD
    echo -e "      ⬆️  Upload completato.   "

    # --- Importazione (CRI-O/Buildah) ---
    # Nota: Usiamo 'docker-archive' come trasporto per buildah pull
    echo -ne "      📦 Estrazione nei container storage... \r"

    # Import Guardian
    minikube ssh -n $NODE "sudo buildah pull docker-archive:/tmp/$TAR_GUARDIAN" >/dev/null 2>&1

    # Import Payload
    minikube ssh -n $NODE "sudo buildah pull docker-archive:/tmp/$TAR_PAYLOAD" >/dev/null 2>&1

    echo -e "      📦 Estrazione completata.            "

    # --- Cleanup ---
    minikube ssh -n $NODE "sudo rm -f /tmp/$TAR_GUARDIAN /tmp/$TAR_PAYLOAD"

    echo -e "${GREEN}      ✅ Nodo $NODE Sincronizzato.${NC}"
done

# ==============================================================================
# FASE 3: PULIZIA LOCALE
# ==============================================================================
echo -e "\n${YELLOW}[3/3] Pulizia post-lancio...${NC}"
rm -f $TAR_GUARDIAN $TAR_PAYLOAD
echo -e "${GREEN}✅ Missione Iniezione Completata. Sistemi pronti.${NC}"