#!/bin/bash

# ==============================================================================
# SPACE CLOUD V5.2 - TRIPLE VECTOR INJECTOR
# Compila e inietta le immagini 'Guardian', 'Payload' e 'Node Agent' nei nodi.
# Bypass del Registry per ambienti Edge/Air-gapped.
# ==============================================================================

# --- CONFIGURAZIONE ---
# Nodi bersaglio (Satelliti)
NODES=("minikube-m02" "minikube-m03" "minikube-m04")

# Configurazioni Immagini
IMG_GUARDIAN="localhost/space-sidecar:latest"
IMG_PAYLOAD="localhost/space-workload:latest"
IMG_AGENT="localhost/space-node-agent:latest"

# Percorsi contesti di build (Relativi alla root del progetto)
PATH_GUARDIAN="./src/state-sidecar"
PATH_PAYLOAD="./src/training-workload"
PATH_AGENT="./infrastructure/grpc_agent"

# Nomi file temporanei
TAR_GUARDIAN="sidecar-guardian.tar"
TAR_PAYLOAD="payload-phoenix.tar"
TAR_AGENT="node-agent.tar"

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

echo -e "${BLUE}>>> 🚀 INIZIALIZZAZIONE PROCEDURA DI LANCIO V5.2 <<<${NC}"

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

# --- C. Build Node Agent ---
echo -e "   🕵️  Costruzione NODE AGENT (P2P Daemon)..."
docker build -t $IMG_AGENT -f "$PATH_AGENT/Dockerfile.agent" "$PATH_AGENT" >/dev/null
if [ $? -ne 0 ]; then echo -e "${RED}❌ Build Node Agent Fallita${NC}"; exit 1; fi
echo -e "   💾 Salvataggio archivio Node Agent..."
docker save $IMG_AGENT -o $TAR_AGENT

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
    minikube cp $TAR_AGENT $NODE:/tmp/$TAR_AGENT
    echo -e "      ⬆️  Upload completato.   "

    # --- Importazione (CRI-O/Buildah) ---
    echo -ne "      📦 Estrazione nei container storage... \r"
    minikube ssh -n $NODE "sudo buildah pull docker-archive:/tmp/$TAR_GUARDIAN" >/dev/null 2>&1
    minikube ssh -n $NODE "sudo buildah pull docker-archive:/tmp/$TAR_PAYLOAD" >/dev/null 2>&1
    minikube ssh -n $NODE "sudo buildah pull docker-archive:/tmp/$TAR_AGENT" >/dev/null 2>&1
    echo -e "      📦 Estrazione completata.            "

    # --- Cleanup ---
    minikube ssh -n $NODE "sudo rm -f /tmp/$TAR_GUARDIAN /tmp/$TAR_PAYLOAD /tmp/$TAR_AGENT"

    echo -e "${GREEN}      ✅ Nodo $NODE Sincronizzato.${NC}"
done

# ==============================================================================
# FASE 3: PULIZIA LOCALE
# ==============================================================================
echo -e "\n${YELLOW}[3/3] Pulizia post-lancio...${NC}"
rm -f $TAR_GUARDIAN $TAR_PAYLOAD $TAR_AGENT
echo -e "${GREEN}✅ Missione Iniezione Completata. Sistemi pronti.${NC}"