#!/bin/bash

# ==============================================================================
# SPACE CLOUD V5.2 - TRIPLE VECTOR INJECTOR
# Compila e inietta le immagini 'Guardian', 'Payload' e 'Node Agent' nei nodi.
# ==============================================================================

# --- CONFIGURATION ---
NODES=("minikube-m02" "minikube-m03" "minikube-m04")
IMG_GUARDIAN="localhost/space-sidecar:latest"
IMG_PAYLOAD="localhost/space-workload:latest"
IMG_AGENT="localhost/space-node-agent:latest"

PATH_GUARDIAN="./src/state-sidecar"
PATH_PAYLOAD="./src/training-workload"
PATH_AGENT="./infrastructure/grpc_agent"

TAR_GUARDIAN="sidecar-guardian.tar"
TAR_PAYLOAD="payload-phoenix.tar"
TAR_AGENT="node-agent.tar"

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# --- VALIDATION ---
if [ ! -d "$PATH_GUARDIAN" ]; then
    echo -e "${RED}❌ ERRORE: Esegui lo script dalla root del progetto!${NC}"
    echo "   Esempio: ./ops/build_and_inject.sh"
    exit 1
fi

echo -e "${BLUE}>>> 🚀 INIZIALIZZAZIONE PROCEDURA DI LANCIO V5.2 <<<${NC}"

# --- BLOCK 1: BUILD & PACKAGE ---
# Compiles each module using Docker and exports them as TAR archives for manual transport.
echo -e "\n${YELLOW}[1/3] Compilazione Moduli (Docker Build)...${NC}"

# A. Build Guardian
docker build -t $IMG_GUARDIAN -f "$PATH_GUARDIAN/Dockerfile.sidecar" "$PATH_GUARDIAN" >/dev/null
docker save $IMG_GUARDIAN -o $TAR_GUARDIAN

# B. Build Payload
docker build -t $IMG_PAYLOAD -f "$PATH_PAYLOAD/Dockerfile.workload" "$PATH_PAYLOAD" >/dev/null
docker save $IMG_PAYLOAD -o $TAR_PAYLOAD

# C. Build Node Agent
docker build -t $IMG_AGENT -f "$PATH_AGENT/Dockerfile.agent" "$PATH_AGENT" >/dev/null
docker save $IMG_AGENT -o $TAR_AGENT

echo -e "${GREEN}✅ Artefatti pronti al lancio.${NC}"

# --- BLOCK 2: TRANSFER & INGESTION ---
# Uploads the TAR files to the satellites and uses Buildah to inject them into the local runtime storage.
echo -e "\n${YELLOW}[2/3] Iniezione sui Satelliti...${NC}"

for NODE in "${NODES[@]}"; do
    echo -e "   📡 Uplink verso nodo: ${BLUE}$NODE${NC}"

    minikube cp $TAR_GUARDIAN $NODE:/tmp/$TAR_GUARDIAN
    minikube cp $TAR_PAYLOAD $NODE:/tmp/$TAR_PAYLOAD
    minikube cp $TAR_AGENT $NODE:/tmp/$TAR_AGENT

    # Importing into container storage using Buildah to bypass the need for a registry.
    minikube ssh -n $NODE "sudo buildah pull docker-archive:/tmp/$TAR_GUARDIAN" >/dev/null 2>&1
    minikube ssh -n $NODE "sudo buildah pull docker-archive:/tmp/$TAR_PAYLOAD" >/dev/null 2>&1
    minikube ssh -n $NODE "sudo buildah pull docker-archive:/tmp/$TAR_AGENT" >/dev/null 2>&1

    minikube ssh -n $NODE "sudo rm -f /tmp/$TAR_GUARDIAN /tmp/$TAR_PAYLOAD /tmp/$TAR_AGENT"
    echo -e "${GREEN}      ✅ Nodo $NODE Sincronizzato.${NC}"
done

# --- BLOCK 3: LOCAL CLEANUP ---
# Removes temporary TAR archives from the host machine to save space.
echo -e "\n${YELLOW}[3/3] Pulizia post-lancio...${NC}"
rm -f $TAR_GUARDIAN $TAR_PAYLOAD $TAR_AGENT
echo -e "${GREEN}✅ Missione Iniezione Completata. Sistemi pronti.${NC}"