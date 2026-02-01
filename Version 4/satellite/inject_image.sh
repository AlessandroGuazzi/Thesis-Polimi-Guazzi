#!/bin/bash

# ======================================================
# SPACE CLOUD - IMAGE INJECTOR
# Carica l'immagine nei nodi satellite (bypassando il registry)
# ======================================================

IMAGE_NAME="localhost/space-dashboard:native"
TAR_FILE="space-mission.tar"
NODES=("minikube-m02" "minikube-m03" "minikube-m04")

GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

echo -e "${GREEN}>>> 💉 INIEZIONE IMMAGINE SPAZIALE <<<${NC}"

# 1. CONTROLLO E BUILD
if [ -f "$TAR_FILE" ]; then
    echo -e "${YELLOW}📦 Trovato archivio esistente: $TAR_FILE${NC}"
    read -p "   Vuoi usare questo file (y) o rigenerarlo (n)? [y/n]: " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "   🔨 Rigenerazione immagine (Docker Build)..."
        docker build -t $IMAGE_NAME .
        echo "   💾 Salvataggio in TAR..."
        docker save $IMAGE_NAME -o $TAR_FILE
    else
        echo "   ⏩ Uso archivio esistente (Skip Build)."
    fi
else
    echo "   ⚠️  Nessun archivio trovato. Avvio Build..."
    docker build -t $IMAGE_NAME .
    docker save $IMAGE_NAME -o $TAR_FILE
fi

# 2. INIEZIONE SUI NODI
echo -e "\n${GREEN}🚚 Inizio Trasferimento sui Satelliti...${NC}"

for NODE in "${NODES[@]}"; do
    echo -e "   ➡️  Target: $NODE"

    # Copia
    minikube cp $TAR_FILE $NODE:/tmp/$TAR_FILE

    # Importazione (Silenziosa se va bene)
    minikube ssh -n $NODE "sudo buildah pull docker-archive:/tmp/$TAR_FILE" >/dev/null

    # Pulizia remota
    minikube ssh -n $NODE "sudo rm -f /tmp/$TAR_FILE"

    echo "      ✅ Caricata."
done

echo -e "${GREEN}✅ Procedura completata.${NC}"