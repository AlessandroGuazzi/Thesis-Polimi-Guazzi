#!/bin/bash

# ==============================================================================
# SPACE CLOUD V2.0 - INFRASTRUCTURE SETUP
# Architettura: 1 Control Plane (Ground) + 2 Workers (Satellites)
# ==============================================================================

# Colori per output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}>>> 🚀 INIZIALIZZAZIONE CLUSTER SATELLITARE <<<${NC}"

# 1. PULIZIA E AVVIO
# ------------------------------------------------------------------------------
echo -e "${GREEN}[1/5] Reset dell'ambiente precedente...${NC}"
minikube delete --all

echo -e "${GREEN}[2/5] Avvio Cluster Multi-Nodo (3 Nodi)...${NC}"
# --nodes 3: Crea il nodo master + 2 worker (m02, m03)
# --driver=docker: Fondamentale per gestire i file con docker cp
# --container-runtime=cri-o: Obbligatorio per il Checkpoint API
minikube start \
  --nodes 3 \
  --driver=docker \
  --container-runtime=cri-o \
  --feature-gates=ContainerCheckpoint=true \
  --cpus=2 \
  --memory=2048 \
  --profile=minikube

# 2. INSTALLAZIONE DIPENDENZE (CRIU & BUILDAH)
# ------------------------------------------------------------------------------
# Dobbiamo installare il software su TUTTI i nodi, altrimenti la migrazione fallisce.
NODES=("minikube" "minikube-m02" "minikube-m03")

echo -e "${GREEN}[3/5] Installazione CRIU e Buildah sulla flotta...${NC}"

for NODE in "${NODES[@]}"; do
    echo -e "    🛠️  Configurazione nodo: ${BLUE}$NODE${NC}"

    # Eseguiamo comandi SSH dentro ogni nodo
    minikube ssh -n $NODE -p minikube "
        # Diventiamo root
        sudo -i <<EOF

        # 1. Aggiornamento repo (silenzioso)
        apt-get update -qq

        # 2. Installazione pacchetti
        # Usiamo force-overwrite per evitare conflitti noti su immagini Ubuntu/Debian
        DEBIAN_FRONTEND=noninteractive apt-get -o Dpkg::Options::='--force-overwrite' install -y criu buildah iproute2 iptables -qq

        # 3. Configurazione CRI-O per abilitare CRIU
        # Usiamo una config drop-in, più sicura che modificare crio.conf
        mkdir -p /etc/crio/crio.conf.d
        echo '[crio.runtime]' > /etc/crio/crio.conf.d/99-enable-criu.conf
        echo 'enable_criu_support = true' >> /etc/crio/crio.conf.d/99-enable-criu.conf

        # 4. Riavvio Runtime per applicare modifiche
        systemctl reload crio
EOF
    "
done

# 3. ETICHETTATURA (TOPOLOGIA)
# ------------------------------------------------------------------------------
echo -e "${GREEN}[4/5] Definizione Topologia Spaziale (Labels)...${NC}"

# Nodo 1 (Master) -> Ground Station
# Ospiterà Redis e Ingress Controller
kubectl label node minikube type=ground-station --overwrite
echo "    📍 minikube -> Ground Station (Control Plane)"

# Nodi 2 & 3 -> Satelliti
# Ospiteranno il carico di lavoro (Space Mission)
kubectl label node minikube-m02 type=satellite --overwrite
echo "    🛰️  minikube-m02 -> Satellite Alpha"

kubectl label node minikube-m03 type=satellite --overwrite
echo "    🛰️  minikube-m03 -> Satellite Beta"

# 4. ADDONS
# ------------------------------------------------------------------------------
echo -e "${GREEN}[5/5] Attivazione Sistemi di Comunicazione (Ingress)...${NC}"
minikube addons enable ingress -p minikube

echo -e "${BLUE}>>> ✅ CLUSTER PRONTO E OPERATIVO! <<<${NC}"
echo -e "    Usa 'kubectl get nodes --show-labels' per verificare."