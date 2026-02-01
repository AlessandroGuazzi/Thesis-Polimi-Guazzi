#!/bin/bash

# ==============================================================================
# SPACE CLOUD V6.1 - PROGRESSIVE SETUP (CONFLICT FIX)
# Strategia: Start Default -> Remove Conflicts -> Upgrade In-Place -> Force Runc
# ==============================================================================

GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'
CRIO_VERSION_MAJOR="v1.30"

echo -e "${BLUE}>>> 🚀 SPACE CLOUD SETUP V6.1 (CONFLICT FIX) <<<${NC}"

# 1. AVVIO STANDARD
# ------------------------------------------------------------------------------
echo -e "${GREEN}[1/4] Avvio Cluster Base (CRI-O 1.24 + Runc)...${NC}"
minikube delete --all
minikube start \
  --nodes 4 \
  --driver=docker \
  --container-runtime=cri-o \
  --feature-gates=ContainerCheckpoint=true \
  --cpus=2 \
  --memory=2048 \
  --profile=minikube

NODES=("minikube" "minikube-m02" "minikube-m03" "minikube-m04")

# 2. AGGIORNAMENTO IN-PLACE (CRI-O 1.30)
# ------------------------------------------------------------------------------
echo -e "${GREEN}[2/4] Aggiornamento CRI-O alla v1.30 (Mantenendo RUNC)...${NC}"

for NODE in "${NODES[@]}"; do
    echo -e "    🛠️  Aggiornamento nodo: ${BLUE}$NODE${NC}"

    minikube ssh -n $NODE -p minikube "sudo -i <<EOF
        # A. PREPARAZIONE REPO
        apt-get update -qq
        DEBIAN_FRONTEND=noninteractive apt-get install -y apt-transport-https ca-certificates curl gnupg lsb-release -qq
        mkdir -p /etc/apt/keyrings
        curl -fsSL https://pkgs.k8s.io/addons:/cri-o:/stable:/$CRIO_VERSION_MAJOR/deb/Release.key | gpg --dearmor -o /etc/apt/keyrings/cri-o-apt-keyring.gpg --yes
        echo 'deb [signed-by=/etc/apt/keyrings/cri-o-apt-keyring.gpg] https://pkgs.k8s.io/addons:/cri-o:/stable:/$CRIO_VERSION_MAJOR/deb/ /' | tee /etc/apt/sources.list.d/cri-o.list

        # B. FERMIAMO I MOTORI E RIMUOVIAMO CONFLITTI
        systemctl stop kubelet
        systemctl stop crio

        # --- FIX CRITICO: RIMOZIONE VECCHIO CONMON ---
        # Rimuoviamo il pacchetto che causa il conflitto 'trying to overwrite'
        apt-get remove -y conmon >/dev/null 2>&1 || true

        # C. AGGIORNAMENTO PACCHETTI (FORCE OVERWRITE)
        apt-get update -qq
        # Usiamo --force-overwrite per garantire che CRI-O 1.30 possa scrivere i suoi file
        DEBIAN_FRONTEND=noninteractive apt-get -o Dpkg::Options::='--force-overwrite' install -y criu cri-o buildah -qq

        # D. CONFIGURAZIONE IBRIDA (CRI-O 1.30 + RUNC)
        mkdir -p /etc/crio/crio.conf.d

        echo '[crio.runtime]' > /etc/crio/crio.conf.d/99-custom.conf
        echo 'enable_criu_support = true' >> /etc/crio/crio.conf.d/99-custom.conf
        echo 'manage_ns_lifecycle = true' >> /etc/crio/crio.conf.d/99-custom.conf
        echo 'drop_infra_ctr = false' >> /etc/crio/crio.conf.d/99-custom.conf

        # FORZA RUNC (Evita errore libcriu.so)
        echo 'default_runtime = \"runc\"' >> /etc/crio/crio.conf.d/99-custom.conf

        echo 'cgroup_manager = \"systemd\"' >> /etc/crio/crio.conf.d/99-custom.conf

        # E. FIX KUBELET
        sed -i 's/cgroupDriver: cgroupfs/cgroupDriver: systemd/g' /var/lib/kubelet/config.yaml

        # F. RIAVVIO
        systemctl daemon-reload
        systemctl start crio
        systemctl start kubelet
EOF"
done

# 3. ATTESA
# ------------------------------------------------------------------------------
echo -e "${GREEN}[3/4] Riavvio servizi e stabilizzazione (45s)...${NC}"
sleep 45

# 4. LABELING & INGRESS
# ------------------------------------------------------------------------------
echo -e "${GREEN}[4/4] Configurazione Finale...${NC}"

for NODE in "${NODES[@]}"; do
    until kubectl get node $NODE >/dev/null 2>&1; do echo "Wait for $NODE..."; sleep 3; done
done
kubectl label node minikube type=ground-station --overwrite >/dev/null
kubectl label node minikube-m02 type=satellite --overwrite >/dev/null
kubectl label node minikube-m03 type=satellite --overwrite >/dev/null
kubectl label node minikube-m04 type=satellite --overwrite >/dev/null

minikube addons enable ingress -p minikube >/dev/null 2>&1 || true

echo -e "${BLUE}>>> SETUP COMPLETATO (4 NODI ONLINE) <<<${NC}"
kubectl get nodes -o wide
echo -e "\n${BLUE}Verifica Versione e Runtime:${NC}"
minikube ssh -n minikube "crio --version | head -n 1 && grep default_runtime /etc/crio/crio.conf.d/99-custom.conf"