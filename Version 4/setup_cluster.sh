#!/bin/bash

# ==============================================================================
# SPACE CLOUD V6.2 - SETUP INTEGRALE (CRI-O 1.30 + PATCHES)
# Strategia: Start Default -> Upgrade In-Place -> Sysctl Fix -> Runc Wrapper
# ==============================================================================

GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'
CRIO_VERSION_MAJOR="v1.30"

echo -e "${BLUE}>>> 🚀 SPACE CLOUD SETUP V6.2 (FULL PATCHED) <<<${NC}"

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

# 2. AGGIORNAMENTO E PATCHING (Cuore del sistema)
# ------------------------------------------------------------------------------
echo -e "${GREEN}[2/4] Aggiornamento CRI-O 1.30 e Applicazione Patch CRIU...${NC}"

for NODE in "${NODES[@]}"; do
    echo -e "    🛠️  Configurazione nodo: ${BLUE}$NODE${NC}"

    minikube ssh -n $NODE -p minikube "sudo -i <<EOF
        # --- A. PREPARAZIONE REPO ---
        apt-get update -qq
        DEBIAN_FRONTEND=noninteractive apt-get install -y apt-transport-https ca-certificates curl gnupg lsb-release -qq
        mkdir -p /etc/apt/keyrings
        curl -fsSL https://pkgs.k8s.io/addons:/cri-o:/stable:/$CRIO_VERSION_MAJOR/deb/Release.key | gpg --dearmor -o /etc/apt/keyrings/cri-o-apt-keyring.gpg --yes
        echo 'deb [signed-by=/etc/apt/keyrings/cri-o-apt-keyring.gpg] https://pkgs.k8s.io/addons:/cri-o:/stable:/$CRIO_VERSION_MAJOR/deb/ /' | tee /etc/apt/sources.list.d/cri-o.list

        # --- B. STOP SERVIZI E CLEANUP ---
        systemctl stop kubelet
        systemctl stop crio
        apt-get remove -y conmon >/dev/null 2>&1 || true

        # --- C. INSTALLAZIONE PACCHETTI ---
        apt-get update -qq
        DEBIAN_FRONTEND=noninteractive apt-get -o Dpkg::Options::='--force-overwrite' install -y criu cri-o buildah -qq

        # --- D. CONFIGURAZIONE CRI-O ---
        mkdir -p /etc/crio/crio.conf.d
        cat > /etc/crio/crio.conf.d/99-custom.conf <<CONF
[crio.runtime]
enable_criu_support = true
manage_ns_lifecycle = true
drop_infra_ctr = false
default_runtime = \"runc\"
cgroup_manager = \"systemd\"
CONF

        # --- E. FIX KUBELET ---
        sed -i 's/cgroupDriver: cgroupfs/cgroupDriver: systemd/g' /var/lib/kubelet/config.yaml

        # ======================================================================
        # PATCH 1: AUMENTO LIMITI INOTIFY (Per evitare 'Too many open files')
        # ======================================================================
        echo '>>> Applicazione Patch Sysctl (Inotify)...'
        sysctl -w fs.inotify.max_user_watches=524288
        sysctl -w fs.inotify.max_user_instances=8192
        # Rendiamolo persistente nel caso (improbabile) di riavvio del container docker
        echo 'fs.inotify.max_user_watches=524288' >> /etc/sysctl.conf
        echo 'fs.inotify.max_user_instances=8192' >> /etc/sysctl.conf

        # ======================================================================
        # PATCH 2: RUNC WRAPPER (Per forzare --tcp-established)
        # ======================================================================
        echo '>>> Applicazione Patch Runc Wrapper...'
        if [ ! -f /usr/bin/runc.real ]; then
            # 1. Rinomina il binario originale
            mv /usr/bin/runc /usr/bin/runc.real

            # 2. Crea lo script wrapper
            cat > /usr/bin/runc <<'WRAPPER'
#!/bin/bash
# Wrapper per forzare il salvataggio delle connessioni TCP aperte
exec /usr/bin/runc.real \"\$@\" --tcp-established
WRAPPER

            # 3. Rendi eseguibile
            chmod +x /usr/bin/runc
        fi

        # --- F. RIAVVIO SERVIZI ---
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
echo -e "${GREEN}[4/4] Configurazione Finale (Labels & Ingress)...${NC}"

for NODE in "${NODES[@]}"; do
    until kubectl get node $NODE >/dev/null 2>&1; do echo "Wait for $NODE..."; sleep 3; done
done

# Assegnazione ruoli
kubectl label node minikube type=ground-station --overwrite >/dev/null
kubectl label node minikube-m02 type=satellite --overwrite >/dev/null
kubectl label node minikube-m03 type=satellite --overwrite >/dev/null
kubectl label node minikube-m04 type=satellite --overwrite >/dev/null

# Attivazione Ingress
minikube addons enable ingress -p minikube >/dev/null 2>&1 || true

echo -e "${BLUE}>>> SETUP COMPLETATO CON SUCCESSO <<<${NC}"
echo -e "✅ CRI-O 1.30 Installato"
echo -e "✅ Patch Inotify Applicata"
echo -e "✅ Runc Wrapper (--tcp-established) Attivo"
echo -e "\n${BLUE}Verifica Finale:${NC}"
minikube ssh -n minikube-m02 "sysctl fs.inotify.max_user_watches && ls -l /usr/bin/runc"