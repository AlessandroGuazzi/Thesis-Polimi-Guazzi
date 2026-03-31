#!/bin/bash

# ==============================================================================
# SPACE CLOUD V6 - INFRASTRUCTURE PROVISIONING
# Configures Minikube nodes with patched CRI-O for CRIU sidecar support,
# installs iproute2 (for tc ISL throttling) and openssh-client (for relay_transfer.sh).
# ==============================================================================

GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'
CRIO_VERSION_MAJOR="v1.30"

echo -e "${BLUE}>>> 🏗️  SPACE CLOUD V6: PREPARAZIONE CANTIERE ORBITALE <<<${NC}"

# --- BLOCK 1: CLUSTER INITIALIZATION ---
# Resets any existing environment and starts a 4-node cluster using the Docker driver.
# Enables the ContainerCheckpoint feature gate required for CRIU.
echo -e "${GREEN}[1/4] Lancio Istanza Minikube (4 Nodi)...${NC}"
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

# --- BLOCK 2: NODE PATCHING LOOP ---
# Connects to each node via SSH to install low-level tools and patch the container runtime.
echo -e "${GREEN}[2/4] Installazione Runtime Spaziale (CRI-O 1.30 + Tools)...${NC}"

for NODE in "${NODES[@]}"; do
    echo -e "    🔧 Patching nodo: ${BLUE}$NODE${NC}"

    minikube ssh -n $NODE -p minikube "sudo -i <<EOF
        # A. Setup CRI-O Repositories: Configures the official apt sources for CRI-O 1.30.
        apt-get update -qq
        DEBIAN_FRONTEND=noninteractive apt-get install -y apt-transport-https ca-certificates curl gnupg lsb-release -qq
        mkdir -p /etc/apt/keyrings
        curl -fsSL https://pkgs.k8s.io/addons:/cri-o:/stable:/$CRIO_VERSION_MAJOR/deb/Release.key | gpg --dearmor -o /etc/apt/keyrings/cri-o-apt-keyring.gpg --yes
        echo 'deb [signed-by=/etc/apt/keyrings/cri-o-apt-keyring.gpg] https://pkgs.k8s.io/addons:/cri-o:/stable:/$CRIO_VERSION_MAJOR/deb/ /' | tee /etc/apt/sources.list.d/cri-o.list

        # B. Service Management: Stops active runtime services to apply deep patches.
        systemctl stop kubelet crio
        apt-get remove -y conmon >/dev/null 2>&1 || true

        # C. Binary Installation: Installs CRIU (for memory checkpointing), Buildah (for
        #    image layering), iproute2 (for 'tc' ISL bandwidth throttling per §1.5),
        #    and openssh-client (for relay_transfer.sh multi-hop SSH pipe per §5.1).
        apt-get update -qq
        DEBIAN_FRONTEND=noninteractive apt-get -o Dpkg::Options::='--force-overwrite' install -y \
            criu cri-o buildah iproute2 openssh-client -qq

        # D. CRI-O Configuration: Enables the critical CRIU support flag and manages namespace lifecycles.
        mkdir -p /etc/crio/crio.conf.d
        cat > /etc/crio/crio.conf.d/99-sidecar.conf <<CONF
[crio.runtime]
enable_criu_support = true
manage_ns_lifecycle = true
drop_infra_ctr = false
default_runtime = \"runc\"
cgroup_manager = \"systemd\"
CONF

        # E. Kubelet Tuning: Aligns the Cgroup Driver with the systemd manager.
        sed -i 's/cgroupDriver: cgroupfs/cgroupDriver: systemd/g' /var/lib/kubelet/config.yaml

        # F. Kernel Optimization: Increases Inotify limits to allow real-time file system watching on /tmp.
        sysctl -w fs.inotify.max_user_watches=524288
        sysctl -w fs.inotify.max_user_instances=8192
        echo 'fs.inotify.max_user_watches=524288' >> /etc/sysctl.conf

        # G. Runc Wrapper: Implements a binary wrapper to bypass TCP connection checks during restore.
        if [ ! -f /usr/bin/runc.real ]; then
            mv /usr/bin/runc /usr/bin/runc.real
            cat > /usr/bin/runc <<'WRAPPER'
#!/bin/bash
exec /usr/bin/runc.real \"\$@\" --tcp-established
WRAPPER
            chmod +x /usr/bin/runc
        fi

        # H. Service Restart: Reloads configurations and brings the node back online.
        systemctl daemon-reload
        systemctl start crio kubelet
EOF"
done

# --- BLOCK 3: FINAL STABILIZATION & LABELING ---
# Waits for the cluster to normalize before assigning functional roles (Ground vs Satellite).
echo -e "${GREEN}[3/4] Attesa allineamento costellazione (45s)...${NC}"
sleep 45

echo -e "${GREEN}[4/4] Assegnazione Ruoli (Ground vs Satellite)...${NC}"
kubectl label node minikube type=ground-station --overwrite >/dev/null
kubectl label node minikube-m02 type=satellite --overwrite >/dev/null
kubectl label node minikube-m03 type=satellite --overwrite >/dev/null
kubectl label node minikube-m04 type=satellite --overwrite >/dev/null

minikube addons enable ingress -p minikube > /dev/null 2>&1 || true

# --- BLOCK 5: ISL THROTTLING ---
# Apply Linux tc (Traffic Control) rules to simulate 50 Mbps / 40 ms ISL constraints.
# This is called here so throttling is active before any workload is deployed.
# Expected transfer time for ≤25 MB SAMKNN checkpoint: ~4.0 s/hop.
echo -e "${GREEN}[5/5] Applying ISL bandwidth/latency throttling (tc)...${NC}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
bash "$DIR/apply_tc_throttling.sh"

echo -e "${BLUE}>>> INFRASTRUTTURA PRONTA. ESEGUIRE build_and_inject.sh <<<${NC}"