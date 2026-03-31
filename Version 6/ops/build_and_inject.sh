#!/bin/bash

# ==============================================================================
# SPACE CLOUD V6 - QUAD VECTOR INJECTOR
# Builds and injects all four container images into the satellite nodes:
#   1. space-sidecar:latest       — Guardian stateful sidecar
#   2. space-workload:latest      — tinySML SAMKNN wildfire worker (was train_loop)
#   3. space-node-agent:latest    — Edge-autonomous MPC Node Agent (was gRPC agent)
#   4. space-topology-dashboard   — Floating Master ISL topology dashboard sidecar
#
# Changes from V5:
#   - PATH_AGENT updated to infrastructure/node_agent/ (gRPC agent directory removed)
#   - Added PATH_TOPO_DASH + image build for the Floating Master sidecar (§1.8)
#   - Added Dockerfile for topology dashboard
# ==============================================================================

# --- CONFIGURATION ---
NODES=(minikube-m02 minikube-m03 minikube-m04)

IMG_GUARDIAN=localhost/space-sidecar:latest
IMG_PAYLOAD=localhost/space-workload:latest
IMG_AGENT=localhost/space-node-agent:latest
IMG_TOPO_DASH=localhost/space-topology-dashboard:latest

PATH_GUARDIAN=./src/state-sidecar
PATH_PAYLOAD=./src/training-workload
PATH_AGENT=./infrastructure/node_agent        # V6: moved from grpc_agent/
PATH_TOPO_DASH=./infrastructure               # floating_master_dashboard.py lives here

TAR_GUARDIAN=sidecar-guardian.tar
TAR_PAYLOAD=payload-phoenix.tar
TAR_AGENT=node-agent.tar
TAR_TOPO_DASH=topology-dashboard.tar

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# --- VALIDATION ---
if [ ! -d "$PATH_GUARDIAN" ]; then
    echo -e "${RED}❌ ERROR: Run this script from the project root!${NC}"
    echo "   Example: ./ops/build_and_inject.sh"
    exit 1
fi

echo -e "${BLUE}>>> 🚀 SPACE CLOUD V6 — QUAD VECTOR INJECTOR <<<${NC}"

# =============================================================================
# BLOCK 1: BUILD & PACKAGE
# Compiles each module using Docker and exports as TAR archives for transport.
# =============================================================================
echo -e "\n${YELLOW}[1/3] Building images (Docker)...${NC}"

# A. Guardian sidecar (stateful, CRIU-migratable)
echo "   Building Guardian..."
docker build -t $IMG_GUARDIAN -f "$PATH_GUARDIAN/Dockerfile.sidecar" "$PATH_GUARDIAN" > /dev/null
docker save $IMG_GUARDIAN -o $TAR_GUARDIAN

# B. tinySML SAMKNN wildfire worker (was train_loop.py — §2.1)
echo "   Building tinySML Worker..."
docker build -t $IMG_PAYLOAD -f "$PATH_PAYLOAD/Dockerfile.workload" "$PATH_PAYLOAD" > /dev/null
docker save $IMG_PAYLOAD -o $TAR_PAYLOAD

# C. Node Agent (Edge-autonomous MPC — no gRPC, §4.2)
#    Build context includes ops/relay_transfer.sh via COPY in the Dockerfile
echo "   Building Node Agent..."
docker build -t $IMG_AGENT \
    -f "$PATH_AGENT/Dockerfile.agent" \
    --build-context ops=./ops \
    "$PATH_AGENT" > /dev/null
docker save $IMG_AGENT -o $TAR_AGENT

# D. Topology Dashboard sidecar for Floating Master (§1.8)
#    Uses a minimal Python slim image; floating_master_dashboard.py is the entrypoint
echo "   Building Topology Dashboard..."
cat > /tmp/Dockerfile.topo_dash <<'EOF'
FROM python:3.9-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
RUN pip install --no-cache-dir redis
COPY floating_master_dashboard.py .
CMD ["python", "floating_master_dashboard.py"]
EOF
docker build -t $IMG_TOPO_DASH -f /tmp/Dockerfile.topo_dash "$PATH_TOPO_DASH" > /dev/null
docker save $IMG_TOPO_DASH -o $TAR_TOPO_DASH

echo -e "${GREEN}✅ All four images built and packaged.${NC}"

# =============================================================================
# BLOCK 2: TRANSFER & INGESTION
# Uploads the TAR files to every satellite node and loads them into Buildah.
# =============================================================================
echo -e "\n${YELLOW}[2/3] Injecting images into satellite nodes...${NC}"

for NODE in "${NODES[@]}"; do
    echo -e "   📡 Uplink to node: ${BLUE}$NODE${NC}"

    # Transfer all four TARs to the node
    minikube cp $TAR_GUARDIAN  $NODE:/tmp/$TAR_GUARDIAN
    minikube cp $TAR_PAYLOAD   $NODE:/tmp/$TAR_PAYLOAD
    minikube cp $TAR_AGENT     $NODE:/tmp/$TAR_AGENT
    minikube cp $TAR_TOPO_DASH $NODE:/tmp/$TAR_TOPO_DASH

    # Load into the local container runtime via Buildah (bypasses registry requirement)
    minikube ssh -n $NODE "sudo buildah pull docker-archive:/tmp/$TAR_GUARDIAN"  > /dev/null 2>&1
    minikube ssh -n $NODE "sudo buildah pull docker-archive:/tmp/$TAR_PAYLOAD"   > /dev/null 2>&1
    minikube ssh -n $NODE "sudo buildah pull docker-archive:/tmp/$TAR_AGENT"     > /dev/null 2>&1
    minikube ssh -n $NODE "sudo buildah pull docker-archive:/tmp/$TAR_TOPO_DASH" > /dev/null 2>&1

    # Clean up transferred TARs to save node disk space
    minikube ssh -n $NODE "sudo rm -f /tmp/$TAR_GUARDIAN /tmp/$TAR_PAYLOAD /tmp/$TAR_AGENT /tmp/$TAR_TOPO_DASH"

    echo -e "${GREEN}      ✅ Node $NODE synchronized.${NC}"
done

# =============================================================================
# BLOCK 3: LOCAL CLEANUP
# Removes temporary TAR archives from the host machine.
# =============================================================================
echo -e "\n${YELLOW}[3/3] Post-launch cleanup...${NC}"
rm -f $TAR_GUARDIAN $TAR_PAYLOAD $TAR_AGENT $TAR_TOPO_DASH /tmp/Dockerfile.topo_dash
echo -e "${GREEN}✅ Injection complete. All systems primed for launch.${NC}"