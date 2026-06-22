#!/bin/bash

# ==============================================================================
# SPACE CLOUD V7.1 - MISSION CONTROL LAUNCHER
# Changes from V6:
#   - Payload ResNet-18 Early Fusion (Stateless) + Guardian Sidecar (Stateful)
#   - Removed inline space-udp-uplink (moved cleanly to service-dashboard.yaml)
#   - Fixed data_streamer.py path to point to 3_ground_station/
# ==============================================================================

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# =============================================================================
# CAMPAIGN MODE DETECTION (Phase 2, Step 2.4)
# Pass --campaign to activate deterministic evaluation mode:
#   - Exports CAMPAIGN_MODE=True for local Python processes
#     (environment_sim.py, data_streamer.py will self-abort on startup)
#   - Patches the K8s payload-phoenix container to set CAMPAIGN_MODE=True
#     (tinysml_worker.py will idle with full ONNX memory footprint)
# =============================================================================
CAMPAIGN_MODE_FLAG="False"
for arg in "$@"; do
    if [ "$arg" = "--campaign" ]; then
        CAMPAIGN_MODE_FLAG="True"
    fi
done

if [ "$CAMPAIGN_MODE_FLAG" = "True" ]; then
    export CAMPAIGN_MODE="True"
    echo -e "${YELLOW}🧪 CAMPAIGN MODE ACTIVE — Stochastic subsystems will be deactivated.${NC}"
    echo -e "${YELLOW}   • environment_sim.py → Ghost Publisher takes control${NC}"
    echo -e "${YELLOW}   • data_streamer.py   → Ghost Worker takes control${NC}"
    echo -e "${YELLOW}   • tinysml_worker.py  → Idle with locked memory footprint${NC}"
fi

# Confirm the script is run from the project root (2_orbit_payload)
if [ ! -d "./infrastructure" ]; then
    echo -e "${RED}❌ Error: Run this script from the project root (2_orbit_payload)!${NC}"
    exit 1
fi

echo -e "${BLUE}>>> 🚀 SPACE CLOUD V7.1 — MISSION CONTROL LAUNCHER <<<${NC}"

# =============================================================================
# BLOCK 1.5: REUSABLE CLEANUP LOGIC
# =============================================================================
do_cleanup() {
    kubectl delete deployment space-mission topology-master ground-redis --ignore-not-found=true
    kubectl delete daemonset  space-node-agent --ignore-not-found=true
    kubectl delete service    ground-redis topology-master topology-dashboard \
                              space-dashboard-svc space-telemetry-udp space-udp-uplink --ignore-not-found=true

    # Clean restored images from satellite nodes
    for NODE in minikube-m02 minikube-m03 minikube-m04; do
        minikube ssh -n $NODE "sudo buildah rmi localhost/space-sidecar:restored" > /dev/null 2>&1 || true
    done
}

# =============================================================================
# BLOCK 1: TABULA RASA (optional full cleanup)
# Removes all previous deployments and stale images for a clean start.
# =============================================================================
read -p "❓ Run TABULA RASA (full cluster cleanup) before starting? (y/N): " choice
if [[ "$choice" =~ ^([yY])$ ]]; then
    echo -e "${RED}🧹 Running Tabula Rasa...${NC}"
    do_cleanup
    echo -e "${GREEN}✅ Cluster clean. Ready for launch.${NC}\n"
    sleep 10
fi

# =============================================================================
# BLOCK 2: SIGNAL HANDLER & PRE-FLIGHT CLEANUP
# =============================================================================
trap_cleanup() {
    echo -e "\n${RED}🛑 ABORT: Shutting down local subsystems...${NC}"
    kill $(jobs -p) 2>/dev/null
    pkill -f -9 "global_dashboard.py|data_streamer.py|environment_sim.py" 2>/dev/null || true
    do_cleanup
    exit
}
trap trap_cleanup SIGINT SIGTERM EXIT

# Pre-flight cleanup: Terminate any orphan ground-station python processes from previous runs
pkill -f -9 "global_dashboard.py|data_streamer.py|environment_sim.py" 2>/dev/null || true

# =============================================================================
# BLOCK 3: KUBERNETES DEPLOYMENT
# =============================================================================
echo -e "${YELLOW}[1/6] Deploying K8s manifests...${NC}"

# Ground Redis — must come first
kubectl apply -f k8s/service-redis.yaml

# Floating Master (topology-redis + topology-dashboard sidecar) — on satellite node
kubectl apply -f k8s/floating-master.yaml

# Dashboard K8s Service + UDP NodePort for Telemetry (V7.1)
kubectl apply -f k8s/service-dashboard.yaml

# Node Agent DaemonSet — one agent per satellite
kubectl apply -f infrastructure/daemonset-agent.yaml

# Mission Pod — Guardian sidecar + tinySML Phoenix
kubectl apply -f k8s/pod-dual-container.yaml

# Campaign Mode K8s Injection (Phase 2, Step 2.4)
# If --campaign was passed, patch the payload-phoenix container's CAMPAIGN_MODE
# env var from "False" to "True" so the worker enters its idle gate in-cluster.
if [ "$CAMPAIGN_MODE_FLAG" = "True" ]; then
    echo -e "${YELLOW}🧪 Patching space-mission deployment for Campaign Mode...${NC}"
    kubectl set env deployment/space-mission CAMPAIGN_MODE=True -c payload-phoenix
fi

# =============================================================================
# BLOCK 4: WAIT FOR REDIS BUSES TO BE READY
# =============================================================================
echo -e "${YELLOW}[2/6] Waiting for Ground Redis...${NC}"
kubectl wait --for=condition=ready pod -l app=ground-redis --timeout=90s > /dev/null

echo -e "${YELLOW}[3/6] Waiting for Floating Master...${NC}"
kubectl wait --for=condition=ready pod -l app=topology-master --timeout=90s > /dev/null

# =============================================================================
# BLOCK 5: PORT FORWARDS
# =============================================================================
echo -e "${YELLOW}[4/6] Opening communication uplinks...${NC}"

# Ground Redis telemetry bus
(
    while true; do
        kubectl port-forward svc/ground-redis 6379:6379 > /dev/null 2>&1
        sleep 1
    done
) &

# Floating Master topology store
(
    while true; do
        kubectl port-forward svc/topology-master 6380:6379 > /dev/null 2>&1
        sleep 1
    done
) &

# Guardian SML Payload Dashboard
(
    while true; do
        kubectl port-forward svc/space-dashboard-svc 8080:80 > /dev/null 2>&1
        sleep 1
    done
) &

# ISL Topology Dashboard
(
    while true; do
        kubectl port-forward svc/topology-dashboard 8081:8081 > /dev/null 2>&1
        sleep 1
    done
) &

# =============================================================================
# BLOCK 6: LOCAL GROUND STATION PROCESSES
# =============================================================================
echo -e "${YELLOW}[5/6] Launching Digital Twin (Environment Simulator)...${NC}"
python3 infrastructure/environment_sim.py > /dev/null 2>&1 &
sleep 2

echo -e "${YELLOW}[6/6] Launching UDP Data Streamer (Ground Station V7.1)...${NC}"
# Avvia la Ground Station aggiornata puntando alla cartella corretta 3_ground_station
python3 ../3_ground_station/data_streamer.py &

echo -e "${GREEN}[+] Launching Global Swarm Dashboard (port 8090)...${NC}"
python3 infrastructure/global_dashboard.py &
DASHBOARD_PID=$!

# =============================================================================
# READY
# =============================================================================
echo -e "\n${GREEN}✅ SPACE CLOUD V7.1 OPERATIONAL!${NC}"
echo "─────────────────────────────────────────────────────"
echo -e "🖥️  SML Payload Dashboard:      http://localhost:8080   (Guardian SSE, follows migrations)"
echo -e "🌍  Global Swarm Dashboard:      http://localhost:8090   (Ground Station overview)"
echo -e "🌐  ISL Topology Dashboard:      http://localhost:8081   (Floating Master SSE)"
echo -e "📡  Ground Redis (debug):         redis-cli -p 6379"
echo -e "🗺️  Topology Redis (debug):       redis-cli -p 6380"
echo "─────────────────────────────────────────────────────"

wait $DASHBOARD_PID