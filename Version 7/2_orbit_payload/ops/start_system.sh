#!/bin/bash

# ==============================================================================
# SPACE CLOUD V6 - MISSION CONTROL LAUNCHER
# Changes from V5:
#   - MPC Controller REMOVED (mpc_controller.py is deleted — §4.1)
#   - Dual Redis: ground-redis (telemetry) + topology-master (Dijkstra)
#   - floating-master.yaml added to K8s deployment block
#   - Data Streamer launched (infrastructure/data_streamer.py)
#   - Global Swarm Dashboard launched (infrastructure/global_dashboard.py)
#   - Port-forward: ground-redis on 6379, topology-master on 6380 (debug)
#   - Dashboard tunnel updated to health-check loop for persistence
#   - ADDED: UDP NodePort Service for Ground-to-Space Data Streaming
# ==============================================================================

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Confirm the script is run from the project root
if [ ! -d "./infrastructure" ]; then
    echo -e "${RED}❌ Error: Run this script from the project root!${NC}"
    exit 1
fi

echo -e "${BLUE}>>> 🚀 SPACE CLOUD V6 — MISSION CONTROL LAUNCHER <<<${NC}"

# =============================================================================
# BLOCK 1: TABULA RASA (optional full cleanup)
# Removes all previous deployments and stale images for a clean start.
# =============================================================================
read -p "❓ Run TABULA RASA (full cluster cleanup) before starting? (y/N): " choice
if [[ "$choice" =~ ^([yY])$ ]]; then
    echo -e "${RED}🧹 Running Tabula Rasa...${NC}"

    # Delete V6 workloads
    kubectl delete deployment space-mission topology-master ground-redis --ignore-not-found=true
    kubectl delete daemonset  space-node-agent --ignore-not-found=true
    kubectl delete service    ground-redis topology-master topology-dashboard \
                              space-dashboard-svc space-udp-uplink --ignore-not-found=true

    # Clean restored images from satellite nodes
    for NODE in minikube-m02 minikube-m03 minikube-m04; do
        minikube ssh -n $NODE "sudo buildah rmi localhost/space-sidecar:restored" > /dev/null 2>&1 || true
        minikube ssh -n $NODE "sudo buildah rmi localhost/space-topology-dashboard:latest" > /dev/null 2>&1 || true
    done

    echo -e "${GREEN}✅ Cluster clean. Ready for launch.${NC}\n"
    sleep 2
fi

# =============================================================================
# BLOCK 2: SIGNAL HANDLER & PRE-FLIGHT CLEANUP
# Kills all background processes (simulator, streamer, dashboards) on Ctrl+C.
# =============================================================================
cleanup() {
    echo -e "\n${RED}🛑 ABORT: Shutting down local subsystems...${NC}"
    kill $(jobs -p) 2>/dev/null
    pkill -f -9 "global_dashboard.py|data_streamer.py|environment_sim.py" 2>/dev/null || true
    kubectl delete deployment space-mission topology-master ground-redis --ignore-not-found=true
    kubectl delete daemonset  space-node-agent --ignore-not-found=true
    kubectl delete service    ground-redis topology-master topology-dashboard \
                              space-dashboard-svc space-udp-uplink --ignore-not-found=true
    exit
}
trap cleanup SIGINT SIGTERM EXIT

# Pre-flight cleanup: Terminate any orphan ground-station python processes from previous runs
pkill -f -9 "global_dashboard.py|data_streamer.py|environment_sim.py" 2>/dev/null || true

# =============================================================================
# BLOCK 3: KUBERNETES DEPLOYMENT
# Deploys all manifests in the correct dependency order:
#   1. ground-redis first (other pods need it running)
#   2. Floating Master (topology engine + dashboard sidecar)
#   3. Space UDP Uplink (NodePort for Data Streamer)
#   4. Node Agent DaemonSet
#   5. Mission Pod (Guardian + tinySML Worker)
#   6. Dashboard Service (for the Guardian's SSE UI port-forward)
# =============================================================================
echo -e "${YELLOW}[1/6] Deploying K8s manifests...${NC}"

# Ground Redis — must come first
kubectl apply -f k8s/service-redis.yaml

# Floating Master (topology-redis + topology-dashboard sidecar) — on satellite node
kubectl apply -f k8s/floating-master.yaml

# THE SPACE POST OFFICE (UDP NodePort for Data Streaming)
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Service
metadata:
  name: space-udp-uplink
spec:
  type: NodePort
  selector:
    app: space-mission
  ports:
    - protocol: UDP
      port: 5005
      targetPort: 5005
      nodePort: 32005
EOF

# Dashboard K8s Service (port 80 → NodePort for Guardian dashboard)
kubectl apply -f k8s/service-dashboard.yaml

# Node Agent DaemonSet — one agent per satellite
kubectl apply -f infrastructure/daemonset-agent.yaml

# Mission Pod — Guardian sidecar + tinySML Phoenix
kubectl apply -f k8s/pod-dual-container.yaml

# =============================================================================
# BLOCK 4: WAIT FOR REDIS BUSES TO BE READY
# =============================================================================
echo -e "${YELLOW}[2/6] Waiting for Ground Redis...${NC}"
kubectl wait --for=condition=ready pod -l app=ground-redis --timeout=90s > /dev/null

echo -e "${YELLOW}[3/6] Waiting for Floating Master...${NC}"
kubectl wait --for=condition=ready pod -l app=topology-master --timeout=90s > /dev/null

# =============================================================================
# BLOCK 5: PORT FORWARDS
# Opens host-accessible tunnels for monitoring tools and the Redis debug port.
# =============================================================================
echo -e "${YELLOW}[4/6] Opening communication uplinks...${NC}"

# Ground Redis telemetry bus (port 6379 → accessible as redis-cli -p 6379)
kubectl port-forward svc/ground-redis 6379:6379 > /dev/null 2>&1 &
sleep 1

# Floating Master topology store (port 6380 → inspect graph with redis-cli -p 6380)
# Useful for debugging: redis-cli -p 6380 HGETALL node:minikube-m02
kubectl port-forward svc/topology-master 6380:6379 > /dev/null 2>&1 &
sleep 1

# Guardian SML Payload Dashboard (persistent — auto-reconnects during migrations)
(
    while true; do
        kubectl port-forward svc/space-dashboard-svc 8080:80 > /dev/null 2>&1
        sleep 1
    done
) &

# ISL Topology Dashboard (persistent — port 8081)
(
    while true; do
        kubectl port-forward svc/topology-dashboard 8081:8081 > /dev/null 2>&1
        sleep 1
    done
) &

# =============================================================================
# BLOCK 6: LOCAL GROUND STATION PROCESSES
# Launches the Digital Twin, Data Streamer, and Global Dashboard on the host.
# =============================================================================
echo -e "${YELLOW}[5/6] Launching Digital Twin (Environment Simulator)...${NC}"
# Reads K8s pod placement to set is_working flags and publishes orbital physics
# on telemetry/<node_name> channels to ground-redis.
python3 infrastructure/environment_sim.py > /dev/null 2>&1 &
sleep 2

echo -e "${YELLOW}[6/6] Launching UDP Data Streamer (Wildfire Dataset)...${NC}"
# Reads Kaggle wildfire JSON samples and streams 64x64 frames to the active pod
# via UDP:5005. Fires into the void during migrations — UDP handles this gracefully.
# Note: We NO LONGER silence the output so we can see the data flowing!
python3 infrastructure/data_streamer.py &

echo -e "${GREEN}[+] Launching Global Swarm Dashboard (port 8090)...${NC}"
# Stateless "God's Eye" operator dashboard — subscribes to telemetry/* on ground-redis
# and renders the orbital health view + migration timeline.
python3 infrastructure/global_dashboard.py &
DASHBOARD_PID=$!

# =============================================================================
# READY
# =============================================================================
echo -e "\n${GREEN}✅ SPACE CLOUD V6 OPERATIONAL!${NC}"
echo "─────────────────────────────────────────────────────"
echo -e "🖥️  SML Payload Dashboard:      http://localhost:8080   (Guardian SSE, follows migrations)"
echo -e "🌍  Global Swarm Dashboard:      http://localhost:8090   (Ground Station overview)"
echo -e "🌐  ISL Topology Dashboard:      http://localhost:8081   (Floating Master SSE)"
echo -e "📡  Ground Redis (debug):         redis-cli -p 6379"
echo -e "🗺️  Topology Redis (debug):       redis-cli -p 6380"
echo "─────────────────────────────────────────────────────"

# Keep alive until the global dashboard exits or user presses Ctrl+C
wait $DASHBOARD_PID