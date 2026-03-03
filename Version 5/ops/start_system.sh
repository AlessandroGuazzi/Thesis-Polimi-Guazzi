#!/bin/bash

# ==============================================================================
# SPACE CLOUD V5.2 - MISSION CONTROL LAUNCHER (P2P EDITION)
# ==============================================================================

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

if [ ! -d "./infrastructure" ]; then
    echo -e "${RED}❌ Errore: Esegui dalla root del progetto!${NC}"
    exit 1
fi

echo -e "${BLUE}>>> 🚀 INIZIALIZZAZIONE MISSIONE V5.2 (PEER-TO-PEER) <<<${NC}"

# --- BLOCK 1: TABULA RASA PROTOCOL ---
# Optional full cleanup of previous deployments and stale images to ensure a fresh environment.
read -p "❓ Desideri eseguire una TABULA RASA (pulizia completa) prima di iniziare? (s/N): " choice
if [[ "$choice" =~ ^([sS][ìÌyY])$ ]]; then
    echo -e "${RED}🧹 Esecuzione Tabula Rasa in corso...${NC}"
    kubectl delete deployment space-mission --ignore-not-found=true
    kubectl delete daemonset space-node-agent --ignore-not-found=true
    kubectl delete deployment system-redis --ignore-not-found=true
    kubectl delete service space-dashboard-svc system-redis --ignore-not-found=true
    kubectl delete ingress space-ingress --ignore-not-found=true

    NODES=("minikube-m02" "minikube-m03" "minikube-m04")
    for NODE in "${NODES[@]}"; do
        minikube ssh -n $NODE "sudo buildah rmi localhost/space-sidecar:restored" >/dev/null 2>&1
    done
    echo -e "${GREEN}✅ Cluster pulito. Pronto per il decollo.${NC}\n"
    sleep 2
fi

# --- BLOCK 2: CLEANUP HANDLER ---
# Ensures background processes (simulator, controller) are killed when the script is stopped.
cleanup() {
    echo -e "\n${RED}🛑 ABORT: Chiusura sottosistemi locali...${NC}"
    kill $(jobs -p) 2>/dev/null
    exit
}
trap cleanup SIGINT SIGTERM EXIT

# --- BLOCK 3: K8S DEPLOYMENT ---
# Applies the core manifest files to create the Redis bus, Node Agents, and the Dual-Container Pod.
echo -e "${YELLOW}[1/5] Applicazione Configurazioni Orbitali (K8s)...${NC}"
kubectl apply -f k8s/service-redis.yaml
kubectl apply -f k8s/service-dashboard.yaml
kubectl apply -f infrastructure/daemonset-agent.yaml
kubectl apply -f k8s/pod-dual-container.yaml

# --- BLOCK 4: DATA BUS & UPLINK ---
# Waits for Redis to be ready and establishes a port-forward for local monitoring tools.
echo -e "${YELLOW}[2/5] Attesa Bus Dati (Redis)...${NC}"
kubectl wait --for=condition=ready pod -l app=system-redis --timeout=60s >/dev/null

echo -e "${YELLOW}[3/5] Apertura Uplink Dati (Port 6379)...${NC}"
kubectl port-forward svc/system-redis 6379:6379 >/dev/null 2>&1 &
sleep 3

# --- BLOCK 5: DIGITAL TWIN & CONTROLLER ---
# Starts the orbital physics engine and the MPC predictive scheduler in the background.
echo -e "${YELLOW}[4/5] Avvio Digital Twin (Environment Sim)...${NC}"
python3 infrastructure/environment_sim.py > /dev/null 2>&1 &

echo -e "${YELLOW}[5/5] Avvio MPC Controller (Brain)...${NC}"
python3 infrastructure/mpc_controller.py &
SCHED_PID=$!

# --- BLOCK 6: DASHBOARD TUNNEL ---
# Establishes a persistent tunnel to expose the web dashboard on the local host.
echo -e "\n${BLUE}ℹ️  Stabilizzazione Link Dashboard (Port 8080)...${NC}"
(
    while true; do
        kubectl port-forward svc/space-dashboard-svc 8080:80 >/dev/null 2>&1
        sleep 1
    done
) &

echo -e "\n${GREEN}✅ SISTEMA V5 OPERATIVO!${NC}"
echo "---------------------------------------------------"
echo -e "🖥️  Cockpit:  http://localhost:8080"
echo "---------------------------------------------------"

wait $SCHED_PID