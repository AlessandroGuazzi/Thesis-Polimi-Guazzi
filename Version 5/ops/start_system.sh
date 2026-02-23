#!/bin/bash

# ==============================================================================
# SPACE CLOUD V5.2 - MISSION CONTROL LAUNCHER (P2P EDITION)
# Avvia: Redis, Agenti di Nodo, Dashboard e i Cervelli della missione
# Opzione: Protocollo TABULA RASA per ripartire da zero.
# ==============================================================================

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Check posizione esecuzione
if [ ! -d "./infrastructure" ]; then
    echo -e "${RED}❌ Errore: Esegui dalla root del progetto!${NC}"
    exit 1
fi

echo -e "${BLUE}>>> 🚀 INIZIALIZZAZIONE MISSIONE V5.2 (PEER-TO-PEER) <<<${NC}"

# ==============================================================================
# PROTOCOLLO TABULA RASA (Opzionale)
# ==============================================================================
read -p "❓ Desideri eseguire una TABULA RASA (pulizia completa) prima di iniziare? (s/N): " choice
if [[ "$choice" =~ ^([sS][ìÌyY])$ ]]; then
    echo -e "${RED}🧹 Esecuzione Tabula Rasa in corso...${NC}"
    # Rimuove tutti i componenti della missione
    kubectl delete deployment space-mission --ignore-not-found=true
    kubectl delete daemonset space-node-agent --ignore-not-found=true
    kubectl delete deployment system-redis --ignore-not-found=true
    kubectl delete service space-dashboard-svc system-redis --ignore-not-found=true
    kubectl delete ingress space-ingress --ignore-not-found=true
    # Rimuove l'immagine 'restored' dai nodi per forzare un avvio pulito
    NODES=("minikube-m02" "minikube-m03" "minikube-m04")
    for NODE in "${NODES[@]}"; do
        echo -e "   ✨ Pulizia immagini residue su $NODE..."
        minikube ssh -n $NODE "sudo buildah rmi localhost/space-sidecar:restored" >/dev/null 2>&1
    done
    echo -e "${GREEN}✅ Cluster pulito. Pronto per il decollo.${NC}\n"
    sleep 2
fi

# 1. PULIZIA AUTOMATICA (SIGINT)
cleanup() {
    echo -e "\n${RED}🛑 ABORT: Chiusura sottosistemi locali...${NC}"
    kill $(jobs -p) 2>/dev/null
    exit
}
trap cleanup SIGINT SIGTERM EXIT

# 2. DEPLOY MANIFESTI KUBERNETES
echo -e "${YELLOW}[1/5] Applicazione Configurazioni Orbitali (K8s)...${NC}"
kubectl apply -f k8s/service-redis.yaml
kubectl apply -f k8s/service-dashboard.yaml
kubectl apply -f infrastructure/daemonset-agent.yaml
kubectl apply -f k8s/pod-dual-container.yaml
echo -e "${GREEN}   -> Manifesti applicati (incluso P2P Agent).${NC}"

# 3. ATTESA REDIS & TUNNEL
echo -e "${YELLOW}[2/5] Attesa Bus Dati (Redis)...${NC}"
kubectl wait --for=condition=ready pod -l app=system-redis --timeout=60s >/dev/null
echo -e "${GREEN}   -> Redis Pod Ready.${NC}"

echo -e "${YELLOW}[3/5] Apertura Uplink Dati (Port 6379)...${NC}"
kubectl port-forward svc/system-redis 6379:6379 >/dev/null 2>&1 &
sleep 3
echo -e "${GREEN}   -> Uplink Stabilito.${NC}"

# 4. AVVIO SIMULAZIONE (DIGITAL TWIN)
echo -e "${YELLOW}[4/5] Avvio Digital Twin (Environment Sim)...${NC}"
if [ -f "infrastructure/environment_sim.py" ]; then
    python3 infrastructure/environment_sim.py > /dev/null 2>&1 &
    echo -e "${GREEN}   -> Simulatore: ONLINE (PID: $!)${NC}"
else
    echo -e "${RED}⚠️  File environment_sim.py non trovato!${NC}"
fi

# 5. AVVIO CONTROLLER (MPC SCHEDULER)
echo -e "${YELLOW}[5/5] Avvio MPC Controller (Brain)...${NC}"
if [ -f "infrastructure/mpc_controller.py" ]; then
    python3 infrastructure/mpc_controller.py &
    SCHED_PID=$!
    echo -e "${GREEN}   -> Controller: ONLINE (PID: $SCHED_PID)${NC}"
else
    echo -e "${RED}⚠️  File mpc_controller.py non trovato!${NC}"
fi

# 6. TUNNEL DASHBOARD
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
echo -e "🕵️  P2P Agent: Ready (DaemonSet gRPC)"
echo -e "🛡️  Guardian:   Ready (Stateful Sidecar)"
echo "---------------------------------------------------"

wait $SCHED_PID