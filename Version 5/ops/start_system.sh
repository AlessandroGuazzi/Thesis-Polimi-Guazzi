#!/bin/bash

# ==============================================================================
# SPACE CLOUD V5 - MISSION CONTROL LAUNCHER
# Avvia: Redis, Digital Twin, MPC Controller e Dashboard Tunnel
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

echo -e "${BLUE}🚀 INIZIALIZZAZIONE MISSIONE V5 (MICROSERVICES)...${NC}"

# 1. PULIZIA AUTOMATICA
cleanup() {
    echo -e "\n${RED}🛑 ABORT: Chiusura sottosistemi...${NC}"
    kill $(jobs -p) 2>/dev/null
    exit
}
trap cleanup SIGINT SIGTERM EXIT

# 2. DEPLOY MANIFESTI KUBERNETES
echo -e "${YELLOW}[1/5] Applicazione Configurazioni Orbitali (K8s)...${NC}"
kubectl apply -f k8s/service-redis.yaml
kubectl apply -f k8s/service-dashboard.yaml
# Nota: Il pod dual-container viene lanciato per ultimo o manualmente se preferisci
kubectl apply -f k8s/pod-dual-container.yaml
echo -e "${GREEN}   -> Manifesti applicati.${NC}"

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
        # Punta al servizio definito in k8s/service-dashboard.yaml
        kubectl port-forward svc/space-dashboard-svc 8080:80 >/dev/null 2>&1
        sleep 1
    done
) &

echo -e "\n${GREEN}✅ SISTEMA V5 OPERATIVO!${NC}"
echo "---------------------------------------------------"
echo -e "🖥️  Cockpit: http://localhost:8080"
echo -e "🛡️  Guardian:  Ready (Stateful Sidecar)"
echo -e "🔥 Payload:   Ready (Stateless Worker)"
echo "---------------------------------------------------"

# Mantieni vivo lo script
wait $SCHED_PID