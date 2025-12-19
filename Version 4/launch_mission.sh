#!/bin/bash

# ================================================================
# SPACE CLOUD V3.0 - LAUNCH CONTROL CENTER (LINUX/UBUNTU EDITION)
# ================================================================
# Architecture: Sidecar Pattern + MPC Scheduler + System Bus
# Networking: NGINX Ingress Controller (HTTP Port 80)
# Persistence: CRIU Simulation
# ================================================================

# Definizione Colori per Output
CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
GRAY='\033[0;90m'
NC='\033[0m' # No Color

# Imposta la directory di lavoro corrente
cd "$(dirname "$0")"

echo -e "${CYAN}>>> SPACE CLOUD MISSION V4.0: INITIALIZING...${NC}"
echo -e "${GRAY}    Mode: INGRESS ROUTING ACTIVATED (Linux Native)${NC}"

# ----------------------------------------------------------------
# FASE 1: INFRASTRUTTURA & CLUSTER
# ----------------------------------------------------------------
CLUSTER_NAME="space-cloud"

if ! kind get clusters | grep -q "^$CLUSTER_NAME$"; then
    echo -e "${RED}   [INFRA] Cluster non trovato. Esegui la configurazione manuale FASE 1!${NC}"
    exit 1
else
    echo -e "${GREEN}   [INFRA] Cluster '$CLUSTER_NAME' attivo.${NC}"
fi

# ----------------------------------------------------------------
# FASE 2: BUILD & DEPLOY
# ----------------------------------------------------------------
echo -e "${YELLOW}   [DEPLOY] Applicazione Manifesti...${NC}"

# 1. System Redis (Il Bus Dati)
if [ -f "system-redis.yaml" ]; then
    kubectl apply -f system-redis.yaml
fi

# 2. Mission Pod (L'Applicazione)
if [ -f "space-mission.yaml" ]; then
    # Force delete per simulare un lancio pulito (silenzioso sugli errori)
    kubectl delete -f space-mission.yaml --ignore-not-found=true > /dev/null 2>&1
    sleep 1
    kubectl apply -f space-mission.yaml
fi

# 3. Ingress Rules (Il Cartello Stradale)
if [ -f "space-ingress.yaml" ]; then
    echo -e "${CYAN}   [NET] Aggiornamento Regole di Navigazione (Ingress)...${NC}"
    kubectl apply -f space-ingress.yaml
fi

# ----------------------------------------------------------------
# FASE 3: RETE E TUNNELS (HYBRID MODE)
# ----------------------------------------------------------------
echo -e "${CYAN}   [NETWORK] Stabilizzazione Uplink...${NC}"

# A. SYSTEM BUS TUNNEL
# Apriamo un nuovo terminale per il Port-Forwarding persistente
# Usiamo 'gnome-terminal' per creare una finestra separata
gnome-terminal --title="SYSTEM BUS (Telemetry Link)" -- bash -c "
    echo -e '${CYAN}Target: svc/system-redis (Internal Only)${NC}';
    while true; do
        kubectl port-forward svc/system-redis 6379:6379;
        echo -e '${YELLOW}Connection lost. Reconnecting to System Bus...${NC}';
        sleep 2;
    done;
    exec bash"

# B. UI LINK
echo -e "${GRAY}   [INFO] UI Port-Forwarding disabilitato. Traffico gestito da NGINX.${NC}"

# Attendiamo che il tunnel Redis sia su
echo -e "${GRAY}   [WAIT] Calibrazione Sistemi (5s)...${NC}"
sleep 5

# ----------------------------------------------------------------
# FASE 4: INTELLIGENZA ARTIFICIALE & FISICA
# ----------------------------------------------------------------
echo -e "${GREEN}   [LAUNCH] Avvio Motori di Calcolo...${NC}"

# C. MOTORE FISICO
gnome-terminal --title="PHYSICS ENGINE" -- bash -c "
    python3 physics_sim.py;
    echo -e '${RED}Physics Engine Terminated.${NC}';
    read -p 'Press Enter to close...';
    exec bash"

# D. SCHEDULER MPC (CRIU ENABLED)
gnome-terminal --title="MPC SCHEDULER (CRIU)" -- bash -c "
    python3 mpc_scheduler.py;
    echo -e '${RED}Scheduler Terminated.${NC}';
    read -p 'Press Enter to close...';
    exec bash"

echo -e "\n${GREEN}>>> MISSION LAUNCHED SUCCESSFULLY.${NC}"
echo -e "--------------------------------------------------------"
echo -e "${CYAN}   DASHBOARD LINK:  http://mission-control.local${NC}"
echo -e "--------------------------------------------------------"
echo -e "   1. Apri il link nel browser."
echo -e "   2. Monitora la finestra 'MPC SCHEDULER' per vedere la migrazione."
echo -e "   3. Goditi la continuità del servizio."
echo -e "--------------------------------------------------------"