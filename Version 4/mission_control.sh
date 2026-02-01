#!/bin/bash

# ============================================================
# 🎮 SPACE MISSION CONTROL CENTER V4.5 (Full Autonomy)
# Avvia: Redis Tunnel + Physics Engine + Scheduler IA + Dashboard
# ============================================================

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}🚀 INIZIALIZZAZIONE MISSION CONTROL V4.5...${NC}"

# Check prerequisiti
if ! kubectl get svc system-redis >/dev/null 2>&1; then
    echo -e "${RED}❌ ERRORE: Il servizio 'system-redis' non esiste.${NC}"
    echo "   Esegui: kubectl apply -f system-redis.yaml"
    exit 1
fi

# --- 1. FUNZIONE DI PULIZIA ---
cleanup() {
    echo -e "\n${RED}🛑 SHUTDOWN: Chiusura di tutti i sistemi...${NC}"
    # Uccide tutti i processi avviati in background (Redis, Python, etc.)
    kill $(jobs -p) 2>/dev/null
    wait $(jobs -p) 2>/dev/null
    echo -e "${GREEN}✅ Missione terminata. A presto Comandante.${NC}"
    exit
}
trap cleanup SIGINT SIGTERM EXIT

# --- 2. AVVIO TUNNEL REDIS ---
echo -e "${YELLOW}[1/4] Apertura canale dati Redis (Port 6379)...${NC}"
kubectl port-forward svc/system-redis 6379:6379 >/dev/null 2>&1 &
REDIS_PID=$!
sleep 3

if ! ps -p $REDIS_PID > /dev/null; then
    echo -e "${RED}❌ Errore critico: Tunnel Redis fallito.${NC}"
    exit 1
fi
echo -e "${GREEN}   -> Redis Link: STABILITO${NC}"

# --- 3. AVVIO MOTORE FISICO ---
echo -e "${YELLOW}[2/4] Avvio Motore Fisico (physics_sim.py)...${NC}"
if [ ! -f "physics_sim.py" ]; then
    echo -e "${RED}❌ File physics_sim.py non trovato!${NC}"
    cleanup
fi

# Avviamo il simulatore
python3 physics_sim.py > /dev/null 2>&1 &
PHYSICS_PID=$!
echo -e "${GREEN}   -> Physics Engine: IN ORBITA (PID: $PHYSICS_PID)${NC}"

# --- 4. AVVIO SCHEDULER MPC (IL NUOVO CERVELLO) ---
echo -e "${YELLOW}[3/4] Avvio Scheduler IA (space_scheduler.py)...${NC}"
if [ ! -f "space_scheduler.py" ]; then
    echo -e "${RED}❌ File space_scheduler.py non trovato!${NC}"
    cleanup
fi

# Avviamo lo scheduler. Lo lasciamo stampare a video così vedi le decisioni!
python3 space_scheduler.py &
SCHEDULER_PID=$!
echo -e "${GREEN}   -> Scheduler Brain: ONLINE (PID: $SCHEDULER_PID)${NC}"

# --- 5. AVVIO DASHBOARD ---
echo -e "${YELLOW}[4/4] Connessione Dashboard (Port 8080)...${NC}"
echo -e "${BLUE}ℹ️  Il sistema gestirà le riconnessioni durante la migrazione.${NC}"

(
    while true; do
        # Cerchiamo il nome del pod attivo dinamicamente
        POD_NAME=$(kubectl get pod -l app=space-mission -o jsonpath="{.items[0].metadata.name}" 2>/dev/null)

        if [ ! -z "$POD_NAME" ]; then
            # Tenta connessione (blocca finché non cade)
            kubectl port-forward $POD_NAME 8080:80 >/dev/null 2>&1
        fi

        # Se arriviamo qui, il tunnel è caduto. Riprova tra 2 secondi.
        sleep 2
    done
) &

echo -e "\n${GREEN}✅ TUTTI I SISTEMI OPERATIVI!${NC}"
echo "---------------------------------------------------"
echo -e "🖥️  Dashboard: http://localhost:8080"
echo -e "🧠 IA:        Lo scheduler sta monitorando la telemetria..."
echo -e "🔥 Test:      Aspetta che il nodo attivo entri in ECLISSI o si surriscaldi."
echo "---------------------------------------------------"

# Mantieni vivo lo script e attendi che lo scheduler finisca (o CTRL+C)
wait $SCHEDULER_PID