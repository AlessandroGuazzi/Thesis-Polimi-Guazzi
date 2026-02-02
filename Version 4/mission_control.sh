#!/bin/bash

# ============================================================
# 🎮 SPACE MISSION CONTROL CENTER V4.6 (Stable Dashboard)
# Avvia: Redis Tunnel + Physics Engine + Scheduler IA + Dashboard
# ============================================================

# --- COLORI PER OUTPUT ---
# Codici colore ANSI per rendere i messaggi più chiari a video
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'  # reset colore

# Messaggio di avvio sistema
echo -e "${BLUE}🚀 INIZIALIZZAZIONE MISSION CONTROL V4.6...${NC}"

# --- CHECK PREREQUISITI ---
# Verifica che il servizio Redis esista nel cluster Kubernetes
if ! kubectl get svc system-redis >/dev/null 2>&1; then
    echo -e "${RED}❌ ERRORE: Il servizio 'system-redis' non esiste.${NC}"
    echo "   Esegui: kubectl apply -f system-redis.yaml"
    exit 1
fi

# --- 1. FUNZIONE DI PULIZIA ---
# Funzione chiamata in uscita o con CTRL+C per chiudere tutto in modo ordinato
cleanup() {
    echo -e "\n${RED}🛑 SHUTDOWN: Chiusura di tutti i sistemi...${NC}"
    # Termina tutti i processi lanciati in background da questo script
    kill $(jobs -p) 2>/dev/null
    # Attende la chiusura effettiva dei processi
    wait $(jobs -p) 2>/dev/null
    echo -e "${GREEN}✅ Missione terminata. A presto Comandante.${NC}"
    exit
}

# Registra cleanup come handler su exit / CTRL+C / kill
trap cleanup SIGINT SIGTERM EXIT

# --- 2. AVVIO TUNNEL REDIS ---
# Apre port-forward locale verso il servizio Redis del cluster
echo -e "${YELLOW}[1/4] Apertura canale dati Redis (Port 6379)...${NC}"
kubectl port-forward svc/system-redis 6379:6379 >/dev/null 2>&1 &

# Salva PID del processo di port-forward
REDIS_PID=$!
sleep 3  # attende stabilizzazione tunnel

# Verifica che il processo sia realmente attivo
if ! ps -p $REDIS_PID > /dev/null; then
    echo -e "${RED}❌ Errore critico: Tunnel Redis fallito.${NC}"
    exit 1
fi
echo -e "${GREEN}   -> Redis Link: STABILITO${NC}"

# --- 3. AVVIO MOTORE FISICO ---
# Avvia il simulatore fisico della costellazione
echo -e "${YELLOW}[2/4] Avvio Motore Fisico (physics_sim.py)...${NC}"

# Controlla che il file esista
if [ ! -f "physics_sim.py" ]; then
    echo -e "${RED}❌ File physics_sim.py non trovato!${NC}"
    cleanup
fi

# Avvia il simulatore in background silenziando l’output
python3 physics_sim.py > /dev/null 2>&1 &

# Salva PID del physics engine
PHYSICS_PID=$!
echo -e "${GREEN}   -> Physics Engine: IN ORBITA (PID: $PHYSICS_PID)${NC}"

# --- 4. AVVIO SCHEDULER MPC (IL NUOVO CERVELLO) ---
# Avvia lo scheduler intelligente che decide le migrazioni
echo -e "${YELLOW}[3/4] Avvio Scheduler IA (space_scheduler.py)...${NC}"

# Verifica presenza file scheduler
if [ ! -f "space_scheduler.py" ]; then
    echo -e "${RED}❌ File space_scheduler.py non trovato!${NC}"
    cleanup
fi

# Avvia lo scheduler (output visibile per vedere le decisioni IA)
python3 space_scheduler.py &

# Salva PID scheduler
SCHEDULER_PID=$!
echo -e "${GREEN}   -> Scheduler Brain: ONLINE (PID: $SCHEDULER_PID)${NC}"

# --- 5. AVVIO DASHBOARD (STABLE SERVICE LINK) ---
# Prepara il tunnel stabile verso la dashboard web
echo -e "${YELLOW}[4/4] Connessione Dashboard (Port 8080)...${NC}"

# A. CREA SERVICE STABILE SE NON ESISTE
# Espone il deployment come Service Kubernetes (NodePort)
if ! kubectl get svc space-mission >/dev/null 2>&1; then
    echo -e "${BLUE}ℹ️  Creazione Service stabile per il tunneling...${NC}"
    kubectl expose deployment space-mission --type=NodePort --port=80 --name=space-mission
fi

# B. PORT-FORWARD VERSO IL SERVICE (NON POD)
# Usa un loop infinito: se il tunnel cade (es. migrazione), riparte subito
(
    while true; do
        # Tunnel locale 8080 → porta 80 del service Kubernetes
        kubectl port-forward svc/space-mission 8080:80 >/dev/null 2>&1

        # Piccola pausa prima del retry in caso di caduta
        sleep 0.1
    done
) &

# --- MESSAGGI FINALI DI STATO ---
echo -e "\n${GREEN}✅ TUTTI I SISTEMI OPERATIVI!${NC}"
echo "---------------------------------------------------"
echo -e "🖥️  Dashboard: http://localhost:8080"
echo -e "🧠 IA:        Lo scheduler sta monitorando la telemetria..."
echo -e "🔥 Test:      Aspetta che il nodo attivo entri in ECLISSI o si surriscaldi."
echo "---------------------------------------------------"

# Mantiene vivo lo script finché lo scheduler è attivo
wait $SCHEDULER_PID
