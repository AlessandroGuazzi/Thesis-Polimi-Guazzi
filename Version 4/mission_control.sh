#!/bin/bash

# ============================================================
# 🎮 SPACE MISSION CONTROL CENTER
# Avvia tutti i sistemi di supporto in background.
# Gestisce riconnessioni automatiche durante la migrazione.
# ============================================================

# Colori
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}🚀 INIZIALIZZAZIONE MISSION CONTROL...${NC}"

# --- 1. FUNZIONE DI PULIZIA (TRAP) ---
# Quando premi Ctrl+C, questo blocca uccide tutti i processi figli
cleanup() {
    echo -e "\n${RED}🛑 SHUTDOWN: Chiusura di tutti i canali di comunicazione...${NC}"
    kill $(jobs -p) 2>/dev/null
    echo -e "${GREEN}✅ Mission Control terminato. A presto Comandante.${NC}"
    exit
}
trap cleanup SIGINT SIGTERM

# --- 2. AVVIO CANALE REDIS (Background) ---
echo -e "${YELLOW}[1/3] Apertura canale dati Redis (Port 6379)...${NC}"
kubectl port-forward svc/system-redis 6379:6379 >/dev/null 2>&1 &
REDIS_PID=$!
sleep 2 # Diamo tempo di connettersi

if ps -p $REDIS_PID > /dev/null; then
    echo -e "${GREEN}   -> Redis Link: ATTIVO${NC}"
else
    echo -e "${RED}   -> Errore connessione Redis!${NC}"
    exit 1
fi

# --- 3. AVVIO SIMULATORE FISICO (Background) ---
echo -e "${YELLOW}[2/3] Avvio motore fisico (physics_sim.py)...${NC}"
# Controlliamo se siamo nel venv
if [[ "$VIRTUAL_ENV" == "" ]]; then
    echo -e "${RED}⚠️  ATTENZIONE: Non sembra che tu sia nel virtual environment!${NC}"
    echo "   Premi INVIO per continuare lo stesso o Ctrl+C per uscire."
    read
fi

python physics_sim.py > /dev/null 2>&1 &
# Nota: Ho nascosto l'output del simulatore (> /dev/null) per tenere pulito.
# Se vuoi vederlo, togli "> /dev/null 2>&1"
echo -e "${GREEN}   -> Physics Engine: IN ESECUZIONE${NC}"

# --- 4. AVVIO DASHBOARD (LOOP INFINITO) ---
echo -e "${YELLOW}[3/3] Apertura canale Dashboard (Port 8080)...${NC}"
echo -e "${BLUE}ℹ️  Questo canale si riconnetterà automaticamente dopo la migrazione.${NC}"

# Funzione che gira in background per tenere su il port-forward
(
    while true; do
        # Tentativo di connessione
        kubectl port-forward svc/space-mission-service 8080:80 >/dev/null 2>&1

        # Se arriviamo qui, il port-forward è caduto (es. migrazione)
        echo -e "\n${YELLOW}📡 CONNESSIONE PERSA (MIGRAZIONE?). Ricerca nuovo segnale...${NC}"
        sleep 2
    done
) &

# --- 5. STATO PRONTO ---
echo -e "\n${GREEN}✅ TUTTI I SISTEMI OPERATIVI!${NC}"
echo "---------------------------------------------------"
echo -e "🖥️  Dashboard:  http://localhost:8080"
echo -e "📜 Migrazione:  Apri un altro terminale per lanciare lo script."
echo -e "🛑 Uscita:      Premi Ctrl+C per spegnere tutto."
echo "---------------------------------------------------"

# Mantieni lo script vivo per catturare il Ctrl+C
wait