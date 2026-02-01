import time
import json
import redis
import logging
import subprocess
import os
from kubernetes import client, config

# =============================================================================
#  SPACE CLOUD SCHEDULER V2.0 (MPC Controller)
# =============================================================================

# --- CONFIGURAZIONE SOGLIE DI SICUREZZA ---
CRITICAL_BATTERY = 30.0  # % (Sotto questa soglia si rischia lo shutdown)
CRITICAL_TEMP_HIGH = 80.0  # °C (Sopra questa soglia si rischia il danno)
FUSION_TEMP = 120.0  # °C (Soglia Watchdog: spegnimento immediato)
PREDICTION_HORIZON = 60  # Secondi (Guardiamo 60 secondi nel futuro)
MIGRATION_COOLDOWN = 20  # Secondi di pausa dopo una migrazione

# --- PARAMETRI FISICI (Devono coincidere con physics_sim.py per una previsione accurata) ---
ORBIT_PERIOD = 120.0
ECLIPSE_START = 220
ECLIPSE_END = 320
# Nuovi valori termici
HEATING_SUN = 100.0
HEATING_CPU_IDLE = 10.0
HEATING_CPU_LOAD = 85.0
COOLING_K = 4.0
THERMAL_MASS = 40.0
# Nuovi valori batteria
BATTERY_CHARGE_RATE = 5.0
BATTERY_DRAIN_LOAD = 2.5

# --- SETUP LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [SCHEDULER] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("Scheduler")


# --- CONNESSIONI ---
def connect_redis():
    try:
        r = redis.Redis(host='localhost', port=6379, decode_responses=True, socket_connect_timeout=2)
        r.ping()
        return r
    except:
        return None


def connect_k8s():
    try:
        config.load_kube_config()
        return client.CoreV1Api()
    except:
        return None


# --- SIMULATORE PREDITTIVO (IL "CERVELLO") ---
class VirtualSatellite:
    """
    Una copia virtuale del satellite usata per prevedere il futuro.
    """

    def __init__(self, data_dict):
        self.name = "Unknown"
        self.type = data_dict.get('type', 'satellite')
        self.battery = float(data_dict.get('battery', 100))
        self.temp = float(data_dict.get('temp', 20))
        self.angle = float(data_dict.get('angle', 0))
        # Se stiamo simulando, assumiamo che questo nodo avrà il carico
        self.is_working = True

    def predict_future(self, seconds):
        """
        Simula cosa accade in 'seconds' secondi se il nodo continua a lavorare.
        Ritorna (is_safe, reason)
        """
        # Simuliamo a passi di 1 secondo per semplicità (Euler integration)
        dt = 1.0
        steps = int(seconds / dt)

        # Parametri orbitali
        degrees_per_sec = 360.0 / ORBIT_PERIOD

        for _ in range(steps):
            # 1. Avanzamento Orbita
            self.angle = (self.angle + degrees_per_sec) % 360.0

            # 2. Check Eclissi
            in_eclipse = (ECLIPSE_START <= self.angle <= ECLIPSE_END)

            # 3. Evoluzione Termica
            p_in = HEATING_CPU_LOAD  # Assumiamo carico attivo
            if not in_eclipse:
                p_in += HEATING_SUN

            p_out = COOLING_K * (self.temp - (-270)) * 0.1  # Semplificato (T_space = -270)

            self.temp += (p_in - p_out) / THERMAL_MASS * dt

            # 4. Batteria
            charge = 0.0
            if not in_eclipse:
                charge = BATTERY_CHARGE_RATE

            self.battery += (charge - BATTERY_DRAIN_LOAD) * dt

            # Limiti fisici
            self.battery = max(0.0, min(100.0, self.battery))

            # --- CHECK CONSTRAINT DURANTE LA SIMULAZIONE ---
            if self.battery < CRITICAL_BATTERY:
                return False, f"BATTERY LOW (<{CRITICAL_BATTERY}%)"

            if self.temp > CRITICAL_TEMP_HIGH:
                return False, f"OVERHEATING (>{CRITICAL_TEMP_HIGH}°C)"

        return True, "SAFE"


# --- LOGICA DI CONTROLLO ---

def find_best_target(fleet_data, current_node_name):
    """
    Cerca il nodo migliore per la migrazione.
    Criteri: Più batteria, meno temperatura, possibilmente al sole.
    """
    best_node = None
    best_score = -9999

    for name, data in fleet_data.items():
        if name == current_node_name: continue  # Saltiamo noi stessi
        if data['type'] == 'ground': continue  # Ignoriamo ground station

        # Calcolo Punteggio (Score)
        # Più alto è meglio
        score = data['battery'] * 2  # La batteria è prioritaria
        score -= data['temp']  # La temperatura abbassa il punteggio

        # Bonus se è al sole (si ricarica)
        if not data['eclipse']: score += 50

        logger.info(f"   Analisi target {name}: Bat={data['battery']}% Temp={data['temp']}° -> Score={score:.1f}")

        if score > best_score:
            best_score = score
            best_node = name

    return best_node


def trigger_migration(source, dest):
    """Lancia lo script bash di migrazione"""
    logger.warning(f"🚨 DECISIONE PRESA: MIGRAZIONE {source} -> {dest}")

    # Assumiamo che lo script sia nella stessa cartella
    script_path = "./demo_migration_buildah.sh"

    if not os.path.exists(script_path):
        logger.error(f"❌ Script di migrazione non trovato: {script_path}")
        return

    cmd = [script_path, source, dest]

    try:
        # Eseguiamo lo script e attendiamo che finisca (Bloccante)
        subprocess.run(cmd, check=True)
        logger.info("✅ Migrazione completata con successo.")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Migrazione fallita (Exit Code {e.returncode})")


def watchdog_check(current_temp):
    """Ultima linea di difesa: Spegne tutto se stiamo fondendo."""
    if current_temp > FUSION_TEMP:
        logger.critical(f"🔥 EMERGENZA TERMICA ({current_temp:.1f}°C > {FUSION_TEMP}°C). INIZIO PROCEDURE DI SICUREZZA.")
        try:
            subprocess.run(["kubectl", "delete", "pod", "-l", "app=space-mission"], stdout=subprocess.DEVNULL)
            logger.info("💀 Pod terminato per sicurezza.")
            time.sleep(5)  # Diamo tempo a Kubernetes di pulire
        except Exception as e:
            logger.error(f"Errore Watchdog: {e}")
        return True
    return False


def main():
    logger.info("🛰️  MPC SCHEDULER ONLINE. In attesa di telemetria...")

    # Connessioni iniziali
    r = connect_redis()
    k8s = connect_k8s()

    # Timer per evitare "ping-pong" tra nodi
    last_migration_time = 0

    while True:
        time.sleep(1)  # Ciclo di controllo a 1Hz

        # Riconnessione resiliente
        if not r: r = connect_redis()
        if not r: continue

        # 1. Leggi Telemetria
        try:
            raw_data = r.get("fleet_telemetry")
            if not raw_data: continue
            fleet = json.loads(raw_data)
        except Exception as e:
            logger.error(f"Errore lettura Redis: {e}")
            continue

        # 2. Trova chi sta lavorando (Active Node)
        active_node_name = None
        for name, data in fleet.items():
            if data.get('is_working', False):
                active_node_name = name
                break

        if not active_node_name:
            # Nessun carico attivo, attendiamo...
            continue

        current_data = fleet[active_node_name]

        # 3. WATCHDOG DI SICUREZZA (Safety Cut-off)
        if watchdog_check(current_data['temp']):
            continue  # Se il watchdog è scattato, il pod è morto, ricominciamo il ciclo

        # 4. MPC: Previsione Futura
        # Creiamo un clone virtuale
        sim_sat = VirtualSatellite(current_data)

        # Chiediamo: "Sopravvivrà ai prossimi 10 minuti?"
        is_safe, reason = sim_sat.predict_future(PREDICTION_HORIZON)

        # 5. Processo Decisionale
        if not is_safe:
            time_since_last = time.time() - last_migration_time

            if time_since_last < MIGRATION_COOLDOWN:
                logger.info(
                    f"⚠️  Rischio rilevato ({reason}), ma sistema in COOLDOWN ({int(MIGRATION_COOLDOWN - time_since_last)}s).")
            else:
                logger.warning(f"🔮 PREVISIONE MPC: {reason}")

                # Cerca un posto sicuro
                target = find_best_target(fleet, active_node_name)

                if target:
                    trigger_migration(active_node_name, target)
                    last_migration_time = time.time()
                else:
                    logger.error("😱 NESSUN NODO SICURO DISPONIBILE! BRACE FOR IMPACT.")
        else:
            # Log heartbeat ogni 10 secondi per dire "tutto ok"
            if int(time.time()) % 10 == 0:
                logger.info(f"✅ Stato Nominale su {active_node_name}. MPC Prediction: SAFE.")


if __name__ == "__main__":
    main()