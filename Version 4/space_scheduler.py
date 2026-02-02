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
CRITICAL_BATTERY = 30.0  # % sotto la quale il nodo è considerato a rischio spegnimento
CRITICAL_TEMP_HIGH = 80.0  # °C sopra la quale il nodo è considerato a rischio danni
FUSION_TEMP = 120.0  # °C soglia estrema: watchdog forza spegnimento immediato
PREDICTION_HORIZON = 60  # secondi di previsione nel futuro per il controllo MPC
MIGRATION_COOLDOWN = 20  # secondi minimi tra due migrazioni (evita ping-pong)

# --- PARAMETRI FISICI (Devono coincidere con physics_sim.py per una previsione accurata) ---
ORBIT_PERIOD = 120.0
ECLIPSE_START = 220
ECLIPSE_END = 320
# Parametri termici usati nella simulazione predittiva
HEATING_SUN = 100.0
HEATING_CPU_IDLE = 10.0
HEATING_CPU_LOAD = 85.0
COOLING_K = 4.0
THERMAL_MASS = 40.0
# Parametri batteria usati nella simulazione predittiva
BATTERY_CHARGE_RATE = 5.0
BATTERY_DRAIN_LOAD = 2.5

# --- SETUP LOGGING ---
# Configura formato e livello dei log su console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [SCHEDULER] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("Scheduler")


# --- CONNESSIONI ---
def connect_redis():
    # Prova a connettersi a Redis locale; ritorna client o None se fallisce
    try:
        r = redis.Redis(host='localhost', port=6379, decode_responses=True, socket_connect_timeout=2)
        r.ping()  # verifica che Redis risponda
        return r
    except:
        return None


def connect_k8s():
    # Carica la config Kubernetes e ritorna il client API o None se errore
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
        # Inizializza lo stato virtuale leggendo la telemetria reale
        self.name = "Unknown"
        self.type = data_dict.get('type', 'satellite')
        self.battery = float(data_dict.get('battery', 100))
        self.temp = float(data_dict.get('temp', 20))
        self.angle = float(data_dict.get('angle', 0))
        # Nella simulazione assumiamo che questo nodo continui a lavorare
        self.is_working = True

    def predict_future(self, seconds):
        """
        Simula cosa accade in 'seconds' secondi se il nodo continua a lavorare.
        Ritorna (is_safe, reason)
        """
        # Integrazione a passi di 1 secondo per semplicità numerica
        dt = 1.0
        steps = int(seconds / dt)

        # Velocità angolare orbitale (gradi al secondo)
        degrees_per_sec = 360.0 / ORBIT_PERIOD

        for _ in range(steps):
            # 1. Avanzamento Orbita: aggiorna posizione angolare
            self.angle = (self.angle + degrees_per_sec) % 360.0

            # 2. Check Eclissi: verifica se è in zona d’ombra
            in_eclipse = (ECLIPSE_START <= self.angle <= ECLIPSE_END)

            # 3. Evoluzione Termica: calcola calore entrante e raffreddamento
            p_in = HEATING_CPU_LOAD  # assumiamo carico CPU attivo
            if not in_eclipse:
                p_in += HEATING_SUN  # contributo del sole se illuminato

            # Raffreddamento proporzionale alla differenza con spazio profondo
            p_out = COOLING_K * (self.temp - (-270)) * 0.1  # T_space semplificata

            # Aggiorna temperatura con modello a massa termica
            self.temp += (p_in - p_out) / THERMAL_MASS * dt

            # 4. Batteria: carica al sole, scarica sotto carico
            charge = 0.0
            if not in_eclipse:
                charge = BATTERY_CHARGE_RATE

            self.battery += (charge - BATTERY_DRAIN_LOAD) * dt

            # Applica limiti fisici 0–100%
            self.battery = max(0.0, min(100.0, self.battery))

            # --- CHECK CONSTRAINT DURANTE LA SIMULAZIONE ---
            # Se viola i limiti, fermiamo la previsione e segnaliamo rischio
            if self.battery < CRITICAL_BATTERY:
                return False, f"BATTERY LOW (<{CRITICAL_BATTERY}%)"

            if self.temp > CRITICAL_TEMP_HIGH:
                return False, f"OVERHEATING (>{CRITICAL_TEMP_HIGH}°C)"

        # Se arriva qui, la previsione è considerata sicura
        return True, "SAFE"


# --- LOGICA DI CONTROLLO ---

def find_best_target(fleet_data, current_node_name):
    """
    Cerca il nodo migliore per la migrazione.
    Criteri: Più batteria, meno temperatura, possibilmente al sole.
    """
    best_node = None
    best_score = -9999  # punteggio iniziale molto basso

    for name, data in fleet_data.items():
        if name == current_node_name: continue  # salta nodo attuale
        if data['type'] == 'ground': continue  # ignora ground station

        # Calcolo punteggio: batteria pesa di più, temperatura penalizza
        score = data['battery'] * 2
        score -= data['temp']

        # Bonus se non è in eclissi (può ricaricare)
        if not data['eclipse']: score += 50

        logger.info(f"   Analisi target {name}: Bat={data['battery']}% Temp={data['temp']}° -> Score={score:.1f}")

        # Tiene il nodo con score più alto
        if score > best_score:
            best_score = score
            best_node = name

    return best_node


def trigger_migration(source, dest):
    """Lancia lo script bash di migrazione"""
    logger.warning(f"🚨 DECISIONE PRESA: MIGRAZIONE {source} -> {dest}")

    # Percorso script di migrazione (assunto nella stessa cartella)
    script_path = "./demo_migration_buildah.sh"

    # Verifica che lo script esista prima di eseguirlo
    if not os.path.exists(script_path):
        logger.error(f"❌ Script di migrazione non trovato: {script_path}")
        return

    cmd = [script_path, source, dest]  # comando con parametri sorgente/destinazione

    try:
        # Esegue lo script e aspetta la fine (chiamata bloccante)
        subprocess.run(cmd, check=True)
        logger.info("✅ Migrazione completata con successo.")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Migrazione fallita (Exit Code {e.returncode})")


def watchdog_check(current_temp):
    """Ultima linea di difesa: Spegne tutto se stiamo fondendo."""
    # Se la temperatura supera la soglia estrema, forza stop dei pod
    if current_temp > FUSION_TEMP:
        logger.critical(f"🔥 EMERGENZA TERMICA ({current_temp:.1f}°C > {FUSION_TEMP}°C). INIZIO PROCEDURE DI SICUREZZA.")
        try:
            # Cancella tutti i pod dell’applicazione via kubectl
            subprocess.run(["kubectl", "delete", "pod", "-l", "app=space-mission"], stdout=subprocess.DEVNULL)
            logger.info("💀 Pod terminato per sicurezza.")
            time.sleep(5)  # attende cleanup cluster
        except Exception as e:
            logger.error(f"Errore Watchdog: {e}")
        return True  # watchdog attivato
    return False  # tutto ok


def main():
    # Messaggio iniziale di avvio scheduler
    logger.info("🛰️  MPC SCHEDULER ONLINE. In attesa di telemetria...")

    # Connessioni iniziali a Redis e Kubernetes
    r = connect_redis()
    k8s = connect_k8s()

    # Timestamp ultima migrazione (per cooldown)
    last_migration_time = 0

    while True:
        time.sleep(1)  # ciclo di controllo a 1 Hz

        # Riconnessione resiliente a Redis se cade
        if not r: r = connect_redis()
        if not r: continue

        # 1. Leggi Telemetria dal database Redis
        try:
            raw_data = r.get("fleet_telemetry")
            if not raw_data: continue
            fleet = json.loads(raw_data)  # parse JSON -> dict
        except Exception as e:
            logger.error(f"Errore lettura Redis: {e}")
            continue

        # 2. Trova il nodo attualmente attivo (quello con carico)
        active_node_name = None
        for name, data in fleet.items():
            if data.get('is_working', False):
                active_node_name = name
                break

        if not active_node_name:
            # Nessun nodo con workload attivo
            continue

        current_data = fleet[active_node_name]

        # 3. WATCHDOG DI SICUREZZA: stop immediato se temperatura estrema
        if watchdog_check(current_data['temp']):
            continue  # se scatta, salta decisioni di scheduling

        # 4. MPC: Previsione Futura sul nodo attivo
        # Crea un clone virtuale per simulare il futuro
        sim_sat = VirtualSatellite(current_data)

        # Verifica se resterà sicuro nell’orizzonte di previsione
        is_safe, reason = sim_sat.predict_future(PREDICTION_HORIZON)

        # 5. Processo Decisionale di migrazione
        if not is_safe:
            time_since_last = time.time() - last_migration_time

            # Rispetta periodo di cooldown tra migrazioni
            if time_since_last < MIGRATION_COOLDOWN:
                logger.info(
                    f"⚠️  Rischio rilevato ({reason}), ma sistema in COOLDOWN ({int(MIGRATION_COOLDOWN - time_since_last)}s).")
            else:
                logger.warning(f"🔮 PREVISIONE MPC: {reason}")

                # Cerca il nodo alternativo migliore
                target = find_best_target(fleet, active_node_name)

                if target:
                    # Avvia migrazione e aggiorna timestamp
                    trigger_migration(active_node_name, target)
                    last_migration_time = time.time()
                else:
                    logger.error("😱 NESSUN NODO SICURO DISPONIBILE! BRACE FOR IMPACT.")
        else:
            # Heartbeat periodico per indicare stato nominale
            if int(time.time()) % 10 == 0:
                logger.info(f"✅ Stato Nominale su {active_node_name}. MPC Prediction: SAFE.")


# Entry point del programma
if __name__ == "__main__":
    main()
