import time
import json
import redis
import logging
import os

# =============================================================================
#  SPACE CLOUD V5.2 - MPC CONTROLLER (Event-Driven Brain)
#  Ruolo: Ascolta i sensori via Pub/Sub e trasmette ordini via Redis.
# =============================================================================

# Soglie di Sicurezza
CRITICAL_BATTERY = 30.0
CRITICAL_TEMP_HIGH = 80.0
FUSION_TEMP = 120.0
PREDICTION_HORIZON = 60
MIGRATION_COOLDOWN = 20

# Parametri fisici
ORBIT_PERIOD = 120.0
ECLIPSE_START = 220
ECLIPSE_END = 320
HEATING_SUN = 100.0
HEATING_CPU_LOAD = 85.0
COOLING_K = 4.0
THERMAL_MASS = 40.0
BATTERY_CHARGE_RATE = 5.0
BATTERY_DRAIN_LOAD = 2.5

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [MPC] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("MPC")


def connect_redis():
    try:
        r = redis.Redis(host='localhost', port=6379, decode_responses=True, socket_connect_timeout=2)
        r.ping()
        return r
    except:
        return None


# --- MOTORE PREDITTIVO ---
class VirtualSatellite:
    def __init__(self, data_dict):
        self.battery = float(data_dict.get('battery', 100))
        self.temp = float(data_dict.get('temp', 20))
        self.angle = float(data_dict.get('angle', 0))

    def predict_future(self, seconds):
        dt = 1.0
        steps = int(seconds / dt)
        deg_per_sec = 360.0 / ORBIT_PERIOD

        for _ in range(steps):
            self.angle = (self.angle + deg_per_sec) % 360.0
            in_eclipse = (ECLIPSE_START <= self.angle <= ECLIPSE_END)

            p_in = HEATING_CPU_LOAD
            if not in_eclipse: p_in += HEATING_SUN
            p_out = COOLING_K * (self.temp - (-270)) * 0.1
            self.temp += (p_in - p_out) / THERMAL_MASS * dt

            charge = BATTERY_CHARGE_RATE if not in_eclipse else 0.0
            self.battery += (charge - BATTERY_DRAIN_LOAD) * dt
            self.battery = max(0.0, min(100.0, self.battery))

            if self.battery < CRITICAL_BATTERY:
                return False, f"LOW BATTERY PREDICTION (<{CRITICAL_BATTERY}%)"
            if self.temp > CRITICAL_TEMP_HIGH:
                return False, f"THERMAL RUNAWAY PREDICTION (>{CRITICAL_TEMP_HIGH}°C)"

        return True, "SAFE"


# --- LOGICA DI CONTROLLO ---

def find_best_target(fleet_state, current_node):
    best_node = None
    best_score = -9999

    for name, data in fleet_state.items():
        if name == current_node: continue
        if data['type'] == 'ground': continue

        score = data['battery'] * 2
        score -= data['temp']
        if not data['eclipse']: score += 50

        # Rimosso il log dettagliato di ogni singolo target per tenere la console più pulita
        if score > best_score:
            best_score = score
            best_node = name

    return best_node


# =============================================================================
# V5.2: COMMAND PUBLISH (Addio Script Bash!)
# =============================================================================
def trigger_migration(redis_client, source, dest):
    logger.warning(f"🚨 ACTION: ORDINE DI MIGRAZIONE {source} -> {dest}")

    # Prepariamo l'ordine in formato JSON
    payload = {
        "action": "MIGRATE",
        "target_node": dest
    }

    # Pubblichiamo l'ordine sul canale di ascolto dell'Agente del nodo sotto sforzo
    channel = f"commands/{source}"
    try:
        redis_client.publish(channel, json.dumps(payload))
        logger.info(f"✅ Ordine trasmesso con successo sul canale '{channel}'.")
    except Exception as e:
        logger.error(f"❌ Impossibile trasmettere l'ordine: {e}")


def watchdog(current_temp):
    if current_temp > FUSION_TEMP:
        logger.critical(f"🔥 MELTDOWN IMMINENTE ({current_temp}°C). EMERGENCY KILL.")
        # Sostituito subprocess con os.system per pulizia dei moduli
        os.system("kubectl delete deployment space-mission > /dev/null 2>&1")
        return True
    return False


def main():
    logger.info("🧠 MPC BRAIN V5.2 ONLINE. Attesa Link Sensori (Pub/Sub)...")

    fleet_state = {}
    last_migration_time = 0

    while True:
        r = connect_redis()
        if not r:
            time.sleep(1)
            continue

        try:
            pubsub = r.pubsub()
            pubsub.psubscribe('telemetry/*')
            logger.info("📡 Link stabilito. Iscritto ai canali telemetry/*")

            for message in pubsub.listen():
                if message['type'] == 'pmessage':
                    channel = message['channel']
                    node_name = channel.split('/')[1]

                    node_data = json.loads(message['data'])
                    fleet_state[node_name] = node_data

                    active_node = None
                    for name, data in fleet_state.items():
                        if data.get('is_working', False):
                            active_node = name
                            break

                    if not active_node: continue

                    if node_name == active_node:
                        current_data = fleet_state[active_node]

                        if watchdog(current_data['temp']): continue

                        sim = VirtualSatellite(current_data)
                        is_safe, reason = sim.predict_future(PREDICTION_HORIZON)

                        if not is_safe:
                            time_since = time.time() - last_migration_time
                            if time_since < MIGRATION_COOLDOWN:
                                logger.info(f"⚠️  Risk: {reason} (Cooldown: {int(MIGRATION_COOLDOWN - time_since)}s)")
                            else:
                                logger.warning(f"🔮 PREDICTION SUL NODO {active_node}: {reason}")
                                target = find_best_target(fleet_state, active_node)
                                if target:
                                    # Passiamo l'oggetto connessione 'r' alla funzione
                                    trigger_migration(r, active_node, target)
                                    last_migration_time = time.time()
                                else:
                                    logger.error("😱 NO SAFE TARGETS! BRACE FOR IMPACT.")
                        else:
                            if int(time.time()) % 10 == 0:
                                logger.info(f"✅ Nodo {active_node} Operativo. Prediction: SAFE.")

        except redis.ConnectionError:
            logger.error("❌ Connessione Redis caduta. Riconnessione in corso...")
            time.sleep(2)
        except Exception as e:
            logger.error(f"Errore Loop Eventi: {e}")
            time.sleep(1)


if __name__ == "__main__":
    main()