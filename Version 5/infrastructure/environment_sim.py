import time
import json
import redis
import logging
from kubernetes import client, config

# =============================================================================
#  SPACE CLOUD V5.2 - ENVIRONMENT SIMULATOR (Pure Pub/Sub)
#  Ruolo: Simula la fisica e la trasmette ESCLUSIVAMENTE via Message Broker.
# =============================================================================

# --- CONFIGURAZIONE FISICA ---
ORBIT_PERIOD = 120.0
ECLIPSE_START = 220
ECLIPSE_END = 320

TEMP_SPACE = -270.0
THERMAL_MASS = 40.0
HEATING_SUN = 100.0
HEATING_CPU_IDLE = 10.0
HEATING_CPU_LOAD = 85.0
COOLING_K = 4.0

BATTERY_CHARGE_RATE = 5.0
BATTERY_DRAIN_IDLE = 1.0
BATTERY_DRAIN_LOAD = 2.5

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [SIM] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("EnvironmentSim")


class Satellite:
    def __init__(self, name, node_type, start_offset_deg=0):
        self.name = name
        self.type = node_type
        self.offset = start_offset_deg
        self.battery = 100.0
        self.temp = 20.0
        self.angle = 0
        self.in_eclipse = False
        self.is_working = False

    def update(self, elapsed_time, has_workload):
        self.is_working = has_workload
        if self.type == 'ground': return

        # 1. Orbita
        raw_angle = ((elapsed_time % ORBIT_PERIOD) / ORBIT_PERIOD * 360.0 + self.offset)
        self.angle = raw_angle % 360.0
        self.in_eclipse = (ECLIPSE_START <= self.angle <= ECLIPSE_END)

        # 2. Termica
        p_in = HEATING_CPU_LOAD if self.is_working else HEATING_CPU_IDLE
        if not self.in_eclipse: p_in += HEATING_SUN
        p_out = COOLING_K * (self.temp - TEMP_SPACE) * 0.1
        self.temp += (p_in - p_out) / THERMAL_MASS

        # 3. Energetica
        charge = BATTERY_CHARGE_RATE if not self.in_eclipse else 0.0
        drain = BATTERY_DRAIN_LOAD if self.is_working else BATTERY_DRAIN_IDLE
        self.battery += (charge - drain)
        self.battery = max(0.0, min(100.0, self.battery))

    def get_forecast(self, horizon_seconds=60):
        sim_angle = self.angle
        sim_temp = self.temp
        sim_batt = self.battery
        deg_per_sec = 360.0 / ORBIT_PERIOD

        for _ in range(horizon_seconds):
            sim_angle = (sim_angle + deg_per_sec) % 360.0
            in_eclipse = (ECLIPSE_START <= sim_angle <= ECLIPSE_END)

            p_in = HEATING_CPU_LOAD if self.is_working else HEATING_CPU_IDLE
            if not in_eclipse: p_in += HEATING_SUN
            p_out = COOLING_K * (sim_temp - TEMP_SPACE) * 0.1
            sim_temp += (p_in - p_out) / THERMAL_MASS

            charge = BATTERY_CHARGE_RATE if not in_eclipse else 0.0
            drain = BATTERY_DRAIN_LOAD if self.is_working else BATTERY_DRAIN_IDLE
            sim_batt += (charge - drain)
            sim_batt = max(0.0, min(100.0, sim_batt))

        return {"temp_60s": round(sim_temp, 1), "bat_60s": int(sim_batt)}

    def get_telemetry(self):
        return {
            "type": self.type,
            "battery": round(self.battery, 1),
            "temp": round(self.temp, 1),
            "angle": int(self.angle),
            "eclipse": self.in_eclipse,
            "is_working": self.is_working,
            "forecast": self.get_forecast(60)
        }


def connect_k8s():
    try:
        config.load_kube_config()
        return client.CoreV1Api()
    except:
        logger.warning("Impossibile connettersi a K8s. Riprovo...")
        return None


def connect_redis():
    try:
        r = redis.Redis(host='localhost', port=6379, decode_responses=True, socket_connect_timeout=1)
        r.ping()
        return r
    except:
        return None


def get_pod_node_map(v1_client):
    active_nodes = set()
    if not v1_client: return active_nodes
    try:
        pods = v1_client.list_namespaced_pod(namespace="default", label_selector="app=space-mission")
        for pod in pods.items:
            if pod.status.phase in ["Running", "Pending"] and not pod.metadata.deletion_timestamp:
                if pod.spec.node_name:
                    active_nodes.add(pod.spec.node_name)
    except Exception as e:
        logger.error(f"Errore K8s API: {e}")
    return active_nodes


def main():
    print("\n🌍 ENVIRONMENT SIMULATOR V5.2 ONLINE (PURE PUB/SUB MODE).")
    k8s_api = connect_k8s()
    redis_db = connect_redis()

    fleet = [
        Satellite("minikube", "ground"),
        Satellite("minikube-m02", "satellite", start_offset_deg=0),
        Satellite("minikube-m03", "satellite", start_offset_deg=120),
        Satellite("minikube-m04", "satellite", start_offset_deg=240)
    ]

    start_time = time.time()

    while True:
        active_nodes = get_pod_node_map(k8s_api)
        elapsed = time.time() - start_time

        console_log = "\r"

        for sat in fleet:
            is_working = (sat.name in active_nodes)
            sat.update(elapsed, is_working)
            sat_data = sat.get_telemetry()

            # --- V5.2: PURE EVENT-DRIVEN PUBLISH ---
            # Ogni satellite pubblica il proprio stato sul proprio canale.
            # Rimosso completamente il salvataggio dello stato globale "fleet_telemetry"
            if redis_db:
                try:
                    channel = f"telemetry/{sat.name}"
                    redis_db.publish(channel, json.dumps(sat_data))
                except:
                    redis_db = connect_redis()
            # ---------------------------------------

            status_icon = "🔥" if is_working else ("🌑" if sat.in_eclipse else "☀️")
            console_log += f"[{sat.name[-3:]} {int(sat.battery)}% {int(sat.temp)}° {status_icon}] "

        print(console_log, end="", flush=True)
        time.sleep(1.0)


if __name__ == "__main__":
    main()