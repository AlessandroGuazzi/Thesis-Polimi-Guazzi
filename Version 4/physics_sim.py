import time
import math
import json
import redis
import logging
from kubernetes import client, config

# =============================================================================
#  SPACE CLOUD PHYSICS ENGINE V4.1 (With Telemetry Forecasting)
# =============================================================================

# --- CONFIGURAZIONE FISICA (REALISTIC CONSTELLATION) ---
# Parametri dell’orbita e delle zone di eclissi
ORBIT_PERIOD = 120.0          # Secondi per completare un’orbita completa
ECLIPSE_START = 220           # Angolo inizio eclissi (gradi)
ECLIPSE_END = 320             # Angolo fine eclissi (gradi)

# Temperature di riferimento
TEMP_SPACE = -270.0           # Temperatura spazio profondo
TEMP_OPTIMAL = 20.0           # Temperatura target operativa

# Coefficienti Termici (modello semplificato di riscaldamento/raffreddamento)
THERMAL_MASS = 40.0
HEATING_SUN = 100.0
HEATING_CPU_IDLE = 10.0
HEATING_CPU_LOAD = 85.0
COOLING_K = 4.0

# Coefficienti Batteria (carica/scarica per secondo)
BATTERY_CHARGE_RATE = 5.0
BATTERY_DRAIN_IDLE = 1.0
BATTERY_DRAIN_LOAD = 2.5

# --- SETUP LOGGING ---
# Configura logging base su console con timestamp corto
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("PhysicsEngine")


class Satellite:
    # Rappresenta un nodo/satellite con stato fisico simulato
    def __init__(self, name, node_type, start_offset_deg=0):
        self.name = name
        self.type = node_type
        self.offset = start_offset_deg

        # Stato iniziale del satellite
        self.battery = 100.0
        self.temp = 20.0
        self.angle = 0
        self.in_eclipse = False
        self.is_working = False

    def update(self, elapsed_time, has_workload):
        # Aggiorna lo stato fisico in base al tempo e al carico
        self.is_working = has_workload

        # I nodi ground non orbitano → nessun aggiornamento fisico
        if self.type == 'ground': return

        # 1. Calcolo Orbita: converte il tempo in angolo orbitale
        raw_angle = ((elapsed_time % ORBIT_PERIOD) / ORBIT_PERIOD * 360.0 + self.offset)
        self.angle = raw_angle % 360.0

        # 2. Verifica se il satellite è in eclissi (senza sole)
        self.in_eclipse = (ECLIPSE_START <= self.angle <= ECLIPSE_END)

        # 3. Modello Termico: calcolo potenza in ingresso/uscita
        p_in = HEATING_CPU_LOAD if self.is_working else HEATING_CPU_IDLE
        if not self.in_eclipse: p_in += HEATING_SUN  # aggiunge calore solare se illuminato
        p_out = COOLING_K * (self.temp - TEMP_SPACE) * 0.1  # raffreddamento verso lo spazio
        self.temp += (p_in - p_out) / THERMAL_MASS

        # 4. Modello Batteria: carica al sole, scarica con carico
        charge = BATTERY_CHARGE_RATE if not self.in_eclipse else 0.0
        drain = BATTERY_DRAIN_LOAD if self.is_working else BATTERY_DRAIN_IDLE
        self.battery += (charge - drain)

        # Limita batteria tra 0% e 100%
        self.battery = max(0.0, min(100.0, self.battery))

    def get_forecast(self, horizon_seconds=60):
        """Simula il futuro per la dashboard (senza modificare lo stato attuale)"""

        # Copie temporanee dello stato corrente
        sim_angle = self.angle
        sim_temp = self.temp
        sim_batt = self.battery

        dt = 1.0
        deg_per_sec = 360.0 / ORBIT_PERIOD  # velocità angolare

        # Simula passo-passo i prossimi N secondi
        for _ in range(horizon_seconds):
            sim_angle = (sim_angle + deg_per_sec) % 360.0
            in_eclipse = (ECLIPSE_START <= sim_angle <= ECLIPSE_END)

            # Assume carico invariato rispetto allo stato attuale
            p_in = HEATING_CPU_LOAD if self.is_working else HEATING_CPU_IDLE
            if not in_eclipse: p_in += HEATING_SUN
            p_out = COOLING_K * (sim_temp - TEMP_SPACE) * 0.1
            sim_temp += (p_in - p_out) / THERMAL_MASS

            # Aggiorna batteria simulata
            charge = BATTERY_CHARGE_RATE if not in_eclipse else 0.0
            drain = BATTERY_DRAIN_LOAD if self.is_working else BATTERY_DRAIN_IDLE
            sim_batt += (charge - drain)

        # Ritorna previsione arrotondata per dashboard
        return {
            "temp_60s": round(sim_temp, 1),
            "bat_60s": max(0, min(100, int(sim_batt)))
        }

    def get_telemetry(self):
        # Costruisce il pacchetto telemetria completo + forecast
        forecast = self.get_forecast(60)
        return {
            "type": self.type,
            "battery": round(self.battery, 1),
            "temp": round(self.temp, 1),
            "angle": int(self.angle),
            "eclipse": self.in_eclipse,
            "is_working": self.is_working,
            "forecast": forecast
        }


# --- FUNZIONI DI SUPPORTO ---

def connect_k8s():
    # Prova a connettersi a Kubernetes usando kubeconfig locale
    try:
        config.load_kube_config()
        return client.CoreV1Api()
    except:
        return None


def connect_redis():
    # Prova a connettersi a Redis locale
    try:
        r = redis.Redis(host='localhost', port=6379, decode_responses=True, socket_connect_timeout=2)
        r.ping()  # test connessione
        return r
    except:
        return None


def get_pod_node_map(v1_client):
    # Restituisce l’insieme dei nodi che stanno eseguendo pod attivi
    active_nodes = set()
    if not v1_client: return active_nodes
    try:
        # Lista pod con label specifica
        pods = v1_client.list_namespaced_pod(namespace="default", label_selector="app=space-mission")
        for pod in pods.items:
            # Considera solo pod attivi e non in terminazione
            if pod.status.phase in ["Running", "Pending"] and not pod.metadata.deletion_timestamp:
                if pod.spec.node_name:
                    active_nodes.add(pod.spec.node_name)
    except:
        pass
    return active_nodes


def main():
    # Entry point motore fisico
    print("\n🚀 PHYSICS ENGINE V4.1 STARTED.")
    k8s_api = connect_k8s()      # Connessione Kubernetes
    redis_db = connect_redis()   # Connessione Redis

    # Flotta: 1 ground + 3 satelliti sfasati di 120°
    fleet = [
        Satellite("minikube", "ground"),
        Satellite("minikube-m02", "satellite", start_offset_deg=0),
        Satellite("minikube-m03", "satellite", start_offset_deg=120),
        Satellite("minikube-m04", "satellite", start_offset_deg=240)
    ]

    start_time = time.time()  # Tempo di avvio simulazione

    # Loop infinito di simulazione
    while True:
        # Nodi con workload attivo
        active_nodes = get_pod_node_map(k8s_api)

        # Tempo trascorso dall’avvio
        elapsed = time.time() - start_time

        telemetry_data = {}   # Dizionario telemetria completa
        console_log = "\r"    # Riga console compatta

        # Aggiorna ogni satellite
        for sat in fleet:
            is_working = (sat.name in active_nodes)
            sat.update(elapsed, is_working)
            telemetry_data[sat.name] = sat.get_telemetry()

            # Log console minimale per stato batteria
            icon = "⚙️ " if is_working else ""
            console_log += f"[{sat.name[-3:]} {int(sat.battery)}%{icon}] "

        # Scrive telemetria su Redis per la dashboard
        if redis_db:
            try:
                redis_db.set("fleet_telemetry", json.dumps(telemetry_data))
            except:
                pass

        # Stampa riga stato in-place
        print(console_log, end="", flush=True)

        # Attende 1 secondo prima del prossimo step
        time.sleep(1.0)


# Avvio script se eseguito direttamente
if __name__ == "__main__":
    main()
