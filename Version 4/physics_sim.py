import time
import math
import json
import redis
import logging
from kubernetes import client, config

# =============================================================================
#  SPACE CLOUD PHYSICS ENGINE V4.0 (Enhanced Dynamics)
# =============================================================================

# --- CONFIGURAZIONE SIMULAZIONE ---
ORBIT_PERIOD = 120.0  # Secondi per un'orbita completa (2 minuti)
ECLIPSE_START = 220  # Gradi inizio cono d'ombra
ECLIPSE_END = 320  # Gradi fine cono d'ombra (100 gradi di buio)

# --- CONFIGURAZIONE FISICA ---
# Temp (°C)
TEMP_SPACE = -270.0  # Zero assoluto (quasi)
TEMP_OPTIMAL = 20.0  # Temp interna Ground Station

# Coefficienti Termici
# Più alto è THERMAL_MASS, più lentamente cambia la temperatura
THERMAL_MASS = 50.0
HEATING_SUN = 150.0  # Calore dal Sole
HEATING_CPU_IDLE = 10.0  # Calore basale elettronica
HEATING_CPU_LOAD = 80.0  # Calore extra quando il Pod lavora
COOLING_K = 0.8  # Costante di dissipazione radiativa

# Coefficienti Batteria (% al secondo)
BATTERY_CHARGE_RATE = 2.5  # Velocità ricarica al sole
BATTERY_DRAIN_IDLE = 0.5  # Consumo base
BATTERY_DRAIN_LOAD = 3.5  # Consumo pesante (CPU al 100%)

# --- SETUP LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("PhysicsEngine")


# --- CLASSE SATELLITE ---
class Satellite:
    def __init__(self, name, node_type, start_offset_deg=0):
        self.name = name
        self.type = node_type  # 'ground' o 'satellite'
        self.offset = start_offset_deg

        # Stato Iniziale
        self.battery = 100.0
        self.temp = 20.0
        self.angle = 0
        self.in_eclipse = False
        self.is_working = False  # True se ha il Pod

        # Ground Station è sempre stabile
        if self.type == 'ground':
            self.temp = 22.0

    def update(self, elapsed_time, has_workload):
        self.is_working = has_workload

        # SE È GROUND STATION: Parametri fissi
        if self.type == 'ground':
            self.battery = 100.0
            self.temp = 22.0
            return

        # 1. CALCOLO ORBITA
        # Calcoliamo l'angolo attuale (0-360) basandoci sul tempo trascorso e l'offset iniziale
        raw_angle = ((elapsed_time % ORBIT_PERIOD) / ORBIT_PERIOD * 360.0 + self.offset)
        self.angle = raw_angle % 360.0

        # 2. RILEVAMENTO ECLISSI
        # Controlliamo se siamo nel cono d'ombra
        if ECLIPSE_START <= self.angle <= ECLIPSE_END:
            self.in_eclipse = True
        else:
            self.in_eclipse = False

        # 3. MODELLO TERMICO (Equazione Differenziale Semplificata)
        # dT/dt = (P_in - P_out) / MassaTermica

        # P_in (Input Calore)
        p_in = HEATING_CPU_LOAD if self.is_working else HEATING_CPU_IDLE
        if not self.in_eclipse:
            p_in += HEATING_SUN  # Aggiunge il sole

        # P_out (Dissipazione) - Legge di Stefan-Boltzmann linearizzata per semplicità
        # Il satellite cerca di raggiungere l'equilibrio con lo spazio profondo
        # Più è caldo, più dissipa.
        p_out = COOLING_K * (self.temp - TEMP_SPACE) * 0.1

        delta_temp = (p_in - p_out) / THERMAL_MASS
        self.temp += delta_temp

        # 4. BILANCIO ENERGETICO (Batteria)
        charge = 0.0
        drain = BATTERY_DRAIN_LOAD if self.is_working else BATTERY_DRAIN_IDLE

        if not self.in_eclipse:
            charge = BATTERY_CHARGE_RATE

        self.battery += (charge - drain)

        # Clamp valori (Limiti fisici)
        self.battery = max(0.0, min(100.0, self.battery))
        # La temp non ha limiti hard, ma sotto i -50 o sopra i 100 si rompe (simulato dallo scheduler)

    def get_telemetry(self):
        """Restituisce il dizionario pronto per JSON/Redis"""
        status_code = "IDLE"
        if self.is_working: status_code = "ACTIVE"

        return {
            "type": self.type,
            "battery": round(self.battery, 2),
            "temp": round(self.temp, 2),
            "angle": int(self.angle),
            "eclipse": self.in_eclipse,
            "status": status_code,
            "is_working": self.is_working
        }


# --- FUNZIONI DI SUPPORTO ---

def connect_k8s():
    """Tenta la connessione a Kubernetes (Locale o In-Cluster)"""
    try:
        config.load_kube_config()  # Cerca ~/.kube/config
        v1 = client.CoreV1Api()
        logger.info("✅ K8s API Connected")
        return v1
    except Exception as e:
        logger.error(f"❌ K8s Connection Failed: {e}")
        return None


def connect_redis():
    """Connette a Redis (richiede port-forward)"""
    try:
        r = redis.Redis(host='localhost', port=6379, decode_responses=True, socket_connect_timeout=2)
        r.ping()
        logger.info("✅ Redis Connected")
        return r
    except Exception as e:
        logger.warning(f"⚠️  Redis connection failed (Is port-forward running?): {e}")
        return None


def get_pod_node_map(v1_client):
    """
    Interroga K8s per trovare dove sono i Pod 'space-mission'.
    Ritorna un set di nomi di nodi attivi.
    """
    active_nodes = set()
    if not v1_client: return active_nodes

    try:
        # Cerchiamo i pod con label 'app=space-mission'
        pods = v1_client.list_namespaced_pod(namespace="default", label_selector="app=space-mission")
        for pod in pods.items:
            # Consideriamo il pod attivo solo se sta girando o si sta avviando
            if pod.status.phase in ["Running", "Pending"] and not pod.metadata.deletion_timestamp:
                if pod.spec.node_name:
                    active_nodes.add(pod.spec.node_name)
    except Exception as e:
        # Silenzioso per non spammare la console in caso di timeout
        pass

    return active_nodes


# --- MAIN LOOP ---

def main():
    print("\n🚀 AVVIO SIMULATORE FISICO ORBITALE...")
    print("   Premi CTRL+C per arrestare.\n")

    # 1. Inizializzazione Sistemi
    k8s_api = connect_k8s()
    redis_db = connect_redis()

    # 2. Creazione Flotta
    # Offset: 0 = Inizia all'equatore lato Sole, 180 = Lato notte
    fleet = [
        Satellite("minikube", "ground"),
        Satellite("minikube-m02", "satellite", start_offset_deg=0),  # Parte al Sole
        Satellite("minikube-m03", "satellite", start_offset_deg=160)  # Parte vicino all'ombra
    ]

    start_time = time.time()

    try:
        while True:
            # A. Dove sta il carico?
            active_nodes = get_pod_node_map(k8s_api)

            # B. Aggiorna Fisica di ogni nodo
            elapsed = time.time() - start_time
            telemetry_data = {}

            # Stringa di log per la console (stile Dashboard)
            console_log = "\r"

            for sat in fleet:
                # Controlla se questo satellite sta lavorando
                is_working = (sat.name in active_nodes)

                # Calcola fisica
                sat.update(elapsed, is_working)

                # Prepara dati
                telemetry_data[sat.name] = sat.get_telemetry()

                # --- Visualizzazione Console ---
                # Icone: 🌍(Terra) ☀️(Sole) 🌑(Ombra) ⚙️(Lavoro)
                icon = "🌍" if sat.type == 'ground' else ("🌑" if sat.in_eclipse else "☀️ ")
                work_indicator = "⚙️ " if is_working else "  "

                # Colora output se c'è un problema (solo visivo per terminale)
                # ANSI codes: \033[91m = Red, \033[0m = Reset
                stats = f"B:{int(sat.battery)}% T:{int(sat.temp)}°"
                if sat.battery < 20 or sat.temp > 80:
                    stats = f"\033[91m{stats}\033[0m"  # Rosso se critico

                name_short = sat.name.replace("minikube", "MK").replace("-", "")
                console_log += f"[{name_short} {icon}{work_indicator} {stats}]  "

            # C. Pubblica su Redis
            if redis_db:
                try:
                    redis_db.set("fleet_telemetry", json.dumps(telemetry_data))
                    # Opzionale: Pubblica anche un messaggio pub/sub per eventi real-time
                    # redis_db.publish("telemetry_update", "new_data")
                except Exception:
                    pass  # Ignora errori temporanei di rete

            # Stampa e attendi
            print(console_log, end="", flush=True)
            time.sleep(1.0)

    except KeyboardInterrupt:
        print("\n\n🛑 Simulazione Arrestata. Atterraggio completato.")


if __name__ == "__main__":
    main()