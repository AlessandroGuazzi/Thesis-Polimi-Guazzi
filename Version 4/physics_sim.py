import time
import math
import json
import redis
from kubernetes import client, config

# =============================================================================
#  SPACE CLOUD PHYSICS ENGINE V3.0 (Multi-Node & Ground Station)
# =============================================================================

# --- 1. PARAMETRI ORBITALI ---
ORBIT_PERIOD = 60.0  # Secondi per un'orbita completa (velocizzato per demo)
ECLIPSE_START = 180  # Gradi inizio cono d'ombra
ECLIPSE_END = 330  # Gradi fine cono d'ombra

# --- 2. PARAMETRI TERMODINAMICI ---
COOLING_RATE = 0.5  # Raffreddamento radiativo
HEATING_SUN = 2.0  # Riscaldamento solare
HEATING_CPU_IDLE = 0.5  # Calore basale
HEATING_CPU_LOAD = 5.0  # Calore sotto carico (Pod attivo)
TEMP_SPACE = -50.0  # Temperatura ambiente

# --- 3. PARAMETRI BATTERIA ---
CHARGE_RATE = 6.0  # Ricarica pannelli solari
DRAIN_IDLE = 1.0  # Consumo avionica
DRAIN_LOAD = 4.0  # Consumo con Missione attiva

# Setup Redis (Bus Dati)
try:
    # Nota: richiede 'kubectl port-forward svc/system-redis 6379:6379'
    redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
    redis_client.ping()
    print("✅ Connesso al Bus Dati (Redis)")
except Exception as e:
    print(f"⚠️  Redis non raggiungibile: {e}")
    print("   (Hai lanciato il port-forward?)")

# Setup Kubernetes (Per sapere DOVE gira il pod)
try:
    config.load_kube_config()
    v1 = client.CoreV1Api()
    print("✅ Connesso alle API Kubernetes")
except Exception as e:
    print(f"❌ Errore connessione K8s: {e}")
    exit(1)

# STATO INIZIALE FLOTTA
# Le chiavi devono corrispondere esattamente ai nomi dei nodi (kubectl get nodes)
satellites = {
    # GROUND STATION (Master Node)
    "minikube": {
        "type": "ground",
        "battery": 100.0,
        "temp": 22.0,
        "offset": 0.0
    },
    # SATELLITE ALPHA (Worker 1)
    "minikube-m02": {
        "type": "satellite",
        "battery": 100.0,
        "temp": 10.0,
        "offset": 0.0  # Parte al Sole
    },
    # SATELLITE BETA (Worker 2)
    "minikube-m03": {
        "type": "satellite",
        "battery": 100.0,
        "temp": 10.0,
        "offset": 160.0  # Parte sfasato (quasi in ombra)
    }
}


def get_active_pods_map():
    """
    Controlla su quali nodi sta girando il pod della missione.
    Fondamentale per simulare il surriscaldamento della CPU.
    """
    active_nodes = set()
    try:
        # AGGIORNATO: Label selector corretto per il nuovo YAML
        pods = v1.list_namespaced_pod("default", label_selector="app=space-mission")
        for pod in pods.items:
            if pod.status.phase == "Running" and not pod.metadata.deletion_timestamp:
                if pod.spec.node_name:
                    active_nodes.add(pod.spec.node_name)
    except Exception:
        pass
    return active_nodes


def calculate_physics(node_name, state, start_time, is_working):
    # CASO 1: GROUND STATION (Non orbita, parametri stabili)
    if state["type"] == "ground":
        return 0, False  # Angolo 0, No eclissi

    # CASO 2: SATELLITI

    # 1. Calcolo Angolo Orbitale
    elapsed = time.time() - start_time
    angle = ((elapsed % ORBIT_PERIOD) / ORBIT_PERIOD * 360.0 + state["offset"]) % 360.0

    # 2. Calcolo Eclissi
    in_eclipse = (angle >= ECLIPSE_START and angle <= ECLIPSE_END)

    # 3. Simulazione Termica
    heat_in = HEATING_CPU_LOAD if is_working else HEATING_CPU_IDLE
    if not in_eclipse:
        heat_in += HEATING_SUN  # Aggiungi calore solare

    # Delta termico: Input - Dissipazione verso lo spazio
    delta_temp = heat_in - (COOLING_RATE * (state['temp'] - TEMP_SPACE) * 0.1)
    state['temp'] += delta_temp

    # 4. Simulazione Batteria
    if in_eclipse:
        # In ombra: solo consumo
        drain = DRAIN_LOAD if is_working else DRAIN_IDLE
        state['battery'] -= drain
    else:
        # Al sole: Ricarica netta
        drain = DRAIN_LOAD if is_working else DRAIN_IDLE
        state['battery'] += (CHARGE_RATE - drain)

    # Clamp valori (0-100%)
    state['battery'] = max(0.0, min(100.0, state['battery']))

    return int(angle), in_eclipse


def main():
    print("--- 🌌 SPACE CLOUD V3.0: PHYSICS ENGINE ---")
    print("    Simulazione avviata. Premi CTRL+C per terminare.")

    start_simulation_time = time.time()

    try:
        while True:
            # 1. Scopriamo dove sta girando il carico (CPU Load)
            active_nodes_map = get_active_pods_map()

            telemetry_batch = {}
            log_line = "\r"

            for node_name, state in satellites.items():
                # Il satellite sta lavorando?
                is_working = (node_name in active_nodes_map)

                # Calcola fisica frame corrente
                angle, is_eclipse = calculate_physics(node_name, state, start_simulation_time, is_working)

                # Prepara pacchetto dati per Redis
                status_str = "ACTIVE" if is_working else "IDLE"
                if is_eclipse: status_str += " (ECLIPSE)"

                telemetry_batch[node_name] = {
                    "type": state["type"],
                    "battery": round(state['battery'], 1),
                    "temp": round(state['temp'], 1),
                    "angle": angle,
                    "eclipse": is_eclipse,
                    "status": status_str,
                    "is_working": is_working
                }

                # Grafica Console
                if state["type"] == "ground":
                    icon = "🌍"
                    name_short = "GND"
                else:
                    name_short = node_name[-3:].upper()  # M02, M03
                    icon = "🌑" if is_eclipse else "☀️ "

                # Indicatore di lavoro (Asterisco se attivo)
                work_icon = "⚙️ " if is_working else "  "

                log_line += f"[{name_short} {icon}{work_icon} B:{state['battery']:.0f}% T:{state['temp']:.1f}°]  "

            # Pubblica su Redis
            try:
                redis_client.set("fleet_telemetry", json.dumps(telemetry_batch))
            except Exception:
                pass

            print(log_line, end="")
            time.sleep(1.0)

    except KeyboardInterrupt:
        print("\n\n🛑 Simulazione terminata.")
        exit(0)


if __name__ == "__main__":
    main()