import time
import math
import json
import redis
from kubernetes import client, config

# =============================================================================
# CONFIGURAZIONE FISICA (V3 - SPACE MESH)
# =============================================================================
LABEL_BATTERY = "spacecloud.io/battery_level"
LABEL_STATUS = "spacecloud.io/power_status"

# Parametri Simulazione Orbitale
ORBIT_DURATION = 60    # Durata orbita in secondi (accelerata)
SUN_DURATION = 30      # Durata luce solare
UPDATE_INTERVAL = 1.0  # Tick rate

# Tassi Energetici (Discharge Rates)
# V3: Introdotto il costo energetico della memoria distribuita
CHARGE_RATE_IDLE = 5.0      # Ricarica rapida a riposo
CHARGE_RATE_ACTIVE = 1.0    # Ricarica lenta sotto carico (consumo concorrente)

DISCHARGE_RATE_IDLE = 0.5   # Standby (Telemetria base)
DISCHARGE_RATE_MEMORY = 1.0 # V3: Redis + Sentinel (Radio link continuo, Sync dati)
DISCHARGE_RATE_COMPUTE = 2.0 # Dashboard (CPU intensive)

THRESH_SHUTDOWN = 20
THRESH_RECOVERY = 40

satellites_state = {}

def get_orbital_data(timestamp, offset):
    """Calcola fase e progresso orbitale (0.0 - 1.0)"""
    cycle_time = (timestamp + offset) % ORBIT_DURATION
    progress = cycle_time / ORBIT_DURATION
    phase = "SUN" if cycle_time < SUN_DURATION else "ECLIPSE"
    return phase, progress

def get_real_node_health(node_json):
    """Estrae lo stato di salute (Ready/NotReady) dal JSON k8s."""
    conditions = node_json.get('status', {}).get('conditions', [])
    for cond in conditions:
        if cond.get('type') == "Ready":
            return "ONLINE" if cond.get('status') == "True" else "OFFLINE"
    return "UNKNOWN"

def get_node_load_type(v1, node_name):
    """
    V3: Determina il tipo di carico sul satellite.
    Priorità: COMPUTE > MEMORY > IDLE
    """
    try:
        # Cerchiamo tutti i pod su questo specifico nodo
        field_selector = f"spec.nodeName={node_name}"
        pods = v1.list_namespaced_pod(namespace="default", field_selector=field_selector, _preload_content=False)
        data = json.loads(pods.data)
        
        has_compute = False
        has_memory = False

        for item in data.get('items', []):
            # Ignoriamo pod che stanno morendo
            if item.get('metadata', {}).get('deletionTimestamp'):
                continue
                
            labels = item.get('metadata', {}).get('labels', {})
            app_label = labels.get('app')

            if app_label == "space-app":
                has_compute = True
            elif app_label == "redis":
                has_memory = True

        if has_compute:
            return "COMPUTE" # Carico Massimo
        elif has_memory:
            return "MEMORY"  # Carico Medio (Consenso Distribuito)
        else:
            return "IDLE"    # Standby
            
    except:
        return "IDLE"

def update_satellite_physics(node_name, current_time, load_type):
    # Inizializzazione stato satellite se nuovo
    if node_name not in satellites_state:
        offset = len(satellites_state) * (ORBIT_DURATION / 3) # Spaziatura orbite
        satellites_state[node_name] = { "battery": 60.0, "status": "OPERATIONAL", "offset": offset }

    sat = satellites_state[node_name]
    phase, progress = get_orbital_data(current_time, sat['offset'])
    
    delta = 0.0
    
    # LOGICA DI CARICA/SCARICA V3
    if phase == "SUN":
        # Se siamo al sole, ci carichiamo.
        # Se stiamo lavorando, carichiamo più lentamente (netto = carica - consumo)
        if load_type == "COMPUTE":
            delta = CHARGE_RATE_ACTIVE 
        elif load_type == "MEMORY":
            delta = CHARGE_RATE_ACTIVE * 1.5 # Memoria consuma meno del compute
        else:
            delta = CHARGE_RATE_IDLE
    else:
        # Se siamo all'ombra, ci scarichiamo.
        if load_type == "COMPUTE":
            delta = -DISCHARGE_RATE_COMPUTE
        elif load_type == "MEMORY":
            delta = -DISCHARGE_RATE_MEMORY
        else:
            delta = -DISCHARGE_RATE_IDLE
            
    # Aggiornamento e limiti batteria (0-100%)
    sat['battery'] = max(0, min(100, sat['battery'] + (delta * UPDATE_INTERVAL)))
    
    # Isteresi Stato (Flight Rules)
    if sat['status'] == "OPERATIONAL" and sat['battery'] < THRESH_SHUTDOWN:
        sat['status'] = "CRITICAL"
    elif sat['status'] == "CRITICAL" and sat['battery'] > THRESH_RECOVERY:
        sat['status'] = "OPERATIONAL"

    return int(sat['battery']), sat['status'], phase, progress

def main():
    print("--- 🌍 Simulatore Orbitale V3 (Distributed Physics) ---")
    print(f"Rates: IDLE={DISCHARGE_RATE_IDLE}/s | MEM={DISCHARGE_RATE_MEMORY}/s | CPU={DISCHARGE_RATE_COMPUTE}/s")
    
    try:
        config.load_kube_config()
        v1 = client.CoreV1Api()
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        r.ping()
        print("✅ Connesso a Kubernetes e Redis.")
    except Exception as e:
        print(f"Errore connessione: {e}")
        return

    while True:
        loop_start = time.time()
        try:
            nodes = v1.list_node(_preload_content=False)
            nodes_data = json.loads(nodes.data)
            
            fleet_telemetry = {}

            if int(loop_start) % 5 == 0:
                print(f"⏱️  Ciclo: {int(loop_start % ORBIT_DURATION)}s")

            for item in nodes_data['items']:
                node_name = item['metadata']['name']
                
                # MODIFICA V3: Rimosso il filtro "control-plane".
                # Ora anche il Generale è soggetto alle leggi della fisica.
                # if "control-plane" in node_name: continue 
                
                health = get_real_node_health(item)
                
                # 1. Determina il carico (Compute vs Memory vs Idle)
                load_type = get_node_load_type(v1, node_name)
                
                # 2. Calcola fisica
                batt, status, phase, progress = update_satellite_physics(node_name, loop_start, load_type)
                
                if health == "OFFLINE":
                    status = "HARDWARE_FAILURE"

                # 3. Patch K8s (Solo se il nodo è vivo)
                if health == "ONLINE":
                    try:
                        body = { "metadata": { "labels": { LABEL_BATTERY: str(batt), LABEL_STATUS: status } } }
                        v1.patch_node(node_name, body)
                    except:
                        pass

                fleet_telemetry[node_name] = {
                    "battery": batt,
                    "phase": phase,
                    "orbit_pos": progress,
                    "load": load_type, # Ora mostra COMPUTE, MEMORY o IDLE
                    "status": status,
                    "health": health
                }

            r.set("constellation_telemetry", json.dumps(fleet_telemetry))
            
            elapsed = time.time() - loop_start
            sleep_time = max(0, UPDATE_INTERVAL - elapsed)
            time.sleep(sleep_time)
            
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Errore ciclo: {e}")
            time.sleep(UPDATE_INTERVAL)

if __name__ == "__main__":
    main()