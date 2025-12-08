import time
import math
import json
import redis
from kubernetes import client, config

# =============================================================================
# CONFIGURAZIONE
# =============================================================================
LABEL_BATTERY = "spacecloud.io/battery_level"
LABEL_STATUS = "spacecloud.io/power_status"

# Parametri Simulazione
ORBIT_DURATION = 60
SUN_DURATION = 30
UPDATE_INTERVAL = 1.0

# Tassi Energetici
CHARGE_RATE_IDLE = 5.0
CHARGE_RATE_ACTIVE = 1.0
DISCHARGE_RATE_IDLE = 0.5
DISCHARGE_RATE_ACTIVE = 2.0

THRESH_SHUTDOWN = 20
THRESH_RECOVERY = 40

satellites_state = {}

def get_orbital_data(timestamp, offset):
    """Calcola fase e progresso orbitale (0.0 - 1.0)"""
    cycle_time = (timestamp + offset) % ORBIT_DURATION
    progress = cycle_time / ORBIT_DURATION
    phase = "SUN" if cycle_time < SUN_DURATION else "ECLIPSE"
    return phase, progress

# def get_orbital_phase(timestamp, offset):
#     cycle_time = (timestamp + offset) % ORBIT_DURATION
#     return "SUN" if cycle_time < SUN_DURATION else "ECLIPSE"

def is_node_working(v1, node_name):
    try:
        # Ottimizzazione: Usiamo _preload_content=False per non parsare tutto l'oggetto
        field_selector = f"spec.nodeName={node_name}"
        pods = v1.list_namespaced_pod(namespace="default", field_selector=field_selector, _preload_content=False)
        data = json.loads(pods.data)
        
        for item in data.get('items', []):
            labels = item.get('metadata', {}).get('labels', {})
            # Se c'è un pod space-app e non sta morendo
            if labels.get('app') == "space-app" and not item.get('metadata', {}).get('deletionTimestamp'):
                return True
        return False
    except:
        return False

def get_real_node_health(node_json):
    """
    Estrae lo stato di salute direttamente dal JSON grezzo di Kubernetes.
    Molto più veloce che istanziare oggetti Python.
    """
    conditions = node_json.get('status', {}).get('conditions', [])
    for cond in conditions:
        if cond.get('type') == "Ready":
            # Se Ready è True -> ONLINE, altrimenti OFFLINE (Fault)
            return "ONLINE" if cond.get('status') == "True" else "OFFLINE"
    return "UNKNOWN"

def update_satellite_physics(node_name, current_time, is_active):
    if node_name not in satellites_state:
        offset = len(satellites_state) * (ORBIT_DURATION / 2)
        satellites_state[node_name] = { "battery": 50.0, "status": "OPERATIONAL", "offset": offset }

    sat = satellites_state[node_name]
    phase, progress = get_orbital_data(current_time, sat['offset'])
    
    delta = 0.0
    if phase == "SUN":
        delta = CHARGE_RATE_ACTIVE if is_active else CHARGE_RATE_IDLE
    else:
        delta = -DISCHARGE_RATE_ACTIVE if is_active else -DISCHARGE_RATE_IDLE
            
    sat['battery'] = max(0, min(100, sat['battery'] + (delta * UPDATE_INTERVAL)))
    
    if sat['status'] == "OPERATIONAL" and sat['battery'] < THRESH_SHUTDOWN:
        sat['status'] = "CRITICAL"
    elif sat['status'] == "CRITICAL" and sat['battery'] > THRESH_RECOVERY:
        sat['status'] = "OPERATIONAL"

    return int(sat['battery']), sat['status'], phase, progress

def main():
    print("--- 🌍 Simulatore Orbitale + Telemetria Redis ---")
    
    try:
        config.load_kube_config()
        v1 = client.CoreV1Api()
        # Connessione a Redis (tramite localhost grazie al port-forward)
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        r.ping() # Test connessione
        print("✅ Connesso a Kubernetes e Redis.")
    except Exception as e:
        print(f"Errore connessione: {e}")
        print("⚠️  Assicurati di aver fatto: kubectl port-forward service/redis-sat 6379:6379")
        return

    while True:
        loop_start = time.time()
        try:
            nodes = v1.list_node(_preload_content=False)
            nodes_data = json.loads(nodes.data)
            
            # Dizionario per raccogliere tutti i dati da mandare alla GUI
            fleet_telemetry = {}

            if int(loop_start) % 5 == 0:
                print(f"⏱️  Ciclo: {int(loop_start % ORBIT_DURATION)}s")

            for item in nodes_data['items']:
                node_name = item['metadata']['name']
                if "control-plane" in node_name: continue
                
                # Check Hardware Reale (JSON parsing)
                health = get_real_node_health(item)
                
                is_working = is_node_working(v1, node_name)
                batt, status, phase, progress = update_satellite_physics(node_name, loop_start, is_working)
                
                # Se il nodo è morto, forziamo lo status a CRITICAL per coerenza
                if health == "OFFLINE":
                    status = "HARDWARE_FAILURE"

                # Patch K8s (Solo se il nodo è vivo proviamo a patcharlo, altrimenti errore)
                if health == "ONLINE":
                    try:
                        body = { "metadata": { "labels": { LABEL_BATTERY: str(batt), LABEL_STATUS: status } } }
                        v1.patch_node(node_name, body)
                    except:
                        pass # Se fallisce la patch, probabilmente il nodo sta morendo

                fleet_telemetry[node_name] = {
                    "battery": batt,
                    "phase": phase,
                    "orbit_pos": progress,
                    "load": "WORKING" if is_working else "IDLE",
                    "status": status,
                    "health": health # ORA è il valore vero (OFFLINE/ONLINE)
                }

            r.set("constellation_telemetry", json.dumps(fleet_telemetry))
            
            # 2. DRIFT CORRECTION
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