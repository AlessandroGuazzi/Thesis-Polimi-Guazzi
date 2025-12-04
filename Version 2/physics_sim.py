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
UPDATE_INTERVAL = 2

# Tassi Energetici
CHARGE_RATE_IDLE = 5.0
CHARGE_RATE_ACTIVE = 1.0
DISCHARGE_RATE_IDLE = 0.5
DISCHARGE_RATE_ACTIVE = 2.0

THRESH_SHUTDOWN = 20
THRESH_RECOVERY = 40

satellites_state = {}

def get_orbital_phase(timestamp, offset):
    cycle_time = (timestamp + offset) % ORBIT_DURATION
    return "SUN" if cycle_time < SUN_DURATION else "ECLIPSE"

def is_node_working(v1, node_name):
    try:
        field_selector = f"spec.nodeName={node_name}"
        pods = v1.list_namespaced_pod(namespace="default", field_selector=field_selector).items
        active_pods = [p for p in pods if not p.metadata.deletion_timestamp and p.metadata.labels.get("app") == "space-app"]
        return len(active_pods) > 0
    except:
        return False

def get_node_health(node):
    """
    Controlla le condizioni del nodo (Ready/NotReady).
    Restituisce 'ONLINE' se Ready=True, altrimenti 'OFFLINE'.
    """
    if not node.status.conditions:
        return "UNKNOWN"
    
    for condition in node.status.conditions:
        if condition.type == "Ready":
            return "ONLINE" if condition.status == "True" else "OFFLINE"
            
    return "UNKNOWN"

def update_satellite_physics(node_name, current_time, is_active):
    if node_name not in satellites_state:
        offset = len(satellites_state) * (ORBIT_DURATION / 2)
        satellites_state[node_name] = { "battery": 50.0, "status": "OPERATIONAL", "offset": offset }

    sat = satellites_state[node_name]
    phase = get_orbital_phase(current_time, sat['offset'])
    
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

    return int(sat['battery']), sat['status'], phase

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
        try:
            current_time = time.time()
            nodes = v1.list_node().items
            
            # Dizionario per raccogliere tutti i dati da mandare alla GUI
            fleet_telemetry = {}

            print(f"\n⏱️  Ciclo: {int(current_time % ORBIT_DURATION)}/{ORBIT_DURATION}s")

            for node in nodes:
                node_name = node.metadata.name
                if "control-plane" in node_name: continue
                
                is_working = is_node_working(v1, node_name)
                batt, status, phase = update_satellite_physics(node_name, current_time, is_working)

                health = get_node_health(node)
                
                # Icone Terminale
                weather_icon = "☀️" if phase == "SUN" else "🌑"
                load_icon = "⚙️" if is_working else "💤"
                print(f"   🛰️  {node_name}: {weather_icon} | {load_icon} | 🔋 {batt}%")

                # 1. Aggiorna Kubernetes (Labels)
                body = { "metadata": { "labels": { LABEL_BATTERY: str(batt), LABEL_STATUS: status } } }
                v1.patch_node(node_name, body)

                # 2. Prepara dati per la Dashboard
                fleet_telemetry[node_name] = {
                    "battery": batt,
                    "phase": phase,      # SUN / ECLIPSE
                    "load": "WORKING" if is_working else "IDLE",
                    "status": status,    # OPERATIONAL / CRITICAL
                    "health": health
                }

            # 3. Scrivi Telemetria Completa su Redis
            r.set("constellation_telemetry", json.dumps(fleet_telemetry))
            
            time.sleep(UPDATE_INTERVAL)
            
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Errore ciclo: {e}")
            time.sleep(UPDATE_INTERVAL)

if __name__ == "__main__":
    main()