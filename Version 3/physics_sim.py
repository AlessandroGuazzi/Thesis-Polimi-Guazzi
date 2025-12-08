import time
import json
import redis
from kubernetes import client, config, stream

# =============================================================================
# CONFIGURAZIONE FISICA V3 (Role Aware)
# =============================================================================
LABEL_BATTERY = "spacecloud.io/battery_level"
LABEL_STATUS = "spacecloud.io/power_status"

ORBIT_DURATION = 60
SUN_DURATION = 30
UPDATE_INTERVAL = 2.0 

# Tassi Energetici
CHARGE_RATE_ACTIVE = 1.5
CHARGE_RATE_IDLE = 5.0

DISCHARGE_RATE_IDLE = 0.5
DISCHARGE_RATE_DASHBOARD = 2.0 
DISCHARGE_RATE_DB_REPLICA = 1.0 
DISCHARGE_RATE_DB_MASTER = 2.5 

satellites_state = {}

def get_redis_role(v1, pod_name):
    try:
        resp = stream.stream(v1.connect_get_namespaced_pod_exec,
            name=pod_name,
            namespace="default",
            command=["redis-cli", "role"],
            stderr=True, stdin=False, stdout=True, tty=False
        )
        if "master" in resp: return "MASTER"
        if "slave" in resp: return "REPLICA"
    except:
        pass
    return "UNKNOWN"

def get_orbital_data(timestamp, offset):
    cycle_time = (timestamp + offset) % ORBIT_DURATION
    phase = "SUN" if cycle_time < SUN_DURATION else "ECLIPSE"
    progress = cycle_time / ORBIT_DURATION
    return phase, progress

def get_node_load_info(v1, node_name):
    try:
        pods = v1.list_namespaced_pod(namespace="default", field_selector=f"spec.nodeName={node_name}")
        has_dashboard = False
        redis_role = None

        for item in pods.items:
            if item.metadata.deletion_timestamp: continue
            labels = item.metadata.labels or {}
            app = labels.get('app')

            if app == "space-app":
                has_dashboard = True
            elif app == "redis":
                redis_role = get_redis_role(v1, item.metadata.name)

        if has_dashboard:
            return "COMPUTE", "DASHBOARD"
        elif redis_role:
            return "MEMORY", redis_role
        else:
            return "IDLE", "STANDBY"
    except:
        return "IDLE", "UNKNOWN"

def update_satellite_physics(node_name, current_time, load_type, load_role):
    if node_name not in satellites_state:
        offset = len(satellites_state) * (ORBIT_DURATION / 3)
        satellites_state[node_name] = { "battery": 60.0, "status": "OPERATIONAL", "offset": offset }

    sat = satellites_state[node_name]
    phase, _ = get_orbital_data(current_time, sat['offset'])
    
    discharge = DISCHARGE_RATE_IDLE
    if load_type == "COMPUTE":
        discharge = DISCHARGE_RATE_DASHBOARD
    elif load_type == "MEMORY":
        if load_role == "MASTER":
            discharge = DISCHARGE_RATE_DB_MASTER
        else:
            discharge = DISCHARGE_RATE_DB_REPLICA

    delta = 0
    if phase == "SUN":
        delta = CHARGE_RATE_ACTIVE if load_type != "IDLE" else CHARGE_RATE_IDLE
    else:
        delta = -discharge

    sat['battery'] = max(0, min(100, sat['battery'] + (delta * UPDATE_INTERVAL)))
    progress = (current_time + sat['offset']) % ORBIT_DURATION / ORBIT_DURATION
    
    return int(sat['battery']), sat['status'], phase, progress

def main():
    print("--- 🌍 Physics Engine V3.2 (K8s Sync Enabled) ---")
    print(f"Rates: MASTER={DISCHARGE_RATE_DB_MASTER}/s | REPLICA={DISCHARGE_RATE_DB_REPLICA}/s")
    
    config.load_kube_config()
    v1 = client.CoreV1Api()
    
    try:
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    except:
        r = None

    while True:
        loop_start = time.time()
        try:
            nodes = v1.list_node().items
            fleet_telemetry = {}

            for node in nodes:
                name = node.metadata.name
                
                # 1. Calcolo Dati
                load_type, load_role = get_node_load_info(v1, name)
                batt, status, phase, progress = update_satellite_physics(name, loop_start, load_type, load_role)
                
                # 2. Patch Kubernetes (IMPORTANTE!)
                # Senza questo, Scheduler e Watchdog sono ciechi.
                try:
                    body = {
                        "metadata": {
                            "labels": {
                                LABEL_BATTERY: str(batt),
                                LABEL_STATUS: status
                            }
                        }
                    }
                    v1.patch_node(name, body)
                except Exception as e:
                    print(f"⚠️ Errore patch nodo {name}: {e}")

                # 3. Telemetria UI
                fleet_telemetry[name] = {
                    "battery": batt,
                    "phase": phase,
                    "orbit_pos": progress,
                    "load": load_type,
                    "role": load_role,
                    "status": status,
                    "health": "ONLINE"
                }

            if r:
                try:
                    r.set("constellation_telemetry", json.dumps(fleet_telemetry))
                except: pass # Ignora errori redis temporanei
            
            # Debug Log
            masters = [n for n, d in fleet_telemetry.items() if d.get('role') == 'MASTER']
            print(f"⏱️  Sync OK. Master DB: {masters[0] if masters else 'Searching...'}")

            elapsed = time.time() - loop_start
            time.sleep(max(0, UPDATE_INTERVAL - elapsed))
            
        except Exception as e:
            print(f"Errore ciclo: {e}")
            time.sleep(UPDATE_INTERVAL)

if __name__ == "__main__":
    main()