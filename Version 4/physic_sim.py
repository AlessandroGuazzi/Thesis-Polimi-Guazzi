import time
import math
import json
import redis
from kubernetes import client, config

# =============================================================================
#  SPACE CLOUD PHYSICS ENGINE V2.3 (Stable & Interruptible)
# =============================================================================

# --- 1. PARAMETRI ORBITALI ---
ORBIT_PERIOD = 60.0       
ECLIPSE_START = 180       
ECLIPSE_END = 240         

# --- 2. PARAMETRI TERMODINAMICI ---
COOLING_RATE = 0.05       
HEATING_SUN = 0.8         
HEATING_CPU_IDLE = 0.1    
HEATING_CPU_LOAD = 2.5    
TEMP_SPACE = -20.0        

# --- 3. PARAMETRI BATTERIA ---
CHARGE_RATE = 4.0         
DRAIN_IDLE = 0.5          
DRAIN_LOAD = 2.0          

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

try:
    config.load_kube_config()
    v1 = client.CoreV1Api()
    print("✅ Connesso al Cluster Kubernetes: space-cloud")
except Exception as e:
    print(f"❌ Errore connessione K8s: {e}")
    exit(1)

# STATO INIZIALE
satellites = {
    "space-cloud-control-plane": { "battery": 100.0, "temp": 22.0, "offset": 0.0 },
    "space-cloud-worker":        { "battery": 100.0, "temp": 20.0, "offset": 20.0 },
    "space-cloud-worker2":       { "battery": 100.0, "temp": 20.0, "offset": 40.0 }
}

# Cache per evitare chiamate API inutili (Smart Update)
last_k8s_update = {} 

def get_active_pods_map():
    """
    Legge TUTTI i pod in una sola chiamata API.
    """
    active_nodes = set()
    try:
        pods = v1.list_namespaced_pod("default", label_selector="app=space-app")
        for pod in pods.items:
            if pod.status.phase == "Running" and not pod.metadata.deletion_timestamp:
                if pod.spec.node_name:
                    active_nodes.add(pod.spec.node_name)
    except Exception: # MODIFICATO: Cattura solo errori, non CTRL+C
        pass 
    return active_nodes

def calculate_physics(sat_state, current_timestamp, is_working):
    # 1. Posizione
    time_in_orbit = (current_timestamp + sat_state['offset']) % ORBIT_PERIOD
    angle = (time_in_orbit / ORBIT_PERIOD) * 360.0
    
    # 2. Eclissi
    in_eclipse = (angle >= ECLIPSE_START and angle <= ECLIPSE_END)
    
    # 3. Temperatura
    heat_in = HEATING_CPU_LOAD if is_working else HEATING_CPU_IDLE
    if not in_eclipse:
        heat_in += HEATING_SUN
    
    delta_temp = heat_in - (COOLING_RATE * (sat_state['temp'] - TEMP_SPACE))
    sat_state['temp'] += delta_temp
    
    # 4. Batteria
    if in_eclipse:
        drain = DRAIN_LOAD if is_working else DRAIN_IDLE
        sat_state['battery'] -= drain
    else:
        drain = DRAIN_LOAD if is_working else DRAIN_IDLE
        sat_state['battery'] += (CHARGE_RATE - drain)
    
    sat_state['battery'] = max(0.0, min(100.0, sat_state['battery']))
    
    return angle, in_eclipse

def smart_update_k8s(node_name, batt, temp, eclipse):
    """
    Aggiorna K8s SOLO se i dati sono cambiati significativamente.
    """
    global last_k8s_update
    
    last_state = last_k8s_update.get(node_name, {"batt": -1, "temp": -1, "eclipse": None})
    
    diff_batt = abs(batt - last_state["batt"])
    diff_temp = abs(temp - last_state["temp"])
    eclipse_changed = (eclipse != last_state["eclipse"])
    
    if diff_batt > 1.0 or diff_temp > 0.5 or eclipse_changed:
        try:
            body = {
                "metadata": {
                    "labels": {
                        "sat/battery": str(int(batt)),
                        "sat/temp": str(int(temp)),
                        "sat/eclipse": "true" if eclipse else "false"
                    }
                }
            }
            v1.patch_node(node_name, body)
            last_k8s_update[node_name] = {"batt": batt, "temp": temp, "eclipse": eclipse}
            return True 
        except Exception: # MODIFICATO: Cattura solo errori
            return False
    return False 

def main():
    print("--- 🛰️  SPACE CLOUD V2.3: PHYSICS ENGINE ---")
    print("Premi CTRL+C per terminare.")
    
    try:
        while True:
            start_time = time.time()
            
            # 1. Lettura Batch
            active_nodes_map = get_active_pods_map()
            
            telemetry_batch = {} 
            print("\nStatus: ", end="")

            for node_name, state in satellites.items():
                is_working = (node_name in active_nodes_map)
                
                # Calcoli
                angle, is_eclipse = calculate_physics(state, start_time, is_working)
                
                # Scrittura Smart
                smart_update_k8s(node_name, state['battery'], state['temp'], is_eclipse)
                
                status_str = "PROCESSING" if is_working else "IDLE"
                if is_eclipse: status_str += " (ECLIPSE)"
                
                telemetry_batch[node_name] = {
                    "battery": round(state['battery'], 1),
                    "temp": round(state['temp'], 1),
                    "angle": int(angle),
                    "eclipse": is_eclipse,
                    "status": status_str
                }
                
                # Log Console
                icon = "🌑" if is_eclipse else "☀️ "
                short_name = "M" if "control" in node_name else ("W1" if "worker" == node_name[-6:] else "W2")
                
                # Formattazione con 1 decimale per temperatura
                print(f"[{short_name} {icon} B:{state['battery']:.0f}% T:{state['temp']:.1f}°]", end=" ")
            
            # Redis
            try:
                redis_client.set("fleet_telemetry", json.dumps(telemetry_batch))
            except Exception: # MODIFICATO
                pass
            
            elapsed = time.time() - start_time
            sleep_time = max(0.0, 1.0 - elapsed)
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\n\n🛑 Atterraggio completato. Simulazione terminata.")
        exit(0)

if __name__ == "__main__":
    main()