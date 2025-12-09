import time
import json
import redis
import subprocess
import shutil
from kubernetes import client, config

# =============================================================================
# CONFIGURAZIONE FISICA V3.6 (Consensus Aware)
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

def check_requirements():
    path = shutil.which("kubectl")
    if not path:
        print("❌ ERRORE CRITICO: Python non trova 'kubectl'.")
        return False
    return True

def get_redis_role(pod_name):
    """Chiede al singolo pod chi crede di essere."""
    try:
        cmd = f"kubectl exec {pod_name} -c redis -- redis-cli info replication"
        result = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, timeout=5)
        output = result.decode('utf-8', errors='ignore').lower()
        
        if "role:master" in output: return "MASTER"
        if "role:slave" in output or "role:replica" in output: return "REPLICA"
    except:
        pass
    return "UNKNOWN"

def get_sentinel_consensus():
    """
    Chiede al QUORUM (Sentinel) chi è il vero master.
    Risolve i casi di Split-Brain dove due nodi si credono master.
    """
    for i in range(3):
        pod = f"satellite-memory-{i}"
        try:
            # Chiediamo a Sentinel l'IP del master attuale
            cmd = f"kubectl exec {pod} -c sentinel -- redis-cli -p 26379 sentinel get-master-addr-by-name mymaster"
            out = subprocess.check_output(cmd, shell=True, stderr=subprocess.DEVNULL, timeout=2).decode()
            
            # Sentinel risponde con:
            # 1) "10.244.X.X"
            # 2) "6379"
            if "10." in out: # Controllo grezzo se c'è un IP
                lines = out.replace('\r', '').split('\n')
                # Puliamo l'output per trovare l'IP
                for line in lines:
                    if "10." in line:
                        return line.strip().replace('"', '')
        except: continue
    return None

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
        redis_ip = None
        found_pod_name = None  # <--- NUOVO: Salviamo il nome del pod

        for item in pods.items:
            if item.metadata.deletion_timestamp: continue
            labels = item.metadata.labels or {}
            app = labels.get('app')

            if app == "space-app":
                has_dashboard = True
            elif app == "redis":
                # Salviamo il nome del pod Redis se lo troviamo
                found_pod_name = item.metadata.name 
                role = get_redis_role(item.metadata.name)
                if role != "UNKNOWN":
                    redis_role = role
                    redis_ip = item.status.pod_ip 

        # Logica di Priorità
        if redis_role == "MASTER":
            return "MEMORY", "MASTER", redis_ip, found_pod_name # <--- RITORNA POD NAME
        elif has_dashboard:
            return "COMPUTE", "DASHBOARD", None, None
        elif redis_role == "REPLICA":
            return "MEMORY", "REPLICA", redis_ip, found_pod_name # <--- RITORNA POD NAME
        else:
            return "IDLE", "STANDBY", None, None
            
    except Exception as e:
        return "IDLE", "UNKNOWN", None, None

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

def inject_telemetry_to_master(master_pod, telemetry_data):
    """
    Scrive nel Master Pod usando PIPE (stdin).
    Molto più sicuro per evitare problemi di virgolette col JSON su Windows.
    """
    if not master_pod or master_pod == "Searching..." or master_pod == "ELECTION...":
        return

    try:
        # Prepariamo il comando per leggere da stdin (-x in redis-cli legge il valore da stdin)
        # Nota: usiamo '-i' in kubectl per abilitare stdin
        cmd = f"kubectl exec -i {master_pod} -c redis -- redis-cli -x set constellation_telemetry"
        
        # Avviamo il processo aprendo una 'pipe' per passargli i dati
        process = subprocess.Popen(
            cmd, 
            shell=True, 
            stdin=subprocess.PIPE, 
            stdout=subprocess.DEVNULL, 
            stderr=subprocess.PIPE
        )
        
        # Inviamo il JSON puro (senza escape manuali!)
        json_bytes = json.dumps(telemetry_data).encode('utf-8')
        stdout, stderr = process.communicate(input=json_bytes)
        
        if process.returncode != 0:
             print(f"⚠️ Errore PIPE Redis: {stderr.decode()}")

    except Exception as e:
        print(f"⚠️ Exception scrittura DB: {e}")

def main():
    print("--- 🌍 Physics Engine V3.6 (Consensus Aware) ---")
    
    if not check_requirements(): return
    config.load_kube_config()
    v1 = client.CoreV1Api()
    
    try: r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    except: r = None

    print("\n[INFO] Simulazione avviata. Protocollo anti-Split-Brain attivo.\n")

    while True:
        loop_start = time.time()
        try:
            nodes = v1.list_node().items
            fleet_telemetry = {}
            
            # Strutture temporanee per il Consensus Check
            master_candidates = [] # Lista di nodi che dicono "Sono Master"
            node_redis_ips = {}    # Mappa NomeNodo -> IP Redis

            # Mappa per ricordarci "NomeNodo" -> "NomePod"
            master_pod_map = {}

            for node in nodes:
                name = node.metadata.name
                
                # 1. Rilevamento Ruolo (Locale al nodo)
                load_type, load_role, pod_ip, pod_name = get_node_load_info(v1, name)
                
                if load_role == "MASTER":
                    master_candidates.append(name)
                    # Se è master, salviamo il nome del suo pod (es. satellite-memory-0)
                    if pod_name:
                        master_pod_map[name] = pod_name
                
                if pod_ip:
                    node_redis_ips[name] = pod_ip

                # 2. Aggiornamento Fisica
                batt, status, phase, progress = update_satellite_physics(name, loop_start, load_type, load_role)
                
                # 3. Patch Kubernetes (Silenziosa)
                try:
                    body = {"metadata": {"labels": {LABEL_BATTERY: str(batt), LABEL_STATUS: status}}}
                    v1.patch_node(name, body)
                except: pass

                # 4. Costruzione Dati UI (Provvisori)
                fleet_telemetry[name] = {
                    "battery": batt,
                    "phase": phase,
                    "orbit_pos": progress,
                    "load": load_type,
                    "role": load_role,
                    "status": status,
                    "health": "ONLINE"
                }

            # --- CONSENSUS CHECK (Anti Split-Brain) ---
            # Se più di un nodo si crede Master, chiediamo a Sentinel chi ha ragione
            final_master = "Searching..."
            
            if len(master_candidates) > 1:
                print(f"⚠️  SPLIT BRAIN RILEVATO: {master_candidates} dicono di essere Master.")
                true_master_ip = get_sentinel_consensus()
                
                if true_master_ip:
                    print(f"⚖️  SENTINEL HA PARLATO: Il vero master è IP {true_master_ip}")
                    # Correggiamo la telemetria
                    for node in master_candidates:
                        node_ip = node_redis_ips.get(node)
                        if node_ip == true_master_ip:
                            final_master = node # Lui è il vero Re
                        else:
                            # Lui è un usurpatore (o un vecchio master morente)
                            # Lo declassiamo visivamente per l'utente
                            fleet_telemetry[node]['role'] = 'REPLICA'
                            fleet_telemetry[node]['status'] = 'SYNCING' 
                else:
                    final_master = "ELECTION..."
            elif len(master_candidates) == 1:
                final_master = master_candidates[0]

            # DEBUG: Vediamo quanti nodi stiamo provando a inviare
            node_count = len(fleet_telemetry)
            print(f"DEBUG: Trovati {node_count} nodi in telemetria.")

            # 5. Invio dati corretti alla UI
            # Iniettiamo i dati direttamente nel pod che abbiamo identificato come Master.
            if final_master not in ["Searching...", "ELECTION...", None]:
                target_pod = master_pod_map.get(final_master)
                
                if target_pod:
                    inject_telemetry_to_master(target_pod, fleet_telemetry)
                    final_master_display = f"{final_master} ({target_pod})" # Solo per log
                else:
                    print(f"⚠️ Trovato master node {final_master} ma nessun pod name!")
            
            elapsed = time.time() - loop_start
            
            # Feedback visuale dello stato
            status_symbol = "🟢" if final_master not in ["Searching...", "ELECTION..."] else "🔴"
            print(f"{status_symbol} Sync OK. Master DB: {final_master} | Cycle: {elapsed:.2f}s")

            time.sleep(max(0, UPDATE_INTERVAL - elapsed))
            
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"❌ Errore ciclo: {e}")
            time.sleep(UPDATE_INTERVAL)

if __name__ == "__main__":
    main()