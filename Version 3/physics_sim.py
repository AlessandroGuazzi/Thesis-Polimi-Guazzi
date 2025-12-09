import time
import json
import redis
import subprocess
import shutil
import os
import sys
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

# --- UTILS GRAFICHE ---
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def draw_battery_bar(percentage):
    bar_len = 10
    filled_len = int(round(bar_len * percentage / 100))
    
    # Colore in base alla carica
    color = Colors.GREEN
    if percentage < 40: color = Colors.WARNING
    if percentage < 20: color = Colors.FAIL
    
    bar = '█' * filled_len + '░' * (bar_len - filled_len)
    return f"{color}[{bar}] {percentage:>3}%{Colors.ENDC}"

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
            cmd = f"kubectl exec {pod} -c sentinel -- redis-cli -p 26379 sentinel get-master-addr-by-name mymaster"
            out = subprocess.check_output(cmd, shell=True, stderr=subprocess.DEVNULL, timeout=2).decode()
            if "10." in out: 
                lines = out.replace('\r', '').split('\n')
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
        found_pod_name = None 

        for item in pods.items:
            if item.metadata.deletion_timestamp: continue
            labels = item.metadata.labels or {}
            app = labels.get('app')

            if app == "space-app":
                has_dashboard = True
            elif app == "redis":
                found_pod_name = item.metadata.name 
                role = get_redis_role(item.metadata.name)
                if role != "UNKNOWN":
                    redis_role = role
                    redis_ip = item.status.pod_ip 

        if redis_role == "MASTER":
            return "MEMORY", "MASTER", redis_ip, found_pod_name 
        elif has_dashboard:
            return "COMPUTE", "DASHBOARD", None, None
        elif redis_role == "REPLICA":
            return "MEMORY", "REPLICA", redis_ip, found_pod_name 
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
    """
    if not master_pod or master_pod == "Searching..." or master_pod == "ELECTION...":
        return

    try:
        cmd = f"kubectl exec -i {master_pod} -c redis -- redis-cli -x set constellation_telemetry"
        process = subprocess.Popen(
            cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE
        )
        json_bytes = json.dumps(telemetry_data).encode('utf-8')
        stdout, stderr = process.communicate(input=json_bytes)
        
        # Gestione errori silenziosa per non sporcare la UI
        if process.returncode != 0:
             pass # Gli errori verranno catturati dal watchdog o dalla logica di retry

    except Exception as e:
        pass

def print_dashboard(telemetry, final_master, master_pod_name, elapsed_time, system_msg=""):
    clear_screen()
    print(f"{Colors.HEADER}================================================================{Colors.ENDC}")
    print(f"{Colors.HEADER}   🌍  SPACE CLOUD PHYSICS ENGINE v3.6 - ORBITAL TELEMETRY   {Colors.ENDC}")
    print(f"{Colors.HEADER}================================================================{Colors.ENDC}")
    print("")
    
    # STATUS HEADER
    master_display = f"{Colors.GREEN}{final_master} ({master_pod_name}){Colors.ENDC}" if master_pod_name else f"{Colors.WARNING}{final_master}{Colors.ENDC}"
    if final_master == "Searching..." or final_master == "ELECTION...":
        master_display = f"{Colors.FAIL}⚠️  ELECTION IN PROGRESS{Colors.ENDC}"

    print(f"   👑  {Colors.BOLD}DB MASTER NODE:{Colors.ENDC}   {master_display}")
    print(f"   ⏱️  {Colors.BOLD}SIMULATION CYCLE:{Colors.ENDC} {elapsed_time:.2f}s")
    print("")

    # TABLE HEADER
    print(f"{Colors.CYAN}{'NODE NAME':<25} {'BATTERY':<20} {'PHASE':<10} {'LOAD':<10} {'ROLE':<10}{Colors.ENDC}")
    print(f"{Colors.BLUE}{'-'*80}{Colors.ENDC}")

    # TABLE ROWS
    sorted_nodes = sorted(telemetry.keys())
    for node in sorted_nodes:
        data = telemetry[node]
        
        # Icone Phase
        phase_icon = "☀️ SUN" if data['phase'] == "SUN" else "🌑 ECLIPSE"
        phase_str = f"{Colors.WARNING if data['phase'] == 'SUN' else Colors.BLUE}{phase_icon:<10}{Colors.ENDC}"
        
        # Load Color
        load_color = Colors.ENDC
        if data['load'] == "COMPUTE": load_color = Colors.CYAN
        elif data['load'] == "MEMORY": load_color = Colors.HEADER
        
        # Role Color
        role_str = data['role']
        if data['role'] == "MASTER": role_str = f"{Colors.BOLD}{Colors.WARNING}👑 MASTER{Colors.ENDC}"
        elif data['role'] == "REPLICA": role_str = f"{Colors.BLUE}🔹 REPLICA{Colors.ENDC}"
        elif data['role'] == "DASHBOARD": role_str = f"{Colors.GREEN}💻 DASHBOARD{Colors.ENDC}"
        
        batt_bar = draw_battery_bar(data['battery'])
        
        print(f"{node:<25} {batt_bar:<20} {phase_str} {load_color}{data['load']:<10}{Colors.ENDC} {role_str}")

    print(f"{Colors.BLUE}{'-'*80}{Colors.ENDC}")
    
    # SYSTEM LOG
    if system_msg:
        print(f"\n[SYSTEM EVENT]: {system_msg}")
    else:
        print(f"\n{Colors.GREEN}[SYSTEM]: Nominal Operation.{Colors.ENDC}")

def main():
    if not check_requirements(): return
    config.load_kube_config()
    v1 = client.CoreV1Api()
    
    last_system_msg = ""
    system_msg_timer = 0

    while True:
        loop_start = time.time()
        try:
            nodes = v1.list_node().items
            fleet_telemetry = {}
            master_candidates = [] 
            node_redis_ips = {}    
            master_pod_map = {}

            # --- RACCOLTA DATI ---
            for node in nodes:
                name = node.metadata.name
                load_type, load_role, pod_ip, pod_name = get_node_load_info(v1, name)
                
                if load_role == "MASTER":
                    master_candidates.append(name)
                    if pod_name: master_pod_map[name] = pod_name
                
                if pod_ip: node_redis_ips[name] = pod_ip

                batt, status, phase, progress = update_satellite_physics(name, loop_start, load_type, load_role)
                
                try:
                    body = {"metadata": {"labels": {LABEL_BATTERY: str(batt), LABEL_STATUS: status}}}
                    v1.patch_node(name, body)
                except: pass

                fleet_telemetry[name] = {
                    "battery": batt,
                    "phase": phase,
                    "orbit_pos": progress,
                    "load": load_type,
                    "role": load_role,
                    "status": status,
                    "health": "ONLINE"
                }

            # --- CONSENSUS CHECK ---
            final_master = "Searching..."
            current_msg = ""

            if len(master_candidates) > 1:
                current_msg = f"{Colors.FAIL}⚠️  SPLIT BRAIN DETECTED: {master_candidates}{Colors.ENDC}"
                true_master_ip = get_sentinel_consensus()
                
                if true_master_ip:
                    current_msg = f"{Colors.WARNING}⚖️  SENTINEL RESOLUTION: True Master is {true_master_ip}{Colors.ENDC}"
                    for node in master_candidates:
                        node_ip = node_redis_ips.get(node)
                        if node_ip == true_master_ip:
                            final_master = node 
                        else:
                            fleet_telemetry[node]['role'] = 'REPLICA'
                            fleet_telemetry[node]['status'] = 'SYNCING' 
                else:
                    final_master = "ELECTION..."
            elif len(master_candidates) == 1:
                final_master = master_candidates[0]

            # Gestione persistenza messaggi di errore
            if current_msg:
                last_system_msg = current_msg
                system_msg_timer = 5 # Mostra il messaggio per 5 cicli
            elif system_msg_timer > 0:
                system_msg_timer -= 1
            else:
                last_system_msg = ""

            # --- INIEZIONE DATI ---
            target_pod_display = None
            if final_master not in ["Searching...", "ELECTION...", None]:
                target_pod = master_pod_map.get(final_master)
                target_pod_display = target_pod
                if target_pod:
                    inject_telemetry_to_master(target_pod, fleet_telemetry)
            
            elapsed = time.time() - loop_start
            
            # --- VISUALIZZAZIONE ---
            print_dashboard(fleet_telemetry, final_master, target_pod_display, elapsed, last_system_msg)

            time.sleep(max(0, UPDATE_INTERVAL - elapsed))
            
        except KeyboardInterrupt:
            print("\n🛑 Simulazione terminata.")
            break
        except Exception as e:
            # In caso di crash, stampiamo l'errore senza pulire lo schermo per poterlo leggere
            print(f"❌ Errore ciclo: {e}")
            time.sleep(UPDATE_INTERVAL)

if __name__ == "__main__":
    main()