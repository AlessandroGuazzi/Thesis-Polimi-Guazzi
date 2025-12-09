import time
import os
import sys
import datetime
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException

SCHEDULER_NAME = "space-scheduler"
BATTERY_LABEL = "spacecloud.io/battery_level"
MIN_BATTERY = 20

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

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def get_timestamp():
    return datetime.datetime.now().strftime("%H:%M:%S")

def print_header():
    clear_screen()
    print(f"{Colors.HEADER}╔════════════════════════════════════════════════════════════════════╗{Colors.ENDC}")
    print(f"{Colors.HEADER}║      🛰️   SPACE CLOUD SCHEDULER (Decision Maker AI)               ║{Colors.ENDC}")
    print(f"{Colors.HEADER}╚════════════════════════════════════════════════════════════════════╝{Colors.ENDC}")
    print(f"{Colors.BOLD} STATUS: {Colors.GREEN}ACTIVE{Colors.ENDC} | {Colors.BOLD}STRATEGY:{Colors.ENDC} {Colors.CYAN}Energy-Aware + Anti-Affinity{Colors.ENDC}")
    print(f"{Colors.BLUE}{'-'*70}{Colors.ENDC}")
    print("")

def print_scanning_line():
    sys.stdout.write(f"\r{Colors.CYAN}📡 Scanning for Pending Pods...{Colors.ENDC}")
    sys.stdout.flush()

def get_node_battery(node):
    labels = node.metadata.labels
    if labels and BATTERY_LABEL in labels:
        return int(labels[BATTERY_LABEL])
    return 0

def check_collision(v1, node_name, app_label):
    """Verifica se esiste già un pod dello stesso tipo sul nodo."""
    if not app_label: return False
    try:
        field_selector = f"spec.nodeName={node_name}"
        label_selector = f"app={app_label}"
        pods = v1.list_namespaced_pod("default", field_selector=field_selector, label_selector=label_selector)
        for p in pods.items:
            if not p.metadata.deletion_timestamp:
                return True
    except:
        pass
    return False

def schedule_pod(v1, name, node, namespace="default"):
    target = client.V1ObjectReference(kind="Node", api_version="v1", name=node)
    meta = client.V1ObjectMeta(name=name)
    body = client.V1Binding(api_version="v1", kind="Binding", metadata=meta, target=target)
    try:
        v1.create_namespaced_binding(namespace=namespace, body=body, _preload_content=False)
        return True
    except ApiException as e:
        if e.status == 409: return True
        return False
    except:
        return False

def print_decision_process(pod_name, app_label, candidates_log, winner):
    print(f"\n{Colors.BOLD}🚀 NEW REQUEST DETECTED:{Colors.ENDC} {Colors.WARNING}{pod_name}{Colors.ENDC} (App: {app_label})")
    print(f"{'NODE CANDIDATE':<20} {'BATTERY':<10} {'STATUS':<20} {'REASON'}")
    print(f"{'-'*60}")
    
    for log in candidates_log:
        node, batt, status, reason = log
        
        status_color = Colors.GREEN
        if status == "REJECTED": status_color = Colors.FAIL
        elif status == "WINNER": status_color = Colors.HEADER

        batt_str = f"{batt}%"
        if batt < MIN_BATTERY: batt_str = f"{Colors.FAIL}{batt}%{Colors.ENDC}"
        
        print(f"{node:<20} {batt_str:<20} {status_color}{status:<10}{Colors.ENDC} {reason}")
    
    print(f"{'-'*60}")
    if winner:
        print(f"🎯 {Colors.BOLD}DECISION:{Colors.ENDC} Assigned to {Colors.GREEN}{winner}{Colors.ENDC}")
    else:
        print(f"⛔ {Colors.BOLD}DECISION:{Colors.ENDC} {Colors.FAIL}SCHEDULING FAILED (No suitable nodes){Colors.ENDC}")
    print("\n")

def main():
    print_header()
    config.load_kube_config()
    v1 = client.CoreV1Api()
    w = watch.Watch()
    
    while True:
        try:
            print_scanning_line()
            
            for event in w.stream(v1.list_namespaced_pod, "default", timeout_seconds=2):
                pod = event['object']
                
                if pod.status.phase == "Pending" and \
                   pod.spec.scheduler_name == SCHEDULER_NAME and \
                   pod.spec.node_name is None:
                    
                    # Cancelliamo la riga di scanning per stampare il report
                    sys.stdout.write("\r" + " " * 50 + "\r")
                    
                    pod_name = pod.metadata.name
                    app_label = pod.metadata.labels.get('app')
                    
                    all_nodes = v1.list_node().items
                    candidates = [] 
                    decision_log = [] # Per la stampa finale
                    
                    for node in all_nodes:
                        node_name = node.metadata.name
                        
                        # Check Salute
                        is_ready = False
                        for condition in node.status.conditions:
                            if condition.type == "Ready" and condition.status == "True":
                                is_ready = True
                                break
                        
                        if not is_ready:
                            decision_log.append((node_name, 0, "REJECTED", "Node Not Ready"))
                            continue 

                        battery = get_node_battery(node)
                        
                        # LOGICA ANTI-COLLISIONE
                        has_collision = False
                        if app_label == "redis":
                            has_collision = check_collision(v1, node_name, app_label)
                        
                        if battery < MIN_BATTERY:
                            decision_log.append((node_name, battery, "REJECTED", "Low Battery"))
                        elif has_collision:
                            decision_log.append((node_name, battery, "REJECTED", "Anti-Affinity Collision"))
                        else:
                            candidates.append((node_name, battery))
                            decision_log.append((node_name, battery, "CANDIDATE", "Valid"))
                    
                    if not candidates:
                        print_decision_process(pod_name, app_label, decision_log, None)
                        continue

                    # Ordiniamo per Batteria
                    candidates.sort(key=lambda x: x[1], reverse=True)
                    best_node = candidates[0][0]
                    
                    # Aggiorniamo il log per mostrare il vincitore
                    final_log = []
                    for item in decision_log:
                        n, b, s, r = item
                        if n == best_node:
                            final_log.append((n, b, "WINNER", "Best Energy Score"))
                        else:
                            final_log.append(item)

                    schedule_pod(v1, pod_name, best_node)
                    print_decision_process(pod_name, app_label, final_log, best_node)
                    
                    # Breve pausa per leggere
                    time.sleep(2)
                    print_scanning_line()

        except KeyboardInterrupt:
            print(f"\n{Colors.WARNING}🛑 Scheduler terminato.{Colors.ENDC}")
            break
        except Exception as e:
            # print(f"Riavvio Watcher... {e}") # Debug silenzioso
            time.sleep(1)

if __name__ == "__main__":
    main()