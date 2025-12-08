import time
import redis
from kubernetes import client, config, watch

# =============================================================================
# CONFIGURAZIONE COMPLETA (BATTERIA + HARDWARE)
# =============================================================================

DEPLOYMENT_NAME = "missione-web"
NAMESPACE = "default"
APP_LABEL = "space-app"

LABEL_BATTERY = "spacecloud.io/battery_level"

# Soglie Energetiche
THRESHOLD_SHUTDOWN = 20  # Isteresi OFF
THRESHOLD_RESTART = 40   # Isteresi ON
THRESHOLD_EVICT = 20     # Sfratto locale

# Setup Redis Client
try:
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
except:
    r = None
    print("⚠️ Redis non disponibile per il Watchdog")

def update_dashboard_status(replicas, status_msg):
    """Scrive lo stato della missione su Redis per la GUI"""
    if r:
        try:
            r.set("mission_replicas", replicas)
            r.set("mission_status_text", status_msg)
        except:
            pass

# =============================================================================
# FUNZIONI DI DIAGNOSTICA
# =============================================================================

def is_node_healthy(node):
    """
    Controlla se il nodo è vivo (Ready=True).
    Restituisce False se è guasto (NotReady) o sconosciuto.
    """
    if not node.status.conditions:
        return False
    
    for cond in node.status.conditions:
        if cond.type == "Ready":
            return cond.status == "True"
    return False

def get_node_battery(v1, node_name):
    """Legge la batteria. Restituisce 0 se il nodo non esiste o è illeggibile."""
    try:
        node = v1.read_node(node_name)
        # Se il nodo è fisicamente morto, la sua batteria vale 0 per noi
        if not is_node_healthy(node):
            return 0
            
        labels = node.metadata.labels
        if labels and LABEL_BATTERY in labels:
            return int(labels[LABEL_BATTERY])
    except:
        return 0
    return 0

def get_max_fleet_battery(v1):
    """
    Trova la batteria massima SOLO tra i nodi SANI.
    Se un nodo è al 100% ma è guasto (NotReady), viene ignorato.
    """
    nodes = v1.list_node().items
    max_batt = 0
    healthy_nodes_count = 0

    for node in nodes:
        if "control-plane" in node.metadata.name: continue
        
        # Ignora nodi guasti per il calcolo della capacità della flotta
        if not is_node_healthy(node):
            continue

        healthy_nodes_count += 1
        val = int(node.metadata.labels.get(LABEL_BATTERY, 0))
        if val > max_batt: max_batt = val
        
    return max_batt, healthy_nodes_count

def get_current_replicas(apps_v1):
    try:
        dep = apps_v1.read_namespaced_deployment(DEPLOYMENT_NAME, NAMESPACE)
        return dep.spec.replicas
    except:
        return -1

# =============================================================================
# FUNZIONI DI AZIONE
# =============================================================================

def scale_mission(apps_v1, replicas, reason):
    """Accende/Spegne la missione."""
    body = {"spec": {"replicas": replicas}}
    try:
        print(f"\n⚙️  GLOBAL ({reason}): Scale -> {replicas} repliche.")
        apps_v1.patch_namespaced_deployment_scale(DEPLOYMENT_NAME, NAMESPACE, body)
    except Exception as e:
        print(f"❌ Errore Scaling: {e}")

def evict_pod(v1, pod_name, node_name, reason):
    """Sfratta un pod (Reschedule)."""
    try:
        print(f"\n🚑 LOCAL ({reason}): Sfratto Pod {pod_name} da {node_name}.")
        v1.delete_namespaced_pod(pod_name, NAMESPACE, grace_period_seconds=0)
    except Exception as e:
        print(f"❌ Errore Eviction: {e}")

# =============================================================================
# MAIN LOOP
# =============================================================================

def main():
    print("--- Space Cloud WATCHDOG v2.2 (Hardware Aware) ---")
    print("   1. Monitors Battery Levels")
    print("   2. Monitors Hardware Health (Ready/NotReady)")

    config.load_kube_config()
    v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()
    w = watch.Watch()

    while True:
        try:
            # Check Redis Connection
            try: r.ping() 
            except: pass

            # Check ciclico ogni 3 secondi (più veloce per rilevare guasti)
            for event in w.stream(v1.list_node, timeout_seconds=3):
                pass # Consumiamo lo stream solo per il timeout (loop ciclico)

            # --- 1. ANALISI GLOBALE (FLOTTA) ---
            best_battery, healthy_count = get_max_fleet_battery(v1)
            replicas = get_current_replicas(apps_v1)

            # Sync continuo con la Dashboard (per sicurezza)
            status_text = "NOMINAL"
            if replicas == 0: status_text = "HIBERNATED (Low Power)"
            update_dashboard_status(replicas, status_text)

            # Se non ci sono nodi sani, spegni tutto (Safety)
            if healthy_count == 0 and replicas > 0:
                scale_mission(apps_v1, 0, "CRITICAL: NO HEALTHY SATELLITES")
                continue

            # ISTERESI BATTERIA
            if best_battery < THRESHOLD_SHUTDOWN and replicas > 0:
                scale_mission(apps_v1, 0, f"Low Energy Max={best_battery}%")
                continue
            elif best_battery > THRESHOLD_RESTART and replicas == 0:
                scale_mission(apps_v1, 1, f"Energy Recovered Max={best_battery}%")
                continue

            # --- 2. ANALISI LOCALE (POD & HARDWARE) ---
            if replicas > 0:
                pods = v1.list_namespaced_pod(NAMESPACE, label_selector=f"app={APP_LABEL}").items
                
                for pod in pods:
                    if pod.metadata.deletion_timestamp or pod.status.phase == "Pending":
                        continue
                        
                    node_name = pod.spec.node_name
                    if not node_name: continue
                    
                    # Recuperiamo l'oggetto nodo per controllarne la salute
                    try:
                        node_obj = v1.read_node(node_name)
                        is_healthy = is_node_healthy(node_obj)
                        node_batt = int(node_obj.metadata.labels.get(LABEL_BATTERY, 0))
                    except:
                        # Se non riusciamo a leggere il nodo, assumiamo sia morto
                        is_healthy = False
                        node_batt = 0

                    # CASO A: GUASTO HARDWARE (Priorità Massima)
                    if not is_healthy:
                        evict_pod(v1, pod.metadata.name, node_name, "HARDWARE FAILURE")
                        continue # Passa al prossimo pod

                    # CASO B: BATTERIA LOCALE BASSA
                    if node_batt < THRESHOLD_EVICT:
                        evict_pod(v1, pod.metadata.name, node_name, f"Battery Low {node_batt}%")

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Errore ciclo: {e}")
            time.sleep(2)

if __name__ == "__main__":
    main()