import time
from kubernetes import client, config, watch

# =============================================================================
# CONFIGURAZIONE ORCHESTRATORE (FASE 3 - IBRIDA)
# =============================================================================

DEPLOYMENT_NAME = "missione-web"
NAMESPACE = "default"
APP_LABEL = "space-app"  # Etichetta per trovare il Pod

LABEL_BATTERY = "spacecloud.io/battery_level"

# Soglie Globali (Missione ON/OFF)
THRESHOLD_SHUTDOWN = 20  # Se TUTTI sono sotto 20% -> Spegni missione
THRESHOLD_RESTART = 40   # Se QUALCUNO è sopra 40% -> Accendi missione

# Soglia Locale (Sfratto Pod)
THRESHOLD_EVICT = 20     # Se il nodo del pod scende sotto 20% -> Spostalo

# =============================================================================
# FUNZIONI DI SUPPORTO
# =============================================================================

def get_node_battery(v1, node_name):
    """Legge la batteria di un nodo specifico."""
    try:
        node = v1.read_node(node_name)
        labels = node.metadata.labels
        if labels and LABEL_BATTERY in labels:
            return int(labels[LABEL_BATTERY])
    except:
        return 0
    return 0

def get_max_fleet_battery(v1):
    """Trova la batteria massima nella costellazione."""
    nodes = v1.list_node().items
    max_batt = 0
    for node in nodes:
        if "control-plane" in node.metadata.name: continue
        val = int(node.metadata.labels.get(LABEL_BATTERY, 0))
        if val > max_batt: max_batt = val
    return max_batt

def get_current_replicas(apps_v1):
    try:
        dep = apps_v1.read_namespaced_deployment(DEPLOYMENT_NAME, NAMESPACE)
        return dep.spec.replicas
    except:
        return -1

def scale_mission(apps_v1, replicas):
    """Accende o Spegne l'intera missione (Deployment)."""
    body = {"spec": {"replicas": replicas}}
    try:
        print(f"\n⚙️  GLOBAL: Cambio stato missione -> {replicas} repliche...")
        apps_v1.patch_namespaced_deployment_scale(DEPLOYMENT_NAME, NAMESPACE, body)
    except Exception as e:
        print(f"❌ Errore Scaling: {e}")

def evict_pod(v1, pod_name, node_name):
    """Uccide un pod specifico per forzare lo spostamento (Reschedule)."""
    try:
        print(f"\n♻️  LOCAL: Sfratto Pod {pod_name} da {node_name} (Batteria Bassa)...")
        v1.delete_namespaced_pod(pod_name, NAMESPACE, grace_period_seconds=0)
    except Exception as e:
        print(f"❌ Errore Eviction: {e}")

# =============================================================================
# CICLO PRINCIPALE
# =============================================================================

def main():
    print("--- Space Cloud WATCHDOG ---")
    print("   1. GLOBAL: Gestisce ON/OFF missione in base alla flotta.")
    print("   2. LOCAL:  Sposta il pod se il singolo satellite si scarica.")

    config.load_kube_config()
    v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()
    w = watch.Watch()

    while True:
        try:
            # Ascoltiamo eventi (ogni 5 secondi resetto per fare check ciclici)
            for event in w.stream(v1.list_node, timeout_seconds=5):
                if event['type'] != "MODIFIED": continue
                
                # --- 1. ANALISI GLOBALE (FLOTTA) ---
                best_battery = get_max_fleet_battery(v1)
                replicas = get_current_replicas(apps_v1)

                # LOGICA ISTERESI (Missione ON/OFF)
                if best_battery < THRESHOLD_SHUTDOWN and replicas > 0:
                    print(f"📉 BLACKOUT FLOTTA (Max: {best_battery}%). Spegnimento missione.")
                    scale_mission(apps_v1, 0)
                    continue # Se spengo, non serve controllare altro
                
                elif best_battery > THRESHOLD_RESTART and replicas == 0:
                    print(f"📈 ENERGIA RECUPERATA (Max: {best_battery}%). Riavvio missione.")
                    scale_mission(apps_v1, 1)
                    continue

                # --- 2. ANALISI LOCALE (POD) ---
                # Se la missione è ACCESA, controlliamo se il pod è sul satellite giusto.
                if replicas > 0:
                    # Troviamo il pod attivo
                    pods = v1.list_namespaced_pod(NAMESPACE, label_selector=f"app={APP_LABEL}").items
                    
                    for pod in pods:
                        # Ignoriamo pod che si stanno già spegnendo o sono pending
                        if pod.metadata.deletion_timestamp or pod.status.phase == "Pending":
                            continue
                            
                        node_name = pod.spec.node_name
                        if not node_name: continue
                        
                        # Controlliamo la batteria del nodo OSPITE
                        node_batt = get_node_battery(v1, node_name)
                        
                        # SE IL NODO OSPITE È SCARICO (ma la flotta ha energia altrove)
                        if node_batt < THRESHOLD_EVICT:
                            print(f"⚠️  Pod su {node_name} con {node_batt}% (Critico).")
                            evict_pod(v1, pod.metadata.name, node_name)
                            # Nota: Il Deployment ne creerà uno nuovo.
                            # Lo Scheduler (space_scheduler.py) lo metterà sul nodo carico (best_battery).

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Errore stream: {e}")
            time.sleep(2)

if __name__ == "__main__":
    main()