import time
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException

SCHEDULER_NAME = "space-scheduler"
BATTERY_LABEL = "spacecloud.io/battery_level"
MIN_BATTERY = 20

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
            # Se c'è un pod attivo (non in fase di cancellazione)
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
        print(f"📡 BINDING: Pod {name} -> Nodo {node}")
        v1.create_namespaced_binding(namespace=namespace, body=body, _preload_content=False)
        print(f"✅ SUCCESSO: {name} assegnato a {node}")
        return True
    except ApiException as e:
        if e.status == 409: return True
        print(f"❌ Errore API: {e.status}")
        return False
    except:
        return False

def main():
    print("--- Space Cloud Scheduler V3 (Anti-Affinity & HA) ---")
    config.load_kube_config()
    v1 = client.CoreV1Api()
    w = watch.Watch()
    
    while True:
        try:
            for event in w.stream(v1.list_namespaced_pod, "default", timeout_seconds=5):
                pod = event['object']
                
                if pod.status.phase == "Pending" and \
                   pod.spec.scheduler_name == SCHEDULER_NAME and \
                   pod.spec.node_name is None:
                    
                    pod_name = pod.metadata.name
                    app_label = pod.metadata.labels.get('app')
                    print(f"\n🛰️  Rilevato Pod Pending: {pod_name} (App: {app_label})")
                    
                    all_nodes = v1.list_node().items
                    candidates = [] 
                    
                    for node in all_nodes:
                        node_name = node.metadata.name
                        
                        # Check Salute
                        is_ready = False
                        for condition in node.status.conditions:
                            if condition.type == "Ready" and condition.status == "True":
                                is_ready = True
                                break
                        if not is_ready: continue 

                        battery = get_node_battery(node)
                        
                        # --- LOGICA ANTI-COLLISIONE (Cruciale per V3) ---
                        # Se è un Redis e c'è già un Redis qui, SALTA il nodo.
                        # Questo garantisce la distribuzione geografica.
                        has_collision = False
                        if app_label == "redis":
                            has_collision = check_collision(v1, node_name, app_label)
                        
                        if battery >= MIN_BATTERY:
                            if has_collision:
                                print(f"   - {node_name}: ⛔ SCARTATO (Collisione App)")
                                continue
                            
                            candidates.append((node_name, battery))
                            print(f"   - {node_name}: Batt {battery}% -> Candidato")
                    
                    if not candidates:
                        print("⛔ Nessun satellite disponibile (Batteria o Collisioni).")
                        continue

                    # Ordiniamo per Batteria
                    candidates.sort(key=lambda x: x[1], reverse=True)
                    best_node = candidates[0][0]
                    
                    print(f"⭐ VINCITORE: {best_node}")
                    schedule_pod(v1, pod_name, best_node)

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Riavvio Watcher...")

if __name__ == "__main__":
    main()