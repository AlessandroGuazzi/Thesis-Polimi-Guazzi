import time
import json
import redis
import sys
from kubernetes import client, config

# --- CONFIGURAZIONE ---
PREDICTION_HORIZON = 15.0  
CRITICAL_BATTERY = 85.0    # Soglia alta per test
ECLIPSE_START = 180
ECLIPSE_END = 240
FUTURE_DRAIN_LOAD = 2.0    
CHARGE_RATE = 4.0

# --- MEMORIA DI PRENOTAZIONE (TARGET LOCK) ---
# Mappa: { 'nome_app': 'nodo_destinazione_obbligatorio' }
# Esempio: { 'space-app': 'space-cloud-worker2' }
MIGRATION_TARGETS = {} 

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

try:
    config.load_kube_config()
    v1 = client.CoreV1Api()
    print("✅ MPC Scheduler: OPERATIVO (Target Lock Logic)")
except Exception as e:
    print(f"❌ Errore Init K8s: {e}")
    sys.exit(1)

def get_telemetry_safe():
    try:
        return redis_client.get("fleet_telemetry")
    except:
        return None

def predict_future_state(current_telemetry, horizon_seconds):
    curr_angle = current_telemetry['angle']
    curr_batt = current_telemetry['battery']
    
    angle_travelled = (horizon_seconds / 60.0) * 360.0
    future_angle = (curr_angle + angle_travelled) % 360.0
    future_eclipse = (future_angle >= ECLIPSE_START and future_angle <= ECLIPSE_END)
    
    if future_eclipse:
        delta_batt = -(FUTURE_DRAIN_LOAD * horizon_seconds)
    else:
        net_charge = CHARGE_RATE - FUTURE_DRAIN_LOAD
        delta_batt = (net_charge * horizon_seconds)
    
    future_batt = max(0.0, min(100.0, curr_batt + delta_batt))
    return future_batt, future_eclipse

def find_best_node(fleet_data, exclude_node=None):
    best_node = None
    best_score = -99999
    
    if not fleet_data: return None

    # Filtra i candidati
    candidates = [n for n in fleet_data.keys() if n != exclude_node]
    
    # Se non rimane nessuno, resetta
    if not candidates and exclude_node:
        candidates = fleet_data.keys()

    for node in candidates:
        data = fleet_data[node]
        f_batt, f_eclipse = predict_future_state(data, PREDICTION_HORIZON)
        
        score = f_batt
        if f_eclipse: score -= 500
        if f_batt < 30: score -= 100
        if data['temp'] > 75: score -= 200
        
        # Penalità al control-plane per preferire i worker
        if "control-plane" in node: score -= 10
        
        if score > best_score:
            best_score = score
            best_node = node
            
    return best_node

def bind_pod(pod_name, node_name):
    if not node_name: return False
        
    target = client.V1ObjectReference(kind="Node", api_version="v1", name=node_name)
    meta = client.V1ObjectMeta(name=pod_name)
    body = client.V1Binding(api_version="v1", kind="Binding", metadata=meta, target=target)
    
    try:
        v1.create_namespaced_binding(namespace="default", body=body)
        print(f"   🔗 BINDING ESEGUITO: {pod_name} -> {node_name}")
        return True
    except client.exceptions.ApiException as e:
        if e.status == 409: return True 
        print(f"   ❌ Errore K8s: {e.reason}")
        return False
    except Exception:
        return False

# --- LOGICA DI ASSEGNAZIONE (PENDING) ---
def handle_pending_pods(fleet_data):
    try:
        pods = v1.list_namespaced_pod("default", field_selector="status.phase=Pending")
        for pod in pods.items:
            if pod.spec.node_name: continue
            
            if pod.spec.scheduler_name == "space-scheduler" and not pod.metadata.deletion_timestamp:
                print(f"🆕 Pod Pending: {pod.metadata.name}")
                app_label = pod.metadata.labels.get('app', 'space-app')
                
                # 1. CONTROLLO PRENOTAZIONI (TARGET LOCK)
                reserved_node = MIGRATION_TARGETS.get(app_label)
                
                target_node = None
                
                if reserved_node:
                    print(f"   🎫 Trovata Prenotazione per: {reserved_node}")
                    # Verifichiamo che il nodo esista ancora nei dati (safety check)
                    if reserved_node in fleet_data:
                        target_node = reserved_node
                        # Consumiamo il biglietto (lo rimuoviamo)
                        del MIGRATION_TARGETS[app_label]
                    else:
                        print(f"   ⚠️ Nodo prenotato {reserved_node} non risponde. Ricalcolo...")
                        target_node = find_best_node(fleet_data)
                else:
                    # 2. NESSUNA PRENOTAZIONE -> Calcolo Standard
                    # (Succede al primo avvio o se la prenotazione fallisce)
                    target_node = find_best_node(fleet_data)
                
                # ESECUZIONE BINDING
                if target_node:
                    bind_pod(pod.metadata.name, target_node)
                else:
                    print("   ⏳ Nessun nodo adatto. Attendo...")

    except Exception as e:
        print(f"Errore pending: {e}")

# --- LOGICA DI MIGRAZIONE (RUNNING) ---
def migrate_workload_hot(current_node, pod_name, fleet_data, app_label="space-app"):
    print(f"\n🚨 [MPC MIGRATION] Triggered per {pod_name}")
    
    # 1. Calcoliamo ORA la destinazione precisa
    target_node = find_best_node(fleet_data, exclude_node=current_node)
    
    if not target_node:
        print("   ⚠️ ABORT: Nessun'altra destinazione valida trovata.")
        return

    print(f"   👉 Destinazione Calcolata: {target_node}")
    
    try:
        # 2. EMETTIAMO IL BIGLIETTO DI PRENOTAZIONE
        MIGRATION_TARGETS[app_label] = target_node
        print(f"   🔒 Target Lock attivato: Il prossimo pod ANDRÀ su {target_node}")

        # 3. Riavvio Pod
        print(f"   🔄 [RE-ORBIT] Riavvio Pod...")
        v1.delete_namespaced_pod(pod_name, "default", grace_period_seconds=0)
        
        time.sleep(5)
        
    except Exception as e:
        print(f"   ❌ Errore API: {e}")
        # Se fallisce la cancellazione, rimuoviamo la prenotazione per non bloccare tutto
        if app_label in MIGRATION_TARGETS:
            del MIGRATION_TARGETS[app_label]

def handle_running_pods(fleet_data):
    try:
        pods = v1.list_namespaced_pod("default", label_selector="app=space-app")
        for pod in pods.items:
            if pod.status.phase == "Running" and not pod.metadata.deletion_timestamp:
                curr_node = pod.spec.node_name
                pod_name = pod.metadata.name
                app_label = pod.metadata.labels.get('app', 'space-app')
                
                node_stats = fleet_data.get(curr_node)
                if not node_stats: continue
                
                pred_batt, pred_eclipse = predict_future_state(node_stats, PREDICTION_HORIZON)
                should_migrate = False
                
                if pred_batt < CRITICAL_BATTERY:
                    should_migrate = True
                    reason = f"LOW BATTERY (<{CRITICAL_BATTERY}%)"
                elif pred_eclipse and node_stats['battery'] < 60:
                    should_migrate = True
                    reason = "ECLIPSE ENTRY"
                
                icon = "🌑" if pred_eclipse else "☀️"
                short_node = curr_node.replace("space-cloud-", "")
                print(f"✅ {pod_name[-5:]} @ {short_node} | Batt: {node_stats['battery']:.0f}% -> Fut: {pred_batt:.0f}% {icon}   ", end="\r")
                
                if should_migrate:
                    print(f"\n🔮 RISCHIO PREVISTO: {reason}")
                    migrate_workload_hot(curr_node, pod_name, fleet_data, app_label)
                    time.sleep(5)
                    
    except Exception:
        pass

def main():
    print(f"--- 🧠 MPC WATCHDOG ATTIVO ---")
    try:
        while True:
            time.sleep(1.5)
            raw_data = get_telemetry_safe()
            if not raw_data: continue
            fleet = json.loads(raw_data)
            handle_pending_pods(fleet)
            handle_running_pods(fleet)
    except KeyboardInterrupt:
        print("\n🛑 Scheduler terminato.")
        sys.exit(0)

if __name__ == "__main__":
    main()