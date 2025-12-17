import time
import json
import redis
from kubernetes import client, config

# --- CONFIGURAZIONE ---
PREDICTION_HORIZON = 15.0  
CRITICAL_BATTERY = 20.0    
ECLIPSE_START = 180
ECLIPSE_END = 240
FUTURE_DRAIN_LOAD = 2.0    
CHARGE_RATE = 4.0

# Connessione al SYSTEM REDIS (Infrastruttura)
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

try:
    config.load_kube_config()
    v1 = client.CoreV1Api()
    print("✅ MPC Scheduler: FULL CONTROL (Placement + Migration).")
except Exception as e:
    print(f"❌ Errore K8s: {e}")
    exit(1)

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
    return curr_batt + delta_batt, future_eclipse

def find_best_node(fleet_data, exclude_node=None):
    best_node = None
    best_score = -9999
    
    for node, data in fleet_data.items():
        if node == exclude_node: continue
        
        # PREDIZIONE FUTURA
        f_batt, f_eclipse = predict_future_state(data, PREDICTION_HORIZON)
        
        # Calcolo Punteggio
        score = f_batt
        if f_eclipse: score -= 500  # Penalità gravissima per eclissi futura
        if f_batt < 30: score -= 100 # Penalità batteria scarica
        if data['temp'] > 75: score -= 200 # Penalità termica
        
        if score > best_score:
            best_score = score
            best_node = node
            
    return best_node

# --- 1. INITIAL PLACEMENT (Gestione Pending) ---
def bind_pod(pod_name, node_name):
    target = client.V1ObjectReference(kind="Node", api_version="v1", name=node_name)
    meta = client.V1ObjectMeta(name=pod_name)
    body = client.V1Binding(api_version="v1", kind="Binding", metadata=meta, target=target)
    try:
        v1.create_namespaced_binding(namespace="default", body=body)
        print(f"   🔗 BINDING: Pod {pod_name} -> {node_name}")
        return True
    except Exception as e:
        print(f"   ❌ Errore Binding: {e}")
        return False

def handle_pending_pods(fleet_data):
    try:
        # Cerca SOLO i pod che hanno richiesto il nostro scheduler
        pods = v1.list_namespaced_pod("default", field_selector="status.phase=Pending")
        for pod in pods.items:
            if pod.spec.scheduler_name == "space-scheduler" and not pod.metadata.deletion_timestamp:
                print(f"🆕 Pod Pending rilevato: {pod.metadata.name}")
                best_node = find_best_node(fleet_data)
                if best_node:
                    bind_pod(pod.metadata.name, best_node)
                else:
                    print("   ⚠️ Nessun nodo adatto disponibile al momento.")
    except Exception as e:
        print(f"Errore Pending: {e}")

# --- 2. RUNTIME MIGRATION (Gestione Running) ---
def migrate_workload_hot(current_node, pod_name, fleet_data):
    print(f"\n🚨 [MPC MIGRATION] Spostamento Strategico {pod_name}...")
    target_node = find_best_node(fleet_data, exclude_node=current_node)
    
    if not target_node:
        print("   ⚠️ Nessun target valido!")
        return

    print(f"   👉 Destinazione: {target_node}")
    
    try:
        # 1. Cordon Node (Blocca nuove schedulazioni qui)
        v1.patch_node(current_node, {"spec": {"unschedulable": True}})
        
        # 2. Kill Pod (Triggera la ri-creazione)
        # Il nuovo pod sarà Pending -> handle_pending_pods lo assegnerà al target_node
        print(f"   🔄 [RE-ORBIT] Riavvio Pod verso {target_node}...")
        v1.delete_namespaced_pod(pod_name, "default", grace_period_seconds=0)
        
        # 3. Wait & Uncordon
        time.sleep(5)
        v1.patch_node(current_node, {"spec": {"unschedulable": False}})
        print("   ✅ Migrazione avviata.")
        
    except Exception as e:
        print(f"   ❌ Errore API: {e}")

def handle_running_pods(fleet_data):
    try:
        # Cerchiamo il nostro pod (Running)
        pods = v1.list_namespaced_pod("default", label_selector="app=space-app")
        for pod in pods.items:
            if pod.status.phase == "Running" and not pod.metadata.deletion_timestamp:
                curr_node = pod.spec.node_name
                pod_name = pod.metadata.name
                
                # Controllo Salute
                node_stats = fleet_data.get(curr_node)
                if not node_stats: continue
                
                pred_batt, pred_eclipse = predict_future_state(node_stats, PREDICTION_HORIZON)
                
                should_migrate = False
                reason = ""
                
                if pred_batt < CRITICAL_BATTERY:
                    should_migrate = True
                    reason = "LOW BATTERY"
                elif pred_eclipse and node_stats['battery'] < 60:
                    should_migrate = True
                    reason = "ECLIPSE ENTRY"
                
                status_msg = f"Pod: {pod_name} @ {curr_node[-7:]} | Futuro: {pred_batt:.1f}%"
                
                if should_migrate:
                    print(f"\n🔮 RISCHIO PREVISTO: {reason}")
                    migrate_workload_hot(curr_node, pod_name, fleet_data)
                    time.sleep(5) # Cooldown per evitare loop rapidi
                else:
                    print(f"✅ {status_msg} [NOMINALE]", end="\r")
                    
    except Exception as e:
        pass

# --- MAIN LOOP ---
def main():
    print(f"--- 🧠 MPC ORCHESTRATOR V2.3 ---")
    
    while True:
        time.sleep(2.0)
        
        # 1. Telemetria
        raw_data = get_telemetry_safe()
        if not raw_data: 
            print("In attesa di System Redis...", end="\r")
            continue
        fleet = json.loads(raw_data)
        
        # 2. Fase STARTUP: Gestisci i nuovi arrivi
        handle_pending_pods(fleet)
        
        # 3. Fase RUNTIME: Monitora chi sta lavorando
        handle_running_pods(fleet)

if __name__ == "__main__":
    main()


# Immagina che CRIU (il software che fa la Hot Migration) sia un Chirurgo.
# Il suo lavoro è: "Addormentare il paziente (App), trasferirlo in un altro ospedale (Satellite), e svegliarlo esattamente con
# lo stesso pensiero che aveva prima".
#
# Nella Realtà (Su un vero Satellite): Il chirurgo ha accesso completo alla sala operatoria e a tutti gli organi vitali (il Kernel
# di Linux). Può fermare il cuore, spostare il corpo e riavviarlo. Risultato: La migrazione è perfetta.
# 
# Nella Tua Simulazione (Kind/Docker): # Tu non stai operando in una vera sala operatoria, ma in una "casa delle bambole" dentro
# un'altra casa (Docker dentro il tuo PC). Per motivi di sicurezza, Docker impedisce ai programmi (anche se sono chirurghi) di
# toccare gli organi vitali profondi (il Kernel dell'Host).
# 
# Cosa succede: Il chirurgo riesce ad addormentare il paziente (Checkpoint Trigger funziona). Riesce a caricarlo in ambulanza
# (Transfer funziona). Ma quando arriva all'altro ospedale e prova a "ricollegare i nervi" (Restore), la sicurezza dell'ospedale lo
# blocca: "Non hai i permessi per toccare questi fili!".
# 
# 
# La Frase "Onesta" per la Tesi: 
# "Professori, io ho progettato tutto il sistema per fare l'operazione completa. Il mio codice ordina l'anestesia e il trasporto.
# Tuttavia, siccome sto simulando tutto sul mio laptop e non su hardware spaziale dedicato, l'ultimo passaggio (il risveglio
# identico al bit) fallisce per blocchi di sicurezza di Docker. Quindi, simulo il risveglio riavviando una copia gemella del
# paziente."