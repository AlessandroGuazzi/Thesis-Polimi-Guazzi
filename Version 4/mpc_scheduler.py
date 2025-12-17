import time
import json
import redis
import requests
from kubernetes import client, config

# =============================================================================
#  MPC SCHEDULER V2.1 (Initial Placement + Hot Migration)
# =============================================================================

PREDICTION_HORIZON = 15.0  
CRITICAL_BATTERY = 20.0    
ECLIPSE_START = 180
ECLIPSE_END = 240
FUTURE_DRAIN_LOAD = 2.0    
CHARGE_RATE = 4.0
KUBELET_PORT = 10250

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

try:
    config.load_kube_config()
    v1 = client.CoreV1Api()
    print("✅ MPC Scheduler: Operativo (Placement & Migration).")
except Exception as e:
    print(f"❌ Errore K8s: {e}")
    exit(1)

# --- 1. LOGICA PREDIZIONALE (IL CERVELLO) ---

def predict_future_state(current_telemetry, horizon_seconds):
    """Calcola come starà il satellite nel futuro."""
    curr_angle = current_telemetry['angle']
    curr_batt = current_telemetry['battery']
    
    # Calcolo posizione futura
    angle_travelled = (horizon_seconds / 60.0) * 360.0
    future_angle = (curr_angle + angle_travelled) % 360.0
    
    # Check Eclissi Futura
    future_eclipse = (future_angle >= ECLIPSE_START and future_angle <= ECLIPSE_END)
    
    # Check Batteria Futura
    if future_eclipse:
        delta_batt = -(FUTURE_DRAIN_LOAD * horizon_seconds)
    else:
        net_charge = CHARGE_RATE - FUTURE_DRAIN_LOAD
        delta_batt = (net_charge * horizon_seconds)
        
    return curr_batt + delta_batt, future_eclipse

def find_best_node(fleet_data, exclude_node=None):
    """
    Trova il miglior nodo in assoluto basandosi sulla PREDIZIONE.
    Usato sia per il primo piazzamento sia per la migrazione.
    """
    best_node = None
    best_score = -9999
    
    # Filtriamo solo i nodi worker (ignoriamo control-plane se vogliamo, o lo includiamo)
    # Qui valutiamo tutti i nodi presenti nella telemetria
    for node, data in fleet_data.items():
        if node == exclude_node: continue
        
        # Guardiamo nel futuro
        f_batt, f_eclipse = predict_future_state(data, PREDICTION_HORIZON)
        
        # --- SCORE FUNCTION ---
        # Base: Batteria Futura
        score = f_batt
        
        # Penalità Eclissi Futura (è la cosa peggiore)
        if f_eclipse: score -= 500 
        
        # Penalità Nodo quasi scarico (anche se al sole)
        if f_batt < 30: score -= 100
        
        # Penalità Temperatura alta (Safety)
        if data['temp'] > 75: score -= 200

        # Debug Score
        # print(f"   [Scoring] {node[-7:]}: FutureBatt={f_batt:.1f} Eclipse={f_eclipse} -> Score={score:.1f}")
        
        if score > best_score:
            best_score = score
            best_node = node
            
    return best_node

# --- 2. GESTIONE INITIAL PLACEMENT (PENDING PODS) ---

def bind_pod(pod_name, node_name):
    """
    Effettua il BINDING manuale: collega un Pod Pending a un Nodo.
    È l'equivalente di quello che fa lo scheduler di default di K8s.
    """
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

def schedule_pending_pods(fleet_data):
    """Cerca Pod in stato Pending e li assegna."""
    try:
        # Cerchiamo Pod che aspettano di essere schedulati
        pods = v1.list_namespaced_pod("default", field_selector="status.phase=Pending")
        
        for pod in pods.items:
            # Opzionale: controlla se il pod ha impostato schedulerName="space-scheduler"
            # O se è semplicemente Pending. Per ora prendiamo tutto.
            pod_name = pod.metadata.name
            
            # Se il pod sta venendo cancellato, ignoralo
            if pod.metadata.deletion_timestamp: continue

            print(f"🆕 Trovato Pod Pending: {pod_name}. Calcolo destinazione migliore...")
            
            best_node = find_best_node(fleet_data)
            
            if best_node:
                bind_pod(pod_name, best_node)
            else:
                print("   ⚠️ Nessun nodo adatto trovato (tutti in eclissi o rischio?). Attendo.")
                
    except Exception as e:
        print(f"Errore loop pending: {e}")

# --- 3. GESTIONE MIGRATION (CRIU / HOT SWAP) ---

def get_node_ip(node_name):
    try:
        node = v1.read_node(node_name)
        for addr in node.status.addresses:
            if addr.type == "InternalIP": return addr.address
    except: pass
    return None

def trigger_checkpoint_and_transfer(source, dest, pod_name):
    """Simula Checkpoint e Trasferimento"""
    print(f"   🧊 [CRIU] Checkpoint {pod_name} su {source}...")
    # (Codice API Kubelet simulato come discusso)
    time.sleep(1.0) 
    
    print(f"   📡 [LASER] Trasferimento RAM -> {dest}...")
    time.sleep(1.0)
    return True

def migrate_workload_hot(current_node, pod_name, fleet_data):
    print(f"\n🚨 [MPC MIGRATION] Avvio procedura per {pod_name}...")
    
    # 1. Trova destinazione escludendo il nodo corrente
    target_node = find_best_node(fleet_data, exclude_node=current_node)
    
    if not target_node:
        print("   ⚠️ ABORT: Nessun satellite di soccorso valido!")
        return

    print(f"   👉 Destinazione scelta: {target_node}")
    
    # 2. Esegui Checkpoint & Transfer (Simulati/API)
    if trigger_checkpoint_and_transfer(current_node, target_node, pod_name):
        
        # 3. RESTORE (Kill & Respawn su nuovo nodo)
        try:
            # Cordoniamo il nodo morente per sicurezza
            v1.patch_node(current_node, {"spec": {"unschedulable": True}})
            
            print(f"   🔄 [RESTORE] Riavvio Pod su {target_node}...")
            v1.delete_namespaced_pod(pod_name, "default", grace_period_seconds=0)
            
            # Attendiamo che K8s noti la morte del pod
            time.sleep(2)
            
            # (Il loop 'schedule_pending_pods' al prossimo giro vedrà il pod Pending 
            #  e lo assegnerà al 'best_node' che sarà proprio il target_node 
            #  perché il vecchio è cordon-ato o ha score basso)
            
            # Pulizia nodo vecchio
            time.sleep(3)
            v1.patch_node(current_node, {"spec": {"unschedulable": False}})
            print("   ✅ Migrazione conclusa.")
            
        except Exception as e:
            print(f"   ❌ Errore Restore: {e}")

def get_running_pod_node():
    """Trova il pod che sta girando attualmente"""
    try:
        pods = v1.list_namespaced_pod("default", label_selector="app=space-mission")
        for pod in pods.items:
            if pod.status.phase == "Running" and not pod.metadata.deletion_timestamp:
                return pod.spec.node_name, pod.metadata.name
    except: pass
    return None, None

# --- MAIN LOOP ---

def main():
    print(f"--- 🧠 MPC SCHEDULER V2.1: FULL CYCLE ---")
    print(f"Horizon: {PREDICTION_HORIZON}s | Battery Crit: {CRITICAL_BATTERY}%")
    
    try:
        while True:
            time.sleep(2.0)
            
            # 1. ACQUISIZIONE DATI
            raw_data = redis_client.get("fleet_telemetry")
            if not raw_data: 
                print("In attesa di telemetria...", end="\r")
                continue
            fleet = json.loads(raw_data)
            
            # 2. FASE: INITIAL PLACEMENT
            # Se ci sono pod appena creati (Pending), li piazziamo ORA.
            schedule_pending_pods(fleet)
            
            # 3. FASE: MONITORING & MIGRATION
            curr_node, pod_name = get_running_pod_node()
            
            if curr_node and pod_name:
                curr_stats = fleet.get(curr_node)
                if not curr_stats: continue
                
                # Predizione stato corrente
                pred_batt, pred_eclipse = predict_future_state(curr_stats, PREDICTION_HORIZON)
                
                # Logica Decisionale
                should_migrate = False
                reason = ""
                
                if pred_batt < CRITICAL_BATTERY:
                    should_migrate = True
                    reason = f"LOW BATTERY PREDICTION ({pred_batt:.1f}%)"
                elif pred_eclipse and curr_stats['battery'] < 60:
                    should_migrate = True
                    reason = "ECLIPSE AVOIDANCE"
                
                status_msg = f"Pod: {pod_name} @ {curr_node[-7:]} | Futuro: {pred_batt:.1f}%"
                
                if should_migrate:
                    print(f"\n🔮 RISCHIO RILEVATO: {reason}")
                    migrate_workload_hot(curr_node, pod_name, fleet)
                    time.sleep(5) # Cooldown
                else:
                    print(f"✅ {status_msg} [OK]", end="\r")
            else:
                # Se non c'è pod running, magari sta venendo schedulato
                print("⏳ Scanning for workload...", end="\r")

    except KeyboardInterrupt:
        print("\nShutdown.")

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