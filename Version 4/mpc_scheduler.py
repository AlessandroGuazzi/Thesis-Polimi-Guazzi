import time
import json
import redis
import sys
from kubernetes import client, config
from kubernetes.stream import stream

# ==========================================
# CONFIGURAZIONE MISSIONE
# ==========================================
PREDICTION_HORIZON = 15.0   # Secondi nel futuro
CRITICAL_BATTERY = 20.0     # Soglia alta per testare la migrazione frequente
ECLIPSE_START = 180         # Gradi inizio ombra
ECLIPSE_END = 240           # Gradi fine ombra
FUTURE_DRAIN_LOAD = 2.0     # Consumo previsto
CHARGE_RATE = 4.0           # Ricarica solare

# ==========================================
# STATO GLOBALE SCHEDULER
# ==========================================
# Mappa per il "Target Lock": { 'app_name': 'nodo_destinazione_prenotato' }
MIGRATION_TARGETS = {} 

# Connessione al Bus di Sistema (System Redis)
# Serve per leggere la telemetria e salvare il checkpoint CRIU
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Inizializzazione Kubernetes
try:
    config.load_kube_config()
    v1 = client.CoreV1Api()
    print("\n✅ MPC SCHEDULER: ONLINE")
    print("   Modo: CRIU Hot Migration + Target Lock")
    print("   Soglia Batteria: <" + str(CRITICAL_BATTERY) + "%")
except Exception as e:
    print(f"❌ Errore Init K8s: {e}")
    sys.exit(1)


# ==========================================
# 1. LOGICA FISICA E PREDIZIONE
# ==========================================

def get_telemetry_safe():
    try:
        return redis_client.get("fleet_telemetry")
    except:
        return None

def predict_future_state(current_telemetry, horizon_seconds):
    curr_angle = current_telemetry['angle']
    curr_batt = current_telemetry['battery']
    
    # Calcolo nuova posizione orbitale
    angle_travelled = (horizon_seconds / 60.0) * 360.0
    future_angle = (curr_angle + angle_travelled) % 360.0
    future_eclipse = (future_angle >= ECLIPSE_START and future_angle <= ECLIPSE_END)
    
    # Calcolo delta energetico
    if future_eclipse:
        delta_batt = -(FUTURE_DRAIN_LOAD * horizon_seconds)
    else:
        net_charge = CHARGE_RATE - FUTURE_DRAIN_LOAD
        delta_batt = (net_charge * horizon_seconds)
    
    # Clamp valori (0% - 100%)
    future_batt = max(0.0, min(100.0, curr_batt + delta_batt))
    
    return future_batt, future_eclipse

def find_best_node(fleet_data, exclude_node=None):
    best_node = None
    best_score = -99999
    
    if not fleet_data: return None

    # Filtra candidati escludendo il nodo attuale
    candidates = [n for n in fleet_data.keys() if n != exclude_node]
    
    # Safety: se non ci sono candidati, resetta i filtri
    if not candidates and exclude_node:
        candidates = fleet_data.keys()

    for node in candidates:
        data = fleet_data[node]
        f_batt, f_eclipse = predict_future_state(data, PREDICTION_HORIZON)
        
        # Punteggio MPC
        score = f_batt
        if f_eclipse: score -= 500         # Penalità Eclissi
        if f_batt < 30: score -= 100       # Penalità Batteria Bassa
        if data['temp'] > 75: score -= 200 # Penalità Termica
        
        # Penalità al Master Node (Preferiamo i Worker)
        if "control-plane" in node: score -= 50
        
        if score > best_score:
            best_score = score
            best_node = node
            
    return best_node

# ==========================================
# 2. SIMULAZIONE CRIU (Checkpoint/Restore)
# ==========================================

def perform_criu_checkpoint(pod_name):
    """
    Esegue un comando dentro il sidecar Redis del Pod per estrarre lo stato
    prima che venga terminato.
    """
    print(f"   📸 CRIU: Estrazione stato memoria da {pod_name}...")
    try:
        # Comando: redis-cli get mission_step
        exec_command = ['redis-cli', 'get', 'mission_step']
        
        resp = stream(v1.connect_get_namespaced_pod_exec,
                      pod_name,
                      'default',
                      container='memory-unit', # Targettiamo il Sidecar
                      command=exec_command,
                      stderr=True, stdin=False,
                      stdout=True, tty=False)
        
        # Pulizia stringa risposta
        checkpoint_value = resp.strip().replace('"', '')
        
        if checkpoint_value and checkpoint_value != "None":
            # Salviamo nel bus condiviso (System Redis)
            redis_client.set("criu_checkpoint_mission_step", checkpoint_value)
            print(f"   💾 CHECKPOINT SAVED: Step {checkpoint_value} salvato nel buffer.")
            return True
        else:
            print("   ⚠️  CRIU: Nessun dato in memoria (Cold Start?).")
            return False

    except Exception as e:
        print(f"   ❌ CRIU ERROR: Impossibile contattare il pod ({e})")
        return False

# ==========================================
# 3. GESTIONE KUBERNETES (Binding & Migration)
# ==========================================

def bind_pod(pod_name, node_name):
    if not node_name: return False
    
    target = client.V1ObjectReference(kind="Node", api_version="v1", name=node_name)
    meta = client.V1ObjectMeta(name=pod_name)
    body = client.V1Binding(api_version="v1", kind="Binding", metadata=meta, target=target)
    
    try:
        v1.create_namespaced_binding(namespace="default", body=body)
        print(f"   🔗 BINDING: {pod_name} -> {node_name}")
        return True
    except client.exceptions.ApiException as e:
        if e.status == 409: return True # Già bindato, ok
        return False
    except: return False

def migrate_workload_hot(current_node, pod_name, fleet_data, app_label="space-app"):
    print(f"\n🚨 [MPC MIGRATION] Triggered per {pod_name}")
    
    # 1. Calcolo Destinazione Ottimale (escludendo nodo corrente)
    target_node = find_best_node(fleet_data, exclude_node=current_node)
    
    if not target_node:
        print("   ⚠️ ABORT: Nessuna destinazione valida disponibile.")
        return

    print(f"   👉 Destinazione Calcolata: {target_node}")
    
    try:
        # 2. CRIU CHECKPOINT (Salva lo stato)
        perform_criu_checkpoint(pod_name)

        # 3. TARGET LOCK (Prenota il nodo destinazione)
        MIGRATION_TARGETS[app_label] = target_node

        # 4. TERMINAZIONE POD (Simula spostamento)
        print(f"   🔄 [RE-ORBIT] Riavvio Pod su nuovo nodo...")
        v1.delete_namespaced_pod(pod_name, "default", grace_period_seconds=0)
        
        # Pausa tecnica per propagazione stato K8s
        time.sleep(5)
        
    except Exception as e:
        print(f"   ❌ Errore API Migrazione: {e}")
        # Se fallisce, rimuoviamo la prenotazione
        if app_label in MIGRATION_TARGETS: del MIGRATION_TARGETS[app_label]

# ==========================================
# 4. LOOP PRINCIPALI
# ==========================================

def handle_pending_pods(fleet_data):
    try:
        pods = v1.list_namespaced_pod("default", field_selector="status.phase=Pending")
        for pod in pods.items:
            # Ignora pod già assegnati
            if pod.spec.node_name: continue
            
            # Gestisci solo i nostri pod
            if pod.spec.scheduler_name == "space-scheduler" and not pod.metadata.deletion_timestamp:
                print(f"🆕 Pod Pending: {pod.metadata.name}")
                app_label = pod.metadata.labels.get('app', 'space-app')
                
                # A. CONTROLLO PRENOTAZIONI (CRIU TARGET LOCK)
                target_node = MIGRATION_TARGETS.get(app_label)
                
                if target_node:
                    print(f"   🎫 Trovata Prenotazione per: {target_node}")
                    if target_node in fleet_data:
                        bind_pod(pod.metadata.name, target_node)
                        # Consuma il biglietto
                        del MIGRATION_TARGETS[app_label]
                        return 
                    else:
                        print(f"   ⚠️ Nodo prenotato non disponibile. Ricalcolo...")
                        target_node = None

                # B. CALCOLO STANDARD (Se non c'è prenotazione)
                if not target_node:
                    target_node = find_best_node(fleet_data)
                
                if target_node: 
                    bind_pod(pod.metadata.name, target_node)
                else:
                    print("   ⏳ Nessun nodo adatto. Attendo...")

    except Exception as e:
        print(f"Errore Pending: {e}")

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
                
                # Predizione MPC
                pred_batt, pred_eclipse = predict_future_state(node_stats, PREDICTION_HORIZON)
                
                should_migrate = False
                reason = ""
                
                if pred_batt < CRITICAL_BATTERY:
                    should_migrate = True
                    reason = f"LOW BATTERY (<{CRITICAL_BATTERY}%)"
                elif pred_eclipse and node_stats['battery'] < 60:
                    should_migrate = True
                    reason = "ECLIPSE ENTRY"
                
                # Visualizzazione Stato
                icon = "🌑" if pred_eclipse else "☀️"
                short_node = curr_node.replace("space-cloud-", "")
                print(f"✅ {pod_name[-5:]} @ {short_node} | Batt: {node_stats['battery']:.0f}% -> Fut: {pred_batt:.0f}% {icon}   ", end="\r")
                
                if should_migrate:
                    print(f"\n🔮 RISCHIO RILEVATO: {reason}")
                    migrate_workload_hot(curr_node, pod_name, fleet_data, app_label)
                    time.sleep(5) # Cooldown post migrazione

    except Exception:
        pass

def main():
    try:
        while True:
            time.sleep(1.5)
            raw_data = get_telemetry_safe()
            if not raw_data: continue
            
            fleet = json.loads(raw_data)
            
            handle_pending_pods(fleet)
            handle_running_pods(fleet)
            
    except KeyboardInterrupt:
        print("\n🛑 Scheduler terminato manualmente.")
        sys.exit(0)

if __name__ == "__main__":
    main()