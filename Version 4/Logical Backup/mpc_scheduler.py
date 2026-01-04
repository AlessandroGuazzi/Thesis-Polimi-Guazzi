import time
import json
import redis
import sys
import subprocess
import os
from kubernetes import client, config

# ==========================================
# CONFIGURAZIONE MISSIONE
# ==========================================
PREDICTION_HORIZON = 15.0
CRITICAL_BATTERY = 20.0  # Soglia per scatenare il checkpoint
# URL del proxy locale (deve essere attivo con: kubectl proxy --port=8001)
KUBE_PROXY_URL = "http://localhost:8001"
# Percorso sul filesystem dell'Host (Minikube Driver None)
CHECKPOINT_DIR = "/var/lib/kubelet/checkpoints"

# Label usata nel Deployment (space-mission.yaml)
APP_LABEL = "space-mission"

# ==========================================
# INIT SISTEMA
# ==========================================
MIGRATION_TARGETS = {}
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

try:
    config.load_kube_config()
    v1 = client.CoreV1Api()
    print("\n✅ MPC SCHEDULER: ONLINE (NATIVE BARE METAL MODE)")
except Exception as e:
    print(f"❌ Errore Init K8s: {e}")
    sys.exit(1)


# ==========================================
# 1. LOGICA FISICA E PREDITTIVA
# ==========================================
def get_telemetry_safe():
    try:
        return redis_client.get("fleet_telemetry")
    except:
        return None


def predict_future_state(current_telemetry, horizon_seconds):
    curr_batt = current_telemetry['battery']
    # Logica semplice: sotto il 30% la batteria cala, altrimenti si carica
    if curr_batt < 30: return curr_batt - 5, False
    return curr_batt + 2, False


def find_best_node(fleet_data, exclude_node=None):
    best_node = None
    best_score = -99999

    if not fleet_data: return None

    # Filtra i nodi candidati (escludendo quello attuale se richiesto)
    candidates = [n for n in fleet_data.keys() if n != exclude_node]

    # === LOGICA SINGLE NODE (MINIKUBE) ===
    # Se la lista candidati è vuota (perché c'è un solo nodo nel cluster),
    # forziamo la scelta sul nodo attuale. La "migrazione" diventa un "riavvio".
    if not candidates:
        # print("   ⚠️ Single Node Mode: Restart sul nodo attuale.")
        candidates = list(fleet_data.keys())
    # =====================================

    for node in candidates:
        data = fleet_data[node]
        f_batt, f_eclipse = predict_future_state(data, PREDICTION_HORIZON)

        # Calcolo Punteggio (Cost Function)
        score = f_batt
        if f_eclipse: score -= 500
        if f_batt < 30: score -= 100
        if data['temp'] > 75: score -= 200

        if score > best_score:
            best_score = score
            best_node = node

    return best_node


# ==========================================
# 2. GESTIONE CHECKPOINT (API + RENAMING)
# ==========================================

def perform_api_checkpoint(pod_name, node_name):
    """
    1. Chiama l'API del Kubelet tramite Proxy per generare il .tar
    2. Rinomina il file risultante in 'checkpoint.tar' per l'auto-restore
    """
    print(f"   📸 RICHIESTA CHECKPOINT API per {pod_name}...")

    # Costruzione URL API (tramite proxy kubectl)
    url = f"{KUBE_PROXY_URL}/api/v1/nodes/{node_name}/proxy/checkpoint/default/{pod_name}/main-app"

    cmd = ["curl", "-X", "POST", url]

    try:
        # Eseguiamo la chiamata CURL
        result = subprocess.run(cmd, capture_output=True, text=True)

        # Controllo successo (HTTP 200 OK non è garantito da curl, controlliamo l'output)
        if result.returncode == 0 and "items" not in result.stdout:
            print("   ✅ API Risposta: OK (Checkpoint Triggered)")

            # === POST-PROCESSING (CRUCIALE) ===
            # Il file viene creato con un nome lungo (es. checkpoint-space-mission-...).
            # Lo script di avvio cerca 'checkpoint.tar'. Lo rinominiamo.
            # Usiamo 'sudo' perché /var/lib/kubelet è protetta.
            print("   🔧 Standardizzazione nome file (sudo richiesto)...")

            # Rimuoviamo vecchi checkpoint standardizzati per sicurezza
            subprocess.run(f"sudo rm -f {CHECKPOINT_DIR}/checkpoint.tar", shell=True)

            # Rinominiamo l'ultimo arrivato
            rename_cmd = f"sudo mv {CHECKPOINT_DIR}/checkpoint-*.tar {CHECKPOINT_DIR}/checkpoint.tar 2>/dev/null"
            subprocess.run(rename_cmd, shell=True)

            return True
        else:
            print(f"   ❌ Errore API o Risposta Inattesa: {result.stdout}")
            return False

    except Exception as e:
        print(f"   ❌ Eccezione Python: {e}")
        return False


# ==========================================
# 3. ORCHESTRAZIONE MIGRAZIONE (RESTART)
# ==========================================

def migrate_workload_hot(current_node, pod_name, fleet_data):
    print(f"\n🚨 [MPC ALERT] Migrazione Richiesta per {pod_name}")
    print(f"   📉 Causa: Livello energetico critico su {current_node}")

    # 1. Esegui il Checkpoint
    success = perform_api_checkpoint(pod_name, current_node)

    if not success:
        print("   ⚠️ Checkpoint fallito. Annullamento procedura.")
        return

    print(f"   💾 Stato salvato su disco. Avvio procedura di ripristino...")
    time.sleep(1)

    # 2. "Migrazione" (Delete & Recreate)
    # Cancellando il pod, il Deployment ne creerà subito uno nuovo.
    # Il nuovo pod troverà 'checkpoint.tar' e ripartirà da lì.
    print(f"   💀 Terminazione Pod (Simulazione Guasto/Spostamento)...")
    try:
        v1.delete_namespaced_pod(pod_name, "default", grace_period_seconds=0)
        print("   ✅ Pod eliminato. Kubernetes ne avvierà uno nuovo a breve.")
    except Exception as e:
        print(f"   ❌ Errore cancellazione pod: {e}")

    # Pausa estetica per i log
    time.sleep(5)


# ==========================================
# 4. LOOP DI CONTROLLO PRINCIPALE
# ==========================================

def handle_running_pods(fleet_data):
    try:
        # Cerchiamo i pod con la label corretta (space-mission)
        pods = v1.list_namespaced_pod("default", label_selector=f"app={APP_LABEL}")

        if not pods.items:
            print(f"   ⚠️  Nessun pod trovato con label 'app={APP_LABEL}'...", end="\r")
            return

        for pod in pods.items:
            if pod.status.phase == "Running" and not pod.metadata.deletion_timestamp:
                curr_node = pod.spec.node_name
                pod_name = pod.metadata.name

                # Recuperiamo i dati del nodo (o usiamo dati mock se manca il simulatore fisico)
                # NOTA: Se il simulatore non gira, usiamo 'battery': 50 come default sicuro.
                node_stats = fleet_data.get(curr_node, {'battery': 50})

                print(f"✅ MONITOR: {pod_name} su {curr_node} | Batt: {node_stats.get('battery')}%   ", end="\r")

                # === LOGICA DI DECISIONE ===
                # Se la batteria scende sotto la soglia critica, attiviamo la procedura.
                if node_stats.get('battery', 100) < CRITICAL_BATTERY:
                    # Troviamo il target (che nel single node sarà sempre lo stesso nodo)
                    target = find_best_node(fleet_data, exclude_node=curr_node)
                    migrate_workload_hot(curr_node, pod_name, fleet_data)

                    # Attendiamo che il sistema si stabilizzi prima di riprendere il monitoraggio
                    print("\n⏳ Cooldown sistema (20s)...")
                    time.sleep(20)

    except Exception as e:
        print(f"\n❌ Errore nel Loop: {e}")


def main():
    print(f"🔭 SPACE CLOUD SCHEDULER V6.0 (Label Target: {APP_LABEL})")
    print("   Premere CTRL+C per terminare.")

    try:
        while True:
            time.sleep(2)
            # 1. Leggi Telemetria dal Bus Redis
            raw = get_telemetry_safe()

            # 2. Parsing o Mock Data
            if raw:
                fleet = json.loads(raw)
            else:
                # Se non c'è Redis, assumiamo che il nodo si chiami come quello rilevato da K8s o 'minikube'
                fleet = {"minikube": {"battery": 100, "temp": 25}}

            # 3. Logica di Controllo
            handle_running_pods(fleet)

    except KeyboardInterrupt:
        print("\n🛑 Scheduler terminato dall'utente.")


if __name__ == "__main__":
    main()