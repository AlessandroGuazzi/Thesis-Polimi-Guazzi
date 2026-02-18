import time
import json
import requests
import signal
import sys
import os
import random
import math

# =============================================================================
#  PHOENIX PAYLOAD (Stateless Compute Engine)
#  Ruolo: Esegue il carico di lavoro CPU (Training AI)
#  Comportamento: Effimero. Recupera lo stato dal Sidecar all'avvio.
# =============================================================================

# Configurazione
SIDECAR_URL = os.getenv("STATE_ENDPOINT", "http://localhost:80")
SAVE_INTERVAL = 2  # Salva lo stato ogni N epoche

# Stato locale (volatile)
current_epoch = 0
current_loss = 1.0
current_accuracy = 0.0
mission_timer = 0  # Sincronizzato dal sidecar


# --- GESTIONE SEGNALI (Graceful Shutdown) ---
def handle_sigterm(signum, frame):
    print("\n🔥 PHOENIX: Segnale di terminazione ricevuto (SIGTERM).")
    print("🔥 PHOENIX: Il container sta per essere distrutto (o migrato).")
    print("🔥 PHOENIX: Arrivederci. La memoria è salva nel Guardian.")
    sys.exit(0)


signal.signal(signal.SIGTERM, handle_sigterm)


# --- FUNZIONI DI INTERFACCIA CON IL GUARDIAN ---

def load_initial_state():
    """Chiede al Sidecar lo stato precedente (Recupero Memoria)"""
    print(f"📡 PHOENIX: Connessione al Guardian su {SIDECAR_URL}...")

    # Retry loop per attendere che il Sidecar sia pronto all'avvio del Pod
    while True:
        try:
            response = requests.get(f"{SIDECAR_URL}/state", timeout=2)
            if response.status_code == 200:
                data = response.json()
                print(f"✅ PHOENIX: Memoria recuperata! {json.dumps(data)}")
                return data
        except requests.exceptions.RequestException:
            print("⏳ PHOENIX: In attesa del Guardian...")
            time.sleep(1)


def save_state_to_sidecar(epoch, loss, accuracy, timer):
    """Invia lo stato corrente al Sidecar per la persistenza"""
    payload = {
        "epoch": epoch,
        "loss": round(loss, 4),
        "accuracy": round(accuracy, 4),
        "mission_timer": timer,
        "status": "TRAINING"
    }

    try:
        requests.post(f"{SIDECAR_URL}/state", json=payload, timeout=1)
        # Non stampiamo nulla per non intasare i log, salvo errori
    except requests.exceptions.RequestException as e:
        print(f"⚠️ PHOENIX: Errore salvataggio stato: {e}")


# --- SIMULAZIONE CARICO (CPU BURNER) ---

def simulate_heavy_computation():
    """
    Simula un calcolo matriciale pesante per consumare CPU.
    In uno scenario reale, qui ci sarebbe PyTorch/TensorFlow.
    """
    size = 300  # Dimensione matrice (aumentare per più carico CPU)
    matrix_a = [[random.random() for _ in range(size)] for _ in range(size)]
    matrix_b = [[random.random() for _ in range(size)] for _ in range(size)]

    # Moltiplicazione semplice O(N^3) per bruciare cicli
    # Nota: Non usiamo numpy per mantenere l'immagine docker 'slim' e pura
    result = [[0] * size for _ in range(size)]
    for i in range(len(matrix_a)):
        for j in range(len(matrix_b[0])):
            for k in range(len(matrix_b)):
                result[i][j] += matrix_a[i][k] * matrix_b[k][j]

    return True


# =============================================================================
#  MAIN LOOP
# =============================================================================

if __name__ == "__main__":
    print("🚀 PHOENIX: Compute Engine Avviato.")

    # 1. RECUPERO STATO
    initial_state = load_initial_state()

    # Se il Guardian ha uno stato valido, lo adottiamo
    if "epoch" in initial_state:
        current_epoch = initial_state["epoch"]
        current_loss = initial_state.get("loss", 1.0)
        current_accuracy = initial_state.get("accuracy", 0.0)
        mission_timer = initial_state.get("mission_timer", 0)
        print(f"🔄 PHOENIX: Ripristino sessione dall'epoca {current_epoch}.")
    else:
        print("🆕 PHOENIX: Nessuna memoria precedente. Inizio nuova sessione.")

    # 2. TRAINING LOOP INFINITO
    while True:
        start_time = time.time()

        # A. Simulazione Carico (AI Training)
        simulate_heavy_computation()

        # B. Aggiornamento Metriche Fittizie
        current_epoch += 1
        mission_timer += 1  # In uno scenario reale userei un delta time

        # Simulazione convergenza modello (Loss scende, Accuracy sale)
        decay = math.exp(-current_epoch / 500.0)
        noise = (random.random() - 0.5) * 0.05
        current_loss = max(0.1, (1.0 * decay) + noise)
        current_accuracy = min(0.99, (1.0 - decay) + noise)

        # C. Output Console
        print(
            f"🧠 TRAIN: Epoch {current_epoch} | Loss: {current_loss:.4f} | Acc: {current_accuracy:.1%} | T+{mission_timer}s")

        # D. Salvataggio nel Sidecar (Checkpoint)
        if current_epoch % SAVE_INTERVAL == 0:
            save_state_to_sidecar(current_epoch, current_loss, current_accuracy, mission_timer)

        # Piccola pausa per non saturare i log (ma il calcolo sopra consuma già tempo)
        # time.sleep(0.5)