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
#  Ottimizzato per AI Context (Solo Epoch, Loss, Accuracy)
# =============================================================================

SIDECAR_URL = os.getenv("STATE_ENDPOINT", "http://localhost:80")
SAVE_INTERVAL = 1

# Stato locale (volatile)
current_epoch = 0
current_loss = 1.0
current_accuracy = 0.0

def handle_sigterm(signum, frame):
    print("\n🔥 PHOENIX: Segnale di terminazione ricevuto (SIGTERM).", flush=True)
    sys.exit(0)

signal.signal(signal.SIGTERM, handle_sigterm)

def load_initial_state():
    print(f"📡 PHOENIX: Connessione al Guardian su {SIDECAR_URL}...", flush=True)
    while True:
        try:
            response = requests.get(f"{SIDECAR_URL}/state", timeout=2)
            if response.status_code == 200:
                data = response.json()
                print(f"✅ PHOENIX: Memoria recuperata! {json.dumps(data)}", flush=True)
                return data
        except requests.exceptions.RequestException:
            print("⏳ PHOENIX: In attesa del Guardian...", flush=True)
            time.sleep(1)

def save_state_to_sidecar(epoch, loss, accuracy):
    payload = {
        "epoch": epoch,
        "loss": round(loss, 4),
        "accuracy": round(accuracy, 4),
        "status": "TRAINING"
    }
    try:
        requests.post(f"{SIDECAR_URL}/state", json=payload, timeout=1)
    except requests.exceptions.RequestException as e:
        print(f"⚠️ PHOENIX: Errore salvataggio stato: {e}", flush=True)

def simulate_heavy_computation():
    size = 200
    matrix_a = [[random.random() for _ in range(size)] for _ in range(size)]
    matrix_b = [[random.random() for _ in range(size)] for _ in range(size)]

    result = [[0] * size for _ in range(size)]
    for i in range(len(matrix_a)):
        for j in range(len(matrix_b[0])):
            for k in range(len(matrix_b)):
                result[i][j] += matrix_a[i][k] * matrix_b[k][j]

    time.sleep(0.5)
    return True

if __name__ == "__main__":
    print("🚀 PHOENIX: Compute Engine Avviato.", flush=True)

    initial_state = load_initial_state()

    # Ripristino
    if "epoch" in initial_state and initial_state["epoch"] > 0:
        current_epoch = initial_state["epoch"]
        current_loss = initial_state.get("loss", 1.0)
        current_accuracy = initial_state.get("accuracy", 0.0)
        print(f"🔄 PHOENIX: Ripristino sessione dall'epoca {current_epoch}.", flush=True)
    else:
        print("🆕 PHOENIX: Nessuna memoria precedente. Inizio nuova sessione.", flush=True)

    # Loop Infinito
    while True:
        simulate_heavy_computation()

        current_epoch += 1

        decay = math.exp(-current_epoch / 500.0)
        noise = (random.random() - 0.5) * 0.05
        current_loss = max(0.1, (1.0 * decay) + noise)
        current_accuracy = max(0.0, min(0.99, (1.0 - decay) + noise))

        print(f"🧠 TRAIN: Epoch {current_epoch} | Loss: {current_loss:.4f} | Acc: {current_accuracy:.1%}", flush=True)

        if current_epoch % SAVE_INTERVAL == 0:
            save_state_to_sidecar(current_epoch, current_loss, current_accuracy)