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
#  Role: Performs heavy AI training simulations. It has no internal memory;
#  it must synchronize with the sidecar (Guardian) to survive migrations.
# =============================================================================

# Configuration for sidecar communication via localhost (Kubernetes pod loopback)
SIDECAR_URL = os.getenv("STATE_ENDPOINT", "http://localhost:80")
SAVE_INTERVAL = 1  # Frequency of state backups to the sidecar

# Local volatile state - These variables are wiped if the container is killed
current_epoch = 0
current_loss = 1.0
current_accuracy = 0.0


def handle_sigterm(signum, frame):
    """
    Graceful Shutdown Handler: Catches Kubernetes SIGTERM.
    Since Phoenix is stateless, we can exit immediately without corruption,
    as long as the last epoch was saved to the Guardian.
    """
    print("\n🔥 PHOENIX: Termination signal received (SIGTERM).", flush=True)
    sys.exit(0)


# Register the signal handler for clean exits during migration
signal.signal(signal.SIGTERM, handle_sigterm)


def load_initial_state():
    """
    State Recovery (Warm Boot): Connects to the Guardian to retrieve
    the last saved progress before starting the computation loop.
    """
    print(f"📡 PHOENIX: Connecting to Guardian at {SIDECAR_URL}...", flush=True)
    while True:
        try:
            # Polling the Guardian's /state endpoint
            response = requests.get(f"{SIDECAR_URL}/state", timeout=2)
            if response.status_code == 200:
                data = response.json()
                print(f"✅ PHOENIX: Memory recovered! {json.dumps(data)}", flush=True)
                return data
        except requests.exceptions.RequestException:
            # If the Guardian is still booting or being restored by CRIU, wait
            print("⏳ PHOENIX: Waiting for Guardian to wake up...", flush=True)
            time.sleep(1)


def save_state_to_sidecar(epoch, loss, accuracy):
    """
    State Externalization: Sends training metrics to the sidecar for persistence.
    This ensures that if the satellite overheats, our progress is safe in the Guardian's RAM.
    """
    payload = {
        "epoch": epoch,
        "loss": round(loss, 4),
        "accuracy": round(accuracy, 4),
        "status": "TRAINING"
    }
    try:
        # POST request to store the state in the sidecar's memory
        requests.post(f"{SIDECAR_URL}/state", json=payload, timeout=1)
    except requests.exceptions.RequestException as e:
        print(f"⚠️ PHOENIX: State backup error: {e}", flush=True)


def simulate_heavy_computation():
    """
    CPU Stress Test: Simulates a real AI workload by performing
    intensive matrix multiplication to generate heat on the node.
    """
    size = 200
    # Create two random 200x200 matrices
    matrix_a = [[random.random() for _ in range(size)] for _ in range(size)]
    matrix_b = [[random.random() for _ in range(size)] for _ in range(size)]

    # Standard O(n^3) matrix multiplication to saturate the CPU
    result = [[0] * size for _ in range(size)]
    for i in range(len(matrix_a)):
        for j in range(len(matrix_b[0])):
            for k in range(len(matrix_b)):
                result[i][j] += matrix_a[i][k] * matrix_b[k][j]

    time.sleep(0.5)  # Slight delay to regulate the thermal ramp
    return True


if __name__ == "__main__":
    print("🚀 PHOENIX: Compute Engine Started.", flush=True)

    # STEP 1: Sync with the Guardian
    initial_state = load_initial_state()

    # STEP 2: Restore variables if previous data exists (Warm Boot vs Cold Start)
    if "epoch" in initial_state and initial_state["epoch"] > 0:
        current_epoch = initial_state["epoch"]
        current_loss = initial_state.get("loss", 1.0)
        current_accuracy = initial_state.get("accuracy", 0.0)
        print(f"🔄 PHOENIX: Resuming session from Epoch {current_epoch}.", flush=True)
    else:
        print("🆕 PHOENIX: No previous memory found. Starting new session.", flush=True)

    # STEP 3: Infinite Training Loop
    while True:
        # Generate thermal load through computation
        simulate_heavy_computation()

        current_epoch += 1

        # Simulate AI Learning Curve: Loss decreases and Accuracy increases over time
        # Formula: decay = e^(-epoch/500)
        decay = math.exp(-current_epoch / 500.0)
        noise = (random.random() - 0.5) * 0.05  # Add stochastic noise for realism

        current_loss = max(0.1, (1.0 * decay) + noise)
        current_accuracy = max(0.0, min(0.99, (1.0 - decay) + noise))

        print(f"🧠 TRAIN: Epoch {current_epoch} | Loss: {current_loss:.4f} | Acc: {current_accuracy:.1%}", flush=True)

        # STEP 4: Frequent state backup for migration resilience
        if current_epoch % SAVE_INTERVAL == 0:
            save_state_to_sidecar(current_epoch, current_loss, current_accuracy)