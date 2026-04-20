"""
===============================================================================
TEACHING EDITION — tinysml_worker.py
===============================================================================

This file is the SCIENTIFIC PAYLOAD running on the satellite.

While the Node Agent is responsible for orchestration and migration,
THIS file is the actual workload being protected and migrated.

What this worker does:

1) Receives 64×64 multi-channel sensor grids via UDP from the Ground Station
2) Reconstructs frames from multiple UDP chunks
3) Extracts 3×3 neighbourhood features for EVERY pixel (sliding window)
4) Feeds those features into an ultra-optimized SAMKNN streaming classifier
5) Predicts the wildfire mask for the entire grid
6) Computes the center of mass of the predicted fire
7) Continuously syncs compact state to the Guardian
8) Provides a /flush HTTP endpoint for pre-CRIU state dumping

CRITICAL DESIGN IDEA:
This process CANNOT be safely CRIU-restored because NumPy holds CPU-specific
register state. Therefore:

    → On migration, this worker performs a cold boot
    → Downloads its state from the Guardian
    → Continues from where it left off

This file is optimized for:
   • extreme speed (vectorized NumPy KNN)
   • state compactness
   • migration resilience
"""

import socket
import struct
import zlib
import time
import os
import signal
import sys
import threading
import collections
import queue
import requests
import random
import numpy as np
from http.server import HTTPServer, BaseHTTPRequestHandler


# =============================================================================
# CONFIGURATION
# =============================================================================

UDP_PORT = 5005  # Ground station sends frames here
GUARDIAN_URL = os.getenv("STATE_ENDPOINT", "http://localhost:80")
STATE_SYNC_INTERVAL = 2.0  # Periodic state push to Guardian

# SAMKNN memory limits
MAX_INSTANCES = 20000
STM_MAX = 5000
LTM_MAX = 15000
K_NEIGHBOURS = 5

# These 12 channels form the feature space
FEATURE_CHANNELS = [
    "PrevFireMask", "sph", "th", "elevation", "pdsi", "pr", "population",
    "erc", "NDVI", "tmmn", "vs", "tmmx"
]

LABEL_CHANNEL = "FireMask"
GRID_W = 64
GRID_H = 64
N_CHANNELS = len(FEATURE_CHANNELS)


# =============================================================================
# GLOBAL STATE (what must survive migration)
# =============================================================================

# Short Term Memory (recent examples)
stm = collections.deque(maxlen=STM_MAX)

# Long Term Memory (stable historical examples)
ltm = []

# Prediction results used by Node Agent (Trigger B)
predicted_fire_mask = [0] * (GRID_W - 2) * (GRID_H - 2)
center_of_mass = {"x": 0.0, "y": 0.0}

# Metrics
sample_count = 0
fire_pixel_count = 0
instances_trained = 0

# Synchronization with Guardian during migration
flush_requested = threading.Event()
flush_done = threading.Event()
running = True


# =============================================================================
# SECTION 1: ULTRA-FAST SAMKNN IMPLEMENTATION
# =============================================================================

def fast_knn_predict_batch(Q, X, y, X_sq, k=K_NEIGHBOURS):
    """
    Vectorized KNN for many query points at once.

    Uses algebraic trick:
        ||x - q||² = ||x||² - 2 x·q + ||q||²

    This avoids computing (x-q)**2 explicitly.
    """
    M = Q.shape[0]

    # If memory is empty, return -1 (unknown)
    if len(y) == 0:
        return np.full(M, -1, dtype=int)

    # Precompute ||Q||²
    Q_sq = np.sum(Q ** 2, axis=1, keepdims=True)

    # Compute dot products X·Q
    Xq = np.dot(Q, X.T)

    # Distance matrix using algebra
    distances = X_sq - (2 * Xq) + Q_sq

    # Take only k nearest
    k_actual = min(k, len(y))
    nearest_idx = np.argpartition(distances, k_actual - 1, axis=1)[:, :k_actual]

    votes = y[nearest_idx]

    # Majority vote
    pred = np.where(np.sum(votes, axis=1) >= (k_actual / 2.0), 1, 0)
    return pred


def fast_samknn_predict_batch(Q, stm_X, stm_y, stm_X_sq, ltm_X, ltm_y, ltm_X_sq):
    """
    Combine STM and LTM predictions.

    STM has precedence because it models recent concept drift.
    """
    stm_pred = fast_knn_predict_batch(Q, stm_X, stm_y, stm_X_sq)
    ltm_pred = fast_knn_predict_batch(Q, ltm_X, ltm_y, ltm_X_sq)

    M = Q.shape[0]
    final_pred = np.zeros(M, dtype=int)

    for i in range(M):
        sp = stm_pred[i]
        lp = ltm_pred[i]

        if sp == -1 and lp == -1:
            final_pred[i] = 0
        elif sp == -1:
            final_pred[i] = lp
        elif lp == -1:
            final_pred[i] = sp
        else:
            final_pred[i] = 1 if (sp * 2 + lp) >= 2 else 0

    return final_pred


def samknn_train(feature_vector, label):
    """
    Add a new example to STM.
    Every 1000 samples, promote consistent ones to LTM.
    """
    global ltm, instances_trained

    stm.append((feature_vector, label))
    instances_trained += 1

    if instances_trained % 1000 == 0:
        _stm_to_ltm_cleaning()


def _stm_to_ltm_cleaning():
    """
    Promote STM examples to LTM if they are self-consistent.
    Uses the Infinity Trick to avoid self-neighbour.
    """
    global ltm

    stm_list_full = list(stm)
    n_full = len(stm_list_full)
    if n_full == 0:
        return

    stm_X_full = np.array([item[0] for item in stm_list_full])
    stm_y_full = np.array([item[1] for item in stm_list_full])
    stm_X_full_sq = np.sum(stm_X_full ** 2, axis=1)

    k_actual = min(K_NEIGHBOURS, n_full - 1)
    if k_actual == 0:
        return

    start_idx = max(0, n_full - 1000)
    consistent = []

    for i in range(start_idx, n_full):
        feat = stm_X_full[i]
        lbl = stm_y_full[i]

        q_sq = stm_X_full_sq[i]
        Xq = np.dot(stm_X_full, feat)
        distances = stm_X_full_sq - (2 * Xq) + q_sq

        distances[i] = np.inf  # Infinity Trick

        nearest_idx = np.argpartition(distances, k_actual - 1)[:k_actual]
        votes = stm_y_full[nearest_idx]
        pred = 1 if np.sum(votes) >= (k_actual / 2.0) else 0

        if pred == lbl:
            consistent.append(stm_list_full[i])

    ltm.extend(consistent)
    if len(ltm) > LTM_MAX:
        ltm = ltm[-LTM_MAX:]


# =============================================================================
# SECTION 2: FRAME DECODING
# =============================================================================

def decode_frame(raw_udp_bytes):
    """
    Decompress and unpack the UDP payload into channel grids.
    """
    try:
        decompressed = zlib.decompress(raw_udp_bytes)

        n_floats = (N_CHANNELS + 1) * GRID_W * GRID_H
        all_values = struct.unpack(f'!{n_floats}f', decompressed)

        channels = {}
        for ci, name in enumerate(FEATURE_CHANNELS):
            start = ci * GRID_W * GRID_H
            end = start + GRID_W * GRID_H
            channels[name] = [
                list(all_values[start:end])[r * GRID_W:(r + 1) * GRID_W]
                for r in range(GRID_H)
            ]

        fire_start = N_CHANNELS * GRID_W * GRID_H
        fire_mask = [
            list(all_values[fire_start:fire_start + GRID_W * GRID_H])[r * GRID_W:(r + 1) * GRID_W]
            for r in range(GRID_H)
        ]

        return channels, fire_mask
    except Exception:
        return None, None


# =============================================================================
# SECTION 3: CENTER OF MASS
# =============================================================================

def compute_center_of_mass(pred_mask_2d):
    """
    Computes the center of mass of predicted fire pixels.
    Used by Node Agent Trigger B.
    """
    total_mass, cx, cy = 0, 0.0, 0.0
    inner_h, inner_w = GRID_H - 2, GRID_W - 2

    for r in range(inner_h):
        for c in range(inner_w):
            if pred_mask_2d[r][c] == 1:
                cx += c
                cy += r
                total_mass += 1

    if total_mass == 0:
        return {"x": inner_w / 2.0, "y": inner_h / 2.0}

    return {"x": cx / total_mass, "y": cy / total_mass}


# =============================================================================
# SECTION 4: FRAME PROCESSING PIPELINE
# =============================================================================

def process_frame(channels, fire_mask_label):
    """
    Core pipeline executed for every received frame.
    """
    global predicted_fire_mask, center_of_mass, fire_pixel_count, sample_count

    inner_h, inner_w = GRID_H - 2, GRID_W - 2

    # Build STM/LTM matrices
    stm_X = np.array([item[0] for item in stm]) if stm else np.empty((0, 108))
    stm_y = np.array([item[1] for item in stm]) if stm else np.empty(0)
    ltm_X = np.array([item[0] for item in ltm]) if ltm else np.empty((0, 108))
    ltm_y = np.array([item[1] for item in ltm]) if ltm else np.empty(0)

    stm_X_sq = np.sum(stm_X ** 2, axis=1) if len(stm_X) > 0 else np.empty(0)
    ltm_X_sq = np.sum(ltm_X ** 2, axis=1) if len(ltm_X) > 0 else np.empty(0)

    # Build the giant feature matrix for ALL pixels
    Q_array = np.zeros((inner_h * inner_w, 108), dtype=float)
    idx = 0

    for dr in [-1, 0, 1]:
        for dc in [-1, 0, 1]:
            for name in FEATURE_CHANNELS:
                ch_slice = np.array(channels[name])[1+dr:GRID_H-1+dr, 1+dc:GRID_W-1+dc].flatten()
                Q_array[:, idx] = ch_slice
                idx += 1

    # Predict in chunks to avoid memory spikes
    CHUNK_SIZE = 256
    M = Q_array.shape[0]
    preds = np.zeros(M, dtype=int)

    for i in range(0, M, CHUNK_SIZE):
        end_idx = min(i + CHUNK_SIZE, M)
        preds[i:end_idx] = fast_samknn_predict_batch(
            Q_array[i:end_idx], stm_X, stm_y, stm_X_sq, ltm_X, ltm_y, ltm_X_sq
        )

    pred_2d = preds.reshape(inner_h, inner_w).tolist()

    # Selectively train new examples
    new_memories = []
    for r in range(1, GRID_H - 1):
        for c in range(1, GRID_W - 1):
            label = int(fire_mask_label[r][c])
            if label == 1 or random.random() < 0.02:
                pixel_idx = (r - 1) * inner_w + (c - 1)
                new_memories.append((Q_array[pixel_idx].tolist(), label))

    for feat, label in new_memories:
        samknn_train(feat, label)

    predicted_fire_mask = preds.tolist()
    fire_pixel_count = sum(predicted_fire_mask)
    center_of_mass = compute_center_of_mass(pred_2d)
    sample_count += 1


# =============================================================================
# SECTION 5: GUARDIAN STATE SYNC
# =============================================================================

def build_state_payload():
    """
    Minimal state sent periodically to Guardian.
    """
    return {
        "predicted_fire_mask": predicted_fire_mask,
        "center_of_mass": center_of_mass,
        "fire_pixel_count": fire_pixel_count,
        "sample_count": sample_count,
        "instances_trained": instances_trained,
        "stm_size": len(stm),
        "ltm_size": len(ltm),
        "status": "TRACKING"
    }


def sync_state_to_guardian():
    """
    Background thread continuously pushing state to Guardian.
    """
    _pending_state = None

    while running:
        try:
            payload = _pending_state if _pending_state else build_state_payload()
            resp = requests.post(f"{GUARDIAN_URL}/state", json=payload, timeout=1)

            if resp.status_code == 503:
                if _pending_state is None:
                    _pending_state = payload
            else:
                _pending_state = None
        except requests.exceptions.RequestException:
            pass

        time.sleep(STATE_SYNC_INTERVAL)


def load_initial_state():
    """
    On cold boot, restore last state from Guardian.
    """
    global predicted_fire_mask, center_of_mass, fire_pixel_count, sample_count, instances_trained

    while True:
        try:
            resp = requests.get(f"{GUARDIAN_URL}/state", timeout=2)
            if resp.status_code == 200:
                data = resp.json()
                if "sample_count" in data and data["sample_count"] > 0:
                    predicted_fire_mask = data.get("predicted_fire_mask", predicted_fire_mask)
                    center_of_mass = data.get("center_of_mass", center_of_mass)
                    fire_pixel_count = data.get("fire_pixel_count", 0)
                    sample_count = data.get("sample_count", 0)
                    instances_trained = data.get("instances_trained", 0)

                    # --- THE AMNESIA FIX ---
                    # Explicitly extract the memory arrays from the JSON payload
                    # and convert them back into tuples (feature_vector, label)
                    if "stm" in data:
                        stm.clear()
                        stm.extend([(f, l) for f, l in data["stm"]])
                        print(f"🧠 WORKER: Restored {len(stm)} STM memories from Guardian.", flush=True)

                    if "ltm" in data:
                        ltm.clear()
                        ltm.extend([(f, l) for f, l in data["ltm"]])
                        print(f"🧠 WORKER: Restored {len(ltm)} LTM memories from Guardian.", flush=True)
                return
        except requests.exceptions.RequestException:
            time.sleep(1)


# =============================================================================
# SECTION 6: PRE-FREEZE FLUSH SERVER
# =============================================================================

class FlushHandler(BaseHTTPRequestHandler):
    """
    HTTP endpoint called by Guardian before CRIU.
    Dumps FULL STM and LTM state.
    """
    def do_POST(self):
        if self.path == "/flush":
            flush_requested.set()

            full_state = build_state_payload()
            full_state["stm"] = [[list(f), lbl] for f, lbl in stm]
            full_state["ltm"] = [[list(f), lbl] for f, lbl in ltm]

            try:
                requests.post(f"{GUARDIAN_URL}/state", json=full_state, timeout=5)
            except Exception:
                pass

            flush_done.set()
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"status": "FLUSHED"}')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass


def start_flush_server():
    """
    Starts HTTP server listening for /flush.
    """
    server = HTTPServer(("0.0.0.0", 9000), FlushHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()


# =============================================================================
# SECTION 7: UDP INGESTION
# =============================================================================

_udp_queue = queue.Queue(maxsize=4)

def _udp_ingestion_thread(sock):
    """
    Reconstruct frames from multiple UDP chunks.
    Drops old/incomplete frames.
    """
    frame_buffer = {}

    while running:
        try:
            raw_data, addr = sock.recvfrom(65536)

            header = raw_data[:6]
            chunk_data = raw_data[6:]

            frame_id, total_chunks, chunk_index = struct.unpack("!IBB", header)

            if frame_id not in frame_buffer:
                frame_buffer[frame_id] = {'chunks': {}, 'timestamp': time.time()}

            frame_buffer[frame_id]['chunks'][chunk_index] = chunk_data

            if len(frame_buffer[frame_id]['chunks']) == total_chunks:
                complete_blob = bytearray()
                for i in range(total_chunks):
                    complete_blob.extend(frame_buffer[frame_id]['chunks'][i])

                try:
                    _udp_queue.put_nowait(complete_blob)
                except queue.Full:
                    try:
                        _udp_queue.get_nowait()
                    except queue.Empty:
                        pass
                    _udp_queue.put_nowait(complete_blob)

                del frame_buffer[frame_id]

            # Remove stale partial frames
            current_time = time.time()
            stale = [fid for fid, data in frame_buffer.items()
                     if current_time - data['timestamp'] > 5.0]
            for fid in stale:
                del frame_buffer[fid]

        except socket.timeout:
            continue
        except Exception:
            pass


def handle_sigterm(signum, frame):
    """
    Graceful shutdown.
    """
    global running
    running = False
    sys.exit(0)


signal.signal(signal.SIGTERM, handle_sigterm)


# =============================================================================
# MAIN LOOP
# =============================================================================

def main():
    """
    Main runtime loop.
    """
    load_initial_state()
    start_flush_server()

    threading.Thread(target=sync_state_to_guardian, daemon=True).start()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 262144)
    sock.settimeout(1.0)
    sock.bind(("0.0.0.0", UDP_PORT))

    threading.Thread(target=_udp_ingestion_thread, args=(sock,), daemon=True).start()

    while running:
        if flush_requested.is_set():
            flush_done.wait(timeout=10)
            flush_requested.clear()
            flush_done.clear()
            continue

        try:
            raw_data = _udp_queue.get(timeout=1.0)
        except queue.Empty:
            continue

        channels, fire_mask_label = decode_frame(raw_data)
        if channels is None:
            continue

        process_frame(channels, fire_mask_label)


if __name__ == "__main__":
    print("🛰️  TINYSML WORKER V6: ONLINE", flush=True)
    main()