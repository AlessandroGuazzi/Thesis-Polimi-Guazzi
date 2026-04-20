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

CRITICAL DESIGN IDEA (THE ASYMMETRIC MIGRATION PATTERN):
This process CANNOT be safely CRIU-restored because NumPy holds CPU-specific
register state. Therefore:

    → On migration, this worker performs a cold boot on the new satellite.
    → It downloads its state (STM/LTM memories) from the Guardian sidecar via HTTP.
    → It instantly continues from where it left off without experiencing "amnesia."

This file is optimized for:
   • extreme speed (vectorized NumPy KNN)
   • state compactness (Strict 25 MB RAM limits)
   • migration resilience (Pre-Freeze Handshakes)
"""

# --- STANDARD LIBRARY IMPORTS ---
# socket/struct: Used to catch raw UDP packets and unpack their binary headers.
import socket
import struct
# zlib: Used to decompress the highly-compressible wildfire masks.
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
# numpy: The mathematical engine. Replaces slow Python loops with C-optimized matrix math.
import numpy as np
from http.server import HTTPServer, BaseHTTPRequestHandler


# =============================================================================
# CONFIGURATION
# =============================================================================

UDP_PORT = 5005  # Ground station sends frames here
# The endpoint of our stateful sidecar. We use localhost because they share a network namespace.
GUARDIAN_URL = os.getenv("STATE_ENDPOINT", "http://localhost:80")
STATE_SYNC_INTERVAL = 2.0  # Periodic lightweight state push to Guardian

# --- SAMKNN MEMORY LIMITS ---
# These limits are strictly tuned to ensure the total RAM footprint of this Python
# process never exceeds the 25 MB limit required for a rapid CRIU migration.
MAX_INSTANCES = 20000
STM_MAX = 5000     # Short-Term Memory: highly adaptable to sudden concept drift.
LTM_MAX = 15000    # Long-Term Memory: stable historical truths about the terrain.
K_NEIGHBOURS = 5   # The algorithm checks the 5 closest historical pixels to vote on fire presence.

# These 12 channels form the feature space (Temperature, Humidity, Wind, etc.)
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

# Short Term Memory (recent examples). We use a deque so older memories automatically
# fall off the end when it reaches STM_MAX, preventing memory leaks.
stm = collections.deque(maxlen=STM_MAX)

# Long Term Memory (stable historical examples)
ltm = []

# Prediction results used by Node Agent (Trigger B - Lateral Tracking)
predicted_fire_mask = [0] * (GRID_W - 2) * (GRID_H - 2)
center_of_mass = {"x": 0.0, "y": 0.0}

# Metrics for the dashboard
sample_count = 0
fire_pixel_count = 0
instances_trained = 0

# Synchronization flags used during the pre-freeze flush handshake with the Guardian
flush_requested = threading.Event()
flush_done = threading.Event()
running = True


# =============================================================================
# SECTION 1: ULTRA-FAST SAMKNN IMPLEMENTATION (The Mathematical Engine)
# =============================================================================

# -----------------------------------------------------------------------------
# Function: fast_knn_predict_batch
# Purpose: Calculates the distance from query pixels to all memories instantly.
# Detail: Uses the algebraic expansion ||x - q||^2 = ||x||^2 - 2x*q + ||q||^2
#         to bypass standard Python for-loops, relying entirely on Matrix Math.
# -----------------------------------------------------------------------------
def fast_knn_predict_batch(Q, X, y, X_sq, k=K_NEIGHBOURS):
    """
    Vectorized KNN for many query points at once.

    Uses algebraic trick:
        ||x - q||² = ||x||² - 2 x·q + ||q||²

    This avoids computing (x-q)**2 explicitly.
    """
    M = Q.shape[0] # Number of queries (e.g., 256 pixels in a chunk)

    # If memory is empty, return -1 (unknown classification)
    if len(y) == 0:
        return np.full(M, -1, dtype=int)

    # Step A: Precompute ||q||² (The squared magnitude of the queries)
    Q_sq = np.sum(Q ** 2, axis=1, keepdims=True)

    # Step B: Compute dot products -2 x·q (The C-optimized matrix multiplication)
    Xq = np.dot(Q, X.T)

    # Step C: Distance matrix using algebra (||x||^2 is passed in as X_sq to save CPU)
    distances = X_sq - (2 * Xq) + Q_sq

    # Step D: Find the 'k' closest neighbors.
    # We use argpartition instead of sort because sorting 15,000 items is slow.
    # argpartition just grabs the top 'k' smallest distances instantly.
    k_actual = min(k, len(y))
    nearest_idx = np.argpartition(distances, k_actual - 1, axis=1)[:, :k_actual]

    # Grab the actual "Fire" or "No Fire" labels of those closest neighbors
    votes = y[nearest_idx]

    # Majority vote: If sum of votes is >= half of k, we predict 1 (Fire). Otherwise 0.
    pred = np.where(np.sum(votes, axis=1) >= (k_actual / 2.0), 1, 0)
    return pred


# -----------------------------------------------------------------------------
# Function: fast_samknn_predict_batch
# Purpose: Orchestrates the predictions from both Short and Long Term Memory.
# -----------------------------------------------------------------------------
def fast_samknn_predict_batch(Q, stm_X, stm_y, stm_X_sq, ltm_X, ltm_y, ltm_X_sq):
    """
    Combine STM and LTM predictions.

    STM has precedence because it models recent concept drift.
    """
    # Ask both memory banks for their predictions
    stm_pred = fast_knn_predict_batch(Q, stm_X, stm_y, stm_X_sq)
    ltm_pred = fast_knn_predict_batch(Q, ltm_X, ltm_y, ltm_X_sq)

    M = Q.shape[0]
    final_pred = np.zeros(M, dtype=int)

    for i in range(M):
        sp = stm_pred[i]
        lp = ltm_pred[i]

        # Resolution logic:
        # If both don't know, guess 0 (No Fire)
        if sp == -1 and lp == -1:
            final_pred[i] = 0
        # If STM doesn't know, trust LTM
        elif sp == -1:
            final_pred[i] = lp
        # If LTM doesn't know, trust STM
        elif lp == -1:
            final_pred[i] = sp
        # If both know, STM gets a weighted advantage (sp * 2) because it represents
        # the most recent reality (concept drift).
        else:
            final_pred[i] = 1 if (sp * 2 + lp) >= 2 else 0

    return final_pred


# -----------------------------------------------------------------------------
# Function: samknn_train
# Purpose: Adds new pixels into the memory banks to continuously adapt.
# -----------------------------------------------------------------------------
def samknn_train(feature_vector, label):
    """
    Add a new example to STM.
    Every 1000 samples, promote consistent ones to LTM.
    """
    global ltm, instances_trained

    # Always push to Short Term Memory first
    stm.append((feature_vector, label))
    instances_trained += 1

    # Every 1000 training instances, trigger the garbage collector / validation loop
    if instances_trained % 1000 == 0:
        _stm_to_ltm_cleaning()


# -----------------------------------------------------------------------------
# Function: _stm_to_ltm_cleaning (Concept Drift & The Infinity Trick)
# Purpose: Tests STM memories against themselves. If a memory is logically
#          consistent, it promotes it to LTM. If it's noise, it gets overwritten.
# -----------------------------------------------------------------------------
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

    # Convert the python lists back into highly optimized NumPy arrays
    stm_X_full = np.array([item[0] for item in stm_list_full])
    stm_y_full = np.array([item[1] for item in stm_list_full])
    stm_X_full_sq = np.sum(stm_X_full ** 2, axis=1)

    k_actual = min(K_NEIGHBOURS, n_full - 1)
    if k_actual == 0:
        return

    # We only check the oldest 1000 items in the STM
    start_idx = max(0, n_full - 1000)
    consistent = []

    for i in range(start_idx, n_full):
        feat = stm_X_full[i]
        lbl = stm_y_full[i]

        # Calculate this memory's distance against ALL other memories in the STM
        q_sq = stm_X_full_sq[i]
        Xq = np.dot(stm_X_full, feat)
        distances = stm_X_full_sq - (2 * Xq) + q_sq

        # === THE INFINITY TRICK ===
        # The distance between a point and ITSELF is mathematically 0.
        # If we leave it as 0, the point will always vote for itself, creating a false validation.
        # By artificially setting its self-distance to Infinity, we force the algorithm
        # to look at its ACTUAL neighbors to see if it belongs there.
        distances[i] = np.inf  # Infinity Trick

        nearest_idx = np.argpartition(distances, k_actual - 1)[:k_actual]
        votes = stm_y_full[nearest_idx]
        pred = 1 if np.sum(votes) >= (k_actual / 2.0) else 0

        # If the neighbors agree with the label, it's a valid pattern. Promote to LTM.
        if pred == lbl:
            consistent.append(stm_list_full[i])

    # Add to LTM and enforce the LTM RAM ceiling (Drop oldest if over 15,000)
    ltm.extend(consistent)
    if len(ltm) > LTM_MAX:
        ltm = ltm[-LTM_MAX:]


# =============================================================================
# SECTION 2: FRAME DECODING (Reconstructing the UDP Payload)
# =============================================================================

# -----------------------------------------------------------------------------
# Function: decode_frame
# Purpose: Takes raw binary bytes from the network and inflates them into 3D grids.
# -----------------------------------------------------------------------------
def decode_frame(raw_udp_bytes):
    """
    Decompress and unpack the UDP payload into channel grids.
    """
    try:
        # Step 1: Inflate the zlib compression
        decompressed = zlib.decompress(raw_udp_bytes)

        # Step 2: Unpack the binary back into Python Floats.
        # The '!f' means network-byte-order (Big Endian) 32-bit floats.
        n_floats = (N_CHANNELS + 1) * GRID_W * GRID_H
        all_values = struct.unpack(f'!{n_floats}f', decompressed)

        # Step 3: Reshape the massive 1D list back into a 2D Grid (64x64) per channel
        channels = {}
        for ci, name in enumerate(FEATURE_CHANNELS):
            start = ci * GRID_W * GRID_H
            end = start + GRID_W * GRID_H
            channels[name] = [
                list(all_values[start:end])[r * GRID_W:(r + 1) * GRID_W]
                for r in range(GRID_H)
            ]

        # Extract the ground truth FireMask separately
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

# -----------------------------------------------------------------------------
# Function: compute_center_of_mass
# Purpose: Calculates the X/Y focal point of the fire. The Node Agent uses this
#          to trigger Lateral Migrations if the fire drifts too close to the edge.
# -----------------------------------------------------------------------------
def compute_center_of_mass(pred_mask_2d):
    """
    Computes the center of mass of predicted fire pixels.
    Used by Node Agent Trigger B.
    """
    total_mass, cx, cy = 0, 0.0, 0.0
    inner_h, inner_w = GRID_H - 2, GRID_W - 2

    # Loop over the grid. Every time we see a '1' (Fire), add its X and Y coordinates.
    for r in range(inner_h):
        for c in range(inner_w):
            if pred_mask_2d[r][c] == 1:
                cx += c
                cy += r
                total_mass += 1

    # If there's no fire, default to the exact center of the screen
    if total_mass == 0:
        return {"x": inner_w / 2.0, "y": inner_h / 2.0}

    # Divide by total fire pixels to find the true mathematical center
    return {"x": cx / total_mass, "y": cy / total_mass}


# =============================================================================
# SECTION 4: FRAME PROCESSING PIPELINE (Spatial Translation)
# =============================================================================

# -----------------------------------------------------------------------------
# Function: process_frame
# Purpose: Translates the 2D grid into 1D features, chunks the RAM, and predicts.
# -----------------------------------------------------------------------------
def process_frame(channels, fire_mask_label):
    """
    Core pipeline executed for every received frame.
    """
    global predicted_fire_mask, center_of_mass, fire_pixel_count, sample_count

    inner_h, inner_w = GRID_H - 2, GRID_W - 2

    # Build STM/LTM matrices (Convert lists to NumPy arrays for speed)
    stm_X = np.array([item[0] for item in stm]) if stm else np.empty((0, 108))
    stm_y = np.array([item[1] for item in stm]) if stm else np.empty(0)
    ltm_X = np.array([item[0] for item in ltm]) if ltm else np.empty((0, 108))
    ltm_y = np.array([item[1] for item in ltm]) if ltm else np.empty(0)

    # Pre-square the memories here so we don't have to do it inside the math loop
    stm_X_sq = np.sum(stm_X ** 2, axis=1) if len(stm_X) > 0 else np.empty(0)
    ltm_X_sq = np.sum(ltm_X ** 2, axis=1) if len(ltm_X) > 0 else np.empty(0)

    # === THE 3x3 SLIDING WINDOW (Spatial Extraction) ===
    # We allocate a giant empty array to hold our queries.
    # Dimensions: 3844 valid pixels × 108 features (12 channels * 9 pixels).
    Q_array = np.zeros((inner_h * inner_w, 108), dtype=float)
    idx = 0

    # This loop shifts our view up/down/left/right to grab the neighbor pixels,
    # flattening the 2D spatial context into a flat 1D array the ML model can read.
    for dr in [-1, 0, 1]:
        for dc in [-1, 0, 1]:
            for name in FEATURE_CHANNELS:
                ch_slice = np.array(channels[name])[1+dr:GRID_H-1+dr, 1+dc:GRID_W-1+dc].flatten()
                Q_array[:, idx] = ch_slice
                idx += 1

    # === CHUNK BATCHING (The RAM Saver) ===
    # If we predicted all 3844 pixels simultaneously, the intermediate matrices
    # allocated by NumPy would shatter our 25 MB RAM ceiling.
    # By strictly limiting the prediction to chunks of 256 pixels, we make our
    # peak RAM footprint highly deterministic and safely below the limit.
    CHUNK_SIZE = 256
    M = Q_array.shape[0]
    preds = np.zeros(M, dtype=int)

    for i in range(0, M, CHUNK_SIZE):
        end_idx = min(i + CHUNK_SIZE, M)
        preds[i:end_idx] = fast_samknn_predict_batch(
            Q_array[i:end_idx], stm_X, stm_y, stm_X_sq, ltm_X, ltm_y, ltm_X_sq
        )

    pred_2d = preds.reshape(inner_h, inner_w).tolist()

    # Selectively train new examples (We train on all actual fires, and 2% of non-fires
    # to prevent the model from heavily biasing toward 'No Fire').
    new_memories = []
    for r in range(1, GRID_H - 1):
        for c in range(1, GRID_W - 1):
            label = int(fire_mask_label[r][c])
            if label == 1 or random.random() < 0.02:
                pixel_idx = (r - 1) * inner_w + (c - 1)
                new_memories.append((Q_array[pixel_idx].tolist(), label))

    for feat, label in new_memories:
        samknn_train(feat, label)

    # Update global state variables for the dashboard and the Node Agent
    predicted_fire_mask = preds.tolist()
    fire_pixel_count = sum(predicted_fire_mask)
    center_of_mass = compute_center_of_mass(pred_2d)
    sample_count += 1


# =============================================================================
# SECTION 5: GUARDIAN STATE SYNC (The Heartbeat & Amnesia Fix)
# =============================================================================

# -----------------------------------------------------------------------------
# Function: build_state_payload
# Purpose: Packages the lightweight metrics to send to the Guardian dashboard.
#          Notice we do NOT send the heavy STM/LTM matrices here.
# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
# Function: sync_state_to_guardian
# Purpose: Runs in a background thread, quietly updating the sidecar every 2 seconds.
# -----------------------------------------------------------------------------
def sync_state_to_guardian():
    """
    Background thread continuously pushing state to Guardian.
    """
    _pending_state = None

    while running:
        try:
            payload = _pending_state if _pending_state else build_state_payload()
            resp = requests.post(f"{GUARDIAN_URL}/state", json=payload, timeout=1)

            # If Guardian is currently freezing (503 status), hold the data for later
            if resp.status_code == 503:
                if _pending_state is None:
                    _pending_state = payload
            else:
                _pending_state = None
        except requests.exceptions.RequestException:
            pass

        time.sleep(STATE_SYNC_INTERVAL)


# -----------------------------------------------------------------------------
# Function: load_initial_state
# Purpose: THE AMNESIA FIX. When a CRIU migration finishes, this container is
#          cold-booted. It asks the Guardian (which survived the freeze) for its
#          historical memory matrices so it can instantly resume tracking.
# -----------------------------------------------------------------------------
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
# SECTION 6: PRE-FREEZE FLUSH SERVER (The TOCTOU Race Condition Fix)
# =============================================================================

# -----------------------------------------------------------------------------
# Class: FlushHandler
# Purpose: When the Node Agent orders an emergency migration, the Guardian hits
#          this endpoint. It halts the ML loop, packages the heavy STM/LTM memory
#          matrices, and blasts them to the Guardian synchronously BEFORE the freeze.
# -----------------------------------------------------------------------------
class FlushHandler(BaseHTTPRequestHandler):
    """
    HTTP endpoint called by Guardian before CRIU.
    Dumps FULL STM and LTM state.
    """
    def do_POST(self):
        if self.path == "/flush":
            # 1. Set the flag to halt the main UDP ML loop (No new data processed)
            flush_requested.set()

            # 2. Package the massive memory matrices
            full_state = build_state_payload()
            full_state["stm"] = [[list(f), lbl] for f, lbl in stm]
            full_state["ltm"] = [[list(f), lbl] for f, lbl in ltm]

            # 3. Synchronously push to the Guardian
            try:
                requests.post(f"{GUARDIAN_URL}/state", json=full_state, timeout=5)
            except Exception:
                pass

            # 4. Tell the main loop we are done dumping data
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
# SECTION 7: UDP INGESTION (The Space Antenna)
# =============================================================================

_udp_queue = queue.Queue(maxsize=4)

# -----------------------------------------------------------------------------
# Function: _udp_ingestion_thread
# Purpose: UDP is stateless. Packets can arrive out of order, get duplicated,
#          or drop entirely. This thread safely reconstructs the chunks into
#          full frames without relying on TCP's safety nets.
# -----------------------------------------------------------------------------
def _udp_ingestion_thread(sock):
    """
    Reconstruct frames from multiple UDP chunks.
    Drops old/incomplete frames.
    """
    frame_buffer = {}

    while running:
        try:
            raw_data, addr = sock.recvfrom(65536)

            # Extract the 6-byte Custom Header injected by the Ground Station
            header = raw_data[:6]
            chunk_data = raw_data[6:]

            # Unpack: Frame ID (4 bytes), Total Chunks (1 byte), Chunk Index (1 byte)
            frame_id, total_chunks, chunk_index = struct.unpack("!IBB", header)

            # Initialize a holding buffer for this specific frame
            if frame_id not in frame_buffer:
                frame_buffer[frame_id] = {'chunks': {}, 'timestamp': time.time()}

            # Store the chunk in its proper slot
            frame_buffer[frame_id]['chunks'][chunk_index] = chunk_data

            # Check if we have received every single piece of this frame
            if len(frame_buffer[frame_id]['chunks']) == total_chunks:
                complete_blob = bytearray()
                for i in range(total_chunks):
                    complete_blob.extend(frame_buffer[frame_id]['chunks'][i])

                # Drop-Tail Queuing: If the ML algorithm is running behind, we intentionally
                # throw away the oldest unread frame to make room for the newest data.
                try:
                    _udp_queue.put_nowait(complete_blob)
                except queue.Full:
                    try:
                        _udp_queue.get_nowait()
                    except queue.Empty:
                        pass
                    _udp_queue.put_nowait(complete_blob)

                # Clear the buffer once assembled
                del frame_buffer[frame_id]

            # Fault Tolerance (Garbage Collection): If a packet was lost in space,
            # purge any incomplete frames older than 5.0 seconds so we don't run out of RAM.
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
    # 1. Ask Guardian if we are recovering from a migration
    load_initial_state()
    # 2. Arm the emergency flush endpoint
    start_flush_server()
    # 3. Start the periodic dashboard sync
    threading.Thread(target=sync_state_to_guardian, daemon=True).start()

    # 4. Bind the UDP Socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # Increase the OS receive buffer so rapid packet bursts aren't dropped by the kernel
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 262144)
    sock.settimeout(1.0)
    sock.bind(("0.0.0.0", UDP_PORT))

    # 5. Start listening to space
    threading.Thread(target=_udp_ingestion_thread, args=(sock,), daemon=True).start()

    # 6. Execute the SML workload
    while running:
        # Check if the Node Agent ordered an emergency freeze. If so, pause execution.
        if flush_requested.is_set():
            flush_done.wait(timeout=10)
            flush_requested.clear()
            flush_done.clear()
            continue

        try:
            # Pull a complete, reassembled binary frame from the UDP thread
            raw_data = _udp_queue.get(timeout=1.0)
        except queue.Empty:
            continue

        # Decompress and translate the binary into 3D scientific arrays
        channels, fire_mask_label = decode_frame(raw_data)
        if channels is None:
            continue

        # Run the SAMKNN math
        process_frame(channels, fire_mask_label)


if __name__ == "__main__":
    print("🛰️  TINYSML WORKER V6: ONLINE", flush=True)
    main()