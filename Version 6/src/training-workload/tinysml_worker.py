"""
SPACE CLOUD V6 - TINYSML WORKER (Stateful 2D Wildfire Tracker)
==============================================================
Role: This is the satellite's scientific payload — it receives 64×64 sensor grids
      from the Ground Station via UDP, runs a sliding-window feature extraction,
      and feeds each pixel's neighbourhood into a SAMKNN streaming classifier.

The state (STM + LTM + current fire mask) is the object that CRIU will freeze
and restore across multi-hop migrations. It must stay ≤ 25 MB total.

Dependencies: stdlib only — socket, json, struct, zlib, math, http.server,
              threading, signal, time, os, collections, requests.
              NO numpy, sklearn, torch, or any C-extension. CRIU-safe.
"""

import socket
import struct
import zlib
import json
import math
import time
import os
import signal
import sys
import threading
import collections
import queue          # Issue #5: async UDP ingestion buffer
import requests
from http.server import HTTPServer, BaseHTTPRequestHandler


# =============================================================================
# CONFIGURATION
# =============================================================================

# Port where this worker listens for incoming 64×64 UDP data frames
UDP_PORT = 5005

# Port where the Guardian sidecar lives (same Pod, same network namespace)
GUARDIAN_URL = os.getenv("STATE_ENDPOINT", "http://localhost:80")

# How often (in seconds) we push our state to the Guardian's RAM
STATE_SYNC_INTERVAL = 2.0

# How often (seconds) we push a state update while idle (heartbeat)
HEARTBEAT_INTERVAL = 5.0

# SAMKNN memory bounds — strictly enforced to guarantee the ≤ 25 MB RAM budget.
# Each instance = 108 float64 + 1 label = 865 bytes → 20,000 × 865 ≈ 17 MB
MAX_INSTANCES = 700
STM_MAX = 200    # Short-Term Memory window size
LTM_MAX = 500   # Long-Term Memory maximum size

# kNN inference parameter
K_NEIGHBOURS = 5

# The 12 input channel names, in the order they are packed by the streamer.
# These names match the Kaggle "Next Day Wildfire Spread" dataset exactly.
FEATURE_CHANNELS = [
    "PrevFireMask",  # Previous day's fire presence (0 or 1)
    "sph",           # Specific humidity
    "th",            # Wind direction (potential temperature proxy)
    "elevation",     # Terrain height above sea level
    "pdsi",          # Palmer Drought Severity Index (moisture deficit)
    "pr",            # Precipitation
    "population",    # Population density near the pixel
    "erc",           # Energy Release Component (fire behavior)
    "NDVI",          # Vegetation greenness index
    "tmmn",          # Minimum daily temperature
    "vs",            # Wind speed
    "tmmx",          # Maximum daily temperature
]

# The 13th channel is the ground-truth label (next-day fire spread)
LABEL_CHANNEL = "FireMask"

# Grid dimensions
GRID_W = 64
GRID_H = 64
N_CHANNELS = len(FEATURE_CHANNELS)  # 12

# Lateral tracking threshold: if Center of Mass crosses this many pixels from
# the edge, the Node Agent will initiate a lateral migration (see §4.2)
LATERAL_THRESHOLD = 8  # pixels from the border


# =============================================================================
# GLOBAL STATE — this is what CRIU serializes across migrations
# =============================================================================

# SAMKNN Short-Term Memory: deque of (feature_108, label) tuples
# A deque automatically discards the oldest instance when full
stm = collections.deque(maxlen=STM_MAX)

# SAMKNN Long-Term Memory: list of (feature_108, label) tuples
# Manually capped at LTM_MAX to prevent unbounded growth
ltm = []

# Latest predicted fire mask (62×62 flattened into a list of ints)
# Initialized to all-zeros (no fire detected yet)
predicted_fire_mask = [0] * (GRID_W - 2) * (GRID_H - 2)

# Current Center of Mass of fire-positive pixels
center_of_mass = {"x": 0.0, "y": 0.0}

# Statistics counters
sample_count = 0          # Total frames processed since boot / last migration restore
fire_pixel_count = 0      # Number of fire-positive pixels in the latest prediction

# Controls the flush endpoint: when True, UDP ingestion is paused for state sync
flush_requested = threading.Event()
flush_done = threading.Event()

# Controls the main loop: set to False by SIGTERM
running = True


# =============================================================================
# SECTION 1: PURE-PYTHON SAMKNN IMPLEMENTATION
# Self-Adjusting Memory k-Nearest Neighbor streaming classifier.
# Works on concept-drifting streams by maintaining separate STM and LTM.
# =============================================================================

def euclidean_distance_sq(a, b):
    """
    Computes the squared Euclidean distance between two equal-length lists.
    We use squared distance to avoid the sqrt call — safe for KNN ranking.
    """
    d = 0.0
    for x, y in zip(a, b):
        diff = x - y
        d += diff * diff
    return d


def knn_predict(query_vector, memory_pool, k=K_NEIGHBOURS):
    """
    Performs k-Nearest Neighbour classification over a given memory pool.

    query_vector: 108-float feature vector for the pixel being classified
    memory_pool:  list/deque of (feature_108, label) tuples
    Returns:      0 or 1 (majority vote among k nearest neighbours),
                  or -1 if the pool is empty
    """
    if not memory_pool:
        return -1  # No memory yet — cannot classify

    # Sort all instances by distance to the query point
    distances = []
    for feat, label in memory_pool:
        dist_sq = euclidean_distance_sq(query_vector, feat)
        distances.append((dist_sq, label))

    distances.sort(key=lambda x: x[0])

    # Majority vote among the k closest instances
    votes = [lbl for _, lbl in distances[:k]]
    return 1 if sum(votes) >= (k / 2.0) else 0


def samknn_predict(query_vector):
    """
    SAMKNN prediction combining STM and LTM.

    The STM captures recent concept; the LTM captures historical patterns.
    We query both independently and combine via weighted majority vote:
    - STM vote is weighted 2× (recent data is more reliable on a drifting stream)
    - LTM vote is weighted 1×
    """
    stm_pred = knn_predict(query_vector, stm, K_NEIGHBOURS)
    ltm_pred = knn_predict(query_vector, ltm, K_NEIGHBOURS)

    if stm_pred == -1 and ltm_pred == -1:
        return 0  # No memory at all → default to "no fire"
    if stm_pred == -1:
        return ltm_pred
    if ltm_pred == -1:
        return stm_pred

    # Weighted vote: STM is twice as important as LTM
    return 1 if (stm_pred * 2 + ltm_pred) >= 2 else 0


def samknn_train(feature_vector, label):
    """
    Adds a new labelled instance to the Short-Term Memory.

    The deque automatically evicts the oldest instance once maxlen is reached.
    Every CLEAN_CYCLE new STM instances, we run the LTM cleaning step.
    """
    global ltm

    # Add to STM (deque auto-evicts oldest if full)
    stm.append((feature_vector, label))

    # Periodically move stable STM knowledge to Long-Term Memory
    # We do this every 500 new instances to amortise the cost
    if len(stm) % 500 == 0 and len(stm) == STM_MAX:
        _stm_to_ltm_cleaning()


def _stm_to_ltm_cleaning():
    """
    Moves consistently classified STM instances to LTM, discards noisy ones.

    Algorithm:
    1. For each instance in STM, classify it using the *rest of STM* (leave-one-out).
    2. If its label matches the STM prediction → "consistent" → migrate to LTM.
    3. If it contradicts → "noisy" → discard it.
    This step removes contradictory instances that would corrupt the LTM.
    """
    global ltm
    consistent = []

    stm_list = list(stm)   # Snapshot for the leave-one-out check

    for i, (feat, lbl) in enumerate(stm_list):
        # Build a temporary pool excluding this instance
        others = stm_list[:i] + stm_list[i+1:]
        pred = knn_predict(feat, others, K_NEIGHBOURS)
        if pred == lbl:
            # This instance is consistent with its neighbourhood → keep it
            consistent.append((feat, lbl))

    # Append consistent instances to LTM, then enforce the LTM size cap
    ltm.extend(consistent)
    if len(ltm) > LTM_MAX:
        # Drop oldest LTM instances to stay within the 15,000-instance cap
        ltm = ltm[-LTM_MAX:]


# =============================================================================
# SECTION 2: FRAME DESERIALIZATION
# The data streamer sends zlib-compressed binary blobs.
# We reverse the encoding here.
# =============================================================================

def decode_frame(raw_udp_bytes):
    """
    Converts a received UDP datagram back into a structured 64×64 grid.

    The streamer encodes 13 channels × 4096 float32 values = 53,248 floats.
    Order: [PrevFireMask, sph, th, elevation, pdsi, pr, population,
            erc, NDVI, tmmn, vs, tmmx, FireMask]

    Returns: (channels_dict, fire_mask_label) or (None, None) on error
    """
    try:
        # Step 1: Decompress the payload (streamer uses zlib.compress())
        decompressed = zlib.decompress(raw_udp_bytes)

        # Step 2: Unpack all 53,248 float32 values from the binary blob
        # '!' = network byte order, 'f' = float32
        n_floats = (N_CHANNELS + 1) * GRID_W * GRID_H   # 13 × 4096
        all_values = struct.unpack(f'!{n_floats}f', decompressed)

        # Step 3: Slice and reshape each channel into a GRID_H × GRID_W 2D list
        channels = {}
        for ci, name in enumerate(FEATURE_CHANNELS):
            start = ci * GRID_W * GRID_H
            end = start + GRID_W * GRID_H
            flat = list(all_values[start:end])
            # Reshape flat list into 64 rows of 64 values each
            channels[name] = [flat[r * GRID_W:(r + 1) * GRID_W] for r in range(GRID_H)]

        # The last channel (channel 12) is the FireMask ground-truth label
        fire_start = N_CHANNELS * GRID_W * GRID_H
        fire_flat = list(all_values[fire_start:fire_start + GRID_W * GRID_H])
        fire_mask = [[fire_flat[r * GRID_W + c] for c in range(GRID_W)] for r in range(GRID_H)]

        return channels, fire_mask

    except Exception as e:
        print(f"⚠️ WORKER: Frame decode error: {e}", flush=True)
        return None, None


# =============================================================================
# SECTION 3: 3×3 SLIDING WINDOW FEATURE EXTRACTION
# =============================================================================

def extract_3x3_features(channels, row, col):
    """
    Builds a 108-float feature vector for pixel (row, col) by flattening
    the 3×3 neighbourhood across all 12 input channels.

    Think of it as: for each of the 9 pixels in the 3×3 patch,
    read all 12 feature values → 9 × 12 = 108 floats total.

    row, col must satisfy 1 ≤ row ≤ 62 and 1 ≤ col ≤ 62 (interior pixels only).
    """
    feature_vector = []
    for dr in [-1, 0, 1]:      # 3 vertical neighbours
        for dc in [-1, 0, 1]:  # 3 horizontal neighbours
            r = row + dr
            c = col + dc
            for channel_name in FEATURE_CHANNELS:
                # Read the feature value for this pixel from the channel grid
                feature_vector.append(channels[channel_name][r][c])
    return feature_vector  # Length is exactly 108


# =============================================================================
# SECTION 4: CENTER OF MASS COMPUTATION
# Used by Trigger B in the Node Agent to detect lateral fire spread.
# =============================================================================

def compute_center_of_mass(pred_mask_2d):
    """
    Computes the 2D centroid (average x, average y) of all fire-positive pixels.

    pred_mask_2d: a (GRID_H-2) × (GRID_W-2) list of binary values (0 or 1)

    Returns: {"x": float, "y": float}
    If no fire pixels are found, returns the centre of the grid.
    """
    total_mass = 0
    cx = 0.0
    cy = 0.0

    inner_h = GRID_H - 2  # 62
    inner_w = GRID_W - 2  # 62

    for r in range(inner_h):
        for c in range(inner_w):
            if pred_mask_2d[r][c] == 1:
                cx += c       # Horizontal position (column index)
                cy += r       # Vertical position (row index)
                total_mass += 1

    if total_mass == 0:
        # No fire detected → return geometric centre
        return {"x": inner_w / 2.0, "y": inner_h / 2.0}

    return {"x": cx / total_mass, "y": cy / total_mass}


# =============================================================================
# SECTION 5: FRAME PROCESSING PIPELINE
# Runs the full inference loop for one 64×64 observation frame.
# =============================================================================

def process_frame(channels, fire_mask_label):
    """
    Full processing pipeline for a single 64×64 satellite observation:
    1. For every interior pixel, extract a 108-float 3×3 feature vector.
    2. Run SAMKNN prediction (fire tomorrow: 1 or 0).
    3. Train the SAMKNN with the ground-truth label.
    4. Accumulate predictions into the predicted fire mask.
    5. Compute the Center of Mass of predicted fire pixels.

    Updates the global state variables used by the Guardian sync and flush.
    """
    global predicted_fire_mask, center_of_mass, fire_pixel_count, sample_count

    inner_h = GRID_H - 2  # 62 usable rows (skip border pixels)
    inner_w = GRID_W - 2  # 62 usable columns

    # Temporary 2D grid to collect predictions for this frame
    pred_2d = [[0] * inner_w for _ in range(inner_h)]

    for r in range(1, GRID_H - 1):   # Row 1 to 62 (interior only)
        for c in range(1, GRID_W - 1):  # Col 1 to 62

            # Extract the 108-float neighbourhood feature vector
            feat = extract_3x3_features(channels, r, c)

            # Predict whether this pixel will be on fire tomorrow
            pred = samknn_predict(feat)

            # Store the prediction in the 2D output grid
            pred_2d[r - 1][c - 1] = pred

            # Get the ground-truth label for this pixel (for online training)
            label = int(fire_mask_label[r][c])

            # Train the SAMKNN with the correct answer for this pixel
            samknn_train(feat, label)

    # Flatten the 2D prediction grid into a 1D list for JSON serialization
    predicted_fire_mask = [pred_2d[r][c] for r in range(inner_h) for c in range(inner_w)]

    # Count how many pixels are predicted to be on fire
    fire_pixel_count = sum(predicted_fire_mask)

    # Compute the Center of Mass for Trigger B lateral tracking
    center_of_mass = compute_center_of_mass(pred_2d)

    sample_count += 1

    print(
        f"🔥 WORKER: Frame {sample_count} processed | "
        f"Fire pixels: {fire_pixel_count} | "
        f"CoM: ({center_of_mass['x']:.1f}, {center_of_mass['y']:.1f}) | "
        f"STM: {len(stm)} | LTM: {len(ltm)}",
        flush=True
    )


# =============================================================================
# SECTION 6: GUARDIAN STATE SYNCHRONIZATION
# Sends the current SAMKNN state to the Guardian's RAM for CRIU persistence.
# =============================================================================

def build_state_payload():
    """
    Packages the complete SAMKNN state into a JSON-serialisable dict.
    This is what the Guardian stores in its RAM — and therefore what CRIU
    freezes and restores across satellite hops.
    """
    return {
        # The fire prediction output for the latest frame
        "predicted_fire_mask": predicted_fire_mask,
        # Centroid of fire pixels — used by Node Agent Trigger B
        "center_of_mass": center_of_mass,
        "fire_pixel_count": fire_pixel_count,
        # How many frames have been processed (survives migration)
        "sample_count": sample_count,
        # Memory utilisation (for the dashboard gauges)
        "stm_size": len(stm),
        "ltm_size": len(ltm),
        # Worker self-report status
        "status": "TRACKING"
    }


def sync_state_to_guardian():
    """
    Periodically sends our state to the Guardian via a localhost HTTP POST.
    This runs in its own daemon thread and does not block frame processing.

    Issue #4 fix: If the Guardian returns 503 (flightMode), we save the
    payload in a one-slot retry buffer (_pending_state) instead of discarding
    it silently. On the next sync cycle after warm boot, the buffered state
    is retried first, ensuring the ML delta generated during the freeze
    window is never lost.
    """
    _pending_state = None  # One-slot retry buffer for 503 rejections

    while running:
        try:
            # If there is a buffered state from a previous 503, retry it first
            payload = _pending_state if _pending_state else build_state_payload()

            resp = requests.post(f"{GUARDIAN_URL}/state", json=payload, timeout=1)

            if resp.status_code == 503:
                # Guardian is in flightMode — buffer this payload for retry
                # instead of silently dropping it (Issue #4)
                if _pending_state is None:
                    _pending_state = payload
                    print("⚠️ WORKER: Guardian returned 503 — state buffered for retry.", flush=True)
            else:
                # Success (200) or any other code — clear the retry buffer
                _pending_state = None

        except requests.exceptions.RequestException:
            # Guardian is temporarily unavailable (e.g., during CRIU freeze or boot)
            # Keep the pending state if it exists; it will be retried next cycle
            pass
        time.sleep(STATE_SYNC_INTERVAL)


def load_initial_state():
    """
    Warm-Boot Recovery: After a CRIU migration restore, the Guardian holds
    our previous state. We fetch it here and reinstate the STM/LTM contents
    so training can resume exactly from where it left off.
    """
    global predicted_fire_mask, center_of_mass, fire_pixel_count, sample_count

    print(f"📡 WORKER: Connecting to Guardian at {GUARDIAN_URL}...", flush=True)
    while True:
        try:
            resp = requests.get(f"{GUARDIAN_URL}/state", timeout=2)
            if resp.status_code == 200:
                data = resp.json()
                # Recover all state fields if they exist (Warm Boot scenario)
                if "sample_count" in data and data["sample_count"] > 0:
                    predicted_fire_mask = data.get("predicted_fire_mask", predicted_fire_mask)
                    center_of_mass      = data.get("center_of_mass", center_of_mass)
                    fire_pixel_count    = data.get("fire_pixel_count", 0)
                    sample_count        = data.get("sample_count", 0)
                    # Note: STM and LTM are not stored in the Guardian to keep
                    # the JSON size small. The flush endpoint handles full serialization.
                    print(f"🔄 WORKER: Warm boot — resuming from frame {sample_count}.", flush=True)
                else:
                    print("🆕 WORKER: Cold start — no previous state found.", flush=True)
                return
        except requests.exceptions.RequestException:
            print("⏳ WORKER: Waiting for Guardian to wake up...", flush=True)
            time.sleep(1)


# =============================================================================
# SECTION 7: PRE-FREEZE FLUSH ENDPOINT (localhost:9000/flush)
# Called by the Guardian just before CRIU freezes the pod.
# Must be intra-Pod only — never touches external network.
# =============================================================================

class FlushHandler(BaseHTTPRequestHandler):
    """
    Minimal HTTP server that handles the single POST /flush request.
    When the Guardian detects /tmp/flush_state, it calls this endpoint.
    We pause UDP ingestion, serialize everything, and POST it back.
    """

    def do_POST(self):
        if self.path == "/flush":
            # Signal the main UDP loop to pause at the next safe point
            flush_requested.set()

            # Build the complete state, including STM and LTM for full recovery
            # (stored temporarily — the Guardian will write this to its RAM)
            full_state = build_state_payload()
            # Serialize STM and LTM as lists of [feature_list, label] pairs
            full_state["stm"] = [[list(f), lbl] for f, lbl in stm]
            full_state["ltm"] = [[list(f), lbl] for f, lbl in ltm]

            # Push the full state to the Guardian right now (synchronous)
            try:
                requests.post(f"{GUARDIAN_URL}/state", json=full_state, timeout=5)
                print("💾 WORKER: Pre-freeze flush complete. SAMKNN state secured.", flush=True)
            except Exception as e:
                print(f"⚠️ WORKER: Flush POST failed: {e}", flush=True)

            # Signal back to the flush_requested waiter that we are done
            flush_done.set()

            # Respond 200 OK to the Guardian
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"status": "FLUSHED"}')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        # Suppress default HTTP request logs to keep the console clean
        pass


def start_flush_server():
    """Starts the /flush endpoint on port 9000 in a background daemon thread."""
    server = HTTPServer(("0.0.0.0", 9000), FlushHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print("🔌 WORKER: Flush server listening on :9000", flush=True)


# =============================================================================
# SECTION 8: ASYNC UDP INGESTION (Issue #5 fix)
# Decouples network I/O from SAMKNN compute using a producer-consumer queue.
# The background thread drains the OS UDP buffer as fast as the kernel delivers
# datagrams; the main thread pulls frames at its own pace.
# =============================================================================

# Bounded queue: keeps at most 4 frames in flight (1 current + 3 buffered).
# If SAMKNN falls behind, older frames are silently dropped at the queue level
# rather than at the OS UDP buffer level (where we have no control or visibility).
_udp_queue = queue.Queue(maxsize=4)


def _udp_ingestion_thread(sock):
    """
    Background thread that continuously reads UDP datagrams and pushes them
    into the queue. Runs until the 'running' flag is cleared by SIGTERM.

    If the queue is full (SAMKNN is too slow), the oldest frame is popped
    and the new one is inserted — this implements a 'latest-N' policy.
    """
    while running:
        try:
            raw_data, addr = sock.recvfrom(512 * 1024)
            try:
                _udp_queue.put_nowait(raw_data)
            except queue.Full:
                # Queue is full — drop the oldest frame and insert the new one.
                # This ensures we always process the MOST RECENT data.
                try:
                    _udp_queue.get_nowait()
                except queue.Empty:
                    pass
                _udp_queue.put_nowait(raw_data)
        except socket.timeout:
            continue  # No data arrived — loop back
        except Exception:
            if not running:
                break


def handle_sigterm(signum, frame):
    """Catches Kubernetes SIGTERM for graceful shutdown."""
    global running
    print("\n📴 WORKER: SIGTERM received — shutting down cleanly.", flush=True)
    running = False
    sys.exit(0)


signal.signal(signal.SIGTERM, handle_sigterm)


def main():
    # --- Boot Sequence ---

    # Step 1: Recover previous state from the Guardian (handles warm boot after migration)
    load_initial_state()

    # Step 2: Start the pre-freeze flush server in the background
    start_flush_server()

    # Step 3: Start the background Guardian state sync thread
    sync_thread = threading.Thread(target=sync_state_to_guardian, daemon=True)
    sync_thread.start()

    # Step 4: Open the UDP socket for incoming 64×64 observation frames
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # Large receive buffer to handle the compressed frames (~30-60 KB each)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 262144)
    sock.settimeout(1.0)   # Non-blocking with 1-second timeout for flush checks
    sock.bind(("0.0.0.0", UDP_PORT))
    print(f"📡 WORKER: Listening for wildfire frames on UDP port {UDP_PORT}", flush=True)

    # Step 5 (Issue #5): Start background UDP ingestion thread
    # This thread drains the OS UDP buffer continuously, preventing kernel drops
    # when SAMKNN inference takes longer than the stream interval.
    ingestion = threading.Thread(target=_udp_ingestion_thread, args=(sock,), daemon=True)
    ingestion.start()
    print("📡 WORKER: Async UDP ingestion thread started.", flush=True)

    # --- Main Processing Loop ---
    # Pulls pre-received frames from the queue instead of blocking on recvfrom()
    while running:
        # If the Guardian has triggered a pre-freeze flush, pause and wait
        if flush_requested.is_set():
            print("⏸️  WORKER: Pausing ingestion — flush in progress...", flush=True)
            flush_done.wait(timeout=10)  # Wait up to 10 seconds for flush to complete
            flush_requested.clear()
            flush_done.clear()
            continue

        try:
            # Pull the next frame from the async ingestion queue
            # Timeout of 1s ensures we check flush_requested regularly
            raw_data = _udp_queue.get(timeout=1.0)
        except queue.Empty:
            continue  # No data arrived — loop back to check flush flag

        # Decode the compressed binary frame into structured channel grids
        channels, fire_mask_label = decode_frame(raw_data)

        if channels is None:
            continue  # Skip malformed frames silently

        # Run the full SAMKNN pipeline on this frame
        process_frame(channels, fire_mask_label)


if __name__ == "__main__":
    print("🛰️  TINYSML WORKER V6: 2D Wildfire Tracker ONLINE.", flush=True)
    main()
