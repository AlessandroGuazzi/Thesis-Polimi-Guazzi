"""
===============================================================================
TEACHING EDITION — tinysml_worker.py
===============================================================================

This file is the highly optimized SCIENTIFIC PAYLOAD running on the satellite.

ARCHITECTURAL UPGRADES IN THIS VERSION (The Velocity Fixes):
1. Native NumPy Ring Buffers (Zero-Allocation Execution):
   Python lists and deques have been entirely eradicated from the ML pipeline.
   State is now stored in pre-allocated, contiguous C-memory blocks (NumPy arrays).
   This eliminates the O(N) Array Reallocation Penalty that was choking the CPU.

2. Vectorized Self-Validation (Concept Drift):
   The cleaning loop no longer runs a Python `for` loop to check memories.
   It now uses a single, massive Matrix Dot Product to validate 1,000 memories
   simultaneously, applying a fully vectorized Infinity Trick.

These two changes guarantee that the processing speed remains flat (O(1) allocation)
even when the 20,000-instance memory banks are completely full.
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
K_NEIGHBOURS = 15  # The algorithm checks the 15 closest historical pixels to vote on fire presence.
STD_FLOOR = 0.1    # Minimum standard deviation to prevent noise amplification
WELFORD_MAX_N = 50000  # Cap on Welford sample count to prevent statistics freezing
MAX_TRAIN_PER_FRAME = 500  # Maximum number of samples to learn from a single frame

# These 12 channels form the feature space (Temperature, Humidity, Wind, etc.)
FEATURE_CHANNELS = [
    "PrevFireMask", "sph", "th", "elevation", "pdsi", "pr", "population",
    "erc", "NDVI", "tmmn", "vs", "tmmx"
]

GRID_W = 64
GRID_H = 64
N_CHANNELS = len(FEATURE_CHANNELS)

# --- THE ACCURACY PARADOX FIX (Feature Scaling) ---
_CHANNEL_SCALES = [
    1.0,          # PrevFireMask
    50.0,         # sph
    0.004,        # th
    0.00025,      # elevation
    0.05,         # pdsi
    0.005,        # pr
    0.001,        # population
    0.00667,      # erc
    0.0000833,    # NDVI
    0.00909,      # tmmn
    0.0667,       # vs
    0.00833       # tmmx
]
SCALE_VECTOR = np.array(_CHANNEL_SCALES * 9, dtype=np.float32)


# =============================================================================
# GLOBAL STATE: THE NATIVE NUMPY RING BUFFERS (Zero-Allocation Memory)
# =============================================================================
# Instead of Python lists, we pre-allocate exactly ~8.6 MB of RAM for our matrices.
# This guarantees our memory footprint is deterministic and safely under 25 MB.

# Short-Term Memory (STM)
stm_X    = np.zeros((STM_MAX, 13), dtype=np.float32)
stm_y    = np.zeros(STM_MAX, dtype=np.int8)
stm_ptr  = 0
stm_count = 0

# Long-Term Memory (LTM)
ltm_X    = np.zeros((LTM_MAX, 13), dtype=np.float32)
ltm_y    = np.zeros(LTM_MAX, dtype=np.int8)
ltm_ptr  = 0
ltm_count = 0

# Prediction results used by Node Agent (Trigger B - Lateral Tracking)
predicted_fire_mask = [0] * (GRID_W) * (GRID_H)
center_of_mass = {"x": 0.0, "y": 0.0}

# Metrics for the dashboard
sample_count = 0
fire_pixel_count = 0
instances_trained = 0
current_prev_mask = None
current_gt_inner = None

# ── EVALUATION BUFFERS (Zero-Allocation) ──────────────────────
EVAL_ROLLING_N     = 50                                       # Rolling window size

# Rolling F1 ring buffer (pre-allocated, never resized)
f1_ring            = np.zeros(EVAL_ROLLING_N, dtype=np.float32)
f1_ring_ptr        = 0
f1_ring_count      = 0

# Migration delta tracking
iou_accum_pre      = np.float64(0.0)   # Sum of IoU values before migration
iou_count_pre      = np.int32(0)
iou_accum_post     = np.float64(0.0)
iou_count_post     = np.int32(0)
migration_epoch    = 0                 # Incremented on each restore
last_migration_sample = 0             # sample_count at last restore

# Latest scalars (updated every frame, read by build_state_payload)
current_iou        = np.float32(0.0)
current_f1         = np.float32(0.0)
rolling_f1_mean    = np.float32(0.0)
delta_mig          = np.float32(0.0)

# Welford's online algorithm state for Z-Score Normalization
welford_count = 0
welford_mean  = np.zeros(13, dtype=np.float64)
welford_M2    = np.zeros(13, dtype=np.float64)

# Synchronization flags used during the pre-freeze flush handshake with the Guardian
flush_requested = threading.Event()
flush_done = threading.Event()
# FIX: Permanent one-way kill switch for the background sync thread.
# Set by FlushHandler after a successful heavy POST — never cleared within a process
# lifetime. Because the worker cold-boots as a fresh process on the destination node,
# a new Event() is created automatically, so the sync thread runs normally after landing.
sync_thread_killed = threading.Event()
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

    # Calibrated strict consensus threshold: >= 8 of 15 neighbors, scaled proportionally for smaller memories
    FIRE_VOTE_THRESHOLD = max(1, k_actual * 8 // 15)
    return np.where(np.sum(votes, axis=1) >= FIRE_VOTE_THRESHOLD, 1, 0)


# -----------------------------------------------------------------------------
# Function: fast_samknn_predict_batch
# Purpose: Orchestrates the predictions from both Short and Long Term Memory.
# -----------------------------------------------------------------------------
def fast_samknn_predict_batch(Q, stm_X_v, stm_y_v, stm_X_sq_v, ltm_X_v, ltm_y_v, ltm_X_sq_v):
    """
    Combine STM and LTM predictions.

    STM has precedence because it models recent concept drift.
    """
    # Ask both memory banks for their predictions
    stm_pred = fast_knn_predict_batch(Q, stm_X_v, stm_y_v, stm_X_sq_v)
    ltm_pred = fast_knn_predict_batch(Q, ltm_X_v, ltm_y_v, ltm_X_sq_v)

    M = Q.shape[0]
    final_pred = np.zeros(M, dtype=int)

    stm_valid = (stm_pred != -1)
    ltm_valid = (ltm_pred != -1)

    # STM Priority: Use STM if it has a valid prediction. Otherwise use LTM.
    final_pred[stm_valid] = stm_pred[stm_valid]

    ltm_only = ~stm_valid & ltm_valid
    final_pred[ltm_only] = ltm_pred[ltm_only]

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
    global stm_ptr, stm_count, instances_trained

    # Overwrite the ring buffer slot
    stm_X[stm_ptr] = feature_vector
    stm_y[stm_ptr] = label

    # Advance pointer circularly
    stm_ptr = (stm_ptr + 1) % STM_MAX
    stm_count = min(stm_count + 1, STM_MAX)
    instances_trained += 1

    # Every 3000 training instances, trigger the garbage collector / validation loop
    if instances_trained % 3000 == 0:
        _stm_to_ltm_cleaning()

def _batch_samknn_train(X_batch, y_batch):
    global stm_ptr, stm_count, instances_trained
    n = len(y_batch)
    if n == 0: return

    # Fully vectorized ring-buffer insertion (O(1) memory, no Python loop)
    indices = (stm_ptr + np.arange(n)) % STM_MAX

    stm_X[indices] = X_batch
    stm_y[indices] = y_batch

    stm_ptr = (stm_ptr + n) % STM_MAX
    stm_count = min(stm_count + n, STM_MAX)

    prev_trained = instances_trained
    instances_trained += n
    if (prev_trained // 3000) != (instances_trained // 3000):
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
    global ltm_ptr, ltm_count

    if stm_count == 0:
        return

    # Normalization parameters
    variance = welford_M2 / max(1, welford_count)
    std = np.sqrt(variance).astype(np.float32)
    std = np.maximum(std, STD_FLOOR)
    w_mean = welford_mean.astype(np.float32)

    # We validate the 1,000 most recently added items.
    N = min(1000, stm_count)
    k_actual = min(K_NEIGHBOURS, stm_count - 1)
    if k_actual == 0: return

    # 1. Identify where the newest N items sit in the ring buffer
    if stm_ptr >= N:
        indices = np.arange(stm_ptr - N, stm_ptr)
    else:
        # It wrapped around the buffer
        indices = np.concatenate((np.arange(STM_MAX - (N - stm_ptr), STM_MAX), np.arange(0, stm_ptr)))

    # 2. Extract Queries (Q) and Total Memory (X), and Normalize
    Q_raw = stm_X[indices]
    Q = (Q_raw - w_mean) / std
    Q_sq = np.sum(Q ** 2, axis=1, keepdims=True)

    X_raw = stm_X[:stm_count]
    X = (X_raw - w_mean) / std
    X_sq = np.sum(X ** 2, axis=1)

    # 3. Vectorized Math (1,000 queries x up to 5,000 memories instantly)
    Xq = np.dot(Q, X.T)
    distances = X_sq - (2 * Xq) + Q_sq

    # 4. THE VECTORIZED INFINITY TRICK
    # distances is an (N x stm_count) matrix.
    # We set the exact coordinate where a point intersects with ITSELF to infinity.
    valid_mask = indices < stm_count
    distances[np.arange(N)[valid_mask], indices[valid_mask]] = np.inf

    # 5. Fast k-NN Voting (STM)
    nearest_idx = np.argpartition(distances, k_actual - 1, axis=1)[:, :k_actual]
    votes = stm_y[:stm_count][nearest_idx]
    FIRE_VOTE_THRESHOLD_STM = max(1, k_actual * 8 // 15)
    stm_preds = np.where(np.sum(votes, axis=1) >= FIRE_VOTE_THRESHOLD_STM, 1, 0)

    actual_labels = stm_y[indices]
    stm_consistent = (stm_preds == actual_labels)

    # 5b. Cross-validation against LTM
    k_ltm = min(K_NEIGHBOURS, ltm_count)
    if k_ltm > 0:
        ltm_X_raw = ltm_X[:ltm_count]
        ltm_X_norm = (ltm_X_raw - w_mean) / std
        ltm_X_sq = np.sum(ltm_X_norm ** 2, axis=1)
        
        Q_sq_ltm = np.sum(Q ** 2, axis=1, keepdims=True)
        Xq_ltm = np.dot(Q, ltm_X_norm.T)
        distances_ltm = ltm_X_sq - (2 * Xq_ltm) + Q_sq_ltm
        
        nearest_idx_ltm = np.argpartition(distances_ltm, k_ltm - 1, axis=1)[:, :k_ltm]
        votes_ltm = ltm_y[:ltm_count][nearest_idx_ltm]
        FIRE_VOTE_THRESHOLD_LTM = max(1, k_ltm * 8 // 15)
        ltm_preds = np.where(np.sum(votes_ltm, axis=1) >= FIRE_VOTE_THRESHOLD_LTM, 1, 0)
        
        ltm_consistent = (ltm_preds == actual_labels)
        consistent_mask = stm_consistent & ltm_consistent
    else:
        consistent_mask = stm_consistent

    # 6. Promotion
    consistent_indices = indices[consistent_mask]

    # FIX 4: Class-Balanced LTM Promotion
    if ltm_count > 1000:
        ltm_fire_ratio = np.sum(ltm_y[:ltm_count] == 1) / max(1, ltm_count)
        if ltm_fire_ratio < 0.35:
            fire_mask = stm_y[consistent_indices] == 1
            if fire_mask.any():
                consistent_indices = consistent_indices[fire_mask]

    # Fully vectorized batch insertion into LTM ring buffer
    n_consistent = len(consistent_indices)
    if n_consistent > 0:
        ltm_indices = (ltm_ptr + np.arange(n_consistent)) % LTM_MAX
        ltm_X[ltm_indices] = stm_X[consistent_indices]
        ltm_y[ltm_indices] = stm_y[consistent_indices]

        ltm_ptr = (ltm_ptr + n_consistent) % LTM_MAX
        ltm_count = min(ltm_count + n_consistent, LTM_MAX)

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
        decompressed = zlib.decompress(raw_udp_bytes)
        # Direct buffer → C-contiguous float32 array (ZERO Python objects)
        all_values = np.frombuffer(decompressed, dtype='>f4')  # big-endian float32

        # Reshape into (N_CHANNELS+1, H, W) — one operation, zero copies
        grids = all_values.reshape(N_CHANNELS + 1, GRID_H, GRID_W)
        channels = grids[:N_CHANNELS]   # shape (12, 64, 64) — a view, not a copy
        fire_mask = grids[N_CHANNELS]   # shape (64, 64) — a view

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
def compute_center_of_mass(pred_flat, inner_h, inner_w):
    """
    Computes the center of mass of predicted fire pixels.
    Used by Node Agent Trigger B.
    """
    pred_2d = pred_flat.reshape(inner_h, inner_w)
    fire_coords = np.argwhere(pred_2d == 1)
    if len(fire_coords) == 0:
        return {"x": inner_w / 2.0, "y": inner_h / 2.0}
    return {"x": float(fire_coords[:, 1].mean()), "y": float(fire_coords[:, 0].mean())}


# -----------------------------------------------------------------------------
# Function: evaluate_frame
# Purpose: Compares prediction(T) against ground_truth(T) from the SAME
#          spatially-independent wildfire event to compute IoU, F1, and Migration Delta.
# -----------------------------------------------------------------------------
def evaluate_frame(gt_flat, pred_flat):
    """
    Intra-Frame Evaluation: compare prediction(T) against ground_truth(T).
    All ops are bitwise NumPy — zero Python-level allocation.
    """
    global current_iou, current_f1, rolling_f1_mean
    global f1_ring_ptr, f1_ring_count
    global iou_accum_pre, iou_count_pre, iou_accum_post, iou_count_post, delta_mig

    # ── IoU (Intersection over Union) ────────────────────────
    # Uses bitwise AND/OR on int8 arrays — runs in C, zero temp allocs
    intersection = np.count_nonzero(pred_flat & gt_flat)
    union        = np.count_nonzero(pred_flat | gt_flat)
    current_iou  = np.float32(intersection / union) if union > 0 else np.float32(1.0)

    # ── F1-Score (Condition-Activated) ───────────────────────
    if union > 0:
        tp = intersection                                    # pred=1 AND gt=1
        fp = np.count_nonzero(pred_flat & ~gt_flat)          # pred=1 AND gt=0
        fn = np.count_nonzero(~pred_flat & gt_flat)          # pred=0 AND gt=1
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall    = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        current_f1 = np.float32(
            2 * precision * recall / (precision + recall)
            if (precision + recall) > 0 else 0.0
        )

        # ── Rolling F1 Ring Buffer Update ─────────────────────
        f1_ring[f1_ring_ptr] = current_f1
        f1_ring_ptr   = (f1_ring_ptr + 1) % EVAL_ROLLING_N
        f1_ring_count = min(f1_ring_count + 1, EVAL_ROLLING_N)
        rolling_f1_mean = np.float32(np.mean(f1_ring[:f1_ring_count]))
    else:
        # Active Threat = 0 AND False Alarm = 0. The engine is Idle.
        # Set current_f1 to None (JSON null) to signal idle to Dashboard.
        # Ring buffer is frozen to prevent metric inflation/spikes.
        current_f1 = None

    # ── Migration Delta (ΔMig) ───────────────────────────────
    if sample_count <= last_migration_sample:
        iou_accum_pre += current_iou
        iou_count_pre += 1
    else:
        iou_accum_post += current_iou
        iou_count_post += 1

    avg_pre  = (iou_accum_pre / iou_count_pre)   if iou_count_pre  > 0 else 0.0
    avg_post = (iou_accum_post / iou_count_post) if iou_count_post > 0 else 0.0
    delta_mig = np.float32(avg_post - avg_pre)


# =============================================================================
# SECTION 4: FRAME PROCESSING PIPELINE (Zero-Allocation Execution)
# =============================================================================

# -----------------------------------------------------------------------------
# Function: process_frame
# Purpose: Translates the 2D grid into 1D features, chunks the RAM, and predicts.
# -----------------------------------------------------------------------------
def process_frame(channels, fire_mask_label):
    """
    Core pipeline executed for every received frame.
    """
    global predicted_fire_mask, center_of_mass, fire_pixel_count, sample_count, current_prev_mask, current_gt_inner
    global welford_count, welford_mean, welford_M2

    inner_h, inner_w = GRID_H, GRID_W

    # --- THE ALLOCATION FIX ---
    # We no longer force Python to build matrices from lists.
    # We simply take "views" (slices) of our pre-existing C-memory.
    # This takes 0.0000ms. Order doesn't matter for distance checks.
    valid_stm_X = stm_X[:stm_count]
    valid_stm_y = stm_y[:stm_count]

    valid_ltm_X = ltm_X[:ltm_count]
    valid_ltm_y = ltm_y[:ltm_count]

    # === THE 3x3 SLIDING WINDOW (Spatial Extraction) ===
    # Extract Center Pixel for all 12 channels without slicing off the borders
    center_pixels = channels.transpose(1, 2, 0).reshape(-1, 12)

    # Extract Neighborhood Aggregation using zero-padding on PrevFireMask
    padded_mask = np.pad(channels[0], pad_width=1, mode='constant', constant_values=0)
    neighbor_sum = np.zeros((GRID_H, GRID_W), dtype=np.float32)
    for dr in [0, 1, 2]:
        for dc in [0, 1, 2]:
            neighbor_sum += padded_mask[dr:GRID_H+dr, dc:GRID_W+dc]

    neighbor_sum_flat = neighbor_sum.reshape(-1, 1)

    Q_array = np.concatenate([center_pixels, neighbor_sum_flat], axis=1).astype(np.float32)

    # Batch Welford Update for Z-Score Normalization
    m = Q_array.shape[0]
    batch_mean = np.mean(Q_array, axis=0, dtype=np.float64)
    batch_M2 = np.sum((Q_array - batch_mean) ** 2, axis=0, dtype=np.float64)
    
    n = welford_count
    n_new = n + m
    
    if n == 0:
        welford_mean = batch_mean
        welford_M2 = batch_M2
        welford_count = n_new
    else:
        # FIX 3: Capped Effective Window
        if n_new > WELFORD_MAX_N:
            scale = WELFORD_MAX_N / n_new
            welford_M2 = welford_M2 * scale
            n_new = WELFORD_MAX_N

        delta = batch_mean - welford_mean
        welford_mean = welford_mean + delta * (m / n_new)
        welford_M2 = welford_M2 + batch_M2 + (delta ** 2) * (n * m / n_new)
        welford_count = n_new

    # Z-score normalization
    variance = welford_M2 / welford_count
    std = np.sqrt(variance).astype(np.float32)
    # FIX 1: Robust Standard Deviation Floor
    std = np.maximum(std, STD_FLOOR)
    Q_array_norm = (Q_array - welford_mean.astype(np.float32)) / std

    # Normalize memory banks with current parameters
    if stm_count > 0:
        valid_stm_X_norm = (valid_stm_X - welford_mean.astype(np.float32)) / std
        valid_stm_X_sq_norm = np.sum(valid_stm_X_norm ** 2, axis=1)
    else:
        valid_stm_X_norm = np.empty((0, 13), dtype=np.float32)
        valid_stm_X_sq_norm = np.empty(0, dtype=np.float32)

    if ltm_count > 0:
        valid_ltm_X_norm = (valid_ltm_X - welford_mean.astype(np.float32)) / std
        valid_ltm_X_sq_norm = np.sum(valid_ltm_X_norm ** 2, axis=1)
    else:
        valid_ltm_X_norm = np.empty((0, 13), dtype=np.float32)
        valid_ltm_X_sq_norm = np.empty(0, dtype=np.float32)

    # === CHUNK BATCHING (The RAM Saver) ===
    CHUNK_SIZE = 256
    M = Q_array_norm.shape[0]
    preds = np.zeros(M, dtype=int)

    for i in range(0, M, CHUNK_SIZE):
        end_idx = min(i + CHUNK_SIZE, M)
        preds[i:end_idx] = fast_samknn_predict_batch(
            Q_array_norm[i:end_idx],
            valid_stm_X_norm, valid_stm_y, valid_stm_X_sq_norm,
            valid_ltm_X_norm, valid_ltm_y, valid_ltm_X_sq_norm
        )
    # Ensure unknown classifications (-1) from empty memory map to 0
    preds[preds == -1] = 0

    # ── INTRA-FRAME EVALUATION (before tolist destroys the numpy view) ──
    gt_inner = fire_mask_label.ravel().astype(np.int8)
    evaluate_frame(gt_inner, preds.astype(np.int8))

    # Selectively train new examples (Balanced sampling)
    fire_mask_bool = (gt_inner == 1)
    n_fire = fire_mask_bool.sum()

    if n_fire > 0:
        fire_indices = np.where(fire_mask_bool)[0]
        # Cap fire samples to half the max training budget
        if len(fire_indices) > MAX_TRAIN_PER_FRAME // 2:
            fire_indices = np.random.choice(fire_indices, size=MAX_TRAIN_PER_FRAME // 2, replace=False)

        nofire_indices = np.where(~fire_mask_bool)[0]
        n_sample_nofire = min(len(fire_indices), len(nofire_indices))
        if n_sample_nofire > 0:
            sampled_nofire = np.random.choice(nofire_indices, size=n_sample_nofire, replace=False)
            sample_mask = np.concatenate([fire_indices, sampled_nofire])
        else:
            sample_mask = fire_indices
    else:
        # Idle background sampling decimated
        sample_mask = np.where(np.random.random(len(gt_inner)) < 0.002)[0]

    # Insert raw features
    sampled_X = Q_array[sample_mask]
    sampled_y = gt_inner[sample_mask]

    # Batch insert into STM
    _batch_samknn_train(sampled_X, sampled_y)

    # Update global state variables for the dashboard and the Node Agent
    predicted_fire_mask = preds.tolist()
    current_prev_mask = channels[0].ravel().astype(np.int8).tolist()
    current_gt_inner = gt_inner.tolist()
    fire_pixel_count = sum(predicted_fire_mask)
    center_of_mass = compute_center_of_mass(preds, inner_h, inner_w)
    sample_count += 1


# =============================================================================
# SECTION 5: GUARDIAN STATE SYNC
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
        "prev_fire_mask": current_prev_mask,
        "gt_fire_mask": current_gt_inner,
        "center_of_mass": center_of_mass,
        "fire_pixel_count": fire_pixel_count,
        "sample_count": sample_count,
        "instances_trained": instances_trained,
        "stm_size": int(stm_count),
        "ltm_size": int(ltm_count),
        "status": "TRACKING",
        "eval_metrics": {
            "iou":             float(current_iou),
            "f1":              float(current_f1) if current_f1 is not None else None,
            "rolling_f1":      float(rolling_f1_mean),
            "delta_mig":       float(delta_mig),
            "migration_epoch": int(migration_epoch),
            "sample_at_migration": int(last_migration_sample)
        },
        # Explicitly declare this is a lightweight telemetry sync, NOT the
        # massive pre-freeze memory flush. The FlushHandler will override
        # this to True before appending the massive STM/LTM matrices.
        "is_full_flush": False
    }


# -----------------------------------------------------------------------------
# Function: sync_state_to_guardian
# Purpose: Runs in a background thread, quietly updating the sidecar every 2 seconds.
# -----------------------------------------------------------------------------
def sync_state_to_guardian():
    """
    Background thread continuously pushing lightweight telemetry to Guardian.

    Permanently terminates (returns) after a successful pre-freeze flush so that
    no lightweight POST can race against the heavy STM/LTM payload in Guardian RAM.
    """
    _pending_state = None

    while running:
        # FIX: Permanent kill switch — once the flush handler confirms a successful
        # heavy POST, this thread exits entirely. It will never touch the Guardian
        # again in this process lifetime, eliminating the post-flush TOCTOU window.
        if sync_thread_killed.is_set():
            print("🛑 WORKER: Sync thread permanently terminated after flush.", flush=True)
            return

        # --- THE CHANNEL POLLUTION FIX ---
        # Silence periodic syncs while the emergency serialization is running.
        if flush_requested.is_set():
            time.sleep(0.5)
            continue

        try:
            payload = _pending_state if _pending_state else build_state_payload()
            resp = requests.post(f"{GUARDIAN_URL}/state", json=payload, timeout=1)

            if resp.status_code == 503:
                if _pending_state is None:
                    _pending_state = payload
            elif resp.status_code == 409:
                # Guardian is flush-locked — the heavy payload is already stored.
                # Stop immediately; any further POST risks being a destructive overwrite.
                print("🔒 WORKER: Guardian flush-locked. Halting sync thread permanently.", flush=True)
                return
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
    On cold boot, restore last state from Guardian or Baseline.
    Follows a strict Decision Tree to prioritize network migrations.
    """
    global predicted_fire_mask, center_of_mass, fire_pixel_count, sample_count, instances_trained
    global stm_ptr, stm_count, ltm_ptr, ltm_count
    global f1_ring_ptr, f1_ring_count, migration_epoch, last_migration_sample
    global iou_accum_pre, iou_count_pre, iou_accum_post, iou_count_post
    global welford_count, welford_mean, welford_M2

    while True:
        try:
            resp = requests.get(f"{GUARDIAN_URL}/state", timeout=2)
            if resp.status_code == 200:
                data = resp.json()
                
                # CASE A: Post-Migration Recovery
                if "sample_count" in data and data["sample_count"] > 0:
                    print(f"📡 WORKER: Network payload detected (sample_count={data['sample_count']}). Restoring from Guardian RAM.", flush=True)
                    predicted_fire_mask = data.get("predicted_fire_mask", predicted_fire_mask)
                    center_of_mass = data.get("center_of_mass", center_of_mass)
                    fire_pixel_count = data.get("fire_pixel_count", 0)
                    sample_count = data.get("sample_count", 0)
                    instances_trained = data.get("instances_trained", 0)

                    # --- THE RESTORE FIX ---
                    if "stm_X" in data and "stm_y" in data:
                        stm_ptr = stm_count = 0
                        for f, l in zip(data["stm_X"], data["stm_y"]):
                            arr = np.array(f, dtype=np.float32)
                            if len(arr) == 108:
                                arr = np.concatenate([arr[48:60], [np.sum(arr[0::12])]])
                            samknn_train(arr, l)
                        print(f"✅ WORKER: Restored {stm_count} STM memories from Network.", flush=True)

                    if "ltm_X" in data and "ltm_y" in data:
                        ltm_ptr = ltm_count = 0
                        for f, l in zip(data["ltm_X"], data["ltm_y"]):
                            arr = np.array(f, dtype=np.float32)
                            if len(arr) == 108:
                                arr = np.concatenate([arr[48:60], [np.sum(arr[0::12])]])
                            ltm_X[ltm_ptr] = arr
                            ltm_y[ltm_ptr] = l
                            ltm_ptr = (ltm_ptr + 1) % LTM_MAX
                            ltm_count = min(ltm_count + 1, LTM_MAX)
                        print(f"✅ WORKER: Restored {ltm_count} LTM memories from Network.", flush=True)

                    if "eval_state" in data:
                        ev = data["eval_state"]
                        restored_f1 = np.array(ev.get("f1_ring", []), dtype=np.float32)
                        n_f1 = min(len(restored_f1), EVAL_ROLLING_N)
                        if n_f1 > 0:
                            f1_ring[:n_f1] = restored_f1[:n_f1]
                        f1_ring_ptr   = ev.get("f1_ring_ptr", 0)
                        f1_ring_count = ev.get("f1_ring_count", 0)

                        migration_epoch       = ev.get("migration_epoch", 0) + 1
                        last_migration_sample = sample_count

                        iou_accum_pre  = np.float64(ev.get("iou_accum_post", 0.0))
                        iou_count_pre  = np.int32(ev.get("iou_count_post", 0))
                        iou_accum_post = np.float64(0.0)
                        iou_count_post = np.int32(0)
                        print(f"✅ WORKER: Restored eval state (epoch {migration_epoch}) from Network.", flush=True)

                    if "welford_count" in data:
                        welford_count = data["welford_count"]
                        w_mean = np.array(data["welford_mean"], dtype=np.float64)
                        w_M2 = np.array(data["welford_M2"], dtype=np.float64)
                        if len(w_mean) == 108:
                            welford_mean = np.concatenate([w_mean[48:60], [np.sum(w_mean[0::12])]])
                            welford_M2 = np.concatenate([w_M2[48:60], [np.sum(w_M2[0::12])]])
                        else:
                            welford_mean = w_mean
                            welford_M2 = w_M2
                        print(f"✅ WORKER: Restored Z-score normalization state from Network.", flush=True)

                    return
                
                # CASE B & C: No valid network payload
                else:
                    # Check for Pre-Baked Artifact
                    if os.path.exists('/app/baseline.npz'):
                        print("💾 WORKER: Blank slate detected on Guardian. Found pre-baked baseline.npz. Initiating Warm Boot.", flush=True)
                        try:
                            with np.load('/app/baseline.npz') as npz_data:
                                sc = int(npz_data['stm_count'])
                                lc = int(npz_data['ltm_count'])
                                
                                if sc > 0:
                                    loaded_stm_X = npz_data['stm_X']
                                    if loaded_stm_X.shape[1] == 108:
                                        new_stm_X = np.zeros((loaded_stm_X.shape[0], 13), dtype=np.float32)
                                        new_stm_X[:, :12] = loaded_stm_X[:, 48:60]
                                        new_stm_X[:, 12] = np.sum(loaded_stm_X[:, 0::12], axis=1)
                                        loaded_stm_X = new_stm_X
                                    stm_X[:sc] = loaded_stm_X
                                    stm_y[:sc] = npz_data['stm_y']
                                    stm_count = sc
                                    stm_ptr = sc % STM_MAX
                                
                                if lc > 0:
                                    loaded_ltm_X = npz_data['ltm_X']
                                    if loaded_ltm_X.shape[1] == 108:
                                        new_ltm_X = np.zeros((loaded_ltm_X.shape[0], 13), dtype=np.float32)
                                        new_ltm_X[:, :12] = loaded_ltm_X[:, 48:60]
                                        new_ltm_X[:, 12] = np.sum(loaded_ltm_X[:, 0::12], axis=1)
                                        loaded_ltm_X = new_ltm_X
                                    ltm_X[:lc] = loaded_ltm_X
                                    ltm_y[:lc] = npz_data['ltm_y']
                                    ltm_count = lc
                                    ltm_ptr = lc % LTM_MAX
                                
                                sample_count = int(npz_data['sample_count'])
                                instances_trained = int(npz_data['instances_trained'])
                                
                                if 'f1_ring_count' in npz_data:
                                    f1c = int(npz_data['f1_ring_count'])
                                    if f1c > 0:
                                        f1_ring[:f1c] = npz_data['f1_ring']
                                    f1_ring_count = f1c
                                    f1_ring_ptr = int(npz_data['f1_ring_ptr'])
                                    migration_epoch = int(npz_data['migration_epoch'])
                                    iou_accum_pre = float(npz_data['iou_accum_pre'])
                                    iou_count_pre = int(npz_data['iou_count_pre'])
                                    iou_accum_post = float(npz_data['iou_accum_post'])
                                    iou_count_post = int(npz_data['iou_count_post'])

                                if 'welford_count' in npz_data:
                                    welford_count = int(npz_data['welford_count'])
                                    w_mean = npz_data['welford_mean']
                                    w_M2 = npz_data['welford_M2']
                                    if len(w_mean) == 108:
                                        welford_mean = np.concatenate([w_mean[48:60], [np.sum(w_mean[0::12])]])
                                        welford_M2 = np.concatenate([w_M2[48:60], [np.sum(w_M2[0::12])]])
                                    else:
                                        welford_mean = w_mean
                                        welford_M2 = w_M2

                            print(f"✅ WORKER: Warm Boot successful. Loaded {stm_count} STM and {ltm_count} LTM vectors.", flush=True)
                            
                            # Publish state to Guardian to initialize it
                            _pending_state = build_state_payload()
                            requests.post(f"{GUARDIAN_URL}/state", json=_pending_state, timeout=1)
                            
                            return
                        except Exception as e:
                            print(f"⚠️ WORKER: Failed to load baseline.npz: {e}. Falling back to zero start.", flush=True)
                    
                    # CASE C: Fresh Cold-Start WITHOUT Baseline
                    print("🌱 WORKER: No baseline found. Initializing naturally from zero.", flush=True)
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
# =============================================================================
# SECTION 6: PRE-FREEZE FLUSH SERVER (The TOCTOU Race Condition Fix)
# =============================================================================

class FlushHandler(BaseHTTPRequestHandler):
    """
    HTTP endpoint called by Guardian before CRIU or by Developer for extraction.
    """
    def do_POST(self):
        if self.path == "/extract_baseline":
            try:
                print("💾 WORKER: Baseline extraction requested. Serializing to disk...", flush=True)
                np.savez_compressed(
                    '/app/baseline.npz',
                    stm_X=stm_X[:stm_count],
                    stm_y=stm_y[:stm_count],
                    ltm_X=ltm_X[:ltm_count],
                    ltm_y=ltm_y[:ltm_count],
                    stm_count=np.array(stm_count),
                    ltm_count=np.array(ltm_count),
                    sample_count=np.array(sample_count),
                    instances_trained=np.array(instances_trained),
                    f1_ring=f1_ring[:f1_ring_count],
                    f1_ring_ptr=np.array(f1_ring_ptr),
                    f1_ring_count=np.array(f1_ring_count),
                    migration_epoch=np.array(migration_epoch),
                    iou_accum_pre=np.array(iou_accum_pre),
                    iou_count_pre=np.array(iou_count_pre),
                    iou_accum_post=np.array(iou_accum_post),
                    iou_count_post=np.array(iou_count_post),
                    welford_count=np.array(welford_count),
                    welford_mean=welford_mean,
                    welford_M2=welford_M2
                )
                print("✅ WORKER: Baseline successfully saved to /app/baseline.npz", flush=True)
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{"status": "BASELINE_EXTRACTED"}')
            except Exception as e:
                print(f"⚠️ WORKER: Baseline extraction failed: {e}", flush=True)
                self.send_response(500)
                self.end_headers()
                self.wfile.write(b'{"error": "EXTRACTION_FAILED"}')
            return

        if self.path == "/flush":
            flush_requested.set()

            # FIX: TOCTOU drain window.
            time.sleep(0.3)

            full_state = build_state_payload()
            full_state["is_full_flush"] = True

            # --- THE C-OPTIMIZED SERIALIZATION FIX ---
            full_state["stm_X"] = stm_X[:stm_count].tolist() if stm_count > 0 else []
            full_state["stm_y"] = stm_y[:stm_count].tolist() if stm_count > 0 else []
            full_state["ltm_X"] = ltm_X[:ltm_count].tolist() if ltm_count > 0 else []
            full_state["ltm_y"] = ltm_y[:ltm_count].tolist() if ltm_count > 0 else []

            full_state["eval_state"] = {
                "f1_ring":          f1_ring[:f1_ring_count].tolist(),
                "f1_ring_ptr":      int(f1_ring_ptr),
                "f1_ring_count":    int(f1_ring_count),
                "migration_epoch":  int(migration_epoch),
                "iou_accum_pre":    float(iou_accum_pre),
                "iou_count_pre":    int(iou_count_pre),
                "iou_accum_post":   float(iou_accum_post),
                "iou_count_post":   int(iou_count_post)
            }

            full_state["welford_count"] = int(welford_count)
            full_state["welford_mean"] = welford_mean.tolist()
            full_state["welford_M2"] = welford_M2.tolist()

            try:
                print("🚨 WORKER: Beginning massive POST /state to Guardian...", flush=True)
                # Timeout raised to 25s to match Agent's FLUSH_TIMEOUT_SECONDS.
                resp = requests.post(f"{GUARDIAN_URL}/state", json=full_state, timeout=25)
                print("✅ WORKER: Massive POST /state completed.", flush=True)
                if resp.status_code == 200:
                    flush_done.set()
                    sync_thread_killed.set()
                    print("🛑 WORKER: Sync thread kill switch engaged.", flush=True)
                else:
                    print(f"⚠️ WORKER: Guardian rejected flush payload: HTTP {resp.status_code}", flush=True)
            except Exception as e:
                print(f"⚠️ WORKER: Failed to POST full state to Guardian: {e}", flush=True)

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
            raw_data, _ = sock.recvfrom(65536)
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
            # FIX (Change 1.1): Do NOT clear the Events after the flush completes.
            # Keeping flush_requested set permanently blocks the periodic sync thread
            # (sync_state_to_guardian) from overwriting the flushed STM/LTM matrices
            # in the Guardian's RAM before CRIU freezes this process.
            # On the destination node the worker cold-boots with fresh (unset) Events.
            flush_done.wait(timeout=30)
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