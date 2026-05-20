"""
SPACE CLOUD V7 - WSTS DATA STREAMER (Ground Station)
=====================================================

ROLE:
This script acts as the Ground Station Transmitter. It reads the raw WSTS HDF5
Test dataset (Year 2021), applies Earth-side preprocessing (Normalization, 
One-Hot Encoding, 120-Channel Early Fusion deduplication), and streams the 
flight-ready float32 tensor to the satellite worker via UDP.

ARCHITECTURAL CONSTRAINTS:
1. Earth-Side Preprocessing: To save orbital compute and RAM, all complex 
   math (Sine angle conversion, means/stds) is baked here. The satellite 
   receives pure, ready-to-compute ONNX tensors.
2. UDP (Fire-and-Forget): Ensures the satellite worker can seamlessly resume 
   after a CRIU migration without TCP reconnection logic.
3. zlib Deflate: Drastically reduces the 120-channel tensor size to save bandwidth.
"""

import socket
import struct
import zlib
import time
import os
import sys
import subprocess
import numpy as np
import torch

# =============================================================================
# 0. PATH INJECTION MAGIC (Link to Earth Training Facility)
# =============================================================================
# We dynamically resolve the absolute path to isolate 'Version 7' safely,
# handling spaces in directory names and preventing path truncation.
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__)) # .../Version 7/3_ground_station
VERSION_7_ROOT = os.path.dirname(CURRENT_DIR)            # .../Version 7

TRAINING_FACILITY_SRC = os.path.join(VERSION_7_ROOT, "1_earth_training_facility", "src")
sys.path.append(TRAINING_FACILITY_SRC)

try:
    from dataloader.FireSpreadDataset import FireSpreadDataset
    print("✅ STREAMER: Successfully linked with official FireSpreadDataset dataloader.")
except ImportError as e:
    print("❌ STREAMER ERROR: Could not import FireSpreadDataset.")
    print(f"Debug - Looked into injected path: {TRAINING_FACILITY_SRC}")
    print(f"Original Error: {e}")
    sys.exit(1)


# =============================================================================
# 1. CONFIGURATION
# =============================================================================

WSTS_EVAL_DIR = os.getenv("WSTS_EVAL_DIR", "evaluation_data")

# The NodePort the satellite worker is actively listening to
WORKER_UDP_NODEPORT = 32005

# How long to pause between frames (gives the orbital CPU time to process)
STREAM_INTERVAL = float(os.getenv("STREAM_INTERVAL", "1.0"))

# Chunk size tuned well below the 64KB UDP MTU to safely survive fragmentation
CHUNK_SIZE = 60_000

# Constants reflecting the new Data-Level Fusion architecture
GRID_H       = 64
GRID_W       = 64
N_CHANNELS   = 120  # The deduplicated 5-day history tensor

TEST_MODE = os.getenv("TEST_MODE", "true").lower() == "true"
TEST_SLICE_SIZE = int(os.getenv("TEST_SLICE_SIZE", "20"))


# =============================================================================
# 2. KUBERNETES ROUTING
# =============================================================================

def get_minikube_ip() -> str:
    try:
        return subprocess.check_output(
            ["minikube", "ip", "-p", "minikube"], stderr=subprocess.DEVNULL
        ).decode("utf-8").strip()
    except Exception:
        return "192.168.49.2"

def resolve_target_ip() -> str:
    print("🔍 STREAMER: Resolving Minikube cluster IP...")
    ip = get_minikube_ip()
    print(f"✅ STREAMER: Cluster IP located at {ip}")
    return ip


# =============================================================================
# 3. ENCODING & CHUNKED UDP TRANSMISSION
# =============================================================================

def encode_frame(frame: np.ndarray) -> bytes:
    """
    Serialises the (120, 64, 64) float32 numpy array into a zlib-compressed blob.
    We pack as big-endian ('>f4') to match the worker's native parse logic.
    """
    raw_bytes = frame.astype('>f4').tobytes()
    return zlib.compress(raw_bytes)

def send_frame(sock: socket.socket, blob: bytes, target_ip: str,
               target_port: int, frame_id: int) -> int:
    """
    Splits the compressed blob into datagrams with structured headers.
    Header: [frame_id (uint32), total_chunks (uint8), chunk_idx (uint8)].
    """
    total_chunks = (len(blob) + CHUNK_SIZE - 1) // CHUNK_SIZE

    for chunk_idx in range(total_chunks):
        chunk_data = blob[chunk_idx * CHUNK_SIZE : (chunk_idx + 1) * CHUNK_SIZE]
        header = struct.pack("!IBB", frame_id, total_chunks, chunk_idx)
        sock.sendto(header + chunk_data, (target_ip, target_port))

    return total_chunks


# =============================================================================
# 4. MAIN EXECUTION LOOP
# =============================================================================

def main():
    print("🚀 STREAMER: Booting WSTS Uplink (Early Fusion Mode)...", flush=True)

    if not os.path.exists(WSTS_EVAL_DIR):
        print(f"❌ STREAMER ERROR: Evaluation directory '{WSTS_EVAL_DIR}' not found.")
        print("Please copy the 2021 test data into evaluation_data/2021/ first.")
        sys.exit(1)

    # 1. Initialize the Official Dataset with strict Earth-side constraints
    print("🌍 STREAMER: Initializing Earth-side Geospatial Preprocessing...")
    dataset = FireSpreadDataset(
        data_dir=WSTS_EVAL_DIR,
        included_fire_years=[2021],       # Test Year ONLY (No Data Leakage)
        n_leading_observations=5,         # T=5 days history
        crop_side_length=64,
        load_from_hdf5=True,
        is_train=False,                   # Disable random geometric augmentations
        remove_duplicate_features=True,   # ENABLE 120-channel deduplication!
        stats_years=[2018, 2019]          # Apply identical normalization from training
    )

    dataset_size = len(dataset)
    print(f"📂 STREAMER: Loaded {dataset_size} evaluation frames from 2021.")

    target_ip   = resolve_target_ip()
    target_port = WORKER_UDP_NODEPORT
    print(f"🎯 STREAMER: Target locked → {target_ip}:{target_port}", flush=True)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    frame_id_counter = 0
    limit = min(TEST_SLICE_SIZE, dataset_size) if TEST_MODE else dataset_size

    while True:
        for idx in range(limit):
            # The dataset outputs a tuple: (input_tensor_120ch, target_mask)
            # We only send the input tensor to the satellite.
            x_tensor, _ = dataset[idx]
            
            # Convert PyTorch tensor to pure Numpy float32
            frame_array = x_tensor.numpy().astype(np.float32)

            frame_id_counter = (frame_id_counter + 1) & 0xFFFF_FFFF
            blob = encode_frame(frame_array)

            try:
                n_chunks = send_frame(sock, blob, target_ip, target_port, frame_id_counter)
                compressed_kb = len(blob) // 1024
                print(
                    f"📤 STREAMER: Frame #{frame_id_counter} [Index {idx}] "
                    f"→ {compressed_kb}KB, {n_chunks} chunk(s)",
                    flush=True
                )
            except Exception as e:
                print(f"⚠️ STREAMER: UDP send error on frame #{frame_id_counter}: {e}", flush=True)

            time.sleep(STREAM_INTERVAL)

        print("🔁 STREAMER: Test pass complete. Looping telemetry...", flush=True)

if __name__ == "__main__":
    main()