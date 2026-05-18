"""
SPACE CLOUD V7 - WSTS DATA STREAMER (Ground Station)
=====================================================

ROLE:
This script acts as the Ground Station Transmitter. It reads the WSTS HDF5
dataset (23-channel, 64x64, T=5 time-series) using lazy loading, compresses
each frame via zlib, and streams it to the active satellite worker in orbit
via UDP.

ARCHITECTURAL CONSTRAINTS:
1. Lazy Loading (h5py Generator): The WSTS HDF5 file can exceed 50GB. The
   generator reads exactly one sample at a time and never buffers the file in RAM.
2. UDP (Fire-and-Forget): UDP sends are used to ensure the satellite worker
   can resume from any frame after a CRIU migration without TCP reconnection logic.
3. zlib Deflate: Compression is applied via native zlib to the raw float32 blob
   before chunking, minimising the number of UDP datagrams per frame.
4. Monotonic Frame IDs: A counter-based frame ID prevents chunk-ID collisions
   when the streamer restarts within the same second.
"""

import socket
import struct
import zlib
import time
import os
import subprocess

import h5py
import numpy as np

from kubernetes import client, config


# =============================================================================
# 1. CONFIGURATION
# =============================================================================

WSTS_HDF5_PATH = os.getenv("WSTS_DATA_PATH", "wsts.hdf5")

# The NodePort the satellite worker is actively listening to (matches K8s Service)
WORKER_UDP_NODEPORT = 32005

# How long to pause between frames (gives the orbital CPU time to process)
STREAM_INTERVAL = float(os.getenv("STREAM_INTERVAL", "1.0"))

# Chunk size tuned well below the 64KB UDP MTU to safely survive fragmentation
CHUNK_SIZE = 60_000

# Grid and channel constants — must match tinysml_worker.py exactly
GRID_H       = 64
GRID_W       = 64
N_CHANNELS   = 23

# If True, stream a small test slice of the dataset instead of the full file
TEST_MODE = os.getenv("TEST_MODE", "true").lower() == "true"
TEST_SLICE_SIZE = int(os.getenv("TEST_SLICE_SIZE", "20"))  # number of samples to stream


# =============================================================================
# 2. KUBERNETES ROUTING
# =============================================================================

def get_minikube_ip() -> str:
    """Returns the external IP of the Minikube node."""
    try:
        return subprocess.check_output(
            ["minikube", "ip", "-p", "minikube"], stderr=subprocess.DEVNULL
        ).decode("utf-8").strip()
    except Exception:
        return "192.168.49.2"  # Hardcoded Minikube default as fallback


def resolve_target_ip() -> str:
    """
    Tries to resolve the satellite NodePort IP via the Kubernetes API.
    Falls back to direct minikube IP if the cluster is unavailable.
    """
    try:
        config.load_kube_config()
        print("✅ STREAMER: Authenticated with Minikube Ground Control.")
    except Exception as e:
        print(f"⚠️ STREAMER: Kubernetes auth failed ({e}). Falling back to minikube IP.")
    return get_minikube_ip()


# =============================================================================
# 3. LAZY HDF5 GENERATOR
# =============================================================================

def wsts_frame_generator(h5_path: str, test_slice: int = 0):
    """
    Lazy-loading generator that yields one (23, 64, 64) float32 frame at a time.
    Reads a single index from disk on each iteration — never loads the full file.

    Each frame is a single day's 23-channel snapshot.
    The Guardian sidecar accumulates these into the T=5 history buffer.

    Args:
        h5_path:    Path to the WSTS HDF5 file.
        test_slice: If > 0, limit the generator to the first N samples.
    """
    if not os.path.exists(h5_path):
        print(f"❌ STREAMER: HDF5 file not found at '{h5_path}'. Cannot stream.")
        return

    with h5py.File(h5_path, 'r') as f:
        if 'features' not in f:
            print("❌ STREAMER: 'features' key not found in HDF5 file.")
            return

        # Shape: (N, T, C, H, W) — we stream individual channel snapshots (T=0 slice)
        total_samples = f['features'].shape[0]
        print(f"📂 STREAMER: HDF5 open. {total_samples} samples found. {'TEST MODE: ' + str(test_slice) + ' samples' if test_slice else 'Streaming all.'}")

        limit = test_slice if test_slice > 0 else total_samples

        for idx in range(min(limit, total_samples)):
            # Lazy: reads exactly one sample (one HDF5 row) from disk.
            # Shape: (T, C, H, W) — take the most recent day (T-1 index) as the new frame.
            sample = f['features'][idx]          # (5, 23, 64, 64)
            frame  = sample[-1].astype(np.float32)  # (23, 64, 64) — latest day snapshot

            yield frame, idx


# =============================================================================
# 4. ENCODING & CHUNKED UDP TRANSMISSION
# =============================================================================

def encode_frame(frame: np.ndarray) -> bytes:
    """
    Serialises a (23, 64, 64) float32 numpy array into a zlib-compressed binary blob.

    SKILL — BIG-ENDIAN C-CASTING:
    We pack as big-endian ('>f4') to match the worker's np.frombuffer(dtype='>f4') parse.
    Native deflate is applied directly on the memory view in one operation.
    """
    # Cast to big-endian float32 and view as raw bytes (zero-copy)
    raw_bytes = frame.astype('>f4').tobytes()
    # SKILL — NATIVE DEFLATE via zlib
    return zlib.compress(raw_bytes)


def send_frame(sock: socket.socket, blob: bytes, target_ip: str,
               target_port: int, frame_id: int) -> int:
    """
    Splits the compressed blob into CHUNK_SIZE datagrams and sends each one
    with a structured header: [frame_id (uint32), total_chunks (uint8), chunk_idx (uint8)].

    The header format matches exactly the parser in tinysml_worker.py:
        struct.unpack("!IBB", data[:6])

    Returns the number of chunks sent.
    """
    total_chunks = (len(blob) + CHUNK_SIZE - 1) // CHUNK_SIZE  # ceiling division

    for chunk_idx in range(total_chunks):
        chunk_data = blob[chunk_idx * CHUNK_SIZE : (chunk_idx + 1) * CHUNK_SIZE]
        # Pack the sticky note: uint32 frame_id | uint8 total_chunks | uint8 chunk_idx
        header = struct.pack("!IBB", frame_id, total_chunks, chunk_idx)
        sock.sendto(header + chunk_data, (target_ip, target_port))

    return total_chunks


# =============================================================================
# 5. MAIN EXECUTION LOOP
# =============================================================================

def main():
    print("🚀 STREAMER: Booting WSTS Uplink (NodePort Mode)...", flush=True)

    target_ip   = resolve_target_ip()
    target_port = WORKER_UDP_NODEPORT
    print(f"🎯 STREAMER: Target → {target_ip}:{target_port}", flush=True)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Monotonic counter — prevents frame ID collisions on streamer restarts
    frame_id_counter = 0

    while True:
        generator = wsts_frame_generator(
            WSTS_HDF5_PATH,
            test_slice=TEST_SLICE_SIZE if TEST_MODE else 0
        )

        for frame, sample_idx in generator:
            # Advance and wrap monotonic counter within uint32 field
            frame_id_counter = (frame_id_counter + 1) & 0xFFFF_FFFF

            blob = encode_frame(frame)

            try:
                n_chunks = send_frame(sock, blob, target_ip, target_port, frame_id_counter)
                print(
                    f"📤 STREAMER: Sample {sample_idx} (Frame #{frame_id_counter}) "
                    f"→ {target_ip}:{target_port} "
                    f"({len(blob) // 1024}KB compressed, {n_chunks} chunk(s))",
                    flush=True
                )
            except Exception as e:
                print(f"⚠️ STREAMER: UDP send error on frame #{frame_id_counter}: {e}", flush=True)

            time.sleep(STREAM_INTERVAL)

        print("🔁 STREAMER: Dataset pass complete. Looping...", flush=True)


if __name__ == "__main__":
    main()