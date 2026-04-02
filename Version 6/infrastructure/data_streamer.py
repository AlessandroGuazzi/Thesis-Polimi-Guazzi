"""
SPACE CLOUD V6 - KAGGLE WILDFIRE DATA STREAMER (Ground Station)
===============================================================
Role: Runs on the Ground Station (Minikube host). Reads pre-processed samples
      from the Kaggle "Next Day Wildfire Spread" dataset, encodes each 64×64
      observation frame as a compressed binary blob, and sends it to the active
      satellite pod via UDP.

Key design choice: UDP is fire-and-forget. While the pod is migrating, datagrams
hit nothing. The instant the pod lands on a new satellite and rebinds its socket,
it immediately starts receiving the next frame. No reconnection needed.

Data format (from sample_fire_data.json):
  - 12 input channels + 1 label channel (FireMask)
  - Each channel: 4,096 float32 values (flattened 64×64 grid)
  - Packed into a binary blob and zlib-compressed before transmission
"""

import socket
import struct
import zlib
import json
import tensorflow as tf
import time
import os
import sys

# Use the official Kubernetes Python client instead of subprocess('kubectl')
# to avoid the O(fork+exec) syscall cost per frame (Issue #1: subprocess bottleneck)
from kubernetes import client, config

# =============================================================================
# CONFIGURATION
# =============================================================================

# Path to the pre-processed dataset directory.
# Each file is a JSON sample with the same schema as sample_fire_data.json
DATASET_DIR = os.getenv(
    "DATASET_DIR",
    os.path.join(os.path.dirname(__file__), "wildfire_data")
)

# UDP port where the tinySML worker listens for incoming frames
WORKER_UDP_PORT = 5005

# How many seconds to wait between sending frames.
# Use 1.0 for real-time simulation, or 0.0 for fastest possible throughput.
STREAM_INTERVAL = float(os.getenv("STREAM_INTERVAL", "1.0"))

# Grid dimensions — must match the worker's expectations exactly
GRID_W = 64
GRID_H = 64

# The 12 input channels, in transmission order.
# This order must match what tinysml_worker.py's FEATURE_CHANNELS defines.
FEATURE_CHANNELS = [
    "PrevFireMask",  # 0 — previous fire presence
    "sph",           # 1 — specific humidity
    "th",            # 2 — wind direction
    "elevation",     # 3 — terrain height
    "pdsi",          # 4 — drought index
    "pr",            # 5 — precipitation
    "population",    # 6 — population density
    "erc",           # 7 — energy release component
    "NDVI",          # 8 — vegetation index
    "tmmn",          # 9 — min temperature
    "vs",            # 10 — wind speed
    "tmmx",          # 11 — max temperature
]

# The ground-truth label channel (next-day fire spread)
LABEL_CHANNEL = "FireMask"


# =============================================================================
# WORKER POD DISCOVERY (via K8s Python Client — not subprocess)
# Issue #1 fix: The old implementation spawned 'kubectl get pod...' via
# subprocess.check_output() inside the while-loop, forking a new OS process
# every single frame. This has ~50ms overhead per call due to fork+exec+kubectl
# startup. Using the Python client reuses a persistent HTTP connection.
# =============================================================================

# Initialise the K8s API client once at module load (reuses TCP connection)
try:
    config.load_kube_config()       # Minikube host — reads ~/.kube/config
except Exception:
    config.load_incluster_config()  # Fallback: running inside a Pod
_k8s_v1 = client.CoreV1Api()


# Define the exact shapes and types expected by the Kaggle dataset
def _parse_tfrecord(example_proto):
    # The dataset features 12 input channels and 1 label channel, all 64x64 grids
    feature_description = {
        'PrevFireMask': tf.io.FixedLenFeature([64, 64], tf.float32),
        'sph':          tf.io.FixedLenFeature([64, 64], tf.float32),
        'th':           tf.io.FixedLenFeature([64, 64], tf.float32),
        'elevation':    tf.io.FixedLenFeature([64, 64], tf.float32),
        'pdsi':         tf.io.FixedLenFeature([64, 64], tf.float32),
        'pr':           tf.io.FixedLenFeature([64, 64], tf.float32),
        'population':   tf.io.FixedLenFeature([64, 64], tf.float32),
        'erc':          tf.io.FixedLenFeature([64, 64], tf.float32),
        'NDVI':         tf.io.FixedLenFeature([64, 64], tf.float32),
        'tmmn':         tf.io.FixedLenFeature([64, 64], tf.float32),
        'vs':           tf.io.FixedLenFeature([64, 64], tf.float32),
        'tmmx':         tf.io.FixedLenFeature([64, 64], tf.float32),
        'FireMask':     tf.io.FixedLenFeature([64, 64], tf.float32),
    }
    return tf.io.parse_single_example(example_proto, feature_description)


def get_worker_pod_ip():
    """
    Queries the Kubernetes API to find the current IP address of the satellite
    pod hosting the tinySML worker. Returns None if no pod is currently running
    (expected during migrations — the streamer will simply skip that frame).

    Uses the official K8s Python client instead of subprocess('kubectl') to
    avoid the O(fork+exec) overhead. The API client maintains a persistent
    HTTP connection pool, so repeated calls are near-zero cost.
    """
    try:
        pods = _k8s_v1.list_namespaced_pod(
            namespace="default",
            label_selector="app=space-mission",
            _request_timeout=3
        )
        for pod in pods.items:
            # Only use pods that are Running (not Pending/Terminating)
            if (pod.status.phase == "Running"
                    and not pod.metadata.deletion_timestamp
                    and pod.status.pod_ip):
                return pod.status.pod_ip
        return None
    except Exception:
        return None


# =============================================================================
# FRAME ENCODING
# =============================================================================

def encode_frame(sample):
    """
    Converts a single JSON sample into a compressed binary UDP payload.

    Encoding:
    1. Read each of the 12 input channels as a flat list of 4,096 float32 values.
    2. Append the FireMask label channel (also 4,096 floats).
    3. Pack all 13 × 4,096 = 53,248 floats into a network-byte-order binary blob.
    4. Compress with zlib to reduce to ~30–60 KB (fire masks are sparse ≈ mostly zeros).

    The worker decodes this with zlib.decompress() + struct.unpack().
    """
    all_values = []

    # Pack the 12 input feature channels in the agreed-upon order
    for channel_name in FEATURE_CHANNELS:
        channel_data = sample.get(channel_name, {})
        # The dataset stores each channel as {"data_type": ..., "total_length": 4096, ...}
        # but we expect a pre-flattened list; support both formats for flexibility
        if isinstance(channel_data, dict):
            # Full JSON format from the Kaggle extraction script
            flat = channel_data.get("values", [0.0] * GRID_W * GRID_H)
        elif isinstance(channel_data, list):
            # Pre-flattened format (simpler files)
            flat = channel_data
        else:
            flat = [0.0] * GRID_W * GRID_H

        # Ensure we have exactly 4,096 values — pad or truncate if needed
        flat = flat[:GRID_W * GRID_H]
        while len(flat) < GRID_W * GRID_H:
            flat.append(0.0)

        all_values.extend(flat)

    # Pack the FireMask label channel last
    label_data = sample.get(LABEL_CHANNEL, {})
    if isinstance(label_data, dict):
        label_flat = label_data.get("values", [0.0] * GRID_W * GRID_H)
    elif isinstance(label_data, list):
        label_flat = label_data
    else:
        label_flat = [0.0] * GRID_W * GRID_H

    label_flat = label_flat[:GRID_W * GRID_H]
    while len(label_flat) < GRID_W * GRID_H:
        label_flat.append(0.0)
    all_values.extend(label_flat)

    # Pack as network-byte-order float32 values ('!' = big-endian, 'f' = float32)
    total_floats = (len(FEATURE_CHANNELS) + 1) * GRID_W * GRID_H  # 53,248
    binary_blob = struct.pack(f"!{total_floats}f", *all_values)

    # Compress to reduce transmission size (typical compression 5–8×)
    compressed = zlib.compress(binary_blob, level=6)  # Level 6 = good balance

    return compressed


# =============================================================================
# DATASET LOADING
# =============================================================================

def load_dataset():
    """
    Loads all .tfrecord sample files from DATASET_DIR.
    Each file represents one 64x64 geographic region at one timestamp.
    """
    samples = []

    if not os.path.isdir(DATASET_DIR):
        print(f"⚠️  STREAMER: Dataset directory not found: {DATASET_DIR}")
        return samples

    # Find all .tfrecord files instead of .json
    tfrecord_files = [os.path.join(DATASET_DIR, f) for f in sorted(os.listdir(DATASET_DIR)) if f.endswith('.tfrecord')]
    
    if not tfrecord_files:
        print(f"❌ STREAMER: No .tfrecord files found in {DATASET_DIR}.")
        return samples

    print(f"📂 STREAMER: Found {len(tfrecord_files)} TFRecord files. Parsing natively...")
    
    # Load and parse the dataset using TensorFlow
    raw_dataset = tf.data.TFRecordDataset(tfrecord_files)
    parsed_dataset = raw_dataset.map(_parse_tfrecord)
    
    for parsed_record in parsed_dataset:
        sample_dict = {}
        for key in parsed_record.keys():
            # Flatten the 64x64 tensor into a 1D list of 4096 floats
            flat_values = parsed_record[key].numpy().flatten().tolist()
            
            # Format exactly like sample_fire_data.json
            sample_dict[key] = {
                "data_type": "float_list",
                "total_length": 4096,
                "values": flat_values
            }
            
        samples.append(sample_dict)
        
    print(f"✅ STREAMER: Successfully extracted {len(samples)} frames directly from TFRecords.")
    return samples


# =============================================================================
# MAIN STREAMING LOOP
# =============================================================================

def main():
    print("📡 WILDFIRE DATA STREAMER V6 ONLINE.", flush=True)

    # Load the dataset (or fall back to sample_fire_data.json for testing)
    samples = load_dataset()
    if not samples:
        print("❌ STREAMER: No dataset frames found. Exiting.", flush=True)
        sys.exit(1)

    # Pre-encode all frames into compressed binary blobs to avoid encoding overhead
    # during the hot streaming path
    print(f"🗜️  STREAMER: Encoding {len(samples)} frames...", flush=True)
    encoded_frames = []
    for i, sample in enumerate(samples):
        blob = encode_frame(sample)
        encoded_frames.append(blob)
        if (i + 1) % 10 == 0:
            print(f"   → Encoded {i+1}/{len(samples)} frames", flush=True)

    print(f"✅ STREAMER: All frames encoded. Sizes: "
          f"min={min(len(f) for f in encoded_frames)//1024}KB, "
          f"max={max(len(f) for f in encoded_frames)//1024}KB", flush=True)

    # Open the sending UDP socket (no bind — we are the sender, not the receiver)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    frame_idx = 0
    frames_sent = 0
    frames_dropped = 0  # Frames dropped because no pod was running during migration

    while True:
        # Cycle through the dataset in a loop (replays indefinitely)
        blob = encoded_frames[frame_idx % len(encoded_frames)]
        frame_idx += 1

        # Discover the current Pod IP (pod may have moved since last frame)
        pod_ip = get_worker_pod_ip()

        if pod_ip:
            try:
                # Transmit the compressed frame to the worker's UDP socket
                sock.sendto(blob, (pod_ip, WORKER_UDP_PORT))
                frames_sent += 1
                print(
                    f"📤 STREAMER: Frame {frame_idx} → {pod_ip}:{WORKER_UDP_PORT} "
                    f"({len(blob)//1024}KB) | Sent: {frames_sent}, Dropped: {frames_dropped}",
                    flush=True
                )
            except Exception as e:
                print(f"⚠️  STREAMER: Send error: {e}", flush=True)
        else:
            # Pod is migrating — fire into the void (this is expected and correct)
            frames_dropped += 1
            print(
                f"🌌 STREAMER: Frame {frame_idx} → no pod running (migration in flight). "
                f"Dropped so far: {frames_dropped}",
                flush=True
            )

        # Wait before sending the next frame
        time.sleep(STREAM_INTERVAL)


if __name__ == "__main__":
    main()
