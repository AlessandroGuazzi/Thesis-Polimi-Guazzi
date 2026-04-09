"""
===============================================================================
SPACE CLOUD V6 - KAGGLE WILDFIRE DATA STREAMER (Ground Station)
===============================================================================

🧠 HIGH-LEVEL PURPOSE:
This script simulates a "Ground Station Transmitter" in a distributed satellite
system.

Its job is to:
1. Read historical wildfire satellite data from disk (.tfrecord files)
2. Convert each data frame into a compact binary format
3. Stream it via UDP to a satellite worker (running inside Kubernetes)

-------------------------------------------------------------------------------
🏗️ ARCHITECTURAL DESIGN CHOICES (VERY IMPORTANT TO UNDERSTAND):

1. LAZY LOADING (Generators)
   - The dataset is huge (~12GB)
   - Instead of loading everything into memory, we process ONE frame at a time
   - This avoids memory crashes and keeps the system lightweight

2. UDP COMMUNICATION (Fire-and-Forget)
   - UDP is used instead of TCP
   - Why?
     → In Kubernetes, pods can move between nodes
     → TCP connections would break
     → UDP doesn't care — it just keeps sending packets
   - The receiving worker simply processes whatever arrives next

3. DIRECT KUBERNETES API USAGE
   - Instead of calling shell commands repeatedly
   - We directly query Kubernetes via Python client
   - This is faster and avoids subprocess overhead

-------------------------------------------------------------------------------
"""

import socket
import struct
import zlib
import time
import os
import subprocess

# TensorFlow is used ONLY to read TFRecord files (Google's binary dataset format)
import tensorflow as tf

# Kubernetes client used to interact with cluster (find worker location)
from kubernetes import client, config


# =============================================================================
# 1. CONFIGURATION & SETUP
# =============================================================================

# Path where TFRecord dataset is stored
# If environment variable DATASET_DIR exists → use it
# Otherwise → default to local folder "wildfire_data"
DATASET_DIR = os.getenv(
    "DATASET_DIR",
    os.path.join(os.path.dirname(__file__), "wildfire_data")
)

# UDP port where the satellite worker is listening
WORKER_UDP_PORT = 5005

# Time interval between sending frames (in seconds)
STREAM_INTERVAL = float(os.getenv("STREAM_INTERVAL", "3.0"))

# -----------------------------------------------------------------------------
# ⚠️ TensorFlow Thread Limitation (CRITICAL PERFORMANCE FIX)
# -----------------------------------------------------------------------------
# By default, TensorFlow uses ALL CPU cores.
# Here we restrict it to 1 thread because we are ONLY reading files,
# not doing heavy ML computation.
# This avoids CPU contention with Kubernetes (Minikube).
tf.config.threading.set_inter_op_parallelism_threads(1)
tf.config.threading.set_intra_op_parallelism_threads(1)

# -----------------------------------------------------------------------------
# Initialize Kubernetes Client
# -----------------------------------------------------------------------------
# This loads your local kubeconfig (~/.kube/config)
# and allows Python to query cluster state
try:
    config.load_kube_config()
    k8s_v1 = client.CoreV1Api()
    print("✅ STREAMER: Authenticated with Minikube Ground Control.")
except Exception as e:
    print(f"❌ STREAMER: Failed to load Kube config. Is Minikube running? Error: {e}")
    k8s_v1 = None


# =============================================================================
# 2. DATA EXTRACTION (Lazy Generator)
# =============================================================================

def dataset_generator():
    """
    🧠 PURPOSE:
    This function streams dataset frames ONE AT A TIME using a generator.

    Instead of:
        return [frame1, frame2, ..., frame10000]

    It behaves like:
        yield frame1
        (pause)
        yield frame2
        (pause)

    This is essential for:
    - memory efficiency
    - scalability
    """

    # --- Safety check: dataset directory must exist ---
    if not os.path.isdir(DATASET_DIR):
        print(f"⚠️  STREAMER: Dataset directory not found: {DATASET_DIR}")
        return

    # --- Find all TFRecord files ---
    tfrecord_files = [
        os.path.join(DATASET_DIR, f)
        for f in sorted(os.listdir(DATASET_DIR))
        if f.endswith('.tfrecord')
    ]

    if not tfrecord_files:
        print(f"❌ STREAMER: No .tfrecord files found in {DATASET_DIR}.")
        return

    print(f"📂 STREAMER: Found {len(tfrecord_files)} TFRecord files. Commencing lazy-stream...")

    # TensorFlow dataset (lazy reader — does NOT load everything)
    raw_dataset = tf.data.TFRecordDataset(tfrecord_files)

    # --- Iterate ONE record at a time ---
    for raw_record in raw_dataset:

        # Parse raw binary into TensorFlow Example object
        example = tf.train.Example()
        example.ParseFromString(raw_record.numpy())  # ⚠️ Converts tensor → raw bytes

        # Dictionary that will store parsed data
        sample_dict = {}

        # --- Extract features dynamically ---
        for key, feature in example.features.feature.items():

            # Case 1: float data
            if feature.HasField('float_list'):
                data_array = list(feature.float_list.value)  # Convert TF list → Python list
                sample_dict[key] = {
                    "data_type": "float_list",
                    "total_length": len(data_array),
                    "values": data_array
                }

            # Case 2: integer data
            elif feature.HasField('int64_list'):
                data_array = list(feature.int64_list.value)
                sample_dict[key] = {
                    "data_type": "int64_list",
                    "total_length": len(data_array),
                    "values": data_array
                }

        # --- Yield ONE frame ---
        yield sample_dict


# =============================================================================
# 3. NETWORK ROUTING & ENCODING
# =============================================================================

def get_minikube_ip():
    """
    🧠 PURPOSE:
    Retrieves the IP address of the Minikube cluster.

    WHY:
    The satellite worker runs inside Kubernetes → we need its external IP.

    FALLBACK:
    If command fails → return default Minikube IP
    """
    try:
        return subprocess.check_output(
            ["minikube", "ip", "-p", "minikube"]
        ).decode("utf-8").strip()  # ⚠️ bytes → string conversion
    except Exception:
        return "192.168.49.2"  # Default fallback


def encode_frame(sample_dict):
    """
    🧠 PURPOSE:
    Converts a Python dictionary (huge, inefficient) into a compact binary blob.

    WHY:
    - UDP has size limits
    - Dictionaries are too large and slow to send
    - Binary + compression = fast + efficient

    PROCESS:
    1. Extract each channel
    2. Flatten data
    3. Pack into binary (float32)
    4. Compress using zlib
    """

    GRID_W, GRID_H = 64, 64  # Fixed spatial resolution

    # ⚠️ CRITICAL: Channel order MUST match worker expectations
    CHANNELS = [
        "PrevFireMask", "sph", "th", "elevation", "pdsi", "pr",
        "population", "erc", "NDVI", "tmmn", "vs", "tmmx", "FireMask"
    ]

    blob = bytearray()  # Mutable byte container

    for ch in CHANNELS:

        # Extract channel values safely
        channel_data = sample_dict.get(ch, {})
        flat = channel_data.get("values", [0.0] * (GRID_W * GRID_H))

        # Ensure exact size (4096 values)
        flat = flat[:(GRID_W * GRID_H)]

        # Pad if too short
        while len(flat) < (GRID_W * GRID_H):
            flat.append(0.0)

        # ⚠️ struct.pack("!f", value)
        # "!f" means:
        #   ! → network byte order (big-endian)
        #   f → 32-bit float
        for val in flat:
            blob.extend(struct.pack("!f", float(val)))

    # Compress entire binary payload
    return zlib.compress(blob)


# =============================================================================
# 4. MAIN EXECUTION LOOP
# =============================================================================

def main():
    """
    🧠 PURPOSE:
    Main loop that:
    1. Reads dataset frames
    2. Encodes them
    3. Sends them via UDP in chunks
    """

    print("🚀 STREAMER: Booting UDP Telemetry Uplink (NodePort Mode)...")

    # Create UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    frame_idx = 0

    # Target = Kubernetes NodePort service
    target_ip = get_minikube_ip()
    target_port = 32005

    while True:

        # Iterate over dataset frames
        for frame_dict in dataset_generator():
            frame_idx += 1

            # Encode into compressed binary
            blob = encode_frame(frame_dict)

            if target_ip:

                # ⚠️ UDP max safe size ≈ 64KB → we stay below
                CHUNK_SIZE = 60000

                # Compute number of chunks needed
                total_chunks = (
                    (len(blob) // CHUNK_SIZE) +
                    (1 if len(blob) % CHUNK_SIZE > 0 else 0)
                )

                # Unique frame ID (timestamp-based, 32-bit masked)
                frame_id = int(time.time() * 1000) & 0xFFFFFFFF

                try:
                    for i in range(total_chunks):

                        # Slice chunk
                        chunk_data = blob[i * CHUNK_SIZE: (i + 1) * CHUNK_SIZE]

                        # Header structure:
                        # !IBB →
                        #   I = unsigned int (frame_id)
                        #   B = unsigned char (total_chunks)
                        #   B = unsigned char (chunk index)
                        header = struct.pack("!IBB", frame_id, total_chunks, i)

                        # Send packet
                        sock.sendto(
                            header + chunk_data,
                            (target_ip, target_port)
                        )

                    print(
                        f"📤 STREAMER: Frame {frame_idx} (Order #{frame_id}) → "
                        f"{target_ip}:{target_port} "
                        f"({len(blob) // 1024}KB in {total_chunks} chunks)",
                        flush=True
                    )

                except Exception as e:
                    print(f"⚠️  STREAMER: UDP Send error: {e}", flush=True)

            # Throttle sending rate
            time.sleep(STREAM_INTERVAL)


# -----------------------------------------------------------------------------
# Entry point
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    main()