"""
SPACE CLOUD V6 - KAGGLE WILDFIRE DATA STREAMER (Ground Station)
===============================================================

ROLE:
This script acts as the "Ground Station Transmitter." It reads historical wildfire
satellite imagery (the Kaggle Next Day Wildfire Spread dataset) from local .tfrecord files,
compresses it, and blasts it up to the active satellite worker in orbit via UDP.

KEY ARCHITECTURAL CHOICES:
1. Lazy Loading (Generators): Instead of loading 12 GB of data into RAM, this script
   reads exactly one frame at a time, sends it, and deletes it. This keeps host CPU
   and memory usage near zero.
2. UDP (Fire-and-Forget): We use UDP instead of TCP. Why? Because when a satellite
   gets too hot and migrates the worker, TCP connections break and crash. UDP just
   fires into the void. When the worker lands on the new satellite, it instantly
   catches the next UDP packet. Zero reconnection logic needed!
3. Native K8s API: We query the K8s API directly via Python to find where the worker
   is currently orbiting, completely eliminating the lag caused by spawning terminal
   subprocesses.
"""

import socket
import struct
import zlib
import time
import os
import subprocess

# We import TensorFlow solely to read Google's specific .tfrecord binary file format
import tensorflow as tf

# We import the official Kubernetes client to monitor where our satellite pod is
from kubernetes import client, config


# =============================================================================
# 1. CONFIGURATION & SETUP
# =============================================================================

# Directory containing your massive Kaggle .tfrecord files
DATASET_DIR = os.getenv(
    "DATASET_DIR",
    os.path.join(os.path.dirname(__file__), "wildfire_data")
)

# The network port the satellite worker is actively listening to
WORKER_UDP_PORT = 5005

# How fast we send frames (1 frame every 2 seconds gives the CPU time to breathe)
STREAM_INTERVAL = float(os.getenv("STREAM_INTERVAL", "3.0"))

# CRITICAL FIX: Force TensorFlow to be "quiet".
# By default, TF tries to use all available CPU cores. Since we are only reading files,
# we restrict it to 1 thread so it doesn't fight Minikube for CPU power.
tf.config.threading.set_inter_op_parallelism_threads(1)
tf.config.threading.set_intra_op_parallelism_threads(1)

# INITIALIZE KUBERNETES CLIENT
# We load the config from your local laptop (~/.kube/config) so this script
# has the authority to ask Minikube questions.
try:
    config.load_kube_config()
    k8s_v1 = client.CoreV1Api()
    print("✅ STREAMER: Authenticated with Minikube Ground Control.")
except Exception as e:
    print(f"❌ STREAMER: Failed to load Kube config. Is Minikube running? Error: {e}")
    k8s_v1 = None


# =============================================================================
# 2. DATA EXTRACTION (The Lazy-Loading Generator)
# =============================================================================

def dataset_generator():
    """
    This function is a 'Generator'. Notice the 'yield' keyword instead of 'return'.

    Instead of reading 10,000 images and returning a giant 12GB list, a generator
    acts like a conveyor belt. It pauses, hands exactly ONE image to the main loop,
    waits for the main loop to send it, and only then reads the next image.
    This prevents Out-Of-Memory (OOM) crashes.
    """

    # 1. Safety check: does the folder exist?
    if not os.path.isdir(DATASET_DIR):
        print(f"⚠️  STREAMER: Dataset directory not found: {DATASET_DIR}")
        return

    # 2. Find all files ending with .tfrecord in the folder
    tfrecord_files = [os.path.join(DATASET_DIR, f) for f in sorted(os.listdir(DATASET_DIR)) if f.endswith('.tfrecord')]

    if not tfrecord_files:
        print(f"❌ STREAMER: No .tfrecord files found in {DATASET_DIR}.")
        return

    print(f"📂 STREAMER: Found {len(tfrecord_files)} TFRecord files. Commencing lazy-stream...")

    # 3. Tell TensorFlow to open the files, but NOT to read them all into RAM yet
    raw_dataset = tf.data.TFRecordDataset(tfrecord_files)

    # 4. Loop through the dataset, reading ONE record at a time
    for raw_record in raw_dataset:

        # Parse the raw binary Protocol Buffer into an empty TensorFlow Example object
        example = tf.train.Example()
        example.ParseFromString(raw_record.numpy())

        # Create an empty Python dictionary to hold this specific frame's data
        sample_dict = {}

        # 5. Extract the features (Temperature, Wind, Vegetation, etc.)
        # The Kaggle dataset stores lists of numbers (float_list) or integers (int64_list).
        for key, feature in example.features.feature.items():
            if feature.HasField('float_list'):
                data_array = list(feature.float_list.value)
                sample_dict[key] = {
                    "data_type": "float_list",
                    "total_length": len(data_array),
                    "values": data_array
                }
            elif feature.HasField('int64_list'):
                data_array = list(feature.int64_list.value)
                sample_dict[key] = {
                    "data_type": "int64_list",
                    "total_length": len(data_array),
                    "values": data_array
                }

        # 6. YIELD the frame.
        # This pauses the function and hands the 'sample_dict' back to the main() loop.
        yield sample_dict


# =============================================================================
# 3. NETWORK ROUTING & ENCODING
# =============================================================================

def get_minikube_ip():
    """Finds the external IP of the Minikube virtual machine."""
    try:
        return subprocess.check_output(["minikube", "ip", "-p", "minikube"]).decode("utf-8").strip()
    except Exception:
        return "192.168.49.2" # Fallback to default Minikube IP


def encode_frame(sample_dict):
    """
    Takes the massive Python dictionary containing the 13 layers of satellite data
    and packs it into a highly compressed, raw binary string.

    Why? Because UDP has a size limit, and Python dictionaries are huge. We must
    squash this into raw bytes before shooting it over the network.
    """
    GRID_W, GRID_H = 64, 64

    # We must pack the channels in the exact order the Worker expects to receive them
    CHANNELS = [
        "PrevFireMask", "sph", "th", "elevation", "pdsi", "pr",
        "population", "erc", "NDVI", "tmmn", "vs", "tmmx", "FireMask"
    ]

    # Create an empty array of bytes
    blob = bytearray()

    for ch in CHANNELS:
        # Extract the flat list of 4096 numbers for this specific channel
        channel_data = sample_dict.get(ch, {})
        flat = channel_data.get("values", [0.0] * (GRID_W * GRID_H))

        flat = flat[:(GRID_W * GRID_H)]
        while len(flat) < (GRID_W * GRID_H):
            flat.append(0.0)

        # Pack each number into a 4-byte network-byte-order Float32 ('!f')
        for val in flat:
            blob.extend(struct.pack("!f", float(val)))

    # Finally, use zlib to compress the binary blob.
    # Because wildfire masks have a lot of empty space (zeros), it compresses beautifully!
    return zlib.compress(blob)


# =============================================================================
# 4. MAIN EXECUTION LOOP
# =============================================================================

def main():
    print("🚀 STREAMER: Booting UDP Telemetry Uplink (NodePort Mode)...")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    frame_idx = 0

    # We now target the K8s Post Office instead of the internal Pod
    target_ip = get_minikube_ip()
    target_port = 32005

    while True:
        for frame_dict in dataset_generator():
            frame_idx += 1

            blob = encode_frame(frame_dict)

            if target_ip:
                # 60 KB chunks keep us safely below the 64 KB UDP MTU limit
                CHUNK_SIZE = 60000
                total_chunks = (len(blob) // CHUNK_SIZE) + (1 if len(blob) % CHUNK_SIZE > 0 else 0)

                # Create a Unique Order Number for this specific frame
                frame_id = int(time.time() * 1000) & 0xFFFFFFFF

                try:
                    for i in range(total_chunks):
                        chunk_data = blob[i * CHUNK_SIZE: (i + 1) * CHUNK_SIZE]

                        # Pack the Sticky Note: [Order Number] [Total Boxes] [Box Number]
                        header = struct.pack("!IBB", frame_id, total_chunks, i)

                        # Fire the box at the K8s Post Office
                        sock.sendto(header + chunk_data, (target_ip, target_port))

                    print(
                        f"📤 STREAMER: Frame {frame_idx} (Order #{frame_id}) → {target_ip}:{target_port} ({len(blob) // 1024}KB in {total_chunks} chunks)",
                        flush=True)
                except Exception as e:
                    print(f"⚠️  STREAMER: UDP Send error: {e}", flush=True)

            time.sleep(STREAM_INTERVAL)

# Standard Python boilerplate to ensure main() runs when the script starts
if __name__ == "__main__":
    main()