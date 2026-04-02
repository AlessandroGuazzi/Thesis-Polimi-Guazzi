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
STREAM_INTERVAL = float(os.getenv("STREAM_INTERVAL", "2.0"))

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

def get_worker_pod_ip():
    """
    Queries the K8s API to find the exact IP address of the satellite pod
    that is currently running the 'space-mission' workload.

    If the pod is migrating, this returns None, and the streamer will simply
    drop the frame into the void (which is correct behavior).
    """
    if not k8s_v1:
        return None
    try:
        # Ask K8s: "Give me all pods labeled 'app=space-mission' in the default namespace"
        pods = k8s_v1.list_namespaced_pod(namespace="default", label_selector="app=space-mission")
        for pod in pods.items:
            # We only want the IP if the pod is fully awake ("Running")
            if pod.status.phase == "Running" and pod.status.pod_ip:
                return pod.status.pod_ip
    except Exception:
        pass
    return None


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
    print("🚀 STREAMER: Booting UDP Telemetry Uplink...")

    # Open a UDP socket. We do not "bind" it because we are the sender, not the receiver.
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    frame_idx = 0
    frames_dropped = 0

    # The infinite loop keeps the ground station running forever
    while True:

        # We loop over our Generator. It gives us one frame, then pauses.
        # When all .tfrecord files are fully read, the 'for' loop finishes,
        # but the 'while True' loop immediately restarts it from the beginning!
        for frame_dict in dataset_generator():
            frame_idx += 1

            # STEP 1: Compress the Python dictionary into raw network bytes
            blob = encode_frame(frame_dict)

            # STEP 2: Find out which satellite currently has the AI worker
            pod_ip = get_worker_pod_ip()

            # STEP 3: Transmit the data!
            if pod_ip:
                try:
                    # Shoot the compressed blob to the satellite's IP and Port
                    sock.sendto(blob, (pod_ip, WORKER_UDP_PORT))

                    # Print success (showing the compressed size in Kilobytes)
                    print(f"📤 STREAMER: Frame {frame_idx} → {pod_ip}:{WORKER_UDP_PORT} ({len(blob)//1024}KB)", flush=True)
                except Exception as e:
                    print(f"⚠️  STREAMER: Send error: {e}", flush=True)
            else:
                # If no IP was found, the satellite is in the middle of a CRIU jump!
                frames_dropped += 1
                print(f"🌌 STREAMER: Frame {frame_idx} → No pod running (migration in flight).", flush=True)

            # STEP 4: Pace the transmission.
            # Wait 2 seconds before asking the generator for the next frame.
            time.sleep(STREAM_INTERVAL)

# Standard Python boilerplate to ensure main() runs when the script starts
if __name__ == "__main__":
    main()