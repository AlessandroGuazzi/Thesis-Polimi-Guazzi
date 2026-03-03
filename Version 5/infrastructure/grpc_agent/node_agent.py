import os
import time
import json
import subprocess
import threading
from concurrent import futures
import requests
import socket

import grpc
import redis

# Importing generated gRPC and Protocol Buffer stubs for Peer-to-Peer communication
import checkpoint_transfer_pb2
import checkpoint_transfer_pb2_grpc

# Environment Configuration
# NODE_NAME: Current physical satellite host
# REDIS_HOST: Central message broker address
NODE_NAME = os.getenv("NODE_NAME", "minikube-m02")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
GRPC_PORT = "50051"


# ==============================================================================
# 2.1 THE RECEIVER (gRPC Server) - Handles incoming memory dumps
# ==============================================================================
class FileTransferServicer(checkpoint_transfer_pb2_grpc.FileTransferServicer):

    def StreamCheckpoint(self, request_iterator, context):
        """Receives a binary stream of the memory dump and saves it to disk."""
        print(f"\n📥 [gRPC SERVER] Incoming data stream directed to {NODE_NAME}...")
        save_path = "/tmp/checkpoint.tar"

        # Sequential writing of incoming chunks to a TAR archive
        with open(save_path, "wb") as f:
            for chunk in request_iterator:
                f.write(chunk.content)

        print(f"✅ [gRPC SERVER] File received! Triggering background reconstruction...")

        # We launch the local deployment logic in a separate thread to immediately
        # release the gRPC connection while the node works on the restore
        threading.Thread(target=self.rebuild_and_deploy, args=(save_path,)).start()

        return checkpoint_transfer_pb2.TransferStatus(success=True, message="Reception completed!")

    # ==============================================================================
    # 2.2 LOCAL ORCHESTRATOR (Buildah + Kube) - Restores the workload locally
    # ==============================================================================
    def rebuild_and_deploy(self, tar_path):
        """Converts the TAR memory dump into a K8s-ready container image."""
        t_start_total = time.time()

        # --- STEP 1: BUILDAH IMAGE RECONSTRUCTION ---
        t0 = time.time()
        print("🔨 [BUILDAH] Reconstructing container image from memory dump...")
        # We use Buildah to create a new image 'localhost/space-sidecar:restored'
        # based on the incoming memory pages
        build_script = f"""
        buildah rm restoration-lab 2>/dev/null || true
        buildah from --name restoration-lab scratch
        buildah add restoration-lab {tar_path} /
        MNT=$(buildah mount restoration-lab) && rm -f $MNT/tmp/prepare_jump && buildah unmount restoration-lab
        buildah config --annotation "io.kubernetes.cri-o.annotations.checkpoint.name=sidecar-guardian" restoration-lab
        buildah commit restoration-lab localhost/space-sidecar:restored
        buildah rm restoration-lab
        """
        subprocess.run(build_script, shell=True, check=False)
        t_buildah = time.time() - t0
        print(f"⏱️  [TIMING] Buildah compilation completed in {t_buildah:.2f} seconds.")

        # --- STEP 2: K8S DEPLOYMENT SWITCH ---
        t0 = time.time()
        print("⚡ [K8S] Patching Deployment to switch execution to this node...")
        # Patching JSON to update nodeSelector and image name to the 'restored' version
        patch_json = {
            "spec": {
                "template": {
                    "spec": {
                        "terminationGracePeriodSeconds": 0,
                        "nodeSelector": {"type": "satellite", "kubernetes.io/hostname": NODE_NAME},
                        "containers": [
                            {"name": "sidecar-guardian", "image": "localhost/space-sidecar:restored"},
                            {"name": "payload-phoenix", "image": "localhost/space-workload:latest"}
                        ]
                    }
                }
            }
        }
        patch_str = json.dumps(patch_json)

        # Sequence to force-refresh the Pod on the current node
        # 1. Scale down to zero
        subprocess.run("kubectl scale deployment space-mission --replicas=0", shell=True)
        # 2. Force delete any lingering pod from the previous node (bypasses 'Terminating' delay)
        subprocess.run("kubectl delete pod -l app=space-mission --force --grace-period=0", shell=True,
                       stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        # 3. Apply the patch with new coordinates
        subprocess.run(f"kubectl patch deployment space-mission --type='strategic' -p '{patch_str}'", shell=True)
        # 4. Scale up to one to spawn the pod on this node
        subprocess.run("kubectl scale deployment space-mission --replicas=1", shell=True)

        t_patch = time.time() - t0
        print(f"⏱️  [TIMING] K8s reprogramming sent in {t_patch:.2f} seconds.")

        # --- STEP 3: INTELLIGENT WAIT FOR CONTAINER AWAKENING ---
        t0 = time.time()
        print("⏳ [K8S] Waiting for Pod restart and container awakening...")
        success = False
        pod_name = None

        # Polling loop to detect when the pod is ready to receive commands
        for _ in range(50):
            if not pod_name:
                try:
                    output = subprocess.check_output(
                        f"kubectl get pod -l app=space-mission --field-selector spec.nodeName={NODE_NAME} -o jsonpath='{{.items[0].metadata.name}}'",
                        shell=True, stderr=subprocess.DEVNULL)
                    found_name = output.decode('utf-8').strip()
                    if found_name:
                        pod_name = found_name
                except Exception:
                    pass

            if pod_name:
                try:
                    exec_cmd = f"kubectl exec {pod_name} -c sidecar-guardian -- touch /tmp/landed"
                    res = subprocess.run(exec_cmd, shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
                    if res.returncode == 0:
                        success = True
                        break
                except Exception:
                    pass

            time.sleep(0.1)

        t_wait = time.time() - t0

        if success:
            print(f"⏱️  [TIMING] Pod emerged and container awakened in {t_wait:.2f} seconds!")
        else:
            print(f"❌ [TIMING] Timeout! Pod did not wake up after {t_wait:.2f} seconds.")

        t_total = time.time() - t_start_total
        print(f"🎉 [ORCHESTRATOR] Migration completed locally in {t_total:.2f}s total!")


# ==============================================================================
# 2.3 THE SENDER (gRPC Client) - Freezes and ships the state
# ==============================================================================
def generate_chunks(filepath, chunk_size=1024 * 1024):
    """Generator to split the memory TAR into 1MB chunks for binary streaming."""
    with open(filepath, 'rb') as f:
        while True:
            data = f.read(chunk_size)
            if not data: break
            yield checkpoint_transfer_pb2.Chunk(content=data)


def execute_migration_sender(target_node):
    """Orchestrates the local checkpointing and P2P streaming to the target satellite."""
    t_start_total = time.time()
    print(f"\n🚀 [SENDER] Executing order! Freezing and shipping to {target_node}...")

    # Identify local pod name to target the checkpoint
    pod_name = subprocess.check_output(
        f"kubectl get pod -l app=space-mission --field-selector spec.nodeName={NODE_NAME} -o jsonpath='{{.items[0].metadata.name}}'",
        shell=True).decode('utf-8').strip()

    # Notify the Sidecar Guardian to prepare for migration (close sockets, etc.)
    subprocess.run(f"kubectl exec {pod_name} -c sidecar-guardian -- touch /tmp/prepare_jump", shell=True)
    time.sleep(0.5)  # Grace period for the app to disconnect safely

    # --- PHASE 1: CRIU CHECKPOINT ---
    t0 = time.time()
    print("📸 [SENDER] Requesting RAM Snapshot from Kubelet API...")
    # Open a local proxy to securely communicate with the K8s API
    proxy_proc = subprocess.Popen(["kubectl", "proxy", "--port=8001"], stdout=subprocess.DEVNULL)

    health_url = f"http://127.0.0.1:8001/api/v1/nodes/{NODE_NAME}/proxy/healthz"
    for _ in range(30):  # Massimo 3 secondi di attesa (30 * 0.1s)
        try:
            # Se il Kubelet risponde "ok" tramite il proxy, il tunnel è perfettamente allineato
            if requests.get(health_url, timeout=0.2).status_code == 200:
                break
        except requests.exceptions.RequestException:
            pass
        time.sleep(0.1)

    api_url = f"http://127.0.0.1:8001/api/v1/nodes/{NODE_NAME}/proxy/checkpoint/default/{pod_name}/sidecar-guardian"
    response = requests.post(api_url)
    proxy_proc.terminate()  # Close proxy immediately after the request

    if response.status_code != 200:
        print(f"❌ [SENDER] Kubelet API rejected the request (Status {response.status_code}): {response.text}")
        return

    try:
        data = response.json()
    except Exception as e:
        print(f"❌ [SENDER] Kubelet didn't answer in JSON. Raw answer: {response.text}")
        return

    checkpoint_path = data["items"][0]
    t_checkpoint = time.time() - t0
    print(f"⏱️  [TIMING] Checkpoint generated in {t_checkpoint:.2f} seconds.")

    # Find the IP address of the target Node Agent for Peer-to-Peer transfer
    print(f"🔍 [SENDER] IP request fo node: {target_node}...")
    try:
        target_ip = subprocess.check_output(
            f"kubectl get node {target_node} -o jsonpath='{{.status.addresses[?(@.type==\"InternalIP\")].address}}'",
            shell=True).decode('utf-8').strip()
    except Exception as e:
        print(f"❌ [SENDER] Error in recovery of IP: {e}")
        return

    # --- PHASE 2: gRPC STREAMING TRANSFER ---
    t0 = time.time()
    print(f"📡 [SENDER] Opening gRPC stream towards {target_ip}:{GRPC_PORT}...")
    channel = grpc.insecure_channel(f"{target_ip}:{GRPC_PORT}")
    stub = checkpoint_transfer_pb2_grpc.FileTransferStub(channel)

    # Send the memory dump chunk by chunk over the binary tunnel
    try:
        file_size_bytes = os.path.getsize(checkpoint_path)
        file_size_mb = file_size_bytes / (1024 * 1024)
    except Exception:
        file_size_mb = 0.0

    status = stub.StreamCheckpoint(generate_chunks(checkpoint_path))
    t_transfer = time.time() - t0
    print(f"⏱️  [TIMING] gRPC {file_size_mb:.2f}MB transfer completed in {t_transfer:.2f} seconds.")

    t_total = time.time() - t_start_total
    print(f"🏁 [SENDER] Outbound operations concluded in {t_total:.2f}s total!")


# ==============================================================================
# THE NERVOUS SYSTEM (Redis Listener) - Listens for central commands
# ==============================================================================
def redis_listener():
    """Subscribes to the specific node channel to receive migration triggers."""
    channel = f"commands/{NODE_NAME}"
    print(f"📻 [REDIS] Subscribing to command channel: {channel}...")

    while True:
        try:
            r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True, socket_connect_timeout=3)
            r.ping()  # Connection check

            pubsub = r.pubsub()
            pubsub.subscribe(channel)
            print(f"✅ [REDIS] Connected to Central DB! Listening for migration orders...")

            # Blocking loop waiting for JSON commands from the MPC Controller
            for message in pubsub.listen():
                if message['type'] == 'message':
                    data = json.loads(message['data'])
                    if data.get('action') == 'MIGRATE':
                        target = data.get('target_node')
                        # Trigger the sender logic in a new thread for non-blocking execution
                        threading.Thread(target=execute_migration_sender, args=(target,)).start()

        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            print(f"⚠️ [REDIS] Connection lost with {REDIS_HOST}. Retrying in 3s...")
            time.sleep(3)
        except Exception as e:
            print(f"❌ [REDIS] Unexpected receiver error: {e}")
            time.sleep(3)


# ==============================================================================
# INITIALIZER
# ==============================================================================
if __name__ == '__main__':
    # 1. Start the gRPC Receiver Server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    checkpoint_transfer_pb2_grpc.add_FileTransferServicer_to_server(FileTransferServicer(), server)
    server.add_insecure_port(f'[::]:{GRPC_PORT}')
    server.start()
    print(f"🛡️  [AGENT] gRPC Server active on port {GRPC_PORT}.")

    # 2. Start the Redis Listener (Blocking main loop)
    try:
        redis_listener()
    except KeyboardInterrupt:
        server.stop(0)