import os
import time
import json
import subprocess
import threading
from concurrent import futures
import requests

import grpc
import redis

# Importiamo le impalcature generate nella Fase 1
import checkpoint_transfer_pb2
import checkpoint_transfer_pb2_grpc

# Configurazione Ambiente
NODE_NAME = os.getenv("NODE_NAME", "minikube-m02")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
GRPC_PORT = "50051"


# ==============================================================================
# 2.1 IL RICEVITORE (gRPC Server) - Gestisce i pacchetti in arrivo
# ==============================================================================
class FileTransferServicer(checkpoint_transfer_pb2_grpc.FileTransferServicer):

    def StreamCheckpoint(self, request_iterator, context):
        print(f"\n📥 [gRPC SERVER] Inizio ricezione flusso dati verso {NODE_NAME}...")
        save_path = "/tmp/checkpoint.tar"

        # Riceve i chunk in streaming e li incolla su disco
        with open(save_path, "wb") as f:
            for chunk in request_iterator:
                f.write(chunk.content)

        print(f"✅ [gRPC SERVER] File ricevuto! Avvio ricostruzione in background...")
        # Avviamo il processo Kube/Buildah in un thread separato
        # per liberare subito la connessione gRPC
        threading.Thread(target=self.rebuild_and_deploy, args=(save_path,)).start()

        return checkpoint_transfer_pb2.TransferStatus(success=True, message="Ricezione completata!")

    # ==============================================================================
    # 2.2 ORCHESTRATORE LOCALE (Buildah + Kube)
    # ==============================================================================
    def rebuild_and_deploy(self, tar_path):
        t_start_total = time.time()

        # --- CRONOMETRO 3: BUILDAH ---
        t0 = time.time()
        print("🔨 [BUILDAH] Ricostruzione immagine in corso...")
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
        print(f"⏱️  [TIMING] Compilazione Buildah completata in {t_buildah:.2f} secondi.")

        # --- CRONOMETRO 4: SWITCH K8S ---
        t0 = time.time()
        print("⚡ [K8S] Patching Deployment per eseguire lo switch...")
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
        subprocess.run(f"kubectl patch deployment space-mission --type='strategic' -p '{patch_str}'", shell=True)

        # 1. Diciamo al deployment di non aspettarsi più nessun Pod
        subprocess.run("kubectl scale deployment space-mission --replicas=0", shell=True)
        # 2. Polverizziamo istantaneamente il vecchio Pod (bypassa il blocco 'Terminating')
        subprocess.run("kubectl delete pod -l app=space-mission --force --grace-period=0", shell=True,
                       stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        # 3. Aggiorniamo le coordinate e l'immagine
        subprocess.run(f"kubectl patch deployment space-mission --type='strategic' -p '{patch_str}'", shell=True)
        # 4. Riaccendiamo i motori
        subprocess.run("kubectl scale deployment space-mission --replicas=1", shell=True)

        t_patch = time.time() - t0
        print(f"⏱️  [TIMING] Riprogrammazione K8s inviata in {t_patch:.2f} secondi.")

        # --- CRONOMETRO 5: ATTESA INTELLIGENTE POD E CONTAINER ---
        t0 = time.time()
        print("⏳ [K8S] In attesa del riavvio del Pod e risveglio container...")
        success = False

        for _ in range(40):
            try:
                # 1. Trova il nome del nuovo pod (ora siamo sicuri che il vecchio è morto)
                output = subprocess.check_output(
                    f"kubectl get pod -l app=space-mission --field-selector spec.nodeName={NODE_NAME} -o jsonpath='{{.items[0].metadata.name}}'",
                    shell=True, stderr=subprocess.DEVNULL)
                pod_name = output.decode('utf-8').strip()

                if pod_name:
                    # 2. Tenta di lanciare il comando exec. Finché il container non è fisicamente nato, questo fallirà in silenzio.
                    exec_cmd = f"kubectl exec {pod_name} -c sidecar-guardian -- touch /tmp/landed"
                    res = subprocess.run(exec_cmd, shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)

                    # Se il comando ha successo (exit code 0), significa che il container è vivo e pronto!
                    if res.returncode == 0:
                        success = True
                        break
            except Exception:
                pass

            # Aspetta 1 secondo e riprova
            time.sleep(1)

        t_wait = time.time() - t0

        if success:
            print(f"⏱️  [TIMING] Pod emerso e container risvegliato in {t_wait:.2f} secondi!")
        else:
            print(f"❌ [TIMING] Timeout! Il Pod non si è risvegliato dopo {t_wait:.2f} secondi.")

        t_total = time.time() - t_start_total
        print(f"🎉 [ORCHESTRATOR] Migrazione completata localmente in {t_total:.2f}s totali!")


# ==============================================================================
# 2.3 IL MITTENTE (gRPC Client) - Congela e spedisce
# ==============================================================================
def generate_chunks(filepath, chunk_size=1024 * 1024):
    """Spezza il file in chunk da 1MB per lo streaming"""
    with open(filepath, 'rb') as f:
        while True:
            data = f.read(chunk_size)
            if not data: break
            yield checkpoint_transfer_pb2.Chunk(content=data)


def execute_migration_sender(target_node):
    t_start_total = time.time()
    print(f"\n🚀 [MITTENTE] Esecuzione ordine! Congelamento verso {target_node}...")

    pod_name = subprocess.check_output(
        f"kubectl get pod -l app=space-mission --field-selector spec.nodeName={NODE_NAME} -o jsonpath='{{.items[0].metadata.name}}'",
        shell=True).decode('utf-8').strip()
    subprocess.run(f"kubectl exec {pod_name} -c sidecar-guardian -- touch /tmp/prepare_jump", shell=True)
    time.sleep(3)  # Tempo fisiologico per disconnettere i client

    # --- CRONOMETRO 1: CHECKPOINT ---
    t0 = time.time()
    print("📸 [MITTENTE] Richiesta Snapshot RAM a Kubelet...")
    proxy_proc = subprocess.Popen(["kubectl", "proxy", "--port=8001"], stdout=subprocess.DEVNULL)
    time.sleep(2)  # Attesa start proxy

    api_url = f"http://127.0.0.1:8001/api/v1/nodes/{NODE_NAME}/proxy/checkpoint/default/{pod_name}/sidecar-guardian"
    response = requests.post(api_url)
    proxy_proc.terminate()

    if "items" not in response.json():
        print(f"❌ [MITTENTE] Errore API: {response.text}")
        return

    checkpoint_path = response.json()["items"][0]
    t_checkpoint = time.time() - t0
    print(f"⏱️  [TIMING] Checkpoint generato in {t_checkpoint:.2f} secondi.")
    print(f"📦 [MITTENTE] Path: {checkpoint_path}")

    target_ip = subprocess.check_output(
        f"kubectl get pod -l app=space-node-agent --field-selector spec.nodeName={target_node} -o jsonpath='{{.items[0].status.podIP}}'",
        shell=True).decode('utf-8').strip()

    # --- CRONOMETRO 2: TRASFERIMENTO GRPC ---
    t0 = time.time()
    print(f"📡 [MITTENTE] Apertura streaming gRPC verso {target_ip}:{GRPC_PORT}...")
    channel = grpc.insecure_channel(f"{target_ip}:{GRPC_PORT}")
    stub = checkpoint_transfer_pb2_grpc.FileTransferStub(channel)

    status = stub.StreamCheckpoint(generate_chunks(checkpoint_path))
    t_transfer = time.time() - t0
    print(f"⏱️  [TIMING] Trasferimento gRPC 50MB completato in {t_transfer:.2f} secondi.")

    t_total = time.time() - t_start_total
    print(f"🏁 [MITTENTE] Operazioni di invio concluse in {t_total:.2f}s totali!")


# ==============================================================================
# IL SISTEMA NERVOSO (Redis Listener)
# ==============================================================================
def redis_listener():
    channel = f"commands/{NODE_NAME}"
    print(f"📻 [REDIS] Avvio fase di aggancio al canale: {channel}...")

    while True:
        try:
            # Tenta la connessione
            r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True, socket_connect_timeout=3)
            r.ping()  # Testa se il server è veramente pronto

            pubsub = r.pubsub()
            pubsub.subscribe(channel)
            print(f"✅ [REDIS] Connesso al DB Centrale! In ascolto di comandi...")

            # Loop bloccante che intercetta i messaggi
            for message in pubsub.listen():
                if message['type'] == 'message':
                    data = json.loads(message['data'])
                    if data.get('action') == 'MIGRATE':
                        target = data.get('target_node')
                        # Quando arriva un ordine, innesca il Mittente in un thread
                        threading.Thread(target=execute_migration_sender, args=(target,)).start()

        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            print(f"⚠️ [REDIS] Segnale da {REDIS_HOST} assente. Riprovo tra 3 secondi...")
            time.sleep(3)
        except Exception as e:
            print(f"❌ [REDIS] Errore imprevisto nel ricevitore: {e}")
            time.sleep(3)


# ==============================================================================
# INIZIALIZZATORE
# ==============================================================================
if __name__ == '__main__':
    # 1. Accende il Ricevitore gRPC
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    checkpoint_transfer_pb2_grpc.add_FileTransferServicer_to_server(FileTransferServicer(), server)
    server.add_insecure_port(f'[::]:{GRPC_PORT}')
    server.start()
    print(f"🛡️  [AGENT] gRPC Server attivo sulla porta {GRPC_PORT}.")

    # 2. Accende l'ascolto su Redis (Bloccante)
    try:
        redis_listener()
    except KeyboardInterrupt:
        server.stop(0)