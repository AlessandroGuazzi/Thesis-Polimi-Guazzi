"""
SPACE CLOUD V7.1 - WSTS DATA STREAMER (Masterstroke Edition)
=====================================================
ROLE: Genera il tensore perfetto a 120 canali, ma ne "affetta" solo l'ultimo 
giorno (40 canali) per costringere il satellite a fare l'Early Fusion a bordo.
"""
import socket
import struct
import zlib
import time
import os
import sys
import subprocess
import numpy as np

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
VERSION_7_ROOT = os.path.dirname(CURRENT_DIR)
TRAINING_FACILITY_SRC = os.path.join(VERSION_7_ROOT, "1_earth_training_facility", "src")
sys.path.append(TRAINING_FACILITY_SRC)

try:
    from dataloader.FireSpreadDataset import FireSpreadDataset
    print("✅ STREAMER: Linked with official FireSpreadDataset.")
except ImportError as e:
    print(f"❌ STREAMER ERROR: {e}")
    sys.exit(1)

WSTS_EVAL_DIR = os.getenv("WSTS_EVAL_DIR", os.path.join(CURRENT_DIR, "evaluation_data"))
WORKER_UDP_NODEPORT = 32005
STREAM_INTERVAL = float(os.getenv("STREAM_INTERVAL", "1.0"))
CHUNK_SIZE = 60_000
TEST_MODE = os.getenv("TEST_MODE", "true").lower() == "true"
TEST_SLICE_SIZE = int(os.getenv("TEST_SLICE_SIZE", "20"))

def get_minikube_ip() -> str:
    try:
        return subprocess.check_output(["minikube", "ip", "-p", "minikube"], stderr=subprocess.DEVNULL).decode("utf-8").strip()
    except:
        return "192.168.49.2"

def encode_frame(frame: np.ndarray) -> bytes:
    return zlib.compress(frame.astype('>f4').tobytes())

def send_frame(sock: socket.socket, blob: bytes, target_ip: str, target_port: int, frame_id: int) -> int:
    total_chunks = (len(blob) + CHUNK_SIZE - 1) // CHUNK_SIZE
    for chunk_idx in range(total_chunks):
        chunk_data = blob[chunk_idx * CHUNK_SIZE : (chunk_idx + 1) * CHUNK_SIZE]
        header = struct.pack("!IBB", frame_id, total_chunks, chunk_idx)
        sock.sendto(header + chunk_data, (target_ip, target_port))
    return total_chunks

def main():
    print("🚀 STREAMER: Booting WSTS Uplink (Stateful 40-Channel Mode)...")
    
    dataset = FireSpreadDataset(
        data_dir=WSTS_EVAL_DIR,
        included_fire_years=[2021],
        n_leading_observations=5,         # 1. Generiamo i 5 giorni completi...
        crop_side_length=64,
        load_from_hdf5=True,
        is_train=False,
        remove_duplicate_features=True,   # 2. ...perfettamente ottimizzati a 120 canali!
        stats_years=[2018, 2019]
    )

    dataset_size = len(dataset)
    if dataset_size == 0:
        print("\n❌ ERRORE: Dataset vuoto. Controlla la cartella evaluation_data/2021.")
        sys.exit(1)
        
    print(f"📊 STREAMER: Caricamento completato. {dataset_size} frame pronti.")
    target_ip = get_minikube_ip()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    frame_id_counter = 0
    limit = min(TEST_SLICE_SIZE, dataset_size) if TEST_MODE else dataset_size

    while True:
        for idx in range(limit):
            x_tensor, _ = dataset[idx]
            full_120_array = x_tensor.numpy().astype(np.float32) # Shape: (120, 64, 64)

            # IL BISTURI MATEMATICO: Estraiamo SOLO i dati dell'ultimo giorno (Giorno T)
            # - Dinamiche del Giorno T: canali da 80 a 100 (20 canali)
            # - Statica & LandCover: canali da 100 a 120 (20 canali)
            day_t_dynamic = full_120_array[80:100, :, :]
            day_t_static = full_120_array[100:120, :, :]
            frame_array = np.concatenate([day_t_dynamic, day_t_static], axis=0) # Shape: (40, 64, 64)

            frame_id_counter = (frame_id_counter + 1) & 0xFFFF_FFFF
            blob = encode_frame(frame_array)

            try:
                n_chunks = send_frame(sock, blob, target_ip, WORKER_UDP_NODEPORT, frame_id_counter)
                print(f"📤 STREAMER: Inviato Giorno T (Index {idx}) → {len(blob)//1024}KB, {n_chunks} chunk(s)")
            except Exception as e:
                print(f"⚠️ STREAMER: Errore UDP: {e}")

            time.sleep(STREAM_INTERVAL)

if __name__ == "__main__":
    main()