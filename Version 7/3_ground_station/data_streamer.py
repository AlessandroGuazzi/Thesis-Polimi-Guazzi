"""
SPACE CLOUD V7.1 - WSTS DATA STREAMER (Multi-Mission Fleet Edition)
=====================================================
ROLE: Raggruppa i dataset per incendio, trasmette le sequenze in modo 
isolato e inietta gli ID (Fire e Day) nel pacchetto UDP.
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
STREAM_INTERVAL = float(os.getenv("STREAM_INTERVAL", "3.0"))
CHUNK_SIZE = 60_000

def get_minikube_ip() -> str:
    try:
        return subprocess.check_output(["minikube", "ip", "-p", "minikube"], stderr=subprocess.DEVNULL).decode("utf-8").strip()
    except:
        return "192.168.49.2"

def encode_frame(frame: np.ndarray) -> bytes:
    return zlib.compress(frame.astype('>f4').tobytes())

def main():
    print("🚀 STREAMER: Booting WSTS Uplink (Multi-Mission Fleet Edition)...")
    
    year_dir = os.path.join(WSTS_EVAL_DIR, "2021")
    if not os.path.exists(year_dir):
        sys.exit(1)
        
    dataset = FireSpreadDataset(
        data_dir=WSTS_EVAL_DIR,
        included_fire_years=[2021],
        n_leading_observations=1,         
        crop_side_length=128,
        load_from_hdf5=True,
        is_train=False,
        remove_duplicate_features=False,
        stats_years=[2018, 2019],
        features_to_keep=[0, 1, 2, 3, 4, 38, 39]
    )

    dataset_size = len(dataset)
    if dataset_size == 0: sys.exit(1)
        
    # =========================================================================
    # RAGGRUPPAMENTO MULTI-MISSIONE
    # =========================================================================
    fires = []
    current_fire_name = None
    current_indices = []
    
    for i in range(dataset_size):
        _, fire_name, _ = dataset.find_image_index_from_dataset_index(i)
        if fire_name != current_fire_name:
            if current_fire_name is not None:
                fires.append((current_fire_name, current_indices))
            current_fire_name = fire_name
            current_indices = [i]
        else:
            current_indices.append(i)
    if current_indices:
        fires.append((current_fire_name, current_indices))
        
    print(f"🌲 Analisi Globale completata: Trovati {len(fires)} incendi distinti in evaluation_data.")
    # =========================================================================

    target_ip = get_minikube_ip()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    while True:
        # Iteriamo su tutti e 12 gli incendi in modo sequenziale
        for fire_idx, (fire_name, indices) in enumerate(fires):
            print(f"\n=======================================================")
            print(f"🔥 INIZIO MISSIONE: Incendio {fire_idx+1}/{len(fires)} [{fire_name}]")
            print(f"=======================================================")
            
            # Radar: Troviamo l'epicentro per QUESTO specifico incendio
            best_cy, best_cx = 144, 112 
            max_fire_px = 0
            
            for i in indices:
                x_test, _ = dataset[i]
                fire_mask = x_test.numpy()[0, -1, :, :] 
                fire_coords = np.argwhere(fire_mask > 0)
                if len(fire_coords) > max_fire_px:
                    max_fire_px = len(fire_coords)
                    best_cy = int(fire_coords[:, 0].mean())
                    best_cx = int(fire_coords[:, 1].mean())
                    
            fixed_sy = max(0, min(fire_mask.shape[0] - 128, best_cy - 64))
            fixed_sx = max(0, min(fire_mask.shape[1] - 128, best_cx - 64))
            print(f"🎯 Ottica bloccata sull'epicentro (Max Estensione: {max_fire_px} px).")
            print(f"📡 Inizio trasmissione di {len(indices)} giorni sequenziali...\n")
            
            for day_idx, dataset_idx in enumerate(indices):
                x_tensor, _ = dataset[dataset_idx]
                full_array = x_tensor.numpy().astype(np.float32)

                # full_array shape: (1, 7, H, W) — squeeze time dim, apply spatial crop
                frame_array = full_array[0, :, fixed_sy:fixed_sy+128, fixed_sx:fixed_sx+128]

                blob = encode_frame(frame_array)
                
                # UPGRADE UDP: Aggiungiamo Fire_ID e Day_ID nell'header (Formato: !IIBB)
                total_chunks = (len(blob) + CHUNK_SIZE - 1) // CHUNK_SIZE
                for chunk_idx in range(total_chunks):
                    chunk_data = blob[chunk_idx * CHUNK_SIZE : (chunk_idx + 1) * CHUNK_SIZE]
                    header = struct.pack("!IIBB", fire_idx, day_idx, total_chunks, chunk_idx)
                    try:
                        sock.sendto(header + chunk_data, (target_ip, WORKER_UDP_NODEPORT))
                    except:
                        pass
                
                # Il Log formattato esattamente come da tua richiesta
                print(f"📤 STREAMER [Incendio {fire_idx+1}/{len(fires)}]: Inviato Giorno {day_idx+1} → {len(blob)//1024}KB, {total_chunks} chunk(s)")
                time.sleep(STREAM_INTERVAL)
                
            print(f"\n🔄 Incendio terminato. Pausa di 5 secondi e riavvio simulazione...")
            time.sleep(10)

if __name__ == "__main__":
    main()