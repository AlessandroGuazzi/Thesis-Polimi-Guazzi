"""
SPACE CLOUD V7.2 - WSTS DATA STREAMER (LEO Orbital Interleave Edition)
=====================================================
ROLE: Simulates realistic LEO satellite data acquisition by interleaving
daily observations across all active fire missions. Epicenters are
pre-computed, then data is streamed orbit-by-orbit (day-by-day) with
each orbit visiting every fire location sequentially.
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
STREAM_INTERVAL = float(os.getenv("STREAM_INTERVAL", "1.5"))
WARMUP_INTERVAL = 0.15
CHUNK_SIZE = 60_000

def get_minikube_ip() -> str:
    try:
        return subprocess.check_output(["minikube", "ip", "-p", "minikube"], stderr=subprocess.DEVNULL).decode("utf-8").strip()
    except:
        return "192.168.49.2"

def encode_frame(frame: np.ndarray) -> bytes:
    return zlib.compress(frame.astype('>f4').tobytes())

def main():
    print("🚀 STREAMER: Booting WSTS Uplink (LEO Orbital Interleave Edition)...")
    
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
    # PRE-COMPUTATION PHASE: Compute epicenters for ALL fires (Lightweight)
    # =========================================================================
    print(f"\n🎯 Pre-computing epicenters for all {len(fires)} fires (Lightweight scan)...")
    fire_epicenters = []  # List of (fixed_sy, fixed_sx) per fire

    for fire_idx, (fire_name, indices) in enumerate(fires):
        best_cy, best_cx = 144, 112
        max_fire_px = 0
        last_fire_mask = None

        # Robust epicenter lock: Scan all days to find the max fire spread
        for i in indices:
            x_test, _ = dataset[i]
            fire_mask = x_test.numpy()[0, -1, :, :]
            last_fire_mask = fire_mask
            
            fire_coords = np.argwhere(fire_mask > 0)
            if len(fire_coords) > max_fire_px:
                max_fire_px = len(fire_coords)
                best_cy = int(fire_coords[:, 0].mean())
                best_cx = int(fire_coords[:, 1].mean())

        fixed_sy = max(0, min(last_fire_mask.shape[0] - 128, best_cy - 64))
        fixed_sx = max(0, min(last_fire_mask.shape[1] - 128, best_cx - 64))
        fire_epicenters.append((fixed_sy, fixed_sx))
        print(f"   ✅ Fire {fire_idx+1}/{len(fires)} [{fire_name}]: epicenter locked (max {max_fire_px} px)")

    print(f"🎯 All epicenters securely locked. Commencing Orbital Simulation...\n")
    # =========================================================================

    # Determine the maximum number of days across all fires
    max_days = max(len(indices) for _, indices in fires)

    target_ip = get_minikube_ip()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    while True:
        # =====================================================================
        # INTERLEAVED ORBITAL SIMULATION
        # Outer loop = day (orbital pass), Inner loop = fire (regions visited)
        # =====================================================================
        for day_idx in range(max_days):
            is_warmup = day_idx < 4
            current_interval = WARMUP_INTERVAL if is_warmup else STREAM_INTERVAL
            phase_name = "WARM-UP (FAST FORWARD)" if is_warmup else "ACTIVE INFERENCE"

            print(f"\n{'='*60}")
            print(f"🛰️  ORBITAL PASS {day_idx+1}/{max_days} — [{phase_name}] Scanning all active missions")
            print(f"{'='*60}")

            for fire_idx, (fire_name, indices) in enumerate(fires):
                # Skip this fire if it has fewer days than the current orbit
                if day_idx >= len(indices):
                    continue

                dataset_idx = indices[day_idx]
                fixed_sy, fixed_sx = fire_epicenters[fire_idx]

                x_tensor, _ = dataset[dataset_idx]
                full_array = x_tensor.numpy().astype(np.float32)

                # full_array shape: (1, 7, H, W) — squeeze time dim, apply spatial crop
                frame_array = full_array[0, :, fixed_sy:fixed_sy+128, fixed_sx:fixed_sx+128]

                blob = encode_frame(frame_array)

                # UDP header: Fire_ID and Day_ID (Formato: !IIBB)
                total_chunks = (len(blob) + CHUNK_SIZE - 1) // CHUNK_SIZE
                for chunk_idx in range(total_chunks):
                    chunk_data = blob[chunk_idx * CHUNK_SIZE : (chunk_idx + 1) * CHUNK_SIZE]
                    header = struct.pack("!IIBB", fire_idx, day_idx, total_chunks, chunk_idx)
                    try:
                        sock.sendto(header + chunk_data, (target_ip, WORKER_UDP_NODEPORT))
                    except:
                        pass

                print(f"📤 ORBIT {day_idx+1} | Fire {fire_idx+1}/{len(fires)} [{fire_name}]: Day {day_idx+1}/{len(indices)} → {len(blob)//1024}KB, {total_chunks} chunk(s)")
                time.sleep(current_interval)

        print(f"\n🔄 All orbital passes completed. Restarting simulation cycle...")
        time.sleep(10)

if __name__ == "__main__":
    main()