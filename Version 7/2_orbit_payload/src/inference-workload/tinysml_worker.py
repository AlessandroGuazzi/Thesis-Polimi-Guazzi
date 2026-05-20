"""
SPACE CLOUD V7 - ULTRA-STATELESS PHOENIX WORKER
Strict Edge ML constraints applied:
- Input: 120 Channels (Early Fusion completed on Earth)
- NO PyTorch, only onnxruntime and numpy
- Pure stateless execution (Zero memory retention)
"""

import socket
import struct
import zlib
import time
import os
import requests
import numpy as np
import onnxruntime as ort

GUARDIAN_URL = os.getenv("STATE_ENDPOINT", "http://localhost:80")
UDP_PORT = 5005
GRID_W = 64
GRID_H = 64
CHANNELS_INPUT = 120  # L'input è la matrice Early Fusion già pronta!

print("🚀 WORKER: Initializing ONNX Runtime Session...", flush=True)
opts = ort.SessionOptions()
opts.inter_op_num_threads = 1
opts.intra_op_num_threads = 1

try:
    session = ort.InferenceSession('wsts_model.onnx', sess_options=opts, providers=['CPUExecutionProvider'])
    print("✅ WORKER: ONNX Session ready.", flush=True)
except Exception as e:
    print(f"⚠️ WORKER: Could not load wsts_model.onnx. Error: {e}")
    session = None

def compute_center_of_mass(pred_2d):
    fire_coords = np.argwhere(pred_2d == 1)
    if len(fire_coords) == 0:
        return {"x": float(GRID_W / 2.0), "y": float(GRID_H / 2.0)}
    return {"x": float(fire_coords[:, 1].mean()), "y": float(fire_coords[:, 0].mean())}

def process_frame(frame_array):
    if session is None:
        return

    # 1. Ottieni il conteggio attuale dal Guardian (per i log)
    try:
        resp = requests.get(f"{GUARDIAN_URL}/state", timeout=2)
        sample_count = resp.json().get("sample_count", 0) if resp.status_code == 200 else 0
    except:
        sample_count = 0

    # 2. Reshape Diretto: (1, 120, 64, 64) - Nessun assemblaggio storico richiesto!
    input_tensor = frame_array.reshape(1, CHANNELS_INPUT, GRID_H, GRID_W)

    # 3. Inferenza ONNX
    input_name = session.get_inputs()[0].name
    output_name = session.get_outputs()[0].name
    
    logits = session.run([output_name], {input_name: input_tensor})[0]
    pred_mask = (logits > 0).astype(int)[0, 0] 

    # 4. Estraiamo la mappa incendi del giorno precedente (per la Dashboard)
    # L'ultimo canale (-1) del tensore a 120 canali contiene i fuochi attivi del Giorno 5
    prev_fire_mask = (frame_array[-1, :, :] > 0).astype(int)

    fire_pixel_count = int(np.sum(pred_mask))
    com = compute_center_of_mass(pred_mask)
    
    # 5. Push al Guardian (Solo Metriche, Niente Array Pesanti in RAM!)
    payload = {
        "metrics": {
            "prev_fire_mask": prev_fire_mask.tolist(),
            "predicted_fire_mask": pred_mask.tolist(),
            "center_of_mass": com,
            "fire_pixel_count": fire_pixel_count,
            "sample_count": sample_count + 1
        }
    }
    
    try:
        requests.post(f"{GUARDIAN_URL}/state", json=payload, timeout=2)
        print(f"✅ WORKER: Frame processed. Sample={sample_count+1}, Fire Pixels={fire_pixel_count}", flush=True)
    except Exception as e:
        print(f"⚠️ WORKER: Failed to push state to Guardian: {e}", flush=True)

def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", UDP_PORT))
    print(f"📡 WORKER: UDP Receiver active on port {UDP_PORT}", flush=True)

    frame_buffers = {}
    last_cleanup = time.time()

    while True:
        data, addr = sock.recvfrom(65535)
        if len(data) < 6: continue
            
        frame_id, total_chunks, chunk_idx = struct.unpack("!IBB", data[:6])
        chunk_data = data[6:]

        if frame_id not in frame_buffers:
            frame_buffers[frame_id] = [None] * total_chunks

        if chunk_idx < total_chunks:
            frame_buffers[frame_id][chunk_idx] = chunk_data

        if all(c is not None for c in frame_buffers[frame_id]):
            compressed_blob = b''.join(frame_buffers[frame_id])
            del frame_buffers[frame_id]

            try:
                decompressed = zlib.decompress(compressed_blob)
                frame_array = np.frombuffer(decompressed, dtype='>f4').astype(np.float32)
                frame_array = frame_array.reshape(CHANNELS_INPUT, GRID_H, GRID_W)
                process_frame(frame_array)
            except Exception as e:
                print(f"⚠️ WORKER: Processing failed: {e}", flush=True)

        now = time.time()
        if now - last_cleanup > 10.0:
            sorted_keys = sorted(frame_buffers.keys())
            if len(sorted_keys) > 5:
                for k in sorted_keys[:-5]: del frame_buffers[k]
            last_cleanup = now

if __name__ == "__main__":
    main()