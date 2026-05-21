"""
SPACE CLOUD V7.1 - PHOENIX WORKER (Orbital Fusion Engine)
ROLE: Receives 1 day, pulls history from Guardian, builds the 120-channel tensor.
"""
import socket
import struct
import zlib
import base64
import time
import os
import requests
import numpy as np
import onnxruntime as ort

GUARDIAN_URL = os.getenv("STATE_ENDPOINT", "http://localhost:80")
UDP_PORT = 5005
GRID_W = 64
GRID_H = 64
CHANNELS_PER_DAY = 23

print("🚀 WORKER: Initializing ONNX Runtime Session...", flush=True)
opts = ort.SessionOptions()
opts.inter_op_num_threads = 1
opts.intra_op_num_threads = 1

try:
    session = ort.InferenceSession('wsts_model.onnx', sess_options=opts, providers=['CPUExecutionProvider'])
    print("✅ WORKER: ONNX Session ready.", flush=True)
except Exception as e:
    print(f"⚠️ WORKER: Model loading error: {e}")
    session = None

def compute_center_of_mass(pred_2d):
    fire_coords = np.argwhere(pred_2d == 1)
    if len(fire_coords) == 0:
        return {"x": 32.0, "y": 32.0}
    return {"x": float(fire_coords[:, 1].mean()), "y": float(fire_coords[:, 0].mean())}

def assemble_120_channels(history_list, current_frame):
    """
    Fonde i 5 giorni applicando la deduplicazione in orbita per ottenere 120 canali.
    Struttura WSTS a 23 canali:
      - [0:20] -> 20 Canali Dinamici (l'indice 19 è la Land Cover categorica)
      - [20:23] -> 3 Canali Statici (Topografia)
    """
    all_days = history_list + [current_frame] # Garantisce T=5 frame totali
    
    # 1. Estraiamo i 20 canali dinamici per tutti e 5 i giorni (5 * 20 = 100 canali)
    dynamic_channels = [day[0:20] for day in all_days]
    tensor_dynamic = np.concatenate(dynamic_channels, axis=0) # Shape: (100, 64, 64)
    
    # 2. Prendiamo i 3 canali di topografia fissa SOLO dall'ultimo giorno (3 canali)
    tensor_topo = all_days[-1][20:23] # Shape: (3, 64, 64)
    
    # 3. Prendiamo lo strato Land Cover (canale 19) dell'ultimo giorno ed espandiamo One-Hot (17 canali)
    land_cover = all_days[-1][19].astype(np.int32)
    land_cover_bounded = np.clip(land_cover, 0, 16)
    one_hot = np.eye(17)[land_cover_bounded].transpose(2, 0, 1).astype(np.float32) # Shape: (17, 64, 64)
    
    # 4. Fusione Finale: 100 + 3 + 17 = 120 Canali!
    final_input = np.concatenate([tensor_dynamic, tensor_topo, one_hot], axis=0)
    return final_input.reshape(1, 120, GRID_H, GRID_W)

def process_frame(frame_array):
    if session is None: return

    # 1. Recupera la cronologia dei frame precedentemente accumulati dal Guardian
    try:
        resp = requests.get(f"{GUARDIAN_URL}/state", timeout=2)
        state = resp.json() if resp.status_code == 200 else {}
    except:
        state = {}

    history_b64 = state.get("history_frames", [])
    sample_count = state.get("sample_count", 0)

    history_tensors = []
    for h in history_b64:
        arr = np.frombuffer(base64.b64decode(h), dtype=np.float32).reshape(CHANNELS_PER_DAY, GRID_H, GRID_W)
        history_tensors.append(arr)

    # Mantieni solo i passati T-1 (4) giorni
    if len(history_tensors) > 4: history_tensors = history_tensors[-4:]
    while len(history_tensors) < 4:
        history_tensors.insert(0, np.zeros((CHANNELS_PER_DAY, GRID_H, GRID_W), dtype=np.float32))

    # 2. Esegui la fusione a 120 canali a bordo
    input_tensor = assemble_120_channels(history_tensors, frame_array)

    # 3. Inferenza ONNX
    input_name = session.get_inputs()[0].name
    output_name = session.get_outputs()[0].name
    logits = session.run([output_name], {input_name: input_tensor})[0]
    pred_mask = (logits > 0).astype(int)[0, 0]

    # Mappa incendi ieri (canale 0 del giorno corrente) per la dashboard
    prev_fire_mask = (frame_array[0, :, :] > 0).astype(int)

    fire_pixel_count = int(np.sum(pred_mask))
    com = compute_center_of_mass(pred_mask)
    
    # 4. Spedisci il nuovo giorno ed i calcoli al Guardian per salvarli in RAM
    current_b64 = base64.b64encode(frame_array.tobytes()).decode('ascii')
    payload = {
        "new_frame": current_b64,
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
        print(f"✅ WORKER: Elaborato Giorno {sample_count+1}. Fuoco rilevato: {fire_pixel_count} pixel.")
    except Exception as e:
        print(f"⚠️ WORKER: Errore push Guardian: {e}")

def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", UDP_PORT))
    print(f"📡 WORKER: UDP Telemetry Receiver attivo sulla porta {UDP_PORT}")

    frame_buffers = {}
    last_cleanup = time.time()

    while True:
        data, addr = sock.recvfrom(65535)
        if len(data) < 6: continue
        frame_id, total_chunks, chunk_idx = struct.unpack("!IBB", data[:6])
        chunk_data = data[6:]

        if frame_id not in frame_buffers: frame_buffers[frame_id] = [None] * total_chunks
        if chunk_idx < total_chunks: frame_buffers[frame_id][chunk_idx] = chunk_data

        if all(c is not None for c in frame_buffers[frame_id]):
            blob = b''.join(frame_buffers[frame_id])
            del frame_buffers[frame_id]
            try:
                decompressed = zlib.decompress(blob)
                frame_array = np.frombuffer(decompressed, dtype='>f4').astype(np.float32).reshape(CHANNELS_PER_DAY, GRID_H, GRID_W)
                process_frame(frame_array)
            except Exception as e:
                print(f"⚠️ WORKER: Decodifica fallita: {e}")

        now = time.time()
        if now - last_cleanup > 10.0:
            sorted_keys = sorted(frame_buffers.keys())
            if len(sorted_keys) > 5:
                for k in sorted_keys[:-5]: del frame_buffers[k]
            last_cleanup = now

if __name__ == "__main__":
    main()