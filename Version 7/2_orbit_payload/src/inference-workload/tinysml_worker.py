"""
SPACE CLOUD V7.1 - PHOENIX WORKER (Orbital Fusion Engine)
ROLE: Riceve 40 canali (1 giorno), chiede lo storico al Guardian, 
      ricuce i 5 giorni e ottiene il tensore a 120 canali per ResNet-18.
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
CHANNELS_PER_DAY = 40  # 20 Dinamici + 20 Statici/LandCover

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

def assemble_120_channels(all_days):
    """
    all_days è una lista di 5 array (ognuno di shape 40, 64, 64).
    Dobbiamo ricreare il super-tensore a 120 canali:
    - 5 * 20 canali dinamici = 100
    - 1 * 20 canali statici (solo dell'ultimo giorno) = 20
    Totale: 120 canali.
    """
    # 1. Estraiamo i 20 canali dinamici da TUTTI e 5 i giorni
    dynamic_parts = [day[0:20, :, :] for day in all_days]
    
    # 2. Estraiamo i 20 canali statici SOLO dal giorno corrente (l'ultimo)
    static_part = all_days[-1][20:40, :, :]
    
    # 3. Cucitura Finale
    final_tensor = np.concatenate(dynamic_parts + [static_part], axis=0)
    return final_tensor.reshape(1, 120, GRID_H, GRID_W)

def process_frame(frame_array):
    if session is None: return

    # 1. Recupero memoria di stato dal Guardian Sidecar
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

    # Assicuriamoci di avere i 4 giorni precedenti in RAM
    if len(history_tensors) > 4: history_tensors = history_tensors[-4:]
    while len(history_tensors) < 4:
        history_tensors.insert(0, np.zeros((CHANNELS_PER_DAY, GRID_H, GRID_W), dtype=np.float32))

    # Aggiungiamo il giorno appena arrivato
    history_tensors.append(frame_array)

    # 2. Esegui la fusione a 120 canali a bordo!
    input_tensor = assemble_120_channels(history_tensors)

    # 3. Inferenza ONNX
    input_name = session.get_inputs()[0].name
    output_name = session.get_outputs()[0].name
    logits = session.run([output_name], {input_name: input_tensor})[0]
    pred_mask = (logits > 0).astype(int)[0, 0]

    # Il canale 0 del nostro array da 40 canali è la maschera fuochi reali di oggi
    prev_fire_mask = (frame_array[0, :, :] > 0).astype(int)
    fire_pixel_count = int(np.sum(pred_mask))
    com = compute_center_of_mass(pred_mask)
    
    # 4. Invia il nuovo frame e le metriche al Guardian per salvare lo stato
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
        print(f"✅ WORKER: Elaborato Giorno {sample_count+1}. Fuoco rilevato: {fire_pixel_count} pixel.", flush=True)
    except Exception as e:
        print(f"⚠️ WORKER: Errore push Guardian: {e}", flush=True)

def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", UDP_PORT))
    print(f"📡 WORKER: UDP Telemetry Receiver attivo sulla porta {UDP_PORT}", flush=True)

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
                print(f"⚠️ WORKER: Decodifica fallita: {e}", flush=True)

        now = time.time()
        if now - last_cleanup > 10.0:
            sorted_keys = sorted(frame_buffers.keys())
            if len(sorted_keys) > 5:
                for k in sorted_keys[:-5]: del frame_buffers[k]
            last_cleanup = now

if __name__ == "__main__":
    main()