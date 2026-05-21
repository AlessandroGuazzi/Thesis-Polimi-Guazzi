"""
SPACE CLOUD V7.1 - PHOENIX WORKER (Mission Calibrated Edition)
=====================================================
ROLE: Inferenza orbitale con Threshold Moving per contrastare 
lo sbilanciamento di classe delle 5 epoche di training.
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
CHANNELS_PER_DAY = 40  

# =====================================================================
# MISSION CALIBRATION (Threshold Moving)
# Abbassiamo la soglia per far emergere i pixel predetti dalla rete.
# =====================================================================
CALIBRATION_THRESHOLD = 0.0

print("🚀 WORKER: Initializing ONNX Runtime Session...", flush=True)
opts = ort.SessionOptions()
opts.inter_op_num_threads = 1
opts.intra_op_num_threads = 1

try:
    session = ort.InferenceSession('wsts_model.onnx', sess_options=opts, providers=['CPUExecutionProvider'])
    print("✅ WORKER: ONNX Session ready.", flush=True)
except Exception as e:
    session = None

def compute_center_of_mass(pred_2d):
    fire_coords = np.argwhere(pred_2d == 1)
    if len(fire_coords) == 0:
        return {"x": 32.0, "y": 32.0}
    return {"x": float(fire_coords[:, 1].mean()), "y": float(fire_coords[:, 0].mean())}

def assemble_120_channels(all_days):
    dynamic_idx = list(range(12)) + [15] + list(range(33, 40))
    dynamic_parts = [day[dynamic_idx, :, :] for day in all_days[:-1]]
    last_day = all_days[-1]
    final_tensor = np.concatenate(dynamic_parts + [last_day], axis=0)
    return final_tensor.reshape(1, 120, GRID_H, GRID_W)

current_fire_id = None

def process_frame(frame_array, fire_id, day_id):
    global current_fire_id
    if session is None: return

    if current_fire_id != fire_id:
        print(f"\n🔥 WORKER: Rilevato Nuovo Incendio (ID: {fire_id+1})! Inizializzazione nuova missione...", flush=True)
        current_fire_id = fire_id

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

    if len(history_tensors) > 4: 
        history_tensors = history_tensors[-4:]

    # PROTOCOLLO DI ISOLAMENTO MATEMATICO
    valid_history_length = min(day_id, 4)
    if valid_history_length == 0:
        history_tensors = []
    else:
        history_tensors = history_tensors[-valid_history_length:]

    while len(history_tensors) < 4:
        if len(history_tensors) > 0:
            history_tensors.insert(0, np.copy(history_tensors[0]))
        else:
            history_tensors.insert(0, np.copy(frame_array))

    history_tensors.append(frame_array)
    input_tensor = assemble_120_channels(history_tensors)

    input_name = session.get_inputs()[0].name
    output_name = session.get_outputs()[0].name
    logits = session.run([output_name], {input_name: input_tensor})[0]
    
    # ESTRAZIONE PIXEL CON LA NUOVA SOGLIA CALIBRATA
    pred_mask = (logits > CALIBRATION_THRESHOLD).astype(int)[0, 0]
    max_logit = float(np.max(logits))

    prev_fire_mask = (frame_array[39, :, :] > 0).astype(int)
    
    input_fire_px = int(np.sum(prev_fire_mask))
    fire_pixel_count = int(np.sum(pred_mask))
    com = compute_center_of_mass(pred_mask)
    
    current_b64 = base64.b64encode(frame_array.tobytes()).decode('ascii')
    payload = {
        "new_frame": current_b64,
        "metrics": {
            "prev_fire_mask": prev_fire_mask.tolist(),
            "predicted_fire_mask": pred_mask.tolist(),
            "center_of_mass": com,
            "fire_pixel_count": fire_pixel_count,
            "sample_count": sample_count + 1,
            # ====== NUOVI DATI PER LA DASHBOARD ======
            "fire_id": fire_id + 1,
            "day_id": day_id + 1,
            "input_fire_px": input_fire_px,
            "ai_confidence": round(max_logit, 2)
            # =========================================
        }
    }
    try:
        requests.post(f"{GUARDIAN_URL}/state", json=payload, timeout=2)
        print(f"✅ WORKER [Incendio {fire_id+1} | Giorno {day_id+1}] | Input: {input_fire_px} px -> Previsto: {fire_pixel_count} px (Conf: {max_logit:.2f})", flush=True)
    except Exception as e:
        pass

def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", UDP_PORT))
    print(f"📡 WORKER: UDP Telemetry Receiver attivo sulla porta {UDP_PORT}", flush=True)

    frame_buffers = {}
    last_cleanup = time.time()

    while True:
        data, addr = sock.recvfrom(65535)
        
        if len(data) < 10: continue
        fire_id, day_id, total_chunks, chunk_idx = struct.unpack("!IIBB", data[:10])
        chunk_data = data[10:]

        frame_id = f"{fire_id}_{day_id}"

        if frame_id not in frame_buffers: frame_buffers[frame_id] = [None] * total_chunks
        if chunk_idx < total_chunks: frame_buffers[frame_id][chunk_idx] = chunk_data

        if all(c is not None for c in frame_buffers[frame_id]):
            blob = b''.join(frame_buffers[frame_id])
            del frame_buffers[frame_id]
            try:
                decompressed = zlib.decompress(blob)
                frame_array = np.frombuffer(decompressed, dtype='>f4').astype(np.float32)
                
                EXPECTED_SIZE = CHANNELS_PER_DAY * GRID_H * GRID_W
                if frame_array.size != EXPECTED_SIZE:
                    continue
                    
                frame_array = frame_array.reshape(CHANNELS_PER_DAY, GRID_H, GRID_W)
                process_frame(frame_array, fire_id, day_id)
            except Exception as e:
                pass

        now = time.time()
        if now - last_cleanup > 10.0:
            sorted_keys = sorted(frame_buffers.keys())
            if len(sorted_keys) > 5:
                for k in sorted_keys[:-5]: del frame_buffers[k]
            last_cleanup = now

if __name__ == "__main__":
    main()