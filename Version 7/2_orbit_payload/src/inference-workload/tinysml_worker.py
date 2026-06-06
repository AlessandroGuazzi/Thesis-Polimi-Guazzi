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
GRID_W = 128
GRID_H = 128
CHANNELS_PER_DAY = 7    # 7 features per day (post-preprocessing, vegetation-only)
TOTAL_CHANNELS = 35     # 7 features × 5 days (assembled from Guardian FIFO + current frame)

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
        return {"x": 64.0, "y": 64.0}
    return {"x": float(fire_coords[:, 1].mean()), "y": float(fire_coords[:, 0].mean())}

def assemble_35_channels(history_tensors, current_frame):
    """Concatenate 4 past frames + current frame along channel axis.
    All 7 features are dynamic, so simple concat replicates training flattening.
    Input:  history_tensors = list of 4 arrays, each (7, 64, 64)
            current_frame   = (7, 64, 64)
    Output: (1, 35, 64, 64) ready for ONNX"""
    all_days = history_tensors + [current_frame]
    stacked = np.concatenate(all_days, axis=0)  # (35, 64, 64)
    return stacked.reshape(1, TOTAL_CHANNELS, GRID_H, GRID_W)


def process_frame(frame_array, fire_id, day_id):
    """Stateless conditional inference cycle:
    ① GET history from Guardian (with exponential backoff during CRIU freezes)
    ② If cache < 4 frames → WARM-UP: skip ONNX, POST frame to populate buffer
    ③ If cache = 4 frames → ACTIVE: assemble 35-ch, ONNX, POST frame + real metrics"""
    if session is None: return

    # ① Fetch temporal history with exponential backoff (CRIU freeze resilience)
    MAX_RETRIES = 3
    RETRY_BASE_DELAY = 0.5  # seconds
    state = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(f"{GUARDIAN_URL}/state", timeout=2)
            if resp.status_code == 200:
                state = resp.json()
                break
        except Exception:
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            print(f"⚠️ WORKER: Guardian unreachable (attempt {attempt+1}/{MAX_RETRIES}). Retrying in {delay:.1f}s...", flush=True)
            time.sleep(delay)

    if state is None:
        print(f"🚨 WORKER: Guardian offline after {MAX_RETRIES} retries. Dropping frame to preserve stateless integrity.", flush=True)
        return

    history_b64 = state.get("history_frames", [])
    sample_count = state.get("sample_count", 0)

    # Extract basic input metrics (available in both phases)
    prev_fire_mask = (frame_array[6, :, :] > 0).astype(int)
    input_fire_px = int(np.sum(prev_fire_mask))

    # Encode current frame for Guardian storage
    current_b64 = base64.b64encode(frame_array.tobytes()).decode('ascii')

    # =========================================================================
    # PHASE GATE: Warm-Up vs Active Inference
    # =========================================================================
    if len(history_b64) < 4:
        # ② WARM-UP PHASE (Days 1–4): Populate Guardian cache, skip ONNX
        print(f"⏳ WORKER [Incendio {fire_id+1} | Giorno {day_id+1}] | Warm-Up ({len(history_b64)+1}/4 cache) | Input: {input_fire_px} px", flush=True)
        payload = {
            "new_frame": current_b64,
            "metrics": {
                "prev_fire_mask": prev_fire_mask.tolist(),
                "predicted_fire_mask": [],
                "predicted_probability_mask": [],
                "center_of_mass": {"x": 64.0, "y": 64.0},
                "fire_pixel_count": 0,
                "sample_count": sample_count + 1,
                "fire_id": fire_id + 1,
                "day_id": day_id + 1,
                "input_fire_px": input_fire_px,
                "ai_confidence": 0.0,
                "tracking_iou": 0.0
            }
        }
    else:
        # ③ ACTIVE INFERENCE PHASE (Day 5+): Full 35-channel ONNX execution
        history_tensors = []
        for h in history_b64[-4:]:
            arr = np.frombuffer(base64.b64decode(h), dtype=np.float32).reshape(CHANNELS_PER_DAY, GRID_H, GRID_W)
            history_tensors.append(arr)

        input_tensor = assemble_35_channels(history_tensors, frame_array)

        input_name = session.get_inputs()[0].name
        output_name = session.get_outputs()[0].name
        logits = session.run([output_name], {input_name: input_tensor})[0]

        # Sigmoid: convert raw logits to continuous probabilities [0.0, 1.0]
        prob_mask = 1.0 / (1.0 + np.exp(-logits.astype(np.float64)))
        prob_mask = prob_mask.astype(np.float32)[0, 0]  # (128, 128) float32

        # Binary mask at fixed threshold 0.5 for debug logging & CoM
        DEBUG_THRESHOLD = 0.5
        pred_mask = (prob_mask >= DEBUG_THRESHOLD).astype(int)
        max_prob = float(np.max(prob_mask))
        fire_pixel_count = int(np.sum(pred_mask))
        com = compute_center_of_mass(pred_mask)

        # Tracking IoU at debug threshold (console only)
        intersection = int(np.sum((prev_fire_mask == 1) & (pred_mask == 1)))
        union = int(np.sum((prev_fire_mask == 1) | (pred_mask == 1)))
        tracking_iou = round(intersection / union, 4) if union > 0 else 0.0

        print(f"✅ WORKER [Incendio {fire_id+1} | Giorno {day_id+1}] | Input: {input_fire_px} px -> Previsto: {fire_pixel_count} px (MaxProb: {max_prob:.4f}, IoU@0.5: {tracking_iou:.2f})", flush=True)
        payload = {
            "new_frame": current_b64,
            "metrics": {
                "prev_fire_mask": prev_fire_mask.tolist(),
                "predicted_fire_mask": pred_mask.tolist(),
                "predicted_probability_mask": prob_mask.tolist(),
                "center_of_mass": com,
                "fire_pixel_count": fire_pixel_count,
                "sample_count": sample_count + 1,
                "fire_id": fire_id + 1,
                "day_id": day_id + 1,
                "input_fire_px": input_fire_px,
                "ai_confidence": round(max_prob, 4),
                "tracking_iou": tracking_iou
            }
        }

    try:
        requests.post(f"{GUARDIAN_URL}/state", json=payload, timeout=2)
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
                
                EXPECTED_SIZE = CHANNELS_PER_DAY * GRID_H * GRID_W  # 7 * 64 * 64 = 28,672
                if frame_array.size != EXPECTED_SIZE:
                    continue
                    
                frame_array = frame_array.reshape(CHANNELS_PER_DAY, GRID_H, GRID_W)  # (7, 64, 64)
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