"""
SPACE CLOUD V7 - STATELESS PHOENIX WORKER
Strict Edge ML constraints applied:
- Under 256Mi RAM
- NO PyTorch, only onnxruntime and numpy
- Stateless execution (memory held in Guardian)
- Thread limits explicitly enforced to prevent CPU throttling
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
CHANNELS_PER_FRAME = 23
T_HISTORY = 5

# =============================================================================
# 1. SINGLE INFERENCE SESSION & ENGINE PINNING
# =============================================================================
print("🚀 WORKER: Initializing ONNX Runtime Session...", flush=True)
opts = ort.SessionOptions()

# Explicitly cap thread pools to prevent Kubernetes CPU throttling & memory ballooning
opts.inter_op_num_threads = 1
opts.intra_op_num_threads = 1

try:
    # Enforce CPUExecutionProvider for standard Kubernetes edge compatibility
    session = ort.InferenceSession('wsts_model.onnx', sess_options=opts, providers=['CPUExecutionProvider'])
    print("✅ WORKER: ONNX Session ready.", flush=True)
except Exception as e:
    print(f"⚠️ WORKER: Could not load wsts_model.onnx. Ensure it is present in the container. Error: {e}")
    session = None


def compute_center_of_mass(pred_2d):
    """Computes the X/Y focal point of the fire for orbital tracking."""
    fire_coords = np.argwhere(pred_2d == 1)
    if len(fire_coords) == 0:
        return {"x": float(GRID_W / 2.0), "y": float(GRID_H / 2.0)}
    return {"x": float(fire_coords[:, 1].mean()), "y": float(fire_coords[:, 0].mean())}


def process_frame(frame_array):
    """
    Main execution pipeline for a fully assembled UDP frame.
    Stateless logic: Fetches history -> Infers -> Pushes updated state.
    """
    if session is None:
        return

    # 1. Fetch Authoritative State & History from Guardian
    try:
        resp = requests.get(f"{GUARDIAN_URL}/state", timeout=2)
        if resp.status_code == 200:
            state = resp.json()
        else:
            state = {}
    except Exception as e:
        print(f"⚠️ WORKER: Guardian unreachable. Skipping frame. {e}")
        return

    sample_count = state.get("sample_count", 0)
    history_b64 = state.get("history_frames", [])

    # 2. Reconstruct History Tensors (O(1) memory strategy)
    history_tensors = []
    for h in history_b64:
        try:
            arr = np.frombuffer(base64.b64decode(h), dtype=np.float32).reshape(CHANNELS_PER_FRAME, GRID_H, GRID_W)
            history_tensors.append(arr)
        except Exception:
            pass

    # We need T-1 (4) history frames to append our new frame and get T=5
    target_history = T_HISTORY - 1
    if len(history_tensors) > target_history:
        history_tensors = history_tensors[-target_history:]
        
    # Pad with zeros if we don't have enough history (e.g. cold start)
    while len(history_tensors) < target_history:
        history_tensors.insert(0, np.zeros((CHANNELS_PER_FRAME, GRID_H, GRID_W), dtype=np.float32))

    # Append current frame
    history_tensors.append(frame_array)

    # 3. Stack into Fixed Static Graph Shape: (1, 115, 64, 64)
    # The normalisation (mean/std) is already baked into the ONNX model, so we pass raw unscaled floats!
    input_tensor = np.concatenate(history_tensors, axis=0)
    input_tensor = input_tensor.reshape(1, CHANNELS_PER_FRAME * T_HISTORY, GRID_H, GRID_W)

    # 4. Run ONNX Inference
    input_name = session.get_inputs()[0].name
    output_name = session.get_outputs()[0].name
    
    logits = session.run([output_name], {input_name: input_tensor})[0]
    pred_mask = (logits > 0).astype(int)[0, 0] # Extract the 64x64 binary mask

    # Compute Metrics
    fire_pixel_count = int(np.sum(pred_mask))
    com = compute_center_of_mass(pred_mask)
    
    # 5. Push New State to Guardian
    # We serialize the frame as base64 to minimize HTTP JSON parsing overhead & memory
    new_frame_b64 = base64.b64encode(frame_array.tobytes()).decode('ascii')
    
    payload = {
        "new_frame": new_frame_b64,
        "metrics": {
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
    print(f"📡 WORKER: UDP Telemetry Receiver active on port {UDP_PORT}", flush=True)

    frame_buffers = {}
    last_cleanup = time.time()

    while True:
        data, addr = sock.recvfrom(65535)
        
        # Parse chunk header: !IBB = uint32 (frame_id), uint8 (total_chunks), uint8 (chunk_index)
        if len(data) < 6:
            continue
            
        frame_id, total_chunks, chunk_idx = struct.unpack("!IBB", data[:6])
        chunk_data = data[6:]

        if frame_id not in frame_buffers:
            frame_buffers[frame_id] = [None] * total_chunks

        if chunk_idx < total_chunks:
            frame_buffers[frame_id][chunk_idx] = chunk_data

        # Check if frame is fully assembled
        if all(c is not None for c in frame_buffers[frame_id]):
            compressed_blob = b''.join(frame_buffers[frame_id])
            del frame_buffers[frame_id]

            try:
                decompressed = zlib.decompress(compressed_blob)
                # Network sends big-endian floats (>f4). We parse it, then cast to native little-endian float32
                frame_array = np.frombuffer(decompressed, dtype='>f4').astype(np.float32)
                frame_array = frame_array.reshape(CHANNELS_PER_FRAME, GRID_H, GRID_W)
                
                process_frame(frame_array)
            except Exception as e:
                print(f"⚠️ WORKER: Frame decompression/processing failed: {e}", flush=True)

        # Cleanup old partial buffers to prevent memory leaks during packet loss
        now = time.time()
        if now - last_cleanup > 10.0:
            sorted_keys = sorted(frame_buffers.keys())
            if len(sorted_keys) > 5:
                for k in sorted_keys[:-5]:
                    del frame_buffers[k]
            last_cleanup = now

if __name__ == "__main__":
    main()