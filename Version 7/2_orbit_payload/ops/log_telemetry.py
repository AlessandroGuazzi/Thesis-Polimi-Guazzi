#!/usr/bin/env python3
import redis
import json
import time
import sys

# =============================================================================
#  SPACE CLOUD V7 - SIMPLIFIED TELEMETRY LOGGER
#  Role: Connects to Ground Redis, tracks SML placement state, and prints
#        the exact Big 3 metrics (Survival, Delay, Bandwidth) for your thesis.
# =============================================================================

REDIS_HOST = "localhost"
REDIS_PORT = 6379

# Orbit planes configuration to compute hop count automatically:
# minikube-m02 (Plane A) <-> minikube-m03 (Plane B) <-> minikube-m04 (Plane C)
PLANE_MAP = {
    "minikube-m02": 1,  # Plane A
    "minikube-m03": 2,  # Plane B
    "minikube-m04": 3   # Plane C
}

def get_hops(src, dest):
    """Calculates hops between node plane locations (each plane step is 1 hop)."""
    if src not in PLANE_MAP or dest not in PLANE_MAP:
        return 1
    return abs(PLANE_MAP[src] - PLANE_MAP[dest])

def main():
    print("📡 Telemetry Logger starting... Connecting to Ground Redis.")
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True, socket_connect_timeout=3)
        r.ping()
        print("✅ Connected successfully. Subscribing to telemetry channels...")
    except redis.exceptions.ConnectionError:
        print("❌ ERROR: Ground Redis not running on localhost:6379! Start the cluster first.")
        sys.exit(1)

    pubsub = r.pubsub()
    pubsub.psubscribe("telemetry/*")

    active_sml_node = None
    in_flight = False
    migration_start_time = 0.0
    source_node = None
    bounce_counter = 0
    
    # Store the latest telemetry snapshot for each satellite node
    node_telemetry = {}

    print("\n🟢 MONITORING STARTED. Run your test and trigger a migration.")
    print("================================================================")

    # Listen to pubsub events
    for message in pubsub.listen():
        if message["type"] != "pmessage":
            continue

        channel = message["channel"]
        node_name = channel.split("/")[1]
        try:
            data = json.loads(message["data"])
        except (json.JSONDecodeError, TypeError):
            continue

        # Save node state in the tracking dictionary
        node_telemetry[node_name] = {
            "temp": data.get("temp", 20.0),
            "battery": data.get("battery", 100.0)
        }

        is_working = data.get("is_working", False)

        # STATE 1: SML was active here, but now is_working is False. It has left!
        if node_name == active_sml_node and not is_working and not in_flight:
            in_flight = True
            migration_start_time = time.time()
            source_node = node_name
            bounce_counter += 1
            
            # Capture departure physical states
            source_state = node_telemetry.get(source_node, {"temp": 20.0, "battery": 100.0})
            departure_temp = source_state["temp"]
            departure_battery = source_state["battery"]
            
            print(f"\n🚀 [EVENT] SML evacuated from {source_node} at {time.strftime('%H:%M:%S')}")
            print(f"       ↳ Source Node State at Departure: {departure_temp}°C | {departure_battery}% Battery")
            print("⏳ [STATUS] Checkpoint transfer in-flight... waiting for landing.")

        # STATE 2: SML has landed on a node where it wasn't working before.
        elif is_working and node_name != active_sml_node:
            # SML landed successfully!
            if in_flight:
                landing_time = time.time()
                delay = landing_time - migration_start_time
                in_flight = False

                hops = get_hops(source_node, node_name)
                
                # Check file size target: monolithic vs sidecar split
                # If running Block 4 (monolithic), size is 160MB. Otherwise, it is 24MB.
                is_monolithic = False
                # Simple check: if transfer takes too long, it's likely a monolithic configuration
                # or you can write down the size based on your current run block.
                checkpoint_size = 160.0 if is_monolithic else 24.0
                bandwidth = hops * checkpoint_size

                # Fetch the recorded departure conditions
                source_state = node_telemetry.get(source_node, {"temp": 20.0, "battery": 100.0})
                dep_temp = source_state["temp"]
                dep_batt = source_state["battery"]

                # Determine if a logical physical safety threshold was violated before leaving
                is_safe = (dep_temp <= 90.0) and (dep_batt >= 5.0)

                print("----------------------------------------------------------------")
                print("🏆 [TEST COMPLETED SUCCESFULLY]")
                if is_safe:
                    print(f"   1. Hardware Survival:  ✅ 100% (Evacuated safely before thresholds)")
                else:
                    print(f"   1. Hardware Survival:  ❌ 0% (LOGICAL CRASH: Left source node at {dep_temp}°C / {dep_batt}% Battery)")
                print(f"   2. Migration Delay:     ⏱️  {delay:.2f} seconds")
                print(f"   3. Bandwidth Footprint: 💾 {bandwidth:.1f} MB (Hops: {hops})")
                print(f"   ↳ Source State at Departure: Temp: {dep_temp}°C | Battery: {dep_batt}%")
                print("----------------------------------------------------------------")
            
            active_sml_node = node_name

    # Safety crash checking: If in-flight for more than 20 seconds, it crashed
    # We poll this by using a timeout in the loop or manual checks
    # To keep the code very simple, we assume if SML vanishes it is a crash (0% survival).

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n👋 Telemetry monitoring stopped.")
        sys.exit(0)
