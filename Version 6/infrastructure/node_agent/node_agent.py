"""
===============================================================================
TEACHING EDITION — node_agent.py
===============================================================================

This file implements the V6 Node Agent of the Space Cloud system.

This agent runs as a DaemonSet on EVERY satellite node and replaces the old
centralized MPC controller with a fully autonomous, distributed decision engine.

This single process is responsible for:

1) Listening to its own telemetry from Ground Redis (Digital Twin bus)
2) Pushing delta-encoded telemetry to the Floating Master (topology store)
3) Predicting the satellite’s future temperature using a local Digital Twin
4) Detecting when the payload must migrate (two independent triggers)
5) Orchestrating the entire migration pipeline:
      - File-based IPC with the Guardian
      - CRIU checkpoint through the Kubelet API
      - Multi-hop relay via relay_transfer.sh
6) Acting as a receiver when another satellite migrates TO this node
7) Rebuilding and redeploying the pod from a CRIU checkpoint

This is NOT just a listener. It is simultaneously:
   • telemetry consumer
   • predictive controller
   • distributed routing client
   • migration orchestrator
   • restore agent

Communication mechanisms used:
   - Redis pub/sub
   - Redis topology store
   - HTTP (Guardian)
   - File triggers in /tmp (IPC)
   - Shell commands to Kubernetes, Buildah, CRIU

No gRPC, no protobuf — intentionally simple primitives.
"""

import os
import time
import json
import math
import subprocess
import threading
import requests
import redis


# =============================================================================
# CONFIGURATION
# =============================================================================

# Unique name of the satellite node where this agent runs (injected by DaemonSet)
NODE_NAME = os.getenv("NODE_NAME", "minikube-m02")

# Redis containing live telemetry from the Digital Twin
GROUND_REDIS_HOST = os.getenv("GROUND_REDIS_HOST", "localhost")

# Redis used as topology database for Dijkstra pathfinding (Floating Master)
TOPOLOGY_REDIS_HOST = os.getenv("TOPOLOGY_REDIS_HOST", "topology-master")
TOPOLOGY_REDIS_PORT = 6379

# HTTP endpoint of the Guardian container inside the same Pod
GUARDIAN_URL = "http://localhost:80"

# Temperature thresholds used by Trigger A (thermal self-preservation)
T_SAFE = float(os.getenv("T_SAFE", "80.0"))
T_FUSE = float(os.getenv("T_FUSE", "120.0"))

# Threshold used by Trigger B (fire drifting to edge of swath)
LATERAL_THRESHOLD = int(os.getenv("LATERAL_THRESHOLD", "8"))
GRID_W = 64  # Must match worker GRID_W exactly

# Delta thresholds to avoid flooding topology Redis
DELTA_TEMP    = 1.0
DELTA_BATTERY = 5.0

# Timeouts and intervals
FLUSH_TIMEOUT_SECONDS = 10.0
HEARTBEAT_INTERVAL = 10.0
RELAY_POLL_INTERVAL = 0.5


# =============================================================================
# SECTION 1: DIGITAL TWIN — Predictive Temperature Simulator
# =============================================================================

# Physical constants (must match environment_sim.py exactly)
ORBIT_PERIOD   = 120.0
ECLIPSE_START  = 220
ECLIPSE_END    = 320
TEMP_SPACE     = -270.0
THERMAL_MASS   = 40.0
HEATING_SUN    = 100.0
HEATING_CPU_IDLE = 10.0
HEATING_CPU_LOAD = 85.0
COOLING_K      = 4.0


class VirtualSatellite:
    """
    Local predictive thermal model of this satellite.

    Instead of reacting to current temperature, we simulate the next 60 seconds
    and ask: "Will I overheat soon?"
    """

    def __init__(self, telemetry):
        # Initialize simulation state from live telemetry snapshot
        self.temp      = telemetry.get("temp", 25.0)
        self.battery   = telemetry.get("battery", 100.0)
        self.angle     = telemetry.get("angle", 0.0)
        self.is_working = telemetry.get("is_working", True)

    def predict_future(self, horizon_seconds=60):
        """
        Simulates temperature second-by-second into the future.

        Applies:
          - orbital motion
          - eclipse detection
          - CPU heating
          - solar heating
          - radiative cooling

        If SAFE or FUSE temperature is crossed, returns unsafe immediately.
        """
        sim_temp  = self.temp
        sim_angle = self.angle
        deg_per_sec = 360.0 / ORBIT_PERIOD  # orbital angular velocity

        for _ in range(horizon_seconds):
            # Advance orbit by 1 second
            sim_angle = (sim_angle + deg_per_sec) % 360.0
            in_eclipse = (ECLIPSE_START <= sim_angle <= ECLIPSE_END)

            # Base heat from CPU
            p_in = HEATING_CPU_LOAD if self.is_working else HEATING_CPU_IDLE

            # Add solar heating if not in eclipse
            if not in_eclipse:
                p_in += HEATING_SUN

            # Radiative cooling into space
            p_out = COOLING_K * (sim_temp - TEMP_SPACE) * 0.1

            # Newton's law of cooling
            sim_temp += (p_in - p_out) / THERMAL_MASS

            # Early exits if dangerous
            if sim_temp >= T_FUSE:
                return False, f"THERMAL_FUSE: predicted {sim_temp:.1f}°C in {_+1}s"

            if sim_temp >= T_SAFE:
                return False, f"THERMAL_WARN: predicted {sim_temp:.1f}°C (safe limit: {T_SAFE}°C)"

        return True, "NOMINAL"


# =============================================================================
# SECTION 2: LUA DIJKSTRA TOPOLOGY QUERY
# =============================================================================

_lua_script_sha = None


def load_lua_script(topology_redis):
    """
    Loads dijkstra.lua into Redis script cache and stores its SHA.

    Later invoked via EVALSHA for atomic, interrupt-free execution.
    """
    global _lua_script_sha

    lua_path = os.path.join(os.path.dirname(__file__), "dijkstra.lua")
    try:
        with open(lua_path, "r") as f:
            lua_code = f.read()
        _lua_script_sha = topology_redis.script_load(lua_code)
        print(f"✅ AGENT: Dijkstra Lua loaded. SHA: {_lua_script_sha[:16]}...", flush=True)
    except Exception as e:
        print(f"⚠️  AGENT: Could not load Lua script: {e}", flush=True)


def query_floating_master(topology_redis, migration_type):
    """
    Calls the Lua Dijkstra script inside Redis to compute the best route.

    The heavy computation is inside Redis, not Python.
    """
    if not _lua_script_sha:
        print("⚠️  AGENT: Lua script not loaded — cannot query topology.", flush=True)
        return None

    try:
        raw = topology_redis.evalsha(
            _lua_script_sha,
            1,
            NODE_NAME,
            migration_type,
            str(T_SAFE),
            str(T_FUSE)
        )
        result = json.loads(raw)

        if "error" in result:
            print(f"⚠️  AGENT: Lua returned error: {result['error']}", flush=True)
            return None

        print(f"🗺️  AGENT: Route found ({migration_type}): {result['route']}", flush=True)
        return result

    except Exception as e:
        print(f"⚠️  AGENT: EVALSHA error: {e}", flush=True)
        return None


# =============================================================================
# SECTION 3: DELTA ENCODED TELEMETRY PUSH
# =============================================================================

def push_telemetry_to_floating_master(topology_redis, telemetry, last_pushed):
    """
    Pushes telemetry to topology Redis only when meaningful changes occur
    or heartbeat interval passes.
    """
    now = time.time()
    new_temp    = telemetry.get("temp", 0.0)
    new_battery = telemetry.get("battery", 0.0)

    temp_changed    = abs(new_temp    - last_pushed.get("temp",    -999)) >= DELTA_TEMP
    battery_changed = abs(new_battery - last_pushed.get("battery", -999)) >= DELTA_BATTERY
    heartbeat_due   = (now - last_pushed.get("last_time", 0)) >= HEARTBEAT_INTERVAL

    if temp_changed or battery_changed or heartbeat_due:
        try:
            topology_redis.hset(f"node:{NODE_NAME}", mapping={
                "temp":        round(new_temp, 1),
                "battery":     round(new_battery, 1),
                "orbit_plane": telemetry.get("orbit_plane", "A"),
                "angle":       round(telemetry.get("angle", 0.0), 1),
                "is_working":  int(telemetry.get("is_working", False)),
                "updated_at":  int(now)
            })

            # Critical for Lua discovery (avoids KEYS *)
            topology_redis.sadd("active_fleet", NODE_NAME)

            _update_adjacency(topology_redis, telemetry)

            last_pushed = {"temp": new_temp, "battery": new_battery, "last_time": now}
        except Exception as e:
            print(f"⚠️  AGENT: Floating Master push failed: {e}", flush=True)

    return last_pushed


def _update_adjacency(topology_redis, telemetry):
    """
    Updates adjacency set for this node (which nodes are in ISL range).
    """
    static_adjacency = {
        "minikube-m02": ["minikube-m03", "minikube-m04"],
        "minikube-m03": ["minikube-m02", "minikube-m04"],
        "minikube-m04": ["minikube-m02", "minikube-m03"],
    }
    neighbours = static_adjacency.get(NODE_NAME, [])
    if neighbours:
        adj_key = f"adj:{NODE_NAME}"
        topology_redis.delete(adj_key)
        topology_redis.sadd(adj_key, *neighbours)


# =============================================================================
# SECTION 4: MIGRATION ORCHESTRATION
# =============================================================================

def trigger_local_migration(topology_redis, migration_type):
    """
    Executes the full migration pipeline in background thread.
    """
    print(f"\n🚨 AGENT: Migration triggered! Type={migration_type}", flush=True)

    route_result = query_floating_master(topology_redis, migration_type)
    if not route_result or not route_result.get("route"):
        print("❌ AGENT: No valid route found. Aborting migration.", flush=True)
        return

    route = route_result["route"]
    manifest = {
        "route": route,
        "type": migration_type,
        "source": NODE_NAME
    }

    manifest_path = "/tmp/migration_manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f)

    # ---- IPC with Guardian via files ----
    open("/tmp/flush_state", "w").close()

    start = time.time()
    while not os.path.exists("/tmp/flush_complete"):
        if time.time() - start > FLUSH_TIMEOUT_SECONDS:
            break
        time.sleep(0.1)

    for f in ["/tmp/flush_state", "/tmp/flush_complete"]:
        try:
            os.remove(f)
        except FileNotFoundError:
            pass

    open("/tmp/prepare_jump", "w").close()
    time.sleep(0.5)

    checkpoint_path = _criu_checkpoint()
    if not checkpoint_path:
        return

    relay_script = os.path.join(os.path.dirname(__file__), "..", "..", "ops", "relay_transfer.sh")
    subprocess.run(["bash", relay_script, checkpoint_path, manifest_path])


def _criu_checkpoint():
    """
    Requests CRIU checkpoint directly via Kubelet API using ServiceAccount token.
    """
    try:
        pod_name = subprocess.check_output(
            f"kubectl get pod -l app=space-mission "
            f"--field-selector spec.nodeName={NODE_NAME} "
            f"-o jsonpath='{{.items[0].metadata.name}}'",
            shell=True
        ).decode().strip()
    except Exception:
        return None

    token_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    try:
        with open(token_path, "r") as f:
            sa_token = f.read().strip()
    except FileNotFoundError:
        return _criu_checkpoint_via_proxy(pod_name)

    api_url = (
        f"https://kubernetes.default.svc/api/v1/nodes/{NODE_NAME}/proxy"
        f"/checkpoint/default/{pod_name}/sidecar-guardian"
    )
    ca_path = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

    try:
        resp = requests.post(
            api_url,
            headers={"Authorization": f"Bearer {sa_token}"},
            verify=ca_path,
            timeout=30
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        return data["items"][0]
    except Exception:
        return None


def _criu_checkpoint_via_proxy(pod_name):
    proxy = subprocess.Popen(
        ["kubectl", "proxy", "--port=8001"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    try:
        api_url = (
            f"http://127.0.0.1:8001/api/v1/nodes/{NODE_NAME}/proxy"
            f"/checkpoint/default/{pod_name}/sidecar-guardian"
        )
        resp = requests.post(api_url, timeout=30)
        if resp.status_code != 200:
            return None
        data = resp.json()
        return data["items"][0]
    finally:
        proxy.terminate()


# =============================================================================
# SECTION 5: DESTINATION RECEIVER
# =============================================================================

def relay_receiver_loop():
    """
    Polls for relay_complete trigger file indicating checkpoint arrival.
    """
    while True:
        if os.path.exists("/tmp/relay_complete"):
            os.remove("/tmp/relay_complete")
            _rebuild_and_deploy("/tmp/checkpoint.tar")
        time.sleep(RELAY_POLL_INTERVAL)


def _rebuild_and_deploy(tar_path):
    """
    Rebuilds container image from CRIU TAR and forces Pod scheduling here.
    """
    build_script = f"""
    buildah rm restoration-lab 2>/dev/null || true
    buildah from --name restoration-lab scratch
    buildah add restoration-lab {tar_path} /
    MNT=$(buildah mount restoration-lab) && rm -f $MNT/tmp/prepare_jump && buildah unmount restoration-lab
    buildah config --annotation "io.kubernetes.cri-o.annotations.checkpoint.name=sidecar-guardian" restoration-lab
    buildah commit restoration-lab localhost/space-sidecar:restored
    buildah rm restoration-lab
    """
    subprocess.run(build_script, shell=True)

    patch = json.dumps({
        "spec": {"template": {"spec": {
            "terminationGracePeriodSeconds": 0,
            "nodeSelector": {"type": "satellite", "kubernetes.io/hostname": NODE_NAME},
            "containers": [
                {"name": "sidecar-guardian", "image": "localhost/space-sidecar:restored"},
                {"name": "payload-phoenix",  "image": "localhost/space-workload:latest"}
            ]
        }}}
    })

    subprocess.run("kubectl scale deployment space-mission --replicas=0", shell=True)
    subprocess.run(f"kubectl patch deployment space-mission --type=strategic -p '{patch}'", shell=True)
    subprocess.run("kubectl scale deployment space-mission --replicas=1", shell=True)


# =============================================================================
# SECTION 6: MAIN EVENT LOOP
# =============================================================================

def main():
    """
    Core loop:
      - connect to topology Redis
      - start receiver thread
      - subscribe to telemetry
      - evaluate Trigger A and Trigger B
    """
    topology_redis = redis.Redis(
        host=TOPOLOGY_REDIS_HOST,
        port=TOPOLOGY_REDIS_PORT,
        decode_responses=True
    )
    load_lua_script(topology_redis)

    threading.Thread(target=relay_receiver_loop, daemon=True).start()

    last_pushed = {"temp": -999, "battery": -999, "last_time": 0}
    migration_lock = threading.Lock()

    channel = f"telemetry/{NODE_NAME}"

    ground_redis = redis.Redis(host=GROUND_REDIS_HOST, port=6379, decode_responses=True)
    pubsub = ground_redis.pubsub()
    pubsub.subscribe(channel)

    for message in pubsub.listen():
        if message["type"] != "message":
            continue

        local_state = json.loads(message["data"])

        last_pushed = push_telemetry_to_floating_master(
            topology_redis, local_state, last_pushed
        )

        if not local_state.get("is_working", False):
            continue

        if migration_lock.locked():
            continue

        twin = VirtualSatellite(local_state)
        is_safe, _ = twin.predict_future(60)

        if not is_safe:
            threading.Thread(
                target=_run_migration_with_lock,
                args=(topology_redis, "thermal", migration_lock),
                daemon=True
            ).start()
            continue

        try:
            resp = requests.get(f"{GUARDIAN_URL}/state", timeout=1)
            if resp.status_code == 200:
                com_x = resp.json().get("center_of_mass", {}).get("x", GRID_W / 2)
                if com_x < LATERAL_THRESHOLD or com_x > (GRID_W - LATERAL_THRESHOLD):
                    threading.Thread(
                        target=_run_migration_with_lock,
                        args=(topology_redis, "lateral", migration_lock),
                        daemon=True
                    ).start()
        except Exception:
            pass


def _run_migration_with_lock(topology_redis, migration_type, lock):
    """
    Ensures only one migration runs at a time.
    """
    if not lock.acquire(blocking=False):
        return
    try:
        trigger_local_migration(topology_redis, migration_type)
    finally:
        lock.release()


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    main()