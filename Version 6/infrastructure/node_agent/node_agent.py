"""
SPACE CLOUD V6 - NODE AGENT (Distributed Edge MPC)
===================================================
Role: Runs as a DaemonSet on every satellite node.
Architecture: Uses the 'Simulation Oracle' pattern. It trusts the Ground Redis
              telemetry bus as the absolute source of truth for both hardware
              temperature and workload placement (has_sml, has_master).
"""

import os
import time
import json
import subprocess
import threading
import requests
import redis


# =============================================================================
# CONFIGURATION
# =============================================================================

# The physical satellite this agent is running on (injected by the DaemonSet)
NODE_NAME = os.getenv("NODE_NAME", "minikube-m02")

# Ground Redis (Digital Twin telemetry bus)
GROUND_REDIS_HOST = os.getenv("GROUND_REDIS_HOST", "localhost")

# Floating Master Redis (topology store for Dijkstra pathfinding)
TOPOLOGY_REDIS_HOST = os.getenv("TOPOLOGY_REDIS_HOST", "topology-master")
TOPOLOGY_REDIS_PORT = 6379

# Guardian HTTP endpoint (intra-Pod, localhost only)
GUARDIAN_URL = "http://localhost:80"

# Temperature thresholds for Trigger A (values in Celsius)
T_SAFE = float(os.getenv("T_SAFE", "80.0"))   # Below this → no thermal threat
T_FUSE = float(os.getenv("T_FUSE", "120.0"))  # At or above this → critical danger

# Lateral tracking threshold for Trigger B (pixels from grid edge)
# If Center of Mass X coordinate goes below this OR above (64 - this), trigger
LATERAL_THRESHOLD = int(os.getenv("LATERAL_THRESHOLD", "8"))
GRID_W = 64  # Width of the satellite's visual swath (matches the worker's GRID_W)

# Telemetry delta thresholds: only push to Floating Master if value changed enough
DELTA_TEMP    = 1.0   # Degrees Celsius
DELTA_BATTERY = 5.0   # Percentage points

# How long (seconds) to wait for the Guardian's flush_complete signal
FLUSH_TIMEOUT_SECONDS = 10.0

# Heartbeat: push telemetry to Floating Master every N seconds regardless of delta
HEARTBEAT_INTERVAL = 10.0

# Polling interval for the /tmp/relay_complete trigger file (seconds)
RELAY_POLL_INTERVAL = 0.5


# =============================================================================
# SECTION 1: DIGITAL TWIN (Predictive Temperature Simulator)
# Ported from environment_sim.py — runs locally without any network calls.
# =============================================================================

# Physical constants (must match environment_sim.py exactly)
ORBIT_PERIOD   = 300.0
ECLIPSE_START  = 220
ECLIPSE_END    = 320
TEMP_SPACE     = -270.0
THERMAL_MASS   = 80.0
HEATING_SUN    = 100.0
HEATING_CPU_IDLE = 10.0
# --- DUAL WORKLOAD THERMAL CONSTANTS ---
HEATING_SML_LOAD     = 10.0
HEATING_MASTER_LOAD  = 85.0
COOLING_K      = 4.0


class VirtualSatellite:
    """
    A local predictive model of this satellite's thermal and orbital state.
    Instantiated from the latest telemetry reading, it simulates N seconds
    into the future to detect dangerous temperature trends before they happen.
    """

    def __init__(self, telemetry, has_sml, has_master):
        """
        Build the twin from current live telemetry data.
        telemetry: dict from Ground Redis with keys: temp, battery, angle, eclipse
        """
        self.temp       = telemetry.get("temp", 25.0)
        self.battery    = telemetry.get("battery", 100.0)
        self.angle      = telemetry.get("angle", 0.0)
        self.has_sml    = has_sml
        self.has_master = has_master

    def predict_future(self, horizon_seconds=30):
        """
        Runs the thermal model forward by 'horizon_seconds' simulation steps.
        Returns (is_safe: bool, reason: str).
          is_safe = False means the satellite will overheat within the horizon window.
        """
        sim_temp  = self.temp
        sim_angle = self.angle
        deg_per_sec = 360.0 / ORBIT_PERIOD

        for _ in range(horizon_seconds):
            # Advance orbital angle by one second
            sim_angle = (sim_angle + deg_per_sec) % 360.0
            in_eclipse = (ECLIPSE_START <= sim_angle <= ECLIPSE_END)

            # Compute heat input (CPU load + optional solar radiation)
            p_in = HEATING_CPU_IDLE
            if self.has_sml:
                p_in += HEATING_SML_LOAD
            if self.has_master:
                p_in += HEATING_MASTER_LOAD

            if not in_eclipse:
                p_in += HEATING_SUN

            # Compute radiative heat loss into the vacuum
            p_out = COOLING_K * (sim_temp - TEMP_SPACE) * 0.1

            # Apply Newton's law of cooling
            sim_temp += (p_in - p_out) / THERMAL_MASS

            # Early exit: if we already exceed fuse temperature, no need to simulate further
            if sim_temp >= T_FUSE:
                return False, f"THERMAL_FUSE: predicted {sim_temp:.1f}°C in {_+1}s"

            # Moderate early warning: exceeds safe limit but not yet critical
            if sim_temp >= T_SAFE:
                return False, f"THERMAL_WARN: predicted {sim_temp:.1f}°C (safe limit: {T_SAFE}°C)"

        # Satellite will remain cool throughout the forecast horizon
        return True, "NOMINAL"


# =============================================================================
# SECTION 2: FLOATING MASTER TOPOLOGY QUERIES
# Executes the Lua Dijkstra script atomically via EVALSHA.
# =============================================================================

# Cached SHA of the Lua Dijkstra script (loaded once at startup)
_lua_script_sha = None


def load_lua_script(topology_redis):
    """
    Loads the Dijkstra Lua script into the Floating Master's script cache.
    Uses SCRIPT LOAD so the script is stored by SHA and can be called
    atomically with EVALSHA on every subsequent query.
    """
    global _lua_script_sha

    # Path to the Lua file (relative to this script's location)
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
    Calls the Dijkstra Lua script on the Floating Master to find the best
    multi-hop route for this migration.

    migration_type: "thermal" → coolest trailing satellite
                    "lateral" → adjacent parallel orbital plane (follow the fire)

    Returns: result dict with {"route": [...], "type": "...", "cost": float}
             or None if the query fails.
    """
    if not _lua_script_sha:
        print("⚠️  AGENT: Lua script not loaded — cannot query topology.", flush=True)
        return None

    try:
        # EVALSHA: atomic execution inside Redis — no concurrent telemetry update can interrupt
        raw = topology_redis.evalsha(
            _lua_script_sha,
            1,             # Number of keys
            NODE_NAME,     # KEYS[1] = source node
            migration_type,  # ARGV[1] = "thermal" or "lateral"
            str(T_SAFE),   # ARGV[2] = safe temperature threshold
            str(T_FUSE)    # ARGV[3] = fuse temperature threshold
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
# SECTION 3: FLOATING MASTER TELEMETRY PUSH (Delta Encoding)
# Each Node Agent pushes only its own node's telemetry to the topology store.
# =============================================================================

def push_telemetry_to_floating_master(topology_redis, telemetry, last_pushed):
    """
    Pushes this node's telemetry to the Floating Master only if values have
    changed significantly (delta encoding). A heartbeat is sent every 10 seconds
    regardless of delta to prevent stale topology data.

    topology_redis: Redis connection to the Floating Master
    telemetry:      latest telemetry dict from Ground Redis
    last_pushed:    dict with {"temp", "battery", "last_time"} from the previous push

    Returns: updated last_pushed dict
    """
    now = time.time()
    new_temp    = telemetry.get("temp", 0.0)
    new_battery = telemetry.get("battery", 0.0)

    # Check if any value has changed enough to warrant a push
    temp_changed    = abs(new_temp    - last_pushed.get("temp",    -999)) >= DELTA_TEMP
    battery_changed = abs(new_battery - last_pushed.get("battery", -999)) >= DELTA_BATTERY
    heartbeat_due   = (now - last_pushed.get("last_time", 0)) >= HEARTBEAT_INTERVAL

    if temp_changed or battery_changed or heartbeat_due:
        try:
            # Write to the Floating Master's Hash for this node
            # The Lua script reads these values during pathfinding
            topology_redis.hset(f"node:{NODE_NAME}", mapping={
                "temp":        round(new_temp, 1),
                "battery":     round(new_battery, 1),
                "orbit_plane": telemetry.get("orbit_plane", "A"),  # For lateral routing
                "angle":       round(telemetry.get("angle", 0.0), 1),
                "is_working":  int(telemetry.get("is_working", False)),
                "updated_at":  int(now)
            })

            # Issue #3 fix: Register this node in the 'active_fleet' Set so the Lua
            # Dijkstra script can discover nodes via SMEMBERS instead of KEYS *.
            # SADD is idempotent — repeated calls are harmless and O(1).
            topology_redis.sadd("active_fleet", NODE_NAME)

            # Also update adjacency: compute which satellites are currently in range
            # (In production this comes from ephemeris data; here we use static config)
            _update_adjacency(topology_redis, telemetry)

            last_pushed = {"temp": new_temp, "battery": new_battery, "last_time": now}
        except Exception as e:
            print(f"⚠️  AGENT: Floating Master push failed: {e}", flush=True)

    return last_pushed


def _update_adjacency(topology_redis, telemetry):
    """
    Updates the adjacency Set for this node on the Floating Master.
    In V6 this is derived from orbital angle proximity: satellites within
    ±60° of each other share a line-of-sight ISL.
    """
    # Static ISL topology for the 3-satellite Minikube constellation
    # In a real deployment this would be computed from TLE ephemeris data
    static_adjacency = {
        "minikube-m02": ["minikube-m03", "minikube-m04"],
        "minikube-m03": ["minikube-m02", "minikube-m04"],
        "minikube-m04": ["minikube-m02", "minikube-m03"],
    }
    neighbours = static_adjacency.get(NODE_NAME, [])
    if neighbours:
        # Replace the whole adjacency set atomically
        adj_key = f"adj:{NODE_NAME}"
        topology_redis.delete(adj_key)
        topology_redis.sadd(adj_key, *neighbours)


# =============================================================================
# SECTION 4: MIGRATION ORCHESTRATION (SML & MASTER)
# =============================================================================

def trigger_local_migration(topology_redis, migration_type):
    """Sequence for migrating the SAMKNN Payload"""
    print(f"\n🚨 AGENT: Payload Migration triggered! Type={migration_type}", flush=True)
    route_result = query_floating_master(topology_redis, migration_type)
    if not route_result or not route_result.get("route"): return

    manifest = {"route": route_result["route"], "type": migration_type, "source": NODE_NAME}
    manifest_path = "/tmp/migration_manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f)

    open("/tmp/flush_state", "w").close()
    start = time.time()
    while not os.path.exists("/tmp/flush_complete"):
        if time.time() - start > FLUSH_TIMEOUT_SECONDS: break
        time.sleep(0.1)

    for f in ["/tmp/flush_state", "/tmp/flush_complete"]:
        try:
            os.remove(f)
        except:
            pass

    open("/tmp/prepare_jump", "w").close()
    time.sleep(0.5)

    # Use the consolidated checkpoint engine
    checkpoint_path = _request_checkpoint("space-mission", "sidecar-guardian")
    if not checkpoint_path: return

    print(f"📡 AGENT: Starting relay transfer to {route_result['route']}...", flush=True)
    relay_script = os.path.join(os.path.dirname(__file__), "..", "..", "ops", "relay_transfer.sh")
    subprocess.run(["bash", relay_script, checkpoint_path, manifest_path], capture_output=False)


def trigger_master_migration(topology_redis, migration_type):
    """Independent sequence for migrating the Topology Master"""
    print(f"\n🚨 AGENT: MASTER Topology Migration triggered! Type={migration_type}", flush=True)
    route_result = query_floating_master(topology_redis, migration_type)
    if not route_result or not route_result.get("route"): return

    manifest = {"route": route_result["route"], "type": migration_type, "source": NODE_NAME}
    manifest_path = "/tmp/master_migration_manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f)

    # Use the consolidated checkpoint engine
    checkpoint_path = _request_checkpoint("topology-master", "topology-redis")
    if not checkpoint_path: return

    print(f"📡 AGENT: Starting relay transfer to {route_result['route']}...", flush=True)
    relay_script = os.path.join(os.path.dirname(__file__), "..", "..", "ops", "relay_transfer.sh")
    subprocess.run(["bash", relay_script, checkpoint_path, manifest_path], capture_output=False)


def _request_checkpoint(app_label, container_name):
    """
    Consolidated, highly verbose CRIU checkpoint requester.
    Prints exact Kubelet HTTP responses to diagnose failures instantly.
    """
    print(f"📸 AGENT: Requesting {container_name} CRIU checkpoint from Kubelet...", flush=True)

    # 1. Dynamically discover the exact pod name
    try:
        cmd = f"kubectl get pod -l app={app_label} --field-selector status.phase=Running,spec.nodeName={NODE_NAME} -o jsonpath='{{.items[*].metadata.name}}'"
        out = subprocess.check_output(cmd, shell=True).decode().strip()
        if not out:
            print(f"❌ AGENT: No active pod found for app={app_label}", flush=True)
            return None
        pod_name = out.split()[0]  # Take the first one to avoid multi-string URL errors
    except Exception as e:
        print(f"❌ AGENT: kubectl get pod exception: {e}", flush=True)
        return None

    print(f"🎯 AGENT: Target Pod: {pod_name} | Container: {container_name}", flush=True)

    # 2. Attempt Direct Kubelet Proxy Request
    token_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    if not os.path.exists(token_path):
        print("⚠️  AGENT: SA token not found. Falling back to kubectl proxy.", flush=True)
        return _checkpoint_via_proxy(pod_name, container_name)

    try:
        with open(token_path, "r") as f:
            sa_token = f.read().strip()
        api_url = f"https://kubernetes.default.svc/api/v1/nodes/{NODE_NAME}/proxy/checkpoint/default/{pod_name}/{container_name}"
        ca_path = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

        resp = requests.post(api_url, headers={"Authorization": f"Bearer {sa_token}"}, verify=ca_path, timeout=30)

        if resp.status_code != 200:
            print(f"❌ AGENT: Kubelet Checkpoint API Error {resp.status_code}: {resp.text}", flush=True)
            return None

        path = resp.json()["items"][0]
        print(f"✅ AGENT: Checkpoint created at {path}", flush=True)
        return path

    except Exception as e:
        print(f"❌ AGENT: Checkpoint HTTP exception: {e}", flush=True)
        return _checkpoint_via_proxy(pod_name, container_name)


def _checkpoint_via_proxy(pod_name, container_name):
    """Fallback mechanism using localhost kubectl proxy to bypass SSL/Auth issues."""
    print("🔄 AGENT: Initiating kubectl proxy fallback...", flush=True)
    proxy = subprocess.Popen(["kubectl", "proxy", "--port=8001"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        health_url = f"http://127.0.0.1:8001/api/v1/nodes/{NODE_NAME}/proxy/healthz"
        for _ in range(30):
            try:
                if requests.get(health_url, timeout=0.2).status_code == 200: break
            except Exception:
                pass
            time.sleep(0.1)

        api_url = f"http://127.0.0.1:8001/api/v1/nodes/{NODE_NAME}/proxy/checkpoint/default/{pod_name}/{container_name}"
        resp = requests.post(api_url, timeout=30)

        if resp.status_code != 200:
            print(f"❌ AGENT: Proxy Checkpoint API returned {resp.status_code}: {resp.text}", flush=True)
            return None

        path = resp.json()["items"][0]
        print(f"✅ AGENT: Checkpoint created at {path}", flush=True)
        return path
    except Exception as e:
        print(f"❌ AGENT: Proxy Checkpoint exception: {e}", flush=True)
        return None
    finally:
        proxy.terminate()
        try:
            proxy.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proxy.kill()


# =============================================================================
# SECTION 5: DESTINATION-SIDE RELAY RECEIVER & DEMULTIPLEXER
# =============================================================================

def relay_receiver_loop():
    print("👂 AGENT: Receiver loop armed. Polling for /tmp/relay_complete...", flush=True)
    while True:
        if os.path.exists("/tmp/relay_complete"):
            print("\n📦 AGENT: relay_complete detected — inspecting payload!", flush=True)
            try: os.remove("/tmp/relay_complete")
            except: pass

            tar_path = "/tmp/checkpoint.tar"

            # Binary Inspection Demultiplexer
            is_master = False
            try:
                subprocess.check_call(f"grep -a 'sidecar-guardian' {tar_path}", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            except subprocess.CalledProcessError:
                is_master = True

            if is_master: _rebuild_and_deploy_master(tar_path)
            else: _rebuild_and_deploy(tar_path)

        time.sleep(RELAY_POLL_INTERVAL)


def _rebuild_and_deploy(tar_path):
    print("🔨 AGENT [BUILDAH]: Reconstructing SML Payload...", flush=True)
    build_script = f"""
    buildah rm restoration-lab 2>/dev/null || true
    buildah from --name restoration-lab scratch
    buildah add restoration-lab {tar_path} /
    MNT=$(buildah mount restoration-lab) && rm -f $MNT/tmp/prepare_jump && buildah unmount restoration-lab
    buildah config --annotation "io.kubernetes.cri-o.annotations.checkpoint.name=sidecar-guardian" restoration-lab
    buildah commit restoration-lab localhost/space-sidecar:restored
    buildah rm restoration-lab
    """
    subprocess.run(build_script, shell=True, check=False)

    patch = json.dumps({"spec": {"template": {"spec": {
        "terminationGracePeriodSeconds": 0,
        "nodeSelector": {"type": "satellite", "kubernetes.io/hostname": NODE_NAME},
        "containers": [
            {"name": "sidecar-guardian", "image": "localhost/space-sidecar:restored", "imagePullPolicy": "Never"},
            {"name": "payload-phoenix", "image": "localhost/space-workload:latest", "imagePullPolicy": "Never"}
        ]
    }}}})
    subprocess.run("kubectl scale deployment space-mission --replicas=0", shell=True)
    subprocess.run("kubectl delete pod -l app=space-mission --force --grace-period=0", shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    subprocess.run(f"kubectl patch deployment space-mission --type=strategic -p '{patch}'", shell=True)
    subprocess.run("kubectl scale deployment space-mission --replicas=1", shell=True)

    pod_name = None
    for _ in range(50):
        if not pod_name:
            try:
                out = subprocess.check_output(f"kubectl get pod -l app=space-mission --field-selector spec.nodeName={NODE_NAME} -o jsonpath='{{.items[0].metadata.name}}'", shell=True, stderr=subprocess.DEVNULL)
                if out.decode().strip(): pod_name = out.decode().strip()
            except Exception: pass

        if pod_name:
            res = subprocess.run(f"kubectl exec {pod_name} -c sidecar-guardian -- touch /tmp/landed", shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
            if res.returncode == 0: return
        time.sleep(0.1)


def _rebuild_and_deploy_master(tar_path):
    print("🔨 AGENT [BUILDAH]: Reconstructing Topology Master...", flush=True)
    build_script = f"""
    buildah rm restoration-master 2>/dev/null || true
    buildah from --name restoration-master scratch
    buildah add restoration-master {tar_path} /
    buildah config --annotation "io.kubernetes.cri-o.annotations.checkpoint.name=redis" restoration-master
    buildah commit restoration-master localhost/space-master:restored
    buildah rm restoration-master
    """
    subprocess.run(build_script, shell=True, check=False)

    patch = json.dumps({"spec": {"template": {"spec": {
        "terminationGracePeriodSeconds": 0,
        "nodeSelector": {"type": "satellite", "kubernetes.io/hostname": NODE_NAME},
        "containers": [
            {"name": "redis", "image": "localhost/space-master:restored", "imagePullPolicy": "Never"},
            {"name": "topology-dashboard", "image": "localhost/space-topology-dashboard:latest", "imagePullPolicy": "Never"}
        ]
    }}}})
    subprocess.run("kubectl scale deployment topology-master --replicas=0", shell=True)
    subprocess.run("kubectl delete pod -l app=topology-master --force --grace-period=0", shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    subprocess.run(f"kubectl patch deployment topology-master --type=strategic -p '{patch}'", shell=True)
    subprocess.run("kubectl scale deployment topology-master --replicas=1", shell=True)


# =============================================================================
# SECTION 6: MAIN EVENT LOOP (Dual-Gate Telemetry Listener)
# =============================================================================

def main():
    print(f"🛸 NODE AGENT V6 ONLINE — satellite: {NODE_NAME}", flush=True)

    topology_redis = None
    while not topology_redis:
        try:
            r = redis.Redis(host=TOPOLOGY_REDIS_HOST, port=TOPOLOGY_REDIS_PORT, decode_responses=True, socket_connect_timeout=3)
            r.ping()
            topology_redis = r
            print("✅ AGENT: Connected to Floating Master Redis.", flush=True)
        except Exception:
            time.sleep(3)

    load_lua_script(topology_redis)
    threading.Thread(target=relay_receiver_loop, daemon=True).start()

    last_pushed = {"temp": -999, "battery": -999, "last_time": 0}
    migration_lock = threading.Lock()

    last_mig_sml = 0
    last_mig_master = 0
    COOLDOWN_SEC = 15.0

    channel = f"telemetry/{NODE_NAME}"
    print(f"📻 AGENT: Subscribing to {channel}...", flush=True)

    while True:
        try:
            ground_redis = redis.Redis(host=GROUND_REDIS_HOST, port=6379, decode_responses=True, socket_connect_timeout=3)
            ground_redis.ping()
            pubsub = ground_redis.pubsub()
            pubsub.subscribe(channel)
            print("✅ AGENT: Connected to Ground Redis. Listening for telemetry...", flush=True)

            for message in pubsub.listen():
                if message["type"] != "message": continue
                local_state = json.loads(message["data"])

                # Push telemetry to Master
                last_pushed = push_telemetry_to_floating_master(topology_redis, local_state, last_pushed)

                # ---- GATE 0: Telemetry as the Source of Truth ----
                # We trust the simulation environment's flags completely.
                has_sml = local_state.get("is_working", False)
                has_master = local_state.get("has_master", False)

                if not has_sml and not has_master: continue
                if migration_lock.locked(): continue

                # ---- TRIGGER A: Thermal Self-Preservation ----
                twin = VirtualSatellite(local_state, has_sml, has_master)
                is_safe, reason = twin.predict_future(30)

                if not is_safe:
                    print(f"🌡️  TRIGGER A FIRED: {reason}", flush=True)

                    # Enforce Cooldowns to prevent Oracle Race Condition
                    if has_sml and (time.time() - last_mig_sml > COOLDOWN_SEC):
                        last_mig_sml = time.time()
                        threading.Thread(target=_run_migration_with_lock,
                                         args=(topology_redis, "thermal", migration_lock), daemon=True).start()

                    # Changed from 'elif' to 'if' so it creates a sequential queue
                    if has_master and (time.time() - last_mig_master > COOLDOWN_SEC):
                        last_mig_master = time.time()
                        threading.Thread(target=_run_master_migration_with_lock,
                                         args=(topology_redis, "thermal", migration_lock), daemon=True).start()

                    continue

                    # ---- TRIGGER B: Lateral Fire Tracking ----
                if has_sml and (time.time() - last_mig_sml > COOLDOWN_SEC):
                    try:
                        resp = requests.get(f"{GUARDIAN_URL}/state", timeout=1)
                        if resp.status_code == 200:
                            com_x = resp.json().get("center_of_mass", {}).get("x", GRID_W / 2)
                            if (com_x < LATERAL_THRESHOLD or com_x > (GRID_W - LATERAL_THRESHOLD)):
                                print(f"🔥 TRIGGER B FIRED: CoM_x={com_x:.1f}", flush=True)
                                last_mig_sml = time.time()
                                threading.Thread(target=_run_migration_with_lock,
                                                 args=(topology_redis, "lateral", migration_lock), daemon=True).start()
                    except requests.exceptions.RequestException:
                        pass

        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            time.sleep(3)
        except Exception:
            time.sleep(3)

def _run_migration_with_lock(topology_redis, migration_type, lock):
    acquired = lock.acquire(blocking=False)
    if not acquired: return
    try: trigger_local_migration(topology_redis, migration_type)
    finally: lock.release()

def _run_master_migration_with_lock(topology_redis, migration_type, lock):
    acquired = lock.acquire(blocking=False)
    if not acquired: return
    try: trigger_master_migration(topology_redis, migration_type)
    finally: lock.release()

if __name__ == "__main__":
    main()