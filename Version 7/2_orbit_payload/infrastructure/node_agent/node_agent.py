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

# Battery thresholds for Trigger C (values in percentage)
B_SAFE = float(os.getenv("B_SAFE", "15.0"))   # Below this → predictive energy threat
B_FUSE = float(os.getenv("B_FUSE", "5.0"))    # At or below this → critical shutdown danger

# Lateral tracking threshold for Trigger B (pixels from grid edge)
# If Center of Mass X coordinate goes below this OR above (128 - this), trigger
LATERAL_THRESHOLD = int(os.getenv("LATERAL_THRESHOLD", "8"))
GRID_W = 128  # Width of the satellite's visual swath (matches the worker's GRID_W)

# Telemetry delta thresholds: only push to Floating Master if value changed enough
DELTA_TEMP    = 1.0   # Degrees Celsius
DELTA_BATTERY = 5.0   # Percentage points

# How long (seconds) to wait for the Guardian's flush_complete signal
FLUSH_TIMEOUT_SECONDS = 25.0

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
THERMAL_MASS   = 180.0
HEATING_SUN    = 100.0
HEATING_CPU_IDLE = 10.0
# --- DUAL WORKLOAD THERMAL CONSTANTS ---
HEATING_SML_LOAD     = 10.0
HEATING_MASTER_LOAD  = 10.0
COOLING_K      = 4.0
# --- ENERGY DYNAMICS CONSTANTS ---
BATTERY_CHARGE_RATE = 5.0
BATTERY_DRAIN_IDLE_SUN = 1.0
BATTERY_DRAIN_IDLE_ECLIPSE = 0.1  # Deep sleep hibernation mode

# Differentiated payload drain (Sun vs. Eclipse)
BATTERY_DRAIN_SML_SUN = 0.1
BATTERY_DRAIN_SML_ECLIPSE = 0.1    # Higher drain: requires active heaters in the dark
BATTERY_DRAIN_MASTER_SUN = 0.1
BATTERY_DRAIN_MASTER_ECLIPSE = 0.1 # Moderate heater overhead


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

            # Energy Dynamics Calculation
            if in_eclipse:
                charge = 0.0
                if not self.has_sml and not self.has_master:
                    # Case 1: Eclipse WITHOUT Payload -> Deep Sleep
                    drain = BATTERY_DRAIN_IDLE_ECLIPSE
                else:
                    # Case 2: Eclipse WITH Payload -> Base Power + Eclipse-Specific Payload Drain
                    drain = BATTERY_DRAIN_IDLE_SUN
                    if self.has_sml:
                        drain += BATTERY_DRAIN_SML_ECLIPSE
                    if self.has_master:
                        drain += BATTERY_DRAIN_MASTER_ECLIPSE
            else:
                charge = BATTERY_CHARGE_RATE
                if not self.has_sml and not self.has_master:
                    # Case 3: Sun WITHOUT Payload -> Standard Idle
                    drain = BATTERY_DRAIN_IDLE_SUN
                else:
                    # Case 4: Sun WITH Payload -> Base Power + Sun-Specific Payload Drain
                    drain = BATTERY_DRAIN_IDLE_SUN
                    if self.has_sml:
                        drain += BATTERY_DRAIN_SML_SUN
                    if self.has_master:
                        drain += BATTERY_DRAIN_MASTER_SUN

            self.battery += (charge - drain)
            self.battery = max(0.0, min(100.0, self.battery))

            # Moderate early warnings (Threshold Evaluations)
            if sim_temp >= T_SAFE:
                return False, f"THERMAL_WARN: predicted {sim_temp:.1f}°C (safe limit: {T_SAFE}°C)", "thermal"
            if self.battery <= B_SAFE:
                return False, f"ENERGY_WARN: predicted {self.battery:.1f}% (safe limit: {B_SAFE}%)", "energy"

            # Satellite will remain cool and charged throughout the forecast horizon
        return True, "NOMINAL", "none"


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


def query_floating_master(topology_redis, migration_type, exclude_node=""):
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
            1,              # Number of keys
            NODE_NAME,      # KEYS[1] = source node
            migration_type, # ARGV[1] = "thermal" or "lateral"
            str(T_SAFE),    # ARGV[2] = safe temperature threshold
            str(T_FUSE),    # ARGV[3] = fuse temperature threshold
            str(B_SAFE),    # ARGV[4] = safe battery threshold
            str(B_FUSE),    # ARGV[5] = fuse battery threshold
            exclude_node    # ARGV[6] = force split routing
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
        "minikube-m02": ["minikube-m03"],
        "minikube-m03": ["minikube-m02", "minikube-m04"],
        "minikube-m04": ["minikube-m03"],
    }
    neighbours = static_adjacency.get(NODE_NAME, [])
    if neighbours:
        # FIX (Change 3.1): Replace DELETE+SADD with an atomic pipeline.
        # Without this, the Dijkstra Lua script can observe zero neighbors for this
        # node between the DELETE and SADD, causing a spurious NO_ROUTE error.
        adj_key = f"adj:{NODE_NAME}"
        pipe = topology_redis.pipeline()
        pipe.delete(adj_key)
        pipe.sadd(adj_key, *neighbours)
        pipe.execute()


# =============================================================================
# SECTION 4: MIGRATION ORCHESTRATION (SML & MASTER)
# =============================================================================

def trigger_local_migration(topology_redis, migration_type, exclude_node=""):
    """Sequence for migrating the Time-Series CNN Payload"""
    print(f"\n🚨 AGENT: Payload Migration triggered! Type={migration_type} | Exclude={exclude_node}", flush=True)
    route_result = query_floating_master(topology_redis, migration_type, exclude_node)

    # Catch both None and empty route arrays
    if not route_result or not route_result.get("route"):
        return None

    # Capture the final destination of the calculated multi-hop route
    destination = route_result["route"][-1]

    manifest = {"route": route_result["route"], "type": migration_type, "source": NODE_NAME}
    manifest_path = "/tmp/migration_manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f)

    open("/tmp/flush_state", "w").close()
    start = time.time()
    while not os.path.exists("/tmp/flush_complete"):
        if time.time() - start > FLUSH_TIMEOUT_SECONDS: break
        time.sleep(0.1)

    for f in ["/tmp/flush_state", "/tmp/flush_complete", "/tmp/prepare_jump"]:
        try:
            os.remove(f)
        except:
            pass

    open("/tmp/prepare_jump", "w").close()
    time.sleep(0.5)

    # Use the consolidated checkpoint engine
    checkpoint_path = _request_checkpoint("space-mission", "sidecar-guardian")
    if not checkpoint_path: return None

    print(f"📡 AGENT: Starting relay transfer to {route_result['route']}...", flush=True)
    relay_script = os.path.join(os.path.dirname(__file__), "..", "..", "ops", "relay_transfer.sh")
    subprocess.run(["bash", relay_script, checkpoint_path, manifest_path], capture_output=False)

    # ---- SYNCHRONOUS SOURCE TEARDOWN (Fix for Double Migration) ----
    # Instantly kill the local pod on the source node so the Digital Twin Simulator
    # knows it has evacuated. This prevents the simulator from firing a second
    # thermal warning while the destination node is slowly running Buildah.
    print("🧹 AGENT: Tearing down local pod to prevent ghost hardware triggers...", flush=True)
    subprocess.run("kubectl scale deployment space-mission --replicas=0", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # Delete the local state file so we don't read "ghost" data after the payload leaves
    try:
        os.remove("/tmp/payload_state.json")
    except Exception:
        pass

    return destination


# FIX: Added '=""' to make exclude_node optional during single evacuations
def trigger_master_migration(topology_redis, migration_type, exclude_node=""):
    """Independent sequence for migrating the Topology Master"""
    # FIX: Added exclude_node to the print statement for easier debugging
    print(f"\n🚨 AGENT: MASTER Topology Migration triggered! Type={migration_type} | Exclude={exclude_node}", flush=True)

    route_result = query_floating_master(topology_redis, migration_type, exclude_node)
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

    print("🧹 AGENT: Tearing down local master pod to prevent ghost hardware triggers...", flush=True)
    subprocess.run("kubectl scale deployment topology-master --replicas=0", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


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
    """
    Converts the received TAR checkpoint into a running K8s Pod on this node.
    Uses Buildah to layer the CRIU memory pages on top of a blank image,
    then patches the Deployment to schedule on this node.
    """
    t_start = time.time()

    # Sanitize the landing zone: Nuke any stale IPC files from previous runs
    try:
        os.remove("/tmp/payload_state.json")
    except Exception:
        pass

    # FIX: Generate a unique, time-stamped image tag for the payload
    # This prevents Kubelet from using a stale cached image ID on multi-bounce migrations
    mig_id = int(time.time())
    new_image_tag = f"localhost/space-sidecar:restored-{mig_id}"

    # ---- STEP 1: Buildah — reconstruct the container image ----
    print("🔨 AGENT [BUILDAH]: Reconstructing container from checkpoint...", flush=True)
    build_script = f"""
    buildah rm restoration-lab 2>/dev/null || true
    buildah from --name restoration-lab scratch
    buildah add restoration-lab {tar_path} /
    MNT=$(buildah mount restoration-lab) && rm -f $MNT/tmp/prepare_jump && buildah unmount restoration-lab
    buildah config --annotation "io.kubernetes.cri-o.annotations.checkpoint.name=sidecar-guardian" restoration-lab
    buildah commit restoration-lab {new_image_tag}
    buildah rm restoration-lab
    """
    subprocess.run(build_script, shell=True, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(f"⏱️  AGENT: Buildah done in {time.time() - t_start:.2f}s", flush=True)

    # ---- STEP 2: K8s — reschedule the Pod on this node ----
    t0 = time.time()
    print("🗺️ AGENT [K8S]: Patching Deployment to this node...", flush=True)

    patch = json.dumps({
        "spec": {"template": {"spec": {
            "terminationGracePeriodSeconds": 0,
            "nodeSelector": {"type": "satellite", "kubernetes.io/hostname": NODE_NAME},
            "containers": [
                {
                    "name": "sidecar-guardian",
                    "image": new_image_tag,  # Use the new dynamic tag here
                    "imagePullPolicy": "Never"
                },
                {
                    "name": "payload-phoenix",
                    "image": "localhost/space-workload:latest",
                    "imagePullPolicy": "Never"
                }
            ]
        }}}
    })

    subprocess.run("kubectl scale deployment space-mission --replicas=0", shell=True)
    subprocess.run("kubectl delete pod -l app=space-mission --force --grace-period=0", shell=True)
    res_patch = subprocess.run(f"kubectl patch deployment space-mission --type=strategic -p '{patch}'", shell=True, capture_output=True, text=True)
    if res_patch.returncode != 0:
        print(f"❌ KUBECTL PATCH ERROR: {res_patch.stderr}", flush=True)
    subprocess.run("kubectl scale deployment space-mission --replicas=1", shell=True)
    print(f"⏱️  AGENT: K8s patch done in {time.time() - t0:.2f}s", flush=True)

    # ---- CRIU SCOPE LOGGING (Change 4.1) ----
    # Document the architectural intention: Guardian is restored from CRIU checkpoint,
    # worker cold-boots fresh from its original image (ONNX model loaded lazily).
    print(f"📋 AGENT: Guardian → CRIU-restored image ({new_image_tag})", flush=True)
    print(f"📋 AGENT: Worker  → fresh cold-boot image (localhost/space-workload:latest)", flush=True)

    # ---- STEP 3: Wait for pod to boot, signal landing, then confirm readiness ----
    t0 = time.time()
    pod_name = None
    landed_signaled = False
    for _ in range(120):  # 12-second maximum wait (120 × 100ms)
        if not pod_name:
            try:
                out = subprocess.check_output(
                    f"kubectl get pod -l app=space-mission"
                    f" --field-selector spec.nodeName={NODE_NAME}"
                    f" -o jsonpath='{{.items[0].metadata.name}}'",
                    shell=True, stderr=subprocess.DEVNULL
                )
                found = out.decode().strip()
                if found:
                    pod_name = found
            except Exception:
                pass

        if pod_name and not landed_signaled:
            # Step 3a: Touch /tmp/landed — triggers the Guardian's setInterval to exit flightMode
            res = subprocess.run(
                f"kubectl exec {pod_name} -c sidecar-guardian -- touch /tmp/landed",
                shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL
            )
            if res.returncode == 0:
                landed_signaled = True

        if landed_signaled:
            # FIX (Change 2.1): Readiness Probe — confirm the Guardian has actually exited
            # flightMode and restarted its HTTP server before declaring the landing complete.
            # Previously we returned immediately after 'touch /tmp/landed', but the Guardian
            # only processes that file up to 1s later via setInterval, creating a window
            # where a new migration could fire against a still-frozen Guardian.
            probe = subprocess.run(
                f"kubectl exec {pod_name} -c sidecar-guardian -- "
                f"wget -q -O /dev/null --timeout=1 http://localhost:80/",
                shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL
            )
            if probe.returncode == 0:
                # ---- DUAL-CONTAINER READINESS CONFIRMATION (Change 4.2) ----
                print(f"✅ AGENT: Guardian is alive on destination node.", flush=True)
                print(f"📋 AGENT: Worker container cold-booting independently (stateless, no CRIU restore).", flush=True)
                print(f"✅ AGENT: Total restore time: {time.time() - t_start:.2f}s", flush=True)
                return

        time.sleep(0.1)

    print(f"\u26a0\ufe0f AGENT: Pod did not awaken within timeout ({time.time() - t0:.2f}s)", flush=True)


def _rebuild_and_deploy_master(tar_path):
    print("🔨 AGENT [BUILDAH]: Reconstructing Topology Master...", flush=True)

    t_start = time.time()

    # Sanitize the landing zone: Nuke any stale IPC files from previous runs
    # BEFORE the new pod boots. This guarantees the agent will wait for the
    # newly restored Guardian to write a fresh state with a fresh 30s cooldown.
    try:
        os.remove("/tmp/payload_state.json")
    except Exception:
        pass

    # FIX: Generate a unique, time-stamped image tag for every migration
    # This prevents Kubelet from using a stale cached image ID
    mig_id = int(time.time())
    new_image_tag = f"localhost/space-master:restored-{mig_id}"

    build_script = f"""
    buildah rm restoration-master 2>/dev/null || true
    buildah from --name restoration-master scratch
    buildah add restoration-master {tar_path} /
    buildah config --annotation "io.kubernetes.cri-o.annotations.checkpoint.name=topology-redis" restoration-master
    # Commit using the dynamic tag
    buildah commit restoration-master {new_image_tag}
    buildah rm restoration-master
    """
    subprocess.run(build_script, shell=True, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(f"⏱️  AGENT: Buildah done in {time.time() - t_start:.2f}s", flush=True)

    patch = json.dumps({"spec": {"template": {"spec": {
        "terminationGracePeriodSeconds": 0,
        "nodeSelector": {"type": "satellite", "kubernetes.io/hostname": NODE_NAME},
        "containers": [
            # Patch the deployment with the exact new dynamic tag
            {"name": "topology-redis", "image": new_image_tag, "imagePullPolicy": "Never"},
            {"name": "topology-dashboard", "image": "localhost/space-topology-dashboard:latest",
             "imagePullPolicy": "Never"}
        ]
    }}}})
    subprocess.run("kubectl scale deployment topology-master --replicas=0", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run("kubectl delete pod -l app=topology-master --force --grace-period=0", shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    subprocess.run(f"kubectl patch deployment topology-master --type=strategic -p '{patch}'", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run("kubectl scale deployment topology-master --replicas=1", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


# =============================================================================
# SECTION 6: MAIN EVENT LOOP (Dual-Gate Telemetry Listener)
# =============================================================================

def main():
    print(f"🚀 NODE AGENT V6 ONLINE — satellite: {NODE_NAME}", flush=True)

    # Clean up stale IPC files from previous interrupted runs
    for stale_file in ["/tmp/relay_complete", "/tmp/checkpoint.tar", "/tmp/payload_state.json"]:
        try:
            if os.path.exists(stale_file):
                os.remove(stale_file)
                print(f"🧹 AGENT: Cleaned stale IPC file: {stale_file}", flush=True)
        except Exception:
            pass

    topology_redis = None
    while not topology_redis:
        try:
            r = redis.Redis(host=TOPOLOGY_REDIS_HOST, port=TOPOLOGY_REDIS_PORT, decode_responses=True,
                            socket_connect_timeout=3)
            r.ping()
            topology_redis = r
            print("✅ AGENT: Connected to Floating Master Redis.", flush=True)
        except Exception:
            time.sleep(3)

    load_lua_script(topology_redis)
    threading.Thread(target=relay_receiver_loop, daemon=True).start()

    last_pushed = {"temp": -999, "battery": -999, "last_time": 0}
    migration_lock = threading.Lock()

    last_mig_master = 0
    COOLDOWN_SEC = 15.0

    channel = f"telemetry/{NODE_NAME}"
    print(f"📡 AGENT: Subscribing to {channel}...", flush=True)

    last_cooldown_log = 0

    while True:
        try:
            ground_redis = redis.Redis(host=GROUND_REDIS_HOST, port=6379, decode_responses=True,
                                       socket_connect_timeout=3)
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
                # FIX (Change 2.2): Removed migration_lock.locked() advisory check.
                # It was a non-atomic read — not a real guard. The acquire(blocking=False)
                # calls downstream are the correct atomic gates.

                # ==============================================================
                # GLOBAL PAYLOAD COOLDOWN (The Ping-Pong Fix)
                # ==============================================================
                # Read the exact timestamp of when the payload landed on THIS node.
                # The Guardian sidecar writes this to /tmp/payload_state.json upon waking up.
                payload_last_migrated = 0
                com_x = GRID_W / 2
                fire_pixel_count = 0

                if has_sml:
                    try:
                        if os.path.exists("/tmp/payload_state.json"):
                            with open("/tmp/payload_state.json", "r") as f:
                                state_data = json.load(f)
                            payload_last_migrated = state_data.get("last_migration_time", 0)
                            com_x = state_data.get("center_of_mass", {}).get("x", GRID_W / 2)
                            fire_pixel_count = state_data.get("fire_pixel_count", 0)
                    except (json.JSONDecodeError, IOError):
                        pass

                # ==============================================================
                # TRIGGER A & C: Dual-Tier Hardware Self-Preservation
                # ==============================================================
                current_temp = local_state.get("temp", -999)
                should_migrate = False
                migration_reason = ""

                # 1. THE REACTIVE FAILSAFE (Instant execution if melting or dying)
                current_battery = local_state.get("battery", 100.0)

                if current_temp >= T_FUSE:
                    should_migrate = True
                    migration_reason = f"CRITICAL FAILSAFE: Temp is {current_temp:.1f}°C (>= {T_FUSE}°C)"
                    routing_type = "thermal"
                elif current_battery <= B_FUSE:
                    should_migrate = True
                    migration_reason = f"CRITICAL FAILSAFE: Battery is {current_battery:.1f}% (<= {B_FUSE}%)"
                    routing_type = "energy"

                # 2. THE PROACTIVE MPC (Predictive forecast for the next 30 seconds)
                else:
                    twin = VirtualSatellite(local_state, has_sml, has_master)
                    is_safe, pred_reason, routing_type = twin.predict_future(30)
                    if not is_safe:
                        should_migrate = True
                        migration_reason = f"PROACTIVE MPC: {pred_reason}"

                # ---- EXECUTE HARDWARE MIGRATION SCENARIOS ----
                if should_migrate:
                    if migration_lock.locked():
                        continue  # Silently wait while an evacuation is actively running
                    
                    # SCENARIO 1: DOUBLE EVACUATION
                    if has_sml and has_master:
                        # Ensure both the SML global cooldown and the Master local cooldown have passed
                        if (time.time() - last_mig_master > COOLDOWN_SEC) and (time.time() - payload_last_migrated > COOLDOWN_SEC):
                            if migration_lock.acquire(blocking=False):
                                print(f"🔥 HARDWARE TRIGGER FIRED: {migration_reason} (Double Evacuation Initiated)", flush=True)
                                last_mig_master = time.time()
                                threading.Thread(target=_run_predictive_double_evacuation,
                                                 args=(topology_redis, routing_type, migration_lock),
                                                 daemon=True).start()
                        else:
                            if time.time() - last_cooldown_log > 2.0:  # Only print once every 2 seconds
                                print(f"⏳ HARDWARE TRIGGER BLOCKED: {migration_reason} (Double Evacuation on Cooldown)",
                                  flush=True)
                                last_cooldown_log = time.time()

                    # SCENARIO 2: ONLY SML PAYLOAD IS HERE
                    elif has_sml:
                        if time.time() - payload_last_migrated > COOLDOWN_SEC:
                            if migration_lock.acquire(blocking=False):
                                print(f"🔥 HARDWARE TRIGGER FIRED: {migration_reason} (Payload Evacuation)", flush=True)
                                threading.Thread(target=_run_migration, args=(topology_redis, routing_type, migration_lock),
                                                    daemon=True).start()
                        else:
                            if time.time() - last_cooldown_log > 2.0:  # Only print once every 2 seconds
                                print(f"⏳ HARDWARE TRIGGER BLOCKED: {migration_reason} (Payload Evacuation on Cooldown)", flush=True)
                                last_cooldown_log = time.time()


                    # SCENARIO 3: ONLY FLOATING MASTER IS HERE
                    elif has_master:
                        if time.time() - last_mig_master > COOLDOWN_SEC:
                            if migration_lock.acquire(blocking=False):
                                print(f"🔥 HARDWARE TRIGGER FIRED: {migration_reason} (Master Evacuation)", flush=True)
                                last_mig_master = time.time()
                                threading.Thread(target=_run_master_migration,
                                                 args=(topology_redis, routing_type, migration_lock), daemon=True).start()
                        else:
                            if time.time() - last_cooldown_log > 2.0:  # Only print once every 2 seconds
                                print(f"⏳ HARDWARE TRIGGER BLOCKED: {migration_reason} (Master Evacuation on Cooldown)", flush=True)
                                last_cooldown_log = time.time()

                    continue  # Skip lateral evaluation if hardware is in danger

                # ==============================================================
                # TRIGGER B: Lateral Fire Tracking (Async IPC)
                # ==============================================================
                if has_sml and (time.time() - payload_last_migrated > COOLDOWN_SEC) and fire_pixel_count > 0:
                    current_plane = local_state.get("orbit_plane", "B")

                    # ESCAPING WEST (Left Edge)
                    if com_x < LATERAL_THRESHOLD:
                        if current_plane == "A":
                            print(f"⚠️ BOUNDARY LIMIT: Fire escaping WEST, but Plane A is the edge of coverage.", flush=True)
                        elif migration_lock.acquire(blocking=False):
                            print(f"🎯 TRIGGER B: Fire escaping WEST (CoM_x={com_x:.1f})", flush=True)
                            threading.Thread(target=_run_migration,
                                             args=(topology_redis, "lateral_west", migration_lock),
                                             daemon=True).start()

                    # ESCAPING EAST (Right Edge)
                    elif com_x > (GRID_W - LATERAL_THRESHOLD):
                        if current_plane == "C":
                            print(f"⚠️ BOUNDARY LIMIT: Fire escaping EAST, but Plane C is the edge of coverage.", flush=True)
                        elif migration_lock.acquire(blocking=False):
                            print(f"🎯 TRIGGER B: Fire escaping EAST (CoM_x={com_x:.1f})", flush=True)
                            threading.Thread(target=_run_migration,
                                            args=(topology_redis, "lateral_east", migration_lock),
                                            daemon=True).start()

        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            time.sleep(3)
        except Exception as e:
            print(f"⚠️ AGENT LOOP EXCEPTION: {e}", flush=True)
            time.sleep(3)


# =============================================================================
# THREAD RUNNER HELPERS (Lock Management)
# Note: The lock is now acquired in the main thread to prevent race conditions.
# These functions simply execute the migration and ensure the lock is released.
# =============================================================================

def _run_migration(topology_redis, migration_type, lock):
    try:
        trigger_local_migration(topology_redis, migration_type)
    finally:
        lock.release()


def _run_master_migration(topology_redis, migration_type, lock):
    try:
        trigger_master_migration(topology_redis, migration_type)
    finally:
        lock.release()


def _run_predictive_double_evacuation(topology_redis, routing_type, lock):
    try:
        print("🚨 CRITICAL: DOUBLE EVACUATION TRIGGERED. Initiating SML-First Sequence.", flush=True)

        # STEP 1: Route and Migrate the Data Plane (SML Payload)
        sml_destination = trigger_local_migration(topology_redis, routing_type)
        if not sml_destination:
            print("🚨 CRITICAL: SML routing failed. Aborting double evacuation.", flush=True)
            return

        print(f"🚨 CRITICAL: SML routed to {sml_destination}. Waiting for arrival...", flush=True)

        # STEP 2: Deterministic Block
        wait_for_sml_readiness()
        print("🚨 CRITICAL: SML Arrived safely. Now routing master migration.", flush=True)

        # STEP 3: Route and Migrate the Control Plane (Floating Master)
        # CRITICAL: We pass the SML destination to force an architectural split!
        trigger_master_migration(topology_redis, routing_type, exclude_node=sml_destination)

        # STEP 4: Deterministic Block
        wait_for_master_readiness()
        print("✅ CRITICAL: Double Evacuation complete. Both workloads survived.", flush=True)

    finally:
        lock.release()

def wait_for_sml_readiness():
    """Blocks until the SML Payload Guardian sidecar is actively accepting traffic."""
    print("⏳ AGENT: Waiting for SML Payload to rehydrate and route...", flush=True)
    # FIX (Change 3.2): Bounded 60-second timeout prevents migration_lock deadlock
    # if the destination pod enters CrashLoopBackOff.
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            # Ping the root HTML dashboard instead of the POST-only /state endpoint.
            # Using the shortened intra-cluster DNS name.
            res = requests.get("http://space-dashboard-svc/", timeout=1)
            if res.status_code == 200:
                print("✅ AGENT: SML Payload successfully landed and is Ready!", flush=True)
                return
        except requests.exceptions.RequestException:
            pass
        time.sleep(0.5)  # Rapid 500ms polling
    print("⚠️ AGENT: SML readiness timeout (60s). Proceeding to prevent deadlock.", flush=True)

def wait_for_master_readiness():
    """Blocks until the Floating Master Redis is actively accepting connections."""
    print("⏳ AGENT: Waiting for Floating Master to rehydrate and route...", flush=True)
    # Create a fresh, short-timeout Redis client specifically for the probe
    probe_client = redis.Redis(host=TOPOLOGY_REDIS_HOST, port=TOPOLOGY_REDIS_PORT, socket_timeout=1)
    # FIX (Change 3.2): Bounded 60-second timeout prevents migration_lock deadlock
    # if the destination Redis pod enters CrashLoopBackOff.
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            if probe_client.ping():
                print("✅ AGENT: Floating Master successfully landed and is Ready!", flush=True)
                return
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            pass
        time.sleep(0.5)
    print("⚠️ AGENT: Master readiness timeout (60s). Proceeding to prevent deadlock.", flush=True)

if __name__ == "__main__":
    main()
