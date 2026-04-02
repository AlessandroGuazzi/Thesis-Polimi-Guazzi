"""
SPACE CLOUD V6 - NODE AGENT (Distributed Edge MPC)
===================================================
Role: Runs as a DaemonSet on every satellite node. Replaces the centralized
      MPC Controller from V5 with a fully autonomous onboard decision engine.

This agent does three things simultaneously:
  1. TELEMETRY PUSH: Listens to its own node's telemetry on Ground Redis and
     pushes delta-encoded updates to the Floating Master for constellation topology.
  2. DUAL-GATE TRIGGER: Evaluates two independent migration triggers every second:
       Trigger A — Thermal Self-Preservation (CPU temperature forecast)
       Trigger B — Lateral Fire Tracking (SAMKNN Center of Mass drift)
  3. MIGRATION ORCHESTRATION: Coordinates the pre-freeze flush handshake and
     delegates the physical transfer to relay_transfer.sh.

Dependencies: requests, redis. No gRPC, no protobuf.
Communication: file-based IPC for intra-Pod commands (/tmp/flush_state, etc.),
               HTTP for worker state queries, Redis for topology and telemetry.
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
    A local predictive model of this satellite's thermal and orbital state.
    Instantiated from the latest telemetry reading, it simulates N seconds
    into the future to detect dangerous temperature trends before they happen.
    """

    def __init__(self, telemetry):
        """
        Build the twin from current live telemetry data.
        telemetry: dict from Ground Redis with keys: temp, battery, angle, eclipse
        """
        self.temp      = telemetry.get("temp", 25.0)
        self.battery   = telemetry.get("battery", 100.0)
        self.angle     = telemetry.get("angle", 0.0)
        self.is_working = telemetry.get("is_working", True)

    def predict_future(self, horizon_seconds=60):
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
            p_in = HEATING_CPU_LOAD if self.is_working else HEATING_CPU_IDLE
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
    lua_path = os.path.join(os.path.dirname(__file__), "..", "dijkstra.lua")
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
        if neighbours:
            topology_redis.sadd(adj_key, *neighbours)


# =============================================================================
# SECTION 4: MIGRATION ORCHESTRATION (File-Based IPC + CRIU)
# =============================================================================

def trigger_local_migration(topology_redis, migration_type):
    """
    Full migration sequence:
    Step 1: Query the Floating Master for the optimal multi-hop route.
    Step 2: Pre-freeze flush (file-based IPC handshake with Guardian).
    Step 3: CRIU checkpoint via Kubelet API.
    Step 4: Invoke relay_transfer.sh for SSH-staged multi-hop delivery.

    This function runs in a background thread so the telemetry listener
    continues operating while the migration executes.
    """
    print(f"\n🚨 AGENT: Migration triggered! Type={migration_type}", flush=True)

    # ---- Step 1: Topology Query ----
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

    # Write the manifest for relay_transfer.sh to read
    manifest_path = "/tmp/migration_manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f)

    # ---- Step 2: Pre-Freeze Flush Handshake ----
    # Write the trigger file that the Guardian is watching for
    print("💾 AGENT: Writing /tmp/flush_state — triggering Guardian flush...", flush=True)
    open("/tmp/flush_state", "w").close()

    # Poll for /tmp/flush_complete (written by Guardian after worker flushes)
    start = time.time()
    while not os.path.exists("/tmp/flush_complete"):
        if time.time() - start > FLUSH_TIMEOUT_SECONDS:
            print("⚠️  AGENT: Flush timeout — proceeding with last known state.", flush=True)
            break
        time.sleep(0.1)

    # Clean up the flush handshake files
    for f in ["/tmp/flush_state", "/tmp/flush_complete"]:
        try:
            os.remove(f)
        except FileNotFoundError:
            pass

    # Tell the Guardian to enter flightMode and close all sockets for CRIU
    print("🔒 AGENT: Writing /tmp/prepare_jump — initiating CRIU preparation...", flush=True)
    open("/tmp/prepare_jump", "w").close()

    # Wait briefly for the Guardian to complete its graceful disconnect
    time.sleep(0.5)

    # ---- Step 3: CRIU Checkpoint ----
    checkpoint_path = _criu_checkpoint()
    if not checkpoint_path:
        print("❌ AGENT: Checkpoint failed. Migration aborted.", flush=True)
        return

    # ---- Step 4: Relay Transfer ----
    print(f"📡 AGENT: Starting relay transfer to {route}...", flush=True)
    relay_script = os.path.join(os.path.dirname(__file__), "..", "..", "ops", "relay_transfer.sh")
    result = subprocess.run(
        ["bash", relay_script, checkpoint_path, manifest_path],
        capture_output=False
    )
    if result.returncode != 0:
        print(f"❌ AGENT: relay_transfer.sh failed with code {result.returncode}.", flush=True)
    else:
        print("✅ AGENT: Relay transfer complete. Awaiting restore on destination.", flush=True)


def _criu_checkpoint():
    """
    Requests a CRIU checkpoint of the 'sidecar-guardian' container from the Kubelet.

    Issue #2 fix: Instead of spawning a background 'kubectl proxy' process (which
    can become a zombie leaking port 8001 on crash), we use the ServiceAccount
    token mounted at /var/run/secrets/kubernetes.io/serviceaccount/ to authenticate
    directly to the Kubelet REST API. This is cleaner, faster, and leak-proof.
    """
    print("📸 AGENT: Requesting CRIU checkpoint from Kubelet...", flush=True)

    # Find the pod currently running on this node
    try:
        pod_name = subprocess.check_output(
            f"kubectl get pod -l app=space-mission"
            f" --field-selector spec.nodeName={NODE_NAME}"
            f" -o jsonpath='{{.items[0].metadata.name}}'",
            shell=True
        ).decode().strip()
    except Exception as e:
        print(f"❌ AGENT: Cannot find local pod: {e}", flush=True)
        return None

    # Read the ServiceAccount token (mounted by K8s into every Pod)
    token_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    try:
        with open(token_path, "r") as f:
            sa_token = f.read().strip()
    except FileNotFoundError:
        print("⚠️  AGENT: No ServiceAccount token found — falling back to kubectl proxy.", flush=True)
        return _criu_checkpoint_via_proxy(pod_name)

    # POST directly to the Kubelet checkpoint API using the ServiceAccount bearer token.
    # This eliminates the need for a background 'kubectl proxy' process entirely.
    # The API server proxies the request to the Kubelet on the correct node.
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
            print(f"❌ AGENT: Checkpoint API returned {resp.status_code}: {resp.text}", flush=True)
            return None

        data = resp.json()
        path = data["items"][0]
        print(f"✅ AGENT: Checkpoint created at {path}", flush=True)
        return path

    except Exception as e:
        print(f"❌ AGENT: Checkpoint exception: {e}", flush=True)
        return None


def _criu_checkpoint_via_proxy(pod_name):
    """
    Fallback: Uses kubectl proxy when ServiceAccount token is not available
    (e.g., running outside the cluster during development). Includes proper
    cleanup via try/finally to prevent zombie proxy processes.
    """
    proxy = subprocess.Popen(
        ["kubectl", "proxy", "--port=8001"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

    try:
        # Wait up to 3 seconds for the proxy to be ready
        health_url = f"http://127.0.0.1:8001/api/v1/nodes/{NODE_NAME}/proxy/healthz"
        for _ in range(30):
            try:
                if requests.get(health_url, timeout=0.2).status_code == 200:
                    break
            except Exception:
                pass
            time.sleep(0.1)

        # Submit the checkpoint request to the Kubelet
        api_url = (
            f"http://127.0.0.1:8001/api/v1/nodes/{NODE_NAME}/proxy"
            f"/checkpoint/default/{pod_name}/sidecar-guardian"
        )
        resp = requests.post(api_url, timeout=30)

        if resp.status_code != 200:
            print(f"❌ AGENT: Checkpoint API returned {resp.status_code}: {resp.text}", flush=True)
            return None

        data = resp.json()
        path = data["items"][0]
        print(f"✅ AGENT: Checkpoint created at {path}", flush=True)
        return path

    except Exception as e:
        print(f"❌ AGENT: Checkpoint exception: {e}", flush=True)
        return None

    finally:
        # Always terminate the proxy — this prevents the port 8001 zombie leak
        proxy.terminate()
        try:
            proxy.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proxy.kill()


# =============================================================================
# SECTION 5: DESTINATION-SIDE RELAY RECEIVER
# Polls for /tmp/relay_complete instead of running a gRPC server.
# This avoids the TOCTOU race where the agent could read a partial TAR file.
# =============================================================================

def relay_receiver_loop():
    """
    Runs in a background daemon thread. Polls for /tmp/relay_complete —
    the atomic trigger file written by relay_transfer.sh only AFTER the
    incoming TAR file has been fully transferred AND SHA256-verified.
    Once detected, triggers the Buildah + K8s restore sequence.
    """
    print("👂 AGENT: Receiver loop armed. Polling for /tmp/relay_complete...", flush=True)

    while True:
        if os.path.exists("/tmp/relay_complete"):
            print("\n📦 AGENT: relay_complete detected — restoring checkpoint!", flush=True)

            # Remove the trigger file first to avoid double-triggering
            try:
                os.remove("/tmp/relay_complete")
            except FileNotFoundError:
                pass

            checkpoint_path = "/tmp/checkpoint.tar"
            _rebuild_and_deploy(checkpoint_path)

        time.sleep(RELAY_POLL_INTERVAL)


def _rebuild_and_deploy(tar_path):
    """
    Converts the received TAR checkpoint into a running K8s Pod on this node.
    Uses Buildah to layer the CRIU memory pages on top of a blank image,
    then patches the Deployment to schedule on this node.
    """
    t_start = time.time()

    # ---- STEP 1: Buildah — reconstruct the container image ----
    print("🔨 AGENT [BUILDAH]: Reconstructing container from checkpoint...", flush=True)
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
    print(f"⏱️  AGENT: Buildah done in {time.time()-t_start:.2f}s", flush=True)

    # ---- STEP 2: K8s — reschedule the Pod on this node ----
    t0 = time.time()
    print("⚡ AGENT [K8S]: Patching Deployment to this node...", flush=True)
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
    subprocess.run("kubectl delete pod -l app=space-mission --force --grace-period=0",
                   shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    subprocess.run(f"kubectl patch deployment space-mission --type=strategic -p '{patch}'", shell=True)
    subprocess.run("kubectl scale deployment space-mission --replicas=1", shell=True)
    print(f"⏱️  AGENT: K8s patch done in {time.time()-t0:.2f}s", flush=True)

    # ---- STEP 3: Wait for pod to boot and signal it has landed ----
    t0 = time.time()
    pod_name = None
    for _ in range(50):
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

        if pod_name:
            # Touch /tmp/landed inside the Guardian container — this exits flightMode
            res = subprocess.run(
                f"kubectl exec {pod_name} -c sidecar-guardian -- touch /tmp/landed",
                shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL
            )
            if res.returncode == 0:
                print(f"🛬 AGENT: Pod landed and Guardian awakened in {time.time()-t0:.2f}s", flush=True)
                print(f"🎉 AGENT: Total restore time: {time.time()-t_start:.2f}s", flush=True)
                return

        time.sleep(0.1)

    print(f"❌ AGENT: Pod did not awaken within timeout ({time.time()-t0:.2f}s)", flush=True)


# =============================================================================
# SECTION 6: MAIN EVENT LOOP (Dual-Gate Telemetry Listener)
# =============================================================================

def main():
    print(f"🛸 NODE AGENT V6 ONLINE — satellite: {NODE_NAME}", flush=True)

    # ---- Connect to Floating Master Redis (for topology / Dijkstra) ----
    topology_redis = None
    while not topology_redis:
        try:
            r = redis.Redis(
                host=TOPOLOGY_REDIS_HOST,
                port=TOPOLOGY_REDIS_PORT,
                decode_responses=True,
                socket_connect_timeout=3
            )
            r.ping()
            topology_redis = r
            print("✅ AGENT: Connected to Floating Master Redis.", flush=True)
        except Exception:
            print("⏳ AGENT: Waiting for Floating Master...", flush=True)
            time.sleep(3)

    # Load the Dijkstra script into the Floating Master's script cache
    load_lua_script(topology_redis)

    # ---- Start the relay receiver in a background thread ----
    # This thread polls for /tmp/relay_complete (inbound checkpoint arrival)
    recv_thread = threading.Thread(target=relay_receiver_loop, daemon=True)
    recv_thread.start()

    # ---- Connect to Ground Redis (for our own telemetry subscription) ----
    last_pushed = {"temp": -999, "battery": -999, "last_time": 0}
    local_state = {}    # Most recent telemetry for this node
    migration_lock = threading.Lock()   # Prevent concurrent migration attempts

    channel = f"telemetry/{NODE_NAME}"
    print(f"📻 AGENT: Subscribing to {channel}...", flush=True)

    while True:
        try:
            ground_redis = redis.Redis(
                host=GROUND_REDIS_HOST,
                port=6379,
                decode_responses=True,
                socket_connect_timeout=3
            )
            ground_redis.ping()
            pubsub = ground_redis.pubsub()
            pubsub.subscribe(channel)
            print("✅ AGENT: Connected to Ground Redis. Listening for telemetry...", flush=True)

            # Heartbeat timer for last telemetry check
            for message in pubsub.listen():
                if message["type"] != "message":
                    continue

                # Parse the latest telemetry packet from the Digital Twin
                local_state = json.loads(message["data"])

                # ---- Push delta-encoded update to Floating Master ----
                last_pushed = push_telemetry_to_floating_master(
                    topology_redis, local_state, last_pushed
                )

                # ---- GATE 0: Am I hosting the payload? ----
                # If no pod is running here, nothing to migrate
                if not local_state.get("is_working", False):
                    continue

                # Prevent starting two migrations simultaneously
                if migration_lock.locked():
                    continue  # Migration already in progress

                # ---- TRIGGER A: Thermal Self-Preservation ----
                twin = VirtualSatellite(local_state)
                is_safe, reason = twin.predict_future(60)

                if not is_safe:
                    print(f"🌡️  TRIGGER A FIRED: {reason}", flush=True)
                    # Run migration in background thread so listener keeps ticking
                    threading.Thread(
                        target=_run_migration_with_lock,
                        args=(topology_redis, "thermal", migration_lock),
                        daemon=True
                    ).start()
                    continue   # Skip Trigger B check this cycle

                # ---- TRIGGER B: Lateral Fire Tracking ----
                # Query the Guardian for the worker's latest Center of Mass
                try:
                    resp = requests.get(f"{GUARDIAN_URL}/state", timeout=1)
                    if resp.status_code == 200:
                        worker_state = resp.json()
                        com = worker_state.get("center_of_mass", {})
                        com_x = com.get("x", GRID_W / 2)  # Default to centre

                        # Check if the fire has drifted to the edge of the swath
                        lateral_edge = (
                            com_x < LATERAL_THRESHOLD or
                            com_x > (GRID_W - LATERAL_THRESHOLD)
                        )
                        if lateral_edge:
                            print(
                                f"🔥 TRIGGER B FIRED: CoM_x={com_x:.1f} "
                                f"(threshold={LATERAL_THRESHOLD}px from edge)",
                                flush=True
                            )
                            threading.Thread(
                                target=_run_migration_with_lock,
                                args=(topology_redis, "lateral", migration_lock),
                                daemon=True
                            ).start()
                except requests.exceptions.RequestException:
                    pass  # Guardian temporarily unavailable — safe to skip this tick

        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            print(f"⚠️  AGENT: Ground Redis connection lost. Retrying in 3s...", flush=True)
            time.sleep(3)
        except Exception as e:
            print(f"❌ AGENT: Unexpected listener error: {e}", flush=True)
            time.sleep(3)


def _run_migration_with_lock(topology_redis, migration_type, lock):
    """
    Wrapper that acquires the migration lock before running the full migration
    sequence, then releases it when done. Prevents overlapping migrations.
    """
    acquired = lock.acquire(blocking=False)  # Non-blocking: skip if already locked
    if not acquired:
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
