import time
import json
import os
import sys
import redis
import logging
import threading

# =============================================================================
#  MANUAL OVERRIDE STATE
#  A thread-safe registry mapping node names to operator-injected values.
#  { "minikube-m02": {"temp": 130.0, "battery": None}, ... }
#  None = "don't override this parameter, keep physics"
# =============================================================================
_overrides = {}
_overrides_lock = threading.Lock()
from kubernetes import client, config, watch

# =============================================================================
#  SPACE CLOUD V6 - ENVIRONMENT SIMULATOR (The "Simulation Oracle")
#  Role: Simulates orbital physics and acts as the "God's Eye" Oracle for the
#        Delay-Tolerant Network (DTN). Because edge nodes cannot reliably reach
#        the K8s API during simulated blackouts, this script monitors workload
#        placement and explicitly injects 'is_working' and 'has_master' flags
#        into the telemetry payload.
# =============================================================================

# --- PHYSICAL CONFIGURATION ---
ORBIT_PERIOD  = 300.0  # Time (s) for a full 360° orbit around Earth
ECLIPSE_START = 220    # Degrees where the satellite enters Earth's shadow
ECLIPSE_END   = 320    # Degrees where the satellite exits Earth's shadow

TEMP_SPACE = -270.0  # Deep space background temperature (°C)
THERMAL_MASS = 180.0  # Resistance of the satellite body to temperature changes
HEATING_SUN = 100.0  # Heat gain from direct solar radiation
HEATING_CPU_IDLE = 10.0  # Heat gain from hardware in standby

# --- DUAL WORKLOAD THERMAL CONSTANTS ---
HEATING_SML_LOAD = 10.0  # Massive matrix multiplications
HEATING_MASTER_LOAD = 10.0  # Graph database & Lua script execution

COOLING_K = 4.0  # Radiative cooling efficiency constant

BATTERY_CHARGE_RATE = 5.0  # Power gain per second from solar panels
BATTERY_DRAIN_IDLE_SUN = 0.1  # Power consumption in standby (sunlight)
BATTERY_DRAIN_IDLE_ECLIPSE = 0.1  # Deep sleep hibernation in eclipse

# Differentiated payload drain (Sun vs. Eclipse)
BATTERY_DRAIN_SML_SUN = 0.1
BATTERY_DRAIN_SML_ECLIPSE = 0.1    # Higher drain: requires active heaters in the dark
BATTERY_DRAIN_MASTER_SUN = 0.1
BATTERY_DRAIN_MASTER_ECLIPSE = 0.1 # Moderate heater overhead

# Setup logging for simulation monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [SIM] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("EnvironmentSim")


class Satellite:
    """
    Digital Twin of a physical satellite — models orbital mechanics, thermal
    dynamics, and energy balance. Publishes CURRENT state only.
    """

    def __init__(self, name, node_type, start_offset_deg=0, orbit_plane="A"):
        self.name = name
        self.type = node_type
        self.offset = start_offset_deg
        self.orbit_plane = orbit_plane
        self.battery = 100.0
        self.temp = 20.0
        self.angle = 0
        self.in_eclipse = False
        self.has_sml = False
        self.has_master = False

    def update(self, elapsed_time, has_sml, has_master):
        """Advances the physical simulation by one time step."""
        self.has_sml = has_sml
        self.has_master = has_master
        if self.type == 'ground':
            return  # Ground stations don't follow orbital physics

        # 1. Orbital Mechanics — advance orbital angle proportionally
        raw_angle   = ((elapsed_time % ORBIT_PERIOD) / ORBIT_PERIOD * 360.0 + self.offset)
        self.angle  = raw_angle % 360.0
        self.in_eclipse = (ECLIPSE_START <= self.angle <= ECLIPSE_END)

        # 2. Thermal Dynamics (Dual Workload Calculation)
        p_in = HEATING_CPU_IDLE
        if self.has_sml:
            p_in += HEATING_SML_LOAD
        if self.has_master:
            p_in += HEATING_MASTER_LOAD

        if not self.in_eclipse:
            p_in += HEATING_SUN   # Solar heating only applies in sunlight

        p_out = COOLING_K * (self.temp - TEMP_SPACE) * 0.1
        self.temp += (p_in - p_out) / THERMAL_MASS

        # 3. Energy Dynamics
        if self.in_eclipse:
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

    def get_telemetry(self):
        """Serializes the CURRENT physical state."""
        return {
            "type": self.type,
            "battery": round(self.battery, 1),
            "temp": round(self.temp, 1),
            "angle": int(self.angle),
            "eclipse": self.in_eclipse,
            "is_working": self.has_sml,  # Flag for SAMKNN Payload
            "has_master": self.has_master,  # Flag for Topology Master
            "orbit_plane": self.orbit_plane,
        }


# =============================================================================
# KUBERNETES CONNECTION & WATCHER (The Oracle)
# =============================================================================

def connect_k8s():
    """Initializes connection to the local Kubernetes API (Minikube)."""
    try:
        config.load_kube_config()
        return client.CoreV1Api()
    except Exception:
        logger.warning("Unable to connect to K8s. Retrying...")
        return None


def connect_redis():
    """
    Connects to the Ground Station Redis message broker.
    In V6 the service is renamed from 'system-redis' to 'ground-redis' (§1.3)
    to distinguish it from the Floating Master Redis topology store.
    """
    try:
        r = redis.Redis(
            host='ground-redis',   # V6: renamed from 'system-redis'
            port=6379,
            decode_responses=True,
            socket_connect_timeout=1
        )
        r.ping()
        return r
    except Exception:
        return None


# =============================================================================
# OVERRIDE COMMAND LISTENER
# Runs in a daemon thread. Subscribes to 'override/commands' and updates the
# global override registry. Non-blocking relative to the main simulation loop.
# =============================================================================

def _override_listener_thread():
    """
    Listens for operator override commands published by the Dashboard backend.

    Command shape:
      { "action": "set"|"release", "node": str,
        "temp": float|null, "battery": float|null }

    On "set"   → inject values into _overrides; Oracle skips physics for that node.
    On "release" → remove entry; Oracle resumes physics calculations.
    """
    global _overrides
    while True:
        try:
            r = redis.Redis(
                host='ground-redis',
                port=6379,
                decode_responses=True,
                socket_connect_timeout=3
            )
            r.ping()
            pubsub = r.pubsub()
            pubsub.subscribe("override/commands")
            logger.info("Override listener subscribed to override/commands.")

            for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                try:
                    cmd = json.loads(message["data"])
                except (json.JSONDecodeError, TypeError):
                    logger.warning("Override listener: malformed command received.")
                    continue

                node   = cmd.get("node", "")
                action = cmd.get("action", "")

                with _overrides_lock:
                    if action == "set" and node:
                        _overrides[node] = {
                            "temp":    cmd.get("temp"),     # None = keep physics
                            "battery": cmd.get("battery"),  # None = keep physics
                        }
                        logger.info(
                            f"OVERRIDE SET: {node} → "
                            f"temp={cmd.get('temp')}, battery={cmd.get('battery')}"
                        )
                    elif action == "release" and node:
                        _overrides.pop(node, None)
                        logger.info(f"OVERRIDE RELEASED: {node} → returning to physics")

        except Exception as e:
            logger.warning(f"Override listener error: {e}. Reconnecting in 3s...")
            time.sleep(3)


# =============================================================================
# K8s WATCH API (Dual-Workload Tracking)
# =============================================================================

_active_sml_nodes = set()
_master_node = None
_active_nodes_lock = threading.Lock()


def _pod_watcher_thread(v1_client):
    """
    Maintains a persistent watch on the Kubernetes API.
    Identifies exact physical placement of the SML Payload and Topology Master.
    Filters out terminating pods to prevent the "Phantom Limb" bug.
    """
    global _master_node
    w = watch.Watch()

    while True:
        try:
            for event in w.stream(
                    v1_client.list_namespaced_pod,
                    namespace="default",
                    label_selector="app in (space-mission, topology-master)",
                    timeout_seconds=60
            ):
                pod = event["object"]
                event_type = event["type"]
                node_name = pod.spec.node_name
                phase = pod.status.phase if pod.status else None
                deleting = pod.metadata.deletion_timestamp is not None
                app_label = pod.metadata.labels.get("app") if pod.metadata.labels else None

                with _active_nodes_lock:
                    if event_type in ("ADDED", "MODIFIED"):
                        # Only register active, healthy workloads
                        if phase in ("Running", "Pending") and not deleting and node_name:
                            if app_label == "space-mission":
                                _active_sml_nodes.add(node_name)
                            elif app_label == "topology-master":
                                _master_node = node_name
                        # If a pod is terminating/completed, wipe it from our state
                        elif node_name:
                            if app_label == "space-mission" and node_name in _active_sml_nodes:
                                _active_sml_nodes.discard(node_name)
                            elif app_label == "topology-master" and _master_node == node_name:
                                _master_node = None

                    elif event_type == "DELETED":
                        if node_name:
                            if app_label == "space-mission":
                                _active_sml_nodes.discard(node_name)
                            elif app_label == "topology-master" and _master_node == node_name:
                                _master_node = None

        except Exception as e:
            logger.warning(f"Pod watcher error: {e}. Reconnecting in 3s...")
            time.sleep(3)


def get_active_workloads():
    with _active_nodes_lock:
        return set(_active_sml_nodes), _master_node


# =============================================================================
# MAIN EVENT LOOP
# =============================================================================

def main():
    # =========================================================================
    # CAMPAIGN MODE FAIL-SAFE (Phase 2, Step 2.3)
    # If CAMPAIGN_MODE is active, the simulator must be entirely shut down.
    # The Campaign Orchestrator's "Ghost Publisher" assumes total control of
    # the telemetry/{NODE_NAME} Redis channels. If left running, this
    # simulator's 1.0Hz physics loop would continuously overwrite the
    # Orchestrator's deterministic flatlined baseline and targeted thermal
    # injections, destroying the statistical validity of the DoE campaign.
    # =========================================================================
    if os.getenv("CAMPAIGN_MODE", "False") == "True":
        print("⚠️ ENVIRONMENT SIM [CAMPAIGN MODE]: Campaign Orchestrator (Ghost Publisher) is assuming "
              "total control of the telemetry bus. Environment Simulator deactivated.", flush=True)
        sys.exit(0)

    print("\n🌍 ENVIRONMENT SIMULATOR V6 ONLINE (Simulation Oracle Active).")
    k8s_api = connect_k8s()
    redis_db = connect_redis()

    if k8s_api:
        watcher = threading.Thread(target=_pod_watcher_thread, args=(k8s_api,), daemon=True)
        watcher.start()
        print("👁️  WATCH: K8s Oracle Pod Watcher thread started.")
    else:
        print("⚠️  WATCH: K8s API unavailable — workload detection disabled.")

    # Start the Manual Override command listener (daemon — never blocks the sim loop)
    # Runs unconditionally: override control is independent of K8s availability.
    override_listener = threading.Thread(target=_override_listener_thread, daemon=True)
    override_listener.start()
    print("🎮 OVERRIDE: Telemetry override command listener started.")

    fleet = [
        Satellite("minikube", "ground"),
        Satellite("minikube-m02", "satellite", start_offset_deg=0, orbit_plane="A"),
        Satellite("minikube-m03", "satellite", start_offset_deg=45, orbit_plane="B"),
        Satellite("minikube-m04", "satellite", start_offset_deg=90, orbit_plane="C"),
    ]

    start_time = time.time()

    while True:
        active_sml, master_node = get_active_workloads()
        elapsed = time.time() - start_time
        console_log = "\r"

        for sat in fleet:
            has_sml = (sat.name in active_sml)
            has_master = (sat.name == master_node)

            # ── MANUAL OVERRIDE CHECK ──────────────────────────────────────────
            # If an operator has injected values for this node, bypass physics
            # and broadcast those values directly. Orbital mechanics (angle,
            # eclipse) still advance so the globe visualization stays dynamic.
            with _overrides_lock:
                override = dict(_overrides.get(sat.name, {}))  # snapshot, release lock fast

            if override:
                # Advance orbital angle only (no thermal/energy physics)
                raw_angle  = ((elapsed % ORBIT_PERIOD) / ORBIT_PERIOD * 360.0 + sat.offset)
                sat.angle  = raw_angle % 360.0
                sat.in_eclipse = (ECLIPSE_START <= sat.angle <= ECLIPSE_END)
                sat.has_sml    = has_sml
                sat.has_master = has_master
                # Inject operator values (only for non-None parameters)
                if override.get("temp") is not None:
                    sat.temp = float(override["temp"])
                if override.get("battery") is not None:
                    sat.battery = max(0.0, min(100.0, float(override["battery"])))
            else:
                # Normal automated physics path (unchanged)
                sat.update(elapsed, has_sml, has_master)

            sat_data = sat.get_telemetry()

            # Publish oracle data to the telemetry bus
            if redis_db:
                try:
                    channel = f"telemetry/{sat.name}"
                    redis_db.publish(channel, json.dumps(sat_data))
                except Exception:
                    redis_db = connect_redis()

            # Dynamic icon based on primary workload (⚡ marks overridden nodes)
            override_icon = "⚡" if override else ""
            status_icon = "🔥" if has_sml else ("🧠" if has_master else ("🌑" if sat.in_eclipse else "☀️"))
            console_log += f"[{sat.name[-3:]} {int(sat.battery)}% {int(sat.temp)}° {status_icon}{override_icon}] "

        print(console_log, end="", flush=True)
        time.sleep(1.0)


if __name__ == "__main__":
    main()
