"""
===============================================================================
SPACE CLOUD V6 - ENVIRONMENT SIMULATOR (DIGITAL TWIN)
===============================================================================

🧠 HIGH-LEVEL PURPOSE:
This script simulates a fleet of satellites and a ground station.

It acts as a **Digital Twin**, meaning:
→ It models the REAL physical behavior of satellites (temperature, battery, orbit)
→ It continuously publishes CURRENT telemetry (NO predictions)

-------------------------------------------------------------------------------
🎯 WHAT THIS COMPONENT DOES:

1. Simulates:
   - Orbital motion (angle around Earth)
   - Thermal dynamics (heating + cooling)
   - Battery behavior (charging + draining)

2. Detects workload placement:
   - Uses Kubernetes API to know where pods are running
   - If a satellite hosts a workload → it heats up and drains battery faster

3. Publishes telemetry:
   - Sends real-time data to Redis (Pub/Sub)
   - Each satellite publishes to its own channel:
        telemetry/<satellite_name>

-------------------------------------------------------------------------------
⚠️ IMPORTANT DESIGN CHOICES (V6):

- ❌ NO forecasting
  → Predictions are handled locally by each node (Node Agent)

- ✅ PURE PUB/SUB
  → This component only broadcasts current state

- ✅ EVENT-DRIVEN K8s WATCH
  → Instead of polling Kubernetes every second (inefficient),
    we use a streaming API that pushes updates

-------------------------------------------------------------------------------
"""

import time
import json
import redis
import logging
import threading
from kubernetes import client, config, watch


# =============================================================================
# PHYSICAL CONFIGURATION (SIMULATION CONSTANTS)
# =============================================================================

# Orbital timing
ORBIT_PERIOD  = 120.0  # seconds for full orbit (360°)

# Eclipse region (when satellite is in Earth's shadow → no solar power)
ECLIPSE_START = 220
ECLIPSE_END   = 320

# Thermal physics constants
TEMP_SPACE       = -270.0  # Deep space temperature (°C)
THERMAL_MASS     = 40.0    # Resistance to temperature change
HEATING_SUN      = 100.0   # Heat from sun
HEATING_CPU_IDLE = 10.0    # Heat when idle
HEATING_CPU_LOAD = 85.0    # Heat under heavy workload
COOLING_K        = 4.0     # Cooling efficiency constant

# Battery dynamics
BATTERY_CHARGE_RATE = 5.0
BATTERY_DRAIN_IDLE  = 1.0
BATTERY_DRAIN_LOAD  = 2.5


# -----------------------------------------------------------------------------
# Logging setup (used instead of print for structured logs)
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [SIM] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("EnvironmentSim")


# =============================================================================
# SATELLITE CLASS (DIGITAL TWIN MODEL)
# =============================================================================

class Satellite:
    """
    🧠 PURPOSE:
    Represents a single satellite (or ground station) in the simulation.

    Each instance:
    - Tracks its own physical state
    - Updates itself over time
    - Produces telemetry data
    """

    def __init__(self, name, node_type, start_offset_deg=0, orbit_plane="A"):
        """
        🧠 Initializes satellite state.

        PARAMETERS:
        - name: Kubernetes node name
        - node_type: 'satellite' or 'ground'
        - start_offset_deg: initial orbital position offset
        - orbit_plane: orbital shell identifier (A, B, C)
        """

        self.name        = name
        self.type        = node_type
        self.offset      = start_offset_deg

        # Used for routing decisions (multi-orbit system)
        self.orbit_plane = orbit_plane

        # Initial physical state
        self.battery     = 100.0
        self.temp        = 20.0
        self.angle       = 0

        # State flags
        self.in_eclipse  = False
        self.is_working  = False  # True if running a workload

    def update(self, elapsed_time, has_workload):
        """
        🧠 PURPOSE:
        Advances simulation by ONE time step.

        This function models:
        1. Orbital movement
        2. Temperature changes
        3. Battery dynamics
        """

        # Update workload status
        self.is_working = has_workload

        # Ground nodes do not orbit → skip physics
        if self.type == 'ground':
            return

        # ---------------------------------------------------------------------
        # 1. ORBITAL MECHANICS
        # ---------------------------------------------------------------------

        # Compute angle based on elapsed time
        raw_angle = (
            (elapsed_time % ORBIT_PERIOD) / ORBIT_PERIOD * 360.0
            + self.offset
        )

        # Normalize angle to [0, 360)
        self.angle = raw_angle % 360.0

        # Determine if satellite is in Earth's shadow
        self.in_eclipse = (ECLIPSE_START <= self.angle <= ECLIPSE_END)

        # ---------------------------------------------------------------------
        # 2. THERMAL DYNAMICS
        # ---------------------------------------------------------------------

        # Heat input depends on workload
        p_in = HEATING_CPU_LOAD if self.is_working else HEATING_CPU_IDLE

        # Add solar heating if not in eclipse
        if not self.in_eclipse:
            p_in += HEATING_SUN

        # Cooling proportional to temperature difference
        # ⚠️ Newton-like cooling model
        p_out = COOLING_K * (self.temp - TEMP_SPACE) * 0.1

        # Update temperature
        self.temp += (p_in - p_out) / THERMAL_MASS

        # ---------------------------------------------------------------------
        # 3. ENERGY DYNAMICS (BATTERY)
        # ---------------------------------------------------------------------

        # Charging only when in sunlight
        charge = BATTERY_CHARGE_RATE if not self.in_eclipse else 0.0

        # Drain depends on workload
        drain = BATTERY_DRAIN_LOAD if self.is_working else BATTERY_DRAIN_IDLE

        # Update battery level
        self.battery += (charge - drain)

        # Clamp battery between 0% and 100%
        self.battery = max(0.0, min(100.0, self.battery))

    def get_telemetry(self):
        """
        🧠 PURPOSE:
        Converts internal state into a telemetry dictionary.

        IMPORTANT:
        - Only CURRENT values are reported
        - NO forecasting
        """

        return {
            "type":        self.type,
            "battery":     round(self.battery, 1),  # Rounded for readability
            "temp":        round(self.temp, 1),
            "angle":       int(self.angle),
            "eclipse":     self.in_eclipse,
            "is_working":  self.is_working,
            "orbit_plane": self.orbit_plane,
        }


# =============================================================================
# CONNECTION HELPERS
# =============================================================================

def connect_k8s():
    """
    🧠 Connects to Kubernetes API.

    Returns:
    - CoreV1Api client if successful
    - None otherwise
    """
    try:
        config.load_kube_config()
        return client.CoreV1Api()
    except Exception:
        logger.warning("Unable to connect to K8s. Retrying...")
        return None


def connect_redis():
    """
    🧠 Connects to Redis message broker.

    Redis is used for:
    - Pub/Sub messaging
    - Real-time telemetry distribution
    """

    try:
        r = redis.Redis(
            host='ground-redis',
            port=6379,
            decode_responses=True,
            socket_connect_timeout=1
        )

        r.ping()  # ⚠️ Forces connection test
        return r

    except Exception:
        return None


# =============================================================================
# KUBERNETES WATCH SYSTEM (EVENT-DRIVEN)
# =============================================================================

# Shared set of active nodes running workloads
_active_nodes = set()

# Lock for thread-safe access
_active_nodes_lock = threading.Lock()


def _pod_watcher_thread(v1_client):
    """
    🧠 PURPOSE:
    Background thread that listens to Kubernetes events.

    Instead of polling:
        every second → list all pods (expensive)

    We use:
        watch API → event stream (efficient)
    """

    w = watch.Watch()

    while True:
        try:
            # Stream events from Kubernetes
            for event in w.stream(
                v1_client.list_namespaced_pod,
                namespace="default",
                label_selector="app=space-mission",
                timeout_seconds=60
            ):
                pod = event["object"]
                event_type = event["type"]

                node_name = pod.spec.node_name

                # ⚠️ pod.status might be None (edge case)
                phase = pod.status.phase if pod.status else None

                deleting = pod.metadata.deletion_timestamp is not None

                # Thread-safe update
                with _active_nodes_lock:

                    if event_type in ("ADDED", "MODIFIED"):

                        if phase in ("Running", "Pending") and not deleting and node_name:
                            _active_nodes.add(node_name)

                        elif node_name and node_name in _active_nodes:
                            _active_nodes.discard(node_name)

                    elif event_type == "DELETED":
                        if node_name:
                            _active_nodes.discard(node_name)

        except Exception as e:
            logger.warning(f"Pod watcher error: {e}. Reconnecting in 3s...")
            time.sleep(3)


def get_active_nodes():
    """
    🧠 Returns a COPY of active nodes set (thread-safe).
    """
    with _active_nodes_lock:
        return set(_active_nodes)  # ⚠️ return copy, not reference


# =============================================================================
# MAIN LOOP
# =============================================================================

def main():
    """
    🧠 SYSTEM ORCHESTRATOR:

    1. Connect to K8s and Redis
    2. Start watcher thread
    3. Initialize satellite fleet
    4. Run simulation loop:
        - update physics
        - publish telemetry
    """

    print("\n🌍 ENVIRONMENT SIMULATOR V6 ONLINE (Forecast-Free, Orbit-Plane-Aware).")

    k8s_api  = connect_k8s()
    redis_db = connect_redis()

    # Start watcher thread if K8s available
    if k8s_api:
        watcher = threading.Thread(
            target=_pod_watcher_thread,
            args=(k8s_api,),
            daemon=True  # ⚠️ dies automatically with main thread
        )
        watcher.start()

        print("👁️  WATCH: Pod watcher thread started (event-driven, no polling).")
    else:
        print("⚠️  WATCH: K8s API unavailable — workload detection disabled.")

    # -------------------------------------------------------------------------
    # Define satellite fleet
    # -------------------------------------------------------------------------
    fleet = [
        Satellite("minikube",     "ground"),
        Satellite("minikube-m02", "satellite", start_offset_deg=0,   orbit_plane="A"),
        Satellite("minikube-m03", "satellite", start_offset_deg=120, orbit_plane="B"),
        Satellite("minikube-m04", "satellite", start_offset_deg=240, orbit_plane="C"),
    ]

    start_time = time.time()

    # -------------------------------------------------------------------------
    # MAIN SIMULATION LOOP
    # -------------------------------------------------------------------------
    while True:

        # Get current active nodes from watcher
        active_nodes = get_active_nodes()

        # Compute elapsed time
        elapsed = time.time() - start_time

        console_log = "\r"

        for sat in fleet:

            # Determine if satellite is running workload
            is_working = (sat.name in active_nodes)

            # Update physics
            sat.update(elapsed, is_working)

            # Generate telemetry
            sat_data = sat.get_telemetry()

            # -----------------------------------------------------------------
            # REDIS PUBLISH (PUB/SUB)
            # -----------------------------------------------------------------
            if redis_db:
                try:
                    channel = f"telemetry/{sat.name}"
                    redis_db.publish(channel, json.dumps(sat_data))
                except Exception:
                    redis_db = connect_redis()  # reconnect if needed

            # Console visualization
            status_icon = (
                "🔥" if is_working
                else ("🌑" if sat.in_eclipse else "☀️")
            )

            console_log += (
                f"[{sat.name[-3:]} "
                f"{int(sat.battery)}% "
                f"{int(sat.temp)}° "
                f"{status_icon}] "
            )

        print(console_log, end="", flush=True)

        # Simulation tick rate (1 Hz)
        time.sleep(1.0)


# -----------------------------------------------------------------------------
# Entry point
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    main()