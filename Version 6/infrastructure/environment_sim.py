import time
import json
import redis
import logging
import threading
from kubernetes import client, config, watch

# =============================================================================
#  SPACE CLOUD V6 - ENVIRONMENT SIMULATOR (Digital Twin, Pure Pub/Sub)
#  Role: Simulates orbital physics and broadcasts CURRENT hardware readings
#        via Redis Pub/Sub. Does NOT produce forecasts — in V6, each satellite's
#        local Node Agent runs its own VirtualSatellite predictive engine.
#
#  Key changes from V5:
#   - 'forecast' field REMOVED from the telemetry payload (§3.3).
#     Prediction is the Node Agent's responsibility. The Twin only reports
#     what the physical sensors measure RIGHT NOW.
#   - 'orbit_plane' field ADDED (§3.1). Used by the Lua Dijkstra script
#     to compute the orbital-plane bias for lateral fire-tracking migrations.
#   - Redis host renamed from 'system-redis' to 'ground-redis' (§1.3).
# =============================================================================

# --- PHYSICAL CONFIGURATION ---
ORBIT_PERIOD  = 300.0  # Time (s) for a full 360° orbit around Earth
ECLIPSE_START = 220    # Degrees where the satellite enters Earth's shadow
ECLIPSE_END   = 320    # Degrees where the satellite exits Earth's shadow

TEMP_SPACE       = -270.0  # Deep space background temperature (°C)
THERMAL_MASS     = 80.0    # Resistance of the satellite body to temperature changes
HEATING_SUN      = 100.0   # Heat gain from direct solar radiation
HEATING_CPU_IDLE = 10.0    # Heat gain from hardware in standby
HEATING_CPU_LOAD = 80.0    # Heat gain from heavy SAMKNN workload computation
COOLING_K        = 4.0     # Radiative cooling efficiency constant

BATTERY_CHARGE_RATE = 5.0  # Power gain per second from solar panels
BATTERY_DRAIN_IDLE  = 1.0  # Power consumption in standby
BATTERY_DRAIN_LOAD  = 2.5  # Power consumption during SAMKNN workload

# Setup logging for simulation monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [SIM] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("EnvironmentSim")


class Satellite:
    """
    Digital Twin of a physical satellite — models orbital mechanics, thermal
    dynamics, and energy balance. Publishes CURRENT state only; no forecasting.
    """

    def __init__(self, name, node_type, start_offset_deg=0, orbit_plane="A"):
        self.name        = name
        self.type        = node_type
        self.offset      = start_offset_deg   # Initial orbital position offset
        self.orbit_plane = orbit_plane         # Which orbital shell: "A", "B", or "C"
                                               # Used by Lua Dijkstra for lateral routing
        self.battery     = 100.0
        self.temp        = 20.0
        self.angle       = 0
        self.in_eclipse  = False
        self.is_working  = False  # True when a K8s Pod is running on this node

    def update(self, elapsed_time, has_workload):
        """Advances the physical simulation by one time step."""
        self.is_working = has_workload
        if self.type == 'ground':
            return  # Ground stations don't follow orbital physics

        # 1. Orbital Mechanics — advance orbital angle proportionally
        raw_angle   = ((elapsed_time % ORBIT_PERIOD) / ORBIT_PERIOD * 360.0 + self.offset)
        self.angle  = raw_angle % 360.0
        self.in_eclipse = (ECLIPSE_START <= self.angle <= ECLIPSE_END)

        # 2. Thermal Dynamics (Newton's Law of Cooling + Solar/Internal Heating)
        p_in  = HEATING_CPU_LOAD if self.is_working else HEATING_CPU_IDLE
        if not self.in_eclipse:
            p_in += HEATING_SUN   # Solar heating only applies in sunlight
        p_out = COOLING_K * (self.temp - TEMP_SPACE) * 0.1
        self.temp += (p_in - p_out) / THERMAL_MASS

        # 3. Energy Dynamics — charge from solar panels, drain from load
        charge      = BATTERY_CHARGE_RATE if not self.in_eclipse else 0.0
        drain       = BATTERY_DRAIN_LOAD if self.is_working else BATTERY_DRAIN_IDLE
        self.battery += (charge - drain)
        self.battery  = max(0.0, min(100.0, self.battery))  # Clamp to [0%, 100%]

    def get_telemetry(self):
        """
        Serializes the CURRENT physical state into a telemetry packet.

        NOTE — 'forecast' is intentionally ABSENT from this payload (§3.3).
        In V6, prediction is the exclusive responsibility of each Node Agent's
        local VirtualSatellite engine. The Digital Twin only reports sensor data.

        The 'orbit_plane' field is NEW in V6 — it allows the Lua Dijkstra
        script to distinguish trailing satellites (same plane) from laterally
        adjacent ones (different plane) when routing a Trigger B migration.
        """
        return {
            "type":        self.type,
            "battery":     round(self.battery, 1),
            "temp":        round(self.temp, 1),
            "angle":       int(self.angle),
            "eclipse":     self.in_eclipse,
            "is_working":  self.is_working,
            "orbit_plane": self.orbit_plane,   # NEW in V6 — "A", "B", or "C"
            # NOTE: 'forecast' field intentionally removed (§3.3)
            # The Node Agent predicts locally using its own VirtualSatellite twin
        }


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
# Issue #7 fix: K8s WATCH API for pod placement (replaces per-second polling)
# The old implementation called list_namespaced_pod() every second in the main
# loop, generating a full etcd read each time. The Watch API holds a single
# long-poll HTTP connection and receives push notifications when pods change.
# =============================================================================

# Thread-safe set of node names currently hosting the space-mission pod
_active_nodes = set()
_active_nodes_lock = threading.Lock()


def _pod_watcher_thread(v1_client):
    """
    Background thread that watches for space-mission pod events.
    Updates the shared _active_nodes set on every ADDED/MODIFIED/DELETED event.
    Automatically reconnects on watch timeout or API errors.
    """
    w = watch.Watch()
    while True:
        try:
            for event in w.stream(
                v1_client.list_namespaced_pod,
                namespace="default",
                label_selector="app=space-mission",
                timeout_seconds=60  # Reconnect every 60s to prevent stale watches
            ):
                pod = event["object"]
                event_type = event["type"]
                node_name = pod.spec.node_name
                phase = pod.status.phase if pod.status else None
                deleting = pod.metadata.deletion_timestamp is not None

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
    """Returns a snapshot of the currently active node set (thread-safe)."""
    with _active_nodes_lock:
        return set(_active_nodes)


def main():
    print("\n🌍 ENVIRONMENT SIMULATOR V6 ONLINE (Forecast-Free, Orbit-Plane-Aware).")
    k8s_api  = connect_k8s()
    redis_db = connect_redis()

    # Issue #7: Start the K8s Watch thread for event-driven pod tracking.
    # This replaces the per-second list_namespaced_pod() call that was
    # stressing the K8s control plane (etcd) with unnecessary reads.
    if k8s_api:
        watcher = threading.Thread(target=_pod_watcher_thread, args=(k8s_api,), daemon=True)
        watcher.start()
        print("👁️  WATCH: Pod watcher thread started (event-driven, no polling).")
    else:
        print("⚠️  WATCH: K8s API unavailable — workload detection disabled.")

    # Define the fleet: 1 ground station + 3 satellites in separate orbital planes.
    # Each satellite gets a different orbit_plane label ("A", "B", "C") so the
    # Lua Dijkstra script can distinguish trailing vs. lateral neighbours
    # during Trigger B (lateral fire-tracking) migrations.
    fleet = [
        Satellite("minikube",     "ground"),
        Satellite("minikube-m02", "satellite", start_offset_deg=0,   orbit_plane="A"),
        Satellite("minikube-m03", "satellite", start_offset_deg=120, orbit_plane="B"),
        Satellite("minikube-m04", "satellite", start_offset_deg=240, orbit_plane="C"),
    ]

    start_time = time.time()

    while True:
        # Issue #7: Read from the event-driven watcher instead of polling the API
        active_nodes = get_active_nodes()
        elapsed = time.time() - start_time

        console_log = "\r"

        # Step 2: Update physics and broadcast each satellite's current telemetry
        for sat in fleet:
            is_working = (sat.name in active_nodes)
            sat.update(elapsed, is_working)
            sat_data = sat.get_telemetry()

            # --- V6 PURE PUBLISH — current sensor state only ---
            # Each Node Agent subscribes only to its OWN channel (telemetry/<name>)
            # to mirror the physical reality of an onboard thermistor.
            # The Guardian subscribes to telemetry/* for the fleet dashboard overview.
            if redis_db:
                try:
                    channel = f"telemetry/{sat.name}"
                    redis_db.publish(channel, json.dumps(sat_data))
                except Exception:
                    redis_db = connect_redis()  # Auto-reconnect if broker drops

            # Visual console feedback
            status_icon = "🔥" if is_working else ("🌑" if sat.in_eclipse else "☀️")
            console_log += f"[{sat.name[-3:]} {int(sat.battery)}% {int(sat.temp)}° {status_icon}] "

        print(console_log, end="", flush=True)
        time.sleep(1.0)  # Simulation runs at 1 Hz


if __name__ == "__main__":
    main()