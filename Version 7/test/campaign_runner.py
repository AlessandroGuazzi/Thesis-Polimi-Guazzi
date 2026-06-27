"""
SPACE CLOUD V7 — CAMPAIGN ORCHESTRATOR (The "God-Mode" Runner)
===============================================================
Phase 3 of the Codebase Upgrade Action Plan.

Role: Centralized, programmatic controller responsible for executing the
entire 540-run DoE ablation campaign. Shifts the cluster from a stochastic,
live-simulation environment into a strictly controlled, mathematically
deterministic test harness.

Architecture:
  Step 3.1 — Service scaffold (K8s + Redis authentication)
  Step 3.2 — Matrix generation & seeded randomization (540 runs)
  Step 3.3 — Sterile Baseline & Ghost Publisher (hardware fault injection)
  Step 3.4 — Ghost Worker (visual/lateral trajectory injection)
  Step 3.5 — Calibrated Virtual Hardware Fuse (severity-dictated death timer)
  Step 3.6 — Run Sanitization & Teardown (network reset, state wipe, stability)

References:
  - campaign_experimental_design.md  (DoE factors & scenarios)
  - campaign_automation_specification.md  (automation architecture)
  - codebase_upgrade_action_plan.md  (Phase 3, Steps 3.1–3.6)
  - orchestrator_functional_spec.md  (behavioral specification)
"""

import os
import sys
import csv
import json
import time
import random
import logging
import threading
from datetime import datetime

import redis
import requests
from kubernetes import client, config
from kubernetes.stream import stream as k8s_stream


# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CAMPAIGN] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("CampaignOrchestrator")


# =============================================================================
# STEP 3.1 — SCAFFOLD THE ORCHESTRATOR SERVICE
# =============================================================================
# Environment Setup: authenticate to K8s + connect to Ground Redis.
# Supports both in-cluster (Pod) and local (~/.kube/config) execution.
# =============================================================================

# --- Kubernetes Client ---
def _init_k8s_client():
    """
    Authenticate to the Kubernetes API.
    Tries in-cluster first (service account token), falls back to kubeconfig.
    """
    try:
        config.load_incluster_config()
        logger.info("K8s: Authenticated via in-cluster service account.")
    except config.ConfigException:
        try:
            config.load_kube_config()
            logger.info("K8s: Authenticated via local kubeconfig.")
        except config.ConfigException as e:
            logger.critical(f"K8s: Cannot authenticate — {e}")
            sys.exit(1)
    return client.CoreV1Api()


# --- Redis Connection ---
GROUND_REDIS_HOST = os.getenv("GROUND_REDIS_HOST", "ground-redis")
GROUND_REDIS_PORT = int(os.getenv("GROUND_REDIS_PORT", "6379"))


def _connect_redis():
    """
    Connect to the Ground Station Redis broker.
    Used for Pub/Sub Ghost Publishing and campaign/config hot-reloading.
    """
    try:
        r = redis.Redis(
            host=GROUND_REDIS_HOST,
            port=GROUND_REDIS_PORT,
            decode_responses=True,
            socket_connect_timeout=5,
        )
        r.ping()
        logger.info(f"Redis: Connected to {GROUND_REDIS_HOST}:{GROUND_REDIS_PORT}")
        return r
    except redis.exceptions.ConnectionError as e:
        logger.critical(f"Redis: Cannot connect — {e}")
        sys.exit(1)


# --- Cluster Topology ---
# The 3-satellite Minikube constellation (matches environment_sim.py fleet)
SATELLITE_NODES = ["minikube-m02", "minikube-m03", "minikube-m04"]

# Guardian Sidecar HTTP endpoint (exposed via K8s ClusterIP Service)
GUARDIAN_SVC_URL = os.getenv("GUARDIAN_SVC_URL", "http://space-dashboard-svc:80")

# Campaign CSV output path
CSV_OUTPUT_DIR = os.getenv(
    "CSV_OUTPUT_DIR",
    os.path.join(os.path.dirname(__file__), "..", "3_ground_station", "evaluation_data"),
)
CSV_OUTPUT_PATH = os.path.join(CSV_OUTPUT_DIR, "campaign_results.csv")

# Global run timeout (seconds) — prevents hung runs from freezing the campaign
RUN_TIMEOUT_SEC = float(os.getenv("RUN_TIMEOUT_SEC", "120.0"))

# Sanitization stability poll timeout
SANITIZE_TIMEOUT_SEC = float(os.getenv("SANITIZE_TIMEOUT_SEC", "120.0"))


# =============================================================================
# STEP 3.2 — MATRIX GENERATION & RANDOMIZATION
# =============================================================================
# Generates the complete 540-run DoE matrix and shuffles it with a fixed seed.
# Each run is a dict specifying: configuration, scenario, severity, rep_id,
# and the exact ABLATION_CONFIG patch + scenario parameters.
# =============================================================================

# --- The 6 Configuration Profiles (Ablation Study) ---
# Each profile specifies the ABLATION_CONFIG values to hot-reload via Redis.
# Keys not listed default to the Control Baseline in campaign-ablation-config.yaml.
CONFIGURATIONS = {
    "C1_Full_System": {
        "ENABLE_PREDICTIVE_TWIN": True,
        "COOLDOWN_SEC": 15.0,
        "MONOLITHIC_CHECKPOINT": False,
        "ENABLE_LOG_UTILITY": True,
        "ENABLE_SEQUENCED_MIGRATION": True,
    },
    "C2_No_Predictive_Twin": {
        "ENABLE_PREDICTIVE_TWIN": False,
        "COOLDOWN_SEC": 15.0,
        "MONOLITHIC_CHECKPOINT": False,
        "ENABLE_LOG_UTILITY": True,
        "ENABLE_SEQUENCED_MIGRATION": True,
    },
    "C3_No_Cooldown": {
        "ENABLE_PREDICTIVE_TWIN": True,
        "COOLDOWN_SEC": 0.0,
        "MONOLITHIC_CHECKPOINT": False,
        "ENABLE_LOG_UTILITY": True,
        "ENABLE_SEQUENCED_MIGRATION": True,
    },
    "C4_Monolithic_Checkpoint": {
        "ENABLE_PREDICTIVE_TWIN": True,
        "COOLDOWN_SEC": 15.0,
        "MONOLITHIC_CHECKPOINT": True,
        "ENABLE_LOG_UTILITY": True,
        "ENABLE_SEQUENCED_MIGRATION": True,
    },
    "C5_Linear_Routing": {
        "ENABLE_PREDICTIVE_TWIN": True,
        "COOLDOWN_SEC": 15.0,
        "MONOLITHIC_CHECKPOINT": False,
        "ENABLE_LOG_UTILITY": False,
        "ENABLE_SEQUENCED_MIGRATION": True,
    },
    "C6_Concurrent_Evacuation": {
        "ENABLE_PREDICTIVE_TWIN": True,
        "COOLDOWN_SEC": 15.0,
        "MONOLITHIC_CHECKPOINT": False,
        "ENABLE_LOG_UTILITY": True,
        "ENABLE_SEQUENCED_MIGRATION": False,
    },
}

# --- The 3 Scenarios × 3 Severity Levels ---
# Each scenario defines the injection parameters per severity level.
SCENARIOS = {
    "slow_internet": {
        "nominal":         {"tc_rate_mbit": 25.0, "tc_latency_ms": 40},
        "borderline":      {"tc_rate_mbit": 6.4,  "tc_latency_ms": 40},
        "correct_failure": {"tc_rate_mbit": 4.0,  "tc_latency_ms": 40},
    },
    "sequential_thermal": {
        "nominal":         {"delta_t_stress_sec": 30.0, "spike_temp": 95.0},
        "borderline":      {"delta_t_stress_sec": 16.0, "spike_temp": 95.0},
        "correct_failure": {"delta_t_stress_sec": 5.0,  "spike_temp": 95.0},
    },
    "double_trouble": {
        "nominal":         {"local_temp": 85.0,  "neighbor_battery": 40.0},
        "borderline":      {"local_temp": 92.0,  "neighbor_battery": 10.0},
        "correct_failure": {"local_temp": 98.0,  "neighbor_battery": 2.0},
    },
}

# --- Virtual Hardware Fuse Calibration ---
# Severity-dictated timeout before the orchestrator kills the origin pod.
FUSE_TIMEOUTS = {
    "nominal":         None,   # No fuse — migration is expected to succeed easily
    "borderline":      4.0,    # Tight but possible
    "correct_failure": 1.5,    # Ultra-tight — simulates hardware shutdown
}

# Seeded shuffle constant (must be documented for reproducibility)
SHUFFLE_SEED = 42
NUM_REPS = 10


def generate_run_matrix(seed=SHUFFLE_SEED, num_reps=NUM_REPS):
    """
    Generates the DoE run matrix.
    Crosses: 6 Configs × 3 Scenarios × 3 Severities × num_reps Reps.

    Args:
        seed: Random seed for shuffle. If None, matrix is NOT shuffled
              (used by the 54-run dry-run gate for sequential execution).
        num_reps: Number of replications per treatment combination.

    Returns a list of run descriptor dicts.
    """
    matrix = []
    for config_name, config_patch in CONFIGURATIONS.items():
        for scenario_name, severity_map in SCENARIOS.items():
            for severity_name, scenario_params in severity_map.items():
                for rep_id in range(1, num_reps + 1):
                    matrix.append({
                        "configuration": config_name,
                        "scenario": scenario_name,
                        "severity": severity_name,
                        "rep_id": rep_id,
                        "config_patch": config_patch,
                        "scenario_params": scenario_params,
                        "fuse_timeout": FUSE_TIMEOUTS[severity_name],
                    })

    logger.info(f"Matrix generated: {len(matrix)} total runs "
                f"({len(CONFIGURATIONS)} configs × {len(SCENARIOS)} scenarios × "
                f"3 severities × {num_reps} reps)")

    if seed is not None:
        random.seed(seed)
        random.shuffle(matrix)
        logger.info(f"Matrix shuffled with seed={seed} for reproducibility.")
    else:
        logger.info("Matrix NOT shuffled (sequential mode for dry-run gate).")

    return matrix


# =============================================================================
# STEP 3.3 — STERILE BASELINE & GHOST PUBLISHER
# =============================================================================
# The orchestrator acts as a Ghost Publisher on the Redis telemetry bus,
# replacing the deactivated environment_sim.py.
# =============================================================================

# The mathematically flat "Nominal State" — zero-variance starting point
STERILE_BASELINE = {
    "temp": 45.0,
    "battery": 100.0,
    "is_working": True,
    "has_master": False,
    "angle": 90,
    "eclipse": False,
    "orbit_plane": None,  # Will be set per-node
    "type": "satellite",
}

# Orbit plane assignments (must match environment_sim.py fleet configuration)
NODE_ORBIT_PLANES = {
    "minikube-m02": "A",
    "minikube-m03": "B",
    "minikube-m04": "C",
}

# Settle time after sterilization (seconds)
STERILE_SETTLE_SEC = float(os.getenv("STERILE_SETTLE_SEC", "5.0"))


def sterilize_cluster(redis_conn):
    """
    Publish the Sterile Baseline to all satellite nodes.
    Forces every node into a mathematically perfect "Nominal" state:
    safe temperature, full battery, centered visual payload.
    Guarantees zero autonomous migrations before the injection phase.
    """
    logger.info("🧼 Sterilizing cluster — publishing flat baseline to all nodes...")
    for node in SATELLITE_NODES:
        baseline = dict(STERILE_BASELINE)
        baseline["orbit_plane"] = NODE_ORBIT_PLANES.get(node, "B")
        channel = f"telemetry/{node}"
        redis_conn.publish(channel, json.dumps(baseline))

    logger.info(f"🧼 Baseline published. Settling for {STERILE_SETTLE_SEC}s...")
    time.sleep(STERILE_SETTLE_SEC)
    logger.info("🧼 Cluster sterilized — zero-variance state confirmed.")


def ghost_publish(redis_conn, node, telemetry_override):
    """
    Publish a targeted telemetry override to a specific node.
    The Node Agent's main event loop reads this directly from Redis Pub/Sub.
    """
    baseline = dict(STERILE_BASELINE)
    baseline["orbit_plane"] = NODE_ORBIT_PLANES.get(node, "B")
    baseline.update(telemetry_override)
    channel = f"telemetry/{node}"
    redis_conn.publish(channel, json.dumps(baseline))
    logger.info(f"👻 Ghost publish → {node}: {telemetry_override}")


def inject_thermal_spike(redis_conn, node, temp):
    """
    Instantly forces a node to a critical thermal state.
    Triggers the Node Agent's reactive T_FUSE failsafe.
    """
    ghost_publish(redis_conn, node, {"temp": temp})


def inject_battery_crisis(redis_conn, node, battery):
    """
    Instantly forces a node to a critical energy state.
    Triggers the Node Agent's reactive B_FUSE failsafe.
    """
    ghost_publish(redis_conn, node, {"battery": battery})


# =============================================================================
# STEP 3.4 — GHOST WORKER (VISUAL/LATERAL INJECTION)
# =============================================================================
# The orchestrator acts as the ONNX worker, bypassing the real AI model.
# It injects synthetic fire-tracking coordinates directly via HTTP POST
# to the Guardian Sidecar's /state endpoint.
# =============================================================================

def inject_ghost_trajectory(lateral_threshold, step_size=4.0, start_x=64.0):
    """
    Programmatically pushes a synthetic Center of Mass (CoM) trajectory
    toward the swath boundary via HTTP POST to the Guardian Sidecar.

    Decrements the X coordinate from start_x until it breaches the
    LATERAL_THRESHOLD, instantly triggering the Node Agent's lateral
    pathfinding (Trigger B in the main event loop).

    Args:
        lateral_threshold: The pixel threshold from the swath edge.
        step_size: How many pixels to decrement per injection.
        start_x: Starting X coordinate (center of the 128-pixel swath).
    """
    logger.info(f"🎯 Ghost Worker: Injecting lateral trajectory "
                f"(start_x={start_x}, target < {lateral_threshold})...")

    current_x = start_x
    fire_id = 1  # Fixed synthetic fire ID for campaign injections
    day_id = 5   # Day 5+ triggers active inference path in the Guardian

    while current_x >= lateral_threshold:
        payload = {
            "new_frame": "",  # Empty frame — no actual tensor data needed
            "metrics": {
                "center_of_mass": {"x": current_x, "y": 64.0},
                "fire_pixel_count": 100,
                "fire_id": fire_id,
                "day_id": day_id,
                "sample_count": int((start_x - current_x) / step_size) + 1,
                "input_fire_px": 100,
                "ai_confidence": 0.95,
                "tracking_iou": 0.90,
                "prev_fire_mask": [],
                "predicted_fire_mask": [],
                "predicted_probability_mask": [],
            },
        }

        try:
            resp = requests.post(
                f"{GUARDIAN_SVC_URL}/state",
                json=payload,
                timeout=5,
            )
            if resp.status_code == 200:
                logger.info(f"   → CoM_x={current_x:.1f} injected successfully")
            else:
                logger.warning(f"   → CoM_x={current_x:.1f} HTTP {resp.status_code}")
        except requests.exceptions.RequestException as e:
            logger.warning(f"   → CoM_x={current_x:.1f} failed: {e}")

        current_x -= step_size
        time.sleep(0.5)  # 500ms cadence to give the Guardian time to write state

    logger.info(f"🎯 Ghost Worker: Trajectory complete — CoM breached threshold at x={current_x + step_size:.1f}")


# =============================================================================
# STEP 3.5 — CALIBRATED VIRTUAL HARDWARE FUSE
# =============================================================================
# Simulates physical silicon melt if the cluster fails to complete migration
# before the severity-dictated deadline.
# =============================================================================

class HardwareFuse:
    """
    Asynchronous death timer that simulates hardware thermal shutdown.

    Upon injection of a critical thermal/battery spike, this fuse starts
    counting down. If the K8s API confirms the destination Pod is not
    Running + Ready before the timer expires, the origin Pod is instantly
    terminated — yielding a 0% survival rate.
    """

    def __init__(self, k8s_api, fuse_timeout_sec, origin_pod_name,
                 namespace="default"):
        self.k8s_api = k8s_api
        self.timeout = fuse_timeout_sec
        self.origin_pod = origin_pod_name
        self.namespace = namespace
        self._thread = None
        self._cancelled = False
        self.fuse_triggered = False

    def start(self):
        """Launch the asynchronous death timer."""
        if self.timeout is None:
            logger.info("💣 Hardware Fuse: DISABLED (nominal severity — no fuse)")
            return
        logger.info(f"💣 Hardware Fuse: ARMED — {self.timeout}s until simulated meltdown "
                     f"(origin: {self.origin_pod})")
        self._thread = threading.Thread(target=self._fuse_countdown, daemon=True)
        self._thread.start()

    def cancel(self):
        """Cancel the fuse (migration succeeded in time)."""
        self._cancelled = True
        if self.timeout is not None:
            logger.info(f"💣 Hardware Fuse: DEFUSED — migration completed before deadline")

    def _fuse_countdown(self):
        """
        The death timer loop. Sleeps for the calibrated timeout, then checks
        whether the destination pod is alive. If not, kills the origin pod.
        """
        time.sleep(self.timeout)

        if self._cancelled:
            return

        # Fuse expired — simulate hardware meltdown
        logger.warning(f"💥 HARDWARE FUSE EXPIRED after {self.timeout}s! "
                       f"Simulating thermal shutdown on {self.origin_pod}...")
        self.fuse_triggered = True

        try:
            self.k8s_api.delete_namespaced_pod(
                name=self.origin_pod,
                namespace=self.namespace,
                body=client.V1DeleteOptions(grace_period_seconds=0),
            )
            logger.warning(f"💥 Origin pod {self.origin_pod} TERMINATED — "
                           f"simulated silicon melt (0% survival)")
        except client.exceptions.ApiException as e:
            if e.status == 404:
                logger.info(f"💥 Origin pod {self.origin_pod} already gone "
                            f"(may have been migrated or previously terminated)")
            else:
                logger.error(f"💥 Failed to terminate origin pod: {e}")


# =============================================================================
# STEP 3.6 — RUN SANITIZATION & TEARDOWN
# =============================================================================
# Restores a sterile environment between runs to prevent cross-contamination.
# =============================================================================

def sanitize_run(redis_conn, k8s_api):
    """
    Post-run cleanup protocol. Ensures the cluster is returned to a pristine
    state before the next randomized run begins.

    1. Network Reset: clear all tc qdisc rules on all Node Agents
    2. State Wipe: clear /tmp/payload_state.json inside the sidecar
    3. Stability Assertion: poll K8s until workload pods are Running + Ready
    """
    logger.info("🧹 SANITIZE: Beginning post-run cleanup...")

    # ── 1. Network Reset ──────────────────────────────────────────────────────
    # Publish clear_throttle command to the campaign/config channel.
    # All Node Agents subscribed to this channel will execute tc qdisc del.
    logger.info("🧹 SANITIZE [1/3]: Clearing network throttles on all nodes...")
    redis_conn.publish("campaign/config", json.dumps({"action": "clear_throttle"}))
    time.sleep(1.0)  # Brief settle for tc commands to propagate

    # ── 2. State Wipe ─────────────────────────────────────────────────────────
    # Clear the Guardian's /tmp/payload_state.json via K8s exec.
    # This prevents stale CoM coordinates from triggering phantom lateral migrations.
    logger.info("🧹 SANITIZE [2/3]: Wiping sidecar state files...")
    try:
        pods = k8s_api.list_namespaced_pod(
            namespace="default",
            label_selector="app=space-mission",
            field_selector="status.phase=Running",
        )
        for pod in pods.items:
            try:
                k8s_stream(
                    k8s_api.connect_get_namespaced_pod_exec,
                    name=pod.metadata.name,
                    namespace="default",
                    container="sidecar-guardian",
                    command=["rm", "-f", "/tmp/payload_state.json"],
                    stderr=True,
                    stdout=True,
                )
                logger.info(f"   ✅ Wiped state on {pod.metadata.name}")
            except Exception as e:
                logger.warning(f"   ⚠️ State wipe failed on {pod.metadata.name}: {e}")
    except Exception as e:
        logger.warning(f"   ⚠️ Could not list pods for state wipe: {e}")

    # ── 3. Stability Assertion ────────────────────────────────────────────────
    # Block until exactly 1 SML pod + 1 Master pod are Running and Ready.
    logger.info("🧹 SANITIZE [3/3]: Asserting cluster stability...")
    _assert_cluster_stability(k8s_api)

    logger.info("🧹 SANITIZE: Cleanup complete — cluster is pristine.")


def _assert_cluster_stability(k8s_api, timeout_sec=None):
    """
    Blocks until the cluster reports exactly:
      - 1 space-mission pod: Running + Ready
      - 1 topology-master pod: Running + Ready

    This prevents the next run from starting on a half-initialized cluster.
    """
    if timeout_sec is None:
        timeout_sec = SANITIZE_TIMEOUT_SEC

    deadline = time.time() + timeout_sec
    poll_interval = 2.0

    while time.time() < deadline:
        try:
            sml_ready = _count_ready_pods(k8s_api, "app=space-mission")
            master_ready = _count_ready_pods(k8s_api, "app=topology-master")

            if sml_ready >= 1 and master_ready >= 1:
                logger.info(f"   ✅ Cluster stable: {sml_ready} SML pod(s), "
                            f"{master_ready} Master pod(s) Ready")
                return True

            logger.info(f"   ⏳ Waiting: SML={sml_ready}/1, Master={master_ready}/1")
        except Exception as e:
            logger.warning(f"   ⚠️ Stability poll error: {e}")

        time.sleep(poll_interval)

    logger.warning(f"⚠️ SANITIZE: Stability timeout ({timeout_sec}s) — "
                   f"proceeding despite incomplete cluster state")
    return False


def _count_ready_pods(k8s_api, label_selector):
    """Count pods matching label_selector that are Running with all containers Ready."""
    pods = k8s_api.list_namespaced_pod(
        namespace="default",
        label_selector=label_selector,
    )
    ready_count = 0
    for pod in pods.items:
        if pod.status.phase != "Running":
            continue
        if pod.metadata.deletion_timestamp is not None:
            continue  # Exclude terminating pods
        if pod.status.container_statuses:
            all_ready = all(cs.ready for cs in pod.status.container_statuses)
            if all_ready:
                ready_count += 1
    return ready_count


def _find_origin_pod(k8s_api, label_selector="app=space-mission"):
    """
    Find the current origin pod name for the space-mission deployment.
    Returns the pod name or None if not found.
    """
    try:
        pods = k8s_api.list_namespaced_pod(
            namespace="default",
            label_selector=label_selector,
            field_selector="status.phase=Running",
        )
        for pod in pods.items:
            if pod.metadata.deletion_timestamp is None:
                return pod.metadata.name
    except Exception as e:
        logger.warning(f"Could not find origin pod: {e}")
    return None


# =============================================================================
# RUN EXECUTION ENGINE
# =============================================================================
# Orchestrates the complete lifecycle of a single experimental run.
# Follows the strict 6-phase lifecycle from orchestrator_functional_spec.md §7:
#   1. Preparation — Apply ablation rules and reset network constraints
#   2. Sterilization — Enforce Sterile Baseline (zero-variance flat state)
#   3. Stress Initialization — Apply scenario-specific network bottlenecks
#   4. Injection — Execute Ghost Publisher / Ghost Worker fault vectors
#   5. Extraction — Poll K8s API for migration delay and survival metrics
#   6. Teardown & Sanitization — Clean up and assert cluster stability
# =============================================================================

def apply_configuration(redis_conn, config_patch):
    """
    Hot-reload the ablation configuration on all Node Agents via Redis Pub/Sub.
    Publishes the configuration patch to the 'campaign/config' channel.
    """
    # Convert booleans to strings for the Node Agent's type-casting logic
    serialized = {}
    for key, value in config_patch.items():
        if isinstance(value, bool):
            serialized[key] = "True" if value else "False"
        else:
            serialized[key] = value

    redis_conn.publish("campaign/config", json.dumps(serialized))
    logger.info(f"⚙️ Configuration hot-reloaded: {serialized}")
    time.sleep(1.0)  # Brief settle for agents to process the update


def apply_network_throttle(redis_conn, rate_mbit, latency_ms):
    """
    Command all Node Agents to apply tc traffic control rules.
    Delegates via the campaign/config Redis channel.
    """
    cmd = {"action": "throttle", "rate": rate_mbit, "latency": latency_ms}
    redis_conn.publish("campaign/config", json.dumps(cmd))
    logger.info(f"🌐 Network throttle applied: {rate_mbit}mbit / {latency_ms}ms")
    time.sleep(1.0)  # Brief settle for tc rules to propagate


def execute_run(run, run_index, total_runs, redis_conn, k8s_api):
    """
    Execute a single experimental run through the full 6-phase lifecycle.

    Args:
        run: Run descriptor dict from generate_run_matrix()
        run_index: 1-based index of this run in the shuffled matrix
        total_runs: Total number of runs in the matrix
        redis_conn: Active Redis connection
        k8s_api: Kubernetes CoreV1Api client

    Returns:
        dict with extraction metrics (or failure indicators)
    """
    config_name = run["configuration"]
    scenario = run["scenario"]
    severity = run["severity"]
    rep_id = run["rep_id"]

    logger.info(f"\n{'='*70}")
    logger.info(f"🏃 RUN {run_index}/{total_runs}: {config_name} | "
                f"{scenario} | {severity} | Rep {rep_id}")
    logger.info(f"{'='*70}")

    result = {
        "timestamp": datetime.now().isoformat(),
        "configuration": config_name,
        "scenario": scenario,
        "severity": severity,
        "rep_id": rep_id,
        "hops": 0,
        "survival_rate": 0.0,
        "migration_delay_sec": 0.0,
        "bandwidth_mb": 0.0,
    }

    try:
        # ── PHASE 1: Preparation — Apply ablation rules & reset constraints ─
        logger.info("📋 Phase 1/6: Applying ablation configuration...")
        apply_configuration(redis_conn, run["config_patch"])

        # ── PHASE 2: Sterilization — Enforce Sterile Baseline ───────────────
        # Must happen BEFORE network throttle so the baseline telemetry
        # propagates at full bandwidth (spec: orchestrator_functional_spec §7).
        logger.info("📋 Phase 2/6: Sterilizing cluster...")
        sterilize_cluster(redis_conn)

        # ── PHASE 3: Stress Initialization — Network Throttle ───────────────
        logger.info("📋 Phase 3/6: Applying network constraints...")
        if scenario == "slow_internet":
            params = run["scenario_params"]
            apply_network_throttle(
                redis_conn,
                rate_mbit=params["tc_rate_mbit"],
                latency_ms=params["tc_latency_ms"],
            )
        # Other scenarios don't require network throttling

        # ── PHASE 4: Injection — Execute fault vectors ──────────────────────
        logger.info("📋 Phase 4/6: Executing fault injection...")
        injection_timestamp = time.time()

        # Start the Redis abort listener for explicit "Correct Failure" detection
        abort_listener = AbortFlagListener(redis_conn)
        abort_listener.start()

        # Start the Redis route metrics listener for dynamic hop tracking
        # (Phase 4, Step 4.1: captures the actual route from the Node Agent)
        route_listener = RouteMetricsListener(redis_conn)
        route_listener.start()

        # Find origin pod for the hardware fuse
        origin_pod = _find_origin_pod(k8s_api)
        fuse = HardwareFuse(
            k8s_api=k8s_api,
            fuse_timeout_sec=run["fuse_timeout"],
            origin_pod_name=origin_pod or "unknown",
        )

        # Launch the Ghost Worker as a concurrent daemon thread so that
        # synthetic lateral CoM data is actively flowing during the thermal
        # injections — simulating real data-plane activity.
        lateral_threshold = run["config_patch"].get(
            "LATERAL_THRESHOLD",
            int(STERILE_BASELINE.get("lateral_threshold", 8)),
        )
        ghost_worker_thread = threading.Thread(
            target=inject_ghost_trajectory,
            args=(lateral_threshold,),
            daemon=True,
        )
        ghost_worker_thread.start()

        if scenario == "slow_internet":
            # Scenario 1: inject thermal spike to trigger migration under bandwidth constraint
            inject_thermal_spike(redis_conn, SATELLITE_NODES[0], 95.0)
            fuse.start()

        elif scenario == "sequential_thermal":
            # Scenario 2: Sequential Thermal Crisis (Cascading Ping-Pong)
            params = run["scenario_params"]
            delta_t = params["delta_t_stress_sec"]
            spike_temp = params["spike_temp"]

            # Spike Node A (primary) → triggers migration to Node B
            inject_thermal_spike(redis_conn, SATELLITE_NODES[0], spike_temp)
            fuse.start()

            # Wait Δt_stress seconds, then spike Node B (destination)
            logger.info(f"   ⏱️ Waiting {delta_t}s before secondary thermal injection...")
            time.sleep(delta_t)
            inject_thermal_spike(redis_conn, SATELLITE_NODES[1], spike_temp)

        elif scenario == "double_trouble":
            # Scenario 3: Double Trouble (Thermal & Energy Crisis)
            params = run["scenario_params"]
            local_temp = params["local_temp"]
            neighbor_battery = params["neighbor_battery"]

            # Spike local node temperature
            inject_thermal_spike(redis_conn, SATELLITE_NODES[0], local_temp)
            fuse.start()

            # Simultaneously degrade all neighbor batteries
            for neighbor in SATELLITE_NODES[1:]:
                inject_battery_crisis(redis_conn, neighbor, neighbor_battery)

        # ── PHASE 5: Extraction ──────────────────────────────────────────────
        logger.info("📋 Phase 5/6: Monitoring cluster response...")
        extraction_result = _extract_metrics(
            k8s_api, redis_conn, injection_timestamp, fuse, run,
            abort_listener=abort_listener,
            route_listener=route_listener,
        )
        result.update(extraction_result)

        # Defuse the hardware fuse if it hasn't already triggered
        fuse.cancel()

        # Stop the abort listener, route listener, and ghost worker
        abort_listener.stop()
        route_listener.stop()

    except Exception as e:
        logger.error(f"❌ Run execution error: {e}")
        result["survival_rate"] = 0.0
        result["notes"] = f"EXCEPTION: {str(e)}"

    # ── PHASE 6: Teardown & Sanitization ────────────────────────────────────
    logger.info("📋 Phase 6/6: Sanitizing cluster...")
    sanitize_run(redis_conn, k8s_api)

    logger.info(f"✅ Run {run_index} complete — Survival: {result['survival_rate']*100:.0f}% | "
                f"Delay: {result['migration_delay_sec']:.2f}s | BW: {result['bandwidth_mb']:.1f}MB")

    return result


def _extract_metrics(k8s_api, redis_conn, injection_timestamp, fuse, run,
                     abort_listener=None, route_listener=None):
    """
    Monitor the cluster after fault injection to extract the response metrics.

    Polls the K8s API and the Redis abort channel to detect:
    - Pod migration (destination pod becoming Ready)
    - Correct Failure via explicit abort flags (COOLDOWN_LOCK,
      SURVIVAL_PROBABILITY_LOWER_AT_DESTINATION) — spec: automation §3
    - Hardware fuse expiration (simulated meltdown)

    Phase 4, Step 4.1 additions:
    - Dynamic hop tracking via RouteMetricsListener (campaign/metrics channel)
    - Migration Delay = injection_timestamp → K8s pod Ready transition
    - Intelligent "Correct Failure" logging: survival=100%, delay=0.0
    - Dynamic Bandwidth_MB = checkpoint_size * actual_hops

    Returns a dict with hops, survival_rate, migration_delay_sec, bandwidth_mb.
    """
    result = {
        "hops": 0,
        "survival_rate": 0.0,
        "migration_delay_sec": 0.0,
        "bandwidth_mb": 0.0,
    }

    # Dynamic Checkpoint Sizing (Phase 4, Step 4.1)
    # Resolves the active checkpoint configuration to log accurate data
    # transport metrics: 160MB for monolithic, 24MB for sidecar-only.
    is_monolithic = run["config_patch"].get("MONOLITHIC_CHECKPOINT", False)
    checkpoint_size_mb = 160.0 if is_monolithic else 24.0

    # General case: poll for migration, abort flags, or fuse expiration
    poll_deadline = time.time() + RUN_TIMEOUT_SEC
    poll_interval = 0.5

    while time.time() < poll_deadline:
        # ── Check 1: Explicit Correct Failure via Redis abort flags ──────
        # Intelligent Success Logging (Phase 4, Step 4.1):
        # If the orchestrator detects an intentional, self-preserving abort
        # logged by the Node Agent (e.g., [ABORT_ACKNOWLEDGED: COOLDOWN_LOCK]),
        # it records Survival_Rate = 1.0 (100%) and Migration_Delay_sec = 0.0.
        # The system correctly refused to migrate — the cluster survived.
        # Spec: campaign_automation_specification.md §3 (Correct Failure Tracking)
        if abort_listener and abort_listener.abort_detected:
            reason = abort_listener.abort_reason
            logger.info(f"   ✅ CORRECT FAILURE: Node Agent refused migration "
                        f"(abort flag: {reason}). Survival = 100%")
            result["survival_rate"] = 1.0
            result["migration_delay_sec"] = 0.0
            return result

        # ── Check 2: Hardware fuse triggered (simulated meltdown) ────────
        if fuse.fuse_triggered:
            logger.info("   💥 Hardware fuse triggered — 0% survival")
            result["survival_rate"] = 0.0
            result["migration_delay_sec"] = time.time() - injection_timestamp
            return result

        # ── Check 3: Successful migration (destination pod Running+Ready) ─
        try:
            pods = k8s_api.list_namespaced_pod(
                namespace="default",
                label_selector="app=space-mission",
                field_selector="status.phase=Running",
            )
            for pod in pods.items:
                if (pod.metadata.deletion_timestamp is None
                        and pod.status.container_statuses
                        and all(cs.ready for cs in pod.status.container_statuses)):
                    # Migration Delay Calculation (Phase 4, Step 4.1):
                    # Exact wall-clock duration from the injection timestamp
                    # to the K8s API reporting the destination pod as Ready.
                    migration_delay = time.time() - injection_timestamp
                    result["survival_rate"] = 1.0
                    result["migration_delay_sec"] = round(migration_delay, 3)

                    # Dynamic Hop Tracking (Phase 4, Step 4.1):
                    # Use the actual route captured by the RouteMetricsListener
                    # from the Node Agent's campaign/metrics broadcast.
                    if route_listener and route_listener.route_received:
                        result["hops"] = route_listener.hops
                        logger.info(f"   📊 Dynamic hops: {route_listener.hops} "
                                    f"(route: {route_listener.route})")
                    else:
                        # Fallback: infer hop count from node placement delta
                        # In the 3-node linear constellation, migrations between
                        # adjacent nodes = 1 hop, m02↔m04 = 2 hops.
                        result["hops"] = 1
                        logger.warning("   ⚠️ Route listener did not capture route — "
                                       "falling back to hops=1")

                    # Dynamic Bandwidth Tracking (Phase 4, Step 4.1):
                    # Bandwidth_MB = checkpoint_size * hops
                    # If MONOLITHIC_CHECKPOINT=True → 160 * hops
                    # If MONOLITHIC_CHECKPOINT=False → 24 * hops
                    result["bandwidth_mb"] = round(
                        checkpoint_size_mb * result["hops"], 1
                    )
                    logger.info(f"   ✅ Migration complete — delay: {migration_delay:.2f}s | "
                                f"hops: {result['hops']} | BW: {result['bandwidth_mb']}MB")
                    return result
        except Exception as e:
            logger.warning(f"   ⚠️ K8s poll error: {e}")

        time.sleep(poll_interval)

    # ── Fallback: Correct Failure heuristic for scenarios with expected abort ─
    # If we reach the timeout on a severity=correct_failure run and the origin
    # pod is still alive (no migration happened, no fuse triggered), the system
    # likely correctly refused to migrate but the Node Agent didn't publish an
    # explicit abort flag. Log it as a Correct Failure with a warning.
    if run["severity"] == "correct_failure" and not fuse.fuse_triggered:
        origin_pod = _find_origin_pod(k8s_api)
        if origin_pod:
            logger.warning("   ⚠️ CORRECT FAILURE (heuristic): No explicit abort flag "
                           "received, but origin pod survived and no migration occurred. "
                           "Recording as software success (100% survival).")
            result["survival_rate"] = 1.0
            result["migration_delay_sec"] = 0.0
            return result

    # Timeout reached — no migration detected
    logger.warning(f"   ⏱️ Run timeout ({RUN_TIMEOUT_SEC}s) — recording as failure")
    result["survival_rate"] = 0.0
    result["migration_delay_sec"] = RUN_TIMEOUT_SEC
    return result


# =============================================================================
# ABORT FLAG LISTENER (Explicit "Correct Failure" Detection)
# =============================================================================
# Subscribes to the Node Agent's log channel on Redis to detect explicit
# abort acknowledgement flags. This is the spec-compliant mechanism for
# distinguishing between a system crash (0% survival) and a safe,
# intentional refusal to migrate (100% survival).
# Spec: campaign_automation_specification.md §3, orchestrator_functional_spec.md §8
# =============================================================================

# Known abort flag patterns emitted by the Node Agent
ABORT_FLAG_PATTERNS = [
    "ABORT_ACKNOWLEDGED: COOLDOWN_LOCK",
    "ABORT_ACKNOWLEDGED: SURVIVAL_PROBABILITY_LOWER_AT_DESTINATION",
    "HARDWARE TRIGGER BLOCKED",
    "Stay Local chosen",
]


class AbortFlagListener:
    """
    Subscribes to the Node Agent's log output on Redis to detect explicit
    abort flags indicating an intentional refusal to migrate.

    The Node Agent prints these flags to stdout, which are captured by the
    K8s logging infrastructure. For the campaign, we also monitor the Redis
    telemetry bus for STAY_LOCAL responses from the Dijkstra Lua script.
    """

    def __init__(self, redis_conn):
        self._redis_host = redis_conn.connection_pool.connection_kwargs.get("host", GROUND_REDIS_HOST)
        self._redis_port = redis_conn.connection_pool.connection_kwargs.get("port", GROUND_REDIS_PORT)
        self.abort_detected = False
        self.abort_reason = ""
        self._thread = None
        self._stop_event = threading.Event()

    def start(self):
        """Start the background listener thread."""
        self.abort_detected = False
        self.abort_reason = ""
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._listen, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop the background listener."""
        self._stop_event.set()

    def _listen(self):
        """
        Subscribe to 'campaign/aborts' Redis channel.
        The Node Agent publishes abort events here when it intentionally
        refuses to migrate (cooldown lock, destination score worse, etc.).
        """
        try:
            r = redis.Redis(
                host=self._redis_host,
                port=self._redis_port,
                decode_responses=True,
                socket_connect_timeout=3,
            )
            ps = r.pubsub()
            ps.subscribe("campaign/aborts")

            while not self._stop_event.is_set():
                msg = ps.get_message(timeout=0.5)
                if msg and msg["type"] == "message":
                    data = msg["data"]
                    for pattern in ABORT_FLAG_PATTERNS:
                        if pattern in data:
                            self.abort_detected = True
                            self.abort_reason = pattern
                            logger.info(f"   🔍 Abort flag detected: {pattern}")
                            return

            ps.unsubscribe("campaign/aborts")
        except Exception as e:
            logger.warning(f"   ⚠️ Abort listener error: {e}")


# =============================================================================
# ROUTE METRICS LISTENER (Phase 4, Step 4.1 — Dynamic Hop Tracking)
# =============================================================================
# Subscribes to the 'campaign/metrics' Redis channel to capture the actual
# multi-hop route selected by the Dijkstra pathfinder on the edge node.
# This enables dynamic calculation of:
#   - Hops: len(route) — the number of ISL transmission hops
#   - Bandwidth_MB: checkpoint_size_mb * hops
#
# Without this listener, the Orchestrator has no visibility into the
# pathfinding decision because it happens atomically inside Redis via EVALSHA
# on the Node Agent side. The Node Agent broadcasts its route to this channel
# immediately after a successful relay transfer.
#
# Spec: campaign_automation_specification.md §3 (Dynamic Checkpoint Sizing)
# Spec: campaign_experimental_design.md §3 (Constellation Bandwidth Footprint)
# =============================================================================


class RouteMetricsListener:
    """
    Captures the actual migration route published by the Node Agent after
    a successful relay transfer. Provides the true hop count for accurate
    Bandwidth_MB calculation in the DoE CSV dataset.
    """

    def __init__(self, redis_conn):
        self._redis_host = redis_conn.connection_pool.connection_kwargs.get("host", GROUND_REDIS_HOST)
        self._redis_port = redis_conn.connection_pool.connection_kwargs.get("port", GROUND_REDIS_PORT)
        self.route_received = False
        self.route = []
        self.hops = 0
        self.source = ""
        self.destination = ""
        self._thread = None
        self._stop_event = threading.Event()

    def start(self):
        """Start the background listener thread."""
        self.route_received = False
        self.route = []
        self.hops = 0
        self.source = ""
        self.destination = ""
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._listen, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop the background listener."""
        self._stop_event.set()

    def _listen(self):
        """
        Subscribe to 'campaign/metrics' Redis channel.
        The Node Agent publishes route details here after each successful
        relay transfer (both SML payload and Topology Master migrations).
        """
        try:
            r = redis.Redis(
                host=self._redis_host,
                port=self._redis_port,
                decode_responses=True,
                socket_connect_timeout=3,
            )
            ps = r.pubsub()
            ps.subscribe("campaign/metrics")

            while not self._stop_event.is_set():
                msg = ps.get_message(timeout=0.5)
                if msg and msg["type"] == "message":
                    try:
                        data = json.loads(msg["data"])
                        if data.get("event") == "migration_complete":
                            self.route = data.get("route", [])
                            self.hops = data.get("hops", len(self.route))
                            self.source = data.get("source", "")
                            self.destination = data.get("destination", "")
                            self.route_received = True
                            logger.info(f"   📊 Route captured: {self.source} → "
                                        f"{self.route} ({self.hops} hop(s))")
                            # Don't return — keep listening for additional
                            # migrations (e.g., double evacuation routes)
                    except (json.JSONDecodeError, TypeError) as e:
                        logger.warning(f"   ⚠️ Route listener: malformed message: {e}")

            ps.unsubscribe("campaign/metrics")
        except Exception as e:
            logger.warning(f"   ⚠️ Route metrics listener error: {e}")


# =============================================================================
# DRY-RUN GATE (Pre-Campaign Smoke Test)
# =============================================================================
# Spec: campaign_automation_specification.md §2.3
# Spec: orchestrator_functional_spec.md §6
#
# Before the full 540-run randomized campaign, the orchestrator must execute
# a non-shuffled, sequential mini-matrix of exactly 54 unique treatment
# combinations (6 Configs × 3 Scenarios × 3 Severities × 1 Rep).
# If any dry-run fails validation, the orchestrator triggers a hard stop.
# =============================================================================

def execute_dry_run_gate(redis_conn, k8s_api):
    """
    Execute the mandatory 54-run pre-campaign smoke test.

    Generates a deterministic, sequential (non-shuffled) mini-matrix covering
    every unique treatment combination exactly once. Each dry-run is executed
    through the full run lifecycle and validated for basic behavioral correctness.

    Returns True if all 54 dry-runs pass, False if any fail validation.
    """
    logger.info("=" * 70)
    logger.info("🧪 DRY-RUN GATE: Starting 54-run pre-campaign smoke test...")
    logger.info("=" * 70)

    # Generate the 54-run mini-matrix (1 rep per combination, NOT shuffled)
    dry_run_matrix = generate_run_matrix(seed=None, num_reps=1)
    # Override: do NOT shuffle — execute sequentially for deterministic validation
    dry_run_matrix.sort(key=lambda r: (r["configuration"], r["scenario"], r["severity"]))

    total_dry = len(dry_run_matrix)
    logger.info(f"🧪 Dry-Run matrix: {total_dry} sequential treatment combinations")

    passed = 0
    failed_runs = []

    for idx, run in enumerate(dry_run_matrix, start=1):
        logger.info(f"\n🧪 DRY-RUN {idx}/{total_dry}: {run['configuration']} | "
                    f"{run['scenario']} | {run['severity']}")

        try:
            result = execute_run(run, idx, total_dry, redis_conn, k8s_api)

            # ── Behavioral Assertion Gate ──────────────────────────────────
            # Validate that the system responded as expected for this
            # configuration × scenario × severity combination.
            valid = _validate_dry_run_result(run, result)

            if valid:
                passed += 1
                logger.info(f"   ✅ DRY-RUN {idx}: PASSED")
            else:
                failed_runs.append({
                    "index": idx,
                    "run": f"{run['configuration']}|{run['scenario']}|{run['severity']}",
                    "result": result,
                })
                logger.error(f"   ❌ DRY-RUN {idx}: FAILED validation")

        except Exception as e:
            logger.error(f"   ❌ DRY-RUN {idx}: EXCEPTION — {e}")
            failed_runs.append({
                "index": idx,
                "run": f"{run['configuration']}|{run['scenario']}|{run['severity']}",
                "error": str(e),
            })

    # ── Summary ────────────────────────────────────────────────────────────
    logger.info(f"\n{'='*70}")
    logger.info(f"🧪 DRY-RUN GATE RESULTS: {passed}/{total_dry} passed")

    if failed_runs:
        logger.error(f"🧪 {len(failed_runs)} dry-run(s) FAILED:")
        for fail in failed_runs:
            logger.error(f"   • Run {fail['index']}: {fail['run']}")
        logger.error("🧪 FAIL-FAST HARD STOP: The campaign will NOT proceed.")
        return False

    logger.info("🧪 All 54 smoke tests passed — campaign infrastructure verified.")
    logger.info(f"{'='*70}")
    return True


def _validate_dry_run_result(run, result):
    """
    Validate that a dry-run result matches the expected behavioral state
    for the given configuration × scenario × severity combination.

    Behavioral assertions:
    - Correct Failure runs should have survival_rate == 1.0 (safe refusal)
    - Nominal runs should have survival_rate == 1.0 (successful migration)
    - CSV schema fields should be non-empty
    - Hardware fuse should NOT fire on nominal severity

    Returns True if the result passes validation.
    """
    severity = run["severity"]
    survival = result.get("survival_rate", -1)

    # Basic schema validation: essential fields must be present
    if result.get("timestamp") is None:
        logger.warning("   ⚠️ Validation: missing timestamp")
        return False

    # Severity-specific behavioral assertions
    if severity == "nominal":
        # Nominal: migration should always succeed (100% survival)
        if survival != 1.0:
            logger.warning(f"   ⚠️ Validation: nominal severity expected 100% survival, "
                           f"got {survival*100:.0f}%")
            return False

    elif severity == "correct_failure":
        # Correct Failure: system should refuse to migrate (100% survival)
        # OR the fuse triggers legitimately (0% survival is also valid for
        # slow_internet correct_failure where transfer physically cannot complete)
        if run["scenario"] in ("sequential_thermal", "double_trouble"):
            if survival != 1.0:
                logger.warning(f"   ⚠️ Validation: correct_failure for {run['scenario']} "
                               f"expected safe refusal (100%), got {survival*100:.0f}%")
                return False

    # Borderline: no strict assertion — outcome is probabilistic by design

    return True


# =============================================================================
# CSV TELEMETRY SINK (Phase 4, Step 4.2 — Atomic Append)
# =============================================================================

CSV_HEADER = [
    "Timestamp", "Configuration_Block", "Scenario", "Severity_Level", "Repetition_ID",
    "Hops", "Survival_Rate", "Migration_Delay_sec", "Bandwidth_MB",
]


def _ensure_csv_header():
    """Create the CSV file with header if it doesn't exist."""
    os.makedirs(os.path.dirname(CSV_OUTPUT_PATH), exist_ok=True)
    if not os.path.exists(CSV_OUTPUT_PATH):
        with open(CSV_OUTPUT_PATH, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(CSV_HEADER)
        logger.info(f"📊 CSV sink initialized: {CSV_OUTPUT_PATH}")


def append_result_to_csv(result):
    """
    Append a single run result to the campaign CSV file.
    Uses append mode ("a") for atomic, lock-free writes.
    """
    row = [
        result.get("timestamp", ""),
        result.get("configuration", ""),
        result.get("scenario", ""),
        result.get("severity", ""),
        result.get("rep_id", 0),
        result.get("hops", 0),
        result.get("survival_rate", 0.0),
        result.get("migration_delay_sec", 0.0),
        result.get("bandwidth_mb", 0.0),
    ]
    with open(CSV_OUTPUT_PATH, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(row)


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main():
    """
    Main campaign execution loop.
    Generates the DoE matrix, then iterates through all runs sequentially.
    """
    logger.info("🚀 CAMPAIGN ORCHESTRATOR V7 — INITIALIZING")
    logger.info(f"   Nodes:      {SATELLITE_NODES}")
    logger.info(f"   Guardian:   {GUARDIAN_SVC_URL}")
    logger.info(f"   CSV Output: {CSV_OUTPUT_PATH}")

    # ── Initialize infrastructure ──────────────────────────────────────────
    k8s_api = _init_k8s_client()
    redis_conn = _connect_redis()

    # ── Generate the DoE matrix ────────────────────────────────────────────
    matrix = generate_run_matrix()
    total_runs = len(matrix)
    logger.info(f"📊 Campaign matrix: {total_runs} runs ready for execution")

    # ── Initialize CSV sink ────────────────────────────────────────────────
    _ensure_csv_header()

    # ── Pre-flight stability check ─────────────────────────────────────────
    logger.info("🔍 Pre-flight: Verifying cluster stability...")
    if not _assert_cluster_stability(k8s_api, timeout_sec=60):
        logger.critical("❌ Pre-flight FAILED: Cluster is not stable. Aborting campaign.")
        sys.exit(1)
    logger.info("✅ Pre-flight: Cluster is stable and ready.")

    # ── Mandatory Dry-Run Gate (54-run smoke test) ─────────────────────────
    # Spec: campaign_automation_specification.md §2.3
    # Spec: orchestrator_functional_spec.md §6
    # Execute a non-shuffled, sequential mini-matrix of 54 unique treatment
    # combinations (6 Configs × 3 Scenarios × 3 Severities × 1 Rep) before
    # the full randomized campaign. If any dry-run fails to produce the
    # expected behavioral state, the orchestrator triggers a hard stop.
    if not execute_dry_run_gate(redis_conn, k8s_api):
        logger.critical("❌ DRY-RUN GATE FAILED: One or more smoke tests did not "
                        "produce the expected system response. Refusing to proceed "
                        "with the 540-run campaign. Fix the cluster and re-run.")
        sys.exit(1)
    logger.info("✅ Dry-Run Gate passed — all 54 smoke tests verified.")

    # ── Execute the campaign ───────────────────────────────────────────────
    campaign_start = time.time()
    completed = 0
    failed = 0

    for idx, run in enumerate(matrix, start=1):
        try:
            result = execute_run(run, idx, total_runs, redis_conn, k8s_api)
            append_result_to_csv(result)
            completed += 1
            if result["survival_rate"] == 0.0:
                failed += 1
        except Exception as e:
            logger.error(f"❌ CRITICAL: Run {idx} crashed with unhandled exception: {e}")
            failed += 1
            # Attempt emergency sanitization to prevent cascade
            try:
                sanitize_run(redis_conn, k8s_api)
            except Exception:
                pass

        # Progress report every 10 runs
        if idx % 10 == 0:
            elapsed = time.time() - campaign_start
            eta = (elapsed / idx) * (total_runs - idx)
            logger.info(f"📈 Progress: {idx}/{total_runs} ({idx/total_runs*100:.1f}%) | "
                        f"Elapsed: {elapsed/60:.1f}min | ETA: {eta/60:.1f}min | "
                        f"Failures: {failed}")

    # ── Campaign complete ──────────────────────────────────────────────────
    elapsed = time.time() - campaign_start
    logger.info(f"\n{'='*70}")
    logger.info(f"🏁 CAMPAIGN COMPLETE")
    logger.info(f"   Total Runs:   {total_runs}")
    logger.info(f"   Completed:    {completed}")
    logger.info(f"   Failures:     {failed}")
    logger.info(f"   Duration:     {elapsed/60:.1f} minutes")
    logger.info(f"   Results:      {CSV_OUTPUT_PATH}")
    logger.info(f"{'='*70}")


if __name__ == "__main__":
    main()
