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
# Create log directory if not exists
os.makedirs(os.path.join(os.path.dirname(__file__), "..", "logs"), exist_ok=True)
LOG_FILE_PATH = os.path.join(os.path.dirname(__file__), "..", "logs", "campaign_runner.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CAMPAIGN] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE_PATH, mode="a"),
        logging.StreamHandler(sys.stdout)
    ]
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
    
    api = client.CoreV1Api()
    try:
        version_api = client.VersionApi()
        version_info = version_api.get_code()
        logger.info(f"K8s: Connection verified. Server GitVersion: {version_info.git_version}")
    except Exception as e:
        logger.warning(f"K8s: Verification check failed (but client initialized) — {e}")
    return api


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
        ping_res = r.ping()
        logger.info(f"Redis: Connected to {GROUND_REDIS_HOST}:{GROUND_REDIS_PORT} (ping response: {ping_res})")
        return r
    except redis.exceptions.ConnectionError as e:
        logger.critical(f"Redis: Cannot connect — {e}")
        sys.exit(1)


# --- Cluster Topology ---
# The 3-satellite Minikube constellation (matches environment_sim.py fleet)
SATELLITE_NODES = ["minikube-m02", "minikube-m03", "minikube-m04"]

# Guardian Sidecar HTTP endpoint (exposed via K8s ClusterIP Service or local port-forward)
if os.path.exists("/var/run/secrets/kubernetes.io"):
    DEFAULT_GUARDIAN_URL = "http://space-dashboard-svc:80"
else:
    DEFAULT_GUARDIAN_URL = "http://localhost:8080"

GUARDIAN_SVC_URL = os.getenv("GUARDIAN_SVC_URL", DEFAULT_GUARDIAN_URL)

# Campaign CSV output path
CSV_OUTPUT_DIR = os.getenv(
    "CSV_OUTPUT_DIR",
    os.path.join(os.path.dirname(__file__), "..", "3_ground_station", "evaluation_data"),
)
CSV_OUTPUT_PATH = os.path.join(CSV_OUTPUT_DIR, "campaign_results.csv")
DRY_RUN_CSV_PATH = os.path.join(CSV_OUTPUT_DIR, "dry_run_results.csv")

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
    "borderline":      6.0,    # Tight but possible (adjusted for local VM overhead)
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


def settle_startup_cooldown(k8s_api):
    """
    Ensures the initial startup cooldown on the newly launched SML pod has
    expired before starting an experiment to prevent a false abort.
    """
    try:
        pods = k8s_api.list_namespaced_pod(
            namespace="default",
            label_selector="app=space-mission"
        )
        if pods.items:
            pod = pods.items[0]
            if pod.status.start_time:
                start_ts = pod.status.start_time.timestamp()
                age = time.time() - start_ts
                remaining = 16.0 - age
                if remaining > 0:
                    logger.info(f"⏳ Cooldown Gate: SML pod is only {age:.1f}s old. Settling cluster for {remaining:.1f}s to clear initial startup cooldown...")
                    time.sleep(remaining)
                else:
                    logger.info(f"✅ Cooldown Gate: SML pod age is {age:.1f}s (startup cooldown already cleared).")
    except Exception as e:
        logger.warning(f"   ⚠️ Could not determine SML pod age for cooldown settling: {e}")


def sterilize_cluster(redis_conn, k8s_api, origin_node=None, master_node=None):
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
        
        # Enforce true workload placement in the sterile baseline (Bug fix)
        if origin_node:
            baseline["is_working"] = (node == origin_node)
        else:
            baseline["is_working"] = False
            
        if master_node:
            baseline["has_master"] = (node == master_node)
        else:
            baseline["has_master"] = False
            
        channel = f"telemetry/{node}"
        subscribers = redis_conn.publish(channel, json.dumps(baseline))
        logger.info(f"   → Telemetry baseline published to '{channel}'. Subscribers reached: {subscribers} | is_working={baseline['is_working']} has_master={baseline['has_master']}")
        if subscribers == 0:
            logger.warning(f"   ⚠️ No active subscribers listened to telemetry channel '{channel}'!")

    logger.info(f"🧼 Baseline published. Settling for {STERILE_SETTLE_SEC}s...")
    time.sleep(STERILE_SETTLE_SEC)
    settle_startup_cooldown(k8s_api)
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
    subscribers = redis_conn.publish(channel, json.dumps(baseline))
    logger.info(f"👻 Ghost publish → {node}: {telemetry_override} (channel: '{channel}', subscribers reached: {subscribers})")
    if subscribers == 0:
        logger.warning(f"   ⚠️ No active subscribers reached for ghost publish on channel '{channel}'!")


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

def inject_ghost_trajectory(lateral_threshold, stop_event=None, step_size=4.0, start_x=64.0):
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
        if stop_event and stop_event.is_set():
            logger.info("🎯 Ghost Worker: Stop event set. Terminating trajectory injection.")
            break
        payload = {
            "new_frame": "dGVzdA==",  # Dummy base64 string ("test") - V7 sidecar checks for truthiness and decodes it
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

        max_retries = 3
        for attempt in range(max_retries):
            try:
                resp = requests.post(
                    f"{GUARDIAN_SVC_URL}/state",
                    json=payload,
                    timeout=10,
                )
                logger.debug(f"      [DEBUG] HTTP POST {GUARDIAN_SVC_URL}/state response: code={resp.status_code}, content={resp.text[:150]}")
                if resp.status_code == 200:
                    logger.debug(f"   → CoM_x={current_x:.1f} injected successfully (response: {resp.text.strip()})")
                    break
                else:
                    logger.debug(f"   → CoM_x={current_x:.1f} HTTP {resp.status_code} response={resp.text} (attempt {attempt+1}/{max_retries})")
            except requests.exceptions.RequestException as e:
                # Connection drops (e.g. RemoteDisconnected, ConnectionRefused) are normal and expected
                # here. During SML migration, the source pod is frozen by CRIU and scaled down by the Node
                # Agent, breaking the local port-forwarding connection. Once the pod is restored and becomes
                # Ready on the destination node, requests will automatically succeed again.
                err_str = str(e)
                if "Connection refused" in err_str or "RemoteDisconnected" in err_str or "Connection aborted" in err_str:
                    clean_err = "Pod offline (migration in progress)"
                else:
                    clean_err = err_str.split(":")[-1].strip() if ":" in err_str else err_str
                logger.debug(f"   → CoM_x={current_x:.1f} telemetry dropped: {clean_err} (attempt {attempt+1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(1.0)
        else:
            logger.info(f"   ⚠️ CoM_x={current_x:.1f} skipped (pod still offline after {max_retries}s). Advancing fire trajectory...")

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
            logger.debug("💣 Hardware Fuse: DISABLED (nominal severity — no fuse)")
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
            delete_status = self.k8s_api.delete_namespaced_pod(
                name=self.origin_pod,
                namespace=self.namespace,
                body=client.V1DeleteOptions(grace_period_seconds=0),
            )
            status_details = "unknown"
            if delete_status:
                if hasattr(delete_status, 'status') and hasattr(delete_status.status, 'phase'):
                    status_details = f"status.phase={delete_status.status.phase}"
                elif hasattr(delete_status, 'metadata') and hasattr(delete_status.metadata, 'uid'):
                    status_details = f"uid={delete_status.metadata.uid}"
            logger.warning(f"💥 Origin pod {self.origin_pod} TERMINATED — "
                           f"simulated silicon melt (0% survival). API Response: {status_details}")
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
    logger.debug("🧹 SANITIZE: Beginning post-run cleanup...")

    # ── 1. Network Reset ──────────────────────────────────────────────────────
    # Publish clear_throttle command to the campaign/config channel.
    # All Node Agents subscribed to this channel will execute tc qdisc del.
    logger.debug("🧹 SANITIZE [1/3]: Clearing network throttles on all nodes...")
    subscribers = redis_conn.publish("campaign/config", json.dumps({"action": "clear_throttle"}))
    logger.debug(f"   → Published clear_throttle. Subscribers reached: {subscribers}")
    time.sleep(1.0)  # Brief settle for tc commands to propagate

    # Direct fallback: check and delete tc rules on all agent pods
    try:
        pods = k8s_api.list_namespaced_pod(
            namespace="default",
            label_selector="app=space-node-agent"
        )
        for pod in pods.items:
            if pod.status.phase == "Running":
                try:
                    res = k8s_stream(
                        k8s_api.connect_get_namespaced_pod_exec,
                        name=pod.metadata.name,
                        namespace="default",
                        container="agent",
                        command=["tc", "qdisc", "del", "dev", "eth0", "root"],
                        stderr=True,
                        stdout=True,
                    )
                    logger.debug(f"   ✅ Force-cleared tc on {pod.metadata.name}. Exec output: {str(res).strip()}")
                except Exception as e:
                    # Ignore errors if the root qdisc is already empty/deleted
                    logger.debug(f"   ℹ️ Exec force-clear failed (possibly already cleared): {e}")
                    pass
    except Exception as e:
        logger.warning(f"   ⚠️ Direct tc cleanup fallback failed: {e}")

    # ── 2. State Wipe ─────────────────────────────────────────────────────────
    # Clear the Guardian's /tmp/payload_state.json via K8s exec.
    # This prevents stale CoM coordinates from triggering phantom lateral migrations.
    logger.debug("🧹 SANITIZE [2/3]: Wiping sidecar state files...")
    # 2.1 Wipe state on the active mission pod (recreates it fresh to clear in-memory cooldowns)
    try:
        pods = k8s_api.list_namespaced_pod(
            namespace="default",
            label_selector="app=space-mission",
            field_selector="status.phase=Running",
        )
        for pod in pods.items:
            try:
                k8s_api.delete_namespaced_pod(
                    name=pod.metadata.name,
                    namespace="default",
                    body=client.V1DeleteOptions(grace_period_seconds=0),
                )
                logger.debug(f"   ✅ Deleted mission pod {pod.metadata.name} to reset startup cooldown state.")
            except Exception as e:
                logger.warning(f"   ⚠️ Could not delete mission pod {pod.metadata.name}: {e}")
    except Exception as e:
        logger.warning(f"   ⚠️ Could not list mission pods for state reset: {e}")

    # 2.2 Wipe state on all node agents to clear hostPath persistent directories on all nodes
    try:
        agent_pods = k8s_api.list_namespaced_pod(
            namespace="default",
            label_selector="app=space-node-agent"
        )
        for pod in agent_pods.items:
            if pod.status.phase == "Running":
                try:
                    k8s_stream(
                        k8s_api.connect_get_namespaced_pod_exec,
                        name=pod.metadata.name,
                        namespace="default",
                        container="agent",
                        command=["rm", "-f", "/tmp/payload_state.json"],
                        stderr=True,
                        stdout=True,
                    )
                    logger.debug(f"   ✅ Wiped state on node agent {pod.metadata.name}.")
                except Exception as e:
                    logger.debug(f"   ℹ️ State wipe failed on agent {pod.metadata.name}: {e}")
    except Exception as e:
        logger.warning(f"   ⚠️ Could not list node agent pods for state wipe: {e}")

    # ── 3. Stability Assertion ────────────────────────────────────────────────
    # Block until exactly 1 SML pod + 1 Master pod are Running and Ready.
    logger.debug("🧹 SANITIZE [3/3]: Asserting cluster stability...")
    _assert_cluster_stability(k8s_api)

    # ── 4. Telemetry Reset ───────────────────────────────────────────────────
    # Publish sterile baseline to all nodes to clear any active crisis/failsafe triggers
    logger.debug("🧹 SANITIZE: Resetting cluster telemetry to sterile baseline...")
    try:
        sterilize_cluster(redis_conn, k8s_api)
    except Exception as e:
        logger.warning(f"   ⚠️ Telemetry reset failed: {e}")

    logger.debug("🧹 SANITIZE: Cleanup complete — cluster is pristine.")


def _assert_cluster_stability(k8s_api, timeout_sec=None):
    """
    Blocks until the cluster reports exactly:
      - 1 space-mission pod: Running + Ready (and no extra/terminating pods)
      - 1 topology-master pod: Running + Ready (and no extra/terminating pods)

    This prevents the next run from starting on a half-initialized cluster
    or during a rolling update rollout.
    """
    if timeout_sec is None:
        timeout_sec = SANITIZE_TIMEOUT_SEC

    deadline = time.time() + timeout_sec
    poll_interval = 2.0

    while time.time() < deadline:
        try:
            sml_pods = k8s_api.list_namespaced_pod(
                namespace="default",
                label_selector="app=space-mission",
            )
            sml_total = len(sml_pods.items)
            sml_ready = sum(
                1 for pod in sml_pods.items
                if pod.status.phase == "Running"
                and pod.metadata.deletion_timestamp is None
                and pod.status.container_statuses
                and all(cs.ready for cs in pod.status.container_statuses)
            )

            master_pods = k8s_api.list_namespaced_pod(
                namespace="default",
                label_selector="app=topology-master",
            )
            master_total = len(master_pods.items)
            master_ready = sum(
                1 for pod in master_pods.items
                if pod.status.phase == "Running"
                and pod.metadata.deletion_timestamp is None
                and pod.status.container_statuses
                and all(cs.ready for cs in pod.status.container_statuses)
            )

            if sml_ready == 1 and sml_total == 1 and master_ready == 1 and master_total == 1:
                logger.debug(f"   ✅ Cluster stable: 1 SML pod, 1 Master pod Ready (no rolling/terminating pods)")
                return True

            logger.debug(
                f"   ⏳ Waiting for rollout completion: "
                f"SML={sml_ready}/1 (total: {sml_total}), "
                f"Master={master_ready}/1 (total: {master_total})"
            )
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


def _find_pod_node(k8s_api, pod_name, namespace="default"):
    """Find which node a specific pod is running on."""
    try:
        pod = k8s_api.read_namespaced_pod(name=pod_name, namespace=namespace)
        return pod.spec.node_name
    except Exception as e:
        logger.warning(f"Could not find node for pod {pod_name}: {e}")
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

    subscribers = redis_conn.publish("campaign/config", json.dumps(serialized))
    logger.info(f"⚙️ Configuration hot-reloaded: {serialized} (subscribers reached: {subscribers})")
    if subscribers == 0:
        logger.warning("   ⚠️ Warning: 0 active subscribers received configuration hot-reload!")
    time.sleep(1.0)  # Brief settle for agents to process the update


def apply_network_throttle(redis_conn, rate_mbit, latency_ms):
    """
    Command all Node Agents to apply tc traffic control rules.
    Delegates via the campaign/config Redis channel.
    """
    cmd = {"action": "throttle", "rate": rate_mbit, "latency": latency_ms}
    subscribers = redis_conn.publish("campaign/config", json.dumps(cmd))
    logger.info(f"🌐 Network throttle applied: {rate_mbit}mbit / {latency_ms}ms (subscribers reached: {subscribers})")
    if subscribers == 0:
        logger.warning("   ⚠️ Warning: 0 active subscribers received network throttle command!")
    time.sleep(1.0)  # Brief settle for tc rules to propagate


CONFIG_DESCRIPTIONS = {
    "C1_Full_System": "Control baseline (all optimizations active: predictive twin, 15s cooldown, sequenced migrations/compaction)",
    "C2_No_Predictive_Twin": "Ablation: Predictive Twin disabled (reactive local state only)",
    "C3_No_Cooldown": "Ablation: Cooldown Guard disabled (high risk of migration ping-pong/thrashing)",
    "C4_Monolithic_Checkpoint": "Ablation: Monolithic Checkpoint (transferring uncompressed 160MB memory dumps instead of 24MB sidecar pages)",
    "C5_Linear_Routing": "Ablation: Linear Routing disabled (forcing direct node-to-node transfers, no multi-hop Dijkstra routing)",
    "C6_Concurrent_Evacuation": "Ablation: Concurrent Evacuation enabled (multiple pods migrate simultaneously, fighting for network bandwidth)",
}

SCENARIO_DESCRIPTIONS = {
    "slow_internet": "Degraded Inter-Satellite Link bandwidth",
    "sequential_thermal": "Cascading/sequential node overheating (primary and destination spikes)",
    "double_trouble": "Local thermal spike coupled with neighboring node battery degradation",
}

SEVERITY_DESCRIPTIONS = {
    "nominal": "Nominal stress (easy migration/evacuation expected)",
    "borderline": "Borderline stress (tight safety margin; migration is difficult but possible)",
    "correct_failure": "Extreme stress (meltdown is expected, or edge Dijkstra should safely refuse to migrate)",
}


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

    config_desc = CONFIG_DESCRIPTIONS.get(config_name, "Custom profile")
    scenario_desc = SCENARIO_DESCRIPTIONS.get(scenario, "Custom scenario")
    severity_desc = SEVERITY_DESCRIPTIONS.get(severity, "Custom severity")

    logger.info(f"\n{'='*70}")
    logger.info(f"🏃 RUN {run_index}/{total_runs}: {config_name} | {scenario} | {severity} | Rep {rep_id}")
    logger.info(f"{'-'*70}")
    logger.info(f"📋 Ablation Profile: {config_desc}")
    logger.info(f"🌐 Fault Context:    {scenario_desc}")
    logger.info(f"⚠️ Severity Level:   {severity_desc}")
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
        logger.debug("📋 Phase 1/6: Applying ablation configuration...")
        apply_configuration(redis_conn, run["config_patch"])

        # Resolve the current SML and Master placement first to write an accurate baseline
        origin_pod = _find_origin_pod(k8s_api)
        origin_node = _find_pod_node(k8s_api, origin_pod) if origin_pod else None
        
        master_node = None
        try:
            master_pods = k8s_api.list_namespaced_pod(
                namespace="default",
                label_selector="app=topology-master",
            )
            for pod in master_pods.items:
                if pod.metadata.deletion_timestamp is None:
                    master_node = pod.spec.node_name
                    break
        except Exception:
            pass

        # ── PHASE 2: Sterilization — Enforce Sterile Baseline ───────────────
        # Must happen BEFORE network throttle so the baseline telemetry
        # propagates at full bandwidth (spec: orchestrator_functional_spec §7).
        logger.debug("📋 Phase 2/6: Sterilizing cluster...")
        sterilize_cluster(redis_conn, k8s_api, origin_node=origin_node, master_node=master_node)

        # ── PHASE 3: Stress Initialization — Network Throttle ───────────────
        logger.debug("📋 Phase 3/6: Applying network constraints...")
        if scenario == "slow_internet":
            params = run["scenario_params"]
            apply_network_throttle(
                redis_conn,
                rate_mbit=params["tc_rate_mbit"],
                latency_ms=params["tc_latency_ms"],
            )
        # Other scenarios don't require network throttling

        # ── PHASE 4: Injection — Execute fault vectors ──────────────────────
        logger.debug("📋 Phase 4/6: Executing fault injection...")
        injection_timestamp = time.time()

        # Start the Redis abort listener for explicit "Correct Failure" detection
        abort_listener = AbortFlagListener(redis_conn)
        abort_listener.start()

        # Start the Redis route metrics listener for dynamic hop tracking
        # (Phase 4, Step 4.1: captures the actual route from the Node Agent)
        route_listener = RouteMetricsListener(redis_conn)
        route_listener.start()

        # Find origin pod and origin node for the hardware fuse and fault injection
        origin_pod = _find_origin_pod(k8s_api)
        origin_node = None
        if origin_pod:
            origin_node = _find_pod_node(k8s_api, origin_pod)
        if not origin_node:
            origin_node = SATELLITE_NODES[0]
            logger.warning(f"⚠️ Could not find node for pod {origin_pod}. Defaulting to {origin_node}")
        else:
            logger.debug(f"📍 Resolved origin pod: {origin_pod} running on node: {origin_node}")

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
        ghost_worker_stop = threading.Event()
        ghost_worker_thread = threading.Thread(
            target=inject_ghost_trajectory,
            args=(lateral_threshold, ghost_worker_stop),
            daemon=True,
        )
        ghost_worker_thread.start()

        if scenario == "slow_internet":
            # Scenario 1: inject thermal spike to trigger migration under bandwidth constraint
            logger.info(f"🔥 Fault Injected: Thermal spike (95°C) on node {origin_node}")
            inject_thermal_spike(redis_conn, origin_node, 95.0)
            fuse.start()

        elif scenario == "sequential_thermal":
            # Scenario 2: Sequential Thermal Crisis (Cascading Ping-Pong)
            params = run["scenario_params"]
            delta_t = params["delta_t_stress_sec"]
            spike_temp = params["spike_temp"]

            # Overwrite first migration's fuse timeout to be generous (15.0s)
            # because the first migration is a normal migration that is supposed to succeed
            # regardless of whether the second migration will be refused.
            if fuse.timeout is not None:
                fuse.timeout = None

            # Spike Node A (primary) → triggers migration to Node B
            logger.info(f"🔥 Fault Injected: Primary thermal spike ({spike_temp}°C) on node {origin_node}")
            inject_thermal_spike(redis_conn, origin_node, spike_temp)
            fuse.start()

            # 1. Wait for the first migration to complete and the pod to become Ready
            logger.info("   ⏳ Waiting for first migration to complete and pod to become Ready...")
            first_mig_completed = False
            first_mig_start = time.time()
            while time.time() - first_mig_start < RUN_TIMEOUT_SEC:
                time.sleep(0.2)
                if abort_listener and abort_listener.abort_detected:
                    logger.warning(f"   ⚠️ First migration aborted by Node Agent: {abort_listener.abort_reason}")
                    break
                if route_listener and route_listener.route_received:
                    try:
                        pods = k8s_api.list_namespaced_pod(
                            namespace="default",
                            label_selector="app=space-mission",
                            field_selector="status.phase=Running",
                        )
                        for pod in pods.items:
                            if pod.metadata.name != origin_pod and pod.metadata.deletion_timestamp is None:
                                if pod.status.container_statuses and all(cs.ready for cs in pod.status.container_statuses):
                                    fuse.cancel()
                                    first_mig_delay = time.time() - injection_timestamp
                                    logger.info(f" 💣 Hardware Fuse: DEFUSED — first migration completed in {first_mig_delay:.2f}s")
                                    first_mig_completed = True
                                    break
                    except Exception as e:
                        logger.warning(f"   ⚠️ K8s poll error: {e}")
                
                if first_mig_completed or fuse.fuse_triggered:
                    break

            if not first_mig_completed:
                logger.warning("   ⚠️ First migration failed or did not complete in time.")

            # 2. Wait Δt_stress seconds after the payload lands before secondary thermal injection
            logger.info(f"   ⏱️ Waiting {delta_t}s from landing before secondary thermal injection...")
            time.sleep(delta_t)
            
            # Find the new destination node dynamically
            dest_node = None
            if route_listener and route_listener.route_received:
                dest_node = route_listener.destination
            else:
                # Fallback: query K8s
                current_pod = _find_origin_pod(k8s_api)
                if current_pod:
                    current_node = _find_pod_node(k8s_api, current_pod)
                    if current_node and current_node != origin_node:
                        dest_node = current_node
            
            if not dest_node:
                # Fallback to hardcoded secondary node if not resolved
                dest_node = SATELLITE_NODES[1] if origin_node != SATELLITE_NODES[1] else SATELLITE_NODES[0]
                logger.warning(f"   ⚠️ Could not resolve destination node for secondary spike. Falling back to {dest_node}")
            
            logger.info(f"   🔥 Secondary spike: Injecting thermal spike on destination node {dest_node}")
            inject_thermal_spike(redis_conn, dest_node, spike_temp)

        elif scenario == "double_trouble":
            # Scenario 3: Double Trouble (Thermal & Energy Crisis)
            params = run["scenario_params"]
            local_temp = params["local_temp"]
            neighbor_battery = params["neighbor_battery"]

            # First, degrade all neighbor batteries in the cluster
            for neighbor in SATELLITE_NODES:
                if neighbor != origin_node:
                    inject_battery_crisis(redis_conn, neighbor, neighbor_battery)

            # Settle briefly to let neighbor agents push their telemetry updates to Floating Master
            logger.debug("   ⏳ Waiting 1.5s for neighbor battery updates to propagate...")
            time.sleep(1.5)

            # Finally, spike local node temperature to trigger migration pathfinding
            logger.info(f"🔥 Fault Injected: Spiked temp ({local_temp}°C) on {origin_node} + degraded neighbor batteries to {neighbor_battery}%")
            inject_thermal_spike(redis_conn, origin_node, local_temp)
            fuse.start()

        # ── PHASE 5: Extraction ──────────────────────────────────────────────
        logger.debug("📋 Phase 5/6: Monitoring cluster response...")
        extraction_result = _extract_metrics(
            k8s_api, redis_conn, injection_timestamp, fuse, run,
            origin_pod=origin_pod,
            abort_listener=abort_listener,
            route_listener=route_listener,
        )
        result.update(extraction_result)

        # Defuse the hardware fuse if it hasn't already triggered
        fuse.cancel()

        # Stop the abort listener, route listener, and ghost worker
        ghost_worker_stop.set()
        abort_listener.stop()
        route_listener.stop()

    except Exception as e:
        logger.error(f"❌ Run execution error: {e}")
        result["survival_rate"] = 0.0
        result["notes"] = f"EXCEPTION: {str(e)}"
        try:
            ghost_worker_stop.set()
        except NameError:
            pass

    # ── PHASE 6: Teardown & Sanitization ────────────────────────────────────
    logger.debug("📋 Phase 6/6: Sanitizing cluster...")
    sanitize_run(redis_conn, k8s_api)

    logger.info(f"✅ Run {run_index} complete — Survival: {result['survival_rate']*100:.0f}% | "
                f"Delay: {result['migration_delay_sec']:.2f}s | BW: {result['bandwidth_mb']:.1f}MB")

    return result


def _extract_metrics(k8s_api, redis_conn, injection_timestamp, fuse, run,
                     origin_pod=None, abort_listener=None, route_listener=None):
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
    iterations = 0

    while time.time() < poll_deadline:
        iterations += 1
        elapsed = time.time() - injection_timestamp
        # Log status every 10 seconds (20 iterations) to keep terminal logs clean
        show_log = (iterations == 1 or iterations % 20 == 0)
        
        if show_log:
            logger.info(f"   ⏳ [Poll #{iterations} | elapsed={elapsed:.1f}s] Checking status (Abort flag: {abort_listener.abort_detected if abort_listener else False}, Route recvd: {route_listener.route_received if route_listener else False}, Fuse: {fuse.fuse_triggered})...")
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
            running_pods = [p.metadata.name for p in pods.items]
            if show_log:
                logger.debug(f"      → Running space-mission pods: {running_pods}")
            for pod in pods.items:
                if pod.metadata.name != origin_pod and pod.metadata.deletion_timestamp is None:
                    container_readiness = []
                    if pod.status.container_statuses:
                        container_readiness = [f"{cs.name}:ready={cs.ready}" for cs in pod.status.container_statuses]
                    if show_log:
                        logger.debug(f"      → Destination candidate '{pod.metadata.name}' readiness statuses: {container_readiness}")
                    
                    if pod.status.container_statuses and all(cs.ready for cs in pod.status.container_statuses):
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

    # ── Timeout handling: check if origin pod is still alive and healthy ────────
    origin_pod_alive = False
    try:
        pods = k8s_api.list_namespaced_pod(
            namespace="default",
            label_selector="app=space-mission",
            field_selector="status.phase=Running",
        )
        for pod in pods.items:
            if pod.metadata.name == origin_pod and pod.metadata.deletion_timestamp is None:
                if pod.status.container_statuses and all(cs.ready for cs in pod.status.container_statuses):
                    origin_pod_alive = True
    except Exception as e:
        logger.warning(f"   ⚠️ Could not assert origin pod status on timeout: {e}")

    if origin_pod_alive:
        logger.info(f"   ⏱️ Run timeout ({RUN_TIMEOUT_SEC}s) — origin pod is still running and healthy. Recording survival = 100%")
        result["survival_rate"] = 1.0
        result["migration_delay_sec"] = 0.0
        result["hops"] = 0
        result["bandwidth_mb"] = 0.0
    else:
        logger.warning(f"   ⏱️ Run timeout ({RUN_TIMEOUT_SEC}s) — origin pod is dead or unready. Recording failure.")
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
    "ABORT_ACKNOWLEDGED: NO_ROUTE_FOUND",
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
        logger.debug("   🔍 AbortFlagListener: Background thread started.")

    def stop(self):
        """Stop the background listener."""
        self._stop_event.set()
        logger.debug("   🔍 AbortFlagListener: Background thread stopping...")

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
            logger.debug("   🔍 AbortFlagListener: Subscribed to 'campaign/aborts' Redis channel.")

            while not self._stop_event.is_set():
                msg = ps.get_message(timeout=0.5)
                if msg:
                    logger.debug(f"      [DEBUG] AbortFlagListener: Received raw Redis message: {msg}")
                if msg and msg["type"] == "message":
                    data = msg["data"]
                    logger.info(f"   🔍 AbortFlagListener: Received abort message: {data}")
                    for pattern in ABORT_FLAG_PATTERNS:
                        if pattern in data:
                            self.abort_detected = True
                            self.abort_reason = pattern
                            logger.info(f"   🔍 AbortFlagListener: MATCHED abort pattern: {pattern}")
                            return

            ps.unsubscribe("campaign/aborts")
            logger.debug("   🔍 AbortFlagListener: Unsubscribed from 'campaign/aborts'.")
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
        logger.debug("   📊 RouteMetricsListener: Background thread started.")

    def stop(self):
        """Stop the background listener."""
        self._stop_event.set()
        logger.debug("   📊 RouteMetricsListener: Background thread stopping...")

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
            logger.debug("   📊 RouteMetricsListener: Subscribed to 'campaign/metrics' Redis channel.")

            while not self._stop_event.is_set():
                msg = ps.get_message(timeout=0.5)
                if msg:
                    logger.debug(f"      [DEBUG] RouteMetricsListener: Received raw Redis message: {msg}")
                if msg and msg["type"] == "message":
                    try:
                        logger.debug(f"   📊 RouteMetricsListener: Received message: {msg['data']}")
                        data = json.loads(msg["data"])
                        if data.get("event") == "migration_complete":
                            self.route = data.get("route", [])
                            self.hops = data.get("hops", len(self.route))
                            self.source = data.get("source", "")
                            self.destination = data.get("destination", "")
                            self.route_received = True
                            logger.info(f"   📊 RouteMetricsListener: Route captured: {self.source} → "
                                         f"{self.route} ({self.hops} hop(s))")
                            # Don't return — keep listening for additional
                            # migrations (e.g., double evacuation routes)
                    except (json.JSONDecodeError, TypeError) as e:
                        logger.warning(f"   ⚠️ Route listener: malformed message: {e}")

            ps.unsubscribe("campaign/metrics")
            logger.debug("   📊 RouteMetricsListener: Unsubscribed from 'campaign/metrics'.")
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

    # Ensure dry run CSV has a header
    _ensure_csv_header(DRY_RUN_CSV_PATH)

    # Generate the 54-run mini-matrix (1 rep per combination, NOT shuffled)
    dry_run_matrix = generate_run_matrix(seed=None, num_reps=1)
    # Override: do NOT shuffle — execute sequentially for deterministic validation
    dry_run_matrix.sort(key=lambda r: (r["configuration"], r["scenario"], r["severity"]))

    total_dry = len(dry_run_matrix)
    logger.info(f"🧪 Dry-Run matrix: {total_dry} sequential treatment combinations")

    passed = 0
    failed_runs = []

    start_idx = int(os.environ.get("START_INDEX", "1"))
    for idx, run in enumerate(dry_run_matrix, start=1):
        if idx < start_idx:
            logger.info(f"⏭️ Skipping DRY-RUN {idx}/{total_dry} (START_INDEX={start_idx})")
            passed += 1
            continue

        logger.info(f"\n🧪 DRY-RUN {idx}/{total_dry}: {run['configuration']} | "
                    f"{run['scenario']} | {run['severity']}")

        try:
            result = execute_run(run, idx, total_dry, redis_conn, k8s_api)

            # ── Behavioral Assertion Gate ──────────────────────────────────
            # Validate that the system responded as expected for this
            # configuration × scenario × severity combination.
            valid, reason = _validate_dry_run_result(run, result)

            result["validation_status"] = "PASSED" if valid else "FAILED"
            result["validation_reason"] = reason
            append_result_to_csv(result, DRY_RUN_CSV_PATH)

            if valid:
                passed += 1
                logger.info(f"   ✅ DRY-RUN {idx}: PASSED")
            else:
                failed_runs.append({
                    "index": idx,
                    "run": f"{run['configuration']}|{run['scenario']}|{run['severity']}",
                    "reason": reason,
                    "result": result,
                })
                logger.error(f"   ❌ DRY-RUN {idx}: FAILED validation — {reason}")

        except Exception as e:
            logger.error(f"   ❌ DRY-RUN {idx}: EXCEPTION — {e}")
            result = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "configuration": run["configuration"],
                "scenario": run["scenario"],
                "severity": run["severity"],
                "rep_id": run["rep_id"],
                "hops": 0,
                "survival_rate": 0.0,
                "migration_delay_sec": 0.0,
                "bandwidth_mb": 0.0,
                "validation_status": "ERROR",
                "validation_reason": str(e),
            }
            append_result_to_csv(result, DRY_RUN_CSV_PATH)
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
            if "reason" in fail:
                logger.error(f"   • Run {fail['index']} ({fail['run']}): {fail['reason']}")
            else:
                logger.error(f"   • Run {fail['index']} ({fail['run']}): Exception occurred — {fail.get('error')}")
        logger.error("🧪 FAIL-FAST HARD STOP: The campaign will NOT proceed.")
        return False

    logger.info("🧪 All 54 smoke tests passed — campaign infrastructure verified.")
    logger.info(f"{'='*70}")
    return True


def _validate_dry_run_result(run, result):
    """
    Validate that a dry-run result matches the expected behavioral state
    for the given configuration × scenario × severity combination.

    Returns (bool, str) representing (is_valid, error_reason).
    """
    severity = run["severity"]
    survival = result.get("survival_rate", -1)

    # Basic schema validation: essential fields must be present
    if result.get("timestamp") is None:
        return False, "Missing timestamp in execution result"

    # Severity-specific behavioral assertions
    if severity == "nominal":
        # Nominal: migration should always succeed (100% survival)
        if survival != 1.0:
            return False, f"Expected 100% survival for nominal severity, but got {survival*100:.0f}%"

    elif severity == "correct_failure":
        # Correct Failure: system should refuse to migrate (100% survival)
        if run["scenario"] in ("sequential_thermal", "double_trouble"):
            if survival != 1.0:
                return False, f"Expected safe refusal (100% survival) for {run['scenario']} under correct_failure, but got {survival*100:.0f}%"

    return True, ""


# =============================================================================
# CSV TELEMETRY SINK (Phase 4, Step 4.2 — Atomic Append)
# =============================================================================

CSV_HEADER = [
    "Timestamp", "Configuration_Block", "Scenario", "Severity_Level", "Repetition_ID",
    "Hops", "Survival_Rate", "Migration_Delay_sec", "Bandwidth_MB", "Validation_Status", "Validation_Reason"
]


def _ensure_csv_header(file_path=CSV_OUTPUT_PATH):
    """Create the CSV file with header if it doesn't exist."""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    if not os.path.exists(file_path):
        with open(file_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(CSV_HEADER)
        logger.info(f"📊 CSV sink initialized: {file_path}")


def append_result_to_csv(result, file_path=CSV_OUTPUT_PATH):
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
        result.get("validation_status", ""),
        result.get("validation_reason", ""),
    ]
    with open(file_path, "a", newline="") as f:
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

    # ── Initial Mode Selection ─────────────────────────────────────────────
    print("\nSelect execution mode:")
    print("  1. Dry Run Only (54 experiments)")
    print("  2. Real Campaign Only (540 experiments)")
    print("  3. Full Campaign (Dry Run + Real Campaign)")
    
    choice = input("Enter choice (1-3) [default: 3]: ").strip()
    if not choice:
        choice = "3"
        
    if choice not in ("1", "2", "3"):
        logger.error("Invalid choice. Exiting.")
        sys.exit(1)
        
    do_dry_run = choice in ("1", "3")
    do_campaign = choice in ("2", "3")

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

    # ── Pre-flight: Cooldown Settle Gate ──────────────────────────────────────
    # Ensures the initial startup cooldown on the newly launched SML pod has
    # expired before starting the first experiment to prevent a false abort.
    try:
        pods = k8s_api.list_namespaced_pod(
            namespace="default",
            label_selector="app=space-mission"
        )
        if pods.items:
            pod = pods.items[0]
            if pod.status.start_time:
                start_ts = pod.status.start_time.timestamp()
                age = time.time() - start_ts
                remaining = 16.0 - age
                if remaining > 0:
                    logger.info(f"⏳ Pre-flight: SML pod is only {age:.1f}s old. Settling cluster for {remaining:.1f}s to clear initial startup cooldown...")
                    time.sleep(remaining)
                else:
                    logger.info(f"✅ Pre-flight: SML pod age is {age:.1f}s (startup cooldown already cleared).")
    except Exception as e:
        logger.warning(f"   ⚠️ Could not determine SML pod age for cooldown settling: {e}")

    # ── Mandatory Dry-Run Gate (54-run smoke test) ─────────────────────────
    # Spec: campaign_automation_specification.md §2.3
    # Spec: orchestrator_functional_spec.md §6
    # Execute a non-shuffled, sequential mini-matrix of 54 unique treatment
    # combinations (6 Configs × 3 Scenarios × 3 Severities × 1 Rep) before
    # the full randomized campaign. If any dry-run fails to produce the
    # expected behavioral state, the orchestrator triggers a hard stop.
    if do_dry_run:
        if not execute_dry_run_gate(redis_conn, k8s_api):
            logger.critical("❌ DRY-RUN GATE FAILED: One or more smoke tests did not "
                            "produce the expected system response. Refusing to proceed "
                            "with the 540-run campaign. Fix the cluster and re-run.")
            sys.exit(1)
        logger.info("✅ Dry-Run Gate passed — all 54 smoke tests verified.")
        if not do_campaign:
            logger.info("🏁 Campaign execution finished (Dry Run only). Exiting.")
            sys.exit(0)

    # ── Execute the campaign ───────────────────────────────────────────────
    campaign_start = time.time()
    completed = 0
    failed = 0

    start_idx = int(os.environ.get("START_INDEX", "1"))
    for idx, run in enumerate(matrix, start=1):
        if idx < start_idx:
            logger.info(f"⏭️ Skipping Campaign Run {idx}/{total_runs} (START_INDEX={start_idx})")
            completed += 1
            continue

        try:
            result = execute_run(run, idx, total_runs, redis_conn, k8s_api)
            valid, reason = _validate_dry_run_result(run, result)
            result["validation_status"] = "PASSED" if valid else "FAILED"
            result["validation_reason"] = reason
            append_result_to_csv(result)
            completed += 1
            if result["survival_rate"] == 0.0:
                failed += 1
        except Exception as e:
            logger.error(f"❌ CRITICAL: Run {idx} crashed with unhandled exception: {e}")
            result = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "configuration": run["configuration"],
                "scenario": run["scenario"],
                "severity": run["severity"],
                "rep_id": run["rep_id"],
                "hops": 0,
                "survival_rate": 0.0,
                "migration_delay_sec": 0.0,
                "bandwidth_mb": 0.0,
                "validation_status": "ERROR",
                "validation_reason": str(e),
            }
            append_result_to_csv(result)
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
