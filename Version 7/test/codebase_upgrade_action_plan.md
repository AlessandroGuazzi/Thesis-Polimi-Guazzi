# Test Campaign Implementation Plan

This plan outlines the sequential steps required to build out the deterministic test harness for the Space Cloud V7 ablation campaign. The goal is to safely transition the cluster from a stochastic, live environment to a strictly controlled, programmatic test bed.

## Proposed Changes

We will approach this in four distinct phases to ensure stability and verify functionality progressively.

---

### Phase 1: Ablation Parameterization & Edge Agent Refactoring

**Goal:** Enable the Kubernetes cluster and edge nodes to dynamically toggle optimization features without requiring container rebuilds, while avoiding rolling restart overhead.

1. **Create the K8s ConfigMap Manifest (`2_orbit_payload/k8s/campaign-ablation-config.yaml`)**
   - Create a Kubernetes `ConfigMap` to define the baseline startup defaults for the campaign.
   - Include configuration keys for all architectural ablations:
     - `ENABLE_PREDICTIVE_TWIN`: (Boolean) Toggles the 30s lookahead thermal/energy model.
     - `COOLDOWN_SEC`: (Float) Global suppression timer to prevent ping-pong migrations.
     - `MONOLITHIC_CHECKPOINT`: (Boolean) Toggles Phase C (whether to checkpoint just the 24MB sidecar or the 160MB monolith).
     - `ENABLE_LOG_UTILITY`: (Boolean) Toggles Phase D Dijkstra cost function (Logarithmic vs Linear).
     - `ENABLE_SEQUENCED_MIGRATION`: (Boolean) Toggles concurrent vs sequenced double evacuations.
   - Include baseline hardware thresholds: `T_SAFE`, `T_FUSE`, `B_SAFE`, `B_FUSE`, `LATERAL_THRESHOLD`.

2. **Parameterize & Add Redis Hot-Reload API to the Node Agent (`2_orbit_payload/infrastructure/node_agent/node_agent.py`)**
   - **Global State Dictionary:** Replace the static `T_SAFE`, `T_FUSE`, etc., `os.getenv` declarations with a global dictionary (e.g., `ABLATION_CONFIG`) populated initially from environment variables.
   - **Redis Pub/Sub Listener:** Implement a new dedicated background thread in `main()` that subscribes to a Redis channel `campaign/config`.
   - **Hot-Reload Logic:** When a JSON message is received on `campaign/config`, update the `ABLATION_CONFIG` dictionary in-memory immediately.
   - **Conditional Logic Wrappers:**
     - *Predictive Twin:* Wrap the `VirtualSatellite.predict_future()` call in a check for `ABLATION_CONFIG["ENABLE_PREDICTIVE_TWIN"]`. If false, bypass prediction and rely purely on the reactive fuses.
     - *Cooldown Timer:* Replace the hardcoded `COOLDOWN_SEC = 15.0` with `ABLATION_CONFIG["COOLDOWN_SEC"]`.
     - *Monolithic Checkpoint:* Modify `_request_checkpoint` and `_rebuild_and_deploy`. If `MONOLITHIC_CHECKPOINT` is true, the agent must target the 160MB `payload-phoenix` container instead of the `sidecar-guardian` to simulate monolithic transfer overhead.
     - *Sequenced Migration:* In `_run_predictive_double_evacuation`, check `ENABLE_SEQUENCED_MIGRATION`. If false, trigger `_run_migration` and `_run_master_migration` concurrently without the `wait_for_sml_readiness` blocking barrier.
     - *Dijkstra Arguments:* Update the `topology_redis.evalsha` call to pass `ENABLE_LOG_UTILITY` as a new `ARGV`.

3. **Update the Dijkstra Pathfinding Script (`2_orbit_payload/infrastructure/dijkstra.lua`)**
   - Accept the new `ENABLE_LOG_UTILITY` flag as a script argument (`ARGV[6]`).
   - Implement conditional routing logic:
     - If `ENABLE_LOG_UTILITY` is true, calculate the edge cost using the logarithmic utility function for temperature and battery (prioritizing extreme resource exhaustion).
     - If false, fallback to a standard linear cost equation.

4. **Add Network Throttling Executor (`tc` wrapper) to the Node Agent (`node_agent.py` & `apply_tc_throttling.sh`)**
   - **Translate Script Logic:** Port the `tc qdisc` commands from `2_orbit_payload/ops/apply_tc_throttling.sh` into a new Python function `apply_network_throttle(rate_mbit, latency_ms)` inside `node_agent.py`.
   - **Subprocess Execution:** The Python function should execute `subprocess.run(["tc", "qdisc", "add", "dev", "eth0", "root", "handle", "1:", "tbf", ...])` directly on the host interface.
   - **Reset Function:** Create a `clear_network_throttle()` function to execute `tc qdisc del dev eth0 root`.
   - **Redis Command Handler:** Extend the Redis `campaign/config` listener to accept network commands (e.g., `{"action": "throttle", "rate": 50, "latency": 40}`) and trigger the corresponding Python functions.

5. **Update Node Agent Container Dependencies (`2_orbit_payload/infrastructure/node_agent/Dockerfile.agent`)**
   - Add `iproute2` to the `apt-get install` list. The `iproute2` package provides the `tc` binary, which the Python script needs to execute the network throttling commands locally from within the container.

6. **Expose K8s DaemonSet to ConfigMap and Host Network**
   - In the deployment manifests for the Node Agent (DaemonSet), mount the `campaign-ablation-config` ConfigMap as environment variables using `envFrom: configMapRef`.
   - Ensure the Node Agent DaemonSet is deployed with `hostNetwork: true` and `securityContext.privileged: true` so the `tc` commands affect the physical satellite node's `eth0` interface, not just an isolated container namespace.

7. **Prepare Dual-Container Pod for Monolithic Toggle (`2_orbit_payload/k8s/pod-dual-container.yaml`)**
   - Review and ensure the `payload-phoenix` container can be checkpointed via CRIU if `MONOLITHIC_CHECKPOINT` is triggered. The Node Agent's hot-reload logic will dynamically swap the container target in the Kubelet API request.

---

### Phase 2: Workload Throttling & Environmental Decoupling

**Goal:** Freeze out background stochastic noise and ensure the AI workload maintains a predictable, static memory footprint for deterministic CRIU migrations.

1. **Throttle the Inference Worker (`2_orbit_payload/src/inference-workload/tinysml_worker.py`)**
   - **Environment Toggle:** Add a startup check in `main()` for `os.getenv("CAMPAIGN_MODE", "False") == "True"`.
   - **Bypass UDP & Logic:** If active, completely bypass the `socket.bind()` and UDP `recvfrom` loop. The worker must not process any real network frames.
   - **Force Memory Footprint:** The worker MUST explicitly call `_ensure_session()` before entering its idle loop. Normally, the ONNX model is lazy-loaded on Day 5. Calling it immediately forces the container to allocate its full ~160MB memory footprint. This is absolutely critical to ensure the `MONOLITHIC_CHECKPOINT` ablation tests face the correct network transfer overhead during migration.
   - **Idle Loop:** After memory is allocated, enter an infinite `time.sleep(10)` loop. No dummy math is needed because the system's thermal accumulation is calculated logically based on the `has_sml` Kubernetes placement flag, not actual CPU utilization.

2. **Hard-Kill the Ground Station Streamer (`3_ground_station/data_streamer.py`)**
   - **Fail-Safe Abort:** Implement a fail-safe block at the very top of `main()`: if `CAMPAIGN_MODE=True`, log a warning ("Campaign Mode active: Data stream deactivated") and execute `sys.exit(0)`.
   - **Justification:** This guarantees that even if a legacy bash script attempts to launch the streamer, it will immediately abort. This ensures absolutely zero stochastic UDP packets reach the edge, preventing interference with the Orchestrator's precise "Ghost Worker" HTTP trajectory injections.

3. **Deactivate the Physical Simulator (`2_orbit_payload/infrastructure/environment_sim.py`)**
   - **Fail-Safe Abort:** Implement a similar fail-safe block at the start of `main()`: if `CAMPAIGN_MODE=True`, log that the "Campaign Orchestrator (Ghost Publisher) is assuming total control" and execute `sys.exit(0)`.
   - **Justification:** This entirely shuts down the simulator’s 1.0Hz loop. If left running, the simulator would constantly publish normal orbital physics to the Redis bus, continuously fighting and overwriting the Orchestrator's deterministic `telemetry/{NODE_NAME}` flatlined baseline and thermal injections.

4. **Inject Campaign Mode into Deployments (`ops/` or K8s Manifests)**
   - Update the relevant deployment scripts (e.g., `ops/start_system.sh` or the payload deployment YAMLs) to securely inject `CAMPAIGN_MODE=True` into both the cluster pods and the local Python environments when initializing the evaluation campaign.

---

### Phase 3: The Python Campaign Orchestrator

**Goal:** Build the central "God-Mode" runner (`test/campaign_runner.py`) that strictly enforces the DoE matrix, deterministically injects faults, and evaluates system response.

1. **Scaffold the Orchestrator Service (`test/campaign_runner.py`)**
   - **Dependencies:** Import `kubernetes` (for pod tracking and termination), `redis` (for config hot-reloading and telemetry ghost-publishing), `requests` (for Ghost Worker lateral tracking injections), and `csv` (for telemetry sinking).
   - **Environment Setup:** Configure the runner to authenticate using the in-cluster service account token (when running as a Pod) or via the local `~/.kube/config` (when running locally for debugging).

2. **Implement Matrix Generation & Randomization**
   - **Matrix Engine:** Write a generator that crosses: Configs (6) × Scenarios (3) × Severities (3) × Reps (10) = 540 total runs.
   - **Seeded Shuffle:** Use `random.seed(42)` (or a logged timestamp) before calling `random.shuffle()`. This guarantees the randomization is mathematically sound while remaining fully reproducible if the campaign crashes midway and needs to be restarted from a specific index.

3. **Implement the "Sterile Baseline" & "Ghost Publisher" (Hardware Faults)**
   - **Sterilization Protocol:** At the start of each run loop, the orchestrator must publish `{"temp": 45.0, "battery": 100.0, "is_working": True}` to `telemetry/{NODE_NAME}` for all nodes in the cluster. It must then `time.sleep()` to let the cluster settle into a zero-variance state.
   - **Deterministic Spikes:** Create targeted injection functions. For example, for the *Sequential Thermal Crisis* scenario, the orchestrator publishes 95°C to Node A, waits for the exact scenario-dictated $\Delta T_{stress}$ (e.g., 5s, 16s, or 30s), and then immediately publishes 95°C to Node B.

4. **Implement the "Ghost Worker" (Visual/Lateral Injection)**
   - **API Bypass:** Rather than waiting for UDP data, the orchestrator acts as the ONNX worker.
   - **Trajectory Loop:** Execute an `HTTP POST /state` directly to the Guardian Sidecar (`http://space-mission-svc:80`).
   - **Payload Forging:** Submit a forged payload containing `{"center_of_mass": {"x": <incremented_value>, "y": 64.0}, "fire_pixel_count": 100}`.
   - **Boundary Breach:** Increment the X coordinate programmatically until it breaches the `LATERAL_THRESHOLD` (e.g., moving from 64 to < 8), instantly triggering the Node Agent's lateral pathfinding.

5. **Implement the Calibrated Virtual Hardware Fuse**
   - **Asynchronous Death Timer:** Immediately upon injecting a critical thermal/battery spike, start an asynchronous timer thread.
   - **Severity-Dictated Calibration:** If the run is "Borderline", allow a slightly longer fuse (e.g., 4.0s) to observe if CRIU snapshot I/O variance can squeeze through. If "Correct Failure", use an ultra-tight fuse (e.g., 1.5s).
   - **Simulated Meltdown:** If the K8s API confirms the destination Pod is not `Running` and `Ready` before the fuse expires, use `client.CoreV1Api().delete_namespaced_pod()` to instantly terminate the origin Pod. This mimics a hardware thermal shutdown and yields a 0% survival rate.

6. **Implement Run Sanitization & Teardown Routine**
   - **Network Reset:** Publish a JSON command to the `campaign/config` Redis channel instructing all Node Agents to execute `tc qdisc del dev eth0 root`, clearing bandwidth throttles.
   - **State Wipe:** Clear `payload_state.json` inside the sidecar via a K8s `exec` command or HTTP reset endpoint.
   - **Stability Assertion:** Block the orchestrator loop by polling K8s until exactly one SML pod and one Master pod are verified as `Running` and fully `Ready`.

---

### Phase 4: Telemetry CSV Sinks & Data Collection

**Goal:** Capture all independent and dependent variables accurately and append them directly to persistent storage.

1. **Implement Dynamic Metric Extraction**
   - **Migration Delay Calculation:** Calculate the exact `Migration_Delay_sec` by subtracting the injection timestamp from the exact timestamp the Kubernetes API reports the destination pod transitioning to `Ready`.
   - **Intelligent Success Logging ("Correct Failures"):** If the orchestrator detects an intentional, self-preserving abort logged by the Node Agent (e.g., `[ABORT_ACKNOWLEDGED: COOLDOWN_LOCK]`), it must record `Survival_Rate = 1.0 (100%)` and `Migration_Delay_sec = 0.0`. It correctly refused to migrate, meaning the cluster survived.

2. **Deploy the Atomic CSV Sink**
   - **File I/O:** At the conclusion of every run, open `evaluation_data/campaign_results.csv` in `"a"` (append) mode.
   - **Data Schema:** Write a single row: `Timestamp, Configuration, Scenario, Severity, Rep_ID, Hops, Survival_Rate, Migration_Delay, Bandwidth_MB`.
   - **Dynamic Bandwidth Tracking:** Automatically calculate the `Bandwidth_MB` field. If the current run configuration has `MONOLITHIC_CHECKPOINT=True`, record `160 * Hops`. If `False`, record `24 * Hops`.

## Verification Plan

### Automated Tests

- **The Dry-Run Gate:** Once Phase 3 is complete, we will execute a mandatory pre-flight check inside `campaign_runner.py`. The runner will execute a non-shuffled mini-matrix of exactly 5 runs. The runner will assert that:
  - Configuration hot-reloads apply instantly over Redis.
  - The Ghost Publisher successfully flatlines the baseline.
  - The Node Agents successfully apply `tc` throttling when commanded.
  - The Virtual Hardware Fuse correctly deletes an origin pod if the timer is intentionally set too low.
  - The CSV sink appends the 5 rows without file lock errors.

### Manual Verification

- We will monitor the `kubectl get pods -w` stream and the Node Agent logs simultaneously to ensure the orchestrator's sanitization protocol correctly resets the environment between each of the 5 dry-runs without manual intervention.
