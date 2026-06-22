# Campaign Implementation & Automation Manual

## Introduction: The Deterministic Automation Architecture

This document defines the automation architecture and implementation requirements designed to execute the 540-run Factorial Experimental Campaign for the **Space Cloud V7** orchestration layer.

To ensure the statistical validity of the Design of Experiments (DoE) and to satisfy strict Analysis of Variance (ANOVA) assumptions, the campaign execution must be entirely programmatic and completely decoupled from stochastic environmental variance.

This manual is structured into four core implementation areas:

1. **ConfigMap Parameterization:** Dynamic configuration of edge agent ablation modes.
2. **The Deterministic Orchestrator:** The programmatic "God-Mode" runner that handles Sterile Baselines, Redis Ghost Publishing, HTTP Ghost Worker injections, and pre-campaign smoke testing.
3. **CSV Telemetry Sinks:** Upgrading logging infrastructure to write response metrics directly to standardized datasets.
4. **Factorial DoE Scaling:** Multi-factor analysis setup.

---

## 1. Parameterize the Ablation via ConfigMaps

To support rapid configuration changes without rebuilding container images, the system configurations (Ablation levels) are parameterized using Kubernetes resources.

### Architectural Setup

* **Externalize Variables:** Modify agent and routing source code to read configuration flags from environment variables (e.g., `ENABLE_PREDICTIVE_TWIN`, `COOLDOWN_SEC`).
* **Create a ConfigMap:** Centralize all independent variables in a Kubernetes ConfigMap named `campaign-ablation-config`.
* **Execution Strategy & Optimization:** To avoid the massive overhead of rolling DaemonSet restarts (~15-30 seconds per run, totaling 2.5 to 4.5 hours of overhead over the 540-run campaign), configurations are hot-reloaded. While the ConfigMap serves as the source of truth for initial setup, the Orchestrator publishes live configuration updates directly to the Node Agents via a dedicated Redis configuration channel or HTTP endpoint (e.g., `POST /config`), enabling sub-second, zero-restart runtime toggling of ablation flags.

---

## 2. Automate the Orchestrator (The "Campaign Runner")

To eliminate temporal biases, confounding variables, and human error, all 540 runs are executed by a centralized Python Pod (`campaign-runner`) deployed directly within the cluster.

Crucially, to maintain strict variance control, the real-world stochastic components (`environment_sim.py` and `data_streamer.py`) are **deactivated** during the campaign. The Orchestrator assumes total control of the data and control planes using the following strategies:

### 2.1 The "Sterile Baseline" & Ghost Publisher (Hardware Limits)

To test Phase A (Predictive Twin) and Phase B (Cooldown Damping) without background orbital noise triggering false migrations, the Orchestrator acts as a **Ghost Publisher**.

* **Sterile Baseline:** At the start of every run, the Orchestrator publishes a mathematically flat "Nominal State" (e.g., exactly 45.0°C, 100% Battery) to all `telemetry/{NODE_NAME}` Redis channels. This prevents any autonomous migrations.
* **Deterministic Injection:** At precise timestamps, the Orchestrator constructs exact JSON payloads and publishes them to the Redis bus, instantly forcing hardware states to targeted stress limits (e.g., 95°C) to validate migration responsiveness.

### 2.2 The "Ghost Worker" (Visual Boundary Limits)

To evaluate lateral boundary tracking capabilities without relying on randomized Machine Learning fire datasets, the system employs **Data-Plane Decoupling**.

* **Throttled Payload:** The active ML worker (`tinysml_worker.py`) is throttled to a static idle-load, fixing its thermal and memory footprint (ensuring exact 24MB/160MB CRIU checkpoints) but stopping random coordinate generation.
* **HTTP Trajectory Injection:** The Orchestrator natively resolves the `space-mission-svc` K8s Service and executes `HTTP POST /state` requests directly to the Guardian Sidecar. It incrementally steps the synthetic Center of Mass (CoM) coordinates toward the swath edge (e.g., $X=128$) at a mathematically precise velocity ($v_{lat}$).
* **Execution:** The Sidecar transparently commits this vector to its shared local volume (`/tmp/payload_state.json`), natively triggering the Node Agent to route laterally with millisecond precision.

### 2.3 Pre-Campaign Smoke Testing (The Dry-Run Phase)

To guarantee the programmatic validity of the campaign runner before initiating the extensive 540-run randomized execution, the Orchestrator includes a mandatory, non-randomized validation gate.

* **Deterministic Single-Pass Matrix:** Before shuffling the execution order, the Orchestrator compiles a mini-matrix comprising exactly 1 sample run for each of the 54 unique treatment combinations (6 Configurations × 3 Scenarios × 3 Severity Levels = 54 dry-runs).
* **Behavioral Assertion Gate:** During this phase, the execution sequence is sequential. The Orchestrator intercepts cluster states and validates that the underlying operating system and kernel tools respond exactly as specified (e.g., asserting that `tc` locks the link to exactly 6.4 Mbps and verifying that Redis registers a programmatic abort flag under `Correct Failure` profiles).
* **Fail-Fast Hard Stop:** If any of the 54 dry-run passes fails to produce the expected telemetry schema or system response, the Orchestrator triggers an absolute execution halt, preventing the multi-hour factorial campaign from running on an unverified cluster environment.

### 2.4 Network Throttling, Sanitization, & Calibration

To maintain absolute determinism and prevent resource leaks or cross-contamination between randomized runs, the Orchestrator manages the environment with the following mechanisms:

* **Node-Agent Driven Network Throttling (`tc`):** Because the centralized Orchestrator pod lacks direct root/host namespace permissions to run traffic control (`tc`), the Orchestrator delegates network throttling commands to the privileged Node Agents (via Redis or HTTP). The Node Agents execute the local `tc qdisc` modifications to constrain Inter-Satellite Link (ISL) bandwidth on their respective host interfaces.
* **Strict Run Sanitization & Teardown:** After each run (and before initiating the next run), the Orchestrator executes a cleanup protocol to restore a sterile environment:
  * Removes all active `tc qdiscs` on all nodes (resets links to full bandwidth).
  * Clears all snapshot folders, local checkpoint directories, and temporary state files (e.g., `/tmp/payload_state.json` on the sidecar).
  * Verifies that all workload pods are fully running, stable, and have returned to their nominal state.
* **Death Timer Calibration:** The Virtual Hardware Fuse (which simulates hardware meltdown if migration is too slow) is scenario-specific rather than a flat threshold. It is calibrated dynamically to allow borderline scenarios to execute near their physical limits without triggering false-positive system deaths due to transient container runtime (CRI-O) snapshot restoration overhead.

### 2.5 The Run Lifecycle

For each of the 540 runs, the Orchestrator loops through:

1. **Setup & Ablation Hot-Reload:** Publish dynamic configuration flags to Node Agents over Redis/HTTP (avoiding K8s rolling restarts).
2. **Network Throttle:** Trigger the respective Node Agents to apply scenario-specific `tc` constraints.
3. **Sterilization:** Lock all hardware metrics to the flatlined baseline.
4. **Injection:** Execute the scenario (e.g., Synthetic trajectory breach followed immediately by a thermal spike on the destination node).
5. **Extraction:** Monitor Redis and K8s API for pod readiness, record metrics, and capture "Correct Failure" abort flags.
6. **Teardown & Sanitization:** Clean up checkpoints, delete `tc` rules, reset sidecar temporary states, and assert cluster stability.

---

## 3. Upgrade Telemetry Logging to a CSV Sink

To capture, structure, and store response variables systematically, the telemetry reporting writes directly to a structured dataset.

### Telemetry Sink Design

* **Decoupled Persistent Storage:** Telemetry scripts flush results directly to an attached Persistent Volume in file-append mode immediately after each run.
* **Standardized Schema:** Output data is formatted with the following columns:
`Timestamp, Configuration_Block, Scenario, Severity_Level, Repetition_ID, Hops, Survival_Rate, Migration_Delay_sec, Bandwidth_MB`
* **Dynamic Checkpoint Sizing:** The telemetry collector dynamically resolves the target configuration status via the Kubernetes API to log accurate data transport metrics (e.g., 160MB for Control, 24MB for Sidecar).
* **Correct Failure Tracking:** Captures specific Redis flags (e.g., `[ABORT_ACKNOWLEDGED: COOLDOWN_LOCK]` and `[ABORT_ACKNOWLEDGED: SURVIVAL_PROBABILITY_LOWER_AT_DESTINATION]`) to distinguish between a hardware system crash ($0\%$ Survival) and a safe system refusal ($100\%$ Survival).

---

## 4. Shift to an Ablation-Based Factorial Design

With automated run coordination and deterministic fault injection, the campaign scales to evaluate interactions between factors rigorously.

### Factorial Analysis

* **Multi-Factor Orthogonality:** The configuration matrix evaluates $6 \text{ Configurations} \times 3 \text{ Scenarios} \times 3 \text{ Severity Levels} \times 10 \text{ Replications} = 540 \text{ Runs}$.
* **Variance Control:** Run-order is mathematically randomized to prevent temporal sequence biases.
* **Analysis of Variance (ANOVA):** Output datasets are perfectly formatted to support unified 3-way ANOVA and response surface analysis, quantifying the statistical significance of the orchestration layer's specific architectural features.
