# Campaign Implementation & Automation Manual

## Introduction: Campaign Automation Architecture

This document defines the automation architecture and implementation requirements designed to execute the [Thesis Experimental Campaign](file:///home/aless6/.gemini/antigravity-ide/brain/3694be95-a8a7-469c-90d0-fd93b338fd69/experimental_campaign.md) for the **Space Cloud V7** orchestration layer. 

To ensure the statistical validity of the 180-run factorial design, the campaign execution is fully programmatic. This guarantees precise timing for environmental stress injection, enforces strict variance control on network delays, and removes manual telemetry collection steps.

This manual is structured into four core implementation areas:
1. **ConfigMap Parameterization:** Dynamic configuration of edge agent ablation modes via Kubernetes ConfigMaps to prevent image rebuilding.
2. **Orchestrator Automation:** The programmatic execution logic of the "Campaign Runner" to coordinate test setup, timing, stress injection, and teardown.
3. **CSV Telemetry Sinks:** Upgrading logging infrastructure to write response metrics directly to standardized datasets.
4. **Factorial DoE Scaling:** Design adjustments to scale the campaign structure to a full Ablation-based factorial experiment.

---

## 1. Parameterize the Ablation via ConfigMaps

To support rapid configuration changes without rebuilding container images, the system configurations are parameterized using Kubernetes resources.

### Architectural Setup
*   **Externalize Variables:** Modify agent and routing source code to read configuration flags from environment variables.
    *   *Example in Python:* `ENABLE_PREDICTIVE_TWIN = os.getenv('ENABLE_PREDICTIVE_TWIN', 'true').lower() == 'true'`
    *   *Example for Cooldown:* `COOLDOWN_SEC = float(os.getenv('COOLDOWN_SEC', '15.0'))`
    *   *Example for Routing:* Read a `ROUTING_MODE` flag (e.g., `LOG_UTILITY` vs. `LINEAR`) to branch routing logic.
*   **Create a ConfigMap:** Centralize all independent variables in a Kubernetes ConfigMap named `campaign-ablation-config`.
*   **Mount to DaemonSet:** Map the ConfigMap fields into the `daemonset-agent.yaml` to dynamically inject these variables into edge agent pods.
*   **Execution Strategy:** Configurations are modified on the fly by patching the ConfigMap (`kubectl patch configmap campaign-ablation-config ...`) and executing a rolling update of the DaemonSet (`kubectl rollout restart daemonset edge-node-agent`).

---

## 2. Automate the Orchestrator (The "Campaign Runner")

To ensure timing consistency and remove human-introduced variance during run setup and fault injection, execution is coordinated by a programmatic orchestrator.

### Runner Architecture (`run_campaign.py`)
To bypass K8s port-forward instability during the 12-hour execution, the orchestrator and telemetry sink are **containerized and deployed as a K8s Pod** (`campaign-runner`) within the cluster. This allows native DNS resolution (e.g., `ground-redis.default.svc.cluster.local:6379`) for rock-solid communication.

A master Python orchestrator loops through the 180 runs (6 Configurations $\times$ 3 Scenarios $\times$ 10 Repetitions) performing the following lifecycle for each run:
1.  **Configure:** Patch the ConfigMap for the active configuration profile, restart the DaemonSet, and poll `kubectl rollout status daemonset/space-node-agent` to guarantee a fully initialized state.
2.  **Network Setup:** Invoke the throttling scripts (e.g., `apply_tc_throttling.sh`) using Python's `subprocess` to configure scenario-specific bandwidth and delays.
3.  **Adaptive Warm-Up:** Instead of a hard 90-second sleep, the runner actively polls the Redis telemetry bus. Once the target node reports 3-5 consecutive stable telemetry pings and the SML payload is `Ready`, the test proceeds immediately.
4.  **Stress Injection:** Publish the scenario's physical stress values (temperature and battery overrides) to the target Redis command channel.
5.  **Data Extraction & Sandbox Timeout:** Wait for migration completion within a strict ~60s timeout wrapper. If CRIU hangs due to socket state drift or PID collisions, kill the run, mark `Survival = 0%`, and forcefully delete the workload pod to prevent the entire batch from freezing.
6.  **Teardown & Reset:** Clear active network filters, reset cluster states, and prepare the environment for the next iteration.

---

## 3. Upgrade Telemetry Logging to a CSV Sink

To capture, structure, and store response variables systematically, the telemetry reporting writes directly to a structured dataset.

### Telemetry Sink Design
*   **Decoupled Persistent Storage:** To prevent memory limits or data loss on a runner crash (e.g., at run 179), telemetry scripts flush results directly to disk immediately after each run using file-append mode, or write individual files per-run (e.g., `results/run_150.csv`) which are concatenated post-campaign.
*   **Standardized Schema:** Output data is formatted with the following columns:
    `Timestamp, Configuration_Block, Scenario, Repetition_ID, Hops, Survival_Rate, Migration_Delay_sec, Bandwidth_MB`
*   **Dynamic Checkpoint Sizing:** The telemetry collector dynamically resolves the target configuration status via the Kubernetes API to compute accurate data transport metrics (e.g., logging 160MB for monolithic configurations and 24MB for optimized sidecar state configurations).

---

## 4. Shift to a Ablation-based Factorial Design

With automated run coordination and parameterization, the campaign can scale to evaluate interactions between factors rather than treating them in isolation.

### Factorial Analysis
*   **Multi-Factor Interactions:** Instead of traditional one-factor-at-a-time (OFAT) testing, the configuration matrix evaluates all combinations of the optimizations (e.g., testing the combined effect of removing the predictive twin and changing routing behavior).
*   **Analysis of Variance (ANOVA):** Output datasets support multi-way ANOVA and response surface analysis to quantify statistical significance and synergistic effects between different software architectural phases.
