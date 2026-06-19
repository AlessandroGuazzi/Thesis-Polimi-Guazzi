# Test Campaign Implementation Plan

This plan outlines the sequential steps required to build out the deterministic test harness for the Space Cloud V7 ablation campaign. The goal is to safely transition the cluster from a stochastic, live environment to a strictly controlled, programmatic test bed.

## User Review Required

> [!IMPORTANT]
> Please review the sequential steps below. Once you approve this plan, I will execute these steps one by one to upgrade your codebase for the campaign.

## Proposed Changes

We will approach this in four distinct phases to ensure stability and verify functionality progressively.

---

### Phase 1: Ablation Parameterization & Edge Agent Refactoring

**Goal:** Enable the Kubernetes cluster and edge nodes to dynamically toggle optimization features without requiring container rebuilds.

1. **Create the K8s ConfigMap Manifest**
   - Create a new file `campaign-ablation-config.yaml` in the `2_orbit_payload/k8s/` directory.
   - Define all required ablation variables (e.g., `ENABLE_PREDICTIVE_TWIN`, `COOLDOWN_SEC`, `ENABLE_LOG_UTILITY`, `ENABLE_SEQUENCED_MIGRATION`).

2. **Parameterize the Node Agent**
   - Modify `2_orbit_payload/infrastructure/node_agent/node_agent.py`.
   - Read the ablation variables using `os.getenv()`.
   - Wrap the Predictive Twin (Phase A), Global Cooldown Timer (Phase B), and Log-Utility routing (Phase D) in conditional logic based on these variables.

3. **Expose K8s DaemonSet to ConfigMap**
   - Update `2_orbit_payload/infrastructure/node_agent/Dockerfile.agent` or the deployment scripts to ensure the variables from `campaign-ablation-config` are securely injected into the agent's environment during rollout.

---

### Phase 2: Workload Throttling & Environmental Decoupling

**Goal:** Freeze out background noise and ensure the AI workload maintains a predictable, stable footprint for deterministic CRIU migrations.

1. **Add "Campaign Mode" to the Inference Worker**
   - Modify `2_orbit_payload/src/inference-workload/tinysml_worker.py`.
   - Add a `CAMPAIGN_MODE` toggle via environment variable.
   - When active, bypass the UDP listener entirely. Instead, run a dummy mathematical loop that maintains a constant thermal/CPU footprint and preserves the 160MB memory footprint without resetting or dropping history.

2. **Decouple the Data Streamer**
   - Update deployment scripts (e.g., `ops/start_system.sh`) to explicitly exclude the launch of `3_ground_station/data_streamer.py` and `environment_sim.py` when the system starts in Campaign Mode.

---

### Phase 3: The Python Campaign Orchestrator

**Goal:** Build the central "God-Mode" runner that iterates through the 540-run DoE matrix.

1. **Scaffold the Orchestrator Service**
   - Create a new directory and Python file: `test/campaign_runner.py`.
   - Initialize the K8s Client (for patching ConfigMaps and reading Pod readiness) and Redis Client (for publishing telemetry).

2. **Implement Matrix Generation & Randomization**
   - Implement the DoE matrix generator crossing 6 configs × 3 scenarios × 3 severities × 10 repetitions.
   - Implement the mathematical shuffle to guarantee run-order randomization.

3. **Implement the "Sterile Baseline" & "Ghost Publisher"**
   - Build functions to publish flatlined Nominal states to `telemetry/{NODE_NAME}` across the Redis bus.
   - Build injection functions to publish deterministic thermal/battery spikes at exact timestamps.

4. **Implement the "Ghost Worker" Lateral Injection**
   - Build an HTTP client routine that sends `POST /state` requests directly to the Guardian Sidecar (`space-mission-svc:80`) to artificially advance the payload's Center of Mass toward visual boundaries.

5. **Implement the Virtual Hardware Fuse**
   - Introduce a strict countdown timer attached to critical thermal spikes. If the migration doesn't resolve before the timer expires, programmatically kill the origin Pod to simulate hardware meltdown (0% survival).

---

### Phase 4: Telemetry CSV Sinks & Data Collection

**Goal:** Capture all dependent variables (Survival Rate, Migration Delay, Bandwidth) securely to disk.

1. **Implement the Telemetry Collector in the Orchestrator**
   - Add an extraction phase to the orchestrator loop that calculates the exact wall-clock migration delay by polling the K8s API.

2. **Deploy the CSV Sink**
   - Ensure the Orchestrator writes directly to an appended CSV file (`evaluation_data/campaign_results.csv`) after every run with the standardized column schema.

## Verification Plan

### Automated Tests
- Once Phase 3 is complete, we will execute a dry-run of the `campaign_runner.py` with only 5 test iterations to verify that it successfully patches the ablation ConfigMap, triggers a migration, and writes to the CSV sink without errors.

### Manual Verification
- We will monitor the Redis bus manually to ensure the Orchestrator is successfully enforcing the Sterile Baseline.
- We will verify that the inference worker correctly enters the throttled Campaign Mode and ignores real UDP data.
