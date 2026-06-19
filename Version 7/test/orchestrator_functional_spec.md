# Space Cloud V7: Campaign Orchestrator Specification

## 1. Architectural Role and Execution Paradigm

The Campaign Orchestrator is the centralized, programmatic "God-Mode" controller responsible for executing the entire Space Cloud V7 experimental evaluation. To eliminate human error and temporal biases, the Orchestrator must be fully automated and deployed directly within the Kubernetes cluster. Its primary mandate is to shift the system from a stochastic, live-simulation environment into a strictly controlled, mathematically deterministic test harness.

## 2. Design of Experiments (DoE) Enforcement

The Orchestrator is the strict enforcer of the system's statistical validity. It must possess the following characteristics regarding experiment design:

* **Factorial Matrix Generation:** It must dynamically generate the complete 540-run execution matrix, consisting of all combinations of the 6 architectural configurations, 3 environmental scenarios, 3 severity levels, and 10 absolute replications.
* **Run-Order Randomization:** To prevent nuisance variables (such as memory leaks, hardware degradation, or caching) from skewing the results, the Orchestrator must mathematically shuffle the entire 540-run matrix before execution begins. It must never run tests sequentially by configuration or scenario.
* **Ablation Management:** The Orchestrator must be capable of seamlessly transitioning the cluster between the 6 different architectural configurations. It achieves this by programmatically patching cluster configuration maps and orchestrating rolling restarts of the edge agents between runs.

## 3. Environmental Decoupling and State Control

To satisfy Analysis of Variance (ANOVA) assumptions, the Orchestrator must exert absolute control over the physical and visual realities of the satellite constellation.

* **Suppression of Stochastic Systems:** The Orchestrator must operate under the strict assumption that all natural environmental simulators and live Earth-data streamers are completely deactivated.
* **The "Sterile Baseline":** Before any test begins, the Orchestrator must flatline the environment. It must force every satellite node into a mathematically perfect "Nominal" state (e.g., safe temperatures, full batteries, centered visual payloads). This guarantees that no autonomous background migrations occur and provides a clean, zero-variance starting point for every run.
* **Workload Throttling:** It must assume the active AI payload is running in a static, throttled loop—generating realistic CPU heat and maintaining a precise memory footprint (24MB or 160MB)—but stripped of its ability to generate random boundary-tracking coordinates.

## 4. Deterministic Fault Injection Mechanisms

The Orchestrator executes test scenarios not by simulating gradual environments, but through instantaneous, mathematically precise fault injections.

* **The "Ghost Publisher" (Hardware Overrides):** The Orchestrator must perfectly mimic the telemetry bus. To trigger thermal or energetic crises, it must publish synthetic, instantaneous threshold breaches directly to the cluster's internal messaging system, targeting specific nodes with millisecond precision.
* **The "Ghost Worker" (Visual/Data Overrides):** To evaluate lateral fire-tracking capabilities, the Orchestrator must bypass the AI model entirely. It must interface directly with the Stateful Sidecar's memory via internal network requests, injecting a synthetic trajectory that pushes the "fire" toward the visual boundary at a strictly controlled, programmable velocity.

## 5. The Virtual Hardware Fuse

To accurately validate the Predictive Twin optimization against the Reactive baseline, the Orchestrator must enforce the physical limits of aerospace silicon.

* **Simulated Silicon Melt:** When testing reactive configurations, the Orchestrator must recognize that real hardware does not wait for a network transfer to finish once critical temperatures are reached.
* **The Death Timer:** Upon injecting a critical temperature threshold, the Orchestrator must start an absolute countdown timer (e.g., 2.0 seconds).
* **Execution of the Fuse:** If the Kubernetes cluster has not fully completed the workload migration and activated the destination pod before the timer expires, the Orchestrator must brutally terminate the origin pod. This simulates total power loss and records a deterministic system failure (0% survival).

## 6. Strict Run Lifecycle Management

For every single one of the 540 runs, the Orchestrator must strictly adhere to the following execution loop:

1. **Preparation Phase:** Apply the specific architectural ablation rules and reset all network constraints.
2. **Sterilization Phase:** Enforce the Sterile Baseline across all satellite nodes and wait for the cluster to report absolute stability.
3. **Stress Initialization:** Apply scenario-specific network bottlenecks (e.g., simulating 6.4 Mbps slow inter-satellite links).
4. **Injection Phase:** Execute the "Ghost Publisher" or "Ghost Worker" vectors to trigger the necessary migrations at highly specific timestamps.
5. **Extraction Phase:** Actively poll the cluster's API to calculate the exact, wall-clock migration delay—measured from the moment of injection to the moment the destination node reports workload readiness.
6. **Teardown Phase:** Clean up network throttles, clear temporary state files, and prepare the environment for the next randomized run.

## 7. Intelligent Telemetry and Evaluation

The Orchestrator is not just a test runner; it is the definitive data collector for the thesis evaluation.

* **Direct File Sinking:** To avoid memory limits or data loss during the potentially multi-hour execution window, the Orchestrator must write all results directly to a persistent, locally mounted dataset after every single run.
* **Standardized Schema Logging:** It must format all outputs according to a strict schema, capturing timestamps, configurations, scenarios, severity levels, repetition IDs, data transfer sizes, survival rates, and migration delays.
* **"Correct Failure" Recognition:** The Orchestrator must be intelligent enough to differentiate between a system crash (a bug or timeout) and a safe "Correct Failure." If the system intelligently refuses to migrate into a dangerous situation (e.g., recognizing a cooldown lock or evaluating that the cost of moving to a dead battery node is worse than staying), the Orchestrator must log this as a 100% survival success, validating that the software correctly recognized its physical limitations.
* **Sandbox Timeout Protection:** If a specific architectural configuration causes the cluster runtime to hang indefinitely, the Orchestrator must enforce an absolute timeout wrapper. If the timeout is breached, the Orchestrator must kill the run, log a complete failure, forcefully clean the hung nodes, and seamlessly continue to the next run without freezing the overall campaign.
