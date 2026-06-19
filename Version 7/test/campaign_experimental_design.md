# Thesis Experimental Campaign: Design and Structure

This document outlines the experimental campaign designed to evaluate the **Space Cloud V7** LEO constellation orchestration layer. The campaign is structured under strict **Design of Experiments (DoE)** principles to assess the resilience, efficiency, and speed of the orchestration system under various environmental stressors and system configurations.

---

## 1. Environmental Scenarios & Quantized Boundary States

To evaluate the orchestration layer under realistic orbital conditions, the experimental campaign consolidates environmental stressors into three representative scenarios. Crucially, each scenario is mathematically quantized into three operational strata: **Nominal** (guaranteed survival), **Borderline** (probabilistic survival near physical limits), and **Correct Failure** (deterministic failure due to absolute physics/link constraints, validating that the system recognizes its physical limits).

### Scenario 1: The "Slow Internet" Link Degradation

This scenario manipulates Inter-Satellite Link (ISL) throughput via kernel-level traffic control to test the temporal limits of Phase C (State-Sidecar split) against Phase A (the 30-second predictive forecasting horizon).

* **Nominal State (25 Mbps):** The 24MB sidecar transfers well within the 30-second thermal safety window. Survival expected: 100%.
* **Borderline State (6.4 Mbps):** The 24MB sidecar transfers in exactly ~30 seconds. This perfectly aligns the serialization/transfer latency with the predicted thermal crash horizon. Survival expected: ~50% (dependent on CRIU I/O variance).
* **Correct Failure State (4.0 Mbps):** The transfer requires >45 seconds. The hardware deterministically reaches critical thermal limits before the transfer completes. The system correctly initiates migration but succumbs to physical constraints. Survival expected: 0%.

### Scenario 2: The Sequential Thermal Crisis (Cascading Ping-Pong)

This scenario replaces lateral boundary tracking to explicitly evaluate Phase B (Global Cooldown Timer). It tests system stability by forcing a primary migration, followed immediately by a secondary thermal injection on the new host node.

* **Nominal State ($\Delta T_{stress} = 30\text{s}$):** A thermal spike is injected into Node B 30 seconds after the payload lands. The 15-second global cooldown has safely expired, and Node B immediately routes the payload to Node C. Survival expected: 100%.
* **Borderline State ($\Delta T_{stress} = 16\text{s}$):** The thermal spike is injected exactly 16 seconds after landing, mere milliseconds after the cooldown lock releases. This heavily stresses the Kubernetes CRI-O scheduling queue. Survival expected: High, but subject to container runtime thrashing.
* **Correct Failure State ($\Delta T_{stress} = 5\text{s}$):** The thermal spike is injected 5 seconds after landing. The system registers the critical temperature but **correctly refuses to migrate** due to the active 15-second cooldown lock, safely absorbing the thermal penalty to prevent an infinite routing loop and a cascading cluster collapse. Survival expected: 100% (hardware dependent).

### Scenario 3: The Double Trouble Injection (Thermal & Energy Crisis)

This scenario evaluates Phase D (Log-Utility Dijkstra) and Phase E (Sequenced Migration) by triggering a concurrent dual-hardware crisis. It forces the system's routing engine to evaluate the localized state-space against the neighbor state-space, ensuring it does not route the payload to a destination that offers thermal relief but poses an immediate energy brown-out risk.

* **Nominal State (85°C Local / 40% Neighbor Battery):** The primary node experiences localized heating, prompting a migration trigger. Neighboring nodes sit at a stable 40% battery and baseline temperatures. The routing algorithm easily computes a highly favorable utility cost score for the destination relative to the source. Survival expected: 100%.
* **Borderline State (92°C Local / 10% Neighbor Battery):** The primary node hits a critical thermal threshold, while the available neighbors are energy-constrained at 10% battery. Because the neighbor battery is low but remains above the absolute hardware cut-off, the algorithm determines that the cost of moving is marginally lower than the cost of localized thermal degradation. Survival expected: Probabilistic / Mixed (highly sensitive to CRIU snapshot restoration I/O power spikes).
* **Correct Failure State (98°C Local / 2% Neighbor Battery):** The primary node reaches an extreme 98°C crisis, but all surrounding neighbors are functionally dead at 2% battery. The routing algorithm computes both states and determines that the destination score is significantly worse than the source score. Rather than executing a blind migration into an energy void, the orchestration layer gracefully aborts the migration and stays put to safeguard cluster integrity. Survival expected: 100% (Logged as a software success via `[ABORT_ACKNOWLEDGED: SURVIVAL_PROBABILITY_LOWER_AT_DESTINATION]`).

---

## 2. The 6 Configuration Profiles (Ablation Study)

The campaign evaluates the system across 6 distinct configuration profiles to isolate the performance impact of each individual architectural enhancement.

### Configuration 1: Full System Baseline (Control Group)

* **System State:** All optimization phases (Predictive Twin, Cooldown Timer, State-Sidecar split, Log-Utility routing, and Sequenced Migration) are fully active.
* **Goal:** Establish baseline performance where the system survives all stressors and maintains service continuity.

### Configuration 2: Reactive Migration Only ($\Delta$ Phase A)

* **System State:** The predictive forecasting model is disabled; the system triggers migrations reactively only when absolute physical resource thresholds are breached.
* **Goal:** Demonstrate that reactive-only migrations fail to initiate early enough to complete state transfers prior to hardware shutdown.

### Configuration 3: No Cooldown Damping ($\Delta$ Phase B)

* **System State:** The global cooldown timer is disabled, allowing consecutive migrations to be triggered immediately.
* **Goal:** Demonstrate that sequential thermal crises (Scenario 2) without cooldown limits trigger rapid, cyclic migration loops ("Ping-Pong Effect") between neighboring nodes, resulting in cluster exhaustion.

### Configuration 4: Monolithic Checkpoint Transfer ($\Delta$ Phase C)

* **System State:** The state-sidecar split optimization is disabled; the system performs full monolithic checkpointing (transferring 160MB monolithic worker checkpoints instead of 24MB state-only checkpoints).
* **Goal:** Prove that massive checkpoints over bandwidth-constrained LEO links cause timeout failures (Scenario 1).

### Configuration 5: Linear Routing Utility ($\Delta$ Phase D)

* **System State:** The log-utility path planning algorithm is replaced with a standard linear routing metric.
* **Goal:** Prove that linear utility functions allow moderate conditions in one metric to offset terminal risks in another (Scenario 3), leading to suboptimal routing.

### Configuration 6: Concurrent Evacuation ($\Delta$ Phase E)

* **System State:** Sequenced migration logic is disabled; uncoordinated evacuations of containerized workloads are executed simultaneously.
* **Goal:** Show that uncoordinated double-evacuations route multiple workloads to the same destination simultaneously, resulting in resource contention.

---

## 3. Core Evaluation Metrics

To ensure the evaluation is both comprehensive and clean, the campaign measures three core dependent variables:

1. **Workload Survival Rate (%)**: A binary metric tracking whether the system successfully migrates and maintains running instances of the SML and Master workloads (100% or 0%).
2. **Migration Delay (Seconds)**: The total wall-clock duration from the initiation of the checkpointing process to the complete activation of the workload container on the destination node.
3. **Constellation Bandwidth Footprint (MB)**: The total volume of data transmitted across LEO links, calculated as the product of the number of transmission hops and the checkpoint size.

---

## 4. Statistical Framework & Design of Experiments (DoE)

To validate the reproducibility of the orchestration layer and support rigorous hypothesis testing, the evaluation campaign is structured around a formalized **Ablation-based Factorial Design**. This methodological framework ensures that the distinct contribution of each architectural phase can be isolated, quantified, and statistically verified against environmental stochasticity.

### 4.1. DoE Principles and Categorical Factors

The experiment is formulated as a multi-factor fixed-effects design, operating across three primary categorical independent variables:

1. **Architectural Configuration Factor ($A$, $\alpha = 6$ levels):** Consists of the fully optimized control baseline alongside five targeted, single-variable software ablations.
2. **Environmental Stressor Factor ($E$, $\beta = 3$ levels):** Represents the operational boundary scenarios designed to induce deterministic hardware and network bottlenecks.
3. **Severity Level Factor ($S$, $\gamma = 3$ levels):** Represents the quantized operational strata (Nominal, Borderline, Correct Failure) guaranteeing variance across the evaluation.

#### 4.1.1. Methodological Justification: Ablation-based Factorial Strategy

A traditional $2^k$ full factorial design investigating all software interactions across $k=5$ architectural phases would yield $2^5 = 32$ system profiles. When crossed with the 3 environmental scenarios, 3 severity levels, and 10 replications, this would require an unsustainable **2,880 computational runs**. To maintain mathematical soundness within a realistic execution window, this campaign utilizes an **Ablation-based Factorial Design** ($6 \times 3 \times 3$). By crossing 6 structurally isolated profiles with all 9 scenario states, the design accurately exposes Main Effects ($\Delta \mu_{A_i}$) and multi-way Interaction Effects ($A \times E \times S$).

#### 4.1.2. Variance Control and Nuisance Blocking

To satisfy the underlying assumptions required for subsequent parametric analysis, three control mechanisms are systematically enforced:

* **Adequate Replication ($N = 10$):** Each unique treatment combination $(A_i, E_j, S_k)$ is executed exactly 10 times, providing sufficient degrees of freedom to calculate pure experimental error variance ($\sigma^2$) for a unified 3-way Analysis of Variance (ANOVA).
* **Run-Order Randomization:** The execution sequence of all 540 runs is randomized to eliminate temporal biases such as memory leaks or disk storage degradation.
* **Nuisance Blocking:** Non-experimental conditions (background cluster load, initial SML/Master states) are strictly held constant as blocking factors via the Orchestrator's "Sterile Baseline" initialization.

### 4.2. Experimental Run Matrix Breakdown

The execution space maps to a beautifully balanced, orthogonal 3-factor design matrix defined by:

$$\text{Total Runs} = \alpha \text{ Configs} \times \beta \text{ Scenarios} \times \gamma \text{ Severity Levels} \times N \text{ Reps} = 6 \times 3 \times 3 \times 10 = \mathbf{540 \text{ runs}}$$

| Factor | Type | Levels / Details |
| --- | --- | --- |
| **System Configurations ($A$)** | Categorical | 6 levels (Control Baseline, $\Delta\text{Twin}$, $\Delta\text{Cooldown}$, $\Delta\text{Sidecar}$, $\Delta\text{LogUtility}$, $\Delta\text{Sequence}$) |
| **Environmental Scenarios ($E$)** | Categorical | 3 levels (Slow Internet, Sequential Thermal Crisis, Double Trouble) |
| **Severity Levels ($S$)** | Categorical | 3 levels (Nominal, Borderline, Correct Failure) |
| **Repetitions ($N$)** | Integer | 10 replication runs |
