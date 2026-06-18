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

### Scenario 3: The Double Trouble Injection

Triggers a dual hardware crisis with randomized starting conditions to ensure variance control (temperature randomized between 93°C–97°C, battery randomized between 3%–6% at the moment of injection).

* **Evaluation:** This scenario specifically stresses Phase D (Log-Utility Dijkstra) and Phase E (Sequenced Migration), ensuring the system does not route the payload to a node that is thermally safe but critically low on battery.



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


* **Goal:** Demonstrate that sequential thermal crises (Scenario 2) without cooldown limits trigger rapid, cyclic migration loops ("Ping-Pong Effect") between neighboring nodes, resulting in cluster exhaustion[cite: 3, 8].

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

The experiment is formulated as a multi-factor fixed-effects design, operating across two primary categorical independent variables:

1. **Architectural Configuration Factor ($A$, $\alpha = 6$ levels):** Consists of the fully optimized control baseline alongside five targeted, single-variable software ablations.
2. **Environmental Stressor Factor ($E$, $\beta = 3$ levels):** Represents the operational boundary scenarios designed to induce deterministic hardware and network bottlenecks.

#### 4.1.1. Methodological Justification: Ablation-based Factorial Strategy

A traditional $2^k$ full factorial design investigating all software interactions across $k=5$ architectural phases would yield $2^5 = 32$ system profiles. When crossed with the 3 environmental scenarios and 10 replications, this would require 960 computational runs. To maintain mathematical soundness within a sustainable execution window, this campaign utilizes an **Ablation-based Factorial Design** ($6 \times 3$). By crossing 6 structurally isolated profiles with all 3 environmental stress levels, the design accurately exposes Main Effects ($\Delta \mu_{A_i}$) and Interaction Effects ($A \times E$).

#### 4.1.2. Variance Control and Nuisance Blocking

To satisfy the underlying assumptions required for subsequent parametric analysis, three control mechanisms are systematically enforced:

* **Adequate Replication ($N = 10$):** Each unique treatment combination $(A_i, E_j)$ is executed exactly 10 times, providing sufficient degrees of freedom to calculate pure experimental error variance ($\sigma^2$) for Analysis of Variance (ANOVA).


* **Run-Order Randomization:** The execution sequence of all 180 runs is randomized to eliminate temporal biases such as memory leaks or disk storage degradation.


* **Nuisance Blocking:** Non-experimental conditions (background cluster load, initial SML/Master states) are strictly held constant as blocking factors.



### 4.2. Experimental Run Matrix Breakdown

The execution space maps to a balanced, orthogonal design matrix defined by:

$$\text{Total Runs} = \alpha \text{ Configurations} \times \beta \text{ Scenarios} \times N \text{ Replications} = 6 \times 3 \times 10 = \mathbf{180 \text{ runs}}$$

| Factor | Type | Levels / Details |
| --- | --- | --- |
| **System Configurations ($A$)** | Categorical | 6 levels (Control Baseline, $\Delta\text{Twin}$, $\Delta\text{Cooldown}$, $\Delta\text{Sidecar}$, $\Delta\text{LogUtility}$, $\Delta\text{Sequence}$) |
| **Environmental Scenarios ($E$)** | Categorical | 3 levels (Slow Internet, Sequential Thermal Crisis, Double Trouble) |
| **Repetitions ($N$)** | Integer | 10 replication runs |
