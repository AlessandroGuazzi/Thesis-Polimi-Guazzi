# 🛰️ Space Cloud V6: Decentralized Satellite Mesh & Wildfire Tracker

**Space Cloud V6** is a state-of-the-art refactoring of the satellite edge computing framework. It transitions from the centralized V5 architecture to a fully decentralized, edge-autonomous mesh network designed for real-time **2D Lateral Wildfire Tracking** using Streaming Machine Learning (SML).

---

## 🌌 Architectural Overview

The system operates as a simulated LEO (Low Earth Orbit) constellation on a 4-node Minikube cluster. It employs a **Dual-Plane Architecture**:

1.  **The Control Plane (Floating Master):** A migratable Redis instance + Topology Dashboard sidecar that rides on satellite nodes. It maintains the live ISL (Inter-Satellite Link) mesh graph and executes atomic Dijkstra pathfinding via server-side Lua scripts.
2.  **The Data Plane (tinySML Payload):** A dual-container Pod (Guardian + Phoenix) that tracking wildfire spread using a **SAMKNN (Self-Adjusting Memory k-Nearest Neighbor)** model.

### Key Innovations in V6:
*   **UDP-Based Ingestion:** Replaced fragile TCP streams with a resilient UDP data plane optimized for the intermittent nature of satellite links.
*   **Dual-Gate Migration Triggers:**
    *   **Trigger A (Thermal):** Self-preservation via predictive hardware modeling (Newton’s Law of Cooling).
    *   **Trigger B (Lateral):** Mission-preservation via Center of Mass (CoM) drift detection in the Predicted Fire Mask.
*   **Multi-Hop Store-and-Forward Relay:** Replaced direct gRPC streams with SSH-piped hop-by-hop transfers using SHA256 integrity verification, simulating CCSDS protocols.
*   **CRIU Persistence:** Guaranteed state survival across nodes for the SAMKNN model (≤25MB footprint) and the Guardian sidecar.

---

## 🛠️ System Components

| Component | Role | Technology |
| :--- | :--- | :--- |
| **Guardian Sidecar** | Stateful persistence, local IPC orchestrator, and dashboard host. | Node.js / Express |
| **tinySML Worker** | Pure-Python SAMKNN 2D wildfire classifier (64x64 grid). | Python (Stdlib only) |
| **Node Agent** | Autonomous edge controller; runs local predictive MPC and CRIU logic. | Python / Bash |
| **Data Streamer** | Ground station component; pushes Kaggle wildfire samples via UDP. | Python / struct / zlib |
| **Floating Master** | Distributed topology store and Dijkstra pathfinder. | Redis / Lua |
| **Digital Twin** | Orbital physics simulator; broadcasts real-time telemetry. | Python / Redis |

---

## 🚀 How to Make the System Work (Step-by-Step)

Follow these steps in order from the project root.

### 1. Prerequisites
Ensure you have the following installed on your Linux host:
*   **Minikube** (with Docker driver support)
*   **Docker**
*   **Python 3.9+** (with `redis` and `requests` libraries)
*   **kubectl**

### 2. Phase 1: Provisioning the Cluster
Execute the setup script to create the 4-node cluster and patch the container runtime (CRI-O) for CRIU support.
```bash
bash ops/setup_nodes.sh
```
*   **What it does:** Launches 4 nodes, installs CRIU/Buildah/iproute2, injects the `--tcp-established` runc wrapper, and applies `tc` throttling (50 Mbps / 40 ms) to simulate ISL constraints.

### 3. Phase 2: Building & Injecting Images
Build the four core container images and inject them into the satellite node storage.
```bash
bash ops/build_and_inject.sh
```
*   **What it does:** Compiles the Guardian, tinySML Worker, Node Agent, and Topology Dashboard. Packages them as TARs and loads them into each minikube node's Buildah storage.

### 4. Phase 3: Mission Launch
Start the entire simulation ecosystem.
```bash
bash ops/start_system.sh
```
*   **What it does:** Deploys K8s manifests (Redis, Node Agents, Mission Pods), starts the Digital Twin, launches the Kaggle Data Streamer, and opens the Global Swarm Dashboard.

---

## 📊 Operational Cockpit

Once the system is active, you can monitor the wildfire mission through five distinct interfaces:

1.  **SML Payload Dashboard (`http://localhost:8080`):**
    *   *Source:* Hosted by the Guardian sidecar inside the migrating Pod.
    *   *View:* 2D grids of Current vs. Predicted fire masks, CoM crosshair, and SAMKNN memory gauges.
2.  **Global Swarm Dashboard (`http://localhost:8090`):**
    *   *Source:* Ground Station host.
    *   *View:* "God's Eye" overview of the 4-node constellation health and a real-time migration event log.
3.  **ISL Topology Dashboard (`minikube service topology-dashboard`):**
    *   *Source:* Floating Master sidecar on a satellite node.
    *   *View:* The live mesh graph showing active links and Dijkstra edge weights.
4.  **Ground Redis (Debug):** `redis-cli -p 6379` (Telemetery Pub/Sub).
5.  **Topology Redis (Debug):** `redis-cli -p 6380` (ISL Mesh Graph).

---

## 🧪 Simulation Scenarios

### Test Thermal Migration (Trigger A)
The Digital Twin (`environment_sim.py`) simulates heating during CPU load. If a satellite's battery/thermal model predicts a violation of `T_SAFE` within the 60s horizon, the Node Agent will autonomously query the Floating Master for the **coolest trailing satellite** and initiate a "thermal" migration.

### Test Lateral Tracking (Trigger B)
As the `data_streamer.py` pushes wildfire frames, watch the CoM crosshair on the Payload Dashboard. If the fire spreads toward the edge of the 64x64 grid (CoM < 8 or > 54), the Node Agent will trigger a "lateral" migration to an **adjacent parallel orbital plane** to continue the mission.

---

## 📂 Project Structure

```text
├── infrastructure/
│   ├── node_agent/           # Autonomous MPC & CRIU logic
│   ├── environment_sim.py    # Digital Twin (Physics)
│   ├── data_streamer.py      # UDP Kaggle Streamer
│   ├── global_dashboard.py   # Ground Station Overview
│   └── dijkstra.lua          # Redis-side pathfinding
├── k8s/                      # Kubernetes Manifests (V6)
├── ops/                      # Orchestration & Setup Scripts
├── src/
│   ├── state-sidecar/        # Guardian (Node.js)
│   └── training-workload/    # tinySML Worker (SAMKNN)
└── Doc.md                    # Technical Architecture Specification
```

---

## ⚠️ Important Notes
*   **CRIU Compatibility:** All Python code in the Payload must remain **Pure-Python (Stdlib only)**. Do not install `numpy` or `torch` in the workload container, as C-extension threads will break the checkpoint process.
*   **ISL Simulation:** Throttling is enforced via `tc`. A 25MB checkpoint takes ~4.0 seconds to jump one hop at 50 Mbps. Multi-hop migrations will scale linearly.