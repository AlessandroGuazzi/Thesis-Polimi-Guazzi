# 🚀 Space Cloud V5: The "Dual-Vector" Architecture

**Stateful Kubernetes Migration for Edge-AI Satellites**

> **"Separate the Memory from the Muscle."**

**Space Cloud V5** is a research-grade framework demonstrating **Selective Stateful Migration** in Kubernetes. Unlike standard microservices (which are stateless) or standard migration (which moves the *entire* heavy process), V5 implements the **Sidecar Pattern** to decouple application state from computational logic.

This architecture allows a 1.5GB AI Training workload to "teleport" between physical nodes by migrating only a lightweight (50MB) "Guardian" container, while the heavy compute engine is destroyed and respawned, seamlessly re-attaching to the preserved state.

---

## 🌟 The V5 Paradigm Shift

In Version 4 (Monolithic), we migrated the entire Node.js runtime. In V5, we split the atom:

| Feature | V4 (Monolith) | **V5 (Microservices Sidecar)** |
| --- | --- | --- |
| **Architecture** | Single Container | **Dual Container Pod** (Guardian + Payload) |
| **Migration Target** | Everything (Heavy) | **Guardian Only** (Lightweight) |
| **Migration Size** | ~500MB - 1.5GB | **~30MB - 50MB** (RAM only) |
| **AI Runtime** | Frozen & Transferred | **Destroyed & Respawned** |
| **State Loss** | Zero | **Zero** (Persisted in Sidecar) |
| **Bandwidth Savings** | 0% | **~90-95%** |

### The Components

1. **🛡️ The Guardian (Sidecar):** A lightweight Node.js process. It holds critical state (epoch data, weights, mission timer) and maintains the Dashboard connection. **This container is frozen via CRIU.**
2. **🔥 The Phoenix (Payload):** A heavy Python/PyTorch workload simulating AI training. It is stateless. If the node fails, it dies. When respawned on a new node, it asks the Guardian: *"Where were we?"*

---

## 🏗️ Architecture & Topology

The system simulates a Low Earth Orbit (LEO) constellation where satellites face thermal runaways and battery depletion.

### 1. The Pod Structure (`pod-dual-container.yaml`)

A single Kubernetes Pod containing two containers sharing the same network namespace (`localhost`):

* **Container A (`sidecar-guardian`):** Runs as `privileged`. Mounts `/var/lib/kubelet/checkpoints`. Exposes port 80.
* **Container B (`payload-phoenix`):** Runs unprivileged. Connects to `http://localhost:80/state` to sync training progress.

### 2. The Control Plane

* **Digital Twin (`environment_sim.py`):** Simulates orbital mechanics, eclipses, thermodynamics, and battery physics.
* **MPC Scheduler (`mpc_controller.py`):** A "Model Predictive Control" AI that forecasts node failure 60 seconds into the future and triggers preemptive migration.

### 3. The "Teleportation" Mechanism

We use a custom bash orchestration layer that performs "Open Heart Surgery" on the cluster:

1. **Freeze:** Signals the Guardian to lock memory.
2. **Checkpoint:** Calls the raw Kubelet API to dump the Guardian's RAM to disk using **CRIU**.
3. **Transfer:** Moves the RAM checkpoint to a healthy node.
4. **Restore:** Injects the RAM into a new container, while simultaneously starting a fresh instance of the heavy Payload.

---

## 📂 Project Structure

```text
/SpaceCloud_V5
│
├── /src                          # Source Code
│   ├── /state-sidecar            # [The Guardian] Node.js State Manager
│   │   ├── state_manager.js      # API for state persistence & Dashboard
│   │   └── Dockerfile.sidecar    # Node 20 + CRIU tools
│   └── /training-workload        # [The Phoenix] Python AI Worker
│       ├── train_loop.py         # Simulates heavy matrix multiplication
│       └── Dockerfile.workload   # Python 3.9 Slim (Stateless)
│
├── /infrastructure               # Control Plane (Python)
│   ├── environment_sim.py        # Physics Engine (Digital Twin)
│   └── mpc_controller.py         # AI Scheduler (Decision Maker)
│
├── /ops                          # Orchestration Scripts
│   ├── setup_nodes.sh            # Infrastructure Provisioning (CRI-O 1.30)
│   ├── build_and_inject.sh       # Compiles & loads images to satellites
│   ├── start_system.sh           # Launches the mission (Redis, Sim, Dashboard)
│   └── migrate_sidecar.sh        # <--- THE CORE LOGIC (Selective Migration)
│
└── /k8s                          # Kubernetes Manifests
    ├── pod-dual-container.yaml   # The Sidecar Architecture definition
    ├── service-dashboard.yaml    # Ingress/Service for UI
    └── service-redis.yaml        # Telemetry Bus
```

---

## 🛠️ Prerequisites

* **OS:** Linux (Native) or WSL2 (Ubuntu 22.04+ recommended).
* **Runtime:** Minikube (Latest).
* **Tools:** `kubectl`, `python3`, `pip`, `docker` (client).
* **Python Libs:** `pip install kubernetes redis`

---

## 🚀 Flight Manual (Installation)

### Phase 1: Infrastructure Provisioning

We need to patch the Minikube nodes to support CRIU (Checkpoint/Restore) and install a custom `runc` wrapper to bypass TCP security checks.

```bash
# This creates a 4-node cluster and installs CRI-O 1.30
./ops/setup_nodes.sh
```

*Wait until you see: `>>> INFRASTRUTTURA PRONTA.*`

### Phase 2: Fuel Injection

Build the Docker images locally and inject them directly into the satellites' CRI-O cache (bypassing external registries).

```bash
# Builds 'space-sidecar' and 'space-workload'
./ops/build_and_inject.sh
```

### Phase 3: Mission Launch

Start the Control Plane (Redis, Physics Engine, Scheduler) and deploy the Pods.

```bash
./ops/start_system.sh
```

### Phase 4: Access Mission Control

Open your browser to:
**`http://localhost:8080`**

You should see:

1. **Guardian:** Connected.
2. **Payload:** Computing (Training Epochs increasing).
3. **Telemetry:** Live battery/thermal data from the Physics Engine.

---

## 🎮 How to Test Migration

You don't need to wait for the AI Scheduler to trigger an emergency. You can force a **Selective Sidecar Migration** manually.

**Scenario:** The Pod is running on `minikube-m02`. We want to move it to `minikube-m03`.

Run this command in a separate terminal:

```bash
# Syntax: ./ops/migrate_sidecar.sh <SOURCE> <DESTINATION>
./ops/migrate_sidecar.sh minikube-m02 minikube-m03
```

**Observe the Magic:**

1. **The "Switching Satellite" Overlay** appears on the Dashboard.
2. **The Payload Stops:** The heavy Python process is terminated.
3. **The Guardian Freezes:** Its memory is dumped to disk.
4. **Transfer:** Checkpoint is copied to `m03`.
5. **Restoration:**
* The Guardian is restored from RAM (Mission Timer continues exactly where it left off).
* A *fresh* Python Payload starts, queries `localhost`, and says: *"Ah, we were at Epoch 150. Resuming."*



---

## 🧠 Technical Deep Dive

### 1. The Runc Wrapper (`--tcp-established`)

Standard Kubernetes forbids checkpointing containers with open TCP connections. We inject a shim in `/usr/bin/runc` on every node:

```bash
#!/bin/bash
exec /usr/bin/runc.real "$@" --tcp-established
```

This forces the runtime to ignore the open Dashboard socket. When the process wakes up on the new node, the client (browser) automatically reconnects via the Kubernetes Service.

### 2. The Smart Lock

The migration script uses a heuristic lock to ensure data integrity. It waits for the destination Pod to be fully `Running` and for the container start timestamp to be recent before sending the "Wake Up" signal (`touch /tmp/landed`).

### 3. The Phoenix Protocol

A file-based handshake between the Orchestrator and the Application:

* `touch /tmp/prepare_jump` -> **Guardian** closes sockets and flushes RAM.
* `touch /tmp/landed` -> **Guardian** wakes up, re-opens sockets, and accepts connections from the new Payload.

---

## ⚠️ Disclaimer

This project utilizes **experimental Kubernetes features** (`ContainerCheckpoint`) and performs invasive modifications to the container runtime. It is intended for **research and simulation purposes only**. Do not run this on production clusters.

**Author:** Alessandro Guazzi