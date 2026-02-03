# 🚀 Space Cloud: Stateful Kubernetes Migration Framework

**Space Cloud** is a research-grade simulation environment demonstrating **Stateful Process Migration** within a Kubernetes cluster.

Unlike standard stateless microservices, this project implements a **"Teleportation" mechanism** for live applications. It moves a running Node.js process—preserving its RAM, internal variables, and execution state—between distinct Kubernetes nodes ("Satellites") to adapt to simulated physical constraints like orbital eclipses, thermal runaway, and battery drainage.

---

## 🌟 Key Features

### 1. True Stateful Migration (CRIU Integration)

* Leverages **CRIU (Checkpoint/Restore In Userspace)** to freeze a running process into a disk image.
* Preserves the **V8 Heap** and internal variables (e.g., `mission_timer`) across node restarts.
* **Zero-Loss State:** The application resumes execution on the new node exactly where it left off, without relying on external databases for session state.

### 2. Infrastructure Hacking

* Custom **Minikube** configuration with **CRI-O 1.30**.
* **Kernel-level Patching:** Custom `sysctl` tuning for high-throughput `inotify` usage.
* **Runc Shim:** A custom wrapper injects `--tcp-established` flags to bypass standard Kubernetes security constraints, allowing checkpointing of applications with open TCP sockets.

### 3. "Phoenix Protocol" (Application Awareness)

* Implements a custom handshake between the orchestration layer (Bash) and the application layer (Node.js).
* **Flight Mode:** The application intelligently detects an incoming migration, flushes buffers, and severs network connections to prepare for a "clean freeze."
* **Auto-Wakeup:** Upon restoration, the application detects the new environment and automatically re-establishes network listeners.

### 4. MPC (Model Predictive Control) Scheduler

* Uses an AI-driven **Scheduler** (`space_scheduler.py`) that predicts future satellite states.
* Simulates thermal dynamics and battery charging curves 60 seconds into the future.
* Proactively triggers migration *before* a node fails (e.g., predicting overheating or eclipse entry).

### 5. Digital Twin Physics Engine

* A Python-based simulation engine (`physics_sim.py`) runs in parallel with the cluster.
* Injects physical laws (Thermodynamics, Orbital Mechanics) into static Kubernetes nodes.
* Real-time telemetry broadcasting via **Redis**.

---

## 🏗️ Architecture

The system is composed of four distinct layers:

1. **Infrastructure Layer:** Minikube nodes acting as physical Satellites, running a patched CRI-O runtime.
2. **Control Plane:**
* **Physics Engine:** Simulates environmental data.
* **MPC Scheduler:** Decisions logic based on telemetry forecasting.
* **Redis:** High-speed data bus connecting the simulation to the application.


3. **Actuation Layer:** Imperative Bash scripts (`demo_migration_buildah.sh`) that coordinate Kubernetes API, Buildah, and SCP to physically move memory pages.
4. **Presentation Layer:** A stateless HTML5 Dashboard (`index.html`) that uses aggressive polling and heuristic state analysis to visualize the migration in real-time.

---

## 🛠️ Prerequisites

* **Linux Environment** (Native or WSL2)
* **Minikube** (Latest version)
* **Kubectl**
* **Python 3.x** (with `redis` and `kubernetes` pip packages)
* **Node.js & NPM**

---

## 🚀 Installation & Setup

### 1. Cluster Initialization

We provide an automated script that patches the nodes and installs the specific CRI-O version required for checkpointing.

```bash
# This will delete existing minikube clusters and start a fresh one
./setup_cluster.sh

```

*Note: This script performs "In-Place" upgrades on the Minikube nodes, installing CRIU, `iproute2`, and applying `sysctl` patches.*

### 2. Image Injection (Pre-Flight)

Since the environment runs in an isolated network without a remote registry, you must manually inject the application image into the satellite nodes.

```bash
./inject_image.sh

```

*This script builds the Docker image locally, exports it to a `.tar` archive, and pushes it directly into the CRI-O storage of every satellite node using `buildah`, bypassing the need for a Docker Registry.*

### 3. Mission Control Launch

Once the cluster is active and the images are loaded, use the master orchestration script to deploy the manifests and start the simulation components.

```bash
./mission_control.sh

```

### 4. Access the Dashboard

Open your browser at `http://localhost:8080`.

* **Orbit Mode:** Shows real-time telemetry of the constellation.
* **Flight Mode:** Visualizes the migration process when the active pod moves.

---

## 🧠 Technical Deep Dive

### The "Smart Lock" Migration Logic

The migration script (`demo_migration_buildah.sh`) implements a sophisticated "Smart Lock" mechanism during the restore phase. Instead of blindly targeting a pod, it scans the destination node for a Pod that is strictly in the `Running` phase, ignoring terminating zombies or pending containers. This ensures the "Wake Up" signal is sent only when the destination container is fully receptive.

### The Runc Wrapper

Standard Kubernetes runtimes do not allow checkpointing containers with established TCP connections. We solved this by creating a bash wrapper around `runc` inside the nodes:

```bash
#!/bin/bash
exec /usr/bin/runc.real "$@" --tcp-established

```

This forces the low-level runtime to ignore network safety checks, delegating connection recovery to our application's "Phoenix Protocol."

---

## 📂 Project Structure

* `setup_cluster.sh` - Infrastructure provisioning, Kernel patching, CRI-O installation.
* `mission_control.sh` - Main entry point. Builds Docker images and deploys manifests.
* `demo_migration_buildah.sh` - The "Arm". Handles Checkpoint, Transfer, Image Rebuild, and Restore.
* `server.js` - The "Payload". A stateful Node.js app implementing the Phoenix Protocol.
* `physics_sim.py` - The "World". Simulates orbit, temperature, and battery logic.
* `space_scheduler.py` - The "Brain". MPC algorithm for decision making.
* `public/index.html` - The "Eye". Stateless dashboard for visualization.

---

## ⚠️ Disclaimer

This project uses **experimental features** of Kubernetes (`ContainerCheckpoint`) and modifies low-level system binaries (`runc`). It is intended for research and demonstration purposes. **Do not use these specific configurations in a production environment** without understanding the security implications of enabling ptrace and disabling TCP checks.

---

**Author:** [Il Tuo Nome]
**License:** MIT