# Space Cloud: Resilient Orbital Edge Computing PoC

   

## 🛰️ Abstract

**Space Cloud** is a Proof of Concept (PoC) architecture developed as part of a Master's Thesis in Computer Engineering. It demonstrates the feasibility of adapting cloud-native technologies (Kubernetes) to the extreme constraints of the Low Earth Orbit (LEO) environment.

Unlike terrestrial data centers where resources are assumed to be infinite and ubiquitous, orbital edge nodes face cyclic energy availability (Day/Night cycles), intermittent connectivity, and harsh thermal conditions. This project implements a **closed-loop control system** that extends Kubernetes to be "energy-aware," ensuring service continuity for critical mission workloads despite frequent node failures and power drains.

-----

## 🏗️ Architecture

The system simulates a constellation of satellites using **Kind (Kubernetes in Docker)**. The architecture is composed of three logical layers:

1.  **The Physical Layer (Simulation):** A Python-based "Digital Twin" that simulates orbital mechanics, solar charging, and battery discharge curves for every node.
2.  **The Orchestration Layer (The Brain):** Custom control loops that override standard Kubernetes behaviors to respect physical constraints.
3.  **The Application Layer (The Payload):** A stateful telemetry dashboard backed by Redis, designed to survive node evictions.

### Key Components

  * **🌍 Physics Simulator (`physics_sim.py`):** Deterministic simulation of LEO orbits (60s cycle). It injects real-time telemetry (Battery Level, Power Status) into Kubernetes Nodes via Labels and publishes data to Redis for visualization.
  * **🧠 Space Scheduler (`space_scheduler.py`):** A custom scheduler that filters nodes based on energy availability (`>20%`) and implements "Best Fit" logic. It also handles **Data Locality**, prioritizing nodes where the Redis backend is already running.
  * **🛡️ Reactive Watchdog (`space_watchdog_reactive.py`):** An Event-Driven FDIR (Fault Detection, Isolation, and Recovery) system. It monitors the cluster stream and performs immediate **Load Shedding** (Pod Eviction) if a satellite's battery drops below critical thresholds or hardware failure (`NotReady`) is detected.

-----

## 📂 Repository Structure

```text
.
├── k8s/                        # Infrastructure Manifests
│   ├── space-cloud-config.yaml # Kind Cluster Topology (Control Plane + Workers)
│   ├── space-redis.yaml        # StatefulSet definition for Persistence
│   └── space-dashboard.yaml    # Mission Workload Deployment
├── satellite/                  # Mission Payload (Node.js App)
│   ├── Dockerfile              # Container definition
│   ├── server.js               # Backend logic & Redis connection
│   ├── index.html              # Telemetry Dashboard UI
│   └── ...
├── launch_mission_reactive.ps1 # Automated Orchestration Script (PowerShell)
├── physics_sim.py              # Orbital Physics Engine
├── space_scheduler.py          # Custom Energy-Aware Scheduler
├── space_watchdog_reactive.py  # FDIR Safety System
└── requirements.txt            # Python dependencies
```

-----

## 🚀 Quick Start

### Prerequisites

  * **Docker Desktop** (Running with WSL2 backend on Windows)
  * **Kind** (Kubernetes in Docker)
  * **Kubectl**
  * **Python 3.x**
  * **PowerShell**

### 1\. Initialize the Constellation

Create the virtual cluster with the specific control-plane configuration:

```powershell
kind create cluster --name space-cloud --config k8s/space-cloud-config.yaml
```

### 2\. Build and Load the Payload

Compile the satellite software and load it into the air-gapped cluster nodes:

```powershell
docker build -t space-satellite:v1 ./satellite
kind load docker-image space-satellite:v1 --name space-cloud
```

### 3\. Launch Mission Control

Install dependencies and start the orchestration suite. The script will open multiple terminals for the Physics Engine, Scheduler, and Watchdog.

```powershell
pip install -r requirements.txt
.\launch_mission_reactive.ps1
```

### 4\. Deploy Mission Workloads

Once the control systems are online, deploy the persistent storage and the application:

```powershell
kubectl apply -f k8s/space-redis.yaml
# Wait for Redis to be Running, then:
kubectl apply -f k8s/space-dashboard.yaml
```

### 5\. Access Telemetry

Open your browser at **http://localhost:8080** to view the real-time telemetry dashboard.

-----

## 🧪 Simulation Scenarios

### Scenario A: The Eclipse (Energy Drain)

The **Physics Simulator** will automatically cycle between SUN and ECLIPSE phases.

1.  Observe the battery level dropping on the active node during the Eclipse.
2.  When battery hits **\<20%**, the **Watchdog** triggers an alarm.
3.  The Workload is evicted to save the satellite.
4.  **Self-Healing:** Kubernetes reschedules the workload to a charged satellite via the **Space Scheduler**. Service resumes automatically.

### Scenario B: Hardware Failure (Hard Kill)

Simulate a catastrophic hardware failure on a specific satellite:

```powershell
docker stop space-cloud-worker
```

1.  The dashboard will display a **⚠️ FAULT** badge for that node.
2.  The Watchdog detects the `NotReady` state and evacuates workloads immediately.
3.  The application migrates to the remaining healthy node.

-----

## 🛠️ Technology Stack

  * **Orchestration:** Kubernetes (Custom Controllers via Python Client)
  * **Simulation Environment:** Kind (Docker)
  * **Logic & Control:** Python 3 (async/event-driven)
  * **Payload:** Node.js (Express), Redis (Persistence/AOF)
  * **Frontend:** HTML5/JS (Real-time polling)

-----

## 📝 Author

**Alessandro Guazzi**
*Master's Thesis in Computer Engineering*
*Politecnico di Milano*

-----

> **Note:** This project is a simulation. The orbital mechanics are simplified for demonstration purposes (60-second orbit duration). The "Battery" metric is a synthetic metadata injected into Kubernetes Nodes.