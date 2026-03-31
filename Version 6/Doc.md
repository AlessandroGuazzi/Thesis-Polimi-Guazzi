# The To-Be System: Space Cloud V6 Architecture

The core objective of V6 is to abandon the "flat" networking and simple heuristics of V5 to simulate a true Inter-Satellite Link (ISL) mesh network, complete with multi-hop physical routing and highly specialized anomaly detection. 

## 1. Data Plane: Workload & Anomaly Detection

### 1.0 The Operational Scenario: Earth Observation & Wildfire Detection
To elevate the Space Cloud V6 architecture from a theoretical distributed systems experiment to a practical aerospace application, the Data Plane is assigned a highly specific, tactical workload: Real-Time Wildfire Hotspot Detection. In this scenario, a Low Earth Orbit (LEO) satellite constellation scans the Earth's surface using Infrared (IR) line-scanning sensors, streaming Sea Surface Temperature (SST) and land surface temperature data at 10 Hz. The payload (the tinySML worker container) must ingest this 1D data stream and detect the outbreak of wildfires in real-time. 

This presents a unique mathematical and engineering challenge governed by severe orbital constraints: 
* **The Physics of the Signal:** Unlike faint thermal anomalies, a wildfire presents as a massive, unmistakable scalar spike, jumping from a normal terrestrial background of 20°C to over 300°C instantly. This makes a 1D streaming anomaly detector highly realistic and physically appropriate. 
* **Extreme Concept Drift:** As the satellite traverses the globe at 7.5 km/s, the "normal" baseline temperature is constantly shifting (non-stationary). A forest canopy naturally warms during the day and cools at night, and orbital eclipses introduce sudden step-changes in the ambient thermal baseline. 
* **Resource & CRIU Constraints:** To ensure the stateful migration.tar snapshot remains small enough to transfer rapidly over heavily constrained Inter-Satellite Links, the application must consume $\le25$ MB of RAM. Furthermore, to guarantee zero-downtime CRIU compatibility, the Machine Learning model must rely purely on the Python standard library, strictly avoiding C-extension threads or GPU contexts (which instantly eliminates heavy frameworks like PyTorch or TensorFlow).

To solve this, we evaluated three distinct streaming machine learning algorithms for deployment on the edge node.

### 1.1 Candidate Evaluation & Mathematical Comparison

**Candidate 1: Hoeffding Adaptive Tree (HAT) with ADWIN** 
The Hoeffding Tree grows incrementally, utilizing the Hoeffding Bound to decide when a leaf has collected sufficient statistical evidence to split: 
$$\epsilon=\sqrt{\frac{R^{2}ln(1/\delta)}{2n}}$$ 
To handle the shifting terrestrial baseline, it utilizes the ADWIN (ADaptive WINdowing) sub-module, which monitors the stream for statistical shifts and replaces outdated sub-trees with fresh learners. 
* **Pros:** It handles concept drift natively via ADWIN and can be implemented in pure Python (e.g., using the River library), making it CRIU-safe. 
* **Cons:** It is a supervised classification algorithm, requiring a pre-labeled data stream, which is unrealistic for an autonomous satellite. More critically, the tree's memory footprint grows unboundedly as it encounters concept drift and splits nodes. This bloats the serialized state from 50 KB to over 200 KB, driving the total RAM footprint to ~20 MB and the resulting CRIU .tar snapshot to ~25 MB, dangerously slowing down the migration transfer time. 

**Candidate 2: Online Gaussian Anomaly Detector (Streaming Z-Score)** 
This algorithm maintains a running estimate of the mean $(\hat{\mu}_{t})$ and variance $(\hat{\sigma}_{t}^{2})$ using Welford's online algorithm. An anomaly is flagged when the standard z-score exceeds a rigid threshold: 
$$z_{t}=\frac{x_{t}-\hat{\mu}_{t}}{\hat{\sigma}_{t}}>\tau$$ 
* **Pros:** It is fully unsupervised and mathematically trivial. The serialized state consists of exactly 6 floating-point numbers, totaling a mere 48 bytes. 
* **Cons:** It possesses zero concept drift adaptation. Because the running mean integrates all historical data equally $(\frac{1}{t}\rightarrow0)$, the baseline becomes rigid. As the satellite crosses from a sun-baked desert to a shadowed mountain range, the rigid baseline will cause the detector to either trigger endless false alarms or become entirely blind to actual fires. 

**Candidate 3: EWMA-Residual Anomaly Detector with Adaptive Threshold** 
This is a constant-space $O(1)$ streaming architecture designed to isolate sudden spikes within a shifting baseline.
1.  **Exponentially Weighted Moving Average (EWMA) Baseline Tracker:** It tracks the shifting terrestrial baseline by giving exponentially decaying weight to older observations:
    $$\hat{\mu}_{t}=\alpha\cdot x_{t}+(1-\alpha)\cdot\hat{\mu}_{t-1}$$ 
2.  **Residual Computation:** It isolates the sudden thermal spike by subtracting the baseline from the current reading:
    $$r_{t}=x_{t}-\hat{\mu}_{t}$$ 
3.  **Adaptive Threshold:** It dynamically tracks the variance of the residuals to avoid false alarms on naturally "warm" spots:
    $$\hat{\sigma}_{t}=\beta\cdot|r_{t}|+(1-\beta)\cdot\hat{\sigma}_{t-1}$$ 
* **Pros:** This perfectly aligns with the physics of a wildfire. The intrinsic forgetting factor (a) seamlessly adapts to the natural warming and cooling of the Earth below (Concept Drift). When the sensor sweeps over a fire, the residual $(r_{t})$ explodes, immediately triggering the alarm.
* **Architectural Supremacy:** Because the old baseline is continuously overwritten, the algorithm only ever holds exactly 5 float64 variables and 2 counters, capping the state at a permanent 56 bytes. This ultra-lean, stdlib-only implementation restricts the RAM footprint to ~12 MB, keeping the CRIU tar snapshot to a highly optimized ~15 MB.

### 1.2 Verdict and Architectural Impact

The following table summarizes the evaluation metrics:

| Criterion | HAT + ADWIN | Gaussian Z-Score | EWMA-Residual (Winner) |
| :--- | :--- | :--- | :--- |
| **Matches the Physics** | Classification | Rigid Threshold | Designed for massive scalar spikes |
| **Concept Drift** | Yes (ADWIN) | X None | Intrinsic (a) |
| **Labels Required?** | Yes (Supervised) | No | No (Unsupervised) |
| **State Size** | 50-200 KB | 48 bytes | 56 bytes |
| **CRIUtar Size** | ~25 MB | ~15 MB | ~15 MB |

The EWMA-Residual Detector is the definitive choice for the Space Cloud V6 Data Plane. It acts as a rigorously constrained proxy workload, proving that complex mathematical state can be preserved across physical nodes. Crucially, the $O(1)$ memory constraint guarantees that the model size will never increase. By keeping the CRIU tar strictly at ~15 MB, we mathematically enforce the survival of the swarm during an emergency multi-hop relay. 

At an ISL bandwidth of 50 Mbps, the transfer equation yields:
$$T_{transfer}=\frac{15~MB\times8}{50~Mbps}=2.4~seconds$$
This 40% reduction in migration time (compared to Candidate 1) provides the critical thermal safety margin necessary for the satellite to successfully evacuate the workload before reaching the 120°C hardware failure threshold.

---

## 2. Control Plane: Topology & Predictive Routing

To simulate a truly autonomous LEO constellation, the network management must be strictly bifurcated. We separate the simulation infrastructure (the environmental message bus) from the in-orbit routing intelligence (the topology master).

### 2a. The Environment Broker: K8s Control Plane (Pub/Sub)
The pure Event-Driven messaging backbone for the simulation's sensors and actuators. Static, running on the Minikube K8s Control Plane (Ground Station). In our simulated environment, the hardware sensors (thermistors, battery monitors) are represented by the Digital Twin (`environment_sim.py`). To prevent memory leaks and ensure real-time responsiveness, this Redis instance is stripped of all storage responsibilities and acts strictly as a stateless Message Broker for the simulation's physical telemetry. The centralized predictive logic (`mpc_controller.py`) has been entirely deprecated in favor of edge autonomy.

1.  **The Telemetry Flow (Sensors):** Instead of saving a global state object in a database, the Digital Twin compiles the physical parameters of each satellite into a JSON packet and broadcasts it via Redis Pub/Sub. The command used is `redis_db.publish(channel, json.dumps(sat_data))` where the channel is specific to the node, such as `telemetry/minikube-m02`. Because this is pure Pub/Sub, if no one is listening, the message simply vanishes in RAM, guaranteeing zero latency and no disk I/O bottlenecks.
2.  **Local Prediction & Self-Evacuation (The Distributed MPC):** To ensure absolute swarm autonomy, the architecture deprecates the centralized Ground Station controller. The Predictive Engine (VirtualSatellite) is embedded directly into the Node Agent running on every individual satellite. The Local MPC (inside Node A's Agent) subscribes only to its own hardware telemetry (e.g., `telemetry/minikube-m02`), mathematically isolating its sensory input to mimic a physical onboard thermistor.
3.  **Local MPC Verification Gate:** Crucially, the Local MPC operates on a two-step verification gate. First, it queries the local container runtime to verify if it is currently hosting the active tinySML payload (the `is_working` state). If the node is overheating but empty, it simply throttles its own CPU to cool down. However, if it is hosting the payload and predicts a thermal runaway within the 60-second horizon, it does not wait for a central command. Instead, it autonomously queries the Floating Master for an escape route, signals the local Guardian to enter flightMode, and triggers its own evacuation.

**Crucial Distinction:** This Ground Station Redis instance does not know the network topology, nor does it issue migration commands. It simply moves simulated hardware telemetry from the Digital Twin to the isolated Local MPCs running independently on each worker node.

### 2b. The Floating Master: Distributed Topology Engine
The centralized but fully mobile "Brain" of the constellation, responsible for maintaining the live Inter-Satellite Link (ISL) mesh graph and executing shortest-path escape queries. A stateful, migratable Kubernetes Pod running directly on a worker node (satellite) within the swarm, completely untethered from the Ground Station.

In a physically realistic LEO constellation, satellites cannot rely on a permanent uplink to a Ground Station to calculate life-saving escape routes during a thermal emergency. A communication blackout during a thermal runaway would result in catastrophic hardware loss. To solve this, the Space Cloud V6 architecture implements a Floating Master paradigm. Exactly one satellite in the swarm hosts the Redis Topology Engine. Rather than employing CPU-intensive consensus algorithms (like Raft or Paxos) which would drain battery and generate excess heat, the system uses a single migratable master that relocates itself dynamically when its host environment degrades.

**1. The Dynamic Graph & Atomic Pathfinding**
The Floating Master does not hold a static routing table, as orbital mechanics and thermal loads render static paths obsolete within seconds. Instead, it maintains a live representation of the network graph using highly efficient Redis core data structures:
* **Node Telemetry (Hashes):** Stores the current metrics for every node (e.g., `HSET node:m02 temp 45.2 battery 78`).
* **Adjacency Lists (Sets):** Defines which satellites currently possess physical line-of-sight to one another (e.g., `SADD adj:m02 m03 m04`).
* **Edge Weights (Hashes):** Represents the routing cost between any two connected nodes.

The cost of an edge is calculated via a Composite Weight Function that translates physical constraints into a mathematical heuristic:
$$w(u,v)=\frac{d(u,v)}{d_{max}}+\frac{1}{SNR(u,v)}+(1-\frac{B_{v}}{100})+\frac{max(T_{v}-T_{safe},0)}{T_{fuse}-T_{safe}}$$

To eliminate Time-of-Check/Time-of-Use (TOCTOU) race conditions during an emergency, the pathfinding logic is embedded directly inside the Floating Master as a server-side Lua script. When a worker node predicts an imminent thermal runaway, it sends an EVALSHA command to the Master. The Lua script executes an atomic Dijkstra shortest-path algorithm in $O(V^{2}+E)$ time, returning a JSON array of the safest multi-hop route (e.g., `["m02", "m03", "m04"]`).

**2. Asynchronous Telemetry Ingestion (The Directed Push Model)**
To evaluate the equation $w(u,v)$, the Master must know the destination's battery $(B_{v})$ and temperature $(T_{v})$. However, requiring every satellite to broadcast its telemetry to the entire swarm would trigger a "Broadcast Storm" ($O(N^{2})$ traffic), rapidly depleting the limited ISL bandwidth and battery reserves. To prevent this, the architecture utilizes a Directed Push Model with Event-Triggered Updates (Delta Encoding):
* **Targeted Transmission:** Worker nodes do not broadcast to the mesh. They send their telemetry only to the Floating Master's address.
* **Delta Encoding:** Satellites only transmit updates when their internal state deviates significantly from their last broadcast. For example, a transmission is triggered only if $\Delta T\ge1.0$ (temperature changes by 1°C) or $\Delta B\ge5$ (battery drops by 5%).
* **Heartbeat Mechanism:** To ensure the Master does not classify a stable, quiet node as "dead," a microscopic heartbeat ping is sent every 10 seconds.

This guarantees that the Master's Redis Hashes remain >95% accurate while reducing fleet network traffic by orders of magnitude compared to a continuous polling or full-mesh approach.

**3. Migration of the Brain (Self-Preservation)**
The architectural triumph of the Floating Master is that it is itself a stateful, migratable workload. The node hosting the "Brain" is equally susceptible to orbital eclipses and solar radiation. If the MPC local to the Floating Master predicts a thermal runaway, the Control Plane executes a self-preservation protocol:
1.  The Master rejects incoming telemetry updates.
2.  The Redis process (containing the entire graph and Lua script) is suspended, and CRIU captures its RAM state into a tar snapshot.
3.  The original Pod is destroyed via a Kubernetes forceful deletion.
4.  The tar is transmitted via the Multi-Hop L7 Relay to a cooler satellite.
5.  A new Pod is instantiated on the target node, and CRIU injects the memory back into the Redis process. The database resumes instantly with all edge weights and adjacency lists intact.

**4. Fleet Auto-Adjustment and Service Discovery**
When the Floating Master migrates, its physical IP address changes. The worker nodes (running the tinySML Data Plane) adapt to this topology shift automatically, requiring zero complex consensus protocols. This is simulated via Kubernetes Cloud Native service discovery. The worker nodes are hardcoded to query a static DNS name (e.g., `topology-master:6379`) backed by a Kubernetes ClusterIP Service.
* **During Master Migration:** The K8s Endpoint disappears. If a worker node attempts to push telemetry or request an escape route, the TCP connection will fail (Connection Refused). The worker node's software catches this exception and enters a Retry Loop (buffering the telemetry/request locally).
* **Post-Migration:** The moment the Master's container is rehydrated by CRIU on the new satellite, the Kubernetes Endpoints controller updates the routing tables. The worker nodes' subsequent TCP retries succeed, buffered telemetry is instantly flushed to the Master, and the swarm's operations continue seamlessly.

---

## 3. Network Architecture & Multi-Hop Relay

### 3.0 The Shift from Flat Networking to Physical Reality
In Version 5 of the Space Cloud architecture, the migration of the memory snapshot relied on a direct, peer-to-peer gRPC tunnel between the source and destination satellites. While highly performant, this approach exposed a critical simulation flaw: Kubernetes provides a "flat" Layer 3 fabric where any Pod can communicate directly with any other Pod via its Cluster IP, completely bypassing physical topology constraints. 

In a real Low Earth Orbit (LEO) constellation, satellites communicate via Inter-Satellite Links (ISL) that require direct line-of-sight. If Node A needs to migrate its payload to Node C, but Node B is in the way, the data must bounce through Node B. End-to-end gRPC streams are brittle in this scenario; if a single intermediate link drops due to an orbital eclipse, the entire Layer 7 stream collapses. To accurately reflect physical reality, Version 6 deprecates the direct gRPC tunnel and introduces a Store-and-Forward Multi-Hop Relay, segregating the decision-making intelligence from the physical data transmission.

### 3.1 Software-Defined Networking in Space: L7 vs. L2/L3
To orchestrate this multi-hop routing, the architecture utilizes a strict separation of concerns, acting as a space-grade Software-Defined Network (SDN):
* **Layer 7 (The Orchestrator):** The Floating Master (Redis) operates at the Application Layer. Standard network routers only understand IP addresses and latency; they are blind to the physical health of the nodes. Therefore, the Floating Master computes the semantic path based on hardware metrics (battery, thermal headroom, SNR) and generates a routing manifest (e.g., `["m02", "m03", "m04"]`).
* **Layer 2 / Layer 3 (The Muscle):** The actual movement of the bits is constrained by simulated physics. We use the Linux `tc` (Traffic Control) utility at the operating system level to manipulate the kernel's queueing discipline (`qdisc`). By enforcing a rate limit of 50 Mbps and injecting 40 ms of propagation delay per hop, we perfectly mirror the bandwidth and speed-of-light constraints of physical RF/optical transceivers.

### 3.2 The Store-and-Forward Relay Mechanism: Simulation vs. Orbital Reality
When the Model Predictive Control (MPC) triggers an emergency migration, the Guardian sidecar receives a full routing manifest from the Floating Master (e.g., `["minikube-m02", "minikube-m03", "minikube-m04"]`) and writes it to `/tmp/migration_manifest.json`. However, translating this manifest into the physical movement of a 15 MB frozen memory snapshot requires fundamentally different mechanisms in orbit compared to our local cluster. To prove the viability of this architecture, our simulation meticulously replicates the constraints of a space-grade Store-and-Forward network.

**The Orbital Reality: Delay-Tolerant Networking (DTN)**
In a physical Low Earth Orbit (LEO) constellation, satellites do not possess continuous, unbroken ethernet fabrics. They communicate via Inter-Satellite Links (ISL) using Radio Frequency (RF) or Free-Space Optical (FSO) laser transceivers. Because satellites move at 7.5 km/s, intermediate links can frequently drop due to micro-vibrations, orbital occlusions, or solar eclipses. Standard end-to-end TCP connections collapse under these conditions. Instead, aerospace networks utilize Delay-Tolerant Networking (DTN) powered by standards like the CCSDS File Delivery Protocol (CFDP). If Node A must send a frozen workload to Node C via Node B, the process in orbit looks like this:
1.  Node A's CFDP engine segments the memory snapshot into space packets and transmits them via its ISL transceiver.
2.  Node B receives the packets, reassembles the file into its onboard non-volatile storage, and mathematically verifies the payload to ensure cosmic radiation hasn't caused a bit-flip.
3.  Once verified, Node B establishes an ISL connection with Node C and forwards the payload. If the $B\rightarrow C$ link is temporarily blocked, Node B safely stores the payload until line-of-sight is restored.

**The Terrestrial Simulation: relay_transfer.sh**
In our Minikube cluster, Kubernetes provides a flat L3 virtual network that attempts to bypass these physical realities. To enforce the strict Store-and-Forward behavior of CFDP, we utilize a custom orchestrator script: `relay_transfer.sh`. While this script uses standard SSH pipes as the underlying transport, it is architected to behave exactly like a DTN node:
* **The Staging Bounce:** Rather than streaming directly to the final destination, the script pipes the CRIU tar file through Minikube SSH tunnels, moving it sequentially from the source node to a staging directory (`/tmp/relay/`) on the intermediate hop.
* **Cryptographic Verification:** Mimicking the radiation checks of space hardware, the script executes a `sha256sum` integrity check at every single intermediate stop. Only if the source and destination hashes match perfectly does the relay proceed to the next hop.
* **Mathematical Enforcement (L2 Throttling):** To ensure the SSH pipe perfectly mimics the bandwidth of a physical RF or optical ISL transceiver, the architecture applies the Linux `tc` (Traffic Control) utility at the operating system level. We artificially enforce a strict bandwidth limit of 50 Mbps and inject 40 ms of propagation delay per hop.

By combining the L7 routing manifest with OS-level L2 throttling, the terrestrial simulation is mathematically locked to the physical transfer equation:
$$T_{transfer} = \frac{15~MB \times 8}{50~Mbps} = 2.4~seconds~per~hop$$

While the `minikube ssh` command is merely a terrestrial simulation vehicle, architecturally, it flawlessly mimics a Delay-Tolerant Network utilizing the CCSDS File Delivery Protocol. By enforcing intermediate staging, SHA256 cryptographic verification, and precise OS-level bandwidth throttling, the system proves that stateful migration can survive the hostile, disconnected realities of deep space routing.

### 3.3 The Danger Zone: Stateful Socket Migration & The TCP Sequence Gap
Migrating the frozen memory of a container is straightforward; migrating its live network connections is exceptionally dangerous. Originally, the architecture used TCP sockets, requiring the `--tcp-established` flag to force CRIU to extract the Linux kernel's internal TCP Control Block (TCB). However, bouncing a frozen TCP socket across a multi-hop relay introduces a fatal flaw known as the TCP Sequence Gap:
1.  **The Snapshot:** CRIU freezes the tinySML userspace process and saves the kernel's expected sequence number (e.g., $RCV.NXT=1000$) into the tar file. The star begins its 4.8-second multi-hop journey.
2.  **Phantom ACKs:** The data streamer on Earth continues sending packets (1001, 1002). Because the Linux kernel on the source satellite is still running, it receives these packets in its buffer and sends TCP ACKs back to Earth.
3.  **The Fatal Collision:** 4.8 seconds later, the tar is restored on the destination satellite. CRIU forcefully overwrites the new kernel's state, telling it to expect packet 1000. The data streamer, having already received ACKs for the previous packets, sends packet 1003. The new kernel detects a massive sequence gap, assumes severe network corruption, and immediately fires a TCP RST (Reset), instantly and permanently killing the connection.

While this can be mitigated using aggressive OS-level hacks (like injecting sub-microsecond iptables DROP rules to pause the source kernel), doing so is fragile and misaligned with the workload's actual needs.

### 3.4 The UDP Paradigm Shift: Architectural Elegance
To solve the stateful networking crisis, Version 6 fundamentally changes the transport protocol. We abandon TCP entirely for the Data Plane and transition the data streamer and tinySML worker to UDP (User Datagram Protocol). This is not a compromise; it is an elegant alignment of the network protocol with the application payload:
* **Stateless Transport:** UDP has no TCP Control Block (TCB), no sequence numbers, and no ACKs. It simply fires datagrams.
* **Loss-Tolerant Workload:** The Streaming Machine Learning (SML) algorithm executing the EWMA-Residual anomaly detection processes data at 10 Hz. If it misses 48 packets during the 4.8-second flight, the algorithm does not fail; it simply ingests the very next packet that arrives and continues learning.

**The UDP Migration Flow:**
1.  The tinySML container is frozen via CRIU. No complex `--tcp-established` flags or kernel state extractions are required.
2.  The data streamer continues firing UDP packets into space.
3.  During the multi-hop flight, packets arriving at the old satellite bounce off the closed port and are safely dropped.
4.  The moment the container is restored on the new satellite, its UDP listening port opens. The very next packet transmitted from Earth is immediately received and processed.

By switching to UDP, we eliminate the need for iptables hacks, bypass the TCP Sequence Gap entirely, and demonstrate a profound understanding of how to engineer resilient Edge AI systems in extreme, lossy environments.

---

## 4. Critical Danger Zones & Mitigations

Migrating a stateful, live container across a physical mesh network in under 12 seconds is an extreme engineering challenge. During the critical execution window where the application is suspended, the separation between userspace memory and operating system kernel state creates severe vulnerabilities. To guarantee zero-downtime Checkpoint/Restore In Userspace (CRIU) migrations, the Space Cloud V6 architecture proactively addresses three catastrophic edge cases.

### 4.1 Danger Zone 1: The State Race Condition
In our Dual-Plane architecture, the Streaming Machine Learning (SML) computation happens in the tinySML container (the Phoenix), but the memory state is preserved by the sidecar (the Guardian). The tinySML worker updates its EWMA baseline $(\hat{\mu}_{t})$ and variance $(\hat{\sigma}_{t})$ at 10 Hz. It periodically saves this state to the Guardian via an HTTP POST. If the Model Predictive Control (MPC) triggers a thermal evacuation, the Guardian enters `flightMode` and immediately closes its sockets to prepare for the CRIU freeze. If the Guardian locks its sockets just before the tinySML worker pushes its latest calculation, the Guardian is frozen holding a stale state. At a 10 Hz ingestion rate, a 1-second drift in the baseline estimate is enough to trigger a false positive (flagging a phantom wildfire) immediately upon restoration on the new satellite.

**The Mitigation (Pre-Freeze Flush)**
To eliminate this Time-of-Check/Time-of-Use (TOCTOU) vulnerability, we implemented a synchronous Pre-Freeze Flush. Before the Guardian enters `flightMode`, it publishes a highly prioritized Redis command: `PUBLISH commands/(hostname) {"action": "FLUSH_STATE"}`. The tinySML worker intercepts this, halts processing, and immediately POSTs its final exact state to the Guardian. The Guardian accepts this final update before locking. This orchestration narrows the race window from a full second down to the Redis Pub/Sub latency margin of ~2 milliseconds.

### 4.2 Danger Zone 2: Redis I/O Blocking (Control Plane Saturation)
The Floating Master (Redis) is the "Brain" of the constellation. Redis operates on a strictly single-threaded event loop. By default, standard Redis deployments use RDB (Redis Database) snapshots or AOF (Append-Only File) persistence to save data to the disk. In our simulation, the `topology_manager.py` daemon executes over 60 HSET commands per second to update the highly volatile node telemetry and edge weights across the mesh. If AOF persistence is enabled, every single HSET command triggers a disk fsync operation. Because disk I/O is orders of magnitude slower than RAM, these constant fsync operations block the Redis single thread. If a satellite overheats and requests an escape route during an fsync block, the Dijkstra Lua script will stall. The query times out, and the Guardian is forced to migrate blindly, potentially landing on another overheating satellite.

**The Mitigation (Total Ephemerality)**
Because the telemetry and topology data is derived dynamically every second, there is zero architectural value in persisting it to a hard drive. We strictly modify the Kubernetes/Helm configuration for the Redis Pod to completely disable all persistence mechanisms:
* `--save ""` (Disables RDB snapshots)
* `--appendonly no` (Disables AOF)

This guarantees that Redis operates 100% in memory, ensuring the Dijkstra Lua script executes with absolute atomicity and sub-millisecond latency, entirely decoupled from disk I/O bottlenecks.

---

## 5. Summary: Simulation vs. Orbital Reality

To perfectly defend your thesis, here is the consolidated, numbered list of the core differences between our simulated architecture and a physical deployment in orbit.

### 5.1 Orchestration Architecture
* **Simulation:** Centralized Kubernetes. The Ground Station (Minikube host) runs an omniscient `etcd` database that demands ultra-low latency to maintain cluster quorum. If a node loses connection, the Control Plane attempts to evict it.
* **Reality:** Federated Edge Orchestration. Satellites run autonomous micro-orchestrators (like KubeEdge, K3s, or NASA's Core Flight System). If the ground link drops, the satellite continues operating its local workloads entirely autonomously.

### 5.2 Telemetry Acquisition
* **Simulation:** The Digital Twin (`environment_sim.py`). A centralized Python script mathematically generates fake temperatures and battery levels for the entire fleet and publishes them to the Redis message broker.
* **Reality:** Physical Hardware Buses. The satellite's On-Board Computer (OBC) reads actual voltage and thermal data directly from the Battery Management System (BMS) and hardware thermistors via physical I2C or CAN bus lines.

### 5.3 Network Transport Layer
* **Simulation:** SSH Pipes & OS Throttling. We bounce the CRIU tar file through the flat virtual network using `minikube ssh` and enforce physical bandwidth/latency limits using Linux `tc` (Traffic Control).
* **Reality:** Delay-Tolerant Networking (DTN). Space relies on the CCSDS File Delivery Protocol (CFDP) and Bundle Protocol over Radio Frequency (RF) or Free-Space Optical (laser) links. These protocols are specifically engineered to store-and-forward packets across highly fragmented networks where direct TCP handshakes are impossible.

### 5.4 Hardware and Memory Stability
* **Simulation:** Standard x86_64 Virtual Machines. The RAM where the tinySML payload resides is perfectly stable, making the CRIU snapshot highly predictable.
* **Reality:** Radiation-Hardened Silicon. Satellites operate in extreme radiation environments causing Single Event Upsets (random bit-flips in memory). They use Rad-Hardened CPUs and ECC (Error Correction Code) memory. Capturing a CRIU snapshot in this environment requires the OS to aggressively scrub the memory for radiation corruption before packaging the tar file, ensuring a corrupted state isn't migrated to a healthy node.