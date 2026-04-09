"""
===============================================================================
SPACE CLOUD V6 - FLOATING MASTER TOPOLOGY DASHBOARD
===============================================================================

🧠 HIGH-LEVEL PURPOSE:
This service is a **real-time topology visualization dashboard** for the
satellite swarm's ISL (Inter-Satellite Link) mesh network.

It runs as a **sidecar container** inside the same Kubernetes Pod as the
Floating Master (Redis-based topology engine).

Because of this:
- It connects to Redis via localhost (same network namespace)
- It migrates together with the Floating Master across satellites
- It always visualizes the "current brain" of the system

-------------------------------------------------------------------------------
🏗️ CORE RESPONSIBILITIES:

1. TOPOLOGY EXTRACTION (Redis Polling)
   - Reads:
     - Node telemetry (Redis Hashes: node:<name>)
     - Active fleet membership (Set: active_fleet)
     - Adjacency graph (Sets: adj:<name>)
   - Computes edge weights using a **Dijkstra-compatible cost function**

2. STATE MANAGEMENT (In-Memory)
   - topology_state = { nodes, edges }
   - Protected with threading locks

3. REAL-TIME DELIVERY (SSE)
   - Clients connect to /stream
   - Server pushes updates continuously

4. FRONTEND VISUALIZATION
   - Graph layout (canvas)
   - Node telemetry table
   - Edge weights table

-------------------------------------------------------------------------------
⚠️ DESIGN CHOICE: POLLING (NOT PUB/SUB)

Unlike the global dashboard:
- This system POLLS Redis every 1 second
- Reason: topology must be recomputed as a full snapshot

-------------------------------------------------------------------------------
"""

import json
import threading
import time
import redis
import os
from http.server import HTTPServer, BaseHTTPRequestHandler


# =============================================================================
# CONFIGURATION
# =============================================================================

# Redis is local because this runs in the SAME Pod as the Floating Master
REDIS_HOST = "localhost"
REDIS_PORT = 6379

# Port exposed by this dashboard
DASHBOARD_PORT = 8081

# Polling interval (controls refresh rate vs CPU usage)
POLL_INTERVAL = 1.0

# Thermal thresholds (must match routing logic in Lua script)
T_SAFE = float(os.getenv("T_SAFE", "80.0"))   # below this → safe
T_FUSE = float(os.getenv("T_FUSE", "120.0"))  # above this → unusable


# =============================================================================
# TOPOLOGY READER
# =============================================================================

def read_topology(r):
    """
    🧠 PURPOSE:
    Reads the FULL network topology from Redis and builds a graph representation.

    RETURNS:
    {
        nodes: {
            node_name: {
                temp, battery, orbit_plane, angle, is_working, updated_at
            }
        },
        edges: [
            { from, to, weight, same_plane }
        ]
    }

    🧠 KEY DESIGN:
    - Uses Redis Sets instead of KEYS (critical for scalability)
    - Computes edge weights dynamically (not stored in Redis)
    - Produces DIRECTED edges (A → B different from B → A)
    """

    nodes = {}
    edges = []

    # -------------------------------------------------------------------------
    # READ ACTIVE NODES
    # -------------------------------------------------------------------------

    # ⚠️ IMPORTANT:
    # Using SMEMBERS instead of KEYS avoids blocking Redis
    fleet_members = r.smembers("active_fleet")

    for name in fleet_members:
        data = r.hgetall(f"node:{name}")

        if data:
            nodes[name] = {
                "temp":        float(data.get("temp", 999)),  # default = extreme value
                "battery":     float(data.get("battery", 0)),
                "orbit_plane": data.get("orbit_plane", "?"),
                "angle":       float(data.get("angle", 0)),
                "is_working":  bool(int(data.get("is_working", 0))),  # ⚠️ string → int → bool
                "updated_at":  int(data.get("updated_at", 0)),
            }

    # -------------------------------------------------------------------------
    # BUILD EDGES (GRAPH CONSTRUCTION)
    # -------------------------------------------------------------------------

    for name, node_data in nodes.items():

        adj_key = f"adj:{name}"
        neighbours = r.smembers(adj_key)

        for neighbour in neighbours:

            # ⚠️ Skip nodes not yet in local snapshot (consistency guard)
            if neighbour not in nodes:
                continue

            nb = nodes[neighbour]

            T_v = nb["temp"]
            B_v = nb["battery"]

            # -----------------------------------------------------------------
            # EDGE WEIGHT COMPUTATION (CRITICAL LOGIC)
            # -----------------------------------------------------------------

            if T_v >= T_FUSE:
                # Node is overheated → cannot be used → infinite cost
                weight = float("inf")

            else:
                # Heat penalty increases as temperature approaches fuse
                heat_penalty = max(0, (T_v - T_SAFE) / (T_FUSE - T_SAFE))

                # Lower battery → higher cost
                battery_cost = 1.0 - (B_v / 100.0)

                # SNR proxy: low battery → worse signal → higher cost
                # ⚠️ max(..., 0.01) prevents division by zero
                snr_cost = 1.0 / max(B_v / 100.0, 0.01)

                # Final composite cost (rounded for readability)
                weight = round(snr_cost + battery_cost + heat_penalty, 3)

            edges.append({
                "from":        name,
                "to":          neighbour,
                "weight":      weight,
                "same_plane":  node_data["orbit_plane"] == nb["orbit_plane"],
            })

    return {"nodes": nodes, "edges": edges}


# =============================================================================
# SHARED STATE + SYNCHRONIZATION
# =============================================================================

topology_state = {"nodes": {}, "edges": []}

# Protects topology_state from concurrent access
topology_lock  = threading.Lock()

# SSE client management
sse_clients    = []
sse_lock       = threading.Lock()


# =============================================================================
# BACKGROUND POLLER
# =============================================================================

def topology_poller():
    """
    🧠 PURPOSE:
    Continuously polls Redis and updates topology_state.

    FLOW:
        Redis → read_topology() → update state → broadcast

    RESILIENCE:
    - Reconnects automatically if Redis fails
    """

    global topology_state

    while True:
        try:
            r = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                decode_responses=True,  # ⚠️ ensures string decoding
                socket_connect_timeout=2
            )

            r.ping()
            print(f"✅ TOPOLOGY DASHBOARD: Connected to localhost Redis.", flush=True)

            while True:
                data = read_topology(r)

                # ⚠️ Critical section (shared state write)
                with topology_lock:
                    topology_state = data

                _broadcast(data)

                time.sleep(POLL_INTERVAL)

        except Exception as e:
            print(f"⚠️  TOPOLOGY DASHBOARD: Redis error: {e}. Retrying in 3s...", flush=True)
            time.sleep(3)


def _broadcast(data):
    """
    🧠 PURPOSE:
    Sends updated topology to all connected browsers via SSE.

    DETAILS:
    - Formats message as SSE event
    - Removes dead clients
    """

    payload = f"data: {json.dumps(data)}\n\n"

    with sse_lock:
        dead = []

        for client in sse_clients:
            try:
                client.wfile.write(payload.encode())
                client.wfile.flush()  # ⚠️ immediate push (no buffering)

            except Exception:
                dead.append(client)

        # Cleanup disconnected clients
        for d in dead:
            sse_clients.remove(d)


# =============================================================================
# DASHBOARD HTML
# =============================================================================

# ⚠️ Inline HTML eliminates need for static file server
DASHBOARD_HTML = r"""... (UNCHANGED HTML CONTENT) ..."""


# =============================================================================
# HTTP SERVER
# =============================================================================

class TopoHandler(BaseHTTPRequestHandler):
    """
    🧠 PURPOSE:
    Handles:
    - /stream → SSE connection
    - /       → Dashboard HTML
    """

    def do_GET(self):

        if self.path == "/stream":

            # -----------------------------------------------------------------
            # SSE SETUP
            # -----------------------------------------------------------------

            self.send_response(200)

            self.send_header("Content-Type",  "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection",    "keep-alive")

            self.end_headers()

            # Register new client
            with sse_lock:
                sse_clients.append(self)

            # Send immediate snapshot
            try:
                with topology_lock:
                    snap = dict(topology_state)  # ⚠️ shallow copy to avoid race

                self.wfile.write(f"data: {json.dumps(snap)}\n\n".encode())
                self.wfile.flush()

            except Exception:
                pass

            # Keep connection alive
            try:
                while True:
                    time.sleep(1)

            except Exception:
                pass

            finally:
                # Remove client on disconnect
                with sse_lock:
                    if self in sse_clients:
                        sse_clients.remove(self)

        else:
            # Serve dashboard UI
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()

            self.wfile.write(DASHBOARD_HTML.encode())

    def log_message(self, format, *args):
        """
        🧠 Disable noisy HTTP logs
        """
        pass


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    """
    🧠 BOOT PROCESS:

    1. Start background topology poller
    2. Start HTTP server (blocking)
    """

    print(f"🌐 TOPOLOGY DASHBOARD starting on http://0.0.0.0:{DASHBOARD_PORT}", flush=True)

    poller = threading.Thread(
        target=topology_poller,
        daemon=True  # ⚠️ auto-killed with main process
    )
    poller.start()

    server = HTTPServer(
        ("0.0.0.0", DASHBOARD_PORT),  # listen on all interfaces
        TopoHandler
    )

    print(f"✅ Topology Dashboard ready at http://0.0.0.0:{DASHBOARD_PORT}", flush=True)

    server.serve_forever()  # ⚠️ blocking call