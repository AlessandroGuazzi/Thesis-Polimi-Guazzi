import json
import threading
import time
import redis
import os
from http.server import HTTPServer, BaseHTTPRequestHandler

# =============================================================================
# CONFIGURATION
# =============================================================================
REDIS_HOST = "localhost"
REDIS_PORT = 6379
DASHBOARD_PORT = 8081
POLL_INTERVAL = 1.0
T_SAFE = float(os.getenv("T_SAFE", "80.0"))
T_FUSE = float(os.getenv("T_FUSE", "120.0"))

# =============================================================================
# HELPER: READ HTML FILE
# =============================================================================
def get_html():
    filepath = os.path.join(os.path.dirname(__file__), "floating_master_dashboard.html")
    with open(filepath, "r", encoding="utf-8") as f:
        return f.read()

# =============================================================================
# TOPOLOGY READER & SSE BROADCAST
# =============================================================================
topology_state = {"nodes": {}, "edges": []}
topology_lock  = threading.Lock()
sse_clients    = []
sse_lock       = threading.Lock()

def read_topology(r):
    nodes = {}
    edges = []
    fleet_members = r.smembers("active_fleet")
    for name in fleet_members:
        data = r.hgetall(f"node:{name}")
        if data:
            nodes[name] = {
                "temp":        float(data.get("temp", 999)),
                "battery":     float(data.get("battery", 0)),
                "orbit_plane": data.get("orbit_plane", "?"),
                "angle":       float(data.get("angle", 0)),
                "is_working":  bool(int(data.get("is_working", 0))),
                "updated_at":  int(data.get("updated_at", 0)),
            }

    for name, node_data in nodes.items():
        adj_key = f"adj:{name}"
        neighbours = r.smembers(adj_key)
        for neighbour in neighbours:
            if neighbour not in nodes: continue
            nb = nodes[neighbour]
            T_v = nb["temp"]
            B_v = nb["battery"]
            if T_v >= T_FUSE:
                weight = float("inf")
            else:
                heat_penalty = max(0, (T_v - T_SAFE) / (T_FUSE - T_SAFE))
                battery_cost = 1.0 - (B_v / 100.0)
                snr_cost = 1.0 / max(B_v / 100.0, 0.01)
                weight = round(snr_cost + battery_cost + heat_penalty, 3)

            edges.append({
                "from":        name,
                "to":          neighbour,
                "weight":      weight,
                "same_plane":  node_data["orbit_plane"] == nb["orbit_plane"],
            })
    return {"nodes": nodes, "edges": edges}

def topology_poller():
    global topology_state
    while True:
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True, socket_connect_timeout=2)
            r.ping()
            print(f"✅ TOPOLOGY DASHBOARD: Connected to localhost Redis.", flush=True)
            while True:
                data = read_topology(r)
                with topology_lock:
                    topology_state = data
                _broadcast(data)
                time.sleep(POLL_INTERVAL)
        except Exception as e:
            print(f"⚠️  TOPOLOGY DASHBOARD: Redis error: {e}. Retrying in 3s...", flush=True)
            time.sleep(3)

def _broadcast(data):
    payload = f"data: {json.dumps(data)}\n\n"
    with sse_lock:
        dead = []
        for client in sse_clients:
            try:
                client.wfile.write(payload.encode())
                client.wfile.flush()
            except Exception:
                dead.append(client)
        for d in dead:
            sse_clients.remove(d)

# =============================================================================
# HTTP SERVER
# =============================================================================
class TopoHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/stream":
            self.send_response(200)
            self.send_header("Content-Type",  "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection",    "keep-alive")
            self.end_headers()
            with sse_lock:
                sse_clients.append(self)
            try:
                with topology_lock:
                    snap = dict(topology_state)
                self.wfile.write(f"data: {json.dumps(snap)}\n\n".encode())
                self.wfile.flush()
            except Exception: pass
            try:
                while True: time.sleep(1)
            except Exception: pass
            finally:
                with sse_lock:
                    if self in sse_clients: sse_clients.remove(self)
        else:
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            # Read the HTML dynamically from the separated file
            self.wfile.write(get_html().encode('utf-8'))

    def log_message(self, format, *args):
        pass 

if __name__ == "__main__":
    print(f"🌐 TOPOLOGY DASHBOARD starting on http://0.0.0.0:{DASHBOARD_PORT}", flush=True)
    poller = threading.Thread(target=topology_poller, daemon=True)
    poller.start()
    server = HTTPServer(("0.0.0.0", DASHBOARD_PORT), TopoHandler)
    print(f"✅ Topology Dashboard ready at http://0.0.0.0:{DASHBOARD_PORT}", flush=True)
    server.serve_forever()