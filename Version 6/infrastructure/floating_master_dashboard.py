"""
SPACE CLOUD V6 - FLOATING MASTER TOPOLOGY DASHBOARD
====================================================
Role: A lightweight SSE-based web server that runs as a SIDECAR inside the
      Floating Master Pod, alongside the Redis topology engine. It reads the
      live ISL mesh graph from the co-located Redis (via localhost:6379) and
      pushes real-time updates to any connected browser.

Because this sidecar lives in the same Pod as the Floating Master Redis,
it travels with the topology engine across satellite migrations — the
"brain" and its "eyes" move together.

Visualizes:
  - All constellation nodes with live temperature + battery readings
  - ISL adjacency graph (which satellites are currently in line-of-sight)
  - Edge weights from the Dijkstra composite cost function
  - Orbital plane labels (used for lateral routing bias)

Exposes: http://<pod-ip>:8081  (also reachable via topology-dashboard K8s Service)
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

# Connect to the co-located Redis via localhost (same Pod network namespace)
REDIS_HOST = "localhost"
REDIS_PORT = 6379
DASHBOARD_PORT = 8081

# How often to poll Redis for topology updates (seconds)
POLL_INTERVAL = 1.0

# Physical constants matching the Lua Dijkstra script
T_SAFE = float(os.getenv("T_SAFE", "80.0"))
T_FUSE = float(os.getenv("T_FUSE", "120.0"))


# =============================================================================
# TOPOLOGY READER — polls Redis for node Hashes and adjacency Sets
# =============================================================================

def read_topology(r):
    """
    Reads the full ISL mesh graph from Redis and computes the Dijkstra
    composite edge weight for each directed edge.

    Returns a dict with:
      nodes: { node_name: { temp, battery, orbit_plane, angle, is_working } }
      edges: [ { from, to, weight } ]
    """
    nodes = {}
    edges = []

    # Read all node Hash entries (pushed by each Node Agent)
    node_keys = r.keys("node:*")
    for key in node_keys:
        name = key[5:]  # Strip "node:" prefix
        data = r.hgetall(key)
        if data:
            nodes[name] = {
                "temp":        float(data.get("temp", 999)),
                "battery":     float(data.get("battery", 0)),
                "orbit_plane": data.get("orbit_plane", "?"),
                "angle":       float(data.get("angle", 0)),
                "is_working":  bool(int(data.get("is_working", 0))),
                "updated_at":  int(data.get("updated_at", 0)),
            }

    # Read adjacency Sets and compute edge weights
    for name, node_data in nodes.items():
        adj_key = f"adj:{name}"
        neighbours = r.smembers(adj_key)
        for neighbour in neighbours:
            if neighbour not in nodes:
                continue  # Neighbour not yet registered

            nb = nodes[neighbour]
            T_v = nb["temp"]
            B_v = nb["battery"]

            # Skip completely overheated nodes (they are impassable)
            if T_v >= T_FUSE:
                weight = float("inf")
            else:
                # Heat penalty: 0 when cool, approaches 1 near the fuse temperature
                heat_penalty = max(0, (T_v - T_SAFE) / (T_FUSE - T_SAFE))
                # Battery cost: low battery → higher routing cost
                battery_cost = 1.0 - (B_v / 100.0)
                # SNR proxy: satellites with more battery have better radio power
                snr_cost = 1.0 / max(B_v / 100.0, 0.01)
                weight = round(snr_cost + battery_cost + heat_penalty, 3)

            edges.append({
                "from":        name,
                "to":          neighbour,
                "weight":      weight,
                "same_plane":  node_data["orbit_plane"] == nb["orbit_plane"],
            })

    return {"nodes": nodes, "edges": edges}


# =============================================================================
# SSE BROADCAST — shared state updated by the poller thread
# =============================================================================

topology_state = {"nodes": {}, "edges": []}
topology_lock  = threading.Lock()
sse_clients    = []
sse_lock       = threading.Lock()


def topology_poller():
    """
    Runs in a background thread. Polls Redis every POLL_INTERVAL seconds
    and broadcasts fresh topology data to all connected SSE clients.
    """
    global topology_state

    while True:
        try:
            r = redis.Redis(
                host=REDIS_HOST, port=REDIS_PORT,
                decode_responses=True,
                socket_connect_timeout=2
            )
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
    """Sends the topology JSON to all connected SSE clients."""
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
# DASHBOARD HTML (inline — no static file server needed)
# =============================================================================

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Space Cloud V6 — Topology Dashboard</title>
<style>
  :root {
    --bg:#090d1a; --panel:#111827; --card:#1a2236;
    --fire:#ff6b35; --pred:#00d4ff; --green:#00ff88;
    --warn:#ffcc00; --dim:#64748b; --border:#1e3a5f; --text:#e2e8f0;
  }
  * { box-sizing:border-box; margin:0; padding:0; }
  body { background:var(--bg); color:var(--text); font-family:'Courier New',monospace; padding:20px; }
  header {
    background:var(--panel); border:1px solid var(--border); border-radius:8px;
    padding:14px 22px; margin-bottom:20px;
    display:flex; align-items:center; justify-content:space-between;
  }
  .title { color:var(--pred); font-size:1.1rem; letter-spacing:2px; font-weight:bold; }
  .grid { display:grid; grid-template-columns:1fr 340px; gap:16px; }
  @media(max-width:800px){ .grid{ grid-template-columns:1fr; } }
  .panel { background:var(--panel); border:1px solid var(--border); border-radius:8px; padding:16px; }
  .pt { font-size:.72rem; text-transform:uppercase; letter-spacing:2px; color:var(--dim); margin-bottom:14px; }
  canvas { display:block; width:100%; max-width:560px; margin:0 auto; }
  table { width:100%; border-collapse:collapse; font-size:.78rem; }
  th { color:var(--dim); text-align:left; padding:6px 4px; text-transform:uppercase;
       font-size:.68rem; letter-spacing:1px; border-bottom:1px solid var(--border); }
  td { padding:8px 4px; border-bottom:1px solid rgba(30,58,95,.4); }
  .ok{color:var(--green)} .warm{color:var(--warn)} .hot{color:var(--fire)}
  #conn { font-size:.75rem; }
</style>
</head>
<body>
<header>
  <div class="title">🌐 FLOATING MASTER — ISL TOPOLOGY DASHBOARD</div>
  <div id="conn" style="color:var(--dim)">Connecting...</div>
</header>
<div class="grid">
  <div class="panel">
    <div class="pt">📡 Live ISL Mesh Graph</div>
    <!--
      Each node is drawn as a labelled circle positioned on a ring.
      Edges are drawn as lines with width proportional to weight (thin = cheap, thick = expensive).
      Color: green = nominal, yellow = warm, red = hot.
    -->
    <canvas id="graph" width="560" height="400"></canvas>
  </div>
  <div class="panel">
    <div class="pt">🛰️ Node Telemetry</div>
    <table><thead><tr><th>Node</th><th>Plane</th><th>Temp</th><th>Battery</th><th>Angle</th></tr></thead>
    <tbody id="node-tbody"></tbody></table>
    <div class="pt" style="margin-top:18px;">⚡ Edge Weights</div>
    <table><thead><tr><th>From</th><th>To</th><th>Weight</th><th>Plane</th></tr></thead>
    <tbody id="edge-tbody"></tbody></table>
  </div>
</div>
<script>
const canvas = document.getElementById('graph');
const ctx    = canvas.getContext('2d');

// Map node names to fixed positions on a ring
function placeNodes(names) {
  const cx=280, cy=200, r=150;
  const pos={};
  names.forEach((n,i)=>{
    const a = (i/names.length)*Math.PI*2 - Math.PI/2;
    pos[n] = { x: cx+r*Math.cos(a), y: cy+r*Math.sin(a) };
  });
  return pos;
}

function tempColor(t) {
  if(t>80) return '#ff6b35';
  if(t>60) return '#ffcc00';
  return '#00ff88';
}

function drawGraph(data) {
  const {nodes, edges} = data;
  const names = Object.keys(nodes);
  if(!names.length){ ctx.clearRect(0,0,canvas.width,canvas.height); return; }

  const pos = placeNodes(names);
  ctx.clearRect(0,0,canvas.width,canvas.height);

  // Draw edges first (behind nodes)
  edges.forEach(e=>{
    const a=pos[e.from], b=pos[e.to];
    if(!a||!b) return;
    const w = isFinite(e.weight) ? e.weight : 99;
    // Thick red = expensive, thin green = cheap
    const alpha = Math.min(1, 0.2 + w*0.08);
    ctx.beginPath();
    ctx.moveTo(a.x,a.y); ctx.lineTo(b.x,b.y);
    ctx.strokeStyle = e.weight > 5 ? `rgba(255,107,53,${alpha})` : `rgba(0,212,255,${alpha})`;
    ctx.lineWidth = Math.max(0.5, 3 - w*0.3);
    ctx.stroke();

    // Edge weight label at midpoint
    const mx=(a.x+b.x)/2, my=(a.y+b.y)/2;
    ctx.fillStyle='#64748b';
    ctx.font='9px Courier New';
    ctx.textAlign='center';
    ctx.fillText(isFinite(e.weight)?e.weight.toFixed(2):'∞', mx, my);
  });

  // Draw nodes
  names.forEach(n=>{
    const p=pos[n], nd=nodes[n];
    if(!p) return;
    const col = tempColor(nd.temp);

    // Outer ring for working nodes
    if(nd.is_working){
      ctx.beginPath(); ctx.arc(p.x,p.y,18,0,Math.PI*2);
      ctx.strokeStyle=col; ctx.lineWidth=2; ctx.globalAlpha=.4; ctx.stroke(); ctx.globalAlpha=1;
    }

    ctx.beginPath(); ctx.arc(p.x,p.y,12,0,Math.PI*2);
    ctx.fillStyle=col; ctx.fill();

    ctx.fillStyle='#090d1a'; ctx.font='bold 8px Courier New'; ctx.textAlign='center';
    ctx.fillText(nd.orbit_plane||'?', p.x, p.y+3);

    ctx.fillStyle='#e2e8f0'; ctx.font='9px Courier New'; ctx.textAlign='center';
    ctx.fillText(n.replace('minikube-',''), p.x, p.y+26);
    ctx.fillStyle='#64748b';
    ctx.fillText(`${nd.temp}° ${nd.battery}%`, p.x, p.y+37);
  });
}

function renderTables(data) {
  const {nodes, edges} = data;

  // Node table
  const nb = document.getElementById('node-tbody');
  nb.innerHTML = Object.entries(nodes).map(([name,nd])=>{
    const t=nd.temp, cls = t>80?'hot':t>60?'warm':'ok';
    return `<tr><td>${name.replace('minikube-','')}</td>
      <td>${nd.orbit_plane}</td><td class="${cls}">${t}°C</td>
      <td>${nd.battery}%</td><td>${nd.angle}°</td></tr>`;
  }).join('');

  // Edge table
  const eb = document.getElementById('edge-tbody');
  eb.innerHTML = edges.map(e=>`<tr>
    <td>${e.from.replace('minikube-','')}</td>
    <td>${e.to.replace('minikube-','')}</td>
    <td>${isFinite(e.weight)?e.weight.toFixed(3):'∞'}</td>
    <td>${e.same_plane?'Same':'Diff'}</td></tr>`).join('');
}

const src = new EventSource('/stream');
document.getElementById('conn').textContent='● LIVE';
document.getElementById('conn').style.color='#00ff88';
src.onerror = ()=>{ document.getElementById('conn').textContent='⏳ Reconnecting...';
                    document.getElementById('conn').style.color='#ffcc00'; };
src.onmessage = e => {
  const data = JSON.parse(e.data);
  drawGraph(data);
  renderTables(data);
};
</script>
</body>
</html>
"""


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

            # Send the current snapshot immediately so the browser gets data fast
            try:
                with topology_lock:
                    snap = dict(topology_state)
                self.wfile.write(f"data: {json.dumps(snap)}\n\n".encode())
                self.wfile.flush()
            except Exception:
                pass

            # Keep this response alive — the browser holds the SSE connection open
            try:
                while True:
                    time.sleep(1)
            except Exception:
                pass
            finally:
                with sse_lock:
                    if self in sse_clients:
                        sse_clients.remove(self)
        else:
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(DASHBOARD_HTML.encode())

    def log_message(self, format, *args):
        pass  # Suppress default HTTP access log spam


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    print(f"🌐 TOPOLOGY DASHBOARD starting on http://0.0.0.0:{DASHBOARD_PORT}", flush=True)

    poller = threading.Thread(target=topology_poller, daemon=True)
    poller.start()

    server = HTTPServer(("0.0.0.0", DASHBOARD_PORT), TopoHandler)
    print(f"✅ Topology Dashboard ready at http://0.0.0.0:{DASHBOARD_PORT}", flush=True)
    server.serve_forever()
