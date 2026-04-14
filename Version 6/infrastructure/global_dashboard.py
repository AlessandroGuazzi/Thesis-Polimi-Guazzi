"""
SPACE CLOUD V6 - GLOBAL SWARM DASHBOARD (Ground Station — Stateless)
=====================================================================
Role: "God's Eye" operator overview running on the Ground Station host machine.
      Shows all satellite hardware health, active migration paths, and a
      timeline of migration events in real-time.

This dashboard is INTENTIONALLY STATELESS:
  - It subscribes to Ground Redis (telemetry/*) for satellite hardware data.
  - It never participates in migration. Losing and restarting it has zero
    impact on the flying swarm — it just reconnects and re-subscribes.

Exposes: http://localhost:8090
"""

import json
import threading
import os
import time
import redis
from http.server import HTTPServer, BaseHTTPRequestHandler

# =============================================================================
# CONFIGURATION
# =============================================================================

GROUND_REDIS_HOST = os.getenv("GROUND_REDIS_HOST", "localhost")
GROUND_REDIS_PORT = 6379
DASHBOARD_PORT    = 8090

# In-memory store for the latest telemetry from all satellites
fleet_state = {}
# Timeline log of migration events (most recent first)
migration_log = []
# SSE client response objects (connected browsers)
sse_clients = []
sse_lock = threading.Lock()


# =============================================================================
# REDIS TELEMETRY LISTENER
# =============================================================================

def redis_listener():
    """
    Subscribes to telemetry/* on Ground Redis and updates fleet_state.
    Runs in a background daemon thread — reconnects automatically on failure.
    """
    global fleet_state
    while True:
        try:
            r = redis.Redis(
                host=GROUND_REDIS_HOST,
                port=GROUND_REDIS_PORT,
                decode_responses=True,
                socket_connect_timeout=3
            )
            r.ping()
            pubsub = r.pubsub()

            # Subscribe to all satellite telemetry channels at once
            pubsub.psubscribe("telemetry/*")
            print(f"✅ GLOBAL DASHBOARD: Subscribed to telemetry/* on Ground Redis.", flush=True)

            for message in pubsub.listen():
                if message["type"] != "pmessage":
                    continue

                # Extract the satellite name from "telemetry/minikube-m02"
                channel  = message["channel"]
                node_name = channel.split("/")[1]
                data      = json.loads(message["data"])

                # Update the global fleet snapshot with fresh sensor data
                fleet_state[node_name] = {**data, "last_seen": time.time()}

                # Push the updated state to all connected dashboard browsers
                broadcast_to_sse()

        except Exception as e:
            print(f"⚠️  GLOBAL DASHBOARD: Redis error: {e}. Retrying in 3s...", flush=True)
            time.sleep(3)


def broadcast_to_sse():
    """Sends the current fleet snapshot and migration log to all SSE clients."""
    payload = json.dumps({
        "fleet":         fleet_state,
        "migration_log": migration_log[-20:]  # Last 20 events for the timeline
    })
    event_str = f"data: {payload}\n\n"

    with sse_lock:
        dead = []
        for client in sse_clients:
            try:
                client.wfile.write(event_str.encode())
                client.wfile.flush()
            except Exception:
                dead.append(client)    # Browser disconnected
        for d in dead:
            sse_clients.remove(d)


# =============================================================================
# HTTP + SSE SERVER
# =============================================================================

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Space Cloud V6 — Global Swarm Dashboard</title>
<style>
  :root {
    --bg: #090d1a; --panel: #111827; --card: #1a2236;
    --fire: #ff6b35; --pred: #00d4ff; --green: #00ff88;
    --warn: #ffcc00; --dim: #64748b; --border: #1e3a5f;
    --text: #e2e8f0;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: 'Courier New', monospace; padding: 20px; }

  header {
    background: var(--panel); border: 1px solid var(--border);
    border-radius: 8px; padding: 14px 22px; margin-bottom: 20px;
    display: flex; align-items: center; justify-content: space-between;
  }
  .title { color: var(--pred); font-size: 1.1rem; letter-spacing: 2px; font-weight: bold; }

  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
  @media (max-width: 800px) { .grid { grid-template-columns: 1fr; } }

  .panel { background: var(--panel); border: 1px solid var(--border); border-radius: 8px; padding: 16px; }
  .panel-title { font-size: 0.72rem; text-transform: uppercase; letter-spacing: 2px; color: var(--dim); margin-bottom: 14px; }

  /* Orbital topology view — circular canvas */
  #orbit-canvas { display: block; margin: 0 auto; }

  /* Fleet table */
  table { width: 100%; border-collapse: collapse; font-size: 0.8rem; }
  th { color: var(--dim); text-align: left; padding: 6px 4px; text-transform: uppercase;
       font-size: 0.68rem; letter-spacing: 1px; border-bottom: 1px solid var(--border); }
  td { padding: 8px 4px; border-bottom: 1px solid rgba(30,58,95,0.4); }
  .ok   { color: var(--green); }
  .warm { color: var(--warn);  }
  .hot  { color: var(--fire);  }

  /* Migration timeline */
  #timeline { font-size: 0.72rem; color: var(--dim); max-height: 200px; overflow-y: auto; line-height: 1.8; }
  .mig-thermal { color: var(--warn); }
  .mig-lateral { color: var(--pred); }
  .mig-land    { color: var(--green); }

  /* Connection dot */
  .dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; margin-right: 6px; }
  .dot-on  { background: var(--green); box-shadow: 0 0 6px var(--green); }
  .dot-off { background: var(--dim); }
</style>
</head>
<body>

<header>
  <div class="title">🌍 SPACE CLOUD V6 — GLOBAL SWARM DASHBOARD</div>
  <div id="conn-status" style="font-size:0.75rem; color:var(--dim)">Connecting...</div>
</header>

<div class="grid">

  <!-- Orbital topology canvas -->
  <div class="panel">
    <div class="panel-title">🛰️ Orbital Topology (Schematic)</div>
    <!--
      Each satellite is drawn as a dot on a circle proportional to its orbital angle.
      Color: green = nominal, yellow = warm, red = overheating, pulsing = active payload.
    -->
    <canvas id="orbit-canvas" width="300" height="300"></canvas>
  </div>

  <!-- Fleet hardware table -->
  <div class="panel">
    <div class="panel-title">📡 Constellation Hardware Status</div>
    <table>
      <thead>
        <tr><th>Node</th><th>Plane</th><th>Angle</th><th>Temp</th><th>Battery</th><th>Eclipse</th><th>Payload</th></tr>
      </thead>
      <tbody id="fleet-tbody"></tbody>
    </table>
  </div>

  <!-- Migration timeline (full width) -->
  <div class="panel" style="grid-column: 1 / -1;">
    <div class="panel-title">📋 Migration Event Timeline</div>
    <div id="timeline">Waiting for satellite telemetry...</div>
  </div>

</div>

<script>
// ============================================================================
// ORBITAL CANVAS RENDERER
// Draws each satellite as a colored dot on a circle at its orbital angle.
// ============================================================================
const canvas = document.getElementById('orbit-canvas');
const ctx    = canvas.getContext('2d');
const CX     = 150, CY = 150, R = 110;  // Centre and orbital radius

function drawOrbitalView(fleet) {
  ctx.clearRect(0, 0, canvas.width, canvas.height);

  // Draw Earth at centre
  ctx.beginPath();
  ctx.arc(CX, CY, 20, 0, Math.PI * 2);
  ctx.fillStyle = '#1a4a8a';
  ctx.fill();
  ctx.strokeStyle = '#2a6abc';
  ctx.lineWidth = 1;
  ctx.stroke();
  ctx.fillStyle = '#e2e8f0';
  ctx.font = '10px Courier New';
  ctx.textAlign = 'center';
  ctx.fillText('🌍', CX, CY + 4);

  // Draw orbital ring
  ctx.beginPath();
  ctx.arc(CX, CY, R, 0, Math.PI * 2);
  ctx.strokeStyle = 'rgba(30,58,95,0.6)';
  ctx.lineWidth = 1;
  ctx.stroke();

  // Draw each satellite on the ring
  Object.entries(fleet).forEach(([name, data]) => {
    if (data.type === 'ground') return;

    const angleDeg = data.angle || 0;
    const angleRad = (angleDeg - 90) * Math.PI / 180;  // -90° so 0° is at top
    const sx = CX + R * Math.cos(angleRad);
    const sy = CY + R * Math.sin(angleRad);

    // Pick satellite color by temperature
    let color = '#00ff88';         // Nominal (green)
    if (data.temp > 80) color = '#ff6b35';      // Hot (orange)
    else if (data.temp > 60) color = '#ffcc00'; // Warm (yellow)

    // Draw active payload with a pulsing outer ring
    if (data.is_working) {
      ctx.beginPath();
      ctx.arc(sx, sy, 9, 0, Math.PI * 2);
      ctx.strokeStyle = color;
      ctx.lineWidth = 1.5;
      ctx.globalAlpha = 0.4;
      ctx.stroke();
      ctx.globalAlpha = 1;
    }

    // Draw the satellite dot
    ctx.beginPath();
    ctx.arc(sx, sy, 5, 0, Math.PI * 2);
    ctx.fillStyle = data.eclipse ? '#3a4a6a' : color;
    ctx.fill();

    // Label
    const shortName = name.replace('minikube-', '');
    ctx.fillStyle = '#e2e8f0';
    ctx.font = '8px Courier New';
    ctx.textAlign = 'center';
    ctx.fillText(shortName, sx, sy - 10);
  });
}

// ============================================================================
// FLEET TABLE RENDERER
// ============================================================================
function renderFleetTable(fleet) {
  const tbody = document.getElementById('fleet-tbody');
  tbody.innerHTML = '';

  Object.entries(fleet).forEach(([name, data]) => {
    if (data.type === 'ground') return;

    const temp     = data.temp || 0;
    let tempClass  = 'ok';
    if (temp > 80) tempClass = 'hot';
    else if (temp > 60) tempClass = 'warm';

    const shortName = name.replace('minikube-', '');
    const eclipse   = data.eclipse ? '🌑' : '☀️';
    const payload   = data.is_working ? '<span class="dot dot-on"></span>ACTIVE' : '<span class="dot dot-off"></span>—';

    tbody.innerHTML += `<tr>
      <td>${shortName}</td>
      <td>${data.orbit_plane || '—'}</td>
      <td>${data.angle || 0}°</td>
      <td class="${tempClass}">${temp}°C</td>
      <td>${data.battery || 0}%</td>
      <td>${eclipse}</td>
      <td>${payload}</td>
    </tr>`;
  });
}

// ============================================================================
// SSE CONNECTION
// ============================================================================
const evtSource = new EventSource('/stream');
const connStatus = document.getElementById('conn-status');

evtSource.onopen    = () => { connStatus.textContent = '● LIVE'; connStatus.style.color = '#00ff88'; };
evtSource.onerror   = () => { connStatus.textContent = '⏳ Reconnecting...'; connStatus.style.color = '#ffcc00'; };

evtSource.onmessage = (event) => {
  const data     = JSON.parse(event.data);
  const fleet    = data.fleet || {};
  const migLog   = data.migration_log || [];

  drawOrbitalView(fleet);
  renderFleetTable(fleet);

  // Render migration timeline
  if (migLog.length > 0) {
    const timeline = document.getElementById('timeline');
    timeline.innerHTML = migLog.reverse().map(e => {
      const ts  = new Date(e.ts * 1000).toLocaleTimeString();
      const cls = e.type === 'lateral' ? 'mig-lateral' : e.type === 'land' ? 'mig-land' : 'mig-thermal';
      return `<div class="${cls}">[${ts}] ${e.message}</div>`;
    }).join('');
  }
};
</script>
</body>
</html>
"""


class DashboardHandler(BaseHTTPRequestHandler):
    """
    Simple HTTP handler serving the dashboard HTML and the SSE stream.
    GET /         → Returns the dashboard HTML page
    GET /stream   → Server-Sent Events stream for real-time telemetry updates
    """

    def do_GET(self):
        if self.path == "/stream":
            # Set up the SSE connection headers (persistent keep-alive stream)
            self.send_response(200)
            self.send_header("Content-Type",  "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection",    "keep-alive")
            self.end_headers()

            # Register this browser as an SSE client
            with sse_lock:
                sse_clients.append(self)

            # Send the current state immediately on connect
            try:
                payload = json.dumps({"fleet": fleet_state, "migration_log": migration_log[-20:]})
                self.wfile.write(f"data: {payload}\n\n".encode())
                self.wfile.flush()
            except Exception:
                pass

            # Block here — the thread stays alive as long as the browser is connected
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
            # Serve the main HTML dashboard page
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(DASHBOARD_HTML.encode())

    def log_message(self, format, *args):
        # Suppress HTTP log spam — we handle our own logging
        pass


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    print(f"🌍 GLOBAL SWARM DASHBOARD starting on http://localhost:{DASHBOARD_PORT}", flush=True)

    # Start the Redis telemetry listener in a background daemon thread
    listener = threading.Thread(target=redis_listener, daemon=True)
    listener.start()

    # Start the HTTP + SSE server (blocking)
    server = HTTPServer(("0.0.0.0", DASHBOARD_PORT), DashboardHandler)
    print(f"✅ Dashboard ready at http://localhost:{DASHBOARD_PORT}", flush=True)
    server.serve_forever()
