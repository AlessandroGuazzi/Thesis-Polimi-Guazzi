"""
===============================================================================
SPACE CLOUD V6 - GLOBAL SWARM DASHBOARD (Ground Station — Stateless)
===============================================================================

🧠 HIGH-LEVEL PURPOSE:
This script implements a **real-time monitoring dashboard** for the entire
satellite swarm.

It provides a "God’s Eye View" of:
- Satellite hardware state (temperature, battery, orbit, etc.)
- Active workloads (which node is computing)
- Migration events timeline

-------------------------------------------------------------------------------
🏗️ CORE ARCHITECTURE:

1. DATA SOURCE (Redis Pub/Sub)
   - Subscribes to: telemetry/*
   - Receives real-time updates from environment_sim.py

2. STATE MANAGEMENT (IN-MEMORY)
   - fleet_state → latest snapshot of all satellites
   - migration_log → recent system events

3. DELIVERY (Server-Sent Events - SSE)
   - Browser connects to /stream
   - Server pushes updates in real-time (no polling)

4. FRONTEND (Embedded HTML + JS)
   - Canvas visualization (orbital positions)
   - Table view (hardware status)
   - Timeline (migration events)

-------------------------------------------------------------------------------
⚠️ STATELESS DESIGN (VERY IMPORTANT):
- This service holds NO critical system state
- If it crashes → system keeps running normally
- On restart → it simply reconnects to Redis and rebuilds state

-------------------------------------------------------------------------------
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

# Redis connection settings (Ground Station Redis)
GROUND_REDIS_HOST = os.getenv("GROUND_REDIS_HOST", "localhost")
GROUND_REDIS_PORT = 6379

# HTTP server port
DASHBOARD_PORT = 8090


# -----------------------------------------------------------------------------
# GLOBAL IN-MEMORY STATE (SHARED ACROSS THREADS)
# -----------------------------------------------------------------------------

# Stores latest telemetry per satellite:
# {
#   "minikube-m02": {battery: ..., temp: ..., ...},
#   ...
# }
fleet_state = {}

# Stores migration events (latest last)
migration_log = []

# List of connected SSE clients (HTTP handlers)
sse_clients = []

# Lock to protect concurrent access to sse_clients
sse_lock = threading.Lock()


# =============================================================================
# REDIS TELEMETRY LISTENER
# =============================================================================

def redis_listener():
    """
    🧠 PURPOSE:
    Background thread that listens to Redis Pub/Sub telemetry channels.

    RESPONSIBILITIES:
    - Subscribe to telemetry/*
    - Parse incoming messages
    - Update fleet_state
    - Trigger broadcast to all connected browsers

    RESILIENCE:
    - Automatically reconnects if Redis connection fails
    """

    global fleet_state

    while True:
        try:
            # Create Redis connection
            r = redis.Redis(
                host=GROUND_REDIS_HOST,
                port=GROUND_REDIS_PORT,
                decode_responses=True,  # ⚠️ ensures strings instead of bytes
                socket_connect_timeout=3
            )

            r.ping()  # Force connection check

            pubsub = r.pubsub()

            # Subscribe to ALL telemetry channels
            pubsub.psubscribe("telemetry/*")

            print(f"✅ GLOBAL DASHBOARD: Subscribed to telemetry/* on Ground Redis.", flush=True)

            # Listen to incoming messages indefinitely
            for message in pubsub.listen():

                # Ignore non-data messages (like subscription confirmations)
                if message["type"] != "pmessage":
                    continue

                # Example channel: "telemetry/minikube-m02"
                channel = message["channel"]

                # Extract node name from channel string
                node_name = channel.split("/")[1]

                # Parse JSON payload into Python dict
                data = json.loads(message["data"])

                # Update global state
                # ⚠️ dictionary merge (**data) + timestamp injection
                fleet_state[node_name] = {
                    **data,
                    "last_seen": time.time()  # track freshness
                }

                # Push update to all connected clients
                broadcast_to_sse()

        except Exception as e:
            print(f"⚠️  GLOBAL DASHBOARD: Redis error: {e}. Retrying in 3s...", flush=True)
            time.sleep(3)


def broadcast_to_sse():
    """
    🧠 PURPOSE:
    Sends the latest system snapshot to all connected browsers.

    MECHANISM:
    - Uses Server-Sent Events (SSE)
    - Sends JSON payload as text stream

    ALSO HANDLES:
    - Removing disconnected clients
    """

    # Prepare JSON payload
    payload = json.dumps({
        "fleet": fleet_state,
        "migration_log": migration_log[-20:]  # limit size
    })

    # SSE format requires "data: ...\n\n"
    event_str = f"data: {payload}\n\n"

    with sse_lock:

        dead = []  # track disconnected clients

        for client in sse_clients:
            try:
                # Write data to HTTP stream
                client.wfile.write(event_str.encode())

                # Flush buffer immediately (real-time)
                client.wfile.flush()

            except Exception:
                dead.append(client)  # client disconnected

        # Cleanup dead clients
        for d in dead:
            sse_clients.remove(d)


# =============================================================================
# HTTP + SSE SERVER
# =============================================================================

DASHBOARD_HTML = """... (UNCHANGED HTML CONTENT) ..."""


class DashboardHandler(BaseHTTPRequestHandler):
    """
    🧠 PURPOSE:
    Handles HTTP requests for:
    - Dashboard page (/)
    - SSE stream (/stream)

    Each connected browser creates one handler instance.
    """

    def do_GET(self):
        """
        🧠 Handles incoming GET requests.

        ROUTES:
        - /stream → SSE connection
        - /       → HTML dashboard
        """

        if self.path == "/stream":

            # -----------------------------------------------------------------
            # SSE CONNECTION SETUP
            # -----------------------------------------------------------------

            self.send_response(200)

            # ⚠️ Required headers for SSE protocol
            self.send_header("Content-Type",  "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection",    "keep-alive")

            self.end_headers()

            # Register client
            with sse_lock:
                sse_clients.append(self)

            # Send initial snapshot immediately
            try:
                payload = json.dumps({
                    "fleet": fleet_state,
                    "migration_log": migration_log[-20:]
                })

                self.wfile.write(f"data: {payload}\n\n".encode())
                self.wfile.flush()

            except Exception:
                pass

            # -----------------------------------------------------------------
            # KEEP CONNECTION OPEN (CRITICAL FOR SSE)
            # -----------------------------------------------------------------
            try:
                while True:
                    time.sleep(1)  # keep thread alive

            except Exception:
                pass

            finally:
                # Remove client on disconnect
                with sse_lock:
                    if self in sse_clients:
                        sse_clients.remove(self)

        else:
            # -----------------------------------------------------------------
            # SERVE DASHBOARD HTML
            # -----------------------------------------------------------------

            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()

            self.wfile.write(DASHBOARD_HTML.encode())

    def log_message(self, format, *args):
        """
        🧠 OVERRIDE:
        Disable default HTTP logging (reduces noise).
        """
        pass


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    """
    🧠 BOOT SEQUENCE:

    1. Start Redis listener thread
    2. Start HTTP server (blocking)
    """

    print(f"🌍 GLOBAL SWARM DASHBOARD starting on http://localhost:{DASHBOARD_PORT}", flush=True)

    # Start background Redis listener
    listener = threading.Thread(
        target=redis_listener,
        daemon=True  # ⚠️ auto-stops with main process
    )
    listener.start()

    # Start HTTP server (blocks forever)
    server = HTTPServer(
        ("0.0.0.0", DASHBOARD_PORT),  # listen on all interfaces
        DashboardHandler
    )

    print(f"✅ Dashboard ready at http://localhost:{DASHBOARD_PORT}", flush=True)

    server.serve_forever()  # ⚠️ blocking call