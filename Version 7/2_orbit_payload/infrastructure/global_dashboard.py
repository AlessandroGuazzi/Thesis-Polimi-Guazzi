import json
import threading
import os
import time
import redis
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

# =============================================================================
# CONFIGURATION
# =============================================================================
GROUND_REDIS_HOST = os.getenv("GROUND_REDIS_HOST", "localhost")
GROUND_REDIS_PORT = 6379
DASHBOARD_PORT    = 8090

fleet_state = {}
migration_log = []
sse_clients = []
sse_lock = threading.Lock()

# =============================================================================
# HELPER: READ HTML FILE
# =============================================================================
def get_html():
    filepath = os.path.join(os.path.dirname(__file__), "global_dashboard.html")
    with open(filepath, "r", encoding="utf-8") as f:
        return f.read()

# =============================================================================
# REDIS TELEMETRY LISTENER
# =============================================================================
def redis_listener():
    global fleet_state
    while True:
        try:
            r = redis.Redis(host=GROUND_REDIS_HOST, port=GROUND_REDIS_PORT, decode_responses=True, socket_connect_timeout=3)
            r.ping()
            pubsub = r.pubsub()
            pubsub.psubscribe("telemetry/*")
            print(f"✅ GLOBAL DASHBOARD: Subscribed to telemetry/* on Ground Redis.", flush=True)

            for message in pubsub.listen():
                if message["type"] != "pmessage": continue
                channel  = message["channel"]
                node_name = channel.split("/")[1]
                data      = json.loads(message["data"])
                fleet_state[node_name] = {**data, "last_seen": time.time()}
                broadcast_to_sse()
        except Exception as e:
            print(f"⚠️  GLOBAL DASHBOARD: Redis error: {e}. Retrying in 3s...", flush=True)
            time.sleep(3)

def broadcast_to_sse():
    payload = json.dumps({"fleet": fleet_state, "migration_log": migration_log[-20:]})
    event_str = f"data: {payload}\n\n"
    with sse_lock:
        dead = []
        for client in sse_clients:
            try:
                client.wfile.write(event_str.encode())
                client.wfile.flush()
            except Exception: dead.append(client)
        for d in dead: sse_clients.remove(d)

# =============================================================================
# HTTP + SSE SERVER
# =============================================================================
class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/stream":
            self.send_response(200)
            self.send_header("Content-Type",  "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection",    "keep-alive")
            self.end_headers()
            with sse_lock: sse_clients.append(self)
            try:
                payload = json.dumps({"fleet": fleet_state, "migration_log": migration_log[-20:]})
                self.wfile.write(f"data: {payload}\n\n".encode())
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

    def do_POST(self):
        """
        POST /api/override — Telemetry Injector endpoint.

        Accepts a JSON body and forwards it as a command to the Simulation
        Oracle via the 'override/commands' Redis Pub/Sub channel.

        Body shape:
          { "action": "set"|"release",
            "node":   "minikube-m02" | "minikube-m03" | "minikube-m04",
            "temp":   <float|null>,
            "battery": <float|null> }

        The backend is intentionally stateless — it only proxies the command.
        The Oracle is the single source of truth for override state.
        """
        if self.path != "/api/override":
            self.send_response(404)
            self.end_headers()
            return

        try:
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length)
            cmd  = json.loads(body)

            # Validate required fields
            if "action" not in cmd or "node" not in cmd:
                self._json_response(400, {"error": "Missing required fields: 'action' and 'node'"})
                return

            if cmd["action"] not in ("set", "release"):
                self._json_response(400, {"error": "'action' must be 'set' or 'release'"})
                return

            # Forward command to the Oracle via Ground Redis Pub/Sub
            r = redis.Redis(
                host=GROUND_REDIS_HOST,
                port=GROUND_REDIS_PORT,
                decode_responses=True,
                socket_connect_timeout=3
            )
            r.publish("override/commands", json.dumps(cmd))

            print(f"🎮 DASHBOARD: Override command forwarded → {cmd}", flush=True)
            self._json_response(200, {"status": "ok", "forwarded": cmd})

        except json.JSONDecodeError:
            self._json_response(400, {"error": "Invalid JSON body"})
        except Exception as e:
            self._json_response(500, {"error": str(e)})

    def _json_response(self, status, payload):
        """Helper: sends a JSON response with CORS header."""
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type",  "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args): pass

if __name__ == "__main__":
    print(f"🌍 GLOBAL SWARM DASHBOARD starting on http://localhost:{DASHBOARD_PORT}", flush=True)
    listener = threading.Thread(target=redis_listener, daemon=True)
    listener.start()
    # ThreadingHTTPServer: spawns a new thread per connection.
    # This is critical because the SSE /stream handler blocks indefinitely
    # (while True: sleep), which would starve POST /api/override on a
    # single-threaded HTTPServer.
    server = ThreadingHTTPServer(("0.0.0.0", DASHBOARD_PORT), DashboardHandler)
    print(f"✅ Dashboard ready at http://localhost:{DASHBOARD_PORT}", flush=True)
    server.serve_forever()