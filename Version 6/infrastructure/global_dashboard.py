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

    def log_message(self, format, *args): pass

if __name__ == "__main__":
    print(f"🌍 GLOBAL SWARM DASHBOARD starting on http://localhost:{DASHBOARD_PORT}", flush=True)
    listener = threading.Thread(target=redis_listener, daemon=True)
    listener.start()
    server = HTTPServer(("0.0.0.0", DASHBOARD_PORT), DashboardHandler)
    print(f"✅ Dashboard ready at http://localhost:{DASHBOARD_PORT}", flush=True)
    server.serve_forever()