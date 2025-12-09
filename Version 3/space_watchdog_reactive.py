import time
import logging
import sys
import os
import datetime
from kubernetes import client, config, watch

# =============================================================================
# CONFIGURAZIONE WATCHDOG DISTRIBUTO (V3) - UI ENHANCED
# =============================================================================

# Soglie Energetiche
THRESH_EVICT = 20    # Sotto questa soglia, il satellite scarica il barile (Sfratto)
LABEL_BATTERY = "spacecloud.io/battery_level"

# Carichi da Monitorare e Migrare
MONITORED_APPS = [
    {"name": "Dashboard", "label": "app=space-app"},  # La tua UI
    {"name": "Redis",     "label": "app=redis"}       # Il Database Distribuito
]

# --- UTILS GRAFICHE ---
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

def get_timestamp():
    return datetime.datetime.now().strftime("%H:%M:%S")

class SpaceWatchdogDistributed:
    def __init__(self):
        self.os_clear_cmd = 'cls' if os.name == 'nt' else 'clear'
        try:
            config.load_kube_config()
            self.v1 = client.CoreV1Api()
            self.print_header()
            self.log_event("SYSTEM", "FDIR Watchdog Connected to Cluster", "OK")
        except Exception as e:
            print(f"{Colors.FAIL}❌ Errore connessione K8s: {e}{Colors.ENDC}")
            exit(1)

    def print_header(self):
        os.system(self.os_clear_cmd)
        print(f"{Colors.HEADER}╔════════════════════════════════════════════════════════════════════╗{Colors.ENDC}")
        print(f"{Colors.HEADER}║      🛡️   SPACE CLOUD FDIR SYSTEM (Fault Detection & Recovery)    ║{Colors.ENDC}")
        print(f"{Colors.HEADER}╚════════════════════════════════════════════════════════════════════╝{Colors.ENDC}")
        print(f"{Colors.BOLD}{'TIMESTAMP':<10} {'EVENT TYPE':<15} {'TARGET NODE':<20} {'DETAILS':<20}{Colors.ENDC}")
        print(f"{Colors.BLUE}{'-'*70}{Colors.ENDC}")

    def log_event(self, event_type, details, status="INFO", node="---"):
        """Stampa una riga di log formattata che rimane nello storico"""
        
        # Puliamo la riga corrente (dove c'è lo spinner)
        sys.stdout.write("\r" + " " * 80 + "\r")
        
        timestamp = get_timestamp()
        
        color = Colors.GREEN
        icon = "ℹ️ "
        
        if status == "WARNING":
            color = Colors.WARNING
            icon = "⚠️ "
        elif status == "CRITICAL":
            color = Colors.FAIL
            icon = "🚀 " # Icona razzo per l'eviction (migrazione)
        elif status == "ERROR":
            color = Colors.FAIL
            icon = "❌ "

        # Formattazione colonne
        type_str = f"{color}{icon}{event_type}{Colors.ENDC}"
        print(f"{timestamp:<10} {type_str:<25} {node:<20} {details}")

    def print_heartbeat(self, count):
        """Stampa una riga temporanea che sovrascrive se stessa"""
        spinner = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
        idx = count % len(spinner)
        sys.stdout.write(f"\r{Colors.CYAN}{spinner[idx]} Scanning Constellation Health... [{count} cycles]{Colors.ENDC}")
        sys.stdout.flush()

    def get_node_battery(self, node_name):
        """Legge la batteria di uno specifico satellite."""
        try:
            node = self.v1.read_node(node_name)
            # Verifica salute hardware
            conditions = node.status.conditions
            for cond in conditions:
                if cond.type == "Ready" and cond.status != "True":
                    return 0 # Nodo guasto

            # Lettura etichetta
            labels = node.metadata.labels
            if labels and LABEL_BATTERY in labels:
                return int(labels[LABEL_BATTERY])
        except Exception as e:
            # Non logghiamo errori di lettura singoli per non sporcare la UI
            pass
        return 0

    def evict_pod(self, pod_name, namespace, node_name, reason):
        """
        Sfratta il pod. Questo innesca la MIGRAZIONE.
        """
        try:
            self.log_event("EVICTION", f"Pod: {pod_name}", "CRITICAL", node_name)
            self.log_event("REASON", reason, "WARNING", node_name)
            
            self.v1.delete_namespaced_pod(pod_name, namespace, grace_period_seconds=0)
            
            self.log_event("ACTION", "Migration Sequence Initiated", "INFO", node_name)
            
        except Exception as e:
            self.log_event("ERROR", str(e), "ERROR", node_name)

    def run(self):
        cycle_count = 0
        while True:
            try:
                # Cuore pulsante grafico
                self.print_heartbeat(cycle_count)
                cycle_count += 1
                
                # Ciclo su tutte le app critiche
                for app in MONITORED_APPS:
                    pods = self.v1.list_namespaced_pod("default", label_selector=app['label'])
                    
                    for pod in pods.items:
                        if pod.metadata.deletion_timestamp or pod.status.phase == "Pending":
                            continue
                            
                        pod_name = pod.metadata.name
                        node_name = pod.spec.node_name
                        
                        if not node_name: continue

                        # 1. Controllo Batteria
                        battery = self.get_node_battery(node_name)
                        
                        # LOGICA DI MIGRAZIONE
                        if battery < THRESH_EVICT:
                            # Trovata anomalia! Il log_event cancellerà la riga di heartbeat per scrivere questo
                            self.evict_pod(pod_name, "default", node_name, f"Battery Critical: {battery}%")
                        else:
                            pass 

                time.sleep(2) 

            except KeyboardInterrupt:
                print(f"\n{Colors.WARNING}🛑 Watchdog terminato manualmente.{Colors.ENDC}")
                break
            except Exception as e:
                self.log_event("CRASH", str(e), "ERROR")
                time.sleep(2)

if __name__ == "__main__":
    w = SpaceWatchdogDistributed()
    w.run()