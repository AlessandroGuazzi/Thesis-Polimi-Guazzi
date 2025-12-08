import time
import logging
from kubernetes import client, config

# =============================================================================
# CONFIGURAZIONE WATCHDOG REATTIVO (BMS - Battery Management System)
# =============================================================================

# Configurazione Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [SPACE-BMS] - %(message)s',
    datefmt='%H:%M:%S'
)

# SOGLIE ENERGETICHE (Isteresi)
# Sotto il 20%: Entra in Safe Mode (Spegne carichi, mantiene API Server)
# Sopra il 40%: Esce da Safe Mode (Ripristina carichi)
BATTERY_CRITICAL_THRESHOLD = 20.0  
BATTERY_RECOVERY_THRESHOLD = 40.0  

# ETICHETTA DA MONITORARE
LABEL_ENERGY = "satellite.energy"

# CARICHI SACRIFICABILI (Payload Utente)
# Questi servizi verranno spenti (replicas=0) in Safe Mode per salvare il Master.
SACRIFICIAL_WORKLOADS = [
    {"ns": "default", "name": "redis-leader", "replicas": 1},
    {"ns": "default", "name": "redis-follower", "replicas": 2},
    # Nota: Assicurati che i nomi dei deployment della dashboard siano corretti per la tua installazione
    {"ns": "kubernetes-dashboard", "name": "kubernetes-dashboard", "replicas": 1},
    {"ns": "kubernetes-dashboard", "name": "dashboard-metrics-scraper", "replicas": 1}
]

class SpaceWatchdogReactive:
    def __init__(self):
        # Connessione al Cluster (simula il bus dati interno del satellite)
        try:
            config.load_kube_config()
            self.v1 = client.CoreV1Api()
            self.apps_v1 = client.AppsV1Api()
            logging.info("✅ BMS Connesso al Control Plane del Satellite.")
        except Exception as e:
            logging.error(f"❌ Errore critico di connessione K8s: {e}")
            exit(1)

        self.safe_mode_active = False
        self.master_node_name = None

    def get_master_node_name(self):
        """Trova il nome del nodo master/control-plane."""
        try:
            # Cerca nodi con label control-plane (standard k8s)
            nodes = self.v1.list_node(label_selector="node-role.kubernetes.io/control-plane")
            if not nodes.items:
                # Fallback per Minikube (spesso non ha la label standard su versioni vecchie)
                nodes = self.v1.list_node()
                # Prendiamo il primo nodo (in minikube è unico)
            
            if nodes.items:
                return nodes.items[0].metadata.name
        except Exception as e:
            logging.error(f"Errore ricerca nodo master: {e}")
        return None

    def get_battery_level(self):
        """Legge il livello di batteria dall'etichetta del Nodo Master."""
        if not self.master_node_name:
            self.master_node_name = self.get_master_node_name()
            if not self.master_node_name:
                return 100.0 # Fallback safe

        try:
            node = self.v1.read_node(self.master_node_name)
            battery_str = node.metadata.labels.get(LABEL_ENERGY, "100")
            return float(battery_str)
        except Exception as e:
            logging.warning(f"Impossibile leggere telemetria da {self.master_node_name}: {e}")
            return 100.0

    def enter_safe_mode(self):
        """
        SOFT-KILL: Attiva la modalità sopravvivenza.
        Obiettivo: Ridurre il consumo CPU/RAM a zero, mantenendo vivo l'API Server.
        """
        if self.safe_mode_active:
            return

        logging.warning(f"⚠️  BATTERIA CRITICA (<{BATTERY_CRITICAL_THRESHOLD}%)! ATTIVAZIONE SAFE MODE.")
        logging.warning("   -> Priorità: Salvare il Control Plane. Spegnimento Payload...")
        
        # 1. Cordoning del Nodo (Impedisce nuovi pod)
        self._set_unschedulable(True)

        # 2. Scaling a 0 dei deployment (Ibernazione)
        for workload in SACRIFICIAL_WORKLOADS:
            self._scale_deployment(workload['ns'], workload['name'], 0)

        self.safe_mode_active = True
        logging.info("🛡️  Safe Mode Attiva. Payload Ibernati.")

    def exit_safe_mode(self):
        """
        RIPRISTINO: La batteria è sufficiente per riprendere le operazioni.
        """
        if not self.safe_mode_active:
            return

        logging.info(f"🔋 BATTERIA RECUPERATA (>{BATTERY_RECOVERY_THRESHOLD}%). RIPRISTINO OPERAZIONI.")

        # 1. Uncordoning del Nodo
        self._set_unschedulable(False)

        # 2. Ripristino dei deployment
        for workload in SACRIFICIAL_WORKLOADS:
            self._scale_deployment(workload['ns'], workload['name'], workload['replicas'])

        self.safe_mode_active = False
        logging.info("✅ Operazioni Nominali Ripristinate.")

    def _scale_deployment(self, namespace, name, replicas):
        """Helper per scalare i deployment."""
        try:
            # Verifica esistenza deployment per evitare errori 404 nei log
            self.apps_v1.read_namespaced_deployment(name, namespace)
            
            patch = {"spec": {"replicas": replicas}}
            self.apps_v1.patch_namespaced_deployment(name, namespace, patch)
            logging.info(f"   -> [SCALING] {namespace}/{name} impostato a {replicas} repliche.")
        except client.exceptions.ApiException as e:
            if e.status == 404:
                logging.debug(f"   -> Deployment {name} non trovato. Ignorato.")
            else:
                logging.error(f"   -> Errore scaling {name}: {e}")

    def _set_unschedulable(self, unschedulable: bool):
        """Helper per Cordon/Uncordon del nodo Master."""
        if not self.master_node_name: return

        try:
            body = {"spec": {"unschedulable": unschedulable}}
            self.v1.patch_node(self.master_node_name, body)
            state = "ISOLATO (Cordoned)" if unschedulable else "ATTIVO (Uncordoned)"
            logging.info(f"   -> [NODO] {self.master_node_name} è ora {state}.")
        except Exception as e:
            logging.error(f"Failed to patch node: {e}")

    def run(self):
        logging.info("🛰️  Space BMS Online. Monitoraggio EPS avviato...")
        
        # Identifica subito il nodo
        self.master_node_name = self.get_master_node_name()
        logging.info(f"   -> Nodo Master identificato: {self.master_node_name}")

        while True:
            try:
                battery = self.get_battery_level()
                
                # Logica di controllo Isteresi
                if battery < BATTERY_CRITICAL_THRESHOLD:
                    self.enter_safe_mode()
                elif battery > BATTERY_RECOVERY_THRESHOLD:
                    self.exit_safe_mode()
                
                # Telemetry heartbeat (meno frequente per non intasare i log)
                status_icon = "🟢" if not self.safe_mode_active else "🔴"
                mode_text = "NOMINALE" if not self.safe_mode_active else "SAFE MODE"
                # Stampa lo stato ogni ciclo (o riduci la frequenza se vuoi)
                logging.info(f"Telemetria: Batteria={battery:.1f}% | Stato={status_icon} {mode_text}")
                
                time.sleep(5) # Controllo ogni 5 secondi

            except KeyboardInterrupt:
                logging.info("Spegnimento Watchdog richiesto.")
                break
            except Exception as e:
                logging.error(f"Errore nel main loop: {e}")
                time.sleep(5)

if __name__ == "__main__":
    bms = SpaceWatchdogReactive()
    bms.run()