import time
import logging
from kubernetes import client, config, watch

# =============================================================================
# CONFIGURAZIONE WATCHDOG DISTRIBUTO (V3)
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [SPACE-FDIR] - %(message)s',
    datefmt='%H:%M:%S'
)

# Soglie Energetiche
THRESH_EVICT = 20    # Sotto questa soglia, il satellite scarica il barile (Sfratto)
LABEL_BATTERY = "spacecloud.io/battery_level"

# Carichi da Monitorare e Migrare
# 'label': l'etichetta usata nel deployment/statefulset
MONITORED_APPS = [
    {"name": "Dashboard", "label": "app=space-app"},  # La tua UI
    {"name": "Redis",     "label": "app=redis"}       # Il Database Distribuito
]

class SpaceWatchdogDistributed:
    def __init__(self):
        try:
            config.load_kube_config()
            self.v1 = client.CoreV1Api()
            logging.info("✅ FDIR Watchdog Connesso al Cluster.")
        except Exception as e:
            logging.error(f"❌ Errore connessione K8s: {e}")
            exit(1)

    def get_node_battery(self, node_name):
        """Legge la batteria di uno specifico satellite."""
        try:
            node = self.v1.read_node(node_name)
            # Verifica salute hardware
            conditions = node.status.conditions
            for cond in conditions:
                if cond.type == "Ready" and cond.status != "True":
                    return 0 # Nodo guasto = Batteria 0 virtuale

            # Lettura etichetta
            labels = node.metadata.labels
            if labels and LABEL_BATTERY in labels:
                return int(labels[LABEL_BATTERY])
        except Exception as e:
            logging.warning(f"Impossibile leggere nodo {node_name}: {e}")
        return 0

    def evict_pod(self, pod_name, namespace, node_name, reason):
        """
        Sfratta il pod. Questo innesca la MIGRAZIONE.
        Il Pod muore -> K8s ne crea uno nuovo -> SpaceScheduler sceglie il nodo migliore.
        """
        try:
            logging.warning(f"🚀 MIGRAZIONE AVVIATA: {pod_name} abbandona {node_name} ({reason})")
            self.v1.delete_namespaced_pod(pod_name, namespace, grace_period_seconds=0)
        except Exception as e:
            logging.error(f"Errore durante lo sfratto di {pod_name}: {e}")

    def run(self):
        logging.info("🛰️  Monitoraggio Flotta Attivo. In attesa di anomalie...")
        
        while True:
            try:
                # Ciclo su tutte le app critiche (Dashboard e Redis)
                for app in MONITORED_APPS:
                    # Troviamo tutti i pod di quel tipo
                    pods = self.v1.list_namespaced_pod("default", label_selector=app['label'])
                    
                    for pod in pods.items:
                        # Ignoriamo pod che stanno già morendo o non sono schedulati
                        if pod.metadata.deletion_timestamp or pod.status.phase == "Pending":
                            continue
                            
                        pod_name = pod.metadata.name
                        node_name = pod.spec.node_name
                        
                        if not node_name: continue

                        # 1. Controllo Batteria del Nodo Ospitante
                        battery = self.get_node_battery(node_name)
                        
                        # LOGICA DI MIGRAZIONE
                        if battery < THRESH_EVICT:
                            logging.warning(f"⚠️  ALLARME NODO {node_name}: Batteria Critica ({battery}%)")
                            self.evict_pod(pod_name, "default", node_name, f"Low Battery < {THRESH_EVICT}%")
                        else:
                            # Log di debug opzionale (verboso)
                            # logging.info(f"   OK: {pod_name} su {node_name} (Batt: {battery}%)")
                            pass

                time.sleep(2) # Controllo rapido ogni 2 secondi

            except KeyboardInterrupt:
                logging.info("🛑 Watchdog terminato.")
                break
            except Exception as e:
                logging.error(f"Errore ciclo Watchdog: {e}")
                time.sleep(2)

if __name__ == "__main__":
    w = SpaceWatchdogDistributed()
    w.run()