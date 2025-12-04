# Importazione delle librerie
import time
from kubernetes import client, config

# =============================================================================
# CONFIGURAZIONE OPERATIVA
# =============================================================================

# La chiave dell'etichetta (Label) per leggere il livello di batteria dai nodi.
LABEL_KEY = "spacecloud.io/battery_level"

# Soglia di sicurezza energetica (Flight Rule).
# Se la batteria scende sotto questo valore, il satellite entra in "modalità sopravvivenza"
# e deve scaricare (evict) tutti i carichi non essenziali.
MIN_BATTERY = 20  

# Frequenza di controllo (Polling Rate).
# Ogni quanti secondi il Watchdog si sveglia per controllare la salute della costellazione.
CHECK_INTERVAL = 3 

# =============================================================================
# FUNZIONI AUSILIARIE
# =============================================================================

def get_node_batteries(v1):
    """
    Scansiona tutti i nodi e costruisce una mappa aggiornata della situazione energetica.
    
    Args:
        v1: Il client API di Kubernetes.
        
    Returns:
        dict: Un dizionario { 'nome_nodo': livello_batteria_int }
              Esempio: { 'space-cloud-worker': 80, 'space-cloud-worker2': 15 }
    """
    # Scarica la lista completa dei nodi dal cluster
    nodes = v1.list_node().items
    battery_map = {}
    
    for node in nodes:
        name = node.metadata.name
        labels = node.metadata.labels
        
        # Se il nodo ha l'etichetta della batteria, salviamo il valore
        if labels and LABEL_KEY in labels:
            battery_map[name] = int(labels[LABEL_KEY])
        else:
            # Se non ha dati, assumiamo 0% per sicurezza (Worst Case Scenario)
            battery_map[name] = 0 
            
    return battery_map

# =============================================================================
# CICLO PRINCIPALE
# =============================================================================

def main():
    print("--- Space Cloud Watchdog (Descheduler) Avviato ---")
    
    # 1. Connessione al Cluster
    try:
        config.load_kube_config()
        v1 = client.CoreV1Api()
    except Exception as e:
        print(f"Errore critico di connessione: {e}")
        return

    # 2. Loop di Controllo Continuo
    while True:
        try:
            print("\n🔍 [WATCHDOG] Scansione salute della costellazione...")
            
            # PASSO A: Aggiornamento Telemetria
            # Chiamiamo la funzione helper per sapere quanta batteria ha ogni satellite ORA.
            batteries = get_node_batteries(v1)
            
            # PASSO B: Censimento Carichi di Lavoro
            # Chiediamo a Kubernetes: "Chi sta girando dove?"
            pods = v1.list_namespaced_pod("default").items
            
            # Analizziamo ogni singolo Pod trovato
            for pod in pods:
                pod_name = pod.metadata.name
                node_name = pod.spec.node_name
                
                # --- FILTRO 1: Pod non schedulati o già morti ---
                # Se node_name è None, il Pod è "Pending" (ci pensa lo Scheduler, non noi).
                # Se deletion_timestamp esiste, il Pod sta già morendo.
                if not node_name or pod.metadata.deletion_timestamp:
                    continue
                
                # --- FILTRO 2: Target Selection ---
                # Tocchiamo SOLO i Pod della nostra missione ("space-app").
                # Non vogliamo uccidere per sbaglio i Pod di sistema di Kubernetes!
                if pod.metadata.labels.get("app") != "space-app":
                    continue

                # --- CONTROLLO VITALE (Health Check) ---
                # Recuperiamo la batteria del nodo su cui gira QUESTO pod specifico
                current_battery = batteries.get(node_name, 0)
                
                print(f"   - Pod {pod_name} è su {node_name} (Batteria: {current_battery}%)")
                
                # --- LOGICA DI DECISIONE (Threshold Logic) ---
                if current_battery < MIN_BATTERY:
                    # SITUAZIONE CRITICA: Il satellite sta morendo.
                    print(f"     ⚠️ ALLARME: Batteria CRITICA (< {MIN_BATTERY}%). Eseguo EVICTION!")
                    
                    # --- AZIONE DI EVICTION (Terminazione Forzata) ---
                    try:
                        v1.delete_namespaced_pod(
                            name=pod_name,
                            namespace="default",
                            grace_period_seconds=0 # Uccisione immediata (SIGKILL), nessuna attesa.
                        )
                        print(f"     💀 Pod {pod_name} TERMINATO per sicurezza.")
                        # Nota importante per la tesi:
                        # Poiché usiamo un Deployment, Kubernetes noterà che il Pod è morto
                        # e ne creerà subito uno nuovo (Self-Healing).
                        # Il nuovo Pod sarà 'Pending' e verrà gestito dallo Scheduler,
                        # che lo metterà su un satellite CARICO.
                        print(f"     ♻️  Il Deployment ne creerà uno nuovo automaticamente.")
                        
                    except Exception as e:
                        print(f"     ❌ Errore durante l'eviction: {e}")
                else:
                    # SITUAZIONE NOMINALE: Tutto ok, lasciamo vivere il Pod.
                    print(f"     ✅ Situazione Nominale.")

            # Pausa tattica prima della prossima scansione (Polling Interval)
            time.sleep(CHECK_INTERVAL)

        except KeyboardInterrupt:
            # Uscita pulita con CTRL+C
            print("\n🛑 Watchdog fermato.")
            break
            
        except Exception as e:
            # Resistenza agli errori: se succede qualcosa di strano, aspettiamo e riproviamo
            print(f"Errore ciclo: {e}")
            time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()