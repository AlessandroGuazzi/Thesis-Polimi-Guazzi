import time
from kubernetes import client, config, watch

# =============================================================================
# CONFIGURAZIONE OPERATIVA
# =============================================================================

# L'etichetta che contiene il livello di batteria del nodo (aggiornata da physics_sim.py)
LABEL_KEY = "spacecloud.io/battery_level"

# Soglia critica: sotto questo livello il satellite rischia di spegnersi.
MIN_BATTERY = 20

# Filtro di sicurezza: tocchiamo solo i Pod che appartengono alla nostra applicazione "space-app".
# Questo evita di uccidere per sbaglio i Pod di sistema di Kubernetes (es. DNS, Proxy).
APP_LABEL = "space-app" 

# =============================================================================
# FUNZIONI AUSILIARIE
# =============================================================================

def get_node_battery(node):
    """
    Estrae il livello di batteria dai metadati di un Nodo.
    Restituisce un intero (es. 85) o 0 se il dato manca.
    """
    labels = node.metadata.labels
    if labels and LABEL_KEY in labels:
        return int(labels[LABEL_KEY])
    return 0

def is_node_ready(node):
    """
    Verifica se il satellite è 'Ready' (Vivo e Connesso).
    Ritorna True se è sano, False se è guasto (NotReady/Unknown).
    """
    if not node.status.conditions:
        return False
        
    for condition in node.status.conditions:
        if condition.type == "Ready":
            return condition.status == "True"
            
    return False

def evict_pods_on_node(v1, node_name, reason_msg):
    """
    Questa funzione è il 'braccio armato' del Watchdog.
    Viene chiamata solo se la batteria è bassa, ma è abbastanza intelligente da
    NON fare rumore se il satellite è vuoto.
    
    Args:
        v1: Client API di Kubernetes
        node_name: Il nome del satellite in crisi
        reason_msg: Motivo dell'eviction
    """
    
    # Costruiamo un filtro per chiedere a Kubernetes SOLO i Pod su questo nodo specifico.
    # Ottimizzazione importante per ridurre il traffico di rete.
    field_selector = f"spec.nodeName={node_name}"
    
    try:
        # 1. CENSIMENTO: Chiediamo la lista dei Pod presenti sul nodo.
        all_pods = v1.list_namespaced_pod(
            namespace="default", 
            field_selector=field_selector
        ).items
        
        # 2. SELEZIONE: Filtriamo solo i Pod che ci interessano.
        # Scartiamo tutto ciò che non ha l'etichetta 'app=space-app'.
        victim_pods = [p for p in all_pods if p.metadata.labels.get("app") == APP_LABEL]

        # 3. SILENZIO TATTICO:
        # Se la lista 'victim_pods' è vuota, significa che il satellite è scarico ma
        # non sta facendo nulla di importante. Non c'è emergenza operativa.
        # Usciamo subito (return) senza stampare nulla per non intasare i log.
        if not victim_pods:
            return 

        # 4. AZIONE:
        # Se siamo arrivati qui, significa che ci sono Pod da salvare.
        # Solo ORA stampiamo l'allarme visibile all'operatore.
        print(f"\n🚨 {reason_msg} su {node_name}!")
        print(f"     🔎 Trovati {len(victim_pods)} pod critici da evacuare.")

        # Ciclo di terminazione (Eviction)
        for pod in victim_pods:
            print(f"     💀 TERMINO Pod {pod.metadata.name} (Forced Reschedule)...")
            
            # Eseguiamo la cancellazione immediata (grace_period=0).
            # Non diamo tempo al Pod di spegnersi con calma (es. salvare dati),
            # perché la priorità è salvare l'energia del satellite ORA.
            v1.delete_namespaced_pod(
                name=pod.metadata.name,
                namespace="default",
                grace_period_seconds=0
            )
            
    except Exception as e:
        print(f"❌ Errore durante l'eviction su {node_name}: {e}")

# =============================================================================
# CICLO PRINCIPALE
# =============================================================================

def main():
    print("--- Space Cloud REACTIVE Watchdog Avviato ---")
    print("📡 Monitoraggio Telemetria (Batteria) e Stato Salute (Ready/NotReady)...")
    
    # 1. Connessione al Cluster
    try:
        config.load_kube_config()
        v1 = client.CoreV1Api()
    except Exception as e:
        print(f"Errore connessione: {e}")
        return

    # 2. Avvio del Watcher (Radar)
    w = watch.Watch()

    while True:
        try:
            # CICLO EVENT-DRIVEN
            # Invece di chiedere ogni 3 secondi "come va?", restiamo in ascolto passivo.
            # Questo stream ci invia un evento SOLO quando qualcosa cambia in un Nodo.
            # 'timeout_seconds=30': Ogni 30 secondi resetta la connessione per evitare blocchi.
            for event in w.stream(v1.list_node, timeout_seconds=15):
                event_type = event['type']
                node = event['object']
                node_name = node.metadata.name
                
                # FILTRO EVENTI:
                # Ci interessano solo le MODIFICHE (es. physics_sim cambia l'etichetta batteria).
                # Ignoriamo eventi come "ADDED" (nuovo nodo) o "DELETED".
                if event_type != "MODIFIED":
                    continue

                # --- CHECK 1: STATO SALUTE (Hardware Failure) ---
                # Questo ha la priorità massima. Se il satellite è rotto, 
                # dobbiamo spostare i carichi indipendentemente dalla batteria.
                if not is_node_ready(node):
                    # Se il nodo è diventato NotReady, controlliamo se ci sono pod da salvare
                    # (Anche se Kubernetes lo vede offline, forzare la cancellazione del pod
                    # permette al Deployment di ricrearne subito uno nuovo altrove).
                    evict_pods_on_node(v1, node_name, "GUASTO HARDWARE RILEVATO (Node NotReady)")
                    
                    # Se è rotto, non serve controllare la batteria. Passiamo al prossimo evento.
                    continue
                
                # --- CHECK 2: STATO ENERGETICO (Battery Low) ---
                # PASSO A: Leggiamo la nuova telemetria appena arrivata
                battery = get_node_battery(node)
                
                # PASSO B: Controllo Soglia (Decisione Rapida)
                if battery < MIN_BATTERY:
                    # NOTA BENE: Non stampiamo nulla qui!
                    # Passiamo la palla alla funzione operativa 'evict_pods_on_node'.
                    # Sarà lei a controllare se c'è davvero qualcosa da uccidere e,
                    # solo in quel caso, a lanciare l'allarme visivo.
                    evict_pods_on_node(v1, node_name, f"ALLARME BATTERIA ({battery}%)")
                
                # Se la batteria è OK, il ciclo continua silenziosamente in background.
                
        except KeyboardInterrupt:
            # Uscita pulita
            print("\n🛑 Watchdog fermato.")
            break
        except Exception as e:
            # Gestione errori di rete
            print(f"Errore nello stream (riconnessione...): {e}")
            time.sleep(1)

if __name__ == "__main__":
    main()