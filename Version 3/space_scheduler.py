import time
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException

# =============================================================================
# CONFIGURAZIONE DELLA MISSIONE (PARAMETRI DELLO SCHEDULER)
# =============================================================================

# Il nome univoco del nostro scheduler.
# Nel file YAML del Pod (space-task.yaml) dobbiamo specificare:
# "schedulerName: space-scheduler" affinché Kubernetes ci passi il controllo.
SCHEDULER_NAME = "space-scheduler"

# La chiave dell'etichetta (Label) che usiamo per simulare la telemetria.
# Deve corrispondere a quella usata nello script 'physics_sim.py'.
BATTERY_LABEL = "spacecloud.io/battery_level"

# Soglia critica di missione.
# Se un satellite ha meno del 20% di batteria, non è sicuro assegnargli nuovi task.
MIN_BATTERY = 20

# =============================================================================
# FUNZIONI AUSILIARIE
# =============================================================================

def get_node_battery(node):
    """
    Legge il livello di batteria dai metadati del Nodo (Satellite).
    
    Args:
        node: L'oggetto Nodo restituito dalle API di Kubernetes.
        
    Returns:
        int: Il valore della batteria (0-100). Restituisce 0 se il dato manca.
    """
    # I metadati contengono le 'labels' (le etichette appiccicate sul nodo)
    labels = node.metadata.labels
    
    # Verifichiamo se l'etichetta della batteria esiste
    if labels and BATTERY_LABEL in labels:
        # Trovata! Convertiamo la stringa "80" in intero 80 e la restituiamo
        return int(labels[BATTERY_LABEL])
    
    # Se l'etichetta non c'è, assumiamo che il satellite sia spento o non monitorato.
    # Restituire 0 evita che venga selezionato per errore.
    return 0

def schedule_pod(v1, name, node, namespace="default"):
    """
    Esegue l'operazione di BINDING (Assegnazione).
    Collega ufficialmente un Pod 'Pending' a un Nodo specifico.
    
    Args:
        v1: Il client API di Kubernetes.
        name: Il nome del Pod da schedulare.
        node: Il nome del Nodo (satellite) scelto.
        namespace: Il namespace di lavoro (default).
    """
    
    # 1. Controllo di sicurezza: non possiamo assegnare a un nodo nullo
    if not node:
        print("❌ Errore interno: Nome del nodo mancante!")
        return False

    # 2. Creiamo il riferimento al Nodo bersaglio (Target)
    target = client.V1ObjectReference(
        kind="Node",
        api_version="v1",
        name=node
    )
    
    # 3. Creiamo i metadati per l'oggetto Binding (usando il nome del Pod)
    meta = client.V1ObjectMeta(name=name)
    
    # 4. Costruiamo l'oggetto Binding completo
    # Questo è il "documento ufficiale" che invieremo a Kubernetes
    body = client.V1Binding(
        api_version="v1",
        kind="Binding",
        metadata=meta,
        target=target
    )
    
    try:
        print(f"📡 Invio comando di Binding: Pod {name} -> Nodo {node}")
        
        # --- CHIAMATA API CRITICA ---
        # Usiamo il parametro speciale '_preload_content=False'.
        # MOTIVO: Esiste un bug noto nella libreria Python di Kubernetes che causa
        # un errore di validazione ("Invalid value for target") quando legge la risposta.
        # Impostando questo a False, diciamo alla libreria di inviare il comando
        # e ignorare la risposta, evitando il crash.
        v1.create_namespaced_binding(
            namespace=namespace, 
            body=body, 
            _preload_content=False
        )
        
        print(f"✅ BINDING RIUSCITO: Pod {name} assegnato al Nodo {node}")
        return True

    except ApiException as e:
        # Gestione dell'errore 409 (Conflict).
        # Succede se proviamo ad assegnare un Pod che è appena stato assegnato (doppio click).
        # Non è un errore grave, quindi lo trattiamo come un successo.
        if e.status == 409:
            print(f"⚠️  Info: Pod {name} già in fase di assegnazione (Conflitto 409 ignorato).")
            return True
        else:
            # Altri errori API sono veri problemi (es. permessi negati)
            print(f"❌ Errore API critico: {e.status} - {e.reason}")
            return False
            
    except Exception as e:
        # Cattura errori generici di Python (es. bug nel codice)
        print(f"❌ Errore generico imprevisto: {e}")
        return False

# =============================================================================
# CICLO PRINCIPALE (MAIN LOOP)
# =============================================================================

def main():
    print("--- Space Cloud Custom Scheduler Avviato ---")
    
    # 1. Connessione al Cluster
    # Carica la configurazione dal file ~/.kube/config (creato da Kind)
    config.load_kube_config()
    v1 = client.CoreV1Api()
    
    # 2. Creazione del Watcher
    # Il Watcher è un "radar" che ci notifica in tempo reale quando cambia qualcosa.
    w = watch.Watch()
    
    # Ciclo infinito per mantenere lo scheduler sempre attivo
    while True:
        try:
            # print("⏳ In attesa di nuovi Pod... (CTRL+C per uscire)")
            
            # Apriamo uno stream di eventi sui Pod.
            # 'timeout_seconds=5': Ogni 5 secondi la connessione si chiude e il ciclo ricomincia.
            # Questo è necessario per permettere a Python di ricevere il segnale di stop (CTRL+C).
            for event in w.stream(v1.list_namespaced_pod, "default", timeout_seconds=5):
                pod = event['object']
                
                # --- FASE 0: PRE-FILTRO (Identificazione) ---
                # Analizziamo il Pod solo se soddisfa TUTTE queste condizioni:
                # 1. Status è 'Pending' (è in attesa di un nodo).
                # 2. Richiede specificamente il NOSTRO scheduler ('space-scheduler').
                # 3. Non ha ancora un nodo assegnato (node_name è vuoto).
                if pod.status.phase == "Pending" and \
                   pod.spec.scheduler_name == SCHEDULER_NAME and \
                   pod.spec.node_name is None:
                    
                    print(f"\n🛰️ Rilevato Pod da schedulare: {pod.metadata.name}")
                    
                    # --- FASE 1: FILTERING (Selezione Candidati) ---
                    # Scarichiamo la lista aggiornata di tutti i satelliti
                    all_nodes = v1.list_node().items
                    candidates = [] # Lista per i nodi validi
                    
                    for node in all_nodes:
                        # Ignoriamo il nodo di comando (Control Plane)
                        if "control-plane" in node.metadata.name:
                            continue
                        
                        # Cerchiamo la condizione "Ready" nella lista delle condizioni del nodo
                        is_ready = False
                        for condition in node.status.conditions:
                            if condition.type == "Ready" and condition.status == "True":
                                is_ready = True
                                break
                        
                        if not is_ready:
                            print(f"   - Analisi {node.metadata.name}: ❌ NODO NON PRONTO (Guasto/Offline)")
                            continue # Salta questo nodo, è rotto

                        # Leggiamo la telemetria (livello batteria)
                        battery = get_node_battery(node)
                        print(f"   - Analisi {node.metadata.name}: Batteria {battery}%")
                        
                        # Applichiamo la "Flight Rule": Batteria deve essere >= 20%
                        if battery >= MIN_BATTERY:
                            candidates.append((node.metadata.name, battery))
                        else:
                            print(f"     ⚠️ SCARTATO: Batteria insufficiente (< {MIN_BATTERY}%)")
                    
                    # Se nessun satellite ha abbastanza energia, non possiamo fare nulla.
                    if not candidates:
                        print("⛔ Nessun satellite disponibile. Il Pod rimarrà in Pending.")
                        continue # Passiamo al prossimo evento o aspettiamo

                    # --- FASE 2: SCORING (Graduatoria) ---
                    # Ordiniamo i candidati in base alla batteria (dal più alto al più basso).
                    # x[1] indica il secondo elemento della tupla (la batteria).
                    candidates.sort(key=lambda x: x[1], reverse=True)
                    
                    # Il vincitore è il primo della lista (quello con più energia)
                    best_node = candidates[0][0]
                    best_battery = candidates[0][1]
                    
                    print(f"⭐ SCELTA MIGLIORE: {best_node} con {best_battery}% di energia.")
                    
                    # --- FASE 3: BINDING (Esecuzione) ---
                    # Chiamiamo la funzione che applica la decisione su Kubernetes
                    schedule_pod(v1, pod.metadata.name, best_node)

        except KeyboardInterrupt:
            # Gestione pulita dell'uscita con CTRL+C
            print("\n🛑 Scheduler fermato dall'utente.")
            break
            
        except Exception as e:
            # Se cade la connessione o c'è un errore imprevisto, stampiamo e riproviamo
            print(f"Errore o Timeout nel Watcher (riavvio...): {e}")
            # Il 'while True' farà ripartire il monitoraggio immediatamente

# Entry point dello script
if __name__ == "__main__":
    main()