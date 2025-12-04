# Importiamo le librerie necessarie:
import time     # Serve per mettere in pausa lo script (aspettare 5 secondi)
import random   # Serve per generare numeri casuali (simula la carica/scarica della batteria)
from kubernetes import client, config # Libreria ufficiale per parlare con Kubernetes

# --- CONFIGURAZIONE ---
# Questa è l'etichetta (il "post-it") che appiccicheremo sui nodi.
# Lo scheduler leggerà questa etichetta per decidere se usare il satellite.
LABEL_KEY = "spacecloud.io/battery_level"

# Ogni quanti secondi dobbiamo aggiornare i dati?
UPDATE_INTERVAL = 20

def main():
    print("--- Avvio Simulazione Fisica Space Cloud ---")
    
    # ---------------------------------------------------------
    # FASE 1: CONNESSIONE AL CLUSTER
    # ---------------------------------------------------------
    try:
        # Carica il file di configurazione dal tuo computer (solitamente in ~/.kube/config)
        # È come inserire la password per accedere al "Controllo Missione".
        config.load_kube_config()
        
        # Crea un "client" per parlare con le API principali (CoreV1) di Kubernetes.
        # Con questo oggetto 'v1' possiamo leggere e modificare i Nodi e i Pod.
        v1 = client.CoreV1Api()
        print("Connessione al cluster riuscita. Inizio telemetria...")
        
    except Exception as e:
        # Se qualcosa va storto (es. Docker non è avviato), stampa l'errore ed esci.
        print(f"Errore critico di connessione: {e}")
        return

    # ---------------------------------------------------------
    # FASE 2: CICLO INFINITO (La simulazione vera e propria)
    # ---------------------------------------------------------
    # Questo ciclo gira per sempre finché non premi CTRL+C per fermarlo.
    while True:
        try:
            # Chiediamo a Kubernetes: "Dammi la lista di tutti i nodi (satelliti) attivi ora".
            # .items serve per ottenere la lista vera e propria degli oggetti nodo.
            nodes = v1.list_node().items
            
            # Ora analizziamo ogni singolo nodo trovato nella lista...
            for node in nodes:
                # Leggiamo il nome del nodo (es. "space-cloud-worker")
                node_name = node.metadata.name
                
                # --- FILTRO DI SICUREZZA ---
                # Il nodo "control-plane" è il cervello del cluster, non un satellite operativo.
                # Non vogliamo simulare batterie scariche su di lui, altrimenti il cluster si rompe.
                # Se il nome contiene "control-plane", saltiamo al prossimo nodo (continue).
                if "control-plane" in node_name:
                    continue
                
                # --- SIMULAZIONE FISICA (Il cuore della tesi) ---
                # Qui simuliamo il livello di carica.
                # In futuro, questo numero verrà da un calcolo orbitale (Sole/Ombra).
                # Per ora, generiamo un numero casuale tra 0 (morto) e 100 (pieno).
                current_battery = random.randint(0, 50)
                
                # --- PREPARAZIONE DELL'AGGIORNAMENTO (PATCH) ---
                # Creiamo un dizionario (un pacchetto dati JSON) che dice a Kubernetes:
                # "Per favore, modifica SOLO i metadata -> labels -> battery_level di questo nodo".
                body = {
                    "metadata": {
                        "labels": {
                            # È importante convertire il numero in stringa (str) perché
                            # le etichette di Kubernetes accettano solo testo.
                            LABEL_KEY: str(current_battery)
                        }
                    }
                }
                
                # --- INVIO AL CLUSTER ---
                # Usiamo il comando 'patch_node' per applicare la modifica senza riavviare nulla.
                # È come attaccare un nuovo adesivo sul satellite mentre è in volo.
                v1.patch_node(node_name, body)
                
                # Stampiamo un messaggio a video per confermare che ha funzionato.
                print(f"[TELEMETRIA] {node_name}: Batteria aggiornata a {current_battery}%")
            
            # Finito il giro di tutti i satelliti, aspettiamo prima del prossimo aggiornamento.
            print(f"Attendo {UPDATE_INTERVAL} secondi prima del prossimo ciclo...\n")
            time.sleep(UPDATE_INTERVAL)
            
        except KeyboardInterrupt:
            # Se l'utente preme CTRL+C nel terminale, fermiamo tutto gentilmente.
            print("\nSimulazione interrotta manualmente dall'utente.")
            break
            
        except Exception as e:
            # Se succede un errore imprevisto (es. cade la rete), lo stampiamo ma
            # riproviamo al prossimo giro invece di crashare tutto lo script.
            print(f"Errore durante il ciclo: {e}")
            time.sleep(UPDATE_INTERVAL)

# Questa riga serve a dire a Python: "Se lanci questo file direttamente, esegui la funzione main".
if __name__ == "__main__":
    main()