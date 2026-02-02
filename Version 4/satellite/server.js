// Import delle librerie necessarie
const express = require('express');   // Framework web per creare il server HTTP
const redis = require('redis');       // Client Redis per leggere dati esterni
const fs = require('fs');             // Modulo file system per leggere/scrivere file locali

// Creazione app Express
const app = express();
const PORT = 80; // Porta su cui ascolta il server HTTP

// === 1. STATO SISTEMA ===
// Oggetto che mantiene lo stato interno del “satellite”
let internalState = {
    mission_timer: 0,                         // Timer missione in secondi
    boot_time: new Date().toISOString()      // Timestamp di avvio
};

let flightMode = false;          // Indica se il sistema è in modalità volo/migrazione
let serverInstance = null;       // Riferimento all’istanza del server HTTP
let activeSockets = new Set();   // Insieme delle connessioni attive
let dirWatcher = null;           // Watcher per controllare i file di trigger

// === 2. GESTIONE SERVER HTTP (FUNZIONI START/STOP) ===
function startServer() {
    if (serverInstance) return; // Se il server è già attivo non fare nulla

    // Avvia il server HTTP
    serverInstance = app.listen(PORT, () => {
        console.log(`📡 SATELLITE ONLINE SU PORTA ${PORT}`);
    });

    // Gestione nuove connessioni socket
    serverInstance.on('connection', (socket) => {
        // Se siamo in flight mode rifiutiamo subito la connessione
        if (flightMode) {
            socket.destroy();
            return;
        }

        // Salviamo il socket tra quelli attivi
        activeSockets.add(socket);

        // Quando il socket si chiude lo rimuoviamo dall’elenco
        socket.on('close', () => activeSockets.delete(socket));
    });
}

// Avvio iniziale del server
startServer();

// === 3. HELPER REDIS ===
// Funzione di utilità per usare Redis in modo sicuro e temporaneo
async function useRedis(callback) {
    if (flightMode) return; // Non usare Redis durante la migrazione

    // Creazione client Redis con timeout breve
    const client = redis.createClient({
        url: 'redis://system-redis:6379',
        socket: { connectTimeout: 1000 }
    });

    // Ignora errori di connessione (gestione silenziosa)
    client.on('error', (err) => { });

    try {
        // Connessione, uso callback, poi operazioni Redis
        await client.connect();
        await callback(client);
    } catch (e) { 
        // Errori ignorati per non bloccare il sistema
    }
    finally {
        // Disconnessione sicura se il client è aperto
        if (client.isOpen) await client.disconnect();
    }
}

// === 4. LOOP MISSIONE (IL CUORE PULSANTE) ===
// Loop che gira ogni secondo per aggiornare stato e controlli
setInterval(() => {
    // Incremento del timer missione (persistente tra migrazioni)
    internalState.mission_timer++;

    // Log ogni 5 secondi dello stato corrente
    if (internalState.mission_timer % 5 === 0) {
        console.log(`[SAT-LOG] T+${internalState.mission_timer}s | FlightMode: ${flightMode}`);
    }

    // === CHECK DI ATTERRAGGIO (PHOENIX PROTOCOL) ===
    // Se siamo in flight mode controlliamo se esiste il file di atterraggio
    if (flightMode) {
        // Controllo sincrono leggero dell’esistenza file
        if (fs.existsSync('/tmp/landed')) {
            console.log("🛬 ATTERRAGGIO CONFERMATO! Riavvio sistemi...");

            // 1. Rimuove il file trigger di atterraggio
            try { fs.unlinkSync('/tmp/landed'); } catch(e){}

            // 2. Disattiva la modalità volo
            flightMode = false;

            // 3. Riavvia il server web
            startServer();

            // 4. Riattiva il watcher per futuri salti
            setupWatcher();
        }
    }
}, 1000);

// === 5. TRIGGER DICOLLO (PREPARE JUMP) ===
function setupWatcher() {
    const TRIGGER_FILE = '/tmp/prepare_jump';

    // Se il file trigger esiste già lo rimuove
    if (fs.existsSync(TRIGGER_FILE)) try { fs.unlinkSync(TRIGGER_FILE); } catch(e){}

    try {
        // Osserva la cartella /tmp per nuovi file
        dirWatcher = fs.watch('/tmp', (eventType, filename) => {

            // Se compare il file di trigger e non siamo già in volo
            if (filename === 'prepare_jump' && !flightMode) {
                console.log("🚨 PREPARAZIONE AL SALTO INIZIATA...");
                flightMode = true; // Attiva modalità volo

                // Chiude il watcher per evitare problemi con CRIU
                if (dirWatcher) dirWatcher.close();

                // Attende 2 secondi prima di spegnere i servizi
                setTimeout(() => {
                    console.log(`🔌 SPEGNIMENTO MOTORI. Terminazione connessioni...`);

                    // Chiude tutte le connessioni attive
                    for (const socket of activeSockets) {
                        socket.destroy();
                    }
                    activeSockets.clear();

                    // Spegne il server HTTP in modo controllato
                    if (serverInstance) {
                        serverInstance.close(() => {
                            console.log("🔒 SERVER HTTP SPENTO. Pronto per CRIU.");
                            serverInstance = null;
                        });
                    }
                }, 2000);
            }
        });
    } catch (e) { 
        // Log di eventuali errori del watcher
        console.log("Errore watcher:", e); 
    }
}

// Avvio watcher iniziale
setupWatcher();

// === 6. API WEB ===

// Serve file statici dalla cartella "public"
app.use(express.static('public'));

// Endpoint API per ottenere la telemetria
app.get('/api/telemetry', async (req, res) => {
    let envData = { status: "NO_DATA" }; // Valore di default

    // Se non siamo in flight mode leggiamo i dati da Redis
    if (!flightMode) {
        await useRedis(async (client) => {
            const raw = await client.get('fleet_telemetry');
            if (raw) envData = JSON.parse(raw); // Parse JSON se presente
        });
    } else {
        // Durante migrazione segnaliamo stato speciale
        envData = { status: "MIGRATION_IN_PROGRESS" };
    }

    // Risposta JSON con stato completo del sistema
    res.json({
        satellite_id: process.env.HOSTNAME, // ID container/host
        internal_state: internalState,      // Stato interno
        environment: envData,               // Dati ambiente da Redis
        flight_mode: flightMode             // Stato modalità volo
    });
});
