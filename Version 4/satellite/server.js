const express = require('express');
const redis = require('redis');
const fs = require('fs');
const app = express();
const PORT = 80;

// === 1. STATO SISTEMA ===
let internalState = {
    mission_timer: 0,
    boot_time: new Date().toISOString()
};

let flightMode = false;
let serverInstance = null; // Variabile per gestire il server
let activeSockets = new Set();
let dirWatcher = null;

// === 2. GESTIONE SERVER HTTP (FUNZIONI START/STOP) ===
function startServer() {
    if (serverInstance) return; // Già attivo

    serverInstance = app.listen(PORT, () => {
        console.log(`📡 SATELLITE ONLINE SU PORTA ${PORT}`);
    });

    serverInstance.on('connection', (socket) => {
        if (flightMode) {
            socket.destroy();
            return;
        }
        activeSockets.add(socket);
        socket.on('close', () => activeSockets.delete(socket));
    });
}

// Avvio iniziale
startServer();

// === 3. HELPER REDIS ===
async function useRedis(callback) {
    if (flightMode) return;
    const client = redis.createClient({
        url: 'redis://system-redis:6379',
        socket: { connectTimeout: 1000 }
    });
    client.on('error', (err) => { });
    try {
        await client.connect();
        await callback(client);
    } catch (e) { }
    finally {
        if (client.isOpen) await client.disconnect();
    }
}

// === 4. LOOP MISSIONE (IL CUORE PULSANTE) ===
setInterval(() => {
    // Incremento timer (questo sopravvive alla migrazione!)
    internalState.mission_timer++;

    if (internalState.mission_timer % 5 === 0) {
        console.log(`[SAT-LOG] T+${internalState.mission_timer}s | FlightMode: ${flightMode}`);
    }

    // === CHECK DI ATTERRAGGIO (PHOENIX PROTOCOL) ===
    // Se siamo in volo, controlliamo se siamo atterrati cercando un file
    if (flightMode) {
        // Usiamo try/catch sincrono perché è leggero
        if (fs.existsSync('/tmp/landed')) {
            console.log("🛬 ATTERRAGGIO CONFERMATO! Riavvio sistemi...");

            // 1. Rimuovi il trigger
            try { fs.unlinkSync('/tmp/landed'); } catch(e){}

            // 2. Disattiva Flight Mode
            flightMode = false;

            // 3. RIACCENDI IL SERVER WEB!
            startServer();

            // 4. Riattiva il watcher per il prossimo salto (Opzionale)
            setupWatcher();
        }
    }
}, 1000);


// === 5. TRIGGER DICOLLO (PREPARE JUMP) ===
function setupWatcher() {
    const TRIGGER_FILE = '/tmp/prepare_jump';
    if (fs.existsSync(TRIGGER_FILE)) try { fs.unlinkSync(TRIGGER_FILE); } catch(e){}

    try {
        dirWatcher = fs.watch('/tmp', (eventType, filename) => {
            if (filename === 'prepare_jump' && !flightMode) {
                console.log("🚨 PREPARAZIONE AL SALTO INIZIATA...");
                flightMode = true;

                // Chiudiamo il watcher per evitare errori fsnotify di CRIU
                if (dirWatcher) dirWatcher.close();

                setTimeout(() => {
                    console.log(`🔌 SPEGNIMENTO MOTORI. Terminazione connessioni...`);

                    for (const socket of activeSockets) {
                        socket.destroy();
                    }
                    activeSockets.clear();

                    if (serverInstance) {
                        serverInstance.close(() => {
                            console.log("🔒 SERVER HTTP SPENTO. Pronto per CRIU.");
                            serverInstance = null;
                        });
                    }
                }, 2000);
            }
        });
    } catch (e) { console.log("Errore watcher:", e); }
}

// Avvio watcher iniziale
setupWatcher();


// === 6. API WEB ===
app.use(express.static('public'));

app.get('/api/telemetry', async (req, res) => {
    let envData = { status: "NO_DATA" };

    if (!flightMode) {
        await useRedis(async (client) => {
            const raw = await client.get('fleet_telemetry');
            if (raw) envData = JSON.parse(raw);
        });
    } else {
        envData = { status: "MIGRATION_IN_PROGRESS" };
    }

    res.json({
        satellite_id: process.env.HOSTNAME,
        internal_state: internalState,
        environment: envData,
        flight_mode: flightMode
    });
});