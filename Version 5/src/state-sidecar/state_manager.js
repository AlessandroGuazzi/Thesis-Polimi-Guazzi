// =============================================================================
//  SPACE GUARDIAN V5 (Sidecar State Manager)
//  Ruolo: Custode della Memoria RAM e Interfaccia di Telemetria
//  Nota: Questo processo viene congelato/migrato via CRIU.
// =============================================================================

const express = require('express');   // Framework web
const redis = require('redis');       // Per leggere telemetria fisica (Redis di sistema)
const fs = require('fs');             // Per gestione trigger file (Phoenix Protocol)
const bodyParser = require('body-parser'); // Per parsare i JSON dal Payload

// Configurazione App
const app = express();
const PORT = 80; // Porta Sidecar (raggiungibile dal Payload su localhost:80)

// Middleware per parsing JSON (fondamentale per ricevere lo stato dal Payload)
app.use(bodyParser.json({ limit: '50mb' })); // Limit alto per eventuali pesi AI pesanti

// === 1. MEMORIA CUSTODITA (LO STATO) ===
// Questo oggetto risiede in RAM. CRIU salverà questa variabile su disco.
// Al ripristino, i dati saranno esattamente come lasciati.
let guardianMemory = {
    // Valori di default se il Payload non ha ancora scritto nulla
    mission_timer: 0,
    status: "WAITING_PAYLOAD",
    last_contact: null
};

// Variabili di controllo migrazione
let flightMode = false;
let serverInstance = null;
let activeSockets = new Set();
let dirWatcher = null;

// === 2. API DI STATO (INTERFACCIA PAYLOAD) ===

// [POST] /state - Il Payload invia il suo stato corrente per salvarlo
app.post('/state', (req, res) => {
    if (flightMode) {
        // Se stiamo migrando, rifiutiamo scritture per evitare incoerenze
        return res.status(503).json({ error: "MIGRATION_IN_PROGRESS" });
    }

    // Aggiornamento atomico della memoria custodita
    guardianMemory = {
        ...req.body,           // Dati dal Payload (es. timer, pesi, epoch)
        last_contact: Date.now()
    };

    res.json({ status: "SAVED", timestamp: Date.now() });
});

// [GET] /state - Il Payload appena nato chiede: "Dove eravamo rimasti?"
app.get('/state', (req, res) => {
    res.json(guardianMemory);
});


// === 3. API DASHBOARD & TELEMETRIA ===

// Serve la Dashboard UI (Static Assets)
// NOTA: Assicurati che la cartella si chiami 'dashboard-ui' come da piano V5
app.use(express.static('dashboard-ui'));

// Helper Redis (invariato)
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
    finally { if (client.isOpen) await client.disconnect(); }
}

// Endpoint Telemetria aggregata (Stato Memoria + Dati Fisici Redis)
app.get('/api/telemetry', async (req, res) => {
    let envData = { status: "NO_DATA" };

    // Recupera dati ambientali dal Digital Twin (su Redis)
    if (!flightMode) {
        await useRedis(async (client) => {
            const raw = await client.get('fleet_telemetry');
            if (raw) envData = JSON.parse(raw);
        });
    } else {
        envData = { status: "MIGRATION_IN_PROGRESS" };
    }

    // Costruisce la risposta unificata per la Dashboard
    res.json({
        guardian_id: process.env.HOSTNAME,
        internal_state: guardianMemory,    // Lo stato che custodiamo
        environment: envData,              // Lo stato del mondo fisico
        flight_mode: flightMode            // Se stiamo volando
    });
});


// === 4. PHOENIX PROTOCOL (GESTIONE MIGRAZIONE) ===
// Questa logica rimane IDENTICA perché è ciò che permette a CRIU di funzionare.

function startServer() {
    if (serverInstance) return;

    serverInstance = app.listen(PORT, () => {
        console.log(`🛡️  GUARDIAN ONLINE SU PORTA ${PORT}`);
    });

    serverInstance.on('connection', (socket) => {
        if (flightMode) { socket.destroy(); return; }
        activeSockets.add(socket);
        socket.on('close', () => activeSockets.delete(socket));
    });
}

// Check di atterraggio (Loop di controllo leggero)
setInterval(() => {
    if (flightMode) {
        if (fs.existsSync('/tmp/landed')) {
            console.log("🛬 ATTERRAGGIO CONFERMATO! Risveglio memoria...");
            try { fs.unlinkSync('/tmp/landed'); } catch(e){}

            flightMode = false;
            startServer(); // Riapre le porte al Payload e alla Dashboard
            setupWatcher();
        }
    }
}, 1000);

// Trigger di Decollo (Prepare Jump)
function setupWatcher() {
    const TRIGGER_FILE = '/tmp/prepare_jump';
    if (fs.existsSync(TRIGGER_FILE)) try { fs.unlinkSync(TRIGGER_FILE); } catch(e){}

    try {
        dirWatcher = fs.watch('/tmp', (eventType, filename) => {
            if (filename === 'prepare_jump' && !flightMode) {
                console.log("🚨 PREPARAZIONE MIGRAZIONE RILEVATA...");
                flightMode = true;

                if (dirWatcher) dirWatcher.close();

                // Graceful Shutdown
                setTimeout(() => {
                    console.log(`🔌 DISCONNESSIONE SISTEMI (Protezione Memoria)...`);
                    for (const socket of activeSockets) socket.destroy();
                    activeSockets.clear();

                    if (serverInstance) {
                        serverInstance.close(() => {
                            console.log("🔒 GUARDIAN PRONTO PER CRIU FREEZE.");
                            serverInstance = null;
                        });
                    }
                }, 2000);
            }
        });
    } catch (e) { console.log("Errore watcher:", e); }
}

// Avvio Iniziale
startServer();
setupWatcher();