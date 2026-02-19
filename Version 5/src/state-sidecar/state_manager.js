// =============================================================================
//  SPACE GUARDIAN V5.2 (Event-Driven State Manager)
// =============================================================================

const express = require('express');
const redis = require('redis');
const fs = require('fs');
const bodyParser = require('body-parser');

const app = express();
const PORT = 80;

app.use(bodyParser.json({ limit: '50mb' }));

// Rimosso mission_timer, inizializzato con epoch: 0
let guardianMemory = { epoch: 0, status: "WAITING_PAYLOAD", last_contact: null };
let flightMode = false;
let serverInstance = null;
let activeSockets = new Set();
let dirWatcher = null;

let fleetState = {};
let sseClients = [];
let redisSubscriber = null;

async function initRedisSub() {
    if (redisSubscriber) return;

    redisSubscriber = redis.createClient({ url: 'redis://system-redis:6379' });
    redisSubscriber.on('error', () => {});

    try {
        await redisSubscriber.connect();
        await redisSubscriber.pSubscribe('telemetry/*', (message, channel) => {
            const nodeName = channel.split('/')[1];
            fleetState[nodeName] = JSON.parse(message);
            broadcastToClients();
        });
        console.log("📡 Guardian iscritto al bus eventi Redis.");
    } catch(e) {
        console.log("⚠️  Errore connessione Redis:", e.message);
        redisSubscriber = null;
    }
}

app.post('/state', (req, res) => {
    if (flightMode) return res.status(503).json({ error: "MIGRATION_IN_PROGRESS" });
    guardianMemory = { ...req.body, last_contact: Date.now() };
    broadcastToClients();
    res.json({ status: "SAVED" });
});

app.get('/state', (req, res) => res.json(guardianMemory));

app.use(express.static('dashboard-ui'));

function broadcastToClients() {
    const payload = JSON.stringify({
        guardian_id: process.env.HOSTNAME,
        internal_state: guardianMemory,
        environment: fleetState,
        flight_mode: flightMode
    });
    sseClients.forEach(client => client.write(`data: ${payload}\n\n`));
}

app.get('/api/stream', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();

    sseClients.push(res);
    broadcastToClients();

    req.on('close', () => {
        sseClients = sseClients.filter(c => c !== res);
    });
});

function startServer() {
    if (serverInstance) return;
    serverInstance = app.listen(PORT, () => console.log(`🛡️  GUARDIAN ONLINE (PORT ${PORT})`));
    serverInstance.on('connection', (socket) => {
        if (flightMode) { socket.destroy(); return; }
        activeSockets.add(socket);
        socket.on('close', () => activeSockets.delete(socket));
    });
}

setInterval(() => {
    if (flightMode && fs.existsSync('/tmp/landed')) {
        console.log("🛬 ATTERRAGGIO CONFERMATO! Risveglio memoria...");
        try { fs.unlinkSync('/tmp/landed'); } catch(e){}
        flightMode = false;

        startServer();
        initRedisSub();
        setupWatcher();
    }
}, 1000);

function setupWatcher() {
    const TRIGGER_FILE = '/tmp/prepare_jump';
    if (fs.existsSync(TRIGGER_FILE)) try { fs.unlinkSync(TRIGGER_FILE); } catch(e){}
    try {
        dirWatcher = fs.watch('/tmp', (eventType, filename) => {
            if (filename === 'prepare_jump' && !flightMode) {
                console.log("🚨 PREPARAZIONE MIGRAZIONE...");
                flightMode = true;
                if (dirWatcher) dirWatcher.close();

                sseClients.forEach(c => c.write(`data: ${JSON.stringify({flight_mode: true})}\n\n`));

                setTimeout(async () => {
                    console.log(`🔌 DISCONNESSIONE SISTEMI (Protezione Memoria)...`);

                    sseClients.forEach(c => c.end());
                    sseClients = [];

                    for (const socket of activeSockets) socket.destroy();
                    activeSockets.clear();
                    if (serverInstance) serverInstance.close(() => serverInstance = null);

                    if (redisSubscriber) {
                        try { await redisSubscriber.quit(); } catch(e) {}
                        redisSubscriber = null;
                    }

                    console.log("🔒 GUARDIAN PRONTO PER CRIU FREEZE.");
                }, 2000);
            }
        });
    } catch (e) {}
}

startServer();
initRedisSub();
setupWatcher();