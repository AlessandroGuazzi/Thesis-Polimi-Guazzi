// =============================================================================
//  SPACE GUARDIAN V5.2 (Event-Driven State Manager)
//  Role: Acts as the stateful sidecar that survives migrations via CRIU.
//  It manages the mission state, UI dashboard, and pre-migration cleanup.
// =============================================================================

const express = require('express');
const redis = require('redis');
const fs = require('fs');
const bodyParser = require('body-parser');

const app = express();
const PORT = 80;

// Middleware to handle large JSON payloads from the compute container (Phoenix)
app.use(bodyParser.json({ limit: '50mb' }));

// --- Internal Global State ---
// guardianMemory: Persistent storage for AI training metrics (Epoch, Loss, Acc)
let guardianMemory = { epoch: 0, status: "WAITING_PAYLOAD", last_contact: null };
// flightMode: Safety flag to block operations during active migration transit
let flightMode = false;
let serverInstance = null;
let activeSockets = new Set(); // Tracks open TCP sockets for forceful cleanup
let dirWatcher = null;

let fleetState = {}; // Local cache of fleet telemetry for the dashboard
let sseClients = []; // List of connected web browsers (Server-Sent Events)
let redisSubscriber = null;

async function initRedisSub() {
    /** * Subscribes to the global Redis telemetry bus.
     * This allows the Guardian to act as a "BFF" (Backend For Frontend),
     * funneling physical fleet data to the web dashboard.
     */
    if (redisSubscriber) return;

    redisSubscriber = redis.createClient({ url: 'redis://system-redis:6379' });
    redisSubscriber.on('error', () => {});

    try {
        await redisSubscriber.connect();
        // Wildcard subscription: listens to all satellites simultaneously
        await redisSubscriber.pSubscribe('telemetry/*', (message, channel) => {
            const nodeName = channel.split('/')[1];
            fleetState[nodeName] = JSON.parse(message);
            // Push updated fleet data to all connected web clients
            broadcastToClients();
        });
        console.log("📡 Guardian subscribed to Redis Event Bus.");
    } catch(e) {
        console.log("⚠️  Redis connection error:", e.message);
        redisSubscriber = null;
    }
}

// Endpoint used by the Phoenix container to save training progress
app.post('/state', (req, res) => {
    // Prevent state writes if the pod is currently being frozen/migrated
    if (flightMode) return res.status(503).json({ error: "MIGRATION_IN_PROGRESS" });

    // Update local memory with fresh data from calculation engine
    guardianMemory = { ...req.body, last_contact: Date.now() };
    broadcastToClients();
    res.json({ status: "SAVED" });
});

// Endpoint used by the Phoenix container to recover state after a "Warm Boot"
app.get('/state', (req, res) => res.json(guardianMemory));

// Serves the static HTML/JS files for the Mission Control dashboard
app.use(express.static('dashboard-ui'));

function broadcastToClients() {
    /**
     * Packages AI metrics and physical telemetry into a single JSON payload.
     * Dispatched via Server-Sent Events (SSE) for a real-time reactive UI.
     */
    const payload = JSON.stringify({
        guardian_id: process.env.HOSTNAME,
        internal_state: guardianMemory,
        environment: fleetState,
        flight_mode: flightMode
    });
    sseClients.forEach(client => client.write(`data: ${payload}\n\n`));
}

// SSE Endpoint: Establishes a persistent one-way tunnel to the web browser
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
    /** * Starts the Express server and implements a custom socket tracker.
     * This is critical for the "Graceful Freeze" logic required by CRIU.
     */
    if (serverInstance) return;
    serverInstance = app.listen(PORT, () => console.log(`🛡️  GUARDIAN ONLINE (PORT ${PORT})`));
    serverInstance.on('connection', (socket) => {
        // Block new connections during migration transit
        if (flightMode) { socket.destroy(); return; }
        activeSockets.add(socket);
        socket.on('close', () => activeSockets.delete(socket));
    });
}

// Recovery Watcher: Detects when the pod has "landed" on a new node
setInterval(() => {
    if (flightMode && fs.existsSync('/tmp/landed')) {
        console.log("🛬 LANDING CONFIRMED! Reanimating memory...");
        try { fs.unlinkSync('/tmp/landed'); } catch(e){}
        flightMode = false;

        // Restart all subsystems after the CRIU Restore phase
        startServer();
        initRedisSub();
        setupWatcher();
    }
}, 1000);

function setupWatcher() {
    /**
     * File-system Watcher: Monitors for the 'prepare_jump' signal.
     * This signal is created by the Node Agent to trigger the migration choreography.
     */
    const TRIGGER_FILE = '/tmp/prepare_jump';
    if (fs.existsSync(TRIGGER_FILE)) try { fs.unlinkSync(TRIGGER_FILE); } catch(e){}
    try {
        dirWatcher = fs.watch('/tmp', (eventType, filename) => {
            if (filename === 'prepare_jump' && !flightMode) {
                console.log("🚨 PREPARING MIGRATION...");
                flightMode = true; // Lock state to protect memory integrity
                if (dirWatcher) dirWatcher.close();

                // Notify UI immediately so it can show the "Teleporting" overlay
                sseClients.forEach(c => c.write(`data: ${JSON.stringify({flight_mode: true})}\n\n`));

                // GRACEFUL DISCONNECT PHASE
                // We wait 2 seconds to allow the OS to flush buffers before freezing
                setTimeout(async () => {
                    console.log(`🔌 DISCONNECTING SYSTEMS (Memory Protection)...`);

                    // Close all web streams
                    sseClients.forEach(c => c.end());
                    sseClients = [];

                    // Brutally destroy any remaining TCP sockets to avoid CRIU restore errors
                    for (const socket of activeSockets) socket.destroy();
                    activeSockets.clear();

                    // Shutdown the HTTP server
                    if (serverInstance) serverInstance.close(() => serverInstance = null);

                    // Disconnect from Redis Bus to release network handles
                    if (redisSubscriber) {
                        try { await redisSubscriber.quit(); } catch(e) {}
                        redisSubscriber = null;
                    }

                    console.log("🔒 GUARDIAN READY FOR CRIU FREEZE.");
                }, 2000);
            }
        });
    } catch (e) {}
}

// --- Initial Boot Sequence ---
startServer();
initRedisSub();
setupWatcher();