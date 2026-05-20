// =============================================================================
//  SPACE GUARDIAN V7 (Stateful Sidecar — Time-Series CNN Wildfire Tracker)
//  Role: Survives satellite migrations via CRIU. Stores the FIFO T=5 history
//        buffer and the latest prediction mask.
//        Provides the pre-freeze flush handshake, Ground Redis subscriber,
//        and the SSE dashboard stream.
// =============================================================================

const express = require('express');
const redis = require('redis');
const fs = require('fs');
const bodyParser = require('body-parser');

const app = express();
const PORT = 80;

app.use(bodyParser.json({ limit: '50mb' }));

// ---------------------------------------------------------------------------
// INTERNAL STATE (The Single Source of Truth)
// ---------------------------------------------------------------------------
let guardianMemory = {
    history_frames: [], // FIFO rolling buffer for the last T=5 days (base64 encoded)
    predicted_fire_mask: [],
    center_of_mass: { x: 32, y: 32 },
    fire_pixel_count: 0,
    sample_count: 0,
    migration_epoch: 0,
    status: "WAITING_PAYLOAD",
    last_contact: null
};

let flightMode = false;
let serverInstance = null;
let activeSockets = new Set();
let dirWatcher = null;
let fleetState = {};
let sseClients = [];
let redisSubscriber = null;
let payloadLastLandedTime = Date.now() / 1000;

// ---------------------------------------------------------------------------
// REDIS — subscribes to ground-redis for fleet hardware telemetry
// ---------------------------------------------------------------------------
async function initRedisSub() {
    if (redisSubscriber) return;

    redisSubscriber = redis.createClient({ url: 'redis://ground-redis:6379' });
    redisSubscriber.on('error', () => { });

    try {
        await redisSubscriber.connect();
        await redisSubscriber.pSubscribe('telemetry/*', (message, channel) => {
            const nodeName = channel.split('/')[1];
            fleetState[nodeName] = JSON.parse(message);
        });
        console.log("📡 Guardian subscribed to Ground Redis telemetry bus.");
    } catch (e) {
        console.log("⚠️  Redis connection error:", e.message);
        redisSubscriber = null;
    }
}

// ---------------------------------------------------------------------------
// HTTP ENDPOINTS
// ---------------------------------------------------------------------------

// The stateless Python worker POSTs the newest frame and its predictions here
app.post('/state', (req, res) => {
    if (flightMode) {
        console.log("⚠️ GUARDIAN: Rejecting /state POST — flightMode is ACTIVE.");
        return res.status(503).json({ error: "MIGRATION_IN_PROGRESS" });
    }

    const body = req.body;

    // Aggiorniamo direttamente le metriche e le maschere (molto più leggero per la RAM)
    if (body.metrics) {
        Object.assign(guardianMemory, body.metrics);
    }

    guardianMemory.last_contact = Date.now();
    guardianMemory.status = "TRACKING";

    res.json({ status: "SAVED" });
});

// The stateless Python worker GETs this to assemble the T=5 input tensor
app.get('/state', (req, res) => res.json(guardianMemory));

// Serves the static HTML + JS files for the 2D fire mask dashboard
app.use(express.static('dashboard-ui'));

// ---------------------------------------------------------------------------
// SSE BROADCAST — pushes state to all connected dashboard browsers
// ---------------------------------------------------------------------------
function broadcastToClients() {
    const payload = JSON.stringify({
        guardian_id: process.env.HOSTNAME,
        internal_state: {
            ...guardianMemory,
            // Map state to what index.html expects for the progress bars
            // We use history_frames.length as stm_size (0-5 range) and 5 as maximum
            stm_size: guardianMemory.history_frames.length,
            ltm_size: 5
        },
        environment: fleetState,
        flight_mode: flightMode,
        migration_epoch: guardianMemory.migration_epoch
    });
    sseClients.forEach(client => client.write(`data: ${payload}\n\n`));
}

// SSE Endpoint: Browsers connect here to receive a live event stream
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

// ---------------------------------------------------------------------------
// SERVER LIFECYCLE
// ---------------------------------------------------------------------------
function startServer() {
    if (serverInstance) return;

    serverInstance = app.listen(PORT, () =>
        console.log(`🛡️  GUARDIAN ONLINE (PORT ${PORT})`)
    );

    serverInstance.on('connection', (socket) => {
        if (flightMode) { socket.destroy(); return; }
        activeSockets.add(socket);
        socket.on('close', () => activeSockets.delete(socket));
    });
}

// ---------------------------------------------------------------------------
// FILE-BASED IPC WATCHER & LANDING RECOVERY WATCHER
// ---------------------------------------------------------------------------
function setupWatcher() {
    const FILES_TO_CLEAN = ['/tmp/flush_complete', '/tmp/prepare_jump'];
    FILES_TO_CLEAN.forEach(f => {
        if (fs.existsSync(f)) {
            try { fs.unlinkSync(f); } catch (e) { }
        }
    });

    if (fs.existsSync('/tmp/flush_state')) {
        try { fs.unlinkSync('/tmp/flush_state'); } catch (e) { }
    }

    try {
        dirWatcher = fs.watch('/tmp', async (eventType, filename) => {
            if (filename === 'prepare_jump' && !flightMode) {
                console.log("🚨 GUARDIAN: prepare_jump detected — entering flight mode...");
                flightMode = true;
                if (dirWatcher) dirWatcher.close();

                // Tell the dashboard UI to show the "Migrating..." overlay immediately
                sseClients.forEach(c => c.write(
                    `data: ${JSON.stringify({ flight_mode: true })}\n\n`
                ));

                setTimeout(async () => {
                    console.log("💤 GUARDIAN: Disconnecting all systems for CRIU...");
                    sseClients.forEach(c => c.end());
                    sseClients = [];

                    for (const socket of activeSockets) socket.destroy();
                    activeSockets.clear();

                    if (serverInstance) serverInstance.close(() => serverInstance = null);

                    if (redisSubscriber) {
                        try { await redisSubscriber.quit(); } catch (e) { }
                        redisSubscriber = null;
                    }

                    console.log("❄️ GUARDIAN: Ready for CRIU checkpoint freeze.");
                }, 200);
            }
        });
    } catch (e) {
        console.log("⚠️  GUARDIAN: fs.watch error:", e.message);
    }
}

// LANDING RECOVERY WATCHER
setInterval(() => {
    if (flightMode && fs.existsSync('/tmp/landed')) {
        console.log("✅ LANDING CONFIRMED! Reanimating Guardian...");
        try { fs.unlinkSync('/tmp/landed'); } catch (e) { }

        flightMode = false;
        payloadLastLandedTime = Date.now() / 1000;
        guardianMemory.migration_epoch += 1;

        startServer();
        initRedisSub();
        setupWatcher();
    }
}, 1000);

// BOOT
startServer();
initRedisSub();
setupWatcher();

// PACEMAKER (500ms)
setInterval(() => {
    if (!flightMode) {
        broadcastToClients();

        // Write IPC state for the Node Agent
        const agentState = {
            center_of_mass: guardianMemory.center_of_mass || { x: 32, y: 32 },
            fire_pixel_count: guardianMemory.fire_pixel_count || 0,
            last_migration_time: payloadLastLandedTime
        };

        fs.writeFile('/tmp/payload_state.json', JSON.stringify(agentState), (err) => {
            // ignore errors
        });

        // Autocomplete flush request (stateless worker requires zero flushing)
        if (fs.existsSync('/tmp/flush_state')) {
            console.log("🚨 GUARDIAN: Node Agent requested pre-freeze flush. Auto-acknowledging instantly.");
            try { fs.writeFileSync('/tmp/flush_complete', ''); } catch (e) { }
            try { fs.unlinkSync('/tmp/flush_state'); } catch (e) { }
        }
    }
}, 500);