// =============================================================================
//  SPACE GUARDIAN V6 (Stateful Sidecar — SAMKNN 2D Wildfire Tracker)
//  Role: Survives satellite migrations via CRIU. Stores the tinySML worker's
//        state (fire mask, Center of Mass, SAMKNN memory) in its own RAM.
//        Provides the pre-freeze flush handshake and the SSE dashboard stream.
//
//  Key changes from V5:
//   - /state handler accepts SAMKNN state shape (fire mask, CoM, STM/LTM)
//   - setupWatcher() also watches /tmp/flush_state to orchestrate the flush
//   - broadcastToClients() sends the 2D fire mask and CoM to the dashboard UI
// =============================================================================

const express    = require('express');
const redis      = require('redis');
const fs         = require('fs');
const http       = require('http');   // Used for the local /flush call to the worker
const bodyParser = require('body-parser');

const app  = express();
const PORT = 80;

// Accept large JSON payloads because the fire mask is a 4096-element array
// and the STM/LTM serialization during flush can be significantly larger
app.use(bodyParser.json({ limit: '50mb' }));

// ---------------------------------------------------------------------------
// INTERNAL STATE
// ---------------------------------------------------------------------------

// guardianMemory: Stores whatever the tinySML worker POSTs to /state.
// During normal operation this contains the latest predicted_fire_mask and CoM.
// During a pre-freeze flush, it contains the full STM + LTM arrays too.
let guardianMemory = {
    predicted_fire_mask: [],
    center_of_mass:      { x: 0, y: 0 },
    fire_pixel_count:    0,
    sample_count:        0,
    stm_size:            0,
    ltm_size:            0,
    status:              "WAITING_PAYLOAD",
    last_contact:        null
};

// flightMode: Blocks new state writes while the pod is being CRIU-frozen
let flightMode     = false;
let serverInstance = null;
let activeSockets  = new Set();  // Tracks open TCP sockets for cleanup before freeze
let dirWatcher     = null;

let fleetState  = {};   // Cache of satellite hardware telemetry for the dashboard
let sseClients  = [];   // Connected web browser SSE streams
let redisSubscriber = null;


// ---------------------------------------------------------------------------
// REDIS — subscribes to ground-redis for fleet hardware telemetry
// ---------------------------------------------------------------------------

async function initRedisSub() {
    /**
     * Subscribes to the Ground Redis telemetry bus.
     * The Guardian acts as a "BFF" (Backend For Frontend): it listens to
     * hardware telemetry (temperature, battery, orbital angle) from the
     * Digital Twin and forwards the data to the web dashboard via SSE.
     * Note: migration commands are handled via file-based IPC, not Redis.
     */
    if (redisSubscriber) return;

    // Connect to the Ground Station Redis (renamed from "system-redis" in V6)
    redisSubscriber = redis.createClient({ url: 'redis://ground-redis:6379' });
    redisSubscriber.on('error', () => {});  // Silently retry on connection drop

    try {
        await redisSubscriber.connect();
        // Wildcard subscription: captures telemetry for all satellites at once
        await redisSubscriber.pSubscribe('telemetry/*', (message, channel) => {
            const nodeName = channel.split('/')[1];
            fleetState[nodeName] = JSON.parse(message);
            // Push the updated fleet snapshot to all dashboard browsers
            broadcastToClients();
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

// The tinySML worker POSTs its current SAMKNN state here (periodic sync + flush)
app.post('/state', (req, res) => {
    // Reject writes during the CRIU freeze window — data would be lost anyway
    if (flightMode) return res.status(503).json({ error: "MIGRATION_IN_PROGRESS" });

    // Merge the incoming state with a timestamp (spread operator keeps all fields)
    guardianMemory = { ...req.body, last_contact: Date.now() };

    // Relay the new state to any open dashboard browser windows in real-time
    broadcastToClients();
    res.json({ status: "SAVED" });
});

// The tinySML worker GETs this on Warm Boot to recover its previous state
app.get('/state', (req, res) => res.json(guardianMemory));

// Serves the static HTML + JS files for the 2D fire mask dashboard
app.use(express.static('dashboard-ui'));


// ---------------------------------------------------------------------------
// SSE BROADCAST — pushes state to all connected dashboard browsers
// ---------------------------------------------------------------------------

function broadcastToClients() {
    /**
     * Packages the SAMKNN state and fleet telemetry into one JSON event.
     * The 'predicted_fire_mask' is a 3,844-element (62×62) binary array —
     * the dashboard renders it as a coloured pixel grid.
     * The 'center_of_mass' drives the crosshair overlay on the predicted mask.
     */
    const payload = JSON.stringify({
        guardian_id:    process.env.HOSTNAME,
        internal_state: guardianMemory,   // Contains fire mask, CoM, memory sizes
        environment:    fleetState,        // Hardware telemetry for all satellites
        flight_mode:    flightMode
    });
    sseClients.forEach(client => client.write(`data: ${payload}\n\n`));
}

// SSE Endpoint: Browsers connect here to receive a live event stream
app.get('/api/stream', (req, res) => {
    res.setHeader('Content-Type',  'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection',    'keep-alive');
    res.flushHeaders();  // Flush the headers immediately to open the stream

    sseClients.push(res);
    broadcastToClients();  // Send current state immediately upon connection

    // Clean up when the browser disconnects
    req.on('close', () => {
        sseClients = sseClients.filter(c => c !== res);
    });
});


// ---------------------------------------------------------------------------
// SERVER LIFECYCLE
// ---------------------------------------------------------------------------

function startServer() {
    /**
     * Starts the Express HTTP server on port 80.
     * Tracks every TCP connection so we can forcefully destroy them before CRIU.
     * (CRIU + runc wrapper can serialise TCP sockets, but destroying them first
     *  gives a cleaner checkpoint and smaller TAR file.)
     */
    if (serverInstance) return;

    serverInstance = app.listen(PORT, () =>
        console.log(`🛡️  GUARDIAN ONLINE (PORT ${PORT})`)
    );

    serverInstance.on('connection', (socket) => {
        // Reject new connections immediately if we are in the freeze window
        if (flightMode) { socket.destroy(); return; }
        activeSockets.add(socket);
        socket.on('close', () => activeSockets.delete(socket));
    });
}


// ---------------------------------------------------------------------------
// LANDING RECOVERY WATCHER
// Polls for '/tmp/landed' which the Node Agent writes after CRIU restore
// ---------------------------------------------------------------------------

setInterval(() => {
    if (flightMode && fs.existsSync('/tmp/landed')) {
        console.log("🛬 LANDING CONFIRMED! Reanimating Guardian...");
        try { fs.unlinkSync('/tmp/landed'); } catch (e) {}
        flightMode = false;

        // Restart all subsystems in order after the CRIU restore
        startServer();
        initRedisSub();
        setupWatcher();
    }
}, 1000);


// ---------------------------------------------------------------------------
// FILE-BASED IPC WATCHER
// The Node Agent communicates with the Guardian via files in /tmp — this
// avoids any external network dependency for local orchestration.
// ---------------------------------------------------------------------------

function setupWatcher() {
    /**
     * Monitors /tmp for two trigger files:
     *
     * 1. /tmp/flush_state — written by the Node Agent to start the pre-freeze flush.
     *    Flow: Node Agent writes flush_state →
     *          Guardian calls POST localhost:9000/flush on the tinySML worker →
     *          Worker serializes and POSTs its full SAMKNN state back →
     *          Guardian writes /tmp/flush_complete →
     *          Node Agent detects flush_complete and writes /tmp/prepare_jump
     *
     * 2. /tmp/prepare_jump — written by the Node Agent after flush_complete.
     *    Triggers the graceful freeze: lock state, close sockets, disconnect Redis.
     */

    // Clean up any stale trigger files from a previous migration cycle
    const FILES_TO_CLEAN = ['/tmp/flush_state', '/tmp/flush_complete', '/tmp/prepare_jump'];
    FILES_TO_CLEAN.forEach(f => { if (fs.existsSync(f)) try { fs.unlinkSync(f); } catch(e){} });

    try {
        dirWatcher = fs.watch('/tmp', async (eventType, filename) => {

            // ----------------------------------------------------------------
            // TRIGGER 1: Pre-Freeze Flush
            // ----------------------------------------------------------------
            if (filename === 'flush_state' && fs.existsSync('/tmp/flush_state')) {
                console.log("💾 GUARDIAN: flush_state detected — calling worker /flush...");
                try { fs.unlinkSync('/tmp/flush_state'); } catch(e) {}

                // Call the tinySML worker's flush endpoint (intra-Pod localhost)
                // The worker will serialize its SAMKNN state and POST it back to /state
                await callWorkerFlush();

                // Signal the Node Agent that the flush is complete
                // The Node Agent is polling for this file before writing prepare_jump
                fs.writeFileSync('/tmp/flush_complete', '');
                console.log("✅ GUARDIAN: flush_complete written. Node Agent may proceed.");
            }

            // ----------------------------------------------------------------
            // TRIGGER 2: Graceful Freeze (CRIU Preparation)
            // ----------------------------------------------------------------
            if (filename === 'prepare_jump' && !flightMode) {
                console.log("🚨 GUARDIAN: prepare_jump detected — entering flight mode...");
                flightMode = true;
                if (dirWatcher) dirWatcher.close();

                // Tell the dashboard UI to show the "Migrating..." overlay immediately
                sseClients.forEach(c => c.write(
                    `data: ${JSON.stringify({ flight_mode: true })}\n\n`
                ));

                // Graceful disconnect: give the OS 200ms to flush TCP buffers,
                // then destroy all sockets and shut down cleanly before CRIU freezes
                setTimeout(async () => {
                    console.log("🔌 GUARDIAN: Disconnecting all systems for CRIU...");

                    // Close all SSE browser streams gracefully
                    sseClients.forEach(c => c.end());
                    sseClients = [];

                    // Forcefully destroy remaining TCP sockets to avoid kernel state issues
                    // (The runc --tcp-established wrapper allows CRIU to handle them,
                    //  but destroying them first makes the checkpoint smaller and cleaner)
                    for (const socket of activeSockets) socket.destroy();
                    activeSockets.clear();

                    // Shut down the Express server
                    if (serverInstance) serverInstance.close(() => serverInstance = null);

                    // Disconnect gracefully from Ground Redis
                    if (redisSubscriber) {
                        try { await redisSubscriber.quit(); } catch (e) {}
                        redisSubscriber = null;
                    }

                    console.log("🔒 GUARDIAN: Ready for CRIU checkpoint freeze.");
                }, 200);
            }
        });
    } catch (e) {
        console.log("⚠️  GUARDIAN: fs.watch error:", e.message);
    }
}


// ---------------------------------------------------------------------------
// WORKER FLUSH HELPER
// Makes a synchronous-style HTTP POST to localhost:9000/flush on the worker
// ---------------------------------------------------------------------------

function callWorkerFlush() {
    /**
     * Sends POST http://localhost:9000/flush to the tinySML worker.
     * The worker responds only after it has serialized and POSTed its full state.
     * We wrap the http.request in a Promise so we can await it cleanly.
     */
    return new Promise((resolve) => {
        const options = {
            hostname: 'localhost',
            port:     9000,
            path:     '/flush',
            method:   'POST',
            headers:  { 'Content-Length': 0 }
        };

        const req = http.request(options, (res) => {
            // Drain the response body (we don't need to read it)
            res.on('data', () => {});
            res.on('end', resolve);
        });

        req.on('error', (e) => {
            // Worker may be temporarily unavailable — log and continue
            console.log(`⚠️  GUARDIAN: Worker flush request failed: ${e.message}`);
            resolve();  // Resolve anyway so the migration can still proceed
        });

        // 5-second timeout: if the worker doesn't respond, proceed without it
        req.setTimeout(5000, () => { req.destroy(); resolve(); });

        req.end();
    });
}


// ---------------------------------------------------------------------------
// BOOT SEQUENCE
// ---------------------------------------------------------------------------
startServer();
initRedisSub();
setupWatcher();