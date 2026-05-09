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

let isFlushing  = false;
let flushLocked = false;  // FIX (Change 1.3): Blocks lightweight POSTs after a full flush is received

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
let payloadLastLandedTime = Date.now() / 1000;


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
            // TODO Push the updated fleet snapshot to all dashboard browsers
            //broadcastToClients();
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
    if (flightMode) {
        console.log("⚠️ GUARDIAN: Rejecting /state POST — flightMode is ACTIVE.");
        return res.status(503).json({ error: "MIGRATION_IN_PROGRESS" });
    }

    const isFullFlush = req.body.is_full_flush === true;

    // FIX (Change 1.3): Defense-in-depth against the amnesia bug.
    // After the massive flush payload is accepted, reject all subsequent lightweight
    // POSTs from the periodic sync thread. This prevents the 2-second sync cycle
    // from overwriting guardianMemory (and erasing STM/LTM) before CRIU freezes.
    if (flushLocked && !isFullFlush) {
        console.log("🔒 GUARDIAN: Rejecting lightweight POST — flush payload is locked in memory.");
        return res.status(409).json({ error: "FLUSH_LOCKED" });
    }

    // =========================================================================
    // FIX: DEFENSIVE MERGE (replaces the former destructive spread operator)
    // =========================================================================
    // Invariant: lightweight periodic syncs must NEVER delete the stm_X/stm_y/
    // ltm_X/ltm_y keys from guardianMemory. Those keys are written exclusively
    // by a full flush (is_full_flush === true) and must survive until CRIU freeze.
    //
    // Strategy:
    //   Full flush  → wholesale replace (payload is fully authoritative).
    //   Lightweight → merge only telemetry keys; existing heavy arrays are
    //                 preserved by keeping them from the previous guardianMemory.
    // =========================================================================
    const PROTECTED_KEYS = ['stm_X', 'stm_y', 'ltm_X', 'ltm_y', 'eval_state'];

    if (isFullFlush) {
        // Full flush: replace everything — the payload contains all keys
        guardianMemory = { ...req.body, last_contact: Date.now() };
    } else {
        // Lightweight sync: strip any protected keys from the incoming body
        // (they should not be present, but this is defense-in-depth), then
        // merge over the existing guardianMemory so heavy arrays are preserved.
        const incoming = { ...req.body, last_contact: Date.now() };
        for (const key of PROTECTED_KEYS) {
            delete incoming[key];
        }
        guardianMemory = { ...guardianMemory, ...incoming };
    }

    const stmSize = guardianMemory.stm_size ?? 0;
    const ltmSize = guardianMemory.ltm_size ?? 0;

    // ----- THE DEMULTIPLEXING SYNCHRONIZATION GATE -----
    const fs = require('fs');
    if (fs.existsSync('/tmp/flush_state')) {
        if (isFullFlush) {
            console.log(`✅ GUARDIAN: Massive pre-freeze payload verified (STM: ${stmSize}). Signaling Agent.`);
            flushLocked = true;  // FIX (Change 1.3): Lock memory — no more lightweight overwrites
            try { fs.writeFileSync('/tmp/flush_complete', ''); } catch (e) {}
        } else {
            // Log the collision but DO NOT write flush_complete!
            console.log("⏳ GUARDIAN: Received periodic lightweight sync. Ignoring. Awaiting massive flush payload...");
        }
    } else {
        // Normal periodic logging when no migration is pending
        console.log(`💾 GUARDIAN: Periodic Sync. STM: ${stmSize} | LTM: ${ltmSize}`);
    }

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
        flight_mode:    flightMode,
        migration_epoch: (guardianMemory.eval_metrics || {}).migration_epoch || 0
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

// ---------------------------------------------------------------------------
// LANDING RECOVERY WATCHER
// Polls for '/tmp/landed' which the Node Agent writes after CRIU restore
// ---------------------------------------------------------------------------

setInterval(() => {
    const fs = require('fs');
    if (flightMode && fs.existsSync('/tmp/landed')) {
        console.log("✅ LANDING CONFIRMED! Reanimating Guardian...");
        try { fs.unlinkSync('/tmp/landed'); } catch (e) {}

        flightMode  = false;
        isFlushing  = false;
        flushLocked = false;  // FIX (Change 1.3): Unlock for the next migration cycle

        // Reset the migration cooldown timer exactly when we wake up on the new node
        payloadLastLandedTime = Date.now() / 1000;

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
     * Monitors /tmp for the CRIU preparation trigger.
     * Note: The flush_state handshake is now securely handled by the setInterval
     * loop to prevent asynchronous event-loop race conditions.
     */

    // FIX (Change 1.4): Only clean flush_complete and prepare_jump unconditionally.
    // flush_state is only removed if no active flush is pending — otherwise the Guardian
    // could delete a trigger the Node Agent just wrote for the next migration, causing
    // a 25s timeout before the satellite can leave.
    const FILES_TO_CLEAN = ['/tmp/flush_complete', '/tmp/prepare_jump'];
    FILES_TO_CLEAN.forEach(f => {
        if (fs.existsSync(f)) {
            try { fs.unlinkSync(f); } catch(e){}
        }
    });
    if (!isFlushing && fs.existsSync('/tmp/flush_state')) {
        try { fs.unlinkSync('/tmp/flush_state'); } catch(e){}
    }

    try {
        dirWatcher = fs.watch('/tmp', async (eventType, filename) => {
            // ----------------------------------------------------------------
            // TRIGGER: Graceful Freeze (CRIU Preparation)
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
                    console.log("💤 GUARDIAN: Disconnecting all systems for CRIU...");

                    // Close all SSE browser streams gracefully
                    sseClients.forEach(c => c.end());
                    sseClients = [];

                    // Forcefully destroy remaining TCP sockets to avoid kernel state issues
                    for (const socket of activeSockets) socket.destroy();
                    activeSockets.clear();

                    // Shut down the Express server
                    if (serverInstance) serverInstance.close(() => serverInstance = null);

                    // Disconnect gracefully from Ground Redis
                    if (redisSubscriber) {
                        try { await redisSubscriber.quit(); } catch (e) {}
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


// ---------------------------------------------------------------------------
// WORKER FLUSH HELPER
// Makes a synchronous-style HTTP POST to localhost:9000/flush on the worker
// ---------------------------------------------------------------------------

function callWorkerFlush() {
    console.log("🚨 GUARDIAN: flush_state detected — calling worker /flush...");

    const options = {
        hostname: 'localhost',
        port:     9000,
        path:     '/flush',
        method:   'POST',
        headers:  { 'Content-Length': 0 }
    };

    const req = http.request(options, (res) => {
        // We only care that the worker acknowledged the command.
        // We DO NOT write flush_complete here.
        res.on('data', () => {});
        res.on('end', () => {
             console.log("✅ GUARDIAN: Worker acknowledged flush command. Awaiting payload...");
        });
    });

    req.on('error', (e) => {
        console.log(`⚠️ GUARDIAN: Worker /flush trigger failed: ${e.message}`);
        // Failsafe: If the worker is dead, we must release the Agent's lock
        // to prevent the entire node from stalling during a thermal emergency.
        require('fs').writeFileSync('/tmp/flush_complete', '');
    });

    // 25-second timeout to accommodate CPU-throttled serialization
    req.setTimeout(25000, () => {
        console.log(`⚠️ GUARDIAN: Worker /flush trigger timed out!`);
        req.destroy();
        require('fs').writeFileSync('/tmp/flush_complete', '');
    });

    req.end();
}


// ---------------------------------------------------------------------------
// BOOT SEQUENCE
// ---------------------------------------------------------------------------
startServer();
initRedisSub();
setupWatcher();

// The Pacemaker: Broadcasts to the browser exactly once per second
setInterval(() => {
    if (!flightMode) {
        broadcastToClients();

        // ASYNC IPC: Dump the state for the Node Agent to read instantly
        const agentState = {
            center_of_mass: guardianMemory.center_of_mass || {x: 32, y: 32},
            fire_pixel_count: guardianMemory.fire_pixel_count || 0,
            last_migration_time: payloadLastLandedTime
        };

        // Write non-blockingly to the shared /tmp volume
        const fs = require('fs');
        fs.writeFile('/tmp/payload_state.json', JSON.stringify(agentState), (err) => {
            // Silently ignore write collisions
        });

        // ---------------------------------------------------------
        // NEW: THE SYNCHRONIZATION GATE (Handshake Trigger)
        // ---------------------------------------------------------
        if (fs.existsSync('/tmp/flush_state') && !isFlushing) {
            console.log("🚨 GUARDIAN: Node Agent requested pre-freeze flush.");
            isFlushing = true; // Lock the trigger to prevent spamming the worker
            callWorkerFlush();
        }

        // Release the lock automatically when the Agent deletes the trigger file
        // (which happens right after the Guardian writes flush_complete)
        if (!fs.existsSync('/tmp/flush_state')) {
            isFlushing = false;
        }
    }
}, 500);