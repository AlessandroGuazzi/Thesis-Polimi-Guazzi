// =============================================================================
//  SPACE GUARDIAN V7.2 (Multi-Tenant Sidecar — Spatial Multiplexing Edition)
//  Role: Survives satellite migrations via CRIU. Stores a FIFO T=5 history
//        buffer PER FIRE MISSION in a fleetMemory dictionary.
//        Supports interleaved LEO orbital data streams from multiple fires.
//        Provides the pre-freeze flush handshake, Ground Redis subscriber,
//        and the SSE dashboard stream with AI Confidence metrics.
// =============================================================================

const express = require('express');
const redis = require('redis');
const fs = require('fs');
const path = require('path');
const bodyParser = require('body-parser');

const app = express();
const PORT = 80;

app.use(bodyParser.json({ limit: '50mb' }));

// --- DASHBOARD STATIC ASSET DELIVERY ---
app.use(express.static(path.join(__dirname, 'dashboard-ui')));

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'dashboard-ui', 'index.html'));
});

// ---------------------------------------------------------------------------
// INTERNAL STATE (Multi-Tenant Fleet Memory)
// ---------------------------------------------------------------------------
// Each fire_id gets its own isolated state object in this dictionary.
// CRIU will snapshot the entire fleetMemory during migration.
let fleetMemory = {};  // { fire_id: { history_frames, predicted_fire_mask, ... } }
let lastUpdatedFireId = null;  // Pointer to the most recently updated fire
let migrationEpoch = 0;

// Factory: creates a fresh, empty state object for a new fire mission
function createMissionState(fireId) {
    return {
        history_frames: [],
        predicted_fire_mask: [],
        predicted_probability_mask: [],
        prev_fire_mask: [],
        center_of_mass: { x: 64, y: 64 },
        fire_pixel_count: 0,
        sample_count: 0,
        status: "WAITING_PAYLOAD",
        last_contact: null,
        fire_id: fireId,
        day_id: 0,
        input_fire_px: 0,
        ai_confidence: 0,
        tracking_iou: 0
    };
}

let flightMode = false;
let serverInstance = null;
let activeSockets = new Set();
let dirWatcher = null;
let fleetState = {};
let sseClients = [];
let redisSubscriber = null;
let payloadLastLandedTime = Date.now() / 1000;

// ---------------------------------------------------------------------------
// GROUND TELEMETRY (REDIS BUS)
// ---------------------------------------------------------------------------
function initRedisSub() {
    if (redisSubscriber) {
        redisSubscriber.quit();
    }
    redisSubscriber = redis.createClient({ url: 'redis://ground-redis.default.svc.cluster.local:6379' });
    redisSubscriber.on('error', (err) => console.log('Redis Client Error', err));
    redisSubscriber.connect().then(() => {
        redisSubscriber.pSubscribe('telemetry/*', (message, channel) => {
            try {
                const nodeName = channel.split('/')[1];
                fleetState[nodeName] = JSON.parse(message);
            } catch (e) { }
        });
    });
}

// ---------------------------------------------------------------------------
// SERVER / SSE
// ---------------------------------------------------------------------------
function broadcastToClients() {
    // Send the state of the most recently updated fire mission to the dashboard.
    // Also include a lightweight summary of all active missions.
    let liteState = {
        status: "WAITING_PAYLOAD",
        fire_id: 0, day_id: 0, input_fire_px: 0,
        ai_confidence: 0, tracking_iou: 0,
        fire_pixel_count: 0, sample_count: 0,
        center_of_mass: { x: 64, y: 64 },
        predicted_fire_mask: [], predicted_probability_mask: [], prev_fire_mask: [],
        history_count: 0
    };

    if (lastUpdatedFireId !== null && fleetMemory[lastUpdatedFireId]) {
        const mem = fleetMemory[lastUpdatedFireId];
        liteState = Object.assign({}, mem, {
            history_count: mem.history_frames.length
        });
        delete liteState.history_frames;
    }

    // Build a lightweight list of all tracked missions for the dashboard
    // IMPORTANT: We do NOT spread fleetMemory[fid].internal_state here because
    // it contains massive 128x128 arrays for every single fire, which causes 
    // a 15MB JSON payload that freezes the dashboard.
    const activeMissions = Object.keys(fleetMemory).map(fid => ({
        fire_id: fleetMemory[fid].fire_id,
        day_id: fleetMemory[fid].day_id,
        history_count: fleetMemory[fid].history_frames.length,
        status: fleetMemory[fid].status,
        input_fire_px: fleetMemory[fid].internal_state ? fleetMemory[fid].internal_state.input_fire_px : 0,
        fire_pixel_count: fleetMemory[fid].internal_state ? fleetMemory[fid].internal_state.fire_pixel_count : 0,
        ai_confidence: fleetMemory[fid].internal_state ? fleetMemory[fid].internal_state.ai_confidence : 0,
        tracking_iou: fleetMemory[fid].internal_state ? fleetMemory[fid].internal_state.tracking_iou : 0
    }));

    const payload = {
        internal_state: liteState,
        active_missions: activeMissions,
        active_mission_count: activeMissions.length,
        environment: fleetState,
        flight_mode: flightMode,
        migration_epoch: migrationEpoch,
        guardian_id: process.env.HOSTNAME || "GUARDIAN-UNKNOWN"
    };

    const dataStr = `data: ${JSON.stringify(payload)}\n\n`;
    sseClients.forEach(client => {
        client.res.write(dataStr);
    });
}

app.get('/api/stream', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    sseClients.push({ req, res });
    req.on('close', () => { sseClients = sseClients.filter(c => c.res !== res); });
});

app.get('/state', (req, res) => {
    res.json({
        fleet_memory: fleetMemory,
        last_updated_fire_id: lastUpdatedFireId,
        migration_epoch: migrationEpoch
    });
});

// Dedicated lightweight endpoint: returns only the history frames and sample_count.
// Used by the Python worker before inference to avoid parsing heavy prediction masks.
// SPATIAL MULTIPLEXING: Each fire_id has its own isolated buffer. No flushing needed.
app.get('/state/history', (req, res) => {
    const incomingFireId = parseInt(req.query.fire_id, 10) || 0;
    // If this fire_id doesn't exist yet, initialize a fresh state for it
    if (!fleetMemory[incomingFireId]) {
        console.log(`🆕 GUARDIAN: New fire mission ${incomingFireId} registered in fleet memory.`);
        fleetMemory[incomingFireId] = createMissionState(incomingFireId);
    }
    const missionState = fleetMemory[incomingFireId];
    res.json({
        history_frames: missionState.history_frames,
        sample_count: missionState.sample_count
    });
});

// IL WORKER PYTHON INVIA I DATI QUI (Multi-Tenant: routes to correct fire state)
app.post('/state', (req, res) => {
    const { new_frame, metrics } = req.body;
    if (new_frame && metrics) {
        const fireId = metrics.fire_id || 0;

        // Ensure this fire has a state object
        if (!fleetMemory[fireId]) {
            fleetMemory[fireId] = createMissionState(fireId);
        }
        const mem = fleetMemory[fireId];

        mem.history_frames.push(new_frame);
        if (mem.history_frames.length > 4) {
            mem.history_frames.shift(); // FIFO T=4 (Worker brings the 5th day)
        }

        mem.prev_fire_mask = metrics.prev_fire_mask || [];
        mem.predicted_fire_mask = metrics.predicted_fire_mask || [];
        mem.predicted_probability_mask = metrics.predicted_probability_mask || [];
        mem.center_of_mass = metrics.center_of_mass || { x: 64, y: 64 };
        mem.fire_pixel_count = metrics.fire_pixel_count || 0;
        mem.sample_count = (metrics.sample_count !== undefined) ? metrics.sample_count : (mem.sample_count + 1);

        mem.fire_id = fireId;
        mem.day_id = metrics.day_id || 0;
        mem.input_fire_px = metrics.input_fire_px || 0;
        mem.ai_confidence = metrics.ai_confidence || 0;
        mem.tracking_iou = metrics.tracking_iou || 0;

        mem.status = "TRACKING_ACTIVE";
        mem.last_contact = Date.now();

        // Update the global pointer so the dashboard knows which fire just reported
        lastUpdatedFireId = fireId;

        // Event-driven broadcast: push to dashboard only when new data arrives
        broadcastToClients();

        res.sendStatus(200);
    } else {
        res.sendStatus(400);
    }
});

app.on('connection', (socket) => {
    activeSockets.add(socket);
    socket.on('close', () => activeSockets.delete(socket));
});

function startServer() {
    if (serverInstance) serverInstance.close();
    serverInstance = app.listen(PORT, () => {
        console.log(`🚀 Guardian V7.2 Multi-Tenant Sidecar listening on port ${PORT}`);
    });
}

// ---------------------------------------------------------------------------
// CRIU MIGRATION WATCHERS
// ---------------------------------------------------------------------------
function setupWatcher() {
    try {
        if (dirWatcher) dirWatcher.close();
        dirWatcher = fs.watch('/tmp', (eventType, filename) => {
            if (filename === 'freeze_signal' && !flightMode) {
                console.log("❄️ GUARDIAN: FREEZE SIGNAL DETECTED. PREPARING FOR MIGRATION...");
                flightMode = true;
                if (serverInstance) {
                    serverInstance.close(() => console.log("Guardian HTTP Server closed."));
                }
                for (const socket of activeSockets) socket.destroy();
                activeSockets.clear();
                if (redisSubscriber) {
                    redisSubscriber.quit();
                    redisSubscriber = null;
                }
                fs.writeFileSync('/tmp/freeze_ack', 'ACK');
                console.log("❄️ GUARDIAN: ACK sent. Ready to be frozen by CRIU.");
            }
        });
    } catch (e) {
        console.log("⚠️ GUARDIAN: fs.watch error:", e.message);
    }
}

// LANDING RECOVERY WATCHER
setInterval(() => {
    if (flightMode && fs.existsSync('/tmp/landed')) {
        console.log("✅ LANDING CONFIRMED! Reanimating Guardian...");
        try { fs.unlinkSync('/tmp/landed'); } catch (e) { }

        flightMode = false;
        payloadLastLandedTime = Date.now() / 1000;
        migrationEpoch += 1;

        startServer();
        initRedisSub();
        setupWatcher();
    }
}, 1000);

// BOOT
startServer();
initRedisSub();
setupWatcher();

// HOUSEKEEPING (500ms) – lightweight file I/O only, NO SSE broadcast
setInterval(() => {
    if (!flightMode) {
        // Report the last updated fire's metrics for the node agent
        let agentState = {
            center_of_mass: { x: 64, y: 64 },
            fire_pixel_count: 0,
            last_migration_time: payloadLastLandedTime,
            active_missions: Object.keys(fleetMemory).length
        };
        if (lastUpdatedFireId !== null && fleetMemory[lastUpdatedFireId]) {
            agentState.center_of_mass = fleetMemory[lastUpdatedFireId].center_of_mass || { x: 64, y: 64 };
            agentState.fire_pixel_count = fleetMemory[lastUpdatedFireId].fire_pixel_count || 0;
        }

        fs.writeFile('/tmp/payload_state.json', JSON.stringify(agentState), (err) => { });

        if (fs.existsSync('/tmp/flush_state')) {
            console.log("🚨 GUARDIAN: Node Agent requested pre-freeze flush. Auto-acknowledging instantly.");
            try {
                fs.unlinkSync('/tmp/flush_state');
                fs.writeFileSync('/tmp/flush_ack', 'ACK');
            } catch (e) { }
        }
    }
}, 500);