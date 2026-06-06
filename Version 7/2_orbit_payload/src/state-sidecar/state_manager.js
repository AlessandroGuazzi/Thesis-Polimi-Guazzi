// =============================================================================
//  SPACE GUARDIAN V7.1 (Stateful Sidecar — Time-Series CNN Wildfire Tracker)
//  Role: Survives satellite migrations via CRIU. Stores the FIFO T=5 history
//        buffer and the latest prediction mask.
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
// INTERNAL STATE (The Single Source of Truth)
// ---------------------------------------------------------------------------
let guardianMemory = {
    history_frames: [], // FIFO rolling buffer: last 4 past days (7-ch frames, ~610 KB total)
    predicted_fire_mask: [],
    predicted_probability_mask: [],  // 128x128 float [0.0–1.0] continuous probability map
    prev_fire_mask: [],
    center_of_mass: { x: 64, y: 64 },
    fire_pixel_count: 0,
    sample_count: 0,
    migration_epoch: 0,
    status: "WAITING_PAYLOAD",
    last_contact: null,

    // --- NUOVE METRICHE AI (Mission Tracking) ---
    fire_id: 0,
    day_id: 0,
    input_fire_px: 0,
    ai_confidence: 0,
    tracking_iou: 0
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
    const payload = {
        internal_state: guardianMemory,
        environment: fleetState,
        flight_mode: flightMode,
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
    res.json(guardianMemory);
});

// IL WORKER PYTHON INVIA I DATI QUI
app.post('/state', (req, res) => {
    const { new_frame, metrics } = req.body;
    if (new_frame && metrics) {
        // FIRE ISOLATION: Reset FIFO buffer when a new fire mission begins
        const incomingFireId = metrics.fire_id || 0;
        if (incomingFireId !== guardianMemory.fire_id && guardianMemory.fire_id !== 0) {
            console.log(`🔥 GUARDIAN: New fire mission detected (${guardianMemory.fire_id} → ${incomingFireId}). Flushing history buffer.`);
            guardianMemory.history_frames = [];
        }

        guardianMemory.history_frames.push(new_frame);
        if (guardianMemory.history_frames.length > 4) {
            guardianMemory.history_frames.shift(); // FIFO T=4 (Worker brings the 5th day)
        }

        guardianMemory.prev_fire_mask = metrics.prev_fire_mask || [];
        guardianMemory.predicted_fire_mask = metrics.predicted_fire_mask || [];
        guardianMemory.predicted_probability_mask = metrics.predicted_probability_mask || [];
        guardianMemory.center_of_mass = metrics.center_of_mass || { x: 64, y: 64 };
        guardianMemory.fire_pixel_count = metrics.fire_pixel_count || 0;
        guardianMemory.sample_count = (metrics.sample_count !== undefined) ? metrics.sample_count : (guardianMemory.sample_count + 1);

        // ESTRAZIONE NUOVI DATI MISSIONE
        guardianMemory.fire_id = metrics.fire_id || 0;
        guardianMemory.day_id = metrics.day_id || 0;
        guardianMemory.input_fire_px = metrics.input_fire_px || 0;
        guardianMemory.ai_confidence = metrics.ai_confidence || 0;
        guardianMemory.tracking_iou = metrics.tracking_iou || 0;

        guardianMemory.status = "TRACKING_ACTIVE";
        guardianMemory.last_contact = Date.now();

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
        console.log(`🚀 Guardian V7.1 Sidecar listening on port ${PORT}`);
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

        const agentState = {
            center_of_mass: guardianMemory.center_of_mass || { x: 64, y: 64 },
            fire_pixel_count: guardianMemory.fire_pixel_count || 0,
            last_migration_time: payloadLastLandedTime
        };

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