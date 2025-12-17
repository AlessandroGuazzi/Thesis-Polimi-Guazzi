const express = require('express');
const Redis = require('ioredis');
const app = express();
const port = 3000;

// 1. MEMORIA LOCALE (SIDECAR) - Qui vive lo stato reale
const localMemory = new Redis({
    host: 'localhost',
    port: 6379,
    retryStrategy: times => Math.min(times * 50, 2000)
});

// 2. BUS DI TRASFERIMENTO (Simula la rete per CRIU)
const transferBus = new Redis({
    host: 'system-redis',
    port: 6379,
    lazyConnect: true
});

// --- FASE DI RESTORE (Simulazione CRIU Restore) ---
async function restoreMemoryState() {
    try {
        // Controlliamo se c'è un pacchetto di memoria in transito per me
        // Usiamo il nome del deployment (o un ID condiviso) per trovare il checkpoint
        const checkpoint = await transferBus.get('criu_checkpoint_mission_step');
        
        if (checkpoint) {
            console.log(`♻️  CRIU RESTORE: Trovato stato precedente (${checkpoint}). Caricamento in RAM...`);
            await localMemory.set('mission_step', checkpoint);
            // Opzionale: Puliamo il checkpoint per evitare reload doppi
            // await transferBus.del('criu_checkpoint_mission_step'); 
        } else {
            console.log("🆕  COLD START: Nessun checkpoint trovato. Inizio da zero.");
        }
    } catch (e) {
        console.warn("⚠️  Errore durante il Restore CRIU:", e.message);
    }
}

// Avvio Restore
restoreMemoryState();

// --- API ---
app.get('/', (req, res) => {
    res.sendFile(__dirname + '/index.html');
});

app.get('/api/telemetry', async (req, res) => {
    try {
        // A. Incremento su REDIS LOCALE (Sidecar)
        let step = 0;
        try {
            step = await localMemory.incr('mission_step');
        } catch (e) { step = -1; }

        // B. Lettura Telemetria Globale (Solo lettura)
        let fleet = {};
        try {
            const raw = await transferBus.get('fleet_telemetry');
            if (raw) fleet = JSON.parse(raw);
        } catch (e) {}

        // C. Info Pod
        const myNode = process.env.NODE_NAME || 'Unknown';
        
        res.json({
            mission_step: step,
            pod_info: { 
                node: myNode,
                memory_status: step > 0 ? "ONLINE (Local)" : "OFFLINE"
            },
            fleet: fleet
        });

    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.listen(port, () => console.log(`🚀 App listening on port ${port}`));