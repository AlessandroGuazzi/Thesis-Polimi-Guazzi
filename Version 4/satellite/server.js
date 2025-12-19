const express = require('express');
const Redis = require('ioredis');
const app = express();
const port = 3000;

// ============================================================
// 0. MEMORIA VOLATILE (RAM PURA - IL TEST PER CRIU)
// ============================================================
// Questa variabile vive SOLO nella RAM del processo. 
// Non viene mai salvata su Redis.
// - Se il Pod si riavvia normalmente -> Torna a 0.
// - Se il Restore CRIU funziona -> Continua dal valore precedente.
let criuVolatileCounter = 0;
const nodeName = process.env.NODE_NAME || 'Unknown';

// Incrementa ogni secondo (simula un calcolo in corso)
setInterval(() => {
    criuVolatileCounter++;
    // Log su una sola riga per non sporcare troppo
    //process.stdout.write(`\r[${nodeName}] RAM (CRIU): ${criuVolatileCounter} | Redis: (async)   `);
    console.log(`[${nodeName}] RAM (CRIU): ${criuVolatileCounter} | Status: RUNNING`);
}, 1000);

// ============================================================
// 1. MEMORIA LOCALE (SIDECAR) - Stato Applicativo
// ============================================================
const localMemory = new Redis({
    host: 'localhost',
    port: 6379,
    retryStrategy: times => Math.min(times * 50, 2000)
});

// ============================================================
// 2. BUS DI TRASFERIMENTO - Simulazione Rete
// ============================================================
const transferBus = new Redis({
    host: 'system-redis',
    port: 6379,
    lazyConnect: true
});

// --- FASE DI RESTORE LEGACY (Simulazione Applicativa) ---
// Nota per la tesi: Con CRIU puro, questa parte diventerebbe ridondante
// perché anche la connessione Redis verrebbe ripristinata "aperta".
// La manteniamo per ibridazione e sicurezza.
async function restoreMemoryState() {
    try {
        const checkpoint = await transferBus.get('criu_checkpoint_mission_step');
        
        if (checkpoint) {
            console.log(`\n♻️  APP RESTORE: Trovato stato Redis precedente (${checkpoint}).`);
            await localMemory.set('mission_step', checkpoint);
        } else {
            console.log("\n🆕  COLD START: Nessun checkpoint Redis. Inizio da zero.");
        }
    } catch (e) {
        console.warn("\n⚠️  Errore durante il Restore Redis:", e.message);
    }
}

// Avvio Restore Applicativo
restoreMemoryState();

// ============================================================
// 3. API & ROUTING
// ============================================================
app.get('/', (req, res) => {
    res.sendFile(__dirname + '/index.html');
});

app.get('/api/telemetry', async (req, res) => {
    try {
        // A. Incremento su REDIS LOCALE (Persistenza Dati)
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

        // C. Risposta combinata
        res.json({
            // Stato salvato su DB (Approccio Vecchio)
            mission_step: step, 
            
            // Stato salvato in RAM (Approccio CRIU)
            criu_check: {
                ram_counter: criuVolatileCounter,
                node_id: nodeName,
                status: "RUNNING"
            },

            // Info Pod generiche
            pod_info: { 
                node: nodeName,
                memory_status: step > 0 ? "ONLINE (Local)" : "OFFLINE"
            },
            
            // Stato della flotta
            fleet: fleet
        });

    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.listen(port, () => console.log(`\n🚀 Space App listening on port ${port}`));