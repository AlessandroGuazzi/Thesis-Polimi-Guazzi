const express = require('express');
const Redis = require('ioredis');
const path = require('path');
const app = express();
const port = 3000;

// =========================================================
// 1. CONNESSIONE AL SYSTEM BUS (Per la Mappa Globale)
// =========================================================
// Questo Redis contiene la telemetria scritta da physics_sim.py
// Il nome host 'system-redis' è risolto dal DNS di Kubernetes
const systemRedis = new Redis({
    host: 'system-redis', 
    port: 6379,
    connectTimeout: 2000,
    lazyConnect: true,
    retryStrategy: (times) => Math.min(times * 100, 3000) // Riprova senza crashare
});

systemRedis.on('error', (err) => {
    console.error("⚠️  System Redis (Telemetry) non raggiungibile:", err.message);
});

// =========================================================
// 2. CONNESSIONE AL SIDECAR (Memoria Locale)
// =========================================================
// Questo Redis viaggia col pod (localhost). Serve per provare che il sidecar è vivo.
const sidecarRedis = new Redis({
    host: 'localhost',
    port: 6379,
    connectTimeout: 1000,
    lazyConnect: true,
    retryStrategy: (times) => Math.min(times * 100, 3000)
});

sidecarRedis.on('error', (err) => {
    console.error("⚠️  Sidecar Redis (Local Memory) non raggiungibile:", err.message);
});

// =========================================================
// 3. SERVER WEB & API
// =========================================================

// Serve l'interfaccia grafica (index.html)
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

// API per il Frontend: Restituisce TUTTO lo stato
app.get('/api/telemetry', async (req, res) => {
    try {
        // A. Leggiamo la telemetria globale dal System Bus
        let fleetData = {};
        try {
            const rawData = await systemRedis.get('fleet_telemetry');
            if (rawData) fleetData = JSON.parse(rawData);
        } catch (e) {
            // Se system-redis fallisce, mandiamo dati vuoti ma non crashiamo
            console.warn("Lettura telemetria fallita");
        }

        // B. Verifichiamo se il Sidecar è vivo (Healthcheck locale)
        let sidecarStatus = "OFFLINE";
        try {
            await sidecarRedis.ping();
            sidecarStatus = "ONLINE (Low Latency)";
        } catch (e) {
            sidecarStatus = "ERROR";
        }

        // C. Chi sono io? (Info dal Downward API di K8s)
        const myPodName = process.env.POD_NAME || 'Unknown-Pod';
        const myNodeName = process.env.NODE_NAME || 'Unknown-Orbit';

        // D. Costruiamo la risposta JSON per la Dashboard
        res.json({
            timestamp: Date.now(),
            pod_info: { 
                pod: myPodName, 
                node: myNodeName,
                memory_status: sidecarStatus
            },
            fleet: fleetData
        });

    } catch (error) {
        console.error("Server Error:", error);
        res.status(500).json({ error: "Internal System Error" });
    }
});

// Avvio Server
app.listen(port, () => {
    console.log(`🚀 Space Cloud Mission Control v2.3 running on port ${port}`);
    console.log(`   👉 Connected to System Bus: system-redis:6379`);
    console.log(`   👉 Connected to Sidecar: localhost:6379`);
});