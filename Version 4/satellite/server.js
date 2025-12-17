const express = require('express');
const Redis = require('ioredis');
const fs = require('fs');
const path = require('path');

const app = express();
const port = 3000;

// Connessione a Redis (dove il Python scrive la fisica)
// Nel cluster Kubernetes, l'host sarà 'redis-service' o 'localhost' se testato con port-forward
const redis = new Redis({
    host: process.env.REDIS_HOST || 'localhost', 
    port: 6379,
    connectTimeout: 2000,
    lazyConnect: true
});

// Serve la pagina HTML
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

// API per il Frontend: Restituisce lo stato della flotta
app.get('/api/telemetry', async (req, res) => {
    try {
        // Legge la stringa JSON scritta da physics_sim.py
        const data = await redis.get('fleet_telemetry');
        
        // Info su dove sta girando QUESTA dashboard (il Pod attivo)
        const myPodName = process.env.POD_NAME || 'Unknown';
        const myNodeName = process.env.NODE_NAME || 'Unknown';

        res.json({
            timestamp: Date.now(),
            pod_info: { pod: myPodName, node: myNodeName },
            fleet: data ? JSON.parse(data) : {}
        });
    } catch (error) {
        console.error("Redis Error:", error);
        res.status(500).json({ error: "Errore recupero telemetria" });
    }
});

// Avvio server
app.listen(port, () => {
    console.log(`🚀 Space Cloud Dashboard v2 attiva su porta ${port}`);
});