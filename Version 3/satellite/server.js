const http = require('http');
const fs = require('fs');
// MODIFICA V3: Usiamo ioredis per il supporto Sentinel
const Redis = require('ioredis');

const port = 3000;
const podName = process.env.POD_NAME || 'Unknown-Pod';
const nodeName = process.env.NODE_NAME || 'Unknown-Node';

// --- CONFIGURAZIONE SENTINEL (V3) ---
// Invece di un solo host, diamo la lista dei Sentinel (gli arbitri).
// Loro ci diranno chi è il vero Master attuale.
const redis = new Redis({
  sentinels: [
    // Usiamo i nomi DNS stabili dello StatefulSet di Kubernetes
    { host: 'satellite-memory-0.redis-sat', port: 26379 },
    { host: 'satellite-memory-1.redis-sat', port: 26379 },
    { host: 'satellite-memory-2.redis-sat', port: 26379 },
  ],
  name: 'mymaster', // Deve combaciare con 'sentinel monitor mymaster' nel config yaml
  
  // Logica di Resilienza (Retry)
  // Se perdiamo la connessione (es. durante un'elezione del leader),
  // riproviamo ogni 100ms fino a un massimo di 2 secondi per tentativo.
  retryStrategy: function (times) {
    const delay = Math.min(times * 50, 2000);
    return delay;
  },
  
  // Non crashare se Redis è temporaneamente giù
  reconnectOnError: function (err) {
    const targetError = 'READONLY';
    if (err.message.includes(targetError)) {
      // Se proviamo a scrivere su un nodo diventato Replica, forziamo la riconnessione
      return true;
    }
    return false;
  }
});

redis.on('connect', () => {
    console.log(`🛰️  Satellite [${podName}] connesso alla Memoria Distribuita (Sentinel)`);
});

redis.on('error', (err) => {
    console.log('⚠️  Redis Cluster Error (in elezione?):', err.message);
});

const requestListener = async (req, res) => {
  if (req.url === '/data') {
    try {
      // Operazione di Scrittura (va sul Master attuale)
      const count = await redis.incr('mission_step');
      
      // Operazione di Lettura
      const telemetryRaw = await redis.get('constellation_telemetry');
      const replicas = await redis.get('mission_replicas') || "1";
      const missionMsg = await redis.get('mission_status_text') || "NOMINAL";

      // Info extra: Chiediamo a Redis chi è lui (per vedere se l'IP cambia quando il master muore)
      // Questo è utile per il debug nella tua tesi
      const info = await redis.info('server');
      const match = info.match(/tcp_port:(\d+)/); 
      // Nota: ioredis astrae l'IP, ma sappiamo che sta parlando col master.

      const telemetry = telemetryRaw ? JSON.parse(telemetryRaw) : {};

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ 
        satellite_pod: podName, 
        satellite_node: nodeName,
        count: count,
        fleet: telemetry,
        status: "OPERATIONAL",
        mission_control: {
            replicas: replicas,
            message: missionMsg
        },
        // Aggiungiamo info su chi è la memoria attuale (Master)
        memory_mode: "DISTRIBUTED / SENTINEL"
      }));
    } catch (e) {
      console.error(e);
      res.writeHead(500);
      res.end(JSON.stringify({ error: "Errore connessione memoria", details: e.message }));
    }
  } else {
    fs.readFile(__dirname + "/index.html", (err, data) => {
      if (err) {
        res.writeHead(500);
        res.end("Errore: index.html mancante.");
        return;
      }
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end(data);
    });
  }
};

const server = http.createServer(requestListener);
server.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});