const http = require('http');
const fs = require('fs');
const { createClient } = require('redis');

// Configurazione da Variabili d'Ambiente (Iniettate da K8s)
const redisHost = process.env.REDIS_HOST || 'redis-sat';
const port = 3000;
const podName = process.env.POD_NAME || 'Unknown-Pod';
const nodeName = process.env.NODE_NAME || 'Unknown-Node';

// Connessione al Database Persistente
const client = createClient({ url: `redis://${redisHost}:6379` });
client.on('error', (err) => console.log('Redis Client Error', err));

(async () => {
  await client.connect();
  console.log(`🛰️  Satellite Online [${podName}] su Nodo [${nodeName}]`);
})();

const requestListener = async (req, res) => {
  if (req.url === '/data') {
    try {
      // 1. Incrementa il contatore atomico (sopravvive al riavvio)
      const count = await client.incr('mission_step');
      
      // 2. Legge la telemetria della flotta (scritta da physics_sim.py)
      const telemetryRaw = await client.get('constellation_telemetry');
      const telemetry = telemetryRaw ? JSON.parse(telemetryRaw) : {};

      res.writeHead(200, { 'Content-Type': 'application/json' });
      
      // Risposta JSON completa
      res.end(JSON.stringify({ 
        satellite_pod: podName, 
        satellite_node: nodeName,
        count: count,
        fleet: telemetry,
        status: "OPERATIONAL"
      }));
      
      console.log(`📡 Dato #${count} elaborato da ${podName} @ ${nodeName}`);
    } catch (e) {
      res.writeHead(500);
      res.end(JSON.stringify({ error: e.message }));
    }
  } else {
    // Serve l'interfaccia grafica
    fs.readFile(__dirname + "/index.html", (err, data) => {
      if (err) {
        res.writeHead(500);
        res.end("Errore critico: index.html non trovato nel container.");
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