const http = require('http');
const fs = require('fs');
const { createClient } = require('redis');

const redisHost = process.env.REDIS_HOST || 'redis-sat';
const port = 3000;
const podName = process.env.POD_NAME || 'Unknown-Pod';
const nodeName = process.env.NODE_NAME || 'Unknown-Node';

const client = createClient({ url: `redis://${redisHost}:6379` });
client.on('error', (err) => console.log('Redis Client Error', err));

(async () => {
  await client.connect();
  console.log(`🛰️  Satellite Online [${podName}] su Nodo [${nodeName}]`);
})();

const requestListener = async (req, res) => {
  if (req.url === '/data') {
    try {
      const count = await client.incr('mission_step');
      const telemetryRaw = await client.get('constellation_telemetry');
      
      // NUOVO: Leggi info dal Watchdog
      const replicas = await client.get('mission_replicas') || "1";
      const missionMsg = await client.get('mission_status_text') || "NOMINAL";

      const telemetry = telemetryRaw ? JSON.parse(telemetryRaw) : {};

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ 
        satellite_pod: podName, 
        satellite_node: nodeName,
        count: count,
        fleet: telemetry,
        status: "OPERATIONAL",
        // NUOVO: Dati per la GUI
        mission_control: {
            replicas: replicas,
            message: missionMsg
        }
      }));
    } catch (e) {
      res.writeHead(500);
      res.end(JSON.stringify({ error: e.message }));
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