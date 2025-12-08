# Partiamo da Node
FROM node:18-alpine

# Creiamo la cartella di lavoro
WORKDIR /app

# MODIFICA V3: Installiamo 'ioredis' invece di 'redis'
# ioredis ha il supporto nativo per Sentinel e Failover automatico
RUN npm install ioredis

# Copiamo il codice del server
COPY server.js .
# Copiamo anche l'interfaccia grafica
COPY index.html .

# Comando di avvio
CMD ["node", "server.js"]