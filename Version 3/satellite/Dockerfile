# Partiamo da Node
FROM node:18-alpine

# Creiamo la cartella di lavoro
WORKDIR /app

# Installiamo Redis (dipendenza)
RUN npm install redis

# Copiamo il codice del server
COPY server.js .

# --- NOVITÀ: Copiamo anche l'interfaccia grafica ---
COPY index.html .

# Comando di avvio
CMD ["node", "server.js"]