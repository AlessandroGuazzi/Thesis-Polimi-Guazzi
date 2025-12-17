# Usa un'immagine leggera di Node.js
FROM node:18-alpine

# Imposta la directory di lavoro
WORKDIR /app

# Inizializza il progetto e installa le dipendenze
# Usiamo 'ioredis' per connetterci a Redis e leggere la telemetria
RUN npm init -y && npm install ioredis express

# Copia i file dell'applicazione
COPY server.js .
COPY index.html .

# Espone la porta
EXPOSE 3000

# Avvia il server
CMD ["node", "server.js"]