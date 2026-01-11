# CAMBIAMENTO: Usiamo 'node:20-bookworm' invece di 'slim'.
# Bookworm è Debian 12 (più recente) e la versione completa ha tutti i repo per CRIU.
FROM node:20-bookworm

# 1. Installazione tool di sistema per CRIU e Network Restore
# criu: per il ripristino dei processi
# iproute2/iptables: per riconfigurare la rete dopo il ripristino
# procps: per debug (comando ps)
RUN apt-get update && apt-get install -y \
    criu \
    iproute2 \
    iptables \
    procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 2. Dipendenze Software
# express: per la dashboard web
# redis: per leggere i sensori (physics_sim.py)
RUN npm install express redis

# 3. Copia del Codice Sorgente
COPY server.js .
COPY entrypoint.sh .

# 4. Copia dell'Interfaccia Grafica
# (Assicurati che la cartella 'public' esista sul tuo PC!)
COPY public/ ./public/

# 5. Permessi di esecuzione per lo script di avvio
RUN chmod +x entrypoint.sh

# 6. Preparazione cartelle per i checkpoint
RUN mkdir -p /var/lib/kubelet/checkpoints /tmp/restore

# 7. Il container usa lo script entrypoint per decidere come avviarsi
ENTRYPOINT ["/app/entrypoint.sh"]