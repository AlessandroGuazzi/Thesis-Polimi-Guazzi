#!/bin/bash

# ==========================================================
# SPACE MISSION ENTRYPOINT (LITE VERSION)
# Questo script serve SOLO per il primo avvio (Cold Start).
# Durante la migrazione (Restore), CRI-O bypassa questo script
# e ripristina direttamente la memoria.
# ==========================================================

echo "🚀 SPACE MISSION: COLD START INITIALIZATION"
echo "🆕 Avvio pulito del satellite..."

# Avvia l'applicazione normalmente
exec node server.js