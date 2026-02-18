#!/bin/bash
# SPACE GUARDIAN ENTRYPOINT
echo "🛡️  GUARDIAN: Cold Start Initialization..."
# Avvia il gestore dello stato
exec node state_manager.js