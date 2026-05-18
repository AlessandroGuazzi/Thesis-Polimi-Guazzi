#!/bin/bash
# SPACE GUARDIAN ENTRYPOINT

# --- BLOCK 1: COLD START INITIALIZATION ---
# This block is executed only during the first boot of the pod.
# It uses 'exec' to ensure the Node.js process becomes PID 1,
# which is essential for CRIU to correctly identify the target process.
echo "🛡️  GUARDIAN: Cold Start Initialization..."

exec node state_manager.js