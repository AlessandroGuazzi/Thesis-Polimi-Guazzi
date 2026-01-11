#!/bin/bash

# ==============================================================================
# SPACE CLOUD V2.0 - RUNTIME UPGRADE PROTOCOL
# Aggiorna CRI-O dalla v1.24 alla v1.28 su tutti i nodi (OS: Ubuntu 22.04)
# ==============================================================================

# Definiamo i nodi da aggiornare
NODES=("minikube" "minikube-m02" "minikube-m03")

# Versione target e OS (fondamentale che sia esatto!)
CRIO_VERSION="1.28"
OS="xUbuntu_22.04"

echo "🔧 INIZIO AGGIORNAMENTO FLOTTA A CRI-O v$CRIO_VERSION..."

for NODE in "${NODES[@]}"; do
    echo "--------------------------------------------------"
    echo "🛰️  Aggiornamento nodo: $NODE"

    # Eseguiamo i comandi come root dentro ogni nodo
    minikube ssh -n $NODE "sudo -i <<EOF
        # 1. Aggiungiamo il repository ufficiale di Kubic (dove vive CRI-O aggiornato)
        echo 'deb https://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable:/cri-o:/$CRIO_VERSION/$OS/ /' > /etc/apt/sources.list.d/devel:kubic:libcontainers:stable:cri-o:$CRIO_VERSION.list

        # 2. Aggiungiamo la chiave di sicurezza GPG
        curl -L https://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable:/cri-o:/$CRIO_VERSION/$OS/Release.key | apt-key add - 2>/dev/null

        # 3. Aggiorniamo la lista pacchetti
        apt-get update -qq

        # 4. Installiamo la nuova versione (sovrascrivendo la vecchia)
        # Usiamo opzioni per non chiedere conferme interattive sui file di config
        DEBIAN_FRONTEND=noninteractive apt-get install -y cri-o cri-o-runc -qq -o Dpkg::Options::='--force-confdef' -o Dpkg::Options::='--force-confold'

        # 5. Verifica rapida
        echo 'Versione installata:'
        crio --version | head -n 1

        # 6. Riavvio dei servizi vitali
        systemctl daemon-reload
        systemctl restart crio
        systemctl restart kubelet
EOF"
    echo "✅ Nodo $NODE aggiornato e riavviato."
done

echo "--------------------------------------------------"
echo "🎉 Aggiornamento completato! Attendi 1 minuto che i nodi tornino 'Ready'."