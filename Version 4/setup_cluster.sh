#!/bin/bash

# ==============================================================================
# SPACE CLOUD V6.2 - SETUP INTEGRALE (CRI-O 1.30 + PATCHES)
# Strategia: Start Default -> Upgrade In-Place -> Sysctl Fix -> Runc Wrapper
# ==============================================================================

# --- COLORI PER OUTPUT ---
# Variabili colore per rendere i messaggi più leggibili a terminale
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

# Versione major di CRI-O da installare dai repository ufficiali
CRIO_VERSION_MAJOR="v1.30"

# Messaggio iniziale di avvio setup
echo -e "${BLUE}>>> 🚀 SPACE CLOUD SETUP V6.2 (FULL PATCHED) <<<${NC}"

# 1. AVVIO STANDARD
# ------------------------------------------------------------------------------
# Crea da zero un cluster minikube multi-nodo con CRI-O come runtime
echo -e "${GREEN}[1/4] Avvio Cluster Base (CRI-O 1.24 + Runc)...${NC}"

# Elimina eventuali cluster minikube esistenti
minikube delete --all

# Avvia un nuovo cluster con 4 nodi e configurazione specifica
minikube start \
  --nodes 4 \                       # numero di nodi
  --driver=docker \                 # usa docker come driver
  --container-runtime=cri-o \       # runtime container CRI-O
  --feature-gates=ContainerCheckpoint=true \  # abilita checkpoint container
  --cpus=2 \                        # CPU per nodo
  --memory=2048 \                   # RAM per nodo (MB)
  --profile=minikube                # nome profilo cluster

# Lista dei nodi che verranno patchati uno per uno
NODES=("minikube" "minikube-m02" "minikube-m03" "minikube-m04")

# 2. AGGIORNAMENTO E PATCHING (Cuore del sistema)
# ------------------------------------------------------------------------------
# Aggiorna CRI-O alla 1.30 e applica patch necessarie su ogni nodo
echo -e "${GREEN}[2/4] Aggiornamento CRI-O 1.30 e Applicazione Patch CRIU...${NC}"

# Ciclo su tutti i nodi del cluster
for NODE in "${NODES[@]}"; do
    echo -e "    🛠️  Configurazione nodo: ${BLUE}$NODE${NC}"

    # Esegue comandi root via SSH dentro il nodo minikube
    minikube ssh -n $NODE -p minikube "sudo -i <<EOF

        # --- A. PREPARAZIONE REPO ---
        # Installa strumenti base e configura il repository CRI-O 1.30
        apt-get update -qq
        DEBIAN_FRONTEND=noninteractive apt-get install -y apt-transport-https ca-certificates curl gnupg lsb-release -qq
        mkdir -p /etc/apt/keyrings
        curl -fsSL https://pkgs.k8s.io/addons:/cri-o:/stable:/$CRIO_VERSION_MAJOR/deb/Release.key | gpg --dearmor -o /etc/apt/keyrings/cri-o-apt-keyring.gpg --yes
        echo 'deb [signed-by=/etc/apt/keyrings/cri-o-apt-keyring.gpg] https://pkgs.k8s.io/addons:/cri-o:/stable:/$CRIO_VERSION_MAJOR/deb/ /' | tee /etc/apt/sources.list.d/cri-o.list

        # --- B. STOP SERVIZI E CLEANUP ---
        # Ferma kubelet e crio prima dell’upgrade, rimuove conmon se presente
        systemctl stop kubelet
        systemctl stop crio
        apt-get remove -y conmon >/dev/null 2>&1 || true

        # --- C. INSTALLAZIONE PACCHETTI ---
        # Installa CRI-O aggiornato + criu + buildah forzando eventuali overwrite
        apt-get update -qq
        DEBIAN_FRONTEND=noninteractive apt-get -o Dpkg::Options::='--force-overwrite' install -y criu cri-o buildah -qq

        # --- D. CONFIGURAZIONE CRI-O ---
        # Crea file di configurazione custom per abilitare CRIU e systemd cgroups
        mkdir -p /etc/crio/crio.conf.d
        cat > /etc/crio/crio.conf.d/99-custom.conf <<CONF
[crio.runtime]
enable_criu_support = true          # abilita checkpoint/restore
manage_ns_lifecycle = true          # gestione lifecycle namespace
drop_infra_ctr = false              # non eliminare infra container
default_runtime = \"runc\"           # runtime di default
cgroup_manager = \"systemd\"         # usa systemd per cgroup
CONF

        # --- E. FIX KUBELET ---
        # Allinea il driver cgroup di kubelet a systemd
        sed -i 's/cgroupDriver: cgroupfs/cgroupDriver: systemd/g' /var/lib/kubelet/config.yaml

        # ======================================================================
        # PATCH 1: AUMENTO LIMITI INOTIFY (Per evitare 'Too many open files')
        # ======================================================================
        # Aumenta i limiti inotify per evitare errori su molti file osservati
        echo '>>> Applicazione Patch Sysctl (Inotify)...'
        sysctl -w fs.inotify.max_user_watches=524288
        sysctl -w fs.inotify.max_user_instances=8192

        # Rende i limiti persistenti su eventuale riavvio
        echo 'fs.inotify.max_user_watches=524288' >> /etc/sysctl.conf
        echo 'fs.inotify.max_user_instances=8192' >> /etc/sysctl.conf

        # ======================================================================
        # PATCH 2: RUNC WRAPPER (Per forzare --tcp-established)
        # ======================================================================
        # Sostituisce runc con un wrapper che aggiunge sempre --tcp-established
        echo '>>> Applicazione Patch Runc Wrapper...'
        if [ ! -f /usr/bin/runc.real ]; then
            # 1. Salva il binario originale
            mv /usr/bin/runc /usr/bin/runc.real

            # 2. Crea script wrapper che inoltra i parametri + flag extra
            cat > /usr/bin/runc <<'WRAPPER'
#!/bin/bash
# Wrapper per forzare il salvataggio delle connessioni TCP aperte
exec /usr/bin/runc.real \"\$@\" --tcp-established
WRAPPER

            # 3. Rende eseguibile il wrapper
            chmod +x /usr/bin/runc
        fi

        # --- F. RIAVVIO SERVIZI ---
        # Ricarica systemd e riavvia CRI-O e kubelet
        systemctl daemon-reload
        systemctl start crio
        systemctl start kubelet
EOF"
done

# 3. ATTESA
# ------------------------------------------------------------------------------
# Attende che tutti i servizi si stabilizzino dopo patch e riavvii
echo -e "${GREEN}[3/4] Riavvio servizi e stabilizzazione (45s)...${NC}"
sleep 45

# 4. LABELING & INGRESS
# ------------------------------------------------------------------------------
# Etichetta i nodi e abilita l’ingress controller
echo -e "${GREEN}[4/4] Configurazione Finale (Labels & Ingress)...${NC}"

# Attende che ogni nodo sia visibile via kubectl
for NODE in "${NODES[@]}"; do
    until kubectl get node $NODE >/dev/null 2>&1; do echo "Wait for $NODE..."; sleep 3; done
done

# Assegna label di ruolo ai nodi (ground station / satellite)
kubectl label node minikube type=ground-station --overwrite >/dev/null
kubectl label node minikube-m02 type=satellite --overwrite >/dev/null
kubectl label node minikube-m03 type=satellite --overwrite >/dev/null
kubectl label node minikube-m04 type=satellite --overwrite >/dev/null

# Abilita addon ingress su minikube (se non già attivo)
minikube addons enable ingress -p minikube >/dev/null 2>&1 || true

# Messaggi finali di conferma setup
echo -e "${BLUE}>>> SETUP COMPLETATO CON SUCCESSO <<<${NC}"
echo -e "✅ CRI-O 1.30 Installato"
echo -e "✅ Patch Inotify Applicata"
echo -e "✅ Runc Wrapper (--tcp-established) Attivo"

# Verifica finale: controlla sysctl e wrapper runc su un nodo satellite
echo -e "\n${BLUE}Verifica Finale:${NC}"
minikube ssh -n minikube-m02 "sysctl fs.inotify.max_user_watches && ls -l /usr/bin/runc"
