#!/bin/bash

# ==============================================================================
# MICRO DEMO: KUBERNETES CHECKPOINT & RESTORE
# Metodo: "Build in Place" (Esecuzione diretta nel nodo)
# ==============================================================================

# CONFIGURAZIONE
POD_NAME="counter-pod"
CONTAINER_NAME="main-app"
NAMESPACE="default"
RESTORE_IMG_NAME="localhost/counter-restored:latest"

# Colori
GREEN='\033[0;32m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${CYAN}--- FASE 1: PREPARAZIONE ---${NC}"

# Verifica Pod
if ! kubectl get pod $POD_NAME | grep -q "Running"; then
    echo "❌ Errore: Il pod '$POD_NAME' non è in esecuzione."
    exit 1
fi

# Avvio Proxy K8s
PROXY_PORT=8001
PROXY_URL="http://127.0.0.1:$PROXY_PORT"
if ! curl -s $PROXY_URL/version > /dev/null; then
    echo "⚠️  Proxy non trovato. Avvio..."
    kubectl proxy --port=$PROXY_PORT &
    PROXY_PID=$!
    sleep 2
fi

echo -e "${CYAN}--- FASE 2: CHECKPOINT (API TRIGGER) ---${NC}"
NODE_NAME=$(kubectl get pod $POD_NAME -o jsonpath='{.spec.nodeName}')
API_PATH="/api/v1/nodes/$NODE_NAME/proxy/checkpoint/$NAMESPACE/$POD_NAME/$CONTAINER_NAME"
FULL_URL="$PROXY_URL$API_PATH"

echo "📸 Richiedo congelamento a: $FULL_URL"
RESPONSE=$(curl -X POST -sk "$FULL_URL")

# Estraiamo il percorso remoto
REMOTE_PATH=$(echo "$RESPONSE" | sed -e 's/.*"items":\["//' -e 's/"].*//')

if [[ "$REMOTE_PATH" != *"/var/lib/kubelet/checkpoints"* ]]; then
    echo "❌ Errore API Kubelet: $RESPONSE"
    exit 1
fi
echo "📍 File creato sul nodo: $REMOTE_PATH"

echo -e "${CYAN}--- FASE 3: COSTRUZIONE IMMAGINE (DENTRO IL NODO) ---${NC}"
# Qui avviene la magia: comandiamo al nodo di trasformare il file in immagine
# Usiamo un nome fisso per il container di lavoro per facilità
BUILD_CMD="
sudo buildah from --name restoration-lab scratch > /dev/null
sudo buildah add restoration-lab $REMOTE_PATH /
sudo buildah config --annotation \"io.kubernetes.cri-o.annotations.checkpoint.name=$CONTAINER_NAME\" restoration-lab
sudo buildah commit restoration-lab $RESTORE_IMG_NAME
sudo buildah rm restoration-lab > /dev/null
"

echo "🔨 Esecuzione Buildah nel nodo Minikube..."
minikube ssh "$BUILD_CMD"

echo -e "${GREEN}✅ Immagine $RESTORE_IMG_NAME creata e pronta nel runtime del nodo!${NC}"

echo -e "${CYAN}--- FASE 4: RIPRISTINO (RESTORE) ---${NC}"
echo "💀 Terminazione vecchio pod..."
kubectl delete pod $POD_NAME --grace-period=0 --force > /dev/null 2>&1

echo "🌱 Avvio nuovo pod (Privileged Mode)..."
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: counter-pod
  labels:
    app: counter
spec:
  containers:
  - name: main-app
    image: $RESTORE_IMG_NAME
    imagePullPolicy: Never
    securityContext:
      privileged: true
  nodeName: $NODE_NAME
EOF

echo -e "${GREEN}✅ OPERAZIONE COMPLETATA!${NC}"
echo "👉 Controlla ora: kubectl logs -f $POD_NAME"

# Cleanup Proxy
if [ ! -z "$PROXY_PID" ]; then kill $PROXY_PID; fi