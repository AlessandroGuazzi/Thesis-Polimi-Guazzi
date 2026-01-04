#!/bin/bash
# 1. Build immagine docker
docker build -t localhost/space-counter:latest .
# 2. Carica in minikube (se usi driver docker)
minikube image load localhost/space-counter:latest
# 3. Deploy
kubectl delete pod counter-pod --ignore-not-found=true --now