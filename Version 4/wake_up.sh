#!/bin/bash
echo "🔔 Invio segnale di atterraggio al satellite..."
POD=$(kubectl get pod -l app=space-mission -o jsonpath="{.items[0].metadata.name}")
kubectl exec $POD -- touch /tmp/landed
echo "✅ Satellite svegliato!"