#!/usr/bin/env bash
set -euo pipefail

namespace="nervix"

kubectl create namespace "${namespace}" --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f kube/resources/nervix.yaml
kubectl -n "${namespace}" rollout status statefulset/nervix --timeout=300s
kubectl -n "${namespace}" wait --for=condition=Ready pod \
    --selector=app.kubernetes.io/name=nervix \
    --timeout=300s
