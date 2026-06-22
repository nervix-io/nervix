#!/usr/bin/env bash
set -euo pipefail

kubectl delete -f kube/resources/nervix.yaml --ignore-not-found
kubectl delete namespace nervix nervix-deps --ignore-not-found
