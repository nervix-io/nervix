#!/usr/bin/env bash
set -euo pipefail

namespaces=(
    nervix-kafka
    nervix-rabbitmq
    nervix-redis
    nervix-emqx
    nervix-nats
    nervix-prometheus
    nervix-pulsar
    nervix-elasticmq
    nervix-mock-server
)

kubectl delete -f kube/resources/rabbitmq.yaml --ignore-not-found || true
kubectl delete -f kube/resources/kafka.yaml --ignore-not-found || true
kubectl delete -f kube/resources/cert-manager-internal-ca.yaml --ignore-not-found || true

helm uninstall mock-server --namespace nervix-mock-server >/dev/null 2>&1 || true
helm uninstall elasticmq --namespace nervix-elasticmq >/dev/null 2>&1 || true
helm uninstall pulsar --namespace nervix-pulsar >/dev/null 2>&1 || true
helm uninstall prometheus --namespace nervix-prometheus >/dev/null 2>&1 || true
helm uninstall nats --namespace nervix-nats >/dev/null 2>&1 || true
helm uninstall emqx --namespace nervix-emqx >/dev/null 2>&1 || true
helm uninstall redis --namespace nervix-redis >/dev/null 2>&1 || true

helm uninstall strimzi-kafka-operator --namespace strimzi-system >/dev/null 2>&1 || true
helm uninstall cert-manager --namespace cert-manager >/dev/null 2>&1 || true

kubectl delete -f https://github.com/rabbitmq/cluster-operator/releases/download/v2.20.1/cluster-operator.yml --ignore-not-found || true
kubectl delete namespace "${namespaces[@]}" nervix-deps strimzi-system rabbitmq-system cert-manager --ignore-not-found
