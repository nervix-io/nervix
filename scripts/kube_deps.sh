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

repo_add() {
    helm repo add "$1" "$2" >/dev/null
}

wait_deployments() {
    local ns="$1"
    if [ -n "$(kubectl -n "${ns}" get deployment -o name)" ]; then
        kubectl -n "${ns}" wait --for=condition=available deployment --all --timeout=300s
    fi
}

repo_add jetstack https://charts.jetstack.io
repo_add strimzi https://strimzi.io/charts/
repo_add bitnami https://charts.bitnami.com/bitnami
repo_add prometheus-community https://prometheus-community.github.io/helm-charts
repo_add nats https://nats-io.github.io/k8s/helm/charts/
repo_add emqx https://repos.emqx.io/charts
repo_add apache https://pulsar.apache.org/charts
helm repo update >/dev/null

for namespace in "${namespaces[@]}"; do
    kubectl create namespace "${namespace}" --dry-run=client -o yaml | kubectl apply -f -
done

helm upgrade --install cert-manager jetstack/cert-manager \
    --namespace cert-manager \
    --create-namespace \
    --version v1.20.2 \
    --set crds.enabled=true \
    --wait \
    --timeout 300s

kubectl apply -f kube/resources/cert-manager-internal-ca.yaml
kubectl -n cert-manager wait --for=condition=Ready certificate/nervix-internal-ca --timeout=180s
for namespace in "${namespaces[@]}"; do
    kubectl -n "${namespace}" wait --for=condition=Ready certificate --all --timeout=180s
done

helm upgrade --install strimzi-kafka-operator strimzi/strimzi-kafka-operator \
    --namespace strimzi-system \
    --create-namespace \
    --version 1.0.0 \
    --set watchAnyNamespace=true \
    --wait \
    --timeout 300s
kubectl wait --for=condition=Established crd/kafkas.kafka.strimzi.io --timeout=180s
kubectl apply -f kube/resources/kafka.yaml
kubectl -n nervix-kafka wait kafka/kafka --for=condition=Ready --timeout=600s

kubectl apply -f https://github.com/rabbitmq/cluster-operator/releases/download/v2.20.1/cluster-operator.yml
kubectl wait --for=condition=Established crd/rabbitmqclusters.rabbitmq.com --timeout=180s
kubectl -n rabbitmq-system wait --for=condition=available deployment/rabbitmq-cluster-operator --timeout=300s
kubectl apply -f kube/resources/rabbitmq.yaml
kubectl -n nervix-rabbitmq wait rabbitmqcluster/rabbitmq --for=condition=AllReplicasReady --timeout=600s

helm upgrade --install redis bitnami/redis \
    --namespace nervix-redis \
    --version 25.4.1 \
    --values kube/values/redis.yaml \
    --wait \
    --timeout 300s

helm upgrade --install emqx emqx/emqx \
    --namespace nervix-emqx \
    --version 5.8.4 \
    --values kube/values/emqx.yaml \
    --wait \
    --timeout 300s

helm upgrade --install nats nats/nats \
    --namespace nervix-nats \
    --version 2.12.6 \
    --values kube/values/nats.yaml \
    --wait \
    --timeout 300s

helm upgrade --install prometheus prometheus-community/kube-prometheus-stack \
    --namespace nervix-prometheus \
    --version 84.5.0 \
    --values kube/values/prometheus.yaml \
    --wait \
    --timeout 300s

helm upgrade --install pulsar apache/pulsar \
    --namespace nervix-pulsar \
    --version 4.6.0 \
    --values kube/values/pulsar.yaml \
    --wait \
    --timeout 600s

helm upgrade --install elasticmq kube/charts/elasticmq \
    --namespace nervix-elasticmq \
    --wait \
    --timeout 300s

minikube image build -t nervix-mock-server:local -f docker/mock-server/Dockerfile .
helm upgrade --install mock-server kube/charts/mock-server \
    --namespace nervix-mock-server \
    --wait \
    --timeout 300s

for namespace in "${namespaces[@]}"; do
    wait_deployments "${namespace}"
    kubectl -n "${namespace}" wait --for=condition=Ready pod --field-selector=status.phase!=Succeeded --all --timeout=300s
done
