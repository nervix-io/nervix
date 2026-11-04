#!/usr/bin/env bash
# A complete current public parameter record, shared by the controller's observation checks.

cluster_parameter_publication_fixture() {
    local unavailable="$1"
    shift
    local values
    values="$(cat <<VALUES
node-unavailability-timeout ${unavailable}
domain-drain-timeout 1m
entity-gate-deadline 1m
transaction-idle-timeout 15m
transaction-tombstone-retention 15m
command-retry-validity 15m
state-snapshot-interval 30s
transaction-max-statements 256
transaction-max-source-bytes 1048576B
transaction-max-open 1024
command-execution-capacity 65536
replica-count 0
resource-max-archive-bytes 4294967296B
resource-max-extracted-bytes 17179869184B
resource-max-file-count 1000000
max-batch-messages 65536
client-producer-session-bytes 33554432B
client-consumer-session-bytes 33554432B
udf-code-bytes 65536B
http-header-bytes 32768B
http-header-fields 128
client-batch-rows 65536
VALUES
)"
    local parameter value node
    while read -r parameter value; do
        printf '%s committed=%s revision=17\n' "${parameter}" "${value}"
    done <<<"${values}"
    for node in "$@"; do
        while read -r parameter value; do
            printf 'node=%s %s running=%s revision=17\n' "${node}" "${parameter}" "${value}"
        done <<<"${values}"
    done
}
