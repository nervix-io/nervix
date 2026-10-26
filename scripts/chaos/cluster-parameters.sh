#!/usr/bin/env bash
# The external harness's requested Nervix policy, installed through Plexus after cluster readiness.
# Node launches carry local prerequisites and capabilities. Restarts keep the committed policy.

configure_cluster_parameters() {
    local replicas="${CHAOS_REPLICA_COUNT:-0}"
    local snapshot="${CHAOS_STATE_SNAPSHOT_INTERVAL:-30s}"
    local unavailable="${CHAOS_NODE_UNAVAILABILITY_TIMEOUT:-10s}"
    local retry="${CHAOS_COMMAND_RETRY_VALIDITY:-15m}"
    [[ "${replicas}" =~ ^[0-9]+$ ]] || {
        printf 'replica count must be an integer\n' >&2
        return 2
    }
    local duration
    for duration in "${snapshot}" "${unavailable}" "${retry}"; do
        [[ "${duration}" =~ ^[0-9][0-9[:space:].a-zA-Z]*$ ]] || {
            printf 'cluster parameter duration must contain only numeric duration text\n' >&2
            return 2
        }
    done
    cli_command "ALTER CLUSTER SET REPLICA_COUNT = ${replicas}, STATE_SNAPSHOT_INTERVAL = ${snapshot}, NODE_UNAVAILABILITY_TIMEOUT = ${unavailable}, COMMAND_RETRY_VALIDITY = ${retry};" || return $?
    cli_command 'DESCRIBE CLUSTER;'
}
