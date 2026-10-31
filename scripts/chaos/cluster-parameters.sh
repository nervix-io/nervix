#!/usr/bin/env bash
# The external harness's requested Nervix policy, installed through Plexus after cluster readiness.
# Node launches carry local prerequisites and capabilities. Restarts keep the committed policy.

requested_cluster_parameters() {
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
    jq -cn --arg replicas "${replicas}" --arg snapshot "${snapshot}" \
        --arg unavailable "${unavailable}" --arg retry "${retry}" \
        '{"replica-count":$replicas,"state-snapshot-interval":$snapshot,
          "node-unavailability-timeout":$unavailable,"command-retry-validity":$retry}'
}

configure_cluster_parameters() {
    local parameters statement
    parameters="$(requested_cluster_parameters)" || return $?
    statement="$(jq -r '"ALTER CLUSTER SET " + (to_entries | map("\(.key | gsub("-"; "_") | ascii_upcase) = \(.value)") | join(", ")) + ";"' <<<"${parameters}")" || return $?
    cli_command "${statement}" || return $?
    cli_command 'DESCRIBE CLUSTER;'
}

# Reads the current public description, retaining the complete committed record and each named
# node's running record. A successful observation has one positive revision and identical records.
cluster_parameter_observation() {
    local transcript="${1:?cluster description is required}"
    shift
    local nodes
    nodes="$(printf '%s\n' "$@" | jq -Rsc 'split("\n")[:-1]')" || return $?
    jq -Rse --argjson nodes "${nodes}" '
        split("\n") | map(sub("\r$"; "")) as $lines |
        [$lines[] | capture("^(?<parameter>[a-z][a-z0-9-]*) committed=(?<value>.+) revision=(?<revision>[1-9][0-9]*)$")] as $committed |
        [$lines[] | capture("^node=(?<node>[^ ]+) (?<parameter>[a-z][a-z0-9-]*) running=(?<value>.+) revision=(?<revision>[1-9][0-9]*)$")] as $running |
        ($committed | map({key:.parameter,value:.value}) | from_entries) as $values |
        ($committed | map(.revision) | unique) as $revisions |
        if ($nodes | length) > 0 and all($nodes[]; length > 0)
            and ($nodes | unique | length) == ($nodes | length)
            and ($committed | length) > 0 and ($revisions | length) == 1
            and ($committed | group_by(.parameter) | all(length == 1))
            and (["replica-count","state-snapshot-interval","node-unavailability-timeout","command-retry-validity"] - ($values | keys) | length) == 0
            and (($committed | length) + ($running | length)) == ($lines | map(select(test(" committed=| running="))) | length)
            and ($running | map(.node) | unique | sort) == ($nodes | sort)
            and ($running | group_by([.node,.parameter]) | all(length == 1))
            and all($nodes[]; . as $node |
                ($running | map(select(.node == $node)) | map({key:.parameter,value:.value}) | from_entries) == $values
                and all($running[] | select(.node == $node); .revision == $revisions[0]))
        then {revision:$revisions[0],committed:$values,
              running:($running | group_by(.node) | map({key:.[0].node,
                  value:{revision:.[0].revision,values:(map({key:.parameter,value:.value}) | from_entries)}}) | from_entries)}
        else error("the expected nodes have not installed one complete committed parameter revision") end
    ' "${transcript}"
}

capture_cluster_parameters() {
    local prefix="${1:?parameter evidence prefix is required}"
    shift
    cli_command 'DESCRIBE CLUSTER;' >"${prefix}.txt" 2>&1 || return $?
    cluster_parameter_observation "${prefix}.txt" "$@" >"${prefix}.json.tmp" || return $?
    mv "${prefix}.json.tmp" "${prefix}.json"
}
