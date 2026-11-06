#!/usr/bin/env bash
# Exact global intake-unit owners read from the public Nervix description.

read_instance_owners() {
    local description="$1"
    local output="$2"
    awk '/^instance [0-9]+: node=node-[123] status=running ready=true$/ {
        sub(/:$/, "", $2); split($3, node, "="); print $2 " " node[2]
    }' "${description}" | jq -Rn '[inputs | split(" ") | {index: (.[0] | tonumber), owner: .[1]}] | sort_by(.index)' >"${output}" || return 1
    jq -e 'map(.index) == [0,1,2]' "${output}" >/dev/null
}

verify_spread_instance_start() {
    read_instance_owners "$1" "$2" || return 1
    jq -e 'map(.owner) | unique | length == 3' "$2" >/dev/null
}

describe_spread_instances_ready() {
    local description="$1"
    local owners="$2"
    domain_cli_command 'DESCRIBE INGESTOR chaos_ingestor;' >"${description}" 2>&1 \
        && verify_spread_instance_start "${description}" "${owners}"
}

instance_source_traffic_ready() {
    local host
    for host in "${node_hosts[@]}"; do
        if metric_is_positive "${artifact_dir}/public/metrics-${host}.txt" \
            'direction="sent"' "physical_node_id=\"node-${host##*-}\"" \
            'relay="chaos_records"' 'target="chaos_ingestor"'; then
            return 0
        fi
    done
    return 1
}

verify_instance_handoff() {
    local before="$1"
    local description="$2"
    local failed_owner="$3"
    local output="$4"
    read_instance_owners "${description}" "${output}" || return 1
    jq -e --slurpfile before "${before}" --arg failed "${failed_owner}" '
        . as $after | all($before[0][]; . as $prior |
            ($after[] | select(.index == $prior.index)) as $now |
            if $prior.owner == $failed then $now.owner != $failed
            else $now.owner == $prior.owner end)
    ' "${output}" >/dev/null
}
