#!/usr/bin/env bash
# Sourced after endpoints-scenario.sh for its placement and client-log helpers. Installs no HTTP
# routes: this scenario owns one typed client domain, idempotent effects and a settlement ledger.

binding_placement() {
    endpoints_placement chaos_binding "$1" "$2" \
        'RELAY chaos_binding_records' 'EMITTER chaos_binding_out'
}

binding_placed_on() {
    local owner="$1"
    local output="$2"
    binding_placement "${node_hosts[0]}" "${output}" || return 1
    jq -e --arg owner "${owner}" 'all(.[]; .owner == $owner)' "${output}.json" >/dev/null
}

binding_moved_off() {
    local output="${binding_case_dir}/placement-during"
    binding_placement "${binding_survivor}" "${output}" || return 1
    jq -e --arg away "${binding_target_node}" \
        'all(.[]; (.owner | test("^node-[0-9]+$")) and .owner != $away)' "${output}.json" >/dev/null
}

binding_failover() {
    local started_ms
    started_ms="$(epoch_ms)"
    wait_for "binding work off ${binding_target_node}" 120 binding_moved_off \
        || recovery_finding "${binding_case_dir}" "binding work stayed on ${binding_target_node} during its fault"
    binding_failover_ms=$(($(epoch_ms) - started_ms))
}

binding_before_fault() {
    binding_fault_started_at="$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)"
    # Keep a tail for after the controller has proved the restart. A fast failover must not let
    # the publisher finish every record before that node returns.
    if [[ "${fault}" == none ]]; then
        binding_release
    else
        printf '320\n' >"${binding_case_dir}/control/hold-binding.next"
        mv "${binding_case_dir}/control/hold-binding.next" "${binding_case_dir}/control/hold-binding"
    fi
}

binding_start() {
    local owner="$1"
    local owner_host="$2"
    endpoints_cli chaos_binding "RELOCATE RELAY chaos_binding_records, EMITTER chaos_binding_out ONTO NODE ${owner} IGNORE PREFERENCES;" \
        >"${artifact_dir}/endpoints/relocate-binding.txt" 2>&1
    wait_for "binding work on ${owner}" 120 binding_placed_on "${owner}" \
        "${artifact_dir}/endpoints/placement-before"
    jq -nc --arg run_id "${run_id}" --argjson count 400 --arg kind binding \
        -f "${script_dir}/fixtures/generate-endpoints.jq" >"${artifact_dir}/endpoints/binding-input.ndjson"
    printf '40\n' >"${artifact_dir}/endpoints/control/hold-binding"
    local seeds=()
    local host
    for host in "${node_hosts[@]}"; do
        seeds+=("http://${host}:47391")
    done
    local seed_list
    seed_list="$(IFS=,; printf '%s' "${seeds[*]}")"
    CHAOS_BINDING_URI="http://${owner_host}:47391" CHAOS_BINDING_SEEDS="${seed_list}" \
        compose up --detach --no-deps endpoint-binding
    wait_for 'binding client completed its pre-fault records' 240 binding_at_hold \
        || endpoints_fail product 'binding client did not reach its pre-fault boundary'
}

run_binding_endpoints() {
    local case_dir="${artifact_dir}/endpoints"
    mkdir -p "${case_dir}/control"
    recovery_traffic_startup "${case_dir}"
    binding_case_dir="${case_dir}"
    binding_failover_ms=null
    binding_fault_started_at=""

    failure_category=product
    phase 'binding endpoints: placement and pre-fault boundary'
    local owner="${node_names[0]}"
    if [[ "${node_count}" == 3 ]]; then
        wait_for 'settled cluster before binding placement' 120 cluster_settled "${case_dir}"
        local leader
        leader="$(<"${case_dir}/leader.txt")"
        local node
        for node in node-3 node-2 node-1; do
            if [[ "${node}" != "${leader}" ]]; then
                owner="${node}"
                break
            fi
        done
    fi
    local owner_host="nervix-${owner##*-}"
    binding_target_node="${owner}"
    binding_survivor="${node_hosts[0]}"
    local host
    for host in "${node_hosts[@]}"; do
        if [[ "${host}" != "${owner_host}" ]]; then
            binding_survivor="${host}"
            break
        fi
    done
    binding_start "${owner}" "${owner_host}"

    local fault_seconds="${outage_seconds}"
    phase "binding endpoints: ${fault}"
    case "${fault}" in
        none) node_fault_begin binding_before_fault ;;
        owner-crash)
            node_fault_kill "${case_dir}" "${owner_host}" "${fault_seconds}" binding_before_fault binding_failover
            ;;
        owner-stop)
            node_fault_stop "${case_dir}" "${owner_host}" "${fault_seconds}" binding_before_fault binding_failover
            ;;
        cluster-restart)
            node_fault_restart_cluster "${case_dir}" "${fault_seconds}" binding_before_fault
            ;;
    esac
    if [[ "${fault}" != none ]]; then
        if [[ "${node_count}" == 3 ]]; then
            wait_for 'settled cluster after the binding fault' 180 cluster_settled "${case_dir}" \
                || recovery_finding "${case_dir}" 'the cluster did not settle after the binding fault'
        fi
        node_fault_check_events "${case_dir}"
    fi
    check_support_containers
    binding_release
    binding_finish "${case_dir}"
    recovery_final_boundary
    capture_all_metrics "${case_dir}"
    jq -n --arg fault "${fault}" --arg owner "${owner}" --argjson nodes "${node_count}" \
        --argjson fault_seconds "${fault_seconds}" --arg started "${binding_fault_started_at}" \
        --argjson failover_ms "${binding_failover_ms}" --slurpfile binding "${case_dir}/verdict-binding.json" \
        '{fault: $fault, topology_nodes: $nodes, owner: $owner, domain: "chaos_binding",
          fault_seconds: (if $fault == "none" then null else $fault_seconds end),
          fault_started_at: $started, failover_ms: $failover_ms, binding: $binding[0]}' \
        >"${artifact_dir}/results/binding-endpoints-progress.json"
}

binding_at_hold() {
    endpoints_read_ledger endpoint-binding "${artifact_dir}/endpoints/binding.ndjson" || return 1
    jq -e -s 'any(.[]; .kind == "outcome" and .sequence == 40 and .result == "completed")
        and all(.[] | select(.kind == "submitted"); .sequence <= 40)' \
        "${artifact_dir}/endpoints/binding.ndjson" >/dev/null
}

binding_release() {
    rm -f "${artifact_dir}/endpoints/control/hold-binding"
}

binding_finish() {
    local case_dir="$1"
    wait_for 'binding client finished its post-fault records' 300 endpoints_client_exited endpoint-binding \
        || endpoints_fail product 'binding client did not finish after the endpoint fault'
    endpoints_read_ledger endpoint-binding "${case_dir}/binding.ndjson" \
        || endpoints_fail controller 'binding client ledger could not be read'
    local container_id
    container_id="$(endpoints_client_container endpoint-binding)"
    run_bounded 20 docker logs "${container_id}" 1>/dev/null 2>"${case_dir}/binding.stderr"
    local status=0
    run_bounded 30 python3 "${script_dir}/binding_evidence.py" \
        --input "${case_dir}/binding-input.ndjson" --ledger "${case_dir}/binding.ndjson" \
        --result "${case_dir}/verdict-binding.json" >"${case_dir}/verdict-binding.txt" 2>&1 || status=$?
    if ((status == 1)); then
        recovery_finding "${case_dir}" "binding: $(jq -r '.failures | join("; ")' "${case_dir}/verdict-binding.json")"
    elif ((status != 0)); then
        endpoints_fail controller 'binding verifier could not judge its evidence'
    fi
}
