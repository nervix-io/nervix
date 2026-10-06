#!/usr/bin/env bash
# Sourced by run-baseline.sh after the external cluster, the baseline graph and the paced domain
# are ready. Follows the paced domain's clock through every node with independent observers that
# run the packaged CLI, drives two interleaved branches through a window that closes on logical
# deadlines, faults every voter in turn or the whole cluster, and judges the public clock, tick
# progress and window output against the documented domain-time guarantees.

# shellcheck source=cluster-restart-scenario.sh
source "${script_dir}/cluster-restart-scenario.sh"
# shellcheck source=node-faults.sh
source "${script_dir}/node-faults.sh"

domain_time_entities=(
    'INGESTOR chaos_paced_ingestor' 'RELAY chaos_paced_records'
    'WINDOW PROCESSOR chaos_paced_window_processor' 'RELAY chaos_paced_windows'
    'EMITTER chaos_paced_emitter'
)
# The paced domain runs at four times physical time with a one-second period; its windows are
# eight logical seconds wide. These match fixtures/paced.nspl and its START statement.
domain_time_rate=4
domain_time_period_ms=1000
domain_time_width_ms=8000
domain_time_fixture_records=4000
# External budgets, in physical seconds.
domain_time_observe_seconds=20
domain_time_settle_bound=150
domain_time_delivery_bound=60
domain_time_placement_bound=120
domain_time_attach_bound=120

domain_time_fail() {
    recovery_fail "$@"
}

domain_time_fault_seconds() {
    if [[ "${outage_option_set}" == true ]]; then
        printf '%s\n' "${outage_seconds}"
        return 0
    fi
    case "${fault}" in
        *-pause | *-partition) printf '30\n' ;;
        *) printf '%s\n' "${outage_seconds}" ;;
    esac
}

# Writes the owner of every paced entity, read through HOST, to OUTPUT_DIR.json.
domain_time_placement() {
    local host="$1"
    local output_dir="$2"
    local pairs=()
    local entity
    for entity in "${domain_time_entities[@]}"; do
        pairs+=("describe-${entity// /_}" "DESCRIBE ${entity};")
    done
    admin_cli_batch "${host}" chaos_paced "${output_dir}" 90 "${pairs[@]}" || return 1
    local owners_file
    owners_file="$(mktemp "${output_dir}/.owners.XXXXXX")"
    for entity in "${domain_time_entities[@]}"; do
        local label="describe-${entity// /_}"
        batch_read_succeeded "${output_dir}" "${label}" || { rm -f "${owners_file}"; return 1; }
        jq -n --arg entity "${entity}" \
            --arg owner "$(awk -F ': ' '$1 == "owner" { print $2; exit }' "${output_dir}/${label}.txt")" \
            '{key: $entity, value: $owner}' >>"${owners_file}"
    done
    jq -s 'from_entries' "${owners_file}" >"${output_dir}.json"
    rm -f "${owners_file}"
}

domain_time_placed_on() {
    local node="$1"
    local host="$2"
    local output_dir="$3"
    domain_time_placement "${host}" "${output_dir}" || return 1
    jq -e --arg node "${node}" 'all(.[]; . == $node)' "${output_dir}.json" >/dev/null
}

# Moves the paced graph onto NODE with one acknowledged RELOCATE and waits until every entity is
# owned there.
domain_time_place() {
    local node="$1"
    local round_dir="$2"
    local selection
    selection="$(printf '%s, ' "${domain_time_entities[@]}")"
    cli_host="${node_hosts[0]}"
    run_cli chaos_paced "RELOCATE ${selection%, } ONTO NODE ${node} IGNORE PREFERENCES;" \
        >"${round_dir}/relocate.txt" 2>&1 \
        || domain_time_fail product "the paced graph could not be relocated onto ${node}"
    wait_for "paced graph owned by ${node}" "${domain_time_placement_bound}" \
        domain_time_placed_on "${node}" "${node_hosts[0]}" "${round_dir}/placement" \
        || domain_time_fail product "the paced graph did not move onto ${node}"
}

domain_time_observer_name() {
    printf '%s-clock-%s\n' "${project_name}" "$1"
}

# Starts one clock observer per node, each following the paced domain through that node's route.
domain_time_start_observers() {
    local host
    for host in "${node_hosts[@]}"; do
        compose run --detach --no-deps --name "$(domain_time_observer_name "${host}")" \
            -e "CHAOS_CLOCK_HOST=${host}" clock-observer >/dev/null
    done
}

# True when every observer has attached and has seen a tick after SINCE_NS.
domain_time_observers_ticking() {
    local since_ns="$1"
    local host
    for host in "${node_hosts[@]}"; do
        run_bounded 20 docker logs "$(domain_time_observer_name "${host}")" 2>&1 \
            | awk -v since="${since_ns}" '$1 >= since && index($0, "] tick: generation ") { found = 1 } END { exit !found }' \
            || return 1
    done
}

domain_time_output_progressed() {
    topic_progressed chaos_paced_output "$1"
}

# True when the paced output stayed unchanged for longer than one window's physical width.
domain_time_output_settled() {
    local first second
    first="$(topic_end_offset chaos_paced_output)" || return 1
    sleep "$(( domain_time_width_ms / domain_time_rate / 1000 + 1 ))"
    second="$(topic_end_offset chaos_paced_output)" || return 1
    [[ "${first}" =~ ^[0-9]+$ && "${first}" == "${second}" ]]
}

# Runs as a fault's BEFORE hook: samples every node's own public status from the moment the fault
# begins, so the round records when the surviving nodes reported the faulted node unavailable.
domain_time_before_fault() {
    start_status_sampler "${domain_time_round_dir}/status-samples.log" \
        "$((domain_time_round_seconds + domain_time_settle_bound))"
}

domain_time_stop_sampler() {
    if [[ -n "${cluster_restart_sampler_pid:-}" ]]; then
        kill "${cluster_restart_sampler_pid}" 2>/dev/null || true
        wait "${cluster_restart_sampler_pid}" 2>/dev/null || true
        cluster_restart_sampler_pid=""
        run_bounded 30 docker container rm --force "${cluster_restart_sampler_name}" >/dev/null 2>&1 || true
    fi
}

# Prints, as one JSON line per answer, how each node that answered its own status route saw
# TARGET: the status of its interconnect entry, empty once the node no longer retains it, and
# whether a warning named it unavailable. A warning from gossip alone does not make a node
# unavailable to the clock; its interconnect entry turning unavailable or leaving does.
domain_time_availability_samples() {
    local transcript="$1"
    local target_node="$2"
    awk -v target="${target_node}" '
        function emit() {
            if (host != "") {
                printf "{\"host\":\"%s\",\"at_ms\":%s,\"node\":\"%s\",\"target_status\":\"%s\",\"marked_unavailable\":%s}\n",
                    host, substr(at, 1, length(at) - 6), node, status, (warned ? "true" : "false")
            }
        }
        /^=== status / {
            emit()
            host = $3; at = $4; node = ""; status = ""; warned = 0; section = ""
            next
        }
        /^raft\.id: / { node = $2; next }
        /^\[/ { section = $0; next }
        section == "[interconnect]" && index($0, "- " target ": ") == 1 {
            for (i = 1; i <= NF; i++) {
                if (index($i, "status=") == 1) { status = substr($i, 8) }
            }
            next
        }
        section == "[warnings]" && index($0, "\047" target "\047") && index($0, "unavailable") { warned = 1 }
        END { emit() }
    ' "${transcript}"
}

# Runs as a fault's DURING hook: the paced graph lives on a node the fault does not touch, so its
# logical deadlines must keep closing windows while the fault holds.
domain_time_during_fault() {
    local before
    before="$(topic_end_offset chaos_paced_output)"
    wait_for 'paced windows kept closing during the fault' "${domain_time_delivery_bound}" \
        domain_time_output_progressed "${before}" \
        || recovery_finding "${domain_time_case_dir}" "paced windows stopped closing while ${domain_time_round_target} was faulted"
}

# Runs as a graceful stop's DURING hook: the stopped node owned the paced graph, so its windows must
# resume on the node that takes the graph over.
domain_time_during_stop() {
    local before
    before="$(topic_end_offset chaos_paced_output)"
    wait_for 'paced windows resumed on another node' "${domain_time_settle_bound}" \
        domain_time_output_progressed "${before}" \
        || recovery_finding "${domain_time_case_dir}" "paced windows did not resume after ${domain_time_round_target} stopped"
}

# Waits until the cluster settles after a fault, every observer ticks again and windows close
# again, and appends the round to rounds.ndjson.
domain_time_recover() {
    local round_dir="$1"
    local ordinal="$2"
    local kind="$3"
    local target="$4"
    local graph_node="$5"
    local ended_ns
    ended_ns="$(date +%s%N)"
    mkdir -p "${round_dir}/recovered"
    wait_for 'every node caught up, connected and executing' "${domain_time_settle_bound}" \
        cluster_settled_and_connected "${round_dir}/recovered" \
        || domain_time_fail product "the cluster did not settle within ${domain_time_settle_bound}s after the fault"
    local settled_ns
    settled_ns="$(date +%s%N)"
    wait_for 'every clock observer ticking after the fault' "${domain_time_attach_bound}" \
        domain_time_observers_ticking "${settled_ns}" \
        || recovery_finding "${domain_time_case_dir}" "a clock observer saw no tick after the fault on ${target}"
    local before
    before="$(topic_end_offset chaos_paced_output)"
    wait_for 'paced windows closing after the fault' "${domain_time_delivery_bound}" \
        domain_time_output_progressed "${before}" \
        || recovery_finding "${domain_time_case_dir}" "paced windows did not close after the fault on ${target}"
    node_fault_check_events "${round_dir}"
    domain_time_stop_sampler
    local unavailable_ms=null
    if [[ -s "${round_dir}/status-samples.log" && "${target}" != all ]]; then
        domain_time_availability_samples "${round_dir}/status-samples.log" "node-${target##*-}" \
            >"${round_dir}/availability.ndjson"
        unavailable_ms="$(jq -s --arg target "node-${target##*-}" '
            [.[] | select(.node != "" and .node != $target
                          and (.target_status == "unavailable" or .target_status == "")) | .at_ms]
            | min // null' "${round_dir}/availability.ndjson")"
    fi
    jq -nc \
        --argjson ordinal "${ordinal}" \
        --arg kind "${kind}" \
        --arg target "${target}" \
        --arg graph_node "${graph_node}" \
        --argjson since_ns "${node_fault_since_ns}" \
        --argjson ended_ns "${ended_ns}" \
        --argjson recovered_ns "$(date +%s%N)" \
        --argjson stop_ms "${node_fault_stop_ms:-null}" \
        --argjson relocation_started_ns "${domain_time_relocation_started_ns}" \
        --argjson relocation_completed_ns "${domain_time_relocation_completed_ns}" \
        --argjson unavailable_ms "${unavailable_ms}" \
        --arg directory "${round_dir#"${artifact_dir}/"}" \
        '{ordinal:$ordinal,kind:$kind,target:$target,graph_node:$graph_node,
          relocation_started_ns:$relocation_started_ns,relocation_completed_ns:$relocation_completed_ns,
          fault_since_ns:$since_ns,fault_ended_ns:$ended_ns,recovered_ns:$recovered_ns,stop_ms:$stop_ms,
          unavailable_ms:$unavailable_ms,directory:$directory}' \
        >>"${domain_time_case_dir}/rounds.ndjson"
    node_fault_stop_ms=""
}

# Faults one voter: the paced graph first moves onto another node, or onto the voter itself for a
# graceful stop, whose shutdown must then never emit a partial window.
domain_time_voter_round() {
    local ordinal="$1"
    local target_host="$2"
    local seconds="$3"
    local kind="${fault#voter-}"
    local round_dir="${domain_time_case_dir}/round-${ordinal}-${kind}-${target_host}"
    mkdir -p "${round_dir}/before"
    phase "domain time ${ordinal}/${node_count}: ${kind} of ${target_host}"
    failure_category=product
    local graph_node="node-${target_host##*-}"
    if [[ "${kind}" != stop ]]; then
        local host
        for host in "${node_hosts[@]}"; do
            if [[ "${host}" != "${target_host}" ]]; then
                graph_node="node-${host##*-}"
                break
            fi
        done
    fi
    domain_time_relocation_started_ns="$(date +%s%N)"
    domain_time_place "${graph_node}" "${round_dir}"
    domain_time_relocation_completed_ns="$(date +%s%N)"
    wait_for 'settled, connected cluster before the fault' "${domain_time_settle_bound}" \
        cluster_settled_and_connected "${round_dir}/before"
    check_support_containers
    domain_time_round_target="${target_host}"
    domain_time_round_dir="${round_dir}"
    domain_time_round_seconds="${seconds}"
    case "${kind}" in
        crash) node_fault_kill "${round_dir}" "${target_host}" "${seconds}" domain_time_before_fault domain_time_during_fault ;;
        pause) node_fault_pause "${round_dir}" "${target_host}" "${seconds}" domain_time_before_fault domain_time_during_fault ;;
        partition) node_fault_isolate "${round_dir}" "${target_host}" "${seconds}" domain_time_before_fault domain_time_during_fault ;;
        stop) node_fault_stop "${round_dir}" "${target_host}" "${seconds}" domain_time_before_fault domain_time_during_stop ;;
    esac
    domain_time_recover "${round_dir}" "${ordinal}" "${kind}" "${target_host}" "${graph_node}"
}

domain_time_cluster_restart_round() {
    local seconds="$1"
    local round_dir="${domain_time_case_dir}/round-1-cluster-restart"
    mkdir -p "${round_dir}/before"
    phase "domain time: SIGKILL of every node (${node_count})"
    failure_category=product
    wait_for 'settled, connected cluster before the restart' "${domain_time_settle_bound}" \
        cluster_settled_and_connected "${round_dir}/before"
    check_support_containers
    domain_time_round_target=all
    domain_time_round_dir="${round_dir}"
    domain_time_round_seconds="${seconds}"
    domain_time_relocation_started_ns="$(date +%s%N)"
    domain_time_relocation_completed_ns="${domain_time_relocation_started_ns}"
    node_fault_restart_cluster "${round_dir}" "${seconds}" domain_time_before_fault
    domain_time_recover "${round_dir}" 1 cluster-restart all \
        "$(jq -r '.[]' "${domain_time_case_dir}/placement.json" | sort -u | head -1)"
}

# Copies every observer's stamped transcript into the case directory.
domain_time_collect_observers() {
    local host
    for host in "${node_hosts[@]}"; do
        run_bounded 30 docker logs "$(domain_time_observer_name "${host}")" \
            >"${domain_time_case_dir}/clock-${host}.log" 2>&1 \
            || domain_time_fail controller "the clock observer of ${host} could not be read"
    done
}

run_domain_time() {
    local case_dir="${artifact_dir}/domain-time"
    domain_time_case_dir="${case_dir}"
    mkdir -p "${case_dir}"
    : >"${case_dir}/rounds.ndjson"
    recovery_traffic_startup "${case_dir}"

    failure_category=product
    phase 'domain time: paced traffic and clock observers'
    rm -f "${artifact_dir}/traffic/stop-paced-load"
    jq -nc --arg run_id "${run_id}" --argjson count "${domain_time_fixture_records}" \
        -f "${script_dir}/fixtures/generate-paced.jq" >"${CHAOS_PACED_LOAD_FILE}"
    [[ "$(wc -l <"${CHAOS_PACED_LOAD_FILE}")" -eq "${domain_time_fixture_records}" ]] \
        || domain_time_fail controller 'the paced fixture generation was incomplete'
    compose up --detach --no-deps paced-load
    local started_ns
    started_ns="$(date +%s%N)"
    domain_time_start_observers
    wait_for 'every clock observer attached and ticking' "${domain_time_attach_bound}" \
        domain_time_observers_ticking "${started_ns}" \
        || domain_time_fail product 'the clock observers saw no tick from the paced domain'
    wait_for 'paced source started' 30 topic_progressed chaos_paced_input 0
    wait_for 'paced windows closing' 90 domain_time_output_progressed 0 \
        || domain_time_fail product 'the paced window emitted nothing'
    domain_time_placement "${node_hosts[0]}" "${case_dir}/placement" \
        || domain_time_fail product 'the paced graph placement could not be read'
    local container_id
    container_id="$(owned_service_container "${node_hosts[0]}")"
    inspect_target "${container_id}" "${case_dir}/node-1-inspect.json"
    sleep "${domain_time_observe_seconds}"

    local seconds
    seconds="$(domain_time_fault_seconds)"
    case "${fault}" in
        none)
            phase 'domain time: healthy observation'
            sleep "${domain_time_observe_seconds}"
            ;;
        cluster-restart)
            domain_time_cluster_restart_round "${seconds}"
            ;;
        *)
            local ordinal=0
            local host
            for host in "${node_hosts[@]}"; do
                ordinal=$((ordinal + 1))
                domain_time_voter_round "${ordinal}" "${host}" "${seconds}"
            done
            ;;
    esac
    phase 'domain time: final observation'
    sleep "${domain_time_observe_seconds}"
    check_support_containers
    touch "${artifact_dir}/traffic/stop-paced-load"
    local load_id
    load_id="$(owned_service_container paced-load)" || return 1
    wait_for 'paced load flushed and exited' 40 load_exited_cleanly "${load_id}"
    wait_for 'paced windows settled after the load stopped' 60 domain_time_output_settled \
        || domain_time_fail product 'paced windows kept closing after the load stopped'
    local paced_end
    paced_end="$(topic_end_offset chaos_paced_output)"
    [[ "${paced_end}" =~ ^[0-9]+$ ]] || domain_time_fail setup 'the paced output offset was unavailable'
    if ((paced_end > 0)); then
        kcat -q -b broker:9092 -C -t chaos_paced_output -p 0 -o beginning -c "${paced_end}" \
            -f '%T %s\n' >"${case_dir}/paced-output.log" 2>"${case_dir}/paced-output.stderr"
    else
        : >"${case_dir}/paced-output.log"
    fi
    domain_time_collect_observers

    phase 'domain time: clock and window verdicts'
    local unavailability
    unavailability="$(jq -r '.[0].Config.Env[] | select(startswith("NERVIX_NODE_UNAVAILABILITY_TIMEOUT=")) | sub("^[^=]*="; "")' \
        "${case_dir}/node-1-inspect.json")"
    local observer_args=()
    for host in "${node_hosts[@]}"; do
        observer_args+=(--observer "${host}=${case_dir}/clock-${host}.log")
    done
    local status=0
    "${script_dir}/verify-clock-evidence.sh" clock "${observer_args[@]}" \
        --rounds "${case_dir}/rounds.ndjson" --fault "${fault}" \
        --period-ms "${domain_time_period_ms}" --rate "${domain_time_rate}" \
        --result "${case_dir}/verdict-clock.json" >"${case_dir}/verdict-clock.txt" 2>&1 || status=$?
    if ((status == 1)); then
        recovery_finding "${case_dir}" "domain clock: $(jq -r '.failures | join("; ")' "${case_dir}/verdict-clock.json")"
    elif ((status != 0)); then
        domain_time_fail controller 'the clock verifier could not judge its evidence; see domain-time/verdict-clock.txt'
    fi
    status=0
    "${script_dir}/verify-clock-evidence.sh" windows --output "${case_dir}/paced-output.log" \
        --rounds "${case_dir}/rounds.ndjson" --clock "${case_dir}/verdict-clock.json" --fault "${fault}" \
        --width-ms "${domain_time_width_ms}" --rate "${domain_time_rate}" \
        --result "${case_dir}/verdict-windows.json" >"${case_dir}/verdict-windows.txt" 2>&1 || status=$?
    if ((status == 1)); then
        recovery_finding "${case_dir}" "paced windows: $(jq -r '.failures | join("; ")' "${case_dir}/verdict-windows.json")"
    elif ((status != 0)); then
        domain_time_fail controller 'the window verifier could not judge its evidence; see domain-time/verdict-windows.txt'
    fi
    capture_all_metrics "${case_dir}"
    jq -n \
        --arg fault "${fault}" \
        --argjson nodes "${node_count}" \
        --argjson seconds "${seconds}" \
        --argjson rate "${domain_time_rate}" \
        --argjson period_ms "${domain_time_period_ms}" \
        --argjson width_ms "${domain_time_width_ms}" \
        --arg unavailability "${unavailability}" \
        --slurpfile rounds <(cat "${case_dir}/rounds.ndjson") \
        --slurpfile clock "${case_dir}/verdict-clock.json" \
        --slurpfile windows "${case_dir}/verdict-windows.json" \
        '{fault: $fault, topology_nodes: $nodes, fault_seconds: (if $fault == "none" then null else $seconds end),
          domain: {name: "chaos_paced", period_ms: $period_ms, skew_ms: $period_ms, time_rate: $rate,
                   window_width_logical_ms: $width_ms,
                   window_width_physical_ms: ($width_ms / $rate)},
          physical_limits: {node_unavailability_timeout: $unavailability,
                            graceful_stop_grace_seconds: 60},
          rounds: $rounds,
          verdicts: {clock: ($clock[0] | del(.observers)), windows: $windows[0]},
          observers: ($clock[0].observers // null),
          qualification_limit: "no public command, metric or info-level log names the clock authority; each fault rotation covers every voter and the tick gaps show which round removed it"}' \
        >"${artifact_dir}/results/domain-time-progress.json"

    recovery_final_boundary
}
