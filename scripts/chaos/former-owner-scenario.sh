#!/usr/bin/env bash
# Sourced by run-baseline.sh after the external cluster and graph are ready. Moves the ingestor,
# relay and emitter onto one follower, SIGKILLs it, changes its schedule through the packaged CLI
# while it is down, and starts its original container behind peer-side isolation. Until the
# isolation heals, the restarted former owner must answer on its public listeners, produce no graph
# output and stay unadmitted; after it heals, its public answers and the traffic must follow the
# current schedule.

# shellcheck source=recovery-scenario.sh
source "${script_dir}/recovery-scenario.sh"

# External budgets, in seconds.
former_owner_move_bound=60
former_owner_failover_bound=120
former_owner_group_bound=120
former_owner_sample_interval=5
# Longest accepted gap between pre-admission observations, in milliseconds. One observation reads
# the listeners, status, metrics, consumer group and log of the isolated node.
former_owner_max_sample_gap_ms=20000
former_owner_admission_bound=90
former_owner_convergence_bound=150
former_owner_execution_bound=90
former_owner_delivery_bound=90

former_owner_entity() {
    case "$1" in
        ingestor) printf 'chaos_ingestor\n' ;;
        relay) printf 'chaos_records\n' ;;
        emitter) printf 'chaos_emitter\n' ;;
    esac
}

# True when DESCRIBE through HOST names OWNER for the ingestor, relay and emitter alike, or the
# owner each one is mapped to in OWNERS_JSON.
owners_are() {
    local host="$1"
    local output_dir="$2"
    local owners_json="$3"
    mkdir -p "${output_dir}"
    admin_cli_batch "${host}" chaos_baseline "${output_dir}" 60 \
        ingestor 'DESCRIBE INGESTOR chaos_ingestor;' \
        relay 'DESCRIBE RELAY chaos_records;' \
        emitter 'DESCRIBE EMITTER chaos_emitter;' || return 1
    local kind
    for kind in ingestor relay emitter; do
        batch_read_succeeded "${output_dir}" "${kind}" || return 1
        [[ "$(owner_from_description "${output_dir}/${kind}.txt")" \
            == "$(jq -r --arg kind "${kind}" '.[$kind]' <<<"${owners_json}")" ]] || return 1
    done
}

# True when Kafka ingestion runs only on OWNER: every source consumer-group member is at OWNER's
# fixed address and one of them holds the partition. No member can then remain at another node.
ingestion_on() {
    local output_dir="$1"
    local owner="$2"
    mkdir -p "${output_dir}"
    consumer_group_snapshot "${output_dir}/consumer-group.json" || return 1
    jq -e --arg owner "nervix-${owner##*-}" '
        (.members | length) > 0
        and (.members | all(.node_host == $owner))
        and ([.members[].partitions] | add) == 1
    ' "${output_dir}/consumer-group.json" >/dev/null
}

# True when the log of a node container since SINCE contains TEXT.
node_log_contains() {
    local container_id="$1"
    local since="$2"
    local text="$3"
    run_bounded 20 docker logs --since "${since}" "${container_id}" 2>&1 | grep -Fq -- "${text}"
}

# True when every running peer of the stopped former owner carries exactly its planned rules.
peer_rules_match_plan() {
    local case_dir="$1"
    local isolated="$2"
    injectors_running "${case_dir}" || return 1
    mkdir -p "${case_dir}/rules-peers"
    local host
    for host in "${node_hosts[@]}"; do
        [[ "${host}" == "${isolated}" ]] && continue
        inspect_node_rules "${host}" "${case_dir}/rules-peers/${host}.txt" || return 1
        "${script_dir}/verify-partition-evidence.sh" rules "${case_dir}/plan.json" "${host}" \
            "${case_dir}/rules-peers/${host}.txt" 2>"${case_dir}/rules-peers/${host}.verdict.txt" || return 1
    done
}

# Records one observation of the isolated former owner: its public listeners and readiness, its own
# status, the graph messages on its metrics, the source consumer-group members, and whether its log
# reports runtime admission yet.
sample_former_owner() {
    local case_dir="$1"
    local host="$2"
    local container_id="$3"
    local started_at="$4"
    local label="$5"
    local sample_dir="${case_dir}/isolated/${label}"
    mkdir -p "${sample_dir}"
    local at_ms
    at_ms="$(epoch_ms)"
    compose run --rm --no-deps -T probe sh -c '
        nc -z -w 2 "$1" 47391 && echo "session true" || echo "session false"
        nc -z -w 2 "$1" 47395 && echo "interconnect true" || echo "interconnect false"
        wget -q -T 3 -O /dev/null "http://$1:47420/console/" && echo "console true" || echo "console false"
        wget -q -T 3 -O /dev/null "http://$1:9090/livez" && echo "observability true" || echo "observability false"
        wget -q -T 3 -O /dev/null "http://$1:9090/readyz" && echo "readyz ready" || echo "readyz unready"
    ' -- "${host}" </dev/null >"${sample_dir}/listeners.txt" 2>&1 || true
    local status_route=false
    local last_applied=-1
    if partition_node_status "${host}" "${sample_dir}/status.txt"; then
        status_route=true
        last_applied="$(jq -r '.last_applied' "${sample_dir}/status.json")"
        status_record "${sample_dir}/status.txt" "${host}" "$(date +%s%N)" >"${sample_dir}/status-record.json"
    fi
    local graph_messages=-1
    if node_metrics "${host}" "${sample_dir}/metrics.txt"; then
        graph_messages="$(metric_sum "${sample_dir}/metrics.txt" nervix_messages_total)"
    fi
    consumer_group_snapshot "${sample_dir}/consumer-group.json" \
        || jq -n '{members: [], unavailable: true}' >"${sample_dir}/consumer-group.json"
    run_bounded 20 docker logs --since "${started_at}" "${container_id}" >"${sample_dir}/node.log" 2>&1 || true
    local admitted=false
    if grep -Fq 'runtime execution admitted after linearizable consensus catch-up' "${sample_dir}/node.log"; then
        admitted=true
    fi
    # A metrics scrape that fails leaves the observability listener unanswered for this sample.
    jq -nc \
        --arg label "${label}" \
        --argjson at_ms "${at_ms}" \
        --rawfile listeners "${sample_dir}/listeners.txt" \
        --argjson status_route "${status_route}" \
        --argjson last_applied "${last_applied}" \
        --argjson graph_messages "${graph_messages}" \
        --argjson admitted "${admitted}" \
        --slurpfile group "${sample_dir}/consumer-group.json" '
        ($listeners | split("\n") | map(select(length > 0) | split(" ")) | map({key: .[0], value: .[1]})
         | from_entries) as $probe
        | {sample: $label, at_ms: $at_ms,
           listeners: {session: ($probe.session == "true"), interconnect: ($probe.interconnect == "true"),
                       console: ($probe.console == "true"),
                       observability: ($probe.observability == "true" and $graph_messages >= 0),
                       status_route: $status_route},
           readyz: ($probe.readyz // "unobserved"),
           members: $group[0].members,
           graph_messages: (if $graph_messages < 0 then 0 else $graph_messages end),
           admitted_logged: $admitted, last_applied: $last_applied}' \
        >>"${case_dir}/pre-admission.ndjson"
}

run_former_owner_restart() {
    local case_dir="${artifact_dir}/former-owner"
    mkdir -p "${case_dir}"
    : >"${case_dir}/changes.ndjson"
    : >"${case_dir}/pre-admission.ndjson"
    recovery_traffic_startup "${case_dir}"

    failure_category=product
    phase 'former owner: settled roles'
    wait_for 'settled, connected cluster' 150 observe_partition_roles "${case_dir}/before"
    verify_node_addresses
    local leader owner_node owner_host leader_host
    leader="$(jq -r '.leader' "${case_dir}/before/role.json")"
    owner_node="$(jq -r '.follower' "${case_dir}/before/role.json")"
    leader_host="nervix-${leader##*-}"
    owner_host="nervix-${owner_node##*-}"
    local owner_address
    owner_address="$(node_address "${owner_host}")"
    local outcome
    outcome="$(configuration_change "${case_dir}" chaos_owner_before "${leader_host}" chaos_baseline \
        'CREATE RESOURCE chaos_owner_before;' "created resource 'chaos_owner_before'")"
    [[ "${outcome}" == acknowledged ]] \
        || recovery_fail product "the control canary before the fault was ${outcome}, not acknowledged"

    phase "former owner: moving the ingestor, relay and emitter onto ${owner_host}"
    local kind
    for kind in ingestor relay emitter; do
        [[ "$(jq -r --arg kind "${kind}" '.[$kind + "_owner"]' "${case_dir}/before/role.json")" != "${owner_node}" ]] \
            || continue
        outcome="$(configuration_change "${case_dir}" "chaos_owner_move_${kind}" "${leader_host}" chaos_baseline \
            "RELOCATE ${kind^^} $(former_owner_entity "${kind}") ONTO NODE ${owner_node} IGNORE PREFERENCES;" \
            "relocated 1 of 1 runtime node(s) onto node '${owner_node}'" relocation)"
        [[ "${outcome}" == acknowledged ]] \
            || recovery_fail product "relocating the ${kind} onto ${owner_node} was ${outcome}"
    done
    local all_on_owner
    all_on_owner="$(jq -nc --arg owner "${owner_node}" '{ingestor: $owner, relay: $owner, emitter: $owner}')"
    wait_for "${owner_host} owns the ingestor, relay and emitter" "${former_owner_move_bound}" \
        owners_are "${leader_host}" "${case_dir}/owned" "${all_on_owner}"
    wait_for "Kafka ingestion runs on ${owner_host}" 90 ingestion_on "${case_dir}/owned" "${owner_node}"
    local output_owned
    output_owned="$(topic_end_offset chaos_output)"
    wait_for "sink output advanced through ${owner_host}" 60 topic_progressed chaos_output "${output_owned}" \
        || recovery_fail product "${owner_host} did not deliver output after owning the graph"
    capture_all_metrics "${case_dir}/owned"
    metric_is_positive "${case_dir}/owned/metrics-${owner_host}.txt" \
        'direction="received"' "physical_node_id=\"${owner_node}\"" 'target="chaos_emitter"' \
        || recovery_fail product "${owner_host} metrics do not show it emitting output"
    check_support_containers
    check_other_nodes ""

    phase "former owner: SIGKILL of ${owner_host}"
    failure_category=injection
    local node_ids=()
    local host
    for host in "${node_hosts[@]}"; do
        node_ids+=("$(owned_service_container "${host}")")
    done
    run_bounded 20 docker inspect "${node_ids[@]}" >"${case_dir}/before-all-nodes.json"
    record_node_volumes "${case_dir}/volumes-before.json"
    local container_id container_name
    container_id="$(owned_service_container "${owner_host}")" || return 1
    inspect_target "${container_id}" "${case_dir}/selected.json"
    "${script_dir}/verify-crash-evidence.sh" before "${run_id}" "${project_name}" \
        "${owner_host}" "${image_id}" "${case_dir}/selected.json" "${case_dir}/selected.json"
    container_name="$(jq -r '.[0].Name | ltrimstr("/")' "${case_dir}/selected.json")"
    run_bounded 20 docker run --rm \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=fault \
        --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        "${pumba_image_id}" --dry-run --log-level info \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=node \
        kill --signal SIGKILL --limit 1 "${container_name}" \
        >"${case_dir}/pumba-dry-run.txt" 2>&1
    [[ "$(grep -Fc 'msg="killing container"' "${case_dir}/pumba-dry-run.txt")" -eq 1 ]] \
        && grep -Fq "dryrun=true id=${container_id}" "${case_dir}/pumba-dry-run.txt" \
        || recovery_fail injection "Pumba did not resolve exactly the owned ${owner_host} container"
    # The owner must still be the follower that holds the whole graph when the fault lands.
    wait_for 'settled public roles immediately before the SIGKILL' 30 \
        observe_partition_roles "${case_dir}/immediately-before" \
        || recovery_fail injection 'public roles were unavailable immediately before the SIGKILL'
    jq -e --arg owner "${owner_node}" --arg leader "${leader}" '
        .leader == $leader and .ingestor_owner == $owner and .relay_owner == $owner and .emitter_owner == $owner
    ' "${case_dir}/immediately-before/role.json" >/dev/null \
        || recovery_fail injection "the leader or an owner moved off ${owner_node} before the SIGKILL"
    local fault_since fault_since_ns
    fault_since="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    fault_since_ns="$(date -d "${fault_since}" +%s%N)"
    jq -n \
        --arg image "${pumba_image_id}" \
        --arg target "${container_name}" \
        --arg container_id "${container_id}" \
        --arg run_id "${run_id}" \
        --arg fault_since "${fault_since}" \
        '{pumba_image_id:$image,command:["pumba","--label","io.nervix.chaos.run="+$run_id,"--label","io.nervix.chaos.role=node","kill","--signal","SIGKILL","--limit","1",$target],target:$target,container_id:$container_id,fault_since:$fault_since,public_role:"immediately-before/role.json"}' \
        >"${case_dir}/fault-command.json"
    recovery_node_stopped "${container_id}"
    run_bounded 30 docker run --rm \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=fault \
        --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        "${pumba_image_id}" --log-level info \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=node \
        kill --signal SIGKILL --limit 1 "${container_name}" \
        >"${case_dir}/pumba.txt" 2>&1
    grep -Fq "dryrun=false id=${container_id}" "${case_dir}/pumba.txt" \
        || recovery_fail injection 'Pumba did not report the former owner killed'
    inspect_target "${container_id}" "${case_dir}/killed.json"
    node_event_window "${fault_since_ns}" "${container_id}" "${case_dir}/kill-events.ndjson"
    "${script_dir}/verify-crash-evidence.sh" killed "${run_id}" "${project_name}" \
        "${owner_host}" "${image_id}" "${case_dir}/selected.json" \
        "${case_dir}/killed.json" "${case_dir}/kill-events.ndjson"
    check_support_containers
    check_other_nodes "${owner_host}"

    failure_category=product
    phase "former owner: schedule change while ${owner_host} is down"
    wait_for 'survivors agree on a caught-up leader' 90 survivors_settled "${owner_host}" "${case_dir}"
    local survivor_leader survivor_leader_host
    survivor_leader="$(<"${case_dir}/survivor-leader.txt")"
    survivor_leader_host="nervix-${survivor_leader##*-}"
    wait_for 'failover placed the graph on the survivors' "${former_owner_failover_bound}" \
        surviving_owners_ready "${owner_host}" "${case_dir}" "${survivor_leader_host}" \
        || recovery_fail product "the survivors did not take over the work of ${owner_host}"
    # Every unit moves again, through an acknowledged public command, to the survivor that failover
    # did not choose for it.
    local -A current_schedule=()
    for kind in ingestor relay emitter; do
        local failed_over target=""
        failed_over="$(owner_from_description "${case_dir}/${kind}-survivor.attempt.txt")"
        for host in "${node_hosts[@]}"; do
            if [[ "${host}" != "${owner_host}" && "node-${host##*-}" != "${failed_over}" ]]; then
                target="node-${host##*-}"
            fi
        done
        [[ "${target}" =~ ^node-[123]$ ]] || recovery_fail controller "no survivor can take the ${kind}"
        outcome="$(configuration_change "${case_dir}" "chaos_owner_schedule_${kind}" "${survivor_leader_host}" \
            chaos_baseline \
            "RELOCATE ${kind^^} $(former_owner_entity "${kind}") ONTO NODE ${target} IGNORE PREFERENCES;" \
            "relocated 1 of 1 runtime node(s) onto node '${target}'" relocation)"
        [[ "${outcome}" == acknowledged ]] \
            || recovery_fail product "changing the ${kind} schedule while ${owner_host} was down was ${outcome}"
        current_schedule["${kind}"]="${target}"
    done
    local schedule_json
    schedule_json="$(jq -nc \
        --arg ingestor "${current_schedule[ingestor]}" \
        --arg relay "${current_schedule[relay]}" \
        --arg emitter "${current_schedule[emitter]}" \
        '{ingestor: $ingestor, relay: $relay, emitter: $emitter}')"
    printf '%s\n' "${schedule_json}" >"${case_dir}/schedule.json"
    wait_for 'the survivors serve the changed schedule' "${former_owner_move_bound}" \
        owners_are "${survivor_leader_host}" "${case_dir}/scheduled" "${schedule_json}"
    wait_for "Kafka ingestion runs only on ${current_schedule[ingestor]}" "${former_owner_group_bound}" \
        ingestion_on "${case_dir}/scheduled" "${current_schedule[ingestor]}" \
        || recovery_fail product "the source consumer group kept a member at ${owner_address} or did not converge on the scheduled owner"
    local output_scheduled
    output_scheduled="$(topic_end_offset chaos_output)"
    wait_for 'sink output advanced on the changed schedule' 60 \
        topic_progressed chaos_output "${output_scheduled}" \
        || recovery_fail product 'the survivors did not deliver output on the changed schedule'

    phase "former owner: peer-side isolation of ${owner_host}"
    failure_category=injection
    plan_isolation former-owner "${owner_host}" "${case_dir}/plan.json"
    "${script_dir}/verify-partition-evidence.sh" plan "${case_dir}/plan.json" \
        || recovery_fail controller 'the isolation plan does not block exactly the intended links'
    mkdir -p "${case_dir}/rules-before"
    for host in "${node_hosts[@]}"; do
        [[ "${host}" == "${owner_host}" ]] && continue
        inspect_node_rules "${host}" "${case_dir}/rules-before/${host}.txt"
        "${script_dir}/verify-partition-evidence.sh" rules "${case_dir}/plan.json" "${host}" \
            "${case_dir}/rules-before/${host}.txt" --healed \
            || recovery_fail injection "${host} already carries a qdisc, filter or INPUT rule; refusing to overlap faults"
    done
    for host in "${node_hosts[@]}"; do
        [[ "${host}" == "${owner_host}" ]] && continue
        start_injector "${case_dir}" 1 "${host}" netem "${owner_address}"
        start_injector "${case_dir}" 1 "${host}" iptables "${owner_address}"
    done
    wait_for 'peer-side isolation installed as planned' 30 peer_rules_match_plan "${case_dir}" "${owner_host}" \
        || recovery_fail injection "the peers of ${owner_host} did not carry exactly the planned rules"

    phase "former owner: restart of ${owner_host} behind the isolation"
    local restart_started_at restart_started_ms
    restart_started_at="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    restart_started_ms="$(epoch_ms)"
    run_bounded 30 docker start "${container_id}" >"${case_dir}/docker-start.txt"
    recovery_node_started "${container_id}"
    inspect_target "${container_id}" "${case_dir}/started.json"
    "${script_dir}/verify-crash-evidence.sh" started "${run_id}" "${project_name}" \
        "${owner_host}" "${image_id}" "${case_dir}/selected.json" "${case_dir}/started.json"
    [[ "$(container_address "${container_id}")" == "${owner_address}" ]] \
        || recovery_fail injection "the restarted ${owner_host} did not keep its planned address"
    mkdir -p "${case_dir}/rules-isolated"
    rules_match_plan "${case_dir}/plan.json" "${case_dir}/rules-isolated" \
        || recovery_fail injection 'the restarted former owner or its peers do not carry exactly the planned rules'
    probe_links "${case_dir}/links-isolated.json"
    "${script_dir}/verify-partition-evidence.sh" links "${case_dir}/plan.json" \
        "${case_dir}/links-isolated.json" "${case_dir}/links-isolated-verdict.json" \
        || recovery_fail injection 'the observed links of the restarted former owner differ from the plan'
    local isolation_verified_ms
    isolation_verified_ms="$(epoch_ms)"

    failure_category=product
    phase "former owner: ${owner_host} before startup admission"
    local output_isolated
    output_isolated="$(topic_end_offset chaos_output)"
    # Samples run until one starts after the declared window, so they cover all of it.
    local sample=0
    local hold_until_ms=$((isolation_verified_ms + isolation_seconds * 1000))
    while true; do
        local sample_started_ms
        sample_started_ms="$(epoch_ms)"
        sample_former_owner "${case_dir}" "${owner_host}" "${container_id}" "${restart_started_at}" \
            "sample-${sample}"
        sample=$((sample + 1))
        ((sample_started_ms < hold_until_ms)) || break
        local pause_ms=$((former_owner_sample_interval * 1000 - ($(epoch_ms) - sample_started_ms)))
        local remaining_ms=$((hold_until_ms - $(epoch_ms)))
        if ((pause_ms > remaining_ms)); then
            pause_ms="${remaining_ms}"
        fi
        if ((pause_ms > 0)); then
            sleep "$((pause_ms / 1000)).$(printf '%03d' $((pause_ms % 1000)))"
        fi
    done
    "${script_dir}/verify-recovery-evidence.sh" pre-admission \
        --samples "${case_dir}/pre-admission.ndjson" --address "${owner_address}" \
        --isolated-at-ms "${isolation_verified_ms}" --min-window-ms "$((isolation_seconds * 1000))" \
        --max-gap-ms "${former_owner_max_sample_gap_ms}" --output "${case_dir}/pre-admission.json" \
        || recovery_finding "${case_dir}" "the isolated former owner ${owner_host} did not stay inert before startup admission"
    topic_progressed chaos_output "${output_isolated}" \
        || recovery_finding "${case_dir}" 'the survivors delivered no output while the former owner was isolated'
    local stale_schedule=null
    if [[ -s "${case_dir}/isolated/sample-0/status-record.json" ]]; then
        stale_schedule="$(jq -c '.owners' "${case_dir}/isolated/sample-0/status-record.json")"
    fi

    heal_partition "${case_dir}" former-owner
    local heal_started_ns=$((heal_started_ms * 1000000))

    failure_category=product
    phase "former owner: ${owner_host} after the isolation healed"
    wait_for "${owner_host} reported runtime admission" "${former_owner_admission_bound}" \
        node_log_contains "${container_id}" "${restart_started_at}" \
        'runtime execution admitted after linearizable consensus catch-up' \
        || recovery_finding "${case_dir}" "${owner_host} reported no runtime admission within ${former_owner_admission_bound}s of healing"
    run_bounded 20 docker logs --since "${restart_started_at}" "${container_id}" \
        >"${case_dir}/node-since-restart.log" 2>&1
    trim_file "${case_dir}/node-since-restart.log" 2097152
    "${script_dir}/verify-recovery-evidence.sh" admission --log "${case_dir}/node-since-restart.log" \
        --not-before-ns "${heal_started_ns}" --output "${case_dir}/admission.json" \
        || recovery_finding "${case_dir}" "${owner_host} was admitted before its isolation healed or never reported waiting for catch-up"
    mkdir -p "${case_dir}/recovered"
    wait_for 'every node rejoined, caught up and connected' "${former_owner_convergence_bound}" \
        cluster_settled_and_connected "${case_dir}/recovered" \
        || recovery_fail product "the cluster did not converge within ${former_owner_convergence_bound}s of healing"
    local convergence_ms="$(( $(epoch_ms) - heal_completed_ms ))"
    wait_for "${owner_host} answers with the changed schedule" 60 \
        owners_are "${owner_host}" "${case_dir}/rejoined" "${schedule_json}" \
        || recovery_finding "${case_dir}" "DESCRIBE through the rejoined ${owner_host} does not report the changed schedule"
    partition_node_status "${owner_host}" "${case_dir}/rejoined/status.txt" \
        || recovery_fail product "the rejoined ${owner_host} did not answer its public status route"
    status_record "${case_dir}/rejoined/status.txt" "${owner_host}" "$(date +%s%N)" \
        >"${case_dir}/rejoined/status-record.json"
    jq -e --argjson schedule "${schedule_json}" '.owners == $schedule' \
        "${case_dir}/rejoined/status-record.json" >/dev/null \
        || recovery_finding "${case_dir}" "the schedule in the rejoined ${owner_host} status differs from the changed schedule"
    wait_for 'Kafka ingestion runs only on the scheduled ingestor owner' "${former_owner_execution_bound}" \
        ingestion_on "${case_dir}/rejoined-group" "${current_schedule[ingestor]}" \
        || recovery_finding "${case_dir}" "Kafka ingestion did not follow the changed schedule after healing"
    local output_healed
    output_healed="$(topic_end_offset chaos_output)"
    wait_for 'sink output advanced after healing' "${former_owner_delivery_bound}" \
        topic_progressed chaos_output "${output_healed}" \
        || recovery_fail product 'sink output did not advance after healing'
    local delivery_ms="$(( $(epoch_ms) - heal_completed_ms ))"
    outcome="$(configuration_change "${case_dir}" chaos_owner_after "${owner_host}" chaos_baseline \
        'CREATE RESOURCE chaos_owner_after;' "created resource 'chaos_owner_after'")"
    [[ "${outcome}" == acknowledged ]] \
        || recovery_fail product "the control canary through the rejoined former owner was ${outcome}, not acknowledged"
    for host in "${node_hosts[@]}"; do
        wait_for "control canaries through ${host}" 60 \
            capture_configuration "${host}" "${case_dir}/canaries-after/${host}" \
            chaos_owner_before chaos_owner_after
        jq -e '.resources | all(.[]; . == "present")' "${case_dir}/canaries-after/${host}.json" >/dev/null \
            || recovery_finding "${case_dir}" "an acknowledged control canary is absent through ${host}"
    done

    docker_event_window "${fault_since_ns}" "${case_dir}/node-events.ndjson" --role node \
        || recovery_fail controller 'the live Docker event recording does not cover the fault through recovery'
    "${script_dir}/verify-docker-events.sh" lifecycle --events "${case_dir}/node-events.ndjson" \
        --target "${container_id}" --expect kill:9 --expect die:137 --expect start \
        || recovery_finding "${case_dir}" 'node lifecycle events other than the SIGKILL, exit and explicit start of the former owner occurred'
    other_node_instances_unchanged "${owner_host}" "${case_dir}/before-all-nodes.json" \
        || recovery_finding "${case_dir}" 'a survivor changed its container or process incarnation'
    inspect_target "${container_id}" "${case_dir}/final.json"
    "${script_dir}/verify-crash-evidence.sh" recovered "${run_id}" "${project_name}" \
        "${owner_host}" "${image_id}" "${case_dir}/started.json" "${case_dir}/final.json" \
        || recovery_finding "${case_dir}" "${owner_host} restarted again after the explicit Docker start"
    node_volumes_unchanged "${case_dir}/volumes-before.json" "${case_dir}/volumes-after.json"
    capture_all_metrics "${case_dir}"
    local observer_id
    observer_id="$(owned_service_container observer)" || return 1
    run_bounded 20 docker logs --since "${fault_since}" "${observer_id}" >"${case_dir}/observer.log" 2>&1
    trim_file "${case_dir}/observer.log" 1048576

    jq -n \
        --arg owner "${owner_node}" \
        --arg address "${owner_address}" \
        --arg leader "${leader}" \
        --arg survivor_leader "${survivor_leader}" \
        --arg fault_since "${fault_since}" \
        --argjson schedule "${schedule_json}" \
        --argjson stale_schedule "${stale_schedule}" \
        --argjson isolation_seconds "${isolation_seconds}" \
        --argjson install_ms "$((isolation_verified_ms - restart_started_ms))" \
        --argjson isolation_window_ms "$((heal_started_ms - isolation_verified_ms))" \
        --argjson heal_ms "$((heal_completed_ms - heal_started_ms))" \
        --argjson convergence_ms "${convergence_ms}" \
        --argjson delivery_ms "${delivery_ms}" \
        --slurpfile pre_admission "${case_dir}/pre-admission.json" \
        --slurpfile admission "${case_dir}/admission.json" \
        '{former_owner: $owner, address: $address, leader_before: $leader, survivor_leader: $survivor_leader,
          fault_since: $fault_since, changed_schedule: $schedule,
          schedule_reported_by_isolated_owner: $stale_schedule,
          minimum_isolation_seconds: $isolation_seconds,
          restart_to_verified_isolation_ms: $install_ms, verified_isolation_window_ms: $isolation_window_ms,
          heal_ms: $heal_ms, convergence_after_heal_ms: $convergence_ms,
          delivery_after_heal_ms: $delivery_ms,
          pre_admission: $pre_admission[0], admission: $admission[0],
          samples: "former-owner/pre-admission.ndjson",
          links: {isolated: "former-owner/links-isolated.json", healed: "former-owner/links-healed.json"},
          node_events: "former-owner/node-events.ndjson",
          findings: "former-owner/findings.ndjson"}' \
        >"${artifact_dir}/results/former-owner-restart-progress.json"

    recovery_final_boundary
}
