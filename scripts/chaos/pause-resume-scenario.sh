#!/usr/bin/env bash
# Sourced by run-baseline.sh after the external cluster and graph are ready.

# shellcheck source=crash-scenario.sh
source "${script_dir}/crash-scenario.sh"

pause_fail() {
    failure_category="$1"
    shift
    printf 'pause %s failure: %s\n' "${failure_category}" "$*" >&2
    return 1
}

pause_target_is() {
    local container_id="$1"
    local expected="$2"
    [[ "$(run_bounded 10 docker inspect --format '{{.State.Paused}}' "${container_id}")" == "${expected}" ]]
}

pause_topic_progressed() {
    local topic="$1"
    local before="$2"
    pause_observed_offset="$(topic_end_offset "${topic}")" || return 1
    [[ "${pause_observed_offset}" =~ ^[0-9]+$ ]] && ((pause_observed_offset > before))
}

wait_for_pause_state() {
    local container_id="$1"
    local expected="$2"
    local deadline_ms="$(( $(epoch_ms) + 5000 ))"
    while (( $(epoch_ms) < deadline_ms )); do
        if pause_target_is "${container_id}" "${expected}"; then
            return 0
        fi
        sleep 0.05
    done
    printf 'Docker state did not become paused=%s for %s\n' "${expected}" "${container_id}" >&2
    return 1
}

select_pause_target() {
    local role="$1"
    local output_dir="$2"
    mkdir -p "${output_dir}"
    cluster_settled_and_connected "${output_dir}" || return 1
    local leader ingestor_owner relay_owner emitter_owner
    leader="$(<"${output_dir}/leader.txt")"
    ingestor_owner="$(owner_from_description "${output_dir}/ingestor.attempt.txt")"
    relay_owner="$(owner_from_description "${output_dir}/relay.attempt.txt")"
    emitter_owner="$(owner_from_description "${output_dir}/emitter.attempt.txt")"
    [[ "${leader}" =~ ^node-[123]$ && "${ingestor_owner}" =~ ^node-[123]$ \
        && "${relay_owner}" =~ ^node-[123]$ && "${emitter_owner}" =~ ^node-[123]$ ]] \
        || return 1

    local chosen_node=""
    local chosen_kind=""
    if [[ "${role}" == leader ]]; then
        chosen_node="${leader}"
        chosen_kind=leader
    else
        local candidate
        for candidate in relay ingestor emitter; do
            local owner="${candidate}_owner"
            if [[ "${!owner}" != "${leader}" ]]; then
                chosen_node="${!owner}"
                chosen_kind="${candidate}"
                break
            fi
        done
    fi
    [[ "${chosen_node}" =~ ^node-[123]$ ]] || return 1
    target_host="nervix-${chosen_node##*-}"
    target_node="${chosen_node}"
    target_kind="${chosen_kind}"
    jq -n \
        --arg role "${role}" \
        --arg kind "${chosen_kind}" \
        --arg leader "${leader}" \
        --arg ingestor "${ingestor_owner}" \
        --arg relay "${relay_owner}" \
        --arg emitter "${emitter_owner}" \
        --arg target "${chosen_node}" \
        '{role:$role,target_kind:$kind,target_node:$target,leader:$leader,ingestor_owner:$ingestor,relay_owner:$relay,emitter_owner:$emitter}' \
        >"${output_dir}/role.json"
}

confirm_pause_target_before_fault() {
    local role="$1"
    local selected_node="$2"
    local selected_kind="$3"
    local output_dir="$4"
    if ! wait_for 'settled public role immediately before pause' 30 \
        select_pause_target "${role}" "${output_dir}"; then
        pause_fail injection 'settled public role was unavailable before pause'
        return 1
    fi
    [[ "${target_node}" == "${selected_node}" && "${target_kind}" == "${selected_kind}" ]] \
        || pause_fail injection 'observed leader or execution owner moved before pause'
}

peer_status_during_pause() {
    local peer="$1"
    local output="$2"
    local status=0
    run_bounded 6 docker compose "${compose_args[@]}" run --rm --no-deps admin \
        nervix-cli --server "http://${peer}:47391" --password "${CHAOS_PASSWORD}" \
        --command 'SHOW CLUSTER STATUS;' >"${output}" 2>&1 || status=$?
    printf '%s\n' "${status}" >"${output}.exit-code"
}

pause_node_events_expected() {
    local target_id="$1"
    local output="$2"
    local since="$3"
    run_bounded 20 docker events \
        --since "${since}" --until "$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)" \
        --filter "label=io.nervix.chaos.run=${run_id}" \
        --filter label=io.nervix.chaos.role=node \
        --format '{{json .}}' >"${output}"
    jq -s -e --arg target_id "${target_id}" '
        all(.[] | select(.Action == "pause" or .Action == "unpause"); .Actor.ID == $target_id) and
        ([.[] | select(.Action == "die" or .Action == "kill" or .Action == "stop" or .Action == "start")] | length == 0)
    ' "${output}" >/dev/null
}

pause_one_node() {
    local duration="$1"
    local role="$2"
    local ordinal="$3"
    local round_dir="${artifact_dir}/pauses/${ordinal}-${duration}-${role}"
    mkdir -p "${round_dir}/before" "${round_dir}/immediately-before" "${round_dir}/recovered"
    phase "${duration} pause of observed ${role} (${ordinal}/4)"
    failure_category=product
    wait_for 'cluster settled before pause role selection' 90 \
        select_pause_target "${role}" "${round_dir}/before"
    local selected_host="${target_host}"
    local selected_node="${target_node}"
    local selected_kind="${target_kind}"
    local observed_leader
    observed_leader="$(jq -r '.leader' "${round_dir}/before/role.json")"
    check_support_containers
    check_other_nodes ""

    local peer=""
    local host
    for host in "${node_hosts[@]}"; do
        if [[ "${host}" != "${selected_host}" ]]; then
            peer="${host}"
            break
        fi
    done
    [[ -n "${peer}" ]] || pause_fail setup 'three-node peer was not available'
    local before_canary="chaos_pause_${ordinal}_before"
    local during_canary="chaos_pause_${ordinal}_during"
    local after_canary="chaos_pause_${ordinal}_after"
    control_attempt "${before_canary}" "${peer}" "${round_dir}"
    [[ "$(jq -r '.outcome' "${round_dir}/control-${before_canary}.json")" == acknowledged ]] \
        || pause_fail product 'pre-pause control canary was not acknowledged'
    control_effect_present "${before_canary}" "${round_dir}" \
        || pause_fail product 'acknowledged pre-pause control effect is absent'

    failure_category=injection
    local container_id container_name
    container_id="$(owned_service_container "${selected_host}")" || return 1
    inspect_target "${container_id}" "${round_dir}/selected.json"
    "${script_dir}/verify-pause-evidence.sh" before "${run_id}" "${project_name}" \
        "${selected_host}" "${image_id}" "${round_dir}/selected.json" "${round_dir}/selected.json"
    container_name="$(jq -r '.[0].Name | ltrimstr("/")' "${round_dir}/selected.json")"
    run_bounded 20 docker run --rm \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=fault \
        --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        "${pumba_image_id}" --dry-run --log-level info \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=node \
        pause --duration 1ms --limit 1 "${container_name}" \
        >"${round_dir}/pumba-dry-run.txt" 2>&1
    [[ "$(grep -Fc 'msg="pausing container"' "${round_dir}/pumba-dry-run.txt")" -eq 1 ]] \
        && grep -Fq "dryrun=true id=${container_id}" "${round_dir}/pumba-dry-run.txt" \
        && grep -Fq "name=/${container_name}" "${round_dir}/pumba-dry-run.txt" \
        || pause_fail injection 'Pumba dry run did not select exactly the observed node'

    confirm_pause_target_before_fault "${role}" "${selected_node}" "${selected_kind}" \
        "${round_dir}/immediately-before"
    inspect_target "${container_id}" "${round_dir}/before.json"
    "${script_dir}/verify-pause-evidence.sh" before "${run_id}" "${project_name}" \
        "${selected_host}" "${image_id}" "${round_dir}/selected.json" "${round_dir}/before.json"
    [[ "$(owned_service_container "${selected_host}")" == "${container_id}" ]] \
        || pause_fail injection 'selected container changed before pause'
    local node_container_ids=()
    for host in "${node_hosts[@]}"; do
        node_container_ids+=("$(owned_service_container "${host}")")
    done
    run_bounded 20 docker inspect "${node_container_ids[@]}" >"${round_dir}/before-all-nodes.json"
    local source_before output_before
    source_before="$(topic_end_offset chaos_input)"
    output_before="$(topic_end_offset chaos_output)"
    [[ "${source_before}" =~ ^[0-9]+$ && "${output_before}" =~ ^[0-9]+$ ]] \
        || pause_fail setup 'broker offsets were unavailable before pause'

    local fault_since requested_ms
    fault_since="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    requested_ms="$(epoch_ms)"
    jq -n \
        --arg image "${pumba_image_id}" \
        --arg target "${container_name}" \
        --arg container_id "${container_id}" \
        --arg run_id "${run_id}" \
        --arg fault_since "${fault_since}" \
        --argjson seconds "${duration}" \
        '{pumba_image_id:$image,command:["pumba","--label","io.nervix.chaos.run="+$run_id,"--label","io.nervix.chaos.role=node","pause","--duration",($seconds|tostring)+"s","--limit","1",$target],target:$target,container_id:$container_id,fault_since:$fault_since,intended_pause_seconds:$seconds,public_role:"immediately-before/role.json"}' \
        >"${round_dir}/fault-command.json"

    pause_target_id="${container_id}"
    run_bounded "$((duration + 20))" docker run --rm \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=fault \
        --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        "${pumba_image_id}" --log-level info \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=node \
        pause --duration "${duration}s" --limit 1 "${container_name}" \
        >"${round_dir}/pumba.txt" 2>&1 &
    pause_injector_pid=$!
    wait_for_pause_state "${container_id}" true \
        || pause_fail injection 'Pumba never paused the selected node'
    inspect_target "${container_id}" "${round_dir}/paused.json"
    "${script_dir}/verify-pause-evidence.sh" paused "${run_id}" "${project_name}" \
        "${selected_host}" "${image_id}" "${round_dir}/before.json" "${round_dir}/paused.json"
    local observer_id
    observer_id="$(owned_service_container observer)" || return 1
    local source_during=null output_during=null
    if [[ "${duration}" -gt 1 ]]; then
        wait_for 'source traffic progressed while target paused' 10 \
            pause_topic_progressed chaos_input "${source_before}" \
            || pause_fail product 'independent Kafka producer stopped during pause'
        source_during="${pause_observed_offset}"
        pause_target_is "${container_id}" true \
            || pause_fail controller 'source progress was sampled after the pause window'
    fi
    local control_pid="" peer_status_pid=""
    if [[ "${duration}" -eq 1 ]]; then
        control_attempt "${during_canary}" "${peer}" "${round_dir}" 4 &
        control_pid=$!
        peer_status_during_pause "${peer}" "${round_dir}/peer-status-around.txt" &
        peer_status_pid=$!
    else
        wait_for "independent observer saw ${selected_host} unavailable" 8 \
            observer_saw "${observer_id}" "${selected_host}" unavailable "${fault_since}" \
            || pause_fail product 'independent listener observer missed the paused node'
        peer_status_during_pause "${peer}" "${round_dir}/peer-status-during.txt"
    fi

    local election_ms=null placement_ms=null delivery_ms=null
    if [[ "${duration}" -gt 1 ]]; then
        failure_category=product
        wait_for 'survivors elected and caught up during long pause' 35 \
            survivors_settled "${selected_host}" "${round_dir}" \
            || pause_fail product 'survivors did not settle during the failover-length pause'
        pause_target_is "${container_id}" true \
            || pause_fail product 'survivor settlement was observed only after unpause'
        if [[ "${selected_node}" == "${observed_leader}" ]]; then
            election_ms="$(( $(epoch_ms) - requested_ms ))"
        fi
        wait_for 'execution owners relocated during long pause' 35 \
            surviving_owners_ready "${selected_host}" "${round_dir}" "${peer}" \
            || pause_fail product 'execution owners did not relocate during the pause'
        pause_target_is "${container_id}" true \
            || pause_fail product 'placement was observed only after unpause'
        placement_ms="$(( $(epoch_ms) - requested_ms ))"
        local output_before_progress
        output_before_progress="$(topic_end_offset chaos_output)"
        wait_for 'sink traffic progressed with target paused' 20 \
            pause_topic_progressed chaos_output "${output_before_progress}" \
            || pause_fail product 'sink did not progress on surviving nodes during pause'
        output_during="${pause_observed_offset}"
        pause_target_is "${container_id}" true \
            || pause_fail controller 'sink progress was sampled after the pause window'
        delivery_ms="$(( $(epoch_ms) - requested_ms ))"
        control_attempt "${during_canary}" "${peer}" "${round_dir}" 12
        pause_target_is "${container_id}" true \
            || pause_fail product 'long-pause control attempt ended after unpause'
    fi

    failure_category=injection
    wait "${pause_injector_pid}" || pause_fail injection 'Pumba pause command failed'
    pause_injector_pid=""
    if [[ -n "${control_pid}" ]]; then
        wait "${control_pid}" || pause_fail controller 'short-pause control recorder failed'
        wait "${peer_status_pid}" || pause_fail controller 'short-pause peer status recorder failed'
    fi
    grep -Fq "dryrun=false id=${container_id}" "${round_dir}/pumba.txt" \
        || pause_fail injection 'Pumba did not report the selected node paused'
    wait_for_pause_state "${container_id}" false \
        || pause_fail injection 'finite Pumba pause did not unpause the target'
    inspect_target "${container_id}" "${round_dir}/resumed.json"
    local resumed_since
    resumed_since="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    run_bounded 20 docker events --since "${fault_since}" \
        --until "${resumed_since}" --filter "container=${container_id}" \
        --format '{{json .}}' >"${round_dir}/pause-events.ndjson"
    local min_ms max_ms
    if [[ "${duration}" -eq 1 ]]; then
        min_ms=800
        max_ms=2000
    else
        min_ms=15001
        max_ms=100000
    fi
    "${script_dir}/verify-pause-evidence.sh" resumed "${run_id}" "${project_name}" \
        "${selected_host}" "${image_id}" "${round_dir}/before.json" \
        "${round_dir}/resumed.json" "${round_dir}/pause-events.ndjson" \
        "${min_ms}" "${max_ms}" "${round_dir}/duration.json" \
        || pause_fail injection 'actual Docker pause interval missed the declared fault window'
    pause_target_id=""
    local resumed_ms output_at_resume
    resumed_ms="$(jq -r '.pause_ended_ns / 1000000 | floor' "${round_dir}/duration.json")"
    output_at_resume="$(topic_end_offset chaos_output)"
    if [[ "${duration}" -eq 1 ]]; then
        if observer_saw "${observer_id}" "${selected_host}" unavailable "${fault_since}"; then
            printf 'observed\n' >"${round_dir}/short-listener-outage.txt"
        else
            printf 'below-observer-resolution\n' >"${round_dir}/short-listener-outage.txt"
        fi
    fi

    failure_category=product
    phase "${duration}s pause public recovery of ${selected_host}"
    wait_for "observer saw ${selected_host} ready after resume" 30 \
        observer_saw "${observer_id}" "${selected_host}" ready "${resumed_since}"
    for host in "${node_hosts[@]}"; do
        wait_for "${host} listeners available after resume" 30 probe_node "${host}"
    done
    wait_for 'all public cluster and placement observations settled after resume' 150 \
        cluster_settled_and_connected "${round_dir}/recovered"
    local settled_ms="$(( $(epoch_ms) - resumed_ms ))"
    local recovered_leader
    recovered_leader="$(<"${round_dir}/recovered/leader.txt")"
    if [[ "${duration}" -eq 1 ]]; then
        [[ "${recovered_leader}" == "${observed_leader}" ]] \
            || pause_fail product 'short stall changed the observed leader'
        local kind
        for kind in ingestor relay emitter; do
            local before_owner recovered_owner
            before_owner="$(jq -r ".${kind}_owner" "${round_dir}/before/role.json")"
            recovered_owner="$(owner_from_description "${round_dir}/recovered/${kind}.attempt.txt")"
            [[ "${before_owner}" == "${recovered_owner}" ]] \
                || pause_fail product "short stall changed ${kind} placement"
        done
    elif [[ "${selected_node}" == "${observed_leader}" ]]; then
        [[ "${recovered_leader}" != "${observed_leader}" ]] \
            || pause_fail product 'failover-length leader pause did not elect a different leader'
    fi
    check_support_containers
    check_other_nodes ""
    cli_host="${peer}"
    control_attempt "${after_canary}" "${peer}" "${round_dir}"
    [[ "$(jq -r '.outcome' "${round_dir}/control-${after_canary}.json")" == acknowledged ]] \
        || pause_fail product 'post-resume control canary was not acknowledged'
    local canary
    for canary in "${before_canary}" "${after_canary}"; do
        control_effect_present "${canary}" "${round_dir}" \
            || pause_fail product "acknowledged ${canary} effect is absent after resume"
    done
    local during_effect=absent
    if control_effect_present "${during_canary}" "${round_dir}"; then
        during_effect=present
    fi
    if [[ "$(jq -r '.outcome' "${round_dir}/control-${during_canary}.json")" == acknowledged \
        && "${during_effect}" != present ]]; then
        pause_fail product 'acknowledged during-pause control effect is absent after resume'
    fi
    jq -n \
        --slurpfile before "${round_dir}/control-${before_canary}.json" \
        --slurpfile during "${round_dir}/control-${during_canary}.json" \
        --slurpfile after "${round_dir}/control-${after_canary}.json" \
        --arg during_effect "${during_effect}" \
        '{before:$before[0],during:($during[0]+{observed_effect:$during_effect}),after:$after[0],acknowledged_effects_present:true}' \
        >"${round_dir}/control-results.json"
    wait_for 'sink output advanced after resume' 90 \
        topic_progressed chaos_output "${output_at_resume}"
    pause_node_events_expected "${container_id}" "${round_dir}/all-node-events.ndjson" "${fault_since}" \
        || pause_fail product 'a Nervix process restarted or a different node was paused'
    other_node_instances_unchanged "${selected_host}" "${round_dir}/before-all-nodes.json" \
        || pause_fail product 'a non-target node changed process incarnation'
    inspect_target "${container_id}" "${round_dir}/final.json"
    "${script_dir}/verify-pause-evidence.sh" before "${run_id}" "${project_name}" \
        "${selected_host}" "${image_id}" "${round_dir}/before.json" "${round_dir}/final.json"
    for host in "${node_hosts[@]}"; do
        capture_metrics "${host}"
        cp "${artifact_dir}/public/metrics-${host}.txt" "${round_dir}/metrics-${host}.txt"
    done
    run_bounded 20 docker logs --since "${fault_since}" "${observer_id}" \
        >"${round_dir}/observer.log" 2>&1
    trim_file "${round_dir}/observer.log" 1048576
    local source_after output_after
    source_after="$(topic_end_offset chaos_input)"
    output_after="$(topic_end_offset chaos_output)"
    jq -n \
        --slurpfile duration "${round_dir}/duration.json" \
        --slurpfile role "${round_dir}/immediately-before/role.json" \
        --arg target_host "${selected_host}" \
        --arg target_id "${container_id}" \
        --arg recovered_leader "${recovered_leader}" \
        --argjson requested_seconds "${duration}" \
        --argjson election_ms "${election_ms}" \
        --argjson placement_ms "${placement_ms}" \
        --argjson delivery_ms "${delivery_ms}" \
        --argjson recovery_ms "${settled_ms}" \
        --argjson source_before "${source_before}" \
        --argjson source_during "${source_during}" \
        --argjson source_after "${source_after}" \
        --argjson output_before "${output_before}" \
        --argjson output_during "${output_during}" \
        --argjson output_at_resume "${output_at_resume}" \
        --argjson output_after "${output_after}" \
        '{role:$role[0],target_host:$target_host,container_id:$target_id,requested_pause_seconds:$requested_seconds,actual_pause:$duration[0],recovered_leader:$recovered_leader,election_ms:$election_ms,placement_ms:$placement_ms,delivery_during_pause_ms:$delivery_ms,recovery_after_unpause_ms:$recovery_ms,source_offsets:{before:$source_before,during:$source_during,after:$source_after},output_offsets:{before:$output_before,during:$output_during,at_resume:$output_at_resume,after:$output_after},control:"control-results.json",observer:"observer.log",public_status:"recovered/status-nervix-1.attempt.txt",node_events:"all-node-events.ndjson"}' \
        >"${round_dir}/result.json"
}

run_pause_resume() {
    phase 'pause-resume continuous traffic startup'
    mkdir -p "${artifact_dir}/pauses/initial"
    jq -nc --arg run_id "${run_id}" --argjson count "${record_count}" \
        -f "${fixture_generator}" >"${artifact_dir}/fixtures/input.ndjson"
    [[ "$(wc -l <"${artifact_dir}/fixtures/input.ndjson")" -eq "${record_count}" ]] \
        || pause_fail controller 'pause fixture generation was incomplete'
    compose up --detach --no-deps load observer
    wait_for 'independent load, observer and broker' 30 check_support_containers
    wait_for 'source traffic started' 30 topic_progressed chaos_input 0
    wait_for 'sink traffic started' 60 topic_progressed chaos_output 0
    wait_for 'initial cross-node traffic metrics' 60 initial_remote_path_ready
    local host
    for host in "${node_hosts[@]}"; do
        cp "${artifact_dir}/public/metrics-${host}.txt" \
            "${artifact_dir}/pauses/initial/metrics-${host}.txt"
    done
    local remote_path_tmp
    remote_path_tmp="$(mktemp "${artifact_dir}/results/.remote-path.XXXXXX")"
    jq '.traffic_observed_before_pause = true | .traffic_evidence = ["pauses/initial/metrics-nervix-1.txt", "pauses/initial/metrics-nervix-2.txt", "pauses/initial/metrics-nervix-3.txt"]' \
        "${artifact_dir}/results/remote-path.json" >"${remote_path_tmp}"
    mv "${remote_path_tmp}" "${artifact_dir}/results/remote-path.json"

    # Run the longest leader pause last so other cases retain their evidence if a
    # product defect prevents former-leader rejoin.
    pause_one_node 1 leader 1
    pause_one_node 1 execution-owner 2
    pause_one_node 75 execution-owner 3
    pause_one_node 75 leader 4

    phase 'pause-resume final traffic boundary'
    touch "${artifact_dir}/traffic/stop-load"
    local load_id
    load_id="$(owned_service_container load)" || return 1
    wait_for 'load producer flushed and exited' 40 load_exited_cleanly "${load_id}"
    producer_status="$(run_bounded 20 docker inspect --format '{{.State.ExitCode}}' "${load_id}")"
    run_bounded 20 docker logs "${load_id}" >"${artifact_dir}/traffic/producer.log" 2>&1
    trim_file "${artifact_dir}/traffic/producer.log" 1048576
    input_end="$(topic_end_offset chaos_input)"
    [[ "${input_end}" =~ ^[0-9]+$ && "${input_end}" -le "${record_count}" ]] \
        || pause_fail product 'source boundary exceeded the bounded fixture'
    kcat -q -b broker:9092 -C -t chaos_input -p 0 -o beginning -c "${input_end}" \
        >"${artifact_dir}/traffic/accepted-input.ndjson" \
        2>"${artifact_dir}/traffic/source-consumer.stderr"
    [[ "$(wc -l <"${artifact_dir}/traffic/accepted-input.ndjson")" -eq "${input_end}" ]] \
        || pause_fail product 'accepted-input ledger did not cover the final source boundary'
    jq -s \
        --argjson source_end "${input_end}" \
        '{configured_liveness:{raft_heartbeat_interval:"250ms",raft_election_timeout_min:"10s",raft_election_timeout_max:"12s",node_unavailability_timeout:"15s"},source_final_boundary:$source_end,rounds:.}' \
        "${artifact_dir}"/pauses/[1-4]-*/result.json \
        >"${artifact_dir}/results/pause-progress.json"
}
