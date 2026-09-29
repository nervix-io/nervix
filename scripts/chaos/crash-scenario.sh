#!/usr/bin/env bash
# Sourced by run-baseline.sh after the external cluster and graph are ready.

crash_fail() {
    failure_category="$1"
    shift
    printf 'crash %s failure: %s\n' "${failure_category}" "$*" >&2
    return 1
}

owner_from_description() {
    awk -F ': ' '$1 == "owner" {print $2; exit}' "$1"
}

observe_crash_target() {
    local output_dir="$1"
    mkdir -p "${output_dir}"
    cluster_settled "${output_dir}" || return 1
    local leader ingestor_owner emitter_owner
    leader="$(<"${output_dir}/leader.txt")"
    ingestor_owner="$(owner_from_description "${output_dir}/ingestor.attempt.txt")"
    emitter_owner="$(owner_from_description "${output_dir}/emitter.attempt.txt")"
    [[ "${leader}" =~ ^node-[123]$ \
        && "${ingestor_owner}" =~ ^node-[123]$ \
        && "${emitter_owner}" =~ ^node-[123]$ ]] || return 1

    case "${scenario}" in
        leader-crash) target_node="${leader}" ;;
        follower-crash)
            target_node=""
            local node
            for node in "${node_names[@]}"; do
                if [[ "${node}" != "${leader}" ]]; then
                    target_node="${node}"
                    break
                fi
            done
            ;;
        ingestor-owner-crash) target_node="${ingestor_owner}" ;;
        emitter-owner-crash) target_node="${emitter_owner}" ;;
    esac
    [[ "${target_node}" =~ ^node-[123]$ ]] || return 1
    target_host="nervix-${target_node##*-}"
    local host_found=false
    local host
    for host in "${node_hosts[@]}"; do
        if [[ "${host}" == "${target_host}" ]]; then
            host_found=true
            break
        fi
    done
    [[ "${host_found}" == true ]] || return 1
    jq -n \
        --arg scenario "${scenario}" \
        --arg leader "${leader}" \
        --arg ingestor "${ingestor_owner}" \
        --arg emitter "${emitter_owner}" \
        --arg target "${target_node}" \
        '{scenario:$scenario,leader:$leader,ingestor_owner:$ingestor,emitter_owner:$emitter,target_node:$target}' \
        >"${output_dir}/role.json"
}

confirm_crash_target_before_fault() {
    local selected_node="$1"
    local output_dir="$2"
    # A committed canary can still leave sequential public status reads at different log indexes.
    if ! wait_for 'settled public role immediately before SIGKILL' 30 \
        observe_crash_target "${output_dir}"; then
        crash_fail injection 'settled public role was unavailable before SIGKILL'
        return 1
    fi
    [[ "${target_node}" == "${selected_node}" ]] \
        || crash_fail injection "observed role moved from ${selected_node} to ${target_node} before SIGKILL"
}

control_attempt() {
    local name="$1"
    local host="$2"
    local output_dir="$3"
    local status=0
    local outcome=acknowledged
    local limit="${4:-60}"
    if [[ "${name}" == chaos_canary_during ]]; then
        limit=20
    fi
    run_bounded "${limit}" docker compose "${compose_args[@]}" run --rm --no-deps admin \
        nervix-cli --server "http://${host}:47391" --domain chaos_baseline \
        --password "${CHAOS_PASSWORD}" --command "CREATE RESOURCE ${name};" \
        >"${output_dir}/control-${name}.txt" 2>&1 || status=$?
    if [[ "${status}" -ne 0 ]] \
        || grep -Eq 'Error:|^error:' "${output_dir}/control-${name}.txt"; then
        outcome=uncertain
    fi
    jq -n \
        --arg name "${name}" \
        --arg host "${host}" \
        --arg outcome "${outcome}" \
        --argjson exit_code "${status}" \
        '{name:$name,host:$host,outcome:$outcome,exit_code:$exit_code}' \
        >"${output_dir}/control-${name}.json"
}

control_effect_present() {
    local name="$1"
    local output_dir="$2"
    domain_cli_command "DESCRIBE RESOURCE ${name};" \
        >"${output_dir}/describe-${name}.txt" 2>&1
}

surviving_owners_ready() {
    local target="$1"
    local output_dir="$2"
    local survivor="$3"
    cli_host="${survivor}"
    domain_cli_command 'DESCRIBE INGESTOR chaos_ingestor;' \
        >"${output_dir}/ingestor-survivor.attempt.txt" 2>&1 || return 1
    domain_cli_command 'DESCRIBE RELAY chaos_records;' \
        >"${output_dir}/relay-survivor.attempt.txt" 2>&1 || return 1
    domain_cli_command 'DESCRIBE EMITTER chaos_emitter;' \
        >"${output_dir}/emitter-survivor.attempt.txt" 2>&1 || return 1
    local description owner
    for description in ingestor relay emitter; do
        owner="$(owner_from_description "${output_dir}/${description}-survivor.attempt.txt")"
        [[ "${owner}" =~ ^node-[123]$ && "${owner}" != "node-${target##*-}" ]] \
            || return 1
    done
    grep -Fxq 'status: running' "${output_dir}/ingestor-survivor.attempt.txt" || return 1
    grep -Fxq 'ready: true' "${output_dir}/ingestor-survivor.attempt.txt" || return 1
    grep -Fxq 'status: OK' "${output_dir}/emitter-survivor.attempt.txt" || return 1
}

other_node_instances_unchanged() {
    local target="$1"
    local before="$2"
    local host
    for host in "${node_hosts[@]}"; do
        [[ "${host}" == "${target}" ]] && continue
        local container_id before_started current_started
        container_id="$(owned_service_container "${host}")" || return 1
        before_started="$(jq -r --arg id "${container_id}" \
            '.[] | select(.Id == $id) | .State.StartedAt' "${before}")"
        [[ -n "${before_started}" ]] || return 1
        current_started="$(run_bounded 20 docker inspect --format '{{.State.StartedAt}}' "${container_id}")" \
            || return 1
        [[ "${current_started}" == "${before_started}" ]] || return 1
        container_running "${container_id}" || return 1
    done
}

run_crash() {
    phase 'crash traffic startup'
    local crash_dir="${artifact_dir}/crash"
    mkdir -p "${crash_dir}/initial" "${crash_dir}/before" "${crash_dir}/immediately-before"
    jq -nc --arg run_id "${run_id}" --argjson count "${record_count}" \
        -f "${fixture_generator}" >"${artifact_dir}/fixtures/input.ndjson"
    [[ "$(wc -l <"${artifact_dir}/fixtures/input.ndjson")" -eq "${record_count}" ]] \
        || crash_fail controller 'crash fixture generation was incomplete'
    compose up --detach --no-deps load observer
    wait_for 'independent load, observer and broker' 30 check_support_containers
    wait_for 'source traffic started' 30 topic_progressed chaos_input 0
    wait_for 'sink traffic started' 60 topic_progressed chaos_output 0
    wait_for 'initial cross-node traffic metrics' 60 initial_remote_path_ready
    local host
    for host in "${node_hosts[@]}"; do
        cp "${artifact_dir}/public/metrics-${host}.txt" \
            "${crash_dir}/initial/metrics-${host}.txt"
    done
    local remote_path_tmp
    remote_path_tmp="$(mktemp "${artifact_dir}/results/.remote-path.XXXXXX")"
    jq '.traffic_observed_before_crash = true' \
        "${artifact_dir}/results/remote-path.json" >"${remote_path_tmp}"
    mv "${remote_path_tmp}" "${artifact_dir}/results/remote-path.json"

    failure_category=product
    phase 'observed crash role and control canary'
    wait_for 'settled cluster before role selection' 90 \
        observe_crash_target "${crash_dir}/before"
    local selected_node="${target_node}"
    local selected_host="${target_host}"
    cli_host="${node_hosts[0]}"
    control_attempt chaos_canary_before "${cli_host}" "${crash_dir}"
    [[ "$(jq -r '.outcome' "${crash_dir}/control-chaos_canary_before.json")" == acknowledged ]] \
        || crash_fail product 'precrash control canary was not acknowledged'
    control_effect_present chaos_canary_before "${crash_dir}" \
        || crash_fail product 'acknowledged precrash control canary is absent'
    check_support_containers
    check_other_nodes ""

    phase "SIGKILL ${scenario}: ${selected_host}"
    failure_category=injection
    local container_id container_name
    container_id="$(owned_service_container "${selected_host}")" || return 1
    inspect_target "${container_id}" "${crash_dir}/selected.json"
    "${script_dir}/verify-crash-evidence.sh" before "${run_id}" "${project_name}" \
        "${selected_host}" "${image_id}" "${crash_dir}/selected.json" "${crash_dir}/selected.json"
    container_name="$(jq -r '.[0].Name | ltrimstr("/")' "${crash_dir}/selected.json")"
    run_bounded 20 docker run --rm \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=fault \
        --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        "${pumba_image_id}" --dry-run --log-level info \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=node \
        kill --signal SIGKILL --limit 1 "${container_name}" \
        >"${crash_dir}/pumba-dry-run.txt" 2>&1
    [[ "$(grep -Fc 'msg="killing container"' "${crash_dir}/pumba-dry-run.txt")" -eq 1 ]] \
        && grep -Fq "dryrun=true id=${container_id}" "${crash_dir}/pumba-dry-run.txt" \
        && grep -Fq "name=/${container_name} signal=SIGKILL" "${crash_dir}/pumba-dry-run.txt" \
        || crash_fail injection 'Pumba dry run did not resolve the exact observed container'

    confirm_crash_target_before_fault "${selected_node}" "${crash_dir}/immediately-before"
    inspect_target "${container_id}" "${crash_dir}/before.json"
    "${script_dir}/verify-crash-evidence.sh" before "${run_id}" "${project_name}" \
        "${selected_host}" "${image_id}" "${crash_dir}/selected.json" "${crash_dir}/before.json"
    [[ "$(owned_service_container "${selected_host}")" == "${container_id}" ]] \
        || crash_fail injection 'observed container changed before SIGKILL'
    local node_container_ids=()
    for host in "${node_hosts[@]}"; do
        node_container_ids+=("$(owned_service_container "${host}")")
    done
    run_bounded 20 docker inspect "${node_container_ids[@]}" \
        >"${crash_dir}/before-all-nodes.json"

    local source_before output_before
    source_before="$(topic_end_offset chaos_input)"
    output_before="$(topic_end_offset chaos_output)"
    [[ "${source_before}" =~ ^[0-9]+$ && "${output_before}" =~ ^[0-9]+$ ]] \
        || crash_fail setup 'broker offsets unavailable before SIGKILL'
    local fault_since fault_since_ns kill_requested_ms kill_completed_ms
    fault_since="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    fault_since_ns="$(date -d "${fault_since}" +%s%N)"
    kill_requested_ms="$(epoch_ms)"
    jq -n \
        --arg image "${pumba_image_id}" \
        --arg target "${container_name}" \
        --arg container_id "${container_id}" \
        --arg run_id "${run_id}" \
        --arg fault_since "${fault_since}" \
        --argjson outage_seconds "${outage_seconds}" \
        --argjson source_offset "${source_before}" \
        --argjson output_offset "${output_before}" \
        '{pumba_image_id:$image,command:["pumba","--label","io.nervix.chaos.run="+$run_id,"--label","io.nervix.chaos.role=node","kill","--signal","SIGKILL","--limit","1",$target],target:$target,container_id:$container_id,fault_since:$fault_since,outage_seconds:$outage_seconds,source_offset_before:$source_offset,output_offset_before:$output_offset,public_role:"immediately-before/role.json"}' \
        >"${crash_dir}/fault-command.json"

    crash_target_id="${container_id}"
    crash_restarted=false

    run_bounded 30 docker run --rm \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=fault \
        --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        "${pumba_image_id}" --log-level info \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=node \
        kill --signal SIGKILL --limit 1 "${container_name}" \
        >"${crash_dir}/pumba.txt" 2>&1
    kill_completed_ms="$(epoch_ms)"
    grep -Fq "dryrun=false id=${container_id}" "${crash_dir}/pumba.txt" \
        || crash_fail injection 'Pumba did not report the selected container killed'
    inspect_target "${container_id}" "${crash_dir}/killed.json"
    docker_event_window "${fault_since_ns}" "${crash_dir}/kill-events.ndjson" \
        --container "${container_id}" \
        || crash_fail controller 'the live Docker event recording does not cover the SIGKILL'
    "${script_dir}/verify-crash-evidence.sh" killed "${run_id}" "${project_name}" \
        "${selected_host}" "${image_id}" "${crash_dir}/before.json" \
        "${crash_dir}/killed.json" "${crash_dir}/kill-events.ndjson"
    check_support_containers
    check_other_nodes "${selected_host}"

    failure_category=product
    phase 'outage and failover observations'
    compose_call_timeout=20
    local observer_id
    observer_id="$(owned_service_container observer)" || return 1
    wait_for "observer saw ${selected_host} unavailable" 20 \
        observer_saw "${observer_id}" "${selected_host}" unavailable "${fault_since}"
    local control_host="${selected_host}"
    if [[ "${node_count}" == 3 ]]; then
        for host in "${node_hosts[@]}"; do
            if [[ "${host}" != "${selected_host}" ]]; then
                control_host="${host}"
                break
            fi
        done
    fi
    control_attempt chaos_canary_during "${control_host}" "${crash_dir}"

    local election_ms=null placement_ms=null delivery_resume_ms=null
    if [[ "${node_count}" == 3 ]]; then
        wait_for 'survivors elected and caught up' 90 \
            survivors_settled "${selected_host}" "${crash_dir}"
        if [[ "${selected_node}" == "$(jq -r '.leader' "${crash_dir}/immediately-before/role.json")" ]]; then
            election_ms="$(( $(epoch_ms) - kill_requested_ms ))"
        fi
        wait_for 'all execution owners placed on survivors' 120 \
            surviving_owners_ready "${selected_host}" "${crash_dir}" "${control_host}"
        placement_ms="$(( $(epoch_ms) - kill_requested_ms ))"
        local output_after_placement
        output_after_placement="$(topic_end_offset chaos_output)"
        [[ "${output_after_placement}" =~ ^[0-9]+$ ]] \
            || crash_fail product 'sink offset unavailable during owner recovery'
        wait_for 'sink traffic resumed during outage' 60 \
            topic_progressed chaos_output "${output_after_placement}"
        delivery_resume_ms="$(( $(epoch_ms) - kill_requested_ms ))"
    fi
    wait_for 'source traffic progressed during outage' 30 \
        topic_progressed chaos_input "${source_before}"
    local elapsed_seconds=$(( ( $(epoch_ms) - kill_requested_ms ) / 1000 ))
    if ((elapsed_seconds < outage_seconds)); then
        sleep "$((outage_seconds - elapsed_seconds))"
    fi
    inspect_target "${container_id}" "${crash_dir}/held.json"
    "${script_dir}/verify-crash-evidence.sh" killed "${run_id}" "${project_name}" \
        "${selected_host}" "${image_id}" "${crash_dir}/before.json" \
        "${crash_dir}/held.json" "${crash_dir}/kill-events.ndjson"
    check_support_containers
    check_other_nodes "${selected_host}"
    local source_while_stopped output_while_stopped
    source_while_stopped="$(topic_end_offset chaos_input)"
    output_while_stopped="$(topic_end_offset chaos_output)"
    local outage_ms="$(( $(epoch_ms) - kill_requested_ms ))"

    phase "explicit Docker restart of ${selected_host}"
    failure_category=injection
    local start_requested_ms
    start_requested_ms="$(epoch_ms)"
    run_bounded 30 docker start "${container_id}" >"${crash_dir}/docker-start.txt"
    crash_restarted=true
    inspect_target "${container_id}" "${crash_dir}/started.json"
    "${script_dir}/verify-crash-evidence.sh" started "${run_id}" "${project_name}" \
        "${selected_host}" "${image_id}" "${crash_dir}/before.json" "${crash_dir}/started.json"

    failure_category=product
    phase 'public recovery and control reconciliation'
    wait_for "${selected_host} listeners restored" 120 probe_node "${selected_host}"
    local listener_ms="$(( $(epoch_ms) - start_requested_ms ))"
    wait_for "observer saw ${selected_host} ready again" 30 \
        observer_saw "${observer_id}" "${selected_host}" ready "${fault_since}"
    for host in "${node_hosts[@]}"; do
        wait_for "${host} listeners reachable" 30 probe_node "${host}"
    done
    wait_for 'all nodes caught up and execution settled' 150 \
        cluster_settled "${crash_dir}"
    local settled_ms="$(( $(epoch_ms) - start_requested_ms ))"
    check_support_containers
    check_other_nodes ""
    wait_for 'output progressed after restart' 90 \
        topic_progressed chaos_output "${output_while_stopped}"
    if [[ "${node_count}" == 1 ]]; then
        delivery_resume_ms="$(( $(epoch_ms) - kill_requested_ms ))"
    fi
    cli_host="${node_hosts[0]}"
    control_attempt chaos_canary_after "${cli_host}" "${crash_dir}"
    [[ "$(jq -r '.outcome' "${crash_dir}/control-chaos_canary_after.json")" == acknowledged ]] \
        || crash_fail product 'postcrash control canary was not acknowledged'
    local canary_name
    for canary_name in chaos_canary_before chaos_canary_after; do
        control_effect_present "${canary_name}" "${crash_dir}" \
            || crash_fail product "acknowledged ${canary_name} effect is absent after recovery"
    done
    local during_effect=absent
    if control_effect_present chaos_canary_during "${crash_dir}"; then
        during_effect=present
    fi
    if [[ "$(jq -r '.outcome' "${crash_dir}/control-chaos_canary_during.json")" == acknowledged \
        && "${during_effect}" != present ]]; then
        crash_fail product 'acknowledged during-outage control effect is absent after recovery'
    fi
    jq -n \
        --slurpfile before "${crash_dir}/control-chaos_canary_before.json" \
        --slurpfile during "${crash_dir}/control-chaos_canary_during.json" \
        --slurpfile after "${crash_dir}/control-chaos_canary_after.json" \
        --arg during_effect "${during_effect}" \
        '{before:$before[0],during:($during[0]+{observed_effect:$during_effect}),after:$after[0],acknowledged_effects_present:true}' \
        >"${crash_dir}/control-results.json"
    local source_after output_after
    source_after="$(topic_end_offset chaos_input)"
    output_after="$(topic_end_offset chaos_output)"
    docker_event_window "${fault_since_ns}" "${crash_dir}/all-node-events.ndjson" --role node \
        || crash_fail controller 'the live Docker event recording does not cover the fault through recovery'
    "${script_dir}/verify-docker-events.sh" lifecycle \
        --events "${crash_dir}/all-node-events.ndjson" --target "${container_id}" \
        --expect kill:9 --expect die:137 --expect start \
        || crash_fail product 'node lifecycle events other than the SIGKILL, its exit and the explicit restart occurred during the fault'
    other_node_instances_unchanged "${selected_host}" "${crash_dir}/before-all-nodes.json" \
        || crash_fail product 'a non-target node changed container or process incarnation'
    inspect_target "${container_id}" "${crash_dir}/final.json"
    "${script_dir}/verify-crash-evidence.sh" recovered "${run_id}" "${project_name}" \
        "${selected_host}" "${image_id}" "${crash_dir}/started.json" \
        "${crash_dir}/final.json" \
        || crash_fail product 'the target restarted unexpectedly after the explicit Docker start'
    for host in "${node_hosts[@]}"; do
        capture_metrics "${host}"
        cp "${artifact_dir}/public/metrics-${host}.txt" "${crash_dir}/metrics-${host}.txt"
    done
    run_bounded 20 docker logs --since "${fault_since}" "${observer_id}" \
        >"${crash_dir}/observer.log" 2>&1
    trim_file "${crash_dir}/observer.log" 1048576
    jq -n \
        --arg scenario "${scenario}" \
        --arg node "${selected_node}" \
        --arg container_id "${container_id}" \
        --arg observed_leader "$(jq -r '.leader' "${crash_dir}/immediately-before/role.json")" \
        --arg recovered_leader "$(<"${crash_dir}/leader.txt")" \
        --arg fault_since "${fault_since}" \
        --argjson kill_ms "$((kill_completed_ms - kill_requested_ms))" \
        --argjson outage_ms "${outage_ms}" \
        --argjson election_ms "${election_ms}" \
        --argjson placement_ms "${placement_ms}" \
        --argjson delivery_resume_ms "${delivery_resume_ms}" \
        --argjson listener_recovery_ms "${listener_ms}" \
        --argjson settled_recovery_ms "${settled_ms}" \
        --argjson source_before "${source_before}" \
        --argjson source_while_stopped "${source_while_stopped}" \
        --argjson source_after "${source_after}" \
        --argjson output_before "${output_before}" \
        --argjson output_while_stopped "${output_while_stopped}" \
        --argjson output_after "${output_after}" \
        '{scenario:$scenario,node:$node,container_id:$container_id,observed_leader:$observed_leader,recovered_leader:$recovered_leader,fault_since:$fault_since,kill_duration_ms:$kill_ms,held_outage_ms:$outage_ms,election_ms:$election_ms,placement_ms:$placement_ms,delivery_resume_ms:$delivery_resume_ms,listener_recovery_ms:$listener_recovery_ms,settled_recovery_ms:$settled_recovery_ms,source_offset_before:$source_before,source_offset_while_stopped:$source_while_stopped,source_offset_after:$source_after,output_offset_before:$output_before,output_offset_while_stopped:$output_while_stopped,output_offset_after:$output_after,kill_verified:true,listener_recovered:true,cluster_settled:true,control:"crash/control-results.json",node_events:"crash/all-node-events.ndjson",node_event_recording:"crash/all-node-events.recording.json"}' \
        >"${artifact_dir}/results/crash-progress.json"

    phase 'crash traffic final boundary'
    touch "${artifact_dir}/traffic/stop-load"
    local load_id
    load_id="$(owned_service_container load)" || return 1
    wait_for 'load producer flushed and exited' 40 load_exited_cleanly "${load_id}"
    producer_status="$(run_bounded 20 docker inspect --format '{{.State.ExitCode}}' "${load_id}")"
    run_bounded 20 docker logs "${load_id}" >"${artifact_dir}/traffic/producer.log" 2>&1
    trim_file "${artifact_dir}/traffic/producer.log" 1048576
    input_end="$(topic_end_offset chaos_input)"
    [[ "${input_end}" =~ ^[0-9]+$ && "${input_end}" -le "${record_count}" ]] \
        || crash_fail product 'source boundary exceeded the bounded fixture'
    kcat -q -b broker:9092 -C -t chaos_input -p 0 -o beginning -c "${input_end}" \
        >"${artifact_dir}/traffic/accepted-input.ndjson" \
        2>"${artifact_dir}/traffic/source-consumer.stderr"
    [[ "$(wc -l <"${artifact_dir}/traffic/accepted-input.ndjson")" -eq "${input_end}" ]] \
        || crash_fail product 'accepted-input ledger did not cover the source boundary'
}
