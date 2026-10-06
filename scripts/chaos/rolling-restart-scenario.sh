#!/usr/bin/env bash
# Sourced by run-baseline.sh after the shared external cluster and graph are ready.

rolling_fail() {
    printf '%s failure: %s\n' "${scenario}" "$*" >&2
    return 1
}

epoch_ms() {
    local epoch_ns
    epoch_ns="$(date +%s%N)"
    printf '%d\n' "$((epoch_ns / 1000000))"
}

owned_service_container() {
    local service="$1"
    local ids
    ids="$(run_bounded 20 docker container ls --all --no-trunc --quiet \
        --filter "label=io.nervix.chaos.run=${run_id}" \
        --filter "label=com.docker.compose.project=${project_name}" \
        --filter "label=com.docker.compose.service=${service}")" || return 1
    if [[ ! "${ids}" =~ ^[a-f0-9]{64}$ ]]; then
        rolling_fail "expected exactly one owned ${service} container, found '${ids}'"
        return 1
    fi
    printf '%s\n' "${ids}"
}

container_running() {
    local container_id="$1"
    [[ "$(run_bounded 20 docker inspect --format '{{.State.Running}}' "${container_id}")" == true ]]
}

check_support_containers() {
    local service
    for service in broker load observer; do
        local container_id
        container_id="$(owned_service_container "${service}")" || return 1
        if ! container_running "${container_id}"; then
            failure_category=setup
            if [[ "${service}" == load ]]; then
                rolling_fail 'load fixture ended before fault verification; increase --records'
                return 1
            fi
            rolling_fail "${service} stopped during fault traffic"
            return 1
        fi
    done
}

inspect_target() {
    local container_id="$1"
    local output="$2"
    run_bounded 20 docker inspect "${container_id}" >"${output}"
}

check_other_nodes() {
    local target="$1"
    local host
    for host in "${node_hosts[@]}"; do
        [[ "${host}" == "${target}" ]] && continue
        local container_id
        container_id="$(owned_service_container "${host}")" || return 1
        if ! container_running "${container_id}"; then
            rolling_fail "non-target ${host} stopped"
            return 1
        fi
    done
}

status_leader() {
    local file="$1"
    awk -F ': ' '$1 == "raft.current_leader" {print $2}' "${file}"
}

status_is_settled() {
    local host="$1"
    local file="$2"
    local expected_id="node-${host##*-}"
    grep -Fxq "raft.id: ${expected_id}" "${file}" || return 1
    grep -Fxq 'raft.cordoned_nodes: (none)' "${file}" || return 1
    grep -Fq -- '- chaos_baseline status=Running' "${file}" || return 1
    ! grep -Fq 'terminating: true' "${file}" || return 1
    ! grep -Eq 'state_recovery=(pending|recovering|failed)' "${file}" || return 1
    local leader
    leader="$(status_leader "${file}")"
    [[ "${leader}" =~ ^node-[123]$ ]] || return 1
    local last_log last_applied
    last_log="$(awk -F ': ' '$1 == "raft.last_log_index" {print $2}' "${file}")"
    last_applied="$(awk -F ': ' '$1 == "raft.last_applied" {print $2}' "${file}")"
    [[ "${last_log}" =~ ^[0-9]+$ && "${last_log}" == "${last_applied}" ]] || return 1
    local node
    for node in "${node_names[@]}"; do
        grep -Eq "^- ${node} \\[voter\\] nervix-[123]:47395$" "${file}" || return 1
        grep -Fq "node_id: ${node}" "${file}" || return 1
    done
}

cluster_settled() {
    local output_dir="$1"
    local leader=""
    local term=""
    local last_log=""
    local host
    for host in "${node_hosts[@]}"; do
        cli_host="${host}"
        local file="${output_dir}/status-${host}.attempt.txt"
        cli_command 'SHOW CLUSTER STATUS;' >"${file}" 2>&1 || return 1
        status_is_settled "${host}" "${file}" || return 1
        local observed_leader
        observed_leader="$(status_leader "${file}")"
        local observed_term observed_log
        observed_term="$(awk -F ': ' '$1 == "raft.current_term" {print $2}' "${file}")"
        observed_log="$(awk -F ': ' '$1 == "raft.last_log_index" {print $2}' "${file}")"
        [[ "${observed_term}" =~ ^[0-9]+$ && "${observed_log}" =~ ^[0-9]+$ ]] || return 1
        if [[ -n "${leader}" ]]; then
            [[ "${leader}" == "${observed_leader}" \
                && "${term}" == "${observed_term}" \
                && "${last_log}" == "${observed_log}" ]] || return 1
        fi
        leader="${observed_leader}"
        term="${observed_term}"
        last_log="${observed_log}"
    done
    cli_host="${node_hosts[0]}"
    domain_cli_command 'DESCRIBE INGESTOR chaos_ingestor;' \
        >"${output_dir}/ingestor.attempt.txt" 2>&1 || return 1
    grep -Fxq 'status: running' "${output_dir}/ingestor.attempt.txt" || return 1
    grep -Fxq 'ready: true' "${output_dir}/ingestor.attempt.txt" || return 1
    domain_cli_command 'DESCRIBE EMITTER chaos_emitter;' \
        >"${output_dir}/emitter.attempt.txt" 2>&1 || return 1
    grep -Fxq 'status: OK' "${output_dir}/emitter.attempt.txt" || return 1
    domain_cli_command 'DESCRIBE RELAY chaos_records;' \
        >"${output_dir}/relay.attempt.txt" 2>&1 || return 1
    grep -Eq '^owner: node-[123]$' "${output_dir}/relay.attempt.txt" || return 1
    printf '%s\n' "${leader}" >"${output_dir}/leader.txt"
}

# Settled as above, and every node also reports no warnings and a healthy application probe to each
# of its peers.
cluster_settled_and_connected() {
    local output_dir="$1"
    cluster_settled "${output_dir}" || return 1
    local host peer
    for host in "${node_hosts[@]}"; do
        local status_file="${output_dir}/status-${host}.attempt.txt"
        awk '/^\[warnings\]$/ { found = 1; getline; if ($0 == "- none") clean = 1 }
             END { exit !(found && clean) }' "${status_file}" || return 1
        for peer in "${node_hosts[@]}"; do
            [[ "${peer}" == "${host}" ]] && continue
            grep -Eq "^- node-${peer##*-}: .* status=connected$" "${status_file}" \
                || return 1
        done
    done
}

survivors_settled() {
    local target="$1"
    local output_dir="$2"
    local leader=""
    local term=""
    local last_log=""
    local host
    for host in "${node_hosts[@]}"; do
        [[ "${host}" == "${target}" ]] && continue
        cli_host="${host}"
        local file="${output_dir}/survivor-${host}.attempt.txt"
        cli_command 'SHOW CLUSTER STATUS;' >"${file}" 2>&1 || return 1
        grep -Fxq "raft.id: node-${host##*-}" "${file}" || return 1
        grep -Fq -- '- chaos_baseline status=Running' "${file}" || return 1
        local observed_leader observed_term observed_log observed_applied
        observed_leader="$(status_leader "${file}")"
        observed_term="$(awk -F ': ' '$1 == "raft.current_term" {print $2}' "${file}")"
        observed_log="$(awk -F ': ' '$1 == "raft.last_log_index" {print $2}' "${file}")"
        observed_applied="$(awk -F ': ' '$1 == "raft.last_applied" {print $2}' "${file}")"
        [[ "${observed_leader}" =~ ^node-[123]$ ]] || return 1
        [[ "${observed_leader}" != "node-${target##*-}" ]] || return 1
        [[ "${observed_term}" =~ ^[0-9]+$ && "${observed_log}" =~ ^[0-9]+$ ]] || return 1
        [[ "${observed_log}" == "${observed_applied}" ]] || return 1
        local node
        for node in "${node_names[@]}"; do
            grep -Eq "^- ${node} \\[voter\\] nervix-[123]:47395$" "${file}" || return 1
            if [[ "${node}" != "node-${target##*-}" ]]; then
                grep -Fq "node_id: ${node}" "${file}" || return 1
            fi
        done
        if [[ -n "${leader}" ]]; then
            [[ "${leader}" == "${observed_leader}" \
                && "${term}" == "${observed_term}" \
                && "${last_log}" == "${observed_log}" ]] || return 1
        fi
        leader="${observed_leader}"
        term="${observed_term}"
        last_log="${observed_log}"
    done
    printf '%s\n' "${leader}" >"${output_dir}/survivor-leader.txt"
}

topic_progressed() {
    local topic="$1"
    local previous="$2"
    local current
    current="$(topic_end_offset "${topic}")" || return 1
    [[ "${current}" =~ ^[0-9]+$ ]] && ((current > previous))
}

load_exited_cleanly() {
    local container_id="$1"
    local state
    state="$(run_bounded 20 docker inspect --format '{{.State.Running}} {{.State.ExitCode}}' "${container_id}")" || return 1
    [[ "${state}" == 'false 0' ]]
}

observer_saw() {
    local container_id="$1"
    local host="$2"
    local state="$3"
    local since="$4"
    run_bounded 20 docker logs --since "${since}" "${container_id}" 2>&1 \
        | grep -F "${host} ${state}" >/dev/null
}

metric_is_positive() {
    local file="$1"
    shift
    local line
    while IFS= read -r line; do
        [[ "${line}" == nervix_messages_total\{* ]] || continue
        local expected
        local matches=true
        for expected in "$@"; do
            if [[ "${line}" != *"${expected}"* ]]; then
                matches=false
                break
            fi
        done
        if [[ "${matches}" == true ]]; then
            local total="${line##* }"
            if [[ "${total}" =~ ^[0-9]+$ ]] && ((total > 0)); then
                return 0
            fi
        fi
    done <"${file}"
    return 1
}

initial_remote_path_ready() {
    local host
    for host in "${node_hosts[@]}"; do
        capture_metrics "${host}" >/dev/null 2>&1 || return 1
    done
    metric_is_positive "${artifact_dir}/public/metrics-nervix-1.txt" \
        'direction="sent"' 'physical_node_id="node-1"' 'relay="chaos_records"' \
        'target="chaos_ingestor"' || return 1
    if [[ "${node_count}" == "3" ]]; then
        metric_is_positive "${artifact_dir}/public/metrics-nervix-2.txt" \
            'direction="received"' 'physical_node_id="node-2"' 'relay="chaos_records"' \
            'target="chaos_records"' 'target_kind="RELAY"' || return 1
        metric_is_positive "${artifact_dir}/public/metrics-nervix-3.txt" \
            'direction="received"' 'physical_node_id="node-3"' 'relay="chaos_records"' \
            'target="chaos_emitter"' 'target_kind="EMITTER"' || return 1
    else
        metric_is_positive "${artifact_dir}/public/metrics-nervix-1.txt" \
            'direction="received"' 'physical_node_id="node-1"' 'relay="chaos_records"' \
            'target="chaos_emitter"' 'target_kind="EMITTER"' || return 1
    fi
}

restart_one_node() {
    local target="$1"
    local ordinal="$2"
    local round_dir="${artifact_dir}/restarts/${ordinal}-${target}"
    mkdir -p "${round_dir}"
    phase "graceful restart ${ordinal}/${node_count}: ${target}"
    check_support_containers
    check_other_nodes "${target}"
    wait_for "settled cluster before stopping ${target}" 120 cluster_settled "${round_dir}"
    local observed_leader
    observed_leader="$(<"${round_dir}/leader.txt")"
    local source_before output_before
    source_before="$(topic_end_offset chaos_input)"
    output_before="$(topic_end_offset chaos_output)"
    [[ "${source_before}" =~ ^[0-9]+$ && "${output_before}" =~ ^[0-9]+$ ]] \
        || rolling_fail 'broker offsets unavailable before stop'

    local container_id container_name
    container_id="$(owned_service_container "${target}")" || return 1
    inspect_target "${container_id}" "${round_dir}/before.json"
    "${script_dir}/verify-restart-evidence.sh" before "${run_id}" "${project_name}" \
        "${target}" "${image_id}" "${round_dir}/before.json" "${round_dir}/before.json"
    container_name="$(jq -r '.[0].Name | ltrimstr("/")' "${round_dir}/before.json")"
    run_bounded 20 docker run --rm \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=fault \
        --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        "${pumba_image_id}" \
        --dry-run --log-level info \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=node \
        stop --time 60 --limit 1 "${container_name}" \
        >"${round_dir}/pumba-dry-run.txt" 2>&1
    [[ "$(grep -Fc 'msg="stopping container"' "${round_dir}/pumba-dry-run.txt")" -eq 1 ]] \
        && grep -Fq "dryrun=true id=${container_id}" "${round_dir}/pumba-dry-run.txt" \
        && grep -Fq "name=/${container_name}" "${round_dir}/pumba-dry-run.txt" \
        || rolling_fail "Pumba did not resolve exactly the owned ${target} container"
    local stopped_at
    stopped_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    local stop_requested_ms
    stop_requested_ms="$(epoch_ms)"
    jq -n \
        --arg image "${pumba_image_id}" \
        --arg target "${container_name}" \
        --arg container_id "${container_id}" \
        --arg run_id "${run_id}" \
        --arg stopped_at "${stopped_at}" \
        --arg observed_leader "${observed_leader}" \
        --argjson grace_seconds 60 \
        --argjson source_offset "${source_before}" \
        --argjson output_offset "${output_before}" \
        '{pumba_image_id:$image, command:["pumba","--label","io.nervix.chaos.run="+$run_id,"--label","io.nervix.chaos.role=node","stop","--time","60","--limit","1",$target], target:$target, container_id:$container_id, stopped_at:$stopped_at, observed_leader:$observed_leader, grace_seconds:$grace_seconds, source_offset_before:$source_offset, output_offset_before:$output_offset}' \
        >"${round_dir}/fault-command.json"

    run_bounded 75 docker run --rm \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=fault \
        --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        "${pumba_image_id}" \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=node \
        stop --time 60 --limit 1 "${container_name}" \
        >"${round_dir}/pumba.txt" 2>&1
    local stop_completed_at stop_completed_ms
    stop_completed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    stop_completed_ms="$(epoch_ms)"

    inspect_target "${container_id}" "${round_dir}/stopped.json"
    run_bounded 20 docker logs --since "${stopped_at}" "${container_id}" \
        >"${round_dir}/shutdown.log" 2>&1
    trim_file "${round_dir}/shutdown.log" 2097152
    "${script_dir}/verify-restart-evidence.sh" stopped "${run_id}" "${project_name}" \
        "${target}" "${image_id}" "${round_dir}/before.json" "${round_dir}/stopped.json" \
        "${round_dir}/shutdown.log"
    grep -E 'shutdown (admission|drain-support|terminal-teardown) phase finished' \
        "${round_dir}/shutdown.log" >"${round_dir}/phase-outcomes.txt"
    if [[ "${node_count}" == "3" ]]; then
        # The other two nodes were settled, uncordoned voters when the stop began, so the stopped
        # node had a live replacement and must have handed its work over rather than leaving it
        # to fail over.
        if ! "${script_dir}/verify-drain-evidence.sh" "${target}" "${round_dir}/shutdown.log"; then
            failure_category=product
            rolling_fail "${target} did not complete its graceful drain while a live replacement node existed"
        fi
    fi
    check_support_containers
    check_other_nodes "${target}"
    local observer_id
    observer_id="$(owned_service_container observer)" || return 1
    wait_for "observer saw ${target} unavailable" 20 \
        observer_saw "${observer_id}" "${target}" unavailable "${stopped_at}"
    wait_for "source traffic progressed while ${target} was stopped" 20 \
        topic_progressed chaos_input "${source_before}"

    if [[ "${node_count}" == "3" ]]; then
        wait_for "survivors agree on a caught-up leader after ${target} stop" 90 \
            survivors_settled "${target}" "${round_dir}"
        local output_stopped
        output_stopped="$(topic_end_offset chaos_output)"
        [[ "${output_stopped}" =~ ^[0-9]+$ ]] || rolling_fail 'sink offset unavailable during stop'
        wait_for "sink traffic progressed while ${target} was stopped" 60 \
            topic_progressed chaos_output "${output_stopped}"
    fi
    local source_while_stopped output_while_stopped
    source_while_stopped="$(topic_end_offset chaos_input)"
    output_while_stopped="$(topic_end_offset chaos_output)"

    local restarted_at
    restarted_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    local start_requested_ms
    start_requested_ms="$(epoch_ms)"
    run_bounded 30 docker start "${container_id}" >"${round_dir}/docker-start.txt"
    inspect_target "${container_id}" "${round_dir}/started.json"
    "${script_dir}/verify-restart-evidence.sh" started "${run_id}" "${project_name}" \
        "${target}" "${image_id}" "${round_dir}/before.json" "${round_dir}/started.json"
    wait_for "${target} listeners restored" 120 probe_node "${target}"
    local listeners_restored_at listeners_restored_ms
    listeners_restored_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    listeners_restored_ms="$(epoch_ms)"
    wait_for "observer saw ${target} ready again" 30 \
        observer_saw "${observer_id}" "${target}" ready "${restarted_at}"
    local host
    for host in "${node_hosts[@]}"; do
        wait_for "${host} listeners reachable after ${target} restart" 30 probe_node "${host}"
    done
    wait_for "all nodes caught up and execution settled after ${target} restart" 150 \
        cluster_settled "${round_dir}"
    local settled_at settled_ms
    settled_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    settled_ms="$(epoch_ms)"
    check_support_containers
    check_other_nodes ""
    wait_for "output progressed after ${target} restart" 90 \
        topic_progressed chaos_output "${output_before}"
    local source_after output_after
    source_after="$(topic_end_offset chaos_input)"
    output_after="$(topic_end_offset chaos_output)"
    for host in "${node_hosts[@]}"; do
        capture_metrics "${host}"
        cp "${artifact_dir}/public/metrics-${host}.txt" "${round_dir}/metrics-${host}.txt"
    done
    run_bounded 20 docker logs --since "${stopped_at}" "${observer_id}" \
        >"${round_dir}/observer.log" 2>&1
    trim_file "${round_dir}/observer.log" 1048576
    jq -n \
        --arg node "${target}" \
        --arg container_id "${container_id}" \
        --arg observed_leader "${observed_leader}" \
        --arg recovered_leader "$(<"${round_dir}/leader.txt")" \
        --arg stopped_at "${stopped_at}" \
        --arg stop_completed_at "${stop_completed_at}" \
        --arg restarted_at "${restarted_at}" \
        --arg listeners_restored_at "${listeners_restored_at}" \
        --arg settled_at "${settled_at}" \
        --argjson stop_duration_ms "$((stop_completed_ms - stop_requested_ms))" \
        --argjson listener_recovery_ms "$((listeners_restored_ms - start_requested_ms))" \
        --argjson settled_recovery_ms "$((settled_ms - start_requested_ms))" \
        --argjson source_before "${source_before}" \
        --argjson source_while_stopped "${source_while_stopped}" \
        --argjson source_after "${source_after}" \
        --argjson output_before "${output_before}" \
        --argjson output_while_stopped "${output_while_stopped}" \
        --argjson output_after "${output_after}" \
        '{node:$node,container_id:$container_id,observed_leader:$observed_leader,recovered_leader:$recovered_leader,stopped_at:$stopped_at,stop_completed_at:$stop_completed_at,restarted_at:$restarted_at,listeners_restored_at:$listeners_restored_at,settled_at:$settled_at,stop_duration_ms:$stop_duration_ms,listener_recovery_ms:$listener_recovery_ms,settled_recovery_ms:$settled_recovery_ms,exit_code:0,graceful_teardown:true,source_offset_before:$source_before,source_offset_while_stopped:$source_while_stopped,source_offset_after:$source_after,output_offset_before:$output_before,output_offset_while_stopped:$output_while_stopped,output_offset_after:$output_after,listener_recovered:true,cluster_settled:true,phase_outcomes:"phase-outcomes.txt"}' \
        >"${round_dir}/result.json"
}

run_rolling_restart() {
    phase 'rolling traffic startup'
    mkdir -p "${artifact_dir}/restarts"
    jq -nc --arg run_id "${run_id}" --argjson count "${record_count}" \
        -f "${fixture_generator}" >"${artifact_dir}/fixtures/input.ndjson"
    [[ "$(wc -l <"${artifact_dir}/fixtures/input.ndjson")" -eq "${record_count}" ]] \
        || rolling_fail 'rolling fixture generation was incomplete'
    compose up --detach --no-deps load observer
    wait_for 'independent load, observer and broker' 30 check_support_containers
    wait_for 'source traffic started' 30 topic_progressed chaos_input 0
    wait_for 'sink traffic started' 60 topic_progressed chaos_output 0
    wait_for 'initial cross-node traffic metrics' 60 initial_remote_path_ready
    mkdir -p "${artifact_dir}/restarts/initial"
    for host in "${node_hosts[@]}"; do
        cp "${artifact_dir}/public/metrics-${host}.txt" \
            "${artifact_dir}/restarts/initial/metrics-${host}.txt"
    done
    local remote_path_tmp
    remote_path_tmp="$(mktemp "${artifact_dir}/results/.remote-path.XXXXXX")"
    jq \
        --arg source_metrics 'restarts/initial/metrics-nervix-1.txt' \
        --arg relay_metrics "restarts/initial/metrics-nervix-$([[ "${node_count}" == "3" ]] && printf 2 || printf 1).txt" \
        --arg emitter_metrics "restarts/initial/metrics-nervix-$([[ "${node_count}" == "3" ]] && printf 3 || printf 1).txt" \
        '.traffic_evidence = [$source_metrics, $relay_metrics, $emitter_metrics]
         | .traffic_observed_before_restarts = true' \
        "${artifact_dir}/results/remote-path.json" >"${remote_path_tmp}"
    mv "${remote_path_tmp}" "${artifact_dir}/results/remote-path.json"
    wait_for 'initial cluster settled' 90 cluster_settled "${artifact_dir}/restarts"
    local initial_leader
    initial_leader="$(<"${artifact_dir}/restarts/leader.txt")"
    [[ "${initial_leader}" =~ ^node-[123]$ ]] || rolling_fail 'no observed leader before rotation'
    local targets=("nervix-${initial_leader##*-}")
    local host
    for host in "${node_hosts[@]}"; do
        [[ "${host}" == "${targets[0]}" ]] || targets+=("${host}")
    done
    local ordinal=0
    for host in "${targets[@]}"; do
        ordinal=$((ordinal + 1))
        restart_one_node "${host}" "${ordinal}"
    done

    phase 'rolling traffic final boundary'
    touch "${artifact_dir}/traffic/stop-load"
    local load_id
    load_id="$(owned_service_container load)" || return 1
    wait_for 'load producer flushed and exited' 40 load_exited_cleanly "${load_id}"
    producer_status="$(run_bounded 20 docker inspect --format '{{.State.ExitCode}}' "${load_id}")"
    run_bounded 20 docker logs "${load_id}" >"${artifact_dir}/traffic/producer.log" 2>&1
    trim_file "${artifact_dir}/traffic/producer.log" 1048576
    input_end="$(topic_end_offset chaos_input)"
    [[ "${input_end}" =~ ^[0-9]+$ && "${input_end}" -le "${record_count}" ]] \
        || rolling_fail 'source boundary exceeded the bounded fixture'
    kcat -q -b broker:9092 -C -t chaos_input -p 0 -o beginning -c "${input_end}" \
        >"${artifact_dir}/traffic/accepted-input.ndjson" \
        2>"${artifact_dir}/traffic/source-consumer.stderr"
    [[ "$(wc -l <"${artifact_dir}/traffic/accepted-input.ndjson")" -eq "${input_end}" ]] \
        || rolling_fail 'accepted-input ledger did not cover the final source boundary'
    jq -s \
        --arg observed_leader "${initial_leader}" \
        --argjson source_end "${input_end}" \
        '{observed_initial_leader:$observed_leader,source_final_boundary:$source_end,rounds:.}' \
        "${artifact_dir}"/restarts/[0-9]-nervix-*/result.json \
        >"${artifact_dir}/results/rolling-progress.json"
}
