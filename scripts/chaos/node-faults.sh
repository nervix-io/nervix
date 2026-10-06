#!/usr/bin/env bash
# Sourced by the stateful and domain-time scenarios. Owns the node faults they compose with their
# fixtures: an abrupt kill with an explicit restart, a graceful stop and restart, a finite pause, a
# peer-side network isolation, and a kill of every node. Each fault proves its own effect through
# Docker inspection, Pumba's own report and the run's live Docker event recording, runs the
# caller's BEFORE hook immediately before it injects anything and its DURING hook while it holds,
# and leaves in node_fault_expected_events the node lifecycle events it is responsible for.

# The moment the fault began, in each form a caller reads.
node_fault_since=""
node_fault_since_ns=""
node_fault_started_ms=""
node_fault_expected_events=()
node_fault_restarted_ids=()

node_fault_fail() {
    recovery_fail "$@"
}

node_fault_begin() {
    local before_hook="$1"
    if [[ -n "${before_hook}" ]]; then
        "${before_hook}"
    fi
    node_fault_since="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    node_fault_since_ns="$(date -d "${node_fault_since}" +%s%N)"
    node_fault_started_ms="$(epoch_ms)"
}

# Holds the fault until SECONDS have passed since it began.
node_fault_hold() {
    local seconds="$1"
    local remaining_ms=$((node_fault_started_ms + seconds * 1000 - $(epoch_ms)))
    if ((remaining_ms > 0)); then
        sleep "$((remaining_ms / 1000)).$(printf '%03d' $((remaining_ms % 1000)))"
    fi
}

# Runs a Pumba dry run of COMMAND... against the node container NAME and requires it to select
# exactly CONTAINER_ID with MESSAGE.
node_fault_dry_run() {
    local output="$1"
    local container_id="$2"
    local message="$3"
    shift 3
    run_bounded 20 docker run --rm \
        --label "io.nervix.chaos.run=${run_id}" --label io.nervix.chaos.role=fault \
        --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        "${pumba_image_id}" --dry-run --log-level info \
        --label "io.nervix.chaos.run=${run_id}" --label io.nervix.chaos.role=node \
        "$@" >"${output}" 2>&1
    [[ "$(grep -Fc "msg=\"${message}\"" "${output}")" -eq 1 ]] \
        && grep -Fq "dryrun=true id=${container_id}" "${output}" \
        || node_fault_fail injection "the Pumba dry run did not select exactly ${container_id:0:12}"
}

node_fault_pumba() {
    local limit="$1"
    local output="$2"
    shift 2
    run_bounded "${limit}" docker run --rm \
        --label "io.nervix.chaos.run=${run_id}" --label io.nervix.chaos.role=fault \
        --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        "${pumba_image_id}" --log-level info \
        --label "io.nervix.chaos.run=${run_id}" --label io.nervix.chaos.role=node \
        "$@" >"${output}" 2>&1
}

node_fault_paused_is() {
    [[ "$(run_bounded 20 docker inspect --format '{{.State.Paused}}' "$1")" == "$2" ]]
}

# SIGKILLs HOST, runs DURING while it is down, holds the outage for SECONDS and starts the same
# container again from its image and volume.
node_fault_kill() {
    local case_dir="$1"
    local host="$2"
    local seconds="$3"
    local before_hook="$4"
    local during_hook="$5"
    failure_category=injection
    local container_id container_name
    container_id="$(owned_service_container "${host}")" || return 1
    inspect_target "${container_id}" "${case_dir}/selected.json"
    "${script_dir}/verify-crash-evidence.sh" before "${run_id}" "${project_name}" \
        "${host}" "${image_id}" "${case_dir}/selected.json" "${case_dir}/selected.json"
    container_name="$(jq -r '.[0].Name | ltrimstr("/")' "${case_dir}/selected.json")"
    node_fault_dry_run "${case_dir}/pumba-dry-run.txt" "${container_id}" 'killing container' \
        kill --signal SIGKILL --limit 1 "${container_name}"
    node_fault_begin "${before_hook}"
    jq -n --arg image "${pumba_image_id}" --arg target "${container_name}" --arg id "${container_id}" \
        --arg run_id "${run_id}" --arg since "${node_fault_since}" --argjson seconds "${seconds}" \
        '{pumba_image_id:$image,command:["pumba","--label","io.nervix.chaos.run="+$run_id,"--label","io.nervix.chaos.role=node","kill","--signal","SIGKILL","--limit","1",$target],target:$target,container_id:$id,fault_since:$since,outage_seconds:$seconds}' \
        >"${case_dir}/fault-command.json"
    recovery_node_stopped "${container_id}"
    node_fault_pumba 30 "${case_dir}/pumba.txt" kill --signal SIGKILL --limit 1 "${container_name}"
    grep -Fq "dryrun=false id=${container_id}" "${case_dir}/pumba.txt" \
        || node_fault_fail injection "Pumba did not report ${host} killed"
    inspect_target "${container_id}" "${case_dir}/killed.json"
    node_event_window "${node_fault_since_ns}" "${container_id}" "${case_dir}/kill-events.ndjson"
    "${script_dir}/verify-crash-evidence.sh" killed "${run_id}" "${project_name}" \
        "${host}" "${image_id}" "${case_dir}/selected.json" "${case_dir}/killed.json" \
        "${case_dir}/kill-events.ndjson"
    failure_category=product
    if [[ -n "${during_hook}" ]]; then
        "${during_hook}"
    fi
    node_fault_hold "${seconds}"
    failure_category=injection
    inspect_target "${container_id}" "${case_dir}/held.json"
    "${script_dir}/verify-crash-evidence.sh" killed "${run_id}" "${project_name}" \
        "${host}" "${image_id}" "${case_dir}/selected.json" "${case_dir}/held.json" \
        "${case_dir}/kill-events.ndjson"
    run_bounded 30 docker start "${container_id}" >"${case_dir}/docker-start.txt"
    recovery_node_started "${container_id}"
    inspect_target "${container_id}" "${case_dir}/started.json"
    "${script_dir}/verify-crash-evidence.sh" started "${run_id}" "${project_name}" \
        "${host}" "${image_id}" "${case_dir}/selected.json" "${case_dir}/started.json"
    failure_category=product
    wait_for "${host} listeners restored" 120 probe_node "${host}"
    node_fault_expected_events=(--target "${container_id}" --expect kill:9 --expect die:137 --expect start)
}

# Stops HOST gracefully with a 60-second grace, requires a completed shutdown, runs DURING while it
# is stopped, holds the outage for SECONDS and starts the same container again.
node_fault_stop() {
    local case_dir="$1"
    local host="$2"
    local seconds="$3"
    local before_hook="$4"
    local during_hook="$5"
    failure_category=injection
    local container_id container_name
    container_id="$(owned_service_container "${host}")" || return 1
    inspect_target "${container_id}" "${case_dir}/selected.json"
    "${script_dir}/verify-restart-evidence.sh" before "${run_id}" "${project_name}" \
        "${host}" "${image_id}" "${case_dir}/selected.json" "${case_dir}/selected.json"
    container_name="$(jq -r '.[0].Name | ltrimstr("/")' "${case_dir}/selected.json")"
    node_fault_dry_run "${case_dir}/pumba-dry-run.txt" "${container_id}" 'stopping container' \
        stop --time 60 --limit 1 "${container_name}"
    node_fault_begin "${before_hook}"
    jq -n --arg image "${pumba_image_id}" --arg target "${container_name}" --arg id "${container_id}" \
        --arg run_id "${run_id}" --arg since "${node_fault_since}" --argjson seconds "${seconds}" \
        '{pumba_image_id:$image,command:["pumba","--label","io.nervix.chaos.run="+$run_id,"--label","io.nervix.chaos.role=node","stop","--time","60","--limit","1",$target],target:$target,container_id:$id,fault_since:$since,grace_seconds:60,outage_seconds:$seconds}' \
        >"${case_dir}/fault-command.json"
    recovery_node_stopped "${container_id}"
    local stop_started_ms
    stop_started_ms="$(epoch_ms)"
    node_fault_pumba 75 "${case_dir}/pumba.txt" stop --time 60 --limit 1 "${container_name}"
    node_fault_stop_ms="$(( $(epoch_ms) - stop_started_ms ))"
    inspect_target "${container_id}" "${case_dir}/stopped.json"
    run_bounded 20 docker logs --since "${node_fault_since}" "${container_id}" \
        >"${case_dir}/shutdown.log" 2>&1
    trim_file "${case_dir}/shutdown.log" 2097152
    "${script_dir}/verify-restart-evidence.sh" stopped "${run_id}" "${project_name}" \
        "${host}" "${image_id}" "${case_dir}/selected.json" "${case_dir}/stopped.json" \
        "${case_dir}/shutdown.log"
    grep -E 'shutdown (admission|drain-support|terminal-teardown) phase finished' \
        "${case_dir}/shutdown.log" >"${case_dir}/phase-outcomes.txt" || true
    failure_category=product
    if [[ -n "${during_hook}" ]]; then
        "${during_hook}"
    fi
    node_fault_hold "${seconds}"
    failure_category=injection
    run_bounded 30 docker start "${container_id}" >"${case_dir}/docker-start.txt"
    recovery_node_started "${container_id}"
    inspect_target "${container_id}" "${case_dir}/started.json"
    "${script_dir}/verify-restart-evidence.sh" started "${run_id}" "${project_name}" \
        "${host}" "${image_id}" "${case_dir}/selected.json" "${case_dir}/started.json"
    failure_category=product
    wait_for "${host} listeners restored" 120 probe_node "${host}"
    node_fault_expected_events=(--target "${container_id}" --expect kill:15 --expect die:0 --expect start)
}

# Pauses HOST with Pumba for SECONDS, between 16 and 99 so the pause outlasts the deployed 15-second
# unavailability timeout inside the pause verifier's window, and runs DURING while it is paused.
node_fault_pause() {
    local case_dir="$1"
    local host="$2"
    local seconds="$3"
    local before_hook="$4"
    local during_hook="$5"
    ((seconds > 15 && seconds < 100)) \
        || node_fault_fail setup "a pause must hold between 16 and 99 seconds, not ${seconds}"
    failure_category=injection
    local container_id container_name
    container_id="$(owned_service_container "${host}")" || return 1
    inspect_target "${container_id}" "${case_dir}/selected.json"
    "${script_dir}/verify-pause-evidence.sh" before "${run_id}" "${project_name}" \
        "${host}" "${image_id}" "${case_dir}/selected.json" "${case_dir}/selected.json"
    container_name="$(jq -r '.[0].Name | ltrimstr("/")' "${case_dir}/selected.json")"
    node_fault_dry_run "${case_dir}/pumba-dry-run.txt" "${container_id}" 'pausing container' \
        pause --duration 1ms --limit 1 "${container_name}"
    node_fault_begin "${before_hook}"
    jq -n --arg image "${pumba_image_id}" --arg target "${container_name}" --arg id "${container_id}" \
        --arg run_id "${run_id}" --arg since "${node_fault_since}" --argjson seconds "${seconds}" \
        '{pumba_image_id:$image,command:["pumba","--label","io.nervix.chaos.run="+$run_id,"--label","io.nervix.chaos.role=node","pause","--duration",($seconds|tostring)+"s","--limit","1",$target],target:$target,container_id:$id,fault_since:$since,intended_pause_seconds:$seconds}' \
        >"${case_dir}/fault-command.json"
    node_fault_pumba "$((seconds + 20))" "${case_dir}/pumba.txt" \
        pause --duration "${seconds}s" --limit 1 "${container_name}" &
    local injector_pid=$!
    wait_for "${host} paused" 15 node_fault_paused_is "${container_id}" true \
        || node_fault_fail injection "Pumba never paused ${host}"
    inspect_target "${container_id}" "${case_dir}/paused.json"
    "${script_dir}/verify-pause-evidence.sh" paused "${run_id}" "${project_name}" \
        "${host}" "${image_id}" "${case_dir}/selected.json" "${case_dir}/paused.json"
    failure_category=product
    if [[ -n "${during_hook}" ]]; then
        "${during_hook}"
    fi
    node_fault_paused_is "${container_id}" true \
        || recovery_finding "${case_dir}" "the observations during the pause of ${host} ended only after it resumed"
    failure_category=injection
    wait "${injector_pid}" || node_fault_fail injection "the Pumba pause of ${host} failed"
    grep -Fq "dryrun=false id=${container_id}" "${case_dir}/pumba.txt" \
        || node_fault_fail injection "Pumba did not report ${host} paused"
    wait_for "${host} resumed" 15 node_fault_paused_is "${container_id}" false \
        || node_fault_fail injection "the finite Pumba pause did not unpause ${host}"
    inspect_target "${container_id}" "${case_dir}/resumed.json"
    docker_event_window "${node_fault_since_ns}" "${case_dir}/pause-events.ndjson" \
        --container "${container_id}" \
        || node_fault_fail controller 'the live Docker event recording does not cover the pause'
    "${script_dir}/verify-pause-evidence.sh" resumed "${run_id}" "${project_name}" \
        "${host}" "${image_id}" "${case_dir}/selected.json" "${case_dir}/resumed.json" \
        "${case_dir}/pause-events.ndjson" 15001 100000 "${case_dir}/pause-duration.json" \
        || node_fault_fail injection 'the actual Docker pause interval missed the declared fault window'
    failure_category=product
    node_fault_expected_events=(--target "${container_id}" --expect pause --expect unpause)
}

# Isolates HOST from every peer with Pumba rules placed on the peers, runs DURING while the verified
# isolation holds, keeps it for at least SECONDS and heals it.
node_fault_isolate() {
    local case_dir="$1"
    local host="$2"
    local seconds="$3"
    local before_hook="$4"
    local during_hook="$5"
    local partition_dir="${case_dir}/partition"
    mkdir -p "${partition_dir}"
    plan_isolation isolation "${host}" "${partition_dir}/plan.json"
    node_fault_begin "${before_hook}"
    install_partition "${partition_dir}" 1
    failure_category=product
    if [[ -n "${during_hook}" ]]; then
        "${during_hook}"
    fi
    local hold_until_ms=$((isolation_verified_ms + seconds * 1000))
    local remaining_ms=$((hold_until_ms - $(epoch_ms)))
    if ((remaining_ms > 0)); then
        sleep "$((remaining_ms / 1000)).$(printf '%03d' $((remaining_ms % 1000)))"
    fi
    heal_partition "${partition_dir}" 1
    failure_category=product
    jq -n --arg target "${host}" --argjson seconds "${seconds}" \
        --arg verified_at "${isolation_verified_at}" --arg healed_at "${heal_completed_at}" \
        --argjson window_ms "$((heal_started_ms - isolation_verified_ms))" \
        '{target:$target,minimum_isolation_seconds:$seconds,isolation_verified_at:$verified_at,heal_completed_at:$healed_at,isolation_window_ms:$window_ms,plan:"partition/plan.json",links:{isolated:"partition/links-isolated.json",healed:"partition/links-healed.json"}}' \
        >"${case_dir}/fault-command.json"
    ((heal_started_ms - isolation_verified_ms >= seconds * 1000)) \
        || node_fault_fail controller 'the verified isolation window was shorter than declared'
    node_fault_expected_events=()
}

# SIGKILLs every node with one Pumba call, holds the outage for SECONDS and starts each original
# container from its own volume.
node_fault_restart_cluster() {
    local case_dir="$1"
    local seconds="$2"
    local before_hook="$3"
    failure_category=injection
    local container_ids=()
    local container_names=()
    local host
    for host in "${node_hosts[@]}"; do
        local container_id
        container_id="$(owned_service_container "${host}")" || return 1
        inspect_target "${container_id}" "${case_dir}/selected-${host}.json"
        "${script_dir}/verify-crash-evidence.sh" before "${run_id}" "${project_name}" \
            "${host}" "${image_id}" "${case_dir}/selected-${host}.json" "${case_dir}/selected-${host}.json"
        container_ids+=("${container_id}")
        container_names+=("$(jq -r '.[0].Name | ltrimstr("/")' "${case_dir}/selected-${host}.json")")
    done
    run_bounded 20 docker run --rm \
        --label "io.nervix.chaos.run=${run_id}" --label io.nervix.chaos.role=fault \
        --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        "${pumba_image_id}" --dry-run --log-level info \
        --label "io.nervix.chaos.run=${run_id}" --label io.nervix.chaos.role=node \
        kill --signal SIGKILL "${container_names[@]}" >"${case_dir}/pumba-dry-run.txt" 2>&1
    [[ "$(grep -Fc 'msg="killing container"' "${case_dir}/pumba-dry-run.txt")" -eq "${node_count}" ]] \
        || node_fault_fail injection 'the Pumba dry run did not select exactly every node container'
    record_node_volumes "${case_dir}/volumes-before.json"
    node_fault_begin "${before_hook}"
    jq -n --arg image "${pumba_image_id}" --arg run_id "${run_id}" --arg since "${node_fault_since}" \
        --argjson targets "$(printf '%s\n' "${container_names[@]}" | jq -R . | jq -s .)" \
        --argjson seconds "${seconds}" \
        '{pumba_image_id:$image,command:(["pumba","--label","io.nervix.chaos.run="+$run_id,"--label","io.nervix.chaos.role=node","kill","--signal","SIGKILL"] + $targets),targets:$targets,fault_since:$since,outage_seconds:$seconds}' \
        >"${case_dir}/fault-command.json"
    local index
    for index in "${!container_ids[@]}"; do
        recovery_node_stopped "${container_ids[${index}]}"
    done
    node_fault_pumba 30 "${case_dir}/pumba.txt" kill --signal SIGKILL "${container_names[@]}"
    for index in "${!container_ids[@]}"; do
        host="${node_hosts[${index}]}"
        grep -Fq "dryrun=false id=${container_ids[${index}]}" "${case_dir}/pumba.txt" \
            || node_fault_fail injection "Pumba did not report ${host} killed"
        inspect_target "${container_ids[${index}]}" "${case_dir}/killed-${host}.json"
        node_event_window "${node_fault_since_ns}" "${container_ids[${index}]}" \
            "${case_dir}/kill-events-${host}.ndjson"
        "${script_dir}/verify-crash-evidence.sh" killed "${run_id}" "${project_name}" \
            "${host}" "${image_id}" "${case_dir}/selected-${host}.json" \
            "${case_dir}/killed-${host}.json" "${case_dir}/kill-events-${host}.ndjson"
    done
    node_fault_hold "${seconds}"
    run_bounded 60 docker start "${container_ids[@]}" >"${case_dir}/docker-start.txt"
    for index in "${!container_ids[@]}"; do
        host="${node_hosts[${index}]}"
        recovery_node_started "${container_ids[${index}]}"
        inspect_target "${container_ids[${index}]}" "${case_dir}/started-${host}.json"
        "${script_dir}/verify-crash-evidence.sh" started "${run_id}" "${project_name}" \
            "${host}" "${image_id}" "${case_dir}/selected-${host}.json" "${case_dir}/started-${host}.json"
    done
    failure_category=product
    for host in "${node_hosts[@]}"; do
        wait_for "${host} listeners restored" 120 probe_node "${host}"
    done
    node_volumes_unchanged "${case_dir}/volumes-before.json" "${case_dir}/volumes-after.json"
    node_fault_restarted_ids=("${container_ids[@]}")
    node_fault_expected_events=()
}

# Checks the node lifecycle events the live recording holds from the fault through now against
# the events the fault is responsible for, recording a product finding for any other.
node_fault_check_events() {
    local case_dir="$1"
    docker_event_window "${node_fault_since_ns}" "${case_dir}/node-events.ndjson" --role node \
        || node_fault_fail controller 'the live Docker event recording does not cover the fault through recovery'
    if ((${#node_fault_restarted_ids[@]} > 0)); then
        local index
        for index in "${!node_fault_restarted_ids[@]}"; do
            local host="${node_hosts[${index}]}"
            jq -c --arg id "${node_fault_restarted_ids[${index}]}" 'select(.Actor.ID == $id)' \
                "${case_dir}/node-events.ndjson" >"${case_dir}/node-events-${host}.ndjson"
            "${script_dir}/verify-docker-events.sh" lifecycle \
                --events "${case_dir}/node-events-${host}.ndjson" \
                --target "${node_fault_restarted_ids[${index}]}" \
                --expect kill:9 --expect die:137 --expect start \
                || recovery_finding "${case_dir}" "${host} had node lifecycle events other than its SIGKILL, exit and explicit start"
        done
        node_fault_restarted_ids=()
        return 0
    fi
    "${script_dir}/verify-docker-events.sh" lifecycle --events "${case_dir}/node-events.ndjson" \
        ${node_fault_expected_events[@]+"${node_fault_expected_events[@]}"} \
        || recovery_finding "${case_dir}" 'node lifecycle events differ from the planned fault'
}
