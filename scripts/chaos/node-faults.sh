#!/usr/bin/env bash
# Sourced by the stateful, domain-time and mixed-instability scenarios. Owns the node faults they
# compose with their fixtures: an abrupt kill with an explicit restart, a graceful stop and restart,
# a finite pause, a peer-side network isolation, and a kill of every node. Each fault proves its own
# effect through Docker inspection, Pumba's own report and the run's live Docker event recording.
# Every fault that a mixed-instability step can hold together with another one comes as an inject
# half and a heal half; the whole faults the other scenarios hold compose those halves, run the
# caller's BEFORE hook immediately before they inject anything and its DURING hook while they hold,
# and leave in node_fault_expected_events the node lifecycle events they are responsible for.

# The moment the latest fault began, in each form a caller reads.
node_fault_since=""
node_fault_since_ns=""
node_fault_started_ms=""
node_fault_expected_events=()
node_fault_restarted_ids=()
# The latest graceful stop's duration and the controller process waiting for the latest pause.
node_fault_stop_ms=""
node_fault_pause_pid=""

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

# True once Docker reports CONTAINER_ID paused, or not paused with false, within LIMIT_MS. The state
# is polled every 50 ms, so a pause of a second cannot pass unobserved.
node_fault_wait_paused() {
    local container_id="$1"
    local expected="$2"
    local limit_ms="$3"
    local deadline_ms=$(($(epoch_ms) + limit_ms))
    while (($(epoch_ms) < deadline_ms)); do
        if [[ "$(run_bounded 10 docker inspect --format '{{.State.Paused}}' "${container_id}" 2>/dev/null)" == "${expected}" ]]; then
            return 0
        fi
        sleep 0.05
    done
    return 1
}

# SIGKILLs HOSTS with one Pumba call whose dry run selects exactly their containers, after the
# BEFORE hook, and verifies each kill through Docker inspection and the live event recording. The
# evidence keeps one file per host, and SECONDS is the outage the caller holds at least.
node_fault_kill_inject() {
    local case_dir="$1"
    local seconds="$2"
    local before_hook="$3"
    shift 3
    local hosts=("$@")
    failure_category=injection
    local container_ids=()
    local container_names=()
    local host
    for host in "${hosts[@]}"; do
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
    [[ "$(grep -Fc 'msg="killing container"' "${case_dir}/pumba-dry-run.txt")" -eq "${#hosts[@]}" ]] \
        || node_fault_fail injection "the Pumba dry run did not select exactly the ${#hosts[@]} node container(s)"
    local index
    for index in "${!container_ids[@]}"; do
        grep -Fq "dryrun=true id=${container_ids[${index}]}" "${case_dir}/pumba-dry-run.txt" \
            || node_fault_fail injection "the Pumba dry run did not select ${hosts[${index}]}"
    done
    record_node_volumes "${case_dir}/volumes-before.json"
    node_fault_begin "${before_hook}"
    jq -n --arg image "${pumba_image_id}" --arg run_id "${run_id}" --arg since "${node_fault_since}" \
        --argjson targets "$(printf '%s\n' "${container_names[@]}" | jq -R . | jq -s .)" \
        --argjson ids "$(printf '%s\n' "${container_ids[@]}" | jq -R . | jq -s .)" \
        --argjson seconds "${seconds}" \
        '{pumba_image_id:$image,command:(["pumba","--label","io.nervix.chaos.run="+$run_id,"--label","io.nervix.chaos.role=node","kill","--signal","SIGKILL"] + $targets),targets:$targets,container_ids:$ids,fault_since:$since,outage_seconds:$seconds}' \
        >"${case_dir}/fault-command.json"
    for index in "${!container_ids[@]}"; do
        recovery_node_stopped "${container_ids[${index}]}"
    done
    node_fault_pumba 30 "${case_dir}/pumba.txt" kill --signal SIGKILL "${container_names[@]}"
    for index in "${!container_ids[@]}"; do
        host="${hosts[${index}]}"
        grep -Fq "dryrun=false id=${container_ids[${index}]}" "${case_dir}/pumba.txt" \
            || node_fault_fail injection "Pumba did not report ${host} killed"
        inspect_target "${container_ids[${index}]}" "${case_dir}/killed-${host}.json"
        node_event_window "${node_fault_since_ns}" "${container_ids[${index}]}" \
            "${case_dir}/kill-events-${host}.ndjson"
        "${script_dir}/verify-crash-evidence.sh" killed "${run_id}" "${project_name}" \
            "${host}" "${image_id}" "${case_dir}/selected-${host}.json" \
            "${case_dir}/killed-${host}.json" "${case_dir}/kill-events-${host}.ndjson"
    done
    failure_category=product
}

# Requires HOSTS still killed, starts each original container with one Docker call, and verifies
# that each kept its image and volume. Waiting for the hosts to serve again is the caller's.
node_fault_kill_restart() {
    local case_dir="$1"
    shift
    local hosts=("$@")
    failure_category=injection
    local container_ids=()
    local host
    for host in "${hosts[@]}"; do
        local container_id
        container_id="$(owned_service_container "${host}")" || return 1
        inspect_target "${container_id}" "${case_dir}/held-${host}.json"
        "${script_dir}/verify-crash-evidence.sh" killed "${run_id}" "${project_name}" \
            "${host}" "${image_id}" "${case_dir}/selected-${host}.json" \
            "${case_dir}/held-${host}.json" "${case_dir}/kill-events-${host}.ndjson"
        container_ids+=("${container_id}")
    done
    run_bounded 60 docker start "${container_ids[@]}" >"${case_dir}/docker-start.txt"
    local index
    for index in "${!container_ids[@]}"; do
        host="${hosts[${index}]}"
        recovery_node_started "${container_ids[${index}]}"
        inspect_target "${container_ids[${index}]}" "${case_dir}/started-${host}.json"
        "${script_dir}/verify-crash-evidence.sh" started "${run_id}" "${project_name}" \
            "${host}" "${image_id}" "${case_dir}/selected-${host}.json" "${case_dir}/started-${host}.json"
    done
    node_volumes_unchanged "${case_dir}/volumes-before.json" "${case_dir}/volumes-after.json"
    failure_category=product
}

# Stops HOST with Pumba and a 60-second grace after the BEFORE hook and requires a completed
# shutdown. When REPLACEMENTS_LIVE says every other node was a live replacement, the stopped node
# must also have handed its work over through the leader; a drain it did not complete leaves its work
# to fail over as after a crash, which keeps the run meaningful, so it is a finding.
node_fault_stop_inject() {
    local case_dir="$1"
    local host="$2"
    local seconds="$3"
    local before_hook="$4"
    local replacements_live="$5"
    failure_category=injection
    local container_id container_name
    container_id="$(owned_service_container "${host}")" || return 1
    inspect_target "${container_id}" "${case_dir}/selected.json"
    "${script_dir}/verify-restart-evidence.sh" before "${run_id}" "${project_name}" \
        "${host}" "${image_id}" "${case_dir}/selected.json" "${case_dir}/selected.json"
    container_name="$(jq -r '.[0].Name | ltrimstr("/")' "${case_dir}/selected.json")"
    node_fault_dry_run "${case_dir}/pumba-dry-run.txt" "${container_id}" 'stopping container' \
        stop --time 60 --limit 1 "${container_name}"
    record_node_volumes "${case_dir}/volumes-before.json"
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
    if [[ "${replacements_live}" == true ]] && ! "${script_dir}/verify-drain-evidence.sh" "${host}" \
        "${case_dir}/shutdown.log" 2>"${case_dir}/drain-verdict.txt"; then
        recovery_finding "${case_dir}" \
            "${host} did not complete its graceful drain while live replacement nodes existed: $(head -n 1 "${case_dir}/drain-verdict.txt")"
    fi
}

# Starts the stopped HOST's original container and verifies that it kept its image and volume.
# Waiting for the host to serve again is the caller's.
node_fault_stop_restart() {
    local case_dir="$1"
    local host="$2"
    failure_category=injection
    local container_id
    container_id="$(owned_service_container "${host}")" || return 1
    run_bounded 30 docker start "${container_id}" >"${case_dir}/docker-start.txt"
    recovery_node_started "${container_id}"
    inspect_target "${container_id}" "${case_dir}/started.json"
    "${script_dir}/verify-restart-evidence.sh" started "${run_id}" "${project_name}" \
        "${host}" "${image_id}" "${case_dir}/selected.json" "${case_dir}/started.json"
    node_volumes_unchanged "${case_dir}/volumes-before.json" "${case_dir}/volumes-after.json"
    failure_category=product
}

# Pauses HOST with a finite Pumba pause of SECONDS after the BEFORE hook and verifies that it took
# effect. The waiting Pumba process is left in node_fault_pause_pid for the resume.
node_fault_pause_inject() {
    local case_dir="$1"
    local host="$2"
    local seconds="$3"
    local before_hook="$4"
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
    node_fault_pause_pid=$!
    node_fault_wait_paused "${container_id}" true 15000 \
        || node_fault_fail injection "Pumba never paused ${host}"
    inspect_target "${container_id}" "${case_dir}/paused.json"
    if jq -e '.[0].State.Paused' "${case_dir}/paused.json" >/dev/null; then
        "${script_dir}/verify-pause-evidence.sh" paused "${run_id}" "${project_name}" \
            "${host}" "${image_id}" "${case_dir}/selected.json" "${case_dir}/paused.json"
    else
        # A pause of a second or two can end before its inspection; its recorded pause and unpause
        # events prove it when it resumes.
        printf '%s\n' 'resumed before the paused inspection' >"${case_dir}/paused-inspection.txt"
    fi
    failure_category=product
}

# Waits for the Pumba process PID that holds HOST's finite pause, requires the node running again,
# and requires the pause interval its live event recording holds to lie within MIN_MS..MAX_MS.
node_fault_pause_resume() {
    local case_dir="$1"
    local host="$2"
    local pid="$3"
    local min_ms="$4"
    local max_ms="$5"
    failure_category=injection
    local container_id
    container_id="$(owned_service_container "${host}")" || return 1
    wait "${pid}" || node_fault_fail injection "the Pumba pause of ${host} failed"
    grep -Fq "dryrun=false id=${container_id}" "${case_dir}/pumba.txt" \
        || node_fault_fail injection "Pumba did not report ${host} paused"
    node_fault_wait_paused "${container_id}" false 15000 \
        || node_fault_fail injection "the finite Pumba pause did not unpause ${host}"
    inspect_target "${container_id}" "${case_dir}/resumed.json"
    local since_ns
    since_ns="$(date -d "$(jq -r '.fault_since' "${case_dir}/fault-command.json")" +%s%N)"
    docker_event_window "${since_ns}" "${case_dir}/pause-events.ndjson" --container "${container_id}" \
        || node_fault_fail controller 'the live Docker event recording does not cover the pause'
    "${script_dir}/verify-pause-evidence.sh" resumed "${run_id}" "${project_name}" \
        "${host}" "${image_id}" "${case_dir}/selected.json" "${case_dir}/resumed.json" \
        "${case_dir}/pause-events.ndjson" "${min_ms}" "${max_ms}" "${case_dir}/pause-duration.json" \
        || node_fault_fail injection 'the actual Docker pause interval missed the declared fault window'
    failure_category=product
}

# SIGKILLs HOST, runs DURING while it is down, holds the outage for SECONDS and starts the same
# container again from its image and volume.
node_fault_kill() {
    local case_dir="$1"
    local host="$2"
    local seconds="$3"
    local before_hook="$4"
    local during_hook="$5"
    node_fault_kill_inject "${case_dir}" "${seconds}" "${before_hook}" "${host}"
    if [[ -n "${during_hook}" ]]; then
        "${during_hook}"
    fi
    node_fault_hold "${seconds}"
    node_fault_kill_restart "${case_dir}" "${host}"
    wait_for "${host} listeners restored" 120 probe_node "${host}"
    node_fault_expected_events=(--target "$(owned_service_container "${host}")"
        --expect kill:9 --expect die:137 --expect start)
}

# Stops HOST gracefully with a 60-second grace, requires a completed shutdown, runs DURING while it
# is stopped, holds the outage for SECONDS and starts the same container again. Every other node is
# a settled live voter when the stop begins, so on more than one node the stop must drain.
node_fault_stop() {
    local case_dir="$1"
    local host="$2"
    local seconds="$3"
    local before_hook="$4"
    local during_hook="$5"
    local replacements_live=false
    if ((node_count > 1)); then
        replacements_live=true
    fi
    node_fault_stop_inject "${case_dir}" "${host}" "${seconds}" "${before_hook}" "${replacements_live}"
    if [[ -n "${during_hook}" ]]; then
        "${during_hook}"
    fi
    node_fault_hold "${seconds}"
    node_fault_stop_restart "${case_dir}" "${host}"
    wait_for "${host} listeners restored" 120 probe_node "${host}"
    node_fault_expected_events=(--target "$(owned_service_container "${host}")"
        --expect kill:15 --expect die:0 --expect start)
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
    node_fault_pause_inject "${case_dir}" "${host}" "${seconds}" "${before_hook}"
    if [[ -n "${during_hook}" ]]; then
        "${during_hook}"
    fi
    node_fault_paused_is "$(owned_service_container "${host}")" true \
        || recovery_finding "${case_dir}" "the observations during the pause of ${host} ended only after it resumed"
    node_fault_pause_resume "${case_dir}" "${host}" "${node_fault_pause_pid}" 15001 100000
    node_fault_expected_events=(--target "$(owned_service_container "${host}")" --expect pause --expect unpause)
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
    node_fault_kill_inject "${case_dir}" "${seconds}" "${before_hook}" "${node_hosts[@]}"
    node_fault_hold "${seconds}"
    node_fault_kill_restart "${case_dir}" "${node_hosts[@]}"
    local host
    for host in "${node_hosts[@]}"; do
        wait_for "${host} listeners restored" 120 probe_node "${host}"
    done
    node_fault_restarted_ids=()
    for host in "${node_hosts[@]}"; do
        node_fault_restarted_ids+=("$(owned_service_container "${host}")")
    done
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
