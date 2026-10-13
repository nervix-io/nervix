#!/usr/bin/env bash
# Sourced by the degraded-links and mixed-instability scenarios after partition-scenario.sh. Owns
# degrading one directed node link with Pumba netem: the qualified profiles, measuring the link with
# ICMP and with a transfer its receiver counts, installing one owned netem configuration on the sender
# after every node interface proved default, and healing it through the injector's SIGTERM.

link_degradation_fail() {
    failure_category="$1"
    shift
    printf '%s %s failure: %s\n' "${scenario}" "${failure_category}" "$*" >&2
    return 1
}

# Sets link_degradation_effect to the netem arguments of PROFILE.
link_degradation_effect() {
    case "$1" in
        delay) link_degradation_effect=(delay --time 180 --jitter 0 --correlation 0) ;;
        jitter) link_degradation_effect=(delay --time 180 --jitter 100 --correlation 0 --distribution normal) ;;
        random-loss) link_degradation_effect=(loss --percent 30 --correlation 0) ;;
        burst-loss) link_degradation_effect=(loss-state --p13 20 --p31 15 --p32 0 --p23 100 --p14 0) ;;
        rate-limit) link_degradation_effect=(rate --rate 256kbit) ;;
        combined) link_degradation_effect=(combine --delay --delay-time 180 --delay-jitter 60
            --loss --loss-percent 20 --rate --rate-value 256kbit --) ;;
    esac
}

# Samples RECEIVER from SENDER's network namespace in approximately four seconds, and summarizes
# the complete packet count as JSON beside OUTPUT. Correlated burst loss needs more observations
# in that same window; every other profile and healthy observation uses forty packets.
link_ping() {
    [[ "$#" -eq 4 ]] || {
        link_degradation_fail observation 'link ping needs output, sender, receiver and profile'
        return 1
    }
    local output="$1"
    local sender="$2"
    local receiver="$3"
    local profile="$4"
    local probe_count=40 probe_interval=0.1
    case "${profile}" in
        burst-loss) probe_count=400; probe_interval=0.01 ;;
        baseline | healed | delay | jitter | random-loss | rate-limit | combined) ;;
        *)
            link_degradation_fail observation "unknown link probe profile ${profile}"
            return 1
            ;;
    esac
    local sender_id address
    sender_id="$(owned_service_container "${sender}")"
    address="$(node_address "${receiver}")"
    local ping_status=0
    run_bounded 30 docker run --rm \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=link-probe \
        --network "container:${sender_id}" "${CHAOS_PROBE_IMAGE}" \
        ping -c "${probe_count}" -i "${probe_interval}" -W 1 "${address}" >"${output}" 2>&1 || ping_status=$?
    [[ "${ping_status}" -eq 0 || "${ping_status}" -eq 1 ]] \
        || link_degradation_fail observation "ping helper failed with ${ping_status}"
    "${script_dir}/verify-degraded-evidence.sh" ping "${output}" "${output%.txt}.json" "${probe_count}"
}

# Times one 262,144-byte transfer from the sender's connect to receiver completion on their shared
# kernel clock, keeping Docker control time separate, within the same total budget. BusyBox nc
# half-closes after stdin ends and exits when the receiver closes its side; an idle timeout would
# abandon a valid loss-stalled connection. Keep raw logs and complete helper outcomes on failure.
link_rate_probe() {
    local output="$1" sender="$2" receiver="$3"
    local sender_id receiver_id receiver_ip server_id
    local evidence="${output%.json}"
    local transfer_deadline_seconds=90
    local helper_script
    helper_script="$(cat "$(dirname "${BASH_SOURCE[0]}")/link-rate-helper.sh")"
    sender_id="$(owned_service_container "${sender}")"
    receiver_id="$(owned_service_container "${receiver}")"
    receiver_ip="$(node_address "${receiver}")"
    local server_start_status=0 ready=false
    server_id="$(run_bounded 30 docker run --detach \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=link-probe \
        --network "container:${receiver_id}" --entrypoint sh "${CHAOS_PROBE_IMAGE}" \
        -c "${helper_script}" -- receiver 2>"${evidence}.server-start.log")" \
        || server_start_status=$?
    if [[ "${server_start_status}" -eq 0 ]]; then
        local readiness_deadline=$((SECONDS + 10))
        while ((SECONDS < readiness_deadline)); do
            if run_bounded 2 docker exec "${server_id}" sh -c \
                'netstat -ltn | grep -q ":18081 "' \
                >"${evidence}.readiness.log" 2>&1; then
                ready=true
                break
            fi
            sleep 0.2
        done
    fi
    local started=null finished=null status=null server_status=null receiver_exit_code=null
    local sender_cleanup_status=0 server_cleanup_status=0 log_status=null receiver_bytes=""
    local sender_helper_id=""
    local transfer_started=null transfer_finished=null sender_resolution=null receiver_resolution=null
    local sender_clock_namespace="" receiver_clock_namespace=""
    local sender_boot_id="" receiver_boot_id="" sender_boottime_offset="" receiver_boottime_offset=""
    if [[ "${ready}" == true ]]; then
        started="$(epoch_ms)"
        local transfer_deadline=$((SECONDS + transfer_deadline_seconds))
        status=0
        run_bounded "${transfer_deadline_seconds}" docker run --rm \
            --cidfile "${evidence}.sender-id.txt" \
            --label "io.nervix.chaos.run=${run_id}" \
            --label io.nervix.chaos.role=link-probe \
            --network "container:${sender_id}" --entrypoint sh "${CHAOS_PROBE_IMAGE}" \
            -c "${helper_script}" -- sender "${receiver_ip}" >"${evidence}.log" 2>&1 || status=$?
        if [[ "${status}" -eq 0 ]]; then
            transfer_started="$(jq -s -er 'select(length == 1) | .[0].started_uptime_ms
                | select(type == "number" and . > 0 and . == floor)' "${evidence}.log")" \
                || transfer_started=null
            sender_resolution="$(jq -s -er 'select(length == 1) | .[0].clock_resolution_ms
                | select(. == 10)' "${evidence}.log")" || sender_resolution=null
            sender_clock_namespace="$(jq -s -er 'select(length == 1) | .[0].clock_namespace
                | select(type == "string" and test("^time:\\[[0-9]+\\]$"))' "${evidence}.log")" \
                || sender_clock_namespace=""
            sender_boot_id="$(jq -s -er 'select(length == 1) | .[0].boot_id
                | select(type == "string" and test("^[0-9a-f-]{36}$"))' "${evidence}.log")" || sender_boot_id=""
            sender_boottime_offset="$(jq -s -er 'select(length == 1) | .[0].boottime_offset
                | select(type == "string" and test("^-?[0-9]+ [0-9]+$"))' "${evidence}.log")" || sender_boottime_offset=""
        fi
        if [[ -s "${evidence}.sender-id.txt" ]]; then
            sender_helper_id="$(cat "${evidence}.sender-id.txt")"
            # Timing out the Docker client does not end the container. End this exact sender so
            # its peer can finish; the run's exit trap remains responsible if cleanup fails.
            if [[ "${status}" -ne 0 ]]; then
                run_bounded 20 docker container rm --force "${sender_helper_id}" \
                    >"${evidence}.sender-cleanup.log" 2>&1 || sender_cleanup_status=$?
            fi
        fi
        local receiver_seconds=$((transfer_deadline - SECONDS))
        server_status=124
        : >"${evidence}.server-exit.txt"
        if ((receiver_seconds > 0)); then
            server_status=0
            run_bounded "${receiver_seconds}" docker wait "${server_id}" \
                >"${evidence}.server-exit.txt" 2>&1 || server_status=$?
        fi
        finished="$(epoch_ms)"
        if [[ "${server_status}" -eq 0 ]]; then
            receiver_exit_code="$(tr -d '[:space:]' <"${evidence}.server-exit.txt")"
        fi
    fi
    if [[ "${server_start_status}" -eq 0 ]]; then
        log_status=0
        run_bounded 20 docker logs "${server_id}" >"${evidence}.server.log" \
            2>"${evidence}.server.stderr.log" || log_status=$?
        if [[ "${log_status}" -eq 0 ]]; then
            receiver_bytes="$(jq -s -er 'select(length == 1) | .[0].received_bytes
                | select(type == "number" and . >= 0 and . == floor)' "${evidence}.server.log")" \
                || receiver_bytes=""
            transfer_finished="$(jq -s -er 'select(length == 1) | .[0].finished_uptime_ms
                | select(type == "number" and . > 0 and . == floor)' "${evidence}.server.log")" \
                || transfer_finished=null
            receiver_resolution="$(jq -s -er 'select(length == 1) | .[0].clock_resolution_ms
                | select(. == 10)' "${evidence}.server.log")" || receiver_resolution=null
            receiver_clock_namespace="$(jq -s -er 'select(length == 1) | .[0].clock_namespace
                | select(type == "string" and test("^time:\\[[0-9]+\\]$"))' "${evidence}.server.log")" \
                || receiver_clock_namespace=""
            receiver_boot_id="$(jq -s -er 'select(length == 1) | .[0].boot_id
                | select(type == "string" and test("^[0-9a-f-]{36}$"))' "${evidence}.server.log")" || receiver_boot_id=""
            receiver_boottime_offset="$(jq -s -er 'select(length == 1) | .[0].boottime_offset
                | select(type == "string" and test("^-?[0-9]+ [0-9]+$"))' "${evidence}.server.log")" || receiver_boottime_offset=""
        fi
        run_bounded 20 docker container rm --force "${server_id}" \
            >"${evidence}.server-cleanup.log" 2>&1 || server_cleanup_status=$?
    fi
    jq -n --arg sender "${sender}" --arg receiver "${receiver}" --arg address "${receiver_ip}" \
        --arg sender_container_id "${sender_id}" --arg receiver_container_id "${receiver_id}" \
        --arg sender_helper_id "${sender_helper_id}" --arg receiver_helper_id "${server_id}" \
        --argjson ready "${ready}" --argjson started "${started}" --argjson finished "${finished}" \
        --argjson deadline "${transfer_deadline_seconds}" --argjson sender_status "${status}" \
        --argjson start_status "${server_start_status}" --argjson wait_status "${server_status}" \
        --arg exit_code "${receiver_exit_code}" --arg bytes "${receiver_bytes}" \
        --argjson log_status "${log_status}" --argjson sender_cleanup "${sender_cleanup_status}" \
        --argjson server_cleanup "${server_cleanup_status}" \
        --argjson transfer_started "${transfer_started}" --argjson transfer_finished "${transfer_finished}" \
        --argjson sender_resolution "${sender_resolution}" --argjson receiver_resolution "${receiver_resolution}" \
        --arg sender_clock_namespace "${sender_clock_namespace}" --arg receiver_clock_namespace "${receiver_clock_namespace}" \
        --arg sender_boot_id "${sender_boot_id}" --arg receiver_boot_id "${receiver_boot_id}" \
        --arg sender_boottime_offset "${sender_boottime_offset}" --arg receiver_boottime_offset "${receiver_boottime_offset}" '
        def count: if test("^[0-9]+$") then tonumber else null end;
        {sender:$sender,receiver:$receiver,address:$address,port:18081,bytes:262144,
         sender_container_id:$sender_container_id,receiver_container_id:$receiver_container_id,
         sender_helper_id:$sender_helper_id,receiver_helper_id:$receiver_helper_id,
         receiver_ready:$ready,transfer_deadline_seconds:$deadline,
         started_at_ms:$started,finished_at_ms:$finished,
         sender_status:$sender_status,receiver_start_status:$start_status,
         receiver_wait_status:$wait_status,receiver_exit_code:($exit_code | count),
         receiver_log_status:$log_status,received_bytes:($bytes | count),
         sender_cleanup_status:$sender_cleanup,receiver_cleanup_status:$server_cleanup,
         transfer_started_uptime_ms:$transfer_started,transfer_finished_uptime_ms:$transfer_finished,
         sender_clock_resolution_ms:$sender_resolution,receiver_clock_resolution_ms:$receiver_resolution,
         sender_clock_namespace:$sender_clock_namespace,receiver_clock_namespace:$receiver_clock_namespace,
         sender_boot_id:$sender_boot_id,receiver_boot_id:$receiver_boot_id,
         sender_boottime_offset:$sender_boottime_offset,receiver_boottime_offset:$receiver_boottime_offset,
         helper_lifetime_ms:(if $started == null or $finished == null then null else $finished-$started end)}' \
        >"${evidence}.probe.json"
    local endpoint="rate probe ${sender} -> ${receiver} (${receiver_ip}:18081)"
    if [[ "${server_start_status}" -ne 0 || "${ready}" != true ]]; then
        link_degradation_fail observation "${endpoint}: receiver did not listen; see ${evidence}.probe.json and receiver logs"
        return 1
    fi
    if [[ "${status}" -ne 0 || "${server_status}" -ne 0 || "${receiver_exit_code}" != 0 \
        || "${receiver_bytes}" != 262144 || "${log_status}" -ne 0 \
        || "${sender_cleanup_status}" -ne 0 || "${server_cleanup_status}" -ne 0 ]]; then
        link_degradation_fail observation "${endpoint}: sender=${status}, receiver wait=${server_status}, exit=${receiver_exit_code}, bytes=${receiver_bytes:-unknown}/262144, logs=${log_status}, cleanup=${sender_cleanup_status}/${server_cleanup_status}; see ${evidence}.probe.json and helper logs"
        return 1
    fi
    if [[ "${transfer_started}" == null || "${transfer_finished}" == null \
        || "${sender_resolution}" != 10 || "${receiver_resolution}" != 10 ]] \
        || [[ -z "${sender_clock_namespace}" || -z "${receiver_clock_namespace}" \
            || -z "${sender_boot_id}" || "${sender_boot_id}" != "${receiver_boot_id}" \
            || -z "${sender_boottime_offset}" || "${sender_boottime_offset}" != "${receiver_boottime_offset}" ]] \
        || ((transfer_finished < transfer_started)); then
        link_degradation_fail observation "${endpoint}: invalid helper transfer clocks; see ${evidence}.probe.json and raw helper logs"
        return 1
    fi
    # /proc/uptime has ten-millisecond resolution. Keep an upper duration bound and its precision;
    # the effect verifier compares the fault's lower bound to twice the baseline's upper bound.
    jq -n --argjson bytes 262144 --argjson received_bytes "${receiver_bytes}" \
        --argjson duration_ms "$((transfer_finished - transfer_started + 10))" \
        '{bytes:$bytes,received_bytes:$received_bytes,duration_ms:$duration_ms,clock_resolution_ms:10,
          bytes_per_second:($received_bytes * 1000 / $duration_ms)}' \
        >"${output}"
}

# Refuses to overlap faults unless every node interface is in its default state, then starts the
# detached Pumba injector NAME that degrades the link from SENDER to RECEIVER with PROFILE until it
# receives SIGTERM or LIFETIME seconds pass, after a dry run that selects exactly the sender. Sets
# link_degradation_injector_id.
link_degradation_start() {
    local case_dir="$1"
    local sender="$2"
    local receiver="$3"
    local profile="$4"
    local name="$5"
    local lifetime="$6"
    local address
    address="$(node_address "${receiver}")"
    local host
    for host in "${node_hosts[@]}"; do
        inspect_node_rules "${host}" "${case_dir}/rules-before-${host}.txt"
        "${script_dir}/verify-degraded-evidence.sh" rules default \
            "${case_dir}/rules-before-${host}.txt" "${address}" \
            || link_degradation_fail injection "${host} already carries a qdisc, filter or INPUT rule; refusing to overlap faults"
    done
    local sender_id sender_name
    sender_id="$(owned_service_container "${sender}")"
    sender_name="$(run_bounded 20 docker inspect --format '{{.Name}}' "${sender_id}")"
    sender_name="${sender_name#/}"
    link_degradation_effect "${profile}"
    local common=(--interface eth0 --tc-image "${CHAOS_NETTOOLS_IMAGE}" --pull-image=false --target "${address}")
    run_bounded 30 docker run --rm \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=fault \
        --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        "${pumba_image_id}" --dry-run --log-level info \
        --label "io.nervix.chaos.run=${run_id}" --label io.nervix.chaos.role=node \
        netem --duration 1s "${common[@]}" "${link_degradation_effect[@]}" "${sender_name}" \
        >"${case_dir}/pumba-dry-run.txt" 2>&1
    local selections
    selections="$(grep -F 'msg="running netem on container"' "${case_dir}/pumba-dry-run.txt" || true)"
    if [[ "$(grep -c . <<<"${selections}")" -ne 1 ]] \
        || ! grep -Fq "id=${sender_id}" <<<"${selections}" \
        || ! grep -Fq "name=/${sender_name}" <<<"${selections}"; then
        link_degradation_fail injection "Pumba dry run did not select exactly ${sender}"
    fi
    local arguments=(netem --duration "${lifetime}s" "${common[@]}" "${link_degradation_effect[@]}")
    printf '%s\n' "${arguments[@]}" | jq -R . | jq -s . >"${case_dir}/pumba-arguments.json"
    link_degradation_injector_id="$(run_bounded 30 docker run --detach \
        --name "${name}" \
        --label "io.nervix.chaos.run=${run_id}" --label io.nervix.chaos.role=fault \
        --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        "${pumba_image_id}" --log-level info \
        --label "io.nervix.chaos.run=${run_id}" --label io.nervix.chaos.role=node \
        "${arguments[@]}" "${sender_name}")"
    printf '%s\n' "${link_degradation_injector_id}" >"${case_dir}/injector-id.txt"
}

# Waits until the sender carries exactly the started PROFILE toward RECEIVER and every other node
# still carries its default state, and requires the broker and every node's listeners to answer. A
# node's readiness is not part of it, because the degraded link can start an election.
link_degradation_verify() {
    local case_dir="$1"
    local sender="$2"
    local receiver="$3"
    local profile="$4"
    local address
    address="$(node_address "${receiver}")"
    local _
    for _ in $(seq 1 30); do
        inspect_node_rules "${sender}" "${case_dir}/rules-installed.txt"
        if "${script_dir}/verify-degraded-evidence.sh" rules "${profile}" \
            "${case_dir}/rules-installed.txt" "${address}" \
            >"${case_dir}/rules-verdict.txt" 2>&1; then
            local host
            for host in "${node_hosts[@]}"; do
                [[ "${host}" == "${sender}" ]] && continue
                inspect_node_rules "${host}" "${case_dir}/rules-installed-${host}.txt"
                "${script_dir}/verify-degraded-evidence.sh" rules default \
                    "${case_dir}/rules-installed-${host}.txt" "${address}" \
                    || link_degradation_fail injection "${host} received an unintended network rule"
            done
            probe_broker || link_degradation_fail injection 'broker endpoint changed during the fault'
            for host in "${node_hosts[@]}"; do
                probe_node_listeners "${host}" || link_degradation_fail injection "${host} application endpoint changed during the fault"
            done
            return 0
        fi
        container_running "${link_degradation_injector_id}" || break
        sleep 1
    done
    link_degradation_fail injection "${profile} rules did not match the owned netem plan"
}

# Stops the injector INJECTOR_ID with SIGTERM, requires its exit code zero, every node interface back
# in its default state, and the link's ICMP delay back at the BASELINE ping.
link_degradation_heal() {
    local case_dir="$1"
    local sender="$2"
    local receiver="$3"
    local injector_id="$4"
    local baseline="$5"
    run_bounded 60 docker stop -t 40 "${injector_id}" \
        >"${case_dir}/pumba-stop.txt" 2>&1
    local exit_code
    exit_code="$(run_bounded 20 docker inspect --format '{{.State.ExitCode}}' "${injector_id}")"
    run_bounded 20 docker logs "${injector_id}" >"${case_dir}/pumba.log" 2>&1
    run_bounded 20 docker container rm "${injector_id}" >/dev/null
    [[ "${exit_code}" == 0 ]] || link_degradation_fail injection "Pumba exited ${exit_code} after SIGTERM"
    local address
    address="$(node_address "${receiver}")"
    local host
    for host in "${node_hosts[@]}"; do
        inspect_node_rules "${host}" "${case_dir}/rules-healed-${host}.txt"
        "${script_dir}/verify-degraded-evidence.sh" rules default \
            "${case_dir}/rules-healed-${host}.txt" "${address}"
    done
    link_ping "${case_dir}/ping-healed.txt" "${sender}" "${receiver}" healed
    "${script_dir}/verify-degraded-evidence.sh" effect healed \
        "${baseline}" "${case_dir}/ping-healed.json"
}
