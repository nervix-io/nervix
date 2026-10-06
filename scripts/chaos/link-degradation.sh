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

# Pings RECEIVER forty times from SENDER's network namespace and summarizes the result as JSON beside
# OUTPUT.
link_ping() {
    local output="$1"
    local sender="$2"
    local receiver="$3"
    local sender_id address
    sender_id="$(owned_service_container "${sender}")"
    address="$(node_address "${receiver}")"
    local ping_status=0
    run_bounded 30 docker run --rm \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=link-probe \
        --network "container:${sender_id}" "${CHAOS_PROBE_IMAGE}" \
        ping -c 40 -i 0.1 -W 1 "${address}" >"${output}" 2>&1 || ping_status=$?
    [[ "${ping_status}" -eq 0 || "${ping_status}" -eq 1 ]] \
        || link_degradation_fail observation "ping helper failed with ${ping_status}"
    "${script_dir}/verify-degraded-evidence.sh" ping "${output}" "${output%.txt}.json"
}

# Times a 262,144-byte transfer from SENDER to RECEIVER that the receiver counts.
link_rate_probe() {
    local output="$1"
    local sender="$2"
    local receiver="$3"
    local sender_id receiver_id receiver_ip server_id
    sender_id="$(owned_service_container "${sender}")"
    receiver_id="$(owned_service_container "${receiver}")"
    receiver_ip="$(node_address "${receiver}")"
    server_id="$(run_bounded 30 docker run --detach \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=link-probe \
        --network "container:${receiver_id}" --entrypoint sh "${CHAOS_PROBE_IMAGE}" \
        -c 'nc -l -p 18081 | wc -c')"
    local ready=false
    local _
    for _ in $(seq 1 10); do
        if run_bounded 5 docker exec "${server_id}" sh -c \
            'netstat -ltn | grep -q ":18081 "'; then
            ready=true
            break
        fi
        sleep 1
    done
    if [[ "${ready}" != true ]]; then
        run_bounded 20 docker container rm --force "${server_id}" >/dev/null 2>&1 || true
        link_degradation_fail observation 'rate receiver did not start listening within 10 seconds'
    fi
    local started finished status=0
    started="$(epoch_ms)"
    # The positional parameter belongs to the helper container's shell.
    # shellcheck disable=SC2016
    run_bounded 35 docker run --rm \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=link-probe \
        --network "container:${sender_id}" --entrypoint sh "${CHAOS_PROBE_IMAGE}" \
        -c 'dd if=/dev/zero bs=1024 count=256 2>/dev/null | nc -w 1 "$1" 18081' \
        -- "${receiver_ip}" >"${output%.json}.log" 2>&1 || status=$?
    local server_status=0 receiver_bytes=""
    run_bounded 40 docker wait "${server_id}" >"${output%.json}.server-exit.txt" \
        2>&1 || server_status=$?
    finished="$(epoch_ms)"
    receiver_bytes="$(run_bounded 20 docker logs "${server_id}" 2>/dev/null | tr -d '[:space:]')"
    run_bounded 20 docker container rm --force "${server_id}" >/dev/null 2>&1 || true
    [[ "${status}" -eq 0 ]] || link_degradation_fail observation "rate probe failed with ${status}"
    [[ "${server_status}" -eq 0 && "${receiver_bytes}" == 262144 \
        && "$(cat "${output%.json}.server-exit.txt")" == 0 ]] \
        || link_degradation_fail observation "rate receiver got ${receiver_bytes:-no count} bytes instead of 262144"
    jq -n --argjson bytes 262144 --argjson received_bytes "${receiver_bytes}" \
        --argjson duration_ms "$((finished - started))" \
        '{bytes:$bytes,received_bytes:$received_bytes,duration_ms:$duration_ms,
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
    link_ping "${case_dir}/ping-healed.txt" "${sender}" "${receiver}"
    "${script_dir}/verify-degraded-evidence.sh" effect healed \
        "${baseline}" "${case_dir}/ping-healed.json"
}
