#!/usr/bin/env bash
# Sourced by run-baseline.sh after the external cluster and graph are ready.

# shellcheck source=partition-scenario.sh
source "${script_dir}/partition-scenario.sh"

degradation_root="${artifact_dir}/degraded"
degradation_sample_number=0
degradation_injector_id=""
degradation_event_recorder_pid=""

degradation_fail() {
    failure_category="$1"
    shift
    printf 'degraded-links %s failure: %s\n' "${failure_category}" "$*" >&2
    return 1
}

degradation_action() {
    local profile="$1" action="$2"
    jq -nc --arg profile "${profile}" --arg action "${action}" \
        --argjson at_ms "$(epoch_ms)" \
        '{profile:$profile,action:$action,at_ms:$at_ms}' \
        >>"${degradation_root}/actions.ndjson"
}

degradation_sample() {
    local stage="$1"
    local profile="$2"
    kill -0 "${degradation_event_recorder_pid}" 2>/dev/null \
        || degradation_fail observation 'Docker event recorder stopped before the run ended'
    degradation_sample_number=$((degradation_sample_number + 1))
    local sample_dir
    sample_dir="${degradation_root}/samples/$(printf '%04d' "${degradation_sample_number}")"
    mkdir -p "${sample_dir}"
    local host
    for host in "${node_hosts[@]}"; do
        capture_metrics "${host}" || degradation_fail observation "public metrics unavailable on ${host}"
        cp "${artifact_dir}/public/metrics-${host}.txt" "${sample_dir}/${host}.prom"
    done
    local ids=()
    for host in "${node_hosts[@]}"; do
        ids+=("$(owned_service_container "${host}")")
    done
    run_bounded 30 docker stats --no-stream --format '{{json .}}' "${ids[@]}" \
        >"${sample_dir}/docker-stats.ndjson"
    local memory_json
    memory_json="$(jq -sc '
        def bytes:
          (split(" / ")[0] | capture("^(?<n>[0-9.]+)(?<u>B|kB|MB|GB|KiB|MiB|GiB)$")) as $v
          | ($v.n | tonumber) * ({B:1,kB:1000,MB:1000000,GB:1000000000,
                               KiB:1024,MiB:1048576,GiB:1073741824}[$v.u]);
        map({container:.Name,bytes:(.MemUsage | bytes)})
      ' "${sample_dir}/docker-stats.ndjson")" \
        || degradation_fail observation 'Docker stats memory could not be parsed'
    local metrics
    metrics="$(awk '
        /^nervix_interconnect_pending_operations(\{| )/ {pending += $NF}
        /^nervix_interconnect_relay_attempts(\{| )/ {relay_attempts += $NF}
        /^nervix_interconnect_connection_failures_total(\{| )/ {retry_signals += $NF}
        /^nervix_interconnect_stream_resets_total(\{| )/ {retry_signals += $NF}
        /^nervix_interconnect_relay_admissions_total(\{| )/ {attempt_resolutions += $NF}
        /^nervix_delivery_latency_seconds_sum(\{| )/ {latency_sum += $NF}
        /^nervix_delivery_latency_seconds_count(\{| )/ {latency_count += $NF}
        END {printf "%.0f %.0f %.0f %.0f %.9f %.0f", pending, relay_attempts, retry_signals, attempt_resolutions, latency_sum, latency_count}
      ' "${sample_dir}"/*.prom)"
    local pending relay_attempts retry_signals attempt_resolutions latency_sum latency_count
    read -r pending relay_attempts retry_signals attempt_resolutions latency_sum latency_count <<<"${metrics}"
    local tcp_retransmissions sender_id
    sender_id="$(owned_service_container nervix-2)"
    # The awk field references belong to the helper container's shell, not this shell.
    # shellcheck disable=SC2016
    tcp_retransmissions="$(run_bounded 20 docker run --rm \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=link-probe \
        --network "container:${sender_id}" "${CHAOS_PROBE_IMAGE}" \
        awk '$1 == "Tcp:" {
            if (!column) {for (i=1;i<=NF;i++) if ($i == "RetransSegs") column=i; next}
            print $column; exit
        }' /proc/net/snmp)"
    [[ "${tcp_retransmissions}" =~ ^[0-9]+$ ]] \
        || degradation_fail observation 'TCP retransmission counter was unavailable'
    local source_end output_end at_ms
    output_end="$(topic_end_offset chaos_output)"
    source_end="$(topic_end_offset chaos_input)"
    [[ "${source_end}" =~ ^[0-9]+$ && "${output_end}" =~ ^[0-9]+$ ]] \
        || degradation_fail observation 'broker offsets were not numeric'
    at_ms="$(epoch_ms)"
    jq -nc \
        --arg stage "${stage}" --arg profile "${profile}" \
        --arg raw "${sample_dir#"${artifact_dir}/"}" \
        --argjson at_ms "${at_ms}" --argjson source_end "${source_end}" \
        --argjson output_end "${output_end}" --argjson pending "${pending}" \
        --argjson relay_attempts "${relay_attempts}" \
        --argjson retry_signals "${retry_signals}" \
        --argjson tcp_retransmissions "${tcp_retransmissions}" \
        --argjson attempt_resolutions "${attempt_resolutions}" \
        --argjson latency_sum "${latency_sum}" --argjson latency_count "${latency_count}" \
        --argjson memory "${memory_json}" \
        '{stage:$stage,profile:$profile,at_ms:$at_ms,source_end:$source_end,
          output_end:$output_end,backlog:([$source_end-$output_end,0]|max),
          pending_operations:$pending,relay_attempts:$relay_attempts,
          retry_signals_total:$retry_signals,tcp_retransmissions_total:$tcp_retransmissions,
          relay_attempt_resolutions_total:$attempt_resolutions,
          delivery_latency_sum_seconds:$latency_sum,delivery_latency_count:$latency_count,
          docker_memory:$memory,max_docker_memory_bytes:([$memory[].bytes]|max),raw:$raw}' \
        | tee "${sample_dir}/sample.json" >>"${degradation_root}/samples.ndjson"
    "${script_dir}/verify-degraded-evidence.sh" limits \
        "${sample_dir}/sample.json" "${degradation_root}/limits.json" \
        "${degradation_root}/findings.ndjson"
}

degradation_sample_window() {
    local stage="$1" profile="$2" duration="$3"
    local until=$((SECONDS + duration))
    while ((SECONDS < until)); do
        degradation_sample "${stage}" "${profile}"
        if ((SECONDS < until)); then
            sleep 3
        fi
    done
    degradation_sample "${stage}" "${profile}"
}

degradation_ping() {
    local output="$1"
    local sender_id address
    sender_id="$(owned_service_container nervix-2)"
    address="$(node_address nervix-3)"
    local ping_status=0
    run_bounded 30 docker run --rm \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=link-probe \
        --network "container:${sender_id}" "${CHAOS_PROBE_IMAGE}" \
        ping -c 40 -i 0.1 -W 1 "${address}" >"${output}" 2>&1 || ping_status=$?
    [[ "${ping_status}" -eq 0 || "${ping_status}" -eq 1 ]] \
        || degradation_fail observation "ping helper failed with ${ping_status}"
    "${script_dir}/verify-degraded-evidence.sh" ping "${output}" "${output%.txt}.json"
}

degradation_rate_probe() {
    local output="$1"
    local sender_id receiver_id receiver_ip server_id
    sender_id="$(owned_service_container nervix-2)"
    receiver_id="$(owned_service_container nervix-3)"
    receiver_ip="$(node_address nervix-3)"
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
        degradation_fail observation 'rate receiver did not start listening within 10 seconds'
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
    [[ "${status}" -eq 0 ]] || degradation_fail observation "rate probe failed with ${status}"
    [[ "${server_status}" -eq 0 && "${receiver_bytes}" == 262144 \
        && "$(cat "${output%.json}.server-exit.txt")" == 0 ]] \
        || degradation_fail observation "rate receiver got ${receiver_bytes:-no count} bytes instead of 262144"
    jq -n --argjson bytes 262144 --argjson received_bytes "${receiver_bytes}" \
        --argjson duration_ms "$((finished - started))" \
        '{bytes:$bytes,received_bytes:$received_bytes,duration_ms:$duration_ms,
          bytes_per_second:($received_bytes * 1000 / $duration_ms)}' \
        >"${output}"
}

degradation_fault_args() {
    local profile="$1"
    degradation_args=(netem --duration 300s --interface eth0 --tc-image "${CHAOS_NETTOOLS_IMAGE}"
        --pull-image=false --target "$(node_address nervix-3)")
    case "${profile}" in
        delay) degradation_args+=(delay --time 180 --jitter 0 --correlation 0) ;;
        jitter) degradation_args+=(delay --time 180 --jitter 100 --correlation 0 --distribution normal) ;;
        random-loss) degradation_args+=(loss --percent 30 --correlation 0) ;;
        burst-loss) degradation_args+=(loss-state --p13 20 --p31 15 --p32 0 --p23 100 --p14 0) ;;
        rate-limit) degradation_args+=(rate --rate 256kbit) ;;
        combined) degradation_args+=(combine --delay --delay-time 180 --delay-jitter 60
            --loss --loss-percent 20 --rate --rate-value 256kbit --) ;;
    esac
}

degradation_install() {
    local case_dir="$1" profile="$2" ordinal="$3"
    local sender_id sender_name
    sender_id="$(owned_service_container nervix-2)"
    sender_name="$(run_bounded 20 docker inspect --format '{{.Name}}' "${sender_id}")"
    sender_name="${sender_name#/}"
    local host
    for host in "${node_hosts[@]}"; do
        inspect_node_rules "${host}" "${case_dir}/rules-before-${host}.txt"
        "${script_dir}/verify-degraded-evidence.sh" rules default \
            "${case_dir}/rules-before-${host}.txt" "$(node_address nervix-3)"
    done
    degradation_fault_args "${profile}"
    local dry_args=("${degradation_args[@]}")
    dry_args[2]=1s
    run_bounded 30 docker run --rm \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=fault \
        --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        "${pumba_image_id}" --dry-run --log-level info \
        --label "io.nervix.chaos.run=${run_id}" --label io.nervix.chaos.role=node \
        "${dry_args[@]}" "${sender_name}" >"${case_dir}/pumba-dry-run.txt" 2>&1
    local selections
    selections="$(grep -F 'msg="running netem on container"' "${case_dir}/pumba-dry-run.txt" || true)"
    if [[ "$(grep -c . <<<"${selections}")" -ne 1 ]] \
        || ! grep -Fq "id=${sender_id}" <<<"${selections}" \
        || ! grep -Fq "name=/${sender_name}" <<<"${selections}"; then
        degradation_fail injection 'Pumba dry run did not select exactly nervix-2'
    fi
    printf '%s\n' "${degradation_args[@]}" | jq -R . | jq -s . >"${case_dir}/pumba-arguments.json"
    degradation_injector_id="$(run_bounded 30 docker run --detach \
        --name "${project_name}-degraded-${ordinal}-${profile}" \
        --label "io.nervix.chaos.run=${run_id}" --label io.nervix.chaos.role=fault \
        --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        "${pumba_image_id}" --log-level info \
        --label "io.nervix.chaos.run=${run_id}" --label io.nervix.chaos.role=node \
        "${degradation_args[@]}" "${sender_name}")"
    degradation_action "${profile}" injector_started
    printf '%s\n' "${degradation_injector_id}" >"${case_dir}/injector-id.txt"
    local _
    for _ in $(seq 1 30); do
        inspect_node_rules nervix-2 "${case_dir}/rules-installed.txt"
        if "${script_dir}/verify-degraded-evidence.sh" rules "${profile}" \
            "${case_dir}/rules-installed.txt" "$(node_address nervix-3)" \
            >"${case_dir}/rules-verdict.txt" 2>&1; then
            for host in nervix-1 nervix-3; do
                inspect_node_rules "${host}" "${case_dir}/rules-installed-${host}.txt"
                "${script_dir}/verify-degraded-evidence.sh" rules default \
                    "${case_dir}/rules-installed-${host}.txt" "$(node_address nervix-3)" \
                    || degradation_fail injection "${host} received an unintended network rule"
            done
            degradation_action "${profile}" fault_verified
            probe_broker || degradation_fail injection 'broker endpoint changed during the fault'
            for host in "${node_hosts[@]}"; do
                probe_node "${host}" || degradation_fail injection "${host} application endpoint changed during the fault"
            done
            return 0
        fi
        container_running "${degradation_injector_id}" || break
        sleep 1
    done
    degradation_fail injection "${profile} rules did not match the owned netem plan"
}

degradation_heal() {
    local case_dir="$1"
    local profile="${case_dir##*/}"
    profile="${profile#*-}"
    degradation_action "${profile}" heal_requested
    run_bounded 60 docker stop -t 40 "${degradation_injector_id}" \
        >"${case_dir}/pumba-stop.txt" 2>&1
    local exit_code
    exit_code="$(run_bounded 20 docker inspect --format '{{.State.ExitCode}}' "${degradation_injector_id}")"
    run_bounded 20 docker logs "${degradation_injector_id}" >"${case_dir}/pumba.log" 2>&1
    run_bounded 20 docker container rm "${degradation_injector_id}" >/dev/null
    degradation_injector_id=""
    [[ "${exit_code}" == 0 ]] || degradation_fail injection "Pumba exited ${exit_code} after SIGTERM"
    local host
    for host in "${node_hosts[@]}"; do
        inspect_node_rules "${host}" "${case_dir}/rules-healed-${host}.txt"
        "${script_dir}/verify-degraded-evidence.sh" rules default \
            "${case_dir}/rules-healed-${host}.txt" "$(node_address nervix-3)"
    done
    degradation_ping "${case_dir}/ping-healed.txt"
    "${script_dir}/verify-degraded-evidence.sh" effect healed \
        "${case_dir}/ping-baseline.json" "${case_dir}/ping-healed.json"
    degradation_action "${profile}" link_healed
}

degradation_recover() {
    local case_dir="$1" profile="$2"
    local deadline=$((SECONDS + drain_seconds))
    local window
    while ((SECONDS < deadline)); do
        check_support_containers \
            || degradation_fail setup 'load or observer stopped during post-heal recovery'
        degradation_sample recovery "${profile}"
        window="$("${script_dir}/verify-degraded-evidence.sh" recovery \
            "${degradation_root}/samples.ndjson" "${profile}" \
            "${degradation_baseline_rps}" "${min_throughput_pct}" \
            "${max_recovery_backlog}")"
        if [[ "$(jq -r '.qualified' <<<"${window}")" == true ]]; then
            local current at backlog remaining
            current="$(tail -n 1 "${degradation_root}/samples.ndjson")"
            at="$(jq -r '.at_ms' <<<"${current}")"
            backlog="$(jq -r '.backlog' <<<"${current}")"
            mkdir -p "${case_dir}/cluster-healed"
            remaining=$((deadline - SECONDS))
            ((remaining > 0)) || break
            wait_for "${profile} cluster health and connectivity" "${remaining}" \
                cluster_settled_and_connected "${case_dir}/cluster-healed" \
                || degradation_fail product "${profile} cluster did not reconnect before its recovery deadline"
            degradation_action "${profile}" recovered
            jq -n --argjson recovered_at_ms "${at}" --argjson backlog "${backlog}" \
                --argjson rate "$(jq -r '.output_records_per_second' <<<"${window}")" \
                '{recovered_at_ms:$recovered_at_ms,consecutive_progress_intervals:3,
                  output_records_per_second:$rate,backlog:$backlog}' \
                >"${case_dir}/recovery.json"
            return 0
        fi
        sleep 3
    done
    degradation_action "${profile}" recovery_deadline_exceeded
    local current
    current="$(tail -n 1 "${degradation_root}/samples.ndjson")"
    jq -n --arg profile "${profile}" \
        --argjson deadline_seconds "${drain_seconds}" \
        --argjson baseline_rps "${degradation_baseline_rps}" \
        --argjson min_throughput_pct "${min_throughput_pct}" \
        --argjson max_recovery_backlog "${max_recovery_backlog}" \
        --argjson window "${window}" --argjson sample "${current}" \
        '{profile:$profile,deadline_seconds:$deadline_seconds,
          baseline_output_records_per_second:$baseline_rps,
          minimum_output_records_per_second:($baseline_rps*$min_throughput_pct/100),
          max_recovery_backlog:$max_recovery_backlog,
          observed_window:$window,last_sample:$sample,
          sample_timeline:"degraded/samples.ndjson",
          action_timeline:"degraded/actions.ndjson"}' \
        >"${case_dir}/recovery-deadline.json"
    jq -nc --arg profile "${profile}" \
        --argjson sample "${current}" \
        --argjson window "${window}" \
        --argjson minimum_rate "$(jq '.minimum_output_records_per_second' "${case_dir}/recovery-deadline.json")" \
        --argjson max_backlog "${max_recovery_backlog}" \
        '{metric:"post_heal_recovery_window",profile:$profile,stage:"recovery",
          at_ms:$sample.at_ms,
          observed:{sample_count:$window.sample_count,
                    positive_progress_intervals:$window.positive_progress_intervals,
                    output_records_per_second:$window.output_records_per_second,
                    backlog:$window.backlog},
          limit:{sample_count:4,positive_progress_intervals:3,
                 minimum_output_records_per_second:$minimum_rate,
                 maximum_backlog:$max_backlog},
          backlog:$sample.backlog,raw:$sample.raw,
          action_timeline:"degraded/actions.ndjson"}' \
        >>"${degradation_root}/findings.ndjson"
    degradation_fail product "${profile} did not sustain post-heal progress within ${drain_seconds}s"
}

run_degraded_links() {
    mkdir -p "${degradation_root}/samples"
    : >"${degradation_root}/samples.ndjson"
    : >"${degradation_root}/findings.ndjson"
    : >"${degradation_root}/actions.ndjson"
    local cases=()
    if [[ "${degradation_profile}" == all ]]; then
        cases=(delay jitter random-loss burst-loss rate-limit combined)
    else
        cases=("${degradation_profile}")
    fi
    local minimum_records=$(((baseline_seconds + ${#cases[@]} * (degrade_seconds + 25) + 45) * 1000 / load_interval_ms))
    ((record_count >= minimum_records)) \
        || degradation_fail setup "${record_count} records cannot sustain the declared fixed rate across ${#cases[@]} profile(s); use at least ${minimum_records}"
    run_bounded 20 docker info --format '{{json .}}' \
        | jq '{name:.Name,server_version:.ServerVersion,cpus:.NCPU,memory_bytes:.MemTotal,
               operating_system:.OperatingSystem,kernel:.KernelVersion,driver:.Driver,architecture:.Architecture}' \
        >"${degradation_root}/worker.json"
    docker events --filter "label=io.nervix.chaos.run=${run_id}" \
        --format '{{json .}}' >"${degradation_root}/docker-events.ndjson" 2>&1 &
    degradation_event_recorder_pid=$!
    jq -n --argjson max_backlog "${max_backlog}" \
        --argjson max_recovery_backlog "${max_recovery_backlog}" \
        --argjson max_memory_bytes "${max_memory_bytes}" \
        --argjson max_pending "${max_pending}" \
        '{max_backlog:$max_backlog,max_recovery_backlog:$max_recovery_backlog,max_memory_bytes:$max_memory_bytes,max_pending:$max_pending}' \
        >"${degradation_root}/limits.json"
    phase 'degraded-links fixed-rate traffic and independent healthy baseline'
    jq -nc --arg run_id "${run_id}" --argjson count "${record_count}" \
        -f "${fixture_generator}" >"${artifact_dir}/fixtures/input.ndjson"
    compose up --detach --no-deps load observer
    wait_for 'independent load, observer and broker' 30 check_support_containers
    wait_for 'source traffic started' 30 topic_progressed chaos_input 0
    wait_for 'sink traffic started' 60 topic_progressed chaos_output 0
    wait_for 'initial cross-node traffic metrics' 60 initial_remote_path_ready
    local remote_path_tmp
    remote_path_tmp="$(mktemp "${artifact_dir}/results/.remote-path.XXXXXX")"
    jq '.traffic_observed_before_degradation = true' \
        "${artifact_dir}/results/remote-path.json" >"${remote_path_tmp}"
    mv "${remote_path_tmp}" "${artifact_dir}/results/remote-path.json"
    degradation_ping "${degradation_root}/ping-baseline.txt"
    degradation_rate_probe "${degradation_root}/rate-baseline.json"
    degradation_action healthy baseline_started
    degradation_sample_window baseline healthy "${baseline_seconds}"
    degradation_baseline_rps="$(jq -s -r '
        (.[-1].output_end - .[0].output_end) * 1000 / (.[-1].at_ms - .[0].at_ms)
      ' "${degradation_root}/samples.ndjson")"
    jq -n --argjson rps "${degradation_baseline_rps}" \
        --slurpfile ping "${degradation_root}/ping-baseline.json" \
        --slurpfile rate "${degradation_root}/rate-baseline.json" \
        '{output_records_per_second:$rps,ping:$ping[0],rate_probe:$rate[0]}' \
        >"${degradation_root}/baseline.json"
    degradation_action healthy baseline_completed
    jq -e '.output_records_per_second >= 0.5 and .ping.received == 40' \
        "${degradation_root}/baseline.json" >/dev/null \
        || degradation_fail setup 'healthy baseline did not sustain 0.5 output record/s or a clean link'
    local ordinal=0 profile
    for profile in "${cases[@]}"; do
        ordinal=$((ordinal + 1))
        local case_dir
        case_dir="${degradation_root}/$(printf '%02d' "${ordinal}")-${profile}"
        mkdir -p "${case_dir}"
        cp "${degradation_root}/ping-baseline.json" "${case_dir}/ping-baseline.json"
        cp "${degradation_root}/rate-baseline.json" "${case_dir}/rate-baseline.json"
        phase "degraded-links ${ordinal}/${#cases[@]}: ${profile}"
        check_support_containers || degradation_fail setup 'load ended before all profiles completed; increase --records'
        degradation_install "${case_dir}" "${profile}" "${ordinal}"
        degradation_ping "${case_dir}/ping-fault.txt"
        if [[ "${profile}" == rate-limit || "${profile}" == combined ]]; then
            degradation_rate_probe "${case_dir}/rate-fault.json"
        fi
        "${script_dir}/verify-degraded-evidence.sh" effect "${profile}" \
            "${case_dir}/ping-baseline.json" "${case_dir}/ping-fault.json" \
            "${case_dir}/rate-baseline.json" "${case_dir}/rate-fault.json" \
            >"${case_dir}/effect-verdict.txt"
        degradation_sample_window fault "${profile}" "${degrade_seconds}"
        phase "degraded-links ${ordinal}/${#cases[@]}: healing ${profile}"
        degradation_heal "${case_dir}"
        degradation_recover "${case_dir}" "${profile}"
        jq -n --arg profile "${profile}" \
            --slurpfile effect "${case_dir}/ping-fault.json" \
            --slurpfile recovery "${case_dir}/recovery.json" \
            '{profile:$profile,ping:$effect[0],recovery:$recovery[0]}' \
            >"${case_dir}/result.json"
    done
    phase 'degraded-links final traffic boundary'
    touch "${artifact_dir}/traffic/stop-load"
    local load_id
    load_id="$(owned_service_container load)"
    wait_for 'load producer flushed and exited' 40 load_exited_cleanly "${load_id}"
    producer_status="$(run_bounded 20 docker inspect --format '{{.State.ExitCode}}' "${load_id}")"
    run_bounded 20 docker logs "${load_id}" >"${artifact_dir}/traffic/producer.log" 2>&1
    input_end="$(topic_end_offset chaos_input)"
    [[ "${input_end}" =~ ^[0-9]+$ && "${input_end}" -gt 0 && "${input_end}" -le "${record_count}" ]] \
        || degradation_fail product 'source boundary is outside the fixture'
    kcat -q -b broker:9092 -C -t chaos_input -p 0 -o beginning -c "${input_end}" \
        >"${artifact_dir}/traffic/accepted-input.ndjson" \
        2>"${artifact_dir}/traffic/source-consumer.stderr"
    [[ "$(wc -l <"${artifact_dir}/traffic/accepted-input.ndjson")" -eq "${input_end}" ]] \
        || degradation_fail product 'accepted ledger does not cover the source boundary'
    degradation_sample final healthy
    jq -s --slurpfile baseline "${degradation_root}/baseline.json" \
        --slurpfile limits "${degradation_root}/limits.json" \
        --slurpfile phases "${artifact_dir}/phases.ndjson" \
        --slurpfile actions "${degradation_root}/actions.ndjson" '
        . as $samples
        | {baseline:$baseline[0],limits:$limits[0],phases:$phases,actions:$actions,
           samples:[range(0; $samples|length) as $i
             | $samples[$i] as $current
             | $current +
                 (if $i == 0 then {output_records_per_second:null,delivery_latency_avg_ms:null}
                  else ($samples[$i-1]) as $previous
                    | {output_records_per_second:
                        (if $current.at_ms > $previous.at_ms then
                            ($current.output_end-$previous.output_end)*1000/($current.at_ms-$previous.at_ms) else null end),
                       delivery_latency_avg_ms:
                        (if $current.delivery_latency_count > $previous.delivery_latency_count then
                            ($current.delivery_latency_sum_seconds-$previous.delivery_latency_sum_seconds)*1000/
                            ($current.delivery_latency_count-$previous.delivery_latency_count) else null end)}
                  end)],
           retry_signal_delta:($samples[-1].retry_signals_total-$samples[0].retry_signals_total),
           tcp_retransmission_delta:($samples[-1].tcp_retransmissions_total-$samples[0].tcp_retransmissions_total),
           relay_attempt_resolution_delta:($samples[-1].relay_attempt_resolutions_total-$samples[0].relay_attempt_resolutions_total)}' \
        "${degradation_root}/samples.ndjson" >"${artifact_dir}/results/degraded-progress.json"
    kill "${degradation_event_recorder_pid}" 2>/dev/null || true
    wait "${degradation_event_recorder_pid}" 2>/dev/null || true
    degradation_event_recorder_pid=""
    [[ -s "${degradation_root}/docker-events.ndjson" ]] \
        || degradation_fail observation 'Docker event recorder retained no events'
}
