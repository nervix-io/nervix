#!/usr/bin/env bash
# Owns Kafka end-offset observations and consecutive stable output reads. The controller supplies
# broker_offset_query, remaining_seconds and the run's artifact directory and overall deadline.

topic_end_offset() {
    : "${artifact_dir:?the controller supplies its artifact directory}"
    local topic="$1"
    local query_deadline="${2:-$((SECONDS + 120))}"
    local requested_seconds=$((query_deadline - SECONDS))
    if ((requested_seconds < 0)); then requested_seconds=0; fi
    local remaining effective_seconds attempt started_at started_ns
    mkdir -p "${artifact_dir}/traffic/offsets" || return $?
    attempt="$(mktemp -d "${artifact_dir}/traffic/offsets/${topic}.XXXXXX")" || return $?
    started_at="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    started_ns="$(date +%s%N)"
    : >"${attempt}/stdout.txt" || return $?
    : >"${attempt}/stderr.txt" || return $?
    jq -n --arg topic "${topic}" --arg started_at "${started_at}" \
        --argjson requested_seconds "${requested_seconds}" \
        '{status:"running",operation:"kafka-end-offset",topic:$topic,started_at:$started_at,
          requested_seconds:$requested_seconds,effective_seconds:null,
          command:["/opt/kafka/bin/kafka-get-offsets.sh","--bootstrap-server","broker:9092","--topic",$topic],
          command_exit_code:null,outcome:null,offset:null}' >"${attempt}/result.json" || return $?

    local command_status=0 outcome offset="" query_status=0
    # Recording preparation also belongs to the observation window. Recompute immediately before
    # launching the command, using the caller's absolute deadline rather than granting fresh time.
    remaining="$(remaining_seconds)"
    effective_seconds=$((query_deadline - SECONDS))
    if ((effective_seconds < 0)); then effective_seconds=0; fi
    if ((effective_seconds > remaining)); then effective_seconds="${remaining}"; fi
    # broker_offset_query reaches compose; its dynamically scoped command bound also respects the
    # remaining overall budget in run_bounded. A stability read never borrows the 120s default.
    local compose_call_timeout="${effective_seconds}"
    if ((effective_seconds == 0)); then
        command_status=124
        if ((remaining == 0)); then
            outcome='overall-deadline'
        else
            outcome='query-deadline'
        fi
        query_status=124
    else
        broker_offset_query /opt/kafka/bin/kafka-get-offsets.sh \
            --bootstrap-server broker:9092 --topic "${topic}" \
            >"${attempt}/stdout.txt" 2>"${attempt}/stderr.txt" || command_status=$?
        if ((command_status != 0)); then
            outcome='command-failed'
            query_status="${command_status}"
        elif offset="$(awk -F: -v topic="${topic}" '
            $1 == topic && $2 == "0" { value = $3; count++ }
            END {
                if (count == 1 && value ~ /^(0|[1-9][0-9]*)$/) print value
                else exit 1
            }' "${attempt}/stdout.txt")"; then
            outcome=observed
        else
            outcome='invalid-offset'
            query_status=1
        fi
    fi
    local ended_ns result_tmp
    ended_ns="$(date +%s%N)"
    result_tmp="${attempt}/result.tmp"
    jq --arg outcome "${outcome}" --arg offset "${offset}" \
        --argjson command_status "${command_status}" --argjson elapsed_ns "$((ended_ns - started_ns))" \
        --argjson effective_seconds "${effective_seconds}" \
        '.status = "complete" | .outcome = $outcome | .command_exit_code = $command_status
         | .effective_seconds = $effective_seconds
         | .elapsed_ms = ($elapsed_ns / 1000000)
         | .offset = (if $offset == "" then null else ($offset | tonumber) end)' \
        "${attempt}/result.json" >"${result_tmp}" || return $?
    mv "${result_tmp}" "${attempt}/result.json" || return $?
    if ((query_status == 0)); then printf '%s\n' "${offset}"; fi
    return "${query_status}"
}

wait_for_stable_output() {
    : "${artifact_dir:?the controller supplies its artifact directory}"
    : "${overall_deadline:?the controller supplies its whole-run deadline}"
    local minimum="$1"
    local started_seconds="${SECONDS}"
    local deadline=$((started_seconds + 30))
    local started_at previous="" stable_polls=0 observations=0 valid_observations=0
    local last_offset="" query_status="" reason="" category=observation status=124
    started_at="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    jq -n --arg started_at "${started_at}" --argjson minimum "${minimum}" \
        '{status:"observing",started_at:$started_at,minimum:$minimum,observation_seconds:30}' \
        >"${artifact_dir}/results/output-boundary.json" || return $?
    : >"${artifact_dir}/traffic/output-boundary.ndjson" || return $?
    while ((SECONDS < deadline && SECONDS < overall_deadline)); do
        local current=""
        query_status=0
        current="$(topic_end_offset chaos_output "${deadline}")" || query_status=$?
        observations=$((observations + 1))
        if ((query_status == 0)) && [[ "${current}" =~ ^[0-9]+$ ]]; then
            valid_observations=$((valid_observations + 1))
            last_offset="${current}"
            if ((current >= minimum)); then
                if [[ "${current}" == "${previous}" ]]; then
                    stable_polls=$((stable_polls + 1))
                else
                    stable_polls=0
                fi
                previous="${current}"
            else
                previous=""
                stable_polls=0
            fi
        else
            current=""
            previous=""
            stable_polls=0
        fi
        jq -nc --arg at "$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)" --arg offset "${current}" \
            --argjson query_status "${query_status}" --argjson stable_polls "${stable_polls}" \
            '{at:$at,query_exit_code:$query_status,stable_polls:$stable_polls,
              offset:(if $offset == "" then null else ($offset | tonumber) end)}' \
            >>"${artifact_dir}/traffic/output-boundary.ndjson" || return $?
        if ((stable_polls >= 2 && SECONDS < deadline && SECONDS < overall_deadline)); then
            status=0
            reason=stable
            break
        fi
        if ((SECONDS < deadline && SECONDS < overall_deadline)); then sleep 1; fi
    done
    local remaining elapsed_seconds
    remaining="$(remaining_seconds)"
    elapsed_seconds=$((SECONDS - started_seconds))
    if [[ -z "${reason}" ]]; then
        if ((remaining == 0)); then
            reason='overall-deadline'
            category=controller
        elif ((valid_observations == 0)); then
            reason='observations-failed'
        elif ((query_status != 0)); then
            reason='observations-failed'
        elif [[ -n "${last_offset}" ]] && ((last_offset < minimum)); then
            reason='below-minimum'
        else
            reason='unstable-output'
        fi
    fi
    local result_tmp="${artifact_dir}/results/output-boundary.tmp"
    jq --arg reason "${reason}" --arg category "${category}" --arg offset "${last_offset}" \
        --arg query_status "${query_status}" --argjson status "${status}" \
        --argjson remaining "${remaining}" --argjson elapsed_seconds "${elapsed_seconds}" \
        --argjson observations "${observations}" --argjson valid_observations "${valid_observations}" \
        --argjson stable_polls "${stable_polls}" \
        '.status = (if $status == 0 then "passed" else "failed" end)
         | .reason = $reason | .category = (if $status == 0 then null else $category end)
         | .exit_code = $status | .elapsed_seconds = $elapsed_seconds
         | .overall_remaining_seconds = $remaining | .observations = $observations
         | .valid_observations = $valid_observations | .stable_polls = $stable_polls
         | .last_offset = (if $offset == "" then null else ($offset | tonumber) end)
         | .last_query_exit_code = (if $query_status == "" then null else ($query_status | tonumber) end)' \
        "${artifact_dir}/results/output-boundary.json" >"${result_tmp}" || return $?
    mv "${result_tmp}" "${artifact_dir}/results/output-boundary.json" || return $?
    if ((status == 0)); then
        printf '%s\n' "${last_offset}"
    else
        printf 'output boundary observation failed: %s (%ss elapsed, %ss overall remaining); see results/output-boundary.json\n' \
            "${reason}" "${elapsed_seconds}" "${remaining}" >&2
    fi
    return "${status}"
}
