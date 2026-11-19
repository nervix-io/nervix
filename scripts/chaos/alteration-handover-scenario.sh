#!/usr/bin/env bash
# Sourced by run-baseline.sh after the external cluster and the baseline graph are ready. Owns the
# alteration-handover scenario: an alteration that replaces a deduplicator holding thousands of
# branches while records from an acknowledged and an unacknowledged Kafka source are in motion,
# judged by a strict verdict. The baseline path keeps its own traffic and exact ledger in its domain.
#
# The domain chaos_alter installs fixtures/alteration-handover.plexus. Each source receives two
# burst loads on its own topic. The backlog, produced before START and read at once, gives every
# record a branch of its own, so the deduplicator holds one branch per backlog record. Once the
# backlog is delivered, a second burst on two interleaved branches is produced, the source without
# acknowledgements last, and ALTER DEDUPLICATOR changes the deduplication key at once, while that
# burst is still passing through the domain. The alteration replaces the deduplicator through an
# entity swap, in which the replaced deduplicator hands its branches over to its replacement one at
# a time.
#
# The scenario lowers DOMAIN_DRAIN_TIMEOUT first. The task stop grace is one fifteenth of it. An
# image whose swap gives the whole handover only that grace aborts a handover of this many
# branches, fails the swap and rebuilds the domain on that node without a drain: the records in
# motion are cut: the acknowledged source's record in flight ends with a negative outcome, and a
# record the source without acknowledgements read during the swap is lost. A handover that waits
# for every branch cuts nothing.
#
# The verdict is strict (verify-alteration-handover.sh over verify-strict-delivery.sh): every record
# delivered exactly once, no duplicate of any kind, no missing record, no increase of a
# negative-outcome counter, no error event, an alteration answer that acknowledges the change, and
# no node log of a domain rebuild standing in for the swap. A failing verdict names its symptoms in
# results/symptoms.json, which an expected failure (--expect-failure) matches.

# shellcheck source=recovery-scenario.sh
source "${script_dir}/recovery-scenario.sh"
# shellcheck source=burst-load.sh
source "${script_dir}/burst-load.sh"
# shellcheck source=events-observer.sh
source "${script_dir}/events-observer.sh"

alteration_handover_domain=chaos_alter
alteration_handover_deduplicator=chaos_alter_dedup
alteration_handover_output_topic=chaos_alter_output
# The source without acknowledgements comes last, so its burst in motion is the freshest when the
# alteration arrives.
alteration_handover_sources=(acked unacked)
alteration_handover_ingestors=(chaos_alter_acked chaos_alter_unacked)
alteration_handover_emitters=(chaos_alter_output)
# The backlog records of each source, each opening a branch of its own. A swap that gives the whole
# handover one task stop grace cannot hand this many branches over within the grace below.
alteration_handover_backlog_records=3000
# The records of each source's burst in motion, on two interleaved branches.
alteration_handover_motion_records=4000
# The domain drain timeout the scenario sets. The task stop grace is one fifteenth of it: 100 ms.
alteration_handover_drain_timeout=1500ms
# External bounds, in seconds.
alteration_handover_backlog_bound=300
alteration_handover_answer_bound=300
alteration_handover_settle_bound=300

alteration_handover_fail() {
    recovery_fail "$@"
}

alteration_handover_cli() {
    run_cli "${alteration_handover_domain}" "$1"
}

alteration_handover_topic() {
    printf 'chaos_alter_%s_input\n' "$1"
}

# The records both sources hold once their bursts are produced.
alteration_handover_total_records() {
    printf '%s\n' "$((2 * (alteration_handover_backlog_records + alteration_handover_motion_records)))"
}

# Writes the burst fixtures of SOURCE: its backlog, one branch per record, and its burst in motion
# on two interleaved branches, whose sequences follow the backlog's.
alteration_handover_fixtures() {
    local source="$1"
    local backlog="${artifact_dir}/fixtures/alteration-handover-${source}-backlog.ndjson"
    local motion="${artifact_dir}/fixtures/alteration-handover-${source}-motion.ndjson"
    jq -nc --arg run_id "${run_id}" --arg source "${source}" --arg branches distinct \
        --argjson first 0 --argjson count "${alteration_handover_backlog_records}" \
        -f "${script_dir}/fixtures/generate-alteration-handover.jq" >"${backlog}"
    [[ "$(wc -l <"${backlog}")" -eq "${alteration_handover_backlog_records}" ]] \
        || alteration_handover_fail controller "the ${source} backlog fixture generation was incomplete"
    jq -nc --arg run_id "${run_id}" --arg source "${source}" --arg branches interleaved \
        --argjson first "${alteration_handover_backlog_records}" \
        --argjson count "${alteration_handover_motion_records}" \
        -f "${script_dir}/fixtures/generate-alteration-handover.jq" >"${motion}"
    [[ "$(wc -l <"${motion}")" -eq "${alteration_handover_motion_records}" ]] \
        || alteration_handover_fail controller "the ${source} burst in motion fixture generation was incomplete"
}

# True once the output holds as many records as both backlogs and both sources committed their
# backlog through its end. Writes CASE_DIR/backlog-delivered.json.
alteration_handover_backlog_delivered() {
    local case_dir="$1"
    local output_end acked_committed unacked_committed
    output_end="$(topic_end_offset "${alteration_handover_output_topic}")" || return 1
    acked_committed="$(burst_load_committed chaos_alter_acked chaos_alter_acked_input)" || return 1
    unacked_committed="$(burst_load_committed chaos_alter_unacked chaos_alter_unacked_input)" || return 1
    jq -n --argjson at_ms "$(epoch_ms)" --argjson output_end "${output_end}" \
        --argjson acked_committed "${acked_committed}" --argjson unacked_committed "${unacked_committed}" \
        --argjson backlog "${alteration_handover_backlog_records}" \
        '{at_ms: $at_ms, output_end: $output_end, acked_committed: $acked_committed,
          unacked_committed: $unacked_committed, backlog: $backlog,
          delivered: ($output_end == 2 * $backlog and $acked_committed == $backlog
                      and $unacked_committed == $backlog)}' >"${case_dir}/backlog-delivered.json"
    jq -e '.delivered' "${case_dir}/backlog-delivered.json" >/dev/null
}

# Requests the alteration in the background. Its answer, exit status and the moment it answered go
# to CASE_DIR.
alteration_handover_request() {
    local case_dir="$1"
    alteration_handover_requested_ms="$(epoch_ms)"
    (
        local compose_call_timeout="${alteration_handover_answer_bound}"
        local status=0
        alteration_handover_cli "ALTER DEDUPLICATOR ${alteration_handover_deduplicator} SET DEDUPLICATE ON input.content;" \
            >"${case_dir}/alter-answer.txt" 2>&1 || status=$?
        epoch_ms >"${case_dir}/alter.answered-ms"
        printf '%s\n' "${status}" >"${case_dir}/alter.exit"
    ) &
    alteration_handover_alter_pid=$!
}

# True once both sources committed through their topic's end and the output has not moved for three
# observations in a row, five seconds apart. Appends every observation to CASE_DIR/settle.ndjson.
alteration_handover_settled_ends=()
alteration_handover_settled() {
    local case_dir="$1"
    local acked_end unacked_end acked_committed unacked_committed output_end
    acked_end="$(topic_end_offset chaos_alter_acked_input)" || return 1
    unacked_end="$(topic_end_offset chaos_alter_unacked_input)" || return 1
    acked_committed="$(burst_load_committed chaos_alter_acked chaos_alter_acked_input)" || return 1
    unacked_committed="$(burst_load_committed chaos_alter_unacked chaos_alter_unacked_input)" || return 1
    output_end="$(topic_end_offset "${alteration_handover_output_topic}")" || return 1
    alteration_handover_settled_ends+=("${output_end}")
    jq -nc --argjson at_ms "$(epoch_ms)" --argjson acked_end "${acked_end}" --argjson unacked_end "${unacked_end}" \
        --argjson acked_committed "${acked_committed}" --argjson unacked_committed "${unacked_committed}" \
        --argjson output_end "${output_end}" \
        '{at_ms: $at_ms, acked_end: $acked_end, acked_committed: $acked_committed,
          unacked_end: $unacked_end, unacked_committed: $unacked_committed, output_end: $output_end}' \
        >>"${case_dir}/settle.ndjson"
    local count="${#alteration_handover_settled_ends[@]}"
    if ((acked_committed != acked_end || unacked_committed != unacked_end || count < 3)); then
        sleep 4
        return 1
    fi
    if [[ "${alteration_handover_settled_ends[$((count - 1))]}" != "${alteration_handover_settled_ends[$((count - 2))]}" \
        || "${alteration_handover_settled_ends[$((count - 2))]}" != "${alteration_handover_settled_ends[$((count - 3))]}" ]]; then
        sleep 4
        return 1
    fi
}

# Keeps what every node logged since the scenario began, for the verdict's rebuild symptom.
alteration_handover_node_logs() {
    local case_dir="$1"
    local host container_id
    for host in "${node_hosts[@]}"; do
        container_id="$(owned_service_container "${host}")" || return 1
        run_bounded 30 docker logs --since "${alteration_handover_since}" "${container_id}" \
            >"${case_dir}/node-${host}.log" 2>&1 || return 1
    done
}

# Judges the scenario and records every symptom as a recovery finding.
alteration_handover_verify() {
    local case_dir="$1"
    phase 'alteration handover: verdict'
    local source
    for source in "${alteration_handover_sources[@]}"; do
        burst_load_accepted "alteration-handover-${source}" "$(alteration_handover_topic "${source}")" \
            "${artifact_dir}/traffic/alteration-handover-${source}-accepted.ndjson" \
            || alteration_handover_fail controller "the accepted-input ledger of the ${source} source could not be read"
    done
    local output_end
    output_end="$(topic_end_offset "${alteration_handover_output_topic}")" \
        || alteration_handover_fail controller 'the alteration-handover output boundary could not be read'
    if ((output_end > 0)); then
        kcat -q -b broker:9092 -C -t "${alteration_handover_output_topic}" -p 0 -o beginning -c "${output_end}" \
            >"${artifact_dir}/traffic/${alteration_handover_output_topic}.ndjson" \
            2>"${artifact_dir}/traffic/${alteration_handover_output_topic}.stderr" \
            || alteration_handover_fail controller 'the alteration-handover output could not be read'
    else
        : >"${artifact_dir}/traffic/${alteration_handover_output_topic}.ndjson"
    fi
    [[ "$(wc -l <"${artifact_dir}/traffic/${alteration_handover_output_topic}.ndjson")" -eq "${output_end}" ]] \
        || alteration_handover_fail controller "the alteration-handover output ledger does not hold its ${output_end} records"
    local transcripts=() node_logs=()
    local host
    for host in "${node_hosts[@]}"; do
        transcripts+=(--events "${case_dir}/events-${host}.log")
        node_logs+=(--node-log "${case_dir}/node-${host}.log")
    done
    local status=0
    "${script_dir}/verify-strict-delivery.sh" \
        --output "${artifact_dir}/traffic/${alteration_handover_output_topic}.ndjson" \
        --source "acked=${artifact_dir}/traffic/alteration-handover-acked-accepted.ndjson" \
        --source "unacked=${artifact_dir}/traffic/alteration-handover-unacked-accepted.ndjson" \
        --negative-outcomes "${case_dir}/negative-outcomes-before.json" "${case_dir}/negative-outcomes-after.json" \
        --events-window "${case_dir}/events-window.json" "${transcripts[@]}" \
        --result "${case_dir}/delivery.json" >"${case_dir}/delivery.txt" 2>&1 || status=$?
    ((status <= 1)) || alteration_handover_fail observation "the strict delivery evidence cannot be judged; see alteration-handover/delivery.txt"
    status=0
    "${script_dir}/verify-alteration-handover.sh" --delivery "${case_dir}/delivery.json" \
        --alter-answer "${case_dir}/alter-answer.txt" --alter-exit "$(<"${case_dir}/alter.exit")" \
        --deduplicator "${alteration_handover_deduplicator}" "${node_logs[@]}" \
        --alter-timing "${case_dir}/alter-timing.json" \
        --result "${case_dir}/verdict.json" >"${case_dir}/verdict.txt" 2>&1 || status=$?
    ((status <= 1)) || alteration_handover_fail controller "the alteration-handover verdict cannot be judged; see alteration-handover/verdict.txt"
    jq --arg scenario "${scenario}" '{scenario: $scenario, verdict_file: "alteration-handover/verdict.json",
            verdict: .verdict, symptoms: .symptoms}' "${case_dir}/verdict.json" \
        >"${artifact_dir}/results/symptoms.json"
    local finding
    while IFS= read -r finding; do
        recovery_finding "${case_dir}" "${finding}"
    done < <(jq -r '.symptoms[] | "\(.name): " + ({sources, count, logs, exit_code} | with_entries(select(.value != null)) | tojson)' \
        "${case_dir}/verdict.json")
    if ((status == 0)) && [[ "${alteration_handover_settled_within_bound}" != true ]]; then
        alteration_handover_fail observation "the sources did not settle within ${alteration_handover_settle_bound}s after the alteration; see alteration-handover/settle.ndjson"
    fi
}

run_alteration_handover() {
    local case_dir="${artifact_dir}/alteration-handover"
    mkdir -p "${case_dir}"
    recovery_traffic_startup "${case_dir}"
    alteration_handover_settled_within_bound=false
    alteration_handover_since="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

    failure_category=controller
    phase 'alteration handover: error-event observers'
    events_observer_prepare "${case_dir}" \
        || alteration_handover_fail controller 'the error-event observers'"'"' quiet domain could not be created; see alteration-handover/observer-*.txt'
    events_observer_start "${case_dir}" \
        || alteration_handover_fail controller 'the error-event observers did not attach'

    failure_category=product
    phase 'alteration handover: task stop grace'
    cli_command "ALTER CLUSTER SET DOMAIN_DRAIN_TIMEOUT = ${alteration_handover_drain_timeout};" \
        >"${case_dir}/drain-timeout.txt" 2>&1 \
        || alteration_handover_fail product "DOMAIN_DRAIN_TIMEOUT could not be set to ${alteration_handover_drain_timeout}; see alteration-handover/drain-timeout.txt"

    phase 'alteration handover: graph installation'
    cli_command "CREATE UNPACED DOMAIN ${alteration_handover_domain};" >"${case_dir}/create-domain.txt" 2>&1 \
        || alteration_handover_fail product "the domain ${alteration_handover_domain} could not be created"
    alteration_handover_cli "$(<"${script_dir}/fixtures/alteration-handover.plexus")" >"${case_dir}/configure-plexus.txt" 2>&1 \
        || alteration_handover_fail product 'the alteration-handover graph could not be installed; see alteration-handover/configure-plexus.txt'
    cp "${script_dir}/fixtures/alteration-handover.plexus" "${artifact_dir}/fixtures/alteration-handover.plexus"

    phase 'alteration handover: counters before START'
    failure_category=controller
    capture_all_metrics "${case_dir}/metrics-before" \
        || alteration_handover_fail controller 'the metrics before START could not be read'
    local outcome_args=(--domain "${alteration_handover_domain}")
    local name
    for name in "${alteration_handover_ingestors[@]}"; do outcome_args+=(--ingestor "${name}"); done
    for name in "${alteration_handover_emitters[@]}"; do outcome_args+=(--emitter "${name}"); done
    "${script_dir}/negative-outcomes.sh" snapshot "${outcome_args[@]}" \
        --output "${case_dir}/negative-outcomes-before.json" "${case_dir}/metrics-before"/metrics-*.txt \
        || alteration_handover_fail controller 'the negative-outcome counters before START could not be read'

    phase 'alteration handover: burst backlog of distinct branches'
    local source
    for source in "${alteration_handover_sources[@]}"; do
        alteration_handover_fixtures "${source}"
        burst_load_produce "alteration-handover-${source}" "$(alteration_handover_topic "${source}")" \
            "${artifact_dir}/fixtures/alteration-handover-${source}-backlog.ndjson" \
            || alteration_handover_fail controller "the ${source} backlog was not produced at once"
    done

    phase 'alteration handover: START with the backlog waiting'
    failure_category=product
    local started_ms
    started_ms="$(epoch_ms)"
    alteration_handover_cli 'START;' >"${case_dir}/start.txt" 2>&1 \
        || alteration_handover_fail product "START of ${alteration_handover_domain} was not acknowledged; see alteration-handover/start.txt"
    wait_for 'every backlog record delivered and confirmed' "${alteration_handover_backlog_bound}" \
        alteration_handover_backlog_delivered "${case_dir}" \
        || alteration_handover_fail product 'the backlog was not delivered; see alteration-handover/backlog-delivered.json'
    local delivered_ms
    delivered_ms="$(epoch_ms)"

    phase 'alteration handover: ALTER while a burst is in motion'
    failure_category=controller
    for source in "${alteration_handover_sources[@]}"; do
        burst_load_produce "alteration-handover-${source}" "$(alteration_handover_topic "${source}")" \
            "${artifact_dir}/fixtures/alteration-handover-${source}-motion.ndjson" \
            || alteration_handover_fail controller "the ${source} burst in motion was not produced at once"
    done
    failure_category=product
    alteration_handover_request "${case_dir}"
    # The alteration must meet records in motion: the output may not yet hold every record.
    failure_category=injection
    local output_at_request
    output_at_request="$(topic_end_offset "${alteration_handover_output_topic}")" \
        || alteration_handover_fail controller 'the output boundary at the alteration could not be read'
    ((output_at_request < $(alteration_handover_total_records))) \
        || alteration_handover_fail injection "every record had reached the output before the alteration, so none was in motion; see alteration-handover/alter-timing.json"
    failure_category=product
    wait "${alteration_handover_alter_pid}" || true
    [[ -s "${case_dir}/alter.exit" && -s "${case_dir}/alter.answered-ms" ]] \
        || alteration_handover_fail controller 'the alteration recorded no outcome'
    local answered_ms
    answered_ms="$(<"${case_dir}/alter.answered-ms")"
    jq -n --argjson started "${started_ms}" --argjson delivered "${delivered_ms}" \
        --argjson requested "${alteration_handover_requested_ms}" --argjson answered "${answered_ms}" \
        --argjson exit_code "$(<"${case_dir}/alter.exit")" \
        --argjson output_at_request "${output_at_request}" --argjson total "$(alteration_handover_total_records)" \
        '{start_ms: $started, backlog_delivered_ms: $delivered, requested_ms: $requested,
          answered_ms: $answered, exit_code: $exit_code, duration_ms: ($answered - $requested),
          output_at_request: $output_at_request, records: $total,
          in_motion_at_request: ($total - $output_at_request)}' \
        >"${case_dir}/alter-timing.json"
    printf 'ALTER answered in %sms with exit %s while %s records were in motion: %s\n' \
        "$((answered_ms - alteration_handover_requested_ms))" "$(<"${case_dir}/alter.exit")" \
        "$(($(alteration_handover_total_records) - output_at_request))" \
        "$(tr '\n' ' ' <"${case_dir}/alter-answer.txt")"

    phase 'alteration handover: both ledgers settle'
    failure_category=product
    alteration_handover_settled_ends=()
    if wait_for 'both sources confirmed and the output settled' "${alteration_handover_settle_bound}" \
        alteration_handover_settled "${case_dir}"; then
        alteration_handover_settled_within_bound=true
    fi

    failure_category=controller
    capture_all_metrics "${case_dir}/metrics-after" \
        || alteration_handover_fail controller 'the metrics after the alteration could not be read'
    "${script_dir}/negative-outcomes.sh" snapshot "${outcome_args[@]}" \
        --output "${case_dir}/negative-outcomes-after.json" "${case_dir}/metrics-after"/metrics-*.txt \
        || alteration_handover_fail controller 'the negative-outcome counters after the alteration could not be read'
    events_observer_collect "${case_dir}" \
        || alteration_handover_fail controller 'the error-event observers'"'"' transcripts could not be read'
    alteration_handover_node_logs "${case_dir}" \
        || alteration_handover_fail controller 'the node logs could not be read'
    check_support_containers

    failure_category=product
    alteration_handover_verify "${case_dir}"
    recovery_final_boundary
    jq -n --arg domain "${alteration_handover_domain}" \
        --argjson backlog "${alteration_handover_backlog_records}" \
        --argjson motion "${alteration_handover_motion_records}" \
        --arg drain_timeout "${alteration_handover_drain_timeout}" \
        --argjson settled "${alteration_handover_settled_within_bound}" \
        --slurpfile timing "${case_dir}/alter-timing.json" --slurpfile verdict "${case_dir}/verdict.json" \
        '{domain: $domain, backlog_records_per_source: $backlog, motion_records_per_source: $motion,
          domain_drain_timeout: $drain_timeout,
          alteration: ($timing[0] + {answer: $verdict[0].alteration.answer}),
          aborted_branches: $verdict[0].aborted_branches,
          settled: $settled, verdict: $verdict[0].verdict, symptoms: $verdict[0].symptoms,
          evidence: {verdict: "alteration-handover/verdict.json", delivery: "alteration-handover/delivery.json",
                     symptoms: "results/symptoms.json"}}' \
        >"${artifact_dir}/results/alteration-handover-progress.json"
}
