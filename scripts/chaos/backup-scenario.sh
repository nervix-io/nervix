#!/usr/bin/env bash
# Sourced by run-baseline.sh before graph installation: the run installs the graph that
# backup_graph_fixture prints, and run_backup_scenario or run_backup_cycles starts once the
# packaged cluster and that graph are ready. The backup fault and fresh-cluster restore scenarios
# reuse its archive, verification, restore and traffic steps.

backup_fail() {
    printf 'backup chaos failure: %s\n' "$*" >&2
    return 1
}

# The backup scenario needs source positions owned by the domain archive, so its Kafka ingestor
# reads domain-owned offsets where the other chaos scenarios keep their consumer group. Prints the
# backup form of the baseline graph text $1, or fails and prints nothing unless that text holds
# exactly one consumer-group offset clause to replace.
backup_graph_fixture() {
    local baseline_graph="$1"
    # The baseline fixture quotes the group, as Plexus spells every consumer-group name.
    local consumer_group_clause="OFFSET BY CONSUMER GROUP 'chaos_baseline'"
    local after_first_clause="${baseline_graph#*"${consumer_group_clause}"}"
    if [[ "${after_first_clause}" == "${baseline_graph}" ]]; then
        return 1
    fi
    # A second clause would leave one ingestor's offsets with its consumer group.
    if [[ "${after_first_clause}" == *"${consumer_group_clause}"* ]]; then
        return 1
    fi
    printf '%s' "${baseline_graph/"${consumer_group_clause}"/OFFSET BY DOMAIN}"
}

# Backs DOMAIN up through the packaged CLI on cli_host into ARCHIVE, a file directly under
# ARCHIVE_DIR, and writes the CLI's JSON report to REPORT. Further CLI options follow. Returns the
# CLI's exit status; judging the report is the caller's.
backup_archive_domain() {
    local archive_dir="$1"
    local domain="$2"
    local archive="$3"
    local report="$4"
    shift 4
    compose run --rm --no-deps --user "$(id -u):$(id -g)" \
        -v "${archive_dir}:/chaos-backup" admin \
        nervix-cli --server "http://${cli_host}:47391" \
        --domain "${domain}" --password "${CHAOS_PASSWORD}" \
        backup domain "${domain}" --output "/chaos-backup/${archive##*/}" \
        --timeout 30s --format json "$@" >"${report}"
}

# Requires the backup REPORT to name one domain with a completed quiesced cut.
backup_require_quiesced_report() {
    local report="$1"
    jq -e '.domains | length == 1' "${report}" >/dev/null \
        || backup_fail 'the backup report does not name one domain'
    jq -e '.domains[0].cut.kind == "quiesced" and .domains[0].cut.engaged_at != null and .domains[0].cut.released_at != null' \
        "${report}" >/dev/null \
        || backup_fail 'the backup report does not contain a completed quiesced cut'
}

# Describes ARCHIVE, a file directly under ARCHIVE_DIR, offline through the packaged CLI into
# DESCRIPTION, and requires a quiesced cut of chaos_baseline that holds branch lifecycle state and
# acknowledged Kafka domain offsets at or past MINIMUM_OFFSET.
backup_verify_archive() {
    local archive_dir="$1"
    local archive="$2"
    local description="$3"
    local minimum_offset="$4"
    compose run --rm --no-deps --user "$(id -u):$(id -g)" \
        -v "${archive_dir}:/chaos-backup:ro" admin \
        nervix-cli --command \
        "DESCRIBE BACKUP '/chaos-backup/${archive##*/}' FORMAT JSON;" \
        >"${description}" \
        || backup_fail 'the packaged CLI could not verify the archive offline'
    jq -e '.scope == "domain" and .domains[0].domain == "chaos_baseline" and .domains[0].cut.kind == "quiesced"' \
        "${description}" >/dev/null \
        || backup_fail 'the verified archive has the wrong domain or cut'
    jq -e '[.domains[0].runtime_state[] | select(.kind == "branch_lifecycle")] | length > 0' \
        "${description}" >/dev/null \
        || backup_fail 'the archive omitted branch lifecycle state'
    jq -e --argjson minimum "${minimum_offset}" \
        '[.domains[0].runtime_state[] | select(.kind == "kafka_offsets") | .positions[] |
            select(.topic == "chaos_input" and .partition == 0 and .next_offset >= $minimum)] |
            length > 0' \
        "${description}" >/dev/null \
        || backup_fail 'the archive omitted acknowledged Kafka domain offsets'
}

# Restores ARCHIVE, described in DESCRIPTION, as the stopped domain TARGET, backs TARGET up again and
# requires its public checkpoint identities, revisions, branch counts and exact source positions to
# equal the archive's. The restore report, the re-export's archive, report and description, and
# both state projections go to the paths that follow TARGET, all directly under ARCHIVE_DIR.
backup_verify_restore() {
    local archive_dir="$1"
    local archive="$2"
    local description="$3"
    local target="$4"
    local restore_report="$5"
    local restored_archive="$6"
    local restored_report="$7"
    local restored_description="$8"
    local expected_state="$9"
    local restored_state="${10}"
    compose run --rm --no-deps --user "$(id -u):$(id -g)" \
        -v "${archive_dir}:/chaos-backup:ro" admin \
        nervix-cli --server "http://${cli_host}:47391" \
        --domain chaos_baseline --password "${CHAOS_PASSWORD}" \
        restore domain chaos_baseline --as "${target}" \
        --input "/chaos-backup/${archive##*/}" --format json \
        >"${restore_report}" \
        || backup_fail 'the packaged CLI could not publish the complete restored generation'
    jq -e '.domains | length == 1' "${restore_report}" >/dev/null \
        || backup_fail 'the restore report does not name one domain'
    jq -e --arg target "${target}" '.domains[0].domain == $target' \
        "${restore_report}" >/dev/null \
        || backup_fail 'the restore report names the wrong target'
    backup_archive_domain "${archive_dir}" "${target}" "${restored_archive}" "${restored_report}" \
        || backup_fail 'the stopped restored domain could not be backed up'
    compose run --rm --no-deps --user "$(id -u):$(id -g)" \
        -v "${archive_dir}:/chaos-backup:ro" admin \
        nervix-cli --command \
        "DESCRIBE BACKUP '/chaos-backup/${restored_archive##*/}' FORMAT JSON;" \
        >"${restored_description}" \
        || backup_fail 'the restored generation could not be verified offline'
    # The renamed domain has different section paths and encoded record digests. Compare its
    # public checkpoint identities, revisions, branch counts and exact source positions.
    local state_projection='[.domains[0].runtime_state[] |
        {kind,owner_kind,entity,schema_fingerprint,revision,branches,positions}] |
        sort_by(.kind,.owner_kind,.entity,.schema_fingerprint)'
    jq -S "${state_projection}" "${description}" >"${expected_state}"
    jq -S "${state_projection}" "${restored_description}" >"${restored_state}"
    cmp "${expected_state}" "${restored_state}" \
        || backup_fail 'restored lifecycle or source checkpoints differ from the complete cut'
}

# Sets backup_engaged_at, backup_released_at, backup_engaged_ms and backup_released_ms from the
# completed cut the backup REPORT names, and requires the release not to precede the engagement.
backup_cut_window() {
    local report="$1"
    backup_engaged_at="$(jq -r '.domains[0].cut.engaged_at' "${report}")"
    backup_released_at="$(jq -r '.domains[0].cut.released_at' "${report}")"
    backup_engaged_ms="$(date -u -d "${backup_engaged_at}" +%s%3N)" \
        || backup_fail 'the cut engagement time cannot be parsed'
    backup_released_ms="$(date -u -d "${backup_released_at}" +%s%3N)" \
        || backup_fail 'the cut release time cannot be parsed'
    ((backup_released_ms >= backup_engaged_ms)) || backup_fail 'the cut release precedes engagement'
}

# Generates the run's baseline fixture, starts the load and the listener observer, and waits until
# the source and the sink have each passed their first ten records.
backup_traffic_start() {
    jq -nc --arg run_id "${run_id}" --argjson count "${record_count}" \
        --argjson branches "${scale_branches:-3}" \
        -f "${fixture_generator}" >"${artifact_dir}/fixtures/input.ndjson"
    [[ "$(wc -l <"${artifact_dir}/fixtures/input.ndjson")" -eq "${record_count}" ]] \
        || backup_fail 'input fixture generation was incomplete'
    compose up --detach --no-deps load observer
    wait_for 'independent load and observer' 30 check_support_containers
    wait_for 'source baseline' 40 topic_progressed chaos_input 9
    wait_for 'acknowledged sink baseline' 60 topic_progressed chaos_output 9
}

# Stops the load and reconstructs the accepted-input ledger from the source topic.
backup_final_boundary() {
    touch "${artifact_dir}/traffic/stop-load"
    local load_id
    load_id="$(owned_service_container load)" || return 1
    wait_for 'load producer flushed and exited' 40 load_exited_cleanly "${load_id}"
    producer_status="$(run_bounded 20 docker inspect --format '{{.State.ExitCode}}' "${load_id}")"
    input_end="$(topic_end_offset chaos_input)"
    [[ "${input_end}" =~ ^[0-9]+$ && "${input_end}" -le "${record_count}" ]] \
        || backup_fail 'source boundary exceeded the bounded fixture'
    kcat -q -b broker:9092 -C -t chaos_input -p 0 -o beginning -c "${input_end}" \
        >"${artifact_dir}/traffic/accepted-input.ndjson" \
        2>"${artifact_dir}/traffic/source-consumer.stderr"
    [[ "$(wc -l <"${artifact_dir}/traffic/accepted-input.ndjson")" -eq "${input_end}" ]] \
        || backup_fail 'accepted-input ledger did not cover the final source boundary'
}

# The accepted records' expected output counts of the backup state graph: the unique records below
# sequence 30, and every branch's complete windows of twelve records.
backup_state_expected_counts() {
    jq -s --argjson modulus "$((${scale_branches:-3} * 10))" '{unique: (map(select(.sequence < $modulus)) | length),
            windows: (group_by(.branch_name) | map(length / 12 | floor) | add // 0)}' "$1"
}

# Judges the deduplicator and window outputs of the backup state graph against the accepted-input
# ledger once both topics hold as many outputs as the accepted records produce, and writes the
# verdict to results/backup-state.json. Returns the verifier's status.
backup_state_verdict() {
    local case_dir="$1"
    local expected unique_count window_count
    expected="$(backup_state_expected_counts "${artifact_dir}/traffic/accepted-input.ndjson")"
    unique_count="$(jq -r '.unique' <<<"${expected}")"
    window_count="$(jq -r '.windows' <<<"${expected}")"
    # A topic that never reaches its count leaves the verifier to name the missing outputs.
    wait_for 'every unique record on the deduplicator output' 120 \
        topic_progressed chaos_unique_output "$((unique_count - 1))" || true
    wait_for 'every closed window on the window output' 120 \
        topic_progressed chaos_window_output "$((window_count - 1))" || true
    local topic
    for topic in chaos_unique_output chaos_window_output; do
        local topic_end
        topic_end="$(topic_end_offset "${topic}")"
        if ((topic_end > 0)); then
            kcat -q -b broker:9092 -C -t "${topic}" -p 0 -o beginning -c "${topic_end}" \
                >"${case_dir}/${topic}.ndjson" 2>"${case_dir}/${topic}.stderr"
        else
            : >"${case_dir}/${topic}.ndjson"
        fi
    done
    CHAOS_SCALE_MODULUS="$((${scale_branches:-3} * 10))" \
        "${script_dir}/verify-backup-state.sh" "${artifact_dir}/traffic/accepted-input.ndjson" \
        "${case_dir}/chaos_unique_output.ndjson" "${case_dir}/chaos_window_output.ndjson" \
        "${artifact_dir}/results/backup-state.json" >"${case_dir}/backup-state.txt"
}

# The cycles a backup-cycles run alternates with its traffic: each backs the domain up, verifies
# the archive offline, restores it as a new stopped domain and compares the restored generation.
backup_cycle_count="${scale_cycles:-8}"

# The highest chaos_input offset the archive DESCRIPTION records for the domain.
backup_archived_offset() {
    jq -r '[.domains[0].runtime_state[] | select(.kind == "kafka_offsets") | .positions[] |
        select(.topic == "chaos_input" and .partition == 0) | .next_offset] | max' "$1"
}

# The deduplicator keys and retained window rows the archive DESCRIPTION holds, as a JSON object,
# with a summary of every kind's sections.
backup_archived_state() {
    jq -c '[.domains[0].runtime_state[]] as $state
        | {deduplicator_keys: ([$state[] | select(.kind == "deduplicator") | .keys] | add // 0),
           window_rows: ([$state[] | select(.kind == "window_processor") | .rows] | add // 0),
           kinds: ($state | group_by(.kind) | map({kind: .[0].kind, sections: length,
                     rows: (map(.rows // 0) | add), keys: (map(.keys // 0) | add),
                     branches: (map(.branches // 0) | add)}))}' "$1"
}

# Campaign only: while the cycles run, one probe container samples every node's resident memory,
# executor reservations by class and restore staging every two seconds, and a host loop samples
# Docker's memory view of every container of the project.
campaign_sampler_start() {
    local cycles_dir="$1"
    local nodes="nervix-1"
    if [[ "${node_count}" == 3 ]]; then
        nodes="nervix-1 nervix-2 nervix-3"
    fi
    local script="while true; do t=\$(date +%s); for n in ${nodes}; do wget -q -T 3 -O - http://\$n:9090/metrics 2>/dev/null | grep -E '^nervix_(execution_memory_reserved_bytes|execution_memory_capacity_bytes|jemalloc_resident_bytes|restore_staging_bytes)' | sed \"s/^/\$t \$n /\"; done; sleep 2; done"
    campaign_sampler_container="$( (compose run -d --no-deps probe sh -c "${script}" 2>/dev/null || true) | tail -n 1)"
    (
        set +e
        while true; do
            stamp="$(date +%s)"
            docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' $(docker compose "${compose_args[@]}" ps -q 2>/dev/null) 2>/dev/null \
                | sed "s/^/${stamp} /"
            sleep 2
        done
    ) >"${cycles_dir}/docker-stats.txt" 2>/dev/null &
    campaign_stats_pid=$!
}

campaign_sampler_stop() {
    local cycles_dir="$1"
    kill "${campaign_stats_pid}" 2>/dev/null || true
    wait "${campaign_stats_pid}" 2>/dev/null || true
    if [[ -n "${campaign_sampler_container:-}" ]]; then
        docker logs "${campaign_sampler_container}" >"${cycles_dir}/metric-samples.txt" 2>/dev/null || true
        docker rm -f "${campaign_sampler_container}" >/dev/null 2>&1 || true
    fi
}

run_backup_cycles() {
    phase 'healthy acknowledged Kafka traffic'
    local cycles_dir="${artifact_dir}/backup-cycles"
    mkdir -p "${cycles_dir}"
    backup_traffic_start
    : >"${cycles_dir}/cycles.ndjson"
    campaign_sampler_start "${cycles_dir}"
    local previous_offset=9
    local cycle
    for cycle in $(seq 1 "${backup_cycle_count}"); do
        local cycle_dir="${cycles_dir}/cycle-${cycle}"
        mkdir -p "${cycle_dir}"
        phase "backup cycle ${cycle} of ${backup_cycle_count}"
        local output_before
        output_before="$(topic_end_offset chaos_output)"
        wait_for "new acknowledged traffic before cycle ${cycle}" 1800 \
            topic_progressed chaos_output "$((output_before + ${scale_cycle_records:-2}))"
        local started_ms
        started_ms="$(epoch_ms)"
        backup_archive_domain "${cycle_dir}" chaos_baseline "${cycle_dir}/domain.nvxb" \
            "${cycle_dir}/report.json" \
            || backup_fail "cycle ${cycle}: the packaged CLI could not create the quiesced archive"
        local backup_ms=$(($(epoch_ms) - started_ms))
        [[ -s "${cycle_dir}/domain.nvxb" ]] \
            || backup_fail "cycle ${cycle}: the backup archive was not retained"
        backup_require_quiesced_report "${cycle_dir}/report.json"
        backup_cut_window "${cycle_dir}/report.json"
        # Every cycle's archive holds the acknowledged source offset at or past the previous one's.
        backup_verify_archive "${cycle_dir}" "${cycle_dir}/domain.nvxb" \
            "${cycle_dir}/description.json" "${previous_offset}"
        local archived_offset
        archived_offset="$(backup_archived_offset "${cycle_dir}/description.json")"
        started_ms="$(epoch_ms)"
        backup_verify_restore "${cycle_dir}" "${cycle_dir}/domain.nvxb" "${cycle_dir}/description.json" \
            "chaos_cycle_${cycle}" "${cycle_dir}/restore-report.json" \
            "${cycle_dir}/restored.nvxb" "${cycle_dir}/restored-backup-report.json" \
            "${cycle_dir}/restored-description.json" \
            "${cycle_dir}/expected-state.json" "${cycle_dir}/restored-state.json"
        local restore_ms=$(($(epoch_ms) - started_ms))
        jq -nc --argjson cycle "${cycle}" --argjson offset "${archived_offset}" \
            --argjson freeze_ms "$((backup_released_ms - backup_engaged_ms))" \
            --argjson backup_ms "${backup_ms}" --argjson restore_ms "${restore_ms}" \
            --argjson archive_bytes "$(stat -c %s "${cycle_dir}/domain.nvxb")" \
            --argjson state "$(backup_archived_state "${cycle_dir}/description.json")" \
            --arg engaged_at "${backup_engaged_at}" --arg released_at "${backup_released_at}" \
            '{cycle:$cycle, archived_offset:$offset, freeze_duration_ms:$freeze_ms,
              backup_command_ms:$backup_ms, restore_and_compare_ms:$restore_ms,
              archive_bytes:$archive_bytes, archived_state:$state,
              engaged_at:$engaged_at, released_at:$released_at,
              restored_domain:("chaos_cycle_\($cycle)")}' >>"${cycles_dir}/cycles.ndjson"
        previous_offset="${archived_offset}"
    done
    campaign_sampler_stop "${cycles_dir}"

    phase 'post-cycle continuity'
    check_support_containers
    local source_after_cycles output_after_cycles
    source_after_cycles="$(topic_end_offset chaos_input)"
    output_after_cycles="$(topic_end_offset chaos_output)"
    wait_for 'Kafka source advanced past the last cycle' 30 \
        topic_progressed chaos_input "${source_after_cycles}"
    wait_for 'Kafka sink advanced past the last cycle' 60 \
        topic_progressed chaos_output "${output_after_cycles}"
    jq -s '{cycles: ., freeze_duration_ms: ([.[].freeze_duration_ms] | {min: min, max: max,
              median: (sort | .[(length / 2 | floor)])}),
            archived_offsets: [.[].archived_offset],
            evidence: "backup-cycles/cycles.ndjson"}' \
        "${cycles_dir}/cycles.ndjson" >"${artifact_dir}/results/backup-cycles-progress.json"

    phase 'backup traffic final boundary'
    backup_final_boundary
    phase 'deduplicator and window outputs'
    backup_state_verdict "${cycles_dir}" \
        || backup_fail 'the deduplicator or window outputs differ from what the accepted records produce'
}

run_backup_scenario() {
    phase 'healthy acknowledged Kafka traffic'
    mkdir -p "${artifact_dir}/backup"
    backup_traffic_start
    local source_before output_before
    source_before="$(topic_end_offset chaos_input)"
    output_before="$(topic_end_offset chaos_output)"
    check_support_containers

    phase 'quiesced backup under load'
    local archive_dir="${artifact_dir}/backup"
    local backup_report="${archive_dir}/report.json"
    local archive="${archive_dir}/domain.nvxb"
    backup_archive_domain "${archive_dir}" chaos_baseline "${archive}" "${backup_report}" \
        || backup_fail 'the packaged CLI could not create the quiesced archive'
    [[ -s "${archive}" ]] || backup_fail 'the backup archive was not retained'
    backup_require_quiesced_report "${backup_report}"

    phase 'offline archive verification'
    backup_verify_archive "${archive_dir}" "${archive}" "${archive_dir}/description.json" 9

    phase 'complete generation restore through the packaged CLI'
    backup_verify_restore "${archive_dir}" "${archive}" "${archive_dir}/description.json" \
        chaos_restored "${archive_dir}/restore-report.json" \
        "${archive_dir}/restored.nvxb" "${archive_dir}/restored-backup-report.json" \
        "${archive_dir}/restored-description.json" \
        "${archive_dir}/expected-state.json" "${archive_dir}/restored-state.json"

    phase 'post-cut continuity'
    check_support_containers
    wait_for 'Kafka source advanced through the cut' 30 \
        topic_progressed chaos_input "${source_before}"
    wait_for 'Kafka sink advanced through the cut' 60 \
        topic_progressed chaos_output "${output_before}"
    local source_after output_after
    source_after="$(topic_end_offset chaos_input)"
    output_after="$(topic_end_offset chaos_output)"
    backup_cut_window "${backup_report}"
    jq -n \
        --argjson source_before "${source_before}" --argjson source_after "${source_after}" \
        --argjson output_before "${output_before}" --argjson output_after "${output_after}" \
        --argjson freeze_duration_ms "$((backup_released_ms - backup_engaged_ms))" \
        --arg engaged_at "${backup_engaged_at}" --arg released_at "${backup_released_at}" \
        '{source_before:$source_before,source_after:$source_after,
          output_before:$output_before,output_after:$output_after,
          engaged_at:$engaged_at,released_at:$released_at,
          freeze_duration_ms:$freeze_duration_ms,
          archive:"backup/domain.nvxb",archive_description:"backup/description.json",
          backup_report:"backup/report.json",
          restore_report:"backup/restore-report.json",
          restored_archive_description:"backup/restored-description.json"}' \
        >"${artifact_dir}/results/backup-progress.json"

    phase 'backup traffic final boundary'
    backup_final_boundary
}
