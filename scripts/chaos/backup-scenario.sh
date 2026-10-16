#!/usr/bin/env bash
# Sourced by run-baseline.sh before graph installation: the run installs the graph that
# backup_graph_fixture prints, and run_backup_scenario starts once the packaged cluster and that
# graph are ready.

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

run_backup_scenario() {
    phase 'healthy acknowledged Kafka traffic'
    mkdir -p "${artifact_dir}/backup"
    jq -nc --arg run_id "${run_id}" --argjson count "${record_count}" \
        -f "${fixture_generator}" >"${artifact_dir}/fixtures/input.ndjson"
    [[ "$(wc -l <"${artifact_dir}/fixtures/input.ndjson")" -eq "${record_count}" ]] \
        || backup_fail 'input fixture generation was incomplete'
    compose up --detach --no-deps load observer
    wait_for 'independent load and observer' 30 check_support_containers
    wait_for 'source baseline' 40 topic_progressed chaos_input 9
    wait_for 'acknowledged sink baseline' 60 topic_progressed chaos_output 9
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
