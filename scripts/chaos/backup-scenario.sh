#!/usr/bin/env bash
# Sourced by run-baseline.sh after the packaged cluster and graph are ready.

backup_fail() {
    printf 'backup chaos failure: %s\n' "$*" >&2
    return 1
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
    compose run --rm --no-deps --user "$(id -u):$(id -g)" \
        -v "${archive_dir}:/chaos-backup" admin \
        nervix-cli --server "http://${cli_host}:47391" \
        --domain chaos_baseline --password "${CHAOS_PASSWORD}" \
        backup domain chaos_baseline --output /chaos-backup/domain.nvxb \
        --timeout 30s --format json >"${backup_report}" \
        || backup_fail 'the packaged CLI could not create the quiesced archive'
    [[ -s "${archive}" ]] || backup_fail 'the backup archive was not retained'
    jq -e '.domains | length == 1' "${backup_report}" >/dev/null \
        || backup_fail 'the backup report does not name one domain'
    jq -e '.domains[0].cut.kind == "quiesced" and .domains[0].cut.engaged_at != null and .domains[0].cut.released_at != null' \
        "${backup_report}" >/dev/null \
        || backup_fail 'the backup report does not contain a completed quiesced cut'

    phase 'offline archive verification'
    compose run --rm --no-deps --user "$(id -u):$(id -g)" \
        -v "${archive_dir}:/chaos-backup:ro" admin \
        nervix-cli --command \
        "DESCRIBE BACKUP '/chaos-backup/domain.nvxb' FORMAT JSON;" \
        >"${archive_dir}/description.json" \
        || backup_fail 'the packaged CLI could not verify the archive offline'
    jq -e '.scope == "domain" and .domains[0].domain == "chaos_baseline" and .domains[0].cut.kind == "quiesced"' \
        "${archive_dir}/description.json" >/dev/null \
        || backup_fail 'the verified archive has the wrong domain or cut'
    jq -e '[.domains[0].runtime_state[] | select(.kind == "branch_lifecycle")] | length > 0' \
        "${archive_dir}/description.json" >/dev/null \
        || backup_fail 'the archive omitted branch lifecycle state'
    jq -e '[.domains[0].runtime_state[] | select(.kind == "kafka_offsets") | .positions[] |
            select(.topic == "chaos_input" and .partition == 0 and .next_offset >= 9)] |
            length > 0' \
        "${archive_dir}/description.json" >/dev/null \
        || backup_fail 'the archive omitted acknowledged Kafka domain offsets'

    phase 'post-cut continuity'
    check_support_containers
    wait_for 'Kafka source advanced through the cut' 30 \
        topic_progressed chaos_input "${source_before}"
    wait_for 'Kafka sink advanced through the cut' 60 \
        topic_progressed chaos_output "${output_before}"
    local source_after output_after
    source_after="$(topic_end_offset chaos_input)"
    output_after="$(topic_end_offset chaos_output)"
    local engaged_at released_at engaged_ms released_ms
    engaged_at="$(jq -r '.domains[0].cut.engaged_at' "${backup_report}")"
    released_at="$(jq -r '.domains[0].cut.released_at' "${backup_report}")"
    engaged_ms="$(date -u -d "${engaged_at}" +%s%3N)" \
        || backup_fail 'the cut engagement time cannot be parsed'
    released_ms="$(date -u -d "${released_at}" +%s%3N)" \
        || backup_fail 'the cut release time cannot be parsed'
    ((released_ms >= engaged_ms)) || backup_fail 'the cut release precedes engagement'
    jq -n \
        --argjson source_before "${source_before}" --argjson source_after "${source_after}" \
        --argjson output_before "${output_before}" --argjson output_after "${output_after}" \
        --argjson freeze_duration_ms "$((released_ms - engaged_ms))" \
        --arg engaged_at "${engaged_at}" --arg released_at "${released_at}" \
        '{source_before:$source_before,source_after:$source_after,
          output_before:$output_before,output_after:$output_after,
          engaged_at:$engaged_at,released_at:$released_at,
          freeze_duration_ms:$freeze_duration_ms,
          archive:"backup/domain.nvxb",archive_description:"backup/description.json",
          backup_report:"backup/report.json"}' \
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
