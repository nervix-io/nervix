#!/usr/bin/env bash
# Sourced by run-baseline.sh for the restore-fault and restore-volume scenarios, after
# backup-scenario.sh. Owns what both restore scenarios share and the restore-volume run itself: the
# padded guest state their archive carries, the node sampler that records every node's restore
# staging, storage and memory series every two seconds, the packaged CLI's restores and the verdict
# on each one's outcome, the START gate of a restore that failed, the comparison of a restored
# stopped domain with the archive, and the verdict that staging returns to zero bytes on every node
# through completed sweeps. Product violations are recovery findings; the run then reaches the final
# ledger.

# shellcheck source=recovery-scenario.sh
source "${script_dir}/recovery-scenario.sh"
# shellcheck source=node-faults.sh
source "${script_dir}/node-faults.sh"

# The branches the baseline fixture's records name; each saves padded guest state once.
restore_branches=(alpha beta gamma)
# The sampler reads every node this often.
restore_sample_seconds=2
# A restore's CLI ends within this bound: the client resends across leader changes and polls a
# restore that still applies, and bounds only each frame and each reply wait itself.
restore_command_seconds=900
# Staging returns to zero bytes on every node within this bound of the restores' end.
restore_settle_seconds=300
# The concurrent restores of the restore-volume run.
restore_volume_restores=3

restore_fail() {
    recovery_fail "$@"
}

# The node host serving NODE.
restore_host() {
    printf 'nervix-%s\n' "${1##*-}"
}

# The node a HOST serves.
restore_node() {
    printf 'node-%s\n' "${1##*-}"
}

# Publishes one record a branch to the padded guest's topic, so every branch saves its padded state
# once, and waits until the processor reports a committed checkpoint of every branch that the
# deployment's replica count confirmed.
restore_seed_guest_state() {
    local case_dir="$1"
    local seed="${case_dir}/padded-input.ndjson"
    : >"${seed}"
    local index
    for index in "${!restore_branches[@]}"; do
        jq -nc --arg run_id "${run_id}" --argjson sequence "${index}" \
            --arg branch "${restore_branches[${index}]}" \
            '{event_id: ($run_id + "-padded-" + ($sequence | tostring)), branch_name: $branch,
              sequence: $sequence, content: "chaos-padded-seed"}' >>"${seed}"
    done
    kcat -b broker:9092 -P -t chaos_padded_input <"${seed}" >"${case_dir}/padded-producer.txt" 2>&1 \
        || restore_fail setup 'the padded guest records could not be published'
    wait_for 'every padded branch checkpoint committed and replicated' 180 \
        restore_guest_checkpoints_committed "${case_dir}/padded-checkpoints.json" \
        || restore_fail product 'the padded guest never committed and replicated a checkpoint of every branch'
}

# True when the padded processor reports a committed checkpoint of every branch that the
# deployment's replica count confirmed, written to OUTPUT.
restore_guest_checkpoints_committed() {
    local output="$1"
    domain_cli_command 'DESCRIBE WASM PROCESSOR chaos_padded FORMAT JSON;' >"${output%.json}.txt" 2>&1 \
        || return 1
    # The answer is the JSON document after the administration container's own progress lines.
    sed -n '/^{/,$p' "${output%.json}.txt" >"${output}"
    jq -e --argjson branches "${#restore_branches[@]}" --argjson replicas "${CHAOS_REPLICA_COUNT:-0}" '
        .checkpoint_counts.total == $branches and .checkpoint_counts.failed == 0
        and (.checkpoints | length) == $branches
        and all(.checkpoints[]; .stage == "ReplicaConfirmed"
                and .committed_revision == .latest_revision
                and .required_replicas == $replicas and .confirmed_replicas == $replicas)
    ' "${output}" >/dev/null 2>&1
}

# Starts the sampler: one probe container that reads every node's restore staging, storage-class
# and memory series every restore_sample_seconds and appends them, stamped with the probe's clock
# in seconds and the sample's ordinal, to CASE_DIR/samples/samples.txt until the stop file exists.
# A node that does not answer within the interval is recorded as unavailable.
restore_sampler_start() {
    local case_dir="$1"
    restore_sampler_dir="${case_dir}/samples"
    mkdir -p "${restore_sampler_dir}"
    chmod 0777 "${restore_sampler_dir}"
    : >"${restore_sampler_dir}/samples.txt"
    chmod 0666 "${restore_sampler_dir}/samples.txt"
    restore_sampler_name="${project_name}-restore-sampler"
    local series='^(nervix_restore_[a-z_]+|nervix_execution_jobs_(running|pending)[{]class="filesystem_storage"[}]|nervix_execution_memory_reserved_bytes[{]class="(bulk|restore_metadata)"[}]|nervix_jemalloc_resident_bytes) '
    compose run --detach --no-deps --name "${restore_sampler_name}" \
        -v "${restore_sampler_dir}:/samples" probe sh -c "
            ordinal=0
            while [ ! -e /samples/stop ]; do
                ordinal=\$((ordinal + 1))
                for host in ${node_hosts[*]}; do
                    now=\$(date +%s)
                    if wget -q -T ${restore_sample_seconds} -O /tmp/metrics http://\${host}:9090/metrics; then
                        grep -E '${series}' /tmp/metrics | sed \"s/^/\${now} \${ordinal} \${host} /\" >>/samples/samples.txt
                    else
                        echo \"\${now} \${ordinal} \${host} unavailable 1\" >>/samples/samples.txt
                    fi
                done
                sleep ${restore_sample_seconds}
            done" >"${case_dir}/sampler-start.txt" 2>&1 \
        || restore_fail controller 'the node sampler could not start'
}

# Stops the sampler and converts its samples into CASE_DIR/samples/samples.ndjson.
restore_sampler_stop() {
    [[ -n "${restore_sampler_name:-}" ]] || return 0
    touch "${restore_sampler_dir}/stop"
    run_bounded 60 docker wait "${restore_sampler_name}" >"${restore_sampler_dir}/sampler-exit.txt" 2>&1 || true
    run_bounded 30 docker rm -f "${restore_sampler_name}" >/dev/null 2>&1 || true
    restore_sampler_name=""
    restore_samples_json "${restore_sampler_dir}/samples.txt" >"${restore_sampler_dir}/samples.ndjson"
}

# Prints the samples of SAMPLES_TXT as JSON lines, {at, ordinal, host, series}, in the order the
# sampler took them. A series with a class label is named after its family and class, such as
# nervix_execution_memory_reserved_bytes_bulk, and a node that did not answer has only
# "unavailable". A line the sampler is still writing is left out.
restore_samples_json() {
    awk '
        NF == 5 && $5 ~ /^[0-9][0-9.e+]*$/ {
            key = $1 " " $2 " " $3
            if (!(key in seen)) {
                seen[key] = 1
                order[++count] = key
            }
            name = $4
            gsub(/[{]class="/, "_", name)
            gsub(/"[}]/, "", name)
            values[key] = values[key] (values[key] == "" ? "" : ",") sprintf("\"%s\":%s", name, $5)
        }
        END {
            for (i = 1; i <= count; i++) {
                split(order[i], part, " ")
                printf "{\"at\":%s,\"ordinal\":%s,\"host\":\"%s\",\"series\":{%s}}\n",
                    part[1], part[2], part[3], values[order[i]]
            }
        }
    ' "$1"
}

# Converts the sampler's samples so far into current.ndjson beside them, without stopping it.
restore_samples_now() {
    restore_samples_json "${restore_sampler_dir}/samples.txt" >"${restore_sampler_dir}/current.ndjson"
}

# Prints, as a JSON array, the hosts whose latest answered sample reports unpublished staging.
restore_staging_hosts() {
    restore_samples_now
    jq -sc '
        [group_by(.host)[]
         | [.[] | select(.series | has("nervix_restore_staging_bytes"))] | last
         | select(. != null and .series.nervix_restore_staging_bytes > 0)
         | .host]
    ' "${restore_sampler_dir}/current.ndjson"
}

# True when one of HOSTS... is staging, by its latest answered sample.
restore_staging_on() {
    local staging
    staging="$(restore_staging_hosts)" || return 1
    local host
    for host in "$@"; do
        jq -e --arg host "${host}" 'index($host) != null' <<<"${staging}" >/dev/null && return 0
    done
    return 1
}

# True when every host of HOSTS... is staging, by its latest answered sample.
restore_staging_on_all() {
    local staging
    staging="$(restore_staging_hosts)" || return 1
    local host
    for host in "$@"; do
        jq -e --arg host "${host}" 'index($host) != null' <<<"${staging}" >/dev/null || return 1
    done
}

# True when every node completed a sweep after the epoch second SINCE that measured no unpublished
# staging: two of its consecutive answered samples taken after SINCE both report zero staging bytes
# and keys, and its count of completed sweeps rose between them. A node that restarted in between
# counts its sweeps from zero again, so only a pair within one process qualifies.
restore_staging_settled() {
    local since="$1"
    restore_samples_now
    jq -se --argjson since "${since}" \
        --argjson hosts "$(printf '%s\n' "${node_hosts[@]}" | jq -R . | jq -sc .)" '
        def zero: .series.nervix_restore_staging_bytes == 0 and .series.nervix_restore_staging_keys == 0;
        def settled:
            [.[] | select(.at > $since and (.series | has("nervix_restore_staging_bytes")))] as $after
            | [range(1; $after | length) as $index
               | $after[$index - 1] as $before
               | $after[$index] as $now
               | ($before | zero) and ($now | zero)
                 and $now.series.nervix_restore_staging_sweeps_total
                     > $before.series.nervix_restore_staging_sweeps_total]
            | any;
        . as $samples
        | all($hosts[]; . as $host | [$samples[] | select(.host == $host)] | settled)
    ' "${restore_sampler_dir}/current.ndjson" >/dev/null 2>&1
}

# Waits until staging returns to zero bytes on every node through completed sweeps after the epoch
# second SINCE, and records a finding naming the nodes that still stage when the bound passes.
restore_staging_returns_to_zero() {
    local case_dir="$1"
    local since="$2"
    local started_ms
    started_ms="$(epoch_ms)"
    if wait_for 'staging back to zero bytes on every node through completed sweeps' \
        "${restore_settle_seconds}" restore_staging_settled "${since}"; then
        restore_settled_ms=$(($(epoch_ms) - started_ms))
        return 0
    fi
    restore_settled_ms=null
    recovery_finding "${case_dir}" "staging did not return to zero bytes through a completed sweep on every node within ${restore_settle_seconds} seconds; still staging: $(restore_staging_hosts)"
}

# Summarizes the samples of SAMPLES_NDJSON taken from the epoch second FROM through UNTIL, per node:
# the peak and the last value of every series, and how many samples each node answered.
restore_samples_summary() {
    local samples="$1"
    local from="$2"
    local until="$3"
    jq -sc --argjson from "${from}" --argjson until "${until}" '
        [.[] | select(.at >= $from and .at <= $until)]
        | group_by(.host)
        | map({key: .[0].host,
               value: ([.[] | select(.series | has("unavailable") | not)] as $answered
                       | {samples: length, answered: ($answered | length),
                          peak: ([$answered[].series | to_entries[]] | group_by(.key)
                                 | map({key: .[0].key, value: (map(.value) | max)}) | from_entries),
                          last: (($answered | last // {series: {}}).series)})})
        | from_entries
    ' "${samples}"
}

# Starts restoring ARCHIVE, a file directly under ARCHIVE_DIR, as the domain TARGET through the
# packaged CLI on SERVER_HOST in the background, in a container named after LABEL. Further CLI
# options follow. The report, the CLI's standard error, the time the CLI exited and its exit status
# go to CASE_DIR/LABEL.*, so they hold while the run's foreground still holds or heals a fault. Sets
# restore_pid and restore_container.
restore_start() {
    local case_dir="$1"
    local label="$2"
    local archive_dir="$3"
    local archive="$4"
    local target="$5"
    local server_host="$6"
    shift 6
    rm -f "${case_dir}/${label}.exit" "${case_dir}/${label}.ended-ms"
    : >"${case_dir}/${label}.stderr"
    epoch_ms >"${case_dir}/${label}.started-ms"
    restore_container="${project_name}-${label}"
    (
        local status=0
        compose_call_timeout="${restore_command_seconds}"
        compose run --rm --no-deps --name "${restore_container}" --user "$(id -u):$(id -g)" \
            -v "${archive_dir}:/chaos-backup:ro" admin \
            nervix-cli --server "http://${server_host}:47391" \
            --domain chaos_baseline --password "${CHAOS_PASSWORD}" \
            restore domain chaos_baseline --as "${target}" \
            --input "/chaos-backup/${archive##*/}" --format json "$@" \
            >"${case_dir}/${label}.json" 2>"${case_dir}/${label}.stderr" || status=$?
        epoch_ms >"${case_dir}/${label}.ended-ms"
        printf '%s\n' "${status}" >"${case_dir}/${label}.exit"
    ) &
    restore_pid=$!
}

# True once the CLI of the restore LABEL exited.
restore_ended() {
    local case_dir="$1"
    local label="$2"
    [[ -s "${case_dir}/${label}.exit" ]]
}

# Runs the restore LABEL with the arguments of restore_start to its end in the foreground and
# judges it.
restore_run() {
    local case_dir="$1"
    local label="$2"
    local target="$5"
    restore_start "$@"
    wait "${restore_pid}" || true
    restore_judge "${case_dir}" "${label}" "${target}"
}

# The milliseconds the CLI of the restore LABEL ran, or null while it has not ended.
restore_command_ms() {
    local case_dir="$1"
    local label="$2"
    if [[ ! -s "${case_dir}/${label}.ended-ms" ]]; then
        printf 'null\n'
        return 0
    fi
    printf '%s\n' "$(($(<"${case_dir}/${label}.ended-ms") - $(<"${case_dir}/${label}.started-ms")))"
}

# Judges the ended restore LABEL of TARGET against the restore contract and records its outcome in
# restore_outcome and its execution reference in restore_reference:
#   completed   the CLI exited 0 with a report naming TARGET and every step applied
#   incomplete  RESTORE_INCOMPLETE with a report naming TARGET and the one step that failed
#   refused     RESTORE_REFUSED: the restore was refused before it changed anything
#   failed      any other end, which leaves its outcome unrecorded
# A completed report without every step applied, an incomplete one without exactly one failed step,
# a refusal of a restore whose target exists, and a failure all are findings.
restore_judge() {
    local case_dir="$1"
    local label="$2"
    local target="$3"
    local report="${case_dir}/${label}.json"
    local status
    status="$(<"${case_dir}/${label}.exit")"
    restore_outcome=failed
    restore_reference=""
    if [[ "${status}" == 0 ]]; then
        restore_outcome=completed
        restore_reference="$(jq -r '.execution_reference // empty' "${report}" 2>/dev/null || true)"
        jq -e --arg target "${target}" '
            (.domains | length) == 1 and .domains[0].domain == $target
            and (.steps | length) > 0 and all(.steps[]; .outcome == "applied")
        ' "${report}" >/dev/null 2>&1 \
            || recovery_finding "${case_dir}" "restore ${label} reported success without applying every step of ${target}"
        return 0
    fi
    local code
    code="$(jq -r '.error.code // empty' "${report}" 2>/dev/null || true)"
    case "${code}" in
        RESTORE_INCOMPLETE)
            restore_outcome=incomplete
            restore_reference="$(jq -r '.error.report.execution_reference // empty' "${report}")"
            jq -e --arg target "${target}" '
                .error.report.domains[0].domain == $target
                and ([.error.report.steps[] | select(.outcome == "failed")] | length) == 1
            ' "${report}" >/dev/null 2>&1 \
                || recovery_finding "${case_dir}" "restore ${label} failed without a report naming the one step of ${target} that failed"
            ;;
        RESTORE_REFUSED)
            restore_outcome=refused
            if restore_target_exists "${case_dir}" "${target}"; then
                recovery_finding "${case_dir}" "restore ${label} was refused although it created ${target}"
            fi
            ;;
        *)
            recovery_finding "${case_dir}" "restore ${label} ended with status ${status} and ${code:-no JSON failure report}, not with its recorded outcome"
            ;;
    esac
}

# True when the cluster lists the domain TARGET.
restore_target_exists() {
    local case_dir="$1"
    local target="$2"
    cli_command 'LIST DOMAINS;' >"${case_dir}/domains-${target}.txt" 2>&1 || return 1
    grep -Eq "(^|[^a-z0-9_])${target}([^a-z0-9_]|$)" "${case_dir}/domains-${target}.txt"
}

# Requires START of TARGET, whose restore failed, to be refused because its state installation is
# incomplete, and records a finding otherwise.
restore_require_gated() {
    local case_dir="$1"
    local target="$2"
    local status=0
    run_cli "${target}" 'START;' >"${case_dir}/start-${target}.txt" 2>&1 || status=$?
    if ((status == 0)); then
        recovery_finding "${case_dir}" "START of ${target} succeeded although its restore failed"
        return 0
    fi
    grep -Fq "domain '${target}' restore state installation is incomplete" "${case_dir}/start-${target}.txt" \
        || recovery_finding "${case_dir}" "START of ${target} was refused without naming its incomplete restore state installation"
}

# Backs the stopped restored domain TARGET up into ARCHIVE_DIR, describes that archive offline and
# requires it to hold the state of the archive DESCRIPTION describes: the backup state projection,
# and every padded guest save byte for byte. Returns nonzero, with the reason on standard error,
# when it does not.
restore_compare() {
    local archive_dir="$1"
    local description="$2"
    local target="$3"
    backup_archive_domain "${archive_dir}" "${target}" "${archive_dir}/${target}.nvxb" \
        "${archive_dir}/${target}-backup-report.json" \
        || { printf 'the restored domain %s could not be backed up\n' "${target}" >&2; return 1; }
    compose run --rm --no-deps --user "$(id -u):$(id -g)" \
        -v "${archive_dir}:/chaos-backup:ro" admin \
        nervix-cli --command "DESCRIBE BACKUP '/chaos-backup/${target}.nvxb' FORMAT JSON;" \
        >"${archive_dir}/${target}-description.json" \
        || { printf 'the archive of %s could not be verified offline\n' "${target}" >&2; return 1; }
    jq -S "${backup_state_projection}" "${description}" >"${archive_dir}/expected-state.json"
    jq -S "${backup_state_projection}" "${archive_dir}/${target}-description.json" \
        >"${archive_dir}/${target}-state.json"
    cmp -s "${archive_dir}/expected-state.json" "${archive_dir}/${target}-state.json" \
        || { printf 'the checkpoints of %s differ from the archive\n' "${target}" >&2; return 1; }
    local guests='[.domains[0].runtime_state[] | select(.kind == "wasm_processor")
        | {entity, schema_fingerprint, branch_fingerprint, bytes: .guest.bytes, blake3: .guest.blake3}]
        | sort_by(.entity, .branch_fingerprint)'
    jq -S "${guests}" "${description}" >"${archive_dir}/expected-guests.json"
    jq -S "${guests}" "${archive_dir}/${target}-description.json" >"${archive_dir}/${target}-guests.json"
    jq -e --argjson branches "${#restore_branches[@]}" 'length == $branches' \
        "${archive_dir}/expected-guests.json" >/dev/null \
        || { printf 'the archive does not hold a padded guest save of every branch\n' >&2; return 1; }
    cmp -s "${archive_dir}/expected-guests.json" "${archive_dir}/${target}-guests.json" \
        || { printf 'the guest saves of %s differ from the archive\n' "${target}" >&2; return 1; }
}

# Compares the stopped restored domain TARGET with the archive and records a finding when it differs.
restore_require_equal() {
    local case_dir="$1"
    local archive_dir="$2"
    local description="$3"
    local target="$4"
    local reason
    if ! reason="$(restore_compare "${archive_dir}" "${description}" "${target}" 2>&1 >/dev/null)"; then
        recovery_finding "${case_dir}" "restored domain ${target} does not equal the archive: ${reason}"
        return 1
    fi
}

# Takes the quiesced backup the restores restore, verifies it offline with its guest saves of
# GUEST_BYTES each, and records it in CASE_DIR/archive.
restore_take_archive() {
    local case_dir="$1"
    local guest_bytes="$2"
    local archive_dir="${case_dir}/archive"
    mkdir -p "${archive_dir}"
    wait_for 'source baseline' 40 topic_progressed chaos_input 9
    wait_for 'acknowledged sink baseline' 60 topic_progressed chaos_output 9
    backup_archive_domain "${archive_dir}" chaos_baseline "${archive_dir}/domain.nvxb" \
        "${archive_dir}/report.json" \
        || restore_fail product 'the packaged CLI could not create the quiesced archive'
    backup_require_quiesced_report "${archive_dir}/report.json"
    backup_verify_archive "${archive_dir}" "${archive_dir}/domain.nvxb" "${archive_dir}/description.json" 9
    jq -e --argjson branches "${#restore_branches[@]}" --argjson bytes "${guest_bytes}" '
        [.domains[0].runtime_state[] | select(.kind == "wasm_processor" and .entity == "chaos_padded")]
        | length == $branches and all(.[]; .guest.bytes == $bytes)
    ' "${archive_dir}/description.json" >/dev/null \
        || restore_fail product "the archive does not hold a ${guest_bytes}-byte guest save of every padded branch"
}

run_restore_volume() {
    local case_dir="${artifact_dir}/restore-volume"
    local archive_dir="${case_dir}/archive"
    mkdir -p "${archive_dir}"
    recovery_traffic_startup "${case_dir}"
    failure_category=product

    phase 'restore volume: padded guest state'
    restore_seed_guest_state "${case_dir}"
    phase 'restore volume: quiesced backup under load'
    restore_take_archive "${case_dir}" "${restore_guest_bytes}"
    local archive_bytes
    archive_bytes="$(stat -c %s "${archive_dir}/domain.nvxb")"

    restore_sampler_start "${case_dir}"
    phase 'restore volume: steady state before the restores'
    # The sampler has read every node at least twice before the restores begin.
    wait_for 'two samples of every node before the restores' 60 restore_sampled_twice
    local before_at
    before_at="$(date +%s)"

    phase "restore volume: ${restore_volume_restores} concurrent restores"
    local labels=()
    local pids=()
    local index
    for index in $(seq 1 "${restore_volume_restores}"); do
        local label="volume-${index}"
        local host="${node_hosts[$(((index - 1) % ${#node_hosts[@]}))]}"
        restore_start "${case_dir}" "${label}" "${archive_dir}" "${archive_dir}/domain.nvxb" \
            "chaos_volume_${index}" "${host}"
        labels+=("${label}")
        pids+=("${restore_pid}")
    done
    local pid
    for pid in "${pids[@]}"; do
        wait "${pid}" || true
    done
    local restores_ended_at
    restores_ended_at="$(date +%s)"
    local outcomes=()
    for index in $(seq 1 "${restore_volume_restores}"); do
        restore_judge "${case_dir}" "volume-${index}" "chaos_volume_${index}"
        outcomes+=("$(jq -nc --arg label "volume-${index}" --arg target "chaos_volume_${index}" \
            --arg outcome "${restore_outcome}" --arg reference "${restore_reference}" \
            --argjson command_ms "$(restore_command_ms "${case_dir}" "volume-${index}")" \
            '{label:$label, target:$target, outcome:$outcome,
              execution_reference:(if $reference == "" then null else $reference end),
              command_ms:$command_ms}')")
        [[ "${restore_outcome}" == completed ]] \
            || recovery_finding "${case_dir}" "concurrent restore volume-${index} ended ${restore_outcome}"
    done

    phase 'restore volume: staging back to zero'
    restore_staging_returns_to_zero "${case_dir}" "${restores_ended_at}"
    local settled_at
    settled_at="$(date +%s)"
    # Allocation after the restores is recorded over the sweeps that follow the settled staging.
    wait_for 'two samples of every node after staging settled' 60 restore_sampled_twice_since "${settled_at}"
    restore_sampler_stop

    phase 'restore volume: restored domains against the archive'
    for index in $(seq 1 "${restore_volume_restores}"); do
        if [[ "$(jq -r '.outcome' <<<"${outcomes[$((index - 1))]}")" == completed ]]; then
            restore_require_equal "${case_dir}" "${archive_dir}" "${archive_dir}/description.json" \
                "chaos_volume_${index}" || true
        fi
    done
    check_support_containers

    local samples="${restore_sampler_dir}/samples.ndjson"
    jq -n \
        --arg image_id "${image_id}" --arg image_digest "${image_digest}" \
        --arg image_ref "${image_ref}" \
        --argjson restores "${restore_volume_restores}" \
        --argjson guest_bytes "${restore_guest_bytes}" \
        --argjson branches "${#restore_branches[@]}" \
        --argjson replicas "${CHAOS_REPLICA_COUNT:-0}" \
        --argjson archive_bytes "${archive_bytes}" \
        --argjson outcomes "$(printf '%s\n' "${outcomes[@]}" | jq -sc .)" \
        --argjson before "$(restore_samples_summary "${samples}" 0 "${before_at}")" \
        --argjson during "$(restore_samples_summary "${samples}" "${before_at}" "${restores_ended_at}")" \
        --argjson after "$(restore_samples_summary "${samples}" "${settled_at}" 9999999999)" \
        --argjson settle_ms "${restore_settled_ms}" \
        '{image:{reference:$image_ref, id:$image_id, digest:$image_digest},
          archive:{bytes:$archive_bytes, guest_save_bytes:$guest_bytes, branches:$branches,
                   replicas:$replicas},
          concurrent_restores:$restores, restores:$outcomes,
          staging_settled_after_restores_ms:$settle_ms,
          samples:{interval_seconds:2, before:$before, during:$during, after:$after},
          evidence:{samples:"restore-volume/samples/samples.ndjson",
                    archive_description:"restore-volume/archive/description.json"}}' \
        >"${artifact_dir}/results/restore-volume-progress.json"
    recovery_final_boundary
    phase 'restore volume: deduplicator and window outputs'
    backup_state_verdict "${case_dir}" \
        || recovery_finding "${case_dir}" 'the deduplicator or window outputs differ from what the accepted records produce'
}

# True when the sampler answered from every node at least twice.
restore_sampled_twice() {
    restore_sampled_twice_since 0
}

# True when the sampler answered from every node at least twice after the epoch second SINCE.
restore_sampled_twice_since() {
    local since="$1"
    restore_samples_now
    jq -se --argjson since "${since}" \
        --argjson hosts "$(printf '%s\n' "${node_hosts[@]}" | jq -R . | jq -sc .)" '
        . as $samples
        | all($hosts[]; . as $host
              | [$samples[] | select(.host == $host and .at > $since
                                     and (.series | has("nervix_restore_staging_bytes")))]
              | length >= 2)
    ' "${restore_sampler_dir}/current.ndjson" >/dev/null 2>&1
}
