#!/usr/bin/env bash
# Sourced by run-baseline.sh for the backup-fault scenario, after backup-scenario.sh,
# recovery-scenario.sh and node-faults.sh. Owns one fault against a quiesced backup of the backup
# graph: a SIGKILL of the state owner or the leader, or an isolation of the state owner, while the
# cut holds the domain, or a rolling restart of every node while the archive streams to the client.
# It judges the backup's outcome against the backup contract, the domain's recovery, a recovery by
# the backup's execution reference where one applies, and a fresh backup restored and compared
# after the fault. Product violations are recovery findings; the run then reaches the final ledger.

# shellcheck source=recovery-scenario.sh
source "${script_dir}/recovery-scenario.sh"
# shellcheck source=node-faults.sh
source "${script_dir}/node-faults.sh"

# A gate that drops every packet the broker sends to the emitter's node holds a cut's drain open:
# the broker appends a batch the emitter publishes behind it, but the emitter receives no
# acknowledgement, the ingestor's sequential acknowledgement holds every later record behind it,
# and the drain waits for every admitted publish. The gate lifts only once the fault has taken
# effect, so the fault always meets a held cut. A cut whose drain the gate holds
# past the backup's 30-second quiesce timeout fails, and a publish the gate holds past the graph's
# 30-second ACK TIMEOUT is retried, which the ledger admits as a replay duplicate.
# The cut must be observed held within this bound after the backup starts.
backup_fault_cut_seconds=30
# The CLI's overall wait for the fault backup and its recovery. A cut whose owners never answer
# ends at its 30-second quiesce timeout; a new leader's repeated cut needs another.
backup_fault_wait="4m"
# The fault backup's CLI process ends within this bound of the fault.
backup_fault_exit_seconds=300
# The isolation holds longer than the quiesce timeout, so the cut cannot wait it out.
backup_fault_isolation_seconds=35
# The archive the rolling restart interrupts carries an incompressible resource this large, and the
# archive holder streams it to the client at this rate, so the stream outlasts both followers'
# restarts.
backup_fault_payload_mib=16
backup_fault_stream_rate=512kbit

backup_fault_fail() {
    recovery_fail "$@"
}

# The node host serving NODE.
backup_fault_host() {
    printf 'nervix-%s\n' "${1##*-}"
}

# Places the backup graph so that the leader coordinates, a follower owns the ingestor and relay
# state, and the other follower runs the emitter whose broker link the run delays. Sets
# backup_fault_leader, backup_fault_owner and backup_fault_sink to node names.
backup_fault_place() {
    local case_dir="$1"
    wait_for 'settled cluster before backup placement' 120 cluster_settled "${case_dir}"
    backup_fault_leader="$(<"${case_dir}/leader.txt")"
    local followers=()
    local node
    for node in node-3 node-2 node-1; do
        [[ "${node}" == "${backup_fault_leader}" ]] || followers+=("${node}")
    done
    backup_fault_owner="${followers[0]}"
    backup_fault_sink="${followers[1]}"
    domain_cli_command "RELOCATE INGESTOR chaos_ingestor ONTO NODE ${backup_fault_owner} IGNORE PREFERENCES;" \
        >"${case_dir}/relocate-ingestor.txt" 2>&1
    domain_cli_command "RELOCATE RELAY chaos_records ONTO NODE ${backup_fault_owner} IGNORE PREFERENCES;" \
        >"${case_dir}/relocate-relay.txt" 2>&1
    domain_cli_command "RELOCATE EMITTER chaos_emitter ONTO NODE ${backup_fault_sink} IGNORE PREFERENCES;" \
        >"${case_dir}/relocate-emitter.txt" 2>&1
    wait_for "ingestor owner ${backup_fault_owner}" 60 \
        describe_owner_ready 'DESCRIBE INGESTOR chaos_ingestor;' \
        "${backup_fault_owner}" "${case_dir}/describe-ingestor.txt"
    wait_for "relay owner ${backup_fault_owner}" 60 \
        describe_owner_ready 'DESCRIBE RELAY chaos_records;' \
        "${backup_fault_owner}" "${case_dir}/describe-relay.txt"
    wait_for "emitter owner ${backup_fault_sink}" 60 \
        describe_owner_ready 'DESCRIBE EMITTER chaos_emitter;' \
        "${backup_fault_sink}" "${case_dir}/describe-emitter.txt"
    jq -n --arg leader "${backup_fault_leader}" --arg owner "${backup_fault_owner}" \
        --arg sink "${backup_fault_sink}" \
        '{leader:$leader,state_owner:$owner,emitter_owner:$sink,
          evidence:["describe-ingestor.txt","describe-relay.txt","describe-emitter.txt","leader.txt"]}' \
        >"${case_dir}/placement.json"
}

backup_fault_gate_name() {
    printf '%s-acknowledgement-gate\n' "${run_id}"
}

# Closes the acknowledgement gate: the broker drops every packet it sends to the emitter's node.
backup_fault_close_gate() {
    local case_dir="$1"
    local sink_id sink_address
    sink_id="$(owned_service_container "$(backup_fault_host "${backup_fault_sink}")")" || return 1
    sink_address="$(container_address "${sink_id}")"
    [[ "${sink_address}" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] \
        || backup_fault_fail controller 'the emitter node has no address on the run network'
    node_fault_netem_start "${case_dir}" "$(backup_fault_gate_name)" broker broker "${sink_address}" 600 \
        'loss 100%' loss --percent 100
    backup_fault_gated_output=""
}

# The fault's DURING hook: lifts the gate once the fault has taken effect. The time recorded before
# the injector stops bounds every acknowledgement the gate withheld from below.
backup_fault_open_gate() {
    backup_fault_gate_opened_ms="$(epoch_ms)"
    node_fault_netem_heal "${backup_fault_case_dir}" "$(backup_fault_gate_name)" broker
}

# True once the output stays unchanged across two observations while the source runs at least two
# records ahead of it: a publish awaits the acknowledgement the gate withholds, and the ingestor's
# sequential acknowledgement holds every record after it.
backup_fault_gate_holds_publish() {
    local source_end output_end
    source_end="$(topic_end_offset chaos_input)" || return 1
    output_end="$(topic_end_offset chaos_output)" || return 1
    local previous_output="${backup_fault_gated_output}"
    backup_fault_gated_output="${output_end}"
    [[ "${output_end}" == "${previous_output}" ]] && ((source_end >= output_end + 2))
}

# Starts the fault backup through the sink node in the background. The report, the CLI's standard
# error, the time the CLI exited and its exit status go to the case directory, so the end time
# holds even while the run's foreground still holds or heals the fault.
backup_fault_start_backup() {
    local case_dir="${backup_fault_case_dir}"
    local archive_dir="${case_dir}/archive"
    mkdir -p "${archive_dir}"
    : >"${case_dir}/fault-backup.stderr"
    rm -f "${case_dir}/fault-backup.exit" "${case_dir}/fault-backup.ended-ms"
    backup_fault_backup_started_ms="$(epoch_ms)"
    (
        local status=0
        cli_host="$(backup_fault_host "${backup_fault_sink}")"
        compose_call_timeout=$((backup_fault_exit_seconds + 60))
        backup_archive_domain "${archive_dir}" chaos_baseline "${archive_dir}/fault.nvxb" \
            "${case_dir}/fault-backup.json" --backup-wait-timeout "${backup_fault_wait}" \
            2>"${case_dir}/fault-backup.stderr" || status=$?
        epoch_ms >"${case_dir}/fault-backup.ended-ms"
        printf '%s\n' "${status}" >"${case_dir}/fault-backup.exit"
    ) &
    backup_fault_backup_pid=$!
}

# True once the sink node reports the ingestor quiesced by the domain pause.
backup_fault_cut_held() {
    local case_dir="${backup_fault_case_dir}"
    cli_host="$(backup_fault_host "${backup_fault_sink}")"
    local status=0
    domain_cli_command 'DESCRIBE INGESTOR chaos_ingestor;' \
        >"${case_dir}/cut-observation.attempt.txt" 2>&1 || status=$?
    cli_host="${node_hosts[0]}"
    ((status == 0)) || return 1
    grep -Fxq 'status: quiesced' "${case_dir}/cut-observation.attempt.txt" \
        && grep -Fxq 'quiesce state: domain pause' "${case_dir}/cut-observation.attempt.txt"
}

backup_fault_cut_or_exit() {
    backup_fault_cut_held && return 0
    [[ -s "${backup_fault_case_dir}/fault-backup.exit" ]] && return 0
    return 1
}

# The fault's BEFORE hook: starts the backup and returns once the cut is observed holding the
# domain. A backup that ends first never let the fault reach a held cut.
backup_fault_begin_cut() {
    local case_dir="${backup_fault_case_dir}"
    backup_fault_start_backup
    wait_for 'the backup cut holding the domain' "${backup_fault_cut_seconds}" backup_fault_cut_or_exit \
        || backup_fault_fail injection 'the backup cut was never observed holding the domain'
    if [[ -s "${case_dir}/fault-backup.exit" ]]; then
        backup_fault_fail injection "the backup ended with status $(<"${case_dir}/fault-backup.exit") before its cut was observed held"
    fi
    cp "${case_dir}/cut-observation.attempt.txt" "${case_dir}/cut-observation.txt"
    backup_fault_cut_observed_ms="$(epoch_ms)"
}

backup_fault_backup_exited() {
    [[ -s "${backup_fault_case_dir}/fault-backup.exit" ]]
}

# The client stages a download in a private file beside its destination whose name starts with
# this prefix, and replaces the destination with it only once the whole archive verified.
backup_fault_staging_prefix=.nervix-backup-

# True when neither the archive NAME nor a client staging file remains in ARCHIVE_DIR.
backup_fault_destination_clean() {
    local archive_dir="$1"
    local name="$2"
    [[ ! -e "${archive_dir}/${name}" ]] || return 1
    local leftovers
    leftovers="$(find "${archive_dir}" -mindepth 1 -maxdepth 1 -name "${backup_fault_staging_prefix}*" -print -quit)"
    [[ -z "${leftovers}" ]]
}

# Judges the fault backup's outcome and records it in backup_fault_outcome: "refused" when the CLI
# failed with a backup refusal and left nothing at its destination, or "completed" when it wrote an
# archive that verifies offline and restores. Anything else is a finding. FAULT_MS is when the fault
# began and GATE_MS when the acknowledgement gate began to lift.
backup_fault_judge_outcome() {
    local case_dir="$1"
    local fault_ms="$2"
    local gate_ms="$3"
    local archive_dir="${case_dir}/archive"
    local status
    status="$(<"${case_dir}/fault-backup.exit")"
    backup_fault_outcome=invalid
    backup_fault_reference=""
    if [[ "${status}" == 0 ]]; then
        backup_fault_outcome=completed
        [[ -s "${archive_dir}/fault.nvxb" ]] \
            || { recovery_finding "${case_dir}" 'the backup reported success without an archive'; return 0; }
        backup_require_quiesced_report "${case_dir}/fault-backup.json"
        backup_cut_window "${case_dir}/fault-backup.json"
        backup_fault_cut_engaged_ms="${backup_engaged_ms}"
        backup_fault_cut_released_ms="${backup_released_ms}"
        if [[ "${fault}" == leader-crash ]] && ((backup_engaged_ms < fault_ms)); then
            recovery_finding "${case_dir}" 'an archive was assembled from a cut engaged before its coordinator crashed'
        fi
        if ((backup_released_ms < gate_ms)); then
            recovery_finding "${case_dir}" 'the cut released while the acknowledgement gate withheld an admitted publish'
        fi
        backup_verify_archive "${archive_dir}" "${archive_dir}/fault.nvxb" \
            "${archive_dir}/fault-description.json" 9
        backup_verify_restore "${archive_dir}" "${archive_dir}/fault.nvxb" \
            "${archive_dir}/fault-description.json" chaos_fault_restored \
            "${archive_dir}/fault-restore-report.json" \
            "${archive_dir}/fault-restored.nvxb" "${archive_dir}/fault-restored-backup-report.json" \
            "${archive_dir}/fault-restored-description.json" \
            "${archive_dir}/fault-expected-state.json" "${archive_dir}/fault-restored-state.json"
        return 0
    fi
    if ! jq -e '.error.code | type == "string"' "${case_dir}/fault-backup.json" >/dev/null 2>&1; then
        recovery_finding "${case_dir}" "the backup ended with status ${status} without a JSON failure report"
        return 0
    fi
    backup_fault_reference="$(jq -r '.error.execution_reference // empty' "${case_dir}/fault-backup.json")"
    jq -e '.error.message | startswith("backup failed:")' "${case_dir}/fault-backup.json" >/dev/null \
        || recovery_finding "${case_dir}" "the failed backup reported $(jq -r '.error.code' "${case_dir}/fault-backup.json") without a backup failure reason"
    backup_fault_destination_clean "${archive_dir}" fault.nvxb \
        || recovery_finding "${case_dir}" 'the failed backup left an archive or a partial file at its destination'
    backup_fault_outcome=refused
}

# Recovers the fault backup by its execution reference and records whether it returned a recorded
# failure or completed with an archive that verifies offline.
backup_fault_recover_reference() {
    local case_dir="$1"
    local archive_dir="${case_dir}/archive"
    backup_fault_recovery=none
    [[ -n "${backup_fault_reference}" ]] || return 0
    phase "backup fault: recovery by execution reference"
    local status=0
    compose_call_timeout=$((backup_fault_exit_seconds + 60))
    backup_archive_domain "${archive_dir}" chaos_baseline "${archive_dir}/recovered.nvxb" \
        "${case_dir}/recovered-backup.json" --backup-wait-timeout "${backup_fault_wait}" \
        --execution-reference "${backup_fault_reference}" \
        2>"${case_dir}/recovered-backup.stderr" || status=$?
    compose_call_timeout=120
    printf '%s\n' "${status}" >"${case_dir}/recovered-backup.exit"
    if ((status == 0)); then
        backup_fault_recovery=completed
        [[ -s "${archive_dir}/recovered.nvxb" ]] \
            || { recovery_finding "${case_dir}" 'the recovery reported success without an archive'; return 0; }
        backup_require_quiesced_report "${case_dir}/recovered-backup.json"
        backup_verify_archive "${archive_dir}" "${archive_dir}/recovered.nvxb" \
            "${archive_dir}/recovered-description.json" 9
        return 0
    fi
    backup_fault_recovery=refused
    jq -e --arg reference "${backup_fault_reference}" \
        '.error.code | type == "string"' "${case_dir}/recovered-backup.json" >/dev/null 2>&1 \
        || recovery_finding "${case_dir}" "the recovery ended with status ${status} without a JSON failure report"
    backup_fault_destination_clean "${archive_dir}" recovered.nvxb \
        || recovery_finding "${case_dir}" 'the failed recovery left an archive or a partial file at its destination'
}

# Waits for the cluster to settle and for traffic to move past the records the fault found.
backup_fault_recover_domain() {
    local case_dir="$1"
    phase "backup fault: domain recovery"
    mkdir -p "${case_dir}/settled"
    local started_ms
    started_ms="$(epoch_ms)"
    wait_for 'settled cluster after the fault' 240 cluster_settled "${case_dir}/settled" \
        || recovery_finding "${case_dir}" 'the cluster did not settle within 240 seconds of the fault'
    backup_fault_settled_ms=$(($(epoch_ms) - started_ms))
    check_support_containers
    wait_for 'Kafka source advanced past the fault' 60 \
        topic_progressed chaos_input "${backup_fault_source_at_fault}" \
        || recovery_finding "${case_dir}" 'the source did not advance after the fault'
    wait_for 'Kafka sink advanced past the fault' 180 \
        topic_progressed chaos_output "${backup_fault_output_at_fault}" \
        || recovery_finding "${case_dir}" 'the domain did not resume its output after the fault'
    backup_fault_resumed_ms=$(($(epoch_ms) - started_ms))
}

# Takes a fresh backup after the fault, verifies it offline, restores it as a new domain and
# compares it, then records its cut window.
backup_fault_fresh_backup() {
    local case_dir="$1"
    local archive_dir="${case_dir}/archive"
    phase "backup fault: fresh backup after recovery"
    local started_ms
    started_ms="$(epoch_ms)"
    backup_archive_domain "${archive_dir}" chaos_baseline "${archive_dir}/fresh.nvxb" \
        "${case_dir}/fresh-backup.json" 2>"${case_dir}/fresh-backup.stderr" \
        || { recovery_finding "${case_dir}" 'a fresh backup after the recovered fault failed'; return 0; }
    # The command covers the cut, assembly and the whole download into the client's destination.
    backup_fault_fresh_command_ms=$(($(epoch_ms) - started_ms))
    backup_fault_fresh_archive_bytes="$(stat -c %s "${archive_dir}/fresh.nvxb")"
    backup_require_quiesced_report "${case_dir}/fresh-backup.json"
    backup_cut_window "${case_dir}/fresh-backup.json"
    backup_fault_fresh_freeze_ms=$((backup_released_ms - backup_engaged_ms))
    backup_verify_archive "${archive_dir}" "${archive_dir}/fresh.nvxb" \
        "${archive_dir}/fresh-description.json" 9
    backup_verify_restore "${archive_dir}" "${archive_dir}/fresh.nvxb" \
        "${archive_dir}/fresh-description.json" chaos_restored \
        "${archive_dir}/fresh-restore-report.json" \
        "${archive_dir}/fresh-restored.nvxb" "${archive_dir}/fresh-restored-backup-report.json" \
        "${archive_dir}/fresh-restored-description.json" \
        "${archive_dir}/fresh-expected-state.json" "${archive_dir}/fresh-restored-state.json"
}

# The fault's DURING hook: records the traffic boundaries while the gate still holds the cut, then
# lifts the gate.
backup_fault_after_fault() {
    backup_fault_source_at_fault="$(topic_end_offset chaos_input)"
    backup_fault_output_at_fault="$(topic_end_offset chaos_output)"
    backup_fault_open_gate
}

# Holds a cut open, injects the fault while it holds and judges the backup.
backup_fault_during_cut() {
    local case_dir="$1"
    backup_fault_case_dir="${case_dir}"
    phase "backup fault: acknowledgement gate on the broker's link to the emitter"
    backup_fault_close_gate "${case_dir}"
    wait_for 'a published batch awaiting its acknowledgement behind the gate' 60 \
        backup_fault_gate_holds_publish \
        || backup_fault_fail injection 'the emitter published no batch behind the acknowledgement gate'
    local owner_host leader_host
    owner_host="$(backup_fault_host "${backup_fault_owner}")"
    leader_host="$(backup_fault_host "${backup_fault_leader}")"
    case "${fault}" in
        owner-crash)
            phase "backup fault: SIGKILL of state owner ${owner_host} during the cut"
            node_fault_kill "${case_dir}" "${owner_host}" "${outage_seconds}" \
                backup_fault_begin_cut backup_fault_after_fault
            ;;
        leader-crash)
            phase "backup fault: SIGKILL of leader ${leader_host} during the cut"
            node_fault_kill "${case_dir}" "${leader_host}" "${outage_seconds}" \
                backup_fault_begin_cut backup_fault_after_fault
            ;;
        owner-partition)
            phase "backup fault: isolation of state owner ${owner_host} during the cut"
            node_fault_isolate "${case_dir}" "${owner_host}" "${backup_fault_isolation_seconds}" \
                backup_fault_begin_cut backup_fault_after_fault
            ;;
    esac
    local fault_ms="${node_fault_started_ms}"
    wait_for 'the fault backup ended' "${backup_fault_exit_seconds}" backup_fault_backup_exited \
        || recovery_finding "${case_dir}" "the backup did not end within ${backup_fault_exit_seconds} seconds of the fault"
    if [[ -s "${case_dir}/fault-backup.exit" ]]; then
        wait "${backup_fault_backup_pid}" || true
        backup_fault_backup_ended_ms="$(<"${case_dir}/fault-backup.ended-ms")"
        backup_fault_judge_outcome "${case_dir}" "${fault_ms}" "${backup_fault_gate_opened_ms}"
    else
        backup_fault_outcome=unended
        backup_fault_backup_ended_ms=null
    fi
}

# Uploads an incompressible resource so the archive is large enough to outlast the rolling restart.
backup_fault_add_payload() {
    local case_dir="$1"
    local payload_dir="${case_dir}/payload"
    mkdir -p "${payload_dir}"
    head -c "$((backup_fault_payload_mib * 1048576))" /dev/urandom >"${payload_dir}/payload.bin"
    chmod 0755 "${payload_dir}"
    chmod 0644 "${payload_dir}/payload.bin"
    domain_cli_command 'CREATE RESOURCE chaos_backup_payload;' \
        >"${case_dir}/create-payload-resource.txt" 2>&1
    compose run --rm --no-deps -v "${payload_dir}:/chaos-payload:ro" admin \
        nervix-cli --server "http://${cli_host}:47391" --domain chaos_baseline \
        --password "${CHAOS_PASSWORD}" \
        --command "UPLOAD RESOURCE chaos_backup_payload VERSION '/chaos-payload';" \
        >"${case_dir}/upload-payload-resource.txt" 2>&1
    grep -Fxq 'uploaded resource version 1' "${case_dir}/upload-payload-resource.txt" \
        || backup_fault_fail setup 'the packaged CLI did not upload the backup payload as resource version 1'
}

# The size of the staging file the download is writing beside the destination, or zero.
backup_fault_download_bytes() {
    local archive_dir="$1"
    local largest=0
    local path
    while IFS= read -r -d '' path; do
        local size
        size="$(stat -c %s "${path}")"
        if ((size > largest)); then
            largest="${size}"
        fi
    done < <(find "${archive_dir}" -mindepth 1 -maxdepth 1 -type f -name "${backup_fault_staging_prefix}*" -print0)
    printf '%s\n' "${largest}"
}

# True once the download has written more than BYTES.
backup_fault_download_grew() {
    local archive_dir="$1"
    local bytes="$2"
    (($(backup_fault_download_bytes "${archive_dir}") > bytes))
}

# The rolling restart's DURING hook for a follower: the download must keep streaming while the
# follower is down.
backup_fault_stream_continues() {
    local archive_dir="${backup_fault_case_dir}/archive"
    local before
    before="$(backup_fault_download_bytes "${archive_dir}")"
    wait_for 'the archive download streaming while a follower is down' 60 \
        backup_fault_download_grew "${archive_dir}" "${before}" \
        || recovery_finding "${backup_fault_case_dir}" 'the archive download stalled while a follower was down'
}

# The rolling restart's DURING hook for the archive holder: its rate limit ended with the network
# namespace of its stopped container.
backup_fault_discard_stream_rate() {
    node_fault_netem_discard "${backup_fault_case_dir}" "${run_id}-stream-rate"
}

# Restarts every node in turn, the followers first, while the archive streams to the client at a
# bounded rate, and judges the interrupted download, its recovery and a fresh backup.
backup_fault_rolling_restart() {
    local case_dir="$1"
    backup_fault_case_dir="${case_dir}"
    local archive_dir="${case_dir}/archive"
    mkdir -p "${archive_dir}"
    phase "backup fault: incompressible ${backup_fault_payload_mib} MiB resource"
    backup_fault_add_payload "${case_dir}"
    backup_fault_source_at_fault="$(topic_end_offset chaos_input)"
    backup_fault_output_at_fault="$(topic_end_offset chaos_output)"
    local leader_host
    leader_host="$(backup_fault_host "${backup_fault_leader}")"

    phase 'backup fault: archive download at a bounded rate'
    # The client starts only once the archive holder's egress toward it is rate-limited, so the whole
    # stream crosses the limit.
    local client_name="${project_name}-backup-download"
    compose run --detach --no-deps --name "${client_name}" --user "$(id -u):$(id -g)" \
        -v "${archive_dir}:/chaos-backup" admin sh -c \
        "until [ -e /chaos-backup/.start ]; do sleep 0.2; done
         status=0
         nervix-cli --server http://${leader_host}:47391 --domain chaos_baseline \
             --password \"\${NERVIX_PASSWORD}\" --backup-wait-timeout ${backup_fault_wait} \
             backup domain chaos_baseline --output /chaos-backup/stream.nvxb --timeout 30s \
             --format json >/chaos-backup/stream-report.json 2>/chaos-backup/stream-stderr.txt \
             || status=\$?
         echo \"\${status}\" >/chaos-backup/stream-exit.txt" \
        >"${case_dir}/download-client.txt" 2>&1
    local client_address
    client_address="$(container_address "${client_name}")"
    [[ "${client_address}" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] \
        || backup_fault_fail controller 'the download client has no address on the run network'
    node_fault_netem_start "${case_dir}" "${run_id}-stream-rate" "${leader_host}" node \
        "${client_address}" 900 'rate 512Kbit' rate --rate "${backup_fault_stream_rate}"
    touch "${archive_dir}/.start"
    backup_fault_backup_started_ms="$(epoch_ms)"
    wait_for 'the archive download streaming' 120 backup_fault_download_grew "${archive_dir}" 0 \
        || backup_fault_fail injection 'the archive download never started streaming'
    backup_fault_cut_observed_ms="$(epoch_ms)"

    local host
    for host in "${node_hosts[@]}"; do
        [[ "${host}" == "${leader_host}" ]] && continue
        phase "backup fault: graceful restart of follower ${host} during the download"
        [[ ! -s "${archive_dir}/stream-exit.txt" ]] \
            || backup_fault_fail injection "the download ended before ${host} restarted"
        mkdir -p "${case_dir}/${host}"
        node_fault_stop "${case_dir}/${host}" "${host}" "${outage_seconds}" '' backup_fault_stream_continues
        node_fault_check_events "${case_dir}/${host}"
    done
    phase "backup fault: graceful restart of archive holder ${leader_host} during the download"
    [[ ! -s "${archive_dir}/stream-exit.txt" ]] \
        || backup_fault_fail injection 'the download ended before the archive holder restarted'
    mkdir -p "${case_dir}/${leader_host}"
    node_fault_stop "${case_dir}/${leader_host}" "${leader_host}" "${outage_seconds}" '' \
        backup_fault_discard_stream_rate
    local fault_ms="${node_fault_started_ms}"
    wait_for "the default interface state on the restarted ${leader_host}" 30 \
        node_fault_netem_removed "${leader_host}" "${case_dir}/${run_id}-stream-rate-rules-restarted.txt" \
        || backup_fault_fail injection "the restarted ${leader_host} carries a qdisc, filter or INPUT rule"
    node_fault_check_events "${case_dir}/${leader_host}"
    wait_for 'the interrupted download ended' 120 test -s "${archive_dir}/stream-exit.txt" \
        || recovery_finding "${case_dir}" 'the download did not end after the archive holder restarted'
    run_bounded 30 docker rm -f "${client_name}" >/dev/null 2>&1 || true
    backup_fault_backup_ended_ms="$(epoch_ms)"
    cp "${archive_dir}/stream-report.json" "${case_dir}/fault-backup.json" 2>/dev/null || true
    printf '%s\n' "$(cat "${archive_dir}/stream-exit.txt" 2>/dev/null || printf 'none')" \
        >"${case_dir}/fault-backup.exit"
    backup_fault_outcome=invalid
    backup_fault_reference=""
    if [[ "$(<"${case_dir}/fault-backup.exit")" == 0 ]]; then
        recovery_finding "${case_dir}" 'the download completed although its archive holder restarted while it streamed'
        backup_fault_outcome=completed
    elif jq -e '.error.code | type == "string"' "${case_dir}/fault-backup.json" >/dev/null 2>&1; then
        backup_fault_outcome=refused
        backup_fault_reference="$(jq -r '.error.execution_reference // empty' "${case_dir}/fault-backup.json")"
        [[ -n "${backup_fault_reference}" ]] \
            || recovery_finding "${case_dir}" 'the interrupted download reported no execution reference'
        backup_fault_destination_clean "${archive_dir}" stream.nvxb \
            || recovery_finding "${case_dir}" 'the interrupted download left an archive or a partial file at its destination'
    else
        recovery_finding "${case_dir}" 'the interrupted download ended without a JSON failure report'
    fi
    backup_fault_recover_domain "${case_dir}"
    backup_fault_recover_reference "${case_dir}"
    if [[ "${backup_fault_recovery}" == completed ]]; then
        recovery_finding "${case_dir}" 'a download recovered an archive its restarted holder no longer retains'
    fi
    jq -n --arg fault_ms "${fault_ms}" '{fault_ms:($fault_ms|tonumber)}' >"${case_dir}/fault-time.json"
}

run_backup_fault() {
    local case_dir="${artifact_dir}/backup-fault"
    mkdir -p "${case_dir}"
    recovery_traffic_startup "${case_dir}"
    failure_category=product
    phase 'backup fault: placement'
    backup_fault_place "${case_dir}"
    backup_fault_case_dir="${case_dir}"
    backup_fault_backup_started_ms=null
    backup_fault_cut_observed_ms=null
    backup_fault_cut_engaged_ms=null
    backup_fault_cut_released_ms=null
    backup_fault_gate_opened_ms=null
    backup_fault_isolation='null'
    backup_fault_recovery=none
    backup_fault_fresh_freeze_ms=null
    backup_fault_fresh_command_ms=null
    backup_fault_fresh_archive_bytes=null
    if [[ "${fault}" == rolling-restart ]]; then
        backup_fault_rolling_restart "${case_dir}"
    else
        backup_fault_during_cut "${case_dir}"
        if [[ "${fault}" == owner-partition ]]; then
            backup_fault_isolation="$(jq -nc --argjson installed "${rules_installed_ms}" \
                --argjson verified "${isolation_verified_ms}" --argjson heal_started "${heal_started_ms}" \
                '{installed:$installed, verified:$verified, heal_started:$heal_started}')"
        else
            node_fault_check_events "${case_dir}"
        fi
        backup_fault_recover_domain "${case_dir}"
        backup_fault_recover_reference "${case_dir}"
    fi
    backup_fault_fresh_backup "${case_dir}"
    check_support_containers
    # A completed report names its execution reference at the top, a failure inside its error.
    local error_code="" error_message="" reported_reference=""
    if [[ -s "${case_dir}/fault-backup.json" ]]; then
        error_code="$(jq -r '.error.code // empty' "${case_dir}/fault-backup.json" 2>/dev/null || true)"
        error_message="$(jq -r '.error.message // empty' "${case_dir}/fault-backup.json" 2>/dev/null || true)"
        reported_reference="$(jq -r '.execution_reference // .error.execution_reference // empty' \
            "${case_dir}/fault-backup.json" 2>/dev/null || true)"
    fi
    jq -n \
        --arg fault "${fault}" \
        --slurpfile placement "${case_dir}/placement.json" \
        --arg outcome "${backup_fault_outcome}" \
        --arg recovery "${backup_fault_recovery}" \
        --arg reference "${reported_reference}" \
        --arg error_code "${error_code}" \
        --arg error_message "${error_message}" \
        --argjson started "${backup_fault_backup_started_ms}" \
        --argjson observed "${backup_fault_cut_observed_ms}" \
        --argjson fault_ms "${node_fault_started_ms:-null}" \
        --argjson gate_opened "${backup_fault_gate_opened_ms}" \
        --argjson ended "${backup_fault_backup_ended_ms:-null}" \
        --argjson engaged "${backup_fault_cut_engaged_ms}" \
        --argjson released "${backup_fault_cut_released_ms}" \
        --argjson settled "${backup_fault_settled_ms:-null}" \
        --argjson resumed "${backup_fault_resumed_ms:-null}" \
        --argjson fresh_freeze "${backup_fault_fresh_freeze_ms}" \
        --argjson fresh_command "${backup_fault_fresh_command_ms}" \
        --argjson fresh_bytes "${backup_fault_fresh_archive_bytes}" \
        --argjson source_at_fault "${backup_fault_source_at_fault}" \
        --argjson output_at_fault "${backup_fault_output_at_fault}" \
        --argjson isolation "${backup_fault_isolation}" \
        '{fault:$fault, placement:$placement[0], outcome:$outcome, recovery_by_reference:$recovery,
          execution_reference:(if $reference == "" then null else $reference end),
          failure:(if $error_code == "" then null else {code:$error_code,message:$error_message} end),
          boundaries:{source_at_fault:$source_at_fault, output_at_fault:$output_at_fault},
          timings_ms:{backup_started:$started, cut_observed:$observed, fault:$fault_ms,
                      gate_opened:$gate_opened, backup_ended:$ended, cut_engaged:$engaged,
                      cut_released:$released, settled_after_fault:$settled,
                      resumed_after_fault:$resumed, fresh_backup_freeze:$fresh_freeze,
                      fresh_backup_command:$fresh_command, isolation:$isolation},
          fresh_archive_bytes:$fresh_bytes,
          evidence:{fault_backup:"backup-fault/fault-backup.json",
                    fresh_backup:"backup-fault/fresh-backup.json",
                    placement:"backup-fault/placement.json"}}' \
        >"${artifact_dir}/results/backup-fault-progress.json"
    recovery_final_boundary
}
