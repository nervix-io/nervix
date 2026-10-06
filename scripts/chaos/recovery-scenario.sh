#!/usr/bin/env bash
# Sourced by the stale-follower, former-owner-restart and cluster-restart scenarios after the shared
# external cluster and graph are ready. Owns what the three restart and recovery cases share:
# traffic around the fault, administration batches through the packaged CLI, node metric and status
# records, committed-configuration captures, product findings and the final traffic boundary.

# shellcheck source=partition-scenario.sh
source "${script_dir}/partition-scenario.sh"

# Product violations that leave the experiment meaningful accumulate here. The run continues
# through recovery and the final ledger, then fails with every violation it observed.
recovery_findings_ledger="${artifact_dir}/results/recovery-findings.ndjson"

# Node containers a case has stopped and not started again. The exit trap starts them before it
# captures diagnostics, so neither retained resources nor evidence leave a node down.
recovery_stopped_ids=()

# The SHOW CREATE statement of every model the baseline graph installs, keyed by a file label.
declare -A recovery_graph_models=(
    [schema-chaos_record]='SHOW CREATE SCHEMA chaos_record;'
    [wire-chaos_wire]='SHOW CREATE WIRE JSON SCHEMA chaos_wire;'
    [codec-chaos_json]='SHOW CREATE CODEC chaos_json;'
    [schema-chaos_branch_key]='SHOW CREATE SCHEMA chaos_branch_key;'
    [branch-chaos_branch]='SHOW CREATE BRANCH chaos_branch;'
    [relay-chaos_records]='SHOW CREATE RELAY chaos_records;'
    [client-chaos_kafka]='SHOW CREATE CLIENT chaos_kafka;'
    [ingestor-chaos_ingestor]='SHOW CREATE INGESTOR chaos_ingestor;'
    [emitter-chaos_emitter]='SHOW CREATE EMITTER chaos_emitter;'
)

recovery_fail() {
    failure_category="$1"
    shift
    printf '%s %s failure: %s\n' "${scenario}" "${failure_category}" "$*" >&2
    return 1
}

# Records a product violation that does not stop the experiment.
recovery_finding() {
    local case_dir="$1"
    shift
    printf '%s product finding: %s\n' "${scenario}" "$*" >&2
    jq -nc \
        --arg scenario "${scenario}" \
        --arg message "$*" \
        --arg phase "${current_phase}" \
        --arg at "$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)" \
        '{scenario:$scenario,phase:$phase,message:$message,at:$at}' \
        | tee -a "${case_dir}/findings.ndjson" >>"${recovery_findings_ledger}"
}

recovery_node_stopped() {
    recovery_stopped_ids+=("$1")
}

recovery_node_started() {
    local started="$1"
    local remaining=()
    local container_id
    for container_id in "${recovery_stopped_ids[@]}"; do
        [[ "${container_id}" == "${started}" ]] || remaining+=("${container_id}")
    done
    recovery_stopped_ids=("${remaining[@]}")
}

# Starts continuous producer traffic and the listener observer, then waits for source and sink
# progress and for the public cross-node path before any fault.
recovery_traffic_startup() {
    local case_dir="$1"
    phase "${scenario} traffic startup"
    mkdir -p "${case_dir}/initial"
    : >"${recovery_findings_ledger}"
    jq -nc --arg run_id "${run_id}" --argjson count "${record_count}" \
        -f "${fixture_generator}" >"${artifact_dir}/fixtures/input.ndjson"
    [[ "$(wc -l <"${artifact_dir}/fixtures/input.ndjson")" -eq "${record_count}" ]] \
        || recovery_fail controller 'fixture generation was incomplete'
    compose up --detach --no-deps load observer
    wait_for 'independent load, observer and broker' 30 check_support_containers
    wait_for 'source traffic started' 30 topic_progressed chaos_input 0
    wait_for 'sink traffic started' 60 topic_progressed chaos_output 0
    wait_for 'initial cross-node traffic metrics' 60 initial_remote_path_ready
    local host
    local evidence=()
    for host in "${node_hosts[@]}"; do
        cp "${artifact_dir}/public/metrics-${host}.txt" "${case_dir}/initial/metrics-${host}.txt"
        evidence+=("${case_dir#"${artifact_dir}/"}/initial/metrics-${host}.txt")
    done
    local remote_path_tmp
    remote_path_tmp="$(mktemp "${artifact_dir}/results/.remote-path.XXXXXX")"
    jq --argjson evidence "$(printf '%s\n' "${evidence[@]}" | jq -R . | jq -s .)" \
        '.traffic_observed_before_fault = true | .traffic_evidence = $evidence' \
        "${artifact_dir}/results/remote-path.json" >"${remote_path_tmp}"
    mv "${remote_path_tmp}" "${artifact_dir}/results/remote-path.json"
}

# Stops the producer and reconstructs the accepted-input ledger from the source topic.
recovery_final_boundary() {
    phase "${scenario} final traffic boundary"
    touch "${artifact_dir}/traffic/stop-load"
    local load_id
    load_id="$(owned_service_container load)" || return 1
    wait_for 'load producer flushed and exited' 40 load_exited_cleanly "${load_id}"
    producer_status="$(run_bounded 20 docker inspect --format '{{.State.ExitCode}}' "${load_id}")"
    run_bounded 20 docker logs "${load_id}" >"${artifact_dir}/traffic/producer.log" 2>&1
    trim_file "${artifact_dir}/traffic/producer.log" 1048576
    input_end="$(topic_end_offset chaos_input)"
    [[ "${input_end}" =~ ^[0-9]+$ && "${input_end}" -le "${record_count}" ]] \
        || recovery_fail product 'source boundary exceeded the bounded fixture'
    kcat -q -b broker:9092 -C -t chaos_input -p 0 -o beginning -c "${input_end}" \
        >"${artifact_dir}/traffic/accepted-input.ndjson" \
        2>"${artifact_dir}/traffic/source-consumer.stderr"
    [[ "$(wc -l <"${artifact_dir}/traffic/accepted-input.ndjson")" -eq "${input_end}" ]] \
        || recovery_fail product 'accepted-input ledger did not cover the final source boundary'
}

# Runs each LABEL STATEMENT pair through the packaged CLI against HOST in one administration
# container, one CLI process and session per statement, and splits the transcript into
# OUTPUT_DIR/LABEL.txt with the exit status in OUTPUT_DIR/LABEL.status. A statement that started
# but did not finish within LIMIT seconds has an answer file and no status; one that never started
# has neither. The named container is removed after the bound, so no statement completes later.
admin_cli_batch() {
    local host="$1"
    local domain="$2"
    local output_dir="$3"
    local limit="$4"
    shift 4
    mkdir -p "${output_dir}"
    local batch
    batch="${output_dir}/batch-$(epoch_ms)-${RANDOM}"
    local container_name="${project_name}-admin-${RANDOM}${RANDOM}"
    local status=0
    run_bounded "${limit}" docker compose "${compose_args[@]}" run --rm --no-deps -T \
        --name "${container_name}" admin \
        sh -c '
            host="$1"
            domain="$2"
            shift 2
            while [ "$#" -ge 2 ]; do
                label="$1"
                statement="$2"
                shift 2
                printf "=== begin %s\n" "${label}"
                nervix-cli --server "http://${host}:47391" --domain "${domain}" \
                    --password "${NERVIX_PASSWORD}" --command "${statement}" 2>&1
                code=$?
                printf "\n=== end %s %s\n" "${label}" "${code}"
            done
        ' sh "${host}" "${domain}" "$@" </dev/null >"${batch}.log" 2>"${batch}.stderr" || status=$?
    run_bounded 30 docker container rm --force "${container_name}" >/dev/null 2>&1 || true
    split_batch_transcript "${batch}.log" "${output_dir}"
    return "${status}"
}

# Splits a batch transcript into one answer file per statement and one status file per statement
# that finished.
split_batch_transcript() {
    local transcript="$1"
    local output_dir="$2"
    awk -v dir="${output_dir}" '
        /^=== begin / { label = $3; answer = dir "/" label ".txt"; printf "" >answer; next }
        /^=== end / {
            close(answer)
            status = dir "/" $3 ".status"
            print $4 >status
            close(status)
            label = ""
            next
        }
        label != "" { print >answer }
    ' "${transcript}"
}

# True when LABEL in DIR finished with status zero and its answer reports no error.
batch_read_succeeded() {
    local dir="$1"
    local label="$2"
    [[ -s "${dir}/${label}.status" && "$(<"${dir}/${label}.status")" == 0 ]] || return 1
    ! grep -Eq 'Error:|^error:' "${dir}/${label}.txt"
}

# Classifies one mutating statement of a batch. An acknowledgement is a completed command whose
# answer names its effect; a refusal is a definite "not executed"; anything else, including a
# statement interrupted by the external bound, is uncertain and is reconciled after recovery.
batch_write_outcome() {
    local dir="$1"
    local label="$2"
    local acknowledgement="$3"
    if [[ ! -e "${dir}/${label}.txt" ]]; then
        printf 'unattempted\n'
    elif [[ ! -s "${dir}/${label}.status" ]]; then
        printf 'uncertain\n'
    elif [[ "$(<"${dir}/${label}.status")" == 0 ]] \
        && grep -Fq -- "${acknowledgement}" "${dir}/${label}.txt" \
        && ! grep -Eq 'Error:|^error:|uncertain|not known yet' "${dir}/${label}.txt"; then
        printf 'acknowledged\n'
    elif grep -Eq '^topology: not-a-leader|^error: ' "${dir}/${label}.txt" \
        && ! grep -Eq 'uncertain|not known yet' "${dir}/${label}.txt"; then
        printf 'refused\n'
    else
        printf 'uncertain\n'
    fi
}

# Runs one configuration change through the packaged CLI, appends its outcome and KIND to
# OUTPUT_DIR/changes.ndjson, and prints the outcome. KIND names what a later read of the change's
# effect looks for; a control canary is a resource.
configuration_change() {
    local output_dir="$1"
    local name="$2"
    local host="$3"
    local domain="$4"
    local statement="$5"
    local acknowledgement="$6"
    local kind="${7:-resource}"
    local started_ms
    started_ms="$(epoch_ms)"
    admin_cli_batch "${host}" "${domain}" "${output_dir}/changes" 90 "${name}" "${statement}" || true
    local outcome
    outcome="$(batch_write_outcome "${output_dir}/changes" "${name}" "${acknowledgement}")"
    jq -nc \
        --arg name "${name}" \
        --arg host "${host}" \
        --arg domain "${domain}" \
        --arg statement "${statement}" \
        --arg outcome "${outcome}" \
        --arg kind "${kind}" \
        --argjson elapsed_ms "$(( $(epoch_ms) - started_ms ))" \
        '{name:$name,entry_host:$host,domain:$domain,statement:$statement,outcome:$outcome,kind:$kind,elapsed_ms:$elapsed_ms}' \
        >>"${output_dir}/changes.ndjson"
    printf '%s\n' "${outcome}"
}

node_metrics() {
    local host="$1"
    local output="$2"
    compose run --rm --no-deps -T probe wget -q -T 5 -O - "http://${host}:9090/metrics" </dev/null >"${output}"
}

# Prints the value of the first series of FAMILY in FILE whose labels contain every LABEL argument.
metric_value() {
    local file="$1"
    local family="$2"
    shift 2
    awk -v family="${family}" -v required="$*" '
        BEGIN { count = split(required, labels, " ") }
        $1 == family || index($1, family "{") == 1 {
            for (i = 1; i <= count; i++) {
                if (index($1, labels[i]) == 0) { next }
            }
            print $NF
            exit
        }
    ' "${file}"
}

# Sums every series of FAMILY in FILE, or prints 0 when it has none.
metric_sum() {
    local file="$1"
    local family="$2"
    awk -v family="${family}" '
        $1 == family || index($1, family "{") == 1 { total += $NF }
        END { printf "%d\n", total }
    ' "${file}"
}

# Writes HOST's consensus log positions and the snapshot transfers it has had answered as JSON.
consensus_record() {
    local host="$1"
    local metrics="$2"
    local output="$3"
    node_metrics "${host}" "${metrics}" || return 1
    local last snapshot purged requests
    last="$(metric_value "${metrics}" nervix_consensus_log_last_index)"
    snapshot="$(metric_value "${metrics}" nervix_consensus_log_snapshot_index)"
    purged="$(metric_value "${metrics}" nervix_consensus_log_purged_index)"
    requests="$(metric_value "${metrics}" nervix_interconnect_requests_total \
        'operation="snapshot"' 'outcome="answered"')"
    [[ "${last}" =~ ^-?[0-9]+$ && "${snapshot}" =~ ^-?[0-9]+$ && "${purged}" =~ ^-?[0-9]+$ \
        && "${requests}" =~ ^[0-9]+$ ]] || return 1
    jq -n \
        --arg host "${host}" \
        --arg observed_at "$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)" \
        --argjson last "${last}" \
        --argjson snapshot "${snapshot}" \
        --argjson purged "${purged}" \
        --argjson requests "${requests}" \
        '{host:$host,observed_at:$observed_at,last_index:$last,snapshot_index:$snapshot,purged_index:$purged,snapshot_requests:$requests}' \
        >"${output}"
}

# Prints one JSON record of a node's own SHOW CLUSTER STATUS answer in FILE: the node that
# answered, its Raft state, leader, term and log positions, its voters, the nodes its gossip reports
# live including itself, and the owner of each scheduled ingestor, relay and emitter.
status_record() {
    local file="$1"
    local host="$2"
    local at_ns="$3"
    local node
    node="$(status_value "${file}" raft.id)"
    if [[ ! "${node}" =~ ^node-[0-9]+$ || "${node}" != "node-${host##*-}" ]]; then
        jq -nc --arg host "${host}" --argjson at_ns "${at_ns}" '{host:$host,at_ns:$at_ns,answered:false}'
        return 0
    fi
    local voters owners
    voters="$(awk '/^raft\.membership:$/ { section = 1; next }
                   section && /^- node-[0-9]+ \[voter\] / { print $2; next }
                   section { section = 0 }' "${file}")"
    owners="$(awk '/^\[schedule\]$/ { section = 1; next }
                   /^\[/ { section = 0 }
                   section && / domain=chaos_baseline / {
                       kind = ""; owner = ""
                       for (i = 1; i <= NF; i++) {
                           split($i, pair, "=")
                           if (pair[1] == "kind") kind = pair[2]
                           if (pair[1] == "owner") owner = pair[2]
                       }
                       if (kind == "ingestor" || kind == "relay" || kind == "emitter") print kind " " owner
                   }' "${file}")"
    jq -nc \
        --arg host "${host}" \
        --argjson at_ns "${at_ns}" \
        --arg node "${node}" \
        --arg state "$(status_value "${file}" raft.state)" \
        --arg leader "$(status_value "${file}" raft.current_leader)" \
        --arg term "$(status_value "${file}" raft.current_term)" \
        --arg last_log "$(status_value "${file}" raft.last_log_index)" \
        --arg applied "$(status_value "${file}" raft.last_applied)" \
        --arg live "$(status_gossip_live "${file}")" \
        --arg voters "${voters}" \
        --arg owners "${owners}" '
        def lines: split("\n") | map(select(length > 0));
        {host: $host, at_ns: $at_ns, answered: true, node: $node, state: $state, leader: $leader,
         term: ($term | tonumber? // null), last_log_index: ($last_log | tonumber? // null),
         last_applied: ($applied | tonumber? // null),
         live: ([$node] + ($live | lines) | unique), voters: ($voters | lines),
         owners: ($owners | lines | map(split(" ") | {key: .[0], value: .[1]}) | from_entries)}'
}

# Reads a node's committed configuration through its own public route: its domains, voting
# membership and schedule, the SHOW CREATE text of every baseline model, and whether each named
# resource exists. Writes the answers under OUTPUT_DIR and their summary to OUTPUT_DIR.json.
capture_configuration() {
    local host="$1"
    local output_dir="$2"
    shift 2
    local resources=("$@")
    local pairs=(status 'SHOW CLUSTER STATUS;')
    local label
    for label in "${!recovery_graph_models[@]}"; do
        pairs+=("model-${label}" "${recovery_graph_models[${label}]}")
    done
    local resource
    for resource in "${resources[@]}"; do
        pairs+=("resource-${resource}" "DESCRIBE RESOURCE ${resource};")
    done
    admin_cli_batch "${host}" chaos_baseline "${output_dir}" 120 "${pairs[@]}" || return 1
    batch_read_succeeded "${output_dir}" status || return 1
    grep -Fxq "raft.id: node-${host##*-}" "${output_dir}/status.txt" || return 1
    local models_file
    models_file="$(mktemp "${output_dir}/.models.XXXXXX")"
    for label in "${!recovery_graph_models[@]}"; do
        batch_read_succeeded "${output_dir}" "model-${label}" || { rm -f "${models_file}"; return 1; }
        jq -n --arg label "${label}" --rawfile text "${output_dir}/model-${label}.txt" \
            '{key: $label, value: $text}' >>"${models_file}"
    done
    local resources_file
    resources_file="$(mktemp "${output_dir}/.resources.XXXXXX")"
    for resource in "${resources[@]}"; do
        local effect=unknown
        if batch_read_succeeded "${output_dir}" "resource-${resource}"; then
            effect=present
        elif grep -Fq "resource '${resource}' does not exist" "${output_dir}/resource-${resource}.txt"; then
            effect=absent
        fi
        jq -n --arg name "${resource}" --arg effect "${effect}" '{key: $name, value: $effect}' \
            >>"${resources_file}"
    done
    jq -n \
        --arg host "${host}" \
        --arg domains "$(awk '/^\[domains\]$/ { s = 1; next } /^\[/ { s = 0 } s && NF' "${output_dir}/status.txt")" \
        --arg membership "$(awk '/^raft\.membership:$/ { s = 1; next } s && /^- / { print; next } s { s = 0 }' "${output_dir}/status.txt")" \
        --slurpfile models "${models_file}" \
        --slurpfile resources "${resources_file}" '
        def lines: split("\n") | map(select(length > 0));
        {host: $host, domains: ($domains | lines), membership: ($membership | lines),
         models: ($models | from_entries), resources: ($resources | from_entries)}' \
        >"${output_dir}.json"
    rm -f "${models_file}" "${resources_file}"
}

# Prints the parts of committed configuration on which two captures disagree, one per line.
configuration_differences() {
    jq -r -n --slurpfile expected "$1" --slurpfile observed "$2" '
        ["domains", "membership", "models", "resources"][] as $part
        | select($expected[0][$part] != $observed[0][$part]) | $part'
}

# Prints the consensus, liveness and shutdown settings a node container runs with, read from the
# environment of its Docker inspection, as one JSON object.
configured_settings() {
    jq -c '.[0].Config.Env
        | map(select(test("^NERVIX_(RAFT_|NODE_UNAVAILABILITY_TIMEOUT=|SHUTDOWN_TIMEOUT=|DRAIN_TIMEOUT=)")))
        | map(capture("^(?<key>[^=]+)=(?<value>.*)$")) | from_entries' "$1"
}

# Records the Docker identity of every run-owned node volume to OUTPUT: its name, creation time and
# mount point. A volume recreated under the same name would show another creation time.
record_node_volumes() {
    local output="$1"
    local volumes=()
    mapfile -t volumes < <(run_bounded 20 docker volume ls --quiet \
        --filter "label=io.nervix.chaos.run=${run_id}")
    ((${#volumes[@]} == node_count)) \
        || recovery_fail controller "expected ${node_count} run-owned node volumes, found ${#volumes[@]}"
    run_bounded 20 docker volume inspect "${volumes[@]}" \
        | jq '[.[] | {name: .Name, created_at: .CreatedAt, mountpoint: .Mountpoint}] | sort_by(.name)' \
        >"${output}"
}

# Every node volume must keep the identity it had before the fault.
node_volumes_unchanged() {
    local before="$1"
    local after="$2"
    record_node_volumes "${after}"
    jq -e --slurpfile before "${before}" '. == $before[0]' "${after}" >/dev/null \
        || recovery_fail injection 'a node volume changed identity across the restart'
}

# Records every node's metrics into DIR, so the evidence of a phase outlives later scrapes.
capture_all_metrics() {
    local dir="$1"
    mkdir -p "${dir}"
    local host
    for host in "${node_hosts[@]}"; do
        capture_metrics "${host}"
        cp "${artifact_dir}/public/metrics-${host}.txt" "${dir}/metrics-${host}.txt"
    done
}

# Writes the events of one node container from START_NS through now, read from the run's live
# recording, to OUTPUT. A recording that does not cover the window is a controller failure.
node_event_window() {
    local start_ns="$1"
    local container_id="$2"
    local output="$3"
    docker_event_window "${start_ns}" "${output}" --container "${container_id}" \
        || recovery_fail controller "the live Docker event recording does not cover ${output##*/}"
}
