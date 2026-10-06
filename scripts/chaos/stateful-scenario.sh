#!/usr/bin/env bash
# Sourced by run-baseline.sh after the external cluster, the baseline graph and the stateful graph
# are ready. Drives two interleaved concrete branches through a deduplicator, a window processor, a
# junction reading materialized relay state and a checkpointed WASM processor, holds the stateful
# source at an observed durability milestone, composes one fault with that state, and judges every
# processor's output against the external ledger of what the source accepted.

# shellcheck source=recovery-scenario.sh
source "${script_dir}/recovery-scenario.sh"
# shellcheck source=node-faults.sh
source "${script_dir}/node-faults.sh"

# The stateful entities, by the kind their DESCRIBE and RELOCATE statements spell. The ingestors run
# apart from the state owner, so a fault on the owner leaves their Kafka consumers in place.
stateful_owned_entities=(
    'RELAY chaos_state_records' 'DEDUPLICATOR chaos_dedup' 'RELAY chaos_unique'
    'EMITTER chaos_unique_emitter' 'WINDOW PROCESSOR chaos_window' 'RELAY chaos_windows'
    'EMITTER chaos_window_emitter' 'RELAY chaos_profiles' 'JUNCTION chaos_enrich'
    'RELAY chaos_enriched' 'EMITTER chaos_enriched_emitter' 'WASM PROCESSOR chaos_counter'
    'RELAY chaos_counted' 'EMITTER chaos_counted_emitter'
)
stateful_ingestors=('INGESTOR chaos_state_ingestor' 'INGESTOR chaos_profile_ingestor')
stateful_output_topics=(chaos_unique_output chaos_window_output chaos_enriched_output chaos_counted_output)

# The stateful fixture, its phases and the profile versions the runner publishes. The load holds
# before record 24n + 4, so at the milestone every branch holds two rows in an open 12-row window.
stateful_fixture_records=1000
stateful_milestone_records=100
stateful_profile_milestone_at=40
stateful_hold_requested_at=80
stateful_volatile_records=4
# After the milestone the run continues until every branch has seen the duplicates that refer 79
# branch positions back, and for 48 records after recovery, two windows of each branch.
stateful_records_after_milestone=184
stateful_records_after_recovery=48
stateful_profile_loaded=1
stateful_profile_milestone=2
stateful_profile_volatile=3
# The milestone holds the drained source for this many configured snapshot intervals.
stateful_milestone_publications=5
# External budgets, in seconds.
stateful_placement_bound=120
stateful_drain_bound=120
stateful_failover_bound=120
stateful_settle_bound=150
stateful_delivery_bound=120
stateful_phase_bound=600

stateful_fail() {
    recovery_fail "$@"
}

# The configured duration a fault is held for: the explicit --outage-seconds, or the fault's own
# default, which outlasts failover for a pause or a partition.
stateful_fault_seconds() {
    if [[ "${outage_option_set}" == true ]]; then
        printf '%s\n' "${outage_seconds}"
        return 0
    fi
    case "${fault}" in
        *-pause | *-partition) printf '45\n' ;;
        *) printf '%s\n' "${outage_seconds}" ;;
    esac
}

# Prints one JSON object naming the owner and replicas of every stateful entity, read through HOST.
stateful_placement() {
    local host="$1"
    local output_dir="$2"
    local pairs=()
    local entity
    for entity in "${stateful_owned_entities[@]}" "${stateful_ingestors[@]}"; do
        pairs+=("describe-${entity// /_}" "DESCRIBE ${entity};")
    done
    admin_cli_batch "${host}" chaos_baseline "${output_dir}" 90 "${pairs[@]}" || return 1
    local placements_file
    placements_file="$(mktemp "${output_dir}/.placement.XXXXXX")"
    for entity in "${stateful_owned_entities[@]}" "${stateful_ingestors[@]}"; do
        local label="describe-${entity// /_}"
        batch_read_succeeded "${output_dir}" "${label}" || { rm -f "${placements_file}"; return 1; }
        jq -n --arg entity "${entity}" \
            --arg owner "$(awk -F ': ' '$1 == "owner" { print $2; exit }' "${output_dir}/${label}.txt")" \
            --arg replicas "$(awk -F ': ' '$1 == "replicas" { print $2; exit }' "${output_dir}/${label}.txt")" \
            '{key: $entity, value: {owner: $owner, replicas: $replicas}}' >>"${placements_file}"
    done
    jq -s 'from_entries' "${placements_file}" >"${output_dir}.json"
    rm -f "${placements_file}"
}

# True when every stateful entity is owned by OWNER and both ingestors by INGEST_NODE.
stateful_placed_on() {
    local owner="$1"
    local ingest_node="$2"
    local output_dir="$3"
    stateful_placement "${node_hosts[0]}" "${output_dir}" || return 1
    jq -e --arg owner "${owner}" --arg ingest "${ingest_node}" '
        to_entries | all(.[]; if (.key | startswith("INGESTOR ")) then .value.owner == $ingest
                              else .value.owner == $owner end)
    ' "${output_dir}.json" >/dev/null
}

# True when, read through HOST, every stateful entity is owned by a node other than AWAY.
stateful_moved_off() {
    local away="$1"
    local host="$2"
    local output_dir="$3"
    stateful_placement "${host}" "${output_dir}" || return 1
    jq -e --arg away "${away}" '
        to_entries | all(.[]; (.value.owner | test("^node-[0-9]+$")) and .value.owner != $away)
    ' "${output_dir}.json" >/dev/null
}

# Publishes one profile version for both branches to the profile topic.
stateful_publish_profiles() {
    local version="$1"
    local output="$2"
    printf '{"branch_name":"alpha","profile_version":%d}\n{"branch_name":"beta","profile_version":%d}\n' \
        "${version}" "${version}" >"${output}.ndjson"
    kcat -b broker:9092 -P -t chaos_profile_input <"${output}.ndjson" >"${output}.producer.txt" 2>&1 \
        || stateful_fail setup "the profile producer did not accept version ${version}"
}

# Writes the rows `SHOW RELAY chaos_profiles MATERIALIZED STATE` reports through HOST as JSON, and
# is true when both branches hold VERSION.
stateful_profiles_at() {
    local version="$1"
    local host="$2"
    local output="$3"
    cli_host="${host}"
    domain_cli_command 'SHOW RELAY chaos_profiles MATERIALIZED STATE;' >"${output}.txt" 2>&1 || return 1
    sed -n 's/^key=\({[^}]*}\) payload=\({[^}]*}\) low=.*$/{"key":\1,"payload":\2}/p' "${output}.txt" \
        | jq -s '.' >"${output}.json" || return 1
    jq -e --argjson version "${version}" '
        ([.[] | select(.payload.branch_name == .key.branch_name
                       and .payload.profile_version == $version) | .key.branch_name] | sort)
        == ["alpha", "beta"]
    ' "${output}.json" >/dev/null
}

stateful_source_end() {
    topic_end_offset chaos_state_input
}

# True once the stateful source holds at least COUNT records.
stateful_source_reached() {
    local count="$1"
    local current
    current="$(stateful_source_end)" || return 1
    [[ "${current}" =~ ^[0-9]+$ ]] && ((current >= count))
}

# True when consumer group GROUP has committed TOPIC up to its end offset EXPECTED with no lag.
stateful_group_committed() {
    local group="$1"
    local topic="$2"
    local expected="$3"
    local output="$4"
    broker_admin /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker:9092 \
        --group "${group}" --describe >"${output}" 2>&1 || return 1
    awk -v topic="${topic}" -v expected="${expected}" '
        $2 == topic && $3 == "0" {
            found = 1
            if ($4 == expected && $5 == expected && $6 == 0) { committed = 1 }
        }
        END { exit !(found && committed) }
    ' "${output}"
}

# Prints the committed offset of consumer group GROUP on TOPIC partition 0.
stateful_group_offset() {
    local group="$1"
    local topic="$2"
    local output="$3"
    broker_admin /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker:9092 \
        --group "${group}" --describe >"${output}" 2>&1 || return 1
    awk -v topic="${topic}" '$2 == topic && $3 == "0" && $4 ~ /^[0-9]+$/ { print $4; exit }' "${output}"
}

# Prints the end offset of every stateful output topic as one JSON object.
stateful_output_offsets() {
    local topic offset
    local pairs=()
    for topic in "${stateful_output_topics[@]}"; do
        offset="$(topic_end_offset "${topic}")" || return 1
        [[ "${offset}" =~ ^[0-9]+$ ]] || return 1
        pairs+=(--argjson "${topic}" "${offset}")
    done
    jq -nc "${pairs[@]}" '$ARGS.named'
}

# True when the stateful output offsets stayed equal across two reads two seconds apart, which it
# then writes to FILE.
stateful_outputs_stable() {
    local output="$1"
    local first second
    first="$(stateful_output_offsets)" || return 1
    sleep 2
    second="$(stateful_output_offsets)" || return 1
    [[ "${first}" == "${second}" ]] || return 1
    printf '%s\n' "${second}" >"${output}"
}

# True when every branch checkpoint the WASM processor reports, read through HOST, is committed at
# its latest revision and confirmed by the replica count the deployment assigns.
stateful_checkpoints_committed() {
    local host="$1"
    local output="$2"
    cli_host="${host}"
    domain_cli_command 'DESCRIBE WASM PROCESSOR chaos_counter FORMAT JSON;' >"${output%.json}.txt" 2>&1 \
        || return 1
    # The answer is the JSON document after the administration container's own progress lines.
    sed -n '/^{/,$p' "${output%.json}.txt" >"${output}"
    jq -e --argjson replicas "${CHAOS_REPLICA_COUNT:-0}" '
        .checkpoint_counts.total == 2 and .checkpoint_counts.failed == 0
        and (.checkpoints | length) == 2
        and all(.checkpoints[]; .stage == "ReplicaConfirmed"
                and .committed_revision == .latest_revision
                and .required_replicas == $replicas and .confirmed_replicas == $replicas)
    ' "${output}" >/dev/null 2>&1
}

# The configured runtime snapshot interval of the first node, in whole milliseconds.
stateful_snapshot_interval_ms() {
    local container_id interval
    container_id="$(owned_service_container "${node_hosts[0]}")" || return 1
    interval="$(run_bounded 20 docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "${container_id}" \
        | awk -F '=' '$1 == "NERVIX_STATE_SNAPSHOT_INTERVAL" { print $2; exit }')"
    case "${interval}" in
        *ms) printf '%d\n' "${interval%ms}" ;;
        *s) printf '%d\n' "$(( ${interval%s} * 1000 ))" ;;
        *) return 1 ;;
    esac
}

stateful_load_running() {
    local load_id
    load_id="$(owned_service_container state-load)" || return 1
    container_running "${load_id}" \
        || { failure_category=setup; stateful_fail setup 'the stateful load ended before the run stopped it; increase its fixture'; }
}

# Holds the stateful source at its next hold boundary once it reached the milestone size, drains
# every stateful consumer, and records the milestone the durability verdicts cite.
stateful_milestone() {
    local case_dir="$1"
    local milestone_dir="${case_dir}/milestone"
    mkdir -p "${milestone_dir}"
    phase 'stateful: durability milestone'
    wait_for "stateful source past ${stateful_hold_requested_at} records" "${stateful_phase_bound}" \
        stateful_source_reached "${stateful_hold_requested_at}"
    touch "${artifact_dir}/traffic/hold-state-load"
    wait_for "stateful source holding at ${stateful_milestone_records} records" 60 \
        stateful_source_reached "${stateful_milestone_records}"
    local held_end=""
    local previous=""
    local stable=0
    local deadline=$((SECONDS + 30))
    while ((SECONDS < deadline)); do
        held_end="$(stateful_source_end)" || held_end=""
        if [[ "${held_end}" == "${previous}" && -n "${held_end}" ]]; then
            stable=$((stable + 1))
        else
            stable=0
        fi
        ((stable >= 3)) && break
        previous="${held_end}"
        sleep 1
    done
    [[ "${held_end}" == "${stateful_milestone_records}" ]] \
        || stateful_fail controller "the stateful load held at ${held_end:-no} records instead of ${stateful_milestone_records}"
    stateful_load_running
    wait_for 'stateful source acknowledged and committed through the milestone' "${stateful_drain_bound}" \
        stateful_group_committed chaos_stateful chaos_state_input "${held_end}" "${milestone_dir}/state-group.txt" \
        || stateful_fail product 'the stateful source was not acknowledged through the milestone'
    local profile_end
    profile_end="$(topic_end_offset chaos_profile_input)"
    wait_for 'profile source committed through its end' "${stateful_drain_bound}" \
        stateful_group_committed chaos_profiles chaos_profile_input "${profile_end}" "${milestone_dir}/profile-group.txt" \
        || stateful_fail product 'the profile source was not acknowledged through its end'
    wait_for 'stateful outputs stable' 60 stateful_outputs_stable "${milestone_dir}/outputs.json" \
        || stateful_fail product 'stateful outputs kept changing after the source drained'
    local reader="${node_hosts[0]}"
    wait_for 'WASM checkpoints committed and replica-confirmed' 60 \
        stateful_checkpoints_committed "${reader}" "${milestone_dir}/wasm-checkpoints.json" \
        || stateful_fail product 'the WASM processor did not report committed, replica-confirmed checkpoints at the milestone'
    wait_for "materialized profile version ${stateful_profile_milestone} for both branches" 60 \
        stateful_profiles_at "${stateful_profile_milestone}" "${reader}" "${milestone_dir}/profiles" \
        || stateful_fail product 'the materialized relay did not hold the milestone profile version'
    local interval_ms
    interval_ms="$(stateful_snapshot_interval_ms)" \
        || stateful_fail controller 'the configured snapshot interval could not be read from Docker inspection'
    local drained_ms hold_ms
    drained_ms="$(epoch_ms)"
    hold_ms=$((interval_ms * stateful_milestone_publications))
    sleep "$((hold_ms / 1000)).$(printf '%03d' $((hold_ms % 1000)))"
    stateful_outputs_stable "${milestone_dir}/outputs-after-hold.json" \
        || stateful_fail product 'stateful outputs changed while the milestone was held'
    cmp -s "${milestone_dir}/outputs.json" "${milestone_dir}/outputs-after-hold.json" \
        || stateful_fail product 'stateful outputs changed while the milestone was held'
    stateful_milestone_records_observed="${held_end}"
    jq -n \
        --argjson records "${held_end}" \
        --argjson interval_ms "${interval_ms}" \
        --argjson publications "${stateful_milestone_publications}" \
        --argjson drained_ms "${drained_ms}" \
        --argjson released_ms "$(epoch_ms)" \
        --slurpfile outputs "${milestone_dir}/outputs-after-hold.json" \
        --slurpfile checkpoints "${milestone_dir}/wasm-checkpoints.json" \
        --slurpfile profiles "${milestone_dir}/profiles.json" \
        '{source_records: $records,
          committed: {chaos_stateful: $records},
          outputs: $outputs[0],
          snapshot_interval_ms: $interval_ms,
          held_ms: ($released_ms - $drained_ms),
          required_hold_ms: ($interval_ms * $publications),
          wasm_checkpoints: $checkpoints[0].checkpoints,
          materialized_profiles: $profiles[0],
          evidence: ["milestone/state-group.txt", "milestone/profile-group.txt", "milestone/outputs.json",
                     "milestone/outputs-after-hold.json", "milestone/wasm-checkpoints.txt",
                     "milestone/profiles.txt"],
          not_observable: "deduplicator, window and materialized relay publications are not exposed publicly; the milestone holds the drained state for the configured publication intervals"}' \
        >"${case_dir}/milestone.json"
}

# Records the end offsets of the stateful source and outputs and the stateful group's committed
# offset at the moment the fault starts.
stateful_record_fault_start() {
    local case_dir="$1"
    stateful_fault_committed="$(stateful_group_offset chaos_stateful chaos_state_input \
        "${case_dir}/fault-state-group.txt")" || stateful_fault_committed=""
    stateful_pre_fault_outputs="$(stateful_output_offsets)" \
        || stateful_fail setup 'stateful output offsets were unavailable before the fault'
    stateful_fault_source="$(stateful_source_end)"
    [[ "${stateful_fault_source}" =~ ^[0-9]+$ ]] \
        || stateful_fail setup 'the stateful source offset was unavailable before the fault'
}

# Requires the stateful work to leave the faulted owner, read through a survivor, and the stateful
# outputs to advance there while the fault holds. Runs as the faults' DURING hook.
stateful_failover() {
    local case_dir="${stateful_case_dir}"
    local target_node="${stateful_target_node}"
    local survivor="${stateful_survivor}"
    wait_for 'survivors agree on a caught-up leader' 90 \
        survivors_settled "nervix-${target_node##*-}" "${case_dir}" \
        || stateful_fail product 'the survivors did not settle while the state owner was out'
    wait_for "stateful work moved off ${target_node}" "${stateful_failover_bound}" \
        stateful_moved_off "${target_node}" "${survivor}" "${case_dir}/placement-during" \
        || stateful_fail product "the stateful work did not fail over from ${target_node}"
    stateful_failover_ms="$(( $(epoch_ms) - node_fault_started_ms ))"
    local counted_before
    counted_before="$(topic_end_offset chaos_counted_output)"
    wait_for 'stateful outputs advanced on the survivors' "${stateful_delivery_bound}" \
        topic_progressed chaos_counted_output "${counted_before}" \
        || recovery_finding "${case_dir}" 'stateful outputs did not advance on the survivors during the fault'
}

# Runs as the faults' BEFORE hook, immediately before anything is injected.
stateful_before_fault() {
    stateful_record_fault_start "${stateful_case_dir}"
}

# Waits until the cluster has settled after the fault, the stateful work is owned and running, and
# every stateful output advances again, then records where the source stood.
stateful_recover() {
    local case_dir="$1"
    phase 'stateful: public recovery'
    failure_category=product
    mkdir -p "${case_dir}/recovered"
    wait_for 'every node caught up, connected and executing' "${stateful_settle_bound}" \
        cluster_settled_and_connected "${case_dir}/recovered" \
        || stateful_fail product "the cluster did not settle within ${stateful_settle_bound}s after the fault"
    stateful_settled_ms="$(( $(epoch_ms) - node_fault_started_ms ))"
    wait_for 'every stateful entity owned by a live node' 60 \
        stateful_moved_off none "${node_hosts[0]}" "${case_dir}/placement-recovered" \
        || stateful_fail product 'stateful entities were left without an owner after recovery'
    local before
    before="$(topic_end_offset chaos_counted_output)"
    wait_for 'stateful outputs advanced after recovery' "${stateful_delivery_bound}" \
        topic_progressed chaos_counted_output "${before}" \
        || stateful_fail product 'stateful outputs did not advance after recovery'
    stateful_recovered_source="$(stateful_source_end)"
    [[ "${stateful_recovered_source}" =~ ^[0-9]+$ ]] \
        || stateful_fail setup 'the stateful source offset was unavailable after recovery'
    stateful_recovered_ms="$(( $(epoch_ms) - node_fault_started_ms ))"
    cli_host="${node_hosts[0]}"
    cli_command 'SHOW CLUSTER STATUS;' >"${case_dir}/recovered/cluster-status.txt" 2>&1 || true
    domain_cli_command 'DESCRIBE WASM PROCESSOR chaos_counter FORMAT JSON;' \
        >"${case_dir}/recovered/wasm-checkpoints.txt" 2>&1 || true
    stateful_profiles_at "${stateful_profile_milestone}" "${node_hosts[0]}" "${case_dir}/recovered/profiles" || true
}

# Stops the stateful load once the run has seen enough records after the milestone and after
# recovery, drains every stateful consumer, and saves the accepted input and every output.
stateful_final_boundary() {
    local case_dir="$1"
    phase 'stateful: final traffic boundary'
    local target=$((stateful_milestone_records_observed + stateful_records_after_milestone))
    if ((stateful_recovered_source + stateful_records_after_recovery > target)); then
        target=$((stateful_recovered_source + stateful_records_after_recovery))
    fi
    ((target <= stateful_fixture_records)) \
        || stateful_fail setup "the stateful fixture of ${stateful_fixture_records} records cannot reach ${target} records"
    stateful_load_running
    wait_for "stateful source reached ${target} records" "${stateful_phase_bound}" \
        stateful_source_reached "${target}" \
        || stateful_fail setup 'the stateful load did not reach its final boundary'
    touch "${artifact_dir}/traffic/stop-state-load"
    local load_id
    load_id="$(owned_service_container state-load)" || return 1
    wait_for 'stateful load flushed and exited' 40 load_exited_cleanly "${load_id}"
    run_bounded 20 docker logs "${load_id}" >"${artifact_dir}/traffic/state-producer.log" 2>&1
    local state_end
    state_end="$(stateful_source_end)"
    [[ "${state_end}" =~ ^[0-9]+$ && "${state_end}" -le "${stateful_fixture_records}" ]] \
        || stateful_fail product 'the stateful source boundary exceeded the bounded fixture'
    kcat -q -b broker:9092 -C -t chaos_state_input -p 0 -o beginning -c "${state_end}" \
        >"${artifact_dir}/traffic/state-accepted-input.ndjson" 2>"${case_dir}/state-source.stderr"
    [[ "$(wc -l <"${artifact_dir}/traffic/state-accepted-input.ndjson")" -eq "${state_end}" ]] \
        || stateful_fail product 'the stateful accepted-input ledger did not cover the source boundary'
    wait_for "stateful source acknowledged through ${state_end}" "${stateful_drain_bound}" \
        stateful_group_committed chaos_stateful chaos_state_input "${state_end}" "${case_dir}/final-state-group.txt" \
        || recovery_finding "${case_dir}" "the stateful source was not acknowledged through its final boundary ${state_end}"
    wait_for 'stateful outputs stable' 90 stateful_outputs_stable "${case_dir}/final-outputs.json" \
        || stateful_fail product 'stateful outputs did not settle after the source drained'
    local topic count
    for topic in "${stateful_output_topics[@]}"; do
        count="$(jq -r --arg topic "${topic}" '.[$topic]' "${case_dir}/final-outputs.json")"
        if ((count > 0)); then
            kcat -q -b broker:9092 -C -t "${topic}" -p 0 -o beginning -c "${count}" \
                >"${artifact_dir}/traffic/${topic}.ndjson" 2>"${case_dir}/${topic}.stderr"
        else
            : >"${artifact_dir}/traffic/${topic}.ndjson"
        fi
        [[ "$(wc -l <"${artifact_dir}/traffic/${topic}.ndjson")" -eq "${count}" ]] \
            || stateful_fail product "the ${topic} ledger did not cover its final boundary"
    done
    stateful_final_source="${state_end}"
}

# Runs every processor verdict and records each failure as a product finding.
stateful_verify() {
    local case_dir="$1"
    phase 'stateful: processor verdicts'
    jq -n \
        --arg fault "${fault}" \
        --argjson milestone "${stateful_milestone_records_observed}" \
        --argjson recovered "${stateful_recovered_source}" \
        --argjson pre "${stateful_pre_fault_outputs}" \
        --argjson loaded "${stateful_profile_loaded}" \
        --argjson milestone_version "${stateful_profile_milestone}" \
        --argjson volatile_version "${stateful_profile_volatile}" \
        '{fault:$fault,milestone_records:$milestone,recovered_records:$recovered,pre_fault_outputs:$pre,
          profiles:{loaded:$loaded,milestone:$milestone_version,volatile:$volatile_version}}' \
        >"${case_dir}/boundaries.json"
    local mode topic
    for mode in dedup window enrich counter; do
        case "${mode}" in
            dedup) topic=chaos_unique_output ;;
            window) topic=chaos_window_output ;;
            enrich) topic=chaos_enriched_output ;;
            counter) topic=chaos_counted_output ;;
        esac
        local status=0
        "${script_dir}/verify-state-evidence.sh" "${mode}" \
            --input "${artifact_dir}/traffic/state-accepted-input.ndjson" \
            --output "${artifact_dir}/traffic/${topic}.ndjson" \
            --boundaries "${case_dir}/boundaries.json" \
            --result "${case_dir}/verdict-${mode}.json" \
            >"${case_dir}/verdict-${mode}.txt" 2>&1 || status=$?
        if ((status == 1)); then
            recovery_finding "${case_dir}" "$(jq -r '.processor + ": " + (.failures | join("; "))' "${case_dir}/verdict-${mode}.json")"
        elif ((status != 0)); then
            stateful_fail controller "the ${mode} verifier could not judge its evidence; see stateful/verdict-${mode}.txt"
        fi
    done
}

run_stateful() {
    local case_dir="${artifact_dir}/stateful"
    mkdir -p "${case_dir}"
    recovery_traffic_startup "${case_dir}"
    stateful_failover_ms=null

    failure_category=product
    phase 'stateful: placement'
    local state_owner="${node_names[0]}"
    local ingest_node="${node_names[0]}"
    if [[ "${node_count}" == 3 ]]; then
        wait_for 'settled cluster before stateful placement' 120 cluster_settled "${case_dir}"
        local leader
        leader="$(<"${case_dir}/leader.txt")"
        local node
        for node in node-3 node-2 node-1; do
            if [[ "${node}" != "${leader}" ]]; then
                state_owner="${node}"
                break
            fi
        done
        for node in node-1 node-2 node-3; do
            if [[ "${node}" != "${state_owner}" ]]; then
                ingest_node="${node}"
                break
            fi
        done
        local selection
        selection="$(printf '%s, ' "${stateful_owned_entities[@]}")"
        domain_cli_command "RELOCATE ${selection%, } ONTO NODE ${state_owner} IGNORE PREFERENCES;" \
            >"${case_dir}/relocate-state.txt" 2>&1
        selection="$(printf '%s, ' "${stateful_ingestors[@]}")"
        domain_cli_command "RELOCATE ${selection%, } ONTO NODE ${ingest_node} IGNORE PREFERENCES;" \
            >"${case_dir}/relocate-ingestors.txt" 2>&1
    fi
    wait_for "stateful work on ${state_owner}, ingestion on ${ingest_node}" "${stateful_placement_bound}" \
        stateful_placed_on "${state_owner}" "${ingest_node}" "${case_dir}/placement-before"

    phase 'stateful: traffic startup'
    stateful_publish_profiles "${stateful_profile_loaded}" "${case_dir}/profiles-loaded"
    wait_for "materialized profile version ${stateful_profile_loaded} for both branches" 90 \
        stateful_profiles_at "${stateful_profile_loaded}" "${node_hosts[0]}" "${case_dir}/profiles-loaded" \
        || stateful_fail product 'the materialized relay never held the loaded profiles'
    rm -f "${artifact_dir}/traffic/hold-state-load" "${artifact_dir}/traffic/stop-state-load"
    jq -nc --arg run_id "${run_id}" --argjson count "${stateful_fixture_records}" \
        -f "${script_dir}/fixtures/generate-stateful.jq" >"${CHAOS_STATE_LOAD_FILE}"
    [[ "$(wc -l <"${CHAOS_STATE_LOAD_FILE}")" -eq "${stateful_fixture_records}" ]] \
        || stateful_fail controller 'the stateful fixture generation was incomplete'
    compose up --detach --no-deps state-load
    wait_for 'stateful source started' 30 topic_progressed chaos_state_input 0
    wait_for 'stateful outputs started' 90 topic_progressed chaos_counted_output 0 \
        || stateful_fail product 'the stateful graph produced no output'
    wait_for "stateful source past ${stateful_profile_milestone_at} records" "${stateful_phase_bound}" \
        stateful_source_reached "${stateful_profile_milestone_at}"
    stateful_publish_profiles "${stateful_profile_milestone}" "${case_dir}/profiles-milestone"

    stateful_milestone "${case_dir}"

    phase 'stateful: volatile interval'
    rm -f "${artifact_dir}/traffic/hold-state-load"
    stateful_publish_profiles "${stateful_profile_volatile}" "${case_dir}/profiles-volatile"
    wait_for "stateful source ${stateful_volatile_records} records past the milestone" 60 \
        stateful_source_reached "$((stateful_milestone_records_observed + stateful_volatile_records))"
    check_support_containers
    stateful_load_running

    local fault_seconds
    fault_seconds="$(stateful_fault_seconds)"
    stateful_case_dir="${case_dir}"
    stateful_target_node="${state_owner}"
    stateful_survivor="${node_hosts[0]}"
    local host
    for host in "${node_hosts[@]}"; do
        if [[ "${host}" != "nervix-${state_owner##*-}" ]]; then
            stateful_survivor="${host}"
            break
        fi
    done
    if [[ "${fault}" != none && "${fault}" != cluster-restart ]]; then
        wait_for "stateful work still on ${state_owner} immediately before the fault" 30 \
            stateful_placed_on "${state_owner}" "${ingest_node}" "${case_dir}/placement-immediately-before" \
            || stateful_fail injection "the stateful work moved off ${state_owner} before the fault"
    fi
    local owner_host="nervix-${state_owner##*-}"
    case "${fault}" in
        none)
            phase 'stateful: no fault'
            node_fault_begin stateful_before_fault
            ;;
        owner-crash)
            phase "stateful: SIGKILL of state owner ${owner_host}"
            node_fault_kill "${case_dir}" "${owner_host}" "${fault_seconds}" stateful_before_fault stateful_failover
            ;;
        owner-pause)
            phase "stateful: ${fault_seconds}s pause of state owner ${owner_host}"
            node_fault_pause "${case_dir}" "${owner_host}" "${fault_seconds}" stateful_before_fault stateful_failover
            ;;
        owner-partition)
            phase "stateful: isolation of state owner ${owner_host}"
            node_fault_isolate "${case_dir}" "${owner_host}" "${fault_seconds}" stateful_before_fault stateful_failover
            ;;
        cluster-restart)
            phase "stateful: SIGKILL of every node (${node_count})"
            node_fault_restart_cluster "${case_dir}" "${fault_seconds}" stateful_before_fault
            ;;
    esac
    if [[ "${fault}" == none ]]; then
        stateful_recovered_source="${stateful_fault_source}"
        stateful_recovered_ms=0
        stateful_settled_ms=0
    else
        stateful_recover "${case_dir}"
        node_fault_check_events "${case_dir}"
    fi
    check_support_containers

    stateful_final_boundary "${case_dir}"
    stateful_verify "${case_dir}"
    capture_all_metrics "${case_dir}"
    jq -n \
        --arg fault "${fault}" \
        --arg state_owner "${state_owner}" \
        --arg ingest_node "${ingest_node}" \
        --argjson nodes "${node_count}" \
        --argjson fault_seconds "${fault_seconds}" \
        --argjson replicas "${CHAOS_REPLICA_COUNT:-0}" \
        --arg snapshot_interval "${CHAOS_STATE_SNAPSHOT_INTERVAL:-30s}" \
        --argjson milestone_records "${stateful_milestone_records_observed}" \
        --argjson fault_records "${stateful_fault_source}" \
        --arg fault_committed "${stateful_fault_committed}" \
        --argjson recovered_records "${stateful_recovered_source}" \
        --argjson final_records "${stateful_final_source}" \
        --argjson failover_ms "${stateful_failover_ms}" \
        --argjson settled_ms "${stateful_settled_ms}" \
        --argjson recovered_ms "${stateful_recovered_ms}" \
        --argjson pre "${stateful_pre_fault_outputs}" \
        --slurpfile milestone "${case_dir}/milestone.json" \
        --slurpfile manifest "${artifact_dir}/manifest.json" \
        --slurpfile dedup "${case_dir}/verdict-dedup.json" \
        --slurpfile window "${case_dir}/verdict-window.json" \
        --slurpfile enrich "${case_dir}/verdict-enrich.json" \
        --slurpfile counter "${case_dir}/verdict-counter.json" \
        '{fault: $fault, topology_nodes: $nodes, fault_seconds: (if $fault == "none" then null else $fault_seconds end),
          deployment: {replica_count: $replicas, state_snapshot_interval: $snapshot_interval,
                       state_owner: $state_owner, ingestion_node: $ingest_node},
          wasm_fixture: $manifest[0].fixtures.wasm,
          branches: ["alpha", "beta"],
          boundaries: {milestone_records: $milestone_records, fault_records: $fault_records,
                       committed_at_fault: ($fault_committed | tonumber? // null),
                       recovered_records: $recovered_records, final_records: $final_records,
                       pre_fault_outputs: $pre},
          timings_ms: {failover: $failover_ms, settled: $settled_ms, recovered: $recovered_ms},
          milestone: $milestone[0],
          verdicts: {deduplicator: ($dedup[0] | {verdict, checks, replay_duplicates, volatile_reemissions, durability, failures}),
                     window: ($window[0] | {verdict, checks, volatile_losses, replay_duplicates, durability, failures}),
                     materialized_relay: ($enrich[0] | {verdict, checks, versions_after_recovery, replay_duplicates, durability, failures}),
                     wasm_processor: ($counter[0] | {verdict, checks, replay_excess_after_recovery, replay_duplicates, durability, failures})},
          evidence_class: (if $fault == "none" then "healthy baseline" else "process and network faults on one host; host power loss and storage corruption are not established" end),
          qualification_limit: $milestone[0].not_observable}' \
        >"${artifact_dir}/results/stateful-progress.json"

    recovery_final_boundary
}
