#!/usr/bin/env bash
# Sourced by stateful-scenario.sh when a run inspects state (--inspect-state). Reads every state
# inspection statement family through the packaged CLI four times: at the durability milestone,
# right after the state owner is killed, once the survivors have taken its state over while it is
# still down, and after recovery. Each typed answer is kept as an observation, and
# verify-inspection-evidence.sh judges them: every report must name the unit's current owner as the
# node and process that served it, and a read of the outage must be refused with a typed reason or
# be answered by a live owner.

# The families and the statement that reads each of them. A read selects every branch under the
# serving node's default row bound, which the fixtures stay far below, so a page is complete.
inspection_families=(
    'relay|SHOW RELAY chaos_profiles MATERIALIZED STATE'
    'deduplicator|SHOW DEDUPLICATOR chaos_dedup STATE'
    'window|SHOW WINDOW PROCESSOR chaos_window STATE'
    'reorderer|SHOW REORDERER chaos_reorderer STATE'
    'correlator|SHOW CORRELATOR chaos_correlator STATE'
    'wasm|SHOW WASM PROCESSOR chaos_counter STATE'
    'inferencer|SHOW INFERENCER chaos_running STATE'
)
# The inspection graph's entities, placed with the stateful work, and its ingestor, placed with the
# stateful ingestors.
inspection_owned_entities=(
    'RELAY chaos_inspect_records' 'REORDERER chaos_reorderer' 'RELAY chaos_released'
    'RELAY chaos_settlements' 'CORRELATOR chaos_correlator' 'RELAY chaos_settled'
)
inspection_ingestors=('INGESTOR chaos_inspect_ingestor')
# External bounds and the pause between reads of a family that is still refused, in seconds.
inspection_held_bound=120
inspection_failover_bound=120
inspection_retry_seconds=2

# Prints the statement that reads FAMILY, or fails for a family the run does not read.
inspection_statement() {
    local family="$1"
    local entry
    for entry in "${inspection_families[@]}"; do
        if [[ "${entry%%|*}" == "${family}" ]]; then
            printf '%s\n' "${entry#*|}"
            return 0
        fi
    done
    printf 'unknown inspection family %s\n' "${family}" >&2
    return 1
}

# Reads FAMILY through HOST with optional CLAUSES and writes the normalized observation, labeled
# READ_PHASE, to OUTPUT.json. The transcript stays in OUTPUT.txt and the answer in
# OUTPUT.document.json. A refusal is an observation like a page; an answer the normalizer cannot
# read ends the controller, because it is neither product state nor a product refusal.
inspection_read() {
    local family="$1"
    local host="$2"
    local output="$3"
    local read_phase="$4"
    local clauses="${5:-}"
    local text
    if ! text="$(inspection_statement "${family}")"; then
        failure_category=controller
        exit 1
    fi
    if [[ -n "${clauses}" ]]; then
        text+=" ${clauses}"
    fi
    text+=" FORMAT JSON;"
    rm -f "${output}.txt" "${output}.document.json" "${output}.json" "${output}.reader-error.txt"
    local previous_host="${cli_host}"
    local status=0
    local started finished
    started="$(epoch_ms)"
    cli_host="${host}"
    domain_cli_command "${text}" >"${output}.txt" 2>&1 || status=$?
    cli_host="${previous_host}"
    finished="$(epoch_ms)"
    # The administration container's progress lines precede the document and diagnostics may
    # follow it, so the answer is the first JSON document of the transcript.
    sed -n '/^{/,$p' "${output}.txt" | jq -c -n 'input' >"${output}.document.json" 2>/dev/null \
        || : >"${output}.document.json"
    if ! jq -n --slurpfile document "${output}.document.json" \
        --arg family "${family}" --arg phase "${read_phase}" --arg host "${host}" --arg statement "${text}" \
        --arg started "${started}" --arg finished "${finished}" --arg status "${status}" \
        -f "${script_dir}/state-inspection-observation.jq" \
        >"${output}.json.next" 2>"${output}.reader-error.txt"; then
        rm -f "${output}.json.next"
        failure_category=controller
        printf 'state inspection answer could not be read: %s\n' "${output}.reader-error.txt" >&2
        exit 1
    fi
    rm -f "${output}.reader-error.txt"
    mv -f "${output}.json.next" "${output}.json"
}

# Reads every family once through HOST into DIR/<family>.json, labeled READ_PHASE.
inspection_round() {
    local read_phase="$1"
    local host="$2"
    local dir="$3"
    mkdir -p "${dir}"
    local entry
    for entry in "${inspection_families[@]}"; do
        inspection_read "${entry%%|*}" "${host}" "${dir}/${entry%%|*}" "${read_phase}"
    done
}

# Writes the incarnation of every live node, as DESCRIBE CLUSTER answers through HOST, to
# OUTPUT.json, and is true when it names every node in the remaining arguments.
inspection_cluster_incarnations() {
    local host="$1"
    local output="$2"
    shift 2
    local previous_host="${cli_host}"
    local status=0
    cli_host="${host}"
    cli_command 'DESCRIBE CLUSTER;' >"${output}.txt" 2>&1 || status=$?
    cli_host="${previous_host}"
    ((status == 0)) || return 1
    jq -Rn '
        [inputs] as $lines
        | [range(0; $lines | length) as $at
           | ($lines[$at] | capture("^node: (?<node>node-[0-9]+)( \\(leader\\))?$")) as $node
           | (($lines[$at + 1] // "") | capture("^  incarnation: (?<value>[0-9]+)$")) as $incarnation
           | {key: $node.node, value: ($incarnation.value | tonumber)}]
        | from_entries' "${output}.txt" >"${output}.json" || return 1
    local node
    for node in "$@"; do
        jq -e --arg node "${node}" 'has($node)' "${output}.json" >/dev/null || return 1
    done
}

# Produces the inspection graph's BATCH, as fixtures/generate-inspection.jq writes it, to
# chaos_inspect_input, and keeps the records in OUTPUT.ndjson.
inspection_produce() {
    local batch="$1"
    local output="$2"
    jq -nc --arg run_id "${run_id}" --arg batch "${batch}" \
        -f "${script_dir}/fixtures/generate-inspection.jq" >"${output}.ndjson"
    kcat -b broker:9092 -P -t chaos_inspect_input <"${output}.ndjson" >"${output}.producer.txt" 2>&1 \
        || stateful_fail setup "the inspection producer did not accept the ${batch} batch"
}

# True when, read through HOST, the reorderer holds every record of EXPECTED, an NDJSON file, in
# its pending rows and the correlator holds each of them on its left side.
inspection_holds() {
    local expected="$1"
    local host="$2"
    local output="$3"
    inspection_read reorderer "${host}" "${output}-reorderer" holding
    inspection_read correlator "${host}" "${output}-correlator" holding
    jq -e -n --slurpfile expected "${expected}" \
        --slurpfile reorderer "${output}-reorderer.json" --slurpfile correlator "${output}-correlator.json" '
        def held($observation; $section):
            [$observation.rows[] | select(.section == $section) | {branch, sequence: .fields.sequence}] | unique;
        ($expected | map({branch: .branch_name, sequence}) | unique) as $want
        | $reorderer[0].status == "page" and $correlator[0].status == "page"
          and ($want - held($reorderer[0]; "pending") | length) == 0
          and ($want - held($correlator[0]; "left") | length) == 0
    ' >/dev/null
}

# Produces the batch the inspection graph holds through the fault. Runs before the stateful load
# starts, so the reorderer and the correlator hold it at the milestone.
inspection_startup() {
    local case_dir="$1"
    mkdir -p "${case_dir}/inspection"
    inspection_produce held "${case_dir}/inspection/held"
}

# At the durability milestone, with the stateful source drained and held: reads every family, the
# incarnations and the placement through HOST, and a first page of the deduplicator's keys whose
# continuation the run resumes after recovery.
inspection_milestone() {
    local case_dir="$1"
    local host="$2"
    local owner="$3"
    local dir="${case_dir}/inspection/milestone"
    mkdir -p "${dir}"
    phase 'stateful: inspection at the milestone'
    wait_for 'the inspection graph holding its first batch' "${inspection_held_bound}" \
        inspection_holds "${case_dir}/inspection/held.ndjson" "${host}" "${dir}/holding" \
        || stateful_fail product 'the reorderer and the correlator did not report the rows they hold'
    inspection_cluster_incarnations "${host}" "${dir}/cluster" "${node_names[@]}" \
        || stateful_fail product 'DESCRIBE CLUSTER did not name the incarnation of every node at the milestone'
    inspection_owner_incarnation="$(jq -r --arg owner "${owner}" '.[$owner]' "${dir}/cluster.json")"
    stateful_placement "${host}" "${dir}/placement" \
        || stateful_fail product 'the stateful entities could not be described at the milestone'
    inspection_round milestone "${host}" "${dir}/reads"
    inspection_read deduplicator "${host}" "${dir}/reads/deduplicator-first-page" continuation 'LIMIT 2'
}

# Starts one read of every family through HOST in the background, immediately after the state
# owner was killed and before the survivors have taken its state over.
inspection_outage_start() {
    local host="$1"
    local dir="$2"
    mkdir -p "${dir}"
    inspection_outage_jobs=()
    local entry
    for entry in "${inspection_families[@]}"; do
        inspection_read "${entry%%|*}" "${host}" "${dir}/${entry%%|*}" outage &
        inspection_outage_jobs+=("$!")
    done
}

# Waits for the outage reads and requires an observation of every family.
inspection_outage_join() {
    local dir="$1"
    local job
    for job in "${inspection_outage_jobs[@]}"; do
        wait "${job}" || true
    done
    inspection_outage_jobs=()
    local entry
    for entry in "${inspection_families[@]}"; do
        [[ -s "${dir}/${entry%%|*}.json" ]] \
            || stateful_fail controller "the outage read of ${entry%%|*} left no observation; see ${dir}/${entry%%|*}.txt"
    done
}

# Once the survivors have taken the killed owner's state over, and while it is still down: reads
# every family through HOST until it answers a page, keeping every refused attempt, then the
# incarnations and the placement the pages must name.
inspection_after_failover() {
    local case_dir="$1"
    local host="$2"
    local killed="$3"
    local dir="${case_dir}/inspection/failover"
    mkdir -p "${dir}/reads"
    local survivors=()
    local node
    for node in "${node_names[@]}"; do
        if [[ "${node}" != "${killed}" ]]; then
            survivors+=("${node}")
        fi
    done
    local deadline=$((SECONDS + inspection_failover_bound))
    local entry family attempt
    for entry in "${inspection_families[@]}"; do
        family="${entry%%|*}"
        attempt=1
        while true; do
            inspection_read "${family}" "${host}" "${dir}/reads/${family}-${attempt}" failover
            if jq -e '.status == "page"' "${dir}/reads/${family}-${attempt}.json" >/dev/null; then
                break
            fi
            ((SECONDS < deadline && SECONDS < overall_deadline)) || break
            attempt=$((attempt + 1))
            sleep "${inspection_retry_seconds}"
        done
    done
    inspection_cluster_incarnations "${host}" "${dir}/cluster" "${survivors[@]}" \
        || stateful_fail product 'DESCRIBE CLUSTER did not name the incarnation of every survivor during the outage'
    stateful_placement "${host}" "${dir}/placement" \
        || stateful_fail product 'the stateful entities could not be described during the outage'
}

# After recovery: produces a second batch, which the graph's owner after the fault must hold, then
# reads every family, the incarnations and the placement through HOST, and resumes the milestone's
# continuation, which no longer names the state being read.
inspection_after_recovery() {
    local case_dir="$1"
    local host="$2"
    local dir="${case_dir}/inspection/recovered"
    mkdir -p "${dir}"
    phase 'stateful: inspection after recovery'
    inspection_produce recovered "${case_dir}/inspection/recovered-batch"
    wait_for 'the inspection graph holding the batch produced after recovery' "${inspection_held_bound}" \
        inspection_holds "${case_dir}/inspection/recovered-batch.ndjson" "${host}" "${dir}/holding" \
        || recovery_finding "${case_dir}" 'the owner after the fault did not report the rows produced after recovery'
    inspection_cluster_incarnations "${host}" "${dir}/cluster" "${node_names[@]}" \
        || stateful_fail product 'DESCRIBE CLUSTER did not name the incarnation of every node after recovery'
    stateful_placement "${host}" "${dir}/placement" \
        || stateful_fail product 'the stateful entities could not be described after recovery'
    inspection_round recovered "${host}" "${dir}/reads"
    local continuation
    continuation="$(jq -r '.continuation // empty' "${case_dir}/inspection/milestone/reads/deduplicator-first-page.json")"
    if [[ -n "${continuation}" ]]; then
        inspection_read deduplicator "${host}" "${dir}/reads/deduplicator-stale" stale \
            "LIMIT 2 AFTER '${continuation}'"
    fi
}

# Judges the inspection evidence and records each failure as a product finding.
inspection_verify() {
    local case_dir="$1"
    local state_owner="$2"
    phase 'stateful: inspection verdict'
    local dir="${case_dir}/inspection"
    jq -n \
        --arg state_owner "${state_owner}" \
        --argjson owner_incarnation "${inspection_owner_incarnation}" \
        --argjson partitions "${stateful_partitions}" \
        --argjson profile_milestone "${stateful_profile_milestone}" \
        --slurpfile held "${dir}/held.ndjson" \
        --slurpfile recovered "${dir}/recovered-batch.ndjson" \
        '{state_owner: $state_owner, killed: $state_owner, owner_incarnation: $owner_incarnation,
          partitions: $partitions, profile_milestone: $profile_milestone,
          batches: {held: $held, recovered: $recovered}}' \
        >"${dir}/context.json"
    local status=0
    "${script_dir}/verify-inspection-evidence.sh" --evidence "${dir}" --result "${case_dir}/verdict-inspection.json" \
        >"${case_dir}/verdict-inspection.txt" 2>&1 || status=$?
    if ((status == 1)); then
        recovery_finding "${case_dir}" "state inspection: $(jq -r '.failures | join("; ")' "${case_dir}/verdict-inspection.json")"
    elif ((status != 0)); then
        stateful_fail controller 'the inspection verifier could not judge its evidence; see stateful/verdict-inspection.txt'
    fi
}
