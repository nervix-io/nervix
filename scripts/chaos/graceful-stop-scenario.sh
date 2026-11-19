#!/usr/bin/env bash
# Sourced by run-baseline.sh after the external cluster and the baseline graph are ready. Owns the
# graceful-stop scenario: a domain STOP that arrives while records are in motion at every stage of
# the domain, followed by a START, judged by a strict verdict. The baseline path keeps its own
# traffic and exact ledger in its domain.
#
# The domain chaos_stop installs fixtures/graceful-stop.plexus. An acknowledged Kafka source and a
# source without acknowledgements each receive a burst load: a backlog of one payload holding every
# lane, produced before START and read at once. Every record names its lane, and each lane rests in
# one buffer while the faster lanes pass:
#
#   deliver    passes every stage and is delivered before the stop
#   ingestor   rests in each ingestor's route buffer, on node-1
#   collector  rests in the input collector of the junction chaos_stop_collector, on node-3
#   junction   rests in the route buffer of the junction chaos_stop_forward, on node-3
#   emitter    rests in the buffer of the emitter chaos_stop_holder, on node-2
#   publish    a second burst once the destination is slow
#
# The acknowledged source confirms its backlog only once every lane of it is delivered, so its
# delivered lane stays unconfirmed while the others rest, and it reads its next payload only after
# that: its publish burst waits in Kafka through the stop, and START reads it. The source without
# acknowledgements confirms what it reads at once, so its publish burst goes out and is on the wire
# at the stop.
#
# Records cross two relays between nodes: node-1 to node-3 and node-3 to node-2. The resting
# cadences are three minutes, longer than the scenario needs from START to the stop, so every
# resting lane is still in its buffer when the stop arrives; a stop that drains flushes them.
#
# The slow destination is an acknowledgement gate: Pumba netem on the broker drops every packet the
# broker sends to the emitters' node, so the broker appends the publish while the emitter waits on
# the wire for its answer. The gate holds for a few seconds after the STOP request, then heals, so
# the destination comes back and the publish can run to its outcome.
#
# The graph keeps out every kind a stop still cuts by design: no reorderer, correlator, window or
# REQUIRED WAIT, a branch with no instance limit, relays with room to spare, and a destination that
# comes back. The verdict is therefore strict (verify-graceful-stop.sh over
# verify-strict-delivery.sh): every record delivered exactly once, no duplicate of any kind, no
# missing record, no increase of a negative-outcome counter, no error event, and a STOP answer that
# acknowledges the stop and reports no cut. A failing verdict names its symptoms in
# results/symptoms.json, which an expected failure (--expect-failure) matches.

# shellcheck source=recovery-scenario.sh
source "${script_dir}/recovery-scenario.sh"
# shellcheck source=node-faults.sh
source "${script_dir}/node-faults.sh"
# shellcheck source=burst-load.sh
source "${script_dir}/burst-load.sh"
# shellcheck source=events-observer.sh
source "${script_dir}/events-observer.sh"

graceful_stop_domain=chaos_stop
graceful_stop_output_topic=chaos_stop_output
graceful_stop_ingestors=(chaos_stop_acked chaos_stop_unacked)
graceful_stop_emitters=(chaos_stop_publisher chaos_stop_holder)
graceful_stop_relays=(chaos_stop_incoming chaos_stop_held chaos_stop_collected chaos_stop_outgoing chaos_stop_parked)
graceful_stop_junctions=(chaos_stop_collector chaos_stop_forward)
# Where each part runs: intake on node-1, the relays and junctions on node-3, and the emitters on
# node-2, the one node no other Kafka client of the run uses, so the gate slows nothing else.
graceful_stop_intake_node='node-1'
graceful_stop_processing_node='node-3'
graceful_stop_emitter_node='node-2'
# The records of each lane, per source. The backlog is one payload holding these lanes in this order,
# and the publish lane follows as a second burst of one payload.
graceful_stop_backlog_lanes='[{"lane":"deliver","records":8},{"lane":"ingestor","records":6},{"lane":"collector","records":6},{"lane":"junction","records":6},{"lane":"emitter","records":6}]'
graceful_stop_publish_lanes='[{"lane":"publish","records":8}]'
# The fixture's resting cadences (FLUSH EACH and COLLECT FOR), in seconds. The stop must be requested
# well inside them after START, or a buffer would flush on its own before the stop arrives.
graceful_stop_rest_seconds=180
graceful_stop_stop_deadline=120
# The gate holds this long after the STOP request, then heals. By then the publish has waited on
# the wire for less than 20 seconds, and the broker's answer arrives with its next TCP
# retransmission, whose backoff doubles, so within twice that wait. A driver timeout that passed
# first would only retry the publish, which the idempotent producer never writes twice, and the
# CLI's 120-second timeout is never near.
graceful_stop_hold_seconds=4
# The longest the gate may live if the controller never heals it.
graceful_stop_gate_lifetime=180
# External bounds, in seconds. On an image whose stop cuts, the acknowledged source sends its cut
# records again after START and they rest once more for the resting cadence before they arrive.
graceful_stop_rest_bound=60
graceful_stop_held_bound=45
graceful_stop_answer_bound=300
graceful_stop_settle_bound=$((graceful_stop_rest_seconds + 120))

graceful_stop_fail() {
    recovery_fail "$@"
}

graceful_stop_gate_name() {
    printf '%s-acknowledgement-gate\n' "${run_id}"
}

graceful_stop_cli() {
    run_cli "${graceful_stop_domain}" "$1"
}

# Every entity of the graph, as DESCRIBE names its kind.
graceful_stop_described_entities() {
    local name
    for name in "${graceful_stop_ingestors[@]}"; do printf 'INGESTOR %s\n' "${name}"; done
    for name in "${graceful_stop_relays[@]}"; do printf 'RELAY %s\n' "${name}"; done
    for name in "${graceful_stop_junctions[@]}"; do printf 'JUNCTION %s\n' "${name}"; done
    for name in "${graceful_stop_emitters[@]}"; do printf 'EMITTER %s\n' "${name}"; done
}

# The node every entity must run on.
graceful_stop_planned_owner() {
    case "$1" in
        INGESTOR\ *) printf '%s\n' "${graceful_stop_intake_node}" ;;
        EMITTER\ *) printf '%s\n' "${graceful_stop_emitter_node}" ;;
        *) printf '%s\n' "${graceful_stop_processing_node}" ;;
    esac
}

# True when every entity reports its planned owner through DESCRIBE. Writes OUTPUT_DIR.json.
graceful_stop_placed() {
    local output_dir="$1"
    mkdir -p "${output_dir}"
    local placements="[]"
    local entity
    while IFS= read -r entity; do
        local file="${output_dir}/describe-${entity// /_}.txt"
        graceful_stop_cli "DESCRIBE ${entity};" >"${file}" 2>&1 || return 1
        local owner planned
        owner="$(awk -F ': ' '$1 == "owner" { print $2; exit }' "${file}")"
        planned="$(graceful_stop_planned_owner "${entity}")"
        placements="$(jq -c --arg entity "${entity}" --arg owner "${owner}" --arg planned "${planned}" \
            '. + [{entity: $entity, owner: $owner, planned: $planned}]' <<<"${placements}")"
    done < <(graceful_stop_described_entities)
    jq '.' <<<"${placements}" >"${output_dir}.json"
    jq -e 'all(.[]; .owner == .planned)' "${output_dir}.json" >/dev/null
}

# Places the graph before START: intake, processing and the emitters on three different nodes.
graceful_stop_place() {
    local case_dir="$1"
    local intake=() processing=() emitters=()
    local entity
    while IFS= read -r entity; do
        case "$(graceful_stop_planned_owner "${entity}")" in
            "${graceful_stop_intake_node}") intake+=("${entity}") ;;
            "${graceful_stop_emitter_node}") emitters+=("${entity}") ;;
            *) processing+=("${entity}") ;;
        esac
    done < <(graceful_stop_described_entities)
    local selection
    selection="$(printf '%s, ' "${intake[@]}")"
    graceful_stop_cli "RELOCATE ${selection%, } ONTO NODE ${graceful_stop_intake_node} IGNORE PREFERENCES;" \
        >"${case_dir}/relocate-intake.txt" 2>&1 \
        || graceful_stop_fail product "the intake could not be placed on ${graceful_stop_intake_node}; see graceful-stop/relocate-intake.txt"
    selection="$(printf '%s, ' "${processing[@]}")"
    graceful_stop_cli "RELOCATE ${selection%, } ONTO NODE ${graceful_stop_processing_node} IGNORE PREFERENCES;" \
        >"${case_dir}/relocate-processing.txt" 2>&1 \
        || graceful_stop_fail product "the relays and junctions could not be placed on ${graceful_stop_processing_node}; see graceful-stop/relocate-processing.txt"
    selection="$(printf '%s, ' "${emitters[@]}")"
    graceful_stop_cli "RELOCATE ${selection%, } ONTO NODE ${graceful_stop_emitter_node} IGNORE PREFERENCES;" \
        >"${case_dir}/relocate-emitters.txt" 2>&1 \
        || graceful_stop_fail product "the emitters could not be placed on ${graceful_stop_emitter_node}; see graceful-stop/relocate-emitters.txt"
    wait_for 'the graceful-stop graph on its planned nodes' 60 graceful_stop_placed "${case_dir}/placement" \
        || graceful_stop_fail product 'the graceful-stop graph did not report its planned owners; see graceful-stop/placement.json'
}

# Sums the series of FAMILY in every metrics-*.txt of DIR whose labels hold every LABEL pair
# exactly, or prints absent when no series matches.
graceful_stop_series_total() {
    local dir="$1"
    local family="$2"
    shift 2
    awk -v family="${family}" -v required="$*" '
        BEGIN { count = split(required, labels, " ") }
        /^#/ || NF < 2 { next }
        {
            brace = index($1, "{")
            if (brace == 0 || substr($1, 1, brace - 1) != family) { next }
            series = substr($1, brace)
            for (i = 1; i <= count; i++) {
                if (series !~ ("[{,]" labels[i] "[,}]")) { next }
            }
            total += $2
            found += 1
        }
        END { if (found == 0) { print "absent" } else { printf "%d\n", total } }
    ' "${dir}"/metrics-*.txt
}

# Records where the records of the graph are, from every node's public metrics, into DIR and
# DIR/stages.json: what each ingestor read and sent to each relay, what each junction received and
# sent, what the holding emitter buffers, and what the publishing emitter delivered and holds on
# the wire. An absent series reads as null.
graceful_stop_record_stages() {
    local dir="$1"
    capture_all_metrics "${dir}" || return 1
    local domain="domain=\"${graceful_stop_domain}\""
    local readings=()
    local source ingestor
    for source in acked unacked; do
        ingestor="chaos_stop_${source}"
        readings+=("${source}_read nervix_messages_total direction=\"received\" target=\"${ingestor}\" target_kind=\"INGESTOR\"")
        local relay
        for relay in incoming held collected; do
            readings+=("${source}_to_${relay} nervix_messages_total direction=\"sent\" target=\"${ingestor}\" relay=\"chaos_stop_${relay}\"")
        done
    done
    readings+=(
        "collector_received nervix_messages_total direction=\"received\" target=\"chaos_stop_collector\" relay=\"chaos_stop_collected\""
        "collector_sent nervix_messages_total direction=\"sent\" target=\"chaos_stop_collector\" relay=\"chaos_stop_outgoing\""
        "forward_received_incoming nervix_messages_total direction=\"received\" target=\"chaos_stop_forward\" relay=\"chaos_stop_incoming\""
        "forward_received_held nervix_messages_total direction=\"received\" target=\"chaos_stop_forward\" relay=\"chaos_stop_held\""
        "forward_sent_outgoing nervix_messages_total direction=\"sent\" target=\"chaos_stop_forward\" relay=\"chaos_stop_outgoing\""
        "forward_sent_parked nervix_messages_total direction=\"sent\" target=\"chaos_stop_forward\" relay=\"chaos_stop_parked\""
        "holder_buffered nervix_emitter_work_messages emitter=\"chaos_stop_holder\" state=\"buffered\""
        "publisher_delivered nervix_messages_total direction=\"sent\" target=\"chaos_stop_publisher\" relay=\"-\""
        "publisher_in_flight nervix_emitter_work_messages emitter=\"chaos_stop_publisher\" state=\"in_flight\""
    )
    local stages="{}"
    local reading
    for reading in "${readings[@]}"; do
        local key family labels value
        read -r key family labels <<<"${reading}"
        # The label pairs hold no spaces, so they split back into their words.
        # shellcheck disable=SC2086
        value="$(graceful_stop_series_total "${dir}" "${family}" "${domain}" ${labels})" || return 1
        stages="$(jq -c --arg key "${key}" --arg value "${value}" \
            '.[$key] = (if $value == "absent" then null else ($value | tonumber) end)' <<<"${stages}")"
    done
    jq '.' <<<"${stages}" >"${dir}/stages.json"
}

# The counts STAGES must show, from the lane sizes: before the publish burst with PUBLISH false, and
# while the gate holds it with PUBLISH true. Only the source without acknowledgements reads its
# publish burst before the stop: the acknowledged source reads its next payload only once its backlog
# is confirmed, which its resting lanes hold back. The publishing emitter's in-flight count is judged
# apart.
graceful_stop_expected_stages() {
    local publish="$1"
    jq -n --argjson backlog "${graceful_stop_backlog_lanes}" --argjson publish_lanes "${graceful_stop_publish_lanes}" \
        --argjson with_publish "${publish}" '
        def records($lane): [($backlog + $publish_lanes)[] | select(.lane == $lane) | .records] | add // 0;
        (records("deliver")) as $d | (records("ingestor")) as $i | (records("collector")) as $c
        | (records("junction")) as $j | (records("emitter")) as $e
        | (if $with_publish then records("publish") else 0 end) as $p
        | {acked_read: ($d + $i + $c + $j + $e), acked_to_incoming: ($d + $j + $e),
           acked_to_held: 0, acked_to_collected: $c,
           unacked_read: ($d + $i + $c + $j + $e + $p), unacked_to_incoming: ($d + $j + $e + $p),
           unacked_to_held: 0, unacked_to_collected: $c,
           collector_received: (2 * $c), collector_sent: 0,
           forward_received_incoming: (2 * ($d + $j + $e) + $p), forward_received_held: 0,
           forward_sent_outgoing: (2 * ($d + $e) + $p), forward_sent_parked: 0,
           holder_buffered: (2 * $e), publisher_delivered: (2 * $d)}'
}

# The committed offset of consumer GROUP on partition 0 of TOPIC, or -1 while it has none.
graceful_stop_committed_offset() {
    local group="$1"
    local topic="$2"
    local output
    output="$(broker_offset_query /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server broker:9092 --group "${group}" --describe 2>/dev/null)" || return 1
    awk -v topic="${topic}" '
        $2 == topic && $3 == "0" { found = 1; offset = ($4 == "-") ? -1 : $4 }
        END { if (!found) { exit 1 } print offset }
    ' <<<"${output}"
}

# Whether a stage matches its expected count, as a jq definition. The series a route opens with its
# first flush is absent until then, which reads as zero only where zero is expected.
# The dollars in this jq filter are jq variables, not shell expansion.
# shellcheck disable=SC2016
graceful_stop_matches_jq='def graceful_stop_matches($s): (if .value == 0 then ($s[.key] // 0) else $s[.key] end) == .value;'

# True once records rest in every buffer before the publish burst: the deliver lanes delivered, the
# source without acknowledgements committed through its backlog, and every stage holding exactly
# its lane. Keeps the last observation in CASE_DIR/at-rest.
graceful_stop_at_rest() {
    local case_dir="$1"
    local output_end unacked_committed
    output_end="$(topic_end_offset "${graceful_stop_output_topic}")" || return 1
    unacked_committed="$(graceful_stop_committed_offset chaos_stop_unacked chaos_stop_unacked_input)" || return 1
    graceful_stop_record_stages "${case_dir}/at-rest" || return 1
    local expected
    expected="$(graceful_stop_expected_stages false)"
    jq -n --slurpfile stages "${case_dir}/at-rest/stages.json" --argjson expected "${expected}" \
        --argjson output_end "${output_end}" --argjson unacked_committed "${unacked_committed}" \
        "${graceful_stop_matches_jq}"'
        {stages: $stages[0], expected: $expected, output_end: $output_end,
         unacked_committed: $unacked_committed,
         at_rest: ($stages[0] as $s | all($expected | to_entries[]; graceful_stop_matches($s))
                   and $output_end == $expected.publisher_delivered
                   and $unacked_committed == 1)}' >"${case_dir}/at-rest.json"
    jq -e '.at_rest' "${case_dir}/at-rest.json" >/dev/null
}

# True once the gate holds the publish burst of the source without acknowledgements on the wire: the
# publishing emitter holds records in flight awaiting the destination's answer, every resting lane is
# still in its buffer, and two observations in a row agree on what the publishing emitter holds and
# delivered, so the answer is withheld, not late. The observations read only the nodes' metrics, so
# the slowdown stays short; graceful_stop_publish_taken then proves the destination took the publish.
# Keeps them in CASE_DIR/in-motion.
graceful_stop_held_previous=""
graceful_stop_publish_held() {
    local case_dir="$1"
    graceful_stop_record_stages "${case_dir}/in-motion" || return 1
    local expected
    expected="$(graceful_stop_expected_stages true)"
    local previous="${graceful_stop_held_previous:-null}"
    graceful_stop_held_previous="$(jq -c '{publisher_in_flight, publisher_delivered}' "${case_dir}/in-motion/stages.json")"
    jq -n --slurpfile stages "${case_dir}/in-motion/stages.json" --argjson expected "${expected}" \
        --argjson previous "${previous}" --argjson at_ms "$(epoch_ms)" \
        "${graceful_stop_matches_jq}"'
        $stages[0] as $s
        | {at_ms: $at_ms, stages: $s, expected: $expected, previous: $previous,
           held: (all($expected | to_entries[]; graceful_stop_matches($s))
                  and ($s.publisher_in_flight // 0) > 0
                  and $previous == {publisher_in_flight: $s.publisher_in_flight,
                                    publisher_delivered: $s.publisher_delivered})}' >"${case_dir}/in-motion.json"
    jq -c '.' "${case_dir}/in-motion.json" >>"${case_dir}/in-motion.ndjson"
    jq -e '.held' "${case_dir}/in-motion.json" >/dev/null
}

# True when the destination took part of the held publish: the output grew past the deliver lanes
# by no more than the publish burst it holds. Writes CASE_DIR/held-publish.json.
graceful_stop_publish_taken() {
    local case_dir="$1"
    local output_end
    output_end="$(topic_end_offset "${graceful_stop_output_topic}")" || return 1
    jq -n --slurpfile in_motion "${case_dir}/in-motion.json" --argjson output_end "${output_end}" \
        --argjson publish "$(jq -n --argjson p "${graceful_stop_publish_lanes}" '[$p[].records] | add')" '
        ($in_motion[0].expected.publisher_delivered) as $delivered
        | {output_end: $output_end, delivered: $delivered, publish: $publish,
           in_flight: $in_motion[0].stages.publisher_in_flight,
           taken: ($output_end > $delivered and $output_end <= $delivered + $publish)}' \
        >"${case_dir}/held-publish.json"
    jq -e '.taken' "${case_dir}/held-publish.json" >/dev/null
}

# Requests the STOP in the background. Its answer, exit status and the moment it answered go to
# CASE_DIR, so the foreground can hold and heal the gate while the stop waits.
graceful_stop_request() {
    local case_dir="$1"
    graceful_stop_requested_ms="$(epoch_ms)"
    (
        local compose_call_timeout="${graceful_stop_answer_bound}"
        local status=0
        graceful_stop_cli 'STOP;' >"${case_dir}/stop-answer.txt" 2>&1 || status=$?
        epoch_ms >"${case_dir}/stop.answered-ms"
        printf '%s\n' "${status}" >"${case_dir}/stop.exit"
    ) &
    graceful_stop_stop_pid=$!
}

# True once both sources have confirmed every position they read and the output has not moved for
# three observations in a row, five seconds apart. Appends every observation to CASE_DIR/settle.ndjson.
graceful_stop_settled_ends=()
graceful_stop_settled() {
    local case_dir="$1"
    local acked_end unacked_end acked_committed unacked_committed output_end
    acked_end="$(topic_end_offset chaos_stop_acked_input)" || return 1
    unacked_end="$(topic_end_offset chaos_stop_unacked_input)" || return 1
    acked_committed="$(graceful_stop_committed_offset chaos_stop_acked chaos_stop_acked_input)" || return 1
    unacked_committed="$(graceful_stop_committed_offset chaos_stop_unacked chaos_stop_unacked_input)" || return 1
    output_end="$(topic_end_offset "${graceful_stop_output_topic}")" || return 1
    graceful_stop_settled_ends+=("${output_end}")
    jq -nc --argjson at_ms "$(epoch_ms)" --argjson acked_end "${acked_end}" --argjson unacked_end "${unacked_end}" \
        --argjson acked_committed "${acked_committed}" --argjson unacked_committed "${unacked_committed}" \
        --argjson output_end "${output_end}" \
        '{at_ms: $at_ms, acked_end: $acked_end, acked_committed: $acked_committed,
          unacked_end: $unacked_end, unacked_committed: $unacked_committed, output_end: $output_end}' \
        >>"${case_dir}/settle.ndjson"
    local count="${#graceful_stop_settled_ends[@]}"
    if ((acked_committed != acked_end || unacked_committed != unacked_end || count < 3)); then
        sleep 4
        return 1
    fi
    if [[ "${graceful_stop_settled_ends[$((count - 1))]}" != "${graceful_stop_settled_ends[$((count - 2))]}" \
        || "${graceful_stop_settled_ends[$((count - 2))]}" != "${graceful_stop_settled_ends[$((count - 3))]}" ]]; then
        sleep 4
        return 1
    fi
}

# Writes the burst fixtures of SOURCE: its backlog and its publish burst.
graceful_stop_fixtures() {
    local source="$1"
    local first
    first="$(jq -n --argjson lanes "${graceful_stop_backlog_lanes}" '[$lanes[].records] | add')"
    jq -nc --arg run_id "${run_id}" --arg source "${source}" --argjson first 0 \
        --argjson lanes "${graceful_stop_backlog_lanes}" -f "${script_dir}/fixtures/generate-graceful-stop.jq" \
        >"${artifact_dir}/fixtures/graceful-stop-${source}-backlog.ndjson"
    jq -nc --arg run_id "${run_id}" --arg source "${source}" --argjson first "${first}" \
        --argjson lanes "${graceful_stop_publish_lanes}" -f "${script_dir}/fixtures/generate-graceful-stop.jq" \
        >"${artifact_dir}/fixtures/graceful-stop-${source}-publish.ndjson"
}

# Judges the scenario and records every symptom as a recovery finding.
graceful_stop_verify() {
    local case_dir="$1"
    phase 'graceful stop: verdict'
    local source
    for source in acked unacked; do
        burst_load_accepted "graceful-stop-${source}" "chaos_stop_${source}_input" \
            "${artifact_dir}/traffic/graceful-stop-${source}-accepted.ndjson" \
            || graceful_stop_fail controller "the accepted-input ledger of the ${source} source could not be read"
    done
    local output_end
    output_end="$(topic_end_offset "${graceful_stop_output_topic}")" \
        || graceful_stop_fail controller 'the graceful-stop output boundary could not be read'
    if ((output_end > 0)); then
        kcat -q -b broker:9092 -C -t "${graceful_stop_output_topic}" -p 0 -o beginning -c "${output_end}" \
            >"${artifact_dir}/traffic/${graceful_stop_output_topic}.ndjson" \
            2>"${artifact_dir}/traffic/${graceful_stop_output_topic}.stderr" \
            || graceful_stop_fail controller 'the graceful-stop output could not be read'
    else
        : >"${artifact_dir}/traffic/${graceful_stop_output_topic}.ndjson"
    fi
    [[ "$(wc -l <"${artifact_dir}/traffic/${graceful_stop_output_topic}.ndjson")" -eq "${output_end}" ]] \
        || graceful_stop_fail controller "the graceful-stop output ledger does not hold its ${output_end} records"
    local transcripts=()
    local host
    for host in "${node_hosts[@]}"; do
        transcripts+=(--events "${case_dir}/events-${host}.log")
    done
    local status=0
    "${script_dir}/verify-strict-delivery.sh" \
        --output "${artifact_dir}/traffic/${graceful_stop_output_topic}.ndjson" \
        --source "acked=${artifact_dir}/traffic/graceful-stop-acked-accepted.ndjson" \
        --source "unacked=${artifact_dir}/traffic/graceful-stop-unacked-accepted.ndjson" \
        --negative-outcomes "${case_dir}/negative-outcomes-before.json" "${case_dir}/negative-outcomes-after.json" \
        --events-window "${case_dir}/events-window.json" "${transcripts[@]}" \
        --result "${case_dir}/delivery.json" >"${case_dir}/delivery.txt" 2>&1 || status=$?
    ((status <= 1)) || graceful_stop_fail observation "the strict delivery evidence cannot be judged; see graceful-stop/delivery.txt"
    local entity_args=()
    local entity
    while IFS= read -r entity; do
        entity_args+=(--entity "${entity#* }")
    done < <(graceful_stop_described_entities)
    status=0
    "${script_dir}/verify-graceful-stop.sh" --delivery "${case_dir}/delivery.json" \
        --stop-answer "${case_dir}/stop-answer.txt" --stop-exit "$(<"${case_dir}/stop.exit")" \
        --domain "${graceful_stop_domain}" "${entity_args[@]}" --stop-timing "${case_dir}/stop-timing.json" \
        --result "${case_dir}/verdict.json" >"${case_dir}/verdict.txt" 2>&1 || status=$?
    ((status <= 1)) || graceful_stop_fail controller "the graceful-stop verdict cannot be judged; see graceful-stop/verdict.txt"
    jq --arg scenario "${scenario}" '{scenario: $scenario, verdict_file: "graceful-stop/verdict.json",
            verdict: .verdict, symptoms: .symptoms}' "${case_dir}/verdict.json" \
        >"${artifact_dir}/results/symptoms.json"
    local finding
    while IFS= read -r finding; do
        recovery_finding "${case_dir}" "${finding}"
    done < <(jq -r '.symptoms[] | "\(.name): " + ({sources, count, entities, exit_code} | with_entries(select(.value != null)) | tojson)' \
        "${case_dir}/verdict.json")
    if ((status == 0)) && [[ "${graceful_stop_settled_within_bound}" != true ]]; then
        graceful_stop_fail observation "the sources did not settle within ${graceful_stop_settle_bound}s after START; see graceful-stop/settle.ndjson"
    fi
}

run_graceful_stop() {
    local case_dir="${artifact_dir}/graceful-stop"
    mkdir -p "${case_dir}"
    [[ "${node_count}" == 3 ]] || graceful_stop_fail setup 'graceful-stop requires three nodes'
    recovery_traffic_startup "${case_dir}"
    graceful_stop_settled_within_bound=false

    failure_category=controller
    phase 'graceful stop: error-event observers'
    events_observer_prepare "${case_dir}" \
        || graceful_stop_fail controller 'the error-event observers'"'"' quiet domain could not be created; see graceful-stop/observer-*.txt'
    events_observer_start "${case_dir}" \
        || graceful_stop_fail controller 'the error-event observers did not attach'

    failure_category=product
    phase 'graceful stop: graph installation and placement'
    cli_command "CREATE UNPACED DOMAIN ${graceful_stop_domain};" >"${case_dir}/create-domain.txt" 2>&1 \
        || graceful_stop_fail product "the domain ${graceful_stop_domain} could not be created"
    graceful_stop_cli "$(<"${script_dir}/fixtures/graceful-stop.plexus")" >"${case_dir}/configure-plexus.txt" 2>&1 \
        || graceful_stop_fail product 'the graceful-stop graph could not be installed; see graceful-stop/configure-plexus.txt'
    cp "${script_dir}/fixtures/graceful-stop.plexus" "${artifact_dir}/fixtures/graceful-stop.plexus"
    graceful_stop_place "${case_dir}"

    phase 'graceful stop: counters before START'
    failure_category=controller
    capture_all_metrics "${case_dir}/metrics-before" \
        || graceful_stop_fail controller 'the metrics before START could not be read'
    "${script_dir}/negative-outcomes.sh" snapshot --domain "${graceful_stop_domain}" \
        --ingestor chaos_stop_acked --ingestor chaos_stop_unacked \
        --emitter chaos_stop_publisher --emitter chaos_stop_holder \
        --output "${case_dir}/negative-outcomes-before.json" "${case_dir}/metrics-before"/metrics-*.txt \
        || graceful_stop_fail controller 'the negative-outcome counters before START could not be read'

    phase 'graceful stop: burst backlog of both sources'
    local source
    for source in acked unacked; do
        graceful_stop_fixtures "${source}"
        burst_load_produce "graceful-stop-${source}" "chaos_stop_${source}_input" \
            "${artifact_dir}/fixtures/graceful-stop-${source}-backlog.ndjson" \
            || graceful_stop_fail controller "the ${source} backlog was not produced at once"
    done

    phase 'graceful stop: START with the backlog waiting'
    failure_category=product
    local started_ms
    started_ms="$(epoch_ms)"
    graceful_stop_cli 'START;' >"${case_dir}/start.txt" 2>&1 \
        || graceful_stop_fail product "START of ${graceful_stop_domain} was not acknowledged; see graceful-stop/start.txt"

    phase 'graceful stop: records at rest in every buffer'
    failure_category=injection
    wait_for 'records resting in every buffer and the deliver lanes delivered' "${graceful_stop_rest_bound}" \
        graceful_stop_at_rest "${case_dir}" \
        || graceful_stop_fail injection 'the backlog did not come to rest in every buffer; see graceful-stop/at-rest.json'
    local at_rest_ms
    at_rest_ms="$(epoch_ms)"

    phase 'graceful stop: slow destination holds a publish on the wire'
    # The acknowledged source reads its publish burst only once its backlog is confirmed, so it can
    # wait in Kafka before the destination slows; producing it first keeps the slowdown short.
    failure_category=controller
    burst_load_produce graceful-stop-acked chaos_stop_acked_input \
        "${artifact_dir}/fixtures/graceful-stop-acked-publish.ndjson" \
        || graceful_stop_fail controller 'the acked publish burst was not produced at once'
    local emitter_id emitter_address gate_closed_ms
    emitter_id="$(owned_service_container "nervix-${graceful_stop_emitter_node##*-}")" \
        || graceful_stop_fail controller 'the emitter node container could not be identified'
    emitter_address="$(container_address "${emitter_id}")"
    [[ "${emitter_address}" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] \
        || graceful_stop_fail controller 'the emitter node has no address on the run network'
    gate_closed_ms="$(epoch_ms)"
    node_fault_netem_start "${case_dir}" "$(graceful_stop_gate_name)" broker broker "${emitter_address}" \
        "${graceful_stop_gate_lifetime}" 'loss 100%' loss --percent 100
    failure_category=controller
    local publish_ms
    publish_ms="$(epoch_ms)"
    burst_load_produce graceful-stop-unacked chaos_stop_unacked_input \
        "${artifact_dir}/fixtures/graceful-stop-unacked-publish.ndjson" \
        || graceful_stop_fail controller 'the unacked publish burst was not produced at once'
    failure_category=injection
    wait_for 'a publish held on the wire behind the slow destination' "${graceful_stop_held_bound}" \
        graceful_stop_publish_held "${case_dir}" \
        || graceful_stop_fail injection 'the slow destination held no publish on the wire, or a buffer released its lane; see graceful-stop/in-motion.ndjson'
    graceful_stop_publish_taken "${case_dir}" \
        || graceful_stop_fail injection 'the destination took no part of the held publish, or more than it; see graceful-stop/held-publish.json'
    local stop_due_ms=$((started_ms + graceful_stop_stop_deadline * 1000))
    (($(epoch_ms) < stop_due_ms)) \
        || graceful_stop_fail injection "the stop could not be requested within ${graceful_stop_stop_deadline}s of START, so a buffer might release its lane first"

    phase 'graceful stop: STOP while records are in motion'
    failure_category=product
    graceful_stop_request "${case_dir}"
    local hold_until_ms=$((graceful_stop_requested_ms + graceful_stop_hold_seconds * 1000))
    local wait_ms=$((hold_until_ms - $(epoch_ms)))
    if ((wait_ms > 0)); then
        sleep "$((wait_ms / 1000)).$(printf '%03d' $((wait_ms % 1000)))"
    fi
    local gate_opened_ms
    gate_opened_ms="$(epoch_ms)"
    node_fault_netem_heal "${case_dir}" "$(graceful_stop_gate_name)" broker
    failure_category=product
    wait "${graceful_stop_stop_pid}" || true
    [[ -s "${case_dir}/stop.exit" && -s "${case_dir}/stop.answered-ms" ]] \
        || graceful_stop_fail controller 'the STOP request recorded no outcome'
    local answered_ms
    answered_ms="$(<"${case_dir}/stop.answered-ms")"
    jq -n --argjson started "${started_ms}" --argjson at_rest "${at_rest_ms}" \
        --argjson gate_closed "${gate_closed_ms}" --argjson publish "${publish_ms}" \
        --argjson requested "${graceful_stop_requested_ms}" \
        --argjson gate_opened "${gate_opened_ms}" --argjson answered "${answered_ms}" \
        --argjson exit_code "$(<"${case_dir}/stop.exit")" \
        '{start_ms: $started, at_rest_ms: $at_rest, gate_closed_ms: $gate_closed, publish_ms: $publish,
          requested_ms: $requested, gate_opened_ms: $gate_opened, answered_ms: $answered,
          exit_code: $exit_code, duration_ms: ($answered - $requested),
          slowdown_ms: ($gate_opened - $gate_closed), publish_held_ms: ($gate_opened - $publish),
          answered_after_destination_returned: ($answered > $gate_opened)}' >"${case_dir}/stop-timing.json"
    printf 'STOP answered in %sms with exit %s: %s\n' "$((answered_ms - graceful_stop_requested_ms))" \
        "$(<"${case_dir}/stop.exit")" "$(tr '\n' ' ' <"${case_dir}/stop-answer.txt")"

    phase 'graceful stop: START after the stop'
    graceful_stop_cli 'START;' >"${case_dir}/start-after-stop.txt" 2>&1 \
        || recovery_finding "${case_dir}" "START after the stop was not acknowledged; see graceful-stop/start-after-stop.txt"

    phase 'graceful stop: both ledgers settle'
    failure_category=product
    graceful_stop_settled_ends=()
    if wait_for 'both sources confirmed and the output settled' "${graceful_stop_settle_bound}" \
        graceful_stop_settled "${case_dir}"; then
        graceful_stop_settled_within_bound=true
    fi

    failure_category=controller
    capture_all_metrics "${case_dir}/metrics-after" \
        || graceful_stop_fail controller 'the metrics after the stop could not be read'
    "${script_dir}/negative-outcomes.sh" snapshot --domain "${graceful_stop_domain}" \
        --ingestor chaos_stop_acked --ingestor chaos_stop_unacked \
        --emitter chaos_stop_publisher --emitter chaos_stop_holder \
        --output "${case_dir}/negative-outcomes-after.json" "${case_dir}/metrics-after"/metrics-*.txt \
        || graceful_stop_fail controller 'the negative-outcome counters after the stop could not be read'
    events_observer_collect "${case_dir}" \
        || graceful_stop_fail controller 'the error-event observers'"'"' transcripts could not be read'
    check_support_containers

    failure_category=product
    graceful_stop_verify "${case_dir}"
    recovery_final_boundary
    jq -n --arg domain "${graceful_stop_domain}" \
        --argjson backlog "${graceful_stop_backlog_lanes}" --argjson publish "${graceful_stop_publish_lanes}" \
        --argjson rest_seconds "${graceful_stop_rest_seconds}" --argjson stop_deadline "${graceful_stop_stop_deadline}" \
        --argjson hold_seconds "${graceful_stop_hold_seconds}" \
        --argjson settled "${graceful_stop_settled_within_bound}" \
        --slurpfile placement "${case_dir}/placement.json" \
        --slurpfile at_rest "${case_dir}/at-rest.json" --slurpfile in_motion "${case_dir}/in-motion.json" \
        --slurpfile held "${case_dir}/held-publish.json" \
        --slurpfile timing "${case_dir}/stop-timing.json" --slurpfile verdict "${case_dir}/verdict.json" \
        '{domain: $domain, lanes: {backlog: $backlog, publish: $publish},
          rest_seconds: $rest_seconds, stop_deadline_seconds: $stop_deadline, hold_seconds: $hold_seconds,
          placement: $placement[0], at_rest: $at_rest[0], in_motion: $in_motion[0],
          held_publish: $held[0],
          stop: ($timing[0] + {answer: $verdict[0].stop.answer}),
          settled: $settled, verdict: $verdict[0].verdict, symptoms: $verdict[0].symptoms,
          evidence: {verdict: "graceful-stop/verdict.json", delivery: "graceful-stop/delivery.json",
                     symptoms: "results/symptoms.json"}}' \
        >"${artifact_dir}/results/graceful-stop-progress.json"
}
