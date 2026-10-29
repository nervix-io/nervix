#!/usr/bin/env bash
set -euo pipefail

# Decides the verdicts of the lifetimes scenario from external ledgers: the records each source
# accepted, each output in offset order, and the boundaries the runner observed. Every mode writes
# its verdict to --result and exits 1 when the evidence contradicts the contract it checks or does
# not exercise it.
#
# Life records are numbered from 1, and a record's sequence is its number. Red, blue, gold and green
# interleave strictly. Records up to `milestone_records` were accepted and acknowledged before the
# fault; with fault `none`, the run had no fault and every deviation is a violation. After a fault,
# a record past the milestone may be replayed identically, because the fault can interrupt an
# acknowledgement after its record was written.

usage() {
    cat >&2 <<'EOF'
usage:
  verify-lifetimes-evidence.sh enrich|generator|window|rotation|expiring|late|flush|pool
      --traffic DIRECTORY --boundaries FILE --result FILE

enrich requires every life record to reach the junction's output with its branch's materialized
profile: blue's first records only once blue's state arrived, red's version as of its last
replacement by a strictly newer timestamp, never a superseded version.

generator requires every generated row to carry a profile its branch held, never a superseded
version, with versions that never go back, and every branch to generate.

window requires every tumbling window to hold a contiguous run of one branch, with exact count and
sum and sketches within their bounds, and its closed windows to aggregate every record of the run
exactly once, without a gap. After a fault, a window that holds a record of the volatile interval
is exempt, with every record it holds.

rotation requires every life record to pass the LRU-bounded relay exactly once while its branches
were evicted.

expiring requires the expiring branch's records before its expiry to carry its first profile and
those after its recreation its second, the branch to have expired on its TTL, and every record
exactly once.

late requires every late record exactly once after its emitter recovered from the missing topic.

flush requires every buffered record exactly once, held until the alteration and force-flushed
within its bound, long before the flush interval.

pool requires both pooled tables to hold every pooled record exactly once, an emitter to have
reported its wait for the client's full pool, again after a fault, and no node to have held more
than the pool's one connection while the emitter waited.
EOF
}

fail_usage() {
    printf '%s\n' "$*" >&2
    usage
    exit 2
}

mode="${1:-}"
[[ -n "${mode}" ]] || fail_usage 'a verifier mode is required'
shift
traffic=""
boundaries=""
result=""
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --traffic | --boundaries | --result)
            [[ "$#" -ge 2 ]] || fail_usage "$1 requires a value"
            case "$1" in
                --traffic) traffic="$2" ;;
                --boundaries) boundaries="$2" ;;
                --result) result="$2" ;;
            esac
            shift 2
            ;;
        *) fail_usage "unknown ${mode} argument: $1" ;;
    esac
done
[[ -n "${result}" ]] || fail_usage '--result is required'
[[ -d "${traffic}" ]] || fail_usage "the traffic directory is missing: ${traffic}"
[[ -s "${boundaries}" ]] || fail_usage "the observed boundaries are missing: ${boundaries}"
jq -e '
    (.fault | type == "string")
    and (.boundaries.milestone_records | type == "number")
    and (.boundaries.recovered_records | type == "number")
    and (.red_versions | type == "array")
' "${boundaries}" >/dev/null 2>&1 || fail_usage "the boundaries do not match their contract: ${boundaries}"

require_file() {
    [[ -f "$1" ]] || fail_usage "a ledger is missing: $1"
}

# The jq definitions every mode shares: the fault, the exact-once rule with the identical replays a
# fault permits past the milestone, and record identity.
common='
    $boundaries[0] as $b
    | def faulted: $b.fault != "none";
    def replayable($sequence): faulted and $sequence > $b.boundaries.milestone_records;
    def counts($rows): $rows | group_by(.event_id) | map({key: .[0].event_id, value: length}) | from_entries;
    def exactly_once($accepted; $rows; may_repeat):
        counts($rows) as $c
        | [$accepted[] | . as $record | ($c[$record.event_id] // 0) as $n
           | select($n == 0 or ($n > 1 and (($record | may_repeat) | not)))
           | {sequence: $record.sequence, count: $n}];
'

case "${mode}" in
    enrich)
        input="${traffic}/life-accepted-input.ndjson"
        output="${traffic}/chaos_life_enriched_output.ndjson"
        require_file "${input}"
        require_file "${output}"
        # shellcheck disable=SC2016
        jq -n --slurpfile boundaries "${boundaries}" --slurpfile accepted "${input}" --slurpfile rows "${output}" \
            "${common}"'
            def expected_version($record):
                if $record.branch_name == "red" then
                    (first($b.red_versions[] | select($record.sequence <= .until) | .version) // $b.red_latest)
                else 1 end;
            ($accepted | map({key: .event_id, value: .}) | from_entries) as $by_id
            | exactly_once($accepted; $rows; replayable(.sequence)) as $count_failures
            | [$rows[] | . as $row | $by_id[$row.event_id] as $source
               | select($source == null
                        or ([$row.event_id, $row.branch_name, $row.sequence, $row.branch_index, $row.content]
                            != [$source.event_id, $source.branch_name, $source.sequence, $source.branch_index, $source.content]))
               | $row.sequence] | unique as $altered
            | [$rows[] | . as $row | $by_id[$row.event_id] as $source | select($source != null)
               | expected_version($source) as $v
               | select($row.profile_version != $v or $row.label != "\($row.branch_name)-v\($v)")
               | {sequence: $row.sequence, branch: $row.branch_name, version: $row.profile_version, expected: $v}]
                as $wrong_versions
            | [$rows[] | select(.branch_name == "red" and ((.profile_version as $v | $b.red_superseded | index($v)) != null))
               | .sequence] as $superseded
            | [$accepted[] | select(.branch_name == "blue" and .sequence <= $b.boundaries.parked_until)] as $parked
            | [
                (if $count_failures != [] then "records not delivered exactly once: \($count_failures)" else empty end),
                (if $altered != [] then "records whose enriched fields differ from the accepted record: \($altered)" else empty end),
                (if $wrong_versions != [] then "records enriched with another profile than their branch held: \($wrong_versions[0:20])" else empty end),
                (if $superseded != [] then "red records carry a superseded version: \($superseded)" else empty end),
                (if $parked == [] then "no blue record arrived before blue had state" else empty end),
                (if ($accepted | length) <= $b.boundaries.milestone_records then "the source accepted no record past the milestone" else empty end)
              ] as $failures
            | {processor: "enrich", verdict: (if $failures == [] then "pass" else "fail" end), failures: $failures,
               accepted: ($accepted | length), delivered: ($rows | length), parked_blue: ($parked | length)}
        ' >"${result}"
        ;;
    generator)
        output="${traffic}/chaos_life_generated_output.ndjson"
        require_file "${output}"
        # shellcheck disable=SC2016
        jq -n --slurpfile boundaries "${boundaries}" --slurpfile rows "${output}" '
            $boundaries[0] as $b
            | {red: ([$b.red_versions[].version] + [$b.red_latest] | unique), blue: [1], gold: [1], green: [1]} as $held
            | [$rows[] | select(($held[.branch_name] // null) == null
                                or (.profile_version as $v | $held[.branch_name] | index($v)) == null
                                or .label != "\(.branch_name)-v\(.profile_version)")]
                as $invalid
            | [$rows | group_by(.branch_name)[] | . as $branch
               | [range(1; length) | select($branch[.].profile_version < $branch[. - 1].profile_version)]
               | select(length > 0) | $branch[0].branch_name] as $regressed
            | ["red", "blue", "gold", "green"] as $branches
            | [$branches[] | . as $name | select(([$rows[] | select(.branch_name == $name)] | length) < 3)] as $silent
            | [
                (if $invalid != [] then "generated rows carry a profile their branch never held: \($invalid[0:10])" else empty end),
                (if $regressed != [] then "generated versions went back for: \($regressed)" else empty end),
                (if $silent != [] then "branches generated fewer than three rows: \($silent)" else empty end)
              ] as $failures
            | {processor: "generator", verdict: (if $failures == [] then "pass" else "fail" end), failures: $failures,
               generated: ($rows | length),
               per_branch: ($rows | group_by(.branch_name) | map({key: .[0].branch_name, value: length}) | from_entries)}
        ' >"${result}"
        ;;
    window)
        input="${traffic}/life-accepted-input.ndjson"
        output="${traffic}/chaos_life_window_output.ndjson"
        require_file "${input}"
        require_file "${output}"
        # shellcheck disable=SC2016
        jq -n --slurpfile boundaries "${boundaries}" --slurpfile accepted "${input}" --slurpfile rows "${output}" '
            $boundaries[0] as $b
            # The branch index of the first record past the milestone, and of the first one after the
            # fault recovered: a window that holds a row between them may be lost or aggregated twice.
            | (($b.boundaries.milestone_records / 4) | floor) as $volatile_from
            | ((($b.boundaries.recovered_records + 3) / 4) | ceil + 1) as $volatile_to
            | def volatile($window):
                $b.fault != "none" and $window.last_index > $volatile_from and $window.first_index <= $volatile_to;
            # A tumbling time window holds a contiguous run of one branch, in arrival order.
            [$rows[] | select(.records < 1 or .last_index - .first_index + 1 != .records
                              or (.index_sum * 2) != (.records * (.first_index + .last_index))
                              or .distinct_residues < 1 or .distinct_residues > ([.records, 3] | min)
                              or (.median_index != null and (.median_index < .first_index or .median_index > .last_index)))
               | select(volatile(.) | not)] as $malformed
            # A volatile window exempts the records it holds past the interval as well, because the
            # fault decided which of its records it aggregated.
            | [$rows | group_by(.branch_name)[] | . as $windows
               | [$windows[] | select(volatile(.) | not) | range(.first_index; .last_index + 1)] as $covered
               | [$windows[] | select(volatile(.)) | range(.first_index; .last_index + 1)] as $exempt
               | ($covered | group_by(.) | map(select(length > 1) | .[0])) as $twice
               | (($covered + $exempt) | unique) as $seen
               | (if $seen == [] then [] else [range(1; ($seen | max) + 1)] - $seen end) as $gaps
               | (if $b.fault == "none" then $gaps else [$gaps[] | select(. <= $volatile_from or . > $volatile_to)] end) as $gaps
               | select($twice != [] or $gaps != [])
               | {branch: $windows[0].branch_name, aggregated_twice: $twice, never_aggregated: $gaps}] as $coverage
            | ["red", "blue", "gold", "green"] as $branches
            | [$branches[] | . as $name | select(([$rows[] | select(.branch_name == $name)] | length) == 0)] as $silent
            | [
                (if $malformed != [] then "windows outside the volatile interval with wrong bounds or sketches: \($malformed[0:10])" else empty end),
                (if $coverage != [] then "records aggregated twice or skipped by the closed windows: \($coverage)" else empty end),
                (if $silent != [] then "branches without a window: \($silent)" else empty end)
              ] as $failures
            | {processor: "window", verdict: (if $failures == [] then "pass" else "fail" end), failures: $failures,
               windows: ($rows | length)}
        ' >"${result}"
        ;;
    rotation)
        input="${traffic}/life-accepted-input.ndjson"
        output="${traffic}/chaos_life_rotating_output.ndjson"
        require_file "${input}"
        require_file "${output}"
        # shellcheck disable=SC2016
        jq -n --slurpfile boundaries "${boundaries}" --slurpfile accepted "${input}" --slurpfile rows "${output}" \
            "${common}"'
            ($accepted | map({key: .event_id, value: .}) | from_entries) as $by_id
            | exactly_once($accepted; $rows; replayable(.sequence)) as $count_failures
            | [$rows[] | select($by_id[.event_id] != .) | .sequence] | unique as $altered
            | [
                (if $count_failures != [] then "records not delivered exactly once through the rotating relay: \($count_failures[0:20])" else empty end),
                (if $altered != [] then "records whose rotated copy differs from the accepted record: \($altered[0:20])" else empty end),
                (if ($b.lru_evictions // 0) < 1 then "no branch of the rotating relay was evicted by its LRU bound" else empty end)
              ] as $failures
            | {processor: "rotation", verdict: (if $failures == [] then "pass" else "fail" end), failures: $failures,
               accepted: ($accepted | length), delivered: ($rows | length), lru_evictions: $b.lru_evictions}
        ' >"${result}"
        ;;
    expiring)
        input="${traffic}/chaos_life_expiring_input.ndjson"
        output="${traffic}/chaos_life_expiring_output.ndjson"
        require_file "${input}"
        require_file "${output}"
        # shellcheck disable=SC2016
        jq -n --slurpfile boundaries "${boundaries}" --slurpfile accepted "${input}" --slurpfile rows "${output}" \
            "${common}"'
            ($accepted | map({key: .event_id, value: .}) | from_entries) as $by_id
            | exactly_once($accepted; $rows; false) as $count_failures
            | [$rows[] | . as $row | $by_id[$row.event_id] as $source | select($source != null)
               | (if $source.sequence <= $b.expiring.before then 1 else 2 end) as $v
               | select($row.profile_version != $v or $row.label != "green-v\($v)")
               | {sequence: $row.sequence, version: $row.profile_version, expected: $v}] as $wrong
            | [
                (if $count_failures != [] then "expiring records not delivered exactly once: \($count_failures)" else empty end),
                (if $wrong != [] then "expiring records carry another profile than the one their branch held: \($wrong)" else empty end),
                (if ($accepted | length) != ($b.expiring.before + $b.expiring.after) then "the expiring source did not accept every record" else empty end),
                (if ($b.expiring.ttl_evictions_after // 0) <= ($b.expiring.ttl_evictions_before // 0)
                 then "the expiring branch was not evicted by its TTL" else empty end)
              ] as $failures
            | {processor: "expiring", verdict: (if $failures == [] then "pass" else "fail" end), failures: $failures,
               accepted: ($accepted | length), delivered: ($rows | length)}
        ' >"${result}"
        ;;
    late | flush)
        if [[ "${mode}" == late ]]; then
            output="${traffic}/chaos_life_late_output.ndjson"
        else
            output="${traffic}/chaos_life_buffered_output.ndjson"
        fi
        require_file "${output}"
        # shellcheck disable=SC2016
        jq -n --arg mode "${mode}" --arg run_id "${RUN_ID:-}" --slurpfile boundaries "${boundaries}" --slurpfile rows "${output}" '
            $boundaries[0] as $b
            | (if $mode == "late" then $b.late_records else $b.flush_records end) as $count
            | ($rows | group_by(.sequence) | map({key: (.[0].sequence | tostring), value: length}) | from_entries) as $c
            | [range(1; $count + 1) | select(($c[tostring] // 0) != 1)] as $not_once
            | [$rows[] | select(.branch_name != $mode or .content != "chaos-\($mode)-payload-\(.sequence)"
                                or .branch_index != .sequence or .sequence < 1 or .sequence > $count)
               | .sequence] as $altered
            | (if $mode == "flush" then $b.force_flush else null end) as $f
            | [
                (if $not_once != [] then "\($mode) records not delivered exactly once: \($not_once)" else empty end),
                (if $altered != [] then "\($mode) records that differ from the records produced: \($altered)" else empty end),
                (if $mode == "flush" and $f.output_before_alteration != 0
                 then "the buffered output held \($f.output_before_alteration) records before the alteration force-flushed it" else empty end),
                (if $mode == "flush" and $f.arrived_ms == null then "the force-flushed records never arrived" else empty end),
                (if $mode == "flush" and $f.arrived_ms != null and ($f.arrived_ms - $f.produced_ms) >= $f.flush_interval_ms
                 then "the buffered records arrived only at their flush interval" else empty end),
                (if $mode == "flush" and ($f.admission_observed | not)
                 then "the buffered route never counted the records before the alteration" else empty end)
              ] as $failures
            | {processor: $mode, verdict: (if $failures == [] then "pass" else "fail" end), failures: $failures,
               delivered: ($rows | length), expected: $count, force_flush: $f}
        ' >"${result}"
        ;;
    pool)
        first="${traffic}/chaos_life_pool_first.json"
        second="${traffic}/chaos_life_pool_second.json"
        require_file "${first}"
        require_file "${second}"
        # shellcheck disable=SC2016
        jq -n --slurpfile boundaries "${boundaries}" --slurpfile first "${first}" --slurpfile second "${second}" '
            $boundaries[0] as $b
            | $b.pool as $p
            | def table_failures($table; $rows):
                ($rows | group_by(.sequence) | map({key: (.[0].sequence | tostring), value: length}) | from_entries) as $c
                | [range(1; $p.records + 1) | select(($c[tostring] // 0) != 1)] as $not_once
                | [$rows[] | . as $row
                   | select($row.sequence < 1 or $row.sequence > $p.records
                            or ($row.event_id | endswith("-pool-\($row.sequence)") | not))
                   | $row.sequence] as $altered
                | [
                    (if $not_once != [] then "\($table) does not hold every pooled record exactly once: \($not_once)" else empty end),
                    (if $altered != [] then "\($table) holds rows that differ from the records produced: \($altered)" else empty end)
                  ];
            ($p.connections_while_waiting | map(select(.connections > 1))) as $crowded
            | [
                table_failures("chaos_life_pool_first"; $first[0])[],
                table_failures("chaos_life_pool_second"; $second[0])[],
                (if $p.waiting_emitter == null then "no pooled emitter reported a wait for the full pool" else empty end),
                (if $b.fault != "none" and $p.waiting_after_fault == null
                 then "no pooled emitter reported its wait for the full pool after the fault" else empty end),
                (if ($p.connections_while_waiting | length) == 0
                 then "the pool held no connection while an emitter waited for it" else empty end),
                (if $crowded != [] then "a node held more connections than its pool allows: \($crowded)" else empty end)
              ] as $failures
            | {processor: "pool", verdict: (if $failures == [] then "pass" else "fail" end), failures: $failures,
               first: ($first[0] | length), second: ($second[0] | length), expected: $p.records,
               waiting_emitter: $p.waiting_emitter, waiting_after_fault: $p.waiting_after_fault,
               connections_while_waiting: $p.connections_while_waiting}
        ' >"${result}"
        ;;
    *)
        fail_usage "unknown verifier mode: ${mode}"
        ;;
esac
jq -e '.verdict == "pass"' "${result}" >/dev/null || exit 1
