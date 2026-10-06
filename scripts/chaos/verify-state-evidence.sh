#!/usr/bin/env bash
set -euo pipefail

# Decides the processor verdicts of the stateful scenario from external ledgers: the stateful
# records the source accepted, each processor's Kafka output in offset order, and the boundaries
# the runner observed around its fault. Every mode writes its verdict to --result and exits 1 when
# the evidence contradicts the contract it checks or does not exercise it.
#
# Records are classified by their source sequence. Those below `milestone_records` were accepted,
# acknowledged and processed before the held durability milestone; those from `milestone_records`
# below `recovered_records` were produced between the milestone and the end of the fault's
# recovery, the permitted volatile interval; later ones were produced after recovery. An output is
# "before the fault" when its offset is below the processor's entry in `pre_fault_outputs`. With
# fault `none`, the run had no fault and every deviation is a violation.

usage() {
    cat >&2 <<'EOF'
usage:
  verify-state-evidence.sh dedup|window|enrich|counter --input FILE --output FILE
      --boundaries FILE --result FILE

dedup requires every first occurrence of a key in its branch to be emitted, a later occurrence to
pass only when its key was first seen in the volatile interval, and identical replays only after a
fault.

window decodes each 12-row window's membership and requires its rows to belong to its branch with
the sums it reports, every row outside the volatile interval to be aggregated exactly once, and
each row admitted before the milestone to survive the fault.

enrich requires every record to be emitted with its own branch's materialized profile, never the
default, and with at least the version durable at the milestone once the milestone has passed.

counter requires each record's guest count to equal its branch index before the fault and never
fall below it, to exceed it only by records the volatile interval could replay, and to advance by
one per record after recovery.
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
input=""
output=""
boundaries=""
result=""
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --input | --output | --boundaries | --result)
            [[ "$#" -ge 2 ]] || fail_usage "$1 requires a value"
            case "$1" in
                --input) input="$2" ;;
                --output) output="$2" ;;
                --boundaries) boundaries="$2" ;;
                --result) result="$2" ;;
            esac
            shift 2
            ;;
        *) fail_usage "unknown ${mode} argument: $1" ;;
    esac
done
[[ -n "${result}" ]] || fail_usage '--result is required'
[[ -s "${input}" ]] || fail_usage "the accepted stateful input is missing or empty: ${input}"
[[ -f "${output}" ]] || fail_usage "the processor output ledger is missing: ${output}"
[[ -s "${boundaries}" ]] || fail_usage "the observed boundaries are missing: ${boundaries}"
jq -e '
    (.fault | type == "string")
    and (.milestone_records | type == "number")
    and (.recovered_records | type == "number")
    and .recovered_records >= .milestone_records
    and (.pre_fault_outputs | type == "object")
    and (.profiles.milestone | type == "number")
    and (.profiles.volatile | type == "number")
' "${boundaries}" >/dev/null 2>&1 || fail_usage "the boundaries do not match their contract: ${boundaries}"
jq -e -s 'all(.[]; (.event_id | type == "string") and (.branch_name | type == "string")
    and (.sequence | type == "number") and (.branch_index | type == "number")
    and (.dedup_key | type == "string") and (.content | type == "string"))' "${input}" >/dev/null 2>&1 \
    || fail_usage "the accepted stateful input does not match the fixture contract: ${input}"
mkdir -p "$(dirname "${result}")"

# Shared definitions: the class of a source sequence and the record each event names.
# The dollars in this jq program are jq variables, not shell expansion.
# shellcheck disable=SC2016
common='
    ($bounds[0]) as $b
    | ($b.fault == "none") as $strict
    | def class($sequence):
        if $sequence < $b.milestone_records then "milestone"
        elif $sequence < $b.recovered_records then "volatile"
        else "recovered" end;
      ($input | map({key: .event_id, value: .}) | from_entries) as $by_id
'

finish_verdict() {
    if [[ "$(jq -r '.verdict' "${result}")" != pass ]]; then
        printf '%s verdict failed: %s\n' "${mode}" "$(jq -c '.failures' "${result}")" >&2
        exit 1
    fi
}

dedup() {
    jq -n --slurpfile input "${input}" --slurpfile output "${output}" \
        --slurpfile bounds "${boundaries}" "${common}"'
        | ($b.pre_fault_outputs.chaos_unique_output // 0) as $pre
        | ($input | group_by([.branch_name, .dedup_key])
           | map({key: (.[0].branch_name + "/" + .[0].dedup_key), value: (map(.sequence) | min)})
           | from_entries) as $first
        | def first_sequence($record): $first[$record.branch_name + "/" + $record.dedup_key];
          [$input[] | select(.sequence == first_sequence(.)) | .event_id] as $expected
        | [$output | to_entries[] | {offset: .key, record: .value}] as $rows
        | ([$output[].event_id] | unique) as $observed
        | [$rows[] | select($by_id[.record.event_id] == null) | .record] as $unexpected
        | [$rows[] | select($by_id[.record.event_id] != null and $by_id[.record.event_id] != .record)
           | {offset, observed: .record, expected: $by_id[.record.event_id]}] as $incorrect
        | [$expected[] | select(. as $id | $observed | bsearch($id) < 0)] as $missing
        | [$rows[] | select($by_id[.record.event_id] == .record)
           | . as $row | $by_id[$row.record.event_id] as $record
           | first_sequence($record) as $first_sequence
           | select($record.sequence != $first_sequence)
           | {offset, event_id: $record.event_id, branch_name: $record.branch_name,
              dedup_key: $record.dedup_key, sequence: $record.sequence, first_sequence: $first_sequence,
              class: (if $strict or .offset < $pre then "passed_without_a_fault"
                      elif class($first_sequence) == "milestone" then "milestone_key_forgotten"
                      elif class($first_sequence) == "volatile" then "volatile"
                      else "recovered_key_forgotten" end)}] as $passed
        | [$rows | group_by(.record.event_id)[] | select(length > 1)
           | {event_id: .[0].record.event_id, count: length,
              before_fault: (map(select(.offset < $pre)) | length)}] as $replays
        | [$input[] | select(.sequence >= $b.recovered_records)
           | select(.sequence != first_sequence(.) and class(first_sequence(.)) == "milestone")] as $probes
        | [$probes[] | select(.event_id as $id | $observed | bsearch($id) >= 0)] as $probes_passed
        | ([$passed[] | select(.class != "volatile")]) as $violating_passes
        | ([$replays[] | select($strict or .before_fault > 1)]) as $violating_replays
        | {
            missing_first_occurrences: ($missing | length),
            unexpected_records: ($unexpected | length),
            incorrect_content: ($incorrect | length),
            duplicates_passed_outside_the_volatile_interval: ($violating_passes | length),
            replays_without_a_fault: ($violating_replays | length),
            milestone_keys_probed_after_recovery: (if $strict then null else ($probes | length) end)
          } as $counts
        | [
            (if ($missing | length) > 0 then "first occurrences missing from the output" else empty end),
            (if ($unexpected | length) > 0 then "output records the source never accepted" else empty end),
            (if ($incorrect | length) > 0 then "output records with wrong content or branch" else empty end),
            (if ($violating_passes | map(select(.class == "milestone_key_forgotten")) | length) > 0
             then "a duplicate of a key seen before the milestone passed after the fault" else empty end),
            (if ($violating_passes | map(select(.class == "recovered_key_forgotten")) | length) > 0
             then "a duplicate of a key first seen after recovery passed" else empty end),
            (if ($violating_passes | map(select(.class == "passed_without_a_fault")) | length) > 0
             then "a duplicate key passed while no fault had occurred" else empty end),
            (if ($violating_replays | length) > 0 then "records were emitted twice while no fault had occurred" else empty end),
            (if ($strict | not) and ($probes | length) == 0
             then "no duplicate of a pre-milestone key arrived after recovery, so durability is unverified" else empty end)
          ] as $failures
        | {
            processor: "deduplicator",
            verdict: (if ($failures | length) == 0 then "pass" else "fail" end),
            fault: $b.fault,
            input_records: ($input | length),
            output_records: ($output | length),
            expected_first_occurrences: ($expected | length),
            checks: $counts,
            replay_duplicates: {records: ([$replays[].count - 1] | add // 0), events: ($replays | length)},
            volatile_reemissions: ([$passed[] | select(.class == "volatile")] | length),
            durability: (if $strict then null else
              {milestone: "source acknowledged and committed, outputs stable, held for the configured publication intervals",
               milestone_records: $b.milestone_records,
               later_occurrences_of_milestone_keys_after_recovery: ($probes | length),
               of_which_passed: ($probes_passed | length)} end),
            missing_ids: $missing,
            unexpected: $unexpected,
            incorrect: $incorrect,
            passed_duplicates: $passed,
            replays: $replays,
            failures: $failures
          }' >"${result}"
    finish_verdict
}

window() {
    jq -n --slurpfile input "${input}" --slurpfile output "${output}" \
        --slurpfile bounds "${boundaries}" "${common}"'
        | ($b.pre_fault_outputs.chaos_window_output // 0) as $pre
        | ($input | map({key: (.branch_name + "/" + (.branch_index | tostring)), value: .})
           | from_entries) as $at
        # A window reports the sum of 4 to the power of each row index modulo 24, so each base-4
        # digit counts how often one index inside a 24-index span was aggregated.
        | def members($window):
            [range(0; 24) as $position
             | ((($window.membership / pow(4; $position)) | floor) % 4) as $count
             | select($count > 0)
             | {index: ($window.first_index + (($position - ($window.first_index % 24) + 24) % 24)),
                count: $count}];
          [$output | to_entries[] | .key as $offset | .value as $window
           | members($window) as $members
           | ($members | map(.count) | add // 0) as $decoded
           | [$members[] | $at[$window.branch_name + "/" + (.index | tostring)] as $record
              | . + {record: $record}] as $resolved
           | {offset: $offset, branch_name: $window.branch_name, records: $window.records,
              first_index: $window.first_index, last_index: $window.last_index,
              decodable: (($window.last_index - $window.first_index) < 24 and $decoded == $window.records
                          and (($members | map(.index) | min) == $window.first_index)
                          and (($members | map(.index) | max) == $window.last_index)),
              own_branch: (all($resolved[]; .record != null and .record.branch_name == $window.branch_name)),
              sum_matches: ((if all($resolved[]; .record != null)
                             then ($resolved | map(.count * .record.sequence) | add) else null end) == $window.sequence_sum),
              members: ($resolved | map({index, count, sequence: .record.sequence}))}] as $windows
        | ($windows | map(select(.decodable and .own_branch and .sum_matches))) as $valid
        | [$windows[] | select(.decodable | not)] as $undecodable
        | [$windows[] | select(.decodable and ((.own_branch | not) or (.sum_matches | not)))] as $wrong
        | [$windows[] | select(.records != 12)] as $wrong_size
        | [$windows[] | select(.offset < $pre)
           | select((.members | length) != 12 or any(.members[]; .count != 1)
                    or ((.first_index - 1) % 12) != 0 or .last_index != .first_index + 11)] as $inexact_before_fault
        | ($valid | map(.members[] as $member | {key: (.branch_name + "/" + ($member.index | tostring)),
                                                  count: $member.count, after_fault: (.offset >= $pre)})
           | group_by(.key) | map({key: .[0].key, value: {count: (map(.count) | add),
                                  after_fault: any(.[]; .after_fault)}}) | from_entries) as $coverage
        | ($valid | group_by(.branch_name) | map({key: .[0].branch_name, value: (map(.last_index) | max)})
           | from_entries) as $closed_through
        | [$input[] | select(.branch_index <= ($closed_through[.branch_name] // 0))
           | . as $record | ($coverage[$record.branch_name + "/" + ($record.branch_index | tostring)]) as $seen
           | {event_id, branch_name, branch_index, sequence, class: class(.sequence),
              count: ($seen.count // 0), after_fault: ($seen.after_fault // false)}] as $rows
        | [$rows[] | select(.count == 0)] as $lost
        | [$rows[] | select(.count > 1)] as $repeated
        | [$lost[] | select($strict or .class != "volatile")] as $violating_losses
        | [$repeated[] | select($strict or .class != "volatile")] as $violating_repeats
        # Rows of the window still open at the milestone: their window must close after the fault.
        | ($input | map(select(.sequence < $b.milestone_records)) | group_by(.branch_name)
           | map({key: .[0].branch_name, value: (map(.branch_index) | max)}) | from_entries) as $milestone_index
        | [$rows[] | select(.class == "milestone"
                            and .branch_index > ((($milestone_index[.branch_name] // 0) / 12 | floor) * 12))] as $open_at_milestone
        | [$open_at_milestone[] | select(.count == 1 and .after_fault)] as $restored_rows
        | [$open_at_milestone[] | select(.count >= 1 and (.after_fault | not))] as $closed_before_fault
        | [
            (if ($undecodable | length) > 0 then "windows whose membership cannot be decoded" else empty end),
            (if ($wrong | length) > 0 then "windows with rows of another branch or a wrong sum" else empty end),
            (if ($wrong_size | length) > 0 then "windows that did not aggregate exactly 12 rows" else empty end),
            (if ($inexact_before_fault | length) > 0 then "windows before the fault that were not 12 consecutive rows" else empty end),
            (if ($violating_losses | map(select(.class == "milestone")) | length) > 0
             then "rows admitted before the milestone were lost from the restored window" else empty end),
            (if ($violating_losses | map(select(.class != "milestone")) | length) > 0
             then "rows outside the volatile interval were never aggregated" else empty end),
            (if ($violating_repeats | length) > 0
             then "rows outside the volatile interval were aggregated more than once" else empty end),
            (if ($strict | not) and ($open_at_milestone | length) == 0
             then "no row was open in a window at the milestone, so window durability is unverified" else empty end),
            (if ($strict | not) and ($closed_before_fault | length) > 0
             then "the windows open at the milestone closed before the fault, so window durability is unverified" else empty end)
          ] as $failures
        | {
            processor: "window",
            verdict: (if ($failures | length) == 0 then "pass" else "fail" end),
            fault: $b.fault,
            input_records: ($input | length),
            windows: ($windows | length),
            checks: {undecodable: ($undecodable | length), wrong_branch_or_sum: ($wrong | length),
                     wrong_size: ($wrong_size | length), inexact_before_fault: ($inexact_before_fault | length),
                     lost_outside_volatile_interval: ($violating_losses | length),
                     repeated_outside_volatile_interval: ($violating_repeats | length)},
            volatile_losses: ([$lost[] | select(.class == "volatile")] | length),
            replay_duplicates: {rows: ([$repeated[] | select(.class == "volatile") | .count - 1] | add // 0)},
            durability: (if $strict then null else
              {milestone: "source acknowledged and committed, outputs stable, held for the configured publication intervals",
               rows_open_at_milestone: ($open_at_milestone | length),
               restored_after_fault: ($restored_rows | length)} end),
            undecodable: $undecodable,
            wrong: $wrong,
            inexact_before_fault: $inexact_before_fault,
            lost: $lost,
            repeated: $repeated,
            failures: $failures
          }' >"${result}"
    finish_verdict
}

enrich() {
    jq -n --slurpfile input "${input}" --slurpfile output "${output}" \
        --slurpfile bounds "${boundaries}" "${common}"'
        | ($b.pre_fault_outputs.chaos_enriched_output // 0) as $pre
        | [$output | to_entries[] | {offset: .key, record: .value}] as $rows
        | ([$output[].event_id] | unique) as $observed
        | [$rows[] | select($by_id[.record.event_id] == null) | .record] as $unexpected
        | [$rows[] | select($by_id[.record.event_id] != null)
           | select((.record | del(.profile_branch, .profile_version)) != $by_id[.record.event_id])
           | {offset, observed: .record}] as $incorrect
        | [$rows[] | select($by_id[.record.event_id] != null) | . as $row
           | $by_id[$row.record.event_id] as $record
           | {offset, event_id: $record.event_id, branch_name: $record.branch_name,
              sequence: $record.sequence, class: class($record.sequence),
              profile_branch: $row.record.profile_branch, version: $row.record.profile_version}] as $enriched
        | [$enriched[] | select(.version == 0 or .profile_branch == "none")] as $defaults
        | [$enriched[] | select(.version > 0 and .profile_branch != "none" and .profile_branch != .branch_name)] as $wrong_branch
        | [$enriched[] | select(.version < 0 or .version > $b.profiles.volatile)] as $unknown_versions
        | [$enriched[] | select(.version > 0 and .sequence >= $b.milestone_records and .version < $b.profiles.milestone)] as $regressed
        | [$input[].event_id | select(. as $id | $observed | bsearch($id) < 0)] as $missing
        | [$enriched | group_by(.event_id)[] | select(length > 1)
           | {event_id: .[0].event_id, count: length, before_fault: (map(select(.offset < $pre)) | length)}] as $replays
        | [$replays[] | select($strict or .before_fault > 1)] as $violating_replays
        # Before the fault, and throughout a run without one, each branch sees its profile versions
        # in source order without going back.
        | [$enriched | map(select($strict or .offset < $pre)) | group_by(.branch_name)[]
           | sort_by(.sequence) | . as $ordered
           | range(1; length) as $i | select($ordered[$i].version < $ordered[$i - 1].version)
           | $ordered[$i]] as $went_back
        | [$enriched[] | select(.class == "recovered" and .version >= $b.profiles.milestone)] as $recovered
        | [
            (if ($unexpected | length) > 0 then "output records the source never accepted" else empty end),
            (if ($incorrect | length) > 0 then "output records with wrong content" else empty end),
            (if ($missing | length) > 0 then "accepted records missing from the enriched output" else empty end),
            (if ($defaults | length) > 0 then "records enriched with the default instead of materialized state" else empty end),
            (if ($wrong_branch | length) > 0 then "records enriched with another branch'"'"'s materialized state" else empty end),
            (if ($unknown_versions | length) > 0 then "records enriched with a profile version that was never published" else empty end),
            (if ($regressed | length) > 0 then "materialized state regressed below the version durable at the milestone" else empty end),
            (if ($went_back | length) > 0 then "a branch'"'"'s profile version went back while no fault had occurred" else empty end),
            (if ($violating_replays | length) > 0 then "records were emitted twice while no fault had occurred" else empty end),
            (if ($strict | not) and ($recovered | length) == 0
             then "no record produced after recovery was enriched, so durability is unverified" else empty end)
          ] as $failures
        | {
            processor: "materialized relay",
            verdict: (if ($failures | length) == 0 then "pass" else "fail" end),
            fault: $b.fault,
            input_records: ($input | length),
            output_records: ($output | length),
            checks: {missing: ($missing | length), unexpected: ($unexpected | length),
                     incorrect_content: ($incorrect | length), defaults: ($defaults | length),
                     wrong_branch: ($wrong_branch | length), unknown_versions: ($unknown_versions | length),
                     regressed_below_milestone: ($regressed | length), went_back: ($went_back | length),
                     replays_without_a_fault: ($violating_replays | length)},
            versions_after_recovery: ([$enriched[] | select(.class == "recovered") | .version]
                                      | group_by(.) | map({key: (.[0] | tostring), value: length}) | from_entries),
            replay_duplicates: {records: ([$replays[].count - 1] | add // 0), events: ($replays | length)},
            durability: (if $strict then null else
              {milestone: "both branches showed the milestone profile version in the materialized relay, held for the configured publication intervals",
               milestone_version: $b.profiles.milestone,
               records_after_recovery_at_or_above_it: ($recovered | length)} end),
            missing_ids: $missing,
            unexpected: $unexpected,
            incorrect: $incorrect,
            defaults: $defaults,
            wrong_branch: $wrong_branch,
            regressed: $regressed,
            went_back: $went_back,
            replays: $replays,
            failures: $failures
          }' >"${result}"
    finish_verdict
}

counter() {
    jq -n --slurpfile input "${input}" --slurpfile output "${output}" \
        --slurpfile bounds "${boundaries}" "${common}"'
        | ($b.pre_fault_outputs.chaos_counted_output // 0) as $pre
        | ($input | map(select(class(.sequence) == "volatile")) | group_by(.branch_name)
           | map({key: .[0].branch_name, value: length}) | from_entries) as $volatile_rows
        | [$output | to_entries[] | {offset: .key, record: .value}] as $rows
        | ([$output[].event_id] | unique) as $observed
        | [$rows[] | select($by_id[.record.event_id] == null) | .record] as $unexpected
        | [$rows[] | select($by_id[.record.event_id] != null)
           | select((.record | del(.branch_count)) != $by_id[.record.event_id])
           | {offset, observed: .record}] as $incorrect
        | [$rows[] | select($by_id[.record.event_id] != null) | . as $row
           | $by_id[$row.record.event_id] as $record
           | {offset, event_id: $record.event_id, branch_name: $record.branch_name,
              branch_index: $record.branch_index, sequence: $record.sequence,
              class: class($record.sequence), count: $row.record.branch_count,
              excess: ($row.record.branch_count - $record.branch_index)}] as $counted
        | [$counted[] | select(.excess < 0)] as $below
        | [$counted[] | select(($strict or .offset < $pre) and .excess != 0)] as $inexact
        | [$counted[] | select(.excess > ($volatile_rows[.branch_name] // 0))] as $beyond
        | [$counted | map(select(.class == "recovered")) | group_by(.branch_name)[]
           | select((map(.excess) | unique | length) > 1)
           | {branch_name: .[0].branch_name, excesses: (map(.excess) | unique)}] as $unsteady
        | [$input[].event_id | select(. as $id | $observed | bsearch($id) < 0)] as $missing
        | [$counted | group_by(.event_id)[] | select(length > 1)
           | {event_id: .[0].event_id, count: length, counts: map(.count),
              before_fault: (map(select(.offset < $pre)) | length)}] as $replays
        | [$replays[] | select($strict or .before_fault > 1)] as $violating_replays
        | [$counted[] | select(.class == "recovered")] as $recovered
        | [
            (if ($unexpected | length) > 0 then "output records the source never accepted" else empty end),
            (if ($incorrect | length) > 0 then "output records with wrong content or branch" else empty end),
            (if ($missing | length) > 0 then "accepted records missing from the guest output" else empty end),
            (if ($below | length) > 0 then "a guest count fell below the branch index: acknowledged input was lost from guest state" else empty end),
            (if ($inexact | length) > 0 then "a guest count differed from the branch index while no fault had occurred" else empty end),
            (if ($beyond | length) > 0 then "a guest count exceeded what the volatile interval could replay" else empty end),
            (if ($unsteady | length) > 0 then "guest counts after recovery did not advance by one per record" else empty end),
            (if ($violating_replays | length) > 0 then "records were emitted twice while no fault had occurred" else empty end),
            (if ($strict | not) and ($recovered | length) == 0
             then "no record produced after recovery was counted, so durability is unverified" else empty end)
          ] as $failures
        | {
            processor: "wasm processor",
            verdict: (if ($failures | length) == 0 then "pass" else "fail" end),
            fault: $b.fault,
            input_records: ($input | length),
            output_records: ($output | length),
            checks: {missing: ($missing | length), unexpected: ($unexpected | length),
                     incorrect_content: ($incorrect | length), below_branch_index: ($below | length),
                     inexact_without_a_fault: ($inexact | length), beyond_replay_bound: ($beyond | length),
                     unsteady_after_recovery: ($unsteady | length),
                     replays_without_a_fault: ($violating_replays | length)},
            replay_excess_after_recovery: ($recovered | group_by(.branch_name)
                                           | map({key: .[0].branch_name, value: (map(.excess) | max)})
                                           | from_entries),
            replay_duplicates: {records: ([$replays[].count - 1] | add // 0), events: ($replays | length)},
            durability: (if $strict then null else
              {milestone: "every input acknowledged before the fault is reflected in the restored guest count",
               counted_after_recovery: ($recovered | length)} end),
            missing_ids: $missing,
            unexpected: $unexpected,
            incorrect: $incorrect,
            below: $below,
            inexact: $inexact,
            beyond: $beyond,
            unsteady: $unsteady,
            replays: $replays,
            failures: $failures
          }' >"${result}"
    finish_verdict
}

case "${mode}" in
    dedup) dedup ;;
    window) window ;;
    enrich) enrich ;;
    counter) counter ;;
    -h | --help) usage; exit 0 ;;
    *) fail_usage "unknown verifier mode: ${mode}" ;;
esac
