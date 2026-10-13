#!/usr/bin/env bash
set -euo pipefail

# Self-test of the stateful and domain-time scenarios: their option checks, every processor verdict
# of verify-state-evidence.sh and every clock and window verdict of verify-clock-evidence.sh on
# passing evidence, on wrong-branch state and content, on incorrect restoration, on the permitted
# replay and volatile deviations, and on evidence that does not exercise the contract.

chaos_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
state="${chaos_dir}/verify-state-evidence.sh"
clock="${chaos_dir}/verify-clock-evidence.sh"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

fail() {
    printf 'stateful self-test failed: %s\n' "$*" >&2
    exit 1
}

expect_setup_rejection() {
    local case_name="$1"
    local expected="$2"
    shift 2
    local status=0
    "$@" >"${tmp_dir}/setup-rejection.out" 2>&1 || status=$?
    [[ "${status}" -eq 2 ]] || fail "${case_name} returned ${status}, expected setup error 2"
    grep -Fq -- "${expected}" "${tmp_dir}/setup-rejection.out" \
        || fail "${case_name} did not explain the rejected option"
}

# Runs a verifier that must accept its evidence.
expect_pass() {
    local case_name="$1"
    shift
    "$@" >"${tmp_dir}/pass.out" 2>&1 \
        || fail "${case_name} was rejected: $(cat "${tmp_dir}/pass.out")"
}

# Runs a verifier that must reject its evidence with verdict 1 and name FAILURE among its failures.
expect_failure() {
    local case_name="$1"
    local failure="$2"
    local result="$3"
    shift 3
    local status=0
    "$@" >"${tmp_dir}/failure.out" 2>&1 || status=$?
    [[ "${status}" -eq 1 ]] || fail "${case_name} returned ${status}, expected verdict 1"
    jq -e --arg failure "${failure}" 'any(.failures[]; contains($failure))' "${result}" >/dev/null \
        || fail "${case_name} did not report '${failure}': $(jq -c '.failures' "${result}")"
}

list_output="$("${chaos_dir}/chaos.sh" list)"
for scenario in stateful domain-time; do
    grep -Fq "${scenario}" <<<"${list_output}" || fail "scenario list omits ${scenario}"
done
expect_setup_rejection 'stateful fault is validated' '--fault for stateful must be none, owner-crash' \
    "${chaos_dir}/run-baseline.sh" --scenario stateful --image fixture --fault voter-crash
expect_setup_rejection 'domain-time fault is validated' '--fault for domain-time must be none, voter-crash' \
    "${chaos_dir}/run-baseline.sh" --scenario domain-time --image fixture --fault owner-crash
expect_setup_rejection 'one node supports only restart faults' 'owner-crash requires --nodes 3' \
    "${chaos_dir}/run-baseline.sh" --scenario stateful --image fixture --nodes 1 --fault owner-crash
expect_setup_rejection 'one node rejects voter rotations' 'voter-stop requires --nodes 3' \
    "${chaos_dir}/run-baseline.sh" --scenario domain-time --image fixture --nodes 1 --fault voter-stop
expect_setup_rejection 'other scenarios reject --fault' '--fault applies only to stateful and domain-time' \
    "${chaos_dir}/run-baseline.sh" --scenario baseline --image fixture --fault none

# One hundred and twenty stateful records: sequences below 52 precede the milestone, 52 through 59
# form the volatile interval and the rest were produced after recovery. Each branch then holds 26
# rows at the milestone, two of them in an open window of 12.
input="${tmp_dir}/input.ndjson"
jq -nc --arg run_id self --argjson count 120 -f "${chaos_dir}/fixtures/generate-stateful.jq" >"${input}"
boundaries() {
    local fault="$1"
    local unique="$2"
    local windows="$3"
    local enriched="$4"
    local counted="$5"
    local output="$6"
    jq -n --arg fault "${fault}" --argjson unique "${unique}" --argjson windows "${windows}" \
        --argjson enriched "${enriched}" --argjson counted "${counted}" '
        {fault: $fault, milestone_records: 52, recovered_records: 60,
         pre_fault_outputs: {chaos_unique_output: $unique, chaos_window_output: $windows,
                             chaos_enriched_output: $enriched, chaos_counted_output: $counted},
         profiles: {loaded: 1, milestone: 2, volatile: 3}}' >"${output}"
}

# The exact outputs of a run without a fault.
jq -c -s '
    (map({key: (.branch_name + "/" + .dedup_key), value: .sequence}) | group_by(.key)
     | map({key: .[0].key, value: (map(.value) | min)}) | from_entries) as $first
    | .[] | select(.sequence == $first[.branch_name + "/" + .dedup_key])' "${input}" >"${tmp_dir}/unique.ndjson"
jq -c -s '
    group_by(.branch_name)[] | sort_by(.branch_index) | . as $rows
    | range(0; (length / 12 | floor)) as $window
    | $rows[($window * 12):($window * 12 + 12)]
    | {branch_name: .[0].branch_name, records: length, first_index: .[0].branch_index,
       last_index: .[-1].branch_index,
       membership: (map(pow(4; .branch_index % 24)) | add), sequence_sum: (map(.sequence) | add)}' \
    "${input}" >"${tmp_dir}/windows-by-branch.ndjson"
jq -c -s 'sort_by(.last_index, .branch_name)[]' "${tmp_dir}/windows-by-branch.ndjson" >"${tmp_dir}/windows.ndjson"
jq -c '. + {profile_branch: .branch_name, profile_version: (if .sequence < 40 then 1 else 2 end)}' \
    "${input}" >"${tmp_dir}/enriched.ndjson"
jq -c '. + {branch_count: .branch_index}' "${input}" >"${tmp_dir}/counted.ndjson"
unique_count="$(wc -l <"${tmp_dir}/unique.ndjson")"
boundaries none "${unique_count}" 10 120 120 "${tmp_dir}/none.json"
for mode in dedup window enrich counter; do
    case "${mode}" in
        dedup) output="${tmp_dir}/unique.ndjson" ;;
        window) output="${tmp_dir}/windows.ndjson" ;;
        enrich) output="${tmp_dir}/enriched.ndjson" ;;
        counter) output="${tmp_dir}/counted.ndjson" ;;
    esac
    expect_pass "exact ${mode} output without a fault" "${state}" "${mode}" --input "${input}" \
        --output "${output}" --boundaries "${tmp_dir}/none.json" --result "${tmp_dir}/${mode}.json"
done

# Outputs emitted before a fault at sequence 56: every record below it, and the windows that closed.
pre_unique="$(jq -s 'map(select(.sequence < 56)) | length' "${tmp_dir}/unique.ndjson")"
boundaries owner-crash "${pre_unique}" 4 56 56 "${tmp_dir}/crash.json"

# Deduplicator verdicts.
expect_pass 'deduplicator output after a fault' "${state}" dedup --input "${input}" \
    --output "${tmp_dir}/unique.ndjson" --boundaries "${tmp_dir}/crash.json" --result "${tmp_dir}/dedup.json"
jq -e '.durability.later_occurrences_of_milestone_keys_after_recovery > 0' "${tmp_dir}/dedup.json" >/dev/null \
    || fail 'the deduplicator verdict did not count the milestone keys probed after recovery'
jq -c 'if .event_id == "self-state-10" then .branch_name = "beta" else . end' "${tmp_dir}/unique.ndjson" \
    >"${tmp_dir}/dedup-wrong-branch.ndjson"
expect_failure 'deduplicator output of the wrong branch' 'wrong content or branch' "${tmp_dir}/dedup.json" \
    "${state}" dedup --input "${input}" --output "${tmp_dir}/dedup-wrong-branch.ndjson" \
    --boundaries "${tmp_dir}/crash.json" --result "${tmp_dir}/dedup.json"
jq -c 'select(.event_id != "self-state-3")' "${tmp_dir}/unique.ndjson" >"${tmp_dir}/dedup-missing.ndjson"
expect_failure 'deduplicator lost a first occurrence' 'first occurrences missing' "${tmp_dir}/dedup.json" \
    "${state}" dedup --input "${input}" --output "${tmp_dir}/dedup-missing.ndjson" \
    --boundaries "${tmp_dir}/crash.json" --result "${tmp_dir}/dedup.json"
# Sequence 78 is alpha's index 40, whose key alpha first used at index 17, before the milestone.
{ cat "${tmp_dir}/unique.ndjson"; jq -c 'select(.sequence == 78)' "${input}"; } >"${tmp_dir}/dedup-forgotten.ndjson"
expect_failure 'deduplicator forgot a key durable at the milestone' 'seen before the milestone passed' \
    "${tmp_dir}/dedup.json" "${state}" dedup --input "${input}" --output "${tmp_dir}/dedup-forgotten.ndjson" \
    --boundaries "${tmp_dir}/crash.json" --result "${tmp_dir}/dedup.json"
# Sequence 86 is alpha's index 44, whose key alpha first used at index 41, after recovery.
{ cat "${tmp_dir}/unique.ndjson"; jq -c 'select(.sequence == 86)' "${input}"; } >"${tmp_dir}/dedup-recovered.ndjson"
expect_failure 'deduplicator forgot a key first seen after recovery' 'first seen after recovery passed' \
    "${tmp_dir}/dedup.json" "${state}" dedup --input "${input}" --output "${tmp_dir}/dedup-recovered.ndjson" \
    --boundaries "${tmp_dir}/crash.json" --result "${tmp_dir}/dedup.json"
# Sequence 62 is alpha's index 32, whose key alpha first used at index 29, sequence 56: volatile.
{ cat "${tmp_dir}/unique.ndjson"; jq -c 'select(.sequence == 62)' "${input}"; } >"${tmp_dir}/dedup-volatile.ndjson"
expect_pass 'deduplicator forgot a key of the volatile interval' "${state}" dedup --input "${input}" \
    --output "${tmp_dir}/dedup-volatile.ndjson" --boundaries "${tmp_dir}/crash.json" --result "${tmp_dir}/dedup.json"
jq -e '.volatile_reemissions == 1' "${tmp_dir}/dedup.json" >/dev/null \
    || fail 'the deduplicator verdict did not count the volatile re-emission separately'
{ cat "${tmp_dir}/unique.ndjson"; jq -c 'select(.sequence == 56)' "${tmp_dir}/unique.ndjson"; } \
    >"${tmp_dir}/dedup-replay.ndjson"
expect_pass 'deduplicator replayed a record after the fault' "${state}" dedup --input "${input}" \
    --output "${tmp_dir}/dedup-replay.ndjson" --boundaries "${tmp_dir}/crash.json" --result "${tmp_dir}/dedup.json"
jq -e '.replay_duplicates.records == 1' "${tmp_dir}/dedup.json" >/dev/null \
    || fail 'the deduplicator verdict did not count the replay duplicate separately'
expect_failure 'deduplicator replayed a record without a fault' 'emitted twice while no fault' \
    "${tmp_dir}/dedup.json" "${state}" dedup --input "${input}" --output "${tmp_dir}/dedup-replay.ndjson" \
    --boundaries "${tmp_dir}/none.json" --result "${tmp_dir}/dedup.json"
jq -c 'select(.sequence < 60)' "${input}" >"${tmp_dir}/input-short.ndjson"
jq -c 'select(.sequence < 60)' "${tmp_dir}/unique.ndjson" >"${tmp_dir}/unique-short.ndjson"
expect_failure 'deduplicator run without a probe after recovery' 'durability is unverified' \
    "${tmp_dir}/dedup.json" "${state}" dedup --input "${tmp_dir}/input-short.ndjson" \
    --output "${tmp_dir}/unique-short.ndjson" --boundaries "${tmp_dir}/crash.json" --result "${tmp_dir}/dedup.json"

# Window verdicts. Alpha's window of indexes 25 through 36 held 25 and 26 open at the milestone.
expect_pass 'window output after a fault' "${state}" window --input "${input}" \
    --output "${tmp_dir}/windows.ndjson" --boundaries "${tmp_dir}/crash.json" --result "${tmp_dir}/window.json"
jq -e '.durability.rows_open_at_milestone == 4 and .durability.restored_after_fault == 4' \
    "${tmp_dir}/window.json" >/dev/null || fail 'the window verdict did not cite the rows open at the milestone'
window_edit() {
    local filter="$1"
    local output="$2"
    jq -c --argjson input "$(jq -s '.' "${input}")" "${filter}" "${tmp_dir}/windows.ndjson" >"${output}"
}
# Alpha's index 31 is sequence 60; beta's index 31 is 61. Swapping the row leaves the membership.
window_edit 'if .branch_name == "alpha" and .first_index == 25 then .sequence_sum += 1 else . end' \
    "${tmp_dir}/window-wrong-branch.ndjson"
expect_failure 'window aggregated a row of the other branch' 'another branch or a wrong sum' \
    "${tmp_dir}/window.json" "${state}" window --input "${input}" --output "${tmp_dir}/window-wrong-branch.ndjson" \
    --boundaries "${tmp_dir}/crash.json" --result "${tmp_dir}/window.json"
# Index 25 of alpha, sequence 48, aggregated twice: once restored and once more, outside a replay.
window_edit 'if .branch_name == "alpha" and .first_index == 25
             then .membership += pow(4; 25 % 24) | .sequence_sum += 48 | .records += 1 | .last_index = 36
             else . end' "${tmp_dir}/window-double.ndjson"
expect_failure 'window aggregated a pre-milestone row twice' 'aggregated more than once' \
    "${tmp_dir}/window.json" "${state}" window --input "${input}" --output "${tmp_dir}/window-double.ndjson" \
    --boundaries "${tmp_dir}/crash.json" --result "${tmp_dir}/window.json"
# Index 25 of alpha lost from the restored window: the window closes one row later instead.
window_edit 'if .branch_name == "alpha" and .first_index == 25
             then .membership -= pow(4; 25 % 24) | .sequence_sum -= 48 | .first_index = 26
                  | .membership += pow(4; 37 % 24) | .sequence_sum += 72 | .last_index = 37
             elif .branch_name == "alpha" and .first_index == 37
             then .membership -= pow(4; 37 % 24) | .sequence_sum -= 72 | .first_index = 38
                  | .membership += pow(4; 49 % 24) | .sequence_sum += 96 | .last_index = 49
             elif .branch_name == "alpha" and .first_index == 49
             then .membership -= pow(4; 49 % 24) | .sequence_sum -= 96 | .first_index = 50
             else . end' "${tmp_dir}/window-lost.ndjson"
jq -c 'if .branch_name == "alpha" and .first_index == 50 then .records = 12 | .last_index = 61
       | .membership += pow(4; 61 % 24) | .sequence_sum += 120 else . end' "${tmp_dir}/window-lost.ndjson" \
    >"${tmp_dir}/window-lost-closed.ndjson"
jq -c 'select(.sequence < 120)' "${input}" >"${tmp_dir}/input-window-lost.ndjson"
jq -nc --arg run_id self --argjson count 122 -f "${chaos_dir}/fixtures/generate-stateful.jq" \
    >"${tmp_dir}/input-window-lost.ndjson"
expect_failure 'window lost a row admitted before the milestone' 'admitted before the milestone were lost' \
    "${tmp_dir}/window.json" "${state}" window --input "${tmp_dir}/input-window-lost.ndjson" \
    --output "${tmp_dir}/window-lost-closed.ndjson" --boundaries "${tmp_dir}/crash.json" --result "${tmp_dir}/window.json"
# Index 27 of alpha, sequence 52, replayed into its window: a volatile row may be aggregated twice.
window_edit 'if .branch_name == "alpha" and .first_index == 25
             then .membership += pow(4; 27 % 24) | .sequence_sum += 52 | .records += 1 else . end' \
    "${tmp_dir}/window-replay.ndjson"
jq -c 'if .records == 13 then .records = 12 | .membership -= pow(4; 36 % 24) | .sequence_sum -= 70
       | .last_index = 35 else . end' "${tmp_dir}/window-replay.ndjson" >"${tmp_dir}/window-replay-12.ndjson"
jq -c 'if .branch_name == "alpha" and .first_index == 37
       then .first_index = 36 | .membership += pow(4; 36 % 24) - pow(4; 48 % 24) | .sequence_sum += 70 - 94
            | .last_index = 47
       elif .branch_name == "alpha" and .first_index == 49
       then .first_index = 48 | .membership += pow(4; 48 % 24) - pow(4; 60 % 24) | .sequence_sum += 94 - 118
            | .last_index = 59
       else . end' "${tmp_dir}/window-replay-12.ndjson" >"${tmp_dir}/window-replay-shifted.ndjson"
expect_pass 'window replayed a row of the volatile interval' "${state}" window --input "${input}" \
    --output "${tmp_dir}/window-replay-shifted.ndjson" --boundaries "${tmp_dir}/crash.json" \
    --result "${tmp_dir}/window.json"
jq -e '.replay_duplicates.rows == 1' "${tmp_dir}/window.json" >/dev/null \
    || fail 'the window verdict did not count the replayed volatile row separately'
window_edit 'if .branch_name == "beta" and .first_index == 13 then .last_index = 40 else . end' \
    "${tmp_dir}/window-undecodable.ndjson"
expect_failure 'window membership that cannot be decoded' 'cannot be decoded' \
    "${tmp_dir}/window.json" "${state}" window --input "${input}" --output "${tmp_dir}/window-undecodable.ndjson" \
    --boundaries "${tmp_dir}/crash.json" --result "${tmp_dir}/window.json"
boundaries owner-crash "${pre_unique}" 6 56 56 "${tmp_dir}/crash-late.json"
expect_failure 'window open at the milestone closed before the fault' 'closed before the fault' \
    "${tmp_dir}/window.json" "${state}" window --input "${input}" --output "${tmp_dir}/windows.ndjson" \
    --boundaries "${tmp_dir}/crash-late.json" --result "${tmp_dir}/window.json"

# Materialized relay verdicts.
expect_pass 'enrichment after a fault' "${state}" enrich --input "${input}" \
    --output "${tmp_dir}/enriched.ndjson" --boundaries "${tmp_dir}/crash.json" --result "${tmp_dir}/enrich.json"
jq -c 'if .event_id == "self-state-70" then .profile_branch = "beta" else . end' "${tmp_dir}/enriched.ndjson" \
    >"${tmp_dir}/enrich-wrong-branch.ndjson"
expect_failure "enrichment with another branch's state" "another branch's materialized state" \
    "${tmp_dir}/enrich.json" "${state}" enrich --input "${input}" --output "${tmp_dir}/enrich-wrong-branch.ndjson" \
    --boundaries "${tmp_dir}/crash.json" --result "${tmp_dir}/enrich.json"
jq -c 'if .sequence == 80 then .profile_branch = "none" | .profile_version = 0 else . end' \
    "${tmp_dir}/enriched.ndjson" >"${tmp_dir}/enrich-default.ndjson"
expect_failure 'enrichment that fell back to the default' 'default instead of materialized state' \
    "${tmp_dir}/enrich.json" "${state}" enrich --input "${input}" --output "${tmp_dir}/enrich-default.ndjson" \
    --boundaries "${tmp_dir}/crash.json" --result "${tmp_dir}/enrich.json"
jq -c 'if .sequence >= 70 then .profile_version = 1 else . end' "${tmp_dir}/enriched.ndjson" \
    >"${tmp_dir}/enrich-regressed.ndjson"
expect_failure 'materialized state restored below the milestone' 'regressed below the version durable' \
    "${tmp_dir}/enrich.json" "${state}" enrich --input "${input}" --output "${tmp_dir}/enrich-regressed.ndjson" \
    --boundaries "${tmp_dir}/crash.json" --result "${tmp_dir}/enrich.json"
jq -c 'if .sequence >= 54 and .sequence < 66 then .profile_version = 3 else . end' "${tmp_dir}/enriched.ndjson" \
    >"${tmp_dir}/enrich-volatile.ndjson"
expect_pass 'materialized state lost its volatile version' "${state}" enrich --input "${input}" \
    --output "${tmp_dir}/enrich-volatile.ndjson" --boundaries "${tmp_dir}/crash.json" --result "${tmp_dir}/enrich.json"
jq -c 'select(.event_id != "self-state-90")' "${tmp_dir}/enriched.ndjson" >"${tmp_dir}/enrich-missing.ndjson"
expect_failure 'enrichment lost an accepted record' 'missing from the enriched output' \
    "${tmp_dir}/enrich.json" "${state}" enrich --input "${input}" --output "${tmp_dir}/enrich-missing.ndjson" \
    --boundaries "${tmp_dir}/crash.json" --result "${tmp_dir}/enrich.json"

# WASM processor verdicts.
expect_pass 'guest counts after a fault' "${state}" counter --input "${input}" \
    --output "${tmp_dir}/counted.ndjson" --boundaries "${tmp_dir}/crash.json" --result "${tmp_dir}/counter.json"
jq -c 'if .branch_name == "alpha" and .sequence >= 60 then .branch_count = .branch_index - 30 else . end' \
    "${tmp_dir}/counted.ndjson" >"${tmp_dir}/counter-reset.ndjson"
expect_failure 'guest state reset after the fault' 'acknowledged input was lost from guest state' \
    "${tmp_dir}/counter.json" "${state}" counter --input "${input}" --output "${tmp_dir}/counter-reset.ndjson" \
    --boundaries "${tmp_dir}/crash.json" --result "${tmp_dir}/counter.json"
jq -c 'if .sequence >= 56 then .branch_count = .branch_index + 2 else . end' "${tmp_dir}/counted.ndjson" \
    >"${tmp_dir}/counter-replayed.ndjson"
expect_pass 'guest counted replayed volatile input again' "${state}" counter --input "${input}" \
    --output "${tmp_dir}/counter-replayed.ndjson" --boundaries "${tmp_dir}/crash.json" --result "${tmp_dir}/counter.json"
jq -e '.replay_excess_after_recovery.alpha == 2' "${tmp_dir}/counter.json" >/dev/null \
    || fail 'the guest verdict did not report the replay excess after recovery'
jq -c 'if .sequence >= 56 then .branch_count = .branch_index + 5 else . end' "${tmp_dir}/counted.ndjson" \
    >"${tmp_dir}/counter-beyond.ndjson"
expect_failure 'guest count beyond what the volatile interval replays' 'exceeded what the volatile interval' \
    "${tmp_dir}/counter.json" "${state}" counter --input "${input}" --output "${tmp_dir}/counter-beyond.ndjson" \
    --boundaries "${tmp_dir}/crash.json" --result "${tmp_dir}/counter.json"
jq -c 'if .sequence == 20 then .branch_count += 1 else . end' "${tmp_dir}/counted.ndjson" \
    >"${tmp_dir}/counter-inexact.ndjson"
expect_failure 'guest count wrong before the fault' 'differed from the branch index while no fault' \
    "${tmp_dir}/counter.json" "${state}" counter --input "${input}" --output "${tmp_dir}/counter-inexact.ndjson" \
    --boundaries "${tmp_dir}/crash.json" --result "${tmp_dir}/counter.json"
jq -c 'if .sequence == 100 then .branch_count += 1 else . end' "${tmp_dir}/counted.ndjson" \
    >"${tmp_dir}/counter-unsteady.ndjson"
expect_failure 'guest counts after recovery skipped' 'did not advance by one per record' \
    "${tmp_dir}/counter.json" "${state}" counter --input "${input}" --output "${tmp_dir}/counter-unsteady.ndjson" \
    --boundaries "${tmp_dir}/crash.json" --result "${tmp_dir}/counter.json"
jq -c 'if .event_id == "self-state-44" then .content = "tampered" else . end' "${tmp_dir}/counted.ndjson" \
    >"${tmp_dir}/counter-content.ndjson"
expect_failure 'guest output with changed content' 'wrong content or branch' \
    "${tmp_dir}/counter.json" "${state}" counter --input "${input}" --output "${tmp_dir}/counter-content.ndjson" \
    --boundaries "${tmp_dir}/crash.json" --result "${tmp_dir}/counter.json"
status=0
"${state}" counter --input "${input}" --output "${tmp_dir}/counted.ndjson" \
    --boundaries "${tmp_dir}/missing.json" --result "${tmp_dir}/counter.json" >/dev/null 2>&1 || status=$?
[[ "${status}" -eq 2 ]] || fail "missing boundaries returned ${status}, expected usage error 2"

# Clock verdicts over three observers, one tick every 250 ms of physical time from a mapping at
# rate 4 and period 1 s. Rounds fault nervix-1, nervix-2 and nervix-3 in turn; removing nervix-2
# stalls every tick for 12 seconds.
origin_seconds=1790000000
clock_line() {
    local ms="$1"
    local id="$2"
    local lead_ms="${3:-50}"
    local boundary_seconds=$((origin_seconds + id - 1))
    local logical_ms=$(((id - 1) * 1000 + lead_ms))
    printf '%d%06d [events] domain clock [chaos_paced] tick: generation 1, id %d, boundary %s, authority UTC %s, node logical %s\n' \
        "${ms}" 0 "${id}" \
        "$(date -u -d "@${boundary_seconds}" +%Y-%m-%dT%H:%M:%S).250Z" \
        "$(date -u -d "@$((ms / 1000))" +%Y-%m-%dT%H:%M:%S).000Z" \
        "$(date -u -d "@$((origin_seconds + logical_ms / 1000))" +%Y-%m-%dT%H:%M:%S).$(printf '%03d' $(((250 + logical_ms % 1000) % 1000)))Z"
}
attach_line() {
    local ms="$1"
    printf '%d%06d attached to the clock of domain '"'"'chaos_paced'"'"': generation 1, paced: period 1s, skew 1s, logical origin %s.250Z, UTC anchor 2026-09-01T00:00:00Z, time rate 4\n' \
        "${ms}" 0 "$(date -u -d "@${origin_seconds}" +%Y-%m-%dT%H:%M:%S)"
}
start_ms=1791000000000
write_observer() {
    local output="$1"
    local stall_from_ms="$2"
    local stall_to_ms="$3"
    jq -nr --argjson start "${start_ms}" --argjson origin "${origin_seconds}" \
        --argjson from "${stall_from_ms}" --argjson to "${stall_to_ms}" '
        def at($seconds; $fraction): $seconds | todate | sub("Z$"; "." + $fraction + "Z");
        def stamp($ms): ($ms | tostring) + "000000";
        (stamp($start) + " attached to the clock of domain '"'"'chaos_paced'"'"': generation 1, paced: period 1s, skew 1s, logical origin "
         + at($origin; "250") + ", UTC anchor 2026-09-01T00:00:00Z, time rate 4"),
        (range(0; 480) as $step | ($start + $step * 250) as $ms
         | select(($ms > $from and $ms < $to) | not)
         | ($step + 1) as $id
         | stamp($ms) + " [events] domain clock [chaos_paced] tick: generation 1, id " + ($id | tostring)
           + ", boundary " + at($origin + $id - 1; "250")
           + ", authority UTC " + at($ms / 1000 | floor; "000")
           + ", node logical " + at($origin + $id - 1; "300"))' >"${output}"
}
write_rounds() {
    local output="$1"
    : >"${output}"
    local ordinal
    for ordinal in 1 2 3; do
        local since=$((start_ms + ordinal * 30000))
        jq -nc --argjson ordinal "${ordinal}" --arg target "nervix-${ordinal}" \
            --argjson since "${since}" '
            {ordinal: $ordinal, kind: "crash", target: $target, graph_node: "node-1",
             relocation_started_ns: (($since - 5000) * 1000000), relocation_completed_ns: (($since - 4000) * 1000000),
             fault_since_ns: ($since * 1000000), fault_ended_ns: (($since + 8000) * 1000000),
             recovered_ns: (($since + 20000) * 1000000), stop_ms: null}' >>"${output}"
    done
}
write_rounds "${tmp_dir}/rounds.ndjson"
stall_from=$((start_ms + 60000))
stall_to=$((start_ms + 72000))
for host in nervix-1 nervix-2 nervix-3; do
    write_observer "${tmp_dir}/clock-${host}.log" "${stall_from}" "${stall_to}"
done
clock_args=(--observer "nervix-1=${tmp_dir}/clock-nervix-1.log" --observer "nervix-2=${tmp_dir}/clock-nervix-2.log"
    --observer "nervix-3=${tmp_dir}/clock-nervix-3.log" --period-ms 1000 --rate 4)
expect_pass 'clock through a voter rotation that removed the authority' "${clock}" clock "${clock_args[@]}" \
    --rounds "${tmp_dir}/rounds.ndjson" --fault voter-crash --result "${tmp_dir}/clock.json"
jq -e '.authority_rounds == [2] and (.authority_losses | length) == 2' "${tmp_dir}/clock.json" >/dev/null \
    || fail "the clock verdict did not attribute the authority stall to round 2: $(jq -c '.authority_rounds' "${tmp_dir}/clock.json")"
# The survivors reported nervix-2 unavailable 5 seconds into its 12-second stall: replaced in time.
jq -c --argjson marked $((stall_from + 5000)) --argjson ended $((stall_from + 18000)) \
    'if .ordinal == 2 then .unavailable_ms = $marked | .fault_ended_ns = ($ended * 1000000) else . end' \
    "${tmp_dir}/rounds.ndjson" >"${tmp_dir}/rounds-marked.ndjson"
expect_pass 'clock authority replaced soon after its node was reported unavailable' "${clock}" clock \
    "${clock_args[@]}" --rounds "${tmp_dir}/rounds-marked.ndjson" --fault voter-crash --result "${tmp_dir}/clock.json"
# Reported unavailable one second into the stall, the authority stayed stalled 11 more seconds.
jq -c --argjson marked $((stall_from + 1000)) --argjson ended $((stall_from + 18000)) \
    'if .ordinal == 2 then .unavailable_ms = $marked | .fault_ended_ns = ($ended * 1000000) else . end' \
    "${tmp_dir}/rounds.ndjson" >"${tmp_dir}/rounds-marked.ndjson"
expect_failure 'clock authority not replaced after its node was reported unavailable' 'no other voter took the clock over' \
    "${tmp_dir}/clock.json" "${clock}" clock "${clock_args[@]}" --rounds "${tmp_dir}/rounds-marked.ndjson" \
    --fault voter-crash --result "${tmp_dir}/clock.json"
# Ticks continued after the availability observation, then a four-second stall began later in the
# same recovery round. The replacement bound measures the stalled interval after that observation.
jq -c --argjson marked $((start_ms + 62000)) --argjson ended $((start_ms + 88000)) \
    --argjson recovered $((start_ms + 89000)) \
    'if .ordinal == 2 then .unavailable_ms = $marked | .fault_ended_ns = ($ended * 1000000)
     | .recovered_ns = ($recovered * 1000000) else . end' \
    "${tmp_dir}/rounds.ndjson" >"${tmp_dir}/rounds-marked.ndjson"
for host in nervix-1 nervix-2 nervix-3; do
    write_observer "${tmp_dir}/clock-${host}.log" $((start_ms + 70000)) $((start_ms + 74000))
done
expect_pass 'clock kept ticking before a later bounded stall' "${clock}" clock "${clock_args[@]}" \
    --rounds "${tmp_dir}/rounds-marked.ndjson" --fault voter-crash --result "${tmp_dir}/clock.json"
for host in nervix-1 nervix-2 nervix-3; do
    write_observer "${tmp_dir}/clock-${host}.log" $((start_ms + 70000)) $((start_ms + 82000))
done
expect_failure 'a later clock stall exceeded the replacement bound' 'no other voter took the clock over' \
    "${tmp_dir}/clock.json" "${clock}" clock "${clock_args[@]}" --rounds "${tmp_dir}/rounds-marked.ndjson" \
    --fault voter-crash --result "${tmp_dir}/clock.json"
# Ticks already demonstrated recovery while the voter was absent. Its return can install another
# authority and wait for runtime readiness; this later gap still has the 60-second authority bound.
jq -c --argjson ended $((start_ms + 70250)) \
    'if .ordinal == 2 then .fault_ended_ns = ($ended * 1000000) else . end' \
    "${tmp_dir}/rounds-marked.ndjson" >"${tmp_dir}/rounds-returned.ndjson"
expect_pass 'clock authority installation after a voter returned' "${clock}" clock "${clock_args[@]}" \
    --rounds "${tmp_dir}/rounds-returned.ndjson" --fault voter-crash --result "${tmp_dir}/clock.json"
for host in nervix-1 nervix-2 nervix-3; do
    write_observer "${tmp_dir}/clock-${host}.log" 0 0
done
expect_failure 'voter rotation that never stalled the ticks' 'authority recovery is unverified' \
    "${tmp_dir}/clock.json" "${clock}" clock "${clock_args[@]}" --rounds "${tmp_dir}/rounds.ndjson" \
    --fault voter-crash --result "${tmp_dir}/clock.json"
: >"${tmp_dir}/no-rounds.ndjson"
expect_pass 'clock without a fault' "${clock}" clock "${clock_args[@]}" --rounds "${tmp_dir}/no-rounds.ndjson" \
    --fault none --result "${tmp_dir}/clock.json"
write_observer "${tmp_dir}/clock-nervix-2.log" $((start_ms + 10000)) $((start_ms + 15000))
expect_failure 'ticks stalled while no fault was held' 'stalled while no fault was held' \
    "${tmp_dir}/clock.json" "${clock}" clock "${clock_args[@]}" --rounds "${tmp_dir}/no-rounds.ndjson" \
    --fault none --result "${tmp_dir}/clock.json"
write_observer "${tmp_dir}/clock-nervix-2.log" 0 0
sed '3s/generation 1, id 2,/generation 2, id 2,/' "${tmp_dir}/clock-nervix-1.log" >"${tmp_dir}/clock-generation.log"
expect_failure 'tick of another generation' 'another generation' "${tmp_dir}/clock.json" \
    "${clock}" clock --observer "nervix-1=${tmp_dir}/clock-generation.log" --period-ms 1000 --rate 4 \
    --rounds "${tmp_dir}/no-rounds.ndjson" --fault none --result "${tmp_dir}/clock.json"
sed '4s/\.250Z, authority/.750Z, authority/' "${tmp_dir}/clock-nervix-1.log" >"${tmp_dir}/clock-grid.log"
expect_failure 'tick off the period grid' 'left the period grid' "${tmp_dir}/clock.json" \
    "${clock}" clock --observer "nervix-1=${tmp_dir}/clock-grid.log" --period-ms 1000 --rate 4 \
    --rounds "${tmp_dir}/no-rounds.ndjson" --fault none --result "${tmp_dir}/clock.json"
{ head -n 40 "${tmp_dir}/clock-nervix-1.log"; clock_line $((start_ms + 9800)) 20; } >"${tmp_dir}/clock-decreasing.log"
expect_failure 'tick ids going back within an attachment' 'did not increase' "${tmp_dir}/clock.json" \
    "${clock}" clock --observer "nervix-1=${tmp_dir}/clock-decreasing.log" --period-ms 1000 --rate 4 \
    --rounds "${tmp_dir}/no-rounds.ndjson" --fault none --result "${tmp_dir}/clock.json"
{ attach_line "${start_ms}"; clock_line $((start_ms + 250)) 2 -500; } >"${tmp_dir}/clock-ahead.log"
expect_failure 'tick ahead of the serving node' "ahead of, or far behind" "${tmp_dir}/clock.json" \
    "${clock}" clock --observer "nervix-1=${tmp_dir}/clock-ahead.log" --period-ms 1000 --rate 4 \
    --rounds "${tmp_dir}/no-rounds.ndjson" --fault none --result "${tmp_dir}/clock.json"
{ cat "${tmp_dir}/clock-nervix-1.log"; printf '%d%06d [events] domain clock [chaos_paced]: generation 1, paced: period 1s, skew 1s, logical origin 2026-01-01T00:00:00Z, UTC anchor 2026-09-01T00:00:00Z, time rate 4\n' $((start_ms + 200000)) 0; } \
    >"${tmp_dir}/clock-remapped.log"
expect_failure 're-attached clock with another mapping' 'another generation or mapping' "${tmp_dir}/clock.json" \
    "${clock}" clock --observer "nervix-1=${tmp_dir}/clock-remapped.log" --period-ms 1000 --rate 4 \
    --rounds "${tmp_dir}/no-rounds.ndjson" --fault none --result "${tmp_dir}/clock.json"

# Window verdicts against the clock verdict of the rotation that removed the authority.
for host in nervix-1 nervix-2 nervix-3; do
    write_observer "${tmp_dir}/clock-${host}.log" "${stall_from}" "${stall_to}"
done
"${clock}" clock "${clock_args[@]}" --rounds "${tmp_dir}/rounds.ndjson" --fault voter-crash \
    --result "${tmp_dir}/clock-rotation.json" >/dev/null 2>&1 || fail 'the rotation clock evidence was rejected'
write_windows() {
    local output="$1"
    local span_ms="$2"
    local skip_from="$3"
    local skip_to="$4"
    jq -nr --argjson start "${start_ms}" --argjson origin "${origin_seconds}" --argjson span "${span_ms}" \
        --argjson from "${skip_from}" --argjson to "${skip_to}" '
        def at($ms): (($ms / 1000 | floor) | todate | sub("Z$"; "")) + "."
            + ("000000000" + (($ms % 1000) * 1000000 | tostring))[-9:] + "+00:00";
        range(0; 60) as $step | ($start + $step * 2000) as $ms
        | select(($ms >= $from and $ms <= $to) | not)
        | (($origin + ($ms / 1000 | floor)) * 1000) as $opened
        | (["alpha", 0], ["beta", 1]) as [$branch, $offset]
        | ($ms | tostring) + " "
          + ({branch_name: $branch, records: 4, first_sequence: ($step * 8 + $offset),
              last_sequence: ($step * 8 + $offset + 6), opened_at: at($opened),
              closed_at: at($opened + $span)} | tojson)' >"${output}"
}
window_args=(--rounds "${tmp_dir}/rounds.ndjson" --clock "${tmp_dir}/clock-rotation.json" --fault voter-crash
    --width-ms 8000 --rate 4)
write_windows "${tmp_dir}/windows.log" 8020 0 0
expect_pass 'paced windows through an authority stall' "${clock}" windows --output "${tmp_dir}/windows.log" \
    "${window_args[@]}" --result "${tmp_dir}/windows.json"
jq -e '.closed_during_authority_losses | length == 2 and all(.[]; .windows_closed > 0)' "${tmp_dir}/windows.json" \
    >/dev/null || fail 'the window verdict did not count windows closed during the authority stall'
write_windows "${tmp_dir}/windows-early.log" 7900 0 0
expect_failure 'paced windows closed early' 'before their logical width' "${tmp_dir}/windows.json" \
    "${clock}" windows --output "${tmp_dir}/windows-early.log" "${window_args[@]}" --result "${tmp_dir}/windows.json"
write_windows "${tmp_dir}/windows-late.log" 20000 0 0
expect_failure 'paced windows closed late away from any move' 'later than the lateness bound' \
    "${tmp_dir}/windows.json" "${clock}" windows --output "${tmp_dir}/windows-late.log" "${window_args[@]}" \
    --result "${tmp_dir}/windows.json"
write_windows "${tmp_dir}/windows-stopped.log" 8020 "${stall_from}" "${stall_to}"
expect_failure 'logical deadlines stopped with the ticks' 'stopped while an authority stall' \
    "${tmp_dir}/windows.json" "${clock}" windows --output "${tmp_dir}/windows-stopped.log" "${window_args[@]}" \
    --result "${tmp_dir}/windows.json"
# A graceful stop of the authority that also owned the graph: the graph's own outage excuses the
# windows that did not close during the stall.
jq -c 'if .ordinal == 2 then .kind = "stop" | .graph_node = "node-2" else . end' "${tmp_dir}/rounds.ndjson" \
    >"${tmp_dir}/rounds-stop.ndjson"
"${clock}" clock "${clock_args[@]}" --rounds "${tmp_dir}/rounds-stop.ndjson" --fault voter-stop \
    --result "${tmp_dir}/clock-stop.json" >/dev/null 2>&1 || fail 'the stop rotation clock evidence was rejected'
expect_pass 'paced windows paused while the stopped authority owned the graph' "${clock}" windows \
    --output "${tmp_dir}/windows-stopped.log" --rounds "${tmp_dir}/rounds-stop.ndjson" \
    --clock "${tmp_dir}/clock-stop.json" --fault voter-stop --width-ms 8000 --rate 4 --result "${tmp_dir}/windows.json"
jq -e '.closed_during_authority_losses | length == 2 and all(.[]; .graph_down and .windows_closed == 0)' \
    "${tmp_dir}/windows.json" >/dev/null || fail 'the window verdict did not excuse the stall of the graph'"'"'s own node'
sed '5s/"first_sequence":16,/"first_sequence":17,/' "${tmp_dir}/windows.log" >"${tmp_dir}/windows-mixed.log"
expect_failure 'paced window with rows of both branches' 'mixed rows of the two branches' \
    "${tmp_dir}/windows.json" "${clock}" windows --output "${tmp_dir}/windows-mixed.log" "${window_args[@]}" \
    --result "${tmp_dir}/windows.json"

printf 'stateful and domain-time self-test passed\n'
