#!/usr/bin/env bash
set -euo pipefail

# Self-test of the lifetimes scenario: its option checks and every verdict of
# verify-lifetimes-evidence.sh on passing evidence, on records enriched with another or a superseded
# profile, on lost, repeated and altered records, on generated rows of a state never held or going
# back, on malformed and missing windows, on unexercised evictions and expiry, on buffered records
# that were not held or arrived only at their flush interval, and on pooled tables missing or
# altering a row or a pool wait that went unreported or exceeded the pool.

chaos_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
verifier="${chaos_dir}/verify-lifetimes-evidence.sh"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

fail() {
    printf 'lifetimes self-test failed: %s\n' "$*" >&2
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

expect_pass() {
    local case_name="$1"
    shift
    "$@" >"${tmp_dir}/pass.out" 2>&1 \
        || fail "${case_name} was rejected: $(cat "${tmp_dir}/pass.out") $(cat "${tmp_dir}/result.json" 2>/dev/null)"
}

expect_failure() {
    local case_name="$1"
    local failure="$2"
    shift 2
    local status=0
    "$@" >"${tmp_dir}/failure.out" 2>&1 || status=$?
    [[ "${status}" -eq 1 ]] || fail "${case_name} returned ${status}, expected verdict 1: $(cat "${tmp_dir}/failure.out")"
    jq -e --arg failure "${failure}" 'any(.failures[]; contains($failure))' "${tmp_dir}/result.json" >/dev/null \
        || fail "${case_name} did not report '${failure}': $(jq -c '.failures' "${tmp_dir}/result.json")"
}

list_output="$("${chaos_dir}/chaos.sh" list)"
grep -Fq 'lifetimes' <<<"${list_output}" || fail 'scenario list omits lifetimes'
expect_setup_rejection 'lifetimes fault is validated' '--fault for lifetimes must be none, owner-crash' \
    "${chaos_dir}/run-baseline.sh" --scenario lifetimes --image fixture --fault owner-pause
expect_setup_rejection 'one node supports only restart faults' 'owner-partition requires --nodes 3' \
    "${chaos_dir}/run-baseline.sh" --scenario lifetimes --image fixture --nodes 1 --fault owner-partition

generate() {
    jq -nc --arg run_id run --arg kind "$1" --argjson first "$2" --argjson count "$3" \
        -f "${chaos_dir}/fixtures/generate-lifetimes.jq"
}

# A run without a fault: 100 life records, red replaced at records 24, 40 (ignored lower), 56
# (ignored equal) and 72, then held at version 5 past the milestone of 88.
traffic="${tmp_dir}/traffic"
mkdir -p "${traffic}"
generate life 1 100 >"${traffic}/life-accepted-input.ndjson"
boundaries_for() {
    local fault="$1"
    local recovered="$2"
    local lru="$3"
    local ttl_after="$4"
    local held="$5"
    jq -n --arg fault "${fault}" --argjson recovered "${recovered}" --argjson lru "${lru}" \
        --argjson ttl_after "${ttl_after}" --argjson held "${held}" '
        {fault: $fault,
         boundaries: {parked_until: 24, replaced_until: 40, lower_until: 56, equal_until: 72,
                      milestone_records: 88, recovered_records: $recovered},
         red_versions: [{until: 24, version: 1}, {until: 40, version: 2}, {until: 56, version: 2},
                        {until: 72, version: 2}, {until: 88, version: 5}],
         red_latest: 5, red_superseded: [3, 4],
         expiring: {before: 8, after: 8, ttl_evictions_before: 1, ttl_evictions_after: $ttl_after},
         late_records: 5, flush_records: 6, lru_evictions: $lru,
         force_flush: {produced_ms: 1000, alteration_started_ms: 15000, alteration_completed_ms: 16000,
                       arrived_ms: 17000, flush_interval_ms: 300000, output_before_alteration: $held,
                       admission_observed: true},
         pool: {records: 12, waiting_emitter: "chaos_life_pool_second", waiting_after_fault: null,
                connections_while_waiting: [{address: "172.30.0.7", connections: 1}]}}'
}
boundaries="${tmp_dir}/boundaries.json"
boundaries_for none 88 12 3 0 >"${boundaries}"
jq -c --slurpfile b "${boundaries}" '
    $b[0] as $b
    | (if .branch_name == "red" then
         (. as $r | first($b.red_versions[] | select($r.sequence <= .until) | .version) // $b.red_latest)
       else 1 end) as $v
    | . + {profile_version: $v, label: "\(.branch_name)-v\($v)"}
' "${traffic}/life-accepted-input.ndjson" >"${traffic}/chaos_life_enriched_output.ndjson"
{
    for version in 1 1 2 2 5 5; do printf '{"branch_name":"red","profile_version":%d,"label":"red-v%d"}\n' "${version}" "${version}"; done
    for branch in blue gold green; do
        for _ in 1 2 3; do printf '{"branch_name":"%s","profile_version":1,"label":"%s-v1"}\n' "${branch}" "${branch}"; done
    done
} >"${traffic}/chaos_life_generated_output.ndjson"
jq -c '.' "${traffic}/life-accepted-input.ndjson" >"${traffic}/chaos_life_rotating_output.ndjson"
# Prints one closed window of every branch for each FIRST:LAST range of branch indexes.
windows() {
    local branch range
    for branch in red blue gold green; do
        for range in "$@"; do
            jq -nc --arg branch "${branch}" --argjson first "${range%%:*}" --argjson last "${range##*:}" \
                '($last - $first + 1) as $n
                 | {branch_name: $branch, records: $n, first_index: $first, last_index: $last,
                    index_sum: ($n * ($first + $last) / 2), distinct_residues: 3, median_index: (($first + $last) / 2)}'
        done
    done
}
# Every branch holds 25 records: tumbling windows of three, then four, records; the last records of
# each branch stay in a window that has not closed.
windows 1:3 4:7 8:10 11:14 15:17 18:21 >"${traffic}/chaos_life_window_output.ndjson"
generate expiring 1 16 >"${traffic}/chaos_life_expiring_input.ndjson"
jq -c '(if .sequence <= 8 then 1 else 2 end) as $v | . + {profile_version: $v, label: "green-v\($v)"}' \
    "${traffic}/chaos_life_expiring_input.ndjson" >"${traffic}/chaos_life_expiring_output.ndjson"
generate late 1 5 >"${traffic}/chaos_life_late_output.ndjson"
generate flush 1 6 >"${traffic}/chaos_life_buffered_output.ndjson"
for table in chaos_life_pool_first chaos_life_pool_second; do
    generate pool 1 12 | jq -s 'map({event_id, sequence})' >"${traffic}/${table}.json"
done

verdict() {
    "${verifier}" "$1" --traffic "${2:-${traffic}}" --boundaries "${3:-${boundaries}}" --result "${tmp_dir}/result.json"
}

for mode in enrich generator window rotation expiring late flush pool; do
    expect_pass "the ${mode} verdict of a faultless run" verdict "${mode}"
done
jq -e '.parked_blue == 6 and .accepted == 100 and .delivered == 100' <(verdict enrich && cat "${tmp_dir}/result.json") \
    >/dev/null || fail "the enrich verdict summary is wrong: $(cat "${tmp_dir}/result.json")"

# Copies the passing traffic into DIRECTORY with FILE replaced by the output of FILTER over it.
variant() {
    local directory="$1"
    local file="$2"
    local filter="$3"
    rm -rf "${directory}"
    cp -r "${traffic}" "${directory}"
    jq -c "${filter}" "${traffic}/${file}" >"${directory}/${file}"
}

variant "${tmp_dir}/superseded" chaos_life_enriched_output.ndjson \
    'if .sequence == 45 then .profile_version = 3 | .label = "red-v3" else . end'
expect_failure 'a red record enriched with an ignored lower timestamp' 'carry a superseded version: [45]' \
    verdict enrich "${tmp_dir}/superseded"
variant "${tmp_dir}/stale" chaos_life_enriched_output.ndjson \
    'if .sequence == 77 then .profile_version = 2 | .label = "red-v2" else . end'
expect_failure 'a red record enriched before its replacement applied' 'enriched with another profile than their branch held' \
    verdict enrich "${tmp_dir}/stale"
variant "${tmp_dir}/lost" chaos_life_enriched_output.ndjson 'select(.sequence != 2)'
expect_failure 'a lost parked blue record' 'records not delivered exactly once: [{"sequence":2,"count":0}]' \
    verdict enrich "${tmp_dir}/lost"
variant "${tmp_dir}/repeated" chaos_life_rotating_output.ndjson 'if .sequence == 90 then ., . else . end'
expect_failure 'a repeat without a fault' 'not delivered exactly once through the rotating relay' \
    verdict rotation "${tmp_dir}/repeated"
boundaries_for owner-crash 92 12 3 0 >"${tmp_dir}/faulted.json"
expect_pass 'a replay past the milestone after a fault' verdict rotation "${tmp_dir}/repeated" "${tmp_dir}/faulted.json"
variant "${tmp_dir}/early-repeat" chaos_life_rotating_output.ndjson 'if .sequence == 10 then ., . else . end'
expect_failure 'a replay before the milestone after a fault' 'not delivered exactly once through the rotating relay' \
    verdict rotation "${tmp_dir}/early-repeat" "${tmp_dir}/faulted.json"
boundaries_for none 88 0 3 0 >"${tmp_dir}/no-eviction.json"
expect_failure 'a rotation that evicted no branch' 'was evicted by its LRU bound' \
    verdict rotation "${traffic}" "${tmp_dir}/no-eviction.json"

variant "${tmp_dir}/never-held" chaos_life_generated_output.ndjson \
    'if .branch_name == "blue" then .profile_version = 2 | .label = "blue-v2" else . end'
expect_failure 'a generated profile never held' 'a profile their branch never held' \
    verdict generator "${tmp_dir}/never-held"
variant "${tmp_dir}/regressed" chaos_life_generated_output.ndjson '.'
{ cat "${tmp_dir}/regressed/chaos_life_generated_output.ndjson"; printf '{"branch_name":"red","profile_version":1,"label":"red-v1"}\n'; } \
    >"${tmp_dir}/regressed/next.ndjson"
mv "${tmp_dir}/regressed/next.ndjson" "${tmp_dir}/regressed/chaos_life_generated_output.ndjson"
expect_failure 'generated versions going back' 'generated versions went back for: ["red"]' \
    verdict generator "${tmp_dir}/regressed"

variant "${tmp_dir}/malformed-window" chaos_life_window_output.ndjson \
    'if .branch_name == "gold" and .first_index == 8 then .index_sum = 0 else . end'
expect_failure 'a window with a wrong sum' 'wrong bounds or sketches' verdict window "${tmp_dir}/malformed-window"
variant "${tmp_dir}/sketch-window" chaos_life_window_output.ndjson \
    'if .branch_name == "blue" and .first_index == 15 then .median_index = 40 else . end'
expect_failure 'a median outside its window' 'wrong bounds or sketches' verdict window "${tmp_dir}/sketch-window"
variant "${tmp_dir}/missing-window" chaos_life_window_output.ndjson 'select(.branch_name != "red" or .first_index != 8)'
expect_failure 'a lost window' 'aggregated twice or skipped by the closed windows' verdict window "${tmp_dir}/missing-window"
variant "${tmp_dir}/doubled-window" chaos_life_window_output.ndjson 'if .branch_name == "gold" and .first_index == 4 then ., . else . end'
expect_failure 'a window aggregated twice' 'aggregated twice or skipped by the closed windows' verdict window "${tmp_dir}/doubled-window"
variant "${tmp_dir}/split-window" chaos_life_window_output.ndjson \
    'if .branch_name == "blue" and .first_index == 11 then .records = 3 else . end'
expect_failure 'a window whose count is not its run' 'wrong bounds or sketches' verdict window "${tmp_dir}/split-window"

variant "${tmp_dir}/enrich-altered" chaos_life_enriched_output.ndjson \
    'if .sequence == 10 then .content = "chaos-life-payload-other" else . end'
expect_failure 'an enriched record that lost its own fields' 'enriched fields differ from the accepted record: [10]' \
    verdict enrich "${tmp_dir}/enrich-altered"
jq '.boundaries.parked_until = 1' "${boundaries}" >"${tmp_dir}/unparked.json"
expect_failure 'a blue branch that never parked' 'no blue record arrived before blue had state' \
    verdict enrich "${traffic}" "${tmp_dir}/unparked.json"
jq '.boundaries.milestone_records = 100' "${boundaries}" >"${tmp_dir}/short-source.json"
expect_failure 'a source that stopped at the milestone' 'the source accepted no record past the milestone' \
    verdict enrich "${traffic}" "${tmp_dir}/short-source.json"
variant "${tmp_dir}/generator-silent" chaos_life_generated_output.ndjson 'select(.branch_name != "gold")'
expect_failure 'a branch the generator never served' 'branches generated fewer than three rows: ["gold"]' \
    verdict generator "${tmp_dir}/generator-silent"
# After a fault that recovered at record 100, the windows holding branch indexes 23 through 27 are
# volatile. The window from 26 to 29 holds both, so its records past the interval are exempt too,
# while the closed window after it must still aggregate its records.
boundaries_for owner-crash 100 12 3 0 >"${tmp_dir}/recovered-100.json"
rm -rf "${tmp_dir}/straddle"
cp -r "${traffic}" "${tmp_dir}/straddle"
windows 1:3 4:7 8:10 11:14 15:17 18:21 22:25 26:29 30:32 33:36 >"${tmp_dir}/straddle/chaos_life_window_output.ndjson"
expect_pass 'a window that straddles the recovery after a fault' verdict window "${tmp_dir}/straddle" "${tmp_dir}/recovered-100.json"
rm -rf "${tmp_dir}/straddle-gap"
cp -r "${traffic}" "${tmp_dir}/straddle-gap"
windows 1:3 4:7 8:10 11:14 15:17 18:21 22:25 26:29 33:36 >"${tmp_dir}/straddle-gap/chaos_life_window_output.ndjson"
expect_failure 'a lost window after the recovery' '"never_aggregated":[30,31,32]' \
    verdict window "${tmp_dir}/straddle-gap" "${tmp_dir}/recovered-100.json"
variant "${tmp_dir}/window-silent" chaos_life_window_output.ndjson 'select(.branch_name != "blue")'
expect_failure 'a branch without a window' 'branches without a window: ["blue"]' verdict window "${tmp_dir}/window-silent"
variant "${tmp_dir}/rotation-altered" chaos_life_rotating_output.ndjson \
    'if .sequence == 30 then .branch_index = 99 else . end'
expect_failure 'a rotated record that lost its own fields' 'rotated copy differs from the accepted record: [30]' \
    verdict rotation "${tmp_dir}/rotation-altered"
variant "${tmp_dir}/expiring-lost" chaos_life_expiring_output.ndjson 'select(.sequence != 5)'
expect_failure 'a lost expiring record' 'expiring records not delivered exactly once: [{"sequence":5,"count":0}]' \
    verdict expiring "${tmp_dir}/expiring-lost"
jq '.expiring.after = 9' "${boundaries}" >"${tmp_dir}/expiring-short.json"
expect_failure 'an expiring source that missed a record' 'the expiring source did not accept every record' \
    verdict expiring "${traffic}" "${tmp_dir}/expiring-short.json"
variant "${tmp_dir}/expired-stale" chaos_life_expiring_output.ndjson \
    'if .sequence == 12 then .profile_version = 1 | .label = "green-v1" else . end'
expect_failure 'a recreated branch record with its expired profile' 'carry another profile than the one their branch held' \
    verdict expiring "${tmp_dir}/expired-stale"
boundaries_for none 88 12 1 0 >"${tmp_dir}/unexpired.json"
expect_failure 'a branch that never expired' 'was not evicted by its TTL' verdict expiring "${traffic}" "${tmp_dir}/unexpired.json"

variant "${tmp_dir}/late-lost" chaos_life_late_output.ndjson 'select(.sequence != 3)'
expect_failure 'a lost late record' 'late records not delivered exactly once: [3]' verdict late "${tmp_dir}/late-lost"
variant "${tmp_dir}/late-altered" chaos_life_late_output.ndjson \
    'if .sequence == 2 then .content = "chaos-late-payload-9" else . end'
expect_failure 'an altered late record' 'late records that differ from the records produced: [2]' \
    verdict late "${tmp_dir}/late-altered"
jq '.force_flush.arrived_ms = null' "${boundaries}" >"${tmp_dir}/unflushed.json"
expect_failure 'force-flushed records that never arrived' 'the force-flushed records never arrived' \
    verdict flush "${traffic}" "${tmp_dir}/unflushed.json"
jq '.force_flush.admission_observed = false' "${boundaries}" >"${tmp_dir}/unadmitted.json"
expect_failure 'a buffered route that never counted its records' 'never counted the records before the alteration' \
    verdict flush "${traffic}" "${tmp_dir}/unadmitted.json"
boundaries_for none 88 12 3 2 >"${tmp_dir}/unheld.json"
expect_failure 'buffered records that were not held' 'held 2 records before the alteration' \
    verdict flush "${traffic}" "${tmp_dir}/unheld.json"
jq '.force_flush.arrived_ms = 301000' "${boundaries}" >"${tmp_dir}/interval.json"
expect_failure 'buffered records that waited for their interval' 'arrived only at their flush interval' \
    verdict flush "${traffic}" "${tmp_dir}/interval.json"

variant "${tmp_dir}/pool-lost" chaos_life_pool_second.json 'map(select(.sequence != 7))'
expect_failure 'a lost pooled row' 'chaos_life_pool_second does not hold every pooled record exactly once: [7]' \
    verdict pool "${tmp_dir}/pool-lost"
variant "${tmp_dir}/pool-altered" chaos_life_pool_first.json \
    'map(if .sequence == 3 then .event_id = "run-pool-30" else . end)'
expect_failure 'an altered pooled row' 'chaos_life_pool_first holds rows that differ from the records produced: [3]' \
    verdict pool "${tmp_dir}/pool-altered"
jq '.pool.waiting_emitter = null' "${boundaries}" >"${tmp_dir}/pool-unreported.json"
expect_failure 'a pool wait that went unreported' 'no pooled emitter reported a wait for the full pool' \
    verdict pool "${traffic}" "${tmp_dir}/pool-unreported.json"
jq '.fault = "owner-crash"' "${boundaries}" >"${tmp_dir}/pool-faulted.json"
expect_failure 'a pool wait unreported after the fault' 'reported its wait for the full pool after the fault' \
    verdict pool "${traffic}" "${tmp_dir}/pool-faulted.json"
jq '.fault = "owner-crash" | .pool.waiting_after_fault = "chaos_life_pool_second"' "${boundaries}" \
    >"${tmp_dir}/pool-rewaited.json"
expect_pass 'a pool wait reported again after the fault' verdict pool "${traffic}" "${tmp_dir}/pool-rewaited.json"
jq '.pool.connections_while_waiting = [{address: "172.30.0.7", connections: 2}]' "${boundaries}" \
    >"${tmp_dir}/pool-crowded.json"
expect_failure 'a pool that exceeded its one connection' 'held more connections than its pool allows' \
    verdict pool "${traffic}" "${tmp_dir}/pool-crowded.json"
jq '.pool.connections_while_waiting = []' "${boundaries}" >"${tmp_dir}/pool-unconnected.json"
expect_failure 'a wait without the connection it waited for' 'the pool held no connection while an emitter waited' \
    verdict pool "${traffic}" "${tmp_dir}/pool-unconnected.json"

expect_setup_rejection 'a verdict needs its boundaries' 'the observed boundaries are missing' \
    "${verifier}" enrich --traffic "${traffic}" --boundaries "${tmp_dir}/missing.json" --result "${tmp_dir}/result.json"
expect_setup_rejection 'a verdict needs its ledger' 'a ledger is missing' \
    "${verifier}" late --traffic "${tmp_dir}" --boundaries "${boundaries}" --result "${tmp_dir}/result.json"

printf '%s\n' 'lifetimes self-test passed'
