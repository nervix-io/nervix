#!/usr/bin/env bash
set -euo pipefail

# Self-test of the stateful scenario's state inspection (--inspect-state): the option checks, the
# observation normalizer on a page, a typed refusal and a transcript without an answer, and every
# verdict of verify-inspection-evidence.sh on passing evidence and on each way a report can name
# the wrong owner, the wrong process or the killed node, describe running state as stored, lose
# durable state, hold rows its input never carried, or accept a stale continuation.

chaos_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
verifier="${chaos_dir}/verify-inspection-evidence.sh"
normalizer="${chaos_dir}/state-inspection-observation.jq"
fixture="${chaos_dir}/tests/state-inspection-fixture.jq"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

fail() {
    printf 'state inspection self-test failed: %s\n' "$*" >&2
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
        || fail "${case_name} did not explain the rejected option: $(cat "${tmp_dir}/setup-rejection.out")"
}

expect_setup_rejection 'only the stateful scenario inspects state' '--inspect-state applies only to stateful' \
    "${chaos_dir}/run-baseline.sh" --scenario baseline --image fixture --inspect-state
expect_setup_rejection 'inspection needs a crashed owner on three nodes' \
    '--inspect-state requires --nodes 3 and --fault owner-crash' \
    "${chaos_dir}/run-baseline.sh" --scenario stateful --image fixture --inspect-state
expect_setup_rejection 'inspection refuses another fault' \
    '--inspect-state requires --nodes 3 and --fault owner-crash' \
    "${chaos_dir}/run-baseline.sh" --scenario stateful --image fixture --fault owner-pause --inspect-state

families=(relay deduplicator window reorderer correlator wasm inferencer)
jq -nc --arg run_id self --arg batch held -f "${chaos_dir}/fixtures/generate-inspection.jq" >"${tmp_dir}/held.ndjson"
jq -nc --arg run_id self --arg batch recovered -f "${chaos_dir}/fixtures/generate-inspection.jq" \
    >"${tmp_dir}/recovered.ndjson"
[[ "$(wc -l <"${tmp_dir}/held.ndjson")" -eq 8 && "$(wc -l <"${tmp_dir}/recovered.ndjson")" -eq 4 ]] \
    || fail 'the inspection batches did not hold eight and four records'

# The state each family holds at a stage of a passing run: exact at the milestone, durable state
# kept after the fault, and the reorderer and correlator holding what their owner was sent.
rows_for() {
    local family="$1"
    local stage="$2"
    jq -nc --arg family "${family}" --arg stage "${stage}" \
        --slurpfile held "${tmp_dir}/held.ndjson" --slurpfile recovered "${tmp_dir}/recovered.ndjson" '
        def branches: ("alpha", "beta");
        def held_rows($section; $records):
            [$records[] | {section: $section, branch: .branch_name,
                           fields: {event_id, branch_name, sequence, received_at: 1}}];
        if $family == "relay" then
            [branches as $branch | {section: "records", branch: $branch,
              fields: {branch_name: $branch, profile_version: (if $stage == "milestone" then 2 else 3 end)}}]
        elif $family == "deduplicator" then
            [branches as $branch | (if $stage == "milestone" then ["key-1", "key-2"] else ["key-1", "key-2", "key-3"] end)[]
             | {section: "keys", branch: $branch, fields: {key_1: ., seen_at: 1}}]
        elif $family == "window" then
            [branches as $branch | (if $stage == "milestone" then [49, 51] else [] end)[]
             | {section: "rows", branch: $branch, fields: {branch_name: $branch, sequence: .}}]
        elif $family == "reorderer" then
            if $stage == "milestone" then held_rows("pending"; $held)
            elif $stage == "failover" then [] else held_rows("pending"; $recovered) end
        elif $family == "correlator" then
            if $stage == "milestone" then held_rows("left"; $held)
            elif $stage == "failover" then [] else held_rows("left"; $recovered) end
        elif $family == "inferencer" then
            [branches as $branch | {section: "header", branch: $branch, fields: {branch_name: $branch}}]
        else [] end'
}

# The answer of FAMILY at STAGE, one unit owned and served by OWNER in INCARNATION at STAGE_LABEL.
spec_for() {
    local family="$1"
    local stage="$2"
    local owner="$3"
    local incarnation="$4"
    local replicas="$5"
    local stage_label="${6:-live}"
    jq -nc --arg family "${family}" --arg owner "${owner}" --argjson incarnation "${incarnation}" \
        --argjson replicas "${replicas}" --arg stage "${stage_label}" --argjson rows "$(rows_for "${family}" "${stage}")" '
        {family: $family,
         units: [{unit: "single", owner: $owner, replicas: $replicas,
                  origin: {node: $owner, incarnation: $incarnation, stage: $stage}, rows: $rows}
                 + (if $family == "wasm" then {wasm: {total: 2, failed: 0, checkpoints: [5, 6]}} else {} end)]}'
}

# Writes the observation of SPEC, read in PHASE, to OUTPUT.json through the fixture and normalizer.
observe() {
    local read_phase="$1"
    local spec="$2"
    local output="$3"
    local family
    family="$(jq -r '.family' <<<"${spec}")"
    jq -n --argjson spec "${spec}" -f "${fixture}" >"${output}.document.json"
    jq -n --slurpfile document "${output}.document.json" --arg family "${family}" --arg phase "${read_phase}" \
        --arg host nervix-1 --arg statement "SHOW ${family} STATE FORMAT JSON;" \
        --arg started "${observed_at}" --arg finished "$((observed_at + 5))" --arg status 0 \
        -f "${normalizer}" >"${output}.json"
    observed_at=$((observed_at + 10))
}
observed_at=1000

# The normalizer keeps a page's units and rows, a refusal's typed reason, and reads a transcript
# without an answer as unreadable.
observe milestone "$(spec_for reorderer milestone node-3 13 '["node-2"]')" "${tmp_dir}/normalized-page"
jq -e '.status == "page" and .units[0].owner == "node-3" and .units[0].origin.incarnation == 13
       and ([.rows[] | select(.section == "pending")] | length) == 8' "${tmp_dir}/normalized-page.json" >/dev/null \
    || fail 'the normalizer did not keep the page, its owner and its rows'
observe outage '{"family":"relay","rejection":"owner_unavailable"}' "${tmp_dir}/normalized-refusal"
jq -e '.status == "rejected" and .rejection == "owner_unavailable"' "${tmp_dir}/normalized-refusal.json" >/dev/null \
    || fail 'the normalizer did not keep a typed refusal'
: >"${tmp_dir}/no-answer.document.json"
jq -n --slurpfile document "${tmp_dir}/no-answer.document.json" --arg family relay --arg phase outage \
    --arg host nervix-1 --arg statement s --arg started 1 --arg finished 2 --arg status 1 \
    -f "${normalizer}" >"${tmp_dir}/no-answer.json"
jq -e '.status == "unreadable"' "${tmp_dir}/no-answer.json" >/dev/null \
    || fail 'the normalizer did not read a transcript without an answer as unreadable'

# Passing evidence: node-3 owned the state with node-2 as its replica, was killed, and node-2 took
# it over. node-3 restarted in incarnation 14 while node-2 kept the state.
evidence="${tmp_dir}/evidence"
placement() {
    local owner="$1"
    local replicas="$2"
    jq -nc --arg owner "${owner}" --arg replicas "${replicas}" '
        ["RELAY chaos_profiles", "DEDUPLICATOR chaos_dedup", "WINDOW PROCESSOR chaos_window",
         "REORDERER chaos_reorderer", "CORRELATOR chaos_correlator", "WASM PROCESSOR chaos_counter",
         "INFERENCER chaos_running"]
        | map({key: ., value: {units: [], owner: $owner, replicas: $replicas}}) | from_entries'
}
mkdir -p "${evidence}"/{milestone,outage,failover,recovered}/reads
jq -n --slurpfile held "${tmp_dir}/held.ndjson" --slurpfile recovered "${tmp_dir}/recovered.ndjson" '
    {state_owner: "node-3", killed: "node-3", owner_incarnation: 13, partitions: 1, profile_milestone: 2,
     batches: {held: $held, recovered: $recovered}}' >"${evidence}/context.json"
placement node-3 node-2 >"${evidence}/milestone/placement.json"
placement node-2 node-1 >"${evidence}/failover/placement.json"
placement node-2 node-3 >"${evidence}/recovered/placement.json"
printf '{"node-1":11,"node-2":12,"node-3":13}\n' >"${evidence}/milestone/cluster.json"
printf '{"node-1":11,"node-2":12}\n' >"${evidence}/failover/cluster.json"
printf '{"node-1":11,"node-2":12,"node-3":14}\n' >"${evidence}/recovered/cluster.json"
for family in "${families[@]}"; do
    relay_stage=live
    observe milestone "$(spec_for "${family}" milestone node-3 13 '["node-2"]')" "${evidence}/milestone/reads/${family}"
    if [[ "${family}" == relay || "${family}" == window ]]; then
        observe outage "{\"family\":\"${family}\",\"rejection\":\"owner_unavailable\"}" "${evidence}/outage/reads/${family}"
    elif [[ "${family}" == deduplicator ]]; then
        observe outage "{\"family\":\"${family}\",\"rejection\":\"deadline\"}" "${evidence}/outage/reads/${family}"
    else
        observe outage "$(spec_for "${family}" failover node-2 12 '["node-1"]')" "${evidence}/outage/reads/${family}"
    fi
    observe failover "{\"family\":\"${family}\",\"rejection\":\"not_ready\"}" "${evidence}/failover/reads/${family}-1"
    if [[ "${family}" == relay ]]; then
        relay_stage=stored
    fi
    observe failover "$(spec_for "${family}" failover node-2 12 '["node-1"]' "${relay_stage}")" \
        "${evidence}/failover/reads/${family}-2"
    observe recovered "$(spec_for "${family}" recovered node-2 12 '["node-3"]')" "${evidence}/recovered/reads/${family}"
done
first_page="$(spec_for deduplicator milestone node-3 13 '["node-2"]' | jq -c '.continuation = "issued-at-the-milestone"')"
observe continuation "${first_page}" "${evidence}/milestone/reads/deduplicator-first-page"
observe stale '{"family":"deduplicator","rejection":"stale_continuation"}' "${evidence}/recovered/reads/deduplicator-stale"

expect_pass() {
    local case_name="$1"
    "${verifier}" --evidence "${evidence}" --result "${tmp_dir}/verdict.json" >"${tmp_dir}/pass.out" 2>&1 \
        || fail "${case_name} was rejected: $(cat "${tmp_dir}/pass.out")"
}

# Replaces the observation at OUTPUT with SPEC read in PHASE, runs the verifier, which must reject
# the evidence naming FAILURE, and restores the passing observation.
expect_failure() {
    local case_name="$1"
    local failure="$2"
    local read_phase="$3"
    local spec="$4"
    local output="$5"
    cp "${output}.json" "${tmp_dir}/saved.json"
    observe "${read_phase}" "${spec}" "${output}"
    # The replacement keeps its place among a family's attempts, which the verifier orders by time.
    jq --slurpfile saved "${tmp_dir}/saved.json" \
        '.started_ms = $saved[0].started_ms | .finished_ms = $saved[0].finished_ms' \
        "${output}.json" >"${output}.json.next"
    mv "${output}.json.next" "${output}.json"
    local status=0
    "${verifier}" --evidence "${evidence}" --result "${tmp_dir}/verdict.json" >"${tmp_dir}/failure.out" 2>&1 \
        || status=$?
    cp "${tmp_dir}/saved.json" "${output}.json"
    [[ "${status}" -eq 1 ]] || fail "${case_name} returned ${status}, expected verdict 1: $(cat "${tmp_dir}/failure.out")"
    jq -e --arg failure "${failure}" 'any(.failures[]; contains($failure))' "${tmp_dir}/verdict.json" >/dev/null \
        || fail "${case_name} did not report '${failure}': $(jq -c '.failures' "${tmp_dir}/verdict.json")"
}

expect_pass 'the passing inspection evidence'
jq -e '.verdict == "pass" and .owners.failover.relay == ["node-2"] and .stages.failover.relay == ["stored"]
       and .outage.relay.rejection == "owner_unavailable" and .failover_refusals.wasm == ["not_ready"]
       and .stale_continuation.rejection == "stale_continuation"' "${tmp_dir}/verdict.json" >/dev/null \
    || fail "the passing verdict did not record owners, stages and refusals: $(cat "${tmp_dir}/verdict.json")"

expect_failure 'a failover page served by the killed owner' 'names the killed node node-3' failover \
    "$(spec_for window failover node-3 13 '["node-2"]')" "${evidence}/failover/reads/window-2"
expect_failure 'an outage read refused with an untyped outage reason' 'refused as unknown_entity' outage \
    '{"family":"reorderer","rejection":"unknown_entity"}' "${evidence}/outage/reads/reorderer"
expect_failure 'an outage read the CLI could not answer' \
    'the outage read of correlator was unreadable: request_failed: connection reset' outage \
    '{"family":"correlator","error":{"code":"request_failed","message":"connection reset"}}' \
    "${evidence}/outage/reads/correlator"
expect_failure 'a page naming a stale process incarnation' 'names incarnation 13 of node-2' recovered \
    "$(spec_for relay recovered node-2 13 '["node-3"]')" "${evidence}/recovered/reads/relay"
expect_failure 'running processor state described as stored' 'reports a "stored" read of running state' failover \
    "$(spec_for deduplicator failover node-2 12 '["node-1"]' stored)" "${evidence}/failover/reads/deduplicator-2"
expect_failure 'an owner listed among its replicas' 'lists its owner among its replicas' recovered \
    "$(spec_for wasm recovered node-2 12 '["node-2"]')" "${evidence}/recovered/reads/wasm"
expect_failure 'a report naming another owner than DESCRIBE' 'names node-1 as its owner where DESCRIBE names node-2' \
    recovered "$(spec_for inferencer recovered node-1 11 '["node-3"]')" "${evidence}/recovered/reads/inferencer"
expect_failure 'a milestone read away from the state owner' 'not the state owner node-3' milestone \
    "$(spec_for correlator milestone node-2 12 '["node-3"]')" "${evidence}/milestone/reads/correlator"
lost_keys="$(spec_for deduplicator recovered node-2 12 '["node-3"]' \
    | jq -c '.units[0].rows |= map(select(.fields.key_1 != "key-2"))')"
expect_failure 'a deduplicator key lost after the fault' 'no longer remembers 2 keys durable at the milestone' \
    recovered "${lost_keys}" "${evidence}/recovered/reads/deduplicator"
old_profile="$(spec_for relay failover node-2 12 '["node-1"]' | jq -c '.units[0].rows |= map(.fields.profile_version = 1)')"
expect_failure 'a relay profile regressed below the milestone' 'holds no profile of version 2 or later for alpha' \
    failover "${old_profile}" "${evidence}/failover/reads/relay-2"
expect_failure 'a reorderer that lost the rows produced after recovery' 'does not hold alpha/5, alpha/6' recovered \
    "$(spec_for reorderer failover node-2 12 '["node-3"]')" "${evidence}/recovered/reads/reorderer"
foreign="$(spec_for correlator recovered node-2 12 '["node-3"]' \
    | jq -c '.units[0].rows += [{section: "left", branch: "beta", fields: {event_id: "x", branch_name: "beta", sequence: 99, received_at: 1}}]')"
expect_failure 'a correlator holding a row its input never carried' 'holds beta/99 in left' recovered \
    "${foreign}" "${evidence}/recovered/reads/correlator"
expect_failure 'a milestone guest page without both checkpoints' 'lists 0 guest checkpoints' milestone \
    '{"family":"wasm","units":[{"unit":"single","owner":"node-3","replicas":["node-2"],"origin":{"node":"node-3","incarnation":13,"stage":"live"},"rows":[]}]}' \
    "${evidence}/milestone/reads/wasm"
expect_failure 'a page served by a node other than its owner' 'was served by node-1, not by its owner node-2' recovered \
    "$(spec_for window recovered node-2 12 '["node-3"]' | jq -c '.units[0].origin.node = "node-1" | .units[0].origin.incarnation = 11')" \
    "${evidence}/recovered/reads/window"
expect_failure 'a failover attempt refused with an untyped reason before its page' \
    'a failover read of reorderer before its page was rejected as reply_too_large' failover \
    '{"family":"reorderer","rejection":"reply_too_large"}' "${evidence}/failover/reads/reorderer-1"
expect_failure 'a stale continuation answered after recovery' 'not refused as stale_continuation' stale \
    "$(spec_for deduplicator recovered node-2 12 '["node-3"]')" "${evidence}/recovered/reads/deduplicator-stale"
expect_failure 'a milestone page that left keys unread' 'left selected state unread' milestone \
    "$(spec_for deduplicator milestone node-3 13 '["node-2"]' | jq -c '.continuation = "more"')" \
    "${evidence}/milestone/reads/deduplicator"
expect_failure 'a failover read that never reached a page' 'the failover read of inferencer was rejected as not_ready' \
    failover '{"family":"inferencer","rejection":"not_ready"}' "${evidence}/failover/reads/inferencer-2"

# Evidence without an observation of every family is unusable, not a verdict.
mv "${evidence}/outage/reads/wasm.json" "${tmp_dir}/wasm-outage.json"
status=0
"${verifier}" --evidence "${evidence}" --result "${tmp_dir}/verdict.json" >"${tmp_dir}/unusable.out" 2>&1 || status=$?
mv "${tmp_dir}/wasm-outage.json" "${evidence}/outage/reads/wasm.json"
[[ "${status}" -eq 2 ]] || fail "evidence without an outage read of wasm returned ${status}, expected 2"
grep -Fq '0 outage reads of wasm' "${tmp_dir}/unusable.out" \
    || fail "the unusable verdict did not name the missing read: $(cat "${tmp_dir}/unusable.out")"
expect_pass 'the restored passing inspection evidence'

printf 'state inspection self-test passed\n'
