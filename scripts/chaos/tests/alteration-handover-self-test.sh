#!/usr/bin/env bash
set -euo pipefail

# Self-test of the alteration-handover scenario: its option checks, its fixture generator, and the
# verdict of its alteration answer and node logs over a strict delivery verdict
# (verify-alteration-handover.sh). The shared harness it uses has its own controls in
# correctness-harness-self-test.sh. Every check runs without Docker or a product image.

chaos_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

fail() {
    printf 'alteration-handover self-test failed: %s\n' "$*" >&2
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

# Runs COMMAND..., which must exit STATUS.
expect_status() {
    local case_name="$1"
    local expected="$2"
    shift 2
    local status=0
    "$@" >"${tmp_dir}/status.out" 2>&1 || status=$?
    [[ "${status}" -eq "${expected}" ]] \
        || fail "${case_name} returned ${status}, expected ${expected}: $(cat "${tmp_dir}/status.out")"
}

# --- Options -------------------------------------------------------------------------------------

grep -Fq 'alteration-handover' <<<"$("${chaos_dir}/chaos.sh" list)" || fail 'scenario list omits alteration-handover'
expect_setup_rejection 'alteration-handover takes no fault' '--fault applies only to' \
    "${chaos_dir}/run-baseline.sh" --scenario alteration-handover --image fixture --fault owner-crash
expect_setup_rejection 'an expected failure names a symptom of the scenario' \
    'alteration-handover names no symptom lost-records' \
    "${chaos_dir}/run-baseline.sh" --scenario alteration-handover --image fixture \
    --expect-failure missing-records,lost-records

# --- The fixture generator -----------------------------------------------------------------------

generate() {
    jq -nc --arg run_id run --arg source "$1" --arg branches "$2" --argjson first "$3" --argjson count "$4" \
        -f "${chaos_dir}/fixtures/generate-alteration-handover.jq"
}
generate acked distinct 0 4 >"${tmp_dir}/backlog.ndjson"
generate acked interleaved 4 4 >"${tmp_dir}/paced.ndjson"
jq -e -s '
    length == 4
    and (map(.sequence) == [0, 1, 2, 3])
    and (map(.branch_name) | unique | length == 4)
    and all(.branch_name | startswith("acked-branch-"))
' "${tmp_dir}/backlog.ndjson" >/dev/null \
    || fail 'the backlog did not give every record a branch of its own'
jq -e -s '
    length == 4
    and (map(.sequence) == [4, 5, 6, 7])
    and (map(.branch_name) == ["alpha", "beta", "alpha", "beta"])
' "${tmp_dir}/paced.ndjson" >/dev/null \
    || fail 'the paced records did not follow the backlog on two interleaved branches'
jq -e -s '
    (map(.event_id) | unique | length == 8)
    and (map(.content) | unique | length == 8)
    and all(.source == "acked" and (.event_id | startswith("run-acked-")))
' <(cat "${tmp_dir}/backlog.ndjson" "${tmp_dir}/paced.ndjson") >/dev/null \
    || fail 'the records do not carry unique event ids and contents'

# --- The alteration answer and the node logs -----------------------------------------------------

# Strict delivery verdicts of two sources, one exact and one whose source without acknowledgements
# lost two records.
for source in acked unacked; do
    generate "${source}" interleaved 0 4 >"${tmp_dir}/accepted-${source}.ndjson"
done
cat "${tmp_dir}/accepted-acked.ndjson" "${tmp_dir}/accepted-unacked.ndjson" >"${tmp_dir}/output-exact.ndjson"
head -n 6 "${tmp_dir}/output-exact.ndjson" >"${tmp_dir}/output-cut.ndjson"
strict_verdict() {
    "${chaos_dir}/verify-strict-delivery.sh" --output "$1" --result "$2" \
        --source "acked=${tmp_dir}/accepted-acked.ndjson" --source "unacked=${tmp_dir}/accepted-unacked.ndjson" \
        >/dev/null
}
strict_verdict "${tmp_dir}/output-exact.ndjson" "${tmp_dir}/strict-pass.json" \
    || fail 'the exact delivery did not pass the strict verdict'
expect_status 'the cut delivery fails the strict verdict' 1 \
    strict_verdict "${tmp_dir}/output-cut.ndjson" "${tmp_dir}/strict-cut.json"

printf '%s\n' 'INFO nervix_server::runtime::schedule_apply: applied schedule' >"${tmp_dir}/node-quiet.log"
printf '%s\n' \
    'WARN nervix_server::runtime::schedule_apply: entity-level schedule apply failed; rebuilding domain domain="chaos_alter"' \
    >"${tmp_dir}/node-rebuilt.log"
printf '%s\n' \
    'WARN nervix_server::runtime::processor_branch_task: processor branch task exceeded shutdown grace period; aborting' \
    >"${tmp_dir}/node-aborted.log"
alter_verdict() {
    local delivery="$1"
    local answer_text="$2"
    local exit_code="$3"
    shift 3
    printf '%s\n' "${answer_text}" >"${tmp_dir}/answer.txt"
    local logs=()
    local log
    for log in "$@"; do
        logs+=(--node-log "${tmp_dir}/${log}")
    done
    "${chaos_dir}/verify-alteration-handover.sh" --delivery "${delivery}" \
        --alter-answer "${tmp_dir}/answer.txt" --alter-exit "${exit_code}" --deduplicator stand_in_dedup \
        "${logs[@]}" --result "${tmp_dir}/alter.json"
}
acknowledged="altered deduplicator 'stand_in_dedup' in domain 'stand_in'; quiesce level: ENTITY_PAUSE"
expect_status 'an acknowledged alteration after an exact delivery passes' 0 \
    alter_verdict "${tmp_dir}/strict-pass.json" "${acknowledged}" 0 node-quiet.log node-quiet.log
expect_status 'an aborted idle branch alone judges nothing' 0 \
    alter_verdict "${tmp_dir}/strict-pass.json" "${acknowledged}" 0 node-quiet.log node-aborted.log
jq -e '.aborted_branches == 1' "${tmp_dir}/alter.json" >/dev/null \
    || fail 'the aborted branch was not recorded beside the verdict'
expect_status 'a rebuild standing in for the swap fails' 1 \
    alter_verdict "${tmp_dir}/strict-cut.json" "${acknowledged}" 0 node-quiet.log node-rebuilt.log
jq -e '[.symptoms[].name] == ["missing-records", "domain-rebuilt"] and .symptoms[1].count == 1' \
    "${tmp_dir}/alter.json" >/dev/null \
    || fail "a domain rebuild was not named: $(jq -c '[.symptoms[].name]' "${tmp_dir}/alter.json")"
expect_status 'a refused alteration fails' 1 alter_verdict "${tmp_dir}/strict-pass.json" \
    "error: failed runtime activation" 1 node-quiet.log
jq -e '[.symptoms[].name] == ["alteration-refused"]' "${tmp_dir}/alter.json" >/dev/null \
    || fail 'a refused alteration was not named'
expect_status 'an answer that names another deduplicator fails' 1 alter_verdict "${tmp_dir}/strict-pass.json" \
    "altered deduplicator 'other_dedup' in domain 'stand_in'" 0 node-quiet.log
expect_status 'an unjudged delivery cannot be judged' 2 \
    alter_verdict "${tmp_dir}/none.json" "${acknowledged}" 0 node-quiet.log
expect_status 'a missing node log cannot be judged' 2 \
    alter_verdict "${tmp_dir}/strict-pass.json" "${acknowledged}" 0 node-missing.log

printf 'alteration-handover self-test passed\n'
