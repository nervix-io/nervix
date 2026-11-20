#!/usr/bin/env bash
set -euo pipefail

# Self-test of the graceful-stop scenario: its option checks, its fixture generator, and the verdict
# of its STOP answer over a strict delivery verdict (verify-graceful-stop.sh). The shared harness it
# uses has its own controls in correctness-harness-self-test.sh. Every check runs without Docker or
# a product image.

chaos_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

fail() {
    printf 'graceful-stop self-test failed: %s\n' "$*" >&2
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

grep -Fq 'graceful-stop' <<<"$("${chaos_dir}/chaos.sh" list)" || fail 'scenario list omits graceful-stop'
expect_setup_rejection 'graceful-stop needs three nodes' 'graceful-stop requires --nodes 3' \
    "${chaos_dir}/run-baseline.sh" --scenario graceful-stop --image fixture --nodes 1
expect_setup_rejection 'graceful-stop takes no fault' '--fault applies only to' \
    "${chaos_dir}/run-baseline.sh" --scenario graceful-stop --image fixture --fault owner-crash
expect_setup_rejection 'an expected failure names a symptom of the scenario' \
    'graceful-stop names no symptom lost-records' \
    "${chaos_dir}/run-baseline.sh" --scenario graceful-stop --image fixture --expect-failure missing-records,lost-records

# --- The fixture generator -----------------------------------------------------------------------

lanes='[{"lane":"deliver","records":4},{"lane":"ingestor","records":2},{"lane":"collector","records":2}]'
jq -nc --arg run_id run --arg source acked --argjson first 0 --argjson lanes "${lanes}" \
    -f "${chaos_dir}/fixtures/generate-graceful-stop.jq" >"${tmp_dir}/payloads.ndjson"
jq -nc --arg run_id run --arg source acked --argjson first 8 --argjson lanes '[{"lane":"publish","records":2}]' \
    -f "${chaos_dir}/fixtures/generate-graceful-stop.jq" >>"${tmp_dir}/payloads.ndjson"
jq -e -s '
    length == 2
    and (map(type == "array") | all)
    and (map(length) == [8, 2])
    and ([.[][]] | map(.sequence) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    and ([.[][]] | map(.event_id) | unique | length == 10)
    and ([.[][]] | all(.source == "acked" and (.event_id | startswith("run-acked-"))))
    and ([.[][]] | all(.branch_name == (if .sequence % 2 == 0 then "alpha" else "beta" end)))
    and (.[0] | map(.lane) == ["deliver", "deliver", "deliver", "deliver", "ingestor", "ingestor",
                               "collector", "collector"])
    and (.[1] | all(.lane == "publish"))
' "${tmp_dir}/payloads.ndjson" >/dev/null \
    || fail 'the generator did not write one payload of every lane with alternating branches and distinct IDs'

# --- The STOP answer -----------------------------------------------------------------------------

# Strict delivery verdicts of two sources, one exact and one whose acknowledged source sent two
# records again after the restart.
record() {
    jq -nc --arg source "$1" --argjson sequence "$2" \
        '{event_id: ("run-\($source)-\($sequence)"), source: $source, lane: "deliver",
          branch_name: (if $sequence % 2 == 0 then "alpha" else "beta" end), sequence: $sequence,
          content: ("payload-\($sequence)")}'
}
for source in acked unacked; do
    for sequence in 0 1 2 3; do
        record "${source}" "${sequence}"
    done >"${tmp_dir}/accepted-${source}.ndjson"
done
cat "${tmp_dir}/accepted-acked.ndjson" "${tmp_dir}/accepted-unacked.ndjson" >"${tmp_dir}/output-exact.ndjson"
{ cat "${tmp_dir}/output-exact.ndjson"; record acked 1; record acked 2; } >"${tmp_dir}/output-replayed.ndjson"
strict_verdict() {
    "${chaos_dir}/verify-strict-delivery.sh" --output "$1" --result "$2" \
        --source "acked=${tmp_dir}/accepted-acked.ndjson" --source "unacked=${tmp_dir}/accepted-unacked.ndjson" \
        >/dev/null
}
strict_verdict "${tmp_dir}/output-exact.ndjson" "${tmp_dir}/strict-pass.json" \
    || fail 'the exact delivery did not pass the strict verdict'
expect_status 'the replayed delivery fails the strict verdict' 1 \
    strict_verdict "${tmp_dir}/output-replayed.ndjson" "${tmp_dir}/strict-cut.json"

stop_verdict() {
    local delivery="$1"
    local answer_text="$2"
    local exit_code="$3"
    printf '%s\n' "${answer_text}" >"${tmp_dir}/answer.txt"
    "${chaos_dir}/verify-graceful-stop.sh" --delivery "${delivery}" --stop-answer "${tmp_dir}/answer.txt" \
        --stop-exit "${exit_code}" --domain stand_in --entity stand_in_acked --entity stand_in_emitter \
        --result "${tmp_dir}/stop.json"
}
expect_status 'a plain answer after an exact delivery passes' 0 \
    stop_verdict "${tmp_dir}/strict-pass.json" "stopped domain 'stand_in'" 0
expect_status 'an answer saying nothing was cut passes' 0 \
    stop_verdict "${tmp_dir}/strict-pass.json" "stopped domain 'stand_in'; nothing was cut" 0
expect_status 'a silent cut fails' 1 stop_verdict "${tmp_dir}/strict-cut.json" "stopped domain 'stand_in'" 0
jq -e '[.symptoms[].name] == ["duplicate-records", "silent-cut"]' "${tmp_dir}/stop.json" >/dev/null \
    || fail "a silent cut was not named: $(jq -c '[.symptoms[].name]' "${tmp_dir}/stop.json")"
expect_status 'a reported cut fails even when nothing is missing' 1 stop_verdict "${tmp_dir}/strict-pass.json" \
    "stopped domain 'stand_in'; cut: node-2 stand_in_emitter 3 records" 0
jq -e '[.symptoms[].name] == ["stop-reported-cut"] and .symptoms[0].entities == ["stand_in_emitter"]' \
    "${tmp_dir}/stop.json" >/dev/null || fail 'a reported cut was not named with its entity'
expect_status 'a cut that was reported is not silent' 1 stop_verdict "${tmp_dir}/strict-cut.json" \
    "stopped domain 'stand_in'; cut: node-1 stand_in_acked 2 records" 0
jq -e '[.symptoms[].name] == ["duplicate-records", "stop-reported-cut"]' "${tmp_dir}/stop.json" >/dev/null \
    || fail 'a reported cut was judged silent'
expect_status 'an enforced stop fails' 1 stop_verdict "${tmp_dir}/strict-pass.json" \
    "stopped domain 'stand_in'; the stop was enforced" 0
expect_status 'a refused stop fails' 1 stop_verdict "${tmp_dir}/strict-pass.json" "error: domain is held" 1
jq -e '[.symptoms[].name] == ["stop-not-acknowledged"]' "${tmp_dir}/stop.json" >/dev/null \
    || fail 'a refused stop was not named'
expect_status 'an unjudged delivery cannot be judged' 2 stop_verdict "${tmp_dir}/none.json" "stopped domain 'stand_in'" 0

printf 'graceful-stop self-test passed\n'
