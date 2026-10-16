#!/usr/bin/env bash
set -euo pipefail

# Self-test of the backup state verdict: the deduplicator and window outputs it accepts, with and
# without replayed copies, and each way a restore that lost or doubled state shows in them.

chaos_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
verifier="${chaos_dir}/verify-backup-state.sh"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

fail() {
    printf 'backup state self-test failed: %s\n' "$*" >&2
    exit 1
}

# Eighty accepted records of the baseline fixture's shape, in its three branches.
jq -nc 'range(0; 80) as $sequence
    | {event_id: "run-\($sequence)", branch_name: ["alpha", "beta", "gamma"][$sequence % 3],
       sequence: $sequence, content: "chaos-baseline-payload-\($sequence)"}' >"${tmp_dir}/accepted.ndjson"
# The first thirty records are the only unique ones: sequence modulo 30 repeats from then on.
head -n 30 "${tmp_dir}/accepted.ndjson" >"${tmp_dir}/unique.ndjson"
# Each branch closes two windows of twelve of its records: alpha and beta hold 27 records, gamma 26.
jq -nc 'range(0; 3) as $branch | range(0; 2) as $window
    | ($branch + 36 * $window) as $first
    | {branch_name: ["alpha", "beta", "gamma"][$branch], records: 12, first_sequence: $first,
       last_sequence: ($first + 33), sequence_sum: (12 * $first + 198)}' >"${tmp_dir}/windows.ndjson"
[[ "$(wc -l <"${tmp_dir}/windows.ndjson")" -eq 6 ]] || fail 'the fixture does not close six windows'

# Runs the verifier over UNIQUE and WINDOWS and expects STATUS, and with status 1 the failure that
# FILTER selects from the result.
expect() {
    local case_name="$1"
    local unique="$2"
    local windows="$3"
    local expected_status="$4"
    local filter="${5:-}"
    local status=0
    "${verifier}" "${tmp_dir}/accepted.ndjson" "${unique}" "${windows}" "${tmp_dir}/${case_name}.json" \
        >"${tmp_dir}/${case_name}.out" 2>&1 || status=$?
    [[ "${status}" -eq "${expected_status}" ]] \
        || fail "${case_name} returned ${status}, expected ${expected_status}: $(cat "${tmp_dir}/${case_name}.out")"
    if [[ -n "${filter}" ]]; then
        jq -e "${filter}" "${tmp_dir}/${case_name}.json" >/dev/null \
            || fail "${case_name} did not report its failure: $(cat "${tmp_dir}/${case_name}.json")"
    fi
}

expect exact "${tmp_dir}/unique.ndjson" "${tmp_dir}/windows.ndjson" 0 \
    '.deduplicator.expected == 30 and .window.expected == 6 and .deduplicator.duplicate_copies == 0'

# A restore replays the records after its archived offset, so outputs may repeat exactly.
cat "${tmp_dir}/unique.ndjson" <(sed -n 21,25p "${tmp_dir}/unique.ndjson") >"${tmp_dir}/replayed-unique.ndjson"
cat "${tmp_dir}/windows.ndjson" <(tail -n 1 "${tmp_dir}/windows.ndjson") >"${tmp_dir}/replayed-windows.ndjson"
expect replayed "${tmp_dir}/replayed-unique.ndjson" "${tmp_dir}/replayed-windows.ndjson" 0 \
    '.deduplicator.duplicate_copies == 5 and .window.duplicate_copies == 1'

# Lost deduplicator keys emit a repeated key again.
cat "${tmp_dir}/unique.ndjson" <(sed -n 46p "${tmp_dir}/accepted.ndjson") >"${tmp_dir}/lost-keys.ndjson"
expect lost-keys "${tmp_dir}/lost-keys.ndjson" "${tmp_dir}/windows.ndjson" 1 \
    '.verdict == "fail" and (.deduplicator.unexpected | map(.sequence)) == [45]'

# A replayed copy that differs from its record is incorrect, not a duplicate.
jq -c 'if .sequence == 7 then .content = "changed" else . end' "${tmp_dir}/unique.ndjson" \
    >"${tmp_dir}/changed-unique.ndjson"
expect changed-record "${tmp_dir}/changed-unique.ndjson" "${tmp_dir}/windows.ndjson" 1 \
    '(.deduplicator.incorrect | length) == 1 and (.deduplicator.missing | map(.sequence)) == [7]'

head -n 29 "${tmp_dir}/unique.ndjson" >"${tmp_dir}/missing-unique.ndjson"
expect missing-unique "${tmp_dir}/missing-unique.ndjson" "${tmp_dir}/windows.ndjson" 1 \
    '(.deduplicator.missing | map(.sequence)) == [29]'

# A restore that lost the rows its alpha window retained closes the next alpha window over later
# records, so its windows shift.
jq -c 'if .branch_name == "alpha" and .first_sequence == 36
       then .first_sequence = 42 | .last_sequence = 75 | .sequence_sum = (12 * 42 + 198) else . end' \
    "${tmp_dir}/windows.ndjson" >"${tmp_dir}/lost-rows.ndjson"
expect lost-rows "${tmp_dir}/unique.ndjson" "${tmp_dir}/lost-rows.ndjson" 1 \
    '(.window.unexpected | map(.first_sequence)) == [42] and (.window.missing | map(.first_sequence)) == [36]'

# A restore that admitted retained rows twice closes a window over more records than its width.
jq -c 'if .branch_name == "beta" and .first_sequence == 37 then .records = 16 else . end' \
    "${tmp_dir}/windows.ndjson" >"${tmp_dir}/doubled-rows.ndjson"
expect doubled-rows "${tmp_dir}/unique.ndjson" "${tmp_dir}/doubled-rows.ndjson" 1 \
    '(.window.incorrect | map(.observed.records)) == [16]'

head -n 5 "${tmp_dir}/windows.ndjson" >"${tmp_dir}/missing-window.ndjson"
expect missing-window "${tmp_dir}/unique.ndjson" "${tmp_dir}/missing-window.ndjson" 1 \
    '(.window.missing | map([.branch_name, .first_sequence])) == [["gamma", 38]]'

# The open window of each branch never closes, so a summary over fewer than twelve records is
# unexpected.
cat "${tmp_dir}/windows.ndjson" \
    <(jq -nc '{branch_name: "alpha", records: 3, first_sequence: 72, last_sequence: 78, sequence_sum: 225}') \
    >"${tmp_dir}/open-window.ndjson"
expect open-window "${tmp_dir}/unique.ndjson" "${tmp_dir}/open-window.ndjson" 1 \
    '(.window.unexpected | map(.first_sequence)) == [72]'

: >"${tmp_dir}/empty.ndjson"
expect no-output "${tmp_dir}/empty.ndjson" "${tmp_dir}/empty.ndjson" 1 \
    '(.deduplicator.missing | length) == 30 and (.window.missing | length) == 6'

printf '{"event_id":"run-0"}\n' >"${tmp_dir}/malformed.ndjson"
expect malformed-output "${tmp_dir}/malformed.ndjson" "${tmp_dir}/windows.ndjson" 2 \
    '.verdict == "error" and .category == "invalid_unique_output"'
expect missing-file "${tmp_dir}/unique.ndjson" "${tmp_dir}/absent.ndjson" 2 \
    '.verdict == "error" and .category == "missing_output"'
cat "${tmp_dir}/accepted.ndjson" <(head -n 1 "${tmp_dir}/accepted.ndjson") >"${tmp_dir}/twice.ndjson"
status=0
"${verifier}" "${tmp_dir}/twice.ndjson" "${tmp_dir}/unique.ndjson" "${tmp_dir}/windows.ndjson" \
    "${tmp_dir}/twice.json" >/dev/null 2>&1 || status=$?
[[ "${status}" -eq 2 ]] && jq -e '.category == "invalid_accepted_ledger"' "${tmp_dir}/twice.json" >/dev/null \
    || fail 'an accepted ledger that holds an event twice was not refused'
status=0
"${verifier}" "${tmp_dir}/accepted.ndjson" >/dev/null 2>&1 || status=$?
[[ "${status}" -eq 2 ]] || fail "a call without its outputs returned ${status}, expected usage error 2"

printf 'backup state self-test passed\n'
