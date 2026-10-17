#!/usr/bin/env bash
set -euo pipefail

# Judges the deduplicator and window outputs of the backup state graph against the accepted-input
# ledger. The deduplicator keys each branch's records by their sequence modulo 30, so exactly the
# accepted records below sequence 30 are unique, and every later record repeats a key its branch has
# seen. The window summarizes each run of twelve consecutive records of a branch, in source order.
# A record the source delivers again after a restore may be emitted again, so each expected output
# appears at least once and every copy must equal it exactly. An output that no accepted record
# explains, a copy that differs from its expected value, and a missing expected output each fail
# the verdict: a restore that lost deduplicator keys emits a repeated key again, and one that lost
# or doubled window rows closes a window over another set of records.

usage() {
    printf 'usage: %s ACCEPTED_NDJSON UNIQUE_NDJSON WINDOW_NDJSON RESULT_JSON\n' "$(basename "$0")" >&2
}

if [[ "$#" -ne 4 ]]; then
    usage
    exit 2
fi
accepted_path="$1"
unique_path="$2"
window_path="$3"
result_path="$4"
mkdir -p "$(dirname "${result_path}")"

write_error() {
    jq -n --arg category "$1" --arg message "$2" \
        '{verdict: "error", category: $category, message: $message}' >"${result_path}"
}

record_contract='type == "object"
    and (.event_id | type == "string" and length > 0)
    and (.branch_name | type == "string" and length > 0)
    and (.sequence | type == "number" and isfinite and floor == . and . >= 0)
    and (.content | type == "string")'
window_contract='type == "object"
    and (.branch_name | type == "string" and length > 0)
    and ([.records, .first_sequence, .last_sequence, .sequence_sum]
         | all(type == "number" and isfinite and floor == .))'

if [[ ! -s "${accepted_path}" ]] || ! jq -e "${record_contract}" "${accepted_path}" >/dev/null 2>&1; then
    write_error invalid_accepted_ledger 'the accepted-input ledger is missing, empty or outside the fixture contract'
    exit 2
fi
if [[ -n "$(jq -s '[group_by(.event_id)[] | select(length > 1)] | .[0] // empty' "${accepted_path}")" ]]; then
    write_error invalid_accepted_ledger 'the accepted-input ledger holds an event_id twice'
    exit 2
fi
for observed in "${unique_path}" "${window_path}"; do
    [[ -e "${observed}" ]] || { write_error missing_output "${observed} does not exist"; exit 2; }
done
if [[ -s "${unique_path}" ]] && ! jq -e "${record_contract}" "${unique_path}" >/dev/null 2>&1; then
    write_error invalid_unique_output 'the deduplicator output holds a record outside the fixture contract'
    exit 2
fi
if [[ -s "${window_path}" ]] && ! jq -e "${window_contract}" "${window_path}" >/dev/null 2>&1; then
    write_error invalid_window_output 'the window output holds a summary outside the window contract'
    exit 2
fi

jq -n \
    --slurpfile accepted "${accepted_path}" \
    --slurpfile unique "${unique_path}" \
    --slurpfile windows "${window_path}" '
    def summary($group):
      {branch_name: $group[0].branch_name, records: ($group | length),
       first_sequence: $group[0].sequence, last_sequence: $group[-1].sequence,
       sequence_sum: ($group | map(.sequence) | add)};
    def complete_windows($records):
      [range(0; ($records | length) / 12 | floor) as $index
       | summary($records[$index * 12:($index + 1) * 12])];
    # Every copy of an output must equal the one expected value with its identity, and an expected
    # value is missing until one copy equals it.
    def judge($expected; $observed; identity):
      ($expected | map({key: (identity | tostring), value: .}) | from_entries) as $by_identity
      | ($observed | map(identity | tostring) | group_by(.)
         | map({key: .[0], value: length}) | from_entries) as $copies
      | ([$observed[] | select($by_identity[identity | tostring] == .) | identity | tostring]
         | map({key: ., value: true}) | from_entries) as $produced
      | {expected: ($expected | length),
         observed: ($observed | length),
         duplicate_copies: ([$copies[] | . - 1] | add // 0),
         missing: [$expected[] | select($produced[identity | tostring] == null)],
         unexpected: [$observed[] | select($by_identity[identity | tostring] == null)],
         incorrect: [$observed[] | . as $copy
                     | $by_identity[identity | tostring] as $wanted
                     | select($wanted != null and $copy != $wanted)
                     | {expected: $wanted, observed: $copy}]};

    ($accepted | sort_by(.sequence)) as $source
    | ($source | map(select(.sequence < 30))) as $expected_unique
    | ($source | group_by(.branch_name) | map(sort_by(.sequence) | complete_windows(.)) | add // [])
        as $expected_windows
    | judge($expected_unique; $unique; .event_id) as $deduplicator
    | judge($expected_windows; $windows; [.branch_name, .first_sequence]) as $window
    | {verdict: (if ([$deduplicator, $window]
                     | all((.missing | length) == 0 and (.unexpected | length) == 0
                           and (.incorrect | length) == 0))
                 then "pass" else "fail" end),
       accepted_records: ($accepted | length),
       deduplicator: $deduplicator,
       window: $window}
    ' >"${result_path}"

verdict="$(jq -r '.verdict' "${result_path}")"
printf 'backup state verdict: %s\n' "${verdict}"
for owner in deduplicator window; do
    printf '%s outputs: %s expected, %s observed, %s duplicate copies, %s missing, %s unexpected, %s incorrect\n' \
        "${owner}" \
        "$(jq -r --arg owner "${owner}" '.[$owner].expected' "${result_path}")" \
        "$(jq -r --arg owner "${owner}" '.[$owner].observed' "${result_path}")" \
        "$(jq -r --arg owner "${owner}" '.[$owner].duplicate_copies' "${result_path}")" \
        "$(jq -r --arg owner "${owner}" '.[$owner].missing | length' "${result_path}")" \
        "$(jq -r --arg owner "${owner}" '.[$owner].unexpected | length' "${result_path}")" \
        "$(jq -r --arg owner "${owner}" '.[$owner].incorrect | length' "${result_path}")"
done
[[ "${verdict}" == pass ]]
