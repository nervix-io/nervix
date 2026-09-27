#!/usr/bin/env bash
set -euo pipefail

usage() {
    printf 'usage: %s EXPECTED_NDJSON OBSERVED_NDJSON RESULT_JSON [--allow-replay-duplicates]\n' "$(basename "$0")" >&2
}

write_error() {
    local category="$1"
    local message="$2"
    jq -n --arg category "${category}" --arg message "${message}" \
        '{verdict: "error", category: $category, message: $message}' >"${result_path}"
}

if [[ "$#" -ne 3 && "$#" -ne 4 ]]; then
    usage
    exit 2
fi

expected_path="$1"
observed_path="$2"
result_path="$3"
allow_replay_duplicates=false
if [[ "$#" -eq 4 ]]; then
    [[ "$4" == --allow-replay-duplicates ]] || { usage; exit 2; }
    allow_replay_duplicates=true
fi

mkdir -p "$(dirname "${result_path}")"

if [[ ! -s "${expected_path}" ]]; then
    write_error "invalid_expected_ledger" "expected ledger is missing or empty"
    exit 2
fi
if [[ ! -s "${observed_path}" ]]; then
    if [[ ! -e "${observed_path}" ]]; then
        : >"${observed_path}"
    fi
fi

record_contract='type == "object"
    and (.event_id | type == "string" and length > 0)
    and (.branch_name | type == "string" and length > 0)
    and (.sequence | type == "number" and isfinite and floor == .)
    and (.content | type == "string")'

if ! jq -e -c "${record_contract}" "${expected_path}" >/dev/null 2>&1; then
    write_error "invalid_expected_ledger" "expected ledger contains invalid JSON or a record outside the fixture contract"
    exit 2
fi

if [[ -s "${observed_path}" ]] \
    && ! jq -e -c "${record_contract}" "${observed_path}" >/dev/null 2>&1; then
    write_error "invalid_observed_ledger" "observed ledger contains invalid JSON or a record outside the fixture contract"
    exit 2
fi

expected_duplicates="$(jq -s '[group_by(.event_id)[] | select(length > 1) | .[0].event_id]' "${expected_path}")"
if [[ "$(jq 'length' <<<"${expected_duplicates}")" -ne 0 ]]; then
    write_error "invalid_expected_ledger" "expected ledger contains duplicate event_id values"
    exit 2
fi

jq -n \
    --argjson allow_replay_duplicates "${allow_replay_duplicates}" \
    --slurpfile expected "${expected_path}" \
    --slurpfile observed "${observed_path}" '
    def ids($rows): [$rows[].event_id] | unique;
    def duplicates($rows):
      [$rows
       | group_by(.event_id)[]
       | select(length > 1)
       | {event_id: .[0].event_id, count: length}];

    (ids($expected)) as $expected_ids
    | (ids($observed)) as $observed_ids
    | (duplicates($observed)) as $duplicates
    | ([$expected_ids[] | select(. as $id | $observed_ids | index($id) | not)]) as $missing
    | ([$observed_ids[] | select(. as $id | $expected_ids | index($id) | not)]) as $unexpected
    | ([$expected[] as $wanted
        | $observed[]
        | select(.event_id == $wanted.event_id and . != $wanted)
        | {event_id: .event_id, expected: $wanted, observed: .}]) as $incorrect
    | {
        verdict: (if ((($duplicates | length) == 0 or $allow_replay_duplicates)
                      and ($missing | length) == 0
                      and ($unexpected | length) == 0
                      and ($incorrect | length) == 0)
                  then "pass" else "fail" end),
        expected_records: ($expected | length),
        observed_records: ($observed | length),
        replay_duplicates_allowed: $allow_replay_duplicates,
        duplicate_records: ($duplicates | map(.count - 1) | add // 0),
        duplicates: $duplicates,
        missing_ids: $missing,
        unexpected_ids: $unexpected,
        incorrect_content: $incorrect
      }
    ' >"${result_path}"

verdict="$(jq -r '.verdict' "${result_path}")"
printf 'ledger verdict: %s\n' "${verdict}"
printf 'expected records: %s\n' "$(jq -r '.expected_records' "${result_path}")"
printf 'observed records: %s\n' "$(jq -r '.observed_records' "${result_path}")"
printf 'duplicate IDs: %s\n' "$(jq -r '.duplicates | length' "${result_path}")"
printf 'missing IDs: %s\n' "$(jq -r '.missing_ids | length' "${result_path}")"
printf 'unexpected IDs: %s\n' "$(jq -r '.unexpected_ids | length' "${result_path}")"
printf 'incorrect content: %s\n' "$(jq -r '.incorrect_content | length' "${result_path}")"

if [[ "${verdict}" != "pass" ]]; then
    exit 1
fi
