#!/usr/bin/env bash
# Judges how a continuous load paced its records from the timestamps the broker stored.
#
# TIMESTAMPS holds one "CREATE_TIME_MS OFFSET" line per record of the load's topic, in offset
# order, as `kcat -C -f '%T %o\n'` prints them. A producer stamps a record's create time when it
# hands the record to its client, so the gaps between them are the gaps between the load's kcat
# calls. The load keeps two records at least half an interval apart, less the 10 ms its clock
# resolves; the verdict fails when two are closer than a quarter of the interval. A gap longer
# than the interval is reported and never fails: a held or stalled load is late, not bursting.
set -euo pipefail

usage() {
    printf 'usage: %s TIMESTAMPS INTERVAL_MS RESULT_JSON\n' "$(basename "$0")" >&2
}

write_error() {
    local category="$1"
    local message="$2"
    jq -n --arg category "${category}" --arg message "${message}" \
        '{verdict: "error", category: $category, message: $message}' >"${result_path}"
    printf 'load pacing error: %s\n' "${message}" >&2
}

if [[ "$#" -ne 3 ]]; then
    usage
    exit 2
fi

timestamps_path="$1"
interval_ms="$2"
result_path="$3"

mkdir -p "$(dirname "${result_path}")"

if [[ ! "${interval_ms}" =~ ^[1-9][0-9]*$ ]]; then
    write_error invalid_interval 'the load interval is not a positive whole number of milliseconds'
    exit 2
fi
if [[ ! -s "${timestamps_path}" ]]; then
    write_error invalid_timestamps 'the record timestamps are missing or empty'
    exit 2
fi
if grep -Evq '^[0-9]+ [0-9]+$' "${timestamps_path}"; then
    write_error invalid_timestamps 'a line is not a create time in milliseconds followed by an offset'
    exit 2
fi

jq -R -s --argjson interval_ms "${interval_ms}" '
    [split("\n")[] | select(length > 0) | split(" ") | {at_ms: (.[0] | tonumber), offset: (.[1] | tonumber)}]
    as $records
    | [range(1; $records | length)
       | {offset: $records[.].offset,
          follows: $records[. - 1].offset,
          gap_ms: ($records[.].at_ms - $records[. - 1].at_ms)}] as $gaps
    | if ([$gaps[] | select(.offset != .follows + 1)] | length) > 0 then
        {verdict: "error", category: "invalid_timestamps",
         message: "the timestamps do not cover consecutive offsets in order"}
      else
        ($interval_ms / 4 | floor) as $minimum_gap_ms
        | ($interval_ms * 3 / 2) as $late_gap_ms
        | ([$gaps[] | select(.gap_ms < $minimum_gap_ms) | {offset, gap_ms}]) as $early
        | ([$gaps[].gap_ms] | sort) as $sorted
        | ($sorted | length) as $count
        | {
            verdict: (if ($early | length) == 0 then "pass" else "fail" end),
            interval_ms: $interval_ms,
            records: ($records | length),
            span_ms: (if $count == 0 then 0 else $records[-1].at_ms - $records[0].at_ms end),
            gap_ms: (if $count == 0 then null else {
                minimum: $sorted[0],
                median: $sorted[($count / 2 | floor)],
                p95: $sorted[($count * 95 / 100 | floor)],
                maximum: $sorted[-1],
                mean: (($sorted | add) * 10 / $count | round / 10)
              } end),
            minimum_allowed_gap_ms: $minimum_gap_ms,
            early_gap_count: ($early | length),
            early_gaps: $early[:20],
            late_gap_count: ([$gaps[] | select(.gap_ms > $late_gap_ms)] | length),
            longest_gap: (if $count == 0 then null else
                first($gaps[] | select(.gap_ms == $sorted[-1]) | {offset, gap_ms}) end)
          }
      end
' "${timestamps_path}" >"${result_path}"

verdict="$(jq -r '.verdict' "${result_path}")"
if [[ "${verdict}" == error ]]; then
    printf 'load pacing error: %s\n' "$(jq -r '.message' "${result_path}")" >&2
    exit 2
fi
printf 'load pacing verdict: %s\n' "${verdict}"
printf 'declared interval: %s ms\n' "${interval_ms}"
printf 'records: %s\n' "$(jq -r '.records' "${result_path}")"
printf 'gaps (minimum/median/p95/maximum ms): %s\n' \
    "$(jq -r 'if .gap_ms == null then "none" else "\(.gap_ms.minimum)/\(.gap_ms.median)/\(.gap_ms.p95)/\(.gap_ms.maximum)" end' "${result_path}")"
printf 'gaps under %s ms: %s\n' "$(jq -r '.minimum_allowed_gap_ms' "${result_path}")" \
    "$(jq -r '.early_gap_count' "${result_path}")"
printf 'gaps over one and a half intervals: %s\n' "$(jq -r '.late_gap_count' "${result_path}")"

if [[ "${verdict}" != pass ]]; then
    exit 1
fi
