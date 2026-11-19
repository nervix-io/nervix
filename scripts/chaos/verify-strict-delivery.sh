#!/usr/bin/env bash
set -euo pipefail

# The strict verdict of an operation meant to lose and repeat nothing. It judges the records several
# sources accepted against what one output holds, each source by its own exact ledger, and admits
# no tolerance: no duplicate of any kind, replay duplicates included, and no missing, unexpected or
# changed record. Two optional inputs join the verdict: the negative-outcome counters of the
# domain's ingestors and emitters (negative-outcomes.sh), which may not increase, and the error-event
# observers' transcripts (events-observer.sh), which may hold no error event inside their window.
#
# Each source's ledger is verify-ledger.sh without --allow-replay-duplicates, over the output records
# whose event_id the source accepted. An output record no source accepted is unexpected. The verdict
# names every violation as a symptom, which a scenario reports and an expected failure matches.
#
# Exit status: 0 pass, 1 fail, 2 evidence that cannot be judged.

usage() {
    cat >&2 <<'EOF'
usage: verify-strict-delivery.sh --output OBSERVED_NDJSON --result RESULT_JSON
           --source NAME=ACCEPTED_NDJSON [--source NAME=ACCEPTED_NDJSON]...
           [--negative-outcomes BEFORE_JSON AFTER_JSON]
           [--events-window WINDOW_JSON --events TRANSCRIPT [--events TRANSCRIPT]...]
EOF
    exit 2
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
observed=""
result=""
sources=()
outcomes_before=""
outcomes_after=""
events_window=""
transcripts=()
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --output) [[ "$#" -ge 2 ]] || usage; observed="$2"; shift 2 ;;
        --result) [[ "$#" -ge 2 ]] || usage; result="$2"; shift 2 ;;
        --source) [[ "$#" -ge 2 ]] || usage; sources+=("$2"); shift 2 ;;
        --negative-outcomes) [[ "$#" -ge 3 ]] || usage; outcomes_before="$2"; outcomes_after="$3"; shift 3 ;;
        --events-window) [[ "$#" -ge 2 ]] || usage; events_window="$2"; shift 2 ;;
        --events) [[ "$#" -ge 2 ]] || usage; transcripts+=("$2"); shift 2 ;;
        *) usage ;;
    esac
done
[[ -n "${observed}" && -n "${result}" && "${#sources[@]}" -gt 0 ]] || usage
if [[ -n "${events_window}" && "${#transcripts[@]}" -eq 0 ]] \
    || [[ -z "${events_window}" && "${#transcripts[@]}" -gt 0 ]]; then
    usage
fi

mkdir -p "$(dirname "${result}")"
work="${result%.json}"
invalid() {
    jq -n --arg message "$*" '{verdict: "invalid", message: $message}' >"${result}"
    printf 'strict delivery evidence cannot be judged: %s\n' "$*" >&2
    exit 2
}

[[ -e "${observed}" ]] || invalid "the observed output ${observed} does not exist"
if [[ -s "${observed}" ]] && ! jq -e -s 'all(type == "object" and (.event_id | type == "string" and length > 0))' \
    "${observed}" >/dev/null 2>&1; then
    invalid 'the observed output holds a record without an event_id'
fi

# Every source's accepted ledger, keyed by its name. Their event IDs must be disjoint, so every
# output record belongs to at most one source.
declare -A accepted_of=()
names=()
for source in "${sources[@]}"; do
    name="${source%%=*}"
    accepted="${source#*=}"
    [[ "${name}" =~ ^[a-z][a-z0-9-]*$ && "${accepted}" != "${source}" ]] \
        || invalid "a source is NAME=ACCEPTED_NDJSON with a lowercase name, not ${source}"
    [[ -z "${accepted_of[${name}]:-}" ]] || invalid "the source ${name} is named twice"
    [[ -s "${accepted}" ]] || invalid "the accepted ledger of ${name} is missing or empty"
    jq -e -s 'all(type == "object" and (.event_id | type == "string" and length > 0))' "${accepted}" \
        >/dev/null 2>&1 || invalid "the accepted ledger of ${name} holds a record without an event_id"
    accepted_of["${name}"]="${accepted}"
    names+=("${name}")
done
shared="$(for name in "${names[@]}"; do jq -r '.event_id' "${accepted_of[${name}]}" | sort -u; done | sort | uniq -d | head -n 1)"
[[ -z "${shared}" ]] || invalid "the event_id ${shared} belongs to more than one source"

# Partitions the output by the source whose ledger holds each record's event_id.
for name in "${names[@]}"; do
    jq -c -n --slurpfile accepted "${accepted_of[${name}]}" \
        '($accepted | map({key: .event_id, value: true}) | from_entries) as $ids
         | inputs | select($ids[.event_id] == true)' "${observed}" >"${work}-${name}-observed.ndjson"
done
jq -c -n --slurpfile accepted <(for name in "${names[@]}"; do cat "${accepted_of[${name}]}"; done) \
    '($accepted | map({key: .event_id, value: true}) | from_entries) as $ids
     | inputs | select($ids[.event_id] != true)' "${observed}" >"${work}-unattributed.ndjson"

# Each source's exact ledger, strict.
for name in "${names[@]}"; do
    status=0
    "${script_dir}/verify-ledger.sh" "${accepted_of[${name}]}" "${work}-${name}-observed.ndjson" \
        "${work}-${name}.json" >"${work}-${name}.txt" 2>&1 || status=$?
    ((status <= 1)) || invalid "the ledger of ${name} cannot be judged: $(jq -r '.message // empty' "${work}-${name}.json" 2>/dev/null)"
done

# The negative-outcome counters, which may not increase.
outcomes_json=null
if [[ -n "${outcomes_before}" ]]; then
    status=0
    "${script_dir}/negative-outcomes.sh" compare --before "${outcomes_before}" --after "${outcomes_after}" \
        --output "${work}-negative-outcomes.json" 2>"${work}-negative-outcomes.txt" || status=$?
    ((status <= 1)) || invalid "the negative-outcome counters cannot be judged: $(head -n 1 "${work}-negative-outcomes.txt")"
    outcomes_json="$(cat "${work}-negative-outcomes.json")"
fi

# The error events every observer saw inside the window. A seen event is conclusive. Seeing none
# needs complete observation: every observer attached before the window opened and kept following
# until it closed, without its follow ending or its CLI reporting notices it lost.
events_json=null
if [[ -n "${events_window}" ]]; then
    jq -e '(.start_ns | type == "number") and (.end_ns | type == "number") and .end_ns >= .start_ns' \
        "${events_window}" >/dev/null 2>&1 || invalid "the error-event window ${events_window} has no start and end"
    observers='{}'
    for transcript in "${transcripts[@]}"; do
        [[ -e "${transcript}" ]] || invalid "the error-event transcript ${transcript} does not exist"
        observer="$(basename "${transcript}" .log)"
        observers="$(jq -c --arg observer "${observer}" --slurpfile window "${events_window}" \
            --rawfile transcript "${transcript}" '
            $window[0] as $w
            | ($transcript | split("\n") | map(select(length > 0)
                | capture("^(?<ns>[0-9]+) (?<text>.*)$")? // {ns: null, text: .}
                | .ns |= (if . == null then null else tonumber end))) as $lines
            | ($lines | map(select(.ns != null and .ns >= $w.start_ns and .ns <= $w.end_ns))) as $inside
            | .[$observer] = {
                attached_before_window: any($lines[]; .ns != null and .ns <= $w.start_ns
                                            and (.text | contains("listening for events from relay"))),
                unstamped_lines: ($lines | map(select(.ns == null)) | length),
                gaps: [$inside[] | select((.text | startswith("observer: the notice follow ended"))
                                         or (.text | startswith("[events] notice: ")))],
                events: [$inside[] | select((.text | startswith("[events] runtime "))
                                           or (.text | test("^\\[events\\] control plane (WARN|ERROR): ")))]
              }' <<<"${observers}")"
    done
    events_json="$(jq -c --slurpfile window "${events_window}" '
        {window: $window[0], observers: .,
         count: ([.[].events[]] | length),
         distinct: ([.[].events[].text] | unique),
         complete: all(.[]; .attached_before_window and (.gaps | length) == 0 and .unstamped_lines == 0)}
        ' <<<"${observers}")"
    if [[ "$(jq -r '.count' <<<"${events_json}")" -eq 0 && "$(jq -r '.complete' <<<"${events_json}")" != true ]]; then
        jq -n --argjson events "${events_json}" '{verdict: "invalid", message: "an error-event observer did not follow the whole window", error_events: $events}' >"${result}"
        printf 'strict delivery evidence cannot be judged: an error-event observer did not follow the whole window\n' >&2
        exit 2
    fi
fi

ledgers='{}'
for name in "${names[@]}"; do
    ledgers="$(jq -c --arg name "${name}" --slurpfile ledger "${work}-${name}.json" \
        '.[$name] = $ledger[0]' <<<"${ledgers}")"
done

jq -n --argjson ledgers "${ledgers}" --slurpfile unattributed "${work}-unattributed.ndjson" \
    --argjson outcomes "${outcomes_json}" --argjson events "${events_json}" '
    def per_source(f): [$ledgers | to_entries[] | {source: .key, count: (.value | f)} | select(.count > 0)];
    (per_source(.duplicate_records)) as $duplicates
    | (per_source(.missing_ids | length)) as $missing
    | (per_source(.incorrect_content | length)) as $changed
    | ([$unattributed[].event_id] | unique) as $unexpected
    | ([if ($duplicates | length) > 0 then {name: "duplicate-records", sources: $duplicates} else empty end,
        if ($missing | length) > 0 then {name: "missing-records", sources: $missing} else empty end,
        if ($changed | length) > 0 then {name: "changed-records", sources: $changed} else empty end,
        if ($unexpected | length) > 0 then {name: "unexpected-records", count: ($unexpected | length)} else empty end,
        if $outcomes != null and $outcomes.negative_acknowledgements > 0
        then {name: "negative-acknowledgements", count: $outcomes.negative_acknowledgements,
              increases: [$outcomes.increases[] | select(.kind == "ingestor")]} else empty end,
        if $outcomes != null and $outcomes.emitter_failures > 0
        then {name: "emitter-failures", count: $outcomes.emitter_failures,
              increases: [$outcomes.increases[] | select(.kind == "emitter")]} else empty end,
        if $events != null and $events.count > 0
        then {name: "error-events", count: $events.count, distinct: $events.distinct} else empty end
       ]) as $symptoms
    | {verdict: (if ($symptoms | length) == 0 then "pass" else "fail" end),
       symptoms: $symptoms,
       sources: $ledgers,
       unexpected: {records: ($unattributed | length), ids: $unexpected},
       negative_outcomes: $outcomes,
       error_events: $events}
    ' >"${result}"

verdict="$(jq -r '.verdict' "${result}")"
printf 'strict delivery verdict: %s\n' "${verdict}"
jq -r '.sources | to_entries[] | "\(.key): expected \(.value.expected_records), observed \(.value.observed_records), duplicates \(.value.duplicate_records), missing \(.value.missing_ids | length), changed \(.value.incorrect_content | length)"' "${result}"
jq -r '"unexpected records: \(.unexpected.records)"' "${result}"
jq -r 'if .negative_outcomes == null then empty else "negative acknowledgements: \(.negative_outcomes.negative_acknowledgements), emitter failures: \(.negative_outcomes.emitter_failures)" end' "${result}"
jq -r 'if .error_events == null then empty else "error events: \(.error_events.count)" end' "${result}"
jq -r '.symptoms[] | "symptom: \(.name)"' "${result}"
[[ "${verdict}" == pass ]] || exit 1
