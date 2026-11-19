#!/usr/bin/env bash
set -euo pipefail

# Owns the negative-outcome counters a strict verdict takes as input: the public counters with which
# the named ingestors and emitters of one domain report a negative acknowledgement or an undelivered
# record, read from retained /metrics scrapes of every node.
#
#   snapshot  sums each counter over the scrapes and records how many series it read, an absent
#             series counting as no series and zero: a unit not yet started has no counters, and
#             every counter of a started unit exists at zero.
#   compare   judges two snapshots of the same processes: every counter must be present after, none
#             may go back, which a process restart would cause, and none may increase.
#
# The ingestor counters are the acknowledgement waits an ingestor settled as failed, timed out or
# ended by its stop, the read positions its transport rejected, and the positions it stopped before
# its transport confirmed them. The emitter counters are the rows it gave up after a terminal
# publish failure, the rows its finish reported unsent, and the rows the destination refused.

usage() {
    cat >&2 <<'EOF'
usage: negative-outcomes.sh snapshot --domain DOMAIN [--ingestor NAME]... [--emitter NAME]...
           --output FILE METRICS_FILE...
       negative-outcomes.sh compare --before FILE --after FILE --output FILE
EOF
    exit 2
}

# kind, counter name, metric family and the label selecting its series, or none.
counter_definitions='ingestor ack_failed nervix_ingestor_ack_outcomes_total outcome="failed"
ingestor ack_timed_out nervix_ingestor_ack_outcomes_total outcome="timed_out"
ingestor ack_shutdown nervix_ingestor_ack_outcomes_total outcome="shutdown"
ingestor transport_rejections nervix_ingestor_transport_acknowledgements_total operation="reject"
ingestor unconfirmed_stop_positions nervix_ingestor_unconfirmed_stop_positions_total -
emitter records_failed nervix_emitter_records_failed_total -
emitter records_unsent nervix_emitter_unsent_records_total -
emitter records_rejected nervix_emitter_records_rejected_total -'

snapshot() {
    local domain=""
    local output=""
    local ingestors=()
    local emitters=()
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --domain) [[ "$#" -ge 2 ]] || usage; domain="$2"; shift 2 ;;
            --ingestor) [[ "$#" -ge 2 ]] || usage; ingestors+=("$2"); shift 2 ;;
            --emitter) [[ "$#" -ge 2 ]] || usage; emitters+=("$2"); shift 2 ;;
            --output) [[ "$#" -ge 2 ]] || usage; output="$2"; shift 2 ;;
            --) shift; break ;;
            -*) usage ;;
            *) break ;;
        esac
    done
    [[ -n "${domain}" && -n "${output}" && "$#" -gt 0 ]] || usage
    ((${#ingestors[@]} + ${#emitters[@]} > 0)) || usage
    local name
    for name in "${domain}" ${ingestors[@]+"${ingestors[@]}"} ${emitters[@]+"${emitters[@]}"}; do
        [[ "${name}" =~ ^[A-Za-z0-9_]+$ ]] || { printf 'not a Nervix name: %s\n' "${name}" >&2; exit 2; }
    done
    local metrics
    for metrics in "$@"; do
        [[ -s "${metrics}" ]] || { printf 'the scrape %s is missing or empty\n' "${metrics}" >&2; exit 2; }
    done
    local readings
    readings="$(awk -v domain="${domain}" -v definitions="${counter_definitions}" \
        -v ingestors="${ingestors[*]+"${ingestors[*]}"}" -v emitters="${emitters[*]+"${emitters[*]}"}" '
        function labelled(labels, pair) {
            return labels ~ ("[{,]" pair "[,}]")
        }
        BEGIN {
            count = split(definitions, rows, "\n")
            for (i = 1; i <= count; i++) {
                split(rows[i], fields, " ")
                kind[i] = fields[1]; counter[i] = fields[2]; family[i] = fields[3]
                selector[i] = (fields[4] == "-") ? "" : fields[4]
            }
            entity_count["ingestor"] = split(ingestors, ingestor_names, " ")
            entity_count["emitter"] = split(emitters, emitter_names, " ")
        }
        /^#/ || NF < 2 { next }
        {
            series = $1
            brace = index(series, "{")
            if (brace == 0) { next }
            name = substr(series, 1, brace - 1)
            labels = substr(series, brace)
            if (!labelled(labels, "domain=\"" domain "\"")) { next }
            if ($2 !~ /^[0-9]+(\.[0-9]+)?([eE][+]?[0-9]+)?$/) {
                printf "unreadable %s %s\n", FILENAME, series
                next
            }
            for (i = 1; i <= count; i++) {
                if (name != family[i]) { continue }
                if (selector[i] != "" && !labelled(labels, selector[i])) { continue }
                for (j = 1; j <= entity_count[kind[i]]; j++) {
                    entity = (kind[i] == "ingestor") ? ingestor_names[j] : emitter_names[j]
                    if (labelled(labels, kind[i] "=\"" entity "\"")) {
                        printf "reading %s %s %s %s\n", kind[i], entity, counter[i], $2
                    }
                }
            }
        }' "$@")"
    if grep -q '^unreadable ' <<<"${readings}"; then
        printf 'a counter value is not a number: %s\n' "$(grep -m 1 '^unreadable ' <<<"${readings}")" >&2
        exit 2
    fi
    jq -n --arg domain "${domain}" --arg readings "${readings}" --arg definitions "${counter_definitions}" \
        --argjson ingestors "$(printf '%s\n' ${ingestors[@]+"${ingestors[@]}"} | jq -R . | jq -s 'map(select(length > 0))')" \
        --argjson emitters "$(printf '%s\n' ${emitters[@]+"${emitters[@]}"} | jq -R . | jq -s 'map(select(length > 0))')" \
        --argjson metrics "$(printf '%s\n' "$@" | jq -R . | jq -s .)" '
        ($readings | split("\n") | map(select(startswith("reading ")) | split(" ")
            | {kind: .[1], entity: .[2], counter: .[3], value: (.[4] | tonumber)})) as $read
        | ($definitions | split("\n") | map(split(" ") | {kind: .[0], counter: .[1]})) as $counters
        | def entity_counters($kind; $names):
            reduce $names[] as $entity ({};
              .[$entity] = (reduce ($counters[] | select(.kind == $kind) | .counter) as $counter ({};
                .[$counter] = ([$read[] | select(.kind == $kind and .entity == $entity and .counter == $counter)]
                               | {value: (map(.value) | add // 0), series: length}))));
          {domain: $domain, metrics: $metrics,
           ingestors: entity_counters("ingestor"; $ingestors),
           emitters: entity_counters("emitter"; $emitters)}
        ' >"${output}"
}

compare() {
    local before=""
    local after=""
    local output=""
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --before) [[ "$#" -ge 2 ]] || usage; before="$2"; shift 2 ;;
            --after) [[ "$#" -ge 2 ]] || usage; after="$2"; shift 2 ;;
            --output) [[ "$#" -ge 2 ]] || usage; output="$2"; shift 2 ;;
            *) usage ;;
        esac
    done
    [[ -n "${before}" && -n "${after}" && -n "${output}" ]] || usage
    local snapshot_contract='type == "object" and (.domain | type == "string")
        and (.ingestors | type == "object") and (.emitters | type == "object")
        and ([.ingestors[], .emitters[] | .[] | (.value | type == "number" and . >= 0)
              and (.series | type == "number" and . >= 0)] | all)'
    local snapshot
    for snapshot in "${before}" "${after}"; do
        jq -e "${snapshot_contract}" "${snapshot}" >/dev/null 2>&1 \
            || { printf 'not a negative-outcome snapshot: %s\n' "${snapshot}" >&2; exit 2; }
    done
    jq -n --slurpfile before "${before}" --slurpfile after "${after}" '
        $before[0] as $b | $after[0] as $a
        | [("ingestors", "emitters") as $group
           | ($a[$group] | to_entries[]) as $entity
           | ($entity.value | to_entries[]) as $counter
           | ($b[$group][$entity.key][$counter.key] // {value: 0, series: 0}) as $earlier
           | {kind: (if $group == "ingestors" then "ingestor" else "emitter" end), entity: $entity.key,
              counter: $counter.key, before: $earlier.value, after: $counter.value.value,
              series_after: $counter.value.series,
              increase: ($counter.value.value - $earlier.value)}] as $counters
        | ([$counters[] | select(.series_after == 0)
            | "\(.kind) \(.entity) has no \(.counter) series after the run"]
           + [$counters[] | select(.increase < 0)
            | "\(.kind) \(.entity) \(.counter) went back from \(.before) to \(.after), so its process restarted"]
           + (if $a.domain == $b.domain then [] else ["the snapshots read different domains"] end)
           + ([$b.ingestors, $b.emitters | keys[]] - [$a.ingestors, $a.emitters | keys[]]
              | map("\(.) was read before the run and not after"))) as $invalid
        | ($counters | map(select(.increase > 0))) as $increases
        | {verdict: (if ($invalid | length) > 0 then "invalid"
                     elif ($increases | length) > 0 then "fail" else "pass" end),
           domain: $a.domain,
           invalid: $invalid,
           negative_acknowledgements: ([$increases[] | select(.kind == "ingestor") | .increase] | add // 0),
           emitter_failures: ([$increases[] | select(.kind == "emitter") | .increase] | add // 0),
           increases: $increases,
           counters: $counters}
        ' >"${output}"
    case "$(jq -r '.verdict' "${output}")" in
        pass) return 0 ;;
        fail) return 1 ;;
        *)
            jq -r '.invalid[]' "${output}" >&2
            exit 2
            ;;
    esac
}

[[ "$#" -ge 1 ]] || usage
command_name="$1"
shift
case "${command_name}" in
    snapshot) snapshot "$@" ;;
    compare) compare "$@" ;;
    *) usage ;;
esac
