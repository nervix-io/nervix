#!/usr/bin/env bash
set -euo pipefail

# Decides the verdicts of the endpoints scenario from external ledgers: every attempt the endpoint
# client made, the records it sent, the Kafka output of each domain's HTTP route and of the
# WebSocket route in offset order, and the phase boundaries the runner observed. Every mode writes
# its verdict to --result and exits 1 when the evidence contradicts the contract it checks or does
# not exercise it.
#
# HTTP records are numbered from 1 in the order the client sent them, and a record's sequence is
# its number. The client finishes every attempt of a record before it sends the next one, so a
# phase boundary is a record number: the runner held the client after record `withdrawn_after`,
# stopped the peer domain, and held it again after record `reshared_after` to start the peer domain
# again. Records between the two boundaries reached a withdrawn peer route.

usage() {
    cat >&2 <<'EOF'
usage:
  verify-endpoint-evidence.sh http --domain primary|peer --attempts FILE --input FILE
      --output FILE --phases FILE --result FILE
  verify-endpoint-evidence.sh websocket --frames FILE --input FILE --output FILE --result FILE

http requires every record the endpoint answered with HTTP 200 while the domain's route was bound
to reach the domain's output exactly once with its exact content. A record that the domain's
route served while it was withdrawn must never reach that domain. A record may reach a domain more
than once, or without a 200, only when one of its attempts ended without an answer or with an
answer that does not refuse admission, because such an attempt may have completed. Every record
must eventually be answered with HTTP 200, and the client must reach the end of its input.

websocket requires the retained connection to be accepted and its frames to reach the output
exactly once, the frame sent on it after the source was replaced to be refused with close code
1013 and never delivered, and the frames of the new connection to reach the output exactly once.
EOF
}

fail_usage() {
    printf '%s\n' "$*" >&2
    usage
    exit 2
}

mode="${1:-}"
[[ -n "${mode}" ]] || fail_usage 'a verifier mode is required'
shift
domain=""
attempts=""
frames=""
input=""
output=""
phases=""
result=""
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --domain | --attempts | --frames | --input | --output | --phases | --result)
            [[ "$#" -ge 2 ]] || fail_usage "$1 requires a value"
            case "$1" in
                --domain) domain="$2" ;;
                --attempts) attempts="$2" ;;
                --frames) frames="$2" ;;
                --input) input="$2" ;;
                --output) output="$2" ;;
                --phases) phases="$2" ;;
                --result) result="$2" ;;
            esac
            shift 2
            ;;
        *) fail_usage "unknown ${mode} argument: $1" ;;
    esac
done
[[ -n "${result}" ]] || fail_usage '--result is required'
[[ -s "${input}" ]] || fail_usage "the sent records are missing or empty: ${input}"
[[ -f "${output}" ]] || fail_usage "the output ledger is missing: ${output}"

case "${mode}" in
    http)
        [[ "${domain}" == primary || "${domain}" == peer ]] || fail_usage '--domain must be primary or peer'
        [[ -s "${attempts}" ]] || fail_usage "the client attempts are missing or empty: ${attempts}"
        [[ -s "${phases}" ]] || fail_usage "the observed phases are missing: ${phases}"
        jq -e '
            (.withdrawn_after | type == "number") and (.reshared_after | type == "number")
            and .withdrawn_after >= 1 and .reshared_after > .withdrawn_after
        ' "${phases}" >/dev/null 2>&1 || fail_usage "the phases do not match their contract: ${phases}"
        # The dollars in this program are jq variables, not shell expansion.
        # shellcheck disable=SC2016
        jq -n \
            --arg domain "${domain}" \
            --slurpfile phases "${phases}" \
            --slurpfile sent "${input}" \
            --slurpfile ledger "${attempts}" \
            --slurpfile delivered "${output}" '
            $phases[0] as $p
            | ($sent | map({key: (.sequence | tostring), value: .}) | from_entries) as $records
            | [$ledger[] | select(.kind == "http_attempt")] as $tries
            | ($tries | group_by(.sequence) | map({key: (.[0].sequence | tostring), value: .}) | from_entries)
                as $by_sequence
            # A domain route is bound for every record outside the peer withdrawal.
            | def bound($sequence):
                $domain == "primary"
                or $sequence <= $p.withdrawn_after or $sequence > $p.reshared_after;
            # An attempt that ended without an answer, or with an answer that does not refuse
            # admission, may have completed: the endpoint holds its outcome back or lost the
            # connection while the graph worked.
              def uncertain($try):
                $try.status == null
                or ($try.status != 200 and $try.status != 404 and $try.status != 400
                    and (($try.outcome.outcome // "") != "not_admitted"));
              def accepted($sequence): any($by_sequence[$sequence | tostring][]?; .status == 200);
              def may_repeat($sequence): any($by_sequence[$sequence | tostring][]?; uncertain(.));
            ($delivered | group_by(.sequence) | map({sequence: .[0].sequence, count: length, records: .}))
                as $counts
            | ($counts | map({key: (.sequence | tostring), value: .count}) | from_entries) as $delivered_count
            | [$records | keys[] | tonumber] as $sequences
            | [$sequences[] | select(accepted(.) and bound(.) and (($delivered_count[tostring] // 0) == 0))]
                as $missing
            | [$counts[] | .sequence as $s
               | select(($records[$s | tostring] == null) or (bound($s) | not)
                        or ((accepted($s) | not) and (may_repeat($s) | not)))
               | $s] as $unexpected
            | [$counts[] | select(.count > 1 and (may_repeat(.sequence) | not)) | .sequence] as $duplicated
            | [$delivered[] | . as $record
               | select($records[$record.sequence | tostring] != null and $records[$record.sequence | tostring] != $record)
               | .sequence] | unique as $altered
            | [$sequences[] | select(accepted(.) | not)] as $never_accepted
            | [$ledger[] | select(.kind == "http_abandoned") | .sequence] as $abandoned
            | ([$ledger[] | select(.kind == "http_finished")] | length) as $finished
            | [$tries[] | select(.status == 404 and bound(.sequence)) | .sequence] | unique as $withdrawn_answers
            | [$sequences[] | select(bound(.) | not)] as $withdrawn_records
            | [
                (if $missing != [] then "accepted records missing from the \($domain) output: \($missing)" else empty end),
                (if $unexpected != [] then "records the \($domain) route never accepted while bound reached it: \($unexpected)" else empty end),
                (if $duplicated != [] then "records the \($domain) output repeated without an uncertain attempt: \($duplicated)" else empty end),
                (if $altered != [] then "records whose \($domain) output differs from the record sent: \($altered)" else empty end),
                (if $never_accepted != [] then "records the endpoint never answered with HTTP 200: \($never_accepted)" else empty end),
                (if $abandoned != [] then "records the client abandoned at their deadline: \($abandoned)" else empty end),
                (if $finished != 1 then "the client did not reach the end of its input" else empty end),
                (if $withdrawn_answers != [] then "a bound route answered HTTP 404 for records \($withdrawn_answers)" else empty end),
                (if $domain == "peer" and $withdrawn_records == [] then "no record exercised the withdrawn peer route" else empty end)
              ] as $failures
            | {processor: "http-\($domain)", verdict: (if $failures == [] then "pass" else "fail" end),
               failures: $failures,
               records: ($sequences | length),
               accepted: ([$sequences[] | select(accepted(.))] | length),
               bound_records: ([$sequences[] | select(bound(.))] | length),
               withdrawn_records: ($withdrawn_records | length),
               delivered: ($delivered | length),
               attempts: ($tries | length),
               uncertain_attempts: ([$tries[] | select(uncertain(.))] | length),
               permitted_repeats: ([$counts[] | select(.count > 1 and may_repeat(.sequence))] | length)}
        ' >"${result}"
        ;;
    websocket)
        [[ -s "${frames}" ]] || fail_usage "the WebSocket ledger is missing or empty: ${frames}"
        # shellcheck disable=SC2016
        jq -n \
            --slurpfile sent "${input}" \
            --slurpfile ledger "${frames}" \
            --slurpfile delivered "${output}" '
            ($sent | map({key: (.sequence | tostring), value: .}) | from_entries) as $records
            | [$ledger[] | select(.kind == "ws_open")] as $opens
            | [$ledger[] | select(.kind == "ws_sent")] as $sends
            | [$sends[] | select(.connection == "retained") | .sequence] as $retained
            | [$sends[] | select(.connection == "replacement") | .sequence] as $replacement
            | ([$ledger[] | select(.kind == "ws_probe_sent") | .sequence] | first) as $probe
            | ([$ledger[] | select(.kind == "ws_retained_closed")] | first) as $closed
            | ($delivered | group_by(.sequence) | map({key: (.[0].sequence | tostring), value: length}) | from_entries)
                as $count
            | [($retained + $replacement)[] | select(($count[tostring] // 0) != 1)] as $not_once
            | [$delivered[] | .sequence | select((. as $s | ($retained + $replacement) | index($s)) == null)]
                | unique as $unexpected
            | [$delivered[] | . as $record
               | select($records[$record.sequence | tostring] != null and $records[$record.sequence | tostring] != $record)
               | .sequence] | unique as $altered
            | [
                (if ([$opens[] | select(.connection == "retained" and .accepted)] | length) != 1
                 then "the retained connection was not accepted" else empty end),
                (if ([$opens[] | select(.connection == "replacement" and .accepted)] | length) != 1
                 then "the replacement connection was not accepted" else empty end),
                (if $retained == [] then "the retained connection sent no frame" else empty end),
                (if $replacement == [] then "the replacement connection sent no frame" else empty end),
                (if $probe == null then "no frame was sent on the retained connection after the source was replaced" else empty end),
                (if $closed == null then "the retained connection was not observed closing"
                 elif $closed.code != 1013 then "the retained connection closed with \($closed.code // "no code") (\($closed.closed)), not 1013"
                 else empty end),
                (if $probe != null and (($count[$probe | tostring] // 0) > 0) then "the frame sent after the source was replaced was delivered" else empty end),
                (if $not_once != [] then "frames not delivered exactly once: \($not_once)" else empty end),
                (if ($unexpected - [$probe]) != [] then "frames no connection sent reached the output: \($unexpected - [$probe])" else empty end),
                (if $altered != [] then "frames whose output differs from the frame sent: \($altered)" else empty end)
              ] as $failures
            | {processor: "websocket", verdict: (if $failures == [] then "pass" else "fail" end),
               failures: $failures, retained_frames: ($retained | length),
               replacement_frames: ($replacement | length), probe: $probe,
               retained_close: $closed, delivered: ($delivered | length)}
        ' >"${result}"
        ;;
    *)
        fail_usage "unknown verifier mode: ${mode}"
        ;;
esac
jq -e '.verdict == "pass"' "${result}" >/dev/null || exit 1
