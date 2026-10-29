#!/usr/bin/env bash
set -euo pipefail

# Self-test of the endpoints scenario: its option checks and every route verdict of
# verify-endpoint-evidence.sh on passing evidence, on lost, unexpected, repeated and altered records,
# on records the endpoint never accepted, on a retained WebSocket that was not refused or whose
# refused frame was delivered, and on evidence that does not exercise the contract.

chaos_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
verifier="${chaos_dir}/verify-endpoint-evidence.sh"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

fail() {
    printf 'endpoint self-test failed: %s\n' "$*" >&2
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
        || fail "${case_name} did not explain the rejected option"
}

# Runs the verifier, which must accept its evidence.
expect_pass() {
    local case_name="$1"
    shift
    "$@" >"${tmp_dir}/pass.out" 2>&1 \
        || fail "${case_name} was rejected: $(cat "${tmp_dir}/pass.out") $(cat "${tmp_dir}/result.json" 2>/dev/null)"
}

# Runs the verifier, which must reject its evidence with verdict 1 and name FAILURE.
expect_failure() {
    local case_name="$1"
    local failure="$2"
    shift 2
    local status=0
    "$@" >"${tmp_dir}/failure.out" 2>&1 || status=$?
    [[ "${status}" -eq 1 ]] || fail "${case_name} returned ${status}, expected verdict 1: $(cat "${tmp_dir}/failure.out")"
    jq -e --arg failure "${failure}" 'any(.failures[]; contains($failure))' "${tmp_dir}/result.json" >/dev/null \
        || fail "${case_name} did not report '${failure}': $(jq -c '.failures' "${tmp_dir}/result.json")"
}

list_output="$("${chaos_dir}/chaos.sh" list)"
grep -Fq 'endpoints' <<<"${list_output}" || fail 'scenario list omits endpoints'
expect_setup_rejection 'endpoints fault is validated' '--fault for endpoints must be none, owner-crash' \
    "${chaos_dir}/run-baseline.sh" --scenario endpoints --image fixture --fault owner-pause
expect_setup_rejection 'one node supports only restart faults' 'owner-crash requires --nodes 3' \
    "${chaos_dir}/run-baseline.sh" --scenario endpoints --image fixture --nodes 1 --fault owner-crash

# Ten HTTP records: the peer route is withdrawn for records 4 to 6. Record 5 is first answered 503
# with an outcome the endpoint could not decide, record 8 first loses its connection; each is then
# accepted, so both may reach an output twice.
http_input="${tmp_dir}/http-input.ndjson"
jq -nc 'range(1; 11) as $s | {event_id: ("run-http-\($s)"), sequence: $s, content: ("chaos-endpoint-http-payload-\($s)")}' \
    >"${http_input}"
http_attempts="${tmp_dir}/http-attempts.ndjson"
{
    jq -nc 'range(1; 11) as $s | select($s != 5 and $s != 8)
        | {kind: "http_attempt", sequence: $s, attempt: 0, target: "nervix-1:8080", status: 200}'
    jq -nc '{kind: "http_attempt", sequence: 5, attempt: 0, target: "nervix-2:8080", status: 503,
             retry_after: "1", outcome: {outcome: "outcome_unknown", cause: "interrupted"}}'
    jq -nc '{kind: "http_attempt", sequence: 5, attempt: 1, target: "nervix-3:8080", status: 200}'
    jq -nc '{kind: "http_attempt", sequence: 8, attempt: 0, target: "nervix-1:8080", status: null,
             error: "ConnectionRefusedError: [Errno 111] Connection refused"}'
    jq -nc '{kind: "http_attempt", sequence: 8, attempt: 1, target: "nervix-2:8080", status: 200}'
    jq -nc '{kind: "http_finished", produced: 10}'
} >"${http_attempts}"
phases="${tmp_dir}/phases.json"
jq -n '{withdrawn_after: 3, reshared_after: 6}' >"${phases}"
primary_output="${tmp_dir}/primary.ndjson"
jq -c '.' "${http_input}" >"${primary_output}"
jq -c 'select(.sequence == 5)' "${http_input}" >>"${primary_output}"
peer_output="${tmp_dir}/peer.ndjson"
jq -c 'select(.sequence <= 3 or .sequence > 6)' "${http_input}" >"${peer_output}"
jq -c 'select(.sequence == 8)' "${http_input}" >>"${peer_output}"

http_verdict() {
    local domain="$1"
    local output="$2"
    local attempts="${3:-${http_attempts}}"
    local phase_file="${4:-${phases}}"
    "${verifier}" http --domain "${domain}" --attempts "${attempts}" --input "${http_input}" \
        --output "${output}" --phases "${phase_file}" --result "${tmp_dir}/result.json"
}

expect_pass 'primary output of every accepted record' http_verdict primary "${primary_output}"
jq -e '.verdict == "pass" and .records == 10 and .accepted == 10 and .delivered == 11
       and .uncertain_attempts == 2 and .permitted_repeats == 1' "${tmp_dir}/result.json" >/dev/null \
    || fail "the primary verdict summary is wrong: $(cat "${tmp_dir}/result.json")"
expect_pass 'peer output outside its withdrawal' http_verdict peer "${peer_output}"
jq -e '.withdrawn_records == 3 and .bound_records == 7' "${tmp_dir}/result.json" >/dev/null \
    || fail "the peer verdict did not count its withdrawn records: $(cat "${tmp_dir}/result.json")"

jq -c 'select(.sequence != 2)' "${primary_output}" >"${tmp_dir}/lost.ndjson"
expect_failure 'a lost accepted record' 'accepted records missing from the primary output: [2]' \
    http_verdict primary "${tmp_dir}/lost.ndjson"
{ cat "${peer_output}"; jq -c 'select(.sequence == 4)' "${http_input}"; } >"${tmp_dir}/withdrawn.ndjson"
expect_failure 'a record served while withdrawn' 'never accepted while bound reached it: [4]' \
    http_verdict peer "${tmp_dir}/withdrawn.ndjson"
{ cat "${primary_output}"; jq -c 'select(.sequence == 1)' "${http_input}"; } >"${tmp_dir}/repeated.ndjson"
expect_failure 'a repeat without an uncertain attempt' 'repeated without an uncertain attempt: [1]' \
    http_verdict primary "${tmp_dir}/repeated.ndjson"
jq -c 'if .sequence == 7 then .content = "altered" else . end' "${primary_output}" >"${tmp_dir}/altered.ndjson"
expect_failure 'an altered record' 'differs from the record sent: [7]' \
    http_verdict primary "${tmp_dir}/altered.ndjson"
{ cat "${primary_output}"; jq -nc '{event_id: "run-http-11", sequence: 11, content: "never sent"}'; } \
    >"${tmp_dir}/unsent.ndjson"
expect_failure 'a record no attempt sent' 'never accepted while bound reached it: [11]' \
    http_verdict primary "${tmp_dir}/unsent.ndjson"

jq -c 'select(.sequence != 10 or .kind != "http_attempt")' "${http_attempts}" >"${tmp_dir}/abandoned-attempts.ndjson"
{
    cat "${tmp_dir}/abandoned-attempts.ndjson"
    jq -nc '{kind: "http_attempt", sequence: 10, attempt: 0, target: "nervix-1:8080", status: 503,
             outcome: {outcome: "not_admitted", cause: "busy"}}'
    jq -nc '{kind: "http_abandoned", sequence: 10, attempts: 1}'
} >"${tmp_dir}/abandoned.ndjson"
jq -c 'select(.sequence != 10)' "${primary_output}" >"${tmp_dir}/without-ten.ndjson"
expect_failure 'a record never accepted' 'never answered with HTTP 200: [10]' \
    http_verdict primary "${tmp_dir}/without-ten.ndjson" "${tmp_dir}/abandoned.ndjson"
expect_failure 'an abandoned record' 'abandoned at their deadline: [10]' \
    http_verdict primary "${tmp_dir}/without-ten.ndjson" "${tmp_dir}/abandoned.ndjson"
expect_failure 'a refused record that was still delivered' 'never accepted while bound reached it: [10]' \
    http_verdict primary "${primary_output}" "${tmp_dir}/abandoned.ndjson"
jq -c 'select(.kind != "http_finished")' "${http_attempts}" >"${tmp_dir}/unfinished.ndjson"
expect_failure 'a client that stopped early' 'did not reach the end of its input' \
    http_verdict primary "${primary_output}" "${tmp_dir}/unfinished.ndjson"
{
    jq -c 'select(.kind != "http_finished")' "${http_attempts}"
    jq -nc '{kind: "http_attempt", sequence: 9, attempt: 1, target: "nervix-3:8080", status: 404}'
    jq -nc '{kind: "http_finished", produced: 10}'
} >"${tmp_dir}/withdrawn-answer.ndjson"
expect_failure 'a bound route answering 404' 'answered HTTP 404 for records [9]' \
    http_verdict primary "${primary_output}" "${tmp_dir}/withdrawn-answer.ndjson"
{ cat "${primary_output}"; jq -c 'select(.sequence == 9)' "${http_input}"; } >"${tmp_dir}/refused-repeat.ndjson"
expect_failure 'a 404 is an answer that refuses admission' 'repeated without an uncertain attempt' \
    http_verdict primary "${tmp_dir}/refused-repeat.ndjson" "${tmp_dir}/withdrawn-answer.ndjson"

expect_setup_rejection 'the http verdict needs a domain' '--domain must be primary or peer' \
    "${verifier}" http --attempts "${http_attempts}" --input "${http_input}" --output "${primary_output}" \
    --phases "${phases}" --result "${tmp_dir}/result.json"
jq -n '{withdrawn_after: 20, reshared_after: 30}' >"${tmp_dir}/late-withdrawal.json"
expect_failure 'a withdrawal after the last record' 'no record exercised the withdrawn peer route' \
    http_verdict peer "${peer_output}" "${http_attempts}" "${tmp_dir}/late-withdrawal.json"
jq -n '{withdrawn_after: 6, reshared_after: 6}' >"${tmp_dir}/empty-withdrawal.json"
expect_setup_rejection 'a withdrawal must hold records' 'the phases do not match their contract' \
    http_verdict peer "${peer_output}" "${http_attempts}" "${tmp_dir}/empty-withdrawal.json"

# Five frames: the retained connection sends 1 and 2 and, after its source was replaced, the probe
# 3, which the endpoint refuses with 1013; the new connection sends 4 and 5.
frame_input="${tmp_dir}/frame-input.ndjson"
jq -nc 'range(1; 6) as $s | {event_id: ("run-frame-\($s)"), sequence: $s, content: ("chaos-endpoint-frame-payload-\($s)")}' \
    >"${frame_input}"
frames="${tmp_dir}/frames.ndjson"
{
    jq -nc '{kind: "ws_open", connection: "retained", target: "nervix-2:8080", status: 101, accepted: true}'
    jq -nc '{kind: "ws_sent", connection: "retained", sequence: 1}'
    jq -nc '{kind: "ws_sent", connection: "retained", sequence: 2}'
    jq -nc '{kind: "ws_retained_sent", count: 2}'
    jq -nc '{kind: "ws_probe_sent", connection: "retained", sequence: 3}'
    jq -nc '{kind: "ws_retained_closed", connection: "retained", closed: "frame", code: 1013, reason: ""}'
    jq -nc '{kind: "ws_open", connection: "replacement", target: "nervix-2:8080", status: 101, accepted: true}'
    jq -nc '{kind: "ws_sent", connection: "replacement", sequence: 4}'
    jq -nc '{kind: "ws_sent", connection: "replacement", sequence: 5}'
    jq -nc '{kind: "ws_finished", count: 2}'
} >"${frames}"
frame_output="${tmp_dir}/frame-output.ndjson"
jq -c 'select(.sequence != 3)' "${frame_input}" >"${frame_output}"

websocket_verdict() {
    local ledger="$1"
    local output="$2"
    "${verifier}" websocket --frames "${ledger}" --input "${frame_input}" --output "${output}" \
        --result "${tmp_dir}/result.json"
}

expect_pass 'a retained connection refused after its source was replaced' websocket_verdict "${frames}" "${frame_output}"
jq -e '.retained_frames == 2 and .replacement_frames == 2 and .probe == 3 and .retained_close.code == 1013' \
    "${tmp_dir}/result.json" >/dev/null || fail "the websocket verdict summary is wrong: $(cat "${tmp_dir}/result.json")"
expect_failure 'the refused probe was delivered' 'sent after the source was replaced was delivered' \
    websocket_verdict "${frames}" "${frame_input}"
jq -c 'if .kind == "ws_retained_closed" then .code = 1006 else . end' "${frames}" >"${tmp_dir}/abnormal.ndjson"
expect_failure 'a retained connection closed abnormally' 'closed with 1006 (frame), not 1013' \
    websocket_verdict "${tmp_dir}/abnormal.ndjson" "${frame_output}"
jq -c 'if .kind == "ws_retained_closed" then .closed = "connection" | .code = null else . end' "${frames}" \
    >"${tmp_dir}/dropped.ndjson"
expect_failure 'a retained connection dropped without a close frame' 'closed with no code (connection), not 1013' \
    websocket_verdict "${tmp_dir}/dropped.ndjson" "${frame_output}"
jq -c 'select(.sequence != 4)' "${frame_output}" >"${tmp_dir}/lost-frame.ndjson"
expect_failure 'a lost replacement frame' 'frames not delivered exactly once: [4]' \
    websocket_verdict "${frames}" "${tmp_dir}/lost-frame.ndjson"
{ cat "${frame_output}"; jq -c 'select(.sequence == 1)' "${frame_input}"; } >"${tmp_dir}/repeated-frame.ndjson"
expect_failure 'a repeated retained frame' 'frames not delivered exactly once: [1]' \
    websocket_verdict "${frames}" "${tmp_dir}/repeated-frame.ndjson"
jq -c 'if .sequence == 5 then .content = "altered" else . end' "${frame_output}" >"${tmp_dir}/altered-frame.ndjson"
expect_failure 'an altered frame' 'output differs from the frame sent: [5]' \
    websocket_verdict "${frames}" "${tmp_dir}/altered-frame.ndjson"
jq -c 'if .kind == "ws_open" and .connection == "replacement" then .accepted = false | .status = 503 else . end' \
    "${frames}" >"${tmp_dir}/refused-open.ndjson"
expect_failure 'a refused replacement connection' 'the replacement connection was not accepted' \
    websocket_verdict "${tmp_dir}/refused-open.ndjson" "${frame_output}"
jq -c 'select(.kind != "ws_probe_sent" and .kind != "ws_retained_closed")' "${frames}" >"${tmp_dir}/no-probe.ndjson"
expect_failure 'a run that never probed the retained connection' 'no frame was sent on the retained connection' \
    websocket_verdict "${tmp_dir}/no-probe.ndjson" "${frame_output}"
{ cat "${frame_output}"; jq -nc '{event_id: "run-frame-6", sequence: 6, content: "chaos-endpoint-frame-payload-6"}'; } \
    >"${tmp_dir}/unsent-frame.ndjson"
expect_failure 'a frame no connection sent' 'frames no connection sent reached the output: [6]' \
    websocket_verdict "${frames}" "${tmp_dir}/unsent-frame.ndjson"
jq -c 'if .kind == "ws_open" and .connection == "retained" then .accepted = false | .status = 503 else . end' \
    "${frames}" >"${tmp_dir}/refused-retained.ndjson"
expect_failure 'a refused retained connection' 'the retained connection was not accepted' \
    websocket_verdict "${tmp_dir}/refused-retained.ndjson" "${frame_output}"
jq -c 'select(.kind != "ws_sent" or .connection != "retained")' "${frames}" >"${tmp_dir}/silent-retained.ndjson"
expect_failure 'a retained connection that sent nothing' 'the retained connection sent no frame' \
    websocket_verdict "${tmp_dir}/silent-retained.ndjson" "${frame_output}"
jq -c 'select(.kind != "ws_sent" or .connection != "replacement")' "${frames}" >"${tmp_dir}/silent-replacement.ndjson"
expect_failure 'a replacement connection that sent nothing' 'the replacement connection sent no frame' \
    websocket_verdict "${tmp_dir}/silent-replacement.ndjson" "${frame_output}"
jq -c 'select(.kind != "ws_retained_closed")' "${frames}" >"${tmp_dir}/unclosed.ndjson"
expect_failure 'a retained connection never seen closing' 'the retained connection was not observed closing' \
    websocket_verdict "${tmp_dir}/unclosed.ndjson" "${frame_output}"
expect_setup_rejection 'the websocket verdict needs its ledger' 'the WebSocket ledger is missing or empty' \
    "${verifier}" websocket --frames "${tmp_dir}/missing.ndjson" --input "${frame_input}" \
    --output "${frame_output}" --result "${tmp_dir}/result.json"

printf '%s\n' 'endpoint self-test passed'
