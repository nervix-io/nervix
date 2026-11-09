#!/usr/bin/env bash
set -euo pipefail

# Self-test of the leader-crash scenario's outward TLS policy check: the evidence verifier against
# passing and contradicting observations, and tls-probe.py against local TLS servers that admit or
# exclude its restricted offer, so the probe distinguishes a refused suite, a refused group and an
# accepted offer, and refuses to judge without its OpenSSL configuration.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
chaos_dir="$(cd "${script_dir}/.." && pwd)"
# shellcheck source=../require-jq.sh
source "${chaos_dir}/require-jq.sh"

fail() {
    printf 'outward TLS self-test failed: %s\n' "$*" >&2
    exit 1
}

chaos_jq_supported || fail "the chaos scripts need jq 1.8 or later, not $(chaos_jq_found)"
for command_name in openssl python3 timeout; do
    command -v "${command_name}" >/dev/null 2>&1 || fail "required command is unavailable: ${command_name}"
done

verifier="${chaos_dir}/verify-outward-tls-evidence.sh"
probe="${chaos_dir}/tls-probe.py"
probe_config="${chaos_dir}/tls-probe-openssl.cnf"
tmp_dir="$(mktemp -d)"
server_pids=()
self_test_cleanup() {
    local pid
    for pid in "${server_pids[@]}"; do
        kill "${pid}" 2>/dev/null || true
    done
    rm -rf "${tmp_dir}"
}
trap self_test_cleanup EXIT

fips_suites="('TLS13_AES_256_GCM_SHA384', 'TLS13_AES_128_GCM_SHA256', 'TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384')"
fips_groups="('X25519MLKEM768', 'secp256r1', 'secp384r1')"
open_suites="('TLS13_AES_256_GCM_SHA384', 'TLS13_AES_128_GCM_SHA256', 'TLS13_CHACHA20_POLY1305_SHA256')"
open_groups="('X25519MLKEM768', 'secp256r1', 'secp384r1', 'X25519')"

# description FILE REVISION SUITES GROUPS [NODE=REVISION:SUITES:GROUPS]...: a DESCRIBE CLUSTER
# transcript whose nodes run the committed lists unless an override names another record.
description() {
    local file="$1"
    local revision="$2"
    local suites="$3"
    local groups="$4"
    shift 4
    {
        printf 'replica-count committed=1 revision=%s\n' "${revision}"
        printf 'tls-cipher-suites committed=%s revision=%s\n' "${suites}" "${revision}"
        printf 'tls-key-exchange-groups committed=%s revision=%s\n' "${groups}" "${revision}"
        local node override node_revision node_suites node_groups
        for node in node-1 node-2 node-3; do
            node_revision="${revision}"
            node_suites="${suites}"
            node_groups="${groups}"
            for override in "$@"; do
                if [[ "${override%%=*}" == "${node}" ]]; then
                    IFS=':' read -r node_revision node_suites node_groups <<<"${override#*=}"
                fi
            done
            printf 'node=%s replica-count running=1 revision=%s\n' "${node}" "${node_revision}"
            printf 'node=%s tls-cipher-suites running=%s revision=%s\n' "${node}" "${node_suites}" "${node_revision}"
            printf 'node=%s tls-key-exchange-groups running=%s revision=%s\n' "${node}" "${node_groups}" "${node_revision}"
        done
    } >"${file}"
}

# probe_document FILE OUTCOME [HOST=OUTCOME]...: a tls-probe.py document for nervix-1 to nervix-3.
probe_document() {
    local file="$1"
    local outcome="$2"
    shift 2
    local overrides='{}'
    if [[ "$#" -gt 0 ]]; then
        overrides="$(printf '%s\n' "$@" | jq -Rsc 'split("\n")[:-1] | map(split("=") | {key: .[0], value: .[1]}) | from_entries')"
    fi
    jq -nc --arg outcome "${outcome}" --argjson overrides "${overrides}" '
        def result($host; $outcome):
            {host: $host, port: 47421, elapsed_ms: 3, outcome: $outcome}
            + (if $outcome == "accepted" then {version: "TLSv1.3", cipher: "TLS_CHACHA20_POLY1305_SHA256"}
               elif $outcome == "refused" then {reason: "SSLV3_ALERT_HANDSHAKE_FAILURE"}
               else {error: "ConnectionRefusedError: [Errno 111] Connection refused"} end);
        {client: {openssl: "OpenSSL 3.5.5", version: "TLSv1.3",
                  cipher_suites: ["TLS_CHACHA20_POLY1305_SHA256"], groups: ["X25519"]},
         targets: [("nervix-1", "nervix-2", "nervix-3") as $host
             | ($overrides[$host] // $outcome) as $selected
             | select($selected != "missing") | result($host; $selected)]}' >"${file}"
}

# Every case judges the interconnect probe this names, by default one every node refused.
interconnect_probe="${tmp_dir}/interconnect-refused.json"
verify() {
    "${verifier}" "$@" --interconnect-probe "${interconnect_probe}" \
        --node node-1=nervix-1 --node node-2=nervix-2 --node node-3=nervix-3
}

expect_pass() {
    local case_name="$1"
    shift
    verify "$@" 2>"${tmp_dir}/${case_name}.stderr" \
        || fail "${case_name} was refused: $(cat "${tmp_dir}/${case_name}.stderr")"
}

expect_contradiction() {
    local case_name="$1"
    local problem="$2"
    shift 2
    local status=0
    verify "$@" 2>"${tmp_dir}/${case_name}.stderr" || status=$?
    [[ "${status}" -eq 1 ]] || fail "${case_name} exited ${status}, expected the verdict 1"
    grep -Fq -- "${problem}" "${tmp_dir}/${case_name}.stderr" \
        || fail "${case_name} did not report '${problem}': $(cat "${tmp_dir}/${case_name}.stderr")"
}

result="${tmp_dir}/result.json"

description "${tmp_dir}/fips.txt" 7 "${fips_suites}" "${fips_groups}"
probe_document "${tmp_dir}/refused.json" refused
cp "${tmp_dir}/refused.json" "${tmp_dir}/interconnect-refused.json"
expect_pass fips-refused --policy excludes --description "${tmp_dir}/fips.txt" \
    --probe "${tmp_dir}/refused.json" --result "${result}"
jq -e '.verdict == "pass" and .revision == "7" and (.nodes | length) == 3
    and .committed.key_exchange_groups == ["X25519MLKEM768", "secp256r1", "secp384r1"]' "${result}" >/dev/null \
    || fail 'a passing observation did not record its revision, nodes and committed lists'

description "${tmp_dir}/open.txt" 9 "${open_suites}" "${open_groups}"
probe_document "${tmp_dir}/accepted.json" accepted
expect_pass open-accepted --policy permits --description "${tmp_dir}/open.txt" \
    --probe "${tmp_dir}/accepted.json" --after-revision 7 --result "${result}"

expect_contradiction permits-not-committed 'do not contain TLS13_CHACHA20_POLY1305_SHA256' \
    --policy permits --description "${tmp_dir}/fips.txt" --probe "${tmp_dir}/accepted.json" --result "${result}"
expect_contradiction excludes-committed 'the committed key exchange groups contain X25519' \
    --policy excludes --description "${tmp_dir}/open.txt" --probe "${tmp_dir}/refused.json" --result "${result}"
expect_contradiction revision-not-after 'the committed revision 9 does not exceed 9' \
    --policy permits --description "${tmp_dir}/open.txt" --probe "${tmp_dir}/accepted.json" \
    --after-revision 9 --result "${result}"

description "${tmp_dir}/stale.txt" 9 "${open_suites}" "${open_groups}" "node-2=7:${fips_suites}:${fips_groups}"
expect_contradiction stale-node 'node-2 does not run the committed TLS lists at revision 9' \
    --policy permits --description "${tmp_dir}/stale.txt" --probe "${tmp_dir}/accepted.json" --result "${result}"

description "${tmp_dir}/other-list.txt" 9 "${open_suites}" "${open_groups}" "node-3=9:${fips_suites}:${open_groups}"
expect_contradiction other-running-list 'node-3 does not run the committed TLS lists at revision 9' \
    --policy permits --description "${tmp_dir}/other-list.txt" --probe "${tmp_dir}/accepted.json" --result "${result}"

grep -v '^node=node-1 ' "${tmp_dir}/open.txt" >"${tmp_dir}/no-node-1.txt"
expect_contradiction unreported-node 'node-1 does not run the committed TLS lists at revision 9' \
    --policy permits --description "${tmp_dir}/no-node-1.txt" --probe "${tmp_dir}/accepted.json" --result "${result}"

probe_document "${tmp_dir}/one-accepted.json" refused nervix-3=accepted
expect_contradiction accepted-under-excludes 'nervix-3 did not refuse the restricted client' \
    --policy excludes --description "${tmp_dir}/fips.txt" --probe "${tmp_dir}/one-accepted.json" --result "${result}"

probe_document "${tmp_dir}/one-failed.json" accepted nervix-2=failed
expect_contradiction failed-target 'nervix-2 did not accept the restricted client: failed' \
    --policy permits --description "${tmp_dir}/open.txt" --probe "${tmp_dir}/one-failed.json" --result "${result}"

probe_document "${tmp_dir}/one-missing.json" accepted nervix-1=missing
expect_contradiction missing-target 'nervix-1 has 0 probe results, not one' \
    --policy permits --description "${tmp_dir}/open.txt" --probe "${tmp_dir}/one-missing.json" --result "${result}"

jq '.client.cipher_suites += ["TLS_AES_128_GCM_SHA256"]' "${tmp_dir}/accepted.json" >"${tmp_dir}/open-client.json"
expect_contradiction unrestricted-client 'the probe did not record the restricted client offer' \
    --policy permits --description "${tmp_dir}/open.txt" --probe "${tmp_dir}/open-client.json" --result "${result}"

# The interconnect's algorithms are fixed: it must refuse the probe whatever the outward policy.
probe_document "${tmp_dir}/interconnect-one-accepted.json" refused nervix-2=accepted
interconnect_probe="${tmp_dir}/interconnect-one-accepted.json"
expect_contradiction interconnect-accepted 'the interconnect of nervix-2 did not refuse the restricted client' \
    --policy permits --description "${tmp_dir}/open.txt" --probe "${tmp_dir}/accepted.json" --result "${result}"
probe_document "${tmp_dir}/interconnect-one-missing.json" refused nervix-3=missing
interconnect_probe="${tmp_dir}/interconnect-one-missing.json"
expect_contradiction interconnect-missing 'nervix-3 has 0 interconnect probe results, not one' \
    --policy excludes --description "${tmp_dir}/fips.txt" --probe "${tmp_dir}/refused.json" --result "${result}"
interconnect_probe="${tmp_dir}/open-client.json"
expect_contradiction interconnect-unrestricted-client 'the probe did not record the restricted client offer' \
    --policy excludes --description "${tmp_dir}/fips.txt" --probe "${tmp_dir}/refused.json" --result "${result}"
interconnect_probe="${tmp_dir}/interconnect-refused.json"

{ cat "${tmp_dir}/fips.txt"; printf "tls-cipher-suites committed=%s revision=8\n" "${fips_suites}"; } \
    >"${tmp_dir}/two-committed.txt"
expect_contradiction two-committed-lines 'exactly one committed line for each TLS list' \
    --policy excludes --description "${tmp_dir}/two-committed.txt" --probe "${tmp_dir}/refused.json" --result "${result}"

sed 's/^tls-key-exchange-groups committed=\(.*\) revision=7$/tls-key-exchange-groups committed=\1 revision=8/' \
    "${tmp_dir}/fips.txt" >"${tmp_dir}/split-revision.txt"
expect_contradiction split-revision 'the committed TLS lists name different revisions' \
    --policy excludes --description "${tmp_dir}/split-revision.txt" --probe "${tmp_dir}/refused.json" --result "${result}"

sed "s/^tls-cipher-suites committed=.*/tls-cipher-suites committed=fips revision=7/" \
    "${tmp_dir}/fips.txt" >"${tmp_dir}/alias.txt"
expect_contradiction alias-instead-of-names 'malformed' \
    --policy excludes --description "${tmp_dir}/alias.txt" --probe "${tmp_dir}/refused.json" --result "${result}"
jq -e '.verdict == "fail"' "${result}" >/dev/null || fail 'a malformed observation left no failing verdict'

usage_status=0
"${verifier}" --policy maybe --description "${tmp_dir}/fips.txt" --probe "${tmp_dir}/refused.json" \
    --node node-1=nervix-1 --result "${result}" 2>/dev/null || usage_status=$?
[[ "${usage_status}" -eq 2 ]] || fail "an unknown policy exited ${usage_status}, expected the usage status 2"

# The probe against local servers: a certificate authority, a server certificate for localhost and
# one OpenSSL server per case, each accepting a single connection.
openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 1 -subj '/CN=outward-tls-self-test' \
    -keyout "${tmp_dir}/ca-key.pem" -out "${tmp_dir}/ca.pem" >"${tmp_dir}/openssl.txt" 2>&1
printf 'basicConstraints=CA:FALSE\nextendedKeyUsage=serverAuth\nsubjectAltName=DNS:localhost\n' \
    >"${tmp_dir}/server.ext"
openssl req -newkey rsa:2048 -sha256 -nodes -subj '/CN=localhost' \
    -keyout "${tmp_dir}/server-key.pem" -out "${tmp_dir}/server.csr" >>"${tmp_dir}/openssl.txt" 2>&1
openssl x509 -req -sha256 -days 1 -in "${tmp_dir}/server.csr" -CA "${tmp_dir}/ca.pem" \
    -CAkey "${tmp_dir}/ca-key.pem" -CAcreateserial -extfile "${tmp_dir}/server.ext" \
    -out "${tmp_dir}/server.pem" >>"${tmp_dir}/openssl.txt" 2>&1

# start_server NAME SUITES GROUPS: starts a TLS 1.3 server and sets server_port.
start_server() {
    local name="$1"
    local suites="$2"
    local groups="$3"
    local log="${tmp_dir}/server-${name}.log"
    # -www answers from the server itself; without it s_server ends a connection when its closed
    # standard input does, before the handshake can complete or fail.
    timeout 60 openssl s_server -www -accept 127.0.0.1:0 -naccept 1 -tls1_3 \
        -cert "${tmp_dir}/server.pem" -key "${tmp_dir}/server-key.pem" \
        -ciphersuites "${suites}" -groups "${groups}" >"${log}" 2>&1 </dev/null &
    server_pids+=("$!")
    local attempt
    server_port=""
    for attempt in $(seq 1 100); do
        server_port="$(sed -n 's/^ACCEPT 127\.0\.0\.1:\([0-9][0-9]*\)$/\1/p' "${log}")"
        [[ -n "${server_port}" ]] && return 0
        sleep 0.1
    done
    fail "the ${name} server did not start: $(cat "${log}")"
}

run_probe() {
    local output="$1"
    shift
    OPENSSL_CONF="${probe_config}" timeout 30 python3 -I "${probe}" --ca "${tmp_dir}/ca.pem" --timeout 5 "$@" \
        >"${output}" 2>"${output}.stderr"
}

expect_probe() {
    local case_name="$1"
    local expected="$2"
    local suites="$3"
    local groups="$4"
    local status=0
    start_server "${case_name}" "${suites}" "${groups}"
    run_probe "${tmp_dir}/probe-${case_name}.json" --port "${server_port}" localhost || status=$?
    [[ "${status}" -eq 0 ]] || fail "the ${case_name} probe exited ${status}: $(cat "${tmp_dir}/probe-${case_name}.json" "${tmp_dir}/probe-${case_name}.json.stderr")"
    jq -e --arg expected "${expected}" '
        .targets | length == 1 and .[0].outcome == $expected
        and (if $expected == "accepted" then .[0].cipher == "TLS_CHACHA20_POLY1305_SHA256" and .[0].version == "TLSv1.3"
             else .[0].reason == "SSLV3_ALERT_HANDSHAKE_FAILURE" end)' \
        "${tmp_dir}/probe-${case_name}.json" >/dev/null \
        || fail "the ${case_name} probe did not report ${expected}: $(cat "${tmp_dir}/probe-${case_name}.json")"
}

expect_probe approved-only refused 'TLS_AES_256_GCM_SHA384:TLS_AES_128_GCM_SHA256' 'P-256:P-384'
expect_probe chacha-without-x25519 refused 'TLS_CHACHA20_POLY1305_SHA256' 'P-256'
expect_probe x25519-without-chacha refused 'TLS_AES_128_GCM_SHA256' 'X25519'
expect_probe chacha-and-x25519 accepted 'TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256' 'P-256:X25519'

start_server wrong-name 'TLS_CHACHA20_POLY1305_SHA256' 'X25519'
probe_status=0
run_probe "${tmp_dir}/probe-wrong-name.json" --port "${server_port}" 127.0.0.1 || probe_status=$?
[[ "${probe_status}" -eq 1 ]] || fail "a certificate that names another host exited ${probe_status}, expected 1"
jq -e '.targets[0].outcome == "failed" and (.targets[0].error | startswith("certificate verification"))' \
    "${tmp_dir}/probe-wrong-name.json" >/dev/null \
    || fail "a certificate that names another host was not a failed target: $(cat "${tmp_dir}/probe-wrong-name.json")"

# A port nothing listens on: the probe reports the refused connection as a failed target.
closed_port="$(python3 -I -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')"
probe_status=0
run_probe "${tmp_dir}/probe-closed.json" --port "${closed_port}" localhost || probe_status=$?
[[ "${probe_status}" -eq 1 ]] || fail "a closed port exited ${probe_status}, expected 1"
jq -e '.targets[0].outcome == "failed" and (.targets[0].error | startswith("ConnectionRefusedError"))' \
    "${tmp_dir}/probe-closed.json" >/dev/null \
    || fail "a closed port was not a failed target: $(cat "${tmp_dir}/probe-closed.json")"

unconfigured_status=0
env -u OPENSSL_CONF timeout 30 python3 -I "${probe}" --ca "${tmp_dir}/ca.pem" --port 9 localhost \
    >"${tmp_dir}/unconfigured.json" 2>"${tmp_dir}/unconfigured.stderr" || unconfigured_status=$?
[[ "${unconfigured_status}" -eq 2 ]] \
    || fail "a probe without its OpenSSL configuration exited ${unconfigured_status}, expected 2"
grep -Fq 'OPENSSL_CONF must name tls-probe-openssl.cnf' "${tmp_dir}/unconfigured.stderr" \
    || fail 'a probe without its OpenSSL configuration did not say why it refused to judge'
[[ ! -s "${tmp_dir}/unconfigured.json" ]] || fail 'a probe that refused to judge still wrote a document'

printf 'outward TLS self-test passed\n'
