#!/usr/bin/env bash
set -euo pipefail

# Self-test of the node-settings scenarios without Docker: their option rules, the node launcher
# that applies a node's chosen settings, and every verdict of verify-node-settings-evidence.sh on
# passing and contradicting evidence. The descriptions, logs and transcripts below are rendered the
# way the packaged server and CLI write them.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
chaos_dir="$(cd "${script_dir}/.." && pwd)"
verify="${chaos_dir}/verify-node-settings-evidence.sh"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

fail() {
    printf 'node-settings self-test failed: %s\n' "$*" >&2
    exit 1
}

# Runs a verifier that must accept its evidence.
expect_accepted() {
    local case_name="$1"
    shift
    local status=0
    "$@" >"${tmp_dir}/accepted.txt" 2>&1 || status=$?
    [[ "${status}" -eq 0 ]] || { cat "${tmp_dir}/accepted.txt" >&2; fail "${case_name} returned ${status}, expected 0"; }
}

# Runs a verifier that must reject its evidence with verdict 1, naming EXPECTED among its problems.
expect_rejected() {
    local case_name="$1"
    local expected="$2"
    shift 2
    local status=0
    "$@" >"${tmp_dir}/rejected.txt" 2>&1 || status=$?
    [[ "${status}" -eq 1 ]] || { cat "${tmp_dir}/rejected.txt" >&2; fail "${case_name} returned ${status}, expected verdict 1"; }
    grep -Fq -- "${expected}" "${tmp_dir}/rejected.txt" \
        || { cat "${tmp_dir}/rejected.txt" >&2; fail "${case_name} did not name '${expected}'"; }
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

# The four scenarios are listed and refuse what they do not take before anything starts.
list_output="$("${chaos_dir}/chaos.sh" list)"
for scenario in capability-roll parameter-return parameter-live prerequisite-refusal; do
    grep -Eq "^${scenario} " <<<"${list_output}" || fail "scenario list omits ${scenario}"
    expect_setup_rejection "${scenario} requires three nodes" 'requires --nodes 3' \
        "${chaos_dir}/run-baseline.sh" --scenario "${scenario}" --image fixture --nodes 1
    expect_setup_rejection "${scenario} holds no fixed outage" '--outage-seconds is for crash' \
        "${chaos_dir}/run-baseline.sh" --scenario "${scenario}" --image fixture --outage-seconds 8
    expect_setup_rejection "${scenario} takes no fault" '--fault applies only to' \
        "${chaos_dir}/run-baseline.sh" --scenario "${scenario}" --image fixture --fault owner-crash
done
status=0
"${chaos_dir}/chaos.sh" run capability-roll >"${tmp_dir}/missing-image.out" 2>&1 || status=$?
[[ "${status}" -eq 2 ]] || fail "capability-roll without --image returned ${status}, expected 2"

# The launcher exports each assignment for one process and runs the server in its place. Each case
# runs it in an environment holding only PATH and the deployment settings the case names, so the
# fake server's listing shows exactly what the launcher passed on: a caller's own NERVIX_ variables,
# such as those the justfile exports to every recipe, are not part of the case.
launcher="${chaos_dir}/node-launch.sh"
launch() {
    env -i PATH="${PATH}" "$@"
}
fake_server="${tmp_dir}/fake-server"
cat >"${fake_server}" <<'EOF'
#!/bin/sh
env | grep '^NERVIX_' | LC_ALL=C sort
exit 7
EOF
chmod 0755 "${fake_server}"
settings="${tmp_dir}/settings.env"
: >"${settings}"
status=0
launch NERVIX_RAFT_HEARTBEAT_INTERVAL=250ms sh "${launcher}" "${settings}" "${fake_server}" \
    >"${tmp_dir}/launch.out" 2>"${tmp_dir}/launch.err" || status=$?
[[ "${status}" -eq 7 ]] || fail "the launcher did not pass on the server's exit status 7, it returned ${status}"
[[ "$(cat "${tmp_dir}/launch.out")" == 'NERVIX_RAFT_HEARTBEAT_INTERVAL=250ms' ]] \
    || fail 'an empty settings file changed the deployment settings'
[[ ! -s "${tmp_dir}/launch.err" ]] || fail 'an empty settings file logged a setting'
printf '%s\n' NERVIX_RAFT_HEARTBEAT_INTERVAL=1s NERVIX_INTERCONNECT_RELAY_BATCH_BYTES=8192 >"${settings}"
status=0
launch NERVIX_RAFT_HEARTBEAT_INTERVAL=250ms sh "${launcher}" "${settings}" "${fake_server}" \
    >"${tmp_dir}/launch.out" 2>"${tmp_dir}/launch.err" || status=$?
[[ "${status}" -eq 7 ]] || fail "the launcher returned ${status} with chosen settings"
[[ "$(cat "${tmp_dir}/launch.out")" == $'NERVIX_INTERCONNECT_RELAY_BATCH_BYTES=8192\nNERVIX_RAFT_HEARTBEAT_INTERVAL=1s' ]] \
    || fail 'the launcher did not replace the deployment setting and add the chosen one'
[[ "$(cat "${tmp_dir}/launch.err")" == $'node launch: NERVIX_RAFT_HEARTBEAT_INTERVAL=1s\nnode launch: NERVIX_INTERCONNECT_RELAY_BATCH_BYTES=8192' ]] \
    || fail 'the launcher did not log every setting it applied'
printf '%s' NERVIX_RAFT_HEARTBEAT_INTERVAL=1s >"${settings}"
launch sh "${launcher}" "${settings}" "${fake_server}" >"${tmp_dir}/launch.out" 2>/dev/null || true
[[ "$(cat "${tmp_dir}/launch.out")" == 'NERVIX_RAFT_HEARTBEAT_INTERVAL=1s' ]] \
    || fail 'the launcher lost a final assignment without a newline'
for malformed in 'RAFT_HEARTBEAT_INTERVAL=1s' 'NERVIX_raft=1s' 'NERVIX_HEARTBEAT' ''; do
    printf '%s\n' "${malformed}" >"${settings}"
    status=0
    launch sh "${launcher}" "${settings}" "${fake_server}" >"${tmp_dir}/launch.out" 2>/dev/null || status=$?
    [[ "${status}" -eq 2 && ! -s "${tmp_dir}/launch.out" ]] \
        || fail "the launcher ran the server with the malformed line '${malformed}'"
done
status=0
launch sh "${launcher}" "${tmp_dir}/absent.env" "${fake_server}" >"${tmp_dir}/launch.out" 2>/dev/null || status=$?
[[ "${status}" -eq 7 ]] || fail 'the launcher did not start the server without a settings file'
status=0
launch sh "${launcher}" "${settings}" >/dev/null 2>&1 || status=$?
[[ "${status}" -eq 2 ]] || fail 'the launcher accepted a call without its server'

# Renders the DESCRIBE CLUSTER answer the packaged server prints from a JSON spec on standard input:
# the leader, each node's incarnation, claim (a relay-batch-bytes count, "awaiting" or
# "retained:INCARNATION"), heartbeat and liveness, the effective bound, the adopted peers and the
# parameter record with each node's running revision ("not installed" or a revision).
render_description() {
    jq -r '
        def bounds($bytes):
            "relay-batch-bytes=\($bytes) relay-decoded-bytes=33554432 bulk-chunk-bytes=65536 bulk-window-bytes=1048576";
        . as $spec
        | "leader: \($spec.leader)",
          "cluster uid: 72f3a93e-64f0-40a5-b95d-790c8a738fee",
          "cluster key: Xq3p7ffXr5i1mKQkc2FTgJ8dM2hKXZ6UjsTxGJm4p8A",
          "cluster created: 2026-11-21T10:00:00+00:00",
          "timing drift: \($spec.drift // "none")",
          ($spec.nodes | to_entries[] | .key as $name | .value as $node
           | "node: \($name)\(if $name == $spec.leader then " (leader)" else "" end)",
             "  incarnation: \($node.incarnation)",
             (if $node.claim == "awaiting" then "  capabilities: awaiting committed claim"
              elif ($node.claim | tostring | startswith("retained:"))
              then "  capabilities: awaiting this incarnation; retained \($name)#\($node.claim | tostring | ltrimstr("retained:"))"
              else "  capabilities: committed \($name)#\($node.incarnation) cpu-count=4 memory-bytes=8589934592 \(bounds($node.claim)) vm-inline-row-limit=1024 vm-regex-search-cache-bytes=2097152 wasm-stack-bytes=524288 wasm-parallel-compilation=false connector-retry-multiplier=2 delivery-capacity=8192 drain-timeout=40s shutdown-timeout=50s task-grace-period=2s listeners=ListenerBounds { max_connections: 1024 } membership-round=1s"
              end),
             (if $node.claim == "awaiting" or ($node.claim | tostring | startswith("retained:")) then empty
              else "  interconnect: handshakes=32 queue=1024 stream-window=65536 connection-window=262144 setup=5s request=10s progress=5s backoff=200ms max-backoff=5s jitter-permille=0",
                   "  dns: cache-entries=4096 positive-max-ttl=1h negative-max-ttl=30s hook-budget=30s families=dual-stack trust-negative=true concurrent-lookups=64",
                   "  database: cache-bytes=33554432 journal-bytes=536870912 workers=4",
                   "  raft database: cache-bytes=33554432 journal-bytes=536870912 workers=4",
                   "  consensus events: capacity=256",
                   "  consensus: append-batch-entries=256 outstanding-append-batches=16 outstanding-append-bytes=16777216"
              end),
             "  interconnect liveness: \($node.liveness // "ping-interval=5s ping-timeout=2s 500ms probe-timeout=1s")",
             "  cluster identity: chaos-self-test",
             "  trust root: 6c3f0a1b",
             "  wire contract: 9d2e4b7c",
             "  largest command: 1MiB",
             "  largest management event: 64KiB",
             "  durability promise: fsync every append",
             "  raft-heartbeat-interval: \($node.heartbeat // "250ms")\(if ($node.heartbeat // "250ms") != "250ms" then ", the leader'"'"'s 250ms" else "" end)",
             "  gossip-interval: 500ms",
             "  clock-skew-tolerance: 500ms",
             "  record: revision 7 (incarnation \($node.incarnation))",
             "  properties:",
             "    architecture: x86_64",
             "    cpu.model: undetected (the operating system did not report it)",
             "  tags: none"),
          (if $spec.effective == "awaiting" then "cluster-effective receive bounds: awaiting committed live claims"
           else "cluster-effective receive bounds: \(bounds($spec.effective))" end),
          ($spec.adopted // {} | to_entries[]
           | "adopted peer receive bounds: node=\(.key) incarnation=\(.value.incarnation) \(bounds(.value.bytes))"),
          ($spec.parameters | to_entries[] | "\(.key) committed=\(.value) revision=\($spec.revision)"),
          ($spec.running | to_entries[] | .key as $name | .value as $running
           | if $running == "not installed" then "node=\($name) cluster parameters running=not installed"
             else (($spec.running_values // {})[$name] // $spec.parameters) | to_entries[]
                  | "node=\($name) \(.key) running=\(.value) revision=\($running)" end),
          "local execution capacity:",
          "  execution workers: class=control-cpu workers=1 pending_jobs=64",
          "  execution memory: class=relay bytes=1073741824"
    '
}

# Writes the projection of SPEC_JSON, rendered as NODE answers it, to OUTPUT.
describe_spec() {
    local node="$1" spec="$2" output="$3"
    render_description <<<"${spec}" >"${output%.json}.txt"
    "${verify}" describe --transcript "${output%.json}.txt" --node "${node}" --output "${output}"
}

parameters_before='{"node-unavailability-timeout":"10s","state-snapshot-interval":"30s","replica-count":"0","command-retry-validity":"15m","remote-ack-alive-interval":"100ms","remote-ack-silence-timeout":"15s","tls-cipher-suites":"('"'"'TLS13_AES_128_GCM_SHA256'"'"', '"'"'TLS13_AES_256_GCM_SHA384'"'"')"}'
# The settled cluster before a roll: every node claims the deployment's relay bound.
settled_spec="$(jq -nc --argjson parameters "${parameters_before}" '
    {leader: "node-1", revision: 2, parameters: $parameters,
     nodes: {"node-1": {incarnation: 1, claim: 33554432}, "node-2": {incarnation: 2, claim: 33554432},
             "node-3": {incarnation: 3, claim: 33554432}},
     effective: 33554432, running: {"node-1": 2, "node-2": 2, "node-3": 2}}')"
for node in node-1 node-2 node-3; do
    describe_spec "${node}" "$(jq -c --arg node "${node}" '
        .adopted = (.nodes | with_entries(select(.key != $node) | .value = {incarnation: .value.incarnation, bytes: .value.claim}))
        ' <<<"${settled_spec}")" "${tmp_dir}/settled-${node}.json"
done
jq -e '.answered and .leader == "node-1" and .timing_drift == "none"
    and .nodes["node-1"].leader and (.nodes["node-2"].leader | not)
    and .nodes["node-2"].incarnation == "2"
    and .nodes["node-2"].claim == {claimed: "node-2", incarnation: "2", relay_batch_bytes: 33554432,
                                   relay_decoded_bytes: 33554432, bulk_chunk_bytes: 65536, bulk_window_bytes: 1048576}
    and .nodes["node-2"].claim_state == "committed"
    and .nodes["node-2"].liveness == "ping-interval=5s ping-timeout=2s 500ms probe-timeout=1s"
    and .nodes["node-2"].prerequisites["raft-heartbeat-interval"] == "250ms"
    and .nodes["node-2"].prerequisites["cluster identity"] == "chaos-self-test"
    and .nodes["node-2"].record == {revision: "7", incarnation: "2"}
    and .effective.relay_batch_bytes == 33554432 and .effective_state == "committed"
    and (.adopted | keys) == ["node-2", "node-3"] and .adopted["node-3"].incarnation == "3"
    and .committed_revision == "2" and .committed_consistent
    and .committed["tls-cipher-suites"] == "('"'"'TLS13_AES_128_GCM_SHA256'"'"', '"'"'TLS13_AES_256_GCM_SHA384'"'"')"
    and .running["node-3"] == {state: "installed", values: .committed, revision: "2", consistent: true}' \
    "${tmp_dir}/settled-node-1.json" >/dev/null || fail 'the description of a settled cluster was not projected'
printf '%s\n' 'Error: the node could not be reached' >"${tmp_dir}/unanswered.txt"
status=0
"${verify}" describe --transcript "${tmp_dir}/unanswered.txt" --node node-1 --output "${tmp_dir}/unanswered.json" || status=$?
[[ "${status}" -eq 1 ]] || fail "an answer that describes no cluster returned ${status}, expected 1"

settled_claims=(--expect node-1=33554432 --expect node-2=33554432 --expect node-3=33554432)
expect_accepted 'a settled cluster with one claimed bound' \
    "${verify}" claims "${settled_claims[@]}" --output "${tmp_dir}/claims.json" "${tmp_dir}"/settled-node-*.json
jq -e '.verdict == "converged" and .incarnations == {"node-1": "1", "node-2": "2", "node-3": "3"}' \
    "${tmp_dir}/claims.json" >/dev/null || fail 'the settled claims did not record the incarnations'
expect_rejected 'a claim the expectation does not name' 'claiming relay-batch-bytes=33554432, not 8192' \
    "${verify}" claims --expect node-1=8192 --expect node-2=33554432 --expect node-3=33554432 \
    --output "${tmp_dir}/claims.json" "${tmp_dir}"/settled-node-*.json

# The roll mid-way: node-1 restarted as incarnation 11 with the lower bound, and its peers adopted it.
mixed_spec="$(jq -c '.nodes["node-1"] = {incarnation: 11, claim: 8192} | .effective = 8192' <<<"${settled_spec}")"
for node in node-1 node-2 node-3; do
    describe_spec "${node}" "$(jq -c --arg node "${node}" '
        .adopted = (.nodes | with_entries(select(.key != $node) | .value = {incarnation: .value.incarnation, bytes: .value.claim}))
        ' <<<"${mixed_spec}")" "${tmp_dir}/mixed-${node}.json"
done
mixed_claims=(--expect node-1=8192 --expect node-2=33554432 --expect node-3=33554432 --restarted node-1=1)
expect_accepted 'a mixed roll whose peers adopted every live claim' \
    "${verify}" claims "${mixed_claims[@]}" --output "${tmp_dir}/claims.json" "${tmp_dir}"/mixed-node-*.json
jq -e '.effective["node-2"].relay_batch_bytes == 8192 and .adopted["node-2"]["node-1"].relay_batch_bytes == 8192' \
    "${tmp_dir}/claims.json" >/dev/null || fail 'the mixed claims did not record the adopted and effective bounds'
expect_rejected 'a restarted node still at its former incarnation' 'the one it ran before its restart' \
    "${verify}" claims --expect node-1=8192 --expect node-2=33554432 --expect node-3=33554432 --restarted node-1=11 \
    --output "${tmp_dir}/claims.json" "${tmp_dir}"/mixed-node-*.json
# node-2 still sends to node-1 with the bound of node-1's former incarnation.
describe_spec node-2 "$(jq -c '.adopted = {"node-1": {incarnation: 1, bytes: 33554432}, "node-3": {incarnation: 3, bytes: 33554432}}' \
    <<<"${mixed_spec}")" "${tmp_dir}/stale-adoption.json"
expect_rejected 'a peer that adopted a former incarnation' 'not of its live incarnation 11' \
    "${verify}" claims "${mixed_claims[@]}" --output "${tmp_dir}/claims.json" \
    "${tmp_dir}/mixed-node-1.json" "${tmp_dir}/stale-adoption.json" "${tmp_dir}/mixed-node-3.json"
describe_spec node-2 "$(jq -c '.adopted = {"node-1": {incarnation: 11, bytes: 33554432}, "node-3": {incarnation: 3, bytes: 33554432}}' \
    <<<"${mixed_spec}")" "${tmp_dir}/wrong-adoption.json"
expect_rejected 'a peer that adopted another bound' 'other than its committed claim' \
    "${verify}" claims "${mixed_claims[@]}" --output "${tmp_dir}/claims.json" \
    "${tmp_dir}/mixed-node-1.json" "${tmp_dir}/wrong-adoption.json" "${tmp_dir}/mixed-node-3.json"
describe_spec node-2 "$(jq -c '.adopted = {"node-3": {incarnation: 3, bytes: 33554432}}' <<<"${mixed_spec}")" \
    "${tmp_dir}/missing-adoption.json"
expect_rejected 'a peer that adopted nothing of a node' 'has adopted no receive bounds of node-1' \
    "${verify}" claims "${mixed_claims[@]}" --output "${tmp_dir}/claims.json" \
    "${tmp_dir}/mixed-node-1.json" "${tmp_dir}/missing-adoption.json" "${tmp_dir}/mixed-node-3.json"
describe_spec node-3 "$(jq -c '.effective = 33554432
    | .adopted = {"node-1": {incarnation: 11, bytes: 8192}, "node-2": {incarnation: 2, bytes: 33554432}}' \
    <<<"${mixed_spec}")" "${tmp_dir}/wide-effective.json"
expect_rejected 'an effective bound above the smallest live claim' 'other than the componentwise minimum' \
    "${verify}" claims "${mixed_claims[@]}" --output "${tmp_dir}/claims.json" \
    "${tmp_dir}/mixed-node-1.json" "${tmp_dir}/mixed-node-2.json" "${tmp_dir}/wide-effective.json"
describe_spec node-3 "$(jq -c '.nodes["node-1"].claim = "retained:1"
    | .adopted = {"node-1": {incarnation: 11, bytes: 8192}, "node-2": {incarnation: 2, bytes: 33554432}}' \
    <<<"${mixed_spec}")" "${tmp_dir}/awaiting-claim.json"
expect_rejected 'a restarted node whose claim is not committed yet' 'awaiting this incarnation' \
    "${verify}" claims "${mixed_claims[@]}" --output "${tmp_dir}/claims.json" \
    "${tmp_dir}/mixed-node-1.json" "${tmp_dir}/mixed-node-2.json" "${tmp_dir}/awaiting-claim.json"
expect_rejected 'answering nodes that disagree on an incarnation' 'disagree on the live incarnation of node-1' \
    "${verify}" claims --expect node-1=33554432 --expect node-2=33554432 --expect node-3=33554432 \
    --output "${tmp_dir}/claims.json" "${tmp_dir}/settled-node-1.json" "${tmp_dir}/mixed-node-2.json"

# A committed change: revision 3 raises the unavailability timeout and the remote ACK window, and
# every node runs it with the liveness that timeout derives.
changed_spec="$(jq -c '.revision = 3
    | .parameters["node-unavailability-timeout"] = "20s"
    | .parameters["remote-ack-alive-interval"] = "200ms"
    | .parameters["remote-ack-silence-timeout"] = "20s"
    | .running = {"node-1": 3, "node-2": 3, "node-3": 3}
    | .nodes |= map_values(.liveness = "ping-interval=10s ping-timeout=5s probe-timeout=2s")' <<<"${settled_spec}")"
for node in node-1 node-2 node-3; do
    describe_spec "${node}" "${changed_spec}" "${tmp_dir}/changed-${node}.json"
done
changed_expectations=(--expect node-unavailability-timeout=20s --expect remote-ack-alive-interval=200ms
    --expect remote-ack-silence-timeout=20s --liveness 'ping-interval=10s ping-timeout=5s probe-timeout=2s')
expect_accepted 'every node running the committed change' \
    "${verify}" installed --before "${tmp_dir}/settled-node-1.json" "${changed_expectations[@]}" \
    --output "${tmp_dir}/installed.json" "${tmp_dir}"/changed-node-*.json
jq -e '.verdict == "installed" and .revision == "3" and .previous_revision == "2"' "${tmp_dir}/installed.json" \
    >/dev/null || fail 'the installed change did not record its revisions'
describe_spec node-2 "$(jq -c '.running["node-3"] = 2 | .running_values = {"node-3": '"${parameters_before}"'}' \
    <<<"${changed_spec}")" "${tmp_dir}/behind.json"
expect_rejected 'a node still running the previous revision' 'node-3 running revision 2, not the committed 3' \
    "${verify}" installed --before "${tmp_dir}/settled-node-1.json" "${changed_expectations[@]}" \
    --output "${tmp_dir}/installed.json" "${tmp_dir}/changed-node-1.json" "${tmp_dir}/behind.json"
describe_spec node-2 "$(jq -c '.running["node-3"] = "not installed"' <<<"${changed_spec}")" "${tmp_dir}/uninstalled.json"
expect_rejected 'a node that installed nothing' 'node-3 running not installed' \
    "${verify}" installed --before "${tmp_dir}/settled-node-1.json" "${changed_expectations[@]}" \
    --output "${tmp_dir}/installed.json" "${tmp_dir}/uninstalled.json"
describe_spec node-1 "$(jq -c '.parameters["replica-count"] = "1"' <<<"${changed_spec}")" "${tmp_dir}/extra-change.json"
expect_rejected 'a change of a parameter the statement did not assign' 'differ from the change in replica-count' \
    "${verify}" installed --before "${tmp_dir}/settled-node-1.json" "${changed_expectations[@]}" \
    --output "${tmp_dir}/installed.json" "${tmp_dir}/extra-change.json"
describe_spec node-1 "$(jq -c '.nodes["node-2"].liveness = "ping-interval=5s ping-timeout=2s 500ms probe-timeout=1s"' \
    <<<"${changed_spec}")" "${tmp_dir}/old-liveness.json"
expect_rejected 'a node whose interconnect kept the former liveness' 'liveness of node-2 as ping-interval=5s' \
    "${verify}" installed --before "${tmp_dir}/settled-node-1.json" "${changed_expectations[@]}" \
    --output "${tmp_dir}/installed.json" "${tmp_dir}/old-liveness.json"
expect_rejected 'a committed revision that did not advance' 'not one after 3' \
    "${verify}" installed --before "${tmp_dir}/changed-node-1.json" "${changed_expectations[@]}" \
    --output "${tmp_dir}/installed.json" "${tmp_dir}"/changed-node-*.json

# A returning node's own status samples: not installed while it discovers the live record, then the
# committed revision.
status_answer() {
    local node="$1" initialization="$2"
    shift 2
    printf '%s\n' '[chitchat]' 'live_nodes:' "- node_id: ${node}" '' '[raft]' "raft.id: ${node}" \
        'raft.current_leader: node-1' '' '[cluster parameters]' "$@" "node.initialization=${initialization}"
}
{
    printf '=== status nervix-3 1800000000000000000\n'
    printf 'Error: the session listener refused the connection\n'
    printf '=== status nervix-3 1800000000200000000\n'
    status_answer node-3 initializing 'node=node-1 node-unavailability-timeout running=15s revision=3' \
        'node=node-3 cluster parameters running=not installed'
    printf '=== status nervix-3 1800000000400000000\n'
    status_answer node-3 ready 'node=node-3 node-unavailability-timeout running=15s revision=3' \
        'node=node-3 state-snapshot-interval running=20s revision=3'
} >"${tmp_dir}/samples.log"
return_expectations=(--expect node-unavailability-timeout=15s --expect state-snapshot-interval=20s)
expect_accepted 'a returning node that installed only the live record' \
    "${verify}" samples --transcript "${tmp_dir}/samples.log" --node node-3 --revision 3 \
    "${return_expectations[@]}" --output "${tmp_dir}/samples.json"
jq -e '.samples == 2 and .not_installed_samples == 1 and .running_samples == 1
    and .first_running_ns == 1800000000400000000 and .initialization == ["initializing", "ready"]' \
    "${tmp_dir}/samples.json" >/dev/null || fail 'the return samples were not summarized'
{
    printf '=== status nervix-3 1800000000200000000\n'
    status_answer node-3 initializing 'node=node-3 node-unavailability-timeout running=10s revision=2' \
        'node=node-3 state-snapshot-interval running=30s revision=2'
    printf '=== status nervix-3 1800000000400000000\n'
    status_answer node-3 ready 'node=node-3 node-unavailability-timeout running=15s revision=3' \
        'node=node-3 state-snapshot-interval running=20s revision=3'
} >"${tmp_dir}/stale-samples.log"
expect_rejected 'a returning node that ran its stale record' 'reported running revisions 2' \
    "${verify}" samples --transcript "${tmp_dir}/stale-samples.log" --node node-3 --revision 3 \
    "${return_expectations[@]}" --output "${tmp_dir}/samples.json"
{
    printf '=== status nervix-3 1800000000200000000\n'
    status_answer node-3 initializing 'node=node-3 cluster parameters running=not installed'
} >"${tmp_dir}/pending-samples.log"
expect_rejected 'a returning node that never ran the live record' 'never reported running revision 3' \
    "${verify}" samples --transcript "${tmp_dir}/pending-samples.log" --node node-3 --revision 3 \
    "${return_expectations[@]}" --output "${tmp_dir}/samples.json"
{
    printf '=== status nervix-3 1800000000200000000\n'
    status_answer node-1 ready 'node=node-3 node-unavailability-timeout running=15s revision=3'
} >"${tmp_dir}/foreign-samples.log"
expect_rejected 'answers another node gave' 'answered no sample through its own route' \
    "${verify}" samples --transcript "${tmp_dir}/foreign-samples.log" --node node-3 --revision 3 \
    "${return_expectations[@]}" --output "${tmp_dir}/samples.json"

# The parameter revisions a process records installing.
installed_line() {
    printf '2026-11-21T10:00:0%sZ  INFO nervix_server::application::cluster_parameters: cluster parameters committed and installed revision=%s\n' "$1" "$2"
}
installed_line 1 3 >"${tmp_dir}/installations.log"
installed_line 2 4 >>"${tmp_dir}/installations.log"
expect_accepted 'a process whose first installation is the live revision' \
    "${verify}" installations --log "${tmp_dir}/installations.log" --revision 3 --output "${tmp_dir}/installations.json"
{ installed_line 1 2; installed_line 2 3; } >"${tmp_dir}/stale-installations.log"
expect_rejected 'a process that installed its stale record first' 'was 2, not 3' \
    "${verify}" installations --log "${tmp_dir}/stale-installations.log" --revision 3 --output "${tmp_dir}/installations.json"
: >"${tmp_dir}/no-installations.log"
expect_rejected 'a process that installed nothing' 'recorded installing no parameter revision' \
    "${verify}" installations --log "${tmp_dir}/no-installations.log" --revision 3 --output "${tmp_dir}/installations.json"

# A run refused at join: the error line, then the exit report error-stack renders with emphasis.
mismatch='raft-heartbeat-interval is 1s on this node and 250ms on the peer, not within a factor of 2'
{
    printf 'node launch: NERVIX_RAFT_HEARTBEAT_INTERVAL=1s\n'
    printf '2026-11-21T10:00:01Z  INFO nervix_server::application: starting nervix server raft_heartbeat_interval=1s\n'
    printf "2026-11-21T10:00:02Z ERROR nervix_server::application: this node was refused at join and stops refusal=node 'node-1' cannot run beside this node: %s\n" "${mismatch}"
    printf '2026-11-21T10:00:03Z  INFO nervix_server::application::shutdown: shutdown terminal-teardown phase finished outcome=Completed\n'
    printf 'Error: \033[1mthis node was refused at join\033[0m\n'
    printf "╰─▶ node 'node-1' cannot run beside this node: %s\n" "${mismatch}"
} >"${tmp_dir}/refused.log"
expect_accepted 'a run refused at join by a survivor' \
    "${verify}" join-refusal --log "${tmp_dir}/refused.log" --survivor node-1 --survivor node-2 \
    --mismatch "${mismatch}" --output "${tmp_dir}/refusal.json"
jq -e '.peers == ["node-1"] and (.exit_report | length) == 1' "${tmp_dir}/refusal.json" >/dev/null \
    || fail 'the refusal verdict did not name the refusing survivor and the exit report'
expect_rejected 'a refusal with other values' 'not raft-heartbeat-interval is 1s on this node and 250ms' \
    "${verify}" join-refusal --log "${tmp_dir}/refused.log" --survivor node-1 --survivor node-2 \
    --mismatch 'raft-heartbeat-interval is 1s on this node and 250ms on the peer, not within a factor of 3' \
    --output "${tmp_dir}/refusal.json"
expect_rejected 'a refusal naming a node that is no survivor' 'names node-1, not a survivor' \
    "${verify}" join-refusal --log "${tmp_dir}/refused.log" --survivor node-2 \
    --mismatch "${mismatch}" --output "${tmp_dir}/refusal.json"
{ cat "${tmp_dir}/refused.log"; printf '2026-11-21T10:00:02Z  INFO nervix_server::application: runtime execution admitted after linearizable consensus catch-up\n'; } \
    >"${tmp_dir}/admitted.log"
expect_rejected 'a refused run that admitted runtime execution' 'admitted runtime execution' \
    "${verify}" join-refusal --log "${tmp_dir}/admitted.log" --survivor node-1 --mismatch "${mismatch}" \
    --output "${tmp_dir}/refusal.json"
grep -v 'refused at join' "${tmp_dir}/refused.log" >"${tmp_dir}/joined.log"
expect_rejected 'a run that was not refused' 'never reported that it was refused at join' \
    "${verify}" join-refusal --log "${tmp_dir}/joined.log" --survivor node-1 --mismatch "${mismatch}" \
    --output "${tmp_dir}/refusal.json"
grep -v '^Error: ' "${tmp_dir}/refused.log" >"${tmp_dir}/unreported.log"
expect_rejected 'a refused run without its exit report' 'without an exit report' \
    "${verify}" join-refusal --log "${tmp_dir}/unreported.log" --survivor node-1 --mismatch "${mismatch}" \
    --output "${tmp_dir}/refusal.json"

# A start that names a cluster parameter as the node's own setting.
{
    printf 'node launch: NERVIX_NODE_UNAVAILABILITY_TIMEOUT=10s\n'
    printf 'Error: \033[1mnode-unavailability-timeout is a first-bootstrap seed; use ALTER CLUSTER SET\033[0m\n'
} >"${tmp_dir}/seed.log"
expect_accepted 'a start refused for its parameter seed' \
    "${verify}" seed-refusal --log "${tmp_dir}/seed.log" --parameter node-unavailability-timeout \
    --output "${tmp_dir}/seed.json"
expect_rejected 'a start refused for another seed' 'did not refuse state-snapshot-interval' \
    "${verify}" seed-refusal --log "${tmp_dir}/seed.log" --parameter state-snapshot-interval \
    --output "${tmp_dir}/seed.json"

# The survivors' side of a refusal: the handshake an acceptor refused, or the leader's verdict.
printf '%s\n' '2026-11-21T10:00:02Z  WARN nervix_interconnect::connection: refused an interconnect peer that cannot run beside this node peer=node-3 peer_addr=10.213.7.13:41000 prerequisite=raft-heartbeat-interval refusal=raft-heartbeat-interval is 250ms on this node and 1s on the peer, not within a factor of 2' \
    >"${tmp_dir}/nervix-1.log"
printf '%s\n' '2026-11-21T10:00:02Z  INFO nervix_server::application: the startup preflight passed' >"${tmp_dir}/nervix-2.log"
expect_accepted 'a survivor that refused the handshake' \
    "${verify}" survivor-refusals --node node-3 --prerequisite raft-heartbeat-interval \
    --output "${tmp_dir}/survivors.json" "${tmp_dir}/nervix-1.log" "${tmp_dir}/nervix-2.log"
jq -e '.refusals == 1 and (.logs | map(.log)) == ["nervix-1", "nervix-2"]' "${tmp_dir}/survivors.json" >/dev/null \
    || fail 'the survivor refusals were not attributed to their logs'
printf '%s\n' "2026-11-21T10:00:02Z  WARN nervix_consensus: refused a node's run that cannot run beside a voter candidate=node-3#17 voter=node-1 prerequisite=raft-heartbeat-interval refusal=raft-heartbeat-interval is 250ms on this node and 1s on the peer, not within a factor of 2" \
    >"${tmp_dir}/leader.log"
expect_accepted "the leader's verdict on the refused run" \
    "${verify}" survivor-refusals --node node-3 --prerequisite raft-heartbeat-interval \
    --output "${tmp_dir}/survivors.json" "${tmp_dir}/leader.log" "${tmp_dir}/nervix-2.log"
expect_rejected 'survivors that reported no refusal' 'no survivor reported refusing node-3' \
    "${verify}" survivor-refusals --node node-3 --prerequisite raft-heartbeat-interval \
    --output "${tmp_dir}/survivors.json" "${tmp_dir}/nervix-2.log"
expect_rejected 'a refusal of another node' 'no survivor reported refusing node-2' \
    "${verify}" survivor-refusals --node node-2 --prerequisite raft-heartbeat-interval \
    --output "${tmp_dir}/survivors.json" "${tmp_dir}/nervix-1.log"

# The observers' cluster-parameter notices across two live changes.
jq -n '{start_ns: 1800000000000000000, end_ns: 1800000060000000000}' >"${tmp_dir}/window.json"
observer_transcript() {
    local output="$1"
    shift
    printf '%s\n' '1799999990000000000 listening for events from relay chaos_observer_quiet' "$@" >"${output}"
}
notice() {
    printf '%s [events] cluster parameters INFO: cluster parameters committed at revision %s' "$1" "$2"
}
for host in nervix-1 nervix-2 nervix-3; do
    observer_transcript "${tmp_dir}/events-${host}.log" "$(notice 1800000010000000000 3)" "$(notice 1800000030000000000 4)"
done
expect_accepted 'every node announcing both revisions' \
    "${verify}" notices --window "${tmp_dir}/window.json" --revision 3 --revision 4 \
    --output "${tmp_dir}/notices.json" "${tmp_dir}"/events-nervix-*.log
observer_transcript "${tmp_dir}/events-missing.log" "$(notice 1800000030000000000 4)"
expect_rejected 'a node that announced only the later revision' 'revisions [4], not exactly [3, 4]' \
    "${verify}" notices --window "${tmp_dir}/window.json" --revision 3 --revision 4 \
    --output "${tmp_dir}/notices.json" "${tmp_dir}/events-nervix-1.log" "${tmp_dir}/events-missing.log"
observer_transcript "${tmp_dir}/events-twice.log" "$(notice 1800000010000000000 3)" \
    "$(notice 1800000011000000000 3)" "$(notice 1800000030000000000 4)"
expect_rejected 'a node that announced a revision twice' 'revisions [3, 3, 4]' \
    "${verify}" notices --window "${tmp_dir}/window.json" --revision 3 --revision 4 \
    --output "${tmp_dir}/notices.json" "${tmp_dir}/events-twice.log"
observer_transcript "${tmp_dir}/events-gap.log" "$(notice 1800000010000000000 3)" \
    '1800000020000000000 observer: the notice follow ended' "$(notice 1800000030000000000 4)"
expect_rejected 'an observer whose follow ended inside the window' 'did not follow the whole window' \
    "${verify}" notices --window "${tmp_dir}/window.json" --revision 3 --revision 4 \
    --output "${tmp_dir}/notices.json" "${tmp_dir}/events-gap.log"
observer_transcript "${tmp_dir}/events-membership.log" "$(notice 1800000010000000000 3)" \
    '1800000020000000000 [events] membership INFO: node node-2 left' "$(notice 1800000030000000000 4)"
expect_rejected 'a membership change inside the window' 'membership or leadership notice' \
    "${verify}" notices --window "${tmp_dir}/window.json" --revision 3 --revision 4 \
    --output "${tmp_dir}/notices.json" "${tmp_dir}/events-membership.log"
printf '%s\n' '1800000005000000000 listening for events from relay chaos_observer_quiet' \
    "$(notice 1800000010000000000 3)" "$(notice 1800000030000000000 4)" >"${tmp_dir}/events-late.log"
expect_rejected 'an observer that attached after the window opened' 'attached only after the window opened' \
    "${verify}" notices --window "${tmp_dir}/window.json" --revision 3 --revision 4 \
    --output "${tmp_dir}/notices.json" "${tmp_dir}/events-late.log"

printf 'node-settings self-test passed\n'
