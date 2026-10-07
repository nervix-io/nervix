#!/usr/bin/env bash
set -euo pipefail

# Self-test of the restart and recovery scenarios: their option checks, the parsing helpers that
# turn packaged-CLI answers, metrics and status into evidence, and every verdict of
# verify-recovery-evidence.sh on passing and failing evidence.

chaos_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
verify="${chaos_dir}/verify-recovery-evidence.sh"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

fail() {
    printf 'recovery self-test failed: %s\n' "$*" >&2
    exit 1
}

# Runs a verifier that must reject its evidence with verdict 1.
expect_rejected() {
    local case_name="$1"
    shift
    local status=0
    "$@" >"${tmp_dir}/rejected.txt" 2>&1 || status=$?
    [[ "${status}" -eq 1 ]] || fail "${case_name} returned ${status}, expected verdict 1"
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

list_output="$("${chaos_dir}/chaos.sh" list)"
for scenario in stale-follower former-owner-restart cluster-restart; do
    grep -Fq "${scenario}" <<<"${list_output}" || fail "scenario list omits ${scenario}"
done
expect_setup_rejection 'stale follower requires three nodes' 'requires --nodes 3' \
    "${chaos_dir}/run-baseline.sh" --scenario stale-follower --image fixture --nodes 1
expect_setup_rejection 'former owner requires three nodes' 'requires --nodes 3' \
    "${chaos_dir}/run-baseline.sh" --scenario former-owner-restart --image fixture --nodes 1
expect_setup_rejection 'stale follower rejects a fixed outage' '--outage-seconds is for crash and cluster-restart' \
    "${chaos_dir}/run-baseline.sh" --scenario stale-follower --image fixture --outage-seconds 8
expect_setup_rejection 'former owner rejects a fixed outage' '--outage-seconds is for crash and cluster-restart' \
    "${chaos_dir}/run-baseline.sh" --scenario former-owner-restart --image fixture --outage-seconds 8
expect_setup_rejection 'isolation window is bounded' '--isolation-seconds must be an integer from 20 through 600' \
    "${chaos_dir}/run-baseline.sh" --scenario former-owner-restart --image fixture --isolation-seconds 19
expect_setup_rejection 'isolation window needs the former-owner scenario' 'applies only to former-owner-restart' \
    "${chaos_dir}/run-baseline.sh" --scenario cluster-restart --image fixture --isolation-seconds 30
expect_setup_rejection 'cluster restart topology is validated' '--nodes must be 1 or 3' \
    "${chaos_dir}/run-baseline.sh" --scenario cluster-restart --image fixture --nodes 2
status=0
"${chaos_dir}/chaos.sh" run cluster-restart >"${tmp_dir}/restart-missing-image.out" 2>&1 || status=$?
[[ "${status}" -eq 2 ]] || fail "cluster-restart without --image returned ${status}, expected 2"

# The helpers below are sourced from the scenario with the run state they read.
script_dir="${chaos_dir}"
artifact_dir="${tmp_dir}/artifacts"
mkdir -p "${artifact_dir}/results"
# shellcheck source=../recovery-scenario.sh
source "${chaos_dir}/recovery-scenario.sh"

# The final ledger query owns a budget separate from live probes and preserves a controller
# failure before any accepted-input ledger is constructed. These commands stand in for Docker;
# the real loaded-JVM boundary is exercised by the immutable-image restart experiments.
(
    artifact_dir="${tmp_dir}/final-boundary"
    mkdir -p "${artifact_dir}/traffic"
    scenario=cluster-restart
    record_count=100
    compose_call_timeout=20
    phase() { :; }
    owned_service_container() { printf '%s\n' load-container; }
    wait_for() { :; }
    run_bounded() {
        if [[ "$3" == inspect ]]; then
            printf '0\n'
        fi
    }
    trim_file() { :; }
    topic_end_offset() {
        ((compose_call_timeout >= 30)) || return 124
        printf '1\n'
    }
    kcat() { printf '{"id":1}\n'; }
    recovery_final_boundary || fail 'final source-boundary query inherited the live-probe budget'
    [[ "${compose_call_timeout}" -eq 20 ]] || fail 'final query changed later live-probe budgets'
    jq -e '.exit_code == 0 and .timeout_seconds == 60' \
        "${artifact_dir}/traffic/source-boundary-query.json" >/dev/null \
        || fail 'final query did not retain its successful boundary'
    [[ "$(wc -l <"${artifact_dir}/traffic/accepted-input.ndjson")" -eq 1 ]] \
        || fail 'successful final query did not build the accepted ledger'
    rm "${artifact_dir}/traffic/accepted-input.ndjson"
    topic_end_offset() { return 124; }
    if recovery_final_boundary >"${tmp_dir}/final-query-failure.txt" 2>&1; then
        fail 'an expired final source query was accepted'
    fi
    [[ "${failure_category}" == controller ]] || fail 'a source-query timeout was a product failure'
    jq -e '.exit_code == 124 and .timeout_seconds == 60' \
        "${artifact_dir}/traffic/source-boundary-query.json" >/dev/null \
        || fail 'final query timeout evidence was lost'
    [[ ! -f "${artifact_dir}/traffic/accepted-input.ndjson" ]] \
        || fail 'failed final query constructed an accepted ledger'
)

batch_dir="${tmp_dir}/batch"
mkdir -p "${batch_dir}"
cat >"${tmp_dir}/transcript.log" <<'EOF'
=== begin created
created resource 'chaos_one'

=== end created 0
=== begin refused
error: resource 'chaos_two' already exists

=== end refused 0
=== begin unknown
outcome: not known yet; the command was admitted, and retrying the same reference reports it

=== end unknown 0
=== begin interrupted
EOF
split_batch_transcript "${tmp_dir}/transcript.log" "${batch_dir}"
[[ "$(batch_write_outcome "${batch_dir}" created "created resource 'chaos_one'")" == acknowledged ]] \
    || fail 'a completed creation was not acknowledged'
[[ "$(batch_write_outcome "${batch_dir}" refused "created resource 'chaos_two'")" == refused ]] \
    || fail 'an explicit error was not classified as a refusal'
[[ "$(batch_write_outcome "${batch_dir}" unknown "created resource 'chaos_three'")" == uncertain ]] \
    || fail 'an unknown outcome was not classified as uncertain'
[[ "$(batch_write_outcome "${batch_dir}" interrupted "created resource 'chaos_four'")" == uncertain ]] \
    || fail 'a statement interrupted by the bound was not classified as uncertain'
[[ "$(batch_write_outcome "${batch_dir}" never "created resource 'chaos_five'")" == unattempted ]] \
    || fail 'a statement that never started was not classified as unattempted'
batch_read_succeeded "${batch_dir}" created || fail 'a successful read was rejected'
if batch_read_succeeded "${batch_dir}" refused || batch_read_succeeded "${batch_dir}" interrupted; then
    fail 'a failed or unfinished read was accepted'
fi

cat >"${tmp_dir}/metrics.txt" <<'EOF'
# HELP nervix_consensus_log_last_index The highest Raft index this node's log holds.
nervix_consensus_log_last_index 184
nervix_consensus_log_snapshot_index -1
nervix_interconnect_requests_total{operation="liveness",outcome="answered"} 40
nervix_interconnect_requests_total{operation="snapshot",outcome="answered"} 6
nervix_interconnect_requests_total{operation="snapshot",outcome="failed"} 1
nervix_messages_total{direction="sent",physical_node_id="node-2",relay="chaos_records",target="chaos_ingestor"} 7
nervix_messages_total{direction="received",physical_node_id="node-2",relay="chaos_records",target="chaos_emitter"} 5
EOF
[[ "$(metric_value "${tmp_dir}/metrics.txt" nervix_consensus_log_last_index)" == 184 ]] \
    || fail 'an unlabeled gauge was not read'
[[ "$(metric_value "${tmp_dir}/metrics.txt" nervix_consensus_log_snapshot_index)" == -1 ]] \
    || fail 'an absent position was not read as -1'
[[ "$(metric_value "${tmp_dir}/metrics.txt" nervix_interconnect_requests_total 'operation="snapshot"' 'outcome="answered"')" == 6 ]] \
    || fail 'a labeled series was not selected by every label'
[[ -z "$(metric_value "${tmp_dir}/metrics.txt" nervix_consensus_log_purged_index)" ]] \
    || fail 'a missing family produced a value'
[[ "$(metric_sum "${tmp_dir}/metrics.txt" nervix_messages_total)" == 12 ]] \
    || fail 'graph messages were not summed across series'
[[ "$(metric_sum "${tmp_dir}/metrics.txt" nervix_missing_total)" == 0 ]] \
    || fail 'a missing family did not sum to zero'

cat >"${tmp_dir}/status.txt" <<'EOF'
[chitchat]
cluster_id: chaos-self-test
seed_nodes: 10.213.7.11:47395
self:
- node_id: node-1
  gossip_addr: 10.213.7.11:47395
  terminating: false
live_nodes:
- node_id: node-3
  gossip_addr: 10.213.7.13:47395
  terminating: false

[raft]
raft.id: node-1
raft.current_leader: node-1
raft.current_term: 2
raft.state: Leader
raft.cordoned_nodes: (none)
raft.last_log_index: 192
raft.last_applied: 191
raft.membership:
- node-1 [voter] nervix-1:47395
- node-2 [voter] nervix-2:47395
- node-3 [voter] nervix-3:47395

[interconnect]
- node-3: addr=nervix-3:47395 generation=1 observation=healthy observation_age=1s status=connected

[domains]
- chaos_baseline status=Running pace=UNPACED

[schedule]
- domain=chaos_baseline kind=codec name=chaos_json owner=- replicas=-
- domain=chaos_baseline kind=ingestor name=chaos_ingestor owner=node-1 replicas=-
- domain=chaos_baseline kind=relay name=chaos_records owner=node-3 replicas=- transition_from=node-2 state_recovery=complete
- domain=chaos_baseline kind=emitter name=chaos_emitter owner=node-3 replicas=-

[warnings]
- raft member 'node-2' is not currently visible in gossip
EOF
status_record "${tmp_dir}/status.txt" nervix-1 1000 >"${tmp_dir}/status-record.json"
jq -e '
    .answered and .node == "node-1" and .state == "Leader" and .leader == "node-1" and .term == 2
    and .last_log_index == 192 and .last_applied == 191
    and .live == ["node-1", "node-3"] and .voters == ["node-1", "node-2", "node-3"]
    and .owners == {ingestor: "node-1", relay: "node-3", emitter: "node-3"}
' "${tmp_dir}/status-record.json" >/dev/null || fail "a node status was parsed incorrectly: $(cat "${tmp_dir}/status-record.json")"
status_record "${tmp_dir}/status.txt" nervix-2 1000 >"${tmp_dir}/misattributed.json"
jq -e '.answered == false' "${tmp_dir}/misattributed.json" >/dev/null \
    || fail 'a status that names another node was attributed to the probed node'
printf 'Error: transport error\n' >"${tmp_dir}/unanswered.txt"
status_record "${tmp_dir}/unanswered.txt" nervix-1 1000 >"${tmp_dir}/unanswered.json"
jq -e '.answered == false' "${tmp_dir}/unanswered.json" >/dev/null \
    || fail 'an unanswered status was recorded as an answer'

jq -n '{host:"nervix-1",domains:["- chaos_baseline status=Running pace=UNPACED"],
    membership:["- node-1 [voter] nervix-1:47395"],models:{"schema-chaos_record":"CREATE SCHEMA chaos_record (\n);\n"},
    resources:{chaos_restart_before:"present"}}' >"${tmp_dir}/configuration-before.json"
cp "${tmp_dir}/configuration-before.json" "${tmp_dir}/configuration-after.json"
[[ -z "$(configuration_differences "${tmp_dir}/configuration-before.json" "${tmp_dir}/configuration-after.json")" ]] \
    || fail 'identical configuration was reported as different'
jq '.resources.chaos_restart_before = "absent" | .models["schema-chaos_record"] = "CREATE SCHEMA other;\n"' \
    "${tmp_dir}/configuration-before.json" >"${tmp_dir}/configuration-lost.json"
[[ "$(configuration_differences "${tmp_dir}/configuration-before.json" "${tmp_dir}/configuration-lost.json" | tr '\n' ' ')" == 'models resources ' ]] \
    || fail 'lost configuration was not reported by part'

jq -n '[{Config:{Env:["PATH=/usr/bin","NERVIX_RAFT_HEARTBEAT_INTERVAL=250ms",
    "NERVIX_NODE_UNAVAILABILITY_TIMEOUT=10s","NERVIX_NODE_ID=node-1","NERVIX_RAFT_SNAPSHOT_ENTRY_THRESHOLD=64"]}}]' \
    >"${tmp_dir}/inspection.json"
[[ "$(configured_settings "${tmp_dir}/inspection.json")" == '{"NERVIX_RAFT_HEARTBEAT_INTERVAL":"250ms","NERVIX_NODE_UNAVAILABILITY_TIMEOUT":"10s","NERVIX_RAFT_SNAPSHOT_ENTRY_THRESHOLD":"64"}' ]] \
    || fail "configured settings were read incorrectly: $(configured_settings "${tmp_dir}/inspection.json")"

# Snapshot catch-up: the survivors purged past the follower, which then holds a covering snapshot,
# applied the leader's restart boundary, and was sent snapshot sections.
jq -n '{follower:"nervix-2",follower_log_bound:41,
    restart:{leader_applied_index:183,survivors:[
        {host:"nervix-1",last_index:183,snapshot_index:149,purged_index:139,snapshot_requests:0},
        {host:"nervix-3",last_index:183,snapshot_index:149,purged_index:133,snapshot_requests:0}]},
    caught_up:{follower:{last_index:184,snapshot_index:149,purged_index:149,last_applied:184},
        survivors:[{host:"nervix-1",snapshot_requests:6},{host:"nervix-3",snapshot_requests:0}]}}' \
    >"${tmp_dir}/catch-up.json"
"${verify}" snapshot-catch-up --evidence "${tmp_dir}/catch-up.json" --output "${tmp_dir}/catch-up-verdict.json"
jq -e '.verdict == "pass" and .survivors_purged_at_restart == 133 and .snapshot_requests_answered == 6' \
    "${tmp_dir}/catch-up-verdict.json" >/dev/null || fail 'valid snapshot catch-up did not pass'
check_catch_up_failure() {
    local case_name="$1"
    local mutation="$2"
    local failure="$3"
    jq "${mutation}" "${tmp_dir}/catch-up.json" >"${tmp_dir}/catch-up-case.json"
    expect_rejected "${case_name}" "${verify}" snapshot-catch-up \
        --evidence "${tmp_dir}/catch-up-case.json" --output "${tmp_dir}/catch-up-case-verdict.json"
    jq -e --arg failure "${failure}" '.failures == [$failure]' "${tmp_dir}/catch-up-case-verdict.json" >/dev/null \
        || fail "${case_name} did not report ${failure}"
}
check_catch_up_failure 'a survivor still retained the follower suffix' \
    '.restart.survivors[1].purged_index = 41' log_suffix_purged
check_catch_up_failure 'the follower never installed a covering snapshot' \
    '.caught_up.follower.snapshot_index = 41' snapshot_installed
check_catch_up_failure 'the follower stayed behind the restart boundary' \
    '.caught_up.follower.last_applied = 182' caught_up
check_catch_up_failure 'no snapshot section was transferred' \
    '.caught_up.survivors[0].snapshot_requests = 0' transfer_observed
jq 'del(.follower_log_bound)' "${tmp_dir}/catch-up.json" >"${tmp_dir}/catch-up-invalid.json"
status=0
"${verify}" snapshot-catch-up --evidence "${tmp_dir}/catch-up-invalid.json" \
    --output "${tmp_dir}/catch-up-invalid-verdict.json" >/dev/null 2>&1 || status=$?
[[ "${status}" -eq 2 ]] || fail "incomplete catch-up evidence returned ${status}, expected 2"

# Before admission the isolated former owner answers on every listener and stays inert, observed
# from the verified isolation until the declared window has passed.
pre_admission_sample() {
    jq -nc --argjson at_ms "$1" --arg address "$2" --argjson graph "$3" --argjson admitted "$4" \
        --argjson applied "$5" --argjson session "$6" '
        {sample: "fixture", at_ms: $at_ms,
         listeners: {session: $session, interconnect: true, console: true, observability: true, status_route: true},
         readyz: "ready", members: [{consumer: "rdkafka-1", address: $address, node_host: "nervix-3", partitions: 1}],
         graph_messages: $graph, admitted_logged: $admitted, last_applied: $applied}'
}
inert_samples() {
    local at_ms
    for at_ms in "$@"; do
        pre_admission_sample "${at_ms}" 10.213.7.13 0 false 191 true
    done
}
verify_pre_admission() {
    "${verify}" pre-admission --samples "$1" --address 10.213.7.12 --isolated-at-ms 500 \
        --min-window-ms 45000 --max-gap-ms 20000 --output "$2"
}
inert_samples 1000 6000 11000 16000 21000 26000 31000 36000 41000 46000 >"${tmp_dir}/pre-admission.ndjson"
verify_pre_admission "${tmp_dir}/pre-admission.ndjson" "${tmp_dir}/pre-admission-verdict.json"
jq -e '.verdict == "pass" and .window_ms == 45500 and .longest_gap_ms == 5000 and .readyz == ["ready"]' \
    "${tmp_dir}/pre-admission-verdict.json" >/dev/null || fail 'an inert isolated node did not pass'
check_pre_admission_failure() {
    local case_name="$1"
    local failure="$2"
    shift 2
    {
        inert_samples 1000 6000 11000 16000 21000 26000 31000 36000 41000
        printf '%s\n' "$@"
    } >"${tmp_dir}/pre-admission-case.ndjson"
    expect_rejected "${case_name}" verify_pre_admission \
        "${tmp_dir}/pre-admission-case.ndjson" "${tmp_dir}/pre-admission-case-verdict.json"
    jq -e --arg failure "${failure}" '.failures == [$failure]' "${tmp_dir}/pre-admission-case-verdict.json" \
        >/dev/null || fail "${case_name} did not report ${failure}: $(jq -c .failures "${tmp_dir}/pre-admission-case-verdict.json")"
}
check_pre_admission_failure 'the isolated node joined the source consumer group' no_consumer_membership \
    "$(pre_admission_sample 46000 10.213.7.12 0 false 191 true)"
check_pre_admission_failure 'the isolated node produced graph output' no_graph_output \
    "$(pre_admission_sample 46000 10.213.7.13 3 false 191 true)"
check_pre_admission_failure 'the isolated node reported admission' not_admitted \
    "$(pre_admission_sample 46000 10.213.7.13 0 true 191 true)"
check_pre_admission_failure 'the isolated node applied a new entry' applied_frozen \
    "$(pre_admission_sample 46000 10.213.7.13 0 false 192 true)"
check_pre_admission_failure 'a public listener of the isolated node did not answer' listeners_answered \
    "$(pre_admission_sample 46000 10.213.7.13 0 false 191 false)"
check_pre_admission_failure 'the observations ended before the declared window' window_covered
inert_samples 1000 6000 31000 36000 41000 46000 >"${tmp_dir}/pre-admission-gap.ndjson"
expect_rejected 'the observations left a long gap' verify_pre_admission \
    "${tmp_dir}/pre-admission-gap.ndjson" "${tmp_dir}/pre-admission-gap-verdict.json"
jq -e '.failures == ["observation_continuous"] and .longest_gap_ms == 25000' \
    "${tmp_dir}/pre-admission-gap-verdict.json" >/dev/null || fail 'a gap in the observations was not reported'
inert_samples 200 6000 11000 16000 21000 26000 31000 36000 41000 46000 >"${tmp_dir}/pre-admission-early.ndjson"
expect_rejected 'an observation from before the verified isolation' verify_pre_admission \
    "${tmp_dir}/pre-admission-early.ndjson" "${tmp_dir}/pre-admission-early-verdict.json"

# Admission is logged after waiting for catch-up, and only once the isolation healed.
cat >"${tmp_dir}/node.log" <<'EOF'
2026-10-05T16:05:59.393171Z  INFO nervix_consensus: raft transition: state=Follower leader=node-1 term=1 last_log_index=192 last_applied=192
2026-10-05T16:06:04.395793Z  WARN nervix_server::application::runtime_admission: runtime execution is waiting for linearizable consensus catch-up error=failed to establish a linearizable consensus read
2026-10-05T16:06:29.198981Z  INFO nervix_server::application::runtime_admission: runtime execution admitted after linearizable consensus catch-up committed_log_index=203
EOF
healed_ns="$(date -d 2026-10-05T16:06:27.770231560Z +%s%N)"
"${verify}" admission --log "${tmp_dir}/node.log" --not-before-ns "${healed_ns}" \
    --output "${tmp_dir}/admission.json"
jq -e '.verdict == "pass" and .committed_log_index == 203 and .admitted_after_boundary_ms == 1428' \
    "${tmp_dir}/admission.json" >/dev/null || fail 'admission after healing did not pass'
expect_rejected 'admission before the isolation healed' "${verify}" admission --log "${tmp_dir}/node.log" \
    --not-before-ns "$(date -d 2026-10-05T16:06:30Z +%s%N)" --output "${tmp_dir}/admission-early.json"
jq -e '.failures == ["admitted_after_boundary"]' "${tmp_dir}/admission-early.json" >/dev/null \
    || fail 'early admission was not reported'
grep -v 'waiting for linearizable' "${tmp_dir}/node.log" >"${tmp_dir}/node-no-wait.log"
expect_rejected 'admission without waiting for catch-up' "${verify}" admission \
    --log "${tmp_dir}/node-no-wait.log" --not-before-ns "${healed_ns}" --output "${tmp_dir}/admission-no-wait.json"
grep -v 'execution admitted' "${tmp_dir}/node.log" >"${tmp_dir}/node-unadmitted.log"
expect_rejected 'no admission after healing' "${verify}" admission \
    --log "${tmp_dir}/node-unadmitted.log" --not-before-ns "${healed_ns}" --output "${tmp_dir}/admission-none.json"
jq -e '.failures == ["admitted", "admitted_after_boundary"]' "${tmp_dir}/admission-none.json" >/dev/null \
    || fail 'a missing admission was not reported'

# Ownership after a whole-cluster restart follows the voter observation grace.
jq -n '{owners:{ingestor:"node-1",relay:"node-2",emitter:"node-3"},voters:["node-1","node-2","node-3"]}' \
    >"${tmp_dir}/owners-before.json"
jq -n '{owners:{ingestor:"node-1",relay:"node-2",emitter:"node-3"},live_voters:["node-1","node-2","node-3"]}' \
    >"${tmp_dir}/owners-kept.json"
jq '.owners.emitter = "node-1"' "${tmp_dir}/owners-kept.json" >"${tmp_dir}/owners-moved.json"
jq -n '{"node-1":1000000000000,"node-2":1000100000000,"node-3":1000200000000}' >"${tmp_dir}/starts.json"
{
    jq -nc '{host:"nervix-3",at_ns:1000300000000,answered:false}'
    jq -nc '{host:"nervix-1",node:"node-1",at_ns:1000500000000,answered:true,state:"Leader",live:["node-1"]}'
    jq -nc '{host:"nervix-2",node:"node-2",at_ns:1001000000000,answered:true,state:"Follower",live:["node-1","node-2"]}'
    jq -nc '{host:"nervix-1",node:"node-1",at_ns:1002000000000,answered:true,state:"Leader",live:["node-1","node-2","node-3"]}'
} >"${tmp_dir}/samples-prompt.ndjson"
sed 's/1002000000000/1012000000000/' "${tmp_dir}/samples-prompt.ndjson" >"${tmp_dir}/samples-late.ndjson"
"${verify}" ownership --before "${tmp_dir}/owners-before.json" --after "${tmp_dir}/owners-kept.json" \
    --samples "${tmp_dir}/samples-prompt.ndjson" --starts "${tmp_dir}/starts.json" --grace-ms 10000 \
    --output "${tmp_dir}/ownership.json"
jq -e '.verdict == "pass" and .contract == "owners keep their work"
    and .leaders_observed[0].voters_observed_ns == {"node-1":1000500000000,"node-2":1002000000000,"node-3":1002000000000}' \
    "${tmp_dir}/ownership.json" >/dev/null || fail 'owners kept within the grace did not pass'
expect_rejected 'an owner observed within the grace lost its work' "${verify}" ownership \
    --before "${tmp_dir}/owners-before.json" --after "${tmp_dir}/owners-moved.json" \
    --samples "${tmp_dir}/samples-prompt.ndjson" --starts "${tmp_dir}/starts.json" --grace-ms 10000 \
    --output "${tmp_dir}/ownership-moved.json"
jq -e '.failures == ["owners_kept_within_grace"] and .moved == [{entity:"emitter",from:"node-3",to:"node-1"}]' \
    "${tmp_dir}/ownership-moved.json" >/dev/null || fail 'a lost owner was not reported'
"${verify}" ownership --before "${tmp_dir}/owners-before.json" --after "${tmp_dir}/owners-moved.json" \
    --samples "${tmp_dir}/samples-late.ndjson" --starts "${tmp_dir}/starts.json" --grace-ms 10000 \
    --output "${tmp_dir}/ownership-late.json"
jq -e '.verdict == "pass" and .contract == "failover permitted"' "${tmp_dir}/ownership-late.json" >/dev/null \
    || fail 'failover of a voter observed after the grace was rejected'
jq '.live_voters = ["node-1", "node-2"]' "${tmp_dir}/owners-kept.json" >"${tmp_dir}/owners-dead.json"
expect_rejected 'an owner on a node that is not live' "${verify}" ownership \
    --before "${tmp_dir}/owners-before.json" --after "${tmp_dir}/owners-dead.json" \
    --samples "${tmp_dir}/samples-late.ndjson" --starts "${tmp_dir}/starts.json" --grace-ms 10000 \
    --output "${tmp_dir}/ownership-dead.json"
: >"${tmp_dir}/samples-none.ndjson"
"${verify}" ownership --before "${tmp_dir}/owners-before.json" --after "${tmp_dir}/owners-moved.json" \
    --samples "${tmp_dir}/samples-none.ndjson" --starts "${tmp_dir}/starts.json" --grace-ms 10000 \
    --output "${tmp_dir}/ownership-unobserved.json"
jq -e '.contract == "failover permitted" and .leaders_observed == []' "${tmp_dir}/ownership-unobserved.json" \
    >/dev/null || fail 'a restart without an observed leader was judged against the grace'

printf 'chaos recovery self-test passed\n'
