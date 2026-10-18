#!/usr/bin/env bash
# Self-test of the sink progress a failover-length pause requires once its graph has moved to the
# survivors. The timelines are those pause-resume retained in soak shard 5 of Docker Build run
# 38933948103. In the 75-second pause of the leader node-1, which also ran the Kafka ingestor and
# the relay, the relocated consumer joined the source group 22.9 seconds after the runner requested
# the pause, the runner observed the relocation at 23.7 seconds, and the broker's group coordinator
# removed node-1's paused member and gave the partition to node-2 at 45.0 seconds. The 75-second
# pause of the relay owner node-2 left the consumer running: the runner observed the relocation at
# 18.6 seconds and sink progress at 20.8 seconds. Each observation runs on a simulated clock and
# costs about what it cost in that run.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT
current_phase='pause delivery self-test'
# shellcheck source=../pause-resume-scenario.sh
source "${script_dir}/pause-resume-scenario.sh"

fail() {
    printf 'pause delivery self-test failed: %s\n' "$*" >&2
    exit 1
}

# The handoff bound starts from librdkafka's default session timeout, which the source consumer
# keeps only while the baseline graph leaves session.timeout.ms unset.
baseline_graph="$(<"${script_dir}/fixtures/baseline.nspl")"
if [[ "${baseline_graph}" == *session.timeout.ms* ]]; then
    fail "fixtures/baseline.nspl sets session.timeout.ms, but the handoff bound assumes librdkafka's default of ${pause_group_session_timeout_seconds} seconds"
fi

node_hosts=(nervix-1 nervix-2 nervix-3)
export CHAOS_NODE_1_ADDRESS=10.213.206.11
export CHAOS_NODE_2_ADDRESS=10.213.206.12
export CHAOS_NODE_3_ADDRESS=10.213.206.13

# The simulated clock counts milliseconds since the runner requested the pause. It lives in a file
# because the runner reads offsets through command substitutions, which run in subshells.
clock="${tmp_dir}/clock"
group_reads="${tmp_dir}/group-reads"
never=1000000000
unpause_ms=75000
# The relocated consumer joins the source group, and the coordinator starts a rebalance.
group_rebalance_ms="${never}"
# The coordinator removes the paused member and gives the partition to the relocated consumer.
group_handoff_ms="${never}"
# The first record the relocated graph delivers reaches the sink topic.
sink_resumes_ms="${never}"
sink_offset_before=140

now() {
    printf '%s\n' "$(<"${clock}")"
}

advance() {
    printf '%s\n' "$(($(now) + $1))" >"${clock}"
}

epoch_ms() {
    now
}

# The runner's wait_for on the simulated clock: an attempt costs what its observations cost, and a
# failed attempt sleeps for one second before the next, until the bound passes.
wait_for() {
    local description="$1"
    local seconds="$2"
    shift 2
    local deadline_ms=$(($(now) + seconds * 1000))
    while (($(now) < deadline_ms)); do
        if "$@"; then
            printf 'ready: %s\n' "${description}"
            return 0
        fi
        advance 1000
    done
    printf 'timed out waiting for %s during phase %s\n' "${description}" "${current_phase}" >&2
    return 124
}

# A Kafka end-offset read took about a second in the retained run.
topic_end_offset() {
    [[ "$1" == chaos_output ]] || fail "read the end offset of $1"
    advance 1050
    local at
    at="$(now)"
    if ((at < sink_resumes_ms)); then
        printf '%s\n' "${sink_offset_before}"
    else
        printf '%s\n' "$((sink_offset_before + 1 + (at - sink_resumes_ms) / 1000))"
    fi
}

pause_target_is() {
    [[ "$1" == pause-target ]] || fail "inspected container $1"
    advance 100
    local paused=false
    if (($(now) < unpause_ms)); then
        paused=true
    fi
    [[ "${paused}" == "$2" ]]
}

# The broker's consumer-group tool, as kafka-consumer-groups.sh --describe --members prints the
# source group: a stable group lists each member's partitions, and a rebalancing one lists its
# members without any.
broker_admin() {
    [[ "$1" == /opt/kafka/bin/kafka-consumer-groups.sh && " $* " == *' --group chaos_baseline --describe --members '* ]] \
        || fail "ran an unexpected broker command: $*"
    printf '%s\n' "$(($(<"${group_reads}") + 1))" >"${group_reads}"
    advance 2000
    local at
    at="$(now)"
    local header='GROUP           CONSUMER-ID                                  HOST            CLIENT-ID       #PARTITIONS     '
    local paused_member='rdkafka-5210d5a6-ef74-45b1-8062-e554c101407d /10.213.206.11  rdkafka'
    local relocated_member='rdkafka-a4d7f4e9-766f-4804-93c3-99d86eaec536 /10.213.206.12  rdkafka'
    if ((at >= group_handoff_ms)); then
        printf '\n%s\n' "${header}"
        printf 'chaos_baseline  %s         1               \n' "${relocated_member}"
    elif ((at >= group_rebalance_ms)); then
        printf "\nWarning: Consumer group 'chaos_baseline' is rebalancing.\n\n%s\n" "${header}"
        printf 'chaos_baseline  %s         0               \n' "${paused_member}" "${relocated_member}"
    else
        printf '\n%s\n' "${header}"
        printf 'chaos_baseline  %s         1               \n' "${paused_member}"
    fi
}

# Writes the public evidence of one round: the roles read immediately before the pause, and the
# survivor's description of the relocated ingestor.
round_with() {
    local round_dir="$1"
    local role="$2"
    local ingestor_after="$3"
    mkdir -p "${round_dir}/immediately-before"
    printf '%s\n' "${role}" >"${round_dir}/immediately-before/role.json"
    printf 'ingestor: chaos_ingestor\nkind: INGESTOR\nowner: %s\nstatus: running\nready: true\n' \
        "${ingestor_after}" >"${round_dir}/ingestor-survivor.attempt.txt"
}

leader_role='{"role":"leader","target_kind":"leader","target_node":"node-1","leader":"node-1","ingestor_owner":"node-1","relay_owner":"node-1","emitter_owner":"node-3"}'
relay_owner_role='{"role":"execution-owner","target_kind":"relay","target_node":"node-2","leader":"node-1","ingestor_owner":"node-1","relay_owner":"node-2","emitter_owner":"node-3"}'

# Runs the check from the moment the runner observed the relocation. It runs in a subshell, as a
# failed check ends the run, and keeps its status, its message and what it recorded.
run_check() {
    local round_dir="$1"
    local placement_ms="$2"
    printf '%s\n' "${placement_ms}" >"${clock}"
    printf '0\n' >"${group_reads}"
    check_status=0
    (
        pause_delivery_during_pause pause-target 0 "${round_dir}" >"${tmp_dir}/stdout" 2>"${tmp_dir}/stderr" \
            || exit $?
        printf '%s %s\n' "${pause_delivery_ms}" "${pause_output_during}" >"${tmp_dir}/recorded"
    ) || check_status=$?
    check_message="$(<"${tmp_dir}/stderr")"
}

expect_delivery() {
    local case_name="$1"
    [[ "${check_status}" -eq 0 ]] \
        || fail "${case_name}: the check failed with status ${check_status}: ${check_message}"
    local delivery_ms output_during
    read -r delivery_ms output_during <"${tmp_dir}/recorded"
    ((output_during > sink_offset_before)) \
        || fail "${case_name}: the check recorded sink offset ${output_during} without progress"
    ((delivery_ms >= sink_resumes_ms && delivery_ms == $(now) && delivery_ms < unpause_ms)) \
        || fail "${case_name}: the check recorded delivery at ${delivery_ms} ms"
}

expect_failure() {
    local case_name="$1"
    local category="$2"
    local reason="$3"
    [[ "${check_status}" -ne 0 ]] || fail "${case_name}: the check passed"
    [[ "${check_message}" == *"pause ${category} failure: ${reason}"* ]] \
        || fail "${case_name}: the check failed without naming '${reason}' as ${category}: ${check_message}"
}

expect_product_failure() {
    expect_failure "$1" product "$2"
}

# The retained leader pause: the coordinator gives the partition to node-2 21.3 seconds after the
# runner observed the relocation, and the sink resumes 600 ms later.
leader_round="${tmp_dir}/4-75-leader"
round_with "${leader_round}" "${leader_role}" node-2
group_rebalance_ms=22932
group_handoff_ms=45019
sink_resumes_ms=45619
run_check "${leader_round}" 23715
expect_delivery 'retained leader pause'
(($(<"${group_reads}") > 0)) || fail 'retained leader pause: the check never read the source consumer group'
jq -e '.members == [{consumer: "rdkafka-a4d7f4e9-766f-4804-93c3-99d86eaec536", address: "10.213.206.12",
    node_host: "nervix-2", partitions: 1}]' "${leader_round}/group-handoff/consumer-group.json" >/dev/null \
    || fail 'retained leader pause: the check did not keep the group it accepted'

# The relocation is observed before the relocated consumer joins. The first read still shows the
# paused member holding the partition, which is not the handoff.
round_with "${tmp_dir}/early-relocation" "${leader_role}" node-2
run_check "${tmp_dir}/early-relocation" 20000
expect_delivery 'relocation observed before the consumer joined'
grep -Fq 'chaos_baseline  rdkafka-a4d7f4e9-766f-4804-93c3-99d86eaec536 /10.213.206.12  rdkafka         1' \
    "${tmp_dir}/early-relocation/group-handoff/consumer-group.json.txt" \
    || fail 'relocation observed before the consumer joined: the check accepted another group'

# The relocation is observed after the handoff deadline: one read of the group still decides.
round_with "${tmp_dir}/late-relocation" "${leader_role}" node-2
run_check "${tmp_dir}/late-relocation" 61000
expect_delivery 'relocation observed after the handoff deadline'
[[ "$(<"${group_reads}")" -eq 1 ]] \
    || fail "relocation observed after the handoff deadline: the check read the group $(<"${group_reads}") times"

# The coordinator never removes the paused member, so the relocated consumer never reads.
round_with "${tmp_dir}/handoff-missing" "${leader_role}" node-2
group_handoff_ms="${never}"
sink_resumes_ms="${never}"
run_check "${tmp_dir}/handoff-missing" 23715
expect_product_failure 'missing handoff' \
    'the source consumer group did not move to the relocated ingestor during the pause'

# The partition reaches the relocated consumer, but nothing reaches the sink.
round_with "${tmp_dir}/sink-stalled" "${leader_role}" node-2
group_handoff_ms=45019
sink_resumes_ms="${never}"
run_check "${tmp_dir}/sink-stalled" 23715
expect_product_failure 'stalled sink' 'sink did not progress on surviving nodes during pause'

# The handoff is first observed after the unpause, outside the window the case judges.
round_with "${tmp_dir}/handoff-after-unpause" "${leader_role}" node-2
unpause_ms=46000
sink_resumes_ms=45619
run_check "${tmp_dir}/handoff-after-unpause" 23715
expect_product_failure 'late handoff' 'the source consumer group moved only after unpause'
unpause_ms=75000

# The retained relay-owner pause left the consumer in place, so its sink window opens at the
# relocation and the group is never read.
relay_round="${tmp_dir}/3-75-execution-owner"
round_with "${relay_round}" "${relay_owner_role}" node-1
group_rebalance_ms="${never}"
group_handoff_ms="${never}"
sink_resumes_ms=19800
run_check "${relay_round}" 18587
expect_delivery 'retained relay-owner pause'
[[ "$(<"${group_reads}")" -eq 0 ]] || fail 'retained relay-owner pause: the check read the consumer group'

# Without a consumer handoff, the 20-second window still opens at the relocation.
round_with "${tmp_dir}/late-sink" "${relay_owner_role}" node-1
sink_resumes_ms=45619
run_check "${tmp_dir}/late-sink" 18587
expect_product_failure 'late sink' 'sink did not progress on surviving nodes during pause'

# The sink progresses, but the target was unpaused before the check confirmed the pause.
round_with "${tmp_dir}/sink-after-unpause" "${relay_owner_role}" node-1
sink_resumes_ms=19800
unpause_ms=20700
run_check "${tmp_dir}/sink-after-unpause" 18587
expect_failure 'sink after unpause' controller 'sink progress was sampled after the pause window'
unpause_ms=75000

printf 'pause delivery self-test passed\n'
