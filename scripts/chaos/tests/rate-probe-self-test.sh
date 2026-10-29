#!/usr/bin/env bash
set -euo pipefail

chaos_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
script_dir="${chaos_dir}"
# shellcheck source=../tool-images.sh
source "${chaos_dir}/tool-images.sh"
# shellcheck source=../link-degradation.sh
source "${chaos_dir}/link-degradation.sh"
CHAOS_PROBE_IMAGE="${chaos_probe_image}"
scenario=rate-probe-self-test
run_id="rate-probe-check-$$-${RANDOM}"
tmp_dir="${CHAOS_RATE_PROBE_ARTIFACTS:-$(mktemp -d)}"
mkdir -p "${tmp_dir}"

# A failed self-test keeps its probes' raw streams and records: in the evidence directory that the
# verifier self-check names, or where they are.
cleanup() {
    local status=$?
    "${chaos_dir}/cleanup.sh" --run-id "${run_id}" --quiet >/dev/null 2>&1 || true
    [[ -z "${CHAOS_RATE_PROBE_ARTIFACTS:-}" ]] || return 0
    if [[ "${status}" -eq 0 ]]; then
        rm -rf "${tmp_dir}"
        return 0
    fi
    local retained="${tmp_dir}"
    if [[ -n "${CHAOS_SELF_TEST_EVIDENCE:-}" ]] && mkdir -p "${CHAOS_SELF_TEST_EVIDENCE}" \
        && mv "${tmp_dir}" "${CHAOS_SELF_TEST_EVIDENCE}/rate-probe"; then
        retained="${CHAOS_SELF_TEST_EVIDENCE}/rate-probe"
    fi
    printf 'rate probe self-test evidence retained in %s\n' "${retained}" >&2
}
trap cleanup EXIT
fail() { printf 'rate probe self-test failed: %s\n' "$*" >&2; exit 1; }

for image in "${CHAOS_PROBE_IMAGE}" "${chaos_nettools_image}"; do
    if ! timeout --foreground --kill-after=5s 20s docker image inspect "${image}" >/dev/null 2>&1; then
        timeout --foreground --kill-after=5s 180s docker pull "${image}" \
            || fail "pinned tool image ${image} is unavailable"
    fi
done
docker network create --label "io.nervix.chaos.run=${run_id}" "${run_id}" >/dev/null
# Namespace owners live until cleanup, independently of how long the controls take on this
# worker. Each probe keeps its own bound; an owner must not expire between controls.
test_sender_id="$(docker run --detach --label "io.nervix.chaos.run=${run_id}" \
    --network "${run_id}" "${CHAOS_PROBE_IMAGE}" tail -f /dev/null)"
test_receiver_id="$(docker run --detach --label "io.nervix.chaos.run=${run_id}" \
    --network "${run_id}" "${CHAOS_PROBE_IMAGE}" tail -f /dev/null)"
# A namespace owner Docker created but never started, whose namespace no helper can join.
test_stopped_id="$(docker create --label "io.nervix.chaos.run=${run_id}" \
    --network "${run_id}" "${CHAOS_PROBE_IMAGE}" tail -f /dev/null)"

owned_service_container() {
    case "$1" in
        sender) printf '%s\n' "${test_sender_id}" ;;
        receiver) printf '%s\n' "${test_receiver_id}" ;;
        stopped) printf '%s\n' "${test_stopped_id}" ;;
        *) fail "unknown test endpoint $1" ;;
    esac
}
node_address() {
    docker inspect --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
        "$(owned_service_container "$1")"
}
epoch_ms() {
    local nanoseconds
    nanoseconds="$(date +%s%N)"
    printf '%s\n' "$((nanoseconds / 1000000))"
}

# Invoke the production Docker commands. Only the deliberate unreachable control and the ping
# controls of a slow start and a stalled sample shorten a bound; the partial-payload control changes
# the producer's input, not the probe owner.
test_case=delayed-connect
# These controls rewrite code that the helper's shell executes; parameter expressions stay literal.
# shellcheck disable=SC2016
run_bounded() {
    local limit="$1"
    shift
    local arguments=("$@") i status=0
    if [[ "${test_case}" == launcher-delay && "$1 $2" == 'docker run' \
        && " ${arguments[*]} " != *' --detach '* ]]; then
        sleep 6
    fi
    # The ping helper's launch names its cidfile *.helper-id.txt; its sample is the only wait of
    # these cases.
    if [[ "$1 $2" == 'docker run' && " ${arguments[*]} " == *'.helper-id.txt '* ]]; then
        case "${test_case}" in
            ping-slow-start)
                # Docker takes six seconds to start the helper.
                sleep 6
                ;;
            ping-launch-stall)
                # Docker creates the helper, but its start outlasts the launch bound.
                local create=(docker create)
                for i in "${!arguments[@]}"; do
                    if ((i > 1)) && [[ "${arguments[i]}" != --detach ]]; then
                        create+=("${arguments[i]}")
                    fi
                done
                "${create[@]}" >/dev/null || return $?
                sleep 4
                return 124
                ;;
            ping-sample-stall)
                # The sample outlasts its bound: four hundred packets at the same interval.
                local after_separator=false
                for i in "${!arguments[@]}"; do
                    if [[ "${after_separator}" == true && "${arguments[i]}" == -c ]]; then
                        arguments[i + 1]=400
                    fi
                    [[ "${arguments[i]}" != -- ]] || after_separator=true
                done
                ;;
            ping-exit | ping-logs-failure | ping-removal-failure)
                # The helper reports without sampling, and ping-exit with a status ping never uses.
                local helper_status=0
                [[ "${test_case}" != ping-exit ]] || helper_status=3
                for i in "${!arguments[@]}"; do
                    arguments[i]="${arguments[i]//'ping "$@" || status=$?'/"status=${helper_status}"}"
                done
                ;;
        esac
    fi
    if [[ "${test_case}" == ping-logs-failure && "$1 $2" == 'docker logs' ]]; then
        printf 'deliberate ping helper log failure\n' >&2
        return 125
    fi
    if [[ "${test_case}" == ping-removal-failure && "$1 $2 $3" == 'docker container rm' ]]; then
        printf 'deliberate ping helper removal failure\n' >&2
        return 125
    fi
    if [[ "$1 $2" == 'docker wait' ]]; then
        case "${test_case}" in
            ping-slow-start) limit=8 ;;
            ping-sample-stall) limit=4 ;;
        esac
    fi
    if [[ "${test_case}" == unreachable && "$1 $2" == 'docker run' \
        && " ${arguments[*]} " != *' --detach '* ]] \
        || [[ "${test_case}" == unreachable && "$1 $2" == 'docker wait' ]]; then
        limit=4
    fi
    if [[ "${test_case}" == partial ]]; then
        for i in "${!arguments[@]}"; do
            arguments[i]="${arguments[i]//count=256/count=128}"
        done
    fi
    if [[ "${test_case}" == start-failure && "$1 $2" == 'docker run' \
        && " ${arguments[*]} " == *' --detach '* ]]; then
        printf 'deliberate receiver launch failure\n' >&2
        return 125
    fi
    if [[ "${test_case}" == not-listening || "${test_case}" == receiver-failure \
        || "${test_case}" == delayed-receiver ]]; then
        for i in "${!arguments[@]}"; do
            local receive_command='received_bytes="$(nc -l -p 18081 | wc -c)"'
            if [[ "${arguments[i]}" == *"${receive_command}"* ]]; then
                local replacement
                if [[ "${test_case}" == not-listening ]]; then
                    replacement='sleep 60; received_bytes=0'
                elif [[ "${test_case}" == delayed-receiver ]]; then
                    replacement='received_bytes="$(nc -l -p 18081 | { dd bs=1 count=1 2>/dev/null; sleep 12; cat; } | wc -c)"'
                else
                    arguments[i]+=$'\nif [ "$1" = receiver ]; then exit 7; fi'
                    continue
                fi
                arguments[i]="${arguments[i]//"${receive_command}"/"${replacement}"}"
            fi
        done
    fi
    if [[ "${test_case}" == invalid-clock ]]; then
        for i in "${!arguments[@]}"; do
            arguments[i]="${arguments[i]//'"$(uptime_ms)"'/'0'}"
        done
    fi
    if [[ "${test_case}" == clock-coordinate-mismatch || "${test_case}" == missing-clock ]]; then
        for i in "${!arguments[@]}"; do
            if [[ "${test_case}" == missing-clock ]]; then
                arguments[i]="${arguments[i]//started_uptime_ms/unreported_start}"
            else
                arguments[i]="${arguments[i]/'case "$1" in'/'if [ "$1" = receiver ]; then boottime_offset="1 0"; fi; case "$1" in'}"
            fi
        done
    fi
    if [[ "${test_case}" == log-failure && "$1 $2" == 'docker logs' ]]; then
        printf 'deliberate receiver log failure\n' >&2
        return 125
    fi
    if [[ "${test_case}" == cleanup-failure && "$1 $2 $3" == 'docker container rm' ]]; then
        printf 'deliberate receiver cleanup failure\n' >&2
        return 125
    fi
    timeout --foreground --kill-after=5s "${limit}s" "${arguments[@]}" || status=$?
    return "${status}"
}
netem() {
    docker run --rm --label "io.nervix.chaos.run=${run_id}" \
        --network "container:${test_sender_id}" --cap-add NET_ADMIN \
        --entrypoint tc "${chaos_nettools_image}" "$@"
}

# A delay in the Docker control plane is not a slower directed link. The measured transfer keeps
# its original byte count and total deadline while excluding time before its sender begins.
test_case=launcher-delay
link_rate_probe "${tmp_dir}/launcher-delay.json" sender receiver
jq -e '.received_bytes == 262144 and .duration_ms < 6000' \
    "${tmp_dir}/launcher-delay.json" >/dev/null \
    || fail 'Docker launch delay was counted as directed-link transfer time'
jq -e '.helper_lifetime_ms >= 6000 and .sender_clock_resolution_ms == 10
    and .receiver_clock_resolution_ms == 10' "${tmp_dir}/launcher-delay.probe.json" >/dev/null \
    || fail 'control overhead or transfer-clock precision was lost'
test_case=delayed-connect

# A SYN crosses a two-second egress delay. The transfer must finish through the same owner despite
# that connect stall, with exact receiver bytes and both helper outcomes retained.
netem qdisc add dev eth0 root netem delay 2000ms
link_rate_probe "${tmp_dir}/delayed-connect.json" sender receiver
jq -e '.bytes == 262144 and .received_bytes == 262144 and .duration_ms >= 2000' \
    "${tmp_dir}/delayed-connect.json" >/dev/null || fail 'delayed connect lost or mismeasured bytes'
jq -e '.sender_status == 0 and .receiver_wait_status == 0 and .receiver_exit_code == 0
    and .sender == "sender" and .receiver == "receiver" and .port == 18081' \
    "${tmp_dir}/delayed-connect.probe.json" >/dev/null || fail 'successful helper evidence is incomplete'
netem qdisc del dev eth0 root

# The sender's success does not complete the receiver helper. Its complete byte count and final
# outcome must still be collected when completion takes longer than ten seconds inside the budget.
test_case=delayed-receiver
link_rate_probe "${tmp_dir}/delayed-receiver.json" sender receiver
jq -e '.received_bytes == 262144 and .duration_ms >= 12000' \
    "${tmp_dir}/delayed-receiver.json" >/dev/null || fail 'receiver completion was abandoned'
jq -e '.sender_status == 0 and .receiver_wait_status == 0 and .receiver_exit_code == 0' \
    "${tmp_dir}/delayed-receiver.probe.json" >/dev/null || fail 'receiver completion lost its outcome'

test_case=partial
if (set -e; link_rate_probe "${tmp_dir}/partial.json" sender receiver) \
    >"${tmp_dir}/partial.failure.txt" 2>&1; then
    fail 'a partial transfer passed'
fi
jq -e '.received_bytes == 131072 and .sender_status == 0 and .receiver_exit_code == 0' \
    "${tmp_dir}/partial.probe.json" >/dev/null || fail 'partial receiver evidence was lost'
grep -q '131072' "${tmp_dir}/partial.server.log" || fail 'raw partial byte count was lost'

# A listening receiver behind complete loss cannot be reached. It must fail at the total budget,
# retain the endpoint and statuses, and remove its sender and receiver helpers.
test_case=unreachable
netem qdisc add dev eth0 root netem loss 100%
if (set -e; link_rate_probe "${tmp_dir}/unreachable.json" sender receiver) \
    >"${tmp_dir}/unreachable.failure.txt" 2>&1; then
    fail 'an unreachable receiver passed'
fi
jq -e '(.sender_status == 124 or .sender_status == 137)
    and .sender == "sender" and .receiver == "receiver"
    and .transfer_deadline_seconds == 90' "${tmp_dir}/unreachable.probe.json" >/dev/null \
    || fail 'deadline failure lost its endpoints or outcomes'
grep -q 'rate probe.*sender.*receiver.*18081' "${tmp_dir}/unreachable.failure.txt" \
    || fail 'deadline failure did not name the probe'
netem qdisc del dev eth0 root
for test_case in start-failure not-listening receiver-failure log-failure cleanup-failure \
    invalid-clock missing-clock clock-coordinate-mismatch; do
    if (set -e; link_rate_probe "${tmp_dir}/${test_case}.json" sender receiver) \
        >"${tmp_dir}/${test_case}.failure.txt" 2>&1; then
        fail "${test_case} passed"
    fi
done
grep -q 'invalid helper transfer clocks' "${tmp_dir}/invalid-clock.failure.txt" \
    || fail 'an invalid transfer clock lost its diagnostic'
for test_case in missing-clock clock-coordinate-mismatch; do
    grep -q 'invalid helper transfer clocks' "${tmp_dir}/${test_case}.failure.txt" \
        || fail "${test_case} lost its diagnostic"
done
jq -e '.receiver_start_status == 125 and .receiver_ready == false and .sender_status == null' \
    "${tmp_dir}/start-failure.probe.json" >/dev/null || fail 'receiver launch failure lost its outcome'
grep -q 'deliberate receiver launch failure' "${tmp_dir}/start-failure.server-start.log" \
    || fail 'receiver launch failure lost its log'
jq -e '.receiver_ready == false and .sender_status == null and .receiver_cleanup_status == 0' \
    "${tmp_dir}/not-listening.probe.json" >/dev/null || fail 'listener failure lost its outcome'
jq -e '.received_bytes == 262144 and .receiver_wait_status == 0 and .receiver_exit_code == 7' \
    "${tmp_dir}/receiver-failure.probe.json" >/dev/null || fail 'receiver exit failure lost its outcome'
jq -e '.receiver_log_status == 125 and .received_bytes == null and .receiver_cleanup_status == 0' \
    "${tmp_dir}/log-failure.probe.json" >/dev/null || fail 'receiver log failure passed or invented a byte count'
grep -q 'deliberate receiver log failure' "${tmp_dir}/log-failure.server.stderr.log" \
    || fail 'receiver log failure lost its diagnostic'
jq -e '.received_bytes == 262144 and .receiver_cleanup_status == 125' \
    "${tmp_dir}/cleanup-failure.probe.json" >/dev/null || fail 'receiver cleanup failure lost its outcome'
grep -q 'deliberate receiver cleanup failure' "${tmp_dir}/cleanup-failure.server-cleanup.log" \
    || fail 'receiver cleanup failure lost its diagnostic'
# A cleanup failure must retain the exact helper identity for the run's cleanup owner.
retained_helper="$(jq -r .receiver_helper_id "${tmp_dir}/cleanup-failure.probe.json")"
docker inspect "${retained_helper}" >/dev/null || fail 'cleanup failure lost the retained helper'
timeout --foreground --kill-after=5s 20s docker container rm --force "${retained_helper}" >/dev/null

# Docker's start of a ping helper is control time with its own bound: a helper Docker takes six
# seconds to start still completes a sample whose wait allows only eight. A start that outlasts
# the launch bound and a sample that outlasts its own bound each fail with the stage that took the
# time, and keep Docker's own record of the helper before removing it.
test_case=ping-slow-start
link_ping "${tmp_dir}/ping-slow-start.txt" sender receiver baseline
jq -e '.launch_status == 0 and .wait_status == 0 and .helper_exit_code == 0 and .ping_status == 0
    and .shared_clock and .launch_ms >= 6000 and .sample_ms >= 3800 and .sample_ms < 8000
    and .logs_status == 0 and .removal_status == 0 and .docker_state == null' \
    "${tmp_dir}/ping-slow-start.probe.json" >/dev/null \
    || fail 'a slow helper start was counted against its sample'
jq -e '.sent == 40' "${tmp_dir}/ping-slow-start.json" >/dev/null \
    || fail 'a slow helper start lost its sample'
for test_case in ping-launch-stall ping-sample-stall; do
    if (set -e; link_ping "${tmp_dir}/${test_case}.txt" sender receiver baseline) \
        >"${tmp_dir}/${test_case}.failure.txt" 2>&1; then
        fail "${test_case} passed"
    fi
done
grep -q 'Docker did not start it within its 60 s launch bound (launch exited 124); Docker recorded no start of it' \
    "${tmp_dir}/ping-launch-stall.failure.txt" || fail 'a stalled helper start lost its stage'
jq -e '.launch_status == 124 and .wait_status == null and .sample_started_uptime_ms == null
    and .docker_state.status == "created" and .removal_status == 0
    and any(.docker_events[]; .action == "create") and any(.docker_events[]; .action == "destroy")
    and .sender == "sender" and .receiver == "receiver" and .launch_seconds == 60
    and .sample_seconds == 30' "${tmp_dir}/ping-launch-stall.probe.json" >/dev/null \
    || fail 'a stalled helper start lost Docker state, events or removal'
grep -q 'its sample did not end within its 30 s bound (wait exited 124); the sample began [0-9.]* s after the launch' \
    "${tmp_dir}/ping-sample-stall.failure.txt" || fail 'a stalled sample lost its stage'
jq -e '.launch_status == 0 and .wait_status == 124 and .shared_clock and .launch_ms >= 0
    and .sample_finished_uptime_ms == null and .docker_state.status == "running"
    and .removal_status == 0 and any(.docker_events[]; .action == "start")' \
    "${tmp_dir}/ping-sample-stall.probe.json" >/dev/null \
    || fail 'a stalled sample lost its start, Docker state or removal'
for test_case in ping-launch-stall ping-sample-stall; do
    helper="$(cat "${tmp_dir}/${test_case}.helper-id.txt")"
    if timeout --foreground --kill-after=5s 20s docker inspect "${helper}" >/dev/null 2>&1; then
        fail "${test_case} retained its helper"
    fi
done

# Docker's refusal of a launch, ping's own exit, a failed read of the helper's output and a failed
# removal each name their stage.
test_case=ping-launch-refusal
if (set -e; link_ping "${tmp_dir}/${test_case}.txt" stopped receiver baseline) \
    >"${tmp_dir}/${test_case}.failure.txt" 2>&1; then
    fail "${test_case} passed"
fi
grep -q ': Docker refused its launch (launch exited 125: .*non running container.*); see ' \
    "${tmp_dir}/ping-launch-refusal.failure.txt" || fail 'a refused helper launch lost its stage'
jq -e '.launch_status == 125 and .wait_status == null and .docker_state.status == "created"
    and .removal_status == 0 and any(.docker_events[]; .action == "destroy")' \
    "${tmp_dir}/ping-launch-refusal.probe.json" >/dev/null \
    || fail 'a refused helper launch lost Docker state, events or removal'
for test_case in ping-exit ping-logs-failure ping-removal-failure; do
    if (set -e; link_ping "${tmp_dir}/${test_case}.txt" sender receiver baseline) \
        >"${tmp_dir}/${test_case}.failure.txt" 2>&1; then
        fail "${test_case} passed"
    fi
done
grep -q ': ping exited 3; see ' "${tmp_dir}/ping-exit.failure.txt" \
    || fail 'a failed ping lost its exit status'
jq -e '.wait_status == 0 and .helper_exit_code == 3 and .ping_status == 3 and .logs_status == 0
    and .removal_status == 0' "${tmp_dir}/ping-exit.probe.json" >/dev/null \
    || fail 'a failed ping lost its outcome'
grep -q ': its output could not be read (logs exited 125); see ' \
    "${tmp_dir}/ping-logs-failure.failure.txt" || fail 'a failed helper log read lost its stage'
grep -q 'deliberate ping helper log failure' "${tmp_dir}/ping-logs-failure.helper.log" \
    || fail 'a failed helper log read lost its diagnostic'
jq -e '.helper_exit_code == 0 and .logs_status == 125 and .removal_status == 0' \
    "${tmp_dir}/ping-logs-failure.probe.json" >/dev/null || fail 'a failed helper log read lost its outcome'
grep -q ': it could not be removed (removal exited 125); see ' \
    "${tmp_dir}/ping-removal-failure.failure.txt" || fail 'a failed helper removal lost its stage'
grep -q 'deliberate ping helper removal failure' "${tmp_dir}/ping-removal-failure.helper-removal.log" \
    || fail 'a failed helper removal lost its diagnostic'
jq -e '.helper_exit_code == 0 and .logs_status == 0 and .removal_status == 125' \
    "${tmp_dir}/ping-removal-failure.probe.json" >/dev/null || fail 'a failed helper removal lost its outcome'
# A failed removal keeps the exact helper identity for the run's cleanup owner.
retained_helper="$(jq -r .helper_id "${tmp_dir}/ping-removal-failure.probe.json")"
docker inspect "${retained_helper}" >/dev/null || fail 'a failed removal lost the retained helper'
timeout --foreground --kill-after=5s 20s docker container rm --force "${retained_helper}" >/dev/null

# No earlier control leaves a helper in either namespace while the effect is measured.
[[ -z "$(docker ps -aq --filter "label=io.nervix.chaos.run=${run_id}" \
    --filter 'label=io.nervix.chaos.role=link-probe')" ]] \
    || fail 'an earlier control retained a helper'

# Qualify the existing twofold effect against real healthy and 256kbit directed transfers. Each
# successful sample keeps its launch and sample times on the controller's clock.
test_case=rate-effect
link_ping "${tmp_dir}/healthy-ping.txt" sender receiver baseline
link_rate_probe "${tmp_dir}/healthy-rate.json" sender receiver
netem qdisc add dev eth0 root netem rate 256kbit
link_ping "${tmp_dir}/capped-ping.txt" sender receiver rate-limit
link_rate_probe "${tmp_dir}/capped-rate.json" sender receiver
netem qdisc del dev eth0 root
for sample in healthy-ping capped-ping; do
    jq -e '.launch_status == 0 and .wait_status == 0 and .helper_exit_code == 0 and .ping_status == 0
        and .shared_clock and .launch_ms >= 0 and .sample_ms >= 3800 and .logs_status == 0
        and .removal_status == 0 and .docker_state == null' \
        "${tmp_dir}/${sample}.probe.json" >/dev/null || fail "${sample} lost its helper timings"
done
"${chaos_dir}/verify-degraded-evidence.sh" effect rate-limit \
    "${tmp_dir}/healthy-ping.json" "${tmp_dir}/capped-ping.json" \
    "${tmp_dir}/healthy-rate.json" "${tmp_dir}/capped-rate.json" >/dev/null \
    || fail 'the real directed rate limit did not meet the unchanged effect contract'
[[ -z "$(docker ps -aq --filter "label=io.nervix.chaos.run=${run_id}" \
    --filter 'label=io.nervix.chaos.role=link-probe')" ]] \
    || fail 'a failed probe retained a helper'
printf 'rate probe self-test passed\n'
