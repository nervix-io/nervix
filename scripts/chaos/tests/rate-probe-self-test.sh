#!/usr/bin/env bash
set -euo pipefail

chaos_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../tool-images.sh
source "${chaos_dir}/tool-images.sh"
# shellcheck source=../link-degradation.sh
source "${chaos_dir}/link-degradation.sh"
CHAOS_PROBE_IMAGE="${chaos_probe_image}"
scenario=rate-probe-self-test
run_id="rate-probe-check-$$-${RANDOM}"
tmp_dir="${CHAOS_RATE_PROBE_ARTIFACTS:-$(mktemp -d)}"
mkdir -p "${tmp_dir}"

cleanup() {
    "${chaos_dir}/cleanup.sh" --run-id "${run_id}" --quiet >/dev/null 2>&1 || true
    if [[ -z "${CHAOS_RATE_PROBE_ARTIFACTS:-}" ]]; then
        rm -rf "${tmp_dir}"
    fi
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
test_sender_id="$(docker run --detach --label "io.nervix.chaos.run=${run_id}" \
    --network "${run_id}" "${CHAOS_PROBE_IMAGE}" sleep 300)"
test_receiver_id="$(docker run --detach --label "io.nervix.chaos.run=${run_id}" \
    --network "${run_id}" "${CHAOS_PROBE_IMAGE}" sleep 300)"

owned_service_container() {
    case "$1" in
        sender) printf '%s\n' "${test_sender_id}" ;;
        receiver) printf '%s\n' "${test_receiver_id}" ;;
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

# Invoke the production Docker commands. Only the deliberate unreachable control shortens its
# total deadline; the partial-payload control changes the producer's input, not the probe owner.
test_case=delayed-connect
sender_failed=false
run_bounded() {
    local limit="$1"
    shift
    local arguments=("$@") i status=0
    if [[ "${test_case}" == unreachable && "$1 $2" == 'docker run' \
        && " ${arguments[*]} " != *' --detach '* ]] \
        || [[ "${test_case}" == unreachable && "$1 $2" == 'docker wait' ]]; then
        limit=4
    elif [[ "${sender_failed}" == true && "$1 $2" == 'docker wait' ]]; then
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
            if [[ "${arguments[i]}" == 'nc -l -p 18081 | wc -c' ]]; then
                if [[ "${test_case}" == not-listening ]]; then
                    arguments[i]='sleep 60'
                elif [[ "${test_case}" == delayed-receiver ]]; then
                    arguments[i]='nc -l -p 18081 | { dd bs=1 count=1 2>/dev/null; sleep 12; cat; } | wc -c'
                else
                    arguments[i]+='; exit 7'
                fi
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
    if [[ "$1 $2" == 'docker run' && " ${arguments[*]} " != *' --detach '* \
        && "${status}" -ne 0 ]]; then
        sender_failed=true
    fi
    return "${status}"
}
netem() {
    docker run --rm --label "io.nervix.chaos.run=${run_id}" \
        --network "container:${test_sender_id}" --cap-add NET_ADMIN \
        --entrypoint tc "${chaos_nettools_image}" "$@"
}

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
for test_case in start-failure not-listening receiver-failure log-failure cleanup-failure; do
    if (set -e; link_rate_probe "${tmp_dir}/${test_case}.json" sender receiver) \
        >"${tmp_dir}/${test_case}.failure.txt" 2>&1; then
        fail "${test_case} passed"
    fi
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
[[ -z "$(docker ps -aq --filter "label=io.nervix.chaos.run=${run_id}" \
    --filter 'label=io.nervix.chaos.role=link-probe')" ]] \
    || fail 'a failed probe retained a helper'
printf 'rate probe self-test passed\n'
