#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../pause-resume-scenario.sh
source "${script_dir}/pause-resume-scenario.sh"

fail() {
    printf 'role wait self-test failed: %s\n' "$*" >&2
    exit 1
}

wait_for() {
    local condition="$1"
    local bound="$2"
    shift 2
    [[ "${bound}" == 30 && "${condition}" == *'settled public role'* ]] \
        || fail "unexpected pre-fault wait: ${condition} (${bound}s)"
    local attempt
    for attempt in 1 2 3; do
        if "$@"; then
            return 0
        fi
    done
    printf 'timed out waiting for %s\n' "${condition}" >&2
    return 124
}

observation_count=0
observe_crash_target() {
    observation_count=$((observation_count + 1))
    ((observation_count > 1)) || return 1
    target_node=node-1
}
confirm_crash_target_before_fault node-1 /tmp/chaos-crash-role-wait \
    || fail 'crash did not retry a transient public read'
[[ "${observation_count}" -eq 2 ]] || fail 'crash did not stop after a settled read'

observe_crash_target() {
    target_node=node-2
}
failure_category=product
status=0
confirm_crash_target_before_fault node-1 /tmp/chaos-crash-role-moved 2>/dev/null || status=$?
[[ "${status}" -eq 1 && "${failure_category}" == injection ]] \
    || fail 'crash accepted a moved role or misclassified it'

observe_crash_target() {
    return 1
}
failure_category=product
status=0
message="$(confirm_crash_target_before_fault node-1 /tmp/chaos-crash-role-timeout 2>&1)" || status=$?
[[ "${status}" -eq 1 && "${message}" == *'settled public role'* ]] \
    || fail 'crash timeout did not name the unmet condition'

observation_count=0
select_pause_target() {
    observation_count=$((observation_count + 1))
    ((observation_count > 1)) || return 1
    target_node=node-2
    target_kind=relay
}
confirm_pause_target_before_fault execution node-2 relay /tmp/chaos-pause-role-wait \
    || fail 'pause did not retry a transient public read'
[[ "${observation_count}" -eq 2 ]] || fail 'pause did not stop after a settled read'

select_pause_target() {
    target_node=node-3
    target_kind=relay
}
failure_category=product
status=0
confirm_pause_target_before_fault execution node-2 relay /tmp/chaos-pause-role-moved 2>/dev/null || status=$?
[[ "${status}" -eq 1 && "${failure_category}" == injection ]] \
    || fail 'pause accepted a moved role or misclassified it'

select_pause_target() {
    return 1
}
status=0
message="$(confirm_pause_target_before_fault execution node-2 relay /tmp/chaos-pause-role-timeout 2>&1)" || status=$?
[[ "${status}" -eq 1 && "${message}" == *'settled public role'* ]] \
    || fail 'pause timeout did not name the unmet condition'

printf 'role wait self-test passed\n'
