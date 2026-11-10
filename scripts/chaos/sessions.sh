#!/usr/bin/env bash
# Sourced by the chaos runner, the suite and the verifier self-check. A controller starts a run or
# its verifier self-check as the leader of a session of its own, records the leader's number and
# start time, and later ends whatever of that session remains. The start time tells the leader
# apart from any later process given its number.

# Prints the state of process PID and when it started, in clock ticks after boot, or nothing when no
# process has that number. The start time tells a process apart from any later one given its number.
process_stat() {
    local pid="$1"
    [[ "${pid}" =~ ^[1-9][0-9]*$ ]] || return 0
    local stat=""
    stat="$(cat "/proc/${pid}/stat" 2>/dev/null)" || return 0
    # The command name in the second field may hold spaces and parentheses, so the fields are read
    # after its closing parenthesis: the state first and the start time twentieth.
    local fields=()
    read -r -a fields <<<"${stat##*) }"
    printf '%s %s' "${fields[0]:-}" "${fields[19]:-}"
}

# Prints when process PID started, as process_executes compares it, or nothing when no process has
# that number.
process_start_ticks() {
    local stat
    stat="$(process_stat "$1")"
    if [[ -n "${stat}" ]]; then
        printf '%s' "${stat#* }"
    fi
}

# True when process PID executes, not as a zombie, and is the process that started at START_TICKS.
process_executes() {
    local pid="$1"
    local start_ticks="$2"
    [[ -n "${start_ticks}" ]] || return 1
    local stat
    stat="$(process_stat "${pid}")"
    [[ -n "${stat}" && "${stat% *}" != Z && "${stat#* }" == "${start_ticks}" ]]
}

# Kills what remains of the session that a run or verifier self-check led as process PID, started
# at START_TICKS, and prints how many processes it killed. A session's number stays reserved while
# any process belongs to it, so once its leader is gone every process left in the session is the
# session's own; a leader number that now names another process means nothing of the session
# remains.
kill_session() {
    local pid="$1"
    local start_ticks="$2"
    local current_start
    current_start="$(process_start_ticks "${pid}")"
    if [[ -n "${current_start}" && "${current_start}" != "${start_ticks}" ]]; then
        printf '0'
        return 0
    fi
    local -A killed=()
    local round member state
    for ((round = 0; round < 10; round++)); do
        local members=()
        while read -r member state; do
            if [[ -n "${member}" && "${state}" != Z* ]]; then
                members+=("${member}")
            fi
        done < <(ps -o pid=,stat= -s "${pid}" 2>/dev/null || true)
        ((${#members[@]} > 0)) || break
        kill -KILL "${members[@]}" 2>/dev/null || true
        for member in "${members[@]}"; do
            killed["${member}"]=1
        done
        sleep 0.2
    done
    printf '%d' "${#killed[@]}"
}
