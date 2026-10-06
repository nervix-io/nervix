#!/bin/sh
set -eu

# Produces one fixture record per interval to the load's topic until its stop file appears or the
# fixture ends.
#
# Every record leaves through its own kcat call. kcat 1.7.1 reads its standard input in
# 1,024-byte blocks and produces a record only once the block that holds its newline is full or
# the input has ended, so one kcat fed a paced stream delivers the records in bursts of a block.
# A call that has returned has also delivered its record, so none waits inside the producer.
#
# Records are due one interval apart on the kernel's uptime clock. A call that returns within
# half an interval of its record's due time keeps that schedule, so the time a call takes does not
# stretch the interval. A call that returns later restarts the schedule from its return: the next
# record leaves a whole interval after it, and the load never catches up with a burst.
#
# While the load's hold file exists and names a count of records the load has already produced,
# the load waits, so the source stops at a known boundary; the runner moves the boundary by
# rewriting the file and releases the load by removing it. An empty hold file holds the load where
# it stands.
topic="${CHAOS_LOAD_TOPIC:-chaos_input}"
stop_file="/opt/chaos/traffic/${CHAOS_LOAD_STOP_FILE:-stop-load}"
hold_file="/opt/chaos/traffic/${CHAOS_LOAD_HOLD_FILE:-hold-load}"
interval_ms="${CHAOS_LOAD_INTERVAL_MS}"
catch_up_ms=$((interval_ms / 2))

# Reads the uptime clock in milliseconds. /proc/uptime reports hundredths of a second, and reading
# it starts no process.
read_clock() {
    IFS=' .' read -r clock_seconds clock_hundredths _ </proc/uptime
    clock_ms=$((clock_seconds * 1000 + ${clock_hundredths#0} * 10))
}

# Sleeps a whole number of milliseconds.
sleep_ms() {
    millis=$(($1 % 1000))
    case "${#millis}" in
        1) millis="00${millis}" ;;
        2) millis="0${millis}" ;;
    esac
    sleep "$(($1 / 1000)).${millis}"
}

# True while the hold file exists and names a count no larger than the records already produced.
held() {
    [ -e "${hold_file}" ] || return 1
    boundary=""
    read -r boundary <"${hold_file}" 2>/dev/null || true
    [ -z "${boundary}" ] || [ "${produced}" -ge "${boundary}" ]
}

produced=0
read_clock
due_ms="${clock_ms}"
while IFS= read -r record; do
    read_clock
    if [ "${clock_ms}" -lt "${due_ms}" ]; then
        sleep_ms "$((due_ms - clock_ms))"
    fi
    while held && [ ! -e "${stop_file}" ]; do
        sleep_ms 200
    done
    if [ -e "${stop_file}" ]; then
        break
    fi
    if ! printf '%s\n' "${record}" | kcat -b broker:9092 -P -t "${topic}"; then
        printf 'load failure: kcat did not deliver record %d to %s\n' "$((produced + 1))" "${topic}" >&2
        exit 1
    fi
    produced=$((produced + 1))
    read_clock
    if [ "$((clock_ms - due_ms))" -le "${catch_up_ms}" ]; then
        due_ms=$((due_ms + interval_ms))
    else
        due_ms=$((clock_ms + interval_ms))
    fi
done </opt/chaos/input.ndjson
