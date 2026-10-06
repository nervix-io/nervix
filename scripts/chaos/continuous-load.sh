#!/bin/sh
set -eu

# Produces one fixture record per interval to the load's topic until its stop file appears. While
# the load's hold file exists and names a count of records the load has already produced, the load
# waits, so the source stops at a known boundary; the runner moves the boundary by rewriting the
# file and releases the load by removing it. An empty hold file holds the load where it stands.
#
# One kcat process reads a pipe in blocks and sends what it has read only when a block fills or the
# input ends, so records leave in bursts. A load with CHAOS_LOAD_EACH=1 runs one kcat per record
# instead, which delivers each record before the next is read and leaves none waiting in a hold.
topic="${CHAOS_LOAD_TOPIC:-chaos_input}"
stop_file="/opt/chaos/traffic/${CHAOS_LOAD_STOP_FILE:-stop-load}"
hold_file="/opt/chaos/traffic/${CHAOS_LOAD_HOLD_FILE:-hold-load}"
each="${CHAOS_LOAD_EACH:-0}"

# True while the hold file exists and names a count no larger than the records already produced.
held() {
    [ -e "${hold_file}" ] || return 1
    boundary=""
    read -r boundary <"${hold_file}" 2>/dev/null || true
    [ -z "${boundary}" ] || [ "${produced}" -ge "${boundary}" ]
}

records() {
    produced=0
    while IFS= read -r record; do
        while held && [ ! -e "${stop_file}" ]; do
            sleep 0.2
        done
        if [ -e "${stop_file}" ]; then
            break
        fi
        if [ "${each}" = 1 ]; then
            printf '%s\n' "${record}" | kcat -b broker:9092 -P -t "${topic}"
        else
            printf '%s\n' "${record}"
        fi
        produced=$((produced + 1))
        sleep "${CHAOS_LOAD_INTERVAL}"
    done </opt/chaos/input.ndjson
}

if [ "${each}" = 1 ]; then
    records
else
    records | kcat -b broker:9092 -P -t "${topic}"
fi
