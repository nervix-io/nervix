#!/bin/sh
set -eu

# Produces one fixture record per interval to the load's topic until its stop file appears. A load
# with a hold modulus waits, before the record whose count of records already produced leaves that
# remainder, for as long as its hold file exists, so the source ends a hold at a known boundary.
#
# One kcat process reads a pipe in blocks and sends what it has read only when a block fills or the
# input ends, so records leave in bursts. A load with CHAOS_LOAD_EACH=1 runs one kcat per record
# instead, which delivers each record before the next is read and leaves none waiting in a hold.
topic="${CHAOS_LOAD_TOPIC:-chaos_input}"
stop_file="/opt/chaos/traffic/${CHAOS_LOAD_STOP_FILE:-stop-load}"
hold_file="/opt/chaos/traffic/${CHAOS_LOAD_HOLD_FILE:-hold-load}"
hold_modulus="${CHAOS_LOAD_HOLD_MODULUS:-0}"
hold_remainder="${CHAOS_LOAD_HOLD_REMAINDER:-0}"
each="${CHAOS_LOAD_EACH:-0}"

records() {
    produced=0
    while IFS= read -r record; do
        if [ -e "${stop_file}" ]; then
            break
        fi
        if [ "${hold_modulus}" -gt 0 ] && [ $((produced % hold_modulus)) -eq "${hold_remainder}" ]; then
            while [ -e "${hold_file}" ] && [ ! -e "${stop_file}" ]; do
                sleep 0.2
            done
            if [ -e "${stop_file}" ]; then
                break
            fi
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
