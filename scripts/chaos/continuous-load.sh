#!/bin/sh
set -eu

(
    while IFS= read -r record; do
        if [ -e /opt/chaos/traffic/stop-load ]; then
            break
        fi
        printf '%s\n' "${record}"
        sleep "${CHAOS_LOAD_INTERVAL}"
    done </opt/chaos/input.ndjson
) | kcat -b broker:9092 -P -t chaos_input
