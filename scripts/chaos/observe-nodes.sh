#!/bin/sh
set -eu

while :; do
    number=1
    while [ "${number}" -le "${CHAOS_NODE_COUNT}" ]; do
        host="nervix-${number}"
        observed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        if nc -z -w 2 "${host}" 47391 \
            && nc -z -w 2 "${host}" 47395 \
            && wget -q -T 3 -O /dev/null "http://${host}:9090/livez" \
            && wget -q -T 3 -O /dev/null "http://${host}:9090/readyz" \
            && wget -q -T 3 -O /dev/null "http://${host}:9090/metrics" \
            && wget -q -T 3 -O /dev/null "http://${host}:47420/console/"; then
            printf '%s %s ready\n' "${observed_at}" "${host}"
        else
            printf '%s %s unavailable\n' "${observed_at}" "${host}"
        fi
        number=$((number + 1))
    done
    sleep 2
done
