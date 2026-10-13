#!/bin/sh
# shellcheck shell=bash
# Runs inside the pinned Alpine helper. Owns transfer clocks and exact receiver counts, depends
# on the Docker host's shared kernel uptime clock, and knows no product state or Docker API.
set -eu
set -o pipefail
clock_namespace="$(readlink /proc/self/ns/time)"
boot_id="$(cat /proc/sys/kernel/random/boot_id)"
boottime_offset="$(awk '$1 == "boottime" {printf "%s %s", $2, $3}' /proc/self/timens_offsets)"

uptime_ms() {
    awk '{printf "%.0f\n", $1 * 1000}' /proc/uptime
}

case "$1" in
    sender)
        printf '{"started_uptime_ms":%s,"clock_resolution_ms":10,"clock_namespace":"%s","boot_id":"%s","boottime_offset":"%s"}\n' \
            "$(uptime_ms)" "${clock_namespace}" "${boot_id}" "${boottime_offset}"
        dd if=/dev/zero bs=1024 count=256 2>/dev/null | nc "$2" 18081
        ;;
    receiver)
        received_bytes="$(nc -l -p 18081 | wc -c)"
        printf '{"received_bytes":%s,"finished_uptime_ms":%s,"clock_resolution_ms":10,"clock_namespace":"%s","boot_id":"%s","boottime_offset":"%s"}\n' \
            "${received_bytes}" "$(uptime_ms)" "${clock_namespace}" "${boot_id}" "${boottime_offset}"
        ;;
    *) printf 'rate helper needs sender or receiver\n' >&2; exit 2 ;;
esac
