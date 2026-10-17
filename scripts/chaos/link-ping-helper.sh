#!/bin/sh
# shellcheck shell=bash
# Runs inside the pinned Alpine helper in the sending node's network namespace. Owns one ICMP
# sample: BusyBox ping's own output on stdout, and on stderr when the sample began and ended on the
# Docker host's shared kernel uptime clock, with ping's exit status. Knows no product state or
# Docker API.
set -u
uptime_ms() {
    awk '{printf "%.0f\n", $1 * 1000}' /proc/uptime
}
boot_id="$(cat /proc/sys/kernel/random/boot_id)"
boottime_offset="$(awk '$1 == "boottime" {printf "%s %s", $2, $3}' /proc/self/timens_offsets)"
printf '{"started_uptime_ms":%s,"clock_resolution_ms":10,"boot_id":"%s","boottime_offset":"%s"}\n' \
    "$(uptime_ms)" "${boot_id}" "${boottime_offset}" >&2
status=0
ping "$@" || status=$?
printf '{"finished_uptime_ms":%s,"ping_status":%s}\n' "$(uptime_ms)" "${status}" >&2
exit "${status}"
