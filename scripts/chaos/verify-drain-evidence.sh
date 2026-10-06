#!/usr/bin/env bash
# Verifies that a node stopped gracefully while a live replacement node existed handed its scheduled
# work over before it exited: the drain-support phase in its shutdown log completed. A node whose
# drain the leader refused or never answered completes only its local work, and the rest of its
# work fails over once it is gone, which is a crash-like outcome rather than a graceful one.
set -euo pipefail

if [[ "$#" -ne 2 ]]; then
    printf 'usage: %s SERVICE SHUTDOWN_LOG\n' "$0" >&2
    exit 2
fi

service="$1"
shutdown_log="$2"

if [[ ! -f "${shutdown_log}" ]]; then
    printf 'shutdown log %s does not exist\n' "${shutdown_log}" >&2
    exit 2
fi

phase_records="$(grep -F 'shutdown drain-support phase finished' "${shutdown_log}" || true)"
if [[ -z "${phase_records}" ]]; then
    printf '%s did not finish shutdown phase drain-support\n' "${service}" >&2
    exit 1
fi
if [[ "$(grep -c . <<<"${phase_records}")" -ne 1 ]]; then
    printf '%s reported more than one drain-support phase in one stop:\n%s\n' \
        "${service}" "${phase_records}" >&2
    exit 1
fi
if ! grep -Fq 'shutdown drain-support phase finished outcome=Completed' <<<"${phase_records}"; then
    # The drain records say why: a refused or failed move, a leader that never answered, or a
    # local drain that ran out of time.
    drain_records="$(grep -E 'graceful shutdown|shutdown drain cordon|local graph drain' \
        "${shutdown_log}" || true)"
    printf '%s stopped while a live replacement node existed but did not complete its drain: %s\n%s\n' \
        "${service}" "${phase_records}" "${drain_records}" >&2
    exit 1
fi
