#!/usr/bin/env bash
# Verifies that a node stopped gracefully while a live replacement node existed handed its scheduled
# work over before it exited: the move phase in its shutdown log reports that every unit moved. A
# node whose moves the leader refused, never answered or did not finish within the shutdown timeout
# leaves its units saved on itself, and they stop until it restarts, which is a cluster disruption
# rather than a graceful handover.
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

phase_records="$(grep -F 'shutdown move phase finished' "${shutdown_log}" || true)"
if [[ -z "${phase_records}" ]]; then
    printf '%s did not finish shutdown phase move\n' "${service}" >&2
    exit 1
fi
if [[ "$(grep -c . <<<"${phase_records}")" -ne 1 ]]; then
    printf '%s reported more than one move phase in one stop:\n%s\n' \
        "${service}" "${phase_records}" >&2
    exit 1
fi
if ! grep -Fq 'shutdown move phase finished outcome=Moved' <<<"${phase_records}"; then
    # The move records say why: a refused or failed move, a leader that never answered, or a
    # shutdown timeout that passed.
    move_records="$(grep -E 'graceful shutdown|shutdown drain cordon|cluster disruption|units' \
        "${shutdown_log}" || true)"
    printf '%s stopped while a live replacement node existed but did not move all of its units: %s\n%s\n' \
        "${service}" "${phase_records}" "${move_records}" >&2
    exit 1
fi
