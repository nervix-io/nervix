#!/usr/bin/env bash
set -euo pipefail

chaos_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../backup-scenario.sh
source "${chaos_dir}/backup-scenario.sh"

fail() {
    printf 'backup fixture self-test failed: %s\n' "$*" >&2
    exit 1
}

# The run reads the checked-in baseline fixture and installs its backup form.
baseline_graph="$(<"${chaos_dir}/fixtures/baseline.nspl")"
backup_graph="$(backup_graph_fixture "${baseline_graph}")" \
    || fail 'the baseline fixture does not hold the one consumer-group offset clause the backup run replaces'

# Exactly one line changes: the Kafka ingestor's, whose offsets the domain now owns. Putting the
# consumer-group clause back restores the baseline line, so nothing else in it changed.
mapfile -t baseline_lines <<<"${baseline_graph}"
mapfile -t backup_lines <<<"${backup_graph}"
[[ "${#backup_lines[@]}" -eq "${#baseline_lines[@]}" ]] \
    || fail "the backup graph has ${#backup_lines[@]} lines, the baseline fixture ${#baseline_lines[@]}"
changed_lines=()
for index in "${!baseline_lines[@]}"; do
    if [[ "${backup_lines[index]}" != "${baseline_lines[index]}" ]]; then
        changed_lines+=("${index}")
    fi
done
[[ "${#changed_lines[@]}" -eq 1 ]] \
    || fail "the backup graph changes ${#changed_lines[@]} lines of the baseline fixture, expected 1"
backup_line="${backup_lines[changed_lines[0]]}"
baseline_line="${baseline_lines[changed_lines[0]]}"
[[ "${backup_line}" == *' OFFSET BY DOMAIN '* ]] \
    || fail "the changed line does not read domain-owned offsets: ${backup_line}"
consumer_group_clause="OFFSET BY CONSUMER GROUP 'chaos_baseline'"
restored_line="${backup_line/ OFFSET BY DOMAIN / ${consumer_group_clause} }"
[[ "${restored_line}" == "${baseline_line}" ]] \
    || fail "the changed line differs from the baseline beyond its offset clause: ${backup_line}"

# A graph that holds no consumer-group clause, or holds it twice, has no single clause to replace.
status=0
refused="$(backup_graph_fixture "${backup_graph}")" || status=$?
[[ "${status}" -ne 0 && -z "${refused}" ]] \
    || fail 'a graph without the consumer-group clause produced a backup graph'
status=0
refused="$(backup_graph_fixture "${baseline_graph}"$'\n'"${baseline_graph}")" || status=$?
[[ "${status}" -ne 0 && -z "${refused}" ]] \
    || fail 'a graph with two consumer-group clauses produced a backup graph'

printf 'backup fixture self-test passed\n'
