#!/usr/bin/env bash
# Exercise the production request functions without starting a cluster. A helper is a run-owned
# container, but each observation must remain a fresh, noninteractive public client process.
set -euo pipefail

chaos_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT
artifact_dir="${tmp_dir}/run"
mkdir -p "${artifact_dir}/public"
cli_host=nervix-2
export CHAOS_PASSWORD=fixture-password

awk '/^helper_request\(\)/,/^cli_command\(\)/ {if (!/^cli_command\(\)/) print}' \
    "${chaos_dir}/run-baseline.sh" >"${tmp_dir}/requests.sh"
awk '/^node_listener_probe=/,/^cluster_status_ready\(\)/ {if (!/^cluster_status_ready\(\)/) print}' \
    "${chaos_dir}/run-baseline.sh" >>"${tmp_dir}/requests.sh"
awk '/^capture_metrics\(\)/,/^assert_metric_total\(\)/ {if (!/^assert_metric_total\(\)/) print}' \
    "${chaos_dir}/run-baseline.sh" >>"${tmp_dir}/requests.sh"
# shellcheck source=/dev/null
source "${tmp_dir}/requests.sh"

fail() { printf 'observation helper self-test failed: %s\n' "$*" >&2; exit 1; }

request_status=0
request_output=observed
available_seconds=90
remaining_seconds() { printf '%s\n' "${available_seconds}"; }
compose() {
    # A request must neither create another helper nor attach caller input. Verify the input is
    # closed even when the caller has readable data; the endpoint and client arguments are kept.
    [[ "$1" == exec && "$2" == -T && "$3" == --interactive=false ]] || return 65
    [[ "$5" == timeout && "$6" == -k && "$7" == 5s && "$8" == "${available_seconds}s" ]] || return 67
    if read -r -t 1 _; then return 66; fi
    printf '%s\n' "$*" >>"${tmp_dir}/commands.txt"
    printf '%s\n' "${request_output}"
    return "${request_status}"
}
trim_file() { [[ "$2" == 1048576 && -s "$1" ]]; }

run_cli chaos_baseline 'SHOW PLACEMENTS;' <<<caller-input >"${tmp_dir}/cli.txt" \
    || fail 'a public CLI request did not use the run-owned helper with closed input'
[[ "$(cat "${tmp_dir}/cli.txt")" == observed ]] || fail 'CLI output changed'
grep -Fq -- 'admin timeout -k 5s 90s nervix-cli --server http://nervix-2:47391 --domain chaos_baseline --password fixture-password --command SHOW PLACEMENTS;' \
    "${tmp_dir}/commands.txt" || fail 'the selected endpoint, domain or command changed'
request_status=73
status=0
run_cli '' 'SHOW CLUSTER STATUS;' >/dev/null || status=$?
[[ "${status}" == 73 ]] || fail 'the client refusal was lost'
request_status=0
request_output='Error: refused command'
status=0
run_cli '' 'SHOW CLUSTER STATUS;' >/dev/null || status=$?
[[ "${status}" == 1 ]] || fail 'an acknowledged client error passed'
request_output=observed
probe_node nervix-1 <<<caller-input || fail 'readiness request failed'
probe_node_listeners nervix-3 <<<caller-input || fail 'listener request failed'
capture_metrics nervix-2 <<<caller-input || fail 'metrics request failed'
[[ "$(cat "${artifact_dir}/public/metrics-nervix-2.txt")" == observed ]] || fail 'metrics output was lost'
grep -Fq -- 'http://nervix-2:9090/metrics' "${tmp_dir}/commands.txt" || fail 'metrics target changed'
request_status=73
status=0
probe_node nervix-1 || status=$?
[[ "${status}" == 73 ]] || fail 'readiness refusal was lost'
status=0
capture_metrics nervix-2 >/dev/null || status=$?
[[ "${status}" == 73 ]] || fail 'metrics refusal was lost'
available_seconds=0
command_count="$(wc -l <"${tmp_dir}/commands.txt")"
status=0
run_cli '' 'SHOW CLUSTER STATUS;' >/dev/null || status=$?
[[ "${status}" == 124 && "$(wc -l <"${tmp_dir}/commands.txt")" == "${command_count}" ]] \
    || fail 'an exhausted run opened a helper request'
printf 'observation helper self-test passed\n'
