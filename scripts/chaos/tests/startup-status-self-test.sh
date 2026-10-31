#!/usr/bin/env bash
# The sourced controller fragment reads these deadline/phase globals at runtime.
# shellcheck disable=SC2034
set -euo pipefail

chaos_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../startup-status.sh
source "${chaos_dir}/startup-status.sh"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT
artifact_dir="${tmp_dir}/run"
mkdir -p "${artifact_dir}/public" "${tmp_dir}/bin"
printf 'node-1 ready\n' >"${artifact_dir}/public/cluster-status.attempt.txt"

# Borrow the controller's actual deadline/timeout owner, including native timeout outcomes.
awk '/^remaining_seconds\(\)/,/^phase\(\)/ {if (!/^phase\(\)/) print}' \
    "${chaos_dir}/run-baseline.sh" >"${tmp_dir}/bounds.sh"
# shellcheck source=/dev/null
source "${tmp_dir}/bounds.sh"
current_phase='startup status publication'
overall_deadline=$((SECONDS + 5))

# A stalled copy must finish within the remaining run budget and retain its native refusal.
cat >"${tmp_dir}/bin/cp" <<'SH'
#!/usr/bin/env bash
sleep 7
exit 0
SH
chmod +x "${tmp_dir}/bin/cp"
started_seconds="${SECONDS}"
status=0
PATH="${tmp_dir}/bin:${PATH}" publish_startup_status || status=$?
[[ "${status}" == 124 && $((SECONDS - started_seconds)) -le 8 ]] \
    || { printf 'startup status self-test failed: stalled publication returned %s after %ss\n' \
        "${status}" "$((SECONDS - started_seconds))" >&2
        cat "${artifact_dir}/diagnostics/startup-status/stderr.txt" >&2
        exit 1; }
jq -e '.operation == "startup-status-publication" and .command_exit_code == 124
    and .effective_seconds > 0 and .effective_seconds <= 5 and .outcome == "command-failed"' \
    "${artifact_dir}/diagnostics/startup-status/result.json" >/dev/null

[[ ! -e "${artifact_dir}/public/cluster-status.txt" ]] \
    || { printf 'stalled publication produced a final transcript\n' >&2; exit 1; }
overall_deadline=$((SECONDS + 30))
publish_startup_status
cmp "${artifact_dir}/public/cluster-status.attempt.txt" "${artifact_dir}/public/cluster-status.txt"
jq -e '.outcome == "passed" and .command_exit_code == 0 and .elapsed_seconds >= 0' \
    "${artifact_dir}/diagnostics/startup-status/result.json" >/dev/null
rm "${artifact_dir}/public/cluster-status.txt"

cat >"${tmp_dir}/bin/cp" <<'SH'
#!/usr/bin/env bash
[[ "$1" == --reflink=never && "$2" == -- ]] || exit 65
exit 73
SH
status=0
PATH="${tmp_dir}/bin:${PATH}" publish_startup_status || status=$?
[[ "${status}" == 73 && ! -e "${artifact_dir}/public/cluster-status.txt" ]]
jq -e '.command_exit_code == 73 and .outcome == "command-failed"' \
    "${artifact_dir}/diagnostics/startup-status/result.json" >/dev/null

overall_deadline="${SECONDS}"
status=0
PATH="${tmp_dir}/bin:${PATH}" publish_startup_status || status=$?
[[ "${status}" == 124 ]]
jq -e '.command_exit_code == null and .effective_seconds == 0
    and .outcome == "overall-deadline"' "${artifact_dir}/diagnostics/startup-status/result.json" >/dev/null

# Reporting refusal must not launch a publication whose outcome could not be attributed.
rm -r "${artifact_dir}/diagnostics/startup-status"
touch "${artifact_dir}/diagnostics/startup-status"
overall_deadline=$((SECONDS + 30))
status=0
PATH="${tmp_dir}/bin:${PATH}" publish_startup_status 2>/dev/null || status=$?
[[ "${status}" != 0 && "${status}" != 73 && ! -e "${artifact_dir}/public/cluster-status.txt" ]]
printf 'startup status self-test passed\n'


# Startup applies the requested policy once through the cluster command, before installing a graph.
# shellcheck source=../cluster-parameters.sh
source "${chaos_dir}/cluster-parameters.sh"
(
    export CHAOS_REPLICA_COUNT=2 CHAOS_STATE_SNAPSHOT_INTERVAL=250ms
    export CHAOS_NODE_UNAVAILABILITY_TIMEOUT=15s CHAOS_COMMAND_RETRY_VALIDITY=20m
    cli_command() { printf '%s\n' "$1"; }
    configured="$(configure_cluster_parameters)"
    [[ "${configured}" == $'ALTER CLUSTER SET REPLICA_COUNT = 2, STATE_SNAPSHOT_INTERVAL = 250ms, NODE_UNAVAILABILITY_TIMEOUT = 15s, COMMAND_RETRY_VALIDITY = 20m;\nDESCRIBE CLUSTER;' ]]
    export CHAOS_STATE_SNAPSHOT_INTERVAL='1s; SHOW CLUSTER STATUS;'
    status=0
    configure_cluster_parameters >"${tmp_dir}/policy-refusal.txt" 2>&1 || status=$?
    [[ "${status}" == 2 ]]
)
printf 'cluster parameter startup control self-test passed\n'

# The public observation keeps all parameter values and refuses a partial installation.
# shellcheck source=cluster-parameters-fixture.sh
source "${chaos_dir}/tests/cluster-parameters-fixture.sh"
for parameter_topology in 1 3; do
    parameter_nodes=(node-1)
    if ((parameter_topology == 3)); then parameter_nodes+=(node-2 node-3); fi
    cluster_parameter_publication_fixture '1m 30s' "${parameter_nodes[@]}" \
        >"${tmp_dir}/parameters-${parameter_topology}.txt"
    cluster_parameter_observation "${tmp_dir}/parameters-${parameter_topology}.txt" "${parameter_nodes[@]}" \
        >"${tmp_dir}/parameters-${parameter_topology}.json"
    jq -e --argjson nodes "${parameter_topology}" '
        .revision == "17" and (.committed | length) == 22
        and .committed["node-unavailability-timeout"] == "1m 30s"
        and (.running | length) == $nodes
        and all(.running[]; .revision == "17" and (.values | length) == 22)
    ' "${tmp_dir}/parameters-${parameter_topology}.json" >/dev/null
done
sed 's/revision=17$/revision=18446744073709551615/' "${tmp_dir}/parameters-3.txt" \
    >"${tmp_dir}/parameters-maximum-revision.txt"
cluster_parameter_observation "${tmp_dir}/parameters-maximum-revision.txt" node-1 node-2 node-3 \
    >"${tmp_dir}/parameters-maximum-revision.json"
jq -e '.revision == "18446744073709551615" and .running["node-3"].revision == .revision' \
    "${tmp_dir}/parameters-maximum-revision.json" >/dev/null

expect_parameter_refusal() {
    local transcript="$1" status=0
    cluster_parameter_observation "${transcript}" node-1 node-2 node-3 \
        >"${tmp_dir}/parameter-refusal.txt" 2>&1 || status=$?
    ((status != 0)) || { printf 'incomplete parameter installation was accepted: %s\n' "${transcript}" >&2; exit 1; }
}
sed '/^node=node-3 /d' "${tmp_dir}/parameters-3.txt" >"${tmp_dir}/parameters-pending-node.txt"
printf 'node=node-3 cluster parameters running=not installed\n' >>"${tmp_dir}/parameters-pending-node.txt"
expect_parameter_refusal "${tmp_dir}/parameters-pending-node.txt"
sed 's/^node=node-1 node-unavailability-timeout running=1m 30s/node=node-1 node-unavailability-timeout running=2m/' \
    "${tmp_dir}/parameters-3.txt" >"${tmp_dir}/parameters-different-value.txt"
expect_parameter_refusal "${tmp_dir}/parameters-different-value.txt"
sed 's/^node=node-1 node-unavailability-timeout running=1m 30s revision=17$/node=node-1 node-unavailability-timeout running=1m 30s revision=16/' \
    "${tmp_dir}/parameters-3.txt" >"${tmp_dir}/parameters-different-revision.txt"
expect_parameter_refusal "${tmp_dir}/parameters-different-revision.txt"
cp "${tmp_dir}/parameters-3.txt" "${tmp_dir}/parameters-repeated-key.txt"
grep '^node-unavailability-timeout committed=' "${tmp_dir}/parameters-3.txt" >>"${tmp_dir}/parameters-repeated-key.txt"
expect_parameter_refusal "${tmp_dir}/parameters-repeated-key.txt"

(
    cli_command() { cat "${tmp_dir}/parameters-3.txt"; }
    capture_cluster_parameters "${tmp_dir}/published-parameters" node-1 node-2 node-3
    cmp "${tmp_dir}/published-parameters.json" "${tmp_dir}/parameters-3.json"
    cli_command() { cat "${tmp_dir}/parameters-pending-node.txt"; }
    status=0
    capture_cluster_parameters "${tmp_dir}/published-parameters" node-1 node-2 node-3 \
        >"${tmp_dir}/capture-refusal.txt" 2>&1 || status=$?
    ((status != 0))
    cmp "${tmp_dir}/published-parameters.json" "${tmp_dir}/parameters-3.json"
)
printf 'committed and running parameter observation self-test passed\n'
