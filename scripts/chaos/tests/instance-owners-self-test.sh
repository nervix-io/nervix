#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../instance-owners.sh
source "${script_dir}/instance-owners.sh"
# shellcheck source=../rolling-restart-scenario.sh
source "${script_dir}/rolling-restart-scenario.sh"
instance_test_dir="$(mktemp -d)"
trap 'rm -rf "${instance_test_dir}"' EXIT

fail() { printf 'instance owner self-test failed: %s\n' "$*" >&2; exit 1; }

cat >"${instance_test_dir}/before.txt" <<'DESCRIPTION'
instance 0: node=node-1 status=running ready=true
instance 1: node=node-2 status=running ready=true
instance 2: node=node-3 status=running ready=true
DESCRIPTION
verify_spread_instance_start "${instance_test_dir}/before.txt" "${instance_test_dir}/before.json" || fail 'three distinct ready owners were refused'
domain_cli_command() {
    [[ "$1" == 'DESCRIBE INGESTOR chaos_ingestor;' ]] || return 1
    cat "${instance_test_dir}/before.txt"
}
describe_spread_instances_ready "${instance_test_dir}/description.txt" "${instance_test_dir}/observed.json" \
    || fail 'the public description did not expose all ready instances'
cmp "${instance_test_dir}/before.json" "${instance_test_dir}/observed.json" \
    || fail 'the observed public owners changed'

node_hosts=(nervix-1 nervix-2 nervix-3)
artifact_dir="${instance_test_dir}"
mkdir -p "${artifact_dir}/public"
for active in "${node_hosts[@]}"; do
    for host in "${node_hosts[@]}"; do
        total=0
        [[ "${host}" != "${active}" ]] || total=1
        printf 'nervix_messages_total{direction="sent",physical_node_id="node-%s",relay="chaos_records",target="chaos_ingestor"} %s\n' \
            "${host##*-}" "${total}" >"${artifact_dir}/public/metrics-${host}.txt"
    done
    instance_source_traffic_ready || fail "the active consumer on ${active} was not observed"
done
for host in "${node_hosts[@]}"; do
    printf 'nervix_messages_total{direction="received",physical_node_id="node-%s",relay="chaos_records",target="chaos_ingestor"} 5\n' \
        "${host##*-}" >"${artifact_dir}/public/metrics-${host}.txt"
done
if instance_source_traffic_ready; then
    fail 'received traffic qualified as an intake submission'
fi
sed 's/instance 0: node=node-1/instance 0: node=node-2/' "${instance_test_dir}/before.txt" >"${instance_test_dir}/after.txt"
verify_instance_handoff "${instance_test_dir}/before.json" "${instance_test_dir}/after.txt" node-1 "${instance_test_dir}/after.json" || fail 'the exact unit handoff was refused'

for defect in sibling ready duplicate missing failed-owner; do
    case "${defect}" in
        sibling) sed 's/instance 1: node=node-2/instance 1: node=node-3/' "${instance_test_dir}/after.txt" ;;
        ready) sed 's/instance 2: node=node-3 status=running ready=true/instance 2: node=node-3 status=running ready=false/' "${instance_test_dir}/after.txt" ;;
        duplicate) cat "${instance_test_dir}/after.txt"; tail -n 1 "${instance_test_dir}/after.txt" ;;
        missing) head -n 2 "${instance_test_dir}/after.txt" ;;
        failed-owner) cat "${instance_test_dir}/before.txt" ;;
    esac >"${instance_test_dir}/defect.txt"
    if verify_instance_handoff "${instance_test_dir}/before.json" "${instance_test_dir}/defect.txt" node-1 "${instance_test_dir}/defect.json"; then
        fail "${defect} passed the unit invariant"
    fi
done
printf 'instance owner self-test passed\n'
