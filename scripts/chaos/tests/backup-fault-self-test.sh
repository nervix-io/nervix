#!/usr/bin/env bash
set -euo pipefail

# Self-test of the backup fault and fresh-restore scenarios: their option checks, and every outcome
# the backup fault verdict assigns to the fault backup's report and destination, from synthetic CLI
# reports. Archive verification and restore need a cluster and are not exercised here.

chaos_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

fail() {
    printf 'backup fault self-test failed: %s\n' "$*" >&2
    exit 1
}

expect_setup_rejection() {
    local case_name="$1"
    local expected="$2"
    shift 2
    local status=0
    "$@" >"${tmp_dir}/setup-rejection.out" 2>&1 || status=$?
    [[ "${status}" -eq 2 ]] || fail "${case_name} returned ${status}, expected setup error 2"
    grep -Fq -- "${expected}" "${tmp_dir}/setup-rejection.out" \
        || fail "${case_name} did not explain the rejected option: $(cat "${tmp_dir}/setup-rejection.out")"
}

list_output="$("${chaos_dir}/chaos.sh" list)"
for listed in backup-cycles backup-fault restore-fresh-cluster; do
    grep -Eq "^${listed} " <<<"${list_output}" || fail "scenario list omits ${listed}"
done
expect_setup_rejection 'backup-fault requires a fault' \
    '--fault for backup-fault must be owner-crash, leader-crash, owner-partition or rolling-restart, not none' \
    "${chaos_dir}/run-baseline.sh" --scenario backup-fault --image fixture
expect_setup_rejection 'backup-fault validates its fault' \
    '--fault for backup-fault must be owner-crash, leader-crash, owner-partition or rolling-restart, not voter-crash' \
    "${chaos_dir}/run-baseline.sh" --scenario backup-fault --image fixture --fault voter-crash
expect_setup_rejection 'backup-fault needs three nodes' 'backup-fault requires --nodes 3' \
    "${chaos_dir}/run-baseline.sh" --scenario backup-fault --image fixture --fault owner-crash --nodes 1
expect_setup_rejection 'restore-fresh-cluster needs three nodes' 'restore-fresh-cluster requires --nodes 3' \
    "${chaos_dir}/run-baseline.sh" --scenario restore-fresh-cluster --image fixture --nodes 1
expect_setup_rejection 'backup-cycles takes no fault' '--fault applies only to stateful, endpoints, lifetimes, domain-time, backup-fault and restore-fault' \
    "${chaos_dir}/run-baseline.sh" --scenario backup-cycles --image fixture --fault owner-crash

# The verdict reads the run's globals; this fixture run has its own directory.
script_dir="${chaos_dir}"
artifact_dir="${tmp_dir}/run"
scenario=backup-fault
current_phase='self-test'
mkdir -p "${artifact_dir}/results"
# shellcheck source=../backup-scenario.sh
source "${chaos_dir}/backup-scenario.sh"
# shellcheck source=../backup-fault-scenario.sh
source "${chaos_dir}/backup-fault-scenario.sh"
# Verifying and restoring a completed archive needs a cluster; the verdict's own checks do not.
backup_verify_archive() { :; }
backup_verify_restore() { :; }

completed_report() {
    jq -n --arg engaged "$1" --arg released "$2" \
        '{domains: [{domain: "chaos_baseline",
                     cut: {kind: "quiesced", engaged_at: $engaged, released_at: $released}}],
          execution_reference: "01a144ce-eb44-751a-9ba5-383fb748141b"}'
}

# Judges a fault backup that ended with STATUS and REPORT, with FILES left at its destination, for
# FAULT, and expects OUTCOME and the findings matching FINDING (or none). Every fixture fault begins
# at 13:00:00 UTC on 2026-10-16 and its acknowledgement gate begins to lift three seconds later.
judge() {
    local case_name="$1"
    local case_fault="$2"
    local status="$3"
    local report="$4"
    local expected_outcome="$5"
    local expected_finding="$6"
    shift 6
    local case_dir="${tmp_dir}/${case_name}"
    mkdir -p "${case_dir}/archive"
    printf '%s\n' "${report}" >"${case_dir}/fault-backup.json"
    printf '%s\n' "${status}" >"${case_dir}/fault-backup.exit"
    local file
    for file in "$@"; do
        printf 'bytes' >"${case_dir}/archive/${file}"
    done
    : >"${recovery_findings_ledger}"
    fault="${case_fault}"
    backup_fault_judge_outcome "${case_dir}" 1792155600000 1792155603000 2>"${case_dir}/verdict.txt"
    [[ "${backup_fault_outcome}" == "${expected_outcome}" ]] \
        || fail "${case_name} judged ${backup_fault_outcome}, expected ${expected_outcome}"
    if [[ -z "${expected_finding}" ]]; then
        [[ ! -s "${recovery_findings_ledger}" ]] \
            || fail "${case_name} recorded an unexpected finding: $(cat "${recovery_findings_ledger}")"
    else
        jq -se --arg finding "${expected_finding}" 'length == 1 and (.[0].message | contains($finding))' \
            "${recovery_findings_ledger}" >/dev/null \
            || fail "${case_name} did not record only '${expected_finding}': $(cat "${recovery_findings_ledger}")"
    fi
}

refusal='{"error":{"code":"BACKUP_REFUSED","message":"backup failed: failed to capture domain '"'"'chaos_baseline'"'"': timed out draining domain"}}'
judge clean-refusal owner-crash 1 "${refusal}" refused '' fault-backup.stderr
judge refusal-left-partial owner-partition 1 "${refusal}" refused 'left an archive or a partial file' .nervix-backup-7Gq2
judge refusal-left-archive owner-crash 1 "${refusal}" refused 'left an archive or a partial file' fault.nvxb
judge failure-without-report owner-crash 1 '' invalid 'without a JSON failure report'
judge failure-without-backup-reason leader-crash 1 '{"error":{"code":"CONNECTION_FAILED","message":"transport closed"}}' \
    refused 'without a backup failure reason'
[[ "${backup_fault_reference}" == "" ]] || fail 'a failure without a reference recorded one'
judge uncertain-with-reference leader-crash 1 \
    '{"error":{"code":"BACKUP_FAILED","message":"backup failed: the command wait expired","execution_reference":"01a144ce-eb44-751a-9ba5-383fb748141b"}}' \
    refused ''
[[ "${backup_fault_reference}" == 01a144ce-eb44-751a-9ba5-383fb748141b ]] \
    || fail "the uncertain failure's reference was not kept: ${backup_fault_reference}"
judge completed-without-archive owner-crash 0 \
    "$(completed_report '2026-10-16 13:00:05.000000000 UTC' '2026-10-16 13:00:06.000000000 UTC')" \
    completed 'reported success without an archive'
judge leader-cut-before-crash leader-crash 0 \
    "$(completed_report '2026-10-16 12:59:59.000000000 UTC' '2026-10-16 13:00:09.000000000 UTC')" \
    completed 'assembled from a cut engaged before its coordinator crashed' fault.nvxb
judge leader-cut-after-crash leader-crash 0 \
    "$(completed_report '2026-10-16 13:00:06.000000000 UTC' '2026-10-16 13:00:09.000000000 UTC')" \
    completed '' fault.nvxb
[[ "${backup_fault_cut_engaged_ms}" == 1792155606000 && "${backup_fault_cut_released_ms}" == 1792155609000 ]] \
    || fail "the completed cut window was ${backup_fault_cut_engaged_ms}..${backup_fault_cut_released_ms}"
judge owner-cut-spanning-crash owner-crash 0 \
    "$(completed_report '2026-10-16 12:59:59.000000000 UTC' '2026-10-16 13:00:09.000000000 UTC')" \
    completed '' fault.nvxb
judge cut-released-behind-gate owner-partition 0 \
    "$(completed_report '2026-10-16 12:59:59.000000000 UTC' '2026-10-16 13:00:02.000000000 UTC')" \
    completed 'the cut released while the acknowledgement gate withheld an admitted publish' fault.nvxb

printf 'backup fault self-test passed\n'
