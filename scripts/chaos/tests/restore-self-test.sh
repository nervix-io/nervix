#!/usr/bin/env bash
set -euo pipefail

# Self-test of the restore-fault and restore-volume scenarios: their option checks, the padded guest
# generator's bounds, the sampler's records and the staging verdicts read from them, and every
# outcome the restore verdict assigns to a restore's report. Restoring, comparing and injecting
# faults need a cluster and are not exercised here.

chaos_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

fail() {
    printf 'restore self-test failed: %s\n' "$*" >&2
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
for listed in restore-fault restore-volume; do
    grep -Eq "^${listed} " <<<"${list_output}" || fail "scenario list omits ${listed}"
done
expect_setup_rejection 'restore-fault requires a fault' \
    '--fault for restore-fault must be leader-crash, leader-partition, owner-crash or reclamation-owner-crash, not none' \
    "${chaos_dir}/run-baseline.sh" --scenario restore-fault --image fixture
expect_setup_rejection 'restore-fault validates its fault' \
    '--fault for restore-fault must be leader-crash, leader-partition, owner-crash or reclamation-owner-crash, not owner-partition' \
    "${chaos_dir}/run-baseline.sh" --scenario restore-fault --image fixture --fault owner-partition
expect_setup_rejection 'restore-fault needs three nodes' 'restore-fault requires --nodes 3' \
    "${chaos_dir}/run-baseline.sh" --scenario restore-fault --image fixture --fault owner-crash --nodes 1
expect_setup_rejection 'restore-volume needs three nodes' 'restore-volume requires --nodes 3' \
    "${chaos_dir}/run-baseline.sh" --scenario restore-volume --image fixture --nodes 1
expect_setup_rejection 'restore-volume takes no fault' \
    '--fault applies only to stateful, endpoints, lifetimes, domain-time, backup-fault and restore-fault' \
    "${chaos_dir}/run-baseline.sh" --scenario restore-volume --image fixture --fault owner-crash
expect_setup_rejection 'restore-volume holds no outage' \
    'restore-volume injects no fault; --outage-seconds is for crash scenarios' \
    "${chaos_dir}/run-baseline.sh" --scenario restore-volume --image fixture --outage-seconds 10
expect_setup_rejection 'a leader partition holds no outage' \
    'leader-partition holds its isolation until the restore ends; --outage-seconds is for the crash faults' \
    "${chaos_dir}/run-baseline.sh" --scenario restore-fault --image fixture --fault leader-partition --outage-seconds 10

# The padded guest saves exactly the requested bytes, which must be a multiple of eight small
# enough that the save and a host buffer as large fit the fixture's 96 MiB MAX MEMORY. Its host
# buffer starts at the first page past the save. A host-driven test in the nervix-wasm package,
# chaos_padded_state, proves what the module does.
generator="${chaos_dir}/fixtures/wasm/padded-state-wat.sh"
for refused in 0 7 12 49283080 abc; do
    status=0
    "${generator}" "${refused}" >"${tmp_dir}/padded.wat" 2>"${tmp_dir}/padded.err" || status=$?
    [[ "${status}" -eq 2 ]] || fail "the padded guest generator accepted a ${refused}-byte save"
done
"${generator}" 49283072 >"${tmp_dir}/padded.wat" \
    || fail 'the padded guest generator refused the largest save that fits'
"${generator}" 2097152 >"${tmp_dir}/padded.wat"
[[ "$(grep -c 'i32.const 2097152' "${tmp_dir}/padded.wat")" -eq 3 ]] \
    || fail 'the padded guest does not save, report and load exactly 2097152 bytes'
grep -Fq 'i32.const 2162688 i32.ge_u' "${tmp_dir}/padded.wat" \
    || fail 'the padded guest does not fill its save up to the end of the region at 64 KiB'
grep -Fq 'i32.const 2162688)' "${tmp_dir}/padded.wat" \
    || fail 'the padded guest does not hand the host a buffer past its save'
[[ "$(tr -cd '(' <"${tmp_dir}/padded.wat" | wc -c)" -eq "$(tr -cd ')' <"${tmp_dir}/padded.wat" | wc -c)" ]] \
    || fail 'the padded guest text does not balance its parentheses'

# The verdicts read the run's globals; this fixture run has its own directory.
script_dir="${chaos_dir}"
artifact_dir="${tmp_dir}/run"
scenario=restore-fault
current_phase='self-test'
node_hosts=(nervix-1 nervix-2 nervix-3)
mkdir -p "${artifact_dir}/results"
# shellcheck source=../backup-scenario.sh
source "${chaos_dir}/backup-scenario.sh"
# shellcheck source=../restore-scenario.sh
source "${chaos_dir}/restore-scenario.sh"
# shellcheck source=../restore-fault-scenario.sh
source "${chaos_dir}/restore-fault-scenario.sh"
: >"${recovery_findings_ledger}"

# A sample line the sampler is still writing is left out, a labeled series takes its class into
# its name, and a node that did not answer is recorded as unavailable.
restore_sampler_dir="${tmp_dir}/samples"
mkdir -p "${restore_sampler_dir}"
cat >"${restore_sampler_dir}/samples.txt" <<'EOF'
1000 1 nervix-1 nervix_restore_staging_bytes 0
1000 1 nervix-1 nervix_restore_staging_keys 0
1000 1 nervix-1 nervix_restore_staging_sweeps_total 10
1000 1 nervix-1 nervix_execution_memory_reserved_bytes{class="bulk"} 2097152
1000 1 nervix-2 nervix_restore_staging_bytes 4096
1000 1 nervix-2 nervix_restore_staging_keys 3
1000 1 nervix-2 nervix_restore_staging_sweeps_total 7
1000 1 nervix-3 unavailable 1
1002 2 nervix-1 nervix_restore_staging_bytes 0
1002 2 nervix-1 nervix_restore_staging_keys 0
1002 2 nervix-1 nervix_restore_staging_sweeps_total 11
1002 2 nervix-2 nervix_restore_staging_bytes 0
1002 2 nervix-2 nervix_restore_staging_keys 0
1002 2 nervix-2 nervix_restore_staging_sweeps_total 8
1002 2 nervix-3 nervix_restore_staging_bytes 0
1002 2 nervix-3 nervix_restore_staging_keys 0
1002 2 nervix-3 nervix_restore_staging_sweeps_total 2
1004 3 nervix-1 nervix_restore_staging_bytes 0
1004 3 nervix-1 nervix_restore_staging_keys 0
1004 3 nervix-1 nervix_restore_staging_sweeps_total 12
1004 3 nervix-2 nervix_restore_staging_bytes 0
1004 3 nervix-2 nervix_restore_staging_keys 0
1004 3 nervix-2 nervix_restore_staging_sweeps_total 9
1004 3 nervix-3 nervix_restore_staging_bytes 0
1004 3 nervix-3 nervix_restore_staging_keys 0
1004 3 nervix-3 nervix_restore_staging_sweeps_total 1
1006 4 nervix-3 nervix_restore_stag
EOF
restore_samples_json "${restore_sampler_dir}/samples.txt" >"${tmp_dir}/samples.ndjson"
[[ "$(wc -l <"${tmp_dir}/samples.ndjson")" -eq 9 ]] \
    || fail "the sampler records hold $(wc -l <"${tmp_dir}/samples.ndjson") samples, expected 9"
jq -se '.[0].series.nervix_execution_memory_reserved_bytes_bulk == 2097152
        and .[2].series == {unavailable: 1}' "${tmp_dir}/samples.ndjson" >/dev/null \
    || fail "the sampler records do not name classed or unavailable series: $(head -n 3 "${tmp_dir}/samples.ndjson")"

# Staging is read from each node's latest answered sample.
[[ "$(restore_staging_hosts)" == '[]' ]] \
    || fail "the latest samples report staging on $(restore_staging_hosts)"
head -n 8 "${restore_sampler_dir}/samples.txt" >"${tmp_dir}/early.txt"
cp "${restore_sampler_dir}/samples.txt" "${tmp_dir}/all.txt"
cp "${tmp_dir}/early.txt" "${restore_sampler_dir}/samples.txt"
[[ "$(restore_staging_hosts)" == '["nervix-2"]' ]] \
    || fail "the first samples report staging on $(restore_staging_hosts), expected nervix-2"
restore_staging_on nervix-3 nervix-2 || fail 'staging on one of two hosts was not recognized'
! restore_staging_on_all nervix-1 nervix-2 || fail 'staging on every host was claimed for one'

# Staging settles only through a completed sweep that measured zero: two consecutive zero samples of
# one process whose sweep count rose. nervix-3 restarted between its samples, so its sweep count fell
# and it has not settled.
cp "${tmp_dir}/all.txt" "${restore_sampler_dir}/samples.txt"
! restore_staging_settled 999 || fail 'a node whose sweep count fell across its restart counted as settled'
printf '%s\n' '1006 4 nervix-3 nervix_restore_staging_bytes 0' '1006 4 nervix-3 nervix_restore_staging_keys 0' \
    '1006 4 nervix-3 nervix_restore_staging_sweeps_total 2' >>"${restore_sampler_dir}/samples.txt"
restore_staging_settled 999 || fail 'every node completed a zero sweep, yet staging did not settle'
! restore_staging_settled 1003 || fail 'samples taken before the bound counted toward settling'
restore_fault_host_reclaimed nervix-2 999000 || fail 'nervix-2 completed a zero sweep, yet it did not count as reclaimed'
! restore_fault_host_reclaimed nervix-2 1003000 || fail 'a single sample after the bound counted as a reclaiming sweep'
restore_samples_json "${restore_sampler_dir}/samples.txt" >"${tmp_dir}/samples.ndjson"
summary="$(restore_samples_summary "${tmp_dir}/samples.ndjson" 0 9999)"
jq -e '."nervix-2".peak.nervix_restore_staging_bytes == 4096 and ."nervix-2".last.nervix_restore_staging_bytes == 0
       and ."nervix-3".samples == 4 and ."nervix-3".answered == 3' <<<"${summary}" >/dev/null \
    || fail "the sample summary is wrong: ${summary}"

# Judges the restore that ended with STATUS and REPORT for TARGET, and expects OUTCOME and the
# findings matching FINDING (or none). TARGET_EXISTS says what listing domains finds.
judge() {
    local case_name="$1"
    local status="$2"
    local report="$3"
    local expected_outcome="$4"
    local expected_finding="$5"
    local target_exists="${6:-false}"
    local case_dir="${tmp_dir}/${case_name}"
    mkdir -p "${case_dir}"
    printf '%s\n' "${report}" >"${case_dir}/first.json"
    printf '%s\n' "${status}" >"${case_dir}/first.exit"
    restore_target_exists() { [[ "${target_exists}" == true ]]; }
    : >"${recovery_findings_ledger}"
    restore_judge "${case_dir}" first chaos_restored 2>"${case_dir}/verdict.txt"
    [[ "${restore_outcome}" == "${expected_outcome}" ]] \
        || fail "${case_name} judged ${restore_outcome}, expected ${expected_outcome}"
    if [[ -z "${expected_finding}" ]]; then
        [[ ! -s "${recovery_findings_ledger}" ]] \
            || fail "${case_name} recorded an unexpected finding: $(cat "${recovery_findings_ledger}")"
    else
        jq -se --arg finding "${expected_finding}" 'length == 1 and (.[0].message | contains($finding))' \
            "${recovery_findings_ledger}" >/dev/null \
            || fail "${case_name} did not record only '${expected_finding}': $(cat "${recovery_findings_ledger}")"
    fi
}

reference=01a1575b-cb34-70ae-b332-619c0cb7d918
steps() {
    jq -nc --arg last "$1" '[{step:"create domain '"'"'chaos_restored'"'"'",outcome:"applied"},
        {step:"import resource versions of domain '"'"'chaos_restored'"'"'",outcome:"applied"},
        {step:"apply models of domain '"'"'chaos_restored'"'"'",outcome:$last}]'
}
completed() {
    jq -nc --arg reference "${reference}" --argjson steps "$(steps "$1")" \
        '{execution_reference:$reference, mode:"apply", domains:[{domain:"chaos_restored",status:"STOPPED"}],
          steps:$steps}'
}
incomplete() {
    jq -nc --arg reference "${reference}" --argjson steps "$(steps "$1")" \
        '{error:{code:"RESTORE_INCOMPLETE",
                 message:"restore failed at step '"'"'apply models of domain '"'"'chaos_restored'"'"''"'"': failed to send restored state to '"'"'node-2'"'"'",
                 report:{execution_reference:$reference, domains:[{domain:"chaos_restored"}], steps:$steps}}}'
}
judge completed 0 "$(completed applied)" completed ''
[[ "${restore_reference}" == "${reference}" ]] || fail "the completed restore's reference was ${restore_reference}"
judge completed-with-a-failed-step 0 "$(completed failed)" completed 'reported success without applying every step'
judge incomplete 1 "$(incomplete failed)" incomplete ''
[[ "${restore_reference}" == "${reference}" ]] || fail "the incomplete restore's reference was ${restore_reference}"
judge incomplete-without-a-failed-step 1 "$(incomplete applied)" incomplete 'without a report naming the one step'
judge refused-unchanged 1 '{"error":{"code":"RESTORE_REFUSED","message":"restore refused: planning failed"}}' \
    refused '' false
judge refused-after-creating 1 '{"error":{"code":"RESTORE_REFUSED","message":"restore redirect loop exceeded"}}' \
    refused 'was refused although it created chaos_restored' true
judge uncertain 1 '{"error":{"code":"RESTORE_FAILED","message":"command outcome for execution '"'"'01a1575b'"'"' is uncertain"}}' \
    failed 'ended with status 1 and RESTORE_FAILED'
judge without-report 137 '' failed 'and no JSON failure report'

printf 'restore self-test passed\n'
