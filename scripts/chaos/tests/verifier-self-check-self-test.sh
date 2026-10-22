#!/usr/bin/env bash
# Checks, without Docker, when a run adopts the verifier self-check its suite executed: the identity
# of the chaos scripts, the reasons a record does not apply, and the run's refusal of such a record
# before it creates anything.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
chaos_dir="$(cd "${script_dir}/.." && pwd)"
# shellcheck source=../verifier-self-check.sh
source "${chaos_dir}/verifier-self-check.sh"

fail() {
    printf 'verifier self-check self-test failed: %s\n' "$*" >&2
    exit 1
}

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

[[ "${chaos_verifier_self_check_seconds}" =~ ^[1-9][0-9]*$ ]] \
    || fail 'the verifier self-check bound is not a positive number of seconds'

# The identity covers every file's path and content, in a fixed order.
scripts="${tmp_dir}/scripts"
mkdir -p "${scripts}/tests"
printf 'echo verify\n' >"${scripts}/verify-ledger.sh"
printf 'echo self-test\n' >"${scripts}/tests/self-test.sh"
identity="$(chaos_scripts_identity "${scripts}")"
[[ "${identity}" =~ ^[a-f0-9]{64}$ ]] || fail "the scripts identity is not a SHA-256 digest: ${identity}"
[[ "$(chaos_scripts_identity "${scripts}")" == "${identity}" ]] \
    || fail 'the same scripts were given two identities'
printf 'echo verify differently\n' >"${scripts}/verify-ledger.sh"
changed="$(chaos_scripts_identity "${scripts}")"
[[ "${changed}" != "${identity}" ]] || fail 'a changed verifier kept the identity of the scripts'
mv "${scripts}/verify-ledger.sh" "${scripts}/verify-output.sh"
[[ "$(chaos_scripts_identity "${scripts}")" != "${changed}" ]] \
    || fail 'a renamed verifier kept the identity of the scripts'
mv "${scripts}/verify-output.sh" "${scripts}/verify-ledger.sh"
printf 'echo added\n' >"${scripts}/tests/added-self-test.sh"
[[ "$(chaos_scripts_identity "${scripts}")" != "${changed}" ]] \
    || fail 'an added self-test kept the identity of the scripts'
rm "${scripts}/tests/added-self-test.sh"
[[ "$(chaos_scripts_identity "${scripts}")" == "${changed}" ]] \
    || fail 'restoring the scripts did not restore their identity'

boot="$(chaos_boot_identity)" || fail 'this worker has no boot identity'
[[ -n "${boot}" ]] || fail 'the boot identity is empty'

# A suite's record names its output beside it, and binds the passing check to the scripts and
# the boot it ran on.
suite_dir="${tmp_dir}/suite"
mkdir -p "${suite_dir}"
printf 'chaos harness self-test passed\n' >"${suite_dir}/verifier-self-test.txt"
output_sha256="$(openssl dgst -sha256 -r "${suite_dir}/verifier-self-test.txt" | awk '{ print $1 }')"
valid_record() {
    jq -n --arg output_sha256 "${output_sha256}" --arg scripts "$(chaos_scripts_identity "${chaos_dir}")" \
        --arg boot "${boot}" --argjson bound "${chaos_verifier_self_check_seconds}" \
        '{status: "passed", exit_code: 0, started_at: "2026-10-22T08:00:00Z",
          finished_at: "2026-10-22T08:02:31Z", duration_seconds: 151, bound_seconds: $bound,
          output: "verifier-self-test.txt", output_sha256: $output_sha256,
          scripts_sha256: $scripts, boot_id: $boot}'
}
record="${suite_dir}/verifier-self-check.json"
valid_record >"${record}"
chaos_verifier_record_refusal "${record}" "${chaos_dir}" >"${tmp_dir}/refusal.txt" \
    || fail "a passing record of these scripts on this boot was refused: $(cat "${tmp_dir}/refusal.txt")"
[[ ! -s "${tmp_dir}/refusal.txt" ]] || fail 'an applicable record printed a refusal'

expect_refusal() {
    local case_name="$1"
    local path="$2"
    local expected="$3"
    local status=0
    chaos_verifier_record_refusal "${path}" "${chaos_dir}" >"${tmp_dir}/refusal.txt" || status=$?
    [[ "${status}" -eq 1 ]] || fail "${case_name} returned ${status}, expected a refusal"
    grep -Fq -- "${expected}" "${tmp_dir}/refusal.txt" \
        || fail "${case_name} was refused with '$(cat "${tmp_dir}/refusal.txt")', not '${expected}'"
}
expect_refusal 'a missing record' "${suite_dir}/absent.json" 'it does not exist'
printf '[]\n' >"${suite_dir}/array.json"
expect_refusal 'a record that is no object' "${suite_dir}/array.json" 'it is not a JSON object'
valid_record | jq '.status = "failed" | .exit_code = 1' >"${suite_dir}/failed.json"
expect_refusal 'a failed check' "${suite_dir}/failed.json" 'it records status failed, exit code 1'
valid_record | jq '.scripts_sha256 = ("0" * 64)' >"${suite_dir}/other-scripts.json"
expect_refusal 'a check of other scripts' "${suite_dir}/other-scripts.json" \
    "it checked chaos scripts $(printf '0%.0s' {1..64}), not these"
valid_record | jq '.boot_id = "another-boot"' >"${suite_dir}/other-boot.json"
expect_refusal 'a check on another boot' "${suite_dir}/other-boot.json" \
    'it ran on boot another-boot of a worker, not this one'
valid_record | jq '.output = "missing.txt"' >"${suite_dir}/no-output.json"
expect_refusal 'a check without its output' "${suite_dir}/no-output.json" 'its output missing.txt is missing'
valid_record | jq '.output = "../outside.txt"' >"${suite_dir}/outside-output.json"
expect_refusal 'a check whose output is elsewhere' "${suite_dir}/outside-output.json" \
    'its output must be a file beside it'
printf 'something else passed\n' >"${suite_dir}/verifier-self-test.txt"
expect_refusal 'a check whose output changed' "${record}" \
    'its output verifier-self-test.txt no longer has the digest it records'
printf 'chaos harness self-test passed\n' >"${suite_dir}/verifier-self-test.txt"
for field in started_at finished_at duration_seconds bound_seconds; do
    valid_record | jq --arg field "${field}" 'del(.[$field])' >"${suite_dir}/no-${field}.json"
    expect_refusal "a record without ${field}" "${suite_dir}/no-${field}.json" 'it does not record'
done

# A run refuses a record that does not apply before it creates its run directory, so a suite
# reports the entry as refused by setup.
status=0
"${chaos_dir}/run-baseline.sh" --scenario stale-follower --image fixture \
    --artifacts "${tmp_dir}/runs" --run-id refused-record \
    --verifier-self-check "${suite_dir}/failed.json" >"${tmp_dir}/run-refusal.txt" 2>&1 || status=$?
[[ "${status}" -eq 2 ]] || fail "a run given a failed check returned ${status}, expected setup error 2"
grep -Fq "the verifier self-check record ${suite_dir}/failed.json does not apply to this run: it records status failed, exit code 1" \
    "${tmp_dir}/run-refusal.txt" || fail "the run did not name the record it refused: $(cat "${tmp_dir}/run-refusal.txt")"
[[ ! -e "${tmp_dir}/runs/refused-record" ]] || fail 'a run that refused its record created its directory'
status=0
"${chaos_dir}/run-baseline.sh" --scenario stale-follower --image fixture \
    --verifier-self-check >"${tmp_dir}/run-refusal.txt" 2>&1 || status=$?
[[ "${status}" -eq 2 ]] || fail "--verifier-self-check without a record returned ${status}, expected 2"

printf 'verifier self-check self-test passed\n'
