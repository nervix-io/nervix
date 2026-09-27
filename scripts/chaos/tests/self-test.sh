#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
chaos_dir="$(cd "${script_dir}/.." && pwd)"

fail() {
    printf 'self-test failed: %s\n' "$*" >&2
    exit 1
}

expect_verification_failure() {
    local case_name="$1"
    local expected="$2"
    local observed="$3"
    local result="$4"
    local jq_check="$5"
    local status=0

    "${chaos_dir}/verify-ledger.sh" "${expected}" "${observed}" "${result}" || status=$?
    [[ "${status}" -eq 1 ]] || fail "${case_name} returned ${status}, expected verifier verdict 1"
    jq -e "${jq_check}" "${result}" >/dev/null \
        || fail "${case_name} did not report the expected category"
}

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

expected="${tmp_dir}/expected.ndjson"
observed="${tmp_dir}/observed.ndjson"
result="${tmp_dir}/result.json"

printf '%s\n' \
    '{"event_id":"run-0","branch_name":"alpha","sequence":0,"content":"payload-0"}' \
    '{"event_id":"run-1","branch_name":"beta","sequence":1,"content":"payload-1"}' \
    >"${expected}"
cp "${expected}" "${observed}"
"${chaos_dir}/verify-ledger.sh" "${expected}" "${observed}" "${result}"
jq -e '.verdict == "pass" and .expected_records == 2 and .observed_records == 2' \
    "${result}" >/dev/null || fail "matching ledgers did not pass"

head -n 1 "${expected}" >"${observed}"
expect_verification_failure \
    "missing record" "${expected}" "${observed}" "${result}" \
    '.verdict == "fail" and (.missing_ids | length) == 1 and .missing_ids[0] == "run-1"'

{
    head -n 1 "${expected}"
    head -n 1 "${expected}"
} >"${observed}"
expect_verification_failure \
    "duplicate cannot replace missing" "${expected}" "${observed}" "${result}" \
    '.verdict == "fail" and (.duplicates | length) == 1 and .duplicates[0].event_id == "run-0" and .duplicates[0].count == 2 and (.missing_ids | index("run-1")) != null'

printf '%s\n' \
    '{"event_id":"run-0","branch_name":"alpha","sequence":0,"content":"corrupt"}' \
    '{"event_id":"run-1","branch_name":"beta","sequence":1,"content":"payload-1"}' \
    >"${observed}"
expect_verification_failure \
    "corrupt record" "${expected}" "${observed}" "${result}" \
    '.verdict == "fail" and (.incorrect_content | length) == 1 and .incorrect_content[0].event_id == "run-0"'

printf '%s\n' '{not-json}' >"${observed}"
status=0
"${chaos_dir}/verify-ledger.sh" "${expected}" "${observed}" "${result}" || status=$?
[[ "${status}" -eq 2 ]] || fail "invalid JSON returned ${status}, expected setup error 2"
jq -e '.verdict == "error" and .category == "invalid_observed_ledger"' "${result}" >/dev/null \
    || fail "invalid JSON was not classified as an observed-ledger setup error"

list_output="$("${chaos_dir}/chaos.sh" list)"
grep -Fq 'baseline' <<<"${list_output}" || fail "scenario list omits baseline"
grep -Fq 'one-node' <<<"${list_output}" || fail "scenario list omits one-node support"
grep -Fq 'three-node' <<<"${list_output}" || fail "scenario list omits three-node support"

status=0
"${chaos_dir}/chaos.sh" run baseline >"${tmp_dir}/missing-image.out" 2>&1 || status=$?
[[ "${status}" -eq 2 ]] || fail "missing --image returned ${status}, expected setup error 2"
grep -Fq -- '--image is required' "${tmp_dir}/missing-image.out" \
    || fail "missing --image did not produce an explicit setup error"

status=0
invalid_image_run_id="invalid-image-$$-${RANDOM}"
"${chaos_dir}/chaos.sh" run baseline \
    --image 'not a valid image reference' \
    --nodes 1 \
    --artifacts "${tmp_dir}/artifacts" \
    --run-id "${invalid_image_run_id}" \
    --timeout 120 \
    >"${tmp_dir}/invalid-image.out" 2>&1 || status=$?
[[ "${status}" -eq 2 ]] || fail "invalid image returned ${status}, expected setup error 2"
grep -Fq "could not be resolved" "${tmp_dir}/invalid-image.out" \
    || fail "invalid image did not produce an explicit setup error"
jq -e '.status == "failed" and .exit_code == 2 and .final_phase == "preflight"' \
    "${tmp_dir}/artifacts/${invalid_image_run_id}/manifest.json" >/dev/null \
    || fail "invalid image did not preserve a failed preflight manifest"

for script in "${chaos_dir}"/*.sh "${chaos_dir}"/tests/*.sh; do
    bash -n "${script}"
done

placeholder_image="sha256:0000000000000000000000000000000000000000000000000000000000000000"
compose_json="${tmp_dir}/compose.json"
NERVIX_IMAGE="${placeholder_image}" \
CHAOS_RUN_ID="self-test" \
CHAOS_CLUSTER_ID="chaos-self-test" \
CHAOS_TLS_DIR="${tmp_dir}" \
CHAOS_PASSWORD="self-test-password" \
CHAOS_KAFKA_IMAGE="apache/kafka:3.9.1" \
CHAOS_KCAT_IMAGE="edenhill/kcat:1.7.1" \
CHAOS_PROBE_IMAGE="alpine:3.22" \
    docker compose -f "${chaos_dir}/compose.yaml" --profile tools --profile three-node \
    config --format json \
    >"${compose_json}"

jq -e --arg image "${placeholder_image}" '
    ([.services.admin, .services["nervix-1"], .services["nervix-2"], .services["nervix-3"]]
      | all(.image == $image))
    and ([.services[]] | all(has("build") | not))
    and ([.services | to_entries[] | select(.key | test("^nervix-[123]$")) | .value]
         | length == 3)
    and ([.services | to_entries[] | select(.key | test("^nervix-[123]$")) | .value]
         | all(.labels["io.nervix.chaos.run"] == "self-test"))
' "${compose_json}" >/dev/null || fail "Compose does not pin every Nervix service or contains a build"

printf 'chaos harness self-test passed\n'
