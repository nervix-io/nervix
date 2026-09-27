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

restart_verifier="${chaos_dir}/verify-restart-evidence.sh"
inspection_before="${tmp_dir}/before.json"
inspection_stopped="${tmp_dir}/stopped.json"
inspection_started="${tmp_dir}/started.json"
shutdown_log="${tmp_dir}/shutdown.log"
test_run_id="self-test"
test_project="nervix-chaos-self-test"
test_image_id="sha256:$(printf 'f%.0s' {1..64})"
test_container_id="$(printf 'a%.0s' {1..64})"
jq -n \
    --arg id "${test_container_id}" \
    --arg name "/${test_project}-nervix-1-1" \
    --arg image "${test_image_id}" \
    --arg project "${test_project}" '
    [{Id:$id,Name:$name,Image:$image,Config:{Labels:{
      "io.nervix.chaos.run":"self-test",
      "io.nervix.chaos.role":"node",
      "io.nervix.chaos.target":"true",
      "com.docker.compose.project":$project,
      "com.docker.compose.service":"nervix-1"
    }},Mounts:[{Type:"volume",Name:($project+"_node-1-data")}],
    State:{Running:true,ExitCode:0,OOMKilled:false,StartedAt:"2026-09-27T00:00:00Z"}}]
    ' >"${inspection_before}"
jq '.[0].HostConfig.RestartPolicy.Name = "no"' "${inspection_before}" >"${tmp_dir}/before-with-policy.json"
mv "${tmp_dir}/before-with-policy.json" "${inspection_before}"
jq '.[0].State.Running = false' "${inspection_before}" >"${inspection_stopped}"
jq '.[0].State.StartedAt = "2026-09-27T00:01:00Z"' \
    "${inspection_before}" >"${inspection_started}"
printf '%s\n' \
    'shutdown admission phase finished outcome=Completed' \
    'shutdown drain-support phase finished outcome=Completed' \
    'shutdown terminal-teardown phase finished outcome=Completed' \
    >"${shutdown_log}"

"${restart_verifier}" before "${test_run_id}" "${test_project}" nervix-1 \
    "${test_image_id}" "${inspection_before}" "${inspection_before}"
"${restart_verifier}" stopped "${test_run_id}" "${test_project}" nervix-1 \
    "${test_image_id}" "${inspection_before}" "${inspection_stopped}" "${shutdown_log}"
"${restart_verifier}" started "${test_run_id}" "${test_project}" nervix-1 \
    "${test_image_id}" "${inspection_before}" "${inspection_started}"

expect_restart_failure() {
    local case_name="$1"
    shift
    local status=0
    "${restart_verifier}" "$@" >"${tmp_dir}/restart-failure.txt" 2>&1 || status=$?
    [[ "${status}" -eq 1 ]] || fail "${case_name} returned ${status}, expected failure 1"
}

jq '.[0].Config.Labels["com.docker.compose.service"] = "nervix-2"' \
    "${inspection_before}" >"${tmp_dir}/wrong-target.json"
expect_restart_failure 'wrong target' before "${test_run_id}" "${test_project}" nervix-1 \
    "${test_image_id}" "${inspection_before}" "${tmp_dir}/wrong-target.json"
expect_restart_failure 'ineffective stop' stopped "${test_run_id}" "${test_project}" nervix-1 \
    "${test_image_id}" "${inspection_before}" "${inspection_before}" "${shutdown_log}"
jq '.[0].State.ExitCode = 137' "${inspection_stopped}" >"${tmp_dir}/forced.json"
expect_restart_failure 'forced SIGKILL' stopped "${test_run_id}" "${test_project}" nervix-1 \
    "${test_image_id}" "${inspection_before}" "${tmp_dir}/forced.json" "${shutdown_log}"
printf '%s\n' 'shutdown terminal-teardown phase finished outcome=Forced' \
    >>"${shutdown_log}"
expect_restart_failure 'forced phase' stopped "${test_run_id}" "${test_project}" nervix-1 \
    "${test_image_id}" "${inspection_before}" "${inspection_stopped}" "${shutdown_log}"
jq '.[0].Mounts[0].Name = "other-volume"' \
    "${inspection_started}" >"${tmp_dir}/wrong-volume.json"
expect_restart_failure 'changed volume' started "${test_run_id}" "${test_project}" nervix-1 \
    "${test_image_id}" "${inspection_before}" "${tmp_dir}/wrong-volume.json"

crash_verifier="${chaos_dir}/verify-crash-evidence.sh"
inspection_killed="${tmp_dir}/killed.json"
crash_events="${tmp_dir}/crash-events.ndjson"
jq '.[0].State.Running = false | .[0].State.ExitCode = 137' \
    "${inspection_before}" >"${inspection_killed}"
printf '%s\n' \
    "$(jq -nc --arg id "${test_container_id}" '{Type:"container",Action:"kill",Actor:{ID:$id,Attributes:{signal:"9"}}}')" \
    "$(jq -nc --arg id "${test_container_id}" '{Type:"container",Action:"die",Actor:{ID:$id,Attributes:{exitCode:"137"}}}')" \
    >"${crash_events}"
"${crash_verifier}" before "${test_run_id}" "${test_project}" nervix-1 \
    "${test_image_id}" "${inspection_before}" "${inspection_before}"
"${crash_verifier}" killed "${test_run_id}" "${test_project}" nervix-1 \
    "${test_image_id}" "${inspection_before}" "${inspection_killed}" "${crash_events}"
"${crash_verifier}" started "${test_run_id}" "${test_project}" nervix-1 \
    "${test_image_id}" "${inspection_before}" "${inspection_started}"
"${crash_verifier}" recovered "${test_run_id}" "${test_project}" nervix-1 \
    "${test_image_id}" "${inspection_started}" "${inspection_started}"

expect_crash_failure() {
    local case_name="$1"
    shift
    local status=0
    "${crash_verifier}" "$@" >"${tmp_dir}/crash-failure.txt" 2>&1 || status=$?
    [[ "${status}" -eq 1 ]] || fail "${case_name} returned ${status}, expected failure 1"
}

expect_crash_failure 'ineffective kill' killed "${test_run_id}" "${test_project}" nervix-1 \
    "${test_image_id}" "${inspection_before}" "${inspection_before}" "${crash_events}"
expect_crash_failure 'start kept the crashed process incarnation' started \
    "${test_run_id}" "${test_project}" nervix-1 "${test_image_id}" \
    "${inspection_before}" "${inspection_before}"
expect_crash_failure 'target restarted twice' recovered \
    "${test_run_id}" "${test_project}" nervix-1 "${test_image_id}" \
    "${inspection_started}" "${inspection_before}"
expect_crash_failure 'changed target' before "${test_run_id}" "${test_project}" nervix-2 \
    "${test_image_id}" "${inspection_before}" "${inspection_before}"
printf '%s\n' "$(head -n 1 "${crash_events}")" >"${tmp_dir}/missing-die.ndjson"
expect_crash_failure 'missing die event' killed "${test_run_id}" "${test_project}" nervix-1 \
    "${test_image_id}" "${inspection_before}" "${inspection_killed}" "${tmp_dir}/missing-die.ndjson"
sed 's/"signal":"9"/"signal":"15"/' "${crash_events}" >"${tmp_dir}/wrong-signal.ndjson"
expect_crash_failure 'wrong signal' killed "${test_run_id}" "${test_project}" nervix-1 \
    "${test_image_id}" "${inspection_before}" "${inspection_killed}" "${tmp_dir}/wrong-signal.ndjson"
jq '.[0].State.OOMKilled = true' "${inspection_killed}" >"${tmp_dir}/oom-killed.json"
expect_crash_failure 'OOM kill' killed "${test_run_id}" "${test_project}" nervix-1 \
    "${test_image_id}" "${inspection_before}" "${tmp_dir}/oom-killed.json" "${crash_events}"
jq '.[0].HostConfig.RestartPolicy.Name = "always"' "${inspection_before}" >"${tmp_dir}/auto-restart.json"
expect_crash_failure 'automatic restart' before "${test_run_id}" "${test_project}" nervix-1 \
    "${test_image_id}" "${tmp_dir}/auto-restart.json" "${tmp_dir}/auto-restart.json"

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

cp "${expected}" "${observed}"
head -n 1 "${expected}" >>"${observed}"
expect_verification_failure \
    "strict duplicate" "${expected}" "${observed}" "${result}" \
    '.verdict == "fail" and .duplicate_records == 1 and (.missing_ids | length) == 0'
"${chaos_dir}/verify-ledger.sh" "${expected}" "${observed}" "${result}" --allow-replay-duplicates
jq -e '.verdict == "pass" and .duplicate_records == 1 and .duplicates[0].event_id == "run-0"' \
    "${result}" >/dev/null || fail "an exact replay duplicate was not reported separately"

printf '%s\n' \
    '{"event_id":"run-0","branch_name":"alpha","sequence":0,"content":"corrupt"}' \
    '{"event_id":"run-1","branch_name":"beta","sequence":1,"content":"payload-1"}' \
    >"${observed}"
expect_verification_failure \
    "corrupt record" "${expected}" "${observed}" "${result}" \
    '.verdict == "fail" and (.incorrect_content | length) == 1 and .incorrect_content[0].event_id == "run-0"'

printf '%s\n' \
    '{"event_id":"run-0","branch_name":"beta","sequence":0,"content":"payload-0"}' \
    '{"event_id":"run-1","branch_name":"beta","sequence":1,"content":"payload-1"}' \
    >"${observed}"
expect_verification_failure \
    "wrong branch" "${expected}" "${observed}" "${result}" \
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
grep -Fq 'rolling-restart' <<<"${list_output}" || fail "scenario list omits rolling-restart"
for scenario in leader-crash follower-crash ingestor-owner-crash emitter-owner-crash; do
    grep -Fq "${scenario}" <<<"${list_output}" || fail "scenario list omits ${scenario}"
done

expect_setup_rejection() {
    local case_name="$1" expected="$2"
    shift 2
    local status=0
    "$@" >"${tmp_dir}/setup-rejection.out" 2>&1 || status=$?
    [[ "${status}" -eq 2 ]] || fail "${case_name} returned ${status}, expected setup error 2"
    grep -Fq -- "${expected}" "${tmp_dir}/setup-rejection.out" \
        || fail "${case_name} did not explain the rejected option"
}

expect_setup_rejection 'follower requires three nodes' 'requires --nodes 3' \
    "${chaos_dir}/run-baseline.sh" --scenario follower-crash --image fixture --nodes 1
expect_setup_rejection 'outage is bounded' '--outage-seconds must be an integer from 5 through 120' \
    "${chaos_dir}/run-baseline.sh" --scenario leader-crash --image fixture --outage-seconds 4
expect_setup_rejection 'outage option is parsed' '--nodes must be 1 or 3' \
    "${chaos_dir}/run-baseline.sh" --scenario leader-crash --image fixture --nodes 2 --outage-seconds 8
expect_setup_rejection 'unknown scenario' 'unknown scenario: unknown-crash' \
    "${chaos_dir}/run-baseline.sh" --scenario unknown-crash --image fixture

status=0
"${chaos_dir}/chaos.sh" run rolling-restart >"${tmp_dir}/rolling-missing-image.out" 2>&1 || status=$?
[[ "${status}" -eq 2 ]] || fail "rolling-restart missing --image returned ${status}, expected 2"
grep -Fq -- '--image is required' "${tmp_dir}/rolling-missing-image.out" \
    || fail "rolling-restart missing --image did not report the setup error"

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
sh -n "${chaos_dir}/continuous-load.sh" "${chaos_dir}/observe-nodes.sh"

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
CHAOS_SCRIPT_DIR="${chaos_dir}" \
CHAOS_LOAD_FILE="${expected}" \
CHAOS_TRAFFIC_DIR="${tmp_dir}" \
CHAOS_NODE_COUNT="3" \
    docker compose -f "${chaos_dir}/compose.yaml" --profile tools --profile three-node --profile rolling \
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
    and .services.load.labels["io.nervix.chaos.role"] == "load"
    and .services.observer.labels["io.nervix.chaos.role"] == "observer"
' "${compose_json}" >/dev/null || fail "Compose does not pin every Nervix service or contains a build"

printf 'chaos harness self-test passed\n'
