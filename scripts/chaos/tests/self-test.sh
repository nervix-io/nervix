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
self_test_cleanup() {
    if [[ -n "${cleanup_run_id:-}" ]]; then
        "${chaos_dir}/cleanup.sh" --run-id "${cleanup_run_id}" --quiet >/dev/null 2>&1 || true
    fi
    rm -rf "${tmp_dir}"
}
trap self_test_cleanup EXIT

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

pause_verifier="${chaos_dir}/verify-pause-evidence.sh"
pause_before="${tmp_dir}/pause-before.json"
pause_active="${tmp_dir}/pause-active.json"
pause_resumed="${tmp_dir}/pause-resumed.json"
pause_events="${tmp_dir}/pause-events.ndjson"
pause_result="${tmp_dir}/pause-duration.json"
jq '.[0].Config.Env = [
    "NERVIX_RAFT_HEARTBEAT_INTERVAL=250ms",
    "NERVIX_RAFT_ELECTION_TIMEOUT_MIN=10s",
    "NERVIX_RAFT_ELECTION_TIMEOUT_MAX=12s",
    "NERVIX_NODE_UNAVAILABILITY_TIMEOUT=15s"
] | .[0].State.Paused = false' "${inspection_before}" >"${pause_before}"
jq '.[0].State.Paused = true' "${pause_before}" >"${pause_active}"
cp "${pause_before}" "${pause_resumed}"
printf '%s\n' \
    "$(jq -nc --arg id "${test_container_id}" '{Type:"container",Action:"pause",Actor:{ID:$id},timeNano:1800000000000000000}')" \
    "$(jq -nc --arg id "${test_container_id}" '{Type:"container",Action:"unpause",Actor:{ID:$id},timeNano:1800000006000000000}')" \
    >"${pause_events}"
"${pause_verifier}" before "${test_run_id}" "${test_project}" nervix-1 \
    "${test_image_id}" "${pause_before}" "${pause_before}"
"${pause_verifier}" paused "${test_run_id}" "${test_project}" nervix-1 \
    "${test_image_id}" "${pause_before}" "${pause_active}"
"${pause_verifier}" resumed "${test_run_id}" "${test_project}" nervix-1 \
    "${test_image_id}" "${pause_before}" "${pause_resumed}" \
    "${pause_events}" 5000 9999 "${pause_result}"
jq -e '.pause_verified == true and .unpause_verified == true and (.actual_pause_ms - 6000 | fabs) < 1' \
    "${pause_result}" >/dev/null || fail 'verified pause duration was incorrect'
printf '%s\n' \
    "$(jq -nc --arg id "${test_container_id}" '{Type:"container",Action:"pause",Actor:{ID:$id},timeNano:1800000000000000000}')" \
    "$(jq -nc --arg id "${test_container_id}" '{Type:"container",Action:"unpause",Actor:{ID:$id},timeNano:1800000001000000000}')" \
    >"${tmp_dir}/short-pause-events.ndjson"
"${pause_verifier}" resumed "${test_run_id}" "${test_project}" nervix-1 \
    "${test_image_id}" "${pause_before}" "${pause_resumed}" \
    "${tmp_dir}/short-pause-events.ndjson" 800 2000 "${tmp_dir}/short-pause-duration.json"
jq -e '.actual_pause_ms >= 800 and .actual_pause_ms <= 2000' \
    "${tmp_dir}/short-pause-duration.json" >/dev/null \
    || fail 'short pause duration missed its intended window'

expect_pause_failure() {
    local case_name="$1"
    shift
    local status=0
    "${pause_verifier}" "$@" >"${tmp_dir}/pause-failure.txt" 2>&1 || status=$?
    [[ "${status}" -eq 1 ]] || fail "${case_name} returned ${status}, expected failure 1"
}

expect_pause_failure 'no paused state' paused "${test_run_id}" "${test_project}" nervix-1 \
    "${test_image_id}" "${pause_before}" "${pause_resumed}"
expect_pause_failure 'still paused after injector' resumed "${test_run_id}" "${test_project}" nervix-1 \
    "${test_image_id}" "${pause_before}" "${pause_active}" \
    "${pause_events}" 5000 9999 "${pause_result}"
expect_pause_failure 'missed long fault window' resumed "${test_run_id}" "${test_project}" nervix-1 \
    "${test_image_id}" "${pause_before}" "${pause_resumed}" \
    "${pause_events}" 15001 60000 "${pause_result}"
expect_pause_failure 'missed short fault window' resumed "${test_run_id}" "${test_project}" nervix-1 \
    "${test_image_id}" "${pause_before}" "${pause_resumed}" \
    "${pause_events}" 800 2000 "${pause_result}"
jq '.[0].State.StartedAt = "2026-09-27T00:01:00Z"' \
    "${pause_resumed}" >"${tmp_dir}/pause-restarted.json"
expect_pause_failure 'restarted paused process' resumed "${test_run_id}" "${test_project}" nervix-1 \
    "${test_image_id}" "${pause_before}" "${tmp_dir}/pause-restarted.json" \
    "${pause_events}" 5000 9999 "${pause_result}"
sed 's/"Action":"unpause"/"Action":"stop"/' "${pause_events}" \
    >"${tmp_dir}/pause-missing-unpause.ndjson"
expect_pause_failure 'missing unpause event' resumed "${test_run_id}" "${test_project}" nervix-1 \
    "${test_image_id}" "${pause_before}" "${pause_resumed}" \
    "${tmp_dir}/pause-missing-unpause.ndjson" 5000 9999 "${pause_result}"
jq '.[0].Config.Env |= map(select(. != "NERVIX_NODE_UNAVAILABILITY_TIMEOUT=15s"))' \
    "${pause_before}" >"${tmp_dir}/pause-wrong-threshold.json"
expect_pause_failure 'wrong configured threshold' before "${test_run_id}" "${test_project}" nervix-1 \
    "${test_image_id}" "${tmp_dir}/pause-wrong-threshold.json" "${tmp_dir}/pause-wrong-threshold.json"

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
grep -Fq 'pause-resume' <<<"${list_output}" || fail 'scenario list omits pause-resume'

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
expect_setup_rejection 'pause requires three nodes' 'requires --nodes 3' \
    "${chaos_dir}/run-baseline.sh" --scenario pause-resume --image fixture --nodes 1
expect_setup_rejection 'pause rejects crash outage' '--outage-seconds is for crash scenarios' \
    "${chaos_dir}/run-baseline.sh" --scenario pause-resume --image fixture --outage-seconds 8
grep -Fq 'partition-recovery' <<<"${list_output}" || fail 'scenario list omits partition-recovery'
for partition_case in follower asymmetric leader quorum-loss; do
    grep -Fq "${partition_case}" <<<"${list_output}" || fail "scenario list omits partition case ${partition_case}"
done
expect_setup_rejection 'partition requires three nodes' 'requires --nodes 3' \
    "${chaos_dir}/run-baseline.sh" --scenario partition-recovery --image fixture --nodes 1
expect_setup_rejection 'partition case is validated' '--case must be all, follower, asymmetric, leader, or quorum-loss' \
    "${chaos_dir}/run-baseline.sh" --scenario partition-recovery --image fixture --case sideways
expect_setup_rejection 'partition window is bounded' '--partition-seconds must be an integer from 20 through 600' \
    "${chaos_dir}/run-baseline.sh" --scenario partition-recovery --image fixture --partition-seconds 5
expect_setup_rejection 'partition options need the partition scenario' 'apply only to partition-recovery' \
    "${chaos_dir}/run-baseline.sh" --scenario leader-crash --image fixture --case leader
expect_setup_rejection 'partition rejects crash outage' '--outage-seconds is for crash scenarios' \
    "${chaos_dir}/run-baseline.sh" --scenario partition-recovery --image fixture --outage-seconds 8
status=0
"${chaos_dir}/chaos.sh" run partition-recovery >"${tmp_dir}/partition-missing-image.out" 2>&1 || status=$?
[[ "${status}" -eq 2 ]] || fail "partition-recovery missing --image returned ${status}, expected 2"

subnet_selector="${chaos_dir}/select-subnet.sh"
selected_subnet="$("${subnet_selector}" self-test </dev/null)"
[[ "${selected_subnet}" =~ ^10\.213\.[0-9]+\.0/24$ ]] || fail "subnet selector chose ${selected_subnet}"
next_subnet="$(printf '%s\n' "${selected_subnet}" 172.17.0.0/16 fd00::/64 | "${subnet_selector}" self-test)"
[[ "${next_subnet}" != "${selected_subnet}" && "${next_subnet}" =~ ^10\.213\.[0-9]+\.0/24$ ]] \
    || fail 'subnet selector reused a network already in use'
printf '%s\n' "${selected_subnet%.0/24}.200" | "${subnet_selector}" self-test >"${tmp_dir}/subnet.txt"
[[ "$(<"${tmp_dir}/subnet.txt")" != "${selected_subnet}" ]] \
    || fail 'subnet selector chose a network containing a used address'
status=0
printf '10.213.0.0/16\n' | "${subnet_selector}" self-test >/dev/null || status=$?
[[ "${status}" -eq 1 ]] || fail "exhausted subnet space returned ${status}, expected 1"

partition_verifier="${chaos_dir}/verify-partition-evidence.sh"
partition_plan="${tmp_dir}/partition-plan.json"
jq -n '{case:"leader",placement:"peers",isolated:["nervix-1"],
    addresses:{"nervix-1":"10.213.7.11","nervix-2":"10.213.7.12","nervix-3":"10.213.7.13"},
    intended_blocked:["nervix-1>nervix-2","nervix-2>nervix-1","nervix-1>nervix-3","nervix-3>nervix-1"],
    rules:{"nervix-1":{netem_targets:[],iptables_sources:[]},
           "nervix-2":{netem_targets:["10.213.7.11"],iptables_sources:["10.213.7.11"]},
           "nervix-3":{netem_targets:["10.213.7.11"],iptables_sources:["10.213.7.11"]}}}' \
    >"${partition_plan}"
"${partition_verifier}" plan "${partition_plan}"
jq '.rules["nervix-3"].iptables_sources = []' "${partition_plan}" >"${tmp_dir}/one-way-plan.json"
expect_partition_failure() {
    local case_name="$1"
    shift
    local status=0
    "${partition_verifier}" "$@" >"${tmp_dir}/partition-failure.txt" 2>&1 || status=$?
    [[ "${status}" -eq 1 ]] || fail "${case_name} returned ${status}, expected failure 1"
}
expect_partition_failure 'rules leave an intended link open' plan "${tmp_dir}/one-way-plan.json"

installed_rules="${tmp_dir}/installed-rules.txt"
cat >"${installed_rules}" <<'EOF'
# qdisc
qdisc prio 504d: root refcnt 33 bands 3 priomap 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1
qdisc sfq 504e: parent 504d:1 limit 127p quantum 1514b depth 127 divisor 1024
qdisc netem 5050: parent 504d:3 limit 1000 loss 100% seed 16883030988861588660
qdisc sfq 504f: parent 504d:2 limit 127p quantum 1514b depth 127 divisor 1024
# filter
filter protocol ip pref 1 u32 chain 0
filter protocol ip pref 1 u32 chain 0 fh 800: ht divisor 1
filter protocol ip pref 1 u32 chain 0 fh 800::800 order 2048 key ht 800 bkt 0 flowid 504d:3 not_in_hw
  match 0ad5070b/ffffffff at 16
# iptables
-P INPUT ACCEPT
-A INPUT -s 10.213.7.11/32 -i eth0 -m statistic --mode random --probability 1.00000000000 -j DROP
EOF
default_rules="${tmp_dir}/default-rules.txt"
printf '%s\n' '# qdisc' 'qdisc noqueue 0: root refcnt 2' '# filter' '# iptables' '-P INPUT ACCEPT' \
    >"${default_rules}"
"${partition_verifier}" rules "${partition_plan}" nervix-2 "${installed_rules}"
"${partition_verifier}" rules "${partition_plan}" nervix-1 "${default_rules}"
"${partition_verifier}" rules "${partition_plan}" nervix-2 "${default_rules}" --healed
expect_partition_failure 'missing peer rules' rules "${partition_plan}" nervix-2 "${default_rules}"
expect_partition_failure 'rules on the isolated node' rules "${partition_plan}" nervix-1 "${installed_rules}"
expect_partition_failure 'rules left after healing' rules "${partition_plan}" nervix-2 "${installed_rules}" --healed
grep -v '^-A INPUT' "${installed_rules}" >"${tmp_dir}/partial-rules.txt"
expect_partition_failure 'partial installation' rules "${partition_plan}" nervix-2 "${tmp_dir}/partial-rules.txt"
sed 's/0ad5070b/0ad5070c/' "${installed_rules}" >"${tmp_dir}/wrong-target-rules.txt"
expect_partition_failure 'wrong netem target' rules "${partition_plan}" nervix-2 "${tmp_dir}/wrong-target-rules.txt"
sed 's/^qdisc noqueue 0: root refcnt 2$/qdisc netem 1: root refcnt 2 limit 1000 delay 1ms/' "${default_rules}" \
    >"${tmp_dir}/foreign-rules.txt"
expect_partition_failure 'foreign qdisc before installation' rules "${partition_plan}" nervix-2 \
    "${tmp_dir}/foreign-rules.txt" --healed

isolated_links="${tmp_dir}/isolated-links.json"
jq -n '{links: [
    {from:"nervix-1",to:"nervix-2",sent:3,received:0},{from:"nervix-1",to:"nervix-3",sent:3,received:0},
    {from:"nervix-2",to:"nervix-1",sent:3,received:0},{from:"nervix-2",to:"nervix-3",sent:3,received:3},
    {from:"nervix-3",to:"nervix-1",sent:3,received:0},{from:"nervix-3",to:"nervix-2",sent:3,received:3},
    {from:"verifier",to:"nervix-1",sent:3,received:3},{from:"verifier",to:"nervix-2",sent:3,received:3},
    {from:"verifier",to:"nervix-3",sent:3,received:3},{from:"nervix-1",to:"broker",sent:3,received:3},
    {from:"nervix-2",to:"broker",sent:3,received:3},{from:"nervix-3",to:"broker",sent:3,received:3}]}' \
    >"${isolated_links}"
"${partition_verifier}" links "${partition_plan}" "${isolated_links}" "${tmp_dir}/links-verdict.json"
jq -e '.verdict == "pass"' "${tmp_dir}/links-verdict.json" >/dev/null || fail 'isolated links did not pass'
expect_partition_failure 'links still isolated after healing' links "${partition_plan}" "${isolated_links}" \
    "${tmp_dir}/links-verdict.json" --healed
jq '.links |= map(if .from == "nervix-2" and .to == "nervix-1" then .received = 1 else . end)' \
    "${isolated_links}" >"${tmp_dir}/leaking-links.json"
expect_partition_failure 'successful Pumba exit without effective isolation' links "${partition_plan}" \
    "${tmp_dir}/leaking-links.json" "${tmp_dir}/links-verdict.json"
jq -e '.mismatches == [{link:"nervix-2>nervix-1",expected:"blocked",observed:"open",received:1}]' \
    "${tmp_dir}/links-verdict.json" >/dev/null || fail 'leaking link was not reported'
jq '.links |= map(if .from == "verifier" and .to == "nervix-1" then .received = 0 else . end)' \
    "${isolated_links}" >"${tmp_dir}/verifier-cut-links.json"
expect_partition_failure 'fault reached the verifier route' links "${partition_plan}" \
    "${tmp_dir}/verifier-cut-links.json" "${tmp_dir}/links-verdict.json"
jq '.links |= map(select(.to != "broker"))' "${isolated_links}" >"${tmp_dir}/unobserved-links.json"
expect_partition_failure 'unobserved broker links' links "${partition_plan}" \
    "${tmp_dir}/unobserved-links.json" "${tmp_dir}/links-verdict.json"

status=0
"${chaos_dir}/chaos.sh" run rolling-restart >"${tmp_dir}/rolling-missing-image.out" 2>&1 || status=$?
[[ "${status}" -eq 2 ]] || fail "rolling-restart missing --image returned ${status}, expected 2"
grep -Fq -- '--image is required' "${tmp_dir}/rolling-missing-image.out" \
    || fail "rolling-restart missing --image did not report the setup error"
status=0
"${chaos_dir}/chaos.sh" run pause-resume >"${tmp_dir}/pause-missing-image.out" 2>&1 || status=$?
[[ "${status}" -eq 2 ]] || fail "pause-resume missing --image returned ${status}, expected 2"

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
CHAOS_SUBNET="10.213.7.0/24" \
CHAOS_DYNAMIC_RANGE="10.213.7.128/25" \
CHAOS_NODE_1_ADDRESS="10.213.7.11" \
CHAOS_NODE_2_ADDRESS="10.213.7.12" \
CHAOS_NODE_3_ADDRESS="10.213.7.13" \
    docker compose -f "${chaos_dir}/compose.yaml" --profile tools --profile three-node --profile rolling \
    config --format json \
    >"${compose_json}"

jq -e --arg image "${placeholder_image}" \
    --arg election_min "${CHAOS_RAFT_ELECTION_TIMEOUT_MIN:-1500ms}" \
    --arg node_timeout "${CHAOS_NODE_UNAVAILABILITY_TIMEOUT:-10s}" '
    ([.services.admin, .services["nervix-1"], .services["nervix-2"], .services["nervix-3"]]
      | all(.image == $image))
    and ([.services[]] | all(has("build") | not))
    and ([.services | to_entries[] | select(.key | test("^nervix-[123]$")) | .value]
         | length == 3)
    and ([.services | to_entries[] | select(.key | test("^nervix-[123]$")) | .value]
         | all(.labels["io.nervix.chaos.run"] == "self-test"))
    and .services.load.labels["io.nervix.chaos.role"] == "load"
    and .services.observer.labels["io.nervix.chaos.role"] == "observer"
    and .services["nervix-1"].environment.NERVIX_RAFT_ELECTION_TIMEOUT_MIN == $election_min
    and .services["nervix-1"].environment.NERVIX_NODE_UNAVAILABILITY_TIMEOUT == $node_timeout
' "${compose_json}" >/dev/null || fail "Compose does not pin every Nervix service or contains a build"

# Peer-side partition rules follow a node across its restart only if its address stays fixed, and
# no other container may be handed a node address while that node is stopped.
jq -e '
    .networks.chaos.ipam.config == [{subnet: "10.213.7.0/24", ip_range: "10.213.7.128/25"}]
    and .services["nervix-1"].networks.chaos.ipv4_address == "10.213.7.11"
    and .services["nervix-2"].networks.chaos.ipv4_address == "10.213.7.12"
    and .services["nervix-3"].networks.chaos.ipv4_address == "10.213.7.13"
    and ([.services | to_entries[] | select(.key | test("^nervix-[123]$") | not)
          | .value.networks // {} | to_entries[] | .value.ipv4_address // empty] | length == 0)
' "${compose_json}" >/dev/null || fail "Compose does not fix node addresses outside the dynamic range"

# A controller killed while Pumba owns the pause cannot run its EXIT trap.
# The external cleanup path must unpause that exact labeled node before removal.
cleanup_run_id="pause-cleanup-$$-${RANDOM}"
cleanup_container="$(docker run --detach --rm \
    --label "io.nervix.chaos.run=${cleanup_run_id}" \
    --label io.nervix.chaos.role=node alpine:3.22 sleep 60)"
docker pause "${cleanup_container}" >/dev/null
[[ "$(docker inspect --format '{{.State.Paused}}' "${cleanup_container}")" == true ]] \
    || fail 'cleanup exercise did not pause its run-owned target'
"${chaos_dir}/cleanup.sh" --run-id "${cleanup_run_id}" --quiet
if docker inspect "${cleanup_container}" >/dev/null 2>&1; then
    fail 'external cleanup left the paused target behind'
fi

cleanup_run_id="pause-injector-failure-$$-${RANDOM}"
cleanup_container="$(docker run --detach --rm \
    --label "io.nervix.chaos.run=${cleanup_run_id}" \
    --label io.nervix.chaos.role=node alpine:3.22 sleep 60)"
docker pause "${cleanup_container}" >/dev/null
injector_status=0
docker run --rm \
    --label "io.nervix.chaos.run=${cleanup_run_id}" \
    --label io.nervix.chaos.role=fault alpine:3.22 false \
    >"${tmp_dir}/injector-failure.txt" 2>&1 || injector_status=$?
[[ "${injector_status}" -ne 0 ]] || fail 'injector-failure exercise did not fail'
"${chaos_dir}/cleanup.sh" --run-id "${cleanup_run_id}" --quiet
if docker inspect "${cleanup_container}" >/dev/null 2>&1; then
    fail 'injector-failure cleanup left the paused target behind'
fi

# An injector killed with SIGKILL cannot remove its own rules. Healing must remove exactly the
# rules Pumba owns, together with any sidecar left joined to a run-owned namespace.
# shellcheck source=../tool-images.sh
source "${chaos_dir}/tool-images.sh"
network_faults="${chaos_dir}/network-faults.sh"
cleanup_run_id="network-heal-$$-${RANDOM}"
heal_target="$(docker run --detach --rm \
    --label "io.nervix.chaos.run=${cleanup_run_id}" \
    --label io.nervix.chaos.role=node alpine:3.22 sleep 120)"
heal_target_name="$(docker inspect --format '{{.Name}}' "${heal_target}")"
heal_injectors=()
for fault in "netem --duration 120s --tc-image ${chaos_nettools_image} --target 192.0.2.1 loss --percent 100" \
    "iptables --duration 120s --iptables-image ${chaos_nettools_image} --source 192.0.2.1 loss --probability 1.0"; do
    # shellcheck disable=SC2086
    heal_injectors+=("$(docker run --detach \
        --label "io.nervix.chaos.run=${cleanup_run_id}" \
        --label io.nervix.chaos.role=fault \
        --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        "${chaos_pumba_image}" --label "io.nervix.chaos.run=${cleanup_run_id}" \
        ${fault} "${heal_target_name#/}")")
done
installed=false
for _ in $(seq 1 30); do
    "${network_faults}" inspect --run-id "${cleanup_run_id}" --container "${heal_target}" \
        --nettools "${chaos_nettools_image}" --output "${tmp_dir}/heal-installed.txt"
    if grep -q '^qdisc prio 504d: root' "${tmp_dir}/heal-installed.txt" \
        && grep -q '^-A INPUT -s 192.0.2.1/32' "${tmp_dir}/heal-installed.txt"; then
        installed=true
        break
    fi
    sleep 1
done
[[ "${installed}" == true ]] || fail 'the healing exercise could not install Pumba faults'
docker kill "${heal_injectors[@]}" >/dev/null
heal_sidecar="$(docker run --detach --label com.gaiaadm.pumba.skip=true \
    --network "container:${heal_target}" alpine:3.22 sleep 120)"
"${network_faults}" heal --run-id "${cleanup_run_id}" --nettools "${chaos_nettools_image}" \
    --output "${tmp_dir}/heal-report.txt" || fail 'healing did not clear faults left by a killed injector'
grep -Fq 'removed Pumba root qdisc 504d:' "${tmp_dir}/heal-report.txt" \
    && grep -Fq 'removed Pumba INPUT rule' "${tmp_dir}/heal-report.txt" \
    || fail 'healing did not report the owned rules it removed'
if docker inspect "${heal_sidecar}" >/dev/null 2>&1; then
    fail 'healing left a Pumba sidecar joined to a run-owned namespace'
fi
"${network_faults}" inspect --run-id "${cleanup_run_id}" --container "${heal_target}" \
    --nettools "${chaos_nettools_image}" --output "${tmp_dir}/heal-final.txt"
"${partition_verifier}" rules "${partition_plan}" nervix-1 "${tmp_dir}/heal-final.txt" \
    || fail 'healing left the namespace in a state other than its default'
docker rm --force "${heal_injectors[@]}" >/dev/null
"${chaos_dir}/cleanup.sh" --run-id "${cleanup_run_id}" --quiet

# A qdisc Pumba does not own is never removed; healing reports it instead.
cleanup_run_id="network-foreign-$$-${RANDOM}"
foreign_target="$(docker run --detach --rm \
    --label "io.nervix.chaos.run=${cleanup_run_id}" \
    --label io.nervix.chaos.role=node alpine:3.22 sleep 120)"
docker run --rm --cap-add NET_ADMIN --network "container:${foreign_target}" --entrypoint sh \
    "${chaos_nettools_image}" -ec 'tc qdisc add dev eth0 root handle 1: netem delay 1ms' >/dev/null
status=0
"${network_faults}" heal --run-id "${cleanup_run_id}" --nettools "${chaos_nettools_image}" \
    --output "${tmp_dir}/foreign-report.txt" || status=$?
[[ "${status}" -eq 1 ]] || fail "healing a foreign qdisc returned ${status}, expected 1"
"${network_faults}" inspect --run-id "${cleanup_run_id}" --container "${foreign_target}" \
    --nettools "${chaos_nettools_image}" --output "${tmp_dir}/foreign-final.txt"
grep -q '^qdisc netem 1: root' "${tmp_dir}/foreign-final.txt" \
    || fail 'healing removed a qdisc Pumba does not own'

# External cleanup also removes a Pumba sidecar joined to a run-owned container.
foreign_sidecar="$(docker run --detach --label com.gaiaadm.pumba.skip=true \
    --network "container:${foreign_target}" alpine:3.22 sleep 120)"
"${chaos_dir}/cleanup.sh" --run-id "${cleanup_run_id}" --quiet
if docker inspect "${foreign_sidecar}" >/dev/null 2>&1 || docker inspect "${foreign_target}" >/dev/null 2>&1; then
    fail 'external cleanup left a run-owned container or its Pumba sidecar'
fi

printf 'chaos harness self-test passed\n'
