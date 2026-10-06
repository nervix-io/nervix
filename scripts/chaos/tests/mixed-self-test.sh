#!/usr/bin/env bash
set -euo pipefail

# Self-test of mixed-instability and replay: seeded plan generation and its determinism, every
# reason plan validation refuses a plan, the option and replay checks that fail before a run alters a
# container, logical node resolution, and the action trace, coverage and resource verdicts.

chaos_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../tool-images.sh
source "${chaos_dir}/tool-images.sh"
plans="${chaos_dir}/mixed-plan.sh"
mixed_evidence="${chaos_dir}/verify-mixed-evidence.sh"
tmp_dir="$(mktemp -d)"
mixed_self_test_cleanup() {
    local run
    for run in ${owned_runs[@]+"${owned_runs[@]}"}; do
        "${chaos_dir}/cleanup.sh" --run-id "${run}" --quiet >/dev/null 2>&1 || true
    done
    rm -rf "${tmp_dir}"
}
owned_runs=()
trap mixed_self_test_cleanup EXIT

fail() {
    printf 'mixed-instability self-test failed: %s\n' "$*" >&2
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
        || fail "${case_name} did not explain the rejection: $(cat "${tmp_dir}/setup-rejection.out")"
}

# Validates a plan that must be refused with REASON among its reasons.
expect_invalid_plan() {
    local case_name="$1"
    local reason="$2"
    local plan="$3"
    local status=0
    "${plans}" validate --plan "${plan}" --report "${tmp_dir}/report.json" >"${tmp_dir}/invalid.out" 2>&1 || status=$?
    [[ "${status}" -eq 1 ]] || fail "${case_name} returned ${status}, expected an invalid plan"
    jq -e --arg reason "${reason}" '.verdict == "invalid" and any(.reasons[]; contains($reason))' \
        "${tmp_dir}/report.json" >/dev/null \
        || fail "${case_name} did not report '${reason}': $(jq -c '.reasons' "${tmp_dir}/report.json")"
}

# Writes a plan of the given STEPS under POLICY that requires COVERAGE.
plan_with() {
    local output="$1"
    local policy="$2"
    local coverage="$3"
    local steps="$4"
    jq -n --arg policy "${policy}" --argjson coverage "${coverage}" --argjson steps "${steps}" \
        '{seed: null, policy: $policy, nodes: 3, duration_seconds: 900, coverage: $coverage, steps: $steps}' \
        >"${output}"
}

step_with() {
    local kind="$1"
    local role="$2"
    local actions="$3"
    jq -nc --arg kind "${kind}" --arg role "${role}" --argjson actions "${actions}" \
        '{index: 1, kind: $kind, role: $role, pick: 0, at_seconds: 0, estimated_seconds: 200, actions: $actions}'
}

list_output="$("${chaos_dir}/chaos.sh" list)"
grep -Fq 'mixed-instability' <<<"${list_output}" || fail 'scenario list omits mixed-instability'
help_output="$("${chaos_dir}/chaos.sh" run mixed-instability --help)"
for option in --seed --duration --policy --coverage --plan; do
    grep -Fq -- "${option}" <<<"${help_output}" || fail "the run help omits ${option}"
done

# A seed selects one finite plan: the same inputs select it again, another seed selects another, and
# every plan the generator writes validates and plans every required item.
preserve_coverage="$("${plans}" default-coverage --policy preserve-quorum)"
temporary_coverage="$("${plans}" default-coverage --policy temporary-quorum-loss)"
[[ "${preserve_coverage}" == degrade:follower,degrade:leader,kill:follower,kill:leader,partition:follower,partition:leader,pause:follower,pause:leader,stop:follower,stop:leader ]] \
    || fail "the default preserve-quorum coverage is ${preserve_coverage}"
[[ "${temporary_coverage}" == degrade:follower,degrade:leader,kill:follower,kill:leader,partition:follower,partition:leader,pause:follower,pause:leader,quorum-loss,stop:follower,stop:leader ]] \
    || fail "the default temporary-quorum-loss coverage is ${temporary_coverage}"
"${plans}" generate --seed 42 --duration 1800 --policy preserve-quorum --coverage "${preserve_coverage}" \
    --output "${tmp_dir}/seed-42.json"
"${plans}" generate --seed 42 --duration 1800 --policy preserve-quorum --coverage "${preserve_coverage}" \
    --output "${tmp_dir}/seed-42-again.json"
cmp -s "${tmp_dir}/seed-42.json" "${tmp_dir}/seed-42-again.json" || fail 'seed 42 selected two different plans'
"${plans}" generate --seed 43 --duration 1800 --policy preserve-quorum --coverage "${preserve_coverage}" \
    --output "${tmp_dir}/seed-43.json"
! cmp -s "${tmp_dir}/seed-42.json" "${tmp_dir}/seed-43.json" || fail 'seeds 42 and 43 selected the same plan'
"${plans}" generate --seed 7 --duration 2400 --policy temporary-quorum-loss --coverage "${temporary_coverage}" \
    --output "${tmp_dir}/temporary.json"
for plan in seed-42 seed-43 temporary; do
    "${plans}" validate --plan "${tmp_dir}/${plan}.json" || fail "generated plan ${plan} is invalid"
    jq -e '.steps[-1].at_seconds + .steps[-1].estimated_seconds <= .duration_seconds
           and ([.steps[].actions[].id] | unique | length) == ([.steps[].actions[].id] | length)' \
        "${tmp_dir}/${plan}.json" >/dev/null || fail "generated plan ${plan} does not fit its timeline"
done
jq -e '.seed == 7 and .policy == "temporary-quorum-loss" and (.coverage | index("quorum-loss")) != null
       and any(.steps[]; .kind == "quorum-loss" or .kind == "double-outage" or .kind == "cluster-restart")' \
    "${tmp_dir}/temporary.json" >/dev/null || fail 'a temporary-quorum-loss plan planned no quorum loss'
jq -e '[.steps[].kind] | all(. != "quorum-loss" and . != "double-outage" and . != "cluster-restart")' \
    "${tmp_dir}/seed-42.json" >/dev/null || fail 'a preserve-quorum plan planned a quorum loss'
"${plans}" generate --seed 3 --duration 900 --policy temporary-quorum-loss --coverage kill:cluster,partition:cluster \
    --output "${tmp_dir}/cluster.json"
"${plans}" validate --plan "${tmp_dir}/cluster.json" || fail 'a plan of whole-cluster faults is invalid'
jq -e '[.steps[] | select(.role == "cluster") | .kind] | (index("cluster-restart") != null and index("quorum-loss") != null)' \
    "${tmp_dir}/cluster.json" >/dev/null || fail 'whole-cluster coverage did not plan a cluster restart and a quorum-loss partition'

status=0
"${plans}" generate --seed 1 --duration 300 --policy preserve-quorum --coverage "${preserve_coverage}" \
    --output "${tmp_dir}/short.json" 2>"${tmp_dir}/short.out" || status=$?
if [[ "${status}" -ne 2 ]] || ! grep -Fq 'raise --duration or narrow --coverage' "${tmp_dir}/short.out"; then
    fail 'a duration shorter than the required coverage was not refused'
fi
for refused in 'quorum-loss' 'stop:cluster' 'kill:primary' ''; do
    status=0
    "${plans}" generate --seed 1 --duration 900 --policy preserve-quorum --coverage "${refused}" \
        --output "${tmp_dir}/refused.json" >/dev/null 2>&1 || status=$?
    [[ "${status}" -eq 2 ]] || fail "coverage '${refused}' returned ${status}, expected 2"
done

# Plan validation refuses every combination the runner could not inject safely.
kill_target='{"id":"1a","family":"kill","node":"target","start_seconds":0,"hold_seconds":30}'
plan_with "${tmp_dir}/two-network.json" preserve-quorum '["partition:leader"]' "[$(step_with isolate leader '[
    {"id":"1a","family":"partition","partition":"isolate","node":"target","start_seconds":0,"hold_seconds":40},
    {"id":"1b","family":"degrade","node":"peer","to":"other","profile":"delay","start_seconds":5,"hold_seconds":20}]')]"
expect_invalid_plan 'overlapping network faults' 'conflicting interface rules' "${tmp_dir}/two-network.json"
plan_with "${tmp_dir}/rule-carrier.json" temporary-quorum-loss '["partition:leader"]' "[$(step_with isolate leader '[
    {"id":"1a","family":"partition","partition":"isolate","node":"target","start_seconds":0,"hold_seconds":40},
    {"id":"1b","family":"kill","node":"peer","start_seconds":5,"hold_seconds":10}]')]"
expect_invalid_plan 'a node fault on a rule carrier' 'targets a node that carries the rules of 1a' "${tmp_dir}/rule-carrier.json"
plan_with "${tmp_dir}/double.json" preserve-quorum '["kill:leader"]' "[$(step_with double-outage leader "[${kill_target},
    {\"id\":\"1b\",\"family\":\"pause\",\"node\":\"peer\",\"start_seconds\":5,\"hold_seconds\":10}]")]"
expect_invalid_plan 'a double outage under preserve-quorum' 'which preserve-quorum refuses' "${tmp_dir}/double.json"
plan_with "${tmp_dir}/quorum-loss.json" preserve-quorum '["partition:leader"]' "[$(step_with quorum-loss cluster '[
    {"id":"1a","family":"partition","partition":"quorum-loss","node":"cluster","start_seconds":0,"hold_seconds":30}]')]"
expect_invalid_plan 'a quorum-loss partition under preserve-quorum' 'which preserve-quorum refuses' "${tmp_dir}/quorum-loss.json"
plan_with "${tmp_dir}/long-loss.json" temporary-quorum-loss '["quorum-loss"]' "[$(step_with quorum-loss cluster '[
    {"id":"1a","family":"partition","partition":"quorum-loss","node":"cluster","start_seconds":0,"hold_seconds":300}]')]"
expect_invalid_plan 'a quorum loss that is not temporary' 'a temporary quorum loss lasts at most 120 s' "${tmp_dir}/long-loss.json"
plan_with "${tmp_dir}/heal-during-outage.json" preserve-quorum '["kill:leader"]' "[$(step_with outage-under-degrade leader "[
    {\"id\":\"1a\",\"family\":\"degrade\",\"node\":\"peer\",\"to\":\"other\",\"profile\":\"delay\",\"start_seconds\":0,\"hold_seconds\":20},
    {\"id\":\"1b\",\"family\":\"kill\",\"node\":\"target\",\"start_seconds\":5,\"hold_seconds\":30}]")]"
expect_invalid_plan 'a network heal during a node outage' 'would be healed while node fault 1b' "${tmp_dir}/heal-during-outage.json"
plan_with "${tmp_dir}/uncovered.json" preserve-quorum '["kill:leader","pause:follower"]' "[$(step_with kill leader "[${kill_target}]")]"
expect_invalid_plan 'an unplanned coverage item' 'coverage item pause:follower is required but no action' "${tmp_dir}/uncovered.json"
plan_with "${tmp_dir}/hold.json" preserve-quorum '["kill:leader"]' "[$(step_with kill leader \
    '[{"id":"1a","family":"kill","node":"target","start_seconds":0,"hold_seconds":500}]')]"
expect_invalid_plan 'a hold outside its bounds' 'outside 5..120 s' "${tmp_dir}/hold.json"
plan_with "${tmp_dir}/cluster.json" temporary-quorum-loss '["kill:leader"]' "[$(step_with kill leader \
    '[{"id":"1a","family":"kill","node":"cluster","start_seconds":0,"hold_seconds":10}]')]"
expect_invalid_plan 'a cluster action in a role step' 'only cluster steps name the cluster' "${tmp_dir}/cluster.json"
plan_with "${tmp_dir}/policy.json" preserve-quorum '["quorum-loss"]' "[$(step_with kill leader "[${kill_target}]")]"
expect_invalid_plan 'a quorum-loss item under preserve-quorum' 'needs the temporary-quorum-loss policy' "${tmp_dir}/policy.json"
plan_with "${tmp_dir}/stray.json" preserve-quorum '["kill:leader"]' "[$(step_with kill leader \
    '[{"id":"1a","family":"kill","partition":"isolate","node":"target","start_seconds":0,"hold_seconds":30}]')]"
expect_invalid_plan 'a field another family takes' 'has partition, which its family does not take' "${tmp_dir}/stray.json"
plan_with "${tmp_dir}/id.json" preserve-quorum '["kill:leader"]' "[$(step_with kill leader \
    '[{"id":"1-a","family":"kill","node":"target","start_seconds":0,"hold_seconds":30}]')]"
expect_invalid_plan 'an id no canary can carry' 'needs an id of lowercase letters and digits' "${tmp_dir}/id.json"
plan_with "${tmp_dir}/estimate.json" preserve-quorum '["kill:leader"]' \
    "[$(step_with kill leader "[${kill_target}]" | jq -c '.estimated_seconds = 100')]"
expect_invalid_plan 'an estimate shorter than its step' 'needs 80 s beyond its holds, so at least 110 s' \
    "${tmp_dir}/estimate.json"
plan_with "${tmp_dir}/apart.json" temporary-quorum-loss '["quorum-loss"]' "[$(step_with double-outage leader '[
    {"id":"1a","family":"kill","node":"target","start_seconds":0,"hold_seconds":10},
    {"id":"1b","family":"kill","node":"peer","start_seconds":30,"hold_seconds":10}]')]"
expect_invalid_plan 'outages that never overlap' 'coverage item quorum-loss is required but no action' \
    "${tmp_dir}/apart.json"
# One-way loss takes out whichever endpoint does not lead: the sender when a follower drops what it
# sends the leader, so a crash of that leader leaves two voters out.
one_way_then_kill='[{"id":"1a","family":"partition","partition":"one-way","node":"target","to":"peer","start_seconds":0,"hold_seconds":40},
    {"id":"1b","family":"kill","node":"peer","start_seconds":5,"hold_seconds":10}]'
plan_with "${tmp_dir}/one-way-follower.json" preserve-quorum '["partition:follower"]' \
    "[$(step_with one-way follower "${one_way_then_kill}")]"
expect_invalid_plan 'a crash of the leader a follower cannot reach' 'which preserve-quorum refuses' \
    "${tmp_dir}/one-way-follower.json"
plan_with "${tmp_dir}/one-way-owner.json" preserve-quorum '["partition:relay-owner"]' \
    "[$(step_with one-way relay-owner "${one_way_then_kill}")]"
expect_invalid_plan 'a crash that breaks the policy if the peer leads' 'which preserve-quorum refuses' \
    "${tmp_dir}/one-way-owner.json"
plan_with "${tmp_dir}/one-way-leader.json" preserve-quorum '["partition:leader"]' \
    "[$(step_with one-way leader "${one_way_then_kill}")]"
"${plans}" validate --plan "${tmp_dir}/one-way-leader.json" \
    || fail 'a crash of the receiver the leader cannot reach was refused'
printf '[]\n' >"${tmp_dir}/array.json"
expect_invalid_plan 'a plan that is not an object' 'not one JSON object' "${tmp_dir}/array.json"
: >"${tmp_dir}/empty.json"
expect_invalid_plan 'an empty plan file' 'the plan file is missing or empty' "${tmp_dir}/empty.json"
plan_with "${tmp_dir}/valid.json" temporary-quorum-loss '["kill:leader","quorum-loss"]' "[$(step_with double-outage leader "[${kill_target},
    {\"id\":\"1b\",\"family\":\"pause\",\"node\":\"peer\",\"start_seconds\":5,\"hold_seconds\":10}]")]"
"${plans}" validate --plan "${tmp_dir}/valid.json" || fail 'a temporary double outage was refused'

# Options and plans are refused before the run alters a container, and a refused plan leaves its
# failed manifest and every validation reason behind.
expect_setup_rejection 'mixed-instability needs three nodes' 'requires --nodes 3' \
    "${chaos_dir}/run-baseline.sh" --scenario mixed-instability --image fixture --nodes 1
expect_setup_rejection 'mixed options need mixed-instability' 'apply only to mixed-instability' \
    "${chaos_dir}/run-baseline.sh" --scenario leader-crash --image fixture --seed 4
expect_setup_rejection 'a plan carries its own policy' '--plan carries its own seed, duration, policy and coverage' \
    "${chaos_dir}/run-baseline.sh" --scenario mixed-instability --image fixture --plan "${tmp_dir}/valid.json" --seed 4
expect_setup_rejection 'durations take a unit' '--duration must be whole seconds' \
    "${chaos_dir}/run-baseline.sh" --scenario mixed-instability --image fixture --duration 10d
expect_setup_rejection 'durations are bounded' 'from 60 seconds through 4 hours' \
    "${chaos_dir}/run-baseline.sh" --scenario mixed-instability --image fixture --duration 5h
expect_setup_rejection 'policies are named' '--policy must be preserve-quorum or temporary-quorum-loss' \
    "${chaos_dir}/run-baseline.sh" --scenario mixed-instability --image fixture --policy sometimes
expect_setup_rejection 'mixed holds come from the plan' 'takes every hold from its action plan' \
    "${chaos_dir}/run-baseline.sh" --scenario mixed-instability --image fixture --outage-seconds 9
expect_setup_rejection 'the timeout covers the duration' '--timeout must be an integer from 1200 through 21600' \
    "${chaos_dir}/run-baseline.sh" --scenario mixed-instability --image fixture --duration 10m --timeout 900
expect_setup_rejection 'limits need a scenario that judges them' 'apply only to degraded-links and mixed-instability' \
    "${chaos_dir}/run-baseline.sh" --scenario baseline --image fixture --max-pending 4
long_run="mixed-long-$$-${RANDOM}"
owned_runs+=("${long_run}")
expect_setup_rejection 'a four-hour run within its default bound' 'selected no plan' \
    "${chaos_dir}/chaos.sh" run mixed-instability --image fixture --duration 4h --coverage kill:primary \
    --artifacts "${tmp_dir}/artifacts" --run-id "${long_run}"
jq -e '.timeout_seconds == 21600' "${tmp_dir}/artifacts/${long_run}/manifest.json" >/dev/null \
    || fail 'a four-hour run was not bounded at six hours by default'
invalid_run="mixed-invalid-plan-$$-${RANDOM}"
owned_runs+=("${invalid_run}")
expect_setup_rejection 'an invalid combination' 'the action plan is invalid, so no container was altered' \
    "${chaos_dir}/chaos.sh" run mixed-instability --image fixture --plan "${tmp_dir}/two-network.json" \
    --timeout 3000 --artifacts "${tmp_dir}/artifacts" --run-id "${invalid_run}"
jq -e '.status == "failed" and .exit_code == 2 and .final_phase == "action plan and fixtures"
       and (.setup_error | contains("conflicting interface rules"))' \
    "${tmp_dir}/artifacts/${invalid_run}/manifest.json" >/dev/null \
    || fail 'an invalid plan did not leave a failed manifest naming its reason'
jq -e '.verdict == "invalid"' "${tmp_dir}/artifacts/${invalid_run}/mixed/plan-validation.json" >/dev/null \
    || fail 'an invalid plan did not keep its validation report'
jq -e '.category == "setup" and (.reproducer | startswith("just chaos run mixed-instability --image fixture --plan "))
       and (.reproducer | endswith(" --timeout 3000"))' \
    "${tmp_dir}/artifacts/${invalid_run}/results/finding.json" >/dev/null \
    || fail 'an invalid plan did not record a setup finding that reruns its plan with its options'
[[ ! -e "${tmp_dir}/artifacts/${invalid_run}/diagnostics/docker-events.ndjson" ]] \
    || fail 'an invalid plan reached the Docker preflight'
[[ -z "$(docker container ls --all --quiet --filter "label=io.nervix.chaos.run=${invalid_run}")" ]] \
    || fail 'an invalid plan created a container'

# A replay reuses one complete mixed-instability run directory and refuses anything less.
status=0
"${chaos_dir}/chaos.sh" replay >"${tmp_dir}/replay-missing.out" 2>&1 || status=$?
[[ "${status}" -eq 2 ]] || fail "a replay without a directory returned ${status}, expected 2"
"${chaos_dir}/chaos.sh" replay --help | grep -Fq 'just chaos replay RUN_DIRECTORY' \
    || fail 'just chaos replay --help did not explain the replay'
expect_setup_rejection 'a missing run directory' 'the replay run directory does not exist' \
    "${chaos_dir}/chaos.sh" replay "${tmp_dir}/no-such-run"
recorded="${tmp_dir}/recorded"
mkdir -p "${recorded}/mixed" "${recorded}/fixtures"
cp "${tmp_dir}/valid.json" "${recorded}/mixed/plan.json"
jq -nc --arg run_id recorded --argjson count 1500 -f "${chaos_dir}/fixtures/generate-baseline.jq" \
    >"${recorded}/fixtures/input.ndjson"
cp "${chaos_dir}/fixtures/baseline.nspl" "${recorded}/fixtures/baseline.nspl"
sha() {
    openssl dgst -sha256 -r "$1" | awk '{ print $1 }'
}
probe_id="$(docker image inspect --format '{{.Id}}' "${chaos_probe_image}")"
jq -n --arg plan "$(sha "${recorded}/mixed/plan.json")" --arg input "$(sha "${recorded}/fixtures/input.ndjson")" \
    --arg graph "$(sha "${recorded}/fixtures/baseline.nspl")" --arg probe "${chaos_probe_image}" \
    --arg probe_id "${probe_id}" '
    {run_id: "recorded", scenario: "mixed-instability", status: "failed", exit_code: 1, final_phase: "mixed",
     requested_image: "fixture", resolved_image_id: ("sha256:" + ("0" * 64)), resolved_repo_digests: "",
     topology_nodes: 3, timeout_seconds: 2100,
     tool_images: ({kafka: 0, kcat: 0, probe: 0, pumba: 0, nettools: 0} | map_values({reference: $probe, image_id: $probe_id})),
     mixed: {seed: null, policy: "temporary-quorum-loss", plan: {sha256: $plan},
             limits: {max_memory_bytes: 1073741824, max_recovery_backlog: 20, max_pending: 128},
             deployment: {nodes: {NERVIX_RAFT_HEARTBEAT_INTERVAL: "250ms"}, load_interval_ms: 1000}},
     fixtures: {input: {sha256: $input, records: 1500}, graph: {sha256: $graph}}}' >"${recorded}/manifest.json"
jq '.scenario = "baseline"' "${recorded}/manifest.json" >"${tmp_dir}/baseline-manifest.json"
mkdir -p "${tmp_dir}/baseline-run"
cp "${tmp_dir}/baseline-manifest.json" "${tmp_dir}/baseline-run/manifest.json"
expect_setup_rejection 'a run of another scenario' 'only mixed-instability runs can be replayed' \
    "${chaos_dir}/chaos.sh" replay "${tmp_dir}/baseline-run"
expect_setup_rejection 'a replay that changes the experiment' 'so --seed cannot change it' \
    "${chaos_dir}/chaos.sh" replay "${recorded}" --seed 3
cp -R "${recorded}" "${tmp_dir}/incomplete"
jq 'del(.tool_images.pumba) | del(.mixed.deployment.load_interval_ms) | del(.fixtures.graph)' \
    "${recorded}/manifest.json" >"${tmp_dir}/incomplete/manifest.json"
expect_setup_rejection 'an incomplete trace of the experiment' \
    'does not record the pumba image, the load interval, the NSPL graph' \
    "${chaos_dir}/chaos.sh" replay "${tmp_dir}/incomplete"
cp -R "${recorded}" "${tmp_dir}/tampered"
jq '.steps[0].actions[0].hold_seconds = 31' "${recorded}/mixed/plan.json" >"${tmp_dir}/tampered/mixed/plan.json"
expect_setup_rejection 'a plan changed after its run' 'mixed/plan.json in' "${chaos_dir}/chaos.sh" replay "${tmp_dir}/tampered"
cp -R "${recorded}" "${tmp_dir}/no-fixture"
rm "${tmp_dir}/no-fixture/fixtures/input.ndjson"
expect_setup_rejection 'a missing fixture' 'lacks its recorded fixtures/input.ndjson' \
    "${chaos_dir}/chaos.sh" replay "${tmp_dir}/no-fixture"
# A recorded Nervix image that is neither local nor pullable fails the replay in preflight, before the
# replay records any Docker event, with a manifest naming it.
unavailable_run="mixed-unavailable-$$-${RANDOM}"
owned_runs+=("${unavailable_run}")
expect_setup_rejection 'an unavailable recorded image' "the recorded Nervix image sha256:$(printf '0%.0s' {1..64}) is unavailable" \
    "${chaos_dir}/chaos.sh" replay "${recorded}" --artifacts "${tmp_dir}/artifacts" --run-id "${unavailable_run}"
jq -e --slurpfile recorded "${recorded}/manifest.json" '
    .status == "failed" and .final_phase == "preflight" and .replay_of.run_id == "recorded"
    and .mixed.plan.source == "replay" and .tool_images == $recorded[0].tool_images
    and .fixtures.input.sha256 == $recorded[0].fixtures.input.sha256' \
    "${tmp_dir}/artifacts/${unavailable_run}/manifest.json" >/dev/null \
    || fail 'a replay with an unavailable image did not record the reused experiment and its failure'
cmp -s "${recorded}/fixtures/input.ndjson" "${tmp_dir}/artifacts/${unavailable_run}/fixtures/input.ndjson" \
    || fail 'a replay did not reuse the recorded fixture'
jq -e --arg reproducer "just chaos replay ${recorded}" '.category == "setup" and .reproducer == $reproducer' \
    "${tmp_dir}/artifacts/${unavailable_run}/results/finding.json" >/dev/null \
    || fail 'a replay that failed before recording its deployment did not reproduce through its source'
[[ -z "$(docker container ls --all --quiet --filter "label=io.nervix.chaos.run=${unavailable_run}")" ]] \
    || fail 'a replay with an unavailable image created a container'

# Logical node references resolve within the run from one settled observation.
script_dir="${chaos_dir}"
artifact_dir="${tmp_dir}/sourced"
mkdir -p "${artifact_dir}/results"
# shellcheck source=../mixed-scenario.sh
source "${chaos_dir}/mixed-scenario.sh"
jq -n '{leader: "node-2", owners: {ingestor: "node-2", relay: "node-3", emitter: "node-1"},
        nodes: [{node: "node-1"}, {node: "node-2"}, {node: "node-3"}]}' >"${tmp_dir}/observation.json"
resolve() {
    mixed_resolve_refs "${tmp_dir}/observation.json" "$1" "$2" "${tmp_dir}/refs.json"
    jq -c '.refs | map_values(if type == "array" then map(.host) else .host end)' "${tmp_dir}/refs.json"
}
[[ "$(resolve leader 0)" == '{"target":"nervix-2","peer":"nervix-1","other":"nervix-3"}' ]] \
    || fail "the leader resolved to $(resolve leader 0)"
[[ "$(resolve follower 1)" == '{"target":"nervix-3","peer":"nervix-2","other":"nervix-1"}' ]] \
    || fail "the second follower resolved to $(resolve follower 1)"
[[ "$(resolve relay-owner 0)" == '{"target":"nervix-3","peer":"nervix-2","other":"nervix-1"}' ]] \
    || fail "the relay owner resolved to $(resolve relay-owner 0)"
[[ "$(resolve cluster 0)" == '{"cluster":["nervix-1","nervix-2","nervix-3"]}' ]] \
    || fail "the cluster resolved to $(resolve cluster 0)"
mixed_resolve_refs "${tmp_dir}/observation.json" ingestor-owner 0 "${tmp_dir}/refs.json"
jq -e '.refs.target.roles == ["leader", "ingestor-owner"] and .refs.other.roles == ["follower", "relay-owner"]' \
    "${tmp_dir}/refs.json" >/dev/null || fail 'resolved references did not record the roles their nodes held'

# The voters out of a quorum are the union of the holding faults' own, and a degraded link adds none.
mixed_active=(1a 2a 3a)
mixed_action_impaired=([1a]="nervix-1" [2a]="" [3a]="nervix-3 nervix-1")
[[ "$(mixed_impaired_hosts | tr '\n' ' ')" == 'nervix-1 nervix-3 ' ]] \
    || fail "the voters out of a quorum were $(mixed_impaired_hosts | tr '\n' ' ')"
mixed_active=(2a)
[[ -z "$(mixed_impaired_hosts)" ]] || fail 'a degraded link left a voter out of a quorum'
# Each node's lifecycle events are exactly those of the node faults that named it.
mixed_action_hosts=([1a]="nervix-1" [1b]="nervix-1 nervix-2 nervix-3" [1c]="nervix-1" [1d]="nervix-2")
mixed_action_dir=([1a]="${tmp_dir}/1a-kill" [1b]="${tmp_dir}/1b-pause" [1c]="${tmp_dir}/1c-partition-isolate"
    [1d]="${tmp_dir}/1d-stop")
[[ "$(mixed_expected_events nervix-1 1a 1b 1c 1d | tr '\n' ' ')" == '--expect kill:9 --expect die:137 --expect start --expect pause --expect unpause ' ]] \
    || fail "nervix-1 expected $(mixed_expected_events nervix-1 1a 1b 1c 1d | tr '\n' ' ')"
[[ "$(mixed_expected_events nervix-2 1c 1d | tr '\n' ' ')" == '--expect kill:15 --expect die:0 --expect start ' ]] \
    || fail "nervix-2 expected $(mixed_expected_events nervix-2 1c 1d | tr '\n' ' ')"
[[ -z "$(mixed_expected_events nervix-3 1a 1c 1d)" ]] || fail 'a node no fault named expected lifecycle events'

# The action trace must be complete, ordered and cover the plan; resources must stay within limits.
jq -n '{seed: 1, policy: "preserve-quorum", nodes: 3, duration_seconds: 900,
        coverage: ["kill:leader", "degrade:follower"],
        steps: [{index: 1, kind: "kill", role: "leader", pick: 0, at_seconds: 0, estimated_seconds: 100,
                 actions: [{id: "1a", family: "kill", node: "target", start_seconds: 0, hold_seconds: 8}]},
                {index: 2, kind: "degrade", role: "follower", pick: 0, at_seconds: 200, estimated_seconds: 100,
                 actions: [{id: "2a", family: "degrade", node: "target", to: "peer", profile: "delay",
                            start_seconds: 0, hold_seconds: 20}]}]}' >"${tmp_dir}/trace-plan.json"
cat >"${tmp_dir}/trace.ndjson" <<'EOF'
{"event":"step-started","step":1,"at_ms":1}
{"event":"action-started","step":1,"at_ms":2,"action":"1a","family":"kill","intended":{"node":"target"},"targets":[{"host":"nervix-1","roles":["leader","ingestor-owner"]}]}
{"event":"action-verified","step":1,"at_ms":3,"action":"1a"}
{"event":"action-healed","step":1,"at_ms":4,"action":"1a"}
{"event":"step-ended","step":1,"at_ms":5}
{"event":"step-started","step":2,"at_ms":6}
{"event":"action-started","step":2,"at_ms":7,"action":"2a","family":"degrade","intended":{"node":"target"},"targets":[{"host":"nervix-2","roles":["follower"]}]}
{"event":"action-verified","step":2,"at_ms":8,"action":"2a"}
{"event":"action-healed","step":2,"at_ms":9,"action":"2a"}
{"event":"step-ended","step":2,"at_ms":10}
EOF
"${mixed_evidence}" trace --plan "${tmp_dir}/trace-plan.json" --trace "${tmp_dir}/trace.ndjson" \
    --output "${tmp_dir}/trace.json" >/dev/null || fail 'a complete covering trace was rejected'
jq -e '.verdict == "complete" and .coverage.covered == ["degrade:follower", "kill:ingestor-owner", "kill:leader"]' \
    "${tmp_dir}/trace.json" >/dev/null || fail "a complete trace reported $(jq -c . "${tmp_dir}/trace.json")"
expect_trace() {
    local case_name="$1"
    local verdict="$2"
    local reason="$3"
    local plan="$4"
    local trace="$5"
    local status=0
    "${mixed_evidence}" trace --plan "${plan}" --trace "${trace}" --output "${tmp_dir}/trace-failure.json" \
        >/dev/null 2>&1 || status=$?
    [[ "${status}" -eq 1 ]] || fail "${case_name} returned ${status}, expected verdict 1"
    jq -e --arg verdict "${verdict}" --arg reason "${reason}" '
        .verdict == $verdict
        and any((.missing_records + .unexpected_records + .coverage.missing)[]; contains($reason))' \
        "${tmp_dir}/trace-failure.json" >/dev/null \
        || fail "${case_name} reported $(jq -c . "${tmp_dir}/trace-failure.json")"
}
head -n 8 "${tmp_dir}/trace.ndjson" >"${tmp_dir}/interrupted.ndjson"
expect_trace 'an interrupted trace' incomplete 'action 2a has 0 healed records' \
    "${tmp_dir}/trace-plan.json" "${tmp_dir}/interrupted.ndjson"
sed '3d' "${tmp_dir}/trace.ndjson" >"${tmp_dir}/unverified.ndjson"
expect_trace 'an unverified action' incomplete 'action 1a has 0 verified records' \
    "${tmp_dir}/trace-plan.json" "${tmp_dir}/unverified.ndjson"
{
    cat "${tmp_dir}/trace.ndjson"
    printf '%s\n' '{"event":"action-refused","step":2,"at_ms":11,"action":"2a","reason":"policy"}'
} >"${tmp_dir}/refused.ndjson"
expect_trace 'a refused action' incomplete 'action 2a was refused' "${tmp_dir}/trace-plan.json" "${tmp_dir}/refused.ndjson"
jq -c 'if .event == "action-verified" and .action == "1a" then .at_ms = 5 else . end' \
    "${tmp_dir}/trace.ndjson" >"${tmp_dir}/disordered.ndjson"
expect_trace 'an action healed before it was verified' incomplete 'was not started, verified and healed in that order' \
    "${tmp_dir}/trace-plan.json" "${tmp_dir}/disordered.ndjson"
jq -c 'if .event == "action-healed" and .action == "1a" then .step = 2 | .at_ms = 9 else . end' \
    "${tmp_dir}/trace.ndjson" >"${tmp_dir}/outside.ndjson"
expect_trace 'an action recorded in another step' incomplete 'action 1a is recorded outside step 1' \
    "${tmp_dir}/trace-plan.json" "${tmp_dir}/outside.ndjson"
{
    cat "${tmp_dir}/trace.ndjson"
    printf '%s\n' '{"event":"action-started","step":2,"at_ms":9,"action":"2z","family":"kill"}'
} >"${tmp_dir}/unplanned.ndjson"
expect_trace 'an unplanned action' incomplete 'action-started record names unplanned action 2z' \
    "${tmp_dir}/trace-plan.json" "${tmp_dir}/unplanned.ndjson"
jq '.coverage += ["pause:follower"]' "${tmp_dir}/trace-plan.json" >"${tmp_dir}/uncovered-plan.json"
expect_trace 'missing action coverage' uncovered 'pause:follower' "${tmp_dir}/uncovered-plan.json" "${tmp_dir}/trace.ndjson"

printf '%s\n' \
    '{"at_ms":1000,"source_end":10,"output_end":9,"backlog":1,"nodes":[{"container":"n1","status":"running","memory_bytes":1000,"cpu_percent":1}]}' \
    '{"at_ms":11000,"source_end":20,"output_end":19,"backlog":1,"nodes":[{"container":"n1","status":"running","memory_bytes":5000000,"cpu_percent":2}]}' \
    >"${tmp_dir}/samples.ndjson"
"${mixed_evidence}" resources --samples "${tmp_dir}/samples.ndjson" --max-memory-bytes 1073741824 \
    --output "${tmp_dir}/resources.json" >/dev/null || fail 'samples within their limits were rejected'
jq -e '.verdict == "within-limits" and .summary.nodes[0].max_memory_bytes == 5000000' \
    "${tmp_dir}/resources.json" >/dev/null || fail "samples within limits reported $(jq -c . "${tmp_dir}/resources.json")"
"${mixed_evidence}" resources --samples "${tmp_dir}/samples.ndjson" --max-memory-bytes 1048576 \
    --output "${tmp_dir}/resources.json" >/dev/null || fail 'a memory finding failed the resource check itself'
jq -e '.verdict == "over-limit" and (.findings | length) == 1 and .findings[0].peak == 5000000' \
    "${tmp_dir}/resources.json" >/dev/null || fail "a memory excess reported $(jq -c . "${tmp_dir}/resources.json")"
printf '%s\n' '{"at_ms":200000,"source_end":30,"output_end":29,"backlog":1,"nodes":[]}' >>"${tmp_dir}/samples.ndjson"
status=0
"${mixed_evidence}" resources --samples "${tmp_dir}/samples.ndjson" --max-memory-bytes 1073741824 \
    --output "${tmp_dir}/resources.json" >/dev/null 2>&1 || status=$?
if [[ "${status}" -ne 1 ]] || ! jq -e '.verdict == "gapped"' "${tmp_dir}/resources.json" >/dev/null; then
    fail 'a gap in the continuous samples was not reported'
fi

# A run that ended inside its fault phase still judges the trace and samples it recorded, and the
# verdicts of a run that reached them stay as written.
mkdir -p "${mixed_dir}"
cp "${tmp_dir}/trace-plan.json" "${mixed_plan}"
cp "${tmp_dir}/interrupted.ndjson" "${mixed_trace}"
head -n 2 "${tmp_dir}/samples.ndjson" >"${mixed_samples}"
max_memory_bytes=1073741824
mixed_summarize_interrupted
jq -e '.verdict == "incomplete"' "${artifact_dir}/results/mixed-trace.json" >/dev/null \
    || fail "an interrupted run's trace reported $(jq -c . "${artifact_dir}/results/mixed-trace.json")"
jq -e '.verdict == "within-limits" and .summary.nodes[0].max_memory_bytes == 5000000' \
    "${artifact_dir}/results/mixed-resources.json" >/dev/null \
    || fail "an interrupted run's samples reported $(jq -c . "${artifact_dir}/results/mixed-resources.json")"
cp "${tmp_dir}/trace.ndjson" "${mixed_trace}"
mixed_summarize_interrupted
jq -e '.verdict == "incomplete"' "${artifact_dir}/results/mixed-trace.json" >/dev/null \
    || fail 'the exit trap replaced the verdicts a run had already written'

# Every tool refuses a malformed invocation with exit status 2 and explains itself on request.
expect_usage_error() {
    local case_name="$1"
    shift
    local status=0
    "$@" >/dev/null 2>&1 || status=$?
    [[ "${status}" -eq 2 ]] || fail "${case_name} returned ${status}, expected usage error 2"
}
"${plans}" --help 2>/dev/null || fail 'mixed-plan.sh did not explain itself'
"${mixed_evidence}" --help 2>/dev/null || fail 'verify-mixed-evidence.sh did not explain itself'
expect_usage_error 'a plan command is required' "${plans}"
expect_usage_error 'an unknown plan command' "${plans}" shuffle
expect_usage_error 'an unknown generate option' "${plans}" generate --sed 1
expect_usage_error 'a seed beyond its range' "${plans}" generate --seed 2147483647 --duration 900 \
    --policy preserve-quorum --coverage kill:leader --output "${tmp_dir}/unused.json"
expect_usage_error 'a duration that is not whole seconds' "${plans}" generate --seed 1 --duration 15m \
    --policy preserve-quorum --coverage kill:leader --output "${tmp_dir}/unused.json"
expect_usage_error 'an unknown policy' "${plans}" generate --seed 1 --duration 900 --policy sometimes \
    --coverage kill:leader --output "${tmp_dir}/unused.json"
expect_usage_error 'a plan to validate is required' "${plans}" validate
expect_usage_error 'an unknown validate option' "${plans}" validate --plans "${tmp_dir}/valid.json"
expect_usage_error 'default coverage needs a known policy' "${plans}" default-coverage --policy sometimes
expect_usage_error 'an evidence command is required' "${mixed_evidence}"
expect_usage_error 'an unknown evidence command' "${mixed_evidence}" ledger
expect_usage_error 'an unknown trace option' "${mixed_evidence}" trace --plans "${tmp_dir}/trace-plan.json"
expect_usage_error 'a trace without its plan' "${mixed_evidence}" trace --plan "${tmp_dir}/no-plan.json" \
    --trace "${tmp_dir}/trace.ndjson" --output "${tmp_dir}/unused.json"
printf '[1]\n' >"${tmp_dir}/not-records.ndjson"
expect_usage_error 'a trace of something else' "${mixed_evidence}" trace --plan "${tmp_dir}/trace-plan.json" \
    --trace "${tmp_dir}/not-records.ndjson" --output "${tmp_dir}/unused.json"
expect_usage_error 'an unknown resources option' "${mixed_evidence}" resources --sample "${tmp_dir}/samples.ndjson"
expect_usage_error 'a limit that is not a number' "${mixed_evidence}" resources --samples "${tmp_dir}/samples.ndjson" \
    --max-memory-bytes lots --output "${tmp_dir}/unused.json"
expect_usage_error 'no samples' "${mixed_evidence}" resources --samples "${tmp_dir}/no-samples.ndjson" \
    --max-memory-bytes 1 --output "${tmp_dir}/unused.json"
printf '{"at":1}\n' >"${tmp_dir}/not-samples.ndjson"
expect_usage_error 'samples of something else' "${mixed_evidence}" resources --samples "${tmp_dir}/not-samples.ndjson" \
    --max-memory-bytes 1 --output "${tmp_dir}/unused.json"

printf 'mixed-instability self-test passed\n'
