#!/usr/bin/env bash
# Checks the suite runner without Docker: a copy of suite.sh runs beside stand-ins for the chaos
# runner, the cleanup command and the docker CLI, which act out every outcome a run can have.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
chaos_dir="$(cd "${script_dir}/.." && pwd)"

fail() {
    printf 'suite self-test failed: %s\n' "$*" >&2
    exit 1
}

expect_json() {
    local file="$1"
    local filter="$2"
    local message="$3"
    jq -e "${filter}" "${file}" >/dev/null || {
        jq . "${file}" >&2 || true
        fail "${message}"
    }
}

tmp_dir="$(mktemp -d)"
suite_test_cleanup() {
    local pid
    for pid in ${background_pids[@]+"${background_pids[@]}"}; do
        kill -KILL "${pid}" 2>/dev/null || true
    done
    rm -rf "${tmp_dir}"
}
background_pids=()
trap suite_test_cleanup EXIT

image_id="sha256:$(printf 'a%.0s' {1..64})"
digest_reference="registry.example/nervix@sha256:$(printf 'b%.0s' {1..64})"

# The real definitions validate, name only scenarios the runner has, and keep the runner's teardown
# reserve in their step budgets.
real_suites="${tmp_dir}/real-suites.txt"
"${chaos_dir}/suite.sh" list >"${real_suites}" || fail 'the checked-in suite definitions do not validate'
scenarios="$("${chaos_dir}/chaos.sh" list | awk '/^[a-z]/ { print $1 }' | jq -Rsc 'split("\n") | map(select(length > 0))')"
unknown_scenarios="$(jq -r --argjson scenarios "${scenarios}" \
    '[.suites[].entries[] | select((.run[0] | IN($scenarios[])) | not) | .id] | unique | join(", ")' \
    "${chaos_dir}/suites.json")"
[[ -z "${unknown_scenarios}" ]] || fail "suite entries name no runner scenario: ${unknown_scenarios}"
runner_reserve="$(sed -n 's/^teardown_reserve_seconds=\([0-9][0-9]*\)$/\1/p' "${chaos_dir}/run-baseline.sh")"
suite_reserve="$(sed -n 's/^teardown_reserve_minutes=\([0-9][0-9]*\)$/\1/p' "${chaos_dir}/suite.sh")"
[[ -n "${runner_reserve}" && -n "${suite_reserve}" && "${runner_reserve}" -eq $((suite_reserve * 60)) ]] \
    || fail "the suite's teardown reserve of ${suite_reserve} minutes is not the runner's ${runner_reserve} seconds"
for suite in smoke soak; do
    "${chaos_dir}/suite.sh" shards "${suite}" >"${tmp_dir}/${suite}-shards.json"
    jq -e --slurpfile suites "${chaos_dir}/suites.json" --arg suite "${suite}" --argjson reserve "${suite_reserve}" '
        ($suites[0].suites[$suite].entries) as $entries
        | length == ([$entries[].shard] | unique | length)
          and all(.[]; . as $shard
                  | .execution_minutes == ([$entries[] | select(.shard == $shard.shard) | .budget_minutes] | add)
                  and .step_minutes == .execution_minutes + $reserve
                  and .job_minutes == .step_minutes + $suites[0].ci_reserve_minutes
                  and .entries == [$entries[] | select(.shard == $shard.shard) | .id])
    ' "${tmp_dir}/${suite}-shards.json" >/dev/null || fail "the ${suite} shard budgets do not add up"
done

# The copy runs beside stand-ins and its own definitions.
bundle="${tmp_dir}/bundle"
mkdir -p "${bundle}/tests" "${bundle}/bin"
cp "${chaos_dir}/suite.sh" "${chaos_dir}/suite-report.jq" "${bundle}/"
printf '#!/usr/bin/env bash\nexit 0\n' >"${bundle}/tests/suite-self-test.sh"

cat >"${bundle}/bin/docker" <<EOF
#!/usr/bin/env bash
# Knows one local tag and one digest reference, both of the same image, and can pull nothing.
known() {
    [[ "\$1" == nervix:local || "\$1" == "${digest_reference}" || "\$1" == "${image_id}" ]]
}
case "\$1" in
    image)
        [[ "\$2" == inspect ]] || exit 1
        shift 2
        format=""
        if [[ "\$1" == --format ]]; then
            format="\$2"
            shift 2
        fi
        known "\$1" || exit 1
        case "\${format}" in
            '{{.Id}}') printf '%s\n' "${image_id}" ;;
            *RepoDigests*) printf '%s\n' "${digest_reference}" ;;
            *) printf '[{}]\n' ;;
        esac
        ;;
    pull) printf 'pull access denied for %s\n' "\$2" >&2; exit 1 ;;
    version) printf '29.0.0\n' ;;
    compose) printf '5.0.0\n' ;;
    *) exit 1 ;;
esac
EOF

cat >"${bundle}/chaos.sh" <<EOF
#!/usr/bin/env bash
# Acts out the outcome its scenario names, as the runner would leave it in its run directory.
set -euo pipefail
[[ "\$1" == run ]] || exit 9
shift
scenario="\$1"
shift
image="" artifacts="" run_id="" run_timeout="" arguments=()
while [[ "\$#" -gt 0 ]]; do
    case "\$1" in
        --image) image="\$2"; shift 2 ;;
        --artifacts) artifacts="\$2"; shift 2 ;;
        --run-id) run_id="\$2"; shift 2 ;;
        --timeout) run_timeout="\$2"; shift 2 ;;
        *) arguments+=("\$1"); shift ;;
    esac
done
printf '%s|%s|%s|%s|%s\n' "\${scenario}" "\${arguments[*]-}" "\${image}" "\${run_timeout}" "\${run_id}" \
    >>"\${artifacts}/stub-calls.txt"
run_dir="\${artifacts}/\${run_id}"
[[ "\${scenario}" == refused ]] && exit 2
mkdir -p "\${run_dir}/results" "\${run_dir}/tls"
manifest() {
    jq -n --arg scenario "\${scenario}" --arg status "\$1" --argjson exit "\$2" --arg phase "\$3" \
        --arg image_id "\${4:-${image_id}}" --argjson timeout "\${run_timeout}" \
        '{scenario: \$scenario, status: \$status, exit_code: \$exit, final_phase: \$phase,
          resolved_image_id: \$image_id, timeout_seconds: \$timeout, teardown_seconds: 2}' \
        >"\${run_dir}/manifest.json"
}
ledger() {
    jq -n '{verdict: "pass", expected_records: 5, observed_records: 6, duplicate_records: 1,
            missing_ids: [], unexpected_ids: [], incorrect_content: []}' >"\${run_dir}/results/ledger.json"
}
case "\${scenario}" in
    pass)
        manifest passed 0 complete
        ledger
        jq -n '{verdict: "pass", progress: {election_ms: 1200, settled_recovery_ms: 8000, kill_verified: true}}' \
            >"\${run_dir}/results/pass.json"
        ;;
    product)
        manifest failed 1 'crash product findings'
        ledger
        jq -n '{category: "product", phase: "crash product findings", reproducer: "just chaos run product --again"}' \
            >"\${run_dir}/results/finding.json"
        exit 1
        ;;
    setup)
        manifest failed 2 preflight
        jq '.setup_error = "a pinned tool image cannot be pulled"' "\${run_dir}/manifest.json" >"\${run_dir}/m.json"
        mv "\${run_dir}/m.json" "\${run_dir}/manifest.json"
        exit 2
        ;;
    silent) rm -rf "\${run_dir}" ;;
    liar) manifest passed 0 complete; exit 1 ;;
    other-image) manifest passed 0 complete "sha256:\$(printf 'c%.0s' {1..64})" ;;
    unclassified) manifest failed 1 'external ledger verification'; exit 1 ;;
    stopped)
        manifest interrupted 143 signal-term
        jq -n '{category: "product", reproducer: "just chaos run stopped"}' >"\${run_dir}/results/finding.json"
        exit 143
        ;;
    mixed)
        manifest passed 0 complete
        jq '.scenario = "mixed-instability" | .mixed = {seed: 1234, policy: "temporary-quorum-loss"}' \
            "\${run_dir}/manifest.json" >"\${run_dir}/m.json"
        mv "\${run_dir}/m.json" "\${run_dir}/manifest.json"
        ledger
        jq -n '{verdict: "pass", progress: "results/mixed-instability-progress.json"}' \
            >"\${run_dir}/results/mixed-instability.json"
        jq -n '{recovery_ms: {settled_after_heal: {count: 2, max: 3155}, output_advanced_after_heal: {count: 2, max: 19321}}}' \
            >"\${run_dir}/results/mixed-instability-progress.json"
        jq -n '{summary: {max_backlog: 35, longest_output_stall_ms: 44896,
                          nodes: [{max_memory_bytes: 78223769.6}, {max_memory_bytes: 87304437.76}]}}' \
            >"\${run_dir}/results/mixed-resources.json"
        ;;
    slow)
        # Holds until the suite passes on a signal, then records an interrupted run.
        manifest running null 'slow phase'
        trap 'manifest interrupted 143 signal-term; exit 143' TERM
        printf '%s\n' "\$\$" >"\${run_dir}.pid"
        : >"\${run_dir}/tls/ca-key.pem"
        while true; do
            sleep 0.1
        done
        ;;
    *) exit 9 ;;
esac
EOF

cat >"${bundle}/cleanup.sh" <<'EOF'
#!/usr/bin/env bash
# Leaves resources behind for run ids ending in -left and fails to remove those ending in -stuck.
run_id="" evidence=""
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --run-id) run_id="$2"; shift 2 ;;
        --evidence) evidence="$2"; shift 2 ;;
        *) shift ;;
    esac
done
printf '%s\n' "${run_id}" >>"${evidence%/cleanup/*}/cleanup-calls.txt"
case "${run_id}" in
    *-slow | *-left)
        mkdir -p "${evidence}"
        jq -n --arg run_id "${run_id}" '{run_id: $run_id, containers: 2, pumba_sidecars: 1, networks: 1,
            volumes: 3, remaining: {containers: 0, networks: 0, volumes: 0}, exit_code: 0}' \
            >"${evidence}/cleanup.json"
        ;;
    *-stuck) exit 1 ;;
esac
EOF
chmod +x "${bundle}/bin/docker" "${bundle}/chaos.sh" "${bundle}/cleanup.sh" "${bundle}/tests/suite-self-test.sh"

write_definitions() {
    jq -n "$1" >"${bundle}/suites.json"
}
suite() {
    PATH="${bundle}/bin:${PATH}" "${bundle}/suite.sh" "$@"
}

# Definitions that would make a selection ambiguous or unbounded are refused before anything runs.
expect_invalid() {
    local definitions="$1"
    local expected="$2"
    write_definitions "${definitions}"
    local status=0
    suite list >"${tmp_dir}/invalid.txt" 2>&1 || status=$?
    [[ "${status}" -eq 2 ]] || fail "invalid definitions (${expected}) returned ${status}, expected 2"
    grep -Fq "${expected}" "${tmp_dir}/invalid.txt" \
        || fail "invalid definitions did not report: ${expected}; got $(cat "${tmp_dir}/invalid.txt")"
}
entry='{id: "one", shard: 1, budget_minutes: 5, run: ["pass"]}'
expect_invalid "{ci_reserve_minutes: 10, suites: {s: {description: \"d\", entries: [${entry}, ${entry}]}}}" \
    'entry id one appears more than once'
expect_invalid "{ci_reserve_minutes: 10, suites: {s: {description: \"d\", entries: [${entry}, (${entry} | .id = \"two\" | .shard = 3)]}}}" \
    'shards must be numbered from 1 without gaps'
expect_invalid "{ci_reserve_minutes: 10, suites: {s: {description: \"d\", entries: [(${entry} | .run += [\"--timeout\", \"60\"])]}}}" \
    'the suite sets --image, --artifacts, --run-id and --timeout itself'
expect_invalid "{ci_reserve_minutes: 10, suites: {s: {description: \"d\", entries: [(${entry} | .budget_minutes = 1)]}}}" \
    'budget_minutes must be an integer from 2 through 360'
expect_invalid "{ci_reserve_minutes: 10, suites: {s: {description: \"d\", entries: [(${entry} | .budget = 4)]}}}" \
    'unknown field budget'
expect_invalid "{ci_reserve_minutes: 10, suites: {cleanup: {description: \"d\", entries: [${entry}]}}}" \
    'suite name cleanup must be'
expect_invalid "{suites: {s: {description: \"d\", entries: [${entry}]}}}" \
    'ci_reserve_minutes must be a positive integer'

# Every outcome of a run gets its own verdict, and every entry runs after another one fails.
write_definitions '{ci_reserve_minutes: 10, suites: {
    outcomes: {description: "every outcome", entries: [
        {id: "pass", shard: 1, budget_minutes: 3, run: ["pass", "--nodes", "3"]},
        {id: "product", shard: 1, budget_minutes: 4, run: ["product"]},
        {id: "setup", shard: 1, budget_minutes: 5, run: ["setup"]},
        {id: "refused", shard: 1, budget_minutes: 5, run: ["refused"]},
        {id: "silent", shard: 2, budget_minutes: 5, run: ["silent"]},
        {id: "liar", shard: 2, budget_minutes: 5, run: ["liar"]},
        {id: "other-image", shard: 2, budget_minutes: 5, run: ["other-image"]},
        {id: "unclassified", shard: 2, budget_minutes: 5, run: ["unclassified"]},
        {id: "stopped", shard: 2, budget_minutes: 5, run: ["stopped"]},
        {id: "mixed", shard: 3, budget_minutes: 40, note: "a recorded seed", run: ["mixed", "--duration", "30m"]}]},
    green: {description: "passing entries", entries: [
        {id: "first", shard: 1, budget_minutes: 2, run: ["pass"]},
        {id: "second", shard: 1, budget_minutes: 2, run: ["mixed"]}]},
    slow: {description: "a run that holds", entries: [
        {id: "slow", shard: 1, budget_minutes: 2, run: ["slow"]},
        {id: "after", shard: 1, budget_minutes: 2, run: ["pass"]}]}}}'

suite shards outcomes >"${tmp_dir}/shards.json"
expect_json "${tmp_dir}/shards.json" '
    . == [{shard: 1, entries: ["pass", "product", "setup", "refused"], execution_minutes: 17, step_minutes: 32, job_minutes: 42},
          {shard: 2, entries: ["silent", "liar", "other-image", "unclassified", "stopped"], execution_minutes: 25, step_minutes: 40, job_minutes: 50},
          {shard: 3, entries: ["mixed"], execution_minutes: 40, step_minutes: 55, job_minutes: 65}]' \
    'shards did not report each shard with its execution, step and job budgets'
suite list >"${tmp_dir}/list.txt"
grep -Fq '    mixed  40 min  just chaos run mixed --duration 30m' "${tmp_dir}/list.txt" \
    || fail 'list did not print an entry with its budget and command'
grep -Fq '      a recorded seed' "${tmp_dir}/list.txt" || fail 'list did not print the note of an entry'

status=0
suite outcomes --image "${digest_reference}" --artifacts "${tmp_dir}/runs" --suite-id outcomes-1 \
    >"${tmp_dir}/outcomes.txt" 2>&1 || status=$?
[[ "${status}" -eq 1 ]] || fail "a suite with failed entries returned ${status}, expected 1"
record="${tmp_dir}/runs/outcomes-1/suite.json"
expect_json "${record}" '.status == "failed" and .self_check == "passed"
    and .worker.docker == "29.0.0" and .worker.compose == "5.0.0" and .worker.cpus > 0
    and .worker.memory_bytes > 0' 'the suite did not record its self-check and worker'
jq -e --arg digest "${digest_reference}" --arg id "${image_id}" '
    .image == {requested: $digest, id: $id, repo_digests: $digest, run_reference: $digest}' "${record}" >/dev/null \
    || fail 'the suite did not keep the digest reference it was given as the run image'
expect_json "${record}" '[.entries[] | {id, status, category, exit_code}] == [
        {id: "pass", status: "passed", category: null, exit_code: 0},
        {id: "product", status: "failed", category: "product", exit_code: 1},
        {id: "setup", status: "failed", category: "setup", exit_code: 2},
        {id: "refused", status: "failed", category: "setup", exit_code: 2},
        {id: "silent", status: "failed", category: "controller", exit_code: 0},
        {id: "liar", status: "failed", category: "controller", exit_code: 1},
        {id: "other-image", status: "failed", category: "controller", exit_code: 0},
        {id: "unclassified", status: "failed", category: null, exit_code: 1},
        {id: "stopped", status: "interrupted", category: null, exit_code: 143},
        {id: "mixed", status: "passed", category: null, exit_code: 0}]' \
    'the suite did not give every run outcome its own verdict and category'
expect_json "${record}" '
    (.entries[] | select(.id == "setup") | .reason) == "a pinned tool image cannot be pulled"
    and ((.entries[] | select(.id == "silent") | .reason) | test("no manifest.json"))
    and ((.entries[] | select(.id == "liar") | .reason) | test("recorded exit status 0, but its command exited 1"))
    and ((.entries[] | select(.id == "other-image") | .reason) | test("not the suite image"))
    and (.entries[] | select(.id == "stopped") | .reason) == "a signal ended the run"
    and (.entries[] | select(.id == "product") | .reproducer) == "just chaos run product --again"
    and (.entries[] | select(.id == "product") | .finding) == "outcomes-1-product/results/finding.json"
    and ((.entries[] | select(.id == "unclassified") | .reproducer) | startswith("just chaos run unclassified --image "))' \
    'the suite did not record the reason and reproducer of each failure'
# The dollars in this jq filter are jq variables, not shell expansion.
# shellcheck disable=SC2016
expect_json "${record}" '
    (.entries[] | select(.id == "pass")) as $pass
    | $pass.ledger == {verdict: "pass", expected: 5, observed: 6, duplicates: 1, missing: 0, unexpected: 0, incorrect: 0}
      and $pass.recovery == {election_ms: 1200, settled_recovery_ms: 8000}
      and $pass.final_phase == "complete" and $pass.timeout_seconds == 180 and $pass.teardown_seconds == 2
      and $pass.results == ["outcomes-1-pass/results/ledger.json", "outcomes-1-pass/results/pass.json"]
      and $pass.artifacts == "outcomes-1-pass" and $pass.log == "logs/pass.log"
      and (.entries[] | select(.id == "refused") | .artifacts) == null
      and ($pass.duration_seconds | type == "number")' \
    'the suite did not record the delivery and recovery metrics of a passing run'
# The dollars in this jq filter are jq variables, not shell expansion.
# shellcheck disable=SC2016
expect_json "${record}" '
    (.entries[] | select(.id == "mixed")) as $mixed
    | $mixed.seed == 1234 and $mixed.policy == "temporary-quorum-loss"
      and $mixed.recovery == {settled_after_heal_max_ms: 3155, output_advanced_after_heal_max_ms: 19321}
      and $mixed.resources == {max_node_memory_bytes: 87304437.76, max_backlog: 35, longest_output_stall_ms: 44896}' \
    'the suite did not record the seed, recovery distribution and resources of a mixed run'
calls="${tmp_dir}/runs/outcomes-1/stub-calls.txt"
[[ "$(sed -n 1p "${calls}")" == "pass|--nodes 3|${digest_reference}|180|outcomes-1-pass" ]] \
    || fail "the suite ran the first entry as $(sed -n 1p "${calls}")"
[[ "$(sed -n 10p "${calls}")" == "mixed|--duration 30m|${digest_reference}|2400|outcomes-1-mixed" ]] \
    || fail "the suite ran the last entry as $(sed -n 10p "${calls}")"
[[ "$(wc -l <"${calls}")" -eq 10 ]] || fail 'the suite did not run every entry after one failed'
summary="${tmp_dir}/runs/outcomes-1/summary.md"
# The summary is Markdown, so its code spans are written with escaped backticks.
expect_summary() {
    grep -Fq -- "$1" "${summary}" || {
        cat "${summary}" >&2
        fail "the summary does not contain: $1"
    }
}
expect_summary '## Chaos outcomes: failed'
expect_summary "| \`pass\` | passed | 0 | — | complete |"
expect_summary '| 5 / 6 / 1 / 0 | — |'
expect_summary "| \`unclassified\` | failed | 1 | unclassified | external ledger verification |"
expect_summary "| \`mixed\` | passed | 0 | — | complete |"
expect_summary '| 1234 |'
expect_summary "- \`pass\`: election 1.2 s, settled 8 s"
expect_summary 'peak node memory 87 MB, largest backlog 35 records'
expect_summary "- \`product\` failed, product in phase \`crash product findings\`. Reproduce with \`just chaos run product --again\`"
expect_summary "--timeout 300\`; console log \`logs/refused.log\`"
expect_summary "- \`stopped\` interrupted in phase \`signal-term\`: a signal ended the run."
grep -Fq 'chaos suite outcomes failed' "${tmp_dir}/outcomes.txt" \
    || fail 'the suite did not print its verdict'

# A shard and named entries select part of a suite; a tag runs as the image ID it resolves to.
suite outcomes --image nervix:local --artifacts "${tmp_dir}/runs" --suite-id shard-3 --shard 3 \
    >"${tmp_dir}/shard.txt" 2>&1 || fail 'a passing shard failed'
jq -e --arg id "${image_id}" '.status == "passed" and .shard == 3 and [.entries[].id] == ["mixed"]
    and .image.run_reference == $id and (.entries[0].command | index("--image") as $at | .[$at + 1]) == $id' \
    "${tmp_dir}/runs/shard-3/suite.json" >/dev/null \
    || fail 'a shard did not run its own entries against the resolved image ID'
suite green --image nervix:local --artifacts "${tmp_dir}/runs" --suite-id green-1 --entry second \
    >"${tmp_dir}/entry.txt" 2>&1 || fail 'a passing named entry failed'
expect_json "${tmp_dir}/runs/green-1/suite.json" '[.entries[].id] == ["second"] and .shard == null' \
    'a named entry did not select only that entry'
for invalid in '--entry missing' '--shard 9' '--shard x' '--suite-id bad/id'; do
    status=0
    # shellcheck disable=SC2086
    suite outcomes --image nervix:local --artifacts "${tmp_dir}/runs" ${invalid} >"${tmp_dir}/refused.txt" 2>&1 || status=$?
    [[ "${status}" -eq 2 ]] || fail "the selection ${invalid} returned ${status}, expected 2"
done
status=0
suite missing --image nervix:local >"${tmp_dir}/refused.txt" 2>&1 || status=$?
[[ "${status}" -eq 2 ]] || fail "an unknown suite returned ${status}, expected 2"
grep -Fq 'unknown chaos suite: missing' "${tmp_dir}/refused.txt" || fail 'an unknown suite was not named'

# Without --suite-id the suite names its directory after itself.
suite green --image nervix:local --artifacts "${tmp_dir}/default-id" >"${tmp_dir}/default-id.txt" 2>&1 \
    || fail 'a suite without a suite id failed'
default_records=("${tmp_dir}/default-id/green-"*/suite.json)
[[ "${#default_records[@]}" -eq 1 && -s "${default_records[0]}" ]] \
    || fail 'a suite without a suite id did not name its directory after the suite'

# A suite whose own verdict logic fails its self-check starts no run.
printf '#!/usr/bin/env bash\nexit 3\n' >"${bundle}/tests/suite-self-test.sh"
status=0
suite green --image nervix:local --artifacts "${tmp_dir}/runs" --suite-id unchecked-1 \
    >"${tmp_dir}/unchecked.txt" 2>&1 || status=$?
printf '#!/usr/bin/env bash\nexit 0\n' >"${bundle}/tests/suite-self-test.sh"
[[ "${status}" -eq 1 ]] || fail "a failed suite self-check returned ${status}, expected 1"
expect_json "${tmp_dir}/runs/unchecked-1/suite.json" '.status == "failed" and .self_check == "failed"
    and .error.category == "controller" and all(.entries[]; .status == "not-started")' \
    'a failed suite self-check did not stop the suite as a controller failure'
[[ ! -e "${tmp_dir}/runs/unchecked-1/stub-calls.txt" ]] || fail 'the suite ran an entry after its self-check failed'

# An image that is neither local nor pullable stops the suite before any run.
status=0
suite green --image registry.example/nervix:gone --artifacts "${tmp_dir}/runs" --suite-id gone-1 \
    >"${tmp_dir}/gone.txt" 2>&1 || status=$?
[[ "${status}" -eq 2 ]] || fail "an unavailable image returned ${status}, expected 2"
expect_json "${tmp_dir}/runs/gone-1/suite.json" '.status == "failed" and .error.category == "setup"
    and all(.entries[]; .status == "not-started")' 'an unavailable image did not fail the suite as a setup error'
[[ ! -e "${tmp_dir}/runs/gone-1/stub-calls.txt" ]] || fail 'the suite started a run without its image'
grep -Fq '**SETUP failure:** the image registry.example/nervix:gone is neither local nor pullable' \
    "${tmp_dir}/runs/gone-1/summary.md" || fail 'the summary did not name the setup failure'

# A signal reaches the executing run, which records its own interruption, and no later entry starts.
wait_for_file() {
    local path="$1"
    for _ in $(seq 1 100); do
        [[ -s "${path}" ]] && return 0
        sleep 0.1
    done
    fail "nothing wrote ${path} within ten seconds"
}
# The suite starts directly, so the signal reaches the suite and not a shell around it.
PATH="${bundle}/bin:${PATH}" "${bundle}/suite.sh" slow --image nervix:local --artifacts "${tmp_dir}/runs" \
    --suite-id slow-1 >"${tmp_dir}/slow.txt" 2>&1 &
suite_pid=$!
background_pids+=("${suite_pid}")
wait_for_file "${tmp_dir}/runs/slow-1/slow-1-slow.pid"
kill -TERM "${suite_pid}"
status=0
wait "${suite_pid}" || status=$?
[[ "${status}" -eq 143 ]] || fail "a suite ended by TERM returned ${status}, expected 143"
expect_json "${tmp_dir}/runs/slow-1/suite.json" '.status == "interrupted" and .signal == "TERM"
    and [.entries[] | {id, status, exit_code}] == [{id: "slow", status: "interrupted", exit_code: 143},
                                                   {id: "after", status: "not-started", exit_code: null}]
    and (.entries[0].run_status == "interrupted")' \
    'a signal did not interrupt the executing run and keep the next entry from starting'

# A suite whose controller and run were killed leaves resources for cleanup to capture and remove;
# the entry that never reported becomes interrupted and the one that never started not-started.
PATH="${bundle}/bin:${PATH}" "${bundle}/suite.sh" slow --image nervix:local --artifacts "${tmp_dir}/runs" \
    --suite-id killed-1 >"${tmp_dir}/killed.txt" 2>&1 &
suite_pid=$!
background_pids+=("${suite_pid}")
wait_for_file "${tmp_dir}/runs/killed-1/killed-1-slow.pid"
kill -KILL "${suite_pid}" "$(cat "${tmp_dir}/runs/killed-1/killed-1-slow.pid")"
wait "${suite_pid}" 2>/dev/null || true
expect_json "${tmp_dir}/runs/killed-1/suite.json" '.status == "running" and .entries[0].status == "running"' \
    'a killed suite did not leave its executing entry recorded as running'
suite cleanup "${tmp_dir}/runs/killed-1" >"${tmp_dir}/cleanup.txt" 2>&1 \
    || fail "cleanup of a killed suite failed: $(cat "${tmp_dir}/cleanup.txt")"
expect_json "${tmp_dir}/runs/killed-1/suite.json" '.status == "interrupted"
    and [.entries[] | {id, status}] == [{id: "slow", status: "interrupted"}, {id: "after", status: "not-started"}]
    and (.entries[0].reason | test("before the run reported its exit status"))
    and .cleanup.runs == [{run_id: "killed-1-slow", exit_code: 0,
                           leftovers: {run_id: "killed-1-slow", containers: 2, pumba_sidecars: 1, networks: 1,
                                       volumes: 3, remaining: {containers: 0, networks: 0, volumes: 0},
                                       exit_code: 0, evidence: "cleanup/killed-1-slow"}}]' \
    'cleanup did not finish the record of a killed suite'
[[ "$(cat "${tmp_dir}/runs/killed-1/cleanup-calls.txt")" == killed-1-slow ]] \
    || fail 'cleanup did not visit exactly the runs the suite started'
[[ ! -e "${tmp_dir}/runs/killed-1/killed-1-slow/tls/ca-key.pem" ]] \
    || fail 'cleanup kept the private key of a run that ended before its exit trap'
grep -Fq "Cleanup removed what 1 run left behind, after capturing it under \`cleanup/killed-1-slow\`." \
    "${tmp_dir}/runs/killed-1/summary.md" || fail 'the summary did not report what cleanup removed'
suite cleanup "${tmp_dir}/runs/outcomes-1" >"${tmp_dir}/cleanup-passed.txt" 2>&1 \
    || fail 'cleanup after a finished suite failed'
expect_json "${tmp_dir}/runs/outcomes-1/suite.json" '.status == "failed" and all(.cleanup.runs[]; .leftovers == null)
    and (.cleanup.runs | length) == 10' 'cleanup after a finished suite changed its verdict or found leftovers'
suite cleanup "${tmp_dir}/runs/never" >"${tmp_dir}/cleanup-none.txt" 2>&1 \
    || fail 'cleanup of a suite that never started failed'

# A run that finished and recorded its verdict before its controller was killed keeps that verdict.
mkdir -p "${tmp_dir}/runs/finished-1/finished-1-crash/results"
jq -n '{suite: "s", suite_id: "finished-1", status: "running", started_epoch: 0, image: {requested: "x"},
        entries: [{id: "crash", run_id: "finished-1-crash", status: "running", run: ["product"],
                   display: "just chaos run product"}]}' >"${tmp_dir}/runs/finished-1/suite.json"
jq -n '{scenario: "leader-crash", status: "failed", exit_code: 1, final_phase: "crash recovery"}' \
    >"${tmp_dir}/runs/finished-1/finished-1-crash/manifest.json"
jq -n '{category: "product", reproducer: "just chaos run leader-crash --again"}' \
    >"${tmp_dir}/runs/finished-1/finished-1-crash/results/finding.json"
suite cleanup "${tmp_dir}/runs/finished-1" >"${tmp_dir}/cleanup-finished.txt" 2>&1 \
    || fail 'cleanup of a suite whose run had finished failed'
expect_json "${tmp_dir}/runs/finished-1/suite.json" '.status == "interrupted"
    and [.entries[] | {status, category, exit_code, reproducer}]
        == [{status: "failed", category: "product", exit_code: 1, reproducer: "just chaos run leader-crash --again"}]' \
    'cleanup did not keep the verdict a finished run recorded'

# A cleanup that cannot remove a run's resources fails.
mkdir -p "${tmp_dir}/runs/stuck-1"
jq -n '{suite: "s", suite_id: "stuck-1", status: "failed", started_epoch: 0, image: {requested: "x"},
        entries: [{id: "stuck", run_id: "stuck-1-stuck", status: "failed", run: ["pass"]}]}' \
    >"${tmp_dir}/runs/stuck-1/suite.json"
status=0
suite cleanup "${tmp_dir}/runs/stuck-1" >"${tmp_dir}/cleanup-stuck.txt" 2>&1 || status=$?
[[ "${status}" -eq 1 ]] || fail "a failed removal returned ${status}, expected 1"
grep -Fq "**Cleanup failed** for \`stuck-1-stuck\`." "${tmp_dir}/runs/stuck-1/summary.md" \
    || fail 'the summary did not report the failed removal'

# The report of every shard passes only when each shard passed and none is missing.
report="${tmp_dir}/report.md"
suite report --expect-shards 2 "${tmp_dir}/runs/shard-3/suite.json" "${tmp_dir}/runs/green-1/suite.json" \
    >"${report}" || fail 'a report of two passing shards failed'
grep -Fq '## Chaos green, outcomes: passed' "${report}" || fail 'the report did not give the shards one verdict'
grep -Fq '### Chaos outcomes shard 3: passed' "${report}" || fail 'the report did not keep each shard'
status=0
suite report --expect-shards 3 "${tmp_dir}/runs/shard-3/suite.json" "${tmp_dir}/runs/green-1/suite.json" \
    >"${report}" || status=$?
[[ "${status}" -eq 1 ]] || fail "a report with a missing shard returned ${status}, expected 1"
grep -Fq '**Missing evidence:** 2 of 3 shard verdicts are present.' "${report}" \
    || fail 'a report with a missing shard did not name the missing evidence'
status=0
suite report "${tmp_dir}/runs/shard-3/suite.json" "${tmp_dir}/runs/outcomes-1/suite.json" >"${report}" || status=$?
[[ "${status}" -eq 1 ]] || fail 'a report with a failed shard did not fail'
status=0
suite report "${tmp_dir}/runs/shard-3/suite.json" "${tmp_dir}/no-such-suite.json" >"${report}" 2>&1 || status=$?
[[ "${status}" -eq 1 ]] || fail 'a report with an unreadable shard did not fail'

printf 'chaos suite self-test passed\n'
