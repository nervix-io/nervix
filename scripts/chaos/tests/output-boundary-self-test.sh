#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
chaos_dir="$(cd "${script_dir}/.." && pwd)"
selection="${1:-all}"
[[ "$#" -le 1 && "${selection}" =~ ^(all|budgets|observations)$ ]] || exit 2

fail() {
    printf 'output boundary self-test failed: %s\n' "$*" >&2
    exit 1
}

if [[ "${selection}" != observations ]]; then
    # shellcheck source=../verifier-self-check.sh
    source "${chaos_dir}/verifier-self-check.sh"
    preparation_seconds="${chaos_verifier_self_check_seconds}"
    [[ "${preparation_seconds}" =~ ^[1-9][0-9]*$ ]] || fail 'the full verifier preparation bound is missing'
    # A suite executes the full verifier self-check once, before its first run, within the
    # preparation every shard's step funds, so no entry's own budget pays for it.
    for suite in $(jq -r '.suites | keys[]' "${chaos_dir}/suites.json"); do
        "${chaos_dir}/suite.sh" shards "${suite}" | jq -e --argjson preparation "${preparation_seconds}" '
            length > 0 and all(.[]; (.preparation_minutes | type) == "number"
                                    and .preparation_minutes * 60 >= $preparation
                                    and .step_minutes >= .execution_minutes + .preparation_minutes)
        ' >/dev/null || fail "the ${suite} shards do not fund the full verifier preparation beside their runs"
    done
    # The baseline execution allowance is three minutes for one node and four for three nodes.
    jq -e '
        [.suites[].entries[] | select(.run[0] == "baseline")]
        | length > 0 and all(.[];
            (.run | index("--nodes")) as $node_option
            | .run[$node_option + 1] as $nodes
            | (if $nodes == "1" then 180 else 240 end) as $execution
            | .budget_minutes * 60 >= $execution)
    ' "${chaos_dir}/suites.json" >/dev/null \
        || fail 'a baseline suite budget cannot fund baseline execution'
    printf 'suite preparation and baseline execution budget controls passed\n'
fi
[[ "${selection}" != budgets ]] || exit 0

directory_refusal=false
mktemp() {
    if [[ "${directory_refusal}" == true ]]; then
        printf '%s\n' "${artifact_dir}/unadmitted"
        return 73
    fi
    command mktemp "$@"
}
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT
# shellcheck source=../output-boundary.sh
source "${chaos_dir}/output-boundary.sh"

remaining_seconds() {
    local remaining=$((overall_deadline - SECONDS))
    if ((remaining < 0)); then remaining=0; fi
    printf '%s\n' "${remaining}"
}

# Polling advances the controller's clock without waiting on real broker or Docker processes.
sleep() {
    SECONDS=$((SECONDS + $1))
}

broker_offset_query() {
    printf '%s\n' "${compose_call_timeout:-120}" >>"${artifact_dir}/limits.txt"
    local attempt response
    attempt="$(wc -l <"${artifact_dir}/limits.txt")"
    response="$(sed -n "${attempt}p" "${artifact_dir}/responses.txt")"
    case "${response}" in
        failure)
            printf 'broker observation refused\n' >&2
            return 73
            ;;
        invalid) printf 'chaos_output:0:unknown\n' ;;
        missing) printf 'another_topic:0:24\n' ;;
        duplicate) printf 'chaos_output:0:24\nchaos_output:0:25\n' ;;
        moving) printf 'chaos_output:0:%s\n' "$((24 + attempt))" ;;
        *) printf 'chaos_output:0:%s\n' "${response:-24}" ;;
    esac
}

begin_case() {
    artifact_dir="${tmp_dir}/$1"
    mkdir -p "${artifact_dir}/traffic" "${artifact_dir}/results"
    : >"${artifact_dir}/limits.txt"
    : >"${artifact_dir}/responses.txt"
    # Clearing Bash's special timer before assignment makes this the controlled test clock.
    # Host time must not advance a simulated schedule while evidence commands are executing.
    unset SECONDS
    SECONDS=0
    overall_deadline="$2"
}

run_boundary() {
    status=0
    wait_for_stable_output 24 >"${artifact_dir}/value.txt" 2>"${artifact_dir}/error.txt" || status=$?
}

begin_case exhausted 0
run_boundary
[[ "${status}" -eq 124 ]] || fail 'exhausted overall budget did not return 124'
jq -e '.status == "failed" and .reason == "overall-deadline" and .category == "controller"
    and .observations == 0 and .overall_remaining_seconds == 0' \
    "${artifact_dir}/results/output-boundary.json" >/dev/null \
    || fail 'exhausted overall budget was not attributed before an observation'
[[ ! -s "${artifact_dir}/limits.txt" ]] || fail 'an exhausted run opened a broker observation'

begin_case stable 1000
run_boundary
[[ "${status}" -eq 0 && "$(cat "${artifact_dir}/value.txt")" == 24 ]] || fail 'stable offset was not returned'
jq -e '.status == "passed" and .reason == "stable" and .observations == 3 and .stable_polls == 2' \
    "${artifact_dir}/results/output-boundary.json" >/dev/null || fail 'three successful stable reads were not required'
awk '$1 < 1 || $1 > 30 { bad = 1 } END { exit bad }' "${artifact_dir}/limits.txt" \
    || fail 'an offset command could outlive the stability observation budget'

begin_case gap 1000
printf '%s\n' 24 24 failure 24 24 24 >"${artifact_dir}/responses.txt"
run_boundary
[[ "${status}" -eq 0 && "$(wc -l <"${artifact_dir}/limits.txt")" -eq 6 ]] \
    || fail 'an observation failure did not reset consecutive stable reads'
jq -se 'any(.[]; .command_exit_code == 73 and .outcome == "command-failed")' \
    "${artifact_dir}"/traffic/offsets/*/result.json >/dev/null || fail 'the exact failed broker command was lost'
grep -q 'broker observation refused' "${artifact_dir}"/traffic/offsets/*/stderr.txt || fail 'broker stderr was lost'

begin_case moving 1000
for _ in {1..40}; do printf 'moving\n'; done >"${artifact_dir}/responses.txt"
run_boundary
[[ "${status}" -eq 124 ]] || fail 'moving output boundary passed'
jq -e '.status == "failed" and .reason == "unstable-output" and .category == "observation"
    and .valid_observations > 1 and .overall_remaining_seconds > 0' \
    "${artifact_dir}/results/output-boundary.json" >/dev/null || fail 'moving output was confused with the overall deadline'

begin_case refused 1000
for _ in {1..40}; do printf 'failure\n'; done >"${artifact_dir}/responses.txt"
run_boundary
[[ "${status}" -eq 124 ]] || fail 'failed observations passed'
jq -e '.reason == "observations-failed" and .category == "observation" and .valid_observations == 0' \
    "${artifact_dir}/results/output-boundary.json" >/dev/null || fail 'observer refusal was confused with product delivery'

begin_case clipped 2
run_boundary
[[ "${status}" -eq 124 ]] || fail 'a short overall budget was extended'
jq -e '.reason == "overall-deadline" and .category == "controller"' \
    "${artifact_dir}/results/output-boundary.json" >/dev/null || fail 'the clipped observation lost its deadline owner'
awk '$1 < 1 || $1 > 2 { bad = 1 } END { exit bad }' "${artifact_dir}/limits.txt" \
    || fail 'the command ignored the remaining whole-run budget'

begin_case refused-after-observation 1000
printf '24\n' >"${artifact_dir}/responses.txt"
for _ in {1..40}; do printf 'failure\n'; done >>"${artifact_dir}/responses.txt"
run_boundary
[[ "${status}" -eq 124 ]] || fail 'a refused query after one observation passed'
jq -e '.reason == "observations-failed" and .valid_observations == 1 and .last_query_exit_code == 73' \
    "${artifact_dir}/results/output-boundary.json" >/dev/null || fail 'a later command refusal was described as moving output'

begin_case below-minimum 1000
for _ in {1..40}; do printf '23\n'; done >"${artifact_dir}/responses.txt"
run_boundary
[[ "${status}" -eq 124 ]] || fail 'an offset below the accepted input passed'
jq -e '.reason == "below-minimum" and .last_offset == 23 and .stable_polls == 0' \
    "${artifact_dir}/results/output-boundary.json" >/dev/null || fail 'the accepted-input minimum was lost'

begin_case below-gap 1000
printf '%s\n' 24 24 23 24 24 24 >"${artifact_dir}/responses.txt"
run_boundary
[[ "${status}" -eq 0 && "$(wc -l <"${artifact_dir}/limits.txt")" -eq 6 ]] \
    || fail 'an offset below the minimum did not reset stable reads'

begin_case expired-query 0
status=0
topic_end_offset chaos_output >"${artifact_dir}/value.txt" || status=$?
[[ "${status}" -eq 124 && ! -s "${artifact_dir}/limits.txt" ]] || fail 'an expired query opened a broker command'
jq -e '.outcome == "overall-deadline" and .command_exit_code == 124 and .effective_seconds == 0' \
    "${artifact_dir}"/traffic/offsets/*/result.json >/dev/null || fail 'the expired query lost its operation record'

begin_case expired-observation 1000
status=0
topic_end_offset chaos_output -1 >"${artifact_dir}/value.txt" || status=$?
[[ "${status}" -eq 124 && ! -s "${artifact_dir}/limits.txt" ]] || fail 'an expired observation opened a broker command'
jq -e '.outcome == "query-deadline" and .command_exit_code == 124
    and .requested_seconds == 0 and .effective_seconds == 0' \
    "${artifact_dir}"/traffic/offsets/*/result.json >/dev/null || fail 'an expired observation was assigned to the whole-run deadline'

for refusal in invalid missing duplicate; do
    begin_case "${refusal}" 1000
    printf '%s\n' "${refusal}" >"${artifact_dir}/responses.txt"
    status=0
    topic_end_offset chaos_output >"${artifact_dir}/value.txt" || status=$?
    [[ "${status}" -eq 1 && ! -s "${artifact_dir}/value.txt" ]] || fail "${refusal} offset output was accepted"
    jq -e '.command_exit_code == 0 and .outcome == "invalid-offset" and .offset == null' \
        "${artifact_dir}"/traffic/offsets/*/result.json >/dev/null || fail "${refusal} raw command outcome was lost"
done

begin_case directory-refusal 1000
mkdir "${artifact_dir}/unadmitted"
directory_refusal=true
status=0
topic_end_offset chaos_output >"${artifact_dir}/value.txt" || status=$?
directory_refusal=false
[[ "${status}" -eq 73 && ! -s "${artifact_dir}/limits.txt" \
    && ! -e "${artifact_dir}/unadmitted/result.json" ]] \
    || fail 'an evidence-directory refusal was consumed as a successful observation'

# Exercise the controller's provider with a foreign input stream. Kafka administration owns no
# stdin, so the Compose adapter must neither consume it nor create an interactive terminal.
sed -n '/^broker_admin() {$/,/^}$/p' "${chaos_dir}/run-baseline.sh" >"${tmp_dir}/broker-provider.sh"
# The generated fragment is the current controller's provider, exercised by the contract below.
# shellcheck source=/dev/null
source "${tmp_dir}/broker-provider.sh"
# The generated production providers invoke this callback; ShellCheck cannot follow that fragment.
# shellcheck disable=SC2329
compose() {
    local no_tty=false interactive=false argument _input
    for argument in "$@"; do
        if [[ "${argument}" == -T ]]; then no_tty=true; fi
        if [[ "${argument}" == --interactive=false ]]; then interactive=true; fi
    done
    [[ "${no_tty}" == true && "${interactive}" == true ]] || return 73
    if IFS= read -r _input; then return 74; fi
    printf 'chaos_output:0:24\n'
}
status=0
# A here-string, not a pipe: a provider that never reads its input can finish before a piped
# writer does, and pipefail would then report the writer's SIGPIPE as the provider's failure.
broker_admin /opt/kafka/bin/kafka-get-offsets.sh <<<'foreign input' \
    >"${tmp_dir}/provider.txt" || status=$?
[[ "${status}" -eq 0 && "$(cat "${tmp_dir}/provider.txt")" == 'chaos_output:0:24' ]] \
    || fail 'broker administration retained foreign stdin or interactive terminal ownership'

begin_case query-provider 1000
# Load the current providers, so this contract covers the actual controller composition.
sed -n '/^broker_offset_query() {$/,/^}$/p' "${chaos_dir}/run-baseline.sh" >>"${tmp_dir}/broker-provider.sh"
# shellcheck source=/dev/null
source "${tmp_dir}/broker-provider.sh"
compose() {
    [[ "$1" == exec && "$2" == -T && "$3" == broker ]] || return 73
    if IFS= read -r _input; then return 74; fi
    printf 'chaos_output:0:24\n'
}
status=0
topic_end_offset chaos_output >"${artifact_dir}/value.txt" || status=$?
[[ "${status}" -eq 0 && "$(cat "${artifact_dir}/value.txt")" == 24 ]] \
    || fail 'an offset observation included an ephemeral helper lifetime'

printf 'output boundary observation controls passed\n'
