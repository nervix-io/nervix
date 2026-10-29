#!/usr/bin/env bash
set -euo pipefail

chaos_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../tool-images.sh
source "${chaos_dir}/tool-images.sh"
artifact_dir="${CHAOS_NETWORK_PREFLIGHT_ARTIFACTS:-$(mktemp -d)}"
mkdir -p "${artifact_dir}/bin"
run_id="preflight-control-$$-${RANDOM}"
fail() { printf 'network preflight self-test failed: %s\n' "$*" >&2; exit 1; }
cleanup() {
    "${chaos_dir}/cleanup.sh" --run-id "${run_id}" --quiet >/dev/null 2>&1 || true
    if [[ -z "${CHAOS_NETWORK_PREFLIGHT_ARTIFACTS:-}" ]]; then
        rm -rf "${artifact_dir}"
    fi
}
trap cleanup EXIT

export CHAOS_NETWORK_TEST_DOCKER
CHAOS_NETWORK_TEST_DOCKER="$(command -v docker)"

# Pumba uses the daemon directly with --pull-image=false. Prepare its helper before a fault starts;
# an inspector's later Docker run must not be the first operation that supplies that image.
prepare_tool_image() {
    local tool="$1" reference="$2" directory="${artifact_dir}/image-preparation/$1" status=0
    mkdir -p "${directory}"
    printf '%s\n' "${reference}" >"${directory}/reference.txt"
    timeout --foreground --kill-after=10s 20s "${CHAOS_NETWORK_TEST_DOCKER}" image inspect \
        --format '{{.Id}}' "${reference}" \
        >"${directory}/inspect.stdout.txt" 2>"${directory}/inspect.stderr.txt" || status=$?
    printf '%s\n' "${status}" >"${directory}/inspect.exit-code.txt"
    if ((status != 0)); then
        status=0
        timeout --foreground --kill-after=10s 120s "${CHAOS_NETWORK_TEST_DOCKER}" pull "${reference}" \
            >"${directory}/pull.stdout.txt" 2>"${directory}/pull.stderr.txt" || status=$?
        printf '%s\n' "${status}" >"${directory}/pull.exit-code.txt"
        ((status == 0)) || fail "${tool} image preparation returned ${status}; see ${directory}/pull.stderr.txt"
        timeout --foreground --kill-after=10s 20s "${CHAOS_NETWORK_TEST_DOCKER}" image inspect \
            --format '{{.Id}}' "${reference}" \
            >"${directory}/confirm.stdout.txt" 2>"${directory}/confirm.stderr.txt" || status=$?
        printf '%s\n' "${status}" >"${directory}/confirm.exit-code.txt"
        ((status == 0)) || fail "${tool} image confirmation returned ${status}; see ${directory}/confirm.stderr.txt"
    fi
}
prepare_tool_image pumba "${chaos_pumba_image}"
prepare_tool_image nettools "${chaos_nettools_image}"
prepare_tool_image probe "${chaos_probe_image}"

export CHAOS_NETWORK_TEST_STATE="${artifact_dir}/kind"
export CHAOS_NETWORK_TEST_CASE
export CHAOS_NETWORK_TEST_RUN_ID="${run_id}"
export CHAOS_NETWORK_TEST_PROBE="${chaos_probe_image}"
export CHAOS_NETWORK_TEST_SIDECAR="${artifact_dir}/sidecar-id"
export CHAOS_NETWORK_TEST_PENDING_NETEM="${artifact_dir}/pending-netem-id"
cat >"${artifact_dir}/bin/docker" <<'WRAPPER'
#!/usr/bin/env bash
set -euo pipefail
inspector=false
fault=false
kind=""
for argument in "$@"; do
    case "${argument}" in
        io.nervix.chaos.role=fault-inspector) inspector=true ;;
        io.nervix.chaos.role=fault) fault=true ;;
        netem|iptables) kind="${argument}" ;;
    esac
done
if [[ "${fault}" == true ]]; then
    printf '%s\n' "${kind}" >"${CHAOS_NETWORK_TEST_STATE}"
    if [[ "${kind}" == netem && "${CHAOS_NETWORK_TEST_CASE}" == netem-starting ]]; then
        # Hold the real injector until the controller observes the incomplete installation.
        arguments=(create)
        for argument in "${@:2}"; do
            if [[ "${argument}" != --detach ]]; then
                arguments+=("${argument}")
            fi
        done
        "${CHAOS_NETWORK_TEST_DOCKER}" "${arguments[@]}" >"${CHAOS_NETWORK_TEST_PENDING_NETEM}"
        cat "${CHAOS_NETWORK_TEST_PENDING_NETEM}"
        exit 0
    fi
    if [[ "${kind}" == iptables && "${CHAOS_NETWORK_TEST_CASE}" == injector-exit ]]; then
        # A real failed injector, with retained Docker state and an attributable raw log.
        arguments=()
        for argument in "$@"; do
            if [[ "${argument}" == "${CHAOS_NETWORK_TEST_NETTOOLS}" ]]; then
                arguments+=(nervix-preflight-control:unavailable)
            else
                arguments+=("${argument}")
            fi
        done
        exec "${CHAOS_NETWORK_TEST_DOCKER}" "${arguments[@]}"
    fi
fi
if [[ "${CHAOS_NETWORK_TEST_CASE}" == netem-starting && -s "${CHAOS_NETWORK_TEST_PENDING_NETEM}" ]]; then
    if [[ "${inspector}" == true && ! -e "${CHAOS_NETWORK_TEST_PENDING_NETEM}.observed" ]]; then
        # A prio root and loss leaf precede the classifier that sends gateway traffic to it.
        : >"${CHAOS_NETWORK_TEST_PENDING_NETEM}.observed"
        printf '# qdisc\nqdisc prio 504d: root bands 3\nqdisc netem 5050: parent 504d:3 limit 1000 loss 100%%\n# filter\n# iptables\n-P INPUT ACCEPT\n'
        exit 0
    fi
    if [[ "$1" == inspect && "${3:-}" == '{{.State.Running}}' \
        && "${4:-}" == "$(cat "${CHAOS_NETWORK_TEST_PENDING_NETEM}")" \
        && ! -e "${CHAOS_NETWORK_TEST_PENDING_NETEM}.started" ]]; then
        "${CHAOS_NETWORK_TEST_DOCKER}" start "$4" >/dev/null
        : >"${CHAOS_NETWORK_TEST_PENDING_NETEM}.started"
    fi
fi
if [[ "${CHAOS_NETWORK_TEST_CASE}" == canary-launch && "$1" == run && "${fault}" == false && "${inspector}" == false ]]; then
    "${CHAOS_NETWORK_TEST_DOCKER}" "$@"
    printf 'deliberate Docker client refusal after canary creation\n' >&2
    exit 81
fi
if [[ "${CHAOS_NETWORK_TEST_CASE}" == sidecar-vanishes || "${CHAOS_NETWORK_TEST_CASE}" == sidecar-inspect-failure ]]; then
    if [[ "$1 $2" == 'container ls' && "$*" == *label=com.gaiaadm.pumba.skip=true* && ! -f "${CHAOS_NETWORK_TEST_SIDECAR}" ]]; then
        "${CHAOS_NETWORK_TEST_DOCKER}" run --detach --rm \
            --label "io.nervix.chaos.run=${CHAOS_NETWORK_TEST_RUN_ID}" \
            --label com.gaiaadm.pumba.skip=true \
            --network "container:$(cat "${CHAOS_NETWORK_TEST_CANARY_ID}")" \
            "${CHAOS_NETWORK_TEST_PROBE}" sleep 300 >"${CHAOS_NETWORK_TEST_SIDECAR}"
    fi
    if [[ "$1" == inspect && "${3:-}" == '{{.HostConfig.NetworkMode}}' && -s "${CHAOS_NETWORK_TEST_SIDECAR}" \
        && "$4" == "$(cat "${CHAOS_NETWORK_TEST_SIDECAR}")" ]]; then
        if [[ "${CHAOS_NETWORK_TEST_CASE}" == sidecar-vanishes ]]; then
            "${CHAOS_NETWORK_TEST_DOCKER}" container rm --force "$4" >/dev/null
            exec "${CHAOS_NETWORK_TEST_DOCKER}" "$@"
        fi
        printf 'deliberate inspection refusal for a still-present sidecar\n' >&2
        exit 78
    fi
    exec "${CHAOS_NETWORK_TEST_DOCKER}" "$@"
fi
if [[ "${CHAOS_NETWORK_TEST_CASE}" == probe-refusal ]]; then
    if [[ -f "${CHAOS_NETWORK_TEST_STATE}" && "$(cat "${CHAOS_NETWORK_TEST_STATE}")" == netem \
        && "$1" == exec && ! -e "${CHAOS_NETWORK_TEST_STATE}.probe-refusal" ]]; then
        : >"${CHAOS_NETWORK_TEST_STATE}.probe-refusal"
        printf 'deliberate Docker exec refusal for the blocked-traffic probe\n' >&2
        exit 125
    fi
    exec "${CHAOS_NETWORK_TEST_DOCKER}" "$@"
fi
if [[ -f "${CHAOS_NETWORK_TEST_STATE}" && "$(cat "${CHAOS_NETWORK_TEST_STATE}")" == iptables ]]; then
    if [[ "${inspector}" == true ]]; then
        if [[ "${CHAOS_NETWORK_TEST_CASE}" == injector-state || "${CHAOS_NETWORK_TEST_CASE}" == injector-exit ]]; then
            printf '# qdisc\nqdisc noqueue 0: root\n# filter\n# iptables\n-P INPUT ACCEPT\n'
            exit 0
        fi
        if [[ "${CHAOS_NETWORK_TEST_CASE}" == inspector-timeout ]]; then
            "${CHAOS_NETWORK_TEST_DOCKER}" "$@"
            printf 'deliberate blocked Docker client after inspector exit\n' >&2
            sleep 31
            exit 0
        fi
        printf 'deliberate iptables inspector refusal\n' >&2
        exit 73
    fi
    if [[ "${CHAOS_NETWORK_TEST_CASE}" == injector-state && "$1" == inspect && "${3:-}" == '{{.State.Running}}' ]]; then
        printf 'deliberate Docker injector-state refusal\n' >&2
        exit 78
    fi
    if [[ "${CHAOS_NETWORK_TEST_CASE}" == log-failure && "$1" == logs ]]; then
        printf 'deliberate Docker log refusal\n' >&2
        exit 79
    fi
    if [[ "${CHAOS_NETWORK_TEST_CASE}" == cleanup-failure && "$1 $2" == 'container rm' && "$4" != "$(cat "${CHAOS_NETWORK_TEST_CANARY_ID}")" ]]; then
        printf 'deliberate injector cleanup refusal\n' >&2
        exit 80
    fi
fi
exec "${CHAOS_NETWORK_TEST_DOCKER}" "$@"
WRAPPER
chmod +x "${artifact_dir}/bin/docker"
export CHAOS_NETWORK_TEST_NETTOOLS="${chaos_nettools_image}"
export CHAOS_NETWORK_TEST_CANARY_ID

cases=(inspect-failure inspector-timeout injector-state log-failure cleanup-failure canary-launch
    injector-exit sidecar-vanishes sidecar-inspect-failure probe-refusal netem-starting)
if [[ "$#" -gt 0 ]]; then
    cases=("$@")
fi
for test_case in "${cases[@]}"; do
    case "${test_case}" in
        inspect-failure|inspector-timeout|injector-state|log-failure|cleanup-failure|canary-launch|injector-exit|sidecar-vanishes|sidecar-inspect-failure|probe-refusal|netem-starting) ;;
        *) fail "unknown preflight control ${test_case}" ;;
    esac
    CHAOS_NETWORK_TEST_CASE="${test_case}"
    CHAOS_NETWORK_TEST_CANARY_ID="${artifact_dir}/${test_case}/canary-id.txt"
    rm -f "${CHAOS_NETWORK_TEST_STATE}" "${CHAOS_NETWORK_TEST_SIDECAR}" "${CHAOS_NETWORK_TEST_STATE}.probe-refusal"
    rm -f "${CHAOS_NETWORK_TEST_PENDING_NETEM}" "${CHAOS_NETWORK_TEST_PENDING_NETEM}.observed" \
        "${CHAOS_NETWORK_TEST_PENDING_NETEM}.started"
    status=0
    PATH="${artifact_dir}/bin:${PATH}" timeout --foreground --kill-after=10s 120s \
        "${chaos_dir}/network-faults.sh" preflight --run-id "${run_id}" \
        --pumba "${chaos_pumba_image}" --nettools "${chaos_nettools_image}" \
        --probe "${chaos_probe_image}" --output "${artifact_dir}/${test_case}" \
        >"${artifact_dir}/${test_case}.log" 2>&1 || status=$?
    expected=73
    stage=iptables.inspect-rules
    case "${test_case}" in
        inspector-timeout) expected=124 ;;
        injector-state) expected=78; stage=iptables.inspect-injector-running ;;
        canary-launch) expected=81; stage=canary.launch ;;
        injector-exit) expected=1; stage=iptables.installation ;;
        sidecar-vanishes) expected=0; stage=iptables.gateway-probe ;;
        sidecar-inspect-failure) expected=1; stage=cleanup ;;
        probe-refusal) expected=125; stage=netem.gateway-probe ;;
    esac
    [[ "${status}" -eq "${expected}" ]] || fail "${test_case} returned ${status}, expected ${expected}"
    if [[ "${test_case}" == netem-starting ]]; then
        [[ -f "${CHAOS_NETWORK_TEST_PENDING_NETEM}.observed" && -f "${CHAOS_NETWORK_TEST_PENDING_NETEM}.started" ]] \
            || fail 'partial netem installation did not wait for the real injector'
    fi
    result="${artifact_dir}/${test_case}/result.json"
    [[ -s "${result}" ]] || fail "${test_case} lost its structured failure"
    verdict=failed
    if [[ "${expected}" -eq 0 ]]; then verdict=succeeded; fi
    jq -e --arg verdict "${verdict}" --arg stage "${stage}" --argjson code "${expected}" \
        '.status == $verdict and .stage == $stage and .exit_code == $code
         and .elapsed_ms >= 0 and (.canary_id | length > 0)' "${result}" >/dev/null \
        || fail "${test_case} lost its stage, status, time or canary identity"
    if [[ "${test_case}" != injector-exit && "${test_case}" != sidecar-inspect-failure ]]; then
        jq -se --arg stage "${stage}" --argjson code "${expected}" \
            'any(.[]; .stage == $stage and .exit_code == $code and .elapsed_ms >= 0
             and .started_unix_ns > 0 and (.argv | length > 0))' \
            "${artifact_dir}/${test_case}/operations.ndjson" >/dev/null || fail "${test_case} lost its command outcome"
    fi
    if [[ "${test_case}" == inspector-timeout ]]; then
        jq -se 'any(.[]; .stage == "iptables.inspect-rules" and .exit_code == 124
            and .timeout_seconds == 30 and .elapsed_ms >= 30000)' \
            "${artifact_dir}/${test_case}/operations.ndjson" >/dev/null \
            || fail 'the real inspector deadline did not retain its elapsed time and outcome'
    fi
    if [[ "${test_case}" != canary-launch && "${test_case}" != sidecar-* && "${test_case}" != probe-refusal ]]; then
        jq -e '.injector_id | length > 0' "${result}" >/dev/null || fail 'failure lost its injector identity'
        [[ -f "${artifact_dir}/${test_case}/iptables-pumba.txt" ]] || fail 'cleanup lost the raw injector log'
        [[ -s "${artifact_dir}/${test_case}/iptables-injector.json" ]] || fail 'cleanup lost the injector inspection'
    fi
    if [[ "${test_case}" == injector-exit ]]; then
        jq -e '.[0].State.ExitCode != 0' "${artifact_dir}/${test_case}/iptables-injector.json" >/dev/null \
            || fail 'failed injector lost its exit status'
        grep -Fq 'nervix-preflight-control:unavailable' "${artifact_dir}/${test_case}/iptables-pumba.txt" \
            || fail 'failed injector lost its raw refusal'
    fi
    if [[ "${test_case}" == log-failure || "${test_case}" == cleanup-failure || "${test_case}" == sidecar-inspect-failure ]]; then
        jq -e '.cleanup_exit_code != 0' "${result}" >/dev/null || fail 'cleanup failure was presented as success'
    else
        jq -e '.cleanup_exit_code == 0' "${result}" >/dev/null || fail 'ordinary failure did not clean up'
    fi
    if [[ "${test_case}" == sidecar-vanishes ]]; then
        grep -Fq 'disappeared before inspection' "${artifact_dir}/${test_case}/sidecars.txt" \
            || fail 'concurrent disappearance lost its explicit outcome'
    fi
    if [[ "${test_case}" == sidecar-inspect-failure ]]; then
        jq -se 'any(.[]; .stage == "cleanup.inspect-sidecar" and .exit_code == 78)' \
            "${artifact_dir}/${test_case}/operations.ndjson" >/dev/null \
            || fail 'live sidecar inspection failure was not retained'
    fi
    # External cleanup owns a helper whose removal deliberately failed; retained IDs allow recovery.
    "${chaos_dir}/cleanup.sh" --run-id "${run_id}" --quiet
    [[ -z "$(docker container ls --all --quiet --filter "label=io.nervix.chaos.run=${run_id}")" ]] \
        || fail "${test_case} left run-owned helpers behind"
    printf '%s retained its operation, primary outcome and cleanup evidence\n' "${test_case}"
done

# A reused output must not overwrite evidence or make a prior cidfile a cleanup authority.
retained_output="${artifact_dir}/${cases[0]}"
retained_digest="$(sha256sum "${retained_output}/result.json")"
status=0
"${chaos_dir}/network-faults.sh" preflight --run-id "${run_id}" \
    --pumba "${chaos_pumba_image}" --nettools "${chaos_nettools_image}" \
    --probe "${chaos_probe_image}" --output "${retained_output}" \
    >"${artifact_dir}/reused-output.log" 2>&1 || status=$?
[[ "${status}" -eq 2 ]] || fail 'a reused evidence directory was accepted'
[[ "$(sha256sum "${retained_output}/result.json")" == "${retained_digest}" ]] \
    || fail 'output reuse changed the retained failure'

# Actual pinned Pumba/nettools, with the same loss strength, observation count and timeouts.
status=0
timeout --foreground --kill-after=10s 120s "${chaos_dir}/network-faults.sh" preflight \
    --run-id "${run_id}" --pumba "${chaos_pumba_image}" --nettools "${chaos_nettools_image}" \
    --probe "${chaos_probe_image}" --output "${artifact_dir}/qualified" \
    >"${artifact_dir}/qualified.log" 2>&1 || status=$?
[[ "${status}" -eq 0 ]] || fail "actual network preflight returned ${status}; see ${artifact_dir}/qualified"
jq -e '.status == "succeeded" and .exit_code == 0 and .cleanup_exit_code == 0' \
    "${artifact_dir}/qualified/result.json" >/dev/null || fail 'actual preflight lacks a complete success verdict'
for kind in netem iptables; do
    grep -Fq "${kind} fault installed, blocked gateway traffic, and healed" "${artifact_dir}/qualified/summary.txt" \
        || fail "${kind} lacks installation/effect/healing evidence"
    [[ -s "${artifact_dir}/qualified/${kind}-pumba.txt" ]] || fail "${kind} lost its raw injector log"
done
[[ -z "$(docker container ls --all --quiet --filter "label=io.nervix.chaos.run=${run_id}")" ]] \
    || fail 'actual preflight left run-owned helpers behind'

# A supplied real product image also qualifies the public runner's setup error projection.
# No node starts: the deliberate inspection refusal ends the run in network preflight.
if [[ -n "${CHAOS_NETWORK_PREFLIGHT_NERVIX_IMAGE:-}" ]]; then
    CHAOS_NETWORK_TEST_CASE=inspect-failure
    rm -f "${CHAOS_NETWORK_TEST_STATE}"
    runner_id="${run_id}-runner"
    status=0
    PATH="${artifact_dir}/bin:${PATH}" just chaos run degraded-links \
        --image "${CHAOS_NETWORK_PREFLIGHT_NERVIX_IMAGE}" --profile delay --records 20 \
        --run-id "${runner_id}" --artifacts "${artifact_dir}/runner" \
        >"${artifact_dir}/runner.log" 2>&1 || status=$?
    [[ "${status}" -eq 2 ]] || fail "public preflight refusal returned ${status}, expected setup status 2"
    jq -e '.status == "failed" and .exit_code == 2 and .final_phase == "preflight"
        and .resources_retained == false' "${artifact_dir}/runner/${runner_id}/manifest.json" >/dev/null \
        || fail 'public preflight refusal lost its failed setup manifest'
    grep -Fq 'network fault preflight failed: iptables.inspect-rules:' "${artifact_dir}/runner.log" \
        || fail 'public setup error lost the actual refused operation'
    jq -e '.stage == "iptables.inspect-rules" and .exit_code == 73' \
        "${artifact_dir}/runner/${runner_id}/diagnostics/network-preflight/result.json" >/dev/null \
        || fail 'public runner lost the underlying refusal status'
    [[ -z "$(docker container ls --all --quiet --filter "label=io.nervix.chaos.run=${runner_id}")" ]] \
        || fail 'public setup refusal left helpers behind'
    printf 'public runner retained the actual network-preflight refusal\n'
fi
printf 'network preflight self-test passed\n'
