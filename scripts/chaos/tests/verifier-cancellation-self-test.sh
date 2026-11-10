#!/usr/bin/env bash
# Ends a verifier self-check through its controller's production path while the production rate
# probe holds its namespace owners: once at the check's deadline, and once by its controller while
# the check still executes. Each time nothing of the check may outlive it: no process stays in its
# session, nothing carries the run label the probe recorded, the probe's records up to that moment
# stay in the check's evidence directory, and the controller's cleanup evidence names exactly the
# namespace owners and network the probe created. Needs Docker and the pinned probe images.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
chaos_dir="$(cd "${script_dir}/.." && pwd)"
# shellcheck source=../tool-images.sh
source "${chaos_dir}/tool-images.sh"
# shellcheck source=../verifier-self-check.sh
source "${chaos_dir}/verifier-self-check.sh"

fail() {
    printf 'verifier cancellation self-test failed: %s\n' "$*" >&2
    exit 1
}

docker_cli="$(command -v docker)" || fail 'the docker CLI is required'
tmp_dir="$(mktemp -d)"
cases=(deadline controller)
evidence=""

# A failed control ends and removes what its check left, as the controller would, and keeps the
# check's console and evidence.
cancellation_cleanup() {
    local status=$?
    if [[ -n "${chaos_verifier_pid}" ]]; then
        chaos_verifier_end "${evidence}" >/dev/null 2>&1 || true
    fi
    local case_name
    for case_name in "${cases[@]}"; do
        chaos_verifier_reclaim "${tmp_dir}/${case_name}/verifier-self-test" >/dev/null 2>&1 || true
    done
    if [[ "${status}" -eq 0 ]]; then
        rm -rf "${tmp_dir}"
    else
        printf 'verifier cancellation self-test evidence retained in %s\n' "${tmp_dir}" >&2
    fi
}
trap cancellation_cleanup EXIT

for image in "${chaos_probe_image}" "${chaos_nettools_image}"; do
    if ! timeout --foreground --kill-after=5s 20s docker image inspect "${image}" >/dev/null 2>&1; then
        timeout --foreground --kill-after=5s 180s docker pull "${image}" >/dev/null \
            || fail "pinned tool image ${image} is unavailable"
    fi
done

# The check's Docker calls pass through this shim. It holds the probe's first link change, which
# follows the probe's first completed transfer, and every later call, until the check's controller
# ends them. The probe's own exit trap therefore removes nothing, so only the controller can.
mkdir -p "${tmp_dir}/shim"
cat >"${tmp_dir}/shim/docker" <<'EOF'
#!/usr/bin/env bash
if [[ ! -e "${CHAOS_CANCELLATION_GATE}" && "$1" == run && " $* " == *' --entrypoint tc '* ]]; then
    # The probe itself makes this call, so the shim's parent is the probe.
    printf '%s\n%s\n' "${PPID}" "$*" >"${CHAOS_CANCELLATION_GATE}.partial"
    mv "${CHAOS_CANCELLATION_GATE}.partial" "${CHAOS_CANCELLATION_GATE}"
fi
if [[ -e "${CHAOS_CANCELLATION_GATE}" ]]; then
    while true; do
        sleep 1
    done
fi
exec "${CHAOS_CANCELLATION_DOCKER}" "$@"
EOF
# It stands in for tests/self-test.sh: it takes --evidence as the full check does and executes the
# production rate probe as its child.
cat >"${tmp_dir}/self-test.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[[ "$#" -eq 2 && "$1" == --evidence && -n "$2" ]] || exit 2
export CHAOS_SELF_TEST_EVIDENCE="$2"
export PATH="${CHAOS_CANCELLATION_SHIM}:${PATH}"
trap 'printf "verifier stand-in ended\n"' EXIT
"${CHAOS_CANCELLATION_PROBE}"
EOF
chmod +x "${tmp_dir}/shim/docker" "${tmp_dir}/self-test.sh"
export CHAOS_CANCELLATION_DOCKER="${docker_cli}"
export CHAOS_CANCELLATION_SHIM="${tmp_dir}/shim"
export CHAOS_CANCELLATION_PROBE="${script_dir}/rate-probe-self-test.sh"

# Lists the Docker resources whose run label the probe that held its owners, process PROBE, chose.
probe_resources() {
    local probe="$1"
    local listing=""
    local kind part
    for kind in container network volume; do
        local arguments=(--format "${kind} {{.ID}} {{.Label \"io.nervix.chaos.run\"}}")
        case "${kind}" in
            container) arguments+=(--all) ;;
            volume) arguments=(--format "${kind} {{.Name}} {{.Label \"io.nervix.chaos.run\"}}") ;;
        esac
        part="$(timeout --foreground --kill-after=5s 30s docker "${kind}" ls \
            --filter label=io.nervix.chaos.run "${arguments[@]}")" \
            || fail "Docker could not list the chaos ${kind}s"
        listing+="${part}"$'\n'
    done
    awk -v prefix="rate-probe-check-${probe}-" 'index($3, prefix) == 1' <<<"${listing}"
}

# Proves that the check that led SESSION left nothing behind, judged from its evidence directory
# EVIDENCE, and reports every way it did.
expect_nothing_left() {
    local case_name="$1"
    local session="$2"
    local evidence="$3"
    local probe
    probe="$(sed -n 1p "${CHAOS_CANCELLATION_GATE}")"
    local problems=()
    local left
    left="$(ps -o pid=,args= -s "${session}" 2>/dev/null || true)"
    [[ -z "${left}" ]] || problems+=("processes of the check outlived it in its session: ${left//$'\n'/, }")
    if { tr '\0' ' ' <"/proc/${probe}/cmdline"; } 2>/dev/null | grep -q 'rate-probe-self-test.sh'; then
        problems+=("the rate probe ${probe} outlived the check")
    fi
    local owned
    owned="$(probe_resources "${probe}")"
    [[ -z "${owned}" ]] || problems+=("the probe's namespace owners and network outlived the check: ${owned//$'\n'/, }")
    local run_id=""
    if [[ -s "${evidence}/run-ids.txt" && "$(grep -c . "${evidence}/run-ids.txt")" -eq 1 ]]; then
        run_id="$(cat "${evidence}/run-ids.txt")"
    fi
    [[ "${run_id}" =~ ^rate-probe-check-${probe}-[0-9]+$ ]] \
        || problems+=("the check's evidence directory does not record the probe's run label alone")
    local record="${evidence}/rate-probe/launcher-delay.probe.json"
    if ! jq -e '.received_bytes == 262144 and (.sender_container_id | test("^[0-9a-f]{64}$"))
        and (.receiver_container_id | test("^[0-9a-f]{64}$"))' "${record}" >/dev/null 2>&1; then
        problems+=("the probe's completed transfer record is not in the check's evidence directory")
    elif [[ -n "${run_id}" ]]; then
        local cleanup="${evidence}/cleanup/${run_id}"
        jq -e '.containers == 3 and .pumba_sidecars == 0 and .networks == 1 and .volumes == 0
            and .remaining == {containers: 0, networks: 0, volumes: 0} and .exit_code == 0' \
            "${cleanup}/cleanup.json" >/dev/null 2>&1 \
            || problems+=("the controller's cleanup did not remove the three namespace owners and the network: $(cat "${cleanup}/cleanup.json" 2>/dev/null || printf 'no cleanup evidence')")
        jq -e --slurpfile record "${record}" '[.[].Id] as $ids
            | ($record[0].sender_container_id | IN($ids[])) and ($record[0].receiver_container_id | IN($ids[]))' \
            "${cleanup}/containers.json" >/dev/null 2>&1 \
            || problems+=("the controller's cleanup evidence does not name the namespace owners the probe recorded")
    fi
    if ((${#problems[@]} > 0)); then
        fail "${case_name}: $(printf '%s; ' "${problems[@]}")"
    fi
    printf 'verifier cancellation: %s: the controller removed %s of %s; the probe kept %d records\n' \
        "${case_name}" "$(jq -r '"\(.containers) containers and \(.networks) network"' \
            "${evidence}/cleanup/${run_id}/cleanup.json")" "${run_id}" \
        "$(find "${evidence}/rate-probe" -name '*.probe.json' | wc -l)"
}

for case_name in "${cases[@]}"; do
    case_dir="${tmp_dir}/${case_name}"
    evidence="${case_dir}/verifier-self-test"
    mkdir -p "${case_dir}"
    export CHAOS_CANCELLATION_GATE="${case_dir}/held"
    # The bound is far longer than the probe needs to reach the held call, so only this control
    # ends the check.
    chaos_verifier_start 600 "${evidence}" "${case_dir}/verifier-self-test.txt" "${tmp_dir}/self-test.sh"
    session="${chaos_verifier_pid}"
    session_start="${chaos_verifier_start_ticks}"
    for ((waited = 0; ; waited++)); do
        [[ ! -e "${CHAOS_CANCELLATION_GATE}" ]] || break
        process_executes "${session}" "${session_start}" \
            || fail "${case_name}: the check ended before the probe held its owners: $(tail -n 20 "${case_dir}/verifier-self-test.txt")"
        ((waited < 3000)) || fail "${case_name}: the probe did not reach its first link change within 300 s"
        sleep 0.1
    done
    case "${case_name}" in
        deadline)
            # The deadline arrives as the bound's own timer signal, so it ends the check exactly
            # while the probe holds its owners.
            kill -ALRM "${session}"
            status=0
            wait "${session}" || status=$?
            [[ "${status}" -eq 124 || "${status}" -eq 137 ]] \
                || fail "deadline: the bound exited ${status}, not as a deadline"
            ;;
        controller) ;;
    esac
    chaos_verifier_end "${evidence}" || fail "${case_name}: cleanup could not remove what the probe left"
    expect_nothing_left "${case_name}" "${session}" "${evidence}"
done

printf 'verifier cancellation self-test passed\n'
