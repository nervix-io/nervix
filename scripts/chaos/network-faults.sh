#!/usr/bin/env bash
set -euo pipefail

# Owns the network faults a chaos run installs with Pumba: reading the traffic-control and
# packet-filter state of a container's network namespace, proving on this Docker worker that netem
# and iptables faults take effect and heal, and healing every fault a run owns. Healing removes only
# state Pumba owns: its 504d: root qdisc and its random-mode INPUT DROP rules.

usage() {
    cat >&2 <<'EOF'
usage:
  network-faults.sh inspect --run-id RUN_ID --container ID --nettools IMAGE --output FILE
  network-faults.sh heal --run-id RUN_ID --nettools IMAGE --output FILE
  network-faults.sh preflight --run-id RUN_ID --pumba IMAGE --nettools IMAGE --probe IMAGE --output DIR
EOF
}

bounded() {
    local seconds="$1"
    shift
    timeout --foreground --kill-after=5s "${seconds}s" "$@"
}

command_name="${1:-}"
[[ -n "${command_name}" ]] || { usage; exit 2; }
shift
run_id=""
container_id=""
nettools_image=""
pumba_image=""
probe_image=""
output=""
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --run-id | --container | --nettools | --pumba | --probe | --output)
            [[ "$#" -ge 2 ]] || { usage; exit 2; }
            case "$1" in
                --run-id) run_id="$2" ;;
                --container) container_id="$2" ;;
                --nettools) nettools_image="$2" ;;
                --pumba) pumba_image="$2" ;;
                --probe) probe_image="$2" ;;
                --output) output="$2" ;;
            esac
            shift 2
            ;;
        *)
            printf 'unknown network-faults argument: %s\n' "$1" >&2
            usage
            exit 2
            ;;
    esac
done
[[ "${run_id}" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,95}$ ]] || { printf 'invalid run id: %s\n' "${run_id}" >&2; exit 2; }
[[ -n "${nettools_image}" && -n "${output}" ]] || { usage; exit 2; }

run_label="io.nervix.chaos.run=${run_id}"

# Prints the root qdisc, the filters attached to Pumba's root, and the INPUT chain of the
# container's network namespace, each under its own heading.
inspect_rules() {
    local target="$1"
    local destination="$2"
    local command=(docker run --rm \
        --label "${run_label}" \
        --label io.nervix.chaos.role=fault-inspector \
        --cap-add NET_ADMIN \
        --network "container:${target}" \
        --entrypoint sh \
        "${nettools_image}" -ec '
            printf "%s\n" "# qdisc"
            tc qdisc show dev eth0
            printf "%s\n" "# filter"
            tc filter show dev eth0 parent 504d: 2>/dev/null || true
            printf "%s\n" "# iptables"
            iptables -S INPUT
        ')
    if [[ "${command_name}" == preflight ]]; then
        preflight_run "${preflight_kind}.inspect-rules" 30 "${command[@]}"
        cat "${preflight_stdout}" >"${destination}"
    else
        bounded 30 "${command[@]}" >"${destination}"
    fi
}

# A namespace is healed when its only qdisc is the default root and its INPUT chain has no rules.
rules_are_default() {
    local state="$1"
    awk '
        /^# qdisc$/ { section = "qdisc"; next }
        /^# filter$/ { section = "filter"; next }
        /^# iptables$/ { section = "iptables"; next }
        section == "qdisc" && NF > 0 {
            qdiscs++
            if ($0 ~ /^qdisc (noqueue|fq|fq_codel|pfifo_fast) 0: root/) {
                default_root = 1
            }
        }
        section == "filter" && NF > 0 { filters++ }
        section == "iptables" && /^-A / { rules++ }
        section == "iptables" && $0 == "-P INPUT ACCEPT" { accept_policy = 1 }
        END { exit !(qdiscs == 1 && default_root && filters == 0 && rules == 0 && accept_policy) }
    ' "${state}"
}

# Pumba installs the root, loss leaf and destination classifier in separate commands.
# A partial netem installation must not start the blocked-traffic probe.
preflight_rules_are_installed() {
    local kind="$1" state="$2"
    if [[ "${kind}" == iptables ]]; then
        ! rules_are_default "${state}"
        return
    fi
    awk '
        /^# qdisc$/ { section = "qdisc"; next }
        /^# filter$/ { section = "filter"; next }
        /^# iptables$/ { section = "iptables"; next }
        section == "qdisc" && /^qdisc prio 504d: root / { root = 1 }
        section == "qdisc" && /^qdisc netem 5050: parent 504d:3 / && / loss 100%([[:space:]]|$)/ { loss = 1 }
        section == "filter" && / flowid 504d:3([[:space:]]|$)/ { classifier = 1 }
        END { exit !(root && loss && classifier) }
    ' "${state}"
}

# Deletes Pumba's root qdisc and Pumba's INPUT DROP rules, and nothing else.
remove_owned_rules() {
    local target="$1"
    # The script below is interpreted by the namespace helper, not this controller shell.
    # shellcheck disable=SC2016
    bounded 30 docker run --rm \
        --label "${run_label}" \
        --label io.nervix.chaos.role=fault-inspector \
        --cap-add NET_ADMIN \
        --network "container:${target}" \
        --entrypoint sh \
        "${nettools_image}" -ec '
            root="$(tc qdisc show dev eth0 | grep " root " || true)"
            case "${root}" in
                "qdisc prio 504d: root"* | "qdisc netem 504d: root"*)
                    tc qdisc del dev eth0 root handle 504d:
                    printf "removed Pumba root qdisc 504d:\n"
                    ;;
            esac
            iptables -S INPUT \
                | grep -E "^-A INPUT -s [0-9.]+/32 -i eth0 -m statistic --mode random --probability 1\.0+ -j DROP$" \
                | while read -r rule; do
                    # The rule text has no quoting, so splitting it yields its exact arguments.
                    # shellcheck disable=SC2086
                    set -- ${rule}
                    shift
                    iptables -D "$@"
                    printf "removed Pumba INPUT rule: %s\n" "${rule}"
                done
        '
}

stop_injectors() {
    local report="$1"
    local injectors=()
    mapfile -t injectors < <(
        bounded 20 docker container ls --quiet --no-trunc \
            --filter "label=${run_label}" --filter label=io.nervix.chaos.role=fault
    )
    if ((${#injectors[@]} == 0)); then
        return 0
    fi
    printf 'stopping %d run-owned fault injector(s) with SIGTERM\n' "${#injectors[@]}" >>"${report}"
    bounded 60 docker stop -t 40 "${injectors[@]}" >>"${report}" 2>&1
}

# Pumba runs tc and iptables in a sidecar that joins the target's network namespace and carries
# only Pumba's own label. A sidecar left by an interrupted injector is found through the run-owned
# container whose namespace it joined.
remove_sidecars() {
    local report="$1"
    local owned=()
    local sidecars=()
    mapfile -t owned < <(bounded 20 docker container ls --all --quiet --no-trunc --filter "label=${run_label}")
    mapfile -t sidecars < <(bounded 20 docker container ls --all --quiet --no-trunc --filter label=com.gaiaadm.pumba.skip=true)
    local sidecar owned_id network_mode
    for sidecar in "${sidecars[@]}"; do
        network_mode="$(bounded 20 docker inspect --format '{{.HostConfig.NetworkMode}}' "${sidecar}" 2>/dev/null || true)"
        for owned_id in "${owned[@]}"; do
            if [[ "${network_mode}" == "container:${owned_id}" ]]; then
                printf 'removing Pumba sidecar %s joined to %s\n' "${sidecar}" "${owned_id}" >>"${report}"
                bounded 30 docker container rm --force "${sidecar}" >>"${report}" 2>&1
            fi
        done
    done
}

heal() {
    local report="${output}"
    : >"${report}"
    local status=0
    stop_injectors "${report}" || status=1
    remove_sidecars "${report}" || status=1
    local nodes=()
    mapfile -t nodes < <(
        bounded 20 docker container ls --quiet --no-trunc \
            --filter "label=${run_label}" --filter label=io.nervix.chaos.role=node
    )
    local node state
    for node in "${nodes[@]}"; do
        state="$(mktemp)"
        if ! inspect_rules "${node}" "${state}"; then
            printf 'could not inspect the network namespace of %s\n' "${node}" >>"${report}"
            status=1
            rm -f "${state}"
            continue
        fi
        if ! rules_are_default "${state}"; then
            printf 'owned faults remained on %s after injector shutdown:\n' "${node}" >>"${report}"
            cat "${state}" >>"${report}"
            remove_owned_rules "${node}" >>"${report}" 2>&1 || status=1
            inspect_rules "${node}" "${state}" || status=1
        fi
        printf 'final network state of %s:\n' "${node}" >>"${report}"
        cat "${state}" >>"${report}"
        rules_are_default "${state}" || {
            printf 'network state of %s is not the default after healing\n' "${node}" >>"${report}"
            status=1
        }
        rm -f "${state}"
    done
    return "${status}"
}

preflight_canary=""
preflight_injector=""
preflight_kind=canary
preflight_stage=initialization
preflight_operation=0
preflight_stdout=""
preflight_reason=""
preflight_completed=false
preflight_started_ns=0

# Run in this shell, including launches: a failed command substitution must not hide the stage
# from EXIT. Keep both streams and the exact invocation before returning its original status.
preflight_run() {
    preflight_stage="$1"
    local budget="$2"
    shift 2
    preflight_operation=$((preflight_operation + 1))
    local prefix started_ns finished_ns status=0
    printf -v prefix 'operations/%06d' "${preflight_operation}"
    preflight_stdout="${output}/${prefix}.stdout.txt"
    started_ns="$(date +%s%N)"
    jq -cn --arg stage "${preflight_stage}" --arg run_id "${run_id}" \
        --arg canary "${preflight_canary}" --arg injector "${preflight_injector}" \
        --arg stdout "${prefix}.stdout.txt" --arg stderr "${prefix}.stderr.txt" \
        --argjson sequence "${preflight_operation}" \
        --argjson budget "${budget}" --argjson started_ns "${started_ns}" \
        --args -- \
        '{sequence:$sequence,run_id:$run_id,stage:$stage,argv:$ARGS.positional,
          timeout_seconds:$budget,exit_code:null,started_unix_ns:$started_ns,
          elapsed_ms:null,canary_id:($canary|select(length>0)) // null,
          injector_id:($injector|select(length>0)) // null,stdout:$stdout,stderr:$stderr}' \
        "$@" >"${output}/${prefix}.json"
    bounded "${budget}" "$@" >"${preflight_stdout}" 2>"${output}/${prefix}.stderr.txt" || status=$?
    finished_ns="$(date +%s%N)"
    jq -c --argjson status "${status}" --argjson elapsed_ms "$(((finished_ns - started_ns) / 1000000))" \
        '.exit_code=$status | .elapsed_ms=$elapsed_ms' "${output}/${prefix}.json" >"${output}/${prefix}.json.tmp"
    mv "${output}/${prefix}.json.tmp" "${output}/${prefix}.json"
    cat "${output}/${prefix}.json" >>"${output}/operations.ndjson"
    cat "${output}/${prefix}.stderr.txt" >&2
    return "${status}"
}

preflight_result() {
    local verdict="$1" stage="$2" status="$3" operation="$4" reason="$5" cleanup_status="$6"
    local finished_ns
    finished_ns="$(date +%s%N)"
    jq -n --arg status "${verdict}" --arg stage "${stage}" --arg reason "${reason}" \
        --arg run_id "${run_id}" --arg canary "${preflight_canary}" --arg injector "${preflight_injector}" \
        --argjson code "${status}" --argjson operation "${operation}" --argjson cleanup "${cleanup_status}" \
        --argjson elapsed_ms "$(((finished_ns - preflight_started_ns) / 1000000))" \
        '{run_id:$run_id,status:$status,stage:$stage,exit_code:$code,operation:$operation,
          reason:$reason,elapsed_ms:$elapsed_ms,cleanup_exit_code:$cleanup,
          canary_id:($canary|select(length>0)) // null,injector_id:($injector|select(length>0)) // null}' \
        >"${output}/result.json.tmp"
    mv "${output}/result.json.tmp" "${output}/result.json"
}

preflight_remove_sidecars() {
    local owned=() sidecars=() sidecar owned_id network_mode status=0
    preflight_run cleanup.list-owned 20 docker container ls --all --quiet --no-trunc --filter "label=${run_label}" || return $?
    mapfile -t owned <"${preflight_stdout}"
    preflight_run cleanup.list-sidecars 20 docker container ls --all --quiet --no-trunc --filter label=com.gaiaadm.pumba.skip=true || return $?
    mapfile -t sidecars <"${preflight_stdout}"
    for sidecar in "${sidecars[@]}"; do
        if ! preflight_run cleanup.inspect-sidecar 20 docker inspect --format '{{.HostConfig.NetworkMode}}' "${sidecar}"; then
            # Pumba sidecars belong to concurrent runs too and can disappear after listing.
            # Keep the failed inspection, then distinguish absence from a failed live inspection.
            if preflight_run cleanup.confirm-sidecar-exists 20 docker container ls --all --quiet --no-trunc --filter "id=${sidecar}"; then
                if [[ -s "${preflight_stdout}" ]]; then
                    status=1
                else
                    printf 'sidecar %s disappeared before inspection\n' "${sidecar}" >>"${output}/sidecars.txt"
                fi
            else
                status=1
            fi
            continue
        fi
        network_mode="$(cat "${preflight_stdout}")"
        for owned_id in "${owned[@]}"; do
            if [[ "${network_mode}" == "container:${owned_id}" ]]; then
                printf 'removing Pumba sidecar %s joined to %s\n' "${sidecar}" "${owned_id}" >>"${output}/sidecars.txt"
                preflight_run cleanup.remove-sidecar 30 docker container rm --force "${sidecar}" || status=1
                break
            fi
        done
    done
    return "${status}"
}

preflight_cleanup() {
    local status=$?
    trap - EXIT INT TERM
    set +e
    local stage="${preflight_stage}" operation="${preflight_operation}" reason="${preflight_reason}" cleanup_status=0
    local verdict=failed
    if [[ "${preflight_completed}" == true && "${status}" -eq 0 ]]; then
        verdict=succeeded
    elif [[ -z "${reason}" ]]; then
        reason="${stage} returned ${status}; see operations.ndjson and raw command streams"
    fi
    # cidfiles also identify a launch that created a helper before its Docker client failed.
    if [[ -z "${preflight_canary}" && "${stage}" == canary.launch && -s "${output}/canary-id.txt" ]]; then
        preflight_canary="$(cat "${output}/canary-id.txt")"
    fi
    if [[ -z "${preflight_injector}" && "${stage}" == "${preflight_kind}.launch-injector" \
        && -s "${output}/${preflight_kind}-injector-id.txt" ]]; then
        preflight_injector="$(cat "${output}/${preflight_kind}-injector-id.txt")"
    fi
    preflight_result "${verdict}" "${stage}" "${status}" "${operation}" "${reason}" 0
    if [[ -n "${preflight_injector}" ]]; then
        preflight_run cleanup.inspect-injector 20 docker inspect "${preflight_injector}" || cleanup_status=1
        cp "${preflight_stdout}" "${output}/${preflight_kind}-injector.json"
        preflight_run cleanup.injector-logs 20 docker logs "${preflight_injector}" || cleanup_status=1
        cat "${preflight_stdout}" "${preflight_stdout%.stdout.txt}.stderr.txt" >"${output}/${preflight_kind}-pumba.txt"
        preflight_run cleanup.remove-injector 60 docker container rm --force "${preflight_injector}" || cleanup_status=1
    fi
    if [[ -n "${preflight_canary}" ]]; then
        preflight_remove_sidecars || cleanup_status=1
        preflight_run cleanup.remove-canary 30 docker container rm --force "${preflight_canary}" || cleanup_status=1
    fi
    if [[ "${status}" -eq 0 && "${cleanup_status}" -ne 0 ]]; then
        status=1
        verdict=failed
        stage=cleanup
        reason='preflight cleanup failed; see cleanup operations and retained helper identities'
    fi
    preflight_result "${verdict}" "${stage}" "${status}" "${operation}" "${reason}" "${cleanup_status}"
    if [[ "${status}" -ne 0 ]]; then
        printf 'network fault preflight: %s\n' "${reason}" >&2
    fi
    exit "${status}"
}

preflight_canary_reaches() {
    local gateway="$1"
    preflight_run "${preflight_kind}.gateway-probe" 20 docker exec "${preflight_canary}" ping -c 2 -i 0.2 -W 1 -q "${gateway}"
}

preflight_fail() {
    preflight_stage="$1"
    shift
    preflight_reason="$*"
    exit 1
}

# Proves on this worker that the qualified injectors take effect and heal: each Pumba fault must
# block the canary's traffic to or from its gateway while installed, and leave its namespace in
# the default state once Pumba receives SIGTERM.
preflight() {
    [[ -n "${pumba_image}" && -n "${probe_image}" ]] || { usage; exit 2; }
    local directory="${output}"
    if [[ -e "${directory}/operations.ndjson" || -e "${directory}/canary-id.txt" ]]; then
        printf 'network fault preflight: evidence directory already used: %s\n' "${directory}" >&2
        exit 2
    fi
    mkdir -p "${directory}/operations"
    preflight_started_ns="$(date +%s%N)"
    : >"${directory}/operations.ndjson"
    : >"${directory}/sidecars.txt"
    trap preflight_cleanup EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM

    local canary_name="nervix-chaos-${run_id}-network-preflight"
    preflight_run canary.launch 30 docker run --detach --rm --cidfile "${directory}/canary-id.txt" \
        --label "${run_label}" \
        --label io.nervix.chaos.role=preflight \
        --name "${canary_name}" \
        "${probe_image}" sleep 300
    preflight_canary="$(cat "${preflight_stdout}")"
    local gateway
    preflight_run canary.inspect-gateway 20 docker inspect --format '{{range .NetworkSettings.Networks}}{{.Gateway}}{{end}}' "${preflight_canary}"
    gateway="$(cat "${preflight_stdout}")"
    [[ "${gateway}" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] \
        || preflight_fail canary.gateway 'the canary has no IPv4 gateway on the default bridge network'
    preflight_canary_reaches "${gateway}" || preflight_fail canary.gateway 'the canary cannot reach its gateway before any fault'

    local kind
    for kind in netem iptables; do
        preflight_kind="${kind}"
        local fault_args=()
        if [[ "${kind}" == netem ]]; then
            fault_args=(netem --duration 120s --interface eth0 --tc-image "${nettools_image}" --pull-image=false
                --target "${gateway}" loss --percent 100)
        else
            fault_args=(iptables --duration 120s --interface eth0 --iptables-image "${nettools_image}" --pull-image=false
                --source "${gateway}" loss --probability 1.0)
        fi
        preflight_run "${kind}.launch-injector" 30 docker run --detach --cidfile "${directory}/${kind}-injector-id.txt" \
            --label "${run_label}" \
            --label io.nervix.chaos.role=fault \
            --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
            "${pumba_image}" --log-level info --label "${run_label}" \
            "${fault_args[@]}" "${canary_name}"
        preflight_injector="$(cat "${preflight_stdout}")"
        local installed=false
        local attempt
        for attempt in $(seq 1 30); do
            inspect_rules "${preflight_canary}" "${directory}/${kind}-installed.txt"
            if preflight_rules_are_installed "${kind}" "${directory}/${kind}-installed.txt"; then
                installed=true
                break
            fi
            preflight_run "${kind}.inspect-injector-running" 20 docker inspect --format '{{.State.Running}}' "${preflight_injector}"
            [[ "$(cat "${preflight_stdout}")" == true ]] || break
            sleep 1
        done
        if [[ "${installed}" != true ]]; then
            preflight_fail "${kind}.installation" "Pumba ${kind} installation was incomplete on the canary after ${attempt} observations"
        fi
        if preflight_canary_reaches "${gateway}"; then
            preflight_fail "${kind}.effect" "Pumba ${kind} was installed but the canary still reached its gateway"
        else
            local probe_status=$?
            # BusyBox ping returns 1 for missing replies. A Docker refusal or a command timeout
            # judges no traffic effect and must retain its own failure instead of qualifying it.
            if [[ "${probe_status}" -ne 1 ]]; then
                exit "${probe_status}"
            fi
        fi
        preflight_run "${kind}.stop-injector" 60 docker stop -t 40 "${preflight_injector}"
        local exit_code
        preflight_run "${kind}.inspect-injector-exit" 20 docker inspect --format '{{.State.ExitCode}}' "${preflight_injector}"
        exit_code="$(cat "${preflight_stdout}")"
        preflight_run "${kind}.injector-logs" 20 docker logs "${preflight_injector}"
        cat "${preflight_stdout}" "${preflight_stdout%.stdout.txt}.stderr.txt" >"${directory}/${kind}-pumba.txt"
        [[ "${exit_code}" == 0 ]] || preflight_fail "${kind}.injector-exit" "Pumba ${kind} exited ${exit_code} after SIGTERM"
        preflight_run "${kind}.remove-injector" 30 docker container rm --force "${preflight_injector}"
        preflight_injector=""
        inspect_rules "${preflight_canary}" "${directory}/${kind}-healed.txt"
        rules_are_default "${directory}/${kind}-healed.txt" \
            || preflight_fail "${kind}.healing" "Pumba ${kind} left owned state on the canary after SIGTERM"
        preflight_canary_reaches "${gateway}" \
            || preflight_fail "${kind}.healing" "the canary did not reach its gateway after the ${kind} fault healed"
        printf '%s fault installed, blocked gateway traffic, and healed\n' "${kind}" >>"${directory}/summary.txt"
    done
    preflight_completed=true
}

case "${command_name}" in
    inspect)
        [[ -n "${container_id}" ]] || { usage; exit 2; }
        inspect_rules "${container_id}" "${output}"
        ;;
    heal)
        heal
        ;;
    preflight)
        preflight
        ;;
    *)
        usage
        exit 2
        ;;
esac
