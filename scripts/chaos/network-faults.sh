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
  network-faults.sh preflight --run-id RUN_ID --pumba IMAGE --nettools IMAGE --output DIR
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
output=""
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --run-id | --container | --nettools | --pumba | --output)
            [[ "$#" -ge 2 ]] || { usage; exit 2; }
            case "$1" in
                --run-id) run_id="$2" ;;
                --container) container_id="$2" ;;
                --nettools) nettools_image="$2" ;;
                --pumba) pumba_image="$2" ;;
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
    bounded 30 docker run --rm \
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
        ' >"${destination}"
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

# Deletes Pumba's root qdisc and Pumba's INPUT DROP rules, and nothing else.
remove_owned_rules() {
    local target="$1"
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

preflight_cleanup() {
    local status=$?
    trap - EXIT
    set +e
    if [[ -n "${preflight_injector}" ]]; then
        bounded 60 docker container rm --force "${preflight_injector}" >/dev/null 2>&1
    fi
    if [[ -n "${preflight_canary}" ]]; then
        remove_sidecars "${output}/sidecars.txt"
        bounded 30 docker container rm --force "${preflight_canary}" >/dev/null 2>&1
    fi
    exit "${status}"
}

preflight_canary_reaches() {
    local gateway="$1"
    bounded 20 docker exec "${preflight_canary}" ping -c 2 -i 0.2 -W 1 -q "${gateway}" >/dev/null 2>&1
}

preflight_fail() {
    printf 'network fault preflight: %s\n' "$*" >&2
    exit 1
}

# Proves on this worker that the qualified injectors take effect and heal: each Pumba fault must
# block the canary's traffic to or from its gateway while installed, and leave its namespace in
# the default state once Pumba receives SIGTERM.
preflight() {
    [[ -n "${pumba_image}" ]] || { usage; exit 2; }
    local directory="${output}"
    mkdir -p "${directory}"
    : >"${directory}/sidecars.txt"
    trap preflight_cleanup EXIT

    local canary_name="nervix-chaos-${run_id}-network-preflight"
    preflight_canary="$(bounded 30 docker run --detach --rm \
        --label "${run_label}" \
        --label io.nervix.chaos.role=preflight \
        --name "${canary_name}" \
        alpine:3.22 sleep 300)"
    local gateway
    gateway="$(bounded 20 docker inspect --format '{{range .NetworkSettings.Networks}}{{.Gateway}}{{end}}' "${preflight_canary}")"
    [[ "${gateway}" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] \
        || preflight_fail 'the canary has no IPv4 gateway on the default bridge network'
    preflight_canary_reaches "${gateway}" || preflight_fail 'the canary cannot reach its gateway before any fault'

    local kind
    for kind in netem iptables; do
        local fault_args=()
        if [[ "${kind}" == netem ]]; then
            fault_args=(netem --duration 120s --interface eth0 --tc-image "${nettools_image}" --pull-image=false
                --target "${gateway}" loss --percent 100)
        else
            fault_args=(iptables --duration 120s --interface eth0 --iptables-image "${nettools_image}" --pull-image=false
                --source "${gateway}" loss --probability 1.0)
        fi
        preflight_injector="$(bounded 30 docker run --detach \
            --label "${run_label}" \
            --label io.nervix.chaos.role=fault \
            --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
            "${pumba_image}" --log-level info --label "${run_label}" \
            "${fault_args[@]}" "${canary_name}")"
        local installed=false
        local attempt
        for attempt in $(seq 1 30); do
            inspect_rules "${preflight_canary}" "${directory}/${kind}-installed.txt"
            if ! rules_are_default "${directory}/${kind}-installed.txt"; then
                installed=true
                break
            fi
            [[ "$(bounded 20 docker inspect --format '{{.State.Running}}' "${preflight_injector}")" == true ]] || break
            sleep 1
        done
        if [[ "${installed}" != true ]]; then
            bounded 20 docker logs "${preflight_injector}" >"${directory}/${kind}-pumba.txt" 2>&1
            preflight_fail "Pumba ${kind} installed nothing on the canary after ${attempt} observations"
        fi
        if preflight_canary_reaches "${gateway}"; then
            preflight_fail "Pumba ${kind} was installed but the canary still reached its gateway"
        fi
        bounded 60 docker stop -t 40 "${preflight_injector}" >/dev/null
        local exit_code
        exit_code="$(bounded 20 docker inspect --format '{{.State.ExitCode}}' "${preflight_injector}")"
        bounded 20 docker logs "${preflight_injector}" >"${directory}/${kind}-pumba.txt" 2>&1
        bounded 30 docker container rm --force "${preflight_injector}" >/dev/null
        preflight_injector=""
        [[ "${exit_code}" == 0 ]] || preflight_fail "Pumba ${kind} exited ${exit_code} after SIGTERM"
        inspect_rules "${preflight_canary}" "${directory}/${kind}-healed.txt"
        rules_are_default "${directory}/${kind}-healed.txt" \
            || preflight_fail "Pumba ${kind} left owned state on the canary after SIGTERM"
        preflight_canary_reaches "${gateway}" \
            || preflight_fail "the canary did not reach its gateway after the ${kind} fault healed"
        printf '%s fault installed, blocked gateway traffic, and healed\n' "${kind}" >>"${directory}/summary.txt"
    done
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
