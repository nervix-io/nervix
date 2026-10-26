#!/usr/bin/env bash
set -euo pipefail

# Owns the action plan of a mixed-instability run: generating a finite plan from a seed, and
# validating any plan, generated or supplied, before a run alters a container. A plan is a sequence
# of steps. Each step resolves its logical node references from public observations when it runs:
# `target` is the node holding the step's role, `peer` is the leader when the target is not the
# leader and otherwise the lowest-numbered follower, `other` is the remaining node, and `cluster` is
# every node. Each action injects one fault no earlier than its start offset within the step, and
# heals it no earlier than its hold after the fault was verified.

usage() {
    cat >&2 <<'EOF'
usage:
  mixed-plan.sh generate --seed N --duration SECONDS --policy POLICY --coverage LIST --output FILE
  mixed-plan.sh validate --plan FILE [--report FILE]
  mixed-plan.sh default-coverage --policy POLICY

generate writes the finite plan the seed selects: a step for each required coverage item that no
earlier step covers, then further steps while the planned timeline fits the duration. The same
seed, duration, policy and coverage always select the same plan. It exits 2 when the required
coverage does not fit the duration.

validate exits 0 for a valid plan, and 1 with one reason per line, also written as JSON to
--report, when the plan breaks its contract: conflicting interface rules, a combination its quorum
policy refuses, a parameter outside its bound, or a required coverage item that no action plans.

POLICY is preserve-quorum or temporary-quorum-loss. LIST is a comma-separated list of FAMILY:ROLE
items, where FAMILY is kill, stop, pause, partition or degrade and ROLE is leader, follower,
ingestor-owner, relay-owner, emitter-owner or cluster, together with the item quorum-loss.
EOF
}

# Park and Miller's minimal standard generator in 64-bit shell arithmetic, so a seed selects the
# same plan on every shell and host.
prng_state=1

prng_seed() {
    prng_state=$(($1 % 2147483646 + 1))
}

# Sets prng_value to a uniformly chosen integer in 0..BOUND-1.
prng_below() {
    local bound="$1"
    prng_state=$((prng_state * 48271 % 2147483647))
    prng_value=$((prng_state % bound))
}

# Sets prng_value to an integer in LOW..HIGH.
prng_between() {
    local low="$1"
    local high="$2"
    prng_below "$((high - low + 1))"
    prng_value=$((low + prng_value))
}

node_roles=(leader follower ingestor-owner relay-owner emitter-owner)
degradation_profiles=(delay jitter random-loss burst-loss rate-limit combined)

default_coverage() {
    local policy="$1"
    local items=()
    local family role
    for family in kill stop pause partition degrade; do
        for role in leader follower; do
            items+=("${family}:${role}")
        done
    done
    if [[ "${policy}" == temporary-quorum-loss ]]; then
        items+=(quorum-loss)
    fi
    printf '%s\n' "${items[@]}" | LC_ALL=C sort | paste -sd, -
}

# The planned seconds a step of each kind needs beyond its longest hold: selecting and confirming
# roles, injecting and verifying the faults, the checks while they hold, healing, and recovery with
# its evidence. Each is about one and a half times the overhead measured for its kind on a shared
# qualification worker, and a run records its actual timings beside these estimates. Validation
# holds the estimates of every plan, generated or supplied, to them, because a run sizes its fixture
# from the plan's end.
declare -A step_overheads=(
    [kill]=80 [stop]=60 [double-outage]=60 [pause]=50 [isolate]=80 [one-way]=80 [cluster-restart]=80
    [degrade]=90 [isolated-restart]=90 [quorum-loss]=90 [outage-under-degrade]=120
)

# Prints one action as a compact JSON object. Any further jq arguments add named fields.
action_json() {
    local id="$1"
    local family="$2"
    local node="$3"
    local start="$4"
    local hold="$5"
    shift 5
    jq -nc \
        --arg id "${id}" --arg family "${family}" --arg node "${node}" \
        --argjson start "${start}" --argjson hold "${hold}" \
        '{id: $id, family: $family, node: $node, start_seconds: $start, hold_seconds: $hold}
         + ($ARGS.named | del(.id, .family, .node, .start, .hold))' "$@"
}

# Builds the step of KIND for ROLE from the next random parameters into step_json, the coverage
# items it plans into step_items, and its planned seconds, quiet gap included, into step_span.
# NODE_FAMILY selects the node fault of an outage under a degraded link, or is empty to draw one.
build_step() {
    local kind="$1"
    local role="$2"
    local node_family="${3:-}"
    local actions=()
    step_items=()
    prng_below 2
    local pick="${prng_value}"
    case "${kind}" in
        kill)
            prng_between 8 30
            actions+=("$(action_json a kill target 0 "${prng_value}")")
            step_items=("kill:${role}")
            ;;
        stop)
            prng_between 8 20
            actions+=("$(action_json a stop target 0 "${prng_value}")")
            step_items=("stop:${role}")
            ;;
        pause)
            # A short pause stays below the election timeout; a long one outlasts failover.
            prng_below 2
            if ((prng_value == 0)); then
                prng_between 1 2
            else
                prng_between 20 40
            fi
            actions+=("$(action_json a pause target 0 "${prng_value}")")
            step_items=("pause:${role}")
            ;;
        isolate)
            prng_between 30 60
            actions+=("$(action_json a partition target 0 "${prng_value}" --arg partition isolate)")
            step_items=("partition:${role}")
            ;;
        one-way)
            prng_between 30 45
            actions+=("$(action_json a partition target 0 "${prng_value}" \
                --arg partition one-way --arg to peer)")
            step_items=("partition:${role}")
            ;;
        degrade)
            prng_below "${#degradation_profiles[@]}"
            local profile="${degradation_profiles[${prng_value}]}"
            prng_between 20 40
            actions+=("$(action_json a degrade target 0 "${prng_value}" \
                --arg to peer --arg profile "${profile}")")
            step_items=("degrade:${role}")
            ;;
        outage-under-degrade)
            # The link between the two nodes the outage leaves stays degraded around the whole outage.
            if [[ -z "${node_family}" ]]; then
                local node_families=(kill stop pause)
                prng_below 3
                node_family="${node_families[${prng_value}]}"
            fi
            prng_below "${#degradation_profiles[@]}"
            local profile="${degradation_profiles[${prng_value}]}"
            prng_between 5 10
            local lead="${prng_value}"
            case "${node_family}" in
                kill) prng_between 10 25 ;;
                stop) prng_between 8 15 ;;
                pause) prng_between 20 35 ;;
            esac
            local node_hold="${prng_value}"
            prng_between 5 10
            local trail="${prng_value}"
            actions+=("$(action_json a degrade peer 0 "$((lead + node_hold + trail))" \
                --arg to other --arg profile "${profile}")")
            actions+=("$(action_json b "${node_family}" target "${lead}" "${node_hold}")")
            step_items=("${node_family}:${role}")
            ;;
        isolated-restart)
            # The target restarts inside its own isolation, whose rules live on its peers.
            prng_between 45 60
            local isolation="${prng_value}"
            prng_between 8 12
            local delay="${prng_value}"
            prng_between 8 15
            actions+=("$(action_json a partition target 0 "${isolation}" --arg partition isolate)")
            actions+=("$(action_json b kill target "${delay}" "${prng_value}")")
            step_items=("partition:${role}" "kill:${role}")
            ;;
        quorum-loss)
            prng_between 20 40
            actions+=("$(action_json a partition cluster 0 "${prng_value}" --arg partition quorum-loss)")
            step_items=(partition:cluster quorum-loss)
            ;;
        double-outage)
            # A second voter goes out inside the first one's outage, so no quorum remains meanwhile.
            local outage_families=(kill pause)
            prng_below 2
            local first_family="${outage_families[${prng_value}]}"
            prng_below 2
            local second_family="${outage_families[${prng_value}]}"
            prng_between 25 40
            local first_hold="${prng_value}"
            prng_between 4 8
            local delay="${prng_value}"
            prng_between 8 15
            actions+=("$(action_json a "${first_family}" target 0 "${first_hold}")")
            actions+=("$(action_json b "${second_family}" peer "${delay}" "${prng_value}")")
            step_items=("${first_family}:${role}" quorum-loss)
            ;;
        cluster-restart)
            prng_between 8 20
            actions+=("$(action_json a kill cluster 0 "${prng_value}")")
            step_items=(kill:cluster quorum-loss)
            ;;
    esac
    local longest
    longest="$(printf '%s\n' "${actions[@]}" | jq -s '[.[] | .start_seconds + .hold_seconds] | max')"
    local estimate=$((longest + ${step_overheads[${kind}]}))
    prng_between 10 30
    local gap="${prng_value}"
    step_span=$((gap + estimate))
    step_json="$(printf '%s\n' "${actions[@]}" | jq -sc \
        --arg kind "${kind}" --arg role "${role}" --argjson pick "${pick}" \
        --argjson estimate "${estimate}" --argjson gap "${gap}" \
        '{kind: $kind, role: $role, pick: $pick, gap: $gap, estimated_seconds: $estimate, actions: .}')"
}

# Chooses the step kind and role that plan coverage ITEM into item_kind and item_role, with the node
# fault of an outage under a degraded link in item_node_family.
kind_for_item() {
    local item="$1"
    item_node_family=""
    if [[ "${item}" == quorum-loss ]]; then
        local kinds=(quorum-loss double-outage cluster-restart)
        prng_below 3
        item_kind="${kinds[${prng_value}]}"
        item_role=cluster
        if [[ "${item_kind}" == double-outage ]]; then
            prng_below "${#node_roles[@]}"
            item_role="${node_roles[${prng_value}]}"
        fi
        return 0
    fi
    local family="${item%%:*}"
    item_role="${item#*:}"
    if [[ "${item_role}" == cluster ]]; then
        case "${family}" in
            kill) item_kind=cluster-restart ;;
            partition) item_kind=quorum-loss ;;
        esac
        return 0
    fi
    local kinds=()
    case "${family}" in
        kill) kinds=(kill kill outage-under-degrade isolated-restart) ;;
        stop | pause) kinds=("${family}" "${family}" outage-under-degrade) ;;
        partition) kinds=(isolate isolate one-way isolated-restart) ;;
        degrade) kinds=(degrade) ;;
    esac
    prng_below "${#kinds[@]}"
    item_kind="${kinds[${prng_value}]}"
    if [[ "${item_kind}" == outage-under-degrade ]]; then
        item_node_family="${family}"
    fi
}

# Requires every coverage item to be one a plan can hold under POLICY, and prints the sorted list.
normalize_coverage() {
    local list="$1"
    local policy="$2"
    local items=()
    IFS=, read -r -a items <<<"${list}"
    ((${#items[@]} > 0)) || { printf '%s\n' 'the coverage list is empty' >&2; return 2; }
    local item
    for item in "${items[@]}"; do
        if [[ ! "${item}" =~ ^((kill|stop|pause|partition|degrade):(leader|follower|ingestor-owner|relay-owner|emitter-owner|cluster)|quorum-loss)$ ]]; then
            printf 'unknown coverage item: %s\n' "${item}" >&2
            return 2
        fi
        if [[ "${item}" == *:cluster && "${item}" != kill:cluster && "${item}" != partition:cluster ]]; then
            printf 'coverage item %s has no fault that targets the whole cluster\n' "${item}" >&2
            return 2
        fi
        if [[ ( "${item}" == quorum-loss || "${item}" == *:cluster ) && "${policy}" != temporary-quorum-loss ]]; then
            printf 'coverage item %s loses quorum and needs the temporary-quorum-loss policy\n' "${item}" >&2
            return 2
        fi
    done
    printf '%s\n' "${items[@]}" | LC_ALL=C sort -u | paste -sd, -
}

generate() {
    local seed="" duration="" policy="" coverage="" output=""
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --seed | --duration | --policy | --coverage | --output)
                [[ "$#" -ge 2 ]] || { usage; exit 2; }
                case "$1" in
                    --seed) seed="$2" ;;
                    --duration) duration="$2" ;;
                    --policy) policy="$2" ;;
                    --coverage) coverage="$2" ;;
                    --output) output="$2" ;;
                esac
                shift 2
                ;;
            *)
                printf 'unknown generate argument: %s\n' "$1" >&2
                usage
                exit 2
                ;;
        esac
    done
    if [[ ! "${seed}" =~ ^[0-9]{1,10}$ ]] || ((10#${seed} > 2147483646)); then
        printf '%s\n' 'the seed must be an integer from 0 through 2147483646' >&2
        exit 2
    fi
    seed=$((10#${seed}))
    [[ "${duration}" =~ ^[0-9]{1,6}$ ]] || { printf '%s\n' 'the duration must be whole seconds' >&2; exit 2; }
    duration=$((10#${duration}))
    [[ "${policy}" == preserve-quorum || "${policy}" == temporary-quorum-loss ]] \
        || { printf '%s\n' 'the policy must be preserve-quorum or temporary-quorum-loss' >&2; exit 2; }
    [[ -n "${output}" ]] || { usage; exit 2; }
    coverage="$(normalize_coverage "${coverage}" "${policy}")" || exit 2

    prng_seed "${seed}"
    local required=()
    IFS=, read -r -a required <<<"${coverage}"
    # The seed shuffles the order in which required items are planned.
    local index
    for ((index = ${#required[@]} - 1; index > 0; index--)); do
        prng_below "$((index + 1))"
        local swap="${required[${index}]}"
        required[index]="${required[${prng_value}]}"
        required[prng_value]="${swap}"
    done

    local steps=()
    local -A covered=()
    local planned=0
    local item
    for item in "${required[@]}"; do
        [[ -z "${covered[${item}]:-}" ]] || continue
        kind_for_item "${item}"
        build_step "${item_kind}" "${item_role}" "${item_node_family}"
        steps+=("${step_json}")
        planned=$((planned + step_span))
        local covered_item
        for covered_item in "${step_items[@]}"; do
            covered["${covered_item}"]=1
        done
    done
    if ((planned > duration)); then
        printf 'the required coverage needs a %s-second plan, longer than the %s-second duration; raise --duration or narrow --coverage\n' \
            "${planned}" "${duration}" >&2
        exit 2
    fi

    # Further steps follow while the planned timeline still fits the duration, until eight drawn
    # steps in a row no longer fit.
    local kinds=(kill stop pause isolate one-way degrade outage-under-degrade isolated-restart)
    if [[ "${policy}" == temporary-quorum-loss ]]; then
        kinds+=(quorum-loss double-outage cluster-restart)
    fi
    local misses=0
    while ((misses < 8 && ${#steps[@]} < 200)); do
        prng_below "${#kinds[@]}"
        local kind="${kinds[${prng_value}]}"
        local role=cluster
        if [[ "${kind}" != quorum-loss && "${kind}" != cluster-restart ]]; then
            prng_below "${#node_roles[@]}"
            role="${node_roles[${prng_value}]}"
        fi
        build_step "${kind}" "${role}"
        if ((planned + step_span > duration)); then
            misses=$((misses + 1))
            continue
        fi
        misses=0
        steps+=("${step_json}")
        planned=$((planned + step_span))
    done

    # The seed also shuffles the whole sequence, so required steps do not cluster at the start.
    for ((index = ${#steps[@]} - 1; index > 0; index--)); do
        prng_below "$((index + 1))"
        local swap="${steps[${index}]}"
        steps[index]="${steps[${prng_value}]}"
        steps[prng_value]="${swap}"
    done
    printf '%s\n' "${steps[@]}" | jq -s \
        --argjson seed "${seed}" --arg policy "${policy}" --argjson duration "${duration}" \
        --arg coverage "${coverage}" '
        # Each step starts after its quiet gap, once the step before it is planned to end.
        reduce to_entries[] as $entry ({at: 0, steps: []};
            ($entry.key + 1) as $index
            | (.at + $entry.value.gap) as $start
            | .steps += [$entry.value | del(.gap)
                         | {index: $index, kind, role, pick, at_seconds: $start, estimated_seconds,
                            actions: (.actions | map(.id = "\($index)\(.id)"))}]
            | .at = $start + $entry.value.estimated_seconds)
        | {seed: $seed, policy: $policy, nodes: 3, duration_seconds: $duration,
           coverage: ($coverage | split(",")), steps: .steps}' >"${output}"
}

validate() {
    local plan="" report=""
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --plan | --report)
                [[ "$#" -ge 2 ]] || { usage; exit 2; }
                case "$1" in
                    --plan) plan="$2" ;;
                    --report) report="$2" ;;
                esac
                shift 2
                ;;
            *)
                printf 'unknown validate argument: %s\n' "$1" >&2
                usage
                exit 2
                ;;
        esac
    done
    [[ -n "${plan}" ]] || { usage; exit 2; }
    local reasons
    if [[ ! -s "${plan}" ]]; then
        reasons='["the plan file is missing or empty"]'
    elif ! jq -e 'type == "object"' "${plan}" >/dev/null 2>&1; then
        reasons='["the plan is not one JSON object"]'
    else
        local overheads kind
        overheads="$(for kind in "${!step_overheads[@]}"; do
            printf '%s %s\n' "${kind}" "${step_overheads[${kind}]}"
        done | jq -Rnc '[inputs | split(" ") | {(.[0]): (.[1] | tonumber)}] | add')"
        reasons="$(jq -c --argjson overheads "${overheads}" -f "${script_dir}/mixed-plan.jq" "${plan}" 2>/dev/null)" \
            || reasons='["the plan could not be read against its contract"]'
    fi
    if [[ -n "${report}" ]]; then
        jq -n --argjson reasons "${reasons}" \
            '{verdict: (if ($reasons | length) == 0 then "valid" else "invalid" end), reasons: $reasons}' \
            >"${report}"
    fi
    if [[ "$(jq 'length' <<<"${reasons}")" -ne 0 ]]; then
        jq -r '.[]' <<<"${reasons}" >&2
        exit 1
    fi
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
command_name="${1:-}"
[[ -n "${command_name}" ]] || { usage; exit 2; }
shift
case "${command_name}" in
    generate)
        generate "$@"
        ;;
    validate)
        validate "$@"
        ;;
    default-coverage)
        [[ "$#" -eq 2 && "$1" == --policy ]] || { usage; exit 2; }
        [[ "$2" == preserve-quorum || "$2" == temporary-quorum-loss ]] || { usage; exit 2; }
        default_coverage "$2"
        ;;
    -h | --help)
        usage
        ;;
    *)
        printf 'unknown mixed-plan command: %s\n' "${command_name}" >&2
        usage
        exit 2
        ;;
esac
