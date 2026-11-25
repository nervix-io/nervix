#!/usr/bin/env bash
# Sourced by run-baseline.sh after the external cluster and graph are ready. Owns the node-settings
# scenarios, which qualify the rolling-change contract of Node Settings with the packaged image while
# continuous Kafka traffic crosses the nodes:
#
#   capability-roll       restarts every node in turn, leader first, with a lower interconnect relay
#                         receive bound, so for a while the nodes claim different values of a bound
#                         their peers honour, and judges each step by every node's own description
#                         of the committed claims and of the bounds it adopted
#   parameter-return      commits a cluster parameter change while one follower is down, refuses the
#                         follower's start that names the parameter as its own setting, and requires
#                         the returning follower to run the record it takes from the live cluster,
#                         never the one its stale log holds
#   parameter-live        commits two cluster parameter changes under load and requires every node to
#                         install each without a restart, a move of work or leadership, a negative
#                         acknowledgement, an error event or a lost or repeated record
#   prerequisite-refusal  restarts one follower with a Raft heartbeat outside the factor-of-two bound
#                         and requires only that node to be refused at join, naming the prerequisite
#                         and both values, while the others keep serving
#
# Every node starts through node-launch.sh (compose.node-settings.yaml), which applies the settings
# this controller wrote for the node's next process to node-launch/HOST/settings.env. A node changes
# its settings only through a graceful stop and an explicit start of the same container, so every
# restart keeps its image, volume and address, and every restart and Docker event check applies.

# shellcheck source=recovery-scenario.sh
source "${script_dir}/recovery-scenario.sh"
# shellcheck source=node-faults.sh
source "${script_dir}/node-faults.sh"
# shellcheck source=events-observer.sh
source "${script_dir}/events-observer.sh"

node_settings_verifier="${script_dir}/verify-node-settings-evidence.sh"

# The relay receive bound every node claims once the roll has reached it. One baseline row encodes
# to far less, so a batch above it slices into parts that fit and no row is refused, while the
# deployment's default, the executor's encoded relay limit of 32 MiB, is far above it.
capability_roll_relay_bytes=8192
# The deployment's Raft heartbeat, and the one the refused process starts with: four times longer,
# outside the factor of two within which two nodes' timings may differ.
prerequisite_refusal_deployed_heartbeat=250ms
prerequisite_refusal_heartbeat=1s
prerequisite_refusal_mismatch="raft-heartbeat-interval is ${prerequisite_refusal_heartbeat} on this node and ${prerequisite_refusal_deployed_heartbeat} on the peer, not within a factor of 2"
# The change committed while a follower is down, the values the record holds before it, and the
# interconnect liveness the new unavailability timeout derives: a half, a quarter and a tenth of it.
parameter_return_statement='ALTER CLUSTER SET NODE_UNAVAILABILITY_TIMEOUT = 15s, STATE_SNAPSHOT_INTERVAL = 20s;'
parameter_return_assignments=(node-unavailability-timeout=15s state-snapshot-interval=20s)
parameter_return_previous=(node-unavailability-timeout=10s state-snapshot-interval=30s)
parameter_return_liveness='ping-interval=7s 500ms ping-timeout=3s 750ms probe-timeout=1s 500ms'
# The returning follower's first start names the previous unavailability timeout as its own setting,
# the environment form of the parameter's first-bootstrap seed option.
parameter_return_seed='NERVIX_NODE_UNAVAILABILITY_TIMEOUT=10s'
parameter_return_seed_parameter=node-unavailability-timeout
# The two changes applied under load: the first raises the unavailability timeout every connection's
# liveness derives from and extends the admission bound of every remote delivery, and the second
# restores the deployment's values. Each liveness is what its timeout derives.
parameter_live_statements=(
    'ALTER CLUSTER SET NODE_UNAVAILABILITY_TIMEOUT = 20s, REMOTE_ACK_ALIVE_INTERVAL = 200ms, REMOTE_RELAY_TOTAL_TIMEOUT = 10m;'
    'ALTER CLUSTER SET NODE_UNAVAILABILITY_TIMEOUT = 10s, REMOTE_ACK_ALIVE_INTERVAL = 100ms, REMOTE_RELAY_TOTAL_TIMEOUT = 5m;'
)
parameter_live_assignments=(
    'node-unavailability-timeout=20s remote-ack-alive-interval=200ms remote-relay-total-timeout=10m'
    'node-unavailability-timeout=10s remote-ack-alive-interval=100ms remote-relay-total-timeout=5m'
)
parameter_live_liveness=(
    'ping-interval=10s ping-timeout=5s probe-timeout=2s'
    'ping-interval=5s ping-timeout=2s 500ms probe-timeout=1s'
)
# External budgets, in seconds.
node_settings_settle_bound=150
node_settings_survivor_bound=90
node_settings_delivery_bound=90
node_settings_description_bound=60
node_settings_unavailable_bound=120
node_settings_exit_bound=120
node_settings_installation_bound=60
node_settings_announcement_bound=30
node_settings_sampler_seconds=240

node_settings_fail() {
    recovery_fail "$@"
}

# Writes the settings HOST's next process starts with, each ASSIGNMENT a NERVIX_NAME=VALUE pair, or
# none for the deployment's own, and records the change in node-launch/changes.ndjson. HOST is
# stopped whenever this runs, so no process reads the file while it is replaced.
node_launch_settings() {
    local host="$1"
    shift
    local assignment
    for assignment in "$@"; do
        [[ "${assignment}" =~ ^NERVIX_[A-Z0-9_]+=[^[:space:]]+$ ]] \
            || node_settings_fail controller "not a node setting assignment: ${assignment}"
    done
    local directory="${node_launch_dir}/${host}"
    [[ -d "${directory}" ]] || node_settings_fail controller "${host} has no launch directory"
    local staged
    staged="$(mktemp "${directory}/.settings.XXXXXX")"
    if (($# > 0)); then
        printf '%s\n' "$@" >"${staged}"
    fi
    chmod 0644 "${staged}"
    mv "${staged}" "${directory}/settings.env"
    jq -nc --arg host "${host}" --arg at "$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)" \
        --argjson settings "$(printf '%s\n' "$@" | jq -R 'select(length > 0)' | jq -sc .)" \
        '{host: $host, at: $at, settings: $settings}' >>"${node_launch_dir}/changes.ndjson"
}

# Reads DESCRIBE CLUSTER through HOST's own public route into PREFIX.txt and its projection into
# PREFIX.json.
node_settings_describe() {
    local host="$1"
    local prefix="$2"
    local status=0
    cli_host="${host}"
    compose_call_timeout=30 cli_command 'DESCRIBE CLUSTER;' >"${prefix}.txt" 2>&1 || status=$?
    cli_host="${node_hosts[0]}"
    ((status == 0)) || return 1
    "${node_settings_verifier}" describe --transcript "${prefix}.txt" --node "node-${host##*-}" \
        --output "${prefix}.json"
}

# Reads the description of every HOST, a space-separated list, into OUTPUT_DIR/describe-HOST.
node_settings_describe_hosts() {
    local output_dir="$1"
    local hosts="$2"
    mkdir -p "${output_dir}"
    local host
    for host in ${hosts}; do
        node_settings_describe "${host}" "${output_dir}/describe-${host}" || return 1
    done
}

# True when every node's own description shows the claims each --expect NODE=BYTES names, every
# peer's live claim adopted and the cluster-effective bound their minimum, and each --restarted
# NODE=INCARNATION running another incarnation.
node_settings_claims_converged() {
    local output_dir="$1"
    shift
    node_settings_describe_hosts "${output_dir}" "${node_hosts[*]}" || return 1
    "${node_settings_verifier}" claims "$@" --output "${output_dir}/claims.json" \
        "${output_dir}"/describe-nervix-*.json >"${output_dir}/claims.txt" 2>&1
}

# True when the description of every HOST, a space-separated list, shows one committed parameter
# revision after the one BEFORE holds, with each PARAMETER=VALUE applied and nothing else changed,
# run by every live node with the interconnect liveness LIVENESS.
node_settings_installed() {
    local output_dir="$1"
    local before="$2"
    local liveness="$3"
    local hosts="$4"
    shift 4
    node_settings_describe_hosts "${output_dir}" "${hosts}" || return 1
    local expectations=()
    local assignment
    for assignment in "$@"; do
        expectations+=(--expect "${assignment}")
    done
    local descriptions=()
    local host
    for host in ${hosts}; do
        descriptions+=("${output_dir}/describe-${host}.json")
    done
    "${node_settings_verifier}" installed --before "${before}" ${expectations[@]+"${expectations[@]}"} \
        --liveness "${liveness}" --output "${output_dir}/installed.json" "${descriptions[@]}" \
        >"${output_dir}/installed.txt" 2>&1
}

# Commits STATEMENT through HOST's own session route, keeping its answer as PREFIX.txt and its
# outcome, exit status and timing as PREFIX.json, and prints the outcome: acknowledged when the
# command answered that the parameters changed, refused when it answered an error that leaves no
# effect uncertain, and uncertain otherwise.
node_settings_parameter_change() {
    local host="$1"
    local statement="$2"
    local prefix="$3"
    local started_at started_ms
    started_at="$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)"
    started_ms="$(epoch_ms)"
    local status=0
    cli_host="${host}"
    compose_call_timeout=150 cli_command "${statement}" >"${prefix}.txt" 2>&1 || status=$?
    cli_host="${node_hosts[0]}"
    local elapsed_ms=$(($(epoch_ms) - started_ms))
    local outcome=uncertain
    if ((status == 0)) && grep -Fq 'cluster parameters changed' "${prefix}.txt"; then
        outcome=acknowledged
    elif grep -Eq '^error: ' "${prefix}.txt" && ! grep -Eqi 'uncertain|not known yet|unknown outcome' "${prefix}.txt"; then
        outcome=refused
    fi
    jq -n --arg host "${host}" --arg statement "${statement}" --arg outcome "${outcome}" \
        --arg started_at "${started_at}" --argjson exit_code "${status}" --argjson elapsed_ms "${elapsed_ms}" \
        '{entry_host: $host, statement: $statement, outcome: $outcome, exit_code: $exit_code,
          started_at: $started_at, elapsed_ms: $elapsed_ms}' >"${prefix}.json"
    printf '%s\n' "${outcome}"
}

# The prerequisite refusals a node's interconnect counted in METRICS, summed over its pool classes.
# Every class of a started transport exposes the counter, so a scrape without it cannot be judged.
node_settings_prerequisite_failures() {
    awk 'index($1, "nervix_interconnect_connection_failures_total{") == 1 && index($1, "reason=\"prerequisite\"") {
             total += $NF
             found = 1
         }
         END {
             if (!found) exit 1
             printf "%d\n", total
         }' "$1"
}

# Keeps HOST's container log from SINCE in OUTPUT, bounded like a restart log.
node_settings_log() {
    local host="$1"
    local since="$2"
    local output="$3"
    local container_id
    container_id="$(owned_service_container "${host}")" || return 1
    run_bounded 20 docker logs --since "${since}" "${container_id}" >"${output}" 2>&1 || return 1
    trim_file "${output}" 2097152
}

node_settings_container_stopped() {
    [[ "$(run_bounded 20 docker inspect --format '{{.State.Running}}' "$1")" == false ]]
}

# Starts HOST's stopped container with the settings written for it and waits until the process it
# starts ends by itself, keeping CASE_DIR/LABEL.log, its exit status in LABEL.exit and inspections
# before and after. A process still running at the bound was not refused, which ends the run.
node_settings_start_until_exit() {
    local case_dir="$1"
    local host="$2"
    local label="$3"
    failure_category=injection
    local container_id
    container_id="$(owned_service_container "${host}")" || return 1
    inspect_target "${container_id}" "${case_dir}/${label}-before.json"
    jq -e '.[0].State.Running == false' "${case_dir}/${label}-before.json" >/dev/null \
        || node_settings_fail injection "${host} was not stopped before its ${label}"
    local since
    since="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    run_bounded 30 docker start "${container_id}" >"${case_dir}/${label}-docker-start.txt"
    recovery_node_started "${container_id}"
    failure_category=product
    wait_for "${host} ended its ${label} by itself" "${node_settings_exit_bound}" \
        node_settings_container_stopped "${container_id}" \
        || node_settings_fail product "${host} kept running ${node_settings_exit_bound}s after its ${label} instead of being refused"
    recovery_node_stopped "${container_id}"
    inspect_target "${container_id}" "${case_dir}/${label}-ended.json"
    jq -e --slurpfile before "${case_dir}/${label}-before.json" --arg image "${image_id}" '
        .[0].Id == $before[0][0].Id and .[0].Image == $image
        and .[0].State.StartedAt != $before[0][0].State.StartedAt
        and (.[0].State.OOMKilled | not)' "${case_dir}/${label}-ended.json" >/dev/null \
        || node_settings_fail injection "${host} did not start a new process of its own container for its ${label}"
    run_bounded 20 docker logs --since "${since}" "${container_id}" >"${case_dir}/${label}.log" 2>&1
    trim_file "${case_dir}/${label}.log" 2097152
    jq -r '.[0].State.ExitCode' "${case_dir}/${label}-ended.json" >"${case_dir}/${label}.exit"
}

# True when every node except TARGET_HOST no longer counts it available in its own status: its
# interconnect entry reads unavailable or has left the list, and gossip no longer reports it live,
# so no description of these nodes still names its last running record.
node_settings_target_unavailable() {
    local target_host="$1"
    local output_dir="$2"
    local target="node-${target_host##*-}"
    mkdir -p "${output_dir}"
    local host
    for host in "${node_hosts[@]}"; do
        [[ "${host}" == "${target_host}" ]] && continue
        partition_node_status "${host}" "${output_dir}/status-${host}.txt" || return 1
        local entry
        entry="$(awk -v target="${target}" '/^\[interconnect\]$/ { section = 1; next }
                 /^\[/ { section = 0 }
                 section && index($0, "- " target ": ") == 1 { print }' "${output_dir}/status-${host}.txt")"
        [[ -z "${entry}" || "${entry}" == *status=unavailable* ]] || return 1
        if status_gossip_live "${output_dir}/status-${host}.txt" | grep -Fxq "${target}"; then
            return 1
        fi
    done
}

# Leaves a verdict file in place for evidence that a verifier could not judge, so a summary can read
# every verdict it names.
node_settings_verdict_or_unjudged() {
    local verdict="$1"
    if ! jq -e 'type == "object" and has("verdict")' "${verdict}" >/dev/null 2>&1; then
        jq -n '{verdict: "unjudged", problems: ["the evidence could not be judged"]}' >"${verdict}"
    fi
}

# Prints the Raft term and leader a node's own status in FILE names, as "TERM LEADER".
node_settings_term_and_leader() {
    printf '%s %s\n' "$(status_value "$1" raft.current_term)" "$(status_value "$1" raft.current_leader)"
}

# Checks the node lifecycle events the live recording holds from SINCE_NS through now: TARGET_HOST's
# container must have exactly the EXPECTED events, and every other node none.
node_settings_check_events() {
    local case_dir="$1"
    local since_ns="$2"
    local target_host="$3"
    shift 3
    docker_event_window "${since_ns}" "${case_dir}/node-events.ndjson" --role node \
        || node_settings_fail controller 'the live Docker event recording does not cover the case'
    local target_id
    target_id="$(owned_service_container "${target_host}")" || return 1
    jq -c --arg id "${target_id}" 'select(.Actor.ID == $id)' "${case_dir}/node-events.ndjson" \
        >"${case_dir}/node-events-${target_host}.ndjson"
    local expected=()
    local event
    for event in "$@"; do
        expected+=(--expect "${event}")
    done
    "${script_dir}/verify-docker-events.sh" lifecycle --events "${case_dir}/node-events-${target_host}.ndjson" \
        --target "${target_id}" "${expected[@]}" 2>"${case_dir}/node-events-verdict.txt" \
        || recovery_finding "${case_dir}" "${target_host} had lifecycle events other than its planned stop and starts: $(head -n 1 "${case_dir}/node-events-verdict.txt")"
    jq -e -s --arg id "${target_id}" 'all(.[]; .Actor.ID == $id)' "${case_dir}/node-events.ndjson" >/dev/null \
        || recovery_finding "${case_dir}" "a node other than ${target_host} had lifecycle events"
}

# Acknowledged canaries through every node: each named resource must be present through its route.
node_settings_canaries_visible() {
    local case_dir="$1"
    shift
    local host
    for host in "${node_hosts[@]}"; do
        wait_for "control canaries through ${host}" 60 \
            capture_configuration "${host}" "${case_dir}/canaries/${host}" "$@" \
            || node_settings_fail product "the control canaries could not be read through ${host}"
        jq -e '.resources | all(.[]; . == "present")' "${case_dir}/canaries/${host}.json" >/dev/null \
            || recovery_finding "${case_dir}" "an acknowledged control canary is absent through ${host}"
    done
}

# Recovers the whole cluster after TARGET_HOST started again at RESTARTED_AT: listeners, the
# observer, settlement and delivery from OUTPUT_BEFORE on. Writes the settled cluster's evidence to
# CASE_DIR/recovered.
node_settings_recover() {
    local case_dir="$1"
    local target_host="$2"
    local restarted_at="$3"
    local output_before="$4"
    mkdir -p "${case_dir}/recovered"
    wait_for "${target_host} listeners restored" 120 probe_node "${target_host}" \
        || node_settings_fail product "${target_host} did not serve its listeners again after its start"
    local observer_id
    observer_id="$(owned_service_container observer)" || return 1
    wait_for "observer saw ${target_host} ready again" 30 \
        observer_saw "${observer_id}" "${target_host}" ready "${restarted_at}"
    local host
    for host in "${node_hosts[@]}"; do
        wait_for "${host} listeners reachable after the start of ${target_host}" 30 probe_node "${host}"
    done
    wait_for 'every node caught up, connected and executing' "${node_settings_settle_bound}" \
        cluster_settled_and_connected "${case_dir}/recovered" \
        || node_settings_fail product "the cluster did not settle within ${node_settings_settle_bound}s of the start of ${target_host}"
    wait_for "output progressed after the start of ${target_host}" "${node_settings_delivery_bound}" \
        topic_progressed chaos_output "${output_before}" \
        || node_settings_fail product "emitter output did not advance after the start of ${target_host}"
}

# One step of the capability roll: TARGET stops gracefully, starts with the rolled bound, and every
# node must then describe the claims EXPECTATIONS name and adopt them.
capability_roll_step() {
    local case_dir="$1"
    local target="$2"
    local ordinal="$3"
    local deployed_bytes="$4"
    shift 4
    local rolled=("$@")
    local step_dir="${case_dir}/${ordinal}-${target}"
    local target_node="node-${target##*-}"
    mkdir -p "${step_dir}/before"

    phase "capability roll ${ordinal}/${node_count}: graceful stop of ${target}"
    check_support_containers
    check_other_nodes "${target}"
    wait_for "settled cluster before stopping ${target}" "${node_settings_settle_bound}" \
        cluster_settled_and_connected "${step_dir}/before"
    wait_for "${target} describes its own incarnation" "${node_settings_description_bound}" \
        node_settings_describe "${target}" "${step_dir}/before/describe-${target}"
    local incarnation_before
    incarnation_before="$(jq -r --arg node "${target_node}" '.nodes[$node].incarnation // empty' \
        "${step_dir}/before/describe-${target}.json")"
    [[ "${incarnation_before}" =~ ^[0-9]+$ ]] \
        || node_settings_fail product "${target} does not describe its own incarnation"
    # The process about to stop counted no peer connection refused over a prerequisite.
    capture_metrics "${target}"
    cp "${artifact_dir}/public/metrics-${target}.txt" "${step_dir}/metrics-before-stop.txt"
    local refusals
    refusals="$(node_settings_prerequisite_failures "${step_dir}/metrics-before-stop.txt")" \
        || node_settings_fail observation "${target} exposes no interconnect connection failure counters"
    ((refusals == 0)) \
        || recovery_finding "${case_dir}" "${target} counted ${refusals} interconnect connection(s) refused over a prerequisite before its restart"
    local output_before
    output_before="$(topic_end_offset chaos_output)"
    [[ "${output_before}" =~ ^[0-9]+$ ]] || node_settings_fail setup 'the output offset was unavailable before the stop'
    local stop_started_ms
    stop_started_ms="$(epoch_ms)"
    node_fault_stop_inject "${step_dir}" "${target}" 0 "" true
    check_support_containers
    check_other_nodes "${target}"
    wait_for "survivors agree on a caught-up leader while ${target} is stopped" "${node_settings_survivor_bound}" \
        survivors_settled "${target}" "${step_dir}" \
        || node_settings_fail product "the survivors did not agree on a caught-up leader while ${target} was stopped"
    local output_stopped
    output_stopped="$(topic_end_offset chaos_output)"
    wait_for "emitter output advanced while ${target} was stopped" 60 \
        topic_progressed chaos_output "${output_stopped}" \
        || recovery_finding "${case_dir}" "the survivors delivered no output while ${target} was stopped"

    phase "capability roll ${ordinal}/${node_count}: ${target} starts with relay-batch-bytes=${capability_roll_relay_bytes}"
    node_launch_settings "${target}" "NERVIX_INTERCONNECT_RELAY_BATCH_BYTES=${capability_roll_relay_bytes}"
    local restarted_at start_ms
    restarted_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    start_ms="$(epoch_ms)"
    node_fault_stop_restart "${step_dir}" "${target}"
    node_settings_recover "${step_dir}" "${target}" "${restarted_at}" "${output_before}"
    local settled_ms
    settled_ms="$(epoch_ms)"

    phase "capability roll ${ordinal}/${node_count}: claims after ${target} restarted"
    local expectations=()
    local node
    for node in "${node_names[@]}"; do
        if [[ " ${rolled[*]-} ${target_node} " == *" ${node} "* ]]; then
            expectations+=(--expect "${node}=${capability_roll_relay_bytes}")
        else
            expectations+=(--expect "${node}=${deployed_bytes}")
        fi
    done
    expectations+=(--restarted "${target_node}=${incarnation_before}")
    if ! wait_for "every node describes and adopts the claims after ${target} restarted" \
        "${node_settings_description_bound}" node_settings_claims_converged "${step_dir}/claims" "${expectations[@]}"; then
        recovery_finding "${case_dir}" "the nodes did not describe or adopt the relay receive bounds after ${target} restarted: $(jq -r '.problems | join("; ")' "${step_dir}/claims/claims.json" 2>/dev/null)"
    fi
    local converged_ms
    converged_ms="$(epoch_ms)"
    node_settings_verdict_or_unjudged "${step_dir}/claims/claims.json"
    jq -nc \
        --arg node "${target_node}" \
        --arg host "${target}" \
        --arg directory "${ordinal}-${target}" \
        --arg incarnation_before "${incarnation_before}" \
        --argjson stop_ms "${node_fault_stop_ms:-null}" \
        --argjson settled_ms "$((settled_ms - start_ms))" \
        --argjson converged_ms "$((converged_ms - start_ms))" \
        --argjson whole_ms "$((converged_ms - stop_started_ms))" \
        --slurpfile claims "${step_dir}/claims/claims.json" \
        '{node: $node, host: $host, incarnation_before: $incarnation_before,
          incarnation_after: ($claims[0].incarnations // {})[$node],
          stop_duration_ms: $stop_ms, settled_recovery_ms: $settled_ms,
          claims_converged_ms: $converged_ms, step_ms: $whole_ms,
          verdict: $claims[0].verdict, expected: $claims[0].expected,
          effective: $claims[0].effective,
          claims: "capability-roll/\($directory)/claims/claims.json"}' \
        >>"${case_dir}/steps.ndjson"
}

run_capability_roll() {
    local case_dir="${artifact_dir}/capability-roll"
    mkdir -p "${case_dir}"
    : >"${case_dir}/steps.ndjson"
    # The dollars in this jq filter are jq variables, not shell expansion.
    # shellcheck disable=SC2016
    update_manifest '.node_settings = {setting: "NERVIX_INTERCONNECT_RELAY_BATCH_BYTES", value: $bytes,
                                       applied: "node-launch/changes.ndjson"}' \
        --arg bytes "${capability_roll_relay_bytes}"
    recovery_traffic_startup "${case_dir}"

    failure_category=product
    phase 'capability roll: settled cluster and committed claims'
    wait_for 'settled, connected cluster before the roll' "${node_settings_settle_bound}" \
        observe_partition_roles "${case_dir}/before"
    local leader
    leader="$(jq -r '.leader' "${case_dir}/before/role.json")"
    wait_for 'every node describes the committed claims' "${node_settings_description_bound}" \
        node_settings_describe_hosts "${case_dir}/before/claims" "${node_hosts[*]}"
    local deployed_bytes
    deployed_bytes="$(jq -r --arg node "${leader}" '.nodes[$node].claim.relay_batch_bytes // empty' \
        "${case_dir}/before/claims/describe-nervix-${leader##*-}.json")"
    [[ "${deployed_bytes}" =~ ^[1-9][0-9]*$ ]] \
        || node_settings_fail product "the leader ${leader} describes no committed relay receive bound of its own"
    ((deployed_bytes > capability_roll_relay_bytes)) \
        || node_settings_fail setup "the deployment's relay receive bound of ${deployed_bytes} bytes is not above ${capability_roll_relay_bytes}, so the roll would change nothing"
    local expectations=()
    local node
    for node in "${node_names[@]}"; do
        expectations+=(--expect "${node}=${deployed_bytes}")
    done
    wait_for 'every node adopted every committed claim before the roll' "${node_settings_description_bound}" \
        node_settings_claims_converged "${case_dir}/before/claims" "${expectations[@]}" \
        || node_settings_fail product "the nodes did not agree on the relay receive bounds before the roll; see capability-roll/before/claims/claims.json"
    record_node_volumes "${case_dir}/volumes-before.json"
    local roll_since roll_since_ns
    roll_since="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    roll_since_ns="$(date -d "${roll_since}" +%s%N)"

    local targets=("nervix-${leader##*-}")
    local host
    for host in "${node_hosts[@]}"; do
        [[ "${host}" == "${targets[0]}" ]] || targets+=("${host}")
    done
    local rolled=()
    local ordinal=0
    local target
    for target in "${targets[@]}"; do
        ordinal=$((ordinal + 1))
        capability_roll_step "${case_dir}" "${target}" "${ordinal}" "${deployed_bytes}" ${rolled[@]+"${rolled[@]}"}
        rolled+=("node-${target##*-}")
    done
    # The last step's claims name every node at the rolled bound: the change in full effect.
    cp "${case_dir}/${ordinal}-${target}/claims/claims.json" "${case_dir}/final-claims.json"

    phase 'capability roll: no refused peer and the planned lifecycle'
    failure_category=product
    capture_all_metrics "${case_dir}/final-metrics"
    mkdir -p "${case_dir}/logs"
    local refused_peers='{}'
    for host in "${node_hosts[@]}"; do
        local refusals
        refusals="$(node_settings_prerequisite_failures "${case_dir}/final-metrics/metrics-${host}.txt")" \
            || node_settings_fail observation "${host} exposes no interconnect connection failure counters"
        ((refusals == 0)) \
            || recovery_finding "${case_dir}" "${host} counted ${refusals} interconnect connection(s) refused over a prerequisite during the roll"
        node_settings_log "${host}" "${roll_since}" "${case_dir}/logs/${host}.log" \
            || node_settings_fail controller "the log of ${host} could not be read"
        local refusal_lines
        refusal_lines="$(grep -c -E "this node was refused at join|refused an interconnect peer that cannot run beside this node|refused a node's run that cannot run beside a voter" \
            "${case_dir}/logs/${host}.log" || true)"
        ((refusal_lines == 0)) \
            || recovery_finding "${case_dir}" "${host} logged ${refusal_lines} prerequisite refusal(s) during the roll; see capability-roll/logs/${host}.log"
        refused_peers="$(jq -c --arg host "${host}" --argjson counted "${refusals}" --argjson logged "${refusal_lines}" \
            '.[$host] = {counted: $counted, logged: $logged}' <<<"${refused_peers}")"
    done
    docker_event_window "${roll_since_ns}" "${case_dir}/node-events.ndjson" --role node \
        || node_settings_fail controller 'the live Docker event recording does not cover the roll'
    for host in "${node_hosts[@]}"; do
        local container_id
        container_id="$(owned_service_container "${host}")" || return 1
        jq -c --arg id "${container_id}" 'select(.Actor.ID == $id)' "${case_dir}/node-events.ndjson" \
            >"${case_dir}/node-events-${host}.ndjson"
        "${script_dir}/verify-docker-events.sh" lifecycle --events "${case_dir}/node-events-${host}.ndjson" \
            --target "${container_id}" --expect kill:15 --expect die:0 --expect start \
            || recovery_finding "${case_dir}" "${host} had lifecycle events other than its one graceful stop and explicit start"
    done
    node_volumes_unchanged "${case_dir}/volumes-before.json" "${case_dir}/volumes-after.json"

    jq -n \
        --arg leader "${leader}" \
        --arg roll_since "${roll_since}" \
        --argjson deployed "${deployed_bytes}" \
        --argjson rolled "${capability_roll_relay_bytes}" \
        --argjson order "$(printf '%s\n' "${targets[@]}" | jq -R . | jq -sc .)" \
        --argjson refused "${refused_peers}" \
        --slurpfile steps "${case_dir}/steps.ndjson" \
        --slurpfile final "${case_dir}/final-claims.json" \
        '{evidence_class: "graceful rolling restart: every node stopped with a completed drain and started again from its own container, image, volume and address with a lower interconnect relay receive bound",
          setting: "NERVIX_INTERCONNECT_RELAY_BATCH_BYTES", deployed_relay_batch_bytes: $deployed,
          rolled_relay_batch_bytes: $rolled, leader_before: $leader, order: $order, roll_since: $roll_since,
          steps: $steps, final: {verdict: $final[0].verdict, claims: $final[0].claims,
                                 effective: $final[0].effective},
          prerequisite_refusals: $refused,
          node_events: "capability-roll/node-events.ndjson",
          findings: "capability-roll/findings.ndjson"}' \
        >"${artifact_dir}/results/capability-roll-progress.json"

    recovery_final_boundary
}

# Samples HOST's own SHOW CLUSTER STATUS from one administration container several times a second
# for at most SECONDS, stamping each answer when it arrives with the container's clock, which is the
# host's. It runs in the background, so sampling is already running when the node starts; the exit
# trap ends it like the cluster restart's sampler.
parameter_return_start_sampler() {
    local transcript="$1"
    local host="$2"
    local seconds="$3"
    cluster_restart_sampler_name="${project_name}-return-sampler"
    timeout --kill-after=5s "$((seconds + 60))s" \
        docker compose "${compose_args[@]}" run --rm --no-deps -T \
        --name "${cluster_restart_sampler_name}" admin \
        sh -c '
            host="$1"
            end=$(( $(date +%s) + $2 ))
            while [ "$(date +%s)" -lt "${end}" ]; do
                answer="$(timeout 5 nervix-cli --server "http://${host}:47391" \
                    --password "${NERVIX_PASSWORD}" --command "SHOW CLUSTER STATUS;" 2>&1)"
                printf "=== status %s %s\n%s\n" "${host}" "$(date +%s%N)" "${answer}"
                sleep 0.2
            done
        ' sh "${host}" "${seconds}" </dev/null >"${transcript}" 2>"${transcript%.log}.stderr" &
    cluster_restart_sampler_pid=$!
}

parameter_return_stop_sampler() {
    if [[ -n "${cluster_restart_sampler_pid:-}" ]]; then
        kill "${cluster_restart_sampler_pid}" 2>/dev/null || true
        wait "${cluster_restart_sampler_pid}" 2>/dev/null || true
        cluster_restart_sampler_pid=""
        run_bounded 30 docker container rm --force "${cluster_restart_sampler_name}" >/dev/null 2>&1 || true
    fi
}

run_parameter_return() {
    local case_dir="${artifact_dir}/parameter-return"
    mkdir -p "${case_dir}"
    # The dollars in this jq filter are jq variables, not shell expansion.
    # shellcheck disable=SC2016
    update_manifest '.node_settings = {statement: $statement, own_setting: $seed,
                                       applied: "node-launch/changes.ndjson"}' \
        --arg statement "${parameter_return_statement}" --arg seed "${parameter_return_seed}"
    recovery_traffic_startup "${case_dir}"

    failure_category=product
    phase 'parameter return: settled roles and the committed record'
    wait_for 'settled, connected cluster before the outage' "${node_settings_settle_bound}" \
        observe_partition_roles "${case_dir}/before"
    local leader follower follower_host
    leader="$(jq -r '.leader' "${case_dir}/before/role.json")"
    follower="$(jq -r '.follower' "${case_dir}/before/role.json")"
    follower_host="nervix-${follower##*-}"
    wait_for 'every node runs one committed record' "${node_settings_description_bound}" \
        capture_cluster_parameters "${case_dir}/before/parameters" "${node_names[@]}"
    wait_for 'every node describes the committed record' "${node_settings_description_bound}" \
        node_settings_describe_hosts "${case_dir}/before/describe" "${node_hosts[*]}"
    local before_description="${case_dir}/before/describe/describe-${follower_host}.json"
    local revision_before
    revision_before="$(jq -r '.revision' "${case_dir}/before/parameters.json")"
    local previous
    for previous in "${parameter_return_previous[@]}"; do
        jq -e --arg parameter "${previous%%=*}" --arg value "${previous#*=}" '.committed[$parameter] == $value' \
            "${before_description}" >/dev/null \
            || node_settings_fail setup "the committed record does not hold ${previous}, the value the returning follower must not run"
    done
    local outcome
    outcome="$(configuration_change "${case_dir}" chaos_return_before "nervix-${leader##*-}" chaos_baseline \
        'CREATE RESOURCE chaos_return_before;' "created resource 'chaos_return_before'")"
    [[ "${outcome}" == acknowledged ]] \
        || node_settings_fail product "the control canary before the outage was ${outcome}, not acknowledged"
    local node_ids=()
    local host
    for host in "${node_hosts[@]}"; do
        node_ids+=("$(owned_service_container "${host}")")
    done
    run_bounded 20 docker inspect "${node_ids[@]}" >"${case_dir}/before-all-nodes.json"
    record_node_volumes "${case_dir}/volumes-before.json"
    check_support_containers
    check_other_nodes ""

    phase "parameter return: graceful stop of ${follower_host}"
    mkdir -p "${case_dir}/stop" "${case_dir}/stopped"
    local stop_since_ns
    stop_since_ns="$(date +%s%N)"
    node_fault_stop_inject "${case_dir}/stop" "${follower_host}" 0 "" true
    check_support_containers
    check_other_nodes "${follower_host}"
    wait_for 'survivors agree on a caught-up leader' "${node_settings_survivor_bound}" \
        survivors_settled "${follower_host}" "${case_dir}/stopped" \
        || node_settings_fail product "the survivors did not agree on a caught-up leader while ${follower_host} was stopped"
    local survivor_leader survivor_leader_host
    survivor_leader="$(<"${case_dir}/stopped/survivor-leader.txt")"
    survivor_leader_host="nervix-${survivor_leader##*-}"
    wait_for "the survivors no longer count ${follower_host} available" "${node_settings_unavailable_bound}" \
        node_settings_target_unavailable "${follower_host}" "${case_dir}/unavailable" \
        || node_settings_fail product "the survivors kept counting the stopped ${follower_host} available"
    local survivors=()
    for host in "${node_hosts[@]}"; do
        [[ "${host}" == "${follower_host}" ]] || survivors+=("${host}")
    done

    phase 'parameter return: committed change while the follower is down'
    local change_ms
    change_ms="$(epoch_ms)"
    outcome="$(node_settings_parameter_change "${survivor_leader_host}" "${parameter_return_statement}" "${case_dir}/change")"
    [[ "${outcome}" == acknowledged ]] \
        || node_settings_fail product "the parameter change was ${outcome} while ${follower_host} was down; see parameter-return/change.txt"
    wait_for 'the survivors run the committed change' "${node_settings_installation_bound}" \
        node_settings_installed "${case_dir}/survivors" "${before_description}" "${parameter_return_liveness}" \
        "${survivors[*]}" "${parameter_return_assignments[@]}" \
        || node_settings_fail product "the survivors did not run the committed change; see parameter-return/survivors/installed.json"
    local revision_after
    revision_after="$(jq -r '.revision' "${case_dir}/survivors/installed.json")"
    local output_down
    output_down="$(topic_end_offset chaos_output)"
    wait_for 'emitter output advanced while the follower was down' 60 \
        topic_progressed chaos_output "${output_down}" \
        || recovery_finding "${case_dir}" "the survivors delivered no output while ${follower_host} was down"

    phase "parameter return: ${follower_host} names the parameter as its own setting"
    node_launch_settings "${follower_host}" "${parameter_return_seed}"
    node_settings_start_until_exit "${case_dir}" "${follower_host}" seed-start
    local seed_exit
    seed_exit="$(<"${case_dir}/seed-start.exit")"
    [[ "${seed_exit}" == 1 ]] \
        || recovery_finding "${case_dir}" "${follower_host} ended its start with a parameter seed with status ${seed_exit}, not 1"
    "${node_settings_verifier}" seed-refusal --log "${case_dir}/seed-start.log" \
        --parameter "${parameter_return_seed_parameter}" --output "${case_dir}/seed-refusal.json" \
        >"${case_dir}/seed-refusal.txt" 2>&1 \
        || recovery_finding "${case_dir}" "${follower_host} did not refuse ${parameter_return_seed_parameter} as a first-bootstrap seed; see parameter-return/seed-refusal.json"
    node_settings_verdict_or_unjudged "${case_dir}/seed-refusal.json"

    phase "parameter return: ${follower_host} returns with the deployment settings"
    node_launch_settings "${follower_host}"
    parameter_return_start_sampler "${case_dir}/samples.log" "${follower_host}" "${node_settings_sampler_seconds}"
    local restarted_at start_ms start_since
    restarted_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    start_since="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    start_ms="$(epoch_ms)"
    node_fault_stop_restart "${case_dir}/stop" "${follower_host}"
    wait_for "${follower_host} listeners restored" 120 probe_node "${follower_host}" \
        || node_settings_fail product "${follower_host} did not serve its listeners again after its return"
    if ! wait_for "${follower_host} runs the committed record" "${node_settings_installation_bound}" \
        node_settings_installed "${case_dir}/returned" "${before_description}" "${parameter_return_liveness}" \
        "${follower_host}" "${parameter_return_assignments[@]}"; then
        recovery_finding "${case_dir}" "${follower_host} did not run the committed record after its return: $(jq -r '.problems | join("; ")' "${case_dir}/returned/installed.json" 2>/dev/null)"
    fi
    local adopted_ms
    adopted_ms="$(epoch_ms)"
    # The sampler keeps answering for two seconds more, so its last answers show the node keeps the
    # record it adopted; it then ends with its transcript complete.
    sleep 2
    parameter_return_stop_sampler
    local sample_expectations=()
    local assignment
    for assignment in "${parameter_return_assignments[@]}"; do
        sample_expectations+=(--expect "${assignment}")
    done
    "${node_settings_verifier}" samples --transcript "${case_dir}/samples.log" --node "${follower}" \
        --revision "${revision_after}" "${sample_expectations[@]}" \
        --output "${case_dir}/samples.json" >"${case_dir}/samples.txt" 2>&1 \
        || recovery_finding "${case_dir}" "${follower_host} reported running another record than the live one while it returned: $(jq -r '.problems | join("; ")' "${case_dir}/samples.json" 2>/dev/null)"
    node_settings_verdict_or_unjudged "${case_dir}/samples.json"
    node_settings_log "${follower_host}" "${start_since}" "${case_dir}/returned.log" \
        || node_settings_fail controller "the log of the returning ${follower_host} could not be read"
    "${node_settings_verifier}" installations --log "${case_dir}/returned.log" --revision "${revision_after}" \
        --output "${case_dir}/installations.json" >"${case_dir}/installations.txt" 2>&1 \
        || recovery_finding "${case_dir}" "the returning ${follower_host} installed another parameter revision first: $(jq -r '.problems | join("; ")' "${case_dir}/installations.json" 2>/dev/null)"
    node_settings_verdict_or_unjudged "${case_dir}/installations.json"

    phase 'parameter return: no drift and public recovery'
    node_settings_recover "${case_dir}" "${follower_host}" "${restarted_at}" "${output_down}"
    local settled_ms
    settled_ms="$(epoch_ms)"
    wait_for 'every node runs the committed record' "${node_settings_installation_bound}" \
        node_settings_installed "${case_dir}/after" "${before_description}" "${parameter_return_liveness}" \
        "${node_hosts[*]}" "${parameter_return_assignments[@]}" \
        || recovery_finding "${case_dir}" "a node does not run the committed record after the return: $(jq -r '.problems | join("; ")' "${case_dir}/after/installed.json" 2>/dev/null)"
    node_settings_verdict_or_unjudged "${case_dir}/after/installed.json"
    jq -e --arg revision "${revision_after}" '.revision == $revision' "${case_dir}/after/installed.json" >/dev/null 2>&1 \
        || recovery_finding "${case_dir}" "the committed revision moved from ${revision_after} after the return"
    outcome="$(configuration_change "${case_dir}" chaos_return_after "${follower_host}" chaos_baseline \
        'CREATE RESOURCE chaos_return_after;' "created resource 'chaos_return_after'")"
    [[ "${outcome}" == acknowledged ]] \
        || node_settings_fail product "the control canary through the returned ${follower_host} was ${outcome}, not acknowledged"
    node_settings_canaries_visible "${case_dir}" chaos_return_before chaos_return_after
    node_settings_check_events "${case_dir}" "${stop_since_ns}" "${follower_host}" \
        kill:15 die:0 start die:1 start
    other_node_instances_unchanged "${follower_host}" "${case_dir}/before-all-nodes.json" \
        || recovery_finding "${case_dir}" 'a node other than the returning follower changed its container or process'
    node_volumes_unchanged "${case_dir}/volumes-before.json" "${case_dir}/volumes-after.json"
    capture_all_metrics "${case_dir}/final-metrics"

    jq -n \
        --arg follower "${follower}" \
        --arg leader_before "${leader}" \
        --arg survivor_leader "${survivor_leader}" \
        --arg statement "${parameter_return_statement}" \
        --arg seed "${parameter_return_seed}" \
        --arg revision_before "${revision_before}" \
        --arg revision_after "${revision_after}" \
        --arg seed_exit "${seed_exit}" \
        --argjson change_to_return_ms "$((start_ms - change_ms))" \
        --argjson adopted_ms "$((adopted_ms - start_ms))" \
        --argjson settled_ms "$((settled_ms - start_ms))" \
        --slurpfile change "${case_dir}/change.json" \
        --slurpfile seed_refusal "${case_dir}/seed-refusal.json" \
        --slurpfile samples "${case_dir}/samples.json" \
        --slurpfile installations "${case_dir}/installations.json" \
        --slurpfile after "${case_dir}/after/installed.json" \
        '{evidence_class: "a follower stopped gracefully while a committed cluster parameter change applied, then started from its own container, image, volume and address",
          follower: $follower, leader_before: $leader_before, survivor_leader: $survivor_leader,
          change: ($change[0] + {revision_before: $revision_before, revision_after: $revision_after}),
          own_setting: {setting: $seed, exit_code: ($seed_exit | tonumber? // $seed_exit),
                        verdict: $seed_refusal[0].verdict},
          return: {samples: ($samples[0] | {verdict, samples, not_installed_samples, running_samples,
                                            initialization, first_running_ns, stale}),
                   installations: $installations[0].installed,
                   change_to_return_ms: $change_to_return_ms, adopted_ms: $adopted_ms,
                   settled_recovery_ms: $settled_ms},
          drift: {verdict: $after[0].verdict, revision: $after[0].revision, committed: $after[0].committed,
                  liveness: $after[0].liveness},
          node_events: "parameter-return/node-events.ndjson",
          findings: "parameter-return/findings.ndjson"}' \
        >"${artifact_dir}/results/parameter-return-progress.json"

    recovery_final_boundary
}

# True once the error-event observer of every node printed the notice of its node's installation
# of REVISION.
parameter_live_announced() {
    local revision="$1"
    local host
    for host in "${node_hosts[@]}"; do
        run_bounded 20 docker logs "$(events_observer_name "${host}")" 2>&1 \
            | grep -Fq "[events] cluster parameters INFO: cluster parameters committed at revision ${revision}" \
            || return 1
    done
}

run_parameter_live() {
    local case_dir="${artifact_dir}/parameter-live"
    mkdir -p "${case_dir}"
    : >"${case_dir}/transitions.ndjson"
    # The dollars in this jq filter are jq variables, not shell expansion.
    # shellcheck disable=SC2016
    update_manifest '.node_settings = {statements: $statements}' \
        --argjson statements "$(printf '%s\n' "${parameter_live_statements[@]}" | jq -R . | jq -sc .)"
    recovery_traffic_startup "${case_dir}"

    failure_category=controller
    phase 'parameter live: error-event observers'
    events_observer_prepare "${case_dir}" \
        || node_settings_fail controller "the error-event observers' quiet domain could not be created; see parameter-live/observer-*.txt"
    events_observer_start "${case_dir}" \
        || node_settings_fail controller 'the error-event observers did not attach'

    failure_category=product
    phase 'parameter live: settled cluster and the committed record'
    wait_for 'settled, connected cluster before the changes' "${node_settings_settle_bound}" \
        observe_partition_roles "${case_dir}/before"
    local leader leader_host
    leader="$(jq -r '.leader' "${case_dir}/before/role.json")"
    leader_host="nervix-${leader##*-}"
    local term_and_leader_before
    term_and_leader_before="$(node_settings_term_and_leader "${case_dir}/before/status-${leader_host}.attempt.txt")"
    wait_for 'every node runs one committed record' "${node_settings_description_bound}" \
        capture_cluster_parameters "${case_dir}/before/parameters" "${node_names[@]}"
    wait_for 'every node describes the committed record' "${node_settings_description_bound}" \
        node_settings_describe_hosts "${case_dir}/before/describe" "${node_hosts[*]}"
    local before="${case_dir}/before/describe/describe-${leader_host}.json"
    local assignment
    for assignment in ${parameter_live_assignments[1]}; do
        jq -e --arg parameter "${assignment%%=*}" --arg value "${assignment#*=}" '.committed[$parameter] == $value' \
            "${before}" >/dev/null \
            || node_settings_fail setup "the committed record does not hold the deployment's ${assignment}, which the second change restores"
    done
    jq -e --arg liveness "${parameter_live_liveness[1]}" 'all(.nodes[]; .liveness == $liveness)' "${before}" >/dev/null \
        || node_settings_fail controller "the deployment's interconnect liveness is not described as ${parameter_live_liveness[1]}; see parameter-live/before/describe"
    local node_ids=()
    local host
    for host in "${node_hosts[@]}"; do
        node_ids+=("$(owned_service_container "${host}")")
    done
    run_bounded 20 docker inspect "${node_ids[@]}" >"${case_dir}/before-all-nodes.json"
    capture_all_metrics "${case_dir}/metrics-before"
    "${script_dir}/negative-outcomes.sh" snapshot --domain chaos_baseline \
        --ingestor chaos_ingestor --emitter chaos_emitter \
        --output "${case_dir}/negative-outcomes-before.json" "${case_dir}/metrics-before"/metrics-*.txt \
        || node_settings_fail controller 'the negative-outcome counters before the changes could not be read'
    local window_since_ns
    window_since_ns="$(date +%s%N)"

    local revisions=()
    local index
    for index in "${!parameter_live_statements[@]}"; do
        local ordinal=$((index + 1))
        local statement="${parameter_live_statements[${index}]}"
        local liveness="${parameter_live_liveness[${index}]}"
        local assignments=()
        read -r -a assignments <<<"${parameter_live_assignments[${index}]}"
        phase "parameter live: change ${ordinal} of ${#parameter_live_statements[@]} under load"
        check_support_containers
        local output_before change_ms
        output_before="$(topic_end_offset chaos_output)"
        [[ "${output_before}" =~ ^[0-9]+$ ]] || node_settings_fail setup 'the output offset was unavailable before the change'
        change_ms="$(epoch_ms)"
        local outcome
        outcome="$(node_settings_parameter_change "${leader_host}" "${statement}" "${case_dir}/change-${ordinal}")"
        [[ "${outcome}" == acknowledged ]] \
            || node_settings_fail product "change ${ordinal} was ${outcome}; see parameter-live/change-${ordinal}.txt"
        local answered_ms
        answered_ms="$(epoch_ms)"
        if ! wait_for "every node runs change ${ordinal}" "${node_settings_installation_bound}" \
            node_settings_installed "${case_dir}/installed-${ordinal}" "${before}" "${liveness}" \
            "${node_hosts[*]}" "${assignments[@]}"; then
            node_settings_fail product "change ${ordinal} was acknowledged but not run by every node: $(jq -r '.problems | join("; ")' "${case_dir}/installed-${ordinal}/installed.json" 2>/dev/null)"
        fi
        local installed_ms revision
        installed_ms="$(epoch_ms)"
        revision="$(jq -r '.revision' "${case_dir}/installed-${ordinal}/installed.json")"
        revisions+=("${revision}")
        wait_for "every node announced revision ${revision} to its sessions" "${node_settings_announcement_bound}" \
            parameter_live_announced "${revision}" \
            || recovery_finding "${case_dir}" "a node did not announce its installation of revision ${revision} to its sessions"
        local announced_ms
        announced_ms="$(epoch_ms)"
        wait_for "output advanced after change ${ordinal}" 30 topic_progressed chaos_output "${output_before}" \
            || recovery_finding "${case_dir}" "emitter output stalled after change ${ordinal}"
        jq -nc \
            --argjson ordinal "${ordinal}" \
            --arg revision "${revision}" \
            --arg liveness "${liveness}" \
            --argjson answered_ms "$((answered_ms - change_ms))" \
            --argjson installed_ms "$((installed_ms - change_ms))" \
            --argjson announced_ms "$((announced_ms - change_ms))" \
            --slurpfile change "${case_dir}/change-${ordinal}.json" \
            --slurpfile installed "${case_dir}/installed-${ordinal}/installed.json" \
            '$change[0] + {ordinal: $ordinal, revision: $revision, liveness: $liveness,
                           committed: $installed[0].committed, answered_ms: $answered_ms,
                           every_node_running_ms: $installed_ms, every_node_announced_ms: $announced_ms}' \
            >>"${case_dir}/transitions.ndjson"
        before="${case_dir}/installed-${ordinal}/describe-${leader_host}.json"
    done

    phase 'parameter live: nothing else changed'
    wait_for 'settled, connected cluster after the changes' "${node_settings_settle_bound}" \
        observe_partition_roles "${case_dir}/after" \
        || node_settings_fail product 'the cluster did not stay settled and connected through the changes'
    local term_and_leader_after
    term_and_leader_after="$(node_settings_term_and_leader "${case_dir}/after/status-${leader_host}.attempt.txt")"
    [[ "${term_and_leader_after}" == "${term_and_leader_before}" ]] \
        || recovery_finding "${case_dir}" "the Raft term and leader moved from ${term_and_leader_before} to ${term_and_leader_after} through the changes"
    jq -e --slurpfile before "${case_dir}/before/role.json" \
        '.ingestor_owner == $before[0].ingestor_owner and .relay_owner == $before[0].relay_owner
         and .emitter_owner == $before[0].emitter_owner' "${case_dir}/after/role.json" >/dev/null \
        || recovery_finding "${case_dir}" 'the changes moved an execution owner'
    wait_for 'every node describes the record after the changes' "${node_settings_description_bound}" \
        node_settings_describe_hosts "${case_dir}/after/describe" "${node_hosts[*]}"
    jq -e -n --slurpfile before "${case_dir}/before/describe/describe-${leader_host}.json" \
        --slurpfile after "${case_dir}/after/describe/describe-${leader_host}.json" \
        '($before[0].nodes | map_values(.incarnation)) == ($after[0].nodes | map_values(.incarnation))' >/dev/null \
        || recovery_finding "${case_dir}" 'a node changed its incarnation through the changes'
    other_node_instances_unchanged "" "${case_dir}/before-all-nodes.json" \
        || recovery_finding "${case_dir}" 'a node container restarted through the changes'
    docker_event_window "${window_since_ns}" "${case_dir}/node-events.ndjson" --role node \
        || node_settings_fail controller 'the live Docker event recording does not cover the changes'
    "${script_dir}/verify-docker-events.sh" lifecycle --events "${case_dir}/node-events.ndjson" \
        2>"${case_dir}/node-events-verdict.txt" \
        || recovery_finding "${case_dir}" "a node had lifecycle events through the changes: $(head -n 1 "${case_dir}/node-events-verdict.txt")"
    capture_all_metrics "${case_dir}/metrics-after"
    "${script_dir}/negative-outcomes.sh" snapshot --domain chaos_baseline \
        --ingestor chaos_ingestor --emitter chaos_emitter \
        --output "${case_dir}/negative-outcomes-after.json" "${case_dir}/metrics-after"/metrics-*.txt \
        || node_settings_fail controller 'the negative-outcome counters after the changes could not be read'
    events_observer_collect "${case_dir}" \
        || node_settings_fail controller "the error-event observers' transcripts could not be read"
    local transcripts=()
    for host in "${node_hosts[@]}"; do
        transcripts+=("${case_dir}/events-${host}.log")
    done
    local notice_revisions=()
    local revision
    for revision in "${revisions[@]}"; do
        notice_revisions+=(--revision "${revision}")
    done
    "${node_settings_verifier}" notices --window "${case_dir}/events-window.json" "${notice_revisions[@]}" \
        --output "${case_dir}/notices.json" "${transcripts[@]}" >"${case_dir}/notices.txt" 2>&1 \
        || recovery_finding "${case_dir}" "the nodes' sessions did not receive exactly one notice of each installed revision and nothing else: $(jq -r '.problems | join("; ")' "${case_dir}/notices.json" 2>/dev/null)"
    node_settings_verdict_or_unjudged "${case_dir}/notices.json"

    jq -n \
        --arg leader "${leader}" \
        --arg before "${term_and_leader_before}" \
        --arg after "${term_and_leader_after}" \
        --slurpfile changes <(cat "${case_dir}/transitions.ndjson") \
        --slurpfile owners_before "${case_dir}/before/role.json" \
        --slurpfile owners_after "${case_dir}/after/role.json" \
        --slurpfile notices "${case_dir}/notices.json" \
        '{evidence_class: "committed cluster parameter changes applied live, under continuous Kafka traffic across three nodes",
          leader: $leader, raft_term_and_leader: {before: $before, after: $after},
          owners: {before: $owners_before[0], after: $owners_after[0]},
          changes: $changes, notices: ($notices[0] | {verdict, expected_revisions}),
          events_window: "parameter-live/events-window.json",
          negative_outcomes: {before: "parameter-live/negative-outcomes-before.json",
                              after: "parameter-live/negative-outcomes-after.json"},
          delivery: "parameter-live/delivery.json",
          node_events: "parameter-live/node-events.ndjson",
          findings: "parameter-live/findings.ndjson"}' \
        >"${artifact_dir}/results/parameter-live-progress.json"

    recovery_final_boundary
}

# Run by run-baseline.sh once the shared ledger has read the whole output: the strict verdict of the
# baseline path, with the negative-outcome counters and the error-event observers of the changes'
# window as inputs. Every symptom is a finding.
parameter_live_delivery_verdict() {
    local case_dir="${artifact_dir}/parameter-live"
    local transcripts=()
    local host
    for host in "${node_hosts[@]}"; do
        transcripts+=(--events "${case_dir}/events-${host}.log")
    done
    local status=0
    "${script_dir}/verify-strict-delivery.sh" \
        --output "${artifact_dir}/traffic/observed-output.ndjson" \
        --source "baseline=${artifact_dir}/traffic/accepted-input.ndjson" \
        --negative-outcomes "${case_dir}/negative-outcomes-before.json" "${case_dir}/negative-outcomes-after.json" \
        --events-window "${case_dir}/events-window.json" "${transcripts[@]}" \
        --result "${case_dir}/delivery.json" >"${case_dir}/delivery.txt" 2>&1 || status=$?
    ((status <= 1)) \
        || node_settings_fail observation 'the strict delivery evidence of the changes cannot be judged; see parameter-live/delivery.txt'
    local finding
    while IFS= read -r finding; do
        recovery_finding "${case_dir}" "${finding}"
    done < <(jq -r '.symptoms[] | "\(.name) through the live changes: " + ({sources, count, increases, distinct} | with_entries(select(.value != null)) | tojson)' \
        "${case_dir}/delivery.json")
}

run_prerequisite_refusal() {
    local case_dir="${artifact_dir}/prerequisite-refusal"
    mkdir -p "${case_dir}"
    [[ "${CHAOS_RAFT_HEARTBEAT_INTERVAL:-250ms}" == "${prerequisite_refusal_deployed_heartbeat}" ]] \
        || node_settings_fail setup "the deployment's Raft heartbeat is ${CHAOS_RAFT_HEARTBEAT_INTERVAL}, not ${prerequisite_refusal_deployed_heartbeat}"
    # The dollars in this jq filter are jq variables, not shell expansion.
    # shellcheck disable=SC2016
    update_manifest '.node_settings = {setting: "NERVIX_RAFT_HEARTBEAT_INTERVAL", value: $refused,
                                       deployment: $deployed, applied: "node-launch/changes.ndjson"}' \
        --arg refused "${prerequisite_refusal_heartbeat}" --arg deployed "${prerequisite_refusal_deployed_heartbeat}"
    recovery_traffic_startup "${case_dir}"

    failure_category=product
    phase 'prerequisite refusal: settled roles'
    wait_for 'settled, connected cluster before the refusal' "${node_settings_settle_bound}" \
        observe_partition_roles "${case_dir}/before"
    local leader target target_host
    leader="$(jq -r '.leader' "${case_dir}/before/role.json")"
    target="$(jq -r '.follower' "${case_dir}/before/role.json")"
    target_host="nervix-${target##*-}"
    local survivors=() survivor_nodes=() survivor_options=()
    local host
    for host in "${node_hosts[@]}"; do
        if [[ "${host}" != "${target_host}" ]]; then
            survivors+=("${host}")
            survivor_nodes+=("node-${host##*-}")
            survivor_options+=(--survivor "node-${host##*-}")
        fi
    done
    wait_for 'every survivor describes the live nodes' "${node_settings_description_bound}" \
        node_settings_describe_hosts "${case_dir}/before/describe" "${survivors[*]}"
    local outcome
    outcome="$(configuration_change "${case_dir}" chaos_refusal_before "nervix-${leader##*-}" chaos_baseline \
        'CREATE RESOURCE chaos_refusal_before;' "created resource 'chaos_refusal_before'")"
    [[ "${outcome}" == acknowledged ]] \
        || node_settings_fail product "the control canary before the refusal was ${outcome}, not acknowledged"
    local node_ids=()
    for host in "${node_hosts[@]}"; do
        node_ids+=("$(owned_service_container "${host}")")
    done
    run_bounded 20 docker inspect "${node_ids[@]}" >"${case_dir}/before-all-nodes.json"
    record_node_volumes "${case_dir}/volumes-before.json"
    check_support_containers
    check_other_nodes ""

    phase "prerequisite refusal: graceful stop of ${target_host}"
    mkdir -p "${case_dir}/stop" "${case_dir}/stopped" "${case_dir}/refused"
    local stop_since_ns
    stop_since_ns="$(date +%s%N)"
    node_fault_stop_inject "${case_dir}/stop" "${target_host}" 0 "" true
    check_support_containers
    check_other_nodes "${target_host}"
    wait_for 'survivors agree on a caught-up leader' "${node_settings_survivor_bound}" \
        survivors_settled "${target_host}" "${case_dir}/stopped" \
        || node_settings_fail product "the survivors did not agree on a caught-up leader while ${target_host} was stopped"
    local survivor_leader survivor_leader_host
    survivor_leader="$(<"${case_dir}/stopped/survivor-leader.txt")"
    survivor_leader_host="nervix-${survivor_leader##*-}"
    local term_and_leader_stopped
    term_and_leader_stopped="$(node_settings_term_and_leader "${case_dir}/stopped/survivor-${survivor_leader_host}.attempt.txt")"

    phase "prerequisite refusal: ${target_host} starts with raft-heartbeat-interval=${prerequisite_refusal_heartbeat}"
    node_launch_settings "${target_host}" "NERVIX_RAFT_HEARTBEAT_INTERVAL=${prerequisite_refusal_heartbeat}"
    local refusal_since output_at_refusal refused_start_ms
    refusal_since="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    output_at_refusal="$(topic_end_offset chaos_output)"
    [[ "${output_at_refusal}" =~ ^[0-9]+$ ]] || node_settings_fail setup 'the output offset was unavailable before the refused start'
    refused_start_ms="$(epoch_ms)"
    node_settings_start_until_exit "${case_dir}" "${target_host}" refused-start
    local refused_ms refused_exit
    refused_ms="$(($(epoch_ms) - refused_start_ms))"
    refused_exit="$(<"${case_dir}/refused-start.exit")"
    [[ "${refused_exit}" == 1 ]] \
        || recovery_finding "${case_dir}" "${target_host} ended its refused run with status ${refused_exit}, not 1"
    "${node_settings_verifier}" join-refusal --log "${case_dir}/refused-start.log" "${survivor_options[@]}" \
        --mismatch "${prerequisite_refusal_mismatch}" --output "${case_dir}/refusal.json" \
        >"${case_dir}/refusal.txt" 2>&1 \
        || recovery_finding "${case_dir}" "${target_host} was not refused at join with the prerequisite and both values: $(jq -r '.problems | join("; ")' "${case_dir}/refusal.json" 2>/dev/null)"
    node_settings_verdict_or_unjudged "${case_dir}/refusal.json"

    phase 'prerequisite refusal: the other nodes kept serving'
    check_support_containers
    check_other_nodes "${target_host}"
    wait_for 'survivors still agree on a caught-up leader' "${node_settings_survivor_bound}" \
        survivors_settled "${target_host}" "${case_dir}/refused" \
        || node_settings_fail product "the survivors did not keep a caught-up leader after ${target_host} was refused"
    local term_and_leader_refused
    term_and_leader_refused="$(node_settings_term_and_leader "${case_dir}/refused/survivor-${survivor_leader_host}.attempt.txt")"
    [[ "${term_and_leader_refused}" == "${term_and_leader_stopped}" ]] \
        || recovery_finding "${case_dir}" "the survivors' Raft term and leader moved from ${term_and_leader_stopped} to ${term_and_leader_refused} while ${target_host} was refused"
    wait_for 'emitter output advanced across the refused run' 60 \
        topic_progressed chaos_output "${output_at_refusal}" \
        || recovery_finding "${case_dir}" "the survivors delivered no output across the refused run of ${target_host}"
    local canaries=(chaos_refusal_before)
    outcome="$(configuration_change "${case_dir}" chaos_refusal_during "${survivor_leader_host}" chaos_baseline \
        'CREATE RESOURCE chaos_refusal_during;' "created resource 'chaos_refusal_during'")"
    if [[ "${outcome}" == acknowledged ]]; then
        canaries+=(chaos_refusal_during)
    else
        recovery_finding "${case_dir}" "the control canary through the survivors was ${outcome}, not acknowledged, after ${target_host} was refused"
    fi
    mkdir -p "${case_dir}/survivor-logs"
    local survivor_logs=()
    for host in "${survivors[@]}"; do
        node_settings_log "${host}" "${refusal_since}" "${case_dir}/survivor-logs/${host}.log" \
            || node_settings_fail controller "the log of ${host} could not be read"
        survivor_logs+=("${case_dir}/survivor-logs/${host}.log")
    done
    "${node_settings_verifier}" survivor-refusals --node "${target}" --prerequisite raft-heartbeat-interval \
        --output "${case_dir}/survivor-refusals.json" "${survivor_logs[@]}" >"${case_dir}/survivor-refusals.txt" 2>&1 \
        || recovery_finding "${case_dir}" "no survivor reported refusing ${target} over raft-heartbeat-interval"
    node_settings_verdict_or_unjudged "${case_dir}/survivor-refusals.json"
    wait_for 'every survivor describes the live nodes after the refusal' "${node_settings_description_bound}" \
        node_settings_describe_hosts "${case_dir}/refused/describe" "${survivors[*]}"
    for host in "${survivors[@]}"; do
        jq -e -n --slurpfile before "${case_dir}/before/describe/describe-${host}.json" \
            --slurpfile after "${case_dir}/refused/describe/describe-${host}.json" \
            --argjson survivors "$(printf '%s\n' "${survivor_nodes[@]}" | jq -R . | jq -sc .)" \
            'all($survivors[]; $before[0].nodes[.].incarnation == $after[0].nodes[.].incarnation
                               and $after[0].nodes[.].incarnation != null)' >/dev/null \
            || recovery_finding "${case_dir}" "${host} describes a survivor at another incarnation after the refusal"
    done

    phase "prerequisite refusal: ${target_host} returns with the deployment's heartbeat"
    node_launch_settings "${target_host}"
    local restarted_at start_ms
    restarted_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    start_ms="$(epoch_ms)"
    node_fault_stop_restart "${case_dir}/stop" "${target_host}"
    node_settings_recover "${case_dir}" "${target_host}" "${restarted_at}" "${output_at_refusal}"
    local settled_ms
    settled_ms="$(epoch_ms)"
    outcome="$(configuration_change "${case_dir}" chaos_refusal_after "${target_host}" chaos_baseline \
        'CREATE RESOURCE chaos_refusal_after;' "created resource 'chaos_refusal_after'")"
    [[ "${outcome}" == acknowledged ]] \
        || node_settings_fail product "the control canary through the restored ${target_host} was ${outcome}, not acknowledged"
    canaries+=(chaos_refusal_after)
    node_settings_canaries_visible "${case_dir}" "${canaries[@]}"
    wait_for 'every node describes the restored heartbeat' "${node_settings_description_bound}" \
        node_settings_describe_hosts "${case_dir}/restored/describe" "${node_hosts[*]}"
    for host in "${node_hosts[@]}"; do
        jq -e --arg target "${target}" --arg heartbeat "${prerequisite_refusal_deployed_heartbeat}" \
            '.timing_drift == "none" and .nodes[$target].prerequisites["raft-heartbeat-interval"] == $heartbeat' \
            "${case_dir}/restored/describe/describe-${host}.json" >/dev/null \
            || recovery_finding "${case_dir}" "${host} still describes a timing drift or another heartbeat of ${target} after its return"
    done
    node_settings_check_events "${case_dir}" "${stop_since_ns}" "${target_host}" \
        kill:15 die:0 start die:1 start
    other_node_instances_unchanged "${target_host}" "${case_dir}/before-all-nodes.json" \
        || recovery_finding "${case_dir}" 'a survivor changed its container or process'
    node_volumes_unchanged "${case_dir}/volumes-before.json" "${case_dir}/volumes-after.json"
    capture_all_metrics "${case_dir}/final-metrics"

    jq -n \
        --arg target "${target}" \
        --arg leader_before "${leader}" \
        --arg survivor_leader "${survivor_leader}" \
        --arg heartbeat "${prerequisite_refusal_heartbeat}" \
        --arg deployed "${prerequisite_refusal_deployed_heartbeat}" \
        --arg mismatch "${prerequisite_refusal_mismatch}" \
        --arg refused_exit "${refused_exit}" \
        --arg stopped "${term_and_leader_stopped}" \
        --arg refused "${term_and_leader_refused}" \
        --argjson refused_ms "${refused_ms}" \
        --argjson settled_ms "$((settled_ms - start_ms))" \
        --slurpfile refusal "${case_dir}/refusal.json" \
        --slurpfile survivors "${case_dir}/survivor-refusals.json" \
        '{evidence_class: "a follower stopped gracefully and started from its own container, image, volume and address with a Raft heartbeat outside the compatibility bound, then again with the deployment'"'"'s",
          target: $target, leader_before: $leader_before, survivor_leader: $survivor_leader,
          setting: "NERVIX_RAFT_HEARTBEAT_INTERVAL", refused_value: $heartbeat, deployment_value: $deployed,
          refusal: {expected_mismatch: $mismatch, verdict: $refusal[0].verdict, peers: $refusal[0].peers,
                    exit_code: ($refused_exit | tonumber? // $refused_exit), run_ms: $refused_ms},
          survivors: {raft_term_and_leader: {stopped: $stopped, refused: $refused},
                      refusals_reported: $survivors[0].refusals},
          restored: {settled_recovery_ms: $settled_ms},
          node_events: "prerequisite-refusal/node-events.ndjson",
          findings: "prerequisite-refusal/findings.ndjson"}' \
        >"${artifact_dir}/results/prerequisite-refusal-progress.json"

    recovery_final_boundary
}

run_node_settings() {
    case "${scenario}" in
        capability-roll) run_capability_roll ;;
        parameter-return) run_parameter_return ;;
        parameter-live) run_parameter_live ;;
        prerequisite-refusal) run_prerequisite_refusal ;;
        *) node_settings_fail controller "no node-settings scenario is named ${scenario}" ;;
    esac
}
