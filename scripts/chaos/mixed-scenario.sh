#!/usr/bin/env bash
# Sourced by run-baseline.sh after the external cluster and graph are ready. Runs the validated
# mixed-instability plan in mixed/plan.json step by step. Each step resolves its logical node
# references from settled public observations, refuses any fault that would break the plan's quorum
# policy given the current Docker process state and installed link faults, injects and verifies
# each fault, judges the availability the policy promises while the faults hold, heals them and
# measures recovery. The action trace in mixed/actions.ndjson records what each action intended and
# what it met, a sampler records the broker boundaries and every node's Docker state and memory
# throughout, and the final verdicts judge trace completeness, coverage, resources and the ledger.

# shellcheck source=recovery-scenario.sh
source "${script_dir}/recovery-scenario.sh"
# shellcheck source=node-faults.sh
source "${script_dir}/node-faults.sh"
# shellcheck source=link-degradation.sh
source "${script_dir}/link-degradation.sh"

mixed_dir="${artifact_dir}/mixed"
mixed_plan="${mixed_dir}/plan.json"
mixed_trace="${mixed_dir}/actions.ndjson"
mixed_samples="${mixed_dir}/samples.ndjson"
mixed_sampler_stop="${mixed_dir}/.stop-sampler"
mixed_sampler_pid=""
mixed_started_ms=""
mixed_started_ns=""

# External budgets, in seconds. They bound how long the runner waits for each published behavior
# and stay apart from the product's own liveness settings.
mixed_pre_step_bound=150
mixed_role_bound=90
mixed_majority_bound=90
mixed_canary_bound=30
mixed_degraded_canary_bound=60
mixed_unavailable_canary_bound=20
mixed_listener_bound=120
mixed_settle_bound=150
mixed_delivery_bound=90
mixed_drain_bound=120
mixed_sample_interval=10
# A degradation injector heals itself after this long even if nothing stops it.
mixed_degradation_lifetime=900

# The running step's logical node references, resolved to hosts.
declare -A mixed_ref_host=()
# Per action of the run, keyed by action id.
declare -A mixed_action_dir=()
declare -A mixed_action_hosts=()
declare -A mixed_action_impaired=()
declare -A mixed_action_verified_ms=()
declare -A mixed_action_since_ns=()
declare -A mixed_action_lossy_link=()
declare -A mixed_pause_pid=()
# The ids of the running step's faults that currently hold.
mixed_active=()

mixed_sleep_until() {
    local due_ms="$1"
    local remaining_ms=$((due_ms - $(epoch_ms)))
    local limit_ms=$(($(remaining_seconds) * 1000))
    if ((remaining_ms > limit_ms)); then
        remaining_ms="${limit_ms}"
    fi
    if ((remaining_ms > 0)); then
        sleep "$((remaining_ms / 1000)).$(printf '%03d' $((remaining_ms % 1000)))"
    fi
}

# Appends one record to the action trace: the event, the step, the time and every named field.
mixed_trace_record() {
    local event="$1"
    local step="$2"
    shift 2
    jq -nc --arg event "${event}" --argjson step "${step}" \
        --arg at "$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)" --argjson at_ms "$(epoch_ms)" \
        "$@" '$ARGS.named' >>"${mixed_trace}"
}

# Appends one sample of the broker boundaries and of every node's Docker state, memory and CPU to
# the samples ledger. A boundary or state the sample could not read is null.
mixed_take_sample() {
    local at_ms
    at_ms="$(epoch_ms)"
    local offsets=""
    offsets="$(compose_call_timeout=30 broker_admin /opt/kafka/bin/kafka-get-offsets.sh \
        --bootstrap-server broker:9092 --topic '^chaos_(input|output)$' 2>/dev/null)" || offsets=""
    local source_end output_end
    source_end="$(awk -F: '$1 == "chaos_input" && $2 == "0" { print $3 }' <<<"${offsets}")"
    output_end="$(awk -F: '$1 == "chaos_output" && $2 == "0" { print $3 }' <<<"${offsets}")"
    local states=""
    states="$(run_bounded 20 docker inspect \
        --format '{{json .Name}} {{json .State.Status}}' "$@" 2>/dev/null)" || states=""
    local stats=""
    stats="$(run_bounded 30 docker stats --no-stream --format '{{json .}}' "$@" 2>/dev/null)" || stats=""
    jq -nc \
        --argjson at_ms "${at_ms}" \
        --arg source_end "${source_end}" \
        --arg output_end "${output_end}" \
        --arg states "${states}" \
        --arg stats "${stats}" '
        def bytes:
          (split(" / ")[0] | capture("^(?<n>[0-9.]+)(?<u>B|kB|MB|GB|KiB|MiB|GiB)$")) as $v
          | ($v.n | tonumber) * ({B: 1, kB: 1000, MB: 1000000, GB: 1000000000,
                                 KiB: 1024, MiB: 1048576, GiB: 1073741824}[$v.u]);
        def lines: split("\n") | map(select(length > 0));
        ($source_end | tonumber? // null) as $source
        | ($output_end | tonumber? // null) as $output
        | ($states | lines | map(capture("^\"/(?<name>[^\"]+)\" \"(?<status>[^\"]+)\"$"))
           | map({key: .name, value: .status}) | from_entries) as $status
        | ($stats | lines | map(fromjson? // empty)
           | map({key: .Name, value: {memory_bytes: (.MemUsage | bytes? // null),
                                      cpu_percent: (.CPUPerc | rtrimstr("%") | tonumber? // null)}})
           | from_entries) as $usage
        | {at_ms: $at_ms, source_end: $source, output_end: $output,
           backlog: (if $source != null and $output != null then ([$source - $output, 0] | max) else null end),
           nodes: ([$status | keys[], ($usage | keys[])] | unique
                   | map({container: ., status: $status[.], memory_bytes: $usage[.].memory_bytes,
                          cpu_percent: $usage[.].cpu_percent}))}
    ' >>"${mixed_samples}"
}

# Samples every interval until the stop file appears. Runs in the background for the whole fault
# phase, so traffic and resources stay under observation between and during steps.
mixed_sampler() {
    local node_ids=()
    local host
    for host in "${node_hosts[@]}"; do
        node_ids+=("$(owned_service_container "${host}")")
    done
    while [[ ! -e "${mixed_sampler_stop}" ]]; do
        mixed_take_sample "${node_ids[@]}" || true
        local waited=0
        while ((waited < mixed_sample_interval)) && [[ ! -e "${mixed_sampler_stop}" ]]; do
            sleep 1
            waited=$((waited + 1))
        done
    done
}

mixed_start_sampler() {
    rm -f "${mixed_sampler_stop}"
    mixed_sampler &
    mixed_sampler_pid=$!
}

# Asks the sampler to stop and waits for the sample it is taking, so no sample container starts
# after the run's resources are captured or removed.
mixed_stop_sampler() {
    [[ -n "${mixed_sampler_pid}" ]] || return 0
    : >"${mixed_sampler_stop}"
    local waited=0
    while kill -0 "${mixed_sampler_pid}" 2>/dev/null && ((waited < 90)); do
        sleep 1
        waited=$((waited + 1))
    done
    kill "${mixed_sampler_pid}" 2>/dev/null || true
    wait "${mixed_sampler_pid}" 2>/dev/null || true
    mixed_sampler_pid=""
}

# Judges the action trace and summarizes the resource samples of a run that ended inside its fault
# phase, so its artifacts hold both like those of a run that reached its final verdicts. Run by the
# exit trap after the sampler stops; the trace verdict of such a run is incomplete.
mixed_summarize_interrupted() {
    [[ -s "${mixed_trace}" && ! -e "${artifact_dir}/results/mixed-trace.json" ]] || return 0
    # The run already failed, so each verdict is recorded beside its output rather than returned.
    "${script_dir}/verify-mixed-evidence.sh" trace --plan "${mixed_plan}" --trace "${mixed_trace}" \
        --output "${artifact_dir}/results/mixed-trace.json" >"${mixed_dir}/trace-verdict.txt" 2>&1 || true
    [[ -s "${mixed_samples}" ]] || return 0
    "${script_dir}/verify-mixed-evidence.sh" resources --samples "${mixed_samples}" \
        --max-memory-bytes "${max_memory_bytes}" --output "${artifact_dir}/results/mixed-resources.json" \
        >"${mixed_dir}/resources-verdict.txt" 2>&1 || true
}

# Ends every pause injector the controller still waits for. Run by the exit trap, which then
# unpauses every node.
mixed_stop_pause_injectors() {
    local id
    for id in "${!mixed_pause_pid[@]}"; do
        kill "${mixed_pause_pid[${id}]}" 2>/dev/null || true
        wait "${mixed_pause_pid[${id}]}" 2>/dev/null || true
        unset 'mixed_pause_pid[$id]'
    done
}

# Reads each node's own status and the owners of the baseline entities through that node, with one
# administration container per node side by side. True when every node is settled, connected and
# free of warnings and all of them agree on leader, term, log position and owners; the observation
# is written to OUTPUT_DIR.json.
mixed_observe() {
    local output_dir="$1"
    rm -rf "${output_dir}"
    mkdir -p "${output_dir}"
    local host
    local pids=()
    for host in "${node_hosts[@]}"; do
        admin_cli_batch "${host}" chaos_baseline "${output_dir}/${host}" 60 \
            status 'SHOW CLUSTER STATUS;' \
            ingestor 'DESCRIBE INGESTOR chaos_ingestor;' \
            relay 'DESCRIBE RELAY chaos_records;' \
            emitter 'DESCRIBE EMITTER chaos_emitter;' >/dev/null 2>&1 &
        pids+=("$!")
    done
    local pid
    local answered=true
    for pid in "${pids[@]}"; do
        wait "${pid}" || answered=false
    done
    [[ "${answered}" == true ]] || return 1
    local records=()
    for host in "${node_hosts[@]}"; do
        local dir="${output_dir}/${host}"
        local label
        for label in status ingestor relay emitter; do
            batch_read_succeeded "${dir}" "${label}" || return 1
        done
        status_is_settled "${host}" "${dir}/status.txt" || return 1
        [[ "$(status_warnings "${dir}/status.txt")" == none ]] || return 1
        local peer
        for peer in "${node_hosts[@]}"; do
            [[ "${peer}" == "${host}" ]] && continue
            grep -Eq "^- node-${peer##*-}: .* status=connected$" "${dir}/status.txt" || return 1
        done
        grep -Fxq 'status: running' "${dir}/ingestor.txt" || return 1
        grep -Fxq 'ready: true' "${dir}/ingestor.txt" || return 1
        grep -Fxq 'status: OK' "${dir}/emitter.txt" || return 1
        records+=("$(jq -nc \
            --arg host "${host}" \
            --arg node "node-${host##*-}" \
            --arg leader "$(status_leader "${dir}/status.txt")" \
            --arg term "$(status_value "${dir}/status.txt" raft.current_term)" \
            --arg last_log "$(status_value "${dir}/status.txt" raft.last_log_index)" \
            --arg ingestor "$(owner_from_description "${dir}/ingestor.txt")" \
            --arg relay "$(owner_from_description "${dir}/relay.txt")" \
            --arg emitter "$(owner_from_description "${dir}/emitter.txt")" \
            '{host: $host, node: $node, leader: $leader, term: $term, last_log_index: $last_log,
              owners: {ingestor: $ingestor, relay: $relay, emitter: $emitter}}')")
    done
    printf '%s\n' "${records[@]}" | jq -s \
        --arg observed_at "$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)" '
        {observed_at: $observed_at, leader: .[0].leader, term: .[0].term,
         last_log_index: .[0].last_log_index, owners: .[0].owners, nodes: .,
         agreed: (map({leader, term, last_log_index, owners}) | unique | length == 1)}' \
        >"${output_dir}.json"
    jq -e '.agreed and (.leader | test("^node-[1-3]$")) and (.owners | all(.[]; test("^node-[1-3]$")))' \
        "${output_dir}.json" >/dev/null
}

# The state a step's policy is judged from: every node container running and not paused, no
# run-owned fault injector, every node interface in its default state, and a settled public view.
mixed_pre_step_ready() {
    local output_dir="$1"
    mkdir -p "${output_dir}"
    local host
    for host in "${node_hosts[@]}"; do
        local container_id state
        container_id="$(owned_service_container "${host}")" || return 1
        state="$(run_bounded 20 docker inspect --format '{{.State.Running}} {{.State.Paused}}' "${container_id}")" \
            || return 1
        [[ "${state}" == 'true false' ]] || return 1
    done
    local injectors
    injectors="$(run_bounded 20 docker container ls --quiet \
        --filter "label=io.nervix.chaos.run=${run_id}" --filter label=io.nervix.chaos.role=fault)" || return 1
    [[ -z "${injectors}" ]] || return 1
    plan_topology "${output_dir}/topology.json"
    rm -rf "${output_dir}/rules"
    mkdir -p "${output_dir}/rules"
    rules_match_plan "${output_dir}/topology.json" "${output_dir}/rules" --healed || return 1
    mixed_observe "${output_dir}/observation"
}

# Resolves a step's logical node references from one settled OBSERVATION: the target holds ROLE,
# choosing the PICK-th follower in node order for a follower; the peer is the leader when the target
# is not, and otherwise the lowest-numbered follower; the other node is the one left. Each
# reference records the roles its node held. A cluster step names every node.
mixed_resolve_refs() {
    local observation="$1"
    local role="$2"
    local pick="$3"
    local output="$4"
    jq --arg role "${role}" --argjson pick "${pick}" '
        def held($node; $leader; $owners):
          [if $node == $leader then "leader" else "follower" end]
          + [$owners | to_entries[] | select(.value == $node) | "\(.key)-owner"];
        def described($node; $leader; $owners):
          {node: $node, host: ("nervix-" + ($node | ltrimstr("node-"))), roles: held($node; $leader; $owners)};
        ([.nodes[].node] | sort) as $nodes
        | .leader as $leader
        | .owners as $owners
        | ($nodes - [$leader]) as $followers
        | (if $role == "leader" then $leader
           elif $role == "follower" then $followers[$pick % ($followers | length)]
           elif $role == "ingestor-owner" then $owners.ingestor
           elif $role == "relay-owner" then $owners.relay
           elif $role == "emitter-owner" then $owners.emitter
           else null end) as $target
        | {role: $role, pick: $pick, leader: $leader, owners: $owners,
           refs: (if $role == "cluster" then
                    {cluster: [$nodes[] | described(.; $leader; $owners)]}
                  else
                    (if $target != $leader then $leader else $followers[0] end) as $peer
                    | {target: described($target; $leader; $owners),
                       peer: described($peer; $leader; $owners),
                       other: described(($nodes - [$target, $peer])[0]; $leader; $owners)}
                  end)}
    ' "${observation}" >"${output}"
}

# Selects the step's nodes from two settled observations in a row that resolve them identically, so
# the roles are settled immediately before the first fault, and records them in STEP_DIR/nodes.json.
mixed_select_nodes() {
    local step_dir="$1"
    local role="$2"
    local pick="$3"
    local ordinal="$4"
    local selection_dir="${step_dir}/selection"
    mkdir -p "${selection_dir}"
    mixed_resolve_refs "${step_dir}/before/observation.json" "${role}" "${pick}" "${selection_dir}/0.json"
    local attempt=0
    local deadline=$((SECONDS + mixed_role_bound))
    while ((SECONDS < deadline && SECONDS < overall_deadline)); do
        attempt=$((attempt + 1))
        if mixed_observe "${selection_dir}/observation-${attempt}"; then
            mixed_resolve_refs "${selection_dir}/observation-${attempt}.json" "${role}" "${pick}" \
                "${selection_dir}/${attempt}.json"
            if jq -e --slurpfile previous "${selection_dir}/$((attempt - 1)).json" \
                '.refs == $previous[0].refs' "${selection_dir}/${attempt}.json" >/dev/null; then
                cp "${selection_dir}/${attempt}.json" "${step_dir}/nodes.json"
                mixed_ref_host=()
                local ref host
                while IFS=$'\t' read -r ref host; do
                    mixed_ref_host["${ref}"]="${host}"
                done < <(jq -r '.refs | to_entries[] | select(.key != "cluster") | "\(.key)\t\(.value.host)"' \
                    "${step_dir}/nodes.json")
                return 0
            fi
        else
            # A read that is not settled cannot confirm the previous one.
            cp "${selection_dir}/$((attempt - 1)).json" "${selection_dir}/${attempt}.json"
            jq '.refs = null' "${selection_dir}/${attempt}.json" >"${selection_dir}/${attempt}.unsettled.json"
            mv "${selection_dir}/${attempt}.unsettled.json" "${selection_dir}/${attempt}.json"
        fi
        sleep 1
    done
    recovery_fail injection "the roles of step ${ordinal} did not settle within ${mixed_role_bound}s immediately before its faults"
}

# The hosts an action names, one per line: every node for a cluster action.
mixed_hosts_of() {
    local node="$1"
    if [[ "${node}" == cluster ]]; then
        printf '%s\n' "${node_hosts[@]}"
    else
        printf '%s\n' "${mixed_ref_host[${node}]}"
    fi
}

# The hosts the step's verified faults currently leave out of a quorum, one per line.
mixed_impaired_hosts() {
    local id
    for id in ${mixed_active[@]+"${mixed_active[@]}"}; do
        [[ -n "${mixed_action_impaired[${id}]}" ]] || continue
        # The impaired set is a space-separated host list.
        # shellcheck disable=SC2086
        printf '%s\n' ${mixed_action_impaired[${id}]}
    done | sort -u
}

# Writes every node's Docker process state as one JSON object of container name to status.
mixed_docker_states() {
    local ids=()
    local host
    for host in "${node_hosts[@]}"; do
        ids+=("$(owned_service_container "${host}")")
    done
    run_bounded 20 docker inspect --format '{{json .Name}} {{json .State.Status}}' "${ids[@]}" \
        | jq -sRc 'split("\n") | map(select(length > 0) | capture("^\"/(?<name>[^\"]+)\" \"(?<status>[^\"]+)\"$"))
                   | map({key: .name, value: .status}) | from_entries'
}

# Refuses an action before it alters a container when the nodes Docker reports stopped or paused,
# the step's installed link faults and the action itself would leave more voters out of a quorum
# than the plan's policy allows. The judgment is kept in ACTION_DIR/policy.json.
mixed_policy_check() {
    local action_dir="$1"
    local ordinal="$2"
    local id="$3"
    local policy
    policy="$(jq -r '.policy' "${mixed_plan}")"
    local states
    states="$(mixed_docker_states)" || recovery_fail controller "the node containers could not be inspected before ${id}"
    local impaired=()
    mapfile -t impaired < <(
        {
            mixed_impaired_hosts
            jq -r --arg project "${project_name}" 'to_entries[] | select(.value != "running")
                | .key | ltrimstr($project + "-") | rtrimstr("-1")' <<<"${states}"
            # The impaired set is a space-separated host list.
            # shellcheck disable=SC2086
            printf '%s\n' ${mixed_action_impaired[${id}]}
        } | sed '/^$/d' | sort -u
    )
    local verdict=permitted
    if [[ "${policy}" == preserve-quorum ]] && ((${#impaired[@]} >= 2)); then
        verdict=refused
    fi
    jq -n \
        --arg policy "${policy}" \
        --arg action "${id}" \
        --arg verdict "${verdict}" \
        --argjson docker "${states}" \
        --argjson active "$(printf '%s\n' ${mixed_active[@]+"${mixed_active[@]}"} | jq -R . | jq -sc 'map(select(length > 0))')" \
        --argjson impaired "$(printf '%s\n' ${impaired[@]+"${impaired[@]}"} | jq -R . | jq -sc 'map(select(length > 0))')" \
        '{policy: $policy, action: $action, docker_states: $docker, active_faults: $active,
          voters_out_of_quorum_after: $impaired, verdict: $verdict}' >"${action_dir}/policy.json"
    if [[ "${verdict}" == refused ]]; then
        mixed_trace_record action-refused "${ordinal}" --arg action "${id}" \
            --arg reason "${#impaired[@]} of 3 voters would be out of a quorum, which preserve-quorum refuses"
        recovery_fail injection "refused ${id}: it would leave ${#impaired[@]} of 3 voters out of a quorum, which preserve-quorum refuses"
    fi
}

# Waits until HOST serves again. A node restarted inside its own isolation cannot rejoin, so it is
# waited for through its own status route, which answers without a quorum; any other node must restore
# every listener.
mixed_wait_restarted() {
    local action_dir="$1"
    local host="$2"
    local isolated="$3"
    if [[ "${isolated}" == true ]]; then
        local attempt
        for attempt in $(seq 1 60); do
            if partition_node_status "${host}" "${action_dir}/restarted-status-${host}.txt"; then
                cli_host="${node_hosts[0]}"
                return 0
            fi
            sleep 2
        done
        cli_host="${node_hosts[0]}"
        recovery_fail product "${host} did not answer its own status route within 120s of its restart inside its isolation"
    fi
    wait_for "${host} listeners restored" "${mixed_listener_bound}" probe_node "${host}" \
        || recovery_fail product "${host} listeners did not return within ${mixed_listener_bound}s of its restart"
}

# Installs one partition with the partition scenario's verified plans and records the applied
# boundary of the nodes it cuts off from every quorum.
mixed_partition_inject() {
    local action_dir="$1"
    local id="$2"
    local partition="$3"
    local host="$4"
    local to_host="$5"
    local cut_off=()
    local term_frozen=false
    case "${partition}" in
        isolate)
            plan_isolation isolation "${host}" "${action_dir}/plan.json"
            cut_off=("${host}")
            ;;
        one-way)
            plan_one_way "${host}" "${to_host}" "${action_dir}/plan.json"
            ;;
        quorum-loss)
            plan_quorum_loss "${action_dir}/plan.json"
            cut_off=("${node_hosts[@]}")
            term_frozen=true
            ;;
    esac
    : >"${action_dir}/samples.ndjson"
    install_partition "${action_dir}" "${id}"
    failure_category=product
    sample_statuses "${action_dir}" isolated "${node_hosts[@]}"
    record_isolation_boundary "${action_dir}" "${term_frozen}" ${cut_off[@]+"${cut_off[@]}"}
    cli_host="${node_hosts[0]}"
}

# A node cut off from every quorum can neither apply an entry committed after the fault nor, when no
# node can reach a quorum, advance its term. Samples the cut-off nodes still running and records a
# finding for any violation.
mixed_check_cut_off() {
    local action_dir="$1"
    local label="$2"
    local hosts=()
    mapfile -t hosts < <(jq -r '.isolated[]' "${action_dir}/isolation-boundary.json")
    ((${#hosts[@]} > 0)) || return 0
    local running=()
    local host
    for host in "${hosts[@]}"; do
        local state
        state="$(run_bounded 20 docker inspect --format '{{.State.Running}} {{.State.Paused}}' \
            "$(owned_service_container "${host}")")" || state=""
        [[ "${state}" == 'true false' ]] && running+=("${host}")
    done
    ((${#running[@]} > 0)) || return 0
    sample_statuses "${action_dir}" "${label}" "${running[@]}"
    cli_host="${node_hosts[0]}"
    if ! jq -s -e --arg label "${label}" --slurpfile boundary "${action_dir}/isolation-boundary.json" '
        $boundary[0] as $b
        | [.[] | . as $sample | select($sample.sample == $label and ($b.isolated | index($sample.host)) != null)]
        | all(.[]; . as $sample
                   | $sample.last_applied <= $b.applied_boundary
                     and (if $b.term_frozen then $sample.term <= $b.terms[$sample.host] else true end))
    ' "${action_dir}/samples.ndjson" >/dev/null; then
        recovery_finding "${mixed_dir}" \
            "a node cut off from every quorum applied an entry or advanced its term during $(basename "${action_dir}")"
    fi
}

# Reruns the link checks of an isolation that holds after its isolated node restarted inside it: its
# peers' rules and the measured link matrix must still match the plan.
mixed_isolation_survives() {
    local action_dir="$1"
    local label="$2"
    failure_category=injection
    mkdir -p "${action_dir}/${label}/rules"
    rules_match_plan "${action_dir}/plan.json" "${action_dir}/${label}/rules" \
        || recovery_fail injection "peer-side rules changed across the restart inside $(basename "${action_dir}")"
    probe_links "${action_dir}/${label}/links.json"
    "${script_dir}/verify-partition-evidence.sh" links "${action_dir}/plan.json" \
        "${action_dir}/${label}/links.json" "${action_dir}/${label}/links-verdict.json" \
        || recovery_fail injection "the isolation did not survive the restart inside $(basename "${action_dir}")"
    failure_category=product
}

# Degrades the directed link from SENDER to RECEIVER with PROFILE, and proves the profile's effect
# against a baseline of the healthy link measured just before.
mixed_degrade_inject() {
    local action_dir="$1"
    local id="$2"
    local sender="$3"
    local receiver="$4"
    local profile="$5"
    failure_category=injection
    link_ping "${action_dir}/ping-baseline.txt" "${sender}" "${receiver}"
    local effect_evidence=("${action_dir}/ping-baseline.json" "${action_dir}/ping-fault.json")
    if [[ "${profile}" == rate-limit || "${profile}" == combined ]]; then
        link_rate_probe "${action_dir}/rate-baseline.json" "${sender}" "${receiver}"
        effect_evidence+=("${action_dir}/rate-baseline.json" "${action_dir}/rate-fault.json")
    fi
    link_degradation_start "${action_dir}" "${sender}" "${receiver}" "${profile}" \
        "${project_name}-degrade-${id}" "${mixed_degradation_lifetime}"
    link_degradation_verify "${action_dir}" "${sender}" "${receiver}" "${profile}"
    link_ping "${action_dir}/ping-fault.txt" "${sender}" "${receiver}"
    if [[ "${profile}" == rate-limit || "${profile}" == combined ]]; then
        link_rate_probe "${action_dir}/rate-fault.json" "${sender}" "${receiver}"
    fi
    "${script_dir}/verify-degraded-evidence.sh" effect "${profile}" "${effect_evidence[@]}" \
        >"${action_dir}/effect-verdict.txt" 2>&1 \
        || recovery_fail injection "${profile} of ${id} did not measurably affect the link from ${sender} to ${receiver}"
    failure_category=product
}

# Prints a JSON array describing HOSTS through the step's resolved references: node, host, container
# and the roles each held when the step selected it.
mixed_describe_hosts() {
    local ids=()
    local host
    for host in "$@"; do
        ids+=("$(owned_service_container "${host}")")
    done
    jq -c --argjson hosts "$(printf '%s\n' "$@" | jq -R . | jq -sc .)" \
        --argjson ids "$(printf '%s\n' "${ids[@]}" | jq -R . | jq -sc .)" '
        [.refs | to_entries[] | .value | if type == "array" then .[] else . end] as $described
        | [range(0; $hosts | length) as $index
           | $hosts[$index] as $host
           | (first($described[] | select(.host == $host)) // {node: null, roles: []})
           | {host: $host, node, container_id: $ids[$index], roles}]
    ' "${mixed_step_dir}/nodes.json"
}

# Injects one planned action after the policy check, verifies it and records it in the trace.
mixed_inject() {
    local ordinal="$1"
    local action="$2"
    local id family node to partition profile hold
    id="$(jq -r '.id' <<<"${action}")"
    family="$(jq -r '.family' <<<"${action}")"
    node="$(jq -r '.node' <<<"${action}")"
    to="$(jq -r '.to // empty' <<<"${action}")"
    partition="$(jq -r '.partition // empty' <<<"${action}")"
    profile="$(jq -r '.profile // empty' <<<"${action}")"
    hold="$(jq -r '.hold_seconds' <<<"${action}")"
    local action_dir="${mixed_step_dir}/${id}-${family}${partition:+-${partition}}"
    mkdir -p "${action_dir}"
    mixed_action_dir["${id}"]="${action_dir}"
    local hosts=()
    mapfile -t hosts < <(mixed_hosts_of "${node}")
    mixed_action_hosts["${id}"]="${hosts[*]}"
    local to_host=""
    if [[ -n "${to}" ]]; then
        to_host="${mixed_ref_host[${to}]}"
    fi
    # The voters this fault leaves out of the leader's quorum. One-way loss takes out the endpoint that
    # is not the leader: a receiver the leader can no longer reach, or a sender whose acknowledgements
    # the leader no longer hears. Either way it is one voter, as the plan validator counts it.
    case "${family}:${partition}" in
        degrade:) mixed_action_impaired["${id}"]="" ;;
        partition:one-way)
            if jq -e --arg host "${to_host}" 'first(.refs[] | select(.host == $host)).roles | index("leader")' \
                "${mixed_step_dir}/nodes.json" >/dev/null; then
                mixed_action_impaired["${id}"]="${hosts[0]}"
            else
                mixed_action_impaired["${id}"]="${to_host}"
            fi
            ;;
        *) mixed_action_impaired["${id}"]="${hosts[*]}" ;;
    esac
    mixed_policy_check "${action_dir}" "${ordinal}" "${id}"
    local targets link_to=null
    targets="$(mixed_describe_hosts "${hosts[@]}")"
    if [[ -n "${to_host}" ]]; then
        link_to="$(mixed_describe_hosts "${to_host}" | jq -c '.[0]')"
    fi
    mixed_trace_record action-started "${ordinal}" --arg action "${id}" --arg family "${family}" \
        --argjson intended "${action}" --argjson targets "${targets}" --argjson link_to "${link_to}" \
        --argjson conditions "$(jq -c 'del(.action)' "${action_dir}/policy.json")"
    case "${family}" in
        kill)
            node_fault_kill_inject "${action_dir}" "${hold}" "" "${hosts[@]}"
            mixed_action_since_ns["${id}"]="${node_fault_since_ns}"
            ;;
        stop)
            local others
            others="$(mixed_impaired_hosts | grep -Fvx "${hosts[0]}" || true)"
            local replacements_live=true
            [[ -z "${others}" ]] || replacements_live=false
            node_fault_stop_inject "${action_dir}" "${hosts[0]}" "${hold}" "" "${replacements_live}"
            mixed_action_since_ns["${id}"]="${node_fault_since_ns}"
            ;;
        pause)
            node_fault_pause_inject "${action_dir}" "${hosts[0]}" "${hold}" ""
            mixed_action_since_ns["${id}"]="${node_fault_since_ns}"
            mixed_pause_pid["${id}"]="${node_fault_pause_pid}"
            ;;
        partition)
            mixed_partition_inject "${action_dir}" "${id}" "${partition}" "${hosts[0]}" "${to_host}"
            ;;
        degrade)
            mixed_degrade_inject "${action_dir}" "${id}" "${hosts[0]}" "${to_host}" "${profile}"
            if [[ "${profile}" == random-loss || "${profile}" == burst-loss || "${profile}" == combined ]]; then
                mixed_action_lossy_link["${id}"]="${hosts[0]} ${to_host}"
            fi
            ;;
    esac
    mixed_action_verified_ms["${id}"]="$(epoch_ms)"
    mixed_active+=("${id}")
    mixed_trace_record action-verified "${ordinal}" --arg action "${id}"
    local impaired=()
    mapfile -t impaired < <(mixed_impaired_hosts)
    if ((${#impaired[@]} >= 2)); then
        mixed_trace_record quorum-lost "${ordinal}" --arg action "${id}" \
            --argjson impaired "$(printf '%s\n' "${impaired[@]}" | jq -R . | jq -sc .)"
    fi
}

# Heals one action, verifies the heal and records it in the trace.
mixed_heal() {
    local ordinal="$1"
    local action="$2"
    local id family partition hold
    id="$(jq -r '.id' <<<"${action}")"
    family="$(jq -r '.family' <<<"${action}")"
    partition="$(jq -r '.partition // empty' <<<"${action}")"
    hold="$(jq -r '.hold_seconds' <<<"${action}")"
    local action_dir="${mixed_action_dir[${id}]}"
    local hosts=()
    read -r -a hosts <<<"${mixed_action_hosts[${id}]}"
    local heal_requested_ms
    heal_requested_ms="$(epoch_ms)"
    # A node fault that heals inside an isolation of the same node restarts it still isolated.
    local isolated=false
    local active_id
    for active_id in "${mixed_active[@]}"; do
        if [[ "${active_id}" != "${id}" && "${mixed_action_dir[${active_id}]}" == *-partition-isolate \
            && " ${mixed_action_impaired[${active_id}]} " == *" ${hosts[0]} "* ]]; then
            isolated=true
        fi
    done
    case "${family}" in
        kill)
            node_fault_kill_restart "${action_dir}" "${hosts[@]}"
            local host
            for host in "${hosts[@]}"; do
                mixed_wait_restarted "${action_dir}" "${host}" "${isolated}"
            done
            ;;
        stop)
            node_fault_stop_restart "${action_dir}" "${hosts[0]}"
            mixed_wait_restarted "${action_dir}" "${hosts[0]}" "${isolated}"
            ;;
        pause)
            node_fault_pause_resume "${action_dir}" "${hosts[0]}" "${mixed_pause_pid[${id}]}" \
                "$((hold * 1000 - 200))" "$((hold * 1000 + 5000))"
            unset 'mixed_pause_pid[$id]'
            ;;
        partition)
            mixed_check_cut_off "${action_dir}" held
            heal_partition "${action_dir}" "${id}"
            failure_category=product
            ;;
        degrade)
            local to_host
            to_host="$(jq -r --arg ref "$(jq -r '.to' <<<"${action}")" '.refs[$ref].host' "${mixed_step_dir}/nodes.json")"
            failure_category=injection
            link_degradation_heal "${action_dir}" "${hosts[0]}" "${to_host}" "$(<"${action_dir}/injector-id.txt")" \
                "${action_dir}/ping-baseline.json"
            failure_category=product
            ;;
    esac
    local remaining=()
    for active_id in "${mixed_active[@]}"; do
        [[ "${active_id}" == "${id}" ]] || remaining+=("${active_id}")
    done
    mixed_active=(${remaining[@]+"${remaining[@]}"})
    # A node restarted inside an isolation that still holds must come back isolated.
    if [[ "${family}" == kill || "${family}" == stop ]]; then
        for active_id in ${mixed_active[@]+"${mixed_active[@]}"}; do
            if [[ -f "${mixed_action_dir[${active_id}]}/isolation-boundary.json" ]] \
                && [[ " ${mixed_action_impaired[${active_id}]} " == *" ${hosts[0]} "* ]]; then
                mixed_isolation_survives "${mixed_action_dir[${active_id}]}" "after-${id}"
            fi
        done
    fi
    mixed_trace_record action-healed "${ordinal}" --arg action "${id}" \
        --argjson held_ms "$((heal_requested_ms - ${mixed_action_verified_ms[${id}]}))"
}

# Judges the availability the plan's policy promises while the step's faults hold. A quorum that can
# still communicate must agree on a caught-up leader and acknowledge a control canary; with no quorum
# able to communicate, no running node may acknowledge one. Random or burst loss on the one link left
# between the remaining quorum can stall commits until it heals, so availability is then recorded but
# not required. A pause bounds the checks by its end.
mixed_hold_checks() {
    local ordinal="$1"
    local label="$2"
    local checks_dir="${mixed_step_dir}/hold-${label}"
    mkdir -p "${checks_dir}"
    local deadline_ms=0
    local id
    for id in "${mixed_active[@]}"; do
        if [[ "${mixed_action_dir[${id}]}" == *-pause ]]; then
            local hold end_ms
            hold="$(jq -r '.intended_pause_seconds' "${mixed_action_dir[${id}]}/fault-command.json")"
            end_ms=$((${mixed_action_since_ns[${id}]} / 1000000 + hold * 1000 - 2000))
            if ((deadline_ms == 0 || end_ms < deadline_ms)); then
                deadline_ms="${end_ms}"
            fi
        fi
    done
    local window_ms=$((mixed_majority_bound * 1000))
    if ((deadline_ms > 0)); then
        window_ms=$((deadline_ms - $(epoch_ms)))
    fi
    local impaired=()
    mapfile -t impaired < <(mixed_impaired_hosts)
    local degraded=false
    local lossy_quorum_link=false
    for id in "${mixed_active[@]}"; do
        if [[ "${mixed_action_dir[${id}]}" == *-degrade ]]; then
            degraded=true
        fi
        if [[ -n "${mixed_action_lossy_link[${id}]:-}" ]] && ((${#impaired[@]} == 1)); then
            local endpoint
            local survivors_link=true
            for endpoint in ${mixed_action_lossy_link[${id}]}; do
                [[ "${endpoint}" != "${impaired[0]}" ]] || survivors_link=false
            done
            if [[ "${survivors_link}" == true ]]; then
                lossy_quorum_link=true
            fi
        fi
    done
    local verdict='observed'
    local canaries=()
    if ((window_ms < 5000)); then
        # A short pause ends before any public observation could judge it.
        verdict='too-short-to-observe'
    elif ((${#impaired[@]} <= 1)); then
        local excluded="${impaired[0]:-}"
        local seconds=$(((window_ms + 999) / 1000))
        if ((seconds > mixed_majority_bound)); then
            seconds="${mixed_majority_bound}"
        fi
        if ! wait_for "a connected quorum agreeing on a caught-up leader during step ${ordinal}" "${seconds}" \
            survivors_settled "${excluded}" "${checks_dir}"; then
            if ((deadline_ms > 0)); then
                verdict='pause-ended-first'
            elif [[ "${lossy_quorum_link}" == true ]]; then
                verdict='no-leader-over-lossy-quorum-link'
            else
                verdict='no-majority-leader'
                recovery_finding "${mixed_dir}" \
                    "the nodes outside step ${ordinal}'s faults did not agree on a caught-up leader within ${seconds}s"
            fi
        else
            local leader leader_host bound="${mixed_canary_bound}"
            leader="$(<"${checks_dir}/survivor-leader.txt")"
            leader_host="nervix-${leader##*-}"
            if [[ "${degraded}" == true ]]; then
                bound="${mixed_degraded_canary_bound}"
            fi
            if ((deadline_ms > 0)); then
                local left=$(((deadline_ms - $(epoch_ms)) / 1000))
                if ((left < bound)); then
                    bound="${left}"
                fi
            fi
            if ((bound >= 5)); then
                local name="chaos_mixed_${ordinal}_${label}_majority"
                partition_canary "${name}" "${leader_host}" "${checks_dir}" "${bound}" during false
                canaries+=("${name}")
                if [[ "$(canary_outcome "${checks_dir}" "${name}")" != acknowledged ]]; then
                    if [[ "${lossy_quorum_link}" == true ]]; then
                        verdict='unacknowledged-over-lossy-quorum-link'
                    else
                        verdict='majority-did-not-acknowledge'
                        recovery_finding "${mixed_dir}" \
                            "a quorum that could communicate did not acknowledge a control command through ${leader_host} within ${bound}s during step ${ordinal}"
                    fi
                fi
            else
                verdict='pause-ended-first'
            fi
        fi
    else
        local states
        states="$(mixed_docker_states)"
        local running=()
        mapfile -t running < <(jq -r --arg project "${project_name}" 'to_entries[] | select(.value == "running")
            | .key | ltrimstr($project + "-") | rtrimstr("-1")' <<<"${states}")
        local bound="${mixed_unavailable_canary_bound}"
        if ((deadline_ms > 0 && window_ms / 1000 < bound)); then
            bound=$((window_ms / 1000))
        fi
        local pids=()
        local host
        for host in ${running[@]+"${running[@]}"}; do
            local name="chaos_mixed_${ordinal}_${label}_${host//-/_}"
            partition_canary "${name}" "${host}" "${checks_dir}" "${bound}" during true &
            pids+=("$!")
            canaries+=("${name}")
        done
        local pid
        for pid in ${pids[@]+"${pids[@]}"}; do
            wait "${pid}" || recovery_fail controller "a control attempt without a quorum could not be recorded in step ${ordinal}"
        done
        local name
        for name in ${canaries[@]+"${canaries[@]}"}; do
            if [[ "$(canary_outcome "${checks_dir}" "${name}")" == acknowledged ]]; then
                verdict='acknowledged-without-quorum'
                recovery_finding "${mixed_dir}" \
                    "$(jq -r '.entry_host' "${checks_dir}/control-${name}.json") acknowledged a control command while no quorum could communicate in step ${ordinal}"
            fi
        done
    fi
    cli_host="${node_hosts[0]}"
    jq -n \
        --arg label "${label}" \
        --arg verdict "${verdict}" \
        --argjson impaired "$(printf '%s\n' ${impaired[@]+"${impaired[@]}"} | jq -R . | jq -sc 'map(select(length > 0))')" \
        --argjson active "$(printf '%s\n' "${mixed_active[@]}" | jq -R . | jq -sc .)" \
        --argjson canaries "$(printf '%s\n' ${canaries[@]+"${canaries[@]}"} | jq -R . | jq -sc 'map(select(length > 0))')" \
        --argjson lossy "${lossy_quorum_link}" \
        '{label: $label, active_faults: $active, voters_out_of_quorum: $impaired,
          quorum_could_communicate: (($impaired | length) <= 1), quorum_link_lossy: $lossy,
          availability_required: ((($impaired | length) <= 1) and ($lossy | not)), verdict: $verdict,
          canaries: $canaries}' \
        >"${checks_dir}.json"
    mixed_trace_record faults-held "${ordinal}" --arg label "${label}" --arg verdict "${verdict}"
}

# True when the source runs at most the declared recovery backlog ahead of the sink.
mixed_backlog_within() {
    local source output
    source="$(topic_end_offset chaos_input)" || return 1
    output="$(topic_end_offset chaos_output)" || return 1
    [[ "${source}" =~ ^[0-9]+$ && "${output}" =~ ^[0-9]+$ ]] || return 1
    ((source - output <= max_recovery_backlog))
}

# The Docker lifecycle events one host's node faults of a step are responsible for, as verifier
# arguments.
mixed_expected_events() {
    local host="$1"
    shift
    local id
    for id in "$@"; do
        [[ " ${mixed_action_hosts[${id}]} " == *" ${host} "* ]] || continue
        case "${mixed_action_dir[${id}]}" in
            *-kill) printf -- '--expect\nkill:9\n--expect\ndie:137\n--expect\nstart\n' ;;
            *-stop) printf -- '--expect\nkill:15\n--expect\ndie:0\n--expect\nstart\n' ;;
            *-pause) printf -- '--expect\npause\n--expect\nunpause\n' ;;
        esac
    done
}

# Requires each node's lifecycle events from SINCE_NS through now to be exactly those of the given
# actions, recording a finding for any other.
mixed_check_node_events() {
    local since_ns="$1"
    local output_dir="$2"
    local context="$3"
    shift 3
    docker_event_window "${since_ns}" "${output_dir}/node-events.ndjson" --role node \
        || recovery_fail controller "the live Docker event recording does not cover ${context}"
    local host
    for host in "${node_hosts[@]}"; do
        local container_id
        container_id="$(owned_service_container "${host}")"
        jq -c --arg id "${container_id}" 'select(.Actor.ID == $id)' "${output_dir}/node-events.ndjson" \
            >"${output_dir}/node-events-${host}.ndjson"
        local expected=()
        mapfile -t expected < <(mixed_expected_events "${host}" "$@")
        "${script_dir}/verify-docker-events.sh" lifecycle --events "${output_dir}/node-events-${host}.ndjson" \
            --target "${container_id}" ${expected[@]+"${expected[@]}"} 2>"${output_dir}/node-events-${host}.verdict.txt" \
            || recovery_finding "${mixed_dir}" "${host} had node lifecycle events other than the planned faults during ${context}: $(head -n 1 "${output_dir}/node-events-${host}.verdict.txt")"
    done
}

# Waits for the cluster to settle after a step's last heal, for output and backlog to recover, and
# for a canary through each node the step faulted, then checks the step's node lifecycle events.
mixed_recover() {
    local ordinal="$1"
    local healed_ms="$2"
    local output_at_heal="$3"
    shift 3
    local faulted_hosts=("$@")
    local recovery_dir="${mixed_step_dir}/recovered"
    phase "mixed step ${ordinal}: public recovery"
    failure_category=product
    if ! wait_for "every node caught up, connected and executing after step ${ordinal}" "${mixed_settle_bound}" \
        mixed_observe "${recovery_dir}/observation"; then
        sample_statuses "${mixed_step_dir}" not-settled "${node_hosts[@]}" || true
        recovery_fail product "the cluster did not settle within ${mixed_settle_bound}s after step ${ordinal} healed"
    fi
    local settle_ms=$(($(epoch_ms) - healed_ms))
    local delivery_ms=null drain_ms=null
    if wait_for "output advanced after step ${ordinal}" "${mixed_delivery_bound}" \
        topic_progressed chaos_output "${output_at_heal}"; then
        delivery_ms=$(($(epoch_ms) - healed_ms))
    else
        recovery_finding "${mixed_dir}" "output did not advance within ${mixed_delivery_bound}s after step ${ordinal} healed"
    fi
    if wait_for "backlog within ${max_recovery_backlog} records after step ${ordinal}" "${mixed_drain_bound}" \
        mixed_backlog_within; then
        drain_ms=$(($(epoch_ms) - healed_ms))
    else
        recovery_finding "${mixed_dir}" \
            "the source stayed more than ${max_recovery_backlog} records ahead of the sink for ${mixed_drain_bound}s after step ${ordinal} healed"
    fi
    local canaries=()
    local host
    for host in "${faulted_hosts[@]}"; do
        local name="chaos_mixed_${ordinal}_after_${host//-/_}"
        partition_canary "${name}" "${host}" "${recovery_dir}" 60 after true
        canaries+=("${name}")
        [[ "$(canary_outcome "${recovery_dir}" "${name}")" == acknowledged ]] \
            || recovery_finding "${mixed_dir}" "a control command through ${host} was not acknowledged after step ${ordinal} recovered"
    done
    capture_all_metrics "${recovery_dir}"
    local pending
    pending="$(cat "${recovery_dir}"/metrics-*.txt \
        | awk '/^nervix_interconnect_pending_operations(\{| )/ { total += $NF } END { printf "%d\n", total }')"
    if ((pending > max_pending)); then
        recovery_finding "${mixed_dir}" \
            "${pending} interconnect operations were pending after step ${ordinal} recovered, above the declared ${max_pending}"
    fi
    mixed_check_node_events "${mixed_step_since_ns}" "${mixed_step_dir}" "step ${ordinal}" "${mixed_step_ids[@]}"
    jq -n \
        --argjson settle_ms "${settle_ms}" \
        --argjson delivery_ms "${delivery_ms}" \
        --argjson drain_ms "${drain_ms}" \
        --argjson pending "${pending}" \
        --argjson canaries "$(printf '%s\n' ${canaries[@]+"${canaries[@]}"} | jq -R . | jq -sc 'map(select(length > 0))')" \
        '{settled_after_heal_ms: $settle_ms, output_advanced_after_heal_ms: $delivery_ms,
          backlog_drained_after_heal_ms: $drain_ms, pending_operations: $pending, canaries: $canaries}' \
        >"${recovery_dir}.json"
}

# Runs step ORDINAL of the plan: no earlier than its planned offset, against nodes selected from
# settled public roles, injecting and healing its actions in planned order.
mixed_run_step() {
    local ordinal="$1"
    local total="$2"
    local step
    step="$(jq -c --argjson offset "$((ordinal - 1))" '.steps[$offset]' "${mixed_plan}")"
    local kind role pick at
    kind="$(jq -r '.kind' <<<"${step}")"
    role="$(jq -r '.role' <<<"${step}")"
    pick="$(jq -r '.pick' <<<"${step}")"
    at="$(jq -r '.at_seconds' <<<"${step}")"
    mixed_step_dir="${mixed_dir}/steps/$(printf '%03d' "${ordinal}")-${kind}"
    mkdir -p "${mixed_step_dir}"
    local due_ms=$((mixed_started_ms + at * 1000))
    mixed_sleep_until "${due_ms}"
    local started_ms
    started_ms="$(epoch_ms)"
    mixed_step_since_ns="$(date +%s%N)"
    phase "mixed step ${ordinal}/${total}: ${kind} of the ${role}"
    failure_category=product
    mixed_trace_record step-started "${ordinal}" --arg kind "${kind}" --arg role "${role}" \
        --argjson planned_at_seconds "${at}" --argjson late_ms "$((started_ms - due_ms))"
    check_support_containers
    if ! wait_for "a settled cluster with every node running and every interface default before step ${ordinal}" \
        "${mixed_pre_step_bound}" mixed_pre_step_ready "${mixed_step_dir}/before"; then
        recovery_fail product "the cluster was not settled with every node running and every interface default within ${mixed_pre_step_bound}s before step ${ordinal}"
    fi
    mixed_select_nodes "${mixed_step_dir}" "${role}" "${pick}" "${ordinal}"
    mixed_trace_record nodes-selected "${ordinal}" --argjson selection "$(jq -c . "${mixed_step_dir}/nodes.json")"
    mixed_active=()
    mixed_step_ids=()
    mapfile -t mixed_step_ids < <(jq -r '.actions[].id' <<<"${step}")
    local events=()
    mapfile -t events < <(jq -c '
        [.actions[] | {t: .start_seconds, order: 1, id}, {t: (.start_seconds + .hold_seconds), order: 0, id}]
        | sort_by(.t, .order) | .[]' <<<"${step}")
    local faults_started_ms
    faults_started_ms="$(epoch_ms)"
    local output_at_heal
    output_at_heal="$(topic_end_offset chaos_output)"
    local healed_ms="${faults_started_ms}"
    local index
    for index in "${!events[@]}"; do
        local event="${events[${index}]}"
        local id t order action
        id="$(jq -r '.id' <<<"${event}")"
        t="$(jq -r '.t' <<<"${event}")"
        order="$(jq -r '.order' <<<"${event}")"
        action="$(jq -c --arg id "${id}" '.actions[] | select(.id == $id)' <<<"${step}")"
        if ((order == 1)); then
            mixed_sleep_until $((faults_started_ms + t * 1000))
            mixed_inject "${ordinal}" "${action}"
            # The faults stand in their full combination when the next event heals one of them.
            local next="${events[$((index + 1))]:-}"
            if [[ -n "${next}" && "$(jq -r '.order' <<<"${next}")" == 0 ]]; then
                mixed_hold_checks "${ordinal}" "${id}"
            fi
        else
            local hold
            hold="$(jq -r '.hold_seconds' <<<"${action}")"
            local due=$((faults_started_ms + t * 1000))
            local held=$((${mixed_action_verified_ms[${id}]} + hold * 1000))
            if ((held > due)); then
                due="${held}"
            fi
            mixed_sleep_until "${due}"
            if ((${#mixed_active[@]} == 1)); then
                output_at_heal="$(topic_end_offset chaos_output)"
            fi
            mixed_heal "${ordinal}" "${action}"
            healed_ms="$(epoch_ms)"
        fi
    done
    local faulted_hosts=()
    mapfile -t faulted_hosts < <(
        for id in "${mixed_step_ids[@]}"; do
            # The host list is space separated.
            # shellcheck disable=SC2086
            printf '%s\n' ${mixed_action_hosts[${id}]} ${mixed_action_impaired[${id}]}
        done | sed '/^$/d' | sort -u
    )
    mixed_recover "${ordinal}" "${healed_ms}" "${output_at_heal}" "${faulted_hosts[@]}"
    check_support_containers
    jq -n \
        --argjson ordinal "${ordinal}" \
        --argjson step "${step}" \
        --argjson started_ms "${started_ms}" \
        --argjson late_ms "$((started_ms - due_ms))" \
        --argjson faults_started_ms "${faults_started_ms}" \
        --argjson healed_ms "${healed_ms}" \
        --argjson ended_ms "$(epoch_ms)" \
        --slurpfile nodes "${mixed_step_dir}/nodes.json" \
        --slurpfile recovery "${mixed_step_dir}/recovered.json" '
        {step: $ordinal, kind: $step.kind, role: $step.role, planned_at_seconds: $step.at_seconds,
         estimated_seconds: $step.estimated_seconds, late_ms: $late_ms,
         nodes: $nodes[0].refs, started_ms: $started_ms, faults_started_ms: $faults_started_ms,
         healed_ms: $healed_ms, ended_ms: $ended_ms, duration_ms: ($ended_ms - $started_ms),
         recovery: $recovery[0]}' >"${mixed_step_dir}/result.json"
    mixed_trace_record step-ended "${ordinal}" --arg outcome recovered
}

# Reads whether every control canary of the run exists through every node's own route and records a
# finding for an acknowledged canary that is absent, a refused one that is present, or nodes that
# disagree.
mixed_reconcile_canaries() {
    phase 'mixed: control canary reconciliation'
    local canary_files=()
    mapfile -t canary_files < <(find "${mixed_dir}/steps" -name 'control-*.json' -type f | LC_ALL=C sort)
    ((${#canary_files[@]} > 0)) || { printf '[]\n' >"${mixed_dir}/canaries.json"; return 0; }
    local pairs=()
    local file
    for file in "${canary_files[@]}"; do
        local name
        name="$(jq -r '.name' "${file}")"
        pairs+=("${name}" "DESCRIBE RESOURCE ${name};")
    done
    local host
    for host in "${node_hosts[@]}"; do
        admin_cli_batch "${host}" chaos_baseline "${mixed_dir}/canaries/${host}" 300 "${pairs[@]}" \
            || recovery_fail product "${host} did not answer the canary reconciliation within its bound"
    done
    local records=()
    for file in "${canary_files[@]}"; do
        local name outcome
        name="$(jq -r '.name' "${file}")"
        outcome="$(jq -r '.outcome' "${file}")"
        local effects=()
        for host in "${node_hosts[@]}"; do
            local effect=unknown
            if batch_read_succeeded "${mixed_dir}/canaries/${host}" "${name}"; then
                effect=present
            elif grep -Fq "resource '${name}' does not exist" "${mixed_dir}/canaries/${host}/${name}.txt" 2>/dev/null; then
                effect=absent
            fi
            effects+=("${effect}")
        done
        local agreed="${effects[0]}"
        local effect
        for effect in "${effects[@]}"; do
            [[ "${effect}" == "${agreed}" ]] || agreed=disagreed
        done
        if [[ "${agreed}" == disagreed || "${agreed}" == unknown ]]; then
            recovery_finding "${mixed_dir}" "the nodes did not agree whether canary ${name} exists: ${effects[*]}"
        elif [[ "${outcome}" == acknowledged && "${agreed}" != present ]]; then
            recovery_finding "${mixed_dir}" "acknowledged canary ${name} is absent after recovery"
        elif [[ "${outcome}" == refused && "${agreed}" != absent ]]; then
            recovery_finding "${mixed_dir}" "refused canary ${name} took effect"
        fi
        records+=("$(jq -c --arg effect "${agreed}" '. + {observed_effect: $effect}' "${file}")")
    done
    printf '%s\n' "${records[@]}" | jq -s . >"${mixed_dir}/canaries.json"
}

run_mixed_instability() {
    mkdir -p "${mixed_dir}/steps"
    : >"${mixed_trace}"
    : >"${mixed_samples}"
    [[ "$(wc -l <"${artifact_dir}/fixtures/input.ndjson")" -eq "${record_count}" ]] \
        || recovery_fail controller 'the run fixture does not hold its recorded record count'
    recovery_traffic_start "${mixed_dir}"
    failure_category=product
    mixed_start_sampler
    mixed_started_ms="$(epoch_ms)"
    mixed_started_ns="$(date +%s%N)"
    local total
    total="$(jq '.steps | length' "${mixed_plan}")"
    local ordinal
    for ((ordinal = 1; ordinal <= total; ordinal++)); do
        mixed_run_step "${ordinal}" "${total}"
    done
    phase 'mixed: fault phase evidence'
    local fault_phase_ms=$(($(epoch_ms) - mixed_started_ms))
    mixed_stop_sampler
    local all_ids=()
    mapfile -t all_ids < <(jq -r '.steps[].actions[].id' "${mixed_plan}")
    mkdir -p "${mixed_dir}/fault-phase"
    mixed_check_node_events "${mixed_started_ns}" "${mixed_dir}/fault-phase" 'the fault phase' "${all_ids[@]}"
    mixed_reconcile_canaries
    capture_all_metrics "${mixed_dir}/final"
    local verdict_status=0
    "${script_dir}/verify-mixed-evidence.sh" trace --plan "${mixed_plan}" --trace "${mixed_trace}" \
        --output "${artifact_dir}/results/mixed-trace.json" >"${mixed_dir}/trace-verdict.txt" 2>&1 \
        || verdict_status=$?
    ((verdict_status <= 1)) \
        || recovery_fail controller 'the action trace could not be judged; see mixed/trace-verdict.txt'
    verdict_status=0
    "${script_dir}/verify-mixed-evidence.sh" resources --samples "${mixed_samples}" \
        --max-memory-bytes "${max_memory_bytes}" --output "${artifact_dir}/results/mixed-resources.json" \
        >"${mixed_dir}/resources-verdict.txt" 2>&1 || verdict_status=$?
    ((verdict_status <= 1)) \
        || recovery_fail controller 'the resource samples could not be judged; see mixed/resources-verdict.txt'

    local finding
    while IFS= read -r finding; do
        recovery_finding "${mixed_dir}" "${finding}"
    done < <(jq -r '.findings[]?.message' "${artifact_dir}/results/mixed-resources.json" 2>/dev/null)
    jq -s \
        --argjson fault_phase_ms "${fault_phase_ms}" \
        --slurpfile plan "${mixed_plan}" \
        --slurpfile trace "${artifact_dir}/results/mixed-trace.json" \
        --slurpfile resources "${artifact_dir}/results/mixed-resources.json" '
        def stats: sort | if length == 0 then null
                         else {count: length, min: .[0], median: .[length / 2 | floor], max: .[-1]} end;
        {seed: $plan[0].seed, policy: $plan[0].policy, planned_duration_seconds: $plan[0].duration_seconds,
         fault_phase_ms: $fault_phase_ms, steps: .,
         recovery_ms: {settled_after_heal: ([.[].recovery.settled_after_heal_ms | numbers] | stats),
                       output_advanced_after_heal: ([.[].recovery.output_advanced_after_heal_ms | numbers] | stats),
                       backlog_drained_after_heal: ([.[].recovery.backlog_drained_after_heal_ms | numbers] | stats)},
         step_duration_ms: ([.[].duration_ms] | stats),
         trace: $trace[0], resources: $resources[0].summary,
         evidence: {plan: "mixed/plan.json", trace: "mixed/actions.ndjson", samples: "mixed/samples.ndjson",
                    canaries: "mixed/canaries.json", steps: "mixed/steps"}}
    ' "${mixed_dir}"/steps/*/result.json >"${artifact_dir}/results/mixed-instability-progress.json"
    recovery_final_boundary
}
