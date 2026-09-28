#!/usr/bin/env bash
# Sourced by run-baseline.sh after the external cluster and graph are ready.

# shellcheck source=crash-scenario.sh
source "${script_dir}/crash-scenario.sh"

# External budgets, in seconds. They bound how long the runner waits for each published behavior
# and are kept apart from the product liveness settings the result records.
partition_install_bound=30
partition_majority_leader_bound=60
partition_majority_commit_bound=30
partition_minority_attempt_bound=20
partition_failover_bound=90
partition_majority_delivery_bound=60
partition_convergence_bound=150
partition_execution_bound=90
partition_delivery_after_heal_bound=90
# An injector heals itself after this long even if nothing stops it.
partition_injector_lifetime=900
# Seconds between safety samples of the nodes cut off from a quorum.
partition_sample_interval=5
partition_echo_count=3

partition_fail() {
    failure_category="$1"
    shift
    printf 'partition %s failure: %s\n' "${failure_category}" "$*" >&2
    return 1
}

# Records a product violation that does not stop the experiment. The case continues so one run
# retains every violation it can observe, and the run fails after its final ledger check.
partition_finding() {
    local case_dir="$1"
    shift
    printf 'partition product finding: %s\n' "$*" >&2
    jq -nc \
        --arg case "${case_dir##*/}" \
        --arg message "$*" \
        --arg phase "${current_phase}" \
        --arg at "$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)" \
        '{case:$case,phase:$phase,message:$message,at:$at}' \
        | tee -a "${case_dir}/findings.ndjson" >>"${artifact_dir}/results/partition-findings.ndjson"
}

node_address() {
    local variable="CHAOS_NODE_${1##*-}_ADDRESS"
    printf '%s\n' "${!variable}"
}

container_address() {
    run_bounded 20 docker inspect \
        --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$1"
}

verify_node_addresses() {
    local host
    for host in "${node_hosts[@]}"; do
        local container_id
        container_id="$(owned_service_container "${host}")" || return 1
        [[ "$(container_address "${container_id}")" == "$(node_address "${host}")" ]] \
            || partition_fail setup "${host} does not use its planned run address $(node_address "${host}")"
    done
}

# The ICMP echo requests the kernel of a container's network namespace has accepted. Rules dropped
# by Pumba's netem egress or iptables INPUT filter never reach this counter.
received_echo_requests() {
    local container_id="$1"
    run_bounded 20 docker run --rm \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=link-probe \
        --network "container:${container_id}" \
        "${CHAOS_PROBE_IMAGE}" \
        awk '/^Icmp:/ {
                if (!column) { for (i = 1; i <= NF; i++) if ($i == "InEchos") column = i; next }
                print $column
                exit
            }' /proc/net/snmp
}

# The same count, rejected unless the namespace reported one.
echo_request_count() {
    local count
    count="$(received_echo_requests "$1")" || return 1
    [[ "${count}" =~ ^[0-9]+$ ]] || partition_fail controller "no ICMP echo counter in the namespace of $1"
    printf '%s\n' "${count}"
}

send_echo_requests_from() {
    local container_id="$1"
    shift
    run_bounded 60 docker run --rm \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=link-probe \
        --network "container:${container_id}" \
        "${CHAOS_PROBE_IMAGE}" \
        sh -c 'count="$1"; shift; for address; do ping -c "${count}" -i 0.2 -W 1 -q "${address}" >/dev/null 2>&1 || true; done' \
        -- "${partition_echo_count}" "$@"
}

# Measures every directed node link, the verifier's route to each node, and each node's route to the
# broker by counting the echo requests each receiver's kernel accepted from one sender at a time.
probe_links() {
    local output="$1"
    local links_file
    links_file="$(mktemp "${artifact_dir}/.links.XXXXXX")"
    local -A containers=()
    local -A accepted=()
    local host
    for host in "${node_hosts[@]}"; do
        containers["${host}"]="$(owned_service_container "${host}")" || return 1
    done
    containers[broker]="$(owned_service_container broker)" || return 1
    local broker_address
    broker_address="$(container_address "${containers[broker]}")"
    local receiver
    for receiver in "${node_hosts[@]}" broker; do
        accepted["${receiver}"]="$(echo_request_count "${containers[${receiver}]}")" || return 1
    done

    local node_addresses=()
    for host in "${node_hosts[@]}"; do
        node_addresses+=("$(node_address "${host}")")
    done
    compose run --rm --no-deps probe \
        sh -c 'count="$1"; shift; for address; do ping -c "${count}" -i 0.2 -W 1 -q "${address}" >/dev/null 2>&1 || true; done' \
        -- "${partition_echo_count}" "${node_addresses[@]}" >/dev/null 2>&1
    for receiver in "${node_hosts[@]}"; do
        local now
        now="$(echo_request_count "${containers[${receiver}]}")" || return 1
        jq -nc --arg to "${receiver}" --argjson sent "${partition_echo_count}" \
            --argjson received "$((now - accepted[${receiver}]))" \
            '{from:"verifier",to:$to,sent:$sent,received:$received}' >>"${links_file}"
        accepted["${receiver}"]="${now}"
    done

    local source
    for source in "${node_hosts[@]}"; do
        local receivers=()
        local addresses=()
        for host in "${node_hosts[@]}"; do
            [[ "${host}" == "${source}" ]] && continue
            receivers+=("${host}")
            addresses+=("$(node_address "${host}")")
        done
        receivers+=(broker)
        addresses+=("${broker_address}")
        send_echo_requests_from "${containers[${source}]}" "${addresses[@]}"
        for receiver in "${receivers[@]}"; do
            local now
            now="$(echo_request_count "${containers[${receiver}]}")" || return 1
            jq -nc --arg from "${source}" --arg to "${receiver}" --argjson sent "${partition_echo_count}" \
                --argjson received "$((now - accepted[${receiver}]))" \
                '{from:$from,to:$to,sent:$sent,received:$received}' >>"${links_file}"
            accepted["${receiver}"]="${now}"
        done
    done
    jq -s '{links: .}' "${links_file}" >"${output}"
    rm -f "${links_file}"
}

inspect_node_rules() {
    local host="$1"
    local output="$2"
    local container_id
    container_id="$(owned_service_container "${host}")" || return 1
    "${script_dir}/network-faults.sh" inspect --run-id "${run_id}" --container "${container_id}" \
        --nettools "${CHAOS_NETTOOLS_IMAGE}" --output "${output}"
}

# True when every node carries exactly the rules the plan places on it, or none with --healed.
rules_match_plan() {
    local plan="$1"
    local output_dir="$2"
    local healed="${3:-}"
    local host
    for host in "${node_hosts[@]}"; do
        inspect_node_rules "${host}" "${output_dir}/${host}.txt" || return 1
        "${script_dir}/verify-partition-evidence.sh" rules "${plan}" "${host}" \
            "${output_dir}/${host}.txt" ${healed:+"${healed}"} \
            2>"${output_dir}/${host}.verdict.txt" || return 1
    done
}

status_value() {
    awk -F ': ' -v key="$2" '$1 == key { print $2; exit }' "$1"
}

status_gossip_live() {
    awk '/^\[chitchat\]$/ { section = 1; next }
         /^\[/ { section = 0 }
         section && /^live_nodes:/ { live = 1; next }
         section && /^[a-z_]+:/ { live = 0 }
         section && live && /^- node_id: / { print $3 }' "$1"
}

status_peers() {
    awk '/^\[interconnect\]$/ { section = 1; next }
         /^\[/ { section = 0 }
         section && /^- node-[0-9]+: / { node = $2; sub(/:$/, "", node); print node " " $NF }' "$1"
}

status_warnings() {
    awk '/^\[warnings\]$/ { section = 1; next }
         /^\[/ { section = 0 }
         section && /^- / { print substr($0, 3) }' "$1"
}

# A node's own status through its public route. The response must name that node, so the
# observation is attributed to the node that answered even when other commands can redirect.
partition_node_status() {
    local host="$1"
    local file="$2"
    cli_host="${host}"
    compose_call_timeout=20 cli_command 'SHOW CLUSTER STATUS;' >"${file}" 2>&1 || return 1
    grep -Fxq "raft.id: node-${host##*-}" "${file}" || return 1
    jq -n \
        --arg host "${host}" \
        --arg node "node-${host##*-}" \
        --arg leader "$(status_value "${file}" raft.current_leader)" \
        --arg term "$(status_value "${file}" raft.current_term)" \
        --arg state "$(status_value "${file}" raft.state)" \
        --arg last_log "$(status_value "${file}" raft.last_log_index)" \
        --arg last_applied "$(status_value "${file}" raft.last_applied)" \
        --arg gossip "$(status_gossip_live "${file}")" \
        --arg peers "$(status_peers "${file}")" \
        --arg warnings "$(status_warnings "${file}")" \
        --arg observed_at "$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)" '
        def lines: split("\n") | map(select(length > 0));
        {host: $host, node: $node, observed_at: $observed_at, leader: $leader, state: $state,
         term: ($term | tonumber), last_log_index: ($last_log | tonumber),
         last_applied: ($last_applied | tonumber), gossip_live_nodes: ($gossip | lines),
         peers: ($peers | lines), warnings: ($warnings | lines)}' >"${file%.txt}.json"
}

# Samples the given nodes in order and appends their attributed status to the case ledger.
sample_statuses() {
    local output_dir="$1"
    local label="$2"
    shift 2
    mkdir -p "${output_dir}/${label}"
    local host
    for host in "$@"; do
        local attempt
        local sampled=false
        for attempt in 1 2 3; do
            if partition_node_status "${host}" "${output_dir}/${label}/status-${host}.txt"; then
                sampled=true
                break
            fi
            sleep 1
        done
        [[ "${sampled}" == true ]] \
            || partition_fail product "${host} did not answer its own public status route during ${label}"
        jq -c --arg label "${label}" '. + {sample: $label}' \
            "${output_dir}/${label}/status-${host}.json" >>"${output_dir}/samples.ndjson"
    done
}

consumer_group_snapshot() {
    local output="$1"
    broker_admin /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server broker:9092 --group chaos_baseline --describe --members \
        >"${output}.txt" 2>&1 || return 1
    local address_map
    address_map="$(for host in "${node_hosts[@]}"; do printf '%s %s\n' "$(node_address "${host}")" "${host}"; done)"
    awk -v address_map="${address_map}" '
        BEGIN {
            count = split(address_map, entries, "\n")
            for (i = 1; i <= count; i++) {
                split(entries[i], pair, " ")
                host_of[pair[1]] = pair[2]
            }
        }
        $1 == "chaos_baseline" && $3 ~ /^\// {
            address = substr($3, 2)
            host = (address in host_of) ? host_of[address] : "unattributed"
            printf "{\"consumer\":\"%s\",\"address\":\"%s\",\"node_host\":\"%s\",\"partitions\":%d}\n", $2, address, host, $NF
        }
    ' "${output}.txt" | jq -s '{members: .}' >"${output}"
}

# Kafka hands the source partition to exactly one group member. Execution has converged when every
# member runs on the scheduled ingestor owner and that owner holds the partition.
ingestion_converged() {
    local output_dir="$1"
    local owner
    owner="$(owner_from_description "${output_dir}/ingestor.attempt.txt")"
    [[ "${owner}" =~ ^node-[123]$ ]] || return 1
    consumer_group_snapshot "${output_dir}/consumer-group.json" || return 1
    jq -e --arg owner "nervix-${owner##*-}" '
        (.members | length) > 0
        and (.members | all(.node_host == $owner))
        and ([.members[].partitions] | add) == 1
    ' "${output_dir}/consumer-group.json" >/dev/null
}

# Runs one CREATE RESOURCE canary through a node's public route. An acknowledgement is a commit the
# cluster reported; a refusal is a definite "not executed"; anything else, including the external
# bound expiring, is uncertain and is reconciled after recovery.
partition_canary() {
    local name="$1"
    local host="$2"
    local output_dir="$3"
    local limit="$4"
    local window="$5"
    local redirect_possible="$6"
    local container_name="${project_name}-canary-${name}"
    local started_at started_ms ended_ms
    local status=0
    started_at="$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)"
    started_ms="$(epoch_ms)"
    run_bounded "${limit}" docker compose "${compose_args[@]}" run --rm --no-deps \
        --name "${container_name}" admin \
        nervix-cli --server "http://${host}:47391" --domain chaos_baseline \
        --password "${CHAOS_PASSWORD}" --command "CREATE RESOURCE ${name};" \
        >"${output_dir}/control-${name}.txt" 2>&1 || status=$?
    ended_ms="$(epoch_ms)"
    # The CLI is PID 1 in its container and outlives the signal that ended a bounded wait. Remove
    # it so an attempt cannot complete after its fault window.
    run_bounded 30 docker container rm --force "${container_name}" >/dev/null 2>&1 || true
    local outcome=uncertain
    if [[ "${status}" -eq 0 ]] && grep -Fxq "created resource '${name}'" "${output_dir}/control-${name}.txt"; then
        outcome=acknowledged
    elif grep -Eq "^topology: not-a-leader|^error: " "${output_dir}/control-${name}.txt" \
        && ! grep -Eq 'uncertain|not known yet' "${output_dir}/control-${name}.txt"; then
        outcome=refused
    fi
    jq -n \
        --arg name "${name}" \
        --arg host "${host}" \
        --arg window "${window}" \
        --arg outcome "${outcome}" \
        --arg started_at "${started_at}" \
        --argjson redirect_possible "${redirect_possible}" \
        --argjson exit_code "${status}" \
        --argjson elapsed_ms "$((ended_ms - started_ms))" \
        --argjson bound_seconds "${limit}" \
        '{name:$name,entry_host:$host,window:$window,outcome:$outcome,exit_code:$exit_code,started_at:$started_at,elapsed_ms:$elapsed_ms,bound_seconds:$bound_seconds,redirect_possible:$redirect_possible}' \
        >"${output_dir}/control-${name}.json"
}

canary_outcome() {
    jq -r '.outcome' "$1/control-$2.json"
}

observe_partition_roles() {
    local output_dir="$1"
    mkdir -p "${output_dir}"
    cluster_settled_and_connected "${output_dir}" || return 1
    local leader ingestor_owner relay_owner emitter_owner
    leader="$(<"${output_dir}/leader.txt")"
    ingestor_owner="$(owner_from_description "${output_dir}/ingestor.attempt.txt")"
    relay_owner="$(owner_from_description "${output_dir}/relay.attempt.txt")"
    emitter_owner="$(owner_from_description "${output_dir}/emitter.attempt.txt")"
    [[ "${leader}" =~ ^node-[123]$ && "${ingestor_owner}" =~ ^node-[123]$ \
        && "${relay_owner}" =~ ^node-[123]$ && "${emitter_owner}" =~ ^node-[123]$ ]] || return 1
    local follower=""
    local candidate
    for candidate in "${relay_owner}" "${emitter_owner}" "${ingestor_owner}"; do
        if [[ "${candidate}" != "${leader}" ]]; then
            follower="${candidate}"
            break
        fi
    done
    if [[ -z "${follower}" ]]; then
        local node
        for node in "${node_names[@]}"; do
            if [[ "${node}" != "${leader}" ]]; then
                follower="${node}"
                break
            fi
        done
    fi
    jq -n \
        --arg leader "${leader}" \
        --arg follower "${follower}" \
        --arg ingestor "${ingestor_owner}" \
        --arg relay "${relay_owner}" \
        --arg emitter "${emitter_owner}" \
        '{leader:$leader,follower:$follower,ingestor_owner:$ingestor,relay_owner:$relay,emitter_owner:$emitter}' \
        >"${output_dir}/role.json"
}

# Writes a plan with every node and no rules: the healthy topology a case starts from.
plan_topology() {
    local output="$1"
    local host
    local address_args=()
    for host in "${node_hosts[@]}"; do
        address_args+=(--arg "${host}" "$(node_address "${host}")")
    done
    jq -n \
        --argjson hosts "$(printf '%s\n' "${node_hosts[@]}" | jq -R . | jq -s .)" \
        "${address_args[@]}" '
        ($hosts | map({key: ., value: $ARGS.named[.]}) | from_entries) as $addresses
        | {case: "healthy", placement: "none", isolated: [], addresses: $addresses, intended_blocked: [],
           rules: ($hosts | map({key: ., value: {netem_targets: [], iptables_sources: []}}) | from_entries)}' \
        >"${output}"
}

# Writes a plan whose rules make `isolated` unreachable from and unable to reach every other node.
# The rules live on the other nodes, so the isolation survives the isolated container's restart.
plan_isolation() {
    local case_name="$1"
    local isolated="$2"
    local output="$3"
    local host
    local address_args=()
    for host in "${node_hosts[@]}"; do
        address_args+=(--arg "${host}" "$(node_address "${host}")")
    done
    jq -n \
        --arg case "${case_name}" \
        --arg isolated "${isolated}" \
        --argjson hosts "$(printf '%s\n' "${node_hosts[@]}" | jq -R . | jq -s .)" \
        "${address_args[@]}" '
        ($hosts | map({key: ., value: $ARGS.named[.]}) | from_entries) as $addresses
        | {case: $case, placement: "peers", isolated: [$isolated], addresses: $addresses,
           intended_blocked: [$hosts[] | select(. != $isolated) | ("\($isolated)>\(.)", "\(.)>\($isolated)")],
           rules: ($hosts | map({key: ., value: (if . == $isolated
                     then {netem_targets: [], iptables_sources: []}
                     else {netem_targets: [$addresses[$isolated]], iptables_sources: [$addresses[$isolated]]} end)})
                   | from_entries)}' >"${output}"
}

# Writes a plan that drops only the packets `from` sends to `to`, at the sender's egress.
plan_one_way() {
    local from="$1"
    local to="$2"
    local output="$3"
    local host
    local address_args=()
    for host in "${node_hosts[@]}"; do
        address_args+=(--arg "${host}" "$(node_address "${host}")")
    done
    jq -n \
        --arg from "${from}" \
        --arg to "${to}" \
        --argjson hosts "$(printf '%s\n' "${node_hosts[@]}" | jq -R . | jq -s .)" \
        "${address_args[@]}" '
        ($hosts | map({key: ., value: $ARGS.named[.]}) | from_entries) as $addresses
        | {case: "asymmetric", placement: "sender", isolated: [], addresses: $addresses,
           intended_blocked: ["\($from)>\($to)"],
           rules: ($hosts | map({key: ., value: (if . == $from
                     then {netem_targets: [$addresses[$to]], iptables_sources: []}
                     else {netem_targets: [], iptables_sources: []} end)})
                   | from_entries)}' >"${output}"
}

# Writes a plan in which no two nodes can exchange a packet: one netem configuration per node
# drops everything it sends to either peer.
plan_quorum_loss() {
    local output="$1"
    local host
    local address_args=()
    for host in "${node_hosts[@]}"; do
        address_args+=(--arg "${host}" "$(node_address "${host}")")
    done
    jq -n \
        --argjson hosts "$(printf '%s\n' "${node_hosts[@]}" | jq -R . | jq -s .)" \
        "${address_args[@]}" '
        ($hosts | map({key: ., value: $ARGS.named[.]}) | from_entries) as $addresses
        | {case: "quorum-loss", placement: "sender", isolated: $hosts, addresses: $addresses,
           intended_blocked: [$hosts[] as $from | $hosts[] | select(. != $from) | "\($from)>\(.)"],
           rules: ($hosts | map(. as $host | {key: ., value: {
                     netem_targets: [$hosts[] | select(. != $host) | $addresses[.]],
                     iptables_sources: []}}) | from_entries)}' >"${output}"
}

# Starts one detached Pumba injector for one node's planned rules after a dry run proves it selects
# exactly that node's container. The injector keeps its fault until it receives SIGTERM.
start_injector() {
    local case_dir="$1"
    local ordinal="$2"
    local host="$3"
    local kind="$4"
    shift 4
    local addresses=("$@")
    local container_id container_name
    container_id="$(owned_service_container "${host}")" || return 1
    container_name="$(run_bounded 20 docker inspect --format '{{.Name}}' "${container_id}")"
    container_name="${container_name#/}"
    local fault_args=()
    local address
    if [[ "${kind}" == netem ]]; then
        fault_args=(--interface eth0 --tc-image "${CHAOS_NETTOOLS_IMAGE}" --pull-image=false)
        for address in "${addresses[@]}"; do
            fault_args+=(--target "${address}")
        done
        fault_args+=(loss --percent 100)
    else
        fault_args=(--interface eth0 --iptables-image "${CHAOS_NETTOOLS_IMAGE}" --pull-image=false)
        for address in "${addresses[@]}"; do
            fault_args+=(--source "${address}")
        done
        fault_args+=(loss --probability 1.0)
    fi
    local injector_dir="${case_dir}/injectors"
    mkdir -p "${injector_dir}"
    run_bounded 30 docker run --rm \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=fault \
        --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        "${pumba_image_id}" --dry-run --log-level info \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=node \
        "${kind}" --duration 1s "${fault_args[@]}" "${container_name}" \
        >"${injector_dir}/${host}-${kind}-dry-run.txt" 2>&1
    # Pumba logs fields in alphabetical order, so each selector is matched as its own field.
    local selections
    selections="$(grep -F "msg=\"running ${kind} on container\"" "${injector_dir}/${host}-${kind}-dry-run.txt" || true)"
    [[ "$(grep -c . <<<"${selections}")" -eq 1 ]] \
        && grep -Eq "(^| )dryrun=true( |$)" <<<"${selections}" \
        && grep -Eq "(^| )id=${container_id}( |$)" <<<"${selections}" \
        && grep -Eq "(^| )name=/${container_name}( |$)" <<<"${selections}" \
        || partition_fail injection "Pumba ${kind} dry run did not select exactly ${host}"
    local injector_name="${project_name}-fault-${ordinal}-${host}-${kind}"
    local injector_id
    injector_id="$(run_bounded 30 docker run --detach \
        --name "${injector_name}" \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=fault \
        --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        "${pumba_image_id}" --log-level info \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=node \
        "${kind}" --duration "${partition_injector_lifetime}s" "${fault_args[@]}" "${container_name}")"
    local fault_json
    fault_json="$(printf '%s\n' "${fault_args[@]}" | jq -R . | jq -s .)"
    jq -n \
        --arg host "${host}" \
        --arg kind "${kind}" \
        --arg container "${container_name}" \
        --arg container_id "${container_id}" \
        --arg injector "${injector_name}" \
        --arg injector_id "${injector_id}" \
        --arg image "${pumba_image_id}" \
        --arg run_id "${run_id}" \
        --arg started_at "$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)" \
        --argjson lifetime "${partition_injector_lifetime}" \
        --argjson fault "${fault_json}" \
        '{host:$host,kind:$kind,target:$container,target_id:$container_id,injector:$injector,injector_id:$injector_id,pumba_image_id:$image,command:(["pumba","--label","io.nervix.chaos.run="+$run_id,"--label","io.nervix.chaos.role=node",$kind,"--duration",($lifetime|tostring)+"s"] + $fault + [$container]),started_at:$started_at}' \
        >"${injector_dir}/${host}-${kind}.json"
}

injectors_running() {
    local case_dir="$1"
    local command_file
    for command_file in "${case_dir}"/injectors/*-netem.json "${case_dir}"/injectors/*-iptables.json; do
        [[ -e "${command_file}" ]] || continue
        container_running "$(jq -r '.injector_id' "${command_file}")" || return 1
    done
}

installed_as_planned() {
    local case_dir="$1"
    injectors_running "${case_dir}" || return 2
    rules_match_plan "${case_dir}/plan.json" "${case_dir}/rules-installed"
}

install_partition() {
    local case_dir="$1"
    local ordinal="$2"
    phase "partition ${ordinal}: installing $(jq -r '.case' "${case_dir}/plan.json") faults"
    failure_category=injection
    "${script_dir}/verify-partition-evidence.sh" plan "${case_dir}/plan.json" \
        || partition_fail controller 'the partition plan does not block exactly the intended links'
    mkdir -p "${case_dir}/rules-before" "${case_dir}/rules-installed"
    # Faults never stack: every interface must carry only its default state before installation.
    rules_match_plan "${case_dir}/plan.json" "${case_dir}/rules-before" --healed \
        || partition_fail injection 'a node interface already carries a qdisc, filter or INPUT rule; refusing to overlap faults'
    fault_started_at="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    fault_started_ms="$(epoch_ms)"
    local host
    for host in "${node_hosts[@]}"; do
        local netem_targets=()
        local iptables_sources=()
        mapfile -t netem_targets < <(jq -r --arg host "${host}" '.rules[$host].netem_targets[]' "${case_dir}/plan.json")
        mapfile -t iptables_sources < <(jq -r --arg host "${host}" '.rules[$host].iptables_sources[]' "${case_dir}/plan.json")
        if ((${#netem_targets[@]} > 0)); then
            start_injector "${case_dir}" "${ordinal}" "${host}" netem "${netem_targets[@]}"
        fi
        if ((${#iptables_sources[@]} > 0)); then
            start_injector "${case_dir}" "${ordinal}" "${host}" iptables "${iptables_sources[@]}"
        fi
    done
    local deadline=$((SECONDS + partition_install_bound))
    local installed=false
    while ((SECONDS < deadline)); do
        local state=0
        installed_as_planned "${case_dir}" || state=$?
        if ((state == 0)); then
            installed=true
            break
        fi
        if ((state == 2)); then
            partition_fail injection 'a Pumba injector exited before its fault was installed'
        fi
        sleep 1
    done
    [[ "${installed}" == true ]] \
        || partition_fail injection "installed rules did not match the plan within ${partition_install_bound}s"
    rules_installed_ms="$(epoch_ms)"
    probe_links "${case_dir}/links-isolated.json"
    "${script_dir}/verify-partition-evidence.sh" links "${case_dir}/plan.json" \
        "${case_dir}/links-isolated.json" "${case_dir}/links-isolated-verdict.json" \
        || partition_fail injection 'Pumba installed its rules but the observed links differ from the plan'
    isolation_verified_at="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    isolation_verified_ms="$(epoch_ms)"
    phase "partition ${ordinal}: observing the verified $(jq -r '.case' "${case_dir}/plan.json") fault"
}

heal_partition() {
    local case_dir="$1"
    local ordinal="$2"
    phase "partition ${ordinal}: healing through injector shutdown"
    failure_category=injection
    heal_started_ms="$(epoch_ms)"
    local command_file
    local injector_ids=()
    for command_file in "${case_dir}"/injectors/*-netem.json "${case_dir}"/injectors/*-iptables.json; do
        [[ -e "${command_file}" ]] || continue
        injector_ids+=("$(jq -r '.injector_id' "${command_file}")")
    done
    run_bounded 60 docker stop -t 40 "${injector_ids[@]}" >"${case_dir}/injectors/stop.txt" 2>&1
    for command_file in "${case_dir}"/injectors/*-netem.json "${case_dir}"/injectors/*-iptables.json; do
        [[ -e "${command_file}" ]] || continue
        local injector_id exit_code
        injector_id="$(jq -r '.injector_id' "${command_file}")"
        exit_code="$(run_bounded 20 docker inspect --format '{{.State.ExitCode}}' "${injector_id}")"
        run_bounded 20 docker logs "${injector_id}" >"${command_file%.json}.log" 2>&1
        run_bounded 30 docker container rm "${injector_id}" >/dev/null
        local updated
        updated="$(mktemp "${case_dir}/injectors/.command.XXXXXX")"
        jq --argjson exit_code "${exit_code}" '.exit_code_after_sigterm = $exit_code' \
            "${command_file}" >"${updated}"
        mv "${updated}" "${command_file}"
        [[ "${exit_code}" == 0 ]] \
            || partition_fail injection "injector $(jq -r '.injector' "${command_file}") exited ${exit_code} after SIGTERM"
    done
    mkdir -p "${case_dir}/rules-healed"
    rules_match_plan "${case_dir}/plan.json" "${case_dir}/rules-healed" --healed \
        || partition_fail injection 'owned rules remained after every injector received SIGTERM'
    probe_links "${case_dir}/links-healed.json"
    "${script_dir}/verify-partition-evidence.sh" links "${case_dir}/plan.json" \
        "${case_dir}/links-healed.json" "${case_dir}/links-healed-verdict.json" --healed \
        || partition_fail injection 'links did not reopen after the owned rules were removed'
    heal_completed_at="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    heal_completed_ms="$(epoch_ms)"
}

# Holds the verified fault for at least the declared window, sampling the nodes cut off from a
# quorum so their safety invariants stay under observation for the whole fault.
hold_partition() {
    local case_dir="$1"
    shift
    local observed=("$@")
    local hold_until_ms=$((isolation_verified_ms + partition_seconds * 1000))
    local sample=0
    while true; do
        check_isolation_invariants "${case_dir}" || return 1
        (( $(epoch_ms) < hold_until_ms )) || break
        sample=$((sample + 1))
        local remaining_ms=$((hold_until_ms - $(epoch_ms)))
        local pause_ms=$((partition_sample_interval * 1000))
        if ((remaining_ms < pause_ms)); then
            pause_ms="${remaining_ms}"
        fi
        if ((pause_ms > 0)); then
            sleep "$((pause_ms / 1000)).$(printf '%03d' $((pause_ms % 1000)))"
        fi
        if (( $(epoch_ms) < hold_until_ms )) && ((${#observed[@]} > 0)); then
            sample_statuses "${case_dir}" "hold-${sample}" "${observed[@]}"
        fi
    done
}

# Nodes cut off from a quorum can neither apply an entry committed after isolation nor, with
# pre-vote, advance their term when no node can reach a quorum.
check_isolation_invariants() {
    local case_dir="$1"
    jq -s -e \
        --slurpfile boundary "${case_dir}/isolation-boundary.json" '
        ($boundary[0]) as $b
        | [.[] | select(.sample != "before" and .sample != "isolated-majority" and .sample != "recovered")] as $during
        | all($during[]; . as $sample
              | if ($b.isolated | index($sample.host)) != null
                then $sample.last_applied <= $b.applied_boundary else true end)
          and all($during[]; . as $sample
              | if $b.term_frozen and (($b.isolated | index($sample.host)) != null)
                then $sample.term <= $b.terms[$sample.host] else true end)
    ' "${case_dir}/samples.ndjson" >/dev/null && return 0
    if [[ ! -e "${case_dir}/.isolation-invariant-violated" ]]; then
        : >"${case_dir}/.isolation-invariant-violated"
        partition_finding "${case_dir}" 'a node cut off from every quorum applied a new entry or advanced its term'
    fi
}

record_isolation_boundary() {
    local case_dir="$1"
    local term_frozen="$2"
    shift 2
    local isolated_hosts=("$@")
    jq -s \
        --argjson term_frozen "${term_frozen}" \
        --argjson isolated "$(printf '%s\n' "${isolated_hosts[@]}" | jq -R . | jq -s 'map(select(length > 0))')" '
        [.[] | select(.sample == "isolated")] as $isolation
        | {isolated: $isolated,
           applied_boundary: ([$isolation[].last_applied] | max),
           terms: ($isolation | map({key: .host, value: .term}) | from_entries),
           term_frozen: $term_frozen}
    ' "${case_dir}/samples.ndjson" >"${case_dir}/isolation-boundary.json"
}

# Reads whether a canary resource exists through one node's public route. Only an explicit
# "does not exist" answer counts as absence; any other failure is retried and then reported.
resource_effect() {
    local name="$1"
    local host="$2"
    local output="$3"
    local attempt
    for attempt in 1 2 3; do
        cli_host="${host}"
        if domain_cli_command "DESCRIBE RESOURCE ${name};" >"${output}" 2>&1; then
            printf 'present\n'
            return 0
        fi
        if grep -Fq "resource '${name}' does not exist" "${output}"; then
            printf 'absent\n'
            return 0
        fi
        sleep 2
    done
    partition_fail product "${host} did not answer whether ${name} exists"
}

reconcile_canaries() {
    local case_dir="$1"
    local canary_file
    for canary_file in "${case_dir}"/control-*.json; do
        [[ -e "${canary_file}" ]] || continue
        local name outcome
        name="$(jq -r '.name' "${canary_file}")"
        outcome="$(jq -r '.outcome' "${canary_file}")"
        local effects=()
        local host effect_on_host
        for host in "${node_hosts[@]}"; do
            effect_on_host="$(resource_effect "${name}" "${host}" "${case_dir}/reconciled-${name}-${host}.txt")"
            effects+=("${effect_on_host}")
        done
        local effect="${effects[0]}"
        local observed
        for observed in "${effects[@]}"; do
            [[ "${observed}" == "${effect}" ]] \
                || partition_finding "${case_dir}" "nodes disagree whether ${name} exists after recovery"
        done
        local updated
        updated="$(mktemp "${case_dir}/.control.XXXXXX")"
        jq --arg effect "${effect}" '.observed_effect = $effect' "${canary_file}" >"${updated}"
        mv "${updated}" "${canary_file}"
        if [[ "${outcome}" == acknowledged && "${effect}" != present ]]; then
            partition_finding "${case_dir}" "acknowledged ${name} is absent after recovery"
        fi
        if [[ "${outcome}" == refused && "${effect}" != absent ]]; then
            partition_finding "${case_dir}" "refused ${name} took effect"
        fi
    done
    jq -s '{canaries: ., acknowledged_effects_present: all(.[]; .outcome != "acknowledged" or .observed_effect == "present")}' \
        "${case_dir}"/control-*.json \
        >"${case_dir}/control-results.json"
}

# The daemon keeps only a short buffer of past events, so node lifecycle events are recorded by a
# subscriber that starts before the fault and runs until recovery.
start_node_event_recorder() {
    local output="$1"
    docker events \
        --filter "label=io.nervix.chaos.run=${run_id}" \
        --filter label=io.nervix.chaos.role=node \
        --format '{{json .}}' >"${output}" 2>&1 &
    partition_event_recorder_pid=$!
}

stop_node_event_recorder() {
    if [[ -n "${partition_event_recorder_pid:-}" ]]; then
        kill "${partition_event_recorder_pid}" 2>/dev/null || true
        wait "${partition_event_recorder_pid}" 2>/dev/null || true
        partition_event_recorder_pid=""
    fi
}

# Each node must keep its container and process incarnation through the case; only the planned
# restart may replace one process, exactly once.
node_incarnations_expected() {
    local case_dir="$1"
    local restarted_host="${2:-}"
    local host
    for host in "${node_hosts[@]}"; do
        local container_id
        container_id="$(owned_service_container "${host}")" || return 1
        inspect_target "${container_id}" "${case_dir}/node-after-${host}.json"
        local expected_start
        if [[ "${host}" == "${restarted_host}" ]]; then
            expected_start="$(jq -r '.[0].State.StartedAt' "${case_dir}/restart/started.json")"
        else
            expected_start="$(jq -r --arg id "${container_id}" '.[] | select(.Id == $id) | .State.StartedAt' \
                "${case_dir}/nodes-before.json")"
        fi
        jq -e --arg id "${container_id}" --arg started "${expected_start}" '
            .[0].Id == $id and .[0].State.Running == true and .[0].State.StartedAt == $started
            and .[0].RestartCount == 0
        ' "${case_dir}/node-after-${host}.json" >/dev/null || return 1
    done
}

partition_node_events_expected() {
    local output="$1"
    local restarted_id="${2:-}"
    jq -s -e --arg restarted "${restarted_id}" '
        [.[] | select(.Action == "die" or .Action == "kill" or .Action == "stop"
                      or .Action == "start" or .Action == "restart" or .Action == "pause")] as $lifecycle
        | if $restarted == "" then ($lifecycle | length == 0)
          else all($lifecycle[]; .Actor.ID == $restarted)
               and ([$lifecycle[] | select(.Action == "kill" and .Actor.Attributes.signal == "9")] | length == 1)
               and ([$lifecycle[] | select(.Action == "die" and .Actor.Attributes.exitCode == "137")] | length == 1)
               and ([$lifecycle[] | select(.Action == "start")] | length == 1)
               and ([$lifecycle[] | select(.Action == "stop" or .Action == "restart" or .Action == "pause")] | length == 0)
          end
    ' "${output}" >/dev/null
}

begin_partition_case() {
    local case_dir="$1"
    local ordinal="$2"
    local case_name="$3"
    mkdir -p "${case_dir}"
    : >"${case_dir}/samples.ndjson"
    phase "partition ${ordinal}/${partition_case_total}: ${case_name}"
    failure_category=product
    case_started_at="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    start_node_event_recorder "${case_dir}/node-events.ndjson"
    wait_for 'settled, fully connected cluster before the partition' 150 \
        observe_partition_roles "${case_dir}/before"
    check_support_containers
    check_other_nodes ""
    verify_node_addresses
    local node_ids=()
    local host
    for host in "${node_hosts[@]}"; do
        node_ids+=("$(owned_service_container "${host}")")
    done
    run_bounded 20 docker inspect "${node_ids[@]}" >"${case_dir}/nodes-before.json"
    local leader_host
    leader_host="nervix-$(jq -r '.leader | ltrimstr("node-")' "${case_dir}/before/role.json")"
    partition_canary "chaos_partition_${ordinal}_before" "${leader_host}" "${case_dir}" 60 before false
    [[ "$(canary_outcome "${case_dir}" "chaos_partition_${ordinal}_before")" == acknowledged ]] \
        || partition_fail product 'the pre-partition control canary was not acknowledged'
    consumer_group_snapshot "${case_dir}/consumer-group-before.json"
    plan_topology "${case_dir}/topology.json"
    probe_links "${case_dir}/links-before.json"
    "${script_dir}/verify-partition-evidence.sh" links "${case_dir}/topology.json" \
        "${case_dir}/links-before.json" "${case_dir}/links-before-verdict.json" --healed \
        || partition_fail setup 'node links were not all open before the partition'
    source_before_fault="$(topic_end_offset chaos_input)"
    output_before_fault="$(topic_end_offset chaos_output)"
    [[ "${source_before_fault}" =~ ^[0-9]+$ && "${output_before_fault}" =~ ^[0-9]+$ ]] \
        || partition_fail setup 'broker offsets were unavailable before the partition'
    # The role must not move between selection and installation.
    observe_partition_roles "${case_dir}/immediately-before" \
        || partition_fail injection 'public roles were unavailable immediately before the partition'
    jq -e --slurpfile before "${case_dir}/before/role.json" '. == $before[0]' \
        "${case_dir}/immediately-before/role.json" >/dev/null \
        || partition_fail injection 'the observed leader or an owner moved before the partition'
    sample_statuses "${case_dir}" before "${node_hosts[@]}"
}

recover_partition() {
    local case_dir="$1"
    local ordinal="$2"
    local rejoining_host="$3"
    local restarted_host="${4:-}"
    phase "partition ${ordinal}: public recovery"
    failure_category=product
    local output_at_heal
    output_at_heal="$(topic_end_offset chaos_output)"
    mkdir -p "${case_dir}/recovered"
    if ! wait_for 'all nodes rejoined, caught up and connected after healing' "${partition_convergence_bound}" \
        cluster_settled_and_connected "${case_dir}/recovered"; then
        # The settlement check stops at the first unsettled node, so record every node's own view.
        sample_statuses "${case_dir}" not-converged "${node_hosts[@]}" || true
        partition_fail product "the cluster did not converge within ${partition_convergence_bound}s after healing"
    fi
    convergence_ms="$(( $(epoch_ms) - heal_completed_ms ))"
    sample_statuses "${case_dir}" recovered "${node_hosts[@]}"
    execution_ms=null
    if wait_for 'Kafka ingestion runs only on the scheduled ingestor owner' "${partition_execution_bound}" \
        ingestion_converged "${case_dir}/recovered"; then
        execution_ms="$(( $(epoch_ms) - heal_completed_ms ))"
    else
        partition_finding "${case_dir}" 'execution did not converge on the current schedule after healing'
    fi
    wait_for 'sink output advanced after healing' "${partition_delivery_after_heal_bound}" \
        topic_progressed chaos_output "${output_at_heal}" \
        || partition_fail product 'sink output did not advance after healing'
    delivery_after_heal_ms="$(( $(epoch_ms) - heal_completed_ms ))"
    partition_canary "chaos_partition_${ordinal}_after" "${rejoining_host}" "${case_dir}" 60 after true
    [[ "$(canary_outcome "${case_dir}" "chaos_partition_${ordinal}_after")" == acknowledged ]] \
        || partition_fail product 'the post-heal control canary through the rejoined node was not acknowledged'
    reconcile_canaries "${case_dir}"
    stop_node_event_recorder
    local restarted_id=""
    if [[ -n "${restarted_host}" ]]; then
        restarted_id="$(jq -r '.[0].Id' "${case_dir}/restart/started.json")"
    fi
    if ! node_incarnations_expected "${case_dir}" "${restarted_host}" \
        || ! partition_node_events_expected "${case_dir}/node-events.ndjson" "${restarted_id}"; then
        partition_finding "${case_dir}" 'a Nervix node restarted or stopped outside the planned fault'
    fi
    check_support_containers
    check_other_nodes ""
    local host
    for host in "${node_hosts[@]}"; do
        capture_metrics "${host}"
        cp "${artifact_dir}/public/metrics-${host}.txt" "${case_dir}/metrics-${host}.txt"
    done
    local observer_id
    observer_id="$(owned_service_container observer)" || return 1
    run_bounded 20 docker logs --since "${case_started_at}" "${observer_id}" \
        >"${case_dir}/observer.log" 2>&1
    trim_file "${case_dir}/observer.log" 1048576
}

write_partition_result() {
    local case_dir="$1"
    local case_name="$2"
    shift 2
    local source_after output_after
    source_after="$(topic_end_offset chaos_input)"
    output_after="$(topic_end_offset chaos_output)"
    jq -n \
        --arg case "${case_name}" \
        --slurpfile plan "${case_dir}/plan.json" \
        --slurpfile role "${case_dir}/immediately-before/role.json" \
        --slurpfile boundary "${case_dir}/isolation-boundary.json" \
        --slurpfile control "${case_dir}/control-results.json" \
        --arg fault_started_at "${fault_started_at}" \
        --arg isolation_verified_at "${isolation_verified_at}" \
        --arg heal_completed_at "${heal_completed_at}" \
        --argjson install_ms "$((rules_installed_ms - fault_started_ms))" \
        --argjson isolation_window_ms "$((heal_started_ms - isolation_verified_ms))" \
        --argjson heal_ms "$((heal_completed_ms - heal_started_ms))" \
        --argjson convergence_ms "${convergence_ms}" \
        --argjson execution_ms "${execution_ms}" \
        --argjson delivery_after_heal_ms "${delivery_after_heal_ms}" \
        --argjson minimum_window_seconds "${partition_seconds}" \
        --argjson source_before "${source_before_fault}" \
        --argjson output_before "${output_before_fault}" \
        --argjson source_after "${source_after}" \
        --argjson output_after "${output_after}" \
        --argjson details "$1" \
        '{case:$case,placement:$plan[0].placement,intended_blocked:$plan[0].intended_blocked,roles:$role[0],isolation_boundary:$boundary[0],fault_started_at:$fault_started_at,isolation_verified_at:$isolation_verified_at,heal_completed_at:$heal_completed_at,install_ms:$install_ms,isolation_window_ms:$isolation_window_ms,minimum_isolation_window_ms:($minimum_window_seconds * 1000),heal_ms:$heal_ms,convergence_after_heal_ms:$convergence_ms,execution_convergence_after_heal_ms:$execution_ms,delivery_after_heal_ms:$delivery_after_heal_ms,source_offsets:{before:$source_before,after:$source_after},output_offsets:{before:$output_before,after:$output_after},control:$control[0],links:{before:"links-before.json",isolated:"links-isolated.json",healed:"links-healed.json"},samples:"samples.ndjson",node_events:"node-events.ndjson",placement:"placement-before-heal/placement.json",findings:"findings.ndjson"} + $details' \
        >"${case_dir}/result.json"
    local isolation_window_ms=$((heal_started_ms - isolation_verified_ms))
    ((isolation_window_ms >= partition_seconds * 1000)) \
        || partition_fail controller 'the verified isolation window was shorter than declared'
}

# Work owned by a node whose links stayed healthy must not fail over. Only the named nodes, whose
# links the fault cut, may lose their work.
placement_stable() {
    local case_dir="$1"
    local observer="$2"
    shift 2
    local moving=("$@")
    local output_dir="${case_dir}/placement-before-heal"
    mkdir -p "${output_dir}"
    cli_host="${observer}"
    local kind
    local owners=()
    for kind in ingestor relay emitter; do
        local entity="chaos_${kind}"
        [[ "${kind}" == relay ]] && entity=chaos_records
        domain_cli_command "DESCRIBE ${kind^^} ${entity};" >"${output_dir}/${kind}.txt" 2>&1 \
            || partition_fail product "${observer} did not describe the ${kind} while the fault was held"
        owners+=("$(owner_from_description "${output_dir}/${kind}.txt")")
    done
    jq -n \
        --slurpfile role "${case_dir}/immediately-before/role.json" \
        --arg observer "${observer}" \
        --arg ingestor "${owners[0]}" --arg relay "${owners[1]}" --arg emitter "${owners[2]}" \
        --argjson moving "$(printf '%s\n' "${moving[@]}" | jq -R . | jq -s 'map(select(length > 0) | "node-" + ltrimstr("nervix-"))')" '
        {observer: $observer, moving_nodes: $moving,
         before: {ingestor: $role[0].ingestor_owner, relay: $role[0].relay_owner, emitter: $role[0].emitter_owner},
         held: {ingestor: $ingestor, relay: $relay, emitter: $emitter}}
        | .moved_from_healthy_nodes = [.before | to_entries[] | . as $entry
            | select(($moving | index($entry.value)) == null)
            | select($entry.value != $ARGS.named[$entry.key])
            | {kind: $entry.key, from: $entry.value, to: $ARGS.named[$entry.key]}]
    ' >"${output_dir}/placement.json"
    if [[ "$(jq '.moved_from_healthy_nodes | length' "${output_dir}/placement.json")" -ne 0 ]]; then
        partition_finding "${case_dir}" "work failed over from a node whose links stayed healthy: $(jq -c '.moved_from_healthy_nodes' "${output_dir}/placement.json")"
    fi
}

majority_host_other_than() {
    local excluded="$1"
    local leader_file="$2"
    local leader
    leader="$(<"${leader_file}")"
    printf 'nervix-%s\n' "${leader##*-}"
    [[ "nervix-${leader##*-}" != "${excluded}" ]]
}

# The isolated node restarts inside the partition. Its rules live on its peers and its address is
# fixed for the run, so the same isolation must hold for the new process.
restart_isolated_node() {
    local case_dir="$1"
    local host="$2"
    local restart_dir="${case_dir}/restart"
    mkdir -p "${restart_dir}"
    phase "restart of isolated ${host} inside the partition"
    failure_category=injection
    local container_id container_name
    container_id="$(owned_service_container "${host}")" || return 1
    inspect_target "${container_id}" "${restart_dir}/before.json"
    "${script_dir}/verify-crash-evidence.sh" before "${run_id}" "${project_name}" \
        "${host}" "${image_id}" "${restart_dir}/before.json" "${restart_dir}/before.json"
    container_name="$(jq -r '.[0].Name | ltrimstr("/")' "${restart_dir}/before.json")"
    run_bounded 20 docker run --rm \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=fault \
        --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        "${pumba_image_id}" --dry-run --log-level info \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=node \
        kill --signal SIGKILL --limit 1 "${container_name}" \
        >"${restart_dir}/pumba-dry-run.txt" 2>&1
    [[ "$(grep -Fc 'msg="killing container"' "${restart_dir}/pumba-dry-run.txt")" -eq 1 ]] \
        && grep -Fq "dryrun=true id=${container_id}" "${restart_dir}/pumba-dry-run.txt" \
        || partition_fail injection "Pumba kill dry run did not select exactly ${host}"
    local kill_since
    kill_since="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    partition_restart_id="${container_id}"
    partition_restart_started=false
    run_bounded 30 docker run --rm \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=fault \
        --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        "${pumba_image_id}" --log-level info \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=node \
        kill --signal SIGKILL --limit 1 "${container_name}" \
        >"${restart_dir}/pumba.txt" 2>&1
    inspect_target "${container_id}" "${restart_dir}/killed.json"
    run_bounded 20 docker events --since "${kill_since}" \
        --until "$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)" \
        --filter "container=${container_id}" --format '{{json .}}' \
        >"${restart_dir}/kill-events.ndjson"
    "${script_dir}/verify-crash-evidence.sh" killed "${run_id}" "${project_name}" \
        "${host}" "${image_id}" "${restart_dir}/before.json" \
        "${restart_dir}/killed.json" "${restart_dir}/kill-events.ndjson"
    run_bounded 30 docker start "${container_id}" >"${restart_dir}/docker-start.txt"
    partition_restart_started=true
    inspect_target "${container_id}" "${restart_dir}/started.json"
    "${script_dir}/verify-crash-evidence.sh" started "${run_id}" "${project_name}" \
        "${host}" "${image_id}" "${restart_dir}/before.json" "${restart_dir}/started.json"
    [[ "$(container_address "${container_id}")" == "$(node_address "${host}")" ]] \
        || partition_fail injection "restarted ${host} did not keep its planned address"
    mkdir -p "${restart_dir}/rules"
    rules_match_plan "${case_dir}/plan.json" "${restart_dir}/rules" \
        || partition_fail injection 'peer-side rules changed across the isolated node restart'
    probe_links "${restart_dir}/links.json"
    "${script_dir}/verify-partition-evidence.sh" links "${case_dir}/plan.json" \
        "${restart_dir}/links.json" "${restart_dir}/links-verdict.json" \
        || partition_fail injection 'the isolation did not survive the isolated node restart'
    failure_category=product
    local answered=false
    local attempt
    for attempt in $(seq 1 60); do
        if partition_node_status "${host}" "${restart_dir}/status-${host}.txt"; then
            answered=true
            break
        fi
        sleep 2
    done
    [[ "${answered}" == true ]] \
        || partition_fail product "restarted ${host} did not answer its public status route while isolated"
    jq -c '. + {sample: "restarted"}' "${restart_dir}/status-${host}.json" >>"${case_dir}/samples.ndjson"
}

partition_case_follower() {
    local ordinal="$1"
    local case_dir="${artifact_dir}/partitions/${ordinal}-follower"
    mkdir -p "${case_dir}"
    begin_partition_case "${case_dir}" "${ordinal}" 'isolation of an observed follower that owns execution'
    local follower
    follower="$(jq -r '.follower' "${case_dir}/before/role.json")"
    local isolated="nervix-${follower##*-}"
    plan_isolation follower "${isolated}" "${case_dir}/plan.json"
    local survivors=()
    local host
    for host in "${node_hosts[@]}"; do
        [[ "${host}" != "${isolated}" ]] && survivors+=("${host}")
    done
    install_partition "${case_dir}" "${ordinal}"

    failure_category=product
    sample_statuses "${case_dir}" isolated "${isolated}" "${survivors[@]}"
    record_isolation_boundary "${case_dir}" false "${isolated}"
    wait_for 'the connected majority agrees on a caught-up leader' "${partition_majority_leader_bound}" \
        survivors_settled "${isolated}" "${case_dir}" \
        || partition_fail product 'the majority did not agree on a leader while the follower was isolated'
    local majority_leader_host
    majority_leader_host="$(majority_host_other_than "${isolated}" "${case_dir}/survivor-leader.txt")" \
        || partition_fail product 'the majority named the isolated follower as its leader'
    partition_canary "chaos_partition_${ordinal}_majority" "${majority_leader_host}" "${case_dir}" \
        "${partition_majority_commit_bound}" during false
    [[ "$(canary_outcome "${case_dir}" "chaos_partition_${ordinal}_majority")" == acknowledged ]] \
        || partition_fail product 'the connected majority did not acknowledge a control command'
    sample_statuses "${case_dir}" isolated-majority "${survivors[@]}"
    jq -s -e --slurpfile boundary "${case_dir}/isolation-boundary.json" \
        '[.[] | select(.sample == "isolated-majority") | .last_applied] | max > $boundary[0].applied_boundary' \
        "${case_dir}/samples.ndjson" >/dev/null \
        || partition_finding "${case_dir}" 'the majority acknowledged a command without applying past the isolation boundary'
    partition_canary "chaos_partition_${ordinal}_minority" "${isolated}" "${case_dir}" \
        "${partition_minority_attempt_bound}" during true
    failover_ms=null
    if wait_for 'the majority moved the isolated follower work to connected nodes' "${partition_failover_bound}" \
        surviving_owners_ready "${isolated}" "${case_dir}" "${majority_leader_host}"; then
        failover_ms="$(( $(epoch_ms) - isolation_verified_ms ))"
    else
        partition_finding "${case_dir}" 'the majority did not fail over the isolated follower work'
    fi
    local output_after_failover
    output_after_failover="$(topic_end_offset chaos_output)"
    majority_delivery_ms=null
    if wait_for 'sink output advanced on the majority while the follower was isolated' \
        "${partition_majority_delivery_bound}" topic_progressed chaos_output "${output_after_failover}"; then
        majority_delivery_ms="$(( $(epoch_ms) - isolation_verified_ms ))"
    else
        partition_finding "${case_dir}" 'the majority did not deliver output while the follower was isolated'
    fi
    consumer_group_snapshot "${case_dir}/consumer-group-isolated.json"
    sample_statuses "${case_dir}" during "${isolated}"
    hold_partition "${case_dir}" "${isolated}"
    placement_stable "${case_dir}" "${majority_leader_host}" "${isolated}"

    restart_isolated_node "${case_dir}" "${isolated}"
    check_isolation_invariants "${case_dir}"
    heal_partition "${case_dir}" "${ordinal}"
    recover_partition "${case_dir}" "${ordinal}" "${isolated}" "${isolated}"
    write_partition_result "${case_dir}" follower "$(jq -n \
        --arg isolated "${isolated}" \
        --arg majority_leader "node-${majority_leader_host##*-}" \
        --argjson failover_ms "${failover_ms}" \
        --argjson majority_delivery_ms "${majority_delivery_ms}" \
        '{isolated:$isolated,majority_leader:$majority_leader,failover_after_isolation_ms:$failover_ms,majority_delivery_after_isolation_ms:$majority_delivery_ms,isolated_restart:"restart"}')"
}

partition_case_leader() {
    local ordinal="$1"
    local case_dir="${artifact_dir}/partitions/${ordinal}-leader"
    mkdir -p "${case_dir}"
    begin_partition_case "${case_dir}" "${ordinal}" 'isolation of the observed leader'
    local leader
    leader="$(jq -r '.leader' "${case_dir}/before/role.json")"
    local isolated="nervix-${leader##*-}"
    plan_isolation leader "${isolated}" "${case_dir}/plan.json"
    local survivors=()
    local host
    for host in "${node_hosts[@]}"; do
        [[ "${host}" != "${isolated}" ]] && survivors+=("${host}")
    done
    install_partition "${case_dir}" "${ordinal}"

    failure_category=product
    sample_statuses "${case_dir}" isolated "${isolated}" "${survivors[@]}"
    record_isolation_boundary "${case_dir}" false "${isolated}"
    # The isolated former leader can only believe it still leads; any command it acknowledges would
    # be a commit no quorum made.
    partition_canary "chaos_partition_${ordinal}_minority" "${isolated}" "${case_dir}" \
        "${partition_minority_attempt_bound}" during false
    [[ "$(canary_outcome "${case_dir}" "chaos_partition_${ordinal}_minority")" != acknowledged ]] \
        || partition_finding "${case_dir}" 'the isolated former leader acknowledged a command without a quorum'
    wait_for 'the connected majority elected a caught-up leader' "${partition_majority_leader_bound}" \
        survivors_settled "${isolated}" "${case_dir}" \
        || partition_fail product 'the majority did not elect a leader while the former leader was isolated'
    election_ms="$(( $(epoch_ms) - isolation_verified_ms ))"
    local majority_leader_host
    majority_leader_host="$(majority_host_other_than "${isolated}" "${case_dir}/survivor-leader.txt")" \
        || partition_fail product 'the majority still named the isolated node as its leader'
    partition_canary "chaos_partition_${ordinal}_majority" "${majority_leader_host}" "${case_dir}" \
        "${partition_majority_commit_bound}" during false
    [[ "$(canary_outcome "${case_dir}" "chaos_partition_${ordinal}_majority")" == acknowledged ]] \
        || partition_fail product 'the newly elected majority did not acknowledge a control command'
    sample_statuses "${case_dir}" isolated-majority "${survivors[@]}"
    jq -s -e --slurpfile boundary "${case_dir}/isolation-boundary.json" \
        '[.[] | select(.sample == "isolated-majority") | .last_applied] | max > $boundary[0].applied_boundary' \
        "${case_dir}/samples.ndjson" >/dev/null \
        || partition_finding "${case_dir}" 'the new majority acknowledged a command without applying past the isolation boundary'
    failover_ms=null
    if wait_for 'the majority moved the former leader work to connected nodes' "${partition_failover_bound}" \
        surviving_owners_ready "${isolated}" "${case_dir}" "${majority_leader_host}"; then
        failover_ms="$(( $(epoch_ms) - isolation_verified_ms ))"
    else
        partition_finding "${case_dir}" 'the majority did not fail over the isolated former leader work'
    fi
    consumer_group_snapshot "${case_dir}/consumer-group-isolated.json"
    sample_statuses "${case_dir}" during "${isolated}"
    local stale_label=false
    if [[ "$(jq -r '.leader' "${case_dir}/during/status-${isolated}.json")" == "node-${isolated##*-}" ]]; then
        stale_label=true
    fi
    hold_partition "${case_dir}" "${isolated}"
    placement_stable "${case_dir}" "${majority_leader_host}" "${isolated}"

    heal_partition "${case_dir}" "${ordinal}"
    recover_partition "${case_dir}" "${ordinal}" "${isolated}"
    write_partition_result "${case_dir}" leader "$(jq -n \
        --arg isolated "${isolated}" \
        --arg majority_leader "node-${majority_leader_host##*-}" \
        --argjson election_ms "${election_ms}" \
        --argjson failover_ms "${failover_ms}" \
        --argjson stale_label "${stale_label}" \
        '{isolated:$isolated,majority_leader:$majority_leader,election_after_isolation_ms:$election_ms,failover_after_isolation_ms:$failover_ms,isolated_node_reported_stale_leader_label:$stale_label,consumer_group_during:"consumer-group-isolated.json"}')"
}

# A quorum-backed leader exists when it reports itself leader and a majority of the nodes agree on
# it and its term.
quorum_leader_agreed() {
    local output_dir="$1"
    mkdir -p "${output_dir}"
    local host
    for host in "${node_hosts[@]}"; do
        partition_node_status "${host}" "${output_dir}/status-${host}.txt" || return 1
    done
    jq -s -e --argjson quorum "$(( ${#node_hosts[@]} / 2 + 1 ))" '
        . as $statuses
        | [$statuses[] | select(.state == "Leader")] as $leaders
        | any($leaders[]; . as $leader
              | ([$statuses[] | select(.leader == $leader.node and .term == $leader.term)] | length) >= $quorum)
    ' "${output_dir}"/status-*.json >/dev/null
}

partition_case_asymmetric() {
    local ordinal="$1"
    local case_dir="${artifact_dir}/partitions/${ordinal}-asymmetric"
    mkdir -p "${case_dir}"
    begin_partition_case "${case_dir}" "${ordinal}" 'one-way loss from the observed leader to a follower'
    local leader follower
    leader="$(jq -r '.leader' "${case_dir}/before/role.json")"
    follower="$(jq -r '.follower' "${case_dir}/before/role.json")"
    local sender="nervix-${leader##*-}"
    local receiver="nervix-${follower##*-}"
    plan_one_way "${sender}" "${receiver}" "${case_dir}/plan.json"
    local bystander=""
    local host
    for host in "${node_hosts[@]}"; do
        if [[ "${host}" != "${sender}" && "${host}" != "${receiver}" ]]; then
            bystander="${host}"
        fi
    done
    install_partition "${case_dir}" "${ordinal}"

    failure_category=product
    sample_statuses "${case_dir}" isolated "${node_hosts[@]}"
    record_isolation_boundary "${case_dir}" false
    wait_for 'a majority agrees on a quorum-backed leader' "${partition_majority_leader_bound}" \
        quorum_leader_agreed "${case_dir}/quorum" \
        || partition_fail product 'no quorum-backed leader while one link lost packets in one direction'
    jq -c '. + {sample: "quorum"}' "${case_dir}"/quorum/status-*.json >>"${case_dir}/samples.ndjson"
    partition_canary "chaos_partition_${ordinal}_majority" "${bystander}" "${case_dir}" \
        "${partition_majority_commit_bound}" during true
    [[ "$(canary_outcome "${case_dir}" "chaos_partition_${ordinal}_majority")" == acknowledged ]] \
        || partition_fail product 'the fully connected majority did not acknowledge a control command'
    partition_canary "chaos_partition_${ordinal}_receiver" "${receiver}" "${case_dir}" \
        "${partition_minority_attempt_bound}" during true
    consumer_group_snapshot "${case_dir}/consumer-group-isolated.json"
    sample_statuses "${case_dir}" during "${node_hosts[@]}"
    hold_partition "${case_dir}"
    # Whichever endpoint leads can no longer probe the other, so either endpoint may lose its work;
    # the bystander's links stayed healthy both ways.
    placement_stable "${case_dir}" "${bystander}" "${sender}" "${receiver}"

    heal_partition "${case_dir}" "${ordinal}"
    recover_partition "${case_dir}" "${ordinal}" "${receiver}"
    write_partition_result "${case_dir}" asymmetric "$(jq -n \
        --arg sender "${sender}" \
        --arg receiver "${receiver}" \
        --arg bystander "${bystander}" \
        '{blocked_direction:{from:$sender,to:$receiver},bystander:$bystander,quorum_leader:"quorum/status-*.json"}')"
}

partition_case_quorum_loss() {
    local ordinal="$1"
    local case_dir="${artifact_dir}/partitions/${ordinal}-quorum-loss"
    mkdir -p "${case_dir}"
    plan_quorum_loss "${case_dir}/plan.json"
    begin_partition_case "${case_dir}" "${ordinal}" 'no two nodes can communicate'
    install_partition "${case_dir}" "${ordinal}"

    failure_category=product
    sample_statuses "${case_dir}" isolated "${node_hosts[@]}"
    record_isolation_boundary "${case_dir}" true "${node_hosts[@]}"
    # Without a quorum every command is unavailable. An acknowledgement from any node would report a
    # commit that no quorum could have made.
    local attempts=()
    local host
    for host in "${node_hosts[@]}"; do
        partition_canary "chaos_partition_${ordinal}_${host//-/_}" "${host}" "${case_dir}" \
            "${partition_minority_attempt_bound}" during true &
        attempts+=("$!")
    done
    local attempt
    for attempt in "${attempts[@]}"; do
        wait "${attempt}" || partition_fail controller 'a quorum-loss control attempt could not be recorded'
    done
    for host in "${node_hosts[@]}"; do
        [[ "$(canary_outcome "${case_dir}" "chaos_partition_${ordinal}_${host//-/_}")" != acknowledged ]] \
            || partition_finding "${case_dir}" "${host} acknowledged a command while no quorum could communicate"
    done
    consumer_group_snapshot "${case_dir}/consumer-group-isolated.json"
    sample_statuses "${case_dir}" during "${node_hosts[@]}"
    hold_partition "${case_dir}" "${node_hosts[@]}"

    heal_partition "${case_dir}" "${ordinal}"
    recover_partition "${case_dir}" "${ordinal}" "nervix-1"
    write_partition_result "${case_dir}" quorum-loss "$(jq -n \
        --slurpfile boundary "${case_dir}/isolation-boundary.json" \
        '{no_term_advanced:true,applied_boundary:$boundary[0].applied_boundary}')"
}

run_partition_recovery() {
    phase 'partition continuous traffic startup'
    mkdir -p "${artifact_dir}/partitions/initial"
    jq -nc --arg run_id "${run_id}" --argjson count "${record_count}" \
        -f "${fixture_generator}" >"${artifact_dir}/fixtures/input.ndjson"
    [[ "$(wc -l <"${artifact_dir}/fixtures/input.ndjson")" -eq "${record_count}" ]] \
        || partition_fail controller 'partition fixture generation was incomplete'
    compose up --detach --no-deps load observer
    wait_for 'independent load, observer and broker' 30 check_support_containers
    wait_for 'source traffic started' 30 topic_progressed chaos_input 0
    wait_for 'sink traffic started' 60 topic_progressed chaos_output 0
    wait_for 'initial cross-node traffic metrics' 60 initial_remote_path_ready
    local host
    for host in "${node_hosts[@]}"; do
        cp "${artifact_dir}/public/metrics-${host}.txt" \
            "${artifact_dir}/partitions/initial/metrics-${host}.txt"
    done
    local remote_path_tmp
    remote_path_tmp="$(mktemp "${artifact_dir}/results/.remote-path.XXXXXX")"
    jq '.traffic_observed_before_partitions = true | .traffic_evidence = ["partitions/initial/metrics-nervix-1.txt", "partitions/initial/metrics-nervix-2.txt", "partitions/initial/metrics-nervix-3.txt"]' \
        "${artifact_dir}/results/remote-path.json" >"${remote_path_tmp}"
    mv "${remote_path_tmp}" "${artifact_dir}/results/remote-path.json"

    local cases=()
    if [[ "${partition_case}" == all ]]; then
        cases=(follower asymmetric leader quorum-loss)
    else
        cases=("${partition_case}")
    fi
    partition_case_total="${#cases[@]}"
    local ordinal=0
    local case_name
    for case_name in "${cases[@]}"; do
        ordinal=$((ordinal + 1))
        case "${case_name}" in
            follower) partition_case_follower "${ordinal}" ;;
            leader) partition_case_leader "${ordinal}" ;;
            asymmetric) partition_case_asymmetric "${ordinal}" ;;
            quorum-loss) partition_case_quorum_loss "${ordinal}" ;;
        esac
    done

    phase 'partition final traffic boundary'
    touch "${artifact_dir}/traffic/stop-load"
    local load_id
    load_id="$(owned_service_container load)" || return 1
    wait_for 'load producer flushed and exited' 40 load_exited_cleanly "${load_id}"
    producer_status="$(run_bounded 20 docker inspect --format '{{.State.ExitCode}}' "${load_id}")"
    run_bounded 20 docker logs "${load_id}" >"${artifact_dir}/traffic/producer.log" 2>&1
    trim_file "${artifact_dir}/traffic/producer.log" 1048576
    input_end="$(topic_end_offset chaos_input)"
    [[ "${input_end}" =~ ^[0-9]+$ && "${input_end}" -le "${record_count}" ]] \
        || partition_fail product 'source boundary exceeded the bounded fixture'
    kcat -q -b broker:9092 -C -t chaos_input -p 0 -o beginning -c "${input_end}" \
        >"${artifact_dir}/traffic/accepted-input.ndjson" \
        2>"${artifact_dir}/traffic/source-consumer.stderr"
    [[ "$(wc -l <"${artifact_dir}/traffic/accepted-input.ndjson")" -eq "${input_end}" ]] \
        || partition_fail product 'accepted-input ledger did not cover the final source boundary'
    jq -s \
        --argjson source_end "${input_end}" \
        --argjson minimum_window_seconds "${partition_seconds}" \
        '{configured_liveness:{raft_heartbeat_interval:"250ms",raft_election_timeout_min:"1500ms",raft_election_timeout_max:"3000ms",node_unavailability_timeout:"10s"},minimum_isolation_window_seconds:$minimum_window_seconds,source_final_boundary:$source_end,cases:.}' \
        "${artifact_dir}"/partitions/[1-9]-*/result.json \
        >"${artifact_dir}/results/partition-progress.json"
}
