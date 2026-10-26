#!/usr/bin/env bash
# Sourced by run-baseline.sh after the external cluster and graph are ready. SIGKILLs every Nervix
# node at once, starts each from its own container, image and volume, and checks the committed
# configuration, ownership against the voter observation grace, and delivery of every accepted
# record. The evidence is process-crash evidence: the host, its page cache and its disks survive.

# shellcheck source=recovery-scenario.sh
source "${script_dir}/recovery-scenario.sh"

# How long a leader's automatic scheduling waits after its process starts for gossip to observe
# every voter before it fails over the work of a voter it has not heard from. This is the fixed
# product bound of Planned Ownership Handoffs And Failover in the control-plane chapter; the result
# records it beside the configured liveness settings.
cluster_restart_voter_grace_ms=10000
# External budgets, in seconds.
cluster_restart_listener_bound=120
cluster_restart_settle_bound=150
cluster_restart_delivery_bound=90
cluster_restart_sampler_seconds=40

# Samples every node's own SHOW CLUSTER STATUS from one administration container for SECONDS,
# stamping each answer when it arrives with the container's clock, which is the host's, so a state
# is never dated before it was observed. It runs in the background, so sampling is already running
# when the nodes start.
start_status_sampler() {
    local transcript="$1"
    local seconds="$2"
    cluster_restart_sampler_name="${project_name}-status-sampler"
    timeout --kill-after=5s "$((seconds + 60))s" \
        docker compose "${compose_args[@]}" run --rm --no-deps -T \
        --name "${cluster_restart_sampler_name}" admin \
        sh -c '
            seconds="$1"
            shift
            end=$(( $(date +%s) + seconds ))
            while [ "$(date +%s)" -lt "${end}" ]; do
                for host in "$@"; do
                    answer="$(timeout 5 nervix-cli --server "http://${host}:47391" \
                        --password "${NERVIX_PASSWORD}" --command "SHOW CLUSTER STATUS;" 2>&1)"
                    printf "=== status %s %s\n%s\n" "${host}" "$(date +%s%N)" "${answer}"
                done
                sleep 0.2
            done
        ' sh "${seconds}" "${node_hosts[@]}" </dev/null >"${transcript}" 2>"${transcript%.log}.stderr" &
    cluster_restart_sampler_pid=$!
}

# Splits the sampler transcript into one status record per answer.
parse_status_samples() {
    local transcript="$1"
    local output="$2"
    local split_dir
    split_dir="$(mktemp -d "${artifact_dir}/.status-samples.XXXXXX")"
    awk -v dir="${split_dir}" '
        /^=== status / {
            if (answer != "") close(answer)
            count++
            answer = sprintf("%s/%06d", dir, count)
            printf "" >answer
            print count, $3, $4 >>(dir "/index")
            next
        }
        answer != "" { print >answer }
    ' "${transcript}"
    : >"${output}"
    if [[ -s "${split_dir}/index" ]]; then
        local sample host at_ns
        while read -r sample host at_ns; do
            status_record "$(printf '%s/%06d' "${split_dir}" "${sample}")" "${host}" "${at_ns}" \
                >>"${output}"
        done <"${split_dir}/index"
    fi
    rm -rf "${split_dir}"
}

# Maps each node to the Docker start event of its container, read from the live recording.
node_start_times() {
    local events="$1"
    local output="$2"
    local nodes_by_container='{}'
    local host
    for host in "${node_hosts[@]}"; do
        nodes_by_container="$(jq -c \
            --arg container_id "$(owned_service_container "${host}")" \
            --arg node "node-${host##*-}" \
            '. + {($container_id): $node}' <<<"${nodes_by_container}")"
    done
    jq -s --argjson nodes "${nodes_by_container}" '
        [.[] | select(.Action == "start") | {node: $nodes[.Actor.ID], at_ns: .timeNano}
         | select(.node != null)]
        | group_by(.node) | map({key: .[0].node, value: (map(.at_ns) | min)}) | from_entries
    ' "${events}" >"${output}"
}

run_cluster_restart() {
    local case_dir="${artifact_dir}/restart"
    mkdir -p "${case_dir}"
    recovery_traffic_startup "${case_dir}"

    failure_category=product
    phase 'cluster restart: settled cluster and committed configuration'
    wait_for 'settled, connected cluster before the crash' 150 \
        observe_partition_roles "${case_dir}/before"
    local leader_host
    leader_host="nervix-$(jq -r '.leader | ltrimstr("node-")' "${case_dir}/before/role.json")"
    local outcome
    outcome="$(configuration_change "${case_dir}" chaos_restart_before "${leader_host}" chaos_baseline \
        'CREATE RESOURCE chaos_restart_before;' "created resource 'chaos_restart_before'")"
    [[ "${outcome}" == acknowledged ]] \
        || recovery_fail product "the control canary before the crash was ${outcome}, not acknowledged"
    local host
    for host in "${node_hosts[@]}"; do
        wait_for "committed configuration through ${host} before the crash" 60 \
            capture_configuration "${host}" "${case_dir}/configuration-before/${host}" chaos_restart_before
        [[ -z "$(configuration_differences "${case_dir}/configuration-before/${node_hosts[0]}.json" \
            "${case_dir}/configuration-before/${host}.json")" ]] \
            || recovery_fail product "${host} disagrees with ${node_hosts[0]} on committed configuration before the crash"
        jq -e '.resources.chaos_restart_before == "present"' \
            "${case_dir}/configuration-before/${host}.json" >/dev/null \
            || recovery_fail product "the acknowledged control canary is absent through ${host} before the crash"
    done
    jq -n \
        --slurpfile role "${case_dir}/before/role.json" \
        --argjson voters "$(printf '%s\n' "${node_names[@]}" | jq -R . | jq -s .)" \
        '{leader: $role[0].leader,
          owners: {ingestor: $role[0].ingestor_owner, relay: $role[0].relay_owner, emitter: $role[0].emitter_owner},
          voters: $voters}' >"${case_dir}/owners-before.json"
    check_support_containers
    check_other_nodes ""

    phase "cluster restart: SIGKILL of every node (${node_count})"
    failure_category=injection
    local container_ids=()
    local container_names=()
    for host in "${node_hosts[@]}"; do
        local container_id
        container_id="$(owned_service_container "${host}")" || return 1
        inspect_target "${container_id}" "${case_dir}/selected-${host}.json"
        "${script_dir}/verify-crash-evidence.sh" before "${run_id}" "${project_name}" \
            "${host}" "${image_id}" "${case_dir}/selected-${host}.json" "${case_dir}/selected-${host}.json"
        container_ids+=("${container_id}")
        container_names+=("$(jq -r '.[0].Name | ltrimstr("/")' "${case_dir}/selected-${host}.json")")
    done
    run_bounded 20 docker run --rm \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=fault \
        --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        "${pumba_image_id}" --dry-run --log-level info \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=node \
        kill --signal SIGKILL "${container_names[@]}" \
        >"${case_dir}/pumba-dry-run.txt" 2>&1
    [[ "$(grep -Fc 'msg="killing container"' "${case_dir}/pumba-dry-run.txt")" -eq "${node_count}" ]] \
        || recovery_fail injection 'the Pumba dry run did not select exactly every node container'
    local index
    for index in "${!container_ids[@]}"; do
        grep -Fq "dryrun=true id=${container_ids[${index}]}" "${case_dir}/pumba-dry-run.txt" \
            || recovery_fail injection "the Pumba dry run did not select ${node_hosts[${index}]}"
    done
    run_bounded 20 docker inspect "${container_ids[@]}" >"${case_dir}/before-all-nodes.json"
    record_node_volumes "${case_dir}/volumes-before.json"
    local source_before output_before
    source_before="$(topic_end_offset chaos_input)"
    output_before="$(topic_end_offset chaos_output)"
    [[ "${source_before}" =~ ^[0-9]+$ && "${output_before}" =~ ^[0-9]+$ ]] \
        || recovery_fail setup 'broker offsets were unavailable before the crash'
    local fault_since fault_since_ns kill_requested_ms kill_completed_ms
    fault_since="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    fault_since_ns="$(date -d "${fault_since}" +%s%N)"
    kill_requested_ms="$(epoch_ms)"
    jq -n \
        --arg image "${pumba_image_id}" \
        --arg run_id "${run_id}" \
        --arg fault_since "${fault_since}" \
        --argjson targets "$(printf '%s\n' "${container_names[@]}" | jq -R . | jq -s .)" \
        --argjson container_ids "$(printf '%s\n' "${container_ids[@]}" | jq -R . | jq -s .)" \
        --argjson outage_seconds "${outage_seconds}" \
        --argjson source_offset "${source_before}" \
        --argjson output_offset "${output_before}" \
        '{pumba_image_id:$image,command:(["pumba","--label","io.nervix.chaos.run="+$run_id,"--label","io.nervix.chaos.role=node","kill","--signal","SIGKILL"] + $targets),targets:$targets,container_ids:$container_ids,fault_since:$fault_since,outage_seconds:$outage_seconds,source_offset_before:$source_offset,output_offset_before:$output_offset}' \
        >"${case_dir}/fault-command.json"
    for index in "${!container_ids[@]}"; do
        recovery_node_stopped "${container_ids[${index}]}"
    done
    run_bounded 30 docker run --rm \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=fault \
        --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        "${pumba_image_id}" --log-level info \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=node \
        kill --signal SIGKILL "${container_names[@]}" \
        >"${case_dir}/pumba.txt" 2>&1
    kill_completed_ms="$(epoch_ms)"
    for index in "${!container_ids[@]}"; do
        host="${node_hosts[${index}]}"
        grep -Fq "dryrun=false id=${container_ids[${index}]}" "${case_dir}/pumba.txt" \
            || recovery_fail injection "Pumba did not report ${host} killed"
        inspect_target "${container_ids[${index}]}" "${case_dir}/killed-${host}.json"
        node_event_window "${fault_since_ns}" "${container_ids[${index}]}" \
            "${case_dir}/kill-events-${host}.ndjson"
        "${script_dir}/verify-crash-evidence.sh" killed "${run_id}" "${project_name}" \
            "${host}" "${image_id}" "${case_dir}/selected-${host}.json" \
            "${case_dir}/killed-${host}.json" "${case_dir}/kill-events-${host}.ndjson"
    done
    check_support_containers

    failure_category=product
    phase 'cluster restart: whole-cluster outage'
    compose_call_timeout=20
    local observer_id
    observer_id="$(owned_service_container observer)" || return 1
    for host in "${node_hosts[@]}"; do
        wait_for "observer saw ${host} unavailable" 20 \
            observer_saw "${observer_id}" "${host}" unavailable "${fault_since}"
    done
    wait_for 'the broker accepted source traffic while every node was down' 30 \
        topic_progressed chaos_input "${source_before}"
    local elapsed_seconds=$(( ( $(epoch_ms) - kill_requested_ms ) / 1000 ))
    if ((elapsed_seconds < outage_seconds)); then
        sleep "$((outage_seconds - elapsed_seconds))"
    fi
    for index in "${!container_ids[@]}"; do
        host="${node_hosts[${index}]}"
        inspect_target "${container_ids[${index}]}" "${case_dir}/held-${host}.json"
        "${script_dir}/verify-crash-evidence.sh" killed "${run_id}" "${project_name}" \
            "${host}" "${image_id}" "${case_dir}/selected-${host}.json" \
            "${case_dir}/held-${host}.json" "${case_dir}/kill-events-${host}.ndjson"
    done
    check_support_containers
    local source_while_down output_while_down
    source_while_down="$(topic_end_offset chaos_input)"
    output_while_down="$(topic_end_offset chaos_output)"
    local outage_ms="$(( $(epoch_ms) - kill_requested_ms ))"

    phase 'cluster restart: start of every node from its own volume'
    failure_category=injection
    start_status_sampler "${case_dir}/status-samples.log" "${cluster_restart_sampler_seconds}"
    local start_requested_ns start_requested_ms
    start_requested_ns="$(date +%s%N)"
    start_requested_ms="$(epoch_ms)"
    run_bounded 60 docker start "${container_ids[@]}" >"${case_dir}/docker-start.txt"
    for index in "${!container_ids[@]}"; do
        host="${node_hosts[${index}]}"
        recovery_node_started "${container_ids[${index}]}"
        inspect_target "${container_ids[${index}]}" "${case_dir}/started-${host}.json"
        "${script_dir}/verify-crash-evidence.sh" started "${run_id}" "${project_name}" \
            "${host}" "${image_id}" "${case_dir}/selected-${host}.json" "${case_dir}/started-${host}.json"
    done

    failure_category=product
    phase 'cluster restart: public recovery'
    for host in "${node_hosts[@]}"; do
        wait_for "${host} listeners restored" "${cluster_restart_listener_bound}" probe_node "${host}"
    done
    local listener_ms="$(( $(epoch_ms) - start_requested_ms ))"
    for host in "${node_hosts[@]}"; do
        wait_for "observer saw ${host} ready again" 30 \
            observer_saw "${observer_id}" "${host}" ready "${fault_since}"
    done
    mkdir -p "${case_dir}/recovered"
    wait_for 'every node caught up, connected and executing' "${cluster_restart_settle_bound}" \
        cluster_settled_and_connected "${case_dir}/recovered" \
        || recovery_fail product "the cluster did not settle within ${cluster_restart_settle_bound}s after the restart"
    local settled_ms="$(( $(epoch_ms) - start_requested_ms ))"
    wait_for 'sink output advanced after the restart' "${cluster_restart_delivery_bound}" \
        topic_progressed chaos_output "${output_while_down}" \
        || recovery_fail product 'sink output did not advance after the restart'
    local delivery_ms="$(( $(epoch_ms) - start_requested_ms ))"
    wait "${cluster_restart_sampler_pid}" || true
    parse_status_samples "${case_dir}/status-samples.log" "${case_dir}/status-samples.ndjson"
    docker_event_window "${start_requested_ns}" "${case_dir}/start-events.ndjson" --role node \
        || recovery_fail controller 'the live Docker event recording does not cover the restart'
    node_start_times "${case_dir}/start-events.ndjson" "${case_dir}/node-starts.json"
    local owner_files=()
    local kind
    for kind in ingestor relay emitter; do
        owner_files+=(--arg "${kind}" "$(owner_from_description "${case_dir}/recovered/${kind}.attempt.txt")")
    done
    jq -n "${owner_files[@]}" \
        --argjson live "$(printf '%s\n' "${node_names[@]}" | jq -R . | jq -s .)" \
        '{owners: {ingestor: $ARGS.named.ingestor, relay: $ARGS.named.relay, emitter: $ARGS.named.emitter},
          live_voters: $live}' >"${case_dir}/owners-after.json"
    "${script_dir}/verify-recovery-evidence.sh" ownership \
        --before "${case_dir}/owners-before.json" --after "${case_dir}/owners-after.json" \
        --samples "${case_dir}/status-samples.ndjson" --starts "${case_dir}/node-starts.json" \
        --grace-ms "${cluster_restart_voter_grace_ms}" --output "${case_dir}/ownership.json" \
        || recovery_finding "${case_dir}" 'an owner observed within the voter observation grace lost its work in the restart'

    for host in "${node_hosts[@]}"; do
        wait_for "committed configuration through ${host} after the restart" 60 \
            capture_configuration "${host}" "${case_dir}/configuration-after/${host}" chaos_restart_before
        local differences
        differences="$(configuration_differences "${case_dir}/configuration-before/${host}.json" \
            "${case_dir}/configuration-after/${host}.json")"
        [[ -z "${differences}" ]] \
            || recovery_finding "${case_dir}" "${host} reports different committed ${differences//$'\n'/, } after the restart"
    done
    if [[ "${node_count}" == 3 ]] \
        && ! wait_for 'Kafka ingestion runs only on the scheduled ingestor owner' \
            "${cluster_restart_delivery_bound}" ingestion_converged "${case_dir}/recovered"; then
        recovery_finding "${case_dir}" 'Kafka ingestion did not converge on the scheduled ingestor owner after the restart'
    fi
    local recovered_leader
    recovered_leader="$(<"${case_dir}/recovered/leader.txt")"
    leader_host="nervix-${recovered_leader##*-}"
    outcome="$(configuration_change "${case_dir}" chaos_restart_after "${leader_host}" chaos_baseline \
        'CREATE RESOURCE chaos_restart_after;' "created resource 'chaos_restart_after'")"
    [[ "${outcome}" == acknowledged ]] \
        || recovery_fail product "the control canary after the restart was ${outcome}, not acknowledged"
    for host in "${node_hosts[@]}"; do
        wait_for "both control canaries through ${host}" 60 \
            capture_configuration "${host}" "${case_dir}/canaries-after/${host}" \
            chaos_restart_before chaos_restart_after
        jq -e '.resources | all(.[]; . == "present")' "${case_dir}/canaries-after/${host}.json" >/dev/null \
            || recovery_finding "${case_dir}" "an acknowledged control canary is absent through ${host} after the restart"
    done

    docker_event_window "${fault_since_ns}" "${case_dir}/node-events.ndjson" --role node \
        || recovery_fail controller 'the live Docker event recording does not cover the crash through recovery'
    for index in "${!container_ids[@]}"; do
        host="${node_hosts[${index}]}"
        jq -c --arg id "${container_ids[${index}]}" 'select(.Actor.ID == $id)' \
            "${case_dir}/node-events.ndjson" >"${case_dir}/node-events-${host}.ndjson"
        "${script_dir}/verify-docker-events.sh" lifecycle --events "${case_dir}/node-events-${host}.ndjson" \
            --target "${container_ids[${index}]}" --expect kill:9 --expect die:137 --expect start \
            || recovery_finding "${case_dir}" "${host} had node lifecycle events other than its SIGKILL, exit and explicit start"
        inspect_target "${container_ids[${index}]}" "${case_dir}/final-${host}.json"
        "${script_dir}/verify-crash-evidence.sh" recovered "${run_id}" "${project_name}" \
            "${host}" "${image_id}" "${case_dir}/started-${host}.json" "${case_dir}/final-${host}.json" \
            || recovery_finding "${case_dir}" "${host} restarted again after the explicit Docker start"
    done
    node_volumes_unchanged "${case_dir}/volumes-before.json" "${case_dir}/volumes-after.json"
    capture_all_metrics "${case_dir}"
    run_bounded 20 docker logs --since "${fault_since}" "${observer_id}" >"${case_dir}/observer.log" 2>&1
    trim_file "${case_dir}/observer.log" 1048576

    local source_after output_after
    source_after="$(topic_end_offset chaos_input)"
    output_after="$(topic_end_offset chaos_output)"
    jq -n \
        --arg fault_since "${fault_since}" \
        --argjson nodes "${node_count}" \
        --argjson kill_ms "$((kill_completed_ms - kill_requested_ms))" \
        --argjson outage_ms "${outage_ms}" \
        --argjson listener_ms "${listener_ms}" \
        --argjson settled_ms "${settled_ms}" \
        --argjson delivery_ms "${delivery_ms}" \
        --argjson grace_ms "${cluster_restart_voter_grace_ms}" \
        --argjson listener_bound "${cluster_restart_listener_bound}" \
        --argjson settle_bound "${cluster_restart_settle_bound}" \
        --argjson delivery_bound "${cluster_restart_delivery_bound}" \
        --argjson outage_seconds "${outage_seconds}" \
        --argjson source_before "${source_before}" \
        --argjson source_while_down "${source_while_down}" \
        --argjson source_after "${source_after}" \
        --argjson output_before "${output_before}" \
        --argjson output_while_down "${output_while_down}" \
        --argjson output_after "${output_after}" \
        --slurpfile ownership "${case_dir}/ownership.json" \
        --slurpfile starts "${case_dir}/node-starts.json" \
        --argjson configured "$(configured_settings "${case_dir}/selected-${node_hosts[0]}.json")" \
        '{evidence_class: "process crash: every node SIGKILLed and started again on the same host, image and volume",
          not_established: ["host power loss", "storage or filesystem corruption"],
          topology_nodes: $nodes, fault_since: $fault_since,
          configured_bounds: {
            deployment: $configured,
            voter_observation_grace_ms: $grace_ms,
            minimum_outage_seconds: $outage_seconds,
            listener_recovery_bound_seconds: $listener_bound,
            settlement_bound_seconds: $settle_bound,
            delivery_bound_seconds: $delivery_bound},
          kill_duration_ms: $kill_ms, held_outage_ms: $outage_ms,
          listener_recovery_ms: $listener_ms, settled_recovery_ms: $settled_ms,
          delivery_resume_ms: $delivery_ms, node_starts_ns: $starts[0],
          ownership: $ownership[0],
          source_offsets: {before: $source_before, while_down: $source_while_down, after: $source_after},
          output_offsets: {before: $output_before, while_down: $output_while_down, after: $output_after},
          configuration: {before: "restart/configuration-before", after: "restart/configuration-after"},
          status_samples: "restart/status-samples.ndjson",
          node_events: "restart/node-events.ndjson",
          findings: "restart/findings.ndjson"}' \
        >"${artifact_dir}/results/cluster-restart-progress.json"

    recovery_final_boundary
}
