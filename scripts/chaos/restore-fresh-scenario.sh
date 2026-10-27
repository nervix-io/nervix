#!/usr/bin/env bash
# Sourced by run-baseline.sh for the restore-fresh-cluster scenario, after backup-scenario.sh. Owns
# restoring a quiesced backup taken under acknowledged Kafka traffic into a fresh three-node cluster
# with different node names, started as a Compose project of its own on the run's network, and
# judging that the restored domain resumes consumption exactly at the archived offset while the
# original domain stays stopped, and that its deduplicator keys and the rows its window retained at
# the cut carry into the fresh cluster. The run's final ledger then covers both clusters' output.

# shellcheck source=recovery-scenario.sh
source "${script_dir}/recovery-scenario.sh"

# The fresh cluster's node names and the host names that serve them.
fresh_nodes=(fresh-1 fresh-2 fresh-3)
fresh_hosts=(nervix-fresh-1 nervix-fresh-2 nervix-fresh-3)
# External bounds, in seconds.
fresh_startup_bound=180
fresh_restore_bound=300
fresh_resume_bound=180

fresh_fail() {
    recovery_fail "$@"
}

# Issues the fresh nodes' certificates from the run's CA, each naming the fresh cluster and its
# node, as generate_tls does for the original nodes.
fresh_generate_tls() {
    local tls_dir="${artifact_dir}/tls"
    local index
    for index in "${!fresh_nodes[@]}"; do
        local node_id="${fresh_nodes[${index}]}"
        local hostname="${fresh_hosts[${index}]}"
        local extension="${tls_dir}/${node_id}.ext"
        cat >"${extension}" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=DNS:${hostname},URI:nervix://cluster/${CHAOS_FRESH_CLUSTER_ID}/node/${node_id}
EOF
        run_bounded 30 openssl req -newkey rsa:2048 -sha256 -nodes \
            -subj "/CN=${hostname}" \
            -keyout "${tls_dir}/${node_id}-key.pem" \
            -out "${tls_dir}/${node_id}.csr" \
            >>"${artifact_dir}/diagnostics/openssl-fresh-nodes.txt" 2>&1
        run_bounded 30 openssl x509 -req -sha256 -days 2 \
            -in "${tls_dir}/${node_id}.csr" \
            -CA "${tls_dir}/ca.pem" \
            -CAkey "${tls_dir}/ca-key.pem" \
            -CAcreateserial \
            -extfile "${extension}" \
            -out "${tls_dir}/${node_id}.pem" \
            >>"${artifact_dir}/diagnostics/openssl-fresh-nodes.txt" 2>&1
    done
    chmod 0644 "${tls_dir}/"*.pem
}

fresh_compose() {
    run_bounded "${compose_call_timeout:-120}" docker compose "${fresh_cluster_compose_args[@]}" "$@"
}

# True when the fresh cluster lists exactly its three node names as voters, agrees on one leader
# among them, and names none of the original cluster's nodes.
fresh_cluster_formed() {
    local case_dir="$1"
    local leader=""
    local host
    for host in "${fresh_hosts[@]}"; do
        local file="${case_dir}/status-${host}.attempt.txt"
        cli_host="${host}"
        local status=0
        cli_command 'SHOW CLUSTER STATUS;' >"${file}" 2>&1 || status=$?
        cli_host="${node_hosts[0]}"
        ((status == 0)) || return 1
        grep -Fxq "cluster_id: ${CHAOS_FRESH_CLUSTER_ID}" "${file}" || return 1
        local observed
        observed="$(awk -F ': ' '$1 == "raft.current_leader" {print $2}' "${file}")"
        [[ "${observed}" =~ ^fresh-[123]$ ]] || return 1
        if [[ -n "${leader}" && "${leader}" != "${observed}" ]]; then
            return 1
        fi
        leader="${observed}"
        local node
        for node in "${fresh_nodes[@]}"; do
            grep -Eq "^- ${node} \\[voter\\] nervix-${node}:47395$" "${file}" || return 1
        done
        ! grep -Eq '^- node-[123] ' "${file}" || return 1
    done
    printf '%s\n' "${leader}" >"${case_dir}/fresh-leader.txt"
}

# Starts the fresh cluster as its own Compose project on the run's network.
fresh_start_cluster() {
    local case_dir="$1"
    export CHAOS_FRESH_CLUSTER_ID="${cluster_id}-fresh"
    export CHAOS_NETWORK="${project_name}_chaos"
    fresh_cluster_compose_args=(--project-name "${project_name}-fresh"
        --file "${script_dir}/compose.fresh-cluster.yaml")
    if [[ -n "${diagnostic_selection}" ]]; then
        local node
        for node in "${fresh_nodes[@]}"; do
            mkdir -p "${artifact_dir}/deadlock/${node}"
            chmod 0777 "${artifact_dir}/deadlock/${node}"
            fresh_cluster_evidence_nodes+=(--extra-node "${node}=nervix-${node}")
        done
        fresh_cluster_compose_args+=(--file "${script_dir}/compose.fresh-cluster.diagnostic.yaml")
    fi
    fresh_generate_tls
    fresh_compose config --quiet >"${case_dir}/fresh-compose-config.txt" 2>&1 \
        || fresh_fail setup 'the fresh cluster Compose project does not validate'
    fresh_compose up --detach --no-deps nervix-fresh-1 >"${case_dir}/fresh-up-1.txt" 2>&1
    wait_for 'fresh bootstrap node listeners' "${fresh_startup_bound}" probe_node nervix-fresh-1 \
        || fresh_fail product 'the fresh bootstrap node never served'
    fresh_compose up --detach --no-deps nervix-fresh-2 nervix-fresh-3 >"${case_dir}/fresh-up-rest.txt" 2>&1
    local host
    for host in "${fresh_hosts[@]}"; do
        wait_for "${host} listeners" "${fresh_startup_bound}" probe_node "${host}" \
            || fresh_fail product "${host} never served"
        local container_id
        container_id="$(run_bounded 20 docker ps --quiet --filter "label=io.nervix.chaos.run=${run_id}" \
            --filter "label=com.docker.compose.service=${host}")"
        [[ "$(run_bounded 20 docker inspect --format '{{.Image}}' "${container_id}")" == "${image_id}" ]] \
            || fresh_fail controller "${host} does not run the resolved image ${image_id}"
    done
    wait_for 'a formed fresh cluster of three voters' "${fresh_startup_bound}" fresh_cluster_formed "${case_dir}" \
        || fresh_fail product 'the fresh cluster never formed with its three node names'
    local original_cli_host="${cli_host}" policy_status=0
    cli_host="${fresh_hosts[0]}"
    configure_cluster_parameters >"${case_dir}/fresh-cluster-parameter-change.txt" 2>&1 || policy_status=$?
    if ((policy_status == 0)); then
        wait_for 'fresh cluster parameters installed on every node' "${fresh_startup_bound}" \
            capture_cluster_parameters "${case_dir}/fresh-cluster-parameters" "${fresh_nodes[@]}" \
            || policy_status=$?
    fi
    cli_host="${original_cli_host}"
    ((policy_status == 0)) || fresh_fail product 'the fresh cluster refused its requested policy'
}

# True when the restored domain's ingestor runs on a fresh node.
fresh_restored_running() {
    local case_dir="$1"
    cli_host="${fresh_hosts[0]}"
    local status=0
    domain_cli_command 'DESCRIBE INGESTOR chaos_ingestor;' >"${case_dir}/fresh-ingestor.attempt.txt" 2>&1 \
        || status=$?
    cli_host="${node_hosts[0]}"
    ((status == 0)) || return 1
    grep -Fxq 'status: running' "${case_dir}/fresh-ingestor.attempt.txt" \
        && grep -Eq '^owner: fresh-[123]$' "${case_dir}/fresh-ingestor.attempt.txt"
}

run_restore_fresh_cluster() {
    local case_dir="${artifact_dir}/restore-fresh"
    local archive_dir="${case_dir}/archive"
    mkdir -p "${archive_dir}"
    recovery_traffic_startup "${case_dir}"
    failure_category=product

    phase 'fresh restore: quiesced backup under load'
    wait_for 'source baseline' 40 topic_progressed chaos_input 9
    wait_for 'acknowledged sink baseline' 60 topic_progressed chaos_output 9
    backup_archive_domain "${archive_dir}" chaos_baseline "${archive_dir}/domain.nvxb" \
        "${archive_dir}/report.json" \
        || fresh_fail product 'the packaged CLI could not create the quiesced archive'
    backup_require_quiesced_report "${archive_dir}/report.json"
    backup_cut_window "${archive_dir}/report.json"
    local freeze_ms=$((backup_released_ms - backup_engaged_ms))
    backup_verify_archive "${archive_dir}" "${archive_dir}/domain.nvxb" "${archive_dir}/description.json" 9
    local archived_offset
    archived_offset="$(backup_archived_offset "${archive_dir}/description.json")"
    [[ "${archived_offset}" =~ ^[0-9]+$ ]] \
        || fresh_fail product 'the archive holds no chaos_input offset to resume from'

    phase 'fresh restore: original domain stopped'
    domain_cli_command 'STOP;' >"${case_dir}/stop-original.txt" 2>&1 \
        || fresh_fail product 'the original domain could not be stopped'
    wait_for 'the original ingestor stopped' 60 backup_original_stopped "${case_dir}" \
        || fresh_fail product 'the original domain did not stop its ingestor'
    backup_output_boundary=""
    wait_for 'the original output settled' 120 backup_output_settled \
        || fresh_fail product 'the original domain kept emitting after it stopped'
    local original_output_end="${backup_output_boundary}"
    ((original_output_end >= archived_offset)) \
        || recovery_finding "${case_dir}" "the original domain emitted ${original_output_end} records, fewer than the archived offset ${archived_offset}"
    local source_at_stop
    source_at_stop="$(topic_end_offset chaos_input)"

    phase 'fresh restore: fresh three-node cluster'
    local startup_started_ms
    startup_started_ms="$(epoch_ms)"
    fresh_start_cluster "${case_dir}"
    local startup_ms=$(($(epoch_ms) - startup_started_ms))

    phase 'fresh restore: RESUME restore into the fresh cluster'
    local restore_started_ms
    restore_started_ms="$(epoch_ms)"
    local restore_status=0
    compose_call_timeout="${fresh_restore_bound}"
    compose run --rm --no-deps --user "$(id -u):$(id -g)" \
        -v "${archive_dir}:/chaos-backup:ro" admin \
        nervix-cli --server "http://${fresh_hosts[0]}:47391" \
        --password "${CHAOS_PASSWORD}" \
        restore domain chaos_baseline --input /chaos-backup/domain.nvxb --resume --format json \
        >"${archive_dir}/restore-report.json" 2>"${archive_dir}/restore-stderr.txt" \
        || restore_status=$?
    compose_call_timeout=120
    local restore_ms=$(($(epoch_ms) - restore_started_ms))
    ((restore_status == 0)) \
        || fresh_fail product "the RESUME restore into the fresh cluster failed with status ${restore_status}"
    jq -e '.domains | length == 1 and .[0].domain == "chaos_baseline"' "${archive_dir}/restore-report.json" >/dev/null \
        || fresh_fail product 'the fresh restore report does not name chaos_baseline'
    wait_for 'the restored ingestor running on a fresh node' "${fresh_resume_bound}" \
        fresh_restored_running "${case_dir}" \
        || fresh_fail product 'the restored domain never ran its ingestor on a fresh node'
    local resume_ms=$(($(epoch_ms) - restore_started_ms))

    phase 'fresh restore: continued Kafka consumption'
    wait_for 'the fresh cluster emitting' "${fresh_resume_bound}" \
        topic_progressed chaos_output "${original_output_end}" \
        || fresh_fail product 'the fresh cluster never emitted'
    wait_for 'the fresh cluster consuming past the original stop' "${fresh_resume_bound}" \
        topic_progressed chaos_output "$((original_output_end + source_at_stop - archived_offset))" \
        || recovery_finding "${case_dir}" 'the fresh cluster did not consume past where the original cluster stopped'
    # Seventy-two more records close two more windows of every branch in the fresh cluster.
    local output_past_stop
    output_past_stop="$(topic_end_offset chaos_output)"
    wait_for 'the fresh cluster closing two more windows of every branch' "${fresh_resume_bound}" \
        topic_progressed chaos_output "$((output_past_stop + 71))" \
        || recovery_finding "${case_dir}" 'the fresh cluster stopped consuming before two more windows of every branch closed'
    check_support_containers

    recovery_final_boundary
    # The fresh cluster serves the domain from here on, so the run's public diagnostics ask it.
    cli_host="${fresh_hosts[0]}"
    phase 'fresh restore: exact resumption'
    local output_end
    output_end="$(topic_end_offset chaos_output)"
    kcat -q -b broker:9092 -C -t chaos_output -p 0 -o beginning -c "${output_end}" \
        >"${case_dir}/output-at-resumption.ndjson" 2>"${case_dir}/output-consumer.stderr"
    # The first record the fresh cluster emitted follows everything the original cluster emitted;
    # it must be the record at the archived offset, and every record it emits afterwards must come
    # from that offset onward.
    local first_resumed archived_record
    first_resumed="$(sed -n "$((original_output_end + 1))p" "${case_dir}/output-at-resumption.ndjson" | jq -r '.event_id')"
    archived_record="$(sed -n "$((archived_offset + 1))p" "${artifact_dir}/traffic/accepted-input.ndjson" | jq -r '.event_id')"
    [[ -n "${first_resumed}" && "${first_resumed}" == "${archived_record}" ]] \
        || recovery_finding "${case_dir}" "the fresh cluster resumed at ${first_resumed:-nothing}, not at the archived offset's ${archived_record:-missing record}"
    tail -n +"$((original_output_end + 1))" "${case_dir}/output-at-resumption.ndjson" | jq -r '.event_id' | sort -u \
        >"${case_dir}/fresh-emitted.txt"
    tail -n +"$((archived_offset + 1))" "${artifact_dir}/traffic/accepted-input.ndjson" | jq -r '.event_id' | sort -u \
        >"${case_dir}/fresh-expected.txt"
    cmp "${case_dir}/fresh-emitted.txt" "${case_dir}/fresh-expected.txt" >/dev/null \
        || recovery_finding "${case_dir}" 'the fresh cluster emitted a record outside, or missed one inside, the source range after the archived offset'
    phase 'fresh restore: deduplicator and window outputs'
    backup_state_verdict "${case_dir}" \
        || recovery_finding "${case_dir}" 'the deduplicator or window outputs differ from what the accepted records produce'

    jq -n \
        --argjson archived_offset "${archived_offset}" \
        --argjson original_output_end "${original_output_end}" \
        --argjson source_at_stop "${source_at_stop}" \
        --arg first_resumed "${first_resumed}" \
        --argjson freeze_ms "${freeze_ms}" \
        --argjson startup_ms "${startup_ms}" \
        --argjson restore_ms "${restore_ms}" \
        --argjson resume_ms "${resume_ms}" \
        --argjson archive_bytes "$(stat -c %s "${archive_dir}/domain.nvxb")" \
        --argjson state "$(backup_archived_state "${archive_dir}/description.json")" \
        --arg leader "$(cat "${case_dir}/fresh-leader.txt")" \
        --arg cluster "${CHAOS_FRESH_CLUSTER_ID}" \
        '{fresh_cluster:{cluster_id:$cluster, nodes:["fresh-1","fresh-2","fresh-3"], leader:$leader,
                         compose_project_file:"scripts/chaos/compose.fresh-cluster.yaml"},
          boundaries:{archived_offset:$archived_offset, original_output_end:$original_output_end,
                      source_at_original_stop:$source_at_stop, first_resumed_event:$first_resumed},
          timings_ms:{backup_freeze:$freeze_ms, fresh_startup:$startup_ms, restore:$restore_ms,
                      restore_to_running:$resume_ms},
          archive_bytes:$archive_bytes, archived_state:$state,
          state_verdict:"results/backup-state.json",
          evidence:{backup_report:"restore-fresh/archive/report.json",
                    archive_description:"restore-fresh/archive/description.json",
                    restore_report:"restore-fresh/archive/restore-report.json",
                    fresh_status:"restore-fresh/status-nervix-fresh-1.attempt.txt"}}' \
        >"${artifact_dir}/results/restore-fresh-cluster-progress.json"
}
