#!/usr/bin/env bash
# Sourced by run-baseline.sh after the external cluster and graph are ready. Stops one follower
# gracefully with Pumba and keeps it offline while acknowledged configuration changes make the
# survivors snapshot and purge their Raft logs past its position. It then starts the same container
# from its own image and volume and requires snapshot catch-up and every acknowledged change
# through the restarted node's own public route.

# shellcheck source=recovery-scenario.sh
source "${script_dir}/recovery-scenario.sh"

# Configuration changes the survivors commit per round while the follower is offline, and the
# number of rounds after which a log that still covers the follower's position fails the run.
stale_resources_per_round=8
stale_max_rounds=12
# External budgets, in seconds.
stale_survivor_bound=90
stale_failover_bound=120
stale_catch_up_bound=120
stale_convergence_bound=150
stale_delivery_bound=90

# True when a node container runs with the scenario's snapshot policy.
snapshot_policy_configured() {
    jq -e \
        --arg entries "NERVIX_RAFT_SNAPSHOT_ENTRY_THRESHOLD=${CHAOS_RAFT_SNAPSHOT_ENTRY_THRESHOLD}" \
        --arg covered "NERVIX_RAFT_COVERED_LOG_ENTRIES_RETAINED=${CHAOS_RAFT_COVERED_LOG_ENTRIES_RETAINED}" \
        '.[0].Config.Env | (index($entries) != null) and (index($covered) != null)' "$1" >/dev/null
}

# Records every survivor's log positions to OUTPUT_DIR/HOST.json and appends one compaction sample
# to the case ledger.
sample_survivor_logs() {
    local case_dir="$1"
    local output_dir="$2"
    local label="$3"
    local follower_host="$4"
    mkdir -p "${output_dir}"
    local host
    for host in "${node_hosts[@]}"; do
        [[ "${host}" == "${follower_host}" ]] && continue
        consensus_record "${host}" "${output_dir}/metrics-${host}.txt" "${output_dir}/${host}.json" \
            || recovery_fail product "${host} did not report its consensus log positions"
        jq -c --arg label "${label}" '. + {sample: $label}' "${output_dir}/${host}.json" \
            >>"${case_dir}/compaction.ndjson"
    done
}

# True when every survivor has purged its log past BOUND.
survivors_purged_past() {
    local output_dir="$1"
    local bound="$2"
    local follower_host="$3"
    local host
    for host in "${node_hosts[@]}"; do
        [[ "${host}" == "${follower_host}" ]] && continue
        jq -e --argjson bound "${bound}" '.purged_index > $bound' "${output_dir}/${host}.json" >/dev/null \
            || return 1
    done
}

# Creates one round of resources through HOST in one administration container and records each
# outcome; acknowledged and uncertain names are later checked through the restarted follower.
create_resource_round() {
    local case_dir="$1"
    local host="$2"
    local round="$3"
    local pairs=()
    local index
    for index in $(seq 1 "${stale_resources_per_round}"); do
        local name="chaos_stale_r${round}_${index}"
        pairs+=("${name}" "CREATE RESOURCE ${name};")
    done
    admin_cli_batch "${host}" chaos_baseline "${case_dir}/changes" 120 "${pairs[@]}" || true
    for index in $(seq 1 "${stale_resources_per_round}"); do
        local name="chaos_stale_r${round}_${index}"
        jq -nc \
            --arg name "${name}" \
            --arg host "${host}" \
            --arg statement "CREATE RESOURCE ${name};" \
            --arg outcome "$(batch_write_outcome "${case_dir}/changes" "${name}" "created resource '${name}'")" \
            --argjson round "${round}" \
            '{name:$name,entry_host:$host,domain:"chaos_baseline",statement:$statement,outcome:$outcome,round:$round,kind:"resource"}' \
            >>"${case_dir}/changes.ndjson"
    done
}

# True when the restarted follower holds a snapshot covering the survivors' purged logs and has
# applied every entry the leader had applied before the restart.
follower_caught_up() {
    local case_dir="$1"
    local follower_host="$2"
    local purged="$3"
    local leader_applied="$4"
    consensus_record "${follower_host}" "${case_dir}/catch-up/metrics-${follower_host}.txt" \
        "${case_dir}/catch-up/${follower_host}.json" || return 1
    partition_node_status "${follower_host}" "${case_dir}/catch-up/status-${follower_host}.txt" || return 1
    jq -e \
        --argjson purged "${purged}" \
        --argjson leader_applied "${leader_applied}" \
        --slurpfile status "${case_dir}/catch-up/status-${follower_host}.json" \
        '.snapshot_index >= $purged and $status[0].last_applied >= $leader_applied' \
        "${case_dir}/catch-up/${follower_host}.json" >/dev/null
}

# Reads every acknowledged and uncertain change through HOST's own public route into OUTPUT_DIR.
read_applied_configuration() {
    local case_dir="$1"
    local host="$2"
    local output_dir="$3"
    local pairs=(status 'SHOW CLUSTER STATUS;' relay 'DESCRIBE RELAY chaos_records;')
    local name
    while IFS= read -r name; do
        pairs+=("resource-${name}" "DESCRIBE RESOURCE ${name};")
    done < <(jq -r 'select(.kind == "resource" and .outcome != "unattempted") | .name' \
        "${case_dir}/changes.ndjson")
    admin_cli_batch "${host}" chaos_baseline "${output_dir}" 120 "${pairs[@]}" || return 1
    admin_cli_batch "${host}" chaos_stale "${output_dir}" 60 \
        schema-kept 'SHOW CREATE SCHEMA chaos_stale_kept;' \
        schema-dropped 'SHOW CREATE SCHEMA chaos_stale_dropped;' || return 1
    batch_read_succeeded "${output_dir}" status \
        && grep -Fxq "raft.id: node-${host##*-}" "${output_dir}/status.txt"
}

# Writes one JSON verdict per change: what the restarted follower and the leader each report about
# its effect, and whether the follower applied every acknowledged change.
judge_applied_configuration() {
    local case_dir="$1"
    local follower_dir="$2"
    local leader_dir="$3"
    local relay_target="$4"
    local output="$5"
    local -A effects=()
    local side dir
    for side in follower leader; do
        dir="${follower_dir}"
        [[ "${side}" == leader ]] && dir="${leader_dir}"
        local domain_effect=absent
        if awk '/^\[domains\]$/ { s = 1; next } /^\[/ { s = 0 } s' "${dir}/status.txt" \
            | grep -Fxq -- '- chaos_stale status=Stopped pace=UNPACED'; then
            domain_effect=present
        fi
        effects["${side}_domain"]="${domain_effect}"
        local kept_effect=absent
        if batch_read_succeeded "${dir}" schema-kept \
            && grep -Fq 'CREATE SCHEMA chaos_stale_kept (' "${dir}/schema-kept.txt"; then
            kept_effect=present
        fi
        effects["${side}_kept"]="${kept_effect}"
        local dropped_effect=present
        if grep -Fq "schema 'chaos_stale_dropped' does not exist" "${dir}/schema-dropped.txt"; then
            dropped_effect=absent
        fi
        effects["${side}_dropped"]="${dropped_effect}"
        effects["${side}_relay"]="$(owner_from_description "${dir}/relay.txt")"
        effects["${side}_scheduled_relay"]="$(awk '/^\[schedule\]$/ { s = 1; next } /^\[/ { s = 0 }
            s && / kind=relay name=chaos_records / {
                for (i = 1; i <= NF; i++) if ($i ~ /^owner=/) { sub(/^owner=/, "", $i); print $i }
            }' "${dir}/status.txt")"
        effects["${side}_voters"]="$(awk '/^raft\.membership:$/ { s = 1; next }
            s && /^- node-[0-9]+ \[voter\] / { print $2; next } s { s = 0 }' "${dir}/status.txt" \
            | sort | tr '\n' ' ')"
    done
    local expected_voters
    expected_voters="$(printf '%s\n' "${node_names[@]}" | sort | tr '\n' ' ')"
    local verdicts
    verdicts="$(mktemp "${case_dir}/.applied.XXXXXX")"
    local change
    while IFS= read -r change; do
        local name outcome kind expected follower_effect leader_effect
        name="$(jq -r '.name' <<<"${change}")"
        outcome="$(jq -r '.outcome' <<<"${change}")"
        kind="$(jq -r '.kind' <<<"${change}")"
        [[ "${outcome}" == unattempted ]] && continue
        case "${kind}" in
            domain)
                expected=present
                follower_effect="${effects[follower_domain]}"
                leader_effect="${effects[leader_domain]}"
                ;;
            schema-kept)
                expected=present
                follower_effect="${effects[follower_kept]}"
                leader_effect="${effects[leader_kept]}"
                ;;
            schema-dropped)
                # Created and then dropped: its acknowledged final effect is its absence.
                expected=absent
                follower_effect="${effects[follower_dropped]}"
                leader_effect="${effects[leader_dropped]}"
                ;;
            schema-drop)
                expected=absent
                follower_effect="${effects[follower_dropped]}"
                leader_effect="${effects[leader_dropped]}"
                ;;
            relocation)
                follower_effect="${effects[follower_relay]}/${effects[follower_scheduled_relay]}"
                leader_effect="${effects[leader_relay]}/${effects[leader_scheduled_relay]}"
                expected="${relay_target}/${relay_target}"
                ;;
            resource)
                expected=present
                follower_effect=absent
                if batch_read_succeeded "${follower_dir}" "resource-${name}"; then
                    follower_effect=present
                elif ! grep -Fq "resource '${name}' does not exist" "${follower_dir}/resource-${name}.txt"; then
                    follower_effect=unknown
                fi
                leader_effect=absent
                if batch_read_succeeded "${leader_dir}" "resource-${name}"; then
                    leader_effect=present
                elif ! grep -Fq "resource '${name}' does not exist" "${leader_dir}/resource-${name}.txt"; then
                    leader_effect=unknown
                fi
                ;;
            *)
                recovery_fail controller "unknown configuration change kind ${kind}"
                ;;
        esac
        jq -nc \
            --argjson change "${change}" \
            --arg expected "${expected}" \
            --arg follower "${follower_effect}" \
            --arg leader "${leader_effect}" '
            $change + {expected_effect: $expected, restored_follower_effect: $follower,
                       leader_effect: $leader,
                       applied: (if $change.outcome == "acknowledged" then $follower == $expected
                                 else $follower == $leader and $follower != "unknown" end)}' \
            >>"${verdicts}"
    done <"${case_dir}/changes.ndjson"
    jq -s \
        --arg follower_voters "${effects[follower_voters]}" \
        --arg leader_voters "${effects[leader_voters]}" \
        --arg expected_voters "${expected_voters}" '
        {changes: .,
         acknowledged_changes: ([.[] | select(.outcome == "acknowledged")] | length),
         uncertain_changes: ([.[] | select(.outcome == "uncertain")] | length),
         not_applied: [.[] | select(.applied | not) | .name],
         membership: {expected: $expected_voters, restored_follower: $follower_voters, leader: $leader_voters,
                      applied: ($follower_voters == $expected_voters and $leader_voters == $expected_voters)}}' \
        "${verdicts}" >"${output}"
    rm -f "${verdicts}"
}

run_stale_follower() {
    local case_dir="${artifact_dir}/stale"
    mkdir -p "${case_dir}/catch-up"
    : >"${case_dir}/changes.ndjson"
    : >"${case_dir}/compaction.ndjson"
    recovery_traffic_startup "${case_dir}"

    failure_category=product
    phase 'stale follower: settled roles and snapshot policy'
    wait_for 'settled, connected cluster before the outage' 150 \
        observe_partition_roles "${case_dir}/before"
    local leader follower leader_host follower_host
    leader="$(jq -r '.leader' "${case_dir}/before/role.json")"
    follower="$(jq -r '.follower' "${case_dir}/before/role.json")"
    leader_host="nervix-${leader##*-}"
    follower_host="nervix-${follower##*-}"
    local host
    local node_ids=()
    for host in "${node_hosts[@]}"; do
        local node_id
        node_id="$(owned_service_container "${host}")" || return 1
        node_ids+=("${node_id}")
        inspect_target "${node_id}" "${case_dir}/policy-${host}.json"
        snapshot_policy_configured "${case_dir}/policy-${host}.json" \
            || recovery_fail setup "${host} does not run with the scenario's snapshot policy"
    done
    run_bounded 20 docker inspect "${node_ids[@]}" >"${case_dir}/before-all-nodes.json"
    record_node_volumes "${case_dir}/volumes-before.json"
    local outcome
    outcome="$(configuration_change "${case_dir}" chaos_stale_before "${leader_host}" chaos_baseline \
        'CREATE RESOURCE chaos_stale_before;' "created resource 'chaos_stale_before'")"
    [[ "${outcome}" == acknowledged ]] \
        || recovery_fail product "the control canary before the outage was ${outcome}, not acknowledged"
    check_support_containers
    check_other_nodes ""

    phase "stale follower: graceful Pumba stop of ${follower_host}"
    failure_category=injection
    local container_id container_name
    container_id="$(owned_service_container "${follower_host}")" || return 1
    inspect_target "${container_id}" "${case_dir}/before.json"
    "${script_dir}/verify-restart-evidence.sh" before "${run_id}" "${project_name}" \
        "${follower_host}" "${image_id}" "${case_dir}/before.json" "${case_dir}/before.json"
    container_name="$(jq -r '.[0].Name | ltrimstr("/")' "${case_dir}/before.json")"
    run_bounded 20 docker run --rm \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=fault \
        --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        "${pumba_image_id}" --dry-run --log-level info \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=node \
        stop --time 60 --limit 1 "${container_name}" \
        >"${case_dir}/pumba-dry-run.txt" 2>&1
    [[ "$(grep -Fc 'msg="stopping container"' "${case_dir}/pumba-dry-run.txt")" -eq 1 ]] \
        && grep -Fq "dryrun=true id=${container_id}" "${case_dir}/pumba-dry-run.txt" \
        || recovery_fail injection "Pumba did not resolve exactly the owned ${follower_host} container"
    consensus_record "${follower_host}" "${case_dir}/follower-before-metrics.txt" \
        "${case_dir}/follower-before.json" \
        || recovery_fail product "${follower_host} did not report its consensus log positions before the stop"
    local stop_since stop_since_ns stop_requested_ms
    stop_since="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    stop_since_ns="$(date -d "${stop_since}" +%s%N)"
    stop_requested_ms="$(epoch_ms)"
    jq -n \
        --arg image "${pumba_image_id}" \
        --arg target "${container_name}" \
        --arg container_id "${container_id}" \
        --arg run_id "${run_id}" \
        --arg stop_since "${stop_since}" \
        '{pumba_image_id:$image,command:["pumba","--label","io.nervix.chaos.run="+$run_id,"--label","io.nervix.chaos.role=node","stop","--time","60","--limit","1",$target],target:$target,container_id:$container_id,stop_since:$stop_since,grace_seconds:60}' \
        >"${case_dir}/fault-command.json"
    recovery_node_stopped "${container_id}"
    run_bounded 75 docker run --rm \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=fault \
        --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        "${pumba_image_id}" --log-level info \
        --label "io.nervix.chaos.run=${run_id}" \
        --label io.nervix.chaos.role=node \
        stop --time 60 --limit 1 "${container_name}" \
        >"${case_dir}/pumba.txt" 2>&1
    local stop_completed_ms
    stop_completed_ms="$(epoch_ms)"
    inspect_target "${container_id}" "${case_dir}/stopped.json"
    run_bounded 20 docker logs --since "${stop_since}" "${container_id}" >"${case_dir}/shutdown.log" 2>&1
    trim_file "${case_dir}/shutdown.log" 2097152
    "${script_dir}/verify-restart-evidence.sh" stopped "${run_id}" "${project_name}" \
        "${follower_host}" "${image_id}" "${case_dir}/before.json" "${case_dir}/stopped.json" \
        "${case_dir}/shutdown.log"
    # Both survivors were live voters when the stop began, so the follower had a live replacement
    # and must have handed its work over through the leader. A drain it did not complete leaves the
    # catch-up below meaningful, so it is a finding rather than the end of the run.
    if ! "${script_dir}/verify-drain-evidence.sh" "${follower_host}" "${case_dir}/shutdown.log" \
        2>"${case_dir}/drain-verdict.txt"; then
        recovery_finding "${case_dir}" \
            "${follower_host} did not complete its graceful drain while live replacement nodes existed: $(head -n 1 "${case_dir}/drain-verdict.txt")"
    fi
    # The follower logs the last index its own log held as it stopped.
    local follower_shutdown_index
    follower_shutdown_index="$(grep -F 'raft transition: state=Shutdown' "${case_dir}/shutdown.log" \
        | sed -n 's/.* last_log_index=\([0-9][0-9]*\).*/\1/p' | tail -n 1)"
    check_support_containers
    check_other_nodes "${follower_host}"

    failure_category=product
    phase "stale follower: survivors commit while ${follower_host} is offline"
    wait_for 'survivors agree on a caught-up leader' "${stale_survivor_bound}" \
        survivors_settled "${follower_host}" "${case_dir}"
    local survivor_leader survivor_leader_host
    survivor_leader="$(<"${case_dir}/survivor-leader.txt")"
    survivor_leader_host="nervix-${survivor_leader##*-}"
    # Nothing the follower held can lie past what the leader held once the follower had stopped.
    sample_survivor_logs "${case_dir}" "${case_dir}/after-stop" after-stop "${follower_host}"
    local follower_log_bound
    follower_log_bound="$(jq -r '.last_index' "${case_dir}/after-stop/${survivor_leader_host}.json")"
    wait_for 'every execution owner placed on the survivors' "${stale_failover_bound}" \
        surviving_owners_ready "${follower_host}" "${case_dir}" "${survivor_leader_host}"
    local output_during
    output_during="$(topic_end_offset chaos_output)"
    wait_for 'sink output advanced while the follower was offline' 60 \
        topic_progressed chaos_output "${output_during}" \
        || recovery_fail product 'the survivors did not deliver output while the follower was offline'

    phase 'stale follower: acknowledged configuration changes while offline'
    local relay_owner relay_target
    relay_owner="$(owner_from_description "${case_dir}/relay-survivor.attempt.txt")"
    relay_target=""
    for host in "${node_hosts[@]}"; do
        if [[ "${host}" != "${follower_host}" && "node-${host##*-}" != "${relay_owner}" ]]; then
            relay_target="node-${host##*-}"
        fi
    done
    [[ "${relay_target}" =~ ^node-[123]$ ]] \
        || recovery_fail controller 'no survivor other than the relay owner can take the relay'
    local name domain statement acknowledgement kind
    # The changes come from file descriptor 3, so no command in the loop can read them as input.
    while IFS='|' read -r -u 3 name domain statement acknowledgement kind; do
        outcome="$(configuration_change "${case_dir}" "${name}" "${survivor_leader_host}" "${domain}" \
            "${statement}" "${acknowledgement}" "${kind}")"
        [[ "${outcome}" == acknowledged ]] \
            || recovery_fail product "the configuration change ${name} was ${outcome} while one follower was offline"
    done 3<<EOF
chaos_stale_domain|default|CREATE UNPACED DOMAIN chaos_stale;|created domain 'chaos_stale'|domain
chaos_stale_schema_kept|chaos_stale|CREATE SCHEMA chaos_stale_kept (value STRING);|quiesce level:|schema-kept
chaos_stale_schema_dropped|chaos_stale|CREATE SCHEMA chaos_stale_dropped (value STRING);|quiesce level:|schema-dropped
chaos_stale_schema_drop|chaos_stale|DROP SCHEMA chaos_stale_dropped;|dropped model 'chaos_stale_dropped'|schema-drop
chaos_stale_relocation|chaos_baseline|RELOCATE RELAY chaos_records ONTO NODE ${relay_target} IGNORE PREFERENCES;|relocated 1 of 1 runtime node(s) onto node '${relay_target}'|relocation
EOF
    local round=0
    while true; do
        sample_survivor_logs "${case_dir}" "${case_dir}/rounds/${round}" "round-${round}" "${follower_host}"
        if survivors_purged_past "${case_dir}/rounds/${round}" "${follower_log_bound}" "${follower_host}"; then
            break
        fi
        round=$((round + 1))
        ((round <= stale_max_rounds)) \
            || recovery_fail product "the survivors did not purge their logs past index ${follower_log_bound} within $((stale_max_rounds * stale_resources_per_round)) resource changes"
        create_resource_round "${case_dir}" "${survivor_leader_host}" "${round}"
    done
    jq -s -e '[.[] | select(.kind == "resource")] | all(.outcome == "acknowledged")' \
        "${case_dir}/changes.ndjson" >/dev/null \
        || recovery_finding "${case_dir}" 'a resource change was not acknowledged while one follower was offline'

    phase "stale follower: restart of ${follower_host} from its own volume"
    sample_survivor_logs "${case_dir}" "${case_dir}/restart" restart "${follower_host}"
    partition_node_status "${survivor_leader_host}" "${case_dir}/restart/status-${survivor_leader_host}.txt" \
        || recovery_fail product "the survivor leader did not answer its public status route before the restart"
    local leader_applied purged_at_restart
    leader_applied="$(jq -r '.last_applied' "${case_dir}/restart/status-${survivor_leader_host}.json")"
    purged_at_restart="$(jq -s '[.[] | select(.sample == "restart") | .purged_index] | min' \
        "${case_dir}/compaction.ndjson")"
    survivors_purged_past "${case_dir}/restart" "${follower_log_bound}" "${follower_host}" \
        || recovery_fail product "a survivor still held log entries past index ${follower_log_bound} at the restart"
    failure_category=injection
    local start_requested_ms
    start_requested_ms="$(epoch_ms)"
    run_bounded 30 docker start "${container_id}" >"${case_dir}/docker-start.txt"
    recovery_node_started "${container_id}"
    inspect_target "${container_id}" "${case_dir}/started.json"
    "${script_dir}/verify-restart-evidence.sh" started "${run_id}" "${project_name}" \
        "${follower_host}" "${image_id}" "${case_dir}/before.json" "${case_dir}/started.json"

    failure_category=product
    phase "stale follower: snapshot catch-up of ${follower_host}"
    wait_for "${follower_host} installed a snapshot and caught up" "${stale_catch_up_bound}" \
        follower_caught_up "${case_dir}" "${follower_host}" "${purged_at_restart}" "${leader_applied}" \
        || recovery_fail product "${follower_host} did not catch up within ${stale_catch_up_bound}s of its restart"
    local catch_up_ms="$(( $(epoch_ms) - start_requested_ms ))"
    sample_survivor_logs "${case_dir}" "${case_dir}/caught-up" caught-up "${follower_host}"
    jq -n \
        --arg follower "${follower_host}" \
        --argjson bound "${follower_log_bound}" \
        --argjson leader_applied "${leader_applied}" \
        --slurpfile samples "${case_dir}/compaction.ndjson" \
        --slurpfile caught_up "${case_dir}/catch-up/${follower_host}.json" \
        --slurpfile status "${case_dir}/catch-up/status-${follower_host}.json" '
        {follower: $follower, follower_log_bound: $bound,
         restart: {leader_applied_index: $leader_applied,
                   survivors: [$samples[] | select(.sample == "restart")]},
         caught_up: {follower: ($caught_up[0] + {last_applied: $status[0].last_applied}),
                     survivors: [$samples[] | select(.sample == "caught-up")]}}' \
        >"${case_dir}/catch-up-evidence.json"
    "${script_dir}/verify-recovery-evidence.sh" snapshot-catch-up \
        --evidence "${case_dir}/catch-up-evidence.json" --output "${case_dir}/catch-up.json" \
        || recovery_finding "${case_dir}" "${follower_host} did not show snapshot installation for its catch-up"

    phase "stale follower: acknowledged configuration through ${follower_host}"
    wait_for "configuration through the restarted ${follower_host}" 60 \
        read_applied_configuration "${case_dir}" "${follower_host}" "${case_dir}/applied/follower"
    wait_for "configuration through the leader ${survivor_leader_host}" 60 \
        read_applied_configuration "${case_dir}" "${survivor_leader_host}" "${case_dir}/applied/leader"
    judge_applied_configuration "${case_dir}" "${case_dir}/applied/follower" "${case_dir}/applied/leader" \
        "${relay_target}" "${case_dir}/applied.json"
    local not_applied
    not_applied="$(jq -r '.not_applied | join(", ")' "${case_dir}/applied.json")"
    [[ -z "${not_applied}" ]] \
        || recovery_finding "${case_dir}" "the restarted ${follower_host} does not show these changes: ${not_applied}"
    jq -e '.membership.applied' "${case_dir}/applied.json" >/dev/null \
        || recovery_finding "${case_dir}" "the restarted ${follower_host} does not report every voter"

    phase 'stale follower: public recovery'
    for host in "${node_hosts[@]}"; do
        wait_for "${host} listeners reachable" 120 probe_node "${host}"
    done
    local observer_id
    observer_id="$(owned_service_container observer)" || return 1
    wait_for "observer saw ${follower_host} ready again" 30 \
        observer_saw "${observer_id}" "${follower_host}" ready "${stop_since}"
    mkdir -p "${case_dir}/recovered"
    wait_for 'every node caught up, connected and executing' "${stale_convergence_bound}" \
        cluster_settled_and_connected "${case_dir}/recovered" \
        || recovery_fail product "the cluster did not settle within ${stale_convergence_bound}s of the restart"
    local settled_ms="$(( $(epoch_ms) - start_requested_ms ))"
    local output_at_restart
    output_at_restart="$(topic_end_offset chaos_output)"
    wait_for 'sink output advanced after the restart' "${stale_delivery_bound}" \
        topic_progressed chaos_output "${output_at_restart}" \
        || recovery_fail product 'sink output did not advance after the restart'
    outcome="$(configuration_change "${case_dir}" chaos_stale_after "${follower_host}" chaos_baseline \
        'CREATE RESOURCE chaos_stale_after;' "created resource 'chaos_stale_after'")"
    [[ "${outcome}" == acknowledged ]] \
        || recovery_fail product "the control canary through the restarted follower was ${outcome}, not acknowledged"
    for host in "${node_hosts[@]}"; do
        wait_for "control canaries through ${host}" 60 \
            capture_configuration "${host}" "${case_dir}/canaries-after/${host}" \
            chaos_stale_before chaos_stale_after
        jq -e '.resources | all(.[]; . == "present")' "${case_dir}/canaries-after/${host}.json" >/dev/null \
            || recovery_finding "${case_dir}" "an acknowledged control canary is absent through ${host}"
    done

    node_event_window "${stop_since_ns}" "${container_id}" "${case_dir}/follower-events.ndjson"
    "${script_dir}/verify-docker-events.sh" lifecycle --events "${case_dir}/follower-events.ndjson" \
        --target "${container_id}" --expect kill:15 --expect die:0 --expect start \
        || recovery_finding "${case_dir}" "${follower_host} had lifecycle events other than its graceful stop and explicit start"
    docker_event_window "${stop_since_ns}" "${case_dir}/node-events.ndjson" --role node \
        || recovery_fail controller 'the live Docker event recording does not cover the outage through recovery'
    jq -e -s --arg id "${container_id}" 'all(.[]; .Actor.ID == $id)' "${case_dir}/node-events.ndjson" >/dev/null \
        || recovery_finding "${case_dir}" 'a node other than the stopped follower had lifecycle events'
    other_node_instances_unchanged "${follower_host}" "${case_dir}/before-all-nodes.json" \
        || recovery_finding "${case_dir}" 'a survivor changed its container or process incarnation'
    node_volumes_unchanged "${case_dir}/volumes-before.json" "${case_dir}/volumes-after.json"
    capture_all_metrics "${case_dir}"
    run_bounded 20 docker logs --since "${stop_since}" "${observer_id}" >"${case_dir}/observer.log" 2>&1
    trim_file "${case_dir}/observer.log" 1048576

    jq -n \
        --arg follower "${follower_host}" \
        --arg leader "${leader}" \
        --arg survivor_leader "${survivor_leader}" \
        --arg stop_since "${stop_since}" \
        --arg relay_target "${relay_target}" \
        --argjson bound "${follower_log_bound}" \
        --argjson shutdown_index "${follower_shutdown_index:-null}" \
        --argjson stop_ms "$((stop_completed_ms - stop_requested_ms))" \
        --argjson catch_up_ms "${catch_up_ms}" \
        --argjson settled_ms "${settled_ms}" \
        --argjson rounds "${round}" \
        --argjson per_round "${stale_resources_per_round}" \
        --argjson entries "${CHAOS_RAFT_SNAPSHOT_ENTRY_THRESHOLD}" \
        --argjson covered "${CHAOS_RAFT_COVERED_LOG_ENTRIES_RETAINED}" \
        --argjson catch_up_bound "${stale_catch_up_bound}" \
        --argjson settle_bound "${stale_convergence_bound}" \
        --slurpfile catch_up "${case_dir}/catch-up.json" \
        --slurpfile applied "${case_dir}/applied.json" \
        --slurpfile before "${case_dir}/follower-before.json" \
        '{follower: $follower, leader_before: $leader, survivor_leader: $survivor_leader,
          snapshot_policy: {snapshot_entry_threshold: $entries, covered_log_entries_retained: $covered},
          stop_since: $stop_since, stop_duration_ms: $stop_ms,
          follower_before_stop: $before[0], follower_shutdown_last_log_index: $shutdown_index,
          follower_log_bound: $bound,
          resource_rounds: $rounds, resources_per_round: $per_round,
          relay_relocated_to: $relay_target,
          catch_up: $catch_up[0], catch_up_ms: $catch_up_ms, settled_recovery_ms: $settled_ms,
          bounds: {catch_up_seconds: $catch_up_bound, settlement_seconds: $settle_bound},
          applied_configuration: {acknowledged: $applied[0].acknowledged_changes,
                                  uncertain: $applied[0].uncertain_changes,
                                  not_applied: $applied[0].not_applied,
                                  membership: $applied[0].membership,
                                  changes: "stale/applied.json"},
          compaction: "stale/compaction.ndjson",
          node_events: "stale/node-events.ndjson",
          findings: "stale/findings.ndjson"}' \
        >"${artifact_dir}/results/stale-follower-progress.json"

    recovery_final_boundary
}
