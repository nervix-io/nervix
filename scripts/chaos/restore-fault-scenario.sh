#!/usr/bin/env bash
# Sourced by run-baseline.sh for the restore-fault scenario, after backup-scenario.sh and
# restore-scenario.sh. Owns one fault against a restore of a quiesced archive whose padded guest
# saves a hold keeps in staging: a delay on the leader's egress toward both peers, which every
# checkpoint chunk the coordinator sends a peer waits through. The faults are a SIGKILL of the
# leader, an isolation of the leader from its peers and the restoring client, a SIGKILL of a state
# owner of the target domain, and a SIGKILL of a state owner that spans the failure and reclamation
# of a restore whose coordinator and client were both lost. The run judges the restore's outcome
# against the restore contract, requires START of a target whose restore failed to stay gated,
# completes a restore under a new name where one failed, compares a restored stopped domain with
# the archive or judges a resumed one by its traffic, and requires staging back to zero bytes on
# every node through completed sweeps.

# shellcheck source=partition-scenario.sh
source "${script_dir}/partition-scenario.sh"

# The hold: each packet the leader sends a peer waits this long, and a 64 KiB checkpoint chunk takes
# several round trips, so each 2 MiB guest save stages on a peer for tens of seconds.
restore_fault_hold_ms=100
# The restore must be observed staging on a peer within this bound of its start.
restore_fault_staging_seconds=180
# The isolated leader stays isolated until the restore ends, at most this long. The client's reply
# wait of two minutes, its reconnection and the next leader's resumption all fit inside it.
restore_fault_isolation_seconds=360
# A restore whose coordinator and client were lost fails once the three-minute retry validity of its
# execution reference ends; its other target reclaims its staging within this bound of the restore's
# start.
restore_fault_reclamation_seconds=420

restore_fault_hold_name() {
    printf '%s-restore-hold\n' "${run_id}"
}

# Places the restore: reads the settled leader and the two followers into restore_fault_leader and
# restore_fault_followers as node names.
restore_fault_place() {
    local case_dir="$1"
    wait_for 'settled cluster before the restore' 120 cluster_settled "${case_dir}" \
        || restore_fail product 'the cluster did not settle before the restore'
    restore_fault_leader="$(<"${case_dir}/leader.txt")"
    restore_fault_followers=()
    local node
    for node in "${node_names[@]}"; do
        [[ "${node}" == "${restore_fault_leader}" ]] || restore_fault_followers+=("${node}")
    done
}

# The hosts of NODES..., space-separated.
restore_fault_hosts() {
    local node
    local hosts=()
    for node in "$@"; do
        hosts+=("$(restore_host "${node}")")
    done
    printf '%s\n' "${hosts[*]}"
}

# Starts the hold on the leader's egress toward both followers.
restore_fault_hold_start() {
    local case_dir="$1"
    local addresses=()
    local follower
    for follower in "${restore_fault_followers[@]}"; do
        addresses+=("$(node_address "$(restore_host "${follower}")")")
    done
    node_fault_netem_start "${case_dir}" "$(restore_fault_hold_name)" \
        "$(restore_host "${restore_fault_leader}")" node "${addresses[*]}" 3600 \
        "delay ${restore_fault_hold_ms}ms" delay --time "${restore_fault_hold_ms}" --jitter 0 --correlation 0
    restore_fault_hold_active=true
}

# Ends the hold on the leader, which is still running.
restore_fault_hold_heal() {
    [[ "${restore_fault_hold_active}" == true ]] || return 0
    node_fault_netem_heal "${restore_fault_case_dir}" "$(restore_fault_hold_name)" \
        "$(restore_host "${restore_fault_leader}")"
    restore_fault_hold_active=false
}

# A crash's DURING hook for the leader: the hold lived in the network namespace the killed leader
# container released, so its injector is killed instead of asked to remove rules from it.
restore_fault_hold_discard() {
    node_fault_netem_discard "${restore_fault_case_dir}" "$(restore_fault_hold_name)"
    restore_fault_hold_active=false
}

# Waits until the restore LABEL is observed staging on one of NODES..., and records that moment. A
# restore that ends first never let the fault meet its staging.
restore_fault_await_staging() {
    local case_dir="$1"
    local label="$2"
    shift 2
    local hosts
    read -r -a hosts <<<"$(restore_fault_hosts "$@")"
    wait_for "the restore staging on $*" "${restore_fault_staging_seconds}" \
        restore_fault_staging_or_ended "${case_dir}" "${label}" "${hosts[@]}" \
        || restore_fail injection "the restore was never observed staging on $*"
    if restore_ended "${case_dir}" "${label}"; then
        restore_fail injection "the restore ended with status $(<"${case_dir}/${label}.exit") before it was observed staging"
    fi
    restore_fault_staging_ms="$(epoch_ms)"
    restore_staging_hosts >"${case_dir}/staging-observed.json"
}

restore_fault_staging_or_ended() {
    local case_dir="$1"
    local label="$2"
    shift 2
    restore_ended "${case_dir}" "${label}" && return 0
    restore_staging_on "$@"
}

# The followers that are staging the restore, as node names in the order the run placed them.
restore_fault_staging_followers() {
    local staging
    staging="$(restore_staging_hosts)" || return 1
    local follower
    for follower in "${restore_fault_followers[@]}"; do
        if jq -e --arg host "$(restore_host "${follower}")" 'index($host) != null' <<<"${staging}" >/dev/null; then
            printf '%s\n' "${follower}"
        fi
    done
}

# Waits for the restore LABEL's CLI to end within the restore command bound and judges it.
restore_fault_await_outcome() {
    local case_dir="$1"
    local label="$2"
    local target="$3"
    wait_for "the restore ${label} ended" "${restore_command_seconds}" restore_ended "${case_dir}" "${label}" \
        || restore_fail product "the restore ${label} did not end within ${restore_command_seconds} seconds"
    wait "${restore_pid}" || true
    restore_judge "${case_dir}" "${label}" "${target}"
}

# Settles the first restore's outcome: a completed one is the restored domain; a failed one must
# leave START of its target gated, and a restore under a new name through SERVER_HOST, with the
# first restore's options, must then complete. Sets restore_fault_restored to the completed target,
# or to nothing when neither completed.
restore_fault_settle_outcome() {
    local case_dir="$1"
    local first_outcome="$2"
    local server_host="$3"
    shift 3
    local archive_dir="${case_dir}/archive"
    restore_fault_restored=""
    restore_fault_second_outcome=null
    restore_fault_second_command_ms=null
    case "${first_outcome}" in
        completed)
            restore_fault_restored=chaos_restored
            return 0
            ;;
        incomplete | lost)
            phase 'restore fault: START of the failed target'
            restore_require_gated "${case_dir}" chaos_restored
            ;;
    esac
    phase 'restore fault: restore under a new name'
    restore_run "${case_dir}" second "${archive_dir}" "${archive_dir}/domain.nvxb" chaos_restored_2 \
        "${server_host}" "$@"
    restore_fault_second_outcome="${restore_outcome}"
    restore_fault_second_command_ms="$(restore_command_ms "${case_dir}" second)"
    if [[ "${restore_outcome}" == completed ]]; then
        restore_fault_restored=chaos_restored_2
    else
        recovery_finding "${case_dir}" "the restore under a new name ended ${restore_outcome} after the fault"
    fi
}

# True when every node reports one agreed leader and term with its whole log applied, every node a
# voter, no terminating incarnation, and no cordoned node but CORDONED when one is given. Unlike
# cluster_settled it reads no domain, so it holds while the source domain is stopped, and it holds
# while the run keeps a node cordoned.
restore_fault_raft_settled() {
    local output_dir="$1"
    local cordoned="${2:-}"
    mkdir -p "${output_dir}"
    local agreed=""
    local host
    for host in "${node_hosts[@]}"; do
        cli_host="${host}"
        local file="${output_dir}/status-${host}.attempt.txt"
        local status=0
        cli_command 'SHOW CLUSTER STATUS;' >"${file}" 2>&1 || status=$?
        cli_host="${node_hosts[0]}"
        ((status == 0)) || return 1
        grep -Fxq "raft.cordoned_nodes: ${cordoned:-(none)}" "${file}" || return 1
        ! grep -Fq 'terminating: true' "${file}" || return 1
        local leader term last_log last_applied
        leader="$(awk -F ': ' '$1 == "raft.current_leader" {print $2}' "${file}")"
        term="$(awk -F ': ' '$1 == "raft.current_term" {print $2}' "${file}")"
        last_log="$(awk -F ': ' '$1 == "raft.last_log_index" {print $2}' "${file}")"
        last_applied="$(awk -F ': ' '$1 == "raft.last_applied" {print $2}' "${file}")"
        [[ "${leader}" =~ ^node-[123]$ && "${term}" =~ ^[0-9]+$ && "${last_log}" =~ ^[0-9]+$ ]] || return 1
        [[ "${last_log}" == "${last_applied}" ]] || return 1
        local node
        for node in "${node_names[@]}"; do
            grep -Eq "^- ${node} \\[voter\\] nervix-[123]:47395$" "${file}" || return 1
        done
        if [[ -n "${agreed}" && "${agreed}" != "${leader} ${term} ${last_log}" ]]; then
            return 1
        fi
        agreed="${leader} ${term} ${last_log}"
    done
    printf '%s\n' "${agreed%% *}" >"${output_dir}/leader.txt"
}

# Waits for the source domain to settle after the fault and its traffic to move past the boundaries
# recorded at the fault.
restore_fault_source_continues() {
    local case_dir="$1"
    phase 'restore fault: source domain recovery'
    mkdir -p "${case_dir}/settled"
    wait_for 'settled cluster after the fault' 240 cluster_settled "${case_dir}/settled" \
        || recovery_finding "${case_dir}" 'the cluster did not settle within 240 seconds of the fault'
    check_support_containers
    wait_for 'Kafka source advanced past the fault' 60 \
        topic_progressed chaos_input "${restore_fault_source_at_fault}" \
        || recovery_finding "${case_dir}" 'the source did not advance after the fault'
    wait_for 'Kafka sink advanced past the fault' 180 \
        topic_progressed chaos_output "${restore_fault_output_at_fault}" \
        || recovery_finding "${case_dir}" 'the source domain did not resume its output after the fault'
}

# Records the traffic boundaries when the fault lands.
restore_fault_traffic_at_fault() {
    restore_fault_source_at_fault="$(topic_end_offset chaos_input)"
    restore_fault_output_at_fault="$(topic_end_offset chaos_output)"
}

# leader-crash: SIGKILLs the leader, the restore's coordinator, while the restore stages through
# the hold, and starts it again. The client resends the archive to the next leader under the same
# execution reference, which resumes the restore from its first step not recorded.
restore_fault_leader_crash() {
    local case_dir="$1"
    local archive_dir="${case_dir}/archive"
    local server_host
    server_host="$(restore_host "${restore_fault_followers[0]}")"
    restore_fault_hold_start "${case_dir}"
    phase "restore fault: restore through ${server_host}"
    restore_start "${case_dir}" first "${archive_dir}" "${archive_dir}/domain.nvxb" chaos_restored "${server_host}"
    restore_fault_await_staging "${case_dir}" first "${restore_fault_followers[@]}"
    local leader_host
    leader_host="$(restore_host "${restore_fault_leader}")"
    phase "restore fault: SIGKILL of leader ${leader_host} while the restore stages"
    restore_fault_traffic_at_fault
    node_fault_kill "${case_dir}" "${leader_host}" "${outage_seconds}" '' restore_fault_hold_discard
    restore_fault_fault_ms="${node_fault_started_ms}"
    node_fault_check_events "${case_dir}"
    restore_fault_await_outcome "${case_dir}" first chaos_restored
    restore_fault_first_outcome="${restore_outcome}"
    restore_fault_reference="${restore_reference}"
    restore_fault_settle_outcome "${case_dir}" "${restore_fault_first_outcome}" "${server_host}"
    restore_fault_source_continues "${case_dir}"
}

# owner-crash: SIGKILLs a follower that holds the restore's unpublished staging, through the hold,
# and starts it again. The coordinator does not stage again on a node it lost, so a loss that
# interrupts a staging request fails the restore with START gated; a follower whose staging was
# already complete lets the restore publish once it is back.
restore_fault_owner_crash() {
    local case_dir="$1"
    local archive_dir="${case_dir}/archive"
    local server_host
    server_host="$(restore_host "${restore_fault_leader}")"
    restore_fault_hold_start "${case_dir}"
    phase "restore fault: restore through ${server_host}"
    restore_start "${case_dir}" first "${archive_dir}" "${archive_dir}/domain.nvxb" chaos_restored "${server_host}"
    restore_fault_await_staging "${case_dir}" first "${restore_fault_followers[@]}"
    local staging_followers=()
    mapfile -t staging_followers < <(restore_fault_staging_followers)
    local owner="${staging_followers[0]:-}"
    [[ -n "${owner}" ]] || restore_fail injection 'no follower that staged the restore remained to be killed'
    restore_fault_target_node="${owner}"
    local owner_host
    owner_host="$(restore_host "${owner}")"
    phase "restore fault: SIGKILL of state owner ${owner_host} while the restore stages"
    restore_fault_traffic_at_fault
    node_fault_kill "${case_dir}" "${owner_host}" "${outage_seconds}" '' restore_fault_hold_heal
    restore_fault_fault_ms="${node_fault_started_ms}"
    node_fault_check_events "${case_dir}"
    restore_fault_await_outcome "${case_dir}" first chaos_restored
    restore_fault_first_outcome="${restore_outcome}"
    restore_fault_reference="${restore_reference}"
    restore_fault_settle_outcome "${case_dir}" "${restore_fault_first_outcome}" "${server_host}"
    restore_fault_source_continues "${case_dir}"
}

# reclamation-owner-crash: the restore's client and then its coordinator, the leader, are lost
# while the restore stages, so no client sends the archive to the next leader and the restore stays
# applying, its staging retained, until the retry validity of its execution reference ends. A
# follower that staged it is SIGKILLed before then and started again only once the other target has
# reclaimed its staging, so the restored follower finds the restore failed when it catches up and
# reclaims its own staging after its restart. The leader is cordoned for the restore, so both its
# targets are followers.
restore_fault_reclamation_owner_crash() {
    local case_dir="$1"
    local archive_dir="${case_dir}/archive"
    local leader_host
    leader_host="$(restore_host "${restore_fault_leader}")"
    cli_command "CORDON NODE ${restore_fault_leader};" >"${case_dir}/cordon.txt" 2>&1 \
        || restore_fail product "the leader ${restore_fault_leader} could not be cordoned"
    restore_fault_cordoned=true
    restore_fault_hold_start "${case_dir}"
    local server_host
    server_host="$(restore_host "${restore_fault_followers[0]}")"
    phase "restore fault: restore through ${server_host}"
    restore_start "${case_dir}" first "${archive_dir}" "${archive_dir}/domain.nvxb" chaos_restored "${server_host}"
    local started_ms
    started_ms="$(<"${case_dir}/first.started-ms")"
    local follower_hosts
    read -r -a follower_hosts <<<"$(restore_fault_hosts "${restore_fault_followers[@]}")"
    wait_for 'the restore staging on both followers' "${restore_fault_staging_seconds}" \
        restore_staging_on_all "${follower_hosts[@]}" \
        || restore_fail injection 'the restore was never observed staging on both followers'
    restore_ended "${case_dir}" first \
        && restore_fail injection 'the restore ended before it was observed staging on both followers'
    restore_fault_staging_ms="$(epoch_ms)"
    restore_staging_hosts >"${case_dir}/staging-observed.json"

    phase 'restore fault: client lost'
    run_bounded 30 docker kill "${restore_container}" >"${case_dir}/client-kill.txt" 2>&1 \
        || restore_fail injection 'the restoring client could not be killed'
    wait "${restore_pid}" || true
    phase "restore fault: SIGKILL of coordinating leader ${leader_host}"
    restore_fault_traffic_at_fault
    mkdir -p "${case_dir}/leader-crash"
    node_fault_kill "${case_dir}/leader-crash" "${leader_host}" "${outage_seconds}" '' restore_fault_hold_discard
    node_fault_check_events "${case_dir}/leader-crash"
    wait_for 'settled cluster after the leader crash' 240 \
        restore_fault_raft_settled "${case_dir}/after-leader-crash" "${restore_fault_leader}" \
        || restore_fail product 'the cluster did not settle after the coordinating leader crashed'
    local next_leader
    next_leader="$(<"${case_dir}/after-leader-crash/leader.txt")"
    local owner=""
    local follower
    for follower in "${restore_fault_followers[@]}"; do
        [[ "${follower}" == "${next_leader}" ]] || owner="${follower}"
    done
    [[ -n "${owner}" && "${owner}" != "${restore_fault_leader}" ]] \
        || restore_fail injection "the next leader ${next_leader} left no other follower that staged the restore"
    restore_fault_target_node="${owner}"
    local owner_host other_host
    owner_host="$(restore_host "${owner}")"
    other_host="$(restore_host "${next_leader}")"
    restore_staging_on "${owner_host}" \
        || recovery_finding "${case_dir}" "${owner_host} no longer retained the staging of the restore whose coordinator and client were lost"

    phase "restore fault: SIGKILL of state owner ${owner_host} before the restore fails"
    mkdir -p "${case_dir}/owner-crash"
    node_fault_kill_inject "${case_dir}/owner-crash" "${outage_seconds}" '' "${owner_host}"
    restore_fault_fault_ms="${node_fault_started_ms}"
    local remaining=$((restore_fault_reclamation_seconds - ($(epoch_ms) - started_ms) / 1000))
    ((remaining > 0)) || remaining=1
    wait_for "${other_host} reclaiming the failed restore's staging" "${remaining}" \
        restore_fault_host_reclaimed "${other_host}" "${restore_fault_fault_ms}" \
        || recovery_finding "${case_dir}" "${other_host} did not reclaim the staging of the restore that lost its coordinator and client within ${restore_fault_reclamation_seconds} seconds of its start"
    restore_fault_reclaimed_ms="$(epoch_ms)"
    phase "restore fault: start of state owner ${owner_host} after the failure"
    node_fault_kill_restart "${case_dir}/owner-crash" "${owner_host}"
    wait_for "${owner_host} listeners restored" 120 probe_node "${owner_host}"
    node_fault_expected_events=(--target "$(owned_service_container "${owner_host}")"
        --expect kill:9 --expect die:137 --expect start)
    node_fault_check_events "${case_dir}/owner-crash"
    restore_fault_first_outcome=lost
    restore_fault_reference=""
    cli_command "UNCORDON NODE ${restore_fault_leader};" >"${case_dir}/uncordon.txt" 2>&1 \
        || recovery_finding "${case_dir}" "the leader ${restore_fault_leader} could not be uncordoned"
    restore_fault_cordoned=false
    restore_fault_settle_outcome "${case_dir}" lost "${server_host}"
    restore_fault_source_continues "${case_dir}"
}

# True when HOST completed a sweep that measured no staging after the epoch millisecond SINCE_MS.
restore_fault_host_reclaimed() {
    local host="$1"
    local since_ms="$2"
    restore_samples_now
    jq -se --arg host "${host}" --argjson since "$((since_ms / 1000))" '
        [.[] | select(.host == $host and .at > $since and (.series | has("nervix_restore_staging_bytes")))]
        as $after
        | [range(1; $after | length) as $index
           | $after[$index - 1] as $before
           | $after[$index] as $now
           | $now.series.nervix_restore_staging_bytes == 0 and $now.series.nervix_restore_staging_keys == 0
             and $now.series.nervix_restore_staging_sweeps_total
                 > $before.series.nervix_restore_staging_sweeps_total]
        | any
    ' "${restore_sampler_dir}/current.ndjson" >/dev/null 2>&1
}

# The restoring client's address on the run network.
restore_fault_client_address() {
    local address
    address="$(container_address "${restore_container}")" || return 1
    [[ "${address}" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || return 1
    printf '%s\n' "${address}"
}

# True when the leader carries exactly the hold and the client cut, and every follower exactly its
# planned isolation rules, as inspected into PARTITION_DIR.
restore_fault_isolation_installed() {
    local partition_dir="$1"
    local client_address="$2"
    injectors_running "${partition_dir}" || return 2
    local leader_host
    leader_host="$(restore_host "${restore_fault_leader}")"
    node_fault_netem_installed "${leader_host}" "delay ${restore_fault_hold_ms}ms" \
        "${partition_dir}/rules-installed/${leader_host}.txt" || return 1
    local rules
    rules="$(awk '/^# iptables$/ { s = 1; next } /^# / { s = 0 } s && /^-A /' \
        "${partition_dir}/rules-installed/${leader_host}.txt")"
    [[ "${rules}" == "-A INPUT -s ${client_address}/32 -i eth0 -m statistic --mode random --probability 1.00000000000 -j DROP" ]] \
        || return 1
    local follower
    for follower in "${restore_fault_followers[@]}"; do
        local host
        host="$(restore_host "${follower}")"
        inspect_node_rules "${host}" "${partition_dir}/rules-installed/${host}.txt" || return 1
        "${script_dir}/verify-partition-evidence.sh" rules "${partition_dir}/plan.json" "${host}" \
            "${partition_dir}/rules-installed/${host}.txt" \
            2>"${partition_dir}/rules-installed/${host}.verdict.txt" || return 1
    done
}

# Isolates the leader from both followers with the peer-side rules of an ordinary isolation, and
# from the restoring client with an INPUT rule on the leader that drops what the client sends it.
# The leader keeps the hold, which a plain partition would refuse as an overlapping fault, so this
# installation checks the hold beside the planned rules. heal_partition later ends all of them,
# the hold included, and requires every interface and link back in its default state.
restore_fault_isolate() {
    local case_dir="$1"
    local partition_dir="${case_dir}/partition"
    mkdir -p "${partition_dir}/injectors" "${partition_dir}/rules-before" "${partition_dir}/rules-installed"
    local leader_host
    leader_host="$(restore_host "${restore_fault_leader}")"
    plan_isolation isolation "${leader_host}" "${partition_dir}/plan.json"
    failure_category=injection
    "${script_dir}/verify-partition-evidence.sh" plan "${partition_dir}/plan.json" \
        || restore_fail controller 'the isolation plan does not block exactly the intended links'
    local follower
    for follower in "${restore_fault_followers[@]}"; do
        local host
        host="$(restore_host "${follower}")"
        inspect_node_rules "${host}" "${partition_dir}/rules-before/${host}.txt"
        "${script_dir}/verify-partition-evidence.sh" rules "${partition_dir}/plan.json" "${host}" \
            "${partition_dir}/rules-before/${host}.txt" --healed \
            || restore_fail injection "${host} already carries a qdisc, filter or INPUT rule; refusing to overlap faults"
    done
    local client_address
    client_address="$(restore_fault_client_address)" \
        || restore_fail controller 'the restoring client has no address on the run network'
    printf '%s\n' "${client_address}" >"${partition_dir}/client-address.txt"
    # heal_partition stops every injector it finds a command for, so the hold joins them.
    jq -n --arg host "${leader_host}" --arg injector "$(restore_fault_hold_name)" \
        --arg injector_id "$(<"${case_dir}/$(restore_fault_hold_name)-injector-id.txt")" \
        '{host:$host, kind:"netem", injector:$injector, injector_id:$injector_id, role:"restore hold"}' \
        >"${partition_dir}/injectors/${leader_host}-netem.json"
    fault_started_at="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    fault_started_ms="$(epoch_ms)"
    for follower in "${restore_fault_followers[@]}"; do
        local host
        host="$(restore_host "${follower}")"
        start_injector "${partition_dir}" 1 "${host}" netem "$(node_address "${leader_host}")"
        start_injector "${partition_dir}" 1 "${host}" iptables "$(node_address "${leader_host}")"
    done
    start_injector "${partition_dir}" 1 "${leader_host}" iptables "${client_address}"
    local installed=1
    local deadline=$((SECONDS + 60))
    while ((SECONDS < deadline)); do
        installed=0
        restore_fault_isolation_installed "${partition_dir}" "${client_address}" || installed=$?
        ((installed == 0 || installed == 2)) && break
        sleep 1
    done
    ((installed == 0)) || restore_fail injection 'the isolation rules did not match the plan within 60 seconds'
    rules_installed_ms="$(epoch_ms)"
    probe_links "${partition_dir}/links-isolated.json"
    "${script_dir}/verify-partition-evidence.sh" links "${partition_dir}/plan.json" \
        "${partition_dir}/links-isolated.json" "${partition_dir}/links-isolated-verdict.json" \
        || restore_fail injection 'the installed rules did not block exactly the leader links'
    isolation_verified_at="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    isolation_verified_ms="$(epoch_ms)"
    restore_fault_hold_active=false
    failure_category=product
}

# leader-partition: stops the source domain, cordons the leader and restores the archive with
# RESUME through a follower. While it stages through the hold, the leader is isolated from both
# followers and from the client, which then resends the archive to the next leader. The isolation
# holds until the restore ends, so the next leader completes it while the stale coordinator stays
# cut off; after the heal, the resumed domain's traffic shows what the stale coordinator could not
# replace or clear.
restore_fault_leader_partition() {
    local case_dir="$1"
    local archive_dir="${case_dir}/archive"
    phase 'restore fault: source domain stopped'
    domain_cli_command 'STOP;' >"${case_dir}/stop-source.txt" 2>&1 \
        || restore_fail product 'the source domain could not be stopped'
    wait_for 'the source ingestor stopped' 60 backup_original_stopped "${case_dir}" \
        || restore_fail product 'the source domain did not stop its ingestor'
    backup_output_boundary=""
    wait_for 'the source output settled' 120 backup_output_settled \
        || restore_fail product 'the source domain kept emitting after it stopped'
    restore_fault_source_output_end="${backup_output_boundary}"
    restore_fault_source_at_stop="$(topic_end_offset chaos_input)"
    cli_command "CORDON NODE ${restore_fault_leader};" >"${case_dir}/cordon.txt" 2>&1 \
        || restore_fail product "the leader ${restore_fault_leader} could not be cordoned"
    restore_fault_cordoned=true
    restore_fault_hold_start "${case_dir}"
    local server_host
    server_host="$(restore_host "${restore_fault_followers[0]}")"
    phase "restore fault: RESUME restore through ${server_host}"
    restore_start "${case_dir}" first "${archive_dir}" "${archive_dir}/domain.nvxb" chaos_restored \
        "${server_host}" --resume
    restore_fault_await_staging "${case_dir}" first "${restore_fault_followers[@]}"
    local leader_host
    leader_host="$(restore_host "${restore_fault_leader}")"
    phase "restore fault: isolation of coordinating leader ${leader_host}"
    restore_fault_traffic_at_fault
    restore_fault_isolate "${case_dir}"
    restore_fault_fault_ms="${fault_started_ms}"
    wait_for 'the restore ending while the leader is isolated' "${restore_fault_isolation_seconds}" \
        restore_ended "${case_dir}" first \
        || recovery_finding "${case_dir}" "the restore did not end within ${restore_fault_isolation_seconds} seconds of the isolation"
    restore_fault_isolated_end=false
    restore_ended "${case_dir}" first && restore_fault_isolated_end=true
    phase "restore fault: heal of coordinating leader ${leader_host}"
    heal_partition "${case_dir}/partition" 1
    restore_fault_healed_ms="${heal_completed_ms}"
    restore_fault_await_outcome "${case_dir}" first chaos_restored
    restore_fault_first_outcome="${restore_outcome}"
    restore_fault_reference="${restore_reference}"
    cli_command "UNCORDON NODE ${restore_fault_leader};" >"${case_dir}/uncordon.txt" 2>&1 \
        || recovery_finding "${case_dir}" "the leader ${restore_fault_leader} could not be uncordoned"
    restore_fault_cordoned=false
    restore_fault_settle_outcome "${case_dir}" "${restore_fault_first_outcome}" "${server_host}" --resume
    wait_for 'settled cluster after the heal' 240 restore_fault_raft_settled "${case_dir}/settled" \
        || recovery_finding "${case_dir}" 'the cluster did not settle within 240 seconds of the heal'
    [[ -n "${restore_fault_restored}" ]] || return 0
    phase "restore fault: traffic of the resumed ${restore_fault_restored}"
    local archived_offset
    archived_offset="$(backup_archived_offset "${archive_dir}/description.json")"
    wait_for 'the resumed domain emitting' 180 \
        topic_progressed chaos_output "${restore_fault_source_output_end}" \
        || recovery_finding "${case_dir}" "the resumed ${restore_fault_restored} never emitted"
    wait_for 'the resumed domain consuming past the source stop' 180 \
        topic_progressed chaos_output \
        "$((restore_fault_source_output_end + restore_fault_source_at_stop - archived_offset))" \
        || recovery_finding "${case_dir}" "the resumed ${restore_fault_restored} did not consume past where the source domain stopped"
    local output_past_stop
    output_past_stop="$(topic_end_offset chaos_output)"
    # Seventy-two more records close two more windows of every branch.
    wait_for 'the resumed domain closing two more windows of every branch' 180 \
        topic_progressed chaos_output "$((output_past_stop + 71))" \
        || recovery_finding "${case_dir}" "the resumed ${restore_fault_restored} stopped consuming before two more windows of every branch closed"
    check_support_containers
}

# Judges, after the final boundary, the output of the domain the leader-partition run resumed: its
# first record must be the source record at the archived offset, and it must emit exactly the
# source records from that offset onward.
restore_fault_resumed_output() {
    local case_dir="$1"
    local archive_dir="${case_dir}/archive"
    local archived_offset
    archived_offset="$(backup_archived_offset "${archive_dir}/description.json")"
    local output_end
    output_end="$(topic_end_offset chaos_output)"
    kcat -q -b broker:9092 -C -t chaos_output -p 0 -o beginning -c "${output_end}" \
        >"${case_dir}/output-at-resumption.ndjson" 2>"${case_dir}/output-consumer.stderr"
    restore_fault_first_resumed="$(sed -n "$((restore_fault_source_output_end + 1))p" \
        "${case_dir}/output-at-resumption.ndjson" | jq -r '.event_id')"
    local archived_record
    archived_record="$(sed -n "$((archived_offset + 1))p" "${artifact_dir}/traffic/accepted-input.ndjson" \
        | jq -r '.event_id')"
    [[ -n "${restore_fault_first_resumed}" && "${restore_fault_first_resumed}" == "${archived_record}" ]] \
        || recovery_finding "${case_dir}" "the resumed domain resumed at ${restore_fault_first_resumed:-nothing}, not at the archived offset's ${archived_record:-missing record}"
    tail -n +"$((restore_fault_source_output_end + 1))" "${case_dir}/output-at-resumption.ndjson" \
        | jq -r '.event_id' | sort -u >"${case_dir}/resumed-emitted.txt"
    tail -n +"$((archived_offset + 1))" "${artifact_dir}/traffic/accepted-input.ndjson" \
        | jq -r '.event_id' | sort -u >"${case_dir}/resumed-expected.txt"
    cmp -s "${case_dir}/resumed-emitted.txt" "${case_dir}/resumed-expected.txt" \
        || recovery_finding "${case_dir}" 'the resumed domain emitted a record outside, or missed one inside, the source range after the archived offset'
}

run_restore_fault() {
    local case_dir="${artifact_dir}/restore-fault"
    local archive_dir="${case_dir}/archive"
    mkdir -p "${archive_dir}"
    restore_fault_case_dir="${case_dir}"
    restore_fault_hold_active=false
    restore_fault_cordoned=false
    restore_fault_staging_ms=null
    restore_fault_fault_ms=null
    restore_fault_healed_ms=null
    restore_fault_reclaimed_ms=null
    restore_fault_target_node=""
    restore_fault_first_outcome=""
    restore_fault_reference=""
    restore_fault_restored=""
    restore_fault_comparison=null
    restore_fault_first_resumed=""
    restore_fault_second_outcome=null
    restore_fault_second_command_ms=null
    restore_settled_ms=null
    recovery_traffic_startup "${case_dir}"
    failure_category=product

    phase 'restore fault: padded guest state'
    restore_seed_guest_state "${case_dir}"
    phase 'restore fault: quiesced backup under load'
    restore_take_archive "${case_dir}" "${restore_guest_bytes}"
    restore_sampler_start "${case_dir}"
    phase 'restore fault: placement'
    restore_fault_place "${case_dir}"
    case "${fault}" in
        leader-crash) restore_fault_leader_crash "${case_dir}" ;;
        leader-partition) restore_fault_leader_partition "${case_dir}" ;;
        owner-crash) restore_fault_owner_crash "${case_dir}" ;;
        reclamation-owner-crash) restore_fault_reclamation_owner_crash "${case_dir}" ;;
    esac
    local ended_at
    ended_at="$(date +%s)"

    phase 'restore fault: staging back to zero'
    restore_staging_returns_to_zero "${case_dir}" "${ended_at}"
    restore_sampler_stop
    if [[ "${fault}" != leader-partition && -n "${restore_fault_restored}" ]]; then
        phase "restore fault: ${restore_fault_restored} against the archive"
        if restore_require_equal "${case_dir}" "${archive_dir}" "${archive_dir}/description.json" \
            "${restore_fault_restored}"; then
            restore_fault_comparison=true
        else
            restore_fault_comparison=false
        fi
    fi
    check_support_containers
    jq -n \
        --arg fault "${fault}" \
        --arg leader "${restore_fault_leader}" \
        --argjson followers "$(printf '%s\n' "${restore_fault_followers[@]}" | jq -R . | jq -sc .)" \
        --arg target_node "${restore_fault_target_node}" \
        --arg first "${restore_fault_first_outcome}" \
        --arg reference "${restore_fault_reference}" \
        --argjson first_ms "$(restore_command_ms "${case_dir}" first)" \
        --argjson second "$(jq -nc --arg outcome "${restore_fault_second_outcome}" 'if $outcome == "null" then null else $outcome end')" \
        --argjson second_ms "${restore_fault_second_command_ms}" \
        --arg restored "${restore_fault_restored}" \
        --argjson comparison "${restore_fault_comparison}" \
        --argjson started "$(<"${case_dir}/first.started-ms")" \
        --argjson staging "${restore_fault_staging_ms}" \
        --argjson fault_ms "${restore_fault_fault_ms}" \
        --argjson healed "${restore_fault_healed_ms}" \
        --argjson reclaimed "${restore_fault_reclaimed_ms}" \
        --argjson settled "${restore_settled_ms}" \
        --argjson hold_ms "${restore_fault_hold_ms}" \
        --argjson guest_bytes "${restore_guest_bytes}" \
        --arg first_resumed "${restore_fault_first_resumed}" \
        '{fault:$fault, placement:{leader:$leader, followers:$followers,
                                    fault_target:(if $target_node == "" then null else $target_node end)},
          hold:{delay_ms:$hold_ms, guest_save_bytes:$guest_bytes},
          first_restore:{outcome:$first,
                         execution_reference:(if $reference == "" then null else $reference end),
                         command_ms:$first_ms},
          restore_under_new_name:{outcome:$second, command_ms:$second_ms},
          restored_domain:(if $restored == "" then null else $restored end),
          equal_to_archive:$comparison,
          first_resumed_event:(if $first_resumed == "" then null else $first_resumed end),
          timings_ms:{restore_started:$started, staging_observed:$staging, fault:$fault_ms,
                      healed:$healed, reclaimed_by_other_target:$reclaimed,
                      staging_settled_after_restores:$settled},
          evidence:{first_restore:"restore-fault/first.json",
                    staging_observed:"restore-fault/staging-observed.json",
                    samples:"restore-fault/samples/samples.ndjson"}}' \
        >"${artifact_dir}/results/restore-fault-progress.json"
    recovery_final_boundary
    # A SIGKILL that loses the source domain's window owner promotes a snapshot of the detached
    # window that can predate rows relay fan-out already acknowledged upstream, which are never
    # delivered again, so only the isolation, which crashes nothing, judges the deduplicator and
    # window outputs exactly.
    if [[ "${fault}" == leader-partition ]]; then
        if [[ -n "${restore_fault_restored}" ]]; then
            phase 'restore fault: exact resumption'
            restore_fault_resumed_output "${case_dir}"
            # The resumption is judged after the final boundary, so the progress record written
            # before it learns its first resumed record here.
            local progress="${artifact_dir}/results/restore-fault-progress.json"
            jq --arg event "${restore_fault_first_resumed}" \
                '.first_resumed_event = (if $event == "" then null else $event end)' \
                "${progress}" >"${progress}.next"
            mv "${progress}.next" "${progress}"
        fi
        phase 'restore fault: deduplicator and window outputs'
        backup_state_verdict "${case_dir}" \
            || recovery_finding "${case_dir}" 'the deduplicator or window outputs differ from what the accepted records produce'
    fi
}
