#!/usr/bin/env bash
# Sourced by crash-scenario.sh. A three-node leader-crash run also qualifies the committed outward
# TLS policy through the failover: every node serves the web console over HTTPS on 47421
# (compose.outward-tls.yaml), and tls-probe.py offers each one only TLS_CHACHA20_POLY1305_SHA256 and
# X25519, the members the `fips` alias excludes. The run judges five observations, each a public
# DESCRIBE CLUSTER and a probe of the console and of the interconnect of every node it names, with
# verify-outward-tls-evidence.sh. The interconnect's algorithms are fixed, so it refuses the probe in
# every observation, also while the committed outward policy admits both members:
#
#   before     every node refuses the probe under the bootstrap `fips` policy
#   commit     a policy that adds both members is committed through a survivor while the killed
#              leader is down, retried until a command is acknowledged
#   survivors  the survivors run the new revision and accept the probe
#   recovered  every node, the restarted former leader included, runs that revision and accepts the
#              probe, observed through an authenticated session to the restarted node
#   reverted   after the `fips` policy is committed again, every node runs it and refuses the probe

# shellcheck disable=SC2034
outward_tls_permissive_statement="ALTER CLUSTER SET TLS_CIPHER_SUITES = ('fips', 'TLS13_CHACHA20_POLY1305_SHA256'), TLS_KEY_EXCHANGE_GROUPS = ('fips', 'X25519');"
outward_tls_fips_statement="ALTER CLUSTER SET TLS_CIPHER_SUITES = ('fips'), TLS_KEY_EXCHANGE_GROUPS = ('fips');"
# The answer of an acknowledged ALTER CLUSTER SET, sent once the leader applied the new revision.
outward_tls_acknowledgement='cluster parameters changed'

outward_tls_qualified() {
    [[ "${scenario}" == leader-crash && "${node_count}" == 3 ]]
}

# outward_tls_probe OUTPUT PORT HOST...: one tls-probe.py document for PORT on every HOST. Exit
# status 1 means a target failed, which the verifier judges; anything else left no judgeable
# document.
outward_tls_probe() {
    local output="$1"
    local port="$2"
    shift 2
    local probe_status=0
    compose run --rm --no-deps -T tls-probe --port "${port}" "$@" \
        >"${output}.json" 2>"${output}.stderr" </dev/null \
        || probe_status=$?
    [[ "${probe_status}" -eq 0 || "${probe_status}" -eq 1 ]]
}

# outward_tls_observe DIRECTORY POLICY DESCRIBING_HOST AFTER_REVISION HOST...
# One observation attempt: DESCRIBE CLUSTER through DESCRIBING_HOST, then one probe of every HOST's
# console, judged against POLICY (permits or excludes), and one of every HOST's interconnect, which
# must refuse whatever the policy. AFTER_REVISION is empty or the revision the committed one must
# exceed. The judged attempt replaces the directory's previous observation.
outward_tls_observe() {
    local directory="$1"
    local policy="$2"
    local describing_host="$3"
    local after_revision="$4"
    shift 4
    mkdir -p "${directory}"
    cli_host="${describing_host}"
    cli_command 'DESCRIBE CLUSTER;' >"${directory}/description.attempt.txt" 2>&1 || return 1
    outward_tls_probe "${directory}/probe.attempt" 47421 "$@" || return 1
    outward_tls_probe "${directory}/interconnect-probe.attempt" 47395 "$@" || return 1
    local verifier_args=(--policy "${policy}" --description "${directory}/description.attempt.txt"
        --probe "${directory}/probe.attempt.json"
        --interconnect-probe "${directory}/interconnect-probe.attempt.json"
        --result "${directory}/verdict.attempt.json")
    if [[ -n "${after_revision}" ]]; then
        verifier_args+=(--after-revision "${after_revision}")
    fi
    local host
    for host in "$@"; do
        verifier_args+=(--node "node-${host##*-}=${host}")
    done
    "${script_dir}/verify-outward-tls-evidence.sh" "${verifier_args[@]}" \
        2>"${directory}/verdict.attempt.stderr" || return 1
    local name
    for name in description.txt probe.json probe.stderr interconnect-probe.json interconnect-probe.stderr \
        verdict.json verdict.stderr; do
        mv "${directory}/${name%.*}.attempt.${name##*.}" "${directory}/${name}"
    done
}

# outward_tls_revision DIRECTORY: the committed revision of the directory's passing observation.
outward_tls_revision() {
    jq -er '.revision | select(test("^[1-9][0-9]*$"))' "$1/verdict.json"
}

# outward_tls_commit_attempt HOST DIRECTORY: one ALTER CLUSTER SET of the permissive policy through
# HOST. Every attempt keeps its transcript and outcome; the call succeeds once one is acknowledged.
outward_tls_commit_attempt() {
    local host="$1"
    local directory="$2"
    outward_tls_commit_attempts=$((outward_tls_commit_attempts + 1))
    local attempt="${outward_tls_commit_attempts}"
    local started_ms status=0 outcome=uncertain
    started_ms="$(epoch_ms)"
    cli_host="${host}"
    cli_command "${outward_tls_permissive_statement}" >"${directory}/attempt-${attempt}.txt" 2>&1 \
        || status=$?
    if [[ "${status}" -eq 0 ]] && grep -Fq "${outward_tls_acknowledgement}" "${directory}/attempt-${attempt}.txt"; then
        outcome=acknowledged
    fi
    jq -nc \
        --argjson attempt "${attempt}" \
        --arg host "${host}" \
        --arg outcome "${outcome}" \
        --argjson exit_code "${status}" \
        --argjson started_ms "${started_ms}" \
        --argjson finished_ms "$(epoch_ms)" \
        '{attempt:$attempt,host:$host,outcome:$outcome,exit_code:$exit_code,started_ms:$started_ms,finished_ms:$finished_ms}' \
        >>"${directory}/attempts.ndjson"
    [[ "${outcome}" == acknowledged ]]
}

# outward_tls_before CRASH_DIR: the bootstrap policy, before the leader is killed.
outward_tls_before() {
    local directory="$1/outward-tls/before"
    wait_for 'every node refuses the restricted TLS client under the fips policy' 60 \
        outward_tls_observe "${directory}" excludes "${node_hosts[0]}" "" "${node_hosts[@]}" \
        || crash_fail product 'a node did not refuse the restricted TLS client under the bootstrap fips policy'
    outward_tls_before_revision="$(outward_tls_revision "${directory}")" \
        || crash_fail controller 'the bootstrap outward TLS observation has no committed revision'
}

# outward_tls_start_commit CRASH_DIR SURVIVOR: starts committing the permissive policy through
# SURVIVOR in the background as soon as the leader is killed, so the statement crosses the election
# and the re-placement while the scenario measures them undisturbed. Each attempt is bounded by the
# outage's per-call bound, and the attempts stop at the first acknowledgement or after 90 seconds.
outward_tls_start_commit() {
    local directory="$1/outward-tls/commit"
    local survivor="$2"
    mkdir -p "${directory}"
    : >"${directory}/attempts.ndjson"
    (
        outward_tls_commit_attempts=0
        if wait_for 'outward TLS policy committed through a survivor during the failover' 90 \
            outward_tls_commit_attempt "${survivor}" "${directory}"; then
            epoch_ms >"${directory}/acknowledged-ms"
        fi
    ) >"${directory}/commit.log" 2>&1 </dev/null &
    outward_tls_commit_pid=$!
}

# outward_tls_stop_commit: ends a background commit the scenario no longer waits for. The exit trap
# calls it.
outward_tls_stop_commit() {
    if [[ -n "${outward_tls_commit_pid:-}" ]]; then
        kill "${outward_tls_commit_pid}" 2>/dev/null
        wait "${outward_tls_commit_pid}" 2>/dev/null
        outward_tls_commit_pid=""
    fi
    return 0
}

# outward_tls_finish_commit CRASH_DIR CONTAINER_ID KILL_MS: waits for the background commit and
# requires an acknowledgement while the killed leader, CONTAINER_ID, is still down. The target stays
# down until the scenario's explicit restart, and the run's lifecycle verdict later admits no other
# start, so an acknowledgement before this check came while it was down.
outward_tls_finish_commit() {
    local directory="$1/outward-tls/commit"
    local container_id="$2"
    local kill_ms="$3"
    local status=0
    wait "${outward_tls_commit_pid}" || status=$?
    outward_tls_commit_pid=""
    outward_tls_commit_attempts="$(wc -l <"${directory}/attempts.ndjson")"
    [[ "${status}" -eq 0 && -s "${directory}/acknowledged-ms" ]] \
        || crash_fail product 'no outward TLS policy change was acknowledged while the leader was down'
    inspect_target "${container_id}" "${directory}/target-after-acknowledgement.json"
    jq -e '.[0].State.Running == false' "${directory}/target-after-acknowledgement.json" >/dev/null \
        || crash_fail injection 'the killed leader was running again before the outward TLS policy change was confirmed'
    local acknowledged_ms
    acknowledged_ms="$(<"${directory}/acknowledged-ms")"
    outward_tls_acknowledged_after_kill_ms="$((acknowledged_ms - kill_ms))"
}

# outward_tls_survivors CRASH_DIR SURVIVOR TARGET_HOST: the committed change on the survivors.
outward_tls_survivors() {
    local directory="$1/outward-tls/survivors"
    local survivor="$2"
    local target_host="$3"
    local survivors=()
    local host
    for host in "${node_hosts[@]}"; do
        [[ "${host}" == "${target_host}" ]] || survivors+=("${host}")
    done
    wait_for 'survivors run and apply the committed outward TLS policy' 60 \
        outward_tls_observe "${directory}" permits "${survivor}" "${outward_tls_before_revision}" "${survivors[@]}" \
        || crash_fail product 'the survivors did not run and apply the outward TLS policy committed during the failover'
    outward_tls_committed_revision="$(outward_tls_revision "${directory}")" \
        || crash_fail controller 'the survivors observation has no committed revision'
}

# outward_tls_recovered CRASH_DIR TARGET_HOST: the change on every node after the restart, read
# through an authenticated session to the restarted node.
outward_tls_recovered() {
    local directory="$1/outward-tls/recovered"
    local target_host="$2"
    wait_for 'every node, the restarted one included, runs and applies the committed outward TLS policy' 90 \
        outward_tls_observe "${directory}" permits "${target_host}" "${outward_tls_before_revision}" "${node_hosts[@]}" \
        || crash_fail product 'the restarted node did not run and apply the outward TLS policy committed while it was down'
    local recovered_revision
    recovered_revision="$(outward_tls_revision "${directory}")" \
        || crash_fail controller 'the recovered observation has no committed revision'
    [[ "${recovered_revision}" == "${outward_tls_committed_revision}" ]] \
        || crash_fail product "the committed outward TLS revision moved from ${outward_tls_committed_revision} to ${recovered_revision} without a change"
}

# outward_tls_reverted CRASH_DIR: the `fips` policy committed again, and refused on every node.
outward_tls_reverted() {
    local directory="$1/outward-tls/reverted"
    mkdir -p "${directory}"
    cli_host="${node_hosts[0]}"
    cli_command "${outward_tls_fips_statement}" >"${directory}/commit.txt" 2>&1 \
        && grep -Fq "${outward_tls_acknowledgement}" "${directory}/commit.txt" \
        || crash_fail product 'restoring the fips outward TLS policy was not acknowledged'
    wait_for 'every node refuses the restricted TLS client after the fips policy returns' 60 \
        outward_tls_observe "${directory}" excludes "${node_hosts[0]}" "${outward_tls_committed_revision}" "${node_hosts[@]}" \
        || crash_fail product 'a node still accepted the restricted TLS client after the fips policy returned'
    outward_tls_reverted_revision="$(outward_tls_revision "${directory}")" \
        || crash_fail controller 'the reverted observation has no committed revision'
}

# outward_tls_result CRASH_DIR TARGET_NODE: results/outward-tls.json, the summary of the five
# observations that crash-progress.json names.
outward_tls_result() {
    local crash_dir="$1"
    local target_node="$2"
    jq -n \
        --arg target "${target_node}" \
        --arg before "${outward_tls_before_revision}" \
        --arg committed "${outward_tls_committed_revision}" \
        --arg reverted "${outward_tls_reverted_revision}" \
        --argjson acknowledged_ms "${outward_tls_acknowledged_after_kill_ms}" \
        --argjson attempts "${outward_tls_commit_attempts}" \
        --slurpfile before_verdict "${crash_dir}/outward-tls/before/verdict.json" \
        --slurpfile survivors_verdict "${crash_dir}/outward-tls/survivors/verdict.json" \
        --slurpfile recovered_verdict "${crash_dir}/outward-tls/recovered/verdict.json" \
        --slurpfile reverted_verdict "${crash_dir}/outward-tls/reverted/verdict.json" \
        'def refused: [.nodes[] | select(.interconnect_probe.outcome == "refused") | .node];
         {target_node:$target,
          probe:{ports:{console:47421,interconnect:47395},version:"TLSv1.3",
                 cipher_suites:["TLS_CHACHA20_POLY1305_SHA256"],groups:["X25519"]},
          interconnect_refused:{before:($before_verdict[0] | refused),survivors:($survivors_verdict[0] | refused),
                                recovered:($recovered_verdict[0] | refused),reverted:($reverted_verdict[0] | refused)},
          before:{policy:"excludes",revision:$before,committed:$before_verdict[0].committed},
          commit:{attempts:$attempts,acknowledged_after_kill_ms:$acknowledged_ms,target_running:false,
                  transcripts:"crash/outward-tls/commit"},
          survivors:{policy:"permits",revision:$committed,committed:$survivors_verdict[0].committed,
                     nodes:[$survivors_verdict[0].nodes[].node]},
          recovered:{policy:"permits",revision:$committed,nodes:[$recovered_verdict[0].nodes[].node]},
          reverted:{policy:"excludes",revision:$reverted,committed:$reverted_verdict[0].committed},
          observations:"crash/outward-tls"}' \
        >"${artifact_dir}/results/outward-tls.json"
}
