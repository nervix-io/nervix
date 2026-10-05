#!/usr/bin/env bash
set -euo pipefail

# Decides the verdicts of the restart and recovery scenarios from the public evidence their runs
# record: consensus metrics and status, Kafka consumer-group members, node metrics and listener
# probes, node log lines, and Docker start events. Every mode writes its verdict to --output and
# exits 1 when the evidence contradicts the product contract it checks.

usage() {
    cat >&2 <<'EOF'
usage:
  verify-recovery-evidence.sh snapshot-catch-up --evidence FILE --output FILE
  verify-recovery-evidence.sh pre-admission --samples FILE --address ADDRESS --isolated-at-ms MS
      --min-window-ms N --max-gap-ms N --output FILE
  verify-recovery-evidence.sh admission --log FILE --not-before-ns NS --output FILE
  verify-recovery-evidence.sh ownership --before FILE --after FILE --samples FILE --starts FILE
      --grace-ms N --output FILE

snapshot-catch-up requires every survivor to have purged its log past the stopped follower's
last possible index before the follower restarted, the restarted follower to hold a snapshot
that covers that purged suffix, its applied index to reach the leader's restart boundary, and the
survivors to have answered snapshot transfer requests during the catch-up.

pre-admission requires every sample of a restarted node cut off from consensus to show all of its
public listeners answering, no Kafka consumer-group member at its address, no graph message on
its metrics, no applied entry past its first sample, and no runtime admission in its log. The
samples must run from --isolated-at-ms, when the isolation was verified, until at least
--min-window-ms later, with no gap longer than --max-gap-ms.

admission requires the node log to report that runtime execution waited for linearizable
consensus catch-up and was admitted no earlier than --not-before-ns.

ownership compares the owners before and after a whole-cluster restart with the voter observation
grace: when every node that led after the restart observed every voter live within the grace of
its own process start, each owner must keep its work; otherwise failover is permitted.
EOF
}

fail_usage() {
    printf '%s\n' "$*" >&2
    usage
    exit 2
}

require_file() {
    [[ -s "$1" ]] || fail_usage "required evidence is missing or empty: $1"
}

# Writes the verdict document and turns its verdict into the exit status.
finish_verdict() {
    local output="$1"
    local description="$2"
    if [[ "$(jq -r '.verdict' "${output}")" != pass ]]; then
        printf '%s failed: %s\n' "${description}" "$(jq -c '.failures' "${output}")" >&2
        exit 1
    fi
}

snapshot_catch_up() {
    local evidence="" output=""
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --evidence | --output)
                [[ "$#" -ge 2 ]] || fail_usage "$1 requires a value"
                case "$1" in
                    --evidence) evidence="$2" ;;
                    --output) output="$2" ;;
                esac
                shift 2
                ;;
            *) fail_usage "unknown snapshot-catch-up argument: $1" ;;
        esac
    done
    [[ -n "${output}" ]] || fail_usage '--output is required'
    require_file "${evidence}"
    jq -e '
        (.follower_log_bound | type == "number")
        and (.restart.leader_applied_index | type == "number")
        and (.restart.survivors | type == "array" and length > 0)
        and (.caught_up.survivors | type == "array" and length > 0)
        and (.caught_up.follower | type == "object")
    ' "${evidence}" >/dev/null 2>&1 \
        || fail_usage "snapshot catch-up evidence does not match its contract: ${evidence}"
    jq '
        .follower_log_bound as $bound
        | ([.restart.survivors[].purged_index] | min) as $purged
        | ([.restart.survivors[].snapshot_requests] | add) as $requests_before
        | ([.caught_up.survivors[].snapshot_requests] | add) as $requests_after
        | {
            log_suffix_purged: all(.restart.survivors[]; .purged_index > $bound),
            snapshot_installed: (.caught_up.follower.snapshot_index > $bound
                                 and .caught_up.follower.snapshot_index >= $purged),
            caught_up: (.caught_up.follower.last_applied >= .restart.leader_applied_index),
            transfer_observed: ($requests_after > $requests_before)
          } as $checks
        | {
            verdict: (if all($checks[]; .) then "pass" else "fail" end),
            follower: .follower,
            follower_log_bound: $bound,
            survivors_purged_at_restart: $purged,
            leader_applied_index_at_restart: .restart.leader_applied_index,
            follower_snapshot_index: .caught_up.follower.snapshot_index,
            follower_last_applied: .caught_up.follower.last_applied,
            snapshot_requests_answered: ($requests_after - $requests_before),
            checks: $checks,
            failures: [$checks | to_entries[] | select(.value | not) | .key]
          }
    ' "${evidence}" >"${output}"
    finish_verdict "${output}" 'snapshot catch-up'
}

pre_admission() {
    local samples="" address="" isolated_at_ms="" min_window_ms="" max_gap_ms="" output=""
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --samples | --address | --isolated-at-ms | --min-window-ms | --max-gap-ms | --output)
                [[ "$#" -ge 2 ]] || fail_usage "$1 requires a value"
                case "$1" in
                    --samples) samples="$2" ;;
                    --address) address="$2" ;;
                    --isolated-at-ms) isolated_at_ms="$2" ;;
                    --min-window-ms) min_window_ms="$2" ;;
                    --max-gap-ms) max_gap_ms="$2" ;;
                    --output) output="$2" ;;
                esac
                shift 2
                ;;
            *) fail_usage "unknown pre-admission argument: $1" ;;
        esac
    done
    [[ -n "${output}" ]] || fail_usage '--output is required'
    [[ "${address}" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail_usage "invalid node address: ${address}"
    [[ "${isolated_at_ms}" =~ ^[0-9]+$ ]] || fail_usage '--isolated-at-ms must be a millisecond Unix time'
    [[ "${min_window_ms}" =~ ^[0-9]+$ ]] || fail_usage '--min-window-ms must be a millisecond count'
    [[ "${max_gap_ms}" =~ ^[0-9]+$ ]] || fail_usage '--max-gap-ms must be a millisecond count'
    require_file "${samples}"
    jq -s -e '
        length > 0
        and all(.[]; (.at_ms | type == "number")
            and (.listeners | type == "object")
            and (.members | type == "array")
            and (.graph_messages | type == "number")
            and (.admitted_logged | type == "boolean")
            and (.last_applied | type == "number"))
    ' "${samples}" >/dev/null 2>&1 \
        || fail_usage "pre-admission samples do not match their contract: ${samples}"
    jq -s \
        --arg address "${address}" \
        --argjson isolated_at_ms "${isolated_at_ms}" \
        --argjson min_window_ms "${min_window_ms}" \
        --argjson max_gap_ms "${max_gap_ms}" '
        sort_by(.at_ms) as $samples
        | ($samples[-1].at_ms - $isolated_at_ms) as $window_ms
        | ([$isolated_at_ms] + [$samples[].at_ms]) as $times
        | ([range(1; $times | length) | $times[.] - $times[. - 1]] | max) as $longest_gap_ms
        | ($samples[0].last_applied) as $applied_boundary
        | {
            window_covered: ($samples[0].at_ms >= $isolated_at_ms and $window_ms >= $min_window_ms),
            observation_continuous: ($longest_gap_ms <= $max_gap_ms),
            listeners_answered: all($samples[]; .listeners | all(.[]; . == true)),
            no_consumer_membership: all($samples[]; all(.members[]; .address != $address)),
            no_graph_output: all($samples[]; .graph_messages == 0),
            applied_frozen: all($samples[]; .last_applied <= $applied_boundary),
            not_admitted: all($samples[]; .admitted_logged | not)
          } as $checks
        | {
            verdict: (if all($checks[]; .) then "pass" else "fail" end),
            address: $address,
            samples: ($samples | length),
            isolated_at_ms: $isolated_at_ms,
            window_ms: $window_ms,
            minimum_window_ms: $min_window_ms,
            longest_gap_ms: $longest_gap_ms,
            maximum_gap_ms: $max_gap_ms,
            applied_boundary: $applied_boundary,
            readyz: ([$samples[].readyz] | unique),
            checks: $checks,
            failures: [$checks | to_entries[] | select(.value | not) | .key]
          }
    ' "${samples}" >"${output}"
    finish_verdict "${output}" 'pre-admission observation'
}

# Prints the Unix nanosecond time of the first log line containing TEXT, or nothing.
first_log_ns() {
    local log="$1"
    local text="$2"
    local line
    line="$(grep -F -m 1 -- "${text}" "${log}" || true)"
    [[ -n "${line}" ]] || return 0
    date -d "${line%% *}" +%s%N
}

admission() {
    local log="" not_before_ns="" output=""
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --log | --not-before-ns | --output)
                [[ "$#" -ge 2 ]] || fail_usage "$1 requires a value"
                case "$1" in
                    --log) log="$2" ;;
                    --not-before-ns) not_before_ns="$2" ;;
                    --output) output="$2" ;;
                esac
                shift 2
                ;;
            *) fail_usage "unknown admission argument: $1" ;;
        esac
    done
    [[ -n "${output}" ]] || fail_usage '--output is required'
    [[ "${not_before_ns}" =~ ^[0-9]+$ ]] || fail_usage '--not-before-ns must be a nanosecond Unix time'
    [[ -f "${log}" ]] || fail_usage "node log is missing: ${log}"
    local waiting_ns admitted_ns committed_index
    waiting_ns="$(first_log_ns "${log}" 'runtime execution is waiting for linearizable consensus catch-up')"
    admitted_ns="$(first_log_ns "${log}" 'runtime execution admitted after linearizable consensus catch-up')"
    committed_index="$(grep -F -m 1 'runtime execution admitted after linearizable consensus catch-up' "${log}" \
        | sed -n 's/.*committed_log_index=\([0-9][0-9]*\).*/\1/p' || true)"
    jq -n \
        --argjson not_before "${not_before_ns}" \
        --argjson waiting "${waiting_ns:-null}" \
        --argjson admitted "${admitted_ns:-null}" \
        --argjson committed "${committed_index:-null}" '
        {
          waited_for_catch_up: ($waiting != null and ($admitted == null or $waiting <= $admitted)),
          admitted: ($admitted != null),
          admitted_after_boundary: ($admitted != null and $admitted >= $not_before)
        } as $checks
        | {
            verdict: (if all($checks[]; .) then "pass" else "fail" end),
            not_before_ns: $not_before,
            waiting_logged_ns: $waiting,
            admitted_logged_ns: $admitted,
            admitted_after_boundary_ms: (if $admitted == null then null
                                         else (($admitted - $not_before) / 1000000 | floor) end),
            committed_log_index: $committed,
            checks: $checks,
            failures: [$checks | to_entries[] | select(.value | not) | .key]
          }
    ' >"${output}"
    finish_verdict "${output}" 'startup admission'
}

ownership() {
    local before="" after="" samples="" starts="" grace_ms="" output=""
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --before | --after | --samples | --starts | --grace-ms | --output)
                [[ "$#" -ge 2 ]] || fail_usage "$1 requires a value"
                case "$1" in
                    --before) before="$2" ;;
                    --after) after="$2" ;;
                    --samples) samples="$2" ;;
                    --starts) starts="$2" ;;
                    --grace-ms) grace_ms="$2" ;;
                    --output) output="$2" ;;
                esac
                shift 2
                ;;
            *) fail_usage "unknown ownership argument: $1" ;;
        esac
    done
    [[ -n "${output}" ]] || fail_usage '--output is required'
    [[ "${grace_ms}" =~ ^[0-9]+$ ]] || fail_usage '--grace-ms must be a millisecond count'
    require_file "${before}"
    require_file "${after}"
    require_file "${starts}"
    [[ -f "${samples}" ]] || fail_usage "status samples are missing: ${samples}"
    jq -e '(.owners | type == "object" and length > 0) and (.voters | type == "array" and length > 0)' \
        "${before}" >/dev/null 2>&1 || fail_usage "owners before the restart do not match their contract: ${before}"
    jq -e '(.owners | type == "object" and length > 0) and (.live_voters | type == "array")' \
        "${after}" >/dev/null 2>&1 || fail_usage "owners after the restart do not match their contract: ${after}"
    jq -e 'type == "object" and length > 0 and all(.[]; type == "number")' "${starts}" >/dev/null 2>&1 \
        || fail_usage "node start times do not match their contract: ${starts}"
    jq -n \
        --slurpfile before "${before}" \
        --slurpfile after "${after}" \
        --slurpfile starts "${starts}" \
        --slurpfile samples "${samples}" \
        --argjson grace_ms "${grace_ms}" '
        $before[0] as $before
        | $after[0] as $after
        | $starts[0] as $starts
        | ($grace_ms * 1000000) as $grace_ns
        | [$samples[] | select(.answered)] as $answered
        | ([$answered[] | select(.state == "Leader") | .node] | unique) as $leaders
        | [$leaders[] as $leader
            | ($answered | map(select(.node == $leader))) as $views
            | $starts[$leader] as $started
            | (if $started == null then null else $started + $grace_ns end) as $grace_end
            | ($before.voters
               | map(. as $voter
                     | {key: $voter,
                        value: ([$views[] | select(.live | index($voter) != null) | .at_ns] | min)})
               | from_entries) as $observed
            | {node: $leader,
               process_started_ns: $started,
               grace_ends_ns: $grace_end,
               voters_observed_ns: $observed,
               all_observed_within_grace: ($grace_end != null
                   and all($observed[]; . != null and . <= $grace_end))}] as $leader_views
        | (($leader_views | length) > 0 and all($leader_views[]; .all_observed_within_grace)) as $within_grace
        | ([$before.owners | to_entries[]
            | select($after.owners[.key] != .value)
            | {entity: .key, from: .value, to: $after.owners[.key]}]) as $moved
        | ([$after.owners | to_entries[] | .value as $owner
            | select(($after.live_voters | index($owner)) == null)
            | {entity: .key, owner: $owner}]) as $unavailable_owners
        | {
            owners_on_live_voters: (($unavailable_owners | length) == 0),
            owners_kept_within_grace: ($within_grace | not) or (($moved | length) == 0)
          } as $checks
        | {
            verdict: (if all($checks[]; .) then "pass" else "fail" end),
            grace_ms: $grace_ms,
            contract: (if $within_grace then "owners keep their work" else "failover permitted" end),
            leaders_observed: $leader_views,
            owners_before: $before.owners,
            owners_after: $after.owners,
            moved: $moved,
            owners_on_unavailable_nodes: $unavailable_owners,
            checks: $checks,
            failures: [$checks | to_entries[] | select(.value | not) | .key]
          }
    ' >"${output}"
    finish_verdict "${output}" 'whole-cluster restart ownership'
}

mode="${1:-}"
[[ -n "${mode}" ]] || { usage; exit 2; }
shift
case "${mode}" in
    snapshot-catch-up) snapshot_catch_up "$@" ;;
    pre-admission) pre_admission "$@" ;;
    admission) admission "$@" ;;
    ownership) ownership "$@" ;;
    -h | --help) usage ;;
    *) fail_usage "unknown verify-recovery-evidence mode: ${mode}" ;;
esac
