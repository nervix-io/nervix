#!/usr/bin/env bash
# Self-test of the mixed-instability hold check for a degraded link that leaves every voter in a
# quorum. The public statuses below are those mixed-rotating-temporary retained at seed 993081495,
# step 15: burst loss from leader node-3 to node-1 moved leadership to node-1 and left node-3, the
# other end of the lossy link, behind the quorum of node-1 and node-2.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT
artifact_dir="${tmp_dir}/artifacts"
mkdir -p "${artifact_dir}/results" "${artifact_dir}/mixed"
scenario=mixed-instability
current_phase='hold self-test'
# shellcheck source=../rolling-restart-scenario.sh
source "${script_dir}/rolling-restart-scenario.sh"
# shellcheck source=../mixed-scenario.sh
source "${script_dir}/mixed-scenario.sh"

fail() {
    printf 'hold quorum self-test failed: %s\n' "$*" >&2
    exit 1
}

node_hosts=(nervix-1 nervix-2 nervix-3)
node_names=(node-1 node-2 node-3)

# Each host's public status: its leader and its applied log index.
declare -A status_leader_of=()
declare -A status_log_of=()

cli_command() {
    local self="node-${cli_host##*-}"
    printf '[chitchat]\nself:\n- node_id: %s\nlive_nodes:\n' "${self}"
    local node
    for node in "${node_names[@]}"; do
        [[ "${node}" == "${self}" ]] || printf -- '- node_id: %s\n' "${node}"
    done
    printf '\n[raft]\nraft.id: %s\nraft.current_leader: %s\nraft.current_term: 7\n' \
        "${self}" "${status_leader_of[${cli_host}]}"
    printf 'raft.last_log_index: %s\nraft.last_applied: %s\nraft.membership:\n' \
        "${status_log_of[${cli_host}]}" "${status_log_of[${cli_host}]}"
    for node in "${node_names[@]}"; do
        printf -- '- %s [voter] nervix-%s:47395\n' "${node}" "${node##*-}"
    done
    printf '\n[domains]\n- chaos_baseline status=Running pace=UNPACED\n'
}

# One attempt stands for the whole bound: the statuses do not change while the check waits.
wait_for() {
    shift 2
    "$@"
}

canaries_sent=()
partition_canary() {
    local name="$1"
    local host="$2"
    local output_dir="$3"
    canaries_sent+=("${name}@${host}")
    jq -n --arg name "${name}" --arg host "${host}" '{name: $name, entry_host: $host, outcome: "acknowledged"}' \
        >"${output_dir}/control-${name}.json"
}

# Holds burst loss from nervix-3 to nervix-1 with the given statuses and judges the hold.
judge_burst_loss() {
    local label="$1"
    mixed_step_dir="${tmp_dir}/${label}"
    mkdir -p "${mixed_step_dir}"
    mixed_active=("${label}")
    mixed_action_dir=(["${label}"]="${tmp_dir}/${label}/${label}-degrade")
    mixed_action_impaired=(["${label}"]="")
    mixed_action_lossy_link=(["${label}"]="nervix-3 nervix-1")
    canaries_sent=()
    mixed_hold_checks 15 "${label}"
}

# The quorum of node-1 and node-2 agrees on caught-up leader node-1 while node-3 lags behind it.
status_leader_of=([nervix-1]=node-1 [nervix-2]=node-1 [nervix-3]=node-1)
status_log_of=([nervix-1]=380 [nervix-2]=380 [nervix-3]=366)
judge_burst_loss 15a
hold="${tmp_dir}/15a/hold-15a.json"
jq -e '.verdict == "observed" and .availability_required and .settled_without == "nervix-3"
       and .canaries == ["chaos_mixed_15_15a_majority"]' "${hold}" >/dev/null \
    || fail "a quorum outside the lossy link's lagging end was judged $(jq -c . "${hold}")"
[[ "${canaries_sent[*]}" == 'chaos_mixed_15_15a_majority@nervix-1' ]] \
    || fail "the canary went to ${canaries_sent[*]}, not through the quorum's leader"

# With the lossy link's ends in agreement, every voter counts and none is left out.
status_log_of=([nervix-1]=380 [nervix-2]=380 [nervix-3]=380)
judge_burst_loss 15b
jq -e '.verdict == "observed" and .settled_without == null' "${tmp_dir}/15b/hold-15b.json" >/dev/null \
    || fail "a quorum of every voter was judged $(jq -c . "${tmp_dir}/15b/hold-15b.json")"

# A lagging voter outside the lossy link has no such excuse: the quorum did not settle.
status_log_of=([nervix-1]=380 [nervix-2]=366 [nervix-3]=380)
judge_burst_loss 15c
jq -e '.verdict == "no-majority-leader" and .availability_required' "${tmp_dir}/15c/hold-15c.json" >/dev/null \
    || fail "a voter off the lossy link lagging was judged $(jq -c . "${tmp_dir}/15c/hold-15c.json")"
((${#canaries_sent[@]} == 0)) || fail 'an unsettled quorum was sent a canary'

# Leaving one end of the lossy link out still requires the other two voters to agree on the leader.
status_leader_of=([nervix-1]=node-1 [nervix-2]=node-2 [nervix-3]=node-1)
status_log_of=([nervix-1]=380 [nervix-2]=380 [nervix-3]=366)
judge_burst_loss 15d
jq -e '.verdict == "no-majority-leader"' "${tmp_dir}/15d/hold-15d.json" >/dev/null \
    || fail "a quorum split on its leader was judged $(jq -c . "${tmp_dir}/15d/hold-15d.json")"

printf 'hold quorum self-test passed\n'
