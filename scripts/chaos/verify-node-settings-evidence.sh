#!/usr/bin/env bash
set -euo pipefail

# Judges the public evidence of the node-settings scenarios (node-settings-scenario.sh): what a
# node's own DESCRIBE CLUSTER, SHOW CLUSTER STATUS, server notices and log show about a node
# capability rolled across the cluster, a committed cluster parameter change and a node refused over
# a prerequisite. Every command reads files the scenario retained and writes its verdict as JSON. It
# exits 0 when the evidence holds, 1 when it contradicts the expectation, and 2 when it cannot be
# read or the command is malformed.
#
# The lines this verifier reads are the ones the packaged server renders: a node's block in
# DESCRIBE CLUSTER, with its incarnation, the capability claim committed for it, its interconnect
# liveness and its prerequisites; the cluster-effective and adopted peer receive bounds; and the
# committed and running cluster parameter lines.

usage() {
    cat >&2 <<'EOF'
usage:
  verify-node-settings-evidence.sh describe --transcript FILE --node NODE --output FILE
  verify-node-settings-evidence.sh claims --expect NODE=BYTES... [--restarted NODE=INCARNATION]...
      --output FILE DESCRIPTION...
  verify-node-settings-evidence.sh installed --before DESCRIPTION [--expect PARAMETER=VALUE]...
      [--liveness TEXT] --output FILE DESCRIPTION...
  verify-node-settings-evidence.sh samples --transcript FILE --node NODE --revision N
      --expect PARAMETER=VALUE... --output FILE
  verify-node-settings-evidence.sh installations --log FILE --revision N --output FILE
  verify-node-settings-evidence.sh join-refusal --log FILE --survivor NODE... --mismatch TEXT
      --output FILE
  verify-node-settings-evidence.sh seed-refusal --log FILE --parameter NAME --output FILE
  verify-node-settings-evidence.sh survivor-refusals --node NODE --prerequisite NAME --output FILE LOG...
  verify-node-settings-evidence.sh notices --window FILE --revision N... --output FILE TRANSCRIPT...

describe projects the DESCRIBE CLUSTER answer of NODE into JSON and exits 1 unless it describes a
leader and at least one node.

claims requires every description, one per answering node, to name each expected node live with a
committed claim of its live incarnation whose relay-batch-bytes is BYTES; every answering node to
have adopted each peer's live claim, incarnation and all four receive bounds; the cluster-effective
receive bounds to be their componentwise minimum; every description to agree on each node's live
incarnation; and a --restarted node to run another incarnation than the one named.

installed requires every description to name one committed parameter revision after the one in
--before, holding the --before values with each --expect assignment applied and nothing else changed;
every live node to run exactly that revision and those values; and, with --liveness, every live node's
interconnect liveness line to read TEXT.

samples judges a returning node's own SHOW CLUSTER STATUS answers, one per "=== status HOST NS"
header: no answer of NODE may report it running a revision other than N or a value other than the
expected ones, and at least one must report it running N with them.

installations requires the first parameter revision the log records installing to be N, and no
later one to be lower.

join-refusal requires the log of a run refused at join to report its refusal, every refusal to name
a survivor and exactly the mismatch TEXT, its exit report to name the refusal, and the run never to
have admitted runtime execution.

seed-refusal requires the log of a run started with a cluster parameter as its own setting to refuse
it as a first-bootstrap seed and never to have admitted runtime execution.

survivor-refusals requires at least one survivor's log to report refusing NODE over the
prerequisite NAME, at its interconnect handshake or as the leader's verdict on its run.

notices requires every observer transcript (events-observer.sh) to have followed the whole window
and to hold, inside it, exactly one cluster-parameter notice for each revision N, in order, and no
membership or leadership notice.
EOF
    exit 2
}

usage_error() {
    printf '%s\n' "$*" >&2
    usage
}

# The packaged server's log, with any terminal emphasis its exit report carries removed.
plain_log() {
    sed 's/\x1b\[[0-9;]*m//g' "$1"
}

# A JSON object of NAME=VALUE pairs.
assignments_json() {
    local pair
    for pair in "$@"; do
        [[ "${pair}" == *=* && -n "${pair%%=*}" ]] || usage_error "not a NAME=VALUE pair: ${pair}"
        jq -nc --arg key "${pair%%=*}" --arg value "${pair#*=}" '{($key): $value}'
    done | jq -sc 'add // {}'
}

write_verdict() {
    local output="$1"
    local verdict_json="$2"
    local passing="$3"
    jq . <<<"${verdict_json}" >"${output}"
    jq -r --arg passing "${passing}" '"verdict: \(.verdict)", (.problems // [] | .[] | "problem: \(.)")' \
        "${output}"
    [[ "$(jq -r '.verdict' "${output}")" == "${passing}" ]] || exit 1
}

describe() {
    local transcript="" node="" output=""
    while [[ "$#" -gt 0 ]]; do
        [[ "$#" -ge 2 ]] || usage
        case "$1" in
            --transcript) transcript="$2" ;;
            --node) node="$2" ;;
            --output) output="$2" ;;
            *) usage ;;
        esac
        shift 2
    done
    [[ -n "${transcript}" && -n "${output}" && "${node}" =~ ^node-[0-9]+$ ]] || usage
    [[ -f "${transcript}" ]] || { printf 'the description %s does not exist\n' "${transcript}" >&2; exit 2; }
    jq -Rs --arg node "${node}" '
        def bounds:
            (capture("relay-batch-bytes=(?<relay_batch_bytes>[0-9]+) relay-decoded-bytes=(?<relay_decoded_bytes>[0-9]+) bulk-chunk-bytes=(?<bulk_chunk_bytes>[0-9]+) bulk-window-bytes=(?<bulk_window_bytes>[0-9]+)")
             | map_values(tonumber)) // null;
        def node_block($leader):
            {leader: $leader, incarnation: null, claim_state: "missing", claim: null, liveness: null,
             prerequisites: {}, drift: {}, record: null};
        split("\n") | map(sub("\r$"; ""))
        | reduce .[] as $line (
            {node: $node, leader: null, timing_drift: null, nodes: {}, departed: {},
             effective_state: "missing", effective: null, adopted: {}, committed: {},
             committed_revisions: [], running: {}, block: null, block_set: null};
            if ($line | test("^node: [^ ]+( \\((leader|departed)\\))?$")) then
                ($line | capture("^node: (?<name>[^ ]+)(?: \\((?<mark>leader|departed)\\))?$")) as $header
                | (if $header.mark == "departed" then "departed" else "nodes" end) as $set
                | .block = $header.name
                | .block_set = $set
                | .[$set][$header.name] = node_block($header.mark == "leader")
            elif .block != null and ($line | startswith("  ")) then
                .block as $name
                | .block_set as $set
                | if ($line | test("^  incarnation: [0-9]+$")) then
                      .[$set][$name].incarnation = ($line | ltrimstr("  incarnation: "))
                  elif ($line | test("^  capabilities: committed [^ #]+#[0-9]+ ")) then
                      .[$set][$name].claim_state = "committed"
                      | .[$set][$name].claim =
                          (($line | capture("^  capabilities: committed (?<claimed>[^ #]+)#(?<incarnation>[0-9]+) "))
                           + (($line | bounds) // {}))
                  elif ($line | startswith("  capabilities: awaiting this incarnation")) then
                      .[$set][$name].claim_state = "awaiting this incarnation"
                  elif $line == "  capabilities: awaiting committed claim" then
                      .[$set][$name].claim_state = "awaiting committed claim"
                  elif ($line | startswith("  interconnect liveness: ")) then
                      .[$set][$name].liveness = ($line | ltrimstr("  interconnect liveness: "))
                  elif ($line | test("^  (raft-heartbeat-interval|gossip-interval|clock-skew-tolerance|cluster identity|durability promise): ")) then
                      ($line | capture("^  (?<prerequisite>[^:]+): (?<rest>.*)$")) as $described
                      | (($described.rest | capture("^(?<value>.*), the leader.s (?<leader>.*)$"))
                         // {value: $described.rest, leader: null}) as $value
                      | .[$set][$name].prerequisites[$described.prerequisite] = $value.value
                      | if $value.leader == null then .
                        else .[$set][$name].drift[$described.prerequisite] = $value.leader end
                  elif $line == "  prerequisites: not published" then
                      .[$set][$name].prerequisites = null
                  elif ($line | test("^  record: revision [0-9]+ \\(incarnation [0-9]+\\)$")) then
                      .[$set][$name].record =
                          ($line | capture("^  record: revision (?<revision>[0-9]+) \\(incarnation (?<incarnation>[0-9]+)\\)$"))
                  else . end
            else
                .block = null
                | .block_set = null
                | if ($line | startswith("leader: ")) then
                      .leader = ($line | ltrimstr("leader: "))
                  elif ($line | startswith("timing drift: ")) then
                      .timing_drift = ($line | ltrimstr("timing drift: "))
                  elif $line == "cluster-effective receive bounds: awaiting committed live claims" then
                      .effective_state = "awaiting committed live claims"
                  elif ($line | startswith("cluster-effective receive bounds: ")) then
                      .effective_state = "committed"
                      | .effective = ($line | bounds)
                  elif ($line | test("^adopted peer receive bounds: node=[^ ]+ incarnation=[0-9]+ ")) then
                      ($line | capture("^adopted peer receive bounds: node=(?<peer>[^ ]+) incarnation=(?<incarnation>[0-9]+) ")) as $adopted
                      | .adopted[$adopted.peer] = ({incarnation: $adopted.incarnation} + (($line | bounds) // {}))
                  elif ($line | test("^[a-z][a-z0-9-]* committed=.+ revision=[1-9][0-9]*$")) then
                      ($line | capture("^(?<parameter>[a-z][a-z0-9-]*) committed=(?<value>.+) revision=(?<revision>[1-9][0-9]*)$")) as $committed
                      | .committed[$committed.parameter] = $committed.value
                      | .committed_revisions += [$committed.revision]
                  elif ($line | test("^node=[^ ]+ cluster parameters running=not installed$")) then
                      .running[($line | capture("^node=(?<peer>[^ ]+) ").peer)] = {state: "not installed"}
                  elif ($line | test("^node=[^ ]+ [a-z][a-z0-9-]* running=.+ revision=[1-9][0-9]*$")) then
                      ($line | capture("^node=(?<peer>[^ ]+) (?<parameter>[a-z][a-z0-9-]*) running=(?<value>.+) revision=(?<revision>[1-9][0-9]*)$")) as $running
                      | .running[$running.peer] |=
                          ((. // {state: "installed", revisions: [], values: {}})
                           | .revisions += [$running.revision]
                           | .values[$running.parameter] = $running.value)
                  else . end
            end)
        | (.committed_revisions | unique) as $revisions
        | .committed_revision = (if ($revisions | length) == 1 then $revisions[0] else null end)
        | .committed_consistent = (($revisions | length) <= 1)
        | .running |= map_values(
            if .state == "installed"
            then (.revisions | unique) as $node_revisions
                 | {state, values,
                    revision: (if ($node_revisions | length) == 1 then $node_revisions[0] else null end),
                    consistent: (($node_revisions | length) == 1)}
            else . end)
        | .answered = (.leader != null and (.nodes | length) > 0)
        | del(.block, .block_set, .committed_revisions)
    ' "${transcript}" >"${output}.tmp"
    mv "${output}.tmp" "${output}"
    jq -e '.answered' "${output}" >/dev/null
}

claims() {
    local output=""
    local expectations=()
    local restarts=()
    local descriptions=()
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --expect) [[ "$#" -ge 2 ]] || usage; expectations+=("$2"); shift 2 ;;
            --restarted) [[ "$#" -ge 2 ]] || usage; restarts+=("$2"); shift 2 ;;
            --output) [[ "$#" -ge 2 ]] || usage; output="$2"; shift 2 ;;
            -*) usage ;;
            *) descriptions+=("$1"); shift ;;
        esac
    done
    [[ -n "${output}" && "${#expectations[@]}" -gt 0 && "${#descriptions[@]}" -gt 0 ]] || usage
    local expect restarted
    expect="$(assignments_json "${expectations[@]}")"
    jq -e 'all(.[]; test("^[1-9][0-9]*$"))' <<<"${expect}" >/dev/null \
        || usage_error 'an expected relay-batch-bytes must be a positive byte count'
    restarted="$(assignments_json ${restarts[@]+"${restarts[@]}"})"
    local verdict_json
    verdict_json="$(jq -n --argjson expect "${expect}" --argjson restarted "${restarted}" \
        --slurpfile descriptions <(cat "${descriptions[@]}") '
        def fields: ["relay_batch_bytes", "relay_decoded_bytes", "bulk_chunk_bytes", "bulk_window_bytes"];
        ($expect | map_values(tonumber)) as $bytes
        | ($bytes | keys) as $nodes
        | [$descriptions[] as $d
           | ($nodes[] as $n
              | $d.nodes[$n] as $live
              | if $live == null then "\($d.node) does not describe \($n) as a live node"
                elif $live.claim_state != "committed"
                then "\($d.node) describes no committed claim of the live incarnation \($live.incarnation) of \($n): \($live.claim_state)"
                elif $live.claim.incarnation != $live.incarnation
                then "\($d.node) describes the claim of incarnation \($live.claim.incarnation) of \($n), not of its live incarnation \($live.incarnation)"
                elif $live.claim.relay_batch_bytes != $bytes[$n]
                then "\($d.node) describes \($n) claiming relay-batch-bytes=\($live.claim.relay_batch_bytes), not \($bytes[$n])"
                else empty end),
             ($nodes[] | select(. != $d.node) as $peer
              | $d.nodes[$peer] as $live
              | $d.adopted[$peer] as $adopted
              | if $live == null or $live.claim == null then empty
                elif $adopted == null then "\($d.node) has adopted no receive bounds of \($peer)"
                elif $adopted.incarnation != $live.incarnation
                then "\($d.node) has adopted the receive bounds of incarnation \($adopted.incarnation) of \($peer), not of its live incarnation \($live.incarnation)"
                elif any(fields[]; $adopted[.] != $live.claim[.])
                then "\($d.node) has adopted receive bounds of \($peer) other than its committed claim"
                else empty end),
             ([$nodes[] | $d.nodes[.].claim | select(. != null)] as $claims
              | if ($claims | length) != ($nodes | length) then empty
                elif $d.effective == null
                then "\($d.node) describes no cluster-effective receive bounds: \($d.effective_state)"
                elif any(fields[]; . as $f | $d.effective[$f] != ([$claims[] | .[$f]] | min))
                then "\($d.node) describes cluster-effective receive bounds other than the componentwise minimum of the live claims"
                else empty end),
             ($restarted | to_entries[] | select($d.nodes[.key].incarnation == .value)
              | "\($d.node) still describes \(.key) at incarnation \(.value), the one it ran before its restart")
          ] as $problems
        | ($nodes | map(. as $n | {key: $n, value: ([$descriptions[].nodes[$n].incarnation] | unique)})
           | from_entries) as $incarnations
        | [$incarnations | to_entries[] | select((.value | length) > 1)
           | "the answering nodes disagree on the live incarnation of \(.key): \(.value | join(", "))"] as $disagreements
        | ($problems + $disagreements) as $all
        | {verdict: (if ($all | length) == 0 then "converged" else "diverged" end),
           problems: $all,
           expected: $bytes,
           incarnations: ($incarnations | map_values(if length == 1 then .[0] else . end)),
           claims: ($nodes | map(. as $n | {key: $n, value: $descriptions[0].nodes[$n].claim}) | from_entries),
           effective: ([$descriptions[] | {key: .node, value: .effective}] | from_entries),
           adopted: ([$descriptions[] | {key: .node, value: .adopted}] | from_entries)}
    ')"
    write_verdict "${output}" "${verdict_json}" converged
}

installed() {
    local output="" before="" liveness=""
    local expectations=()
    local descriptions=()
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --before) [[ "$#" -ge 2 ]] || usage; before="$2"; shift 2 ;;
            --expect) [[ "$#" -ge 2 ]] || usage; expectations+=("$2"); shift 2 ;;
            --liveness) [[ "$#" -ge 2 ]] || usage; liveness="$2"; shift 2 ;;
            --output) [[ "$#" -ge 2 ]] || usage; output="$2"; shift 2 ;;
            -*) usage ;;
            *) descriptions+=("$1"); shift ;;
        esac
    done
    [[ -n "${output}" && -n "${before}" && "${#descriptions[@]}" -gt 0 ]] || usage
    jq -e '.committed_consistent and (.committed_revision | type == "string")' "${before}" >/dev/null 2>&1 \
        || { printf 'the description %s holds no committed parameter revision\n' "${before}" >&2; exit 2; }
    local expect
    expect="$(assignments_json ${expectations[@]+"${expectations[@]}"})"
    local verdict_json
    verdict_json="$(jq -n --argjson expect "${expect}" --arg liveness "${liveness}" \
        --slurpfile before "${before}" --slurpfile descriptions <(cat "${descriptions[@]}") '
        $before[0] as $b
        | ($b.committed_revision | tonumber) as $previous
        | ($b.committed + $expect) as $values
        | [$descriptions[] as $d
           | (if ($d.committed_consistent | not)
              then "\($d.node) describes more than one committed parameter revision"
              elif $d.committed_revision == null then "\($d.node) describes no committed parameter revision"
              elif ($d.committed_revision | tonumber) <= $previous
              then "\($d.node) describes committed revision \($d.committed_revision), not one after \($previous)"
              else empty end),
             ([($values + $d.committed) | keys[] | select($values[.] != $d.committed[.])] as $changed
              | if ($changed | length) == 0 then empty
                else "\($d.node) describes committed values that differ from the change in \($changed | join(", "))" end),
             ($d.nodes | keys[] as $n
              | $d.running[$n] as $running
              | if $running == null then "\($d.node) describes no running parameters of \($n)"
                elif $running.state != "installed" then "\($d.node) describes \($n) running \($running.state)"
                elif ($running.consistent | not) then "\($d.node) describes \($n) running more than one revision"
                elif $running.revision != $d.committed_revision
                then "\($d.node) describes \($n) running revision \($running.revision), not the committed \($d.committed_revision)"
                elif $running.values != $d.committed
                then "\($d.node) describes \($n) running values other than the committed ones"
                else empty end),
             (if $liveness == "" then empty
              else $d.nodes | to_entries[] | select(.value.liveness != $liveness)
                   | "\($d.node) describes the interconnect liveness of \(.key) as \(.value.liveness // "absent"), not \($liveness)" end)
          ] as $problems
        | ([$descriptions[].committed_revision] | unique) as $revisions
        | ($problems + (if ($revisions | length) > 1
                        then ["the answering nodes describe different committed revisions: \($revisions | map(tostring) | join(", "))"]
                        else [] end)) as $all
        | {verdict: (if ($all | length) == 0 then "installed" else "pending" end),
           problems: $all,
           previous_revision: $b.committed_revision,
           revision: (if ($revisions | length) == 1 then $revisions[0] else null end),
           expected: $expect,
           liveness: (if $liveness == "" then null else $liveness end),
           committed: $descriptions[0].committed}
    ')"
    write_verdict "${output}" "${verdict_json}" installed
}

samples() {
    local transcript="" node="" revision="" output=""
    local expectations=()
    while [[ "$#" -gt 0 ]]; do
        [[ "$#" -ge 2 ]] || usage
        case "$1" in
            --transcript) transcript="$2" ;;
            --node) node="$2" ;;
            --revision) revision="$2" ;;
            --expect) expectations+=("$2") ;;
            --output) output="$2" ;;
            *) usage ;;
        esac
        shift 2
    done
    [[ -n "${transcript}" && -n "${output}" && "${node}" =~ ^node-[0-9]+$ && "${revision}" =~ ^[1-9][0-9]*$ ]] \
        || usage
    [[ "${#expectations[@]}" -gt 0 ]] || usage
    [[ -f "${transcript}" ]] || { printf 'the sample transcript %s does not exist\n' "${transcript}" >&2; exit 2; }
    local expect
    expect="$(assignments_json "${expectations[@]}")"
    local verdict_json
    verdict_json="$(jq -Rs --arg node "${node}" --arg revision "${revision}" --argjson expect "${expect}" '
        split("\n") | map(sub("\r$"; ""))
        | reduce .[] as $line ({samples: [], current: null};
            if ($line | test("^=== status [^ ]+ [0-9]+$")) then
                (if .current == null then . else .samples += [.current] end)
                | ($line | capture("^=== status (?<host>[^ ]+) (?<ns>[0-9]+)$")) as $header
                | .current = {host: $header.host, at_ns: ($header.ns | tonumber), node: null,
                              initialization: null, not_installed: false, running: []}
            elif .current == null then .
            elif ($line | startswith("raft.id: ")) then .current.node = ($line | ltrimstr("raft.id: "))
            elif ($line | startswith("node.initialization=")) then
                .current.initialization = ($line | ltrimstr("node.initialization="))
            elif $line == "node=\($node) cluster parameters running=not installed" then
                .current.not_installed = true
            elif ($line | startswith("node=\($node) ")) and ($line | test(" running=.+ revision=[1-9][0-9]*$")) then
                .current.running += [$line | capture("^node=[^ ]+ (?<parameter>[a-z][a-z0-9-]*) running=(?<value>.+) revision=(?<revision>[1-9][0-9]*)$")]
            else . end)
        | (if .current == null then .samples else .samples + [.current] end)
        | map(select(.node == $node)) as $answers
        | [$answers[] | select((.running | length) > 0)
           | ([.running[] | {key: .parameter, value: .value}] | from_entries) as $values
           | {at_ns, initialization, values: $values,
              revisions: ([.running[].revision] | unique),
              stale: (any(.running[]; .revision != $revision)
                      or any($expect | to_entries[]; $values[.key] != .value))}] as $installed
        | [$installed[] | select(.stale) | del(.stale)] as $stale
        | [$installed[] | select(.stale | not)] as $current
        | [(if ($answers | length) == 0 then "\($node) answered no sample through its own route" else empty end),
           ($stale[] | "\($node) reported running revisions \(.revisions | join(", ")) with \(.values | tojson), not revision \($revision) with \($expect | tojson)"),
           (if ($answers | length) > 0 and ($current | length) == 0
            then "\($node) never reported running revision \($revision) with \($expect | tojson)" else empty end)] as $problems
        | {verdict: (if ($problems | length) == 0 then "adopted" else "stale" end),
           problems: $problems,
           node: $node, revision: $revision, expected: $expect,
           samples: ($answers | length),
           not_installed_samples: ([$answers[] | select(.not_installed)] | length),
           running_samples: ($installed | length),
           initialization: ([$answers[].initialization | select(. != null)] | unique),
           first_running_ns: (if ($current | length) > 0 then $current[0].at_ns else null end),
           stale: $stale}
    ' "${transcript}")"
    write_verdict "${output}" "${verdict_json}" adopted
}

installations() {
    local log="" revision="" output=""
    while [[ "$#" -gt 0 ]]; do
        [[ "$#" -ge 2 ]] || usage
        case "$1" in
            --log) log="$2" ;;
            --revision) revision="$2" ;;
            --output) output="$2" ;;
            *) usage ;;
        esac
        shift 2
    done
    [[ -n "${log}" && -n "${output}" && "${revision}" =~ ^[1-9][0-9]*$ ]] || usage
    [[ -f "${log}" ]] || { printf 'the log %s does not exist\n' "${log}" >&2; exit 2; }
    local verdict_json
    verdict_json="$(plain_log "${log}" | jq -Rs --argjson revision "${revision}" '
        [split("\n")[] | capture("cluster parameters committed and installed revision=(?<revision>[0-9]+)")
         | .revision | tonumber] as $installed
        | [(if ($installed | length) == 0 then "the process recorded installing no parameter revision"
            elif $installed[0] != $revision
            then "the first parameter revision the process installed was \($installed[0]), not \($revision)"
            else empty end),
           ($installed[] | select(. < $revision) | "the process installed revision \(.), before revision \($revision)")
          ] as $problems
        | {verdict: (if ($problems | length) == 0 then "current" else "stale" end),
           problems: ($problems | unique), revision: $revision, installed: $installed}
    ')"
    write_verdict "${output}" "${verdict_json}" current
}

join_refusal() {
    local log="" mismatch="" output=""
    local survivors=()
    while [[ "$#" -gt 0 ]]; do
        [[ "$#" -ge 2 ]] || usage
        case "$1" in
            --log) log="$2" ;;
            --survivor) survivors+=("$2") ;;
            --mismatch) mismatch="$2" ;;
            --output) output="$2" ;;
            *) usage ;;
        esac
        shift 2
    done
    [[ -n "${log}" && -n "${mismatch}" && -n "${output}" && "${#survivors[@]}" -gt 0 ]] || usage
    [[ -f "${log}" ]] || { printf 'the log %s does not exist\n' "${log}" >&2; exit 2; }
    local verdict_json
    verdict_json="$(plain_log "${log}" | jq -Rs --arg mismatch "${mismatch}" \
        --argjson survivors "$(printf '%s\n' "${survivors[@]}" | jq -R . | jq -sc .)" '
        split("\n") as $lines
        | [$lines[] | select(contains("this node was refused at join and stops"))] as $stops
        | [$stops[] | (capture("refusal=(?<refusal>.*)$") // {refusal: null}) | .refusal] as $refusals
        | [$refusals[] | select(. != null)
           | (capture("^node .(?<peer>[a-z0-9][a-z0-9-]*). cannot run beside this node: (?<mismatch>.*)$")
              // {peer: null, mismatch: .})] as $judged
        | [$lines[] | select(contains("this node was refused at join") and (contains("and stops") | not))] as $reports
        | [$lines[] | select(contains("runtime execution admitted"))] as $admitted
        | [(if ($stops | length) == 0 then "the run never reported that it was refused at join and stops" else empty end),
           (if ($stops | length) > 0 and ($judged | length) == 0 then "the refusal names no peer and no mismatch" else empty end),
           ($judged[] | select(.peer == null or (.peer | IN($survivors[]) | not))
            | "the refusal names \(.peer // "no peer"), not a survivor"),
           ($judged[] | select(.mismatch != $mismatch) | "the refusal reads \(.mismatch), not \($mismatch)"),
           (if ($reports | length) == 0 then "the run ended without an exit report naming its refusal at join" else empty end),
           (if ($admitted | length) > 0 then "the refused run admitted runtime execution" else empty end)
          ] as $problems
        | {verdict: (if ($problems | length) == 0 then "refused" else "not-refused" end),
           problems: ($problems | unique),
           mismatch: $mismatch,
           peers: ([$judged[].peer | select(. != null)] | unique),
           refusals: $judged, exit_report: $reports, runtime_admissions: ($admitted | length)}
    ')"
    write_verdict "${output}" "${verdict_json}" refused
}

seed_refusal() {
    local log="" parameter="" output=""
    while [[ "$#" -gt 0 ]]; do
        [[ "$#" -ge 2 ]] || usage
        case "$1" in
            --log) log="$2" ;;
            --parameter) parameter="$2" ;;
            --output) output="$2" ;;
            *) usage ;;
        esac
        shift 2
    done
    [[ -n "${log}" && -n "${output}" && "${parameter}" =~ ^[a-z][a-z0-9-]*$ ]] || usage
    [[ -f "${log}" ]] || { printf 'the log %s does not exist\n' "${log}" >&2; exit 2; }
    local verdict_json
    verdict_json="$(plain_log "${log}" | jq -Rs --arg parameter "${parameter}" '
        split("\n") as $lines
        | "\($parameter) is a first-bootstrap seed; use ALTER CLUSTER SET" as $refusal
        | [$lines[] | select(contains($refusal))] as $refused
        | [$lines[] | select(contains("runtime execution admitted"))] as $admitted
        | [(if ($refused | length) == 0 then "the run did not refuse \($parameter) as a first-bootstrap seed" else empty end),
           (if ($admitted | length) > 0 then "the run admitted runtime execution" else empty end)] as $problems
        | {verdict: (if ($problems | length) == 0 then "refused" else "not-refused" end),
           problems: $problems, parameter: $parameter, refusal: $refusal, lines: $refused}
    ')"
    write_verdict "${output}" "${verdict_json}" refused
}

survivor_refusals() {
    local node="" prerequisite="" output=""
    local logs=()
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --node) [[ "$#" -ge 2 ]] || usage; node="$2"; shift 2 ;;
            --prerequisite) [[ "$#" -ge 2 ]] || usage; prerequisite="$2"; shift 2 ;;
            --output) [[ "$#" -ge 2 ]] || usage; output="$2"; shift 2 ;;
            -*) usage ;;
            *) logs+=("$1"); shift ;;
        esac
    done
    [[ "${node}" =~ ^node-[0-9]+$ && -n "${prerequisite}" && -n "${output}" && "${#logs[@]}" -gt 0 ]] || usage
    local entries=()
    local log
    for log in "${logs[@]}"; do
        [[ -f "${log}" ]] || { printf 'the log %s does not exist\n' "${log}" >&2; exit 2; }
        entries+=("$(plain_log "${log}" | jq -Rsc --arg log "$(basename "${log}" .log)" \
            --arg node "${node}" --arg prerequisite "${prerequisite}" '
            [split("\n")[]
             | select(contains("prerequisite=\($prerequisite)")
                      and ((contains("refused an interconnect peer that cannot run beside this node")
                            and contains("peer=\($node) "))
                           or (contains("refused a node'"'"'s run that cannot run beside a voter")
                               and contains("candidate=\($node)#"))))]
            | {log: $log, refusals: .}')")
    done
    local verdict_json
    verdict_json="$(printf '%s\n' "${entries[@]}" | jq -sc --arg node "${node}" --arg prerequisite "${prerequisite}" '
        ([.[].refusals[]] | length) as $count
        | {verdict: (if $count > 0 then "refused" else "unreported" end),
           problems: (if $count > 0 then []
                      else ["no survivor reported refusing \($node) over \($prerequisite)"] end),
           node: $node, prerequisite: $prerequisite, refusals: $count, logs: .}')"
    write_verdict "${output}" "${verdict_json}" refused
}

notices() {
    local window="" output=""
    local revisions=()
    local transcripts=()
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --window) [[ "$#" -ge 2 ]] || usage; window="$2"; shift 2 ;;
            --revision) [[ "$#" -ge 2 ]] || usage; revisions+=("$2"); shift 2 ;;
            --output) [[ "$#" -ge 2 ]] || usage; output="$2"; shift 2 ;;
            -*) usage ;;
            *) transcripts+=("$1"); shift ;;
        esac
    done
    [[ -n "${window}" && -n "${output}" && "${#revisions[@]}" -gt 0 && "${#transcripts[@]}" -gt 0 ]] || usage
    local revision
    for revision in "${revisions[@]}"; do
        [[ "${revision}" =~ ^[1-9][0-9]*$ ]] || usage_error "not a parameter revision: ${revision}"
    done
    jq -e '(.start_ns | type == "number") and (.end_ns | type == "number") and .end_ns >= .start_ns' \
        "${window}" >/dev/null 2>&1 || { printf 'the window %s has no start and end\n' "${window}" >&2; exit 2; }
    local observers=()
    local transcript
    for transcript in "${transcripts[@]}"; do
        [[ -f "${transcript}" ]] || { printf 'the transcript %s does not exist\n' "${transcript}" >&2; exit 2; }
        observers+=("$(jq -Rsc --arg observer "$(basename "${transcript}" .log)" --slurpfile window "${window}" '
            $window[0] as $w
            | split("\n") | map(select(length > 0)
                | (capture("^(?<ns>[0-9]+) (?<text>.*)$") // {ns: null, text: .})
                | .ns |= (if . == null then null else tonumber end)) as $lines
            | [$lines[] | select(.ns != null and .ns >= $w.start_ns and .ns <= $w.end_ns)] as $inside
            | {observer: $observer,
               attached_before_window: any($lines[]; .ns != null and .ns <= $w.start_ns
                                           and (.text | contains("listening for events from relay"))),
               unstamped_lines: ([$lines[] | select(.ns == null)] | length),
               gaps: [$inside[] | select((.text | startswith("observer: the notice follow ended"))
                                        or (.text | startswith("[events] notice: "))) | .text],
               parameter_revisions: [$inside[]
                   | .text | capture("^\\[events\\] cluster parameters INFO: cluster parameters committed at revision (?<revision>[0-9]+)$")
                   | .revision],
               membership_or_leadership: [$inside[] | select(.text | test("^\\[events\\] (membership|leadership) ")) | .text]}
        ' "${transcript}")")
    done
    local verdict_json
    verdict_json="$(printf '%s\n' "${observers[@]}" | jq -sc \
        --argjson expected "$(printf '%s\n' "${revisions[@]}" | jq -R . | jq -sc .)" '
        . as $observers
        | [$observers[] as $o
           | (if ($o.attached_before_window | not) then "the observer \($o.observer) attached only after the window opened" else empty end),
             (if ($o.gaps | length) > 0 or $o.unstamped_lines > 0
              then "the observer \($o.observer) did not follow the whole window" else empty end),
             (if $o.parameter_revisions != $expected
              then "the observer \($o.observer) saw cluster-parameter notices for revisions [\($o.parameter_revisions | join(", "))], not exactly [\($expected | join(", "))]"
              else empty end),
             (if ($o.membership_or_leadership | length) > 0
              then "the observer \($o.observer) saw \($o.membership_or_leadership | length) membership or leadership notice(s)"
              else empty end)] as $problems
        | {verdict: (if ($problems | length) == 0 then "announced" else "unannounced" end),
           problems: $problems, expected_revisions: $expected, observers: $observers}
    ')"
    write_verdict "${output}" "${verdict_json}" announced
}

command_name="${1:-}"
[[ -n "${command_name}" ]] || usage
shift
case "${command_name}" in
    describe) describe "$@" ;;
    claims) claims "$@" ;;
    installed) installed "$@" ;;
    samples) samples "$@" ;;
    installations) installations "$@" ;;
    join-refusal) join_refusal "$@" ;;
    seed-refusal) seed_refusal "$@" ;;
    survivor-refusals) survivor_refusals "$@" ;;
    notices) notices "$@" ;;
    -h | --help) usage ;;
    *) usage_error "unknown verify-node-settings-evidence command: ${command_name}" ;;
esac
