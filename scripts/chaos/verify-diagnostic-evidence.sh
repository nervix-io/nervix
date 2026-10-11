#!/usr/bin/env bash
# Judges the deadlock evidence the nodes of a run on a Deloxide diagnostic image recorded. Every
# process a node container started records one evidence file when its diagnostic run starts, and
# replaces it after each finding, so the run's live Docker event recording says how many files each
# node owes. The controller qualifies every file with the image's own `nervix-deadlock-report`, and
# this verifier reads those results beside the recording:
#
# - every node holds exactly one complete evidence file for each start of its container;
# - every file is valid, records the selection the image declares, and qualifies: it holds no
#   active cycle, no unreviewed potential cycle and no loss;
# - no node container ended with the status a diagnostic process ends with after an active
#   deadlock (3) or a failed diagnostic execution (4);
# - the evidence fits the run's bounds.
#
# It writes the verdict and the counts to OUTPUT and exits 0 when the evidence qualifies, 1 when it
# does not, and 2 for a usage error.
set -euo pipefail

usage() {
    cat >&2 <<'EOF'
usage: verify-diagnostic-evidence.sh --recording EVENTS_NDJSON --recording-covered true|false
         --qualification QUALIFICATION_NDJSON --selection SELECTION --nodes 1|3
         --max-files-per-node N --max-bytes N --output RESULT_JSON
EOF
    exit 2
}

recording=""
recording_covered=""
qualification=""
selection=""
node_count=""
max_files_per_node=""
max_bytes=""
output=""
while [[ "$#" -gt 0 ]]; do
    [[ "$#" -ge 2 ]] || usage
    case "$1" in
        --recording) recording="$2" ;;
        --recording-covered) recording_covered="$2" ;;
        --qualification) qualification="$2" ;;
        --selection) selection="$2" ;;
        --nodes) node_count="$2" ;;
        --max-files-per-node) max_files_per_node="$2" ;;
        --max-bytes) max_bytes="$2" ;;
        --output) output="$2" ;;
        *) usage ;;
    esac
    shift 2
done
[[ -n "${recording}" && -n "${qualification}" && -n "${output}" ]] || usage
[[ "${recording_covered}" == true || "${recording_covered}" == false ]] || usage
[[ "${node_count}" == 1 || "${node_count}" == 3 ]] || usage
[[ "${max_files_per_node}" =~ ^[1-9][0-9]*$ && "${max_bytes}" =~ ^[1-9][0-9]*$ ]] || usage

# The selection a diagnostic image declares, and the one its nodes record in their evidence.
case "${selection}" in
    deloxide) evidence_selection=ActiveOnly ;;
    deloxide-order) evidence_selection=OrderAnalysis ;;
    deloxide-stress) evidence_selection=StressedActiveOnly ;;
    *)
        printf 'unknown diagnostic selection: %s\n' "${selection}" >&2
        exit 2
        ;;
esac

[[ -e "${recording}" ]] || recording=/dev/null
[[ -e "${qualification}" ]] || qualification=/dev/null

jq -n \
    --slurpfile events "${recording}" \
    --slurpfile files "${qualification}" \
    --argjson covered "${recording_covered}" \
    --arg selection "${selection}" \
    --arg evidence_selection "${evidence_selection}" \
    --argjson nodes "${node_count}" \
    --argjson max_files "${max_files_per_node}" \
    --argjson max_bytes "${max_bytes}" '
    # The key=value counts of a `qualify` summary line, as numbers.
    def summary_counts:
        if type != "string" then null
        else (capture("evidence summary: (?<line>[^\n]*)").line // null)
             | if . == null then null
               else [splits(" ") | select(test("^[a-z-]+=")) | capture("^(?<key>[a-z-]+)=(?<value>.*)$")]
                    | map({key, value: (.value | tonumber? // .)}) | from_entries end end;
    # The selection the first line of an `inspect` names, without a stress configuration.
    def recorded_selection:
        if type != "string" then null
        else (capture("selection (?<selection>[A-Za-z]+)").selection // null) end;
    def node_events($service):
        [$events[] | select(.Type == "container"
                            and .Actor.Attributes["io.nervix.chaos.role"] == "node"
                            and .Actor.Attributes["com.docker.compose.service"] == $service)];

    [range(1; $nodes + 1)] as $numbers
    | [$numbers[] as $number
       | "nervix-\($number)" as $service
       | "node-\($number)" as $node
       | node_events($service) as $node_events
       | [$files[] | select(.node == $node)] as $node_files
       | [$node_files[] | select(.kind == "evidence")] as $evidence
       | [$node_files[] | select(.kind == "partial")] as $partial
       | ($evidence
          | map(. + {summary_counts: (.qualify_output | summary_counts),
                     recorded_selection: (.inspect_head | recorded_selection)})) as $judged
       | {node: $node, service: $service,
          process_starts: ([$node_events[] | select(.Action == "start")] | length),
          exit_codes: ([$node_events[] | select(.Action == "die") | .Actor.Attributes.exitCode // "unknown"]),
          evidence_files: ($evidence | length),
          partial_files: ($partial | length),
          bytes: ([$node_files[].bytes // 0] | add // 0),
          files: $judged}]
    | . as $node_records
    | [ (if $covered | not then
           {category: "controller",
            reason: "the run has no complete Docker event recording, so the processes each node started are unknown"}
         else empty end),
        ($node_records[]
         | . as $record
         | (if $covered and .process_starts != .evidence_files then
              {category: "diagnostic", node: .node,
               reason: "\(.node) started \(.process_starts) process(es) but recorded \(.evidence_files) evidence file(s)"}
            else empty end),
           (if .evidence_files > $max_files then
              {category: "diagnostic", node: .node,
               reason: "\(.node) recorded \(.evidence_files) evidence files, more than the run keeps (\($max_files))"}
            else empty end),
           (if .partial_files > 0 then
              {category: "diagnostic", node: .node,
               reason: "\(.node) left \(.partial_files) partially written evidence file(s), so a recording did not complete"}
            else empty end),
           (.exit_codes[] | select(. == "3")
            | {category: "diagnostic", node: $record.node, finding: "active-deadlock",
               reason: "a process of \($record.node) ended with status 3 after the detector reported an active deadlock"}),
           (.exit_codes[] | select(. == "4")
            | {category: "diagnostic", node: $record.node, finding: "diagnostic-failure",
               reason: "a process of \($record.node) ended with status 4 because its diagnostic execution failed"}),
           (.files[]
            | . as $file
            | if .qualify_exit == null then
                {category: "diagnostic", node: $record.node, file: .file,
                 reason: "\(.file) was not qualified: \(.skipped // "the controller did not run the report tool")"}
              elif .qualify_exit == 5 then
                {category: "diagnostic", node: $record.node, file: .file, finding: "nonqualifying",
                 reason: "\(.file) does not qualify: \((.qualify_output // "") | capture("evidence summary: (?<line>[^\n]*)").line // "no summary")"}
              elif .qualify_exit == 4 then
                {category: "diagnostic", node: $record.node, file: .file,
                 reason: "\(.file) is not valid evidence; the report tool refused it"}
              elif .qualify_exit != 0 then
                {category: "controller", node: $record.node, file: .file,
                 reason: "the report tool exited \(.qualify_exit) on \(.file), so the file was not judged"}
              elif .summary_counts == null then
                {category: "controller", node: $record.node, file: .file,
                 reason: "the report tool qualified \(.file) without printing its evidence summary"}
              elif .recorded_selection != $evidence_selection then
                {category: "diagnostic", node: $record.node, file: .file,
                 reason: "\(.file) records selection \(.recorded_selection // "none"), not the \($evidence_selection) the image declares"}
              else empty end)),
        (([$node_records[].bytes] | add // 0) as $total
         | if $total > $max_bytes then
             {category: "diagnostic",
              reason: "the run recorded \($total) bytes of evidence, more than its bound of \($max_bytes)"}
           else empty end) ] as $problems
    | ([$node_records[].files[].summary_counts | select(. != null)]) as $counts
    | {verdict: (if ($problems | length) == 0 then "passed" else "failed" end),
       category: (if ($problems | length) == 0 then null
                  elif any($problems[]; .category == "diagnostic") then "diagnostic"
                  else "controller" end),
       active_deadlock: any($problems[]; .finding == "active-deadlock"),
       diagnostic_failure: any($problems[]; .finding == "diagnostic-failure"),
       selection: $selection,
       evidence_selection: $evidence_selection,
       recording_covered: $covered,
       limits: {files_per_node: $max_files, bytes: $max_bytes},
       totals: {process_starts: ([$node_records[].process_starts] | add // 0),
                evidence_files: ([$node_records[].evidence_files] | add // 0),
                bytes: ([$node_records[].bytes] | add // 0),
                findings: ([$counts[].findings // 0] | add // 0),
                active: ([$counts[].active // 0] | add // 0),
                potential: ([$counts[].potential // 0] | add // 0),
                unreviewed: ([$counts[].unreviewed // 0] | add // 0),
                lost: ([$counts[] | (.["lost-handoff"] // 0) + (.["lost-order-history"] // 0)
                                    + (.["lost-retention"] // 0)] | add // 0)},
       nodes: [$node_records[] | del(.files)],
       files: [$node_records[].files[] | del(.qualify_output, .inspect_head)],
       problems: $problems}
' >"${output}"

if jq -e '.verdict == "passed"' "${output}" >/dev/null; then
    exit 0
fi
jq -r '.problems[].reason' "${output}" >&2
exit 1
