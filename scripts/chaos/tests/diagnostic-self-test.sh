#!/usr/bin/env bash
set -euo pipefail

# Self-test of the diagnostic evidence verdict: every verdict of verify-diagnostic-evidence.sh on
# evidence a run on a Deloxide diagnostic image could leave behind, from synthetic Docker event
# recordings and the qualification records the controller writes with the image's report tool.

chaos_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
verify="${chaos_dir}/verify-diagnostic-evidence.sh"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

fail() {
    printf 'diagnostic self-test failed: %s\n' "$*" >&2
    exit 1
}

base_ns=1800000000000000000
clean_summary='evidence summary: scope=whole-process findings=0 active=0 potential=0 unreviewed=0 nonqualifying=0 repeated-deliveries=0 lost-handoff=0 lost-order-history=0 lost-retention=0
diagnostic evidence qualifies'
potential_summary='evidence summary: scope=whole-process findings=1 active=0 potential=1 unreviewed=1 nonqualifying=1 repeated-deliveries=2 lost-handoff=0 lost-order-history=0 lost-retention=0
diagnostic evidence cannot qualify: selected scope or active, lost, incomplete or unreviewed findings'
active_summary='evidence summary: scope=whole-process findings=1 active=1 potential=0 unreviewed=0 nonqualifying=1 repeated-deliveries=0 lost-handoff=0 lost-order-history=0 lost-retention=0
diagnostic evidence cannot qualify: selected scope or active, lost, incomplete or unreviewed findings'
lost_summary='evidence summary: scope=whole-process findings=1 active=0 potential=0 unreviewed=0 nonqualifying=1 repeated-deliveries=0 lost-handoff=0 lost-order-history=3 lost-retention=0
diagnostic evidence cannot qualify: selected scope or active, lost, incomplete or unreviewed findings'

# Prints one recorded node event: SERVICE ACTION OFFSET_MS [ATTRIBUTES_JSON].
node_event() {
    jq -nc \
        --arg service "$1" \
        --arg action "$2" \
        --argjson ns "$((base_ns + $3 * 1000000))" \
        --argjson attributes "${4:-{\}}" '
        {Type: "container", Action: $action,
         Actor: {ID: ($service + "-container"),
                 Attributes: ({"io.nervix.chaos.run": "self-test", "io.nervix.chaos.role": "node",
                               "com.docker.compose.service": $service, name: $service} + $attributes)},
         scope: "local", time: ($ns / 1000000000 | floor), timeNano: $ns}'
}

# Prints the qualification record of one evidence file: NODE NAME QUALIFY_EXIT QUALIFY_OUTPUT
# [SELECTION] [BYTES].
evidence_file() {
    jq -nc \
        --arg node "$1" \
        --arg name "$2" \
        --argjson status "$3" \
        --arg output "$4" \
        --arg selection "${5:-OrderAnalysis}" \
        --argjson bytes "${6:-512}" '
        {node: $node, kind: "evidence", file: "deadlock/\($node)/\($name)", bytes: $bytes,
         qualify_exit: $status, qualify_output: $output,
         inspect_head: "process 7, selection \($selection), scope WholeProcess; evidence qualifies: \($status == 0)",
         qualification: "deadlock/\($node)/\($name).qualify.txt",
         inspection: "deadlock/\($node)/\($name).inspect.txt"}'
}

# The recording of a three-node run whose first node crashed once and was started again.
three_node_recording() {
    node_event nervix-1 start 100
    node_event nervix-2 start 200
    node_event nervix-3 start 300
    node_event nervix-1 kill 1000 '{"signal":"9"}'
    node_event nervix-1 die 1001 '{"exitCode":"137"}'
    node_event nervix-1 start 1500
}

clean_qualification() {
    evidence_file node-1 deadlock-7-1.rkyv 0 "${clean_summary}"
    evidence_file node-1 deadlock-7-2.rkyv 0 "${clean_summary}"
    evidence_file node-2 deadlock-7-1.rkyv 0 "${clean_summary}"
    evidence_file node-3 deadlock-7-1.rkyv 0 "${clean_summary}"
}

# Runs the verifier over RECORDING and QUALIFICATION for a three-node deloxide-order run and
# expects STATUS.
judge() {
    local case_name="$1"
    local expected_status="$2"
    local recording="$3"
    local qualification="$4"
    shift 4
    local status=0
    "${verify}" --recording "${recording}" --recording-covered "${covered:-true}" \
        --qualification "${qualification}" --selection "${selection:-deloxide-order}" \
        --nodes "${nodes:-3}" --max-files-per-node "${max_files:-64}" \
        --max-bytes "${max_bytes:-67108864}" --output "${tmp_dir}/${case_name}.json" "$@" \
        >"${tmp_dir}/${case_name}.out" 2>&1 || status=$?
    [[ "${status}" -eq "${expected_status}" ]] \
        || fail "${case_name} returned ${status}, expected ${expected_status}: $(cat "${tmp_dir}/${case_name}.out")"
}

expect_result() {
    local case_name="$1"
    local check="$2"
    jq -e "${check}" "${tmp_dir}/${case_name}.json" >/dev/null \
        || fail "${case_name} recorded $(jq -c '{verdict, category, totals, problems}' "${tmp_dir}/${case_name}.json")"
}

three_node_recording >"${tmp_dir}/recording.ndjson"

# A clean run: one qualifying file per process start, including the restarted one.
clean_qualification >"${tmp_dir}/clean.ndjson"
judge clean 0 "${tmp_dir}/recording.ndjson" "${tmp_dir}/clean.ndjson"
expect_result clean '.verdict == "passed" and .category == null and .problems == []
    and .evidence_selection == "OrderAnalysis"
    and .totals == {process_starts: 4, evidence_files: 4, bytes: 2048, findings: 0, active: 0,
                    potential: 0, unreviewed: 0, lost: 0}
    and (.nodes | map({node, process_starts, evidence_files, exit_codes}))
        == [{node: "node-1", process_starts: 2, evidence_files: 2, exit_codes: ["137"]},
            {node: "node-2", process_starts: 1, evidence_files: 1, exit_codes: []},
            {node: "node-3", process_starts: 1, evidence_files: 1, exit_codes: []}]
    and (.files | length) == 4 and all(.files[]; has("qualify_output") | not)'

# A process start without evidence: the restarted process never recorded its run.
clean_qualification | sed '2d' >"${tmp_dir}/missing.ndjson"
judge missing 1 "${tmp_dir}/recording.ndjson" "${tmp_dir}/missing.ndjson"
expect_result missing '.verdict == "failed" and .category == "diagnostic"
    and any(.problems[]; .node == "node-1" and (.reason | test("started 2 process.*recorded 1 evidence")))'

# Evidence of a process the recording never started is not taken for coverage either.
{
    clean_qualification
    evidence_file node-2 deadlock-9-3.rkyv 0 "${clean_summary}"
} >"${tmp_dir}/extra.ndjson"
judge extra 1 "${tmp_dir}/recording.ndjson" "${tmp_dir}/extra.ndjson"
expect_result extra 'any(.problems[]; .node == "node-2" and (.reason | test("started 1 process.*recorded 2 evidence")))'

# An unreviewed potential cycle fails the run and is counted.
{
    evidence_file node-1 deadlock-7-1.rkyv 0 "${clean_summary}"
    evidence_file node-1 deadlock-7-2.rkyv 5 "${potential_summary}"
    evidence_file node-2 deadlock-7-1.rkyv 0 "${clean_summary}"
    evidence_file node-3 deadlock-7-1.rkyv 0 "${clean_summary}"
} >"${tmp_dir}/potential.ndjson"
judge potential 1 "${tmp_dir}/recording.ndjson" "${tmp_dir}/potential.ndjson"
expect_result potential '.category == "diagnostic" and .active_deadlock == false
    and .totals.potential == 1 and .totals.unreviewed == 1 and .totals.findings == 1
    and any(.problems[]; .finding == "nonqualifying" and (.reason | test("potential=1 unreviewed=1")))'

# Loss is counted and fails the run.
{
    evidence_file node-1 deadlock-7-1.rkyv 0 "${clean_summary}"
    evidence_file node-1 deadlock-7-2.rkyv 0 "${clean_summary}"
    evidence_file node-2 deadlock-7-1.rkyv 5 "${lost_summary}"
    evidence_file node-3 deadlock-7-1.rkyv 0 "${clean_summary}"
} >"${tmp_dir}/lost.ndjson"
judge lost 1 "${tmp_dir}/recording.ndjson" "${tmp_dir}/lost.ndjson"
expect_result lost '.totals.lost == 3 and .category == "diagnostic"'

# A process that ended with the active-deadlock status is an active deadlock, whatever its file says.
{
    three_node_recording
    node_event nervix-2 die 2000 '{"exitCode":"3"}'
} >"${tmp_dir}/active-recording.ndjson"
{
    evidence_file node-1 deadlock-7-1.rkyv 0 "${clean_summary}"
    evidence_file node-1 deadlock-7-2.rkyv 0 "${clean_summary}"
    evidence_file node-2 deadlock-7-1.rkyv 5 "${active_summary}"
    evidence_file node-3 deadlock-7-1.rkyv 0 "${clean_summary}"
} >"${tmp_dir}/active.ndjson"
judge active 1 "${tmp_dir}/active-recording.ndjson" "${tmp_dir}/active.ndjson"
expect_result active '.active_deadlock == true and .diagnostic_failure == false and .totals.active == 1
    and any(.problems[]; .finding == "active-deadlock" and .node == "node-2")'

# A process that ended with the diagnostic-failure status is a failed diagnostic execution.
{
    three_node_recording
    node_event nervix-3 die 2000 '{"exitCode":"4"}'
} >"${tmp_dir}/failure-recording.ndjson"
judge failure 1 "${tmp_dir}/failure-recording.ndjson" "${tmp_dir}/clean.ndjson"
expect_result failure '.diagnostic_failure == true and .category == "diagnostic"
    and any(.problems[]; .finding == "diagnostic-failure" and .node == "node-3")'

# Invalid evidence, a file the tool never judged, and a tool that could not run.
clean_qualification | jq -c 'if .node == "node-2" then .qualify_exit = 4 else . end' >"${tmp_dir}/invalid.ndjson"
judge invalid 1 "${tmp_dir}/recording.ndjson" "${tmp_dir}/invalid.ndjson"
expect_result invalid '.category == "diagnostic" and any(.problems[]; .reason | test("not valid evidence"))'
clean_qualification \
    | jq -c 'if .node == "node-3" then .qualify_exit = null | .skipped = "the node recorded more files than the run keeps" else . end' \
    >"${tmp_dir}/skipped.ndjson"
judge skipped 1 "${tmp_dir}/recording.ndjson" "${tmp_dir}/skipped.ndjson"
expect_result skipped 'any(.problems[]; .reason | test("was not qualified: the node recorded more files"))'
clean_qualification | jq -c 'if .node == "node-3" then .qualify_exit = 125 else . end' >"${tmp_dir}/tool.ndjson"
judge tool 1 "${tmp_dir}/recording.ndjson" "${tmp_dir}/tool.ndjson"
expect_result tool '.category == "controller" and any(.problems[]; .reason | test("report tool exited 125"))'
clean_qualification | jq -c 'if .node == "node-3" then .qualify_output = "diagnostic evidence qualifies" else . end' \
    >"${tmp_dir}/silent.ndjson"
judge silent 1 "${tmp_dir}/recording.ndjson" "${tmp_dir}/silent.ndjson"
expect_result silent '.category == "controller" and any(.problems[]; .reason | test("without printing its evidence summary"))'

# Evidence of another selection than the image declares.
{
    evidence_file node-1 deadlock-7-1.rkyv 0 "${clean_summary}"
    evidence_file node-1 deadlock-7-2.rkyv 0 "${clean_summary}" OrderInstrumentedActiveOnly
    evidence_file node-2 deadlock-7-1.rkyv 0 "${clean_summary}"
    evidence_file node-3 deadlock-7-1.rkyv 0 "${clean_summary}"
} >"${tmp_dir}/selection.ndjson"
judge selection 1 "${tmp_dir}/recording.ndjson" "${tmp_dir}/selection.ndjson"
expect_result selection 'any(.problems[]; .reason | test("records selection OrderInstrumentedActiveOnly, not the OrderAnalysis"))'
selection=deloxide-stress judge stress 0 "${tmp_dir}/recording.ndjson" \
    <(clean_qualification | jq -c '.inspect_head = "process 7, selection StressedActiveOnly(StressConfiguration { probability_millionths: 50000 }), scope WholeProcess; evidence qualifies: true"')
expect_result stress '.verdict == "passed" and .evidence_selection == "StressedActiveOnly"'

# A recording interrupted by a write is never complete evidence.
{
    clean_qualification
    jq -nc '{node: "node-3", kind: "partial", file: "deadlock/node-3/deadlock-7-1.partial", bytes: 64}'
} >"${tmp_dir}/partial.ndjson"
judge partial 1 "${tmp_dir}/recording.ndjson" "${tmp_dir}/partial.ndjson"
expect_result partial 'any(.problems[]; .node == "node-3" and (.reason | test("partially written")))'

# Bounds: files per node and bytes for the whole run.
max_files=1 judge file-bound 1 "${tmp_dir}/recording.ndjson" "${tmp_dir}/clean.ndjson"
expect_result file-bound 'any(.problems[]; .node == "node-1" and (.reason | test("more than the run keeps \\(1\\)")))'
max_bytes=2047 judge byte-bound 1 "${tmp_dir}/recording.ndjson" "${tmp_dir}/clean.ndjson"
expect_result byte-bound 'any(.problems[]; .reason | test("2048 bytes of evidence, more than its bound of 2047"))'

# Without a complete recording the owed files are unknown, which is a controller failure.
covered=false judge uncovered 1 "${tmp_dir}/recording.ndjson" "${tmp_dir}/clean.ndjson"
expect_result uncovered '.category == "controller" and .recording_covered == false and (.problems | length) == 1'

# A one-node run owes evidence only for its one node.
nodes=1 judge one-node 0 "${tmp_dir}/recording.ndjson" <(clean_qualification | jq -c 'select(.node == "node-1")')
expect_result one-node '.verdict == "passed" and (.nodes | map(.node)) == ["node-1"]'

# Usage errors.
status=0
"${verify}" --recording "${tmp_dir}/recording.ndjson" --recording-covered true \
    --qualification "${tmp_dir}/clean.ndjson" --selection ordinary --nodes 3 \
    --max-files-per-node 64 --max-bytes 1 --output "${tmp_dir}/usage.json" >/dev/null 2>&1 || status=$?
[[ "${status}" -eq 2 ]] || fail "an unknown selection returned ${status}, expected 2"

printf 'diagnostic evidence self-test passed\n'
