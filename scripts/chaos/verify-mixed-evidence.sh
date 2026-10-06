#!/usr/bin/env bash
set -euo pipefail

# Judges the evidence of a mixed-instability run: whether its action trace is complete and covers
# what its plan requires, and whether its continuous samples stayed within the declared limits.

usage() {
    cat >&2 <<'EOF'
usage:
  verify-mixed-evidence.sh trace --plan FILE --trace FILE --output FILE
  verify-mixed-evidence.sh resources --samples FILE --max-memory-bytes N [--max-gap-ms N] --output FILE

trace requires every planned step to have started and ended, and every planned action to have
started, been verified and healed, in that order and inside its step, with no record of an
unplanned step or action. It then requires every coverage item of the plan to be covered by a
verified action: FAMILY:ROLE when the action's node held ROLE when its step selected it, or
FAMILY:cluster for an action on every node, and quorum-loss when a step left two voters out of a
quorum. It exits 1 when the trace is incomplete or a required item is uncovered.

resources summarizes the sampled broker boundaries and every node's Docker memory and CPU, and
reports each node whose memory exceeded --max-memory-bytes as a finding. It exits 1 when two
consecutive samples lie more than --max-gap-ms apart (default 120000), because the continuous
observation then has a gap.
EOF
}

trace() {
    local plan="" trace_file="" output=""
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --plan | --trace | --output)
                [[ "$#" -ge 2 ]] || { usage; exit 2; }
                case "$1" in
                    --plan) plan="$2" ;;
                    --trace) trace_file="$2" ;;
                    --output) output="$2" ;;
                esac
                shift 2
                ;;
            *)
                printf 'unknown trace argument: %s\n' "$1" >&2
                usage
                exit 2
                ;;
        esac
    done
    [[ -n "${plan}" && -n "${trace_file}" && -n "${output}" ]] || { usage; exit 2; }
    [[ -s "${plan}" ]] || { printf 'the plan is missing: %s\n' "${plan}" >&2; exit 2; }
    [[ -f "${trace_file}" ]] || { printf 'the action trace is missing: %s\n' "${trace_file}" >&2; exit 2; }
    if ! jq -e -s 'all(.[]; type == "object" and (.event | type == "string") and (.step | type == "number")
                      and (.at_ms | type == "number"))' "${trace_file}" >/dev/null 2>&1; then
        printf 'the action trace is not a sequence of trace records: %s\n' "${trace_file}" >&2
        exit 2
    fi
    jq -n --slurpfile plan "${plan}" --slurpfile records "${trace_file}" '
        $plan[0] as $plan
        | def records($event; $step): [$records[] | select(.event == $event and .step == $step)];
          def action_records($event; $action): [$records[] | select(.event == $event and .action == $action)];
        [ $plan.steps[] as $step
          | records("step-started"; $step.index) as $started
          | records("step-ended"; $step.index) as $ended
          | (if ($started | length) != 1 then "step \($step.index) has \($started | length) start records, not 1" else empty end),
            (if ($ended | length) != 1 then "step \($step.index) has \($ended | length) end records, not 1" else empty end),
            (if ($started | length) == 1 and ($ended | length) == 1 and $ended[0].at_ms < $started[0].at_ms
             then "step \($step.index) ended before it started" else empty end),
            ($step.actions[] as $action
             | [action_records("action-started"; $action.id), action_records("action-verified"; $action.id),
                action_records("action-healed"; $action.id)] as $phases
             | (["started", "verified", "healed"] | to_entries[]
                | select(($phases[.key] | length) != 1)
                | "action \($action.id) has \($phases[.key] | length) \(.value) records, not 1"),
               (if all($phases[]; length == 1) then
                  ($phases | map(.[0])) as $ordered
                  | (if $ordered[0].at_ms > $ordered[1].at_ms or $ordered[1].at_ms > $ordered[2].at_ms
                     then "action \($action.id) was not started, verified and healed in that order" else empty end),
                    (if any($ordered[]; .step != $step.index) then "action \($action.id) is recorded outside step \($step.index)" else empty end),
                    (if ($started | length) == 1 and ($ended | length) == 1
                        and ($ordered[0].at_ms < $started[0].at_ms or $ordered[2].at_ms > $ended[0].at_ms)
                     then "action \($action.id) is recorded outside the bounds of step \($step.index)" else empty end)
                else empty end))
        ] as $missing
        | ([$plan.steps[].index]) as $step_indexes
        | ([$plan.steps[].actions[].id]) as $action_ids
        | [ $records[] | . as $record | select(($step_indexes | index($record.step)) == null)
            | "\(.event) record names unplanned step \(.step)" ]
          + [ $records[] | . as $record | select($record.action != null)
              | select(($action_ids | index($record.action)) == null)
              | "\(.event) record names unplanned action \(.action)" ]
          + [ $records[] | select(.event == "action-refused") | "action \(.action) was refused: \(.reason)" ]
          as $unexpected
        | ([$records[] | select(.event == "action-verified") | .action]) as $verified
        | ([ $records[] | select(.event == "action-started") | . as $start
             | select(($verified | index($start.action)) != null)
             | if $start.intended.node == "cluster" then "\($start.family):cluster"
               else ($start.targets[].roles[] | "\($start.family):\(.)") end ]
           + (if any($records[]; .event == "quorum-lost") then ["quorum-loss"] else [] end)
           | unique) as $covered
        | ([$plan.coverage[] | . as $item | select(($covered | index($item)) == null)]) as $uncovered
        | {verdict: (if ($missing | length) > 0 or ($unexpected | length) > 0 then "incomplete"
                     elif ($uncovered | length) > 0 then "uncovered"
                     else "complete" end),
           missing_records: $missing,
           unexpected_records: $unexpected,
           coverage: {required: $plan.coverage, covered: $covered, missing: $uncovered}}
    ' >"${output}"
    local verdict
    verdict="$(jq -r '.verdict' "${output}")"
    printf 'action trace verdict: %s\n' "${verdict}"
    jq -r '.missing_records[], .unexpected_records[] | "  " + .' "${output}"
    jq -r '.coverage.missing[] | "  coverage item without a verified action: " + .' "${output}"
    [[ "${verdict}" == complete ]] || exit 1
}

resources() {
    local samples="" limit="" max_gap_ms=120000 output=""
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --samples | --max-memory-bytes | --max-gap-ms | --output)
                [[ "$#" -ge 2 ]] || { usage; exit 2; }
                case "$1" in
                    --samples) samples="$2" ;;
                    --max-memory-bytes) limit="$2" ;;
                    --max-gap-ms) max_gap_ms="$2" ;;
                    --output) output="$2" ;;
                esac
                shift 2
                ;;
            *)
                printf 'unknown resources argument: %s\n' "$1" >&2
                usage
                exit 2
                ;;
        esac
    done
    [[ -n "${samples}" && -n "${output}" ]] || { usage; exit 2; }
    [[ "${limit}" =~ ^[0-9]+$ && "${max_gap_ms}" =~ ^[0-9]+$ ]] \
        || { printf '%s\n' '--max-memory-bytes and --max-gap-ms must be whole numbers' >&2; exit 2; }
    [[ -s "${samples}" ]] || { printf 'no samples were recorded: %s\n' "${samples}" >&2; exit 2; }
    if ! jq -e -s 'length > 0 and all(.[]; type == "object" and (.at_ms | type == "number")
                      and (.nodes | type == "array"))' "${samples}" >/dev/null 2>&1; then
        printf 'the samples do not satisfy the sample contract: %s\n' "${samples}" >&2
        exit 2
    fi
    jq -s --argjson limit "${limit}" --argjson max_gap_ms "${max_gap_ms}" '
        def gaps($values): [range(1; $values | length) | $values[.] - $values[. - 1]];
        # The longest interval between two samples whose FIELD advanced, or null without two of them.
        def longest_stall($field):
          [.[] | select(.[$field] != null)] as $known
          | [range(1; $known | length) | select($known[.][$field] > $known[. - 1][$field]) | $known[.].at_ms] as $advances
          | if ($advances | length) < 2 then null else (gaps($advances) | max) end;
        sort_by(.at_ms) as $samples
        | ([.[].nodes[].container] | unique) as $containers
        | [ $containers[] as $container
            | [$samples[] | . as $sample | .nodes[] | select(.container == $container)
               | {at_ms: $sample.at_ms, memory_bytes, cpu_percent, status}] as $series
            | [$series[] | select(.memory_bytes != null)] as $measured
            | {container: $container,
               samples: ($series | length),
               statuses: ([$series[].status] | group_by(.) | map({key: (.[0] // "unknown"), value: length}) | from_entries),
               first_memory_bytes: ($measured[0].memory_bytes // null),
               last_memory_bytes: ($measured[-1].memory_bytes // null),
               max_memory_bytes: ([$measured[].memory_bytes] | max),
               max_cpu_percent: ([$series[].cpu_percent | numbers] | max),
               over_limit: [$measured[] | select(.memory_bytes > $limit)]} ] as $nodes
        | (gaps([$samples[].at_ms]) | max // 0) as $longest_gap
        | {verdict: (if $longest_gap > $max_gap_ms then "gapped"
                     elif any($nodes[]; (.over_limit | length) > 0) then "over-limit"
                     else "within-limits" end),
           findings: [$nodes[] | select((.over_limit | length) > 0)
                      | (.over_limit | max_by(.memory_bytes)) as $peak
                      | {metric: "docker_memory_bytes", container, limit: $limit,
                         samples_over_limit: (.over_limit | length), peak: $peak.memory_bytes, peak_at_ms: $peak.at_ms,
                         message: "Docker memory of \(.container) exceeded the declared \($limit) bytes in \(.over_limit | length) samples, peaking at \($peak.memory_bytes) bytes"}],
           summary: {samples: ($samples | length), first_at_ms: $samples[0].at_ms, last_at_ms: $samples[-1].at_ms,
                     longest_sample_gap_ms: $longest_gap, max_sample_gap_ms: $max_gap_ms,
                     samples_without_broker_boundaries: ([$samples[] | select(.source_end == null or .output_end == null)] | length),
                     max_backlog: ([$samples[].backlog | numbers] | max),
                     longest_source_stall_ms: ($samples | longest_stall("source_end")),
                     longest_output_stall_ms: ($samples | longest_stall("output_end")),
                     max_memory_bytes_limit: $limit,
                     nodes: ($nodes | map(del(.over_limit)))}}
    ' "${samples}" >"${output}"
    local verdict
    verdict="$(jq -r '.verdict' "${output}")"
    printf 'resource verdict: %s\n' "${verdict}"
    jq -r '.findings[] | "  " + .message' "${output}"
    if [[ "${verdict}" == gapped ]]; then
        printf '  consecutive samples lay %s ms apart, more than %s ms\n' \
            "$(jq -r '.summary.longest_sample_gap_ms' "${output}")" "${max_gap_ms}"
        exit 1
    fi
}

command_name="${1:-}"
[[ -n "${command_name}" ]] || { usage; exit 2; }
shift
case "${command_name}" in
    trace)
        trace "$@"
        ;;
    resources)
        resources "$@"
        ;;
    -h | --help)
        usage
        ;;
    *)
        printf 'unknown verify-mixed-evidence command: %s\n' "${command_name}" >&2
        usage
        exit 2
        ;;
esac
