#!/usr/bin/env bash
set -euo pipefail

# Decides the domain-time verdicts from what independent observers recorded: each clock
# observer's transcript of the packaged CLI's `domain-clock` follow, every line stamped with the
# host's clock, the paced window output with its Kafka timestamps, and the rounds the runner
# faulted. Every mode writes its verdict to --result and exits 1 when the evidence contradicts the
# documented domain-time contract or does not exercise it.

usage() {
    cat >&2 <<'EOF'
usage:
  verify-clock-evidence.sh clock --observer HOST=FILE... --rounds FILE --fault FAULT
      --period-ms N --rate N --result FILE
  verify-clock-evidence.sh windows --output FILE --rounds FILE --clock FILE --fault FAULT
      --width-ms N --rate N --result FILE

clock requires every attach and state report to carry the first report's generation and mapping,
every tick to lie on the mapping's period grid in that generation and to be no later than the
serving node's own logical time, tick ids to increase within each attachment, ticks to resume on
every observer after each fault, and every stall of a node that stayed up to fall inside a fault
and end within the authority bound, and within the replacement bound after a surviving node
reported the faulted node unavailable. A rotation over every voter must show the stall that
removing the authority causes.

windows requires every paced window to close no earlier than its logical width after it opened and,
away from relocations and restarts of its own node, no later than the lateness bound; to hold rows
of one branch; and to keep closing while an authority stall withheld ticks, unless the round that
caused the stall also took the graph's own node down, as a graceful stop or a cluster restart does.
EOF
}

fail_usage() {
    printf '%s\n' "$*" >&2
    usage
    exit 2
}

mode="${1:-}"
[[ -n "${mode}" ]] || fail_usage 'a verifier mode is required'
shift
observers=()
rounds=""
fault=""
period_ms=""
rate=""
result=""
output=""
clock=""
width_ms=""
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --observer | --rounds | --fault | --period-ms | --rate | --result | --output | --clock | --width-ms)
            [[ "$#" -ge 2 ]] || fail_usage "$1 requires a value"
            case "$1" in
                --observer) observers+=("$2") ;;
                --rounds) rounds="$2" ;;
                --fault) fault="$2" ;;
                --period-ms) period_ms="$2" ;;
                --rate) rate="$2" ;;
                --result) result="$2" ;;
                --output) output="$2" ;;
                --clock) clock="$2" ;;
                --width-ms) width_ms="$2" ;;
            esac
            shift 2
            ;;
        *) fail_usage "unknown ${mode} argument: $1" ;;
    esac
done
[[ -n "${result}" ]] || fail_usage '--result is required'
[[ -n "${fault}" ]] || fail_usage '--fault is required'
[[ -f "${rounds}" ]] || fail_usage "the round ledger is missing: ${rounds}"
[[ "${rate}" =~ ^[0-9]+$ && "${rate}" -gt 0 ]] || fail_usage '--rate must be a positive integer'
mkdir -p "$(dirname "${result}")"

# A tick that stops for longer than this on an observer whose node stayed up is a stall, and an
# authority repair must end a stall within the authority bound. Both are physical.
stall_ms=2000
authority_bound_ms=60000
# Once a surviving node no longer counts the faulted node available, its interconnect entry turning
# unavailable or leaving, the leader reconciles the authority and the replacement ticks as soon as
# every live node is ready; a stall that outlasts this physical bound after that report means no
# other voter took the clock over.
replace_bound_ms=10000
# A paced window may close at most this much logical time after its width, away from moves and
# restarts of its own node.
late_bound_ms=8000

# Parses one RFC 3339 instant with an optional fraction of up to nine digits into whole seconds and
# nanoseconds, so differences stay exact.
# The dollars in this jq program are jq variables, not shell expansion.
# shellcheck disable=SC2016
instant_definitions='
    def instant:
        capture("^(?<date>[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(\\.(?<fraction>[0-9]{1,9}))?(Z|\\+00:00)$")
        | {seconds: ((.date + "Z") | fromdateiso8601),
           nanos: (((.fraction // "") + "000000000")[0:9] | tonumber)};
    def difference_ms($later; $earlier):
        ($later.seconds - $earlier.seconds) * 1000 + ($later.nanos - $earlier.nanos) / 1000000;
'

finish_verdict() {
    if [[ "$(jq -r '.verdict' "${result}")" != pass ]]; then
        printf '%s verdict failed: %s\n' "${mode}" "$(jq -c '.failures' "${result}")" >&2
        exit 1
    fi
}

clock() {
    ((${#observers[@]} > 0)) || fail_usage 'at least one --observer is required'
    [[ "${period_ms}" =~ ^[0-9]+$ && "${period_ms}" -gt 0 ]] || fail_usage '--period-ms must be a positive integer'
    local events
    events="$(mktemp)"
    local observer
    for observer in "${observers[@]}"; do
        local host="${observer%%=*}"
        local file="${observer#*=}"
        [[ "${host}" =~ ^nervix-[0-9]+$ && -f "${file}" ]] \
            || fail_usage "an observer must be HOST=FILE with an existing file: ${observer}"
        jq -R -c --arg host "${host}" '
            capture("^(?<ns>[0-9]{19}) (?<text>.*)$") as $line
            | (($line.ns[0:16] | tonumber) / 1000) as $ms
            | $line.text as $text
            | (if ($text | startswith("attached to the clock of domain ")) then
                 ($text | capture("^attached to the clock of domain '"'"'(?<domain>[^'"'"']+)'"'"': generation (?<generation>[0-9]+), (?<clock>.*)$"))
                 | {kind: "attach", generation: (.generation | tonumber), clock}
               elif ($text | test("^\\[events\\] domain clock \\[[^]]+\\] tick: ")) then
                 ($text | capture("tick: generation (?<generation>[0-9]+), id (?<id>[0-9]+), boundary (?<boundary>[^,]+), authority UTC (?<authority>[^,]+), node logical (?<logical>.+)$"))
                 | {kind: "tick", generation: (.generation | tonumber), id: (.id | tonumber), boundary, authority, logical}
               elif ($text | test("^\\[events\\] domain clock \\[[^]]+\\]: generation ")) then
                 ($text | capture("\\]: generation (?<generation>[0-9]+), (?<clock>.*)$"))
                 | {kind: "state", generation: (.generation | tonumber), clock}
               elif ($text | test("notice: the session was interrupted")) then {kind: "interrupted"}
               elif ($text | test("notice: attaching the clock again failed")) then {kind: "restore_failed", text: $text}
               elif ($text | test("notice: the attachment ended")) then {kind: "ended", text: $text}
               elif ($text | startswith("observer: the domain-clock follow ended")) then {kind: "follow_ended"}
               else {kind: "other", text: $text} end)
            | . + {host: $host, ms: $ms}
        ' "${file}" >>"${events}"
    done
    jq -n --slurpfile events "${events}" --slurpfile rounds "${rounds}" \
        --arg fault "${fault}" --argjson period_ms "${period_ms}" --argjson rate "${rate}" \
        --argjson stall_ms "${stall_ms}" --argjson authority_bound_ms "${authority_bound_ms}" \
        --argjson replace_bound_ms "${replace_bound_ms}" \
        "${instant_definitions}"'
        ($events | sort_by(.ms)) as $all
        | ($rounds | map(. + {since_ms: (.fault_since_ns / 1000000), ended_ms: (.fault_ended_ns / 1000000),
                             recovered_ms: (.recovered_ns / 1000000)})) as $faults
        | [$all[] | select(.kind == "attach" or .kind == "state")] as $states
        | ($states | first) as $reference
        | if $reference == null then
            {verdict: "fail", failures: ["no observer attached to the clock"]}
          else
          ($reference.clock | capture("logical origin (?<origin>[^,]+), UTC anchor (?<anchor>[^,]+), time rate (?<rate>.+)$")) as $mapping
          | ($mapping.origin | instant) as $origin
          | [$states[] | select(.generation != $reference.generation or .clock != $reference.clock)] as $changed
          | [$all[] | select(.kind == "tick")] as $ticks
          | [$ticks[] | select(.generation != $reference.generation)] as $wrong_generation
          | [$ticks[] | (.boundary | instant) as $boundary
             | select($boundary.nanos != $origin.nanos
                      or ($boundary.seconds - $origin.seconds) * 1000 != (.id - 1) * $period_ms)] as $off_grid
          | [$ticks[] | difference_ms((.logical | instant); (.boundary | instant)) as $lead
             | select($lead < -10 or $lead > 4 * $period_ms * $rate) | . + {lead_ms: $lead}] as $misprojected
          # Each attach or state report begins a new attachment; tick ids increase within one.
          | [$all | group_by(.host)[]
             | reduce .[] as $event ({session: 0, last: null, decreasing: []};
                 if ($event.kind == "attach" or $event.kind == "state") then .session += 1 | .last = null
                 elif $event.kind == "tick" then
                   (if .last != null and $event.id <= .last.id
                    then .decreasing += [{host: $event.host, previous: .last.id, id: $event.id, ms: $event.ms}]
                    else . end) | .last = $event
                 else . end)
             | .decreasing[]] as $decreasing
          # Stalls: consecutive ticks on one observer further apart than the stall threshold.
          | [$ticks | group_by(.host)[] | sort_by(.ms) | . as $sequence
             | range(1; length) as $i
             | ($sequence[$i].ms - $sequence[$i - 1].ms) as $gap
             | select($gap > $stall_ms)
             | {host: $sequence[$i].host, start_ms: $sequence[$i - 1].ms, end_ms: $sequence[$i].ms, gap_ms: $gap}
             | . as $stall
             | . + {rounds: [$faults[] | select(.since_ms <= $stall.end_ms and .recovered_ms >= $stall.start_ms)
                             | {ordinal, kind, target}]}] as $stalls
          | [$stalls[] | select((.rounds | length) == 0)] as $unexplained
          # A stall on an observer whose own node the round faulted is that node'"'"'s outage; a stall
          # on a node that stayed up is the authority'"'"'s.
          | [$stalls[] | . as $stall
             | select(any($stall.rounds[]; .target == $stall.host or .target == "all") | not)] as $authority_stalls
          | [$authority_stalls[] | select(.gap_ms > $authority_bound_ms)] as $unrepaired
          | [$authority_stalls[] | . as $stall
             | ([$faults[] | . as $round | select(any($stall.rounds[]; .ordinal == $round.ordinal))
                 | .unavailable_ms | select(. != null)] | min) as $unavailable_ms
             | select($unavailable_ms != null and $stall.end_ms > $unavailable_ms + $replace_bound_ms)
             | . + {unavailable_ms: $unavailable_ms,
                    stalled_after_unavailable_ms: ($stall.end_ms - $unavailable_ms)}] as $not_replaced
          | ([$authority_stalls[].rounds[].ordinal] | unique) as $authority_rounds
          | [$faults[] | . as $round
             | ($all | map(.host) | unique)[] as $host
             | select(([$ticks[] | select(.host == $host and .ms >= $round.ended_ms)] | length) == 0)
             | {ordinal: $round.ordinal, host: $host}] as $silent_after
          | [($all | map(.host) | unique)[] as $host
             | select(([$ticks[] | select(.host == $host)] | length) == 0) | $host] as $never_ticked
          | (($fault | startswith("voter-")) and ($faults | length) > 0) as $rotation
          | [
              (if ($changed | length) > 0 then "a re-attached clock reported another generation or mapping" else empty end),
              (if ($wrong_generation | length) > 0 then "ticks arrived for another generation" else empty end),
              (if ($off_grid | length) > 0 then "tick boundaries left the period grid of the mapping" else empty end),
              (if ($misprojected | length) > 0 then "ticks were ahead of, or far behind, the serving node'"'"'s logical time" else empty end),
              (if ($decreasing | length) > 0 then "tick ids did not increase within an attachment" else empty end),
              (if ($unexplained | length) > 0 then "ticks stalled while no fault was held" else empty end),
              (if ($unrepaired | length) > 0 then "an authority stall outlasted the authority bound" else empty end),
              (if ($not_replaced | length) > 0
               then "no other voter took the clock over within 10 s after the surviving nodes reported the authority'"'"'s node unavailable"
               else empty end),
              (if ($silent_after | length) > 0 then "an observer saw no tick after a fault ended" else empty end),
              (if ($never_ticked | length) > 0 then "an observer never saw a tick" else empty end),
              (if $rotation and ($authority_rounds | length) == 0
               then "no round of the voter rotation stalled the ticks, so authority recovery is unverified" else empty end)
            ] as $failures
          | {
              verdict: (if ($failures | length) == 0 then "pass" else "fail" end),
              fault: $fault,
              generation: $reference.generation,
              mapping: $reference.clock,
              reports: ($states | length),
              ticks: ($ticks | length),
              stall_threshold_ms: $stall_ms,
              authority_bound_ms: $authority_bound_ms,
              replace_bound_ms: $replace_bound_ms,
              authority_rounds: $authority_rounds,
              authority_losses: [$authority_stalls[] | . + {logical_gap_ms: (.gap_ms * $rate)}],
              stalls: $stalls,
              changed_reports: $changed,
              wrong_generation: ($wrong_generation | length),
              off_grid: $off_grid,
              misprojected: $misprojected,
              decreasing: $decreasing,
              unexplained: $unexplained,
              unrepaired: $unrepaired,
              not_replaced: $not_replaced,
              silent_after: $silent_after,
              observers: ($all | group_by(.host) | map({key: .[0].host, value: {
                  reports: map(select(.kind == "attach" or .kind == "state")) | length,
                  ticks: map(select(.kind == "tick")) | length,
                  interruptions: map(select(.kind == "interrupted")) | length,
                  follows_ended: map(select(.kind == "follow_ended")) | length}}) | from_entries),
              failures: $failures
            }
          end' >"${result}"
    rm -f "${events}"
    finish_verdict
}

windows() {
    [[ -f "${output}" ]] || fail_usage "the paced output ledger is missing: ${output}"
    [[ -s "${clock}" ]] || fail_usage "the clock verdict is missing: ${clock}"
    [[ "${width_ms}" =~ ^[0-9]+$ && "${width_ms}" -gt 0 ]] || fail_usage '--width-ms must be a positive integer'
    jq -R -c 'capture("^(?<ms>[0-9]+) (?<window>\\{.*\\})$") | {ms: (.ms | tonumber), window: (.window | fromjson)}' \
        "${output}" >"${result}.windows"
    jq -n --slurpfile windows "${result}.windows" --slurpfile rounds "${rounds}" \
        --slurpfile clock "${clock}" --arg fault "${fault}" \
        --argjson width_ms "${width_ms}" --argjson rate "${rate}" --argjson late_bound_ms "${late_bound_ms}" \
        "${instant_definitions}"'
        ($rounds | map(. + {relocation_from_ms: (.relocation_started_ns / 1000000),
                             relocation_to_ms: (.relocation_completed_ns / 1000000 + 5000),
                             since_ms: (.fault_since_ns / 1000000), recovered_ms: (.recovered_ns / 1000000)})) as $faults
        # The graph moves before each round, and a graceful stop or a cluster restart takes the
        # graph'"'"'s own node down; windows emitted then may close late.
        | def disrupted($ms):
            any($faults[]; ($ms >= .relocation_from_ms and $ms <= .relocation_to_ms)
                           or ((.kind == "stop" or .kind == "cluster-restart") and $ms >= .since_ms and $ms <= .recovered_ms + 5000));
          [$windows[] | .window as $window
           | difference_ms(($window.closed_at | instant); ($window.opened_at | instant)) as $span
           | {ms, branch_name: $window.branch_name, records: $window.records,
              first_sequence: $window.first_sequence, last_sequence: $window.last_sequence,
              span_ms: $span, late_ms: ($span - $width_ms), disrupted: disrupted(.ms)}] as $closed
        | [$closed[] | select(.span_ms < $width_ms)] as $early
        | [$closed[] | select((.disrupted | not) and .late_ms > $late_bound_ms)] as $late
        | [$closed[] | select(.records < 1
                              or (.branch_name == "alpha" and ((.first_sequence % 2) != 0 or (.last_sequence % 2) != 0))
                              or (.branch_name == "beta" and ((.first_sequence % 2) != 1 or (.last_sequence % 2) != 1))
                              or (.branch_name != "alpha" and .branch_name != "beta"))] as $mixed
        | ($width_ms / $rate * 3) as $evidence_gap_ms
        # A stall whose round also took the graph'"'"'s own node down cannot show whether the
        # deadlines kept running: the graph was itself moving or restarting.
        | [$clock[0].authority_losses[] | select(.gap_ms >= $evidence_gap_ms) | . as $loss
           | . + {windows_closed: ([$closed[] | select(.ms >= $loss.start_ms and .ms <= $loss.end_ms)] | length),
                  graph_down: any($loss.rounds[]?; .kind == "stop" or .kind == "cluster-restart")}] as $losses
        | [$losses[] | select((.graph_down | not) and .windows_closed == 0)] as $stopped
        | [
            (if ($closed | length) == 0 then "no paced window closed" else empty end),
            (if ($early | length) > 0 then "windows closed before their logical width" else empty end),
            (if ($late | length) > 0 then "windows closed later than the lateness bound" else empty end),
            (if ($mixed | length) > 0 then "windows mixed rows of the two branches" else empty end),
            (if ($stopped | length) > 0 then "logical deadlines stopped while an authority stall withheld ticks" else empty end)
          ] as $failures
        | {
            verdict: (if ($failures | length) == 0 then "pass" else "fail" end),
            fault: $fault,
            windows: ($closed | length),
            width_logical_ms: $width_ms,
            width_physical_ms: ($width_ms / $rate),
            late_bound_logical_ms: $late_bound_ms,
            span_logical_ms: {min: ($closed | map(.span_ms) | min), max: ($closed | map(.span_ms) | max)},
            undisrupted_lateness_logical_ms: {max: ([$closed[] | select(.disrupted | not) | .late_ms] | max)},
            closed_during_authority_losses: $losses,
            early: $early,
            late: $late,
            mixed: $mixed,
            failures: $failures
          }' >"${result}"
    rm -f "${result}.windows"
    finish_verdict
}

case "${mode}" in
    clock) clock ;;
    windows) windows ;;
    -h | --help) usage; exit 0 ;;
    *) fail_usage "unknown verifier mode: ${mode}" ;;
esac
