#!/usr/bin/env bash
set -euo pipefail

# Judges the state inspection evidence of a stateful run with --inspect-state. The evidence
# directory holds context.json and, for each of the milestone, the outage, the failover and the
# recovery, the observations state-inspection-observation.jq wrote under <phase>/reads; the
# milestone, failover and recovery also hold the DESCRIBE placement (placement.json) and the live
# incarnations DESCRIBE CLUSTER named (cluster.json) at that time.
#
# Every page must agree with itself and name, for each execution unit, the owner DESCRIBE names, as
# the node that served it in that node's live incarnation, with the owner outside its replicas.
# Running processor state reads as live; a materialized relay reads live or stored. A read of the
# outage is refused as owner_unavailable, not_ready or deadline, or is answered by a live owner,
# never by the killed one. After the fault, a relay profile, a deduplicator key and a guest
# checkpoint the milestone made durable are still reported, the reorderer and the correlator hold
# nothing their input never carried, the rows produced after recovery are held, and the milestone's
# continuation is refused as stale.
#
# Exit status: 0 when the evidence passes, 1 when it fails a check (the result lists every failure),
# 2 when the evidence is incomplete or unreadable.

usage() {
    printf 'usage: verify-inspection-evidence.sh --evidence DIRECTORY --result FILE\n' >&2
}

unusable() {
    printf 'inspection evidence is unusable: %s\n' "$*" >&2
    exit 2
}

evidence=""
result=""
while (($# > 0)); do
    case "$1" in
        --evidence)
            [[ $# -ge 2 ]] || { usage; exit 2; }
            evidence="$2"
            shift 2
            ;;
        --result)
            [[ $# -ge 2 ]] || { usage; exit 2; }
            result="$2"
            shift 2
            ;;
        *)
            usage
            exit 2
            ;;
    esac
done
[[ -n "${evidence}" && -n "${result}" ]] || { usage; exit 2; }

for required in context.json milestone/placement.json milestone/cluster.json failover/placement.json \
    failover/cluster.json recovered/placement.json recovered/cluster.json; do
    [[ -s "${evidence}/${required}" ]] || unusable "missing ${required}"
done

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# Writes a JSON array of the observations under DIRECTORY to OUTPUT, leaving out the raw answers
# kept beside them.
collect_observations() {
    local dir="$1"
    local output="$2"
    local files=()
    local file
    if [[ -d "${dir}" ]]; then
        for file in "${dir}"/*.json; do
            [[ -e "${file}" && "${file}" != *.document.json ]] || continue
            files+=("${file}")
        done
    fi
    if ((${#files[@]} == 0)); then
        printf '[]\n' >"${output}"
    else
        jq -s '.' "${files[@]}" >"${output}"
    fi
}

for phase_dir in milestone outage failover recovered; do
    collect_observations "${evidence}/${phase_dir}/reads" "${work}/${phase_dir}.json" \
        || unusable "the ${phase_dir} observations could not be read"
done

jq -n \
    --slurpfile context "${evidence}/context.json" \
    --slurpfile milestone "${work}/milestone.json" \
    --slurpfile outage "${work}/outage.json" \
    --slurpfile failover "${work}/failover.json" \
    --slurpfile recovered "${work}/recovered.json" \
    --slurpfile milestone_placement "${evidence}/milestone/placement.json" \
    --slurpfile failover_placement "${evidence}/failover/placement.json" \
    --slurpfile recovered_placement "${evidence}/recovered/placement.json" \
    --slurpfile milestone_incarnations "${evidence}/milestone/cluster.json" \
    --slurpfile failover_incarnations "${evidence}/failover/cluster.json" \
    --slurpfile recovered_incarnations "${evidence}/recovered/cluster.json" \
    '
    def families: ["relay", "deduplicator", "window", "reorderer", "correlator", "wasm", "inferencer"];
    def entity_name:
        {relay: "RELAY chaos_profiles", deduplicator: "DEDUPLICATOR chaos_dedup",
         window: "WINDOW PROCESSOR chaos_window", reorderer: "REORDERER chaos_reorderer",
         correlator: "CORRELATOR chaos_correlator", wasm: "WASM PROCESSOR chaos_counter",
         inferencer: "INFERENCER chaos_running"}[.];
    def placed_owner($placement; $family; $unit):
        ($placement[$family | entity_name]) as $entity
        | if $entity == null then null
          elif $unit == "single" then
              (if ($entity.units | length) == 0 then $entity.owner else null end)
          else
              ($unit | ltrimstr("partition-") | tonumber) as $index
              | ([$entity.units[] | select(.unit == $index) | .owner] | first // null)
          end;
    def read_label: "\(.phase) read of \(.family)";
    def allowed_refusal: . == "owner_unavailable" or . == "not_ready" or . == "deadline";
    def pairs($section): [.rows[] | select(.section == $section) | {branch, sequence: .fields.sequence}] | unique;
    def batch_pairs: [.[] | {branch: .branch_name, sequence}] | unique;
    def pair_text: map("\(.branch)/\(.sequence)") | join(", ");
    def outcome_text:
        if .status == "page" then "a page"
        else .status + (if .rejection != null then " as " + .rejection else "" end)
             + (if .error != null then ": " + .error else "" end) end;

    # A page agrees with itself: its units and counts match, every branch is one the graph created,
    # and nothing it selected was left unread.
    def page_failures:
        read_label as $label
        | if .status != "page" then ["the \($label) was \(outcome_text)"]
          else
            [ (if .unit_count != (.units | length)
               then "the \($label) lists \(.units | length) units for a unit count of \(.unit_count)" else empty end),
              (if (.units | map(.unit) | unique | length) != (.units | length)
               then "the \($label) lists an execution unit twice" else empty end),
              (if ([.units[].counts.rows] | add // 0) != .counts.rows
                  or ([.units[].counts.branches] | add // 0) != .counts.branches
                  or ([.units[].counts.bytes] | add // 0) != .counts.bytes
               then "the \($label) has counts that differ from the sum of its units" else empty end),
              (if any(.rows[]; .branch != null and .branch != "alpha" and .branch != "beta")
               then "the \($label) reports a branch the graph never created" else empty end),
              (if .continuation != null or any(.units[].sections[]; .omitted_rows != 0)
               then "the \($label) left selected state unread" else empty end) ]
          end;

    # Each unit is served by the owner DESCRIBE names, in that node`s live incarnation, with its owner
    # outside its replicas, never by KILLED, and with a stage that describes what it read.
    def owner_failures($placement; $incarnations; $killed):
        . as $observation | read_label as $label
        | [ .units[] as $unit
            | placed_owner($placement; $observation.family; $unit.unit) as $placed
            | (if $placed == null then "the \($label) unit \($unit.unit) has no owner in DESCRIBE"
               elif $unit.owner != $placed
               then "the \($label) unit \($unit.unit) names \($unit.owner) as its owner where DESCRIBE names \($placed)"
               else empty end),
              (if $unit.origin.node != $unit.owner
               then "the \($label) unit \($unit.unit) was served by \($unit.origin.node), not by its owner \($unit.owner)"
               else empty end),
              (if ($incarnations[$unit.origin.node] // null) != $unit.origin.incarnation
               then "the \($label) unit \($unit.unit) names incarnation \($unit.origin.incarnation) of \($unit.origin.node), whose live incarnation is \($incarnations[$unit.origin.node] // "unknown")"
               else empty end),
              (if any($unit.replicas[]; . == $unit.owner)
               then "the \($label) unit \($unit.unit) lists its owner among its replicas" else empty end),
              (if $killed != null and ($unit.owner == $killed or $unit.origin.node == $killed)
               then "the \($label) unit \($unit.unit) names the killed node \($killed)" else empty end),
              (if $observation.family == "relay" then
                   (if $unit.origin.stage == "live" or $unit.origin.stage == "stored" then empty
                    else "the \($label) unit \($unit.unit) has the read stage \($unit.origin.stage | tojson)" end)
               elif $unit.origin.stage == "live" then empty
               else "the \($label) unit \($unit.unit) reports a \($unit.origin.stage | tojson) read of running state" end) ];

    def profile_failures($version):
        read_label as $label | . as $observation
        | [ ("alpha", "beta") as $branch
            | if any($observation.rows[]; .section == "records" and .branch == $branch
                      and .fields.branch_name == $branch
                      and (.fields.profile_version | type) == "number" and .fields.profile_version >= $version)
              then empty else "the \($label) holds no profile of version \($version) or later for \($branch)" end ];
    def dedup_keys: [.rows[] | select(.section == "keys") | {branch, key: .fields.key_1}] | unique;
    def durable_key_failures($milestone):
        read_label as $label
        | ((($milestone | dedup_keys) - dedup_keys)) as $lost
        | if ($lost | length) == 0 then []
          else ["the \($label) no longer remembers \($lost | length) keys durable at the milestone, such as \($lost[0].branch)/\($lost[0].key)"] end;
    def branch_failures($section; $what):
        read_label as $label | . as $observation
        | [ ("alpha", "beta") as $branch
            | if any($observation.rows[]; ($section == null or .section == $section) and .branch == $branch)
              then empty else "the \($label) reports no \($what) of \($branch)" end ];
    def guest_failures:
        read_label as $label
        | ([.units[] | (.wasm.checkpoints // [])[]] | length) as $count
        | [ (if $count != 2 then "the \($label) lists \($count) guest checkpoints instead of one for each branch" else empty end),
            (if any(.units[]; (.wasm.failed // 0) != 0) then "the \($label) reports a failed guest checkpoint" else empty end) ];
    # A reorderer section or a correlator side holds all of REQUIRED and nothing outside ALLOWED.
    def held_failures($section; $required; $allowed):
        read_label as $label | pairs($section) as $held
        | (($required | batch_pairs) - $held) as $missing
        | ($held - ($allowed | batch_pairs)) as $foreign
        | [ (if ($missing | length) > 0 then "the \($label) does not hold \($missing | pair_text) in \($section)" else empty end),
            (if ($foreign | length) > 0 then "the \($label) holds \($foreign | pair_text) in \($section), which its input never carried" else empty end) ];

    # What a page must report of the graph`s state. At the milestone the state is known exactly; after
    # the fault, what the milestone made durable must remain and the held rows must come from the input.
    def content_failures($context; $milestone_reads; $stage):
        ($context.batches.held) as $held
        | ($context.batches.held + $context.batches.recovered) as $carried
        | if .status != "page" then []
          elif .family == "relay" then profile_failures($context.profile_milestone)
          elif .family == "deduplicator" then
              if $stage == "milestone" then branch_failures("keys"; "remembered key")
              else durable_key_failures($milestone_reads.deduplicator) end
          elif .family == "window" then
              if $stage == "milestone" then branch_failures("rows"; "retained row") else [] end
          elif .family == "reorderer" then
              if $stage == "milestone" then held_failures("pending"; $held; $held)
              elif $stage == "failover" then held_failures("pending"; []; $held)
              else held_failures("pending"; $context.batches.recovered; $carried) end
          elif .family == "correlator" then
              (if $stage == "milestone" then held_failures("left"; $held; $held)
               elif $stage == "failover" then held_failures("left"; []; $held)
               else held_failures("left"; $context.batches.recovered; $carried) end)
              + (if any(.rows[]; .section == "right") then ["the \(read_label) holds a right side that was never sent"] else [] end)
          elif .family == "wasm" then guest_failures
          elif .family == "inferencer" then
              if $stage == "milestone" then branch_failures(null; "carried state") else [] end
          else [] end;

    def reads_of($observations; $family; $phase):
        [$observations[] | select(.family == $family and .phase == $phase)] | sort_by(.started_ms);

    $context[0] as $context
    | $milestone[0] as $milestone_observations
    | (reduce families[] as $family ({}; .[$family] = (reads_of($milestone_observations; $family; "milestone") | first)))
        as $milestone_reads
    | (reads_of($milestone_observations; "deduplicator"; "continuation") | first) as $first_page
    | (reads_of($recovered[0]; "deduplicator"; "stale") | first) as $stale
    | [ (families[] as $family
          | (reads_of($milestone_observations; $family; "milestone") | length) as $count
          | if $count != 1 then "\($count) milestone reads of \($family)" else empty end),
        (families[] as $family
          | (reads_of($outage[0]; $family; "outage") | length) as $count
          | if $count != 1 then "\($count) outage reads of \($family)" else empty end),
        (families[] as $family
          | if (reads_of($failover[0]; $family; "failover") | length) == 0 then "no failover read of \($family)" else empty end),
        (families[] as $family
          | (reads_of($recovered[0]; $family; "recovered") | length) as $count
          | if $count != 1 then "\($count) recovery reads of \($family)" else empty end),
        (if $first_page == null then "no first page of the milestone deduplicator keys" else empty end) ]
      as $missing
    | if ($missing | length) > 0 then {verdict: "unusable", missing: $missing, failures: []}
      else
        [ # The milestone: the owner the run placed the state on serves exact, known state.
          (families[] as $family
           | $milestone_reads[$family]
           | page_failures
             + (if .status == "page" then
                   owner_failures($milestone_placement[0]; $milestone_incarnations[0]; null)
                   + [ .units[] | select(.owner != $context.state_owner)
                       | "the milestone read of \($family) unit \(.unit) names \(.owner), not the state owner \($context.state_owner)" ]
                   + content_failures($context; $milestone_reads; "milestone")
                else [] end)
           | .[]),
          (if $first_page.status != "page" or $first_page.continuation == null
           then "the first page of two deduplicator keys at the milestone carried no continuation" else empty end),
          # The outage: a typed refusal, or a page a live owner answered.
          (families[] as $family
           | reads_of($outage[0]; $family; "outage")[0]
           | if .status == "rejected" then
                 (if (.rejection | allowed_refusal) then empty
                  else "the outage read of \($family) was refused as \(.rejection), not as owner_unavailable, not_ready or deadline" end)
             elif .status == "page" then
                 (page_failures + owner_failures($failover_placement[0]; $failover_incarnations[0]; $context.killed)) | .[]
             else "the outage read of \($family) was \(outcome_text)" end),
          # The failover: refusals until the survivors serve the state, then the new owner names itself.
          (families[] as $family
           | reads_of($failover[0]; $family; "failover") as $attempts
           | ($attempts[-1]) as $final
           | ([$attempts[:-1][] | select(.status != "rejected" or (.rejection | allowed_refusal | not))
               | "a failover read of \($family) before its page was \(outcome_text)"] | .[]),
             ($final
              | page_failures
                + (if .status == "page" then
                      owner_failures($failover_placement[0]; $failover_incarnations[0]; $context.killed)
                      + content_failures($context; $milestone_reads; "failover")
                   else [] end)
              | .[])),
          # The recovery: the current owner names itself and still reports the durable state.
          (families[] as $family
           | reads_of($recovered[0]; $family; "recovered")[0]
           | page_failures
             + (if .status == "page" then
                   owner_failures($recovered_placement[0]; $recovered_incarnations[0]; null)
                   + content_failures($context; $milestone_reads; "recovered")
                else [] end)
           | .[]),
          (if $stale == null then "the milestone continuation was not resumed after recovery"
           elif $stale.status != "rejected" or $stale.rejection != "stale_continuation"
           then "the milestone continuation resumed after recovery was \($stale | outcome_text), not refused as stale_continuation"
           else empty end) ]
        as $failures
        | {verdict: (if ($failures | length) == 0 then "pass" else "fail" end),
           state_owner: $context.state_owner,
           killed: $context.killed,
           owners: {
             milestone: (reduce families[] as $family ({}; .[$family] = ([$milestone_reads[$family].units[]?.owner] | unique))),
             failover: (reduce families[] as $family ({};
                 .[$family] = ([reads_of($failover[0]; $family; "failover")[-1].units[]?.owner] | unique))),
             recovered: (reduce families[] as $family ({};
                 .[$family] = ([reads_of($recovered[0]; $family; "recovered")[0].units[]?.owner] | unique)))},
           stages: {
             milestone: (reduce families[] as $family ({}; .[$family] = ([$milestone_reads[$family].units[]?.origin.stage] | unique))),
             failover: (reduce families[] as $family ({};
                 .[$family] = ([reads_of($failover[0]; $family; "failover")[-1].units[]?.origin.stage] | unique))),
             recovered: (reduce families[] as $family ({};
                 .[$family] = ([reads_of($recovered[0]; $family; "recovered")[0].units[]?.origin.stage] | unique)))},
           outage: (reduce families[] as $family ({};
               .[$family] = (reads_of($outage[0]; $family; "outage")[0] | {status, rejection}))),
           failover_refusals: (reduce families[] as $family ({};
               .[$family] = [reads_of($failover[0]; $family; "failover")[:-1][] | .rejection // .status])),
           stale_continuation: ($stale | {status, rejection}),
           failures: $failures}
      end
    ' >"${result}" || unusable "the verdict could not be computed from the evidence"

verdict="$(jq -r '.verdict' "${result}")"
case "${verdict}" in
    pass) exit 0 ;;
    fail)
        jq -r '.failures[]' "${result}" >&2
        exit 1
        ;;
    *)
        jq -r '.missing[]' "${result}" >&2
        exit 2
        ;;
esac
