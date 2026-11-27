# Normalizes one public state-inspection answer into the observation that
# verify-inspection-evidence.sh judges. $document holds the first JSON document of the packaged
# CLI's transcript, or nothing when the transcript held none. A typed page and a typed refusal are
# both observations of the product; only a transcript without the typed envelope is unreadable.
def unit_label:
    if . == "Single" then "single"
    elif type == "object" and (.Partition | type) == "number" then "partition-\(.Partition)"
    else error("unknown execution unit \(tojson)") end;
def cell_value:
    if type != "object" then .
    elif .type == "null" then null
    elif .type == "redacted" then "<redacted>"
    elif has("value") then .value
    else . end;
def branch_name:
    if type == "array" and length > 0 then (.[0] | cell_value) else null end;
def checkpoint_facts:
    {branch: (.branch // null), generation: (.checkpoint.generation // null),
     committed_revision: (.checkpoint.committed_revision // null),
     latest_revision: (.checkpoint.latest_revision // null), stage: (.checkpoint.stage // null)};
def unit_observation:
    {unit: (.unit | unit_label), owner, replicas,
     origin: {node: .origin.node.node_id, incarnation: .origin.node.incarnation,
              stage: .origin.stage, generation: .origin.generation, revision: .origin.revision},
     counts,
     sections: [.sections[] | {name, omitted_rows, rows: ([.frames[].rows | length] | add // 0)}],
     wasm: (if .wasm == null then null else
              {total: .wasm.checkpoints.total, failed: .wasm.checkpoints.failed,
               checkpoints: [.wasm.branches[] | select(.checkpoint != null) | checkpoint_facts]} end)};
def unit_rows:
    (.unit | unit_label) as $unit
    | .sections[] as $section
    | ($section.schema.fields | map(.name)) as $names
    | $section.frames[] as $frame
    | $frame.rows[] as $row
    | {unit: $unit, section: $section.name, branch: ($frame.branch_key | branch_name),
       fields: ([range(0; $names | length) as $index
                 | {key: $names[$index], value: ($row[$index] | cell_value)}] | from_entries)};

($document | first // null) as $answer
| {family: $family, phase: $phase, host: $host, statement: $statement,
   started_ms: ($started | tonumber), finished_ms: ($finished | tonumber),
   cli_status: ($status | tonumber)}
+ if ($answer | type) != "object" then
    {status: "unreadable", error: "the transcript holds no JSON document"}
  elif ($answer | has("outcome") | not) then
    {status: "unreadable",
     error: ((($answer.error.code // "unknown") | tostring) + ": "
             + (($answer.error.message // "no message") | tostring))}
  elif $answer.outcome == "rejected" then
    {status: "rejected", rejection: $answer.rejection, entity: $answer.entity}
  elif $answer.outcome == "page" then
    {status: "page", rejection: null, entity: $answer.entity, counts: $answer.counts,
     unit_count: $answer.unit_count, continuation: $answer.continuation,
     units: [$answer.units[] | unit_observation],
     rows: [$answer.units[] | unit_rows]}
  else
    {status: "unreadable", error: "unknown inspection outcome \($answer.outcome | tojson)"}
  end
