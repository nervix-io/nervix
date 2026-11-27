# A public StateInspection answer, as the packaged CLI prints it with FORMAT JSON, for the
# inspection self-test. $spec names the family, the answer and its units:
#   {family, error?, rejection?, continuation?, units: [{unit, owner, replicas, origin: {node,
#    incarnation, stage}, rows: [{section, branch, fields}], wasm?: {total, failed, checkpoints}}]}
# An `error` is the object the CLI prints when the command fails without a typed envelope.
# A row's fields become its section's schema in key order: strings are STRING cells and numbers
# I64 cells.
def entity:
    {relay: {kind: "Relay", name: "chaos_profiles"},
     deduplicator: {kind: "Deduplicator", name: "chaos_dedup"},
     window: {kind: "WindowProcessor", name: "chaos_window"},
     reorderer: {kind: "Reorderer", name: "chaos_reorderer"},
     correlator: {kind: "Correlator", name: "chaos_correlator"},
     wasm: {kind: "WasmProcessor", name: "chaos_counter"},
     inferencer: {kind: "Inferencer", name: "chaos_running"}}[$spec.family]
    | {domain: "chaos_baseline"} + .;
def cell: if type == "string" then {type: "string", value: .} else {type: "i64", value: .} end;
def field($name; $value): {name: $name, ty: (if ($value | type) == "string" then "String" else "I64" end),
                           optional: false, sensitive: false};
def unit_value: if . == "single" then "Single" else {Partition: (ltrimstr("partition-") | tonumber)} end;
def section($name; $rows):
    ($rows[0].fields | to_entries) as $schema
    | {name: $name,
       schema: {fields: [$schema[] | field(.key; .value)],
                branch: {name: "chaos_state_branch", fields: [field("branch_name"; "alpha")]}},
       frames: [$rows | group_by(.branch)[] | {branch_key: [(.[0].branch | cell)],
                rows: [.[] | [.fields | to_entries[] | .value | cell]]}],
       omitted_rows: 0, watermarks: null};
def unit_answer:
    (.rows // []) as $rows
    | {unit: (.unit | unit_value), owner, replicas,
       origin: {node: {node_id: .origin.node, incarnation: .origin.incarnation},
                fingerprint: [range(0; 32) | 7], generation: 3, revision: 17, stage: .origin.stage},
       counts: {branches: ([$rows[].branch] | unique | length), rows: ($rows | length), bytes: (($rows | length) * 64)},
       sections: [$rows | group_by(.section)[] | section(.[0].section; .)]}
    + (if .wasm == null then {} else
         {wasm: {default_generation: 1, reset: null, entries: (.wasm.checkpoints | length),
                 checkpoints: {total: .wasm.total, failed: .wasm.failed},
                 recoveries: {total: 0},
                 branches: [.wasm.checkpoints[] | {branch: [range(0; 32) | 9],
                            checkpoint: {generation: 1, committed_revision: ., latest_revision: .,
                                         stage: "replica_confirmed", required_replicas: 1,
                                         confirmed_replicas: 1}}]}} end);

if $spec.error != null then {error: $spec.error}
else {scope: "domain_node", entity: entity}
+ if $spec.rejection != null then {outcome: "rejected", rejection: $spec.rejection}
  else
    [$spec.units[] | unit_answer] as $units
    | {outcome: "page",
       counts: {branches: ([$units[].counts.branches] | add // 0), rows: ([$units[].counts.rows] | add // 0),
                bytes: ([$units[].counts.bytes] | add // 0)},
       unit_count: ($units | length), units: $units, continuation: ($spec.continuation // null)}
  end
end
