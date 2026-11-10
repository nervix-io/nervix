# Current public StateInspection JSON for the two profile fixtures. Empty partition units remain
# present, just as the coordinator reports them. All field order and typed cells are explicit.
def field($name; $ty): {name: $name, ty: $ty, optional: false, sensitive: false};
def cell($type; $value): {type: $type, value: $value};
def counts($rows): {branches: $rows, rows: $rows, bytes: ($rows * 64)};
def profile($branch):
    [cell("string"; $branch), cell("i64"; 1)]
    + (if $lifetime then [cell("string"; "\($branch)-v1"), cell("datetime"; 1794183779000000001)] else [] end)
    + [cell("datetime"; 1794183779000000001), cell("datetime"; 1794183779000000002)];
def schema:
    {fields: ([field("branch_name"; "String"), field("profile_version"; "I64")]
              + (if $lifetime then [field("label"; "String"), field("updated_at"; "Datetime")] else [] end)
              + [field("ingested_at_low_watermark"; "Datetime"), field("ingested_at_high_watermark"; "Datetime")]),
     branch: {name: (if $lifetime then "chaos_life_branch" else "chaos_state_branch" end),
              fields: [field("branch_name"; "String")]}};

{entity: {domain: "chaos_baseline", kind: "Relay", name: $relay}, outcome: "page",
 counts: counts($branches | length), unit_count: $partitions, continuation: null,
 units: [range(0; $partitions) as $unit
    | [$branches | to_entries[]
       | select(if $partitions == 1 then true else (.key * 2) % $partitions == $unit end)
       | .value] as $selected
    | {unit: (if $partitions == 1 then "Single" else {Partition: $unit} end),
       owner: "node-1", replicas: ["node-2"],
       origin: {node: {node_id: "node-1", incarnation: 1}, fingerprint: [range(0;32) | 37],
                generation: 4, revision: 19, stage: "live"},
       counts: counts($selected | length),
       sections: (if ($selected | length) == 0 then [] else
           [{name: "records", schema: schema, omitted_rows: 0,
             watermarks: {low: (if $lifetime then 4 else 2 end), high: (if $lifetime then 5 else 3 end)},
             frames: [$selected[] | {branch_key: [cell("string"; .)], rows: [profile(.)]}]}]
           end)}]}
