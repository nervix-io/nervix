# Input of the stateful inspection graph, in the stateful record shape. The two branches alpha and
# beta interleave, and each branch's sequences arrive out of order. Before the fault, the `held`
# batch gives each branch sequences 3, 1, 4 and 2; after recovery, the `recovered` batch gives
# each branch 6 and 5. The reorderer orders by sequence and the correlator matches a sequence that
# never arrives on its right side, so both hold every row of a batch for the whole run.
def sequences: if $batch == "held" then [3, 1, 4, 2] elif $batch == "recovered" then [6, 5]
               else error("unknown inspection batch \($batch)") end;

sequences[] as $sequence
| ["alpha", "beta"][] as $branch
| {
    event_id: ($run_id + "-inspect-" + $branch + "-" + ($sequence | tostring)),
    branch_name: $branch,
    sequence: $sequence,
    branch_index: $sequence,
    dedup_key: ("inspect-" + ($sequence | tostring)),
    content: ("chaos-inspect-" + $batch + "-" + ($sequence | tostring))
  }
