def branch_name($sequence):
  ($ARGS.named.branches // 3) as $branches
  | if $branches == 3 then ["alpha", "beta", "gamma"][$sequence % 3]
    else "branch-\($sequence % $branches)" end;

range(0; $count) as $sequence
| {
    event_id: ($run_id + "-" + ($sequence | tostring)),
    branch_name: branch_name($sequence),
    sequence: $sequence,
    content: ("chaos-baseline-payload-" + ($sequence | tostring))
  }
