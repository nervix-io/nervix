def branch_name($sequence):
  ["alpha", "beta", "gamma"][$sequence % 3];

range(0; $count) as $sequence
| {
    event_id: ($run_id + "-" + ($sequence | tostring)),
    branch_name: branch_name($sequence),
    sequence: $sequence,
    content: ("chaos-baseline-payload-" + ($sequence | tostring))
  }
