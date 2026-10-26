# Paced chaos input. Two concrete branches interleave strictly: even sequences belong to alpha and
# odd sequences to beta, so a window of either branch holds only sequences of one parity.
range(0; $count) as $sequence
| {
    event_id: ($run_id + "-paced-" + ($sequence | tostring)),
    branch_name: ["alpha", "beta"][$sequence % 2],
    sequence: $sequence
  }
