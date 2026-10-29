# Endpoints chaos input. Records are numbered from 1, and a record's sequence is its number, so a
# record the client holds after is also the phase boundary the verifier reads. `$kind` is `http` for
# the records the client posts and `frame` for those it sends as WebSocket frames, which keeps every
# event identity of a run distinct.
range(1; $count + 1) as $sequence
| {
    event_id: ($run_id + "-" + $kind + "-" + ($sequence | tostring)),
    sequence: $sequence,
    content: ("chaos-endpoint-" + $kind + "-payload-" + ($sequence | tostring))
  }
