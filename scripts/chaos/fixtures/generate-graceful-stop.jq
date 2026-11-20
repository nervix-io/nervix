# Graceful-stop chaos input of one source: one Kafka payload, a JSON array of the records of every
# lane in $lanes, which the ingestor's JAQ codec unfolds. $lanes lists {lane, records} in order, and
# sequences run on from $first across the lanes, so the payloads of a source never share a sequence.
# Records alternate between the branches alpha and beta, so every lane holds both, and every record
# names its source and lane, which select the buffer it rests in.
#
# One payload carries every lane, so an acknowledged source confirms its position only once every
# lane of it has been delivered: a lane delivered while the others rest is confirmed with them.
def branch_name($sequence): ["alpha", "beta"][$sequence % 2];

reduce $lanes[] as $lane ({next: $first, records: []};
    .next as $start
    | .records += [range($start; $start + $lane.records) as $sequence
        | {
            event_id: ($run_id + "-" + $source + "-" + ($sequence | tostring)),
            source: $source,
            lane: $lane.lane,
            branch_name: branch_name($sequence),
            sequence: $sequence,
            content: ("chaos-stop-" + $source + "-" + $lane.lane + "-payload-" + ($sequence | tostring))
          }]
    | .next = $start + $lane.records)
| .records
