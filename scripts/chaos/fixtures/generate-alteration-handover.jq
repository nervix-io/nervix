# Alteration-handover chaos input of one source: one record per line, each one Kafka payload.
# $branches selects how the records spread over branches:
#
#   "distinct"     every record opens a branch of its own, so the deduplicator holds one branch
#                  per backlog record when the alteration replaces it
#   "interleaved"  records alternate between the branches alpha and beta
#
# Sequences run on from $first for $count records, so the batches of a source never share a
# sequence, and every record names its source. The content is unique as well as the event id, so
# the deduplicator keys of both sides of the alteration let every record through.
def branch_name($sequence):
    if $branches == "distinct" then $source + "-branch-" + ($sequence | tostring)
    else ["alpha", "beta"][$sequence % 2] end;

range($first; $first + $count) as $sequence
| {
    event_id: ($run_id + "-" + $source + "-" + ($sequence | tostring)),
    source: $source,
    branch_name: branch_name($sequence),
    sequence: $sequence,
    content: ("chaos-alter-" + $source + "-payload-" + ($sequence | tostring))
  }
