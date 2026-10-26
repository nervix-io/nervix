# Stateful chaos input. Two concrete branches interleave strictly: even sequences belong to alpha
# and odd sequences to beta, so after 2n records each branch has n. branch_index counts a record's
# position within its branch from 1.
#
# Every fourth record of a branch repeats the deduplication key of an earlier record of the same
# branch, which itself introduced a fresh key: at index 12n + 8 the key from 3 positions back, at
# 12n + 4 past index 23 the key from 23 back, and at 12n past index 79 the key from 79 back. Short
# lags exercise keys seen just before a fault or after recovery; long lags bring keys durable at the
# milestone back after recovery. Both branches name keys after the same indexes, so equal keys in
# different branches must never deduplicate each other.
def branch_name($sequence): ["alpha", "beta"][$sequence % 2];
def branch_index($sequence): ($sequence - ($sequence % 2)) / 2 + 1;
def key_index($index):
  if $index % 12 == 8 then $index - 3
  elif $index % 12 == 4 and $index > 23 then $index - 23
  elif $index % 12 == 0 and $index > 79 then $index - 79
  else $index
  end;

range(0; $count) as $sequence
| branch_index($sequence) as $index
| {
    event_id: ($run_id + "-state-" + ($sequence | tostring)),
    branch_name: branch_name($sequence),
    sequence: $sequence,
    branch_index: $index,
    dedup_key: ("key-" + (key_index($index) | tostring)),
    content: ("chaos-state-payload-" + ($sequence | tostring))
  }
