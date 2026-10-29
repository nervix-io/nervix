# Lifetimes chaos input. Records are numbered from 1 and a record's sequence is its number. With
# `$kind` `life`, four concrete branches interleave strictly, red, blue, gold and green in turn, so
# after 4n records each branch has n; branch_index counts a record's position within its branch
# from 1. Every other kind names one branch for all its records: `expiring` the green branch of
# the expiring relay, `late` and `flush` their unbranched pipelines. `$first` is the first number,
# so one kind's records produced in several runs of this generator stay distinct.
def branch_name($sequence):
  if $kind == "life" then ["red", "blue", "gold", "green"][($sequence - 1) % 4]
  elif $kind == "expiring" then "green"
  else $kind
  end;
def branch_index($sequence):
  if $kind == "life" then (($sequence - 1) - (($sequence - 1) % 4)) / 4 + 1
  else $sequence
  end;

range($first; $first + $count) as $sequence
| {
    event_id: ($run_id + "-" + $kind + "-" + ($sequence | tostring)),
    branch_name: branch_name($sequence),
    sequence: $sequence,
    branch_index: branch_index($sequence),
    content: ("chaos-" + $kind + "-payload-" + ($sequence | tostring))
  }
