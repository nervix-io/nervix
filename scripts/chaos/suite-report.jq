# Renders the Markdown summary of one or more chaos suite records (suite.json), such as every shard
# of a suite, for the console, summary.md and a CI job summary. Input: the records, slurped.

def duration:
  if . == null then "—"
  else (. / 60 | floor) as $minutes | (. % 60) as $seconds
       | if $minutes > 0 then "\($minutes) min \($seconds) s" else "\($seconds) s" end
  end;

def seconds_from_ms: (. / 100 | round) / 10 | "\(.) s";

def cell:
  if . == null then "—" else tostring | gsub("\\|"; "\\|") | gsub("\n"; " ") end;

def code: "`" + (tostring | gsub("`"; "'")) + "`";

def counted($singular; $plural): "\(.) \(if . == 1 then $singular else $plural end)";

def ledger_cell:
  if .ledger == null then "—"
  else "\(.ledger.expected | cell) / \(.ledger.observed | cell) / \(.ledger.duplicates | cell) / \(.ledger.missing | cell)"
  end;

def recovery_label:
  {election_ms: "election",
   placement_ms: "placement",
   listener_recovery_ms: "listeners back",
   settled_recovery_ms: "settled",
   delivery_resume_ms: "delivery resumed",
   settled_after_heal_max_ms: "slowest settle after a heal",
   output_advanced_after_heal_max_ms: "slowest output after a heal",
   backlog_drained_after_heal_max_ms: "slowest drain after a heal"}[.] // .;

def metrics_line:
  [ (.recovery // {} | to_entries[] | "\(.key | recovery_label) \(.value | seconds_from_ms)"),
    (.resources // {} | (if .max_node_memory_bytes != null
                          then "peak node memory \(.max_node_memory_bytes / 1000000 | round) MB" else empty end),
                         (if .max_backlog != null then "largest backlog \(.max_backlog) records" else empty end),
                         (if .longest_output_stall_ms != null
                          then "longest output stall \(.longest_output_stall_ms | seconds_from_ms)" else empty end)) ]
  | join(", ");

def worker_line:
  if .worker == null then empty
  else "Worker: \(.worker.kernel), Docker \(.worker.docker), Compose \(.worker.compose), \(.worker.cpus) CPUs, \(.worker.memory_bytes / 1073741824 * 10 | round / 10) GiB"
  end;

def title: "Chaos \(.suite)\(if .shard == null then "" else " shard \(.shard)" end): \(.status)";

def section($heading):
  . as $suite
  | "\($heading) \(title)",
    "",
    ("Image " + (.image.requested | code)
     + (if .image.id then " resolved to " + (.image.id | code) else "" end)
     + (if (.image.repo_digests // "") != "" and .image.repo_digests != .image.requested
        then " (" + (.image.repo_digests | code) + ")" else "" end)
     + " · suite " + (.suite_id | code)
     + " · " + (.entries | length | counted("entry"; "entries"))
     + (if .duration_seconds != null then " in \(.duration_seconds | duration)" else "" end)),
    (worker_line | "", .),
    (if .error then "", "**\(.error.category | ascii_upcase) failure:** \(.error.message)" else empty end),
    "",
    "| Entry | Verdict | Exit | Category | Final phase | Duration | Expected / observed / duplicates / missing | Seed |",
    "| --- | --- | ---: | --- | --- | ---: | --- | ---: |",
    (.entries[]
     | "| \(.id | code) | \(.status) | \(.exit_code | cell) | \(if .status == "failed" then (.category // "unclassified") else "—" end) | \(.final_phase | cell) | \(.duration_seconds | duration) | \(ledger_cell) | \(.seed | cell) |"),
    ([.entries[] | select(metrics_line != "")] as $measured
     | if ($measured | length) == 0 then empty
       else "", "Recovery and resources:", "",
            ($measured[] | "- \(.id | code): \(metrics_line)")
       end),
    ([.entries[] | select(.status != "passed")] as $failed
     | if ($failed | length) == 0 then empty
       else "", "Failures:", "",
            ($failed[]
             | "- \(.id | code) \(.status)"
               + (if .status == "failed" then ", \(.category // "unclassified")" else "" end)
               + (if .final_phase then " in phase " + (.final_phase | code) else "" end)
               + (if .reason then ": \(.reason)" else "" end)
               + (if .seed != null then "; seed \(.seed)\(if .policy then " under \(.policy)" else "" end)" else "" end)
               + (if .note then " (\(.note))" else "" end)
               + (if .reproducer and .status != "not-started" then ". Reproduce with " + (.reproducer | code) else "" end)
               + (if .finding then "; finding " + (.finding | code) else "" end)
               + (if .artifacts and .log then "; evidence under " + (.artifacts | code) + " and " + (.log | code)
                  elif .log then "; console log " + (.log | code)
                  else "" end))
       end),
    (if .cleanup == null then empty
     else [.cleanup.runs[] | select(.leftovers != null)] as $left
          | "",
            (if ($left | length) == 0
             then "Cleanup found nothing left by any run."
             else "Cleanup removed what \($left | length | counted("run"; "runs")) left behind, after capturing it under " + ([$left[].leftovers.evidence | code] | join(", ")) + "."
             end),
            (if any(.cleanup.runs[]; .exit_code != 0)
             then "**Cleanup failed** for " + ([.cleanup.runs[] | select(.exit_code != 0) | .run_id | code] | join(", ")) + "."
             else empty end)
     end);

if length == 1 then
  .[0] | section("##")
else
  (if all(.[]; .status == "passed") then "passed" else "failed" end) as $overall
  | "## Chaos \([.[].suite] | unique | join(", ")): \($overall)",
    "",
    "| Shard | Verdict | Entries | Passed | Duration |",
    "| ---: | --- | ---: | ---: | ---: |",
    (sort_by(.shard)[]
     | "| \(.shard | cell) | \(.status) | \(.entries | length) | \([.entries[] | select(.status == "passed")] | length) | \(.duration_seconds | duration) |"),
    (sort_by(.shard)[] | "", section("###"))
end
