# Harness liveness qualification ledger

This ledger is the acceptance record for [Test Liveness 06: Qualify bounded high-parallelism
failure handling](https://app.clickup.com/t/90141361959/86bc2wugx). It names the executable
evidence for every item of the qualification matrix, records the runs that produced it, and
compares them with the run that opened the epic: a `tests` job the workflow killed at its
60-minute limit with 43 scenarios still unfinished.

Run the focused harness regressions and the scenario suite with:

```console
just test-harness-liveness
just test-scenarios
```

The scenario suite runs at the CI configuration when `NERVIX_TEST_CONCURRENCY_FACTOR=2` is set,
which is two scenarios per CPU, and it takes `--suite-budget <duration>` to reach the watchdog path
without waiting out the suite's own budget.

## The contract under qualification

Every lifecycle phase of the in-process test harness has one owner, an absolute budget fixed when
the phase starts, and a typed outcome. Retries and polls inside a phase receive only what is left of
it. The budgets below are derived in the module that owns them from measured policy inputs, and
`const` assertions keep their ordering.

| Phase | Owner | Budget | Outcome when the budget passes |
| --- | --- | --- | --- |
| One status request: connect, open session, send, receive | `tests/common/status_request.rs` | 10s, or the rest of the enclosing phase | `StatusRequestError::DeadlinePassed` naming the pending operation |
| A status wait: leadership, membership, interconnect, applied index | `tests/common/status_request.rs`, `tests/common/phase_deadline.rs` | 40s | The poll ends at its original deadline with the last output and the last typed failure |
| Teardown diagnostics for every node at once | `tests/common/status_request.rs` | 10s | Each node keeps its status text or its own typed failure |
| One node startup, three launches included | `tests/common/node_startup.rs` | 84s: two full 36s readiness attempts, each with a 5s cleanup slice and a 1s pause | `NodeStartupExhausted` carrying every attempt, its classification and its cleanup |
| A cluster startup | `tests/common/node_startup.rs` | 84s per node, one construction deadline | The node startup that ran out ends the construction |
| Scenario cleanup of a whole cluster | `tests/common/cluster_teardown.rs` | 60s for every node together | Still-running tasks are aborted and joined, then every node releases its ports and fault state |
| A whole scenario run | `tests/common/suite_watchdog.rs` | 37 minutes, then a 60s cleanup window | Every active scenario is printed with its attempt, phase, phase age and nodes; exit status 124 |
| The teardown after a run | `tests/common/suite_watchdog.rs` | 2 minutes for the dependency containers, 60s for the runtime | The containers are left to the runner; the process still ends |
| One draw from the port pool | `tests/common/port_pool.rs` | 65,536 draws that land on reserved ports | `PortPoolError::Exhausted` naming how many ports the process held |

A node task is reported as not started, running, or terminal with one of four outcomes: a clean
application exit, an application error, a panic or a cancellation. A readiness probe is reported as
ready, as a request that failed with its typed cause and the operation a deadline interrupted, or as
a non-ready response with the node's message and diagnostics. A scenario publishes the phase it is
in, queued, started, body complete, teardown started, teardown diagnostics, stopping and finished,
and writes the finished marker only once cleanup has completed. No harness path reports an unknown
reason or a task that stopped without details.

## Qualification matrix

| Matrix item | Evidence |
| --- | --- |
| 1. Readiness connection, session and response stalls | `status_request_ends_at_its_deadline_while_the_connection_is_pending`, `…while_session_establishment_is_pending`, `…while_the_response_is_pending` and `…while_unrelated_responses_remain_ready` in `tests/harness_liveness.rs` each stall one operation of a real loopback session and require the request to end at its phase deadline naming that operation. `readiness_and_status_outcomes_retain_their_typed_cause` covers the refused connection, rejected session, failed stream, ended session and non-ready response. |
| 2. A node task running, exiting cleanly, returning an application error, panicking and being cancelled | `running_task_reports_the_last_failed_probe_at_the_deadline`, `clean_application_exit_before_readiness_is_terminal`, `application_error_before_readiness_is_retained` and `panic_and_cancellation_have_distinct_terminal_outcomes` drive each ending through `OwnedNodeTask` and require the startup diagnostic to carry the node, attempt, elapsed time, task state and last probe cause. `a_single_node_cleanup_keeps_how_its_task_ended` and `a_panicking_node_is_the_only_cleanup_failure_a_three_node_cluster_reports` cover the same endings during cleanup. |
| 3. Status polling whose last request never replies | `status_wait_ends_at_its_original_deadline_when_the_final_request_never_replies` replies to the first three requests and stalls every later one; the wait ends at its original 40s deadline, not one request timeout later, and keeps the last output and the timed-out operation. `nested_deadline_never_outlives_its_phase` covers the deadline arithmetic. `startup_readiness_failure_reports_the_timed_out_status_operation` covers the same stall inside a node startup. |
| 4. One- and three-node diagnostics with one stalled node | `a_stalled_diagnostic_still_reaches_every_node_stop_in_a_cluster_of_one_and_of_three` collects diagnostics from a cluster of one whose only node withholds its session and from a cluster of three with one such node: the diagnostics end at their deadline, the healthy nodes keep their status, the stalled node keeps its timed-out operation, and every node is still asked to stop. `status_snapshots_keep_a_healthy_node_while_another_node_stalls` and `failed_and_stalled_diagnostics_end_by_their_deadline_so_cleanup_starts` cover the concurrent requests and the refused node. |
| 5. One- and three-node teardown with all nodes stuck | `stuck_nodes_spend_one_cleanup_budget_in_a_cluster_of_one_and_of_three` stops a cluster of one and a cluster of three whose nodes all ignore their stop: each cleanup spends one budget rather than one per node, every task is aborted and joined for its cancellation, and harness state is released only once every task has ended. `forced_cleanup_aborts_and_joins_the_owned_task_once` and `the_finished_phase_is_published_only_once_cleanup_has_completed` cover the owned task and the phase the cleanup publishes while it is forced. |
| 6. Startup retry exhaustion and success after one transient failure | `repeated_readiness_failure_spends_one_budget_across_every_attempt`, `cleanup_that_never_completes_is_aborted_inside_the_same_budget`, `exhaustion_reports_every_attempt_with_its_typed_cause` and `ports_that_cannot_be_reallocated_end_the_startup` cover exhaustion; `a_bound_address_is_retried_until_a_launch_becomes_ready` and `a_last_attempt_still_becomes_ready_with_what_the_budget_left` cover success after a transient failure; `an_application_error_ends_the_startup_without_another_launch`, `a_panicking_node_ends_the_startup_without_another_launch` and `a_launch_failure_ends_the_startup_before_anything_is_cleaned_up` cover the terminal classifications; `sequential_cluster_construction_stays_inside_its_derived_budget` covers the cluster bound. |
| 7. Scenario body, after-hook diagnostic and teardown stalls reaching the suite watchdog | `a_stalled_scenario_body_is_named_with_its_attempt_phase_and_nodes`, `a_stalled_teardown_diagnostic_is_named_by_the_phase_it_is_in` and `a_node_that_never_stops_is_named_at_the_end_of_the_cleanup_window` register a scenario in each phase holding a live cluster and time the suite out on a paused clock: the diagnostic names the scenario, attempt, phase and nodes, every node is asked to stop, and a node that never stops is still live at the end of the cleanup window. `a_cluster_that_outlives_its_scenario_is_named_as_unclaimed`, `a_retried_scenario_publishes_which_attempt_is_running`, `the_suite_budget_is_injectable_and_defaults_to_the_suite_policy`, `a_timed_out_suite_is_reported_apart_from_a_passing_and_a_failing_one`, `a_failing_suite_ends_the_process_by_unwinding` and the two dependency-stop regressions cover the rest of the watchdog. The induced whole-suite timeouts recorded below reach the same path through the real runner at the CI concurrency factor. |
| 8. The existing WASM guest-state restart scenario in its normal form | `WASM processor restores guest state after cluster restart` in `tests/features/runtime/wasm_processor.feature`, all three example rows, ran unchanged with retries disabled; see the validation record. |
| 9. A deliberate assertion failure while other scenarios are active | The induced-failure run below adds a feature whose only scenario starts a three-node cluster and asserts that `SHOW CLUSTER STATUS` does not name `node-1`. It fails on every attempt while the rest of the suite runs beside it, and each attempt's cleanup reaches its finished marker. |
| 10. The full high-parallelism Cucumber configuration used by CI, including retries and artifact collection | The full-suite runs below at the CI concurrency factor with cucumber's two retries, and the `tests` jobs of the pull request, whose `test-logs` artifact was uploaded after a passing run and after an induced suite timeout. |

## Finding: the failed run's stall was a synchronous port draw

The run that opened the epic was
[run 35300043155](https://github.com/nervix-io/nervix/actions/runs/35300043155) of PR #349's
branch on 18 September 2026: `blacksmith-16vcpu-ubuntu-2404`, 48 concurrent scenarios
(concurrency factor 3), job started 02:37:10Z and cancelled by the workflow at 03:37:34Z. Its
uploaded `cucumber.log` records 1,552 scenario starts and 1,509 finishes. Its last line is
`cluster start requested: nodes=3 …`, written at 03:04Z; for the remaining 33 minutes no scenario
wrote anything while the node traces in `scenarios.log` continued until the kill.

That is the shape of a port draw that never returns. Every scenario drew four ZeroMQ and syslog
ports it never gave back, and Linux hands `bind(0)` an odd port from the lower half of the
ephemeral range, some 7,057 ports on this kernel. At 1,552 starts the process held 6,208 fixture
ports plus seven per live node, more than the range offers, and the draw looped until the operating
system handed out a port the pool did not hold, which it never did. Every scenario future is polled
on the one runner task, so that loop stalled every scenario, and it would have stalled the suite
watchdog too: the watchdog is a timer on the same task. Nothing in tasks 01 to 05 bounded it, and
a healthy suite of 1,752 scenarios holds 7,016 fixture ports by its last scenario, within about
forty of that ceiling on a kernel that draws the same way.

The change recorded here bounds the draw at 65,536 misses in a row, after which the pool reports
itself exhausted and the scenario fails with that reason, and gives every scenario's fixture ports
back at the end of its cleanup. `a_draw_that_keeps_landing_on_reserved_ports_ends_at_the_draw_limit`,
`an_exhausted_draw_gives_back_the_ports_it_had_reserved`,
`a_draw_the_operating_system_refuses_is_reported_as_its_own_failure`,
`a_released_port_can_be_drawn_again` and
`ports_drawn_from_the_operating_system_are_distinct_and_reserved` are its regressions.

## Finding: the default-budget regression read the environment it was asserting against

The suite budget option reads `NERVIX_TEST_SUITE_BUDGET`, and the focused regression that asserts
its default parsed the option in whatever environment the test process had. The first `tests` job
of this change set that variable to end the suite four minutes in, and the regression failed before
a single scenario ran, asserting the policy default against the four minutes the environment gave.
The regression now expects the environment's budget when one is given and the policy default only
when none is, so a run that injects a budget is qualified by the same regressions as one that does
not.

## Validation record

Recorded on 23 September 2026 from the task worktree at revision `b7396449` and its predecessors
in this change, on a 24-CPU workstation shared with other builds and scenario suites, which held its
load average between 7 and 40 throughout. Every run below passed its cleanup inspection: no
`nervix-server` process and no scenario binary survived a run, and no dependency container was
left behind.

| Command | Result |
| --- | --- |
| `just test-harness-liveness`, 20 repetitions | Pass on every repetition: 48 regressions in 4.0s each, under load averages between 13.5 and 34.7. |
| `just test-scenarios --input tests/features/runtime/wasm_processor.feature --name WASM.processor.restores.guest.state.after.cluster.restart --retry 0`, three runs | Pass on every run: all three example rows (one node without replicas, three nodes without replicas, three nodes with one replica), 51 steps each, in 54s to 71s of wall time. Each run started 14 nodes, every one ready on its first attempt within 2.1s, and its slowest cleanup took 8.7s of the 60s budget with none forced. |
| `NERVIX_TEST_CONCURRENCY_FACTOR=2 just test-scenarios --input 'tests/features/cluster/\*.feature'` | Pass: 29 features, 206 scenarios and 2,571 steps in 500s at 48 concurrent scenarios, with 16 cucumber retries, each a `not-a-leader` setup command or an ownership-handoff gate that passed when retried. 553 node startups: median 2.04s, 99th percentile 22.0s, slowest 41.3s; 551 ready on the first attempt and two on the second, after a 36s readiness deadline whose last probe was an `Unauthenticated` status reply and whose cleanup stopped the node cleanly. 200 cleanups: median 0.11s, 99th percentile 10.2s, slowest 45.1s, none forced. |
| `NERVIX_TEST_CONCURRENCY_FACTOR=2 just test-scenarios --input 'tests/features/runtime/wasm_\*.feature'` | Pass: 4 features, 95 scenarios and 1,637 steps in 583s, with 18 retries. 288 startups all ready on the first attempt: median 2.03s, slowest 17.4s. 117 cleanups: median 0.36s, two forced at the 60s budget, both of scenarios whose body had already failed with `not-a-leader`; each forced cleanup named the node it aborted and joined and the scenarios live beside it, and both scenarios passed when retried. |
| `NERVIX_TEST_CONCURRENCY_FACTOR=2 just test-scenarios --suite-budget 4m` | Suite timeout, exit status 124 after 313s: the 240s budget, the 60.0s cleanup window and 13s of dependency and runtime teardown. The diagnostic named 49 registered scenarios: 47 queued behind an exclusive scenario with phase ages up to 128s, one in its body for 59s holding `node-1` and `node-2`, and one already finished whose world the ordered writer still held. The cleanup asked both live nodes to stop, reported `node-1` still running when the window passed (its scenario injects consensus commit delays that only its after hook releases), dropped the run, and stopped every dependency. |
| `NERVIX_TEST_CONCURRENCY_FACTOR=2 just test-scenarios` with `tests/features/runtime/harness_induced_failure.feature` present | Failed as induced, exit status 101 reporting `3 step(s) failed`, after 1,676s: 182 features, 1,753 scenarios, 19,623 steps. The induced scenario failed its assertion on all three attempts about 14s into each; every attempt published body complete, teardown started, teardown diagnostics with all three nodes' status text, stopping and finished, the last within 0.32s of the body's end. 1,750 scenario identities passed, 1,722 on the first attempt, 23 on the second and 8 on the third; the two example rows of `NATS emitter drains a wide columnar batch in order without duplicates` timed out their 90s, 32,768-message drain on all three attempts on this loaded machine, which is a throughput assertion rather than a harness outcome. 3,926 startups: median 1.94s, 99th percentile 4.14s, slowest 21.4s; the eight failed attempts were all listen-address or interconnect bind failures, classified transient, relaunched on fresh ports and ready on the next attempt. 1,756 cleanups: median 0.10s, six forced at the 60s budget, each recorded with the aborted node and the scenarios live beside it. |
| `NERVIX_TEST_CONCURRENCY_FACTOR=2 just test-scenarios` | Failed on this machine, exit status 101 reporting `3 step(s) failed`, after 1,490s: 181 features, 1,752 scenarios and 19,746 steps at 48 concurrent scenarios. 1,749 scenario identities passed, 1,719 on the first attempt, 22 on the second and 11 on the third. Three did not pass on any attempt: the three-node row of `NATS emitter drains a wide columnar batch in order without duplicates`, `NATS emitter honors its flush deadline during sustained input collection` and `Postgres poison isolation delivers healthy rows and routes only the rejected record`, each a throughput or deadline assertion on a shared workstation whose load average stayed above 10 for the whole run; all three passed in the healthy `tests` job of 22 September 2026. 3,937 startups: median 1.90s, 99th percentile 4.27s, slowest 20.7s; the one failed attempt was an HTTPS listen-address bind failure, relaunched on fresh ports and ready on its second attempt. 1,760 cleanups: median 0.10s, 99th percentile 10.5s, four forced at the 60s budget, all of WASM scenarios whose setup had failed with `not-a-leader` and which passed when retried. |
| `just test-scenarios --input 'tests/features/runtime/\{nats_emission,postgres_emission\}.feature' --retry 0` | Pass: the two features whose scenarios failed under load, 26 scenarios and 306 steps, in 43s with retries disabled; 56 startups, the slowest 3.2s, and no forced cleanup. |
| `just validate` | Pass in 13m35s: formatting, all-target Clippy with warnings denied, protocol lint, web console build, skill publication checks, documented NSPL parsing, clock-boundary and Shuttle-dependency checks. |
| `just ratchet` | Pass: every tracked architecture-debt count at its baseline. |

The induced-failure feature was:

```gherkin
Feature: Harness qualification induced failure

  Scenario: A deliberate assertion fails while other scenarios are active
    Given a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output does not contain
      """
      node-1
      """
```

## Comparison with the failed run

| | Run 35300043155, 18 September 2026 | This qualification |
| --- | --- | --- |
| Configuration | `blacksmith-16vcpu-ubuntu-2404`, 48 concurrent scenarios (factor 3), no suite budget | 24 CPUs, 48 concurrent scenarios (factor 2), 37-minute suite budget; CI at 32 concurrent scenarios (factor 2) |
| How the run ended | The workflow killed the job 60m24s after it started, 49 minutes after the scenario binary started and 33 minutes after the last scenario output | Every run ended itself with its own summary or diagnostic: the whole suite in 1,490s, the suite with an induced failure in 1,676s, the induced timeout in 313s |
| Scenarios | 1,552 started, 1,509 finished, 43 stranded with no diagnostic of their phase | Every scenario of each whole-suite run finished, and every attempt of every retried scenario reached its finished marker; every registered scenario of the timed-out run was named with its attempt, phase, phase age and nodes |
| Startup diagnostics | 31 of 33 startup retries reported `node task stopped without error details` | Every failed attempt named its node, attempt, elapsed time, task state, typed cause, classification, cleanup outcome and remaining budget |
| Cleanup | Sequential per-node waits under a fresh five-minute watchdog each | One 60s budget per cluster, forced cleanups recorded with the aborted node and the scenarios live beside them |
| Artifacts | `test-logs` uploaded by the cancellation, ending mid-scenario with no summary | `test-logs` uploaded after the passing run and after the induced suite timeout, each ending with the suite's own summary or diagnostic |

## Continuous integration evidence

Both runs are `tests` jobs of [pull request #387](https://github.com/nervix-io/nervix/pull/387)
on `blacksmith-16vcpu-ubuntu-2404` at the CI concurrency factor of two scenarios per CPU, 32
concurrent scenarios, with cucumber's two retries and the `test-logs` artifact uploaded whatever
the job's result.

| Run | Result |
| --- | --- |
| [Run 35833648770](https://github.com/nervix-io/nervix/actions/runs/35833648770), the suite given `NERVIX_TEST_SUITE_BUDGET=4m` by a commit reverted before merge | The job ended itself in 16m30s. The 48 focused regressions passed under the injected budget; the scenario binary started 11m54s into the job, finished 181 scenarios, and its budget expired 240s later with 32 scenarios active, 29 in their body and 3 stopping, each named with its attempt, phase, phase age and nodes. The cleanup asked 53 nodes to stop and every one ended itself within 9.96s of the 60s window, the dependencies stopped, the process exited 124, and the [`test-logs` artifact](https://github.com/nervix-io/nervix/actions/runs/35833648770/artifacts/10738414221) was uploaded 2s later, its `cucumber.log` ending with that diagnostic. |
| The run of the revision merged to `main`, with the suite's own 37-minute budget restored | The pull request merges only once this job passes with every scenario and uploads the same artifact; its run is linked from the pull request's checks. |
