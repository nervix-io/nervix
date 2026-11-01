# Harness liveness qualification map

This map names the executable evidence for each lifecycle and failure-handling contract.
Historical run results and audit findings are retained on [Test Liveness 06](https://app.clickup.com/t/86bc2wugx).

Run the focused regressions with `just test-harness-liveness` and the public suite with
`just test-scenarios`. Use `--suite-budget <duration>` for a deliberate watchdog qualification.
[Integration Test Lifecycle](../docs/src/integration-test-lifecycle.md) owns the current harness
budgets, concurrency policy and cleanup contract.

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
| Scenario cleanup of a whole cluster | `tests/common/cluster_teardown.rs` | 60s for every node together | A node that ended releases its ports and fault state; a node still running is left stopping and keeps its storage, ports and fault state until its own task ends |
| A whole scenario run | `tests/common/suite_watchdog.rs` | 37 minutes, then a 60s cleanup window | Every active scenario is printed with its attempt, phase, phase age and nodes; exit status 124 |
| The teardown after a run | `tests/common/suite_watchdog.rs` | 2 minutes for the dependency containers, 60s for the runtime | The containers are left to the runner; the process still ends |
| One draw from the port pool | `tests/common/port_pool.rs` | 65,536 draws that land on reserved ports | `PortPoolError::Exhausted` naming how many ports the process held |

A node task is reported as not started, running, left stopping, or terminal with one of four
outcomes: a clean application exit, an application error, a panic or a cancellation. No harness path
aborts a node task. A readiness probe is reported as ready, as a request that failed with its typed
cause and the operation a deadline interrupted, or as a non-ready response with the node's message
and diagnostics. A scenario publishes the phase it is in, queued, started, body complete, teardown
started, teardown diagnostics, stopping and finished, and writes the finished marker only once
cleanup has completed. No harness path reports an unknown reason or a task that stopped without
details.

## Qualification matrix

| Matrix item | Evidence |
| --- | --- |
| 1. Readiness connection, session and response stalls | `status_request_ends_at_its_deadline_while_the_connection_is_pending`, `…while_session_establishment_is_pending`, `…while_the_response_is_pending` and `…while_unrelated_responses_remain_ready` in `tests/harness_liveness.rs` each stall one operation of a real loopback session and require the request to end at its phase deadline naming that operation. `readiness_and_status_outcomes_retain_their_typed_cause` covers the refused connection, rejected session, failed stream, ended session and non-ready response. |
| 2. A node task running, exiting cleanly, returning an application error, panicking and being cancelled | `running_task_reports_the_last_failed_probe_at_the_deadline`, `clean_application_exit_before_readiness_is_terminal`, `application_error_before_readiness_is_retained` and `panic_and_cancellation_have_distinct_terminal_outcomes` drive each ending through `OwnedNodeTask` and require the startup diagnostic to carry the node, attempt, elapsed time, task state and last probe cause. `a_single_node_cleanup_keeps_how_its_task_ended` and `a_panicking_node_is_the_only_cleanup_failure_a_three_node_cluster_reports` cover the same endings during cleanup. |
| 3. Status polling whose last request never replies | `status_wait_ends_at_its_original_deadline_when_the_final_request_never_replies` replies to the first three requests and stalls every later one; the wait ends at its original 40s deadline, not one request timeout later, and keeps the last output and the timed-out operation. `nested_deadline_never_outlives_its_phase` covers the deadline arithmetic. `startup_readiness_failure_reports_the_timed_out_status_operation` covers the same stall inside a node startup. |
| 4. One- and three-node diagnostics with one stalled node | `a_stalled_diagnostic_still_reaches_every_node_stop_in_a_cluster_of_one_and_of_three` collects diagnostics from a cluster of one whose only node withholds its session and from a cluster of three with one such node: the diagnostics end at their deadline, the healthy nodes keep their status, the stalled node keeps its timed-out operation, and every node is still asked to stop. `status_snapshots_keep_a_healthy_node_while_another_node_stalls` and `failed_and_stalled_diagnostics_end_by_their_deadline_so_cleanup_starts` cover the concurrent requests and the refused node. |
| 5. One- and three-node teardown with all nodes stuck | `stuck_nodes_spend_one_cleanup_budget_in_a_cluster_of_one_and_of_three` stops a cluster of one and a cluster of three whose nodes all ignore their stop: each cleanup spends one budget rather than one per node, every node is left stopping with its task still running, and nothing a node holds is released under it. `a_node_left_stopping_keeps_what_it_holds_until_its_own_task_ends` releases what such a node holds only once its task has ended, and `a_wait_past_its_deadline_leaves_the_task_running_with_its_owner` and `the_finished_phase_is_published_only_once_cleanup_has_completed` cover the owned task and the phase the cleanup publishes. |
| 6. Startup retry exhaustion and success after one transient failure | `repeated_readiness_failure_spends_one_budget_across_every_attempt`, `a_node_that_does_not_stop_ends_the_startup_instead_of_being_launched_over`, `exhaustion_reports_every_attempt_with_its_typed_cause` and `ports_that_cannot_be_reallocated_end_the_startup` cover exhaustion; `a_bound_address_is_retried_until_a_launch_becomes_ready` and `a_last_attempt_still_becomes_ready_with_what_the_budget_left` cover success after a transient failure; `an_application_error_ends_the_startup_without_another_launch`, `a_panicking_node_ends_the_startup_without_another_launch` and `a_launch_failure_ends_the_startup_before_anything_is_cleaned_up` cover the terminal classifications; `sequential_cluster_construction_stays_inside_its_derived_budget` covers the cluster bound. |
| 7. Scenario body, after-hook diagnostic and teardown stalls reaching the suite watchdog | `a_stalled_scenario_body_is_named_with_its_attempt_phase_and_nodes`, `a_stalled_teardown_diagnostic_is_named_by_the_phase_it_is_in` and `a_node_that_never_stops_is_named_at_the_end_of_the_cleanup_window` register a scenario in each phase holding a live cluster and time the suite out on a paused clock: the diagnostic names the scenario, attempt, phase and nodes, every node is asked to stop, and a node that never stops is still live at the end of the cleanup window. `a_cluster_that_outlives_its_scenario_is_named_as_unclaimed`, `a_retried_scenario_publishes_which_attempt_is_running`, `the_suite_budget_is_injectable_and_defaults_to_the_suite_policy`, `a_timed_out_suite_is_reported_apart_from_a_passing_and_a_failing_one`, `a_failing_suite_ends_the_process_by_unwinding` and the two dependency-stop regressions cover the rest of the watchdog. Whole-suite watchdog qualification reaches the same path through the real runner at the CI concurrency factor. |
| 8. The existing WASM guest-state restart scenario in its normal form | `WASM processor restores guest state after cluster restart` in `tests/features/runtime/wasm_processor.feature` covers all three example rows with retries disabled. |
| 9. A deliberate assertion failure while other scenarios are active | An induced-failure qualification adds a feature whose only scenario starts a three-node cluster and asserts that `SHOW CLUSTER STATUS` does not name `node-1`. It fails on every attempt while the rest of the suite runs beside it, and each attempt's cleanup reaches its finished marker. |
| 10. The full high-parallelism Cucumber configuration used by CI, including retries and artifact collection | Run the full suite at the CI concurrency factor with its configured retries, and retain artifacts after success, failure and an induced suite timeout. |
