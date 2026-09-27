# Transaction quiesce qualification ledger

This is the acceptance record for [Tx Quiesce 11: Qualify inspection, recovery, and large reports
across the full stack](https://app.clickup.com/t/90141361959/86bc16euj). The audit starts from
`origin/main` at `3fb52ed0` and follows the one typed impact report from planning through replicated
storage, execution outcomes, the Rust client, CLI rendering, and the browser inspector.

Run the public qualification matrix with:

```console
just test-scenarios --tags @transaction_quiesce_qualification --retry 0
```

The tag selects the NSPL transaction, actual quiescence, browser inspector, and large report
features. Scenario outlines exercise one-node and three-node clusters. Fault-driven scenarios use
explicit reached and released barriers, while recovery assertions poll replicated state with fixed
deadlines.

## Acceptance coverage

| Acceptance area | Executable evidence |
| --- | --- |
| Operation, execution-step, and transaction-wide results | `runtime/nspl_transactions.feature`: **DESCRIBE TRANSACTION reads the open transaction without becoming its content** and **Transaction commands report planned quiescence and COMMIT reports only the executed aggregate** inspect operation selection, step grouping, the whole report, and the final aggregate on one and three nodes. `application/transaction/rendering_tests.rs`: `text_names_every_operation_reason_effect_and_outcome` and `json_keeps_every_kind_of_the_same_report` cover every typed reason, planned effect, actual engagement, recovery effect, and execution outcome. |
| Mixed, disjoint, overlapping, cancellation, replacement, lifecycle, shared-gate, and branch scopes | `registry/transaction/tests.rs`: `entity_pause_report_and_execution_share_the_exact_affected_graph`, `cancelling_alters_make_the_atomic_run_a_noop`, `drop_recreate_retains_both_sides_of_a_rewired_graph`, and `lifecycle_steps_change_the_effective_pause_of_later_model_runs` own the planner classifications. `runtime/nspl_transactions.feature`: **Atomic model runs use their base-to-final effect and ordered lifecycle state** covers cancellation, drop/recreate, STOP, and START publicly. `web-console/transaction_inspector.feature`: **Parallel data and materialized-state relations keep separate routes and keyboard detail**, **A branched affected graph names its exact branch group**, and **A shared intake gate is drawn once with both contributing operations** cover route, branch, overlap, and shared-gate presentation. |
| Preview versus actual gates and activation | `runtime/nspl_transactions.feature`: **COMMIT remains applying until its durable model effect is activated**, **Transactional START and STOP remain applying through activation**, and **Transaction commands report planned quiescence and COMMIT reports only the executed aggregate** distinguish preview, durable effect, activation completion, and commit output. `web-console/transaction_inspector.feature`: **A retained inspection shows an applying prefix and final actual outcomes** shows both phases through the browser. |
| Failure before engagement, failure after engagement, ambiguous remote results, partial commit, and recovery expansion | Every scenario in `runtime/transaction_actual_quiescence.feature` inspects the retained result: definitive pre-engagement rejection stays `DYNAMIC`; drain failures retain entity or domain engagement and release; a timed-out remote response is `UNCERTAIN`; successor cleanup retains the former leader's pause; a failed later step preserves its applied prefix; and entity swap fallback records the wider recovery pause and rebuild scope. |
| Leader replacement, reconnect, snapshot, restart, historical graphs, and retention expiry | `runtime/nspl_transactions.feature`: **A queued transaction preview survives a full cluster restart**, **An open transaction survives leader failover and the client resumes it**, **An interrupted commit retains domain mutation ownership on the new leader**, **A failing resumed commit records the failing step and preserves its prefix**, and **An orphaned transaction expires and retains its outcome** cover state transitions. `consensus/storage/tests.rs`: `transaction_report_and_frozen_plan_survive_snapshot_installation` covers bounded snapshot transfer. `web-console/transaction_inspector.feature`: **A dropped relay and its disconnected relations remain inspectable** and **An open inspector follows the attached transaction across leader reconnection** retain historical topology and browser state. |
| Large graphs, reports, and committed log ranges | `runtime/transaction_quiesce_qualification.feature`: **A multi-mebibyte report replicates without truncation and survives replay and restart** constructs 120 affected relays and 128 ordered operation contributions in one atomic execution step. It proves that the typed and JSON reports agree and exceed 2 MiB, that the queue admissions add more than 2 MiB of encoded committed Raft log, that a stopped follower replays all 128 operation records and the complete step before becoming leader, and that the committed report is byte-for-byte identical after a full cluster restart. `consensus/storage/tests.rs`: `complete_log_reads_return_every_entry_in_a_large_range` and `log_entry_stream_reads_a_large_range_in_separate_chunks` directly cover complete and bounded reads. |
| Rust API, text, JSON, CLI, and browser agreement | `application/transaction/rendering_tests.rs`: `json_renders_the_same_typed_inspection` compares the JSON document with the typed report. The large public scenario repeats that comparison through the live session stack. `runtime/nspl_transactions.feature`: **A standalone CLI inspection prints one JSON document and a refusal exits nonzero** exercises text and JSON through the real CLI binary. Every scenario in `web-console/transaction_inspector.feature` consumes the same typed inspection envelope and covers keyboard access, topology modes, partial reports, actual outcomes, and stale refresh. |
| Inspect-other isolation, queue position, delayed response ordering, stale preview fencing, and identified commit | `runtime/nspl_transactions.feature`: **DESCRIBE TRANSACTION by identity reads another transaction without adopting it** preserves domain, binding, and queue position; **A commit fenced to a preview the transaction outgrew is refused and stays open** refreshes and commits only the current identified preview. `client-core/src/tests.rs`: `a_basis_read_for_another_transaction_never_fences_this_one`, `an_older_inspection_cannot_replace_a_newer_queue_preview`, `response_reordering_cannot_take_another_requests_waiter`, and `concurrent_commands_capture_transaction_position_in_send_order` cover delayed and out-of-order replies. The browser scenarios repeat stale rejection, refresh, whole-transaction operation selection, and inspect-other isolation. |
| Storage size and cleanup bounds | `consensus/transaction_report.rs`: `reports_round_trip_update_steps_and_deduplicate_topology` proves that reports store content-addressed topology once and reconstruct complete operation and step views. `revisions_are_retained_exactly_until_transaction_cleanup` proves that retention cleanup removes only unreferenced records. The large public scenario crosses the multi-mebibyte response and log boundaries without introducing data-plane work or shutdown behavior. |

## Cross-client contract

`DESCRIBE TRANSACTION ... OPERATION <n>` always returns the whole transaction report. The operation
number selects the initial focus in text and browser views; it does not narrow topology or execution
steps. A successful attached inspection at the current accepted position supplies the preview
identity that `COMMIT` must name. An inspection of another transaction and a delayed older response
cannot replace that identity. A stale commit applies no effects and supplies the current identity so
the caller can refresh and retry.

Text, JSON, the Rust `TransactionInspection`, and the browser graph are projections of the same
server response. The server reconstructs content-addressed topology before dispatch and never sends
a partial success. Size changes transfer and rendering cost, not semantics: the response contains
all operations, all execution steps, both topology sides, and all recorded actual outcomes.

## Validation record

The public qualification selection passed with retries disabled: 4 features, 93 scenarios, and
every step succeeded. The large-report feature also passed by itself under LLVM coverage with 1
scenario and 34 steps. The isolated browser selection matched the two inspector scenarios whose
names contain `Inspecting` and `basis`; both passed under LLVM coverage with 79 steps.

Library coverage ran 116 client-wire, 105 consensus, and 12 execution tests. Combining those LCOV
records with the successful large-report and browser profiles covered 335 of 370 executable changed
Rust lines, or **90.54% patch coverage** against the task base. Direct package runs also passed the
client-wire transport integration tests, for 125 client-wire tests in total.

The repository gates passed: `just validate`, `just ratchet`, `just nspl-completion-walk`,
`just validate-skill`, `just book`, `just cargo-fmt-check`, and `just gherkin-fmt-check`.
