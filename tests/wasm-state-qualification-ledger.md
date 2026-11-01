# WASM state qualification ledger

This ledger is the acceptance record for
[WASM State 11](https://app.clickup.com/t/90141361959/86bc22g2q) under the
[WASM state durability, reset and recovery epic](https://app.clickup.com/t/86bc22g1h). It names the
executable evidence for every guarantee the epic advertises, the crash windows the qualification
pins and the practical limits of the durability contract.

Run the public qualification selection with:

```console
just test-scenarios --input tests/features/runtime/wasm_state_qualification.feature
just test-scenarios --input tests/features/runtime/wasm_checkpoint_durability.feature
just test-scenarios --input tests/features/runtime/wasm_state_reset.feature
just test-scenarios --input tests/features/runtime/wasm_state_recovery.feature
just test-scenarios --input tests/features/runtime/large_wasm_state_replication.feature --tags @wasm_state_qualification
just test-scenarios --input tests/features/runtime/wasm_processor.feature
just test-shuttle wasm_checkpoint
just test-shuttle durability
```

## Crash windows

The `@wasm_state_qualification` feature holds one checkpoint at a named window through a
harness-only pause and ends its owner there. Each window lies after the callback dispatched its
output and before the acknowledgements it decided were released.

| Window | Where the checkpoint is held | Single-node restart continues from | Evidence |
| --- | --- | --- | --- |
| `before_capture` | Output dispatched, guest not saved | The previous checkpoint | Output was observed during the hold; the withheld offset stayed below the input; after restart the replayed input was applied to state that never reflected it |
| `after_capture` | Saved and stamped, not on storage | The previous checkpoint | As above |
| `after_local_durability` | On the owner's storage, replicas not confirmed | The held checkpoint | The replayed input was applied a second time, as documented |
| `before_acknowledgement` | Boundary reached, not committed, acknowledgements held | The held checkpoint | As above |

Owner loss on a three-node cluster with one replica is qualified at the two windows it can tell
apart, `before_capture` and `before_acknowledgement`: the promoted replica continues each branch, and
every withheld input is acknowledged once a checkpoint covering it completes. After local durability
the replica may or may not have fetched the checkpoint yet, so that window has no single outcome
after owner loss. A source that redelivers while no owner accepts can deliver an input more
than once there, so that outline asserts continuity rather than row numbering; the single-node
outline pins the state each window recovers. `docs/src/wasm-processor-guests.md` states the
resulting table.

The reset publication window is qualified by a reset whose initial checkpoint cannot reach storage:
it is committed but not usable (`PUBLISHING`), the cluster restarts in that state, the published
generation survives the restart unchanged, and retrying the same request reference completes that
generation (`READY, generation 2`) without starting another one, on one node and on three nodes
with one replica.

## Acceptance coverage

| Acceptance area | Evidence |
| --- | --- |
| No stale state resurrection | `wasm_state_reset.feature` "A reset survives owner failover and cannot be resurrected by the former owner"; `wasm_processor.feature` "WASM guest state reset by owner loss is not resurrected when the former owner returns"; the qualification reset outline keeps generation 2 through a cluster restart and retry. Superseded-generation checkpoints fail the `state authority check` before anything is persisted. |
| No cross-branch state leak | Every runtime outline interleaves `alpha` and `beta`; the crash-window outlines hold only an `alpha` checkpoint and prove `beta` continues from its own state. `wasm_state_reset.feature` single-branch, all-branch and guest-requested resets leave siblings counting. |
| No double terminal ACK | Shuttle: `shuttle_a_held_input_resolves_once_after_its_completed_checkpoint`, `…whose_checkpoint_failed_is_negatively_acknowledged`, `…whose_delivery_failed_is_negatively_acknowledged`, `…whose_checkpoint_and_delivery_failed_resolves_once` over the production `WasmCheckpointHolds` and `AckSet`; `runtime_ack.rs` Shuttle suite for the terminal transition. |
| No success ACK before the state fence | The crash-window outlines observe the committed Kafka offset below the held input for every window; `wasm_checkpoint_durability.feature` fails storage and replica installation; Shuttle `shuttle_a_checkpoint_waiting_for_its_replicas_misses_no_confirmation`. |
| No double reset on replay | `wasm_state_reset.feature` retry, lost-reply-across-leader-change and expired-identity scenarios; the qualification reset outline retries after a cluster restart. |
| No infinite destructive retry | `wasm_state_recovery.feature` "A failed fresh initialization spends the one recovery attempt for good" survives a cluster restart; guest-requested reset failure leaves the lifetime usable. |
| No state destruction on failed pre-activation rebind | `wasm_state_reset.feature` rebind rejected by an unusable module, and the two-usage batch rebind. |
| Stable-storage completion | Shuttle `shuttle_concurrent_writers_are_reported_durable_only_after_a_covering_synchronization`, `shuttle_a_failed_synchronization_refuses_every_uncovered_writer`, `shuttle_an_abandoned_synchronization_frees_the_barrier` over the production `DurabilityBarrier`. |
| Shutdown and deadline expiry | "A cluster stopped while its owner holds a checkpoint ends the held branch with its node" and the unit test `a_branch_task_ends_with_the_handle_its_processor_task_holds`. |
| Full restart and rejoin | The single-node crash-window outline, the three-node cluster-stop scenario, the reset outline, and `wasm_processor.feature` "WASM processor restores guest state after cluster restart". |
| Saves above the replication message limit | `large_wasm_state_replication.feature` checkpoints two interleaved branches with 3 and 40 MiB saves, verifies replica-confirmed revisions, then exercises promotion and planned drain with 3 MiB saves and no duplicate output. The bulk stream checks length and digest before replica installation. |

## Deterministic race checks

| Check | Production types | Invariant |
| --- | --- | --- |
| `runtime::wasm_checkpoint::shuttle_tests::shuttle_a_checkpoint_waiting_for_its_replicas_misses_no_confirmation` | `ReplicatedWasmProcessorState` | A confirmation wait never misses a replica report; no replica fetches a revision before it is locally durable; inspection never reports confirmation early or moves the committed revision back. |
| `runtime::wasm_checkpoint::shuttle_tests::shuttle_a_held_input_*` (four) | `WasmCheckpointHolds`, `AckSet` | An input resolves exactly once, succeeds only when both its checkpoint and delivery do, and never before its hold is released. |
| `runtime::state_store::durability::shuttle_tests::shuttle_*` (three) | `DurabilityBarrier` | Durability is reported only after a covering synchronization succeeded, synchronizations never overlap, a failure refuses every uncovered writer, and an abandoned runner frees the barrier. |

Each runs under 1,000 random, 1,000 PCT (depth 3) and 1,000 bounded DFS schedules through
`just test-shuttle`. A failing schedule is written under `target/shuttle-failures/<package>/<test>`
and replays with `just test-shuttle-replay <schedule>`. Generation fencing itself is covered by the
existing `state_store_shuttle_tests.rs` checks of the state assignment authority; the ACK tree by
`runtime_ack.rs`.

## Practical durability limits

- A checkpoint has ninety seconds from the guest's save to its last replica's confirmation. One branch
  holds the acknowledgements of one callback at a time.
- Without replicas, only the owner's storage holds a branch's checkpoints: losing that node resets
  the branch. With replicas, a promoted replica holds every checkpoint whose acknowledgements were
  released.
- A failed storage synchronization is not retried; every later checkpoint on that node fails until
  it restarts.
- Replay after a crash is at least once and can be more than once while a source redelivers with no
  owner accepting; a guest applies a redelivered input again unless it recognizes it.
- A whole-cluster restart keeps ownership while every voter rejoins within the ten-second observation
  grace.

Historical run results and audit findings are retained on [WASM State 11](https://app.clickup.com/t/86bc22g2q).
