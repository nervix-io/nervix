# WASM state qualification ledger

This ledger is the acceptance record for
[WASM State 11](https://app.clickup.com/t/90141361959/86bc22g2q) under the
[WASM state durability, reset and recovery epic](https://app.clickup.com/t/86bc22g1h). It names the
executable evidence for every guarantee the epic advertises, the crash windows the qualification
pins, the defects it found and fixed, and the practical limits of the durability contract.

Run the public qualification selection with:

```console
just test-scenarios --input tests/features/runtime/wasm_state_qualification.feature
just test-scenarios --input tests/features/runtime/wasm_checkpoint_durability.feature
just test-scenarios --input tests/features/runtime/wasm_state_reset.feature
just test-scenarios --input tests/features/runtime/wasm_state_recovery.feature
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

## Defects found and fixed

1. **A branch task outlived its node.** Runtime teardown gives a processor task two seconds, and the
   processor task gives each branch two seconds inside it, so a processor task was ended before it
   could end a branch still in its checkpoint. The branch's Tokio handle was then dropped, which
   detaches the task: it kept running without an owner, reported a `state authority check` failure
   after the node had stopped, and held the node's runtime database open, so an in-process restart
   failed with `node returned before releasing its node database lock`. Branch handles now end their
   task when dropped. Red: every three-node cluster restart with a held checkpoint failed. Green:
   the same scenarios restart.
2. **A whole-cluster restart could reset every WASM branch.** The first node to lead after a restart
   formed a quorum with a second node before gossip had heard from the owner, failed the processor
   over "without local replicated state" and started a new generation, discarding the replicated
   checkpoints of every branch. Automatic scheduling now waits up to ten seconds for gossip to
   observe every voter. Red: the three-node cluster-stop scenario lost `beta`'s state. Green: it
   continues from it.

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

- A checkpoint has ten seconds from the guest's save to its last replica's confirmation. One branch
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

## Validation record

Recorded on 2026-09-25 against base revision `b02ef87b` on one Linux host running other worktrees
builds and suites at the same time.

| Command | Result |
| --- | --- |
| `just test-scenarios --input tests/features/runtime/wasm_state_qualification.feature` | 11 of 11 scenarios passed (the one owner-loss retry of an earlier run led to that outline being `@exclusive`, like its sibling in `wasm_checkpoint_durability.feature`) |
| `just test-scenarios --input 'tests/features/runtime/wasm_\*.feature'` | 5 features, 122 of 122 scenarios passed |
| `just test-scenarios --input 'tests/features/cluster/\*.feature'` | 30 features, 209 of 209 scenarios passed, including every restart, failover, drain and relocation scenario the voter observation grace touches |
| `just test-shuttle wasm_checkpoint`, `just test-shuttle durability` | 8 of 8 checks passed under random, PCT and DFS schedules |
| `just test-lib a_branch_task_ends_with_the_handle wasm_checkpoint durability`, `just test-consensus a_voter_gossip_neither` | Passed |
| `just validate`, `just ratchet` | Passed; every ratchet count at its baseline |
| `just test-coverage-feature tests/features/runtime/wasm_state_qualification.feature` | 143 of 149 instrumented changed server lines covered (96.0%) |

Before the fixes, the same feature failed every three-node cluster restart with a held checkpoint
(`node returned before releasing its node database lock`), and the three-node cluster-stop scenario
lost `beta`'s state after the restarted leader failed its owner over.

`just bench-wasm-checkpoint`, run on the same ZFS filesystem for both arms under the shared benchmark
lock, measured one durable checkpoint at 3–18 ms per round for one branch and 6–37 ms for sixteen
concurrent branches that share a synchronization, and the unsynchronized write at 0.3 ms for one
branch and 2.6–2.8 ms for 128. Both arms' intervals are dominated by the host's synchronization
latency and overlap, so the barrier refactor, which moves the coalescing loop without changing it,
shows no attributable cost. Durability is not weakened: every checkpoint still waits for a
synchronization that covers it.
