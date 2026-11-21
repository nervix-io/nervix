# Inferencer state qualification ledger

This ledger is the acceptance record for
[Stateful Inference 09](https://app.clickup.com/t/86bcyakvx) under the
[stateful ONNX inference epic](https://app.clickup.com/t/86bcyaenx). It names the executable
evidence for every guarantee [Inferencer State And
Recovery](../docs/src/inferencer-state.md#guarantees-and-limits) advertises and the limits of the
durability contract. Stateful Inference 09 retains the qualification results: the tested
revisions, the commands, the selection and completion counts, the packaged image digests, the
sanitizer seeds and the benchmark measurements.

Run the public selection with:

```console
just test-scenarios --input tests/features/runtime/inferencer_state.feature
just test-scenarios --input tests/features/runtime/inferencer_admission.feature
just test-scenarios --input tests/features/runtime/inferencer_state_lifecycle.feature
just test-scenarios --input tests/features/runtime/inferencer_state_rebinding.feature
just test-scenarios --input tests/features/runtime/inferencer_describe.feature
just test-scenarios --input tests/features/runtime/inferencer_state_inspection.feature
just test-scenarios --input tests/features/cluster/inferencer_state_acknowledgement.feature
just test-scenarios --input tests/features/cluster/inferencer_state_handoff.feature
just test-scenarios --input tests/features/cluster/inferencer_backup.feature
just test-shuttle inferencer
just test-turmoil-simulation transport::materialized::inferencer_branch_state_transfers_reconnect_to_the_replacement_owner
just test-bolero inferencer
just bench-inferencer-state-cluster
```

The qualification ran on product source `5d985344268516f20a81194b041d836404370746`. Three later
changes touch inferencer files and carry their own evidence: the graceful domain stop
(`538e650f7`), which changed the benchmark helper; the configured backup staging buffer
(`12f68a6ce`), which changed how inferencer capture and restore reserve their staging memory; and
the change that installs a batch's carried state only once the outputs of its jobs are joined,
whose owner test the guarantee row it extends names.

## Guarantees and evidence

The public outlines run on one node and on three nodes unless their subject is a topology, such as
a handoff or an owner loss, and the stateful scenarios interleave the records of two branches, so
they also prove branch isolation.

| Guarantee | Executable evidence |
| --- | --- |
| The declaration names each port, checks fixed dense `F32` shapes and `INITIAL` values, refuses a `BATCH` axis beside carried state, and survives canonical output and recreation. | `inferencer_state.feature`: "Stateful inference refuses an invalid state declaration before activation", "Stateful model ports must all have bindings" and "SHOW CREATE preserves a stateful inferencer declaration through recreation"; `inferencer.feature` for typed ports and model validation; the Plexus completion walk for the generated grammar paths. |
| Each branch advances in arrival order, a filtered row never reaches the model, a refused row leaves both the preceding state and the other rows intact, rows whose batch's outputs cannot be joined leave the preceding state intact, and every route consumes the one admission-time result. Sensitive inputs and outputs keep their declared handling. | `inferencer_state.feature`: "Stateful inference carries one ordered sum per branch across independent routes", "Unbranched inference skips filtered and failed inputs without advancing its constant initial state", "A model refusal preserves the preceding state and routes the original input" and the sensitivity and trace-logging outlines; `inferencer_admission.feature` for route flush policies, the original row on refusal and `COLLECT FOR` batches; `benches/inferencer-state.feature`, which checks a fresh score exactly with and without carried state; the owner test `messages_that_fail_because_their_jobs_outputs_cannot_be_joined_leave_the_carried_state` in `src/runtime/inferencer/session/tests.rs`, for a batch that runs as several jobs whose outputs cannot be joined. |
| An immutable generation publishes after admission and persists on the snapshot interval and at a graceful stop; a restart restores it under the matching identity and incarnation; eviction and recreation start fresh lifetimes; a declaration beyond one snapshot section is refused. | `inferencer_state.feature`: "Stateful inferencer snapshots preserve interleaved branch sums across a cluster restart"; `inferencer_state_lifecycle.feature` for `STOP` and `START`, recreation, `TTL` and capacity eviction, the snapshot-section bound and promotion; the Bolero targets `inferencer-state-snapshots`, `inferencer-state-snapshots-malformed` and `inferencer-native-snapshot-projection` for the frame's shapes, exact bits and fail-closed parsing. |
| A planned handoff installs every branch's generation before activation, a capture stays stable through the handoff freeze, eviction racing a replacement keeps the successor's route, and a forced takeover continues from a surviving snapshot or reports its reset and starts from `INITIAL`. | `inferencer_state_handoff.feature`; the owner tests in `src/runtime/state_replication/inferencer_handoff_tests.rs`; the Shuttle checks `shuttle_inferencer_publication_keeps_captured_tensors_stable_through_handoff_freeze` and `shuttle_inferencer_eviction_racing_replacement_keeps_the_successor_route` in `crates/model-harness/shuttle-inventory.toml`; the Turmoil scenario `transport::materialized::inferencer_branch_state_transfers_reconnect_to_the_replacement_owner` in `tests/turmoil-inventory.toml`; the packaged `stateful-partitions-3-owner-crash` chaos scenario, whose `chaos_running` inferencer reconciles its ledger after the owner is killed. |
| A backup holds each branch's published tensors at its cut, a restore installs them on the owner and replicas with every bit intact, and `WITHOUT STATE` starts fresh. Acknowledgements follow dispatch through a handoff, a promotion and an acknowledging emitter. | `inferencer_backup.feature` and `inferencer_state_acknowledgement.feature`; the Shuttle check `shuttle_a_forced_inferencer_publication_holds_every_row_acknowledged_before_the_cut`; the Bolero targets `backup-inferencer-values` and `backup-inferencer-malformed`. |
| A change to the pinned model starts every branch fresh; an upload, an unchanged version and a refused replacement keep the existing tensors. | `inferencer_state_rebinding.feature`. |
| Operators read the pinned model and state progress from any node with values protected; the typed state read returns shapes and pages, honours redaction and branch selection, and works in every client binding. | `inferencer_describe.feature` and `inferencer_state_inspection.feature`; the Bolero targets `inferencer-describe-envelope`, `client-published-processor-sections` and `interconnect-processor-state-inspection`; client conformance through `crates/client-wire/conformance/server_command_inferencer_state.nxsm` and `server_command_inferencer_state_redacted.nxsm`. |
| Publication and tracked locks keep their existing owners: immutable publication uses the primitive publication boundary, and inference adds no cross-location ordering claim of its own. | The workspace Loom models and their qualification over the existing primitives; the packaged smoke suite on the Deloxide diagnostic image for live processes; the inferencer lifecycle, inspection, description and backup workloads declared in `tests/deloxide-inventory.toml`. |

## Limits

The evidence above proves these limits as the contract states them, not stronger guarantees:

- Updates admitted after the newest durable copy a branch continues from are skipped after a
  restart or an owner loss, because inference acknowledges after dispatch.
- A redelivered input invokes the model again, even when the restored state already reflects it,
  and a failover is at least once.
- An owner lost without a replica takes its branch lifecycle with it: every branch starts from
  `INITIAL`, with the resets reported.
- A key that returns after eviction, and every branch after a rebinding to another version, starts
  from `INITIAL`.
- An enforced stop keeps the last snapshot taken before it.

## What the methods do not establish

- Deloxide: the four lanes were not run, under the temporary CI cost policy; the inventory
  validation and the diagnostic-image chaos smoke establish only their declared coverage.
- Fuzzing: the sanitizer runs were focused, bounded and local; no full campaign ran.
- The benchmark is descriptive. It ran in the development profile on a shared host, so its
  throughput and latency figures compare cases within one run, not across hosts.
- Loom: no model is added, because inference makes no memory-ordering claim beyond the primitives
  it uses.
