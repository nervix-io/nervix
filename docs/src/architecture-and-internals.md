# Architecture And Internals

*Plexus, the Nervix graph definition language.*

This section explains how Nervix is built and how it behaves internally.

Use it for:

- control-plane and data-plane structure
- [coverage reports: producer scopes, source mapping, integrity verdicts, advisory scores and the complexity gate](./coverage-reports.md)
- [execution primitives: backend selection, native and browser capabilities, permissions, and verification boundaries](./execution-primitives.md)
- [data-plane concurrency: publication, ownership, ordering, production-owner models, and diagnostic locks](./data-plane-concurrency.md)
- [execution plans: the committed schedule, typed revisions, local binding, publication, and recovery](./execution-plans.md)
- [typed failure ownership, the report model, fixed outcome boundaries, sensitivity, recovery, enforcement, and public diagnostics](./errors-and-diagnostics.md)
- [typed absence, semantic states, validation boundaries, state identity, and boundary encodings](./typed-states.md)
- [connector and entity kinds through statement heads, Models, typed plans and composition](./connector-contract.md#connector-and-entity-kinds)
- [connector crate ownership, the shared ingestor and emitter contract, and host execution](./connector-contract.md#ownership-and-dependency-direction)
- [HTTP emitter request preparation, delivery, lifecycle, and qualification](./http-emitter-architecture.md)
- [client ingestors and emitters: native batches, application ACKs, credits, forwarding,
  restoration, buffered ALTER and paced workflows](./client-io-architecture.md)
- domain-clock mapping, authority, lifecycle, progress, and execution-time semantics
- cluster interconnect security, traffic isolation, and delivery semantics
- [host name resolution: the node's asynchronous DNS resolver, its configuration, cache and bounds,
  the client-library hooks and every call site, residual driver and browser resolution, deployment
  limits, and the evidence behind them](./name-resolution.md)
- [client-to-node communication: the FlatBuffers session protocol, request correlation and
  dispositions, exact recovery, redirect and reconnection, Row subscriptions, uploads, and the
  shared client binding](./client-session-protocol.md)
- [transaction impact planning, scoped quiescence, inspection, and retained outcomes](./transaction-quiescence.md)
- [consensus durability, replication pacing, log retention, and snapshot recovery](./consensus-storage-and-replication.md)
- [resource versions, pinned bindings, `LATEST` resolution, rebinding, and the HTTPS listener refresh](./resource-versions.md)
- [WASM guest state: checkpoints, generations, resets, rejected-state recovery, and replay](./wasm-state.md)
- [backup and restore: the archive contract, quiesced cuts and owner capture, transfer and
  retention, durable restore progress, complete-generation publication, staging maintenance, and
  the evidence behind them](./backup-and-restore-architecture.md)
- shutdown phases, drain guarantees, and crash recovery
- [integration-test lifecycle: harness deadlines, node startup, teardown, and the suite watchdog](./integration-test-lifecycle.md)
- [deterministic interconnect simulation: the Turmoil build mode, fault model, seeded replay,
  failure records, scenario matrix, and CI budget](./interconnect-simulation.md)
- [external cluster chaos: the packaged image under real process, pause and network faults, proven
  fault injection, scenarios and verdicts, seeded replay, artifacts, suites and their CI](./cluster-chaos.md)
- [property testing and fuzzing: registered Bolero targets, current-domain corpora, ordinary
  regressions, sanitizer libFuzzer runs and exact failure replay](./property-testing-and-fuzzing.md)
- [the expression VM: compilation, columnar execution, kernels and SIMD, function families, window
  aggregates and sketches, and adding a function](./vm-functions.md)
- [SIMD kernels: typed buffer ownership, dispatch and build targets, exact results, caller
  catalog, qualification, and measured evidence](./simd-kernels.md)
- runtime semantics that are easier to understand from the implementation side
- relay/state internals

If you are looking for Plexus usage, setup, or feature-level guidance, start in the [Manual](./manual.md).
