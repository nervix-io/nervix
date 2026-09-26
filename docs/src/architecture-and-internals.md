# Architecture And Internals

This section explains how Nervix is built and how it behaves internally.

Use it for:

- control-plane and data-plane structure
- [typed failure ownership, propagation, recovery, and public diagnostics](./errors-and-diagnostics.md)
- [typed absence, semantic states, validation boundaries, state identity, and boundary encodings](./typed-states.md)
- [connector crate ownership, the shared source and sink contract, and host execution](./connector-contract.md)
- domain-clock mapping, authority, lifecycle, progress, and execution-time semantics
- cluster interconnect security, traffic isolation, and delivery semantics
- [consensus durability, replication pacing, log retention, and snapshot recovery](./consensus-storage-and-replication.md)
- [resource versions, pinned bindings, `LATEST` resolution, rebinding, and the HTTPS listener refresh](./resource-versions.md)
- [WASM guest state: checkpoints, generations, resets, rejected-state recovery, and replay](./wasm-state.md)
- shutdown phases, drain guarantees, and crash recovery
- [integration-test lifecycle: harness deadlines, node startup, teardown, and the suite watchdog](./integration-test-lifecycle.md)
- [deterministic interconnect simulation: the Turmoil build mode, fault model, seeded replay,
  failure records, scenario matrix, and CI budget](./interconnect-simulation.md)
- [the expression VM: compilation, columnar execution, kernels and SIMD, function families, window
  aggregates and sketches, and adding a function](./vm-functions.md)
- runtime semantics that are easier to understand from the implementation side
- relay/state internals

If you are looking for NSPL usage, setup, or feature-level guidance, start in the [Manual](./manual.md).
