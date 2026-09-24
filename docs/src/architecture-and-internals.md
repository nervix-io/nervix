# Architecture And Internals

This section explains how Nervix is built and how it behaves internally.

Use it for:

- control-plane and data-plane structure
- domain-clock mapping, authority, lifecycle, progress, and execution-time semantics
- cluster interconnect security, traffic isolation, and delivery semantics
- [consensus durability, replication pacing, log retention, and snapshot recovery](./consensus-storage-and-replication.md)
- [resource versions, pinned bindings, `LATEST` resolution, rebinding, and the HTTPS listener refresh](./resource-versions.md)
- shutdown phases, drain guarantees, and crash recovery
- [integration-test lifecycle: harness deadlines, node startup, teardown, and the suite watchdog](./integration-test-lifecycle.md)
- runtime semantics that are easier to understand from the implementation side
- relay/state internals

If you are looking for NSPL usage, setup, or feature-level guidance, start in the [Manual](./manual.md).
