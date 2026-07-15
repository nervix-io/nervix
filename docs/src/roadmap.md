#  Roadmap

The current roadmap items are:

- Make sure large payloads on the interconnect do not break interactivity, possibly by switching to h2
- FlatBuffers
- nesting in schema
- add explicit drain handoff protocol: pause old consumer while keeping backpressure, force replica state sync from old primary, wait for acknowledgement, then promote the replica
- add a drain concurrency guard/lease so simultaneous drains cannot race while moving the same runtime node or promoting the same replica
- add cucumber coverage for drain race scenarios, including draining a primary while its preferred replica is also draining
- cleanup materialized data of expired relays
- restore all branch-grouped states from the DB, not just read it on demand - connected with proper expiration
- ALTER
- rebalance across cluster
- formalize field reference resolution by context, including `message.<field>`, `<relay>.<field>`, and bare-field forms
- publish a complete codec grammar/EBNF with explicit alternatives for schema-backed and schemaless wire formats
- add operational visibility for in-progress drain operations and failed per-node handoffs
- tighten WASM processor restart/failover scheduling so multi-node restart scenarios do not depend on retry timing or transient resubscription races
- add structured WASM processor diagnostics with resource/version/file, branch key, guest export name, and compile/instantiate/decode/process/timeout/emit failure phase
- define and enforce WASM processor operational limits for memory growth, batch size, timeout fanout, compiled-module cache lifetime, and branch instance cleanup under churn
- cloud interface
- WS commands
- Revise the struct with many Arc's
- docs: Kinesis in Introduction;
- revise START and cleanup of maternialized state
- RACE between creating a subscription and START/STOP domain. also check that it is preserved while switching between nodes

## Before Release
- Check type safety
- Check all expects/unwrap
- Full security research
- Full crash-resistance research
- Publish docker images
- Publish binaries
