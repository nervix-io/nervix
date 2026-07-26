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
- rebalance across cluster
- decide whether any relay-qualified field form beyond `relay_state.<relay>.<field>` belongs in expressions
- explore batched direct external-database access from processing nodes as a possible supported
  pattern for cross-key enrichment; this is a direction, not a committed interface
- add branch-lifecycle metric families: live branch instances per branch declaration, branch
  creations total, and evictions total labeled by `reason=lru|ttl`
- add measurable per-node state-size signals, including deduplication entry counts and open-window
  counts
- keep capacity-planning Prometheus signals branch-aggregated under the existing cardinality
  policy, with branch-local values exposed through `DESCRIBE`; these signals discharge the
  observability half of the capacity-planning contract
- add UDF invocation-count, invocation-latency, and per-row-error metrics labeled by UDF name in
  Prometheus and `DESCRIBE`, following the existing metric label scheme
- grow the Roto column catalog toward builtin parity, prioritizing datetime arithmetic and
  extraction, vectorized string operations such as upper, lower, and split instead of `get` and
  builder slow paths, and `VEC` operations; most of this is mechanical exposure of existing Arrow
  compute kernels
- define the Roto language-tag migration policy before a tag after `ROTO_0_11` ships: side-by-side
  per-declaration tags, a deprecation window, and preservation of the current activation rejection
  for unsupported tags
- publish a complete codec grammar/EBNF with explicit alternatives for schema-backed and schemaless wire formats
- add operational visibility for in-progress drain operations and failed per-node handoffs
- tighten WASM processor restart/failover scheduling so multi-node restart scenarios do not depend on retry timing or transient resubscription races
- add structured WASM processor diagnostics with resource/version/file, branch key, guest export name, and compile/instantiate/decode/process/timeout/emit failure phase
- define and enforce WASM processor operational limits for memory growth, batch size, timeout fanout, compiled-module cache lifetime, and branch instance cleanup under churn
- cloud interface
- WS commands
- Revise the struct with many Arc's
- revise START and cleanup of maternialized state
- RACE between creating a subscription and START/STOP domain. also check that it is preserved while switching between nodes

## Before Release
- Check type safety
- Check all expects/unwrap
- Full security research
- Full crash-resistance research
- Publish docker images
- Publish binaries
