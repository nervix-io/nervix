# AGENTS.md

## Purpose

Nervix is a domain-owned streaming system configured with Plexus, the Nervix graph definition
language. This file states the repository's durable architecture and correctness rules, each once.
The module tree, the Plexus grammar and the details of each contract belong in the code, the tests
and the `docs/src` chapter that owns the contract ([Authoritative
Chapters](#authoritative-chapters)).

Every rule binds every change from the moment it lands, including work in flight on other branches
and in other epics. When code and this file disagree, resolve the owning model and invariant
directly; never add a compatibility path around the disagreement.

## Sources of Instruction

Instructions come from three sources and no other: this repository (this file, the chapters under
`docs/src`, the skills under `.agents/skills`, and checked-in configuration and tooling); ClickUp,
whose docs, epics, tasks and comments define the work and hold its records; and the human directing
the session.

Work on this repository is tracked in the ClickUp workspace `90141361959`, in the list Product
Development (`901417478428`). Every task of this repository carries the **Component** custom field
value `Nervix`. An epic that spans Nervix and the licensing service in `nervix-io/license-server`,
such as [EPIC: Licensing](https://app.clickup.com/t/86bczaykq), lives in the same list, and each of
its tasks carries the Component value of every repository it changes: `Nervix`, `License Server` or
both.

The licensing service consumes `nervix-licensing-protocol` and `nervix-crypto` from this repository
by pinned git revision. A protocol change lands in Nervix first, and the service then moves its pin
to a revision that contains it; the service never copies either crate or defines the protocol a
second time.

An agent keeps no local memory: no notes, preferences, conventions, decisions or facts stored
outside the repository and ClickUp between sessions, whatever the tooling calls the store (a memory
directory, a saved profile, a scratch file). It writes nothing into one and acts on nothing one
holds; memory recalled into a session unasked is neither an instruction nor a fact, because no one
else can review, correct or share it. A convention worth keeping goes into this file or its owning
chapter; a decision, finding or constraint of a piece of work goes on its ClickUp task or the
ClickUp doc that owns it. What the human states governs the session unless they ask for it to be
recorded in one of those places.

## Alpha Stability and Compatibility

Plexus is in alpha: large incompatible changes are no longer expected, but focused breaking changes
are still made when they materially improve its model or correctness.

Nervix supports exactly one shape of everything, the current one, with no migration, dual read,
version negotiation or deprecation window until an explicit compatibility policy exists. When a
Model, stored shape, Plexus form, protocol message or public interface changes, the previous form is
deleted in the same change. Data in a previous stored shape fails to load with a clear error and is
recreated, never reinterpreted, defaulted into the current shape or routed through a second code
path.

These are defects to reject:

- Types, variants or fields that represent a removed or earlier form, however spelled:
  `RemovedTransport`, `LegacyFoo`, `FooV1`, `OldBar`, `CompatBaz`.
- Decoding, `TryFrom` or deserialization arms that name an older shape, even only to reject it.
- Fields retained, or new fields defaulted, so that previously written data still loads.
- Fallbacks, feature gates or configuration that select between an earlier behavior and the current
  one.
- Identifiers containing `legacy`, `old`, `deprecated`, `compat`, `removed` or a bare version
  number, in types, fields, variants, modules or functions.

Tests cover the current shape only. A test never defines a removed Model, stored shape or wire form,
not even to assert that it is rejected, and removing behavior or output removes the assertions that
named it instead of replacing them with assertions that the old form is absent, rejected or no
longer emitted.

Removing an integration or feature removes all of it in one change, leaving no placeholder for old
data: Models, grammar, stored shapes, runtime paths, configuration, documentation and tests.

The only exceptions are an external contract whose pass-through is the intentional public behavior,
and a compatibility requirement the user states explicitly for the current change.

## No Regressions

A change never leaves Nervix worse than it found it. A regression is any property the change was not
asked to change and leaves worse: behavior, correctness, guarantees, failure handling, performance,
latency, resource use and its bounds, the number, size or layout of what the product writes, stores
or sends, observability or operability, public or internal, whether or not a test, scenario, metric
or document observes it.

Knowingly introducing a regression is never allowed, not as a trade-off, a temporary state or a
known limitation. No justification makes it acceptable: not that the change is a port, refactor,
move, extraction or migration; not that its target contract, abstraction or design cannot express
the current behavior; not that the property is internal, minor, unobserved or out of scope; not that
keeping it makes the change larger. Removing or changing behavior as the stated purpose of a change,
under [Alpha Stability and Compatibility](#alpha-stability-and-compatibility), is not a regression;
a degradation that only makes another goal easier always is, and no task, epic plan or pull request
can redefine it as a purpose.

- A change that replaces a mechanism preserves every property of the old one, not only what tests
  assert, and its pull request names the properties preserved beyond the tests and how they were
  verified.
- When the target cannot express what the current code does, the change extends the target instead
  of giving up the property.
- A regression is fixed in the change that introduces it, before it lands, however small; until then
  the task is unfinished. It is never recorded instead of fixed: a follow-up task, known-limitation
  note, code comment or documentation sentence only shows it was known.
- An existing regression is a defect, not a baseline: fix it or record it as a defect on ClickUp,
  and never build on, extend or widen it.
- When preserving one property appears to require degrading another, do not choose: stop and report
  the conflict to the human directing the session before writing the change.

## Architecture

### Layer order

Nervix is layered. Innermost first, each layer may name only the layers inside it:

1. **Primitives.** Self-contained data structures and conversions that name nothing in Nervix,
   including `nervix-primitives`, the boundary for every [execution-sensitive
   primitive](#execution-sensitive-primitives).
2. **Vocabulary.** Models, names, timestamps, branch keys and node references: the words every other
   layer speaks.
3. **Language.** Plexus lexing, parsing, completion and lowering into Models. The parser is an edge
   dependency named only by the language crate, the formatter, the client tools and the session
   adapter; every other layer consumes Models.
4. **Engines and infrastructure.** The expression VM and its frontend, the UDF and WASM hosts, wire
   codecs, the connector contract and crates, consensus, the interconnect, gossip, the state store
   and the resource store. An engine is driven by its caller and decides nothing about the graph.
5. **Decisions.** Registry validation, placement, scheduling and planning: synchronous pure
   functions from typed inputs to a typed outcome, with no Tokio types, locks or shared maps in
   their signatures, unit-tested directly from those inputs.
6. **Data plane.** Per-node execution: relays, branch-local processor tasks, the connector host,
   materialized state and acknowledgement tracking. It applies decisions and executes plans.
7. **Control plane.** Transactions, domain lifecycle, applying schedules, observation,
   subscriptions, resources and cluster coordination.
8. **Edges.** The session service, HTTP endpoints, the cluster API, metric exposition, the web
   console and the client tools.

Dependencies point inward only. Test and benchmark harnesses sit outside the order, may name any
layer, and are never named by product code.

Each stage has one representation: text becomes a Model, a Model a validated node, a validated node
an execution plan, and a plan running tasks. Each stage converts once at its boundary and hands on a
type that carries the guarantees it is responsible for; the data plane executes plans and never
reads a `Model`. Identities are typed once and shared: a second key type for an identity that has
one is a duplicate to remove.

Every crate and layer module opens with an ownership contract in its `//!` documentation: its layer,
then three lines naming what it **owns**, what it may **depend on** and what it **must not know**,
from which alone a reviewer decides whether a change belongs there. The contract states the target,
not the present: where code contradicts it, the header names the contradiction and the layer it
violates instead of softening the contract to match today's imports.

### Language pipeline

- Plexus is lexed and parsed once into structured public semantic Models, which the compiler
  consumes directly; spans and recovery state stay internal to parsing and diagnostics. There is no
  second public parser AST, raw-expression field or runtime parser adapter, and no persisted or
  public Model holds raw executable Plexus for the runtime to reparse. External connector
  configuration stays raw only where pass-through is the intentional public contract.
- Completion derives from the expectations of the composed top-level grammar; REPLs and clients
  never merge suggestions from independent feature parsers.
- Statement families own their domain grammar. Shared lexical and grammatical concepts have one
  implementation, and semantic reference kinds and repeated patterns use focused shared parser
  primitives, never ad hoc word parsers or duplicated statement-local grammar.
- Keywords and type literals are case-insensitive tokens of the shared language token definition,
  never grammar-local string matches or undeclared words; original spelling may be kept for
  diagnostics. Multi-word logical keywords are composed grammar units that parsing and completion
  treat as one item.
- Grammar labels use underscore style (`field_name`, `string_literal`); completion labels of
  composed keyword phrases use their space-separated human-readable form.
- A `CREATE` head names the entity's kind as one composed keyword phrase and selects that kind's
  own grammar and semantic Model. A kind never enters a generic statement through `TYPE`,
  `FROM <KIND>` or a grammar-selecting `FORMAT` clause, and a combination the selected kind cannot
  express never waits for registry rejection. An alterable property belongs in the body and pairs
  with its corresponding `ALTER` operation, never in the head. A supported kinded `ALTER` repeats
  the stored kind; addressing statements name the Model kind alone unless the kind is part of the identity,
  as with wire schemas. Connector heads name connector and entity even for a single-entity
  connector; client heads name the connector. Shared tokens and vocabulary declarations compose
  heads for parsing and completion. [Statement heads and body
  properties](docs/src/architecture.md#statement-heads-and-body-properties) owns the contract,
  existing body-format choices and the checklist for adding a kind.

### Control plane and runtime

- Models and execution-graph configuration are strongly persisted control-plane state. Records,
  batches, payload attempts, handoff state, ACK guards, ACK tokens and ACK maps are in-memory
  hot-path state, never persisted. Selected execution state may use node-owned snapshot or
  replication mechanisms.
- The registry validates domains, references, schemas, branches, capabilities and execution
  contracts before a graph becomes active.
- Gossip carries only membership-scoped state: identities and endpoints, prerequisites, admission
  verdicts, cluster parameters, termination and subscription interest. Application progress stays
  in one incarnation-bound local record and travels by typed interconnect reports and queries;
  no per-command or per-revision progress fact becomes a gossip key. An application revision barrier
  judges progress only from its local record, received reports and query replies, under its fixed
  revision, deadline, leader tenure and required-incarnation checks. Availability determines its
  participants, never their readiness.
- A membership event reports only a node joining, leaving or changing incarnation. A live-state
  key-version change still refreshes interests, prerequisites and routes without offering a
  membership event. Every server notice carries its semantic kind, and clients and bindings route
  notices by that kind; notice text is display-only.
- Runtime execution is materialized per concrete branch; branch-local state, scheduling, buffering
  and materialized views stay visibly branch-local in types and ownership.
- Server endpoint definitions and bound intakes form one immutable host-and-path publication. A
  request resolves it once, and a WebSocket connection keeps its selected route and signaling
  protocol. An ingestor's close or drop ends its exact intake lifetime before the binding is withdrawn,
  so retained routes reject further intake and cannot attach to a replacement ingestor; a request
  admitted before that ending may finish through the intake it borrowed.
- Every server-side runtime entity that binds a configured listening port runs on every live node,
  with a listener independent of leadership and placement that stays present across every cluster
  event: leader changes, node joins, node restarts and recovery.
- A consensus append-stream adapter consumes the caller's first request before attempting peer
  admission, so the caller's retry and cancellation gates also govern immediate failures. Leading
  empty probes use management capacity, where an unconfirmed first-data prefix is checked before
  data admission under the setup deadline of its ordered append stream; a rejected or incomplete
  prefix ends that input with its exact result. Admitted requests are submitted intact exactly once
  on their owning path, later empty commit updates share the data stream, and ended input opens no
  transport.
- Connectors adapt external systems at explicit data-plane boundaries and never weaken internal
  schema, branch, error or sensitivity rules.

### Client host bindings

- A host-language binding is a Rust crate on its host's Rust binding library: pyo3 for Python,
  magnus for Ruby and `jni` for the JVM. Each calls the shared host runtime over the Rust client.
  The C ABI serves C and C++ only; hand-written ctypes, Fiddle, Foreign Function and Memory or
  cgo declarations of that surface for another host are a change to reject.
- A new binding or probe ships with its conformance-ledger row, its one- and three-node examples
  in all three core conformance outlines, its registered Bolero batch property and the
  `client-conformance` PR label. [Client Host Bindings](docs/src/client-host-bindings.md) owns
  the boundary and qualification contract.

### Connector crates

Every external integration is its own crate under `crates/connectors/` implementing the ingestor
contract, the emitter contract or both from `nervix-connector` in `crates/connector`. [Connector
Crates And The Connector Contract](docs/src/connector-contract.md) owns the contract and its
[hygiene](docs/src/connector-contract.md#connector-hygiene-standard) and
[delivery](docs/src/connector-contract.md#connector-delivery-standard) standards with their review
checklists.

- A connector is an engine the host drives. The host, the server's data plane, owns task lifecycle,
  branch routing, acknowledgement tracking, quiesce, retry and flush cadence, buffering, metrics and
  events; the connector owns its driver, configuration interpretation, header semantics and
  per-record outcomes and decides nothing about the graph. It reads no Model, only the typed plan
  the decision layer converts once, and only the server, the composition root, names every
  connector.
- Models, plans and runtime composition nest connector, then entity. Every entity, a second one
  included, has its own plan variant and concrete connector type; no mode flag or shared private
  mechanism replaces an entity boundary. The type declares its vocabulary kind
  (`IngestorConnector::KIND`, or the emitter's `KIND` and object-safe `EmitterLifecycle::kind()`),
  and the composition root asserts plan-to-implementation kinds at compile time.
- Every connector and entity fact a statement can name lives in its vocabulary declaration:
  reference kind, external locator and name rule, ordered options and required choices, delivery
  and publishing modes, quiesce, header model, typed metadata, envelope clauses, batching, bounds
  and waits. Grammar, registry, planning, completion, console and documentation consume that
  declaration; the host executes its typed projection in the plan. No reader keeps a second case
  table or default, and the registry names no connector crate
  ([Entity declarations](docs/src/connector-contract.md#entity-declarations) owns the checklist).
- Plexus writes only the modes each entity declares, with `MODE NO ACK`, `MODE ACK SEQUENTIAL`,
  `MODE ACK PARALLEL MAX <n>` or a request-response `MODE ACK` as appropriate. Transport levers
  are entity options, not mode variants. External locators are string literals validated by the
  entity and sent with their spelling intact; Nervix references stay typed identifiers.
- Every client declares its `CONFIG` keys, types, required/default and confidential values, and
  whether Nervix or a named driver namespace reads them. `CREATE` and `ALTER` validate
  Nervix-owned keys and run available network-free driver checks before storage; only declared
  driver pass-through remains raw.
- Connector names come from `ConnectorKind`'s display and entity names from their declarations, and
  every label and prefix (configuration errors, transports, metrics, logs, dataflow identifiers,
  `DESCRIBE`) derives from them, with no other spelling or case table; external protocol and
  pass-through names keep their contract. Use ingestor and emitter for transport boundaries, never
  sink or source in that sense.
- Errors keep typed causes (`thiserror`, `error-stack`) until the reporting boundary, never driver
  `Display` text or free-text reasons, and no `nervix::error_boundary` claims a record rejection the
  path does not perform.
- Every publish, window or batch, header and metadata mapping and acknowledgement path has unit
  coverage through its production owner, and every connector-owned encoding or encoded-size measure
  a registered, bounded Bolero property (shared implementations keep theirs with their owner).
  Generated grammar and completion conformance tests read the entity declaration; every declared
  runtime capability has a registered public scenario, checked by
  `just validate-connector-capabilities`. The generated capability matrix is checked against the
  declarations by `just validate-connector-matrix`. Unit tests never replace required scenarios
  or concurrency evidence.
- Every buffer between transport and host is declared in the entity's `DeclaredBounds` with capacity
  and overflow, and every network wait with what ends it, unbounded ones included. A change adds no
  unbounded buffer and no wait only the host's stop ends, and an existing one stays declared
  unbounded until bounded. Client deadline keys (`timeout_ms`, `connect_timeout_ms`) keep their one
  contract meaning and are read through `ConfiguredDeadline`, no thread-parking driver call runs on
  an async worker, and every deadline key an entity's waits name has its `STALLED PEER` scenario in
  the capability inventory ([Bounds and
  deadlines](docs/src/connector-contract.md#bounds-and-deadlines)).
- A connector loses, duplicates or stalls nothing beyond its declarations. An ingestor entity declares
  the position class and acknowledgement confirmation its transport gives, and `acknowledge` returns
  only once that confirmation arrives, so progress, drains and handoffs wait for the transport. A
  cancelled read returns every message it took, and a payload the codec refuses is consumed as a
  decode rejection through the ingestor's general error policy, never presented again. Every emitter
  implements `finish` for what its client holds, by the host's deadline, and a brokered `MODE ACK`
  emitter awaits its broker through the shared `ConfirmationWindow`, so a retry never resends a
  confirmed record. A delivery defect is fixed red first: a public scenario through the real
  transport and, for an interleaving, a Shuttle check over the production owner.
- Before closing a connector epic, review landed parallel work against both standards and record
  evidence and compliance tasks on ClickUp. A connector change keeps the contract chapter, the
  relevant manuals and the ratchet current in the same change; `just ratchet` and the clock-boundary
  check treat connector crates as data plane.

### Execution-sensitive primitives

`nervix-primitives` in `crates/primitives` selects every execution-sensitive primitive for the
build's mode: ordinary, `shuttle`, `loom`, `turmoil` or the `deloxide` diagnostic mode. It sits
below the vocabulary so every layer shares it, and callers state the operation they need, never a
backend. [Execution Primitives](docs/src/execution-primitives.md) owns the surface, what each mode
observes and the permitted real primitives. `just validate-primitive-boundary`,
`just validate-execution-mode-dependencies` and `just validate-execution-mode-conflicts`, in
`just validate` and `just validate-ci`, enforce the rules below; Clippy resolves re-exports and
cannot police import paths, so it keeps only genuinely forbidden types and operations.

- **Import from the boundary**, in all Nervix-authored Rust: tests, harnesses, benchmarks, examples,
  authored macros and the `tools/nervix-lint` workspace included, Cargo build output excluded.
  Atomics, `Ordering` and `fence` come from `sync::atomic`; `Arc` (triomphe's, the default for
  Nervix-owned state), `StdArc` and `StdWeak` (only for a weak reference or an external API that
  needs the standard type, confined there), async synchronization, waker registration and
  cancellation tokens from `sync`; thread-blocking locks with `parking_lot`'s interface, condition
  variables, barriers, one-time initialization and synchronous channels from `sync::blocking`;
  tasks, the cooperative budget and task tracking from `task`; timers and monotonic instants from
  `time`; sockets from `net`; the runtime from `runtime`; channel streams from `stream`; atomic
  reference publication from `publication`; concurrent maps and queues from `collections`; threads
  and thread-local storage from `thread` and `thread_local!`; deadlock detection from `deadlock`;
  async tests and mains from `#[nervix_primitives::test]`, `#[nervix_primitives::main]` and
  `nervix_primitives::select!`.
- **Never import the backends**, however spelled (renamed, grouped, qualified, globbed, through a
  renamed `std`, `core` or `sync`, a selecting alias, a macro body or an inactive `cfg` branch): the
  equivalent items of `std` (its `Arc`, `Weak`, locks, threads, `Instant` and sockets included),
  `core`, Tokio's `sync`, `task`, `runtime`, `time` and `net` with their attributes and macros,
  Tokio Util's `sync` and `task`, Tokio Stream, Turmoil's `net`, the `futures` crates' channels,
  locks, executors, atomic waker, `select!` and abort handles, Shuttle's and Loom's primitives, the
  Shuttle wrapper crates, Deloxide's locks and detector, and `parking_lot`, `dashmap`,
  `concurrent-queue`, `arc-swap`, `triomphe` and `atomic-waker`. Only `nervix-primitives` depends on
  the libraries whose families it selects, and no manifest, tracked or new, renames a governed
  crate. Not governed, and real in every mode: `Duration`, Tokio's I/O traits and its filesystem,
  process and signal modules, and the pure polling combinators of Tokio and `futures`. Import
  boundary items by name: reimported aliases stay confined, and a root glob is rejected.
- **Mechanisms, never policy.** Names resolve through the node's resolver in `nervix-dns`;
  `tokio::net::lookup_host` and `ToSocketAddrs` are rejected, and the boundary's own lookup exists
  only in the Turmoil build. Timers grant no clock permission: UTC and physical deadlines keep the
  owners `scripts/check_clock_boundaries.py` declares, logical deadlines stay in the domain clock's
  coordinate, and only tests of elapsed-time behavior enable the `test-util` paused clock.
- **The bounded executor.** Work off the async workers goes through `nervix-execution` with its CPU
  or storage class, a charged memory class and `Cancellation` checks between bounded units; operator
  code the node cannot bound (UDF calls, JAQ transformations) takes the extension class and password
  hashing the credentials class. A refusal is a typed error the caller maps to its own outcome, and
  one that judged nothing stays retryable, never a decode, encode or authentication failure. A full
  queue refuses work that answers a request; accepted work nothing can present again (a payload a
  quiesce buffer retained, a poll a paced ingestor moved past) states
  `QueueAdmission::WaitForPlace`, and its owner lets shutdown and a new quiesce end the wait and
  keeps or re-decides the work. A payload handed over without an acknowledgement waits only when
  holding the ingestor's loop keeps its transport connected with bounded memory; otherwise its refusal
  is reported and counted. `task::spawn_cpu` belongs to the executor's worker pools,
  `spawn_blocking` to its storage workers, and `block_in_place` to no file by default.
- **Exact permissions.** Any other file naming `spawn_blocking` or `block_in_place`, or creating a
  thread through `thread::{spawn, Builder, scope, spawn_detached}` or `unmodeled::thread::Builder`,
  lists exactly those items in `crates/primitives/blocking-permissions.toml` with its owner, why it
  stays outside the executor and what bounds its work (for threads also their count and lifetime). A
  real primitive kept outside every model comes from `nervix_primitives::unmodeled`, listed in
  `crates/primitives/unmodeled-permissions.toml` with its one file, items, owner, why it must be
  real and what that leaves unverified; the real thread builder needs both permissions. An unmodeled
  primitive may keep runner statistics, serve a thread no model runs, hold once-initialized process
  state, satisfy an external API's type or record observations without a scheduling point, and never
  carries the protocol under test, chooses its branches, supplies its wakeups or orders what an
  assertion relies on; the browser console takes its `futures` channel, `select!` and abort handles
  from `unmodeled::futures` this way. Nothing is exempt, neither tests, models, harnesses,
  `cfg(test)` modules, inactive branches nor authored macros, and unlisted uses, unused entries,
  nonexistent items and directory, glob or ungoverned-file entries all fail.
- **No ambient atomics.** A selected atomic belongs to the model execution that constructs it: never
  in a `static`, directly or wrapped, or a `thread_local!`, and never built in a const context,
  which Loom does not support. Process-wide state moves onto the owner whose lifetime it has, such
  as a node's service or its Raft network, a count a unit test reads is kept per thread, and state
  that outlives every test and model is a real atomic under a permission. The boundary's own statics
  hold real atomics too, except a shared queue whose Shuttle check needs selected atomics, held so
  only in code a `deloxide` build alone compiles.
- **Modes are features, never global cfgs.** A bare `loom`, `shuttle`, `turmoil` or `deloxide` in a
  `cfg` predicate, or `--cfg` with one in any recipe, Cargo configuration, workflow or build script,
  is rejected; only a Turmoil recipe passes `--cfg tokio_unstable`. The analysis cfg `nervix_lint`
  is set only by the synchronization analysis driver, selects nothing, and gates nothing but
  `nervix::` contracts and lint levels. At most one mode is enabled per dependency graph, any pair
  failing to compile with a diagnostic naming both; selection reads features only, never
  `cfg(test)`, never falls back from a modeled primitive to a real one, and fails to compile an
  operation the mode cannot provide. A modeled primitive used outside its model is a test
  configuration failure. Loom models only atomics, its threads and thread-local storage, and
  Loom-only model code names no other family but shared ownership and permitted unmodeled
  primitives. Shared ownership is real in every mode, so no claim relies on reference-count
  ordering, and no model checker simulates a network: under Shuttle the sockets are Tokio's, and a
  socket created inside a check panics and fails it.
- **Deloxide models nothing.** It replaces only the thread-blocking locks and their condition
  variable, with tracked adapters keeping their interface where Deloxide keeps its meaning (a
  `Debug` that never waits, the exact count of waiters a notification wakes); anything outside that
  surface fails to compile. Its detector installs once per process, through `nervix-deadlock`'s
  diagnostic run, after signal registration and before any tracked lock or runtime worker exists; an
  earlier tracked lock panics, and nothing reinstalls or reconfigures it.
- **Dependencies and builds.** A package owning a mode feature depends on `nervix-primitives`
  directly and forwards the mode to it and every workspace dependency owning that mode, through
  dev-dependencies when it has only test targets. Ordinary and modeled suites build in separate
  invocations with explicit features, and a workspace-wide `--all-features` excludes mode-owning
  packages. Only `nervix-primitives` selects Loom and only `nervix-model-harness` runs Loom models
  and Shuttle checks, both taking the model checkers as optional dependencies, and no other package
  depends on `loom`; only `nervix-primitives` depends on `deloxide`, and only optionally. A package
  whose harness drives a simulation takes Turmoil only through its own optional `turmoil` feature;
  ordinary graphs, with or without default features, contain no model checker, simulator, modeled
  wrapper or detector and enable no mode or paused clock. Ordinary builds re-export library items
  directly, adding no allocation, wrapper, dispatch, lock, reference count or scheduling point, and
  adapters exist only in modeled or diagnostic builds, keeping library semantics and stating what
  their mode observes. Atomics and shared ownership are portable to the browser and every other
  family is the `native` capability; a capability or mode the target lacks fails with the boundary's
  diagnostic first, and the vocabulary and browser graphs contain no async runtime or network
  library. Release-image binaries declare `nervix_primitives::product_binary!`, which fails in a
  modeled build; a `deloxide` build is a never-released diagnostic artifact whose only binary,
  `product_binary!("nervix-server", diagnostic)`, starts its diagnostic run at start-up.
- Guest code, the WASM guest SDK and the guests built on it, runs inside a user's WASM guest where
  no mode exists and is outside the source rules; a user's guest never takes Nervix's boundary. A
  new execution-sensitive primitive joins the boundary before any caller uses it, and no change adds
  a bypass.

### Migration discipline

Mechanical moves and behavior changes never share a commit: a move-only commit changes no behavior
and is verified by the build and the existing tests. A port or migration that changes how code works
preserves every property of what it replaces ([No Regressions](#no-regressions)), and architecture
debt only decreases ([the ratchet](#the-ratchet)).

## Authoritative Chapters

Each chapter below is the authoritative reference for its area, and any change within that area
keeps the chapter current in the same change. Where areas meet, each chapter keeps its own contract.

- [Architecture Overview](docs/src/architecture.md#statement-heads-and-body-properties): the
  language-to-Model boundary, statement heads and body properties, kind-preserving alterations,
  addressing identities and adding a statement kind.
- [Connector Crates And The Connector Contract](docs/src/connector-contract.md): a connector crate,
  the shared connector contract, connector planning or composition, host ingestor and emitter
  execution.
- [Emitter Batching](docs/src/emitter-batching.md): compatible-row packing, exact encoded-size
  boundaries and subdivision, retained payload membership and outcomes, batching observation units
  and the scope of measured tuning evidence. The connector contract retains the general host and
  engine boundary; the Emitters and Schemas And Codecs manuals own public clauses and containers.
- [Cluster Interconnect](docs/src/interconnect.md): interconnect code and every node-to-node
  operation, including application progress reports, queries and completion-participant views.
- [Name Resolution](docs/src/name-resolution.md): `nervix-dns`; how a node or a native client loads
  its resolver; a path that resolves a host name, or the hook, budget or dialling policy it uses; a
  dependency feature that selects a resolver; the DNS checks and fixtures.
- [Client Session Protocol](docs/src/client-session-protocol.md), and its normative companion the
  [Client Implementation Manual](docs/src/client-implementation-manual.md) whenever what a client
  implementation must do changes: client protocol code (the session schema and wire crate, the
  session service and its transports, the Row encoder, the Rust client, the shared host runtime
  and binding crates, and the session handling of the web console and the CLI) and every
  client-to-node operation.
  This includes server-notice kinds and their routing through every client and binding.
- [Client Host Bindings](docs/src/client-host-bindings.md): a binding crate, the shared host
  runtime, a host probe or the conformance ledger. Every change to one keeps this chapter current
  in the same change.
- [Errors And Diagnostics](docs/src/errors-and-diagnostics.md): error types, error or diagnostic
  propagation, failure classification, reporting at a layer or public boundary.
- [Consensus Storage And Replication](docs/src/consensus-storage-and-replication.md): consensus
  durability, replication pacing, log reading or retention, snapshot storage or transfer, the
  dedicated consensus database.
- [Transaction Quiescence And Impact Inspection](docs/src/transaction-quiescence.md): transaction
  operation or step impact, affected topology, preview identity or freshness, actual engagement,
  report persistence, inspection presentation.
- [Domain Clock](docs/src/domain-clock.md): the domain-clock architecture, domain architecture that
  changes how time is established, propagated or consumed, and how a client session attaches to and
  follows a domain clock, including a receiver node's attachment to its clock authority.
- [Execution Primitives](docs/src/execution-primitives.md): the primitive boundary's governed
  families, backend selection, feature propagation, target capabilities, authored-source scope,
  permissions, build contracts, and the distinction between API, graph, compiler and execution
  evidence.
- [Data-Plane Concurrency](docs/src/data-plane-concurrency.md): hot-path state publication, task or
  branch ownership, delivery or assignment fences, the typed ratchet's synchronization rule and its
  execution contracts, a Shuttle check or Loom model, and the diagnostic mode's deadlock detection
  and its evidence. A change that adds a concurrent map, or changes how often one is reached or what
  disposes of it, also updates `tests/concurrent-map-inventory-ledger.md`.
- [VM Functions](docs/src/vm-functions.md): expression lowering, the semantic catalog and function
  registration, type or sensitivity checking, constant folding or expression sharing, compiled
  programs and their prepared artifacts, the runtime bridge and execution context, scalar operands,
  selected-row execution of conditional arms, row and batch errors, kernel selection and SIMD use,
  function-family implementations, injected functions, window aggregate and sketch structures.
  [Expression Functions](docs/src/filter-map-functions.md) owns every public function contract.
- [SIMD Kernels](docs/src/simd-kernels.md): kernel ownership, dispatch or build targets, arithmetic
  or bitmap contracts, byte scanning, typed run execution, scalar/level qualification, SIMD
  performance claims.
- [Wire Schema Binding](docs/src/wire-schema-binding.md): a wire format's vocabulary and its Models,
  how a wire type binds internal types, codec `ENCODE` rules, a schemaful codec's decoder, encoder
  or the value model it decodes through, value fidelity, and the stored shapes and canonical text a
  wire type reaches. [Schemas And Codecs](docs/src/schemas-and-codecs.md) owns the statements and
  the public binding table.
- [Resource Versions And Bindings](docs/src/resource-versions.md): the resource catalog, upload
  installation or replication, version resolution, how a binding is validated, classified, pinned or
  loaded, `REBIND RESOURCE`, the HTTPS listener's certificate installation.
- [WASM State And Recovery](docs/src/wasm-state.md): what a guest save holds, the checkpoint that
  completes a guest callback and the acknowledgements it holds back, local or replica durability of
  guest state, state generations and the events that advance them, ownership fencing or forced
  recovery of guest state, coordinated, guest-requested or rejected-state resets, the guest save,
  restore and reset ABI, how rebinding affects guest state.
- [Backup And Restore Architecture](docs/src/backup-and-restore-architecture.md): the archive crate
  or format; how a backup cuts a domain or captures, stages, transfers, assembles, retains or serves
  state; how a restore receives, prepares, plans, admits, records or resumes its steps; how restored
  state is converted, staged, published, gated, reclaimed or resumed. [Backup And
  Restore](docs/src/backup-and-restore.md) owns the statements, the command-line and console
  surfaces, the archive's section and record tables, the operator limits and the reported failures.
- [Shutdown And Recovery](docs/src/shutdown.md), the single canonical shutdown chapter, which
  shutdown documentation extends rather than adding a competing page: shutdown phases, the shutdown
  or drain deadline, termination signals, terminating placement eligibility, intake stop, graph
  drain and force flush, terminal teardown, startup recovery.
- [Typed States And Validation Boundaries](docs/src/typed-states.md): how a missing value, state
  variant, required identity, conversion failure or boundary representation is modeled or validated.
- [Node Settings](docs/src/node-settings.md): setting declarations, defaults, derivations,
  fixed-value contracts, validation, carriers, change procedures, capability sizing, settings
  enforcement.
- [Cryptography](docs/src/cryptography.md): how Nervix obtains a TLS provider, a digest used for
  security, a random value or a password or key derivation; a non-approved hash or the purpose a
  site records for it; a dependency that brings cryptography into the `nervix-server` product graph;
  the `non_aws_lc_crypto` count.
- [Execution Plans](docs/src/execution-plans.md): revision construction; typed node, resource,
  ingestor, emitter, processor or message-error plans; schedule-delta application; graph publication;
  plan-based recovery.
- [Architecture Rules And The Debt Ratchet](docs/src/architecture-enforcement.md): the contract
  matrix, source contracts, analysis toolchain, workaround audit and ratchet summarized in
  [Architecture Rules and the Debt Ratchet](#architecture-rules-and-the-debt-ratchet).
- [Coverage Reports And The Complexity Gate](docs/src/coverage-reports.md): coverage producers and
  exports, source mapping and exclusions, ordinary report ownership, merge records and integrity
  verdicts, patch coverage, the advisory CRAP report, the full workspace union, the mandatory
  complexity count.
- [Property Testing And Fuzzing](docs/src/property-testing-and-fuzzing.md): the Bolero target
  register and documentation-only CI selection.
- [Integration Test Lifecycle](docs/src/integration-test-lifecycle.md): how the harness starts,
  observes, diagnoses or stops in-process nodes, server processes, scenarios or the whole run; a
  harness budget or the measurement it is derived from; the CI job that runs the suite.
- [Deterministic Interconnect Simulation](docs/src/interconnect-simulation.md): the `turmoil` build
  mode or its dependency and scheduler-conflict checks; the socket, certificate-clock, entropy or
  CPU-execution seams the simulation plugs into; the runner, the scenario driver, failure records
  and replay; a simulation scenario or its committed seeds and bounds; the Turmoil recipes or the CI
  job that runs them.
- [External Cluster Chaos](docs/src/cluster-chaos.md): `scripts/chaos`, its Compose deployment,
  fixtures, tool image pins, verifiers or self-tests; the chaos recipes in the `justfile`; the
  `Chaos` workflow or the chaos and diagnostic-image jobs of the `Docker Build` workflow. The
  product contracts its scenarios verify stay with their own chapters.
- [Developing Nervix](docs/src/developing-nervix.md#ci-labels): every label that changes what CI
  runs.

## Language and Model Invariants

### Structured semantics

- Every executable expression is a structured Model: filters, assignments, inheritance, `VALUES`,
  invocations, ordering, deduplication keys, correlations, window expressions, inferencer mappings
  and error mappings.
- Models preserve every semantically significant order, including routes, assignments, invocations
  and materialized-state dependencies.

### Types and sensitivity

- All schemaful operations are exact-type operations. Implicit casts, parsing, stringification,
  numeric widening or narrowing, datetime/string interchange and wire/internal coercions are
  forbidden.
- Nullability is part of the type contract: required uninitialized fields are errors, and optional
  uninitialized fields finalize as typed nulls.
- Sensitivity may be promoted but never downgraded implicitly. External emission of a sensitive
  value requires explicit leakage, including codec payloads, direct `VALUES`, inherited fields and
  written headers.
- Multiple ordinary inputs to one processor reference the same declared schema Model, and they share
  the exact named branch or are all unbranched. Correlators apply the same rule independently to the
  left and right sides.

### Wire schemas

- A wire type names a representation its format actually has, never a Nervix type, and a format
  declares no wire type that no internal type can bind.
- A wire type binds exactly the internal types whose every value its representation carries and
  returns unchanged. The exact-type rule above admits no other binding: AVRO `int` binds only `I32`
  and `long` only `I64`.
- A text interchange of a non-text internal type, a `DATETIME` as RFC 3339 text or `BYTES` as
  base64 text, is an explicit `ENCODE` rule on the codec. The rule declares the representation its
  pair of wire type and internal type already fixes and selects no alternative; a value the format
  carries natively takes no rule.
- An AVRO array declares its item type, `array<item>`, nested exactly as deep as the internal
  `ARRAY` and `VEC` levels, while JSON and CBOR arrays stay bare. Every element binds by the scalar
  rule of its innermost type, under the list field's `ENCODE` rule.
- A format's decoder and encoder work from the validated pair of wire type and internal type, and
  never read or write a Nervix type name in a payload.
- A change to a wire type enum or to the encoding rules changes the registry's Model archive header
  and the consensus keyspace namespace in the same change. A new wire type or rule lands with its
  binding-matrix entry, a public scenario and its generator, as the checklist in [Wire Schema
  Binding](docs/src/wire-schema-binding.md#adding-a-wire-type-or-an-encoding-rule) sets out.

### Construction

- Construction is route-local: there is no global `SET`, global `INHERIT` or implicit input identity
  transformation.
- Transforming routes begin empty and may use `INHERIT` and ordered `SET`.
- Generated and other set-only routes begin empty, do not support `INHERIT`, and explicitly
  initialize required outputs. Generated state is immutable and independent for every route.
- During normal set-only construction, inferencers and WASM processors expose neither `message` nor
  `input`; additional schema-backed data comes from declared materialized state.
- Window output is set-only. Live `input` values are available only inside aggregate arguments,
  although aggregate calls may participate in larger scalar expressions; aggregates never nest
  inside aggregates.
- Correlators use explicit `left` and `right` scopes and never expose an ambiguous default input.
- Relay names are graph references, never top-level field qualifiers; materialized state is the
  explicit exception, through the `relay_state.<relay>.<field>` namespace.

### Branches

- Unbranched execution is an absent branch key; a concrete branch key is non-empty and typed. Never
  encode unbranched execution with an empty string, empty map, zero-field schema, synthetic root
  branch or reserved user identifier.
- Ingestors construct outgoing branches route by route.
- Reingestors are branch-boundary nodes: each route may preserve the incoming branch, construct a
  different branch or become unbranched.
- Junctions, deduplicators, reorderers, window processors, inferencers, WASM processors, correlators
  and generators preserve branch identity. Their branch declaration is node-wide, and every input
  and output uses that exact named branch.
- Emitters collapse branch identity only at the successful external boundary. Their envelope and
  ordering-group expressions may read each source branch's typed fields before that boundary;
  every input retains its own branch schema and partition count through evaluation and publication.
- Normal processors never subscribe to a mixed logical relay, combine branch keys, share mutable
  processing state across branches, or publish through a fallback that bypasses concrete branch
  routing.
- Error routes preserve the branch in which the failed operation executed, and their target relay
  has the same exact branch declaration; ingestor errors are unbranched.

### Materialized state

- Materialized dependencies are explicit, node-wide, same-domain and compatible with the node's
  exact branch; state is never scanned across branches.
- Dependency declarations execute strictly in written order. Available state binds immediately;
  defaults bind typed constant records; `REQUIRED SKIP` skips the message; `REQUIRED WAIT` retains
  it in memory and applies backpressure, and resolution restarts from the first declaration after
  the wait wakes.
- Duplicate dependencies are invalid. Defaults initialize every required field; omitted optional
  fields become typed nulls.
- Individual materialized records may become ready and update independently, but eviction removes
  the whole concrete branch and drops its suspended or buffered branch-local work.
- Ingestors cannot access materialized state. Reingestors resolve state in the incoming branch,
  never a partially constructed outgoing branch.
- A generator declares exactly one materialized relay in the simplified dependency form. A branch's
  appearance starts a branch-local generator task, and its eviction drops the task and its buffered
  output. Generators may have multiple routes, all preserving that source branch.

### Flush and route errors

- `FLUSH` and `ON MESSAGE ERROR` are route-local contracts; a general or global error policy is
  node-wide.
- Every flush-based route requires either `FLUSH EACH <duration> MAX BATCH SIZE <bytes>` or
  `FLUSH IMMEDIATE`; hidden defaults and optional fallback cadences are forbidden. Emitters always
  require a flush policy, and Iceberg also requires an explicit commit cadence and maximum commit
  size. Window processors use `WIDTH` and `STEP` instead of `FLUSH`. WASM processors are not
  flush-based: guest output and guest-requested timeouts own their emission cadence.
- Message errors are structured and carry a stable reference, code, operation, affected fields,
  timestamp and non-sensitive message. Error handlers may inspect the original eligible input, the
  exact captured materialized-state snapshot and an all-optional `partial_output` view.
- Error handling never recursively invokes the same policy when constructing the error record fails.

### Connector envelopes and subscriptions

- Header capabilities are connector-owned: preserve each underlying system's `read_header`,
  `read_headers` and `write_header` semantics, with no generic envelope extraction or injection
  abstraction. Header values do not propagate through relays unless copied into schema-backed
  fields.
- Kafka ingestion metadata remains a typed integration-specific scope.
- Session subscriptions keep their creation, deletion, delivery, sampling and backpressure
  semantics. They are read-only filtered views without construction, inheritance, values or side
  effects.

## Runtime and Infrastructure Invariants

### Columnar data plane

- Arrow columnar batches are the only data-plane payload representation, end to end: ingest
  decoding, relay carriage, VM input and output, stateful processor state, error handling, emission,
  sessions and interconnect. A batch with one row is still a batch.
- No row-oriented payload representation is introduced, reintroduced or extended: no
  `HashMap`-of-fields records, per-row boxed value maps or dual row-plus-column carriers kept in
  sync. A single message is addressed through a row view over an Arc'd Arrow batch (a batch
  reference plus a row index), never a materialized row copy.
- Scalar value enums are permitted only for genuinely scalar concerns: branch keys, literals,
  configuration and boundary key extraction. They are never a payload container.
- Program inputs are built by projection: reuse the carrier batch's Arc'd columns and construct only
  genuinely new columns, such as uninitialized outputs, broadcast state and computed lookups.
  Rebuilding a column from row data that already exists in columnar form is a defect.
- Wire codecs decode directly into typed Arrow builders and encode directly from column values at
  the external boundary, with no decoded intermediate row maps.

### Ownership and execution policy

- The type or crate that owns a dangerous operation exposes one public API enforcing its execution
  policy, with no unsafe synchronous or reactor-blocking alternative publicly callable beside it.
  Callers never choose blocking, yielding, batching, throttling or similar safety behavior unless it
  is an explicit typed part of the operation's contract.
- In async code, a loop whose body performs async work calls
  `nervix_primitives::task::consume_budget().await` once per iteration, near the top of the body.
- State that changes for every record, row, batch, remote frame or acknowledgement has exactly one
  mutable owner: a task, a concrete branch, a delivery channel or an ingestor attempt. The owner keeps
  it in ordinary collections and uses their `entry` API freely; exclusive ownership does not pin the
  task to an operating-system thread. Other tasks observe it only through an immutable value the
  owner publishes when it changes, never through the owner's collection.
- A recurring record, batch, remote-frame, acknowledgement or steady-poll path never reaches a
  shared concurrent map, through `entry` or a borrowed `get`, `contains_key` or iteration (a
  borrowed DashMap read takes a shard lock too); it keeps the handle resolved when its task, branch,
  channel or attempt was created. Registration, first installation, replacement, teardown and
  observer reads are cold paths, classified separately. Hot-path synchronization that remains is an
  ordering fence or bounded protocol named in [Data-Plane
  Concurrency](docs/src/data-plane-concurrency.md), with the key that scopes it and the capacity or
  deadline that bounds its wait.
- Prefer `nervix_primitives::collections::DashMap` over `Arc<Mutex<HashMap<...>>>` for a map that is
  justifiably shared, because several owners register into it or observers read it across owners,
  and whose accesses stay off recurring data-plane paths. State one owner mutates stays in an
  ordinary collection that owner holds.

### Domains and external systems

- Domains are explicit and must already exist: there is no implicit, default or auto-created runtime
  domain. All entities are domain-owned; a globally scoped legacy lookup is technical debt, not
  desired behavior.
- Nervix never creates broker topics, queues, streams, database objects, object-store objects,
  catalogs, namespaces, tables, buckets or collections as a side effect of node startup or data
  processing. External entities are provisioned explicitly by test setup, deployment policy or
  operator action, and a missing one surfaces as an initialization or publish error.

### Observability

- `info` is reserved for lifecycle, topology, startup, shutdown, administration and unusual state
  transitions. Per-message, per-record, per-batch, payload-bearing, duplicate-drop and branch-churn
  details belong at `debug` or `trace`.
- Hot-path logs and structured errors never expose sensitive payload values.

## Node Settings

- A setting is a field of a class-typed group, declared by a `SettingDefinition` with exactly one
  sealed class (`Prerequisites`, `ClusterParameters` or `NodeCapabilities`), its semantic value,
  name, single default, optional derivation and validation. Groups carry `Setting<D>` fields, and
  explicit values and defaults cross the same typed validation boundary.
- A **prerequisite** is what a node must bring to reach the live cluster. It travels through gossip
  and the handshake and is checked at join, and nothing needed to reach the cluster depends on what
  a node learns only after reaching it. [Node Settings](docs/src/node-settings.md) owns the closed
  list and timing bounds.
- A **cluster parameter** is what the cluster accepts or promises: one Raft-committed value, seeded
  only at first bootstrap and changed by an administrative Plexus statement. A value the registry
  validates against is never an independent node flag.
- A **node capability** is what one node can carry: detected or overridden locally and announced
  through Raft, with its bounds honoured by peers and its committed record used by placement. It
  changes no cluster acceptance rule or guarantee.
- Derived values are named methods on their owner, never flags. Defaults are written once, in the
  owning definition, and dependent defaults stay `Option` at the edge until resolved against typed
  context. Operator-supplied relationships are validated before I/O, and transitions at commit.
- A fixed constant declares `nervix::constant(fixed, reason = "...")` under
  `cfg_attr(nervix_lint, ...)`, with a reason naming the wire format, RFC, persisted layout, ABI or
  invariant that fixes it; a convenient literal is not a fixed-value contract.
- A value that depends on the external system is a clause or declared client key of the connector's
  Plexus surface, owned by its connector epic; node settings own host policy.
- A settings change stays correct through a rolling restart in any order, takes full effect when the
  last node adopts it, and rolls back the same way in reverse. Only prerequisites reject
  incompatible peers, and mixed operational values within the current shape introduce no alternate
  data representation.
- Flags are `--<area>-<name>` and `NERVIX_<AREA>_<NAME>`. Resolved settings are observable with
  secrets redacted, including per-node capabilities and committed-versus-running drift.
- The temporary `hidden_tunables` count, of governed numeric constants and literal policy calls
  outside class defaults and reviewed fixed contracts, only decreases as area owners adopt the
  declarations; its compiler-rule replacement removes the detector and the baseline entry together.

## Cryptography

[Cryptography](docs/src/cryptography.md) owns the boundary, the algorithm policy, the classification
register and the inventory these rules refer to.

- Nervix-authored cryptography (the TLS provider, digests used for security, secure randomness,
  password and key derivation) comes only from the `nervix-crypto` boundary. Until that crate exists
  it is the target, and every authored use outside it is debt the `non_aws_lc_crypto` count records.
- Anything that protects confidentiality, integrity or authenticity uses an algorithm AWS-LC
  provides and FIPS 140-3 approves.
- A non-approved hash (BLAKE3, MD5, SHA-1, xxHash, CRC) is allowed only for a non-security purpose,
  which the site records and the chapter lists.
- No change adds a dependency that brings a cryptography implementation other than AWS-LC into the
  `nervix-server` product graph; a connector need that cannot be met is recorded on its connector
  epic.
- A second `aws-lc-*-sys` crate in the product, or a second version of one, is a defect that
  `just ratchet` refuses. An `aws-lc-rs` upgrade that moves to another `aws-lc-sys` moves Nervix's
  `rust-rdkafka` fork to it in the same change.
- The `non_aws_lc_crypto` count only decreases. It counts authored uses of cryptography outside
  AWS-LC, found by the stable source parser in every configuration, and each dependency edge of the
  `nervix-server` normal and build graph onto a crate implementing other cryptography;
  `just ratchet --show non_aws_lc_crypto` lists every site.

## Maintained Implementations

A protocol, format, encoding or algorithm someone else specifies, such as HTTP, WebSocket, TLS, DNS,
URLs, MIME and multipart, JSON, CSV, base64, percent-encoding and RFC 3339 dates, with their
handshakes, escaping and quoting rules and compression, has two acceptable implementations: a
maintained one, or an approved custom one. It is never an ad hoc fragment written inline where it is
needed. Outside an approved custom implementation, these are defects however small:

- Producing its wire or text syntax inline: string concatenation, `format!`, `write!`, `push_str`,
  byte literals, `extend_from_slice` or joined delimiters.
- Parsing it inline: splitting on its delimiters, scanning for its terminators, matching its
  keywords or reading it line by line in place of its parser.
- Implementing the subset a caller needs today: one version or variant, the common case, or no
  escaping, continuation, extension, error or limit handling.
- Re-implementing framing, escaping, checksums, handshakes or negotiation the implementation in use
  already performs, or post-processing its output to change what it produced.
- Pinning one version or variant its implementation could negotiate, without a documented contract
  that requires it.

No justification makes a fragment acceptable: not that it is minimal, internal, single-site,
temporary, a probe or faster; not that the maintained implementation is a heavier dependency or a
larger change; not that nearby code already does it. Use the maintained implementation the workspace
depends on, otherwise add a maintained crate within the [Cryptography](#cryptography) rules, and fix
one that falls short upstream or in a fork the repository maintains.

A custom implementation is legitimate only when no maintained one can express the contract, or a
measurement shows one costs what the product cannot afford, and only once it is:

1. **Isolated** in one owning crate or module whose ownership contract names the specification:
   nothing else in Nervix produces or parses that syntax, and callers reach it only through its API.
2. **Tested** with Bolero properties for every encoding it owns ([Property tests and
   fuzzing](#property-tests-and-fuzzing)), unit tests from the specification's examples and edge
   cases, malformed-input targets for every parser, and differential tests against a reference
   implementation where one exists, used by the product or not.
3. **Properly abstracted**: its API exposes typed concepts such as messages, fields, states and
   errors, never raw syntax; it states its version, variants and extensions and rejects everything
   else with a typed error; it owns its limits and bounds every allocation and wait; and its scope
   is the complete contract it states, never one caller's subset.
4. **Approved** by the human directing the session before it is written, on a report of the
   contract, the evidence that no maintained implementation fits, and the scope. The approval is
   recorded on the owning ClickUp task and covers only that contract and scope, so widening it needs
   a new approval.

Contracts Nervix owns (Plexus, the client session protocol, the interconnect, stored and archive
shapes) each have one owning implementation on maintained lower layers such as FlatBuffers, rkyv and
Arrow, specified by its chapter. Connector codecs that decode into Arrow builders, as the [columnar
data plane](#columnar-data-plane) requires, take the external syntax from a maintained or approved
parser or tokenizer and own only the mapping into columns.

Only a test fixture, meaning code no product build compiles (a file under `tests/`, or a
`#[cfg(test)]` module or file, whatever its name), may write or parse a specified contract's bytes
inline, and only to give the code under test exact or deliberately malformed input, such as a
scripted HTTP, Redis or NATS peer; product code never calls one. An existing ad hoc fragment in
product code, or a custom implementation that misses a condition, is debt recorded as a defect on
ClickUp, never extended, copied or cited as precedent.

### HTTP

Nervix speaks HTTP through `reqwest`, or `hyper` where `reqwest` cannot express a contract, with
`hyper-util` and `tower-http` for what they provide and `tokio-tungstenite` for WebSocket
handshakes. There is no approved custom HTTP implementation, so nothing below `hyper` speaks HTTP.
Defects:

- A request line, status line or header block assembled inline, or a head parsed inline by splitting
  on `\r\n` or `:`, scanning for `\r\n\r\n`, reading lines to a blank one, or calling `httparse`
  directly.
- Inline body framing: counting `Content-Length`, encoding or decoding chunks, or reading to
  connection close.
- An inline `CONNECT` tunnel, proxy exchange, `Upgrade` handshake or `Sec-WebSocket-Accept`
  derivation.
- A client pinned to one protocol version without a documented contract requiring it, instead of
  negotiating every version its stack supports, HTTP/2 through ALPN included.
- A redirect followed by rewriting the request or its connection in place, instead of issuing a new
  request through the same stack.

Dialling through the node resolver within a budget and TLS through `tokio-rustls` sit below HTTP and
hand their stream to `hyper`, which speaks every byte of HTTP on it. Only `nervix-scenarios`, rooted
at `tests/`, depends on `httparse`.

## Failure Handling and Panics

### Typed error conversions

- Fallible operations return `error_stack::Result<T, E>`, with `E` a semantic `thiserror` enum owned
  by the module that decides what the failure means; a bare `Result<T, E>` lacks the contextual
  `Report`. Extend the owning module's enum when one already represents the operation; a second enum
  for the same failure is a duplicate to remove. `anyhow` is only for boundaries where callers
  cannot make a semantic choice, and `String` is never a domain error type:
  `just validate-typed-errors`, in `just validate` with no baseline, rejects `Result<_, String>` in
  product code, and the `nervix::bare_error_signature` diagnostic rejects replacing a `String` error
  with an unreported failure, through aliases, associated outputs, generic calls or inferred future,
  stream and closure contracts included.
- Variants carry the values a caller can act on as typed fields, never preformatted reasons.
  Validation failures identify the owning node, the route when the contract is route-local, the
  operation and the relevant fields, never sensitive payload values. Hot-path errors format no
  per-row or per-batch message when the variant names the failure; formatting happens at the
  reporting boundary.
- A report is created at the failure, and each layer that changes its meaning adds context with
  `change_context`; never format a cause into a `String` field or clone `current_context()` to
  satisfy a signature. A context names its own operation and neither prints its `#[source]`, which
  the frame beneath records, nor repeats a node or domain named above it. Fixed wire, ABI, stored
  and public outcomes are projected from the report only where they are constructed.
- A reporting boundary renders `{error:#}`, since plain `Display`, `to_string` and `%error` show
  only the outermost context and attachments are not part of the chain; a tracing field records a
  report as `%format_args!("{error:#}")`, which the `nervix::partial_report_field` diagnostic
  enforces.
- A decision reads typed data (the current context, a typed context beneath it, a typed field),
  never rendered text. A foreign trait that accepts only a standard error receives one holding the
  whole report, never a clone of its current context.
- Pure conversion refusals, cancellation signals, semantic row, record and wire outcomes and modeled
  library error contracts carry a reason-bearing `nervix::error_boundary(outcome|library, ...)` at
  the exact error type or return contract, naming the actual behavior and never inherited from an
  enclosing callable, module or package; no numeric allowance replaces it ([Errors And
  Diagnostics](docs/src/errors-and-diagnostics.md)).
- Text naming a concrete branch uses the runtime's `BranchScope`, the key's fingerprint, never its
  field values. Parse errors report precise expected and found tokens with renderable source spans,
  and messages prefer semantic labels to generic ones.

### Panics and recovery

- Replace bare `unwrap` and `expect` with `meticulous`'s `ResultExt` and `OptionExt`, imported as
  `use meticulous::{OptionExt as _, ResultExt as _};` to avoid colliding with `error-stack`'s
  `ResultExt`. The method says why failure cannot happen: `assured` (by construction or target
  platform), `verified` (checked earlier in the same code) or `todo` (not implemented yet), and its
  reason states the actual guarantee: `verified("the has_errors branch above already returned")`,
  not `verified("parse must succeed")`.
- A site with no guarantee is a defect, not a renamed `unwrap`: propagate a typed error or make the
  invariant hold in the type, and never invent a guarantee. A build script is the exception and
  panics with its cause.
- `let _ = …` states no recovery class. Use `nervix-recovery`'s `discarded` with the reason dropping
  is correct, or `reported` where the call is the failure's only witness.
- A failed channel send means no receiver is left: `means_shutdown` when it stopped with its node,
  domain or task, `means_peer_left` when the requester, session or observer withdrew. A receiver
  held as long as its sender is a guarantee, so its absence takes `assured` or `verified` and
  panics.
- A `watch::Sender` whose value must be current for later subscribers uses `send_replace`; a failed
  `send` keeps the old value, so discarding its result publishes a stale one.
- Arithmetic is checked, because bare `+`, `-` and `*` wrap silently in release builds: sizes,
  offsets, counters, capacities and timestamps from untrusted or unbounded values use `checked_*`
  and say what bounds them. An overflow ruled out by construction takes `assured` or `verified` with
  the bound as its reason, and one a caller, payload or configured limit can reach is a typed error.
  `saturating_*` and `wrapping_*` serve only computations whose meaning is saturation or wrapping,
  such as a clamped backpressure budget, a bounded retry delay or a hash mixer, and the site says
  which.
- Related compile-time constants make their relationship executable: name the independent policy
  inputs, derive the rest with const arithmetic, and assert what derivation cannot express with
  static `const` assertions, as a capacity partition asserts that each required reservation is
  nonzero, that they fit the owning capacity and that they sum to it. Never align independent magic
  literals by hand.

## Engineering Conventions

### Structure and control flow

- Organize modules around coherent ownership boundaries, never broad technical categories or
  grab-bag `clients`, `helpers`, `utils` or `common` modules. Shared logic belongs in a focused
  abstraction; setup, configuration interpretation, state transitions, I/O, encoding, decoding and
  lifecycle belong to the struct that owns them.
- Behavior that belongs to one model, AST, runtime object or domain type is an inherent method
  (`&self`, `&mut self` or `self`), private helpers included, and boolean predicates over enum
  variants are methods on the enum. Free functions are only for symmetric operations over
  independent types, parser-combinator plumbing and small lexical primitives without a natural
  owner.
- Prefer compact refactors that collapse duplicate logic into the owning parser, model or runtime
  path and remove obsolete layers; add a wrapper, adapter or helper only when it reduces total
  complexity or isolates a real boundary.
- Prefer explicitness to brevity. Collapsing duplicated logic into its owner simplifies; compressing
  a clear sequence of steps into a denser expression does not, so when two forms differ only in
  density, write the longer one. Density is a decision or sequence buried inside an expression: a
  multi-stage iterator chain that selects, rejects or classifies on the way through, a closure
  nested in a closure, an expression-position `match` written to save a binding, one line doing
  several unrelated steps. Name each step with a binding and write the steps in the order they
  happen.
- Take an `Option` or `Result` apart with `match`, `if let`, `let ... else` or `?`, and write what
  follows as statements. Two adapters in a row (`map` feeding `unwrap_or_default`, `and_then`
  feeding `ok_or_else`), `map_or` and `map_or_else` however short their arms, and a closure whose
  body is a block of statements hide a branch or sequence inside a call: write the arms. One adapter
  doing one transformation stays, because it decides nothing, such as a `map_err` or `ok_or_else`
  shaping a value into `?` or an `unwrap_or` supplying a default. Sequences follow the explicitness
  rule instead: a `map`, `filter` and `collect` pipeline whose closures only shape each element
  stays, and a chain whose closures decide, branch, accumulate or fail becomes named steps, however
  short it reads.
- In `if` conditions, prefer `if let` or `if let` chains over `matches!` when they express the same
  logic cleanly; use `matches!` where an `if let` form would be unclear or outside an `if`
  condition.

### State and types

- Absence is `Option`, and distinct semantic states are enum variants with their own data, never
  `0`, an empty string or collection, a numeric extremum, an all-zero hash or a reserved identifier,
  which a named constant does not repair. Keep related state and data together so callers cannot
  construct contradictory combinations. Genuine zero counts, empty content, documented
  scalar-function results and format signatures remain ordinary values.
- Unavailable expression scopes and internal special cases are typed variants or internal-only
  structures, never fabricated names that flow through user-name resolution; diagnostic labels and
  display strings describe a typed state and never select behavior.
- A default is a valid value with the intended meaning, never a substitute for missing required
  state, a failed conversion or a type mismatch: validate at the owning boundary and propagate a
  typed error.
- A required external encoding or packed atomic representation may use raw tags only inside its
  owning boundary, documented there, behind an API that exposes typed states and enforces valid
  transitions and bounds; its sentinels never leak into Models, plans or callers, and no lock is
  added merely to replace an atomic encoding with an enum.
- A structure that can hold a value with a contradicting description of it, such as a string payload
  labeled as an integer or a wire schema paired with a format that reads another kind, is wrong: put
  the kind and its payload in one variant or let a type parameter drive the value's type, then
  delete the consistency checks and unreachable mismatch errors. A hot path may carry a descriptor
  beside its value only where deriving it per batch or row is a measured cost; such a site is agreed
  with the user before it is written and carries a comment naming the path and the cost.
- Avoid tuples beyond a trivial local pair, nested tuples, and tuples shaped by whatever the
  construction site produced. Returns of three or more elements, map keys and values, iterator
  accumulators and channel payloads of several unrelated values are named structs with named fields,
  declared inside the function when their use stays there and at module level otherwise; a type
  alias for a tuple is not a name for its elements.
- Internal runtime, application, persistence and domain-clock time uses semantic datetime or
  timestamp types, never raw Unix integers. Nanosecond Unix integers are boundary representations
  for serialization, public or cross-node protocols and Arrow timestamp arrays, converted to typed
  timestamps immediately after decoding and back only at the boundary.

### Collections, conversions and sharing

- Do not scan a `Vec` to look a value up: "`n` is small" needs proof at the call site that keeps
  holding as domains, graphs and clusters grow. Key the data: `BTreeMap` for small keys where
  ordered comparison costs less than hashing, `HashMap` otherwise, and `IndexMap` or a sorted
  sequence with binary search where order must be kept and keys resolved, never a map and a parallel
  order sequence kept in sync by hand. Sequence types are for genuine sequences, a surviving scan
  states the bound that makes it correct, and a sorted invariant uses `sorted-vec`'s `SortedVec` or
  `SortedSet`.
- Values shared and living together belong in one named struct behind a single `Arc`, with a thin
  cloneable handle over it; a type accumulating independent `Arc` fields is a missing struct whose
  every clone pays one refcount per field, per batch on a hot path. A field keeps its own `Arc` only
  when a second owner outlives the handle's borrow, and says who that owner is. Never wrap a handle
  that is already one `Arc` in another.
- Convert through `From` and `Into` when total and `TryFrom` and `TryInto` when fallible,
  classifying the failure like any panic site. `arch-into` covers pointer width and
  `nervix-approx-into` integer and floating point: `approx_into` rounds to the nearest representable
  value, and `try_approx_into` gives a float no integer value unless it is finite and in range.
- `as` is denied workspace-wide by `clippy::as_conversions`, so a cast is written as its operation:
  `cast_unsigned` for a two's complement reinterpretation, `addr` for a pointer's address, a typed
  binding or annotated collection for an unsizing coercion. The rounding and truncating casts live
  inside `nervix-approx-into`, each behind an `#[expect(clippy::as_conversions, reason = "...")]`
  naming the operation, and generated code that casts carries a reason-bearing `#[expect]` its build
  script applies.
- Prefer deriving declarative enum string conversions and metadata with `strum` (`AsRefStr`,
  `EnumString`, `EnumProperty`, `FromRepr`) over manual match-based helpers. Parse URLs and URL-like
  service addresses with the `url` crate, never by splitting schemes, authorities, hosts, ports,
  paths or query strings by hand, unless the format is not URL-compatible.

### Hygiene

- Do not add `#[allow(...)]` annotations without explicit approval.
- Do not keep browser callbacks alive with `std::mem::forget`, `Closure::forget`, leaked boxes or an
  equivalent lifetime leak; use explicit ownership and cleanup or a crate-managed abstraction.
- Preserve unrelated user changes in dirty worktrees.

## Architecture Rules and the Debt Ratchet

[Architecture Rules And The Debt Ratchet](docs/src/architecture-enforcement.md) owns the complete
eighteen-family contract matrix, the nine source-contract annotations, source and compiler scope,
report identity and reuse, the analysis toolchain and its upgrade, qualification and its CI
selection, what each form of evidence establishes, and the workaround audit and its review limits.

### Source contracts

Real Nervix compiler diagnostics, across the complete declared configuration matrix, check Model
consumption, language dependencies, recurring cluster awaits, node identities, installation state,
synchronization, failure signatures, discarded outcomes, canonical panic APIs, report-bearing
tracing fields, integer and `Duration` clamped arithmetic and `Option` and `Result` adapter control
flow. Finite API recognition is Rust code in the rules, and no policy is inferred from filenames,
sharing traits or method names. Contracts and reviewed exceptions live beside their source owners
under `cfg_attr(nervix_lint, ...)`, move with their module, and state the actual recurring,
lifecycle, observer, outside or bounded execution with reasons and bounded identities; no owner
catalog, count allowance or filename exception substitutes for one.

- `nervix::layer` marks each documented layer boundary. Connector engines and their contract also
  declare `nervix::input(plans, reason = "...")`, because their layer grants no permission to
  consume configuration Models. `nervix::value(model, ...)` marks the public configuration union,
  `nervix::value(name, ...)` shared name types and `nervix::value(cluster_access, ...)` the cluster
  observation capability, as reviewed semantic roles rather than spellings. Resolved references and
  normalized interface and body types enforce the Model and language boundaries, recurring
  data-plane call edges diagnose awaited cluster access, and language dependency and inactive import
  provenance stay a separate source and manifest check.
- `nervix::identity(node, ...)` marks the shared vocabulary identity types and every field,
  parameter, alias or return interface whose value is a node identity; a mixed aggregate is not one
  because one member is typed.
- `nervix::state(installed, ...)` marks installation-only fields and fails on a resolved `RwLock`
  anywhere in their representation, and `state(mutable, ...)` marks genuinely replaceable fields
  with their lifetime reason; `RwLock<Option<name or shared handle>>` without a lifetime contract
  needs explicit review and proves no write-once behavior ([Typed
  States](docs/src/typed-states.md#source-owned-identity-and-installation-contracts), [Data-Plane
  Concurrency](docs/src/data-plane-concurrency.md#resolved-architecture-boundaries)).
- Supported local helper and callback edges are re-evaluated, and callee and trait contracts cross
  compiler metadata boundaries: a hot caller of a lifecycle-only helper is diagnosed, and unknown
  dispatch needs a callable source contract or a diagnostic, never an inherited cold module
  classification. Disjoint structural and primitive provenance checks still cover inactive cfg and
  authored macros ([Data-Plane Concurrency](docs/src/data-plane-concurrency.md#source-contracts),
  which claims no universal ownership proof).

An exception is a reason-bearing expectation on one operation, `expect_lint!` included, and retained
debt names its repair task; a blanket `allow` and a broad, undocumented, unfulfilled or widened
expectation fail, and expectations never form a second allowance catalog. Generated reports are
outputs under `target/`, never enforcement inputs. A new or changed semantic rule ships with allowed
and rejected compiler fixtures distinguished by the resolved fact, false-positive and false-negative
controls, precise diagnostic and span assertions, and invalid, unfulfilled and widened-expectation
checks; source rules also qualify inactive configuration and authored macro fragments, and bounded
Bolero properties qualify changed source parsing and lossless tooling representations. A replaced
detector and its numeric entry go at the same cutover.

### Analysis toolchain

Repository enforcement that reads or writes compiler evidence belongs in the isolated
`tools/nervix-lint` Rust workspace: the driver, report vocabulary, ratchet binary and compiler
fixtures with their macro crate. Matrix reports, compiler side reports, compilation records,
completion evidence and audit reports are consumed through their shared Rust types; Python under
`scripts/` neither reads nor writes them. Add a structural count or gate rule to `nervix-ratchet`,
and give each tooling encode/decode pair its registered bounded Bolero property. Source validators
for primitives, clocks, typed errors and mode dependencies, and the coverage, Bolero and CI runners,
keep their own inputs and evidence; they may invoke the Rust owner as a process without interpreting
its compiler evidence.

The synchronization driver, its isolated artifacts and Bolero's sanitizer campaigns use
`nightly-2026-09-17`; product builds stay stable. An upgrade moves every declaration of that nightly
in one change that keeps every compiler fixture's result. The driver's workspace wrapper composes
beneath the configured kache. Analysis requires current source, annotations, rule, dependency and
configuration inputs and complete declared target evidence, zero findings included, and missing
evidence rebuilds only isolated authored artifacts in a supported cache namespace.
`just test-typed-ratchet` and `just qualify-typed-ratchet-cache` qualify the diagnostics, the
current paired API doctests and completion and cache behavior. Tooling Bolero targets use the shared
inventory and CI policy, and native coverage uses matching LLVM tools.

### The ratchet

Architecture debt is counted and only decreases. `just ratchet` counts oversized files, actual Rust
cast expressions, struct fields whose presence depends on `cfg(feature = "testing")`, functions
whose cyclomatic complexity exceeds 30, [`hidden_tunables`](#node-settings) and
[`non_aws_lc_crypto`](#cryptography), over tracked and newly authored Rust. Its stable source parser
sees inactive branches and named or tuple fields; raw identifiers, import aliases, qualified paths
and conditional documentation attributes are not those facts. CI fails when a structural count
exceeds `debt-baseline.json`. A change may lower a count, never raise one; when a count falls, run
`just ratchet --update` and commit the baseline. `just ratchet --show <count>` lists a count's
sites.

Complexity is measured by the cargo-crap version `nervix-ratchet crap` pins, which
`just install-cargo-crap` builds through kache; another version is refused, and an analysis that
misses a source never reads as zero. That count, which reads no coverage, is the mandatory
complexity verdict: a new function above 30 fails `just validate` and the `checks` job, and
decomposing it is the repair.

A rule that misjudges code is repaired in the rule, or the misjudged operation carries a narrow
reviewed exception; correct code is never reshaped only to satisfy it. Respelling an operation,
splitting one lookup into two, moving code into a helper or another file, or defaulting a failure to
lower a finding is a workaround, and the owner it degraded keeps the repair. `just lint-audit`
reports resolved workaround candidates, attributed to the commits and counts that introduced them,
as review input under `target/`; dispositions and measurements belong on the owning task. A claim
that a workaround costs performance, or that its repair saves it, needs that provenance and a
same-host measurement of the lookups, comparisons and allocations it changes over hit-heavy and
miss-heavy keys; a lower count or a shorter function is not a speedup.

## Specifications

- Specifications and proposals are product documents defining externally observable behavior:
  concepts, states, guarantees, interfaces, limits, failure semantics and behavior changes.
  Implementation material, such as code sketches, type definitions, file or module lists, line
  references and change-size estimates, does not belong in them.
- A requested specification is one complete document, not split into implementation phases,
  milestones or staged deliveries unless the user asks for phasing.
- A specification is correct and fully resolved: its claims are verified against the current system,
  every design question is decided in the document, and nothing is deferred, postponed or marked as
  future work, a fast-follow or an open question unless the user requested or agreed to that
  deferral.

## Validation and Testing

### Test-first changes

- For a bug, first add or identify a focused test or Cucumber scenario and confirm it fails for the
  expected reason. Implement only after the reproducer is red, rerun it until green, then run the
  appropriate broader validation.
- For new runtime, persistence, API, CLI, scheduling, cluster, metric or domain behavior, first add
  or update a Cucumber scenario through the public interface and confirm the expected failure before
  changing product code.
- Unit tests support but never replace Cucumber coverage for behavior observable through a Plexus
  command, an HTTP or public API call, cluster state, runtime output or persisted state, unless the
  user explicitly approves the exception for the current task. An executable statement with
  application or runtime handling is not parser-only.
- Parser-only work requires positive and negative parse tests and completion-context tests that
  guard against grammar-branch leakage, kept near the grammar they protect; composed language
  phrases require completion coverage.
- Compile-time rejection is checked with Rustdoc `compile_fail` doctests beside the owning API,
  paired with compiling examples of the current imports and supported signatures, never with
  `trybuild` or its dependencies, harnesses, fixtures or snapshots. They run in the normal test
  commands, and in CI for a pull request labeled `extra-doctests`, which a change to such an API
  carries.
- Keep verification claims with their evidence. Paired API doctests check one API in the build's
  selected configuration; Cargo graph, feature-conflict and target probes check the configurations
  they build; compiler fixtures qualify resolved diagnostics; primitive conformance checks compare
  backend semantics. None executes a production-owner concurrency model, and compiling under a mode
  is analysis, not execution evidence. A change confined to an exclusively owned collection or to
  compiler analysis makes no concurrency claim and needs no invented model.

### Property tests and fuzzing

- Bolero is the default for lossless encode/decode pairs and representation conversions. Each new or
  changed pair ships with a property over bounded, valid current values asserting that the round
  trip equals the complete original, with every significant field, order, branch, identity, null,
  type and sensitivity, using an explicit complete oracle where equality falls short (float bit
  equality; Arrow schema and logical values with validity), never selected fields or a normalized
  failure.
- Canonicalizing, lossy and one-way conversions test their actual contract, with exact round trips
  tested separately on a lossless domain where one exists. Malformed-input rejection gets separate
  targets, so generators cannot pass on mostly invalid values. Generators produce bounded current
  shapes with supported variants and deliberate boundaries, never historical fixtures.
  Model/reference, operation-sequence, scalar/SIMD differential and encoded-size properties also use
  Bolero when appropriate.
- Every property is also a registered custom fuzz target in `tests/bolero-targets.toml` (stable ID,
  exact package and test identity, required features, domain version, source-adjacent corpus, input
  and case budgets, invariant), running the same production path and complete assertion in
  randomized/corpus and libFuzzer runs; registration in both modes is mandatory, even while other
  work is concurrent. Adding or changing a target updates the register in [Property Testing And
  Fuzzing](docs/src/property-testing-and-fuzzing.md) and any owning representation coverage map,
  with matching domain version, case count and input bound. Generators stay in dev and test code,
  and modeled features and model checkers stay out of ordinary and fuzz builds.
- `just test-bolero [filter]` runs bounded random cases and corpus replay, `just fuzz-list` lists
  registered targets, `just fuzz <target> [duration]` and `just fuzz-all [duration] [shard]` run
  sanitizer-backed libFuzzer, and `just fuzz-replay` and `just fuzz-reduce` handle saved exact
  inputs; a sanitizer finding replays only through `just fuzz-replay-sanitizer`. These commands
  enforce the inventory and scoped discovery, which `just validate-bolero` checks alone, and Bolero
  checks never join `just validate`, `just validate-ci` or Check's validation job. A seed identifies
  one case, not a campaign; keep failures, their minimization and their revision, toolchain and flag
  metadata before cleanup.
- Live campaigns collect source coverage with the pinned sanitizer toolchain through
  `scripts/bolero_coverage.py` under `target/native-coverage/bolero-fuzz/`. Only a `complete` record
  beside a report is evidence: discovery, probes, replay and failure qualification supply no
  counters, stable and nightly profiles never mix, a failed campaign keeps its failure, and only
  `just combine-bolero-fuzz`'s record of every whole-harness shard of one plan claims the inventory.
  These reports stay out of ordinary coverage and CRAP.
- Check calls the reusable Bolero workflow with its tested SHA, event and label snapshot, running
  sanitizer mode for `fuzz` and randomized/corpus mode for `bolero-random` ([CI
  labels](#ci-labels)), and skips it only for a verified prose-only pull request with neither label;
  called runs stay mandatory even on single-job reruns, and a deliberate sanitizer skip publishes no
  fuzz coverage. Check verifies its exact tested merge before skipping ordinary suites for
  documentation-only changes, `test-selection` is not property evidence, and `no-docker` never
  authorizes a test exclusion; [Documentation-only
  selection](docs/src/property-testing-and-fuzzing.md#documentation-only-selection) owns this, and
  `just test-ci-selection` tests it.

### Concurrency evidence

- The evidence forms are complementary and none replaces another: Shuttle for interleavings of
  production owners, Loom for memory-ordering claims of synchronous production owners, Turmoil for
  network claims within the supported simulation, Deloxide for active deadlocks among tracked
  thread-blocking locks of real diagnostic processes, Cucumber for public behavior, and the external
  Chaos suite, against an immutable product image and its diagnostic image, for real-process
  recovery. A model-verification claim records its invariant, selected inventory, completed
  exploration, and weakening and replay evidence.
- **Shuttle.** A lock-free or wait-and-notify data-plane protocol ships with a Shuttle check over
  its production owner that names and asserts its invariant, explores through
  `nervix_model_harness::shuttle`, registers in `crates/model-harness/shuttle-inventory.toml`, runs
  through `just test-shuttle` and keeps a failing schedule for replay. Concurrency tests express
  deadlines and progress as scheduler-visible events, never wall-clock bounds or sleep polls. A
  waiter registers for its notification before reading the state it waits on; the boundary's
  `Notify`, `watch` and atomic waker make registration a Shuttle scheduling point, so the check, not
  review, holds that order. A public outcome still needs its Cucumber scenario.
- **Loom.** A memory-ordering claim, such as cross-location publication or a fence protocol, ships
  with a Loom model over its actual synchronous production owner, because Shuttle's sequentially
  consistent scheduler cannot establish it. When the protocol is embedded in async orchestration,
  make the synchronous protocol independently testable and have the runtime use that same owner; a
  copied algorithm, test-only reconstruction, witness published by a join or extra lock, or real
  atomic does not qualify. The model names an `InvariantId`, explores through
  `nervix_model_harness::loom::explore`, registers in `crates/model-harness/loom-inventory.toml`,
  and registers a `[[qualification]]` weakening that must make it fail; qualifications retain
  counterexample and replay checkpoints, outputs, source, bounds and weakening metadata, which CI
  uploads on success and failure. A standalone relaxed counter carries no cross-location claim, and
  an operation inside an opaque dependency is excluded from a claim rather than modeled fictionally.
  A store-load claim holds only under sequential consistency, which Loom cannot check because it
  models `SeqCst` as acquire and release, so the side off the hot path reads the other side's
  location by read-modify-write.
- **Deloxide.** A change to thread-blocking synchronization, a `sync::blocking` lock or condition
  variable, tracked-lock acquisition order, or a shutdown, drain, restore, cancellation, handoff or
  other lifecycle or ownership path using tracked locks runs `just test-deloxide`,
  `just test-deloxide-order`, `just test-deloxide-stress` and `just test-deloxide-stress-restore`
  locally, under the lane [Data-Plane Concurrency](docs/src/data-plane-concurrency.md) owns:
  - The lane reaches the changed path, or the change extends its probes, owner tests or tagged
    scenarios (`@deloxide_stress` for lifecycle scenarios, `@deloxide_stress_restore` for capture,
    restore and checkpoint transfer) and registers each in `tests/deloxide-inventory.toml` with a
    stable identity, invariant, selections and declared coverage; a scenario that joins the
    active-only selection joins its family's stress selection under a `stress.` identity. A source
    file that starts acquiring tracked locks gets an `[[owner]]` record naming the workloads that
    reach them, or the unreached path and why, which `just validate-deloxide-applicability` checks
    against the compiler's acquisition catalog; a recorded gap is a reviewed declaration, never a
    waiver.
  - Findings are fixed in their owner. A failing stressed workload is first reproduced as a focused
    failing owner test, never hidden by a passing rerun, a wider product deadline or a weaker
    assertion, and an active cycle is never suppressed, retried away or excluded. A potential-order
    finding stays visible until its owner fixes it or records a specific non-overlap, shared-reader
    or lifecycle proof with retained regression evidence. In the lane a potential cycle fails the
    run: the lane keeps no ledger of approved cycles, so a correction removes the order from the
    exercised workload, and a reviewed proof qualifies only the evidence it reviews. Missing source
    context, truncated cycles, overload and unreviewed findings prevent qualification, and a run
    that executed nothing, lost a finding or could not record one failed. A build with graph
    instrumentation and runtime checking disabled remains instrumented, and its cost is reported
    separately.
  - The change records what the run covered and cannot see and the evidence kept: attempts under
    `target/deloxide/test-deloxide`, and CI artifacts when the `deloxide` label ran it. The task and
    pull request record the tested revision and inventory digest, each selection's discovered,
    selected, executed and completed counts, the retained artifacts and which registered workloads
    reached the changed owner. An earlier revision's result never qualifies a later change, and a
    clean run never proves an untracked path or an untaken interleaving deadlock-free.
  - Every concurrency-related change states whether the rule applies. Async-only or otherwise
    untracked work, such as Tokio locks, channels and `Notify`, DashMap, atomic protocols or network
    waits, records that the detector does not see it and keeps its Shuttle, Loom, Turmoil, Chaos and
    Bolero evidence; documentation-only or unrelated work records that the rule does not apply.
- **Mode commands.** A change to a primitive adapter or synchronization protocol runs the checks of
  every mode it affects beside the ordinary suite: `just test-shuttle [filter]`,
  `just test-loom [filter]`, `just test-turmoil` and the four Deloxide lanes. `just test-primitives`
  runs the boundary's own conformance checks once per mode. Every mode command reports discovered,
  selected, executed and completed counts, and fails when its selection executes nothing across its
  whole scope (a package without matches inside a nonempty selection is fine), when a registered
  check, model, test or workload is missing, ignored or incomplete, or when an unregistered one
  appears; `just test-turmoil` also fails when any of its invocations executes no test, and its
  register is `tests/turmoil-inventory.toml`. Shuttle and Loom run each check in its own process,
  Shuttle also under the nondeterminism detector. A failed Shuttle check or Loom model keeps its
  schedule or checkpoint for `just test-shuttle-replay` or `just test-loom-replay`, and every
  Deloxide run keeps an attempt that `just test-deloxide-replay` replays;
  `just test-shuttle-replay-check` proves a persisted schedule reproduces,
  `just test-loom-qualification` that each model fails under its weakening, and
  `just test-deloxide-qualification` that the lane fails, keeps its evidence and leaves no process
  behind for each way a diagnostic process can fail. `just cargo-clippy-loom` and
  `just cargo-clippy-shuttle`, both in `just lint`, keep every Loom build compiling, the server and
  consensus libraries included as shipped and in test mode, and every Shuttle build, each explored
  package in test mode, free of warnings.

### Scenarios

- `Given` establishes preconditions, `When` performs the action or event under test, and `Then`
  asserts the observable outcome.
- Runtime behavior uses scenario outlines over one- and three-node clusters unless it is explicitly
  topology-specific. Three-node clusters use the conditionally compiled random test scheduler by
  default, which assigns schedulable graph nodes independently to exercise cross-node paths and
  keeps assignments stable for unchanged domain, model and topology inputs, so reconciliation causes
  no ownership churn. Scenarios about production placement, cordon, drain, scheduled-node failover
  or full-cluster recovery from node-owned persisted state configure the production sticky scheduler
  before cluster startup. Scheduling, leader/follower restrictions and inter-node mechanics get
  dedicated scenarios or lower-level tests.
- Stateful processor scenarios prove branch isolation and field preservation with interleaved
  records from at least two branches. Browser behavior is tested through the web-console suite's
  Playwright-facing steps, never by bypassing the public browser flow.
- Avoid blind sleeps: wait for explicit conditions with bounded timeouts and useful failure
  messages; short polling intervals are acceptable only inside such a condition-based wait.
  Scenarios run many at a time, so a bound that holds only on an idle machine is a test defect, not
  a flake to retry; bound waits generously, since they end when the condition holds.
- Every scenario runs beside the rest; no tag, lock or harness mode, such as `@exclusive`, runs one
  alone or holds others back. Slots come only from declared weights and retry shares ([Integration
  Test Lifecycle](docs/src/integration-test-lifecycle.md)): `@slots_N` on a feature, rule, scenario
  or examples block declares measured load, the largest inherited declaration applies, and each
  attempt holds the larger of weight and retry share, capped at a quarter of the pool and at least
  one slot. Choose weights from measured per-attempt load, judged on complete sharded runs for
  stalls, retries, utilization and suite length.
- Weights and retries absorb contention but explain no failure, and one they clear may be a masked
  bug. Before calling a failure contention, ask whether it follows host load (CPU steal, stalls and
  retries across unrelated scenarios), repeats on a quiet host and could come from delay at all, and
  record why on the ClickUp task. Delay makes a wait late; it never loses an effect, duplicates one
  or produces a wrong value, which stay defects however often a retry passes. Repair a masked bug in
  its owner, and remove a timing assumption the scenario does not need instead of weighing it away.
- Assert a delay, not a silence: a "not yet" window races a cadence that started at the previous
  event, and load at the step boundary consumes the margin, so record when the triggering event
  happened and require output at least the cadence later. A "never arrives" window is only for
  output that must never arrive. An exact batch-count assertion needs a flush window much wider than
  the spread of the messages it groups, and a unit test comparing wall-clock instants anchors its
  bound to an instant observed after the value under test was computed.
- The scenario suite and harness regressions are the `nervix-scenarios` package rooted at `tests/`,
  with only test targets and dev-dependencies, so the server is never linked twice and its `testing`
  feature stays out of ordinary builds.
- Scenarios run nodes in-process. A test whose subject is a process (its signals, exit status,
  command line, open-file limit or crash) re-runs its own executable through
  `tests/common/process_program.rs`, which starts `current_exe` with `arg0` set to `nervix-server`
  so that `main` calls `run_server_process`, the server executable's entry point; a new helper
  process joins `ProcessProgram`. A test never uses `CARGO_BIN_EXE_nervix-server`, looks for a
  server binary in a target directory or needs a recipe to build one, except a test of one
  particular build, such as the client protocol's release baseline, whose recipe builds that server
  and names it to the test.
- A test compiles nothing and never runs Cargo, a compiler or a package manager: `just` builds every
  tool a test starts beforehand, through `tests-deps` and what `scripts/scenario_shards.py` packs,
  since a shard has no toolchain. Testcontainers provides every dependency:
  `crates/test-environment` declares each service, started on first use, and no recipe, workflow or
  CI step prepares, starts or stops one.

### Coverage

[Coverage Reports And The Complexity Gate](docs/src/coverage-reports.md) owns producers, reports and
their verdicts. Coverage percentages and CRAP scores are advisory and never fail a build, and no
text calls CRAP a gate; report integrity is mandatory, and the only complexity verdict is the
[ratchet's](#the-ratchet) count.

- Every ordinary consumer reads the one merged, repository-relative `lcov-workspace.info`. A new
  ordinary producer registers as a merge input, and a producer outside it states its scope and why;
  mode, diagnostic and live fuzz reports stay out of it and its CRAP report. A merge missing an
  input or workspace member, or an analysis matching nothing or mismatching the sources, fails:
  missing analysis is never zero coverage or zero debt.
- `just coverage-native-extras [producer ...]` instruments the native extra checks inventoried in
  `scripts/native_coverage.py` beneath kache, one build directory per mode, into a fresh
  `target/native-coverage/<producer>/<mode>/<toolchain>/<attempt>/`. Only a report beside a
  `complete` completion record counts, for its tested revision and selection; incomplete collection
  keeps its evidence and fails before export, and coverage bookkeeping stays outside model
  processes. A check that starts executing Nervix code natively joins the inventory with a prepare,
  instrumented and finish recipe, while existing tests, properties, models and scenarios keep their
  canonical runners; a producer change updates its merge inputs or full-union requirement,
  eligibility, workflow downloads and command and artifact tests together.
- `just coverage-patch [base] [report]` writes advisory patch coverage under `target/`
  (`--pr <number>` updates the marked comment after verifying the head; the Turmoil job uses
  `--scope turmoil`). `just check-coverage [report] [record]` publishes the advisory CRAP report
  under `target/crap-report/`, fails only on vacuous analysis, and lists functions in unrecorded
  files as unmeasured, never uncovered. `just coverage-full [input ...]` merges the producers a CI
  run selects (`just coverage-full-select`) into a line-coverage union in `target/coverage-full/`,
  failing on any missing, stale, foreign, corrupt, failed, incomplete or unselected input, and a run
  without `fuzz` requires the Bolero call's deliberate skip. Nothing ordinary reads the union, and a
  change to a producer job, its producer or its artifact updates the requirement table in
  `scripts/coverage_full.py`.
- `just test-native-coverage`, `just test-patch-coverage`, `just test-crap-report` and
  `just test-coverage-full` test these tools, and `just coverage-patch-runner`,
  `just coverage-crap-report-runner` and `just coverage-full-runner` collect their Python coverage.

## Repository Workflow

### Commands and documentation

- Always prefer a `just` recipe to invoking Cargo, or the scenario test binary, directly: recipes
  establish the required dependencies, environment and ordering. When a needed invocation has no
  recipe, add a focused one and use it. `just validate` formats and validates; run it after each
  development cycle with any focused tests added or changed. `just test` runs the full suite and
  `just test-scenarios --input <feature> ...` targeted scenarios.
- Every Rust build, check, lint and test runs through kache, the compiler wrapper the host sets with
  `RUSTC_WRAPPER` or Cargo's `build.rustc-wrapper`, without exception: bypassing it recompiles what
  the shared cache holds and writes a private copy of every artifact. Never unset, clear or override
  `RUSTC_WRAPPER`, `CARGO_BUILD_RUSTC_WRAPPER` or `build.rustc-wrapper` (an empty `RUSTC_WRAPPER`
  disables it), or hide the host's Cargo configuration with another `HOME` or `CARGO_HOME`. If kache
  is unavailable or a build fails under it, stop and report instead of building without it. Coverage
  uses the matching toolchain's declared `llvm-tools`.
- Textual reports that are not stable documentation never enter the repository. Every artifact
  generated while implementing a task, including implementation reports, review and audit findings,
  validation results, logs, benchmark results and screenshots, is recorded or attached on the
  corresponding ClickUp task. Before executing an epic or task whose definition asks for such
  artifacts to be committed, update the definition to require delivery to ClickUp; an outdated
  definition does not override this rule.
- A public interface or Plexus surface change updates the relevant `docs/src` pages and the Plexus
  skill in the same change and regenerates `docs/book` with `just book`, which renders after
  `just test-docs` passes.
- `.agents/skills/plexus/SKILL.md` is the canonical, vendor-neutral, user-facing skill for
  configuring Nervix with Plexus. Use it when helping users author, explain, review or troubleshoot
  Plexus (agents without automatic skill discovery read it directly), and never turn it into a
  workflow for extending the parser, Models, compiler or runtime. Public guidance is not duplicated
  between `docs/src` and the skill: `SKILL.md` and its references may contain documentation routing,
  agent workflow and concise correctness checks, while detailed syntax, semantic explanations,
  rationale, examples and tuning guidance belong in `docs/src` and are read from there.
  `just validate-skill` validates the skill against the Agent Skills publication checks; CI runs
  that validation but creates no GitHub release, since the public default branch is the install
  source.
- `docs/book.toml` owns the allowlist of user-facing Plexus chapters rendered into the versioned
  `llms.txt` agent index, with no architecture, repository-development or local-operation material.
- `just plexus-completion-walk`, kept outside `just test`, fails on any completion branch that
  accepting the parser's suggestions cannot complete; grammar changes keep it passing, and a new
  finding is a defect, never a baseline entry. `just plexus-completion-walk-deep` relaxes
  deduplication to reach branches the gating budget stops short of.
- `just test-client-conformance [tags]` runs the cross-language client probes against one- and
  three-node clusters.
- `just chaos suite smoke|soak --image IMAGE` runs a chaos suite against the one already built image
  it names. Each suite runs against the release image and against the same revision's Deloxide
  diagnostic image, whose runs fail unless the deadlock evidence of every process its nodes started
  qualifies. A change to the chaos runner, or to the membership, failover, recovery or delivery
  behavior it verifies, runs the suite locally, and a change to the diagnostic image, its evidence
  collection or a path its scenarios exercise under tracked locks keeps both green.
- `just release-evidence <pull-request>` holds a pull request's head revision to
  `tests/release-evidence.toml`: every label-gated method's label is present, and the latest run of
  every required check passed on that revision. Its register lists each gap a tracked-lock owner
  declares, a reviewed declaration that does not refuse the revision, and a qualification task
  records its register on ClickUp. Renaming an inventoried CI job or adding a matrix value updates
  the inventory, which `just test-release-evidence` checks against the workflows' Deloxide and
  release-image matrices.

### CI labels

[Developing Nervix](docs/src/developing-nervix.md#ci-labels) lists every label that changes what CI
runs, and adding, removing or renaming one updates that table. Adding or removing any label cancels
and restarts a running Check, so set labels before the run you mean to keep.

- Carry the label of each optional check a change affects: `shuttle`, `loom` or `turmoil` for that
  mode, each running in its own job with its replay check or qualification; `client-conformance` for
  the client protocol, the shared binding or a probe; and, for the optional `extra-tests` job, which
  runs only the steps its labels select, `extra-compiler` for a compiler rule or fixture,
  `extra-primitives` for a primitive, `extra-completion-walk` for grammar, `extra-connector-matrix`
  for a connector declaration, `extra-runners` for a CI reporter, gate or collector,
  `extra-doctests` for an API with paired doctests, `extra-miri` and `extra-mutation` for the expiry
  map, and `extra-bench-smoke` for a benchmark. A new `extra-tests` step takes a label, and
  `tests/release-evidence.toml` requires every one.
- Carry `book` only when the task requires the rendered book (its build, theme, structure,
  screenshots or generated chapters, or a reviewer's preview); `build-book` then publishes the book,
  its PDF and the Plexus agent index and repoints the `snapshot` alias docs.nervix.io serves, as on
  a manual run, while `checks` runs `just test-docs` for every pull request. Carry `bolero-random`
  only when the task requires the randomized/corpus run itself, such as a change to it or a release
  qualification, `benchmark` only for a required measured comparison, and `no-docker` only when
  nothing needs an image.
- `fuzz`, `chaos`, `chaos-soak` and `deloxide` are expensive, so they stay off by default and their
  checks run locally first; `fuzz` is the preferred Bolero evidence. A pull request carries one only
  for a strong, task-specific need local verification cannot reasonably meet, not for touching
  related code; it enables the fewest lanes, and its description says why and records the local
  commands, revision, seeds or scenarios and results. Release qualification still needs them,
  because `just release-evidence` requires their checks. Once a label enabled a check, its result
  stands, and removing the label never hides a failure.
- An agent never changes a job's runner on its own: no `runs-on` label, runner size or provider, or
  the cache bucket that follows them, in any pull request. When CI malfunctions, it stops, reports
  the evidence to the user and asks for approval; a runner change lands only with the user's
  explicit approval for that change, never inside a feature or a fix.

### Finishing a change

- A final implementation report names the Cucumber scenario added or updated, or the explicit
  user-approved reason none was.
- A final report of concurrency-related work states whether the Deloxide rule applies. When it
  applies, the report names the lane runs of every selection, whether the registered workloads reach
  the changed path, and the inventory and owner records the change added; when the work is
  async-only or otherwise untracked, it records that the detector does not see it and names the
  Shuttle, Loom, Turmoil, Chaos or Bolero evidence that covers it instead.
- After completing requested repository changes, the final response proposes a Conventional Commit
  title and description together in one fenced code block, with the type selected from the current
  `type-enum` in `./commitlint.config.js` by [Commit Types](#commit-types). The title and each
  description paragraph stay on one unwrapped line, because Git and GitHub handle display wrapping.
  A follow-up change regenerates both, so they describe the complete resulting change.

## Commit Types

A commit or pull request title takes its type from the `type-enum` in `./commitlint.config.js`, and
each type has exactly one meaning:

- `feat` adds or extends a product feature: something a user of Nervix, a developer building on it,
  or an operator or administrator running it can see.
- `fix` repairs an actual product defect. A repair to a test is `test`, and a repair to CI or a
  script is `build`; neither is ever `fix`.
- `perf` improves the performance of the product. Faster tests are `test` and faster scripts are
  `build`; neither is ever `perf`.
- `refactor` changes product code without changing its functionality or its performance; a product
  code change that also affects performance is `perf`.
- `style` changes style, such as formatting, anywhere in the repository.
- `test` covers anything related to tests of every kind, including new tests and fixes to existing
  ones.
- `build` changes scripts, `justfile` recipes, CI workflows, how Cargo is invoked, and other build
  and CI tooling.
- `doc` changes the published product documentation, such as the book under `docs/src`, or
  documentation in code.
- `chore` covers anything no other type matches, and a change whose affected part is hard to state
  clearly.
- `version` is the commit that changes the product version.
- `dep` changes Cargo dependencies, typically by upgrading them.
- `security` is the preferred type for a security issue, including a dependency upgrade.
- `revert` reverts an earlier commit.
- `drop` removes product functionality.
- `agent` changes the instructions for AI agents: `AGENTS.md` and the agent instructions it
  references, such as the skills under `.agents/skills`.
- `bench` changes benchmarks only.

Choose the most relevant type even where types intersect. A type names the change as a whole rather
than every part it touches: a feature adds tests and is still `feat`, and a fix adds its reproducer
and is still `fix`. Where two types describe the same change, the more specific one is the more
relevant: a dependency upgrade that fixes a vulnerability is `security`, not `dep`. A change to the
`type-enum` keeps this section current in the same change.
