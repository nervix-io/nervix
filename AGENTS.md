# AGENTS.md

## Purpose

Nervix is a domain-owned streaming system whose public language is NSPL. This file records the
repository's durable architecture and correctness rules. It intentionally does not mirror the
current module tree, enumerate parser helpers, or restate the complete NSPL grammar; those details
belong in the code, public specification, and tests.

When code and this document disagree, do not add a compatibility path around the disagreement.
Resolve the owning model and invariant directly, and update the public documentation when the
language or interface changes.

## Architecture

### Language pipeline

- NSPL is lexed and parsed once into structured public semantic Models.
- Parser-only spans and recovery state remain internal to parsing and diagnostics.
- Completion is derived from expectations of the composed top-level grammar. REPLs and clients
  must not merge suggestions from independent feature parsers.
- Statement families own their domain grammar, while shared lexical and grammatical concepts have
  one shared implementation.
- The compiler consumes semantic Models directly. Persisted or public Models must never contain raw
  executable NSPL that the runtime reparses.
- Grammar keywords are case-insensitive language tokens. Original spelling may be retained for
  diagnostics, but unknown words are not a substitute for declaring language keywords.
- External connector configuration remains raw only where pass-through is the intentional public
  contract.

### Control plane and runtime

- Models and execution-graph configuration are control-plane state and are strongly persisted.
- The registry validates domains, references, schemas, branches, capabilities, and execution
  contracts before a graph becomes active.
- Runtime execution is materialized per concrete branch. Branch-local state, scheduling, buffering,
  and materialized views must remain visibly branch-local in types and ownership.
- Selected execution state may use node-owned snapshot or replication mechanisms.
- Records, batches, payload attempts, handoff state, ACK guards, ACK tokens, and ACK maps are
  in-memory hot-path state and are never persisted.
- Connectors adapt external systems at explicit data-plane boundaries. They do not weaken internal
  schema, branch, error, or sensitivity rules.

## Language and Model Invariants

### Structured semantics

- Every executable expression is a structured Model: filters, assignments, inheritance,
  `VALUES`, invocations, ordering, deduplication keys, correlations, window expressions,
  inferencer mappings, and error mappings.
- Models preserve every semantically significant order, including routes, assignments,
  invocations, and materialized-state dependencies.
- Do not introduce a second public parser AST, raw-expression compatibility fields, or runtime
  parser adapters.
- Persisted pre-alpha Models may break. Old incompatible data must fail clearly and be recreated;
  it must not be silently reinterpreted or migrated through a legacy runtime path.

### Types and sensitivity

- All schemaful operations are exact-type operations. Implicit casts, parsing, stringification,
  numeric widening or narrowing, datetime/string interchange, and wire/internal coercions are
  forbidden.
- Nullability is part of the type contract. Required uninitialized fields are errors; optional
  uninitialized fields finalize as typed nulls.
- Sensitivity may be promoted but never downgraded implicitly.
- External emission of sensitive values requires explicit leakage, including codec payloads,
  direct `VALUES`, inherited fields, and written headers.
- Multiple ordinary inputs to one processor must reference the same declared schema Model. They
  must also share the exact named branch, or all be unbranched. Correlators apply the same rule
  independently to the left and right sides.

### Construction

- Construction is route-local. There is no global `SET`, global `INHERIT`, or implicit input
  identity transformation.
- Transforming routes begin empty and may use `INHERIT` and ordered `SET`.
- Generated and other set-only routes begin empty, do not support `INHERIT`, and must explicitly
  initialize required outputs. Generated state is immutable and independent for every route.
- During normal set-only construction, inferencers and WASM processors do not expose `message` or
  `input`; additional schema-backed data comes from declared materialized state.
- Window output is set-only. Live `input` values are available only inside aggregate arguments,
  although aggregate calls may participate in larger scalar expressions. Aggregates may not be
  nested inside aggregates.
- Correlators use explicit `left` and `right` scopes and never expose an ambiguous default input.
- Relay names are graph references, never top-level field qualifiers. Materialized state is the
  explicit exception through the `relay_state.<relay>.<field>` namespace.

### Branches

- Unbranched execution is represented by an absent branch key. A concrete branch key is non-empty
  and typed. Never encode unbranched execution with an empty string, empty map, zero-field schema,
  synthetic root branch, or reserved user identifier.
- Ingestors construct outgoing branches route by route.
- Reingestors are branch-boundary nodes: each route may preserve the incoming branch, construct a
  different branch, or become unbranched.
- Junctions, deduplicators, reorderers, window processors, inferencers, WASM processors,
  correlators, and generators preserve branch identity. Their branch declaration is node-wide and
  every input and output must use that exact named branch.
- Emitters collapse branch identity only at the successful external boundary. They consume every
  concrete source branch but do not expose branch fields as expression values.
- Normal processors must never subscribe to a mixed logical relay, combine branch keys, share
  mutable processing state across branches, or publish through a fallback that bypasses concrete
  branch routing.
- Error routes preserve the branch in which the failed operation executed. Their target relay must
  have the same exact branch declaration; ingestor errors are unbranched.

### Materialized state

- Materialized dependencies are explicit, node-wide, same-domain, and compatible with the node's
  exact branch. State is never scanned across branches.
- Dependency declarations execute strictly in written order. Available state binds immediately;
  defaults bind typed constant records; `REQUIRED SKIP` skips the message; `REQUIRED WAIT` retains
  it in memory and applies backpressure. Resolution restarts from the first declaration after a
  wait wakes.
- Duplicate dependencies are invalid. Defaults must initialize every required field; omitted
  optional fields become typed nulls.
- Individual materialized records may become ready and update independently, but eviction occurs
  for the whole concrete branch and drops its suspended or buffered branch-local work.
- Ingestors cannot access materialized state. Reingestors resolve state in the incoming branch,
  never a partially constructed outgoing branch.
- A generator declares exactly one materialized relay using the simplified dependency form. Branch
  appearance starts a branch-local generator task; branch eviction drops the task and buffered
  output. Generators may have multiple routes, all preserving that source branch.

### Flush and route errors

- `FLUSH` and `ON MESSAGE ERROR` are route-local contracts.
- Every flush-based route requires either `FLUSH EACH <duration> MAX BATCH SIZE <bytes>` or
  `FLUSH IMMEDIATE`. Hidden defaults and optional fallback cadences are forbidden.
- Emitters always require a flush policy. Iceberg also requires an explicit commit cadence and
  maximum commit size.
- Window processors use `WIDTH` and `STEP` instead of `FLUSH`.
- WASM processors are not Nervix flush-based nodes; guest output and guest-requested timeouts own
  their emission cadence.
- General or global error policy is node-wide.
- Message errors are structured and carry a stable reference, code, operation, affected fields,
  timestamp, and non-sensitive message. Error handlers may inspect the original eligible input,
  the exact captured materialized-state snapshot, and an all-optional `partial_output` view.
- Error handling must not recursively invoke the same policy when constructing the error record
  fails.

### Connector envelopes and subscriptions

- Header capabilities are connector-owned. Preserve the existing `read_header`, `read_headers`,
  and `write_header` semantics of each underlying system; do not introduce generic envelope
  extraction or injection abstractions.
- Header values do not propagate through relays unless copied into schema-backed fields.
- Kafka ingestion metadata remains a typed integration-specific scope.
- Session subscriptions retain their existing creation, deletion, delivery, sampling, and
  backpressure semantics. They are read-only filtered views and do not support construction,
  inheritance, values, or side effects.

## Runtime and Infrastructure Invariants

### Ownership and execution policy

- The type or crate that owns a dangerous operation must expose one public API that enforces its
  execution policy. Do not leave unsafe synchronous or reactor-blocking alternatives publicly
  callable beside a safe wrapper.
- Callers must not choose blocking, yielding, batching, throttling, or similar safety behavior
  unless it is an explicit typed part of the operation's contract.
- Async loops that perform async work must cooperatively consume scheduler budget once per
  iteration.

### Domains and external systems

- Domains are explicit and must already exist. There is no implicit, default, or auto-created
  runtime domain.
- All entities are domain-owned. A globally scoped legacy lookup is technical debt, not desired
  behavior.
- Nervix must not create broker topics, queues, streams, database objects, object-store objects,
  catalogs, namespaces, tables, buckets, or collections as a side effect of node startup or data
  processing.
- External entities must be provisioned explicitly by test setup, deployment policy, or operator
  action. Missing entities surface as initialization or publish errors.

### Observability

- `info` is reserved for lifecycle, topology, startup, shutdown, administration, and unusual state
  transitions.
- Per-message, per-record, per-batch, payload-bearing, duplicate-drop, and branch-churn details
  belong at `debug` or `trace`.
- Hot-path logs and structured errors must not expose sensitive payload values.

## Engineering Conventions

- Organize modules around coherent domain ownership, not broad `helpers`, `utils`, or `common`
  categories.
- Behavior that naturally belongs to one model, runtime object, or domain type must be an inherent
  method. Free functions are for symmetric multi-type operations, parser-combinator plumbing, or
  small lexical primitives without an owner.
- Prefer compact refactors that remove obsolete layers and duplicate logic. Do not add wrappers or
  adapters merely to preserve superseded behavior.
- Model internal special cases with typed variants or internal structures, never magic
  user-visible identifiers.
- Use semantic typed errors. Domain error enums use `thiserror`, contextual propagation uses
  `error-stack`, and `anyhow` is limited to boundaries where callers cannot make semantic choices.
  Do not introduce `String` as a domain error type.
- Prefer declarative enum metadata and conversions, invariant-preserving collections, and the
  repository's established concurrency primitives over manual mappings and ad-hoc lock-wrapped
  maps.
- Use typed timestamps internally. Raw Unix nanoseconds are boundary representations only.
- Parse URL-like addresses with a URL parser rather than manual string splitting.
- Do not add lint allowances without explicit approval.
- Do not leak memory to extend callback lifetimes; callbacks require explicit ownership and cleanup.
- Preserve unrelated user changes in dirty worktrees and do not add legacy compatibility unless
  the current task explicitly requires it.

## Validation and Testing

### Test-first changes

- For a bug, first add or identify a focused reproducer and confirm that it fails for the expected
  reason. Implement only after the reproducer is red, then rerun it until green.
- For new runtime, persistence, API, CLI, scheduling, cluster, metric, or domain behavior, first add
  or update a cucumber scenario through the public interface and confirm the expected failure.
- Parser-only work requires positive parse tests, negative parse tests, and completion-context tests
  that guard against grammar-branch leakage. Composed language phrases require completion coverage.

### Integration coverage

- Unit tests support but do not replace cucumber coverage for observable system behavior.
- Runtime cucumber behavior uses scenario outlines covering one-node and three-node clusters unless
  the behavior is explicitly topology-specific.
- Stateful processor scenarios must prove branch isolation and field preservation with interleaved
  records from at least two branches.
- Tests must explicitly provision required external entities.
- Avoid blind sleeps. Wait for explicit conditions with bounded timeouts and useful failure
  messages.
- Browser behavior is tested through the standard web-console cucumber suite and Playwright-facing
  steps, not by bypassing the public browser flow.

### Repository commands and documentation

- Use `just validate` for formatting and validation; do not invoke Cargo formatting directly.
- Use `just test` for the full suite so repository-required environment is configured.
- Use the repository's scenario task for targeted cucumber runs. Add a focused task when a needed
  invocation is not already represented instead of bypassing the configured test environment.
- Regenerate public documentation whenever the public interface or NSPL surface changes.
- Keep parser tests near the grammar they protect.
- Final implementation reports must name the cucumber scenario added or updated. If none was added,
  state the explicit user-approved reason.

## Error and Diagnostic Quality

- Parse errors should report precise expected and found tokens and retain source spans suitable for
  diagnostic rendering.
- Prefer semantic labels over generic messages.
- Validation errors must identify the owning node, route, operation, and relevant fields without
  including sensitive payload values.
