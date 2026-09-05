# AGENTS.md

## Purpose

Nervix is a domain-owned streaming system whose public language is NSPL. This file records the
repository's durable architecture and correctness rules. It intentionally does not mirror the
current module tree, enumerate parser helpers, or restate the complete NSPL grammar; those details
belong in the code, public specification, and tests.

When code and this document disagree, do not add a compatibility path around the disagreement.
Resolve the owning model and invariant directly, and update the public documentation when the
language or interface changes.

## Alpha Stability and Compatibility

NSPL has reached alpha. Its period of very active language experimentation is over, and large
backward-incompatible changes are no longer expected. The language will continue to evolve during
alpha, however, and focused breaking changes may still be made when they materially improve its
model or correctness.

Nervix supports exactly one shape of everything: the current one. Until an explicit compatibility
policy is introduced, there is no migration, dual read, version negotiation, or deprecation window.
When a Model, stored shape, NSPL form, protocol message, or public interface changes, the previous
form is deleted outright in the same change.

When a stored shape changes, previously written data using that shape is expected to break. It must
fail to load with a clear error and be recreated. It must never be reinterpreted, defaulted into the
current shape, or routed through a second code path.

The following are defects, not style preferences. Treat any of them as a change to reject:

- Types, variants, or fields that exist only to represent a removed or earlier form, however they are
  spelled: `RemovedTransport`, `LegacyFoo`, `FooV1`, `OldBar`, `CompatBaz`.
- Decoding, `TryFrom`, or deserialization arms that name an older shape, including arms whose only
  behavior is to reject it. Naming the old shape is itself the violation; delete it instead.
- Fields retained, or newly added fields defaulted, so that previously written data still loads.
- Fallbacks, feature gates, or configuration that select between an earlier behavior and the current
  one.
- Identifiers containing `legacy`, `old`, `deprecated`, `compat`, `removed`, or a bare version
  number, in types, fields, variants, modules, or functions.

Tests are held to the same rule. Do not reconstruct a removed Model, stored shape, or wire form in a
test, not even to assert that the current code rejects it. Defining the historical shape is exactly
what this rule forbids, and such a test pins a shape the product no longer has. Test the current
shape only.

Until Nervix adopts an explicit backward-compatibility policy, removing behavior or output also
removes the assertions that named it. Do not replace a positive assertion for an eliminated form
with a negative assertion that the old form is absent, rejected, or no longer emitted. Assert the
current shape and behavior directly, without memorializing the transient form it replaced.

Removing an integration or feature removes all of it in one change: Models, grammar, stored shapes,
runtime paths, configuration, documentation, and tests. Never leave a placeholder behind so that old
data still loads.

The only permitted exceptions are an external contract where pass-through is the intentional public
behavior, and a compatibility requirement the user states explicitly for the current change.

## Architecture

### Language pipeline

- NSPL is lexed and parsed once into structured public semantic Models.
- Parser-only spans and recovery state remain internal to parsing and diagnostics.
- Completion is derived from expectations of the composed top-level grammar. REPLs and clients
  must not merge suggestions from independent feature parsers.
- Statement families own their domain grammar, while shared lexical and grammatical concepts have
  one shared implementation.
- Keyword and type literals belong to the shared language token definition, not grammar-local raw
  string matching. Multi-word logical keywords are composed grammar units so parsing and completion
  treat them as one item.
- Semantic reference kinds and repeated grammar patterns use focused shared parser primitives, not
  ad hoc word parsers or duplicated statement-local grammar.
- Grammar labels use underscore style, such as `field_name` and `string_literal`. Completion labels
  for composed keyword phrases use their human-readable space-separated form.
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
- Every server-side runtime entity that binds a configured listening port executes on every live
  Nervix node. Its listener is independent of leadership and placement and must remain present
  across all cluster events, including leader changes, node joins, node restarts, and recovery.
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
- Persisted Models may break, and previously written data must fail clearly and be recreated. See
  [Alpha Stability and Compatibility](#alpha-stability-and-compatibility) for what that forbids in
  code and tests.

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

### Columnar data plane

- Arrow columnar batches are the only data-plane payload representation, end to end: ingest
  decoding, relay carriage, VM input and output, stateful processor state, error handling,
  emission, sessions, and interconnect. A batch with one row is still a batch.
- Do not introduce, reintroduce, or extend row-oriented payload representations: no
  `HashMap`-of-fields records, no per-row boxed value maps, no dual row-plus-column carriers kept
  in sync. Where a single message must be addressed, use a row view over an Arc'd Arrow batch
  (batch reference plus row index), never a materialized row copy.
- Scalar value enums are permitted only for genuinely scalar concerns: branch keys, literals,
  configuration, and boundary key extraction. They are never a payload container.
- Program inputs are built by projection: reuse the carrier batch's Arc'd columns and construct
  only genuinely new columns (uninitialized outputs, broadcast state, computed lookups).
  Rebuilding a column from row data that already exists in columnar form is a defect.
- Wire codecs decode directly into typed Arrow builders and encode directly from column values at
  the external boundary. Decoded intermediate row maps are forbidden.

### Ownership and execution policy

- The type or crate that owns a dangerous operation must expose one public API that enforces its
  execution policy. Do not leave unsafe synchronous or reactor-blocking alternatives publicly
  callable beside a safe wrapper.
- Callers must not choose blocking, yielding, batching, throttling, or similar safety behavior
  unless it is an explicit typed part of the operation's contract.
- In async code, a loop whose body performs async work must call
  `tokio::task::consume_budget().await` once per iteration near the top of the loop body.

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

- Keep Rust modules organized around coherent ownership boundaries, not broad technical categories.
  Do not create grab-bag `clients`, `helpers`, `utils`, or `common` modules. Shared logic belongs in
  a focused abstraction; concrete setup, configuration interpretation, state transitions, I/O,
  encoding, decoding, and lifecycle behavior belong to the struct that owns them.
- Behavior that naturally belongs to one model, AST, runtime object, or domain type must be an
  inherent method using `&self`, `&mut self`, or `self`, including private helpers. Boolean
  predicates over enum variants are methods on the enum.
- Use free functions only for symmetric operations over independent types, parser-combinator
  plumbing, or small lexical primitives without a natural owner. Do not use them as convenience
  helpers for behavior owned by one domain object.
- Prefer compact refactors that collapse duplicate logic into the owning parser, model, or runtime
  path and remove obsolete layers. Add a wrapper, adapter, or helper layer only when it reduces
  total complexity or isolates a real boundary.
- Model internal special cases with typed variants or internal-only structures, never magic or
  reserved user-visible identifiers that can collide with user-defined names.
- Use semantic typed errors. Domain error enums use `thiserror`, contextual propagation uses
  `error-stack`, and `anyhow` is limited to boundaries where callers cannot make semantic choices.
  Do not introduce `String` as a domain error type.
- Prefer deriving declarative enum string conversions and metadata with `strum`, including
  `AsRefStr`, `EnumString`, and `EnumProperty`, over manual match-based helpers.
- When sorted vectors or arrays are an invariant, use `sorted-vec`'s `SortedVec` or `SortedSet`
  instead of a plain `Vec` with manual sorting and deduplication.
- Prefer synchronous locks from `parking_lot` over `std::sync` lock types.
- Prefer `DashMap` over `Arc<Mutex<HashMap<...>>>` for shared concurrent maps.
- `triomphe::Arc` is the default shared-ownership type for Nervix-owned state. Use
  `std::sync::Arc` only when weak references or an external API require it. In modules that need
  both, import the standard type as `StdArc` and confine it to that boundary.
- In `if` conditions, prefer `if let` or `if let` chains over `matches!` when they express the same
  logic cleanly. Use `matches!` when an `if let` form would be unclear or outside an `if`
  condition.
- Use semantic datetime or timestamp types for internal runtime, application, persistence, and
  domain-clock state. Raw Unix integers are not internal time models.
- Nanosecond Unix integers are boundary representations for serialization, public or cross-node
  protocols, and Arrow timestamp arrays. Convert them to typed timestamps immediately after
  decoding and back to integers only at the boundary.
- Parse URLs and URL-like service addresses with the `url` crate. Do not manually split schemes,
  authorities, hosts, ports, paths, or query strings unless the format is not URL-compatible.
- Do not add `#[allow(...)]` annotations without explicit approval.
- Do not use `std::mem::forget`, `Closure::forget`, leaked boxes, or equivalent lifetime leaks to
  keep browser callbacks alive. Use explicit ownership and cleanup or a crate-managed abstraction.
- Do not preserve old syntax, implicit fallbacks, compatibility shims, migration shortcuts, or
  parallel legacy paths. See
  [Alpha Stability and Compatibility](#alpha-stability-and-compatibility).
- Preserve unrelated user changes in dirty worktrees.

## Specifications

- Specifications and proposals are product documents. They define externally observable
  behavior: concepts, states, guarantees, interfaces, limits, failure semantics, and behavior
  changes. Implementation material — code sketches, type definitions, file or module lists,
  line references, and change-size estimates — does not belong in a spec.
- A requested specification is one complete document. Do not split it into implementation
  phases, milestones, or staged deliveries unless the user asks for phasing.
- A specification must be correct and fully resolved. Verify its claims against the current
  system, decide every design question in the document, and do not defer, postpone, or mark
  anything as future work, a fast-follow, or an open question unless the user requested or
  agreed to that deferral.

## Validation and Testing

### Test-first changes

- For a bug, first add or identify a focused test or cucumber scenario and confirm that it fails for
  the expected reason. Implement only after the reproducer is red, rerun it until green, then run
  the appropriate broader validation.
- For new runtime, persistence, API, CLI, scheduling, cluster, metric, or domain behavior, first add
  or update a cucumber scenario through the public interface and confirm the expected failure
  before changing product code.
- Parser-only work requires positive parse tests, negative parse tests, and completion-context tests
  that guard against grammar-branch leakage. Composed language phrases require completion coverage.
- Tests cover the current shape only. Never define a removed Model, stored shape, or wire form in a
  test fixture, including to assert that loading it fails. See
  [Alpha Stability and Compatibility](#alpha-stability-and-compatibility).

### Integration coverage

- Unit tests support but do not replace cucumber coverage for behavior observable through an NSPL
  command, HTTP or public API call, cluster state, runtime output, or persisted state. Adding an
  executable statement with application or runtime handling is not parser-only. Do not substitute
  unit tests unless the user explicitly approves the exception for the current task.
- Cucumber scenarios keep step semantics aligned: `Given` establishes preconditions, `When`
  performs the action or event under test, and `Then` asserts the observable outcome.
- Runtime cucumber behavior uses scenario outlines covering one-node and three-node clusters unless
  the behavior is explicitly topology-specific. Every three-node Cucumber cluster uses the
  conditionally compiled random test scheduler by default, independently assigning schedulable graph
  nodes to exercise cross-node paths instead of locality-driven colocation. Random assignments must
  remain stable for unchanged domain, model, and topology inputs so periodic reconciliation never
  causes artificial ownership churn. Scenarios whose subject is production placement, cordon, drain,
  scheduled-node failover, or full-cluster recovery from node-owned persisted state must explicitly
  configure the production sticky scheduler before cluster startup. Scheduling, leader/follower
  restrictions, and inter-node mechanics use dedicated scenarios or lower-level tests rather than
  being folded into generic runtime scenarios.
- Stateful processor scenarios must prove branch isolation and field preservation with interleaved
  records from at least two branches.
- Tests must explicitly provision required external entities.
- Avoid blind sleeps. Wait for explicit conditions with bounded timeouts and useful failure
  messages. Short polling intervals are acceptable only inside such a condition-based wait.
- Browser behavior is tested through the standard web-console cucumber suite and Playwright-facing
  steps, not by bypassing the public browser flow.

### Repository commands and documentation

- `.agents/skills/nspl/SKILL.md` is the canonical, vendor-neutral, user-facing skill for configuring
  Nervix with NSPL. Use it when helping users author, explain, review, or troubleshoot NSPL;
  agents without automatic skill discovery must read it directly. Do not turn it into a workflow
  for extending the parser, Models, compiler, or runtime.
- Avoid duplicating public guidance between `docs/src` and the NSPL skill. `SKILL.md` and its
  references may contain documentation routing, agent workflow, and concise correctness checks;
  detailed syntax, semantic explanations, rationale, examples, and tuning guidance belong in
  `docs/src` and should be read from there rather than restated in the skill.
- Use `just validate` for formatting and validation; do not invoke Cargo formatting directly.
- Architecture debt is counted and only decreases. `just ratchet` counts oversized files, `as`
  casts, bare `unwrap` and `expect`, `Result<_, String>`, signatures returning a Nervix error
  without `Report`, node identities carried as `String`, struct fields gated on
  `cfg(feature = "testing")`, parser references outside the language edges, and `Model` references
  in the data plane, and CI fails when a count is above `debt-baseline.json`. A change may lower a
  count and never raise one. When a count falls, run `just ratchet --update` and commit the
  baseline in the same change; `just ratchet --show <count>` lists the sites behind one count.
- Every Rust build, check, lint, and test invocation must use the repository-configured kache
  compiler wrapper. Never unset, clear, or override `RUSTC_WRAPPER`, including for diagnostics,
  benchmarks, cache troubleshooting, or retries.
- Use `just validate-skill` to validate the public NSPL skill against the Agent Skills publication
  checks. CI runs this validation but does not create GitHub releases; the public default branch is
  the install source.
- `docs/book.toml` owns the curated public NSPL chapter allowlist rendered into the versioned
  `llms.txt` agent index. Keep that list focused on user-facing NSPL configuration and exclude
  architecture, repository-development, and local-operation material.
- After each development cycle, run `just validate` and any focused tests added or changed.
- Use `just test` for the full suite so repository-required environment is configured.
- Use `just test-scenarios --input <feature> ...` for targeted cucumber runs so the configured test
  environment is applied. Add a focused `justfile` task when a needed invocation is not represented
  instead of running the scenario test binary directly.
- `just nspl-completion-walk` walks the NSPL completion graph and fails on any branch that cannot be
  completed by accepting the suggestions the parser offers. It is deliberately outside `just test`
  and runs in CI after the main tests. Grammar changes that add a label must keep it passing; a new
  finding is a defect, not something to record in its baseline. `just nspl-completion-walk-deep`
  relaxes deduplication to reach branches the gating budget stops short of.
- Every public interface or NSPL surface change must update the relevant `docs/src` pages and the
  user-facing NSPL skill in the same change. Keep `.agents/skills/nspl/SKILL.md` and its references
  accurate for users configuring Nervix, then regenerate `docs/book` with `just book`.
- Keep parser tests near the grammar they protect.
- Final implementation reports must name the cucumber scenario added or updated. If none was added,
  state the explicit user-approved reason.
- After completing requested repository changes, include a proposed Conventional Commit title and
  description in the final response. Follow Conventional Commits and select the title type from the
  current `type-enum` in `./commitlint.config.js`. Put the title and description together in one
  fenced code block. Keep the title and each description paragraph on a single unwrapped line; do
  not insert manual line breaks within them because Git and GitHub handle display wrapping.
- When the user requests follow-up changes, regenerate both so the final response contains an
  updated title and description that reflect the complete resulting change instead of a stale
  earlier proposal.

## Error and Diagnostic Quality

- Parse errors should report precise expected and found tokens and retain source spans suitable for
  diagnostic rendering.
- Prefer semantic labels over generic messages.
- Validation errors must identify the owning node, route, operation, and relevant fields without
  including sensitive payload values.
