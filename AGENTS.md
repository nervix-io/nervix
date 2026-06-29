# AGENTS.md

## Purpose
This repository containimplements NSPL parsing with Chumsky.
Primary goals:
- Keep grammar and completion in a single source of truth.
- Keep parser modules focused by domain (schema/transport/ingestor).
- Preserve pass-through semantics where required (e.g. Kafka config key/value).

## Current Architecture
- `crates/nspl/src/lexer.rs`
  - Char-level lexer.
  - Emits `Token` + span (`SpannedToken`).
  - Classifies words into `KnownWord { iden, raw }` and `UnknownWord(raw)`.
  - `Identifier` is case-insensitive (`strum`) and retains `raw` spelling in tokens.

- `crates/nspl/src/parser_support.rs`
  - Shared token-level parser primitives and diagnostics.
  - Contains common helpers (`kw`, `kw_phrase2`, `tok`, `word_raw`, `string_lit`).
  - Converts Chumsky errors to source-span diagnostics.
  - Provides expected-token based suggestion extraction.

- `crates/nspl/src/schema/`
  - `mod.rs`: shared schema grammar + AST + parse/suggest entrypoints.
  - `json.rs`: JSON native type enum.
  - `avro.rs`: AVRO native type enum.
  - `parse_as.rs`: reusable internal parse-as type enum.

- `crates/nspl/src/transport/`
  - `mod.rs`: `CREATE TRANSPORT ... TYPE KAFKA CONFIG { ... }` grammar.
  - Transport config is parsed as raw key/value strings (pass-through style).

- `crates/nspl/src/ingestor/`
  - `mod.rs`: `CREATE INGESTOR ...` grammar and mode variants.

- `crates/nspl/src/emitter/`
  - `mod.rs`: `CREATE EMITTER ... FROM RELAY ... ENCODE USING ... INTO KAFKA ...` grammar.

- `crates/nspl/src/statement.rs`
  - Top-level statement parser/suggester.
  - Must compose all statement subgrammars.
  - REPL/autocomplete should use this module, not per-feature suggestion merging.

- `crates/nspl/examples/parse.rs`
  - Interactive CLI (rustyline).
  - Uses top-level statement parse/suggest.
  - Renders errors with Ariadne.

## Non-Negotiable Design Rules
1. Single-source grammar for completion:
   - Completion must be derived from parser expectations of the top-level grammar.
   - Do not merge independent feature suggesters in the CLI.

2. Shared language primitives stay shared:
   - Common punctuation/keyword parsing belongs in `parser_support`, not feature modules.

3. Preserve raw values where needed:
   - Transport Kafka config keys/values are not semantically interpreted.
   - Keep them as strings suitable for direct librdkafka pass-through.

4. Case-insensitive keywords:
   - `Identifier` matching remains case-insensitive.
   - Keep original raw token text where useful.

5. Grammar must rely on known words:
   - Do not use raw string-literal keyword matching in grammar paths.
   - If grammar needs a keyword/type token, add it to `lexer::Identifier` and parse with `kw(Identifier::...)`.
   - `UnknownWord` should not be used for language keywords.

6. Composed keywords:
   - Multi-word logical keywords (e.g. `PARAMETERIZED BY`) should be represented as composed grammar units.
   - Use `kw_phrase2(...)` (or future phrase helpers) so parser and autocomplete treat them as one logical item.
   - `kw_phrase2` must derive its completion label from identifiers (`FIRST SECOND`); do not pass custom label literals.
   - Autocomplete is allowed to return multi-word suggestions when grammar expects a composed keyword.

7. Reference parser helpers and labels:
   - Do not use ad-hoc `word_raw().labelled(...)` at grammar call sites.
   - Add and use dedicated helper parsers in `parser_support` for each semantic reference kind (e.g. `schema_ref`, `relay_ref`, `transport_ref`).
   - Labels must use underscore style (`field_name`, `string_literal`, `config_key`) and never spaces.
   - Exception: composed keyword phrase labels used for completion (`kw_phrase2`) should be human phrase form with spaces (e.g. `PARAMETERIZED BY`, `STEP BY`), and underscore variants must not be suggested.

8. Parser code reuse:
   - Do not duplicate parser logic across statement modules.
   - When a grammar pattern appears in multiple statements, extract it into a shared helper (typically in `parser_support` or a focused shared submodule) and reuse it.

9. Branch parameterization is explicit and must be preserved:
   - Empty schemas are not valid NSPL, so an empty branch key is impossible.
   - Model non-branched execution as an absent branch key and branched execution as a non-empty typed key. Do not encode non-branched execution as an empty string, empty map, zero-field schema, or synthetic root branch.
   - There are exactly two node families that may erase or cross branch boundaries:
     `REINGESTOR` may change/repartition branch parameterization, and `EMITTER` may fan in records for external output.
   - Every other processing node (`DEDUPLICATOR`, `ROUTER`, `JUNCTION`, `WINDOW PROCESSOR`, and future processing nodes unless explicitly promoted to this list) must execute inside one concrete branch at a time.
   - Non-reingestor/non-emitter processors must consume concrete relay instances and publish concrete relay instances for the same branch parameterization. They must not subscribe to the logical/global relay, mix branch keys, aggregate across branches, or use a shared processor state for multiple concrete branches.
   - `DEDUPLICATOR`, `ROUTER`, `JUNCTION`, and `WINDOW PROCESSOR` must preserve the branch parameterization they receive.
   - A global mixed-consumer implementation for a normal processor is a correctness bug, even if it is simpler or appears to work for singleton cases. Do not implement fallback paths that bypass concrete branch routing.
   - Runtime code should make this invariant obvious in names, types, and comments. Processor templates/nodes should carry concrete branch context; any logical-stream registration path must explicitly reject or skip normal processors that are handled by branch materialization.
   - Tests and cucumber scenarios for processors must explicitly prove branch isolation/parameterization preservation, including interleaved records from at least two branches when stateful behavior is involved.

10. Flush policy is an explicit node contract:
   - Any runtime node that buffers records behind a flush boundary must require either `FLUSH EACH <duration>` or `FLUSH IMMEDIATE` in its NSPL grammar and typed model.
   - Do not implement optional flush cadence, hidden runtime defaults, or ad-hoc immediate-flush fallbacks for flush-based nodes.
   - New flush-based node implementations must make this requirement visible in the AST/model shape by storing the flush policy as a mandatory field.
   - Window processors are the exception: they use `WIDTH` and `STEP` for their scheduling contract. Emitters are terminal drains and do not declare `FLUSH EACH`.
   - WASM processors are also not flush-based Nervix nodes. Do not add `FLUSH EACH` or `FLUSH IMMEDIATE` to `CREATE WASM PROCESSOR`; the guest controls output emission through returned process batches and guest-requested timeouts.

11. Dangerous execution policies must have a single public owner:
   - If an operation is unsafe to run directly in some contexts (for example CPU-bound Arrow kernel execution inside a Tokio reactor), do not leave the unsafe/legacy path publicly callable and add a wrapper beside it.
   - The crate/type that owns the operation must expose one public API that enforces the required execution policy internally.
   - Synchronous or otherwise unsafe lower-level execution functions may exist only as private implementation details or private tests, never as public compatibility paths.
   - Callers must not be responsible for choosing whether a dangerous operation uses `spawn_blocking`, yielding, batching, throttling, or another safety mechanism unless that choice is an explicit part of the operation's typed public contract.

12. Runtime persistence boundaries are explicit:
   - Execution graph configuration is control-plane state and must be persisted with strong consistency guarantees.
   - Execution node state is runtime state and may be persisted through periodic snapshot/replication mechanisms owned by the node state implementation.
   - Message streaming is the hot path. Records, relay batches, processor handoff, emitted payload attempts, ACK guards, ACK tokens, and ACK maps are never persisted as runtime state.
   - Do not implement durable event-log semantics, per-message persistence, or persisted ACK recovery in runtime/interconnect hot paths. If hot-path state is lost, sources and ingestors must react according to their delivery mode, offsets, and retry policy.
   - Documentation and user-facing descriptions must preserve this distinction: strong persistence for graph configuration, snapshot persistence for selected execution state, and in-memory-only handling for in-flight messages and ACKs.

13. External data-plane entities are explicit prerequisites:
   - Runtime ingestors and emitters must not implicitly create external broker, queue, stream, database, object-store, catalog, namespace, table, bucket, or collection entities as a side effect of start, subscribe, publish, flush, or commit.
   - Nervix nodes may rely on entities created by an external service policy, such as broker-side Kafka topic auto-creation, but Nervix runtime code must not issue the creation request itself.
   - Examples and cucumber scenarios must provision required external entities explicitly through setup steps, scripts, or `just` recipes before starting the Nervix graph.
   - Missing external entities must surface as initialization or publish errors instead of being hidden by runtime create-if-absent behavior.

## Style & Evolution Guidance
- Add new statement families as separate modules.
- Register each new statement in `statement::Statement` and `statement_parser()`.
- Extend REPL capabilities only via top-level parser APIs.
- Always regenerate `./docs/src` when changing the public interface or documented NSPL surface.
- Prefer explicit, typed ASTs over ad-hoc maps except where pass-through is intentional.
- Error types must be semantic and typed. Use `thiserror` for domain error enums and carry context with `error-stack::Report`; use `anyhow` only in narrow places where callers cannot make a semantic decision from the error. Do not introduce `String` as an error type.
- All schemaful operations are type-strict. A value flowing into a schema field, parameterization field, relay, codec, processor input/output, lookup key, materialized state, or emitted payload must have exactly the declared type unless the user wrote an explicit operation that performs conversion. Type mismatch is a hard error. Do not add implicit casts, coercions, parsing, stringification, numeric widening/narrowing, datetime/string interchange, wire/internal compatibility shortcuts, or fallback code paths that make mismatched types work.
- Do not preserve legacy behavior unless the user explicitly requests a compatibility path for the current change. If an old syntax, implicit fallback, compatibility shim, migration shortcut, synthetic legacy identifier, or parallel legacy code path conflicts with the requested model, remove it rather than routing around it.
- When refactoring, prefer a narrower and more compact architecture. Collapse duplicated logic into the owning parser/model/runtime path, delete obsolete layers, and make invariants explicit in the smallest surface that can own them. Avoid adding wrappers, adapters, helper layers, or extra lines unless they clearly reduce total complexity or isolate a real boundary.
- Do not encode internal behavior with magic/reserved user-facing identifiers (for example synthetic relay names, field names, model names, or namespaces such as `__nervix_*`). If runtime/compiler/parser internals need a special path, model it with explicit typed variants or internal-only structures so user-defined identifiers cannot collide with it.
- Do not write free functions that take one domain/model/AST/runtime object as their primary argument when the behavior semantically belongs to that object. Implement that behavior as an inherent method on the owning type using `&self`, `&mut self`, or `self`. This is required even for private/local helpers. Boolean predicates over enum variants must be methods on the enum, for example `statement.requires_local_handling()` instead of `client_statement_requires_local_handling(&statement)`.
- Keep Rust modules organized around coherent ownership boundaries, not around broad technical categories. Do not create grab-bag modules that import many unrelated libraries or collect unrelated behaviors just because they are all "clients", "helpers", "utils", or "common". Shared logic is acceptable only when it describes one focused abstraction and lives in its own focused module/type, for example TLS material loading, URL endpoint parsing, SQL value compilation, or a trait shared by a specific family of implementations. Concrete setup, config interpretation, state transitions, I/O operations, encoding/decoding, and lifecycle behavior belong on the relevant owning struct as `self`/`&self` methods whenever that struct naturally owns the behavior.
- Use free functions only when the behavior genuinely has no single owning receiver, spans multiple independent types symmetrically, is parser-combinator plumbing, or is a tiny lexical/primitive helper with no domain owner. Do not use free functions as private convenience helpers for domain/model/AST/runtime behavior that has a natural receiver.
- Always prefer deriving enum string conversions and metadata with `strum` (`AsRefStr`, `EnumString`, `EnumProperty`, etc.) over manual match-based helpers when the mapping is declarative. Do not add new manual enum-to-string match helpers when `strum` can express the same mapping.
- When code relies on sorted vectors/arrays as an invariant, prefer the `sorted-vec` crate (`SortedVec` / `SortedSet`) over a plain `Vec` plus manual `sort`/`dedup`.
- Prefer synchronous locks from `parking_lot` over `std::sync` lock types.
- For shared concurrent maps, prefer `DashMap` over `Arc<Mutex<HashMap<...>>>`.
- Prefer `triomphe::Arc` over `std::sync::Arc` when weak references are not needed and the shared pointer does not cross an API boundary that requires `std::sync::Arc`.
- Do not use `matches!` inside `if` conditions when the same logic can be expressed with `if let` or `if let` chains. Prefer `if let` forms for readability. Fall back to `matches!` only when the condition cannot be expressed cleanly without it, or when it is used outside an `if` condition.
- Time values must use typed datetime/timestamp representations internally. Do not model internal runtime, application, persistence, or domain-clock state as raw Unix integer counters when a semantic timestamp type can be used instead.
- Nanosecond Unix integers are a boundary format only. Use them explicitly for serialization/deserialization, cross-node/public protocol payloads, rkyv/CBOR/JSON encoding, and Arrow timestamp arrays, but convert to typed timestamps immediately after decoding and only back to integers at the boundary.
- Parse URLs and URL-like service addresses with the `url` crate. Do not manually split schemes, authorities, hosts, ports, paths, or query strings except for non-URL formats that have no compatible parser.
- Domain semantics are strict: there is no implicit or auto-created runtime domain in product code. Any command that targets a domain must fail unless that domain was explicitly created first. Do not preserve backward compatibility by treating missing domains as legacy defaults.
- All entities are domain-owned. New models, commands, persistence, lookup paths, and APIs must require an explicit existing domain and must not behave as globally scoped entities. If an entity appears shared today due to legacy implementation details, treat that as technical debt rather than desired behavior.
- Do not add `#[allow(...)]` annotations unless they were explicitly approved for that change.
- Do not use `std::mem::forget`, `Closure::forget`, leaked boxes, or equivalent lifetime leaks to keep browser callbacks alive in the web console. Use a crate-managed abstraction or an explicit owner/cleanup path instead.
- Web-console browser behavior must be tested through the standard cucumber suite with Playwright Rust steps and feature files under `tests/features/web-console`; do not exercise web-console internals or its websocket endpoint directly.
- In async code, any loop whose body performs async work should call `tokio::task::consume_budget().await` once per iteration near the top of the loop body.
- Runtime and interconnect hot paths must not log concrete message, record, or batch processing at `info`. Keep `info` for important lifecycle, topology, startup, shutdown, administrative, and unusual state-transition messages. Per-message, per-record, per-batch, payload-bearing, duplicate-drop, branch-churn, and similar high-volume runtime/interconnect details belong at `debug` or `trace`.
- Keep parser tests close to each feature module.
- Keep keyword/type literal ownership in the lexer enum; avoid grammar-local keyword strings.
- Do not run `cargo fmt` or `cargo +nightly fmt` directly. Use `just validate` for formatting and validation.

## Testing Requirements
When fixing a bug:
- Start by adding or identifying a test or cucumber scenario that reproduces the bug.
- Run that focused test before changing product code and confirm it fails for the expected reason.
- Only implement the fix after the reproducing test is red.
- After the fix, rerun the same test and confirm it is green, then run the appropriate broader validation.

When adding/changing grammar:
- Add positive parse tests.
- Add negative parse tests.
- Add at least one completion-context test that protects against cross-branch leakage.
- Add/adjust completion tests for composed keywords when relevant.
- Run:
  - `just validate`

When adding new functionality beyond local parser-only changes:
- First add or update the cucumber scenario that proves the behavior through the public interface.
- Run the targeted scenario and confirm it fails for the expected reason before changing product code.
- Add or update cucumber coverage under `tests/features/` and `tests/scenarios.rs`.
- Treat cucumber scenarios as the required high-level integration layer for new behavior, especially cluster, runtime, persistence, and API flows.
- Prefer asserting observable system behavior through real commands and multi-node orchestration instead of only unit-level internals.
- Unit tests may be added as support, but they do not replace cucumber coverage.
- Unit tests are not sufficient for runtime, persistence, API, CLI command, scheduling, cluster, metrics, or domain behavior.
- If the change is observable through an NSPL command, HTTP/API call, cluster state, runtime output, or persisted state, it must have cucumber coverage.
- Parser-only changes may use parser unit tests, but adding a new executable statement is not parser-only if application/runtime handling is added.
- Do not substitute unit tests for cucumber coverage unless the user explicitly approves that exception in the current task.
- In the final response, state which cucumber scenario was added or updated. If none was added, state the explicit user-approved reason.
- Avoid blind sleeps in tests.
- Wait for explicit conditions with bounded timeouts instead.
- Polling with a short interval is acceptable only when it is part of a condition-based wait loop with a clear timeout and failure message.

When writing cucumber scenarios:
- Do not mix up `Given`, `When`, and `Then`.
- `Given` sets up initial context and preconditions.
- `When` performs the action or event under test.
- `Then` asserts the observable outcome.
- Keep step keywords semantically aligned with their purpose so scenarios stay readable and step matching stays unambiguous.
- Tests responsible for runtime behavior must be written as `Scenario Outline` and parameterized by cluster size.
- Runtime cucumber scenarios should run against at least `1`-node and `3`-node examples unless the behavior is explicitly topology-specific.
- Topology-specific behavior such as scheduling, leader/follower restrictions, and inter-node mechanics should use dedicated scenarios or lower-level tests instead of being baked into generic runtime scenarios.

After each development cycle:
- Run `just validate` and ensure it passes.
- If new tests were added, run those tests explicitly as part of the cycle.
- Use `just test` for the full test suite because it sets required test environment variables such as `ORT_DYLIB_PATH`.
- For targeted cucumber/scenario runs, use `just test-scenarios --input <feature> ...` so the same test environment setup is applied. If a more specific targeted test command is needed, add a `justfile` task for it instead of running `cargo test --test scenarios` directly.
- Check the roadmap in `docs/src/roadmap.md`, and if the implemented work completed a listed roadmap item, remove that item from the roadmap as part of the same change.

## Error Reporting Requirements
- Parse errors should surface exact expected/found tokens when possible.
- Keep Ariadne-friendly source spans.
- Avoid generic messages like "expected something else" when labels can be provided.
