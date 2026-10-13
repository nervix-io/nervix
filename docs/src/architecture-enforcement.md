# Architecture Rules And The Debt Ratchet

Nervix separates facts about Rust source from facts about the APIs a compilation resolves. The
source checks inspect inactive code too. The compiler checks identify definitions, normalized
types and source-owned contracts in each declared legal configuration. A method name alone is
never evidence of its owner or effect.

`just ratchet` runs both owners. `just validate` and `just validate-ci` require the same gate;
formatting is applied by the former and checked by the latter. The compiler emits ordinary
`nervix::` diagnostics. The three structural counts compare with `debt-baseline.json`; a count
may decrease and cannot increase. `just ratchet --update` records a decrease only after the
complete gate passes. No numeric allowance controls a semantic rule.

## Rule Contracts

The fourteen rule families below have one implementation each. The family name remains useful
for describing its purpose; a compiler family can emit more than one diagnostic where a missing
contract and a violated contract require different corrections.

| Family | Purpose and owning fact | Allowed example | Rejected example | Enforcement and qualification |
| --- | --- | --- | --- | --- |
| `data_plane_lock_acquisitions` | Resolve synchronization APIs and the execution contract of their callers. [Concurrency](data-plane-concurrency.md#source-contracts) owns contexts, effects and bounds. | A task-local `HashMap::entry`; a registry acquisition with a lifecycle contract. | A recurring callback reaching a DashMap shard or a lifecycle-only helper. | Compiler `sync_acquisition`, `lifecycle_call`, `unknown_effect`; resolved receiver, helper, callback, trait, macro and expectation fixtures. |
| `bare_error_signatures` | A Nervix failure contract carries its complete contextual report. [Errors](errors-and-diagnostics.md) owns outcome, library and foreign-interface classifications. | `Result<T, Report<DomainError>>`; an exact reason-bearing outcome contract. | An owned failure in an alias, inferred future or stream without its report. | Compiler `bare_error_signature`; associated-output, alias, inference, metadata and report-carrier fixtures. |
| `discarded_results` | Preserve a failure or classify recovery at its consuming operation. | A propagated failure or the recovery owner's explicit `Discarded` operation. | A failed outcome discarded through a binding, projection or `drop`. | Compiler `discarded_outcome`; complete and partial-discard, borrowed, tuple and collection fixtures. |
| `bare_unwrap_and_expect` | A canonical Option/Result panic needs a construction or checked guarantee, expressed through meticulous. | `verified` after checking the condition; an unrelated object's method named `expect`. | Canonical `unwrap` or `expect`, including aliases and UFCS. | Compiler `bare_panic`; canonical/custom methods, dereference and call-form fixtures. |
| `model_matches_in_data_plane` | Configuration Models stay inside their owning boundary; data plane and connector engines consume plans. | A vocabulary Model or a typed connector plan. | A forbidden Model in a body, normalized interface or nested aggregate. | Compiler `model_in_data_plane`; source `layer`, `value` and `input` contracts, cross-crate and projection fixtures. |
| `parser_imports_outside_edges` | Language dependencies belong to the language and its declared edges. | The session adapter consuming public parsing; a runtime consuming Models. | A language reference outside its authorized layer, including aliases. | Compiler `parser_dependency` plus disjoint inactive import/manifest provenance; qualified names, reexports and dependency fixtures. |
| `data_plane_cluster_awaits` | Recurring processing retains observations instead of awaiting cluster control access. | A lifecycle observation; reading an installed immutable publication. | Recurring work awaiting a declared cluster-access capability. | Compiler `cluster_await`; direct, bound-future, helper and trait fixtures. |
| `string_node_ids` | Declared node identities use the shared typed vocabulary. [Typed States](typed-states.md#source-owned-identity-and-installation-contracts) owns the identity contract. | `ClusterNodeId`; an ordinary string unrelated to node identity. | A declared identity interface or field represented by a string. | Compiler `node_identity`; alias, interface, aggregate, field and metadata fixtures. |
| `write_once_rwlock_fields` | Installation-only state is an installed value rather than replaceable locked absence. | An installed value; a genuinely mutable field with its lifetime reason. | An installed field containing an RwLock, or an unclassified candidate requiring review. | Compiler `installed_state` and `state_contract`; normalized representation, lifetime, review and exception fixtures. |
| `clamped_arithmetic` | The resolved primitive integer or Duration API chooses saturation or wrapping instead of checked overflow. | Checked arithmetic; pointer wrapping; an Instant elapsed-time query; a specifically justified retry ceiling or modular mixer. | An integer/Duration saturating or wrapping operation without its actual arithmetic meaning. | Compiler `clamped_arithmetic`; integral and Duration owners, aliases, UFCS, callbacks, authored macros and narrow-expectation fixtures. |
| `combinator_control_flow` | Canonical Option/Result adapters must not hide a branch or a sequence. | One transformation, an iterator pipeline, or an unrelated custom method. | `map_or`/`map_or_else`, successive governed adapters, or a governed adapter with a closure containing statements. | Compiler `combinator_control_flow`; canonical/custom receiver pairs, UFCS, closure structure, diagnostic cardinality and source spans. |
| `as_casts` | Count actual Rust cast expressions, not words in imports, attributes or qualified paths. | `use Value as Alias`; `r#as`; `<Type as Trait>::item`; ordinary literals. | An expression using `as`, including a const expression nested in a qualified type. | Stable source parser; inactive cfg, raw identifiers, nested syntax, macro fragments and bounded source properties. Clippy independently denies conversions in its broader build scope. |
| `testing_feature_struct_fields` | A product struct has one field shape rather than a shape selected by `testing`. | A test-only item, an API gate or a conditional documentation attribute. | A named or tuple struct field whose presence depends on `testing`, including a conditional `cfg` attribute. | Stable source parser; field ownership, tuple, inactive, compound-predicate and `cfg_attr` controls. Enum fields are outside this specific struct rule. |
| `files_over_3000_lines` | Keep product and in-crate test files within a source-size bound. | At most 3,000 lines, counting a final line without a newline. | An eligible file with more than 3,000 lines. | Source discovery and physical line count; exact boundary and newly authored source tests. |

The Option/Result adapter catalog covers `and_then`, `filter`, `map`, `map_err`, `map_or`,
`map_or_else`, `ok_or`, `ok_or_else`, `or_else`, `unwrap_or`, `unwrap_or_default` and
`unwrap_or_else`. It resolves their inherent owner. A custom `map_or` is not a canonical branch,
and an Iterator's `map` is not an Option/Result adapter. A chain is diagnosed once at its outer
governed call; a later binding separates steps. A closure's statement structure is a source
fact, not a claim that the compiler understands the business meaning of its transformation.

Arithmetic recognition uses the defining crate and inherent primitive integer/Duration owner.
Type aliases and UFCS preserve that identity. A function reference retains the arithmetic
diagnostic before a callback or function-pointer type can erase its definition. A wrapper's
authored body is checked when its configuration compiles; matching words on the wrapper do not
grant it permission or make it a canonical API. Instant's saturating elapsed-time query is a
query with a defined zero result, and pointer wrapping is pointer arithmetic. Neither is the
integer/Duration overflow choice this family enforces.

## Scope And Configuration

Compiler coverage is precisely `tools/nervix-lint/configurations.toml`: the ordinary workspace's
libraries and binaries, browser root, testing library and separate legal Shuttle, Loom, Turmoil
and Deloxide configurations. The diagnostic variants have their own library and binary
invocations. They are never combined with `--all-features`. Every expected target and report
must be present, current and complete. Authored macro expansion and captured caller tokens retain
provenance; generated build-directory code and external definitions are explicit exclusions.
The compiler does not infer the APIs of code that no declared configuration compiles.

Source discovery includes tracked Rust and newly authored Rust not ignored by Git. The three
counts retain their product-debt scope: directory components named `tests`, `benches` or
`examples` are excluded. In-crate `tests.rs`, `*_tests.rs` and `test_fixtures.rs` count toward
file size but are excluded from cast/field debt. Test-only items are excluded from those two
syntax counts. Other inactive configuration branches remain visible. These exclusions belong
to these counts; they do not exempt a file from the primitive, manifest, clock or language
provenance validators, which own their broader authored-source contracts.

The stable source parser reads original Rust text and walks cast expressions and attributes
attached to actual struct fields, retaining token locations even beside Unicode comments.
It also parses Rust fragments in authored macro groups after replacing metavariable markers with
their identifiers, preserving token locations. Repetition bodies are visited independently.
Opaque macro DSL words and invalid fragments are not reinterpreted as Rust expressions. An
invalid top-level Rust file fails with its owning path and location. This does not expand a macro
or prove which tokens an external procedural macro would generate.

The parser requires no rustc internals or analysis driver. `just typed-ratchet-source-build`
builds it on the stable toolchain. Native compiler coverage builds it on that producer's pinned
toolchain so LLVM profiles and exporters match. The tooling workspace stays outside product
dependencies, including browser and ordinary release graphs.

## Exceptions And Ownership Review

A semantic exception is a reason-bearing `expect` at one operation or binding, gated through
`cfg_attr(nervix_lint, ...)` or `nervix_primitives::expect_lint!`. A module or function allowance,
blanket warning suppression, missing reason, unused expectation or expectation covering another
operation fails. Arithmetic reasons state the actual ceiling, capacity or modular computation;
“cannot overflow” does not justify wrapping. A retained defect names its repair task.

Source counts have no semantic exception channel. A current structural occurrence remains counted
until its owner changes it; a compiler expectation cannot hide its inactive source. Likewise,
source provenance cannot be waived by declaring a lifecycle or error contract.

Review still owns the truth of reasons, lifetime classifications, architectural roles and bounds.
The compiler checks their placement, shape and supported effects, not arbitrary whole-program
ownership. Finite API recognition does not establish business semantics for an unrelated custom
method, an erased indirect transformation or an opaque dependency. A paired rejection fixture
proves the specified route; it does not prove every possible alternative absent.

## Authoring And Qualification

Add a semantic rule in Rust beside its owner. Qualify an allowed/rejected pair that differs in the
semantic fact being enforced, not just a name. Include aliases, call forms, macro origin, caller
or metadata effects when applicable, exact diagnostics and source locations, and invalid,
unfulfilled and widened expectations. Change the receiver or definition to an unrelated object
as a false-positive control; restore the canonical API through an alias or UFCS as a
false-negative control. Source rules also qualify inactive branches and lexical distractions.

Use `just test-ratchet-units`, `just test-typed-ratchet-reports`,
`just test-typed-ratchet-compiler`, `just test-typed-ratchet-contracts` and
`just test-typed-ratchet` for the source, report, ordinary and separate mode qualification.
`just typed-ratchet --inventory --show` displays a calibration without claiming the diagnostic
gate passed. `just ratchet` requires the gate. Cache qualification remains
`just qualify-typed-ratchet-cache`; changed source, annotations, rules, dependencies and
configuration invalidate evidence.

The registered `typed-source-syntax` Bolero property uses bounded current Rust syntax, complete
source input/site equality and deliberate cast/field counts. Ordinary corpus/randomized checks
are required; `just fuzz typed-source-syntax` is actual sanitizer-backed fuzzing, and CI runs that
mode only for a PR labeled `fuzz`. A sanitizer skip supplies no fuzz or coverage evidence.
Compiler mode qualification checks analysis, not execution of the runtime's concurrency models.
The sanitizer compiler adapter preserves the source parser's span contract as described in
[Property Testing And Fuzzing](property-testing-and-fuzzing.md).
The [concurrency](data-plane-concurrency.md), [error](errors-and-diagnostics.md) and
[state](typed-states.md) chapters retain those operational owners.
