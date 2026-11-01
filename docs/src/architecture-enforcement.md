# Architecture Rules And The Debt Ratchet

Nervix separates facts about Rust source from facts about the APIs a compilation resolves. The
source checks inspect inactive code too. The compiler checks identify definitions, normalized
types and source-owned contracts in each declared legal configuration. A method name alone is
never evidence of its owner or effect.

`just ratchet` runs both owners. `just validate` and `just validate-ci` require the same gate;
formatting is applied by the former and checked by the latter. The compiler emits ordinary
`nervix::` diagnostics. The five structural counts compare with `debt-baseline.json`; a count
may decrease and cannot increase. `just ratchet --update` records a decrease only after the
complete gate passes. No numeric allowance controls a semantic rule.

## Components

| Part | Location | Responsibility |
| --- | --- | --- |
| Compiler driver | `tools/nervix-lint/driver` | Registers the `nervix` tool, validates source contracts and expectations, resolves definitions, normalized receivers, aliases, UFCS, trait selections and macro origins, emits the `nervix::` diagnostics and writes one side report per compilation, including its review candidates for the audit. |
| Rule catalog and reports | `tools/nervix-lint/report` | The finite API recognition the driver applies (`report/src/rules.rs`), side-report validation and summaries, and the stable source parser behind the structural counts. |
| Fixtures | `tools/nervix-lint/fixtures`, `tools/nervix-lint/fixture-macros` | The allowed and rejected compiler cases, authored-macro cases and paired doctests that qualify each rule. |
| Declared matrix | `tools/nervix-lint/configurations.toml` | The configurations the gate analyzes and the memory admission of their compilations. |
| Runner | `scripts/typed_ratchet.py` (`just typed-ratchet`) | Runs Cargo with the driver once per configuration, computes the report identity, reuses complete evidence and refuses missing, partial, stale or foreign reports. |
| Workspace wrapper | `scripts/typed_lint_wrapper.py`, `scripts/analysis_admission.py` | Runs the driver as a child beneath the compiler wrapper the host configures, records every compilation and admits it against host memory. |
| Gate | `scripts/ratchet.py` (`just ratchet`) | Requires the complete diagnostic gate and compares the five structural counts with `debt-baseline.json`. |
| Complexity analyzer | `scripts/cargo_crap.py` | Pins and runs the analyzer behind `functions_over_complexity_30`. |
| Audit | `scripts/lint_audit.py` (`just lint-audit`) | Attributes review candidates to the history that produced them. |

Product source owns the policy: the [source contracts](#source-contracts) and the reason-bearing
expectations at the operations they cover. The tooling owns recognition and evidence, and nothing it
generates is read back as policy. `just validate-typed-errors` is a separate source rule without a
count: it rejects `Result<_, String>` in product code outright, and
[Errors And Diagnostics](errors-and-diagnostics.md) owns it.

## Rule Contracts

The sixteen rule families below have one implementation each. The family name remains useful
for describing its purpose; a compiler family can emit more than one diagnostic where a missing
contract and a violated contract require different corrections.

| Family | Purpose and owning fact | Allowed example | Rejected example | Enforcement and qualification |
| --- | --- | --- | --- | --- |
| `hidden_tunables` | Numeric policy belongs to a class-typed setting or an explicitly fixed constant. [Node Settings](node-settings.md) owns the classification. | One definition default; a constant with its fixed-format reason; a capacity derived from typed input. | A numeric constant or literal policy call outside that boundary. | Stable source parser; governed-type, alias, exclusion, tracked/new-source, inactive and macro controls; fixed-contract compiler fixtures and the registered bounded `typed-hidden-tunables` property. |
| `data_plane_lock_acquisitions` | Resolve synchronization APIs and the execution contract of their callers. [Concurrency](data-plane-concurrency.md#source-contracts) owns contexts, effects and bounds. | A task-local `HashMap::entry`; a registry acquisition with a lifecycle contract. | A recurring callback reaching a DashMap shard or a lifecycle-only helper. | Compiler `sync_acquisition`, `lifecycle_call`, `unknown_effect`; resolved receiver, helper, callback, trait, foreign item, macro and expectation fixtures. |
| `bare_error_signatures` | A Nervix failure contract carries its complete contextual report. [Errors](errors-and-diagnostics.md) owns outcome, library and foreign-interface classifications. | `Result<T, Report<DomainError>>`; an exact reason-bearing outcome contract. | An owned failure in an alias, inferred future or stream without its report. | Compiler `bare_error_signature`; associated-output, alias, inference, metadata and report-carrier fixtures. |
| `discarded_results` | Preserve a failure or classify recovery at its consuming operation; a replacement retains an informative cause. | A propagated failure, contextualized cause or the recovery owner's explicit `Discarded` operation. | A failed outcome discarded through a binding, projection or `drop`, or replaced with a fresh error that loses its cause. | Compiler `discarded_outcome` and `replaced_error`; complete and partial-discard, replacement, outcome, alias, macro and metadata fixtures. |
| `bare_unwrap_and_expect` | A canonical Option/Result panic needs a construction or checked guarantee, expressed through meticulous. | `verified` after checking the condition; an unrelated object's method named `expect`. | Canonical `unwrap` or `expect`, including aliases and UFCS. | Compiler `bare_panic`; canonical/custom methods, dereference and call-form fixtures. |
| `model_matches_in_data_plane` | Configuration Models stay inside their owning boundary; data plane and connector engines consume plans. | A vocabulary Model or a typed connector plan. | A forbidden Model in a body, normalized interface or nested aggregate. | Compiler `model_in_data_plane`; source `layer`, `value` and `input` contracts, cross-crate and projection fixtures. |
| `parser_imports_outside_edges` | Language dependencies belong to the language and its declared edges. | The session adapter consuming public parsing; a runtime consuming Models. | A language reference outside its authorized layer, including aliases. | Compiler `parser_dependency` plus disjoint inactive import/manifest provenance; qualified names, reexports, foreign items and dependency fixtures. |
| `data_plane_cluster_awaits` | Recurring processing retains observations instead of awaiting cluster control access. | A lifecycle observation; reading an installed immutable publication. | Recurring work awaiting a declared cluster-access capability. | Compiler `cluster_await`; direct, bound-future, helper and trait fixtures. |
| `string_node_ids` | Declared node identities use the shared typed vocabulary. [Typed States](typed-states.md#source-owned-identity-and-installation-contracts) owns the identity contract. | `ClusterNodeId`; an ordinary string unrelated to node identity. | A declared identity interface or field represented by a string. | Compiler `node_identity`; alias, interface, aggregate, field and metadata fixtures. |
| `write_once_rwlock_fields` | Installation-only state is an installed value rather than replaceable locked absence. | An installed value; a genuinely mutable field with its lifetime reason. | An installed field containing an RwLock, or an unclassified candidate requiring review. | Compiler `installed_state` and `state_contract`; normalized representation, lifetime, review and exception fixtures. |
| `clamped_arithmetic` | The resolved primitive integer or Duration API chooses saturation or wrapping instead of checked overflow. | Checked arithmetic; pointer wrapping; an Instant elapsed-time query; a specifically justified retry ceiling or modular mixer. | An integer/Duration saturating or wrapping operation without its actual arithmetic meaning. | Compiler `clamped_arithmetic`; integral and Duration owners, aliases, UFCS, callbacks, authored macros and narrow-expectation fixtures. |
| `combinator_control_flow` | Canonical Option/Result adapters must not hide a branch or a sequence. | One transformation, an iterator pipeline, or an unrelated custom method. | `map_or`/`map_or_else`, successive governed adapters, or a governed adapter with a closure containing statements. | Compiler `combinator_control_flow`; canonical/custom receiver pairs, UFCS, closure structure, diagnostic cardinality and source spans. |
| `as_casts` | Count actual Rust cast expressions, not words in imports, attributes or qualified paths. | `use Value as Alias`; `r#as`; `<Type as Trait>::item`; ordinary literals. | An expression using `as`, including a const expression nested in a qualified type. | Stable source parser; inactive cfg, raw identifiers, nested syntax, macro fragments and bounded source properties. Clippy independently denies conversions in its broader build scope. |
| `testing_feature_struct_fields` | A product struct has one field shape rather than a shape selected by `testing`. | A test-only item, an API gate or a conditional documentation attribute. | A named or tuple struct field whose presence depends on `testing`, including a conditional `cfg` attribute. | Stable source parser; field ownership, tuple, inactive, compound-predicate and `cfg_attr` controls. Enum fields are outside this specific struct rule. |
| `files_over_3000_lines` | Keep product and in-crate test files within a source-size bound. | At most 3,000 lines, counting a final line without a newline. | An eligible file with more than 3,000 lines. | Source discovery and physical line count; exact boundary and newly authored source tests. |
| `functions_over_complexity_30` | Keep each function's decisions within a cyclomatic complexity of 30. At full coverage a function's CRAP score equals its complexity, so a function above 30 exceeds a CRAP threshold of 30 at any coverage. [Function complexity](#function-complexity) owns the analyzer and its limits. | A function the pinned analyzer scores at most 30; an existing function above 30 that grows, which keeps the count. | A function newly scored above 30, including one in an inactive configuration or an in-crate test file. | Pinned cargo-crap analysis over source discovery; exact 30/31 boundary, inactive configuration, macro, trait and nesting limits, eligibility, and missing, failing, other-version, skipped-source, empty, configured, undiscovered and unreadable-file fixtures. |

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

## Source Contracts

What the compiler cannot infer is stated once, at its owner, in one of nine annotations. Each is
gated as `#[cfg_attr(nervix_lint, nervix::...)]`: only the analysis driver registers the `nervix`
tool and sets that cfg, so stable product and browser builds never see an annotation. Every
annotation carries a reason. An unknown `nervix::` annotation, a duplicated or misplaced one, or a
kind its annotation does not accept fails as `nervix::invalid_contract`.

| Annotation | Placed on | States | Owner |
| --- | --- | --- | --- |
| `context(recurring\|lifecycle\|observer\|outside\|bounded)` | function, closure, trait, type, impl, module, crate, or a binding whose initializer owns one anonymous body | The execution context the synchronization and cluster-await rules judge; `bounded` also names its key and bound | [Source contracts](data-plane-concurrency.md#source-contracts) |
| `dispatch` | callable | The external driver or callback boundary that recurring execution enters through generic, dynamic-trait or function-pointer dispatch | [Source contracts](data-plane-concurrency.md#source-contracts) |
| `layer(primitives\|vocabulary\|language\|engine\|decisions\|data_plane\|control_plane\|edge\|harness)` | crate, module, type, trait, impl or callable | The layer that owns the code | [Resolved architecture boundaries](data-plane-concurrency.md#resolved-architecture-boundaries) |
| `input(plans)` | crate, module, type, trait or callable | An engine that receives validated plans, which its layer alone does not grant | [Resolved architecture boundaries](data-plane-concurrency.md#resolved-architecture-boundaries) |
| `value(model\|name\|cluster_access)` | type or trait | The configuration Model union, the shared name vocabulary and the cluster observation capability | [Resolved architecture boundaries](data-plane-concurrency.md#resolved-architecture-boundaries) |
| `identity(node)` | vocabulary type, field, parameter, type alias or callable return | A node identity | [Typed States](typed-states.md#source-owned-identity-and-installation-contracts) |
| `state(installed\|mutable)` | field | An installation-only field, or a genuinely replaceable one with its lifetime | [Typed States](typed-states.md#source-owned-identity-and-installation-contracts) |
| `error_boundary(outcome\|library)` | exact error type or function return contract | An ordinary outcome or a modeled library error contract | [Errors And Diagnostics](errors-and-diagnostics.md) |
| `constant(fixed)` | `const` item | A constant fixed by a wire format, RFC, persisted layout, ABI or invariant | [Hidden Tunables](#hidden-tunables) and [Node Settings](node-settings.md) |

A contract is a reviewed assertion, not a proof. The compiler checks its placement, its kind and the
effects it supports, and a contract moves with its source owner and crosses crate metadata. It does
not prove that a lifecycle function never runs per record, that a bounded protocol's wait ends, that
a fixed constant could not be a setting or that an outcome hides no failure; review owns those.

## Scope And Configuration

Compiler coverage is precisely `tools/nervix-lint/configurations.toml`: the ordinary workspace's
libraries and binaries, browser root, testing library and separate legal Shuttle, Loom, Turmoil
and Deloxide configurations. The diagnostic variants have their own library and binary
invocations. They are never combined with `--all-features`. Every expected target and report
must be present, current and complete. Authored macro expansion and captured caller tokens retain
provenance; generated build-directory code and external definitions are explicit exclusions.
The compiler does not infer the APIs of code that no declared configuration compiles.

Cargo runs the driver for each workspace target through `scripts/typed_lint_wrapper.py`, below the
compiler wrapper the host configures. That compiler wrapper reports a compiler ended by a signal with
the exit status of a compile error, so the driver runs as the workspace wrapper's child instead of
replacing it. It inherits the wrapper's descriptors, Cargo's jobserver among them, and ends when the
wrapper ends, so a compiler wrapper that stops a compilation still stops the driver. Every
compilation leaves a compilation record in a `compilations` directory beside the reports: its exit
status or signal, its start, duration and peak resident size, the out-of-memory kills the kernel
counted in its cgroup while it ran, and its [host memory admission](#host-memory-admission). When
Cargo fails, the gate names each compilation whose record holds a failure as terminated by a signal,
failed with compiler errors, or exited without a compiler error, which means the driver itself
failed, for example while publishing its report. A Cargo failure with no failed compilation happened
outside them, in Cargo, a build script or the compiler wrapper. Records of an earlier run are
discarded before Cargo starts. The kernel's out-of-memory killer ends a compilation with a signal and
no message, which the gate reports as such instead of as a missing report.

### Report Identity And Reuse

A run's identity is a digest of everything that can change a finding: the analysis mode, the
worktree root, the pinned compiler's `rustc -vV`, the driver and report validator binaries, the
workspace wrapper script, every tracked or new unignored `.rs`, `.toml`, `.lock`, `.fbs`, `.proto`,
`.json` and `.py` file, the Cargo configuration, and the compiler environment (`RUSTFLAGS` and
`CARGO_ENCODED_RUSTFLAGS`, `RUSTC`, `RUSTC_WRAPPER`, `KACHE_KEY_SALT` and Cargo's build, target and
profile variables). Every side report carries that identity and states whether it is complete. A
partial report, a report of another identity, and so of another worktree or source state, or a
declared target without its report fails the configuration; a configuration with zero sites still
needs every report.

Each configuration builds in a directory of its own and records its completion under
`target/typed-ratchet/evidence/<identity>/<configuration>/`. A later run reuses that completion only
while it is complete, its declared targets are unchanged, every file it recorded keeps its digest
and its reports still validate against its Cargo log; `--fresh` runs Cargo regardless. A source
change during the run fails it. The compiler wrapper's cache is keyed through `KACHE_KEY_SALT`
extended with the identity, so cached outputs never cross identities. When the wrapper restores
compiler outputs without their side reports, the runner removes that configuration's authored
artifacts and analyzes again in a fresh cache namespace. `RUSTC_WRAPPER` stays configured
throughout, and missing analysis never reads as zero findings.

## Pinned Toolchain

Product code builds with the stable toolchain `rust-toolchain.toml` declares. The analysis driver
links rustc's internal crates, so the isolated `tools/nervix-lint` workspace builds and runs on one
pinned nightly, `nightly-2026-09-17`, which its own `rust-toolchain.toml` declares with `rustc-dev`,
`rust-src`, `rustfmt` and `llvm-tools`. `just toolchains-install` installs both toolchains. The
driver refuses any other analysis compiler: it locates the pinned compiler through rustup and checks
the commit that `rustc -vV` reports. Bolero's sanitizer campaigns use the same nightly, which the
`[tool]` table of `tests/bolero-targets.toml` declares. Native coverage of the typed-ratchet
qualification uses that toolchain's own LLVM tools, so profiles and exporters match the compiler
that produced them.

Upgrading the pinned nightly is one change that moves every declaration together: the tooling
`rust-toolchain.toml`, the driver's compiler check, `scripts/typed_ratchet.py`, the justfile recipes
that name the toolchain, `scripts/native_coverage.py`, the Bolero inventory and the sanitizer
workflow's toolchain step, and the chapters and `AGENTS.md` that name it. It adapts the driver to
rustc's changed internal APIs while every compiler fixture keeps its allowed and rejected result; a
recognition the new compiler changes is a rule change and ships its fixtures. It qualifies with
`just test-typed-ratchet`, `just qualify-typed-ratchet-cache`, the complete `just ratchet` gate and
a `fuzz`-labeled run of the sanitizer campaigns. The new compiler identity re-establishes every
report, so no evidence from the previous compiler is reused.

Source discovery includes tracked Rust and newly authored Rust not ignored by Git. The five
counts retain their product-debt scope: directory components named `tests`, `benches` or
`examples` are excluded. In-crate `tests.rs`, `*_tests.rs` and `test_fixtures.rs` count toward
file size and function complexity but are excluded from cast/field debt. Test-only items are excluded from those two
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

## Hidden Tunables

`hidden_tunables` counts one site for each numeric `const` item typed as `Duration`, `usize`,
`u64`, `u32`, `u16`, `u8`, `i64`, `i32`, `f64`, `NonZero*` or `ByteUnit`. Associated constants
count too. A counted initializer contributes no further policy-call sites. Names are read from
the source type, following explicit import renames and type aliases within their lexical scope,
independent of their declaration order.
This syntax rule does not resolve custom types, glob imports or cross-file reexports; the
eventual compiler rule owns those semantic facts.

The count also includes a call with a numeric literal in its arguments when it constructs a
Duration through `from_*`, chooses `with_capacity*`, a `channel` capacity (excluding a watch
channel's initial value), `bounded` or `sync_channel`, constructs `Semaphore` or `ArrayQueue`,
or calls `add_permits`. Numeric arithmetic inside an argument counts; a capacity supplied
entirely by a named value does not add a call site. These names deliberately describe source
syntax, including same-spelled custom calls; they do not claim resolved API recognition.
Raw identifier spellings retain the same recognized type, policy call and definition owner.

The shared source discovery excludes test directories and in-crate test files. This count also
excludes `tools/`, the `arbitrary`, `benchmark`, `model-harness` and `test-environment` crates,
test-only items and test functions. Other inactive configurations remain visible. Authored
macro fragments use the same token substitution and parsing owner as the cast count.

An exact `SettingDefinition::default_value` implementation owns its one default. A `Default`
implementation of a nonempty struct whose fields are all `Setting<D>` owns its group defaults.
Their bodies are outside the count; unrelated `Default` bodies and other definition methods
remain counted. A fixed constant is excluded only with one valid
`#[cfg_attr(nervix_lint, nervix::constant(fixed, reason = "..."))]` annotation. The source check
refuses invalid contracts before measuring. The compiler independently checks exact placement,
kind, reason and duplication on compiled constants. A reason is reviewed justification, not a
proof that the literal is fixed. No filename or per-site allowance substitutes for it.

The baseline is introduced at the measured repository count and may only decrease. The area
owners classify the counted values while preserving their existing defaults. The settings
compiler-rule cutover removes this detector and its numeric baseline entry in the same change.
`scripts/tests/test_ratchet.py` pins eligibility and the gate; compiler fixtures qualify the
constant contract; the registered `typed-hidden-tunables` property checks complete source/site
representations and every generated location in ordinary and sanitizer execution.

## Host Memory Admission

An analysis compilation needs about the memory of rustc's own check of its target, and the server
library's dwarfs every other. It peaks at about 12.3 GiB in the ordinary configuration and between
about 11.7 and 15.5 GiB across the ten declared configurations that compile it, highest in the
testing configuration and the six Deloxide ones, while no other analysis compilation reaches 2 GiB.
Its 16 GiB reservation therefore leaves about half a GiB above the largest measured peak. Every
configuration builds in a directory of its own, and up to four configurations analyze at once, a
bound `--jobs` or `NERVIX_TYPED_RATCHET_JOBS` sets otherwise; the evidence keeps the declared order
whatever order they end in, and once one analysis fails no configuration starts another.
Configurations at once, and validations in several worktrees of one host, would hold several server
analyses at a time, and nothing else admits them against the host's memory: kache, the compiler
wrapper the host configures, runs a chain with a workspace wrapper directly, outside its own
memory-weighted scheduler. Five server analyses at once exceed a 62 GiB host, and the kernel's
out-of-memory killer ends one of them.

`tools/nervix-lint/configurations.toml` declares the admission: a threshold, a wait limit and a
reservation for every compilation, by crate and target kind, that peaks above the threshold. The
runner hands the policy to every compilation through the workspace wrapper. A compilation with a
reservation takes a place in one queue, `/tmp/nervix-analysis-admission-<uid>`, which every worktree
of the user shares on the host, before its driver starts, and holds it until the driver ends:

- It waits while an older waiter is ahead of it. A waiter that has not decided for ten polls, such
  as a stopped wrapper, keeps its place but no longer holds back the waiters behind it.
- It is admitted when its reservation fits in what each memory domain it runs in has left after the
  growth still to come of every compilation admitted in that domain. The domains are the host, with
  the kernel's estimate of its available memory, and each ancestor cgroup with a memory limit, with
  its limit less its usage plus its inactive file pages. An admitted compilation's growth still to
  come is its reservation less its driver's resident size.
- It is admitted when no other compilation is admitted, whether or not its reservation fits, so the
  queue always progresses: with too little memory the declared compilations run one at a time. A
  compilation admitted alone into too little memory can still be killed, and its record names the
  kill as before.
- It starts at the wait limit when neither happens first, so every wait is bounded. Admission never
  fails a compilation.

A place is an exclusive advisory lock on the compilation's lease file, so the kernel releases the
place of a wrapper that ends without releasing it, and the next decision deletes its lease. Every
decision holds the queue's lock. A waiter reports what it waits for when it starts waiting and every
minute after: its reservation, the waiters ahead of it, each admitted compilation by crate,
configuration, worktree and process, and the memory left. A queue that cannot be used admits
nothing: the compilation starts at once, and its report and record name the failure. A platform
that does not report its memory, such as one without `/proc`, runs the declared compilations one at
a time.

The admission lands in each compilation record: the reservation, the outcome (`fit`, `alone`,
`wait_limit`, `queue_locked` when the queue's lock stayed held through the wait limit, or
`unavailable`), the seconds waited, the memory left at the decision and the number of admitted
compilations. The runner requires a record for every expected target Cargo compiled in the run and
holds every record of the run to the declaration, including those of the workspace libraries a
binary configuration compiles for its binaries: a compilation that peaks above its reservation, or
above the threshold without one, fails the gate and names the declaration to change, so the
reservations keep covering what the compilations use. Such a failure also discards the configuration's analysis, because Cargo
would find the compilation fresh and the next run would measure nothing. The completion evidence
keeps the measurements, and the runner prints each configuration's largest compilation and every
admission that waited or did not fit.

Admission covers the declared analysis compilations only. Clippy's compilations go through kache's
own scheduler, and tests, scenario clusters and other work on the host are admitted by nothing; each
decision sees the memory they use, not the memory they will use. The queue belongs to one user on
one host, so separate users, containers and machines have queues of their own.

## Function Complexity

`functions_over_complexity_30` counts the functions whose cyclomatic complexity exceeds 30, as one
pinned analyzer measures it: cargo-crap at the version `scripts/cargo_crap.py` declares, the same
build the advisory CRAP report of `just check-coverage` runs. `just install-cargo-crap` builds that
version with `cargo install --locked` into `target/tools/cargo-crap-<version>/`, through the
compiler wrapper the host configures, and keeps a build that is already there. `just ratchet`,
`just test-ratchet-units`, `just check-coverage`, `just test-crap-report` and
`just coverage-crap-report-runner` depend on it, and the CI `checks`, `coverage` and `extra-tests`
jobs build it through kache. A binary that reports another version is refused with
the command that installs the pinned one; a different version in `PATH` is never used.

The complexity is classical McCabe: one, plus one for every `if`, `for`, `while`, `loop`, `match`
arm, `&&`, `||` and `?`. A function at exactly 30 is not counted and one at 31 is. Growing a
function that already exceeds 30 leaves the count unchanged; a new function above 30, or one that
crosses it, raises the count.

The analyzer walks the repository root with the eligibility above. It reads syntax and expands
nothing, so these are outside the count:

- a `#[test]` function and everything inside a `#[cfg(test)]` module;
- a function inside a macro, whether an authored `macro_rules!` body or an invoked item macro such
  as `cfg_if!`;
- the body of a trait's provided method;
- a function nested in another function's body, and the decisions of a closure, which belong to
  no reported function.

Fixtures pin each limit, so a new analyzer version that changes one fails a test. Inactive
configuration branches stay visible.

Missing analysis never reads as zero debt. The count fails, naming the cause, when the pinned
binary is missing or reports another version; when the analyzer exits with an error or writes a
warning, which it does only for a source it could not read or parse; when it measures no function;
when its report has another version or a `?` weight other than one; when a `.cargo-crap.toml`
in the repository root or above it could change the analysis; when it measures a file source
discovery does not govern, such as one in a nested repository; and when a discovered file is
hidden, a symbolic link or tracked but ignored by Git, which the analyzer's walk would skip.

`just ratchet --show functions_over_complexity_30` lists each function's file, line, name and
complexity. The baseline holds counts rather than sites, so when a count rises the gate lists every
site of that count and the function a change added is named among them. Decomposing a function is
the repair. Extracting a forwarding wrapper only to move the number is the workaround
`just lint-audit` attributes to a lowering of this count.

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
source input/site equality and deliberate cast/field counts. Its ordinary corpus/randomized test
runs in `just test-typed-ratchet`, and CI's randomized Bolero job runs it only for a PR labeled
`bolero-random`. `just fuzz typed-source-syntax` is actual sanitizer-backed fuzzing, the preferred
Bolero evidence, and CI runs that mode only for a PR labeled `fuzz`. A skip supplies no evidence of
its mode.
Compiler mode qualification checks analysis, not execution of the runtime's concurrency models.
The sanitizer compiler adapter preserves the source parser's span contract as described in
[Property Testing And Fuzzing](property-testing-and-fuzzing.md).
The [concurrency](data-plane-concurrency.md), [error](errors-and-diagnostics.md) and
[state](typed-states.md) chapters retain those operational owners.

CI runs the complete gate for every pull request: the `checks` job runs `just validate-ci 4`, which
runs the source and lint checks four recipes at a time and `just ratchet` after them, through kache.
The tooling's own qualification is selected by labels in the `extra-tests` job. A pull request
labeled `extra-compiler` runs `just coverage-native-extras test-typed-ratchet`, which runs
`just test-typed-ratchet` with its ordinary part under native coverage and its product doctests and
modeled fixture configurations after it, then `just qualify-typed-ratchet-cache` and
`just coverage-typed-ratchet-python`; one labeled `extra-doctests` runs `just test-capability-docs`.
A pull request that changes a compiler rule or fixture carries `extra-compiler`, and one that changes
an API with paired doctests carries `extra-doctests`.

## What The Evidence Establishes

Each form of evidence establishes its own claim, and none substitutes for another:

| Evidence | Establishes | Does not establish |
| --- | --- | --- |
| Compiler fixtures and diagnostics | That a rule recognizes the resolved facts its allowed and rejected cases differ in | That analyzed code is free of races or deadlocks, or that no unrecognized route exists |
| Paired API doctests | That one current API compiles and its rejected form does not, in the build's configuration | Execution of any model or runtime path |
| Bolero properties | Serial semantics: round trips, operation sequences and reference models over bounded inputs | Interleavings or memory ordering |
| Shuttle checks | Interleavings of a production owner's protocol under its declared exploration | Memory-ordering claims, which its sequentially consistent scheduler cannot establish, or network behavior |
| Loom models | A memory-ordering claim of a synchronous production owner, with a registered weakening that must fail | Operations inside opaque dependencies |
| Turmoil simulations | Network and distributed-recovery claims within the supported simulation | Real-process recovery |
| Deloxide lanes | Active deadlocks and potential order cycles among tracked blocking locks in the runs they make | Untracked synchronization, or interleavings the runs did not take |
| Cucumber scenarios | Public behavior through the product's interfaces | Interleavings the suite did not reach |
| External Chaos | Runtime fault and recovery against immutable product images | Paths its scenarios do not reach |

Compiling a configuration under a mode is analysis, not execution: a mode's evidence comes only from
its own runner, which reports the checks it discovered, selected, executed and completed. A change
confined to an exclusively owned collection, or to the compiler's analysis, makes no concurrency
claim and needs no invented model; its owner's unit tests, properties and compiler fixtures verify
it. A change that alters a protocol, a memory-ordering claim or a tracked lock brings that method's
evidence over its production owner, as [Data-Plane Concurrency](data-plane-concurrency.md) and
[Execution Primitives](execution-primitives.md) require.

## Auditing Lint Workarounds

A rule that misjudges code invites code shaped around the rule. The textual counters this gate
replaced matched method spellings inside named files, so lowering a count sometimes meant
respelling an operation or moving it rather than changing what the rule was about. `just
lint-audit` finds code shaped like such a workaround and attributes it to the history that
produced it.

The compiler pass records review candidates beside its acquisition findings in every complete
analysis, so the audit reads the evidence `just ratchet` uses and runs no second analysis.
`just lint-audit --inventory` audits with warning levels while the gate still has open
diagnostics, and `--configuration` selects declared configurations as an inventory. Each
candidate carries its owning body, that body's execution contract (declared, or recurring by
reachability from a recurring caller), its spans and macro origins, and the configurations that
compiled it.

| Kind | Resolved pattern | Controls that are not candidates |
| --- | --- | --- |
| `repeated_lookup` | Keyed operations (`get`, `get_mut`, `contains_key`, `insert`, `remove`, `entry`, indexing and their IndexMap forms) of a standard, hashbrown, IndexMap, DashMap or JSON map collection, on one receiver place with one key value, where a later operation can run after an earlier one. `guarded` marks a later operation in a branch the earlier result selected. | Exclusive branches, a receiver or key assigned between the operations, different keys or collections, and another type with map-like method names. |
| `key_allocation` | A borrowed lookup handed an owned copy of its key, or an `entry` handed a copy that a hit discards. | Borrowed keys, owned insertion keys, `Copy` values, shared-handle clones and formatted keys. |
| `borrowed_copy` | An owned copy whose only use is a borrow, such as `&value.clone()` or `text.to_string().len()`. | Copies that are moved, `Copy` values, shared-handle clones and text formatted from another value. |
| `duplicated_branch` | A call or assignment that every branch of one exhaustive `if` chain or `match` repeats, with the same resolved callees, generic arguments and type. | An `if` without `else`, exits, constructed values, and arms that dispatch one spelling over differently typed bindings. |
| `forwarding_wrapper` | A synchronous function whose whole body passes its parameters unchanged and in order to one local function, with its caller count and whether it carries a source contract, implements a trait method or calls into another file. | Transformed, reordered or partial arguments, library callees and constructors. |
| `discarded_error` | `Result::ok`, `unwrap_or`, `unwrap_or_default`, `is_ok`, `is_err`, an `unwrap_or_else`, `map_err` or `or_else` closure that ignores its failure, an `Err(_)` arm, `if let Ok` without `else` and `let Ok(..) = .. else`, when the failure carries information. | Unit, uninhabited and zero-sized failures, and failures handed on. |
| `disguised_clamp` | Checked primitive-integer or `Duration` arithmetic, or a checked integer conversion, whose absence falls back through `unwrap_or`, `unwrap_or_default` or `unwrap_or_else`. | Overflow decided by its own statement. |
| `relocated_acquisition` | A synchronization acquisition outside the files the textual lock counter read, written by a commit that lowered that count. | Acquisitions in the counter's files, and ones no lowering commit wrote. |

Attribution blames each candidate's lines in the working tree, reads the diff hunk of the commit
that completed them, and replays the history of `debt-baseline.json`. For every count the
candidate's kind could have worked around, it records whether that commit introduced or lowered the
count, whether its hunk, the rest of the candidate's file or the commit's other files removed the
counter's governed spelling (such as `.entry(` for the lock counter, counted only in the files that
counter read at the time), and whether the code predates the count. The report keeps the hunk's
removed and added lines around the candidate's line. When the completing commit only carried the
pattern forward, the earlier commits of the same lines are read for that evidence and the one found
is recorded as the origin. `--pull-requests` also reads, through `gh`, the commits a squash merge
folded away, finding its pull request by the number the subject names or, for a subject without one,
by the merged commit, and records a commit that removed the spelling while writing one of the
candidate's own lines. A read that ends with a server error or a dropped connection is attempted up
to four times with a doubling backoff; a failure that persists, or any other failure, ends the audit
without a report.

The suggestion is `suspected` when a count event coincides with a spelling removed from the
candidate's own hunk or file, or a folded commit rewrote the candidate's lines away from it. A
removal in another file explains the candidate only for the lock counter, which read named files, so
moving an operation out of them lowered it. The suggestion is `possible` when only an event or a
removal is present, or when the code was written into a file the lock counter read before the
compiler resolved that count, since such code could avoid the spelling without changing the count;
`unattributed` when the code was written while a count existed without any of these; `pre-existing`
when it predates every plausible count; and `uncommitted` for lines not yet committed. A suggestion
is evidence for review, never a classification: chronology alone proves nothing.

Candidate identities hash the kind, file, owner, primary source and pattern facts without byte
offsets, so an unrelated edit keeps them, and a repeat within one owner takes an ordinal.
`audit.json` and `audit.md` land in `target/lint-audit/<source identity>/`. They are generated
evidence: review dispositions, inventories and measurements belong on the owning task, and nothing
reads them back as policy.

Review classifies each candidate as confirmed lint-caused, suspected, unrelated, pre-existing or
intentional, recording the source revision, owning rule, before and after code, reason and
disposition. A repair keeps lazy and fallible initialization, side-effect counts, borrowed-key hit
behavior, exact equality, lifetimes, reentrancy, error and acknowledgement semantics and ordering.
An `entry` is not a repair where a hit would discard an allocated key, where a callback runs between
the lookup and the insertion, or where the miss path needs the whole collection. Fewer searches do
not make an `entry` faster either: on a small ordered map whose hits dominate, a lookup can cost
less than building the entry, so the measurement covers hit-heavy and miss-heavy key
distributions. A performance claim needs measured lookup, comparison and allocation counts and
same-host Criterion or `just benchmark-ab` evidence; a shorter function or a lower count is not a
speedup.

### Audit Results

The audit ran over the complete gate at revision `cd03e1bc`, with squashed pull-request history, and
review classified all 1,461 candidates: 26 confirmed lint-caused, 146 suspected, 1,026 unrelated, 202
pre-existing and 61 intentional. Chronology alone confirmed none; the review read each squashed
rewrite the audit associated with a candidate.

- Four candidates in window pane-sketch admission were repaired: one `entry` per pane replaced a
  lookup followed by an insertion. It is the only measured runtime improvement. New-pane admission
  became 16 to 18 percent faster for the distinct, quantile and top-k sketches when every row opens
  a new pane; at 16 and 1,024 rows per pane the difference stayed within run-to-run variance. A
  realistic end-to-end comparison showed no difference the host's variance could resolve, and none
  is attributed to the repair.
- Two confirmed Kafka partition folds keep their lookup-then-insert shape. Measured, `entry` was
  slower for the batches acknowledgements carry and faster only when every position named a new
  partition.
- The 411 sites that replaced or probed a failure and dropped its informative cause, and 33 rendered
  backup causes, were repaired, and replacing a failure without keeping it beneath became the
  compiler diagnostic `nervix::replaced_error`.
- No forwarding wrapper, moved scope or relocated acquisition was lint-caused damage, and no rule
  needed a semantic recalibration.

The per-candidate record, the measurements and the raw reports are on
[Typed Ratchet 09](https://app.clickup.com/t/86bc9etee) and
[Typed Ratchet 09A](https://app.clickup.com/t/86bchy23u).
