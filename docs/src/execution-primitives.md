# Execution Primitives

*Plexus, the Nervix graph definition language.*

This chapter owns the execution primitive boundary: its layer, governed API families, backend
selection, target capabilities, permissions, and build contracts. [Data-Plane Concurrency](./data-plane-concurrency.md)
owns the protocols callers build from those primitives, source-owned synchronization contracts,
production-owner Shuttle and Loom checks, and diagnostic deadlock detection. [Deterministic
Interconnect Simulation](./interconnect-simulation.md) owns the simulated network and its evidence.

## Ownership And Capabilities

Every execution-sensitive primitive Nervix uses comes from the `nervix-primitives` crate, which sits
below the vocabulary and selects each family for the build's execution mode: ordinary execution,
Shuttle, Loom, Turmoil or the Deloxide diagnostic mode. A build uses one mode across its whole
dependency graph. Every package that owns a `shuttle`, `loom`, `turmoil` or `deloxide` feature
forwards it to the primitive crate, and Cargo unifies
that crate's features, so a vocabulary type, an engine and the server compiled into one test binary
all use the same backend. Selection depends only on features, never on `cfg(test)`. Enabling two
modes fails to compile with a diagnostic that names both, including when two different dependencies
each enable one.

A closed channel returns its unaccepted value as an ordinary ownership refusal. The modeled
watch adapter declares that classification on its `SendError` type, matching the selected native
channel contract. Permit and queue refusals likewise carry admission or absence outcomes;
infrastructure and decoding failures retain their contextual reports.

The primitive crate depends on the standard library and the backend libraries it selects. It
names no Nervix vocabulary, graph, policy or scenario. `nervix-model-harness` owns the exploration
of caller models and their invariant identities, separately from backend selection. The server
remains the composition root; enabling a mode changes how its dependencies execute without moving
a decision or a protocol into the primitive layer.

Ordinary execution re-exports the governed library items directly, with no boundary wrapper,
allocation, dispatch or scheduling point. Explicit mechanisms such as detached-thread creation
perform their named operation through ordinary primitives. The atomic and shared-ownership
families are portable and build for the
browser console. Everything else is the explicit `native` capability, and asking for a capability or
a mode the target cannot provide is a compile error rather than another implementation: a mode or
the `native` capability requested for the browser's target fails with the boundary's own diagnostic
as its first error, because the libraries of every mode and of the native capability exist only for
native targets and none of them is compiled there.

## Backend Matrix

| Family | Path | Ordinary, Turmoil and Deloxide | Shuttle | Loom |
| --- | --- | --- | --- | --- |
| Atomic values, `Ordering`, `fence` | `sync::atomic` | The standard library's | Modeled: every operation is a scheduling point, and every ordering behaves as `SeqCst` | Modeled: explored under the C11 orderings Loom supports |
| Shared ownership: Nervix-owned references, and the standard strong and weak references a weak reference or an external API such as Arrow or a Tokio semaphore requires | `sync::Arc`, `sync::StdArc`, `sync::StdWeak` | `triomphe`'s `Arc`, and the standard library's `Arc` and `Weak` | Real, with no scheduling point: no reference count is modeled | Real, outside every model |
| Async locks, semaphores and one-time cells; the `mpsc`, `oneshot` and `broadcast` channels | `sync` | Tokio's | Modeled: Shuttle's Tokio | Real: Tokio's, outside every model |
| `Notify` and the `watch` channel | `sync` | Tokio's | Modeled: the boundary's own, with Tokio's semantics and a scheduling point before every registration, notification and deciding read | Real: Tokio's, outside every model |
| The waker registration of one waiting task | `sync::AtomicWaker` | The `futures` crate's | Opaque, with a scheduling point before and after each registration, wake and take | Real: the `futures` crate's, outside every model |
| Cancellation tokens and their guards | `sync` | Tokio Util's | Modeled: Shuttle's token, wrapped for Tokio Util's clone identity and owned operations | Real: Tokio Util's, outside every model |
| Blocking locks and their condition variable | `sync::blocking` | `parking_lot`'s; under Deloxide, the boundary's tracked adapters over Deloxide's locks, described in [Diagnostic deadlock detection](./data-plane-concurrency.md#diagnostic-deadlock-detection) | Modeled: Shuttle's `parking_lot` locks, and the boundary's condition variable over them | Real: `parking_lot`'s, outside every model |
| Barriers, `Once` and the synchronous channel | `sync::blocking` | The standard library's | Modeled: Shuttle's | Real: the standard library's, outside every model |
| `OnceLock` | `sync::blocking` | The standard library's | Opaque, with a scheduling point before and after each read and write; no initializing read | Real: the standard library's, outside every model |
| `LazyLock` | `sync::blocking` | The standard library's | Unavailable | Real: the standard library's, outside every model |
| Task spawning, joining, yielding, aborting, tracking, the cooperative budget, and polling a future outside it with `unconstrained` | `task` | Tokio's and Tokio Util's | Modeled: Shuttle's Tokio, with the boundary's abort-on-drop handle; Shuttle keeps no cooperative budget, so `unconstrained` only forwards the poll | Real: Tokio's and Tokio Util's, outside every model |
| Blocking the runtime worker thread that calls it: the declared owners outside the executor | `task::block_in_place` | Tokio's | Unavailable | Real: Tokio's, outside every model |
| Draining admitted CPU jobs on one worker | `task::spawn_cpu` | Tokio's blocking pool; under Turmoil, a simulated task yielding between callbacks | Modeled: Shuttle's `spawn_blocking` | Real: Tokio's blocking pool, outside every model |
| Running a job on the blocking pool itself: the executor's storage jobs, and the declared owners outside the executor | `task::spawn_blocking` | Tokio's blocking pool | Modeled: Shuttle's `spawn_blocking` | Real: Tokio's blocking pool, outside every model |
| Timers and the monotonic clock: sleeps, deadlines, timeouts, intervals and instants | `time` | Tokio's, following the clock of the runtime that polls them: the operating system's, a test's paused clock, or the simulated host's under Turmoil | Shuttle's: a sleep or an interval's tick is one scheduling point that takes no time, and a timeout expires only when a check triggers it; `Instant::now` reads the operating system's clock | Real: Tokio's, outside every model |
| Controls of a paused clock: `pause`, `advance` and `resume` | `time`, with the `test-util` capability | Tokio's | Shuttle's, which take no time | Real: Tokio's, outside every model |
| Sockets: TCP listeners and streams with their owned halves, UDP and local sockets | `net` | Tokio's over the operating system's network; under Turmoil, Turmoil's simulated sockets and `lookup_host`, which answers from the simulated DNS table, and no local socket or socket configured before it connects | Real: Tokio's, outside every model. A Shuttle execution has no Tokio reactor, so a socket created inside a check panics and fails it | Real: Tokio's, outside every model |
| The runtime, `#[nervix_primitives::test]`, `#[nervix_primitives::main]`, `select!` | `runtime`, crate root | Tokio's | Shuttle's runtime and `select!`; the attributes build Shuttle's runtime | Real: Tokio's, outside every model |
| Streams over channels | `stream` | Tokio Stream's | Shuttle's Tokio Stream | Real: Tokio Stream's, outside every model |
| Atomic reference publication and its cache | `publication` | ArcSwap's | Opaque, with a yield before and after each load, store, compare-and-swap and read-copy-update | Real: ArcSwap's, outside every model |
| Concurrent maps | `collections` | DashMap's | Modeled: Shuttle's DashMap | Real: DashMap's, outside every model |
| Lock-free queues | `collections` | Concurrent Queue's | Opaque, with a scheduling point before and after each operation | Real: Concurrent Queue's, outside every model |
| Threads: spawning, joining, building, scopes, parking, sleeping, yielding | `thread` | The operating system's | Modeled threads; sleeping and parking with a timeout are scheduling points that take no time | Modeled: Loom's threads to spawn, build, join, park and yield. Real: the operating system's sleeping, timed parking and scopes, outside every model |
| A thread nothing joins | `thread::spawn_detached` | A named operating-system thread | A detached Shuttle task, abandoned when the model's main thread returns | Real: a named operating-system thread, outside every model |
| Thread-local storage | `thread_local!` | The standard library's | Shuttle's, one per task | Modeled: Loom's, one per model thread |
| The host's parallelism | `thread::available_parallelism` | The host's | The host's | The host's |
| Deadlock findings, and the detector that reports them | `deadlock` | The findings in every mode; the detector only under Deloxide | The findings; no detector | The findings; no detector |
| Real primitives outside every model | `unmodeled` | Real | Real | Real |

Every exposed operation has one of three classifications in each mode. A **modeled** operation is
observed by the mode's engine, and a check may make claims within that engine's limits. An
**opaque** operation runs a real primitive between explicit scheduling points: the scheduler can
order an owner's use of it against other tasks, but the primitive's own instructions and memory
ordering stay unmodeled, so a check claims nothing about them. An operation **outside the modeled
execution** uses a real primitive and supplies no evidence about a checked protocol. The matrix
identifies real families retained by a selected backend; an explicit use of
`nervix_primitives::unmodeled` needs a named permission. An operation a mode cannot provide is
unavailable in that mode and fails to compile; it never falls back to a real primitive. Shared
ownership is real in every mode and on every target: no model checker counts references, so a check
claims nothing about an ordering a reference count establishes, such as the one `get_mut` or
`try_unwrap` relies on. Loom models isolated synchronous owners: atomics, the threads a model
spawns, joins, parks and yields, and thread-local storage. It models no async family, lock,
collection or publication, so a Loom build, in which the server and consensus compile for the models
of their owners, takes the ordinary library for each of those, outside every model.
`just validate-primitive-boundary` rejects every such family in Loom model code, an inline module or
a module file whose declaration compiles it only for Loom, so a model never names a real primitive
silently; what an owner a model drives uses internally is excluded from that model's claim. The check
reads each `cfg` of a module through its `all`, `any` and `not`: a module is Loom model code when one
of them is false in every build without the `loom` feature, and a module whose `cfg` the check
cannot read is rejected. Model
code may name shared ownership and a permitted unmodeled primitive, because each says it is real.
`just test-primitives` shows each mode's selection.

Turmoil replaces the network and the clock, not synchronization. A Turmoil build runs the ordinary
primitives on the simulated host whose task uses them: `net` selects Turmoil's sockets, and
`net::lookup_host`, which only that mode has, answers from the simulated DNS table; the timers and
instants of `time` follow the host's simulated clock; and `task::spawn_cpu` runs an admitted CPU job
through a worker task on the host's scheduler. The worker yields between callbacks, so each
bounded synchronous job body remains one scheduling step. What the
simulated hosts run, and what the simulation establishes, belong to
[Deterministic interconnect simulation](./interconnect-simulation.md).

Deloxide models nothing and replaces only the thread-blocking locks and their condition variable. A
`deloxide` build runs every other family as ordinary execution does, and its tracked locks are real
locks whose acquisitions a deadlock detector also observes, so it runs as a product does and claims
no interleaving. What it reports, and what it cannot, belong to
[Diagnostic deadlock detection](./data-plane-concurrency.md#diagnostic-deadlock-detection).

## Policy Boundaries

The boundary supplies mechanisms and decides no policy. `net` never decides what a name means: a
node resolves through its own resolver in `nervix-dns`, no mode but Turmoil's offers a lookup, and
the boundary check rejects `tokio::net::lookup_host` and the `ToSocketAddrs` traits, which resolve
through the operating system. `time` grants no clock permission: actual UTC and physical deadlines
keep the owners `scripts/check_clock_boundaries.py` declares, and a logical deadline stays in the
domain clock's coordinate, as [Domain Clock](./domain-clock.md) describes. `task::spawn_cpu` belongs
to the bounded executor, which admits, charges and cancels every job; the boundary check rejects it
in any file but the executor's worker pools, so it is no way around admission. `task::spawn_blocking`
is the runtime's blocking pool, which the executor's storage workers run on, and
`task::block_in_place` blocks the runtime worker thread that calls it, which no file owns by
default. Work a node runs off its async workers goes through the executor instead, so the check
rejects either in any other file unless a permission in `crates/primitives/blocking-permissions.toml`
lists it for that file, with the owner, why the owner stays outside the executor, and what bounds
its work instead, as [Blocking work outside the executor](#blocking-work-outside-the-executor)
lists. `pause`, `advance` and `resume` exist only with the `test-util` capability, which ordinary
tests of elapsed-time behavior enable and no production graph has, because a clock that can be
paused is checked on every read.

The executor admits decoded materialized snapshots into `state_read` and optional retained
generator source views into `state_read_cache`. Each pool derives its capacity from the relay
budget and counts independently in the startup total. A snapshot reserves its complete retained
charge in the cache before returning its active-read charges. When that immediate reservation
finds no room, the generator keeps only the current uncached read and its `state_read` admission
through evaluation and dispatch. Neither active nor retained source views occupy the relay
capacity that their evaluation, IPC encoding and remote acknowledgement storage need to finish.
Retention adds no wait, worker pool or execution-mode backend.

A snapshot decoder already holds its encoded input under bulk admission. Its additional scratch
request therefore refuses immediately when no room is available, releasing the input so another
read can progress. Metadata and Arrow sections follow the same rule; framing reads release their
charges before reading the next frame.

The runtime attributes build the selected runtime through a crate path fixed to the boundary: Tokio's
attribute builds whatever runtime its crate path names, and an attribute that named `tokio` at the
call site would construct whichever crate a package happened to call `tokio`. Shuttle's own test
attribute finds its runtime by reading the calling package's manifest for a dependency of a fixed
name, so the boundary does not use it; in a Shuttle build a test attribute builds Shuttle's runtime,
which runs only inside a Shuttle execution.

Every async loop whose body performs async work calls `nervix_primitives::task::consume_budget`
once per iteration near its beginning. The selected scheduler owns that yield opportunity;
callers do not add their own backend choice. `nervix_primitives::task::unconstrained` polls a
future outside the cooperative budget. Once a task has spent its budget, Tokio's channels and
timers read as not ready even when their value has arrived. An owner that must learn what has
already arrived, without waiting for anything, reads through it: the confirmation window settles
every answer its broker gave before a publish failed this way. A future polled outside the budget
is never made to yield, so its owner bounds how much it polls. Shuttle keeps no cooperative budget,
and there `unconstrained` only forwards the poll. `std::time::Duration` remains a value in every
mode.
The bounded executor still owns job admission, class isolation, memory charges and cooperative
cancellation; choosing a task primitive grants none of those permissions by itself.

## Unmodeled Primitives

A modeled primitive exists only inside a run of its model. Using one outside that run is a test
configuration failure the backend reports, and nothing falls back to a real primitive. A real
primitive that must stay outside every model is reached through the unmodeled path, and each use
needs a permission in `crates/primitives/unmodeled-permissions.toml` that names the file, the items
by their paths below `unmodeled`, the owner, why a real primitive is required, and what that leaves
unverified:

| Owner | Why the primitive is real | What stays outside every check |
| --- | --- | --- |
| The Shuttle and Loom runners of `nervix-model-harness` | Their statistics span every model execution they start and are read after the last one | Nothing a check claims; they are runner bookkeeping |
| The WASM runtime's epoch driver and guest-callback time | The epoch driver is an operating-system thread no model runs, which sleeps for real and is stopped by a flag; a guest callback reads its domain time from Tokio's task-local storage, which no model isolates | When the epoch thread observes shutdown, and a callback's time scope, which Shuttle tasks would share while polled |
| The C binding's session | The binding owns the runtime its host's threads enter and shuts it down in the background, which is its contract with the host and which Shuttle's runtime does not provide | Everything the binding runs; the Rust client it drives has its own checks |
| The cluster membership transport and its native watch fixtures | Chitchat hands out Tokio's own lock and watch receiver and runs on Tokio's runtime, so the transport tasks it drives wait, sleep and select on Tokio's primitives; native watcher fixtures construct that same external receiver type | Cluster membership gossip; the native watch assertions are not model-checking evidence |
| Process-wide one-time state: the SIMD instruction level, the interconnect's cryptography provider, and the test and benchmark schema and certificate fixtures | Initialized once per process and read by every later model execution in it | Detection, installation and fixture construction |
| The browser console: its executor installation, and the `futures` channel, `select!` and abort handles its tasks use on the browser's event loop | The browser target runs in no execution mode and has no native capability, so the console takes those families from the `futures` crate, which a native build takes from Tokio | Everything the console runs |
| The VM benchmarks' allocation probe | A global allocator counts allocations made on every thread | Nothing; the benchmark claims nothing about synchronization |
| The application unit-test fixtures | Test databases and node ports must differ across every unit test the process runs in parallel, so their identities outlive each test | Nothing a check claims; no model builds the fixtures |
| The records of the relay gate and fan-out, entity gate, emitter record-write, durability barrier, WASM checkpoint, source host-loop, stream-slot, retained-archive and client ingestor Shuttle checks | A record changes in the same scheduling step as the operation it records, so recording adds no scheduling point | Nothing the owner does: records observe and never synchronize, and the owners' own primitives are modeled |
| The Turmoil runner's real-time bound and the scenario driver's attempt deadline | A run's bound must expire even when a host blocks the scheduler thread and simulated time stops advancing, and the driver kills an attempt process that outlived its bound; both read the operating system's monotonic clock | Nothing a scenario claims: they decide only when a run fails, never how a simulated request proceeds |
| The integration-test harness's port pool and the benchmark driver | They reserve free ports from the operating system for real node processes and containers, synchronously and before anything binds them, with the standard library's blocking listener | Nothing a check claims; the ports carry the traffic of real processes |

A real primitive never carries the protocol under test, chooses its branches, supplies its wakeups
or establishes an ordering an assertion relies on. A permission covers one Rust file the boundary
governs and lists real items of the unmodeled path; a permission for a directory, a glob, a file
outside the governed sources, or an item the path does not have is rejected, and so is a permission
or a listed item nothing uses.

A selected atomic belongs to the model execution that constructs it, so it never lives in a
`static`. A static is constructed once per process and would carry its state from one execution into
the next, and Loom's atomics have no const constructor, so a crate that declares one does not build
with Loom at all. Process-wide state lives on the owner whose lifetime it has instead: a node's
session service owns the draws its subscriptions sample with, and a node's Raft network owns the
identities of the snapshot transfers it sends. A count a unit test reads is kept per thread, and
state that must outlive every test and model is a real atomic under a permission, which may live in
a `static`.

The rule binds the boundary itself. The boundary names every backend, so the import rules do not
read its sources, but a `static` it declares holds only real atomics, named through
`crate::unmodeled::sync::atomic` or the standard library, and the state it builds lazily for the
life of the process is real too. The deadlock detector's process-wide state is that kind: whether
this process claimed its installation, whether order checking is on, and the order history its
registry keeps are real atomics. One mechanism the detector reaches is shared rather than owned: its
report queue is the `ReportHandoff` that Shuttle checks, whose refusal counter must stay a selected
atomic for the check to explore it. The detector holds that queue in a `static` only in a `deloxide`
build, which selects the standard library's atomics and compiles no model, so no model execution
ever constructs or outlives it.

## Authored Source And Manifest Scope

`just validate-primitive-boundary` rejects every other path to a governed family however it is
spelled: a direct, renamed, grouped or glob import, a fully qualified path, an attribute, a renamed
crate, an imported `sync`, `time` or `net` module, or a path in a macro body or an inactive `cfg`
branch. That covers shared ownership through `triomphe` or the standard library's `sync`, the
`futures` crates' channels, locks, executors, waker registration, `select!` and abort handles,
Tokio's `time` and `net` modules, Turmoil's `net`, Deloxide's locks and detector, and the standard
library's `Instant` and sockets. Each rejection names the approved path; `tokio::net::lookup_host` and the `ToSocketAddrs` traits
name the node's resolver instead. It also rejects an unmodeled use without its permission, a stale
or misplaced permission, a dependency on a library whose family the boundary selects from any
package but the boundary, a manifest entry that renames a governed crate, `task::spawn_cpu` in
any file but the executor's worker pools, `task::spawn_blocking` in any file but those pools and
the owners a blocking permission declares, `task::block_in_place` in any file but the owners a
blocking permission declares, and a blocking permission that lists an item its file no longer
names. The source and manifest rules read every tracked file and every new file Git does not
ignore, whether or not the workspace lists its package: the isolated analysis workspace under
`tools/nervix-lint`, with its driver, fixtures and fixture macros, is authored source like any
crate. What a build wrote is not: Cargo tags each build directory it creates with a `CACHEDIR.TAG`,
and the check reads nothing below a tagged directory, wherever it is nested. Turmoil is a runner as
well as the network the boundary selects, so beside the boundary, a package whose harness drives a
simulation may depend on it, only as an optional dependency its own `turmoil` feature enables.
Deloxide is the boundary's alone: no other package depends on it, and the boundary depends on it only
optionally, so no ordinary graph contains it.

An execution mode is a feature of the boundary, never a global cfg: `--cfg loom` would reach every
crate of a build, and Tokio and other dependencies read the same names. The check rejects a bare
`loom`, `shuttle`, `turmoil` or `deloxide` in a `cfg` predicate, and `--cfg` with any of those
names in any `justfile` recipe, Cargo configuration, workflow or build script. Tokio's
unstable runtime controls belong to the Turmoil build: the check rejects `--cfg tokio_unstable` in
any `justfile` recipe but a Turmoil one, and in Cargo configuration, a workflow or a build script.

The analysis cfg of the [source contracts](./data-plane-concurrency.md#source-contracts), `nervix_lint`, is tooling only and
selects nothing. Only the analysis driver sets it, for the crates it analyzes, and the analysis must
read the code a product build compiles. The check rejects `--cfg nervix_lint` in any `justfile`
recipe, Cargo configuration, workflow or build script, a `cfg` or `cfg!` predicate that names it,
and a `cfg_attr` under it that holds anything but `nervix::` contracts and lint levels, such as a
`path`, a `derive` or a nested `cfg`. An annotation hides nothing from the other rules: a governed
path is rejected in the code an annotation gates as it is anywhere else.

The check rejects a `static`, including one a `thread_local!` declares, whose declared type names a
selected atomic, directly or through a wrapper, an array, a reference, a module path or a local type
alias, and a `static` or `const` initializer, `const fn` or `const` block that constructs one. It
applies this rule, and only this one, to the boundary's own sources as well, where
`crate::sync::atomic` names a selected atomic and `crate::unmodeled::sync::atomic` and the standard
library's and `core`'s atomics name real ones. It reads declared types and constructions, so a
struct holding an atomic that a static builds lazily is left to review.

Guest code is outside the source rules: the WASM guest SDK and every guest library built on it are
compiled into a user's WASM guest, a single-threaded program in the host's sandbox where no mode
exists and no Nervix process runs, and a user's guest takes Arrow's standard references without the
boundary. The rules still read their manifests. Tokio's I/O traits, filesystem, process and signal
modules are real in every mode, and so are the pure polling combinators of Tokio, `pin!` and
`join!`, and of the `futures` crates, which poll futures without synchronizing anything of their
own. The browser's event loop, its executor reached through `wasm-bindgen-futures` and Leptos and
its timers through `web-sys`, exists only in a target no mode runs. The cells of `std::cell` are
confined to one thread. None of these is a family the boundary selects.

## Builds, Modes And Product Binaries

Each mode is its own build invocation, with explicit features, because Cargo unifies the features of
one invocation:

| Mode | Lint | Tests | Coverage |
| --- | --- | --- | --- |
| Ordinary | `just lint`: every package in ordinary mode, the server, the client, the formatter, the browser console and the wire crate for the browser | `just test`, `just test-primitives-ordinary` | `just test-coverage`; ordinary extras and `test-primitives-ordinary` through `just coverage-native-extras` |
| Shuttle | `just cargo-clippy-shuttle`: every Shuttle library, and each package `just test-shuttle` explores in test mode, where its checks are compiled | `just test-shuttle [filter]`, `just test-shuttle-replay-check`, `just test-primitives-shuttle` | `just coverage-native-extras test-shuttle test-primitives-shuttle`; `just coverage-shuttle <output> [filter]` |
| Loom | `just cargo-clippy-loom` | `just test-loom [filter]`, `just test-loom-qualification`, `just test-primitives-loom` | `just coverage-native-extras test-loom test-primitives-loom`; `just coverage-loom <output> [filter]` |
| Turmoil | The Turmoil targets of `just cargo-clippy` | `just test-turmoil`, `just test-turmoil-replay-check`, `just test-primitives-turmoil` | The canonical suite through `just coverage-native-extras test-turmoil`, with separate CI artifacts and advisory Turmoil patch feedback; `just coverage-turmoil <output>` copies a complete report; native conformance through `just coverage-native-extras test-primitives-turmoil` |
| Deloxide | `just cargo-clippy-deloxide`: every diagnostic selection of the boundary, reporting owner, paced driver and server, including probes and scenarios | `just test-deloxide`, `just test-deloxide-order`, `just test-deloxide-stress`, `just test-deloxide-stress-restore`, `just test-primitives-deloxide` and diagnostic compile checks in `just test-primitives-compile` | Native conformance through `just coverage-native-extras test-primitives-deloxide`; `just coverage-deadlock` collects the `test-deadlock-evidence-order` and ordinary `test-deadlock-report` producers in separate builds with canonical completion records and separately flagged diagnostic-evidence coverage; full diagnostic workloads retain separate evidence |

Model coverage uses the canonical inventories and runners, including per-test process isolation,
Shuttle random/PCT exploration and nondeterminism checking, and Loom InvariantIds and exhaustive
completion. `models.json` retains discovered/selected/executed/completed counts and exact check
identities; the coverage collector requires matching complete evidence before exporting LCOV.
Loom weakening qualification runs independently outside instrumentation. Coverage bookkeeping
runs in the command processes and supplies no ordering, wakeups or branches inside a model.
The per-mode build directories and fresh attempt directories prevent one mode from replacing
another's executable or counters. Native source-line execution complements each model's claim;
it does not broaden the ordering or interleaving guarantee. Mode reports remain separate from
ordinary coverage and its advisory CRAP report. The
[full workspace union](./coverage-reports.md#the-full-workspace-union) adds them to the ordinary
report with their mode attribution, and accepts a Shuttle or Loom collection only with complete,
unfiltered `models.json` evidence. See
[native extra coverage](./coverage-reports.md#native-collection) for collection, artifacts,
filtering and replay commands.

The integrated ordinary cost probe is `just bench-primitives`. It measures the boundary's atomic
access, `ArcSwap` publication, an uncontended blocking mutex, and shared-reference clone/drop
without a model or diagnostic build's scheduling points. The production relay and admitted-work
benches measure their owners, and `just bench-task-handles`
retains timing and allocation samples. Compare uninstrumented builds on one host with the same
profile and load; a model run or coverage build cannot serve as a throughput baseline. The
[integrated qualification procedure](developing-nervix.md#integrated-primitive-qualification)
keeps those measurements beside revision-bound model and external fault evidence.

`just validate-execution-mode-dependencies` resolves the normal dependency graph of the workspace
and of every package on its own, the way a consumer builds it, with default features and without
them, and rejects Loom, Shuttle, a Shuttle wrapper, Turmoil or Deloxide in any of them, and an
execution mode, the diagnostic one included, or the `test-util` capability enabled on the boundary. It also keeps the portable graphs portable:
the vocabulary crate, and the browser console and the wire crate for the browser's target, contain
no async runtime or network library and never enable the boundary's `native` capability.
`just validate-execution-mode-conflicts` requires the combined-mode diagnostic, including for modes
two dependencies select separately, the boundary's own diagnostic for a mode or the native
capability requested for the browser's target and for the diagnostic mode requested without the
native capability it tracks, and the product-binary diagnostics.

A build that selects a modeled mode is a test artifact. Every binary Nervix ships, the server, the
CLI and the Plexus formatter, declares itself with `nervix_primitives::product_binary!`, which fails to
compile in a build that selects `loom`, `shuttle` or `turmoil` and names the binary and the mode, so
no modeled binary can be built, let alone published. A `deloxide` build is a diagnostic artifact
instead, which runs but is never released: it compiles only a binary that declares a diagnostic form,
`product_binary!("nervix-server", diagnostic)`, and such a binary installs the deadlock detector at
start-up. The server is the one binary that declares it; the CLI and the formatter fail to compile in
that build, naming themselves. `just validate-primitive-boundary` requires the declaration, in either
form, in the crate root of every binary the release image's build compiles, and the release image
builds the ordinary server.

## Blocking Work Outside The Executor

A node runs its variable-size encoding, decoding, validation, hashing, compilation, program
execution and synchronous filesystem or database work through the bounded executor, which admits
each job into a CPU or storage class, charges its memory, and gives it the cancellation it checks
between its bounded units. Operator-supplied code the node cannot bound, a Roto UDF call or a JAQ
transformation, takes the extension class, so a program that never returns holds only that class's
workers. Password hashing takes the credentials class and its own budget, so anyone who can reach a
listener spends only those, and saturated data, extension or bulk work never delays a login. The
executor's storage workers are the one built-in owner of the runtime's blocking pool, and no file
owns `task::block_in_place`, which blocks the runtime worker thread that calls it.

Connector network control calls use a storage class of their own, with two admitted workers by
default and the same bounded queue policy as other classes. Kafka source metadata, watermarks,
timestamp queries, seeks, assignments and consumer-group commits use it, and so does the destruction
of a Kafka consumer or producer whose last handle is gone. A synchronous commit may
outlive its caller's thirty-second wait, so sharing filesystem or consensus worker permits would
let an unavailable broker hold those operations behind it. The connector class bounds those jobs
without taking another class's admission. All classes still use the process-wide blocking pool;
admission isolation does not reserve physical threads.

Everything else that blocks a thread is a declared owner in
`crates/primitives/blocking-permissions.toml`, which names the file, the blocking items the owner
names there by their paths below `nervix_primitives`, the owner, why it stays outside the executor,
and what bounds its work instead. A permission declares its file for exactly the items it lists, so
a file whose owners block in different ways, such as the CLI's, has a permission for each:

| Owner | Blocks through | Why it stays outside the executor | What bounds it |
| --- | --- | --- | --- |
| The resolver's configuration load in `nervix-dns` | `task::spawn_blocking` | The Rust client and the CLI build the same resolver without an executor, and a node loads it through the same function in its startup preflight | Two small files, read once for each resolver built |
| The Kafka emitter's final producer-queue drain | `task::spawn_blocking` | librdkafka's flush waits on the network, independently of the source control-call class | The host's physical flush deadline |
| The Rust client's backup download and restore upload, and the CLI's event printer | `task::spawn_blocking` | A client tool is not a node and has no executor | One rename of a written archive, one sequential read of an archive, and one printer thread for the life of the command |
| The CLI's completion prompt | `task::block_in_place` | A client tool is not a node and has no executor; its line editor asks for completions synchronously on the thread that runs the CLI's main task, so the prompt waits there for the completion while the runtime's workers keep the session running | One completion at a time, only while the operator waits at the prompt; each page of suggestions ends by the client's retry deadline, and a repeated continuation ends the completion |
| The benchmark driver and the scenario harness | `task::spawn_blocking` | A harness is not a node; it waits on brokers, databases, probe threads and the C binding from its own process | Each call's own timeout, or the probe or thread it joins |

The boundary check rejects either item in a file no permission lists it for, and a listed item its
file no longer names.

## Executor Admission And Draining

Every class holds a bounded FIFO of admitted jobs. A worker takes the next job directly after its
current callback exits, retaining the same blocking-pool dispatch while a backlog remains. The
consensus storage class has one worker and executes jobs in admission order. CPU workers use
`task::spawn_cpu`: its callback runs one job and returns whether another has been selected. In
ordinary and Shuttle builds the boundary drains those callbacks on one blocking-pool task. Under
Turmoil it yields on the simulated host between callbacks, preserving one bounded body per
simulation step. Storage workers drain through `task::spawn_blocking` in every mode.

Submission, exact queued cancellation, selection and the empty-to-idle transition share one
class-local mutex fence. Each acquisition performs one insert, keyed removal or FIFO selection;
callbacks, destruction, result delivery and semaphore release happen outside it. A submitter either
finds an active worker that will observe its job or claims a worker itself, so submission racing the
last worker's exit cannot strand an admitted job. No idle worker retains a blocking-pool thread.
The queue semaphore still bounds pending work, refuses `RefuseWhenFull` with typed `QueueFull`, and
hands a freed place to an existing `WaitForPlace` waiter before a later requester.

The storage submission adapter and each job's invocation carry explicit dispatch contracts for
the caller-owned synchronous callback. Its effects remain the caller's responsibility; the
executor's bounded contract covers admission and queue transitions outside that callback.

A caller withdrawing while its job remains queued removes that exact entry and immediately returns
its memory charge and queue place. Once selected by a worker, the job owns its charge until its
callback exits and observes the caller's cancellation between bounded units. A detached storage
submission returns only after its job has been selected. Each callback catches its own panic,
returns `JobPanicked` to its caller, records its completed service and leaves successors runnable.
The worker publishes its next selection or its idle transition before delivering the completed
result. Existing per-class admission, refusal, completion, queue-time and work-time counters retain
their meanings; work time measures callbacks, excluding dispatch and queue waits.

Worker state distinguishes a selected callback, a callback executing and an idle worker. Runtime
teardown that drops a worker between callbacks, or before its blocking task starts, returns its
active place and closes the retained queue with `PoolClosed`. Selected and queued charges are
released outside the fence; callbacks already running on other workers keep theirs until they
exit. A subsequently reused class can claim a replacement worker.

The FIFO mutex is tracked by Deloxide. Its owner and public workloads are registered in
`tests/deloxide-inventory.toml`; Shuttle covers submission racing worker exit as well as cancellation,
admission ordering and class isolation. No new atomic publication protocol is introduced.

The public bulk-isolation scenario occupies every node's bulk workers while six sequential
management mutations and a subsequent domain listing complete, on one- and three-node clusters.
A production client connects before occupation; its initial follower route and any leader redirect
remain inside the fully occupied cluster. The mutations share one ten-second budget and the listing
has another. The harness drives and timestamps replies independently of its cooperative scenario
runner, checking the sixth mutation's acknowledgement explicitly. Holds remain through readiness
and stable-leader observations, then cleanup releases them before diagnostics and node stop, even
after a failed assertion. These harness bounds provide public class-isolation evidence; product
request deadlines keep their existing owners.

## Thread Creation Outside The Executor

Starting a thread can bypass the executor's admission, memory charges and cancellation just as
blocking work can. Every caller of `thread::spawn`, `thread::Builder`, `thread::scope`,
`thread::spawn_detached` or `unmodeled::thread::Builder` therefore declares its exact file and
items in the same `crates/primitives/blocking-permissions.toml`. The declaration states its owner,
why the work stays outside the executor, and what bounds the work, thread count and lifetime.
No caller outside the primitive crate has a built-in permission. Naming the builder includes its
ordinary and scoped spawn methods; a glob names every confined item it brings into scope.
Reimported module aliases retain the same confinement regardless of import order. A glob over
the boundary root is rejected; name its modules and items explicitly so confined and unmodeled
paths remain visible.

| Owner | Thread items | Work, count and lifetime bound |
| --- | --- | --- |
| Process termination supervision | `thread::spawn_detached` | Two supervisors per process, parked between signals or deadline changes, plus at most one forced-exit watchdog after the single exit claim; supervisors end with the process, and the watchdog waits only the forced-exit report budget |
| WASM epoch driver | `unmodeled::thread::Builder` | One thread per WASM runtime; one sleep and one engine epoch increment per configured tick; it ends on its next wake after the stop flag is set or the weak stop reference can no longer upgrade |
| Diagnostic finding recorder | `thread::spawn_detached` | At most one watchdog for the process's one finding; it exits the process after the ten-second recording budget |
| Shared Loom participant launcher | `thread::Builder` | Loom's five slots, including coordinator and body, the branch limit and runner watchdog; participants end within one model execution |
| Benchmark load driver | `thread::Builder`, `thread::scope` | One joined summary poller per drain owner, and one scoped watermark query per configured partition, each with the request timeout within the driver deadline |
| Turmoil scenario runner | `thread::Builder` | One scheduler per disposable attempt; physical run and cleanup deadlines, a normal join, and a failed process when either deadline is missed |
| Plexus completion walk | `thread::Builder`, `thread::scope` | At most the configured jobs per finite frontier; every worker is joined before the next level |
| Public C binding probes and external Kafka member | `thread::spawn`, `thread::scope`, `thread::Builder` as listed for each file | Fixed probe participants joined within their bounded call and cancellation waits; one Kafka poller per member, stopped and joined when the member drops |

Unit tests and Loom and Shuttle checks declare every file and its exact thread items too. Their
permissions describe finite participants for each case, joins or model termination, and the
registered exploration bounds where applicable. Diagnostic cycle probes instead use disposable
processes and a parent watchdog, since their tested cycle intentionally cannot join. These
declarations do not exempt a directory or evaluate away `cfg(test)`, a model-only module, an
inactive branch or a macro body. A new item needs its declaration, and a listed item its file no
longer names fails as stale. A file whose product owner and tests use different items names both.

The real epoch builder also keeps its independent unmodeled permission, with its reason and
verification limit. That permission does not supply the work and lifetime bound and cannot alone
authorize thread creation. The ordinary re-exports and every modeled thread operation retain their
existing execution semantics; this confinement changes source validation only.

## Verification Contracts

The boundary and its callers have separate proof obligations. Choose evidence for the fact being
claimed; a successful build, an executed test, a completed model search and a source coverage
report establish different things.

| Question | Owner and entry point | What a passing result establishes |
| --- | --- | --- |
| Did authored code obtain a governed family through the boundary, with the required permission? | `just validate-primitive-boundary` | Source and manifest provenance, including inactive configurations, aliases and authored macro fragments, within the check's documented structural scope |
| Do ordinary and portable dependency graphs contain only their permitted capabilities? | `just validate-execution-mode-dependencies` | The resolved Cargo graphs inspected for the workspace, individual packages and browser targets |
| Do incompatible modes, unsupported targets and product binaries fail with the intended diagnostic? | `just validate-execution-mode-conflicts` | The actual feature/target combinations built by each rejection probe, including transitive mode selection |
| Does one current Rust API accept its supported use and reject a forbidden operation? | Paired compiling and `compile_fail` Rustdoc examples beside that API; `just test-primitives-compile` and `just test-capability-docs` | Those snippets compile or fail in the configuration used for the doctest |
| Do resolved architecture diagnostics recognize their semantic facts and narrow exceptions? | `just test-typed-ratchet`, with [compiler contracts](./architecture-enforcement.md#authoring-and-qualification) | Allowed/rejected fixtures, precise diagnostics and spans, and invalid, unfulfilled and widened expectations in the declared compiler matrix |
| Does an adapter retain its ordinary backend's contract? | `just test-primitives`, once per execution mode | The selected conformance and race scripts actually complete under each backend |
| Is a caller's interleaving or memory-ordering invariant exercised? | Registered production-owner Shuttle checks or Loom models through `just test-shuttle` or `just test-loom` | The reported completed search and bounds for that invariant, with the model's documented visibility limits |
| Does an integrated operation behave correctly? | Public Cucumber, the supported Turmoil suite, or immutable-image Chaos under their owning chapters | The public operations and fault cases actually exercised, within that harness's scope |
| Did tracked real blocking locks deadlock in a diagnostic workload? | The four canonical Deloxide selections | Qualifying detector evidence for the acquired locks and processes that completed; untracked paths remain outside the claim |

A `compile_fail` example belongs beside the API that owns the restriction and is paired with a
compiling example using the current imports and supported signature. An example that fails because
its imports no longer resolve does not establish the intended rejection. Doctests use the
configuration of their containing build; they cannot choose a different Cargo dependency graph for
each snippet. Graph isolation, feature conflicts and browser capability rejection therefore keep
their dedicated Cargo probes. Compiler fixtures keep their semantic diagnostic assertions. Actual
model execution keeps its own inventory and completion records. None is replaced by a collection
of compile failures.

### Caller Models And Failure Evidence

A caller's Shuttle check drives its production owner with scheduler-visible coordination and
registers in `crates/model-harness/shuttle-inventory.toml`. Registration happens before the deciding
read when waiting for notification. A memory-ordering claim uses the synchronous production owner
under Loom, names its `InvariantId`, registers in `crates/model-harness/loom-inventory.toml`, and
has a registered weakening that must fail and replay. Shared references, opaque dependencies,
async I/O and real primitives never silently expand either model's claim. [Deterministic
Concurrency Verification](./data-plane-concurrency.md#deterministic-concurrency-verification)
owns exact exploration, registration and replay requirements.

The canonical runners distinguish discovery, selection, execution and completion. A missing,
ignored, unregistered or incomplete required check fails; selecting no check fails. Preserve the
failure's actual source, inputs, schedule or checkpoint, bounds, toolchain and output before
changing its owner. A later passing run retains its own source and does not explain an earlier
failure. Shuttle replay reproduces a recorded schedule; Loom qualification demonstrates dependence
on the registered ordering; Turmoil replay reproduces its recorded simulation; external Chaos
replays its action plan without claiming deterministic operating-system scheduling.

Deloxide installs once per diagnostic process before any tracked lock or runtime worker exists.
Its [applicability and evidence contract](./data-plane-concurrency.md#diagnostic-deadlock-detection)
requires active, order, stress and stress-restore selections when tracked synchronization or a
lifecycle path using it changes. The owner inventory records which workloads reach each lock site
and which paths remain unexercised. Async synchronization, atomic publication and network waits
need their own applicable evidence. A clean detector run does not establish their safety.

### CI, Coverage And Toolchains

`just validate` includes primitive provenance, dependency and conflict checks beside the resolved
architecture diagnostics. `just test` includes ordinary tests, public scenarios and paired API
doctests. Primitive conformance and the complete model selections also have dedicated recipes;
the [development guide](./developing-nervix.md) owns the full command catalog. A change affecting
Shuttle, Loom or Turmoil carries the corresponding PR label so its dedicated CI jobs run. A change
covered by the Deloxide applicability rule carries `deloxide`; membership, failover, recovery or
delivery behavior exercised by external Chaos carries `chaos`. Labels enable evidence and do not
replace the local checks or source-owned applicability review.

Registered Bolero properties use the same production path and complete assertion in ordinary
randomized/corpus execution and sanitizer fuzzing. Ordinary execution is required when the Bolero
workflow runs. Sanitizer CI requires a pull request carrying `fuzz`; an unlabeled, scheduled or
manually dispatched run expects a sanitizer skip. Check may exclude the entire reusable Bolero
call only after verifying its prose-only selection without `fuzz`. A skip contributes no fuzz
execution or coverage. [Property Testing And Fuzzing](./property-testing-and-fuzzing.md) owns this
policy, its inventory and failure accounting.

Use the toolchain files' declared `llvm-tools`: stable instrumentation and exports use the stable
compiler's tools, and the isolated architecture analysis and sanitizer campaigns use their own
pinned toolchains. Never combine incompatible raw profiles or substitute system LLVM tools for
the producing compiler's tools. Every Rust build, probe, doctest, lint, benchmark and coverage
invocation retains the configured kache wrapper, including isolated compiler and qualification
builds. A cache or compiler failure is reported at its owner; disabling the wrapper changes neither
the requirement nor the validity of the evidence.

Native coverage collection records source-line execution only after its canonical producer
completed, beside its completion and model or lane records. Keep ordinary coverage, model coverage,
diagnostic coverage and sanitizer campaign evidence distinct. A source line being covered does not
prove all interleavings, sufficient memory orderings, or absence of an unobserved deadlock.
The [CRAP report](./coverage-reports.md#advisory-crap-report) has advisory scores; its input validity and
the independent `just ratchet` complexity verdict remain mandatory checks.
[Integrated primitive qualification](./developing-nervix.md#integrated-primitive-qualification)
binds ordinary costs and public/model/fault results to their actual revision and immutable images.
Run records, audit registers and measurements belong on the owning task. Architecture documentation
states durable guarantees and limits, without turning one passing snapshot into a permanent claim
about later source.
