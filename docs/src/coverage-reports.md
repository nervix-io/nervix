# Coverage Reports And The Complexity Gate

This chapter owns ordinary coverage producers and exports, checkout-root and tracked-source
mapping, the merged report and its integrity verdicts, patch coverage, the advisory CRAP report,
the full workspace union, and how they relate to the mandatory complexity count. Changes to those
contracts keep this chapter current.
[Architecture Rules And The Debt Ratchet](./architecture-enforcement.md#function-complexity)
owns the analyzer's source eligibility, complexity measurement and enforcement details.
[Integration Test Lifecycle](./integration-test-lifecycle.md) owns scenario supervision and budgets;
[Execution Primitives](./execution-primitives.md) and each verification method's chapter own what
its execution establishes. Collection adds source counters to that execution, without replacing
its verdict or extending a product deadline.

## Ownership And Verdicts

Every ordinary coverage consumer reads the same repository-relative `lcov-workspace.info`, built
by `just coverage-merge`. Its `target/coverage-merge/record.json` identifies the revision, inputs,
source policy and completeness verdict. The coverage job publishes these together as
`coverage-merged`. Patch and project line percentages and CRAP scores are advisory; report
integrity is mandatory. A merge missing an input or workspace member, or an analysis that cannot
match its input to the sources, fails. Missing evidence never becomes zero coverage or zero debt.

The deterministic complexity verdict belongs to `just ratchet` in the `checks` job. It counts
functions above cyclomatic complexity 30 and reads no coverage. A change cannot raise that count
above `debt-baseline.json`; when it falls, the baseline falls in the same change.

A producer that adds ordinary coverage registers its exported reports in the merge's inputs.
A producer that stays outside the ordinary report states its execution scope and why. Reports of
Shuttle, Loom, Turmoil, Deloxide and live Bolero fuzz campaigns describe different execution modes:
they retain separate reports and attribution. The [full workspace union](#the-full-workspace-union)
combines them with the ordinary report into a report of its own, which no ordinary consumer reads;
its integrity is mandatory too. Chaos runs against prebuilt images and supplies no
LLVM counters. Browser, guest WASM and other-language execution is not inferred from native Rust
counters.

## Ordinary Producers

| Producer | Execution and exported evidence |
| --- | --- |
| `just test-coverage` (`tests`) | Ordinary workspace unit, binary, benchmark test-mode and harness-liveness tests. Packages that own execution modes run separately with ordinary features, so an all-features invocation never combines modes. Exports `lcov-workspace.info` and `coverage-source-root.txt` as `coverage-tests`. |
| `just test-scenarios-coverage` (`scenarios`) | The public Cucumber suite, including its instrumented server, CLI, report tool, paced Rust driver and shared binding. Exports the same report/root pair and the paced Python drivers' `python.lcov` as `coverage-scenarios`. |
| `coverage-native-extras test-typed-ratchet` (`extra-tests`) | Source-contract compiler fixtures, including their paired fixture doctests and loaded procedural macro objects, using the analysis compiler's matching LLVM tools. Product API doctests and modeled fixture checks finish separately without instrumentation. |
| `coverage-native-extras bench-smoke` (`extra-tests`) | The smoke recipe's benchmark bodies in test mode. Other Cargo benchmark test targets run in `test-coverage`. Instrumented timings are not performance evidence. |
| `coverage-native-extras test-primitives-ordinary` (`extra-tests`) | The primitive boundary's native ordinary conformance checks. Other conformance modes remain separate. |
| Language completion walk (`extra-tests`) | The composed grammar's completion reachability check for Plexus, the Nervix graph definition language, collected under the current `plexus-completion-walk` recipe. |
| Connector declaration validation (`extra-tests`) | The compiled Plexus vocabulary export and comparison of generated capability tables and manual regions, collected by `validate-connector-matrix`. |

The `extra-tests` job also exports ordinary Python tooling reports. Each recipe executes the
owner's tests under coverage and publishes `python.lcov`; the merge discovers those names inside
the downloaded artifact:

| Recipe | Tooling it measures |
| --- | --- |
| `coverage-patch-runner` | Patch line accounting and comment publication |
| `coverage-merge-runner` | Ordinary source normalization, merge and integrity verdicts |
| `coverage-crap-report-runner` | CRAP analysis, binding and advisory report construction |
| `coverage-full-runner` | The full workspace union's selection, validation and attribution |
| `coverage-paced-simulation-python` | Paced Python drivers' deterministic open policy |
| `coverage-release-evidence` | Required revision/check evidence accounting |
| `coverage-operator-deployment` | Kubernetes deployment identity, public readiness observations, failure cleanup and image-publication gating |
| `coverage-kache-ci` | The shared compiler cache's remote publication contract and build-runner diagnostic tooling |
| `coverage-connector-matrix` | Plexus connector matrix rendering, generated manual regions and declaration drift checks |
| `coverage-ci-selection` | Exact-source suite and Bolero call selection |
| `coverage-wasm-bindgen` | The console build's wasm-bindgen CLI selection, installation and verification |
| `coverage-model-runner` | Native collector supervision, command and artifact accounting |
| `coverage-typed-ratchet-python` | Architecture source/compiler rule runners and their qualification |
| `coverage-primitive-boundary` | Primitive provenance and thread-owner validation |

Only reports the artifact actually exports are inputs; running a focused coverage recipe does not
register it in CI by itself. Focused Backup, Visual Create, client and runtime recipes export local
ordinary reports for `coverage-patch`, using the same normalization as the workspace merge.
`just coverage-otel` appends the OTEL and related connector-admission scenarios to current-source
ordinary profiles, with the browser build and configured ORT environment. It exports a local
focused report, rather than a CI merge input. Start a new source generation with
`just coverage-clean-workspace`; owner collections and this focused recipe can then append to it.
Checks [outside collected counters](#verification-outside-collected-counters) retain their own
verdicts, including Deloxide supervision and compiler-cache qualification.

The `diagnostic-evidence` job runs `just coverage-deadlock` and retains the `diagnostic-evidence`
artifact separately. It collects `test-deadlock-evidence-order` in `deloxide-order` mode and
`test-deadlock-report`, which exercises the local report CLI, in ordinary mode. Their combined
`target/deadlock.lcov` and individual completion records describe diagnostic tooling coverage;
neither collection is an input to the ordinary workspace merge, including the ordinary report-tool
collection. Its ordinary mode alone does not register it as a workspace-report producer.

## Native Collection

The extra checks that execute Nervix code natively in ordinary mode, the compiler synchronization
fixtures, benchmark bodies, primitive boundary conformance and Plexus completion walk, also measure which source
lines they execute. Run them the way CI does:

```bash
just coverage-native-extras
```

Name producers to run only those: `test-typed-ratchet`, `bench-smoke`, `plexus-completion-walk`, `validate-connector-matrix`,
`test-shuttle`, `test-loom`, `test-turmoil`, `test-deadlock-evidence-order`, `test-deadlock-report`,
`test-deloxide`, `test-deloxide-order`, `test-deloxide-stress`, `test-deloxide-stress-restore`, or
an individual `test-primitives-<mode>` recipe. The focused diagnostic owner and probes use the
`deloxide-order` build; the local report command uses the ordinary build; each diagnostic lane
builds in the instrumented build of the feature its selection compiles, the two stress lanes in one,
and exports only after its
`lane.json` records a complete run, including successful build and workload stages with their
separate budgets and measured durations. The builds stay in that same instrumented target and
preserve kache; moving them before the workload clock does not move them outside instrumentation.
The build stage measures an initial and a warm pass over the selected builds with identical
features and compiler context; both fit inside the same build budget, before workloads start.
An instrumented record missing either measurement fails completion. A build-only measurement's
`built` verdict cannot produce coverage completion. `test-primitives`
selects native conformance in ordinary, Shuttle, Loom, Turmoil and Deloxide execution:

```bash
just coverage-native-extras plexus-completion-walk
just coverage-native-extras test-primitives
just coverage-native-extras test-shuttle test-loom
just coverage-native-extras test-turmoil
just coverage-native-extras test-shuttle --filter cancellation
just coverage-loom target/loom.lcov execution.cancellation
```

A producer runs its check exactly as `just <producer>` does and fails when the check fails.
`rust-toolchain.toml` includes the `llvm-tools` component because only the compiler's own LLVM
tools read its profiles. What the check needs first, such as the web console the server benches
link, is built normally. The recipe
that executes Nervix code then runs in the environment `cargo llvm-cov show-env --sh
--no-rustc-wrapper` describes: every crate is compiled with source coverage instrumentation into
`target/native-coverage-build` for ordinary mode and `target/native-coverage-build-<mode>` for
each other mode, and the configured kache wrapper stays in place. The `test-deloxide`,
`test-deloxide-order`, `test-deloxide-stress` and `test-deloxide-stress-restore` lanes instrument
only workspace crates: the collector moves the
instrumentation flags into `scripts/coverage_workspace_wrapper.py`, which Cargo runs beneath kache
as the workspace compiler wrapper, the scope `cargo llvm-cov` gives the ordinary coverage build.
Their nodes must meet product deadlines while they compile WASM processors and process Arrow state,
and instrumented dependencies made that several times slower; the report is the same, because
export keeps only repository sources. Kache runs a workspace wrapper chain directly, so their
workspace crates are compiled each run while dependencies come from the cache. Each mode has a
separate build lock through export. The parts that only compile or target the browser stay uninstrumented;
`just test-primitives-compile` retains their independent verdict. Miri, mutation testing and
`just test-loom-qualification` never run under this command. The compile and qualification
commands run without the collector's profile environment, so altered source cannot contribute to
the current-source report.

Every run collects into a directory of its own,
`target/native-coverage/<producer>/<mode>/<toolchain>/<attempt>/`. The attempt is the GitHub
Actions run and attempt in CI, and the time and process locally. No run reuses, reads or cleans
another run's directory, so no run can count another's counters. The directory holds:

| Entry | Content |
| --- | --- |
| `lcov.info` | The LCOV report of the repository's sources, with the absolute paths `llvm-cov` writes |
| `completion.json` | The completion record: the verdict and everything it covers |
| `executions.jsonl` | Every executable Cargo ran, with its arguments and build ID |
| `models.json` | For Shuttle and Loom, canonical discovery, selection, execution and completion evidence |
| `export.log` | The diagnostics of `llvm-profdata` and `llvm-cov`, each bounded |
| `profiles/` and `merged.profdata` | The raw counters and their merge; they stay on the machine |

Cargo runs each instrumented executable through the collector as its runner, which records the
execution and points the counters of the executable, and of every process it starts, at `profiles/`,
one file per process and module. Build scripts and procedural macros execute only when a crate is
compiled rather than served from the cache, so their ordinary build-time counters are discarded.
The compiler fixture is itself an executed check: its loaded procedural macro objects are retained
by build ID, including those in nested Cargo output directories. The producer uses its pinned
compiler's LLVM tools. The report is exported with the toolchain's own
`llvm-profdata` and `llvm-cov` from exactly the executables that ran and the children they started.
It keeps the files inside the repository and leaves out dependencies, harness code below a `tests`,
`examples` or `benches` directory, and code generated below a Cargo target directory.

`completion.json` names the producer and mode, the tested commit and whether the working tree was
modified, the CI run and attempt, the compiler and its LLVM version, the instrumentation flags, the
recipes that make up the check, the executions and child executables selected, the profile files
with the binary IDs they name, the warnings `llvm-cov` printed, and the executable and covered lines
of each package. Model producers also embed `models.json`: exact package/test identities,
InvariantIds and bounds for Loom, and paired exploration/nondeterminism records for Shuttle.
The canonical runners validate their own inventories and filters. Missing or ignored checks,
zero overall selection, incomplete exploration and nondeterminism failures retain their failure
verdicts. A package with no filter match is allowed within a nonempty cross-package selection.
Its `verdict` reads `running` from the moment the directory exists. It becomes
`complete` only once the whole check passed and the report was written, and `failed` or
`interrupted` otherwise, with a `failure` naming the stage, `prepare`, `instrument`, `run`, `export`
or `finish`, and what went wrong. A collection fails when the recipe ran no executable, when an
executable it ran is gone or was rebuilt before the export, when one wrote no profile, or when a
profile is unreadable or names an executable that is no longer there. Every attempt keeps its
profiles, so a failed one keeps its evidence; remove `target/native-coverage` when the collections
are no longer needed.

Every profile is matched to the executable that wrote it before the export, so a warning from
`llvm-cov` never means a stale profile. When several executables share a function that only some of
them run, such as an inlined dependency function, the copies that never ran can carry a different
hash, and `llvm-cov` warns that they have mismatched data and reads the function from the
executables that ran it.

CI's extra-tests job collects native extras and per-mode primitive conformance, and runs primitive
compile checks independently. The `shuttle` job, which runs only for a pull request labeled
`shuttle`, collects its complete inventory, including random/PCT exploration and paired
nondeterminism checking, and runs schedule replay qualification independently. For a pull
request labeled `turmoil`, the `turmoil` job collects the canonical simulation suite once,
including each fresh-process attempt, and runs failure replay qualification independently.
For a request labeled `loom`, the `loom` job collects the complete Loom inventory, and two
loom-qualification jobs split weakening qualification into shards, because it rebuilds the server
once for each weakening of a server owner. The jobs upload `lcov.info`, `completion.json`,
`executions.jsonl`, `export.log` and model evidence as `coverage-native-extras`,
`coverage-shuttle`, `coverage-loom` and `coverage-turmoil`, whatever the verdict. The
[ordinary workspace report](#the-ordinary-workspace-report) merges ordinary collections only; mode
reports describe modeled or diagnostic execution and retain separate attribution, which the
[full workspace union](#the-full-workspace-union) keeps when it combines them.
`just coverage-shuttle <output> [filter]`,
`just coverage-loom <output> [filter]` and `just coverage-turmoil <output>` copy a successfully
completed canonical report to the
requested path while retaining its attempt evidence. Replay uses the unchanged exact-check
commands and the failure directory under the matching mode build directory.
The benchmark bodies run instrumented, so
their Criterion test mode shows that each body executes and measures nothing. The collector itself,
including real instrumented runs of a fixture crate and its refusal of every incomplete collection,
is tested with:

```bash
just test-native-coverage
```

## The Ordinary Workspace Report

Every ordinary coverage consumer reads one report, `lcov-workspace.info`: patch line coverage, the
CRAP report and the `coverage-merged` artifact. `just coverage-merge` builds it from the
ordinary producers' reports, and fails, writing no report, rather than publish an incomplete one.

`just test-coverage` and `just test-scenarios-coverage` export `lcov-workspace.info` from every
profile they collected with `--no-default-ignore-filename-regex`, which keeps every workspace crate
and the test sources the run executed. Each writes the absolute checkout path it ran in to
`coverage-source-root.txt` beside the report. Each is a build recipe followed by a run recipe:
`test-coverage-build` and `test-scenarios-coverage-build` compile every binary under the
instrumentation and into the target directory of `cargo llvm-cov`, and `test-coverage-run` and
`test-scenarios-coverage-run` run them and export the report, compiling nothing. CI runs the two as
separate steps and publishes the job's cache between them. `just coverage-report-workspace` writes
the same pair from the profiles a focused recipe collected. CI uploads the pair as the
`coverage-tests` and `coverage-scenarios` artifacts, the second with the Python paced drivers'
report, and the extra-tests job uploads its native collections and Python tooling reports as
`coverage-native-extras`. The coverage job then runs:

```bash
just coverage-merge coverage-inputs/tests coverage-inputs/scenarios coverage-inputs/native-extras
```

An input is a producer's artifact directory or one report, with the producer's
`coverage-source-root.txt` in the directory or beside the report. The merge reads the workspace
export, every Python tooling report named `python.lcov`, and every complete ordinary native
collection below `native-coverage`. A collection of another mode describes modeled or diagnostic
execution, so it is never read: the Shuttle, Loom, Turmoil and Deloxide primitive conformance
collections in the native extras artifact are recorded as left out. The reports of the `shuttle`,
`loom`, `turmoil`, `deloxide` and Bolero fuzz jobs are not inputs at all.

The Deloxide lane selections retain their reports, completion records and lane evidence in
`deloxide-<selection>` artifacts. Live Bolero sanitizer campaigns publish each shard's source
coverage as `coverage-bolero-fuzz-<shard>` and the combined campaign record and report as
`coverage-bolero-fuzz`. These artifacts stay outside the ordinary coverage job's downloads.
Discovery, replay and deliberate qualification failures supply no live campaign counters, and
an incomplete campaign retains its failure instead of claiming complete coverage.

Every record is mapped onto the repository. The merge strips the producer's checkout root and
resolves `..` segments lexically, so the record of a `#[path]` module names the file it includes,
and it keeps a record only when its file is tracked at the tested revision, test sources included.
It drops the rest and counts the dropped files by class: dependencies below `~/.cargo/registry`,
`~/.cargo/git` and `/kache/.cargo/registry`, the standard library below `/rustc/<commit>` or a
local rustup toolchain, files generated below the checkout's Cargo target directory, such as the
FlatBuffers `OUT_DIR` sources, untracked files, and any other path outside the checkout. The counters of every report that
measured a file are summed. The report holds line and function counters. Branch records are left
out: the Rust producers export none, and coverage.py names Python branches in words that LCOV readers
such as `cargo crap` refuse.

A counter at or above 2^63 counts no executions. llvm-cov derives some line and function counts as
differences of counters, and an instrumented executable updates its counters without atomics, so
threads that run the same code concurrently can lose increments and leave such a difference below
zero. The export writes it as its unsigned wrap, such as 18,446,744,073,709,551,615 for −1. The merge
leaves every such counter out of the sum and lists it in the merge record under its report, with its
file and its line or function. The merged report never invents a count for it. A line that only
wrapped counters count is not measured, rather than measured as zero. A function that only a wrapped
counter counts counts no hits. Summing a wrapped counter would publish a count above the 64-bit
range, which the CRAP analysis cannot read, and the same race appears in some runs and not in
others.

The merge fails with a message naming what to fix when:

- an input is missing, or holds neither a workspace export nor a complete ordinary collection;
- an input has no root metadata;
- none of a report's records lies below its producer's checkout root;
- a report contributes no record of a tracked file;
- a native report has no complete ordinary completion record beside it, or one producer has two
  collections;
- a workspace member that `cargo metadata` lists has no Rust record in the merged report.

Either way it writes its merge record, `target/coverage-merge/record.json`: the verdict and the
tested revision, every input with its checkout root, each report it read with its record counts, its
dropped records by class and its counters that wrapped below zero, the collections it left out, the
dropped files by class, the number of wrapped counters left out, and the workspace members the
report measures. CI retains the report and its record as the
`coverage-merged` artifact.

Without inputs, the merge rewrites in place the `lcov-workspace.info` that `just test-coverage`
wrote, into the shape CI publishes, and the CRAP report and patch line coverage read it:

```bash
just test-coverage
just coverage-merge
just check-coverage
just coverage-patch origin/main
```

`just test-coverage-merge` exercises the mapping, every integrity failure and the CI contract, and
`just coverage-merge-runner` exports the merge's measured Python coverage for CI.

## Mandatory Complexity Count

`functions_over_complexity_30` is the number of functions the pinned cargo-crap analyzer scores
above cyclomatic complexity 30. Exactly 30 is outside the count; 31 is inside. The analyzer's
version is declared once in `scripts/cargo_crap.py` and built through kache by
`just install-cargo-crap`. The ratchet and advisory CRAP report use that same build. Another
version, an incomplete source walk, a parsing warning, or an analysis with no measured function
fails the count; it never reads as zero. The [source eligibility and analyzer limits](./architecture-enforcement.md#function-complexity)
include inactive configurations and explain what syntax the analyzer does not expand or score.

```bash
just ratchet
just ratchet --show functions_over_complexity_30
```

The baseline stores a count, not a list of approved functions. A new function above 30 raises it;
raising the complexity of one already above 30 does not. Decomposing an over-complex function is
the repair, with `just ratchet --update` recording a lower count. A source contract or numeric
allowance never substitutes for complete analysis.

CRAP is unsuitable as a deterministic gate because measured line hits vary across executions.
The epic's comparison of two runs on one unchanged base found 369 lines changing hit status in
79 files, 87 functions changing CRAP, and six functions crossing 30. A second unchanged-base pair
changed 384 lines in 83 files. These are observations of those runs, not a promised noise bound.
At full coverage CRAP equals cyclomatic complexity, so counting complexity above 30 captures the
coverage-independent part of that threshold without making incidental execution counters a gate.
The retained run identities and comparison evidence belong to the Coverage Integrity task.

## Advisory CRAP Report

`just check-coverage` publishes an advisory CRAP report over the merged ordinary workspace report.
It runs the pinned cargo-crap, the version `scripts/cargo_crap.py` declares for the complexity count
too, over the whole repository root. The root package's directory holds every workspace member,
and the Cargo projects nested in it, such as the lint workspace, are walked as well, because some
ordinary producers measure them. No default exclusion applies, so test, bench and example sources
are scored like any other. The analyzer is handed each record of the report under its absolute
path, which binds the record to its own file. A relative path would also bind every other file
whose path ends with it, such as another crate's `src/lib.rs`, to that record's coverage.

A function's CRAP score is `complexity² × (1 − coverage)³ + complexity`, from its cyclomatic
complexity and the share of its executable lines the report covers, so at full coverage it equals
the complexity. The report lists every function that scores above 30. A function in a file that
the report holds no record of is unmeasured: mode-only sources such as primitive adapters and
Shuttle checks, build scripts, benches, the lint workspace's fixtures and the WASM guests. The
report lists unmeasured functions with their complexity and never scores them as uncovered. A
report file in which the analyzer scores no function, such as a file of `#[test]` functions, is
listed as read without a scored function.

No CRAP score fails the check. Scores move from run to run with the coverage the suites happen to
record, and the mandatory complexity verdict is the ratchet count above. The check fails only when
the analysis is vacuous, that is when it cannot describe the merged report:

- the merge record is missing, did not complete, or names another revision than the checkout's;
- a record of the report names a source the checkout does not track, such as a path below another
  checkout's root;
- the analyzer's walk cannot read a Rust file the report holds: a hidden path, a symbolic link or
  a file Git ignores;
- the analyzer measured functions in a report file without binding its record to them;
- the analysis matched no report file.

Whatever the verdict, it writes `target/crap-report/crap.json` and `target/crap-report/crap.md`.
The JSON report holds the verdict and its cause, the analysed revision, the scope counts, every
measured function with its complexity, coverage and score, and every unmeasured function. The
Markdown report lists the functions above 30, the unmeasured files and the files without a scored
function. CI's coverage job runs the check after the merge and retains both reports in the
`coverage-merged` artifact. The patch line coverage comment carries the CRAP section: the functions
above 30 in the files the pull request changes, the project's total above 30 and the unmeasured
count, or the cause of a vacuous analysis.

The reports name the analysed report by its file name, so a retained `coverage-merged` artifact
reproduces its reports byte for byte without the coverage binaries, in a checkout of the revision
its merge record names:

```bash
just check-coverage <artifact>/lcov-workspace.info <artifact>/target/coverage-merge/record.json
```

`just test-crap-report` exercises the analysis, every vacuity failure and the CI contract, and
`just coverage-crap-report-runner` exports the report's measured Python coverage for CI.

## Patch Line Coverage

CI runs `just coverage-patch` against the merged ordinary workspace report and updates one PR
comment with patch line coverage, project line coverage, per-file totals and uncovered added line
numbers. The comment names the merge base and tested commit and links to the workflow artifacts.
Patch coverage is advisory: no percentage is a build gate, and a missing report or a failed comment
publication does not fail CI. The complexity verdict is the `just ratchet` count of functions
above cyclomatic complexity 30, which reads no coverage. With `--crap target/crap-report/crap.json`,
as CI passes it, the comment also carries the section of the [CRAP report](#advisory-crap-report) for the
files the pull request changes, including the files it only shortens.

The label-enabled Turmoil job publishes a separate **Turmoil patch coverage** comment using
`--scope turmoil` after its canonical instrumented suite and independent replay qualification
pass. The scope selects a distinct marker, so the two jobs update their own comments regardless
of completion order. This report describes only the current Turmoil build's product sources;
it does not merge modeled counters into the ordinary report or its CRAP report. Failed
collection keeps its verdict and diagnostics without publishing a successful coverage report.

Run the same calculation manually after collecting coverage for the committed source revision:

```bash
just coverage-patch origin/main lcov-workspace.info
just coverage-patch origin/main lcov-workspace.info --pr 123 --repo nervix-io/nervix
just coverage-turmoil target/turmoil.lcov
just coverage-patch origin/main target/turmoil.lcov --scope turmoil --output target/turmoil-patch-coverage.md
```

The first command prints the Markdown report and writes `target/patch-coverage.md`; the second also
creates or updates the authenticated publisher's marked PR comment through `gh`. Additional
`--report <path>` arguments union line hits from multiple LCOV reports. Coverage must describe the
tested commit's line coordinates. The reporter maps sources onto tracked files as
`just coverage-merge` does, so a `..` segment names the file it reaches, and the merged report,
already relative to the repository, needs no checkout root. CI's reporter reads that report, so the
checkout paths of the producers' runners are resolved once, by the merge. A
focused local report, such as the Backup and Visual Create coverage recipes write, describes the
local checkout and needs none either. For reports collected in another checkout, use
`--source-root <original-checkout-path>`, or `--source-root-file` with the producer's
`coverage-source-root.txt`, to remap absolute source paths explicitly. Repeat either when reports
come from multiple source roots.

The command compares the base's merge base to `HEAD`, or to `--head <commit>`. Only added lines with
LCOV `DA` counters enter its denominator; repeated records count each file/line once, and a hit in
any report covers the line. Deleted lines, unchanged lines and non-executable additions do not
enter it. A changed file absent from LCOV is shown as unavailable. An empty measured patch has no
percentage. Project coverage describes the tracked files present in the supplied reports; no
baseline project report is assumed.

CI compares the PR base to the tested synthetic merge commit, so the diff uses the same line
coordinates as the collected coverage. It passes the PR head separately with `--pr-head` and checks
that the PR is still open at that head before publication. Fork PR tokens may lack comment access;
the local report, workflow summary and artifact remain available. `just test-patch-coverage`
exercises accounting and publication, and `just coverage-patch-runner` exports the reporter's
measured Python coverage for CI.

## The Full Workspace Union

The full workspace union combines every coverage producer one CI run selects into one line-coverage
report: the ordinary workspace report, the modeled and diagnostic native collections, and the live
fuzz campaigns a pull request carrying `fuzz` requests. It reads the ordinary report unchanged. The
[CRAP report](#advisory-crap-report) and patch line coverage keep reading the ordinary report
alone, so modeled execution never changes either of them.

The completion check's GitHub Actions identity is `coverage-full` in the `Check` workflow. Its
verdict is mandatory: a missing, failed, cancelled or timed-out selected producer cannot qualify a
release. `just release-evidence <pull-request>` requires the successful union on the revision,
alongside the ordinary report's integrity checks and each method's independent verdict. Its
percentages remain advisory. A verified prose-only change selects no suites and skips the union;
that skip supplies no coverage or release-qualification evidence.

Branch protection is configured outside source control, and nothing in the repository installs or
verifies it. A qualification records the live default-branch ruleset on its ClickUp task: its
identity, enforcement, branch scope and required status checks, with the checked revision. A
source change is never evidence that such a setting is active. A ruleset that requires the
completion check names `coverage-full` from the GitHub Actions app, and that name stays stable
across runner, producer and artifact changes.

Producer jobs retain sampled CPU utilization, steal time, used memory and swap with their build
and execution evidence. Tests, scenarios and fuzz shards record their load directly; native
extras, Shuttle, Loom, Turmoil, diagnostic evidence and workload lanes, client conformance and the
independent Loom weakening shards use `.github/actions/runner-load` around their workloads. Its
finish phase runs after failure too, waits at most ten seconds for the recorder, and requires its
summary before uploading `runner-load-<job>`; diagnostic selections and weakening shards append
their matrix identity so their artifacts cannot overwrite one another. The workflow contract tests
of `just test-coverage-full` hold every producer job of the ordinary report and of the union, and
both qualification jobs, to that recording. These are measurements of the runner, including build
and service memory, rather than individual-process peak RSS. Workflow step times and collection
records delimit producer work. Instrumented timings measure collection cost; runtime performance
claims still require ordinary builds on the same host. Recording the load of a weakening or replay
does not make that altered-source execution coverage evidence.

The `coverage-full` job waits for every producer job, including one that failed or was skipped. It
records the run's selection before downloading artifacts, then rebuilds nothing and combines:

```bash
just coverage-full-select --event "$EVENT" --labels "$LABELS" \
  --tests-required "$TESTS_REQUIRED" --bolero-required "$BOLERO_REQUIRED" --jobs "$JOBS"
just coverage-full coverage-full-inputs/*
```

The selection is the run's event and pull request label snapshot, whether the tested diff required
the suites and the Bolero call, the run and its attempt, the tested revision, and every producer
job's result. It decides which producers the union requires:

| Producer | Job and artifact | Required |
| --- | --- | --- |
| The ordinary workspace report | `coverage`, `coverage-merged` | Whenever the tested diff requires the suites |
| `test-primitives-shuttle`, `test-primitives-loom`, `test-primitives-turmoil` and `test-primitives-deloxide` | `extra-tests`, `coverage-native-extras` | Whenever the tested diff requires the suites |
| `test-deadlock-evidence-order` and `test-deadlock-report` | `diagnostic-evidence`, `diagnostic-evidence` | Whenever the tested diff requires the suites |
| `test-shuttle` | `shuttle`, `coverage-shuttle` | For a pull request labeled `shuttle` |
| `test-loom` | `loom`, `coverage-loom` | For a pull request labeled `loom` |
| `test-turmoil` | `turmoil`, `coverage-turmoil` | For a pull request labeled `turmoil` |
| `test-deloxide`, `test-deloxide-order`, `test-deloxide-stress` and `test-deloxide-stress-restore` | `deloxide`, `deloxide-<selection>` | For a pull request labeled `deloxide` |
| `bolero-fuzz`, the combined live campaign | `bolero`, `coverage-bolero-fuzz` | For a pull request labeled `fuzz` |
| The Bolero call's selection record | `bolero`, `bolero-selection` | Whenever the run calls Bolero |

Every required producer job must have succeeded, and every other one must have been skipped. The
Bolero selection record must show that the randomized properties passed exactly when
`bolero-random` requested them and that the sanitizer campaigns ran exactly when `fuzz` requested
them. Without the label, the recorded deliberate skip of that mode is valid and supplies no report.
A job that a later attempt of the run did not rerun keeps its earlier attempt's verdict and
artifacts, so a collection of any attempt up to the current one is the run's.

The event and tested source decide eligibility together:

| Selection | Randomized properties | Live sanitizer coverage | Full union |
| --- | --- | --- | --- |
| Code or test PR | Required with `bolero-random`; deliberately skipped otherwise | Required for the complete tested inventory with `fuzz`; deliberately skipped otherwise, with no fuzz report | Required, with the label-selected producers |
| Verified prose-only PR carrying `bolero-random` or `fuzz` | Required with `bolero-random`; deliberately skipped otherwise | Required for the complete tested inventory with `fuzz`; deliberately skipped otherwise | Skipped; each requested run keeps its own report and verdict |
| Verified prose-only PR carrying neither label | Entire Bolero call excluded | No campaign or report | Skipped; no union evidence |
| Manual Bolero workflow | Required | Deliberately skipped | No Check union is launched by this standalone workflow |

Check runs on PR open, reopen, push and label changes. Each event records its own label snapshot;
adding or removing a label changes the required producers for that event. `no-docker` never
authorizes excluding code or tests. The [documentation-only selection contract](./property-testing-and-fuzzing.md#documentation-only-selection)
owns the exact allowed paths and tested-merge identity. A skip records eligibility, never successful
execution of a property, model, scenario or sanitizer campaign.

A failed suite-selection job runs and fails the completion check rather than turning it into a
skip. Missing downloads reach the union as missing inputs, with the selection retained, so its
failure record and summary identify the producer, job and artifact to repair. A download failure
also leaves its failing job verdict intact; attempting the union afterwards cannot erase it.

Each input is validated before its lines join the union:

- A native collection is complete; at the tested revision, with an unmodified working tree; from
  this run; and in its producer's declared mode, attempt directory and source policy. A Shuttle or
  Loom collection selected the canonical inventory unfiltered and its `models.json` is complete. A
  Deloxide lane collection's `lane.json` records a complete run.
- The ordinary report's merge record is complete at the tested revision, and so is every ordinary
  collection it merged. The native extras artifact holds those collections too, and they count once.
- The fuzz campaign combines this run's shards, tested this revision for this event and label
  snapshot, and ran every target of the tested inventory once.
- Each report's sources map onto the files the tested revision tracks exactly as the ordinary merge
  maps them. A line beyond the end of its file shows that a report describes another revision.

The union fails when an input is missing, stale, from another run, revision or attempt, corrupt,
failed or incomplete; when two different inputs name one producer; and when an input holds a
producer the selection does not enable, such as a fuzz report without `fuzz`. It then writes its
record and summary, naming every problem, and no report. An identical input named twice counts
once, so neither the order of the inputs nor a repeated artifact changes the result.

The union is line coverage. Its executable lines are the union of every contribution's lines, a line
is covered when any contribution executed it, and its counter is the sum. A counter that wrapped
below zero counts no executions, as in the [ordinary merge](#the-ordinary-workspace-report): the
union leaves it out of the sum and lists it under its contribution in the record, and a line only
such counters count is not measured. Any collection can hold such counters when its instrumented
executables run the same code on concurrent threads. Function and branch
identities depend on the toolchain and configuration each producer built with, so the union carries
none; each producer's own report keeps them. Native collections leave out harness code below
`tests`, `examples` and `benches` directories, so only the ordinary report measures those files.
The job writes `target/coverage-full/` and retains it as the `coverage-full` artifact:

| Entry | Content |
| --- | --- |
| `lcov-full.info` | The union, in source and line order |
| `beyond-ordinary.info` | One `TN:` section per contribution, with the lines it covered that the ordinary report did not cover |
| `record.json` | The verdict, the selection, every required producer and where it was found, each contribution's identity, line attribution and counters that wrapped below zero, the totals per mode, the workspace members and the source policy |
| `summary.md` | The contributor breakdown, which CI also publishes as the job summary |
| `selection.json` | The recorded selection |

No collector measures the browser console's WebAssembly build, WASM guest code, the client
conformance probes in other languages, or the product processes the chaos suites kill.
[Independent verification](#verification-outside-collected-counters) remains required in its own
selected jobs even when it contributes no counters to the union.

The union reads reports and records only. Repeating it over a run's retained artifacts, at the
tested revision the selection names, gives the same lines without binaries or a compiler:

```bash
gh run download <run> --name coverage-full --dir retained
gh run download <run> --dir inputs --name coverage-merged --name coverage-native-extras \
  --name diagnostic-evidence --name bolero-selection
just coverage-full --selection retained/selection.json inputs/*
```

Download the artifacts of the run's label-enabled producers into `inputs` as well. Outside CI,
`just coverage-full-select` records a local selection whose `--labels` name the label-enabled
producers collected locally. It requires collections of the checked-out revision from an
unmodified working tree: name the `lcov-workspace.info` that `just coverage-merge` wrote and each
attempt directory below `target/native-coverage`. `just test-coverage-full` exercises the
selection, every validation, the attribution and the CI contract, and `just coverage-full-runner`
exports the union's measured Python coverage for CI.

## Verification Outside Collected Counters

Coverage completion and a method's verification verdict answer different questions. The following
checks retain their own verdicts outside the native source-line totals; their absence from a report
does not authorize skipping them when their job or owning change requires them.

| Check | Evidence and boundary |
| --- | --- |
| Product API doctests and compile/browser probes | `just test-capability-docs`, `just test-typed-ratchet-product-docs` and `just test-primitives-compile` check current supported calls beside `compile_fail` examples in their declared configurations. Rejected programs supply no executable counters. Positive examples establish the intended imports and signatures; neither proves runtime or concurrency behavior. |
| Compiler-cache qualification | `just qualify-typed-ratchet-cache` checks current source, compiler and doctest identity across fresh, warm, interrupted and two-worktree runs with kache retained. These probe executions do not join the instrumented `test-typed-ratchet` producer. |
| Deloxide supervision qualification | `just test-deloxide-qualification <selection>` proves that diagnostic-process failures fail the lane, preserve evidence and leave no process behind. It runs outside the lane collector; the diagnostic workloads' complete `lane.json` remains separately required for export. |
| Backup cut cleanup supervision | `just test-backup-cut-cleanup` requires deliberately failed public bodies to retain their failure verdict while the real after hook releases lease and capture gates, finishes retained public backup tasks and stops every node on one and three nodes, sharing one unchanged cleanup deadline. This independent supervision verdict supplies no ordinary coverage report; gate-owner and harness-liveness unit tests retain ordinary coverage. |
| Replay and deliberate failure qualification | Shuttle schedule replay, Turmoil failure replay and Bolero failure/minimization checks qualify reproduction and supervision. Saved inputs or reproduced failures do not establish a completed current campaign or exploration. |
| Loom weakening and mutation testing | Deliberately changed source proves that an oracle detects the change. Those counters cannot describe the unchanged source revision. |
| Miri | Its interpreter verdict remains independent of native LLVM source coverage. |

Source-contract compiler fixtures, including their paired fixture doctests, are part of the
instrumented `test-typed-ratchet` recipe. They are distinct from its uninstrumented product API
doctest finish step and the separate cache qualification. The [compiler contract](./data-plane-concurrency.md#resolved-architecture-boundaries)
owns what those fixtures diagnose. Native host counters never imply browser, guest or
other-language execution, and Chaos retains its real-process verdict against immutable images.

## Adding Or Changing A Producer

An existing ordinary test, registered property, model or scenario stays with its canonical runner;
adding a check there does not create a second producer. A new native execution outside those
runners joins `scripts/native_coverage.py` with its prepare, instrumented and finish recipes,
declared mode and compiler. Classify compile-only, altered-source, replay and supervision checks
explicitly as independent verdicts instead of inventing counters for them.

When changing a producer, update its ordinary merge inputs or full-union requirement table and
workflow downloads in the same change, including event/label eligibility, artifact identity and
failure behavior. Keep one inventory for execution and collection: a focused filter can aid local
diagnosis but cannot qualify the full inventory. Changes to a producer job or matrix also update
`tests/release-evidence.toml` and the workflow contract tests that name it.

Run the affected canonical command and its command/artifact tests. Retain the exact revision,
selection, completion and replay records on the owning ClickUp task, including failures and the
limits of reused evidence. A past complete report does not qualify targets added afterwards.
Keep build/runtime, runner load and artifact-size measurements with that task; instrumented cost
is collection overhead, while performance claims require ordinary builds on the same host.

## Limits And Failure Recovery

Workspace-member presence means that each member contributes a Rust source record. It does not
mean that every feature, branch or function ran. A line hit establishes execution, not a complete
concurrency exploration or a correctness proof. The native collector validates profile/object
identity and completion; the ordinary merge validates paths, tracked sources, collection verdicts
and member presence. It does not compare every producer's source contents or feature selection.
CI's common tested checkout and the retained producer records supply that execution context.
Local append recipes require unchanged source; clean their profiles before measuring another
revision. The CRAP report's revision check does not make mismatched counters current.

| Diagnostic | Recovery |
| --- | --- |
| An input `has no root metadata` | Retain its producer's `coverage-source-root.txt` beside the report or in the artifact directory. Do not substitute the consumer runner's checkout root. |
| `none of its … records lies below … the checkout path its producer recorded` | Check the report/root pair and the producing checkout. Use the original root when remapping a retained focused report. |
| A producer `has more than one ordinary collection` | Select the attempt of the tested run. Preserve failed attempts as evidence outside the merge input. |
| `the merged report has no record of the workspace members …` | Restore the missing producer or its workspace export; a focused package report cannot establish whole-workspace completeness. |
| `Counters that wrapped below zero, left out of the sum: N` | Nothing in the report to repair: concurrent threads of an instrumented executable lost counter increments. The merge record names each counter. A line it leaves unmeasured is counted by no other report; collect owner evidence for that line if it matters. |
| A CRAP merge record names another revision | Reproduce in the revision the record identifies, or collect and merge current-source coverage. Editing the record's revision does not repair it. |
| A changed file is `unavailable` in patch coverage | Its executable lines were not measured by the supplied reports. Collect the applicable owner/scenario evidence; missing counters are not a measured zero. |

Keep the failed merge record, collector completion records and export diagnostics with the
owning task or CI artifacts. A report or comment failure cannot turn a failed mandatory execution
into successful evidence. Verification methods retain their own mandatory tests and qualification;
an advisory workspace percentage never substitutes for those verdicts.
