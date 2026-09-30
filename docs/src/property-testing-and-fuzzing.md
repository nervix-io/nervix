# Property Testing And Fuzzing

Bolero is Nervix's default property-test engine for representation correctness. Each property is
an ordinary Rust test beside its production owner and a registered custom libFuzzer target. The
generator, production path and assertion are the same in both modes.
`tests/bolero-targets.toml` is the single inventory. Its IDs remain stable while a
generator-domain version advances when the current representable shape changes.

## Property Contract

A lossless encode/decode or representation conversion asserts equality with the complete original
value over bounded, valid current inputs. Equality includes every significant field and order,
branch and domain identity, nullability, type and sensitivity. Where `PartialEq` is not
the contract, the property uses an explicit complete oracle. Float bit patterns and Arrow schema,
logical values and validity are examples; physical buffer layout need not match when it is not
public semantics. Canonicalizing, lossy and one-way transformations state and test their actual
contract. Malformed input has separate rejection and boundedness targets, so a round-trip
generator cannot pass by producing mostly invalid values. Reference models, operation sequences,
scalar/SIMD differential checks, encoded-size bounds and segmentation invariance are other useful
Bolero properties.

Generators stay in test code, construct bounded valid values, cover supported variants and
deliberate boundaries, and never reintroduce historical shapes. They may not add test-only product
APIs, row carriers, persistence for volatile state, or inner-layer parser dependencies. A Bolero
property is independent of live services. Public behavior, concurrency and recovery retain their
own Cucumber, Shuttle, Loom, Turmoil and external Chaos evidence.

## Target And Representation Register

| ID | Package and invariant | Domain | Ordinary cases | Input limit |
| --- | --- | --- | ---: | ---: |
| `client-emitter-wire` | `nervix-client-wire` native emitter frame round-trip equality | all current request, reply, refusal and settlement variants with bounded exact schema, window, identity, branch and batch fields, v1 | 256 | 128 bytes |
| `nspl-expression` | `nervix-nspl` expression render/reparse equality | current structured expressions, v1 | 256 | 128 bytes |
| `nspl-model` | `nervix-nspl` canonical Model render/reparse equality | current generated Create Models, including client emitters, v2 | 256 | 128 bytes |
| `nspl-archive-model` | `nervix-nspl` archive document/reparse equality | ordered generated Models, including client emitters, v2 | 64 | 128 bytes |
| `backup-record-manifest` | `nervix-backup` record and manifest encode/decode equality | current domain record and manifest, v1 | 256 | 128 bytes |
| `branch-membership` | `nervix-branch-instances` owner steps against the specified visible-set contract: each claim, admission, eviction, expiry and release publishes exactly the current owner lifetime's membership, and a step that changes none publishes nothing | bounded claim, admit, expire and release sequences over six branch keys, v1 | 256 | 256 bytes |

The inventory also records exact full test names, required features, corpus paths, case timeouts
and each invariant. Its corpus path is Bolero's source-adjacent
`__fuzz__/<test-name>/corpus` directory. Ordinary tests replay those files before
randomized cases; checked-in corpus inputs are regression cases. Each target starts with
current-domain boundary seeds. Generated fuzz corpus and crash files live in retained artifacts
under `target/bolero/runs`. Promote a verified minimized failure to the checked-in corpus
when it remains a meaningful regression for the current domain. Breaking shape changes replace
obsolete seeds.

## Commands And Enforcement

Install the dated sanitizer nightly named in the inventory and the pinned CLI:

```bash
rustup toolchain install nightly-2026-09-17 --profile minimal
just install-cargo-bolero
just help
just fuzz-list
just test-bolero
just test-bolero nspl-model
just fuzz nspl-model 30
just fuzz-all 30
just fuzz-replay nspl-model <saved-input>
just fuzz-reduce nspl-model <saved-input>
```

`just validate-bolero` compares the inventory with all workspace packages declaring
Bolero, scans their property macros, and queries compiled targets through filtered library-test
discovery. It rejects missing, unregistered, duplicate, ignored and zero-selected targets, plus
corpus paths different from Bolero's actual work directory. Discovery executes only
`bolero_` library tests under Bolero's selection mode; it cannot start the server's
scenario harness. It reports discovered, selected, executed and completed counts.
`just validate` and `just validate-ci` include this gate.

`just test-bolero` requires the configured randomized-case count and checked-in corpus
replay for every selected property, checking Bolero's reported input counts. Fuzz runs use real
libFuzzer coverage with AddressSanitizer, bounded input length and per-case timeout. The fuzz
profile enables optimization, debug information, debug assertions and overflow checks. Builds
use the pinned nightly and keep the configured kache wrapper. Modeled execution features are
excluded; Loom, Shuttle and Turmoil remain independent build invocations.

PR CI runs a required ordinary randomized/corpus job and a separate required sanitizer
libFuzzer job. Each target gets 30 seconds of engine time on PRs and five minutes in scheduled or
manual campaigns. Job limits reserve additional time for compilation, artifacts and cleanup. A
cache may seed a campaign but cannot skip a target or replace checked-in regressions. An empty
selection, timeout, engine failure, sanitizer finding or property failure fails the job.

Each fuzz run retains its corpus, crashes, log and metadata with revision, target, domain version,
toolchain, features, flags, result and an exact-input reproduction command. Retain failure
inputs before cleanup. `fuzz-replay` stages saved bytes in the target's actual crash
directory and runs the ordinary assertion with zero randomized cases. `fuzz-reduce`
uses libFuzzer crash minimization and verifies that minimized bytes still fail that assertion.
A random seed reproduces one generated case, not an entire entropy-driven campaign.

## Adding A Target

Put the Bolero test beside its production owner, name it `bolero_...`, bound iterations
and input length, and assert the complete current-value contract. Register the package, full
test name, source, domain version, features, corpus, budgets and invariant in the inventory.
Check in current-domain seeds, run `just validate-bolero`,
`just test-bolero <id>` and `just fuzz <id> 30`, then run
`just validate`. Both CI jobs include the target in the same change. A public product
bug found this way first gets a failing focused reproducer and affected public Cucumber scenario
before its fix.
