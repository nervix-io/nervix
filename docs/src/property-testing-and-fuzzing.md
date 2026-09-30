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
| `client-processor-choice-request` | `nervix-client-wire` processor choice request round-trip equality | current targets with relay context, search, page and identity, v1 | 128 | 32 bytes |
| `nspl-expression` | `nervix-nspl` expression render and reparse equality through a statement and the standalone expression, expression-list and route-construction readers | NSPL expressions of every form, v3 | 256 | 512 bytes |
| `nspl-model` | `nervix-nspl` canonical `CREATE` render and client and server reparse equality | NSPL Models of every family, client emitters included, v4 | 256 | 2048 bytes |
| `nspl-archive-model` | `nervix-nspl` archive document and reparse equality | ordered NSPL Models, client emitters included, v4 | 64 | 4096 bytes |
| `nspl-statement` | `nervix-nspl` canonical statement render and client and server reparse equality | NSPL statements of every form, session-only forms included, v2 | 256 | 2048 bytes |
| `nspl-statement-text` | `nervix-nspl` edited statement text is rejected with located diagnostics or reads as canonical statements | edited canonical text, v2 | 256 | 2048 bytes |
| `nspl-expression-text` | `nervix-nspl` edited expression text reads as the same expression, or as none, through a statement and the standalone expression, expression-list and route-construction readers | edited canonical expressions, v1 | 256 | 512 bytes |
| `nspl-format-document` | `nervix-nspl-format` keeps statements and comments and is idempotent | documents with gaps, comments and either line ending, v2 | 256 | 4096 bytes |
| `nspl-format-text` | `nervix-nspl-format` refuses unparseable text or formats it keeping its statements | edited documents, v2 | 256 | 4096 bytes |
| `models-names` | `nervix-models` name text, conversion, JSON, archive and Model-name widening equality | every name type, v1 | 256 | 256 bytes |
| `models-name-validation` | `nervix-models` name parsing matches the name rule; decoders accept only canonical text | arbitrary text, v1 | 256 | 256 bytes |
| `models-timestamps` | `nervix-models` integer, RFC 3339, JSON, archive and chrono equality | every signed Unix nanosecond, v1 | 256 | 64 bytes |
| `models-timestamp-text` | `nervix-models` RFC 3339 text reads as its reference instant or a typed error | generated RFC 3339 text, v1 | 256 | 64 bytes |
| `models-domain-clock` | `nervix-models` period, skew and rate value and bit equality | every period, skew and positive finite rate, v1 | 256 | 64 bytes |
| `models-domain-clock-validation` | `nervix-models` clock text and numbers read in range or fail typed; decoders refuse invalid values | arbitrary text and numbers, v1 | 256 | 64 bytes |
| `models-durations` | `nervix-models` a duration reads back from the text `humantime` writes for it; only one within one of each unit of the longest duration may be refused as too long | every duration, v1 | 256 | 16 bytes |
| `models-duration-text` | `nervix-models` duration text reads as `humantime` reads it or fails typed, and never panics | arbitrary duration text, v1 | 256 | 64 bytes |
| `models-json-paths` | `nervix-models` path text, JSON and archive equality | paths up to the step limit, v1 | 256 | 1024 bytes |
| `models-json-path-validation` | `nervix-models` path text reaches a fixed point or fails typed; decoders admit the constructor's step counts | arbitrary path text, v1 | 256 | 512 bytes |
| `models-batch-limits` | `nervix-models` message and size limit text, JSON and archive equality | every limit and unit, v1 | 256 | 64 bytes |
| `models-batch-limit-validation` | `nervix-models` limit input reads in range or fails typed; decoders refuse out-of-range limits | arbitrary limits, v1 | 256 | 64 bytes |
| `models-identities` | `nervix-models` execution reference, upload identity, endpoint and pool-bound equality | valid identities, v1 | 256 | 512 bytes |
| `models-identity-validation` | `nervix-models` identity parsers and decoders accept exactly their rule | arbitrary identity text and bounds, v1 | 256 | 512 bytes |
| `models-archived-models` | `nervix-models` Model and statement archive equality, resource-version widening and pinning | vocabulary Models and statements of every family and form, v2 | 256 | 4096 bytes |
| `backup-record-manifest` | `nervix-backup` record and manifest encode/decode equality | current domain record and manifest, v1 | 256 | 128 bytes |
| `branch-membership` | `nervix-branch-instances` owner steps against the specified visible-set contract: each claim, admission, eviction, expiry and release publishes exactly the current owner lifetime's membership, and a step that changes none publishes nothing | bounded claim, admit, expire and release sequences over six branch keys, v1 | 256 | 256 bytes |
| `replica-progress` | `nervix-checkpoint-replication` replica reports, offers and announcer steps against the monotonic quorum contract: each replica's progress is the highest revision it reported, a count of replicas holding a revision never falls, and an offered revision keeps exactly one announcer until every assigned replica holds it or the replicated state is gone | bounded report, offer, step, cancel and retire sequences over four replicas and sixteen revisions, v1 | 256 | 256 bytes |

The inventory also records exact full test names, required features, corpus paths, case timeouts
and each invariant. Its corpus path is Bolero's source-adjacent
`__fuzz__/<test-name>/corpus` directory. Ordinary tests replay those files before
randomized cases; checked-in corpus inputs are regression cases. Each target starts with
current-domain boundary seeds. Generated fuzz corpus and crash files live in retained artifacts
under `target/bolero/runs`. Promote a verified minimized failure to the checked-in corpus
when it remains a meaningful regression for the current domain. Breaking shape changes replace
obsolete seeds.

## Generated Domains

`nervix-arbitrary` builds every generated value these properties check. It reads a property's bytes
as a sequence of bounded choices, so the same bytes always build the same value and a saved failure
replays exactly. It is a harness outside the layer order: it depends only on the vocabulary, and
states what NSPL can spell as a rule over values instead of calling the parser, so the vocabulary's
own properties use it without a language dependency. No product code names it.

A generator draws from one of two domains:

- **NSPL** holds the values canonical NSPL spells and reads back as themselves. The language
  properties draw from it.
- **Vocabulary** holds every value the vocabulary types hold, including states NSPL has no
  spelling for: negative, signed-zero and non-finite numeric literals, empty arrays, a `CASE`
  without a `WHEN`, casts to collection types, and the statement and Model states listed under
  [Uncovered Boundaries](#uncovered-boundaries). Stored and archived forms carry these states, so
  the archive properties draw from it.

Both domains reach every Model family, every statement form, and every expression form: integer,
float, boolean, string and null literals with their range ends, arrays, field references in every
scope, every unary and binary operator nested to a bounded depth, casts and `TRY_CAST`, `IF`, `CASE`
with and without an operand, `IN` and `BETWEEN` with their negations, the JSON value and existence
forms, and calls to built-in functions and UDFs. Every form is written in every region a statement
embeds an expression, including a call to a built-in named like a keyword that begins the next
clause, such as `max`, `right` or `replace`, and a field in the `output` or `right` scope. Routes
are generated in every shape a node family allows: transforming and set-only construction,
`INHERIT`, ordered `SET`, `FLUSH EACH` and `FLUSH IMMEDIATE`, and branches declared per route or
node-wide. Names reach both length limits, and counts reach the largest value their field holds.
The language properties also sweep sixteen deterministic byte sequences through every Model
family, every emitter sink and every statement form on every ordinary run, so none is left to the
random cases.

Rejection targets start from valid canonical text and edit up to three characters, inserting,
deleting or replacing delimiters, quotes, comment markers, line endings, digits and non-ASCII
characters. The result is mostly no longer NSPL and sometimes still is, so both the rejection and
the acceptance paths are exercised.

The expression-text target edits a canonical expression up to four times, deleting a character or
inserting a piece or putting one in a character's place. The pieces are whitespace and line
comments, a `.` with and without spaces beside it, exponent letters, digits and a minus, statement
punctuation an expression never reads, and keywords only an expression reserves beside ones it reads
as names. It asserts that every entry point agrees on the result: a subscription's `WHERE`, the
standalone expression reader, a one-element expression list and a route construction's `WHERE` read
it as the same expression, or none of them reads it as one.

## Uncovered Boundaries

The generated domains exclude states whose representation is not lossless. Each exclusion is a
recorded boundary, not a claim:

- Canonical NSPL writes a negative literal, `-0.0` included, as `-` applied to its magnitude, and
  reads it back that way. NaN and the infinities have no spelling, and rendering them fails with a
  typed error. The NSPL domain therefore holds only non-negative finite literals.
- Names inside generated Models and statements are, in both domains, one lower-case ASCII
  identifier that no keyword can match. The name rule admits more, such as `-`, `~`, `.` and a
  leading digit; the name properties cover it on each name type directly.
- `DROP` has no form for branches, generators, hash maps, signaling protocols, WASM processors or
  window processors, an HTTP emitter has no `BATCH` clause, and a correlator's filter has no
  spelling. Only the vocabulary domain generates those states, for the archive properties.
- The archived form of a `usize` count, such as a relay's `CAPACITY`, is 32 bits wide and
  truncates a larger count without an error. The vocabulary domain draws those counts only from the
  range the archive keeps.
- A time rate's JSON form is not asserted: `serde_json` reads a float without correct rounding, so
  it may land one unit in the last place away. Its text and archived forms keep every bit.

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
`bolero_` tests of the registered library and integration-test targets under Bolero's selection
mode; it cannot start the server's scenario harness. It reports discovered, selected, executed and
completed counts.
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
