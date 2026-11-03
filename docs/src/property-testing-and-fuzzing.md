# Property Testing And Fuzzing

Bolero is Nervix's default property-test engine for representation correctness. Each property is
an ordinary Rust test beside its production owner and a registered custom libFuzzer target. The
generator, production path and assertion are the same in both modes.
`tests/bolero-targets.toml` is the single inventory. Its IDs remain stable while a
generator-domain version advances when the current representable shape changes.
The register below records each target's owner, invariant, generated domain version and bounds.
The invariant describes the equality or reference oracle; the domain names the values or operation
sequences under test. Where a representation coverage map owns more detailed value families, a
target addition or change updates that map and the register with the inventory in the same change.

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

Compile-time API restrictions use paired Rustdoc examples beside the owning API: a `compile_fail`
example for the forbidden call and a compiling example with the current imports and supported
signature. `just test` invokes `just test-capability-docs` for the capability owners;
`just test-primitives-compile` exercises primitive doctests in their declared execution modes.
CI runs the first for a pull request labeled `extra-doctests` and the second for one labeled
`extra-primitives`. An import
that no longer resolves cannot serve as evidence of the intended restriction. Nervix uses no
`trybuild` dependency, harness, fixture source or diagnostic snapshot for these checks. Doctests
prove only the build configuration in which they run; Cargo graph and target probes, compiler
fixtures, public scenarios and concurrency models keep their separate contracts in
[Execution Primitives](./execution-primitives.md#verification-contracts).

These product API checks keep independent verdicts outside native coverage totals. The compiler
rules' paired fixture doctests are different: they run inside the instrumented source-contract
producer, with product API doctests finishing separately. Compiler-cache and Deloxide supervision
qualification also stay outside collection; the [coverage evidence boundary](./coverage-reports.md#verification-outside-collected-counters)
records their scope without treating a passing qualification as a completed campaign or model.

## Target And Representation Register

| ID | Package and invariant | Domain | Ordinary cases | Input limit |
| --- | --- | --- | ---: | ---: |
| `client-emitter-wire` | `nervix-client-wire` native emitter frame round-trip equality | all current request, reply, refusal and settlement variants with bounded exact schema, window, identity, branch and batch fields, v1 | 256 | 128 bytes |
| `client-requests` | `nervix-client-wire` complete client request equality | every current request family, identities, positions, schemas and opaque I/O payloads, v3 | 128 | 2048 bytes |
| `client-replies` | `nervix-client-wire` complete reply equality | every reply family, dispositions, ordered diagnostics and structured command metadata, v3 | 128 | 4096 bytes |
| `client-events` | `nervix-client-wire` complete event equality and retained graph ownership | every unsolicited event, discovery, clocks and opaque graph text, v1 | 128 | 2048 bytes |
| `client-streams` | `nervix-client-wire` complete stream equality and shared chunk ownership | all upload, download and restore roots and outcomes, v3 | 128 | 2048 bytes |
| `client-upload-archives` | `nervix-client-core` complete directory tree and deterministic archive bytes against an independent tar decoder | empty directories, nested and GNU long paths, arbitrary file bytes and block boundaries, v1 | 128 | 4096 bytes |
| `client-reply-transfer` | `nervix-client-wire` exact segmentation bytes, reply equality and rejected append isolation | single frames and ordered transfers, incorrect identities and duplicates, v1 | 128 | 256 bytes |
| `client-row-views` | `nervix-client-wire` complete retained Row views | all scalar cells, nested vectors, fixed arrays, nulls, redaction, branches and float bits, v1 | 128 | 512 bytes |
| `client-arbitrary-frames` | `nervix-client-wire` typed verification and decoding within budgets | arbitrary bytes at all eight frame roots, v1 | 256 | 16385 bytes |
| `client-frame-corruption` | `nervix-client-wire` typed verification and decoding within budgets | current frame truncation, bit, offset, length and size mutations, v1 | 256 | 64 bytes |
| `client-discriminators` | `nervix-client-wire` validate discriminants before accessing union members | all byte-valued request tags, v1 | 256 | 64 bytes |
| `client-arrow-rows` | `nervix-server` Arrow logical values and validity against production Row views | scalar columns, nullable and nested lists, arrays, selected rows, concrete branches with every supported key field type, frame splitting and explicit redaction, v3 | 128 | 512 bytes |
| `client-arrow-selection` | `nervix-server` whole-input typed rejection without partial output | invalid row bounds, order, duplicates and branch alignment, v3 | 128 | 512 bytes |
| `client-binding-rows` | `nervix-client-ffi` complete column values and retained frame ownership | native scalar bits, states, string/bytes offsets, branch keys and nested cells, v3 | 128 | 512 bytes |
| `client-ffi-field-descriptors` | `nervix-client-ffi` field descriptor ABI equality both ways: a host's level-by-level description becomes the complete original field, and the field and level accessors read back its canonical description as the header's `nx_type` values | every scalar inside 0 to 32 `LIST` and `FIXED_LIST` levels of 1 to `INT32_MAX` elements, either nullability and sensitivity, names from the whole name rule spelled in any letter case, v3 | 256 | 2048 bytes |
| `client-ffi-field-descriptors-malformed` | `nervix-client-ffi` invalid-argument refusal that leaves the field list as the accepted calls built it | unknown types, lengths on scalar and `LIST` levels, empty and oversized `FIXED_LIST` levels, a 33rd list, refused names, elements without an open field, fields before the open one's element, an open last field, and reads past a field's last level or past the last field, v3 | 256 | 4096 bytes |
| `client-processor-choice-request` | `nervix-client-wire` processor choice request round-trip equality | current targets with relay context, search, page and identity, v1 | 128 | 32 bytes |
| `plexus-expression` | `nervix-plexus` expression render and reparse equality through a statement and the standalone expression, expression-list and route-construction readers | Plexus expressions of every form, v9 | 256 | 512 bytes |
| `connector-entity-presentation` | `nervix-plexus` declared phrases and ordered inspection rows, canonical reparsing to the complete configured Model | every current ingestor and emitter entity, options, modes, headers, mappings and error policies, v21 | 128 | 2048 bytes |
| `console-ingestor-source-drafts` | `nervix-web-console` filled draft reconstructs the complete transport Model | every current ingestor entity, references, options, modes, headers, quiesce and optional body limits, v10 | 128 | 2048 bytes |
| `models-ingestor-instancing` | `nervix-models` preserves fixed count and partition form through production JSON and archive encoding | every positive 16-bit count, min/max and PerPartition, v1 | 256 | 2 bytes |
| `interconnect-intake-units` | `nervix-interconnect` bounded request codec preserves exact gate selection and ingestor unit, releasing admission charges | entity/unit gates, drain status and describe requests, v1 | 256 | 128 bytes |
| `plexus-model` | `nervix-plexus` canonical `CREATE` render and client and server reparse equality | Plexus Models of every family, client emitters included, v38 | 256 | 2048 bytes |
| `plexus-archive-model` | `nervix-plexus` complete ordered canonical Model equality through export, restore reads, reparse and re-export | cluster/domain archives with multiple ordered documents and every Model family, v39 | 128 | 4096 bytes |
| `plexus-statement` | `nervix-plexus` canonical statement render and client and server reparse equality | Plexus statements of every form, session-only forms included, v37 | 256 | 2048 bytes |
| `plexus-statement-text` | `nervix-plexus` edited statement text is rejected with located diagnostics or reads as canonical statements | edited canonical text, v37 | 256 | 2048 bytes |
| `plexus-emitter-envelopes` | `nervix-plexus` canonical envelope section render and shared envelope grammar reparse equality | envelope sections of every clause kind and declared property, with Plexus expressions and durations, v1 | 256 | 2048 bytes |
| `plexus-expression-text` | `nervix-plexus` edited expression text reads as the same expression, or as none, through a statement and the standalone expression, expression-list and route-construction readers | edited canonical expressions, v7 | 256 | 512 bytes |
| `plexus-format-document` | `nervix-plexus-format` keeps statements and comments and is idempotent | documents with gaps, comments and either line ending, v37 | 256 | 4096 bytes |
| `plexus-format-text` | `nervix-plexus-format` refuses unparseable text or formats it keeping its statements | edited documents, v36 | 256 | 4096 bytes |
| `models-names` | `nervix-models` name text, conversion, JSON, archive and Model-name widening equality | every name type, v11 | 256 | 256 bytes |
| `models-name-validation` | `nervix-models` name parsing matches the name rule; decoders accept only canonical text | arbitrary text, v11 | 256 | 256 bytes |
| `models-timestamps` | `nervix-models` integer, RFC 3339, JSON, archive and chrono equality | every signed Unix nanosecond, v3 | 256 | 64 bytes |
| `models-timestamp-text` | `nervix-models` RFC 3339 text reads as its reference instant or a typed error | generated RFC 3339 text, v3 | 256 | 64 bytes |
| `models-domain-clock` | `nervix-models` period, skew and rate value and bit equality | every period, skew and positive finite rate, v3 | 256 | 64 bytes |
| `models-domain-clock-validation` | `nervix-models` clock text and numbers read in range or fail typed; decoders refuse invalid values | arbitrary text and numbers, v3 | 256 | 64 bytes |
| `models-durations` | `nervix-models` a duration reads back from the text `humantime` writes for it; only one within one of each unit of the longest duration may be refused as too long | every duration, v1 | 256 | 16 bytes |
| `models-duration-text` | `nervix-models` duration text reads as `humantime` reads it or fails typed, and never panics | arbitrary duration text, v1 | 256 | 64 bytes |
| `clickhouse-client-tls-pairing` | `nervix-models` HTTP refuses every TLS file, HTTPS requires CA trust and paired client identity files, and each refusal names the exact offending or missing key independently of entry order | every TLS file-presence combination on HTTP and HTTPS, named, IPv4 and IPv6 hosts, positive timeouts and ports, bounded mount paths and reordered entries, v1 | 256 | 32 bytes |
| `models-header-model-round-trip` | `nervix-models` writes carried through a header model read back per name as the model keeps them, and the model refuses exactly the literal writes that round trip would lose | repeated and case-varied header names under every layout and name rule, v1 | 256 | 64 bytes |
| `models-json-paths` | `nervix-models` path text, JSON and archive equality | paths up to the step limit, v3 | 256 | 1024 bytes |
| `models-json-path-validation` | `nervix-models` path text reaches a fixed point or fails typed; decoders admit the constructor's step counts | arbitrary path text, v3 | 256 | 512 bytes |
| `models-batch-limits` | `nervix-models` message and size limit text, JSON and archive equality | every limit and unit, v3 | 256 | 64 bytes |
| `models-batch-limit-validation` | `nervix-models` limit input reads in range or fails typed; decoders refuse out-of-range limits | arbitrary limits, v3 | 256 | 64 bytes |
| `models-identities` | `nervix-models` execution reference, upload identity, endpoint and pool-bound equality | valid identities, v3 | 256 | 512 bytes |
| `models-identity-validation` | `nervix-models` identity parsers and decoders accept exactly their rule | arbitrary identity text and bounds, v3 | 256 | 512 bytes |
| `models-connector-declarations` | `nervix-models` a generated ingestor and emitter state only what their entity declares: a declared mode with its required option choices, an honored quiesce mode, the declared body form, admitted batching and a client of a declared connector | ingestors and emitters of every connector entity, v26 | 256 | 2048 bytes |
| `client-config-schema` | `nervix-models` complete client configuration-entry equality through JSON and archive, retained schema acceptance idempotent effective defaults preserving authored entries, and idempotent public inspection | every current client schema with its declared keys, delegated driver namespaces, omitted defaults, scheme-restricted and scheme-bound keys, percent-encoded headers and bounded type-valid TCP intervals, string, boolean, integer, URL, host-and-port URL, scheme-restricted driver address, driver URLs with Nervix-owned query options and the keys conditioned on them, choice, file and level-template values, including named secret keys, MQTT keep-alive and publish admission bounds, and the Postgres statement deadline, v27 | 256 | 1024 bytes |
| `models-archived-models` | `nervix-models` Model and statement archive equality, resource-version widening and pinning | vocabulary Models and statements of every family and form, full-width counts included, v35 | 256 | 4096 bytes |
