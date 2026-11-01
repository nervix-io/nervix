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
| `client-requests` | `nervix-client-wire` complete client request equality | every current request family, identities, positions, schemas and opaque I/O payloads, v2 | 128 | 2048 bytes |
| `client-replies` | `nervix-client-wire` complete reply equality | every reply family, dispositions, ordered diagnostics and structured command metadata, v2 | 128 | 4096 bytes |
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
| `plexus-expression` | `nervix-plexus` expression render and reparse equality through a statement and the standalone expression, expression-list and route-construction readers | Plexus expressions of every form, v8 | 256 | 512 bytes |
| `connector-entity-presentation` | `nervix-plexus` declared phrases and ordered inspection rows, canonical reparsing to the complete configured Model | every current ingestor and emitter entity, options, modes, headers, mappings and error policies, v13 | 128 | 2048 bytes |
| `console-ingestor-source-drafts` | `nervix-web-console` filled draft reconstructs the complete transport Model | every current ingestor entity, references, options, modes, headers, quiesce and optional body limits, v5 | 128 | 2048 bytes |
| `plexus-model` | `nervix-plexus` canonical `CREATE` render and client and server reparse equality | Plexus Models of every family, client emitters included, v28 | 256 | 2048 bytes |
| `plexus-archive-model` | `nervix-plexus` complete ordered canonical Model equality through export, restore reads, reparse and re-export | cluster/domain archives with multiple ordered documents and every Model family, v29 | 128 | 4096 bytes |
| `plexus-statement` | `nervix-plexus` canonical statement render and client and server reparse equality | Plexus statements of every form, session-only forms included, v27 | 256 | 2048 bytes |
| `plexus-statement-text` | `nervix-plexus` edited statement text is rejected with located diagnostics or reads as canonical statements | edited canonical text, v27 | 256 | 2048 bytes |
| `plexus-emitter-envelopes` | `nervix-plexus` canonical envelope section render and shared envelope grammar reparse equality | envelope sections of every clause kind and declared property, with Plexus expressions and durations, v1 | 256 | 2048 bytes |
| `plexus-expression-text` | `nervix-plexus` edited expression text reads as the same expression, or as none, through a statement and the standalone expression, expression-list and route-construction readers | edited canonical expressions, v6 | 256 | 512 bytes |
| `plexus-format-document` | `nervix-plexus-format` keeps statements and comments and is idempotent | documents with gaps, comments and either line ending, v27 | 256 | 4096 bytes |
| `plexus-format-text` | `nervix-plexus-format` refuses unparseable text or formats it keeping its statements | edited documents, v26 | 256 | 4096 bytes |
| `models-names` | `nervix-models` name text, conversion, JSON, archive and Model-name widening equality | every name type, v8 | 256 | 256 bytes |
| `models-name-validation` | `nervix-models` name parsing matches the name rule; decoders accept only canonical text | arbitrary text, v8 | 256 | 256 bytes |
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
| `models-connector-declarations` | `nervix-models` a generated ingestor and emitter state only what their entity declares: a declared mode with its required option choices, an honored quiesce mode, the declared body form, admitted batching and a client of a declared connector | ingestors and emitters of every connector entity, v18 | 256 | 2048 bytes |
| `client-config-schema` | `nervix-models` complete client configuration-entry equality through JSON and archive, retained schema acceptance idempotent effective defaults preserving authored entries, and idempotent public inspection | every current client schema with its declared keys, delegated driver namespaces, omitted defaults, scheme-restricted and scheme-bound keys, percent-encoded headers and bounded type-valid TCP intervals, string, boolean, integer, URL, host-and-port URL, scheme-restricted driver address, driver URLs with Nervix-owned query options and the keys conditioned on them, choice, file and level-template values, v22 | 256 | 1024 bytes |
| `models-archived-models` | `nervix-models` Model and statement archive equality, resource-version widening and pinning | vocabulary Models and statements of every family and form, full-width counts included, v27 | 256 | 4096 bytes |
| `models-archived-counts` | `nervix-models` count-bearing Model, statement, transaction identity and WASM inspection archive equality | zero where permitted, `u32::MAX + 1`, `u64::MAX` and generated native counts, v3 | 256 | 16 bytes |
| `scheduled-units` | `nervix-models` complete domain schedule equality through archive encoding | current Single, Partition and Instance unit placements with generated primary and assigned nodes, v1 | 256 | 128 bytes |
| `consensus-archived-counts` | `nervix-consensus` complete record equality through the production bounded storage codec | transaction commands, limits, positions, progress, failures, outcomes, plan and report headers with boundary and generated counts, v1 | 256 | 16 bytes |
| `registry-archived-models` | `nervix-server` complete Model equality through the registry's sealed storage codec | every vocabulary Model family with pinned resource versions and full-width counts, v22 | 256 | 4096 bytes |
| `execution-revision-units` | `nervix-server` a revision preserves every unit placement and fingerprint, and reassigns the moved unit alone | current Single, Partition and Instance unit placements and a generated moved unit, v1 | 256 | 128 bytes |
| `runtime-window-archived-counts` | `nervix-server` histogram delayed-removal archive equality | current removals with arbitrary expiry and boundary and generated bucket indices, v3 | 256 | 32 bytes |
| `models-iceberg-declarations` | `nervix-models` complete namespace equality through JSON and archive, preserved level spelling and backend/connector-kind conversion equality | case-preserving namespaces of one to 128 bytes with nonempty dot-separated levels and every Iceberg storage backend, v1 | 256 | 128 bytes |
| `models-emitter-envelopes` | `nervix-models` complete emitter envelope equality through JSON and archive | envelopes of every clause kind, durations, expressions and properties of any name in written order, v1 | 256 | 1024 bytes |
| `backup-record-manifest` | `nervix-backup` All current archive records and manifests preserve complete users, lifecycle, catalog, cut metadata, ordered section identities and native-width values through their owning codecs. | all eleven current record kinds, lifecycle/catalog variants, every cut, full-width counters and ordered metadata, absent or present Kafka applied-start generation, v6 | 256 | 4096 bytes |
| `backup-runtime-state-records` | `nervix-backup` Current WASM and materialized descriptors, typed materialized identities, Kafka offsets with their optional applied-start generation, and branch lifecycle records decode from their own encoding to complete equal values. | guest/materialized descriptors and identities, empty/multi-partition offsets and ordered branch lifecycle with bit-exact nested fields, absent or present Kafka applied-start generation, v5 | 256 | 4096 bytes |
| `backup-materialized-identities` | `nervix-backup` complete materialized descriptors and ordered scalar identity group equality | empty and one-to-four-group generations, unbranched and branched identities, nested typed fields, float bits and per-row watermarks, v5 | 256 | 2048 bytes |
| `backup-materialized-columns` | `nervix-server` exact-schema Arrow/native generation fidelity and stored checkpoint re-export | nullable, sensitive, nested and byte columns; complete headers, ordered identities and original stored Arrow bytes, v4 | 128 | 4096 bytes |
| `backup-branch-state-descriptors` | `nervix-backup` complete deduplicator and window descriptor equality | empty, unbranched and branched keyspaces and windows, model digests, row sequences and watermarks, group counts and delayed histogram removals, v2 | 256 | 4096 bytes |
| `backup-deduplicator-keys` | `nervix-server` typed Arrow key group and restored keyspace fidelity, and the streamed checkpoint byte-identical to the runtime's encoding of the same keys | every `DEDUPLICATE ON` key type with nulls, narrow integers, arbitrary float bit patterns, text, bytes, datetimes and list keys, repeated keys and bounded groups, v1 | 128 | 2048 bytes |
| `backup-window-groups` | `nervix-server` window input and argument group and native checkpoint fidelity with equal aggregates | up to 192 admissions and interleaved steps over nullable integer, string and boolean arguments, concrete multi-type keys with bit-exact finite floats and fixed-offset datetimes, every exact aggregate family and a delayed histogram percentile, bounded groups sealed whole or split under generated identity limits, one-row groups over windows of up to 48 rows, v4 | 64 | 1024 bytes |
| `backup-complete-archives` | `nervix-backup` Every promised current record, ordered section, length, digest and raw payload, including deduplicator key and window row and argument groups, survives export, extraction, restore reads and deterministic re-export under cluster/domain and resource/state options. | cluster/domain scopes, resources included/omitted, every cut and supported state kind, empty/unbranched/multi-group materialized generations with valid Arrow columns, deduplicator key groups and window row and argument groups, synthetic raw payloads at tar block boundaries and long paths, absent or present Kafka applied-start generation, v5 | 256 | 4096 bytes |
| `backup-malformed-records` | `nervix-backup` Bounded current malformed records and headers return safe typed owning errors and free every decoding allocation; accepted records preserve their complete value through re-encoding. | all eleven kinds, truncated/altered headers, invalid current fields and bounded arbitrary bytes, absent or present Kafka applied-start generation, v6 | 256 | 4096 bytes |
| `backup-corrupt-archives` | `nervix-backup` Changed current section lengths, digests, kinds, order, membership and record bytes, missing or misplaced deduplicator and window groups, or truncated sections fail before verified archive contents reach restore installation. | changed lengths/digests/kinds, reordered/missing/extra sections, invalid records, materialized, deduplicator and window group metadata/membership, damaged Arrow sections, truncation and bounded arbitrary tar inputs, absent or present Kafka applied-start generation, v7 | 256 | 4096 bytes |
| `console-pending-backups` | `nervix-web-console` pending backup entry and tab storage round-trip equality, with forgetting only by its own reference | any execution reference and query text, no domain or one in any spelling the name rule admits, recorded over an earlier record, v3 | 256 | 1024 bytes |
| `console-pending-backups-malformed` | `nervix-web-console` exact read-back or refusal of a tab's stored record, which a refusal forgets while other entries stay | missing, canonical, edited, upper-cased and drawn reference, query and domain entries beside an unrelated entry, v3 | 256 | 1024 bytes |
| `console-restore-drafts` | `nervix-web-console` restore draft lowering equality and existing-user option text | every `RESTORE` scope, source, mode, runtime state and lifecycle, domains in any spelling the name rule admits, every policy, v3 | 256 | 512 bytes |
| `console-client-drafts` | `nervix-web-console` client draft rendering and complete Model reparse equality | all 22 Plexus client heads, actual and masked configuration in order, pool bounds, optional resource pins and WebSocket signaling, v5 | 128 | 1024 bytes |
| `backup-capture-wire` | `nervix-interconnect` complete capture/drain/inventory request and response equality and fetch request equality through each request's declared class limit | every drain action and counter, every success/refusal, empty and ordered inventories of all twelve kinds, zero/full-width lengths and counters, arbitrary digests and bounded Unicode paths, v2 | 256 | 1024 bytes |
| `backup-capture-wire-malformed` | `nervix-interconnect` typed decode refusal or complete stable re-encoding, with management/bulk reservations released | truncated, bit-flipped, overwritten and arbitrary capture/drain/inventory request and response bytes and fetch request bytes, v2 | 256 | 1024 bytes |
| `backup-section-mapping` | `nervix-server` total and injective capture-kind/archive-content mapping and distinct validated archive paths | all twelve section kinds over generated identities, group ordinals and branched/unbranched fingerprints, v2 | 256 | 128 bytes |
| `restore-installation-wire` | `nervix-interconnect` complete installation request and success/refusal wire equality through the bulk class limit | all actions, authority, all eight runtime state kinds branched and unbranched, zero/full-width lengths, arbitrary digests, payload and inventory, v3 | 256 | 128 bytes |
| `restore-installation-storage` | `nervix-server` complete staged checkpoint and publication record equality | authority, placement, revision, payload and inventory, v2 | 256 | 128 bytes |
| `restore-staging-kept-measurement` | `nervix-server` the measurement of unpublished restore staging a store kept equals one read afresh from every staged entry, after every step | histories of up to ten steps over two domains, two entities and three generations: staging of up to two whole chunks and a partial one that completes, fails its digest or is cut short, retried staging, sweeps that retain or reclaim, publication and reopening, v3 | 96 | 96 bytes |
| `consensus-state-records` | `nervix-consensus` complete recovered state equality through the state writer, a real keyspace and recovery, for a first revision and a second stored as changes | every record family, including unit-keyed schedules, with replaced and removed records, every transaction, outcome and command execution state, the accepted statements a finished transaction keeps, names across the whole name rule and boundary counts and times, v19 | 128 | 65536 bytes |
| `consensus-state-corruption` | `nervix-consensus` typed `InvalidState` recovery failure or a canonical state its readers can rely on | flipped, truncated, extended, rekeyed, copied, unknown and missing records of a stored revision, drawn from bytes of their own, v19 | 256 | 65600 bytes |
| `consensus-raft-records` | `nervix-consensus` complete Raft record equality through the storage codec, and keys that name their position or section | votes, log positions, entries of every command, current state metadata, snapshot manifests and sections, v17 | 256 | 4096 bytes |
| `consensus-raft-records-malformed` | `nervix-consensus` typed storage failure or a value that stores back to the same record | arbitrary bytes as every Raft record, log key and section key, v10 | 256 | 4096 bytes |
| `registry-stored-models` | `nervix-server` complete Model equality through listing and keyed reads of a reopened registry, from its journal and from flushed tables | Models of every family in domains and with names across the whole name rule, v19 | 64 | 4096 bytes |
| `registry-stored-models-corruption` | `nervix-server` typed registry storage failure or Models whose own keys are exactly the stored keys | flipped, truncated, extended, recased, extended-key, copied and unknown records, v19 | 128 | 4096 bytes |
| `runtime-state-keys` | `nervix-server` exact decoded placement projection, and prefixes, index and chunk keys that reach no other placement | every state kind with schema, generation, Model kind, name, execution unit and branch key, names across the whole name rule, v4 | 256 | 2048 bytes |
| `runtime-state-keys-malformed` | `nervix-server` typed storage failure or a placement whose own key is the stored key | damaged and arbitrary keys, v4 | 256 | 2048 bytes |
| `runtime-state-chunk-keys-malformed` | `nervix-server` typed chunk, storage-format or decoding failure, or a segment set whose placement key decodes canonically and whose chunk prefix and offset respell the key | damaged and arbitrary chunk keys, v4 | 256 | 2048 bytes |
| `runtime-state-staging-namespaces` | `nervix-server` the domain each key names after its namespace, and a maintenance walk that visits every namespace once in key order without passing another's key | placement, index and chunk keys of up to three domains in the initial namespace and restored generations of any value, v4 | 64 | 4096 bytes |
| `runtime-state-staging-namespaces-malformed` | `nervix-server` typed storage-format failure at the first key whose namespace does not decode, or the namespace and domain the key's leading bytes spell exactly | damaged and arbitrary keys, v4 | 128 | 2048 bytes |
| `runtime-state-identity-records` | `nervix-server` complete handoff and forced-recovery record equality with branch values by archived bits, and key identity | transitions with checkpoints of every placement kind, v4 | 256 | 4096 bytes |
| `runtime-state-identity-records-malformed` | `nervix-server` typed decode failure or a record that stores back unchanged | arbitrary bytes as every identity record, v4 | 256 | 4096 bytes |
| `runtime-state-snapshots` | `nervix-server` Kafka offsets, applied-start generation and schedules, branch lifecycles with activity and incarnations, deduplicator keyspaces bit for bit in arrival order, and branch-aggregated metrics restore every stored value through their codecs and the inline checkpoint envelope. | Kafka offsets, branch lifecycles, deduplicator keyspaces with float bits and branch-aggregated metrics, absent or present Kafka applied-start generation, v5 | 256 | 4096 bytes |
| `runtime-state-snapshots-malformed` | `nervix-server` each kind's typed decode failure or a value that stores back to the same payload | arbitrary bytes as every snapshot payload, v4 | 256 | 4096 bytes |
| `runtime-state-store-checkpoints` | `nervix-server` byte equality at the stored revision through both readers and every reader of a backup cut's listing, which lists exactly the domain's checkpoints in key order and by kind with their stored sizes, before and after the database reopens | up to three placements of one domain stored inline, as chunked streams or in a published restore generation at, beside and between the chunk boundaries, beside an unpublished generation and another domain, v5 | 64 | 4096 bytes |
| `runtime-kafka-offset-captures` | `nervix-server` A captured offset table with its topic schedules writes a native checkpoint no longer than the bound its capture reserves, from which the replica's cancellable reader restores every partition offset, applied-start generation and schedule, checking before every native entry and stopping at whichever check refuses. | up to three topics of three partitions at any offset, and three topic schedules, absent or present Kafka applied-start generation, v4 | 256 | 2048 bytes |
| `runtime-kafka-offset-transfer-headers` | `nervix-server` exact revision, digest and payload from a checkpoint stream cut at every position and into generated chunks | advancing revisions beside the held and described ones, payloads of up to 128 bytes, v3 | 256 | 1024 bytes |
| `runtime-kafka-offset-transfer-headers-malformed` | `nervix-server` typed length or revision refusal, or the revision and digest the header spells, with exactly the bytes behind the header forwarded at every split | arbitrary streams, half of them shorter than the header, with revisions beside the one they spell, v2 | 256 | 1024 bytes |
| `runtime-window-snapshots` | `nervix-server` complete sealed window equality, and nothing for another branch lifetime | empty or up to 192 rows, concrete multi-type or absent branch keys, boundary sequences, nullable string arguments of zero or 2049 bytes and up to 96 delayed removals; generated 4–16 KiB column limits include IPC framing and split columns independently of the 1–32 KiB identity/typed limits, v4 | 128 | 2048 bytes |
| `runtime-window-snapshots-malformed` | `nervix-server` typed window snapshot failure or a window that seals back to the same bytes | sealed windows damaged once and arbitrary bytes behind the magic, v3 | 256 | 4096 bytes |
| `runtime-window-sealed-checkpoints` | `nervix-server` a window sealed in quota-owned pieces byte-identical to its resident encoding and restored completely | empty or up to 192 rows with bit-exact concrete branch keys or unbranched execution, boundary sequences, nullable string arguments of zero or 2049 bytes and up to 96 delayed removals under generated 4–16 KiB column and 1–32 KiB identity/typed limits, v4 | 64 | 2048 bytes |
| `runtime-arrow-bodies` | `nervix-server` relay body and sealed snapshot section Arrow IPC equality: exact schema, values, nulls and float bits; several sections refused by the one-section decoders and concatenated by the schema-free one | every field type with nulls, nested lists, masked list elements, empty and sliced views and boundary values, v4 | 128 | 2048 bytes |
| `runtime-arrow-bodies-malformed` | `nervix-server` typed refusal of a misframed or undecodable Arrow body, or a batch of the expected schema that encodes and decodes back to itself, without allocating from a declared length | relay bodies and sealed sections truncated, flipped, overwritten, shifted or trailed, and arbitrary bytes, v4 | 256 | 2048 bytes |
| `resource-store-layout` | `nervix-server` complete manifest and archive read-back for every installed version, with explicit absence after deletion | names across the whole name rule and those spelled like the store's own directories, staging cleanup and removal of another version, v3 | 64 | 512 bytes |
| `client-producer-batches` | `nervix-server` producer batch equality from the client library's stream to the node's batch, with exact row and byte limits | every field type with nulls, nested lists and sliced views, v3 | 128 | 2048 bytes |
| `client-producer-batches-malformed` | `nervix-server` refusal with a batch defect or a batch within the ingestor's schema and row limit | damaged producer streams and arbitrary bytes, v3 | 256 | 2048 bytes |
| `codec-schemaful-rows` | `nervix-server` `WIRE JSON`, `WIRE CBOR` and `WIRE AVRO` row round-trip equality through one group builder | registry-accepted codecs over each format's value domain, generic and exact wire types and shuffled wire field order, v3 | 256 | 4096 bytes |
| `codec-schemaful-payload-groups` | `nervix-server` each payload decodes in a group exactly as alone; refusals are typed | codec payloads interleaved with damaged bytes, swapped values and changed keys, v3 | 256 | 4096 bytes |
| `codec-json-non-finite-floats` | `nervix-server` the JSON projection of non-finite floats: optional top-level fields read back as nulls, required fields and list elements refuse the payload | `WIRE JSON` and `WIRE CBOR` codecs over every float bit pattern and field type, v3 | 256 | 4096 bytes |
| `codec-transforming-numbers` | `nervix-server` number equality through the JAQ-native `JSON`, `YAML`, `TOML` and `CBOR` codecs and a protobuf codec with unchanged programs: `F64` and `F32` bits, alone and in lists, and `I64` values; a protobuf singular zero reads back positive | generated batches of one schema of numbers over every finite float bit pattern, v3 | 256 | 1024 bytes |
| `jaq-native-numbers` | `nervix-jaq` number equality through the identity program and each numeric native format's writer and reader: float bits, integer value and signedness | finite floats of any bit pattern, boundary floats and 64-bit integers, `TOML` within its signed range, v1 | 256 | 256 bytes |
| `remote-relay-rows` | `nervix-server` routed relay batch equality from the sender's payload to the receiver's rows, branch key bits and watermarks | every field type, concrete and absent branches and per-row registrations, v3 | 128 | 2048 bytes |
| `remote-relay-rows-malformed` | `nervix-server` the first payload defect in the receiver's check order | foreign bodies, miscounted watermarks and registrations and invalid branch keys, v3 | 128 | 2048 bytes |
| `relay-wire-messages` | `nervix-interconnect` relay grant, reply, admission, acknowledgement and connection binding equality through the bounded codec | every message variant, branch key values with any float bits and per-row metadata, v5 | 256 | 2048 bytes |
| `bulk-request-envelope-wire` | `nervix-interconnect` complete Bulk stream request envelope equality and input/output charge lifetimes through the production encoder and wire decoder | bounded opaque payload bytes, including empty input and writer growth, v1 | 64 | 4096 bytes |
| `receive-admission-outcomes-wire` | `nervix-interconnect` typed decoder-report projection, stream status and complete response-envelope equality through the production encoder and decoder, with all charges released | every traffic class and memory/execution refusal variant, boundary byte/count fields, v1 | 64 | 64 bytes |
| `relay-wire-messages-malformed` | `nervix-interconnect` typed decode failure or a message that encodes and decodes back to itself, with every allocation of the decoder freed | damaged encodings of every message and arbitrary bytes, v5 | 256 | 2048 bytes |
| `client-producer-link-frames` | `nervix-server` complete equality of every client producer link frame through the bounded duplex codec of its stream, with nothing left charged | the opening message, every serving-node frame and every owning-node answer, with schemas of any field type, limits and counts at their ends, opaque batch bytes, every refusal, outcome, admission state and end reason, v3 | 256 | 2048 bytes |
| `client-producer-link-frames-malformed` | `nervix-server` typed decode failure, never an admission or execution refusal, or a frame that encodes and decodes back to itself | damaged encodings of every link frame, frames read as another direction's type and arbitrary bytes, v3 | 256 | 2048 bytes |
| `client-consumer-stream-frames` | `nervix-server` complete equality of every client consumer stream frame through the bounded duplex codec of its stream, with nothing left charged | the opening message, every serving-node frame and every owning-node answer, with schemas of any field type, policy durations from zero to the longest, every answer, refusal and settlement outcome, delivery headers across their ranges with and without a branch fingerprint, and chunks from empty to the largest one sent, v3 | 256 | 2048 bytes |
| `client-consumer-stream-frames-malformed` | `nervix-server` typed decode failure, never an admission or execution refusal, or a frame that encodes and decodes back to itself | damaged encodings of every stream frame, frames read as another direction's type and arbitrary bytes, v3 | 256 | 2048 bytes |
| `client-emitter-deliveries` | `nervix-server` client emitter delivery equality from the emitter's Arrow IPC stream to the client library's batch: the Arrow schema of exactly the declared fields, every delivered row in order, values, nulls and float bits; refusal under another row count or other fields | every field type with nulls, nested lists and sliced views, deliveries of adjacent and scattered rows, v3 | 128 | 2048 bytes |
| `wasm-generated-pools` | `nervix-server` generated column pool equality from a WASM guest's Arrow IPC stream to the node's batch: unnamed fields, types, nullability, values, nulls and float bits | every field type with nulls, nested lists and sliced views, streams ended by the marker or closed, v3 | 128 | 2048 bytes |
| `wasm-generated-pools-malformed` | `nervix-server` a typed generated-IPC or batch-count defect, or one batch of unnamed columns that writes and decodes back to itself | truncated, flipped, overwritten, shifted, trailing and arbitrary pools, v3 | 256 | 2048 bytes |
| `simd-checked-lanes` | `nervix-simd-kernels` checked integer arithmetic equals scalar overflowing arithmetic at every SIMD level | all integer widths, runs and shared operands, v1 | 256 | 128 bytes |
| `simd-constant-division` | `nervix-simd-kernels` constant quotients and remainders equal checked operations at every SIMD level | all integer widths, divisors, scalar fallback and tails, with full-range signed overflow, v1 | 256 | 128 bytes |
| `deadlock-evidence` | `nervix-deadlock` complete evidence encode/decode equality | every finding and review variant, complete shared-reader cycles, cycles up to the thread bound with omitted threads, texts cut inside multi-byte characters, absent context and extreme identities and times, v4 | 256 | 4096 bytes |
| `deadlock-evidence-malformed` | `nervix-deadlock` arbitrary bytes, bare or behind a valid evidence header, fail typed or decode within every bound to evidence that encodes back to itself | arbitrary bytes, v3 | 256 | 1024 bytes |
| `deadlock-evidence-bounds` | `nervix-deadlock` a valid encoding that breaks one bound is refused as out of bounds | one broken bound of findings, cycle threads, omitted threads, identities, lost findings or kept texts, v3 | 256 | 1024 bytes |
| `deadlock-evidence-summary` | `nervix-deadlock` the summary a qualification prints counts each finding once by kind, keeps repeated deliveries apart from lost findings, agrees with qualification, and names every count once | every finding variant in every scope, reviewed and unreviewed cycles, extreme repetition and loss counts, v3 | 256 | 4096 bytes |
| `branch-membership` | `nervix-branch-instances` owner steps against the specified visible-set contract: each claim, admission, eviction, expiry and release publishes exactly the current owner lifetime's membership, and a step that changes none publishes nothing | bounded claim, admit, expire and release sequences over six branch keys, v1 | 256 | 256 bytes |
| `task-status-transitions` | `nervix-server` task status publishes the complete healthy or failed status, preserving retry on error-only changes | 64 transition bytes over current status and retry values, v2 | 256 | 64 bytes |
| `entity-freeze-transitions` | `nervix-server` freeze publications contain exactly the coordination owners whose holds remain active | 64 insert/remove bytes over eight coordination identities, v1 | 256 | 64 bytes |
| `replica-progress` | `nervix-checkpoint-replication` replica reports, offers and announcer steps against the monotonic quorum contract: each replica's progress is the highest revision it reported, a count of replicas holding a revision never falls, and an offered revision keeps exactly one announcer until every assigned replica holds it or the replicated state is gone | bounded report, offer, step, cancel and retire sequences over four replicas and sixteen revisions, v1 | 256 | 256 bytes |
| `replica-catch-up` | `nervix-server` an owner's catalog of branch checkpoints and a replica's catch-up rounds against convergence: whatever catalog reads and branch fetches fail and however announcements are lost, repeated or reordered, the replica holds every branch its lifecycle names at the owner's revision once reads and fetches succeed, never records holding what its storage does not, and every listing reaches it through its wire form and encoding unchanged | bounded start, publish, stop, catalog replacement, naming, eviction, lifecycle, announcement and round sequences over four branches, unbranched work included, v1 | 256 | 256 bytes |
| `typed-report` | `nervix-lint-report` complete compiler-report serialization equality | empty and multi-item findings, arguments and audit candidates; all current audit patterns, escaped Unicode text, full-width spans and counts, expansions and completion metadata, v3 | 256 | 32 bytes |
| `typed-source-contract` | `nervix-lint-report` complete source-contract parsing equality | bounded current execution-contract annotations, v1 | 256 | 32 bytes |
| `typed-site-union` | `nervix-lint-report` authored-site deduplication preserves every configuration | repeated ordinary/modeled findings over bounded current sites, with complete current reports, v3 | 256 | 32 bytes |
| `typed-source-syntax` | `nervix-lint-report` exact cast/field counts and complete source/site representation equality | bounded inactive Rust casts, testing-gated tuple fields, raw identifiers and Unicode comments with exact locations, v1 | 256 | 32 bytes |
| `typed-hidden-tunables` | `nervix-lint-report` complete numeric policy site equality and source/site JSON round trips | bounded numeric constants, literal capacities, inactive and macro source, fixed contracts and test exclusions with exact locations, v1 | 256 | 32 bytes |
| `expiry-map-operations` | `nervix-expiry-map` operation sequences against an `IndexMap` reference: iteration order, lengths, oldest entry, lookups, shared key allocations and one drop per value | up to 480 weighted insert, shared insert, keyed and oldest removal, lookup, shared-key copy and clear steps over 96 keys, v1 | 256 | 1024 bytes |
| `vm-decimal-rounding-floats` | `nervix-vm` `round(value, digits)` over `F64` and `F32` columns against the exact decimal expansion, the identity on values holding no more digits, symmetry about zero and lane null and failure classification | any bit pattern, values rounded within their own digits, decimal-looking values and exact binary ties, counts near, within and beyond the bound, v1 | 256 | 1024 bytes |
| `vm-decimal-rounding-integers` | `nervix-vm` integer rounding against a 128-bit reference, overflow failures, lane nulls and digit columns of every integer type clamped to the bound | every integer width, values anywhere and beside both limits, counts near, within and beyond the bound, v1 | 256 | 1024 bytes |
| `approx-into-rounding` | `nervix-approx-into` integer-to-float and `F64`-to-`F32` rounding against a bit-level IEEE 754 reference, and float-to-integer truncation against an exact reference | 128-bit patterns read at every width, integers and floats beside powers of two, ties and the integer limits, any `F64` and `F32`, v1 | 256 | 64 bytes |
| `approx-into-exact` | `nervix-approx-into` lossless round trips on the exact domain | integers whose significant bits fit each float's significand, every non-NaN `F32` through `F64`, v1 | 256 | 32 bytes |
| `simd-json-escapes` | `nervix-simd-kernels` JSON escape block masks and row bits at every level against the scalar class, and the first offset defect | sparse and dense buffers up to 1,100 bytes, ascending offsets not starting at zero, empty, negative, out-of-buffer and descending offsets, v1 | 256 | 4096 bytes |
| `simd-paced-admission` | `nervix-simd-kernels` packed admission bits at every level against the scalar definition | reached centers anywhere and at the timeline's ends, every period reduction, skews to beyond half a period, timestamps beside edge and interior centers, v1 | 256 | 2048 bytes |
| `simd-elapsed-histograms` | `nervix-simd-kernels` layout refusals against an integer reference, and latest instant, totals and buckets at every level against HdrHistogram | units from a nanosecond to any 32-bit count, highest units to 2^31, up to six figures, instants before, beside half units and after the reference, v1 | 256 | 4096 bytes |
| `simd-flag-packing` | `nervix-simd-kernels` packed flag words at every level against scalar shifts, appended to held words | runs of up to 600 flag bytes of any density, v1 | 256 | 2048 bytes |
| `simd-window-runs` | `nervix-simd-kernels` validity, boolean, sum, extreme, non-finite and reverse folds against scalar folds at every level; compensated sums exact for integers and within their bound of an exact sum for floats | up to 300 values read at every width and as floats, unaligned validity and boolean bitmaps, v1 | 256 | 4096 bytes |
| `simd-window-moments` | `nervix-simd-kernels` means, M2 and paired cross moments against exact addition/product references, and exact histogram placement | up to 128 finite paired values, independent sliced null masks, constant overflow-scale runs, edges and clamps, v1 | 256 | 4096 bytes |
| `columnar-json-rows` | `nervix-columnar-json` written rows against an independent rendering of their logical values and the first refused null | every supported scalar, list, fixed-size and nested list column, sliced, every null policy and number and bytes encoding, v1 | 256 | 8192 bytes |
| `bounded-write-limits` | `nervix-bounded-write` whole writes within the limit, capacity within it, and the outcome classification against a reference of the rule | writes, encoder failures and writing on after a refusal under limits of 1 to 1,021 bytes, v1 | 256 | 1024 bytes |
| `connector-row-request-division` | `nervix-connector` row request division against the documented halving rule, both limits and the measurement bound | declared and native row and byte limits, up to 160 rows, measures that grow and shrink with a candidate, v1 | 256 | 1024 bytes |
| `connector-confirmation-window` | `nervix-connector` confirmation window outcomes, end instant and in-flight bound against a reference model of the documented window rule, so no record confirmed before the publish ends is left unresolved | `MAX` one to six, `ACK TIMEOUT` one to 40 ms, up to 24 records refused at hand-over or answered confirmed, refused or failed within, at, past or never by their deadlines, pauses between hand-overs and a driver failure, on a paused clock, v2 | 256 | 512 bytes |
| `pulsar-unconfirmed-receipts` | `nervix-connector-pulsar` task-owned NO ACK receipts against an independent completion/count model, retaining the first typed failure and counting every unanswered receipt at finish | Zero to 100 hand-overs with confirmed, failed or unanswered receipts, collection between hand-overs and a finish deadline already expired or reached while waiting on a paused clock, v1 | 256 | 512 bytes |
| `emitter-batch-subdivision` | `nervix-server` batch candidates against the documented rule, message limit, compatibility and encoding bound | up to 96 members in three compatibility classes, failing containers, encodings that grow and shrink, v3 | 256 | 1024 bytes |
| `emitter-batch-packing` | `nervix-server` packed payloads and rejected rows against the documented packing of exact member encodings and container framing | generated schemaful codecs and rows in carriers across two relays, three branches, three envelopes and seals, `MAX SIZE` from below one member's container to above the whole run's, v3 | 256 | 4096 bytes |
| `codec-bounded-encodings` | `nervix-server` bounded row, member and container encodings against the unbounded bytes, and container framing | generated schemaful codecs and batches under limits short of, at, past and around each encoding, and containers of 24, 64 and 256 repeated members where a count's encoding grows, v3 | 256 | 4096 bytes |
| `emitter-row-preparations` | `nervix-server` row request preparations kept unchanged or refused with the first violation | preparations of up to 12 selected rows spoiled by repeated, swapped, dropped, foreign and refused members and empty requests, v1 | 256 | 256 bytes |
| `emitter-retained-payloads` | `nervix-server` retained payloads written unchanged in packing order and answered member by member | packing and write steps with delivered, rejected and unanswered records and failed writes over three buffered batches, v1 | 256 | 512 bytes |
| `sqs-batch-requests` | `nervix-connector-sqs` measured record sizes, refusals above 256 KiB, and batch requests within every limit, ended only by a limit | small and near-limit bodies of one- to four-byte characters, attributes and FIFO groups, v1 | 256 | 1024 bytes |
| `iceberg-nested-datetime-staging` | `nervix-connector-iceberg` staged nested-list schema and values against containing-microsecond division while preserving list boundaries | up to 16 generated UTC nanosecond timestamps in two list levels, including negative and boundary values, v1 | 256 | 160 bytes |
| `iceberg-table-projection` | `nervix-connector-iceberg` staged batches laid out as the table's schema: refused exactly when a required column is left out or mapped from an expression that can produce a null, naming the first such column, and otherwise every staged value unchanged in the table column of its name and null in every column left out | one to six long, string, widened integer and binary columns, required or optional, mapped or left out, in any `VALUES` order, from nullable and never-null expressions, up to eight rows with nulls, v1 | 256 | 512 bytes |
| `iceberg-staged-file-encoding` | `nervix-connector-iceberg` staged Arrow IPC rows read back unchanged and their complete measured Parquet encoding, including its footer, equals the written file size | one to eight mapped rows with generated string values and integer IDs, v1 | 256 | 512 bytes |
| `http-poll-response-headers` | `nervix-connector-http` retains every valid UTF-8 value with exact bytes and repeat order, drops each invalid value alone, and counts it | up to 32 generated visible UTF-8 characters, including non-ASCII, and one of three invalid byte sequences between valid repeated values, v1 | 256 | 128 bytes |
| `http-emitter-requests` | `nervix-connector-http` requests handed to hyper against every prepared field, the target's origin or absolute form, the transport's fields, the proxy's credentials, the declared empty length and the exact body, and the tunnel request for each target | methods that define request content, methods that define none, their ASCII case variants and extension methods, named, IPv4 and IPv6 origins with and without default ports, plain targets, application headers with and without `Accept` and with ASCII or non-ASCII UTF-8 values, bodies up to 64 bytes, HTTP/1.1, HTTP/2 and forwarded routes with and without proxy credentials, v2 | 256 | 512 bytes |
| `mqtt-publish-packets` | `nervix-connector-mqtt` measured PUBLISH sizes against the packet the client writes, the protocol's largest packet and the broker maximum | topics up to 128 bytes, every quality of service, payloads beside every Remaining Length width and the protocol limit, v1 | 256 | 512 bytes |
| `postgres-insert-measures` | `nervix-connector-postgres` measured inserts against the statement and the arrays the driver encodes, and the Bind message bound | up to four text columns of up to eight rows with NULLs, quotes and multi-byte characters, every run of rows, v1 | 256 | 1024 bytes |
| `mysql-insert-measures` | `nervix-connector-mysql` complete quoted SQL and parameter measure equals the driver's binary serializer | one-/two-part MySQL names, three nullable columns, one to eight rows, every conflict action and nonempty row range, v1 | 256 | 1024 bytes |
| `mongodb-write-answers` | `nervix-connector-mongodb` answers for every row of a failed `insert_many` or `bulk_write` against a reference of the server's per-document account, and the failure that names the request and the write concern's code | writes of one to twelve rows, applied results of each kind, refusals for good and for retry, repeated refusals, answers beyond the write, write concern failures, v1 | 256 | 256 bytes |
| `mongodb-bson-mapping` | `nervix-connector-mongodb` complete BSON bytes and stored size against an independent typed document, including the generated identifier reservation | every declared scalar, float bits, nullable lists, null rows, mapped identifiers and nanosecond dates floored to milliseconds, v1 | 256 | 256 bytes |
| `websocket-signaling-protobuf-values` | `nervix-connector-websockets` complete signaling JSON through the production protobuf conversions, with double and float bit equality | explicit scalar presence, absent defaults, full-width integers, booleans, UTF-8 strings and up to eight repeated doubles and floats, v1 | 256 | 256 bytes |
| `websocket-signaling-protobuf-malformed` | `nervix-connector-websockets` typed refusal through the production protobuf decoder | invalid tags, truncated fixed-width fields and strings, oversized varints and incomplete packed floats, v1 | 256 | 256 bytes |
| `otel-hex-ids` | `nervix-connector-otel` complete identifier bytes and acceptance against a per-digit scalar reference, with exact refusal key and reason | nonzero trace, span and parent-span identifiers in all letter cases, arbitrary UTF-8 text, odd lengths, trailing spaces and all-zero identifiers, v1 | 256 | 128 bytes |
| `models-header-pairs` | `nervix-models` complete percent-decoded header values and pair order, with normalized names | zero to four pairs with UTF-8 values, whitespace, commas, equals signs, percent signs and plus signs, v1 | 256 | 128 bytes |
| `models-otel-mappings` | `nervix-models` complete OTEL mapping JSON/archive round trips, exact field types and literal domains | bounded mappings, attributes, resource and scope; Arrow type candidates and signed severity/identifier literals, v1 | 256 | 128 bytes |

| `interconnect-slot-lifetimes` | `nervix-interconnect` Bounded slot claim, retire, replace and dial-update sequences preserve one worker per lifetime, irreversible retirement, shared slot identity for dial updates and exact pool capacities. | bounded slot claims, retirements, replacements and dial updates, v1 | 256 | 64 bytes |
| `relay-channel-lifetimes` | `nervix-server` Bounded bind, send, route replacement, eviction and teardown sequences preserve channel identity and order, cancel retired handles and bound retained live tables to active branches and destinations. | bind, send, replacement, eviction and teardown sequences across branches and destinations, v1 | 256 | 64 bytes |
| `endpoint-route-table` | `nervix-server` Bind, unbind, replace, teardown and installation publish exactly the configured routes, preserve other domains, prevent retained routes from admitting ended source lifetimes, and report a missing route as final only in an installed table. | bind, unbind, replacement, teardown and installation sequences across domains, v2 | 256 | 64 bytes |
| `cluster-peer-clock-probes` | `nervix-server` A peer clock probe and every answer, a reading since the Unix epoch to the nanosecond at either end of the seconds range or a clock that reads before the epoch, round trip through the archive unchanged. | current peer probe and answer variants with boundary nanosecond readings, v1 | 256 | 64 bytes |
| `backup-streamed-state-records` | `nervix-backup` Kafka offsets and branch lifecycle records written from iterators equal the canonical rkyv encoding of their complete wire shape byte for byte and decode to the complete original records. | Kafka offsets and branch lifecycle records, including empty and ordered entries, absent or present Kafka applied-start generation, v3 | 256 | 4096 bytes |
| `client-ffi-host-columns` | `nervix-client-ffi` Every column a host builds, of every scalar, text, bytes and nested list type, nullable or not, reads back level by level unchanged from the built batch and from the batch its canonical Arrow IPC stream decodes to. | nullable and required scalar, text, byte and nested-list columns, v1 | 256 | 512 bytes |
| `models-external-names` | `nervix-models` Every external name type keeps its exact text, case and punctuation through parsing, display, conversions, JSON and the archive. | all external name kinds with case, punctuation and exact text, v9 | 256 | 512 bytes |
| `models-external-name-validation` | `nervix-models` On arbitrary text every external name type's parser, conversion, JSON decoder and archive decoder accept exactly the same text and keep it as written. | arbitrary text for every external name kind, v8 | 256 | 512 bytes |
| `models-prerequisite-records` | `nervix-models` A node's prerequisite record keeps every value through its gossip text and the archive and judges a peer as the written record does, fingerprints keep their digests through text, and a prerequisite mismatch keeps its kind and both values through the archive. | current node prerequisite records and peer values, v1 | 256 | 1024 bytes |
| `models-admission-verdicts` | `nervix-models` The verdicts a leader publishes keep their term and, for every run, whether it was accepted or refused, the voter a refusal names and both values of its mismatch, through their gossip text. | current leader verdicts on up to six runs under any term, v1 | 256 | 2048 bytes |
| `stopping-node-drain-wire` | `nervix-interconnect` Every current stopping-node drain action and every answer, with its complete report, survives the production typed-request payload encoder and decoder. | current stopping-node drain actions, answers and complete reports, v1 | 256 | 128 bytes |
| `runtime-checkpoint-description-wire` | `nervix-interconnect` A current checkpoint description preserves presence, revision, full byte length and digest through the production replication response encoder and decoder. | absent and present checkpoint descriptions with boundary lengths and digests, v2 | 256 | 64 bytes |
| `runtime-state-replication-wire` | `nervix-interconnect` Every current runtime-state request and reply retains its complete placement, state/schema/generation, model kind, ordered branch fields and nested float bits, counters, checkpoint length/digest, listing and refusal variant through its production pool-class codec, releasing all charges. | all current runtime-state request and reply variants with nested branch and placement fields, v4 | 256 | 2048 bytes |
| `runtime-state-replication-malformed` | `nervix-interconnect` Bounded damaged current runtime-state requests and responses either decode or fail with a typed decode outcome; empty frames and zero-byte encoding limits refuse every message kind, and no command or bulk charge survives. | damaged current requests and replies, empty frames and zero-size limits, v4 | 256 | 2048 bytes |
| `runtime-checkpoint-listing-owner` | `nervix-server` Absent, restarted and continued owner listings retain their complete ordered branch keys, state/schema/generation, cursor, revisions, continuation flag and float bits across the semantic conversion and production replication response codec. | absent, restarted and continued listings with ordered entries and cursors, v4 | 256 | 2048 bytes |
| `runtime-checkpoint-listing-malformed` | `nervix-server` Structurally valid production-wire listings with empty keys, invalid field names, invalid datetime text or non-finite scalar/nested floats in either removed or revised entries fail the owner's typed BranchKey boundary, releasing their codec charges. | wire listings with malformed keys, fields, datetime text and scalar or nested floats, v4 | 256 | 2048 bytes |
| `simd-byte-classes` | `nervix-simd-kernels` At every supported SIMD level and the scalar fallback, the first member of a generated buffer, and every member a forward scan finds after each generated jump, equal the scalar definition of a range-only, a listed-only and a mixed byte class. | generated byte buffers and range, listed and mixed classes at all supported SIMD levels, v1 | 256 | 512 bytes |
| `simd-xml-chars` | `nervix-simd-kernels` At every supported SIMD level, the one-pass verdict on generated bytes equals standard UTF-8 decoding followed by the XML 1.0 Char production, invalid UTF-8 outranking an excluded character, and the check of valid text agrees with it. | arbitrary bytes including invalid UTF-8 and XML-excluded characters, v1 | 256 | 1024 bytes |
| `syslog-stream-framing` | `nervix-connector-syslog` However reads split a generated RFC 6587 stream, the stream decoder yields exactly the frames and the ending, clean, incomplete or the typed framing failure, that framing the whole stream defines. | RFC 6587 streams split at generated read boundaries, with clean and incomplete endings, v1 | 256 | 512 bytes |
| `rabbitmq-client-address` | `nervix-connector-rabbitmq` Every generated amqp or amqps address, with percent-encoded credentials and virtual host, a name, IPv4 or IPv6 host, an optional nonzero port and Lapin's query options, names exactly the scheme, credentials, host, port, virtual host and options it was written from, through the one owner that reads it before commit and for every connection. | `amqp` and `amqps` URIs with optional credentials, name, IPv4 and IPv6 hosts, optional ports, virtual hosts and every Lapin query option, v1 | 256 | 256 bytes |
| `syslog-structured-data` | `nervix-server` For generated text, the scanner-based RFC 5424 structured-data parser and header check report exactly the length, or the failing byte and typed reason, of the per-byte reading of the grammar. | generated RFC 5424 structured-data and header text, valid or malformed, v1 | 256 | 512 bytes |
| `paced-simulation-records` | `nervix-paced-simulation` Every ledger and effect-store record the paced simulation driver writes reads back from its one JSON line as the complete original record, timestamps to the nanosecond and escaped text included. | current simulation ledger and effect-store records, escaped text and nanosecond times, v1 | 256 | 96 bytes |
| `state-replication-routing` | `nervix-server` Frame acknowledgements reach only the current installed state; replacement, identity publication and teardown never attach a retained route to its successor, across interleaved entities and branches. | acknowledgement, replacement, identity publication and teardown sequences, v1 | 256 | 64 bytes |
| `materialized-publication-lifetimes` | `nervix-server` Bounded update, capture, delete and rebind sequences preserve strict high/low timestamp selection, ties, branch identities, revision and lifecycle generations; every captured snapshot restores its complete Arrow schema, logical values and metadata despite subsequent live changes. | update, capture, delete and rebind sequences with timestamp ties and nested Arrow snapshots, v1 | 256 | 64 bytes |
| `deadlock-order-normalization` | `nervix-deadlock` Cyclic rotation normalizes directed lock instances without losing modes, lifetimes, source context or multiplicity; deduplication retains the complete cycle and counts callbacks. | directed lock cycles with modes, repeated instances and source contexts, v1 | 256 | 4096 bytes |
| `wasm-protocol-malformed` | `nervix-wasm-protocol` Short and arbitrary current guest messages return the owning typed error or a completely decoded value without panic. | short and arbitrary current guest-message bytes, v1 | 256 | 4096 bytes |
| `deadlock-order-bounds` | `nervix-deadlock` Current order evidence with disconnected, repeated, missing or excessive identities, edges, witnesses, multiplicity or invalid reviews is refused at its owning conversion. | current order evidence with disconnected, repeated, missing or excessive graph elements, v1 | 256 | 1024 bytes |
| `wasm-branch-metadata` | `nervix-wasm-protocol` Every current schema type, optional field, branch identity and schema order survives both production encoders and verified decoding. | all current schema types, optional fields and ordered branch identities, v1 | 256 | 2048 bytes |
| `wasm-envelopes` | `nervix-wasm-protocol` Input and output envelopes preserve complete opaque bytes, token sidecars, typed absence, routes and column order across verified borrowed and owned views. | input and output envelopes with bytes, token sidecars, routes and column order, v1 | 256 | 2048 bytes |
| `wasm-snapshots` | `nervix-wasm-protocol` Current snapshot envelopes preserve every init and opaque application-state byte, including empty state. | current snapshot envelopes with empty and nonempty init and application bytes, v1 | 256 | 2048 bytes |
| `wasm-abi-codes` | `nervix-wasm-protocol` Reset answers and saved-envelope/application rejection verdicts round trip their own ABI codes and classify every other code as absent. | current reset and rejection verdict codes plus other ABI code values, v1 | 256 | 16 bytes |
| `wasm-host-schema` | `nervix-wasm` The host ABI schema projection preserves names, exact nested types, nullability and order; sensitivity stays an explicit host execution rule. | host ABI schemas with nested types, nullability and ordered fields, v3 | 256 | 2048 bytes |
| `wasm-host-envelopes` | `nervix-wasm` Native host envelope conversions preserve complete protocol values, sidecars and retained output bytes after dropping the decoded owner. | native host envelopes with protocol values, sidecars and retained output bytes, v3 | 256 | 2048 bytes |
| `wasm-sdk-snapshots` | `nervix-wasm-sdk` SDK saves restore every opaque byte under the same branch contract or domain rename; a different branch configuration never calls application restore. | SDK snapshots with branch-preserving and branch-changing restore contexts, v1 | 256 | 2048 bytes |
| `wasm-sdk-rejections` | `nervix-wasm-sdk` Snapshot, init metadata and application-state rejection retain their owning typed contexts and distinct restore verdicts. | snapshot, init-metadata and application-state rejection variants, v1 | 256 | 2048 bytes |
| `wasm-sdk-input` | `nervix-wasm-sdk` SDK input preserves every Arrow schema, logical value bit, null, nested collection and sidecar through IPC and cloned buffer ownership. | Arrow input schemas, nested values, nulls and sidecars under cloned ownership, v1 | 128 | 2048 bytes |
| `wasm-sdk-output` | `nervix-wasm-sdk` SDK output preserves the complete shared generated column pool, routed references, sidecars and ordered callback emissions. | shared column pools, routed references, sidecars and ordered callback emissions, v1 | 128 | 2048 bytes |
| `wasm-state-descriptors` | `nervix-backup` Every current durable WASM descriptor preserves complete names, schema, branch identities and values, generation and revision; scalar floats preserve every bit. | current durable descriptors with branches, schema, generations and float bits, v3 | 256 | 2048 bytes |
| `wasm-state-descriptors-malformed` | `nervix-backup` Malformed descriptor bytes and out-of-domain current fields free every decoding allocation and return the owning typed archive failure before runtime installation. | malformed descriptor bytes and out-of-domain current fields, v3 | 256 | 4096 bytes |
| `wasm-protocol-corruption` | `nervix-wasm-protocol` Truncated frames, declared-size mismatches, invalid identifiers, offsets and vector counts fail before decoding or effects for every current message family. | truncated frames, size mismatches and invalid identifiers, offsets or counts, v1 | 256 | 2048 bytes |
| `wasm-protocol-tags` | `nervix-wasm-protocol` Unknown current union, processor-type and column-source tags, missing nested types and nonzero uninitialized indices yield the owning typed protocol error. | unknown union and source tags, missing nested types and uninitialized indices, v1 | 256 | 64 bytes |
| `wasm-stored-checkpoints` | `nervix-server` Stored checkpoint variants preserve complete bytes, revision, length and digest with fixed-size segmented headers; keys preserve complete typed placement and exact branch text, chunk-set projection preserves placement and revision with a cursor beyond every offset, and reset identity stays scoped to its selected branch. | stored checkpoint variants, segmented headers, placement keys and chunk-set cursors, v3 | 256 | 2048 bytes |
| `wasm-stored-checkpoints-malformed` | `nervix-server` Arbitrary stored checkpoints free every decoding allocation and fail with their owning typed error or decode completely and deterministically within the input budget. | arbitrary checkpoint bytes and bounded decoding allocations, v3 | 256 | 4096 bytes |
| `restore-native-lifecycle` | `nervix-server` Streaming native lifecycle encoding preserves every current typed branch field including nested arrays, vector kinds and float bits, timestamp and incarnation in order. | streamed lifecycle records with typed branches, nested values and timestamps, v3 | 256 | 2048 bytes |
| `restore-native-kafka` | `nervix-server` Streaming native Kafka encoding preserves the complete ordered current topic, partition and next-offset set, including the optional applied-start generation, empty sets and integer boundaries. | ordered Kafka topic, partition and next-offset sets, including empty and boundary values, absent or present Kafka applied-start generation, v5 | 256 | 2048 bytes |
| `backup-native-lifecycle` | `nervix-server` A stored native lifecycle streams the archive record of every branch's typed key, nested arrays, vector kinds and float bits, last ingestion and incarnation in LRU order, with its revision and identity. | stored lifecycle states with branch values, revisions and LRU order, v4 | 256 | 2048 bytes |
| `backup-native-kafka` | `nervix-server` A stored native Kafka offset checkpoint in any partition order, with partitions recorded more than once, streams the archive record of every partition in topic and partition order with its last recorded next offset, and the exact optional applied-start generation, as the runtime's own decoding reads them. | stored offset checkpoints with repeated partitions in arbitrary order, absent or present Kafka applied-start generation, v4 | 256 | 2048 bytes |
| `remote-correlation-lifetimes` | `nervix-server` A sequential reference model predicts every record and admission identity and outcome after bounded single/batch/detached registration, admission closure and reclamation, single/bulk admission, Alive and ordered parked/resumed progress, terminal replies, clear, every silence sweep, memory refusal and shutdown; exact per-receiver expirations, stale and foreign-row rejection, FIFO reuse before fresh claims, ever-claimed scan positions, tracker progress and complete capacity/relay-memory reclamation agree at every step. | bounded registration, admission, reply, sweep, reclamation and shutdown sequences, v2 | 256 | 256 bytes |
| `relay-admission-owners` | `nervix-interconnect` Bounded reservation, body, admission, rejection, cancellation, peer epoch and owner ending sequences preserve every terminal verdict and reclaim configured capacity. | reservation, body, admission, rejection, cancellation and owner-ending sequences, v1 | 256 | 64 bytes |
| `relay-channel-watermark-turnover` | `nervix-interconnect` Channel openings, decided outcomes whose deliveries return at once or later, continuations asked for in the receiver's grant order, delayed repeats, idle sweeps and late outcome returns through a peer owner whose three watermarks are fewer than its turnover never admit one delivery identity twice, always admit a channel opened after every forgotten one, give a channel whose latest outcome is decided its next position before that outcome's delivery returns, and keep the retained watermarks within the budget. | channel openings, decisions, delayed returns, continuations and sweeps beyond watermark capacity, v2 | 256 | 96 bytes |
| `runtime-window-sketch-panes` | `nervix-server` Admitting generated rows in runs and stepping past the oldest leaves every distinct, quantile and top-k pane equal to an empty sketch that admitted exactly that pane's retained rows in order, with no pane lacking a contributing row. | generated row runs, pane expirations and distinct, quantile and top-k sketches, v1 | 256 | 2048 bytes |
| `kafka-partition-offsets` | `nervix-connector-kafka` Generated acknowledged and rejected positions fold to each topic partition's latest offset and earliest rejected message, in topic and partition order, whatever their order and repetition, and a position with no message before it is refused. | acknowledged and rejected topic-partition positions, reordered and repeated, v1 | 256 | 1024 bytes |
| `sentry-event-models` | `nervix-models` complete event Model equality through JSON and archive encodings, retaining authored platform presence | omitted, empty, Unicode and arbitrary bounded platform strings, v1 | 128 | 128 bytes |
| `sentry-prepared-envelopes` | `nervix-connector-sentry` One prepared Sentry envelope preserves every supplied event field, event-id spelling, timestamp and platform, fills only absent defaults, frames the complete event with its exact byte length, and every HTTP attempt sends those identical prepared bytes. | bounded event fields, IDs, timestamps, platforms, configured platform defaults and repeated HTTP attempts, v2 | 128 | 128 bytes |
| `ingestor-describe-envelope` | `nervix-server` A current runtime ingestor report, with its readiness, quiesce state and counters, transport redeliveries, decode rejection count and last decode rejection with its reference, codec, Kafka or IPv4 and IPv6 syslog position, sorted fields, message and instant, oversized payload count, backpressure, transient error, reconnect state, Kafka domain offsets, client producer gauges and ordered metric lines, survives the describe envelope conversion and the production command response codec unchanged. | current ingestor reports with counters, positions, diagnostics and ordered metrics, v4 | 256 | 2048 bytes |
| `dns-operation-budget` | `nervix-dns` An operation budget never grows back as time passes, what is left of it is the total less the elapsed time and never less than nothing, the lookup's share never outlives what is left or its own bound, and the equal shares each address attempt takes of what is left together never outlast the budget or waste a nanosecond per answer. | total and lookup durations, elapsed time and address-attempt counts, v1 | 256 | 256 bytes |
| `endpoint-acknowledgement-body` | `nervix-server` The one-way endpoint reply projection encodes exactly the complete existing client outcome class and cause object, with fixed labels, bounded bytes and no payload or diagnostics; local refusal and downstream rejection share the same cause. | current client outcome classes and cause objects without payloads, v1 | 256 | 64 bytes |
| `websocket-acknowledgement-values` | `nervix-connector-websockets` Every complete bounded outcome/cause string object survives the production acknowledgement encoder and decoder in JSON, YAML, CBOR and protobuf; RAW carries an exact JSON representation. | bounded outcome/cause objects across JSON, YAML, CBOR, protobuf and RAW, v1 | 256 | 64 bytes |
| `jaq-yaml-string-values` | `nervix-jaq` Complete bounded current objects preserve every string key and value, whitespace, nested member order, array element, null and scalar through the production YAML writer and reader. | nested objects and arrays with arbitrary string keys, values and scalar kinds, v1 | 256 | 64 bytes |
| `websocket-retained-metadata` | `nervix-server` Both current WebSockets data-frame kinds, the exact immutable connection header snapshot, every header value and duplicate-header order survive the production quiesce retain/replay boundary without copying the snapshot. | both data-frame kinds with ordered duplicate headers and retained snapshots, v1 | 256 | 32 bytes |
| `ingest-metadata-projection` | `nervix-server` Every value a message hands over for its entity's declared metadata fields, absent or of each declared type at its boundaries and borrowed or rendered, survives the connector's retained copy and the quiesce buffer's retain and replay with the message's ordered headers, shared header snapshot and transport position; a group builds exactly one column for each field its programs read, in declared order whatever order and repetition they name it in, holding every message's value with its validity, keeps every message's ordered headers and visits no value when no field is read. | a declaration of every metadata type, repeated and reordered read sets, and messages with absent, borrowed and rendered values, ordered headers and Kafka or syslog positions, v1 | 256 | 1024 bytes |
| `websocket-client-config` | `nervix-models` Every bounded valid ws/wss configuration preserves the complete endpoint, connection and ping durations, effective idle deadline, handshake capacity and incoming message/frame limits while applying the declared TLS pairing rules. | valid ws/wss endpoints, deadlines, capacities and TLS pairings, v2 | 256 | 32 bytes |
| `websocket-upgrade-headers` | `nervix-models` Every bounded canonical upgrade-header collection, with or without a subprotocol offer already written as unique tokens joined by commas, preserves all names, values and order through the production validated representation; malformed and canonicalizing cases have separate owner tests. | bounded canonical header names and values in order, with an optional canonical offer of up to three tokens, v2 | 256 | 32 bytes |
| `websocket-subprotocol-offers` | `nervix-models` Every offer of unique subprotocol tokens, written with any spaces or tabs around its commas, validates to exactly its tokens in written order joined by commas alone. | four unique subprotocol tokens with spaces, tabs or nothing around each comma, v1 | 256 | 32 bytes |
| `node-settings-command-line` | `nervix-server` Current valid node settings of every group, with the Raft timings ordered in whole milliseconds, the drain shorter than the shutdown and the memory watermarks paired and ordered, render as the server's options and read back through the same options and validation unchanged; every duration and byte quantity help prints reads back as the same value. | valid settings across groups, ordered Raft timings and paired memory watermarks, v2 | 256 | 512 bytes |

Each target declares its Cargo manifest. Product targets use the root workspace; the five `typed-*`
compiler-tooling properties use `tools/nervix-lint/Cargo.toml`. The shared runner discovers declared tooling workspaces
from root metadata and uses the selected manifest in ordinary, sanitizer, replay and qualification
commands. Compiler executions are fixture checks, not work done in each fuzz iteration.

A target names the test executable that holds its property: its package's library unit tests,
`lib`, the unit tests of one of its binaries, `bin:<name>`, or one integration test,
`test:<name>`. The web console's backup dialog belongs to the console's browser binary, whose unit
tests compile natively, so its properties are `bin:nervix-web-console` targets; Bolero and the
generators stay development dependencies outside the browser's graph.
Before instrumenting a target, the runner reads Cargo metadata to resolve its package, target kind,
name and source root. It selects the emitted executable by that identity, including integration
tests in folders, a binary's unit tests beside a library of the same name, and explicitly
configured library or test paths. A missing or ambiguous Cargo target or executable fails the run
before the campaign starts.

The sanitizer shards budget instrumented builds separately from each target's bounded campaign.
Each shard job has a four-hour limit; per-build, per-case and per-campaign deadlines remain
enforced by the shared runner. Validation uses a 16-CPU runner and a two-hour limit
to complete Clippy and the declared compiler synchronization matrix from a cold cache. Bolero
inventory and compiled discovery run through the dedicated Bolero commands and workflow.
The native extra-checks job has a 16-CPU runner too and the same two-hour limit for ordinary
instrumentation and per-mode primitive conformance; the Shuttle and Loom suites run in jobs of
their own.

The inventory also records exact full test names, required features, corpus paths, case timeouts
and each invariant. Its corpus path is Bolero's source-adjacent
`__fuzz__/<test-name>/corpus` directory. Ordinary tests replay those files before
randomized cases; checked-in corpus inputs are regression cases. Each target starts with
current-domain boundary seeds. Generated fuzz corpus and crash files live in retained artifacts
under `target/bolero/runs`. Promote a verified minimized failure to the checked-in corpus
when it remains a meaningful regression for the current domain. Breaking shape changes replace
obsolete seeds.

The [storage representation coverage map](https://github.com/nervix-io/nervix/blob/main/tests/storage-representation-coverage.md)
ties the consensus, registry, runtime-state and resource store targets to the stored shapes they
cover, their complete oracles, the projections their keys make and their corruption boundaries.
A filter selects every target whose ID or test name contains it, so `just test-bolero runtime-state`
runs the runtime-state store targets and `just test-bolero storage::` the consensus and registry
storage targets.

The [client representation coverage map](https://github.com/nervix-io/nervix/blob/main/tests/client-representation-coverage.md)
ties the client targets to current schema families, complete oracles, ownership and malformed-input
boundaries. Request, reply and event coverage checks compare generated families with the schema's
union declarations, so adding a family requires extending its generator.

The [Arrow, codec and interconnect representation coverage map](https://github.com/nervix-io/nervix/blob/main/tests/arrow-representation-coverage.md)
ties the relay body, producer batch, client emitter delivery, WASM generated pool, schemaful codec,
JAQ-native and protobuf codec number, routed relay payload, relay wire message, client producer
link frame and client consumer stream frame targets to their oracles, each codec's value domain and
the contract boundaries they record. Its server targets share one generator of schemas and batches
over every current field type and one logical oracle that compares batches cell by cell with floats
by their bits. `just test-bolero codec-` runs the four codec targets, `just test-bolero jaq-native`
the numbers a JAQ program and its native formats carry, and `just test-bolero client-producer-link`
and `just test-bolero client-consumer-stream` the frames a forwarded client producer and consumer
travel in between nodes.

## Generated Domains

The [WASM representation coverage map](https://github.com/nervix-io/nervix/blob/main/tests/wasm-representation-coverage.md)
registers protocol messages, native host conversions, SDK Arrow and snapshot conversions, archive
state descriptors and stored checkpoints. `just test-bolero wasm` selects their seventeen targets;
`just test-wasm` also builds and exercises the supported Rust and Go reference guests. The malformed
targets separately cover header lengths, offsets, vector counts, tags and invalid descriptor fields.
Shared guest-message generators live only in the `nervix-arbitrary` test harness; guests and product
libraries acquire no generator dependency. These properties preserve values and retain the existing
owners of callback, ACK, checkpoint and recovery semantics.

`nervix-arbitrary` supplies bounded vocabulary values and entropy; properties compose these into
their owning frame or Arrow representations in test code. It reads a property's bytes
as a sequence of bounded choices, so the same bytes always build the same value and a saved failure
replays exactly. It is a harness outside the layer order: it depends only on the vocabulary, and
states what Plexus can spell as a rule over values instead of calling the parser, so the vocabulary's
own properties use it without a language dependency. No product code names it.

For native client producers and consumers it draws the window a client asks for and is granted,
with counts at one, at the bound the vocabulary declares and at the largest count their type holds,
a producer's whole description, every outcome of a submitted batch with each cause it names, and
physical durations that land on zero, one nanosecond and the longest duration as often as anywhere
else. It also lists one field of every declared type, each scalar, a list of it and a fixed-length
array of such lists, across the four combinations of optional and sensitive, for a property that
must carry every type on every run.

A generator draws from one of two domains:

- **Plexus** holds the values canonical Plexus spells and reads back as themselves. The language
  properties draw from it.
- **Vocabulary** holds every value the vocabulary types hold, including states Plexus has no
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
node-wide. A name of any kind may spell any Plexus keyword, statement and expression keywords alike,
and a field, UDF or function name inside an expression or a route construction may also hold a `-`,
`~` or `.`, or begin with a digit, which canonical Plexus writes between backticks. Names reach both
length limits, and counts reach the largest value their field holds.
The language properties also sweep sixteen deterministic byte sequences through every Model
family, every emitter entity and every statement form on every ordinary run, so none is left to the
random cases.

The Plexus Postgres generator keeps `DO UPDATE` targets nonempty and leaves at least one `VALUES`
column outside the conflict target, as the grammar requires. The vocabulary domain also generates
updates whose target includes every mapped column, so archive equality still covers those values.

Relay capacities in both domains reach `u64::MAX` on native 64-bit targets. Archived native counts
use the vocabulary's fixed-width [count representation](./typed-states.md#archived-counts).
Dedicated count properties assert complete current-record equality at zero where permitted,
`u32::MAX + 1`, `u64::MAX`, and generated values. The public scenario **Relay capacities keep every
bit through replication and a full cluster restart** checks creation and alteration at both positive
boundaries in one-node and three-node clusters, including a follower read and full restart.

Rejection targets start from valid canonical text and edit up to three characters, inserting,
deleting or replacing delimiters, quotes, comment markers, line endings, digits and non-ASCII
characters. The result is mostly no longer Plexus and sometimes still is, so both the rejection and
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

- Canonical Plexus writes a negative literal, `-0.0` included, as `-` applied to its magnitude, and
  reads it back that way. NaN and the infinities have no spelling, and rendering them fails with a
  typed error. The Plexus domain therefore holds only non-negative finite literals.
- Outside an expression or a route construction, a generated name is one lower-case ASCII
  identifier, because canonical Plexus writes a name holding a `-`, `~` or `.`, or beginning with a
  digit, between backticks only inside one. The name properties cover the whole name rule on each
  name type directly. In the Plexus domain a relay is never named `message` or `branch`, and a
  placement member is never named like an `ALTER` operation keyword, because Plexus reads those words
  there as its own.
- `DROP` has no form for branches, generators, hash maps, signaling protocols, WASM processors or
  window processors, an HTTP emitter has no `BATCH` clause, a correlator's filter has no
  spelling, and a Postgres `ON CONFLICT DO UPDATE` whose target names every mapped column is
  rejected, because it leaves no column to update. Only the vocabulary domain generates those
  states, for the archive properties.
- A time rate's text, JSON and archived forms keep every bit. The workspace builds `serde_json`
  with `float_roundtrip`, which reads a decimal as the float nearest to it; its default reader is
  best-effort and lands one unit in the last place away for a tenth to a quarter of the decimals
  that computed floats print as.

## Behavioral Properties

Registered properties also hold behavioral contracts against independent references:

- **Operation sequences.** `expiry-map-operations` drives the expiry map with weighted operation
  sequences against an `IndexMap` model and a ledger that counts every stored value's drops. Miri
  runs the same property through `just test-expiry-map-miri`, which lifts Miri's isolation so
  Bolero reads its checked-in corpus, with four random cases of at most 512 input bytes; every step
  still runs and checks its own answer, and the complete state is compared every sixteenth step and
  at the end. Mutation testing runs the property in every mutant through
  `just test-expiry-map-mutants`. `connector-confirmation-window` publishes generated hand-overs,
  refusals, pauses and scripted broker answers through the brokered emitter confirmation window on a
  paused clock, and compares the instant the publish ends, the answers the window holds after each
  hand-over, the records it settles and the failure that ends it with a reference model of the
  documented window rule computed in whole milliseconds.
- **Numeric references.** The decimal rounding kernels are compared with the exact decimal
  expansion of each value and with a 128-bit integer reference. That a value holding no more
  fractional digits than the count is its own rounding is checked apart from the rounded results.
  `approx-into-exact` holds the lossless round trips, and `approx-into-rounding` compares every
  rounding and truncating conversion with a bit-level IEEE 754 reference: round to nearest, ties to
  even, and an infinity past the largest finite value, so `u128::MAX` narrows to an `F32` infinity.
- **Kernel differentials.** SIMD targets compare every level the host supports, the baseline and
  the forced scalar fallback with a complete scalar oracle, tails, offsets and unused bits included.
  A compensated float sum is checked against an exact sum of Shewchuk partials within the bound of
  compensated summation, because the window contract promises no bit equality across run partitions.
- **JSON rendering.** `columnar-json-rows` renders generated logical values on its own: strings
  through `serde_json`, bytes through a reference base64 encoder or octet escaping, and instants as
  RFC 3339. A float must be a JSON number of at most its type's significant digits that reads back
  to its exact bits; its exponent's spelling is the writer's, which writes `1e172` where `serde_json`
  writes `1e+172`.
- **Encoded sizes.** The bounded writer, codec row and container encodings, emitter subdivision and
  packing, row request division, row preparations, retained retry payloads, SQS records and batch
  requests, MQTT PUBLISH packets and Postgres inserts are checked against the final bytes, framing,
  headers and escaping included where the size contract includes them. Generated measures grow and
  shrink with a candidate, so no property assumes that a larger candidate encodes larger.

Lifecycle, acknowledgement and cancellation races keep their production-owner Shuttle checks.
Kafka, NATS and Pulsar enforce their size limits inside their client libraries, and the MySQL,
MongoDB, ClickHouse and OTEL measures share the division rule while their own measurements keep
example tests.

## Commands And Enforcement

Server properties embed the real web console assets. The Bolero recipes build them before
discovery, requiring Trunk and the repository Rust toolchain's `wasm32-unknown-unknown` target.
Both CI modes install those prerequisites.
Asset builds stage Trunk's complete output and preserve byte-identical published files, including
their timestamps, so preparing assets again keeps the native server's compiled artifacts reusable.

On a shared host, a cold sanitizer build can exceed the runner's build deadline.
`just prepare-bolero <id>` prepares one exact registered target through the same instrumented
build path that runs its campaigns. `CARGO_BUILD_JOBS=1 just prepare-archive-counts-fuzz` uses that
command for the declared server library shared by the registry and window count properties.
Then run the registered campaigns with `just fuzz`. Preparation records its own build log and
metadata with a 7,200-second compilation deadline; it does not count as a completed fuzz campaign.
An expected sanitizer or randomized skip contributes no execution or coverage evidence. The Check
workflow's validation job and the Bolero randomized job retain their separate scopes and limits.
Check calls the reusable Bolero workflow with its exact tested SHA, event and label snapshot.
Manual Bolero runs execute randomized checks and record a deliberate sanitizer skip.

### Documentation-only Selection

Before starting ordinary suites, Check's `test-selection` job proves which source it selects for.
Its checkout must be the commit the event tests: a merge whose first parent is its base and whose
second parent is the event's exact head. The event and the tested merge need not name the same base.
GitHub records a pull request's base as its base branch's tip when the pull request opens, and moves
it forward only to the newest base commit that a later head merges, while it builds the tested merge
on the base branch's tip whenever the head changes. A push made after the base branch advanced,
whose head does not merge the newest base, therefore tests a merge whose base is newer than the one
its event names, and label events that follow name the same recorded base.

Selection accepts the tested base when it is the event's base, or when the event's base is one of
its ancestors. The checkout holds only the tested merge and its parents, so to decide the second
case selection fetches the tested base's complete history as commits, without trees or file
contents, and then answers from that local history alone. When the tested base is the event's own
base, it fetches nothing. A checkout that is not the tested merge, a tested commit that does not
merge the event's exact head into one base, an event base that the tested base does not descend
from, such as a newer base or one from another branch, and a failed history fetch all fail
selection, so none of them can authorize an exclusion.

Selection then reads every changed path between the tested base and the tested merge: what the
merge changes on the base it was tested against, never the base branch's own advance. Rename
detection is disabled so deleting or moving product code into a prose location still requires
execution. Only `AGENTS.md`, `README.md`, Markdown under `docs/`, and Markdown under
`.agents/skills/` qualify for the documentation-only exclusion. An empty diff, code, tests, build
configuration, workflow definitions, scripts and unknown paths require suites; `no-docker` supplies
no evidence about this choice.

A verified documentation-only selection excludes the ordinary tests, scenarios, extra tests,
diagnostic evidence and their coverage aggregation. It excludes the entire reusable Bolero call
unless the event carries `bolero-random` or `fuzz`. Validation, the script tests of the `checks`
job and the conventional PR title check remain independent and mandatory. Label-selected mode
checks remain selected by their labels; `bolero-random` still requires the ordinary properties and
`fuzz` the complete sanitizer inventory even for documentation. Label addition and removal compute
a fresh selection from that event's label snapshot.

The `ci-test-selection` artifact records run, attempt, job, event and action, the tested merge, the
event's base, the tested base and the head, every changed path, labels and each ordinary/Bolero
requirement. Two different bases record an advance that selection proved. Its `selected` verdict
records a source decision, never successful property execution or coverage. Actual job conclusions
remain in the workflow run. The reusable workflow's `bolero-selection` record is produced only
after all of its required execution succeeds; failed, cancelled, skipped or missing required
producers still fail its gate.

A rerun of `test-selection` reads the original event and tested merge and proves the same ancestry,
so it reaches the same selection after the base branch advances again. A validation-only rerun uses
the original event and tested commit. Its ordinary producer selection remains in the caller, outside
the reusable workflow's `always()` gate, so deliberate non-code exclusion never becomes a cancelled
property campaign to excuse. Historical attempts retain their actual cancellations; this policy does
not turn an interrupted required campaign into success. `just test-ci-selection` exercises source
identity, advanced, stale and tampered bases, stale heads, other merges, shallow checkouts, mixed
changes, renames, label events, reruns and strict gates; `just coverage-ci-selection` measures that
Python contract without launching product campaigns. Selection does not claim a model-checker or
public Cucumber result.

The selection tests keep Git maintenance in the foreground in every temporary repository,
including the bare origin and shallow checkouts. Those repository-local settings also cover
selection's history fetches and the origin's receive process, so waiting for each Git command
finishes its writers before temporary-directory cleanup. A regression forces commit-graph
maintenance and uses Git's process trace to check that it never detaches; cleanup errors remain
test failures.

### Sanitizer Shards

A single job running every sanitizer campaign outgrew its limit as the inventory grew. With all
159 targets it took 209 to 258 minutes: discovery took 27 of them, the two server sanitizer builds
with one Cargo build job 42, and the campaigns 132. CI therefore divides the inventory into shards
that run in parallel, each in a job of its own:

- A harness, one package's test target with one feature set, is never split, so a shard builds
  only the harnesses it runs. The inventory's `prepared_targets` names one target of each harness
  too large for a campaign's build deadline: today the server library with its empty and `testing`
  feature sets.
- The plan deals the harnesses to the shards, the longest estimate first, each to the shard that
  holds the least estimated time. A harness's estimate is its build, 30 minutes for a prepared
  harness and 3 for any other, plus the inventory's PR duration and 15 seconds for each of its
  targets. The plan depends only on the inventory and the shard count, so every runner computes the
  same one, and `just fuzz-plan <count>` prints it.
- Each shard job discovers every registered target, prepares its own declared harnesses with one
  Cargo build job and the 7,200-second preparation deadline, and runs
  `just coverage-bolero 30 <number>/<count>`: the runner tests, its share of the campaigns and the
  complete failure qualification. Only the server preparation is held to one build job; discovery
  and library builds use every core. A shard samples CPU use, steal time and used memory every five
  seconds and adds the summary to its job summary. Samples also retain guest-reported MHz,
  UTC and CPU/memory/I/O pressure. Every build step first records the shared short CPU, memory
  access and filesystem calibration, with JSON and Markdown artifacts. The
  [runner diagnostics contract](integration-test-lifecycle.md) owns its bounds, units and limits;
  these measurements never change campaign budgets or decide a property verdict.
- `bolero-fuzz-combined` downloads every shard's completion evidence, without the raw profiles, and
  runs `just combine-bolero-fuzz`. It accepts only shards 1 to N of one plan, from one workflow run
  and attempt at the tested revision and inventory. Each must be complete, must have run exactly its
  planned targets and must hold a complete record for each of them and a report. Together they must
  select every registered target once. The job then writes the combined record and the merged Rust
  source report.

Each shard job allows 240 minutes. At the measured 205-target revision, the three-shard plan
estimated at most 114 minutes per shard before discovery, failure qualification and uploads.
Funding 1.5 times that estimate left 69 minutes for those surrounding operations and cold-build
variation, including the separately bounded server preparation. Recompute the plan from the
current inventory with `just fuzz-plan 3`; historical estimates are not a current campaign
verdict. `bolero-gate` requires every shard and the combined proof when the PR carries `fuzz`, and
requires all of the sanitizer jobs to be skipped otherwise; it holds the randomized job to
`bolero-random` the same way. `tests/release-evidence.toml` holds a qualifying revision to the same
checks, every shard of the matrix included, and to the randomized job under its own label.

The fuzz profile uses one codegen unit, optimization level two and debug level one. The full
server package uses optimization level one and no debug output to keep its instrumented build
within the compilation budget. Both profiles retain debug assertions, overflow checks,
AddressSanitizer and libFuzzer coverage feedback. Live campaigns also use Rust source
instrumentation; ordinary and release builds retain their own settings.

An instrumented build has a 1,800-second deadline. On a loaded development host,
`BOLERO_BUILD_TIMEOUT_SECONDS=7200 just fuzz <id> 30` grants compilation more time. The override
must be a positive integer and affects only compilation; the inventory's case limit and the
requested campaign duration remain enforced. CI uses the default deadline. The runner prints the
effective compilation deadline alongside the build command and retains timeout evidence.

The campaign process has a deadline of its requested duration plus the greater of 30 seconds
and three case timeouts. The runner passes `-print_funcs=0` to libFuzzer for preparation,
campaigns, minimization and failure qualification. This disables the `NEW_FUNC` progress
messages that otherwise start expensive symbolization on large sanitizer binaries. Crash
reports and exact replay retain symbolization where a symbolizer is available; sanitizer CI
exposes its installed LLVM symbolizer under the executable name libFuzzer discovers.

Ordinary execution prepares each selected package, harness and exact feature set once with
`cargo test --no-run`, under a separate 1,800-second compilation deadline. Discovery can require
the union of a package's features; its binary does not prepare a target with a different declared
feature set. Each property still has a 240-second execution bound and must complete its exact
randomized iteration count and replay every checked-in corpus input. A failed preparation ends
the run before that harness executes any property. The randomized CI job reserves 90 minutes for
discovery, the declared feature builds and every registered target. Run 39649567371 took 47m10s;
1.5 times that measurement is 70m45s, leaving more than 19 minutes for publication and further
runner variation.

The root `rust-toolchain.toml` and `tools/nervix-lint/rust-toolchain.toml` declare `llvm-tools` for
their respective compilers. The sanitizer nightly installs its own `llvm-tools`; coverage export
uses the `llvm-profdata` and `llvm-cov` belonging to the compiler that produced each raw profile.
Neither a system LLVM binary nor another toolchain's raw profile can substitute. Every Rust build,
including doctests and sanitizer preparation, retains the configured kache wrapper.

Install the dated sanitizer nightly named in the inventory and the pinned CLI. Server properties
also require the native server build dependencies, including `protoc` for the Pulsar driver;
both ordinary and sanitizer CI jobs install the Protocol Buffers compiler, Trunk and the browser
target. Every compiled Bolero recipe builds the embedded web console before discovering the server
properties, using the same configured compiler wrapper as the product build.

```bash
rustup toolchain install nightly-2026-09-17 --profile minimal --component llvm-tools
just install-cargo-bolero
just help
just fuzz-list
just test-bolero
just test-bolero plexus-model
just build-bolero
just fuzz plexus-model 30
just fuzz-all 30
just fuzz-plan 3
just fuzz-all 30 2/3
just prepare-bolero-shard 1/3
just coverage-bolero 30
just qualify-bolero
just fuzz-replay plexus-model <saved-input>
just fuzz-replay-sanitizer plexus-model <saved-input>
just fuzz-reduce plexus-model <saved-input>
just combine-bolero-fuzz <shard-evidence> <output>
```

`just validate-bolero` compares the inventory with all workspace packages declaring
Bolero, scans their property macros within each owning Cargo package, and queries compiled targets
through filtered library and integration-test discovery with the inventory's declared features.
Source scanning stops at nested Cargo manifests and excludes Cargo build output; an authored module
named `target` remains part of its package. A property macro belongs to the innermost function
whose body holds it, read without comments and literal contents, so a helper function nested
before the macro is not taken for the property. It rejects missing, unregistered, duplicate,
ignored and zero-selected targets, plus
corpus paths different from Bolero's actual work directory. Discovery executes only
`bolero_` tests of the registered library and integration-test targets under Bolero's selection
mode; it cannot start the server's scenario harness. It reports discovered, selected, executed and
completed counts.
`just validate` and `just validate-ci` do not run Bolero checks. Bolero commands perform discovery
before their selected action, and the Bolero CI jobs own this check: the randomized job before its
properties and every sanitizer shard before its campaigns. A pull request carrying neither
`bolero-random` nor `fuzz` therefore runs no inventory check in CI. The inventory includes
server-owned Arrow properties, so Bolero recipes prepare the web console assets the server library
embeds through `just bolero-deps`. This compiles assets only; it starts no live service. Both Bolero
CI jobs install Trunk, the browser Rust target and native server build dependencies before compiled
discovery.
Console builds stage the complete real Trunk output and publish only changed assets. Identical
assets retain their file identity and timestamps, so repeated preparation preserves an already
instrumented server build; failed builds leave the published assets intact.

`just test-bolero` requires the configured randomized-case count and checked-in corpus replay for
every selected property, checking Bolero's reported input counts. `just build-bolero` compiles every
ordinary harness the same selection runs, as `just test-bolero` builds it, and runs no property; the
randomized CI job builds this way first and publishes its kache cache before the properties run, so
a run that fails or reaches the job's time limit keeps what its build stored. Fuzz runs use real
libFuzzer coverage with AddressSanitizer, bounded input length and per-case timeout. The fuzz
profile enables optimization, debug information, debug assertions and overflow checks. Builds use
the pinned nightly and keep the configured kache wrapper. Modeled execution features are excluded;
Loom, Shuttle and Turmoil remain independent build invocations. The server package uses the current
profile's lighter optimization and omits debug output for its single codegen unit; sanitizer
instrumentation, coverage, debug assertions and overflow checks remain enabled. The pinned
cargo-bolero CLI does not expose a Cargo test-harness selector. Each instrumented build therefore
records a run-local Rustup adapter that forwards the declared `--lib`, `--bin` or `--test` selector
to its Cargo test calls and forwards toolchain/version queries unchanged. It preserves the
toolchain, features, sanitizer flags and compiler wrapper. Library properties compile and execute
only their registered harness; they do not compile or invoke the server's Cucumber executable. The
adapter selects the pinned compiler through `scripts/bolero_rustc.py`, still beneath the configured
kache wrapper. For `proc-macro2` with `span-locations`, that compiler adapter removes only its
generic `cfg(fuzzing)`: the dependency otherwise deliberately replaces source locations with zero,
violating the source parser's contract and collapsing distinct findings. Sanitizer, coverage and
libFuzzer flags remain intact, and every other crate keeps its configuration. The adapter's content
digest separates build directories. The source-location property and saved regression corpus assert
the same exact locations in ordinary and sanitizer execution. On a shared host, a cold server build
can exceed the campaign runner's 30-minute compilation limit. `just prepare-bolero <id>` prepares
that same pinned, scoped sanitizer build with a two-hour limit and retains its log and preparation
metadata. It does not run a campaign or record fuzz completion. `just prepare-archive-counts-fuzz`
uses this shared path for the declared registry library harness. `just prepare-bolero-shard
<number>/<count>` prepares the declared harnesses one shard runs, as each CI shard does before its
campaigns. The target must subsequently pass `just fuzz <id>` with the ordinary build and case
limits; CI campaigns continue to use that bounded runner.

PR CI runs each Bolero mode through Check's reusable Bolero call only when the PR carries its
label. The sanitizer libFuzzer shards run with `fuzz`, with 30 seconds of engine time per target,
and the ordinary randomized/corpus job runs with `bolero-random`. Every job uses the native
[CI linker](./developing-nervix.md#validation-and-tests), including sanitizer builds that supply
their own compiler flags. Adding or removing a label reevaluates the jobs, and a push to a labeled
PR runs them again. Manual workflow runs execute the ordinary properties. Job limits
reserve additional time for compilation, artifacts and cleanup. A cache may seed a campaign but
cannot skip a target or replace checked-in regressions. An empty selection, timeout, engine
failure, sanitizer finding or property failure fails the job.

`fuzz` is the preferred Bolero evidence in CI. Its shards discover the inventory, run a
coverage-guided sanitizer campaign of every registered target from its checked-in corpus, measure
the Rust source those campaigns execute and qualify the runner's failure paths. A pull request whose
diff requires the ordinary suites runs each product property there as an ordinary Rust test that
replays its checked-in corpus, whatever its labels; the five `typed-*` properties of the separate
`tools/nervix-lint` workspace run as ordinary tests in `just test-typed-ratchet`, which CI runs for a
pull request labeled `extra-compiler`. The randomized job adds the run of each harness with its
exact declared features and the check that every property ran the inventory's case count and
replayed its whole corpus. Label a pull request `bolero-random` only when its task requires
that run, such as a change to the randomized run itself, its runner or its job, or a release
qualification, whose register requires it. A new or changed target is checked locally with
`just test-bolero <id>`; in CI, label its pull request `fuzz`.

Each fuzz run retains its corpus, crashes, log and metadata with revision, target, domain version,
toolchain, features, flags, result and exact-input reproduction commands. Retain failure
inputs before cleanup. `fuzz-replay` stages saved bytes in the target's actual crash
directory and runs the ordinary assertion with zero randomized cases. A sanitizer finding passes
there, because only the instrumented build detects it: `fuzz-replay-sanitizer` builds the target's
instrumented harness and runs the exact saved bytes once, and a run's metadata records both
commands. `fuzz-reduce` uses libFuzzer crash minimization, verifies that the minimized bytes still
fail the instrumented target and records whether the ordinary assertion fails on them as well.
A saved input inside a run directory replays only while it matches the digest its run's metadata
holds. A missing, unreadable or foreign record, or changed bytes, is a corrupt replay record and
is refused; a copy outside the run directories carries no record and replays as given.
A random seed reproduces one generated case, not an entire entropy-driven campaign.

`just qualify-bolero` proves that every way a run must fail does fail, and keeps a
`qualification.json` naming each case, its requirement, what it observed and its evidence:

- A property mismatch fails the campaign and saves its input, which then minimizes and replays.
- An AddressSanitizer heap-buffer-overflow fails the campaign with its report, and the
  instrumented replay of the saved input reproduces it.
- A case past its timeout, an engine that cannot start and a campaign naming no compiled target
  fail.
- Compiled discovery of generated packages accepts an exact registration and fails on an ignored,
  an unregistered and a missing property.
- A filter, a shard plan or a campaign that would run nothing fails.
- An input that differs from its run record, and one whose record cannot be read, are refused.

The deliberately failing targets live in the standalone `tests/bolero-qualification` package,
outside the workspace and the inventory. Every CI shard runs the qualification, so each shard's
runner proves its own failure paths.

## Live Rust Source Coverage

`just fuzz <id> <seconds>` and `just fuzz-all <seconds>` collect Rust source coverage from the
inputs the live libFuzzer process executes, including candidates it discards from its retained
corpus. `just coverage-bolero <seconds>` runs the command/artifact tests, all live campaigns and
failure qualification while collecting the separate Python runner report. It does not rerun the
randomized producer, which CI runs as a job of its own when `bolero-random` requests it.

The shared build path adds `-C instrument-coverage` to cargo-bolero's sanitizer flags, retaining
the pinned nightly, assertions, optimization settings, case/input limits and kache wrapper.
Encoded Cargo flags are rejected because they would override cargo-bolero's sanitizer flags.
Preparation and the zero-run build probe discard their counters. Ordinary discovery, exact replay
and deliberate failure qualification also discard counters; they cannot enter a product campaign
report or mix stable profiles with nightly profiles.

Each invocation allocates a fresh
`target/native-coverage/bolero-fuzz/fuzz/<nightly>/<attempt>/` directory; a shard's attempt name
ends in `-shard-<number>-of-<count>`, and its record names the shard and the digest of its plan.
Each selected target
has its own `targets/<id>/` directory there, with `profiles/%p-%m.profraw`, `executions.jsonl`,
`lcov.info`, `export.log` and `completion.json`. The runner records the exact executable returned
by the hashed fuzz build and its Cargo fingerprint, effective compiler flags and profile settings.
It holds the fuzz build directory through execution and export, so another collector cannot
replace those objects. The shared native exporter matches profile binary IDs to retained
executables and any required child objects, using the producing nightly's own `llvm-profdata`
and `llvm-cov`. Missing objects, absent counters and unreadable or mixed-format profiles fail
collection. LCOV paths are repository-relative; dependency, generated and harness files follow
the native collector's exclusion policy. Tests inline in an included source file retain that
file's attribution.

A successful target has a `complete` record only after a successful engine exit with nonzero
completed inputs and a successful source export. The aggregate report beside the invocation's
completion record is complete only after all selected registered targets complete. The record
retains the inventory digest and exact entries, selection and discovered/executed/completed
counts, revision and whether it was modified, compiler/LLVM identity, workflow run and attempt,
event and labels, campaign limits, input throughput and source attribution. Each target's record
also identifies the checked-in corpus the campaign started from, by its input count and a digest
of every seed's name and content. A focused invocation or a shard states its selection and never
claims the rest of the inventory ran; only the combined record of every shard does.

A crash, abort, deadline or interrupt stays failed or interrupted even if it left some counters.
The runner retains its original corpus, crash input, logs and incomplete evidence; it does not
export those counters as a successful campaign. Failure minimization and exact replay remain
independent qualification, with their original failure retained. No corpus replay substitutes for
the live measurement.

Each CI shard publishes `coverage-bolero-fuzz-<number>` with its live campaign reports, records
and raw profiles, `bolero-fuzz-evidence-<number>` with its records and aggregate report but no
profiles or per-target reports, `bolero-runs-<number>` for corpus/crash and qualification artifacts with the Python
runner report as `python.lcov`, and `runner-load-bolero-fuzz-<number>` with its CPU and memory
samples. `coverage-bolero-fuzz` holds the combined record and the merged report, which sums the
shards' line, function and branch counters because they ran disjoint campaigns, and
`bolero-selection` the gate's exact producer verdicts, including deliberate skips. Fuzz reports
remain separate from ordinary workspace coverage and its CRAP report. A sanitizer skip publishes no
fuzz coverage artifact.

Check's `coverage-full` job reads both artifacts. For a pull request carrying `fuzz`, it requires
the gate's successful sanitizer verdicts and the combined campaign of this run's shards over every
target of the tested inventory, and adds the campaign's lines to the
[full workspace union](./coverage-reports.md#the-full-workspace-union). Without `fuzz`, it requires
the recorded deliberate skip and refuses any fuzz report. It holds the randomized verdict to
`bolero-random` the same way: success when the label requests the job, and the recorded deliberate
skip otherwise.

## Adding A Target

Put the Bolero test beside its production owner, name it `bolero_...`, bound iterations
and input length, and assert the complete current-value contract. Register the package, full
test name, source, domain version, features, corpus, budgets and invariant in the inventory.
Check in current-domain seeds, run `just validate-bolero`,
`just test-bolero <id>` and `just fuzz <id> 30`, then run
`just validate`. Both CI jobs include the target in the same change; the shard plan places it with
its harness, or deals a new harness to a shard, so a target needs no shard assignment. Label the
pull request `fuzz`, so CI discovers the inventory and runs the target's campaign; `bolero-random`
is added only when the task requires the ordinary randomized run as well. A new harness
whose sanitizer build exceeds a campaign's build deadline joins `prepared_targets`. A public product
bug found this way first gets a failing focused reproducer and affected public Cucumber scenario
before its fix.
