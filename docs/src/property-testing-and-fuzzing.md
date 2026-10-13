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
| `client-requests` | `nervix-client-wire` complete client request equality | every current request family, identities, positions, schemas and opaque I/O payloads, v1 | 128 | 2048 bytes |
| `client-replies` | `nervix-client-wire` complete reply equality | every reply family, dispositions, ordered diagnostics and structured command metadata, v1 | 128 | 4096 bytes |
| `client-events` | `nervix-client-wire` complete event equality and retained graph ownership | every unsolicited event, discovery, clocks and opaque graph text, v1 | 128 | 2048 bytes |
| `client-streams` | `nervix-client-wire` complete stream equality and shared chunk ownership | all upload, download and restore roots and outcomes, v1 | 128 | 2048 bytes |
| `client-reply-transfer` | `nervix-client-wire` exact segmentation bytes, reply equality and rejected append isolation | single frames and ordered transfers, incorrect identities and duplicates, v1 | 128 | 256 bytes |
| `client-row-views` | `nervix-client-wire` complete retained Row views | all scalar cells, nested vectors, fixed arrays, nulls, redaction, branches and float bits, v1 | 128 | 512 bytes |
| `client-arbitrary-frames` | `nervix-client-wire` typed verification and decoding within budgets | arbitrary bytes at all eight frame roots, v1 | 256 | 16385 bytes |
| `client-frame-corruption` | `nervix-client-wire` typed verification and decoding within budgets | current frame truncation, bit, offset, length and size mutations, v1 | 256 | 64 bytes |
| `client-discriminators` | `nervix-client-wire` validate discriminants before accessing union members | all byte-valued request tags, v1 | 256 | 64 bytes |
| `client-arrow-rows` | `nervix-server` Arrow logical values and validity against production Row views | scalar columns, nullable and nested lists, arrays, selected rows, concrete branches, frame splitting and explicit redaction, v1 | 128 | 512 bytes |
| `client-arrow-selection` | `nervix-server` whole-input typed rejection without partial output | invalid row bounds, order, duplicates and branch alignment, v1 | 128 | 512 bytes |
| `client-binding-rows` | `nervix-client-ffi` complete column values and retained frame ownership | native scalar bits, states, string/bytes offsets, branch keys and nested cells, v1 | 128 | 512 bytes |
| `client-ffi-field-descriptors` | `nervix-client-ffi` field descriptor ABI equality both ways: a host's level-by-level description becomes the complete original field, and the field and level accessors read back its canonical description as the header's `nx_type` values | every scalar inside 0 to 32 `LIST` and `FIXED_LIST` levels of 1 to `INT32_MAX` elements, either nullability and sensitivity, names from the whole name rule spelled in any letter case, v1 | 256 | 2048 bytes |
| `client-ffi-field-descriptors-malformed` | `nervix-client-ffi` invalid-argument refusal that leaves the field list as the accepted calls built it | unknown types, lengths on scalar and `LIST` levels, empty and oversized `FIXED_LIST` levels, a 33rd list, refused names, elements without an open field, fields before the open one's element, an open last field, and reads past a field's last level or past the last field, v1 | 256 | 4096 bytes |
| `client-processor-choice-request` | `nervix-client-wire` processor choice request round-trip equality | current targets with relay context, search, page and identity, v1 | 128 | 32 bytes |
| `nspl-expression` | `nervix-nspl` expression render and reparse equality through a statement and the standalone expression, expression-list and route-construction readers | NSPL expressions of every form, v5 | 256 | 512 bytes |
| `nspl-model` | `nervix-nspl` canonical `CREATE` render and client and server reparse equality | NSPL Models of every family, client emitters included, v6 | 256 | 2048 bytes |
| `nspl-archive-model` | `nervix-nspl` complete ordered canonical Model equality through export, restore reads, reparse and re-export | cluster/domain archives with multiple ordered documents and every Model family, v7 | 128 | 4096 bytes |
| `nspl-statement` | `nervix-nspl` canonical statement render and client and server reparse equality | NSPL statements of every form, session-only forms included, v4 | 256 | 2048 bytes |
| `nspl-statement-text` | `nervix-nspl` edited statement text is rejected with located diagnostics or reads as canonical statements | edited canonical text, v4 | 256 | 2048 bytes |
| `nspl-expression-text` | `nervix-nspl` edited expression text reads as the same expression, or as none, through a statement and the standalone expression, expression-list and route-construction readers | edited canonical expressions, v3 | 256 | 512 bytes |
| `nspl-format-document` | `nervix-nspl-format` keeps statements and comments and is idempotent | documents with gaps, comments and either line ending, v4 | 256 | 4096 bytes |
| `nspl-format-text` | `nervix-nspl-format` refuses unparseable text or formats it keeping its statements | edited documents, v4 | 256 | 4096 bytes |
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
| `models-archived-models` | `nervix-models` Model and statement archive equality, resource-version widening and pinning | vocabulary Models and statements of every family and form, full-width counts included, v4 | 256 | 4096 bytes |
| `models-archived-counts` | `nervix-models` count-bearing Model, statement, transaction identity and WASM inspection archive equality | zero where permitted, `u32::MAX + 1`, `u64::MAX` and generated native counts, v1 | 256 | 16 bytes |
| `consensus-archived-counts` | `nervix-consensus` complete record equality through the production bounded storage codec | transaction commands, limits, positions, progress, failures, outcomes, plan and report headers with boundary and generated counts, v1 | 256 | 16 bytes |
| `registry-archived-models` | `nervix-server` complete Model equality through the registry's sealed storage codec | every vocabulary Model family with pinned resource versions and full-width counts, v2 | 256 | 4096 bytes |
| `runtime-window-archived-counts` | `nervix-server` histogram delayed-removal archive equality | current removals with arbitrary expiry and boundary and generated bucket indices, v1 | 256 | 32 bytes |
| `backup-record-manifest` | `nervix-backup` complete record and manifest encode/decode equality | all eleven current record kinds, lifecycle/catalog variants, every cut, full-width counters and ordered metadata, v4 | 256 | 4096 bytes |
| `backup-runtime-state-records` | `nervix-backup` complete runtime state record equality | guest/materialized descriptors and identities, empty/multi-partition offsets and ordered branch lifecycle with bit-exact nested fields, v3 | 256 | 4096 bytes |
| `backup-materialized-identities` | `nervix-backup` complete materialized descriptors and ordered scalar identity group equality | empty and one-to-four-group generations, unbranched and branched identities, nested typed fields, float bits and per-row watermarks, v2 | 256 | 2048 bytes |
| `backup-materialized-columns` | `nervix-server` exact-schema Arrow/native generation fidelity and stored checkpoint re-export | nullable, sensitive, nested and byte columns; complete headers, ordered identities and original stored Arrow bytes, v2 | 128 | 4096 bytes |
| `backup-branch-state-descriptors` | `nervix-backup` complete deduplicator and window descriptor equality | empty, unbranched and branched keyspaces and windows, model digests, row sequences and watermarks, group counts and delayed histogram removals, v1 | 256 | 4096 bytes |
| `backup-deduplicator-keys` | `nervix-server` typed Arrow key group and restored keyspace fidelity, and the streamed checkpoint byte-identical to the runtime's encoding of the same keys | every `DEDUPLICATE ON` key type with nulls, narrow integers, arbitrary float bit patterns, text, bytes, datetimes and list keys, repeated keys and bounded groups, v1 | 128 | 2048 bytes |
| `backup-window-groups` | `nervix-server` window input and argument group and native checkpoint fidelity with equal aggregates | up to 192 admissions and interleaved steps over nullable integer, string and boolean arguments, concrete multi-type keys with bit-exact finite floats and fixed-offset datetimes, every exact aggregate family and a delayed histogram percentile, bounded groups sealed whole or split under generated identity limits, v3 | 64 | 1024 bytes |
| `backup-complete-archives` | `nervix-backup` complete export/extraction, restore-read and deterministic re-export fidelity | cluster/domain scopes, resources included/omitted, every cut and supported state kind, empty/unbranched/multi-group materialized generations with valid Arrow columns, deduplicator key groups and window row and argument groups, synthetic raw payloads at tar block boundaries and long paths, v3 | 256 | 4096 bytes |
| `backup-malformed-records` | `nervix-backup` safe typed validation of current record headers and values | all eleven kinds, truncated/altered headers, invalid current fields and bounded arbitrary bytes, v3 | 256 | 4096 bytes |
| `backup-corrupt-archives` | `nervix-backup` rejection before verified restore contents are returned | changed lengths/digests/kinds, reordered/missing/extra sections, invalid records, materialized, deduplicator and window group metadata/membership, damaged Arrow sections, truncation and bounded arbitrary tar inputs, v3 | 256 | 4096 bytes |
| `console-pending-backups` | `nervix-web-console` pending backup entry and tab storage round-trip equality, with forgetting only by its own reference | any execution reference and query text, no domain or one in any spelling the name rule admits, recorded over an earlier record, v1 | 256 | 1024 bytes |
| `console-pending-backups-malformed` | `nervix-web-console` exact read-back or refusal of a tab's stored record, which a refusal forgets while other entries stay | missing, canonical, edited, upper-cased and drawn reference, query and domain entries beside an unrelated entry, v1 | 256 | 1024 bytes |
| `console-restore-drafts` | `nervix-web-console` restore draft lowering equality and existing-user option text | every `RESTORE` scope, source, mode, runtime state and lifecycle, domains in any spelling the name rule admits, every policy, v1 | 256 | 512 bytes |
| `backup-capture-wire` | `nervix-interconnect` complete capture/drain/inventory request and response equality and fetch request equality through each request's declared class limit | every drain action and counter, every success/refusal, empty and ordered inventories of all twelve kinds, zero/full-width lengths and counters, arbitrary digests and bounded Unicode paths, v1 | 256 | 1024 bytes |
| `backup-capture-wire-malformed` | `nervix-interconnect` typed decode refusal or complete stable re-encoding, with management/bulk reservations released | truncated, bit-flipped, overwritten and arbitrary capture/drain/inventory request and response bytes and fetch request bytes, v1 | 256 | 1024 bytes |
| `backup-section-mapping` | `nervix-server` total and injective capture-kind/archive-content mapping and distinct validated archive paths | all twelve section kinds over generated identities, group ordinals and branched/unbranched fingerprints, v1 | 256 | 128 bytes |
| `restore-installation-wire` | `nervix-interconnect` complete installation request and success/refusal wire equality through the bulk class limit | all actions, authority, all eight runtime state kinds branched and unbranched, zero/full-width lengths, arbitrary digests, payload and inventory, v2 | 256 | 128 bytes |
| `restore-installation-storage` | `nervix-server` complete staged checkpoint and publication record equality | authority, placement, revision, payload and inventory, v1 | 256 | 128 bytes |
| `consensus-state-records` | `nervix-consensus` complete recovered state equality through the state writer, a real keyspace and recovery, for a first revision and a second stored as changes | every record family with replaced and removed records, every transaction, outcome and command execution state, names across the whole name rule and boundary counts and times, v1 | 128 | 65536 bytes |
| `consensus-state-corruption` | `nervix-consensus` typed `InvalidState` recovery failure or a canonical state its readers can rely on | flipped, truncated, extended, rekeyed, copied, unknown and missing records of a stored revision, drawn from bytes of their own, v1 | 256 | 65600 bytes |
| `consensus-raft-records` | `nervix-consensus` complete Raft record equality through the storage codec, and keys that name their position or section | votes, log positions, entries of every command, state metadata, snapshot manifests and sections, v1 | 256 | 4096 bytes |
| `consensus-raft-records-malformed` | `nervix-consensus` typed storage failure or a value that stores back to the same record | arbitrary bytes as every Raft record, log key and section key, v1 | 256 | 4096 bytes |
| `registry-stored-models` | `nervix-server` complete Model equality through listing and keyed reads of a reopened registry, from its journal and from flushed tables | Models of every family in domains and with names across the whole name rule, v1 | 64 | 4096 bytes |
| `registry-stored-models-corruption` | `nervix-server` typed registry storage failure or Models whose own keys are exactly the stored keys | flipped, truncated, extended, recased, extended-key, copied and unknown records, v1 | 128 | 4096 bytes |
| `runtime-state-keys` | `nervix-server` exact decoded placement projection, and prefixes, index and chunk keys that reach no other placement | every state kind with schema, generation, Model kind and name and branch key, names across the whole name rule, v1 | 256 | 2048 bytes |
| `runtime-state-keys-malformed` | `nervix-server` typed storage failure or a placement whose own key is the stored key | damaged and arbitrary keys, v1 | 256 | 2048 bytes |
| `runtime-state-chunk-keys-malformed` | `nervix-server` typed chunk, storage-format or decoding failure, or a segment set whose placement key decodes canonically and whose chunk prefix and offset respell the key | damaged and arbitrary chunk keys, v1 | 256 | 2048 bytes |
| `runtime-state-staging-namespaces` | `nervix-server` the domain each key names after its namespace, and a maintenance walk that visits every namespace once in key order without passing another's key | placement, index and chunk keys of up to three domains in the initial namespace and restored generations of any value, v1 | 64 | 4096 bytes |
| `runtime-state-staging-namespaces-malformed` | `nervix-server` typed storage-format failure at the first key whose namespace does not decode, or the namespace and domain the key's leading bytes spell exactly | damaged and arbitrary keys, v1 | 128 | 2048 bytes |
| `runtime-state-identity-records` | `nervix-server` complete handoff and forced-recovery record equality with branch values by archived bits, and key identity | transitions with checkpoints of every placement kind, v1 | 256 | 4096 bytes |
| `runtime-state-identity-records-malformed` | `nervix-server` typed decode failure or a record that stores back unchanged | arbitrary bytes as every identity record, v1 | 256 | 4096 bytes |
| `runtime-state-snapshots` | `nervix-server` every owner's complete restored snapshot through its codec and the inline checkpoint envelope | Kafka offsets, branch lifecycles, deduplicator keyspaces with float bits and branch-aggregated metrics, v1 | 256 | 4096 bytes |
| `runtime-state-snapshots-malformed` | `nervix-server` each kind's typed decode failure or a value that stores back to the same payload | arbitrary bytes as every snapshot payload, v1 | 256 | 4096 bytes |
| `runtime-state-store-checkpoints` | `nervix-server` byte equality at the stored revision through both readers and every reader of a backup cut's listing, which lists exactly the domain's checkpoints in key order and by kind with their stored sizes, before and after the database reopens | up to three placements of one domain stored inline, as chunked streams or in a published restore generation at, beside and between the chunk boundaries, beside an unpublished generation and another domain, v2 | 64 | 4096 bytes |
| `runtime-kafka-offset-captures` | `nervix-server` every offset and topic schedule the replica's cancellable reader restores from a capture's native checkpoint, a written length within the capture's reserved bound, and a check before every native entry | up to three topics of three partitions at any offset, and three topic schedules, v1 | 256 | 2048 bytes |
| `runtime-kafka-offset-transfer-headers` | `nervix-server` exact revision, digest and payload from a checkpoint stream cut at every position and into generated chunks | advancing revisions beside the held and described ones, payloads of up to 128 bytes, v1 | 256 | 1024 bytes |
| `runtime-kafka-offset-transfer-headers-malformed` | `nervix-server` typed length or revision refusal, or the revision and digest the header spells, with exactly the bytes behind the header forwarded at every split | arbitrary streams, half of them shorter than the header, with revisions beside the one they spell, v1 | 256 | 1024 bytes |
| `runtime-window-snapshots` | `nervix-server` complete sealed window equality, and nothing for another branch lifetime | empty or up to 192 rows, concrete multi-type or absent branch keys, boundary sequences, nullable string arguments of zero or 2049 bytes and up to 96 delayed removals; generated 4–16 KiB column limits include IPC framing and split columns independently of the 1–32 KiB identity/typed limits, v2 | 128 | 2048 bytes |
| `runtime-window-snapshots-malformed` | `nervix-server` typed window snapshot failure or a window that seals back to the same bytes | sealed windows damaged once and arbitrary bytes behind the magic, v1 | 256 | 4096 bytes |
| `runtime-window-sealed-checkpoints` | `nervix-server` a window sealed in quota-owned pieces byte-identical to its resident encoding and restored completely | empty or up to 192 rows with bit-exact concrete branch keys or unbranched execution, boundary sequences, nullable string arguments of zero or 2049 bytes and up to 96 delayed removals under generated 4–16 KiB column and 1–32 KiB identity/typed limits, v2 | 64 | 2048 bytes |
| `runtime-arrow-bodies` | `nervix-server` relay body and sealed snapshot section Arrow IPC equality: exact schema, values, nulls and float bits; several sections refused by the one-section decoders and concatenated by the schema-free one | every field type with nulls, nested lists, masked list elements, empty and sliced views and boundary values, v2 | 128 | 2048 bytes |
| `runtime-arrow-bodies-malformed` | `nervix-server` typed refusal of a misframed or undecodable Arrow body, or a batch of the expected schema that encodes and decodes back to itself, without allocating from a declared length | relay bodies and sealed sections truncated, flipped, overwritten, shifted or trailed, and arbitrary bytes, v2 | 256 | 2048 bytes |
| `resource-store-layout` | `nervix-server` complete manifest and archive read-back for every installed version | names across the whole name rule and those spelled like the store's own directories, staging cleanup and removal of another version, v1 | 64 | 512 bytes |
| `client-producer-batches` | `nervix-server` producer batch equality from the client library's stream to the node's batch, with exact row and byte limits | every field type with nulls, nested lists and sliced views, v1 | 128 | 2048 bytes |
| `client-producer-batches-malformed` | `nervix-server` refusal with a batch defect or a batch within the ingestor's schema and row limit | damaged producer streams and arbitrary bytes, v1 | 256 | 2048 bytes |
| `codec-schemaful-rows` | `nervix-server` `WIRE JSON`, `WIRE CBOR` and `WIRE AVRO` row round-trip equality through one group builder | registry-accepted codecs over each format's value domain, generic and exact wire types and shuffled wire field order, v1 | 256 | 4096 bytes |
| `codec-schemaful-payload-groups` | `nervix-server` each payload decodes in a group exactly as alone; refusals are typed | codec payloads interleaved with damaged bytes, swapped values and changed keys, v1 | 256 | 4096 bytes |
| `codec-json-non-finite-floats` | `nervix-server` the JSON projection of non-finite floats: optional top-level fields read back as nulls, required fields and list elements refuse the payload | `WIRE JSON` and `WIRE CBOR` codecs over every float bit pattern and field type, v1 | 256 | 4096 bytes |
| `codec-transforming-numbers` | `nervix-server` number equality through the JAQ-native `JSON`, `YAML`, `TOML` and `CBOR` codecs and a protobuf codec with unchanged programs: `F64` and `F32` bits, alone and in lists, and `I64` values; a protobuf singular zero reads back positive | generated batches of one schema of numbers over every finite float bit pattern, v1 | 256 | 1024 bytes |
| `jaq-native-numbers` | `nervix-jaq` number equality through the identity program and each numeric native format's writer and reader: float bits, integer value and signedness | finite floats of any bit pattern, boundary floats and 64-bit integers, `TOML` within its signed range, v1 | 256 | 256 bytes |
| `remote-relay-rows` | `nervix-server` routed relay batch equality from the sender's payload to the receiver's rows, branch key bits and watermarks | every field type, concrete and absent branches and per-row registrations, v1 | 128 | 2048 bytes |
| `remote-relay-rows-malformed` | `nervix-server` the first payload defect in the receiver's check order | foreign bodies, miscounted watermarks and registrations and invalid branch keys, v1 | 128 | 2048 bytes |
| `relay-wire-messages` | `nervix-interconnect` relay grant, reply, admission, acknowledgement and connection binding equality through the bounded codec | every message variant, branch key values with any float bits and per-row metadata, v1 | 256 | 2048 bytes |
| `relay-wire-messages-malformed` | `nervix-interconnect` typed decode failure or a message that encodes and decodes back to itself, with every allocation of the decoder freed | damaged encodings of every message and arbitrary bytes, v1 | 256 | 2048 bytes |
| `client-producer-link-frames` | `nervix-server` complete equality of every client producer link frame through the bounded duplex codec of its stream, with nothing left charged | the opening message, every serving-node frame and every owning-node answer, with schemas of any field type, limits and counts at their ends, opaque batch bytes, every refusal, outcome, admission state and end reason, v1 | 256 | 2048 bytes |
| `client-producer-link-frames-malformed` | `nervix-server` typed decode failure, never an admission or execution refusal, or a frame that encodes and decodes back to itself | damaged encodings of every link frame, frames read as another direction's type and arbitrary bytes, v1 | 256 | 2048 bytes |
| `client-consumer-stream-frames` | `nervix-server` complete equality of every client consumer stream frame through the bounded duplex codec of its stream, with nothing left charged | the opening message, every serving-node frame and every owning-node answer, with schemas of any field type, policy durations from zero to the longest, every answer, refusal and settlement outcome, delivery headers across their ranges with and without a branch fingerprint, and chunks from empty to the largest one sent, v1 | 256 | 2048 bytes |
| `client-consumer-stream-frames-malformed` | `nervix-server` typed decode failure, never an admission or execution refusal, or a frame that encodes and decodes back to itself | damaged encodings of every stream frame, frames read as another direction's type and arbitrary bytes, v1 | 256 | 2048 bytes |
| `client-emitter-deliveries` | `nervix-server` client emitter delivery equality from the sink's Arrow IPC stream to the client library's batch: the Arrow schema of exactly the declared fields, every delivered row in order, values, nulls and float bits; refusal under another row count or other fields | every field type with nulls, nested lists and sliced views, deliveries of adjacent and scattered rows, v1 | 128 | 2048 bytes |
| `wasm-generated-pools` | `nervix-server` generated column pool equality from a WASM guest's Arrow IPC stream to the node's batch: unnamed fields, types, nullability, values, nulls and float bits | every field type with nulls, nested lists and sliced views, streams ended by the marker or closed, v1 | 128 | 2048 bytes |
| `wasm-generated-pools-malformed` | `nervix-server` a typed generated-IPC or batch-count defect, or one batch of unnamed columns that writes and decodes back to itself | truncated, flipped, overwritten, shifted, trailing and arbitrary pools, v1 | 256 | 2048 bytes |
| `simd-checked-lanes` | `nervix-simd-kernels` checked integer arithmetic equals scalar overflowing arithmetic at every SIMD level | all integer widths, runs and shared operands, v1 | 256 | 128 bytes |
| `simd-constant-division` | `nervix-simd-kernels` constant quotients and remainders equal checked operations at every SIMD level | all integer widths, divisors, scalar fallback and tails, with full-range signed overflow, v1 | 256 | 128 bytes |
| `deadlock-evidence` | `nervix-deadlock` complete evidence encode/decode equality | every finding variant, cycles up to the thread bound with omitted threads, texts cut inside multi-byte characters, absent context and extreme identities and times, v1 | 256 | 4096 bytes |
| `deadlock-evidence-malformed` | `nervix-deadlock` arbitrary bytes, bare or behind a valid evidence header, fail typed or decode within every bound to evidence that encodes back to itself | arbitrary bytes, v1 | 256 | 1024 bytes |
| `deadlock-evidence-bounds` | `nervix-deadlock` a valid encoding that breaks one bound is refused as out of bounds | one broken bound of findings, cycle threads, omitted threads, identities, lost findings or kept texts, v1 | 256 | 1024 bytes |
| `deadlock-evidence-summary` | `nervix-deadlock` the summary a qualification prints counts each finding once by kind, keeps repeated deliveries apart from lost findings, agrees with qualification, and names every count once | every finding variant in every scope, reviewed and unreviewed cycles, extreme repetition and loss counts, v1 | 256 | 4096 bytes |
| `branch-membership` | `nervix-branch-instances` owner steps against the specified visible-set contract: each claim, admission, eviction, expiry and release publishes exactly the current owner lifetime's membership, and a step that changes none publishes nothing | bounded claim, admit, expire and release sequences over six branch keys, v1 | 256 | 256 bytes |
| `task-status-transitions` | `nervix-server` task status publishes the complete healthy or failed status, preserving retry on error-only changes | 64 transition bytes over current status and retry values, v1 | 256 | 64 bytes |
| `entity-freeze-transitions` | `nervix-server` freeze publications contain exactly the coordination owners whose holds remain active | 64 insert/remove bytes over eight coordination identities, v1 | 256 | 64 bytes |
| `replica-progress` | `nervix-checkpoint-replication` replica reports, offers and announcer steps against the monotonic quorum contract: each replica's progress is the highest revision it reported, a count of replicas holding a revision never falls, and an offered revision keeps exactly one announcer until every assigned replica holds it or the replicated state is gone | bounded report, offer, step, cancel and retire sequences over four replicas and sixteen revisions, v1 | 256 | 256 bytes |
| `replica-catch-up` | `nervix-server` an owner's catalog of branch checkpoints and a replica's catch-up rounds against convergence: whatever catalog reads and branch fetches fail and however announcements are lost, repeated or reordered, the replica holds every branch its lifecycle names at the owner's revision once reads and fetches succeed, never records holding what its storage does not, and every listing reaches it through its wire form and encoding unchanged | bounded start, publish, stop, catalog replacement, naming, eviction, lifecycle, announcement and round sequences over four branches, unbranched work included, v1 | 256 | 256 bytes |
| `typed-report` | `nervix-lint-report` complete compiler-report serialization equality | bounded current findings, spans, expansions and completion metadata, v1 | 256 | 32 bytes |
| `typed-source-contract` | `nervix-lint-report` complete source-contract parsing equality | bounded current execution-contract annotations, v1 | 256 | 32 bytes |
| `typed-site-union` | `nervix-lint-report` authored-site deduplication preserves every configuration | repeated ordinary/modeled findings over bounded current sites, v1 | 256 | 32 bytes |
| `typed-source-syntax` | `nervix-lint-report` exact cast/field counts and complete source/site representation equality | bounded inactive Rust casts, testing-gated tuple fields, raw identifiers and Unicode comments with exact locations, v1 | 256 | 32 bytes |
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
| `columnar-json-rows` | `nervix-columnar-json` written rows against an independent rendering of their logical values and the first refused null | every supported scalar, list, fixed-size and nested list column, sliced, every null policy and number and bytes encoding, v1 | 256 | 8192 bytes |
| `bounded-write-limits` | `nervix-bounded-write` whole writes within the limit, capacity within it, and the outcome classification against a reference of the rule | writes, encoder failures and writing on after a refusal under limits of 1 to 1,021 bytes, v1 | 256 | 1024 bytes |
| `connector-row-request-division` | `nervix-connector` row request division against the documented halving rule, both limits and the measurement bound | declared and native row and byte limits, up to 160 rows, measures that grow and shrink with a candidate, v1 | 256 | 1024 bytes |
| `emitter-batch-subdivision` | `nervix-server` batch candidates against the documented rule, message limit, compatibility and encoding bound | up to 96 members in three compatibility classes, failing containers, encodings that grow and shrink, v1 | 256 | 1024 bytes |
| `emitter-batch-packing` | `nervix-server` packed payloads and rejected rows against the documented packing of exact member encodings and container framing | generated schemaful codecs and rows in carriers across two relays, three branches, three envelopes and seals, `MAX SIZE` from below one member's container to above the whole run's, v1 | 256 | 4096 bytes |
| `codec-bounded-encodings` | `nervix-server` bounded row, member and container encodings against the unbounded bytes, and container framing | generated schemaful codecs and batches under limits short of, at, past and around each encoding, and containers of 24, 64 and 256 repeated members where a count's encoding grows, v1 | 256 | 4096 bytes |
| `emitter-row-preparations` | `nervix-server` row request preparations kept unchanged or refused with the first violation | preparations of up to 12 selected rows spoiled by repeated, swapped, dropped, foreign and refused members and empty requests, v1 | 256 | 256 bytes |
| `emitter-retained-payloads` | `nervix-server` retained payloads written unchanged in packing order and answered member by member | packing and write steps with delivered, rejected and unanswered records and failed writes over three buffered batches, v1 | 256 | 512 bytes |
| `sqs-batch-requests` | `nervix-connector-sqs` measured record sizes, refusals above 256 KiB, and batch requests within every limit, ended only by a limit | small and near-limit bodies of one- to four-byte characters, attributes and FIFO groups, v1 | 256 | 1024 bytes |
| `mqtt-publish-packets` | `nervix-connector-mqtt` measured PUBLISH sizes against the packet the client writes, the protocol's largest packet and the broker maximum | topics up to 128 bytes, every quality of service, payloads beside every Remaining Length width and the protocol limit, v1 | 256 | 512 bytes |
| `postgres-insert-measures` | `nervix-connector-postgres` measured inserts against the statement and the arrays the driver encodes, and the Bind message bound | up to four text columns of up to eight rows with NULLs, quotes and multi-byte characters, every run of rows, v1 | 256 | 1024 bytes |

Each target declares its Cargo manifest. Product targets use the root workspace; the synchronization
properties use `tools/nervix-lint/Cargo.toml`. The shared runner discovers declared tooling workspaces
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

The CI sanitizer job budgets cold ordinary and instrumented builds separately from each target's
bounded campaign. Its overall limit is three hours; per-build, per-case and per-campaign deadlines
remain enforced by the shared runner. Validation uses an 8-CPU runner and a two-hour limit
to complete Clippy and the declared compiler synchronization matrix from a cold cache. Bolero
inventory and compiled discovery run through the dedicated Bolero commands and workflow.
The native extra-checks job retains its 8-CPU allocation and the same two-hour limit for ordinary
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
states what NSPL can spell as a rule over values instead of calling the parser, so the vocabulary's
own properties use it without a language dependency. No product code names it.

For native client producers and consumers it draws the window a client asks for and is granted,
with counts at one, at the bound the vocabulary declares and at the largest count their type holds,
a producer's whole description, every outcome of a submitted batch with each cause it names, and
physical durations that land on zero, one nanosecond and the longest duration as often as anywhere
else. It also lists one field of every declared type, each scalar, a list of it and a fixed-length
array of such lists, across the four combinations of optional and sensitive, for a property that
must carry every type on every run.

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
node-wide. A name of any kind may spell any NSPL keyword, statement and expression keywords alike,
and a field, UDF or function name inside an expression or a route construction may also hold a `-`,
`~` or `.`, or begin with a digit, which canonical NSPL writes between backticks. Names reach both
length limits, and counts reach the largest value their field holds.
The language properties also sweep sixteen deterministic byte sequences through every Model
family, every emitter sink and every statement form on every ordinary run, so none is left to the
random cases.

The NSPL Postgres generator keeps `DO UPDATE` targets nonempty and leaves at least one `VALUES`
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
- Outside an expression or a route construction, a generated name is one lower-case ASCII
  identifier, because canonical NSPL writes a name holding a `-`, `~` or `.`, or beginning with a
  digit, between backticks only inside one. The name properties cover the whole name rule on each
  name type directly. In the NSPL domain a relay is never named `message` or `branch`, and a
  placement member is never named like an `ALTER` operation keyword, because NSPL reads those words
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
  `just test-expiry-map-mutants`.
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
An expected sanitizer skip contributes no execution or coverage evidence. The Check workflow's
validation job and the Bolero randomized job retain their separate scopes and limits. Check calls
the reusable Bolero workflow with its exact tested SHA, event and label snapshot. Scheduled and
manual Bolero runs execute randomized checks and record a deliberate sanitizer skip.

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
  seconds and adds the summary to its job summary.
- `bolero-fuzz-combined` downloads every shard's completion evidence, without the raw profiles, and
  runs `just combine-bolero-fuzz`. It accepts only shards 1 to N of one plan, from one workflow run
  and attempt at the tested revision and inventory. Each must be complete, must have run exactly its
  planned targets and must hold a complete record for each of them and a report. Together they must
  select every registered target once. The job then writes the combined record and the merged Rust
  source report.

Each shard job allows 180 minutes. `bolero-gate` requires the randomized job, every shard and the
combined proof when the PR carries `fuzz`, and requires all of the sanitizer jobs to be skipped
otherwise. `tests/release-evidence.toml` holds a qualifying revision to the same checks, every shard
of the matrix included.

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
the run before that harness executes any property. The ordinary CI job reserves 60 minutes for
discovery, the declared feature builds and every registered target.

Install the dated sanitizer nightly named in the inventory and the pinned CLI. Server properties
also require the native server build dependencies, including `protoc` for the Pulsar driver;
both ordinary and sanitizer CI jobs install the Protocol Buffers compiler, Trunk and the browser
target. Every compiled Bolero recipe builds the embedded web console before discovering the server
properties, using the same configured compiler wrapper as the product build.

```bash
rustup toolchain install nightly-2026-09-17 --profile minimal
just install-cargo-bolero
just help
just fuzz-list
just test-bolero
just test-bolero nspl-model
just fuzz nspl-model 30
just fuzz-all 30
just fuzz-plan 3
just fuzz-all 30 2/3
just prepare-bolero-shard 1/3
just coverage-bolero 30
just qualify-bolero
just fuzz-replay nspl-model <saved-input>
just fuzz-replay-sanitizer nspl-model <saved-input>
just fuzz-reduce nspl-model <saved-input>
just combine-bolero-fuzz <shard-evidence> <output>
```

`just validate-bolero` compares the inventory with all workspace packages declaring
Bolero, scans their property macros within each owning Cargo package, and queries compiled targets
through filtered library and integration-test discovery with the inventory's declared features.
Source scanning stops at nested Cargo manifests and excludes Cargo build output; an authored module
named `target` remains part of its package. It rejects missing, unregistered, duplicate, ignored and
zero-selected targets, plus
corpus paths different from Bolero's actual work directory. Discovery executes only
`bolero_` tests of the registered library and integration-test targets under Bolero's selection
mode; it cannot start the server's scenario harness. It reports discovered, selected, executed and
completed counts.
`just validate` and `just validate-ci` do not run Bolero checks. Bolero commands perform discovery
before their selected action, and the dedicated ordinary Bolero CI job owns this check. The inventory includes server-owned Arrow
properties, so Bolero recipes prepare the web console assets the server library embeds through
`just bolero-deps`. This compiles assets only; it starts no live service. Both Bolero CI jobs install
Trunk, the browser Rust target and native server build dependencies before compiled discovery.
Console builds stage the complete real Trunk output and publish only changed assets. Identical
assets retain their file identity and timestamps, so repeated preparation preserves an already
instrumented server build; failed builds leave the published assets intact.

`just test-bolero` requires the configured randomized-case count and checked-in corpus
replay for every selected property, checking Bolero's reported input counts. Fuzz runs use real
libFuzzer coverage with AddressSanitizer, bounded input length and per-case timeout. The fuzz
profile enables optimization, debug information, debug assertions and overflow checks. Builds
use the pinned nightly and keep the configured kache wrapper. Modeled execution features are
excluded; Loom, Shuttle and Turmoil remain independent build invocations.
The server package uses the current profile's lighter optimization and omits debug output for its
single codegen unit; sanitizer instrumentation, coverage, debug assertions and overflow checks remain
enabled.
The pinned cargo-bolero CLI does not expose a Cargo test-harness selector. Each instrumented build
therefore records a run-local Rustup adapter that forwards the declared `--lib`, `--bin` or `--test`
selector to its Cargo test calls and forwards toolchain/version queries unchanged. It preserves the
toolchain, features, sanitizer flags and compiler wrapper. Library properties compile and execute only their
registered harness; they do not compile or invoke the server's Cucumber executable.
The adapter selects the pinned compiler through `scripts/bolero_rustc.py`, still beneath the
configured kache wrapper. For `proc-macro2` with `span-locations`, that compiler adapter removes
only its generic `cfg(fuzzing)`: the dependency otherwise deliberately replaces source locations
with zero, violating the source parser's contract and collapsing distinct findings. Sanitizer,
coverage and libFuzzer flags remain intact, and every other crate keeps its configuration. The
adapter's content digest separates build directories. The source-location property and saved
regression corpus assert the same exact locations in ordinary and sanitizer execution.
On a shared host, a cold server build can exceed the campaign runner's 30-minute compilation
limit. `just prepare-bolero <id>` prepares that same pinned, scoped sanitizer build with a two-hour
limit and retains its log and preparation metadata. It does not run a campaign or record fuzz
completion. `just prepare-archive-counts-fuzz` uses this shared path for the declared registry library harness.
`just prepare-bolero-shard <number>/<count>` prepares the declared harnesses one shard runs, as each
CI shard does before its campaigns.
The target must subsequently pass `just fuzz <id>` with the ordinary build and case
limits; CI campaigns continue to use that bounded runner.

PR CI runs a required ordinary randomized/corpus job through Check's reusable Bolero call. The
sanitizer libFuzzer shards run only when the PR has the `fuzz` label, with 30 seconds of engine
time per target. Every job uses the native
[CI linker](./developing-nervix.md#validation-and-tests), including sanitizer builds that supply
their own compiler flags. Adding or removing the label reevaluates the jobs, and a push to a
labeled PR runs the shards again. Scheduled and manual workflow runs execute the ordinary
properties. Job limits reserve additional time for compilation, artifacts and cleanup. A
cache may seed a campaign but cannot skip a target or replace checked-in regressions. An empty
selection, timeout, engine failure, sanitizer finding or property failure fails the job.

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
randomized producer that CI already requires independently.

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
remain separate from ordinary workspace coverage and its CRAP gate. A sanitizer skip publishes no
fuzz coverage artifact.

## Adding A Target

Put the Bolero test beside its production owner, name it `bolero_...`, bound iterations
and input length, and assert the complete current-value contract. Register the package, full
test name, source, domain version, features, corpus, budgets and invariant in the inventory.
Check in current-domain seeds, run `just validate-bolero`,
`just test-bolero <id>` and `just fuzz <id> 30`, then run
`just validate`. Both CI jobs include the target in the same change; the shard plan places it with
its harness, or deals a new harness to a shard, so a target needs no shard assignment. A new harness
whose sanitizer build exceeds a campaign's build deadline joins `prepared_targets`. A public product
bug found this way first gets a failing focused reproducer and affected public Cucumber scenario
before its fix.
