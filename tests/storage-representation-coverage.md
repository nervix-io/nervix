# Storage Representation Coverage

The `licensing-record-rkyv` property preserves licensing time, Free mode and every retained trust
document's original bytes, typed document kind, serial and optional working-key identity. The
consensus state-record properties include the singleton paired with the cluster identity; durable
recovery refuses an incomplete pair. Raft record generators carry captured bounded or unlimited
quotas in domain creation and restore effects. Admission-verdict generation includes complete
licensing refusals, and statement/archive/canonical domains include `DESCRIBE LICENSE`.
`client-state-inspection` and the shared host inspection property preserve cluster-license scope
without manufacturing a domain or graph-node identity.

Names drawn from `nervix-arbitrary` include current Plexus keyword spellings where their name
kind and domain admit them. The [property register](../docs/src/property-testing-and-fuzzing.md)
records each generated domain's revision and input bounds.

`scheduled-units` generates each node's declared current units, including independent partition
placements and WASM state lifetimes, and compares the complete domain schedule after archiving.
`models-archived-counts` compares complete WASM inspections with Single or partition units,
independent generations and full-width counts in each unit's checkpoint and recovery facts.

Sentry event generators retain the optional authored platform, including empty and Unicode
strings. The Model/archive, registry, consensus and canonical Plexus properties preserve that
complete field; entity presentation distinguishes omission from an explicitly written default.
The registry's current archive signature and consensus keyspace namespace fence this shape before
decoding; existing current-header corruption and namespace validation tests qualify those owners.

SQS QUEUE emitter generators draw both `SEND` request shapes beside the `MODE ACK` retry policy and
the optional FIFO group. The Model/archive, registry, consensus and canonical Plexus properties
preserve the request shape, and the archive signature and consensus keyspace namespace fence the
current SQS emitter shape before decoding.

`REDIS PUBSUB` ingestor generators draw both subscriptions, a `CHANNEL` and a `PATTERN`, each from
any text within its byte bound and the pattern from glob characters too. The Model/archive,
registry, consensus and canonical Plexus properties preserve which subscription an ingestor holds
and its exact text, and the archive signature and consensus keyspace namespace fence that shape
before decoding.

Ingestor generators draw every timestamp source, `TIMESTAMP AT metadata.<field>` included, in
`CREATE` and in `ALTER ... SET TIMESTAMP`. The Model/archive, registry, consensus and canonical
Plexus properties preserve the metadata source distinctly from a payload field of the same name,
and the same signature and namespace fence that shape.

NATS ingestor generators draw both entities, `NATS SUBJECT` and `NATS STREAM`, the stream source
with its stream and consumer names, both acknowledged modes and every quiesce mode it declares. The
Model/archive, registry, consensus, canonical Plexus, presentation and console-draft properties
preserve the complete stream source, and the advanced archive signature and keyspace namespace
fence the archived union that holds it.

Pulsar subscription generators draw both required owned creation positions, `EARLIEST` and
`LATEST`, provisioned subscriptions without a position, and all four subscription types, with one
instance for a type that reads through one consumer. The Model, registry and consensus properties
preserve those complete values.

KAFKA TOPIC ingestor generators draw domain-owned offsets, with no position, and a consumer group
with either required `START AT` position. Statement generators mix typed `SET START AT` with shared
ingestor operations. The Model, registry and consensus properties preserve those complete values.

Wire-schema generators carry each format's declared types, and codec generators carry explicit
`BASE64` byte-text rules alongside `RFC3339` datetime-text rules, on scalar fields and on lists of
datetimes or bytes. JSON, CBOR and AVRO retain their own wire type enums; CBOR carries its six
native representation types, and an AVRO array nests its item type, arrays included, to a
bounded depth. The Model/archive, registry and consensus properties preserve those complete
values, including inferencer state bindings and their initial expressions.
The `NERVIX EMIT INST` registry signature and `raft_inferencer_wasm_inspection_operational_retry_listener_bounds_` consensus namespace
identify the current Model layout before decoding.

`ALTER INGESTOR` and `ALTER EMITTER` statement generators draw, for every entity, each change its
own grammar writes: a locator, a declared option and the `DROP` of an optional one, `SET MODE`,
Kafka's and Pulsar's `SET START AT`, a declared quiesce mode and `SET CLIENT` where the entity reads
or writes through a client. They mix them with the shared operations, the in-place construction
changes of a route or an emitter, and an emitter's envelope clauses and properties. The
Model/archive, consensus and canonical Plexus properties preserve the complete operations in order,
and the `raft_inferencer_wasm_inspection_operational_retry_listener_bounds_` consensus namespace identifies the statement layout the
transactions it keeps are stored in before decoding.

`RESET WASM PROCESSOR ... STATE` statement generators draw a processor and an unbranched,
all-branches or concrete-branch scope; the statement takes its domain from the transaction that
keeps it. Consensus generators keep the same reset in a transaction's step effect and commit plan
step, beside the planning inputs that name the domain.

Ingestor count declarations retain their full positive 64-bit range. Compact execution-unit ranks
retain every instance ordinal a supported target can materialize, including ordinals above `u32::MAX`.
`models-ingestor-instancing` covers the declaration through JSON and archive encoding; the
scheduled-unit, physical-key, handoff-record, client-reply and backup-record generators carry
boundary instance identities. Native and archived units occupy eight bytes with four-byte archive
alignment. Semantic JSON and tagged physical keys validate the current ordinal bound. The backup
crate owns its eight-byte primitive `UnitWire` representation; its current unit-bearing record
versions convert the complete identity both ways.

MQTT TOPIC ingestor generators draw either session at QoS 0, 1 or 2 beside `NO ACK`, and the
persistent session at QoS 1 or 2 beside an acknowledged mode, so every pairing the declaration
admits is generated and none it refuses. The Model/archive, registry, consensus and canonical
Plexus properties preserve the session, quality of service and mode as separate written values,
and the archive signature and consensus keyspace namespace fence this shape before decoding.

MQTT TOPIC emitter generators draw `NO ACK` at QoS 0 and `ACK`, with either window, at QoS 1 or 2,
so every pairing the declaration admits is generated and none it refuses. The same properties
preserve the quality of service and the mode as separate written values under the same fence.

NATS emitter generators draw the Core subject and JetStream stream entities, including the stream
name, publication subject, acknowledged window, and optional message ID. Model/archive, registry,
consensus, canonical Plexus and presentation properties preserve the complete destination. The
current archive signature and consensus keyspaces fence this expanded emitter union.

Every target is registered in `tests/bolero-targets.toml` for ordinary randomized/corpus checks and
sanitizer libFuzzer. The same bounded generators, production storage paths and complete assertions
run in both modes. A target writes through the storage engine its boundary owns when it has one:
consensus state, the registry and the runtime-state store use a temporary database, and the
resource store a temporary directory. Codecs without an engine run in memory. All retained inputs
contain synthetic data and describe the current shapes.

| Stored representation | Target | Complete oracle |
| --- | --- | --- |
| Committed cluster parameters | `cluster-parameters` | All 22 class-typed parameter values, duration nanoseconds, byte limits, counts and revision identities survive validated archive and JSON round trips with complete equality |
| Committed node capability records and their typed announcement messages | `node-capability-records` | Complete node name, incarnation, explicit unknown or positive CPU and memory measurements, transport/DNS/database/consensus policy, engine/connector host capacities, operational and listener limits and all four positive receive bounds survive canonical durable encoding and validated JSON; current announcement requests and every publication outcome survive the same validated archive codec |
| Domain schedule units in control-plane storage | `scheduled-units` | The complete schedule, including each unit kind, full-width ordinal, primary, assigned nodes and ownership state, survives archive encoding unchanged |
| Execution revision built from a scheduled domain | `execution-revision-units` | Every declared partition retains its placement and the handoff fingerprint changes with it; moving one unit names that exact unit in the reassignment and preserves every sibling |
| Consensus state records: unit-keyed schedules, domains with their clock mappings and authorities, users, resource versions, node statuses, capability claims and uploads, cordons, admission fences, mutation leases, restore installations, transactions, commit plans, impact reports, command executions and the metadata with its retry fence and required cluster parameters | `consensus-state-records` | The recovered `StateMachineData` equals the stored one, records, derived indexes and metadata alike, for a revision written into an empty keyspace and for a second revision written as the changes from it, with records replaced and removed |
| Damaged consensus state | `consensus-state-corruption` | Recovery fails with `StorageFailure::InvalidState`, or recovers a state in which every record that names its key is stored under it, each resource version has at most one upload, and every transaction counts its pending and completed operations and numbers a failing one within those it accepted, and which stores and recovers again unchanged |
| Raft votes, log positions, log entries of every command, conditional orphaned-pause recovery replies and typed refusal causes, current state metadata, snapshot manifests, log keys, section keys and sections | `consensus-raft-records` | Complete equality through the storage codec and its conversions; a log key names its entry's index and a section key its section |
| Malformed Raft records | `consensus-raft-records-malformed` | Arbitrary bytes fail with the typed storage failure, or decode to a value that stores back to the same record |
| Registry Models keyed by domain, kind and name | `registry-stored-models` | Complete Model equality, listed in key order and read by key, after reopening the registry while its journal serves them and after a flush moved them into on-disk tables |
| Damaged registry records | `registry-stored-models-corruption` | The listing fails with `DecodeKey`, `InvalidModelArchive`, `DeserializeValue` or `StoredModelKindMismatch`, or lists Models whose own keys are exactly the stored keys |
| Runtime-state physical keys, namespace prefixes, index keys and chunk prefixes | `runtime-state-keys` | A key decodes to its namespace, state, schema fingerprint, generation, Model kind and name, execution unit and the fingerprint of its branch's canonical text; it lies under its own domain and namespace prefixes, and its index and chunk keys reach no other placement's |
| Malformed runtime-state keys | `runtime-state-keys-malformed` | Damaged or arbitrary keys fail with the store's typed failure, or decode to a placement whose own key is exactly the stored key |
| Malformed checkpoint chunk keys | `runtime-state-chunk-keys-malformed` | Damaged or arbitrary chunk keys fail with `InvalidCheckpointChunks`, `InvalidStorageFormat` or `DecodeState`, or name a segment set whose placement key decodes canonically and whose chunk prefix followed by the key's own offset is exactly the key, below the set's exclusive end |
| Namespaces restore maintenance walks: the domain a key names after its namespace | `runtime-state-staging-namespaces` | Placement, index and chunk keys of several domains, in the initial namespace and restored generations, name their own domain; a walk from the start visits every namespace of every domain exactly once in key order, every key between where it resumes and where a namespace ends lies in that namespace, and none lies past the last |
| Malformed keys under restore maintenance | `runtime-state-staging-namespaces-malformed` | The domain decoder fails with `InvalidStorageFormat` or names a domain whose text and separator open what it reads; a walk stops with `InvalidStorageFormat` at the first key whose namespace does not decode, and otherwise visits the namespace and domain the key's leading bytes spell exactly, resuming where that namespace ends |
| Handoff preparations, forced-recovery preparations and completions | `runtime-state-identity-records` | Every coordination identity, operation, node, incarnation, entity, unit and fingerprint, and every checkpoint with its placement compared by archived bits; two transitions share a key exactly when they name the same handoff or recovered unit |
| Malformed identity records | `runtime-state-identity-records-malformed` | Arbitrary bytes fail with the typed decode failure, or decode to a record whose placements convert back and which stores back unchanged |
| Kafka offset, branch lifecycle, deduplicator and branch-aggregated metrics snapshots in the inline checkpoint envelope | `runtime-state-snapshots` | Each owner's complete oracle: offsets with their exact optional applied-start generation, lifecycle keys with activity and incarnations, keyspace parts bit for bit in arrival order, and every metrics series |
| Malformed snapshot payloads | `runtime-state-snapshots-malformed` | Each kind's typed decode failure, or a value that stores back to the same payload |
| Inline, chunked and restored checkpoints in the runtime-state store, and a backup cut's listing of them | `runtime-state-store-checkpoints` | The exact payload at its revision through the latest-snapshot read and the streaming reader; a cut of the domain lists exactly its checkpoints in key order, filtered by kind and ignoring an unpublished generation and another domain, each with its stored size: a whole inline value or a chunked payload, and its exact payload and revision through the aligned, entry and bounded readers; before and after the database reopens, at, beside and between the chunk boundaries |
| Native Kafka offset checkpoints and the retained capture used by canonical-start coordination | `runtime-kafka-offset-captures` | Every partition offset and optional applied-start generation, the captured revision, written lengths within the retained topology's bound, and cancellation before every encoded and decoded entry |
| Canonical Kafka start candidates, pending seeds and redistributed instance offsets | `runtime-kafka-unit-layouts` | The complete candidate positions and generation; newest-generation owner progress wins over an unclaimed seed, and repeated grow/shrink retains every position exactly once |
| Historical ingestor metric series under repeated instance layouts | `runtime-ingestor-metric-layouts` | Every original label, counter, float bit, timestamp, rate and histogram bucket occurs exactly once; a repeated source history is refused |
| The Kafka offset transfer header: the checkpoint's little-endian revision and BLAKE3 digest ahead of its payload | `runtime-kafka-offset-transfer-headers` | The exact revision, digest and payload from a stream cut at every position and into generated chunks with empty ones between them |
| Short, stale and arbitrary Kafka offset transfer streams | `runtime-kafka-offset-transfer-headers-malformed` | `HeaderLength` for a stream that ends inside its header, `Revision` for one whose revision does not advance the held one or precedes the described one, and otherwise the revision and digest the header spells; every stream forwards exactly the bytes behind its header, at every split |
| Sealed window processor state | `runtime-window-snapshots` | Every retained row's sequence, ingestion time, branch bits and input and argument values, the next sequence, the branch lifetime and every aggregate's typed state; another branch lifetime restores nothing |
| Malformed window snapshots | `runtime-window-snapshots-malformed` | A sealed window damaged once, or arbitrary bytes behind the snapshot magic, fail with the snapshot's typed failure, open nothing for the branch lifetime, or open a window that seals back to the same bytes |
| Resource store directories and manifests | `resource-store-layout` | Every installed version reads back its complete manifest and the archive built for it after every other installation, after staging cleanup and after another version's removal; the removed version alone fails with `ReadFile` |

The earlier storage targets remain the owners of their codecs: `registry-archived-models` for the
registry's Model archive, `consensus-archived-counts` and `runtime-window-archived-counts` for
archived counts, `restore-installation-storage` for staged restore records,
`restore-native-lifecycle` and `restore-native-kafka` for the lifecycle and Kafka offsets a restore
writes, `backup-native-kafka` for the archive record a backup cut streams from a stored offset
checkpoint, and the WASM targets listed in the
[WASM representation coverage map](./wasm-representation-coverage.md) for guest checkpoints and for
the segment set a current chunk key projects to.

The shared name and Model generators include case-preserving MySQL table and database/table
paths. `models-external-names` compares their exact spelling through parsing, conversions, JSON
and the archive. `models-archived-models`, `registry-archived-models`, `registry-stored-models`
and `registry-stored-models-corruption` carry those current names through their Model storage
boundaries. Scheduled Models, commit-plan transitions and queued statements also carry them
through `consensus-state-records`, `consensus-state-corruption` and `consensus-raft-records`.
The Iceberg namespace and table Model recipe reaches those same consensus consumers through
schedules, commit-plan transitions and queued statements; their domains advance with it too.
The current `DESCRIBE INFERENCER` Statement variant is included in the canonical statement and
formatter generators. Its archived discriminant layout is fenced by the current consensus
namespace and state encoding; state, corruption and Raft-record domains advance together. The
separate `inferencer-describe-envelope` target covers the typed observation request and complete
success/refusal reply through the bounded interconnect codec, plus summary JSON equality.
Their domain versions and case/input budgets are listed in the
[target register](../docs/src/property-testing-and-fuzzing.md#target-and-representation-register).

Kafka native restore and archive capture preserve the exact optional applied-start generation
in addition to the complete ordered partition positions: `restore-native-kafka` and
`backup-native-kafka` compare that identity without deriving it from the domain start version.
The archive record and streaming targets `backup-record-manifest`, `backup-runtime-state-records`,
`backup-streamed-state-records` and `backup-complete-archives` generate both absent and present
markers and compare the complete current record. `backup-malformed-records` and
`backup-corrupt-archives` damage those current shapes and retain their existing refusal and
allocation checks.

## Contract Boundaries

- A runtime-state key holds its branch's exact text, and its decoder exposes the fingerprint of that
  text. The properties compare both the complete encoded key and this projection; a fingerprint does
  not reconstruct branch fields.
- A concrete branch key holds only finite floats. A stored or remote key holding a non-finite float
  fails with `BranchKeyError::NonFiniteFloat`, and one holding a datetime that is not RFC 3339 text
  with `BranchKeyError::RemoteFieldValue`. Generated keys hold finite floats of every bit pattern,
  signed zeros and subnormals included, and datetimes with whole-minute offsets.
- A registry key and a runtime-state key are read only in the exact spelling the writer produces:
  canonical lower-case names, separators where the layout puts them, and no trailing bytes.
- A consensus state record whose value names its own identity is accepted only under that key, and
  the state keyspace holds no record that recovery does not read. A log record is accepted only
  under its own entry's index. A transaction is accepted only in a state its transitions reach,
  and an operation range only with its last operation at or after its first, in every stored and
  transmitted form.
- Values that name no key, such as counters, cordons, admission fences, mutation leases, restore
  installations, commit plan steps and report items, are checked by their codec and by the readers
  that assemble them: report assembly refuses a missing or inconsistent item with a typed error.
  A whole value copied under a sibling key of its own family is not detectable from storage alone.
- A resource name that begins with a dot is written with that dot as `%2E` in its directory's name.
  The generated names include `.`, `..`, `.staging`, `.1.staging` and version numbers.
- The consensus state targets read up to 64 KiB, because a revision holding records of every
  family reads about 38 KiB on average. The corruption target draws its damage from 64 leading
  bytes of its own.
- Generators bound every record family to three records, names to their declared byte budgets,
  chunked payloads to three chunks, windows to three rows and branch keys to three fields of two
  levels of nesting. Volatile state, such as records in flight, ACKs, plans and attempts, is never
  generated or stored.
- Generated Models preserve MongoDB collection case, spaces and Unicode within its collection
  budget, which reserves a nonempty database and the separator from the namespace budget. Model
  archive and structured consensus-record properties inherit that domain and advance with it.
- A Kafka offset capture reserves an upper bound on what it writes, not its exact length. Its
  property writes through the same encoder a replica's transfer stages, and reads back through the
  replica's own reader. The transfer header properties drive the receiver's header owner as the
  transfer loop does; the staged payload's length and digest verification, and installation under
  the replica's assignment token, keep their existing tests and public scenarios.
- The maintenance walk reads namespaces, not placements: a key whose namespace and domain decode is
  visited even when what follows does not spell a placement, and decoding that placement remains
  `runtime-state-keys-malformed`'s contract.
- These are serial representation properties. Storage engine durability, replication, snapshot
  transfer, catch-up and shutdown retain their Cucumber, Turmoil and Chaos evidence. No
  synchronization mechanism or ordering claim is added by this coverage.

The public scenario **Persisted models reload from the on-disk tables a node flushed them into**
covers the registry's on-disk read path on one- and three-node clusters.

## Iceberg Model Coverage

The Model generators include Iceberg declarations with the required case-preserving,
dot-separated namespace, the storage backend and location, catalog reference and typed positive
commit-size limit. Registry and Model archive properties compare those fields with the complete
Model. The vocabulary owner separately checks namespace JSON/archive equality and backend
conversions in `models-iceberg-declarations`; `client-config-schema` covers ordered owned and
delegated configuration values through JSON/archive equality and public inspection. The same property
includes MySQL pool keys with nonnegative idle milliseconds, whole-second maximum ages,
defaulted acquire/maintenance intervals, and complete authored-value preservation.

## Client Configuration And Inspection

`client-config-schema` includes omitted ClickHouse connection defaults, Redis command-pool
lifetimes, Postgres pool and statement-deadline keys and MQTT keep-alive and publish admission
bounds, explicit typed TCP and pool intervals, lifetimes, checkout deadlines and borrow tests,
MQTT credentials whose password public inspection redacts, and a Pulsar token, which public
inspection also redacts, beside its paired client certificate and key files. `mqtt-client-config` separately converts every valid MQTT
client configuration into its typed broker, transport, identity, credentials, keep-alive and
admission bound. JSON and archive equality preserve the authored entries; the effective-default
projection preserves those entries and is idempotent, and public inspection remains idempotent.
Scheme-restricted driver addresses include RabbitMQ's `amqps` TLS-file configurations; the
property retains their complete ordered trust and identity entries through both encodings.
Driver URLs with Nervix-owned query options include Postgres addresses, which set `sslmode` once
to a declared choice; a TLS file conditioned on `verify-full` fixes that choice in the generated
address, and the property retains the address, its file entries and their acceptance through
both encodings. Keys conditioned on a choice include SYSLOG's stream and TLS keys, which fix the
generated `protocol` to a choice every one of them permits, and `host:port` addresses include
SYSLOG's `addr`, written with a name, a literal IPv4 host or a bracketed IPv6 host.
`syslog-client-sides` separately reads every valid SYSLOG client of either side
back through the entries that declare it and refuses a key only the other side reads.
`models-archived-models` includes the typed `DESCRIBE CLIENT` statement and its client identity;
the Plexus statement targets check its canonical text through both composed statement readers.

## Emitter Envelope Coverage

The vocabulary domain of the emitter Model generator draws envelopes of every clause kind, with
durations, expressions and properties of any name in written order. `models-archived-models`,
`registry-archived-models`, `registry-stored-models`, `registry-stored-models-corruption` and the
consensus Model targets carry them through their Model storage boundaries with the complete Model.
`models-emitter-envelopes` compares the envelope alone through JSON and the archive, and
`plexus-emitter-envelopes` reads its canonical section back through the shared envelope grammar.
The Plexus domain draws only what an entity declares, so canonical statement round trips carry the
envelopes connector entities declare, including Kafka/Pulsar `KEY` and `PARTITION FROM BRANCH`, and SQS QUEUE's `DELAY` and `DEDUPLICATION ID`.

The current emitter envelope domains include explicit STRING/BYTES `KEY` expressions and
`PARTITION FROM BRANCH`. SQS FIFO groups are expressions. Complete Model and statement
round trips cover these fields, their SET/DROP operations, and each connector declaration.
The registry and consensus current-shape signatures and the interconnect fingerprint advance
with this archived Model layout. Textual backup documents continue to use the current Plexus
reader under the existing archive-format contract.

Inferencer tensor state uses the registered `inferencer-state-snapshots` lossless frame/size property, `inferencer-state-snapshots-malformed` rejection property and `inferencer-native-snapshot-projection` native conversion property. The placement/key and runtime-state wire generators include the schema and typed model fingerprint. The frame carries its exact revision, optional branch incarnation, shapes and F32 bits.
