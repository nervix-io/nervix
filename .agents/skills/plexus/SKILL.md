---
name: plexus
description: Design, author, explain, review, and troubleshoot Nervix configurations written in Plexus. Use when a user wants to configure domains, schemas, codecs, branches, relays, resources, clients, ingestors, processors, emitters, placement policies, lookups, Roto UDFs, subscriptions, lifecycle commands, or complete Nervix streaming graphs. Produce current, valid Plexus and identify required external provisioning.
license: FCL-1.0-ALv2
---

# Configure Nervix with Plexus

Use Plexus, the Nervix graph definition language, to turn a user's streaming requirements into an explicit, deployable Nervix graph. Open the public
[Nervix Plexus documentation index](https://docs.nervix.io/llms.txt), then read the linked Markdown
needed for the request. Treat that versioned documentation as the authority; never reconstruct
clause order or connector options from memory.

For Plexus connector choices, read [Connector Capabilities](https://docs.nervix.io/connector-capabilities.md)
alongside the entity's Ingestors or Emitters section. Its generated tables show modes, option
requirements, metadata types, each side's header model, batching, buffer bounds, network waits and
the reasons ingest and emit differ. Read `Plexus Overview` → `Statement catalog` for connector and
entity heads, kind-preserving alterations, and name-based inspection. Choose the complete declared
pair before assembling its body, including when its connector has one entity. Read headers the way
the ingestor's model records them, such as
lowercase HTTP field names, and on an emitter that keeps one value per header name write each literal
header name once: a repeated literal name is refused when the statement is validated.
For outbound WebSockets, read the `WEBSOCKETS CONNECTION` section of Ingestors for literal upgrade
headers, subprotocol offers, response-header reads, frame metadata and the closed client CONFIG key
set, including idle deadlines after a delayed host handoff, the trust roots `tls_ca_file` adds to
and how inspection redacts an endpoint. Offer
subprotocols only through `HEADERS { 'sec-websocket-protocol' = '...' }`, and expect the peer to
select one of them: any other answer fails the connection.
For ClickHouse HTTP connection tuning, client certificates, compression and which insert failures
reject a row rather than retry, read the ClickHouse Emitters section and the generated client key
reference. `DESCRIBE CLIENT <name>` shows effective defaults and configured values;
`SHOW CREATE CLIENT <name>` preserves authored configuration. Both redact secrets.

For delivery durations and direct-value mappings, follow the Ingestors delivery-mode and Emitters
direct-value sections. Nervix validates the entity's declared limits and exact mapping types at
`CREATE` and `ALTER`; correct a refused declaration before starting the domain.
For statement heads, endpoint and codec kinds, addressing and input acknowledgement, read
`Plexus Overview` → `Statement catalog`, `Schemas And Codecs` → `Codecs` and the processor or
emitter input synopsis. Select the full kinded head before assembling its body; use only the
alteration operations that the owning chapter supports.
For MQTT ingestors, read [Ingestors → MQTT](../../../docs/src/ingestors.md#mqtt) for the session/QoS/mode
pairings and the broker's shared-subscription requirement, including with one instance. Read its
session and identity rules before sharing a client between entities or promising recovery after an
owner change. Its acknowledgement section distinguishes QoS 0 overflow, acknowledged capacity,
keep-alive during downstream stalls and the refusal of a broker grant below the declared QoS.
For MQTT retained publication, typed publish properties and user-property headers, read
[Emitters → MQTT](../../../docs/src/emitters.md#mqtt) for their exact types, limits and retry contract.
For Pulsar topic names, subscription ownership, creation position, TYPE and instances, read
[Ingestors → Pulsar](../../../docs/src/ingestors.md#pulsar) before choosing the ingestor. Its client
reference owns the shared authentication and TLS keys, and its metadata table owns message identity,
keys, redelivery and event-time windows. Choose
`TIMESTAMP AT metadata.event_time` or `TIMESTAMP AT metadata.publish_time` deliberately: an absent
event time stays null, while an explicit epoch time is preserved.
For Pulsar receipt deadlines, event times and delayed delivery, read
[Emitters → Pulsar](../../../docs/src/emitters.md#pulsar). Check the generated capability mirror
before carrying an option from one direction to the other.

For NATS consumption, choose `NATS SUBJECT INGESTOR` for a Core NATS queue subscription, which
reads in `MODE NO ACK` with a required `QUEUE GROUP` and `INSTANCES` and never sees what is
published while it is not subscribed, or `NATS STREAM INGESTOR` for a JetStream stream and durable
pull consumer an operator provisioned, which acknowledges each delivery. Read
[Ingestors → NATS](../../../docs/src/ingestors.md#nats) for the client's authentication and TLS
keys, the consumer checks, redelivery and what a stream keeps.
For NATS publication, choose `NATS SUBJECT EMITTER` with `MODE NO ACK` for Core NATS, or
`NATS STREAM EMITTER` with a provisioned `STREAM`, a literal `SUBJECT` captured by that stream,
and `MODE ACK SEQUENTIAL` or `MODE ACK PARALLEL MAX <n>` for a JetStream PubAck. The stream
emitter may set `MESSAGE ID <expression>`, which JetStream deduplicates across retries within the
stream's duplicate window. Read [Emitters → NATS](../../../docs/src/emitters.md#nats) for both
entities' complete clause order, failure outcomes and retry boundary.

For RabbitMQ consumption, read [Ingestors → RabbitMQ](../../../docs/src/ingestors.md#rabbitmq)
before choosing a mode: consumer prefetch and per-delivery settlement differ from publisher
confirms. Follow its metadata, sorted-header, recovery and shared client sections.
For RabbitMQ publication, read [Emitters → RabbitMQ](../../../docs/src/emitters.md#rabbitmq) for
the queue/default-exchange and named-exchange choices, `MANDATORY`, persistence, exact AMQP
`PROPERTIES` types and envelope alterations. Do not promise consumer completion from a publisher
confirm or message durability from routing alone.

For connector-specific event counts, including Redis publishes with no subscribers, read
[Connector-declared events](../../../docs/src/metrics-and-observability.md#connector-declared-events)
and the entity's generated capability reference before interpreting delivery.

For applications submitting Plexus, use [Client Libraries](../../../docs/src/client-libraries.md)
to select the host guide; Python programs follow [Python Client Library](../../../docs/src/client-library-python.md),
Java and JVM programs follow [JVM Client Library](../../../docs/src/client-library-jvm.md),
Ruby applications follow [Ruby Client Library](../../../docs/src/client-library-ruby.md),
and C and C++ programs follow [C And C++ Client Library](../../../docs/src/client-library-c.md).
Read its execution, clock and endpoint sections before choosing application-side lifecycle handling.
For using the authenticated producer identity in arrival filters and routes, read
[Client Ingestors](../../../docs/src/ingestors.md#client-ingestors) before choosing metadata fields.
For typed state inspection in Python, Ruby or JVM applications, read that host guide's
`Inspect Materialized State` section for page/refusal metadata, opaque continuation and retained
schema/frame lifetimes before suggesting application code.

For deduplicator keys and retained window state, read [Runtime Nodes → Inspect Remembered Keys](../../../docs/src/processors.md#inspect-remembered-keys)
and [Inspect Retained Window State](../../../docs/src/processors.md#inspect-retained-window-state).
Follow those sections for sensitive argument projection, branch selectors, paging and the distinction
between retained rows and window metadata; use each processor's `DESCRIBE` for a count summary.

For interactive authoring, the [CLI](https://docs.nervix.io/client-tools-cli.html) and
[web console](https://docs.nervix.io/client-tools-web-console.html) use the same server completion
contract. Refer users to those chapters for cursor edits, transaction-aware candidates, and
completion status messages, and completing multi-word creation heads. For connector expression
completion, including filters, HTTP request expressions, ALTER constructions, metadata fields and
entity-specific header functions, follow
the CLI's Completion section.
For live subscription, clock and notice output while editing, and terminal omission notices,
follow the CLI's [Asynchronous Output](../../../docs/src/client-tools-cli.md#asynchronous-output)
section.
For native client deadlines, reconnect pacing and retention, read the CLI's
[Connection Capabilities](../../../docs/src/client-tools-cli.md#connection-capabilities).
For VM admission/cache limits, WASM engine resources and connector-host cadences, read
[Engine And Connector Host Capabilities](../../../docs/src/node-settings.md#engine-and-connector-host-capabilities).
Keep external-system timing and pool choices in the connector's declared client configuration.
For secure CLI connections and console leader redirects, follow those chapters' connection
sections and [Native session TLS](../../../docs/src/installation-cargo.md#native-session-tls).
For scripted CLI use, read [Modes](../../../docs/src/client-tools-cli.md#modes) and
[Exit Status](../../../docs/src/client-tools-cli.md#exit-status) for completion JSON, transaction
inspection JSON, event output and partial drain outcomes. For browser access and transaction
commits, read [Signing In](../../../docs/src/client-tools-web-console.md#signing-in) and
[Inspecting a transaction](../../../docs/src/client-tools-web-console.md#inspecting-a-transaction).
Read [Layout](../../../docs/src/client-tools-web-console.md#layout) before interpreting footer totals.
Listener certificates are node provisioning, configured outside Plexus statements.
For TLS algorithm choices, read [Cryptography](../../../docs/src/cryptography.md#outward-tls-policy):
use ordered string lists with `ALTER CLUSTER SET TLS_CIPHER_SUITES = ('fips'),
TLS_KEY_EXCHANGE_GROUPS = ('fips');`. Read that chapter before choosing exact names or aliases,
changing both lists, or promising which connector handshakes are governed. The interconnect keeps
its fixed provider; existing connections retain their negotiation and connector trust/identity
remain with their declared client keys.
For backend validation claims, follow that chapter's P-256 signing and backend-status guidance;
selecting an approved algorithm does not establish a validated deployment.
For cluster continuity and public identity inspection, read
[Node Settings → Permanent Cluster Identity](../../../docs/src/node-settings.md#permanent-cluster-identity).
For `DESCRIBE LICENSE`, effective policy and quota usage, read
[Node Settings → Cluster Licensing](../../../docs/src/node-settings.md#cluster-licensing) and
[Licensing Metrics](../../../docs/src/metrics-and-observability.md#licensing-metrics).
For source UUID inspection and restore behavior, read
[Backup And Restore](../../../docs/src/backup-and-restore.md#what-a-restore-recreates).
For authentication refusals and retry behavior, follow
[Authentication](../../../docs/src/client-session-protocol.md#authentication); a response delay
does not establish that a user exists.
Read the Ingestors and Emitters chapters for `SHOW INGESTORS`, `SHOW EMITTERS`, and `DESCRIBE`;
their inspection contracts distinguish effective options from canonical `SHOW CREATE` configuration.
For ingestor inspection, follow [Ingestor Source Interactions](../../../docs/src/metrics-and-observability.md#ingestor-source-interactions) before interpreting raw counts, filtered graph traffic, poll duration or endpoint request outcomes.
Before reading an acknowledgement failure, a reconnect loop or a stuck ingestor from `DESCRIBE INGESTOR`, follow [Ingestor Acknowledgement And Lifecycle](../../../docs/src/metrics-and-observability.md#ingestor-acknowledgement-and-lifecycle): an expired `ACK TIMEOUT` is counted as `timed_out`, not as a failed tree.
For batching inspection, read [Emitter payload preparation](../../../docs/src/metrics-and-observability.md#emitter-payload-preparation)
alongside Emitters: check the effective container and distinguish encoded payload measurements,
retained work, retries and confirmed source records before interpreting a throughput figure.
For packing, exact byte boundaries, native containers, retry membership and the scope of measured
tuning evidence, read [Emitter Batching](../../../docs/src/emitter-batching.md). Choose the policy
from the entity's current declaration before applying its tuning guidance.
For emitter recovery and shutdown inspection, read
[Emitter outcomes and lifecycle](../../../docs/src/metrics-and-observability.md#emitter-outcomes-and-lifecycle)
for record outcomes, retry causes and waits, acquisition, pool waits, finish outcomes and the
current failure gauge in `/metrics` and `DESCRIBE EMITTER`.
The web console's visual Create forms also cover internal schemas, declared JSON/CBOR/AVRO wire
schemas, branches, relays, codecs, signaling protocols, clients, VHOSTs, endpoints, hash maps,
Roto UDFs, ingestors and emitters for every connector and entity pair, junctions, deduplicators,
reorderers, window processors, correlators, reingestors, generators, inferencers, WASM processors,
and session subscriptions; use the same web-console chapter for their typed fields, resource
versions, tensor shapes, guest limits, program editors, reference lookup, and transaction
behavior.

For materialized relay inspection, read [Relay → Inspect Materialized State](../../../docs/src/relay.md#inspect-materialized-state)
for selector, LIMIT/AFTER and FORMAT clause order, typed rows, sensitivity and stale continuations.
For the rows a reorderer holds or a correlator's pending records, read
[Processors → Inspect Held Rows](../../../docs/src/processors.md#inspect-held-rows) and
[Inspect Pending Records](../../../docs/src/processors.md#inspect-pending-records); they take the
same clauses and envelope. Read the CLI chapter for one-shot JSON output; programs consume
`CommandOutcome.state` directly.

## Gather the configuration contract

Establish these inputs before finalizing Plexus. Ask only for missing details that materially change
the graph; otherwise use conspicuous placeholders and state the assumptions.

- Domain: paced or unpaced, clock period/skew and start behavior.
- Payload: sample input, wire format, exact internal field types, optional fields, and sensitive
  fields.
- Ingestor and emitter: connector kinds, externally provisioned entity names, whether an operator
  provisions a Pulsar subscription or its ingestor owns it, the owned subscription's EARLIEST or
  LATEST creation position, the subscription's TYPE, endpoints, delivery/ACK expectations,
  emitter publishing mode, confirmation window and timeout where applicable, retry pacing,
  ordering, and offsets.
- Isolation: unbranched or a concrete branch key, branch TTL, optional instance limit, and the
  branch's partition count when one relay owner per branch would funnel too much traffic (see
  [Relay Partitions](https://docs.nervix.io/relay.html#relay-partitions)); the count changes only
  while the domain is stopped.
- Processing: filtering, construction, Roto UDFs, deduplication, ordering, windows, inference,
  WASM fuel and linear-memory budgets, correlation, materialized state, lookup, generation, or
  repartitioning.
- For inference with model-carried state, read `Runtime Nodes` → `Inferencer` → `State tensors`
  and `Shutdown And Recovery` → `What Survives` before choosing port bindings, initialization,
  branch isolation or recovery guarantees. Check the snapshot-section bound and branch retention;
  distinguish a completed graceful stop from recovery of the last periodic snapshot.
- Placement: latency-critical or heavy corridors, colocation enforcement, rule precedence, and
  whether ordinary scheduler heuristics should remain neutral.
- Operations: batching/flush, error routes, credentials/TLS resources, observability, session
  subscriptions, each ingestor's declared quiesce behavior, and whether an existing schema
  must be evolved atomically with its dependents.

Read [references/configuring-nervix.md](references/configuring-nervix.md), then use its routing
guidance to select the relevant Markdown entries from the public index.
For endpoint declarations, read `Ingestors` → `HTTP ENDPOINT And WEBSOCKETS ENDPOINT` for both
delivery modes, their quiesce sets, request/frame metadata and upgrade-header reads. Require an
explicit response window and reject-only quiesce with `MODE ACK`; its absolute response deadline
does not restart on progress. The configuring reference routes the status, signaling and retry
contracts.
For Kafka domain offsets, retain the documented native checkpoint quorum budget and at-least-once
retry boundary when explaining ingestor recovery.
For intake parallelism, read [Ingestors → Instancing](../../../docs/src/ingestors.md#instancing).
Check the entity's instancing capability, Pulsar subscription type, Kafka offset mode and constructing
routes before setting a count or following branch partitions. Use `DESCRIBE INGESTOR` to verify
the planned instance placement.
For payload sizes, client batch row counts or peer-address keys, read
[Ingestors → Payload Facts](../../../docs/src/ingestors.md#payload-facts) and the entity's declaration
in `Connector Capabilities`. Every ingestor exposes required `payload.bytes: U64`; only
`CLIENT PRODUCER` exposes required `payload.rows: U64`. Keep these original transport/batch facts
when filtering or constructing rows. On HTTP, WebSocket and Syslog listeners, use
`metadata.remote_ip` for an IP without a port, including IPv6.

## Assemble the graph

Run the control plane with `nervix-server` and submit configuration through the separate
`nervix-cli` client. Format saved `.plexus` files with `nervix-plexus-format`.
A script driving `nervix-cli --command` branches on its exit status, never on printed text: `0`
completed, `1` failed, `3` outcome not known, `130` cancelled before admission. After `3`, send the
same command again with the `--execution-reference` or `--upload-identity` the CLI printed, which
recovers the outcome; a fresh submission could apply it twice. The CLI guide's Exit Status section
defines each status.
For `RESTORE`, retain the same archive bytes as well as the statement and execution reference after
status `3`, including an invalid reply after upload. Follow the Rust client guide's
[Restoring](https://docs.nervix.io/client-library.html#restoring) contract when recovering it.
For browser restore recovery, read [the web console's Backing Up And Restoring section](../../../docs/src/client-tools-web-console.md#backing-up-and-restoring)
before interpreting a later request failure as the result of an earlier possibly admitted restore.
For a native CLI or Rust-client session addressed by hostname, Hickory resolves the selected
server, seeds, and redirects on connection. Keep the hostname in the `http://` or `https://` server
URL so HTTPS verifies that name. The CLI normally reads the system resolver and hosts files; use
its `--dns-resolver-config`, `--dns-hosts-file`, and repeatable `--dns-name-server` options only
when the session needs an explicitly configured resolver. Browser sessions use browser DNS.

Use the CLI's `subscribe` and `domain-clock` subcommands for shell streams of relay records and
the selected domain's clock. See [Command Line Client](https://docs.nervix.io/client-tools-cli.html)
for their line formats, recovery notices, stream endings and exit statuses before scripting either
observer.

For diagnostic-node lock findings, follow the local report inspection and triage guidance in
[references/configuring-nervix.md](references/configuring-nervix.md#verification-and-troubleshooting).
Retain each potential cycle's source evidence and regression-backed review before qualification.

Build configuration in dependency order:

1. Create the domain, then select it with `USE <domain>;` as a separate client command. `BEGIN`
   is rejected until a selected domain exists.
2. Register and upload resources before statements that reference their versions or mounted files.
   Resources are domain-owned, so declare and upload them in the domain that references them.
   For pending replica diagnosis and upload completion, follow the resource lifecycle guidance.
   Classify upload refusals by their typed failure; follow the Client Session Protocol's Resource
   Uploads contract for identity conflicts and recovery.
   VHOST TLS, protobuf codec, protobuf signaling protocol, inferencer, WASM processor, hash map, and
   client mount bindings require `VERSION <n>` or `VERSION LATEST`. `LATEST` resolves to the highest
   completed version when the statement is applied (at `COMMIT` for queued statements) and the
   model stores that number; a later upload never moves a binding.
   Use `REBIND RESOURCE <name> TO VERSION <n>|LATEST` to move every existing usage atomically, or
   add `FOR <kind> <name>, ...` to select exact kind-qualified usages. Every selected member must
   already bind the resource. The target and all replacements validate together; `LATEST` is
   provisional at queue admission and resolved again at `COMMIT`. Rotating a VHOST certificate
   this way is dynamic: every node's HTTPS listener presents the new bundle and ingestion does not
   pause. A TLS binding error identifies the failing file kind without exposing its path or
   certificate material. Changing VHOST hostnames, adding or removing `WITH TLS`, or binding
   another TLS resource pauses the domain. Moving a WASM processor usage also discards the saved
   guest state of each branch, because a new module cannot restore what the module it replaces
   wrote; an uncompilable candidate module rejects the whole rebinding with every binding and its
   saved state still in place.
3. Define internal schemas, branch-key schemas, branches, wire schemas, and codecs.
4. Define clients with a `<CONNECTOR> CLIENT` head, such as `CREATE KAFKA CLIENT`. Define signaling
   protocols, virtual hosts/endpoints, lookup models, and trusted Roto UDFs as needed.
   Read the generated client CONFIG key tables in Connector Capabilities. Keys are lowercase and
   case-sensitive; declare required keys exactly once, follow their value types and pair TLS
   certificate/key files. Pass other property names only in an explicitly delegated driver
   namespace. CREATE also runs network-free Kafka/connection-URL checks before storing a client;
   credentials and connectivity still validate at initialization. Public SHOW CREATE and previews
   redact declared secrets and address credentials/query options; backups retain secret-bearing
   complete configuration under the archive contract.

5. Define relays before nodes that read or write them.
6. Define ingestors, processors, generators, and emitters in graph order.
7. Define placement rules after every referenced runtime node, including each relay, exists.
8. Commit the graph, inspect it, and start the active domain only when prerequisites exist.

For backup recovery, `RESTORE ... FROM '<file>' RESUME` (CLI `--resume`) installs compatible
materialized relay rows, deduplicator keys, windows and inferencer tensors on owners and replicas,
then makes the domain running at its archived start generation and latest start point. Paced recovery retains the
wall/logical mapping and time rate verbatim and projects downtime under a newly selected clock
authority. The default restore leaves the domain stopped; a normal `START` advances the generation
and clears materialized state while preserving compatible WASM state, source offsets, branch
lifecycle, deduplicator keys, windows and inferencer tensors. For model-carried state and either
`WITHOUT STATE` option, read `Backup And Restore` → `Archive Sections` and `Runtime Nodes` →
`Inferencer` → `State tensors` before promising recovery. `RESUME` precedes the user policy and
`DRY RUN`; the dry run reports the planned status and generation. Read
[Backup And Restore](https://docs.nervix.io/backup-and-restore.html) for format and memory limits.

Keep an uncertain restore's original execution reference, statement and archive across attempts;
a later local read failure or stream refusal does not settle it. Follow the native-client recovery
contract in [Restoring](https://docs.nervix.io/client-library.html#restoring).

`BACKUP ... TIMEOUT` bounds each running domain's cut independently. For a cut timeout, inspect its
active phase and completed phase durations before choosing a new budget; the backup architecture's
Observability section explains owner-side attribution. Native clients use a separate
bounded overall backup wait (CLI `--backup-wait-timeout`, default `10m`). If that wait is uncertain,
retain its execution reference and recover with CLI `backup --execution-reference` and the same
selected domain, scope, resources and capture options; the output file or stdout may change.
Waiting again does not extend server retry validity or archive retention. A `WRITE_FAILED` from
`--output -` that names `error.archive` follows a complete download that released the server's
copy: deliver that verified archive instead of recovering by reference. One without a reference
took no backup, as when stdout is `/dev/null` or closed: fix stdout and run the backup again.

Use `BEGIN; ... COMMIT;` when sending multiple queueable configuration statements. A transaction
belongs to one already-existing domain: `BEGIN` binds it to the selected domain and every queued
statement must select that same domain. Transactions and commit progress are replicated and
resumable, but their content is deliberately limited to that domain's model mutations, domain
configuration/lifecycle, `CREATE RESOURCE`, and `RESET WASM PROCESSOR ... STATE`. Keep
`CREATE DOMAIN`, `CREATE USER`, other read-only statements, subscriptions, `USE`,
`ATTACH DOMAIN CLOCK`/`DETACH DOMAIN CLOCK`, resource uploads, backups, restores, and node
administration outside the transaction. Plan restore capacity for independently admitted archive
metadata and streamed checkpoint I/O, following the backup guidance in `references/configuring-nervix.md`.
Size node memory for every execution pool, including active snapshots and optional retained materialized-source reads;
the default startup requirement is 2,787 MiB, including both database caches. Read [Node Settings](https://docs.nervix.io/node-settings.html)
for the complete prerequisite checks.
For Nervix transport, DNS, database or consensus sizing, read
[Node Settings → Transport, Resolver And Database Capacities](../../../docs/src/node-settings.md#transport-resolver-and-database-capacities).
These are server provisioning options. Change liveness through committed
`ALTER CLUSTER SET NODE_UNAVAILABILITY_TIMEOUT = <duration>;` and verify each node's running
projection with `DESCRIBE CLUSTER`; changing a local flag does not replace committed cluster policy.
For detected CPU/memory claims, capability rolls, peer receive bounds and placement weighting, read
[Node Settings](../../../docs/src/node-settings.md#committed-node-capabilities) and
[Placement](../../../docs/src/placement.md#capacity-at-the-load-tier). Verify the committed
incarnation with `DESCRIBE CLUSTER` before attributing a distribution to an override.
Use `SHOW TRANSACTIONS;` when transaction state or a retained outcome needs
verification. Use `DESCRIBE TRANSACTION [ '<id>' ] [ OPERATION <n> ] [ FORMAT TEXT | JSON ];` to
explain what an open, committing, or retained transaction requires and changes before or after
`COMMIT`. These two reads are allowed while a transaction is open, are sent on their own, and neither
queues content nor shifts operation numbers. Read `Plexus Overview` for its forms and
[Transaction Quiescence And Impact Inspection](https://docs.nervix.io/transaction-quiescence.html)
for report scopes, execution outcomes, and retained inspection. Treat a successful inspection
as one complete result: selecting an operation changes presentation focus without narrowing the
transaction, and neither text, JSON, the Rust result, nor the browser graph paginates or truncates a
large report. Queue admission preflights each statement against the replicated prefix without
applying effects. Consecutive model mutations form one atomic run and report the run's effective
base-to-final quiesce level at the current prefix; a lifecycle, domain, resource-catalog, or WASM
state-reset statement ends that run, and a later run cannot repair it. `COMMIT` reports only the
maximum level actually executed and does not repeat statement outputs. Correct a rejected statement
and continue the same transaction. A `COMMIT` refused because the preview it expected no longer
describes the transaction applies nothing and leaves the transaction open; inspect the attached
transaction again before retrying the commit against its reviewed basis. Do not imply that one
undivided request can mix
those phases.

A committed cluster parameter revision invalidates an identified transaction preview even when
the queued Models still fit its caps or the parameter values later return to their earlier values.
Inspect the transaction again before committing; a parameter notice does not review a new preview.
If a `COMMIT` attempt ends while the transaction still commits, keep its execution reference and
repeat it to recover the outcome. `SHOW TRANSACTIONS` and an attached transaction's status explain
progress but do not replace the result under that reference. See
[Client Session Protocol → Transactions Over The Protocol](../../../docs/src/client-session-protocol.md#transactions-over-the-protocol).

Treat a successful administrative command as a completed effect. After `UPLOAD RESOURCE`, model or
lookup creation, `START`, `STOP`, placement changes, or `COMMIT` returns `OK`, issue the dependent
operation immediately. Do not add a readiness request, `DESCRIBE` polling loop, arbitrary delay, or
retry before that dependent operation. Transaction statement success means durable validation and
staging only; `COMMIT` supplies the full usable-effect boundary. CLI, Rust-client, and browser
flows retain command execution references, upload identities, transaction append positions, and
commit identity through redirects or reconnects. Do not manufacture a new identity for an
uncertain admitted operation.

If a session's COMMIT, REVERT, or attach reports that its transaction is unknown after the retained
tombstone is removed, that session can begin another transaction. Keep the original execution
reference for any earlier command whose outcome is uncertain; the new failure does not identify
that earlier outcome. Follow [Exact Recovery](https://docs.nervix.io/client-session-protocol.html#exact-recovery)
for its recovery window and expiry.

For storage failures or uncertain administrative outcomes, consult `Control Plane` → `Durability
and recovery` before suggesting a retry, and
[Exact Recovery](https://docs.nervix.io/client-session-protocol.html#exact-recovery) for how a retry
under the same execution reference recovers the recorded outcome instead of repeating the effect.
An unreadable restore reply leaves its original execution uncertain, even after the server
completed it. Keep the same statement, archive and execution reference for recovery; CLI status 3
names that uncertainty. Follow `Client Library` → `Restoring` for the native-client contract.

For model evolution, read the `Altering Schemas` section of `Schemas And Codecs` and the transaction
and quiesce semantics in `Control Plane`. Put every interdependent `CREATE`, supported `ALTER`, and
`DROP` for one domain in the same transaction, including schema, wire-schema, relay, junction,
deduplicator, reorderer, emitter, ingestor, reingestor, generator, and placement changes;
Nervix classifies the complete model diff and no user-facing pause command exists. Capacity and
expression-only junction changes and emitter flush changes are dynamic; relay schema or branching
changes pause the domain; structural junction, emitter/client/codec/collect/publishing-mode or
attachment changes, ingestor,
emitter source-predicate, and relay materialized-state changes gate and drain only affected
entities. Changing emitter `FROM` membership pauses the domain because it changes topology.
Deduplicator key and reorderer ordering changes also use entity pause; their `MAX TIME` changes are
dynamic. Write an ingestor's `ALTER` under the stored ingestor's own head, such as
`ALTER KAFKA TOPIC INGESTOR`; an alteration never changes its connector or entity, so reading
another one takes `DROP INGESTOR` and a new `CREATE`. Use that head's complete body after
`SET FROM`, or change one part in place: `SET CLIENT`, `SET <locator>`, `SET|DROP <option>`,
`SET MODE` and `SET QUIESCE` with a mode the entity declares, and
`ALTER ROUTE TO <relay> <construction change>`; see `Ingestors` → `Altering Ingestors`. A client
ingestor takes the complete `SET FROM SCHEMA ...` body under `ALTER CLIENT PRODUCER INGESTOR`, and a
change to its endpoint contract ends its attached producers.
Buffered client routes force-flush and settle their admitted acknowledgements before an alteration
publishes a replacement contract. Keep reading admitted outcomes while admission is suspended;
reopen a producer after a committed contract change, and retain its attachment when an alteration
fails. Unresolved submissions keep their unknown-outcome boundary and must never be replayed
automatically.
Every reingestor and generator ALTER uses entity pause; reingestor route bodies retain their
per-route branch selection, while generator route bodies remain set-only.
Read `Placement Policies` before choosing domain-wide or corridor placement, ranks or exact-unit
relocation. Validate each hop's unit correspondence after rank resolution; a new effective hard
requirement can relocate the corresponding unit groups.

During node startup, `SHOW CLUSTER STATUS;` and `DESCRIBE CLUSTER;` remain available while live cluster policy is discovered. Status reports `node.initialization=initializing` until initialization finishes; wait for `ready` before policy-dependent commands or transfers.

Use `ALTER CLUSTER SET NODE_UNAVAILABILITY_TIMEOUT = 15s, STATE_SNAPSHOT_INTERVAL = 250ms;`
for committed Nervix cluster policy changes. Keep it outside domain transactions; it needs no
selected domain. Read `Domains And Time` → `Duration Literals` for duration spelling, and
`Node Settings` for the complete parameter set, one assignment per parameter, positive duration
bounds, existing-model cap refusals, live installation and committed-versus-running inspection.
Seed flags belong only to the first bootstrap, including when their value equals the default.
Later starts, including the returning bootstrap node, omit them.
`HTTP_HEADER_FIELDS` ranges from 1 to 16,384, leaving capacity for client and transport headers. Committed header byte and field caps also protect
literal HTTP polling headers, at model creation and when a later cap is lowered.

## Preserve Plexus semantics

- For paced domains, use a positive `PERIOD` no larger than `18446744073709551615ns`; `SKEW` may
  be zero but must fit the same nanosecond duration range. `START AT <timestamp>` is limited to the
  inclusive signed Unix-nanosecond range `1677-09-21T00:12:43.145224192Z` through
  `2262-04-11T23:47:16.854775807Z`. `TIME RATE` must be a positive finite `f64`; scientific notation
  is valid. Tick notifications report progress and never redefine the committed start mapping.
  The mapping is installed on every live node before domain execution and remains bound to its
  `START` generation across joins and automatic ALTER pauses. One replicated authority revision
  identifies the producing node incarnation; owner changes preserve the mapping, and progress is
  accepted only from the matching committed incarnation and authenticated peer after every live
  node installs that revision. `STOP` revokes the authority, while automatic ALTER quiescing leaves
  it running. Never describe a missing, stopped, uninstalled, or stale paced clock as falling back
  to wall time. Unpaced domains receive actual UTC through the same domain-time capability.
  Require synchronized and monitored UTC on every cluster host. Reads are nondecreasing per node
  and generation, but the mapping does not promise identical simultaneous reads or a total order
  across hosts. Host offset affects `START AT NOW`, unpaced observations, and paced projection;
  `TIME RATE` multiplies projection error. `SKEW` is event-admission tolerance, not a host-sync
  allowance.
- For ingestion, read `Domains And Time` → `Ingestion Timestamps`: `TIMESTAMP NOW` uses domain
  time at delivery, including after quiescing; explicit input times remain unchanged.
  `TIMESTAMP AT metadata.<field>` takes a `DATETIME` metadata field the ingestor's entity declares,
  such as an HTTP poll's `metadata.polled_at`; check the entity's Metadata row in `Ingestors`
  before writing any `metadata.<field>`, because an undeclared field is refused at `CREATE`. Check
  admission against the newest 256 reached logical centers with inclusive `SKEW`, independently
  of tick notification delivery. Never scale input timestamps by `TIME RATE` or admit against
  an unreached future center. Rejected or missing timestamps follow the route's message-error
  policy per row with code `validation` and operation `admit`.
- Treat HTTP `EVERY`, Prometheus `EVERY`, and generator `EACH` as domain-clock cadence. HTTP and
  generators have an immediate first occurrence; Prometheus first becomes due after one interval.
  Later occurrences stay anchored, coalesce missed periods into one newest-due execution, and
  advance directly to the first future boundary. Prometheus queries at the due instant and
  evaluates returned data with a fresh execution snapshot. The declared quiesce mode still decides
  whether polling is suspended or buffered; withholding work does not pause or re-anchor the
  cadence. Connector timeouts, retries, backoff, and cancellation remain physical.
- For HTTP REQUEST polling, `metadata.polled_at` is the scheduled instant and `metadata.status`
  is the successful HTTP status. A `204 No Content` response emits no row, and any other status
  outside `2xx`, a redirect the client does not follow included, fails the poll; a failed instant
  is not polled again. `read_header` retains non-ASCII UTF-8 values; invalid UTF-8 header values
  are omitted and counted in `nervix_ingestor_header_values_dropped_total`. A `gzip`, `deflate` or
  `br` response arrives decompressed, without its `content-encoding` and `content-length`
  headers, and `MAX BODY SIZE` counts the decompressed body. Read the
  [HTTP REQUEST ingestor contract](../../../docs/src/ingestors.md#http-request) for its
  declaration, header model and failures.
- For RABBITMQ QUEUE, `metadata.timestamp` reads the AMQP timestamp as whole seconds. A publisher
  that writes milliseconds there sends an instant no `DATETIME` holds, so every message is refused
  wherever a route or `TIMESTAMP AT` reads the field; check the producer's unit before choosing
  `TIMESTAMP AT metadata.timestamp`. `metadata.exchange` is `''` for the default exchange. Read the
  [RabbitMQ ingestor contract](../../../docs/src/ingestors.md#rabbitmq) for every field.
- For KAFKA TOPIC, `metadata.timestamp` reads the record timestamp as milliseconds. A producer that
  writes microseconds there sends an instant no `DATETIME` holds, so each such record is refused
  wherever a route or `TIMESTAMP AT` reads the field and is ingested everywhere else; under an
  acknowledged mode, send that route's `ON MESSAGE ERROR` to a relay, because `LOG` negatively
  acknowledges the record and the ingestor reads it again before any later record. Read the
  [Kafka ingestor contract](../../../docs/src/ingestors.md#kafka).
- Treat domain execution time as one snapshot per accepted unit of work. Every expression in that
  unit uses the same instant, including filters, construction, keys, correlation and window
  programs, inferencer mappings, emitter `VALUES` and SQS FIFO groups, and subscription filters.
  Buffered emitter batches and retries retain their assigned snapshot; work resumed after
  `REQUIRED WAIT` starts with a fresh one. Error occurrence and error-route `SET` share the failing
  snapshot. Generator output and tokenless WASM output use it for generated watermarks, while
  source-token WASM output preserves source metadata. Omitted Sentry timestamps use domain time;
  explicit Sentry timestamps are preserved; OTEL `observed_time_unix_nano` and HTTP-date
  `Retry-After` interpretation use actual UTC, and a retried OTEL Export request keeps the observed
  time it was prepared with.
- Treat a session subscription `WHERE` clause as a predicate over the subscribed relay record.
  Bare fields, `message.<field>`, and `input.<field>` are equivalent there. Do not use `output`,
  `branch`, materialized `relay_state`, construction clauses, or side effects; subscription
  creation rejects them, and a selected record is delivered unchanged before sampling.
- Roto `now()` and the WASM domain-time import receive the owning execution snapshot explicitly.
  There is no context-free engine clock. Guest initialization, input, timeout, flush, and state
  lifecycle operations each use the snapshot selected for that operation; guest timeout delays are
  logical, while Wasmtime fuel and epoch yielding are physical safety controls.
- Do not simulate a targeted WASM guest-state reset with `ALTER`, resource rebinding, or a
  stop/start cycle. A coordinated reset is a control-plane state-lifetime operation with an explicit
  unbranched, concrete-branch, or all-branches target and a stable execution reference. Its one
  public statement is `RESET WASM PROCESSOR ... STATE`, described below, and the
  `ON REJECTED STATE RESET` policy triggers it for one refused branch lifetime; do not invent any
  other reset syntax. Guest code reaches the same operation for its own branch through the SDK's
  `request_state_reset` or the raw `nervix_request_state_reset` import, which discards that
  callback's uncommitted output and input and never re-enters the guest. `REBIND RESOURCE` does
  start a fresh lifetime for every branch of each WASM processor it moves, but only as the
  consequence of changing the module, and it cannot select a branch.
- Declare exact schema types and nullability. Use explicit conversions; never invent implicit
  casts between wire, internal, branch, processor, lookup, state, and emitter values.
- For every expression, read `Expression Functions`: `Where Expressions Run` for the functions each
  context allows, `Function Properties` and `Errors` for optional results, sensitivity, and where a
  failure goes, then the section of each operator and function the expression uses. Check that:
  - literals are `I64` and `F64`, so a narrower operand's literal is cast (`input.count > 5 AS U32`),
    a float is one word (`2.5`, never `2 .5`), and there is no exponent, `DATETIME`, or `BYTES`
    literal;
  - `NOT` binds tighter than comparisons and `AS` tighter than unary minus: write `NOT (a > b)` and
    `(-128) AS I8`;
  - a field, call, or `SET`/`INHERIT` target named like a reserved word (`end`, `from`, `in`, `all`,
    `null`, ...) is written between backticks where it stands bare (`` `end` = input.end ``), while
    `input.end` and `udf::case(...)` need none; a statement keyword such as `to`, `on`, or `by` is an
    ordinary name;
  - `AND`, `OR`, and `coalesce` evaluate every operand, so only an `IF` or `CASE` arm shields an
    operand that can fail, such as a division by a field that can be zero;
  - an optional result (`TRY_CAST`, JSON extraction, `nullif`, `LOOKUP_HASH_MAP`, `regexp_substr`,
    `regexp_extract`, URL components, list `first`/`last`/`nth`/`sum`/`min`/`max`/`mean`, sample
    window statistics, `CASE` without `ELSE`) reaches a required field only through `coalesce`;
  - every route whose functions can fail has an `ON MESSAGE ERROR` policy, which also receives a
    failed ingestor `BRANCHED BY ... SET`, and expressions that can fail stay out of deduplication
    keys, reorderer `BY`, `CORRELATE WHERE`, inferencer `INPUTS`, and a reingestor's
    `BRANCHED BY ... SET`, which have no error route;
  - `IN` sets hold non-null constants of the operand's exact type, and string positions count from
    1 while `nth`, `slice`, and JSON path indexes count from 0;
  - datetime units, parts, `date_bin` widths, zones, formats, and disambiguations are literals, a
    local calendar names its zone, `now()` supplies domain time, and foreign timestamps are read
    with a strict `parse_datetime` format;
  - IP text is parsed once into a `BYTES` field, IPv4-mapped addresses are unmapped before IPv4
    network tests, URL inputs are absolute, and many JSON fields are read from one document field;
  - every sensitive value stored in a field that is not sensitive goes through `leak_sensitive(...)`;
    hashing, counting, or testing a sensitive value keeps the result sensitive.
- In window routes, read `Expression Functions` → `Window Aggregates`. Aggregates appear only in
  window route `SET`, `input` only inside their arguments, and `COUNT`, `SUM`, `FIRST`, `LAST`,
  `MIN`, and `MAX` are aggregates there, never list functions. Compute window output from aggregates
  and constants, and prefer the dedicated aggregates (`AVG`, `COUNT_IF`, `ARG_MIN`, `ARG_MAX`, the
  `*_POP` and `*_SAMP` statistics, `CORR`) over hand-built formulas. Before using `COLLECT_VEC`,
  check its required argument and bounded-retention rules in that reference; use the documented
  list functions to consume its result. For `APPROX_COUNT_DISTINCT`,
  `APPROX_QUANTILE`, and `APPROX_TOP_K`, read `Approximate Sketches`: the window needs
  `MAX STATE SIZE` after duration `WIDTH` and `STEP`, a branched window a branch with
  `MAX INSTANCES <n> EVICT LRU`, and the precision or capacity sets both accuracy and memory.
- Use a separate wire schema and codec when transport shape differs from the internal runtime
  schema. Read `Schemas And Codecs` → `Wire Schemas` for each format's vocabulary and exact
  binding matrix, and `Codecs` for the family and format heads. Select the complete head before
  writing the body; a WIRE codec and a JAQ codec of the same format have different contracts.
  Declare text interchange explicitly: `ENCODE <field> AS RFC3339` for a
  datetime in a JSON or AVRO `string` or a CBOR `text`, and `ENCODE <field> AS BASE64` for bytes
  in a JSON or AVRO `string`.
  An AVRO array names its item type (`array<long>`, `array<array<double>>`); a JSON or CBOR
  `array` is bare. A list field's `ENCODE` rule governs its innermost elements: read `List Fields`.
  Read `Value Fidelity` before relying on non-finite floats.
- For every JAQ-backed codec, use `WITH JAQ TRANSFORMATIONS` and declare `ON INGESTION`,
  `ON EMITTING`, or both in that order. At least one direction is required.
- An `ON INGESTION` program runs once per value a payload holds, and every object it yields becomes
  one message: `.[]` unfolds an array, and a program that yields nothing acknowledges the payload
  without a message. A payload is decoded or rejected as a whole, unfolds into at most 65,536
  messages, and stays the unit of ingestor acknowledgement. `ON EMITTING` must yield exactly one
  value per record.
- A payload its codec refuses is consumed, never redelivered: the ingestor's `ON GENERAL ERROR LOG`
  reports it as a decode rejection, `IGNORE` only counts it, and an acknowledged ingestor commits past
  it. Read `Ingestors` → `When The Codec Refuses A Payload` before relying on a broker's
  redelivery or dead-lettering for malformed payloads.
- Unfolding runs on the node's extension workers. Before relying on a `MODE NO ACK` ingestor to keep
  its payloads while those workers are full, read `Ingestors` → `When The Extension Workers Are
  Full`: some ingestors wait for them, and others refuse the payload and count it.
- Give every signaling protocol an explicit `FORMAT` and express the handshake as JAQ:
  `SEND JAQ` programs must each yield exactly one value, and `WAIT JAQ` matchers accept any output
  that is neither null nor false. Match only the fields that matter so acknowledgements carrying
  connection ids or timestamps still match.
- Write signaling steps in the order they must happen; each completes before the next starts, so a
  send that depends on an earlier reply goes after the wait for it. A `WAIT` step may list several
  matchers when their frames may arrive in any order. Use `CAPTURE` on a single-matcher step to
  record values, and read them in later programs through `$state`.
- Use `ACCEPT DATA` on `ON CONNECT` or on the `WAIT` step that must finish before accepting payload.
  Frames arriving before acceptance are dropped. For an outbound client's bounded handshake
  handoff and delivery after readiness, read `Ingestors` → `WEBSOCKETS CONNECTION`.
- Scope rejection to where it applies: `FAIL JAQ` on a `WAIT` step aborts during that step, and a
  `FAIL JAQ` written before `ON CONNECT` applies throughout the handshake.
- Preserve written operation order in schema, relay, junction, deduplicator, reorderer, emitter,
  ingestor, reingestor, generator, and placement ALTER statements.
  Include every dependent wire, internal, codec, and node mutation required for the candidate graph
  to validate in the same transaction.
- Treat JSON, CBOR, and AVRO wire schemas as distinct entity kinds. Their names may coincide, so
  creation, alteration and addressing must select the exact format. For codec reference syntax,
  follow `Schemas And Codecs` → `Codecs`; its head supplies the wire format.
- Use `BYTES` for arbitrary octets, including `ARRAY` and `VEC` elements. Keep branch key schemas
  free of `BYTES`. Check the wire representation and required encoding rule in `Schemas And
  Codecs` → `Codecs`; CBOR and AVRO `bytes` fields need no rule. A cast never converts `BYTES`; use
  the functions of `Expression Functions` → `Bytes, Encodings And Hashes`.
- Declare wire-schema mode after the entity name with `CREATE WIRE <format> SCHEMA <name> MODE
  STRICT|LOOSE`. Change it with `ALTER WIRE <format> SCHEMA <wire_schema> MODE STRICT|LOOSE`;
  the same format-qualified ALTER form owns field evolution.
- Call UDFs only through `udf::<name>(...)`, keep arguments exact-typed, and use `VOLATILE` only
  when the body needs the domain clock or randomness. Roto UDFs are trusted native code; keep
  untrusted custom processing in WASM.
- Include Roto `test` blocks with every agent-generated UDF. Creation runs those tests and rejects
  the UDF without persisting it when any test rejects.
- Select `BRANCHED BY <branch>` or `UNBRANCHED` explicitly. Normal processors preserve their named
  branch; use a reingestor when the graph must repartition or remove branch grouping.
- Treat placement rules as path-gated overlays, not connectivity-independent groups. Use
  `REQUIRE COLOCATION` only for a hard same-cluster-node constraint; `PREFER COLOCATION` and
  `SUGGEST SEPARATION` are soft, `NEUTRAL` leaves scheduler heuristics active, and no hard
  separation policy exists. Lower `RANK` values are stronger, unranked rules are the weakest rule
  tier, and equal-rank different-policy claims conflict.
- For partitioned processors and generators, read [Processors → Partitioned processing](../../../docs/src/processors.md#partitioned-processing)
  and [Placement Policies](../../../docs/src/placement.md). Verify the owner and replicas of every
  unit with `SHOW CLUSTER STATUS` or the node's `DESCRIBE`; size branch retention and buffering for
  all declared partitions. The chapters own locality, materialized reads and whole-node relocation.
- Treat every relay unit as scheduled work with one owner. A direct branch's `PARTITIONS n`
  gives its relay one unit per partition. `CAPACITY` bounds each unit's owner buffer, while each
  producer node and remote consumer node contributes one fixed in-flight dispatch slot. Materialized
  state adds state replicas to that relay; it does not add a
  separate runtime-node kind. All relays are valid placement members and corridor hops.
  On native 64-bit targets, `CAPACITY` accepts positive integers through `18446744073709551615`;
  creation and alteration preserve the exact capacity through storage, replay, and restart.
- Treat `HTTP ENDPOINT`, `WEBSOCKETS ENDPOINT`, and `SYSLOG LISTENER` ingestors as cluster-wide
  listeners and exclude them from placement members. Every other ingestor, a `WEBSOCKETS CONNECTION`
  included, keeps its existing live unit assignments across ordinary schedule recomputation.
  Instance-capable ingestors can have several independent owners. Read the Placement Policies
  relocation section before moving a node with several units; automatic drain and failover preserve
  unit identity.
- Use `RELOCATE <selection> ONTO NODE <node_id> FOLLOW PREFERENCES | IGNORE PREFERENCES;` to move
  chosen work onto a named cluster node. The selection is a kind-qualified list or a
  `FROM ... TO ...` corridor, hard colocation groups always move whole, and the whole unit moves in
  one gated handoff or not at all. It is a one-time move, not pinning.
- An emitter may list multiple `FROM <relay> [WHERE <expr>]` inputs when every relay declares the
  same payload schema. Unlike ordinary processors, those inputs may use differently named
  branches. Keep collection separate per source relay and concrete branch, and remember that one
  node-wide materialized dependency or message-error relay must be exact-branch compatible with
  every source. Check [emitter retention](../../../docs/src/emitters.md#branch-semantics) when
  combining inputs, long flush intervals or required materialized-state waits.
- Give every emitter an explicit transport-supported `MODE` as the final emitter subclause before
  `ENCODE USING`. Include every required variable: all modes declare `RETRY POLICY BACKOFF <d> MAX
  <d>`; asynchronous confirming modes also declare `ACK SEQUENTIAL` or `ACK PARALLEL MAX <n>` and
  `ACK TIMEOUT <d>`. Do not invent a default mode, window, timeout, or retry cadence.
- When an application receives constructed graph output, use `CREATE CLIENT CONSUMER EMITTER <name> FROM <relays> TO SCHEMA <output_schema>
  MODE ACK SEQUENTIAL|ACK PARALLEL MAX <n> ACK TIMEOUT <d> RETRY POLICY BACKOFF <d> MAX <d>`.
  Follow route construction with required `BATCH MAX MESSAGES <1..65536> MAX SIZE <bytes>` and an
  explicit `FLUSH` policy. The output schema is exact, and every sensitive value copied into a
  non-sensitive output field needs `leak_sensitive(...)`. Do not create a `CLIENT` model, codec,
  header operation, or direct `VALUES` body for this emitter. An application opens a competing
  consumer with the Rust client's `subscribe_emitter` or the session protocol, receives native
  Arrow, then explicitly ACKs, retries, or rejects each attempt. `ATTACHED` holds ingestor ACKs
  until the application ACKs; `DETACHED` releases them earlier. Retries preserve the bytes
  and delivery identity but use a fresh reference. Consumers are volatile across owner and
  session loss; the Rust client may restore the same handle with a fresh attachment only when the
  domain `START` generation and endpoint contract still match. The first read after a session gap
  or endpoint relocation reports interruption, and an old delivery reference cannot ACK a
  replacement attempt. Design
  application effects for duplicates and uncertain ACK confirmation. Read `Emitters` → `Client emitters`,
  `Sessions` → `Emitter Consumers`, and the Client Implementation Manual for limits and recovery.
- Request/response emitters do not take `ACK TIMEOUT`. ClickHouse, OTEL and Sentry clients require a
  positive `timeout_ms`; SQS or Iceberg object-store clients use it when a request
  needs an explicit bound, which for SQS bounds every request of both directions. A ClickHouse HTTPS address also requires `tls_ca_file` from a mounted
  resource, while HTTP refuses TLS file keys. Its table locator has one or two case-preserving
  dot-separated parts, each quoted by the connector when sent. The emitter's declared retry
  policy owns pacing after that request fails. A ZeroMQ client's `timeout_ms` bounds each send,
  which otherwise waits for as long as the peer reads nothing. Prefer a bound ZeroMQ PULL
  ingestor and a connecting ZeroMQ emitter: the driver never reconnects a connecting PULL socket
  after its peer goes away, and a
  bound emitter can keep failing after one failed publish; read `Ingestors` → `ZeroMQ` and
  `Emitters` → `ZeroMQ`. OTEL clients
  must also select `grpc` or `http/protobuf` explicitly with the required `protocol` key. An OTEL
  endpoint may use a DNS hostname on either protocol: the emitter opens without contacting it, the
  node's resolver answers when an export needs a connection, and the configured hostname remains
  the HTTPS certificate name. An endpoint that does not resolve or answer is a publish failure
  retried on the emitter's declared retry policy, never an initialization error.
- For a ZeroMQ PULL/PUSH pair, use `MODE NO ACK` on both sides and the typed `ZEROMQ CLIENT`
  `addr` and `bind` keys. A PULL ingestor reads frame 0 only and skips messages with no frames;
  PUSH sends one payload frame without a receiver receipt. TCP hostnames use the ZeroMQ driver's
  system resolver. Read [Ingestors](../../../docs/src/ingestors.md#zeromq) and
  [Emitters](../../../docs/src/emitters.md#zeromq) before setting bind direction and deadlines.
- Client deadline keys mean the same thing everywhere: `timeout_ms` bounds a whole request,
  name resolution, connection and TLS included, `connect_timeout_ms` bounds establishing a
  connection, the protocol handshake included, and `idle_timeout_ms` bounds how long a connection
  may carry nothing while its connector waits to read. RabbitMQ, Redis, Syslog emitter, ZeroMQ
  emitter and WebSocket clients take `connect_timeout_ms` and allow 30 seconds without it, while a
  ZeroMQ PULL ingestor reads none of these keys; a RabbitMQ client's deadline also covers the AMQP
  handshake and opening the entity's channel, and its `timeout_ms` bounds the round trip that ends
  a `NO ACK` write. A NATS client's `timeout_ms` bounds a Core NATS write and its flush to the
  client's socket, which is not a server round trip. A Syslog emitter's client bounds each frame
  write by `timeout_ms`; a TLS Syslog listener's client bounds each accepted handshake by
  `connect_timeout_ms`, 30 seconds without it, and a TCP or TLS listener's client may close idle
  connections with `idle_timeout_ms`. Put each key only on a client whose direction performs the
  wait it bounds. Read `Ingestors` or
  `Emitters` → `Client Deadlines`, and `Connector Capabilities` for each entity's waits and what
  bounds them.
- For HTTP polling, keep a credential out of the statement: `SHOW CREATE` prints a `HEADERS` literal.
  Declare a `secret.<name>` key on the `HTTP` client and write
  `'authorization' = CLIENT SECRET 'secret.<name>'`; see
  [HTTP REQUEST](../../../docs/src/ingestors.md#http-polling-requests). Change `METHOD`, `HEADERS` or
  `MAX BODY SIZE` in place with `ALTER HTTP REQUEST INGESTOR … SET METHOD …`, `SET|DROP HEADERS`
  or `SET|DROP MAX BODY SIZE`. A poll goes to the client's `endpoint` exactly as written, path and
  query included, so declare one client for each polled resource; a client an `HTTP REQUEST`
  emitter also uses must name an origin, and
  [`HTTP CLIENT`](../../../docs/src/ingestors.md#http-client) lists the keys both directions share.
- For an HTTP emitter, write `CREATE HTTP REQUEST EMITTER <name> FROM <relay> TO <client> METHOD <string_expression> PATH
  <string_expression> MODE ACK RETRY POLICY BACKOFF <duration> MAX <duration>` followed by exactly
  one of `ENCODE USING <codec>` or `WITHOUT BODY`. Do not add an ACK window, `ACK TIMEOUT`,
  `NO ACK` or `BATCH`; see [Emitters](../../../docs/src/emitters.md#http-request-configuration)
  for client origin and timeout requirements, request-field types and sensitivity, bodyless
  construction, and ALTER rules. Each record is one request: route `WHERE` filters before any
  request field is evaluated, a failed method, path, header write or body encoding rejects the
  record through `ON MESSAGE ERROR` before any part of its request is sent, and a retry resends the
  prepared request unchanged. Declare
  `Content-Type` and any idempotency key with `write_header`, whose valid UTF-8 value reaches
  the request as the same bytes, including non-ASCII characters; see
  [HTTP requests](../../../docs/src/emitters.md#http-requests). Complete valid `2xx` response
  headers deliver the record without waiting for the response body. `401`, `403`, `407`, `408`,
  `425`, `429`, and `5xx` retain it for retry; other `3xx`/`4xx` statuses and `101` reject that
  record through `ON MESSAGE ERROR`. An HTTP client follows a redirect, on both sides, only with
  `'follow_redirects' = true` and only to its own origin, and uses only its declared `'proxy'`,
  never one from the environment; see
  [HTTP client transport](../../../docs/src/emitters.md#http-client-transport). A retained request holds later records behind it, and an
  `ATTACHED` emitter keeps every unresolved record's upstream acknowledgement open. One valid
  `Retry-After`, in whole seconds or as an HTTP date, can lengthen the physical wait beyond `MAX`.
  A lost response can repeat a request the endpoint applied, so give the endpoint a stable key
  such as `write_header('Idempotency-Key', input.event_id)`; see
  [HTTP retries](../../../docs/src/emitters.md#http-retries-and-acknowledgements). While a request
  is pending after a failed attempt, `DESCRIBE EMITTER` shows that failure's status or transport
  cause as its transient error until the request is delivered. Sent counters count each delivered
  record once and never a refused one, and a `WITHOUT BODY` emitter sends zero payload bytes; see
  [HTTP inspection](../../../docs/src/emitters.md#http-inspection-and-metrics).
- Write a supported emitter's optional `BATCH MAX MESSAGES <1..65536> MAX SIZE <bytes>` after the complete
  emitter clause and route construction, before `FLUSH`; it is required for CLIENT, ClickHouse,
  Postgres, MySQL, and MongoDB emitters and limited to `1MiB` for SQS. A batching Sentry emitter needs a
  codec with `ON EMITTING BATCH`, and a batching protobuf codec needs `BATCH MESSAGE`. Compatible
  rows from successive Arrow carriers in one flush may share a payload, but rows from different
  source relays or concrete branches cannot. For the database emitters, the clause bounds each insert
  or bulk write by row count and the exact size of what it carries; for OTEL, it bounds each
  protobuf export request by successful source-record count and uncompressed encoded size, and an
  OTEL request never combines records of two Arrow carriers; see
  [Emitters](../../../docs/src/emitters.md#batching).
  For Kafka/Pulsar record keys, destination partitions and SQS FIFO group expressions, read
  [Emitters → Envelope Clauses](../../../docs/src/emitters.md#envelope-clauses) and the connector's
  section before choosing the expression and externally provisioned topic or queue.
- Give every client resource mount an explicit `MOUNT <resource> VERSION <u64>|LATEST` clause. Put
  database pool bounds before the mount, and keep a WebSocket client's `WITH SIGNALING PROTOCOL`
  clause before the mount. `LATEST` is resolved when the statement is applied; the stored client
  and `SHOW CREATE CLIENT` contain the resulting number, so later uploads and restarts do not move
  the mount.
- Give every hash map an explicit `FROM RESOURCE <resource> VERSION <u64>|LATEST` clause. `LATEST`
  is resolved when the statement is applied, and the stored hash map and `SHOW CREATE HASH MAP`
  contain the resulting number.
- Declare connection-pool bounds on every `POSTGRES`, `MYSQL`, `MONGODB`, and `REDIS` client, after
  the client name and before an optional `MOUNT`: `POOL SIZE MIN <u32> MAX <positive_u32>`, in that order,
  with the minimum no greater than the maximum. The clause is required even when only ingestors
  reference the client, and no other client type accepts it. Never put pool sizing in connector
  `CONFIG` or an address query parameter. One pool serves every local user of a named client on one
  node, so size it for the node's whole workload rather than per emitter, and expect one pool per
  node the client is placed on. Read the dedicated `Common` → `Database Client Connection Pools`
  documentation entry before choosing values.
- For a `MYSQL` client's checkout budget, connection lifetimes and minimum-maintenance interval,
  read `Connection lifetimes` in `Database Client Connection Pools`. Keep the checkout budget
  separate from the insert's operation bound, and expect maximum age to allow an active insert to finish.
- A `REDIS` client's command pool and a `POSTGRES` client's pool take `pool_idle_timeout_ms` and
  `pool_max_lifetime_ms`, 10 and 30 minutes by default; Redis never applies them to a Pub/Sub
  ingestor's subscription connection. A `POSTGRES` client also takes `pool_acquire_timeout_ms`, 30
  seconds by default, and `pool_test_before_acquire`, `true` by default. Read `Connection lifetimes`
  in that same pools entry before choosing values; `DESCRIBE CLIENT <name>` shows them, marking an
  omitted one `(default)`.
- Give a `POSTGRES` client an absolute `postgres://` or `postgresql://` `addr` URL that sets
  `sslmode` once, to either `disable` or `verify-full`; no other mode is accepted and there is no
  opportunistic fallback. Mounted `tls_ca_file`, `tls_cert_file`, and `tls_key_file` are the
  TLS-file interface, certificate and key must be supplied together, and TLS files require
  `verify-full`. A `POSTGRES TABLE` names `'Orders'` or `'sales.Orders'`, each part sent with its
  case kept; `ON CONFLICT (...) DO UPDATE` needs a target and a mapped column outside it, and
  `MAX SIZE` is at most `1073741822B`. Keep its optional statement `timeout_ms` separate from
  `pool_acquire_timeout_ms`; read their bounds and retry semantics in
  [Emitters → Postgres](../../../docs/src/emitters.md#postgres). Write every connection part in
  `addr`: the driver fills a missing port, user, password or database from the node's `PG*`
  environment and password file, and ignores a query option it does not know. `verify-full` trusts
  the driver's bundled Mozilla roots as well as `tls_ca_file`. Map an `ARRAY` or `VEC` value to a
  `json`, `jsonb` or `text` column, never a native array column. Only SQLSTATE classes 21, 22 and
  23 reject a row; a missing table or column, an unmatched conflict target or a missing privilege
  retries on `RETRY POLICY` until it is fixed.
- Give a `RABBITMQ` client an `amqp://` or `amqps://` `addr` that names a host and no port 0; Nervix
  refuses any other address before commit. Mounted `tls_ca_file`, `tls_cert_file` and
  `tls_key_file` require `amqps`; `CREATE` refuses these keys with `amqp`. Certificate and key must
  be supplied together, and the connection presents them to a broker that requires a client
  certificate. Read
  [Ingestors → RabbitMQ](../../../docs/src/ingestors.md#rabbitmq).
- Treat every route as a newly constructed output. Add `INHERIT` only where that node permits it,
  and initialize every required output field on set-only routes.
- Add a route-local message error policy. Add the required general/global policy for the chosen
  node.
- Add `COLLECT FOR <duration> [MAX BATCH SIZE <bytes>]` after a relay input list only when the node
  should assemble input batches before execution. Omission means no additional input collection.
  The policy is per source relay and concrete branch; correlators configure each side
  independently. Its duration uses the domain clock and starts when an empty collector receives
  data. Never add it to an ingestor.
- Add `FLUSH EACH <duration> MAX BATCH SIZE <bytes>` or `FLUSH IMMEDIATE` to every flush-based
  route. `FLUSH EACH` is domain-logical and branch-local, in emitters and the Iceberg `COMMIT EACH`
  exactly as elsewhere, while emitter retry backoff, acknowledgement keepalive, emitter
  acknowledgement timeouts, and an HTTP `Retry-After` stay physical. Treat `FLUSH IMMEDIATE` as the
  physical, system-owned 100 µs minimum batching window, not a one-message batch guarantee; domain
  pacing never scales it. Both timers start when an empty route buffer receives data. `MAX BATCH SIZE`
  counts logical Arrow value, offset, and validity bytes, not unused buffer capacity or object
  overhead. Windows use `WIDTH` and `STEP`; WASM output cadence is controlled by the guest. Choose
  `FLUSH` values as latency and boundary-cost controls, not as a throughput lever: `MAX BATCH SIZE`
  only clamps a batch, and the flush tuning guidance in the docs records which emitters benefit from
  larger batches.
- Use delivery-mode `MAX <n>` with `ACK PARALLEL` or an endpoint's `ACK`; an ingestor's `NO ACK`
  and an emitter's `NO ACK` have no in-flight ACK window and never accept `MAX`.
- Do not promise that an emitter's `NO ACK` survives a stop on every emitter. A `STOP`, a replacement
  or a node shutdown sends what a Kafka producer still queues, awaits the receipts of what a Pulsar
  producer accepted and waits for an MQTT QoS 0 client to send what it holds, all within its
  deadline, reporting what is outstanding when it passes. Choose an `ACK` mode where a stop must not
  lose emitted records; `Emitters` → `ACK Semantics And Effective Delivery` has the rule.
- Name the connector and entity in every emitter head, such as `CREATE KAFKA TOPIC EMITTER`,
  `CREATE HTTP REQUEST EMITTER`, `CREATE OTEL METRICS EMITTER`, or `CREATE ICEBERG TABLE EMITTER`.
  After `TO`, write its reference, quoted external locator and entity options, then `MODE`, then
  its codec, `VALUES` mapping or `WITHOUT BODY`. Client consumers use `TO SCHEMA <schema> MODE ACK`.
  Iceberg resolves its storage backend from its client; put required `NAMESPACE`, then `LOCATION`,
  `CATALOG` and `COMMIT EACH` before `MODE`. Put OTEL attributes, resource and scope with `VALUES`
  after `MODE`.
  Use the stored kind in every emitter ALTER head. `SET TO` requires that entity's complete body,
  including its payload selection; changing the connector or entity requires drop and create. Change
  one part in place with `SET <locator>`, `SET|DROP <option>`, `SET MODE`, `SET|DROP <envelope
  clause>`, the construction operations and `SET GENERAL ERROR`; see `Emitters` → `Altering
  emitters`.
  External table and collection names retain case. Keep client/schema/codec references as identifiers.
- Put an explicit processor or emitter `ATTACHED | DETACHED` mode after the complete inputs and
  optional `COLLECT FOR`; on a correlator use the position after `RIGHT FROM`. Read `Processors`
  → `Shared input and branch contracts` and `Emitters` → `Statement heads and external names`
  for clause order and defaults.
- Name the connector and entity in every ingestor head, such as `CREATE KAFKA TOPIC INGESTOR`,
  `CREATE HTTP ENDPOINT INGESTOR`, or `CREATE SYSLOG LISTENER INGESTOR`, and write the body after
  `FROM` without a connector keyword. Reference a client of the head's connector, and pair
  `HTTP ENDPOINT` with an `HTTP ENDPOINT` declaration and `WEBSOCKETS ENDPOINT` with a
  `WEBSOCKETS ENDPOINT` declaration. The endpoint head selects its fields; see `Ingestors` →
  `HTTP ENDPOINT And WEBSOCKETS ENDPOINT` for the declarations and optional signaling clause.
  Write every ingestor's `MODE`, using `MODE NO ACK` where nothing waits on an
  acknowledgement and only the modes `Ingestors` → `Delivery Modes` lists for that entity.
- Write external names as string literals spelled exactly as the external system names them:
  topics, consumer groups, subscriptions, queues, channels, subjects, queue groups, and MQTT topic
  filters, within the rules in `Ingestors` → `External Names`. Keep Nervix references such as
  clients, endpoints, schemas, codecs, and relays bare identifiers.
- When an application publishes typed batches itself, use a client ingestor:
  `CREATE CLIENT PRODUCER INGESTOR <name> FROM SCHEMA <schema> MODE ACK SEQUENTIAL|ACK PARALLEL MAX
  <n> ACK TIMEOUT <d> RETRY POLICY BACKOFF <d> MAX <d> ON QUIESCE SUSPEND`, with no client, codec,
  `DECODE USING`, headers, or `NO ACK`. Producers are opened through the client library or session
  protocol, not with a statement. Read `Ingestors` → `Client Ingestors` before explaining
  outcomes, limits, or what ends a producer, and never promise that a batch whose outcome is
  unknown or failed had no effect. The Rust client restores a desired producer across a session gap
  or relocation only while the domain `START` generation and endpoint contract still match; it
  never resends a batch merely because its outcome is missing.
- End every transport ingestor body with an explicit declared `ON QUIESCE` body
  immediately before `DECODE USING`. Include positive `MAX SIZE` and, outside `HTTP ENDPOINT` and
  `WEBSOCKETS ENDPOINT`, an explicit `ON OVERFLOW DROP OLDEST|DROP NEWEST` for `BUFFER`; include
  `RETRY AFTER` for endpoint `REJECT`. Write every `MQTT TOPIC` ingestor's `SESSION CLEAN` or
  `SESSION PERSISTENT` and its `QOS 0`, `QOS 1` or `QOS 2` before `MODE`, and use its `ACK` modes
  and `SUSPEND` only with `SESSION PERSISTENT` at `QOS 1` or `QOS 2`. Write every `MQTT TOPIC`
  emitter's `QOS 0`, `QOS 1` or `QOS 2` before `MODE`, with `NO ACK` only at `QOS 0` and `ACK` only
  at `QOS 1` or `QOS 2`. Do not invent a default or use a mode offered by another entity.
- A server endpoint WebSocket keeps its route and signaling protocol for the connection. After
  its ingestor stops or is replaced, reconnect to use the new ingestor; after signaling, the next
  refused payload on the preceding connection closes with 1013. Configured routes without a live
  intake reject with HTTP 503, while withdrawn routes return 404. A node that has not installed a
  revision since it started answers 503 for every unpublished route, never 404. Another domain
  sharing the host and path retains its own intake when a domain is stopped.
- Declare both required WASM limits immediately after `FILE`, in order: `MAX FUEL <positive_u64>
  MAX MEMORY <positive_byte_size>`. Fuel is reset per logical guest operation; memory caps each
  branch guest's Wasmtime linear memory.
- Write the optional `ON REJECTED STATE PRESERVE|RESET` between the last `TO` clause and
  `ON GLOBAL ERROR`. It answers only a guest's own verdict on the snapshot it was handed, and it
  defaults to `PRESERVE`. Offer `RESET` only when the user accepts losing that branch's computation
  state, and say that it replaces the lifetime once rather than retrying. Never present it as a
  recovery for a compile, initialization, fuel, memory, storage, replication, or authority failure;
  the policy discards state for none of those under either value. Only an owner loss whose forced
  recovery cannot prepare the new owner recreates guest state without the guest's verdict.
- To intentionally replace an existing WASM processor's guest state, use
  `RESET WASM PROCESSOR <processor> STATE FOR UNBRANCHED|ALL BRANCHES|BRANCH VALUES { <field> = <literal>, ... };`.
  It acts on the session's active domain, so `USE` that domain first.
  Choose the explicit scope that matches its declared branch, and supply every branch-key field
  with an exact typed literal for one selected branch. The processor must be running and the
  selected branch must already exist. This command loses the selected computation state; a fresh
  command reference is a new reset, while retrying the same reference returns its original outcome.
  Keep that reference when publication loses leadership while the reset is applying;
  `Command Completion` defines recovery and retained failures.
  See the reset section in `Runtime Nodes` and `Command Completion` before suggesting it.
- To inspect WASM guest-state progress, use `DESCRIBE WASM PROCESSOR <processor> [FORMAT TEXT|JSON];`.
  Read its reset phase and generation beside the checkpoint revision and replica counts; both
  formats use the same typed state inspection. See `Runtime Nodes` and `WASM Processor Guests` for
  the meaning of each stage and the bounded branch details.
- On a flush-based route, treat `ON MESSAGE ERROR SEND TO` as a separately buffered error output
  governed by that route's same interval and maximum batch-size boundaries. General/global errors
  are node-wide and do not inherit route-local `FLUSH`.
- Declare the error relay with the input route's exact named branch, or unbranched for an
  unbranched input. See `Runtime Nodes` for the branch and error-record contract.
- Require explicit sensitive-value leakage for external emission. Never place real credentials in
  an example unless the user explicitly supplied and requested them; prefer obvious placeholders.
- Preserve connector configuration as the documented string key/value surface. Do not translate
  options between different client libraries.
- Use only connector kinds listed in the current Ingestors and Emitters documentation. Treat
  connector syntax retained in older examples or configurations as invalid.
- For syslog, read the dedicated `Common` → `Syslog` documentation entry. Reference the predefined
  singleton wire schema with a `SYSLOG CODEC` head, keep the format independent of the UDP/TCP/TLS
  client transport, and use only the documented `SYSLOG LISTENER INGESTOR ... MODE NO ACK` ingestor
  and `SYSLOG COLLECTOR EMITTER ... MODE NO ACK` forms. Read its `Client`, `Quiesce` and `Emitter`
  sections before choosing direction-specific keys, promising retention across a suspension, or
  sizing a frame; a successful collector write confirms local socket acceptance and flush.
- For a Sentry emitter, reference a `SENTRY CLIENT` containing the project DSN and a positive
  `timeout_ms`, and use a codec that emits one Sentry event JSON object per record. Do not add header
  writes to the Sentry route.
- List topics, queues, streams, tables, buckets, catalogs, namespaces, collections, and other
  external prerequisites separately. Nervix does not create them as a side effect of starting a
  node.

## Deliver usable configuration

When authoring a graph, provide:

1. Assumptions and external prerequisites.
2. Ordered command phases, separating client-local commands from transactional server statements.
3. Complete Plexus with consistent names and no unexplained ellipses. Use placeholders only for
   genuinely deployment-specific values such as endpoints, credentials, file paths, and external
   entity names.
4. A short verification sequence using the relevant `SHOW`, `DESCRIBE`, lookup, or subscription
   commands.

Use `DESCRIBE RELAY <relay>;` to verify its owner and optional state replicas; an ordinary relay
reports no replicas. Use `DESCRIBE JUNCTION <junction>;` when the verification should include a junction's stored
routing contract, scheduled placement, and local edge metrics.

Use `SHOW PLACEMENTS;`, `DESCRIBE PLACEMENT <placement>;`, and `DESCRIBE DOMAIN;` to verify rule
coverage, hop classes, effective policies, exact unit groups, and domain-wide scope. Use
`DESCRIBE RELOCATION ...;` with the clauses of a planned `RELOCATE` to inspect the unit it would
move, its quiesce level, the relays its hold would gate, and the preferences it would leave
unsatisfied.

Before returning the configuration, trace every reference to its declaration and check schema,
branch, construction, flush, error, sensitivity, transaction, and external-provisioning contracts.
If the public docs do not establish a requested capability, say it is not documented as supported
instead of inventing syntax.

For password creation policy, read [Node Settings → Password Creation Policy](../../../docs/src/node-settings.md#password-creation-policy)
and [Cryptography → Password Credentials](../../../docs/src/cryptography.md#password-credentials).
Use the committed cluster setting for creation cost; preserve the documented startup, replay and restore rules.

For node CPU admission, memory ceilings, native runtime threads, client byte credit and derived
watermarks, read [Node Settings → Execution Capabilities](../../../docs/src/node-settings.md#execution-capabilities)
before suggesting local overrides or interpreting DESCRIBE CLUSTER capacity lines.
