# Integration Test Lifecycle

*Plexus, the Nervix graph definition language.*

## Paced Interconnect Fixture

The rate-limited follower catch-up scenario starts a TCP byte pacer before restarting a stopped
node. Its listener takes the existing advertised endpoint and forwards opaque bytes to a private
interconnect listener on the restarted node. The advertised endpoint stays fixed so retained peer
routes also reach the pacer. Its port remains reserved for the test run, as for other retired test
endpoints. The host, TLS identity, resolver policy and product deadlines stay fixed. Each accepted
connection uses a 1024-byte inbound buffer. The first chunk waits for its byte-rate interval;
later chunks follow one monotonic byte schedule, so a late timer wake is not charged again on the
next chunk. After a source has been idle, one available chunk can go promptly and the schedule
resumes at its configured rate. Responses go promptly. A scenario-owned task set retains and reaps
connections. Cancellation or fixture drop closes the listener and aborts all its connections; the
fixture cannot survive its node specification.

The rate-limited case transfers leadership to node-2 while the stopped follower has only its
initial log, making the new leader recover an unconfirmed previous-log boundary. The scenario
starts the follower without the harness's catch-up retries, records append-stream
opening and setup-attempt baselines before that start, and waits for the applied domain backlog
within its explicit assertion bound. Every five seconds it records applied domains, both stream
counters for each source, bytes forwarded by the pacer, and each node's applied revision and log,
snapshot and purged indexes. On failure it also records a concise status for initialization,
leader and voter membership. The opening count is a diagnostic observation of data streams;
initial empty probes use the management path. An atomic maximum of bytes forwarded on one
connection proves that the paced path carried more than one production progress interval. The
pacer also counts accepted, connected, failed, cancelled and completed connections, and total
bytes. These observations do not inspect TLS or frame bytes or establish ordering the product
relies on.

A focused startup scenario occupies the observability port to force the first launch to fail
before runtime initialization.
The harness joins that attempt and retries on a fresh observability port within the same readiness
budget. The pacer's advertised endpoint, private target and retained connections stay in place.

The Cucumber scenario suite runs Nervix the way an operator does. It starts clusters, drives them
through the session API, brokers, and HTTP endpoints, and asserts on what they do. Many scenarios
run at once in one test process, each with nodes of its own, so the harness around them is a
concurrent system with failure modes of its own: a node that never becomes ready, a status request
that never gets a reply, a node that never stops, a step that never returns.

This chapter defines the harness's lifecycle contract: how a node, a scenario, and the whole run
start, how the harness observes them, how it stops them, and which deadline bounds each of those
steps. Three rules hold throughout.

- Every lifecycle operation has one owner and one absolute deadline, fixed when its phase starts. A
  retry, a poll, or a cleanup inside the phase receives only what is left of it.
- Every ending has a typed outcome. A node task is running or ended in one of four ways, a readiness
  probe reports what it observed, and a scenario publishes the phase it is in. No harness path
  reports an unknown reason.
- The run ends itself before the CI job limit and leaves the job time to upload the diagnostics it
  produced.

The harness is test code outside the layer order: it may name any layer, and no product code names
it. It observes product behavior and never redefines it. Product deadlines stay with the chapters
that own them: node shutdown with [Shutdown And Recovery](./shutdown.md), node-to-node communication
with [Cluster Interconnect](./interconnect.md), client requests, redirects, reconnects, and exact
recovery with [Client Session Protocol](./client-session-protocol.md), and command identity and
completion with [Command Completion](./command-completion.md).

## What Runs Where

The emitter assembly outline observes the emitter's public received-message total before each
switch between its two source relays. It waits for all three initial A rows before posting B, and
both B rows before returning to A. An endpoint's `NO ACK` response confirms intake admission;
independent ready relay queues can still interleave at the emitter. The owner regression queues
B5/B6 before A4/A7 after receiving A3 and observes B5, A4, B6, A7. The packing regression requires
separate payloads at each of those source boundaries. The public outline retains all seven rows,
both named branches, source headers, both count and byte limits, one- and three-node clusters,
the twenty-second flush and the forty-five-second exact-payload assertion. The receipt waits end
when their condition holds and do not serialize other scenarios.

The orphaned-transaction expiry scenario holds finished-transaction cleanup for its domain on each
node before it begins the transaction. Expiry and the leadership transfer proceed normally. The
successor reaches its cleanup barrier only after it observes the replicated terminal transaction;
public inspection and attach then verify `EXPIRED` while that record remains retained. Releasing
every node's barrier lets the normal retention sweep remove it, which the scenario also verifies
through inspection and attach. The idle timeout, tombstone age and assertion deadlines stay fixed.
This orders the observations against cleanup instead of assuming leadership transfer and client
round trips finish inside the five-second retention window. The testing-only barrier uses the
existing one-shot pause owner; scenario teardown releases it before stopping nodes, including after
an assertion fails. It neither changes product retention nor serializes other scenarios.

The Pulsar finish scenario runs a public `STOP` concurrently with a timed resumption of its
paused dependency forwarder. The forwarder's resumption future owns its control and cancellation
handles and is awaited beside the command, so no timer task outlives the scenario. It records
the resumption instant before reopening the gate; the command records its completion instant
when it returns. The scenario requires completion after resumption and observes the retained payloads
exactly once after recovery. The longer receipt scenario uses the same fault control to delay the broker
beyond thirty seconds under an explicitly declared forty-five-second `ACK TIMEOUT`. These
dependency faults do not widen a product deadline or replace a readiness observation with a sleep.

The Kafka control-execution scenario uses librdkafka's mock broker to hold replies after a DOMAIN
source delivered its first record. A codec alteration resumes that source, and the scenario waits
for a running connector-storage job before sending a record through another HTTP ingestor. The
same oracle runs on one and three nodes. The broker's native round-trip control releases replies
in the scenario and again during teardown before diagnostics or node drain, including after a
failed assertion. It does not change a product deadline or use a readiness sleep.

The Kafka explicit-start scenarios use a provisioned real topic and an independent broker observer.
Their exact payload sets distinguish new work from acknowledged records replayed after a codec
quiescence, full-cluster restart or partition growth. A later explicit start must replay its selected
history. The restore outline captures the running domain, replaces the cluster with fresh stores,
and requires only new records after `RESUME`. Both topologies use the production sticky scheduler
for recovery from persisted state. Their diagnostic inventory includes active/order execution and
the corresponding lifecycle or restore stress selection. No phase or suite budget changes.

The Kafka consumer-retirement scenario holds the same broker's replies before it drops an ingestor
whose eight instances joined their group, so destroying each consumer waits for its LeaveGroup
answer. It waits for a running connector-storage job before sending a record through an HTTP
ingestor, then releases the replies and waits for the class's pending and running jobs to return to
zero. The same oracle runs on one and three nodes.

The Kafka producer-retirement scenario checks that the owner of a Kafka emitter has run no
connector-storage job, holds the same broker's replies and drops the emitter. It then sends a record
through an HTTP ingestor and waits for that class's completed jobs on the owner to reach one, the
producer's destruction, before it releases the replies and waits for the class's pending and running
jobs to return to zero. The same oracle runs on one and three nodes.

The Pulsar fault forwarder decodes broker frames and advertises its own plain-text service URL
in topic lookup responses, retaining every other command and payload field. The driver's discovered
producer and consumer connections therefore stay behind the same pause gate as bootstrap traffic.
An opaque bootstrap proxy alone does not establish a Pulsar broker fault because discovery can
otherwise connect directly to the broker. The exact payload oracle checks each sequence once after
recovery, including a producer with all 100 receipt slots occupied.

The Pulsar producer-finish outline posts through an acknowledged HTTP endpoint, `MODE ACK MAX
100 ACK TIMEOUT 60s` with `ON QUIESCE REJECT RETRY AFTER 1s`. Its window holds the fixture's maximum
of one hundred concurrent posts. One client posts them concurrently and requires HTTP 200 with a
completed endpoint outcome within each request's sixty-second response budget. The Pulsar emitter
stays in `MODE NO ACK`, so each post completes once that producer accepts its record, without
waiting for the paused broker's receipt. The sent counter's thirty-second wait then observes
producer acceptance rather than timing a queued relay backlog after an intake-only 202 response.
STOP still has to await the broker receipts after the endpoint resumes, with the same drain bound,
measured resume ordering and exact sequenced-payload oracle on one and three nodes. The separate
long-receipt outline keeps its own intake mode.

The MySQL shared-pool isolation scenario owns independent root sessions holding `LOCK TABLES`
write locks. It observes a pending metadata lock on the server before queuing the next writer,
and observes each emitter's public pool-wait state to establish the borrowing order. Releasing
the first table leaves the second locked while the scenario checks the poison write's rejection,
all mapped rows and the insert log's connection identity. This is a one-node scenario because all
three emitters must share one node's pool of exactly one connection. Each table lock is explicitly
released and its session disconnected; the scenario's cleanup releases remaining locks before
cluster shutdown, including when an assertion fails. These are external database locks, outside
Deloxide's tracked thread-blocking locks.

The Iceberg refused-append scenario stands a gate in front of the REST catalog the run shares and
points only its own emitter's catalog client at it. The gate recognizes a commit to the scenario's
table by its method and path, holds it until a step refuses it with the `409 Conflict` a catalog
answers a commit whose requirements no longer hold, or passes it to the catalog, and forwards every
other request unchanged. It records when it refused a commit and when the next one arrived, so the
scenario requires the retried commit to follow its refusal by at least the emitter's backoff
instead of observing a silent window. While the gate holds a commit, that commit cannot complete, so
the scenario reads the staging directory, the table and the producer's batch outcomes as states
that cannot move.

Before starting ordinary CI producers, Check verifies its exact tested merge and every changed
path. A verified documentation-only selection excludes `scenarios` with the other ordinary suites;
validation and title checks remain enabled. This exclusion records a source decision rather than
scenario execution, and cancellation of a required run never supplies successful evidence. Explicit
mode labels retain their own selections. [Documentation-only Selection](./property-testing-and-fuzzing.md#documentation-only-selection)
owns the allowed paths, immutable event snapshot and retained CI selection record.

The suite is the `scenarios` test target, `tests/scenarios.rs`, running the features under
`tests/features`. Its lifecycle owners live in `tests/common`.

The relay-channel turnover scenario posts through an acknowledged HTTP endpoint on a node other
than the relay owner. Its generated-payload step reuses one HTTP client and requires each request
to return HTTP 200 with the complete `completed` / `none` outcome before posting the next tenant.
The client bounds each response, including its body, at 60 seconds; the fixture declares a
30-second graph acknowledgement timeout and a window of one. This bounds outstanding graph work
while the two waves of 600 tenants and the warm-up exercise their 1,201 channel incarnations. The
scenario still observes all 601 first-generation branch expirations and requires every generated
payload exactly once at the independent broker observer. A failed request fails the step without
resubmitting an outcome that might already have produced output.

| Element | Where it runs | Owner |
| --- | --- | --- |
| Scenario steps and hooks | One runner task: Cucumber polls every running scenario, and the suite watchdog around them, from the task the binary's main thread blocks on | `tests/scenarios.rs` |
| Scenario admission and timing | Feature permits, prioritized run slots, the long-running declaration, queue reasons, and accumulated attempt times in the runner process | `scenario_schedule.rs` and `scenario_phase.rs` |
| In-process nodes | One Tokio task per node on the binary's multi-threaded runtime, which has one worker thread per CPU | The cluster fixture, `tests/common/cluster.rs` |
| Server processes | Child processes executing the `nervix-server` binary | The server-process fixture, `tests/common/server_process.rs` |
| Real-process cluster | Three server children with separate durable stores, ports and identities under one test certificate authority | `tests/common/server_process_cluster.rs`, using the server-process fixture |
| Deadlock diagnostics | In a scenario binary built for the `deloxide` mode only: one deadlock detector for the whole process, installed by the binary's `main`, and a diagnostic run for each server child, each recording evidence in a directory of its own | `tests/scenarios.rs`; the `@deadlock_diagnostics` steps, `tests/deadlock_diagnostics` |
| CLI sessions | Child processes executing `nervix-cli`. One-shot commands and the `subscribe` and `domain-clock` subcommands run on pipes; the subscription reader retains at most 256 output lines and the clock reader retains at most 2,048. The interactive REPL runs on a pseudo-terminal, whose fixture retains the newest 256 KiB the terminal displayed | The scenario world, `tests/scenarios.rs`; the terminal fixture, `tests/common/cli_terminal.rs` |
| Test dependencies | Containers started on first use and shared by every scenario of the run | `nervix-test-environment`, through `tests/common/dependencies.rs` |
| Endpoint response observations | One abort-on-drop task per named POST, owning the request through response-body collection. The HTTP client bounds it at 60 seconds; the collection step also bounds the join. Response arrival uses a monotonic instant, compared with a boundary recorded before a consumer ACK/NACK, so an early answer cannot pass an ordering assertion | `tests/endpoint_intake/requests.rs` and the endpoint ACK scenarios |
| HTTP receivers | Tasks on the binary's runtime, one listener and one task per connection, owned by the scenario that started them | The HTTP receiver fixture, `tests/common/http_receiver.rs` |
| HTTP proxies | Tasks on the binary's runtime, one listener and one task per connection, owned by the scenario that started them | The HTTP proxy fixture, `tests/common/http_proxy.rs` |
| Unaccepted addresses | No task: a listening socket at a chosen loopback address and the one connection that fills its accept queue, so the kernel drops every later handshake. The scenario world owns both until the scenario ends | `tests/common/unaccepted_address.rs` |
| Backup cut holds | No harness task: the fault injector's lease or capture gate holds each backup cut that reaches it on that cut's own task in the node, until the scenario releases the gate or for the duration the scenario timed from the cut's arrival | `src/fault_injection/backup_cut.rs`, driven by the backup steps in `tests/backup/mod.rs` and `tests/backup/wait.rs` |
| Bounded row and broker observations | One task on the binary's runtime per observation, owning its subscription session or broker observer until it returns and aborted when the step drops it. Its deadline is captured before dispatch, so a runner stall delays learning the result without extending the observation. Exact payload multiplicities, generator occurrences, broker counts and sequences, and materialized rows retain their assertions and budgets | `tests/common/runner_observation.rs` and `subscription_payloads.rs`, used by the payload, generator occurrence and broker-count steps in `tests/scenarios.rs` and the materialized-row step in `tests/backup/materialized.rs` |
| Timed control probes | One task on the binary's runtime per probe, holding a clone of a named client the scenario connected beforehand until its statements return, and aborted when the step drops it. The deadline is captured before dispatch and enforced on that task, which records each statement's dispatch and completion with its domain, so a runner stall delays learning the result without extending the probe or what it measures | `tests/common/runner_observation.rs`, used by the timed client step in `tests/scenarios.rs` |
| Syslog mixed-framing TCP publication | One abort-on-drop task on the binary's runtime, owning connection establishment, the complete framed bytes and the sender through shutdown | The mixed-framing sender step in `tests/scenarios.rs`, using `tests/common/runner_observation.rs` |
| Syslog TLS publication and split-frame writes | One abort-on-drop task on the binary's runtime, owning TCP connection establishment, the TLS handshake and the complete prepared write. Publication closes the connection there; a split-frame write returns its sender for the later remainder | `tests/common/syslog_tls.rs`, using `tests/common/runner_observation.rs` |
| Command outcome recovery | On the task awaiting the command, including a backup or a backup's recovery a step sends through the raw session, or on its background execution. It repeats the original execution reference on one session at a time, opening each node in turn until the current leader supplies a known outcome | `tests/common/command_recovery.rs`, over the raw session in `tests/common/raw_session.rs` |
| Publication-relative receipt timing | A runtime task owns the subscription read before publication and retains its receipt timestamp. The assertion bounds completion after the HTTP publishing step returns; scenario teardown cancels an unfinished reader before stopping nodes | `tests/common/runner_observation.rs`, `tests/common/raw_session.rs` and the timed subscription steps in `tests/scenarios.rs` |
| Synchronous step work | One task on the runtime's blocking pool per job a step hands off the runner task, such as reading, verifying or rewriting a backup archive, preparing ONNX fixtures or inspecting a stopped node's database; the step awaits the job and fails with its panic | `tests/common/runner_offload.rs` |
| Runner-lag observer | A tick polled beside the whole run on the runner task, which logs every stall of that task of one second or more | `tests/common/runner_lag.rs`, wrapped around the run in `tests/scenarios.rs` |
| gRPC receivers | Tasks on the binary's runtime, one listener, one task per connection and one per call, owned by the scenario that started them | The gRPC receiver fixture, `tests/common/grpc_receiver.rs` |
| Broker observers | One task on the binary's runtime per observer, which reads the broker's client and hands each message to the step over a bounded channel and is aborted when the scenario drops the observer. A core NATS observer has no task of its own, because its client already buffers what the server delivers | The cluster fixture, `tests/common/cluster.rs`, the delivery owner, `tests/common/broker_observer.rs`, and the NATS observer, `tests/common/nats_observer.rs` |
| Dependency endpoints | One listener task and one task per forwarded connection on the binary's runtime, for each endpoint a scenario stands in front of a dependency the run shares, so it can stall, hold or end that dependency's connections for its own clients alone. Dropping the endpoint, at cleanup, ends its listener and every connection it forwards | The stallable TCP proxy in the cluster fixture, `tests/common/cluster.rs`, the controlled NATS endpoint, `tests/common/nats_endpoint.rs`, which reads what a client sends one NATS operation at a time so it can stop after a number of publishes, and the Iceberg catalog gate, `tests/common/iceberg_catalog_gate.rs`, which serves HTTP/1.1 and holds each commit to one table until the scenario refuses or passes it |
| Client probes | A child process per probe of another language, or one blocking task for the in-process probe of the shared Rust binding, owned by the scenario that started it | The client probe fixture, `tests/common/client_conformance.rs` |

The Syslog mixed-framing sender assembles its octet-counted and non-transparent frames before
connecting and writes them together on its runtime task. A stalled scenario runner cannot leave
that sender idle between its count, delimiter and payload writes. The idle-connection scenario
keeps its one-second `idle_timeout_ms`, observes both messages beside the silent connection, and
measures the silent connection's close from before connecting. The runtime task removes runner
polling gaps; it does not remove scheduling delays on the worker itself or change the listener's
deadline.

The Syslog TLS steps prepare their identity, trust and framed bytes before starting the sender.
The sender performs its connection, handshake and write together on a runtime worker, independently
of subsequent runner polls. Dropping the step aborts that task and releases its connection; a
sender panic still fails the awaiting step with its original cause. The handshake-stall scenario
retains the listener's one-second `connect_timeout_ms`, observes the silent client's actual close
at least one second after connection began, and verifies authenticated publication both beside
the stalled handshake and after it closes. The split-frame scenarios retain their connection and
exact remainder across steps. This removes runner polling gaps without changing the listener's
deadline or claiming that runtime workers cannot themselves be delayed.

The controlled NATS endpoint publishes a positive observation when it holds a complete publish
and stops reading behind it. A connection-loss scenario waits for that observation, ends the
active connections, and waits for a refused reconnect before forwarding again. The same fault
can run beside STOP. These event boundaries establish the outage without a sleep or a negative
observation window. The publication scenario requires every original sequence member, allowing
the duplicates an uncertain Core NATS retry permits. The STOP scenario observes the pending-record
uncertainty through a retained public session; terminal domain teardown may cancel unresolved
host work under the source's existing acknowledgement contract.

A scenario binary built for the `deloxide` mode, which `just test-deloxide`, `just test-deloxide-order`,
`just test-deloxide-stress` or `just test-deloxide-stress-restore` builds, starts its
deadlock diagnostics in `main`, right after it configures the lifecycle of its test dependencies and
before it builds its runtime, so its one detector is installed before any tracked lock or runtime
worker exists and every in-process node shares it; the helper process that holds a dependency
container open starts its own. It records its evidence in `NERVIX_DEADLOCK_EVIDENCE` when that is
set. A binary whose diagnostics cannot start exits with status `4` before any scenario runs, and the
first active deadlock any of its tracked locks reports ends the whole run with status `3`, its
description on standard error. A server child started with the fixture's deadlock evidence option
records its own evidence under its root, and is a diagnostic node only when the binary beside the
scenario binary was built for the mode. The ordinary suite leaves the `@deadlock_diagnostics`
scenarios out by default: their steps assert a detector an ordinary build does not have.

The stressed endpoint ACK lifecycle examples use `@deloxide_stress_endpoint_ack` in their own
bounded diagnostic invocation. They run serially as the other stressed lifecycle examples do,
with a 900-second process bound and the existing 360-second teardown reserve. The baseline
lifecycle invocation has a 2,700-second bound; all phases share the 4,800-second
selection workload budget. Inventory discovery checks every group completely, and the ordinary
suite continues to admit these examples beside its other scenarios.

The six transaction-retention examples use `@deloxide_stress_transaction_retention` in the
`stress-transaction-retention` invocation, before the baseline lifecycle process. It uses the
same 900-second process bound and 360-second teardown reserve. Inventory discovery checks that
the baseline, endpoint ACK and retention groups are complete and disjoint. The separate serial
phase accounts for every retention example within the selection's workload budget.

The [diagnostic lane](#diagnostic-lane) also selects the ordinary `@restore_installation`,
`@client_ingestor_alter_drain`, `@memory_pressure_pause`, `@client_io_03_consumer_restore`,
`@client_io_03_generation`, `@udf_column_builder`, `@vhost_tls_rebinding` and
`@inferencer_branch_batches` scenarios and the local `@deadlock_reports`
inspection/export/triage workflow. They exercise
the blocking applied-state guard through interrupted checkpoint staging, complete publication and
runtime handle clearing, including a delayed coordinator after leadership transfer and a
successor's START, the memory-pressure pause of starting and running ingestors, Rust client
consumers restored after a cluster restart and closed by a domain stop, a Roto function's column
builder, the HTTPS listener's TLS configuration as a VHOST's bundle is rebound under HTTPS
ingestion, and a batched inferencer's ONNX session across interleaved branches. The report tool is
an ordinary local executable, even when the scenario process
selects order analysis. Potential findings remain recorded without ending the process; any
unreviewed workload finding prevents diagnostic qualification. Real server children and the
scenario process record the same compile-time/runtime selection. The ordinary CLI built by the test
dependencies drives these diagnostic nodes through its public protocol; the lane supplies its path
explicitly because the diagnostic binary has a separate build directory. The feature input is a
quoted file glob, and the lane requires every tagged scenario run to execute and pass without
retries. The recorded evidence covers tracked blocking locks reached by those workloads; async
waits, capture atomics, dependency locks and cross-node waits retain their other concurrency
checks.

The `@restarted_voter_observation` scenario runs in both selections, reaching whole-cluster teardown, reopening each node's store, and automatic scheduling after the first relayed voter heartbeat. Its watch-channel barrier and concurrent fault map are untracked; the diagnostic evidence covers blocking locks reached by restart and scheduling. The native metadata restore scenario also runs in both selections after its Kafka replication stream and acknowledgement boundaries are repaired, retaining complete primary and replica metadata comparisons and distributed placement checks.

The WASM/Kafka restore outline checks Commands-memory headroom on the restored leader after its
source becomes ready. The cluster fixture reads the public capacity and reserved-byte metrics,
then polls within the step's single deadline until the class can admit the 176-byte source control
call. It reports both last observations on timeout and allows the retirement reservation of a live
Kafka consumer. This assertion preserves the source's existing readiness and error deadlines.

The measured restore and backup steps sample each node's public bulk and restore-metadata
executor gauges every 20 milliseconds while the ordinary CLI runs. They also record the harness
process's jemalloc allocated and resident bytes. The executor measurements separate the fixed
checkpoint I/O and publication grant from retained archive descriptions, typed native values and
planning allocations. Process measurements include every in-process node, dependency fixture and
harness allocation; isolated one-node and three-node runs provide attributable comparisons.
Sampled peaks are observations, not continuous maxima, and allocator resident bytes exclude
mappings outside jemalloc. The scenarios also compare bulk refusal counters before and after each
restore, and a measured backup's check compares them and every node's sampled bulk peak with that
node's published bulk budget.
The large-native-metadata oracle stops the cluster after public restore and exact replay, then
opens each node's database through the production snapshot reader. It compares every lifecycle
key, timestamp, incarnation, revision and Kafka topic/partition/offset on primary and replica
copies. Those complete-value inspection allocations occur outside the restore measurement.
The runtime's testing-only inspection API returns decoded current metadata; the harness owns all
archive comparisons and assertions.
The scenario takes the CLI's execution reference and replays it through the public session before
and after restart, comparing the complete typed restore report.
It saves the restored work's public owners and replicas before restarting, requires distributed
placement in the three-node case, and compares every saved placement after START and after
receiving isolated output for every tenant. A failover that drops replicas or recreates guest state
cannot make this oracle pass by moving all execution onto one node.

`just test-native-metadata-restore-repeat` runs that scenario's three-node example ten consecutive
times at concurrency four with zero retries, preserving its normal harness deadlines. It stops
at the first failure or a summary that does not contain exactly one passing scenario. Every attempt
keeps its source revision and patch, each process output and the available node log under
`target/native-metadata-restore`; its completion record is written only after all ten pass.

The complete-generation fixture allows 120 seconds for every post-restore input to reach its raw
relay. Its forty one-mebibyte guest saves exercise sequential source acknowledgements and durable
replica checkpoints beside the other diagnostic workloads. The full active diagnostic selection
reached 77 of 80 inputs at the former 30-second bound, while the exact same binary reached all
inputs and preserved every branch in the focused four-case replay. This condition ends as soon as
the exact expected count is observed; it retains the complete output, state and branch assertions.

The deduplicator keyspace and window fixture above the bulk budget posts its payments through an
acknowledged HTTP endpoint, `MODE ACK MAX 8 ACK TIMEOUT 60s` with `ON QUIESCE REJECT RETRY AFTER
1s`, and repeats a payment the endpoint answers with 503 until it answers 200 with a completed
outcome, for at most 300 seconds per payment. Under `MODE NO ACK` the endpoint answers 202 before
the payment reaches the deduplicator. Under load, the routed delivery of a one-mebibyte payment to
the deduplicator's node failed when its relay body transfer did not complete or its send timed out,
and the payment was lost without a trace, so the exact summary oracle waited for a key that never
arrived. The deduplicator forgets a key whose summary it could not forward and
drops one it already forwarded, so a repeated payment yields its summary exactly once. The window's
rows keep `MODE NO ACK`: a window acknowledges a row only when it steps past it, so an acknowledged
post of one row would wait for the rows after it.

The web console's busy relay tab outline and the CLI's high-volume subscription outline post their
270 metrics through an acknowledged HTTP endpoint, `MODE ACK MAX 8 ACK TIMEOUT 60s` with `ON
QUIESCE REJECT RETRY AFTER 1s`. They keep four posts in flight, half the window, through one
client. Each post repeats a 503 until the endpoint answers 200 with a completed outcome, for at
most 300 seconds per metric, and the step ends when every post is answered. The endpoint answers
only after the relay owner has handed the row to the subscriber's node, so the 30-second wait for
the final row then covers only the subscriber's node, the session and the client. Under `MODE NO
ACK` the endpoint answered 202 as soon as the row was queued on the intake node. That node's branch
runtime keeps queuing a branch's rows while one of them is in flight to the relay's owner, so
posting does not wait for delivery at all. In a loaded three-node run both relay hops of each row
were slow, and posting finished while most rows were still queued. The console held rows 1 to 135
when its wait ended, and the outline failed on rows the relay was still delivering. `relay owner
fan-out for domain ... waits ... before each batch` makes every owner fan-out of a domain wait
first. The slow busy relay outline uses it to post 20 metrics one at a time while each fan-out
waits three seconds, so the relay needs a minute to deliver them. An unacknowledged post of the
same rows finishes within seconds and leaves most of them queued past the console's wait.

The fixture of guest saves above the bulk budget feeds its WASM processor from a Kafka ingestor in
`MODE ACK SEQUENTIAL`, and every input ends with a 40 MiB guest checkpoint that the processor's
replica confirms before the ingestor reads the next input. Every wait that follows published input
covers at least one such checkpoint, and a wait for a sent-message total up to two in sequence. The
product gives each checkpoint ninety seconds to reach its replica, so these waits are bounded at 300
seconds. On a loaded runner, two checkpoints that each stayed within their budget outlasted the
former 120-second bound of a sent-message total. A sent total counts the inputs the ingestor sent,
not the ones the guest has saved, so before its first cut the fixture waits until both branches have
committed the save of their third input, every replica has confirmed it and no checkpoint has
failed. No cut then drains an input still on its way, whose checkpoint could fail under load and
redeliver the input between the cuts the fixture compares. It then holds the next guest checkpoint
before its save across the source domain's three cuts. A cut that asked a guest to save again would
stall on that checkpoint, so the fixture shows that every cut archives the saves the branches
committed. A checkpoint that misses its ninety seconds still ends the attempt, because the
redelivered input would no longer match the exact sent totals and archives the fixture compares.

The large materialized restore fixture prepares its rows through an acknowledged client producer.
One round has a 120-second budget and one in-flight batch, including explicit replays of reported
processing failures. Each replay replaces the same materialized keys with the same complete values,
so partial effects retain the fixture's value oracle. Every attempt and terminal result is logged;
an unknown outcome, final admission refusal or expired round fails preparation. The producer closes
after every tenant's batch has completed, before the scenario checks all generator rows and takes
the backup cut. Source admission, input delivery and the final archive and restore assertions remain
separate observable steps.

The lane then selects `@paced_simulation_reopen` from the public paced-driver feature. It builds the
Rust driver in the lane's selection and supplies its path to the same scenario fixture, so the
driver installs its own detector before entering its runtime. Both drivers exercise endpoint
replacement, pending submission ownership, explicit replay and replacement refusal against
diagnostic nodes on one and three nodes, without retries. Python uses the ordinary shared binding;
its locks and condition variables are outside this detector.

The buffered-route replacement scenario accounts for every reading in the first run's outcomes
and explicitly replays unresolved ledger entries. Ending a producer can refuse a pending or
already planned submission even when its replacement opens in the same START generation. The
scenario requires every planned reading to have exactly one effect after replay; each invocation's
summary counts only that invocation, including an empty replay when no reading was refused.

The lane's diagnostic owner tests each start their detector in a fresh process before any store,
publication or executor lock: the state store's staging, snapshot views, queued writers,
interrupted publication and bounded cleanup, and the materialized publication lifetimes. Each must
pass its libtest accounting and record exactly one qualifying process observation in both
diagnostic selections. The tracked locks' conformance checks of `nervix-primitives` run the same
way, each in a fresh process in both selections. They record no evidence file, because the
primitive crate sits below the crate that owns evidence: a finding reaches an emitter that aborts the
check, and the lane fails on that ending.

Test dependencies start through one suite-owned environment. If Docker creates a named container
but cannot bind its randomly selected host port, that owner removes the failed container and tries
up to three more times with a new port selection. It also removes the failed container when those
attempts are exhausted, so a scenario retry does not collide with its name. Other startup failures
reach the scenario directly.

The Postgres image starts a temporary Unix-socket server to initialize its database, then starts
the final TCP/TLS server. The fixture waits for both readiness messages before publishing either
endpoint. Initialization stores the password's SCRAM-SHA-256 verifier with one key-derivation
round instead of the image's 4096. A SQLx client derives its key in as many rounds as the verifier
names and yields to the scheduler after every fifth, so 4096 rounds hand an establishing connection
back to the scheduler more than 800 times. On the suite's busy runtime each of those returns waits
for a worker's whole run queue. Establishing one connection that way took longer than the 30-second
pool wait, while queries on connections already established were unaffected. With one round, every
connection the suite establishes, the product's included, still authenticates with SCRAM-SHA-256
and completes in a few polls.

The fixture's plaintext and certificate-verified TLS observation clients belong to that same suite
environment. Each transport shares one SQLx pool of two connections for short setup and observation
queries across all scenarios. The pool establishes both connections when the environment opens it
and keeps them until it closes, with no idle or lifetime retirement, so an observer waits only for
another observer's query and never for a connection to be established. A deliberate table-lock
controller connects on its own with the same transport's options, instead of taking one of the
pool's connections. Its scenario release handle owns that connection's lifetime, so a held lock
takes neither an observation permit nor an established observation connection. The environment
closes both observation pools before stopping their container, including when scenarios still
retain client handles. The existing suite teardown budget bounds that cleanup.

The Postgres fixture scenarios exercise 48 concurrent observers retaining three client handles
each over plaintext and TLS, and compare every returned text, byte sequence and observer number.
Every query also checks its actual transport through Postgres's connection statistics. Two further
examples hold locks through the existing fixture controller while another observer queries, once
over each transport, and require the observer's connection to have been established before both
lock holders connected. Two more establish a connection with each transport's observation options
and require it to complete within 32 polls of its future.
The database batch scenarios retain their one- and three-node row, field, write-size, conflict,
rejection and ambiguous-write assertions. Fixture concurrency adds no scenario admission limit.

MQTT fixture publishers keep their event loop alive through the driver's tracked
publication completion: a socket write for QoS 0 and the broker acknowledgement for QoS 1. The
existing broker-operation timeout bounds that wait, and cancellation drops the driver. Retained
scenario inputs still use unique topics. Bursts queue all their messages before waiting for
completion. In a burst, the timeout bounds each publication's queueing and completion rather than
the whole burst, so a burst fails only once the broker answers none of its publications for that
long. Under the parallel suite, a burst of 2,000 QoS 1 publications took longer than one timeout
in total while the broker answered each publication well within it. The fixture regression pauses
a real broker connection for 500 ms and compares every complete delivered payload for QoS 0 and
QoS 1, as individual messages and bursts of five.

The reclaimed-command-identity scenario configures a 20-second retry validity for its initial
admission and exact retry. Its reclamation wait includes the configured validity followed by
60 seconds for reclamation. The assertions still require the identity to remain expired after a
durable restart, with the schema and committed transaction report preserved.

The OpenTelemetry Collector dependency exposes its stdout and stderr to scenario assertions through
one follower that the suite-owned environment starts with the container. The follower streams both
outputs from the container's start into an in-memory transcript of at most 1 GiB per output, and
every scenario observes that transcript without the environment's lock or another read of the
container's logs. A Docker logs read answers with the output as it stood when the read began, and
the Collector's output grows for the whole run, so no assertion re-reads it. The environment stops
the follower before it tears the container down. A batching scenario reads the Collector's debug
exporter output to check the number and order of records in each received export request, using
unique test markers to distinguish simultaneous scenarios sharing that container.

The debug exporter prints each export as two log entries: a summary that counts the export's
records, and then the export's body. The Collector prints each entry whole, but an entry of a
concurrent export can print between a summary and its body, so a summary does not identify the body
it counts. The batching scenario therefore reads each export from its body alone. The body's first
line names its signal, and each log record, span or data point in it starts a line of its own,
which is what the scenario counts. The logger prints the entry's fields after the body's last line,
so a body is read only once its fields have arrived. The suite's Collector configuration turns off
the sampling of the Collector's own logger. By default that logger prints only the first ten entries
that share a message in each ten-second interval, and every hundredth after that, so it drops the
summaries of exports that arrive close together.

The raw session fixture records domain-clock replies and frames in the order it reads them. A wait
for a detach reply can queue ticks that arrived before the reply; the post-detach assertion checks
the recorded wire order, clears those already queued frames, and waits for any new frame. An unread
pre-reply tick therefore does not masquerade as delivery after detach.

The generator cadence assertion checks execution timestamps rather than subscription throughput.
Every row must preserve the expected source fields and its concrete branch key, timestamps must
increase within each branch, and a complete occurrence has one shared timestamp across both
branches. Rows from different branches may arrive in either order. Early rows from the first branch
remain checked while the second branch becomes ready; they do not count as complete occurrences.

The observation runs on a runtime worker independently of the cooperative scenario runner and has
a 30-second physical liveness budget for four complete occurrences. Delayed subscription reads can
consume already queued rows, so their arrival spacing cannot measure generation rate. The occurrence
timestamps must span at least one logical cadence interval. A pending execution can finish late and
be followed by a coalesced occurrence;
the next boundary may also be close. Requiring equal gaps would reject that supported behavior,
while four complete occurrences within less than one interval expose a catch-up burst.

`Paced generators follow domain logical time` covers `EACH 2s` at rate `20.0` and `EACH 200ms` at
rate `0.1`, with the same one- and three-node examples and replica counts. The slow-rate profile
discriminates against scheduling in physical time: four 200ms physical occurrences would span only
60ms of logical time, below the required 200ms. Longer gaps from missed/coalesced periods remain
valid. The fixture's two-day logical branch TTL exceeds the current 105-minute suite budget at
rate `20.0` (35 logical hours); branch expiry is not the subject of this scenario.

The focused observation regressions use a paused test clock to delay nineteen subscription reads
by a second each, including eleven rows from the first branch before both branches appear. They
retain the same field, key, ordering and shared-timestamp checks, and separately reject a catch-up
burst, physical scheduling at the slow rate, corrupt rows and a stalled or failed subscription.

`tests-deps` builds the CLI, the Plexus formatter, the paced simulation's Rust driver and the shared
C binding its Python driver loads in the normal target directory. The full and focused
client coverage recipes build a standalone instrumented CLI beside their instrumented server binary
and place the normal Plexus formatter there. The scenario runner selects the covered CLI through
`NERVIX_TEST_CLI_PATH`, so its one-shot completion and command paths contribute to the same LCOV
report as the CLI's binary unit tests and the server's public scenarios. The general
`coverage-scenarios` and `coverage-scenarios-append` recipes provision the same instrumented CLI
and formatter, so a selection containing CLI scenarios needs no separate binary setup. The focused
CLI process coverage recipe exercises transaction inspection and the clock-following process
scenarios.

`test-scenarios-coverage`, `coverage-scenarios` and `coverage-scenarios-append` also build the paced
simulation's Rust driver and the shared binding with coverage flags, and the scenario runner selects
them through `NERVIX_PACED_SIMULATION_PATH` and `NERVIX_CLIENT_LIBRARY`. A driver process inherits
the profile location of the scenario binary that starts it, so the Rust driver and the binding
loaded by the Python driver write their profiles into the same report when they exit.

The CI `scenarios` job uses the shared native
[CI linker](./developing-nervix.md#validation-and-tests) for the server, CLI and scenario harness,
including their instrumented builds. Coverage flags do not replace Wild linker selection.

The suite has one pool of **run slots**. Its size is the number of CPUs times the concurrency
factor, set by `NERVIX_TEST_CONCURRENCY_FACTOR` or `--concurrency-factor` and `1` by default;
`--concurrency` sets an absolute slot count. The CI `scenarios` job uses factor `3`, so its
16-CPU runner has 48 slots. Cucumber may take up all parsed scenarios without charging a slot.

The slots are the suite's capacity, and the measurement of 26 October 2026 set their number. Over
the retained artifacts of 20 Check runs, an ordinary run of one inventory of about 3,620 attempts
used between 128,000 and 215,000 slot-seconds on 48 slots. The inventory did not decide that
spread; the runner did. Every scenario of a run slows by the same factor: the median, over
scenarios that passed their first attempt, of an attempt's time against that scenario's median
across runs ranged from 0.74 to 1.25 from run to run, while inside one run half of the scenarios
stayed within about a tenth of the run's own factor. The three slowest runs in that sample were
the only Intel Xeon runners among the 17 whose runner diagnostics name the CPU, and they are the
two passing runs that ended within 25 seconds of the 75-minute budget those runs had and the one
that exhausted it.

The suite waits, but not for the runner's CPUs. Once the long-running attempts that start
together have ended, the runner's CPUs averaged 51 to 55 percent busy while every one of the 48
slots held a scenario, and an ordinary scenario started during the 82 to 86 percent peak of the
first fifteen minutes ran no slower, against its own median, than one started later. What every
scenario waits for is the one thread that polls all of their steps, described under
[Runner Stalls](#runner-stalls). Factor `4` was measured in Check run 40358096404: with 64 slots
that thread ran on a CPU for 86 percent of the run, every scenario took about 1.45 times its
median across the factor `3` runs, and the run needed 279,439 slot-seconds where 48 slots had
needed at most 215,000. More slots added no throughput, so the job keeps factor `3`. Memory is
no limit: the runner used at most 28 GiB of its 62.

Admission grants feature capacity and the attempt's share of the run slots in one decision, after
both are available. A scenario holds both through the end of teardown. A first run holds one slot.
A retry holds a larger share, so it runs beside fewer scenarios than the run that failed. The first
retry holds a sixteenth of the pool, and each later retry holds twice the share of the run before
it. With Cucumber's two retries, the second retry holds an eighth, so the shares are 1, 3 and 6 of
CI's 48 slots. Every run holds at least one slot, so a pool of fewer than sixteen slots gives its
first retry one. A run past the configured retries, as a `--retry` override allows, keeps the last
retry's share. No run holds more than a quarter of the pool, so no scenario runs alone. A retry
whose share is not yet free keeps the slots that free up until its share fits. Otherwise the first
attempts behind it would take those slots one at a time and starve it.

The active-scenario diagnostic names the feature limit or run slot a waiting scenario needs. Each
`scenario started` line names the run slots the attempt holds. The suite summary counts scenario
work as each attempt's running time multiplied by the slots it holds, and it reports the
slot-seconds retries held beyond one slot each. That figure is what the larger shares cost the run.
Replayed against the 15 retries of Check run 40044052881, the shares cost 6,876 slot-seconds,
about 143 seconds of a full 48-slot run. Against the 5 retries of run 39987498652, they cost about
21 seconds.

The coordinated WASM state-reset feature runs one scenario at a time. The web console REPL,
execution graph, transaction inspector, and domain clock features share a limit of two. Both limits
affect only their named features. The parser takes these limited features up before the bulk of the
suite, and
available run slots favor a queued limited successor over queued ordinary work. The web console
group grants its two slots to the feature with the fewest prior grants, so one feature cannot keep
the other three waiting behind its whole queue. Their chains can therefore make progress throughout
the suite rather than after it. Cucumber retries a failed scenario twice; `--retry 0` turns retries
off for a focused run. The retry count is one constant, which also fixes the retries' slot shares.
The before hook reads which run an attempt is from its active-scenario registration. Each feature
queue keeps retries ahead of first attempts, and admission selects an eligible retry before any
first attempt, including a limited feature's first attempt. A retry still needs its feature
capacity and its share of the run slots together: one waiting for a full feature group leaves free
slots available to other work. Ordinary and WASM-reset queues retain FIFO order within an attempt
kind; the web-console group retains its feature fairness. The finite retry count bounds this
priority; first attempts cannot be kept waiting by an unlimited retry stream. A cancelled waiter
returns its feature capacity and its whole share before dispatch continues.

A scenario whose attempts run for minutes declares itself with the `@long_running` tag, on the
scenario, its outline's examples, its rule, or its feature. A free run slot takes a declared first
attempt before any ordinary first attempt, after retries and the limited chains, so the scenario
runs beside the bulk of the suite rather than after it. Without the declaration, feature order
decides when a long scenario starts. It admitted the multi-mebibyte transaction-report scenario of
a late feature in the run's last quarter: in one canonical producer of 17 October 2026, at
1,650.8 seconds, and it then ran alone for the last 22.7 seconds; in another, at 1,465.0 seconds,
where its attempt failed after 329.1 seconds, so the attempt and its retry ran alone from
1,739.2 seconds to 1,937.7 seconds. That run needed 1,725.8 seconds at full occupancy and took
1,939.5, at 89.0% utilization. When a run's work fills its budget, as in the overrun at
116,962.9 of the 118,080 slot-seconds that 48 slots supply in 41 minutes, no order can end it in
time; a declared scenario keeps the order from adding a tail to a run that would otherwise fit.

A scenario declares itself long-running when its attempts reach two minutes in a canonical
producer. In the first run above, 34 attempts outside the limited features reached two minutes,
about one in a hundred, while the median attempt took 25 seconds. The suite summary lists every
attempt that reached two minutes without the declaration. The declaration orders admission only.
A declared scenario's attempts take the same share of the run slots as any other scenario's, and
run beside the rest of the suite. Its retries, deadlines and budgets are unchanged.

The CI jobs divide the work at the scenario boundary:

| Job | Work |
| --- | --- |
| `tests` | Instrumented workspace build and all tests except the scenario target |
| `scenarios` | Instrumented server, CLI and paced simulation drivers, the unsharded scenario suite at factor 3, and scenario logs |
| `coverage` | After tests, scenarios and extra tests, publishes the [ordinary workspace report](./coverage-reports.md#the-ordinary-workspace-report). Merge and analysis integrity are mandatory; patch percentages and CRAP scores are advisory |
| `coverage-full` | After every producer job, including a skipped or failed one: `just coverage-full` combines the ordinary report, the modeled and diagnostic native collections and the requested live fuzz campaigns into the [full workspace union](./coverage-reports.md#the-full-workspace-union), validated against the run's event and label snapshot and every producer job's result. It downloads artifacts only, fails on a missing, stale or incomplete input, and publishes its contributor breakdown as the job summary and the `coverage-full` artifact |
| `extra-tests` | Native coverage collectors plus capability doctests, Miri, mutation, compiler, and primitive conformance checks |
| `shuttle` | Only for a pull request labeled `shuttle`: modeled in-process concurrency checks, uncontrolled-nondeterminism rechecks, and failure schedules |
| `loom` | Only for a pull request labeled `loom`: exhaustive memory-ordering models and failure checkpoints |
| `loom-qualification` | Only for a pull request labeled `loom`: two shards of the Loom weakening qualifications, one weakened server build for each, and the runs of a failed qualification |
| `deloxide` | Only for a pull request labeled `deloxide`: the [diagnostic lane](#diagnostic-lane) of each Deloxide selection side by side, the two stress selections included: its diagnostic builds, probes, owner tests and tagged one- and three-node scenarios under the native coverage collector, the supervision qualification, and every attempt's record and evidence |

The justfile reads `rust-toolchain.toml` through Python's standard-library `tomllib`, using the
Python 3.12 or later runtime already required by repository recipes. CI installs `just` and each
job's execution tools before running the recipes.
Rust setup passes the declared `1.99` channel explicitly to the toolchain action, installing
the name that repository recipes and coverage commands request.

The Check workflow and reusable Bolero workflow fetch Git dependencies through Cargo's Git CLI
transport, selected by `CARGO_NET_GIT_FETCH_WITH_CLI=true`. `Cargo.lock` still pins their revisions.
A dependency download that fails before a test starts provides no test execution or coverage
qualification evidence.

The `checks`, `tests`, and `scenarios` jobs use Go 1.26 and check out the source before installing
their pinned Go formatters. This makes the installer and the later cache publication and report
scripts available when a tool setup step fails. The installer retains every attempt in a
`go-tool-bootstrap-<job>` artifact and retries transport errors reading Go's public module proxy or
checksum database within its eight-minute budget. Checksum verification remains mandatory; compiler,
version and verification failures end installation immediately. The publication and report steps
check for their scripts before calling them,
so a failed checkout does not add a second failure that obscures the original one. The jobs'
existing deadlines and test budgets are unchanged.

The `tests` and `scenarios` jobs also sample runner CPU utilization and steal time every five
seconds, with UTC timestamps, guest-reported MHz and Linux PSI CPU, memory and I/O `some avg10`
pressure. Missing VM counters are blank, not zero measurements.

Every authored Rust build step in Check, Bolero and Docker Build is preceded by the shared
`runner-diagnostics` action. It prints CPU identity/topology, allowed CPUs and visible ancestor
cgroup quotas, exposed frequency/governor/boost controls, memory, filesystem and block-device
information in the step log and job summary. The `runner-diagnostics-<job>-<matrix-index>-<step>`
artifact retains JSON and Markdown with UTC, revision, run/attempt/job identity, tool versions
and exact arguments. Compare matching tool versions, architectures and arguments;
these are observations, not performance gates or sustained hardware benchmarks.

The action installs distro-packaged [sysbench](https://github.com/akopytov/sysbench) and
[fio](https://fio.readthedocs.io/en/latest/fio_doc.html) once per job. The tools run on one allowed
CPU where `taskset` is available. Sysbench runs one second each of CPU work (prime limit 20,000),
sequential memory reads and uniformly random memory reads across a 64 MiB block. Its event latency
measures a whole block operation, not an individual hardware memory load. Fio uses one worker and
queue depth one: 64 MiB of direct writes with `end_fsync`, 64 MiB of direct sequential reads and
128 random direct 4 KiB reads. It touches only a private temporary file on the build filesystem.
The write report's sync statistics and the process duration are retained; its per-I/O bandwidth
must not be mistaken for a measurement including the final sync.

Raw tool reports, wall time and process CPU time are retained separately. Sysbench invocations
have three-second process-group guards and fio a four-second guard, all within a ten-second
calibration allowance. The timeout-retention regression synchronizes on a child-ready signal
after its measurement is flushed, then applies the same short timeout to the stalled child;
it does not assume process startup finishes within that timeout. Completed tool reports survive
a later timeout; the supervisor kills the
tool's process group and removes its temporary directory. Missing tools and unsupported direct
I/O retain their actual error, with no buffered-read substitute and no global cache drops.

Native actions probe the workspace filesystem. Docker's builder also runs the probe against its
mounted `/app/target`, recording the full result in its plain BuildKit log; host workspace and
builder storage observations are distinct. No diagnostics are added to the release image.
Direct I/O requests still traverse the guest filesystem and storage stack and do not establish
physical SSD latency. Reported MHz and cpufreq policy values do not measure effective physical
clocks. Low steal alone does not show a host is fast: CPU execution, cache/memory contention and
storage can slow work without stolen time. The samples support comparison, not a specific cause.

Every kache-backed job uses kache 1.2.1, records `doctor` output without making it a
test failure, publishes a cache report, and diagnoses its five most expensive misses with
`why-miss`. The shared S3 cache keeps executable and test-binary outputs, with stores sized for
the full builds: every kache-backed job and Docker image build uses a 1 TiB store ceiling. This is
an upper bound, not a disk reservation. The runner's actual disk capacity remains the practical
limit.

The shared remote is Cloudflare R2 through its S3 API. Kache reads the base `artifacts` prefix
first. Pull-request jobs also read and publish entries, manifests, and shards under the shared
`artifacts-pr` prefix, configured in the daemon's TOML file. This lets successive PR runs reuse
their builds. Kache lets only a protected-branch push publish to `artifacts`, and no workflow runs
on one; scheduled and manually dispatched native jobs retain kache's read-only policy. Docker image
builds run kache inside BuildKit, which does not see the GitHub Actions environment, so their
builder publishes to `artifacts`. Fork PRs still need credentials to access R2. Kache's `doctor`
displays the generic read-only CI policy even when the PR prefix is enabled; the job's daemon
reports its actual write mode, which the start step prints and publication records.

CI configures the cache bucket through the required repository variables `KACHE_BUCKET` (bucket
name), `KACHE_BUCKET_REGION` (S3 region), and `KACHE_BUCKET_ENDPOINT` (S3 service URL), and reads its
credentials from the repository secrets `KACHE_BUCKET_ACCESS_KEY_ID` and
`KACHE_BUCKET_SECRET_ACCESS_KEY`. Each kache-backed job passes them, as kache's `KACHE_S3_*`
environment variables, to `python3 scripts/kache_remote.py start` right after the setup action.
Docker image builds pass the same settings to their builder, whose build command runs
`scripts/kache_remote.py configure` before it starts the daemon. Either way the service URL is
written into the daemon's TOML configuration before startup.

For Cloudflare R2, set `KACHE_BUCKET_REGION` to `auto` and `KACHE_BUCKET_ENDPOINT` to
`https://<account-id>.r2.cloudflarestorage.com`, without the bucket name in the URL. The bucket name
belongs in `KACHE_BUCKET`. `scripts/kache_remote.py` rejects URLs with a bucket path; supplying
the full bucket URL can make the remote cache appear empty.

### Cache Publication

`scripts/kache_remote.py` owns the remote of every kache-backed job: the `check.yaml`, Bolero,
`benchmark` and `build-book` jobs, and the builder stage of every Docker image build.

`kunobi-ninja/kache-action` installs kache, sets `RUSTC_WRAPPER`, the store and a job-scoped
runtime directory, and stops the job's daemon in its post step. The action and Docker builder
keep the pinned kache version's automatic local-store budget: 5% of the volume that holds the
store, rounded to the nearest GiB and bounded between 5 and 100 GiB. A 250 GiB volume therefore
budgets 13 GiB; a 500 GiB volume budgets 25 GiB. This leaves room for compiled outputs and
coverage profiles, and the budget follows the actual cache volume when the runner changes. If
the volume size cannot be probed, kache uses its bounded 50 GiB fallback. The daemon applies its
normal eviction while the compiler wrapper and shared remote stay enabled. The action receives
no S3 inputs, so its
post step neither saves a manifest nor pushes. The `Start kache remote` step that follows it:

1. writes the daemon's configuration, `$RUNNER_TEMP/kache-remote.toml`, with the bucket, region,
   service URL, base prefix and pull-request prefix. The credentials stay in the environment.
2. exports `KACHE_CONFIG`, the credentials, and the job's `KACHE_MANIFEST_KEY` and
   `KACHE_NAMESPACE` to the job's later steps. A matrix job's key includes its matrix value, so
   every build variant keeps its own manifest and shards.
3. starts the daemon, and fails the job unless `kache --json daemon status` reports it running with
   that file and `kache --json stats` reports that the file names `s3://<bucket>/artifacts`. A
   daemon that has not answered yet gets three polls, two seconds apart. The step prints whether
   the daemon publishes uploads or, under kache's read-only policy, skips them.

The extra-tests job also reclaims completed build phases. Before benchmark smoke it runs
`just clean-ci-docs` to release the completed doctests' Cargo target directory. After the native
benchmark collector has exported coverage and recorded completion, `just clean-ci-benchmarks`
releases that instrumented target's dev-profile build outputs. Both steps run only after their
preceding checks succeed. Exported reports, completion records, raw profiles,
compiler evidence and the source tree remain available; a failed phase keeps its build outputs.
The native collector marks its owned build directory as a Cargo cache while holding its build
lock, so Cargo recognizes it for guarded cleanup. Later phases rebuild any required artifact
through the configured cache.

Native CI jobs and the BuildKit builder also set `KACHE_AUTO_RECOVER_MIN_FREE_BYTES=68719476736`
(64 GiB). Kache 1.2.1 otherwise disables free-space recovery in CI. This floor enables reclamation
of eligible cached artifacts when free space falls below it, while the cache budget above scales
with the actual cache filesystem. Artifacts still shared with active build targets are retained;
target-directory recovery stays disabled. The floor is a trigger checked at kache's existing
cadence, and active build outputs and concurrent writes still consume storage. The compiler
wrapper, remote publication and coverage executable identity checks keep their contracts.

During the build the daemon publishes each entry the build stores: kache records a durable upload
intent, uploads the entry in the background, and retries an upload that failed for a retryable
reason every five seconds.

`Publish kache cache` runs `scripts/kache_remote.py publish` whatever the job's result: just before
the kache report where the job publishes one, and as the job's last step otherwise. For a job whose
work succeeded, it saves the build manifest with `kache save-manifest`, then reads the daemon's
upload counters from `kache telemetry write` every two seconds until no upload is queued or in
flight. It waits at most 120 seconds, its kache commands included. The step's 4-minute
`timeout-minutes` only backstops that bound. Every kache answer it reads is a versioned kache
document, and an answer outside its schema is reported rather than read. The outcome is one of:

| Outcome | When | What remains |
| --- | --- | --- |
| `published` | The manifest is saved and the daemon reports every upload of the job finished | Nothing |
| `incomplete` | The budget ended with uploads queued or in flight, the manifest failed or did not finish, an upload failed for good, or the daemon stopped answering during the wait | Queued uploads go to the daemon's shutdown drain, which kache bounds at 30 seconds |
| `read-only` | Kache made the job's remote read-only, as for a scheduled or manually dispatched run | Nothing: such a job publishes nothing |
| `not-run` | The job's work failed or was cancelled, so publication returns at once | Queued uploads go to the shutdown drain |
| `unavailable` | The job has no kache command or no manifest key, or its daemon does not answer | Whatever a stopped daemon left queued: publication asks `kache --json daemon status` first because `kache stats` would start a new daemon |
| `cancelled` | The step received `SIGINT` or `SIGTERM` | The record names the first signal and the phase in which the step received it |

Publication never changes the job's verdict. Every outcome exits `0` except `cancelled`, whose step
ends with the first signal's status, as the runner asked. `incomplete`, `unavailable` and
`cancelled` also add a workflow warning.

A termination signal never interrupts cleanup. Python runs a signal handler between any two
bytecodes, so a handler that raised at once could land inside library cleanup. On Python 3.12.3, a
signal just after `shutil.rmtree` closed a telemetry directory's descriptor made it close that
descriptor again, and the failure replaced the cancellation before the record was written; later
Python releases leave the directory behind instead. Publication therefore records the first signal
when it arrives and raises it only where publication can stop cleanly. Inside a wait for a kache
command or for the next poll it raises at once and only once, so the interrupted command is killed
and reaped, and the telemetry directory it was writing is removed. A signal received anywhere else
is raised when the next wait begins or the current phase ends, so the record names the phase in
which the step received it. Once the record is written the step stops listening: a signal received
until then ends the step with the first signal's status, and later ones are ignored, because
Python's exit restores the default handlers and would let a later signal end the step with its own
status. A signal that arrives before publication installs its handlers ends the step without a
record. A job that failed before its checkout has no script to run, so its step
adds only a notice. The record, `publication.json` and `publication.md`, lands in
`$RUNNER_TEMP/kache-report`, which the kache report artifact uploads where the job has one, and in
the job summary. An image build runs `publish --job-status success` as the last command of its
build stage, which reaches it only when every artifact has been built.

No job runs `kache sync --push`. Kache's push-only sync lists every key under both prefixes before
it decides what to push. On 2026-10-14 and 2026-10-15 that listing grew from 650,859 to 690,780
keys, took six to eight and a half minutes in every kache-backed job, and found nothing the daemon
had not already published.

## Product Deadlines And Harness Deadlines

Two kinds of deadline meet in every scenario.

A **product deadline** is configuration a node enforces on itself: its shutdown and drain timeouts,
its command retry validity and transaction idle timeout, its node-unavailability timeout and the
election timeouts its Raft heartbeat derives. Test clusters start with test values for some of
them. A scenario about one of them configures it, drives the node to it, and asserts the outcome
the owning chapter defines. No harness deadline shortens a product deadline while the scenario can
still observe it.

A **harness deadline** bounds how long the harness waits: for a status reply, for a node to become
ready, for a cluster to stop, for the whole run. Its expiry changes nothing inside a node. It ends a
wait with a typed failure, takes apart a receiver or a child process the harness owns, or ends the
run. It never ends a node. A node's task is only the future the node was started as: the node
spawns the tasks that hold its databases, its listeners and its peers, and they would outlive an
abort of that future with nothing left to join them. A node that has not stopped when a deadline
passes is therefore left to end by its own shutdown, keeps what it holds until it has, and is
recorded as still stopping, not as something the harness ended.

The cluster health scenario can fail application-health responses for one ordered probing-node and
responding-node pair while other probes remain healthy. It can also hold one node immediately before
that node reports a runtime revision prepared, wait until the hold is reached, and release it after
asserting command completion. These are per-scenario injected conditions; their waits have harness
bounds and do not shorten the product's completion deadline.

An injected hold is the opposite case: a product deadline races it. The scenario holds a controlled
operation at a gate for a workload duration, and the node keeps its own deadline running while the
operation waits. The scenario's margin is the part of the deadline the hold leaves.

The independent-budget backup outline holds one domain's quiesced cuts for 20 seconds and the next
one's for 22 seconds, each inside its own 40-second cut budget. Together the holds outlast one
budget, so a budget the two cuts shared would expire the second, and each hold leaves its cut at
least 18 seconds; the step refuses to start a probe whose holds break either relation. A cut spends
that margin on its lease, its pause, its drain and its capture. On a loaded runner the leader's
raft lease can lapse, and the leader then refuses every proposal, the cut's lease included, until
the lease recovers, while the cut's deadline keeps running. Loaded CI runs keep a backup's lease
refused for seconds at a time, and the margin is several times what such a refusal and a loaded
drain take together. The expiry outline gives its 12-second first hold a 30-second budget for the
same reason, so that only its held second cut expires.

A runner task that another scenario's synchronous step stalls for longer than the margin would
lengthen a hold timed on the runner task until the product deadline expires. The backup cut gate
in `src/fault_injection/backup_cut.rs` therefore times each hold itself, from the cut's own arrival
and on the cut's own task in the node, and the step that injects the holds only learns when the
first one ended. A stalled runner delays when the step learns of a release, never the release
itself. When the command is expected to outlast every hold, the step still watches it and reports a
command that ends early with its own outcome.

A gate holds every cut that reaches it, not only the first. When the leader cannot record a
backup's outcome, because leadership moved or because it could not confirm that it still leads,
the backup runs again under its execution reference, and the outcome and the archive the client
receives are that later run's. A gate that held only the first cut would let the later run pass
unheld: its archive would record cuts the scenario never delayed, and a second cut held past its
budget would complete instead of expiring. `A backup that runs again under its reference returns
the second-domain quiesce expiry again` and `A cluster backup that runs again under its reference
holds each delayed cut again` force that later run on three nodes: they hold the leader's command
finalization and move leadership before releasing it. They give each cut a 30-second budget. A cut
that passed its gate unheld lasts a few seconds whatever its budget, and the wider margin leaves the
held cuts room for the new leader's first lease. The expiry outline's CLI wait covers a second run
of its 12-second hold and 30-second cut budget.

A step waits up to 120 seconds for a backup to reach a gate, as the pause steps do. A cut arrives
only after its lease, its pause and its drain, which a loaded runner delays, and a run that starts
again reaches a later gate only after the earlier gates have held its cuts too. A 30-second bound
failed on loaded runners, both before a backup's first cut arrived and before the second cut of a
backup that had started again arrived. The wait ends as soon as a cut arrives, and the bound
neither measures nor shortens any hold or cut budget.

The bounded native backup recovery outline instead expects the client's ten-second wait to end
with typed uncertainty while the admitted backup continues. It uses both a two-second and a
twelve-second first hold inside the unchanged thirty-second cut budget, on one and three nodes.
The wait may expire before the second cut is reached. The step observes its twelve-second harness
bound from dispatch on a runtime worker, waits independently for both controlled cuts, and recovers
through a fresh client under the original reference. The recovery orchestration also runs off the
runner, so delayed step polling cannot restart the wait bound or leave the second cut held. The
archive must retain both quiesced cuts and the measured first hold.

The paced branch-buffering outline runs its domain at rate 1200 with one-minute logical input
flush and collection, a six-second period and skew, and a five-hour branch TTL. Each of these is
sixty times the physical time it takes, so the flush and the collection each still close 50 ms
after they open, while a physical clock would hold each for a minute. The receive budget is five
seconds: twelve times shorter than the wrong clock's first window, and many times longer than
relay delivery takes on a loaded runner. With one-second windows at rate 20, the budget had to
stay below a one-second physical cadence, and its 800 ms then measured relay delivery as much as
the clock. A relay owner fans out one batch at a time, across every branch of the relay, and waits
for each remote receiver to admit the batch before it takes the next one. On three nodes the
placement puts other nodes between a row's ingestor, both relay owners, the reingestor and the
session, so each owner fan-out and each delivery to a remote owner waits for an admission between
nodes; on one node every hop is local. In a loaded parallel run the first rows of the two tenants
arrived 269 ms apart and both second rows missed the 800 ms deadline. A relay owner that spends
that long on each batch produces exactly that pattern. A companion outline makes every relay owner
wait 250 ms before each batch: at the 800 ms budget that wait failed the same way on one and three
nodes, while the outline requires every row within the five-second budget.

Each outline's bounded payload sample owns the raw session on a runtime task and records every
receipt instant, while Cucumber only awaits the completed sample. Checking the deadline between
rows on the runner task could fail after the first tenant rows even when both second rows were
already queued within the budget. A paused-clock regression supplies those rows at 100 ms and
delays the caller past five seconds; another requires a missing second row to expire at exactly
five seconds despite a duplicate first row. A transport disconnect instead fails immediately with
its cause. The public outlines check each concrete tenant key and both exact payloads while
deliberately delaying collection of the result for six seconds, beyond the unchanged receive
budget. That delay yields to the runtime and other scenarios; it does not block the shared runner.
A scripted one-minute physical cadence still expires the same five-second sample. Every returned
row must also have a receipt instant strictly before the original deadline: a paused-clock reader
that resumes at or beyond either the 500 ms or five-second bound must fail even when its final row
is ready. Offloading removes runner polling gaps; it neither extends the budget nor proves that
runtime workers cannot be delayed.

The slow historical-domain Immediate-flush outline keeps its 500 ms receipt budget, physical
batching minimum, both exact tenant keys and payloads, and one- and three-node topology. It
delays only collection of the observation result for one second. The reader must start and
capture its deadline before the caller first polls the result. Its paused-clock control
supplies both rows at 100 ms and requires receipt signals inside the original 500 ms window
before the caller resumes after one second. A missing or late row still fails, and the
wrong-clock control expires this 500 ms budget with a one-second cadence, as it expires the paced
outlines' five-second budget with a one-minute cadence. The public receive bound qualifies the
physical clock choice without changing domain time or the batching minimum.

The duration window outline establishes its observation boundary through a public subscription:
it first waits for a probe branch's window to emit, then submits all four interleaved tenant rows
as one HTTP payload unfolded into a decoded batch. Explicit source timestamps are already due
when that batch arrives, while `TIMESTAMP NOW` gives the whole batch one fresh ingestion snapshot.
Each branch receives its two rows together before its next timer check. The outline retains both
rates, one- and three-node clusters, exact counts and sums, timestamp preservation and its
ten-second assertion bounds. Independent HTTP requests would allow domain-time completion between
rows, even when their explicit source timestamps are identical.

The reorderer sorting fixtures submit their ordering set as one JSON array and unfold it with JAQ
into one input batch. Each route sorts the rows buffered at its own flush; separate HTTP requests
do not establish a single sorting window. The one-node and three-node examples retain their exact
ordered payloads, tenant keys, and existing flush and receive deadlines.

Redis burst fixtures observe subscriber readiness and then submit all publications as one driver
pipeline. Every reply must report a subscriber; a partially accepted burst fails without replay.
This keeps the bounded-intake scenario's 256 publications inside its held entity gate without
requiring 256 separately scheduled request/reply round trips. The scenario retains its 30-second
gate deadline, exact BUFFER and DROP counters, and observation budgets. The scripted harness peer
withholds every publication reply until it has received the complete burst, checking the same
pipeline owner used by the public step.

The combined message/duration window outline publishes each message-threshold group as one
decoded batch, interleaving two branches and checking their exact counts, sums and keys. After
observing both outputs, it publishes one row per branch and measures each duration output from
that publication, keeping the three-second lower bound and ten-second subscription deadlines.
A late subscription read can lengthen the observed delay. The production-owner regression checks
two retained rows immediately before and at the duration boundary, and three rows meeting the
message threshold before that deadline.

The two scenarios that install a 48 MiB resource on a restarted node wait five minutes for its
ready replica, and the native client waits ten minutes for the `UPLOAD RESOURCE` before it. The
leader answers an upload only once every live node has fetched, verified and installed the
archive, and the restarted node records its pending replica and opens its transfer within about
seven seconds of starting. Both waits are then the pace of a 48 MiB transfer under the suite's
storage and CPU load, which canonical producers on 21 October 2026 measured at 7 to 59 seconds for
the same archive, and 24 such scenarios beside each other on a disk-backed host at 25 to 53
seconds. The former 30-second catch-up bound held only on a lightly loaded runner.

The harness keeps these boundaries explicit.

- **Ordinary commands.** The Plexus commands a scenario runs go through the production Rust client,
  with its execution identity, redirects, and reconnects. This includes the transaction qualification
  graph's setup commands, which retain each execution reference if leadership changes while setup
  is applying. When a command document has server statements after a subscription, the harness
  sends those statements through the client and opens a raw session for the subscription on the
  node requested by the step. A standalone subscription stays on that requested node, including
  after failover. The large materialized backup workload submits
  each graph statement through the production client so a leadership change during its many relay
  definitions recovers that statement by reference; its Row subscription opens on a raw session
  after the graph is ready. A step that sends commands to the leader over a raw wire session,
  because they create a subscription or open a transaction, learns a command's outcome the way a
  client must when leadership moves before the command reached the leader, or after the leader
  admitted it and before its outcome was known. It sends the command again with the same
  execution reference, on a session of the same user and domain, to each node in turn until the
  node that leads answers with an outcome only a leader knows. A follower refuses the command
  before admitting anything, a stopped node refuses the connection, and a node that still believes
  it leads answers with another unknown outcome, so recovery reads no status to find the leader,
  and a step's background execution recovers the same way. No new round starts once 60 seconds
  have passed for one command, and a repeated command that joins an execution still applying waits
  for its reply. While the step's session holds no subscription and no transaction, it moves to the
  node that answered and the step goes on there. A session that holds one stays where it is, so
  its subscriptions keep delivering. For the active session a later step reuses, for a background
  execution's session, and for a setup session once it opened a subscription, another session of
  the same user learns the outcome of a command that leaves what the session holds alone. A
  command sent while the session is bound to a transaction keeps its unknown outcome and fails the
  step, and so does a command that begins or ends a transaction, opens or deletes a subscription,
  attaches a clock or changes the domain on a session that cannot move. A request a step sends once
  under a scenario-chosen execution reference learns its outcome the same way before the step
  records its disposition. The backup steps that send a backup through the raw session learn each
  outcome the same way before they assert what it carries: a cluster backup under a fresh
  reference, its recovery to another local destination under that reference, and a kept archive's
  recovery under the reference the CLI reported. A leader whose lease lapsed at admission
  therefore costs such a step a repetition of the reference, not its assertions. A leadership move
  a scenario arms holds the next persistent command of the node it moves leadership from, either
  after the command's reference lookup and before its admission proposal, or after its durable
  admission. It releases the command once the successor reports itself the leader and the node it
  moved from reports the successor too, so that node knows it no longer leads when the command
  goes on. A proposal held that way answers `OutcomeUnknown` with cause `LeadershipLost`, as a
  lapsed lease's does, and the backup scenario that holds its first backup there requires the step
  to learn the backup from the successor and recover it to another destination.
  The paced simulation's published graph is loaded through one native client so its
  BEGIN/COMMIT state and command references survive an interrupted finalization. Raw wire probes
  retain the protocol's received dispositions. A restore completion probe explicitly requesting
  outcome recovery repeats its same reference, statement and archive against the cluster's nodes
  after an unknown outcome, redirect or interrupted transport. It starts no further attempt after
  its 120-second recovery budget; each stream keeps its existing reply timeout. It returns a
  definitive failure or upload refusal immediately, and keeps the terminal reply's actual origin
  and complete report. A scenario whose subject is what a restore did, rather than how the
  protocol answered one stream, learns the restore's outcome through that probe: the deduplicator
  and window conversions refused for room, the idle lifetime of restored materialized branches,
  and the resume of materialized generations larger than the bulk budget. In a cluster of more
  than one node a single stream can be answered `OutcomeUnknown` with cause `LeadershipLost`
  without any election, when a loaded runner delays the heartbeats that confirm the leader's lease
  as it records the outcome; a single-node leader is its own quorum, and its lease never lapses. A
  raw probe therefore stays only where the answer is the subject: a repetition joining an applying
  restore or returning its recorded outcome, a reference conflict, a redirect, a controlled
  leadership loss, or an interrupted upload sent again. The room and idle-lifetime scenarios hold
  the restore's outcome before its coordinator records it and transfer leadership on three nodes
  before releasing it, so their three-node examples always learn the outcome under the same
  reference as recovered, and their one-node examples report it as executed. The staging scenario
  transfers leadership while publication is paused on three nodes, while retaining the one-node
  example and its staging and branch-state checks.
  A paced-driver file fixture writes only the
  scenario's ledger or effect file before the driver starts. The published driver reads that
  file, and the scenario checks the process exit and the occurrence count of its terminal cause.
  No harness deadline shortens
  native commands. Only
  the status path described below is harness-owned, and it is a separate test-only boundary: it
  opens its own session and never redirects, reconnects, or retries by itself, so it adds no second
  client policy.
- **Controlled command recovery.** Standalone preparation barriers retain a coordinator's
  transaction observation and its preflight result while the scenario transfers leadership.
  Node- and domain-scoped resume barriers then hold the applying command's pause until the
  scenario releases it. Each barrier is one-shot and uses the shared pause owner's reached and
  released observations. The scenario asserts the original execution reference's outcome and a
  dependent command through the public session protocol; it adds no setup delay or request retry.
- **Restore publication and staging failures.** A durable state publication failure armed for a
  restored domain fails every publication of that domain's restored state, on whichever node
  publishes and after the store has made the publication durable, until the scenario clears it. A
  checkpoint staging failure is armed for a restored domain and one checkpoint kind: WASM guest
  state, materialized relay state, or deduplicator and window state. It fails every staging of
  that domain's checkpoints of that kind, on whichever node stages and before the store stages
  the checkpoint, until the scenario clears it. Each kind of one domain is armed, delivered and
  cleared on its own. A failure that the first publication or staging consumed would let a leader
  that resumes the restore, because the failed outcome was never recorded, publish or stage again
  and complete the restore. Each control keeps whether a publication or staging met it, so a
  scenario asserts the failure as pending before the restore and as delivered once the restore
  failed, instead of inferring delivery from a later success. Clearing it ends the condition
  without changing a recorded outcome, and the scenarios clear it before asserting that the
  incomplete-state gate, not the injected failure, refuses `START`, and before a later restore of
  the same domain. The three-node regression, an outline over a durable publication failure and a
  materialized checkpoint staging failure, holds the first coordinator's failed outcome before it
  is recorded, moves leadership, and requires recovery under the same execution reference to end
  failed and stay failed once the condition is cleared.
- **Forced entity drain timeouts.** A forced entity drain timeout armed for a domain refuses the
  next drain observed there, after one complete status round. When a durable execution owns the
  domain's mutation at that drain, the timeout keeps refusing the same drain, the same purpose,
  relays and entities, whenever that execution observes it again on whichever node coordinates it,
  until another execution or none owns the domain. A drain that no execution owns meets the timeout
  once. A timeout that only the first run met would let a leader that runs the command again under
  the same execution reference, because the refused outcome was never recorded, drain and apply
  the alteration, and the scenario would observe that success. The execution's other drains, such
  as a source-intake drain before the full hold or the other units one node drain moves, are left
  alone, and an alteration a scenario runs later owns the domain under its own reference and
  drains normally. The three-node Syslog regression moves leadership while the alteration waits at
  its entity gate pause, holds the new leader's run of the same transaction until the first run
  has refused and released its hold, and requires the alteration to end refused with every frame
  delivered once.
- **Console backup download pauses.** The console reload scenarios hold the archive's first
  download before it streams, so the reload interrupts a download in progress. The archive is
  assembled and served by the leader that completes the backup. A leadership move while the backup
  runs makes the new leader run it again under the same reference and serve the archive it
  assembles, so no step can name that node before the backup ends. The scenarios therefore arm the
  download pause on every node. Only a node that retains an archive under the reference reaches its
  pause, and the step that waits for it reports which node did. Each pause is one-shot, so the
  download the reload starts on that node streams while the first one stays held. The scenario
  then releases every node's pause, and scenario teardown releases any pause still armed. One
  three-node scenario moves leadership under the durably admitted backup command, and requires the
  same paused download, reload and verified resumed archive. The scenario whose reload must resume
  nothing arms the same pauses, so a backup the console resumed by mistake would stay held on
  whichever node served it instead of downloading and clearing its record.
- **Console command response pauses.** The shared console session scenario that loses its session
  connection holds the backup command's reply after the command ran and before either transport
  delivers it, so the lost connection takes the reply with it and the console's reconnect must
  recover the outcome under the command's execution reference. The command runs on whichever node
  leads when the console sends it, and a leadership move after the redirect makes another node run
  it, so no step can name that node in advance. The scenarios therefore arm the response pause on
  every node, and the step that waits for it reports which node reached its pause. Each pause holds
  only the first command its node completes, whoever sent it: the harness's status request is a
  `SHOW CLUSTER STATUS;` command too, so no step polls a node's status while its pause is armed. A
  command sent again under the same reference joins the held execution or returns its recorded
  outcome without stopping there. The scenario releases every node's pause once the connection is
  lost, and scenario teardown releases any pause still armed. Each shared session scenario has a
  three-node variant in which leadership moves after the redirect, from the leader the console was
  redirected to back to the node it was opened on, so the console's session, its download and its
  restore streams follow a leader other than the one the scenario saved before the console opened.
  The variant without a response pause moves leadership under the durably admitted backup command.
  The variant that loses its session connection transfers leadership before the backup, waits until
  the console reports the new leader, and only then arms the pauses, because the wait for a
  leadership move polls the successor's status.
- **Timed control probes.** The silent-peer health scenario connects a named production client
  before holding the peer's health response. Its nine-second probe then sends the user creation,
  relay creation and cluster-status statements in order on that client, under one deadline. The
  bound covers command admission, mutation, visibility and the complete reply, including any recovery
  the production client requires. Initial connection and authentication precede the fault and the
  measurement. The probe runs on an owned runtime task, with its absolute deadline captured before
  dispatch, and records completion there; a delayed Cucumber runner only learns that retained
  result later. The other peer must report connected, and the new relay's scheduled owner must be
  observed before the silent response is released. This is a harness measurement of isolation,
  not a different product deadline.
  Its completion regression also holds every application-revision reply from the two followers
  after sampling the reply. Local revision publication and gossip continue, so the same prepared
  client's user creation, relay creation and cluster-status query must finish in order while
  those replies remain held. This batch has one twenty-second harness budget. On 25 October
  2026, with 16 runtime workers, 48 slots, six large materialized workloads and 192 distinct
  health cases, 88 original requests exceeded nine seconds but all succeeded before fault
  release in 9.016 to 11.998 seconds. Every pre-release status still reported node-3 connected.
  Parallel mutation dispatch also exceeded nine seconds, so adding their latencies was not the
  sole cause. Twenty seconds exceeds 1.5 times that measured maximum; the product's readiness
  propagation and command deadlines remain with their owners.
  The final retained client status must name the new relay's scheduled owner before any fault is
  released. A direct status request to node-1 checks its node-3 peer while the faults remain held;
  it asserts the answering node's Raft identity and never replaces the timed client's retained
  result. A separate controlled leader transfer makes a node-1 client's mutation follow node-3,
  then checks the returned status names node-3 and lists node-1 as connected. The timed helper also
  checks each recorded completion instant:
  a timeout future can return an already-ready reply when polled after its deadline. It captures
  that instant before diagnostic logging and names the client, domain and statement position.
  Reached observations prove both revision pauses were exercised before explicit release. The
  shared pause registry retains these controls through request cancellation; scenario cleanup
  releases and clears them even after an assertion fails. The original health-only scenario keeps
  its nine-second sequential probe and remains an independent check.

  The large-resource control probe connects a named production client to the restarted node once
  it has started, then waits until that node's
  `nervix_interconnect_bulk_bytes_total{class="bulk",direction="received"}` shows archive bytes
  arriving, so the probe cannot run before the transfer it is meant to overlap has begun. Its
  `SHOW CLUSTER STATUS` must then complete within one second of dispatch, answered by the restarted
  node itself while the rest of the archive streams in. The former probe timed a fresh client's
  connection and authentication, the statement and a second authenticated session, all on the
  runner task. With 48 copies of the scenario on 16 runtime workers and no retries on 22 October
  2026 it failed 27 of them: connecting took 0.12 to 1.36 seconds, and the runner task polled the
  probe up to 0.9 seconds late while a runtime worker was at most 0.16 seconds late. Under the
  same load the corrected probe passed every copy in three runs, completing 23 to 157
  milliseconds after dispatch, and sampling in one of them showed each restarted node's archive
  fetch in flight before and after its probe.
- **Bulk execution isolation.** The one- and three-node scenario connects a named production
  client before holding every node's bulk workers. This keeps the complete command path under the
  fault even after a follower redirects the client to the leader. Six sequential domain creations
  share one ten-second budget; a domain listing has its own ten-second budget. The shared timed
  client observer drives and timestamps every reply independently of the Cucumber runner, before
  diagnostic logging, and the scenario checks the sixth creation's acknowledgement explicitly.
  Initial connection and authentication precede the fault. The hold remains through the readiness
  and stable-leader observations. Scenario cleanup releases every recorded bulk hold before
  diagnostics and node stop, including when occupation or an assertion fails; explicit release
  removes that node from the retained set. These are harness bounds for class-isolation evidence,
  not additional product deadlines.
- **Cleanup.** Scenario cleanup runs after the scenario's assertions, so it spends the harness
  cleanup budget rather than waiting for a product shutdown deadline. A scenario whose subject is
  shutdown, drain, or deadline expiry stops its nodes itself, inside its body, under the product
  deadlines it configured.
- **How nodes stop.** The harness stops an in-process node through its shutdown coordinator, with
  the same stop request a first `SIGTERM` makes to a server process, and the node runs its shutdown
  phases under its configured deadline. Registering termination signals, the forced-exit supervisor,
  and exit statuses belong to the process boundary, which only the server-process fixture crosses.
  See [Shutdown And Recovery](./shutdown.md#stop-requests).
  A real-process cluster can deliver `SIGKILL` to all three voters before waiting for any child to
  exit. On restart it supervises every persisted voter's full startup concurrently, including
  executable preparation, launch, readiness and any lost-bind relaunch, within each startup's
  original deadline. One voter's executable preparation cannot postpone another's readiness
  probes, and peers that must move listeners can relaunch while the others await the persisted
  quorum. A start and a whole restart end once every node reports, through its public status
  endpoint, the same leader, all three voters, and a `connected` application-health target for
  every other node, as an in-process cluster's construction does.
  It can also fault one voter while the other two keep the quorum: `SIGKILL`, after which a step
  waits for that child to exit and checks the signal, or `SIGSTOP`, which freezes the process with
  its connections open, so its peers hear nothing from it and see no reset until `SIGCONT` lets it
  run again. A killed voter restarts from its own database and ports. Its killed process never left
  the voter configuration, so the nodes agree on their leader and voters again as soon as the new
  process follows the leader, while each peer still judges the voter by what it learned of the
  killed process. The leader's effective availability, which placement, failover and `RELOCATE`
  read, lists the new process only once the leader's gossip has seen it live and its application
  health no longer holds the killed process's target, as
  [Application Health And Availability](./interconnect.md#application-health-and-availability)
  describes. Before the relaunch the step therefore reads the generation of the target each running
  node observes the killed process through, and afterwards it waits for the same leader and voters
  and for every node to report every other node `connected`, the relaunched voter through a newer
  generation. A peer begins a target only for a process it has discovered, and it never discovers
  the exited process again, so a stale observation of the killed process cannot end the wait, and
  a `RELOCATE` onto the relaunched voter that follows finds it live. While a voter is killed or
  frozen, the fixture asks only the running ones which node leads.
  The held-installation restart fixture also stops every in-process node first. It then holds the
  first runtime installation one named node begins, before that node's process-start catch-up, so
  the restarted cluster answers opens that reach the node before it has installed any committed
  domain again. Startup waits only for Raft catch-up and the interconnect, which the hold does not
  stop, and the scenario releases the installation. Arming the hold after the stop keeps an
  installation the stopping node began from taking it.
  The restarted-voter observation fixture stops every in-process node before arming its fault.
  It blocks direct gossip between the first leader and one voter, then holds that voter's first
  relayed heartbeat while allowing the other voters' heartbeats to advance. The leader's
  reconciliation starts after that sample arrives, so slow harness startup cannot consume the
  product's observation grace. A one-shot barrier captures a scheduling pass inside that grace
  with the voter marked unavailable; its elapsed-time decision is captured before the barrier
  waits. The scenario checks the failure-detector verdict and final ownership through public
  cluster status. Releasing the barrier waits for that pass to finish before restoring heartbeats
  and application health. While the heartbeat is held, peer startup waits for listeners; Raft
  catch-up is checked after release, so the fixture never waits for progress its own fault prevents.
  The slow-readiness restart fixture also stops every in-process node first. It then arms a testing
  hook in the leader's reconciliation and restarts the whole cluster, counting one named node ready
  only once a leader of the restarted cluster has passed its startup voter observation: it observed
  every voter live, or its grace expired. The node runs and gossips from its start, so only the
  harness's own readiness waits on the hook, never the product. Its scenario requires a voter
  restarted beside that node to keep its work. A restart that started each node only once the one
  before it was ready would start that voter after the grace expired, and its work would fail over.
  This is an in-process scheduling regression; immutable-image external
  Chaos runs retain the real-process crash and restart evidence.
  The durable follower catch-up fixture creates client writes while the restarted follower applies
  its backlog and samples commands memory throughout that interval. Once the backlog is applied,
  stopping the writer finishes its admitted command before counting completed writes: the response
  can arrive after all voters have already applied that command. The fixture still checks the
  storage-derived catch-up bound, append-stream limit and commands-memory budget.
  Its initial burst of domain creation also uses one production client, retaining each command's
  execution identity across a leadership change during finalization. A raw session's recoverable
  disposition cannot end the fixture before follower catch-up starts.
  The domain write load fixture keeps several production client sessions committing domain
  creations through the leader, each waiting for its previous command. With slow consensus commits,
  another storage operation then keeps waiting behind the one that commits while a scenario acts.
  Stopping it finishes each session's submitted command and then requires every session to have
  committed at least one domain without a failed write. A session that reaches its write limit
  fails instead of filling the cluster.
  A listener bind failure retries only that listener family: the gRPC pair, HTTP, HTTPS,
  observability, web console, or interconnect. A readiness timeout may still move all listeners
  within the same startup budget. A changed interconnect address remains reserved while a peer
  may dial it. Metrics sampling follows the actual observability endpoint before the next launch.
  The durable catch-up scenario deliberately occupies the stopped follower's observability port
  until cleanup of its first startup attempt, so a metrics bind collision cannot silently turn
  the measured catch-up into an interconnect address and membership change.
  The planned Kafka handoff scenario installs its subscription and emitter stall, publishes and
  flushes both branch records, and then starts intake. Both records are therefore available to
  its first parallel batch. The source host waits for that batch's attached ACKs before polling
  the next batch; separate publications after intake starts do not establish one batch.
  The forced-recovery startup fixture holds the promoted replica's initial installation across a
  whole-cluster restart. After the cluster rejoins, it makes the former owner's application health
  unavailable. The destination records its missing schedule before releasing the held installation
  once, so the regression reaches that boundary without spending the recovery budget between
  scenario steps. Public placement, recovery outcome and both branches' output remain the verdict.
  `just coverage-harness-liveness <filter>` measures the ordinary startup and other harness owner
  regressions with counters of their own, as the [diagnostic lane](#diagnostic-lane) section
  describes. A patch that also changes product code passes the retained `lcov.info` to
  `just coverage-patch` as another `--report`, beside the workspace report.
- **Test defaults.** An in-process node's shutdown timeout defaults to four minutes rather than the
  product's `50s`, which leaves the bounded shutdown phases scenarios configure by default room to
  finish, so only a scenario about the deadline reaches it. A server process runs with the product
  defaults unless its scenario passes an option such as `--shutdown-timeout`.

## Deadline Ownership

Every lifecycle operation of the harness is bounded by exactly one of these deadlines. Each budget
is a constant in the module that owns it, under `tests/common` unless named otherwise; where a
second module runs the operation, it is named after the owner.

| Operation | Owner | Deadline | When it passes |
| --- | --- | --- | --- |
| One status request: connect, open a session, send, receive | `status_request.rs` | 10 seconds, or what is left of the enclosing phase when that is less | The request fails naming the operation that was still pending |
| A status wait: leadership, voters, a consistent leader, interconnect status, applied index, a status fragment | `status_request.rs`, run by `cluster.rs` | 40 seconds from the start of the wait | The step fails with the elapsed time, the last status text, and the last typed failure |
| Status snapshots of every node, for teardown or for a failure message | `status_request.rs`, run by `cluster.rs` | 10 seconds for the whole cluster | Each node keeps its status, its typed failure, or the operation its request was in |
| Readiness of one startup attempt | `node_startup.rs`, run by `node_liveness.rs` | 36 seconds, inside the node's startup budget | The attempt fails naming the node, attempt, elapsed time, task state, and last readiness outcome |
| Cleanup after a failed startup attempt | `node_startup.rs` | Whatever remains of the node's one 84-second startup budget | The node keeps its task, storage and listeners if the budget expires before it ends; a new launch requires that exact task to have ended |
| One node's startup, every attempt included | `node_startup.rs` | 84 seconds | The startup fails with the typed record of every attempt |
| The node startups of one cluster construction | `node_startup.rs`, run by `cluster.rs` | 84 seconds per node, shared by the whole construction | The node that ran out ends the construction |
| A node a scenario stops itself | `cluster.rs`, recorded by `node_stop.rs` | The longest of five minutes, the configured shutdown timeout, and the configured drain phases | The step fails naming how long the node had been stopping and the shutdown phase it was in, and the node keeps its task and everything it holds |
| Scenario cleanup of a whole cluster | `cluster_teardown.rs` | 60 seconds for every node together | Nodes still stopping are left to end themselves, keep their storage and ports until they have, and are recorded as still stopping |
| Stopping a scenario's HTTP and gRPC receivers and its HTTP proxies | `http_receiver.rs`, `grpc_receiver.rs` and `http_proxy.rs`, run by `tests/scenarios.rs` | 6 seconds for every receiver and proxy together: 5 for its connections, 1 for its accept loop | Still-running connections, then the accept loop, are aborted and joined; a receiver records its stop as forced |
| An HTTP receiver wait: captured requests or a recorded fault | `http_receiver.rs`, run by `tests/scenarios.rs` | 60 seconds from the start of the wait | The step fails with the captured count, the fault count, and the latest fault |
| An HTTP proxy wait: a request it forwarded or a tunnel it opened | `http_proxy.rs`, run by `tests/http_transport` | 60 seconds from the start of the wait | The step fails with everything the proxy recorded |
| A gRPC receiver wait: captured calls | `grpc_receiver.rs`, run by `tests/scenarios.rs` | 60 seconds from the start of the wait | The step fails with the captured count, the fault count, and the latest fault |
| A server process's startup, its exit, or a log line | `server_process.rs`, reading the executable through `server_executable.rs` | 120 seconds from the launch for reading the executable, spawning the process and waiting until it accepts commands; 120 and 60 seconds | The step fails, quoting the last 80 lines of the process log. A failed startup also says how reading the executable went, which kinds of answer the readiness probes got and when, and, when its deadline passed, what the still-running process and its host were doing |
| Convergence of a started or restarted real-process cluster, or of one restarted member | `process_cluster_convergence.rs`, run by `server_process_cluster.rs` | 120 seconds for the running members to report the targets they observe a killed member through before it is relaunched, then 120 seconds for convergence after the start or relaunch, with each status request capped at 10 seconds | The step fails naming the first condition the members' latest answers did not meet in the last round that ended before the deadline, with every member's latest status and any later failed request; a status line the wait cannot read fails it at once |
| An `UPLOAD RESOURCE` a step runs through the native client, every attempt and redirect included | `cluster.rs`, run by `tests/scenarios.rs` | 10 minutes for the client's request and retry windows, instead of its 120-second defaults; the client's other commands keep the defaults | The step fails with the client's typed outcome: an uncertain upload or the expired retry deadline |
| A one-shot CLI command or a streaming output assertion | `tests/scenarios.rs` | 60 seconds for a command, 30 seconds for a subscription line, the duration the step names for a clock line, between 10 and 60 seconds, and 10 seconds for clock-process exit after Ctrl-C | The step fails with the process result or retained output lines |
| Text the interactive CLI is expected to display, its startup banner included | `cli_terminal.rs`, run by `tests/scenarios.rs` | 60 seconds, pressing Enter every 250 milliseconds so the REPL draws a prompt and prints the events it queued; a row expected from repeated HTTP posts gets 2 seconds after each post within the same 60 | The step fails quoting the newest 40 lines the terminal displayed, with control sequences removed and repeated lines collapsed |
| The interactive CLI's exit after the scenario types `exit` | `cli_terminal.rs`, run by `tests/scenarios.rs` | 60 seconds | The step fails with the exit status or the elapsed wait, quoting the same transcript |
| One draw from the port pool | `port_pool.rs` | 65,536 consecutive draws that land on reserved ports | The draw fails with the pool exhausted |
| The whole scenario run | `suite_watchdog.rs` | 105 minutes, injectable | Every active scenario is reported, live nodes get a 60-second cleanup window, and the process exits `124` |
| Stopping the test dependencies after the run | `suite_watchdog.rs` | 2 minutes | The containers are left to the runner |
| Dropping the runtime after the run | `suite_watchdog.rs`, run by `tests/scenarios.rs` | 60 seconds | Blocking tasks still running are abandoned |

Everything else a scenario's body does, including its Plexus commands, broker and metric waits, and
assertions, keeps whatever bound its step declares. Some of those steps check their bound only
between requests, so a request that never returns outlasts it; the suite budget bounds them in every
case. It is also the only bound on the time a scenario spends queued for its concurrency permits and
on closing the browser during cleanup, neither of which has a budget of its own. Browser cleanup
explicitly awaits the Playwright driver's shutdown before dropping its handle. The driver's own
five-second exit grace is asynchronous, so cleanup keeps the shared scenario runner and its
watchdog available rather than waiting in Playwright's blocking destructor.

## Absolute Phase Deadlines

A phase deadline is one instant on the monotonic clock: the moment the phase started plus its
budget. Operations inside the phase receive that deadline rather than a fresh timeout, so the phase
ends when its budget says it does, however many requests, retries, or polls it made.

- **Nesting.** An operation inside a phase that may take at most a budget of its own gets whichever
  comes first: that budget from now, or the phase's deadline. A status request inside a status wait,
  a startup attempt inside a node startup, and a node startup inside a cluster construction are all
  nested this way.
- **Bounding.** Running an operation within a deadline ends it when the deadline passes. An
  operation that begins after the deadline has passed does not run at all, so a stream of
  immediately ready work cannot keep a phase alive.
- **Polling.** A poll repeats an attempt until one output is accepted or the deadline passes. Every
  attempt receives the same deadline, the pause between attempts never sleeps past it, and on expiry
  the poll returns the last output it rejected and the last failure it saw.
- **Owned work.** A wait for something its owner must keep, such as the join handle of a node task,
  borrows it for the length of the wait and never takes it. Whether the wait finishes, runs out of
  time, or is cancelled from outside, the owner still holds the handle, so no path leaves a node's
  task running with nothing able to join it.

The deadline keeps its start and its budget rather than their sum, as the product's shutdown
deadline does, so a budget of any length cannot overflow the clock.

## Deriving A Budget

A harness budget is derived from named policy inputs rather than written as an independent literal.
Each input is a measured duration, the slowest healthy value observed while the whole suite ran at
the CI concurrency factor, or a headroom factor that multiplies one. Every derived budget is checked
against the budgets around it by assertions evaluated at compile time, so a change that breaks an
ordering fails to build rather than producing a harness that outwaits itself.

| Budget | Derivation | Measured input |
| --- | --- | --- |
| Status request, 10 seconds | The 40-second status wait divided by the four requests it must outlast when none of them replies | The slowest healthy reply, 3.7 seconds, times a headroom of 2 fits in one request |
| Status snapshots, 10 seconds | One request timeout, because every node is asked at once | None of its own |
| Startup attempt, 36 seconds | A policy input | The slowest healthy node startup, 24.2 seconds, fits in one attempt; the same 3,465 startups had a 2.0-second median and a 4.3-second 99th percentile |
| Node startup, 84 seconds | Two budgeted attempts, each funding 36 seconds of readiness, 5 seconds of cleanup and a 1-second pause; cleanup and later attempts share the remaining total | Stays under a 90-second ceiling |
| Cluster cleanup, 60 seconds | The slowest healthy cluster stop, rounded up to 15 seconds, times a headroom of 4 | 12.2 seconds at the slowest over 163 cleanups, with a 0.15-second median and 1.6 seconds at the 90th percentile |
| Upload command, 10 minutes | The slowest healthy upload of a 48 MiB archive, rounded up to 120 seconds, times a headroom of 5, and shorter than the suite budget | With 48 such uploads on one shared, disk-backed host, those that completed did so inside the client's 120-second window and 35 others expired it; 72 on an idle host took at most 52 seconds |
| Suite, 105 minutes | The 155-minute scenario job limit, less 35 minutes for pre-suite work and a 15-minute reserve after it | Run 40419254390 completed 3,703 attempts in 49m31s once the parser was optimized; twice the rounded 50-minute healthy ceiling plus five minutes of slack fits the budget. Across 17 runs of one inventory, the slowest runners ran every scenario 1.7 times as long as the fastest |
| CLI backup or restore, 900 seconds | The slowest healthy three-node restore, rounded up to 300 seconds, times three installations: a leader change while a restore installs its state makes the next leader install it again once the CLI sends the archive again | The three-node restores that completed in the scenarios jobs of Check runs 39683355582 and 39683521277, both on slow runners, took 291 seconds at the slowest for a 42 MB archive and 260 seconds for the 84 MB keyspace and window archives; restores that took longer ended at the former 300-second budget. Six concurrent local copies of the keyspace and window scenario saw a restore start installing its state three times |

The assertions keep these orderings, among others:

- A status request is materially shorter than the waits and startup attempts that repeat it, and one
  attempt outlasts three readiness requests that never reply.
- Status snapshots are shorter than the waits whose failures they explain.
- A startup attempt outlasts the measured healthy readiness, the cleanup allowance used to fund the
  total is short beside readiness, and the node startup budget stays under its ceiling.
- The cluster cleanup budget is shorter than the watchdog of a node a scenario stops itself, and the
  test shutdown timeout lies between the default shutdown phases and that watchdog.
- The suite watchdog's cleanup window fits inside the reserve, and so do the dependency stop and the
  runtime shutdown together.
- A CLI backup or restore ends inside the suite budget, so its scenario fails with its own reason.

The suite funds a runner at half the measured healthy throughput, with five further minutes for
retries and the final active attempts. Slow-run variation multiplies the whole workload, so the
headroom scales the measured ceiling instead of adding one fixed allowance to an increasingly
large inventory. The factor of two covers the spread that 17 runs of 21-26 October on one
inventory measured between runners. Increasing a measured input eventually fails the compile-time assertion. Measure
again whenever the suite, its concurrency, or the runner changes. A harness regression compares
the workflow's actual job limit with the limit that derives the suite budget.

## Status Requests And Status Waits

The harness reads cluster state with `SHOW CLUSTER STATUS;`. A status request opens a new connection
to the node's session endpoint, in plaintext or over TLS trusting the test certificate authority
when the node serves its session API over HTTPS. It opens a session authenticated as the default
test user, sends the command with a request identity, no domain, and a fresh execution reference,
and reads the session's frames until the reply naming that request identity arrives, keeping the
session's request stream open so the node never sees the session end while it answers.

The request has one deadline: 10 seconds, or what is left of the phase that sent it. Its four
operations run in order, each within whatever that deadline has left.

| Operation | What it waits for |
| --- | --- |
| Endpoint connection | The transport connection, including the TLS handshake when the node serves HTTPS |
| Session establishment | The node accepting the authenticated session |
| Command send | Handing the command to the open session |
| Response receive | The reply naming the command's request identity, past any unsolicited server events that arrive first |

A node that never completes an operation holds the request in it until the deadline: a listener that
never accepts leaves a TLS handshake waiting, a node can accept the connection and never open the
session, and a node can take the command and never answer it, or answer only with unrelated events.

A request ends in one typed failure: the deadline passing, naming the operation still pending and
the budget; an endpoint or TLS authority that could not be configured; a command frame that could
not be encoded; a connection, session, or response-stream error carrying the transport's status; a
session that ended before the result; a frame from the node that does not decode, or a reply to the
command that is not a command result; or, when the caller needs the status text, an unsuccessful
command result with its disposition, message, and diagnostics.
Client diagnostic scenarios inspect the failed disposition and source span from that result, so
the assertion uses the delivered diagnostic rather than parsing the displayed message.

A **status wait** polls one node every 200 milliseconds until its parsed status satisfies a
condition, within 40 seconds from the start of the wait. The waits the harness performs are a
current leader, a specific leader or a leader other than a given node, a Raft state, the expected
voter set, an interconnect status to a peer, an applied index at least the leader's, and a fragment
of the status text. Two waits read every node at once: a leader the running nodes agree on, and a
consistent leader, which requires every node to name the same leader, that leader to report itself
`Leader`, and three consecutive polls to agree. Every request of a wait receives the wait's
deadline, so a wait whose last request never replies still ends at its original deadline rather than
one request timeout later. A wait that expires fails its step with the elapsed time, the last status
text it rejected, and the last typed failure it saw. The meaning of the `connected` interconnect
status these waits read belongs to [Cluster
Interconnect](./interconnect.md#application-health-and-availability).

The stopped-voter `DROP NODE` scenario waits for the public status report to show that the voter
is no longer visible in gossip before removing it, then requires the exact remaining voter set.
Application health can report an unavailable voter while gossip still sees it live, so an unavailable
verdict does not establish membership-removal readiness. Scenarios that exercise application-health
failure classification assert that source under their own controlled health fault.

The correlator match-policy scenario observes each left input's received counter on the correlator
owner before posting the next independent input. An HTTP acceptance alone does not order execution
across ingestors. Its two-minute correlation window accommodates those bounded readiness waits
without expiring the records whose match policy it asserts. The Kafka handoff scenario that pauses
after entity-gate engagement uses a test-configured two-minute gate lease so its broker-delivery
assertions and pause release fit inside the handoff budget under concurrent suite load.

The branch-activity sampling scenario observes each initial input at the correlator through its
received counter, then waits for the initial branch to disappear from the relay and for the retained
branch-instance count to reach zero on both the HTTP intake node and the correlator owner. That
count deduplicates a concrete key while any local owner retains it; zero after confirmed receipt
establishes retirement of the initial state. Each runtime owner has its own physical maintenance
scan, so relay disappearance does not identify the correlator's scan phase.

Both fresh left inputs are posted concurrently after the idle period, followed immediately by both
matching right inputs before either subscription result is awaited. The unordered assertion requires
exactly one complete field set for each expected pairing within the unchanged ten-second bound.
Both TTL/rate examples have a three-second physical lifetime: sleeping that long before posting a
matching input would race legitimate expiration, because HTTP and scheduling delay also consume the
lifetime. The existing three-second physical pause follows the matched outputs, then bounded metric
observations establish retirement on both owners. Fresh-clock sampling has direct production-owner
unit coverage; this public scenario proves initial-state retirement, branch isolation and exact fresh
pairings without assuming the phase of an owner's next scan. Logical TTLs, domain clock rates,
physical scan cadence and correlation policy retain their own contracts.

The materialized-state expiration and recreation outline gives its fixture a 30-second branch TTL,
allows 30 seconds to observe each complete row and 60 seconds to observe retirement, and posts both
recreated branches concurrently. A short fixture lifetime can end while the shared scenario runner
is delayed between an HTTP response and a state query; extending only the query's wait cannot make
an expired row visible again. The outline still requires the initial row, an empty publication after
expiration, and the exact recreated keys and payloads on one- and three-node clusters. These bounds
belong to the fixture and its observations; product retention and deadline policies keep their own
contracts.

The remote materialized reuse scenario requires the generator node's relay memory to read zero while
that node retains the generation it reuses. Relay memory is a node-wide class: a delivery the node
sends keeps its encoded body charged until the receiving owner admits it and its record storage
until the outcomes of its records return, and a fan-out grant the node issues for one of its
sessions' subscriptions reserves the body, the largest decoded batch and decode scratch. While the
generator delivered each 100-millisecond occurrence to an output relay on another node, a
200-millisecond poll read zero only between two deliveries, and a loaded runner can stretch each
delivery until no poll falls between them. The scenario therefore makes the node idle before it
reads the gauge: the output relay is relocated onto the generator's node, its last subscription is
deleted, and the source owner fills its relay memory so that it refuses every relay grant, which
keeps any delivery the generator's node still sends charged. Zero is then the steady state unless
something retains relay memory, and the scenario also requires the retained generation's charge in
`state_read_cache`. The 30-second waits and the five complete observations of each round are
unchanged.

Collection and route-flush timing assertions measure observed row arrival from the HTTP publication
instant. Concurrent branch posts retain one publication origin, and every expected row must match
its tenant and sequence and arrive after the declared lower bound, inside the unchanged delivery
budget. Starting a new silence window after an HTTP response would reject a valid row whose
collection or flush deadline elapsed while the harness awaited that response. The production
owner's timing checks retain the independent branch-deadline assertions.

The WASM reset outline `Reset drains accepted buffered output once and cancels the old branch
timer` starts a bounded receipt observation before posting the fresh alpha input. The observer
owns the subscription session on a runtime worker and confirms its first poll before publication.
It timestamps the next received row there, preserving that instant until the assertion reads the
result, then returns the session for the duplicate-output check. The row must match the alpha key,
tenant and release marker and arrive at least three seconds after that publication. Both cluster
sizes retain the ordered pre-reset alpha/beta drain, the five-second receive budget after the
publishing request returns and the three-second guest timer. The read remains active across that
request; the assertion bounds its completion and teardown cancels it if a step fails. A queued
early row cannot acquire a later timestamp merely because the runner resumes late. This
observation measures client receipt, not server emission, and cannot
remove scheduling delay on the observer's own runtime worker. Ordinary raw sessions still read
only while a caller waits; their later dequeues do not supply independent receipt evidence.

Paused-clock harness controls cover a valid receipt learned late, an early receipt learned late,
a row predating publication, cancellation and a reader panic. The WASM host's isolated-store test
separately drops an instance with an armed request, starts its fresh lifetime and checks exact
domain-time deadlines for it and an untouched sibling. These tests and the public reset outline
provide complementary receipt and instance-ownership evidence; they add no synchronization claim.

The MQTT changed-address restart scenario waits for the post-restart marker within its bounded
delivery window. Its persistent QoS 1 source may replay the pre-restart marker before that row;
broker receipt does not establish that its source acknowledgement reached the broker before the
node stopped. The OTEL metric scenario observes one successful export from each concrete emitter
owner before checking Collector output. HTTP intake acceptance does not establish emitter export,
and the per-owner counters retain exact progress assertions for both HTTP and gRPC.

The OTEL Collector content wait keeps its ten-second bound, from the step's start to the text's
appearance in the followed transcript, counting output printed before the step began. Each look
searches only what arrived since the previous look, plus the bytes an occurrence could span across
it, and every append wakes the wait, so the text is seen as soon as the follower reads it. No look
re-reads the whole output, so a record delivered within the bound cannot be missed by a long read
that began before the delivery and ended after the bound. If the bound expires, or the follower
stops first, the failure reports whether the text arrived after the wait ended, how far the follower
has read each output and how it stopped, the latest 200 Collector lines that name the scenario's
test identifier, and the last command output. A collector timeout alone does not establish that the
emitter lost a record or failed to export it; retained diagnostics and a new attempt remain separate
evidence.

The log/trace DNS recovery outline gives both emitters a 250ms initial retry delay and a one-second
ceiling. That ceiling keeps the next automatic attempt inside the ten-second Collector observation
after the unavailable-client and DNS phases have lasted several attempts. The outline retains its
five-second RPC timeout, both exact signal payloads, failure and DNS-question observations, and
one- and three-node examples. Production retry policies continue to use their declared ceiling.

A scenario step can also read status inside a window of its own, passing that window as the phase
deadline. Where the window bounds how long something is watched rather than how long one read may
take, every read keeps a full request timeout instead.

The harness session client keeps domain-clock replies and frames in arrival order. When a detach
reply ends an attachment, it retires that domain's unread frames from the pending observation
queue while preserving them in the ordered log. Assertions about events after detachment therefore
start at the acknowledged boundary, including when ticks arrived while the client awaited the reply.

## Node Tasks And Readiness

An in-process node is the server application started as one Tokio task. That task is the node's
root, not the whole node: the application spawns the tasks that run its domains, its consensus, its
gossip and its interconnect, and those tasks hold its databases and listeners until the
application's own shutdown stops them. The harness holds the root task through a single owner,
which is in one of four states:

- **Not started.** No task has been spawned.
- **Running.** The task is live, and the owner holds its join handle.
- **Left stopping.** The task outlived the wait its owner could give it and was handed to a keeper,
  which waits for the node to end.
- **Terminal.** The task ended, and the owner holds how: a **clean application exit**, an
  **application error** carrying the node's own report, a **panic**, or a **cancellation**, which
  is how a task ends when the runtime that ran it is dropped.

Running and ended are read from the owner's state and never inferred from whether an error message
exists. Inspecting a task that has finished joins it and keeps its terminal outcome, so a diagnostic
never takes the handle a later stop needs. A wait receives its deadline and reports that the task
was never started, had already ended, ended within the deadline, was still running when the deadline
passed, or had already been left stopping.

The owner never aborts the task. An abort would end only the root future and leave the tasks the
node spawned running with its databases, its listeners and its connections to its peers, owned by
nothing and joined by nothing: a second launch into the same directory would find the database
locked, and the orphaned node would keep working beside every scenario that followed it. A node
ends by its own shutdown instead. One that is still running when its owner stops waiting is
**left stopping**: the owner hands the task to a keeper that waits for it to end, and the keeper
holds what the node still uses, the cluster's storage, the node's seven ports, its consensus fault
registration and the cluster's place in the live-cluster registry, and gives them back only then.
A node whose handle is dropped while it runs is asked to stop and left stopping the same way. A
keeper prints the end it waited for:

```text
node '<node>' ended <elapsed> after it was left stopping: <terminal outcome>
```

A node that never ends keeps what it holds until the process drops the runtime.

A **readiness probe** is one status request, and it reports one of three outcomes:

- **Ready.** The node returned a successful command result, or one saying it is not the leader.
  Either way it answered an authenticated command.
- **Request failed.** The status request ended with its typed failure, including the operation a
  passed deadline interrupted.
- **Non-ready response.** The node answered with an unsuccessful result, kept with its kind,
  message, and diagnostics.

The readiness wait probes every 200 milliseconds within the attempt's deadline and remembers the
most recent outcome. The owner inspects the task before each probe and after it. The wait succeeds
when a probe is ready and the task still runs. It fails with **node task terminated before
readiness** when the task has ended, and with **startup deadline expired** when the deadline passed
while the task was running. Either failure names everything needed to tell resource pressure from a
deterministic failure:

```text
node '<node>' startup attempt <n> failed after <elapsed>: <failure>; task state: <state>; last readiness outcome: <outcome>
```

The task state is `not started`, `running`, or `terminal` with the terminal outcome, and the last
readiness outcome is the most recent probe or `no readiness probe completed`.

## Node Startup

Starting one node has one advertised worst case, 84 seconds, whatever happens inside it. The budget
starts before the first launch, and every readiness probe, every cleanup after a failed attempt, and
every pause between attempts receives only what is left of it.

```text
           one startup budget: 84 s, fixed before the first launch
 |<------------------------------------------------------------------->|
 | readiness | cleanup to task end | pause | readiness | cleanup | ...   |
 |  <= 36 s  | within remainder   |  1 s  |  <= 36 s  | within remainder |
```

The budget is derived from two readiness attempts with cleanup and backoff allowances. The cleanup
allowance funds that total; the actual wait for a node's ending shares the remaining absolute
deadline. A cleanup that takes longer leaves a later readiness attempt less time. A third launch
is allowed because an attempt that fails at once, such as a listener bind failure, leaves most of
the budget unspent. Every launch receives only what the earlier ones left.

After an attempt fails, the harness requests its stop, waits for the exact task it launched,
classifies the failure using both observations, and records the attempt.

| Failure | Classification |
| --- | --- |
| The harness could not launch the node: an address it could not parse, the test certificate authority, the node's database directory, or a setting the node refuses | Terminal: the harness's own configuration, which the next launch repeats |
| Readiness did not arrive before the attempt's deadline, and the requested stop ended cleanly | Transient, selecting all listeners: the resource pressure the budget exists for |
| The node's task ended with an application error binding its gRPC, HTTP, HTTPS, observability, or web console listen address, or starting the interconnect listener | Transient, selecting that listener family: another process took the port first, and its fresh addresses resolve it |
| The task ended with any other application error, a panic, a cancellation, or a clean exit before cleanup requested stop | Terminal: the node decided it from its configuration, its stored state, or its own code |

Cleanup can reveal an initialization failure while a status request was still establishing its
session. That joined application error, panic or cancellation determines the retry decision, with
its typed cause retained in the cleanup outcome. A late bind error selects only its listener
family. A successful stop of a task whose readiness timed out retains that timeout as the retry
cause.

Cleanup receives the remaining node startup budget, and the product's shutdown deadline cannot
restart it. Once the task ends, the node gives back its consensus fault registration. A transient
failure may then move the selected listeners to fresh ports and pause one second before another
launch, provided the same budget still has time. A bind failure preserves unrelated addresses; a
readiness timeout selects all seven ports.

A node still stopping when that budget expires keeps its task, its shutdown coordinator, its fault
registration, its database and its listeners until its own shutdown has finished. The startup
reports exhaustion, and cluster teardown waits for that node or leaves it stopping under its
keeper. It never launches a replacement into resources a running predecessor still holds.

The startup ends in one of four ways, each reported with the node, its budget, the time spent, and
the record of every attempt:

- the last attempt failed for a terminal reason;
- every launch the attempt limit allows was spent;
- the budget ran out during readiness, cleanup, or the preparation for another launch;
- fresh ports for the next launch could not be allocated.

Each attempt's record names its number out of three, its classification, how much of the budget had
been spent once it was cleaned up, its typed failure, and its cleanup: nothing was launched, the
task stopped with its terminal outcome, or the node was still stopping at the cleanup deadline. A
cleanup that the spent budget left no time for records the node as still stopping too. Budget
exhaustion preserves that pending task even when the launch limit has not been reached.

The harness prints every transition to standard error as it happens, so a run heading for exhaustion
is legible while it is still going:

```text
node '<node>' startup: ready after <elapsed> on attempt <n>/3; <remaining> of its 84s budget left
node '<node>' startup attempt <n>/3 [<transient or terminal>] failed <elapsed> into the budget: <failure>; cleanup: <cleanup>; <remaining> of its 84s budget left
```

The budget bounds what the harness waits for, not what the operating system schedules. A runner that
stops running the harness's own tasks for longer than an attempt delays the check that ends it, and
the measured startups above include such a runner.

### Node Settings

Each launch builds the node's identity and its settings the way `nervix-server` builds them from
its command line: the scenario's cluster configuration becomes the owners' class-typed settings
groups, through the same validation. A scenario names any other setting for one node with
`Given <node> starts with <setting> = <value>`, where the setting is the option's long name without
its leading dashes and the value is written as the command line reads it, for example
`Given node-2 starts with drain-timeout = 1m`. The harness applies such settings over the
cluster's own from that node's next launch on, in the order the scenario named them, by rendering
the node's settings as options, appending the named ones and reading the result with the server's
own options groups, so a later setting of the same name replaces an earlier one and no setting
needs a step of its own.

A setting the node refuses fails its launch before the node is spawned, with the refusal's whole
report, and that failure is terminal. A scenario whose subject is the refusal starts the node with
`When node "<node>" attempts to start`, which records the outcome instead of failing the step, and
asserts it with `Then node "<node>" refused to start because "<text>"`. A node the startup preflight
refuses ends the same way, as an application error of its task. The settings of the current launch
also decide how long a scenario-driven stop of that node may take, so a scenario that names a longer
shutdown or drain timeout lengthens that bound with it.

Test clusters start with a 166 ms Raft heartbeat, so the election timeouts consensus derives from
it, 996 ms to 1,992 ms, stay just inside the one and two seconds scenarios size their leader waits
for. A scenario changes the heartbeat for the whole cluster with
`Given raft heartbeat interval is configured as "<duration>"` before the cluster starts. Neither a
test cluster nor a server process names a restore-staging quota of its own, so each node derives
its quota from the volume that holds its database and passes the startup preflight on a host whose
volume is smaller than 128 GiB.

A node refused as it joins may already have answered its readiness probe when it stops, so its
refusal is the ending of its task rather than a failed launch. A scenario whose subject is a join
starts the node, adding it as a new member when the cluster does not have it yet, with
`When node "<node>" attempts to join`. The step waits until the node is a voter of the membership
or its task has ended, and records which. The scenario asserts the ending with
`Then node "<node>" refused to join because "<text>"`, against the report the node's task ended
with, or the admission with `Then node "<node>" joined the cluster`.

### Cluster Construction

A cluster is built one node at a time, bootstrap node first. The construction has one deadline, 84
seconds times the node count, and each node's startup budget is nested inside it, so no node can
outlive the construction. Once a node is ready, the harness asks the other nodes for the current
leader and that leader's applied index within one 40-second lookup, and then waits up to 40 seconds
for the new node to apply as far.

Once every node has started, construction waits for a current leader on the bootstrap node. A
cluster of more than one node must also converge, and each check is its own 40-second status wait: a
current leader on every node, the full voter set, a consistent leader on all nodes, and `connected`
interconnect status from every node to every peer, which is six waits in a three-node cluster. A
construction therefore waits up to 84 seconds per node for startups and then up to 40 seconds for
each status wait that follows.

The start step logs `cluster start requested:` with the node count, domain, and test id. When
construction fails, the harness stops the nodes it had started the way scenario cleanup does, all of
them together under the one 60-second cleanup budget, because a construction that failed has no
product shutdown deadline left to observe. It logs `cluster start failed:` with the error, followed
by the cleanup record and every node that was still stopping or panicked when there was one, and
fails the step.

The other ways a scenario starts nodes reuse the same budgets. Restarting a whole cluster stops its
nodes and then starts every one of them at once under a fresh construction deadline, each with its
own nested startup budget, as an operator's restart of the whole cluster does. Once every node is
ready, it waits for each to catch up with the leader and then for the same convergence checks.
Starting the nodes one after another would hold the last voter back by every earlier node's startup.
On a loaded host that can outlast the ten-second grace a newly started leader gives voters it has
not yet observed before it fails their work over, as [Control Plane](./control-plane.md) describes.
Only the restarted-voter observation fixture keeps the bootstrap node first, because its scenario
observes scheduling while that node leads. Adding a node, or starting one a
scenario stopped, spends one 84-second node startup and the catch-up that follows it; adding a node
also repeats the convergence checks. A node started while its consensus connectivity is blocked
spends the node startup without the catch-up. A scenario whose subject is a node that cannot apply
what the leader committed uses one absolute 36-second budget for launch, readiness, failed-attempt
cleanup and any retry pause. It may relaunch only after a typed `AddrInUse` error binding an early
server listener, before runtime initialization could consume an armed fault. Every other failure,
including a readiness timeout, remains terminal for this startup. The same three-launch limit
applies, and the harness never restarts a node after it becomes ready. The interrupted snapshot
installation scenario also occupies the first launch's observability port and then observes the
armed interruption on the successful launch.

## Scenario Phases

A scenario publishes the phase it is entering, so a reader sees the work in flight rather than the
last work that finished.

| Phase | Published when |
| --- | --- |
| `queued` | The suite has taken the scenario up, and it waits for the permits that let it run beside the scenarios already running |
| `started` | Its steps begin |
| `body complete` | Its steps have ended and their result is known |
| `teardown started` | Cleanup begins by releasing the faults the scenario injected |
| `teardown diagnostics` | Bounded status snapshots and the scenario's context are recorded |
| `stopping` | Its fixtures and cluster nodes are stopped |
| `finished` | The scenario and its cleanup have both ended |

```text
queued -> started -> body complete -> teardown started -> teardown diagnostics -> stopping -> finished
```

`finished` is published only once cleanup has completed. A scenario holding any other phase has not
finished, and a scenario whose log ends at a phase marker is still inside that phase.

Browser assertions that wait for an acknowledged subscription tab to disappear poll for up to 10
seconds. This bounds the server reply and browser update together; on expiry, the step reports the
tab text that remains visible.

A continuous browser assertion begins after its readiness predicate holds. The console reconnect
scenario first waits for the restored clock's `PACED` state, then observes the complete stability
period and advancing ticks. Leader confirmation and the clock's attach reply are separate events;
the bounded selector predicate establishes clock readiness before the stability observation starts.

A visual creation fixture whose scenario has already provisioned its domain waits for that exact
domain's active menu item before it opens a domain-scoped form or types a domain-scoped statement,
such as `BEGIN`. The visual creation and connector presentation features follow this rule, as does
the console REPL case that opens a resource form inside a transaction. Every create form except the
domain and user forms is domain-scoped. Page load and leader confirmation do not establish that the
initial domain publication has arrived: `CONNECTED` follows the server's leadership event, a frame
independent of the domain list. A form opened while the domain list is loading captures no domain.
The later sidebar selection does not replace that retained scope, so its controls correctly report
a missing prerequisite without issuing a choice lookup, and its local validation refuses the
submission for want of a domain. A statement typed in that interval is sent without a domain, and
the server refuses it when the statement needs one. The fixtures keep their original ten-second choice probes and their
sensitivity, domain-change, transaction, leader-following and repartitioning assertions. Fixtures
that provision no domain keep their no-domain refusals and cluster-scoped domain forms unchanged.

The delayed-initial-domain feature exercises this ordering on one and three nodes through the
ordinary browser flow. A page-local gate holds actual session frames until the scenario releases
them, with at most 64 frames and 1 MiB retained. It preserves the bytes and delivery order and fails
on overflow, a non-binary frame or a connection closed before release. After release, the fixture
observes the exact selected domain before opening a form, then requires the form's captured scope
and a real choice from each scoped form family: an ingestor's endpoint, a processor's input, a
branch's schema, a subscription's relay, a codec's internal schema, an endpoint's VHOST and a hash
map's resource. The gate adds no sleep, retry or suite admission rule. Selector-existence failures
include the sidebar domain, captured form scope and neutral or failed choice states, without
recording draft input values.

Every console page starts with a connection recorder, a script the harness adds before the page's
own scripts run. It subclasses the page's `WebSocket` constructor and notes, for each connection the
console constructs, the console it reached, the call its path names (the session, a backup download
or a restore stream), when the page constructed it, saw it open, and let go of it by closing it or
seeing it close, and the first 16 and the last 64 binary frames of at most 16 KiB it received, with
when the page received each, so a busy page keeps a bounded record. The record holds each
connection's origin and path, never its query, which carries the credentials. One counter that only
the page's event loop advances orders those moments, so a step reads the order the page acted in
however late it reads and however loaded the machine is. The page receives a frame before it can
act on it, so a frame comes before every connection the page makes because of it. The console lets go of its session connection before it constructs the next one, so
`the web console never held two session connections at once` tells a reconnect of the console's one
session from a second session without counting the leader notices, which every reconnect repeats.

The harness reads from those frames which node the nodes reported as the leader on the page's own
connections. The leader opens every console session it serves by reporting that it leads; any other
node ends a console session with a redirect naming the leader's web console, and answers a command,
a backup download or a restore it cannot serve because it does not lead with such a redirect too.
A redirect that names no leader reports none. Every report comes first or last on its connection,
because the console lets a session go as soon as a redirect names another leader, so the frames the
recorder keeps hold every report. A frame larger than the recorder's bound is a domain snapshot or an
archive chunk, which reports no leader, and a kept frame that does not decode as a frame of its
connection's call fails the step that reads it. `the web console's session reached
node ... until it was redirected to node ..., and then only nodes reported to it as the leader`
checks that the session started where the page was opened, that its first connection anywhere else
reached the node it names as the redirect's, and that from then on every session connection reached
a node that some report had named the leader before the page made that connection. `the web
console's backup downloads and restores reached only nodes reported to it as the leader` holds every
download and restore stream to the same rule and requires at least one of each. A connection made
again to a former leader, such as a restore streamed again to the node it was streaming to when
leadership moved, still reached a node reported as the leader, and the redirect that node answers
with names the next one. A leadership move after the redirect therefore only adds the new leader to
the reports, and both checks hold however often leadership moves, while a connection to the page's
origin or to a node no report named still fails them. `the web console loses its session
connection` closes the newest session connection the page holds, as a connection lost under the
console closes, and notes that moment; `the web console's session reconnected where it lost its
connection` then checks that a session connection the page constructed after that moment opened on
the console of the connection it lost, because the console connects its session again where its
redirects took it. It reads the record once, after a step that waits for something only the new
connection can deliver.

The **active-scenario registry** holds every scenario that has started and not yet ended. A scenario
registers in its before hook, before it acquires its permits, so a scenario that never gets them is
visible while it waits. Its entry holds:

- its identity: the feature, the scenario name, and the line where it begins, which tells apart the
  example rows of an outline that share one name;
- its attempt, counted from one, which is exact because Cucumber takes up a retry only after the run
  before it has ended;
- its phase, when it entered that phase, and when it started.

The entry is keyed by a registration the scenario's world owns rather than by anything the feature
file supplies, and it leaves the registry when that world is dropped, whether the scenario passed,
failed, or panicked. The suite's report prints scenarios in order and can keep a finished scenario's
world until earlier scenarios have reported, so a `finished` entry can still be present when the
registry is read.

Entering a phase also appends a marker to the scenario log:

```text
scenario <phase>: feature="<feature>" scenario="<scenario>" line=<line> age=<age> <detail>
```

`body complete` carries `result=` with the body's result, which is passed, skipped, before hook
failed, or step failed with its error. `finished` carries `body=` with the same result and
`teardown=` with the cleanup record. Cleanup reports itself separately: a node left stopping and a
node that panicked while stopping are recorded beside the scenario's result and never turn a passing
body into a failure.

## Scenario Teardown

The after hook runs every scenario's cleanup through the phases above.

```text
body complete
     |
teardown started      cancel and join the background CLI backup;
     |                release paused health responses, domain-clock progress pauses, and
     |                command pauses; stop durable catch-up writers and domain write load
     |                sessions, and clear consensus commit delays
teardown diagnostics  every node's status at once within 10 s; the scenario's context
     |
stopping              abort CLI output readers and kill their child processes;
     |                drop HTTP load, held uploads, server processes and process clusters,
     |                and observers;
     |                stop HTTP and gRPC receivers within 6 s; close the browser and the
     |                session;
     |                stop the cluster within 60 s;
     |                release proxies, silent peers, permits, and fixture ports
finished
```

### Teardown Diagnostics

The harness asks every node for its status at once, under one 10-second budget for the whole
cluster, so a node that never replies delays cleanup by at most that long and cannot hide another
node's status. Each node's outcome goes to the scenario log on its own line: its status text, the
operation its request was still in when the budget passed, or its typed failure.

```text
scenario teardown <identity>: node=<node> status=<status text>
scenario teardown <identity>: node=<node> status_timeout=<operation> pending after 10s
scenario teardown <identity>: node=<node> status_error=<failure>
```

A `scenario context:` line follows with the scenario's domain and test id, its last command error
and output, its last server error, and its last subscription and broker payloads. Diagnostics are
best effort: whatever they observe, cleanup continues into `stopping`. Steps that explain their own
failures can collect the same snapshots.

### Cluster Teardown

A cluster's nodes stop within one deadline of 60 seconds for all of them together.

1. Every node is asked to stop before any node is awaited.
2. Every node's task is awaited concurrently under the one deadline. Each wait borrows its task from
   the node's owner, so a node still stopping when the deadline passes is still owned.
3. Once the waits are over, each node whose task ended, whatever ended it, gives back what it holds:
   its seven port leases and its consensus fault registration.
4. Each node still stopping is left to end itself. Its task goes to a keeper together with what the
   node still uses, and the keeper gives that back once the task has ended.

A cluster of three wedged nodes therefore costs one budget rather than three, and no path ends a
node or takes anything from under one that still runs. The cleanup record reads
`stopped <n> of <n> node(s) in <elapsed> of a 60s budget, <n> still stopping`. Dropping the cluster
afterwards removes the temporary storage its nodes wrote as soon as no node left stopping still
shares it. Two more records appear only when they apply:

```text
scenario cleanup unfinished: node "<node>" was still stopping at the cleanup deadline, in shutdown phase <phase>, <the node's own deadline>, and keeps its storage and ports until it ends
scenario live during unfinished cleanup: <identity> attempt=<n> phase=<phase> phase_age=<age> age=<age>
scenario teardown failed: <node record ending in a panic>
```

A node left stopping is read through its shutdown coordinator before it goes to its keeper, as
[Scenario-Driven Stops](#scenario-driven-stops) describes, so the record says which of the node's
own shutdown phases the cleanup deadline found it in.

An unfinished cleanup is usually cleanup that ran beside work heavy enough to starve it, so the
harness names every scenario live at that moment, and the keeper's own line later says when the
node ended. A node whose task panicked while stopping has no other witness than its record. In the
qualification runs that characterized them, cleanups that ran out their budget were nodes of WASM
scenarios whose setup had already failed with `not-a-leader`, and those scenarios passed when
retried.

A step that replaces a scenario's cluster with a fresh one stops the old cluster under the same
budget and fails when a node of it was left stopping: that node still runs beside the cluster that
would replace it, so the replacement has not happened.

The consensus fault registration keeps an observer of the node's consensus state, as does every
observer a step took from it, so the node's consensus database stays open after the node's task has
ended until the last of those observers is dropped. That drop closes the database synchronously, on
the task that makes it: the close synchronizes the journal and waits until none of the database's
background workers is left. Cleanup and [scenario-driven stops](#scenario-driven-stops) give the
registration back on the runner task, so every scenario and the suite watchdog wait for that close,
while a keeper gives it back on a runtime worker. The close never waits for a worker whose flush or
compaction failed; see [Terminal Teardown](./shutdown.md#terminal-teardown).

### What Cleanup Releases

The resource-source-discovery fixture uses the existing node-scoped command-pause registry. Once
reconciliation finds a usable source for an absent local copy, the fixture hides the source view
and records that it was consumed. It leaves the node, transport and all other scenarios running.
The public regression waits for that observation and the pending replica, explicitly restores
source visibility, and verifies upload completion and a lookup from the replicated file. Its
observation wait is bounded at 120 seconds; its public pending-state probe keeps a 30-second bound.
The source view changes only in a testing build. Explicit release removes the control, and the
ordinary scenario cleanup releases and clears it with the other command pauses after failure.

The resource upload-publication fixture retains its selected node's refusal in a watch channel.
It refuses one publication after the local archive is installed, then holds that upload's reply
until reconciliation takes the applying upload up. The client therefore cannot supply the archive
again before the recovery observation. The retained `TakenUp` state permits the scenario's positive
assertion and is disposed with that world's injector. Active, order and stressed selections include
both cluster sizes of this existing public recovery outline.

Harness state goes back only after the tasks that used it have ended, so the next scenario never
finds a port, a fault, or a proxy taken. What a node left stopping still uses stays with its keeper
until that node has ended.

- Every asynchronous CLI command, including one-shot inspection, completion, clock, backup and
  restore commands, starts with `kill_on_drop(true)`. Cancelling its step or expiring its timeout
  therefore kills the child rather than leaving its redirects and reconnects running. The
  background-backup step records its child's process id. Teardown aborts and joins its output
  task before releasing command pauses or stopping the cluster, so the child is dropped before
  the cluster's listener ports can return to the pool. The public one- and three-node outline
  `Cancelling a paused background backup ends its CLI process` holds the cut, cancels that same
  task and requires its exact process to disappear within a 30-second observation budget. A
  surviving child fails the assertion and is killed to contain the failed regression. After
  releasing the cut, the outline recovers that same durable backup by its caller-chosen execution
  reference and collects its archive before stopping the cluster: ending the client does not
  cancel the server's admitted backup.
- CLI output readers, the terminal task of an interactive CLI among them, are aborted and their
  `kill_on_drop` child processes are dropped before node teardown, which also removes the REPL's
  working directory. Background HTTP load, held uploads, server processes and real-process
  clusters are also dropped first. Dropping any server child kills it and returns its ports.
- Broker and syslog observers, HTTP and gRPC receivers, the browser, and the session are closed
  before the cluster stops.
- The scenario-local WebSocket lifecycle peer records Close frames and reconnect instants through
  a bounded channel. Cleanup aborts and joins its listener task before returning its reserved port;
  socket ownership stays in that task, so the same cancellation releases its active connection.
  The declaration peer also records an opening client ending an incomplete upgrade. An HTTP 101
  written by the peer does not prove the client completed its opening: the peer confirms that
  connection with a Ping and its matching Pong before consuming the pending payload gate. A
  transport ending before that confirmation records an opening cancellation and leaves the gate
  for the next connection. The BUFFER/codec-change outlines end a partial upgrade and await that
  cancellation event before starting their one- or three-node cluster. The shared peer's ordinary
  harness tests also end a completed upgrade before its probe, then require a replacement
  connection to receive both gated payloads. After confirmation, unexpected stream-read
  errors still fail the fixture, qualified by a separate transport test; other handshake
  errors remain fatal as well. A received Close frame supplies the graceful-close
  observation. Its queued reply can find the connection already closed or TCP reset or
  broken: the initiator can end TCP after sending Close and before reading that reply.
  The shared close-reply owner treats those endings as completion. Focused harness tests
  parse a real Close frame before injecting reply-write failures; unexpected reply
  failures retain their typed causes and fail the fixture and its After hook.
- The TCP proxies and silent interconnect peers a scenario placed in front of its nodes, and the
  endpoints it stood in front of a shared dependency, are released once those nodes have ended.
- The ZeroMQ and syslog ports drawn for the scenario return to the pool after their nodes and
  observers have ended, the sockets a scenario bound to take one of them close first, and the holds
  on the ingest addresses close with their return. The scenario publishes `finished` and then
  releases its feature permit and run slot, so admission includes all teardown work.

### Scenario-Driven Stops

A step that stops a node, stops every node, or restarts the cluster is part of the scenario's
subject, so it runs under the product's deadlines with a harness watchdog around them rather than
under the cleanup budget. The harness requests the stop and waits for the node's task for the
longest of five minutes, the node's configured shutdown timeout, and its configured drain phases:
the application drain when graceful drain is enabled plus the runtime's branch stop timeout for the
configured domain drain. A scenario that configures a longer product deadline raises the watchdog
with it, so the harness never cuts a product deadline short.
Node-specific stop steps resolve a saved scenario placeholder before selecting the node, including
the graceful stop used to exercise ownership handoff during drain.

Once the node's task has ended, the stop gives back the node's consensus fault registration, which
closes its consensus database as [Cluster Teardown](#cluster-teardown) describes, and a stop that
ended cleanly then checks that the node released its node and consensus database locks. The step
fails when the node's task panicked, when the watchdog passed with the node still stopping, or when
a lock outlived the stop. A node still stopping keeps its task, its ports and its fault
registration, and the scenario's cleanup waits for it again or leaves it stopping.
Stopping every node requests every stop first and then waits for each node in turn. A scenario can
also begin stopping a node without waiting, to observe how the cluster reacts while the node is on
its way down; its cleanup waits for it later. It can hold such a node at the start of its graceful
drain, once the node has advertised that it is terminating and closed its intake but before it asks
for its work to move, so that peers meet a terminating node that is still live; releasing the hold
lets the stop go on, and scenario teardown releases it with the other command pauses.

The harness prints every such stop to standard error when its wait ends, as it prints a startup:

```text
node '<node>' stop: ended <elapsed> after its stop request, inside its <watchdog> watchdog: <terminal outcome>; shutdown phases: stop admission <outcome>, drain support <outcome>, terminal teardown <outcome>
node '<node>' stop: still stopping <elapsed> after its stop request, at the end of its <watchdog> watchdog, in shutdown phase <phase>, <the node's own deadline>
```

The elapsed time runs from the first stop request the harness made of that launch of the node to
the moment its task ended. The task records its own ending, so the stops of a whole cluster, which
are requested together and awaited in turn, each report how long that node took rather than how
long the harness took to reach it. A stop is printed by the wait that observed the ending, and only
by that one.

The second line is also the failure of the step. A node that outlived its watchdog is read through
its shutdown coordinator, which changes nothing in the node: the phase is the product's
`StopRequested`, `DrainSupport` or `TerminalTeardown`, and the node's own deadline reads
`<remaining> before its own shutdown deadline` or `past its own shutdown deadline`. An in-process
node has no deadline supervision to end it, so a node read in `TerminalTeardown` past its own
deadline is running the final stops the product lets run to completion, while one read in
`DrainSupport` is still inside a drain its scenario configured. A node that had already been left
stopping under a keeper has handed its coordinator over, and its record says so instead.

## Ports

Every port the harness binds is drawn from one pool shared by all scenarios in the process. A draw
binds port zero on the loopback interface, reads the port the operating system chose, closes it, and
reserves it in the pool. The operating system would let a second scenario bind a closed port, so the
reservation is the only thing that keeps two scenarios apart. It cannot see sibling worktrees
running the same suite, or any other socket on the runner, which is why a node startup and a server
process startup both relaunch a lost bind on fresh ports.

The constrained restart used for paced catch-up and an armed snapshot-installation fault also
recovers a lost early listener bind. It requires the listener's typed application error and its
`AddrInUse` I/O cause, joins that exact failed task, and moves only the affected listener family.
Its launches, cleanup and one-second retry pauses share the original 36-second budget; it never
uses the general startup's readiness-timeout retry. The early gRPC, HTTP, HTTPS, observability and
web console bindings precede runtime initialization. Interconnect setup happens after stored
runtime changes are applied, so its failure cannot establish that an armed fault is untouched
and remains terminal on this constrained path. An attempt still stopping retains its task and
ports and admits no replacement.

| Holder | Ports | Returned |
| --- | --- | --- |
| In-process node | 7: gRPC, gRPC over HTTPS, HTTP, HTTPS, observability, web console, and interconnect | After its task has ended: at cluster cleanup, when a scenario stops every node, and when a failed startup attempt moves to fresh ports |
| Scenario fixtures | 4: ZeroMQ ingest and emit, syslog ingest and emit, and 1 more for each port an ingest address moved off | At the end of cleanup, when the holds on the ingest addresses close |
| HTTP receiver | 1 per receiver, drawn with the scenario fixtures | At the end of cleanup, with the scenario fixtures |
| HTTP proxy | 1 per proxy, drawn with the scenario fixtures | At the end of cleanup, with the scenario fixtures |
| gRPC receiver | 1 per receiver, drawn with the scenario fixtures | At the end of cleanup, with the scenario fixtures |
| DNS authority | 1 UDP port per cluster addressed by names | When the cluster is dropped at the end of cleanup, after its nodes have stopped |
| TCP forwarders | 1 port, shared by every forwarder a scenario stands in front of a dependency | At the end of cleanup, after the cluster has stopped |
| Server process | 6, and 1 more for each port a relaunch after a lost bind moved it off | When the process is dropped, the ports it gave up included |
| A node moved to a new interconnect address | 1 new interconnect port | The port it gave up stays reserved for the rest of the run |

A port returns only once nothing can still dial it. Every scenario names its nodes `node-1`,
`node-2`, and `node-3`, so a node of an unrelated scenario that binds a port a peer still dials
passes the peer-identity check and is caught only when its introduction fails to verify, which takes
long enough to fail a scenario. Keeping every port reserved is not an option either: the ports a
whole run draws exceed the range the operating system draws from, because Linux hands a port-zero
bind an odd port from the lower half of its ephemeral range, some seven thousand ports.

A draw is synchronous and runs on the runner task. A draw that looped until the operating system
offered a port the pool did not hold would stall every scenario and the suite watchdog with it, so a
draw that lands on reserved ports 65,536 times in a row fails with the pool exhausted, naming how
many ports the process holds, and gives back the ports it had already reserved. A pool with one free
port among seven thousand still yields it within that many draws almost every time, and a pool with
none ends the draw in about a second.

### Addresses A Node Binds

A scenario draws its ZeroMQ ingest address, `{{zeromq_ingest_addr}}`, and its syslog ingest address,
`{{syslog_ingest_addr}}`, with its identity, but a node binds them only once the ingestor that uses
them starts after `START`, seconds later: a ZeroMQ PULL socket whose client declares
`'bind' = 'true'`, and a syslog listener, which every node binds on its own loopback address. Until
then the port is free at the operating system, and a socket that took it would fail every bind the
ingestor retries, while the ingestor's reconnect delay doubles from 250 milliseconds to 30 seconds
and soon outgrows the step that waits for it. Two measures keep these addresses bindable.

- **The hold.** From the draw until cleanup, a TCP socket bound to the port on the wildcard address
  with `SO_REUSEADDR`, which never listens, holds it. Linux picks no held port for a socket bound to
  port zero or for an outgoing connection, and refuses an explicit bind of it without
  `SO_REUSEADDR`, so neither the harness's own listeners on port zero nor any client socket on the
  host can take it. A node's listener sets `SO_REUSEADDR`, as every Tokio listener does, and binds
  beside the hold on its loopback address, after a restart as well. UDP has no hold: a syslog UDP
  listener binds without `SO_REUSEADDR`, and any UDP socket on the port would refuse it.
- **The check at first use.** The first time a scenario names the address, in the statement that
  creates the client its node binds, the harness binds the port once as that node will, TCP with
  `SO_REUSEADDR` on the wildcard address and, for a syslog address, UDP as well, and closes it
  again. A bind refused with `AddrInUse` means another socket took the port, as Docker's proxy does
  for a port it publishes and as any UDP socket can: the address moves to a freshly drawn and held
  port before the statement names it, at most eight ports in all, and `cucumber.log` and the
  standard error record each move with the refusal and what the kernel's socket tables list on the
  taken port, each socket with its table, address, state, user and inode and whether this process
  holds it. Any other refusal fails the scenario. Once named, the address never moves, because a
  model carries it.

A port an address moved off stays reserved with it until cleanup, since the socket that took it may
still hold it.

## Server Processes

Scenarios about signals, exit statuses, process startup, command-line values, and open-file limits
run `nervix-server` as a real child process, because an in-process node cannot show whether the
process boundary delivers a signal to its shutdown coordinator. The fixture gives each process its
own ports, database directory, and interconnect credentials, forms a single-node cluster, and
captures its standard output and error in one log. It removes every `NERVIX_*` variable and
`RUST_LOG` from the child's environment, so the runner's configuration cannot silently reconfigure
the server. The options and environment variables a scenario gives the process are applied after
that removal, written exactly as the scenario states them, so a scenario can hand the node a value
it must refuse; such a scenario starts the process without waiting for readiness and asserts how it
exits. A claim about a
node process dying while its peers keep running, such as what a client producer is told when the
node that executes its ingestor or serves its session is killed or frozen, runs the three-process
cluster and faults one member, because only a real process death closes, or stops answering on,
every connection the node held at once without running any of its shutdown.

Each launch has 120 seconds from its start until the process accepts commands. Reading the
executable, spawning the process and waiting for its readiness share that one deadline, and a
restart starts a deadline of its own. Readiness uses the same probe outcomes as an in-process node,
probing every 100 milliseconds until the deadline, and fails at once with the exit status when the
process exits first. Waiting for an exit is bounded by 120 seconds, and waiting for a log line or an
HTTP admission by 60 seconds. Those failures quote the last 80 lines of the process log. Dropping
the fixture kills the process, so cleanup never waits for a graceful stop and a failed scenario
never leaves a server running. What the process does between a signal and its exit is product
behavior; see [Shutdown And Recovery](./shutdown.md#exit-status).

A whole-cluster restart polls every persisted voter's executable read, launch and readiness
concurrently, then checks convergence through public status endpoints. Each voter retains its own
120-second startup deadline, including its executable read. This lets a started voter probe while
its peers prepare and launch, instead of spending its readiness budget on their serial preparation
before it can recover a quorum. The restart preserves each member's readiness observations and the
cluster's convergence requirement.

**Reading the executable first.** A process does not read its executable when it starts. The kernel
maps the file, and every page the process first touches is read when the process faults on it: one
page at a time, in the order its code happens to run. A debug server executable maps about 450 MB,
the loadable segments of a file of about 2.9 GB whose rest is debug information that is never
mapped, and its first start touches code all over those megabytes. When the file is not in memory
and the storage under it is busy, each of those reads waits for the storage on its own, and on a
rotational disk each one pays a seek. So each launch first reads every range the executable's
loadable segments map, in file order and nothing else, and the process then faults its pages in
from memory. The read runs off the runner task, a 4 MiB chunk at a time, and every chunk receives
only what is left of the launch's deadline. A read that the storage cannot serve in time ends the
startup at that deadline, naming how many of the mapped bytes it had read, and spawns nothing. A
read of an executable that is already in memory costs one copy of its mapped bytes.

The read is what the retained first failure of the stressed lane needed. On 13 October 2026 the
one-node diagnostic process cluster started on a host whose data disk was rotational and busy:
about 78 seconds passed before the server's first log line, and when the 2-minute deadline passed
the server was still initialising its stores and the last probe was still establishing its session.
The next launch of the same executable logged within 0.8 seconds and finished the same
initialisation in 70 ms, then spent 17 seconds in consensus code the first process had not reached.
A later observation on that host counted 26,094 major page faults in a first start, half of its
samples in disk sleep at function entries. Serving the executable through a filesystem that delays
every read that does not continue where the previous one ended by 10 ms reproduces the failure
without the read: the first log line came 111 seconds after the launch, and the deadline passed with
the last probe still establishing its session. The start made 8,667 such reads before that
deadline, and 19,151 for the whole scenario.

**A port another socket took.** The harness draws a process's six ports before the launch reads the
executable, and the process binds them only once it has started. Until then the operating system
lets any other socket bind a drawn port: an outbound connection's ephemeral port, a fixture
container, or a node of a sibling worktree. The retained case is a launch of the instrumented server
on 19 October 2026 that spent 8.4 seconds reading its executable and then found its web console port
taken. A process that cannot bind a listener exits with status 1 after writing its report on
standard error, such as `Error: failed to bind web console listen address`. When the readiness wait
sees the process exit, it reads the outermost context of that report, without the escape sequences
that emphasise it, and classifies it with the same table as a [node startup](#node-startup): a gRPC,
HTTP, HTTPS, observability or web console listener, or the interconnect transport's listener, that
another process bound first is transient and selects that listener alone. Every other ending, and an
exit without a report, stays terminal and fails the startup at once. A transient ending is launched
again on freshly drawn ports for that listener, reading the executable first as every launch does,
within the deadline its startup began with: a relaunch never starts a deadline of its own, and a
startup launches at most three times, as a node startup does. Each relaunch prints the launch that
ended, and a startup that became ready after one prints that too:

```text
server process '<node>' startup launch <n>/3 [transient] exited <elapsed> into its 2m deadline with exit status 1: <reported context>; the next launch moves its <listener> listener to a fresh port; <remaining> of its 2m deadline left
server process '<node>' startup: ready on launch <n>/3 after <elapsed>; <remaining> of its 2m deadline left
```

A startup that still fails names every launch it relaunched before the one that ended it. The ports
a relaunch gives up stay reserved until the fixture is dropped, together with the ports the process
binds then, because a member's peers and a scenario's clients may still dial an address the process
published before a restart. A restart is a startup of its own and relaunches the same way. A joining
member reads the bootstrap member's interconnect address only once the bootstrap member accepts
commands, and the convergence that follows a member's restart asks the new process on the gRPC port
its startup ended on.

**A startup that fails.** A failed startup describes itself before anything kills the process. It
says how reading the executable went, and lists the readiness probes as runs of consecutive probes
that got the same kind of answer, with when each run began and ended: the first eight runs and the
latest eight, with a count of the runs between them. When the deadline passed it adds an observation
of the still-running process and its host, read from procfs: the process's state, CPU time, major
and minor page faults, resident memory and storage reads; its threads grouped by state and the
kernel function each waits in, with their total run-queue wait; and the host's load and pressure
stall averages. That separates what the startup waited for without stopping the process to read its
stacks. A thread in disk sleep waits for storage, in a page fault or a read. A thread asleep in a
futex waits for a lock or a condition variable, or for work, as an idle runtime worker does. A
thread asleep in epoll waits for network or timer events. The run-queue wait is time the threads
were ready to run without a CPU. The description has a fixed bound: at most 16 groups of threads,
each with at most 4 names.

In a diagnostic build, a real-process cluster whose start fails retains what its launched members
leave before they are killed and their stores deleted: every member's deadlock evidence and whole
log, and the failure in `start-failure.txt`, in a directory of its own under `process-clusters/` of
the evidence directory the run keeps. A cluster that started retains its evidence and logs there
when its scenario ends.

## Test Dependencies

The external Chaos directed-rate probe times the sender's connection and the receiver's complete
byte count through helper-local kernel uptime. A successful observation requires the same kernel
boot identity and boot-time offset at both helpers. Namespace identifiers are diagnostic values;
they do not identify the clock coordinate. The ten-millisecond clock precision produces a duration
upper bound. Its lower bound subtracts two ticks from that upper bound because both endpoint
readings can round down. Rate-effect qualification requires the fault's lower bound to reach twice
the baseline's upper bound. Docker control time remains in separate helper-lifetime evidence and the
unchanged ninety-second total probe budget. Missing, malformed or mismatched clocks fail the
observation with raw helper logs retained. Direct Docker controls exercise launch delay, actual
connection and receiver stalls, byte-count failures, failed helper operations and the real rate
effect; they qualify the harness measurement and establish no product recovery outcome.

Brokers, databases, and the other external systems that scenarios use run as containers, started the
first time a scenario needs one and shared by every scenario of the run. The run removes them when
it ends; `NERVIX_TESTCONTAINERS_MODE=reusable`, which `just test-scenarios-reuse` sets, keeps them
for the next run instead. Scenarios still provision the topics, queues, tables, and other entities
they use explicitly.

The RabbitMQ fixture publishes its management HTTP port beside the AMQP endpoints. Its AMQP TLS
listener requires a client certificate the scenario certificate authority issued, so an `amqps`
client that presents none is refused by the broker. The scenario server certificate also carries
the client-authentication usage, and the TLS scenarios mount it with its key as their client
identity. The management listener requests no client certificate. A scenario's
`RabbitMqQueueProbe` observes its explicitly provisioned queue with the fixture credentials:
consumer acknowledgement mode and prefetch, unacknowledged deliveries, or total outstanding
messages. Each observation uses the shared status budget and bounded polling. These are broker
observations. Missing statistics before the broker's first queue sample mean the observation is
pending; they never mean zero messages. The `@rabbitmq_consumer_modes` outlines use them on
one and three nodes to qualify automatic consumption, a four-delivery prefetch window, mixed
graph outcomes and connection-loss recovery. They join the ordinary and diagnostic selections;
the stress selection includes their eight examples in its registered total.

MongoDB write-concern scenarios use a separate single-member replica set, reached through a direct
connection after its primary election completes within the dependency startup budget. A client
requesting two data-bearing members receives the server's unsatisfied write-concern answer beside
the documents it applied. This fault belongs to that client's write concern, so other scenarios
sharing the server keep running. Each scenario provisions its own collection and retains its
server kind with that collection for later assertions; standalone and TLS collections keep their
own server kinds.

MongoDB mutual-TLS resource scenarios use a separate `mongodb-mutual-tls` service and endpoint.
Its TLS connections require a client certificate signed by the fixture CA. Setup and observation
clients use the fixture's private combined identity, while Nervix receives the certificate and key
as separate mounted files. A CA-only setup client must fail its ping within a two-second server
selection budget, while the same endpoint accepts the setup client with its identity. Both
one- and three-node examples assert a document written through
that authenticated transport. The ordinary MongoDB endpoint retains its own authentication mode.
The BSON declaration refusal outline substitutes an actual NUL from a named fixture placeholder;
Plexus string literals remain verbatim, so the fixture does not invent a language escape.

The write-concern outline connects a named public client to the emitter's reported owner before
publishing. That client retains the server's error notices independently of the sessions used by
readiness and `DESCRIBE` probes. After checking all six documents and delaying the observer, the
outline waits for the current emitter status to be `OK`, then reads the retained error notice
within the unchanged twenty-second bound. It checks the insert or upsert operation and numeric
code `100` (`UnsatisfiableWriteConcern`). The notice proves that the failure occurred even after
the live status has cleared. The scenario preserves its retry policy, write concern and exact
document oracle.

All MongoDB test servers set `GLIBC_TUNABLES=glibc.pthread.rseq=1`, enabling glibc's restartable
sequence registration. This avoids the MongoDB/TCMalloc crash on affected Linux kernels when the
image's default disables that registration; the [image maintainers document the setting and kernel
issue](https://github.com/docker-library/mongo/discussions/748). The fixed container environment is
shared by ordinary and diagnostic runs and participates in the fixture's configuration hash.

A scenario that needs a Kafka broker to refuse what the shared broker accepts for every other
scenario starts a broker of its own: librdkafka's in-process mock cluster of one broker, which the
scenario keeps until it ends, across its cluster's restarts. While the scenario asks it to, that
broker refuses produce requests with `NOT_ENOUGH_REPLICAS`, which a producer retries, or offset
commits with `GROUP_AUTHORIZATION_FAILED`, which a consumer does not retry; a scenario can also
refuse only the next commit. The scenario publishes to it, observes its topics and reads a consumer
group's committed offset through clients of its own, so no refusal reaches the shared broker.

The attached- and detached-window Kafka acknowledgement scenarios create their input topic before
`START` and observe the consumer group's membership and the ingestor's readiness before injecting
the output failure. Their bounded subscription assertions therefore exercise replay and
acknowledgement isolation after source initialization, without spending those assertions' budgets
on automatic topic creation and consumer discovery.

The attached-window fixture publishes both rows through one producer and allows a thirty-second
partial-batch wait. Collection ends as soon as its two-message ACK batch is full; the window can
then produce the output whose failure rejects both records. Separate publication steps can exceed
a short collection window, leaving one retained window row waiting for input while the source waits
for that partial batch's ACK. The fixture retains its five-second ACK timeout and exact four-record
replay assertion within eight seconds.

The Pulsar broker announces a `maxMessageSize` of 1 MiB rather than Pulsar's 5 MiB default, the
same limit the MQTT and NATS brokers keep, so one scenario message can exceed each broker's limit.
Its admin API serves the topic-level `maxMessageSize` policy a scenario sets on its own topic; the
step waits up to 60 seconds for the broker to read the policy back, which shows that the policy
applies to the next message.

Harness Redis connections use an explicit ten-second budget for connection setup and each command
response to tolerate scheduling delay under parallel load. The driver's one-second connection and 500ms
response defaults are too short under concurrent scenario load: the three-node JAQ transformation
scenario reached a running ingestor but its publishing client timed out. The budget belongs to the
test client used for individual publishes, bursts, and subscriber-count observations. A request
failure still fails the step; a publish with an ambiguous result is never retried. The existing
bounded subscriber wait only republishes when Redis confirms that the publish reached zero
subscribers. Focused harness regressions delay setup and publish replies by two seconds and verify
that a silent broker still reaches the connection deadline.

After the run, the suite stops its dependencies within 2 minutes. A stop that does not finish is
abandoned and its containers are left to the runner, because waiting without a bound is how a run
that already has its result loses it to the job's own timeout. Dropping the runtime then waits at
most 60 seconds for blocking tasks a scenario left parked in a driver.

A container that fails to stop or to be removed does not keep the others from being torn down. Its
failure keeps a `ContainerTeardownError` report with the Docker or Testcontainers cause beneath it,
and so does a Ryuk cleanup container the suite fails to remove. The teardown stays typed until the
process prints it, on one line: `suite dependency teardown failed:` and the whole chain of every
failure, the failures separated by semicolons. A teardown that failed is not clean, so it fails a
run whose scenarios all passed; after a suite timeout it is printed beside the timeout instead.

## HTTP Receivers

A scenario about a node sending HTTP requests to an external endpoint starts an in-process HTTP
receiver in its place, because only a receiver the harness controls can capture exactly what arrived
and choose exactly how to answer: a status sequence, a delayed, held, or lost response, a stalled
body, or malformed framing. A receiver can serve TLS with a certificate for chosen names and can
require the client certificate it issued, whose files a node mounts as a resource.

The receiver reads and answers HTTP/1.1 itself, byte for byte, which is what lets its script write
framing no valid endpoint would. A TLS receiver can also offer HTTP/2 ahead of HTTP/1.1 in its
handshake. A client that negotiates it is served by hyper's HTTP/2 server: each request is captured
whole, as an HTTP/1.1 request is, and answered with the response its target takes, after that
response's delay. Over HTTP/2 only a complete response has a form, a status, its fields and exactly
its body; a request that takes any other is recorded as a fault and its stream is reset. Every
capture records the HTTP version its client spoke, so a scenario can assert that a request arrived
over HTTP/2.
Its response script can generate exact and over-limit header counts and value sizes separately for
an interim or final block. It timestamps a request when its complete body arrives, before its
scripted response delay begins, so a scenario can assert the measured gap between two requests
without racing a window in which nothing should happen.

Three controls keep the remaining HTTP assertions free of timing races too. A `Retry-After` date
is computed when its response head is written, the scripted delay after the current UTC and
rounded up to a whole second, so the date always asks for at least that delay however late the
request arrives. A response held until released stays unanswered, and so the attempt that sent it
unresolved, until a scenario step releases it with a response of its own choice; only the client's
own timeout or the receiver's stop ends it sooner, so a scenario can observe state that must hold
while a request is unresolved without betting on how long a scripted delay lasts. A request for a
target the scenario answered by target takes that answer instead of the next scripted response,
which gives each request of independent branches or source relays its own outcome whatever order
they arrive in. Answering the same target again replaces its answer, so an endpoint that kept
failing one request can recover while the scenario watches the retries.

The receiver also records two facts about how its clients treat their answers, each read once the
requests it concerns have arrived. It keeps the most requests that were ever awaiting a response at
once: a request awaits from its capture until the receiver begins writing its final head, or until
its connection ends without one, so a client that sends each request only after reading the previous
final head never has two. And it counts the responses its clients abandoned: a held response, a
stalled body, or a body still being written whose connection the client closed first. A response
body can be generated at a scripted size and is written chunk by chunk, so a body many times the
socket buffers of a loopback connection proves that a client which stopped at the final head never
read it. A stop of the receiver itself abandons nothing, and an abandoned response is not a fault.

Everything a receiver holds is bounded, and exceeding a bound is recorded as a fault, not captured.

| Bound | Limit |
| --- | --- |
| One request head | 256 KiB and 512 header fields, room for a request at every HTTP emitter limit at once |
| One request body | 16 MiB |
| Captured requests | 4,096 per receiver |
| Kept faults | 256 per receiver; later faults are counted but not kept |
| One generated response header value | 1 MiB, enough to exceed the 417,792-byte response head hyper reads |
| Generated response headers per block | 512, enough to test both sides of hyper's 100-field limit |
| One generated response body | 64 MiB, written in 16 KiB chunks and never held in memory |
| A scripted `Retry-After` date | One day ahead, far beyond any scenario's wait |

Every await a receiver connection makes also waits for the receiver's stop, so a held response or
a stalled body ends as soon as cleanup begins, and so does every task hyper's HTTP/2 server runs
for a connection's streams. The receivers of a scenario stop together in the `stopping` phase,
before the cluster, under one 6-second budget: 5 seconds for every connection to
end on its own, after which the rest are aborted and joined, and 1 second to join the accept loop,
after which it is aborted and joined too. The derivation asserts that this budget is shorter than
the cluster's cleanup budget. Each stop is recorded in the scenario log:

```text
HTTP receiver cleanup: <name>: stopped <n> connection(s) in <elapsed> of a 6s budget, <n> forced, <n> panicked; captured <n> request(s), recorded <n> fault(s)
scenario cleanup forced: HTTP receiver <name>: <the same record>
```

The second line appears only when a connection or the accept loop had to be aborted, or panicked.
A receiver's port is drawn with the scenario's fixture ports and goes back with them at the end of
cleanup, once the nodes that dialed it have ended.

## HTTP Proxies

A scenario about the proxy an HTTP client declares, or about proxy environment variables a node
must ignore, starts an in-process forward proxy, because only a proxy the harness controls can show
which requests a client sent through it and which it sent past it. A connection carries one
request. `CONNECT <host>:<port>` opens a tunnel, which the proxy copies bytes through without seeing
what TLS carries inside; a request in absolute form is forwarded to its origin in origin form,
without the proxy's own fields. The proxy records the request line of each, so a step can wait for a
request it forwarded or a tunnel it opened, or assert that it carried nothing.

The proxy resolves every name it is asked for to the loopback address where the scenario's
receivers listen. A scenario can therefore name its receiver by a host the nodes cannot resolve,
such as one under `.invalid`, and a request reaches that receiver only through the proxy. A proxy
started with credentials answers `407` to a request without exactly those `Basic` credentials and
records the refusal; a request in origin form is answered `400` and recorded as misdirected.

A request head is bounded at 256 KiB and 512 fields, and a proxy records at most 4,096 requests.
Every await a proxy connection makes also waits for the proxy's stop. The proxies of a scenario
stop together with its receivers under the same 6-second budget: 5 seconds for every connection to
end on its own, after which the rest are aborted and joined, and 1 second to join the accept loop,
after which it is aborted too. A proxy's stop writes no record to the scenario log, and its port
goes back with the scenario's other fixture ports at the end of cleanup.

## gRPC Receivers

The node trace-export scenarios start one or three real server processes with `--otel-enabled`,
separate service names and the scenario's DNS configuration. A local gRPC collector captures their
OTLP trace requests; the steps decode the current trace schema and require spans from every node
and a query for the collector's fixture name. This uses the process cluster's existing readiness
and failure diagnostics. Processes are dropped before the DNS authority, resolver files and
collector, including when a scenario fails. The fixtures use no external collector container.


A scenario about a node calling an external gRPC service, such as an OTLP/gRPC collector, starts an
in-process receiver that serves HTTP/2 without TLS in its place. It reads each unary call to the end
of its request and captures the method, the header fields, the compressed flag, and the one request
message exactly as it arrived. It then answers the call with the next scripted answer: an empty
response message with `grpc-status: 0`, a trailers-only status, no answer at all while the receiver
closes the whole connection, or no answer until the client resets the call. Once the script is
empty, calls are accepted. The receiver never interprets a message, so a scenario step decodes what
it captured.

An OTLP receiver is the protocol-neutral name a scenario uses for either transport:
`Given OTLP receiver "<name>" is running for "<protocol>"` starts a gRPC receiver for `grpc` and an
HTTP receiver for `http/protobuf`, and one script vocabulary — `accept`, `reject`, `unavailable`,
`lose response`, `hold response` — maps to the answer each transport gives for it.

A gRPC receiver has the HTTP receiver's bounds on its message, its captured calls, and its kept
faults. A call whose length prefix already declares more than 16 MiB is refused as a fault before
its message is read, and a request that is not exactly one gRPC message is a fault rather than a
capture. Every await a connection or call makes also waits for the receiver's stop. The gRPC
receivers of a scenario stop together with its HTTP receivers under the same 6-second budget, and
each stop is recorded in the scenario log:

```text
gRPC receiver cleanup: <name>: stopped <n> connection(s) in <elapsed> of a 6s budget, <n> forced, <n> panicked; captured <n> call(s), recorded <n> fault(s)
scenario cleanup forced: gRPC receiver <name>: <the same record>
```

A call that panics takes its connection with it, so the stop counts it among the panicked
connections.

## Broker Observers

A scenario about a node that publishes to a broker observes the destination through a client of its
own: a Kafka consumer, a subscriber of MQTT, RabbitMQ, Redis, Pulsar or SQS, a ZeroMQ socket, or a
core NATS subscription. Its steps then assert on what the broker delivered to that client, in the
order the broker delivered it.

Most observers run one task on the binary's runtime that reads the broker's client and hands
each message to the step over a bounded channel. Dropping the observer aborts that task. A core
NATS observer reads the subscription's existing buffer directly, without a second forwarding
channel or a scheduling round trip for each message.

The bounded exact-count and sequence/header assertions capture their deadline on entry, take
ownership of the observer and start one observation task on the binary's runtime through
`runner_observation::observe_off_the_runner`. That task drains and checks every message, yielding
cooperatively while Cucumber polls its other scenarios. A delayed runner learns the result later
without stopping the drain or extending its captured deadline. Dropping the step's future aborts
its observation and releases the observer; an assertion panic returns to the original step.

The sequence assertion checks each JSON sequence number and every header, in delivery order, then
keeps the existing 500-millisecond duplicate check. On success it returns the same observer and the
final message to the scenario. A missing message still fails at the captured deadline; neither
receipt statistics nor scheduling delay can excuse a gap, duplicate, reordering or header change.

The NATS client buffers at most 65,536 messages for one subscription and, as a slow consumer, drops
what arrives beyond them, so a scenario keeps the messages its step has not yet taken below that
bound.

A step that waits for a sequence of messages and times out names how many messages the observing
client had received from its broker, for a client that counts them, so a message that never reached
the observer reads differently from one the step had not yet taken.

A sequence mismatch also keeps the observed headers and those receipt diagnostics. The NATS
observer connects with a subject-labelled driver callback: each reported connection, disconnection,
slow-consumer, close, server/client error, lame-duck and drain event appears with that subject in
the scenario log. Eight fixed-size counters accompany a failure, without a second delivery queue.
The driver's event channel is bounded, so these count notifications the callback received, not
messages dropped or an exhaustive transport history. Receipt and event counts never excuse a
sequence or header mismatch.

The wide-columnar NATS emission outline uses the production sticky scheduler and explicitly moves
the source and emitter to node-1 before publishing. Its relay is on node-1 in the one-node example
and node-2 in the three-node example; cluster-status assertions establish both remote hops in the
latter. Each example publishes all 32,768 records, waits for the source's sent count, releases the
emitter stall, and checks every sequence and repeated header within the original 90-second budget,
followed by the 500-millisecond duplicate check. The source count does not confirm delivery to the
broker's observing subscriber.

## DNS Authorities

A scenario about how nodes find one another by name selects its cluster's peer addressing before the
cluster starts, with `Given cluster peers are addressed by "<addressing>"`. Every other cluster uses
literal IPv4 loopback endpoints, starts no DNS fixture, and lets its nodes load the host's own
resolver configuration, as a production node does.

| Addressing | Each node listens on | Each node advertises | Its name answers with |
| --- | --- | --- | --- |
| `literal IPv4 endpoints` | `127.0.0.1` | `127.0.0.1` | No fixture |
| `literal IPv6 endpoints` | `::1` | `::1` | No fixture |
| `DNS names` | `127.0.2.<n>` | `node-<n>.nervix.test` | Its listen address |
| `DNS names behind an unreachable address` | `127.0.2.<n>` | `node-<n>.nervix.test` | `127.0.4.<n>`, where nothing listens, then its listen address |
| `single-label DNS names` | `127.0.2.<n>` | `node-<n>`, completed by the search domain | Its listen address |
| `hosts file names` | `127.0.2.<n>` | `node-<n>.nervix.test` | Nothing: the hosts file lists the name and the fixture never answers it |

A cluster addressed by names starts one in-process DNS authority on a loopback UDP port drawn from
the port pool. The harness writes a resolver configuration with the search domain `nervix.test`,
`ndots:1`, and one-second queries with one retry, and a hosts file, into the cluster's directory,
and starts every node with that configuration and the authority as its only name server. Every
answer carries a one-second TTL, positive or negative, so a zone change reaches a node within
seconds of the cached answer expiring. A scenario can move a stopped node to `127.0.3.<n>` behind
the same name, and answer a node's name as a name that does not exist, a name with no address, or
not at all. A name outside the zone does not exist, with a negative TTL of zero. Every test
certificate names `localhost`, `127.0.0.1`, `::1`, `node-<n>`, and `node-<n>.nervix.test`, so each
addressing presents the name its peers expect.

A scenario can also publish a dependency it started under a name in the zone, such as
`rabbitmq.nervix.test`, answered with the dependency's literal address or with a list of loopback
addresses in order, and later answer that name as a name that does not exist, a name with no
address, or not at all. A dependency's published port listens on every local address, so an answer
that cannot connect needs TCP forwarders: listeners at chosen loopback addresses such as
`127.0.5.<n>`, all on one port drawn from the pool, that forward to the dependency and count the
connections each accepted. Stopping a forwarder closes its listener and every connection it
carried, so a client that still holds a cached answer naming it cannot reconnect there. A scenario
can restart that listener on its still-reserved port to let the same client reconnect while its
target node stays up; the CLI clock process scenario uses this to observe reattachment after a
transport loss without changing the node's clock installation. An address
the scenario names without a forwarder refuses connections on that port. The TLS
certificate the harness gives its containerized dependencies names `localhost`, `127.0.0.1` and
`*.nervix.test`, so a dependency reached through a zone name presents a certificate for the name
its client dialled, and a name outside the zone does not match it.

The authority answers from memory and holds nothing else: it counts questions for at most 1,024
names, and a scenario can assert that its nodes asked nothing for their names. It answers until the
cluster is dropped at the end of cleanup, after every node has stopped, so no node's lookup outlives
it; dropping it aborts its answer loop, and its port goes back to the pool. How long a node waits
for an answer is product behavior, bounded by the budget of the path that asked and described in
[Bounds, Deadlines And Cancellation](./name-resolution.md#bounds-deadlines-and-cancellation); the
scenario's own waits are status waits.

The resolver crate's focused checks use the same authority, started on an unused port of their own,
to observe DNS messages directly: TTL expiry, negative caching, refusals, silence, the lookup
budget, the concurrency bound, and runtime teardown. `just test-dns` runs them.
[Evidence](./name-resolution.md#evidence) states what each form of DNS evidence establishes.

## Client Probes

The cross-language conformance scenarios in `client_conformance.feature` run a small client of each
runtime against a scenario's cluster, or against the checked-in conformance corpus. The in-process
probe drives the shared Rust binding's C ABI from a blocking task of the binary's runtime, because
every call of the binding blocks its caller; every other probe is a child process that the fixture
starts with its target in `NERVIX_PROBE_*` variables and its standard input closed. A probe prints
one report line per observation on standard output, and the fixture keeps every line it read.

A probe's waits are bounded twice. The step that starts it waits at most 180 seconds for the line
that says its subscription is open or its clock attach completed. A later step can wait, for the
duration it names, for one more line the probe prints, so a scenario acts between the probe's
observations: the clock probe prints the first tick of a generation before the scenario stops and
starts the domain, and the interruption before the scenario restarts the TCP forwarder the probe
entered through. Every line read on the way is kept for the report. The step that reads the report
waits the duration the step names for the probe to end, 180 seconds against a cluster and 60 against
the corpus. Each probe also ends itself: a binding probe gives up on its rows, or on each stage of
the clock it follows, after 120 seconds, and the Go and TypeScript probes give up on their whole run
after 170. A failure quotes every report line read so far, the exit status, and the probe's
standard error. Dropping the fixture kills a child process, so a failed scenario never leaves a
probe running; the in-process probe ends when its session fails against the stopped cluster, or at
its own deadline.

Every example of a runtime other than the in-process probe is tagged `@client_conformance_toolchain`
and one `@client_probe_<runtime>` tag, and the suite excludes the first tag unless a run selects its
own tags, because those examples need toolchains the suite's job does not install. `just
test-client-conformance` builds every probe artifact and runs them; its first argument is the tag
expression that selects runtimes. The `client-conformance` CI job, which runs only for a pull
request labeled `client-conformance`, runs it on its own runner with a 60-minute limit and a
15-minute suite budget, which leaves the builds before the scenarios up to 40 minutes of the limit
and keeps the same 5-minute reserve.

## The Suite Watchdog

The scenario run has one budget, 105 minutes from the moment it starts, which `--suite-budget` or
`NERVIX_TEST_SUITE_BUDGET` replaces with a duration such as `4m`, read by the same guarded duration
parser as a node's options. The budget is a clock rather than
a count of failures. Cucumber's fail-fast stops scheduling scenarios and leaves those already
running where they are, so it cannot end a run whose step, diagnostic, or node stop never returns;
the clock ends such a run at the same instant as one whose work returned at once. Until the budget
expires, the suite keeps running its intended failure and retry coverage.

The watchdog reads two registries: the active-scenario registry, and the live-cluster registry. A
cluster registers when a scenario builds it. Each node registers when its task is spawned, through a
registration the task itself owns, so the node leaves the registry when its task ends, whether it
returned, failed, or panicked, and the watchdog can tell a cluster that stopped from one still
running without owning either. A cluster stays registered for as long as its scenario holds it or
one of its nodes was left stopping, so a node that outlived its scenario's cleanup is still named.
The registry holds each node's stop, its shutdown coordinator, rather than the node, so the watchdog
never reaches into a scenario that is still using it.

When the budget expires:

1. The watchdog reads both registries before changing anything, so the diagnostic is the state the
   run expired in, and prints it to standard error at once. If the job's own limit kills the process
   during what follows, it has still said what it was doing. The scenario log receives the same
   diagnostic, with the cleanup outcome, once the cleanup window has ended.
2. It asks every live node to stop, then waits one 60-second window for all of them together,
   checking every 50 milliseconds.
3. It drops the run. That ends every scenario future still held, which kills the server processes
   the scenarios started and leaves the nodes they still hold stopping.
4. The suite stops its dependencies within 2 minutes and drops the runtime within 60 seconds, which
   ends every node task that is still running.
5. The process prints the dependency teardown and the cleanup outcome, flushes its output, and exits
   with status `124`.

```text
suite timeout: the <budget> suite budget expired with <n> scenario(s) active
  suite timeout active: <identity> attempt=<n> phase=<phase> phase_age=<age> age=<age> nodes=[<nodes>]
  suite timeout unclaimed cluster: <identity> nodes=[<nodes>]; its scenario had already left the registry
suite timeout cleanup: asked <n> node(s) to stop and waited <elapsed> of a 60s window; every node ended itself
```

The active lines name every scenario still queued for its permits or running. An attempt that has
finished is not active, even while its registration remains: Cucumber keeps the world of a failed
attempt, and the registration with it, for the report it prints at the end of the run, long after the
retry that followed has run. The timeout of Check run 40241968478 listed 13 such attempts of 12
scenarios beside its 142 queued and running scenarios, and they read as retries that had never
started. Every one of them had been followed by a retry that started between 1.8 and 10.3 seconds
after it ended, and each scenario's last retry had passed.

When a node is still running at the end of the window, the cleanup line names its cluster instead of
reporting that every node ended itself, and the scenario log adds
`suite timeout cleanup forced: the run was dropped with nodes still running`. A live cluster whose
scenario has already left the registry is a leak, and the unclaimed line is its only record.

The watchdog does not run after hooks. Faults a scenario injected stay in place while its nodes are
asked to stop, so a node held by one can still be running at the end of the window, and it ends
when the process drops the runtime. The induced timeout during qualification had one such node,
held by a consensus commit delay that only its scenario's cleanup releases.

### The CI Reserve

The suite runs inside `just test-scenarios-coverage` in the CI `scenarios` job, after that job's
instrumented server, CLI and paced simulation driver builds. The `tests` job runs at the same time
on another runner. The `scenarios` job's `timeout-minutes` is 155, and the suite budget is derived
from it so that the job ends on its own.

For focused local coverage, `just coverage-scenarios <lcov-path> <scenario-options>` starts a
fresh measurement. `just coverage-scenarios-append <lcov-path> <scenario-options>` retains the
current profiles and reuses unchanged instrumented artifacts for another scenario selection.
When collecting a different source revision, first run `just coverage-clean-workspace` so
instrumented binaries and line mappings from earlier sources cannot enter the new report.
After collecting unit and scenario profiles, `just coverage-report-workspace` exports all
workspace packages, including executed test files, as `lcov-workspace.info` with the checkout path
beside it, and `just coverage-merge` maps that report onto the repository.

[Coverage Reports And The Complexity Gate](./coverage-reports.md) owns the producer catalog,
export flags, checkout-root mapping, tracked-source policy, ordinary merge record and integrity
failures. That chapter also owns the advisory patch/CRAP reports and the separate deterministic
complexity verdict. The `coverage-merged` artifact retains the ordinary report, its merge record
and both advisory reports. The `coverage-full` job later reads that report unchanged and adds the
modeled, diagnostic and fuzz collections the run selected to form the
[full workspace union](./coverage-reports.md#the-full-workspace-union); their counters never enter
the ordinary report, its patch coverage or its CRAP report.

The Deloxide lane's supervision qualification runs outside its coverage collection. Product API
doctests, compiler-cache qualification, replay and deliberately weakened checks also retain their
[independent verdicts](./coverage-reports.md#verification-outside-collected-counters).
No absent counter turns one of those required checks into an optional execution.

The scenario collector also starts coverage in each published Python paced driver using the shared
`scripts/paced_simulation_coverage` startup hook and configuration. It combines those completed
process reports into `target/paced-simulation/python.lcov`. CI merges this scenario report with the
deterministic Python open-policy report from the extra checks, alongside ordinary Rust coverage.
The STOP/START generation-following outline also carries `@paced_simulation_reopen`, so both full
Deloxide selections reach its Rust and Python drivers on one-node and three-node clusters.

| Part of the job | Budget | Basis |
| --- | --- | --- |
| Work before the scenario binary starts | 35 minutes | 5m42s to 16m54s across the scenarios jobs of 22 Check runs of 21-27 October 2026; twice the slowest, rounded up, funds cold builds and setup variation |
| The scenario run | 105 minutes | What the limit leaves; twice the rounded 50-minute healthy ceiling plus five minutes |
| After the budget expires | 15-minute reserve | At most 60 seconds of cleanup, 2 minutes of dependency stop and 60 seconds of runtime shutdown; eleven further minutes fund slow coverage export, uploads and cache publication after a successful run, which took 0.7 to 1.9 minutes in those 22 jobs |

A healthy suite finishes inside its budget and exits `0` or reports its failures. A wedged one exits
`124` with its diagnostic and leaves the upload its reserve. Either way, a step that runs whatever
the job's result uploads the whole `tests/logs` directory as the `scenario-logs` artifact.

A suite that passes ends before its budget, so the reserve then holds the coverage export, the
uploads and [cache publication](#cache-publication), which waits at most 120 seconds. Saving the
manifest took 5 to 14 seconds in the scenario jobs measured on 2026-10-14, and the daemon had
already published every entry those jobs built. A suite that fails or runs out of its budget leaves
publication `not-run`, so the reserve stays with the cleanup.

Run 40419254390 completed 3,703 attempts in 2,971.1 seconds with 98.9% utilization and
141,081.3 slot-seconds, the first run after the scenario build optimized the parser that had held
the runner thread (see [Runner Stalls](#runner-stalls)). The 17 runs of 21-26 October that came
before it on one inventory of about 3,620 attempts took 2,688.7 to 4,501.9 seconds of a 75-minute
budget: every scenario of a run took the same multiple of its median, from 0.74 to
1.25, so the slowest runners ran the suite 1.7 times as long as the fastest, and the slowest run
exhausted the budget. Those distributed slowdowns justify proportional headroom, and twice the
measured ceiling covers that spread.

The job's limit remains the emergency guard outside the budget rather than the mechanism that ends a
wedged run. A job the limit cancels is killed wherever its scenarios are: the logs it uploads end
mid-scenario, with no summary and no record of what each scenario was doing. The limit and the
harness's copy of it change together.

### CI Budget Review

The review of 21 October 2026 compares execution allowances with retained progress rather than
restarting an unchanged run after its watchdog expires. Its suite allowance of 75 minutes in a
120-minute job became 105 minutes in a 155-minute job on 27 October 2026, when the measurement in
[The CI Reserve](#the-ci-reserve) replaced the 34-minute ceiling it rested on. Other narrow bounds use at least 1.5 times the observed duration or current shard
estimate, with time left for their surrounding work. The rows below cover the workflow job families
and their nested execution guards; a missing completion is reported as incomplete evidence.

| Guard | Evidence | Current allowance and decision |
| --- | --- | --- |
| Scenario suite and job | 49m31s complete in 40419254390; 17 earlier runs of one inventory took 44m49s to 75m02s with a 1.7-times spread between runners | 105-minute suite, 155-minute job, including 35 minutes before and 15 after the suite |
| Ordinary Bolero job | 47m10s in run 39649567371 | 90 minutes; 1.5 times the measurement is 70m45s |
| Sanitizer shards | The current 205-target, three-shard plan estimates at most 114 minutes before discovery, qualification and uploads | 240 minutes; 1.5 times the estimate is 171 minutes, leaving 69 for surrounding work and cold-build variation |
| Ordinary image builds | 65m01s for amd64 in Docker Build 39534273321; 56m43s in 39654966533 | 120 minutes; 1.5 times the slower complete build is about 98 minutes |
| Diagnostic image build | 24m23s in Docker Build 39531575177 | 80 minutes retains ample build and upload margin |
| Stressed lifecycle invocation | 19m49s in Check 39569675589 | 45 minutes, of which 39 are available to scenarios after their six-minute teardown reserve; 1.5 times the measurement is under 30 minutes |
| Other diagnostic invocations and stages | Order workloads completed in 40m46s; stress workloads in 35m02s; restore scenarios in 35m33s in 39569675589 | Retain the 80-minute build and workload stages, 175-minute collecting step and 200-minute job; restore's 70-minute invocation leaves 64 for execution, and the other invocation bounds retain their reserves |
| Workspace checks | 43m13s complete in 39569675589; interrupted at 58m38s in 39656928785 | Retain 120 minutes; the interrupted run establishes progress, not a completion ceiling |
| Extra checks | 56m32s complete in 39656928785 | Retain 120 minutes, beyond 1.5 times that duration plus exports |
| Ordinary tests | 22 minutes in 39649567371 | Retain 60 minutes |
| Loom and qualification shards | 20m43s and at most 49m37s in 39649567371 | Retain 60 and 120 minutes |
| Shuttle | Run 40014913908 reached the 120-minute job guard after 389 check/mode executions completed; two exhaustive ACK explorations took about 64 minutes | 240-minute outer job guard covers preparation, full exploration, nondeterminism rechecks, replay and coverage export; model schedule counts and exploration bounds retain their declared values |
| Turmoil and client conformance | 14m49s and 18m42s in 39649567371 | Retain 30 and 60 minutes, including the runners' own post-build bounds |
| Book build | 24m30s in Docker Build 39534273321 | Retain 60 minutes |
| Ordinary and diagnostic Chaos smoke | 15m15s and 15m22s in Docker Build 39531575177 | Retain the inventory-derived 45-minute step and 66-minute job; soak bounds remain derived from their declared timelines, teardown and CI reserves |
| Coverage merge and union | 1m15s and 27 seconds in 39569675589 | Retain 20 and 15 minutes |
| Diagnostic evidence; selection, title, metadata, manifests and verdicts | Diagnostic evidence took at most 2m30s in the sampled runs; the other jobs took seconds | Retain their explicit 5–45-minute guards and the title job's workflow default |
| Cache publication and failure cleanup | Publication took 3–4 seconds in the sampled diagnostic jobs, bounded at 120 seconds within a four-minute step | Retain the existing bounded cleanup, publication and artifact reserves |
| Benchmark job | No benchmark completion in these sampled runs | Retain its 120-minute job guard; benchmark iterations and measurement time remain with their runner |

Per-case model bounds, sanitizer campaign lengths, product deadlines and the assertions a scenario
declares retain their own meanings. More execution time lets the suite report all of its outcomes;
a failed product deadline or assertion remains a failure that needs its owning repair. The budget
review changes no synchronization protocol or tracked lock path, so Deloxide does not apply to the
budget changes themselves. The inventory and workflow contract tests validate the affected guards.

A same-commit rerun of the scenarios job replaces that job's four report artifacts: scenario logs,
runner load, kache report, and scenario coverage. This lets a rerun publish its own diagnostics
under GitHub Actions' immutable artifact names. When the coverage job is rerun, its advisory
reporter updates its marked PR comment for the same PR head instead of adding a comment.

### Runner Stalls

The watchdog is polled on the runner task, so it cannot see, let alone end, a step that blocks that
task. The run is therefore polled beside a tick on the same task, due every 250 milliseconds. A step
that blocks inside its poll delays the tick until it returns, so how late the tick is polled is how
long the task was held. A stall of one second or more goes to the scenario log with every scenario
that held its run slots during it:

```text
runner task stalled for <duration>
scenario live during runner stall: <identity> attempt=<n> phase=<phase> phase_age=<age> age=<age>
```

A scenario still queued for its permits runs no step and no cleanup, so it cannot hold the task and
the stall does not list it. Cucumber takes up every parsed scenario at once, so early in a run more
than three thousand scenarios wait in that queue; listing them made each stall write thousands of
lines from the runner task itself, which was more than half of the scenario log's 265 MB in Check
run 40241030302.

The log line is a diagnostic, not a failure: a stall delays every wait and bound the other scenarios
measure, and the line says when and for how long. Backup steps hand their archive reads, rewrites,
digests and fixture generation, and the inspection of stopped nodes' databases, to blocking tasks;
the native metadata restore scenario, whose archive holds lifecycle and offset records above 32 MiB
each, held the runner task for about 30 seconds while it inspected three restored nodes before they
did.

One poll of the run polls every scenario woken since the last one, so the task is also late when many
scenarios each do a little synchronous work in the same poll, without any single step reaching a
second. The suite summary therefore adds up the lateness of every tick, not only the stalls, and
reads the scheduling counters of the thread that polls the run from its procfs `schedstat`: how long
that thread ran on a CPU, and how long it was ready to run without one. Beside them it reports how
long the runtime's workers, which run every in-process node, were busy together:

```text
Runner task: <n> ticks polled <late> late in total; <n> stalls took <stalled>, the longest <longest>; its thread ran <on-cpu> on a CPU, <share> of the run, and waited <wait> for one.
Runtime workers: <n> busy for <busy> together, <share> of their time over the run.
```

A runner thread that is on a CPU for most of the run is what every scenario waits for. One that waits
long for a CPU is starved by the rest of the runner rather than by its own steps. Workers busy for
most of their time say that the nodes, not the steps, set the pace.

The runner thread also allocates and frees for every scenario it polls, and jemalloc returns freed
memory to the kernel from whichever thread is allocating when a purge falls due. In a process of
several hundred threads each purge waits for a TLB shootdown across the CPUs that run them, which a
virtual machine makes slow. At 64 slots the runner thread ran on a CPU for 86 percent of Check run
40358096404. A profile of the same workload on a loaded development host found 88 percent of that
thread's samples in the kernel, 79 percent in the cross-CPU call a TLB shootdown waits for, and its
`madvise`, `brk` and `munmap` calls took 48, 421 and 100 milliseconds on average. The scenario
process therefore turns on jemalloc's background threads before the run starts: they purge on the
same decay schedule, and no thread that allocates waits for a purge. On that workload the runner
thread's system time fell from 18 to 8 percent of the run, and its user time, the scenarios' own
work, rose from 2 to 7 percent.

Purging in the background did not free the thread on CI: at 48 slots it still ran on a CPU for 89
percent of Check run 40404803351, about 0.95 seconds per attempt. Much of that was parsing. The
runner thread parsed a command a scenario runs about eight times: the harness parsed the command's
document six times to decide how to send it and what to record from it, parsed each statement again
to size the client's wait, and the client library parsed the statement it sends. The 36,276
statements of that run averaged 54 characters, and the scenario build compiled the parser without
optimization: formatting a 140-character statement, which parses it, took about 38 milliseconds
unoptimized and about 7 with only `nervix-plexus` optimized. The dev profile therefore builds
`nervix-plexus` at `opt-level = 2`, keeping its debug assertions and overflow checks. In Check run
40419254390, the first after that change, the runner thread ran on a CPU for 74 percent of a
2,971-second run, 0.60 seconds per attempt, and its ticks were 883 seconds late in total rather
than 1,705. The harness now also reads each command document once, as a `CommandDocument` that
answers every question the harness asks of it, so a statement is parsed three times rather than
eight: with its document, to size the client's wait, and in the client library.

## Diagnostic Lane

`just test-deloxide`, `just test-deloxide-order`, `just test-deloxide-stress` and
`just test-deloxide-stress-restore` run the Deloxide diagnostic lane: every workload
`tests/deloxide-inventory.toml` registers, in the build of the diagnostic feature its selection
compiles, supervised by `scripts/deloxide_lane.py`. [Data-Plane Concurrency](./data-plane-concurrency.md#diagnostic-deadlock-detection)
owns what the lane proves, its failure classes and its applicability records; this section owns how
it runs and how long it may take.

The lane starts after its prerequisites, `just tests-deps`, and first builds every selected test
executable and diagnostic paced driver with Cargo under a build budget of its own. No workload
runs during that stage. Only after every executable is available does the unchanged workload
budget start; neither compilation nor warming a local build directory consumes it. Both stages
record their budget, start, verdict and elapsed time in `lane.json` and the CI step summary. A
build failure or build-budget expiry retains its stage and starts no workload. Interruption
retains whichever stage was running. The prerequisites provision what the workloads load, as they do for the
ordinary suite: the WASM guests, the generated ONNX models and the ONNX runtime library, whose
path the lane passes to every invocation. The probes, the tracked locks' conformance checks and
the owner tests are libtest executables, run directly so that a signal reaches the lane as a
signal rather than as Cargo's status. The scenario binary runs the lane's tagged scenarios
without retries. The active-only and order selections run them in two inventory invocations: the
restore, client, diagnostic, remote ACK, Roto function, TLS rebinding and inferencer scenarios,
then the paced-driver scenarios with the diagnostic Rust driver; the stress selections run the
same scenarios in three invocations of their own, described below. Active mode, with or without
stress, runs each invocation as one process. Order mode starts a fresh
process for each tagged feature in the first invocation; the materialized restore feature runs
each scenario separately and its six large examples each have their own process, as do the four
complete-generation restore examples, selected by the inventory's `order_tags`. The lane checks
every chunk's Cucumber summary against its registered examples, retains its logs and evidence
separately, and stops on the first failed chunk. Each process of the active-only and order
selections runs the inventory's fixed number of scenarios at once, four, rather than one per CPU: a diagnostic
build pays for its tracked acquisitions on every lock, and a fixed count puts the same load on its
nodes locally and on CI's 16-vCPU runner.

The lane resolves every feature pattern against its repository root before passing it to
Cucumber. A relative pattern would make Cucumber traverse the whole working directory, including
generated build artifacts. An exact feature uses an absolute pattern with two identical literal
alternatives, so the walker starts at that feature's parent directory and still selects only
that file. This bounds discovery without changing the registered examples or their deadlines.

Under the native coverage collector the lane instruments only workspace crates, the scope the
ordinary coverage build has. Before starting the workload clock it repeats every selected build
once in the same instrumented target, with the same features, compiler wrapper and environment.
The record and CI summary retain the initial and warm pass durations. Both passes share the one
build budget; a failed warm build starts no workload. This measures cache conditions without
another cold target or a second workload run. The completion reader requires both measurements
for an instrumented lane. When every crate was instrumented, wasmtime's compiler included, CI's
WASM restore scenarios ran five times as long as an uninstrumented local run, 338 seconds against
64 for one of them, and its restored runtime revision missed the product's readiness deadline: the
test cluster's 10-second node unavailability timeout plus the 30-second propagation bound. It
missed it at four scenarios at once as at eight, because the time went to preparing the restored
WASM processor, not to waiting for a run slot. The product deadline stays as it is; the lane
removes the instrumentation the product's own work never needed. Each invocation records
its evidence in its own directory of the attempt, and the binary's `tests/logs` are copied into the
attempt after it ends, because the next invocation truncates them.

Each scenario invocation passes the binary `NERVIX_TEST_SUITE_BUDGET`, its bound less the
inventory's teardown reserve, so a suite that runs out of time ends itself with the [suite
timeout](#the-suite-watchdog) diagnostic and its own cleanup before the lane would stop it. The
reserve is six minutes: the four the suite's own cleanup can take after its budget, with two to
spare. A bound that expires anyway ends the binary with `SIGTERM` and, after the stop grace period,
`SIGKILL` for its whole group; Ryuk removes the containers the binary started when its connection
closes. A stall of the runner task itself ends only this way, because the suite watchdog runs on
that task, and the binary then prints no Cucumber summary. Unless a tracked lock takes part in the
stall, the lane records it as `timed-out` with no finding: a wait inside a dependency, such as the
storage engine's own worker channel while a database closes, is visible only in a stack of the
binary's threads taken before the bound expires.

In CI the `deloxide` job, which runs only for a pull request labeled `deloxide`, runs each
selection on its own 16-vCPU runner through `just coverage-native-extras test-deloxide`,
`test-deloxide-order`, `test-deloxide-stress` or `test-deloxide-stress-restore`, then
`just test-deloxide-qualification` for the same selection, and uploads every attempt whatever the
verdict.

| Part of the job | Bound | Basis |
| --- | --- | --- |
| Setup before the lane | No bound of its own | Toolchain, LLVM, Go, TinyGo and kache installation, as in the other native jobs: 46 seconds for `deloxide` and 40 for `deloxide-order` in the measured run |
| Diagnostic builds | 80 minutes, `build_budget_seconds` in the inventory | A separate bound for all selected test and driver builds. A measured cold stressed scenario build on a loaded local host took 33m08s; CI builds measured 5.5–11 minutes. The bound leaves time for the other builds and variation without reducing workload time |
| Workloads | 80 minutes, `budget_seconds` in the inventory | Starts after every diagnostic build succeeds. Per-invocation bounds and the six-minute scenario teardown reserve still apply; workload time reduces the time left for later invocations |
| Prerequisites, both stages and the coverage export | 175 minutes, `timeout` in the step | The two 80-minute stage bounds plus 15 minutes for prerequisites and export. The historical combined-stage run took 25m02s for `deloxide` and 23m24s for `deloxide-order`; prerequisites took 3m05s and 2m46s, and export 2 seconds each |
| Supervision qualification | 10 minutes, `timeout` in the step | A small probe build and up to seven bounded cases; only the untracked wait runs to its bound, 15 seconds, and the others end in milliseconds. It took 40 and 41 seconds in the measured run |
| Uploads and the kache report | The rest of the 200-minute job limit | Attempt directories, completion records and LCOV reports, uploaded after either step's verdict, then [cache publication](#cache-publication), which waits at most 120 seconds. In the measured run the uploads took 4 and 3 seconds |

The table's historical CI measurements come from the run of 2026-10-07 in which both selections passed with
every owner of tracked locks reached: 110 workloads in `deloxide` and 118 in `deloxide-order`,
with the lane instrumenting workspace crates only and running 55 scenario examples at four at
once. The first run in which both selections passed, on 2026-10-06 with 97 and 105 workloads and
49 scenario runs, took 30m27s and 23m18s in the lane's step. The six scenario runs and seven
conformance checks added between those runs fit inside their variation. A `deloxide-order` run
that included the one-node restore of a deduplicator keyspace and a window above the bulk budget,
together with its three-node example, took 21m47s in the lane's step with 107 workloads and 51
scenario runs before those six scenario runs and seven conformance checks were added. The table
predates the four remote ACK examples and the order-mode per-example process split, so its
durations are historical measurements rather than a timing claim for the current combined
inventory; the inventory and job bounds remain enforced.

The two stress selections compile one build, the active-only build with the bounded disturbance
of `StressConfiguration::LANE`, and between them run every scenario of the active-only selection but
the report tool's triage, one at a time: a stress build enters Deloxide's process-wide detector
twice on every mutex and write acquisition, and when the selections were introduced, with four
scenarios in one process, their nodes contended on it and missed startup and request deadlines
that a plain diagnostic build of the same scenarios met.
[Two Stressed Scenarios In One Process](#two-stressed-scenarios-in-one-process) records what made
them miss. `deloxide-stress` runs the active-only selection's probes, conformance checks and
owner tests with the disturbance, the probe that proves it, the `stress-scenarios` invocation of
the inventory's lifecycle scenario runs tagged `@deloxide_stress`, bounded at 45 minutes, and the
`stress-paced-simulation` invocation of the 36 paced-driver runs with the stressed Rust driver,
bounded at 40. The six transaction-retention and ten endpoint ACK examples run in separate serial
invocations, each bounded at 15 minutes, within the same selection workload budget.
`deloxide-stress-restore` runs the build's probes and the `stress-restore-scenarios`
invocation of the inventory's capture, restore and checkpoint-transfer runs tagged
`@deloxide_stress_restore`, bounded at 70 minutes. Each selection has the inventory's 80-minute
budget and its own CI runner.

The split follows from what the disturbance costs. It barely lengthens a scenario, because the
lifecycle scenarios wait on product cadences, timers and deadlines rather than on the CPU: in the CI
run of 2026-10-09 the 33 runs the stress selection then held took 854 seconds one at a time,
against 885 seconds of run-slot time for the same runs at four at once in the active-only lane. One
at a time, a stressed lane therefore costs the sum of its scenarios' durations, and the 44 scenario
runs and 36 paced-driver runs not yet stressed took 1,587 and 658 seconds in the same run. One lane
holding all of them one at a time would have needed about 52 minutes of scenarios after 11 minutes
of diagnostic builds, about 63 of its 80 budgeted minutes, before the variation between CI runs,
before the large-state WASM handoff added since, and before any scenario added later. Chunking that lane into a fresh process per
feature keeps the sum and adds a process start-up per chunk. Running more stressed scenarios at once
in one process, or fewer nodes in each of several concurrent scenarios, is the configuration whose
nodes missed product deadlines then.
The restore scenarios are the longest family, so they became their own selection: a budget and a
runner of their own, and the same build, disturbance and probes. In the CI run of 2026-10-10 that
introduced the split, `deloxide-stress` took 28m41s in the lane's step and 27 minutes inside its
budget: about 9.5 minutes of diagnostic builds, 6m19s for the 35 stressed lifecycle runs and
10m51s for the 36 paced-driver runs, with 30 qualifying evidence files and no finding.
`deloxide-stress-restore` took 39m40s in its step and 37 minutes inside its budget: 5.5 minutes of
builds and 31m30s for the 43 restore runs, with no finding. One lane holding both would have needed
58.5 of its 80 minutes, before any variation between runs. The disturbance delays one in
twenty nested mutex and write acquisitions by at most 200µs, and the product's physical and logical
deadlines are not widened for it. A stressed workload that misses a product deadline is a finding to
reproduce and fix in its owner, not a bound to raise.

Separating the stages prevents a build cache's warmth from deciding how much time the workloads
receive. On 2026-10-10 a shared 40-CPU host at a load of 36 to 50 spent 33m08s building the stressed
scenario binary; under the combined budget that left its slower restore workloads too little
time and the lane expired after 17 of 43 runs, all passed. A later loaded-host order run spent
25m35s building and reached its paced scenarios with only 108 seconds of suite time left after
the teardown reserve. Those builds now consume only the build stage. Workloads can still exhaust
their own budget on a saturated host; expiry identifies the stage and ends with status `124`,
never a clean result, and rerunning an unchanged failed workload is not a repair.

`just build-deloxide-lane <selection>` measures the same selected builds without running
workloads. Run it again to measure a warm local build directory; kache remains the configured
compiler wrapper in both runs. The retained record has verdict `built`, zero executed workloads
and only the build stage, so it cannot qualify as completed workload or native coverage evidence.
The stress selections share one diagnostic build; a measurement after the other stress selection
has built it is already warm for that shared work. Build-cache conditions and measured durations
belong with the task's retained evidence rather than an assumption that every first invocation
compiled dependencies from source.

### Two Stressed Scenarios In One Process

At two stressed scenarios in one process, the restore scenarios failed in two ways the
one-at-a-time selections never did: `the cluster is restarted` found a restored node still stopping
at the end of its 300-second watchdog, and node startups spent their whole 84-second budget.
Neither was a deadlock among tracked locks. Every evidence file of those runs qualified with no
finding, and both failures are a node that could not get through terminal teardown, or through
startup, beside the other nodes of its process.

The retained run of `tests/features/cluster/backup_materialized.feature` at `6c0d23de` holds the
signature. Its 28 stops by `the cluster is restarted` took 130.5 seconds at the median and 297.7 at
the slowest, 2.3 seconds inside the watchdog, where the plain diagnostic build of the same revision
took 0.9 and 39.6. Of its 129 node stops, 69 never reported the end of terminal teardown. Three
things made a stop that slow, and CPU contention is what turned them into a failure: the run shared
a 40-CPU host whose one-minute load was 47 at the median and between 28 and 75.

- **Every node swept its whole acknowledgement registry every second, and a sweep that overran
  the second could not be cancelled.** The remote acknowledgement silence sweep took a guard for
  each of a node's 16,384 correlation positions whether or not the node had forwarded anything, and
  a stress build enters Deloxide's one detector twice for each of them. On that host one sweep took
  longer than the second between sweeps. Its task then found the next tick already due and went
  from one sweep into the next without waiting, and the cancellation that runtime teardown joins the
  task with is observed only when the task waits. The run's first cleanups show it: four nodes that
  had served for under ten seconds logged nothing through 59 seconds of terminal teardown while the
  process kept three CPUs busy. The sweep now visits only the positions a node's deliveries and
  admissions have claimed, which keeps it far inside its second, and its task returns to its
  executor after every sweep, so teardown's cancellation ends it after the sweep in progress even
  when that sweep overruns; see [Terminal Teardown](./shutdown.md#terminal-teardown).
- **Nodes the harness gave up on kept running.** A cleanup that reached its deadline aborted the
  node's root task, which ended that one future and left every task the node had spawned, its
  sweep among them, running for the rest of the process. A node aborted this way never reported
  the end of its teardown. The harness never aborts a node now, as
  [Node Tasks And Readiness](#node-tasks-and-readiness) describes.
- **A stopping domain waited for its generators one at a time.** When every node of a cluster stops
  at once, a generator that is reading its source relay from another node, or publishing to one,
  waits on a peer that no longer answers, and runtime teardown gave each such generator its own
  two-second grace in turn. This one is independent of the detector: it is in the plain build's
  39.6 seconds too. The generators of a domain now share one grace; see
  [Terminal Teardown](./shutdown.md#terminal-teardown).

The measurements below repeat that run's configuration on a 32-CPU workstation shared with other
builds: the whole feature in the `deloxide-stress` build, two scenarios in one process, sixteen
runtime workers on sixteen CPUs, no retries, and the row, startup, stop and cleanup deadlines
unchanged.

| Build | Outcome | `the cluster is restarted` stops, median and slowest | Slowest node startup |
| --- | --- | --- | --- |
| `6c0d23de`, retained from the 40-CPU host | Ended by its 3000-second budget with 17 of 18 complete and one failed | 130.5 s and 297.7 s | 29.5 s |
| `c39c5388`, after the sweep and the harness changed | 18 of 18 passed in 422 s | 0.49 s and 36.9 s, the slowest all generator graces | 2.1 s |
| With the generators' shared grace | 18 of 18 passed in 376 s | 0.36 s and 10.9 s | 2.6 s |

Without competing load, `6c0d23de` itself is as fast here as the current revision: in the
feature's first four examples its restarted clusters stopped in 0.06 seconds at the median and
0.48 at the slowest. The sweep's cost shows only once the process has to compete for its CPUs. A
build of `c39c5388` with nothing changed but the full sweep restored was run beside an unchanged
one, the process confined to four CPUs that twelve busy loops competed for. The unchanged build
passed 18 of 18 in 443 seconds. The build with the full sweep took 1,301 seconds and 1.7 times
the CPU time, failed both three-node examples with forty relays when deliveries to a peer timed out
during their input rounds, and stopped its restarted clusters in 5.3 seconds at the median against
0.36. `6c0d23de` itself, run the same way, matched that build and not the unchanged one: 1,345
seconds, the same two examples failed, one of them with the missing rows of the retained run, and
its restarted clusters stopped in 4.5 seconds at the median.

What a three-node stop still costs after this is bounded by the deadlines of the calls its nodes had
in flight when the stop began, and each wait is named in the node's log. A relay owner that is
delivering to a peer which has already stopped waits out the five seconds that delivery's admission
is given and the five its cancellation is given. A checkpoint announcement to a replica that has
stopped is waited for until its five-second dispatch deadline has passed. A replica's placement task
that is fetching or acknowledging a checkpoint when its owner stops does not observe the stop, and
such tasks are aborted one after another, two seconds each. The node's generators share their two
seconds. In `Every node stopping at once ends its generators together`, the slowest node of the
three-node cluster took between 2.0 and 12.0 seconds to stop in ten runs of the ordinary build.
Before the generators shared their grace it took between 22.0 and 30.1 seconds in five of six runs,
and 8.1 in the sixth, where two of the nodes had no generator waiting on a peer.

The stress selections keep running one scenario at a time: their budgets were measured that way,
and two per process remains the comparison below rather than a lane.

For a focused comparison, `just test-diagnostic-scenarios <selection> <feature> --name
<scenario> --concurrency 2 --retry 0` builds and runs the public scenario in `deloxide`,
`deloxide-order` or `deloxide-stress`, with the lane's prerequisites and ordinary CLI. Its builds
share the diagnostic target directory. The recipe resolves its feature file to an absolute path:
Cucumber's relative input glob walks the entire working directory, including build artifacts,
before starting any scenario. The absolute input glob matches the exact file in its containing
directory. Argument boundaries, including a scenario name with spaces, are preserved. Each attempt
retains its output under `target/deloxide/focused/<selection>/run.*` and rejects an empty scenario
selection. It records detector evidence there unless `NERVIX_DEADLOCK_EVIDENCE` names another
directory. Use a fresh evidence directory for each run and retain the standard output, scenario
log and suite summary together. The complete lane remains the entry point for inventory accounting
and diagnostic evidence qualification.

In this comparison, `--concurrency` counts scenarios in one process. Their in-process nodes share
one runtime and one detector; the runtime still has one worker per available CPU. Record the
available CPU count, affinity and host load, and keep them the same for the plain and stressed
selections. A failure's phase matters: missing generator rows immediately after restore and missing
rows after a subsequent restart exercise different installation boundaries. Preserve the first
failure, the missing relay and tenant identities, and any stop or startup deadline outcome before
running a control. A passing control or replay describes that run only.

The oversized materialized-generation assertion preserves its missing relay and tenant pairs,
expected round and value length, domain, and subscription node when its row deadline expires.
Before teardown it queries `SHOW RELAY ... MATERIALIZED STATE` on at most three nodes for the first
four missing relays. The observations share a ten-second budget, with at most two seconds per
query. They keep the placement, branch, round, value byte length and watermarks, without printing
the fixture's large values. Query failure or timeout stays a diagnostic observation; the original
exact-row assertion still fails. This observation budget starts after the row deadline and grants
no additional time for a missing row to satisfy the assertion.

`just test-diagnostic-lib <selection> <test>` runs one exact library owner test in `deloxide`,
`deloxide-order` or `deloxide-stress`. The owner starts its diagnostic process before
creating runtime workers or acquiring tracked locks; isolated children inherit the attempt's
evidence directory and retain their own process records. Each attempt keeps its output under
`target/deloxide/focused/<selection>/library.*`. The command requires completed, non-ignored tests
and a fresh evidence directory, and qualifies every retained record. Each invocation selects one
owner, since the detector installs once per process. Ordinary library selections use `just
test-runtime`. This focused observation does not replace the complete lane's workload inventory
and accounting.

Each registered `owner-tests` workload uses the fresh process the canonical lane launches and
records one process observation. The ONNX queue-recovery body runs directly in that process; its
ordinary test uses a supervised child to establish native-library isolation within the full unit
suite.

`just test-onnx-owner-binary <binary> <library> <filter>` replays a compiled inspection owner
against an explicit runtime library. Retain the binary's source and execution mode, the library
digest, the selected test and the process evidence with the replay.

`just build-diagnostic-scenarios <selection>` prepares library, scenario and server executables for
a supervised replay. Capture their source, mode and artifact identities before replaying the
selected workloads.

ONNX fixture preparation hands native-library loading, directory creation and model copies to
`off_the_runner`; a replacement model copy uses the same owner. The scenario publishes its fixture
placeholder after directory preparation completes and awaits any replacement before its next step.
The runner continues polling other scenarios and the suite watchdog during that work. This is
harness preparation: the node's subsequent metadata inspection retains its own thirty-second
deadline, bulk admission and native-session ownership.

`just test-diagnostic-harness <selection> <filter>` runs focused harness regressions in the chosen
diagnostic build. `just coverage-harness-liveness <filter>` measures ordinary harness regressions
in a separate target directory and retains its log and `lcov.info`, including the harness source,
in a fresh `target/harness-coverage-reports/run.*` directory. Both commands reject a selection
that completed no tests or left selected tests ignored. These counters measure the harness itself
and do not join product coverage or qualify a diagnostic lane.
After replaying an instrumented ordinary scenario binary from that target directory without a
source edit, `just coverage-harness-liveness-report <output>` exports the retained profiles without
another test invocation.

Public materialized-state inspection can read installed or checkpoint state and can forward an
empty non-owner observation to the primary. Record the addressed node and returned placement;
rows returned by a replica's endpoint alone do not prove that replica installed the generation.
Correlate the observations with the restore execution report, runtime log and lifecycle snapshot
before deciding which publication or delivery owner needs a regression.

A debugger attached to this process suspends every in-process node while it collects thread
stacks. Record that suspension separately and retain the attempt as a diagnostic run; its elapsed
time and deadline outcomes cannot qualify an uninterrupted throughput comparison. Preserve an
unprofiled failure before collecting stacks on a diagnostic replay. Thread stacks can identify
workers blocked in a tracked lock or the detector itself, but a suspended async task is not a
worker's call stack. Retain the node's lifecycle and placement observations alongside the dump
when deciding whether the failure belongs to drain, state publication or scheduling contention.

When a step's `timeout` expires it sends `SIGTERM` to the step's process group. The collector
records its attempt as interrupted and forwards the signal to the lane, which ends the process it is
running with that process's whole group, records `interrupted` in `lane.json` and exits; the upload
steps still run. A failed or interrupted attempt is kept as it is: a later run, or a warm rerun,
creates a new attempt and never qualifies the one that failed.

## How Failure Reaches CI Output

The job log carries the scenario binary's standard output and standard error, and the `scenario-logs`
artifact carries `tests/logs`.

| Output | Where | What it holds |
| --- | --- | --- |
| Cucumber report | Standard output | Every scenario and step result in order, the world of a failed step, the summary, and every failed scenario repeated at the end |
| Harness transitions | Standard error | Node startup transitions and outcomes, the stops scenarios waited for, and a suite timeout's diagnostic and cleanup |
| Scenario log | `tests/logs/cucumber.log` | The run's parallelism and suite budget, cluster start requests and failures, the Plexus statements scenarios run, every phase marker, teardown diagnostics and context, unfinished cleanups, and the suite timeout diagnostic |
| Suite summary | `tests/logs/suite-summary.md` and the CI job summary | Length, budget and remaining margin, run-slot utilization and scenario work, retries, the runner task's tick lateness and stalls with its thread's CPU and run-queue time, how busy the runtime workers were, wait time by reason, the ten longest attempts, and every attempt that reached two minutes without declaring itself long-running; captured at watchdog expiry before cleanup on a timed-out run |
| Runner load | `runner-load-tests` and `runner-load-scenarios` artifacts | Five-second CPU utilization and steal-time samples and their mean and peak |
| Node traces | `tests/logs/scenarios.log` | The trace output of every in-process node of the run |

Server processes write their logs into their own temporary directories, and a failure quotes the
tail it needs.

The materialized input round and the delayed-coordinator publication observation each own a
`BackupPhase`. It captures the existing 120-second deadline before dispatching the operation through
`observe_off_the_runner`; a runner delayed while polling other scenarios does not stop the input
round from submitting its next row or the observer from recording a reached barrier. Reading the
result later does not restart that deadline. Dropping the result future aborts its operation, and a
real expiry still fails the step with its identity, budget and elapsed time. Producer close remains
outside the input round's budget, as before.

Input diagnostics retain domain, initial route, producer, attachment, generation, round, tenant,
attempt and submission identity. A submission is logged separately from its ACK wait and terminal
outcome. While an ACK is pending, a five-second progress observation records connection, admission,
producer ending, pending submissions and remaining round budget. It does not resubmit unresolved
work. Only a reported processing failure takes the fixture's existing replay path, preserving every
row's event timestamp and complete value; the intentional successful replay also remains.

The delayed-coordinator materialized restore fixture arms the existing publication hook on every
candidate node, then captures the first coordinator that actually reaches it within that phase's
deadline. The background task retains its immutable execution reference with its handle. Sending,
reply and barrier observations retain that reference, route, archive size, candidate nodes and the
coordinator that reached publication. A terminal background outcome before that barrier is reported
immediately. The fixture releases the unused hooks before transferring leadership, keeping only the captured
coordinator held while its successor completes the same immutable restore and produces new rows.
Releasing that captured hook must be refused and must leave the complete materialized generations
identical. The controlled first-route scenario moves leadership before submission as well; a saved
route does not identify the coordinator that admits the restore.

Exact completed-restore replays in the native metadata fixture use the existing bounded outcome
recovery helper. Each attempt retains the archive bytes, statement and execution reference while
following leader changes; a saved node name is only the first route. The leadership-change scenario
forces the saved route to become a follower and checks Completed/Recovered, every applied step and
the identical complete report after restart. Raw redirect scenarios still observe a single reply.

The large materialized workload records each command before sending it and its elapsed time after
completion. A client failure includes that statement, domain, elapsed time and complete typed
report. An uncertain command therefore remains distinguishable from a rejected command or a
runtime revision that failed preparation; the fixture does not resubmit it under another identity
or extend the client's deadline.

| Failure | How it surfaces |
| --- | --- |
| A step assertion | The step fails with its message; Cucumber retries the scenario and repeats the failure at the end of the report |
| A status wait that expired | The step fails with the elapsed time, the last status text, and the last typed failure |
| A node that never became ready | The step fails with every attempt's record, and a failed construction logs `cluster start failed:` |
| A teardown diagnostic that failed or timed out | The node's `status_error=` or `status_timeout=` line, and cleanup continues |
| A node a scenario stopped that outlived its watchdog | The step fails with the node's stop record: how long it had been stopping, the shutdown phase it was in, and whether its own deadline had passed |
| A cleanup that left nodes stopping, or a node that panicked while stopping | `scenario cleanup unfinished:` lines naming each node's shutdown phase, or `scenario teardown failed:` lines, beside an unchanged scenario result, and the keeper's `ended ... after it was left stopping` line on standard error |
| A scenario that never ends | The suite timeout diagnostic names it with its attempt, phase, phase age, and nodes |
| An exhausted port pool | The step that needed a port fails with the pool's report |

### Exit Status

| Ending | Status |
| --- | --- |
| Every scenario passed and the dependencies stopped cleanly | `0` |
| The run finished with failed steps, parsing errors, or hook errors, reported as a panic naming each count | `101` |
| The run finished but its dependencies failed to stop or were abandoned | `101` |
| The suite budget expired | `124` |
| A `deloxide` build could not start its deadlock diagnostics | `4` |
| A `deloxide` build reported an active deadlock and recorded it | `3` |
| A `deloxide` build failed output/recording, exceeded a diagnostic budget, or lost findings/context | `4` |
| A `deloxide-order` build retained a potential cycle | Workload continues; unreviewed evidence cannot qualify, and the [diagnostic lane](#diagnostic-lane) fails |

`124` is the status `timeout(1)` reports. It differs from `0`, from `1`, and from the `101` a panic
ends with, so a wedged suite is told apart from a passing or a failing one by its exit status alone.

## Adding A Harness Operation

The append-setup regression observes each live source's test-only setup counter before issuing a
burst of public domain creations with one voter stopped. It checks that both survivors apply the
burst, that attempts to the departed voter fit OpenRaft's existing 200 ms retry series plus two
boundary attempts per source, and that the returning voter catches up. Its count bound uses the
whole observation's elapsed time, so parallel-suite delay does not tighten the assertion. A zero
attempt observation fails rather than claiming that pacing was exercised. The counter shares the
probe's existing map with successful opens and is absent from the ordinary production recorder.

A new operation that starts, observes, or stops something on the harness's behalf follows the same
contract.

1. **Find its phase.** An operation inside a phase, such as a startup attempt, a status wait, or a
   cleanup, takes that phase's deadline as a parameter and nests its own budget in it. Only a new
   phase starts a deadline of its own, and never inside a loop that retries within a phase: a retry
   that restarts its budget turns a bounded wait into an unbounded one.
2. **Derive its budget.** Name its policy inputs, usually the slowest healthy duration measured with
   the whole suite at the CI concurrency factor and the headroom that multiplies it. Derive the
   budget from them with constant arithmetic, and assert its order against the budgets around it:
   shorter than the phase that repeats it, and inside the cleanup budget or the reserve when it runs
   during cleanup. Record the measurement beside the input.
3. **Bound it by what it owns.** An operation that owns nothing runs within the deadline, or is
   polled until it, keeping its last output and last failure. A wait for something its owner must
   keep borrows it and leaves it with that owner when the deadline passes. A task that spawned
   nothing of its own may be aborted and then joined; a node is never aborted, because the abort
   would end only its root task. A loop yields on every iteration and pauses only until the
   deadline.
4. **Keep its failure typed.** Return a report of a semantic error enum the module owns, name the
   operation a passed deadline interrupted, and read state from typed variants. Never reduce an
   outcome to a boolean, infer a state from whether a message exists, or report an unknown reason.
   Failures carry no credentials and no payload values.
5. **Never block the runner task.** Every scenario and the suite watchdog share it. A synchronous
   loop needs a bound of its own, as the port draw has, and blocking work belongs on a blocking or
   spawned task: a step hands its file, encoding, digest or database work to `off_the_runner` in
   `tests/common/runner_offload.rs` and awaits it, and the
   [runner-lag observer](#runner-stalls) logs a step that still holds the task. A duration a scenario injects for a product deadline to race is timed off the
   runner task, as [backup cut holds](#product-deadlines-and-harness-deadlines) are at their gate,
   because another scenario can still stall the runner task.
6. **Make cancellation release it.** Whatever a scenario starts belongs to the scenario's world or
   its cluster, so dropping the run releases it: spawned work behind handles that abort on drop,
   child processes killed on drop, and node tasks through the single task owner, which asks a node
   that still runs to stop and leaves it stopping under a keeper. A new kind of node publishes its
   stop to the live-cluster registry through a registration its task owns, and its cleanup belongs
   to the `stopping` phase. Ports, faults, proxies, and storage go back only after the tasks that
   used them have ended.
7. **Report it.** Transitions a reader needs while a run is still going go to standard error or to
   the scenario log with the scenario's identity. A failure fails its step with the typed error's
   message, and diagnostics gathered on a failure path stay within the diagnostic budget.
8. **Test it.** Add a focused regression to `tests/harness_liveness.rs` that drives a stand-in
   endpoint or task, on a paused clock where the budget is long, so the bound is proven without
   production-scale waits, and run `just test-harness-liveness`. Run the affected features with
   `just test-scenarios --input <feature>` at the CI concurrency factor,
   `NERVIX_TEST_CONCURRENCY_FACTOR=3`.
9. **Keep this chapter current** in the same change.

## Guarantees And Limits

The harness guarantees:

- Every status request, status wait, readiness wait, node startup, teardown diagnostic, cluster
  cleanup, and post-run teardown has one absolute deadline, and no retry or poll restarts it.
- No diagnostic, timeout, or cleanup path aborts a node task or leaves one unowned. A node that
  outlasts a wait stays with its owner or with the keeper it was handed to, and keeps its storage,
  its ports and its registrations until its task has ended.
- No node is launched over one that is still stopping, so a launch never meets a database lock its
  own predecessor holds.
- A cluster's cleanup costs one budget whatever its size, and releases what a node held only after
  that node's task has ended.
- `finished` is published only after cleanup has completed.
- A gate a step holds is released its injected duration after the hold observed it, however long
  the runner task is stalled.
- A server process launch reads every range its executable maps, in file order, within its startup
  deadline and before it spawns the process, and a failed startup is described before anything
  kills the process.
- A server process launch that another socket kept from binding a drawn listener is launched again
  on fresh ports for that listener within its startup's deadline, at most three times, and every
  other ending of a launch fails its startup at once.
- No socket bound to port zero and no outgoing connection takes the port of a ZeroMQ or syslog
  address a scenario's nodes bind, from its draw until cleanup, and the first statement that names
  the address names a port no other socket listened on, or held over UDP, when the harness checked
  it.
- The run ends within its budget, the cleanup window, and the bounded teardown, with a status that
  tells a timeout from a failure.

Its limits:

- Budgets bound what the harness waits for, not what the operating system schedules. A starved
  runner delays the checks that end a wait.
- The watchdog is polled on the runner task, so it cannot end a synchronous stall of that task. The
  job's limit is the guard that remains.
- Provisioning a Kafka topic's partitions and waiting for a consumer group's members query the
  broker synchronously on the runner task, for up to 5 seconds per query, and every scenario of the
  run waits while one of those queries runs.
- Cleanup and stops give back a stopped node's consensus fault registration on the runner task, so
  when no step still holds an observer of the node's consensus state, the node's consensus database
  closes there, and every scenario of the run waits until the close has synchronized the journal and
  the database's last background worker has left.
- Kafka committed-offset assertions query on a blocking worker with a one-second request timeout.
  A query error retries within the assertion's original deadline; persistent errors fail rather
  than count as an offset observation. An offset that crosses a forbidden threshold fails
  immediately, and a below-threshold assertion completes only after a successful observation at
  the end of its window.
- The watchdog runs no after hooks, so a node held by a fault its scenario injected can outlast the
  cleanup window and ends with the runtime.
- Nothing in the process can end a node that does not end itself: in-process nodes share one
  runtime with the harness. A node left stopping that never finishes its shutdown keeps running,
  with its storage and ports, until the process drops the runtime.
- The time a scenario spends queued for permits and closing the browser have no budget of their own,
  and some ordinary step waits check their bound only between requests. The suite budget bounds all
  of them.
- A construction's convergence checks are separate 40-second waits after its startup deadline.
- The suite budget's derivation holds with no margin.
- Reading a server executable keeps nothing in memory. Under memory pressure the kernel may evict
  what the read brought in before the process touches it, and the shared libraries a server loads
  are not read.
- A process observation reads procfs at one moment. A thread's wait channel names the kernel
  function it waits in, not the call in the process that led there, and a futex wait does not say
  whether the thread waits for a lock or for work.
- A server process says why its startup ended only in the text of its report, so a lost bind is
  recognised by comparing the report's outermost context with how the node's application error
  renders. Until the process binds its gRPC port, a readiness probe dials whatever holds that port:
  a socket that took it can hold the probe until its request deadline, and another node that took
  it answers the probe in the process's place.
- A socket that binds a node-bound address's port explicitly with `SO_REUSEADDR`, or over UDP,
  after a statement named the address still fails the node's bind, because the address can no
  longer move. Outside Linux the move names no holder, because the kernel's socket tables cannot be
  read.

## Qualification Evidence

`just test-harness-liveness` runs the focused regressions that hold this contract. They drive
stand-in session services and a stand-in NATS server on real loopback sockets, stand-in node
tasks, most of them on a paused clock, stand-in executable images and stand-in child processes. The
`tests` job runs them beside the separate scenario job.

| Contract | Regressions |
| --- | --- |
| A status request ends at its deadline in the operation that stalled, and every probe outcome keeps its cause | `status_request_ends_at_its_deadline_while_the_connection_is_pending`, `status_request_ends_at_its_deadline_while_session_establishment_is_pending`, `status_request_ends_at_its_deadline_while_the_response_is_pending`, `status_request_ends_at_its_deadline_while_unrelated_responses_remain_ready`, `readiness_and_status_outcomes_retain_their_typed_cause` |
| A wait ends at its original deadline | `status_wait_ends_at_its_original_deadline_when_the_final_request_never_replies`, `nested_deadline_never_outlives_its_phase`, `startup_readiness_failure_reports_the_timed_out_status_operation` |
| Node task outcomes are distinct, the task is joined once, and its owner never aborts it | `running_task_reports_the_last_failed_probe_at_the_deadline`, `clean_application_exit_before_readiness_is_terminal`, `application_error_before_readiness_is_retained`, `panic_and_cancellation_have_distinct_terminal_outcomes`, `stop_and_keeper_paths_use_the_task_inspected_for_diagnostics`, `a_wait_past_its_deadline_leaves_the_task_running_with_its_owner` |
| One startup budget with classified retries | `repeated_readiness_failure_spends_one_budget_across_every_attempt`, `a_node_that_does_not_stop_ends_the_startup_instead_of_being_launched_over`, `exhaustion_reports_every_attempt_with_its_typed_cause`, `ports_that_cannot_be_reallocated_end_the_startup`, `a_bound_address_is_retried_until_a_launch_becomes_ready`, `a_last_attempt_still_becomes_ready_with_what_the_budget_left`, `an_application_error_ends_the_startup_without_another_launch`, `a_panicking_node_ends_the_startup_without_another_launch`, `a_launch_failure_ends_the_startup_before_anything_is_cleaned_up`, `sequential_cluster_construction_stays_inside_its_derived_budget` |
| Failed-attempt cleanup spends the remaining startup budget and its joined result determines retry and listener selection | `a_failed_attempt_finishes_its_owned_cleanup_before_retrying_with_the_remaining_budget`, `an_initialization_failure_observed_during_cleanup_keeps_its_terminal_cause`, `a_bind_failure_observed_during_cleanup_moves_only_its_own_listeners` |
| An armed-fault startup retries only a lost early listener bind inside its original readiness budget and launch limit | `armed_startup_retries_only_the_colliding_listener_before_initialization`, `armed_startup_retries_no_other_failure`, `armed_startup_keeps_one_readiness_budget`, `armed_startup_retains_every_collision_when_the_launch_limit_is_spent`; public scenarios `A paced follower recovers an occupied listener before its catch-up start` and `An interrupted snapshot installation finishes on the next start` |
| Diagnostics are concurrent and never keep cleanup from starting | `status_snapshots_keep_a_healthy_node_while_another_node_stalls`, `failed_and_stalled_diagnostics_end_by_their_deadline_so_cleanup_starts`, `a_stalled_diagnostic_still_reaches_every_node_stop_in_a_cluster_of_one_and_of_three` |
| One cleanup budget per cluster, and truthful phases | `stuck_nodes_spend_one_cleanup_budget_in_a_cluster_of_one_and_of_three`, `a_node_left_stopping_keeps_what_it_holds_until_its_own_task_ends`, `a_single_node_cleanup_keeps_how_its_task_ended`, `a_panicking_node_is_the_only_cleanup_failure_a_three_node_cluster_reports`, `the_finished_phase_is_published_only_once_cleanup_has_completed`, `an_active_scenario_publishes_its_phase_and_the_age_of_that_phase` |
| A stop a scenario waits for is timed by the node's own ending, and a node that outlives a wait is named with its shutdown phase | `a_stop_is_timed_from_its_first_request_to_the_ending_of_the_node_task`, `a_node_that_had_ended_before_it_was_asked_to_stop_took_no_time_to_stop`, `a_stopping_node_is_read_with_its_shutdown_phase_and_its_own_deadline`, `a_stop_record_reads_as_the_transition_the_harness_prints`, `a_node_left_stopping_is_recorded_with_where_its_own_shutdown_stood` |
| Each retry holds twice the share of the run before it, from a sixteenth of the pool and never more than a quarter; a retry keeps the slots that free up until its share fits, gives its whole share back when it ends or is cancelled, and the suite summary counts the slot time it holds beyond one slot | `each_retry_holds_twice_the_share_of_the_run_before_it`, `a_retry_holds_its_share_of_the_run_slots_until_it_ends`, `a_retry_waiting_for_its_share_keeps_the_slots_that_free_up`, `a_cancelled_retry_returns_its_whole_share`, `the_suite_summary_counts_the_slot_time_a_retry_holds_beyond_one_slot` |
| Feature waits do not occupy run slots, and limited chains start before and progress beside bulk work | `a_queued_web_console_scenario_does_not_hold_a_run_slot`, `limited_features_are_taken_up_before_the_bulk`, `the_next_limited_scenario_gets_a_slot_beside_bulk_work`, `releasing_a_limited_scenario_hands_its_slot_to_the_next_in_its_chain`, `each_web_console_feature_starts_before_one_feature_consumes_the_group` |
| A declared long-running scenario is admitted ahead of ordinary first attempts and behind retries, runs beside the bulk instead of after it, and the suite summary names every undeclared attempt of two minutes, finished or still running | `a_declared_long_scenario_is_admitted_before_queued_ordinary_first_attempts`, `a_retry_runs_before_a_declared_long_first_attempt`, `a_declared_long_scenario_queued_last_runs_beside_the_bulk_instead_of_after_it`, `long_running_is_declared_by_a_tag_of_the_scenario_its_rule_or_its_feature`, `an_undeclared_attempt_of_two_minutes_names_its_scenario_long_running`, `the_suite_summary_names_every_undeclared_attempt_of_two_minutes_finished_or_running` |
| The port pool is bounded and gives ports back | `a_draw_that_keeps_landing_on_reserved_ports_ends_at_the_draw_limit`, `an_exhausted_draw_gives_back_the_ports_it_had_reserved`, `a_draw_the_operating_system_refuses_is_reported_as_its_own_failure`, `ports_drawn_from_the_operating_system_are_distinct_and_reserved`, `a_released_port_can_be_drawn_again` |
| An address a node binds keeps its drawn port off every ephemeral pick and out of its node's way, moves off a port another socket took at its first use and only then, names what held that port, and gives back every port it drew | `a_held_port_is_no_ephemeral_pick`, `a_node_listener_binds_beside_the_hold_on_any_loopback_address_and_after_a_restart`, `naming_a_free_address_binds_its_port_once_and_keeps_it`, `naming_moves_an_address_off_a_port_another_listener_took`, `naming_moves_a_syslog_address_off_a_port_a_udp_socket_took`, `a_zeromq_address_keeps_a_port_only_a_udp_socket_holds`, `a_refusal_other_than_a_taken_port_fails_and_leaves_the_address_unnamed`, `an_address_whose_every_port_is_taken_ends_at_the_draw_limit`, `release_gives_back_every_port_the_address_drew`, `the_listing_names_a_listener_on_the_port_and_its_process`, `a_socket_table_row_reads_its_address_state_owner_and_inode` |
| An HTTP receiver answers as scripted, records what it cannot capture, and stops within its budget | `the_receiver_captures_requests_and_answers_its_script_in_order`, `a_lost_response_is_captured_and_the_connection_closes_without_an_answer`, `chunked_bodies_interim_responses_and_raw_bytes_are_served_as_scripted`, `held_responses_and_stalled_bodies_end_within_the_stop_budget`, `requests_awaiting_a_response_are_counted_until_their_final_head_begins`, `a_client_that_leaves_an_unfinished_response_abandons_it`, `a_wait_for_a_request_line_ends_once_that_request_is_captured`, `requests_beyond_the_receiver_bounds_are_faults_not_captures`, `a_tls_receiver_offering_http2_captures_and_answers_requests_over_http2`, `a_tls_receiver_accepts_the_client_certificate_it_issued_and_refuses_others`, `a_tls_receiver_is_refused_by_a_client_that_dials_a_name_its_certificate_lacks`, `every_documented_script_form_parses_and_unknown_forms_are_refused` |
| A gRPC receiver answers as scripted, records what it cannot capture, and stops within its budget | `the_grpc_receiver_captures_calls_and_answers_its_script_in_order`, `a_lost_grpc_answer_is_captured_and_its_connection_closes_without_one`, `held_grpc_calls_end_when_the_client_resets_them_or_the_receiver_stops`, `a_request_that_is_not_one_bounded_message_is_a_fault_not_a_capture` |
| A real-process cluster converges only once every node answers and reaches every other node, a relaunched voter through a target newer than the one its killed process had and on the port its new process answers on, and a wait that cannot converge names why | `a_relaunched_member_converges_only_once_every_peer_reaches_its_new_process`, `a_member_that_had_stopped_observing_the_exited_process_converges_on_its_first_target`, `a_started_cluster_converges_once_every_member_reaches_every_peer`, `a_one_member_cluster_converges_without_peers`, `a_member_that_stops_answering_keeps_the_cluster_from_converging`, `a_status_line_convergence_cannot_read_ends_the_wait_with_its_member_and_line`, `an_expired_wait_names_the_condition_a_member_did_not_meet`, `convergence_ends_at_its_budget_with_what_had_not_converged_and_what_each_member_said`, `a_relaunch_relaunches_nothing_while_a_running_member_has_not_reported_its_targets`, `a_failed_relaunch_keeps_its_cause_beneath_the_member_it_relaunched`, `a_relaunched_member_is_asked_where_its_new_process_answers` |
| A server process launch reads exactly the ranges its executable maps, refuses an image it cannot read, and reads nothing once its deadline has passed | `a_preparation_reads_each_mapped_range_once_and_nothing_else`, `the_server_executable_maps_less_than_its_whole_file`, `a_file_that_is_not_an_elf_image_is_refused`, `a_file_shorter_than_an_elf_header_is_refused`, `a_program_header_table_outside_the_file_is_refused`, `program_headers_of_another_size_are_refused`, `a_mapped_range_past_the_end_of_the_file_is_refused`, `a_preparation_whose_deadline_has_passed_reads_nothing` |
| A server process report names the listener another socket held only when its outermost context is a lost bind, read without its emphasis | `a_report_names_its_outermost_context_without_its_emphasis`, `the_last_report_a_launch_wrote_is_the_one_read`, `a_log_without_a_report_names_no_context`, `control_sequences_are_removed_and_the_text_between_them_is_kept`, `a_report_of_a_lost_bind_names_the_listener_the_next_launch_moves`, `a_report_of_any_other_failure_names_no_listener` |
| A failed server process startup keeps what its readiness probes saw and what the still-running process was doing, within a bound | `a_history_without_probes_says_none_completed`, `consecutive_probes_of_one_kind_form_one_run`, `a_kind_that_returns_starts_a_new_run`, `a_long_history_keeps_its_first_and_latest_runs`, `a_sleeping_child_is_described_with_its_thread_and_its_host`, `a_stopped_child_is_described_as_stopped`, `a_reaped_child_is_described_as_ended`, `a_description_lists_a_bounded_number_of_groups_and_names`, `a_description_orders_groups_from_the_largest` |
| Synchronous step work leaves the runner task polling, and a stall of that task is measured | `a_step_job_leaves_the_runner_task_polling_other_scenarios`, `a_step_job_returns_its_result_to_the_step`, `a_step_job_fails_its_step_with_its_own_panic`, `a_step_that_blocks_the_runner_task_is_reported_with_its_length` |
| The actual Syslog TLS sender completes its handshake and full write while its runner stops polling it, under the unchanged one-second peer deadline | `a_syslog_tls_publication_finishes_while_the_runner_is_unpolled`, `a_syslog_tls_split_write_finishes_while_the_runner_is_unpolled` |
| Every tick's lateness adds up beside the stalls, and the summary reports the runner thread's CPU and run-queue time and the runtime workers' busy time | `the_lateness_of_every_tick_adds_up_beside_the_stalls`, `a_tick_below_the_bound_counts_as_lateness_and_not_as_a_stall`, `a_thread_schedstat_reads_its_cpu_time_and_run_queue_wait`, `the_runner_thread_reads_the_cpu_time_it_spent_since_it_was_named`, `the_suite_summary_adds_up_every_runner_tick_beside_its_stalls` |
| A stall lists only the scenarios that hold run slots, and a suite timeout counts only the scenarios still queued or running | `the_running_scenarios_are_those_that_hold_their_run_slots`, `a_suite_timeout_does_not_count_a_finished_attempt_as_active` |
| jemalloc purges freed memory on its background threads once the scenario process asks, so the runner thread does not wait for a purge | `freed_memory_is_purged_on_jemalloc_background_threads` |
| A step takes the messages a NATS observer's client already holds without a poll of the runner task for each one, in the order the server delivered them | `a_step_takes_buffered_deliveries_without_a_poll_per_message` |
| A bounded backup phase keeps progressing while its caller is unpolled, retains the original deadline and identifies a genuine expiry | `a_backup_phase_progresses_while_its_runner_is_unpolled`, `a_stalled_backup_phase_fails_at_its_original_deadline`, `an_expired_backup_phase_does_not_start_another_budget` |
| NATS driver diagnostics distinguish a deliberately overflowing subscription from a disconnected TCP peer, and a disconnect preserves all 32,768 deliveries already buffered | `an_observer_records_the_drivers_slow_consumer_notifications`, `a_disconnected_observer_keeps_its_buffered_burst_and_reports_the_connection_end` |
| The actual sequence/header assertion drains all 32,768 messages while its caller is unpolled past its deadline, preserves each oracle and still fails a stalled delivery at the original deadline | `a_buffered_sequence_meets_its_deadline_while_the_runner_is_stalled`, `a_sequence_keeps_every_message_and_header_oracle`, `a_stalled_delivery_still_fails_at_the_captured_sequence_deadline` |
| The suite watchdog names what was running, publishes timing before cleanup, and ends the run | `a_run_that_finishes_inside_its_budget_keeps_what_it_produced`, `a_suite_timeout_reports_before_it_drops_the_run`, `a_stalled_scenario_body_is_named_with_its_attempt_phase_and_nodes`, `a_stalled_teardown_diagnostic_is_named_by_the_phase_it_is_in`, `a_node_that_never_stops_is_named_at_the_end_of_the_cleanup_window`, `a_cluster_that_outlives_its_scenario_is_named_as_unclaimed`, `a_retried_scenario_publishes_which_attempt_is_running`, `the_suite_budget_is_injectable_and_defaults_to_the_suite_policy`, `a_timed_out_suite_is_reported_apart_from_a_passing_and_a_failing_one`, `a_failing_suite_ends_the_process_by_unwinding`, `a_dependency_stop_that_never_returns_is_abandoned_at_its_budget`, `a_dependency_stop_that_finishes_keeps_what_it_reported` |

The scenarios of `tests/features/cluster/server_process_ports.feature` have a listener of their own
hold a drawn port: at a single process's start, for its web console and for its gRPC listener, at
the bootstrap member of a three-process cluster, for its interconnect listener, and at a restarted
member, for its gRPC listener. A whole-cluster restart also holds two persisted peers' gRPC ports
and requires both peers to relaunch while all voters recover together. Each asserts that the
startup relaunched exactly once, moved exactly that listener, and binds another port for it.
A last scenario starts a process whose startup ends with another report, a web console certificate
without its key, and asserts that the startup fails with the process's own exit after one launch.

The scenarios tagged `@node_bound_port_taken`, in `tests/features/runtime/zeromq_ingestion.feature`
and `tests/features/runtime/syslog_support.feature`, have another socket take the port a scenario
drew for its ingest address before any statement names it: a listener with `SO_REUSEADDR` on every
address for the ZeroMQ address, and a UDP socket on every address for the syslog address, on one-
and three-node clusters. Each asserts that the ingestor becomes ready within 30 seconds and that a
message sent to the address the scenario names reaches its relay.

The high-parallelism qualification was recorded on 23 September 2026 for the change that landed as
`cec5f764`, at the CI concurrency factor of two scenarios per CPU with Cucumber's two retries unless
a run says otherwise. The local runs used a 24-CPU workstation shared with other builds, whose load
average stayed between 7 and 40, so 48 scenarios ran at once; CI used its 16-CPU runner, so 32 did.

| Run | Result |
| --- | --- |
| `just test-harness-liveness`, 20 repetitions | Every repetition passed, 48 regressions in 4.0 seconds each |
| `WASM processor restores guest state after cluster restart`, all three example rows, three runs with `--retry 0` | Every run passed. Every node became ready on its first attempt within 2.1 seconds, and the slowest cleanup took 8.7 of its 60 seconds, so none reached its budget |
| `tests/features/cluster/*.feature` | 206 scenarios passed in 500 seconds with 16 retries. 553 node startups: median 2.04, 99th percentile 22.0, and slowest 41.3 seconds; two became ready on their second attempt, after a 36-second readiness deadline whose last probe was a typed `Unauthenticated` reply. 200 cleanups, the slowest 45.1 seconds, so none reached its budget |
| `tests/features/runtime/wasm_*.feature` | 95 scenarios passed in 583 seconds with 18 retries. 288 startups, the slowest 17.4 seconds. Two cleanups reached the 60-second budget with a node still running, each recorded with that node and the scenarios live beside it, and both scenarios passed when retried |
| The whole suite with `--suite-budget 4m` | Exit `124` after 313 seconds: the 240-second budget, the 60-second cleanup window, and 13 seconds of dependency and runtime teardown. The diagnostic named all 49 registered scenarios with their attempt, phase, phase age, and nodes, and the cleanup named the one node still running at the end of its window |
| The whole suite with a deliberately failing scenario added | Exit `101` reporting `3 step(s) failed` after 1,676 seconds. The failing scenario failed all three attempts while the rest of the suite ran beside it, and each attempt reached `finished` within 0.32 seconds of its body ending. 3,926 startups, 99th percentile 4.14 and slowest 21.4 seconds; eight transient bind failures were relaunched on fresh ports and became ready. Six cleanups reached their budget with a node still running and were recorded |
| The whole suite | Exit `101` after 1,490 seconds, with 1,749 of 1,752 scenarios passing. The three that did not are throughput and deadline assertions that failed only under the shared machine's load and pass in isolation and in CI. 3,937 startups, the slowest 20.7 seconds; four cleanups reached their budget with a node still running and were recorded |
| CI `tests` job with `NERVIX_TEST_SUITE_BUDGET=4m` | The job ended itself in 16m30s. The suite started 11m54s into the job, and its budget expired with 32 scenarios active, 29 in their body and 3 stopping, each named. All 53 live nodes stopped within 9.96 seconds of the cleanup window, the process exited `124`, and the `test-logs` artifact was uploaded 2 seconds later |
| CI `tests` job of the merged revision | Passed in 34m15s. The suite started 12m01s into the job and ran 1,754 scenarios and 19,384 steps in 19m26s with 2 retries. 3,832 node startups, all on their first attempt: median 1.77, 99th percentile 4.91, and slowest 6.37 seconds. No cleanup reached its budget |

After every run, no `nervix-server` process, scenario binary, or dependency container remained. The
[harness liveness qualification
ledger](https://github.com/nervix-io/nervix/blob/main/tests/harness-liveness-qualification-ledger.md)
records each run in full, with its artifacts, the executable evidence for every item of the
qualification matrix, and the comparison with the run the job limit cancelled.
