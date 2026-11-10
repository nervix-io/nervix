# Metrics And Observability

Nervix exposes runtime graph metrics in two forms:

- raw Prometheus metrics at the observability server's `/metrics` endpoint
- owner-routed summarized metrics in `DESCRIBE` command output

The two surfaces use the same semantic labels where they overlap, but they use separate storage.
Prometheus receives raw counters and histogram buckets through a Prometheus registry for external
aggregation. `DESCRIBE` reads Nervix runtime state and includes derived values such as rates and
recent percentiles. Relay descriptions are routed to the scheduled relay owner; nonowners do not
maintain relay metrics.

[Errors And Diagnostics](./errors-and-diagnostics.md) defines the error information that may be
reported and its sensitivity and log-level rules. This chapter defines metric surfaces and labels.

## Observability Server

The observability listener exposes:

- `/livez`: process liveness
- `/readyz`: readiness once a leader is known
- `/metrics`: Prometheus text output for graph, command history, interconnection, and allocator metrics

Use the node's observability address, not the data-plane HTTP listener:

```bash
curl http://127.0.0.1:<observability-port>/metrics
```

Prometheus metrics are intentionally branch-aggregated. They aggregate across concrete relay
branches and do not include a branch key label. This keeps Prometheus cardinality bounded when a
relay is branched by high-cardinality values such as tenant, user, account, or device id. Branch
lifecycle metrics identify the declared branch, not the concrete key. A partitioned relay or processor's series
carry its partition, whose cardinality is bounded by the partition count its branch declares.

## Metric Labels

Graph metric series use these labels:

- `domain`: owning domain
- `target_kind`: runtime target kind, such as `RELAY`, `INGESTOR`, `JUNCTION`, `DEDUPLICATOR`, `REINGESTOR`, `WINDOW_PROCESSOR`, `EMITTER`, or `LOOKUP`
- `target`: relay or node name
- `physical_node_id`: Nervix cluster node where the metric was observed
- `direction`: `received` or `sent` for traffic, `state` for inferencer state
- `relay`: logical relay associated with the observation, or `-` when no relay applies
- `peer_kind` and `peer`: relay peer labels for node-to-relay observations, or `-` when no peer applies
- `partition`: the partition of a partitioned relay or processor recording the series, or `-`
  for a unit without partitions
- `instance`: the global index of an ingestor instance, or `-` for a non-instance unit
- `branch`: named branch declaration on branch lifecycle metrics
- `reason`: branch eviction reason, either `lru` or `ttl`
- `le`: Prometheus histogram bucket boundary

For a single-input emitter, sent metrics retain that input relay as `relay`. A multi-input
emitter's received metrics identify the actual source relay, while its sent metrics aggregate the
shared emitter pipeline with `relay="-"` because one flush may contain work from several sources.

An emitter's sent metrics count the records its destination delivered, each once, however many
attempts it took: a flush that completes after retries counts its delivered records once, and a
record the destination refused follows `ON MESSAGE ERROR` and is not sent. When a flush fails for
good, the records its emitter delivered before the failure are counted as its other records are routed
to their error policy. An emitter that publishes on its own commit, such as Iceberg, counts what each
commit publishes. Sent bytes measure a delivered record's payload as the emitter's buffer measures
it; an HTTP request's method, target and headers are not payload, and a request without a body
carries none. See [HTTP inspection and metrics](./emitters.md#http-inspection-and-metrics).

`DESCRIBE` output uses the same concepts but renders `physical_node_id` as `physical_node` for readability.

Example Prometheus series:

```text
nervix_messages_total{domain="prod",target_kind="RELAY",target="notifications",physical_node_id="node-1",direction="received",relay="notifications",peer_kind="-",peer="-",partition="-",instance="-"} 42
nervix_messages_total{domain="prod",target_kind="RELAY",target="tenant_events",physical_node_id="node-2",direction="received",relay="tenant_events",peer_kind="-",peer="-",partition="1",instance="-"} 17
```

`DESCRIBE` metric lines of a partitioned relay name the partition after the relay, for example
`messages_total received relay=tenant_events partition=1 physical_node=node-2 total=17 ...`.
Every `RELAY` series a partitioned relay records names one of its partitions, so such a relay has no
`RELAY` series with `partition="-"`. A partition's series exist on the node that owns it and are
withdrawn when that owner ends, as when the partition moves to another node.

Processor input, output, latency and branch-aggregated series carry the processor unit's partition.
`DESCRIBE` gathers those series from each unit's execution owner; state replicas do not repeat a
sibling owner's series. Concrete branch keys remain outside the Prometheus label set.

Example `DESCRIBE INGESTOR` edge metric section:

```text
metrics:
  outgoing_edges:
    messages_total sent relay=notifications physical_node=node-1 total=42 wall_rate_per_sec=12.5 domain_rate_per_sec=10 wall_rate_ema_1m_per_sec=11.2 wall_rate_ema_15m_per_sec=8.7 domain_rate_ema_1m_per_sec=9.8 domain_rate_ema_15m_per_sec=7.4
```

## Raw Metrics

Nervix records these raw metric families:

- `nervix_messages_total`: total messages received or sent
- `nervix_batches_total`: total batches received or sent
- `nervix_bytes_total`: total bytes received or sent
- `nervix_messages_per_batch`: histogram of message count per batch. Its finite Prometheus bucket
  boundaries are `1`, `2`, `5`, `10`, `50`, `100`, `500`, `1000`, `1024`, `2048`, `4096`,
  `8192`, `16384`, `32768`, and `65536`, followed by `+Inf`. The rolling HDR histograms used by
  `DESCRIBE` track batch sizes through 65,536 messages.
- `nervix_delivery_latency_seconds`: histogram of delivery latency between graph nodes
- `nervix_emitter_publish_attempts_total`: completed host publish and commit attempts, split by
  `operation` and `outcome`; see [Connector Interactions](#connector-interactions)
- `nervix_emitter_publish_duration_seconds`: elapsed wall seconds per host publish or commit
  attempt, split by `operation`
- `nervix_relay_buffer_len`: histogram of runtime relay buffer occupancy in queued batches
- `nervix_branch_instances`: current concrete branch keys with at least one runtime instance on the
  physical node
- `nervix_branch_evictions_total`: total concrete branches evicted on the physical node, split by
  `reason="lru"` or `reason="ttl"`
- `nervix_inferencer_state_bytes`: declared native tensor bytes held by active stateful branches
- `nervix_inferencer_snapshot_failures_total`: failed inferencer snapshot publication or persistence
  attempts
- `nervix_inferencer_restored_branches_total`: branches initialized from a matching published
  tensor generation
- `nervix_inferencer_reset_branches_total`: branches initialized from their declared `INITIAL`,
  including their first start
- `nervix_ingestor_quiesce_buffered_records`: raw payloads currently retained in an ingestor's
  per-instance quiesce buffers, including a payload that is draining until its messages enter
  their ingest group
- `nervix_ingestor_quiesce_buffered_bytes`: raw payload bytes currently retained in those buffers
- `nervix_ingestor_quiesce_dropped_total`: payloads deliberately discarded by `DROP`, buffer
  overflow, memory-pressure zero-capacity behavior, or an interrupting termination
- `nervix_ingestor_quiesce_rejected_total`: endpoint requests or connections refused while an
  ingestor cannot accept them, counted once per request for each ingestor whose quiesce refused it
- `nervix_ingestor_endpoint_requests_total`: requests and WebSocket data frames an `HTTP ENDPOINT`
  or `WEBSOCKETS ENDPOINT` ingestor answered, counted once per request for each ingestor its route
  was bound to, with bounded `outcome` values `accepted`, `busy`, `refused`, `not_admitted`,
  `decode_rejected` and `processing_failed`; see
  [Ingestor Source Interactions](#ingestor-source-interactions)
- `nervix_ingestor_reads_total`: completed broker reads and scheduled polls, with bounded
  `outcome` values `delivered`, `empty`, `failed` and `interrupted`
- `nervix_ingestor_read_payloads_total` and `nervix_ingestor_read_bytes_total`: raw payloads and
  their bytes handed over by broker reads or paced polls, before decoding, filtering and timestamp
  admission. Transport redelivery counts again; replay of an internal quiesce buffer does not.
- `nervix_ingestor_poll_duration_seconds`: elapsed wall seconds around paced polls, including
  failed or interrupted polls. Broker reads have no duration observation because their wait is idle
  time, rather than the cost of a scheduled operation.
- `nervix_ingestor_ack_outcomes_total`: acknowledgement waits an ingestor settled, by `outcome`:
  `acknowledged`, `failed` for a tree the graph failed, `timed_out` for an `ACK TIMEOUT` that
  expired, and `shutdown` for a wait its stop ended; see
  [Ingestor Acknowledgement And Lifecycle](#ingestor-acknowledgement-and-lifecycle)
- `nervix_ingestor_transport_acknowledgements_total`: acknowledgements and rejections of read
  positions the ingestor's transport answered, by `operation` (`acknowledge` or `reject`) and
  `outcome` (`succeeded` or `failed`)
- `nervix_ingestor_transport_acknowledgement_duration_seconds`: elapsed wall seconds of each of
  those transport operations, by `operation`, such as a Kafka commit or an SQS deletion
- `nervix_ingestor_resumes_total`: completed resumes of an ingestor's connector, by `outcome`:
  `ready`, `waiting` for an external assignment, or `failed`
- `nervix_ingestor_suspends_total`: suspensions of an ingestor's connector, by `cause`: `quiesce`,
  `read_failure` or `rejection`
- `nervix_ingestor_unconfirmed_stop_positions_total`: positions an ingestor read but stopped before
  its transport confirmed them, which its transport presents again
- `nervix_ingestor_transient_failure`: `1` while the ingestor's current task has a published
  transient failure, `0` otherwise and once the task ends
- `nervix_ingestor_unfolding_refused_total`: raw payloads refused because extension-worker
  admission had no place, including acknowledged intake and endpoint requests. A retained payload
  waiting for a place is not a refusal. See [When The Extension Workers Are Full](./ingestors.md#when-the-extension-workers-are-full).
- `nervix_ingestor_transport_redeliveries_total`: pending acknowledgements and rejections an
  ingestor discarded after its RabbitMQ, SQS or MQTT ingestor reconnected. The broker redelivers what
  the lost connection did not acknowledge, so each counted position is ingested again
- `nervix_ingestor_decode_rejections_total`: payloads an ingestor consumed because its codec
  refused them, each reported as a decode rejection under its `ON GENERAL ERROR` policy; see
  [When The Codec Refuses A Payload](./ingestors.md#when-the-codec-refuses-a-payload)
- `nervix_ingestor_oversized_payloads_total`: payloads an ingestor dropped because they
  exceed their client's message size bound, such as a Syslog UDP datagram larger than
  `max_message_size`; see [Oversized payloads](./connector-contract.md#oversized-payloads)
- `nervix_ingestor_header_values_dropped_total`: invalid UTF-8 transport header values an
  ingestor discarded while retaining their message and all valid header values: the response
  headers of an `HTTP REQUEST` poll, or the request headers an `HTTP ENDPOINT` request or a
  `WEBSOCKETS ENDPOINT` upgrade carried, such as a value in Latin-1. An endpoint counts such a value
  once for each ingestor its request or upgrade was routed to, on the node that received it; see
  [HTTP ENDPOINT And WEBSOCKETS ENDPOINT](./ingestors.md#http-endpoint-and-websockets-endpoint)
- `nervix_ingestor_read_ahead_dropped_payloads_total`: QoS 0 publishes an ACK-mode MQTT ingestor
  dropped because its separate 64-outcome read-ahead quota was full. The driver counts each drop
  immediately, including during a downstream acknowledgement wait; see
  [MQTT ingestor connections](./connector-contract.md#mqtt-ingestor-connections)
- `nervix_session_subscriptions`: open session subscriptions the node delivers from a relay. The
  node advertises interest in the relay to the cluster exactly while this is above zero, and the
  series stays at `0` once the relay's last subscription on the node closes
- `nervix_session_subscription_dropped_rows_total`: rows `DROPPING` session subscriptions on the
  node discarded because their session could not take them in time
- `nervix_client_ingestor_producers`: producers attached to a client ingestor the node executes
- `nervix_client_ingestor_forwarded_producers`: those of them whose sessions another node serves
- `nervix_client_ingestor_outstanding_batches`: batches those producers submitted that have no
  outcome yet
- `nervix_client_ingestor_outstanding_bytes`: the Arrow IPC bytes of those batches
- `nervix_client_ingestor_admitted_batches`: batches holding a slot of the ingestor's
  acknowledgement window
- `nervix_client_ingestor_submissions_total`: batches the ingestor answered, by `outcome` and
  `cause`
- `nervix_sessions`: client sessions the node serves now
- `nervix_sessions_opened_total`: client sessions the node has served
- `nervix_session_requests_total`: requests the node's client sessions decoded, by `request`
- `nervix_session_rejections_total`: requests those sessions answered with a rejection, by
  `rejection`
- `nervix_session_cancellations_total`: requests a cancellation ended, by `stage`
- `nervix_session_transfer_parts_total`: transfer parts those sessions queued for replies larger
  than one frame
- `nervix_client_authentications_total`: calls the node's client listeners authenticated or
  refused, by `outcome`
- `nervix_jemalloc_active_bytes`: bytes in active allocator pages
- `nervix_jemalloc_allocated_bytes`: bytes allocated by the process
- `nervix_jemalloc_mapped_bytes`: bytes mapped by active allocator extents
- `nervix_jemalloc_metadata_bytes`: bytes dedicated to allocator metadata
- `nervix_jemalloc_resident_bytes`: resident data-page bytes mapped by the allocator
- `nervix_jemalloc_retained_bytes`: retained virtual-memory mapping bytes

Histograms follow Prometheus conventions and include `_bucket`, `_sum`, and `_count` series. Current bucket boundaries are:

- messages per batch: `1`, `2`, `5`, `10`, `50`, `100`, `500`, `1000`, `+Inf`
- delivery latency seconds: `0.001`, `0.005`, `0.01`, `0.05`, `0.1`, `0.5`, `1`, `5`, `30`, `+Inf`
- relay buffer length: `1`, `2`, `4`, `8`, `16`, `32`, `64`, `128`, `256`, `512`, `1024`, `2048`, `+Inf`

Prometheus receives raw values only. The `/metrics` endpoint is encoded by the Prometheus client registry, not by Nervix internal summary state. Prometheus should compute external queries, alerts, and dashboards with normal PromQL aggregation.

The ingestor families `nervix_ingestor_unfolding_refused_total`,
`nervix_ingestor_transport_redeliveries_total`, `nervix_ingestor_decode_rejections_total`,
`nervix_ingestor_oversized_payloads_total`, `nervix_ingestor_header_values_dropped_total`
and `nervix_ingestor_read_ahead_dropped_payloads_total`
use `domain`, `ingestor`, `physical_node_id`, `instance`, `connector` and `entity` labels.
The same label set applies to the four quiesce families, the read/poll families, the endpoint
request family and the acknowledgement and lifecycle families. Reads, endpoint requests,
acknowledgement outcomes and resumes add `outcome`, suspensions add `cause`, transport
acknowledgements add `operation` and `outcome`, and their duration adds `operation`. Connector and
entity values come from the vocabulary, such as `KAFKA` and `TOPIC`. Buffer families and the
transient-failure family are gauges; dropped, rejected, refused, redelivered, decode-rejection,
oversized, invalid-header, endpoint request, acknowledgement, resume, suspension and
unconfirmed-stop families are monotonic counters. They are process-local: quiesce buffers do not
migrate during termination or failover.

Quiesce, refusal, redelivery, decode-rejection, oversized and invalid-header children are registered when the
ingestor starts, and `DESCRIBE INGESTOR` registers the redelivery, decode-rejection and oversized
children at zero on the node that runs the ingestor. An `HTTP ENDPOINT` or `WEBSOCKETS ENDPOINT`
ingestor registers its six endpoint request children at zero when it starts on a node; no other
ingestor has them.
The MQTT read-ahead counter is registered at zero when that ingestor starts. It is exported through
Prometheus and remains on that physical node across connection restarts; `DESCRIBE INGESTOR` does
not project this counter. The invalid-header counter is exported through Prometheus only;
`DESCRIBE INGESTOR` does not project it.
Client-ingestor outcome children are registered when its endpoint starts, and subscription loss
counters when delivery starts. These bounded series may therefore appear at zero before their
first observation. Their labels and counting contracts are unchanged.

The two session-subscription families use `domain` and `relay` labels and describe the node that
exports them: the subscriptions its sessions hold and the rows those sessions lost. A client is told
of its own losses directly, as described in [Sessions](sessions.md). The rest of a node's session
traffic, and the authentication attempts its client listeners judge, are described under
[Client Traffic](#client-traffic).

## Client Traffic

Seven families describe the traffic the node's two client listeners carry, the native gRPC listener
and the web console listener, which the `transport` label names as `grpc` or `console`. They
describe the node that exports them: a session counts on the node its client connected to, and a
call counts on the node that judged its credentials. Every other label is drawn from a set fixed at
compile time. No label carries a user name, a session, request or execution identity, a domain, or
a payload value, and every child is registered at zero when the node starts, so each series appears
before its first observation.

| Family | Kind | Labels | Counts |
| --- | --- | --- | --- |
| `nervix_sessions` | gauge | `transport` | Sessions the node serves now, from the moment a session's call is authenticated until the session has released its subscriptions and its transaction binding. |
| `nervix_sessions_opened_total` | counter | `transport` | Sessions the node has served. |
| `nervix_session_requests_total` | counter | `transport`, `request` | Requests the sessions decoded, by kind: `command`, `suggest`, `choice`, `list_domains`, `select_domain`, `attach_transaction`, `inspect_transaction`, `subscribe`, `unsubscribe`, `cancel`, `attach_domain_clock`, `detach_domain_clock`, `open_ingestor`, `submit_batch`, `close_ingestor`, `open_emitter`, `read_emitter_batch`, `settle_emitter_batch` or `close_emitter`. A frame that does not decode names no kind and counts only as a rejection. |
| `nervix_session_rejections_total` | counter | `transport`, `rejection` | Requests answered with a rejection, by its reason: `invalid_request`, `unsupported_request`, `unsupported_value`, `duplicate_request_id`, `reply_too_large`, `too_many_requests_in_flight` or `server_busy`. A reply replaced by a rejection because it could not be encoded counts under the replacement's reason. |
| `nervix_session_cancellations_total` | counter | `transport`, `stage` | Requests a cancellation ended, by whether it found them `before_admission` or `after_admission`. A cancellation whose target was already answered ended nothing and is not counted here; the cancel request itself counts under `request="cancel"`. |
| `nervix_session_transfer_parts_total` | counter | `transport` | Transfer parts queued for replies larger than one frame. |
| `nervix_client_authentications_total` | counter | `transport`, `outcome` | Calls whose authentication ended `authenticated`, `missing` (no readable Basic credentials), `rejected` (an unknown user, a wrong password, or a stored hash the node could not verify) or `unavailable` (the node could not judge the credentials now). Every call counts: a session, an upload, a download and a restore, and on the console a `/console/auth` check and a resource upload. |

`rejected` deliberately does not tell an unknown user from a wrong password. The observability
listener is unauthenticated, and that distinction is what enumerating the cluster's users would
look for; an unknown user also costs the node the same credentials work, as
[Authentication](./client-session-protocol.md#authentication) describes. A rate of `rejected`
attempts is the sign of a client guessing passwords, and the `warn` record of each names the address
it came from.

## Command Execution History

Every node exposes its applied command history through these bounded metric families:

| Family | Kind and labels | Meaning |
| --- | --- | --- |
| `nervix_command_execution_records` | Gauge, `state` is `applying`, `finished` or `expired` | Retained executions in each state, including compact expiry tombstones. |
| `nervix_command_execution_retry_fence_timestamp_seconds` | Gauge, no labels | The durable retry fence as Unix seconds, absent until a fence has been committed. |
| `nervix_command_execution_admission_refusals_total` | Counter, `reason` is `expired`, `future`, `missing_timestamp` or `capacity` | Definitive admission refusals handled by this node process. |

The record counts and fence come from one applied revision at scrape time. Reading the three
existing history indexes takes constant time and does not scan executions or advance the fence.
Followers expose their own applied revision, so they may briefly trail the leader. These gauges
describe replicated state: use the leader's values for a cluster total rather than summing copies
across nodes. A restart restores them from the recovered history and fence. `time() -
nervix_command_execution_retry_fence_timestamp_seconds` shows the fence's age against the scraper's
clock, including an unexpectedly advanced fence.

Refusal counters are process-local and start at zero for all four reasons. They count refused
attempts, including a retry whose compact tombstone is still retained, rather than unique execution
references. `future` means the UUIDv7 creation time exceeds the accepted five-minute clock skew or
the timestamp range; `missing_timestamp` means the reference carries no UUIDv7 creation time.
Follower application of a refused proposal adds no event, and a leadership change neither copies
nor resets counters on either live process. Sum their rates across nodes to follow refusals as
leadership moves; a process restart resets its counters. Completion and background recovery add no
admission event, including when history maintenance has expired the completed outcome. Recovered
successful outcomes, reference content conflicts, redirects and uncertain admission outcomes add
no refusal event to these four reasons. No execution identity, domain or user appears in their labels.

See [Bounded History And Expired Identities](./client-session-protocol.md#bounded-history-and-expired-identities)
for retention and admission semantics.

## Licensing Metrics

The `nervix_licensing_` families expose the published policy and process-local admission events.
Label values come from fixed declarations, with every applicable labelled child initialized to
zero. Replicated gauges describe the exporting node's applied state; use the leader's values for
a cluster total. Sum refusal-counter rates across nodes as leadership moves. A process restart
resets its counters.

| Suffix | Kind and labels | Free policy |
| --- | --- | --- |
| `state` | Gauge, `state`: `free`, `licensed_fresh`, `licensed_due`, `licensed_overdue`, `lapsed`, `stopped`, `releasing`, `degraded` | `free` is 1; the other states are 0. |
| `decision_revision` | Gauge | Absent until a committed decision exists. |
| `deadline_timestamp_seconds` | Gauge, `deadline`: `refresh_after`, `overdue_at`, `valid_until`, `term_ends_at`, `ephemeral_expires_at`, `next_grant` | A sample is absent when its deadline does not exist. |
| `licensing_time_seconds` | Gauge | The committed licensing timestamp as Unix seconds, present after first installation. |
| `requests_total` | Counter, `operation` and `outcome` | On the leader, every operation/outcome pair starts at 0. |
| `stream_connected` | Gauge | On the leader, 0. |
| `transitions_active` | Gauge | 0. |
| `over_limit` | Gauge, catalog `condition` | Every cluster quota starts at 0. |
| `admissions_refused_total` | Counter, catalog `condition` and refusal `state` | Every cluster quota/refusal-state pair starts at 0. |

Request operations are `activation.create`, `activation.refresh`, `activation.release`,
`activation.rotate_key` and `events.subscribe`. Outcomes are `committed`, `rejected_by_cluster`,
`refused_by_service`, `unreachable` and `timeout`. Refusal states are `within_grace`, `over_limit`,
`lapsed` and `stopped`. Connectivity and request families are leader-local. Free mode supplies no
decision or deadline and makes no licensing request. The shipped unlimited quotas produce no
admission refusals.

See [Cluster Licensing](node-settings.md#cluster-licensing) for `DESCRIBE LICENSE` and the
committed node/domain usage definitions.

## Client Ingestors

The six client-ingestor families describe the node that executes the ingestor and use `domain` and
`ingestor` labels. The first five are gauges, rewritten whenever the ingestor's endpoint changes
them, and they are `0` once the endpoint ends. `admitted_batches` never exceeds the window the
ingestor declares, however many producers are attached.

`nervix_client_ingestor_submissions_total` is a counter with two more labels drawn from fixed sets.
`outcome` is `not_admitted`, `completed`, `processing_failed`, or `outcome_unknown`. `cause` is the
refusal of a batch that was not admitted (`invalid_batch`, `suspended`, `busy`, `draining`,
`producer_ended`, `credit_exceeded`), the failure of one that failed (`ack_timeout`, `rejected`),
the uncertainty of one whose outcome is unknown (`interrupted`, `owner_lost`), or `none` for a
completed batch. No label carries a payload value, a producer, or an attachment identity. A batch
the serving session refused before it reached the node that executes the ingestor, such as one
beyond its producer's credit, is answered by the session and not counted here. So is every batch the
node that forwards a producer answers itself once it lost the node that executes the ingestor: a
batch it never cleared for admission as `not_admitted` with `producer_ended`, and a cleared one as
`outcome_unknown` with `owner_lost`.
[Ingestors](ingestors.md#observing-client-ingestors) describes the same counts in `SHOW INGESTORS`
and `DESCRIBE INGESTOR`.

## Client Emitters

Client emitter metrics use `domain` and `emitter` labels on the executing node. The gauges
`nervix_client_emitter_consumers`, `nervix_client_emitter_forwarded_consumers`,
`nervix_client_emitter_forwarded_credit_bytes`, `nervix_client_emitter_retained_batches`, and
`nervix_client_emitter_retained_bytes` describe current in-memory attachments and prepared Arrow
IPC awaiting an application decision. `nervix_client_emitter_forwarded_retained_batches` and
`nervix_client_emitter_forwarded_retained_bytes` count the subset assigned to consumers served on
another node. `nervix_client_emitter_incomplete_batches` counts assigned batches whose application
decision is still pending. These gauges return to zero when that endpoint ends. The counters
`nervix_client_emitter_retries_total`, `nervix_client_emitter_acks_total`, and
`nervix_client_emitter_rejections_total` retain results across endpoint restarts on the same node.
Retries include explicit application retry, ACK timeout, and consumer loss; a repeated confirmed
ACK does not increment the ACK counter. `DESCRIBE EMITTER` reports the same values for the
scheduled owner.

Restoring a client producer or consumer creates a new server attachment. The attachment gauges
fall when the prior session detaches and rise only after the replacement open succeeds; retained
batch and incomplete-attempt gauges remain bounded by the existing endpoint budgets. None uses a
session, producer, consumer, delivery or attempt identity as a metric label. Applications can sample
the Rust handles' `connection()` states to distinguish an active, interrupted, restoring or
reopen-required handle. An uncertain producer result and an uncertain consumer settlement remain
typed call outcomes; these gauges do not imply replay or a confirmed ACK.

## Delivery Latency

Every processor, reingestor, and emitter input records the delivery latency of each batch it
accepts. A row's latency runs from its ingestion high watermark to the instant the input accepts the
batch. Processors and reingestors read that instant from the domain clock, so in a paced domain
their latency is measured in domain time; emitters read the wall clock. A row whose high watermark
is later than that instant has no latency and adds nothing to the histograms, although it still
counts as a received message.

A batch is recorded as a whole. Each row's ingestion watermarks travel with the batch in Arrow
buffers of Unix-nanosecond timestamps, and one pass over the high watermarks finds the batch's
latest watermark and folds every row's latency into buckets. Each latency series then takes the whole batch
in one update: the rolling histograms behind `DESCRIBE` merge the buckets under one lock with one
wall-clock reading, and the Prometheus histogram takes the batch in one flush.

- The batch's latest high watermark is its domain timestamp. It stamps the batch's traffic counters
  and places its latencies in the domain-clock windows. A batch older than a window's current step
  lands in its own step only while that step is still retained; an older step is never reopened.
- The rolling histograms record each latency to the nearest millisecond, halves rounding up, and
  record anything above 30 seconds as 30 seconds, so a slow tail is never dropped from the
  percentiles.
- The Prometheus histogram receives every latency in seconds and counts it in the boundaries
  listed under [Raw Metrics](#raw-metrics); a latency above 30 seconds counts only in `+Inf`.

## Interconnection Metrics

Every node also exposes its own interconnection: the authenticated HTTP/2 pools it carries internal
traffic over, the execution admission every variable-size operation passes through, its consensus
retention, the reactor delay they share, and the timing drift it sees between its peers and the
leader. These series describe the exporting node only, so they carry no node label, and they are
aggregated by dimensions whose value sets are fixed: `class`, `direction`, `operation`, `reason`,
and `outcome`. None of them carries a branch key, a peer identity, a domain, or an operation
identifier.

`class` is the traffic class on transport series — `management`, `commands`, `replication`, `relay`,
or `bulk` — and the execution class on admission series — `management`, `commands`, `relay`, `bulk`,
`state_read`, `state_read_cache`, `bulk_window`, `restore_metadata`, `credentials` for memory, and `control_cpu`, `credentials_cpu`, `data_cpu`, `extension_cpu`,
`bulk_cpu`, `consensus_storage`, `filesystem_storage`, `connector_storage` for workers. Execution byte-limit
series use `operation` values `relay_encoded`, `relay_decoded`, `relay_scratch` and `bulk_chunk`.
On transport series, `operation` is the reserved request subquota: `shared`, `append`, `resource`, `snapshot`,
`discovery`, `liveness`, `progress`, `admission`, `cancellation`, or `terminal`.

Transport pools:

- `nervix_interconnect_connections`: physical connections held, by `class` and `direction`
- `nervix_interconnect_streams`: HTTP/2 stream slots leased on outbound connections, by `class`
- `nervix_interconnect_pending_operations`: requests admitted and not yet resolved, by `direction`
  and `operation`
- `nervix_interconnect_connections_established_total`: connections established, by `class`
- `nervix_interconnect_connection_failures_total`: connections that failed to establish or ended,
  by `class` and `reason`, where `reason` is `setup`, `handshake`, `capacity`, `closed`,
  `resolution` for an attempt whose peer's advertised host did not resolve, or `prerequisite` for
  one that ended because one node cannot run beside the other: a prerequisite they must share
  differs, or a timing is outside the bound two nodes may hold apart
- `nervix_interconnect_stream_resets_total`: streams that ended without a result, by `class` and
  `reason`, where `reason` is `deadline`, `capacity`, `peer`, `shutdown`, or `malformed`
- `nervix_interconnect_quota_failures_total`: requests refused because their reserved subquota was
  full, by `direction` and `operation`
- `nervix_interconnect_requests_total` and `nervix_interconnect_request_seconds_total`: typed
  requests completed and the round trip they took, by `operation`. Divide the seconds by the count
  for average latency; `operation="liveness"` is the peer health probe, so that ratio is health
  RTT, and `operation="progress"` is replaceable fenced domain-clock progress
- `nervix_interconnect_bulk_bytes_total`: bytes carried by streamed bodies, by `class` and
  `direction`. This rises while a transfer is running, so it reads as bulk progress

Relay admission:

- `nervix_interconnect_relay_channels`, `nervix_interconnect_relay_attempts`,
  `nervix_interconnect_relay_grants`: logical channels with unresolved work, attempts whose outcome
  has not been retired, and transfer grants issued and not yet spent. These aggregate bounded
  authenticated peer owners. Ending a peer releases its protocol capacity immediately, including
  when the application still borrows an admitted intake; that intake's retained metadata stays
  charged to relay memory until its last borrow ends. A delivery the receiver holds back because
  its relay memory has no room for the delivery's acknowledgement watches stays an unresolved
  attempt here, with its relay memory near `nervix_execution_memory_capacity_bytes`, and counts no
  rejection until it is admitted
- `nervix_interconnect_relay_grant_refusals_total`: relay grants refused, by `direction` and
  `reason`. `direction="inbound"` counts the grants this node refused other nodes and
  `direction="outbound"` the refusals this node received for its own deliveries. The capacity
  reasons, `memory`, `queue_item`, `terminal_outcome` and `channel_busy`, hold the delivery back,
  and its sender asks again; a count of them that keeps rising on a sender means its deliveries are
  waiting for the receiver's room, and its ingestors are held with them. Every other reason fails the
  delivery at once. See [Held Deliveries](./interconnect.md#held-deliveries)
- `nervix_interconnect_relay_admissions_total` and
  `nervix_interconnect_relay_admission_wait_seconds_total`: attempts resolved, by `outcome`
  (`admitted`, `rejected`, `cancelled`), and the time they spent between an accepted reservation
  and their resolution
- `nervix_interconnect_unresolved_outcome_age_seconds`: how long the oldest unacknowledged relay
  outcome has been waiting. A rising value means acknowledgements are not coming back
- `nervix_interconnect_relay_channel_watermarks`: the relay channel watermarks this node retains for
  the channels its peers opened, across every peer. Each peer has a fixed budget of 8,192
- `nervix_interconnect_relay_channel_watermarks_evicted_total`: watermarks forgotten before their
  ten-minute retention ended because their peer's budget needed the position for another channel.
  A count that keeps rising means a peer opens more relay channels within ten minutes than the
  budget holds; see
  [Bounded Correlation And Peer Owners](./interconnect.md#bounded-correlation-and-peer-owners)

Execution admission:

Worker series include the `connector_storage` class. Its running and pending jobs show Kafka
ingestor control calls, consumer-group commits and the destruction of retired consumers and producers
waiting on their broker, independently of `filesystem_storage` and `consensus_storage`. A commit may
keep its worker after its caller's thirty-second deadline; the next commit from that instance waits
for the same job. A retired consumer whose group coordinator does not answer keeps its worker for up
to five seconds, and a retired producer keeps one for its polling thread's last poll and up to half
a second of flushing; see [Kafka client retirement](./connector-contract.md#kafka-client-retirement).

- `nervix_execution_memory_capacity_bytes` and `nervix_execution_memory_reserved_bytes`: the
  transient memory reserved for one class, and what it is currently holding
- `nervix_execution_memory_reservations_total` and `nervix_execution_memory_rejections_total`:
  charges granted and charges refused outright
- `nervix_execution_operation_limit_bytes` and `nervix_execution_resident_replication_batches`: resolved relay/bulk byte ceilings and replication window.
- `nervix_execution_queue_capacity`: configured waiting places for each worker class. Workers,
  queues and memory capacities report the actual [resolved execution settings](node-settings.md#execution-capabilities).
- `nervix_execution_workers`, `nervix_execution_jobs_running`, `nervix_execution_jobs_pending`: how
  many jobs one class may have on the blocking pool at once, how many it holds, and how many wait
- `nervix_execution_jobs_total`, `nervix_execution_jobs_completed_total`,
  `nervix_execution_job_rejections_total`: jobs admitted, jobs that left a worker, and jobs refused
  because the class already held its whole wait queue

For `state_read_cache`, a rejected charge means the optional retained view had no room. The
generator can still return a complete uncached read using `state_read` admission. Active decoded
snapshots hold that class through installation or source evaluation and dispatch. Both read
classes default to the relay budget, accept independent overrides and count separately in the startup total.
- `nervix_execution_job_queue_seconds_total` and `nervix_execution_job_work_seconds_total`: time
  admitted jobs spent waiting for a worker, and time completed jobs spent holding one

Node scheduling:

- `nervix_node_scheduler_delay_seconds_total`, `nervix_node_scheduler_delay_peak_seconds`, and
  `nervix_node_scheduler_samples_total`: how late the node's reactor has been polling work that was
  already runnable. A timer asks to be woken every 250 milliseconds; the excess it is woken by is
  the delay every other task on that worker paid

Consensus retention:

- `nervix_consensus_log_last_index`, `nervix_consensus_log_snapshot_index`,
  `nervix_consensus_log_purged_index`: the highest Raft index the log holds, the highest the
  current snapshot covers, and the highest already removed. A position that does not exist yet
  reports `-1`
- `nervix_consensus_log_retained_bytes`: encoded bytes held by the node's current Raft log entries
- `nervix_consensus_snapshot_pinned_generations`, `nervix_consensus_snapshot_pinned_readers`,
  `nervix_consensus_snapshot_unreferenced_generations`: snapshot generations an outgoing transfer
  holds against deletion, the readers those pins are held for, and generations waiting to be
  deleted

Cluster prerequisites:

- `nervix_cluster_timing_drift_nodes`: the live nodes whose Raft heartbeat, gossip interval or
  clock-skew tolerance differs from the leader's, as this node's gossip sees them. It reads `-1`
  while this node knows no leader or no record the leader published, rather than a zero that would
  claim every node agrees. A difference within the bound two nodes may hold apart is counted too,
  so the series shows a rolling change in progress; `DESCRIBE CLUSTER` names the nodes and the
  values, as [Node Settings](./node-settings.md#prerequisites-at-join) describes

These series answer two operational questions directly. A class that is saturated shows a reserved
byte count at its capacity, or running jobs at its worker count with pending jobs behind them,
while the other classes stay free — that is the isolation the pools exist for. A control answer
arriving late while bulk work runs shows up as reactor delay and as request seconds rising for the
`shared` or `liveness` operation, not as a connection failure.

## Restore Staging Metrics

Every node publishes its restore checkpoint maintenance after each sweep that completes:
`nervix_restore_staging_bytes`, `nervix_restore_staging_keys` and
`nervix_restore_staging_limit_bytes` are gauges of the unpublished restored checkpoint key and
value bytes, their key count and the allowance the node configured or derived from its database
volume; `nervix_restore_staging_reclaimed_bytes_total` and `nervix_restore_staging_sweeps_total`
count the bytes completed sweeps removed and the sweeps themselves; and `nervix_restore_keyspaces_disk_bytes` is the disk allocation of the shared
checkpoint keyspaces. They read zero until the node's first sweep completes. Backups have no metric
family of their own: their transfers appear in the interconnection series and their conversions in
the `bulk` and `restore_metadata` memory classes. [Backup And
Restore](backup-and-restore.md#restore-checkpoint-storage-quota-and-metrics) defines each series.

## Connector Interactions

### Connector-declared events

An entity's generated [capability reference](connector-capabilities.md) names its closed set of
connector events. The host registers one counter at zero for each declared event when its task
starts. Emitters export `nervix_emitter_connector_events_total` with `domain`, `emitter`,
`physical_node_id`, `connector`, `entity` and `event`; ingestors export
`nervix_ingestor_connector_events_total` with `ingestor` instead of `emitter` and their existing
`instance` label. An entity with no declared events creates no event children. Event labels never
contain payloads, external names or driver error text.

The connector resolves a typed counter from its host during construction and retains it while
recording. Event counters appear with their wall rates under `destination:` or `source:` in
owner-routed inspection. They retain process-local history across task restarts with the same
labels; a new process starts at zero. They are not replicated or restored.

Recording performs one retained counter increment. Inspection computes the lifetime wall average
and samples the cumulative total into one-minute and fifteen-minute wall EMA estimates. Their
sampling interval is the time between inspections; an unchanged total contributes no further
events. The first inspection establishes the EMA time baseline; a later inspection with elapsed
wall time supplies its first rate sample. Domain rates are unavailable. No clock read or rate
update is added to publication.

`REDIS PUBSUB` emission declares `published_without_subscribers`. It counts each successful
`PUBLISH` whose receiver count is zero. Such a record is still delivered: Redis accepted it, as
`MODE NO ACK` promises. A positive receiver count leaves the event counter unchanged and does not
prove subscriber processing. An unanswered or failed publish does not count this event.

### Publish attempts

Emitter destination observations describe the host's interaction with a connector, separately
from delivered graph traffic. Both interaction families carry `domain`, `emitter`,
`physical_node_id`, `connector` and `entity`. Connector names use the vocabulary's display form,
such as `REDIS`; entities use their declared name, such as `PUBSUB`. `operation` is `publish` or
`commit`. Each starting emitter registers every operation/outcome child and histogram at zero,
even if that connector has no separate commit operation.

`nervix_emitter_publish_attempts_total` records one outcome per nonempty host publish attempt or
due staged commit, including each retry. It never counts one attempt per record. Its bounded
`outcome` label projects the typed result the host already handles:

- `succeeded`: the operation completed, including settled per-record rejections that do not fail
  the operation
- `transient`: an existing retryable publish failure, a stop deadline or shutdown while stalled,
  or cancellation of an attempt before it has a definitive answer
- `misconfigured`: the connector returned the host's invalid-connector-configuration outcome
- `violation`: a terminal preparation or connector-contract failure, including an unknown or
  repeated record answer

This observation does not change retry, acknowledgement or error policy. In particular, delivered
records still increase the existing `messages_total sent` only once. A connector can make several
external calls inside one host attempt; these families do not count those calls separately.

`nervix_emitter_publish_duration_seconds` observes elapsed wall time once per started attempt,
including failed and cancelled attempts. Its finite bucket boundaries are `0.001`, `0.005`,
`0.01`, `0.05`, `0.1`, `0.5`, `1`, `5` and `30` seconds, followed by `+Inf`. Empty flushes and
commits that are not due record neither a counter nor a duration.

Interaction observations are process-local and are retained for that process's lifetime. A
restarted task with the same labels continues its totals; another connector entity or physical
node has separate children. A new process starts at zero. These observations are neither
replicated nor restored from snapshots, and removing a task does not withdraw its history.

`DESCRIBE EMITTER` renders a `destination:` section after status and before the graph-edge
`metrics:` section. The existing owner route also supplies this section when another node receives
the command. Each attempt counter line includes its full family and label selector, `total`, the wall
average rate and the one-minute and fifteen-minute wall EMA rates. Domain rates are `-`, because
an attempt has no domain timestamp. Each duration line includes its full selector, `count` and
`sum_seconds`. For example:

```text
destination:
  nervix_emitter_publish_attempts_total{domain="prod",emitter="notifications_out",physical_node_id="node-1",connector="REDIS",entity="PUBSUB",operation="publish",outcome="succeeded"} total=2 wall_rate_per_sec=0.5 domain_rate_per_sec=- wall_rate_ema_1m_per_sec=0.4 wall_rate_ema_15m_per_sec=0.1 domain_rate_ema_1m_per_sec=- domain_rate_ema_15m_per_sec=-
  nervix_emitter_publish_duration_seconds{domain="prod",emitter="notifications_out",physical_node_id="node-1",connector="REDIS",entity="PUBSUB",operation="publish"} count=2 sum_seconds=0.003
```

### Emitter outcomes and lifecycle

These host observations carry the same five identity labels as destination attempts. Every
bounded child exists at zero when its emitter starts. Counters retain their process-local history
across task restarts; the failure gauge describes the current task. `DESCRIBE EMITTER` includes
every family in `destination:`, with its full selector and `total`, or `value` for the gauge.
The two wait counters preserve fractional seconds.

| Family | Additional label | Meaning |
| --- | --- | --- |
| `nervix_emitter_records_rejected_total` | `code="external\|oversize\|invalid\|encode"` | Source rows definitively refused by the destination, size limit, value validation or encoding. The host carries the class from the typed refusal site and counts a row when its message-error handling settles its rejection. A container encoding failure remains `encode` even when its public message error is `validation/encode`. |
| `nervix_emitter_records_failed_total` | None | Previously unresolved source rows whose message-error handling completed after a terminal publish failure. Delivered, rejected and connector-staged rows are excluded. |
| `nervix_emitter_records_retried_total` | None | Unresolved source rows selected by each retried host publish attempt, after transport acquisition. Drain retries count too. A staged commit alone presents no source rows again. |
| `nervix_emitter_retries_total` | `cause="publish\|connect\|commit"` | Successfully scheduled retry deferrals and inline retry waits, classified by the typed operation being retried. |
| `nervix_emitter_retry_wait_seconds_total` | None | Sum of the delays actually scheduled for those retries, rather than elapsed time or a suggested backoff. |
| `nervix_emitter_opens_total` | `outcome="succeeded\|failed"` | Initial transport acquisition and every retained transport reacquisition. Failed local preparation counts once; successful local preparation delegates the outcome to acquisition. A ready instance requires no new acquisition. A cancelled acquisition counts as failed. |
| `nervix_emitter_pool_waits_total` | None | Pending pooled-connection borrows that ended by success, failure or cancellation, attributed to the borrowing emitter. A borrow ready on its first poll contributes zero. |
| `nervix_emitter_pool_wait_seconds_total` | None | Elapsed monotonic wall seconds of those pending borrows, including failed and cancelled waits. |
| `nervix_emitter_confirmation_timeouts_total` | None | Host publish or commit attempts whose typed report contains `ConfirmationWindowError::AnswerTimedOut`, counted once per attempt. |
| `nervix_emitter_finishes_total` | `outcome="nothing_held\|flushed\|unsent\|failed"` | Each transport finish's typed outcome. Cancellation before an answer counts as failed. |
| `nervix_emitter_unsent_records_total` | None | Exact record counts reported by `EmitterFinishError::Unsent`, summed across finishes. |
| `nervix_emitter_transient_failure` | None | `1` while the current task has a published host or connector failure, otherwise `0`. Ending the task resets the observation to `0`; an ending predecessor cannot clear its replacement. |

These counters describe host attempts and source membership. A connector can perform several
external calls during one attempt. Retrying a record with an uncertain result can duplicate its
external delivery; an already resolved source row contributes no further retry count. The
existing sent-traffic counter continues to count each delivered source row once.
A forced flush or live drain inherits an already pending retry's classification on its first
attempt, without scheduling another delay.

The gauge samples the existing coherent failure publication. Clearing the host failure leaves a
connector-owned failure visible, and clearing the connector failure leaves a host-owned failure
visible. A successful reacquisition can therefore coexist with a failure while publication or
commit still has unresolved work. Use the gauge with the retry families to distinguish current
unavailability from historical failures and recovery attempts.

### Emitter payload preparation

The [Emitter Batching](./emitter-batching.md#inspection-and-tuning) architecture chapter connects
these observations to packing boundaries, retry ownership and scoped performance measurements.

Payload observations use the same five identity labels as destination attempts. They have no
branch-key, record, request or error-text label. Every child exists at zero when its emitter starts.
`DESCRIBE EMITTER` includes the same totals in its owner-routed `destination:` section.

| Family | Unit and meaning |
| --- | --- |
| `nervix_emitter_payloads_total` | Successfully encoded or sized logical payload preparations. |
| `nervix_emitter_payload_source_messages_total` | Source-record memberships in those preparations. |
| `nervix_emitter_payload_encoded_bytes_total` | Exact encoded bytes at the payload's documented measurement boundary. |
| `nervix_emitter_payload_size_bound_bytes_total` | The corresponding exact size bounds; these equal measured encoded bytes because Nervix measures rather than estimates. |
| `nervix_emitter_prepared_bytes_total` | Prepared content bytes, including connector preparation such as a Sentry envelope, counted once per preparation. |
| `nervix_emitter_messages_per_payload` | Histogram of source records per prepared payload. |
| `nervix_emitter_payload_splits_total` | Candidate halvings caused by an encoded-size limit, excluding count and metadata boundaries. |
| `nervix_emitter_work_messages` | Current source rows by `state="buffered"`, `prepared`, `in_flight` or `staged`. |
| `nervix_emitter_prepared_payloads` | Logical payloads whose bytes and membership the host retains for an answer. |
| `nervix_emitter_prepared_bytes` | Bytes of the prepared content the host currently retains. |

For codec emitters, encoded bytes mean the codec's completed record or container before connector
preparation. Keys, headers and transport framing are excluded, as they are from `MAX SIZE`.
HTTP measures its codec body, or zero for `WITHOUT BODY`; native client emitters measure Arrow IPC;
OTEL measures the prepared export request. Database emitters use the exact insert or document
boundary in [Database writes](emitters.md#database-writes). Iceberg counts the rows and physical
Parquet bytes of each prepared data file. These quantities keep their own units: the existing
`nervix_bytes_total` remains the emitter's Arrow and metadata estimate of delivered source records.
Prepared content is measured before a driver's framing or compression. A Sentry envelope can
therefore increase prepared bytes beyond the codec bytes bounded by `MAX SIZE`, while most codec
emitters prepare their bytes unchanged. Native row emitters use their documented measured request
boundary for both quantities.

A preparation is observed before its destination outcome. It can subsequently succeed, be rejected
or remain uncertain, and therefore does not mean delivery. A retained record, HTTP request, OTEL
export or client IPC payload is observed once: resending its bytes does not increase preparation
totals or the membership histogram. A native row emitter that reconstructs a request on retry
observes that new preparation. Driver request grouping and row isolation do not turn a logical
payload into several observations; these families describe the sized candidates, rather than
protocol calls or bytes on a socket. Use destination attempt outcomes alongside sent source-record
totals to distinguish retries and confirmed progress.

A native row publish loop observes each sized request when it visits that request for submission.
Requests left unvisited by an interrupted loop add no preparation observation.

A row refused before a bounded payload is produced adds no successful payload preparation.
A later destination rejection can follow a successful preparation and does not decrement
that measurement. Structured message errors and existing error-relay traffic remain the rejection
evidence; payload counters do not define a second error classification or count those rows as sent.

Work states overlap. `buffered` counts rows of Arrow carriers the host still holds, including rows
already prepared or partly answered until those carriers leave the buffer. `prepared` counts the
source members of retained payloads. `in_flight` counts source rows handed to a connector in an
active write; a native row write can have in-flight rows without host-retained prepared bytes.
`staged` counts rows waiting for a separate commit. Do not sum these states. Owner destruction
withdraws its gauge contribution, including cancellation and terminal teardown, while counters keep
their process-local history across task restarts.

## DESCRIBE Metrics

`DESCRIBE` commands include metric summaries for the described target when metrics exist. For a
scheduled target, the request is answered from its owner. An ingestor that runs on every live node,
such as an `HTTP ENDPOINT` ingestor, has no single owner, so the node that receives
`DESCRIBE INGESTOR` answers for its own instance. Node traffic is grouped under
`incoming_edges` and `outgoing_edges`; relay descriptions keep owner-buffer utilization under
`relay_buffers` rather than mixing relay traffic into the relay node:

```plexus
DESCRIBE RELAY notifications WHERE (user_id = 42);
DESCRIBE INGESTOR kafka_notifications;
DESCRIBE JUNCTION route_notifications;
DESCRIBE DEDUPLICATOR dedup_txns;
DESCRIBE REINGESTOR repartition_notifications;
DESCRIBE WINDOW PROCESSOR latency_window;
DESCRIBE EMITTER kafka_notifications_out;
DESCRIBE HASH MAP user_profiles;
DESCRIBE DOMAIN;
```

`DESCRIBE DOMAIN` is answered by the node that receives it and summarizes active-domain traffic
from that node's own metric state. Traffic is counted on the node that observed it, under that
node's `physical_node`, so the traffic another node observed is in that node's answer: run
`DESCRIBE DOMAIN` on each node to see the whole cluster. Its
`input_output` section aggregates ingestor and emitter metrics. Its `processed`
section aggregates metrics for all runtime nodes in the domain, including
processing nodes.

`DESCRIBE EMITTER` reports a `status:` of `OK`, `WAITING`, or `ERROR`, with a `detail:` line
explaining anything other than ordinary operation. `WAITING` means the emitter is healthy but holds
no connection because it has asked a shared client's pool for one and has not been given it yet;
see [Database Client Connection Pools](database-client-pools.md). A full pool is a waiting state,
not a failure, so an acquisition timeout or a lost connection is reported as `ERROR` instead.

`DESCRIBE INGESTOR` also reports the declared `quiesce:` policy, current `quiesce state:`, all
four quiesce values and the node's `nervix_ingestor_transport_redeliveries_total`,
`nervix_ingestor_decode_rejections_total` and `nervix_ingestor_oversized_payloads_total`, using
their Prometheus family names, and
`last decode rejection:`, the latest decode rejection on that node or `-`. A connected ingestor in
any active mode is reported as `status: quiesced`, not `stopped`. Per-payload activity remains at
debug or trace log levels and payload values are never logged.

Counter summaries include:

- `total`: accumulated raw counter value
- `wall_rate_per_sec`: total divided by wall-clock elapsed time since the local series started
- `domain_rate_per_sec`: total divided by the observed domain-time span, when records carry domain timestamps
- `wall_rate_ema_1m_per_sec` and `wall_rate_ema_15m_per_sec`: exponentially decayed wall-clock rates
- `domain_rate_ema_1m_per_sec` and `domain_rate_ema_15m_per_sec`: exponentially decayed domain-clock rates

Histogram summaries include:

- `p50_1m`, `p90_1m`, `p99_1m`: one-minute wall-clock decayed percentiles
- `p50_15m`, `p90_15m`, `p99_15m`: fifteen-minute wall-clock decayed percentiles
- `domain_p50_1m`, `domain_p90_1m`, `domain_p99_1m`: one-minute domain-clock decayed percentiles
- `domain_p50_15m`, `domain_p90_15m`, `domain_p99_15m`: fifteen-minute domain-clock decayed percentiles

Graph-edge histogram `DESCRIBE` lines do not include raw `count` / `sum` values or rates. For `messages_per_batch`, the raw observation count is the batch count and the raw sum is the message count; those are already reported clearly by `batches_total` and `messages_total`. The histogram answers distribution questions such as typical and tail batch size. Percentiles are rendered as decimal estimates interpolated within the configured histogram bucket range rather than as raw bucket boundary labels. The same rule applies to delivery latency: `messages_total` and `batches_total` answer throughput questions, while `delivery_latency_seconds` answers latency distribution questions. Connector-interaction durations use the separate `count` and `sum_seconds` summary described above.

Relay buffer summaries use `relay_buffer_len`. The percentile values are queued batch slots
observed in the single buffer on the relay owner, and `capacity=<n>` shows that buffer's declared
bound. A branched relay can hold interleaved concrete branches in the same owner buffer, so
`DESCRIBE RELAY` reports observed lengths as percentiles instead of rendering a separate current
depth per branch. The fixed one-batch producer and consumer-node dispatch slots are not additional
relay-buffer metric series.

A `-` value means the derived value is not available. This is common for domain-clock values when no domain timestamp has been observed or when the observed domain-time span is zero.

## Inferencer State

Every inferencer execution unit registers its four state families at zero when it starts, including
a stateless unit. They use the processor label set with `target_kind="INFERENCER"`,
`direction="state"`, and `relay`, `peer_kind` and `peer` set to `-`. The unit supplies `partition`
and `instance`; concrete branch keys are never labels. State replicas do not register another
execution owner's series. The series are withdrawn when their unit and retained reporting handles
end.

`nervix_inferencer_state_bytes` counts declared native `F32` bytes once for each active branch's
state session. Ending that session releases its count. It excludes ONNX session allocations,
snapshot framing, temporary encoding buffers and stored replica snapshots. The initialization
counters distinguish a branch restored from a matching generation from one started from `INITIAL`;
the latter includes a fresh branch and a branch whose previous incarnation cannot be reused.

`nervix_inferencer_snapshot_failures_total` counts failed snapshot publication and persistence
attempts, including a failed final write. Failures log at `warn` at most once per unit per
snapshot interval, including during shutdown. Every failure increments the counter even when
its warning is suppressed. Input rows do not trigger snapshot-failure logs.

`DESCRIBE INFERENCER` keeps the static model declarations and each unit's published-state counts,
bytes and revision progress in typed fields before rendering text. Its counts read the published
generations and can lag the live session until publication. Each revision summary is a maximum
over independent branch-local sequences at that stage. It is not a common checkpoint or a
readiness verdict. Replica progress is the owner's latest confirmed observation; unavailable
progress stays unavailable, and a known revision zero stays zero. No tensor or branch-key value
is read to produce the summary.

## Branch Lifecycle Signals

Prometheus exposes branch lifecycle state without a concrete branch-key label:

- `nervix_branch_instances{domain,branch,physical_node_id}` is a gauge of live concrete branch
  keys on that physical node. A key is counted once even when multiple local graph nodes hold
  runtime state for it.
- `nervix_branch_evictions_total{domain,branch,physical_node_id,reason}` counts each concrete key
  once when branch eviction begins on that physical node, even if multiple local graph nodes hold
  the key. `reason="lru"` is a `MAX INSTANCES ... EVICT LRU` capacity eviction; `reason="ttl"` is
  idle expiration.

Normal shutdown, schedule replacement, and runtime detachment reduce the live gauge but do not
increment the eviction counter. Relay branch presence and its TTL or LRU decisions exist only on
the relay owner, so a relay-owner failure loses those live values. A schedule replacement reduces
state only for runtime nodes whose assignment changed; a drain, failover, or colocation
consolidation leaves unaffected owners continuous. Lifecycle metrics are live process-local
Prometheus state and are not persisted or replicated.

Concrete branch-local inspection remains available through `DESCRIBE RELAY <relay> WHERE (...)`.
`DESCRIBE` does not provide a common branch inventory or eviction history. Prometheus deliberately
has no branch-key label. See
[Capacity Planning For Branched Graphs](capacity-planning.md) for the cost model.

## Wall Clock And Domain Clock

Nervix reports two time bases because they answer different questions:

- wall-clock rates describe actual handled load per second of real process time
- domain-clock rates describe records per second of event/domain time

For unpaced domains or records without usable timestamps, domain-clock values may be unavailable. For paced domains, domain-clock values follow the event timestamps and domain pace rather than the speed of test execution or wall-clock ingestion.

The moving rates are online exponential summaries. The percentile windows keep one bounded HDR
histogram per step, 10 seconds for the one-minute windows and one minute for the fifteen-minute
windows, rather than every observation. Both keep memory bounded and let metric state be
snapshotted and replicated without retaining the observations themselves.

## Replication And Drain Behavior

Nervix maintains internal metric state for `DESCRIBE`, edge statistics, and runtime recovery.
Stateful processor summaries may use their node-owned snapshot path. Relay metrics are different:
the relay owner is their sole authority, and buffer, traffic, concrete-presence, and branch-local
relay summaries are neither persisted nor replicated.

`DESCRIBE RELAY` and `DESCRIBE RELAY ... WHERE (...)` are routed to that authority. A planned move
fences the moved subgraph, drains admitted work, and activates the committed revision before the
gate opens. Relay metrics start fresh on the new owner because they do not travel with materialized
state. When a state-replica slot exists, a live former primary becomes the first replica candidate
after the move.

Only moved runtime nodes cross that boundary. Internal `DESCRIBE` and edge metrics for unaffected
nodes and concrete branches remain continuous during drain, graceful-shutdown drain, and placement
consolidation. There is no shutdown-specific metric family: a draining node is identified by its
phase log records and by `terminating: true` in its status, as described in
[Shutdown And Recovery](shutdown.md#observability). A timed-out precommit handoff leaves the schedule and metric authority unchanged.
Unexpected owner loss continues through failover immediately and starts fresh relay metrics on the
new owner.

Prometheus export is a separate, live process-local registry. Traffic metrics ignore branch
identity, while lifecycle metrics retain only the bounded declared branch name. Prometheus
registry values are not snapshotted into Nervix internal metric state and do not migrate during a
planned handoff or failover.

## Ingestor Unit Metrics And Recovery

`DESCRIBE INGESTOR` queries each unit's current owner and reports its global index, owner, running
state and readiness. Its edge metric lines render `instance=<i>`. Ingestor-specific quiesce,
redelivery, decode-rejection, oversize, header-drop, read-ahead-drop, unfolding-refusal and
endpoint-request metrics use `domain`, `ingestor`, `physical_node_id`, `instance`, `connector` and
`entity`; a `Single` ingestor uses `instance="-"`.

A `BranchAggregated` checkpoint belongs to an exact execution unit. Capture includes that unit's
series with each series' physical-node attribution intact. Installation replaces only that unit's
namespace, preserving a colocated sibling's live counters. After a move, later checkpoints retain
both the predecessor's and the new owner's contributions. A final snapshot selects its bytes
before a departing owner withdraws the unit's live series. Raw Prometheus recorders belong to
their local assignment lifetime and are not restored from checkpoints; durable summarized
metrics use the checkpoint store.
Inspection discovers the current units through a key-only checkpoint listing, including before a
stopped ingestor is restarted, and never substitutes one sibling's measurements for another's.

## Ingestor Source Interactions

`DESCRIBE INGESTOR` groups its existing seven quiesce, redelivery, decode-rejection and oversized
lines under `source:`, followed by read/poll interaction lines. Each unit reports its own counters
through its execution owner; remote inspection carries the same ordered lines. Counter lines
include totals and wall rates in the connector-interaction format; domain rates are `-`. The poll
histogram reports `count` and `sum_seconds`, using the same duration buckets as emitter interactions.

A returned nonempty message batch is `delivered`; a successful answer carrying no payload, including
an oversized-only answer, a resume request or end of input, is `empty`. A read returning an error is
`failed`. A broker's interrupted answer is `interrupted` even when it returns a gathered prefix;
that prefix still contributes its raw payloads and bytes and follows the existing intake path.
A paced poll cancelled by a quiesce change or stop is `interrupted`. Partial per-record poll
failures retain their existing error reporting; the poll's payload presence determines its
successful read outcome. A driver error that prevents a poll is `failed`.

Raw payload and byte totals measure the batches a broker read or paced poll returned, before
decoding, filtering, timestamp admission or waiting for unfolding capacity. Replaying a retained
buffer does not increment them; a transport redelivery is a new read and does. Request-scoped
endpoints and client producers perform no transport read or scheduled poll, so their read counters
stay zero. Their intake and refusal counters keep their own meaning. Only paced polls contribute
duration samples: a broker read waits for arrivals, so its elapsed time is not measured.

An `HTTP ENDPOINT` or `WEBSOCKETS ENDPOINT` unit follows its read/poll lines with one
`nervix_ingestor_endpoint_requests_total` line for each outcome, with its total and wall rates.
These ingestors execute on every live node, so `DESCRIBE INGESTOR` reports the counts of the node
that answers it, as it does their quiesce counters, and each node's `/metrics` reports its own.
Every request, and every WebSocket data frame, is counted once for each ingestor bound to its
route, under the outcome that ingestor reached:

| `outcome` | The ingestor |
| --- | --- |
| `accepted` | took it in: in `MODE NO ACK` it dispatched it into its graph or its endpoint `BUFFER` retained it (HTTP 202); in `MODE ACK` its acknowledgement root completed (HTTP 200, `completed`). |
| `busy` | had no free slot in its `MODE ACK MAX` response window (HTTP 503, `not_admitted` with `busy`). |
| `refused` | refused it: its quiesce refused new intake, such as `ON QUIESCE REJECT` while it is quiesced or a full endpoint `BUFFER` (HTTP 503), or in `MODE ACK` one of its routes refused the decoded input (HTTP 400). |
| `not_admitted` | could not have it unfolded because the node's extension workers had no room, which `nervix_ingestor_unfolding_refused_total` counts too (HTTP 503). |
| `decode_rejected` | consumed it as a decode rejection because its codec refused the payload, which `nervix_ingestor_decode_rejections_total` counts too (HTTP 202 in `MODE NO ACK`, 400 in `MODE ACK`). |
| `processing_failed` | admitted it without completing it: its `MODE NO ACK` dispatch into the graph failed (HTTP 202), or its `MODE ACK` acknowledgement root failed, its `ACK TIMEOUT` expired or its intake stopped while the response was held (HTTP 503). |

A route refuses a request before reading its body, or a WebSocket upgrade, only when none of its
ingestors can take it, and counts that refusal once for each of them there. Otherwise every
ingestor counts the request when it is dispatched to it, so an ingestor whose quiesce refuses the
requests of a route it shares with an available ingestor counts each of them once, in this family
and in `nervix_ingestor_quiesce_rejected_total` alike. A request the listener answers before it
consults any ingestor, such as one for an unknown route, with a method other than `POST`, whose body
could not be read or that arrives while the node shuts down, is counted by none.

All bounded children start at zero. Read recording touches retained atomic children, with no label
resolution or registry access. Counters are process-local observations, not durable delivery
positions; a completed read does not imply that decoding, filtering or graph execution accepted its
payload. Retirement withdraws only the exact unit's series and leaves siblings intact.

### Ingestor Acknowledgement And Lifecycle

These host observations carry the same six identity labels as the read families. Every bounded
child exists at zero when the unit is registered. Counters retain their process-local history
across task restarts of the same unit; the failure gauge describes the current task. `DESCRIBE
INGESTOR` lists every family in `source:` after the read lines, and an endpoint's request lines,
with its full selector and `total`, `count` and `sum_seconds` for the duration histogram, or
`value` for the gauge.

| Family | Additional labels | Meaning |
| --- | --- | --- |
| `nervix_ingestor_ack_outcomes_total` | `outcome="acknowledged\|failed\|timed_out\|shutdown"` | One per acknowledgement wait the ingestor settled: a batch under batch settlement, or each delivery a RabbitMQ `ACK PARALLEL` ingestor settles individually. `failed` is a tree the graph failed, such as an emitter that refused a record; `timed_out` is a tree that reported no progress for the ingestor's `ACK TIMEOUT`; `shutdown` is a wait that the ingestor's stop ended. |
| `nervix_ingestor_transport_acknowledgements_total` | `operation="acknowledge\|reject"`, `outcome="succeeded\|failed"` | Every acknowledgement or rejection of read positions the ingestor's transport completed, including a rejection the host retries after resuming. |
| `nervix_ingestor_transport_acknowledgement_duration_seconds` | `operation="acknowledge\|reject"` | Elapsed wall seconds of each of those transport operations, such as a Kafka consumer-group commit or an SQS deletion, failed operations included. It uses the connector duration buckets of [Connector Interactions](#connector-interactions). |
| `nervix_ingestor_resumes_total` | `outcome="ready\|waiting\|failed"` | Every resume of the ingestor's connector that completed: ready to read, waiting for an external assignment, or failed. A resume a stop or a quiesce change ended before it completed is not counted. |
| `nervix_ingestor_suspends_total` | `cause="quiesce\|read_failure\|rejection"` | Every suspension of a ready connector: by a quiesce that suspends intake, by a read that failed and left the connector needing a resume, or because a rejection failed and the transport must be reopened before the same positions are read again. A connector asking for a resume without a failed read, such as after a Kafka domain-offset reassignment, counts only its resume. |
| `nervix_ingestor_unconfirmed_stop_positions_total` | None | Positions the ingestor read whose acknowledgement trees had not all resolved when it stopped. Their transport presents them again by its own redelivery contract. |
| `nervix_ingestor_transient_failure` | None | `1` while the current task has a published transient failure, which `DESCRIBE INGESTOR` shows as its transient error, otherwise `0`. Ending the task resets the observation to `0`; an ending predecessor cannot clear its replacement. |

Broker ingestors record every family. Paced ingestors acknowledge nothing on a transport, so they
record resumes, suspensions and the transient failure. An endpoint answers its sender and a client
producer its batch's outcome instead, so their acknowledgement, transport, lifecycle and stop
counters stay at zero. Endpoints count their requests in `nervix_ingestor_endpoint_requests_total`,
and client producers keep the outcomes of [Client Ingestors](#client-ingestors).

An ACK timeout is not a failed tree. The tree that went silent is not resolved by the timeout: the
ingestor rejects the batch, its transport presents the positions again, and the earlier delivery
may still complete. The ingestor's error event names the expired `ACK TIMEOUT` as a timeout, and a
failed tree's event keeps the reason its failing branch gave. Transport durations start when the
ingestor asks its transport to acknowledge or reject and end when the transport answered, as far as
its [acknowledgement confirmation](./connector-contract.md#confirmed-acknowledgements) reaches, so
they exclude the graph wait. A broker ingestor's transient failure clears once its transport
confirms an acknowledgement again; a paced ingestor's clears once a poll or resume succeeds.
Recording touches retained atomic children, with no label resolution or registry access, and
changes no delivery, retry or error policy.
