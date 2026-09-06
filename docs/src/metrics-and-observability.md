# Metrics And Observability

Nervix exposes runtime graph metrics in two forms:

- raw Prometheus metrics at the observability server's `/metrics` endpoint
- owner-routed summarized metrics in `DESCRIBE` command output

The two surfaces use the same semantic labels where they overlap, but they use separate storage.
Prometheus receives raw counters and histogram buckets through a Prometheus registry for external
aggregation. `DESCRIBE` reads Nervix runtime state and includes derived values such as rates and
recent percentiles. Relay descriptions are routed to the scheduled relay owner; nonowners do not
maintain relay metrics.

## Observability Server

The observability listener exposes:

- `/livez`: process liveness
- `/readyz`: readiness once a leader is known
- `/metrics`: Prometheus text output for graph and allocator metrics

Use the node's observability address, not the data-plane HTTP listener:

```bash
curl http://127.0.0.1:<observability-port>/metrics
```

Prometheus metrics are intentionally branch-aggregated. They aggregate across concrete relay
branches and do not include a branch key label. This keeps Prometheus cardinality bounded when a
relay is branched by high-cardinality values such as tenant, user, account, or device id. Branch
lifecycle metrics identify the declared branch, not the concrete key.

## Metric Labels

Graph metric series use these labels:

- `domain`: owning domain
- `target_kind`: runtime target kind, such as `RELAY`, `INGESTOR`, `JUNCTION`, `DEDUPLICATOR`, `REINGESTOR`, `WINDOW_PROCESSOR`, `EMITTER`, or `LOOKUP`
- `target`: relay or node name
- `physical_node_id`: Nervix cluster node where the metric was observed
- `direction`: `received` or `sent`
- `stream`: logical relay associated with the observation, or `-` when no relay applies
- `peer_kind` and `peer`: relay peer labels for node-to-relay observations, or `-` when no peer applies
- `branch`: named branch declaration on branch lifecycle metrics
- `reason`: branch eviction reason, either `lru` or `ttl`
- `le`: Prometheus histogram bucket boundary

For a single-input emitter, sent metrics retain that input relay as `stream`. A multi-input
emitter's received metrics identify the actual source relay, while its sent metrics aggregate the
shared sink pipeline with `stream="-"` because one flush may contain work from several sources.

`DESCRIBE` output uses the same concepts but renders `physical_node_id` as `physical_node` for readability.

Example Prometheus series:

```text
nervix_messages_total{domain="prod",target_kind="RELAY",target="notifications",physical_node_id="node-1",direction="received",stream="notifications",peer_kind="-",peer="-"} 42
```

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
- `nervix_relay_buffer_len`: histogram of runtime relay buffer occupancy in queued batches
- `nervix_branch_instances`: current concrete branch keys with at least one runtime instance on the
  physical node
- `nervix_branch_evictions_total`: total concrete branches evicted on the physical node, split by
  `reason="lru"` or `reason="ttl"`
- `nervix_ingestor_quiesce_buffered_records`: raw payloads currently retained in an ingestor's
  per-instance quiesce buffers
- `nervix_ingestor_quiesce_buffered_bytes`: raw payload bytes currently retained in those buffers
- `nervix_ingestor_quiesce_dropped_total`: payloads deliberately discarded by `DROP`, buffer
  overflow, memory-pressure zero-capacity behavior, or an interrupting termination
- `nervix_ingestor_quiesce_rejected_total`: endpoint requests or connections refused while an
  ingestor cannot accept them
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

The four ingestor-quiesce families use `domain`, `ingestor`, and `physical_node_id` labels. Buffer
families are gauges; dropped and rejected families are monotonic counters. They are process-local:
quiesce buffers do not migrate during termination or failover.

## DESCRIBE Metrics

`DESCRIBE` commands include metric summaries for the described target when metrics exist. For a
scheduled target, the request is answered from its owner. Node traffic is grouped under
`incoming_edges` and `outgoing_edges`; relay descriptions keep owner-buffer utilization under
`relay_buffers` rather than mixing relay traffic into the relay node:

```nspl
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

`DESCRIBE DOMAIN` summarizes active-domain traffic from per-node metric state. Its
`input_output` section aggregates ingestor and emitter metrics. Its `processed`
section aggregates metrics for all runtime nodes in the domain, including
processing nodes.

`DESCRIBE INGESTOR` also reports the declared `quiesce:` policy, current `quiesce state:`, and all
four quiesce values using their Prometheus family names. A connected ingestor in any active mode is
reported as `status: quiesced`, not `stopped`. Per-payload activity remains at debug or trace log
levels and payload values are never logged.

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

Histogram `DESCRIBE` lines do not include raw `count` / `sum` values or rates. For `messages_per_batch`, the raw observation count is the batch count and the raw sum is the message count; those are already reported clearly by `batches_total` and `messages_total`. The histogram answers distribution questions such as typical and tail batch size. Percentiles are rendered as decimal estimates interpolated within the configured histogram bucket range rather than as raw bucket boundary labels. The same rule applies to delivery latency: `messages_total` and `batches_total` answer throughput questions, while `delivery_latency_seconds` answers latency distribution questions.

Relay buffer summaries use `relay_buffer_len`. The percentile values are queued batch slots
observed in the single buffer on the relay owner, and `capacity=<n>` shows that buffer's declared
bound. A branched relay can hold interleaved concrete branches in the same owner buffer, so
`DESCRIBE RELAY` reports observed lengths as percentiles instead of rendering a separate current
depth per branch. The fixed one-batch producer and consumer-node dispatch slots are not additional
relay-buffer metric series.

A `-` value means the derived value is not available. This is common for domain-clock values when no domain timestamp has been observed or when the observed domain-time span is zero.

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

The moving rates and percentile windows are online exponential summaries rather than stored real-time windows. This keeps memory bounded and allows metric state to be snapshotted and replicated without retaining all observations.

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
consolidation. A timed-out precommit handoff leaves the schedule and metric authority unchanged.
Unexpected owner loss continues through failover immediately and starts fresh relay metrics on the
new owner.

Prometheus export is a separate, live process-local registry. Traffic metrics ignore branch
identity, while lifecycle metrics retain only the bounded declared branch name. Prometheus
registry values are not snapshotted into Nervix internal metric state and do not migrate during a
planned handoff or failover.
