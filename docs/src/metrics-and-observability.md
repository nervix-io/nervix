# Metrics And Observability

Nervix exposes runtime graph metrics in two forms:

- raw Prometheus metrics at the observability server's `/metrics` endpoint
- local summarized metrics in `DESCRIBE` command output

The two surfaces use the same semantic labels where they overlap, but they use separate storage. Prometheus receives raw counters and histogram buckets through a Prometheus registry for external aggregation. `DESCRIBE` reads Nervix runtime state and includes derived values such as rates and recent percentiles.

## Observability Server

The observability listener exposes:

- `/livez`: process liveness
- `/readyz`: readiness once a leader is known
- `/metrics`: Prometheus text output for graph and allocator metrics

Use the node's observability address, not the data-plane HTTP listener:

```bash
curl http://127.0.0.1:<observability-port>/metrics
```

Prometheus metrics are intentionally branch-aggregated. They aggregate across concrete relay branches and do not include a branch key label. This keeps Prometheus cardinality bounded when a relay is parameterized by high-cardinality values such as tenant, user, account, or device id.

## Metric Labels

Graph metric series use these labels:

- `domain`: owning domain
- `target_kind`: runtime target kind, such as `RELAY`, `INGESTOR`, `DEDUPLICATOR`, `REINGESTOR`, `WINDOW_PROCESSOR`, `EMITTER`, or `LOOKUP`
- `target`: relay or node name
- `physical_node_id`: Nervix cluster node where the metric was observed
- `direction`: `received` or `sent`
- `stream`: logical relay associated with the observation, or `-` when no relay applies
- `peer_kind` and `peer`: relay peer labels for node-to-relay observations, or `-` when no peer applies
- `le`: Prometheus histogram bucket boundary

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
- `nervix_messages_per_batch`: histogram of message count per batch
- `nervix_delivery_latency_seconds`: histogram of delivery latency between graph nodes
- `nervix_relay_buffer_len`: histogram of runtime relay buffer occupancy in queued batches
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

## DESCRIBE Metrics

`DESCRIBE` commands include local metric summaries for the described target when metrics exist. Node traffic is grouped under `incoming_edges` and `outgoing_edges`; relay descriptions keep relay-local buffer utilization under `relay_buffers` rather than mixing relay traffic into the relay node:

```nspl
DESCRIBE RELAY notifications WHERE (user_id = 42);
DESCRIBE INGESTOR kafka_notifications;
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

Relay buffer summaries use `relay_buffer_len`. The percentile values are queued
batch slots observed on the relay consumer fan-out channel, and `capacity=<n>`
shows the bounded channel capacity for that runtime buffer. Parameterized relays
observe the internal branch-collapse fan-out channel; unparameterized relays
observe their direct consumer fan-out without inserting a collapse node. A
branched relay can receive interleaved concrete branches through the same
collapse point, so `DESCRIBE RELAY` reports observed buffer lengths as
percentiles instead of trying to render one current number per branch.

A `-` value means the derived value is not available. This is common for domain-clock values when no domain timestamp has been observed or when the observed domain-time span is zero.

## Wall Clock And Domain Clock

Nervix reports two time bases because they answer different questions:

- wall-clock rates describe actual handled load per second of real process time
- domain-clock rates describe records per second of event/domain time

For unpaced domains or records without usable timestamps, domain-clock values may be unavailable. For paced domains, domain-clock values follow the event timestamps and domain pace rather than the speed of test execution or wall-clock ingestion.

The moving rates and percentile windows are online exponential summaries rather than stored real-time windows. This keeps memory bounded and allows metric state to be snapshotted and replicated without retaining all observations.

## Replication And Drain Behavior

Nervix maintains two internal metric sets for `DESCRIBE`, edge statistics, and runtime recovery:

- global branch-aggregated metrics for node edge reporting
- concrete branch metrics for branch-local runtime state and `DESCRIBE RELAY ... WHERE (...)`

Branch-aggregated metrics are replicated as branch-aggregated runtime state. Concrete branch metrics travel with the concrete branch state they describe. This is why `DESCRIBE` and graph edge metrics survive node drain and restart paths in the same way as other replicated runtime state.

Prometheus export is a separate, live process-local registry. It intentionally ignores branch identity and reports aggregated raw values to avoid cardinality growth. Prometheus registry values are not snapshotted into Nervix internal metric state.
