# Ingestors

Ingestors are the entry points into the runtime graph.

A typical ingestor:

```nspl
CREATE BRANCH by_user
  SCHEMA user_branch TTL 5m;

CREATE [IF NOT EXISTS] INGESTOR kafka_notifications
  TO notifications
  DECODE USING notification_codec
  BRANCHED BY by_user
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  TIMESTAMP NOW
  FROM KAFKA kafka_main
  TOPIC notifications
  OFFSET BY CONSUMER GROUP nervix_consumer
  INSTANCES 1
  MODE ACK SEQUENTIAL
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Every ingestor defines:

- the destination relay or relays
- the codec used for decoding
- the explicit branch to use, or `UNBRANCHED`
- the batch flush interval
- the timestamp source
- the transport-specific source
- the delivery mode
- optional node-level `FILTER WHERE` and per-route `SET` / `UNSET` / `WHERE` programs

`FLUSH EACH <duration> MAX BATCH SIZE <bytes>` or `FLUSH IMMEDIATE` is required on ingestors and configures the ingestor-local batcher.

At runtime, the ingestor:

- decodes inbound payloads into runtime records
- optionally executes `FILTER WHERE` against the decoded input batch
- resolves the concrete branch group from the referenced `CREATE BRANCH`
- accumulates decoded rows for that group until the flush interval fires
- writes buffered rows into each matching destination relay inside that group's branch

## Branch Semantics

Ingestors are where external mixed flows enter branch-isolated processing. The ingestor references an explicit branch and owns the record-to-key value mapping. The branch declaration owns the key schema, TTL, and optional LRU eviction policy:

- `CREATE BRANCH <branch> SCHEMA <schema> TTL <duration>` declares the branch key schema and lifetime
- `BRANCHED BY <branch> VALUES { field = relay.field, ... }` computes the group from direct values on the outgoing relay record
- `MAX INSTANCES <n> EVICT LRU` may be added to cap active concrete branch instances and evict the least recently used branch when capacity is reached
- `BRANCHED BY <branch>` tells the ingestor to use that explicit branch
- `UNBRANCHED` uses the single root group without declaring or referencing a branch schema, and does not declare TTL
- the named branch defines both the branch identity and its key shape; downstream relays and branch-preserving processors must reference that same branch name
- decoded rows are appended to matching destination relays inside that group's branch
- downstream normal processors keep the same group until a `REINGESTOR` or `EMITTER` boundary

Per-group behavior such as downstream deduplication, reordering, and window aggregation stays scoped to that branch.

Batching follows that same rule:

- an ingestor buffers independently per concrete branch group
- `UNBRANCHED` produces one root branch and one batcher

Client-backed ingestors can use resource-mounted client config values for TLS material and other file-based settings. See [Resources](resources.md#client-config-mounts).

## Filter-Map Programs

Ingestors may declare an optional arrival filter and per-route filter-map clauses:

```nspl
CREATE BRANCH by_tenant
  SCHEMA tenant_branch TTL 5m;

CREATE [IF NOT EXISTS] INGESTOR notifications_in
  FILTER WHERE message.active
  TO notifications
    SET notifications.amount = message.amount + 1, notifications.normalized = lower(message.raw)
    UNSET notifications.raw
    WHERE message.tenant = 'acme'
  DECODE USING notification_codec
  BRANCHED BY by_tenant
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

`FILTER WHERE` is evaluated after codec decoding and before the ingestor writes into any destination relay. Sources that naturally decode batches can evaluate the filter over the whole Arrow batch; sources that receive individual messages evaluate a single-row batch. Each `TO` route then applies its own `SET` / `UNSET` / `WHERE` program before writing to that destination.

Supported blocks:

- `SET <destination_relay>.<field> = <expr>, ...`: rewrites existing fields or appends new fields on the emitted row shape
- `UNSET <destination_relay>.<field>, ...`: removes fields from the emitted row shape
- `WHERE <expr>`: drops rows whose predicate is false or null

General notes:

- `SET` is a single clause with comma-separated assignments
- assignments execute left to right; repeated destination fields are allowed and later expressions read the latest preceding value
- `UNSET` is a single clause with comma-separated field names
- if multiple filter-map blocks are present, they must appear in `SET`, `UNSET`, `WHERE` order
- expressions are validated against the ingestor input scope; do not rely on `WHERE` to reference fields added by `SET`
- `message.<field>` is the readonly decoded input record; the destination relay namespace is the writable output
- `message` is reserved for the decoded input record and cannot be used as a relay name
- all node expressions use explicit typing; there is no implicit cast insertion
- nested predicates and nested/chained builtin calls are supported
- leader-side validation checks that identifiers bind to the input schema and that the post-`SET` / post-`UNSET` schema matches the destination relay schema
- the program source is stored as NSPL, not serialized bytecode
- ingestor payload fields are referenced through `message.<field>`
- source-specific transport metadata may be exposed through `metadata.<field>` when the ingestor supports it; Kafka currently provides `metadata.topic`, `metadata.partition`, and `metadata.offset`
- supported sources expose transport headers through `read_header(name)` and `read_headers(name)`; header names may be any `STRING` expression, and these functions may be used in both top-level `FILTER WHERE` and per-route filter-map expressions

Useful built-ins include string, null-handling, numeric, regex, and contextual functions such as `lower`, `coalesce`, `abs`, `regexp_like`, `now`, and `uuid_v7`.

See [Filter-Map Functions](filter-map-functions.md) for the full function reference.

Common expression patterns include:

- nested conditions such as `(active AND amount > 5) OR NOT flagged`
- chained calls such as `lower(trim(raw))`
- arithmetic expressions such as `(amount + fee) / divisor`
- explicit casts such as `raw AS INT64`

The filter-map type surface matches the full Nervix internal schema type set:

- `U8`, `I8`, `U16`, `I16`, `U32`, `I32`, `U64`, `I64`
- `F32`, `F64`
- `BOOL`, `STRING`, `DATETIME`

`DATETIME` is the internal logical timestamp type. JSON or AVRO string wire values require `ENCODE <field> AS RFC3339` on the codec before they can decode into the named `DATETIME` field.

### Header Context

Some ingestors receive additional source data alongside the decoded payload body. They expose it through two functions:

- `read_header(name)` returns the first value as an optional `STRING`, or `NULL` when the header is absent
- `read_headers(name)` returns every value in transport order as a non-null `VEC<STRING>`, or an empty vector when the header is absent

The `name` argument can be any expression that returns `STRING`. Both functions are available in top-level `FILTER WHERE` and in per-route `SET` / `WHERE` expressions for these sources:

- HTTP endpoints and HTTP client polling expose HTTP headers with UTF-8 values
- WebSocket endpoints expose UTF-8 headers from the opening HTTP upgrade request
- Kafka exposes Kafka record headers
- NATS exposes message headers
- Pulsar exposes message properties
- RabbitMQ exposes AMQP message headers, converting typed AMQP values to strings
- SQS exposes message attributes, converting string, number, and binary values to strings

Header values are not part of `message`, and all values are exposed as strings. To route a header downstream, assign it explicitly, for example:

```nspl
SET notifications.route = read_header(lower(message.route_header))
WHERE read_header("tenant") = message.tenant
```

MQTT, Redis, Kinesis, Prometheus, and ZeroMQ ingestors do not support these functions. Leader-side validation rejects `read_header` or `read_headers` for a source without header support.

## TLS Client Configuration

For outbound ingestor clients, TLS is configured on the `CLIENT`, not on the `INGESTOR`.

General pattern:

```nspl
CREATE [IF NOT EXISTS] CLIENT <name>
  TYPE <kind>
  MOUNT <tls_resource>
  CONFIG {
    ...
    'tls_ca_file' = '{{ tls_resource }}/ca.pem'
  };
```

Transport-specific schemes and keys:

- `KAFKA`: pass-through to librdkafka. Typically set `'security.protocol' = 'ssl'`, `'ssl.ca.location' = '{{ tls_resource }}/ca.pem'`, and if needed `'ssl.certificate.location'` plus `'ssl.key.location'`.
- `HTTP`: use an `https://...` endpoint. Nervix honors `tls_ca_file`, `tls_cert_file`, `tls_key_file`, and optional `timeout_ms`.
- `PROMETHEUS`: use an `https://...` `addr`. Nervix honors `tls_ca_file`, `tls_cert_file`, `tls_key_file`, and optional `timeout_ms`.
- `WEBSOCKETS`: use a `wss://...` endpoint. Nervix honors `tls_ca_file`, `tls_cert_file`, `tls_key_file`.
- `MQTT`: use `mqtts://...` in `addr`. Nervix requires `tls_ca_file` for server trust and also supports `tls_cert_file` plus `tls_key_file` for mTLS.
- `NATS`: use `tls://...` in `addr`. Nervix honors `tls_ca_file`, `tls_cert_file`, `tls_key_file`.
- `PULSAR`: use `pulsar+ssl://...` in `addr`. Nervix honors `tls_ca_file` and optional `tls_allow_insecure_connection` plus `tls_hostname_verification_enabled`. Pulsar client certificate authentication is not currently exposed.
- `KINESIS`: use an `https://...` optional `endpoint` for AWS-compatible targets. Nervix honors `tls_ca_file`; local/test targets can also set `region`, `access_key_id`, `secret_access_key`, and `start_position`.
- `RABBITMQ`: use `amqps://...` in `addr`. Nervix honors `tls_ca_file`.
- `REDIS`: use `rediss://...` in `addr`. Nervix honors `tls_ca_file`, `tls_cert_file`, `tls_key_file`.
- `SQS`: use an `https://...` `endpoint`. Nervix honors `tls_ca_file`. This is primarily useful for SQS-compatible local/test endpoints.

Example Kafka TLS client:

```nspl
CREATE [IF NOT EXISTS] CLIENT kafka_tls
  TYPE KAFKA
  MOUNT dev_tls
  CONFIG {
    'bootstrap.servers' = '127.0.0.1:9094',
    'security.protocol' = 'ssl',
    'ssl.ca.location' = '{{ dev_tls }}/ca.pem'
  };
```

Example HTTP TLS client:

```nspl
CREATE [IF NOT EXISTS] CLIENT http_tls
  TYPE HTTP
  MOUNT dev_tls
  CONFIG {
    'endpoint' = 'https://127.0.0.1:18443/http/notifications',
    'method' = 'GET',
    'timeout_ms' = 5000,
    'tls_ca_file' = '{{ dev_tls }}/ca.pem'
  };
```

## Supported Ingestor Types

### HTTP Client Polling

```nspl
FROM HTTP <client> EVERY <duration>
```

- polls a configured HTTP endpoint periodically
- `204 No Content` is treated as no message

### Kafka

```nspl
FROM KAFKA <client>
TOPIC <topic>
OFFSET BY CONSUMER GROUP <group>|DOMAIN
INSTANCES <count>
MODE ACK PARALLEL MAX <n>|ACK SEQUENTIAL|NO_ACK PARALLEL MAX <n>
```

Kafka is the richest ingestion surface today.

Offset modes:

- `CONSUMER GROUP`: Kafka manages offsets
- `DOMAIN`: Nervix stores the next offset in replicated runtime state and commits partition-to-instance assignment in the Raft-backed domain schedule

`OFFSET BY DOMAIN` is at-least-once because crash recovery may restart from a slightly stale persisted offset snapshot. The leader watches Kafka partition topology and commits any rebalance through the strongly consistent domain schedule, which is persisted through the control-plane Raft/Fjall path. Executing ingestors consume only the committed partition assignment.

Offset recovery details:

- persisted per-partition offsets are clamped to the partition's currently available Kafka watermark range on reassignment
- if a partition appears later and has no stored domain offset yet, unpaced domains start from the normal default behavior, while paced domains seek from the domain's current logical time

### Pulsar

```nspl
FROM PULSAR <client>
TOPIC <topic>
SUBSCRIPTION <subscription>
INSTANCES <count>
MODE ACK PARALLEL MAX <n>|ACK SEQUENTIAL|NO_ACK PARALLEL MAX <n>
```

Pulsar ingestors use Nervix-managed shared subscriptions. The subscription
name is still required by Pulsar, but subscription type is not exposed in NSPL.
Client config currently supports:

- `'addr'`: broker address such as `'pulsar://127.0.0.1:6650'`
- optional `'namespace'`: defaults short topic names to `persistent://public/default/<topic>`; fully qualified topic names are accepted as-is
- optional `'tls_ca_file'`: PEM-encoded CA bundle for `pulsar+ssl://...` connections
- optional `'tls_allow_insecure_connection'`: `true` or `false`; defaults to `false`
- optional `'tls_hostname_verification_enabled'`: `true` or `false`; defaults to `true`

Pulsar TLS currently supports server trust configuration only. Nervix does not yet expose Pulsar client certificate authentication.

### Kinesis

```nspl
FROM KINESIS <client>
RELAY <relay>
INSTANCES <count>
MODE ACK SEQUENTIAL
```

- Kinesis maps cleanly to the emitter-side "publish bytes to a named relay" model
- `UNBRANCHED` always means the single root branch
- transport keys such as the Kinesis partition key do not implicitly choose Nervix relay branches
- if you want branching by transport-derived data, decode that data into record fields and reference those fields from the ingestor `BRANCHED BY ... VALUES { ... }` mapping
- `INSTANCES` spreads open shards across local worker tasks on the assigned execution node
- unlike Kafka, there is no broker-managed `CONSUMER GROUP` clause here
- this first cut does not provide Kafka-style replicated durable checkpoints or rebalance scheduling; startup position is controlled on the `CLIENT` with `start_position = 'latest'|'trim_horizon'`

### RabbitMQ

```nspl
FROM RABBITMQ <client>
QUEUE <queue>
INSTANCES <count>
MODE ACK SEQUENTIAL
```

### Redis Pub/Sub

```nspl
FROM REDIS PUBSUB <client>
CHANNEL <channel>
MODE NO_ACK SEQUENTIAL
```

### MQTT

```nspl
FROM MQTT <client>
TOPIC <topic-filter>
[INSTANCES <count>]
[SESSION CLEAN|PERSISTENT]
[QOS 0|1]
MODE NO_ACK SEQUENTIAL
  | NO_ACK PARALLEL MAX <n>
  | ACK SEQUENTIAL ACK TIMEOUT <duration> RETRY POLICY BACKOFF <duration> MAX <duration>
  | ACK PARALLEL MAX <n> BATCH TIMEOUT <duration> ACK TIMEOUT <duration> RETRY POLICY BACKOFF <duration> MAX <duration>
```

MQTT topic filters may be bare identifiers or string literals for filters containing `/`, `+`, or `#`.

Delivery constraints:

- `NO_ACK` defaults to `SESSION CLEAN QOS 0`; explicit `SESSION` and `QOS` may be supplied before `MODE`
- `ACK` modes require `SESSION PERSISTENT QOS 1`
- `ACK PARALLEL MAX <n>` is the in-flight ACK window and `BATCH TIMEOUT` is the maximum partial-batch wait
- `INSTANCES <count>` controls Nervix consumer parallelism; MQTT delivery always uses Nervix-managed shared subscription groups so instances do not duplicate messages

### NATS

```nspl
FROM NATS <client>
SUBJECT <subject>
QUEUE GROUP <queue_group>
INSTANCES <count>
MODE NO_ACK SEQUENTIAL
```

NATS ingestors use Core NATS queue subscriptions. `QUEUE GROUP` and `INSTANCES`
are mandatory; use `INSTANCES 1` for a single queue member.

### ZeroMQ

```nspl
FROM ZEROMQ <client>
MODE NO_ACK SEQUENTIAL
```

### SQS

```nspl
FROM SQS <client>
QUEUE <queue>
INSTANCES <count>
MODE ACK SEQUENTIAL
```

### Prometheus

```nspl
FROM PROMETHEUS <client>
QUERY '<promql>'
EVERY <duration>
```

Prometheus samples are flattened into JSON before codec decoding.

### HTTP Endpoints

```nspl
FROM ENDPOINT <endpoint> MODE NO_ACK SEQUENTIAL
```

This is how Nervix receives inbound HTTP requests on its own server-side endpoints.

Server-side endpoints are hosted under a `VHOST`. A plain VHOST serves HTTP and WS on the HTTP listener. A TLS-enabled VHOST serves HTTPS and WSS on the separate HTTPS listener.

TLS is configured on the VHOST itself:

```nspl
CREATE [IF NOT EXISTS] VHOST edge api.example.com, ws.example.com
  WITH TLS tls_bundle;
```

or with an explicit pinned resource version:

```nspl
CREATE [IF NOT EXISTS] VHOST edge api.example.com, ws.example.com
  WITH TLS tls_bundle VERSION 3;
```

The referenced resource bundle must contain:

- `tls.crt`
- `tls.key`
- `ca.crt`

### WebSocket Clients

```nspl
FROM WEBSOCKETS <client> MODE NO_ACK SEQUENTIAL
```

This opens an outbound WebSocket connection and decodes text or binary frames.

Outbound WebSocket clients can declare `WITH SIGNALING PROTOCOL <name>` after
`TYPE WEBSOCKETS`. Server-side WebSocket endpoints can declare the same clause
after `TYPE WEBSOCKETS`. On connection, Nervix sends the configured bodies,
waits for the configured acknowledgement bodies, and buffers schema-conforming
data frames received before the handshake completes. Buffered frames are then
ingested in their original order before live frames continue.

## Instancing

`INSTANCES <count>` is currently supported on source types that can safely scale through competing consumers or parallel pollers on one node:

- Kafka
- Kinesis
- MQTT
- RabbitMQ
- SQS

If omitted, the default is `INSTANCES 1`.
