# Emitters

Emitters publish relay records to external systems.

A typical emitter:

```nspl
CREATE IF NOT EXISTS EMITTER kafka_notifications
  FROM notifications
  COLLECT FOR 10ms MAX BATCH SIZE 1MiB
  TO KAFKA kafka_main TOPIC notifications_out
    MODE ACK PARALLEL MAX 1000 ACK TIMEOUT 30s
      RETRY POLICY BACKOFF 250ms MAX 30s
    ENCODE USING notification_codec
  INHERIT ALL
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

An emitter defines:

- one or more source relays that declare the same payload schema
- an optional input collection policy
- the codec used for encoding
- the transport-specific sink
- the sink's explicit publishing mode, confirmation window and bound where applicable, and retry
  pacing
- the flush policy used to collect a batch before publishing
- whether the branch is `ATTACHED` or `DETACHED`
- route-local codec construction or a direct `VALUES` mapping
- optional ordered header invocations on supported codec sinks
- optional ordered materialized-state dependencies

## Branch Semantics

An emitter is the terminal consumer for its source relays. The `FROM` list uses the same
source-local predicate form as other relay-consuming nodes:

```nspl
CREATE EMITTER combined_notifications
  FROM primary_notifications WHERE input.source = 'primary',
       replayed_notifications WHERE input.source = 'replay'
  TO KAFKA kafka_main TOPIC notifications_out
    MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s
    ENCODE USING notification_codec
  INHERIT ALL
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Every listed relay must declare the exact same schema name. Unlike ordinary multi-input
processors, emitter inputs may be unbranched or use differently named branches. Each source keeps
its own branch identity until its records cross the successful external boundary.

That means:

- the emitter consumes from all concrete branches of every source relay
- each optional source `WHERE` is evaluated only for that relay
- the current branch remains available internally for compatible materialized-state lookup
- `branch.field` is unavailable to successful emitter expressions
- branch identity collapses only after successful external publication

A node-wide materialized-state dependency and an `ON MESSAGE ERROR SEND TO` route must be
exact-branch compatible with every source. Consequently, an emitter whose inputs use differently
named branches cannot configure one branch-bound dependency or error relay across those inputs.

All emitters declare `FLUSH EACH <duration> MAX BATCH SIZE <bytes>` or `FLUSH IMMEDIATE`. `FLUSH`
means Nervix collects an in-memory Arrow batch before handing it to the external sink. The
[NSPL Overview](nspl-overview.md) defines the `FLUSH IMMEDIATE` 100 µs minimum batching window.
For most emitters the collected batch is encoded and published on the flush boundary. Iceberg
additionally requires `COMMIT EACH <duration> MAX SIZE <bytes>` as part of its sink clause: flush
writes local Arrow IPC staging files, and commit appends the staged data to object storage. `ON MESSAGE ERROR SEND TO`
buffers failed-message error records separately and delivers them using the emitter's same `FLUSH`
interval or maximum batch-size boundary.

An emitter may place `COLLECT FOR <duration> [MAX BATCH SIZE <bytes>]` immediately after the
complete `FROM <relay> [WHERE ...] [, ...]` list. This input policy runs before emitter filtering,
construction, encoding, and the required output `FLUSH` policy. Omission means no additional input
collection: each incoming relay batch enters emitter execution directly. When configured,
collection is independent for each source relay and concrete branch and releases on the timer or
optional size boundary. Equal keys from differently named branches are never collected together.
Branch identity still collapses only after successful publication.

## Publishing modes

Every emitter sink requires `MODE <body>` as its final sink subclause, immediately before
`ENCODE USING` when the sink uses a codec. There is no implicit mode, confirmation window, ACK
timeout, or retry cadence. `SHOW CREATE EMITTER` and `DESCRIBE EMITTER` render the complete mode.

The shared variables are:

- `ACK SEQUENTIAL` publishes and confirms one record before sending the next.
- `ACK PARALLEL MAX <n>` permits at most `n` records to await confirmation, where `n` is at least
  one. Nervix fills the window, waits for the oldest confirmation when it is full, and completes a
  flush only after every record in that flush has been confirmed.
- `ACK TIMEOUT <duration>` bounds one asynchronous broker confirmation. Expiry is ambiguous, not a
  record rejection: Nervix retries the still-unconfirmed records as an infrastructure failure.
  The broker may have accepted a timed-out record, so confirming modes are at least once and can
  duplicate on this path.
- `RETRY POLICY BACKOFF <duration> MAX <duration>` is required by every mode. Infrastructure retry
  delays begin at `BACKOFF`, double on each attempt, and cap at `MAX`. A server-requested delay,
  such as an HTTP rate-limit interval, can extend an individual delay. Retries continue with
  backpressure until the external system recovers or an operator repairs its provisioning.

Request/response sinks—SQS, Sentry, OTEL, the databases, and Iceberg—do not take `ACK TIMEOUT`;
their client request timeout bounds the response. SQS, Sentry, OTEL, and ClickHouse clients expose
that bound as the optional `timeout_ms` CONFIG key. For every sink, Nervix accounts for records individually
wherever the transport exposes individual results: delivered records acknowledge upstream,
definitively invalid records follow `ON MESSAGE ERROR`, and a retry resends only records that are
neither delivered nor rejected. An ambiguous or infrastructure-wide failure is never used to
discard a record.

| Sink | Mode forms | Publish success boundary |
| --- | --- | --- |
| Kafka | `NO_ACK`; `ACK SEQUENTIAL`; `ACK PARALLEL MAX <n>` | Local producer-queue acceptance for `NO_ACK`; one delivery report per record for `ACK` |
| Pulsar | `NO_ACK`; `ACK SEQUENTIAL`; `ACK PARALLEL MAX <n>` | Producer acceptance for `NO_ACK`; one broker receipt per record for `ACK` |
| RabbitMQ | `NO_ACK`; `ACK SEQUENTIAL`; `ACK PARALLEL MAX <n>` | Channel acceptance for `NO_ACK`; publisher confirm for `ACK` |
| MQTT | `QOS 0`; `QOS 1 ACK ...`; `QOS 2 ACK ...` | Client acceptance, `PUBACK`, or completion of the QoS 2 handshake respectively |
| NATS | `NO_ACK`; `JETSTREAM ACK SEQUENTIAL`; `JETSTREAM ACK PARALLEL MAX <n>` | Core-NATS connection flush for `NO_ACK`; JetStream `PubAck` otherwise |
| Redis Pub/Sub | `NO_ACK` | Server acceptance of `PUBLISH`; the subscriber count is not a delivery guarantee |
| ZeroMQ | `NO_ACK` | Socket acceptance |
| SQS | `SINGLE`; `BATCH` | Successful per-record or per-entry service response |
| Sentry | `ACK` | Successful one-event envelope response |
| OTEL | `ACK` | Successful OTLP Export response; `partial_success` is acknowledged with a warning |
| ClickHouse, Postgres, MySQL, MongoDB | `ACK` | Successful insert/write result |
| Iceberg | `ACK` | Successful catalog commit |

While confirmations or infrastructure retries are pending, the emitter stops consuming from its
relays and keeps upstream ACK leases alive. `FLUSH` still controls when and how much work enters a
flush; `MODE` controls when each record in that flush counts as published. `ATTACHED` and
`DETACHED` are orthogonal: a detached emitter acknowledges upstream immediately but still performs
its declared confirmations and retries for error visibility and backpressure.

## Altering emitters

`ALTER EMITTER` applies one or more comma-separated operations in written order:

```nspl,ignore
ALTER EMITTER <emitter>
    ADD FROM <relay> [WHERE <expr>]
  | DROP FROM <relay>
  | ALTER FROM <relay> SET WHERE <expr>
  | ALTER FROM <relay> DROP WHERE
  | SET TO <full sink clause>
  | SET MODE <transport-specific mode body>
  | SET CLIENT <client>
  | SET ENCODE USING <codec>
  | DROP ENCODE
  | SET COLLECT FOR <duration> [MAX BATCH SIZE <bytes>]
  | DROP COLLECT
  | SET ATTACHED
  | SET DETACHED
  | SET FLUSH EACH <duration> MAX BATCH SIZE <bytes>
  | SET FLUSH IMMEDIATE
  | SET COMMIT EACH <duration> MAX SIZE <bytes>
  [, ...];
```

`SET TO` accepts the same complete transport-specific sink body that follows `TO` in `CREATE
EMITTER`, including its required `MODE`, SQS FIFO group, database maximum batch, and Iceberg commit
policy. The existing construction and output flush policy remain in place. `SET MODE` changes only
the current sink's publishing mode and rejects a body that the sink does not support. `SET CLIENT`
changes only the client of the current sink kind. `SET COMMIT` is valid only for Iceberg. `DROP
ENCODE` fails if the emitter has no codec configured.

`ADD FROM` rejects an already configured relay. `DROP FROM` cannot remove the final input.
`ALTER FROM ... SET WHERE` adds or replaces that source's predicate; `ALTER FROM ... DROP WHERE`
fails when the source has no predicate.

Changing only `FLUSH` is a `DYNAMIC` update. The live emitter keeps its pending Arrow batches,
installs the new cadence, and receives a force-flush kick, so buffered output is neither discarded
nor re-encoded. Source-predicate, sink, publishing-mode, client, codec, collection, and attachment
changes use
`ENTITY_PAUSE`: Nervix gates all of the emitter's source relays, drains collected input and pending
sink output, replaces that emitter task, and releases the gates. Changing source membership uses
`DOMAIN_PAUSE` because it changes graph topology. Other relays continue flowing during an entity
pause; sibling consumers of a gated source may see bounded backpressure until the gate is released.
The complete candidate graph is validated before any change is committed.

## Codec-emitter construction

Codec emitters are transforming routes. They begin with an empty codec-schema payload and use
explicit inheritance and ordered assignment:

```nspl
CREATE IF NOT EXISTS EMITTER kafka_notifications
  FROM notifications
  TO KAFKA kafka_main TOPIC notifications_out
    MODE ACK PARALLEL MAX 1000 ACK TIMEOUT 30s
      RETRY POLICY BACKOFF 250ms MAX 30s
    ENCODE USING notification_codec
  INHERIT ALL EXCEPT raw, secret
  SET secret = leak_sensitive(input.secret),
      normalized = lower(input.raw)
  WHERE output.active
  INVOKE write_header("tenant", input.tenant),
         write_header("route", output.normalized)
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

`message.field` reads the [working message](working-message.md), `input.field` always reads the
source relay row, and `output.field` requires prior initialization. Relay-qualified fields are
invalid. There is no implicit identity transformation and no `UNSET`; use `INHERIT ALL EXCEPT`.

External sensitivity is strict. Every sensitive payload value requires `leak_sensitive(...)` or an
explicit `INHERIT field LEAK SENSITIVE`, even when the codec target field is also sensitive.

## Direct-emitter values

Database, object-store, and OTEL direct emitters construct external name-keyed mappings:

```nspl,ignore
VALUES {
  "tenant" = input.tenant,
  "normalized" = lower(input.action),
  "secret" = leak_sensitive(input.secret)
}
WHERE input.active
```

Entries are independent and do not create variables. Order does not affect evaluation, duplicate
external keys are invalid, `output` is unavailable, and sensitive values require explicit leakage.
Bare fields, `message.field`, and `input.field` read the source row. Direct emitters reject
`INHERIT` and all current direct sinks reject `INVOKE`.

## Header invocations

`write_header` is a side-effect function. It accepts statically non-null `STRING` name and value
expressions and is valid only as a top-level `INVOKE` call. Sensitive values require
`leak_sensitive`. Calls execute left to right after payload finalization and route filtering. Header
mutations are staged in a temporary route-local envelope; invocation failure prevents payload and
partial-envelope publication.

Header output is supported only on codec emitters for Kafka, NATS, Pulsar, RabbitMQ, and SQS.
Kafka and NATS preserve ordered repeated values. Pulsar, RabbitMQ, and SQS use last-write-wins
behavior. Redis, MQTT, Syslog, ZeroMQ, Sentry, OTEL, direct database sinks, and Iceberg reject
header writes.

Emitter expressions use the same typed surface as other runtime nodes:

- arithmetic: `+`, `-`, `*`, `/`, `%`
- comparisons and boolean logic: `=`, `!=`, `>`, `<`, `>=`, `<=`, `AND`, `OR`, `NOT`
- explicit casts: `expr AS TYPE`
- built-ins: string, null-handling, numeric, regex, and contextual functions such as `lower`, `coalesce`, `abs`, `regexp_substr`, `now`, and `uuid_v4`

See [Filter-Map Functions](filter-map-functions.md) for the full function reference.

That expression surface applies to the full Nervix internal schema type set:

- `U8`, `I8`, `U16`, `I16`, `U32`, `I32`, `U64`, `I64`
- `F32`, `F64`
- `BOOL`, `STRING`, `DATETIME`

Nested conditions and chained calls such as `contains(lower(trim(input.raw)), 'warn')` are supported
before encoding.

Client-backed emitters can use resource-mounted client config values for TLS material and other file-based settings. See [Resources](resources.md#client-config-mounts).

## TLS Client Configuration

Emitter TLS is configured on the referenced `CLIENT` exactly the same way as ingestor TLS.

Common pattern:

```nspl,ignore
CREATE [IF NOT EXISTS] CLIENT <name>
  TYPE <kind>
  MOUNT <tls_resource>
  CONFIG {
    ...
    'tls_ca_file' = '{{ tls_resource }}/ca.pem'
  };
```

Transport-specific expectations:

- `KAFKA`: pass-through to librdkafka. Typically set `'security.protocol' = 'ssl'`, `'ssl.ca.location' = '{{ tls_resource }}/ca.pem'`, and optional `'ssl.certificate.location'` plus `'ssl.key.location'`.
- `RABBITMQ`: use `amqps://...` in `addr`; Nervix honors `tls_ca_file`.
- `REDIS`: use `rediss://...` in `addr`; Nervix honors `tls_ca_file`, `tls_cert_file`, `tls_key_file`.
- `MQTT`: use `mqtts://...` in `addr`; Nervix requires `tls_ca_file` and supports `tls_cert_file` plus `tls_key_file`.
- `NATS`: use `tls://...` in `addr`; Nervix honors `tls_ca_file`, `tls_cert_file`, `tls_key_file`.
- `PULSAR`: use `pulsar+ssl://...` in `addr`; Nervix honors `tls_ca_file` and optional `tls_allow_insecure_connection` plus `tls_hostname_verification_enabled`. Pulsar client certificate authentication is not currently exposed.
- `SQS`: use an `https://...` `endpoint`; Nervix honors `tls_ca_file` and optional `timeout_ms`.
- `SENTRY`: the referenced `TYPE SENTRY` client carries an `https://...` `dsn`; Nervix honors the
  client's `tls_ca_file`, `tls_cert_file`, and `tls_key_file`.
- `OTEL`: use an `https://...` `endpoint`; Nervix honors `tls_ca_file`, `tls_cert_file`, and
  `tls_key_file` for both OTLP/gRPC and OTLP/HTTP-protobuf.
- `CLICKHOUSE`: use an `https://...` `addr`; Nervix honors `tls_ca_file` and optional `timeout_ms`.
- `POSTGRES`: include `sslmode=require` in `addr`; Nervix honors `tls_ca_file`.
- `MYSQL`: include `require_ssl=true` in `addr`; Nervix honors `tls_ca_file`.
- `SYSLOG`: select `'protocol' = 'tls'`. Optional `tls_ca_file` adds a server trust root;
  optional `tls_cert_file` and `tls_key_file` configure client authentication and must appear
  together.

Example Kafka TLS emitter client:

```nspl
CREATE IF NOT EXISTS CLIENT kafka_tls
  TYPE KAFKA
  MOUNT dev_tls
  CONFIG {
    'bootstrap.servers' = '127.0.0.1:9094',
    'security.protocol' = 'ssl',
    'ssl.ca.location' = '{{ dev_tls }}/ca.pem'
  };
```

## Supported Emitter Sinks

### Kafka

```nspl,ignore
TO KAFKA <client> TOPIC <topic>
  MODE NO_ACK RETRY POLICY BACKOFF <duration> MAX <duration>
     | ACK (SEQUENTIAL | PARALLEL MAX <n>) ACK TIMEOUT <duration>
         RETRY POLICY BACKOFF <duration> MAX <duration>
```

`ACK` waits for every record's delivery report. Kafka's `acks`, idempotence, batching, linger, and
compression remain pass-through client configuration; Nervix's `ACK TIMEOUT` independently bounds
the report wait. `NO_ACK` is fire-and-forget after local admission: Nervix acknowledges the
emitter's `ATTACHED` ACK share when librdkafka accepts the record into its producer queue and does
not wait for a delivery report. A later broker error, producer timeout, crash, or process exit can
therefore lose an accepted record. A full local queue is an infrastructure condition paced by the
declared retry policy with backpressure. Graceful shutdown drains queued records within the node
drain bound in both modes. A definitive record-specific rejection, such as an oversized message,
follows `ON MESSAGE ERROR`.

### Pulsar

```nspl,ignore
TO PULSAR <client> TOPIC <topic>
  MODE NO_ACK RETRY POLICY BACKOFF <duration> MAX <duration>
     | ACK (SEQUENTIAL | PARALLEL MAX <n>) ACK TIMEOUT <duration>
         RETRY POLICY BACKOFF <duration> MAX <duration>
```

`ACK` waits for each broker receipt. `NO_ACK` acknowledges producer acceptance and does not expose
later broker errors; its throughput advantage may be smaller than Kafka's because Pulsar already
pipelines producer work.

Pulsar emitters use the same client config surface as Pulsar ingestors:

- `'addr'`: broker address such as `'pulsar://127.0.0.1:6650'`
- optional `'namespace'`: defaults short topic names to `persistent://public/default/<topic>`; fully qualified topic names are accepted as-is
- optional `'tls_ca_file'`: PEM-encoded CA bundle for `pulsar+ssl://...` connections
- optional `'tls_allow_insecure_connection'`: `true` or `false`; defaults to `false`
- optional `'tls_hostname_verification_enabled'`: `true` or `false`; defaults to `true`

Pulsar TLS currently supports server trust configuration only. Nervix does not yet expose Pulsar client certificate authentication.

### RabbitMQ

```nspl,ignore
TO RABBITMQ <client> QUEUE <queue>
  MODE NO_ACK RETRY POLICY BACKOFF <duration> MAX <duration>
     | ACK (SEQUENTIAL | PARALLEL MAX <n>) ACK TIMEOUT <duration>
         RETRY POLICY BACKOFF <duration> MAX <duration>
```

`ACK` enables publisher confirms and waits for the confirm of each message. A broker nack is an
infrastructure failure and is retried with backpressure. `NO_ACK` acknowledges channel acceptance.

### Redis Pub/Sub

```nspl,ignore
TO REDIS PUBSUB <client> CHANNEL <channel>
  MODE NO_ACK RETRY POLICY BACKOFF <duration> MAX <duration>
```

Redis Pub/Sub has no subscriber delivery acknowledgment. The awaited `PUBLISH` response confirms
server acceptance only. A record-specific server rejection follows `ON MESSAGE ERROR`; connection
failures retry the undelivered work.

### MQTT

```nspl,ignore
TO MQTT <client> TOPIC <topic>
  MODE QOS 0 RETRY POLICY BACKOFF <duration> MAX <duration>
     | QOS (1 | 2) ACK (SEQUENTIAL | PARALLEL MAX <n>) ACK TIMEOUT <duration>
         RETRY POLICY BACKOFF <duration> MAX <duration>
```

QoS 0 acknowledges client acceptance. QoS 1 waits for `PUBACK`; QoS 2 waits for the complete
exactly-once handshake. QoS 1 and 2 use a persistent session and the emitter client's stable
identity so in-flight messages survive reconnects. Reconnect pacing follows the declared retry
policy. A definitive record rejection, such as an invalid topic or payload-format rejection,
follows `ON MESSAGE ERROR`.

### NATS

```nspl,ignore
TO NATS <client> SUBJECT <subject>
  MODE NO_ACK RETRY POLICY BACKOFF <duration> MAX <duration>
     | JETSTREAM ACK (SEQUENTIAL | PARALLEL MAX <n>) ACK TIMEOUT <duration>
         RETRY POLICY BACKOFF <duration> MAX <duration>
```

`NO_ACK` publishes through Core NATS and acknowledges after the connection flush. `JETSTREAM`
waits for one `PubAck` per record. The stream must already exist and capture the subject; a missing
stream is an infrastructure error that remains under backpressure until an operator provisions it.

### ZeroMQ

```nspl,ignore
TO ZEROMQ <client>
  MODE NO_ACK RETRY POLICY BACKOFF <duration> MAX <duration>
```

ZeroMQ has no delivery acknowledgment; success is socket acceptance. Transient socket failures are
paced by the declared retry policy.

### Syslog

```nspl,ignore
TO SYSLOG <client>
  MODE NO_ACK RETRY POLICY BACKOFF <duration> MAX <duration>
  ENCODE USING <codec>
```

The client sends UDP datagrams, persistent RFC 6587 TCP frames, or RFC 5425 TLS frames. TCP uses
octet-counting by default and may select non-transparent framing; TLS always uses octet counting.
Success is local socket acceptance and flush, not remote delivery confirmation. The sink requires
a codec and rejects header writes. See [Syslog](syslog.md) for client configuration, framing,
message errors, retry behavior, and limits.

### SQS

```nspl,ignore
TO SQS <client> QUEUE <queue> [FIFO GROUP (FROM BRANCH | <string_expression>)]
  MODE (SINGLE | BATCH) RETRY POLICY BACKOFF <duration> MAX <duration>
```

`SINGLE` issues one request per record. `BATCH` groups records within SQS's fixed limit of ten
entries and 256 KiB per request; both modes issue requests sequentially and acknowledge service
responses. Per-entry transient failures retry only those entries, while invalid entries and a
record larger than 256 KiB follow `ON MESSAGE ERROR` individually.

Set the SQS client's optional `timeout_ms` CONFIG key to bound both the complete service operation
and its single SDK attempt. Nervix disables the AWS SDK's internal retries, so a timeout returns to
the emitter and the mode's declared `RETRY POLICY` owns all retry pacing.

For FIFO queues, one batch request contains at most one record from each message group. A partial
batch failure therefore cannot deliver a later record from a group ahead of the failed record;
other groups may still make progress independently.

A queue name ending in `.fifo` requires `FIFO GROUP`, and `FIFO GROUP` is rejected for a queue
without that suffix. `FROM BRANCH` requires branched input and uses the record's branch key as its
message group. Otherwise the expression must have exact `STRING` type for every record and obey
normal external-sensitivity leakage rules. Nervix relies on content-based deduplication, which the
operator must enable while provisioning the FIFO queue. Sends to a FIFO queue without it fail as
publish errors; Nervix never creates or reconfigures the queue.

### Sentry

Sentry emission uses a `TYPE SENTRY` client whose required `dsn` contains the project endpoint and
public key:

```nspl
CREATE CLIENT sentry_main
  TYPE SENTRY
  CONFIG {
    'dsn' = 'https://<public-key>@sentry.example.com/<project-id>',
    'timeout_ms' = 5000
  };

CREATE EMITTER sentry_errors
  FROM errors
  TO SENTRY sentry_main
    MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
    ENCODE USING sentry_event_codec
  INHERIT ALL
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

The codec must produce one top-level JSON object per record. Its fields use the Sentry event
protocol, such as `message`, `level`, `environment`, `release`, `tags`, `extra`, `user`, and
`exception`. Nervix preserves the complete object and supplies `event_id`, `timestamp`, and
`platform` when they are omitted. It then creates a Sentry envelope, derives the envelope URL and
authentication header from the DSN, and submits the event. An invalid event object is handled as a
route-local encoding error.

Sentry sends one event per envelope and acknowledges the successful HTTP response. On `429` or
`503`, Nervix honors `Retry-After` and `X-Sentry-Rate-Limits`; the server interval extends the
declared retry delay when it is longer.

Use a JSON wire codec or a JAQ-native codec with JSON output. Sentry emitters require `ENCODE
USING`, do not accept `write_header`, and still require explicit leakage for sensitive event
fields. The optional Sentry client keys `timeout_ms`, `tls_ca_file`, `tls_cert_file`, and
`tls_key_file` have their usual meanings.

### OTEL

OTEL emission is a codec-free direct sink for OTLP logs, traces, and metric data points. It uses a
`TYPE OTEL` client and supports both OTLP/gRPC and OTLP/HTTP-protobuf. The protocol is always
explicit; there is no hidden default:

```nspl
CREATE CLIENT otel_main
  TYPE OTEL
  CONFIG {
    'endpoint' = 'http://127.0.0.1:4317',
    'protocol' = 'grpc',
    'headers' = 'authorization=Bearer <token>',
    'compression' = 'gzip',
    'timeout_ms' = 5000
  };
```

`endpoint` and `protocol` are required. `protocol` is exactly `grpc` or `http/protobuf`.
`headers` is the OTLP comma-separated `key=value` form, `compression` accepts only `gzip`, and an
absent compression key sends an uncompressed request. `timeout_ms` is an optional positive request
bound. For `http/protobuf`, Nervix appends `/v1/logs`, `/v1/traces`, or `/v1/metrics` to the endpoint
path. Mount TLS files and use `tls_ca_file`, `tls_cert_file`, and `tls_key_file` in the same client;
the certificate and key must be supplied together.

One log record is mapped as follows:

```nspl
CREATE EMITTER audit_to_otel
  FROM audit_events
  TO OTEL otel_main LOGS
  VALUES {
    'time' = input.event_ts,
    'severity_text' = input.level,
    'severity_number' = input.level_num,
    'body' = input.message,
    'trace_id' = input.trace_id,
    'span_id' = input.span_id
  }
  ATTRIBUTES {
    'user.id' = input.user_id,
    'audit.action' = leak_sensitive(input.action)
  }
  RESOURCE {
    'service.name' = 'checkout-pipeline',
    'deployment.environment.name' = 'prod'
  }
  SCOPE 'nervix/audit' VERSION '1.0'
  MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
  FLUSH EACH 2s MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

The clause order is fixed: signal, required `VALUES`, optional `ATTRIBUTES`, optional `RESOURCE`,
optional `SCOPE '<name>' [VERSION '<version>']`, then `MODE`. `RESOURCE` values must be literals or
literal arrays. `ATTRIBUTES` may use exact-typed `STRING`, `BOOL`, integer-family, `F32`, `F64`,
`DATETIME`, or array values; datetimes become RFC 3339 strings. Null attribute values are omitted.
Normal sensitivity rules apply to every expression, including `ATTRIBUTES`.

The closed log `VALUES` set is:

- `time`: required `DATETIME`
- `body`: required `STRING`
- `severity_text`: optional `STRING`
- `severity_number`: optional `I32` in `0..=24`
- `trace_id`: optional nonzero 32-hex-character `STRING`
- `span_id`: optional nonzero 16-hex-character `STRING`

Trace emitters use `TO OTEL <client> TRACES`. Their closed `VALUES` set requires `trace_id`
(nonzero 32-hex `STRING`), `span_id` (nonzero 16-hex `STRING`), `name` (`STRING`), `start_time`
(`DATETIME`), and `end_time` (`DATETIME`). Optional keys are `parent_span_id` (nonzero 16-hex
`STRING`), `kind` (`SERVER`, `CLIENT`, `INTERNAL`, `PRODUCER`, or `CONSUMER`), `status_code` (`OK`,
`ERROR`, or `UNSET`), and `status_message` (`STRING`). Enum strings are case-sensitive.

Each metric emitter defines exactly one metric stream:

```nspl,ignore
TO OTEL otel_main
METRIC 'http.server.request.count' UNIT '1'
DESCRIPTION 'Completed HTTP requests'
SUM MONOTONIC DELTA
VALUES {
  'time' = input.window_end,
  'start_time' = input.window_start,
  'value' = input.request_count
}
ATTRIBUTES { 'http.route' = input.route }
RESOURCE { 'service.name' = 'checkout-pipeline' }
MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
```

Metric shapes are `GAUGE`, `SUM [MONOTONIC] (DELTA | CUMULATIVE)`, and `HISTOGRAM (DELTA |
CUMULATIVE)`. `DESCRIPTION` is optional and follows the required `UNIT`. Gauge and sum points
require `time` (`DATETIME`) and `value` (an exact integer-family, `F32`, or `F64` value). Integer
values use OTLP `as_int`; floating-point values use `as_double`. `start_time` is required for a
delta sum and optional otherwise.

Histogram points require `time`, integer-family `count`, integer-array `bucket_counts`, and `F32`
or `F64` array `explicit_bounds`. Optional keys are `start_time`, numeric `sum`, `min`, and `max`;
`start_time` is required for delta histograms. At runtime, counts must be non-negative and
`len(bucket_counts)` must equal `len(explicit_bounds) + 1`. Exponential histograms and summaries
are not supported.

For each pending Arrow batch, Nervix builds one Export request containing one resource, one scope,
and all successfully converted records. `FLUSH ... MAX BATCH SIZE` measures the Arrow batch before
protobuf encoding, so the encoded request can be larger than the configured boundary and must fit
the receiver's request-size limit. Nervix stamps log `observed_time_unix_nano` at emission.

Connection failures, HTTP `429` and `5xx`, and gRPC `UNAVAILABLE` or `RESOURCE_EXHAUSTED` retry with
backpressure. `Retry-After` and gRPC `RetryInfo` can extend the declared retry delay. Bad IDs, enum
strings, severity values, numeric ranges, or histogram shapes reject only the affected record
through `ON MESSAGE ERROR`. HTTP `400` and gRPC `INVALID_ARGUMENT` reject every record in that
request without retry. OTLP `partial_success` cannot be retried safely: Nervix acknowledges every
record and logs a warning, so records rejected by the receiver in that response are lost.

Nervix does not provision collectors, indexes, tenants, or vendor-side telemetry objects. The OTLP
endpoint must already exist; an unreachable endpoint remains an initialization or publish error.

### ClickHouse

```nspl
CREATE EMITTER to_ch
  FROM notifications
  TO CLICKHOUSE clickhouse_client INSERT TO TABLE my_table
  VALUES {
    "clickhouse_user_id" = input.user_id,
    "clickhouse_now" = NOW(),
    "clickhouse_action" = LOWER(input.action)
  }
  WITH MAX BATCH 500
  MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
  FLUSH EACH 10s MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

ClickHouse clients use the HTTP endpoint:

```nspl
CREATE CLIENT ch
  TYPE CLICKHOUSE
  CONFIG {
    'addr' = 'http://127.0.0.1:8123',
    'user' = 'default',
    'password' = 'nervix',
    'timeout_ms' = 5000
  };
```

Optional config keys are `'user'`, `'password'`, `'database'`, and `'timeout_ms'`. The timeout
bounds both sending an insert body and waiting for ClickHouse to finish the insert and return its
result.
For HTTPS endpoints, mount a TLS resource and set `'tls_ca_file'` to the mounted CA path.

ClickHouse requires `WITH MAX BATCH <n>`. A larger flush is split into sequential inserts of at
most `n` records, and each successful insert is an acknowledgment. For ClickHouse, Postgres, and
MySQL, a failed multi-row insert is classified first as record-specific or infrastructure-wide.
Infrastructure failures retry with backpressure. A record-specific failure is isolated by
re-executing the chunk one record at a time so healthy rows land and only poison rows follow `ON
MESSAGE ERROR`. Isolation can reapply rows from the failed chunk; use the sink's idempotent write
facilities where available. Chunking also means one flush is not an atomic database transaction.

### Postgres

```nspl
CREATE EMITTER to_pg
  FROM notifications
  TO POSTGRES postgres_client INSERT TO TABLE my_table
  VALUES {
    "postgres_user_id" = input.user_id,
    "postgres_now" = NOW() AS STRING,
    "postgres_action" = LOWER(input.action)
  }
  WITH MAX BATCH 500
  MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
  FLUSH EACH 10s MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Postgres emitters use `VALUES` expressions and insert batches with `INSERT ... SELECT ... FROM
unnest(...)`. `WITH MAX BATCH <n>` is required and is enforced as the maximum records in each
sequential insert. The insert result acknowledges those records. On the poison-isolation path,
tables without an idempotent `ON CONFLICT` policy may observe duplicates when healthy records are
re-executed.

Postgres emitters may include an insert conflict policy before `WITH MAX BATCH`:

```nspl,ignore
ON CONFLICT ("postgres_user_id") DO UPDATE
ON CONFLICT ("postgres_user_id") DO NOTHING
ON CONFLICT DO NOTHING
```

`DO UPDATE` updates every mapped `VALUES` column except the conflict target columns, and requires a conflict target. `DO NOTHING` may be used with or without a target.

Postgres clients use a tokio-postgres connection string:

```nspl
CREATE CLIENT pg
  TYPE POSTGRES
  CONFIG {
    'addr' = 'host=127.0.0.1 port=5432 user=postgres password=nervix dbname=postgres'
  };
```

For TLS connections, include `sslmode=require`, mount a TLS resource, and set `'tls_ca_file'` to the mounted CA path.

### MySQL

```nspl
CREATE EMITTER to_mysql
  FROM notifications
  TO MYSQL mysql_client INSERT TO TABLE my_table
  VALUES {
    "mysql_user_id" = input.user_id,
    "mysql_now" = NOW() AS STRING,
    "mysql_action" = LOWER(input.action)
  }
  WITH MAX BATCH 500
  MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
  FLUSH EACH 10s MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

MySQL emitters use `VALUES` expressions and insert batches with a multi-row `INSERT ... VALUES (?,
...), ...` command. `WITH MAX BATCH <n>` is required and is enforced as the maximum records in each
sequential insert. The insert result acknowledges those records. Conflict clauses are the user's
tool for bounding duplicates when poison isolation re-executes a failed chunk.

MySQL emitters may include an insert conflict policy before `WITH MAX BATCH`:

```nspl,ignore
ON CONFLICT DO UPDATE
ON CONFLICT DO NOTHING
```

MySQL and MariaDB resolve conflicts through primary and unique keys already defined on the table, so the NSPL conflict policy does not accept a target list. `DO UPDATE` uses `ON DUPLICATE KEY UPDATE` for all mapped `VALUES` columns. `DO NOTHING` uses a no-op duplicate-key update.

MySQL clients use a mysql_async connection URL:

```nspl
CREATE CLIENT mysql
  TYPE MYSQL
  CONFIG {
    'addr' = 'mysql://nervix:nervix@127.0.0.1:3306/nervix'
  };
```

For TLS connections, include `require_ssl=true`, mount a TLS resource, and set `'tls_ca_file'` to the mounted CA path.

### MongoDB

```nspl
CREATE EMITTER to_mongodb
  FROM notifications
  TO MONGODB mongodb_client INSERT TO COLLECTION my_collection
  VALUES {
    "mongodb_user_id" = input.user_id,
    "mongodb_now" = NOW() AS STRING,
    "mongodb_action" = LOWER(input.action)
  }
  WITH MAX BATCH 500
  MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
  FLUSH EACH 10s MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

MongoDB emitters use `VALUES` expressions and bulk writes. `WITH MAX BATCH <n>` is required and is
enforced as the maximum documents in each write. MongoDB reports per-document outcomes, so healthy
documents acknowledge and poison documents follow `ON MESSAGE ERROR` without a separate isolation
pass. Transient or infrastructure failures retry only the undelivered documents.

MongoDB emitters may include an insert conflict policy before `WITH MAX BATCH`:

```nspl,ignore
ON CONFLICT ("mongodb_user_id") DO UPDATE
ON CONFLICT ("mongodb_user_id") DO NOTHING
```

MongoDB conflict policies require a target list because the emitter must build an explicit upsert filter. Target fields must be mapped in `VALUES`. `DO UPDATE` updates every mapped field except the conflict target fields and inserts the full mapped document when no existing document matches. `DO NOTHING` inserts only when no document matches the target.

Emitters using either MongoDB `ON CONFLICT` form require MongoDB 8.0 or newer because those modes
execute as one bulk write per chunk.

MongoDB clients use a MongoDB connection URL and database name:

```nspl
CREATE CLIENT mongodb
  TYPE MONGODB
  CONFIG {
    'addr' = 'mongodb://root:nervix@127.0.0.1:27017/nervix?authSource=admin',
    'database' = 'nervix'
  };
```

For TLS connections, include `tls=true`, mount a TLS resource, and set `'tls_ca_file'` to the mounted CA path.

### Iceberg

```nspl
CREATE CLIENT s3_main
  TYPE S3
  CONFIG {
    'endpoint' = 'http://127.0.0.1:9900',
    'region' = 'us-east-1',
    'access_key_id' = 'rustfsadmin',
    'secret_access_key' = 'rustfsadmin',
    'path_style_access' = true
  };

CREATE CLIENT iceberg_catalog
  TYPE ICEBERG_REST
  CONFIG {
    'uri' = 'http://127.0.0.1:8181',
    'warehouse' = 's3://nervix-iceberg/warehouse'
  };

CREATE EMITTER iceberg_notifications
  FROM notifications
  TO ICEBERG ON S3 s3_main TABLE notifications
  VALUES {
    'user_id' = input.user_id,
    'action' = input.action
  }
  LOCATION 's3://nervix-iceberg/tables/notifications'
  CATALOG iceberg_catalog COMMIT EACH 1m MAX SIZE 512MiB
  MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
  FLUSH EACH 10s MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Iceberg emitters use explicit `VALUES` expressions and do not declare `ENCODE USING`. The `ON S3`, `ON GCS`, or `ON AZURE_BLOB` backend clause selects the object-store implementation. The referenced blob client supplies the object-store connection for table files. The `CATALOG <client>` clause references a separate `TYPE ICEBERG_REST` client that supplies the REST catalog URI and warehouse. The referenced REST catalog namespace and table must already exist; Nervix loads that table and appends data, but does not create catalog namespaces or tables implicitly. The emitter owns the Iceberg table name, mapped output columns, table location, catalog client reference, and flush policy.

GCS uses the same emitter shape with a `TYPE GCS` client and `gs://` locations:

```nspl
CREATE CLIENT gcs_main
  TYPE GCS
  CONFIG {
    'service_path' = 'https://storage.googleapis.com',
    'token' = '<oauth2-token>'
  };

CREATE CLIENT iceberg_catalog
  TYPE ICEBERG_REST
  CONFIG {
    'uri' = 'https://iceberg-rest.example.com',
    'warehouse' = 'gs://nervix-iceberg/warehouse'
  };

CREATE EMITTER iceberg_notifications
  FROM notifications
  TO ICEBERG ON GCS gcs_main TABLE notifications
  VALUES {
    'user_id' = input.user_id,
    'action' = input.action
  }
  LOCATION 'gs://nervix-iceberg/tables/notifications'
  CATALOG iceberg_catalog COMMIT EACH 1m MAX SIZE 512MiB
  MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
  FLUSH EACH 10s MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Azure Blob uses `TYPE AZURE_BLOB` and `wasbs://` locations. `wasb://` is also accepted for plain-HTTP local endpoints:

```nspl
CREATE CLIENT azure_main
  TYPE AZURE_BLOB
  CONFIG {
    'account_name' = 'myaccount',
    'account_key' = '<account-key>'
  };

CREATE CLIENT iceberg_catalog
  TYPE ICEBERG_REST
  CONFIG {
    'uri' = 'https://iceberg-rest.example.com',
    'warehouse' = 'wasbs://nervix-iceberg@myaccount.blob.core.windows.net/warehouse'
  };

CREATE EMITTER iceberg_notifications
  FROM notifications
  TO ICEBERG ON AZURE_BLOB azure_main TABLE notifications
  VALUES {
    'user_id' = input.user_id,
    'action' = input.action
  }
  LOCATION 'wasbs://nervix-iceberg@myaccount.blob.core.windows.net/tables/notifications'
  CATALOG iceberg_catalog COMMIT EACH 1m MAX SIZE 512MiB
  MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
  FLUSH EACH 10s MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

The REST catalog is the authority for namespace and table metadata. Nervix does not write a separate object-store catalog pointer file and does not provision catalog entries from the emitter runtime path.

Iceberg uses two explicit boundaries. `FLUSH` collects typed in-memory batches and writes them to
local Arrow IPC files under the runtime temporary-file root. `COMMIT EACH <duration> MAX SIZE
<bytes>` reads the staged Arrow IPC batches, concatenates them into one Arrow batch, appends that
batch to the Iceberg table, and commits the catalog update. The temporary-file root defaults to
`/tmp` and can be changed with `--temp-dir` or `NERVIX_TEMP_DIR`.

The sink completion point for `MODE ACK` is the successful catalog commit. Local staging is not an
ACK boundary. Commit conflicts, incompatible table evolution, a dropped table, and unavailable
catalog or object storage are table-level infrastructure failures: Nervix reports them in runtime
and drain status and retries all affected staged records with the declared policy and
backpressure. Record-level expression and construction errors are attributed through `ON MESSAGE
ERROR` before staging. An ambiguous failure after the catalog commit can append the rows again;
Iceberg appends are not idempotent. See [ACK Semantics And Effective
Delivery](#ack-semantics-and-effective-delivery) for attachment and fan-out behavior.

## Codec Behavior On Emission

`ENCODE USING <codec>` follows the sink it encodes for, because whether a codec applies is a
property of the sink. Kafka, Pulsar, RabbitMQ, Redis, MQTT, NATS, ZeroMQ, SQS and Sentry publish an
encoded payload and require it. ClickHouse, Postgres, MySQL, and MongoDB map columns with `VALUES`
and do not take a codec. OTEL maps signal fields and attributes with `VALUES` and takes no codec.
Iceberg writes typed records and takes none.

JAQ-native codecs can reshape outbound payloads with `ON EMITTING` before writing the selected
format:

```nspl
CREATE IF NOT EXISTS CODEC notification_codec
  FROM JSON
  TO SCHEMA notification
  WITH JAQ TRANSFORMATIONS ON EMITTING '{payload: .}';
```

That lets the emitter publish a different JSON envelope for each outbound row without changing the declared relay schema.

## ACK Semantics And Effective Delivery

Nervix composes per-hop ACKs. The effective delivery semantics of a source-to-sink path are the
observable duplicate and loss behavior produced by the source delivery mode, the emitter's
publishing `MODE`, its attachment, and the external service. They are not the ACK mechanics of any
one hop.

Publishing `MODE` selects the sink completion point at which the emitter considers a record
delivered. Attachment determines whether that outcome participates in the upstream ACK chain:

- `ATTACHED`: emitter success or failure at the selected sink completion point stays part of the
  upstream ACK chain.
- `DETACHED`: relay fan-out acknowledges upstream immediately. The emitter still waits for its
  declared confirmations, applies its retry policy, error-routes record failures, and exerts local
  backpressure, but that outcome cannot delay, retry, or fail the source ACK.

Confirming broker modes and request/response `ACK` modes are at least once. A confirmation timeout
or lost response is not proof that the service rejected a record, so retry can duplicate it. The
parallel window limits how many records are exposed to that ambiguity at one time, and Nervix
resends only records not yet confirmed or definitively rejected. `NO_ACK`, MQTT QoS 0, Core NATS,
Redis Pub/Sub, and ZeroMQ expose earlier acceptance boundaries and can lose acknowledged records
after a crash or downstream failure. External broker durability and idempotence settings remain
the user's client and service configuration.

When one source record reaches multiple emitters or multiple attached routes, the upstream ACK
completes only after every attached emitter reaches its sink completion point. A failure on any
attached path reopens source retry for the record on all paths. A sink that already published
successfully may therefore receive the record again because a sibling sink failed. This applies to
every sink without idempotent writes. Iceberg is the canonical case: rows can be appended to the
table again after a sibling emitter fails.

This sibling-retry case assumes a source mode that retries when an attached ACK fails or is lost. A
no-ACK source cannot create that retry duplicate, but it can lose the record instead.

Every `DETACHED` path has a common loss window: a process can fail after relay fan-out acknowledges
upstream but before the emitter reaches its declared sink completion point. The table below calls
out the additional mode- and transport-specific duplicate and loss conditions.

| Sink | Duplicate conditions (`ATTACHED`) | Additional loss conditions | Idempotency available in Nervix |
| --- | --- | --- | --- |
| Kafka | `ACK` retry after an ambiguous delivery report or timeout; either mode after a lost upstream ACK or attached sibling failure | `NO_ACK` can lose a record after local producer-queue admission; broker durability follows Kafka client and topic configuration | None; Kafka producer idempotence is pass-through client configuration |
| Pulsar | `ACK` retry after an ambiguous broker receipt; either mode after a lost upstream ACK or attached sibling failure | `NO_ACK` does not expose broker failures after producer acceptance; retention and durability remain broker policy | None |
| NATS | JetStream retry after an ambiguous `PubAck`; either mode after a lost upstream ACK or attached sibling failure | Core NATS `NO_ACK` connection flush is not durable stream acknowledgement | None |
| RabbitMQ | Confirming `ACK` retry after a nack, timeout, or lost confirm; either mode after a lost upstream ACK or attached sibling failure | `NO_ACK` can lose a record after channel acceptance; queue durability and message persistence remain broker policy | None |
| SQS | Retry after an ambiguous `SendMessage` result, lost ACK, or attached sibling failure | Any failure after detached relay acceptance; SQS retains its own at-least-once behavior | None |
| MQTT | QoS 1 or 2 retry after an ambiguous handshake; any mode after a lost upstream ACK or attached sibling failure | QoS 0 can lose a record after client acceptance; later delivery follows the configured broker and session guarantees | None |
| Redis Pub/Sub | Retry after Redis accepts `PUBLISH` but the Nervix ACK is lost, or after attached sibling failure | Any failure after detached relay acceptance; subscribers that are absent or disconnected miss the message | None |
| ZeroMQ | Retry after socket send acceptance followed by lost ACK or attached sibling failure | Any failure after detached relay acceptance; socket send does not establish durable receiver storage | None |
| Sentry | Retry after an ambiguous HTTP result, lost ACK, or attached sibling failure | Any failure after detached relay acceptance; an accepted event can still be subject to Sentry service policy | None |
| OTEL | Retry after an ambiguous Export result, lost ACK, or attached sibling failure | Any failure after detached relay acceptance; `partial_success` acknowledges the whole request, so receiver-rejected records in that response are lost | None |
| ClickHouse | Retry after an ambiguous insert result, lost ACK, or attached sibling failure | Any failure after detached relay acceptance; a crash after insert but before acknowledgement can also leave an inserted batch that later retries | None |
| Postgres | Retry after an ambiguous transaction result, lost ACK, or attached sibling failure | Any failure after detached relay acceptance; a committed insert can survive a crash before Nervix observes success | `ON CONFLICT` |
| MySQL | Retry after an ambiguous transaction result, lost ACK, or attached sibling failure | Any failure after detached relay acceptance; a committed insert can survive a crash before Nervix observes success | `ON CONFLICT` |
| MongoDB | Retry after an ambiguous write result, lost ACK, or attached sibling failure | Any failure after detached relay acceptance; a committed write can survive a crash before Nervix observes success | `ON CONFLICT` |
| Iceberg | Retry after a commit with a lost ACK or attached sibling failure; appends repeat rows | Crash before catalog commit loses staged work unless the attached source redelivers; detached mode accepts that loss | None; appends are not idempotent |

The [publishing-mode table](#publishing-modes) names each transport's exact completion point.
`ATTACHED` waits only for that declared point and cannot make an earlier `NO_ACK` boundary durable.
MQTT QoS 0, Core NATS, RabbitMQ `NO_ACK`, Redis Pub/Sub, and ZeroMQ can still lose a message after
Nervix observes client-side acceptance. `DETACHED` cannot turn a confirming mode into
fire-and-forget inside the emitter; it changes only whether the result participates upstream.

Emit a stable idempotency key at ingestion, for example with `uuid_v7()`, and carry it through the
graph. Downstream consumers and queries can use that key to suppress retries within that admitted
record's fan-out. A source-provided identifier is stronger because it also survives a fresh source
redelivery. Generate the key once; regenerating it on a downstream route defeats the purpose.

For Postgres, MySQL, and MongoDB, use `ON CONFLICT` against a stable key. This preserves
at-least-once delivery attempts while making the resulting table or collection state
effectively-once for that conflict contract.

Iceberg appends are not idempotent. Deduplicate by the stable key at query time or in downstream
compaction or `MERGE` work. Nervix does not isolate sibling-sink retries by changing ACK mechanics.
That is an [accepted tradeoff](#accepted-tradeoff-shared-retry).

### Accepted Tradeoff: Shared Retry

One input ACK represents all attached descendants. This keeps ACK composition small and preserves
backpressure across the graph. It also means one attached failure retries successful siblings.
Nervix accepts that coupling instead of maintaining a transactional per-sink commit ledger.

### Graph Design

When the same records feed a non-idempotent sink and other emitters, consider `DETACHED` mode for a
non-critical path or separate relays with separate ACK boundaries per sink so one sink's failure
does not drive duplicates into another. `DETACHED` makes that path at-most-once relative to the
upstream ACK: the emitter still confirms and retries according to `MODE`, but a crash after the
detached ACK can lose its in-memory work without causing source redelivery.

See [Data Plane](data-plane.md#ack-composition) for the relay fan-out mechanics and
[What It Is Not](what-it-is-not.md) for the persistence boundary.
