# Emitters

Emitters publish relay records to external systems.

A typical emitter:

```nspl
CREATE [IF NOT EXISTS] EMITTER kafka_notifications
  FROM notifications
  ENCODE USING notification_codec
  TO KAFKA kafka_main TOPIC notifications_out
  INHERIT ALL
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

An emitter defines:

- the source relay
- the codec used for encoding
- the transport-specific sink
- the flush policy used to collect a batch before publishing
- whether the branch is `ATTACHED` or `DETACHED`
- route-local codec construction or a direct `VALUES` mapping
- optional ordered header invocations on supported codec sinks
- optional ordered materialized-state dependencies

## Branch Semantics

An emitter is the terminal consumer for its source relay.

That means:

- the emitter consumes from all concrete branches of its source relay
- the current branch remains available internally for compatible materialized-state lookup
- `branch.field` is unavailable to successful emitter expressions
- branch identity collapses only after successful external publication

All emitters declare `FLUSH EACH <duration> MAX BATCH SIZE <bytes>` or `FLUSH IMMEDIATE`. `FLUSH`
means Nervix collects an in-memory Arrow batch before handing it to the external sink. The
[NSPL Overview](nspl-overview.md) defines the `FLUSH IMMEDIATE` 100 µs minimum batching window.
For most emitters the collected batch is encoded and published on the flush boundary. Iceberg
additionally supports `COMMIT EACH <duration> MAX SIZE <bytes>`: flush writes local Arrow IPC
staging files, and commit appends the staged data to object storage. `ON MESSAGE ERROR SEND TO`
buffers failed-message error records separately and delivers them using the emitter's same `FLUSH`
interval or maximum batch-size boundary.

## Codec-emitter construction

Codec emitters are transforming routes. They begin with an empty codec-schema payload and use
explicit inheritance and ordered assignment:

```nspl
CREATE [IF NOT EXISTS] EMITTER kafka_notifications
  FROM notifications
  ENCODE USING notification_codec
  TO KAFKA kafka_main TOPIC notifications_out
  INHERIT ALL EXCEPT raw, secret
  INHERIT secret LEAK SENSITIVE
  SET normalized = lower(input.raw)
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

Database and object-store direct emitters construct external name-keyed mappings:

```nspl
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
behavior. Kinesis, Redis, MQTT, ZeroMQ, Sentry, direct database sinks, and Iceberg reject header
writes.

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

```nspl
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
- `KINESIS`: use an `https://...` optional `endpoint` for AWS-compatible targets. Nervix honors `tls_ca_file`; local/test targets can also set `region`, `access_key_id`, and `secret_access_key`.
- `SQS`: use an `https://...` `endpoint`; Nervix honors `tls_ca_file`.
- `SENTRY`: the referenced `TYPE SENTRY` client carries an `https://...` `dsn`; Nervix honors the
  client's `tls_ca_file`, `tls_cert_file`, and `tls_key_file`.
- `CLICKHOUSE`: use an `https://...` `addr`; Nervix honors `tls_ca_file`.
- `POSTGRES`: include `sslmode=require` in `addr`; Nervix honors `tls_ca_file`.
- `MYSQL`: include `require_ssl=true` in `addr`; Nervix honors `tls_ca_file`.

Example Kafka TLS emitter client:

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

## Supported Emitter Sinks

### Kafka

```nspl
TO KAFKA <client> TOPIC <topic>
ON MESSAGE ERROR LOG
ON GENERAL ERROR LOG
FLUSH EACH <duration> MAX BATCH SIZE <bytes> | FLUSH IMMEDIATE
```

### Pulsar

```nspl
TO PULSAR <client> TOPIC <topic>
```

Pulsar emitters use the same client config surface as Pulsar ingestors:

- `'addr'`: broker address such as `'pulsar://127.0.0.1:6650'`
- optional `'namespace'`: defaults short topic names to `persistent://public/default/<topic>`; fully qualified topic names are accepted as-is
- optional `'tls_ca_file'`: PEM-encoded CA bundle for `pulsar+ssl://...` connections
- optional `'tls_allow_insecure_connection'`: `true` or `false`; defaults to `false`
- optional `'tls_hostname_verification_enabled'`: `true` or `false`; defaults to `true`

Pulsar TLS currently supports server trust configuration only. Nervix does not yet expose Pulsar client certificate authentication.

### Kinesis

```nspl
TO KINESIS <client> RELAY <relay>
```

### RabbitMQ

```nspl
TO RABBITMQ <client> QUEUE <queue>
```

### Redis Pub/Sub

```nspl
TO REDIS PUBSUB <client> CHANNEL <channel>
```

### MQTT

```nspl
TO MQTT <client> TOPIC <topic>
```

### NATS

```nspl
TO NATS <client> SUBJECT <subject>
```

### ZeroMQ

```nspl
TO ZEROMQ <client>
```

### SQS

```nspl
TO SQS <client> QUEUE <queue>
```

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
  ENCODE USING sentry_event_codec
  TO SENTRY sentry_main
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

Use a JSON wire codec or a JAQ-native codec with JSON output. Sentry emitters require `ENCODE
USING`, do not accept `write_header`, and still require explicit leakage for sensitive event
fields. The optional Sentry client keys `timeout_ms`, `tls_ca_file`, `tls_cert_file`, and
`tls_key_file` have their usual meanings.

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
    'password' = 'nervix'
  };
```

Optional config keys are `'user'`, `'password'`, and `'database'`.
For HTTPS endpoints, mount a TLS resource and set `'tls_ca_file'` to the mounted CA path.

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
  FLUSH EACH 10s MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Postgres emitters use `VALUES` expressions and insert batches with `INSERT ... SELECT ... FROM unnest(...)`. `WITH MAX BATCH <n>` is required and limits the number of buffered records in one insert command.

Postgres emitters may include an insert conflict policy before `WITH MAX BATCH`:

```nspl
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
  FLUSH EACH 10s MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

MySQL emitters use `VALUES` expressions and insert batches with a multi-row `INSERT ... VALUES (?, ...), ...` command. `WITH MAX BATCH <n>` is required and limits the number of buffered records in one insert command.

MySQL emitters may include an insert conflict policy before `WITH MAX BATCH`:

```nspl
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
  FLUSH EACH 10s MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

MongoDB emitters use `VALUES` expressions and insert batches with `insert_many`. `WITH MAX BATCH <n>` is required and limits the number of buffered documents in one insert command.

MongoDB emitters may include an insert conflict policy before `WITH MAX BATCH`:

```nspl
ON CONFLICT ("mongodb_user_id") DO UPDATE
ON CONFLICT ("mongodb_user_id") DO NOTHING
```

MongoDB conflict policies require a target list because the emitter must build an explicit upsert filter. Target fields must be mapped in `VALUES`. `DO UPDATE` updates every mapped field except the conflict target fields and inserts the full mapped document when no existing document matches. `DO NOTHING` inserts only when no document matches the target.

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
  CATALOG iceberg_catalog
  FLUSH EACH 10s MAX BATCH SIZE 1MiB
  COMMIT EACH 1m MAX SIZE 512MiB
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
  CATALOG iceberg_catalog
  FLUSH EACH 10s MAX BATCH SIZE 1MiB
  COMMIT EACH 1m MAX SIZE 512MiB
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
  CATALOG iceberg_catalog
  FLUSH EACH 10s MAX BATCH SIZE 1MiB
  COMMIT EACH 1m MAX SIZE 512MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

The REST catalog is the authority for namespace and table metadata. Nervix does not write a separate object-store catalog pointer file and does not provision catalog entries from the emitter runtime path.

Iceberg uses two explicit boundaries. `FLUSH` collects typed in-memory batches and writes them to
local Arrow IPC files under the runtime temporary-file root. `COMMIT EACH <duration> MAX SIZE
<bytes>` reads the staged Arrow IPC batches, concatenates them into one Arrow batch, appends that
batch to the Iceberg table, and commits the catalog update. The temporary-file root defaults to
`/tmp` and can be changed with `--temp-dir` or `NERVIX_TEMP_DIR`.

The sink completion point is the successful catalog commit in both emitter modes. Local staging is
not an ACK boundary. In `ATTACHED` mode, the upstream ACK remains open until that commit succeeds.
In `DETACHED` mode, the relay removes the emitter from the upstream ACK chain before publication,
so the source path does not wait for staging or commit. A crash before commit loses the in-memory
and locally staged work; an ACK-tracked source can redeliver it only in `ATTACHED` mode. A crash or
ambiguous failure after the table commit but before the attached ACK reaches the source can append
the rows again. See [ACK Semantics And Effective Delivery](#ack-semantics-and-effective-delivery)
for the general retry and fan-out rules.

## Codec Behavior On Emission

Most emitters encode through a codec. ClickHouse, Postgres, MySQL, MongoDB, and Iceberg emitters use `VALUES` expressions instead of `ENCODE USING` and insert or append the mapped row directly.

JAQ-native codecs can reshape outbound payloads with `ON EMITTING` before writing the selected
format:

```nspl
CREATE [IF NOT EXISTS] CODEC notification_codec
  FROM JSON
  TO SCHEMA notification
  WITH JAQ TRANSFORMATIONS ON EMITTING '{payload: .}';
```

That lets the emitter publish a different JSON envelope for each outbound row without changing the declared relay schema.

## ACK Semantics And Effective Delivery

Nervix composes per-hop ACKs. The effective delivery semantics of a source-to-sink path are the
observable duplicate and loss behavior produced by the source delivery mode, emitter mode, and
sink behavior. They are not the ACK mechanics of any one hop.

Emitter modes set one boundary:

- `ATTACHED`: downstream emitter success or failure stays part of the upstream ACK chain.
- `DETACHED`: relay fan-out removes this emitter from the upstream ACK chain. The emitter still
  attempts delivery, but its result cannot delay, retry, or fail the source ACK.

When one source record reaches multiple emitters or multiple attached routes, the upstream ACK
completes only after every attached downstream delivery completes. A failure on any attached path
reopens source retry for the record on all paths. A sink that already published successfully may
therefore receive the record again because a sibling sink failed. This applies to every sink
without idempotent writes. Iceberg is the canonical case: rows can be appended to the table again
after a sibling emitter fails.

The table assumes a source mode that retries when an attached ACK fails or is lost. A no-ACK source
cannot create that retry duplicate, but it can lose the record instead.

| Sink | Duplicate conditions (`ATTACHED`) | Loss conditions (`DETACHED` and crash windows) | Idempotency available in Nervix |
| --- | --- | --- | --- |
| Kafka | Retry after an ambiguous producer result, lost ACK, or attached sibling failure | Any failure after detached relay acceptance; broker durability still follows the configured Kafka producer and topic | None |
| Pulsar | Retry after an ambiguous broker receipt, lost ACK, or attached sibling failure | Any failure after detached relay acceptance; retention and durability remain broker policy | None |
| NATS | Retry after publish acceptance followed by lost ACK or attached sibling failure | Any failure after detached relay acceptance; Core NATS publish is not a durable consumer acknowledgement | None |
| RabbitMQ | Retry after publish acceptance followed by lost ACK or attached sibling failure | Any failure after detached relay acceptance; Nervix does not enable publisher confirms, and queue durability and message persistence remain broker policy | None |
| SQS | Retry after an ambiguous `SendMessage` result, lost ACK, or attached sibling failure | Any failure after detached relay acceptance; SQS retains its own at-least-once behavior | None |
| MQTT | Retry after client acceptance followed by lost ACK or attached sibling failure | Any failure after detached relay acceptance; Nervix emits with MQTT QoS 0, so broker or subscriber loss is possible after client acceptance | None |
| Redis Pub/Sub | Retry after Redis accepts `PUBLISH` but the Nervix ACK is lost, or after attached sibling failure | Any failure after detached relay acceptance; subscribers that are absent or disconnected miss the message | None |
| ZeroMQ | Retry after socket send acceptance followed by lost ACK or attached sibling failure | Any failure after detached relay acceptance; socket send does not establish durable receiver storage | None |
| Kinesis | Retry after an ambiguous `PutRecord` result, lost ACK, or attached sibling failure | Any failure after detached relay acceptance; stream retention remains Kinesis policy | None |
| Sentry | Retry after an ambiguous HTTP result, lost ACK, or attached sibling failure | Any failure after detached relay acceptance; an accepted event can still be subject to Sentry service policy | None |
| ClickHouse | Retry after an ambiguous insert result, lost ACK, or attached sibling failure | Any failure after detached relay acceptance; a crash after insert but before acknowledgement can also leave an inserted batch that later retries | None |
| Postgres | Retry after an ambiguous transaction result, lost ACK, or attached sibling failure | Any failure after detached relay acceptance; a committed insert can survive a crash before Nervix observes success | `ON CONFLICT` |
| MySQL | Retry after an ambiguous transaction result, lost ACK, or attached sibling failure | Any failure after detached relay acceptance; a committed insert can survive a crash before Nervix observes success | `ON CONFLICT` |
| MongoDB | Retry after an ambiguous write result, lost ACK, or attached sibling failure | Any failure after detached relay acceptance; a committed write can survive a crash before Nervix observes success | `ON CONFLICT` |
| Iceberg | Retry after a commit with a lost ACK or attached sibling failure; appends repeat rows | Crash before catalog commit loses staged work unless the attached source redelivers; detached mode accepts that loss | None; appends are not idempotent |

`ATTACHED` waits only for the completion point exposed by the sink client. It does not make an
ephemeral transport durable. MQTT QoS 0, Core NATS, RabbitMQ without publisher confirms, Redis
Pub/Sub, and ZeroMQ can still lose a message after Nervix observes client-side acceptance.

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
upstream ACK: a crash or publish failure after relay acceptance can lose it.

See [Data Plane](data-plane.md#ack-composition) for the relay fan-out mechanics and
[What It Is Not](what-it-is-not.md) for the persistence boundary.
