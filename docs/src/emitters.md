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
means Nervix collects an in-memory Arrow batch before handing it to the external sink. During
normal processing, `FLUSH IMMEDIATE` starts a system-owned 100 µs minimum batching timeout when the
first pending input arrives; it has no size boundary. For most emitters the collected batch is
encoded and published on the flush boundary. Iceberg additionally supports `COMMIT EACH <duration>
MAX SIZE <bytes>`: flush writes local Arrow IPC staging files, and commit appends the staged data to
object storage.

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

`message.field` uses transforming working-output semantics, `input.field` always reads the source
relay row, and `output.field` requires prior initialization. Relay-qualified fields are invalid.
There is no implicit identity transformation and no `UNSET`; use `INHERIT ALL EXCEPT`.

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
behavior. Kinesis, Redis, MQTT, ZeroMQ, direct database sinks, and Iceberg reject header writes.

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

Iceberg uses two explicit boundaries. `FLUSH` collects typed in-memory batches and writes them to local Arrow IPC files under the runtime temporary-file root. `COMMIT EACH <duration> MAX SIZE <bytes>` reads the staged Arrow IPC batches, concatenates them into one Arrow batch, appends that batch to the Iceberg table, and commits the catalog update. The temporary-file root defaults to `/tmp` and can be changed with `--temp-dir` or `NERVIX_TEMP_DIR`. Messages are ACKed only after the staged batches are successfully committed. If the node crashes or hits a fatal error before that point, the in-flight staged data is treated as lost; upstream ingestor policy decides whether the source redelivers. In `DETACHED` mode, Nervix accepts that loss and does not keep the upstream path waiting for the Iceberg commit.

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

## ACK Semantics

Emitters participate in ACK propagation through `ATTACHED` and `DETACHED` mode:

- `ATTACHED`: downstream emitter success or failure stays part of the upstream ACK chain
- `DETACHED`: the upstream path no longer waits for downstream emission success
