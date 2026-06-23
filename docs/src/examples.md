#  Examples

All `CREATE` statements in these examples may also be written as `CREATE IF NOT EXISTS ...` when the desired behavior is idempotent creation.

Runnable end-to-end example files live under `examples/`. `examples/iot/iot.nspl`
pairs with `examples/iot/generate_iot_load.py` for MQTT smart-factory telemetry.
`examples/nats-factory-windows/nats_factory_windows.nspl` pairs with
`examples/nats-factory-windows/generate_nats_factory_load.py` for NATS ingestion,
branch-preserving repartitioning, alert routing, and window aggregate summaries.
`examples/datalake/datalake.nspl` pairs with
`examples/datalake/generate_datalake_load.py` and
`examples/datalake/geo-wasm-guest` for MQTT CBOR IoT activity, NATS protobuf
edge activity, Kafka JSON auth decisions, branch-local correlation, edge-site
lookup enrichment, DB-IP-style GeoIP feature resolution in Rust WASM, Kafka and
Redis alert emission, and event-type Iceberg tables.
The default Docker Compose graph initializes the RustFS bucket and the datalake
REST catalog namespace and tables explicitly before the graph appends data.
For WASM processor guest testing, see `examples/wasm-processors/wasm-dual.nspl`,
which uploads both the Rust and Go guest modules and wires them into one graph.
`examples/binance-websocket/binance_websocket.nspl` connects to the real Binance
spot WebSocket API with an explicit `SIGNALING PROTOCOL` subscription handshake
and normalizes the received stream events with a JAQ-native codec. Binance may
restrict API access by jurisdiction or network location, so this example should
be run only from environments where Binance permits API access.

## Basic Kafka Ingestion

```nspl
CREATE SCHEMA notification (
  user_id U32,
  created_at DATETIME,
  payload STRING OPTIONAL
);

CREATE JSON WIRE SCHEMA notification_wire (
  user_id integer,
  created_at string,
  payload string OPTIONAL
);

CREATE CODEC notification_codec
  FROM WIRE JSON SCHEMA notification_wire
  TO SCHEMA notification
  ENCODE created_at AS RFC3339;

CREATE RELAY notifications SCHEMA notification CAPACITY 1;

CREATE CLIENT kafka_main
  TYPE KAFKA
  CONFIG {
    'bootstrap.servers' = 'localhost:9092',
    'group.id' = 'nervix-dev'
  };

CREATE INGESTOR kafka_notifications
  TO notifications
  DECODE USING notification_codec
  PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 5m
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  FROM KAFKA kafka_main
  TOPIC notifications
  OFFSET BY CONSUMER GROUP nervix_consumer
  MODE ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
```

## Kafka TLS Ingestion With A Mounted Resource

```nspl
CREATE RESOURCE dev_tls;
UPLOAD RESOURCE dev_tls VERSION './tls/dev';

CREATE CLIENT kafka_tls
  TYPE KAFKA
  MOUNT dev_tls
  CONFIG {
    'bootstrap.servers' = 'localhost:9094',
    'security.protocol' = 'ssl',
    'ssl.ca.location' = '{{ dev_tls }}/ca.pem'
  };

CREATE INGESTOR kafka_notifications_tls
  TO notifications
  DECODE USING notification_codec
  PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 5m
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  FROM KAFKA kafka_tls
  TOPIC notifications_tls
  OFFSET BY CONSUMER GROUP nervix_consumer_tls
  MODE ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
```

## Basic Pulsar Ingestion

```nspl
CREATE CLIENT pulsar_main
  TYPE PULSAR
  CONFIG {
    'addr' = 'pulsar://127.0.0.1:6650'
  };

CREATE INGESTOR pulsar_notifications
  TO notifications
  DECODE USING notification_codec
  PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 5m
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  FROM PULSAR pulsar_main
  TOPIC notifications
  SUBSCRIPTION nervix_notifications
  INSTANCES 1
  MODE ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
```

## Basic Pulsar Emission

```nspl
CREATE CLIENT pulsar_main
  TYPE PULSAR
  CONFIG {
    'addr' = 'pulsar://127.0.0.1:6650'
  };

CREATE EMITTER pulsar_notifications
  FROM notifications
  ENCODE USING notification_codec
  TO PULSAR pulsar_main TOPIC notifications_out ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
```

## Pulsar TLS Ingestion With A Mounted Resource

```nspl
CREATE RESOURCE dev_tls;
UPLOAD RESOURCE dev_tls VERSION './tls/dev';

CREATE CLIENT pulsar_tls
  TYPE PULSAR
  MOUNT dev_tls
  CONFIG {
    'addr' = 'pulsar+ssl://127.0.0.1:6651',
    'tls_ca_file' = '{{ dev_tls }}/ca.pem'
  };

CREATE INGESTOR pulsar_notifications_tls
  TO notifications
  DECODE USING notification_codec
  PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 5m
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  FROM PULSAR pulsar_tls
  TOPIC notifications_tls
  SUBSCRIPTION nervix_pulsar_tls
  MODE ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
```

## JSON JAQ On Ingestion

```nspl
CREATE CODEC notification_codec
  FROM JSON
  TO SCHEMA notification
  WITH JAQ TRANSFORMATION '.payload'
  ENCODE created_at AS RFC3339;
```

This is useful when inbound JSON wraps the actual record:

```json
{"payload":{"user_id":42,"created_at":"2025-01-02T03:04:05+00:00","payload":"hello"}}
```

## CBOR Codec

```nspl
CREATE CODEC notification_cbor
  FROM CBOR
  TO SCHEMA notification
  WITH JAQ TRANSFORMATION '.';
```

## Protobuf Codec

```nspl
CREATE CODEC notification_proto
  FROM PROTOBUF
  USING RESOURCE proto_bundle VERSION 1
  CONFIG {'file' = 'notification.proto', 'include' = '.'}
  MESSAGE 'nervix.test.Notification'
  TO SCHEMA notification
  WITH JAQ TRANSFORMATION '{user_id: .user_id, payload: .payload}';
```

## JSON JAQ On Emitting

```nspl
CREATE CODEC notification_codec
  FROM JSON
  TO SCHEMA notification
  WITH JAQ TRANSFORMATIONS ON EMITTING '{payload: .}'
  ENCODE created_at AS RFC3339;
```

This keeps the internal runtime record flat while emitting an envelope.

## Prometheus Ingestion

```nspl
CREATE SCHEMA sample (
  source STRING,
  value F64,
  timestamp DATETIME
);

CREATE JSON WIRE SCHEMA sample_wire (
  source string,
  value number,
  timestamp string
);

CREATE CODEC sample_codec
  FROM WIRE JSON SCHEMA sample_wire
  TO SCHEMA sample
  ENCODE timestamp AS RFC3339;

CREATE RELAY samples SCHEMA sample CAPACITY 1;

CREATE CLIENT prom_main
  TYPE PROMETHEUS
  CONFIG {
    'addr' = 'http://127.0.0.1:9090'
  };

CREATE INGESTOR prom_samples
  TO samples
  DECODE USING sample_codec
  PARAMETERIZED BY source_branch VALUES { source = samples.source } TTL 5m
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  FROM PROMETHEUS prom_main
  QUERY 'label_replace(vector(42.5), "source", "local", "", "")'
  EVERY 1s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

SUBSCRIBE SESSION TO samples;
```

Prometheus vector samples are flattened into JSON objects made of labels plus `value` and `timestamp`, then decoded through the configured codec.

## Forwarding With Filter-Map Rewrites

```nspl
CREATE SCHEMA notification_in (
  tenant STRING,
  user_id U32,
  active BOOL,
  amount I64,
  raw STRING
);

CREATE SCHEMA notification_out (
  tenant STRING,
  user_id U32,
  amount I64,
  normalized STRING
);

CREATE SCHEMA user_branch (
  tenant STRING,
  user_id U32
);

CREATE RELAY notifications SCHEMA notification_in PARAMETERIZED BY user_branch;
CREATE RELAY projected_notifications SCHEMA notification_out PARAMETERIZED BY user_branch;

CREATE ROUTER project_notifications
  FROM notifications
  SET notifications.normalized = lower(trim(notifications.raw)), notifications.amount = notifications.amount + 1
  UNSET notifications.raw, notifications.active
  WHERE trim(notifications.raw) != ''
  DEFAULT TO projected_notifications
  PARAMETERIZED BY user_branch
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
```

This keeps the existing branch grouping, rewrites the record shape, drops rows with empty raw text, and forwards the surviving rows into a second relay without changing native grouping.

## Generate From Materialized State

```nspl
CREATE RELAY notifications
  SCHEMA notification
  WITH MATERIALIZED STATE LAST BY TIMESTAMP;

CREATE RELAY generated_notifications
  SCHEMA notification;

CREATE GENERATOR synth_notifications
  TO generated_notifications
  EACH 100ms
  FLUSH EACH 1s MAX BATCH SIZE 1MiB
  SET generated_notifications.user_id = notifications.user_id,
      generated_notifications.amount = notifications.amount ON MESSAGE ERROR LOG;
```

This periodically reads the current materialized `notifications` state and emits derived rows into `generated_notifications`. A flush policy is mandatory and controls when buffered generated rows are emitted downstream. In a paced domain, both `EACH` and `FLUSH EACH` are evaluated on domain logical time; `FLUSH IMMEDIATE` emits after each generation cycle that produces rows.
