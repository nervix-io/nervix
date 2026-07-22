# Examples

Runnable end-to-end graphs live under `examples/`:

- `examples/iot/iot.nspl` implements MQTT smart-factory telemetry with branch-local quality gates,
  repartitioning, ordering, and alert emission.
- `examples/nats-factory-windows/nats_factory_windows.nspl` demonstrates NATS ingestion and
  route-local window aggregate construction.
- `examples/datalake/datalake.nspl` combines MQTT CBOR, NATS Protobuf, Kafka JSON, correlation,
  materialized lookup state, Rust WASM enrichment, Kafka/Redis output, and Iceberg tables.
- `examples/wasm-processors/wasm-dual.nspl` wires Rust and Go guests into set-only multi-route WASM
  processors.

The Docker Compose setup explicitly provisions the external bucket, catalog namespace, and tables
used by the datalake graph. Nervix never creates those external entities during node startup.

## Kafka ingestion with route-local branch construction

```nspl
CREATE SCHEMA user_id_branch (
  user_id U32
);

CREATE BRANCH by_user SCHEMA user_id_branch TTL 5m;

CREATE SCHEMA notification (
  user_id U32,
  created_at DATETIME,
  payload STRING OPTIONAL
);

CREATE STRICT WIRE JSON SCHEMA notification_wire (
  user_id integer,
  created_at string,
  payload string OPTIONAL
);

CREATE CODEC notification_codec
  FROM WIRE JSON SCHEMA notification_wire
  TO SCHEMA notification
  ENCODE created_at AS RFC3339;

CREATE RELAY notifications
  SCHEMA notification
  BRANCHED BY by_user;

CREATE CLIENT kafka_main
  TYPE KAFKA
  CONFIG {
    'bootstrap.servers' = 'localhost:9092'
  };

CREATE INGESTOR kafka_notifications
  FROM KAFKA kafka_main
  TOPIC notifications
  OFFSET BY CONSUMER GROUP nervix_consumer
  MODE ACK SEQUENTIAL
  DECODE USING notification_codec
  TO notifications
    INHERIT ALL
    BRANCHED BY by_user SET user_id = message.user_id
    FLUSH EACH 100ms MAX BATCH SIZE 1MiB
    ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

The route output is explicitly inherited from the decoded input. The branch key is constructed only
after output finalization and route filtering.

## Transforming projection

```nspl
CREATE DEDUPLICATOR project_notifications
  FROM notifications WHERE input.active
  DEDUPLICATE ON input.tenant, input.event_id
  MAX TIME 10m
  BRANCHED BY by_tenant
  TO projected_notifications
    INHERIT ALL EXCEPT raw, active
    SET amount = amount + 1,
        normalized = lower(trim(input.raw))
    WHERE output.amount > 0
    FLUSH IMMEDIATE
    ON MESSAGE ERROR SEND TO processing_errors
      SET error_reference = error.reference,
          error_code = error.code,
          source_id = input.event_id,
          attempted_amount = partial_output.amount;
```

`INHERIT` and `SET` belong to this route only. Assignments run left to right. `input.raw` is the
original record, while `output.amount` is the finalized route value.

## ONNX inference

Inferencers keep explicit tensor declarations. Their routes are set-only and do not inherit the
source row:

```nspl
CREATE INFERENCER score_message
  FROM input_features
  USING RESOURCE inference VERSION 1
  FILE 'score.onnx'
  INPUTS {
    "features" DENSE TENSOR<F32>[128] = input.features
  }
  OUTPUT SCHEMA {
    "scores" DENSE TENSOR<F32>[10]
  }
  BRANCHED BY by_tenant
  TO scored_output
    SET tenant = branch.tenant,
        scores = scores
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG;
```

The `INPUTS` mapping may read the original row. Route construction sees immutable generated model
output, the branch, and declared materialized state, but not `input` or `message`.

## Window construction

```nspl
CREATE WINDOW PROCESSOR device_summary
  FROM device_measurements
  FILTER WHERE input.value >= 0.0
  WIDTH 5m
  STEP 1m
  BRANCHED BY by_device
  TO device_windows
    SET device_id = branch.device_id,
        samples = COUNT(input.value),
        minimum = MIN(input.value),
        maximum = MAX(input.value),
        average = SUM(input.value) / COUNT(input.value)
    WHERE output.samples > 0
    ON MESSAGE ERROR LOG;
```

Windows have no `AGGREGATE` or `FLUSH` clause. `input` is legal only inside aggregate arguments,
though aggregate calls may participate in larger scalar expressions.

## Generate from materialized state

```nspl
CREATE GENERATOR synth_notifications
  USING MATERIALIZED STATE notification_templates
  EACH 100ms
  BRANCHED BY by_tenant
  TO generated_notifications
    SET tenant = branch.tenant,
        user_id = relay_state.notification_templates.user_id,
        amount = relay_state.notification_templates.amount
    FLUSH EACH 1s MAX BATCH SIZE 1MiB
    ON MESSAGE ERROR LOG;
```

A generator declares exactly one materialized relay. Every concrete materialized branch starts an
independent task, and every route sees the same immutable state snapshot for a tick.

## Codec emission with staged headers

```nspl
CREATE EMITTER kafka_notifications
  FROM notifications
  ENCODE USING notification_codec
  TO KAFKA kafka_main TOPIC notifications_out
  INHERIT ALL EXCEPT secret
  SET secret = leak_sensitive(input.secret)
  WHERE output.active
  INVOKE write_header('tenant', input.tenant),
         write_header('trace-id', input.trace_id)
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Header calls execute in order only after payload construction and filtering. Payload and header
publication are all-or-nothing for a route attempt.
