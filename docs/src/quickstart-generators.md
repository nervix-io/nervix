# Generators

Every node so far reacts to input. A generator is the opposite: it has no input relay and produces
records on a timer, reading a snapshot of [materialized relay
state](relay.md#materialized-state) on every tick. The classic use is a per-key heartbeat or
periodic re-publication of the latest known value.

## Materialize The Relay

Materialized state keeps the latest full record per branch instance. It is a relay property, and
an existing relay can adopt it with `ALTER RELAY`
([Altering Relays](relay.md#altering-relays)):

```plexus
BEGIN;

ALTER RELAY orders_by_customer SET MATERIALIZED STATE LAST BY TIMESTAMP;
```

`LAST BY TIMESTAMP` keeps the newest record per customer according to record timestamp
watermarks — ingestion time works out of the box in an unpaced domain
([Ingestion Timestamps](domains-and-time.md#ingestion-timestamps)).

## The Generator

A generator declares exactly one materialized relay and reads it through the
`relay_state.<relay>.<field>` namespace. Its routes are set-only and, unlike windows, **do**
require a flush policy. Branch identity is node-wide: the generator, its materialized relay, and
its output relay must share the exact branch.

```plexus
CREATE SCHEMA order_heartbeat (
  customer STRING,
  last_order_id STRING,
  last_amount I64
);

CREATE RELAY order_heartbeats SCHEMA order_heartbeat BRANCHED BY by_customer;

CREATE GENERATOR order_heartbeat_source
  USING MATERIALIZED STATE orders_by_customer
  EACH 5s
  BRANCHED BY by_customer
  TO order_heartbeats
    SET customer = relay_state.orders_by_customer.customer,
        last_order_id = relay_state.orders_by_customer.order_id,
        last_amount = relay_state.orders_by_customer.amount
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG;

COMMIT;
```

When a customer's branch first materializes, that branch gets its own generator task; every `5s`
tick reads an immutable snapshot of the customer's latest order and constructs one heartbeat.
Branch eviction (the branch TTL) stops the task and drops its state. In an unpaced domain the
cadence follows wall clock time; paced domains tick on the domain clock
([Domains And Time](domains-and-time.md)).

The first generator occurrence is immediate when the generator starts. Later occurrences stay on
the original `EACH` schedule. Slow work coalesces missed ticks into one generation at the newest
due occurrence, then continues at the first future boundary without a catch-up burst.

## Watch It Tick

```bash
nervix-cli --domain quickstart subscribe heartbeat_watch order_heartbeats
```

Nothing appears yet — a branched generator is silent until a branch exists. Produce one `acme`
order and heartbeats start arriving every five seconds, repeating the latest `acme` order.
Produce a `globex` order and a second, independent heartbeat stream joins in. Send a newer `acme`
order and the `acme` heartbeat switches to it on the next tick.

One lifecycle caveat: `START` clears materialized state for the domain, so after a `STOP`/`START`
the generators stay silent until new records materialize branches again
([Materialized State](relay.md#materialized-state)).

## Collect Samples For A Fixed Tensor

A generator can sample the materialized relay and send each sample to a count window. The window
owns the retained sequence. This example uses the existing `orders_by_customer` relay and
`by_customer` branch, and an uploaded resource `sequence_model` containing `score.onnx`, with a
required `F32[2]` input named `features` and a required `F32[1]` output named `score`:

```plexus
CREATE SCHEMA order_sample (amount F32, sampled_at DATETIME);
CREATE SCHEMA order_sequence (series ARRAY<F32, 2>, sampled_at ARRAY<DATETIME, 2>);
CREATE SCHEMA order_score (score ARRAY<F32, 1>);
CREATE RELAY order_samples SCHEMA order_sample BRANCHED BY by_customer;
CREATE RELAY order_sequences SCHEMA order_sequence BRANCHED BY by_customer;
CREATE RELAY order_scores SCHEMA order_score BRANCHED BY by_customer;

CREATE GENERATOR sample_orders
  USING MATERIALIZED STATE orders_by_customer EACH 5s
  BRANCHED BY by_customer TO order_samples
    SET amount = relay_state.orders_by_customer.amount AS F32,
        sampled_at = now()
    FLUSH IMMEDIATE ON MESSAGE ERROR LOG;

CREATE WINDOW PROCESSOR collect_samples FROM order_samples
  WIDTH 2 MESSAGES STEP 2 MESSAGES BRANCHED BY by_customer
  TO order_sequences
    SET series = COLLECT_ARRAY(input.amount),
        sampled_at = COLLECT_ARRAY(input.sampled_at)
    ON MESSAGE ERROR LOG;

CREATE INFERENCER score_sequence FROM order_sequences
  USING RESOURCE sequence_model VERSION 1 FILE 'score.onnx'
  INPUTS { 'features' DENSE TENSOR<F32>[2] = input.series }
  OUTPUT SCHEMA { 'score' DENSE TENSOR<F32>[1] }
  BRANCHED BY by_customer TO order_scores SET score = score
  FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
```

Each customer has its own two-sample array and score. `COLLECT_ARRAY` gets its fixed length from
the count-only `WIDTH`, so it binds the inferencer's fixed axis directly.
[Window aggregate types](filter-map-functions.md#window-aggregates).

The two samples need not be exactly five seconds apart. Slow execution coalesces occurrences,
and a restart anchors a new generator cadence. `sampled_at` records the actual domain-time
snapshot beside each value; inspect or subscribe to `order_sequences` to see those gaps. `START`
preserves the window's retained samples and clears materialized relay state, so each branch needs
new relay input before generation resumes after that operation. Node recovery restores persisted
materialized entries ([Materialized State](relay.md#materialized-state)).

## Shape A Sequence For A Matrix

The composed sequence scenario builds a two-by-three inference matrix from two samples for
each tenant. In an unpaced domain, send records of the `reading` schema to `readings`, using
`tenant` as the branch key. Upload a directory containing `models/matrix_identity.onnx` as
version 1 of resource `inference`. The model returns the matrix it receives.
[Composed sequence scenario](https://github.com/nervix-io/nervix/blob/main/tests/features/runtime/sequence_values.feature),
[ONNX fixture](https://github.com/nervix-io/nervix/blob/main/tests/fixtures/onnx/matrix_identity.onnx).

Declare the record shapes and branch-local relays. Two readings form `values`, and three features
per reading form the two-by-three `neighborhoods`. The junction prepares six normalized values
and finishes with a matrix whose cells are `F32`.

```plexus
CREATE SCHEMA reading (tenant STRING, sample_id I64, value F64);
CREATE SCHEMA tenant_key (tenant STRING);
CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
CREATE SCHEMA history (
  values ARRAY<F64, 2>,
  neighborhoods ARRAY<F64, 2, 3>,
  sample_count I64,
  total F64,
  average F64,
  deviation F64
);
CREATE SCHEMA analysis (
  values ARRAY<F64, 2>,
  neighborhoods ARRAY<F64, 2, 3>,
  sample_count I64,
  total F64,
  average F64,
  deviation F64,
  list_deviation F64 OPTIONAL,
  normalized ARRAY<F64, 6>,
  matrix ARRAY<F32, 2, 3>
);
CREATE SCHEMA inferred (transformed ARRAY<F32, 2, 3>);
CREATE RELAY readings SCHEMA reading BRANCHED BY by_tenant WITH MATERIALIZED STATE LAST BY TIMESTAMP;
CREATE RELAY samples SCHEMA reading BRANCHED BY by_tenant;
CREATE RELAY unique_samples SCHEMA reading BRANCHED BY by_tenant;
CREATE RELAY histories SCHEMA history BRANCHED BY by_tenant;
CREATE RELAY matrices SCHEMA analysis BRANCHED BY by_tenant;
CREATE RELAY scores SCHEMA inferred BRANCHED BY by_tenant;
```

The generator can sample the same materialized record more than once. The deduplicator lets
each `sample_id` contribute once per tenant during its retention period.

```plexus
CREATE GENERATOR sample_readings USING MATERIALIZED STATE readings EACH 100ms
  BRANCHED BY by_tenant TO samples
    SET tenant = relay_state.readings.tenant,
        sample_id = relay_state.readings.sample_id,
        value = relay_state.readings.value
    FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
CREATE DEDUPLICATOR sample_once FROM samples
  DEDUPLICATE ON input.sample_id MAX TIME 5m
  BRANCHED BY by_tenant TO unique_samples INHERIT ALL
    FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
```

The count window emits one history record when that branch has two distinct samples. It collects
both the raw readings and the three features of each reading.

```plexus
CREATE WINDOW PROCESSOR collect_samples FROM unique_samples
  WIDTH 2 MESSAGES STEP 2 MESSAGES BRANCHED BY by_tenant
  TO histories
    SET values = COLLECT_ARRAY(input.value),
        neighborhoods = COLLECT_ARRAY([input.value - 2.0, input.value, input.value + 2.0]),
        sample_count = COUNT(input.value),
        total = SUM(input.value),
        average = AVG(input.value),
        deviation = STDDEV_POP(input.value)
    ON MESSAGE ERROR LOG;
```

The junction flattens the neighborhoods, subtracts the average and divides by the list's
deviation. It converts the normalized values to `F32` and reshapes the six cells. The junction's
message-error policy handles per-message failures in this preparation. The inferencer receives
the required field already shaped to its declared input.

```plexus
CREATE JUNCTION normalize_samples FROM histories BRANCHED BY by_tenant
  TO matrices INHERIT ALL
    SET list_deviation = list_stddev_pop(input.values),
        normalized = list_div_scalar(list_sub_scalar(list_flatten(input.neighborhoods), input.average),
                                     coalesce(output.list_deviation, 1.0)),
        matrix = list_reshape(output.normalized AS ARRAY<F32, 6>, 2, 3)
    FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
CREATE INFERENCER read_matrix FROM matrices
  USING RESOURCE inference VERSION 1 FILE 'models/matrix_identity.onnx'
  INPUTS { 'matrix' DENSE TENSOR<F32>[2, 3] = input.matrix }
  OUTPUT SCHEMA { 'transformed' DENSE TENSOR<F32>[2, 3] }
  BRANCHED BY by_tenant TO scores SET transformed = transformed
  FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
```

Subscribe to `unique_samples`, `matrices` and `scores`, and start the domain. Send sample ID 1
with values `2.0` for `acme` and `18.0` for `beta`. Once both reach `unique_samples`, send ID 2
with values `6.0` for `acme` and `10.0` for `beta`. The `acme` matrix is
`[[-2.0, -1.0, 0.0], [0.0, 1.0, 2.0]]`. The `beta` matrix is
`[[0.5, 1.0, 1.5], [-1.5, -1.0, -0.5]]`. The `scores` subscription receives the same matrices in
`transformed`, each on its tenant's branch.

Next: join two streams in [Correlators](./quickstart-correlators.md).
