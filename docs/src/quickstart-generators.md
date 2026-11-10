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
the count-only `WIDTH`, so it binds the inferencer's fixed axis directly. A duration or combined
bound requires `COLLECT_VEC`, whose variable length does not bind that fixed axis.

The two samples need not be exactly five seconds apart. Slow execution coalesces occurrences,
and a restart anchors a new generator cadence. `sampled_at` records the actual domain-time
snapshot beside each value; inspect or subscribe to `order_sequences` to see those gaps. `START`
preserves the window's retained samples and clears materialized relay state, so each branch needs
new relay input before generation resumes after that operation. Node recovery restores persisted
materialized entries ([Materialized State](relay.md#materialized-state)).

Next: join two streams in [Correlators](./quickstart-correlators.md).
