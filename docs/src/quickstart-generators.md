# Generators

Every node so far reacts to input. A generator is the opposite: it has no input relay and produces
records on a timer, reading a snapshot of [materialized relay
state](relay.md#materialized-state) on every tick. The classic use is a per-key heartbeat or
periodic re-publication of the latest known value.

## Materialize The Relay

Materialized state keeps the latest full record per branch instance. It is a relay property, and
an existing relay can adopt it with `ALTER RELAY`
([Altering Relays](relay.md#altering-relays)):

```nspl
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

```nspl
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

## Watch It Tick

```bash
nervix-cli --domain quickstart subscribe heartbeat_watch order_heartbeats
```

Nothing appears yet — a branched generator is silent until a branch exists. Produce one `acme`
order and heartbeats start arriving every five seconds, repeating the latest `acme` order.
Produce a `globex` order and a second, independent heartbeat stream joins in. Send a newer `acme`
order and the `acme` heartbeat switches to it on the next tick.

One lifecycle caveat: `START` clears materialized state for the domain, so after a restart the
generators stay silent until new records materialize branches again
([Materialized State](relay.md#materialized-state)).

Next: join two streams in [Correlators](./quickstart-correlators.md).
