# Branched Processing

One external feed usually mixes many tenants, customers, or devices. Branches are how Nervix
processes those groups independently: when a new group key appears, the branched part of the graph
gets its own relay presence and processor state for that group, fully isolated from every other
group. See [Streams, Branch Keys, And Branches](relay.md#streams-branch-keys-and-branches).

This step partitions orders per customer and deduplicates them inside each customer's branch, so
one customer's duplicates can never suppress another customer's orders.

## Declare The Branch

A branch names its key schema and a TTL. When a branch instance goes idle for the TTL, its
branch-local relay and processor state is dropped as a whole ([TTL](relay.md#ttl)). In an unpaced
domain the TTL uses wall clock time.

```nspl
BEGIN;

CREATE SCHEMA customer_branch (
  customer STRING
);

CREATE BRANCH by_customer SCHEMA customer_branch TTL 5m;

CREATE RELAY orders_by_customer SCHEMA order_record BRANCHED BY by_customer;

CREATE RELAY orders_deduped SCHEMA order_record BRANCHED BY by_customer;
```

## Construct The Branch Key

Only ingestors and reingestors construct branch keys. Since `orders` is already flowing, a
[reingestor](processors.md#reingestor) — a branch-boundary node — re-partitions it without
touching the existing graph:

```nspl
CREATE REINGESTOR partition_orders
  FROM orders
  TO orders_by_customer
    INHERIT ALL
    BRANCHED BY by_customer SET customer = message.customer
    FLUSH EACH 100ms MAX BATCH SIZE 1MiB
    ON MESSAGE ERROR LOG;
```

`BRANCHED BY by_customer SET customer = message.customer` builds the concrete key from each
record. The first order from a customer materializes that customer's branch; every relay and
processor downstream that declares `BRANCHED BY by_customer` runs per customer
([Branch Semantics](ingestors.md#branch-semantics)).

## Deduplicate Per Branch

A [deduplicator](processors.md#deduplicator) drops records whose key it has already seen within a
time window. Declared `BRANCHED BY by_customer`, its history is branch-local:

```nspl
CREATE DEDUPLICATOR dedupe_orders
  FROM orders_by_customer
  DEDUPLICATE ON input.order_id
  MAX TIME 5m
  BRANCHED BY by_customer
  TO orders_deduped
    INHERIT ALL
    FLUSH EACH 250ms MAX BATCH SIZE 1MiB
    ON MESSAGE ERROR LOG;
```

## Emit The Deduplicated Stream

An emitter consumes every concrete branch and collapses branch identity at the external boundary
([Branch Semantics](emitters.md#branch-semantics)) — the Redis channel receives all customers:

```nspl
CREATE EMITTER redis_deduped
  FROM orders_deduped
  TO REDIS PUBSUB redis_local CHANNEL orders_deduped_out
    MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s
    ENCODE USING order_codec
  INHERIT ALL
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;

COMMIT;
```

## Prove The Isolation

Subscribe to `orders_deduped_out`, then produce the **same** `order_id` interleaved across two
customers:

```bash
docker exec redis redis-cli SUBSCRIBE orders_deduped_out
```

```json
{"order_id":"o-2001","customer":"acme","status":"new","amount":900,"quantity":1}
{"order_id":"o-2001","customer":"globex","status":"new","amount":900,"quantity":1}
{"order_id":"o-2001","customer":"acme","status":"new","amount":900,"quantity":1}
```

The first two records pass — `acme` and `globex` each have their own deduplication history. The
third is dropped as a duplicate inside the `acme` branch only. That is branch isolation: state,
batching, and routing for one group never interfere with another. For sizing branched graphs, see
[Capacity Planning For Branched Graphs](capacity-planning.md).

Next: [Error Routes](./quickstart-error-routes.md) handles records that fail instead of dropping
them.
