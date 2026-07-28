# Conditional Routing

The pass-through pipeline treats every order the same. This step adds a
[junction](processors.md#junction) that splits the stream: high-value orders get a computed
`tier` field and their own Redis channel, routine orders continue separately.

Everything here is additive. The domain keeps running while you apply it — a transaction of pure
`CREATE` statements rebuilds the schedule without pausing the graph
([Automatic Model-Alteration Quiescing](domains-and-time.md#automatic-model-alteration-quiescing)).

## The Enriched Record

Construction is exact-typed, so a record with an extra `tier` field is a different schema, with
its own wire schema and codec for emission — the same pattern as the first pipeline:

```nspl
BEGIN;

CREATE SCHEMA order_tiered (
  order_id STRING,
  customer STRING,
  status STRING,
  amount I64,
  quantity I64,
  tier STRING
);

CREATE STRICT WIRE JSON SCHEMA order_tiered_wire (
  order_id string,
  customer string,
  status string,
  amount integer,
  quantity integer,
  tier string
);

CREATE CODEC order_tiered_codec
  FROM WIRE JSON SCHEMA order_tiered_wire
  TO SCHEMA order_tiered;

CREATE RELAY high_value_orders SCHEMA order_tiered UNBRANCHED;

CREATE RELAY routine_orders SCHEMA order_record UNBRANCHED;
```

## Route With A Junction

A junction subscribes to one or more relays and fans records out to routes. Each route is an
independent contract: its own construction, its own `WHERE` filter over the finalized output, its
own flush policy. See [Junction](processors.md#junction) and
[The Working Message](working-message.md) for the `input`/`output` scopes used below.

```nspl
CREATE JUNCTION route_orders
  FROM orders
  UNBRANCHED
  TO high_value_orders
    INHERIT ALL
    SET tier = CASE
          WHEN input.amount >= 10000 THEN "vip"
          ELSE "high"
        END
    WHERE output.amount >= 1000
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG
  TO routine_orders
    INHERIT ALL
    WHERE output.amount < 1000
    FLUSH EACH 1s MAX BATCH SIZE 1MiB
    ON MESSAGE ERROR LOG;
```

- `SET` initializes the one field `INHERIT ALL` cannot supply. Assignments run in order, and
  `CASE`/`IF` expressions are part of the expression language
  ([Conditional Expressions](filter-map-functions.md#conditional-expressions)).
- The route `WHERE` filters finalized records: `output.amount` is the value the route just
  constructed, while `input.amount` inside `SET` reads the incoming record.
- The `orders` relay now has two subscribers — the emitter from the first pipeline and this
  junction. Relays fan out to every subscriber ([Streams And State](relay.md)).

## Emit The High-Value Stream

```nspl
CREATE EMITTER redis_high_value
  FROM high_value_orders
  ENCODE USING order_tiered_codec
  TO REDIS PUBSUB redis_local CHANNEL high_value_out
  INHERIT ALL
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;

COMMIT;
```

## Watch The Split

Subscribe to the new channel:

```bash
docker exec redis redis-cli SUBSCRIBE high_value_out
```

Produce three orders with different amounts:

```json
{"order_id":"o-1002","customer":"acme","status":"new","amount":250,"quantity":5}
{"order_id":"o-1003","customer":"globex","status":"new","amount":2500,"quantity":1}
{"order_id":"o-1004","customer":"acme","status":"new","amount":25000,"quantity":2}
```

`o-1002` appears only on `orders_out`. `o-1003` also appears on `high_value_out` with
`"tier":"high"`, and `o-1004` with `"tier":"vip"`.

Next: [Branched Processing](./quickstart-branched-processing.md) partitions this stream per
customer.
