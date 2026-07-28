# Correlators

Orders are one stream; payments are another. A [correlator](processors.md#correlator) joins two
streams on a predicate, holding unmatched records in a bounded buffer until their partner arrives
or a deadline passes. There is no ambiguous default input — a correlator has explicit `LEFT` and
`RIGHT` sides, and every expression reads through the `left.` and `right.` scopes.

Branch identity is node-wide here too: the left relays, right relays, output relays, and timeout
targets must all share the correlator's exact branch. This one is fully unbranched, joining the
Kafka-fed `orders` relay with a new payments feed.

## The Payments Feed

```nspl
BEGIN;

CREATE SCHEMA payment_record (
  order_id STRING,
  payment_id STRING,
  amount I64
);

CREATE STRICT WIRE JSON SCHEMA payment_wire (
  order_id string,
  payment_id string,
  amount integer
);

CREATE CODEC payment_codec
  FROM WIRE JSON SCHEMA payment_wire
  TO SCHEMA payment_record;

CREATE RELAY payments SCHEMA payment_record UNBRANCHED;

CREATE ENDPOINT payment_ingress ON edge PATH '/payments' TYPE HTTP;

CREATE INGESTOR payment_source
  FROM ENDPOINT payment_ingress MODE NO_ACK SEQUENTIAL
  DECODE USING payment_codec
  TO payments
    INHERIT ALL
    UNBRANCHED
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

## The Correlator

Matched pairs become `paid_orders` records. Orders that see no payment within `MAX TIME` are
forwarded to `unpaid_orders` by the left side's timeout action; unmatched payments are dropped by
the right side's:

```nspl
CREATE SCHEMA paid_order (
  order_id STRING,
  customer STRING,
  amount I64,
  payment_id STRING
);

CREATE RELAY paid_orders SCHEMA paid_order UNBRANCHED;

CREATE RELAY unpaid_orders SCHEMA order_record UNBRANCHED;

CREATE CORRELATOR correlate_payments
  LEFT FROM orders
  RIGHT FROM payments
  CORRELATE WHERE left.order_id = right.order_id
  MATCH EARLIEST
  MAX TIME 30s
  ON CORRELATION TIMEOUT SEND TO unpaid_orders, DROP
  UNBRANCHED
  TO paid_orders
    SET order_id = left.order_id,
        customer = left.customer,
        amount = left.amount,
        payment_id = right.payment_id
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG;

COMMIT;
```

Reading the clauses:

- `CORRELATE WHERE` is a full boolean expression — equality is typical, but function calls and
  `AND` chains work (`left.order_id = right.order_id AND left.amount = right.amount`).
- `MATCH EARLIEST` pairs an arriving record with the **oldest** pending candidate on the other
  side (`LATEST` takes the newest). Matching is one-to-one and consuming: the chosen pair leaves
  the buffers, and each record correlates at most once.
- `MAX TIME` bounds how long an unmatched record waits — and therefore the correlator's memory
  ([Capacity Planning For Branched Graphs](capacity-planning.md)).
- `ON CORRELATION TIMEOUT <left-action>, <right-action>` takes exactly two actions — `DROP` or
  `SEND TO <relay>`. A timeout forwards the **original record unchanged**, so the target relay's
  schema must be compatible with that side's schema — `unpaid_orders` reuses `order_record`.
- Routes are set-only: `INHERIT` is rejected, and every required field of `paid_order` must be
  assigned from `left.`, `right.`, or `output.` values
  ([Filters And Construction](processors.md#filters-and-construction)). In an
  `ON MESSAGE ERROR SEND TO` handler, a correlator exposes `left` and `right` instead of `input`
  ([Message Errors](processors.md#message-errors)).

## Pay An Order

Watch both outcomes:

```bash
nervix-cli --domain quickstart subscribe paid_watch paid_orders
nervix-cli --domain quickstart subscribe unpaid_watch unpaid_orders
```

Produce an order on the Kafka topic, then pay it over HTTP:

```json
{"order_id":"o-7001","customer":"acme","status":"new","amount":3000,"quantity":1}
```

```bash
curl -i -X POST http://127.0.0.1:8080/payments \
  -H 'Host: orders.example.com' \
  -H 'Content-Type: application/json' \
  -d '{"order_id":"o-7001","payment_id":"p-1","amount":3000}'
```

The joined record appears on `paid_orders`. Now produce order `o-7002` and pay nothing: thirty
seconds later the original order surfaces on `unpaid_orders` — the timeout path in action.

Next: change the running graph without tearing it down in
[Altering A Running Graph](./quickstart-altering.md).
