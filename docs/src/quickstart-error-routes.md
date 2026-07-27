# Error Routes

So far every failure policy has been `LOG`. Nervix can instead capture a failing record as a
structured error — with a stable reference, code, message, and the partially constructed output —
and send it to a dead-letter relay for inspection or downstream handling. See
[Message Errors](processors.md#message-errors).

To provoke real failures, this step computes a per-unit price: `amount / quantity` fails when a
record arrives with `quantity` of zero.

## The Dead-Letter Model

Error handlers construct an ordinary record on an ordinary relay. Fields taken from
`partial_output` must be `OPTIONAL` — the failed operation may not have produced them:

```nspl
BEGIN;

CREATE SCHEMA priced_order (
  order_id STRING,
  customer STRING,
  amount I64,
  quantity I64,
  unit_price I64
);

CREATE SCHEMA order_error (
  error_reference STRING,
  error_code STRING,
  error_message STRING,
  source_order STRING,
  attempted_price I64 OPTIONAL
);

CREATE RELAY priced_orders SCHEMA priced_order UNBRANCHED;

CREATE RELAY order_errors SCHEMA order_error UNBRANCHED;
```

## Send Failures To The Dead Letter

`ON MESSAGE ERROR SEND TO` replaces `LOG` on the route. The handler can read the error metadata
(`error.*`), the original input (`input.*`), and the partial output of the failed construction
(`partial_output.*`):

```nspl
CREATE JUNCTION price_orders
  FROM orders
  UNBRANCHED
  TO priced_orders
    INHERIT ALL EXCEPT status
    SET unit_price = input.amount / input.quantity
    FLUSH IMMEDIATE
    ON MESSAGE ERROR SEND TO order_errors
      SET error_reference = error.reference,
          error_code = error.code,
          error_message = error.message,
          source_order = input.order_id,
          attempted_price = partial_output.unit_price;

COMMIT;
```

`INHERIT ALL EXCEPT status` inherits everything but the one field `priced_order` does not
declare. Error routes preserve the branch in which the failure executed — this junction is
unbranched, so `order_errors` is unbranched too. The node-level `ON GENERAL ERROR` policy exists
only on ingestors and emitters
([Runtime Node Error Policies](nspl-overview.md#runtime-node-error-policies)).

## Trigger A Failure

Watch the dead-letter relay with a session subscription:

```bash
nervix-cli --domain quickstart subscribe errors order_errors
```

Produce one healthy record and one with `quantity` of zero:

```json
{"order_id":"o-3001","customer":"acme","status":"new","amount":900,"quantity":3}
{"order_id":"o-3002","customer":"acme","status":"new","amount":900,"quantity":0}
```

`o-3001` flows to `priced_orders` with `unit_price` of `300`. `o-3002` fails the division and
arrives on `order_errors` as a structured record: a stable `error_reference`, the machine-readable
`error_code`, the human-readable `error_message`, and a null `attempted_price` — the division
never produced one. Hot-path errors never leak sensitive payload values.

Next: add a second way into the graph in [HTTP Ingestion](./quickstart-http-ingestion.md).
