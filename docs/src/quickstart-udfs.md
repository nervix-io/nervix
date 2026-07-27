# User-Defined Functions

The `CASE` expression in [Conditional Routing](./quickstart-conditional-routing.md) works, but the
banding logic is trapped inside one junction. A UDF packages it as a named function every node can
call. UDFs are written in [Roto](roto-language-reference.md), compiled to native code, and operate
on whole Arrow columns at a time; the source is inline in the statement — there is no separate
upload step. See [Declaration](udfs.md#declaration).

## Declare The Function

```nspl
BEGIN;

CREATE UDF amount_band
  WITH ROTO_0_11
  ARGS (amount I64)
  RETURNS STRING
  CODE $roto$
    fn amount_band(amount: I64Column) -> StringColumn {
        when_s(amount.gt_s(9999), "vip")
            .when_s(amount.gt_s(999), "high")
            .otherwise_s("routine")
    }
  $roto$;
```

The declared signature must exactly match the Roto entry function — same name, matching column
types (an `I64` argument is an `I64Column`), no implicit casts. `$roto$ ... $roto$` is a dollar-quoted string, so
the body spans lines verbatim. The available column operations are cataloged in
[Roto Column Operations](udfs.md#roto-column-operations); UDFs are deterministic unless declared
`VOLATILE` ([Nulls, Errors, And Volatility](udfs.md#nulls-errors-and-volatility)).

## Call It

Calls always use the `udf::` qualifier and compose with builtins and route filters
([Calling UDFs](udfs.md#calling-udfs)):

```nspl
CREATE RELAY banded_orders SCHEMA order_tiered UNBRANCHED;

CREATE JUNCTION band_orders
  FROM orders
  UNBRANCHED
  TO banded_orders
    INHERIT ALL
    SET tier = udf::amount_band(input.amount)
    WHERE udf::amount_band(input.amount) != "routine"
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG;

COMMIT;
```

This reuses the `order_tiered` schema from Conditional Routing — the same enrichment, now
expressed once and callable from any junction, filter, or route in the domain.

## Watch It Classify

```bash
nervix-cli --domain quickstart subscribe band_watch banded_orders
```

Produce orders with amounts `500`, `2500`, and `25000` on the Kafka topic (the junction reads the
Kafka-fed `orders` relay). The `500` order is filtered out by the route `WHERE`; the others arrive
with `"tier":"high"` and `"tier":"vip"`.

Lifecycle notes: there is no `ALTER UDF` — changing the body means `DROP UDF amount_band;` and
recreating it, and the drop is rejected while any node still references the function
([Introspection And Lifecycle](udfs.md#introspection-and-lifecycle)). For where UDFs sit between
builtins and WASM processors, see
[Choosing An Extension Tier](filter-map-functions.md#choosing-an-extension-tier).

Next: aggregate the stream in [Window Aggregates](./quickstart-windows.md).
