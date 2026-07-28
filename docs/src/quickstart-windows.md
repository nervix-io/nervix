# Window Aggregates

Individual orders are flowing; now summarize them. A
[window processor](processors.md#window-processor) collects records into sliding windows and
constructs aggregate output. Declared `BRANCHED BY by_customer`, it windows each customer
independently — building directly on [Branched Processing](./quickstart-branched-processing.md).

## The Summary Model

Window routes are set-only: every output field is an aggregate expression, so the output relay
needs its own summary schema (the order schema cannot be reused unless every field is aggregated):

```nspl
BEGIN;

CREATE SCHEMA order_window (
  customer STRING,
  order_count I64,
  total_amount I64,
  max_amount I64,
  avg_amount I64
);

CREATE RELAY order_windows SCHEMA order_window BRANCHED BY by_customer;
```

## The Window Processor

Windows use `WIDTH` and `STEP` instead of `FLUSH`. Each bound can count messages, elapse a
duration, or combine both — a combined bound closes the window when **either** condition is met:

```nspl
CREATE WINDOW PROCESSOR customer_order_window
  FROM orders_by_customer
  WIDTH 3 MESSAGES 30s DURATION
  STEP 3 MESSAGES 30s DURATION
  BRANCHED BY by_customer
  TO order_windows
    SET customer = FIRST(input.customer),
        order_count = COUNT(input.order_id),
        total_amount = SUM(input.amount),
        max_amount = MAX(input.amount),
        avg_amount = SUM(input.amount) / COUNT(input.amount)
  ON MESSAGE ERROR LOG;

COMMIT;
```

- The aggregate functions are `COUNT`, `FIRST`, `LAST`, `MIN`, `MAX`, `SUM`, and
  `PERCENTILE_LINEAR_HISTOGRAM`. There is no `AVG` — but aggregate calls participate in larger
  scalar expressions, so `SUM(...) / COUNT(...)` computes it.
- `input.<field>` is valid **only** inside aggregate arguments; a bare `customer = input.customer`
  is rejected. Aggregates cannot be nested.
- `WIDTH` equal to `STEP` makes the windows tumbling (no overlap); a smaller `STEP` slides them.
  `STEP` may never exceed `WIDTH`.
- Every required field of `order_window` must be assigned, and every `SET` target must exist in
  the schema.

## Watch The Summaries

```bash
nervix-cli --domain quickstart subscribe window_watch order_windows
```

Produce three `acme` orders. When the third arrives, one summary record for the `acme` branch
appears — `order_count` of `3`, with the totals. Interleave `globex` orders and you will see that
each customer's window fills separately: the branch keeps window state per customer, exactly like
the deduplicator's history. If a window stays partially filled, the `30s` duration bound closes it.

Duration bounds follow record timestamp watermarks — see
[Ingestion Timestamps](domains-and-time.md#ingestion-timestamps).

Next: produce records without any input at all in [Generators](./quickstart-generators.md).
