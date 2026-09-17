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
  avg_amount F64
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
        avg_amount = AVG(input.amount)
  ON MESSAGE ERROR LOG;

COMMIT;
```

- The aggregate functions are `COUNT`, `COUNT_IF`, `SUM`, `AVG`, `MIN`, `MAX`, `FIRST`, `LAST`,
  `ARG_MIN`, `ARG_MAX`, `BOOL_AND`, `BOOL_OR`, `VAR_POP`, `VAR_SAMP`, `STDDEV_POP`, `STDDEV_SAMP`,
  `COVAR_POP`, `COVAR_SAMP`, `CORR`, and `PERCENTILE_LINEAR_HISTOGRAM`. Aggregate calls also
  participate in larger scalar expressions. See
  [Window aggregate functions](processors.md#window-aggregate-functions) for their types, null
  handling, and numerical behavior.
- A null argument contributes nothing to an aggregate. An aggregate that can be null, such as
  `VAR_SAMP` or any aggregate over an `OPTIONAL` field, needs an `OPTIONAL` output field or a
  `COALESCE`.
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

Duration bounds start from record timestamp watermarks and become due when a later watermark or
the bound domain clock reaches the target. A paced domain's `TIME RATE` changes the real wait for a
partially filled window while preserving the records' external event timestamps. The output keeps
the window's minimum low watermark and records the emission snapshot as its high watermark. See
[Ingestion Timestamps](domains-and-time.md#ingestion-timestamps).

Next: produce records without any input at all in [Generators](./quickstart-generators.md).
