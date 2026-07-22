# Processors

Processors consume relay records and create one or more route-local outputs. Relay names identify
graph edges; expressions use language-defined scopes instead of relay-qualified fields.

## Shared input and branch contracts

Ordinary multi-source processors require every `FROM` relay to reference the same declared schema
Model. Merely having structurally equal schemas is not enough. The relays must also use the same
exact named branch, or all be unbranched.

Branch-preserving processors declare one node-wide contract:

```nspl
BRANCHED BY <branch>
```

or:

```nspl
UNBRANCHED
```

Every input and output relay must match it exactly. State, scheduling, buffers, and materialized
views are instantiated independently for every concrete branch. Only ingestors and reingestors
construct branch keys.

## Filters and construction

Processing order is:

1. A source-specific `FROM ... WHERE` predicate.
2. Node-wide `FILTER WHERE`, when the node supports it.
3. Node-specific work such as deduplication, inference, or ordering.
4. Independent route construction and route `WHERE` evaluation.

A transforming route starts empty and may use `INHERIT` and ordered `SET`:

```nspl
TO projected_notifications
  INHERIT tenant, user_id, amount
  SET amount = amount + 1,
      amount = amount * 2,
      normalized = lower(trim(input.raw))
  WHERE output.amount > 10
  FLUSH IMMEDIATE
  ON MESSAGE ERROR LOG
```

`INHERIT ALL`, `INHERIT ALL EXCEPT ...`, and explicit field lists require exact type and nullability
matches. Sensitive values may be promoted but not downgraded. Explicit inheritance leakage is
written `INHERIT password LEAK SENSITIVE`.

Assignments run left to right. `output.field` reads only an already initialized output field.
During transforming construction, `message.field` and a bare RHS field read the working output and
fall back to the exact-compatible `input.field` only while the output field is uninitialized.
After finalization, route `WHERE` sees the finalized output and performs no fallback.

Set-only routes reject `INHERIT`. All required fields must be assigned; omitted optional fields
finalize as typed nulls. Generated inferencer and WASM values are immutable read sources and are
visible independently to every route. They never initialize route outputs automatically.

## Materialized relay state

Normal processors declare ordered node-wide dependencies after their branch declaration:

```nspl
USING MATERIALIZED STATE profiles REQUIRED WAIT
USING MATERIALIZED STATE rules REQUIRED SKIP
USING MATERIALIZED STATE preferences DEFAULT {
  "theme" = "system",
  "alerts" = true
}
```

State is read as `relay_state.<relay>.<field>`. Each relay must be materialized, in the same domain,
and exactly branch-compatible. Duplicate dependencies are invalid.

Dependencies execute in written order. Real state binds immediately; `DEFAULT` binds a typed
constant record; `REQUIRED SKIP` suppresses the input successfully; and `REQUIRED WAIT` retains the
message in memory, keeps its acknowledgement open, and applies backpressure. When state arrives,
resolution restarts at the first declaration. Whole-branch eviction drops both state and suspended
work.

Defaults must initialize every required field. Omitted optional fields become typed nulls. Default
expressions cannot contain field reads, side effects, or nondeterministic calls.

## Junction

Junctions perform transforming fan-out:

```nspl
CREATE JUNCTION route_notifications
  FROM notifications WHERE input.active
  FILTER WHERE input.amount > 0
  BRANCHED BY by_tenant
  USING MATERIALIZED STATE profiles REQUIRED SKIP
  TO accepted
    INHERIT ALL
    WHERE relay_state.profiles.enabled
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG
  TO audit
    INHERIT ALL
    FLUSH EACH 1s MAX BATCH SIZE 1MiB
    ON MESSAGE ERROR LOG;
```

## Deduplicator

Deduplication expressions are structured and evaluated in source order:

```nspl
CREATE DEDUPLICATOR unique_notifications
  FROM notifications
  FILTER WHERE input.active
  DEDUPLICATE ON input.tenant, input.event_id
  MAX TIME 10m
  BRANCHED BY by_tenant
  TO unique_events
    INHERIT ALL
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG;
```

Deduplication state is branch-local. Duplicate details are logged at `debug` or `trace`, never at
`info`.

## Reorderer

```nspl
CREATE REORDERER ordered_notifications
  FROM notifications
  BY input.occurred_at, input.sequence
  MAX TIME 30s
  BRANCHED BY by_tenant
  TO ordered_events
    INHERIT ALL
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG;
```

Ordering buffers and maximum-time release are independent per concrete branch.

## Window processor

Windows are set-only. Aggregates appear directly in route `SET`; there is no `AGGREGATE` clause.
`input.field` is valid only inside aggregate arguments, and aggregates cannot be nested:

```nspl
CREATE WINDOW PROCESSOR latency_windows
  FROM latencies
  FILTER WHERE input.latency >= 0
  WIDTH 5m
  STEP 1m
  BRANCHED BY by_tenant
  TO latency_summary
    SET count = COUNT(input.latency),
        count_plus_one = COUNT(input.latency) + 1,
        minimum = MIN(input.latency),
        maximum = MAX(input.latency),
        tenant = branch.tenant
    WHERE output.count > 0
    ON MESSAGE ERROR LOG;
```

Aggregate calls may participate in larger scalar expressions and may combine with constants,
initialized `output`, `branch`, and declared `relay_state` values. Route `WHERE` cannot read live
input rows. Windows use `WIDTH` and `STEP`, never `FLUSH`.

## Inferencer

Inferencers keep the explicit tensor mapping surface. `INPUTS` expressions may read `input`; route
construction cannot. Routes read immutable generated model fields, declared materialized state,
and the branch:

```nspl
CREATE INFERENCER score_events
  FROM features
  USING RESOURCE scoring VERSION 1
  FILE "score.onnx"
  INPUTS {
    features DENSE TENSOR<F32>[2] = input.features
  }
  OUTPUT SCHEMA {
    score DENSE TENSOR<F32>[1]
  }
  BRANCHED BY by_tenant
  TO scores
    SET tenant = branch.tenant,
        score = score
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG;
```

Every required route-output field is explicit. The source input is not implicitly inherited.

## WASM processor

WASM routes are also set-only and execute only when the guest returns actual output data:

```nspl
CREATE WASM PROCESSOR normalize_events
  FROM events
  USING RESOURCE normalizer VERSION 1
  FILE "processor.wasm"
  BRANCHED BY by_tenant
  TO normalized_events
    SET tenant = tenant,
        normalized = normalized
    WHERE output.normalized != ""
    ON MESSAGE ERROR LOG
  ON GLOBAL ERROR LOG;
```

Generated guest state is immutable across routes. WASM processors do not declare `FLUSH`; guest
output and guest-requested timeouts own emission cadence.

## Correlator

Correlators use explicit sides and have no default input scope:

```nspl
CREATE CORRELATOR correlate_orders
  LEFT FROM orders WHERE left.active
  RIGHT FROM payments WHERE right.approved
  CORRELATE WHERE left.order_id = right.order_id
  MATCH EARLIEST
  MAX TIME 5m
  ON CORRELATION TIMEOUT IGNORE, IGNORE
  BRANCHED BY by_tenant
  TO paid_orders
    SET order_id = left.order_id,
        amount = right.amount,
        label = concat("paid:", output.amount AS STRING)
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG;
```

Relays on each side share one declared schema; left and right schemas may differ. Correlators reject
`FILTER WHERE`, `INHERIT`, bare RHS field reads, `input`, and a separate `OUTPUT` block.
Correlations occur only within one concrete branch.

## Reingestor

Reingestors are branch-boundary transforming nodes. Each route preserves the incoming exact branch,
constructs another branch, or becomes unbranched:

```nspl
CREATE REINGESTOR repartition_events
  FROM events
  USING MATERIALIZED STATE profiles REQUIRED WAIT
  TO by_user_events
    INHERIT ALL
    BRANCHED BY by_user
    SET tenant = message.tenant,
        user_id = message.user_id
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG;
```

When the outgoing branch name equals the incoming branch, no branch `SET` is allowed and the key is
preserved. State lookup always uses the incoming branch, never a partially constructed outgoing
key.

## Message errors

`ON MESSAGE ERROR` terminates each route. `SEND TO` constructs an error relay record with ordered
`SET` assignments:

```nspl
ON MESSAGE ERROR SEND TO processing_errors
SET error_reference = error.reference,
    error_code = error.code,
    operation = error.operation,
    source_id = input.id,
    attempted_total = partial_output.total
```

`error` is structured; `partial_output` is an all-optional view of the failed route output. Eligible
handlers may also read the original `input` (or correlator `left` and `right`) and the exact
`relay_state` snapshot. Error routes preserve the branch in which the failed operation executed and
never construct a new key. Error-route assignments run through the same typed expression VM as
ordinary `SET`, so deterministic scalar functions, casts, unary expressions, binary expressions,
and ordered reads of earlier error-record assignments are supported. Window aggregates and
side-effect functions are not available in error construction.
