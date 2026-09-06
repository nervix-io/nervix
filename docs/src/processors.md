# Processors

Processors consume relay records and create one or more route-local outputs. Relay names identify
graph edges; expressions use language-defined scopes instead of relay-qualified fields.

## Shared input and branch contracts

Ordinary multi-source processors require every `FROM` relay to reference the same declared schema
Model. Merely having structurally equal schemas is not enough. The relays must also use the same
exact named branch, or all be unbranched.

Relay inputs may optionally collect incoming Arrow batches before node execution:

```nspl,ignore
FROM <relay> [WHERE <expr>], ...
COLLECT FOR <duration> [MAX BATCH SIZE <bytes>]
```

Without `COLLECT FOR`, each relay batch is executed immediately and Nervix creates no additional
input buffer. With it, the duration starts when data enters an empty input collector. The node
executes the accumulated batch when that timer expires or the optional maximum size is reached.
Collection is independent for each source relay and concrete branch, and occurs before
source-specific `WHERE`, node-wide `FILTER WHERE`, and node-specific execution.

This clause is available on junctions, deduplicators, reorderers, window processors, inferencers,
WASM processors, and reingestors. Correlators configure it independently after each complete
`LEFT FROM` or `RIGHT FROM` relay list. Ingestors cannot use it because they do not consume relays;
generators are scheduled from materialized state and have no `FROM` relay list.

Branch-preserving processors declare one node-wide contract:

```nspl,ignore
BRANCHED BY <branch>
```

or:

```nspl,ignore
UNBRANCHED
```

Every input and output relay must match it exactly. State, scheduling, buffers, and materialized
views are instantiated independently for every concrete branch. Only ingestors and reingestors
construct branch keys.

## Filters and construction

Transforming routes use the [working-message model](working-message.md). The scopes below describe
fixed points in that construction timeline.

Processing order is:

1. A source-specific `FROM ... WHERE` predicate.
2. Node-wide `FILTER WHERE`, when the node supports it.
3. Node-specific work such as deduplication, inference, or ordering.
4. Independent route construction and route `WHERE` evaluation.

A transforming route starts empty and may use `INHERIT` and ordered `SET`:

```nspl,ignore
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
`message.field` and a bare RHS field read the
[working message](working-message.md). Route `WHERE` reads the finalized route output.

Set-only routes reject `INHERIT`. All required fields must be assigned; omitted optional fields
finalize as typed nulls. Generated inferencer and WASM values are immutable read sources and are
visible independently to every route. They never initialize route outputs automatically.

Every flush-based processor route declares `FLUSH EACH <duration> MAX BATCH SIZE <bytes>` or
`FLUSH IMMEDIATE`. The [NSPL Overview](nspl-overview.md) defines the system-owned 100 µs minimum
batching window and its forced-flush exceptions. A route using `ON MESSAGE ERROR SEND TO` buffers
its error records independently and emits them on that route's same interval or maximum batch-size
boundary.

## Materialized relay state

For mechanism selection, including differently keyed data, see
[Choosing An Enrichment Mechanism](lookups.md#choosing-an-enrichment-mechanism).

Normal processors declare ordered node-wide dependencies after their branch declaration:

```nspl,ignore
USING MATERIALIZED STATE profiles REQUIRED WAIT
USING MATERIALIZED STATE rules REQUIRED SKIP
USING MATERIALIZED STATE preferences DEFAULT {
  theme = "system",
  alerts = true
}
```

State is read as `relay_state.<relay>.<field>`. Each relay must be materialized, in the same domain,
and exactly branch-compatible. Duplicate dependencies are invalid.

Dependencies execute in written order. Real state binds immediately; `DEFAULT` binds a typed
constant record; `REQUIRED SKIP` suppresses the input successfully; and `REQUIRED WAIT` retains the
message in memory, keeps its acknowledgement open, and applies backpressure. When state arrives,
resolution restarts at the first declaration. Whole-branch eviction drops both state and suspended
work.

`REQUIRED SKIP` and `REQUIRED WAIT` gate a node's input. Dependencies resolve once per batch, and
every output route of that batch reads the same resolved values, including the constants bound by
`DEFAULT`. Routes never observe a partially resolved or per-route view of state.

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

`DESCRIBE JUNCTION <junction>` reports the stored inputs, branch and attachment contracts, route
summaries, scheduled owner and replicas, and local incoming and outgoing edge metrics when those
metrics exist.

### Altering Junctions

`ALTER JUNCTION` accepts comma-separated operations and applies them in written order. Input,
materialized-dependency, and route order are preserved:

```nspl
ALTER JUNCTION route_notifications
  ADD FROM priority_notifications WHERE input.active,
  SET COLLECT FOR 25ms MAX BATCH SIZE 1MiB,
  SET FILTER WHERE input.amount >= 10,
  ALTER MATERIALIZED STATE profiles SET REQUIRED WAIT,
  REPLACE ROUTE TO accepted
    INHERIT ALL
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG,
  SET DETACHED;
```

Input operations are `ADD FROM`, `DROP FROM`, and `ALTER FROM ... SET|DROP WHERE`. A junction must
retain at least one input. `DROP FROM` also removes that input's `WHERE`. Collection, node filter,
attachment mode, and branch selection each have `SET` forms; collection and filtering also have
`DROP` forms.

Materialized dependencies support `ADD MATERIALIZED STATE <relay> <policy>`,
`DROP MATERIALIZED STATE <relay>`, and
`ALTER MATERIALIZED STATE <relay> SET <policy>`. Adding appends; altering keeps the existing order
position; duplicate dependencies are invalid.

Routes support `ADD ROUTE TO <relay> <full route body>`, `DROP ROUTE TO <relay>`, and
`REPLACE ROUTE TO <relay> <full route body>`. Adding appends and replacing keeps the route's index.
Multiple routes may target the same relay, so drop and replace require that their target identify
exactly one route. A junction must retain at least one route.

Filter, per-input `WHERE`, construction, flush, collect, and same-target message-error policy
changes are classified dynamic and hot-applied from the published schedule. Existing input
collectors, buffered route output, pending materialized-state work, subscriptions, and branch-local
processor state remain in place. The runtime invalidates only compiled expression programs whose
source changed; a flush-policy update also forces an immediate convergence pass so buffered output
is evaluated against the new policy without waiting for another input.

Input/route topology, attachment, branching, dependencies, and changed error-route targets are
classified entity pause. Nervix gates their source relays across the cluster, drains affected
relay owner buffers, dispatch slots, and node work, and swaps only the altered junction task. Pending materialized-state
work and branch presence residue are handed to the replacement before it resumes. Other nodes in
the domain continue to run; sibling consumers of a gated relay can experience bounded
backpressure until the gate is released.

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

### Altering Deduplicators

`ALTER DEDUPLICATOR` applies comma-separated operations in written order:

```nspl
ALTER DEDUPLICATOR unique_notifications
  SET DEDUPLICATE ON input.tenant, input.external_id,
  SET MAX TIME 30m,
  SET FILTER WHERE input.active,
  REPLACE ROUTE TO unique_events
    INHERIT ALL
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG;
```

The deduplicator-specific operations are `SET DEDUPLICATE ON <expr>, ...` and
`SET MAX TIME <duration>`. It also supports the junction-style input, collection, filter,
attachment, branching, materialized-state, and route operations described above. A processor must
retain at least one input and one route. Duplicate materialized dependencies are rejected, and
drop/replace route operations require a unique target when multiple routes use the same relay.

Changing only `MAX TIME`, filters, per-input `WHERE`, collection, route construction/flush, or a
same-target message-error policy is dynamic. Changing the deduplication expressions is an
entity-pause operation: Nervix gates and drains the input relays, stops the old task, purges its
branch-local and persisted deduplication keyspace, then starts the replacement. Input/route
topology, attachment, branching, dependencies, and changed error-route targets also use entity
pause.

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

### Altering Reorderers

`ALTER REORDERER` uses the same ordered common processor operations, plus `SET BY` and
`SET MAX TIME`:

```nspl
ALTER REORDERER ordered_notifications
  SET BY input.priority, input.occurred_at, input.sequence,
  SET MAX TIME 10s,
  SET COLLECT FOR 25ms MAX BATCH SIZE 1MiB,
  SET ATTACHED;
```

`SET BY <expr>, ...` replaces the complete ordering expression list. `SET MAX TIME <duration>`
changes only the maximum holding time. The shared input, filter, route, materialized-state,
attachment, and branching operations have the same validation and ordering semantics as
deduplicators and junctions.

`MAX TIME` and the shared expression/configuration-only aspects are dynamic. Changing `BY` uses
entity pause: the old ordering buffers are force-flushed while the input relays are gated, then the
node task is replaced with the new ordering program. Structural shared operations also use entity
pause.

## Window processor

Windows are set-only. Aggregates appear directly in route `SET`; there is no `AGGREGATE` clause.
`input.field` is valid only inside aggregate arguments, and aggregates cannot be nested:

```nspl
CREATE WINDOW PROCESSOR latency_windows
  FROM latencies
  FILTER WHERE input.latency >= 0
  WIDTH 5m DURATION
  STEP 1m DURATION
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
    "features" DENSE TENSOR<F32>[2] = input.features
  }
  OUTPUT SCHEMA {
    "score" DENSE TENSOR<F32>[1]
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
  MAX FUEL 1000000000
  MAX MEMORY 64MiB
  BRANCHED BY by_tenant
  TO normalized_events
    SET tenant = tenant,
        normalized = normalized
    WHERE output.normalized != ""
    ON MESSAGE ERROR LOG
  ON GLOBAL ERROR LOG;
```

Generated guest state is immutable across routes. WASM processors do not declare `FLUSH`; guest
output and guest-requested timeouts own emission cadence. `MAX FUEL` and `MAX MEMORY` are both
required, in that order immediately after `FILE`. Fuel bounds one logical guest operation, while
memory bounds the branch instance's Wasmtime linear memory. See
[WASM Processor Guests](wasm-processor-guests.md#execution-limits) for exact accounting and
failure behavior.

## Correlator

Correlators use explicit sides and have no default input scope:

```nspl
CREATE CORRELATOR correlate_orders
  LEFT FROM orders WHERE left.active
  COLLECT FOR 10ms
  RIGHT FROM payments WHERE right.approved
  COLLECT FOR 10ms MAX BATCH SIZE 1MiB
  CORRELATE WHERE left.order_id = right.order_id
  MATCH EARLIEST
  MAX TIME 5m
  ON CORRELATION TIMEOUT DROP, DROP
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
key. The reingestor resolves the outgoing branch before buffering the route, so each concrete
outgoing branch has an independent flush interval and size boundary. Downstream branch execution
receives the completed Arrow batch and does not apply a second flush policy.

### Altering Reingestors

`ALTER REINGESTOR` applies the shared input, collection, filter, attachment, materialized-state,
and route operations in written order:

```nspl
ALTER REINGESTOR repartition_events
  ADD FROM priority_events WHERE input.active,
  SET COLLECT FOR 25ms MAX BATCH SIZE 1MiB,
  SET FILTER WHERE input.amount > 0,
  SET DETACHED,
  REPLACE ROUTE TO by_user_events
    INHERIT ALL
    BRANCHED BY by_user
    SET tenant = message.tenant,
        user_id = message.user_id
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG;
```

The input, collection, filter, attachment, and materialized-state forms are the same as for
junctions. Route add/replace bodies include the reingestor's required per-route `BRANCHED BY ...`
or `UNBRANCHED` construction. Reingestors do not have a node-wide branching operation.

Every reingestor change uses entity pause because inputs, route construction, attachment, and
dependency changes affect its relay consumers or branch-entrypoint wiring. Nervix gates both old
and desired input relays, force-flushes collected input, drains node work, stops only the affected
reingestor tasks, rebuilds their branch entrypoints, and reconnects their relay consumers. Other
domain nodes continue to run.

## Generator

Generators run from a materialized relay on a domain-clock cadence. Their routes are set-only:

```nspl
CREATE GENERATOR synth_notifications
  USING MATERIALIZED STATE notifications
  EACH 100ms
  UNBRANCHED
  TO generated_notifications
    SET user_id = relay_state.notifications.user_id,
        amount = relay_state.notifications.amount
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG;
```

### Altering Generators

`ALTER GENERATOR` supports `SET MATERIALIZED STATE <relay>`, `SET EACH <duration>`,
`SET BRANCHED BY <branch>`, `SET UNBRANCHED`, and the ordered `ADD ROUTE`, `DROP ROUTE`, and
`REPLACE ROUTE` operations:

```nspl
ALTER GENERATOR synth_notifications
  SET EACH 250ms,
  REPLACE ROUTE TO generated_notifications
    SET user_id = relay_state.notifications.user_id,
        amount = relay_state.notifications.amount
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG;
```

Generator route bodies remain set-only and must contain at least one `SET` assignment. Drop and
replace require a unique target when duplicate target relays exist, and the generator must retain
at least one route.

Every generator change uses entity pause. Nervix gates its old and desired materialized source
relays, lets the old timed task force-flush pending route output, waits until that task reports
quiescent, then replaces only that generator task from the published schedule. The gate has a
deadline expiry backstop, so a failed control-plane operation cannot leave generation wedged.

## Message errors

`ON MESSAGE ERROR` terminates each route. `SEND TO` constructs an error relay record with ordered
`SET` assignments:

```nspl,ignore
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
