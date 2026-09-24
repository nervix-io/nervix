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
source-specific `WHERE`, node-wide `FILTER WHERE`, and node-specific execution. The duration is a
domain-logical wait that begins only when an empty collector receives data.

This clause is available on junctions, deduplicators, reorderers, window processors, inferencers,
WASM processors, and reingestors. Correlators configure it independently after each complete
`LEFT FROM` or `RIGHT FROM` relay list. Ingestors cannot use it because they do not consume relays;
generators are scheduled from materialized state and have no `FROM` relay list.

Each accepted input batch receives one domain execution snapshot. Source predicates, the node
filter, construction, keys, aggregate expressions, route predicates, message-error construction,
and any Roto or WASM call made for that execution all see that same instant. A batch released from
`COLLECT FOR` starts a new execution with a fresh snapshot; the collection deadline itself remains
bound to the source relay, concrete branch, domain, and `START` generation.

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
batching window and its forced-flush exceptions. `FLUSH EACH` is domain-logical, while the Immediate
minimum is physical; both start on the empty-to-buffered transition and remain independent for
each concrete branch. A route using `ON MESSAGE ERROR SEND TO` buffers its error records
independently and emits them on that route's same interval or maximum batch-size boundary.

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
  MAX STATE SIZE 1MiB
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

`MAX STATE SIZE <bytes>` caps the live state of each concrete branch. It charges retained Arrow
input and argument allocations, row handles, branch keys, and the reserved capacity of every
sketch in every active pane. A row that would exceed the cap is rejected through the route's
message-error policy before it changes the window. Sketches require this clause, duration `WIDTH`
and `STEP`, and a bounded `MAX INSTANCES ... EVICT LRU` branch when branched. Thus the configured
worst-case live sketch state across branches is bounded by `MAX STATE SIZE * MAX INSTANCES`;
unbranched windows have one state. A sketch's pane size is the greatest common divisor of its
width and step, aligned to the Unix epoch. Each pane includes its starting timestamp and excludes
the next pane's starting timestamp. Stepping removes records strictly before the step cutoff;
records exactly at that cutoff remain. Panes are merged only from rows still in the active
window and are rebuilt after stepping. Published branch snapshots carry retained rows and restore
the same sketches when ownership moves or a node recovers.

In a window route, `COUNT`, `SUM`, `FIRST`, and `LAST` always name window aggregates. The
[array and vector functions](filter-map-functions.md#array-and-vector-functions) with the same names
apply to one `ARRAY` or `VEC` value everywhere else.

### Window aggregate functions

Every aggregate reads per-row arguments from the rows the window retains when it emits. Aggregate
names are case-insensitive, and argument and result types are exact and checked when the processor
is applied.

| Function | Arguments | Returns | Typed null when |
| --- | --- | --- | --- |
| `COUNT(value)` | any | `I64` | never; counts every retained row |
| `COUNT_IF(condition)` | `BOOL` | `I64` | never; counts rows whose condition is true |
| `BOOL_AND(condition)` | `BOOL` | `BOOL` | no row has a condition |
| `BOOL_OR(condition)` | `BOOL` | `BOOL` | no row has a condition |
| `SUM(value)` | numeric | the argument's type | no row has a value |
| `AVG(value)` | numeric | `F64` | no row has a value |
| `MIN(value)`, `MAX(value)` | numeric, `BOOL`, `STRING`, or `DATETIME` | the argument's type | no row has a value |
| `FIRST(value)`, `LAST(value)` | any | the argument's type | no row has a value |
| `ARG_MIN(value, key)`, `ARG_MAX(value, key)` | any value; a numeric, `BOOL`, `STRING`, or `DATETIME` key | the value's type | no row has both a value and a key |
| `VAR_POP(value)`, `STDDEV_POP(value)` | numeric | `F64` | no row has a value |
| `VAR_SAMP(value)`, `STDDEV_SAMP(value)` | numeric | `F64` | fewer than two rows have a value |
| `COVAR_POP(first, second)` | numeric, numeric | `F64` | no row has both values |
| `COVAR_SAMP(first, second)` | numeric, numeric | `F64` | fewer than two rows have both values |
| `CORR(first, second)` | numeric, numeric | `F64` | fewer than two rows have both values, or either variable is constant across them |
| `PERCENTILE_LINEAR_HISTOGRAM(value, percentile, buckets, min, max, delay)` | numeric value, then constants | `F64` | the histogram counts no value |
| `APPROX_COUNT_DISTINCT(value, precision)` | numeric, `BOOL`, `STRING`, or `DATETIME`; constant precision 4–16 | `I64` | never; zero when no row contributes |
| `APPROX_QUANTILE(value, percentile, capacity)` | numeric; constant percentile 0–100 and capacity 32–4096 | `F64` | no row has a value |
| `APPROX_TOP_K(value, k, capacity)` | numeric, `BOOL`, `STRING`, or `DATETIME`; constants `1 <= k <= capacity <= 4096` | `VEC<value type>` | never; empty when no row contributes |

The sketches ignore nulls and refuse non-finite floating-point values as per-message errors.
Distinct uses HyperLogLog registers with a stable type-tagged BLAKE3 key; its approximate relative
standard error is about `1.04 / sqrt(2^precision)`. Quantile uses a bounded t-digest with at most
`capacity` centroids, with smaller centroids near the tails. It returns an interpolated value at
the requested percentile; t-digest has no distribution-independent worst-case rank bound, so
accuracy depends on the distribution and capacity. Top-k uses Misra-Gries frequency candidates
with at most `capacity` keys and returns up to `k` values ordered by estimated frequency, then by
stable key bytes to break ties. Its counts are internal;
values near the frequency cutoff may differ from an exact top-k. Any value occurring more than
`N / (capacity + 1)` times in the active window remains a candidate. Each pane's sketch is mergeable;
expired rows never contribute to later results. The sketches are deterministic for the same
ordered inputs, pane layout, and configuration.

**Nulls.** A row contributes to an aggregate only when every argument that aggregate reads is
present, so a null argument contributes nothing. `COUNT` is the exception: it counts every retained
row whatever its argument holds, so `SUM(input.amount) / COUNT(input.amount)` is not the mean of an
optional field; use `AVG`. A window emits only while it retains at least one row, so an aggregate
whose arguments are all required always has a value, except the sample statistics and `CORR`,
which can be undefined in any window. Assign an aggregate that can be null to an `OPTIONAL` field or
give it a value with `COALESCE`; assigning it to a required field is rejected when the processor is
applied.

**Order and ties.** `MIN`, `MAX`, `ARG_MIN`, and `ARG_MAX` return the earliest admitted row among
rows with equal keys. `FIRST` and `LAST` order rows by their ingestion low watermark, then by
admission. `BOOL` orders `false` before `true`, `STRING` orders by bytes, and floating-point keys
order NaN above every other value and treat both zeros as equal.

**Population and sample.** `VAR_POP`, `STDDEV_POP`, and `COVAR_POP` divide by the number of
contributing rows `n`; `VAR_SAMP`, `STDDEV_SAMP`, and `COVAR_SAMP` divide by `n - 1`. Standard
deviations are the square roots of the matching variances. `CORR` is the Pearson correlation, kept
within `[-1, 1]`.

**Numerical behavior.** `COUNT`, `COUNT_IF`, `BOOL_AND`, `BOOL_OR`, and `SUM` over integers are
exact, and an integer `SUM` that does not fit its argument's type is an error when the window
emits. `SUM` over floating-point values carries the rounding error of every addition beside the
running total. `AVG`, the variances, standard deviations, covariances, and `CORR` convert each
argument to the nearest `F64` and keep centered moments, so a variance is never the difference of
two large sums of squares. Stepping a window never subtracts a floating-point value from a
statistic: the statistic of the rows that remain is rebuilt from the rows themselves, so a value
that left the window, however large, leaves no rounding behind. A NaN or infinite floating-point
argument to `SUM`, `AVG`, a variance, standard deviation, covariance, `CORR`, or
`PERCENTILE_LINEAR_HISTOGRAM` is a per-message error for its row, which the window does not admit;
a statistic or floating-point sum that overflows `F64` is an error when the window emits. An error
at emission fails the acknowledgements of every retained row and clears the window.

Aggregates that can be answered from one structure over the same arguments share it: `AVG`, the
variances, and the standard deviations of one argument share a `moments` structure; the covariances
and `CORR` of one argument pair share `co_moments`; `COUNT_IF`, `BOOL_AND`, and `BOOL_OR` share a
`truth_counter`; `ARG_MIN` and `ARG_MAX` share `arg_extremes`; `MIN` and `MAX` share `extremes`;
`FIRST` and `LAST` share a `sequence`. `DESCRIBE WINDOW PROCESSOR` lists every structure with the
functions it serves and the arguments it reads.

A duration width begins at the first retained record's low watermark and becomes due when an input
watermark or the bound domain clock reaches that logical target. A paced `TIME RATE` therefore
changes how soon a partially filled window becomes due in real time without changing its source
event timestamps. Each concrete branch owns independent entries, aggregate state, and deadlines.
The emitted record keeps the minimum input low watermark and uses the emission execution snapshot
as its high watermark.

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

`USING RESOURCE` requires `VERSION <n>` or `VERSION LATEST`. The inferencer loads the model file
from the one version it stores, so a later upload of the resource does not change the scoring
model; see [Versioning](resources.md#versioning).

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
  ON REJECTED STATE PRESERVE
  ON GLOBAL ERROR LOG;
```

`USING RESOURCE` requires `VERSION <n>` or `VERSION LATEST`, and the processor compiles the module
from the one version it stores. `SHOW CREATE WASM PROCESSOR` renders that version and
`DESCRIBE WASM PROCESSOR` reports it.

Generated guest state is immutable across routes. WASM processors do not declare `FLUSH`; guest
output and guest-requested timeouts own emission cadence. `MAX FUEL` and `MAX MEMORY` are both
required, in that order immediately after `FILE`. Fuel bounds one logical guest operation, while
memory bounds the branch instance's Wasmtime linear memory. See
[WASM Processor Guests](wasm-processor-guests.md#execution-limits) for exact accounting and
failure behavior.

Guest initialization, input processing, requested-timeout callbacks, quiesce flushes, and state
save, load, or reset each receive the snapshot selected for that operation. The guest cannot ask
the engine for wall time or execute without an explicit snapshot.

`ON REJECTED STATE` decides what happens when a recreated guest refuses the snapshot Nervix hands
it. It is optional and defaults to `PRESERVE`, which keeps the refused snapshot and reports the
refusal; `RESET` opts the processor in to replacing that branch's state lifetime once. Only the
guest's own verdict on the saved bytes reaches this policy, so no module, limit, storage or
replication failure can erase computation state. See
[Recovering A Rejected Snapshot](wasm-processor-guests.md#recovering-a-rejected-snapshot).

A WASM processor acknowledges an input only after the guest-state checkpoint that covers it is on
the stable storage of the branch's owner and of every replica the schedule assigns the processor.
A checkpoint that cannot get there negatively acknowledges what it covers and recreates the branch's
guest from its last completed checkpoint. `DESCRIBE WASM PROCESSOR` reports how many of the answering
node's branch checkpoints are awaiting local storage, awaiting replicas, or failed. See
[Checkpoints And Acknowledgements](wasm-processor-guests.md#checkpoints-and-acknowledgements).

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

The first occurrence is immediate. Later occurrences remain anchored to the declared `EACH`
schedule. If reading or generating spans multiple periods, Nervix generates once for the newest
due occurrence and continues at the first future boundary. Route expressions use a fresh domain
execution snapshot taken after the materialized-state read.

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
