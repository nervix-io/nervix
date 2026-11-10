# Processors

*Plexus, the Nervix graph definition language.*

Processors consume relay records and create one or more route-local outputs. Relay names identify
graph edges; expressions use language-defined scopes instead of relay-qualified fields.

## Shared input and branch contracts

Ordinary multi-source processors require every `FROM` relay to reference the same declared schema
Model. Merely having structurally equal schemas is not enough. The relays must also use the same
exact named branch, or all be unbranched.

Relay inputs may optionally collect incoming Arrow batches before node execution:

```plexus,ignore
FROM <relay> [WHERE <expr>], ...
COLLECT FOR <duration> [MAX BATCH SIZE <bytes>]
```

The optional `ATTACHED | DETACHED` clause follows the complete input list and its optional
`COLLECT FOR`, before the node filter, resource binding, branch selection or output routes. On a
correlator it follows the complete `RIGHT FROM` list and its collection policy. Omitting the clause
selects `ATTACHED`; canonical Plexus and `SHOW CREATE` always write it explicitly in that body
position. The supported `ALTER ... SET ATTACHED | SET DETACHED` operations change the same mode.
These operations are available on junctions, deduplicators, reorderers and reingestors. Window
processors, correlators, inferencers and WASM processors have no `ALTER` statement. Their heads
still select the node kind, and acknowledgement remains a body property; see the
[statement-head contract](architecture.md#statement-heads-and-body-properties).

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

```plexus,ignore
BRANCHED BY <branch>
```

or:

```plexus,ignore
UNBRANCHED
```

Every input and output relay must match it exactly. State, scheduling, buffers, and materialized
views are instantiated independently for every concrete branch. Only ingestors and reingestors
construct branch keys.

### Partitioned processing

A processor or generator whose named branch declares `PARTITIONS n` runs as one independently
owned unit per partition. A concrete key selects the same partition index throughout the branch.
Each unit has its own supervisor, branch registry, collection and output buffers, retained state,
and state replicas. Branch TTL and `MAX INSTANCES` apply inside each unit's registry; account for
all partitions when sizing a branched graph.

The scheduler prefers the corresponding upstream partition's owner. A materialized dependency
reads the concrete branch from that relay partition's owner, locally when colocated and remotely
otherwise. Each generator enumerates only its own partition's materialized rows.

`SHOW CLUSTER STATUS` and `DESCRIBE` report each partition's owner and replicas. Processor metrics
include `partition`. Failover and drain transfer unit-qualified state; a sibling unit keeps its
supervisor and checkpoint tasks. `RELOCATE` without a selector moves every unit of the selected
node together; it does not select an individual concrete branch.

## Filters and construction

Transforming routes use the [working-message model](working-message.md). The scopes below describe
fixed points in that construction timeline.

Processing order is:

1. A source-specific `FROM ... WHERE` predicate.
2. Node-wide `FILTER WHERE`, when the node supports it.
3. Node-specific work such as deduplication, inference, or ordering.
4. Independent route construction and route `WHERE` evaluation.

A transforming route starts empty and may use `INHERIT` and ordered `SET`:

```plexus,ignore
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
`FLUSH IMMEDIATE`. The [Plexus Overview](plexus-overview.md) defines the system-owned 100 µs minimum
batching window and its forced-flush exceptions. `FLUSH EACH` is domain-logical, while the Immediate
minimum is physical; both start on the empty-to-buffered transition and remain independent for
each concrete branch. A route using `ON MESSAGE ERROR SEND TO` buffers its error records
independently and emits them on that route's same interval or maximum batch-size boundary.

## Materialized relay state

For mechanism selection, including differently keyed data, see
[Choosing An Enrichment Mechanism](lookups.md#choosing-an-enrichment-mechanism).

Normal processors declare ordered node-wide dependencies after their branch declaration:

```plexus,ignore
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

```plexus
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

```plexus
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
processor state remain in place. Before publication, the runtime binds a complete typed plan for
each changed processor against its installed schemas, branch contract, lookups and UDFs. Existing
branches adopt that plan by typed revision identity, and branches that appear later start from the
same plan. Unchanged processors retain the exact prepared programs they already hold. A flush-policy
update also forces an immediate convergence pass so buffered output is evaluated against the new
policy without waiting for another input.

Input/route topology, attachment, branching, dependencies, and changed error-route targets are
classified entity pause. Nervix gates their source relays across the cluster, drains affected
relay owner buffers, dispatch slots, and node work, and swaps only the altered junction task. Pending materialized-state
work and branch presence residue are handed to the replacement before it resumes. Other nodes in
the domain continue to run; sibling consumers of a gated relay can experience bounded
backpressure until the gate is released.

## Deduplicator

Deduplication expressions are structured and evaluated in source order:

```plexus
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

### Inspect Remembered Keys

```plexus
SHOW DEDUPLICATOR unique_notifications STATE
  FOR BRANCH VALUES { tenant = 'alpha' } LIMIT 100 FORMAT JSON;
```

`SHOW DEDUPLICATOR <name> STATE` reads one immutable publication per selected branch through the
[shared inspection envelope](relay.md#inspect-materialized-state). Its `keys` section contains
`key_1` onwards in `DEDUPLICATE ON` order, with each expression's exact type, followed by
`seen_at`, the key's ingestion time. A key expression that reads a sensitive input or branch field
is redacted. Keys are listed oldest first within each branch. Expired keys are excluded using one
domain-clock reading, including when no new input has caused the branch task to prune them.

The ordered `FOR`, `LIMIT ... AFTER` and `FORMAT` clauses, owner routing and typed refusals are
shared with materialized-state inspection. A continuation is refused as `stale_continuation` when
its publication or visible expiry prefix changes. Reading state does not refresh a key's expiry.
During a [partition transition](relay.md#changing-the-partition-count), each concrete branch is read from its holder
until installation and from its claimant afterwards. Retained transfer copies add no duplicate
rows or counts; a branch-catalog change racing the read produces `not_ready`.
`DESCRIBE DEDUPLICATOR` reports `branches:`, `keys:` and `state bytes:` for each execution unit;
bytes include key values and their `seen_at` timestamps. Partitioned units prefix these lines with
the partition. An unavailable owner reports its typed refusal in place of the count.

### Altering Deduplicators

`ALTER DEDUPLICATOR` applies comma-separated operations in written order:

```plexus
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

A [backup](backup-and-restore.md) archives each branch's keyspace with the time every key was
first seen. A restore installs those keys, so a duplicate of an archived message is suppressed and
each key expires `MAX TIME` after its archived first sighting.

## Reorderer

```plexus
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

```plexus
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

### Inspect Held Rows

```plexus
SHOW REORDERER ordered_notifications STATE
  FOR ALL BRANCHES LIMIT 100 FORMAT JSON;
SHOW REORDERER ordered_notifications STATE
  FOR BRANCH VALUES { tenant = 'alpha' } FORMAT TEXT;
```

`SHOW REORDERER <name> STATE` reads the rows the reorderer holds back, as one `pending` section per
execution unit. It takes the same ordered `FOR`, `LIMIT ... AFTER` and `FORMAT` clauses, typed
envelope, redaction, continuation and refusals as
[`SHOW RELAY ... MATERIALIZED STATE`](relay.md#inspect-materialized-state). A row holds the input
relay's fields, then `output`, the route it waits on numbered from `0` in declaration order as
`DESCRIBE REORDERER` numbers its outputs; `order_key_1` onwards, one column per `BY` expression in
the type that expression produces; `received_at`, when the batch holding the row arrived; and
`release_at`, when the route releases what it holds: `MAX TIME` after its first held row arrived,
or the route's `FLUSH EACH` deadline when that comes first. Every route holds its own rows, so a row
waiting on two routes is listed once for each. An order key computed from a sensitive input or
branch field is redacted like the field. Each branch lists its routes in order and, within a route,
the rows in the order they arrived; rows are released in key order regardless.

Each branch task answers the read between two of its batches with a bounded copy of the rows it
holds, so an inspection never sees a partial batch and never changes what the reorderer releases or
in which order. The read counts every held row and its logical Arrow bytes, the bytes the route
charges against `MAX BATCH SIZE`. A continuation names every read branch's lifetime and the events
it handled, so a page resumed after any branch took another batch, released rows or ended is
refused as `stale_continuation`. A branch that is stopping refuses the read as `owner_unavailable`.
A unit that has not yet installed every branch its node held, while it restores them after its
task starts or takes the unit over, refuses the read as `not_ready` instead of reporting the
branches it has not restored as empty.

`DESCRIBE REORDERER` reports, for each execution unit, the concrete branches the reorderer runs and
the rows and bytes they hold, after its placement lines:

```text
branches: 2
rows: 4
state bytes: 2048
```

A partitioned reorderer prefixes each line with its partition, as in `partition 3 rows: 1`. A unit
whose owner cannot be read reports the typed refusal in place of each count, as in
`rows: unavailable (owner_unavailable)`, and a unit still restoring its branches reports
`rows: unavailable (not_ready)`. A stopped domain keeps its placement but runs no unit, so a stopped
reorderer reports every count of every unit as unavailable, and its `SHOW REORDERER ... STATE` is
refused, both as `owner_unavailable`:

```text
branches: unavailable (owner_unavailable)
rows: unavailable (owner_unavailable)
state bytes: unavailable (owner_unavailable)
```

## Window processor

Windows are set-only. Aggregates appear directly in route `SET`; there is no `AGGREGATE` clause.
`input.field` is valid only inside aggregate arguments, and aggregates cannot be nested:

```plexus
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
        tenant = FIRST(input.tenant)
    WHERE output.count > 0
    ON MESSAGE ERROR LOG;
```

Aggregate calls may participate in larger scalar expressions and combine with constants. Route
`WHERE` reads the finalized output and cannot read live input rows. Windows use `WIDTH` and `STEP`,
never `FLUSH`. [Window Aggregates](filter-map-functions.md#window-aggregates) owns every aggregate's
types, null handling, numerical behavior, errors, and sensitivity, and
[Approximate Sketches](filter-map-functions.md#approximate-sketches) owns the accuracy, memory, and
expiry of the sketch aggregates.

`MAX STATE SIZE <bytes>` caps the live state of each concrete branch. It charges retained Arrow
input and argument allocations, row handles, branch keys, and the reserved capacity of every
sketch in every active pane. A row that would exceed the cap, or a row whose aggregate arguments
fail, is refused before it changes the window: the refusal is logged and the row is not
acknowledged. Sketches require this clause, duration `WIDTH` and `STEP`, and a bounded
`MAX INSTANCES ... EVICT LRU` branch when branched. Thus the configured worst-case live sketch state
across branches is bounded by `MAX STATE SIZE * MAX INSTANCES`; unbranched windows have one state.
A sketch's pane size is the greatest common divisor of its width and step, aligned to the Unix
epoch. Each pane includes its starting timestamp and excludes the next pane's starting timestamp.
Stepping removes records strictly before the step cutoff; records exactly at that cutoff remain.
Panes are merged only from rows still in the active window and are rebuilt after stepping.
Published branch snapshots share the retained Arrow input and argument columns, seal them in bounded
sections, and rebuild the same sketches from those columns when ownership moves, a node recovers or
a [backup](backup-and-restore.md) is restored. A window whose retained rows exceed one 8 MiB
snapshot section persists through bounded pieces on quota-owned disk rather than in memory, so the
window a node persists is bounded by its own `WIDTH` and `MAX STATE SIZE` and by the node's snapshot
staging quota rather than by the bulk memory budget. Histogram delayed removals travel in bounded
typed sections beside the columns. A restored window continues only under the archived window model
and branch lifetime; otherwise it starts empty and the restore reports a warning.

Admission reads each consecutive run of retained rows as typed Arrow value slices with their
validity bits. Counts and integer sums are exact across run boundaries. Floating-point sums and
statistics merge each run's compensated or centered result into the branch-local window, so their
last rounded digits can depend on the run and lane grouping. Stepping refolds surviving runs;
restoring a snapshot re-admits its retained rows.

In a window route, `COUNT`, `SUM`, `FIRST`, `LAST`, `MIN`, and `MAX` always name window aggregates.
The [array and vector functions](filter-map-functions.md#array-and-vector-functions) with the same
names apply to one `ARRAY` or `VEC` value everywhere else.

### Inspect Retained Window State

```plexus
SHOW WINDOW PROCESSOR latency_windows STATE FOR ALL BRANCHES LIMIT 100 FORMAT JSON;
```

`SHOW WINDOW PROCESSOR <name> STATE` reads one published generation per selected branch. The
`rows` section contains the retained input fields, followed by `sequence`, `received_at`, and
`argument_0` onwards: the saved argument columns in flattened aggregate-demand order. Input fields
and argument expressions retain their declared sensitivity and are redacted before encoding.
Rows are listed by branch and then sequence. Sealed publications use bounded native decoding under
bulk admission; a read never changes, emits or steps the live window.

Two additional typed sections describe each selected branch, including an empty published window:

- `header`: `publication_revision`, `rows`, nullable `first_sequence`, `next_sequence`, nullable
  `incarnation`, nullable `due_at`, and `state_bytes`. The due deadline is the earlier of the oldest
  retained input's duration-width deadline and any delayed histogram removal. A branch with neither
  has no due deadline. State bytes use the production `MAX STATE SIZE` charge, including shared
  Arrow allocations, row handles, branch keys and reserved sketch panes.
- `accumulators`: `structure`, the demand index starting at zero; `kind`, the VM's storage-kind
  name; and `count`, the number of contributing retained argument rows, including pending delayed
  histogram removals. Sketch contents and internal values are never exposed.

`LIMIT`, continuation offsets and complete row counts count only retained input rows. Headers and
accumulator facts do not advance the row position; they do consume the reply's byte bound and are
repeated on each page. Metadata that cannot fit is refused as `reply_too_large`. Every section uses
the same captured publication, and a changed publication or branch lifetime invalidates its
continuation. Branch selectors, formats and owner routing use the shared inspection contract.
During a [partition transition](relay.md#changing-the-partition-count), the selected partition contributes the branch's
rows, header, accumulator facts and byte charge once, including while its holder retains the
transfer checkpoint. A branch-catalog change racing the read produces `not_ready`.
`DESCRIBE WINDOW PROCESSOR` reports each unit's `branches:`, `rows:` and `state bytes:` without
returning retained rows; the byte count is the sum of the same per-branch charges in the headers.

### Window aggregate functions

The aggregates a window route can compute are `COUNT`, `COUNT_IF`, `BOOL_AND`, `BOOL_OR`, `SUM`,
`AVG`, `MIN`, `MAX`, `FIRST`, `LAST`, `ARG_MIN`, `ARG_MAX`, `VAR_POP`, `VAR_SAMP`, `STDDEV_POP`,
`STDDEV_SAMP`, `COVAR_POP`, `COVAR_SAMP`, `CORR`, `PERCENTILE_LINEAR_HISTOGRAM`,
`APPROX_COUNT_DISTINCT`, `APPROX_QUANTILE`, and `APPROX_TOP_K`. See
[Window Aggregates](filter-map-functions.md#window-aggregates) for each one's arguments, result, and
behavior. `DESCRIBE WINDOW PROCESSOR` lists the shared structures that answer them.

A duration width begins at the first retained record's low watermark and becomes due when an input
watermark or the bound domain clock reaches that logical target. A paced `TIME RATE` therefore
changes how soon a partially filled window becomes due in real time without changing its source
event timestamps. Each concrete branch owns independent entries, aggregate state, and deadlines.
The emitted record keeps the minimum input low watermark and uses the emission execution snapshot
as its high watermark.

## Inferencer

Commands that bind an ONNX model inspect its tensor metadata on the node's bulk workers. Each
inspection reserves a 16 KiB bulk buffer and uses its admitted worker to read an explicitly configured
absolute native-library image sequentially before ONNX initialization and optimization. Its
thirty-second bound covers memory admission, the worker queue, image preparation, model loading,
metadata extraction and session release. Image preparation is advisory; native library discovery
and loading errors remain owned by ONNX Runtime. A full bulk
queue refuses the inspection with its execution cause; releasing that capacity permits the same
valid inferencer to be created. Tensor names, element types and dimensions are checked before the
model is committed.

Inferencers keep the explicit tensor mapping surface. `INPUTS` expressions may read `input`; route
construction cannot. Routes read immutable generated model fields, declared materialized state,
and the branch:

```plexus
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

### State tensors

A model can carry state between rows of one concrete branch. Declare each state input and its
successor output between `INPUTS` and `OUTPUT SCHEMA`:

```plexus
CREATE INFERENCER running_total
  FROM features
  USING RESOURCE scoring VERSION 1
  FILE 'running_sum.onnx'
  INPUTS { 'features' DENSE TENSOR<F32>[2] = input.features }
  STATE {
    'state_in' FROM 'state_out' DENSE TENSOR<F32>[2] INITIAL ZEROS
  }
  OUTPUT SCHEMA { 'score' DENSE TENSOR<F32>[1] }
  BRANCHED BY by_tenant
  TO scores SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
```

`INITIAL ZEROS` initializes every element to zero. `INITIAL [1 AS F32, 10 AS F32]` supplies a
constant two-element tensor instead. The expression must have exactly the declared non-null type
and shape, use deterministic functions, read no fields and perform no side effects. An uncast
floating-point literal is `F64`, so cast each element to `F32` explicitly.

State tensors require fixed axes and dense `F32` elements. A stateful inferencer cannot declare
`BATCH` in any input, output or state tensor. Port names are distinct across the declaration, and
every ONNX input and output must be bound exactly once, including both sides of every state pair.
Both model state ports must match the declared shape; a dynamic ONNX axis may be instantiated with
the fixed size the declaration supplies.

Each branch invokes the model once for each admitted row, in arrival order. All state successors
advance together after a successful invocation. A failed `INPUTS` expression or model invocation
uses `ON MESSAGE ERROR` and leaves the preceding state intact; `FILTER WHERE` runs before the
model, so a filtered row changes no state. Materialized dependencies resolve before admission.
Every output route receives the same generated result, and its `FLUSH` controls dispatch without
invoking the model again. Acknowledgements wait for dispatch. Without `STATE`, inference keeps its
flush-time execution and supports the existing per-message and batched tensor declarations.

Each admitted batch publishes an immutable tensor generation. Nervix persists the newest
generation on the runtime state snapshot interval and once more during a graceful stop; replicas
receive the same generation. A restart continues from the newest surviving snapshot for that
model binding and branch incarnation. Inputs admitted after that snapshot have already been
acknowledged and are not replayed, so recovery can skip those state updates. A graceful stop
persists the final generation before the branch ends. `STOP` followed by `START` retains it.
A [backup](backup-and-restore.md) also carries each active branch's published tensors with their
shapes, pinned model identity and branch incarnation. Quiesced capture forces publication;
`WITHOUT PAUSE` captures the latest published generation without a common quiesced boundary.
Restore validates that identity and installs exact tensor bits on the scheduled owner and replicas.
Both `RESUME` and a later normal `START` preserve the tensors; `WITHOUT STATE` on backup or restore
starts branches from `INITIAL`.

Branch eviction by `TTL` or `MAX INSTANCES` discards the tensors and publishes an empty final
generation, so a returning key starts from `INITIAL`. Dropping the inferencer removes its stored
state. The state identity includes the resource, pinned version, file and complete `STATE`
declarations; changing that identity starts fresh state. `UNBRANCHED` owns one state sequence;
`BRANCHED BY` owns one per concrete key.

`REBIND RESOURCE` to a different version starts every stateful branch from its declared `INITIAL`,
even when the new version contains identical ONNX bytes. Applying the running domain's schedule
removes snapshots under the replaced fingerprint, so a restart uses only the current binding's
state. Uploading a newer version alone, rebinding to the version already pinned, or rejecting an
incompatible model leaves the existing tensors intact. Dropping and recreating the inferencer with
another `STATE` declaration uses that declaration's initial values. The operation retains its
`ENTITY_PAUSE` classification; see [Rebinding](resource-versions.md#rebinding).

State ports are internal model inputs and outputs, not fields available to route construction.
`SHOW CREATE INFERENCER` preserves the state declaration and its initial expression.

The declared state must fit one configured runtime snapshot section, including its shape and
framing metadata. Nervix checks the derived bound at `CREATE` before storing the inferencer and
again while preparing INITIAL before publishing the processor plan. Native row
execution uses the bounded extension workers; mapped input and public output conversion also
respect the configured relay operation limit. The state is retained once per active branch;
use the branch's `MAX INSTANCES` to bound the number of live copies.

## WASM processor

WASM routes are also set-only and execute only when the guest returns actual output data:

```plexus
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
saves and restores each receive the snapshot selected for that operation. The guest cannot ask
the engine for wall time or execute without an explicit snapshot.

`ON REJECTED STATE` decides what happens when a recreated guest refuses the snapshot Nervix hands
it. It is optional and defaults to `PRESERVE`, which keeps the refused snapshot and reports the
refusal; `RESET` opts the processor in to replacing that branch's state lifetime once. Only the
guest's own verdict on the saved bytes reaches this policy, so no module, limit, storage or
replication failure can erase computation state through it. See
[Recovering A Rejected Snapshot](wasm-processor-guests.md#recovering-a-rejected-snapshot).

An `ATTACHED` WASM processor, the default, acknowledges an input only after the guest-state
checkpoint that covers it is on the stable storage of the branch's owner and of every replica the
schedule assigns the processor; relay fan-out acknowledges the input of a `DETACHED` one upstream.
A checkpoint that cannot get there negatively acknowledges what it covers and recreates the branch's
guest from its last completed checkpoint. `DESCRIBE WASM PROCESSOR` reads the committed binding,
default state generation, latest coordinated reset and retained rejected-state recoveries together
with the owner's current checkpoint stages and revisions. Each reported branch uses a fixed-size
opaque fingerprint rather than a branch value. Required and confirmed replica counts are shown for
checkpoints captured on the current owner; a restored checkpoint whose earlier boundary is unknown
reports those counts as unknown. Counts cover every active branch, while detailed branch and
recovery entries are bounded. `DESCRIBE WASM PROCESSOR <name> FORMAT JSON` serializes the same
typed state inspection used by the text output; the command also returns it as a typed client
outcome in either format. See
[Checkpoints And Acknowledgements](wasm-processor-guests.md#checkpoints-and-acknowledgements) and
[WASM State And Recovery](wasm-state.md).

### Reset guest state from Plexus

`RESET WASM PROCESSOR <processor> STATE FOR <scope>;` replaces the selected guest-state lifetime of
an existing, running processor in the session's active domain, like every other statement that
acts on a domain. Inside `BEGIN` it acts on the domain the transaction is bound to. Without an
active domain it fails with `no active domain selected`. A processor of the same name in another
domain is a different processor and keeps its state. The scope is mandatory:

```plexus
USE sales;
RESET WASM PROCESSOR normalize_events STATE FOR UNBRANCHED;
RESET WASM PROCESSOR normalize_events STATE FOR ALL BRANCHES;
RESET WASM PROCESSOR normalize_events STATE FOR BRANCH VALUES { tenant = 'acme' };
```

Use `FOR UNBRANCHED` only for an unbranched processor. Use `FOR ALL BRANCHES` or exact
`FOR BRANCH VALUES` fields only for a branched processor. Values must match the declared branch
schema exactly. A selected branch must already be active; the command does not create one.
The selected branch loses its previous guest computation state and starts from the guest's initial
state. Sibling branches keep their state and continue processing.

The normal client and CLI command path returns success only after the new generation is durable
and its replacement execution is usable. Repeating the same command execution reference returns
the retained outcome without creating another generation. A fresh command is a new reset. See
[Command Completion](command-completion.md) for retry validity and expired references.

## Correlator

Correlators use explicit sides and have no default input scope:

```plexus
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

A record that finds no partner within `MAX TIME` of its arrival leaves the correlation through its
side's `ON CORRELATION TIMEOUT` action, written left side first: `DROP` discards it, and
`SEND TO <relay>` forwards it unchanged, in its own branch, to a relay whose schema is compatible
with that side's schema.

### Inspect Pending Records

```plexus
SHOW CORRELATOR correlate_orders STATE FOR ALL BRANCHES LIMIT 100 FORMAT JSON;
```

`SHOW CORRELATOR <name> STATE` reads the records the correlator holds waiting for a match: a `left`
section with the left side's fields and a `right` section with the right side's fields, each
followed by `received_at`, when the record arrived, and `timeout_at`, `MAX TIME` later, when its
`ON CORRELATION TIMEOUT` action applies unless a match arrives first. A side with no row on a page
has no section. Each branch lists its left records, then its right records, each side in arrival
order. The clauses, envelope, counts, redaction, continuation and refusals are those of
[`SHOW REORDERER ... STATE`](#inspect-held-rows). A pending record keeps the batch it arrived in
alive, so the byte count is the logical Arrow bytes of the batches the held records belong to, each
batch counted once however many of its records are pending.

`DESCRIBE CORRELATOR` reports the same `branches:`, `rows:` and `state bytes:` lines as
`DESCRIBE REORDERER`, counting the records of both sides.

## Reingestor

Reingestors are branch-boundary transforming nodes. Each route preserves the incoming exact branch,
constructs another branch, or becomes unbranched:

```plexus
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
preserved. A message passes its input's `FROM ... WHERE` and then the reingestor's `FILTER WHERE`
before any route constructs it. State lookup always uses the incoming branch, never a partially
constructed outgoing key. The reingestor resolves the outgoing branch before buffering the route,
so each concrete outgoing branch has an independent flush interval and size boundary. Downstream
branch execution receives the completed Arrow batch and does not apply a second flush policy.

### Altering Reingestors

`ALTER REINGESTOR` applies the shared input, collection, filter, attachment, materialized-state,
and route operations in written order:

```plexus
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

```plexus
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

```plexus
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

[Errors And Diagnostics](./errors-and-diagnostics.md) owns the typed failure and structured
message-error model; this section defines the route behavior and available expressions.

`ON MESSAGE ERROR` terminates each route. `SEND TO` constructs an error relay record with ordered
`SET` assignments:

```plexus,ignore
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

The error relay must declare the same exact named branch as the source route, or be unbranched
when the source is unbranched. A rejected binding identifies the node, source route, error relay,
and both branch declarations.
