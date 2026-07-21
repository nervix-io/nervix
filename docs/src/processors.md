# Runtime Nodes

Nervix supports several runtime node types that transform or route records between relays.

Processing nodes operate on one concrete branch at a time. This keeps tenant-specific, user-specific, or other group-specific state isolated while records move through the graph. A processor receives branch-local relay batches and keeps branch-local buffers or state unless its node type explicitly crosses a branch boundary.

Every normal processor that consumes relays must declare `BRANCHED BY <branch>`. The exact branch name must match the input and output relays; another branch backed by the same schema is not compatible. The declaration is a contract: the processor is materialized once per concrete branch and must not consume the mixed logical relay. Reingestors are different because they reference an explicit branch whose `VALUES` map creates a new downstream branch key. Emitters are terminal drains and do not declare processor branch selection.

`GENERATOR` is a periodic source runtime node.

Relay-consuming runtime nodes support `ATTACHED` or `DETACHED` branch semantics. `GENERATOR` has no upstream branch to attach to and therefore does not declare a branch mode.

- `ATTACHED` keeps the upstream ACK chain open
- `DETACHED` drops ACK state for that branch immediately

Every processor `TO` route owns an `ON MESSAGE ERROR <policy>`. `ON GENERAL ERROR` is reserved for `INGESTOR` and `EMITTER`, because those are the runtime nodes that own external-system failures. A WASM processor keeps its node-level `ON GLOBAL ERROR` contract for guest failures that are not associated with a message or output route.

## Branch Ownership

Branch behavior by node type:

- `INGESTOR` starts a branch
- `DEDUPLICATOR`, `REORDERER`, `CORRELATOR`, and `JUNCTION` run inside that branch under one concrete branch key and may fan records out through output routes
- `WASM PROCESSOR` runs inside that branch under one concrete branch key
- `WINDOW PROCESSOR` keeps window state inside that branch under one concrete branch key
- `REINGESTOR` is the boundary that consumes from the whole input relay and starts new downstream branches
- `EMITTER` is the terminal mixed consumer that drains a relay to an external sink

Processors between ingestor and reingestor/emitter are scoped to one concrete branch. The isolation covers both the relay batches they handle and the state they keep.

Every destination of a flush-based multi-output node must declare either `FLUSH EACH <duration> MAX BATCH SIZE <bytes>` or `FLUSH IMMEDIATE` directly after its `TO <relay>`. Each destination has an independent buffer and deadline, so one output can publish immediately while another batches the same produced rows. Generators and emitters have one terminal output and retain their node-level `FLUSH` clause. Window processors use `WIDTH` and `STEP` instead. WASM processors do not declare a flush policy: output emission is controlled by the guest through `process` output and guest-requested timeouts.

## Processor Outputs

Relay-consuming processors declare one or more destination outputs after their input and optional arrival filter:

```nspl
FROM <input> [WHERE <expr>], ...
[FILTER WHERE <expr>]
TO <relay> (FLUSH EACH <duration> MAX BATCH SIZE <bytes> | FLUSH IMMEDIATE)
  [SET <relay>.<field> = <expr>, ...] [UNSET <input>.<field>, ...] [WHERE <expr>]
  ON MESSAGE ERROR <policy>
[TO <relay> ...]
```

`FROM ... WHERE` is a source-level input filter. It runs first and may read fields from that source relay, for example `FROM notifications WHERE notifications.active`. Most processors may declare multiple `FROM` relays separated by commas. Those input relays must have the same schema, and each source filter applies only to the relay it is attached to. Correlators use side-specific `LEFT FROM` and `RIGHT FROM` clauses instead.

`FILTER WHERE` is a node-level arrival filter. It runs after source filtering and before the processor accepts a row into its buffer or state. It replaces the old global processor-level `WHERE` form.

Each `TO` route owns its flush policy, destination filter-map, and message error policy. `SET` and `UNSET` are destination-owned and therefore appear after the destination relay and flush policy are known. `WHERE` is optional; a route without `WHERE` receives every row produced by the processor. A construction or filter-map failure on one route is handled only by that route's policy.

Assignments inside one `SET` clause execute from left to right. The same destination field may be assigned more than once, and each right-hand expression observes the latest preceding assignment, for example `SET out.amount = 1, out.amount = out.amount + 1` produces `2`. This does not change existing `WHERE` visibility or evaluation order.

Passthrough inheritance only exists for processors with a natural one-input-row to one-output-row shape, such as deduplicators, reorderers, junctions, and reingestors. For those processors, a destination without a filter-map inherits same-named fields from the source row; if the destination schema should not receive a source field, write `UNSET`.

Window processors, correlators, and WASM processors route their generated records rather than inheriting input fields. Inferencers are different: each model result starts with the inbound record, and each `TO` route may add model values with `SET`.

Per-output clauses are row-level filter-map programs:

- `WHERE` drops rows
- `SET` writes destination fields
- `UNSET` removes inherited source fields from the downstream shape on one-to-one processors

Schema validation is applied at statement time against each destination relay's effective output schema after `SET` and `UNSET`. Sensitive fields may flow to external emitters, but sensitive data cannot be stored into a non-sensitive internal destination field unless the expression explicitly uses `leak_sensitive`.

Output routes use fan-out semantics: each row is evaluated independently against every `TO` route. A row is forwarded to every route whose optional `WHERE` condition matches, and to every unconditional route. There is no fallback route for rows that do not match a conditional route.

Processors use the same filter-map expression surface as ingestors and emitters:

- arithmetic: `+`, `-`, `*`, `/`, `%`
- comparisons and boolean logic: `=`, `!=`, `>`, `<`, `>=`, `<=`, `AND`, `OR`, `NOT`
- explicit casts: `expr AS TYPE`
- built-ins: string, null-handling, numeric, regex, and contextual functions such as `lower`, `coalesce`, `abs`, `regexp_replace`, `now`, and `uuid_v7`

See [Filter-Map Functions](filter-map-functions.md) for the full function reference.

That expression surface applies to the full Nervix internal schema type set:

- `U8`, `I8`, `U16`, `I16`, `U32`, `I32`, `U64`, `I64`
- `F32`, `F64`
- `BOOL`, `STRING`, `DATETIME`

Nested conditions and chained calls such as `lower(trim(raw))` are supported here as well.

Example:

```nspl
CREATE BRANCH by_user
  SCHEMA user_branch TTL 5m;

CREATE DEDUPLICATOR project_notifications
  FROM notifications WHERE notifications.active
  TO projected_notifications FLUSH EACH 100ms MAX BATCH SIZE 1MiB
    SET projected_notifications.normalized = lower(trim(notifications.raw)),
        projected_notifications.amount = notifications.amount + 1
    UNSET notifications.raw, notifications.active
    WHERE trim(notifications.raw) != '' ON MESSAGE ERROR LOG
  BRANCHED BY by_user
  DEDUPLICATE ON notifications.tenant, notifications.user_id
  MAX TIME 10m;
```

That example keeps the existing branch grouping, filters inactive rows at the source boundary, rewrites `normalized` and `amount`, removes `raw` and `active`, and forwards the surviving rows into `projected_notifications`.

Branch-preserving processor output programs can also read the current branch key through `branch.<key>`. For example, `SET projected_notifications.tenant = branch.tenant WHERE branch.tenant = notifications.tenant` copies and tests the branch key without requiring the key to be present in the message payload.

## Generator

```nspl
CREATE [IF NOT EXISTS] GENERATOR <name>
  TO <output>
  EACH <duration>
  FLUSH EACH <duration> MAX BATCH SIZE <bytes> | FLUSH IMMEDIATE
  SET <output>.<field> = <materialized_stream>.<field>, ...
  ON MESSAGE ERROR <policy>;
```

A generator has no incoming message. It wakes up periodically, reads the current materialized state of one or more relays in the same domain, and emits rows into one output relay.

Generator rules:

- only `SET` is supported
- source references must target materialized relays
- destination assignments must target the output relay namespace explicitly
- a flush policy is mandatory and defines when buffered generated rows are emitted downstream
- in paced domains, both `EACH <duration>` and `FLUSH EACH <duration> MAX BATCH SIZE <bytes>` are evaluated against domain logical time
- in unpaced domains, both `EACH <duration>` and `FLUSH EACH <duration> MAX BATCH SIZE <bytes>` use wall clock time
- `FLUSH IMMEDIATE` emits after every generation cycle that produces rows

## Junction

```nspl
CREATE [IF NOT EXISTS] [ATTACHED|DETACHED] JUNCTION <name>
  FROM <input> [WHERE <expr>], ...
  [TO <output> (FLUSH EACH <duration> MAX BATCH SIZE <bytes> | FLUSH IMMEDIATE) [SET <output>.<field> = <expr>, ...] [UNSET <input>.<field>, ...] [WHERE <expr>] ON MESSAGE ERROR <policy>]
  [TO <output> ...]
  BRANCHED BY <branch>;
```

A junction is a branch-local pass-through processor. It consumes one or more same-schema upstream relays, applies source `WHERE` and node `FILTER WHERE` filters, and forwards accepted rows into each output route's independent flush buffer.

Each output route may use the normal processor `SET`, `UNSET`, and `WHERE` filter-map clauses. Routes without a `WHERE` receive every accepted record.

Use it when records should be filtered, projected, fanned out, or joined into downstream relay paths without additional node-specific state.

## Inferencer

```nspl
CREATE [IF NOT EXISTS] [ATTACHED|DETACHED] INFERENCER <name>
  FROM <input> [WHERE <expr>], ...
  [TO <output> (FLUSH EACH <duration> MAX BATCH SIZE <bytes> | FLUSH IMMEDIATE) [SET <output>.<field> = <expr>, ...] [WHERE <expr>] ON MESSAGE ERROR <policy>]
  [TO <output> ...]
  BRANCHED BY <branch>
  USING RESOURCE <resource> [VERSION <n>]
  FILE '<model.onnx>'
  INPUTS {
    "<onnx_input_name>" DENSE TENSOR<F32>[<dimensions>] = <input>.<field>,
    ...
  }
  OUTPUT SCHEMA {
    "<onnx_output_name>" DENSE TENSOR<F32>[<dimensions>],
    ...
  };
```

An inferencer declares a branch-local ONNX model execution node. The model file is loaded from a versioned `RESOURCE`; if `VERSION` is omitted, Nervix resolves the latest uploaded resource version.

The optional `FILTER WHERE` clause runs before tensor construction. `INPUTS` maps every ONNX input tensor name to a field on one of the declared input relays after that arrival filter. `OUTPUT SCHEMA` declares every ONNX output tensor name and type without choosing a destination. Each inbound record is inherited by every `TO` route. A route-level `SET` maps model results through `inner_output.<tensor>` and may reuse the actual per-message model inputs through `inner_input.<tensor>`. Because `SET` belongs to a `TO` clause, separate output routes can project different model values.

Every input mapping and output declaration includes its complete tensor schema. The supported schema is currently
`DENSE TENSOR<F32>`. Each dimension is a positive fixed size, `DYNAMIC`, or the
optional `BATCH` axis. An empty list (`DENSE TENSOR<F32>[]`) is a scalar. All
tensor declarations in one inferencer must either contain exactly one `BATCH` axis each or
contain no `BATCH` axes. The batch axis may occur at any rank position.

Tensor dimensions map structurally to one message's schema field: a fixed
dimension consumes one matching `ARRAY` axis, `DYNAMIC` consumes one `VEC` axis,
and `BATCH` consumes no field axis. For example,
`DENSE TENSOR<F32>[3, 224, 224]` maps exactly to
`ARRAY<F32, 3, 224, 224>`, while `DENSE TENSOR<F32>[DYNAMIC, 64]` maps to
`VEC<ARRAY<F32, 64>>`. A flat `ARRAY<F32, 150528>` is not compatible with the
three-dimensional tensor. Nervix preserves the nested shape on input and output;
only the contiguous buffer passed across the ONNX Runtime boundary is linear.

Dense dynamic values must be rectangular. Batched inputs with different concrete
dynamic shapes are grouped by shape into separate ONNX invocations, and results
are restored to their original message order.

Without `BATCH`, each collected message causes an independent ONNX invocation. With `BATCH`, one flush of `N` messages causes one invocation and the declared batch axis is materialized with size `N`; returned slices are assigned to messages in their original order.

Inferencer creation loads the selected ONNX model and requires complete, exact input and output name coverage. It validates dense representation, `F32` element type, rank, and every fixed dimension. An ONNX dynamic dimension may be instantiated by either `BATCH` or a fixed positive size; `BATCH` is incompatible with a fixed ONNX dimension. Inferencers preserve the upstream branch exactly as received, and each concrete branch owns its model session and buffered messages.

Per-message schema:

```nspl
INPUTS {
  "features" DENSE TENSOR<F32>[128] = input.features
}
OUTPUT SCHEMA {
  "scores" DENSE TENSOR<F32>[10]
}
```

Batched schema:

```nspl
INPUTS {
  "features" DENSE TENSOR<F32>[BATCH, 128] = input.features,
  "mask" DENSE TENSOR<F32>[BATCH, 128] = input.mask
}
OUTPUT SCHEMA {
  "scores" DENSE TENSOR<F32>[BATCH, 10]
}
```

## Deduplicator

```nspl
CREATE [IF NOT EXISTS] [ATTACHED|DETACHED] DEDUPLICATOR <name>
  FROM <input> [WHERE <expr>], ...
  [TO <output> (FLUSH EACH <duration> MAX BATCH SIZE <bytes> | FLUSH IMMEDIATE) [SET <output>.<field> = <expr>, ...] [UNSET <input>.<field>, ...] WHERE <expr> ON MESSAGE ERROR <policy>]
  [TO <output> (FLUSH EACH <duration> MAX BATCH SIZE <bytes> | FLUSH IMMEDIATE) [SET <output>.<field> = <expr>, ...] [UNSET <input>.<field>, ...] ON MESSAGE ERROR <policy>]
  BRANCHED BY <branch>
  DEDUPLICATE ON <expr>[, <expr> ...]
  MAX TIME <duration>;
```

```nspl
DESCRIBE DEDUPLICATOR <name>;
```

A deduplicator evaluates the `DEDUPLICATE ON` expression list through the VM and suppresses records whose evaluated key sequence already appears in a bounded recent history. Expressions must use fully-qualified relay field references, for example `lower(trim(input.transaction_id))`.

That bounded history is branch-local. The same deduplication value can appear independently in different branch groups.

Deduplicator state is persistent and replicated when runtime replication is configured. Its internal state structure is one branch-local `recent_key_set`, bounded by `MAX TIME`.

State-holding `DESCRIBE` commands expose the scheduled `owner` and `replicas` uniformly. `DESCRIBE DEDUPLICATOR` also exposes the input/output relays, deduplication key expressions, max history time, branch-local marker, and whether persistent/replicated state is used.

## Reorderer

```nspl
CREATE [IF NOT EXISTS] [ATTACHED|DETACHED] REORDERER <name>
  FROM <input> [WHERE <expr>], ...
  [TO <output> (FLUSH EACH <duration> MAX BATCH SIZE <bytes> | FLUSH IMMEDIATE) [SET <output>.<field> = <expr>, ...] [UNSET <input>.<field>, ...] WHERE <expr> ON MESSAGE ERROR <policy>]
  [TO <output> (FLUSH EACH <duration> MAX BATCH SIZE <bytes> | FLUSH IMMEDIATE) [SET <output>.<field> = <expr>, ...] [UNSET <input>.<field>, ...] ON MESSAGE ERROR <policy>]
  BRANCHED BY <branch>
  BY <expr>, <expr>, ...
  MAX TIME <duration>;
```

A reorderer buffers records for one concrete branch and emits them sorted by the `BY` expression list. Each `BY` expression is evaluated with the same VM expression surface as filter-map programs, so field references and built-ins such as `lower(...)` are valid ordering keys.

Ordering is ascending. When two records have identical `BY` keys, arrival order is the tie-breaker.

`MAX TIME` bounds the ordering horizon: once the oldest record reaches it, the reorderer sorts the pending records. The sorted result then enters each destination's independent `FLUSH` buffer. Reorderers preserve the upstream branch and do not support `ON GENERAL ERROR`.

`DESCRIBE REORDERER <name>` reports the scheduled owner and replicas, input/output relays, ordering expression list, max time, per-output flush policies, filter-map presence, branch-local execution marker, state persistence markers, and runtime metrics when available.

## Correlator

```nspl
CREATE [IF NOT EXISTS] [ATTACHED|DETACHED] CORRELATOR <name>
  LEFT FROM <left_input> [WHERE <expr>]
  [LEFT FROM <left_input> [WHERE <expr>] ...]
  RIGHT FROM <right_input> [WHERE <expr>]
  [RIGHT FROM <right_input> [WHERE <expr>] ...]
  CORRELATE WHERE <left_right_predicate>
  MATCH EARLIEST | LATEST
  TO <output> (FLUSH EACH <duration> MAX BATCH SIZE <bytes> | FLUSH IMMEDIATE)
    SET <output>.<field> = <expr>, ...
    [WHERE <expr>]
    ON MESSAGE ERROR <policy>
  [TO <output> ...]
  BRANCHED BY <branch>
  MAX TIME <duration>
  ON CORRELATION TIMEOUT <left_action>, <right_action>;
```

A correlator stores unmatched records in branch-local pending state and matches a left/right pair when the `CORRELATE WHERE` predicate evaluates to true against both input records. Relays declared on the same side must share that side's schema; the left and right sides may use different schemas. Source-level `WHERE` clauses apply only to the relay they follow. The predicate must compile to `BOOLEAN`. `MATCH EARLIEST` keeps the first pending record on a side for a matching predicate and acknowledges later same-side duplicates. `MATCH LATEST` replaces the pending same-side record and acknowledges the replaced one.

When a left/right pair matches, the pair is removed from pending state and each `TO` route independently projects an output record. Correlators do not implicitly copy input fields. Every route must use `SET` to assign all required fields in that destination relay's schema; optional output fields may be omitted. A route-level `WHERE` may suppress that destination without affecting the other routes.

`MAX TIME` is evaluated against the domain clock and bounds how long an unmatched record can remain pending. `ON CORRELATION TIMEOUT` has one action for the left input and one for the right input. `DROP` acknowledges and forgets the record. `SEND TO <relay>` forwards the original unmodified record to another schema-compatible relay and acknowledges it after the send succeeds.

Correlators preserve the upstream branch. Each concrete branch gets separate pending input state and an output buffer, even when the two inputs receive interleaved records that would match the same predicate in other branches.

## WASM Processor

```nspl
CREATE [IF NOT EXISTS] [ATTACHED|DETACHED] WASM PROCESSOR <name>
  USING RESOURCE <resource> [VERSION <n>]
  FILE '<path>'
  FROM <input> [WHERE <expr>], ...
  [FILTER WHERE <expr>]
  [TO <output> [SET <output>.<field> = <expr>, ...] [WHERE <expr>] ON MESSAGE ERROR <policy>]
  [TO <output> ...]
  BRANCHED BY <branch>
  ON GLOBAL ERROR <policy>;
```

```nspl
DESCRIBE WASM PROCESSOR <name>;
```

A WASM processor loads a native `wasm32-unknown-unknown` module from a Nervix resource and runs one guest instance per concrete branch. Source `FROM ... WHERE` runs first, then `FILTER WHERE` runs before the guest receives a row. Multiple `FROM` relays are consumed as one same-schema stream; the guest still sees a single source schema. The guest receives one verified, size-prefixed FlatBuffer containing an Arrow IPC input batch and an ACK sidecar. Arrow IPC byte vectors are accessed without deserialization or copying inside the host/guest memory that owns the FlatBuffer. Each output envelope names a declared `TO` relay and provides one destination-aligned column descriptor per field: an unchanged input-column reference, a guest-generated column reference, or an uninitialized marker. Nervix applies that route's `SET` and `WHERE` clauses before materializing the exact destination batch.

See [WASM Processor Guests](wasm-processor-guests.md) for the guest ABI and Rust/Go authoring examples.

The declared `BRANCHED BY` branch must match the input and output relays. WASM processors preserve the upstream branch group exactly as received; they do not consume from a mixed logical relay and they do not fan in records across branches. WASM processors do not inherit input fields implicitly: the guest must explicitly select every destination column. An input reference reuses retained host Arrow arrays without guest-side copying, and its row's `source_token` selects both the referenced source row and the original record visible through `input.field`. Route `SET` clauses may read guest output fields through the destination relay namespace, and may read that selected source row through the `input` namespace, for example `TO out1 SET out1.name = lower(out1.name), out1.surname = input.surname`. Reading an uninitialized field gives it the destination type and materializes typed NULL values for normal expression semantics. An uninitialized optional field that remains unread is materialized as typed NULLs at the node boundary; an uninitialized required field fails instead. This state never enters a relay or crosses a node boundary. A generated row without a source token cannot use `input.field`. `UNSET` is not valid on WASM output routes.

The host initializes each branch instance with the FlatBuffers `BranchInit` message:

- domain name
- domain type
- branch key
- input relay schema
- output relay schemas

Guest state is branch-local processor state. The host saves it through the guest `nervix_dump_state` export and restores it through `nervix_load_state` when a branch instance is recreated. Nervix persists and replicates those guest-owned bytes.

ACK tokens and retained input-column sources are different: they are host-local hot-path runtime state and are not persisted or replicated. A guest may carry tokens from input to output, explicitly ack/nack them through the sidecar, and reference a retained input row while its source token is live, but the actual external ACK/NACK operation is always performed by the host. If ACK state and retained Arrow batches are lost with a processor owner, restored references are rejected and the upstream ingestor reacts according to its delivery mode and retry policy.

Flush is guest-controlled for WASM processors. Nervix calls the guest `process` export when input arrives and forwards any batches returned by the guest. The guest may also request a domain-clock timeout through the host timeout import. When the timeout fires, the host calls the guest timeout export and forwards any emitted batches through the same ACK sidecar path as normal processing.

`DESCRIBE WASM PROCESSOR` reports the scheduled owner and replicas, input/output relays, resource, resource version, file, FlatBuffers ABI serialization, guest-controlled flush marker, branch-local marker, persistent/replicated state markers, and runtime metrics when available.

## Window Processor

```nspl
CREATE [IF NOT EXISTS] [ATTACHED|DETACHED] WINDOW PROCESSOR <name>
  FROM <input> [WHERE <expr>], ...
  [TO <output> [SET <output>.<field> = <expr>, ...] [WHERE <expr>] ON MESSAGE ERROR <policy>]
  [TO <output> ...]
  BRANCHED BY <branch>
  WIDTH [<n> MESSAGES] [<duration> DURATION]
  STEP [<n> MESSAGES] [<duration> DURATION]
  AGGREGATE
    <output>.<field> = <aggregate_expr>,
    ...;
```

```nspl
DESCRIBE WINDOW PROCESSOR <name>;
```

A window processor consumes one or more same-schema branch-local input relays and emits aggregate records in the first declared output relay's schema before routing them to matching destinations. It does not inherit input fields automatically. The `AGGREGATE` block fully defines the emitted base record shape.

Window processors preserve the upstream branch exactly as received. Each concrete branch gets its own independent window state, and each window contains records from one branch key.

`WIDTH` defines when the current window is ready to emit. It may use message count, duration, or both:

```nspl
WIDTH 100 MESSAGES
WIDTH 10s DURATION
WIDTH 100 MESSAGES 10s DURATION
```

If both message and duration conditions are present, either one may make the window ready. `STEP` defines how far the window advances after an emit:

```nspl
STEP 10 MESSAGES
STEP 1s DURATION
STEP 10 MESSAGES 1s DURATION
```

When `WIDTH` equals `STEP`, the processor behaves as a tumbling window. When `STEP < WIDTH`, it behaves as a sliding window. `STEP` must not exceed `WIDTH` for any bound kind that is present.

Duration windows use record watermark metadata as the event time. Ingested relay records always carry low and high watermarks. Window membership uses the low watermark. Duration `WIDTH` creates a scheduled timeout at `first_entry_low_watermark + WIDTH`; the timeout uses the shared domain clock, but the deadline is derived from each branch-local window state. When a window emits an aggregate record, its low watermark is the minimum low watermark in the window and its high watermark is the current domain time at emission.

Aggregate expressions use the normal VM expression language, with aggregate calls supplied by the window runtime. An aggregate call may participate in unary, binary, cast, and standard function evaluation, but aggregate calls cannot be nested inside another aggregate call's arguments:

```nspl
AGGREGATE
  summaries.tenant = FIRST(metrics.tenant),
  summaries.sample_count = COUNT(metrics.latency),
  summaries.adjusted_sample_count = COUNT(metrics.latency) + 2,
  summaries.first_latency = FIRST(metrics.latency),
  summaries.last_latency = LAST(metrics.latency),
  summaries.min_latency = MIN(metrics.latency),
  summaries.max_latency = MAX(metrics.latency),
  summaries.total_latency = SUM(metrics.latency),
  summaries.latency_p90 = PERCENTILE_LINEAR_HISTOGRAM(metrics.latency, 90, 2048, 0, 10000, '2s'),
  summaries.latencies = [
    PERCENTILE_LINEAR_HISTOGRAM(metrics.latency, 50, 2048, 0, 10000, '2s'),
    PERCENTILE_LINEAR_HISTOGRAM(metrics.latency, 90, 2048, 0, 10000, '2s'),
    PERCENTILE_LINEAR_HISTOGRAM(metrics.latency, 99, 2048, 0, 10000, '2s')
  ];
```

Supported aggregate functions:

- `COUNT(expr)`
- `FIRST(expr)`
- `LAST(expr)`
- `MAX(expr)`
- `MIN(expr)`
- `SUM(expr)`
- `PERCENTILE_LINEAR_HISTOGRAM(expr, percentile, buckets, min, max, 'delay')`

`PERCENTILE_LINEAR_HISTOGRAM` uses a fixed-width linear histogram. Its configuration arguments must be constants:

- `percentile` is numeric and must be between `0` and `100`
- `buckets` is an integer greater than `0`
- `min` and `max` are numeric and `min < max`
- `delay` is a duration string parsed with `humantime`, such as `'2s'`. When `STEP` removes values from the active window, histogram bucket counts remain eligible until `removal_time + delay`; use `'0ms'` for immediate removal.

Aggregate calls declare runtime state by implication. Before a branch instance runs, Nervix walks the complete `AGGREGATE` expression tree, extracts every concrete function/input/configuration demand, minimizes the set of required physical structures, and compiles the surrounding expressions as VM programs. Repeated uses do not create repeated state, and compatible operations share one structure: `FIRST` and `LAST` over the same expression share a sequence, `MIN` and `MAX` share a sorted map, and histogram percentiles with identical input and configuration share a histogram. Multiple `TO` routes consume the same evaluated base record and do not duplicate aggregate state.

At emission time, the VM calls the window runtime for each aggregate value. Once injected, that value follows the standard VM evaluation chain, so expressions such as `COUNT(metrics.latency) + 2` and `ABS(MIN(metrics.delta))` use ordinary typed VM operations.

`DESCRIBE WINDOW PROCESSOR` exposes that deduplicated demand set. The output includes the scheduled owner and replicas, processor relays, window bounds, branch-local execution marker, aggregate structure count, and one entry per demanded structure with:

- aggregate functions that require the structure
- storage kind (`counter`, `sequence`, `sorted_map`, `sum`, or `linear_histogram`)
- reference count from the aggregate expression tree
- input expression when applicable
- histogram bucket/range/delay configuration when applicable

For example, these two output fields share one histogram accumulator because the input expression and histogram configuration are identical. The percentile itself is a read parameter over that accumulator:

```nspl
summaries.latency_p50 = PERCENTILE_LINEAR_HISTOGRAM(metrics.latency, 50, 2048, 0, 10000, '2s'),
summaries.latency_p90 = PERCENTILE_LINEAR_HISTOGRAM(metrics.latency, 90, 2048, 0, 10000, '2s')
```

These would create different histogram accumulators because the configuration differs:

```nspl
summaries.latency_p90 = PERCENTILE_LINEAR_HISTOGRAM(metrics.latency, 90, 2048, 0, 10000, '2s'),
summaries.latency_p99_wide = PERCENTILE_LINEAR_HISTOGRAM(metrics.latency, 99, 4096, 0, 20000, '2s')
```

ACK behavior follows branch mode. In `ATTACHED` mode, the aggregate output carries the attached ACK entries for all input messages that contributed to the emitted window. In `DETACHED` mode, the window processor detaches upstream ACK state.

## Reingestor

```nspl
CREATE BRANCH by_reingested_tenant
  SCHEMA tenant_branch TTL 5m;

CREATE [IF NOT EXISTS] [ATTACHED|DETACHED] REINGESTOR <name>
  FROM <relay> [WHERE <expr>], ...
  [TO <relay> (FLUSH EACH <duration> MAX BATCH SIZE <bytes> | FLUSH IMMEDIATE) [SET <relay>.<field> = <expr>, ...] [UNSET <input>.<field>, ...] WHERE <expr> ON MESSAGE ERROR <policy>]
  [TO <relay> (FLUSH EACH <duration> MAX BATCH SIZE <bytes> | FLUSH IMMEDIATE) [SET <relay>.<field> = <expr>, ...] [UNSET <input>.<field>, ...] ON MESSAGE ERROR <policy>]
  BRANCHED BY by_reingested_tenant;
```

For reingestors, each `TO` destination declares when its own buffered output is published downstream and how message-specific failures are handled. Different destinations may use different cadences and error policies.

A reingestor republishes records from one or more same-schema relays into another and starts downstream branches with a new branch mapping.

This is the main tool for changing native branch grouping inside the Nervix graph.

A reingestor is an explicit branch boundary:

- it consumes across all concrete input branches of its source relays
- it applies each source filter to its own input relay before repartitioning
- it computes a new branch group for each outgoing record
- it buffers rows under the new downstream branch group
- it starts or resolves downstream branches in the target relay

The referenced branch `VALUES` mappings can read either output relay fields or the source branch key. For example, `VALUES { tenant = branch.tenant }` preserves a tenant grouping even when the payload schema does not carry `tenant` as a row field.

## Materializer

Materializers are not usually created directly in user NSPL. Nervix schedules them when a relay declares materialized state:

```nspl
CREATE [IF NOT EXISTS] RELAY notifications
  SCHEMA notification
  WITH MATERIALIZED STATE LAST BY TIMESTAMP;
```

That produces a materializer runtime node responsible for:

- applying latest-value updates
- deleting expired materialized keys
- persisting snapshots to Fjall
- replicating those snapshots to followers

`SHOW RELAY <relay> MATERIALIZED STATE` exposes the scheduled materializer owner and replicas alongside the current materialized entries.
