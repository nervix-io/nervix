# Runtime Nodes

Nervix supports several runtime node types that transform or route records between relays.

Processing nodes operate on one concrete branch at a time. This keeps tenant-specific, user-specific, or other group-specific state isolated while records move through the graph. A processor receives branch-local relay batches and keeps branch-local buffers or state unless its node type explicitly crosses a branch boundary.

Every normal processor that consumes relays must declare `BRANCHED BY <branch>`. The branch's schema must match the input and output relays. The declaration is a contract: the processor is materialized once per concrete branch and must not consume the mixed logical relay. Reingestors are different because they reference an explicit branch whose `VALUES` map creates a new downstream branch key. Emitters are terminal drains and do not declare processor branch selection.

`GENERATOR` is a periodic source runtime node.

Relay-consuming runtime nodes support `ATTACHED` or `DETACHED` branch semantics. `GENERATOR` has no upstream branch to attach to and therefore does not declare a branch mode.

- `ATTACHED` keeps the upstream ACK chain open
- `DETACHED` drops ACK state for that branch immediately

Pure runtime processors end with `ON MESSAGE ERROR <policy>` only. `ON GENERAL ERROR` is reserved for `INGESTOR` and `EMITTER`, because those are the runtime nodes that own external-system failures.

## Branch Ownership

Branch behavior by node type:

- `INGESTOR` starts a branch
- `DEDUPLICATOR`, `REORDERER`, `CORRELATOR`, and `JUNCTION` run inside that branch under one concrete branch key and may fan records out through output routes
- `WASM PROCESSOR` runs inside that branch under one concrete branch key
- `WINDOW PROCESSOR` keeps window state inside that branch under one concrete branch key
- `REINGESTOR` is the boundary that consumes from the whole input relay and starts new downstream branches
- `EMITTER` is the terminal mixed consumer that drains a relay to an external sink

Processors between ingestor and reingestor/emitter are scoped to one concrete branch. The isolation covers both the relay batches they handle and the state they keep.

Every flush-based runtime node must declare either `FLUSH EACH <duration> MAX BATCH SIZE <bytes>` or `FLUSH IMMEDIATE` in its NSPL definition. `FLUSH EACH` uses the duration as the local batch boundary: the node buffers input under one concrete branch group and forwards it downstream on that cadence. `FLUSH IMMEDIATE` forwards each received batch without waiting for a flush deadline. Emitters also declare `FLUSH` and use it to collect a terminal output batch before publishing externally. Window processors use `WIDTH` and `STEP` instead. WASM processors do not declare a flush policy: output emission is controlled by the guest through `process` output and guest-requested timeouts.

## Processor Outputs

Relay-consuming processors declare one or more destination outputs after their input and optional arrival filter:

```nspl
FROM <input> [WHERE <expr>], ...
[FILTER WHERE <expr>]
TO <relay> [SET <relay>.<field> = <expr>, ...] [UNSET <input>.<field>, ...] [WHERE <expr>]
[TO <relay> ...]
```

`FROM ... WHERE` is a source-level input filter. It runs first and may read fields from that source relay, for example `FROM notifications WHERE notifications.active`. Most processors may declare multiple `FROM` relays separated by commas. Those input relays must have the same schema, and each source filter applies only to the relay it is attached to. Correlators use side-specific `LEFT FROM` and `RIGHT FROM` clauses instead.

`FILTER WHERE` is a node-level arrival filter. It runs after source filtering and before the processor accepts a row into its buffer or state. It replaces the old global processor-level `WHERE` form.

Each `TO` route may carry its own destination filter-map. `SET` and `UNSET` are destination-owned and therefore appear after the destination relay is known. `WHERE` is optional; a route without `WHERE` receives every row produced by the processor.

Passthrough inheritance only exists for processors with a natural one-input-row to one-output-row shape, such as deduplicators, reorderers, junctions, and reingestors. For those processors, a destination without a filter-map inherits same-named fields from the source row; if the destination schema should not receive a source field, write `UNSET`.

Generated-output processors do not inherit input fields. Window processors emit the `AGGREGATE` record, inferencers emit ONNX tensor outputs plus explicitly `SET` fields, correlators emit their `OUTPUT` record, and WASM processors emit the guest's Arrow output. Their route clauses run after that generated record exists.

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
  TO projected_notifications
    SET projected_notifications.normalized = lower(trim(notifications.raw)),
        projected_notifications.amount = notifications.amount + 1
    UNSET notifications.raw, notifications.active
    WHERE trim(notifications.raw) != ''
  BRANCHED BY by_user
  DEDUPLICATE ON notifications.tenant, notifications.user_id
  MAX TIME 10m
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG;
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
  [TO <output> [SET <output>.<field> = <expr>, ...] [WHERE <expr>]]
  [TO <output> ...]
  BRANCHED BY <branch>
  FLUSH EACH <duration> MAX BATCH SIZE <bytes> | FLUSH IMMEDIATE
  ON MESSAGE ERROR <policy>;
```

A junction is a branch-local pass-through processor. It consumes one or more same-schema upstream relays, applies source `WHERE` and node `FILTER WHERE` filters, buffers accepted Arrow batches until its flush policy fires, concatenates buffered batches when needed, and forwards records through one or more output routes.

Each output route may use the normal processor `SET`, `UNSET`, and `WHERE` filter-map clauses. Routes without a `WHERE` receive every accepted record.

Use it when records should be filtered, projected, fanned out, or joined into downstream relay paths without additional node-specific state.

## Inferencer

```nspl
CREATE [IF NOT EXISTS] [ATTACHED|DETACHED] INFERENCER <name>
  FROM <input> [WHERE <expr>], ...
  [TO <output> [SET <output>.<field> = <expr>, ...] [WHERE <expr>]]
  [TO <output> ...]
  BRANCHED BY <branch>
  USING RESOURCE <resource> [VERSION <n>]
  FILE '<model.onnx>'
  INPUTS {
    "<onnx_input_name>" = <input>.<field>,
    ...
  }
  OUTPUTS {
    "<onnx_output_name>" = <output>.<field>,
    ...
  }
  FLUSH EACH <duration> MAX BATCH SIZE <bytes> | FLUSH IMMEDIATE
  ON MESSAGE ERROR <policy>;
```

An inferencer declares a branch-local ONNX model execution node. The model file is loaded from a versioned `RESOURCE`; if `VERSION` is omitted, Nervix resolves the latest uploaded resource version.

The optional `FILTER WHERE` clause runs before tensor construction. `INPUTS` maps ONNX input tensor names to fields on one of the declared input relays after that arrival filter. `OUTPUTS` maps ONNX output tensor names to fields on the output relay. Inferencers do not pass input fields through automatically; every required output field must come from `OUTPUTS` or a route-level `SET`.

Inferencers preserve the upstream branch exactly as received. Runtime ONNX execution is still under implementation; the control plane currently validates the resource/file reference and schema field mappings before accepting the model.

## Deduplicator

```nspl
CREATE [IF NOT EXISTS] [ATTACHED|DETACHED] DEDUPLICATOR <name>
  FROM <input> [WHERE <expr>], ...
  [TO <output> [SET <output>.<field> = <expr>, ...] [UNSET <input>.<field>, ...] WHERE <expr>]
  [TO <output> [SET <output>.<field> = <expr>, ...] [UNSET <input>.<field>, ...]]
  BRANCHED BY <branch>
  DEDUPLICATE ON <expr>[, <expr> ...]
  MAX TIME <duration>
  FLUSH EACH <duration> MAX BATCH SIZE <bytes> | FLUSH IMMEDIATE
  ON MESSAGE ERROR <policy>;
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
  [TO <output> [SET <output>.<field> = <expr>, ...] [UNSET <input>.<field>, ...] WHERE <expr>]
  [TO <output> [SET <output>.<field> = <expr>, ...] [UNSET <input>.<field>, ...]]
  BRANCHED BY <branch>
  BY <expr>, <expr>, ...
  MAX TIME <duration>
  FLUSH EACH <duration> MAX BATCH SIZE <bytes> | FLUSH IMMEDIATE
  ON MESSAGE ERROR <policy>;
```

A reorderer buffers records for one concrete branch and emits them sorted by the `BY` expression list. Each `BY` expression is evaluated with the same VM expression surface as filter-map programs, so field references and built-ins such as `lower(...)` are valid ordering keys.

Ordering is ascending. When two records have identical `BY` keys, arrival order is the tie-breaker.

`MAX TIME` bounds how long the oldest buffered record may wait. `FLUSH EACH` also creates a periodic flush boundary; `FLUSH IMMEDIATE` sorts and emits each received batch immediately. Reorderers preserve the upstream branch and do not support `ON GENERAL ERROR`.

`DESCRIBE REORDERER <name>` reports the scheduled owner and replicas, input/output relays, ordering expression list, max time, flush policy, filter-map presence, branch-local execution marker, state persistence markers, and runtime metrics when available.

## Correlator

```nspl
CREATE [IF NOT EXISTS] [ATTACHED|DETACHED] CORRELATOR <name>
  LEFT FROM <left_input> [WHERE <expr>]
  [LEFT FROM <left_input> [WHERE <expr>] ...]
  RIGHT FROM <right_input> [WHERE <expr>]
  [RIGHT FROM <right_input> [WHERE <expr>] ...]
  CORRELATE WHERE <left_right_predicate>
  MATCH EARLIEST | LATEST
  [TO <output> WHERE <expr>]
  [TO <output>]
  BRANCHED BY <branch>
  FLUSH EACH <duration> MAX BATCH SIZE <bytes> | FLUSH IMMEDIATE
  OUTPUT
    <output>.<field> = <expr>,
    ...
  MAX TIME <duration>
  ON CORRELATION TIMEOUT <left_action>, <right_action>
  ON MESSAGE ERROR <policy>;
```

A correlator stores unmatched records in branch-local pending state and matches a left/right pair when the `CORRELATE WHERE` predicate evaluates to true against both input records. Relays declared on the same side must share that side's schema; the left and right sides may use different schemas. Source-level `WHERE` clauses apply only to the relay they follow. The predicate must compile to `BOOLEAN`. `MATCH EARLIEST` keeps the first pending record on a side for a matching predicate and acknowledges later same-side duplicates. `MATCH LATEST` replaces the pending same-side record and acknowledges the replaced one.

When a left/right pair matches, the pair is removed from pending state and an output record is produced. Correlators do not implicitly copy any input fields into the output. The `OUTPUT` block must explicitly assign every required field on the output relay schema; optional output fields may be omitted.

`MAX TIME` is evaluated against the domain clock and bounds how long an unmatched record can remain pending. `ON CORRELATION TIMEOUT` has one action for the left input and one for the right input. `DROP` acknowledges and forgets the record. `SEND TO <relay>` forwards the original unmodified record to another schema-compatible relay and acknowledges it after the send succeeds.

Correlators preserve the upstream branch. Each concrete branch gets separate pending input state and an output buffer, even when the two inputs receive interleaved records that would match the same predicate in other branches.

## WASM Processor

```nspl
CREATE [IF NOT EXISTS] [ATTACHED|DETACHED] WASM PROCESSOR <name>
  USING RESOURCE <resource> [VERSION <n>]
  FILE '<path>'
  FROM <input> [WHERE <expr>], ...
  [FILTER WHERE <expr>]
  [TO <output> [SET <output>.<field> = <expr>, ...] [WHERE <expr>]]
  [TO <output> ...]
  BRANCHED BY <branch>
  ON MESSAGE ERROR <policy>
  ON GLOBAL ERROR <policy>;
```

```nspl
DESCRIBE WASM PROCESSOR <name>;
```

A WASM processor loads a native `wasm32-unknown-unknown` module from a Nervix resource and runs one guest instance per concrete branch. Source `FROM ... WHERE` runs first, then `FILTER WHERE` runs before the guest receives a row. Multiple `FROM` relays are consumed as one same-schema stream; the guest still sees a single source schema. The guest receives one CBOR envelope containing an Arrow IPC input batch and an ACK sidecar. Each output envelope names a declared `TO` relay and provides one destination-aligned column descriptor per field: either an unchanged input-column reference or a guest-generated single-column Arrow IPC stream. Nervix reconstructs the exact destination batch, then applies that route's `SET` and `WHERE` clauses.

See [WASM Processor Guests](wasm-processor-guests.md) for the guest ABI and Rust/Go authoring examples.

The declared `BRANCHED BY` branch must match the input and output relays. WASM processors preserve the upstream branch group exactly as received; they do not consume from a mixed logical relay and they do not fan in records across branches. WASM processors do not inherit input fields implicitly: the guest must explicitly select every destination column. An input reference reuses retained host Arrow arrays without guest-side copying, and its row's `source_token` selects both the referenced source row and the original record visible through `input.field`. Route `SET` clauses may read guest output fields through the destination relay namespace, and may read that selected source row through the `input` namespace, for example `TO out1 SET out1.name = lower(out1.name), out1.surname = input.surname`. A generated row without a source token cannot use `input.field`. `UNSET` is not valid on WASM output routes.

The host initializes each branch instance with CBOR metadata:

- domain name
- domain type
- branch key
- input relay schema
- output relay schemas

Guest state is branch-local processor state. The host saves it through the guest `nervix_dump_state` export and restores it through `nervix_load_state` when a branch instance is recreated. Nervix persists and replicates those guest-owned bytes.

ACK tokens and retained input-column sources are different: they are host-local hot-path runtime state and are not persisted or replicated. A guest may carry tokens from input to output, explicitly ack/nack them through the sidecar, and reference a retained input row while its source token is live, but the actual external ACK/NACK operation is always performed by the host. If ACK state and retained Arrow batches are lost with a processor owner, restored references are rejected and the upstream ingestor reacts according to its delivery mode and retry policy.

Flush is guest-controlled for WASM processors. Nervix calls the guest `process` export when input arrives and forwards any batches returned by the guest. The guest may also request a domain-clock timeout through the host timeout import. When the timeout fires, the host calls the guest timeout export and forwards any emitted batches through the same ACK sidecar path as normal processing.

`DESCRIBE WASM PROCESSOR` reports the scheduled owner and replicas, input/output relays, resource, resource version, file, guest-controlled flush marker, branch-local marker, persistent/replicated state markers, and runtime metrics when available.

## Window Processor

```nspl
CREATE [IF NOT EXISTS] [ATTACHED|DETACHED] WINDOW PROCESSOR <name>
  FROM <input> [WHERE <expr>], ...
  [TO <output> [SET <output>.<field> = <expr>, ...] [WHERE <expr>]]
  [TO <output> ...]
  BRANCHED BY <branch>
  WIDTH [<n> MESSAGES] [<duration> DURATION]
  STEP [<n> MESSAGES] [<duration> DURATION]
  AGGREGATE
    <output>.<field> = <aggregate_expr>,
    ...
  ON MESSAGE ERROR <policy>;
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

Aggregate expressions are not normal filter-map expressions. Aggregate functions are only valid as top-level aggregate values or as elements inside aggregate arrays:

```nspl
AGGREGATE
  summaries.tenant = FIRST(metrics.tenant),
  summaries.sample_count = COUNT(metrics.latency),
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

Aggregate calls declare runtime state by implication. Nervix first walks the `AGGREGATE` expression tree and extracts the set of demanded aggregation structures. It then deduplicates identical demands before creating branch-local accumulators.

`DESCRIBE WINDOW PROCESSOR` exposes that deduplicated demand set. The output includes the scheduled owner and replicas, processor relays, window bounds, branch-local execution marker, aggregate structure count, and one entry per demanded structure with:

- aggregate function
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
  [TO <relay> [SET <relay>.<field> = <expr>, ...] [UNSET <input>.<field>, ...] WHERE <expr>]
  [TO <relay> [SET <relay>.<field> = <expr>, ...] [UNSET <input>.<field>, ...]]
  BRANCHED BY by_reingested_tenant
  FLUSH EACH <duration> MAX BATCH SIZE <bytes> | FLUSH IMMEDIATE
  ON MESSAGE ERROR <policy>;
```

For reingestors, `FLUSH EACH <duration> MAX BATCH SIZE <bytes>` declares when the processor flushes buffered output downstream. `FLUSH IMMEDIATE` republishes each received batch without waiting.

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
