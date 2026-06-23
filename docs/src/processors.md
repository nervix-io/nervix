# Runtime Nodes

Nervix supports several runtime node types that transform or route records between relays.

Processing nodes operate on one parameter group at a time. This keeps tenant-specific, user-specific, or other group-specific state isolated while records move through the graph. A processor receives branch-local relay batches and keeps branch-local buffers or state unless its node type explicitly crosses a branch boundary.

Every normal processor that consumes relays must declare `PARAMETERIZED BY <schema>`. That schema must match the input and output relays. The declaration is a contract: the processor is materialized once per concrete branch and must not consume the mixed logical relay. Reingestors are different because they declare `PARAMETERIZED BY <schema> VALUES { ... }` to create a new downstream branch key. Emitters are terminal drains and do not declare processor parameterization.

`GENERATOR` is a periodic source runtime node.

Relay-consuming runtime nodes support `ATTACHED` or `DETACHED` branch semantics. `GENERATOR` has no upstream branch to attach to and therefore does not declare a branch mode.

- `ATTACHED` keeps the upstream ACK chain open
- `DETACHED` drops ACK state for that branch immediately

Pure runtime processors end with `ON MESSAGE ERROR <policy>` only. `ON GENERAL ERROR` is reserved for `INGESTOR` and `EMITTER`, because those are the runtime nodes that own external-system failures.

## Branch Ownership

Branch behavior by node type:

- `INGESTOR` starts a branch
- `ROUTER` preserves that branch while fanning records out to multiple downstream relays
- `DEDUPLICATOR`, `REORDERER`, `CORRELATOR`, and `UNIFIER` run inside that branch under one concrete parameter group
- `WASM PROCESSOR` runs inside that branch under one concrete parameter group
- `WINDOW PROCESSOR` keeps window state inside that branch under one concrete parameter group
- `REINGESTOR` is the boundary that consumes from the whole input relay and starts new downstream branches
- `EMITTER` is the terminal mixed consumer that drains a relay to an external sink

Processors between ingestor and reingestor/emitter are scoped to one parameter group. The isolation covers both the relay batches they handle and the state they keep.

Every flush-based runtime node must declare either `FLUSH EACH <duration> MAX BATCH SIZE <bytes>` or `FLUSH IMMEDIATE` in its NSPL definition. `FLUSH EACH` uses the duration as the local batch boundary: the node buffers input under one concrete branch group and forwards it downstream on that cadence. `FLUSH IMMEDIATE` forwards each received batch without waiting for a flush deadline. Emitters also declare `FLUSH` and use it to collect a terminal output batch before publishing externally. Window processors use `WIDTH` and `STEP` instead. WASM processors do not declare a flush policy: output emission is controlled by the guest through `process` output and guest-requested timeouts.

## Filter-Map Programs

Processors may also carry an optional filter-map clause after their main definition:

```nspl
[SET <relay>.<field> = <expr>, ...]
[UNSET <relay>.<field>, ...]
[WHERE <expr>]
```

When more than one filter-map block is present, the grammar order is `SET`, then `UNSET`, then `WHERE`. The order keeps processor declarations consistent; identifiers in each expression are still validated against the processor's input scope.

This surface is available on:

- `UNIFIER`
- `DEDUPLICATOR`
- `REINGESTOR`
- `ROUTER`
- `REORDERER`

The clause is a row-level filter-map over the processor output:

- `WHERE` drops rows
- `SET` rewrites or appends fields
- `UNSET` removes fields from the downstream shape

Schema validation is applied at statement time against the processor's effective output schema after `SET` and `UNSET`.

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

## Router

```nspl
CREATE [IF NOT EXISTS] [ATTACHED|DETACHED] ROUTER <name>
  FROM <input>
  [SET <relay>.<field> = <expr>, ...]
  [UNSET <relay>.<field>, ...]
  [WHERE <expr>]
  [TO <output> WHERE <expr> ...]
  [MATCH FIRST|ALL]
  DEFAULT TO <output>
  PARAMETERIZED BY <schema>
  FLUSH EACH <duration> MAX BATCH SIZE <bytes> | FLUSH IMMEDIATE
  ON MESSAGE ERROR <policy>;
```

A router evaluates rows from one input relay and forwards each surviving row into downstream relays.

The optional filter-map clause runs before route selection. After that, each `TO ... WHERE ...` branch is tested in order and `DEFAULT TO ...` handles the remaining rows. `MATCH ALL` forwards a row to every matching route and is the default when conditional routes are present. `MATCH FIRST` forwards a row only to the first matching route. A router with only `DEFAULT TO` is the single-output pass-through/projection form.

Router outputs keep the upstream branch group exactly as received.

`DESCRIBE ROUTER <name>` reports the scheduled owner and replicas, input relay, default output relay, match policy, route conditions, flush policy, filter-map presence, branch-local execution marker, and runtime metrics when available.

Typical use cases:

- reshape a relay into a smaller downstream schema
- apply a `WHERE` filter before materialization or emitting
- normalize fields with `SET` and remove staging fields with `UNSET`
- make a processor boundary explicit without changing branch grouping

Example:

```nspl
CREATE SCHEMA user_branch (
  tenant STRING,
  user_id U32
);

CREATE [IF NOT EXISTS] ROUTER project_notifications
  FROM notifications
  SET notifications.normalized = lower(trim(notifications.raw)), notifications.amount = notifications.amount + 1
  UNSET notifications.raw, notifications.active
  WHERE notifications.active
  DEFAULT TO projected_notifications
  PARAMETERIZED BY user_branch
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
```

That example keeps the existing branch grouping, drops inactive rows, rewrites `normalized` and `amount`, removes `raw` and `active`, and forwards the surviving rows into `projected_notifications`.

Branch-preserving processor filter-map programs can also read the current parameter group through `branch.<key>`. For example, `SET notifications.tenant = branch.tenant WHERE branch.tenant = notifications.tenant` copies and tests the branch key without requiring the key to be present in the message payload.

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

## Unifier

```nspl
CREATE [IF NOT EXISTS] [ATTACHED|DETACHED] UNIFIER <name>
  FROM <input>, <input>, ...
  TO <output>
  PARAMETERIZED BY <schema>
  FLUSH EACH <duration> MAX BATCH SIZE <bytes> | FLUSH IMMEDIATE
  [SET <relay>.<field> = <expr>, ...]
  [UNSET <relay>.<field>, ...]
  [WHERE <expr>]
  ON MESSAGE ERROR <policy>;
```

A unifier merges multiple upstream relays into one output relay.

Unification happens per aligned parameter group. Each group has its own unifier execution and state.

Use it when multiple sources should feed a common downstream path.

## Inferencer

```nspl
CREATE [IF NOT EXISTS] [ATTACHED|DETACHED] INFERENCER <name>
  FROM <input>
  TO <output>
  PARAMETERIZED BY <schema>
  USING RESOURCE <resource> [VERSION <n>]
  FILE '<model.onnx>'
  [SET <relay>.<field> = <expr>, ...]
  [WHERE <expr>]
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

The optional filter-map clause runs before tensor construction. `INPUTS` maps ONNX input tensor names to fields on the input relay after that filter-map shape is applied. `OUTPUTS` maps ONNX output tensor names to fields on the output relay.

Inferencers preserve the upstream parameter group exactly as received. Runtime ONNX execution is still under implementation; the control plane currently validates the resource/file reference and schema field mappings before accepting the model.

## Deduplicator

```nspl
CREATE [IF NOT EXISTS] [ATTACHED|DETACHED] DEDUPLICATOR <name>
  FROM <input>
  TO <output>
  PARAMETERIZED BY <schema>
  DEDUPLICATE ON <expr>[, <expr> ...]
  MAX TIME <duration>
  FLUSH EACH <duration> MAX BATCH SIZE <bytes> | FLUSH IMMEDIATE
  [SET <relay>.<field> = <expr>, ...]
  [UNSET <relay>.<field>, ...]
  [WHERE <expr>]
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
  FROM <input>
  TO <output>
  PARAMETERIZED BY <schema>
  BY <expr>, <expr>, ...
  MAX TIME <duration>
  FLUSH EACH <duration> MAX BATCH SIZE <bytes> | FLUSH IMMEDIATE
  [SET <relay>.<field> = <expr>, ...]
  [UNSET <relay>.<field>, ...]
  [WHERE <expr>]
  ON MESSAGE ERROR <policy>;
```

A reorderer buffers records for one concrete branch and emits them sorted by the `BY` expression list. Each `BY` expression is evaluated with the same VM expression surface as filter-map programs, so field references and built-ins such as `lower(...)` are valid ordering keys.

Ordering is ascending. When two records have identical `BY` keys, arrival order is the tie-breaker.

`MAX TIME` bounds how long the oldest buffered record may wait. `FLUSH EACH` also creates a periodic flush boundary; `FLUSH IMMEDIATE` sorts and emits each received batch immediately. Reorderers preserve the upstream parameter group and do not support `ON GENERAL ERROR`.

`DESCRIBE REORDERER <name>` reports the scheduled owner and replicas, input/output relays, ordering expression list, max time, flush policy, filter-map presence, branch-local execution marker, state persistence markers, and runtime metrics when available.

## Correlator

```nspl
CREATE [IF NOT EXISTS] [ATTACHED|DETACHED] CORRELATOR <name>
  FROM <left_input>, <right_input>
  ON (<left_expr>, ...), (<right_expr>, ...)
  MATCH EARLIEST | LATEST
  TO <output>
  PARAMETERIZED BY <schema>
  FLUSH EACH <duration> MAX BATCH SIZE <bytes> | FLUSH IMMEDIATE
  OUTPUT
    <output>.<field> = <expr>,
    ...
  MAX TIME <duration>
  ON CORRELATION TIMEOUT <left_action>, <right_action>
  ON MESSAGE ERROR <policy>;
```

A correlator stores unmatched records in a branch-local key map. The left and right key groups must contain the same number of expressions, and corresponding expressions must compile to exactly the same type. `MATCH EARLIEST` keeps the first pending record on a side for a key and acknowledges later same-side duplicates. `MATCH LATEST` replaces the pending same-side record and acknowledges the replaced one.

When both sides are present for a key, the pair is removed from the pending map and an output record is produced. Correlators do not implicitly copy any input fields into the output. The `OUTPUT` block must explicitly assign every required field on the output relay schema; optional output fields may be omitted.

`MAX TIME` is evaluated against the domain clock and bounds how long an unmatched record can remain pending. `ON CORRELATION TIMEOUT` has one action for the left input and one for the right input. `DROP` acknowledges and forgets the record. `SEND TO <relay>` forwards the original unmodified record to another schema-compatible relay and acknowledges it after the send succeeds.

Correlators preserve the upstream parameter group. Each concrete branch gets a separate pending map and output buffer, even when the two inputs receive interleaved records with identical correlation keys.

## WASM Processor

```nspl
CREATE [IF NOT EXISTS] [ATTACHED|DETACHED] WASM PROCESSOR <name>
  USING RESOURCE <resource> [VERSION <n>]
  FILE '<path>'
  FROM <input>
  TO <output>
  PARAMETERIZED BY <schema>
  ON MESSAGE ERROR <policy>
  ON GLOBAL ERROR <policy>;
```

```nspl
DESCRIBE WASM PROCESSOR <name>;
```

A WASM processor loads a native `wasm32-unknown-unknown` module from a Nervix resource and runs one guest instance per concrete branch. The guest receives Arrow IPC record batches plus an ACK sidecar, and it emits Arrow IPC batches back to the configured output relay.

See [WASM Processor Guests](wasm-processor-guests.md) for the guest ABI and Rust/Go authoring examples.

The declared `PARAMETERIZED BY` schema must match the input and output relays. WASM processors preserve the upstream branch group exactly as received; they do not consume from a mixed logical relay and they do not fan in records across branches.

The host initializes each branch instance with CBOR metadata:

- domain name
- domain type
- branch key
- input relay schema
- output relay schema

Guest state is branch-local processor state. The host saves it through the guest `nervix_dump_state` export and restores it through `nervix_load_state` when a branch instance is recreated. Nervix persists and replicates those guest-owned bytes.

ACK tokens are different: they are host-local hot-path runtime capabilities and are not persisted or replicated. A guest may carry tokens from input to output, or explicitly ack/nack them through the sidecar, but the actual external ACK/NACK operation is always performed by the host. If ACK state is lost with a processor owner, the upstream ingestor reacts according to its delivery mode and retry policy.

Flush is guest-controlled for WASM processors. Nervix calls the guest `process` export when input arrives and forwards any batches returned by the guest. The guest may also request a domain-clock timeout through the host timeout import. When the timeout fires, the host calls the guest timeout export and forwards any emitted batches through the same ACK sidecar path as normal processing.

`DESCRIBE WASM PROCESSOR` reports the scheduled owner and replicas, input/output relays, resource, resource version, file, guest-controlled flush marker, branch-local marker, persistent/replicated state markers, and runtime metrics when available.

## Window Processor

```nspl
CREATE [IF NOT EXISTS] [ATTACHED|DETACHED] WINDOW PROCESSOR <name>
  FROM <input>
  TO <output>
  PARAMETERIZED BY <schema>
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

A window processor consumes one branch-local input relay and emits aggregate records into one branch-local output relay. It does not inherit input fields automatically. The `AGGREGATE` block fully defines the emitted record shape.

Window processors preserve the upstream parameter group exactly as received. Each concrete branch gets its own independent window state, and each window contains records from one branch group.

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
CREATE [IF NOT EXISTS] [ATTACHED|DETACHED] REINGESTOR <name>
  FROM <relay>
  TO <relay>
  PARAMETERIZED BY <schema> VALUES { <field> = <relay>.<field>, ... } TTL 5m
  FLUSH EACH <duration> MAX BATCH SIZE <bytes> | FLUSH IMMEDIATE
  [SET <relay>.<field> = <expr>, ...]
  [UNSET <relay>.<field>, ...]
  [WHERE <expr>]
  ON MESSAGE ERROR <policy>;
```

For reingestors, `FLUSH EACH <duration> MAX BATCH SIZE <bytes>` declares when the processor flushes buffered output downstream. `FLUSH IMMEDIATE` republishes each received batch without waiting.

A reingestor republishes records from one relay into another and starts downstream branches with a new parameter grouping.

This is the main tool for changing native branch grouping inside the Nervix graph.

A reingestor is an explicit branch boundary:

- it consumes across all concrete input branches of the source relay
- it computes a new branch group for each outgoing record
- it buffers rows under the new downstream branch group
- it starts or resolves downstream branches in the target relay

Reingestor `PARAMETERIZED BY ... VALUES { ... }` mappings can read either input relay fields or the source branch key. For example, `VALUES { tenant = branch.tenant }` preserves a tenant grouping even when the payload schema does not carry `tenant` as a row field.

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
