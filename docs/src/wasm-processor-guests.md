# WASM Processor Guests

WASM processors are native WebAssembly modules loaded by Wasmtime. They do not use WASI. A guest module exports a small C ABI, owns one reusable linear-memory buffer, and exchanges Arrow IPC record batches with Nervix through that buffer.

Rust guests use the [Rust WASM Guest SDK](./wasm-guest-sdk.md) rather than implementing this ABI by hand. This chapter is the authoritative wire contract that the SDK implements and that guests in other languages implement directly.

See [Choosing An Extension Tier](filter-map-functions.md#choosing-an-extension-tier) when deciding
between builtins, operator-trusted Roto UDFs, and a WASM processor.

The runtime creates one guest instance per concrete branch. Guest state must therefore be branch-local. Do not aggregate across branch keys inside the guest.

## Module Sharing And Branch Memory

The runtime compiles a WASM processor module once for the scheduled processor and reuses that
compiled module to instantiate branches. Each branch instance still has its own Wasmtime store,
linear memory, guest state, and timeout handles. Compiled code is shared. Mutable guest
memory is not.

Capacity therefore grows with the linear-memory pages dirtied by each live branch, not with a full
copy of compiled machine code per branch. See
[Capacity Planning For Branched Graphs](capacity-planning.md#per-branch-cost-structure).

WASM processor output flush is guest-controlled. `CREATE WASM PROCESSOR` does not accept `FLUSH EACH` or `FLUSH IMMEDIATE`; Nervix routes batches returned from `nervix_process_batch`, batches returned later from `nervix_on_timeout` callbacks requested by the guest, and batches released by `nervix_flush` when the host quiesces the branch, through the processor's declared `TO` clauses.

## Execution Limits

Every declaration requires both limits immediately after `FILE`, in this order:

```nspl,ignore
FILE "processor.wasm"
MAX FUEL 1000000000
MAX MEMORY 64MiB
```

`MAX FUEL` is the Wasmtime instruction-fuel budget for one logical guest operation. Nervix resets
the store to that budget before initialization, each input batch, each timeout callback, a
quiesce flush, and each state save, load, or reset. All guest ABI calls made as part of that
operation share the budget: for example, input-buffer allocation, `nervix_process_batch`, global
error inspection, and every subsequent `nervix_read_emit` call are one batch-processing budget.
Module instantiation also receives one budget. Fuel measures guest WebAssembly execution, not
wall-clock time or host-side work.

`MAX MEMORY` is the Wasmtime linear-memory ceiling for each branch store. It applies to the total
initial size of all module memories and every later `memory.grow`; choose a value large enough for
the module's initial pages and reusable ABI buffer. It does not include shared compiled code,
host-side Arrow or FlatBuffer data, or Wasmtime store bookkeeping.

Fuel or memory exhaustion is handled by the processor's node-wide `ON GLOBAL ERROR` policy. Nervix
discards that concrete branch's trapped guest instance; later work may instantiate a replacement
from the last replicated guest snapshot. Other branch instances are independent and continue
running. There is no separate WASM wall-clock timeout clause.

Before every guest operation, Nervix supplies one explicit domain execution snapshot to the WASM
host. Initialization, input processing, a timeout callback, quiesce flush, and state save, load, or
reset each receive the snapshot of their owning execution. Every call to
`nervix_domain_time_nanos()` during that operation returns exactly that value. The host exposes no
context-free guest invocation and the module cannot choose or read host wall time. Wasmtime epoch
yielding and fuel enforcement remain physical execution safeguards and do not change guest-visible
domain time.

## Contract Summary

The guest imports host functions from the `env` module:

```text
nervix_domain_time_nanos() -> i64
nervix_timeout_after_nanos(delay_nanos: i64) -> i64
```

The guest must export:

```text
nervix_buffer_ptr() -> i32
nervix_buffer_len() -> i32
nervix_buffer_capacity() -> i32
nervix_alloc(size: i32) -> i32
nervix_init(ptr: i32, size: i32) -> i32
nervix_current_domain_time_nanos() -> i64
nervix_process_batch(ptr: i32, size: i32) -> i32
nervix_on_timeout(handle: i64) -> i32
nervix_flush() -> i32
nervix_read_emit() -> i32
nervix_dump_state() -> i32
nervix_load_state(ptr: i32, size: i32) -> i32
nervix_reset_state() -> i32
```

Return `0` from fallible functions on success. Return a negative code on guest rejection. Nervix treats negative codes as runtime errors and applies the processor error policy. `nervix_dump_state` returns the size of the snapshot it wrote, or a negative code when it cannot serialize its state; Nervix then keeps the state saved last.

`nervix_load_state` has two reserved codes that classify the saved state itself:

| Code | Verdict |
| --- | --- |
| `-7` | The saved bytes are not a snapshot envelope the guest can decode. |
| `-8` | The guest decoded the snapshot envelope and refuses the application state it carries. |

Only these two codes are a rejection of the saved state. A trap, an exhausted `MAX FUEL` or
`MAX MEMORY`, or any other negative code while restoring is a failure of the restore itself and says
nothing about the saved bytes. Nervix keeps the saved state after every failed restore, including a
rejection, and reports each outcome as a distinct [failure stage](#failure-diagnostics).

## Buffer Ownership

The guest owns one reusable byte buffer in linear memory.

1. Host estimates the required FlatBuffer capacity and calls `nervix_alloc(capacity)`.
2. Guest ensures its buffer can hold `capacity` bytes and returns its current pointer. The buffer
   may move on every call.
3. Host builds the FlatBuffer directly in that guest-memory range.
4. Because FlatBuffers builds backwards, the completed message may occupy a suffix of the
   allocation. Host calls `nervix_process_batch(ptr, size)` with that exact range.
5. Guest writes pending output or state back into the same buffer.
6. Host calls `nervix_read_emit()` or `nervix_dump_state()` and reads the returned byte length from `nervix_buffer_ptr()`.

If the capacity estimate is too small, the host finishes the message in temporary spill storage,
releases its linear-memory borrow, calls `nervix_alloc` again with a larger capacity, and copies the
finished message once. Reallocation is never attempted while the host holds a guest-memory slice.
The host enforces a maximum guest buffer size. A guest must still validate the supplied pointer and
size against its current buffer and reject impossible ranges.

## Init Payload

`nervix_init` receives the `BranchInit` variant of the size-prefixed FlatBuffers `Message` union.
`output_schemas` contains one schema per declared `TO` relay. A guest output envelope must name one
of those relays and provide one destination-aligned column descriptor per field before Nervix
applies the route-level `SET` and `WHERE` clauses. WASM routes are set-only: they reject `INHERIT`
and `UNSET`, and construction does not expose `message` or `input`. Guest-generated columns form an
immutable base independently visible to every route. The authoritative cross-language schema is
[`crates/nervix-wasm-protocol/schema/nervix_wasm.fbs`](https://github.com/nervix-io/nervix/blob/main/crates/nervix-wasm-protocol/schema/nervix_wasm.fbs).

```text
{
  "domain_name": text,
  "domain_type": text,
  "branch_key": bytes,
  "input_schema": {
    "name": text,
    "fields": [
      { "name": text, "ty": WasmProcessorType, "optional": bool }
    ]
  },
  "output_schemas": [
    {
      "name": text,
      "fields": [
        { "name": text, "ty": WasmProcessorType, "optional": bool }
      ]
    }
  ]
}
```

`domain_type` is currently a descriptive host string. Guests should not branch on
it unless they own a strict compatibility rule for the exact Nervix version they
are targeting.

`branch_key` is the serialized concrete branch key for this instance. An
explicit `UNBRANCHED` relay is still represented by one concrete branch
key.

`ProcessorTypeKind` is a FlatBuffers enum with these scalar variants:

```text
"U8" | "I8" | "U16" | "I16" | "U32" | "I32" | "U64" | "I64"
| "Bool" | "String" | "Datetime" | "F32" | "F64"
```

Container variants use the nested `ProcessorType.element` table; arrays also
set `array_len`:

```text
ProcessorType { kind: Array, element: ProcessorType, array_len: u32 }
ProcessorType { kind: Vec, element: ProcessorType }
```

`Datetime` values are Arrow `Timestamp(Nanosecond)` values at the wire boundary.
Treat nanosecond integers as a boundary format and convert them to your guest's
typed timestamp representation immediately after decoding.

Treat this as configuration for the branch instance. Store what you need in guest state; reject it only when the processor cannot run correctly.

## Batch Envelope

Every input, output, init, or bundled guest-state payload is one size-prefixed
FlatBuffer. Its root is the `Message` union and its file identifier is `NVWX`.
The ABI size and internal size prefix must agree exactly. Arrow IPC payloads
are FlatBuffers byte vectors. Generated Rust and Go accessors return slices
into the FlatBuffer, avoiding a deserialization copy. Crossing WebAssembly
linear memory still requires writing the source bytes once. On the normal host-to-guest path,
FlatBuffers writes them directly into their final guest-memory representation instead of creating
and then copying a complete host-side FlatBuffer. After a guest emit, the host keeps the generated
Arrow vector as a shared slice of that retained FlatBuffer.

Input envelopes have this shape:

```text
{
  "kind": "input",
  "arrow_ipc_batch": bytes,
  "acks": AckSidecar
}
```

Output envelopes have this shape:

```text
{
  "kind": "output",
  "generated_arrow_ipc_batch": bytes,
  "outputs": [
    {
      "output_relay": text,
      "columns": [
        { "kind": "input", "column_index": u32 },
        { "kind": "generated", "column_index": u32 },
        { "kind": "uninitialized" }
      ],
      "acks": AckSidecar
    }
  ]
}
```

Each routed output's `columns` entries correspond positionally to its
destination fields. An `input` column references the declared processor input
schema by index; source and destination types and nullability must match
exactly, although their names may differ. A `generated` column references the
common generated Arrow batch by index. Index namespaces are determined by the
variant. An `uninitialized` descriptor is encoded as FlatBuffers
`ColumnSource.Uninitialized` with canonical `column_index = 0`. It has no input
or generated-pool index; its type and nullability come from the positionally
aligned destination field, and its row count comes from `acks.rows.len()`.

`generated_arrow_ipc_batch` is either an empty byte string or exactly one Arrow
IPC stream containing one schema and one record batch. The empty byte string is
the only valid empty generated pool; do not encode a zero-column Arrow stream.
When present, generated schema field names must be empty. Nervix compares every
other field property with each referencing destination field, including data
type, nullability, timestamp units and timezones, nested types, fixed lengths,
and semantic field metadata. Destination schemas remain authoritative for field
names.

Generated columns are immutable and reusable. Several routes, or several
fields in one route, may reference the same generated index. Nervix decodes the
common batch once and clones the same `ArrayRef`; it does not copy, decode, or
serialize that column again. Every generated pool column must be referenced at
least once.

Rows are positional within one output group. For routed row `R`, a generated
reference reads generated array row `R`, while an input reference reads the
host input row selected by that routed row's `source_token`. Every route that
references the generated pool must therefore have the pool's row count and use
the same generated row order. Routes needing different generated counts,
ordering, guest-side filtering, or duplication must be queued as separate
output envelopes and returned by separate positive `nervix_read_emit()` calls.
Generated indexes never cross an envelope boundary. Route-level `SET` and
`WHERE` processing occurs after this materialization and does not prevent
sharing.

Uninitialized columns pass through route processing as explicit VM state. Any
expression read materializes a typed all-NULL column before applying ordinary
NULL semantics, so `coalesce(uninitialized, value)` yields `value` and
`is_null(uninitialized)` yields true. At the node boundary, an uninitialized
optional field becomes typed NULLs, while an uninitialized required field is a
schema error. The marker is not part of a Nervix relay schema and never crosses
a relay, IPC, persistence, interconnect, or node boundary. An uninitialized
descriptor does not by itself require `source_token`.

The ACK sidecar is:

```text
{
  "rows": [
    { "tokens": [u64, ...], "source_token": u64 | null }
  ],
  "acked": [
    { "tokens": [u64, ...] }
  ],
  "nacked": [
    { "tokens": [u64, ...], "reason": text }
  ],
  "message_errors": [
    { "tokens": [u64, ...], "reason": text }
  ]
}
```

For every host input row, Nervix issues one token and sets both `tokens` and
`source_token` to that token. Preserve the complete row sidecar when filtering
or enriching. `source_token` is an optional FlatBuffers scalar and is absent
only for a generated row that has no input source.

If an output envelope contains any `input` column, every output row must have a
non-null, live `source_token`, and that token must occur in the row's `tokens`.
It selects the retained host input row used for every referenced column in that
output row. It also selects the original record exposed through route
expressions such as `input.field`. A source token does not add an ACK use.

If the guest drops an input row, put that row's token set in `acked`. To fail it
directly without invoking the processor message error policy, put it in
`nacked` with a reason.

Use `message_errors` for per-message guest errors that must be handled through `ON MESSAGE ERROR` (`IGNORE`, `LOG`, or `SEND TO`). Global errors are not part of the ACK sidecar because they are guest/node state, not message lineage.

Guests may expose this optional global-error channel:

```text
nervix_global_error_ptr() -> i32
nervix_global_error_len() -> i32
nervix_clear_global_error() -> i32
```

If any of these exports exists, all three must exist. After host calls into the guest (`nervix_init`, `nervix_process_batch`, `nervix_on_timeout`, `nervix_flush`, and emit reads), and after a negative result from `nervix_dump_state`, `nervix_load_state`, or an emit read, it checks `nervix_global_error_len()`. A positive length means `nervix_global_error_ptr()` points at UTF-8 error bytes. The host reads the bytes, calls `nervix_clear_global_error()`, and reports them as the reason for the failure. Failures of initialization, restore, input processing, timeout callbacks, quiesce flush, and output validation apply `ON GLOBAL ERROR`; a failure to save, persist, or replicate guest state is always reported. Wasmtime call failures and traps are also handled as global processor errors.

The guest decides lineage; the host performs the actual ACK/NACK operation. Tokens are host-local hot-path capabilities. They are valid only while the current host instance is alive, and they are never persisted.

Each routed output retains its own sidecar because routes may carry different
row lineage or terminal decisions. The host counts carried token uses across
all routed outputs in the callback, so fan-out completes the original input ACK
only after every downstream delivery completes. A terminal decision may occur
only once across the validated callback.

The sidecars must be internally consistent:

- `rows.len()` is the routed output row count and must equal the generated pool row count when that route references a generated column.
- an uninitialized descriptor uses `rows.len()` directly and must use canonical `column_index = 0`.
- every token in `rows`, `acked`, `nacked`, and `message_errors` must come from the current host-provided input sidecar.
- a token may be carried into output rows, or terminally acked/nacked, but not both.
- a token may have at most one terminal decision across `acked`, `nacked`, and `message_errors`.
- a non-null `source_token` must be live and carried in its output row.

It is valid to carry the same input token into more than one emitted row,
routed output, or output group. The host keeps attached child guards and
resolves the original guard only after all derived deliveries complete. All
output groups from one callback are validated together; no output is dispatched
and no terminal decision is applied if any routed output is invalid.

For input references, Nervix retains the original input Arrow batch while its
tokens are live. Identity selections reuse the source `ArrayRef`, contiguous
selections use a buffer-sharing slice, and filtered, reordered, duplicated, or
cross-batch selections use host-side Arrow kernels. The guest never has to
serialize unchanged field values back to the host.

All table and vector fields are required, including empty vectors. Unknown
fields are ignored for FlatBuffers schema evolution; unknown union or enum
variants, missing required fields, a wrong identifier or size prefix, trailing
bytes, invalid column counts, bad source tokens, malformed or trailing Arrow
IPC, and exact-schema mismatches are global processor errors. Empty output
groups, out-of-range or unreferenced generated columns, and generated
row-layout mismatches are also rejected. Nonzero uninitialized column indexes
are rejected. CBOR and per-output generated-column
envelopes are not supported. There is no format negotiation, legacy decoder,
or fallback path. Rebuild every guest for this FlatBuffers contract.

## State

Use branch-local guest state for the durable computation state a recreated instance needs to
continue: counters, aggregates, open windows, or whatever else the guest derives from the input it
has already accepted.

Nervix saves guest state through `nervix_dump_state` after it dispatches the output of a successful
`nervix_process_batch` or `nervix_on_timeout` call, and when it checkpoints a branch for an
ownership handoff. It persists and replicates the returned bytes and restores them through
`nervix_load_state`, after `nervix_init`, when the branch instance is recreated.

A save that fails — a negative `nervix_dump_state` code, a trap, or an exhausted limit — is reported
as a `state snapshot` failure and leaves the state saved last in place. Nervix never replaces saved
state with the result of a failed save, so a recreated instance restores the last completed save.

Guest state holds computation state only. Everything an instance uses to execute belongs to that
instance and is never saved:

- input the guest still buffers, together with its ACK tokens, row sidecars, and input-column
  references;
- output groups the guest has not emitted yet;
- timeout handles, which the host issues per instance;
- error state the guest latched after a failed callback.

A recreated instance therefore starts without buffered input, pending output, error state, or
pending timeouts. A guest releases buffered input from `nervix_flush` before a handoff, and a guest
whose restored state needs a timer requests it again from its next input or timeout callback.

Keep `nervix_load_state` strict about what it restores. Save what a restore needs to validate the
state, such as the branch configuration it was taken under, and reject state that does not match
the instance it is handed to.

A zero-length `nervix_dump_state` result saves no state at all: the next instance is initialized
without a `nervix_load_state` call. A guest whose computation state can be empty therefore wraps it
in an envelope, as both example guests do with `GuestSnapshot`, so that empty computation state is
still restored as state.

A restore that fails leaves the saved state in place. The next input for that branch instantiates
the guest again and hands it the same saved state, so a guest that rejects its state keeps being
reported with the same saved state revision instead of silently starting fresh. Rejecting the
state with `-7` or `-8` is the only outcome that classifies the saved bytes as unusable; keep
`nervix_load_state` strict and use those codes only for a verdict on the state.

### State Generations

Every branch's guest state belongs to a generation, one lifetime of that state that the committed
schedule names. A save belongs to the generation that was current when the guest saved it, and
Nervix persists, replicates, serves, restores, and recovers saved state only in the generation the
committed schedule names for its branch. A snapshot of an earlier generation never becomes current
again, whatever revision it carries: a former owner that restarts with older saves, a replica that
missed a transition while it was offline, and a handoff prepared before the transition are all
fenced out.

A new WASM processor starts every branch in its first generation, and a planned ownership handoff
keeps the generation. When an owner is lost and its state is recovered without it, the forced
recovery starts a new generation for every branch of the processor in the same schedule
publication that names the new owner. The checkpoint the recovery selects for a branch continues in
that generation; a branch with no surviving checkpoint of the generation being replaced starts
fresh, and `SHOW CLUSTER STATUS` reports it as a `wasm_processor` reset. Applying the same committed
schedule again, after a restart or a rebuild, publishes nothing new.

A save from an instance whose generation or ownership has already moved on is refused before
anything is persisted or replicated, and is reported as a `state authority check` failure. Saves are
written to the node's state store on its storage workers, never on the worker that runs the guest.

ACK tokens are separate from guest state. They are host-local hot-path runtime capabilities and are not persisted or replicated. If ACK state is lost with a processor owner, the upstream ingestor reacts according to its delivery mode and retry policy.

## Timeouts

A guest can call `nervix_timeout_after_nanos(delay)` while processing. The request is anchored at
that operation's supplied domain snapshot, and `delay` is a logical duration. The host returns a
monotonically increasing handle for that branch instance. When the same bound domain and `START`
generation reach the requested instant, the host calls:

```text
nervix_on_timeout(handle)
```

After any successful timeout callback, the host repeatedly calls
`nervix_read_emit()` and forwards every emitted output group. Shared generated
columns work identically in timeout output. Input-column references remain
valid while their source token is live.

Timeout handles belong to the branch instance that requested them. A recreated
instance starts without pending timeouts and never receives a handle issued to
an earlier instance.

## Quiesce Flush

When Nervix quiesces a branch — to hand it to a replacement node during an `ENTITY_PAUSE`
alteration, or to shut it down — it calls:

```text
nervix_flush()
```

Emit every buffered output group during this call; the host drains them with `nervix_read_emit()`
exactly as it does after a timeout, and only then snapshots the guest through `nervix_dump_state`.
A guest that keeps input past `nervix_flush` leaves it unacknowledged until the branch resumes, so
a guest that buffers between calls must release that buffer here instead of waiting for more input
or for its next timeout. A guest that never buffers can return success without emitting.

This is what lets a WASM processor participate in `ENTITY_PAUSE` like every other stateful node:
the host pauses intake at the relay gate, asks the guest to flush, snapshots it, and restores that
snapshot into the replacement instance.

## Rust SDK

The `nervix-wasm-sdk` crate owns the complete exported ABI surface — the
reusable buffer, envelope encoding and decoding, the emit queue, the
global-error channel, panic conversion, error-state latching, and
`GuestSnapshot` plumbing — and exposes the typed `Processor` trait instead.
See [Rust WASM Guest SDK](./wasm-guest-sdk.md) for installation and usage, and
`examples/wasm-processors/rust-guest` for the complete reference guest.

## Go Sketch

The prototype Go guest is in `examples/wasm-processors/go-guest` and is built with TinyGo:

```go
//go:wasmimport env nervix_domain_time_nanos
func hostDomainTimeNanos() int64

//go:wasmimport env nervix_timeout_after_nanos
func hostTimeoutAfterNanos(delayNanos int64) int64

var buffer []byte
var pendingEmit [][]byte

//export nervix_alloc
func nervixAlloc(size int32) int32 {
    if size < 0 {
        return -1
    }
    if cap(buffer) < int(size) {
        buffer = make([]byte, int(size))
    } else {
        buffer = buffer[:int(size)]
        clear(buffer)
    }
    return int32(uintptr(unsafe.Pointer(&buffer[0])))
}

//export nervix_process_batch
func nervixProcessBatch(ptr int32, size int32) int32 {
    input, code := readBufferRange(ptr, size)
    if code != 0 {
        return code
    }
    envelope, code := decodeEnvelope(input)
    if code != 0 {
        return code
    }
    _ = hostTimeoutAfterNanos(1_000_000_000)
    pendingEmit, code = buildOutputEnvelopes(envelope)
    return code
}

//export nervix_read_emit
func nervixReadEmit() int32 {
    if len(pendingEmit) == 0 {
        return 0
    }
    next := pendingEmit[0]
    pendingEmit = pendingEmit[1:]
    buffer = append(buffer[:0], next...)
    return int32(len(buffer))
}
```

The Go guest uses bindings generated from `nervix_wasm.fbs`. FlatBuffers byte
vectors are exposed as slices backed by the input buffer, so parse Arrow IPC
directly from those slices while the ABI buffer remains alive. Guest-side
domain structs should stay explicit before they are passed to the generated
builders:

```go
type ackSidecar struct {
    Rows          []outputRow
    Acked         []ackTokenSet
    Nacked        []nackSet
    MessageErrors []messageErrorSet
}

type outputRow struct {
    Tokens      []uint64
    SourceToken *uint64
}

type ackTokenSet struct {
    Tokens []uint64
}

type nackSet struct {
    Tokens []uint64
    Reason string
}
```

Use the generated `MessagePayload` union and enum values rather than string
tags. In particular, an input reference to column zero must set the generated
column index to `0`. The same filter contract applies in Go: preserve the
complete row sidecar for rows you emit, add dropped rows to `Acked`, and add
rejected rows to `Nacked` with a reason.

Build with TinyGo's non-WASI `wasm-unknown` target; standard Go only produces
`js/wasm` and `wasip1/wasm` modules. TinyGo compiles against the standard library
of the Go toolchain that `go` selects, so select the toolchain pinned in the
guest's `go.mod` explicitly; automatic selection keeps a newer host Go that
TinyGo may not support:

```bash
cd examples/wasm-processors/go-guest
GOTOOLCHAIN="$(sed -n 's/^toolchain //p' go.mod)" tinygo build \
  -target=wasm-unknown \
  -scheduler=none \
  -opt=z \
  -panic=trap \
  -no-debug \
  -o nervix_wasm_processor_go_guest.wasm \
  .
```

## Common Mistakes

- Do not use WASI imports. The host does not provide WASI.
- Do not keep global mixed-branch state. Each module instance is branch-local.
- Do not invent ACK tokens. Only carry, ack, or nack tokens that arrived in the input sidecar.
- Do not omit `source_token` when preserving an input-derived row.
- Do not serialize a generated column separately for every route; place it once in the common generated batch and reference its index.
- Do not give common generated schema fields destination names; their names must be empty.
- Do not emit an encoded zero-column Arrow stream; use an empty byte string for an empty pool.
- Do not leave generated pool columns unreferenced or reuse a pool for routes with different generated row layouts.
- Do not rebuild unchanged input fields in the guest; emit input-column references.
- Do not ack/nack a token and also carry it into an emitted row.
- Do not silently accept an init payload whose schema does not match what the guest implements.
- Do not persist guest state in a custom host-facing format unless `load_state` can reject bad bytes cleanly.
- Do not save buffered input, ACK tokens, pending output, timeout handles, or latched error state in guest state; save only durable computation state.
- Do not call host ACK/NACK directly. The guest only reports lineage and decisions in the sidecar.

## Failure Diagnostics

Nervix reports a module that fails to compile, and every failure of a branch instance's guest
operations and saved state, in one shape:

```text
wasm processor '<processor>' <stage> failed (<branch>, resource '<resource>' version <version> file '<file>'[, export '<export>'][, saved state revision <revision>]): <cause>
```

`<branch>` is `branch` followed by the concrete branch key, or `unbranched`. `<export>` names the
guest export whose call failed, or the export that runs the failed operation. `<revision>` appears
when the failure involves saved state: the revision being restored or the revision being persisted.
`<cause>` is the complete chain of failures below the stage, ending with the guest's own reason when
it put one on the global-error channel. A module that fails to compile is reported without a branch
or export. Diagnostics never contain guest payloads or saved state bytes, and a diagnostic raised on
another node reaches the session unchanged.

| Stage | What failed |
| --- | --- |
| `module compilation` | Resolving, reading, or compiling the pinned module file. |
| `instantiation` | Creating the branch store, instantiating the module, or resolving its exports. |
| `initialization` | `nervix_init`. The saved state revision appears when the instance was about to restore state. |
| `snapshot envelope decoding` | `nervix_load_state` rejected the saved bytes with `-7`. |
| `application state restoration` | `nervix_load_state` rejected the saved application state with `-8`. |
| `state restore` | `nervix_load_state` failed without a verdict: a trap, an exhausted limit, or another negative code. |
| `batch processing` | `nervix_process_batch` and the emit reads that follow it. |
| `timeout callback` | `nervix_on_timeout` and the emit reads that follow it. |
| `quiesce flush` | `nervix_flush` and the emit reads that follow it. |
| `output emission` | An emitted envelope the host cannot decode, or output that fails validation. |
| `state snapshot` | `nervix_dump_state`. Nervix keeps the state saved last. |
| `local state persistence` | Writing the saved state to the node's state store. |
| `state replication` | Confirming the saved state with its replicas. |
| `state authority check` | The state's authority refused it: a replica or peer that is not the state's authority, or this node after the branch's state generation or ownership moved on. |

For example, a branch whose guest rejects its saved counters after an instance was recreated is
reported as:

```text
wasm processor 'sessionizer' application state restoration failed (branch {"tenant":"alpha"}, resource 'sessionizer' version 3 file 'sessionizer.wasm', export 'nervix_load_state', saved state revision 12): wasm guest rejected the application state in its saved snapshot: counters header is truncated
```

## Troubleshooting

`module compilation failed`

: The module is not valid `wasm32-unknown-unknown`, imports something outside
  the `env` functions listed above, or is not actually a WASM module.

`missing required export`

: The guest did not export one of the required `nervix_*` functions with the
  expected C ABI signature.

`guest buffer size ... exceeds configured limit`

: The host refused to write or read a buffer larger than the configured safety
  limit. Split the output, reduce the input batch size, or raise `MAX MEMORY` if
  the processor's linear-memory ceiling is the constraining limit.

`wasm guest exhausted MAX FUEL ...`

: One logical guest operation consumed its complete Wasmtime fuel budget. Raise
  `MAX FUEL` for expected work or remove an unbounded/overly expensive guest loop.

`wasm guest exceeded MAX MEMORY ...`

: The branch guest tried to instantiate or grow linear memory beyond `MAX MEMORY`.
  Reduce guest allocation or raise the declared ceiling.

`generated column ... has ... rows, but the routed output has ... rows`

: A generated pool and one referencing route disagree. Emit exactly one
  `rows` entry per generated Arrow value, or move the incompatible route into a
  separate output group.

`missing source token for output row ...`

: An output uses an input-column reference, but the row does not select a live
  input row. Preserve the host-provided `source_token` and keep it in `tokens`.

`wasm output referenced unknown ack token ...`

: The guest emitted or completed a token that did not come from the host input
  sidecar for the current live instance.

`snapshot envelope decoding failed` or `application state restoration failed`

: The guest rejected the saved state it was asked to restore. Nervix keeps that
  state and reports the rejection again, with the same saved state revision,
  each time the branch is instantiated. Keep `load_state` strict; rejecting the
  state is preferred to running with partially decoded state.

`state restore failed`

: Restoring the saved state failed without a verdict on it, for example because
  the guest trapped or exhausted `MAX FUEL`. Fix the failure; the saved state is
  unchanged and is restored again on the next attempt.
