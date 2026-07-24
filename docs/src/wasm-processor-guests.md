# WASM Processor Guests

WASM processors are native WebAssembly modules loaded by Wasmtime. They do not use WASI. A guest module exports a small C ABI, owns one reusable linear-memory buffer, and exchanges Arrow IPC record batches with Nervix through that buffer.

The runtime creates one guest instance per concrete branch. Guest state must therefore be branch-local. Do not aggregate across branch keys inside the guest.

WASM processor output flush is guest-controlled. `CREATE WASM PROCESSOR` does not accept `FLUSH EACH` or `FLUSH IMMEDIATE`; Nervix routes batches returned from `nervix_process_batch` and batches returned later from `nervix_on_timeout` callbacks requested by the guest through the processor's declared `TO` clauses.

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
nervix_read_emit() -> i32
nervix_dump_state() -> i32
nervix_load_state(ptr: i32, size: i32) -> i32
nervix_reset_state() -> i32
```

Return `0` from fallible functions on success. Return a negative code on guest rejection. Nervix treats negative codes as runtime errors and applies the processor error policy.

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

If any of these exports exists, all three must exist. After host calls into the guest (`nervix_process_batch`, `nervix_on_timeout`, and emit reads), it checks `nervix_global_error_len()`. A positive length means `nervix_global_error_ptr()` points at UTF-8 error bytes. The host reads the bytes, calls `nervix_clear_global_error()`, and applies `ON GLOBAL ERROR`. Wasmtime call failures and traps are also handled as global processor errors.

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

Use branch-local guest state for data the guest needs across runtime instance recreation.

Nervix saves guest state through `nervix_dump_state`, persists and replicates the returned bytes, and restores them through `nervix_load_state` when the branch instance is recreated.

ACK tokens are separate from guest state. They are host-local hot-path runtime capabilities and are not persisted or replicated. If ACK state is lost with a processor owner, the upstream ingestor reacts according to its delivery mode and retry policy.

A guest may include a pending FlatBuffers envelope in its `GuestSnapshot`, but tokens
and input-column references remain usable only while the originating live host
ACK map exists. Restored output that refers to lost tokens is rejected. Guest
state that retains a pending output group must retain the complete generated
Arrow IPC bytes, every routed output, all column references, and every sidecar.
Generated indexes are not host capabilities and have no meaning without that
complete envelope. Guest load code must reject old-format pending envelopes
rather than silently reset the snapshot.

## Timeouts

A guest can call `nervix_timeout_after_nanos(delay)` while processing. The host returns a monotonically increasing handle for that branch instance. When the domain clock reaches the requested time, the host calls:

```text
nervix_on_timeout(handle)
```

After any successful timeout callback, the host repeatedly calls
`nervix_read_emit()` and forwards every emitted output group. Shared generated
columns work identically in timeout output. Input-column references remain
valid while their source token is live.

## Rust Sketch

The prototype Rust guest is in `examples/wasm-processors/rust-guest`. The important shape is:

```rust
#[repr(transparent)]
struct Global<T>(UnsafeCell<T>);

unsafe impl<T> Sync for Global<T> {}

static STATE: Global<GuestState> = Global(UnsafeCell::new(GuestState::new()));

unsafe extern "C" {
    fn nervix_domain_time_nanos() -> i64;
    fn nervix_timeout_after_nanos(delay_nanos: i64) -> i64;
}

#[unsafe(no_mangle)]
pub extern "C" fn nervix_alloc(size: i32) -> i32 {
    with_state(|state| state.alloc(size as usize))
}

#[unsafe(no_mangle)]
pub extern "C" fn nervix_process_batch(ptr: i32, size: i32) -> i32 {
    with_state(|state| {
        let range = match state.buffer_range(ptr, size) {
            Ok(range) => range,
            Err(code) => return code,
        };
        let input = &state.buffer[range];
        let envelope = match WasmEnvelope::decode(input) {
            Ok(envelope) => envelope,
            Err(code) => return code,
        };

        state.last_timeout_handle = unsafe {
            nervix_timeout_after_nanos(1_000_000_000)
        };

        match filter_arrow_batch(envelope) {
            Ok(outputs) => {
                state.pending_emit = outputs;
                0
            }
            Err(code) => code,
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn nervix_read_emit() -> i32 {
    with_state(|state| {
        if state.pending_emit.is_empty() {
            return 0;
        }
        let next = state.pending_emit.remove(0);
        state.buffer.clear();
        state.buffer.extend_from_slice(&next);
        state.buffer.len() as i32
    })
}
```

The core filtering flow in the prototype is:

```rust
fn filter_envelope_by_global_row(
    envelope: WasmEnvelope,
    start_row: u64,
    output_schemas: &[ProcessorSchema],
) -> Result<Vec<WasmEnvelope>, i32> {
    let WasmEnvelope::Input { arrow_ipc_batch, acks } = envelope else {
        return Err(-5);
    };
    let input = read_single_i32_batch(&arrow_ipc_batch)?;
    let mut output_acks = AckSidecar::default();

    for row in 0..input.len() {
        let global_row = start_row + row as u64;
        let row_sidecar = acks.rows.get(row).cloned().ok_or(-5)?;
        if global_row % 2 == 1 {
            output_acks.rows.push(row_sidecar);
        } else {
            output_acks.acked.push(AckTokenSet {
                tokens: row_sidecar.tokens,
            });
        }
    }

    let outputs = output_schemas
        .iter()
        .map(|schema| WasmRoutedOutput {
            output_relay: schema.name.clone(),
            columns: vec![WasmOutputColumnRef::input(0)],
            acks: output_acks.clone(),
        })
        .collect();
    Ok(vec![WasmEnvelope::Output {
        generated_arrow_ipc_batch: Vec::new(),
        outputs,
    }])
}
```

For enrichment, build one destination-neutral Arrow batch whose fields have
empty names, then use `WasmOutputColumnRef::generated(index)` wherever a routed
destination field consumes that array. The constructor helpers keep input and
generated index namespaces explicit. A guest-side builder may additionally
check local index bounds and route row counts, but host validation remains
authoritative.

State restoration uses the `GuestSnapshot` FlatBuffers message from the shared protocol crate:

```rust
use nervix_wasm_protocol::GuestSnapshot;

fn dump_state(&mut self) -> i32 {
    let snapshot = GuestSnapshot {
        processed_batches: self.processed_batches,
        processed_rows: self.processed_rows,
        pending_start_row: self.pending_start_row,
        last_domain_time_nanos: self.last_domain_time_nanos,
        last_timeout_handle: self.last_timeout_handle,
        pending_batch: self.pending_batch.clone(),
        init_metadata: self.init_metadata.clone(),
    };
    self.buffer = match snapshot.encode() {
        Ok(buffer) => buffer,
        Err(_) => return -1,
    };
    self.buffer.len() as i32
}
```

For Arrow IPC, use `arrow_ipc::reader::StreamReader` and `arrow_ipc::writer::StreamWriter`. Use `nervix-wasm-protocol` for the ABI envelope and snapshot types.

Build:

```bash
just wasm-processor-rust-guest
```

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

Build:

```bash
just wasm-processor-go-guest
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
- Do not call host ACK/NACK directly. The guest only reports lineage and decisions in the sidecar.

## Troubleshooting

`failed to compile wasm processor`

: The module is not valid `wasm32-unknown-unknown`, imports something outside
  the `env` functions listed above, or is not actually a WASM module.

`missing export`

: The guest did not export one of the required `nervix_*` functions with the
  expected C ABI signature.

`guest buffer size ... exceeds configured limit`

: The host refused to write or read a buffer larger than the configured safety
  limit. Split the output, reduce batch size, or change the runtime limit when
  that becomes configurable.

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

`nervix_load_state` returned a negative code

: The guest rejected saved state. Keep `load_state` strict; returning an error
  is preferred to running with partially decoded state.
