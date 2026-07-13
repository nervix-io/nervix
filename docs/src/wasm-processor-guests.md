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
nervix_process_batch(size: i32) -> i32
nervix_on_timeout(handle: i64) -> i32
nervix_read_emit() -> i32
nervix_dump_state() -> i32
nervix_load_state(ptr: i32, size: i32) -> i32
nervix_reset_state() -> i32
```

Return `0` from fallible functions on success. Return a negative code on guest rejection. Nervix treats negative codes as runtime errors and applies the processor error policy.

## Buffer Ownership

The guest owns one reusable byte buffer in linear memory.

1. Host calls `nervix_alloc(size)`.
2. Guest grows/resizes its buffer and returns the buffer pointer.
3. Host writes exactly `size` bytes into guest memory.
4. Host calls `nervix_process_batch(size)` or `nervix_load_state(ptr, size)`.
5. Guest writes pending output or state back into the same buffer.
6. Host calls `nervix_read_emit()` or `nervix_dump_state()` and reads the returned byte length from `nervix_buffer_ptr()`.

The host enforces a maximum guest buffer size. A guest should still validate sizes and reject impossible pointer ranges.

## Init Payload

`nervix_init` receives CBOR. `output_schemas` contains one schema per declared `TO` relay. A guest output envelope must name one of those relays and provide one destination-aligned column descriptor per field before Nervix applies the route-level `SET` and `WHERE` clauses. `UNSET` is not valid on WASM output routes.

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

`WasmProcessorType` is encoded with Serde's externally tagged CBOR enum shape.
Unit variants are strings:

```text
"U8" | "I8" | "U16" | "I16" | "U32" | "I32" | "U64" | "I64"
| "Bool" | "String" | "Datetime" | "F32" | "F64"
```

Container variants are maps with one enum tag:

```text
{ "Array": { "element": WasmProcessorType, "len": u32 } }
{ "Vec": { "element": WasmProcessorType } }
```

`Datetime` values are Arrow `Timestamp(Nanosecond)` values at the wire boundary.
Treat nanosecond integers as a boundary format and convert them to your guest's
typed timestamp representation immediately after decoding.

Treat this as configuration for the branch instance. Store what you need in guest state; reject it only when the processor cannot run correctly.

## Batch Envelope

Every input or output payload is exactly one CBOR value. The existing ABI size
argument or return value frames it, so the envelope has no manual length
prefixes. Arrow IPC payloads are CBOR byte strings, not arrays of integers.

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
  "output_relay": text,
  "columns": [
    { "kind": "input", "column_index": u32 },
    { "kind": "guest_arrow", "ipc": bytes }
  ],
  "acks": AckSidecar
}
```

Each `columns` entry corresponds positionally to a destination field. An
`input` column references the declared processor input schema by index; the
source and destination types and nullability must match exactly, although their
names may differ. A `guest_arrow` column is a complete Arrow IPC stream with
exactly one schema field, one record batch, and one column. That field must
exactly match the destination field, including name, data type, nullability,
timestamp properties, nested types, fixed lengths, and relevant metadata. Its
row count must equal the number of output sidecar rows.

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
or enriching. `source_token` is always encoded; use CBOR `null` only for a
generated row that has no input source.

If an output envelope contains any `input` column, every output row must have a
non-null, live `source_token`, and that token must occur in the row's `tokens`.
It selects the retained host input row used for every referenced column in that
output row. It also selects the original record exposed through route
expressions such as `input.field`. A source token does not add an ACK use.

If the guest drops an input row, put that row's token set in `acked`. To fail it
directly without invoking the processor message error policy, put it in
`nacked` with a reason.

Use `message_errors` for per-message guest errors that must be handled through `ON MESSAGE ERROR` (`IGNORE`, `LOG`, or `DLQ`). Global errors are not part of the ACK sidecar because they are guest/node state, not message lineage.

Guests may expose this optional global-error channel:

```text
nervix_global_error_ptr() -> i32
nervix_global_error_len() -> i32
nervix_clear_global_error() -> i32
```

If any of these exports exists, all three must exist. After host calls into the guest (`nervix_process_batch`, `nervix_on_timeout`, and emit reads), it checks `nervix_global_error_len()`. A positive length means `nervix_global_error_ptr()` points at UTF-8 error bytes. The host reads the bytes, calls `nervix_clear_global_error()`, and applies `ON GLOBAL ERROR`. Wasmtime call failures and traps are also handled as global processor errors.

The guest decides lineage; the host performs the actual ACK/NACK operation. Tokens are host-local hot-path capabilities. They are valid only while the current host instance is alive, and they are never persisted.

The sidecar must be internally consistent:

- `rows.len()` is the output row count and must equal every guest Arrow column's row count.
- every token in `rows`, `acked`, `nacked`, and `message_errors` must come from the current host-provided input sidecar.
- a token may be carried into output rows, or terminally acked/nacked, but not both.
- a token may have at most one terminal decision across `acked`, `nacked`, and `message_errors`.
- a non-null `source_token` must be live and carried in its output row.

It is valid to carry the same input token into more than one emitted row or
output envelope. The host keeps attached child guards and resolves the original
guard only after all derived deliveries complete. All output envelopes from
one callback are validated together; no output is dispatched and no terminal
decision is applied if a later envelope is invalid.

For input references, Nervix retains the original input Arrow batch while its
tokens are live. Identity selections reuse the source `ArrayRef`, contiguous
selections use a buffer-sharing slice, and filtered, reordered, duplicated, or
cross-batch selections use host-side Arrow kernels. The guest never has to
serialize unchanged field values back to the host.

All fields are required, including empty arrays. Unknown or missing fields,
unknown `kind` values, trailing CBOR bytes, invalid column counts, bad source
tokens, malformed or trailing Arrow IPC, and exact-schema mismatches are global
processor errors. Old length-prefixed envelopes are not supported.
There is no protocol version, magic prefix, feature negotiation, legacy
decoder, or fallback path. Rebuild every guest for this CBOR contract.

## State

Use branch-local guest state for data the guest needs across runtime instance recreation.

Nervix saves guest state through `nervix_dump_state`, persists and replicates the returned bytes, and restores them through `nervix_load_state` when the branch instance is recreated.

ACK tokens are separate from guest state. They are host-local hot-path runtime capabilities and are not persisted or replicated. If ACK state is lost with a processor owner, the upstream ingestor reacts according to its delivery mode and retry policy.

A guest may include a pending CBOR envelope in its opaque snapshot, but tokens
and input-column references remain usable only while the originating live host
ACK map exists. Restored output that refers to lost tokens is rejected. Guest
load code must reject old-format pending envelopes rather than silently reset
the snapshot.

## Timeouts

A guest can call `nervix_timeout_after_nanos(delay)` while processing. The host returns a monotonically increasing handle for that branch instance. When the domain clock reaches the requested time, the host calls:

```text
nervix_on_timeout(handle)
```

After any successful timeout callback, the host calls `nervix_read_emit()` and forwards emitted envelopes. Input-column references remain valid in timeout output while their source token is live.

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
pub extern "C" fn nervix_process_batch(size: i32) -> i32 {
    with_state(|state| {
        let input = &state.buffer[..size as usize];
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

    output_schemas
        .iter()
        .map(|schema| {
            Ok(WasmEnvelope::Output {
                output_relay: schema.name.clone(),
                columns: vec![WasmOutputColumn::Input { column_index: 0 }],
                acks: output_acks.clone(),
            })
        })
        .collect()
}
```

State restoration in the prototype is plain CBOR:

```rust
#[derive(Serialize, Deserialize)]
struct GuestSnapshot {
    processed_batches: u64,
    processed_rows: u64,
    pending_start_row: u64,
    last_domain_time_nanos: i64,
    last_timeout_handle: i64,
    pending_batch: Vec<u8>,
    init_metadata: Vec<u8>,
}

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
    self.buffer.clear();
    ciborium::into_writer(&snapshot, &mut self.buffer)
        .map(|()| self.buffer.len() as i32)
        .unwrap_or(-1)
}
```

For Arrow IPC, use `arrow_ipc::reader::StreamReader` and `arrow_ipc::writer::StreamWriter`. For CBOR, use `ciborium`.

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
func nervixProcessBatch(size int32) int32 {
    envelope, code := decodeEnvelope(buffer[:int(size)])
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

The prototype Go guest uses `fxamacker/cbor` for state and sidecars. The
sidecar structs should stay explicit:

```go
type ackSidecar struct {
    Rows   []outputRow `cbor:"rows"`
    Acked  []ackTokenSet `cbor:"acked"`
    Nacked []nackSet   `cbor:"nacked"`
    MessageErrors []messageErrorSet `cbor:"message_errors"`
}

type outputRow struct {
    Tokens []uint64 `cbor:"tokens"`
    SourceToken *uint64 `cbor:"source_token"`
}

type ackTokenSet struct {
    Tokens []uint64 `cbor:"tokens"`
}

type nackSet struct {
    Tokens []uint64 `cbor:"tokens"`
    Reason string   `cbor:"reason"`
}
```

Use tagged `envelope` and `outputColumn` structs with exact snake-case CBOR
keys. Validate the allowed fields for each `kind`; `omitempty` is not a variant
validator. In particular, an input reference to column zero must still encode
`column_index: 0`. The same filter contract applies in Go: preserve the complete
row sidecar for rows you emit, add dropped rows to `Acked`, and add rejected
rows to `Nacked` with a reason.

Build:

```bash
just wasm-processor-go-guest
```

## Common Mistakes

- Do not use WASI imports. The host does not provide WASI.
- Do not keep global mixed-branch state. Each module instance is branch-local.
- Do not invent ACK tokens. Only carry, ack, or nack tokens that arrived in the input sidecar.
- Do not omit `source_token` when preserving an input-derived row.
- Do not emit a guest Arrow column without exactly one field, batch, and column.
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

`guest Arrow row count ... does not match output row count ...`

: A generated column and the sidecar disagree. Emit exactly one `rows` entry
  for each generated Arrow value.

`missing source token for output row ...`

: An output uses an input-column reference, but the row does not select a live
  input row. Preserve the host-provided `source_token` and keep it in `tokens`.

`wasm output referenced unknown ack token ...`

: The guest emitted or completed a token that did not come from the host input
  sidecar for the current live instance.

`nervix_load_state` returned a negative code

: The guest rejected saved state. Keep `load_state` strict; returning an error
  is preferred to running with partially decoded state.
