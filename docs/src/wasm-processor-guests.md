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

`nervix_init` receives CBOR. `output_schemas` contains one schema per declared `TO` relay. A guest output envelope must name one of those relays and its Arrow IPC batch must match that relay's schema before Nervix applies the route-level `SET` and `WHERE` clauses. `UNSET` is not valid on WASM output routes.

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
explicit `UNPARAMETERIZED` relay is still represented by one concrete branch
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

Every input and output payload is:

```text
u32 little-endian output relay name byte size
UTF-8 output relay name bytes, empty on host-to-guest input
u32 little-endian Arrow IPC stream byte size
Arrow IPC stream bytes
u32 little-endian ACK sidecar byte size
CBOR ACK sidecar bytes
```

Host-to-guest input envelopes leave the output relay name empty. Guest-to-host
output envelopes must set it to the target relay for that Arrow IPC batch.

The ACK sidecar is:

```text
{
  "rows": [
    { "tokens": [u64, ...] }
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

`rows` is aligned with Arrow rows in the output batch. If an output row is derived from an input row, carry that input row's tokens into the output row. If the guest drops an input row, put that input row's token set in `acked`. If the guest wants to directly fail an input row without invoking the processor message error policy, put it in `nacked` with a reason.

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

- `rows.len()` must equal the number of Arrow rows in the emitted batch.
- every token in `rows`, `acked`, `nacked`, and `message_errors` must come from the current host-provided input sidecar.
- a token may be carried into output rows, or terminally acked/nacked, but not both.
- a token may have at most one terminal decision across `acked`, `nacked`, and `message_errors`.

It is valid to carry the same input token into more than one emitted row. The
host keeps attached child guards and resolves the original guard only after all
derived output rows complete.

## State

Use branch-local guest state for data the guest needs across runtime instance recreation.

Nervix saves guest state through `nervix_dump_state`, persists and replicates the returned bytes, and restores them through `nervix_load_state` when the branch instance is recreated.

ACK tokens are separate from guest state. They are host-local hot-path runtime capabilities and are not persisted or replicated. If ACK state is lost with a processor owner, the upstream ingestor reacts according to its delivery mode and retry policy.

## Timeouts

A guest can call `nervix_timeout_after_nanos(delay)` while processing. The host returns a monotonically increasing handle for that branch instance. When the domain clock reaches the requested time, the host calls:

```text
nervix_on_timeout(handle)
```

After any successful timeout callback, the host calls `nervix_read_emit()` and forwards emitted envelopes.

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
        let envelope = match BatchEnvelope::decode(input) {
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
    envelope: BatchEnvelope,
    start_row: u64,
    output_schemas: &[ProcessorSchema],
) -> Result<Vec<BatchEnvelope>, i32> {
    let input = read_single_i32_batch(&envelope.arrow_ipc_batch)?;
    let mut kept_values = Vec::new();
    let mut output_acks = AckSidecar::default();

    for (row, value) in input.iter().enumerate() {
        let global_row = start_row + row as u64;
        let row_tokens = envelope.acks.rows.get(row).cloned().unwrap_or_default();
        if global_row % 2 == 1 {
            kept_values.push(*value);
            output_acks.rows.push(row_tokens);
        } else {
            output_acks.acked.push(row_tokens);
        }
    }

    output_schemas
        .iter()
        .map(|schema| {
            Ok(BatchEnvelope {
                output_relay: Some(schema.name.clone()),
                arrow_ipc_batch: write_output_batch_for_schema(schema, &kept_values)?,
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
    envelope, code := decodeBatchEnvelope(buffer[:int(size)])
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
    Rows   []rowAckSet `cbor:"rows"`
    Acked  []rowAckSet `cbor:"acked"`
    Nacked []nackSet   `cbor:"nacked"`
}

type rowAckSet struct {
    Tokens []uint64 `cbor:"tokens"`
}

type nackSet struct {
    Tokens []uint64 `cbor:"tokens"`
    Reason string   `cbor:"reason"`
}
```

The same filter contract applies in Go: preserve tokens for rows you emit, add
dropped rows to `Acked`, and add rejected rows to `Nacked` with a reason.

Build:

```bash
just wasm-processor-go-guest
```

## Common Mistakes

- Do not use WASI imports. The host does not provide WASI.
- Do not keep global mixed-branch state. Each module instance is branch-local.
- Do not invent ACK tokens. Only carry, ack, or nack tokens that arrived in the input sidecar.
- Do not emit an Arrow row without a matching sidecar row.
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

`wasm output row count ... does not match ack sidecar row count ...`

: The emitted Arrow batch and sidecar disagree. Emit exactly one `rows` entry
  for each Arrow row.

`wasm output referenced unknown ack token ...`

: The guest emitted or completed a token that did not come from the host input
  sidecar for the current live instance.

`nervix_load_state` returned a negative code

: The guest rejected saved state. Keep `load_state` strict; returning an error
  is preferred to running with partially decoded state.
