# WASM Processor Guest Prototype ABI

This prototype uses a native core WebAssembly module loaded by Wasmtime. It does
not use WASI.

All byte payloads move through the guest module's default exported linear
memory. The guest owns one reusable byte buffer. The host asks the guest to grow
that buffer, writes a batch envelope into it, then calls the processor
entrypoint with the number of valid bytes.

## Imported Host Functions

The guest imports these functions from the `env` module:

| Function | Signature | Meaning |
| --- | --- | --- |
| `nervix_domain_time_nanos` | `() -> i64` | Returns the current domain-clock time as Unix nanoseconds. Nanoseconds are an ABI boundary format only. |
| `nervix_timeout_after_nanos` | `(delay_nanos: i64) -> i64` | Guest requests a domain-clock timeout and receives a monotonically increasing handle. |

## Exported Guest Functions

| Function | Signature | Meaning |
| --- | --- | --- |
| `nervix_buffer_ptr` | `() -> i32` | Current guest buffer offset in linear memory. |
| `nervix_buffer_len` | `() -> i32` | Current logical buffer length. |
| `nervix_buffer_capacity` | `() -> i32` | Current allocated buffer capacity. |
| `nervix_alloc` | `(size: i32) -> i32` | Ensures the single reusable buffer can hold `size` bytes, resizes it, and returns `nervix_buffer_ptr()`. |
| `nervix_init` | `(ptr: i32, size: i32) -> i32` | Reads CBOR init metadata from guest memory. Must be called before processing. |
| `nervix_current_domain_time_nanos` | `() -> i64` | Returns the current domain-clock time by calling the host import. |
| `nervix_process_batch` | `(size: i32) -> i32` | Processes `size` bytes of batch envelope from the guest buffer. Prototype behavior filters the input batch before emitting a new batch envelope. |
| `nervix_on_timeout` | `(handle: i64) -> i32` | Host callback when a previously requested timeout fires. |
| `nervix_read_emit` | `() -> i32` | If the guest has a pending outgoing batch envelope, writes the next envelope into the reusable buffer, removes it from the pending emit queue, and returns the byte size. Returns `0` when nothing is pending. |
| `nervix_dump_state` | `() -> i32` | Serializes guest state into the reusable buffer and returns the byte size. |
| `nervix_load_state` | `(ptr: i32, size: i32) -> i32` | Loads previously dumped guest state bytes. Returns a negative value on rejection. |
| `nervix_reset_state` | `() -> i32` | Clears guest-owned state while keeping the reusable buffer. |

Return code `0` means success. Negative return codes are guest errors:

| Code | Meaning |
| --- | --- |
| `-1` | Negative or invalid size. |
| `-2` | Pointer/size range is outside guest memory. |
| `-3` | Processor called before `nervix_init`. |
| `-4` | Arrow IPC decode/encode error. |
| `-5` | Batch envelope decode/encode error. |

## Batch Envelope

Input and output batches use a small binary envelope:

```text
u32 little-endian output relay name byte size
UTF-8 output relay name bytes, empty on host-to-guest input
u32 little-endian Arrow IPC byte size
Arrow IPC stream bytes
u32 little-endian ack sidecar byte size
CBOR ack sidecar bytes
```

Host-to-guest input envelopes use an empty output relay name. Guest-to-host
output envelopes must name one of the processor's declared `TO` relays, and the
Arrow IPC batch in that envelope must match that relay's schema before route
`SET` and `WHERE` clauses are applied.

The ack sidecar is CBOR encoded:

```text
{
  "rows": [
    { "tokens": [u64, ...] },
    ...
  ],
  "acked": [
    { "tokens": [u64, ...] },
    ...
  ],
  "nacked": [
    { "tokens": [u64, ...], "reason": text },
    ...
  ],
  "message_errors": [
    { "tokens": [u64, ...], "reason": text },
    ...
  ]
}
```

`rows` is positionally aligned with the Arrow batch rows. Each output row must
carry the ack tokens that should follow that row. Multiple tokens represent a
merged lineage. `acked` contains input row token sets that the guest decided to
complete because they were dropped or otherwise consumed. `nacked` contains
input row token sets the guest decided to fail directly. `message_errors`
contains input row token sets to route through `ON MESSAGE ERROR`.

Global errors are not part of the ACK sidecar. They are guest/node state and use
a separate optional export channel:

```text
nervix_global_error_ptr() -> i32
nervix_global_error_len() -> i32
nervix_clear_global_error() -> i32
```

If the guest exposes any of these functions, it must expose all three. After a
host call into the guest, a positive `nervix_global_error_len()` means
`nervix_global_error_ptr()` points at UTF-8 error bytes. The host reads those
bytes, calls `nervix_clear_global_error()`, and applies `ON GLOBAL ERROR`.
Wasmtime traps and call failures are handled as global processor errors too.

The guest owns the lineage decision, but it does not receive host-local
`AckSet` handles. The host maps tokens back to real runtime ack handles and
performs the actual `ack_success` or `no_ack` call after applying the guest's
decision in the surrounding runtime delivery path. This avoids exposing host
`Arc` state to WASM and prevents a guest from completing an ack before the host
has accepted the emitted batch.

Ack tokens are only valid inside the host process that created them. They are
hot-path runtime capabilities, not execution node state. The runtime does not
persist or replicate ACK tokens. If ACK state is lost with a processor owner,
the upstream ingestor reacts according to its delivery mode and retry policy.

WASM guest state is separate from ACK state. The runtime persists and replicates
the guest-owned bytes returned from `nervix_dump_state`, then restores them with
`nervix_load_state` when a branch instance is recreated.

## Init Payload

`nervix_init` receives CBOR encoded metadata. The first iteration stores this
metadata as opaque bytes so the host ABI can settle before the runtime model is
wired in.

The intended shape is:

```text
{
  "domain_name": text,
  "domain_type": text,
  "branch_key": bytes,
  "input_schema": WasmProcessorSchema,
  "output_schemas": [WasmProcessorSchema, ...]
}
```

`branch_key` is the serialized concrete branch key for the branch-local WASM
instance. A singleton root branch is still represented by a concrete, serialized
branch key.

`input_schema` and every entry in `output_schemas` use the `nervix-wasm` ABI
schema contract:

```text
WasmProcessorSchema {
  "name": text,
  "fields": [
    {
      "name": text,
      "ty": WasmProcessorType,
      "optional": bool
    }
  ]
}
```

The host integration converts Nervix model schemas into this contract before
encoding the init payload.

## Prototype State

The Rust and Go prototype guests serialize their branch-local state as CBOR.
The state includes:

- processed batch count
- processed row count
- pending batch start row
- last observed domain time
- last timeout handle
- pending batch envelope
- opaque init metadata

Filtering uses `processed row count` as a global row ordinal. Rows with even
global ordinals are preserved and rows with odd global ordinals are dropped, so
state restoration changes subsequent keep/drop decisions. Preserved rows carry
their input ack token sets into the emitted envelope; dropped rows are listed in
the emitted envelope's `acked` sidecar.
