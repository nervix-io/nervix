# WASM Processor Guest Prototype ABI

This prototype uses a native core WebAssembly module loaded by Wasmtime. It does
not use WASI.

All byte payloads move through the guest module's default exported linear
memory. The guest owns one reusable byte buffer. The host asks the guest to grow
that buffer, builds a batch envelope directly in it, then calls the processor
entrypoint with the exact pointer and number of valid bytes.

## Imported Host Functions

The guest imports these functions from the `env` module:

| Function | Signature | Meaning |
| --- | --- | --- |
| `nervix_domain_time_nanos` | `() -> i64` | Returns the current domain-clock time as Unix nanoseconds. Nanoseconds are an ABI boundary format only. |
| `nervix_timeout_after_nanos` | `(delay_nanos: i64) -> i64` | Guest requests a domain-clock timeout and receives a monotonically increasing handle. |
| `nervix_request_state_reset` | `() -> i32` | Guest asks the host to replace the complete guest-state lifetime of the calling branch. Returns `0` when the host takes the request and `-9` when it refuses. |

## Exported Guest Functions

| Function | Signature | Meaning |
| --- | --- | --- |
| `nervix_buffer_ptr` | `() -> i32` | Current guest buffer offset in linear memory. |
| `nervix_buffer_len` | `() -> i32` | Current logical buffer length. |
| `nervix_buffer_capacity` | `() -> i32` | Current allocated buffer capacity. |
| `nervix_alloc` | `(size: i32) -> i32` | Ensures the single reusable buffer can hold `size` bytes, resizes it, and returns `nervix_buffer_ptr()`. |
| `nervix_init` | `(ptr: i32, size: i32) -> i32` | Reads size-prefixed FlatBuffers init metadata from guest memory. Must be called before processing. |
| `nervix_current_domain_time_nanos` | `() -> i64` | Returns the current domain-clock time by calling the host import. |
| `nervix_process_batch` | `(ptr: i32, size: i32) -> i32` | Processes the exact batch-envelope range in the guest buffer. Prototype behavior filters the input batch before emitting a new batch envelope. |
| `nervix_on_timeout` | `(handle: i64) -> i32` | Host callback when a previously requested timeout fires. |
| `nervix_flush` | `() -> i32` | Host request to release everything the guest still buffers because the branch is being quiesced for a handoff or shutdown. Queue every buffered output envelope for `nervix_read_emit`. Input the guest keeps past this call stays unacknowledged until the branch resumes. |
| `nervix_read_emit` | `() -> i32` | If the guest has a pending outgoing batch envelope, writes the next envelope into the reusable buffer, removes it from the pending emit queue, and returns the byte size. Returns `0` when nothing is pending. |
| `nervix_dump_state` | `() -> i32` | Serializes the guest's durable computation state into the reusable buffer and returns the byte size, or a negative code when it cannot serialize the state. The host keeps the state saved last when a save fails. |
| `nervix_load_state` | `(ptr: i32, size: i32) -> i32` | Loads previously dumped guest state bytes into an instance the host initialized with `nervix_init`. Returns `-7` or `-8` when it rejects the saved state, and another negative code when restoring fails for any other reason. |
| `nervix_reset_state` | `() -> i32` | Clears guest-owned state while keeping the reusable buffer. |

Return code `0` means success. Negative return codes are guest errors:

| Code | Meaning |
| --- | --- |
| `-1` | Negative or invalid size. |
| `-2` | Pointer/size range is outside guest memory. |
| `-3` | Processor called before `nervix_init`. |
| `-4` | Arrow IPC decode/encode error. |
| `-5` | Batch envelope decode/encode error. |
| `-6` | The guest latched into error state; the reason is on the global-error channel. |
| `-7` | `nervix_load_state` only: the saved bytes are not a snapshot envelope the guest can decode. |
| `-8` | `nervix_load_state` only: the guest refuses the application state in a decoded snapshot. |

`-7` and `-8` are the only codes that classify the saved state itself as
unusable. The host keeps the saved state after every failed restore and reports
a rejection separately from a restore that trapped, exhausted a limit, or
failed with another code.

## Guest-Requested State Reset

A guest asks the host to replace the complete guest-state lifetime of the branch it runs in by
calling `nervix_request_state_reset()`. The host records the request and performs the replacement
after the callback returns; it never calls back into the guest to perform it, so the guest can ask
from anywhere inside a callback without re-entering its own exports.

The request names nothing. It is scoped to the calling branch and to the state generation that
branch is running in, so a guest can never select another processor, domain, branch, or generation.

Only the callbacks the host completes with a checkpoint — `nervix_process_batch`,
`nervix_on_timeout`, and `nervix_flush` — own uncommitted effects a reset can discard, so only
those callbacks admit a request. Every other operation, including `nervix_init`,
`nervix_dump_state`, `nervix_load_state`, and `nervix_read_emit`, is answered with `-9` and
schedules nothing.

Once the host accepts a request, the callback that made it is terminal:

- Output the callback queued for `nervix_read_emit` is discarded and never dispatched.
- Every input the branch holds, including input the guest still buffers, is negatively
  acknowledged, so a source with acknowledgements redelivers it into the new lifetime.
- No checkpoint is taken, because the state the callback would save is exactly the state the reset
  discards.
- The instance is dropped with its pending timeout handles, and the branch runs no further callback
  in the lifetime being replaced.
- Effects earlier callbacks already published stand, because their checkpoints completed.

A callback that asks and then fails is still asking, and asking repeatedly — several times inside
one callback, or from several callbacks of one state lifetime — replaces that lifetime exactly
once. The return code says only that the host took the request. It is never proof that a new
lifetime became durable: the guest learns that by being initialized again, with no
`nervix_load_state` call, in a new lifetime.

Clearing the guest's own fields is an ordinary application-state mutation that the next
`nervix_dump_state` saves, and needs none of this.

## Batch Envelope

Each input or output payload is one size-prefixed FlatBuffer whose root is the
`Message` union in
[`nervix_wasm.fbs`](../../crates/nervix-wasm-protocol/schema/nervix_wasm.fbs).
Every buffer carries the `NVWX` file identifier. The ABI function's size
argument or return value must agree with the internal FlatBuffers size prefix.
Arrow IPC values are FlatBuffers byte vectors and are read as borrowed slices
without deserializing or copying them. Crossing the WebAssembly linear-memory
boundary still requires writing source bytes once. Normally the host FlatBuffers
builder writes directly into the final guest-memory range. Because FlatBuffers
builds backwards, that range may start after `nervix_buffer_ptr()`, which is why
`nervix_process_batch` receives both `ptr` and `size`. If the estimated capacity
is insufficient, the host finishes in spill storage, releases the memory borrow,
grows the guest buffer, and copies the finished message once. After a guest emit,
the host retains that transferred FlatBuffer and exposes its generated Arrow
vector as a shared slice rather than copying the vector again.

Host input:

```text
{
  "kind": "input",
  "arrow_ipc_batch": bytes,
  "acks": AckSidecar
}
```

Guest output:

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

Each routed output column is positionally aligned with its destination relay
schema. `input` references an unchanged column in the declared processor input
schema. `generated` references a column in the envelope's common generated
Arrow batch. A generated column may be reused by several routes or more than
once in one route; the host decodes it once and clones the immutable
`ArrayRef`.

`uninitialized` is FlatBuffers `ColumnSource.Uninitialized` with canonical
`column_index = 0`. Its type comes from the aligned destination field and its
length comes from `acks.rows`. Reading it in route `SET` or `WHERE` processing
materializes typed NULLs. If it reaches the node boundary unread, an optional
field becomes typed NULLs and a required field fails. The marker never enters a
relay or crosses a node boundary, and it does not require `source_token` unless
another descriptor or expression needs that source row.

The generated pool is either an empty byte string or one Arrow IPC stream with
exactly one schema and one record batch. An empty byte string is required when
there are no generated columns; an encoded zero-column stream is invalid.
Generated field names must be empty. The host compares all other field
properties with every referencing destination field, including data type,
nullability, timestamp properties, nested types, fixed lengths, and metadata.
Each referencing route must have the generated batch's row count.

Generated row `R` is positional. Input row `R` is selected independently by
that routed row's `source_token`. Routes that need different generated row
counts, ordering, filtering, or duplication must be returned as separate
output envelopes from separate positive `nervix_read_emit()` calls. Generated
indexes are local to one envelope.

The ACK sidecar is:

```text
{
  "rows": [
    { "tokens": [u64, ...], "source_token": u64 | null },
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

Each host input row has one token and a matching non-null `source_token`. A
guest may preserve that row object when filtering or enriching. If any output
column is an input reference, every routed output row must have a live `source_token`
that also appears in that row's `tokens`. The token selects both the retained
input row used by every referenced column and the record exposed through route
expressions such as `input.field`.

Multiple tokens represent merged lineage. `acked` contains token sets consumed
without output, `nacked` contains token sets failed directly, and
`message_errors` contains token sets routed through `ON MESSAGE ERROR`. A token
may be carried into multiple routed output rows or output groups, but it cannot be both
carried and terminally completed in one callback.

Every output group must contain at least one routed output, and every generated
pool column must be referenced at least once. All structural, schema, row,
relay, and token validation completes before any route is dispatched or any
terminal ACK decision is applied.

All vector and table fields are required, including empty vectors.
`source_token` is an optional scalar. FlatBuffers permits unknown fields for
forward schema evolution, while unknown union or enum variants, missing
required fields, a wrong identifier or size prefix, trailing bytes, malformed
Arrow IPC, and schema mismatches reject the complete callback output before any
dispatch or ACK decision is applied. The whole FlatBuffer remains subject to
the configured guest-buffer limit.

This FlatBuffers format completely replaces the former CBOR envelope. There is
no format negotiation or legacy decoder. Guests must be rebuilt with the
current schema.

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
`nervix_load_state` when a branch instance is recreated. A guest that cannot
decode the snapshot envelope, or that finds it was taken under a different
branch configuration, returns `-7`; a guest that decodes it but refuses the
application state it carries returns `-8`. Both example guests put the reason
on the global-error channel before returning either code.

## Init Payload

`nervix_init` receives a `BranchInit` FlatBuffer message. Bundled guests keep
the branch configuration it describes for their snapshots and use zero-copy
FlatBuffers accessors to read the input and output schemas needed to construct
column descriptors.

The shape is:

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

## Guest State

Both example guests serialize their branch-local state as the `GuestSnapshot`
variant of the same size-prefixed FlatBuffers `Message` union. A snapshot holds
two byte vectors:

- `init_metadata`: a `BranchInit` message of the branch configuration the
  instance was initialized with. A restore decodes it and rejects the snapshot
  with `-7` unless it describes the same branch configuration as the instance
  being restored.
- `application_state`: the guest's durable computation state.

The durable computation state of both examples is the processed row count,
encoded as eight little-endian bytes, which a restore requires exactly. The
batch a guest still buffers, its ACK tokens and input-column references, output
it has not emitted, and timeout handles belong to the live instance and are
never saved. A guest releases its buffered batch on `nervix_flush` and requests
its flush timeout each time it starts buffering a batch, so a restored instance
holds no timeout until it buffers input again.

The Rust guest is built on the `nervix-wasm-sdk` crate, which owns the
`GuestSnapshot` envelope: the processor returns its application state from
`save_state` and receives it back in `restore`. The Go guest encodes the same
envelope directly.

Filtering uses the processed row count as a global row ordinal. Rows with even
global ordinals are preserved and rows with odd global ordinals are dropped, so
state restoration changes subsequent keep/drop decisions. Preserved rows carry
their complete input row sidecars, including `source_token`, and unchanged
fields are emitted as input-column references. Dropped rows are listed in the
emitted envelope's `acked` sidecar.

Sentinel first values exercise the error paths in both guests: `-100` routes a
message error, `-200` reports a global error, `-300` fails the batch with a
guest error, `-400` is filtered like any other value but leaves the guest
unable to serialize its state, so every later `nervix_dump_state` fails with the
reason on the global-error channel, and `-500` asks the host for a new guest-state
lifetime instead of counting the row.
