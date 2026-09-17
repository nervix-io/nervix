# Rust WASM Guest SDK

The `nervix-wasm-sdk` crate is the supported way to write Nervix WASM
processor guests in Rust. A guest implements the `Processor` trait for one
branch-local type and registers it with `export_processor!`; the SDK owns the
complete guest side of the [WASM processor ABI](./wasm-processor-guests.md):
the reusable linear-memory buffer, FlatBuffers envelope encoding and decoding,
the emit queue, the global-error channel, panic conversion, error-state
latching, and guest snapshot plumbing. Processor code only sees typed inputs,
typed outputs, and the branch configuration.

Use this chapter to write a Rust guest. The
[WASM Processor Guests](./wasm-processor-guests.md) chapter remains the
authoritative wire contract the SDK implements and the reference for guests
written in other languages.

## Installing The SDK

The SDK is not published to crates.io yet. Install it as a git dependency from
the Nervix repository and pin a revision so builds stay reproducible:

```toml
[package]
edition = "2024"
name = "my-wasm-guest"
version = "0.1.0"

[lib]
crate-type = ["cdylib"]

[dependencies]
arrow-array = "58"
nervix-wasm-sdk = { git = "https://github.com/nervix-io/nervix", rev = "<commit>" }

[profile.release]
codegen-units = 1
lto = true
opt-level = 3
panic = "abort"
strip = true
```

Guests are native `wasm32-unknown-unknown` modules without WASI:

```bash
rustup target add wasm32-unknown-unknown
cargo build --target wasm32-unknown-unknown --release
```

The finished module is
`target/wasm32-unknown-unknown/release/my_wasm_guest.wasm`.

## A Minimal Guest

This guest passes every input field through unchanged and appends one
generated `bucket` column. It assumes the destination schema lists the input
fields first and a required `bucket STRING` field last:

```rust
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int32Array, StringArray};
use nervix_wasm_sdk::{
    BranchContext, GuestContext, GuestError, InputBatch, OutputColumnRef, OutputEnvelope,
    Processor,
};

struct SeverityBucketer;

impl Processor for SeverityBucketer {
    fn create(branch: &BranchContext) -> Result<Self, GuestError> {
        if branch.output_schemas().len() != 1 {
            return Err(GuestError::failed(
                "severity_bucketer expects exactly one TO relay",
            ));
        }
        Ok(Self)
    }

    fn process_batch(
        &mut self,
        ctx: &mut GuestContext<'_>,
        input: InputBatch,
    ) -> Result<(), GuestError> {
        let mut buckets = Vec::new();
        for batch in input.batches() {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| GuestError::failed("input column 0 must be a required I32"))?;
            for row in 0..values.len() {
                buckets.push(if values.value(row) >= 100 { "HIGH" } else { "LOW" });
            }
        }

        let mut output = OutputEnvelope::new();
        let bucket = output.add_generated_column(
            Arc::new(StringArray::from(buckets)) as ArrayRef,
            false,
        );
        let input_fields = u32::try_from(ctx.branch().input_schema().fields.len())
            .map_err(|_| GuestError::InvalidSize)?;
        let mut columns = (0..input_fields)
            .map(|column_index| OutputColumnRef::Input { column_index })
            .collect::<Vec<_>>();
        columns.push(OutputColumnRef::Generated {
            column_index: bucket,
        });
        let relay = ctx.branch().output_schemas()[0].name.clone();
        output.add_route(relay, columns, input.acks().clone());
        ctx.emit(output)
    }
}

nervix_wasm_sdk::export_processor!(SeverityBucketer);
```

Reusing `input.acks().clone()` preserves the complete host row sidecar: every
output row keeps its tokens and `source_token`, so input-column references and
route expressions such as `input.field` keep working, and pre-existing
terminal decisions pass through unchanged.

## Lifecycle

The host creates one guest instance per concrete branch. The SDK maps the ABI
callbacks onto the trait:

| Trait method | When the host calls it |
| --- | --- |
| `create` | Branch initialization, with the decoded `BranchInit` payload as a `BranchContext`: domain, serialized branch key, and the exact input and output schemas. |
| `process_batch` | One input envelope, decoded into an `InputBatch`: Arrow record batches and the ACK sidecar. |
| `on_timeout` | A previously requested domain-clock timeout fired. |
| `flush` | The runtime is quiescing this branch for a handoff or shutdown. Emit everything the processor still buffers; whatever it keeps stays unacknowledged until the branch resumes. |
| `save_state` | After the host dispatches the output of a successful `process_batch` or `on_timeout`, and when it checkpoints the branch for an ownership handoff. Return the processor's durable computation state, or an error when it cannot be serialized. |
| `restore` | The host recreates the branch instance, for example after a restart or on the node a branch moved to, and hands it the application state saved last. |

Keep all state branch-local. Never aggregate across branch keys inside one
guest, and reject init payloads whose schemas the guest does not implement
instead of accepting them silently.

## Building Output

`OutputEnvelope` builds one output group:

- `add_generated_column(array, optional)` places one immutable Arrow array in
  the common generated pool and returns the index used by
  `OutputColumnRef::Generated` references. The SDK encodes the pool with the
  required empty field names; `optional` must match the nullability of every
  destination field that references the column. Several routes, or several
  fields in one route, may reference the same index.
- `add_route(relay, columns, acks)` adds one routed output. `columns` align
  positionally with the destination relay schema:
  `OutputColumnRef::Input { column_index }` references an unchanged input
  column without re-serializing its values, `Generated` references the pool,
  and `Uninitialized` leaves an optional destination field to materialize as
  typed NULLs.
- `GuestContext::emit` encodes the envelope and queues it; the host collects
  every queued envelope after the callback returns. Routes that need different
  generated row counts or ordering belong in separate emitted envelopes.

Row lineage follows the ABI contract: preserve the complete row sidecar for
rows you keep, put dropped rows' tokens in `acked`, failed rows in `nacked`
with a reason, and rows for the processor's `ON MESSAGE ERROR` policy in
`message_errors`. The full validation rules are in
[WASM Processor Guests](./wasm-processor-guests.md).

`ProcessorTypeArrow::arrow_data_type` maps ABI schema types to the exact Arrow
types Nervix compares, including `Datetime` as
`Timestamp(Nanosecond, "+00:00")`. Use it when constructing generated arrays
for non-trivial destination fields.

## Error Handling

Three error levels map onto the processor's declared policies:

- Message errors are lineage: route the affected tokens through the
  `message_errors` sidecar of an emitted envelope. The host applies
  `ON MESSAGE ERROR`.
- `GuestContext::report_global_error(reason)` reports a global processor error
  while the current callback still succeeds. The host applies
  `ON GLOBAL ERROR` and the guest latches into error state.
- Returning `Err(GuestError::failed(reason))` from a callback reports the
  reason through the global-error channel, fails the callback, and latches the
  guest into error state. Other `GuestError` variants return their negative
  ABI code without latching.

Panics are converted into latched global errors; with `panic = "abort"` they
surface as Wasmtime traps, which the host also treats as global errors.

## Guest State

The runtime persists and replicates guest state so it can recreate a branch
instance. `save_state` returns the processor's durable computation state:
whatever a recreated instance needs to continue the computation, such as
counters, aggregates, or open windows. The SDK stores those application bytes
in its snapshot together with the branch configuration the instance was
initialized with, and `restore` receives the application bytes back.

Save computation state, never execution state. The input a processor still
buffers, its ACK tokens and row sidecars, output envelopes it has not emitted,
and timeout handles belong to the live instance and mean nothing to one created
later. Release buffered input from `flush`; ACK tokens are host-local runtime
capabilities that are never persisted or replicated, and when ACK state is lost
with a processor owner, the upstream ingestor reacts according to its delivery
mode and retry policy. The SDK keeps its own execution state out of the
snapshot as well: a recreated instance starts without error state, with an
empty emit queue, and without pending timeouts.

`save_state` is fallible. Return an error when the state cannot be serialized:
Nervix reports a `state snapshot`
[failure](./wasm-processor-guests.md#failure-diagnostics) and keeps the state
saved last, so a recreated instance restores that state rather than a save that
never completed. The error follows the same rules as a callback error:
`GuestError::failed` puts its reason on the global-error channel and latches
the instance into error state.

`restore` must stay strict: reject bytes the current build cannot interpret
instead of silently resetting. Empty application bytes are the valid state of a
stateless processor, not a request to discard state: the default `save_state`
returns them, the SDK still wraps them in a snapshot, and the default `restore`
accepts them and rejects anything else.

The SDK reports every restore verdict with the reserved
[`nervix_load_state` codes](./wasm-processor-guests.md#contract-summary): a
snapshot it cannot decode, including its branch configuration, or a snapshot
taken under a different branch configuration than the one the instance was
just initialized with, is a rejected snapshot envelope, and any error `restore`
returns is a rejected application state with that error as the reason. Nervix
keeps the saved state after a rejection and reports it under the
`snapshot envelope decoding` or `application state restoration`
[failure stage](./wasm-processor-guests.md#failure-diagnostics), so return an
error from `restore` only when the saved state itself is unusable.

## Timeouts

`GuestContext::request_timeout(delay)` asks the host for a domain-clock
timeout and returns its handle; when the domain clock reaches the requested
time the host calls `on_timeout` with that handle. Output emitted from the
timeout callback is routed exactly like batch output. WASM processors have no
`FLUSH` clause — guest-requested timeouts and emitted envelopes own the output
cadence.

Timeouts belong to the branch instance that requested them and are never part
of its saved state. A recreated instance starts without pending timeouts, so a
processor whose restored state needs a timer requests it again from its next
`process_batch` or `on_timeout` call.

## Deploying A Guest

Upload the built module as a resource version and reference it from the
processor declaration:

```sql
CREATE RESOURCE normalizer;
UPLOAD RESOURCE normalizer VERSION 'path/to/resource-directory';

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
    ON MESSAGE ERROR LOG
  ON GLOBAL ERROR LOG;
```

`VERSION` is mandatory. `VERSION LATEST` binds the highest completed version when the statement is
applied; either form stores one concrete version, so a later upload does not replace the module a
processor runs.

See [Resources](./resources.md) for uploads and
[Runtime Nodes](./processors.md) for the complete `CREATE WASM PROCESSOR`
grammar.

## Reference Guest

The complete reference guest — global-row filtering across buffered batches, a
generated column shared by multiple routes, message errors, global errors, a
failing state save, and a row ordinal as its only saved state — is
`examples/wasm-processors/rust-guest` in the Nervix repository. Build it with:

```bash
cargo build \
  --manifest-path examples/wasm-processors/rust-guest/Cargo.toml \
  --target wasm32-unknown-unknown \
  --release
```

The module is produced at
`examples/wasm-processors/rust-guest/target/wasm32-unknown-unknown/release/nervix_wasm_processor_rust_guest.wasm`.
