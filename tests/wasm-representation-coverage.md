# WASM Representation Coverage

Every target is registered in `tests/bolero-targets.toml` for ordinary randomized/corpus checks and
sanitizer libFuzzer. The same bounded generators, production APIs and complete assertions run in
both modes. All retained inputs contain synthetic data and describe the current shapes.

| Current conversion or boundary | Target | Complete oracle |
| --- | --- | --- |
| `BranchInit` and schema metadata | `wasm-branch-metadata` | Every domain/type/key byte, schema and field order, nested type, array length and optional flag; allocating and supplied-builder encoders agree |
| `Envelope` / `EnvelopeRef` | `wasm-envelopes` | Exact IPC bytes, every ordered row/token/source-token/ACK/NACK/message-error set, reasons, output relay and column reference; borrowed vectors lie inside the original frame and owned values survive its drop |
| `GuestSnapshot` | `wasm-snapshots` | Every opaque init and application-state byte, including present empty state; both encoders agree |
| Reset and restore ABI verdicts | `wasm-abi-codes` | Exact round trips for accepted/refused reset answers and envelope/application rejection verdicts; other codes remain absent |
| Host Model schema to ABI | `wasm-host-schema` | Full name, exact recursive type, nullability and field/schema order; sensitivity is a deliberate host-owned projection |
| Host envelope encoding, views and owned output | `wasm-host-envelopes` | Complete equality with both original host and protocol values, including retained shared output bytes after dropping the decoded owner |
| SDK input IPC and retained `InputBatch` | `wasm-sdk-input` | Complete Arrow schemas, logical cells and validity, float bits, timestamp units/timezone, nested arrays/vectors, sliced and empty batches, IPC bytes and sidecars after clone/drop |
| SDK shared generated pool and callback emit queue | `wasm-sdk-output` | Every generated logical value/bit/null, pool type/nullability, routed reference and sidecar; exact route and emission order; empty pool remains empty |
| SDK snapshot encode/restore | `wasm-sdk-snapshots` | Complete opaque bytes and branch configuration; a domain rename preserves the state contract; differing key/type/schema/output configuration is rejected before application restore |
| SDK restore rejection | `wasm-sdk-rejections` | Envelope, init-metadata and application-state contexts, retained protocol/application causes and distinct verdict codes; stateless restore accepts only empty application bytes |
| Archive `WasmStateDescriptor` and remote branch fields | `wasm-state-descriptors` | Complete domain/entity/schema/branch fingerprints, every named scalar/nested branch value with float bits, typed generation and revision |
| Inline/segmented stored checkpoint and placement key | `wasm-stored-checkpoints` | Every inline raw byte/revision or segmented length/digest/revision; fixed segmented encoding size; exact namespace and placement-key bytes; typed state/kind/entity/branch identity; a branch reset changes only its selected generation |
| Chunk key to placement/revision set | `wasm-stored-checkpoints` | One-way projection preserves the complete placement key and revision, discards the offset, and produces an exclusive cursor beyond every offset, including `u64::MAX` |
| Arbitrary protocol frames | `wasm-protocol-malformed` | Every short-header boundary and arbitrary bytes produce a typed error or a fully decoded value that reencodes to itself |
| Structured protocol corruption | `wasm-protocol-corruption` | All four families reject truncation, size mismatches, trailing bytes, invalid identifiers/root offsets and impossible vector counts before effects |
| Protocol tags and column descriptors | `wasm-protocol-tags` | Unknown union/type/source tags, missing collection elements and nonzero uninitialized column indices fail with their specific typed context |
| Malformed archive descriptor | `wasm-state-descriptors-malformed` | Current invalid generation, branch absence/presence/order/name and domain/entity fields fail with the exact owning field context; arbitrary record bodies are verified before conversion |
| Malformed stored checkpoint | `wasm-stored-checkpoints-malformed` | Bounded arbitrary storage bytes fail with the typed persistence error or decode to a complete value that reencodes to itself |

## Contract Boundaries

- Protocol IPC and application-state vectors are opaque. Arrow interpretation belongs to the SDK
  and host execution boundaries; the host's decoding of a guest's generated pool is covered by
  `wasm-generated-pools` and `wasm-generated-pools-malformed` in the
  [Arrow representation coverage map](./arrow-representation-coverage.md). Metadata-only column
  references can describe any `u32` index; actual pool/input indices and row/token membership are
  validated before host output effects.
- The host schema deliberately exposes name/type/nullability. Sensitivity stays in the host's
  execution plan and leakage validation. The generated pool deliberately has unnamed fields;
  the SDK preserves their exact data type and nullability.
- Arrow equality compares logical cells and null validity, with primitive value bits including
  signed zero and NaN payloads. Allocation layout, physical offsets and values beneath nulls are
  private. Nested list items use the contract's nonnullable `item` field.
- A snapshot envelope with empty application state is present. A zero-length raw host save has the
  separately documented no-save meaning. Synthetic callback sidecars exist only in envelopes;
  tests never persist live guards, tokens, attempts, timers or pending emissions.
- Placement keys contain the exact branch text, while the storage decoder exposes its typed
  fingerprint. The property compares both the complete encoded key and this intentional identity
  projection. It does not claim that a fingerprint reconstructs branch fields.
- Generators bound routes and sidecar sets to four, token sets to six, generated schema fields to
  sixteen and type depth to three. The deterministic protocol sweep additionally includes every
  scalar and nested collection kind. SDK columns contain at most eight rows and collection widths
  at most three. Malformed targets accept at most 4 KiB; verifier, archive and host execution
  policies remain owned by their production boundaries.
- These are serial representation properties. Existing Cucumber, Shuttle, Loom, Turmoil and Chaos
  owners establish callback completion, cancellation, durability, publication and recovery. No
  synchronization mechanism or ordering claim is added by this coverage.

The public scenario **Malformed WASM processor output reports a runtime error** covers truncated
headers of lengths one, four, eight and eleven on one- and three-node clusters. The current WASM
processing, save/restore/reset/rebinding and interleaved-branch scenarios remain the public runtime
evidence alongside these properties. `just test-wasm` builds both supported reference guests and
runs host, SDK and protocol checks; `just test-bolero wasm` runs all registered WASM targets.
