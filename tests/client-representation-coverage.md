# Client Representation Coverage

Every target below is registered in `tests/bolero-targets.toml`, runs its production path in ordinary
tests and sanitizer-backed libFuzzer, and replays source-adjacent current-domain corpus seeds.
Generators and Arrow dependencies remain in their owning test harnesses.

| Current representation | Target | Complete oracle |
| --- | --- | --- |
| `ClientMessage` | `client-requests` | Complete typed request equality, request identity, prepared retry bytes and re-encoding equality; command/transaction positions, completion/choice context, subscriptions, clocks, producer and consumer requests |
| `Reply` | `client-replies` | Complete typed equality for every reply union member and disposition; ordered statement results and diagnostics, transaction/impact inspection, backup/restore/resource/WASM metadata, Row opening schemas and client I/O descriptions/outcomes |
| `ServerMessage` events | `client-events`, `client-row-views` | Complete event equality; retained graph text and entities; every Row cell, subscription generation and branch; unsolicited request identity remains absent |
| Upload start/chunk/reply | `client-streams` | Complete start and outcome equality, optional correlation, upload/version identity and opaque chunk bytes; retained slices survive frame and decoded-owner drop |
| Backup download request/start/chunk/complete/failure/redirect | `client-streams` | Complete typed values and exact shared chunk bytes |
| Restore start/chunk/reply | `client-streams` | Complete archive, execution identity, statement text, upload failure and structured command outcome; exact retained chunk bytes |
| Reply parts and assembly | `client-reply-transfer` | Concatenated chunks equal the original complete encoding; offsets, total, identity and final typed reply match; rejected identity and duplicate appends leave progress unchanged |
| Wire Row writer and views | `client-row-views` | Every scalar width and type, float bits, signed nanosecond timestamps, UTF-8/NUL, bytes, null, redaction, fixed arrays and nested/empty vectors; opening schema conformance and cloned buffer lifetime |
| Runtime Arrow to Row | `client-arrow-rows` | Original Arrow logical values and validity recursively compared with production cells; complete opening metadata, selected order, interleaved concrete branches, frame segmentation, explicit sensitivity redaction and retained frames |
| Arrow selection rejection | `client-arrow-selection` | Typed whole-input failure on bounds/order/duplicates and branch-count mismatch; a subsequent complete encoding succeeds |
| Shared C binding | `client-binding-rows` | Exact schema, column states, native scalar bits, variable bytes and offsets, branch values and retained frame bytes after releasing the original reference |
| Every FlatBuffers root under hostile bytes | `client-arbitrary-frames`, `client-frame-corruption` | Production verification and actual decoding under fixed frame, string, collection and depth budgets; typed failure or deterministic acceptance, including Row validation |
| Request union tags | `client-discriminators` | All byte-valued tags structurally verified before use; unknown tags return the owning typed union error |

Request, reply and event family sweeps compare their covered discriminants with the current schema's
declared unions. Reply parts and Row events have their own targets. Each valid-value target treats
an encoding, verification or decoding failure as a failure of its property.
Native producer and consumer openings include their committed generation and endpoint contract in
the complete equality assertion.

## Contract Boundaries

- Float equality means equal bits, including signed zero and NaN payloads. Null differs from a
  present zero or empty value. A redacted sensitive cell is compared with the explicitly redacted
  contract; it is never treated as an equal readable value.
- Arrow physical offsets and allocation layout are private. Its schema, logical cells, validity,
  selected order and branch association are the oracle. Selection of no rows emits no frames;
  every encoded Row batch is nonempty.
- The generic wire properties own no Arrow representation. Graph JSON, connector submission and
  consumer payloads remain exact opaque text or bytes. There is no Row JSON conversion or implicit
  cast in these assertions.
- The binding's scalar column copies use native byte order. Nested vectors remain readable from
  retained Row frames; asking for a scalar variable-width view of a vector fails with the typed
  type error.
- Malformed targets have a 16 KiB frame budget, 32 KiB transfer budget, 64 collection entries,
  2048 string bytes and depth 16. Inputs include truncation, offset/length corruption, unknown union
  tags and the frame-size boundary. They assert pure representation behavior. Session admission,
  cancellation and recovery retain their public Cucumber and concurrency evidence.

The public scenario `A malformed request is refused with a typed rejection and the session keeps
serving` checks invalid execution text, a split UTF-8 cursor, page sizes zero and above 100, then
valid completion and command requests, on both one- and three-node clusters. Existing native and
binding conformance scenarios exercise extrema, sensitive/null cells, interleaved branches,
command errors, closure, client I/O and clock attachment over live sessions.
