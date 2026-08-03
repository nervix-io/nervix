# Configuring Nervix with NSPL

Use this reference to turn a deployment request into a complete Nervix configuration. Open the
public NSPL documentation index linked from `SKILL.md` and read its relevant Markdown entries for
exact syntax and connector-specific options. Examples here describe the configuration process,
not a second grammar.

## Contents

- [Public documentation routes](#public-documentation-routes)
- [Configuration decisions](#configuration-decisions)
- [Graph construction order](#graph-construction-order)
- [Choosing processing nodes](#choosing-processing-nodes)
- [Correctness checks](#correctness-checks)
- [Verification and troubleshooting](#verification-and-troubleshooting)

## Public documentation routes

Always read `NSPL Overview`. Add the indexed topics relevant to the requested graph:

| User need | Documentation index entry |
| --- | --- |
| Domain timing and lifecycle | `Domains And Time` |
| Internal/wire schemas, schema evolution, codecs, JAQ, Protobuf, and type mapping | `Schemas And Codecs` and `Control Plane` |
| Expressions, casts, and built-in functions | `Filter-Map Functions` |
| Trusted Roto user-defined expression functions | `User-Defined Functions` |
| Roto language syntax for UDF bodies | `Roto Language Reference` |
| Branches, relays, capacity, TTL, and materialized state | `Relay` |
| Resources, uploads, mounts, and TLS files | `Resources` |
| Source transports, delivery modes, headers, and ingestor routes | `Ingestors` |
| Junctions, deduplication, ordering, windows, inference, WASM, correlation, reingestion, and error routes | `Runtime Nodes` |
| Timed generation from materialized state | `NSPL Overview` and `Examples` |
| Sink transports, headers, direct values, flush/commit, and ACK behavior | `Emitters` |
| Hash maps and lookup expressions | `Lookups` |
| Session subscriptions | `Sessions` |
| Metrics and runtime inspection | `Metrics And Observability` |
| Full graph examples | `Examples` |
| WASM guest ABI and output timing | `WASM Processor Guests` |
| Writing Rust WASM guests with the SDK | `Rust WASM Guest SDK` |

Prefer the narrow indexed topic over an old copied snippet. Do not leave the immutable version
selected by the documentation index when following related material.

## Configuration decisions

Capture these decisions before choosing syntax:

| Concern | Questions to answer |
| --- | --- |
| Domain | Is input paced by event time or admitted on arrival? What are period, skew, and restart semantics? |
| Input contract | What sample payload and wire format arrive? Which fields are optional or sensitive? |
| Runtime record | What exact internal type and nullability does each field have? |
| Isolation | Which fields form the branch key? How long should inactive branches live? Is an instance cap required? |
| Source | Which connector/client, external entity, offset policy, delivery mode, ordering, timestamp source, and headers are required? |
| Processing | Which records are filtered, transformed, deduplicated, reordered, aggregated, correlated, inferred, enriched, or handled by a trusted Roto UDF? |
| State | Which relays are materialized? Should missing state wait, skip, or use a typed default? |
| Output | Which connector/sink, payload shape, codec or direct mapping, headers, and sensitivity leaks are required? |
| Operations | What input collection and output flush size/cadence, error behavior, TLS resources, metrics, and subscriptions are required? |

If the user supplied a real payload, derive wire and internal schemas field by field and call out
ambiguous types. Do not silently choose numeric width, datetime parsing, optionality, or branch
keys.

## Graph construction order

Use separate execution phases so transaction and active-domain rules stay clear.

1. **Domain bootstrap:** create one paced or unpaced domain as its own server command.
2. **Domain selection:** run `USE <domain>;` as a client-local command outside a transaction.
3. **Resources:** create resource declarations, then upload local directories as separate client
   actions.
4. **Graph transaction:** wrap multiple domain-owned server statements in `BEGIN;` and `COMMIT;`.
   A consecutive one-domain model-mutation run is one atomic candidate-graph update, including
   mixed `CREATE`, supported model `ALTER`, and `DROP`.
5. **Lifecycle:** use `START`, `START AT ...`, or `STOP` against the active domain as intended.

Within the graph transaction, declare dependencies before consumers:

1. internal and branch-key schemas;
2. named branches;
3. wire schemas and codecs;
4. clients, protocols, vhosts/endpoints, hash maps, and UDF declarations;
5. relays, including materialized relays;
6. ingestors;
7. branch-preserving processors, generators, and reingestors;
8. emitters.

Resource upload paths, credentials, broker addresses, and external object names are deployment
inputs. Keep placeholders obvious and list provisioning that must happen outside Nervix.

## Choosing processing nodes

| Desired behavior | NSPL graph element |
| --- | --- |
| Decode an external feed and construct initial branches | `INGESTOR` |
| Filter, transform, or fan out records without changing branch identity | `JUNCTION` |
| Suppress repeated keys for a time bound | `DEDUPLICATOR` |
| Order records by expressions within a time bound | `REORDERER` |
| Produce width/step aggregates | `WINDOW PROCESSOR` |
| Run an ONNX model | `INFERENCER` |
| Run custom guest processing | `WASM PROCESSOR` |
| Reuse trusted batch-column logic inside expressions | `UDF` |
| Match records from left and right relay sets | `CORRELATOR` |
| Change or remove branch grouping | `REINGESTOR` |
| Produce timed records from one materialized relay | `GENERATOR` |
| Publish records outside Nervix | `EMITTER` |
| Read a session-local filtered view | `CREATE SUBSCRIPTION` |

Use materialized relay dependencies when a node needs the latest record from another compatible
relay. Do not use them to scan across branches.

## Correctness checks

- Every referenced name is declared in the active domain before use.
- Every schema and wire schema is non-empty; types and optionality match exactly. JSON, CBOR, and
  AVRO wire schemas are separate entity kinds even when their names coincide. Every wire schema
  declares `MODE STRICT|LOOSE` after its name, and a mode-only change uses `ALTER WIRE <format>
  SCHEMA <wire_schema> MODE STRICT|LOOSE`.
- Every codec explicitly handles any wire/internal datetime or shape difference. Every JAQ-backed
  codec uses `WITH JAQ TRANSFORMATIONS` and declares `ON INGESTION`, `ON EMITTING`, or both in that
  order.
- Every relay declares a schema and explicit branch selection.
- Every ordinary processor input/output uses the same named branch, or all are unbranched.
- Every multi-input emitter source declares the same payload schema. Its sources may use different
  branch names, but each source retains its own branch through collection and external publish;
  node-wide materialized dependencies and message-error relays match every source branch exactly.
- Every optional `COLLECT FOR` policy follows the complete relay input list, has a positive
  duration, and is absent when immediate input execution is intended. Correlator sides are checked
  independently; ingestors never declare input collection.
- Every route constructs all required output fields. `INHERIT` appears only on a transforming
  route; set-only routes use explicit `SET` assignments.
- Every field scope is valid for its node: use documented `input`, `message`, `output`, `branch`,
  `left`, `right`, `relay_state`, `metadata`, `error`, and `partial_output` availability.
- Every `IF` condition and searched `CASE WHEN` condition is Boolean; simple `CASE` match values
  have the operand's exact type; all result arms have one exact type.
- Every UDF call uses `udf::<name>(...)` and has the declaration's exact arity and argument types.
  UDFs using the domain clock or randomness declare `VOLATILE`; untrusted third-party code remains
  in a WASM processor.
- Every agent-generated UDF includes Roto `test` blocks. Those tests run during `CREATE UDF` and
  must pass before the declaration is persisted.
- Every flush-based route has a flush policy and every route has a message error policy.
- Every `MAX BATCH SIZE` is chosen as a logical Arrow payload boundary, excluding unused buffer
  capacity and object overhead. Delivery-mode `MAX <n>` appears only on `ACK PARALLEL`, never on
  `NO_ACK`.
- Every Sentry emitter references a `TYPE SENTRY` client with a project DSN, encodes one event JSON
  object per record, and has no `write_header` invocation.
- Every custom WASM guest is built for the current ABI, accepts
  `nervix_process_batch(ptr, size)`, validates that exact range against its reusable buffer, and
  declares positive `MAX FUEL` then `MAX MEMORY` limits immediately after `FILE`.
- Paced ingestors declare their timestamp source.
- External sensitive values use the required explicit leakage operation.
- Transactions queue multiple server statements, while only consecutive one-domain model-mutation
  runs receive atomic candidate-graph validation and persistence; client-local commands are outside
  the transaction.
- Interdependent schema evolution is one transaction, preserves ALTER operation order, and includes
  all wire schema, internal schema, codec, and dependent-node mutations needed by the new graph.
- Running-domain schema ALTER quiescing is automatic; do not emit `PAUSE` or `RESUME` syntax.
- External entities and resource contents are provisioned before the graph is started.

## Verification and troubleshooting

Choose checks relevant to the configured graph:

- `SHOW CREATE <kind> <name>;` confirms the stored canonical definition.
- `DESCRIBE RELAY <relay>;` and `DESCRIBE RELAY <relay> WHERE (...);` inspect logical and concrete
  branch state.
- `SHOW RELAY <relay> MATERIALIZED STATE;` inspects materialized data and placement.
- `DESCRIBE INGESTOR`, `DESCRIBE JUNCTION`, other processor-specific `DESCRIBE` commands, and
  `DESCRIBE EMITTER` inspect runtime state and edge metrics.
- The observability server's `/metrics` endpoint reports `nervix_branch_instances` per domain,
  branch declaration, and physical node, plus `nervix_branch_evictions_total` split by
  `reason="lru"` or `reason="ttl"`.
- `DESCRIBE RESOURCE` confirms uploads and versions.
- `SHOW UDFS`, `DESCRIBE UDF <name>`, and `SHOW CREATE UDF <name>` inspect trusted Roto functions.
  Creation itself is the test gate: a rejecting Roto `test` block prevents persistence.
- `LOOKUP <hash_map> KEY '<key>';` checks a loaded lookup.
- `CREATE SUBSCRIPTION ...` checks live relay output without modifying the graph.
- `SHOW CLUSTER STATUS;` checks cluster topology before diagnosing a graph as unavailable.

For a parse error, follow the reported expected token and compare clause order with the relevant
public example. For a validation error, trace exact types, declaration order, domain ownership,
branch compatibility, construction completeness, and connector capabilities. For missing data,
check domain lifecycle, source offsets, timestamps, filters, branch keys, route filters, flush
boundaries, input collection boundaries, and external entity provisioning in that order.
