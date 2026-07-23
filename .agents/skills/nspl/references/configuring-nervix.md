# Configuring Nervix with NSPL

Use this reference to turn a deployment request into a complete Nervix configuration. Read the
linked public documents for exact syntax and connector-specific options; examples here describe
the configuration process, not a second grammar.

## Contents

- [Public documentation routes](#public-documentation-routes)
- [Configuration decisions](#configuration-decisions)
- [Graph construction order](#graph-construction-order)
- [Choosing processing nodes](#choosing-processing-nodes)
- [Correctness checks](#correctness-checks)
- [Verification and troubleshooting](#verification-and-troubleshooting)

## Public documentation routes

Always read the [NSPL Overview](https://docs.nervix.io/nspl-overview.html). Add the documents
relevant to the requested graph:

| User need | Public documentation |
| --- | --- |
| Domain timing and lifecycle | [Domains And Time](https://docs.nervix.io/domains-and-time.html) |
| Internal/wire schemas, codecs, JAQ, Protobuf, and type mapping | [Schemas And Codecs](https://docs.nervix.io/schemas-and-codecs.html) |
| Expressions, casts, and built-in functions | [Expression Functions](https://docs.nervix.io/filter-map-functions.html) |
| Branches, relays, capacity, TTL, and materialized state | [Streams And State](https://docs.nervix.io/relay.html) |
| Resources, uploads, mounts, and TLS files | [Resources](https://docs.nervix.io/resources.html) |
| Source transports, delivery modes, headers, and ingestor routes | [Ingestors](https://docs.nervix.io/ingestors.html) |
| Junctions, deduplication, ordering, windows, inference, WASM, correlation, reingestion, and error routes | [Runtime Nodes](https://docs.nervix.io/processors.html) |
| Timed generation from materialized state | [NSPL Overview](https://docs.nervix.io/nspl-overview.html), [Examples](https://docs.nervix.io/examples.html) |
| Sink transports, headers, direct values, flush/commit, and ACK behavior | [Emitters](https://docs.nervix.io/emitters.html) |
| Hash maps and lookup expressions | [Lookups](https://docs.nervix.io/lookups.html) |
| Session subscriptions | [Sessions](https://docs.nervix.io/sessions.html) |
| Metrics and runtime inspection | [Metrics And Observability](https://docs.nervix.io/metrics-and-observability.html) |
| Full graph examples | [Examples](https://docs.nervix.io/examples.html) and the [repository examples](https://github.com/nervix-io/nervix/tree/main/examples) |
| WASM guest ABI and output timing | [WASM Processor Guests](https://docs.nervix.io/wasm-processor-guests.html) |

Use the public book navigation to find newer chapters. Prefer the narrow feature document over an
old copied snippet.

## Configuration decisions

Capture these decisions before choosing syntax:

| Concern | Questions to answer |
| --- | --- |
| Domain | Is input paced by event time or admitted on arrival? What are period, skew, and restart semantics? |
| Input contract | What sample payload and wire format arrive? Which fields are optional or sensitive? |
| Runtime record | What exact internal type and nullability does each field have? |
| Isolation | Which fields form the branch key? How long should inactive branches live? Is an instance cap required? |
| Source | Which connector/client, external entity, offset policy, delivery mode, ordering, timestamp source, and headers are required? |
| Processing | Which records are filtered, transformed, deduplicated, reordered, aggregated, correlated, inferred, or enriched? |
| State | Which relays are materialized? Should missing state wait, skip, or use a typed default? |
| Output | Which connector/sink, payload shape, codec or direct mapping, headers, and sensitivity leaks are required? |
| Operations | What flush size/cadence, error behavior, TLS resources, metrics, and subscriptions are required? |

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
5. **Lifecycle:** use `START`, `START AT ...`, or `STOP` against the active domain as intended.

Within the graph transaction, declare dependencies before consumers:

1. internal and branch-key schemas;
2. named branches;
3. wire schemas and codecs;
4. clients, protocols, vhosts/endpoints, and hash maps;
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
| Match records from left and right relay sets | `CORRELATOR` |
| Change or remove branch grouping | `REINGESTOR` |
| Produce timed records from one materialized relay | `GENERATOR` |
| Publish records outside Nervix | `EMITTER` |
| Read a session-local filtered view | `CREATE SUBSCRIPTION` |

Use materialized relay dependencies when a node needs the latest record from another compatible
relay. Do not use them to scan across branches.

## Correctness checks

- Every referenced name is declared in the active domain before use.
- Every schema and wire schema is non-empty; types and optionality match exactly.
- Every codec explicitly handles any wire/internal datetime or shape difference.
- Every relay declares a schema and explicit branch selection.
- Every ordinary processor input/output uses the same named branch, or all are unbranched.
- Every route constructs all required output fields. `INHERIT` appears only on a transforming
  route; set-only routes use explicit `SET` assignments.
- Every field scope is valid for its node: use documented `input`, `message`, `output`, `branch`,
  `left`, `right`, `relay_state`, `metadata`, `error`, and `partial_output` availability.
- Every flush-based route has a flush policy and every route has a message error policy.
- Paced ingestors declare their timestamp source.
- External sensitive values use the required explicit leakage operation.
- Multiple server statements are transactional; client-local commands are outside the transaction.
- External entities and resource contents are provisioned before the graph is started.

## Verification and troubleshooting

Choose checks relevant to the configured graph:

- `SHOW CREATE <kind> <name>;` confirms the stored canonical definition.
- `DESCRIBE RELAY <relay>;` and `DESCRIBE RELAY <relay> WHERE (...);` inspect logical and concrete
  branch state.
- `SHOW RELAY <relay> MATERIALIZED STATE;` inspects materialized data and placement.
- `DESCRIBE INGESTOR`, processor-specific `DESCRIBE`, and `DESCRIBE EMITTER` inspect runtime state
  and edge metrics.
- `DESCRIBE RESOURCE` confirms uploads and versions.
- `LOOKUP <hash_map> KEY '<key>';` checks a loaded lookup.
- `CREATE SUBSCRIPTION ...` checks live relay output without modifying the graph.
- `SHOW CLUSTER STATUS;` checks cluster topology before diagnosing a graph as unavailable.

For a parse error, follow the reported expected token and compare clause order with the relevant
public example. For a validation error, trace exact types, declaration order, domain ownership,
branch compatibility, construction completeness, and connector capabilities. For missing data,
check domain lifecycle, source offsets, timestamps, filters, branch keys, route filters, flush
boundaries, and external entity provisioning in that order.
