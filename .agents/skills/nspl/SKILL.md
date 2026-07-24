---
name: nspl
description: Design, author, explain, review, and troubleshoot Nervix configurations written in the Nervix Stream Processing Language (NSPL). Use when a user wants to configure domains, schemas, codecs, branches, relays, resources, clients, ingestors, processors, emitters, lookups, subscriptions, lifecycle commands, or complete Nervix streaming graphs. Produce current, valid NSPL and identify required external provisioning.
license: FCL-1.0-ALv2
---

# Configure Nervix with NSPL

Turn a user's streaming requirements into an explicit, deployable Nervix graph. Open the public
[Nervix NSPL documentation index](https://docs.nervix.io/llms.txt), then read the linked Markdown
needed for the request. Treat that versioned documentation as the authority; never reconstruct
clause order or connector options from memory.

## Gather the configuration contract

Establish these inputs before finalizing NSPL. Ask only for missing details that materially change
the graph; otherwise use conspicuous placeholders and state the assumptions.

- Domain: paced or unpaced, clock period/skew, and start behavior.
- Payload: sample input, wire format, exact internal field types, optional fields, and sensitive
  fields.
- Source and sink: connector kinds, externally provisioned entity names, endpoints, delivery/ACK
  expectations, ordering, and offsets.
- Isolation: unbranched or a concrete branch key, branch TTL, and optional instance limit.
- Processing: filtering, construction, deduplication, ordering, windows, inference, WASM,
  correlation, materialized state, lookup, generation, or repartitioning.
- Operations: batching/flush, error routes, credentials/TLS resources, observability, and session
  subscriptions.

Read [references/configuring-nervix.md](references/configuring-nervix.md), then use its routing
guidance to select the relevant Markdown entries from the public index.

## Assemble the graph

Run the control plane with `nervix-server` and submit configuration through the separate
`nervix-cli` client.

Build configuration in dependency order:

1. Create the domain, then select it with `USE <domain>;` as a separate client command.
2. Register and upload resources before statements that reference their versions or mounted files.
3. Define internal schemas, branch-key schemas, branches, wire schemas, and codecs.
4. Define clients, signaling protocols, virtual hosts/endpoints, and lookup models as needed.
5. Define relays before nodes that read or write them.
6. Define ingestors, processors, generators, and emitters in graph order.
7. Commit the graph, inspect it, and start the active domain only when prerequisites exist.

Use `BEGIN; ... COMMIT;` when sending multiple server statements in one request. Keep client-local
commands such as `USE` and resource uploads outside transactions. Do not imply that one undivided
request can mix those phases.

## Preserve NSPL semantics

- Declare exact schema types and nullability. Use explicit conversions; never invent implicit
  casts between wire, internal, branch, processor, lookup, state, and sink values.
- Use `IF ... THEN ... ELSE ... END` or searched/simple `CASE` for conditional values. Keep every
  result at one exact type; remember that omitted `CASE ELSE` yields a typed null and requires an
  optional destination.
- Use a separate wire schema and codec when transport shape differs from the internal runtime
  schema. Declare datetime encoding explicitly when required.
- Select `BRANCHED BY <branch>` or `UNBRANCHED` explicitly. Normal processors preserve their named
  branch; use a reingestor when the graph must repartition or remove branch grouping.
- Treat every route as a newly constructed output. Add `INHERIT` only where that node permits it,
  and initialize every required output field on set-only routes.
- Add a route-local message error policy. Add the required general/global policy for the chosen
  node.
- Add `FLUSH EACH <duration> MAX BATCH SIZE <bytes>` or `FLUSH IMMEDIATE` to every flush-based
  route. Windows use `WIDTH` and `STEP`; WASM output cadence is controlled by the guest.
- Require explicit sensitive-value leakage for external emission. Never place real credentials in
  an example unless the user explicitly supplied and requested them; prefer obvious placeholders.
- Preserve connector configuration as the documented string key/value surface. Do not translate
  options between different client libraries.
- List topics, queues, streams, tables, buckets, catalogs, namespaces, collections, and other
  external prerequisites separately. Nervix does not create them as a side effect of starting a
  node.

## Deliver usable configuration

When authoring a graph, provide:

1. Assumptions and external prerequisites.
2. Ordered command phases, separating client-local commands from transactional server statements.
3. Complete NSPL with consistent names and no unexplained ellipses. Use placeholders only for
   genuinely deployment-specific values such as endpoints, credentials, file paths, and external
   entity names.
4. A short verification sequence using the relevant `SHOW`, `DESCRIBE`, lookup, or subscription
   commands.

Before returning the configuration, trace every reference to its declaration and check schema,
branch, construction, flush, error, sensitivity, transaction, and external-provisioning contracts.
If the public docs do not establish a requested capability, say it is not documented as supported
instead of inventing syntax.
