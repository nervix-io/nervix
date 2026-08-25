# Introduction

Nervix is experimental software in active development. It is intended for evaluation, local testing, and design exploration. It is not suitable for real production workloads.

Nervix is a realtime stream processing system with a robust declarative DSL for defining data-flow graphs.

Nervix stream processing Language (NSPL) is used to declare the schemas, connections, runtime nodes, and external integrations that make up a Nervix graph:

- schemas
- wire schemas
- codecs
- relays
- clients
- ingestors
- processors
- emitters
- vhosts
- endpoints

NSPL has reached alpha. Its period of rapid, broad experimentation is over, so large
backward-incompatible language changes are no longer expected. The language will continue to
evolve, however, and focused breaking changes may still occur. See
[Language Stability](./nspl-overview.md#language-stability) for the current compatibility policy.

The core runtime model is a graph of connected nodes:

- ingestors bring data into the system
- processors, junctions, deduplicators, reingestors, and materializers transform or route that data
- emitters push results out to external systems

Current built-in transport integrations include Kafka, Pulsar, HTTP, Prometheus,
RabbitMQ, Redis, MQTT, NATS, ZeroMQ, SQS, and WebSockets, with Sentry event-envelope emission over
HTTP.

The connections between nodes are expressed through `RELAY`s.

Input data often mixes tenants, users, devices, accounts, or other business groups in the same
external feed. Nervix uses explicit `CREATE BRANCH` declarations to process those groups
independently. A branch names the key schema, such as `CREATE BRANCH by_tenant_user SCHEMA
tenant_user_branch TTL 5m`; ingestor and reingestor routes construct concrete keys with `BRANCHED
BY by_tenant_user SET ...`.

`RELAY`s name the connections between runtime nodes. Relays are declared as `BRANCHED BY <branch>` or `UNBRANCHED`; only ingestors and reingestors carry the `VALUES { ... }` key mapping that materializes concrete branch instances. When a group appears, Nervix runs that part of the graph as a branch instance for the group. The branch contains runtime relay instances and processing node state for that group, so batches, deduplicator history, window state, and downstream routing for one group do not interfere with other groups. An emitter drains records out of the graph. A reingestor can compute a new group and start downstream branches under that grouping.

Nervix already runs clustered deployments, schedules graph nodes across multiple servers, executes codecs in the runtime, replicates selected runtime state, and supports multi-node failover scenarios. It is still evolving, but it is beyond a parser-only prototype.

This book is split into six sections:

- [Quickstart](./quickstart.md): running Nervix and building your first graphs step by step
- [Client Tools](./client-tools.md): the command line client and the browser console
- [Manual](./manual.md): how to use Nervix's public surface
- [Rust WASM Guest SDK](./wasm-guest-sdk.md): writing custom WASM processor guests in Rust
- [Architecture And Internals](./architecture-and-internals.md): control-plane, data-plane, and runtime implementation details
- [Developing Nervix](./developing-nervix.md): working on Nervix itself from a repository clone

Start with the quickstart if you are new to Nervix, and with the manual unless you are
specifically trying to understand internals. Agents can start with the portable
[NSPL Agent Skill](./nspl-agent-skill.md).

Nervix is licensed under the Fair Core License (FCL). See [License](./license.md).
