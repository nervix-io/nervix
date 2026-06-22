# Introduction

Nervix is experimental software in active development. It is intended for evaluation, local testing, and design exploration. It is not suitable for real production workloads.

Nervix is a realtime relay processing system with a robust declarative DSL for defining data-flow graphs.

Nervix Relay Processing Language (NSPL) is used to declare the schemas, connections, runtime nodes, and external integrations that make up a Nervix graph:

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

The core runtime model is a graph of connected nodes:

- ingestors bring data into the system
- processors, unifiers, deduplicators, reingestors, and materializers transform or route that data
- emitters push results out to external systems

Current built-in transport integrations include Kafka, Pulsar, HTTP, Prometheus, RabbitMQ, Redis, MQTT, NATS, ZeroMQ, SQS, and WebSockets.

The connections between nodes are expressed through `RELAY`s.

Input data often mixes tenants, users, devices, accounts, or other business groups in the same external feed. Nervix uses `PARAMETERIZED BY <schema>` to process those groups independently. Branch-starting nodes add a `VALUES` map, such as `PARAMETERIZED BY tenant_user_branch VALUES { tenant = notifications.tenant, user_id = notifications.user_id }`.

`RELAY`s name the connections between runtime nodes. Ingestors and reingestors use `PARAMETERIZED BY <schema> VALUES { ... }` to compute a parameter group for each record. Relays and branch-preserving processors share the schema name without supplying values. When a group appears, Nervix runs that part of the graph as a branch for the group. The branch contains runtime relay instances and processing node state for that group, so batches, deduplicator history, window state, and downstream routing for one group do not interfere with other groups. An emitter drains records out of the graph. A reingestor can compute a new group and start downstream branches under that grouping.

Nervix already runs clustered deployments, schedules graph nodes across multiple servers, executes codecs in the runtime, replicates selected runtime state, and supports multi-node failover scenarios. It is still evolving, but it is beyond a parser-only prototype.

This book is split into two sections:

- [Manual](./manual.md): how to run Nervix and how to use its public surface
- [Architecture And Internals](./architecture-and-internals.md): control-plane, data-plane, and runtime implementation details

Start with the manual unless you are specifically trying to understand internals.

Nervix is licensed under the Fair Core License (FCL). See [License](./license.md).
