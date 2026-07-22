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
- processors, junctions, deduplicators, reingestors, and materializers transform or route that data
- emitters push results out to external systems

Current built-in transport integrations include Kafka, Pulsar, HTTP, Prometheus, RabbitMQ, Redis, MQTT, NATS, ZeroMQ, SQS, and WebSockets.

The connections between nodes are expressed through `RELAY`s.

Input data often mixes tenants, users, devices, accounts, or other business groups in the same
external feed. Nervix uses explicit `CREATE BRANCH` declarations to process those groups
independently. A branch names the key schema, such as `CREATE BRANCH by_tenant_user SCHEMA
tenant_user_branch TTL 5m`; ingestor and reingestor routes construct concrete keys with `BRANCHED
BY by_tenant_user SET ...`.

`RELAY`s name the connections between runtime nodes. Relays are declared as `BRANCHED BY <branch>` or `UNBRANCHED`; only ingestors and reingestors carry the `VALUES { ... }` key mapping that materializes concrete branch instances. When a group appears, Nervix runs that part of the graph as a branch instance for the group. The branch contains runtime relay instances and processing node state for that group, so batches, deduplicator history, window state, and downstream routing for one group do not interfere with other groups. An emitter drains records out of the graph. A reingestor can compute a new group and start downstream branches under that grouping.

Nervix already runs clustered deployments, schedules graph nodes across multiple servers, executes codecs in the runtime, replicates selected runtime state, and supports multi-node failover scenarios. It is still evolving, but it is beyond a parser-only prototype.

This book is split into two sections:

- [Manual](./manual.md): how to run Nervix and how to use its public surface
- [Architecture And Internals](./architecture-and-internals.md): control-plane, data-plane, and runtime implementation details

Start with the manual unless you are specifically trying to understand internals.

Nervix is licensed under the Fair Core License (FCL). See [License](./license.md).
