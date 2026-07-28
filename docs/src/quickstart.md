# Quickstart

Nervix is experimental software in active development. It is intended for evaluation, local testing, and design exploration. It is not suitable for real production workloads.

This section takes you from nothing to a running Nervix graph. It is a hands-on tutorial: each
step is a small, complete change that you apply to a live single-node cluster and verify before
moving on. You build one graph throughout — it starts as a trivial pass-through pipeline and grows
into a branched graph with conditional routing and structured error handling.

- [Installation](./quickstart-installation.md): getting Nervix onto your machine
- [Running Nervix](./quickstart-running.md): starting a single-node server, connecting a client,
  and starting the Kafka and Redis containers the tutorial talks to
- [Your First Pipeline: Kafka To Redis](./quickstart-first-pipeline.md): read JSON orders from a
  Kafka topic and publish them to a Redis Pub/Sub channel
- [Conditional Routing](./quickstart-conditional-routing.md): split the stream with a junction,
  route filters, and computed fields
- [Branched Processing](./quickstart-branched-processing.md): partition the stream per customer
  and deduplicate independently inside each branch
- [Error Routes](./quickstart-error-routes.md): capture failing records as structured errors on a
  dead-letter relay
- [HTTP Ingestion](./quickstart-http-ingestion.md): POST records straight at the server through a
  vhost-hosted endpoint
- [JAQ Transformations](./quickstart-jaq-transformations.md): reshape foreign JSON at the boundary
  with a jq-style codec
- [Protobuf Codecs](./quickstart-protobuf.md): decode binary payloads from uploaded `.proto`
  definitions
- [User-Defined Functions](./quickstart-udfs.md): package expression logic as a reusable compiled
  Roto function
- [Window Aggregates](./quickstart-windows.md): summarize the stream with per-customer sliding
  windows
- [Generators](./quickstart-generators.md): emit periodic records from materialized relay state
- [Correlators](./quickstart-correlators.md): join orders with payments across two streams
- [Altering A Running Graph](./quickstart-altering.md): change routing logic and evolve schemas
  without tearing anything down
- [Paced Domains](./quickstart-paced-domains.md): replay timestamped history on an accelerated
  domain clock

Every page links the [Manual](./manual.md) chapter that documents each feature in full: the
tutorial shows one working path, and the manual holds the reference.
