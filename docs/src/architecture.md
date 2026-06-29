# Architecture Overview

At a high level, Nervix has two major halves:

- a control plane that owns definitions, scheduling, lifecycle, and coordination
- a data plane that moves records through the runtime graph

The graph itself is built out of persisted NSPL models:

- `SCHEMA`
- `WIRE SCHEMA`
- `CODEC`
- `RESOURCE`
- `RELAY`
- `CLIENT`
- `VHOST`
- `ENDPOINT`
- `INGESTOR`
- `JUNCTION`
- `DEDUPLICATOR`
- `REINGESTOR`
- `EMITTER`

Those models are stored in the registry and scheduled into a `DomainSchedule`. The schedule says which nodes exist in a domain, which server is primary for each node, and which servers hold replicas.

This graph configuration is persisted with strong control-plane consistency. It is separate from runtime execution state and from the hot-path records moving through the graph.

The runtime then instantiates that schedule:

- ingestors attach to external systems or local endpoints
- relays buffer and route records
- junctions, deduplicators, reingestors, and materializers route or transform records between relays
- emitters encode records and publish them externally

Runtime execution has its own persistence boundary. Selected execution node state is persisted through periodic snapshots and replication, but in-flight message batches and ACK state are hot-path memory only.

`RESOURCE` sits between the control plane and the runtime. The control plane versions and replicates it across the cluster, while runtime nodes use its unpacked local directory form when a model depends on concrete files.

One concrete example is `VHOST` TLS:

- the control plane tracks uploaded certificate bundles as resource versions
- a `VHOST` can bind one of those resources, optionally pinned to an explicit version
- the data plane serves HTTPS and WSS from a dedicated HTTPS listener using the local replicated resource files

Internal node-to-node networking is also split explicitly:

- gossip membership remains on its own cluster transport
- the cluster API carries Raft and resource-transfer traffic and can run in plain HTTP or HTTPS mode
- the interconnect carries runtime payloads and control envelopes and can run in plain or TLS mode

For both cluster API and interconnect, plain and TLS listeners use separate addresses. The selected mode is an explicit process-level configuration choice.

The rest of this section splits control-plane semantics from data-plane semantics because that distinction is fundamental to how Nervix behaves.
