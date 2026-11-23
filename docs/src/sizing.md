# Sizing A Node

A Nervix node sizes itself from two inputs: how many CPUs it has and how much memory it may use.
It detects both when it starts, and either can be overridden. Every CPU-class and database worker
count, thread count, memory budget, client credit, operation limit and memory-pressure watermark the
node is not given explicitly derives from them, while the three storage worker pools keep fixed
sizes, and the node refuses to start when its memory cannot hold what it derived. Its other node settings have fixed defaults, while cluster parameters hold one value for
the whole cluster and prerequisites must agree between the nodes that run together.

This chapter is the operator's view of those settings. [Node Settings](node-settings.md) owns their
classes, validation and change contracts, [Capacity Planning](capacity-planning.md) sizes a graph's
workload within these budgets, and the [nervix-server Reference](nervix-server-reference.md) lists
every option `nervix-server` accepts. Every setting option has an environment variable named
`NERVIX_` followed by the option in upper case with its hyphens replaced by underscores, such as
`NERVIX_CPU_COUNT` for `--cpu-count`.

## The Two Inputs

| Input | Option | Environment variable | Without the option |
| --- | --- | --- | --- |
| CPU count | `--cpu-count` | `NERVIX_CPU_COUNT` | The parallelism the operating system reports for the process: the CPUs it may run on, limited by its control group's CPU quota |
| Memory limit | `--memory-limit` | `NERVIX_MEMORY_LIMIT` | The smallest memory limit of the process's control group and the groups above it, in the unified hierarchy or the version 1 memory hierarchy, or its physical memory when no limit is smaller |

In a container, the limits its runtime sets are the node's inputs: Docker's `--cpus` and `--memory`,
or the CPU and memory limits of a Kubernetes container. A request is not a limit. A CPU limit
counts in whole CPUs, rounded down and at least one, so a `1500m` limit gives one CPU. A container
with no CPU limit counts every CPU it may run on, and one with no memory limit counts the physical
memory of its host.

An override replaces detection and must be greater than zero; `--memory-limit` takes a byte
quantity such as `3GiB`. An override changes only what the node sizes itself from and the claim it
announces. It limits nothing: the container's own limits still apply to the process. Set one when
the detected value is not the capacity the node should be sized and placed for.

A value the operating system does not report is unknown. Derivation then counts one CPU, and uses
the fixed defaults below for every memory budget. A node with unknown memory checks no budget
against memory, and runs no memory-pressure supervisor unless both watermarks are given.

Both values are part of the capability claim the node commits for every run.
`DESCRIBE CLUSTER` shows them as `cpu-count=` and `memory-bytes=`, or `unknown`, and
[`SHOW NODES`](placement.md#selecting-nodes) lists them with each node's record. Placement weighs a
node's share of independent units by them, as
[Capacity At The Load Tier](placement.md#capacity-at-the-load-tier) describes. The startup log line
`the startup preflight passed`, at `info`, reports the memory limit and where it came from, the
total the node checks against it and the capabilities it resolved.

## What The CPU Count Sizes

| Capacity | Option | Default |
| --- | --- | --- |
| Control CPU workers | `--execution-control-cpu-workers` | 1 |
| Credentials CPU workers | `--execution-credentials-cpu-workers` | 1 |
| Data, extension and bulk CPU workers | `--execution-data-cpu-workers`, `--execution-extension-cpu-workers`, `--execution-bulk-cpu-workers` | The CPUs left after the control and credentials workers, divided among the three |
| Pending jobs of each class | `--execution-<class>-pending-jobs` | 64 per worker of the class, at most 1,024 |
| Node and consensus database workers | `--database-workers`, `--raft-database-workers` | The CPU count, at most 4 |
| Runtime worker threads | `--runtime-worker-threads` | The CPU count |
| Blocking-thread ceiling | `--runtime-max-blocking-threads` | Every execution worker together, at least 512 |

The data, extension and bulk classes divide what the control and credentials workers leave as
evenly as possible, a remainder going to data first and then to extension, and each keeps at least
one worker. Explicit counts are kept as given, and the classes left out divide what remains. The
five CPU classes together hold at most the larger of the CPU count and five: on a small node each
still has its own place. A total above that bound refuses the start, naming both numbers, such as
`the 11 CPU admission places exceed CPU count 8; each of the five independent classes needs at
least one place`. These counts bound admission; they do not promise an idle core.

The three storage classes do not follow the CPU count. Consensus storage has one worker, and
filesystem and connector storage two each, through `--execution-consensus-storage-workers`,
`--execution-filesystem-storage-workers` and `--execution-connector-storage-workers`. The
`<class>` of a pending-jobs option is one of `control-cpu`, `credentials-cpu`, `data-cpu`,
`extension-cpu`, `bulk-cpu`, `consensus-storage`, `filesystem-storage` and `connector-storage`.

An explicit `--runtime-worker-threads` cannot exceed the CPU count, and an explicit
`--runtime-max-blocking-threads` must hold every execution worker. With an unknown CPU count, every
CPU class has one worker and the database workers and runtime worker threads are one each.

An 8-CPU node therefore runs one control and one credentials worker, two data, two extension and
two bulk workers, with 64, 64, 128, 128 and 128 pending jobs; four workers for each database;
eight runtime worker threads; and a blocking-thread ceiling of 512. A 16-CPU node runs five data,
five extension and four bulk workers.

## What Memory Sizes

The node divides its memory into ten independent pools. A pool never borrows from another, so work
that fills one cannot take what another class needs.

| Pool | Option | Share of memory | At least | With unknown memory | Holds |
| --- | --- | --- | --- | --- | --- |
| `intake` | `--execution-intake-memory-bytes` | 1/16 | 128 MiB | 128 MiB | Public request input a listener retains |
| `management` | `--execution-management-memory-bytes` | 1/128 | 64 KiB | 8 MiB | Management messages, licensing exchanges and small storage jobs |
| `commands` | `--execution-commands-memory-bytes` | 1/32 | 24 MiB | 24 MiB | Consensus commands and replication batches |
| `relay` | `--execution-relay-memory-bytes` | 1/16 | | 192 MiB | Relay batches being encoded, decoded and delivered |
| `state_read` | `--execution-state-read-memory-bytes` | The relay pool | | The relay pool | Decoded materialized snapshots |
| `state_read_cache` | `--execution-state-read-cache-memory-bytes` | The relay pool | | The relay pool | Retained materialized source views |
| `bulk` | `--execution-bulk-memory-bytes` | 1/32 | 32 MiB | 32 MiB | Whole transfers and large reads, such as resource archives and snapshots |
| `bulk_window` | `--execution-bulk-window-memory-bytes` | 1/64 | 16 MiB | 16 MiB | The receive window of every open bulk stream |
| `restore_metadata` | `--execution-restore-metadata-memory-bytes` | 1/8 | | 2 GiB | Archive descriptions and restore planning |
| `credentials` | `--execution-credentials-memory-bytes` | 1/256 | 19 MiB | 19 MiB | Password work and mounted NATS credentials files |

A pool holds at most 4,294,967,295 bytes. A derived pool stops there, and a larger explicit value
refuses the start.
[Bounded Execution And Transient Memory](capacity-planning.md#bounded-execution-and-transient-memory)
explains what each class admits.

The limits of single operations, the bounds a node accepts from its peers, the client credit and
the memory-pressure watermarks follow from the same memory:

| Capacity | Option | Default |
| --- | --- | --- |
| Encoded body and decoded data of one relay operation | `--execution-relay-encoded-bytes`, `--execution-relay-decoded-bytes` | One sixth of the relay pool each, at most 32 MiB |
| Scratch of one relay operation | `--execution-relay-scratch-bytes` | One twelfth of the relay pool, at most 16 MiB |
| Resident replication batches | `--execution-resident-replication-batches` | The 2 MiB replication batches the commands pool holds beside one more, from 1 to 4 |
| Bulk chunk | `--execution-bulk-chunk-bytes` | 1/512 of the bulk pool, at most 64 KiB |
| Largest relay body and decoded batch the node accepts from a peer | `--interconnect-relay-batch-bytes`, `--interconnect-relay-decoded-bytes` | The node's own relay operation limits |
| Largest bulk chunk the node accepts from a peer | `--interconnect-bulk-chunk-bytes` | The node's own bulk chunk |
| Producer and consumer byte credit | `--client-producer-node-bytes`, `--client-consumer-node-bytes` | 1/32 of memory each, at least 32 MiB; 128 MiB each with unknown memory |
| Memory-pressure high and low watermarks | `--memory-high-watermark`, `--memory-low-watermark` | 7/8 and 3/4 of memory; none with unknown memory |

A peer receive bound given explicitly cannot exceed the node's own operation limit, and client
credit cannot exceed known memory. The watermarks are given together or not at all, the low one
below the high one, and the high one within known memory; an explicit pair keeps its exact values.
They govern the bytes the node's allocator reports allocated, pausing every ingestor at the high
watermark until allocation falls to the low one, and reserve nothing.
[Execution Capabilities](node-settings.md#execution-capabilities) owns these relationships.

A 1 GiB node derives these pools: `intake` 128 MiB, `management` 8 MiB, `commands` 32 MiB,
`relay`, `state_read` and `state_read_cache` 64 MiB each, `bulk` 32 MiB, `bulk_window` 16 MiB,
`restore_metadata` 128 MiB and `credentials` 19 MiB, 555 MiB together. Its relay operations take
one sixth and one twelfth of that 64 MiB relay pool, it keeps four resident replication batches and
a 64 KiB bulk chunk, it grants 32 MiB of producer credit and 32 MiB of consumer credit, and it
pauses ingestors at 896 MiB and resumes them at 768 MiB.

Between their minimums and the ceiling of a pool, the ten pools take 119/256 of memory and the two
credits another 1/16, so a node's budgets come to a little over half of its memory:

| Memory | Pools | Client credit | Database caches | Checked total | High and low watermarks |
| --- | --- | --- | --- | --- | --- |
| 1 GiB | 555 MiB | 64 MiB | 64 MiB | 683 MiB | 896 MiB and 768 MiB |
| 4 GiB | 1,907 MiB | 256 MiB | 64 MiB | 2,227 MiB | 3.5 GiB and 3 GiB |
| 16 GiB | 7,616 MiB | 1 GiB | 64 MiB | 8.5 GiB | 14 GiB and 12 GiB |

These are ceilings that admission charges work against, not memory allocated at startup. Graph
state, engines and thread stacks need memory beside them; the 8 MiB `--runtime-thread-stack-size`
of each runtime thread, for example, is not part of any pool. Neither are the public listeners'
transport buffers, which their [listener bounds](node-settings.md#listener-bounds) cap instead:
gRPC clients hold at most `--listener-max-connections` times `--grpc-connection-window-bytes` of
input on a node, 1 GiB at the defaults. Nor are the databases' memtables, the buffers that collect
new writes before they reach disk. Each of the 15 keyspaces, 11 in the node database and 4 in the
consensus database, fills one up to 64 MiB, and a keyspace whose writes outpace its disk writes
keeps up to four full ones waiting before its writers pause. Nervix does not limit them, so a
write-heavy node leaves headroom for them beside its checked total. Nor is a WASM guest's memory.
It takes address space, `--wasm-memory-reservation-bytes` for a memory that fits in it, or its
current size and `--wasm-memory-growth-reservation-bytes` for one that has to move to grow, with
`--wasm-memory-guard-bytes` of unmapped guard on each side, and what it commits stays within its
processor's `MAX MEMORY` ([WASM Processor Guests](wasm-processor-guests.md)).

## What A Node Needs To Start

Before it opens a store or binds a listener, a node adds up the ten pools, both client credits and
the caches of its node database and consensus database, `--database-cache-bytes` and
`--raft-database-cache-bytes`, 32 MiB each by default. With a known memory limit the total must fit
it. When it does not, the node exits with status `1`, reporting
`the startup preflight refused this node` above one of these causes:

```text
the execution memory budgets of 229.25MiB exceed node memory 32MiB
memory-limit (500MiB) cannot hold the node's memory budgets of 507.16MiB
```

The first judges the pools alone, the second everything the node checks. With every default, the
smallest memory limit that holds the total is a little over 510 MiB. An explicit value is never
reduced to fit: lower it, or give the node more memory.

The node also measures the volumes of its database directory, `--db-path`, and its temporary
directory, `--temp-dir`. The restore-staging quota and the snapshot staging quotas follow those
volumes unless given, and the committed resource quotas must fit the database volume. The measured
database volume is part of the node's capability claim, so `ALTER CLUSTER SET` refuses a
`RESOURCE_MAX_ARCHIVE_BYTES` or `RESOURCE_MAX_EXTRACTED_BYTES` above the database volume any
admitted node's committed claim carries, naming the node:
`resource-max-extracted-bytes (8EiB) exceeds node 'node-1' database volume (1.82TiB)`. Each claim
records the size its run measured at start, so a grown volume counts once that node restarts.
[What A Node Needs To Start](capacity-planning.md#what-a-node-needs-to-start) sizes the volumes, and
[The Startup Preflight](node-settings.md#the-startup-preflight) lists every check in order.

## Cluster Parameters

A cluster parameter is what the whole cluster accepts or promises, so it has one committed value
rather than one per node. The node that bootstraps a new cluster seeds the record from its options,
and from then on only `ALTER CLUSTER SET` changes it. Each parameter's seed option is its name in
lower case with hyphens, such as `--replica-count` (`NERVIX_REPLICA_COUNT`) for `REPLICA_COUNT`, and
its help in the [reference](nervix-server-reference.md) names its first-bootstrap default. A seed
option refuses every other start, a later start of the bootstrapping node included, even when its
value equals the default:

```text
replica-count is a first-bootstrap seed; use ALTER CLUSTER SET
```

Keep seed options out of the environment a node restarts with. Change the committed record with
one statement instead:

```plexus
ALTER CLUSTER SET REPLICA_COUNT = 2, STATE_SNAPSHOT_INTERVAL = 10s;
```

The statement needs no selected domain and runs outside domain transactions. Its assignments
commit together or not at all, and it assigns each parameter at most once. A duration is a Plexus
[duration literal](domains-and-time.md#duration-literals) such as `250ms` or `13m 49s`, a count is
an integer, a byte limit is a byte quantity such as `64KiB` or `2GiB`, and a TLS list is a
parenthesized list of quoted names or aliases such as `('fips')`. An invalid value, a relationship
that does not hold, or a cap that an existing Model exceeds refuses the whole statement, naming the
parameter and the Model. The statement succeeds once every live node has installed the new record;
a node that is down installs it when it returns, before it reports ready.
[Committed Parameter Changes](node-settings.md#committed-parameter-changes) owns the commit and
installation contract.

Every duration parameter is greater than zero and at most 4,294,967,295 seconds, with nanosecond
precision. The tables give the other bounds.

### Membership And Replication

| Parameter | Default | Bounds | What the cluster promises |
| --- | --- | --- | --- |
| `NODE_UNAVAILABILITY_TIMEOUT` | `10s` | | How long a member may go unobserved before the cluster treats it as unavailable. Node-to-node connections ping every half of it, wait a quarter of it for the answer, and give an application health probe a tenth of it |
| `REPLICA_COUNT` | `0` | 0 to 4,294,967,295 | How many nodes besides its owner hold a replica of each piece of runtime state |
| `STATE_SNAPSHOT_INTERVAL` | `30s` | | How often the owner of runtime state publishes a snapshot of it to its replicas |

`NODE_UNAVAILABILITY_TIMEOUT` must exceed every admitted node's
`--control-plane-health-probe-interval`, 1 second by default, plus the probe timeout, a tenth of
the timeout. A node observes each peer again at most one interval plus one probe timeout after its
last observation, and an observation stays fresh only for the timeout. A shorter timeout leaves
every observation stale before the next one arrives, so the cluster stops detecting members that
fail. `ALTER CLUSTER SET` refuses such a timeout before anything commits, naming the first
admitted node, in name order, whose interval does not fit:

```text
node-unavailability-timeout (1s) must exceed node 'node-1' control-plane-health-probe-interval (1s) plus probe-timeout (100ms), a tenth of node-unavailability-timeout
```

### Domain Lifecycle And Coordination

| Parameter | Default | Bounds | What the cluster promises |
| --- | --- | --- | --- |
| `DOMAIN_DRAIN_TIMEOUT` | `30s` | Within every admitted node's drain budget | How long a domain drain waits for the domain's admitted work |
| `ENTITY_GATE_DEADLINE` | `30s` | Within every admitted node's drain budget | How long a coordinated entity gate may take to engage and drain |
| `DOMAIN_DRAIN_POLL_INTERVAL` | `50ms` | | How often a domain drain observes the work it waits for |
| `ENTITY_GATE_POLL_INTERVAL` | `25ms` | | How often an entity gate observes the drain it waits for |
| `OWNERSHIP_HANDOFF_POLL_INTERVAL` | `250ms` | | How often planned ownership handoffs are reconciled |
| `FORCED_RECOVERY_BUDGET` | `5s` | | How long a forced ownership recovery gives its destination to prepare |
| `RUNTIME_READINESS_PROPAGATION_BOUND` | `30s` | | How long a live node has to report an applied revision ready; a completion barrier waits at most the node-unavailability timeout plus this bound |

A node's drain budget is the smaller of its `--drain-timeout` and `--shutdown-timeout`. A change
that raises `DOMAIN_DRAIN_TIMEOUT` or `ENTITY_GATE_DEADLINE` above any admitted node's budget is
refused, naming the node, such as `domain-drain-timeout (45s) exceeds node 'node-2' drain/shutdown
budget (30s)`. A departed member keeps its claim until `DROP NODE` removes it, so it counts too.

### Remote Acknowledgements

| Parameter | Default | What the cluster promises |
| --- | --- | --- |
| `REMOTE_ACK_ALIVE_INTERVAL` | `100ms` | How often a receiver reports on a remote delivery it retains, and held local work reports the acknowledgements it holds alive |
| `REMOTE_ACK_DISPATCH_TIMEOUT` | `5s` | How long a remote admission attempt may make no progress |
| `REMOTE_ACK_SILENCE_TIMEOUT` | `15s` | How long a sender waits without a report after admission |
| `REMOTE_ACK_SWEEP_INTERVAL` | `1s` | How often a sender counts silence and admission time |
| `REMOTE_RELAY_TOTAL_TIMEOUT` | `5m` | The whole budget of one remote admission |
| `REMOTE_RELAY_INSTANTIATION_WAIT` | `5s` | How long a delivery waits for its route to become available |

The silence timeout exceeds twice the sum of the dispatch timeout and the alive interval, the sweep interval
divides both the silence timeout and the total timeout exactly, and the dispatch timeout and the
instantiation wait each fit the total timeout. Change related values in one statement. Each remote
delivery keeps the values it started with for its lifetime, and held local work reads the alive
interval when it starts waiting, so a change reaches it at its next keepalive without a restart.

### Transactions And Commands

| Parameter | Default | Bounds | What the cluster promises |
| --- | --- | --- | --- |
| `TRANSACTION_IDLE_TIMEOUT` | `15m` | | How long an open transaction may wait for its next statement before the cluster abandons it |
| `TRANSACTION_TOMBSTONE_RETENTION` | `15m` | | How long the cluster remembers how a finished transaction ended |
| `COMMAND_RETRY_VALIDITY` | `15m` | | How long the cluster recognizes a command's execution reference, so a retried command finds its first outcome |
| `COMMAND_EXECUTION_CAPACITY` | `65536` | 1 to 4,294,967,295 | The most command executions whose outcomes the cluster retains at once |
| `TRANSACTION_MAX_STATEMENTS` | `256` | 0 to 4,294,967,295 | The most statements one transaction may queue |
| `TRANSACTION_MAX_SOURCE_BYTES` | `1MiB` | Any byte count | The most statement source bytes one transaction may queue |
| `TRANSACTION_MAX_OPEN` | `1024` | 0 to 4,294,967,295 | The most transactions the cluster keeps open at once |

### Model And Session Limits

| Parameter | Default | Bounds | What the cluster accepts |
| --- | --- | --- | --- |
| `PAYLOAD_UNFOLD_LIMIT` | `65536` | 1 to 4,294,967,295 | The most records a codec unfolds from one ingested payload |
| `MAX_BATCH_MESSAGES` | `65536` | 1 to 4,294,967,295 | The most records one declared emitter batch may contain |
| `CLIENT_BATCH_ROWS` | `65536` | 1 to 4,294,967,295 | The most rows one client payload batch may contain |
| `CLIENT_PRODUCER_SESSION_BYTES` | `32MiB` | At least 1 byte | The outstanding producer payload bytes one session may reserve |
| `CLIENT_CONSUMER_SESSION_BYTES` | `32MiB` | At least 1 byte | The outstanding consumer payload bytes one session may reserve |
| `UDF_CODE_BYTES` | `64KiB` | At least 1 byte | The most source bytes one Roto function may declare |
| `UDF_COMPILE_BUDGET` | `10s` | | The longest that compiling one Roto function and running its `test` blocks may take on the node that compiles it |
| `HTTP_HEADER_BYTES` | `32KiB` | At least 1 byte | The most application header bytes one HTTP request may carry |
| `HTTP_HEADER_FIELDS` | `128` | 1 to 16,384 | The most distinct application header fields one HTTP request may carry |

A change is refused while a committed Model exceeds the new value: a Roto function above
`UDF_CODE_BYTES`, an emitter `BATCH MAX MESSAGES` above `MAX_BATCH_MESSAGES`, a client emitter's
`BATCH ... MAX SIZE` above `CLIENT_CONSUMER_SESSION_BYTES`, or declared HTTP headers above
`HTTP_HEADER_BYTES` or `HTTP_HEADER_FIELDS`. Client attachments keep the limits they were granted
until they end.

`UDF_COMPILE_BUDGET` is judged on the wall clock of the node that compiles a function, so its
verdict depends on that node's speed and load. `CREATE UDF` is refused when the node that validates
it takes longer, and each node holds its own compilation, when it activates or rebuilds the domain,
to the same committed value. A change rechecks no committed UDF: it judges the compilations that
start after it. [User-Defined Functions](udfs.md#compile-budget) describes what it bounds.

### Entity Labels

| Parameter | Default | What the cluster accepts |
| --- | --- | --- |
| `ENTITY_LABELS_MAX_COUNT` | `64` | Labels per entity |
| `ENTITY_LABELS_MAX_KEY_BYTES` | `128B` | UTF-8 bytes per label key |
| `ENTITY_LABELS_MAX_VALUE_BYTES` | `1KiB` | UTF-8 bytes per label value |
| `ENTITY_LABELS_MAX_BYTES` | `16KiB` | Combined key and value bytes per entity |

Each accepts any count or byte count, and a reduction is refused while a committed label map
exceeds it. [Entity Labels](entity-labels.md) owns their use.

### Resources

| Parameter | Default | Bounds | What the cluster accepts |
| --- | --- | --- | --- |
| `RESOURCE_MAX_ARCHIVE_BYTES` | `4GiB` | At least 1 byte | The most archive bytes one resource version may stage |
| `RESOURCE_MAX_EXTRACTED_BYTES` | `16GiB` | At least 1 byte | The most bytes one resource version may extract to |
| `RESOURCE_MAX_FILE_COUNT` | `1000000` | At least 1 | The most files one resource version may contain |

A reduction is refused while a stored resource version exceeds it. Every node's database volume
must also hold the two byte quotas: a node on a smaller volume refuses to start, and a raise above
the database volume any admitted node's committed claim carries is refused, naming that node. Lower
the quotas before a node joins on a smaller volume. To raise them above a volume, first grow it and
restart its node, so that the node's claim carries the new size.

### Security And Licensing

| Parameter | Default | Bounds | What the cluster promises |
| --- | --- | --- | --- |
| `TLS_CIPHER_SUITES` | The `fips` alias | At least one TLS 1.3 suite | The cipher suites of outward TLS connections, in order |
| `TLS_KEY_EXCHANGE_GROUPS` | The `fips` alias | A TLS 1.2 group whenever a TLS 1.2 suite is listed | The key-exchange groups of outward TLS connections, in order |
| `PASSWORD_ITERATIONS` | `210000` | 1 to 1,000,000 | The PBKDF2 iterations of newly created user credentials |
| `LICENSING_REQUEST_TIMEOUT` | `30s` | | One licensing request, including resolution, TLS and the response body |
| `LICENSING_RETRY_BACKOFF_MAX` | `10m` | | The ceiling of the exponential licensing retry |
| `LICENSING_STREAM_IDLE_TIMEOUT` | `90s` | | How long the licensing notification stream may stay idle |

An alias resolves to its names in place, so `DESCRIBE CLUSTER` lists the resolved algorithms. New
outward handshakes use a changed list, while established connections keep what they negotiated;
[Outward TLS Policy](cryptography.md#outward-tls-policy) owns the names and aliases. A
changed iteration count applies to credentials created afterwards, and existing users keep theirs.
[Cluster Licensing](node-settings.md#cluster-licensing) owns the licensing timings.

## Prerequisites

A prerequisite is what a node must bring to reach the live cluster. Nodes compare their
prerequisites as they connect, before consensus, so nothing that a node learns from the cluster can
change one. [The Prerequisite List Is Closed](node-settings.md#the-prerequisite-list-is-closed) owns
the list, and [Prerequisites At Join](node-settings.md#prerequisites-at-join) owns how nodes compare
them.

| Prerequisite | Comes from | Two nodes run together when the values are | How to change it |
| --- | --- | --- | --- |
| Cluster identity | `--cluster-id`, `default` unless given, which the node's certificate also names | Equal | Provision it with the cluster; a node with another identity is refused |
| Trust root | The trust anchors in `--interconnect-tls-ca` | Equal | Replace the anchors on every node; a node reloads its credentials without a restart, every `--interconnect-tls-reload-interval` (`1s`), and connections between nodes that hold different anchors are refused until both ends hold the same ones |
| Wire contract | The build | Equal | Run every node on a build that speaks the same contract |
| Largest command and largest management event | The build: 1 MiB and 64 KiB | Equal | Run every node on a build with the same floors |
| Durability promise | The build: `fsync every append` | Equal | Run every node on a build with the same promise |
| Raft heartbeat | `--raft-heartbeat-interval`, `250ms` | Within a factor of two | A rolling restart that moves it by less than a factor of two |
| Gossip interval | `--gossip-interval`, `500ms` | Within a factor of two | A rolling restart that moves it by less than a factor of two |
| Clock-skew tolerance | `--clock-skew-tolerance`, `500ms` | Within a factor of two | A rolling restart that moves it by less than a factor of two |

Two timings are within a factor of two when the larger is below twice the smaller: a `250ms`
heartbeat runs beside `400ms`, not beside `500ms`. A move of a factor of two or more takes more than
one roll, each finished on every node before the next begins, such as `250ms` to `450ms` and then to
`800ms`. The heartbeat also fixes the election timeouts, six to twelve heartbeats, and the gossip
interval fixes failure detection; neither has an option of its own.

The leader judges every joining node against every live voter, not only the member it reached first.
A node that cannot run beside one of them is refused as it joins, logs
`this node was refused at join and stops` at `error`, and exits with status `1`, while the cluster
keeps running. A joining node also reads its peers' clocks and stops when one is beyond its
`--clock-skew-tolerance`; once admitted, it only logs a skew at `warn`.

## Rolling Changes

A node capability or a prerequisite timing changes through a rolling restart: restart the nodes one
at a time with the new value. The cluster stays correct while two values coexist, whatever order
the nodes restart in, and the change takes full effect once the last node runs with it. Roll back
the same way in reverse. Only a prerequisite makes the cluster refuse a node: a restarted node whose
timing is outside the bound of a live voter's is refused alone, and the nodes already running keep
running. Mixed values never introduce a second data shape; every node reads and writes the one
current shape. [Rolling Changes](node-settings.md#rolling-changes) owns these guarantees.

- **A node capability**, such as `--memory-limit`, an execution pool or a listener bound, may
  differ between nodes for as long as you like. A restarted node announces a claim for its new
  run, and takes no placements until that claim commits. Peers send to it within its own receive
  bounds, and a peer whose batch it refuses slices the batch again and keeps its acknowledgement
  accounting. Sessions adopt the limits of the node they reach. Placement weighs new units by the
  committed claims; existing assignments stay where they are.
- **A prerequisite timing** moves by less than a factor of two in each roll, as above.
- **A cluster parameter** never changes in a roll. Change it with `ALTER CLUSTER SET` at any time,
  during a roll included; a node that is restarting installs the committed record before it
  reports ready.

A restarting or joining node also holds itself to the committed parameters before it reports ready,
and stops with status `1` when its database volume cannot hold `RESOURCE_MAX_ARCHIVE_BYTES` or
`RESOURCE_MAX_EXTRACTED_BYTES`, when its drain budget is shorter than `DOMAIN_DRAIN_TIMEOUT` or
`ENTITY_GATE_DEADLINE`, or when its `--control-plane-health-probe-interval` plus a tenth of
`NODE_UNAVAILABILITY_TIMEOUT` is not shorter than that timeout. Order a change that crosses the two
classes accordingly. To raise `DOMAIN_DRAIN_TIMEOUT`, first roll every node to a drain budget that
holds the new value, then commit it; to give a node a shorter drain budget, lower the parameter
first. In the same way, lower the resource quotas before a node restarts on a smaller volume, and
grow every volume and restart its node before raising them. Raise `NODE_UNAVAILABILITY_TIMEOUT`
before rolling a node to a longer health-probe interval, and shorten the intervals before lowering
the timeout.

## Reading DESCRIBE CLUSTER During A Roll

Any node answers `DESCRIBE CLUSTER;` without a selected domain or a redirect to the leader, a node
that is still starting included. Run it on the node you are restarting and on another one between
steps. An excerpt, from a three-node cluster whose node-3 has just restarted with a `400ms`
heartbeat, with the unchanged lines left out. An incarnation is the run's start time in nanoseconds
since the Unix epoch, so a restart gives a larger one:

```text
leader: node-1
timing drift: node-3
node: node-3
  incarnation: 1763747012882043911
  capabilities: awaiting this incarnation; retained node-3#1763746870315562047
  raft-heartbeat-interval: 400ms, the leader's 250ms
  record: revision 41 (incarnation 1763746870315562047)
cluster-effective receive bounds: relay-batch-bytes=33554432 relay-decoded-bytes=33554432 bulk-chunk-bytes=65536 bulk-window-bytes=1048576
replica-count committed=2 revision=4
node=node-1 replica-count running=2 revision=4
node=node-2 replica-count running=2 revision=4
node=node-3 cluster parameters running=not installed
```

| Line | What it tells you |
| --- | --- |
| `timing drift: node-3` | The nodes whose Raft heartbeat, gossip interval or clock-skew tolerance differs from the leader's; `none` when none does, or `unknown, no leader record is published` |
| `capabilities: committed node-3#1763747012882043911 cpu-count=8 memory-bytes=8589934592 database-volume-bytes=107374182400` | The claim committed for the node's current run: its CPU count, memory and database volume, followed on the same line by its receive bounds, engine, connector host and runtime capabilities, its shutdown timeouts, listener bounds and operational capabilities. The lines after it give its interconnect, membership, DNS, database and consensus capacities |
| `capabilities: awaiting this incarnation; retained node-3#1763746870315562047` | The node has restarted and its new claim has not committed yet; the retained claim is its previous run's |
| `capabilities: awaiting committed claim` | The node has no committed claim at all, as when it joins for the first time |
| `raft-heartbeat-interval: 400ms, the leader's 250ms` | One prerequisite line per prerequisite, a timing that differs from the leader's marked with both values; `prerequisites: not published` when the node publishes none |
| `interconnect liveness: ping-interval=5s ping-timeout=2s 500ms probe-timeout=1s` | The ping and probe budgets the node derives from the `NODE_UNAVAILABILITY_TIMEOUT` it has installed |
| `record: revision 41 (incarnation 1763746870315562047)` | The node's committed record, with its detected properties and tags after it; `record: not announced` before the first one |
| `node: node-3 (departed)` | A member that is no longer live. Its last claim and record stay until `DROP NODE` removes them |
| `cluster-effective receive bounds:` | The smallest of each receive bound among live nodes whose committed claim is for their current run; `awaiting committed live claims` until one is |
| `adopted peer receive bounds: node=node-2 incarnation=1763745911204836625` | The receive bounds the answering node's connections use for each peer. A peer's newly committed claim is adopted on the cadence of `--control-plane-runtime-state-retry-interval`, `500ms` by default |
| `replica-count committed=2 revision=4` | The committed value of each parameter and the revision that committed it |
| `node=node-3 replica-count running=2 revision=4` | The value each live node has installed and its revision; `node=node-3 cluster parameters running=not installed` while a node has installed none |
| `local execution capacity:` | The answering node's own workers and pending jobs per class, memory pools, client credit and operation limits, such as `execution workers: class=data_cpu workers=2 pending_jobs=128` |

Byte values print as byte counts: `memory-bytes=8589934592` is 8 GiB, and a byte parameter reads
like `udf-code-bytes committed=65536B revision=4`. The consensus-floor prerequisite lines print in
binary units instead, as `largest command: 1MiB` and `largest management event: 64KiB`. `SHOW CLUSTER STATUS` repeats the parameter
lines under `[cluster parameters]`, and reports the answering node's `node.initialization`,
`initializing` until its initialization completes and then `ready`. A node's `/readyz`, on its
observability listener, answers 200 only once its initialization has completed with the committed
record installed and it knows a leader.

Before you restart the next node, check that the restarted node's capability line reads `committed`
for its new incarnation, that every node's parameter lines show the committed revision, that its
`/readyz` answers 200, and that `timing drift` names only the nodes whose timings you are moving.
The `nervix_cluster_timing_drift_nodes` metric counts the nodes whose timings differ from the
leader's.
