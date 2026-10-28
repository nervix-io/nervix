# Node Settings

Every node setting has one class, a typed owner, one default, a derivation when applicable,
and a validation boundary. Values that depend on an external system belong to that connector's
declaration in Plexus, the Nervix graph definition language.

The vocabulary provides `SettingDefinition`, three sealed class markers and `Setting<D>`.
The source ratchet counts numeric policy outside that boundary. Every option `nervix-server`
accepts is either a declared setting in its owner's group or part of the node's identity,
addresses and paths, which are not settings; [The Server's Settings](#the-servers-settings) lists
them. Runtime constants that are not options keep their current wiring while their area owners
adopt these declarations, and this chapter states the contract that adoption must satisfy. The
class API alone publishes no record, commits no parameter, detects no capacity and changes no
running service. A node publishes, carries and verifies its prerequisites as
[Prerequisites At Join](#prerequisites-at-join) describes, and its
[startup preflight](#the-startup-preflight) detects its memory and measures its volumes. Cluster
parameters have one committed Raft record, seeded at the first bootstrap and changed through
`ALTER CLUSTER SET`. Capability records are not announced yet; those remain local claims.

Changes to the server's command line, startup settings and initialization contract keep the shipped
Kubernetes operator's generated startup command, chart and example current in the same delivery.
The server-image [deployment checks](installation-kubernetes.md#deployment-qualification) qualify
both native architectures before publication; the operator's own repository checks alone cannot
detect server changes.

## Choose The Class

Apply one test: does the value establish how a node reaches the cluster, what the cluster
accepts or promises, or what this node can carry?

| Class | Meaning | Authority and carrier | Verification | Change procedure |
| --- | --- | --- | --- | --- |
| Prerequisite | What a node must bring to reach the live cluster | Local provisioning or detection; gossip record and join handshake | Compare at join; refuse the incompatible node | Provision identity and trust; roll timing changes within supported relationship bounds |
| Cluster parameter | What the cluster accepts or promises | One committed Raft record; local flags seed first bootstrap only | Validate the transition at commit and Models against committed values | One administrative Plexus statement; apply live and rebuild construction-time owners in place where needed |
| Node capability | What one node can carry | Detected or overridden locally; announced through Raft as that node's capability record | Validate the local claim and its relationships; peers honour advertised bounds | Restart with new overrides or detected capacity; peers and placement adopt the committed record |

Two values that give the cluster two answers about acceptance or guarantees identify a cluster
parameter. A Model-validated cap is never an independent node flag. Capabilities change local
admission, placement or the work a peer can send; they cannot relax cluster guarantees. A node
without a committed capability record receives no placements. If an advertised bound changes,
a peer can refuse an operation with a typed answer so the sender slices it again while retaining
the row acknowledgement accounting.

## The Prerequisite List Is Closed

The prerequisites are cluster identity; the trust root; the node-to-node wire contract; the
command and management-event size floors needed to join consensus; the durability promise of the
Raft log; the Raft heartbeat, from which the election timeouts derive; the gossip interval, from
which failure detection derives; and the physical clock skew a node tolerates. One record holds
them all, and each has one source:

| Prerequisite | Source | Default | Two nodes run together when the values are |
| --- | --- | --- | --- |
| Cluster identity | `--cluster-id`, which the node's certificate must also name | `default` | equal |
| Trust root | Detected: a SHA-256 fingerprint over the SHA-256 digests of the trust anchors in `--interconnect-tls-ca`, sorted and without repeats, so the order of the file does not change it | none | equal |
| Wire contract | Fixed by the build: the fingerprint of the one node-to-node contract it speaks | none | equal |
| Largest command, largest management event | Detected: the executor's operation limits | 1 MiB, 64 KiB | equal |
| Durability promise | Fixed by the build: every Raft log append is synchronized, data and metadata, before the node acknowledges it, written `fsync every append` | none | equal |
| Raft heartbeat | `--raft-heartbeat-interval` | `250ms` | within a factor of two |
| Gossip interval | `--gossip-interval` | `500ms` | within a factor of two |
| Clock-skew tolerance | `--clock-skew-tolerance` | `500ms` | within a factor of two |

Two timings are within a factor of two when they are equal or the larger is below twice the
smaller: a node with a `250ms` heartbeat runs beside one with `400ms`, not beside one with
`500ms`. One rolling change may therefore move a timing by less than a factor of two, and a larger
move takes more than one roll. The factor is a fixed contract rather than a setting, because the
Raft timings rely on it. A node does not configure its election timeouts: a follower stands for
election after a randomized timeout between six and twelve of its own heartbeat intervals, 1.5 to 3
seconds by default. A compatible leader's heartbeat is shorter than twice the follower's, so the
slowest heartbeat a compatible leader sends stays within a third of the follower's minimum
election timeout. Gossip derives its failure detector the same way: it assumes ten gossip
intervals between a peer's heartbeats until it has observed some, and takes no gap longer than
twenty gossip intervals as a sample, 5 and 10 seconds by default.

A proposed new prerequisite first has to establish why a committed cluster parameter applied by
rebuilding its owner cannot express it.

## The Startup Preflight

Before it binds a listener or opens a store, a node checks the environment it is about to run in.
It refuses to start at the first check that fails, exiting with status `1` and
`the startup preflight refused this node` above the cause:

1. It builds its executor from the execution limits, checked against one another, so budgets that
   cannot hold the operations the limits admit refuse the start.
2. It resolves its memory limit: `--memory-limit` when given, otherwise the smallest memory limit
   of its control group and the groups above it, in the unified hierarchy or the version 1 memory
   hierarchy, or its physical memory when no group limit is smaller. A platform that reports
   neither leaves the limit unknown, and the memory checks do not run. A known limit must hold all
   of the executor's memory budgets together, 2,723 MiB by default, and the
   `--memory-high-watermark` when one is set.
   This includes independent pools for decoded materialized snapshots and optional source
   retention. Each derives its capacity from the relay budget, 192 MiB by default, with no
   separate setting.
3. It creates its database directory and its temporary directory when they are absent, proves each
   writable with a probe file it removes again, and measures the total size of the volume each
   lives on.
4. It resolves its restore-staging quota against the total size of the volume that holds the
   database directory: `--restore-staging-max-bytes` when given, otherwise the smaller of 128 GiB
   and that size. It then requires the quota, `--resource-max-archive-bytes` and
   `--resource-max-extracted-bytes` each to fit that size. Each quota is compared with the size on
   its own, and nothing the volume already holds is subtracted.
5. It loads its name resolver, as [Name Resolution](name-resolution.md#resolver-lifetime)
   describes.

Each refusal names the setting or directory it judged, with the values involved:

| Check | Refusal |
| --- | --- |
| Memory budgets | `memory-limit (1GiB) cannot hold the node's memory budgets of 2.66GiB`, or `the detected memory limit of 2GiB cannot hold the node's memory budgets of 2.66GiB` |
| Memory watermark | `memory-high-watermark (4GiB) exceeds memory-limit (3GiB)` |
| Directories | `the database directory <path> cannot be created`, `the temporary directory <path> is not writable` |
| Volumes | `the capacity of the volume holding the database directory <path> cannot be read` |
| Quotas | `restore-staging-max-bytes (128GiB) exceeds the 64GiB capacity of the volume holding the database directory <path>`, for a quota the operator gave, or `resource-max-extracted-bytes (16GiB) exceeds the 5GiB capacity of the volume holding the database directory <path>` |
| Resolver | `failed to load the name resolver configuration`, above the cause that resolver loading reports |

Only the restore-staging quota, a node capability, follows the volume. The resource quotas are
cluster parameters with fixed defaults, 4 GiB and 16 GiB. A node whose database volume is smaller
refuses to start. Provision a larger volume, seed smaller quotas on a new cluster, or change an
existing cluster's committed quotas through `ALTER CLUSTER SET` before joining it. A node that passes logs
`the startup preflight passed` at `info` with its memory limit, its memory budgets, the size of both
volumes and the restore-staging quota it resolved.
[Capacity Planning](capacity-planning.md#what-a-node-needs-to-start) sizes them.

Clock skew is the one environmental check that needs a peer, so a node makes it as it joins, as
[Prerequisites At Join](#prerequisites-at-join) describes. Environmental checks add no setting
class. [Name Resolution](name-resolution.md), [Domain Clock](domain-clock.md) and
[Consensus Storage And Replication](consensus-storage-and-replication.md) own their detailed
failure and timing contracts.

## Reachability Determines Layering

Nothing a node needs to reach the live cluster can depend on a value it learns only after
reaching it. Gossip establishes discovery from local identity, trust, bootstrap hosts and its
prerequisite cadence. The handshake verifies prerequisites. Consensus carries cluster
parameters and node capability records. The registry validates Models against committed
parameters; engines and the data plane receive resolved construction inputs or plans.

The join handshake carries prerequisites and needs no consensus read to establish consensus.
A returning node obtains construction-time cluster parameters from the live cluster's
published state and verifies them against the log as it catches up. Its stale local log is
not the initial policy authority. [Cluster Interconnect](interconnect.md) owns publication,
authentication, wire forms and discovery.

## Prerequisites At Join

A node publishes its record in its gossip state under the key `prerequisites`, and carries it in
the hello of every interconnect connection it dials. The node that accepts a connection judges the
hello's record against its own before it binds the connection, and answers with its own record or
with a refusal that names the first prerequisite they disagree on, in the order of the table above,
and both values. The dialing node judges an accepted answer's record the same way. Neither side
reads consensus, so the check holds for a node that has never reached the cluster. A certificate
that names another cluster is refused as a cluster-identity mismatch before any hello is read,
unless it answers at an endpoint recorded for a member it does not name, as described below.

A refusal affects only the node that is joining. A node that starts with peers to reach, a
bootstrap host or the other members its stored membership names, joins through the first
connection it dials that completes; when a connection it dials is refused before any has
completed, it stops instead. A node leaves that joining state once, and a connection another node
dials never moves it, so an incompatible newcomer never stops a node that has joined. A node that
bootstraps a cluster alone joins nothing.

An endpoint the stored membership recorded for a member can outlive the member: its address may
now serve a node of another cluster that trusts the same authority. A certificate there that names
another cluster and a node other than the recorded member is a stale route, not a refusal: the dial
fails as `TransportError::StaleRoute`, naming the endpoint and the member, refuses no one and leaves
the node joining, and the node joins through another recorded member or its bootstrap host. A
certificate that names the recorded member in another cluster is a cluster-identity refusal, and so
is a certificate of another cluster at a bootstrap host.

The member a joining node happens to reach first does not decide whether the cluster can run
beside it: timing compatibility is not transitive, so a node can run beside the member it joined
through and not beside another voter. The leader decides, on every membership reconciliation. It
judges the runs of the live voters that published a record it reads in the order the runs started:
a run is accepted when it can run beside every run accepted before it, and refused naming the first
of those, in name order, that it cannot. The run that started last is the one that changed,
so a member that returns with a record an older voter cannot run beside is refused, and the voters
that kept running are not. Refusing a voter's run changes no membership. The leader judges a live
node outside the membership against the accepted voters' records: it admits one that can run beside
every one of them, refuses one that cannot, naming the first such voter, and leaves one that has
published no record it can read outside the membership.

The leader publishes its verdicts in its gossip state under the key `admission_verdicts`, with the
Raft term it leads. A node that finds another node leading withdraws the verdicts it published, and
a reader that sees the verdicts of two leaders follows the newer term. A joining node reads the
verdict on its own run, its name and incarnation, and stops on a refusal, which names the voter and
the prerequisite with both values seen from the joining node. A member its stored membership names
returns without changing the voters or the quorum, so its join ends once the leader has accepted
its run and a peer's clock has answered within tolerance. Any other node's join ends once the
membership it has applied admits it, because from then on the cluster may count it among its
voters. Until then it also stops as soon as a live member of that membership publishes a record it
cannot run beside. A live node outside that membership, such as another joining node or one that
was itself refused and has not left gossip yet, never stops it.

Once its first connection completes, a joining node asks the peer it joined through for its
physical clock, then every other live peer it has completed a handshake with. It asks each peer
once, and asks a peer that did not answer again a gossip interval later. The peer read its clock
between the probe's departure and its answer's arrival, so one probe proves skew only when the
reading falls outside that window, and the skew it proves is the distance to the nearer end. A peer
whose clock reads before the Unix epoch answers so, and is behind by more than all the time since
it. A node whose proven skew to one of those peers exceeds its `--clock-skew-tolerance` stops, but
only while it is still joining: a skew that an answer proves once the membership has admitted a
newcomer is logged at `warn`, and the node stays.

A refused node logs `this node was refused at join and stops` at `error`, requests its own stop,
runs the ordinary shutdown phases, and exits with status `1`. Its report, beneath
`this node was refused at join`, names the peer, the prerequisite and both values, or the clock it
judged:

```text
node 'node-1' cannot run beside this node: raft-heartbeat-interval is 1s on this node and 250ms on the peer, not within a factor of 2
the physical clock of node 'node-1' is beyond this node's clock-skew-tolerance of 500ms: the peer is at least 1.2s ahead
```

The trust root follows the credentials a node holds. A credential reload that changes the trust
anchors republishes the record, and the node's later handshakes carry the new fingerprint. Two
nodes that hold different anchors cannot open a connection between them: during a rotation that
has reached some nodes and not others, the connections a reload replaces are refused until both
ends hold the same anchors. A node that has joined does not stop for it.

`DESCRIBE CLUSTER` lists every live node's published record and marks each timing that differs
from the leader's, and `nervix_cluster_timing_drift_nodes` counts the nodes whose timings differ.
Both sides of a refused connection log the refusal at `warn`. The leader logs a run it refuses once
while the refusal holds, at `warn` with `refused a node's run that cannot run beside a voter`, and
a node it leaves outside the membership at `debug` while that node has published no record the
leader can read. A returning member whose run the leader accepted logs
`the leader accepted this node's return beside every live voter` at `info`. An outbound pool
connection that ends in a refusal is counted in `nervix_interconnect_connection_failures_total`
with reason `prerequisite`. [Cluster Interconnect](interconnect.md#the-join-contract) owns the
handshake and its wire forms.

## Typed Definitions And Defaults

An area group's fields use `Setting<D>`. Its `SettingDefinition` must choose exactly one sealed
`Class`: `Prerequisites`, `ClusterParameters` or `NodeCapabilities`. `Value` carries the semantic
type, `NAME` identifies the setting, `DERIVATION` describes a dependent default, and `Context`
supplies typed derivation and relationship inputs. The definition owns `default_value(context)`
and `validate(value, context)`.

`Setting::new` validates an explicit value. `Setting::resolve` chooses an explicit value or
the owner's default, then runs the same validation. Invalid defaults return the owner's report
too. Value access is shared or consumes the setting; replacing a value crosses validation again.
The compiling API example and paired `compile_fail` doctest prove a definition without its
class is incomplete. The class markers classify fields; they are not global configuration bags.

Write the default once in the definition. An edge's `default_value_t` obtains that value from
the definition. A machine-dependent or setting-dependent argument remains `Option` until its
owner resolves it with context. Absence means derivation; zero, empty strings and numeric
extrema never encode an omitted argument. `clap` stays at the edge, which converts arguments
once to validated settings before I/O. The vocabulary does not parse command-line arguments.

Dependent values are named methods of their owners, never flags. Validate relationships on
the operator's actual values, including budget totals and deadline ordering; retain assertions
on defaults too. Typed reports retain the definition owner's cause without flattening it.

## The Server's Settings

`nervix-server` reads its command line as one options group per area, each in its own file at the
server's command-line edge and printed under its own heading by `--help`. One group holds the
node's identity, listen and advertised addresses, TLS files, name resolution files and storage
paths. Those are what a node is provisioned with, not settings. Every other group is the
command-line form of one owner's class-typed group: each option is named by its definition's
`NAME`, has the environment variable `NERVIX_` followed by that name with underscores, and shows
the default its definition returns. Cluster-parameter help identifies it as a bootstrap default. Durations and byte quantities print in the edge's forms, such as `250ms`,
`15m` and `64MiB`, which read back to exactly the same value.

The edge converts each group once, and the application carries the result: a `NodeIdentity` and a
`NodeSettings` holding every group. The shutdown coordinator is derived from the shutdown timeout
the settings hold, so the two cannot disagree, and the runtime is built from its `RuntimeSettings`
and a named environment of the services it uses rather than from positional values. The
[nervix-server Reference](nervix-server-reference.md) is the help output of the release this book
describes, defaults included.

| Option | Class | Owner group |
| --- | --- | --- |
| `--raft-heartbeat-interval` | prerequisite | Raft timings |
| `--gossip-interval`, `--clock-skew-tolerance` | prerequisite | membership |
| `--raft-snapshot-entry-threshold`, `--raft-snapshot-byte-threshold`, `--raft-covered-log-entries-retained`, `--raft-covered-log-bytes-retained`, `--raft-retained-log-cap` | node capability | Raft retention |
| `--node-unavailability-timeout` | cluster parameter | membership |
| `--replica-count` | cluster parameter | scheduling |
| `--state-snapshot-interval` | cluster parameter | runtime |
| `--domain-drain-timeout`, `--entity-gate-deadline` | cluster parameter | cluster policy |
| `--max-batch-messages`, `--client-producer-session-bytes`, `--client-consumer-session-bytes`, `--udf-code-bytes`, `--http-header-bytes`, `--http-header-fields`, `--client-batch-rows` | cluster parameter | model and session policy |
| `--restore-staging-max-bytes` | node capability | runtime |
| `--transaction-idle-timeout`, `--transaction-tombstone-retention`, `--transaction-max-statements`, `--transaction-max-source-bytes`, `--transaction-max-open` | cluster parameter | transaction limits |
| `--command-retry-validity`, `--command-execution-capacity` | cluster parameter | command execution |
| `--memory-high-watermark`, `--memory-low-watermark`, `--memory-pressure-check-interval`, `--memory-pressure-resume-jitter` | node capability | memory pressure |
| `--drain-timeout`, `--shutdown-timeout` | node capability | shutdown |
| `--resource-max-archive-bytes`, `--resource-max-extracted-bytes`, `--resource-max-file-count` | cluster parameter | resources |
| `--otel-trace-sample-ratio` | node capability | trace export |
| `--memory-limit` | node capability | capacity |

Cluster-parameter options are optional seeds for the first bootstrap. An explicit command-line
or environment value, including a value equal to the default, is refused on every later start.
The help text names the definition's default without supplying an implicit seed option. The
memory watermarks have no default: without
them the node runs no memory-pressure supervisor. `--memory-limit` and
`--restore-staging-max-bytes` have no default value either: without them the node detects its
memory limit and derives its restore-staging quota from its database volume, as
[The Startup Preflight](#the-startup-preflight) describes.

### Validation Before Any I/O

The edge validates every value, and every relationship between values, before the node opens a
store, binds a listener or starts a task:

- The Raft heartbeat interval is at least one millisecond, the resolution consensus runs timings
  at, and twelve of it, the longest election timeout, fit the milliseconds consensus counts it in.
- The gossip interval and the clock-skew tolerance are each at least one millisecond, and twenty
  gossip intervals fit a duration.
- A memory limit, when one is given, is above zero.
- The drain timeout does not exceed the shutdown timeout, which bounds every drain step. A drain
  timeout equal to it lets the drain use the whole deadline; the defaults leave time for terminal
  teardown after the drain.
- The memory watermarks are set together or not at all, the low one below the high one, and with
  them the check interval is above zero.
- The trace sample ratio lies between 0 and 1.

A value its option cannot read, such as malformed duration text or a sample ratio outside its
range, ends the process with status `2` as the command line reports it. A relationship that does
not hold, or a value outside its own bounds, ends the process with status `1` before it opens a
store, with a message that names the settings involved and their values, such as
`drain-timeout (60s) must not exceed shutdown-timeout (50s)` or
`raft-heartbeat-interval (500µs) must be at least 1ms, the resolution consensus runs timings at`.
The defaults satisfy every relationship, and the assertions that keep them doing so remain beside
them.

Rendered duration options preserve every nanosecond. Compact values use the largest exact unit,
such as `250ms` or `15m`; a large fractional value uses whole seconds and remaining nanoseconds,
such as `169112806400s 601538000ns`. These values read back unchanged when settings are reapplied
or another option is overridden.

## Rolling Changes

A cluster remains correct while two setting values coexist during a rolling restart in any
order. The change takes full effect after the last node adopts it; rollback follows the same
procedure in reverse. A prerequisite can reject a node outside its supported bounds: a timing
moves by less than a factor of two in one roll, and a restarted node whose timing is outside that
bound of a live peer's is refused as it joins, alone. Identity, trust, the wire contract, the
floors and the durability promise cannot be rolled, because two values of one never run together.
A capability is a local claim; a cluster parameter retains one committed authority throughout.

The alpha rule still requires one current data or message shape. Operational values coexist
within that shape. A roll introduces no earlier schema, dual decoder or migration path.

### Committed Parameter Changes

Cluster parameters have one durable authority. The first node that creates the cluster seeds its
parameter record from the bootstrap options. After that boundary, options for those parameters
are refused at startup, including on a returning bootstrap node. Change the record through
Plexus, the Nervix graph definition language:

```plexus
ALTER CLUSTER SET NODE_UNAVAILABILITY_TIMEOUT = 15s, STATE_SNAPSHOT_INTERVAL = 250ms;
```

The statement runs outside domain transactions and needs no selected domain. Its assignments
form one transition: either every assignment is committed or none is. A duplicate parameter,
an invalid value, or a cap that an existing model exceeds refuses the transition with the
parameter and owning model identified. Timings that drive repeated work or retained validity
must be greater than zero and no longer than `u32::MAX` whole seconds, with nanosecond precision
within that range. Counts used by a wire or native count fit its current representation. HTTP application headers
default to 128 distinct fields; `HTTP_HEADER_FIELDS` changes that count together with the other
committed caps. A prerequisite cannot be assigned through this statement.

Validation and publication share a commit fence. A concurrent model change or parameter change
invalidates a captured transition before it applies, and validation runs again against the
current state. Model admission likewise uses the committed parameter record, so a configuration
cannot be accepted against a superseded cap.
Known HTTP header names contribute their count and byte requirement even when their values depend
on input. Dynamic names may replace earlier values, so transition validation retains only guaranteed
byte contributions; request preparation checks each complete evaluated envelope against its captured
bounds.

Each live node installs the committed record before reporting that revision ready. Runtime timing
and resource-store owners publish a new immutable policy over the same storage. Snapshot tasks
observe interval changes at their existing bounded replication-poll boundary; a changed interval
starts a fresh period, while an unrelated parameter does not postpone a checkpoint. Client
attachments retain their granted byte and row limits until they end; new attachments and HTTP
publication preparation capture the current limits. The change does not discard payloads, acknowledgement
ownership, or stored state. A successful response means the live participants have installed the
change. An uncertain response retains its execution reference and resumes the same recorded
transition, even when a later command has changed another parameter.

`DESCRIBE CLUSTER` reports committed values and each node's running revision and values. A node
that has not applied the current record remains visibly behind. Sessions receive an informational
notice when their node installs a parameter revision. A joining or returning node obtains the
live cluster's published record before activating services that consume it and verifies that
record against consensus as it catches up.
Public session and observability listeners start before live-policy discovery. `SHOW CLUSTER
STATUS` and `DESCRIBE CLUSTER` remain available while an isolated returning node waits; status
reports `node.initialization=initializing` until initialization completes, then `ready`. Liveness
and metrics remain available, while `/readyz` returns 503 until initialization finishes and a
leader is known. Policy-dependent commands, session attachments, resource transfers and configured
HTTP intake refuse work until the live publication is known, the local committed record has caught
up, and initialization has registered its owners. No stale local record supplies startup policy. A durable command retains its execution reference and receives a nonterminal `OutcomeUnknown(NotYetAuthoritative)` during this wait, so reconnecting cannot turn an earlier committed result into a failure.
Discovery waits for a live publication or the first stop request, including a typed join refusal;
shutdown cancels the wait and retains that refusal through cleanup. During a whole-cluster restart,
peers start concurrently: each returning node needs another process's live publication before it
can finish startup.

Only the first bootstrapping node supplies parameter seed flags; another node given them
refuses startup. Changes validate the transition at commit: existing Models must fit a lowered
cap, and prerequisite relationships must still hold. Apply committed changes live. Rebuild an
owner that captures settings at construction over its existing storage. The current record changes
no OpenRaft construction setting: heartbeat and election timing remain prerequisites. Committed
and running revisions are published together, and delayed observations cannot move them backward.

## Fixed Values And The Source Ratchet

A wire format, RFC, persisted layout, ABI or invariant can fix a value. Its exact constant
declares the reason, gated by the analysis configuration:

```rust,ignore
#[cfg_attr(nervix_lint, nervix::constant(fixed, reason = "the frame ABI stores its tag in four bytes"))]
const TAG_BYTES: usize = 4;
```

The compiler accepts this contract on a constant, including an associated constant, with the
`fixed` kind and one nonempty reason. Duplicate, malformed and misplaced contracts fail. The
source scanner validates the gate before excluding a constant, including inactive code. The
reason remains reviewed evidence; neither parser nor compiler proves its prose true.

`just ratchet --show hidden_tunables` lists the temporary count's sites. It counts numeric
`const` items of types `Duration`, `usize`, `u64`, `u32`, `u16`, `u8`, `i64`, `i32`, `f64`,
`NonZero*` and `ByteUnit`, plus numeric literal policy calls for duration construction, channel
and queue capacities, `with_capacity` and semaphore permits. A numeric constant counts once,
including its initializer. Class group defaults, definition defaults and fixed constants are
excluded. Tests, benchmarks, examples, tooling and the arbitrary, benchmark, model-harness and
test-environment crates are outside this count.

The baseline only decreases. The syntax count sees inactive source and authored macro
fragments; it does not resolve custom types, infer business meaning, expand external procedural
macros or discover implicit library defaults.
[Architecture Rules And The Debt Ratchet](architecture-enforcement.md#hidden-tunables) owns
its exact recognition and qualification limits. The settings compiler rule replaces this
detector and its numeric entry together at cutover.

## Names And Observability

Flags use `--<area>-<name>` and environment variables use `NERVIX_<AREA>_<NAME>`. Durations and
sizes use the existing human-readable forms. The [nervix-server Reference](nervix-server-reference.md)
is the help output, whose names and defaults come from the declarations.

Startup reports resolved settings with credentials and sensitive material redacted: the
`starting nervix server` line at `info` names the Raft heartbeat and the election timeouts derived
from it, the gossip interval, the clock-skew tolerance and the memory limit with where it came
from. Gossip exposes per-node prerequisites and the running committed revision. `DESCRIBE CLUSTER`
shows each live node's prerequisites and the timings that differ from the leader's, and
`nervix_cluster_timing_drift_nodes` counts those nodes. Committed parameters and capability
records, once a node announces them, are reported beside the running values they govern, with the
effective negotiated bounds. Refusals identify the setting and failed relationship without
disclosing secrets. Connector-specific values remain in their owning manual and
[Connector Contract](connector-contract.md).

## Verification Boundary

The executor's connector-storage capacity is `Setting<ConnectorStorageWorkers>` with class
`NodeCapabilities` inside `WorkerCounts`. Its one default is two workers. The semantic value is
nonzero, and both an explicit local claim and the default are checked against the primitive's
maximum permit representation before pool construction. Startup resolves it through the local
executor configuration, and the executor snapshot and worker metrics expose the running value.
The existing execution-settings adoption owns detection and capability-record publication.
Filesystem and consensus retain independent admission while a connector call holds its worker.

The interconnect's bulk receive window is `Setting<BulkReceiveWindow>` with class
`NodeCapabilities` inside `TransportOptions`. Its one default is 1 MiB, the HTTP/2 window each
stream of a bulk connection grants its sender, and like the transport's other windows no
command-line option sets it. The value must be
nonzero, and the connection window derived from it, one window for each of the four bulk streams,
must fit the 2^31-1 bytes HTTP/2 can advertise; the definition checks both. A transport refuses to
bind when the executor's `bulk_window` memory budget cannot back one window. The fixed constant
for the HTTP/2 maximum names RFC 9113.

A Kafka consumer's single control-operation turn is an ordering invariant rather than an
operator capacity. Its fixed constant states that a cancelled native mutation cannot be
overtaken by the next operation. The source control-call budget is the external client's
`socket.timeout.ms`; [Connector Contract](connector-contract.md) owns that per-system value.

`Setting<D>` copies or compares a resolved value when its semantic value supports those
operations, without imposing value traits on the definition marker. Generic debug formatting
identifies the declaration; the owning settings renderer controls value exposure and redaction.

The declaration API has value, default and relationship tests plus paired Rustdoc examples.
The source count has tracked and new-source gate fixtures, pinned exclusions, inactive and
macro syntax, compiler contract fixtures and the registered bounded `typed-hidden-tunables`
Bolero property. The declarations alone add no Cucumber scenario and change no lock or lifecycle
path. Public behavior introduced by an area owner requires its one-node and three-node scenarios
and the concurrency evidence its path needs.

The server's options are covered at their edge and at each owner. Unit tests render durations and
byte quantities, check that every settings option is named by its definition and read from its
environment twin, that help lists each area under its own heading, that an empty command line runs
every definition's default, and that each relationship refuses the node with its owner's typed
error. The registered `node-settings-command-line` Bolero property generates valid settings for
every group, relationships held, and requires them to read back unchanged after rendering as
options, along with every duration and byte quantity. Public scenarios start a real
`nervix-server` process with a drain timeout above its shutdown timeout or a Raft heartbeat below
one millisecond and observe the refusal naming the settings, and restart a node of a one-node and a
three-node cluster with such a setting, through the harness step that names a setting for one
node, before restarting it with a valid one. Converting and validating the options takes no lock
and adds no lifecycle path, so the deadlock detector has nothing of it to observe.

The prerequisite record, its compatibility rule, its gossip text and the order `DESCRIBE CLUSTER`
lists it in have unit tests beside the record, and the record's gossip text has a registered Bolero
property that requires it to read back unchanged. A second, `cluster-peer-clock-probes`, requires
the clock probe and every answer to it to read back unchanged through the archive. Public scenarios
on one-node and three-node clusters refuse a joining node whose cluster identity or trust root
differs, or whose Raft heartbeat is outside the bound, and require the refusal to name the
prerequisite and both values; refuse a node that joins through a member it can run beside but not
beside another voter, naming that voter; admit a node whose heartbeat differs within the bound and
show its drift through `DESCRIBE CLUSTER`; and refuse to start a node whose memory limit cannot hold
its budgets. A three-node scenario refuses a returning member that can run beside the voters it
recorded but not beside one admitted while it was away, naming that voter: admitting a voter while
a member is away needs a quorum without it, which fewer voters do not give. Unit tests cover the
leader's judgment of voter runs and candidates, a joining node's handling of its verdict, and the
verdicts' gossip text, whose registered `models-admission-verdicts` Bolero property requires it to
read back unchanged. Interconnect unit tests and the `stale recorded endpoint` case of the
[deterministic interconnect simulation](interconnect-simulation.md) cover the stale route.

Resource-volume checks use the chosen seed before the first commit, and the live publication for
a joining or returning node. Local default resource limits do not refuse a node before it can
learn that policy. The local preflight still measures and validates the restore-staging capability
before opening the database or binding listeners.
