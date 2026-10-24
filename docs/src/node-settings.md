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
class API alone does not publish records, commit parameters, detect machine capacity or change a
running service.

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

The prerequisites are cluster identity; trust roots and node identity credentials; the
node-to-node wire contract; command and management-event size floors needed to join consensus;
the durability promise including fsync mode; Raft heartbeat and election timing relationships;
gossip cadence and its failure-detection relationships; and permitted physical clock skew.

Identity, trust, wire contract, consensus floors and durability require exact agreement. Raft
timing and gossip cadence require bounded coexistence: election timeouts derive from heartbeat
by fixed factors, and every pairing during a roll must satisfy those bounds. Per-node values
remain visible. A proposed new prerequisite first has to establish why a committed cluster
parameter applied by rebuilding its owner cannot express it.

Startup preflight checks environmental facts: peer clock skew, available storage and quota
relationships, memory sufficient for the resolved budgets, and a loadable resolver. Failures
preserve typed causes before starting or joining. Environmental checks do not add a setting
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
`NAME`, has the environment variable `NERVIX_` followed by that name, and prints the default its
definition returns. Durations and byte quantities print in the edge's forms, such as `250ms`,
`15m` and `64MiB`, which read back to exactly the same value.

The edge converts each group once, and the application carries the result: a `NodeIdentity` and a
`NodeSettings` holding every group. The shutdown coordinator is derived from the shutdown timeout
the settings hold, so the two cannot disagree, and the runtime is built from its `RuntimeSettings`
and a named environment of the services it uses rather than from positional values. The
[nervix-server Reference](nervix-server-reference.md) is the help output of the release this book
describes, defaults included.

| Option | Class | Owner group |
| --- | --- | --- |
| `--raft-heartbeat-interval`, `--raft-election-timeout-min`, `--raft-election-timeout-max` | prerequisite | Raft timings |
| `--raft-snapshot-entry-threshold`, `--raft-snapshot-byte-threshold`, `--raft-covered-log-entries-retained`, `--raft-covered-log-bytes-retained`, `--raft-retained-log-cap` | node capability | Raft retention |
| `--node-unavailability-timeout` | cluster parameter | membership |
| `--replica-count` | cluster parameter | scheduling |
| `--state-snapshot-interval` | cluster parameter | runtime |
| `--restore-staging-max-bytes` | node capability | runtime |
| `--transaction-idle-timeout`, `--transaction-tombstone-retention`, `--transaction-max-statements`, `--transaction-max-source-bytes`, `--transaction-max-open` | cluster parameter | transaction limits |
| `--command-retry-validity`, `--command-execution-capacity` | cluster parameter | command execution |
| `--memory-high-watermark`, `--memory-low-watermark`, `--memory-pressure-check-interval`, `--memory-pressure-resume-jitter` | node capability | memory pressure |
| `--drain-timeout`, `--shutdown-timeout` | node capability | shutdown |
| `--resource-max-archive-bytes`, `--resource-max-extracted-bytes`, `--resource-max-file-count` | cluster parameter | resources |
| `--otel-trace-sample-ratio` | node capability | trace export |

Each node still reads its cluster parameters from its own options; until they are committed once
for the cluster, give every node the same value. The memory watermarks have no default: without
them the node runs no memory-pressure supervisor.

### Validation Before Any I/O

The edge validates every value, and every relationship between values, before the node opens a
store, binds a listener or starts a task:

- The Raft heartbeat interval is shorter than the minimum election timeout, which is shorter than
  the maximum, compared in the whole milliseconds consensus runs with.
- The drain timeout does not exceed the shutdown timeout, which bounds every drain step. A drain
  timeout equal to it lets the drain use the whole deadline; the defaults leave time for terminal
  teardown after the drain.
- The memory watermarks are set together or not at all, the low one below the high one, and with
  them the check interval is above zero.
- The trace sample ratio lies between 0 and 1.

A value its option cannot read, such as malformed duration text or a sample ratio outside its
range, ends the process with status `2` as the command line reports it. A relationship that does
not hold ends the process with status `1` before it opens a store, with a message that names the
settings involved and their values, such as
`drain-timeout (60s) must not exceed shutdown-timeout (50s)`. The defaults satisfy every
relationship, and the assertions that keep them doing so remain beside them.

Rendered duration options preserve every nanosecond. Compact values use the largest exact unit,
such as `250ms` or `15m`; a large fractional value uses whole seconds and remaining nanoseconds,
such as `169112806400s 601538000ns`. These values read back unchanged when settings are reapplied
or another option is overridden.

## Rolling Changes

A cluster remains correct while two setting values coexist during a rolling restart in any
order. The change takes full effect after the last node adopts it; rollback follows the same
procedure in reverse. A prerequisite can reject a node outside its supported bounds. A
capability is a local claim; a cluster parameter retains one committed authority throughout.

The alpha rule still requires one current data or message shape. Operational values coexist
within that shape. A roll introduces no earlier schema, dual decoder or migration path.

Only the first bootstrapping node supplies parameter seed flags; another node given them
refuses startup. Changes validate the transition at commit: existing Models must fit a lowered
cap, and prerequisite relationships must still hold. Apply committed changes live. Rebuild an
owner that captures settings at construction over its existing storage; rebuilding the leader's
Raft owner costs a leadership change. Where a library cannot be rebuilt, expose the specific
restart requirement and committed-versus-running drift.

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

Startup reports resolved settings with credentials and sensitive material redacted. Gossip
exposes per-node prerequisites and the running committed revision. `DESCRIBE CLUSTER` and
metrics distinguish committed parameters from running values, expose per-node capabilities,
and show effective negotiated bounds. Refusals identify the setting and failed relationship
without disclosing secrets. Connector-specific values remain in their owning manual and
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
`nervix-server` process with a drain timeout above its shutdown timeout or election timeouts out of
order and observe the refusal naming both settings, and restart a node of a one-node and a
three-node cluster with such a setting, through the harness step that names a setting for one
node, before restarting it with a valid one. Converting and validating the options takes no lock
and adds no lifecycle path, so the deadlock detector has nothing of it to observe.

