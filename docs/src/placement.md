# Placement Policies

*Plexus, the Nervix graph definition language.*

Placement policies control where a domain's schedulable work executes. Use them to keep a
latency-sensitive processing corridor on one machine, prefer locality without forcing it, or bias
expensive stages across machines.

Nervix distinguishes two kinds of nodes throughout this chapter:

- A **runtime node** is a schedulable graph entity such as a relay, ingestor, processor, lookup,
  generator, or emitter.
- A **cluster node** is a machine participating in the Nervix cluster.

A named placement rule marks the directed paths between existing runtime nodes. For example, this
rule keeps corresponding units of `feature_normalizer`, `risk_scorer`, and every runtime node
on a path between them together:

```plexus
CREATE PLACEMENT scoring_local
  FROM feature_normalizer
  TO risk_scorer
  REQUIRE COLOCATION
  RANK 1;
```

Placement changes assignments, not graph routing. Records still follow the routes declared by the
ingestors, processors, and emitters.

## Choosing A Policy

The policies occupy one colocation scale; `NEUTRAL` expresses no opinion on that scale. There is
no hard-separation policy.

| Policy | Scheduling effect | Existing assignments |
| --- | --- | --- |
| `REQUIRE COLOCATION` | Corresponding units on every covered hop share a primary owner. | A newly effective requirement can relocate unit groups. |
| `PREFER COLOCATION` | New placement decisions favor the same cluster node. | The preference does not move existing assignments by itself. |
| `NEUTRAL` | The placement rule expresses no preference, so ordinary scheduler heuristics remain active. | It does not move assignments by itself. |
| `SUGGEST SEPARATION` | New placement decisions favor different cluster nodes. | The suggestion does not move existing assignments by itself. |

Use `REQUIRE COLOCATION` when a remote hop is unacceptable, especially for a short
latency-critical path or a materialized-state reader. Use `PREFER COLOCATION` when locality is
valuable but the scheduler should retain freedom to distribute work. Use `SUGGEST SEPARATION` to
reduce the chance that expensive stages share one machine, not to create an isolation boundary.

For new assignments, an explicit soft policy takes precedence over the scheduler's built-in
upstream-locality preference. It remains advisory: cluster availability and the scheduler's other
placement inputs can still produce a different result. `NEUTRAL` means "no placement opinion";
it does not randomize placement or disable the scheduler's normal heuristics.

## Execution Units And Hop Classes

A runtime node has one or more execution units. Unbranched work and a branch with one partition
have one unit. A branch with several partitions has one unit per partition index. An instanced
ingestor has one unit per instance index. Each unit has its own primary owner, replicas and
ownership transition. Concrete branch keys within one partition share that partition's owner.

Nervix classifies each directed hop before applying its effective policy:

| Hop class | Correspondence | Applicable policies |
| --- | --- | --- |
| `identity` | Both sides are single, or their partition structures correspond index for index. An intake declared `INSTANCES PER PARTITION` maps instance `p` to relay partition `p`, and so does an emitter declared `INSTANCES PER PARTITION` from each input relay. | All four policies. |
| `fiber` | A declared mapping determines the destination unit without inspecting a record. A partitioned relay feeding a single emitter maps every partition to that emitter, and an input relay feeding an emitter declared `INSTANCES n` maps partition `p` to instance `p mod n`. | All four policies, applied to each fiber. |
| `single↔many` | One side has one unit and the other has several, without an identity or fiber contract. | `NEUTRAL` and `SUGGEST SEPARATION`. |
| `data-dependent` | The unit reached depends on record data or competing consumption. Examples are a competing-consumer ingestor feeding partitions and a re-sharding reingestor's output. | `NEUTRAL` only. |

An identity requirement joins partition `p` only to its corresponding units; it does not join
partition `p` to another partition. A fiber requirement joins all units that map to the same
recipient. For example, requiring locality at a single emitter necessarily joins its input
partitions with that emitter. That is the declared correspondence, and inspection shows the class.

Validation uses the winning policy after rank resolution. A stronger `NEUTRAL` rule can therefore
carve an inapplicable hop out of a broader requirement. A refusal names the hop and its class.
Every candidate graph is checked, including changes to instancing, branch partitions, branch
selection and routes. An alteration cannot silently invalidate an existing placement rule.

## Defining A Domain-Wide Rule

A domain-wide rule covers every eligible message or state hop in the active domain:

```plexus
CREATE PLACEMENT local_execution DOMAIN WIDE REQUIRE COLOCATION RANK 2;
```

It is an ordinary named rule, with the same rank resolution, hop validation, inspection and
lifecycle as a corridor rule. A graph containing an intake whose partition is chosen by record data
needs a stronger `NEUTRAL` corridor on that intake before the broad requirement can be created.
Hops without a named rule contribute no placement score and use ordinary scheduler heuristics.

Create the domain separately, then declare the rule in that domain:

```plexus
CREATE UNPACED DOMAIN realtime;
CREATE PLACEMENT prefer_local DOMAIN WIDE PREFER COLOCATION;
```

Changing a running domain's rules activates a new schedule. A newly effective hard requirement can
relocate units; changing only a soft policy preserves existing assignments.

## Defining A Named Corridor

Create a named rule after every referenced member exists in the active domain. Placements are
domain-owned and cannot span domains:

```plexus,ignore
CREATE [IF NOT EXISTS] PLACEMENT <placement_name>
  FROM <runtime_node> [, <runtime_node> ...]
  TO <runtime_node> [, <runtime_node> ...]
  REQUIRE COLOCATION | PREFER COLOCATION | NEUTRAL | SUGGEST SEPARATION
  [RANK <positive_integer>];
```

Each `FROM` member is paired with each `TO` member. Nervix evaluates those directed endpoint pairs
independently and applies the policy only where a path exists.

The following user-declared runtime-node kinds are placement-eligible:

- ingestors, except cluster-wide server-listener ingestors;
- reingestors and generators;
- junctions, deduplicators, correlators, reorderers, and window processors;
- inferencers and WASM processors;
- emitters and `HASH MAP` lookups; and
- every relay, whether or not it has materialized state.

A relay name always denotes its relay runtime node. Enabling materialized state adds
scheduler-selected state replicas to that relay; it does not add another placement member or
runtime-node kind. See [Materialized Relay State](processors.md#materialized-relay-state) for
dependency behavior.

`HTTP ENDPOINT`, `WEBSOCKETS ENDPOINT`, and `SYSLOG LISTENER` ingestors execute on every cluster
node and therefore cannot be constrained by a placement rule. An ingestor's head fixes the pair it
reads, so altering an ingestor never changes whether it can be a placement member. Completion in
`FROM` and `TO` offers only placement-eligible runtime nodes, applying the same exclusion to all
three listener kinds. Member names are unqualified; a name shared by more than one
eligible entity kind is ambiguous and must be changed before it can be used in a placement.

Duplicate names on one side collapse. Both sides must contain at least one member after duplicate
removal. The same runtime node may appear in `FROM` and `TO`, but it contributes coverage only when
the graph contains a directed cycle back to that runtime node.

## Understanding Path-Gated Coverage

For each connected `FROM`/`TO` pair, the rule captures both endpoints and every runtime node that
can carry traffic from the source to the destination. If the graph contains alternate paths, all
runtime nodes on those paths are part of the corridor. A cycle within a corridor is captured in
full.

For this graph:

```text
feature_normalizer -> enrich_features -> risk_scorer
                   -> validate_features -> risk_scorer
```

a rule from `feature_normalizer` to `risk_scorer` covers all four runtime nodes. The policy applies
on each direct hop within that corridor, with the correspondence of that hop; it is not a pairwise
requirement between every captured node.

Placement follows the relationships that can deliver messages or state:

- normal output routes;
- message-error routes;
- correlation-timeout routes;
- generator feeds; and
- materialized-state dependencies, directed from the relay to each reader
  that declares `USING MATERIALIZED STATE`.

Relays are corridor members. A producer-to-relay or relay-to-consumer relationship is one placement
hop. A domain-wide rule covers both independently, and a corridor captures relay nodes between
its endpoints.

A disconnected endpoint pair contributes no coverage. A rule for which every endpoint pair is
disconnected is valid and remains available for inspection with `coverage=empty`. Nervix does not
force unconnected workloads into a group merely because their names occur in one rule.

Separate connected corridors are also evaluated independently. Runtime nodes captured by two
different corridors in one rule are not related to each other unless they occur together in at
least one corridor.

Coverage is recalculated whenever the graph activates. Adding or removing a route can therefore
grow or shrink an existing rule without altering the rule itself. Inspect placement coverage after
topology changes, especially when a broad rule uses several `FROM` or `TO` members.

## Resolving Overlapping Rules

`RANK` decides which named rule wins where rules overlap. The policy names themselves do not
establish precedence: a stronger-ranked `NEUTRAL` rule overrides a weaker-ranked
`REQUIRE COLOCATION` rule on the relationships they both cover.

Precedence is:

1. rank 1;
2. rank 2, rank 3, and subsequent explicit ranks in ascending numeric order;
3. the unranked tier.

Domain-wide and corridor rules use this same order. A domain-wide rule is not weaker merely
because its scope is broader.

Every explicitly ranked rule outranks every unranked rule, regardless of the numeric value. All
unranked rules share one tier. At the strongest rank affecting a hop,
equal-rank rules with the same policy agree. Different policies at that strongest rank conflict,
and Nervix rejects the candidate graph activation with the rule names and an affected runtime-node
pair. Weaker overridden rules do not conflict with one another.

Effective `REQUIRE COLOCATION` unit bonds are transitive. If one requirement joins unit `a` to
unit `b` and another joins unit `b` to unit `c`, all three form one colocation group. A stronger soft or neutral rule
can remove specific hard relationships, but an execution unit remains in the group while any other
effective `REQUIRE COLOCATION` chain still connects it.

For example, first declare a scheduler-managed archive corridor:

```plexus
CREATE PLACEMENT archive_scheduler_managed
  FROM archiver
  TO cold_sink
  NEUTRAL
  RANK 1;
```

Then declare a weaker domain-wide requirement:

```plexus
CREATE PLACEMENT domain_local DOMAIN WIDE REQUIRE COLOCATION RANK 2;
```

The rank-1 `NEUTRAL` rule overrides the rank-2 requirement on its connected corridor's hops. It does not guarantee
that the archive stages run on different cluster nodes; it returns those relationships to ordinary
scheduler heuristics. Use `SUGGEST SEPARATION` instead when the carve-out should also express a
soft spreading preference.

## Runtime Behavior

Placement coverage, rank resolution, and colocation groups are recalculated as part of graph
activation. A conflicting candidate is rejected before it replaces the active graph.

Every single-owner ingestor keeps its existing primary and replica assignment across schedule
recomputation while all assigned cluster nodes are live. This includes outbound WebSocket-client
ingestors. Model changes, cluster-node joins and uncordons, and soft placement changes therefore do
not migrate those ingestors. `HTTP ENDPOINT`, `WEBSOCKETS ENDPOINT`, and `SYSLOG LISTENER`
ingestors are the only ingestors that instead follow live cluster membership, because their server
listeners execute on every cluster node.

When a new `REQUIRE COLOCATION` relationship becomes effective, Nervix consolidates the complete
colocation group onto one eligible cluster node. Existing assignments are preserved only when
they satisfy the hard requirement. On a running domain, Nervix plans every relocation before
holding the graph, gates all moved groups together, drains their admitted work, and publishes the
placement model and assignments in one schedule update. A timeout leaves both the placement model
and schedule unchanged. The immediate command, or the aggregate `COMMIT` result for a transaction,
reports `quiesce level: ENTITY_PAUSE` and the number of planned relocations. A change with no owner
move reports `DYNAMIC`.

`PREFER COLOCATION` and `SUGGEST SEPARATION` affect new placement decisions, including newly
created runtime nodes and targets selected during failover or drain. They never move an existing
assignment merely to improve a preference. `NEUTRAL` contributes no placement score. Removing a
hard requirement also does not spread runtime nodes that are already on the same cluster node.

Failover and drain relocate a hard colocation group as one unit, so an executing assignment never
violates `REQUIRE COLOCATION`. A cordoned cluster node is not considered for a new primary or
ordinary replica assignment. During a successful planned move, however, a live former primary is
kept as the first replica when the configured replica count provides a slot. See
[Control Plane](control-plane.md#planned-ownership-handoffs-and-failover) for the gate, drain,
failure, and activation behavior.

Explicit relocation with `RELOCATE` is the third planned ownership change: it moves a selected
subgraph onto a cluster node you name, through the same handoff, and leaves every runtime node
outside the selected unit groups untouched.

Drain, colocation consolidation, and explicit relocation use a planned handoff and restart only
runtime nodes whose primary owner changed. A runtime node that keeps its assignment continues on every cluster node,
including the former and new owners of a moved neighbor: it keeps buffered work, retained
`REQUIRED WAIT` messages, branch-local state, and external ingestor sessions. Its neighbors re-point
at the committed revision. Draining a host moves hard groups and independent execution units one at a time in
canonical kind/name/unit order. Failover uses immediate termination because the former owner is already
unavailable.

Placement constrains primary owners. It does not control replica count or replica placement. An
ordinary relay has no replicas; a materialized relay's replicas contain only materialized state.
Concrete branch keys do not have separate placement: their declared partition is the unit of
ownership. The scheduler balances primary counts across eligible cluster nodes before comparing
ordinary assignment load, then uses round-robin ordering to break ties. Replicas do not count as
additional active consumers. Existing live assignments remain sticky.

A hard identity group is placed independently for each partition index, so corresponding relay and
processor partitions stay together while different groups can spread. Soft policies also score
corresponding units. Upstream locality favors matching partition owners. Failover and drain move
only the affected unit's complete hard group, preserving every unaffected unit's owner, state and
WASM generation. See [Relay Partitions](relay.md#relay-partitions).

## Relocating Runtime Nodes

Placement rules constrain where the scheduler puts work. `RELOCATE` moves chosen work onto a
cluster node you name, and `DESCRIBE RELOCATION` shows the plan a statement would execute without
executing it:

```plexus,ignore
RELOCATE <selection>
  ONTO NODE <node_id>
  FOLLOW PREFERENCES | IGNORE PREFERENCES
  [FOR <kind> <name> [PARTITION <p> | INSTANCE <i>] FOLLOW PREFERENCES | IGNORE PREFERENCES ...];

DESCRIBE RELOCATION <selection>
  ONTO NODE <node_id>
  FOLLOW PREFERENCES | IGNORE PREFERENCES
  [FOR <kind> <name> [PARTITION <p> | INSTANCE <i>] FOLLOW PREFERENCES | IGNORE PREFERENCES ...];
```

`DESCRIBE RELOCATION` accepts exactly the clauses of `RELOCATE` after the verb, so a plan can be
inspected and then executed by changing one word.

### Selecting The Subgraph

A selection is either an explicit list or a directed corridor, using the same path-gated coverage
named placement rules use:

```plexus
RELOCATE JUNCTION risk_scorer ONTO NODE node-2 FOLLOW PREFERENCES;
```

```plexus
RELOCATE FROM JUNCTION feature_normalizer TO JUNCTION risk_scorer
  ONTO NODE node-2
  FOLLOW PREFERENCES
  FOR DEDUPLICATOR dedup_txns IGNORE PREFERENCES;
```

Every member is kind-qualified, because a name is unique only within its kind. The kind uses the
same spelling as `SHOW CREATE <kind> <name>`:

| `<kind>` | Runtime node |
| --- | --- |
| `INGESTOR` | An ingestor other than an endpoint or Syslog ingestor. |
| `REINGESTOR`, `GENERATOR` | The named reingestor or generator. |
| `JUNCTION`, `DEDUPLICATOR`, `CORRELATOR`, `REORDERER`, `WINDOW PROCESSOR` | The named processor. |
| `INFERENCER`, `WASM PROCESSOR` | The named processor. |
| `EMITTER` | The named emitter. |
| `HASH MAP` | The named lookup; reported as kind `lookup` in schedule output. |
| `RELAY` | The named relay, materialized or not. |

Any other kind keyword is a parse error. Names resolve in the session's active domain, duplicate
members collapse. An unqualified member selects all its units. In an explicit list, append
`PARTITION <p>` or `INSTANCE <i>` to select one exact unit:

```plexus
RELOCATE JUNCTION risk_scorer PARTITION 0
  ONTO NODE node-2 IGNORE PREFERENCES;
```

The selector must name an existing unit of the requested kind; an invalid index is refused before
execution. A corridor whose every endpoint pair is disconnected is rejected; a partially connected
corridor is valid and the plan reports each pair's connectivity. The destination must be a Raft
voter that is live and not cordoned.

### Building The Unit

The set of hard groups a statement moves is its unit. `REQUIRE COLOCATION` is not a preference: a
selected execution unit always moves with its whole hard group. A selection of one partition
does not select its sibling partitions. Soft preferences are shaped by the
strategy:

- `FOLLOW PREFERENCES` moves each `PREFER COLOCATION` partner of the group along and holds each
  `SUGGEST SEPARATION` partner out of the unit.
- `IGNORE PREFERENCES` moves the group and lets its preferences shape nothing: its partners are
  neither captured nor held back by it.

The strategy after `ONTO NODE` is mandatory and applies to every hard group. A `FOR` clause names
a runtime node or one exact unit and sets the strategy of every selected hard group, because a group
cannot be moved in pieces and its preferences are one set. Two `FOR` clauses that disagree within
one hard group are rejected, and a `FOR` clause naming a runtime node outside the plan is rejected.
An unqualified `FOR` member addresses all its declared units; every one must belong to the plan.
Use an exact selector when the relocation contains only one partition or instance of that node.

Capture is transitive and follows the statement default. A captured group inherits that default, so
`FOLLOW PREFERENCES` closes the unit over `PREFER COLOCATION` relationships until a
`FOR ... IGNORE PREFERENCES` cuts the chain, while `IGNORE PREFERENCES` with a
`FOR ... FOLLOW PREFERENCES` opens capture at one group only. Under an effective `PREFER COLOCATION` domain-wide
rule, a `FOLLOW PREFERENCES` relocation therefore moves the whole connected component reachable
from the selection. Soft preferences never cause a rejection on their own; the plan reports every
preference the move leaves unsatisfied.

### Reading The Plan

Both statements return the same block, and `RELOCATE` prefixes it with the executed outcome and
appends the hold duration:

```plain
relocation onto node 'node-2'
quiesce level: ENTITY_PAUSE
gated relays: normalized_features, scored_events
coverage:
- junction feature_normalizer -> junction risk_scorer connected=yes covered=2
unit:
- kind=junction name=feature_normalizer group=1 strategy=follow reason=selected owner=node-1 moves=yes replicas=node-1 promoted_replica=no
- kind=junction name=risk_scorer group=1 strategy=follow reason=selected owner=node-1 moves=yes replicas=node-1 promoted_replica=no
- kind=deduplicator name=dedup_txns group=2 strategy=ignore reason=preferred owner=node-2 moves=no replicas=node-3
unsatisfied preferences: 1
- suggest separation risk_scorer <-> archive_transform (spread_archive)
```

`coverage:` appears for the corridor form only. `gated relays:` lists the relays the hold gates, or
`-` for a `DYNAMIC` plan. `group` numbers the hard groups in the unit and `reason` is `selected`,
`required` for a hard-group mate, or `preferred` for a captured partner. `owner` is the current
owner; a member that moves also reports its new replicas and whether the destination was already a
replica. `unsatisfied preferences` counts, after the move, every `PREFER COLOCATION` relationship
touching the unit whose two owners differ and every `SUGGEST SEPARATION` relationship touching the
unit whose two owners are the same, naming the winning rule.

`kind` and `name` use the schedule spelling shown by `SHOW CLUSTER STATUS`, so a `HASH MAP` member
appears as `kind=lookup` and a `RELAY` member as `kind=relay`.

### Executing The Move

A relocation in a running domain that moves at least one execution unit is a planned ownership change
and executes as one unit, one hold, one commit: the hold gates exactly the relays the plan lists,
every moved execution unit drains its admitted work, one schedule write assigns the whole selection to the
destination, and the gates open once the destination has activated the revision. The unit is
atomic, because the plan is what you inspected and approved; a unit containing a runtime node that
cannot drain is excluded with a narrower selection or `IGNORE PREFERENCES`, or the stall is
resolved and the statement retried. See
[Control Plane](control-plane.md#planned-ownership-handoffs-and-failover) for the shared hold,
drain, and failure behavior.

Successful relocation returns after the destination has activated the exact schedule revision, the
source has drained, and the handoff gates have opened. A following command can use the destination
immediately. Cordon and uncordon similarly return after the eligibility state is authoritative;
drain and node removal return after resulting ownership and schedules are usable on the remaining
live set.

The leader refuses the relocation's schedule write when it lost its leadership or cannot confirm it,
for example because its lease lapsed. Such a relocation moves nothing and is not reported as failed.
Its outcome stays unknown until the statement is repeated under the same execution reference,
which a client that follows the session protocol does on its own, and the leader that serves the
repetition plans the unit again from the schedule committed by then. A relocation whose plan went
stale before its write, for example because the schedule changed, fails, and you retry the
statement.

The reported quiesce level is `ENTITY_PAUSE` when the plan moves at least one execution unit in a
running domain, and `DYNAMIC` when the plan moves nothing or the domain is stopped. A stopped
domain keeps its schedule, so its runtime nodes are relocated without a hold and execute on the
destination when the domain starts. A unit member whose owner is already the destination is
retained in the plan and left untouched; a unit member whose owner is unavailable is rejected until
failover has given it a live owner.

For each moved member the primary assignment becomes the destination, and replicas are recomputed
against the configured replica count: the former owner first when it is a live schedulable voter,
then the member's existing replicas other than the destination, then the cluster nodes the
scheduler would choose. A relay without materialized state has no replicas regardless of the count.
Kafka `OFFSET BY DOMAIN` partition-to-instance assignments are preserved verbatim, and the host of
a moved hard group becomes the destination.

`RELOCATE` is a leader operation that holds the domain's exclusive alteration lock from planning
through release, so a model change, a placement change, another `RELOCATE`, or a `DRAIN NODE` of
the same domain is rejected with the concurrent-alteration diagnostic rather than queued.
`DESCRIBE RELOCATION` is read-only and is served by any cluster node.

A relocation records no affinity. It is a one-time move, not a pin: a later failover, drain, or
newly effective `REQUIRE COLOCATION` consolidation may move the runtime node again, and Nervix does
not move it back when its former owner returns. A preference left unsatisfied by a relocation stays
unsatisfied, because Nervix never moves an existing assignment to improve a preference.

## Altering And Dropping Rules

`ALTER PLACEMENT` accepts one or more comma-separated operations:

```plexus,ignore
ALTER PLACEMENT <placement>
    SET POLICY <policy>
  | SET RANK <positive_integer>
  | DROP RANK
  | SET FROM <runtime_node> [, ...] TO <runtime_node> [, ...]
  | SET DOMAIN WIDE
  | RENAME TO <placement_name>
  [, <operation> ...];

DROP PLACEMENT <placement>;
```

Operations execute in written order, and the complete statement is applied atomically. `SET FROM
... TO ...` replaces the coverage with a corridor; `SET DOMAIN WIDE` covers every eligible hop
and releases endpoint pins. This makes it possible to change policy, precedence,
membership, and name in one statement:

```plexus
ALTER PLACEMENT scoring_local
  SET POLICY PREFER COLOCATION,
  SET RANK 2,
  RENAME TO scoring_preferred;
```

A corridor placement pins the entities named in its `FROM` and `TO` lists even when its current coverage is
empty. Dropping a referenced runtime node is blocked until every pinning placement is altered or
dropped. A change that would make a member ineligible, such as changing an ingestor to an endpoint
or Syslog ingestor, is blocked in the same way. Adding or dropping a relay's materialized state does
not change its placement identity.

When a topology edit and its placement update depend on one another, put the complete set of model
changes in one explicit transaction and order creation before reference. Nervix validates the
resulting candidate graph before publishing it. See [Control Plane](control-plane.md) for
transaction and quiesce behavior.

## Inspecting Placement

Use the placement commands in the active domain:

```plexus
SHOW CREATE PLACEMENT scoring_local;
```

```plexus
SHOW PLACEMENTS;
```

```plexus
DESCRIBE PLACEMENT scoring_local;
```

```plexus
DESCRIBE DOMAIN;
```

`SHOW CREATE PLACEMENT` renders the normalized stored rule as canonical Plexus and omits the
creation-time `IF NOT EXISTS` modifier. `SHOW PLACEMENTS` lists each rule's scope, policy, rank, per-hop classes and effective policies, and one
of these coverage states:

| Coverage | Meaning |
| --- | --- |
| `empty` | No covered hop exists; for a corridor, no endpoint pair has a directed path. |
| `effective` | All endpoint pairs are connected and all of the rule's resulting relationships win rank resolution. |
| `partial` | Only some endpoint pairs are connected, or only some resulting relationships remain effective. |
| `overridden` | Connected coverage exists, but stronger rules win every resulting relationship. |

`DESCRIBE PLACEMENT` provides the stored form, policy, rank, resolved members, connectivity and
covered runtime nodes for each endpoint pair, witness paths for captured intermediates, and the
hop class, effective policy and winning or overriding rule for every covered hop. Domain-wide
rules report their scope and hops directly, without synthetic endpoints. For effective
hard requirements it also reports the colocation group and its current host cluster node.

`DESCRIBE DOMAIN` reports named rules, their domain-wide or corridor scope, the per-hop classes and
effective policies, and every effective colocation group. Each group includes its exact unit
members, host cluster node, and the effective hard bonds holding it together. Use those relationships to diagnose why a runtime node remains
in a larger group after a carve-out.

`SHOW CLUSTER STATUS` provides the complete scheduled-owner view when you need to compare placement
with other runtime nodes outside hard colocation groups.

## Troubleshooting

| Symptom | What To Check |
| --- | --- |
| A rule shows `coverage=empty`. | Confirm the route direction and that a directed message or state path exists from a `FROM` member to a `TO` member. Disconnected rules are valid. |
| A rule is `partial` or `overridden`. | Run `DESCRIBE PLACEMENT` and inspect the winning rule for each affected relationship. |
| Activation reports an inapplicable policy. | Inspect the named hop's class. Choose a permitted policy or add a stronger neutral carve-out before applying a broad requirement. |
| Activation reports a placement conflict. | Two overlapping rules have different policies at the strongest rank for an affected relationship. Change one policy or give one rule a stronger rank. A topology edit can expose a previously latent overlap. |
| `SUGGEST SEPARATION` still leaves stages together. | The policy is advisory and never migrates existing assignments by itself. Re-evaluate it on a new placement decision; do not use it as an isolation control. |
| A soft-policy change did not move anything. | This is expected. Use `REQUIRE COLOCATION` only when movement is justified by a hard locality requirement. |
| A runtime node remains in a hard group after a carve-out. | Inspect the group's hard relationships in `DESCRIBE DOMAIN`; another uncut requirement still connects it to the group. |
| Dropping or reshaping a member is blocked. | Alter or drop every placement named by the diagnostic before changing the member. |

Creation also rejects an unknown or ambiguous member, a non-schedulable entity, a cluster-wide
server-listener ingestor, an empty side, and `RANK 0`.

## Common Patterns

Keep a latency-critical corridor on one cluster node:

```plexus
CREATE PLACEMENT fraud_scoring_local
  FROM normalize_transaction
  TO fraud_scorer
  REQUIRE COLOCATION
  RANK 1;
```

Keep a materialized-state owner with a reader. Here `customer_state` is a materialized relay and
`fraud_scorer` declares `USING MATERIALIZED STATE customer_state`:

```plexus
CREATE PLACEMENT state_read_local
  FROM customer_state
  TO fraud_scorer
  REQUIRE COLOCATION
  RANK 1;
```

Bias a heavy archive corridor across cluster nodes without guaranteeing isolation:

```plexus
CREATE PLACEMENT spread_archive
  FROM archive_transform
  TO archive_emitter
  SUGGEST SEPARATION;
```

Prefer locality for a corridor while retaining scheduler flexibility:

```plexus
CREATE PLACEMENT prefer_enrichment_local
  FROM decode_events
  TO enrich_events
  PREFER COLOCATION;
```

## Operational Boundaries

Placement is not a capacity planner or a general load-balancing control. A hard colocation group
must fit on one cluster node, and Nervix does not use placement rules to calculate CPU or memory
capacity. Keep hard groups as narrow as the latency or state-locality requirement allows, then
observe host resource use and inter-node delivery latency.

Placement policies do not provide:

- hard separation or workload isolation;
- grouping for runtime nodes with no directed path between them;
- cluster-node labels, availability zones, or pinning to a named cluster node;
- per-branch placement; or
- replica-count or replica-placement control.

`RELOCATE` moves work onto a named cluster node once. It is not pinning: nothing keeps the moved
runtime node there, and a later failover, drain, or hard-group consolidation may move it again. It
also does not relocate endpoint or Syslog ingestors, an individual replica, an individual branch, a
unit spanning domains, and it never applies a selection in part. An unqualified partitioned node
selects every partition; an exact selector moves only that unit and its hard-group mates. Each
retains its unit identity, state and replica choice.

Use `SUGGEST SEPARATION` as a performance hint only. Security, tenant isolation, and failure-domain
requirements need controls outside placement policies.

## Ingestor Instance Units

An instance-capable ingestor with `INSTANCES n` has one `Instance(i)` placement per global index.
Every unit has its own primary, replicas and ownership transition. The scheduler balances primary
counts across eligible nodes before comparing ordinary assignment load, so replica slots do not
make one primary appear to be several active consumers. Existing live assignments remain sticky.

`INSTANCES PER PARTITION` introduces an exact colocation component for each index: ingestor instance
`p` and partition `p` of every relay whose branch it constructs. These components move independently.
Named rules add bonds only through each hop's validated correspondence. Placement groups store
exact `NodeUnitRef` members, including different unit kinds within one component. Shared group
drain, failover and explicit relocation update only those members; ordinary relay members need no
state replica to move with a stateful ingestor. See [Instancing](ingestors.md#instancing).

## Emitter Instance Units

An emitter declared `INSTANCES n` or `INSTANCES PER PARTITION` has one `Instance(i)` placement per
instance, each with its own primary and ownership transition; an emitter without the clause keeps
its one `Single` unit. Instance `i` of `n` serves the input partitions `p` with `p mod n = i`.

`INSTANCES PER PARTITION` introduces an exact colocation component for each index, as an intake's
does: emitter instance `p` and partition `p` of every input relay. An instance of `INSTANCES n`
has no hard component of its own. It prefers the node that owns most of the partitions it serves,
and a named `REQUIRE COLOCATION` rule on the fiber hop from an input relay joins it with all of
them. An instance beyond the partition count serves no partition; it is placed like its siblings
and runs nothing. See [Emitter instances](emitters.md#emitter-instances).
