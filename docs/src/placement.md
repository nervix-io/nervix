# Placement Policies

*Plexus, the Nervix graph definition language.*

Placement policies control where a domain's schedulable work executes. Use them to keep a
latency-sensitive processing corridor on one machine, prefer locality without forcing it, bias
expensive stages across machines, or keep work on the cluster nodes that can run it.

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

A node rule instead selects the cluster nodes a runtime node's units may run on, or should prefer,
by the facts in each node's committed record. See [Node Rules](#node-rules).

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
| `identity` | Both sides are single, or their partition structures correspond index for index. An intake declared `INSTANCES PER TRANSPORT PARTITION` maps instance `p` to relay partition `p`, and so does an emitter declared `INSTANCES PER PARTITION` from each input relay. | All four policies. |
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
at the committed revision. A moved processor carries its branch-local state to the destination as
each branch published it at the handoff's freeze: deduplicator keys, window rows, WASM guest
checkpoints and stateful inferencer tensors. Draining a host moves hard groups and independent execution units one at a time in
canonical kind/name/unit order. Failover uses immediate termination because the former owner is already
unavailable.

Colocation rules constrain primary owners. They do not control replica count or replica
placement; a node rule's requirement restricts every assignment slot, replicas included. An
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
  | SET POLICY { REQUIRE NODES WHERE <selector> | PREFER NODES WHERE <selector> | ANY NODE }
  | SET RANK <positive_integer>
  | DROP RANK
  | SET FROM <runtime_node> [, ...] TO <runtime_node> [, ...]
  | SET FOR <runtime_node> [, ...]
  | SET DOMAIN WIDE
  | RENAME TO <placement_name>
  [, <operation> ...];

DROP PLACEMENT <placement>;
```

Operations execute in written order, and the complete statement is applied atomically. `SET FROM
... TO ...` replaces a colocation rule's coverage with a corridor; `SET FOR` replaces a node rule's
members; `SET DOMAIN WIDE` covers every eligible hop or runtime node and releases the member pins.
An operation of the other form is refused: a colocation policy or `SET FROM ... TO ...` on a node
rule, a node policy or `SET FOR` on a colocation rule. This makes it possible to change policy,
precedence, membership, and name in one statement:

```plexus
ALTER PLACEMENT scoring_local
  SET POLICY PREFER COLOCATION,
  SET RANK 2,
  RENAME TO scoring_preferred;
```

A corridor placement pins the entities named in its `FROM` and `TO` lists even when its current coverage is
empty, and a node rule pins its `FOR` members the same way. Dropping a referenced runtime node is blocked until every pinning placement is altered or
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
creation-time `IF NOT EXISTS` modifier. `SHOW PLACEMENTS` lists each colocation rule's scope, policy, rank, per-hop classes and effective policies, and one
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
with other runtime nodes outside hard colocation groups. [Observing Node
Rules](#observing-node-rules) covers what the same commands report for node rules.

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
| A change to a running domain is refused because a node rule does not admit a node. | The change would leave an executing unit on that node. Relocate the unit onto a node the rule admits first, or stop the domain; `DESCRIBE PLACEMENT` lists the matching nodes. |
| `START` does not answer. | A unit has no node its node rules admit, so `START` waits. `DESCRIBE DOMAIN` lists the waiting units and `SHOW CLUSTER STATUS` their `waiting_for=` rules; add or retag a node that satisfies them. |
| `SHOW CLUSTER STATUS` reports `violates=` or `waiting_for=`. | A node's record changed, or its owner was lost with no admitted destination. See [When Node Rules Take Effect](#when-node-rules-take-effect). |
| A node rule is `unmatched`. | No live node satisfies its selector now. Preview it with `SHOW NODES WHERE <selector>`. |

Creation also rejects an unknown or ambiguous member, a non-schedulable entity, a cluster-wide
server-listener ingestor, an empty side or member list, a node selector that does not compile over
the node schema, and `RANK 0`.

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
- per-branch placement; or
- replica-count control, or replica placement beyond the node rules a replica's node must satisfy.

`RELOCATE` moves work onto a named cluster node once. It is not pinning: nothing keeps the moved
runtime node there, and a later failover, drain, or hard-group consolidation may move it again. To
keep work on chosen nodes, declare a [node rule](#node-rules); `RELOCATE` refuses a destination
the moved units' node rules do not admit. It
also does not relocate endpoint or Syslog ingestors, an individual replica, an individual branch, a
unit spanning domains, and it never applies a selection in part. An unqualified partitioned node
selects every partition; an exact selector moves only that unit and its hard-group mates. Each
retains its unit identity, state and replica choice.

Use `SUGGEST SEPARATION` as a performance hint only. Security, tenant isolation, and failure-domain
requirements need controls outside placement policies.

## Capacity At The Load Tier

Only live eligible voters with a committed capability record for their current incarnation receive
placements. Captured claims are part of the replicated schedule fence and transaction planning
basis. A capacity change invalidates a plan captured before it.

The sticky scheduler keeps placement policy and upstream-locality scores ahead of capacity. At its
load tier it divides primary-unit counts and ordinary assignment load by relative capacity, then
keeps round-robin and node-name ties. Relative capacity is the smaller of CPU/minimum-CPU and
memory/minimum-memory among eligible nodes. A dimension participates only when every eligible node
has a measurement; with neither dimension available, weighting is uniform. Ratios compare exactly
as integers, including full-width claims. Doubling both CPU and memory therefore receives twice the
independent units: 32 independent partitions on capacities 1:2:1 distribute 8:16:8.

Unchanged inputs produce the same assignment, and existing live assignments remain sticky. A
restarting node retains its stored assignments while publication catches up; its current run waits
for the matching claim before activation. Eligibility restricts every new automatic or explicit
relocation destination.
Weighting does not override colocation, locality, explicit relocation or the independent publication
of server listeners on every live node. Inspect the claims and negotiated receive bounds with
`DESCRIBE CLUSTER`; [Node Settings](node-settings.md#committed-node-capabilities) owns their detection
and restart procedure.

## Selecting Nodes

Use `SHOW NODES` to list the cluster nodes that the answering node knows about. Each row shows
whether the node is live, a voter, cordoned or terminating. A node's committed record adds its
revision, CPU count, memory limit, detected properties and operator tags. A departed node's record
remains visible until `DROP NODE` removes it. A node that is still announcing its record appears
without a record in the unfiltered list.

Add `WHERE` to preview a selector against the committed records. For example, this lists the
nodes tagged `zone=eu` whose GPU and ONNX Runtime can use CUDA.

```plexus
SHOW NODES WHERE tag('zone') = 'eu' AND node.cuda.available;
```

Any node can answer this command without a selected domain. Its answer reflects the committed
records it has applied and the liveness it observes. The command is read-only. `FORMAT JSON`
selects the JSON projection of the same inspection. See
[Node Selectors](filter-map-functions.md#node-selectors) for the fields, types and `tag()` syntax.

## Node Rules

A node rule selects the cluster nodes a runtime node's units may run on, or should prefer, by the
facts in each node's committed record. It is a named placement rule whose body selects nodes
instead of relating hops:

```plexus,ignore
CREATE [IF NOT EXISTS] PLACEMENT <placement>
  { FOR <runtime_node> [, ...] | DOMAIN WIDE }
  { REQUIRE NODES WHERE <selector> | PREFER NODES WHERE <selector> | ANY NODE }
  [RANK <positive_integer>];
```

The selector is a [node selector](filter-map-functions.md#node-selectors): the boolean expression
`SHOW NODES WHERE` previews, over a node's name, detected properties and operator tags. This rule
keeps the scorer on the nodes tagged `zone=eu`:

```plexus
CREATE PLACEMENT eu_only FOR risk_scorer REQUIRE NODES WHERE tag('zone') = 'eu';
```

| Policy | Effect |
| --- | --- |
| `REQUIRE NODES WHERE` | Every unit of a covered runtime node runs only on a node the selector admits, for its primary and every replica. |
| `PREFER NODES WHERE` | New placement decisions favor the nodes the selector admits. A preference never moves an existing assignment. |
| `ANY NODE` | Lifts every weaker requirement, so a stronger-ranked carve-out frees some members of a broader `REQUIRE` rule. |

`FOR` names the runtime nodes the rule covers. It accepts the members a corridor accepts: relays,
ingestors, processors, lookups, generators and emitters, but not a server-listener ingestor, which
runs on every cluster node. `DOMAIN WIDE` covers every such runtime node of the domain, including
ones created later. A selector is compiled when the rule is admitted, so a selector that names an
unknown node property, or is not a boolean condition, is refused at once. A node on which a selector fails to evaluate is not
admitted, and `DESCRIBE PLACEMENT` reports the failure.

### Requirements And Preferences

Each runtime node a node rule covers resolves two ladders. They are separate from the colocation
rules and from each other, and they use the colocation rules' rank precedence: rank 1, the other
explicit ranks in ascending order, then the unranked tier.

- The **requirements ladder** holds the covering `REQUIRE NODES` and `ANY NODE` rules. At its
  strongest rank every `REQUIRE` selector must admit a node for a unit to run there, and weaker
  rules are overridden. An `ANY NODE` rule at the strongest rank lifts every requirement. An
  `ANY NODE` rule beside a `REQUIRE` rule at the strongest rank is a conflict, and the change is
  refused naming both rules and the runtime node.
- The **preferences ladder** holds the covering `PREFER NODES` rules. Each selector at its
  strongest rank that admits a candidate node adds one to that node's preference score.

A hard colocation group may run only on the nodes that every member's requirements admit, and its
preference score is the sum of its members' scores. The scheduler restricts each unit's candidate
nodes to the admitted ones, then ranks them by preference score, the colocation score, upstream
locality, capacity-weighted load, round robin and node name, in that order. The test-only random
scheduler draws from the same admitted nodes.

For example, a domain-wide requirement with a stronger carve-out and a preference:

```plexus
CREATE PLACEMENT fast_only DOMAIN WIDE REQUIRE NODES WHERE tag('pool') = 'fast' RANK 2;
CREATE PLACEMENT archiver_anywhere FOR archiver ANY NODE RANK 1;
CREATE PLACEMENT prefer_gpu FOR fraud_scorer PREFER NODES WHERE node.cuda.available;
```

Every runtime node runs on a `pool=fast` node except `archiver`, which may run anywhere, and
`fraud_scorer` favors a node with CUDA among the fast ones.

### When Node Rules Take Effect

A node rule binds every placement decision from the moment it is accepted. Fresh schedules,
failover, drain and explicit relocation all choose among the admitted nodes.

On a running domain, a change is refused when it would leave an executing unit on a node its
winning requirements do not admit, naming the rule, the node and the unit: creating or altering a
node rule, or any other model change. A change that would leave a unit without any admitted node is
refused naming the rule and the unit. The model and schedule stay unchanged, and a transaction's
preview reports the same refusal. A breach the cluster left behind, described below, does not block
unrelated changes. `RELOCATE` refuses a destination the moved units' rules do not admit, and so does
its `DESCRIBE RELOCATION` preview.

On a stopped domain nothing executes, so a node rule is accepted whatever the stored assignments
are; it constrains the schedule the domain starts with. `START` first places every unit on a node
its rules admit. A unit stored on a node its rules no longer admit moves, with its state, through
the planned ownership handoff. While some unit has no admitted node, `START` waits, much as a
Kubernetes pod stays Pending until a node matches. It holds the domain's mutation lease, so the domain's
other changes are refused meanwhile, and it continues by itself once a node that satisfies the
rules is live with a committed record. Nothing starts until every unit can be placed. A `START`
in the same transaction as a node-rule change that would leave a breach is refused; commit the
change first, then `START`.

The cluster can still change under an accepted rule:

- When a node's committed record changes so that it no longer satisfies a rule, for example after
  a restart without its tag, the units it runs keep running there. The scheduler keeps the
  assignment and reports it `violates=<rule>`; records keep flowing, and the unit moves only when
  you relocate it onto an admitted node.
- When a unit's owner is lost and no admitted node can take it, the unit keeps its last assignment
  and is reported `waiting_for=<rule>`. It is placed as soon as an admitted node is available.
- On a stopped domain nothing runs, so a unit stored on a node its rules do not admit is reported
  `violates=<rule>` while an admitted node could take it, where `START` moves it, and
  `waiting_for=<rule>` while none could, which is what `START` waits for.

### Observing Node Rules

`SHOW PLACEMENTS` lists each node rule with its scope, policy, rank and coverage, then one line per
member naming the policy that wins there or the rules that override it:

```text
eu_only scope=members policy=REQUIRE NODES rank=unranked coverage=effective
eu_only member=risk_scorer effective=REQUIRE NODES
```

A node rule reports one of these coverage states:

| Coverage | Meaning |
| --- | --- |
| `empty` | The rule covers no runtime node. |
| `effective` | The rule wins on every runtime node it covers. |
| `partial` | The rule wins on some of the runtime nodes it covers, and stronger rules on the others. |
| `overridden` | Stronger rules win on every runtime node it covers. |
| `unmatched` | The rule wins somewhere, but no live node satisfies its selector now. |

`DESCRIBE PLACEMENT` of a node rule shows its stored form and labels, scope, policy, selector,
rank, resolved members, the nodes its selector matches now and its coverage. It then shows every
unit of each member with its owner and satisfaction: `satisfied`, `unplaced`, `violates` or
`waiting` for a requirement, `preferred` or `not preferred` for a preference, and `overridden`
where a stronger rule wins. `DESCRIBE DOMAIN` lists the node rules, then the units violating them
and the units waiting for them.

`SHOW CLUSTER STATUS` adds `violates=<rule>` or `waiting_for=<rule>` to the `[schedule]` row of a
breaking unit, listing the rules comma-separated. The gauges `nervix_placement_violating_units` and
`nervix_placement_waiting_units` count those units per domain, and the leader logs at `info` each
unit that enters or leaves either state. See
[Metrics And Observability](metrics-and-observability.md).

## Ingestor Instance Units

An instance-capable ingestor with `INSTANCES n` has one `Instance(i)` placement per global index.
Every unit has its own primary, replicas and ownership transition. The scheduler balances primary
counts across eligible nodes before comparing ordinary assignment load, so replica slots do not
make one primary appear to be several active consumers. Existing live assignments remain sticky.

An ingestor's `INSTANCES PER TRANSPORT PARTITION` introduces an exact colocation component for each
index: ingestor instance `p` and partition `p` of every relay whose branch it constructs. These
components move independently. Named rules add bonds only through each hop's validated
correspondence. Placement groups store exact `NodeUnitRef` members, including different unit kinds
within one component. Shared group drain, failover and explicit relocation update only those
members; ordinary relay members need no state replica to move with a stateful ingestor. See
[Instancing](ingestors.md#instancing).

## Emitter Instance Units

An emitter declared `INSTANCES n` or `INSTANCES PER PARTITION` has one `Instance(i)` placement per
instance, each with its own primary and ownership transition; an emitter without the clause keeps
its one `Single` unit. Instance `i` of `n` serves the input partitions `p` with `p mod n = i`.

An emitter's `INSTANCES PER PARTITION` introduces an exact colocation component for each index, as
an intake's `INSTANCES PER TRANSPORT PARTITION` does: emitter instance `p` and partition `p` of
every input relay. An instance of `INSTANCES n` has no hard component of its own. It prefers the
node that owns most of the partitions it serves, and a named `REQUIRE COLOCATION` rule on the fiber
hop from an input relay joins it with all of them. An instance beyond the partition count serves no
partition; it is placed like its siblings and runs nothing. See [Emitter
instances](emitters.md#emitter-instances).
