# Placement Policies

Placement policies control where a domain's schedulable work executes. Use them to keep a
latency-sensitive processing corridor on one machine, prefer locality without forcing it, or bias
expensive stages across machines.

Nervix distinguishes two kinds of nodes throughout this chapter:

- A **runtime node** is a schedulable graph entity such as a relay, ingestor, processor, lookup,
  generator, or emitter.
- A **cluster node** is a machine participating in the Nervix cluster.

A named placement rule marks the directed paths between existing runtime nodes. For example, this
rule keeps `feature_normalizer`, `risk_scorer`, and every runtime node on a path between them on
one cluster node:

```nspl
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
| `REQUIRE COLOCATION` | Every runtime node within each captured corridor executes on the same cluster node. | A newly effective requirement can relocate runtime nodes. |
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

## Setting The Domain Default

Every domain has a fallback placement policy. Declare it when creating the domain:

```nspl,ignore
CREATE [IF NOT EXISTS] UNPACED DOMAIN <domain_name>
  [PLACEMENT <policy>];

CREATE [IF NOT EXISTS] PACED DOMAIN <domain_name>
  WITH PERIOD <duration> SKEW <duration>
  [PLACEMENT <policy>];
```

`CREATE DOMAIN` remains the short spelling for `CREATE UNPACED DOMAIN` and accepts the same
placement clause. Omitting `PLACEMENT` selects `NEUTRAL`.

```nspl
CREATE UNPACED DOMAIN realtime
  PLACEMENT PREFER COLOCATION;
```

The default applies to directly connected runtime nodes when no named placement rule claims that
relationship. Relays are runtime nodes, so producer-to-relay and relay-to-consumer are separate
hops. The default is deliberately per hop:

- `REQUIRE COLOCATION` places each connected graph component on one cluster node unless a stronger
  named rule carves out part of it.
- `PREFER COLOCATION` adds a locality preference to each uncovered hop.
- `SUGGEST SEPARATION` adds a spreading preference to each uncovered hop.
- `NEUTRAL` leaves each uncovered hop to ordinary scheduler behavior.

Change the default for the session's active domain with the nameless `ALTER DOMAIN` statement:

```nspl
ALTER DOMAIN SET PLACEMENT REQUIRE COLOCATION;
```

Changing a running domain's default activates a new schedule. A newly effective
`REQUIRE COLOCATION` default can relocate runtime nodes; changing only a soft default does not
migrate existing assignments.

## Defining A Named Corridor

Create a named rule after every referenced member exists in the active domain. Placements are
domain-owned and cannot span domains:

```nspl,ignore
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

Endpoint-source and Syslog ingestors execute on every cluster node and therefore cannot be
constrained by a placement rule. Member names are unqualified; a name shared by more than one
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
across the runtime nodes captured by that corridor, not only to its two named endpoints.

Placement follows the relationships that can deliver messages or state:

- normal output routes;
- message-error routes;
- correlation-timeout routes;
- generator feeds; and
- materialized-state dependencies, directed from the relay to each reader
  that declares `USING MATERIALIZED STATE`.

Relays are corridor members. A producer-to-relay or relay-to-consumer relationship is one placement
hop, so a domain default applies independently on both sides and a named path captures relay nodes
between its endpoints.

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
3. the unranked tier; and
4. the domain default.

Every explicitly ranked rule outranks every unranked rule, regardless of the numeric value. All
unranked rules share one tier. At the strongest rank affecting a runtime-node relationship,
equal-rank rules with the same policy agree. Different policies at that strongest rank conflict,
and Nervix rejects the candidate graph activation with the rule names and an affected runtime-node
pair. Weaker overridden rules do not conflict with one another.

Effective `REQUIRE COLOCATION` relationships are transitive. If one requirement joins `a` to `b`
and another joins `b` to `c`, all three form one colocation group. A stronger soft or neutral rule
can remove specific hard relationships, but a runtime node remains in the group while any other
effective `REQUIRE COLOCATION` chain still connects it.

For example, first declare a scheduler-managed archive corridor:

```nspl
CREATE PLACEMENT archive_scheduler_managed
  FROM archiver
  TO cold_sink
  NEUTRAL
  RANK 1;
```

Then make hard colocation the domain fallback:

```nspl
ALTER DOMAIN SET PLACEMENT REQUIRE COLOCATION;
```

The named `NEUTRAL` rule overrides the default within its connected corridor. It does not guarantee
that the archive stages run on different cluster nodes; it returns those relationships to ordinary
scheduler heuristics. Use `SUGGEST SEPARATION` instead when the carve-out should also express a
soft spreading preference.

## Runtime Behavior

Placement coverage, rank resolution, and colocation groups are recalculated as part of graph
activation. A conflicting candidate is rejected before it replaces the active graph.

Every single-owner ingestor keeps its existing primary and replica assignment across schedule
recomputation while all assigned cluster nodes are live. This includes outbound WebSocket-client
ingestors. Model changes, cluster-node joins and uncordons, and soft placement changes therefore do
not migrate those ingestors. Endpoint-source and Syslog ingestors are the only ingestors that
instead follow live cluster membership, because their server listeners execute on every cluster
node.

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
outside its unit untouched.

Drain, colocation consolidation, and explicit relocation use a planned handoff and restart only
runtime nodes whose primary owner changed. A runtime node that keeps its assignment continues on every cluster node,
including the former and new owners of a moved neighbor: it keeps buffered work, retained
`REQUIRED WAIT` messages, branch-local state, and external source sessions. Its neighbors re-point
at the committed revision. Draining a host moves hard groups and independent nodes one at a time in
canonical kind/name order. Failover uses immediate termination because the former owner is already
unavailable.

Placement constrains primary owners. It does not control replica count or replica placement. An
ordinary relay has no replicas; a materialized relay's replicas contain only materialized state.
Branches also have no separate placement dimension: every concrete relay branch is owned by its
relay owner, and every processor branch executes within that processor's assignment.

## Relocating Runtime Nodes

Placement rules constrain where the scheduler puts work. `RELOCATE` moves chosen work onto a
cluster node you name, and `DESCRIBE RELOCATION` shows the plan a statement would execute without
executing it:

```nspl,ignore
RELOCATE <selection>
  ONTO NODE <node_id>
  FOLLOW PREFERENCES | IGNORE PREFERENCES
  [FOR <kind> <name> FOLLOW PREFERENCES | IGNORE PREFERENCES ...];

DESCRIBE RELOCATION <selection>
  ONTO NODE <node_id>
  FOLLOW PREFERENCES | IGNORE PREFERENCES
  [FOR <kind> <name> FOLLOW PREFERENCES | IGNORE PREFERENCES ...];
```

`DESCRIBE RELOCATION` accepts exactly the clauses of `RELOCATE` after the verb, so a plan can be
inspected and then executed by changing one word.

### Selecting The Subgraph

A selection is either an explicit list or a directed corridor, using the same path-gated coverage
named placement rules use:

```nspl
RELOCATE JUNCTION risk_scorer ONTO NODE node-2 FOLLOW PREFERENCES;
```

```nspl
RELOCATE FROM JUNCTION feature_normalizer TO JUNCTION risk_scorer
  ONTO NODE node-2
  FOLLOW PREFERENCES
  FOR DEDUPLICATOR dedup_txns IGNORE PREFERENCES;
```

Every member is kind-qualified, because a name is unique only within its kind. The kind uses the
same spelling as `SHOW CREATE <kind> <name>`:

| `<kind>` | Runtime node |
| --- | --- |
| `INGESTOR` | An ingestor other than an endpoint or Syslog source. |
| `REINGESTOR`, `GENERATOR` | The named reingestor or generator. |
| `JUNCTION`, `DEDUPLICATOR`, `CORRELATOR`, `REORDERER`, `WINDOW PROCESSOR` | The named processor. |
| `INFERENCER`, `WASM PROCESSOR` | The named processor. |
| `EMITTER` | The named emitter. |
| `HASH MAP` | The named lookup; reported as kind `lookup` in schedule output. |
| `RELAY` | The named relay, materialized or not. |

Any other kind keyword is a parse error. Names resolve in the session's active domain, duplicate
members collapse, and an ingestor with several `INSTANCES` is one runtime node whose instances move
together. A corridor whose every endpoint pair is disconnected is rejected; a partially connected
corridor is valid and the plan reports each pair's connectivity. The destination must be a Raft
voter that is live and not cordoned.

### Building The Unit

The set of hard groups a statement moves is its unit. `REQUIRE COLOCATION` is not a preference: a
selected runtime node always moves with its whole hard group. Soft preferences are shaped by the
strategy:

- `FOLLOW PREFERENCES` moves each `PREFER COLOCATION` partner of the group along and holds each
  `SUGGEST SEPARATION` partner out of the unit.
- `IGNORE PREFERENCES` moves the group and lets its preferences shape nothing: its partners are
  neither captured nor held back by it.

The strategy after `ONTO NODE` is mandatory and applies to every hard group. A `FOR` clause names
one runtime node and sets the strategy of that runtime node's whole hard group, because a group
cannot be moved in pieces and its preferences are one set. Two `FOR` clauses that disagree within
one hard group are rejected, and a `FOR` clause naming a runtime node outside the plan is rejected.

Capture is transitive and follows the statement default. A captured group inherits that default, so
`FOLLOW PREFERENCES` closes the unit over `PREFER COLOCATION` relationships until a
`FOR ... IGNORE PREFERENCES` cuts the chain, while `IGNORE PREFERENCES` with a
`FOR ... FOLLOW PREFERENCES` opens capture at one group only. Under a `PREFER COLOCATION` domain
default, a `FOLLOW PREFERENCES` relocation therefore moves the whole connected component reachable
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
unit whose two owners are the same, naming the winning rule or `domain default`.

`kind` and `name` use the schedule spelling shown by `SHOW CLUSTER STATUS`, so a `HASH MAP` member
appears as `kind=lookup` and a `RELAY` member as `kind=relay`.

### Executing The Move

A relocation in a running domain that moves at least one runtime node is a planned ownership change
and executes as one unit, one hold, one commit: the hold gates exactly the relays the plan lists,
every moved runtime node drains its admitted work, one schedule write assigns the whole unit to the
destination, and the gates open once the destination has activated the revision. The unit is
atomic, because the plan is what you inspected and approved; a unit containing a runtime node that
cannot drain is excluded with a narrower selection or `IGNORE PREFERENCES`, or the stall is
resolved and the statement retried. See
[Control Plane](control-plane.md#planned-ownership-handoffs-and-failover) for the shared hold,
drain, and failure behavior.

The reported quiesce level is `ENTITY_PAUSE` when the plan moves at least one runtime node in a
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

```nspl,ignore
ALTER PLACEMENT <placement>
    SET POLICY <policy>
  | SET RANK <positive_integer>
  | DROP RANK
  | SET FROM <runtime_node> [, ...] TO <runtime_node> [, ...]
  | RENAME TO <placement_name>
  [, <operation> ...];

DROP PLACEMENT <placement>;
```

Operations execute in written order, and the complete statement is applied atomically. `SET FROM
... TO ...` replaces both member lists. This makes it possible to change policy, precedence,
membership, and name in one statement:

```nspl
ALTER PLACEMENT scoring_local
  SET POLICY PREFER COLOCATION,
  SET RANK 2,
  RENAME TO scoring_preferred;
```

A placement pins the entities named in its `FROM` and `TO` lists even when its current coverage is
empty. Dropping a referenced runtime node is blocked until every pinning placement is altered or
dropped. A change that would make a member ineligible, such as changing an ingestor to an endpoint
or Syslog source, is blocked in the same way. Adding or dropping a relay's materialized state does
not change its placement identity.

When a topology edit and its placement update depend on one another, put the complete set of model
changes in one explicit transaction and order creation before reference. Nervix validates the
resulting candidate graph before publishing it. See [Control Plane](control-plane.md) for
transaction and quiesce behavior.

## Inspecting Placement

Use the placement commands in the active domain:

```nspl
SHOW CREATE PLACEMENT scoring_local;
```

```nspl
SHOW PLACEMENTS;
```

```nspl
DESCRIBE PLACEMENT scoring_local;
```

```nspl
DESCRIBE DOMAIN;
```

`SHOW CREATE PLACEMENT` renders the normalized stored rule as canonical NSPL and omits the
creation-time `IF NOT EXISTS` modifier. `SHOW PLACEMENTS` lists each rule's policy, rank, and one
of these coverage states:

| Coverage | Meaning |
| --- | --- |
| `empty` | No `FROM`/`TO` endpoint pair currently has a directed path. |
| `effective` | All endpoint pairs are connected and all of the rule's resulting relationships win rank resolution. |
| `partial` | Only some endpoint pairs are connected, or only some resulting relationships remain effective. |
| `overridden` | Connected coverage exists, but stronger rules win every resulting relationship. |

`DESCRIBE PLACEMENT` provides the stored form, policy, rank, resolved members, connectivity and
covered runtime nodes for each endpoint pair, witness paths for captured intermediates, and the
effective policy and winning or overriding rule for each affected relationship. For effective
hard requirements it also reports the colocation group and its current host cluster node.

`DESCRIBE DOMAIN` reports the domain default, named-rule count, and every effective colocation
group. Each group includes its runtime-node members, host cluster node, and the effective hard
relationships holding it together. Use those relationships to diagnose why a runtime node remains
in a larger group after a carve-out.

`SHOW CLUSTER STATUS` provides the complete scheduled-owner view when you need to compare placement
with other runtime nodes outside hard colocation groups.

## Troubleshooting

| Symptom | What To Check |
| --- | --- |
| A rule shows `coverage=empty`. | Confirm the route direction and that a directed message or state path exists from a `FROM` member to a `TO` member. Disconnected rules are valid. |
| A rule is `partial` or `overridden`. | Run `DESCRIBE PLACEMENT` and inspect the winning rule for each affected relationship. |
| Activation reports a placement conflict. | Two overlapping rules have different policies at the strongest rank for an affected relationship. Change one policy or give one rule a stronger rank. A topology edit can expose a previously latent overlap. |
| `SUGGEST SEPARATION` still leaves stages together. | The policy is advisory and never migrates existing assignments by itself. Re-evaluate it on a new placement decision; do not use it as an isolation control. |
| A soft-policy change did not move anything. | This is expected. Use `REQUIRE COLOCATION` only when movement is justified by a hard locality requirement. |
| A runtime node remains in a hard group after a carve-out. | Inspect the group's hard relationships in `DESCRIBE DOMAIN`; another uncut requirement still connects it to the group. |
| Dropping or reshaping a member is blocked. | Alter or drop every placement named by the diagnostic before changing the member. |

Creation also rejects an unknown or ambiguous member, a non-schedulable entity, a cluster-wide
server-listener ingestor, an empty side, and `RANK 0`.

## Common Patterns

Keep a latency-critical corridor on one cluster node:

```nspl
CREATE PLACEMENT fraud_scoring_local
  FROM normalize_transaction
  TO fraud_scorer
  REQUIRE COLOCATION
  RANK 1;
```

Keep a materialized-state owner with a reader. Here `customer_state` is a materialized relay and
`fraud_scorer` declares `USING MATERIALIZED STATE customer_state`:

```nspl
CREATE PLACEMENT state_read_local
  FROM customer_state
  TO fraud_scorer
  REQUIRE COLOCATION
  RANK 1;
```

Bias a heavy archive corridor across cluster nodes without guaranteeing isolation:

```nspl
CREATE PLACEMENT spread_archive
  FROM archive_transform
  TO archive_emitter
  SUGGEST SEPARATION;
```

Prefer locality for a corridor while retaining scheduler flexibility:

```nspl
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
also does not relocate endpoint or Syslog ingestors, an individual replica, an individual branch, or
a unit spanning domains, and it never applies a unit in part.

Use `SUGGEST SEPARATION` as a performance hint only. Security, tenant isolation, and failure-domain
requirements need controls outside placement policies.
