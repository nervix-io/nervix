# Placement Policies

This document specifies the production behavior and NSPL surface for placement policies. Sections
1–8 are normative product requirements.

## 1. Overview and Terminology

Placement policies govern which cluster node executes each runtime node in a domain's execution
graph.

- A **runtime node** is a schedulable graph entity such as an ingestor, processor, or emitter.
- A **cluster node** is a machine participating in the Nervix cluster.
- A **placement member** is a runtime-node reference accepted by a placement rule. A materialized
  relay name is also accepted and denotes that relay's materializing runtime node.
- An **endpoint pair** is one ordered `(source, destination)` member pair selected from a rule's
  `FROM` and `TO` sets.
- A **corridor** is the set of runtime nodes carrying traffic between one connected endpoint pair.
- A **claim pair** is one distinct, unordered pair of runtime nodes within a corridor. Placement
  policies are resolved on claim pairs.
- A **require bond** is a claim pair whose effective policy is `REQUIRE COLOCATION`.
- A **colocation group** is a connected component of require bonds.

A placement policy is one of four levels on a single axis of colocation enforcement:

| Policy | Meaning |
| --- | --- |
| `REQUIRE COLOCATION` | A hard requirement that the affected runtime nodes execute on the same cluster node. |
| `PREFER COLOCATION` | A soft bias toward the same cluster node for new placement decisions. |
| `NEUTRAL` | No placement-policy opinion; existing scheduler heuristics remain active. |
| `SUGGEST SEPARATION` | A soft bias toward different cluster nodes for new placement decisions. It is never an isolation guarantee. |

Policy level does not resolve competing claims. `RANK` establishes precedence, and equal-rank
claims with different policies conflict even when one policy sounds stronger than another.

Policies attach in two ways:

- **Placement rules** are named, domain-owned entities that mark paths between a source set and a
  destination set.
- **The domain default** is a fallback policy for directly connected runtime-node pairs that no
  placement rule claims.

## 2. Placement Rules

```nspl,ignore
CREATE [IF NOT EXISTS] PLACEMENT <placement_name>
  FROM <runtime_node> [, <runtime_node> ...]
  TO <runtime_node> [, <runtime_node> ...]
  <policy>
  [RANK <n>] [;]

<policy> :=
    REQUIRE COLOCATION
  | PREFER COLOCATION
  | NEUTRAL
  | SUGGEST SEPARATION
```

A placement is a domain-owned entity with its own name kind. Its name and members resolve in the
session's active domain in the same way as other domain-owned entities. Execution graphs are
strictly per-domain, so a placement rule cannot span domains.

Every member must exist when the rule is created and must be placement-eligible. The following
user-declared, schedulable runtime-node kinds are eligible:

- ingestor
- reingestor
- emitter
- junction
- deduplicator
- correlator
- reorderer
- window processor
- inferencer
- WASM processor
- generator
- lookup

A materialized relay name is also a legal member. It denotes the runtime node that materializes
the relay, because that runtime node owns the placement-critical state. A non-materialized relay
is not placement-eligible and produces a clear creation error.

An endpoint-source ingestor is not placement-eligible. It executes structurally on every cluster
node and therefore cannot be placement-constrained.

Duplicate members on either side collapse before validation. Each side must remain non-empty. The
same resolved runtime node may appear on both sides; this is meaningful when a reingestor cycle
creates a corridor back to that runtime node.

`RANK <n>` is optional and requires `n >= 1`. Lower numbers are stronger, so rank 1 is the
strongest. Every explicitly ranked rule outranks every unranked rule, regardless of the numeric
rank. All unranked rules share one weakest rule tier, which is stronger only than the domain
default.

### 2.1 What a Rule Covers: Path-Gating

Placement rules are declarative overlays on the current execution graph. Coverage is recomputed
at every graph activation.

After resolving materialized relay members to their materializing runtime nodes, evaluate each
ordered endpoint pair `(s, d)`, where `s` is in `FROM` and `d` is in `TO`, independently. If a
directed path `s ⇝ d` exists over placement-relevant edges, the rule covers that endpoint pair's
corridor:

```text
{s, d} ∪ {x : s ⇝ x and x ⇝ d}
```

Reachability here requires a path containing at least one placement-relevant edge. Consequently,
when `s` and `d` resolve to the same runtime node, a cycle back to that runtime node is required;
zero-edge reflexive reachability does not create a corridor.

The corridor contains every runtime node that carries traffic from `s` to `d`. Because coverage
is defined by reachability, a cycle inside the corridor is captured in full.

Placement-relevant edges comprise all message-delivery and state-delivery relationships:

- normal routes;
- error routes;
- correlation-timeout routes;
- generator feeds; and
- materialized-state dependencies, directed from the materializing runtime node to every runtime
  node declaring `USING MATERIALIZED STATE` on that relay.

Relays are not placed and are transparent to corridor computation. A relay is a local channel
object; a hop crosses the interconnect only when its producer runtime node and consumer runtime
node execute on different cluster nodes.

For each connected endpoint pair, the rule claims its policy and rank on every distinct,
unordered pair of runtime nodes in that corridor. This is a clique over each individual corridor,
not a clique over the union of separate corridors. Runtime nodes from two different corridors are
not claimed against one another unless they also occur together in a corridor. All four policy
levels use this same coverage shape.

An endpoint pair with no connecting path contributes no corridor and no claims. A rule for which
every endpoint pair is unconnected is valid and has an empty effective claim set. It remains
visible through `DESCRIBE PLACEMENT` and `SHOW PLACEMENTS`; zero effect is not an error.

Topology edits automatically grow and shrink corridors and claims at activation.

### 2.2 Effective Policy, Rank Resolution, and Conflicts

For every claim pair, the claim at the strongest rank wins. Equal-rank claims with the same policy
agree and are co-winners. Equal-rank claims with different policies on the same claim pair are a
graph activation error. The error names both rules and a witness pair.

The unranked tier participates in the same conflict rule: different policies from overlapping
unranked rules conflict. Repeated identical claims from one rule or equal-rank agreeing rules do
not conflict.

The domain default applies to every distinct runtime-node pair joined directly by a
placement-relevant edge and claimed by no rule at any rank. The default is per edge rather than
per path. For `REQUIRE COLOCATION`, transitive closure makes that equivalent to a per-path result;
for soft policies, per-edge application preserves a per-hop bias.

Every effective `REQUIRE COLOCATION` claim becomes a require bond. Require bonds are transitive:
their connected components form colocation groups, and every runtime node in a colocation group
executes on one cluster node. Placement policies add no labels, capacity constraint, or named-host
pin, so they cannot by themselves make an otherwise schedulable graph unschedulable. If any live,
uncordoned, schedulable cluster node is available, it can host an entire colocation group; no
placement-policy-specific pending or unplaced state is needed.

The useful mental model for carve-outs is **glue and cuts**. Effective require bonds add glue
between claim pairs. A stronger-ranked non-require claim replaces a weaker require bond:
`NEUTRAL` or `SUGGEST SEPARATION` cuts it, while `PREFER COLOCATION` downgrades it to a soft
affinity. A runtime node remains in a colocation group while any chain of uncut require bonds
connects it to that group. Detaching it requires carve-out coverage that cuts every such bond;
merely naming the runtime node does not bypass path-gating or transitivity.

Placement introspection exposes the effective require bonds and every owning co-winner so that
questions such as "why is `x` still colocated with `y`?" are mechanically answerable.

## 3. Domain Default Policy

Placement extends each existing valid domain-creation form:

```nspl,ignore
CREATE [IF NOT EXISTS] PACED DOMAIN <domain_name>
  WITH PERIOD <duration> SKEW <duration>
  [PLACEMENT <policy>] [;]

CREATE [IF NOT EXISTS] UNPACED DOMAIN <domain_name>
  [PLACEMENT <policy>] [;]

CREATE [IF NOT EXISTS] DOMAIN <domain_name>
  [PLACEMENT <policy>] [;]
```

As before, `CREATE DOMAIN` is the short spelling for `CREATE UNPACED DOMAIN`. The placement clause
does not change the timing requirements of paced or unpaced domains.

The default can be changed on the active domain:

```nspl,ignore
ALTER DOMAIN SET PLACEMENT <policy> [;]
```

`ALTER DOMAIN` is the first statement in that family. It is deliberately nameless and operates on
the session's active domain, consistent with `START`, `STOP`, and `DESCRIBE DOMAIN`.

Omitting `PLACEMENT` at creation means `NEUTRAL`, preserving current scheduler behavior.

A default of `REQUIRE COLOCATION` puts each connected component of the graph formed by direct
placement-relevant edges on one cluster node, subject to stronger explicit rule claims that cut
require bonds. This is whole-pipeline-local mode. A default of `SUGGEST SEPARATION` supplies a
per-hop spreading bias.

Changing the default on a running domain is a normal graph activation. It may relocate runtime
nodes when it introduces newly effective `REQUIRE COLOCATION` claims.

## 4. Enforcement Semantics

### Activation

Coverage, effective claim pairs, conflicts, and colocation groups are recomputed transactionally
with every graph activation. A conflict rejects the candidate activation without installing any
part of it.

Newly effective `REQUIRE COLOCATION` claims consolidate each affected colocation group onto one
cluster node using the standard handoff machinery. The statement response reports planned
relocations. A prior sticky assignment may be preserved only if it still satisfies every
effective require bond; activation corrects a violating assignment.

Soft policies never cause migration by themselves. `PREFER COLOCATION`, `SUGGEST SEPARATION`, and
`NEUTRAL` influence only new placement decisions, including new runtime nodes, failover targets,
and drain targets. Existing assignments are not moved merely to chase a preference. Removing a
require bond or splitting a colocation group likewise does not require already colocated runtime
nodes to spread.

### Failover and Drain

Failover and drain treat a colocation group as the atomic relocation unit. Failover never creates
an execution placement that violates an effective require bond: the entire group moves to the
chosen target together. Drain relocates one group at a time.

### Scheduler Behavior

`REQUIRE COLOCATION` binds every scheduler, including the production sticky scheduler and the
deterministic random test scheduler. The production sticky scheduler applies explicit
`PREFER COLOCATION` and `SUGGEST SEPARATION` claims ahead of its built-in upstream-locality
heuristic. The random test scheduler may ignore soft policies. `NEUTRAL` leaves built-in
heuristics active; it means "no policy opinion," not "randomize."

### Execution Scope

Claims bind primary execution placements. Replicas are warm state rather than executors; their
placement should be biased toward cluster nodes compatible with the group so that failover is
cheap, but that replica bias is non-normative.

Branches have no placement dimension. All concrete branches of a runtime node execute within its
single execution assignment, while their runtime state remains branch-local. A placement rule
binds every scheduler-placed execution of an eligible member.

Cordon retains its existing semantics. Cordoned cluster nodes are never placement targets for new
decisions, including group relocations.

## 5. Lifecycle and Introspection Statements

```nspl,ignore
ALTER PLACEMENT <ref:placement>
  <operation> [, <operation> ...] [;]

<operation> :=
    SET POLICY <policy>
  | SET RANK <n>
  | DROP RANK
  | SET FROM <runtime_node> [, <runtime_node> ...]
      TO <runtime_node> [, <runtime_node> ...]
  | RENAME TO <placement_name>

DROP PLACEMENT <ref:placement> [;]
SHOW CREATE PLACEMENT <ref:placement> [;]
SHOW PLACEMENTS [;]
DESCRIBE PLACEMENT <ref:placement> [;]
```

`ALTER PLACEMENT` follows the uniform ALTER shape. Comma-separated operations execute in written
order, and each operation observes the result of the operations before it.

Dropping any entity referenced as a placement member is blocked until the placement is altered or
dropped. The error names every placement that pins the target. A mutation that would make a
referenced member non-placeable is blocked in the same way. Examples include changing an ingestor
source to an endpoint source and removing materialized state from a referenced relay. These pins
apply even when the placement currently has no connected endpoint pairs.

`DESCRIBE PLACEMENT` shows:

- the stored `FROM`/`TO` form, policy, and rank or unranked status;
- for each ordered endpoint pair, whether it is connected;
- the covered corridor for each connected endpoint pair, with one witness path through every
  captured non-member runtime node;
- for every claim pair, the effective winning claim or co-winners, or the stronger rule or rules
  that override this placement;
- the effective require bonds and their owning claims; and
- for a placement with effective require claims, every colocation group intersecting those claims
  and the cluster node currently hosting each group.

For `s = d`, a witness is a cycle. For another captured runtime node `x`, its witness demonstrates
`s ⇝ x ⇝ d`.

`SHOW PLACEMENTS` follows the `SHOW UDFS` precedent. It lists each placement's name, policy, rank
or unranked status, and effective coverage status. The status summarizes endpoint-pair
connectivity and whether none, some, or all resulting claims remain effective after rank
resolution, so an unconnected rule and a fully overridden rule are distinguishable.

`DESCRIBE DOMAIN` gains a placement section containing:

- the domain default policy;
- the placement-rule count; and
- every effective colocation group, its member runtime nodes, its effective require bonds and
  owning claims, and its current host cluster node.

## 6. Errors and Diagnostics

Placement creation reports semantic errors for:

- an unknown member;
- a non-placeable member, including an endpoint-source ingestor, a relay without materialized
  state, or a non-schedulable entity kind;
- an empty `FROM` or `TO` side after duplicate collapse; and
- `RANK 0`.

Graph activation fails when different policies make equal-rank claims on the same claim pair.
The diagnostic names the owning domain, both placement rules, and the two runtime nodes that form
a witness pair. It contains no payload values. Equal-rank unranked claims are subject to the same
rule.

A blocked drop or alteration names every placement pinning the target.

A placement with no connected endpoint pairs is valid. `DESCRIBE PLACEMENT` and
`SHOW PLACEMENTS` expose its zero-effect status rather than reporting an error.

## 7. Non-Goals

- There is no hard separation. `SUGGEST SEPARATION` is advisory, there is deliberately no
  `REQUIRE SEPARATION`, and placement policies provide no isolation guarantee.
- There is no connectivity-independent grouping such as a possible future `AMONG a, b, c` clique
  for unconnected workloads. Rules act only through paths that exist in the execution graph.
- There are no cluster-node labels, zones, capacity calculations, or pins to named cluster nodes.
- There is no per-branch placement and no replica-count control.
- Placement policy is not a load-balancing control. Existing scheduler load heuristics remain in
  effect.

## 8. Examples

These examples assume that the intended domain is active in the session.

Pin a latency-critical machine-learning corridor onto one cluster node. Here `enrich` is the name
of a materialized relay, so the corridor includes its materializing runtime node as the state
owner:

```nspl,ignore
CREATE PLACEMENT ml_corridor
  FROM features_ingest, enrich
  TO scorer
  REQUIRE COLOCATION
  RANK 1;
```

Suggest spreading a heavy archive corridor across cluster nodes:

```nspl,ignore
CREATE PLACEMENT spread_archive
  FROM main_ingest
  TO archive_emitter
  SUGGEST SEPARATION;
```

Make each connected pipeline component local to one cluster node while leaving an archive corridor
to ordinary scheduler heuristics through a stronger carve-out:

```nspl,ignore
ALTER DOMAIN SET PLACEMENT REQUIRE COLOCATION;

CREATE PLACEMENT archiver_free
  FROM archiver
  TO cold_sink
  NEUTRAL
  RANK 1;
```

Change precedence and remove an obsolete placement:

```nspl,ignore
ALTER PLACEMENT ml_corridor SET RANK 2;
DROP PLACEMENT spread_archive;
```
