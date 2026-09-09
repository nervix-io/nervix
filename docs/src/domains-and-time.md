# Domains And Time

Every runtime graph in Nervix runs inside a domain.

Nervix currently supports:

- `CREATE PACED DOMAIN <id> WITH PERIOD <duration> SKEW <duration> [PLACEMENT <policy>];`
- `CREATE UNPACED DOMAIN <id> [PLACEMENT <policy>];`
- `CREATE DOMAIN <id> [PLACEMENT <policy>];`

`CREATE DOMAIN <id>` is the short spelling for `CREATE UNPACED DOMAIN <id>`.

Domain creation is never transaction content. A transaction is bound to one already-existing
domain, so `CREATE DOMAIN` runs on its own before `BEGIN`; queueing it is rejected. See
[Replicated NSPL Transactions](control-plane.md#replicated-nspl-transactions).

## Paced Domains

Paced domains maintain a domain clock.

`PERIOD` must be positive and no larger than `18446744073709551615ns`. `SKEW` may be zero but must
fit in that same 64-bit nanosecond duration range. Invalid durations are rejected before the domain
is stored.

While the domain is running:

- Nervix produces domain ticks
- paced ingestors only admit records whose effective timestamp falls inside the tick window
- `SKEW` defines the allowed admission window around each tick

Paced time is also important for expiration:

- branch TTL uses domain logical time in paced domains, including the relay owner's cluster-wide
  branch-presence decision
- materialized-state cleanup follows the same logical-time rule

Deterministic Roto UDFs preserve reproducibility when paced input is replayed at an accelerated
time rate. See the [deterministic-by-default and `VOLATILE` contract](udfs.md#nulls-errors-and-volatility),
including common-subexpression reuse and the rule that user code is never constant-folded.

## Unpaced Domains

Unpaced domains do not produce ticks.

Ingestors in an unpaced domain admit records as they arrive, and branch TTL uses wall clock time.

## Placement Default

Every domain has a fallback placement policy. It applies to directly connected runtime-node pairs
that no named placement rule claims. Because relays are scheduled runtime nodes, a
producer-to-relay edge and a relay-to-consumer edge each receive the default independently. The
clause is optional on every domain-creation form, and omission means `NEUTRAL` so ordinary
scheduler heuristics remain active:

```nspl
CREATE PACED DOMAIN production
  WITH PERIOD 1s SKEW 100ms
  PLACEMENT REQUIRE COLOCATION;

CREATE UNPACED DOMAIN archive
  PLACEMENT SUGGEST SEPARATION;
```

Change the default for the session's active domain with the nameless domain alteration:

```nspl
ALTER DOMAIN SET PLACEMENT PREFER COLOCATION;
```

A newly effective `REQUIRE COLOCATION` default can relocate runtime nodes during graph activation.
Soft policies influence only future placement decisions and do not migrate existing assignments.
See [Placement Policies](placement.md) for named overlays, path-gating, rank resolution, and
enforcement.

## Start And Stop

Domain lifecycle commands apply to the active domain:

- `START;`
- `START AT NOW [TIME RATE <float>];`
- `START AT <rfc3339_timestamp> [TIME RATE <float>];`
- `STOP;`

Important runtime consequences:

- `START;` resumes from persisted domain-owned runtime state when a source supports it
- `START AT NOW` reinitializes paced time and domain-owned source offsets from current wall clock
- `STOP` preserves persisted runtime state
- `START` clears materialized relay state for the active domain before new execution proceeds

The lifecycle state, active paced-clock anchor, and one clock authority are replicated. The
authority identifies a concrete incarnation of a named cluster node and carries a revision that
advances whenever ownership changes or is revoked. After leader failover, reconciliation uses that
committed state; a completed transactional `START` therefore remains effective without
re-executing its commit step, and leadership transfer alone does not establish a new anchor or
authority.

Every live node installs that committed mapping before it builds executable work for the domain.
The installed capability is bound to the domain name and `START` generation. A joining node uses
the existing logical origin, physical UTC anchor, and rate; it does not establish a new anchor from
its join time. An unpaced domain receives actual UTC through the same domain-bound capability. A
missing domain, stopped clock, uninstalled paced mapping, or task bound to an earlier generation is
a lifecycle error and never selects wall time as a paced fallback. Reads on one node do not move
backward within a generation.

The authority begins producing only after every live node reports that it installed the consensus
runtime revision containing the mapping and fence. Join, restart, owner loss, and membership change
can select a new authority without changing the clock mapping. A progress report is accepted only
when its `START` generation, authority revision, node incarnation, and authenticated peer match the
committed authority. Duplicate, delayed, reordered, or superseded progress is ignored and cannot
create a domain. `STOP` revokes the authority in the same replicated lifecycle transition; a later
`START` commits a new generation and mapping. Automatic ALTER quiescing keeps the authority and
clock running.

An explicit `START AT` timestamp must fit exactly in signed Unix nanoseconds. The inclusive range
is `1677-09-21T00:12:43.145224192Z` through `2262-04-11T23:47:16.854775807Z`; valid RFC 3339 values
immediately outside those endpoints are rejected by the command. Nervix converts accepted text to
a timestamp at the language boundary and carries that timestamp through persistence and runtime
state without reparsing it.

`TIME RATE` accepts every positive finite `f64`, including scientific notation such as `5e-324`
and `1.7976931348623157e308`. Zero, negative values, infinities, and NaN are rejected. The committed
start mapping remains the sole clock anchor for that run. Tick delivery records progress but never
re-anchors logical time, so a delayed tick cannot move time backwards.

Clock projection rounds fractional logical nanoseconds down. Converting a logical target to a
physical wait rounds fractional physical nanoseconds up, which prevents a deadline from firing
early. Timestamp, duration, boundary, and tick-id overflow are errors. If tick production wakes
after several periods have passed, it emits one tick at the latest due logical boundary and next
targets the first future boundary.

Domain cadence and data-lifecycle policy create logical deadlines from a bound domain clock.
Connection, retry, cancellation, drain, and other operational policy create physical deadlines on
the process monotonic clock. The two deadline kinds cannot be interchanged. A logical wait
revalidates its domain and generation after every wake and returns both the logical instant that
became due and a fresh execution-time snapshot. Cancellation is a separate typed outcome.

## Automatic Model-Alteration Quiescing

Nervix derives a quiesce level from the complete validated model diff. `ALTER SCHEMA`,
`ALTER WIRE ... SCHEMA`, relay schema changes, and relay branching changes use an internal paused
lifecycle state when their domain is running. Dynamic changes such as relay capacity do not pause
the domain. There is no NSPL `PAUSE` or `RESUME` statement.

Before changing the live graph, the leader validates the complete candidate graph without writing
it while holding the domain's exclusive ALTER lock. For an entity-pause alteration, Nervix gates
the affected relays or stops the affected ingestor instances on every live node, then waits only
for their rings and target-node work to drain. Unrelated graph paths continue to run. For a
domain-pause alteration, Nervix instead stops domain ingestion and generators on every node, keeps
the processing graph and domain clock alive, force-flushes processor and emitter output, and waits
for ingestors, generators, ACK roots, and emitter buffers to drain. Both waits are condition-based
and bounded to 60 seconds by default.

After a successful drain, Nervix atomically installs the model batch, replaces the schedule while
ingestion is still withheld, and resumes the domain on the new graph. A timeout or cutover failure
leaves the mutation unapplied, restores the old graph, and automatically resumes it with a clear
outstanding-work error. An ALTER on a stopped domain only validates and persists the new schedule.
Pure `CREATE` and `DROP` batches keep the immediate schedule-rebuild behavior. An all-no-op batch
writes and publishes nothing and never pauses.

Pause is not a restart: domain clock state, start version, broker offsets, branch identity, and
eligible handoff residue are preserved across the quiesce cycle.

## Ingestion Timestamps

Every ingested record receives internal ingestion metadata, including mandatory low and high watermarks with nanosecond precision.

Timestamp sources:

- `TIMESTAMP NOW`
- `TIMESTAMP AT <field>`

In paced domains, ingestors must declare a timestamp source explicitly. In unpaced domains, timestamp metadata is still recorded, but it is not used to gate admission.

Window processors also use this metadata. Duration windows evaluate input event time from the record low watermark. Emitted aggregate records receive a low watermark equal to the minimum input low watermark in the emitted window and a high watermark equal to the current domain time at emission.
