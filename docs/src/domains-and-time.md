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

While the domain is running:

- Nervix produces domain ticks
- paced ingestors only admit records whose effective timestamp falls inside the tick window
- `SKEW` defines the allowed admission window around each tick

Paced time is also important for expiration:

- branch TTL uses domain logical time in paced domains
- materialized-state cleanup follows the same logical-time rule

Deterministic Roto UDFs preserve reproducibility when paced input is replayed at an accelerated
time rate. See the [deterministic-by-default and `VOLATILE` contract](udfs.md#nulls-errors-and-volatility),
including common-subexpression reuse and the rule that user code is never constant-folded.

## Unpaced Domains

Unpaced domains do not produce ticks.

Ingestors in an unpaced domain admit records as they arrive, and branch TTL uses wall clock time.

## Placement Default

Every domain has a fallback placement policy. It applies to directly connected runtime-node pairs
that no named placement rule claims. The clause is optional on every domain-creation form, and
omission means `NEUTRAL` so ordinary scheduler heuristics remain active:

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

The lifecycle state and active paced-clock anchor are replicated with the domain. After leader
failover, reconciliation reconstructs the running clock from that state; a completed transactional
`START` therefore remains effective without re-executing its commit step.

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
