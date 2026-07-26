# Domains And Time

Every runtime graph in Nervix runs inside a domain.

Nervix currently supports:

- `CREATE PACED DOMAIN <id> WITH PERIOD <duration> SKEW <duration>;`
- `CREATE UNPACED DOMAIN <id>;`
- `CREATE DOMAIN <id>;`

`CREATE DOMAIN <id>` is the short spelling for `CREATE UNPACED DOMAIN <id>`.

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

## Automatic Schema-Change Quiescing

`ALTER SCHEMA` and `ALTER WIRE ... SCHEMA` use an internal paused lifecycle state when their domain
is running. There is no NSPL `PAUSE` or `RESUME` statement.

Before changing the live graph, the leader validates the complete candidate graph without writing
it. Nervix then stops domain ingestion and generators on every node, keeps the processing graph and
domain clock alive, force-flushes processor and emitter output, and waits for ingestors, generators,
ACK roots, and emitter buffers to drain. The wait is condition-based and bounded to 60 seconds by
default.

After a successful drain, Nervix atomically installs the model batch, replaces the schedule while
ingestion is still withheld, and resumes the domain on the new graph. A timeout or cutover failure
leaves the mutation unapplied, restores the old graph, and automatically resumes it with a clear
outstanding-work error. A schema ALTER on a stopped domain only validates and persists the new
schedule. Pure `CREATE` and `DROP` batches keep the immediate schedule-rebuild behavior.

Pause is not a restart: domain clock state, start version, and broker offsets are preserved across
the quiesce cycle.

## Ingestion Timestamps

Every ingested record receives internal ingestion metadata, including mandatory low and high watermarks with nanosecond precision.

Timestamp sources:

- `TIMESTAMP NOW`
- `TIMESTAMP AT <field>`

In paced domains, ingestors must declare a timestamp source explicitly. In unpaced domains, timestamp metadata is still recorded, but it is not used to gate admission.

Window processors also use this metadata. Duration windows evaluate input event time from the record low watermark. Emitted aggregate records receive a low watermark equal to the minimum input low watermark in the emitted window and a high watermark equal to the current domain time at emission.
