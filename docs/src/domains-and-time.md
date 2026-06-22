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

## Ingestion Timestamps

Every ingested record receives internal ingestion metadata, including mandatory low and high watermarks with nanosecond precision.

Timestamp sources:

- `TIMESTAMP NOW`
- `TIMESTAMP AT <field>`

In paced domains, ingestors must declare a timestamp source explicitly. In unpaced domains, timestamp metadata is still recorded, but it is not used to gate admission.

Window processors also use this metadata. Duration windows evaluate input event time from the record low watermark. Emitted aggregate records receive a low watermark equal to the minimum input low watermark in the emitted window and a high watermark equal to the current domain time at emission.
