# Paced Domains

The `quickstart` domain is unpaced: records are admitted as they arrive and every cadence follows
wall clock time. A **paced** domain instead maintains a domain clock — records are admitted by
their event timestamps, and every time-driven behavior (generator cadence, branch TTL, duration
windows) follows the logical clock. Start the clock at a historical point with an accelerated
`TIME RATE` and the same graph replays the past faster than real time. See
[Paced Domains](domains-and-time.md#paced-domains).

This page builds a small replay domain next to `quickstart`.

## A Second Domain

```nspl
CREATE PACED DOMAIN quickstart_replay WITH PERIOD 200ms SKEW 100000h;

USE quickstart_replay;
```

- `PERIOD` is the logical spacing between domain ticks.
- `SKEW` is the admission window around each tick. It is deliberately huge here: the tutorial
  posts records with fixed historical timestamps, and they must fall inside a window anchored to
  the clock. Live feeds whose timestamps track the clock use small skews like `1s`.

## A Timestamped Stream

Paced ingestion needs an event time, so this schema finally carries a `DATETIME` — which is also
the promised demonstration of the codec's datetime encoding: the wire field is a string, and
`ENCODE ... AS RFC3339` bridges it ([Codecs](schemas-and-codecs.md#codecs)).

```nspl
BEGIN;

CREATE SCHEMA order_event (
  order_id STRING,
  amount I64,
  occurred_at DATETIME
);

CREATE WIRE JSON SCHEMA order_event_wire MODE STRICT (
  order_id string,
  amount integer,
  occurred_at string
);

CREATE CODEC order_event_codec
  FROM WIRE JSON SCHEMA order_event_wire
  TO SCHEMA order_event
  ENCODE occurred_at AS RFC3339;

CREATE RELAY replay_orders
  SCHEMA order_event UNBRANCHED
  WITH MATERIALIZED STATE LAST BY TIMESTAMP;

CREATE VHOST replay_edge replay.example.com;

CREATE ENDPOINT replay_ingress ON replay_edge PATH '/events' TYPE HTTP;

CREATE INGESTOR replay_source
  FROM ENDPOINT replay_ingress MODE NO_ACK SEQUENTIAL
  ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING order_event_codec
  TIMESTAMP AT occurred_at
  TO replay_orders
    INHERIT ALL
    UNBRANCHED
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

`TIMESTAMP AT occurred_at` is not optional style here: in a paced domain every ingestor **must**
declare a timestamp source (`TIMESTAMP NOW` or `TIMESTAMP AT <field>`), or ingestion fails with
`requires ingestor ... to declare TIMESTAMP NOW or TIMESTAMP AT <field>`
([Ingestion Timestamps](domains-and-time.md#ingestion-timestamps)).

To make the clock visible, add a generator whose cadence is declared in **logical** seconds:

```nspl
CREATE SCHEMA replay_heartbeat (
  order_id STRING,
  amount I64
);

CREATE RELAY replay_heartbeats SCHEMA replay_heartbeat UNBRANCHED;

CREATE GENERATOR replay_ticker
  USING MATERIALIZED STATE replay_orders
  EACH 10s
  UNBRANCHED
  TO replay_heartbeats
    SET order_id = relay_state.replay_orders.order_id,
        amount = relay_state.replay_orders.amount
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG;

COMMIT;
```

## Start The Clock

A stopped paced domain has no ticks, so nothing is admitted — try POSTing now and the request
fails. Then start the clock at a historical instant, running ten times faster than wall clock
([Start And Stop](domains-and-time.md#start-and-stop)):

```nspl
START AT '2026-01-01T00:00:00Z' TIME RATE 10.0;
```

`START;` alone would resume from persisted state, and `START AT NOW` re-anchors to the current
wall clock; `TIME RATE` defaults to `1.0`.

## Replay The Past

```bash
nervix-cli --domain quickstart_replay subscribe replay_watch replay_heartbeats
```

```bash
curl -i -X POST http://127.0.0.1:8080/events \
  -H 'Host: replay.example.com' \
  -H 'Content-Type: application/json' \
  -d '{"order_id":"o-8001","amount":1500,"occurred_at":"2026-01-01T00:00:05Z"}'
```

The record is admitted against its **January** event time, and heartbeats begin — declared
`EACH 10s`, they arrive roughly **every second** of wall clock, because ten logical seconds pass
per wall second at `TIME RATE 10.0`. Branch TTLs and duration windows in this domain would follow
the same logical clock, which is what makes accelerated replay reproducible; deterministic UDFs
preserve that reproducibility too ([Domains And Time](domains-and-time.md), UDF determinism under
[Nulls, Errors, And Volatility](udfs.md#nulls-errors-and-volatility)).

A record whose timestamp falls outside every tick window is rejected at ingestion with
`rejected ingestor ... event outside any tick window` — that is `SKEW` doing its job. And as
everywhere, `START` clears materialized relay state, so a fresh replay starts from an empty
snapshot surface.

## Where To Go Next

The tutorial graph now ingests from Kafka, HTTP, JSON envelopes, and protobuf; routes
conditionally; partitions and deduplicates per customer; dead-letters failures; classifies with a
custom function; aggregates windows; emits heartbeats from materialized state; joins payments to
orders; evolves live; and replays history on a logical clock. The manual covers what remains:

- [WASM processors](wasm-processor-guests.md) run your own compiled transform logic
- [Lookups](lookups.md) and [materialized dependencies](processors.md#materialized-relay-state)
  enrich in-flight records from reference data
- [Sessions](sessions.md) provide the read-only subscriptions used throughout this tutorial
- [Capacity Planning For Branched Graphs](capacity-planning.md) sizes branched graphs
- [Metrics And Observability](metrics-and-observability.md) instruments everything you just built
- [Examples](examples.md) collects complete runnable graphs, including the `examples/` directory
  in the repository
