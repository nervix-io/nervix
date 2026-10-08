# Paced Simulation Drivers

`examples/paced-simulation` holds two runnable applications that drive a paced simulation through
a Nervix graph. Each one follows the domain clock, submits sensor readings stamped with the tick
centers that clock reaches through a [client ingestor](ingestors.md#client-ingestors), and applies
and acknowledges the readings the graph constructs through [client
emitters](emitters.md#client-emitters), all on one session:

- `nervix-paced-simulation`, in `examples/paced-simulation/rust`, is built on the [Rust Client
  Library](client-library.md).
- `examples/paced-simulation/python/paced_simulation.py` is built on the shared C binding,
  through `ctypes` and the Python standard library alone.

Both take the same options, print the same report and write the same ledger and effect store, so
either one can continue a run the other began. They are complete programs meant to be copied: the
public scenarios in `tests/features/runtime/paced_simulation.feature` run them as published against
the published graph, on one node and on three.

The [Client Ingestors And Emitters](./client-io-architecture.md) architecture chapter explains
the endpoint ownership, ACK, credit, ALTER and recovery contracts these programs exercise.

## The Graph

`examples/paced-simulation/paced_simulation.nspl` creates the paced domain `paced_simulation`
with a 100 ms tick period and an equal admission skew, and this graph in it:

| Entity | Role |
| --- | --- |
| `simulated_readings` | Client ingestor of `reading` with `TIMESTAMP AT occurred_at` |
| `live_readings` | Client ingestor of the same schema with `TIMESTAMP NOW`, for comparison |
| `readings` | Relay of `observed_reading`, branched by sensor |
| `rejected_readings` | Unbranched relay of the rejection notices the ingestors' error routes publish |
| `observed_readings` | Attached client emitter of the constructed readings |
| `rejection_notices` | Attached client emitter of the rejection notices |

Each ingestor route keeps the reading, adds the domain time it was admitted at, and constructs one
concrete branch per sensor. A reading the ingestor cannot admit becomes a rejection notice instead
of an error in a log:

```nspl
CREATE INGESTOR simulated_readings
  FROM CLIENT SCHEMA reading MODE ACK PARALLEL MAX 8 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 2s ON QUIESCE SUSPEND
  TIMESTAMP AT occurred_at
  TO readings
    INHERIT ALL
    SET admitted_at = now(),
        timestamp_source = 'at'
    BRANCHED BY by_sensor SET sensor = message.sensor
    FLUSH EACH 100ms MAX BATCH SIZE 1MiB
    ON MESSAGE ERROR SEND TO rejected_readings SET reading_id = input.reading_id, occurred_at = input.occurred_at, error_code = error.code, error_message = error.message
  ON GENERAL ERROR LOG;
```

The output emitter is attached, so a reading's batch completes for its producer only once a
consumer acknowledged the reading's output. Its batches never mix sensors, because a client batch
never combines concrete branches:

```nspl
CREATE ATTACHED EMITTER observed_readings
  FROM readings
  TO CLIENT SCHEMA observed_reading
    MODE ACK PARALLEL MAX 4 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 2s
  INHERIT ALL
  BATCH MAX MESSAGES 256 MAX SIZE 256KiB
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

The exported schema carries the reading's own event time, `occurred_at`, and the domain time the
ingestor admitted it at, `admitted_at`. Clock observations and deliveries are separate streams
with no order between them, so an application that needs an output's event time reads it from the
output.

The graph needs nothing outside Nervix. Its file includes separate domain bootstrap and `USE`
phases. Create the domain, load the transaction beginning at `BEGIN` with the CLI's domain
selection, then start the clock at the pace the simulation should run at:

```bash
nervix-cli --command "CREATE PACED DOMAIN paced_simulation WITH PERIOD 100ms SKEW 100ms;"
nervix-cli --domain paced_simulation --command \
  "$(sed -n '/^BEGIN;/,$p' examples/paced-simulation/paced_simulation.nspl)"
nervix-cli --domain paced_simulation --command "START AT NOW TIME RATE 4.0;"
```

Check the printed command dispositions: ordinary `--command` currently prints a refusal without
a nonzero exit status. `USE` must run on its own when entered as an NSPL command.

## Running The Drivers

The Rust driver:

```bash
just paced-simulation --server http://127.0.0.1:47391 --username default \
  --password "$NERVIX_PASSWORD" --ticks 100 --sensors 3
```

`just paced-simulation` builds and runs the release binary, which is the same as
`cargo run --release --package nervix-paced-simulation -- <options>`.

The Python driver loads the shared binding named by `--library` or `NERVIX_CLIENT_LIBRARY`, and
needs Python 3.12 or later. `just paced-simulation-python` builds the binding and runs the driver
with it:

```bash
just paced-simulation-python --server http://127.0.0.1:47391 --username default \
  --password "$NERVIX_PASSWORD" --ticks 100 --sensors 3
```

Any live node serves either driver; the node routes producers and consumers to the nodes that
execute the endpoints, so the session's node need not execute any of them. The credentials can
come from `NERVIX_USERNAME` and `NERVIX_PASSWORD` instead.

| Option | Default | Meaning |
| --- | --- | --- |
| `--server` | `http://127.0.0.1:47391` | The gRPC session endpoint of any live node |
| `--username`, `--password` | `NERVIX_USERNAME`, `NERVIX_PASSWORD` | The session's credentials |
| `--domain` | `paced_simulation` | The paced domain the graph runs in |
| `--timestamps` | `at` | `at` submits to `simulated_readings`, `now` to `live_readings` |
| `--ingestor`, `--emitter`, `--rejections` | the example's | The endpoints, when a graph names them differently |
| `--ticks` | `50` | How many tick centers to submit readings for; `0` only replays |
| `--sensors` | `3` | Sensors reporting at each tick center; each is a concrete branch |
| `--burst` | `1` | Readings each sensor reports at each tick center |
| `--invalid-every` | `0` | Stamp the first reading of every Nth tick before the admission window |
| `--consumers` | `1` | Competing consumers of the output emitter |
| `--consumer-delay` | `0s` | How long the output consumers wait before they attach |
| `--consumer-leave-after` | none | The last output consumer leaves after this many deliveries |
| `--processing-time` | `0s` | Application work per delivery, before its effect and its ACK |
| `--credit-batches`, `--credit-bytes` | `8`, `1MiB` | The producer credit to ask for |
| `--inspect-every` | none | Inspect the ingestor and the output emitter at this interval |
| `--ledger` | `paced-simulation-ledger.jsonl` | The application-owned input ledger |
| `--effects` | `paced-simulation-effects.jsonl` | The idempotent effect store |
| `--replay` | off | First resubmit the ledger's readings that did not complete |
| `--follow-generations` | off | After `STOP` and `START`, continue in the new generation |
| `--deadline` | `10m` | How long to wait for outstanding outcomes once submitting stops |

Durations are written like NSPL durations, such as `250ms`, `1s` or `5m`, and byte sizes as a
count of bytes, `KiB`, `MiB` or `GiB`.

## Choosing The Pace

Three settings decide how fast a simulation runs and how much it produces:

- `START AT NOW TIME RATE <rate>` sets how fast domain time runs against wall time: `0.5` runs the
  simulation at half real time and `4.0` at four times real time. A driver submits each tick's
  readings once the clock reaches that tick center, so the rate paces the drivers too.
- The domain's `PERIOD` spaces the tick centers, and so the readings of one sensor.
- `--sensors` and `--burst` set how many readings each tick center produces:
  `sensors × burst` readings per period of logical time, in one batch.

Slow application processing does not slow domain time. A driver that falls behind submits the
centers it missed as soon as it can, stamped with their own times, and the ingestor admits them as
long as its window still retains them: the newest 256 reached centers. A center the window no
longer retains is skipped with a `SKIPPED` line instead of being submitted only to be rejected.

## What A Run Does

1. It opens one session and attaches it to the domain clock with `ATTACH DOMAIN CLOCK`. The clock
   the attach reports decides whether the run can start: a stopped clock, an unpaced domain or
   a clock the node has not installed within 30 seconds ends the run with exit status 2.
2. It opens the rejection consumer and the output consumers before the producer, because a reading
   completes only once a consumer acknowledged its output: a run that waited for its outcomes
   before consuming would wait on itself. An endpoint that is not running on its node yet is
   asked again for up to 30 seconds; any other refusal ends the run with exit status 2.
3. For each tick it reads the latest clock the session observed, waits until domain time reaches
   the next tick center, and stamps every reading of the tick with that center. It waits until
   the batch fits in the producer's credit, then reads the window the ingestor admits event times
   in again, so a batch held back for credit is never checked against a window that has moved on.
   It writes the readings to the ledger, submits them as one batch, and awaits the batch's outcome
   without holding up the next tick.
4. Each consumer decodes a delivery, records its readings in the effect store, and only then
   acknowledges it.
5. Once it submitted its last tick it waits for the outcomes it still awaits, and for the
   rejection notices of its completed batches, up to `--deadline`; closes its producer and
   consumers; detaches the clock; and prints its summary.

The drivers compute no domain time themselves. Every wait, logical instant and admission check
comes from the attached clock's projections: `wall_duration_until`, `logical_time_at` and
`admission_window` in Rust, and `nx_domain_clock_wall_duration_until`,
`nx_domain_clock_logical_time_at`, `nx_domain_clock_admission_window` and `nx_domain_clock_admits`
through the binding. `FLUSH EACH` and the admission window follow domain time, while `ACK TIMEOUT`,
retry backoff, session liveness and the drivers' own deadlines are physical.

## The Report

Both drivers print one line per observation on standard output, each line whole, and the same
lines for the same run. Errors go to standard error, prefixed with `error:`.

| Line | Printed when |
| --- | --- |
| `CONNECTED server=… domain=…` | The session opened |
| `CLOCK generation=… state=paced period=… skew=… origin=… anchor=… rate=…` | The attach, or a later observation, reported a paced clock |
| `CLOCK generation=… state=stopped\|uninstalled\|unpaced` | The clock is in another state |
| `CLOCK restoration_failed`, `CLOCK unavailable`, `CLOCK ended` | Restoring the attachment failed for now, the session could not reopen, or the domain was removed |
| `INTERRUPTED clock domain=…` | The session holding the clock attachment ended |
| `CONSUMER opened consumer=… emitter=… generation=… batches=… bytes=…` | A consumer opened, with its granted credit |
| `PRODUCER opened ingestor=… generation=… batches=… bytes=… max_batch_rows=… max_batch_bytes=…` | The producer opened, with its granted credit |
| `READY generation=…` | The run starts submitting |
| `SUBMIT tick=… occurred_at=… window=…..…` | The clock reached a tick center, inside that admission window |
| `WAITING credit outstanding_batches=… outstanding_bytes=…` | The next batch waits for the producer's credit |
| `SKIPPED tick=… reason=behind_admission_window` | The window no longer retains a tick center the run fell behind |
| `SUBMITTED tick=… readings=… bytes=…` | The producer holds the batch |
| `OUTCOME tick=… readings=… completed\|not_admitted\|processing_failed\|outcome_unknown [cause]` | The batch's terminal outcome |
| `PROCESSING`, `DELIVERY … applied=… duplicates=…`, `ACK … confirmed` | A consumer handled a delivery |
| `REJECTED reading_id=… occurred_at=… error_code=… error_message=…` | A rejection notice arrived |
| `INTERRUPTED consumer=…`, `CONSUMER reopen_required\|reopened\|left\|unavailable …` | A consumer's attachment changed |
| `CONSUMER delayed`, `PRODUCER unavailable`, `… close_failed`, `CLOCK detach_failed` | A consumer waits to join, the session could not be restored for a submission yet, or closing failed |
| `REOPENED generation=… ingestor=… reason=…` | The producer accepted a changed contract within the current START generation |
| `REOPENED generation=… ingestor=… after=…` | The run moved on to a new START generation |
| `REPLAY tick=… readings=…`, `REPLAY expired\|skipped …` | A replay resubmitted, or could not resubmit, ledger readings |
| `INSPECT …` | An inspection of the ingestor and the output emitter |
| `WAITING notices outstanding=…`, `NOTICES missing=…` | The run waits for rejection notices of completed batches |
| `STOPPING reason=…` | The run stops submitting |
| `SUMMARY …` | The run ended |

The summary counts `readings` submitted, the readings of each outcome, the `effects` and
`rejection_notices` the consumers recorded for the first time, `duplicates` of either that the
store already held, the START `generations` the run planned in, `ticks_observed`, `inspections`,
`credit_waits`, the `peak_outstanding_bytes` the producer held, and the output
`consumers_joined` and `consumers_left`. The exit status says how the run ended:

| Status | Meaning |
| --- | --- |
| 0 | Every reading the run submitted completed |
| 3 | A reading did not complete: it was not admitted, its processing failed, or its outcome is unknown |
| 2 | The command line, the graph or the domain is not one the simulation can run against |
| 1 | The run failed on its own side, or outcomes were still outstanding at `--deadline` |

## Observing A Run

`--inspect-every` runs `SHOW INGESTORS` and `DESCRIBE EMITTER` on the driver's own session while it
produces and consumes, and prints the ingestor's admission, producers, outstanding batches and
bytes, and the emitter's consumers and retained batches:

```text
INSPECT simulated_readings source=CLIENT schema=reading owner=node-2 status=running admission=open producers=1 outstanding_batches=1 outstanding_bytes=110592 admitted_batches=1
INSPECT emitter=observed_readings consumers=1 retained_batches=2
```

The same commands, `DESCRIBE INGESTOR simulated_readings` and `nervix-cli domain-clock`, work from
any other session; inspecting an emitter claims and acknowledges nothing. The ledger and the
effect store are ordinary JSON-lines files the application owns:

```text
{"record":"reading","reading_id":"g1-t12-s0-0","generation":1,"tick":12,"sensor":"sensor-0","occurred_at":"2026-10-02T18:00:01.200Z","value":372,"ingestor":"simulated_readings","timestamps":"at","stamp":"center"}
{"record":"outcome","reading_ids":["g1-t12-s0-0","g1-t12-s1-0"],"outcome":"completed","cause":""}
{"kind":"reading","reading_id":"g1-t12-s0-0","sensor":"sensor-0","tick":12,"occurred_at":"2026-10-02T18:00:01.200Z","admitted_at":"2026-10-02T18:00:01.230Z","timestamp_source":"at","value":372,"generation":1}
```

A reading's identity names the START generation, tick, sensor and burst position it was planned
for, so it stays the same when the reading is replayed.

## Event Time: `TIMESTAMP AT` And `TIMESTAMP NOW`

`--invalid-every 3` stamps the first reading of every third tick one nanosecond further than the
skew before the oldest center the window retains. Submitted to `simulated_readings`, which reads
`TIMESTAMP AT occurred_at`, that reading is rejected: its batch still completes, because the error
route acknowledges the reading once its notice is published, and the rejection consumer records the
notice with code `validation`. Submitted to `live_readings` with `--timestamps now`, the same
reading is admitted, stamped with the domain time it arrived at, and its `occurred_at` travels
through unchanged.

`TIMESTAMP NOW` is still tested against the window: the arrival time lies less than one period after
the newest reached center, and it is admitted within the skew of that center. The example's skew
equals its period, so every arrival is admitted. With a skew smaller than the period, arrivals that
fall between two windows are rejected like any other event time outside them.

## Interruptions, Outcomes And Replay

The clock and the data endpoints recover independently, and neither waits for the other:

- When the session ends, the clock attachment reports `INTERRUPTED clock` and the driver stops
  planning: the clock it read before the gap no longer says which event times the ingestor admits.
  It continues once the restored attachment reports the clock again. The producer and the
  consumers restore their own attachments when the domain's START generation and their endpoint
  contracts are unchanged, and a consumer reports `INTERRUPTED consumer` once.
- A batch whose session ended before its outcome arrived ends `outcome_unknown session_lost`. The
  drivers never resend it on their own: it may have been admitted and processed. The run exits
  with status 3.
- `--replay` resubmits, before the run simulates anything, every ledger reading whose latest
  outcome is not `completed`, with its identity and content unchanged, when it belongs to the
  START generation the domain runs now and the window still admits its event time. A reading of
  another generation is never submitted against this one.
- A replayed reading may already have been applied: its output may have been delivered before the
  outcome was lost, or redelivered after the session came back. The effect store is keyed by the
  reading's identity, so the consumer acknowledges such a delivery as a duplicate without applying
  it again. Processing is at least once; each effect happens once because the application makes it
  idempotent, not because delivery is exactly once.
- A delivery whose acknowledgement was lost comes again with the same identity and a fresh
  reference, and is recorded as a duplicate.
- When an endpoint requires a new open because its contract or schema changed, it was removed,
  it violated the protocol, or restoration was refused, the driver opens a fresh producer or
  consumer immediately in the current START generation. Consumers print `CONSUMER reopen_required`
  and `CONSUMER reopened`; the producer prints `REOPENED` with the unchanged generation and the
  reason. An `endpoint unavailable` refusal is retried within the existing 30-second physical
  open budget. A flush-only change preserves the contract and requires no application reopen.
  Clock and credit waits recheck the endpoint every 200ms, so a slow domain rate does not defer
  reopening until the next tick. Suspended admission pauses planning before it creates readings.
  An alteration drains admitted work before it publishes the changed contract. The example keeps
  its buffered `FLUSH EACH 100ms` branched input route: an intake hold force-flushes partial batches
  through the open shared relay and waits for their ACK roots before the full alteration hold
  closes the relay gates. Admission stays suspended across both holds. An unsuccessful alteration
  keeps the committed contract and requires no reopen.
- Each replacement validates the example's exact fields again and requests the same credit
  limits. A schema mismatch, removed endpoint, or unusable credit limit ends the run with status
  2 and the endpoint's refusal text. A fresh producer has fresh credit; outstanding submissions
  retain their original producer and credit until their outcomes resolve. Nothing with an unknown
  outcome is resent. A tick already planned when its producer ended is recorded as `not_admitted
  not_sent` if it never left the client, or with the node's refusal if it reached the node. These
  readings can be resubmitted only by an explicit `--replay`.
- `STOP` ends the producer and the consumers with the generation; their batches that had not
  completed end not admitted or of unknown outcome. With `--follow-generations` the driver waits for
  the next `START`, opens a new producer and new consumers under the new generation, and continues
  from that generation's reached tick center. Without it the run finishes.
  The `domain_stopped` and `generation_changed` reopen reasons wait for this next generation.
  Once the clock reports a later paced generation, the explicit new producer also retries
  `domain stopped` within the same 30-second open budget: the serving node can report that clock
  before its endpoint-open view has applied the corresponding `START`. Both drivers still require
  the opened producer's own generation to have a paced clock before planning any readings.
  Opens in the current generation retain their immediate `domain stopped` refusal, and schema,
  missing-endpoint and other terminal refusals remain terminal while following a generation.
- With `--consumer-delay` the output emitter has no consumer at first: it retains the batches it
  could not deliver, which `DESCRIBE EMITTER` shows, and the producer stops being granted credit once
  its outstanding batches fill it. The backlog is bounded by that credit. Consumers that join drain
  it with explicit acknowledgements, and one that leaves with `--consumer-leave-after` hands the
  rest to the others.

## Stopping

Ctrl-C stops the planning. A batch still waiting for credit has sent nothing and ends not admitted
with the cause `not_sent`; the driver then waits for the outcomes it awaits, closes its endpoints,
detaches the clock and prints its summary, as at the end of every run.

## Limits

The ledger and the effect store are files the example flushes but does not make durable; an
application that needs its input to survive a crash keeps a durable outbox. Nervix persists
neither payloads nor delivery state: an owner lost while it held a batch loses it, and only a
replay from the application's own ledger recovers it. A rejection notice the run did not receive
before `--deadline` is delivered to the next consumer of the rejection emitter, which records it
then.
