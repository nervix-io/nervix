#!/usr/bin/env python3
"""A paced sensor simulation that produces into and consumes from a Nervix graph through the
native Python package, nervix_client, over the Rust client.

It is the Python half of the runnable paced simulation under examples/paced-simulation: it does
what the Rust driver beside it does, prints the same report and writes the same ledger and effect
store, so either can continue a run the other left. See examples/paced-simulation/README.md.

The driver attaches its session to the domain clock and reads the clock the attach reported before
it uses anything else. For every tick center the clock reaches it submits one batch of sensor
readings stamped with that center through a client ingestor, built column by column with the
binding's batch builder. Consumers of the example's two attached client emitters run on the same
session from the start: each applies a delivery to the idempotent effect store, keyed by the
reading's own identity, and only then acknowledges it. Every reading is written to the
application-owned ledger before it is submitted, so a reading whose outcome is lost can be
replayed deliberately with --replay.

The package releases the GIL around every blocking call, so
the clock, consumer, inspection and outcome threads wait in the binding side by side, and
cancelling a token wakes whichever of them waits on it.
"""

import argparse
from array import array
import json
import os
import re
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from enum import Enum

import nervix_client as nx

# The report and ledger use the same names as the Rust driver. Decisions use the package's
# typed outcomes; these tables only render them.
REFUSALS = {
    nx.OpenRefusal.DOMAIN_NOT_FOUND: "domain not found",
    nx.OpenRefusal.DOMAIN_STOPPED: "domain stopped",
    nx.OpenRefusal.ENDPOINT_NOT_FOUND: "endpoint not found",
    nx.OpenRefusal.NOT_CLIENT_ENDPOINT: "not a client endpoint",
    nx.OpenRefusal.ENDPOINT_UNAVAILABLE: "endpoint unavailable",
    nx.OpenRefusal.SCHEMA_MISMATCH: "schema mismatch",
    nx.OpenRefusal.TOO_MANY_ENDPOINTS: "too many endpoints",
    nx.OpenRefusal.SESSION_CAPACITY_EXHAUSTED: "session capacity exhausted",
    nx.OpenRefusal.NODE_CAPACITY_EXHAUSTED: "node capacity exhausted",
    nx.OpenRefusal.INVALID_LIMITS: "invalid limits",
    nx.OpenRefusal.IN_TRANSACTION: "in transaction",
}
SUBMISSION_REFUSALS = {
    nx.SubmissionRefusal.INVALID_BATCH: "invalid_batch",
    nx.SubmissionRefusal.SUSPENDED: "suspended",
    nx.SubmissionRefusal.BUSY: "busy",
    nx.SubmissionRefusal.DRAINING: "draining",
    nx.SubmissionRefusal.PRODUCER_ENDED: "producer_ended",
    nx.SubmissionRefusal.CREDIT_EXCEEDED: "credit_exceeded",
}
PROCESSING_FAILURES = {
    nx.ProcessingFailure.ACK_TIMED_OUT: "ack_timeout",
    nx.ProcessingFailure.REJECTED: "rejected",
}
UNCERTAINTIES = {
    nx.Uncertainty.INTERRUPTED: "interrupted",
    nx.Uncertainty.OWNER_LOST: "owner_lost",
    nx.Uncertainty.SESSION_LOST: "session_lost",
}
REOPEN_REASONS = {
    nx.ReopenReason.DOMAIN_STOPPED: "domain_stopped",
    nx.ReopenReason.ENDPOINT_REMOVED: "endpoint_removed",
    nx.ReopenReason.SCHEMA_CHANGED: "schema_changed",
    nx.ReopenReason.CONTRACT_CHANGED: "contract_changed",
    nx.ReopenReason.GENERATION_CHANGED: "generation_changed",
    nx.ReopenReason.PROTOCOL_VIOLATED: "protocol_violated",
    nx.ReopenReason.REFUSED: "refused",
}
SETTLEMENTS = {
    nx.Settlement.CONFIRMED: "confirmed",
    nx.Settlement.STALE_REFERENCE: "stale_reference",
    nx.Settlement.WRONG_CONSUMER: "wrong_consumer",
    nx.Settlement.INVALID_REASON: "invalid_reason",
    nx.Settlement.CONSUMER_ENDED: "consumer_ended",
}
CLOCK_END_REASONS = {nx.ClockEndReason.DOMAIN_REMOVED: "domain_removed"}


# ---- Report ------------------------------------------------------------------------------------

PRINT = threading.Lock()


def line(message):
    """Prints one report line whole, so lines of concurrent threads never interleave."""
    with PRINT:
        sys.stdout.write(message + "\n")
        sys.stdout.flush()


def error_line(message):
    with PRINT:
        sys.stderr.write("error: " + message + "\n")
        sys.stderr.flush()


class Counters:
    """The counters every thread of a run adds to; the summary prints them."""

    def __init__(self):
        self.lock = threading.Lock()
        self.readings = 0
        self.completed = 0
        self.not_admitted = 0
        self.processing_failed = 0
        self.outcome_unknown = 0
        self.effects = 0
        self.rejection_notices = 0
        self.duplicates = 0
        self.generations = set()
        self.ticks_observed = 0
        self.inspections = 0
        self.credit_waits = 0
        self.peak_outstanding_bytes = 0
        self.consumers_joined = 0
        self.consumers_left = 0

    def add(self, name, amount=1):
        with self.lock:
            setattr(self, name, getattr(self, name) + amount)

    def outcome(self, kind, readings):
        self.add(kind, readings)

    def generation(self, generation):
        with self.lock:
            self.generations.add(generation)

    def outstanding(self, held):
        with self.lock:
            self.peak_outstanding_bytes = max(self.peak_outstanding_bytes, held)

    def all_completed(self):
        with self.lock:
            return self.completed == self.readings

    def summary(self):
        with self.lock:
            return (
                f"SUMMARY readings={self.readings} completed={self.completed} "
                f"not_admitted={self.not_admitted} processing_failed={self.processing_failed} "
                f"outcome_unknown={self.outcome_unknown} effects={self.effects} "
                f"rejection_notices={self.rejection_notices} duplicates={self.duplicates} "
                f"generations={len(self.generations)} ticks_observed={self.ticks_observed} "
                f"inspections={self.inspections} credit_waits={self.credit_waits} "
                f"peak_outstanding_bytes={self.peak_outstanding_bytes} "
                f"consumers_joined={self.consumers_joined} consumers_left={self.consumers_left}"
            )


# ---- Text the report and the files share with the Rust driver ---------------------------------


def rfc3339(nanos):
    """The RFC 3339 text of a Unix-nanosecond instant: UTC, with as many groups of three
    fractional digits as it needs, which is how the Rust driver writes it."""
    seconds, fraction = divmod(nanos, 1_000_000_000)
    moment = datetime.fromtimestamp(seconds, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    if fraction == 0:
        return moment + "Z"
    if fraction % 1_000_000 == 0:
        return f"{moment}.{fraction // 1_000_000:03d}Z"
    if fraction % 1_000 == 0:
        return f"{moment}.{fraction // 1_000:06d}Z"
    return f"{moment}.{fraction:09d}Z"


def parse_rfc3339(value):
    """Reads the RFC 3339 text the drivers write back into Unix nanoseconds."""
    match = re.fullmatch(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?Z", value)
    if match is None:
        raise ValueError(f"'{value}' is not an RFC 3339 UTC instant")
    moment = datetime.strptime(match.group(1), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    seconds = int(moment.timestamp())
    fraction = int((match.group(2) or "").ljust(9, "0"))
    return seconds * 1_000_000_000 + fraction


def duration_text(nanos):
    """A duration the way Plexus writes it, in the largest unit that divides it."""
    if nanos % 1_000_000_000 == 0:
        return f"{nanos // 1_000_000_000}s"
    if nanos % 1_000_000 == 0:
        return f"{nanos // 1_000_000}ms"
    if nanos % 1_000 == 0:
        return f"{nanos // 1_000}us"
    return f"{nanos}ns"


def rate_text(rate):
    """A time rate the way the Rust driver prints it: without a fraction when it has none."""
    rendered = repr(rate)
    if rendered.endswith(".0"):
        return rendered[:-2]
    return rendered


DURATION_UNITS = {"ns": 1, "us": 1_000, "ms": 1_000_000, "s": 1_000_000_000,
                  "m": 60_000_000_000, "h": 3_600_000_000_000}


def duration_argument(value):
    """A duration argument such as 250ms, 1s or 5m, in nanoseconds."""
    match = re.fullmatch(r"(\d+)(ns|us|ms|s|m|h)", value)
    if match is None:
        raise argparse.ArgumentTypeError(f"'{value}' is not a duration such as 250ms, 1s or 5m")
    return int(match.group(1)) * DURATION_UNITS[match.group(2)]


def byte_size_argument(value):
    """A byte size: a count of bytes, or a count of KiB, MiB or GiB."""
    match = re.fullmatch(r"(\d+)(GiB|MiB|KiB)?", value)
    if match is None:
        raise argparse.ArgumentTypeError(
            f"'{value}' is not a byte size such as 65536, 64KiB or 1MiB"
        )
    multiplier = {None: 1, "KiB": 1024, "MiB": 1024 * 1024, "GiB": 1024 * 1024 * 1024}
    size = int(match.group(1)) * multiplier[match.group(2)]
    if size >= 2**64:
        raise argparse.ArgumentTypeError(f"'{value}' is more bytes than a size can hold")
    return size


# ---- Options -----------------------------------------------------------------------------------

MAX_BATCH_ROWS = 65_536
INGESTORS = {"at": "simulated_readings", "now": "live_readings"}


class ConfigurationError(Exception):
    """The command line, the graph or the domain is not one the simulation can run against."""


def options(arguments):
    parser = argparse.ArgumentParser(
        prog="paced_simulation.py",
        description="Paced sensor simulation over a Nervix client ingestor and attached client "
        "emitters, through the native Python package.",
    )
    parser.add_argument("--server", default="http://127.0.0.1:47391",
                        help="the gRPC session endpoint of any live node")
    parser.add_argument("--username", default=os.environ.get("NERVIX_USERNAME"))
    parser.add_argument("--password", default=os.environ.get("NERVIX_PASSWORD"))
    parser.add_argument("--domain", default="paced_simulation")
    parser.add_argument("--timestamps", choices=["at", "now"], default="at",
                        help="submit to the TIMESTAMP AT ingestor or to the TIMESTAMP NOW one")
    parser.add_argument("--ingestor", help="the client ingestor, instead of the one "
                        "--timestamps selects")
    parser.add_argument("--emitter", default="observed_readings")
    parser.add_argument("--rejections", default="rejection_notices")
    parser.add_argument("--ticks", type=int, default=50)
    parser.add_argument("--sensors", type=int, default=3)
    parser.add_argument("--burst", type=int, default=1)
    parser.add_argument("--invalid-every", type=int, default=0)
    parser.add_argument("--consumers", type=int, default=1)
    parser.add_argument("--consumer-delay", type=duration_argument, default=0)
    parser.add_argument("--consumer-leave-after", type=int)
    parser.add_argument("--processing-time", type=duration_argument, default=0)
    parser.add_argument("--credit-batches", type=int, default=8)
    parser.add_argument("--credit-bytes", type=byte_size_argument, default=1024 * 1024)
    parser.add_argument("--inspect-every", type=duration_argument)
    parser.add_argument("--ledger", default="paced-simulation-ledger.jsonl")
    parser.add_argument("--effects", default="paced-simulation-effects.jsonl")
    parser.add_argument("--replay", action="store_true")
    parser.add_argument("--follow-generations", action="store_true")
    parser.add_argument("--deadline", type=duration_argument, default=600_000_000_000)
    settings = parser.parse_args(arguments)
    for option in ("sensors", "burst", "consumers", "credit_batches", "credit_bytes"):
        if getattr(settings, option) < 1:
            raise ConfigurationError(f"--{option.replace('_', '-')} must be at least one")
    for option in ("ticks", "invalid_every"):
        if getattr(settings, option) < 0:
            raise ConfigurationError(f"--{option.replace('_', '-')} must not be negative")
    rows = settings.sensors * settings.burst
    if rows > MAX_BATCH_ROWS:
        raise ConfigurationError(
            f"--sensors {settings.sensors} with --burst {settings.burst} puts {rows} readings in "
            f"one batch, more than the {MAX_BATCH_ROWS} a batch may carry"
        )
    if settings.consumer_leave_after is not None and settings.consumers < 2:
        raise ConfigurationError(
            "--consumer-leave-after needs a second consumer to stay; pass --consumers 2 or more"
        )
    if settings.ingestor is None:
        settings.ingestor = INGESTORS[settings.timestamps]
    return settings


# ---- Readings ----------------------------------------------------------------------------------

# The fields of the example's schemas, in declared order: name and type. None of them is optional
# or sensitive.
READING_FIELDS = [("reading_id", nx.FieldType.STRING), ("sensor", nx.FieldType.STRING), ("tick", nx.FieldType.U64),
                  ("occurred_at", nx.FieldType.DATETIME), ("value", nx.FieldType.I64)]
OBSERVED_FIELDS = [("reading_id", nx.FieldType.STRING), ("sensor", nx.FieldType.STRING), ("tick", nx.FieldType.U64),
                   ("occurred_at", nx.FieldType.DATETIME), ("admitted_at", nx.FieldType.DATETIME),
                   ("timestamp_source", nx.FieldType.STRING), ("value", nx.FieldType.I64)]
REJECTED_FIELDS = [("reading_id", nx.FieldType.STRING), ("occurred_at", nx.FieldType.DATETIME),
                   ("error_code", nx.FieldType.STRING), ("error_message", nx.FieldType.STRING)]


class Reading:
    """One simulated sensor reading, named and valued by where it is planned."""

    def __init__(self, generation, tick, sensor, position, occurred_at, stamp):
        self.reading_id = f"g{generation}-t{tick}-s{sensor}-{position}"
        self.generation = generation
        self.tick = tick
        self.sensor = f"sensor-{sensor}"
        self.occurred_at = occurred_at
        self.value = (tick * 31 + sensor * 17 + position * 7) % 1000
        self.stamp = stamp

    @classmethod
    def recorded(cls, record):
        """A reading as the ledger recorded it."""
        reading = cls.__new__(cls)
        reading.reading_id = record["reading_id"]
        reading.generation = record["generation"]
        reading.tick = record["tick"]
        reading.sensor = record["sensor"]
        reading.occurred_at = parse_rfc3339(record["occurred_at"])
        reading.value = record["value"]
        reading.stamp = record["stamp"]
        return reading


def fields(declared):
    """The expected fields an endpoint open declares, in the graph's field order."""
    expected = nx.Fields()
    for name, kind in declared:
        expected.add(name, kind)
    return expected


def varlen(values):
    """The offsets and bytes of a STRING column."""
    offsets = [0]
    encoded = []
    for value in values:
        encoded.append(value.encode())
        offsets.append(offsets[-1] + len(encoded[-1]))
    return offsets, b"".join(encoded)


def build_batch(schema, readings):
    """One batch of readings, set column by column through the package's builder."""
    builder = nx.BatchBuilder(schema, len(readings))
    for column, values in ((0, [reading.reading_id for reading in readings]),
                           (1, [reading.sensor for reading in readings])):
        offsets, data = varlen(values)
        builder.varlen(column, 0, offsets, data)
    builder.fixed(2, 0, array("Q", (reading.tick for reading in readings)).tobytes())
    builder.fixed(3, 0, array("q", (reading.occurred_at for reading in readings)).tobytes())
    builder.fixed(4, 0, array("q", (reading.value for reading in readings)).tobytes())
    return builder.finish()


def observed_readings(batch):
    """The readings of one delivery, copied in bulk from its typed columns."""
    identities, sensors, ticks, occurred, admitted, sources, values = (
        batch.column(column) for column in range(7)
    )
    return [
        {"reading_id": identities[row], "sensor": sensors[row], "tick": ticks[row],
         "occurred_at": occurred[row], "admitted_at": admitted[row],
         "timestamp_source": sources[row], "value": values[row]}
        for row in range(batch.row_count)
    ]


def rejection_notices(batch):
    """The notices of one delivery, copied in bulk from its typed columns."""
    identities, occurred, codes, messages = (batch.column(column) for column in range(4))
    return [
        {"reading_id": identities[row], "occurred_at": occurred[row], "error_code": codes[row],
         "error_message": messages[row]}
        for row in range(batch.row_count)
    ]


# ---- The ledger and the effect store -----------------------------------------------------------


class Ledger:
    """The application-owned input ledger: every reading before it is submitted, the START
    generation each run plans in, and the outcome of every batch. A production outbox would make
    each append durable before submitting; this example only flushes it."""

    def __init__(self, path):
        self.lock = threading.Lock()
        self.file = open(path, "a", encoding="utf-8")

    @staticmethod
    def unresolved(path):
        """The readings the ledger holds without a completed outcome, in planned order."""
        try:
            with open(path, encoding="utf-8") as ledger:
                content = ledger.read()
        except FileNotFoundError:
            return []
        order = []
        readings = {}
        outcomes = {}
        for number, entry in enumerate(content.splitlines(), start=1):
            try:
                record = json.loads(entry)
                kind = record["record"]
            except (ValueError, KeyError) as error:
                raise ConfigurationError(
                    f"line {number} of the ledger '{path}' is not a ledger record: {error}"
                ) from error
            if kind == "reading":
                if record["reading_id"] not in readings:
                    order.append(record["reading_id"])
                readings[record["reading_id"]] = record
            elif kind == "outcome":
                for reading_id in record["reading_ids"]:
                    outcomes[reading_id] = record["outcome"]
        return [Reading.recorded(readings[reading_id]) for reading_id in order
                if outcomes.get(reading_id) != "completed"]

    def append(self, records):
        rendered = "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records)
        with self.lock:
            self.file.write(rendered)
            self.file.flush()

    def generation(self, generation):
        self.append([{"record": "generation", "generation": generation}])

    def readings(self, readings, ingestor, timestamps):
        self.append([
            {"record": "reading", "reading_id": reading.reading_id,
             "generation": reading.generation, "tick": reading.tick, "sensor": reading.sensor,
             "occurred_at": rfc3339(reading.occurred_at), "value": reading.value,
             "ingestor": ingestor, "timestamps": timestamps, "stamp": reading.stamp}
            for reading in readings
        ])

    def outcome(self, reading_ids, outcome, cause):
        self.append([{"record": "outcome", "reading_ids": reading_ids, "outcome": outcome,
                      "cause": cause}])


class EffectStore:
    """The application's idempotent effect store: each reading and rejection notice is applied
    exactly once, keyed by the reading's own identity, which survives replays."""

    def __init__(self, path):
        self.condition = threading.Condition()
        self.readings = set()
        self.rejections = set()
        try:
            with open(path, encoding="utf-8") as store:
                content = store.read()
        except FileNotFoundError:
            content = ""
        for number, entry in enumerate(content.splitlines(), start=1):
            try:
                record = json.loads(entry)
                kind = record["kind"]
                reading_id = record["reading_id"]
            except (ValueError, KeyError) as error:
                raise ConfigurationError(
                    f"line {number} of the effect store '{path}' is not an effect: {error}"
                ) from error
            if kind == "reading":
                self.readings.add(reading_id)
            else:
                self.rejections.add(reading_id)
        self.file = open(path, "a", encoding="utf-8")

    def apply(self, kind, record):
        """Records `record` unless its reading was recorded before; returns whether it was new."""
        applied = self.readings if kind == "reading" else self.rejections
        with self.condition:
            if record["reading_id"] in applied:
                return False
            self.file.write(json.dumps(record, separators=(",", ":")) + "\n")
            self.file.flush()
            applied.add(record["reading_id"])
            self.condition.notify_all()
            return True

    def await_rejections(self, reading_ids, until, stopping):
        """Waits until the store holds the rejection notice of every reading in `reading_ids`, or
        `until` passes or `stopping` reports an application failure. A notice still missing then
        reaches a later run."""
        reported = False
        with self.condition:
            while True:
                if stopping():
                    return
                outstanding = sum(1 for reading_id in reading_ids
                                  if reading_id not in self.rejections)
                if outstanding == 0:
                    return
                if not reported:
                    line(f"WAITING notices outstanding={outstanding}")
                    reported = True
                remaining = until - time.monotonic()
                if remaining <= 0:
                    line(f"NOTICES missing={outstanding}")
                    return
                self.condition.wait(min(remaining, 0.2))


# ---- The domain clock --------------------------------------------------------------------------


class Window:
    """The event times a TIMESTAMP AT ingestor admits at one UTC instant."""

    def __init__(self, clock, utc, earliest, latest):
        self.clock = clock
        self.utc = utc
        self.earliest = earliest
        self.latest = latest
        self.skew = clock.paced.skew

    @classmethod
    def at(cls, clock, utc):
        bounds = clock.admission_window(utc)
        if bounds is None:
            return None
        return cls(clock, utc, *bounds)

    def contains(self, event):
        return self.clock.admits(self.utc, event)


def clock_line(generation, state, paced):
    """The report line of a clock observation."""
    if state == nx.ClockState.PACED:
        return (f"CLOCK generation={generation} state=paced period={duration_text(paced.period)} "
                f"skew={duration_text(paced.skew)} origin={rfc3339(paced.origin)} "
                f"anchor={rfc3339(paced.anchor)} rate={rate_text(paced.rate)}")
    names = {nx.ClockState.STOPPED: "stopped", nx.ClockState.UNINSTALLED: "uninstalled", nx.ClockState.UNPACED: "unpaced"}
    return f"CLOCK generation={generation} state={names[state]}"


PACED, PAUSED, STOPPED, UNPACED, ENDED = "paced", "paused", "stopped", "unpaced", "ended"


def pace_of(generation, state):
    """What a clock observation lets the simulation do: pace by it, wait, or stop."""
    if state == nx.ClockState.PACED:
        return (PACED, generation)
    if state == nx.ClockState.UNINSTALLED:
        return (PAUSED, generation)
    if state == nx.ClockState.STOPPED:
        return (STOPPED, generation)
    return (UNPACED, generation)


class Clock:
    """The clock the session follows, and the pace its latest observation allows. A session gap
    pauses the pace until the restored attachment reports the clock again."""

    def __init__(self, session, domain, counters, initial):
        self.session = session
        self.domain = domain
        self.counters = counters
        self.condition = threading.Condition()
        self.pace = initial
        self.changes = 0
        self.cancel = nx.Cancel()
        self.thread = threading.Thread(target=self.follow, name="clock", daemon=True)

    def start(self):
        self.thread.start()

    def stop(self):
        self.cancel.trigger()
        self.thread.join()

    def publish(self, pace):
        with self.condition:
            self.pace = pace
            self.changes += 1
            self.condition.notify_all()

    def current(self):
        with self.condition:
            return self.pace, self.changes

    def wait_change(self, changes, stopping, timeout=None):
        """Waits until the pace changes after `changes`, for at most `timeout` seconds. Returns
        False once `stopping` is set."""
        deadline = None if timeout is None else time.monotonic() + timeout
        with self.condition:
            while self.changes == changes and not stopping.is_set():
                remaining = 0.2 if deadline is None else min(0.2, deadline - time.monotonic())
                if remaining <= 0:
                    break
                self.condition.wait(remaining)
        return not stopping.is_set()

    def follow(self):
        while True:
            try:
                event = self.session.next_clock_event(cancel=self.cancel)
            except nx.ClientError as failure:
                if failure.kind == nx.FailureKind.CANCELLED:
                    return
                line(f"CLOCK unavailable reason={failure.message}")
                time.sleep(1.0)
                continue
            try:
                if not self.observe(event):
                    return
            finally:
                del event

    def observe(self, event):
        if event.kind == nx.ClockEventKind.TICK:
            self.counters.add("ticks_observed")
            return True
        if event.kind == nx.ClockEventKind.STATE:
            paced = event.paced if event.state == nx.ClockState.PACED else None
            line(clock_line(event.generation, event.state, paced))
            self.publish(pace_of(event.generation, event.state))
            return True
        if event.kind == nx.ClockEventKind.INTERRUPTED:
            line(f"INTERRUPTED clock domain={self.domain}")
            pace, _ = self.current()
            if pace[0] == PACED:
                self.publish((PAUSED, pace[1]))
            return True
        if event.kind == nx.ClockEventKind.RESTORATION_FAILED:
            line(f"CLOCK restoration_failed domain={self.domain}")
            return True
        line(f"CLOCK ended domain={self.domain} reason={CLOCK_END_REASONS[event.end_reason]}")
        self.publish((ENDED, None))
        return False

    def snapshot(self):
        return self.session.domain_clock(self.domain)

    def paced(self, generation):
        """The paced clock of `generation` as the session last observed it, if it still has it."""
        clock = self.snapshot()
        if clock is None or clock.generation != generation or clock.state != nx.ClockState.PACED:
            return None
        return clock

    def reach(self, generation, center, stopping):
        """Waits until the clock of `generation` reaches `center`. Returns ("center", window),
        ("moved", pace), ("stopping", None) or ("recheck", None) after a bounded wait."""
        while True:
            if stopping.is_set():
                return ("stopping", None)
            pace, changes = self.current()
            if pace == (PAUSED, generation):
                self.wait_change(changes, stopping, timeout=OPEN_RETRY_DELAY)
                return ("recheck", None)
            if pace != (PACED, generation):
                return ("moved", pace)
            clock = self.snapshot()
            if clock is None:
                return ("moved", (ENDED, None))
            if clock.generation != generation:
                self.wait_change(changes, stopping, timeout=OPEN_RETRY_DELAY)
                return ("recheck", None)
            now = time.time_ns()
            try:
                wait = clock.wall_duration_until(now, center)
            except nx.ClientError as failure:
                if failure.kind != nx.FailureKind.TYPE:
                    raise
                self.wait_change(changes, stopping, timeout=OPEN_RETRY_DELAY)
                return ("recheck", None)
            if wait > 0:
                self.wait_change(changes, stopping, timeout=min(wait / 1e9, OPEN_RETRY_DELAY))
                return ("recheck", None)
            window = Window.at(clock, now)
            if window is None:
                return ("moved", (UNPACED, generation))
            return ("center", window)


def attach(session, domain):
    """Attaches to the active domain clock before the driver reads its state."""
    try:
        execution = session.prepare("ATTACH DOMAIN CLOCK;")
        outcome = session.execute(execution)
    except nx.ClientError as failure:
        raise ConfigurationError(
            f"cannot attach to the clock of domain '{domain}': {failure.message}"
        ) from failure
    if outcome.disposition != nx.Disposition.COMPLETED:
        raise ConfigurationError(f"cannot attach to the clock of domain '{domain}': {outcome.message}")


def detach(session):
    try:
        execution = session.prepare("DETACH DOMAIN CLOCK;")
        session.execute(execution)
    except nx.ClientError as failure:
        line(f"CLOCK detach_failed reason={failure.message}")


class TickGrid:
    """The tick centers of one paced generation: origin + n × period."""

    def __init__(self, generation, paced):
        self.generation = generation
        self.origin = paced.origin
        self.period = paced.period

    def reached(self, logical):
        """The newest tick center at or before the logical instant."""
        if logical < self.origin:
            return 0
        return (logical - self.origin) // self.period

    def center(self, tick):
        center = self.origin + tick * self.period
        if center >= 2**63:
            return None
        return center


# ---- Endpoints ---------------------------------------------------------------------------------

OPEN_RETRY_BUDGET = 30.0
OPEN_RETRY_DELAY = 0.2


class OpenIntent(Enum):
    CURRENT_GENERATION = "current generation"
    FOLLOWING_START = "following START"


def open_endpoint(open_call, endpoint, role, intent=OpenIntent.CURRENT_GENERATION):
    """Opens an endpoint, asking again while it is not running on its node yet."""
    deadline = time.monotonic() + OPEN_RETRY_BUDGET
    while True:
        try:
            return open_call()
        except nx.ClientError as failure:
            refusal = failure.open_refusal
            if refusal is None:
                raise ConfigurationError(
                    f"cannot open a {role} on {endpoint}: {failure.message}"
                ) from failure
            retryable = refusal == nx.OpenRefusal.ENDPOINT_UNAVAILABLE or (
                intent == OpenIntent.FOLLOWING_START
                and refusal == nx.OpenRefusal.DOMAIN_STOPPED
            )
            if retryable and time.monotonic() < deadline:
                time.sleep(OPEN_RETRY_DELAY)
                continue
            raise ConfigurationError(
                f"{endpoint} refused the {role}: {REFUSALS[refusal]}"
            ) from failure


def open_consumer(session, domain, emitter, declared):
    expected = fields(declared)
    return open_endpoint(
        lambda: session.subscribe_emitter(domain, emitter, expected, batches=4, bytes=1024 * 1024),
        f"emitter '{emitter}' of domain '{domain}'",
        "consumer",
    )


def open_producer(session, settings, intent=OpenIntent.CURRENT_GENERATION):
    expected = fields(READING_FIELDS)
    return open_endpoint(
        lambda: session.open_ingestor(
            settings.domain, settings.ingestor, expected,
            batches=settings.credit_batches, bytes=settings.credit_bytes),
        f"ingestor '{settings.ingestor}' of domain '{settings.domain}'",
        "producer",
        intent,
    )


def consumer_line(name, emitter, consumer):
    grant = consumer.grant
    return (f"CONSUMER opened consumer={name} emitter={emitter} generation={consumer.generation} "
            f"batches={grant.batches} bytes={grant.bytes}")


def producer_line(settings, producer):
    grant = producer.grant
    return (f"PRODUCER opened ingestor={settings.ingestor} generation={producer.generation} "
            f"batches={grant.batches} bytes={grant.bytes} max_batch_rows={grant.max_batch_rows} "
            f"max_batch_bytes={grant.max_batch_bytes}")


class Reopen:
    """The endpoint's typed reopen reason and its application reporting policy."""

    def __init__(self, reopen):
        self.reason = reopen.reason
        self.refusal = reopen.refusal

    @classmethod
    def of(cls, endpoint):
        reopen = endpoint.reopen
        if reopen is None:
            return None
        return cls(reopen)

    def waits_for_generation(self):
        return self.reason in (nx.ReopenReason.DOMAIN_STOPPED, nx.ReopenReason.GENERATION_CHANGED)

    def text(self):
        if self.reason == nx.ReopenReason.REFUSED:
            return f"refused ({REFUSALS[self.refusal]})"
        return REOPEN_REASONS[self.reason]


class Producer:
    """One native producer, its schema and the credit only its own submissions hold.

    Each outcome thread retains this owner, so replacing the run's producer cannot release
    credit against the replacement while that thread still uses the previous producer.
    """

    def __init__(self, endpoint):
        self.endpoint = endpoint
        self.schema = endpoint.schema
        self.outstanding = {"batches": 0, "bytes": 0}


class ConsumerLoop:
    """One consumer: reads deliveries, records their effects, and only then acknowledges them."""

    def __init__(self, run, name, emitter, kind, processing_time, leave_after):
        self.run = run
        self.name = name
        self.emitter = emitter
        self.kind = kind
        self.processing_time = processing_time
        self.leave_after = leave_after
        self.cancel = nx.Cancel()
        self.thread = None

    def start(self, consumer):
        self.thread = threading.Thread(target=self.loop, args=(consumer,), name=self.name,
                                       daemon=True)
        self.thread.start()

    def stop(self):
        self.cancel.trigger()

    def loop(self, consumer):
        generation = consumer.generation
        handled = 0
        while not self.run.stopped.is_set():
            try:
                delivery = consumer.next(cancel=self.cancel)
            except nx.ClientError as failure:
                if failure.kind in (nx.FailureKind.CANCELLED, nx.FailureKind.CLOSED):
                    break
                if failure.kind == nx.FailureKind.INTERRUPTED:
                    line(f"INTERRUPTED consumer={self.name} emitter={self.emitter}")
                    continue
                if failure.kind == nx.FailureKind.REOPEN_REQUIRED:
                    reason = Reopen.of(consumer)
                    if reason is None:
                        self.run.fail(ConfigurationError(
                            f"cannot reopen consumer '{self.name}': the binding gave no reason"
                        ))
                        break
                    line(f"CONSUMER reopen_required consumer={self.name} "
                         f"emitter={self.emitter} reason={reason.text()}")
                    reopened = self.reopen(generation, reason)
                    if reopened is None:
                        break
                    consumer = reopened
                    generation = consumer.generation
                    continue
                if failure.kind == nx.FailureKind.CONNECT:
                    line(f"CONSUMER unavailable consumer={self.name} emitter={self.emitter}")
                    if self.run.stopped.wait(1.0):
                        break
                    continue
                line(f"CONSUMER failed consumer={self.name} emitter={self.emitter} "
                     f"reason={failure.message}")
                break
            self.handle(delivery, generation)
            # A delivery owns its batch until it is collected, including across a settlement.
            del delivery
            handled += 1
            if self.leave_after is not None and handled >= self.leave_after:
                self.close(consumer)
                self.run.counters.add("consumers_left")
                line(f"CONSUMER left consumer={self.name} emitter={self.emitter} deliveries={handled}")
                return
        self.close(consumer)

    def reopen(self, generation, reason):
        """Accepts a changed contract now; a lifecycle ending waits for the next START."""
        while reason.waits_for_generation() and self.run.generation_now() <= generation:
            if not self.run.wait_generation(generation):
                return None
        try:
            consumer = open_consumer(self.run.session, self.run.settings.domain, self.emitter,
                                     OBSERVED_FIELDS if self.kind == "reading" else REJECTED_FIELDS)
        except ConfigurationError as failure:
            self.run.fail(failure)
            return None
        line(f"CONSUMER reopened consumer={self.name} emitter={self.emitter} "
             f"generation={consumer.generation}")
        return consumer

    def handle(self, delivery, generation):
        try:
            batch = delivery.batch
        except nx.ClientError as failure:
            line(f"DELIVERY undecodable consumer={self.name} emitter={self.emitter} "
                 f"reason={failure.message}")
            self.settle("REJECT", lambda: delivery.reject("the delivery does not decode"))
            return
        if self.kind == "reading":
            records = observed_readings(batch)
        else:
            records = rejection_notices(batch)
        line(f"PROCESSING consumer={self.name} emitter={self.emitter} rows={len(records)}")
        if self.processing_time > 0:
            time.sleep(self.processing_time / 1e9)
        applied = 0
        duplicates = 0
        try:
            for record in records:
                if self.kind == "reading":
                    effect = {"kind": "reading", "reading_id": record["reading_id"],
                              "sensor": record["sensor"], "tick": record["tick"],
                              "occurred_at": rfc3339(record["occurred_at"]),
                              "admitted_at": rfc3339(record["admitted_at"]),
                              "timestamp_source": record["timestamp_source"],
                              "value": record["value"], "generation": generation}
                else:
                    line(f"REJECTED reading_id={record['reading_id']} "
                         f"occurred_at={rfc3339(record['occurred_at'])} "
                         f"error_code={record['error_code']} "
                         f"error_message={record['error_message']}")
                    effect = {"kind": "rejection", "reading_id": record["reading_id"],
                              "occurred_at": rfc3339(record["occurred_at"]),
                              "error_code": record["error_code"],
                              "error_message": record["error_message"],
                              "generation": generation}
                if self.run.effects.apply(self.kind, effect):
                    applied += 1
                    self.run.counters.add("effects" if self.kind == "reading"
                                          else "rejection_notices")
                else:
                    duplicates += 1
                    self.run.counters.add("duplicates")
        except OSError as failure:
            # The effect was not recorded, so the delivery must come again: ask for a retry.
            error_line(f"cannot record an effect: {failure}")
            self.settle("RETRY", delivery.retry)
            return
        line(f"DELIVERY consumer={self.name} emitter={self.emitter} rows={len(records)} "
             f"applied={applied} duplicates={duplicates}")
        self.settle("ACK", delivery.ack)

    def settle(self, action, call):
        try:
            outcome = SETTLEMENTS[call()]
        except nx.ClientError as failure:
            if failure.kind == nx.FailureKind.UNCERTAIN:
                outcome = "unknown"
            elif failure.kind == nx.FailureKind.REJECTED:
                outcome = "expired"
            else:
                outcome = f"failed reason={failure.message}"
        line(f"{action} consumer={self.name} emitter={self.emitter} {outcome}")

    def close(self, consumer):
        try:
            consumer.close()
        except nx.ClientError as failure:
            line(f"CONSUMER close_failed consumer={self.name} emitter={self.emitter} "
                 f"reason={failure.message}")


# ---- One run -----------------------------------------------------------------------------------

INSTALL_BUDGET = 30.0
CLOSE_BUDGET = 30.0


def outcome_of(outcome):
    """The ledger's outcome and cause of a submission's terminal outcome."""
    if outcome.result == nx.SubmissionResult.COMPLETED:
        return "completed", ""
    if outcome.result == nx.SubmissionResult.NOT_ADMITTED:
        return "not_admitted", SUBMISSION_REFUSALS[outcome.refusal]
    if outcome.result == nx.SubmissionResult.PROCESSING_FAILED:
        return "processing_failed", PROCESSING_FAILURES[outcome.failure]
    return "outcome_unknown", UNCERTAINTIES[outcome.uncertainty]


def outcome_line(tick, readings, kind, cause):
    if cause:
        return f"OUTCOME tick={tick} readings={readings} {kind} {cause}"
    return f"OUTCOME tick={tick} readings={readings} {kind}"


class Run:
    """One run of the simulation: its session, its endpoints and its planner."""

    def __init__(self, settings):
        self.settings = settings
        self.counters = Counters()
        self.stopped = threading.Event()
        self.planning_stopped = threading.Event()
        self.planning = nx.Cancel()
        self.refusal_cancel = nx.Cancel()
        self.generation_lock = threading.Condition()
        self.generation = None
        self.session = None
        self.clock = None
        self.producer = None
        # The producer credit the run's submissions hold. The run waits for credit before it
        # checks the admission window and submits, so it never checks a batch's event times and
        # then holds the batch back while the window moves on.
        self.credit = threading.Condition()
        self.outcomes = []
        self.consumer_loops = []
        self.refused = None
        self.ledger = None
        self.effects = None
        # The readings of completed batches whose rejection notices the run waits for before it
        # stops its consumers: an error route acknowledges a reading once its notice is
        # published, before the rejection emitter delivers it.
        self.awaited_notices = []

    # The generation the consumers follow.

    def generation_now(self):
        with self.generation_lock:
            return self.generation

    def set_generation(self, generation):
        with self.generation_lock:
            self.generation = generation
            self.generation_lock.notify_all()

    def wait_generation(self, generation):
        with self.generation_lock:
            while self.generation <= generation and not self.stopped.is_set():
                self.generation_lock.wait(0.2)
        return not self.stopped.is_set()

    def stop_planning(self):
        self.planning_stopped.set()
        self.planning.trigger()

    def fail(self, failure):
        self.refused = failure
        self.refusal_cancel.trigger()
        self.stop_planning()

    def check_refusal(self):
        if self.refused is not None:
            self.stopped.set()
            for consumer_loop in self.consumer_loops:
                consumer_loop.stop()
            raise self.refused

    def connect(self):
        try:
            self.session = nx.Session.connect(
                self.settings.server, domain=self.settings.domain,
                username=self.settings.username, password=self.settings.password)
        except nx.ClientError as failure:
            raise ConfigurationError(
                f"cannot connect to '{self.settings.server}': {failure.message}"
            ) from failure

    def starting_generation(self):
        """The generation a run starts in: the attached clock's, once the node installed it."""
        deadline = time.monotonic() + INSTALL_BUDGET
        domain = self.settings.domain
        while True:
            pace, changes = self.clock.current()
            if pace[0] == PACED:
                return pace[1]
            if pace[0] == STOPPED:
                raise ConfigurationError(
                    f"the clock of domain '{domain}' is stopped at generation {pace[1]}; START "
                    f"the domain before running the simulation"
                )
            if pace[0] == UNPACED:
                raise ConfigurationError(
                    f"the clock of domain '{domain}' is unpaced at generation {pace[1]}; the "
                    f"simulation paces its readings by the tick centers of a paced domain"
                )
            if pace[0] == ENDED:
                raise ConfigurationError(
                    f"cannot attach to the clock of domain '{domain}': the domain was removed"
                )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ConfigurationError(
                    f"the clock of domain '{domain}' is not installed on the serving node within "
                    f"{duration_text(int(INSTALL_BUDGET * 1e9))}"
                )
            self.clock.wait_change(changes, self.stopped, timeout=remaining)

    def open_producer(self, intent=OpenIntent.CURRENT_GENERATION):
        return self.install_producer(Producer(open_producer(self.session, self.settings, intent)))

    def install_producer(self, producer):
        self.producer = producer
        line(producer_line(self.settings, producer.endpoint))
        return producer.endpoint.generation

    def close_producer(self):
        try:
            self.producer.endpoint.close(cancel=self.refusal_cancel)
        except nx.ClientError as failure:
            self.check_refusal()
            line(f"PRODUCER close_failed reason={failure.message}")

    def reopen(self, grid):
        """Accepts a changed contract with fresh credit, leaving pending outcomes with their owner."""
        reason = Reopen.of(self.producer.endpoint)
        if reason is None or reason.waits_for_generation():
            return grid
        self.close_producer()
        producer = Producer(open_producer(self.session, self.settings))
        opened = producer.endpoint.generation
        if opened != grid.generation:
            # START raced the open. The clock and --follow-generations still decide whether
            # this run may plan in that generation.
            del producer
            return self.follow((PACED, opened), grid)
        self.install_producer(producer)
        line(f"REOPENED generation={opened} ingestor={self.settings.ingestor} "
             f"reason={reason.text()}")
        return grid

    def start_consumers(self):
        rejections = ConsumerLoop(self, "rejections", self.settings.rejections, "rejection", 0,
                                  None)
        consumer = open_consumer(self.session, self.settings.domain, self.settings.rejections,
                                 REJECTED_FIELDS)
        line(consumer_line("rejections", self.settings.rejections, consumer))
        rejections.start(consumer)
        self.consumer_loops.append(rejections)
        loops = []
        for index in range(1, self.settings.consumers + 1):
            leave_after = (self.settings.consumer_leave_after
                           if index == self.settings.consumers else None)
            loops.append(ConsumerLoop(self, f"output-{index}", self.settings.emitter, "reading",
                                      self.settings.processing_time, leave_after))
        self.consumer_loops.extend(loops)
        if self.settings.consumer_delay == 0:
            for consumer_loop in loops:
                self.open_output(consumer_loop)
            return
        line(f"CONSUMER delayed emitter={self.settings.emitter} "
             f"for={duration_text(self.settings.consumer_delay)}")

        def open_later():
            if self.stopped.wait(self.settings.consumer_delay / 1e9):
                return
            for consumer_loop in loops:
                try:
                    self.open_output(consumer_loop)
                except ConfigurationError as failure:
                    self.fail(failure)
                    return

        threading.Thread(target=open_later, name="late-consumers", daemon=True).start()

    def open_output(self, consumer_loop):
        consumer = open_consumer(self.session, self.settings.domain, consumer_loop.emitter,
                                 OBSERVED_FIELDS)
        line(consumer_line(consumer_loop.name, consumer_loop.emitter, consumer))
        self.counters.add("consumers_joined")
        consumer_loop.start(consumer)

    # The planner.

    def first_tick(self, grid):
        clock = self.clock.paced(grid.generation)
        if clock is None:
            return 0
        return grid.reached(clock.logical_time_at(time.time_ns()))

    def simulate(self, grid):
        tick = self.first_tick(grid)
        planned = 0
        while planned < self.settings.ticks:
            generation = grid.generation
            grid = self.reopen(grid)
            if grid is None:
                return
            if grid.generation != generation:
                tick = self.first_tick(grid)
            center = grid.center(tick)
            if center is None:
                raise RuntimeError(f"the clock of domain '{self.settings.domain}' leaves the "
                                   f"range of timestamps")
            reached, value = self.clock.reach(grid.generation, center, self.planning_stopped)
            if reached == "stopping":
                return
            if reached == "recheck":
                continue
            if reached == "moved":
                grid = self.follow(value, grid)
                if grid is None:
                    return
                tick = self.first_tick(grid)
                continue
            window = value
            generation = grid.generation
            grid = self.reopen(grid)
            if grid is None:
                return
            if grid.generation != generation:
                tick = self.first_tick(grid)
                continue
            if self.producer.endpoint.admission == nx.Admission.SUSPENDED:
                self.planning_stopped.wait(OPEN_RETRY_DELAY)
                continue
            number = planned + 1
            prepared = self.prepare(tick, self.plan(grid.generation, tick, center, window, number))
            try:
                if not self.wait_for_credit(prepared["size"]):
                    return
                # The window moves on while a batch waits for credit, so it is read again now.
                clock = self.clock.paced(grid.generation)
                window = Window.at(clock, time.time_ns()) if clock is not None else None
                if window is None:
                    continue
                planned = number
                if self.settings.timestamps == "at" and not window.contains(center):
                    line(f"SKIPPED tick={tick} reason=behind_admission_window")
                    tick += 1
                    continue
                line(f"SUBMIT tick={tick} occurred_at={rfc3339(center)} "
                     f"window={rfc3339(window.earliest)}..{rfc3339(window.latest)}")
                self.send(prepared)
            finally:
                del prepared
            tick += 1

    def plan(self, generation, tick, center, window, planned):
        every = self.settings.invalid_every
        stale = None
        if every and planned % every == 0:
            stale = window.earliest - window.skew - 1
        readings = []
        for sensor in range(self.settings.sensors):
            for position in range(self.settings.burst):
                if stale is not None and sensor == 0 and position == 0:
                    readings.append(Reading(generation, tick, sensor, position, stale,
                                            "before_window"))
                else:
                    readings.append(Reading(generation, tick, sensor, position, center, "center"))
        return readings

    def prepare(self, tick, readings):
        """Encodes the readings of one tick as the producer's batch."""
        batch = build_batch(self.producer.schema, readings)
        size = len(batch.ipc)
        max_batch_bytes = self.producer.endpoint.grant.max_batch_bytes
        if size > max_batch_bytes:
            raise RuntimeError(f"cannot submit the readings of tick {tick}: its {size} bytes "
                               f"exceed the {max_batch_bytes} bytes one submission may carry")
        return {"tick": tick, "readings": readings, "batch": batch, "size": size}

    def wait_for_credit(self, size):
        """Waits until a batch of `size` bytes fits in the producer's credit beside the batches it
        holds. Returns False when the planning stops first."""
        grant = self.producer.endpoint.grant
        batches, granted = grant.batches, grant.bytes
        reported = False
        with self.credit:
            while True:
                if Reopen.of(self.producer.endpoint) is not None:
                    # This already-planned batch reaches send's definitely-unsent ledger path.
                    return True
                held = self.producer.outstanding
                if held["batches"] < batches and held["bytes"] + size <= granted:
                    return True
                if not reported:
                    self.counters.add("credit_waits")
                    line(f"WAITING credit outstanding_batches={held['batches']} "
                         f"outstanding_bytes={held['bytes']}")
                    reported = True
                if self.planning_stopped.is_set():
                    return False
                self.credit.wait(0.2)

    def send(self, prepared):
        """Records the readings in the ledger, submits them as one batch, and starts awaiting its
        outcome."""
        tick, readings, batch, size = (prepared["tick"], prepared["readings"], prepared["batch"],
                                       prepared["size"])
        reading_ids = [reading.reading_id for reading in readings]
        rejected_ids = [reading.reading_id for reading in readings
                        if self.settings.timestamps == "at" and reading.stamp == "before_window"]
        self.ledger.readings(readings, self.settings.ingestor, self.settings.timestamps)
        self.counters.add("readings", len(readings))
        while True:
            try:
                submission = self.producer.endpoint.submit(batch, cancel=self.planning)
                break
            except nx.ClientError as failure:
                if failure.kind == nx.FailureKind.CANCELLED:
                    # A submission that still waits for credit has sent nothing.
                    self.not_sent(tick, reading_ids, "not_sent")
                    return
                if failure.kind == nx.FailureKind.CONNECT:
                    line("PRODUCER unavailable retry_after=1s")
                    self.planning_stopped.wait(1.0)
                    continue
                if failure.kind in (nx.FailureKind.CLOSED, nx.FailureKind.REOPEN_REQUIRED):
                    reason = Reopen.of(self.producer.endpoint)
                    cause = ("not_sent" if reason is not None and not reason.waits_for_generation()
                             else "producer_ended")
                    self.not_sent(tick, reading_ids, cause)
                    return
                raise
        with self.credit:
            self.producer.outstanding["batches"] += 1
            self.producer.outstanding["bytes"] += size
            held = self.producer.outstanding["bytes"]
        self.counters.outstanding(held)
        line(f"SUBMITTED tick={tick} readings={len(readings)} bytes={size}")
        thread = threading.Thread(
            target=self.await_outcome,
            args=(self.producer, tick, reading_ids, rejected_ids, size,
                  submission),
            name=f"outcome-{tick}", daemon=True)
        thread.start()
        self.outcomes.append(thread)

    def not_sent(self, tick, reading_ids, cause):
        self.ledger.outcome(reading_ids, "not_admitted", cause)
        self.counters.outcome("not_admitted", len(reading_ids))
        line(outcome_line(tick, len(reading_ids), "not_admitted", cause))

    def await_outcome(self, producer, tick, reading_ids, rejected_ids, size,
                      submission):
        outcome = producer.endpoint.rejoin(submission)
        kind, cause = outcome_of(outcome)
        del outcome
        with self.credit:
            producer.outstanding["batches"] -= 1
            producer.outstanding["bytes"] -= size
            if kind == "completed":
                self.awaited_notices.extend(rejected_ids)
            self.credit.notify_all()
        try:
            self.ledger.outcome(reading_ids, kind, cause)
        except OSError as failure:
            error_line(f"cannot append to the ledger: {failure}")
        self.counters.outcome(kind, len(reading_ids))
        line(outcome_line(tick, len(reading_ids), kind, cause))

    def follow(self, pace, grid):
        """Follows the domain into a later START generation, when the run is asked to."""
        if not self.settings.follow_generations:
            line(f"STOPPING generation={grid.generation} reason=generation_ended")
            return None
        while True:
            if pace[0] == PACED and pace[1] > grid.generation:
                break
            if pace[0] in (ENDED, UNPACED):
                line(f"STOPPING generation={grid.generation} reason=domain_unavailable")
                return None
            _, changes = self.clock.current()
            if not self.clock.wait_change(changes, self.planning_stopped):
                return None
            pace, _ = self.clock.current()
        target = pace[1]
        self.close_producer()
        opened = self.open_producer(OpenIntent.FOLLOWING_START)
        deadline = time.monotonic() + INSTALL_BUDGET
        while True:
            clock = self.clock.paced(opened)
            if clock is not None:
                paced = clock.paced
                break
            _, changes = self.clock.current()
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not self.clock.wait_change(changes, self.planning_stopped,
                                                            timeout=remaining):
                line(f"STOPPING generation={opened} reason=clock_not_observed")
                return None
        grid = TickGrid(opened, paced)
        self.set_generation(opened)
        self.ledger.generation(opened)
        self.counters.generation(opened)
        line(f"REOPENED generation={opened} ingestor={self.settings.ingestor} after={target}")
        return grid

    def replay(self, unresolved, grid):
        """Resubmits the ledger's readings without a completed outcome, when they belong to the
        generation the run is in and the window still admits them."""
        generation = grid.generation
        by_tick = {}
        for reading in unresolved:
            if reading.generation != grid.generation:
                line(f"REPLAY skipped reading_id={reading.reading_id} "
                     f"generation={reading.generation} current={grid.generation}")
                continue
            by_tick.setdefault(reading.tick, []).append(reading)
        for tick in sorted(by_tick):
            grid = self.reopen(grid)
            if grid is None or grid.generation != generation:
                return
            admissible = self.admissible(by_tick[tick], grid)
            if not admissible:
                continue
            prepared = self.prepare(tick, admissible)
            try:
                if not self.wait_for_credit(prepared["size"]):
                    return
                # The window moves on while a batch waits for credit, so it is checked again now.
                admissible = self.admissible(prepared["readings"], grid)
                if not admissible:
                    continue
                if len(admissible) != len(prepared["readings"]):
                    del prepared["batch"]
                    prepared = self.prepare(tick, admissible)
                line(f"REPLAY tick={tick} readings={len(prepared['readings'])}")
                self.send(prepared)
            finally:
                del prepared

    def admissible(self, readings, grid):
        """The readings a replay may still submit: under TIMESTAMP AT, those whose event time the
        window admits now. The others are reported, and stay unresolved in the ledger."""
        clock = self.clock.paced(grid.generation)
        window = Window.at(clock, time.time_ns()) if clock is not None else None
        admissible = []
        for reading in readings:
            if self.settings.timestamps == "now":
                admitted = True
            elif window is None:
                admitted = False
            else:
                admitted = window.contains(reading.occurred_at)
            if admitted:
                admissible.append(reading)
                continue
            line(f"REPLAY expired reading_id={reading.reading_id} "
                 f"occurred_at={rfc3339(reading.occurred_at)}")
        return admissible

    def settle_outcomes(self):
        """Waits for every outcome still outstanding, and then for the rejection notices of the
        completed batches, up to the deadline. Returns how many outcomes never came."""
        until = time.monotonic() + self.settings.deadline / 1e9
        missing = 0
        for thread in self.outcomes:
            while thread.is_alive():
                self.check_refusal()
                remaining = until - time.monotonic()
                if remaining <= 0:
                    missing += 1
                    break
                thread.join(min(remaining, OPEN_RETRY_DELAY))
        with self.credit:
            awaited = list(self.awaited_notices)
            self.awaited_notices.clear()
        self.effects.await_rejections(awaited, until, lambda: self.refused is not None)
        self.check_refusal()
        return missing

    def inspect(self):
        """Inspects the ingestor and the output emitter on the session at an interval."""
        every = self.settings.inspect_every / 1e9
        while not self.stopped.wait(every):
            listing = self.execute("SHOW INGESTORS;")
            if listing is None:
                continue
            prefix = f"{self.settings.ingestor} "
            for entry in listing.splitlines():
                if entry.startswith(prefix):
                    line(f"INSPECT {entry}")
            description = self.execute(f"DESCRIBE EMITTER {self.settings.emitter};")
            if description is None:
                continue
            values = {"consumers: ": "-", "retained batches: ": "-"}
            for entry in description.splitlines():
                for label in values:
                    if entry.strip().startswith(label) and values[label] == "-":
                        values[label] = entry.strip()[len(label):]
            line(f"INSPECT emitter={self.settings.emitter} consumers={values['consumers: ']} "
                 f"retained_batches={values['retained batches: ']}")
            self.counters.add("inspections")

    def execute(self, query):
        try:
            execution = self.session.prepare(query)
            return self.session.execute(execution).message
        except nx.ClientError as failure:
            line(f"INSPECT failed reason={failure.message}")
            return None

    def execute_run(self):
        """Runs the simulation, from connecting to the summary. Returns the exit status."""
        self.connect()
        line(f"CONNECTED server={self.settings.server} domain={self.settings.domain}")
        attach(self.session, self.settings.domain)
        snapshot = self.session.domain_clock(self.settings.domain)
        if snapshot is None:
            raise ConfigurationError(
                f"cannot attach to the clock of domain '{self.settings.domain}': the session "
                f"follows no clock of it"
            )
        state = snapshot.state
        paced = snapshot.paced if state == nx.ClockState.PACED else None
        line(clock_line(snapshot.generation, state, paced))
        self.clock = Clock(self.session, self.settings.domain, self.counters,
                           pace_of(snapshot.generation, state))
        self.clock.start()
        generation = self.starting_generation()
        self.effects = EffectStore(self.settings.effects)
        unresolved = Ledger.unresolved(self.settings.ledger) if self.settings.replay else []
        self.ledger = Ledger(self.settings.ledger)
        self.set_generation(generation)
        # Every consumer starts before the producer submits anything.
        self.start_consumers()
        opened = self.open_producer()
        clock = self.clock.paced(generation)
        if clock is None:
            raise ConfigurationError(
                f"the clock of domain '{self.settings.domain}' changed while the simulation "
                f"started; run the simulation again"
            )
        grid = TickGrid(generation, clock.paced)
        inspector = None
        if self.settings.inspect_every is not None:
            inspector = threading.Thread(target=self.inspect, name="inspector", daemon=True)
            inspector.start()
        continuing = True
        if opened != generation:
            # The producer may belong to a later generation than the attached clock: follow the
            # clock into it before planning anything.
            grid = self.follow((PACED, opened), grid)
            continuing = grid is not None
        current = grid.generation if grid is not None else generation
        self.ledger.generation(current)
        self.counters.generation(current)
        line(f"READY generation={current}")
        if continuing:
            self.replay(unresolved, grid)
            self.simulate(grid)
        self.check_refusal()
        missing = self.settle_outcomes()
        self.close_producer()
        self.stopped.set()
        for consumer_loop in self.consumer_loops:
            consumer_loop.stop()
        deadline = time.monotonic() + CLOSE_BUDGET
        for consumer_loop in self.consumer_loops:
            if consumer_loop.thread is None:
                continue
            consumer_loop.thread.join(max(0.0, deadline - time.monotonic()))
            if consumer_loop.thread.is_alive():
                line("CONSUMER close_timed_out")
        self.clock.stop()
        if inspector is not None:
            inspector.join()
        detach(self.session)
        line(self.counters.summary())
        if self.refused is not None:
            error_line(str(self.refused))
            return 2
        if missing:
            error_line(f"{missing} submissions still had no outcome "
                       f"{duration_text(self.settings.deadline)} after the run stopped submitting")
            return 1
        return 0 if self.counters.all_completed() else 3


def main(arguments):
    try:
        settings = options(arguments)
    except ConfigurationError as failure:
        error_line(str(failure))
        return 2
    run = Run(settings)

    def interrupt(signum, frame):
        line("STOPPING reason=interrupt")
        run.stop_planning()

    signal.signal(signal.SIGINT, interrupt)
    status = {}

    def body():
        try:
            status["code"] = run.execute_run()
        except ConfigurationError as failure:
            error_line(str(failure))
            status["code"] = 2
        except (nx.ClientError, OSError, RuntimeError) as failure:
            error_line(str(failure))
            status["code"] = 1

    worker = threading.Thread(target=body, name="simulation")
    worker.start()
    # The main thread only waits, so an interrupt reaches the handler at once.
    while worker.is_alive():
        worker.join(0.2)
    return status.get("code", 1)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
