"""The CPython probe of the Nervix client's Python package, `nervix_client`.

It prints the same conformance report as every other probe. Bulk data stays borrowed: an event's
frame is a memoryview the package exports, which keeps the event alive for as long as the view
exists, and every column is read in one call. Every call that waits releases the GIL, so a blocked
wait never stops another Python thread from cancelling it. Run with the `clock` argument, it
attaches to the domain's running clock instead, reads the clock the attach reported before its
first tick, follows the generation a STOP and START begin and the attachment restored after its
session ends, and detaches. Run with the `io` argument, it publishes typed batches through a client
ingestor and reads, retries, rejects and acknowledges their output through a client emitter, across
the session the scenario cuts; Arrow data built by pyarrow is published and read through the Arrow
PyCapsule interface on the way.
"""

import gc
import json
import os
from pathlib import Path
import struct
import sys
import threading
import time

import nervix_client as nx


def lower(value):
    """The report's spelling of an enumeration member: its name in lower case."""
    return repr(value).rsplit(".", 1)[1].lower()


def upper(value):
    """The report's spelling of a type: the member's name."""
    return repr(value).rsplit(".", 1)[1]


def bits32(value):
    return struct.unpack("<I", struct.pack("<f", value))[0]


def bits64(value):
    return struct.unpack("<Q", struct.pack("<d", value))[0]


# How each scalar type prints its value. Floats print their bits.
RENDER = {
    "U8": lambda value: f"u8:{value}",
    "I8": lambda value: f"i8:{value}",
    "U16": lambda value: f"u16:{value}",
    "I16": lambda value: f"i16:{value}",
    "U32": lambda value: f"u32:{value}",
    "I32": lambda value: f"i32:{value}",
    "U64": lambda value: f"u64:{value}",
    "I64": lambda value: f"i64:{value}",
    "F32": lambda value: f"f32:{bits32(value):08x}",
    "F64": lambda value: f"f64:{bits64(value):016x}",
    "BOOL": lambda value: f"bool:{'true' if value else 'false'}",
    "DATETIME": lambda value: f"datetime:{value}",
    "STRING": lambda value: f"str:{value.encode().hex()}",
    "BYTES": lambda value: f"bytes:{value.hex()}",
}

# The struct format of each fixed-width scalar, in native byte order.
PACKING = {
    "U8": "=B", "I8": "=b", "U16": "=H", "I16": "=h", "U32": "=I", "I32": "=i", "U64": "=Q",
    "I64": "=q", "F32": "=f", "F64": "=d", "BOOL": "=?", "DATETIME": "=q",
}


def raised(call):
    """The exception `call` raises, which it has to."""
    try:
        call()
    except nx.ClientError as failure:
        return failure
    raise RuntimeError("a call succeeded where it had to fail")


def execute(session, query, cancel=None):
    return session.execute(session.prepare(query), cancel=cancel)


def error_line(outcome):
    diagnostics = outcome.diagnostics
    span = "none"
    if diagnostics and diagnostics[0].span is not None:
        start, end = diagnostics[0].span
        span = f"{start}..{end}"
    return f"ERROR {lower(outcome.disposition)} diagnostics={len(diagnostics)} span={span}"


def described(fields):
    return [
        {
            "name": field.name,
            "type": upper(field.type),
            "nullable": field.nullable,
            "sensitive": field.sensitive,
        }
        for field in fields
    ]


def field_line(prefix, field):
    nullable = "nullable" if field["nullable"] else "required"
    sensitive = "sensitive" if field["sensitive"] else "public"
    return f"{prefix} {field['name']} {field['type']} {nullable} {sensitive}"


def event_column(event, part, index, field):
    """One column of a rows event, read as values in one call and checked against the same values
    copied in bulk, rendered row by row."""
    kind = field["type"]
    states = event.states(index, part)
    values = event.column(index, part)
    if kind in ("STRING", "BYTES"):
        offsets, data = event.varlen(index, part)
        for row, (state, value) in enumerate(zip(states, values)):
            copied = data[offsets[row]:offsets[row + 1]]
            if state == nx.CellState.VALUE:
                read = value.encode() if kind == "STRING" else value
                if read != copied:
                    raise RuntimeError("a value differs from the same value copied in bulk")
    elif kind in PACKING:
        fixed = event.fixed(index, part)
        packing = PACKING[kind]
        width = struct.calcsize(packing)
        for row, (state, value) in enumerate(zip(states, values)):
            copied = struct.unpack(packing, fixed[row * width:(row + 1) * width])[0]
            if state == nx.CellState.VALUE and copied != value and not (
                kind in ("F32", "F64") and bits64(copied) == bits64(value)
            ):
                raise RuntimeError("a value differs from the same value copied in bulk")
    else:
        raise RuntimeError("list columns are read as lists")
    cells = []
    for state, value in zip(states, values):
        if state == nx.CellState.NULL:
            cells.append("null")
        elif state == nx.CellState.REDACTED:
            if value != nx.REDACTED:
                raise RuntimeError("a redacted cell reads as a value")
            cells.append("redacted")
        else:
            cells.append(RENDER[kind](value))
    return cells


def render(event, part, fields, cells):
    rows = [[] for _ in range(cells)]
    for index, field in enumerate(fields):
        for row, rendered in enumerate(event_column(event, part, index, field)):
            rows[row].append(f"{field['name']}={rendered}")
    return [" ".join(row) for row in rows]


def row_lines(event, fields, key_fields):
    key = render(event, nx.Part.BRANCH_KEY, key_fields, 1)[0] if key_fields else ""
    return [f"ROW [{key}] {row}" for row in render(event, nx.Part.ROWS, fields, event.row_count)]


def check_retention(retained, reported, fields, key_fields):
    """Rereads retained events through borrowed frames across collections and threads."""
    views = [event.frame for event in retained]
    snapshots = [bytes(view) for view in views]
    del retained[:]
    gc.collect()
    churn = [bytearray(size * 64) for size in range(2048)]
    del churn
    gc.collect()
    for view, snapshot in zip(views, snapshots):
        if bytes(view) != snapshot or bytes(view[4:8]) != b"NXSM":
            raise RuntimeError("a borrowed frame changed while only its view kept it alive")
    owners = [view.obj for view in views]
    for owner, lines in zip(owners, reported):
        if not isinstance(owner, nx.Event) or row_lines(owner, fields, key_fields) != lines:
            raise RuntimeError("a retained event reads differently than it did")
    # Release the last references on another thread, while the main thread keeps collecting.
    releaser = threading.Thread(target=lambda: (owners.clear(), views.clear(), gc.collect()))
    releaser.start()
    gc.collect()
    releaser.join()


def profile_binding(event, fields, output_dir):
    """Record raw host costs of the package on a live Row event without changing the report."""
    count = event.row_count
    frame_bytes = len(event.frame)
    samples = 1000

    def measure(call):
        for _ in range(20):
            call()
        values = []
        for _ in range(samples):
            started = time.perf_counter_ns()
            call()
            values.append(time.perf_counter_ns() - started)
        return values

    fixed_index = next(index for index, field in enumerate(fields) if field["type"] == "I64")
    text_index = next(index for index, field in enumerate(fields) if field["type"] == "STRING")
    measurements = {
        "borrowed_frame_view": measure(lambda: event.frame),
        "owned_frame_copy": measure(lambda: bytes(event.frame)),
        "fixed_column": measure(lambda: event.fixed(fixed_index)),
        "varlen_column": measure(lambda: event.varlen(text_index)),
        "typed_column": measure(lambda: event.column(text_index)),
        "rows_batch": measure(event.rows),
    }
    disposal = []
    for _ in range(20):
        started = time.perf_counter_ns()
        views = [event.frame for _ in range(256)]
        del views
        gc.collect()
        disposal.append(time.perf_counter_ns() - started)
    report = {
        "schema_version": 2,
        "runtime": sys.version,
        "row_count": count,
        "frame_bytes": frame_bytes,
        "samples_per_stage": samples,
        "samples_nanoseconds": measurements,
        "gc_release_256_views_nanoseconds": disposal,
        "gc_thresholds": gc.get_threshold(),
    }
    os.makedirs(output_dir, exist_ok=True)
    output = os.path.join(output_dir, f"python-{os.getpid()}.json")
    with open(output, "w", encoding="utf-8") as target:
        json.dump(report, target, indent=2)


def check_cancellation(session):
    cancel = nx.Cancel()
    outcome = {}

    def wait():
        outcome["failure"] = raised(lambda: session.next_event(cancel=cancel))

    waiter = threading.Thread(target=wait)
    waiter.start()
    time.sleep(0.1)
    cancel.trigger()
    waiter.join(30)
    if waiter.is_alive() or not isinstance(outcome["failure"], nx.CancelledError):
        raise RuntimeError("a cancelled wait did not report CANCELLED")
    expired = raised(lambda: session.next_event(cancel=nx.Cancel(deadline=0.05)))
    if not isinstance(expired, nx.DeadlineError) or expired.kind != nx.FailureKind.DEADLINE:
        raise RuntimeError("an expired wait did not report DEADLINE")
    cancelled = nx.Cancel()
    cancelled.trigger()
    failure = raised(lambda: execute(session, "SHOW DOMAINS;", cancelled))
    if not isinstance(failure, nx.CancelledError) or not failure.execution_reference:
        raise RuntimeError("a cancelled command did not report CANCELLED with its reference")


def paced_line(prefix, clock, domain):
    """The report line of a paced clock, with `prefix` naming where it was read. The UTC anchor
    depends on when the scenario's START committed, so it is read but not reported."""
    return (
        f"{prefix} domain={domain} generation={clock.generation} state=paced "
        f"period={clock.period} skew={clock.skew} origin={clock.origin} "
        f"rate=f64:{bits64(clock.rate):016x}"
    )


def check_tick(clock, tick):
    """Holds a tick to a clock: the same generation, a boundary of the logical origin plus one
    period for every id before it, and a serving node's reading that never precedes the origin."""
    if tick.generation != clock.generation:
        raise RuntimeError("a tick of one generation followed the state of another")
    if tick.id == 0 or tick.boundary != clock.origin + (tick.id - 1) * clock.period:
        raise RuntimeError(
            "a tick's boundary is not the origin plus one period for every id before it"
        )
    if tick.serving_logical < clock.origin:
        raise RuntimeError("the serving node's logical reading precedes the logical origin")


def tick_line(clock, tick, domain):
    """The report line of a tick a clock holds."""
    check_tick(clock, tick)
    return f"TICK domain={domain} generation={tick.generation} boundary=origin+(id-1)*period"


def projection_line(clock, read, domain):
    """The report line of the projections of `read`, which holds `clock`, at its own UTC anchor: the
    logical time there, the wait for the next tick center, the admission window, and whether an
    event at the skew's edge and one nanosecond past it are admitted."""
    anchor, origin = clock.anchor, clock.origin

    def relative(instant):
        return "origin" if instant == origin else str(instant)

    def admission(admitted):
        return "admitted" if admitted else "refused"

    window = read.admission_window(anchor)
    if window is None:
        raise RuntimeError("a paced clock reports no admission window")
    edge = origin + clock.skew
    return (
        f"PROJECTION domain={domain} generation={clock.generation} "
        f"anchor={relative(read.logical_time_at(anchor))} "
        f"wait={read.wall_duration_until(anchor, origin + clock.period)} "
        f"window={relative(window[0])}..{relative(window[1])} "
        f"skew={admission(read.admits(anchor, edge))} "
        f"beyond={admission(read.admits(anchor, edge + 1))}"
    )


def next_clock_event(session, cancel, domain):
    event = session.next_clock_event(cancel=cancel)
    if event.domain != domain:
        raise RuntimeError("a clock event arrived for another domain")
    return event


def check_clock_cancellation(session, domain):
    """Cancels a clock wait from another thread, then lets a deadline end another. The session
    follows no clock yet, so nothing but its token ends either wait."""
    cancel = nx.Cancel()
    outcome = {}

    def wait():
        outcome["failure"] = raised(lambda: next_clock_event(session, cancel, domain))

    waiter = threading.Thread(target=wait)
    waiter.start()
    time.sleep(0.1)
    cancel.trigger()
    waiter.join(30)
    if waiter.is_alive() or not isinstance(outcome["failure"], nx.CancelledError):
        raise RuntimeError("a cancelled clock wait did not report CANCELLED")
    expired = raised(lambda: next_clock_event(session, nx.Cancel(deadline=0.05), domain))
    if not isinstance(expired, nx.DeadlineError):
        raise RuntimeError("an expired clock wait did not report DEADLINE")


class FollowedClock:
    """What the probe has read about the domain's clock: the generation of the newest state, its
    mapping while it is paced, and whether the session holding the attachment ended since. Every
    event is held to what was read before it, and a read of the clock taken right after it is held
    to be no older."""

    def __init__(self, session, domain, clock):
        self.session = session
        self.domain = domain
        self.generation = clock.generation
        self.paced = clock
        self.interrupted = False

    def clock(self):
        if self.paced is None:
            raise RuntimeError("the followed clock is not paced")
        return self.paced

    def read(self):
        read = self.session.domain_clock(self.domain)
        if read is None:
            raise RuntimeError("the session follows no clock of the domain after an event about it")
        return read

    def next(self, deadline):
        """The next event about the domain, held to what the probe read before it."""
        event = next_clock_event(self.session, deadline, self.domain)
        kind = event.kind
        if kind == nx.ClockEventKind.STATE:
            self.observe(event)
        elif kind == nx.ClockEventKind.TICK:
            self.check_tick(event)
        elif kind == nx.ClockEventKind.INTERRUPTED:
            self.interrupted = True
        elif kind == nx.ClockEventKind.ENDED:
            raise RuntimeError("the server ended the attachment")
        elif kind == nx.ClockEventKind.RESTORATION_FAILED:
            failure = event.restoration_failure
            if failure is None or failure.retry_after_nanos <= 0:
                raise RuntimeError("a refused restoration carries no message and wait")
        return event

    def observe(self, event):
        generation = event.generation
        if generation < self.generation:
            raise RuntimeError("a state went back to an earlier generation")
        state = event.state
        paced = event.paced if state == nx.ClockState.PACED else None
        read = self.read()
        if read.generation < generation:
            raise RuntimeError("a read of the clock is older than the state the probe took")
        if read.generation == generation:
            if read.state != state:
                raise RuntimeError("a read of the clock differs from the state of its generation")
            if paced is not None and read.paced != paced:
                raise RuntimeError("a read of the clock differs from the mapping of its generation")
        self.generation = generation
        self.paced = paced
        self.interrupted = False

    def check_tick(self, event):
        if self.interrupted:
            raise RuntimeError("a tick arrived before the restored attachment reported its clock")
        tick = event.tick
        check_tick(self.clock(), tick)
        read = self.read()
        if read.generation < tick.generation:
            raise RuntimeError("a read of the clock is older than the tick the probe took")
        held = read.tick
        if read.generation == tick.generation and held is not None and held.id < tick.id:
            raise RuntimeError("a read of the clock holds an older tick than the probe took")

    def first_tick(self, deadline):
        """The first tick of the followed generation. A state reporting that generation again is
        taken on the way; one of another generation fails the probe."""
        generation = self.generation
        while True:
            event = self.next(deadline)
            if event.kind == nx.ClockEventKind.TICK:
                return event
            if event.kind != nx.ClockEventKind.STATE or self.generation != generation:
                raise RuntimeError(
                    f"the clock reported {upper(event.kind)} before the first tick of generation "
                    f"{generation}"
                )

    def next_generation(self, deadline):
        """The paced state of a generation after the followed one. The followed generation's
        ticks and the states before the new paced one are taken on the way."""
        previous = self.generation
        while True:
            event = self.next(deadline)
            kind = event.kind
            if kind == nx.ClockEventKind.STATE and self.generation > previous and self.paced:
                return event
            if kind not in (nx.ClockEventKind.TICK, nx.ClockEventKind.STATE):
                raise RuntimeError(
                    f"the clock reported {upper(kind)} before a generation after {previous}"
                )

    def interruption(self, deadline):
        """Waits for the interruption of the attachment. The followed generation's ticks and
        states are taken on the way."""
        generation = self.generation
        while True:
            event = self.next(deadline)
            kind = event.kind
            if kind == nx.ClockEventKind.INTERRUPTED:
                return
            if kind != nx.ClockEventKind.TICK and (
                kind != nx.ClockEventKind.STATE or self.generation != generation
            ):
                raise RuntimeError(f"the clock reported {upper(kind)} before the interruption")

    def restored(self, deadline):
        """The paced state the restored attachment reports. A refused restoration, which the
        session repeats, and a clock reported uninstalled are taken on the way."""
        while True:
            event = self.next(deadline)
            if event.kind == nx.ClockEventKind.STATE and self.paced is not None:
                return event


def report_state_and_first_tick(followed, state, deadline, domain, report):
    """Reports the paced state a STATE event reports and the first tick after it."""
    report(paced_line("STATE", state.paced, domain))
    tick = followed.first_tick(deadline)
    report(tick_line(followed.clock(), tick.tick, domain))


def run_clock(session, domain, report):
    """Attaches to the domain's running clock and reads the clock the attach reported before its
    first tick, then follows the generation the scenario's STOP and START begin and the attachment
    restored after the scenario ends the session, and detaches."""
    check_clock_cancellation(session, domain)
    report(f"ATTACHED {lower(execute(session, 'ATTACH DOMAIN CLOCK;').disposition)}")

    # The clock the attach reported, read before any event about the attachment.
    read = session.domain_clock(domain)
    if read is None:
        raise RuntimeError("the session follows no clock after its attach completed")
    if read.state != nx.ClockState.PACED:
        raise RuntimeError("the attach reported a clock other than the running paced one")
    clock = read.paced
    reported_clock = paced_line("CLOCK", clock, domain)
    report(reported_clock)
    report(projection_line(clock, read, domain))

    followed = FollowedClock(session, domain, clock)
    tick = followed.first_tick(nx.Cancel(deadline=120))
    reported_tick = tick_line(clock, tick.tick, domain)
    report(reported_tick)

    # Keep a second reference to the read and the tick and let the first ones go on another
    # thread, so both must read the same on the second alone.
    retained_read, retained_tick = read, tick
    firsts = [read, tick]
    del read, tick
    releaser = threading.Thread(target=lambda: (firsts.clear(), gc.collect()))
    releaser.start()
    releaser.join()
    clock_again = retained_read.paced
    if (
        paced_line("CLOCK", clock_again, domain) != reported_clock
        or tick_line(clock_again, retained_tick.tick, domain) != reported_tick
    ):
        raise RuntimeError("a retained clock or tick reads differently than it did")

    # The scenario stops the domain and starts it again at another origin and rate.
    deadline = nx.Cancel(deadline=120)
    report_state_and_first_tick(followed, followed.next_generation(deadline), deadline, domain, report)

    # The scenario ends the session, and the package attaches the clock again on the next one.
    deadline = nx.Cancel(deadline=120)
    followed.interruption(deadline)
    report(f"INTERRUPTED domain={domain}")
    report_state_and_first_tick(followed, followed.restored(deadline), deadline, domain, report)

    report(f"DETACHED {lower(execute(session, 'DETACH DOMAIN CLOCK;').disposition)}")
    report("CHECKS ok")
    report("PASS")


# ---- Producers and consumers -----------------------------------------------------------------

WAIT_SECONDS = 120
EXPIRING_SECONDS = 0.2
PRODUCER_BATCHES = 2
CONSUMER_BATCHES = 4
ENDPOINT_BYTES = 1_048_576
SCRIBBLE = 0xAA

# The input schema of the probe's ingestor, in declared order: each field's name, the (type,
# length) of every level of its type, and whether it is nullable and sensitive.
INPUT_FIELDS = [
    ("id", [("U32", 0)], False, False),
    ("tenant", [("STRING", 0)], False, False),
    ("u8v", [("U8", 0)], False, False),
    ("i8v", [("I8", 0)], False, False),
    ("u16v", [("U16", 0)], False, False),
    ("i16v", [("I16", 0)], False, False),
    ("u32v", [("U32", 0)], False, False),
    ("i32v", [("I32", 0)], False, False),
    ("u64v", [("U64", 0)], False, False),
    ("i64v", [("I64", 0)], False, False),
    ("f32v", [("F32", 0)], False, False),
    ("f64v", [("F64", 0)], False, False),
    ("flag", [("BOOL", 0)], False, False),
    ("text", [("STRING", 0)], True, False),
    ("raw", [("BYTES", 0)], True, False),
    ("at", [("DATETIME", 0)], False, False),
    ("maybe", [("I64", 0)], True, False),
    ("tags", [("LIST", 0), ("STRING", 0)], False, False),
    ("grid", [("FIXED_LIST", 2), ("FIXED_LIST", 2), ("I16", 0)], False, False),
    ("spans", [("LIST", 0), ("FIXED_LIST", 2), ("DATETIME", 0)], True, False),
    ("secret", [("STRING", 0)], False, True),
]
ECHO_FIELD = ("echo", [("U32", 0)], False, False)

I64_MIN, I64_MAX = -(1 << 63), (1 << 63) - 1


def plain_row(identifier, tenant):
    """A row of `tenant` whose every other value is the zero of its type."""
    return {
        "id": identifier, "tenant": tenant, "u8v": 0, "i8v": 0, "u16v": 0, "i16v": 0, "u32v": 0,
        "i32v": 0, "u64v": 0, "i64v": 0, "f32v": 0, "f64v": 0, "flag": False, "text": None,
        "raw": None, "at": 0, "maybe": None, "tags": [], "grid": [[0, 0], [0, 0]], "spans": None,
        "secret": "s",
    }


def typed_rows():
    """The typed rows of the first batch. Floats are given as their bits."""
    return [
        {
            "id": 1, "tenant": "acme", "u8v": 255, "i8v": 127, "u16v": 65535, "i16v": 32767,
            "u32v": 4294967295, "i32v": 2147483647, "u64v": (1 << 64) - 1, "i64v": I64_MAX,
            "f32v": 0x7F7FFFFF, "f64v": 0x7FEFFFFFFFFFFFFF, "flag": True,
            "text": "héllo 世界 \U0001f680".encode(), "raw": b"\x00\xff\xfe\x80\x00",
            "at": I64_MAX, "maybe": None, "tags": ["a", "", "héllo"],
            "grid": [[1, -2], [32767, -32768]], "spans": [[I64_MIN, I64_MAX]], "secret": "s1",
        },
        {
            "id": 2, "tenant": "acme", "u8v": 0, "i8v": -128, "u16v": 0, "i16v": -32768,
            "u32v": 0, "i32v": -2147483648, "u64v": 0, "i64v": I64_MIN, "f32v": 0x80000000,
            "f64v": 0x0000000000000001, "flag": False, "text": b"a\x00b", "raw": b"",
            "at": I64_MIN, "maybe": 0, "tags": [], "grid": [[0, 0], [0, 0]], "spans": None,
            "secret": "s2",
        },
        {
            "id": 3, "tenant": "acme", "u8v": 1, "i8v": -1, "u16v": 1, "i16v": -1, "u32v": 1,
            "i32v": -1, "u64v": 9007199254740993, "i64v": -9007199254740993,
            "f32v": 0x3FC00000, "f64v": 0x3FB999999999999A, "flag": True, "text": None,
            "raw": None, "at": 1, "maybe": 9007199254740992, "tags": ["x"],
            "grid": [[-1, 1], [2, -2]], "spans": [], "secret": "s3",
        },
    ]


# The struct format each fixed-width column of the input is written in, in native byte order.
# Floats are written as their bits.
WRITTEN = {
    "id": "=I", "u8v": "=B", "i8v": "=b", "u16v": "=H", "i16v": "=h", "u32v": "=I",
    "i32v": "=i", "u64v": "=Q", "i64v": "=q", "f32v": "=I", "f64v": "=Q", "flag": "=B",
    "at": "=q",
}


def host_column(name, rows):
    """The column `name` of `rows`, as a host lays it out: whether each row holds a value or None,
    the offsets of its level-0 list or None, the level its values fill, and either fixed bytes or
    offsets and data."""
    column = {"present": None, "offsets": None, "leaf": 0, "fixed": None, "varlen": None}

    def varlen(values):
        offsets, data = [0], bytearray()
        for value in values:
            data += value
            offsets.append(len(data))
        column["varlen"] = (offsets, bytes(data))

    source = "id" if name == "echo" else name
    if source in WRITTEN:
        packing = WRITTEN[source]
        column["fixed"] = b"".join(struct.pack(packing, int(row[source])) for row in rows)
    elif name in ("tenant", "secret"):
        varlen(row[name].encode() for row in rows)
    elif name in ("text", "raw"):
        column["present"] = [row[name] is not None for row in rows]
        varlen(row[name] or b"" for row in rows)
    elif name == "maybe":
        column["present"] = [row["maybe"] is not None for row in rows]
        column["fixed"] = b"".join(struct.pack("=q", row["maybe"] or 0) for row in rows)
    elif name == "tags":
        column["leaf"] = 1
        lists = [0]
        for row in rows:
            lists.append(lists[-1] + len(row["tags"]))
        column["offsets"] = lists
        varlen(tag.encode() for row in rows for tag in row["tags"])
    elif name == "grid":
        column["leaf"] = 2
        column["fixed"] = b"".join(
            struct.pack("=h", cell) for row in rows for pair in row["grid"] for cell in pair
        )
    elif name == "spans":
        column["leaf"] = 2
        column["present"] = [row["spans"] is not None for row in rows]
        lists = [0]
        for row in rows:
            lists.append(lists[-1] + len(row["spans"] or []))
        column["offsets"] = lists
        column["fixed"] = b"".join(
            struct.pack("=q", instant) for row in rows for span in row["spans"] or []
            for instant in span
        )
    else:
        raise RuntimeError(f"the probe holds no column named {name}")
    return column


def fields_of(specs):
    fields = nx.Fields()
    for name, levels, nullable, sensitive in specs:
        kind, length = levels[0]
        fields.add(name, getattr(nx.FieldType, kind), length, nullable=nullable, sensitive=sensitive)
        for kind, length in levels[1:]:
            fields.element(getattr(nx.FieldType, kind), length)
    return fields


def type_text(levels):
    """A field's type in Plexus's spelling, with nested fixed-size lists as dimensions."""
    kind, _ = levels[0]
    if kind == "LIST":
        return f"VEC<{type_text(levels[1:])}>"
    if kind == "FIXED_LIST":
        dimensions = []
        for level_kind, length in levels:
            if level_kind != "FIXED_LIST":
                break
            dimensions.append(str(length))
        element = type_text(levels[len(dimensions):])
        return f"ARRAY<{element}, {', '.join(dimensions)}>"
    return kind


def reported_fields(schema):
    fields = []
    for field in schema.fields:
        levels = [(upper(level.type), level.length) for level in field.levels]
        if upper(field.type) != levels[0][0]:
            raise RuntimeError("a field's first level differs from its type")
        fields.append(
            {
                "name": field.name, "levels": levels, "nullable": field.nullable,
                "sensitive": field.sensitive,
            }
        )
    return fields


def reported_line(prefix, field):
    nullability = "nullable" if field["nullable"] else "required"
    sensitivity = "sensitive" if field["sensitive"] else "public"
    return f"{prefix} {field['name']} {type_text(field['levels'])} {nullability} {sensitivity}"


def build(schema, rows):
    """Builds a batch of `rows` for `schema` one level at a time, overwriting every buffer as soon
    as the package has copied it."""
    builder = nx.BatchBuilder(schema, len(rows))
    for index, field in enumerate(reported_fields(schema)):
        column = host_column(field["name"], rows)
        if column["present"] is not None:
            builder.states(index, column["present"])
        if column["offsets"] is not None:
            builder.offsets(index, 0, column["offsets"])
        if column["varlen"] is not None:
            value_offsets, data = column["varlen"]
            buffer = bytearray(data)
            builder.varlen(index, column["leaf"], value_offsets, buffer)
            buffer[:] = bytes([SCRIBBLE]) * len(buffer)
        else:
            buffer = bytearray(column["fixed"])
            builder.fixed(index, column["leaf"], buffer)
            buffer[:] = bytes([SCRIBBLE]) * len(buffer)
    return builder.finish()


def render_value(levels, value):
    """A cell of a batch column: a list of its elements, rendered level by level, or its value."""
    kind = levels[0][0]
    if kind in ("LIST", "FIXED_LIST"):
        return "[" + ",".join(render_value(levels[1:], element) for element in value) + "]"
    return RENDER[kind](value)


def batch_rows(batch, fields):
    columns = []
    for index, field in enumerate(fields):
        values = batch.column(index)
        columns.append(
            ["null" if value is None else render_value(field["levels"], value) for value in values]
        )
    return [
        "ROW " + " ".join(f"{field['name']}={column[row]}" for field, column in zip(fields, columns))
        for row in range(batch.row_count)
    ]


def batch_ids(batch, fields):
    for index, field in enumerate(fields):
        if field["name"] == "id":
            return batch.column(index)
    raise RuntimeError("the batch has no id column")


def delivery_summary(delivery):
    fingerprint = delivery.branch_fingerprint
    branch = str(len(fingerprint)) if fingerprint is not None else "none"
    return (
        f"DELIVERY relay={delivery.source_relay} members={delivery.members} branch={branch} "
        f"identity={len(delivery.identity)} reference={len(delivery.reference)}"
    )


def submission_text(outcome):
    result = outcome.result
    if result == nx.SubmissionResult.COMPLETED:
        return "completed"
    if result == nx.SubmissionResult.NOT_ADMITTED:
        text = f"not_admitted {lower(outcome.refusal)}"
        if outcome.refusal == nx.SubmissionRefusal.INVALID_BATCH:
            text += f" {lower(outcome.defect)}"
        return text
    if result == nx.SubmissionResult.PROCESSING_FAILED:
        return f"processing_failed {lower(outcome.failure)}"
    return f"outcome_unknown {lower(outcome.uncertainty)}"


class Endpoints:
    def __init__(self, session, domain, report):
        self.session = session
        self.domain = domain
        self.report = report
        self.ingestor = os.environ["NERVIX_PROBE_INGESTOR"]
        self.emitter = os.environ["NERVIX_PROBE_EMITTER"]
        self.failures = []

    def expect(self, holds, what):
        if not holds:
            self.failures.append(what)

    def open_producer(self, fields, batches, asked):
        return self.session.open_ingestor(
            self.domain, self.ingestor, fields, batches=batches, bytes=asked
        )

    def open_consumer(self, fields):
        return self.session.subscribe_emitter(
            self.domain, self.emitter, fields, batches=CONSUMER_BATCHES, bytes=ENDPOINT_BYTES
        )

    @staticmethod
    def outcome(producer, submission):
        return submission_text(
            producer.rejoin(submission, cancel=nx.Cancel(deadline=WAIT_SECONDS))
        )

    @staticmethod
    def next(consumer, cancel=None):
        return consumer.next(cancel=cancel or nx.Cancel(deadline=WAIT_SECONDS))

    @staticmethod
    def settle(settle, *reason):
        return lower(settle(*reason, cancel=nx.Cancel(deadline=WAIT_SECONDS)))

    @staticmethod
    def policy(endpoint):
        policy = endpoint.policy
        return f"window={lower(policy.window)}/{policy.outstanding} ack_timeout={policy.ack_timeout_nanos}"

    def check_arrow(self, producer, consumer, input_schema):
        """Publishes a batch pyarrow built through the Arrow PyCapsule interface, and reads its
        output back through pyarrow the same way, without changing the report."""
        try:
            import pyarrow
        except ImportError:
            self.failures.append("pyarrow is not installed in the probe's environment")
            return
        utc = pyarrow.timestamp("ns", tz="+00:00")
        rows = [plain_row(50, "delta"), plain_row(51, "delta")]
        arrays = {
            "id": pyarrow.array([row["id"] for row in rows], pyarrow.uint32()),
            "tenant": pyarrow.array([row["tenant"] for row in rows], pyarrow.string()),
            "u8v": pyarrow.array([0, 0], pyarrow.uint8()),
            "i8v": pyarrow.array([0, 0], pyarrow.int8()),
            "u16v": pyarrow.array([0, 0], pyarrow.uint16()),
            "i16v": pyarrow.array([0, 0], pyarrow.int16()),
            "u32v": pyarrow.array([0, 0], pyarrow.uint32()),
            "i32v": pyarrow.array([0, 0], pyarrow.int32()),
            "u64v": pyarrow.array([0, 0], pyarrow.uint64()),
            "i64v": pyarrow.array([0, 0], pyarrow.int64()),
            "f32v": pyarrow.array([0.0, 0.0], pyarrow.float32()),
            "f64v": pyarrow.array([0.0, 0.0], pyarrow.float64()),
            "flag": pyarrow.array([False, False], pyarrow.bool_()),
            "text": pyarrow.array([None, None], pyarrow.string()),
            "raw": pyarrow.array([None, None], pyarrow.binary()),
            "at": pyarrow.array([0, 0], utc),
            "maybe": pyarrow.array([None, None], pyarrow.int64()),
            "tags": pyarrow.array([["p"], []], pyarrow.list_(pyarrow.string())),
            "grid": pyarrow.array(
                [[[0, 0], [0, 0]], [[0, 0], [0, 0]]],
                pyarrow.list_(pyarrow.list_(pyarrow.int16(), 2), 2),
            ),
            "spans": pyarrow.array([None, [[0, 1]]], pyarrow.list_(pyarrow.list_(utc, 2))),
            "secret": pyarrow.array(["s", "s"], pyarrow.string()),
        }
        names = [field["name"] for field in reported_fields(input_schema)]
        brought = pyarrow.record_batch([arrays[name] for name in names], names=names)
        submission = producer.submit(brought, cancel=nx.Cancel(deadline=WAIT_SECONDS))
        delivery = self.next(consumer)
        read = pyarrow.record_batch(delivery.batch)
        stream = pyarrow.RecordBatchReader.from_stream(delivery.batch).read_all()
        self.expect(read.column("id").to_pylist() == [50, 51], "pyarrow reads the output's ids")
        self.expect(read.column("tags").to_pylist() == [["p"], []], "pyarrow reads the output's lists")
        self.expect(stream.num_rows == 2, "pyarrow reads the output's stream")
        self.expect(delivery.ack(cancel=nx.Cancel(deadline=WAIT_SECONDS)) == nx.Settlement.CONFIRMED,
                    "an output read through pyarrow is acknowledged")
        self.expect(self.outcome(producer, submission) == "completed",
                    "a batch pyarrow built completes")
        # Arrow data of another schema is the host's argument, refused before anything is sent.
        mismatched = pyarrow.record_batch([arrays["id"]], names=["id"])
        failure = raised(lambda: producer.submit(mismatched))
        self.expect(isinstance(failure, nx.InvalidArgumentError), "another Arrow schema is refused")

    def run(self):
        report = self.report
        input_fields = fields_of(INPUT_FIELDS)
        output_fields = fields_of(INPUT_FIELDS + [ECHO_FIELD])
        mismatched = fields_of(
            [
                (name, levels, nullable, False if name == "secret" else sensitive)
                for name, levels, nullable, sensitive in INPUT_FIELDS
            ]
        )

        # An open whose expected fields differ from the endpoint's is refused exactly.
        for what in ("producer", "consumer"):
            if what == "producer":
                opening = lambda: self.open_producer(mismatched, PRODUCER_BATCHES, ENDPOINT_BYTES)
            else:
                opening = lambda: self.open_consumer(input_fields)
            failure = raised(opening)
            if (
                not isinstance(failure, nx.RejectedError)
                or failure.open_refusal != nx.OpenRefusal.SCHEMA_MISMATCH
            ):
                raise RuntimeError("an open with another schema was not refused exactly")
            report(f"REFUSED {what} schema mismatch")
        del mismatched

        consumer = self.open_consumer(output_fields)
        producer = self.open_producer(input_fields, PRODUCER_BATCHES, ENDPOINT_BYTES)
        output_schema = consumer.schema
        input_schema = producer.schema
        output = reported_fields(output_schema)
        inputs = reported_fields(input_schema)

        grant = consumer.grant
        report(
            f"CONSUMER generation={consumer.generation} state={lower(consumer.state)} "
            f"{self.policy(consumer)} credit={grant.batches}/{grant.bytes} "
            f"max={grant.max_batch_rows}/{grant.max_batch_bytes}"
        )
        for field in output:
            report(reported_line("OUTPUT FIELD", field))
        grant = producer.grant
        self.expect(
            0 < grant.max_batch_bytes <= grant.bytes and grant.max_batch_rows > 0,
            "the batch limits fit the grant",
        )
        report(
            f"PRODUCER generation={producer.generation} state={lower(producer.state)} "
            f"admission={lower(producer.admission)} {self.policy(producer)} "
            f"credit={grant.batches}/{grant.bytes}"
        )
        for field in inputs:
            report(reported_line("INPUT FIELD", field))
        report("OPENED")

        # A wait for output that nothing produces ends by its deadline, and one cancelled from
        # another thread by its token; the reads they leave behind are the consumer's.
        self.expect(
            isinstance(
                raised(lambda: self.next(consumer, nx.Cancel(deadline=EXPIRING_SECONDS))),
                nx.DeadlineError,
            ),
            "an expired read reports its deadline",
        )
        report("NEXT deadline")
        cancel = nx.Cancel()
        outcome = {}
        waiter = threading.Thread(
            target=lambda: outcome.update(failure=raised(lambda: self.next(consumer, cancel)))
        )
        waiter.start()
        time.sleep(0.1)
        cancel.trigger()
        waiter.join()
        self.expect(isinstance(outcome.get("failure"), nx.CancelledError), "a cancelled read reports it")
        report("NEXT cancelled")

        # A batch built for another schema is refused before anything is sent, and the same
        # batch written as a stream by other tooling is refused by the server.
        rows = typed_rows()
        other = build(output_schema, rows)
        self.expect(
            isinstance(raised(lambda: producer.submit(other)), nx.InvalidArgumentError),
            "another schema is the host's argument",
        )
        report("SUBMIT invalid argument")
        foreign = bytearray(other.ipc)
        submission = producer.submit(foreign)
        foreign[:] = bytes([SCRIBBLE]) * len(foreign)
        del other
        report(f"OUTCOME {self.outcome(producer, submission)}")

        # The typed batch: its outcome waits for the application's acknowledgement.
        first = producer.submit(build(input_schema, rows))
        report("SUBMITTED first")
        delivery = self.next(consumer)
        report(delivery_summary(delivery))
        pending = producer.pending()
        self.expect(
            len(pending) == 1 and pending[0].id == first and not pending[0].resolved,
            "the only submission waits for its outcome",
        )

        def early():
            producer.rejoin(first, cancel=nx.Cancel(deadline=EXPIRING_SECONDS))

        self.expect(isinstance(raised(early), nx.DeadlineError), "an unacknowledged batch has no outcome")
        report("PENDING first unresolved")
        delivered = delivery.batch
        printed = batch_rows(delivered, output)
        for line in printed:
            report(line)
        self.expect(
            bytes(delivered.ipc) == bytes(delivery.ipc),
            "the batch borrows the stream its delivery carried",
        )

        # A retried attempt comes back with the same identity and a new reference; the first
        # reference is stale from then on.
        report(f"RETRY {self.settle(delivery.retry)}")
        again = self.next(consumer)
        self.expect(again.identity == delivery.identity, "a retry keeps the identity")
        self.expect(again.reference != delivery.reference, "a retry makes a new reference")
        report("REDELIVERED same identity new reference")
        report(f"ACK {self.settle(delivery.ack)}")
        del delivery

        # A reference kept here outlives the others, let go on another thread, and reads and
        # settles the same attempt.
        retained = again
        retained_batch = again.batch
        view = retained_batch.ipc
        before = bytes(retained.ipc)
        holder = [again]
        del again
        releaser = threading.Thread(target=holder.clear)
        releaser.start()
        releaser.join()
        gc.collect()
        self.expect(bytes(retained.ipc) == before, "a retained delivery keeps its stream")
        self.expect(bytes(view) == before, "a borrowed view outlives the other references")
        self.expect(batch_rows(retained_batch, output) == printed, "a retained batch reads the same")
        report(f"ACK {self.settle(retained.ack)}")
        del retained, retained_batch, view
        report(f"OUTCOME {self.outcome(producer, first)}")

        # An application rejection finishes the batch through the emitter's message error policy.
        second = producer.submit(build(input_schema, [plain_row(10, "beta")]))
        report("SUBMITTED second")
        rejected = self.next(consumer)
        report(f"REJECT {self.settle(rejected.reject, 'application refused')}")
        del rejected
        report(f"OUTCOME {self.outcome(producer, second)}")

        # Arrow data pyarrow built goes out and comes back through the PyCapsule interface.
        self.check_arrow(producer, consumer, input_schema)

        # Two outstanding batches use up the producer's credit, so a third waits for an outcome.
        fifth_batch = build(input_schema, [plain_row(24, "acme"), plain_row(25, "acme")])
        third = producer.submit(build(input_schema, [plain_row(20, "acme"), plain_row(21, "acme")]))
        report("SUBMITTED third")
        fourth = producer.submit(build(input_schema, [plain_row(22, "acme"), plain_row(23, "acme")]))
        report("SUBMITTED fourth")
        self.expect(
            isinstance(
                raised(
                    lambda: producer.submit(fifth_batch, cancel=nx.Cancel(deadline=EXPIRING_SECONDS))
                ),
                nx.DeadlineError,
            ),
            "a batch beyond the credit waits",
        )
        report("SUBMIT deadline")
        # A batch refused as busy is sent again after the ingestor's backoff, behind the batch
        # submitted after it, so the two outputs may arrive in either order; each keeps its rows.
        outputs = []
        for _ in range(2):
            output_delivery = self.next(consumer)
            outputs.append(batch_ids(output_delivery.batch, output))
            report(f"ACK {self.settle(output_delivery.ack)}")
        self.expect(sorted(outputs) == [[20, 21], [22, 23]], "a batch keeps its rows")
        report(f"OUTCOME {self.outcome(producer, third)}")
        report(f"OUTCOME {self.outcome(producer, fourth)}")
        fifth = producer.submit(fifth_batch)
        report("SUBMITTED fifth")
        fifth_output = self.next(consumer)
        self.expect(batch_ids(fifth_output.batch, output) == [24, 25], "a batch keeps its rows")
        report(f"ACK {self.settle(fifth_output.ack)}")
        report(f"OUTCOME {self.outcome(producer, fifth)}")

        # The scenario cuts the session while one delivery is held unacknowledged.
        extra = self.open_producer(input_fields, 1, 65536)
        report("PRODUCER extra opened")
        held = producer.submit(build(input_schema, [plain_row(30, "gamma")]))
        report("SUBMITTED held")
        held_delivery = self.next(consumer)
        held_identity = held_delivery.identity
        report("HOLDING")
        self.expect(
            isinstance(raised(lambda: self.next(consumer)), nx.ConsumerInterruptedError),
            "a lost session interrupts the consumer",
        )
        report("NEXT interrupted")
        self.expect(
            isinstance(raised(lambda: held_delivery.ack(cancel=nx.Cancel(deadline=WAIT_SECONDS))), nx.RejectedError),
            "a delivery of a lost session expired",
        )
        report("ACK expired")
        del held_delivery
        report(f"OUTCOME {self.outcome(producer, held)}")
        waited_until = time.monotonic() + WAIT_SECONDS
        while extra.state == nx.EndpointState.ACTIVE:
            if time.monotonic() > waited_until:
                raise RuntimeError("the extra producer stayed active after its session ended")
            time.sleep(0.01)
        closing = nx.Cancel(deadline=WAIT_SECONDS)
        extra.close(cancel=closing)
        self.expect(extra.state == nx.EndpointState.CLOSED, "a producer closed on reconnect")
        report("CLOSED extra producer")
        report("WAITING restore")

        # The restored consumer receives the held batch again, and the restored producer
        # publishes.
        redelivered = self.next(consumer)
        self.expect(redelivered.identity == held_identity, "the held batch keeps its identity")
        report("REDELIVERED held same identity")
        report(f"ACK {self.settle(redelivered.ack)}")
        del redelivered
        sixth = producer.submit(build(input_schema, [plain_row(40, "gamma")]))
        report("SUBMITTED sixth")
        sixth_output = self.next(consumer)
        report(f"ACK {self.settle(sixth_output.ack)}")
        del sixth_output
        report(f"OUTCOME {self.outcome(producer, sixth)}")
        report(
            f"STATE producer={lower(producer.state)} extra={lower(extra.state)} "
            f"consumer={lower(consumer.state)}"
        )
        consumer.close(cancel=closing)
        producer.close(cancel=closing)
        self.expect(
            consumer.state == nx.EndpointState.CLOSED and producer.state == nx.EndpointState.CLOSED,
            "closed handles read closed",
        )
        report("CLOSED completed")
        if self.failures:
            raise RuntimeError(f"the probe's checks failed: {'; '.join(self.failures)}")
        report("CHECKS ok")
        report("PASS")


def run_upload(session, report):
    execution = session.prepare(os.environ["NERVIX_PROBE_UPLOAD_QUERY"])
    for line in ("UPLOAD completed", "RETRY completed"):
        outcome = session.execute(execution)
        assert outcome.disposition == nx.Disposition.COMPLETED
        assert outcome.upload_failure is None
        report(line)
    Path(os.environ["NERVIX_PROBE_UPLOAD_FILE"]).write_bytes(b"different archive\n")
    outcome = session.execute(execution)
    assert outcome.disposition == nx.Disposition.FAILED
    assert outcome.upload_failure == nx.UploadFailure.IDENTITY_CONFLICT
    report("CONFLICT IdentityConflict")


def main():
    environment = os.environ
    domain = environment["NERVIX_PROBE_DOMAIN"]
    session = nx.Session(
        environment["NERVIX_PROBE_GRPC_URI"],
        domain=domain,
        username=environment["NERVIX_PROBE_USERNAME"],
        password=environment["NERVIX_PROBE_PASSWORD"],
    )
    report = lambda line: print(line, flush=True)
    if sys.argv[1:] == ["upload"]:
        run_upload(session, report)
        report("PASS")
        return
    if sys.argv[1:] == ["clock"]:
        run_clock(session, domain, report)
        return
    if sys.argv[1:] == ["io"]:
        Endpoints(session, domain, report).run()
        return

    relay = environment["NERVIX_PROBE_RELAY"]
    subscription = environment["NERVIX_PROBE_SUBSCRIPTION"]
    expected_rows = int(environment["NERVIX_PROBE_ROWS"])

    report(f"OPERATION {lower(execute(session, f'SHOW CREATE RELAY {relay};').disposition)}")
    report(error_line(execute(session, "CREATE RELAY;")))

    opened = execute(session, f"CREATE SUBSCRIPTION {subscription} TO {relay};")
    handle = opened.subscription
    if handle is None or handle.name != subscription or handle.generation == 0:
        raise RuntimeError("the subscription opened under another handle")
    schema = opened.schema
    fields, key_fields = described(schema.fields), described(schema.key_fields)
    for field in fields:
        report(field_line("FIELD", field))
    if schema.branch is not None:
        report(f"BRANCH {schema.branch}")
    for field in key_fields:
        report(field_line("KEY", field))
    report("SUBSCRIBED")

    deadline = nx.Cancel(deadline=120)
    retained, reported, seen = [], [], 0
    while seen < expected_rows:
        event = session.next_event(cancel=deadline)
        if event.kind != nx.EventKind.ROWS:
            raise RuntimeError("the subscription reported something other than rows first")
        if event.subscription != handle:
            raise RuntimeError("rows arrived for another subscription")
        lines = row_lines(event, fields, key_fields)
        for line in lines:
            report(line)
        seen += len(lines)
        reported.append(lines)
        # Keep a second reference and let the first go, so the rows must survive on it alone.
        retained.append(event)
        del event

    if output_dir := environment.get("NERVIX_CLIENT_WIRE_BINDING_PROFILE_DIR"):
        profile_binding(retained[0], fields, output_dir)

    check_retention(retained, reported, fields, key_fields)
    check_cancellation(session)
    report("CHECKS ok")
    report(
        f"CLOSED {lower(execute(session, f'DELETE SUBSCRIPTION {subscription};').disposition)}"
    )
    del session
    report("PASS")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:  # noqa: BLE001 - the probe reports every failure the same way
        print(f"probe failed: {error!r}", file=sys.stderr)
        sys.exit(1)
