"""The CPython probe of the shared Rust binding, through ctypes.

It prints the same conformance report as every other probe. Bulk data stays borrowed: an event's
frame is a memoryview over the binding's buffer that keeps the event alive for as long as the view
exists, and every column is copied into a ctypes array in one call. ctypes releases the GIL for
every call, so a blocked wait never stops another Python thread from cancelling it. Run with the
`clock` argument, it attaches to the domain's running clock instead, reads the clock the attach
reported before its first tick, follows the generation a STOP and START begin and the attachment
restored after its session ends, and detaches. Run with the `io` argument, it publishes typed
batches through a client ingestor and reads, retries, rejects and acknowledges their output through
a client emitter, across the session the scenario cuts.
"""

import ctypes
import gc
import json
import os
import struct
import sys
import threading
import time

LIBRARY = ctypes.CDLL(os.environ["NERVIX_CLIENT_LIBRARY"])

BYTES_P = ctypes.POINTER(ctypes.c_uint8)
OUT_BYTES = ctypes.POINTER(BYTES_P)
SIZE = ctypes.c_size_t
OUT_SIZE = ctypes.POINTER(SIZE)
HANDLE = ctypes.c_void_p
OUT_HANDLE = ctypes.POINTER(HANDLE)

ERROR_CANCELLED = 7
ERROR_DEADLINE = 6
PART_ROWS = 1
PART_BRANCH_KEY = 2
CELL_VALUE, CELL_NULL, CELL_REDACTED = 1, 2, 3
EVENT_ROWS = 1
CLOCK_KINDS = {1: "STATE", 2: "TICK", 3: "ENDED", 4: "INTERRUPTED", 5: "RESTORATION_FAILED"}
CLOCK_PACED = 4

DISPOSITIONS = {
    1: "completed",
    2: "failed",
    3: "not_leader",
    4: "transaction_detached",
    5: "transaction_taken_over",
    6: "outcome_unknown",
    7: "execution_reference_conflict",
    8: "execution_reference_expired",
    9: "preview_stale",
}
TYPES = {
    1: "U8", 2: "I8", 3: "U16", 4: "I16", 5: "U32", 6: "I32", 7: "U64", 8: "I64",
    9: "F32", 10: "F64", 11: "BOOL", 12: "STRING", 13: "BYTES", 14: "DATETIME",
    15: "FIXED_LIST", 16: "LIST",
}
# The ctypes element each fixed-width type is copied into, and how its value is printed. Floats are
# copied as unsigned integers of their width, because the report prints their bits.
FIXED = {
    "U8": (ctypes.c_uint8, lambda value: f"u8:{value}"),
    "I8": (ctypes.c_int8, lambda value: f"i8:{value}"),
    "U16": (ctypes.c_uint16, lambda value: f"u16:{value}"),
    "I16": (ctypes.c_int16, lambda value: f"i16:{value}"),
    "U32": (ctypes.c_uint32, lambda value: f"u32:{value}"),
    "I32": (ctypes.c_int32, lambda value: f"i32:{value}"),
    "U64": (ctypes.c_uint64, lambda value: f"u64:{value}"),
    "I64": (ctypes.c_int64, lambda value: f"i64:{value}"),
    "F32": (ctypes.c_uint32, lambda value: f"f32:{value:08x}"),
    "F64": (ctypes.c_uint64, lambda value: f"f64:{value:016x}"),
    "BOOL": (ctypes.c_uint8, lambda value: f"bool:{'true' if value else 'false'}"),
    "DATETIME": (ctypes.c_int64, lambda value: f"datetime:{value}"),
}


def declare(name, restype, *argtypes):
    function = getattr(LIBRARY, name)
    function.restype = restype
    function.argtypes = list(argtypes)
    return function


nx_error_kind_of = declare("nx_error_kind_of", ctypes.c_int32, HANDLE)
nx_error_message = declare("nx_error_message", None, HANDLE, OUT_BYTES, OUT_SIZE)
nx_error_execution_reference = declare(
    "nx_error_execution_reference", ctypes.c_bool, HANDLE, OUT_BYTES, OUT_SIZE
)
nx_error_free = declare("nx_error_free", None, HANDLE)
nx_cancel_new = declare("nx_cancel_new", HANDLE)
nx_cancel_with_deadline = declare("nx_cancel_with_deadline", HANDLE, ctypes.c_uint64, OUT_HANDLE)
nx_cancel_trigger = declare("nx_cancel_trigger", None, HANDLE)
nx_cancel_free = declare("nx_cancel_free", None, HANDLE)
nx_session_connect = declare(
    "nx_session_connect", HANDLE,
    ctypes.c_char_p, SIZE, ctypes.c_char_p, SIZE, ctypes.c_char_p, SIZE, ctypes.c_char_p, SIZE,
    HANDLE, OUT_HANDLE,
)
nx_session_free = declare("nx_session_free", None, HANDLE)
nx_session_prepare = declare(
    "nx_session_prepare", HANDLE, HANDLE, ctypes.c_char_p, SIZE, HANDLE, OUT_HANDLE
)
nx_execution_free = declare("nx_execution_free", None, HANDLE)
nx_session_execute = declare("nx_session_execute", HANDLE, HANDLE, HANDLE, HANDLE, OUT_HANDLE)
nx_session_next_event = declare("nx_session_next_event", HANDLE, HANDLE, HANDLE, OUT_HANDLE)
nx_outcome_disposition = declare("nx_outcome_disposition", ctypes.c_int32, HANDLE)
nx_outcome_diagnostic_count = declare("nx_outcome_diagnostic_count", SIZE, HANDLE)
nx_outcome_diagnostic = declare(
    "nx_outcome_diagnostic", HANDLE, HANDLE, SIZE, OUT_BYTES, OUT_SIZE,
    ctypes.POINTER(ctypes.c_bool), ctypes.POINTER(ctypes.c_uint32),
    ctypes.POINTER(ctypes.c_uint32),
)
nx_outcome_subscription = declare(
    "nx_outcome_subscription", ctypes.c_bool, HANDLE, OUT_BYTES, OUT_SIZE,
    ctypes.POINTER(ctypes.c_uint64),
)
nx_outcome_schema = declare("nx_outcome_schema", HANDLE, HANDLE, OUT_HANDLE)
nx_outcome_free = declare("nx_outcome_free", None, HANDLE)
nx_schema_field_count = declare("nx_schema_field_count", HANDLE, HANDLE, ctypes.c_int32, OUT_SIZE)
nx_schema_field = declare(
    "nx_schema_field", HANDLE, HANDLE, ctypes.c_int32, SIZE, OUT_BYTES, OUT_SIZE,
    ctypes.POINTER(ctypes.c_int32), ctypes.POINTER(ctypes.c_bool),
    ctypes.POINTER(ctypes.c_bool),
)
nx_schema_branch = declare("nx_schema_branch", ctypes.c_bool, HANDLE, OUT_BYTES, OUT_SIZE)
nx_schema_free = declare("nx_schema_free", None, HANDLE)
nx_event_kind_of = declare("nx_event_kind_of", ctypes.c_int32, HANDLE)
nx_event_subscription = declare(
    "nx_event_subscription", None, HANDLE, OUT_BYTES, OUT_SIZE, ctypes.POINTER(ctypes.c_uint64)
)
nx_event_row_count = declare("nx_event_row_count", ctypes.c_uint64, HANDLE)
nx_event_retain = declare("nx_event_retain", HANDLE, HANDLE)
nx_event_release = declare("nx_event_release", None, HANDLE)
nx_event_frame = declare("nx_event_frame", HANDLE, HANDLE, OUT_BYTES, OUT_SIZE)
nx_event_column_states = declare(
    "nx_event_column_states", HANDLE, HANDLE, ctypes.c_int32, SIZE, BYTES_P, SIZE
)
nx_event_column_fixed = declare(
    "nx_event_column_fixed", HANDLE, HANDLE, ctypes.c_int32, SIZE, ctypes.c_void_p, SIZE
)
nx_event_column_varlen = declare(
    "nx_event_column_varlen", HANDLE, HANDLE, ctypes.c_int32, SIZE,
    ctypes.POINTER(ctypes.c_uint64), SIZE, BYTES_P, SIZE, OUT_SIZE,
)
nx_event_cell_varlen = declare(
    "nx_event_cell_varlen", HANDLE, HANDLE, ctypes.c_int32, SIZE, SIZE, OUT_BYTES, OUT_SIZE
)
nx_session_next_clock_event = declare(
    "nx_session_next_clock_event", HANDLE, HANDLE, HANDLE, OUT_HANDLE
)
nx_clock_event_kind_of = declare("nx_clock_event_kind_of", ctypes.c_int32, HANDLE)
nx_clock_event_domain = declare("nx_clock_event_domain", None, HANDLE, OUT_BYTES, OUT_SIZE)
nx_clock_event_generation = declare(
    "nx_clock_event_generation", HANDLE, HANDLE, ctypes.POINTER(ctypes.c_uint64)
)
nx_clock_event_state = declare(
    "nx_clock_event_state", HANDLE, HANDLE, ctypes.POINTER(ctypes.c_int32)
)
nx_clock_event_paced = declare(
    "nx_clock_event_paced", HANDLE, HANDLE, ctypes.POINTER(ctypes.c_uint64),
    ctypes.POINTER(ctypes.c_uint64), ctypes.POINTER(ctypes.c_int64),
    ctypes.POINTER(ctypes.c_int64), ctypes.POINTER(ctypes.c_double),
)
nx_clock_event_tick = declare(
    "nx_clock_event_tick", HANDLE, HANDLE, ctypes.POINTER(ctypes.c_uint64),
    ctypes.POINTER(ctypes.c_int64), ctypes.POINTER(ctypes.c_int64),
    ctypes.POINTER(ctypes.c_int64),
)
nx_clock_event_retain = declare("nx_clock_event_retain", HANDLE, HANDLE)
nx_clock_event_release = declare("nx_clock_event_release", None, HANDLE)
nx_session_domain_clock = declare(
    "nx_session_domain_clock", HANDLE, HANDLE, ctypes.c_char_p, SIZE, OUT_HANDLE
)
nx_domain_clock_generation = declare("nx_domain_clock_generation", ctypes.c_uint64, HANDLE)
nx_domain_clock_state = declare("nx_domain_clock_state", ctypes.c_int32, HANDLE)
nx_domain_clock_paced = declare(
    "nx_domain_clock_paced", HANDLE, HANDLE, ctypes.POINTER(ctypes.c_uint64),
    ctypes.POINTER(ctypes.c_uint64), ctypes.POINTER(ctypes.c_int64),
    ctypes.POINTER(ctypes.c_int64), ctypes.POINTER(ctypes.c_double),
)
nx_domain_clock_tick = declare(
    "nx_domain_clock_tick", ctypes.c_bool, HANDLE, ctypes.POINTER(ctypes.c_uint64),
    ctypes.POINTER(ctypes.c_int64), ctypes.POINTER(ctypes.c_int64),
    ctypes.POINTER(ctypes.c_int64),
)
nx_domain_clock_logical_time_at = declare(
    "nx_domain_clock_logical_time_at", HANDLE, HANDLE, ctypes.c_int64,
    ctypes.POINTER(ctypes.c_int64),
)
nx_domain_clock_wall_duration_until = declare(
    "nx_domain_clock_wall_duration_until", HANDLE, HANDLE, ctypes.c_int64, ctypes.c_int64,
    ctypes.POINTER(ctypes.c_uint64),
)
nx_domain_clock_admission_window = declare(
    "nx_domain_clock_admission_window", HANDLE, HANDLE, ctypes.c_int64,
    ctypes.POINTER(ctypes.c_bool), ctypes.POINTER(ctypes.c_int64), ctypes.POINTER(ctypes.c_int64),
)
nx_domain_clock_admits = declare(
    "nx_domain_clock_admits", HANDLE, HANDLE, ctypes.c_int64, ctypes.c_int64,
    ctypes.POINTER(ctypes.c_bool),
)
nx_domain_clock_retain = declare("nx_domain_clock_retain", HANDLE, HANDLE)
nx_domain_clock_release = declare("nx_domain_clock_release", None, HANDLE)


class Failure(Exception):
    def __init__(self, kind, message, reference):
        super().__init__(f"kind {kind}: {message}")
        self.kind = kind
        self.reference = reference


def borrowed(pointer, length):
    """Copies bytes the binding lent for the duration of this call."""
    if length.value == 0:
        return b""
    return ctypes.string_at(pointer, length.value)


def check(error):
    """Raises the error a call returned, releasing it."""
    if not error:
        return
    message, message_len = BYTES_P(), SIZE()
    nx_error_message(error, ctypes.byref(message), ctypes.byref(message_len))
    reference, reference_len = BYTES_P(), SIZE()
    named = None
    if nx_error_execution_reference(error, ctypes.byref(reference), ctypes.byref(reference_len)):
        named = borrowed(reference, reference_len)
    failure = Failure(
        nx_error_kind_of(error), borrowed(message, message_len).decode(), named
    )
    nx_error_free(error)
    raise failure


def text(value):
    encoded = value.encode()
    return encoded, len(encoded)


class Cancel:
    def __init__(self, deadline_millis=None):
        if deadline_millis is None:
            self.handle = nx_cancel_new()
        else:
            self.handle = HANDLE()
            check(nx_cancel_with_deadline(deadline_millis, ctypes.byref(self.handle)))

    def trigger(self):
        nx_cancel_trigger(self.handle)

    def __del__(self):
        nx_cancel_free(self.handle)


class Event:
    """One reference to an event, released when the object is collected."""

    def __init__(self, handle):
        self.handle = handle

    def retain(self):
        return Event(nx_event_retain(self.handle))

    def __del__(self):
        nx_event_release(self.handle)

    def kind(self):
        return nx_event_kind_of(self.handle)

    def row_count(self):
        return nx_event_row_count(self.handle)

    def subscription(self):
        name, name_len, generation = BYTES_P(), SIZE(), ctypes.c_uint64()
        nx_event_subscription(
            self.handle, ctypes.byref(name), ctypes.byref(name_len), ctypes.byref(generation)
        )
        return borrowed(name, name_len).decode(), generation.value

    def frame(self):
        """A memoryview of the frame, borrowed without a copy. The view keeps this event alive."""
        frame, frame_len = BYTES_P(), SIZE()
        check(nx_event_frame(self.handle, ctypes.byref(frame), ctypes.byref(frame_len)))
        address = ctypes.cast(frame, ctypes.c_void_p).value
        buffer = (ctypes.c_uint8 * frame_len.value).from_address(address)
        buffer.owner = self
        return memoryview(buffer).cast("B")

    def column(self, part, index, field, cells):
        states = (ctypes.c_uint8 * cells)()
        check(nx_event_column_states(self.handle, part, index, states, cells))
        kind = field["type"]
        if kind in FIXED:
            element, render = FIXED[kind]
            values = (element * cells)()
            check(
                nx_event_column_fixed(self.handle, part, index, values, ctypes.sizeof(values))
            )
            rendered = [render(value) for value in values]
        elif kind in ("STRING", "BYTES"):
            data_len = SIZE()
            check(
                nx_event_column_varlen(
                    self.handle, part, index, None, 0, None, 0, ctypes.byref(data_len)
                )
            )
            offsets = (ctypes.c_uint64 * (cells + 1))()
            data = (ctypes.c_uint8 * max(data_len.value, 1))()
            check(
                nx_event_column_varlen(
                    self.handle, part, index, offsets, cells + 1, data, len(data),
                    ctypes.byref(data_len),
                )
            )
            raw = bytes(data)
            prefix = "str" if kind == "STRING" else "bytes"
            rendered = []
            for row in range(cells):
                copied = raw[offsets[row]:offsets[row + 1]]
                if states[row] == CELL_VALUE and self.borrowed_cell(part, row, index) != copied:
                    raise RuntimeError(
                        "a copied value differs from the same value borrowed from the frame"
                    )
                rendered.append(f"{prefix}:{copied.hex()}")
        else:
            raise RuntimeError("list columns are read from the frame")
        cells_out = []
        for row in range(cells):
            if states[row] == CELL_NULL:
                cells_out.append("null")
            elif states[row] == CELL_REDACTED:
                cells_out.append("redacted")
            elif states[row] == CELL_VALUE:
                cells_out.append(rendered[row])
            else:
                raise RuntimeError(f"unknown cell state {states[row]}")
        return cells_out

    def borrowed_cell(self, part, row, index):
        value, value_len = BYTES_P(), SIZE()
        check(
            nx_event_cell_varlen(
                self.handle, part, row, index, ctypes.byref(value), ctypes.byref(value_len)
            )
        )
        return borrowed(value, value_len)

    def render(self, part, fields, cells):
        rows = [[] for _ in range(cells)]
        for index, field in enumerate(fields):
            for row, rendered in enumerate(self.column(part, index, field, cells)):
                rows[row].append(f"{field['name']}={rendered}")
        return [" ".join(row) for row in rows]

    def row_lines(self, fields, key_fields):
        key = self.render(PART_BRANCH_KEY, key_fields, 1)[0] if key_fields else ""
        return [f"ROW [{key}] {row}" for row in self.render(PART_ROWS, fields, self.row_count())]


class ClockEvent:
    """One reference to a domain clock event, released when the object is collected."""

    def __init__(self, handle):
        self.handle = handle

    def retain(self):
        return ClockEvent(nx_clock_event_retain(self.handle))

    def __del__(self):
        nx_clock_event_release(self.handle)

    def kind(self):
        return CLOCK_KINDS[nx_clock_event_kind_of(self.handle)]

    def domain(self):
        name, name_len = BYTES_P(), SIZE()
        nx_clock_event_domain(self.handle, ctypes.byref(name), ctypes.byref(name_len))
        return borrowed(name, name_len).decode()

    def generation(self):
        generation = ctypes.c_uint64()
        check(nx_clock_event_generation(self.handle, ctypes.byref(generation)))
        return generation.value

    def state(self):
        state = ctypes.c_int32()
        check(nx_clock_event_state(self.handle, ctypes.byref(state)))
        return state.value

    def paced(self):
        return paced_clock(nx_clock_event_paced, self.handle, self.generation())

    def tick(self):
        """The progress a tick reports. The authority's UTC observation depends on when the tick
        was accepted, so it is not read."""
        tick_id, boundary, serving_logical = ctypes.c_uint64(), ctypes.c_int64(), ctypes.c_int64()
        check(
            nx_clock_event_tick(
                self.handle, ctypes.byref(tick_id), ctypes.byref(boundary), None,
                ctypes.byref(serving_logical),
            )
        )
        return {
            "generation": self.generation(),
            "id": tick_id.value,
            "boundary": boundary.value,
            "serving_logical": serving_logical.value,
        }


def paced_clock(read, handle, generation):
    """The committed clock `read` writes for `handle`, a clock event or a domain clock."""
    period, skew = ctypes.c_uint64(), ctypes.c_uint64()
    origin, anchor, rate = ctypes.c_int64(), ctypes.c_int64(), ctypes.c_double()
    check(
        read(
            handle, ctypes.byref(period), ctypes.byref(skew), ctypes.byref(origin),
            ctypes.byref(anchor), ctypes.byref(rate),
        )
    )
    return {
        "generation": generation,
        "period": period.value,
        "skew": skew.value,
        "origin": origin.value,
        "anchor": anchor.value,
        "rate": rate.value,
    }


class DomainClock:
    """One reference to the clock the session held for a followed domain when the probe read it,
    released when the object is collected."""

    def __init__(self, handle):
        self.handle = handle

    def retain(self):
        return DomainClock(nx_domain_clock_retain(self.handle))

    def __del__(self):
        nx_domain_clock_release(self.handle)

    def generation(self):
        return nx_domain_clock_generation(self.handle)

    def state(self):
        return nx_domain_clock_state(self.handle)

    def paced(self):
        return paced_clock(nx_domain_clock_paced, self.handle, self.generation())

    def tick_id(self):
        """The id of the newest tick the read holds, or None when it holds none."""
        tick_id = ctypes.c_uint64()
        if not nx_domain_clock_tick(self.handle, ctypes.byref(tick_id), None, None, None):
            return None
        return tick_id.value

    def logical_time_at(self, utc):
        logical = ctypes.c_int64()
        check(nx_domain_clock_logical_time_at(self.handle, utc, ctypes.byref(logical)))
        return logical.value

    def wall_duration_until(self, utc, target):
        wait = ctypes.c_uint64()
        check(nx_domain_clock_wall_duration_until(self.handle, utc, target, ctypes.byref(wait)))
        return wait.value

    def admission_window(self, utc):
        has_window, earliest, latest = ctypes.c_bool(), ctypes.c_int64(), ctypes.c_int64()
        check(
            nx_domain_clock_admission_window(
                self.handle, utc, ctypes.byref(has_window), ctypes.byref(earliest),
                ctypes.byref(latest),
            )
        )
        if not has_window.value:
            return None
        return earliest.value, latest.value

    def admits(self, utc, event):
        admitted = ctypes.c_bool()
        check(nx_domain_clock_admits(self.handle, utc, event, ctypes.byref(admitted)))
        return admitted.value


class Session:
    def __init__(self, server, domain, username, password):
        self.handle = HANDLE()
        check(
            nx_session_connect(
                *text(server), *text(domain), *text(username), *text(password), None,
                ctypes.byref(self.handle),
            )
        )

    def close(self):
        nx_session_free(self.handle)
        self.handle = None

    def execute(self, query, cancel=None):
        execution = HANDLE()
        check(nx_session_prepare(self.handle, *text(query), None, ctypes.byref(execution)))
        try:
            outcome = HANDLE()
            check(
                nx_session_execute(
                    self.handle, execution, cancel.handle if cancel else None,
                    ctypes.byref(outcome),
                )
            )
            return Outcome(outcome)
        finally:
            nx_execution_free(execution)

    def next_event(self, cancel):
        event = HANDLE()
        check(nx_session_next_event(self.handle, cancel.handle, ctypes.byref(event)))
        return Event(event)

    def domain_clock(self, domain):
        """The clock the session holds for `domain`, or None when it follows none."""
        clock = HANDLE()
        check(nx_session_domain_clock(self.handle, *text(domain), ctypes.byref(clock)))
        if not clock:
            return None
        return DomainClock(clock)

    def next_clock_event(self, cancel, domain):
        event = HANDLE()
        check(nx_session_next_clock_event(self.handle, cancel.handle, ctypes.byref(event)))
        clock_event = ClockEvent(event)
        if clock_event.domain() != domain:
            raise RuntimeError("a clock event arrived for another domain")
        return clock_event


class Outcome:
    def __init__(self, handle):
        self.handle = handle

    def __del__(self):
        nx_outcome_free(self.handle)

    def disposition(self):
        return DISPOSITIONS[nx_outcome_disposition(self.handle)]

    def error_line(self):
        count = nx_outcome_diagnostic_count(self.handle)
        span = "none"
        if count > 0:
            message, message_len = BYTES_P(), SIZE()
            has_span = ctypes.c_bool()
            start, end = ctypes.c_uint32(), ctypes.c_uint32()
            check(
                nx_outcome_diagnostic(
                    self.handle, 0, ctypes.byref(message), ctypes.byref(message_len),
                    ctypes.byref(has_span), ctypes.byref(start), ctypes.byref(end),
                )
            )
            if has_span.value:
                span = f"{start.value}..{end.value}"
        return f"ERROR {self.disposition()} diagnostics={count} span={span}"

    def subscription(self):
        name, name_len, generation = BYTES_P(), SIZE(), ctypes.c_uint64()
        if not nx_outcome_subscription(
            self.handle, ctypes.byref(name), ctypes.byref(name_len), ctypes.byref(generation)
        ):
            raise RuntimeError("the subscribe command opened no subscription")
        return borrowed(name, name_len).decode(), generation.value

    def schema(self):
        schema = HANDLE()
        check(nx_outcome_schema(self.handle, ctypes.byref(schema)))
        try:
            return read_fields(schema, PART_ROWS), read_fields(schema, PART_BRANCH_KEY), branch(
                schema
            )
        finally:
            nx_schema_free(schema)


def read_fields(schema, part):
    count = SIZE()
    check(nx_schema_field_count(schema, part, ctypes.byref(count)))
    fields = []
    for index in range(count.value):
        name, name_len = BYTES_P(), SIZE()
        kind = ctypes.c_int32()
        nullable, sensitive = ctypes.c_bool(), ctypes.c_bool()
        check(
            nx_schema_field(
                schema, part, index, ctypes.byref(name), ctypes.byref(name_len),
                ctypes.byref(kind), ctypes.byref(nullable), ctypes.byref(sensitive),
            )
        )
        fields.append(
            {
                "name": borrowed(name, name_len).decode(),
                "type": TYPES[kind.value],
                "nullable": nullable.value,
                "sensitive": sensitive.value,
            }
        )
    return fields


def branch(schema):
    name, name_len = BYTES_P(), SIZE()
    if not nx_schema_branch(schema, ctypes.byref(name), ctypes.byref(name_len)):
        return None
    return borrowed(name, name_len).decode()


def field_line(prefix, field):
    nullable = "nullable" if field["nullable"] else "required"
    sensitive = "sensitive" if field["sensitive"] else "public"
    return f"{prefix} {field['name']} {field['type']} {nullable} {sensitive}"


def failure_kind(call):
    try:
        call()
    except Failure as failure:
        return failure
    raise RuntimeError("a call succeeded where it had to fail")


def check_retention(retained, reported, fields, key_fields):
    """Rereads retained events through borrowed frames across collections and threads."""
    views = [event.frame() for event in retained]
    snapshots = [bytes(view) for view in views]
    del retained[:]
    gc.collect()
    churn = [bytearray(size * 64) for size in range(2048)]
    del churn
    gc.collect()
    for view, snapshot in zip(views, snapshots):
        if bytes(view) != snapshot or bytes(view[4:8]) != b"NXSM":
            raise RuntimeError("a borrowed frame changed while only its view kept it alive")
    owners = [view.obj.owner for view in views]
    for owner, lines in zip(owners, reported):
        if owner.row_lines(fields, key_fields) != lines:
            raise RuntimeError("a retained event reads differently than it did")
    # Release the last references on another thread, while the main thread keeps collecting.
    releaser = threading.Thread(target=lambda: (owners.clear(), views.clear(), gc.collect()))
    releaser.start()
    gc.collect()
    releaser.join()


def profile_binding(event, fields, output_dir):
    """Record raw host/FFI costs on a live Row event without changing the conformance report."""
    count = event.row_count()
    frame_bytes = len(event.frame())
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

    def retain_release():
        retained = nx_event_retain(event.handle)
        nx_event_release(retained)

    fixed_index = next(index for index, field in enumerate(fields) if field["type"] == "I64")
    fixed = (ctypes.c_int64 * count)()

    def copy_fixed():
        check(nx_event_column_fixed(
            event.handle, PART_ROWS, fixed_index, fixed, ctypes.sizeof(fixed)
        ))

    text_index = next(index for index, field in enumerate(fields) if field["type"] == "STRING")
    data_len = SIZE()
    check(nx_event_column_varlen(
        event.handle, PART_ROWS, text_index, None, 0, None, 0, ctypes.byref(data_len)
    ))
    offsets = (ctypes.c_uint64 * (count + 1))()
    data = (ctypes.c_uint8 * data_len.value)()

    def copy_text():
        check(nx_event_column_varlen(
            event.handle, PART_ROWS, text_index, offsets, count + 1,
            data, data_len.value, ctypes.byref(data_len)
        ))

    measurements = {
        "borrowed_frame_view": measure(event.frame),
        "owned_frame_copy": measure(lambda: bytes(event.frame())),
        "ffi_retain_release": measure(retain_release),
        "ffi_fixed_column": measure(copy_fixed),
        "ffi_varlen_column": measure(copy_text),
    }
    disposal = []
    for _ in range(20):
        started = time.perf_counter_ns()
        owners = [event.retain() for _ in range(256)]
        del owners
        gc.collect()
        disposal.append(time.perf_counter_ns() - started)
    report = {
        "schema_version": 1,
        "runtime": sys.version,
        "row_count": count,
        "frame_bytes": frame_bytes,
        "samples_per_stage": samples,
        "samples_nanoseconds": measurements,
        "gc_release_256_refs_nanoseconds": disposal,
        "gc_thresholds": gc.get_threshold(),
    }
    os.makedirs(output_dir, exist_ok=True)
    output = os.path.join(output_dir, f"python-{os.getpid()}.json")
    with open(output, "w", encoding="utf-8") as target:
        json.dump(report, target, indent=2)


def check_cancellation(session):
    cancel = Cancel()
    outcome = {}

    def wait():
        outcome["failure"] = failure_kind(lambda: session.next_event(cancel))

    waiter = threading.Thread(target=wait)
    waiter.start()
    time.sleep(0.1)
    cancel.trigger()
    waiter.join(30)
    if waiter.is_alive() or outcome["failure"].kind != ERROR_CANCELLED:
        raise RuntimeError("a cancelled wait did not report CANCELLED")
    if failure_kind(lambda: session.next_event(Cancel(50))).kind != ERROR_DEADLINE:
        raise RuntimeError("an expired wait did not report DEADLINE")
    cancelled = Cancel()
    cancelled.trigger()
    failure = failure_kind(lambda: session.execute("SHOW DOMAINS;", cancelled))
    if failure.kind != ERROR_CANCELLED or not failure.reference:
        raise RuntimeError("a cancelled command did not report CANCELLED with its reference")


def paced_line(prefix, clock, domain):
    """The report line of a paced clock, with `prefix` naming where it was read. The UTC anchor
    depends on when the scenario's START committed, so it is read but not reported."""
    rate_bits = struct.unpack("<Q", struct.pack("<d", clock["rate"]))[0]
    return (
        f"{prefix} domain={domain} generation={clock['generation']} state=paced "
        f"period={clock['period']} skew={clock['skew']} origin={clock['origin']} "
        f"rate=f64:{rate_bits:016x}"
    )


def check_tick(clock, tick):
    """Holds a tick to a clock: the same generation, a boundary of the logical origin plus one
    period for every id before it, and a serving node's reading that never precedes the origin."""
    if tick["generation"] != clock["generation"]:
        raise RuntimeError("a tick of one generation followed the state of another")
    if tick["id"] == 0 or tick["boundary"] != clock["origin"] + (tick["id"] - 1) * clock["period"]:
        raise RuntimeError(
            "a tick's boundary is not the origin plus one period for every id before it"
        )
    if tick["serving_logical"] < clock["origin"]:
        raise RuntimeError("the serving node's logical reading precedes the logical origin")


def tick_line(clock, tick, domain):
    """The report line of a tick a clock holds."""
    check_tick(clock, tick)
    return f"TICK domain={domain} generation={tick['generation']} boundary=origin+(id-1)*period"


def projection_line(clock, read, domain):
    """The report line of the projections of `read`, which holds `clock`, at its own UTC anchor: the
    logical time there, the wait for the next tick center, the admission window, and whether an
    event at the skew's edge and one nanosecond past it are admitted."""
    anchor, origin = clock["anchor"], clock["origin"]

    def relative(instant):
        return "origin" if instant == origin else str(instant)

    def admission(admitted):
        return "admitted" if admitted else "refused"

    window = read.admission_window(anchor)
    if window is None:
        raise RuntimeError("a paced clock reports no admission window")
    edge = origin + clock["skew"]
    return (
        f"PROJECTION domain={domain} generation={clock['generation']} "
        f"anchor={relative(read.logical_time_at(anchor))} "
        f"wait={read.wall_duration_until(anchor, origin + clock['period'])} "
        f"window={relative(window[0])}..{relative(window[1])} "
        f"skew={admission(read.admits(anchor, edge))} "
        f"beyond={admission(read.admits(anchor, edge + 1))}"
    )


def check_clock_cancellation(session, domain):
    """Cancels a clock wait from another thread, then lets a deadline end another. The session
    follows no clock yet, so nothing but its token ends either wait."""
    cancel = Cancel()
    outcome = {}

    def wait():
        outcome["failure"] = failure_kind(lambda: session.next_clock_event(cancel, domain))

    waiter = threading.Thread(target=wait)
    waiter.start()
    time.sleep(0.1)
    cancel.trigger()
    waiter.join(30)
    if waiter.is_alive() or outcome["failure"].kind != ERROR_CANCELLED:
        raise RuntimeError("a cancelled clock wait did not report CANCELLED")
    expired = failure_kind(lambda: session.next_clock_event(Cancel(50), domain))
    if expired.kind != ERROR_DEADLINE:
        raise RuntimeError("an expired clock wait did not report DEADLINE")


class FollowedClock:
    """What the probe has read about the domain's clock: the generation of the newest state, its
    mapping while it is paced, and whether the session holding the attachment ended since. Every
    event is held to what was read before it, and a read of the clock taken right after it is held
    to be no older."""

    def __init__(self, session, domain, clock):
        self.session = session
        self.domain = domain
        self.generation = clock["generation"]
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
        event = self.session.next_clock_event(deadline, self.domain)
        kind = event.kind()
        if kind == "STATE":
            self.observe(event)
        elif kind == "TICK":
            self.check_tick(event)
        elif kind == "INTERRUPTED":
            self.interrupted = True
        elif kind == "ENDED":
            raise RuntimeError("the server ended the attachment")
        return event

    def observe(self, event):
        generation = event.generation()
        if generation < self.generation:
            raise RuntimeError("a state went back to an earlier generation")
        state = event.state()
        paced = event.paced() if state == CLOCK_PACED else None
        read = self.read()
        if read.generation() < generation:
            raise RuntimeError("a read of the clock is older than the state the probe took")
        if read.generation() == generation:
            if read.state() != state:
                raise RuntimeError("a read of the clock differs from the state of its generation")
            if paced is not None and read.paced() != paced:
                raise RuntimeError("a read of the clock differs from the mapping of its generation")
        self.generation = generation
        self.paced = paced
        self.interrupted = False

    def check_tick(self, event):
        if self.interrupted:
            raise RuntimeError("a tick arrived before the restored attachment reported its clock")
        tick = event.tick()
        check_tick(self.clock(), tick)
        read = self.read()
        if read.generation() < tick["generation"]:
            raise RuntimeError("a read of the clock is older than the tick the probe took")
        held = read.tick_id()
        if read.generation() == tick["generation"] and held is not None and held < tick["id"]:
            raise RuntimeError("a read of the clock holds an older tick than the probe took")

    def first_tick(self, deadline):
        """The first tick of the followed generation. A state reporting that generation again is
        taken on the way; one of another generation fails the probe."""
        generation = self.generation
        while True:
            event = self.next(deadline)
            if event.kind() == "TICK":
                return event
            if event.kind() != "STATE" or self.generation != generation:
                raise RuntimeError(
                    f"the clock reported {event.kind()} before the first tick of generation "
                    f"{generation}"
                )

    def next_generation(self, deadline):
        """The paced state of a generation after the followed one. The followed generation's
        ticks and the states before the new paced one are taken on the way."""
        previous = self.generation
        while True:
            event = self.next(deadline)
            kind = event.kind()
            if kind == "STATE" and self.generation > previous and self.paced is not None:
                return event
            if kind not in ("TICK", "STATE"):
                raise RuntimeError(f"the clock reported {kind} before a generation after {previous}")

    def interruption(self, deadline):
        """Waits for the interruption of the attachment. The followed generation's ticks and
        states are taken on the way."""
        generation = self.generation
        while True:
            event = self.next(deadline)
            kind = event.kind()
            if kind == "INTERRUPTED":
                return
            if kind != "TICK" and (kind != "STATE" or self.generation != generation):
                raise RuntimeError(f"the clock reported {kind} before the interruption")

    def restored(self, deadline):
        """The paced state the restored attachment reports. A refused restoration, which the
        session repeats, and a clock reported uninstalled are taken on the way."""
        while True:
            event = self.next(deadline)
            if event.kind() == "STATE" and self.paced is not None:
                return event


def report_state_and_first_tick(followed, state, deadline, domain, report):
    """Reports the paced state a STATE event reports and the first tick after it."""
    report(paced_line("STATE", state.paced(), domain))
    tick = followed.first_tick(deadline)
    report(tick_line(followed.clock(), tick.tick(), domain))


def run_clock(session, domain, report):
    """Attaches to the domain's running clock and reads the clock the attach reported before its
    first tick, then follows the generation the scenario's STOP and START begin and the attachment
    restored after the scenario ends the session, and detaches."""
    check_clock_cancellation(session, domain)
    report(f"ATTACHED {session.execute('ATTACH DOMAIN CLOCK;').disposition()}")

    # The clock the attach reported, read before any event about the attachment.
    read = session.domain_clock(domain)
    if read is None:
        raise RuntimeError("the session follows no clock after its attach completed")
    if read.state() != CLOCK_PACED:
        raise RuntimeError("the attach reported a clock other than the running paced one")
    clock = read.paced()
    reported_clock = paced_line("CLOCK", clock, domain)
    report(reported_clock)
    report(projection_line(clock, read, domain))

    followed = FollowedClock(session, domain, clock)
    tick = followed.first_tick(Cancel(120_000))
    reported_tick = tick_line(clock, tick.tick(), domain)
    report(reported_tick)

    # Keep a second reference to the read and the tick and release the first ones on another
    # thread, so both must read the same on the second alone.
    retained_read, retained_tick = read.retain(), tick.retain()
    firsts = [read, tick]
    del read, tick
    releaser = threading.Thread(target=lambda: (firsts.clear(), gc.collect()))
    releaser.start()
    releaser.join()
    clock_again = retained_read.paced()
    if (
        paced_line("CLOCK", clock_again, domain) != reported_clock
        or tick_line(clock_again, retained_tick.tick(), domain) != reported_tick
    ):
        raise RuntimeError("a retained clock or tick reads differently than it did")

    # The scenario stops the domain and starts it again at another origin and rate.
    deadline = Cancel(120_000)
    report_state_and_first_tick(followed, followed.next_generation(deadline), deadline, domain, report)

    # The scenario ends the session, and the binding attaches the clock again on the next one.
    deadline = Cancel(120_000)
    followed.interruption(deadline)
    report(f"INTERRUPTED domain={domain}")
    report_state_and_first_tick(followed, followed.restored(deadline), deadline, domain, report)

    report(f"DETACHED {session.execute('DETACH DOMAIN CLOCK;').disposition()}")
    report("CHECKS ok")
    session.close()
    report("PASS")


# ---- Producers and consumers -----------------------------------------------------------------

WAIT_MILLIS = 120_000
EXPIRING_MILLIS = 200
PRODUCER_BATCHES = 2
CONSUMER_BATCHES = 4
ENDPOINT_BYTES = 1_048_576
SCRIBBLE = 0xAA

ERROR_INVALID_ARGUMENT = 1
ERROR_REJECTED = 5
ERROR_INTERRUPTED = 12
OPEN_SCHEMA_MISMATCH = 6
TYPE_CODES = {name: code for code, name in TYPES.items()}
STATES = {1: "active", 2: "interrupted", 3: "restoring", 4: "reopen_required", 5: "closed"}
ENDPOINT_ACTIVE, ENDPOINT_CLOSED = 1, 5
SETTLEMENTS = {
    1: "confirmed", 2: "stale_reference", 3: "wrong_consumer", 4: "invalid_reason",
    5: "consumer_ended",
}
REFUSALS = {
    1: "invalid_batch", 2: "suspended", 3: "busy", 4: "draining", 5: "producer_ended",
    6: "credit_exceeded",
}
DEFECTS = {
    1: "malformed", 2: "unexpected_message", 3: "compressed", 4: "schema_mismatch",
    5: "not_one_batch", 6: "too_many_rows", 7: "too_large", 8: "invalid_data",
}
FAILURES = {1: "ack_timed_out", 2: "rejected"}
UNCERTAINTIES = {1: "interrupted", 2: "owner_lost", 3: "session_lost"}
SUBMISSION_COMPLETED, SUBMISSION_NOT_ADMITTED, SUBMISSION_FAILED, SUBMISSION_UNKNOWN = 2, 1, 3, 4
REFUSAL_INVALID_BATCH = 1
ADMISSION_OPEN = 1
ACK_SEQUENTIAL = 1

I32_P = ctypes.POINTER(ctypes.c_int32)
U32_P = ctypes.POINTER(ctypes.c_uint32)
U64_P = ctypes.POINTER(ctypes.c_uint64)
BOOL_P = ctypes.POINTER(ctypes.c_bool)

nx_error_open_refusal = declare("nx_error_open_refusal", ctypes.c_bool, HANDLE, I32_P)
nx_fields_new = declare("nx_fields_new", HANDLE)
nx_fields_add = declare(
    "nx_fields_add", HANDLE, HANDLE, ctypes.c_char_p, SIZE, ctypes.c_int32, ctypes.c_uint32,
    ctypes.c_bool, ctypes.c_bool,
)
nx_fields_element = declare(
    "nx_fields_element", HANDLE, HANDLE, ctypes.c_int32, ctypes.c_uint32
)
nx_fields_free = declare("nx_fields_free", None, HANDLE)
nx_schema_field_levels = declare(
    "nx_schema_field_levels", HANDLE, HANDLE, ctypes.c_int32, SIZE, OUT_SIZE
)
nx_schema_field_level = declare(
    "nx_schema_field_level", HANDLE, HANDLE, ctypes.c_int32, SIZE, SIZE, I32_P, U32_P
)
nx_session_open_ingestor = declare(
    "nx_session_open_ingestor", HANDLE, HANDLE, ctypes.c_char_p, SIZE, ctypes.c_char_p, SIZE,
    HANDLE, ctypes.c_uint32, ctypes.c_uint64, HANDLE, OUT_HANDLE,
)
nx_session_subscribe_emitter = declare(
    "nx_session_subscribe_emitter", HANDLE, HANDLE, ctypes.c_char_p, SIZE, ctypes.c_char_p, SIZE,
    HANDLE, ctypes.c_uint32, ctypes.c_uint64, HANDLE, OUT_HANDLE,
)
nx_producer_schema = declare("nx_producer_schema", HANDLE, HANDLE, OUT_HANDLE)
nx_producer_generation = declare("nx_producer_generation", ctypes.c_uint64, HANDLE)
nx_producer_grant = declare("nx_producer_grant", None, HANDLE, U32_P, U64_P, U32_P, U64_P)
nx_producer_policy = declare(
    "nx_producer_policy", None, HANDLE, I32_P, U64_P, U64_P, U64_P, U64_P
)
nx_producer_admission = declare("nx_producer_admission", ctypes.c_int32, HANDLE)
nx_producer_state = declare("nx_producer_state", ctypes.c_int32, HANDLE)
nx_producer_submit = declare("nx_producer_submit", HANDLE, HANDLE, HANDLE, HANDLE, U64_P)
nx_producer_submit_ipc = declare(
    "nx_producer_submit_ipc", HANDLE, HANDLE, ctypes.c_void_p, SIZE, HANDLE, U64_P
)
nx_producer_rejoin = declare(
    "nx_producer_rejoin", HANDLE, HANDLE, ctypes.c_uint64, HANDLE, OUT_HANDLE
)
nx_producer_pending = declare(
    "nx_producer_pending", HANDLE, HANDLE, U64_P, BOOL_P, SIZE, OUT_SIZE
)
nx_producer_close = declare("nx_producer_close", HANDLE, HANDLE, HANDLE)
nx_producer_free = declare("nx_producer_free", None, HANDLE)
nx_submission_outcome_result = declare("nx_submission_outcome_result", ctypes.c_int32, HANDLE)
nx_submission_outcome_refusal = declare("nx_submission_outcome_refusal", HANDLE, HANDLE, I32_P)
nx_submission_outcome_defect = declare("nx_submission_outcome_defect", HANDLE, HANDLE, I32_P)
nx_submission_outcome_failure = declare("nx_submission_outcome_failure", HANDLE, HANDLE, I32_P)
nx_submission_outcome_uncertainty = declare(
    "nx_submission_outcome_uncertainty", HANDLE, HANDLE, I32_P
)
nx_submission_outcome_free = declare("nx_submission_outcome_free", None, HANDLE)
nx_consumer_schema = declare("nx_consumer_schema", HANDLE, HANDLE, OUT_HANDLE)
nx_consumer_generation = declare("nx_consumer_generation", ctypes.c_uint64, HANDLE)
nx_consumer_grant = declare("nx_consumer_grant", None, HANDLE, U32_P, U64_P, U32_P, U64_P)
nx_consumer_policy = declare(
    "nx_consumer_policy", None, HANDLE, I32_P, U64_P, U64_P, U64_P, U64_P
)
nx_consumer_state = declare("nx_consumer_state", ctypes.c_int32, HANDLE)
nx_consumer_next = declare("nx_consumer_next", HANDLE, HANDLE, HANDLE, OUT_HANDLE)
nx_consumer_close = declare("nx_consumer_close", HANDLE, HANDLE, HANDLE)
nx_consumer_free = declare("nx_consumer_free", None, HANDLE)
nx_delivery_identity = declare("nx_delivery_identity", None, HANDLE, OUT_BYTES, OUT_SIZE)
nx_delivery_reference = declare("nx_delivery_reference", None, HANDLE, OUT_BYTES, OUT_SIZE)
nx_delivery_source_relay = declare("nx_delivery_source_relay", None, HANDLE, OUT_BYTES, OUT_SIZE)
nx_delivery_branch_fingerprint = declare(
    "nx_delivery_branch_fingerprint", ctypes.c_bool, HANDLE, OUT_BYTES, OUT_SIZE
)
nx_delivery_members = declare("nx_delivery_members", ctypes.c_uint32, HANDLE)
nx_delivery_ipc = declare("nx_delivery_ipc", None, HANDLE, OUT_BYTES, OUT_SIZE)
nx_delivery_batch = declare("nx_delivery_batch", HANDLE, HANDLE, OUT_HANDLE)
nx_delivery_ack = declare("nx_delivery_ack", HANDLE, HANDLE, HANDLE, I32_P)
nx_delivery_retry = declare("nx_delivery_retry", HANDLE, HANDLE, HANDLE, I32_P)
nx_delivery_reject = declare(
    "nx_delivery_reject", HANDLE, HANDLE, ctypes.c_char_p, SIZE, HANDLE, I32_P
)
nx_delivery_retain = declare("nx_delivery_retain", HANDLE, HANDLE)
nx_delivery_release = declare("nx_delivery_release", None, HANDLE)
nx_batch_row_count = declare("nx_batch_row_count", SIZE, HANDLE)
nx_batch_ipc = declare("nx_batch_ipc", HANDLE, HANDLE, OUT_BYTES, OUT_SIZE)
nx_batch_cells = declare("nx_batch_cells", HANDLE, HANDLE, SIZE, SIZE, OUT_SIZE)
nx_batch_states = declare("nx_batch_states", HANDLE, HANDLE, SIZE, BYTES_P, SIZE)
nx_batch_offsets = declare("nx_batch_offsets", HANDLE, HANDLE, SIZE, SIZE, U64_P, SIZE)
nx_batch_fixed = declare("nx_batch_fixed", HANDLE, HANDLE, SIZE, SIZE, ctypes.c_void_p, SIZE)
nx_batch_varlen = declare(
    "nx_batch_varlen", HANDLE, HANDLE, SIZE, SIZE, U64_P, SIZE, BYTES_P, SIZE, OUT_SIZE
)
nx_batch_retain = declare("nx_batch_retain", HANDLE, HANDLE)
nx_batch_release = declare("nx_batch_release", None, HANDLE)
nx_batch_builder_new = declare("nx_batch_builder_new", HANDLE, HANDLE, SIZE, OUT_HANDLE)
nx_batch_builder_states = declare(
    "nx_batch_builder_states", HANDLE, HANDLE, SIZE, BYTES_P, SIZE
)
nx_batch_builder_offsets = declare(
    "nx_batch_builder_offsets", HANDLE, HANDLE, SIZE, SIZE, U64_P, SIZE
)
nx_batch_builder_fixed = declare(
    "nx_batch_builder_fixed", HANDLE, HANDLE, SIZE, SIZE, ctypes.c_void_p, SIZE
)
nx_batch_builder_varlen = declare(
    "nx_batch_builder_varlen", HANDLE, HANDLE, SIZE, SIZE, U64_P, SIZE, BYTES_P, SIZE
)
nx_batch_builder_finish = declare("nx_batch_builder_finish", HANDLE, HANDLE, OUT_HANDLE)
nx_batch_builder_free = declare("nx_batch_builder_free", None, HANDLE)

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
    """The typed rows of the first batch."""
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


# The struct format of each fixed-width scalar the probe writes, in native byte order.
PACKING = {
    "U8": "=B", "I8": "=b", "U16": "=H", "I16": "=h", "U32": "=I", "I32": "=i", "U64": "=Q",
    "I64": "=q", "F32": "=I", "F64": "=Q", "BOOL": "=B", "DATETIME": "=q",
}


def host_column(name, rows):
    """The column `name` of `rows`, as a host lays it out: row states or None, the offsets of its
    level-0 list or None, the level its values fill, and either fixed bytes or offsets and data."""
    column = {"states": None, "offsets": None, "leaf": 0, "fixed": None, "varlen": None}

    def varlen(values):
        offsets, data = [0], bytearray()
        for value in values:
            data += value
            offsets.append(len(data))
        column["varlen"] = (offsets, bytes(data))

    def states(present):
        column["states"] = bytes(CELL_VALUE if value else CELL_NULL for value in present)

    source = "id" if name == "echo" else name
    kinds = {
        "id": "U32", "u8v": "U8", "i8v": "I8", "u16v": "U16", "i16v": "I16", "u32v": "U32",
        "i32v": "I32", "u64v": "U64", "i64v": "I64", "f32v": "F32", "f64v": "F64",
        "flag": "BOOL", "at": "DATETIME",
    }
    if source in kinds:
        packing = PACKING[kinds[source]]
        column["fixed"] = b"".join(struct.pack(packing, int(row[source])) for row in rows)
    elif name in ("tenant", "secret"):
        varlen(row[name].encode() for row in rows)
    elif name in ("text", "raw"):
        states(row[name] is not None for row in rows)
        varlen(row[name] or b"" for row in rows)
    elif name == "maybe":
        states(row["maybe"] is not None for row in rows)
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
        states(row["spans"] is not None for row in rows)
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


def fields_handle(specs):
    handle = nx_fields_new()
    for name, levels, nullable, sensitive in specs:
        kind, length = levels[0]
        check(nx_fields_add(handle, *text(name), TYPE_CODES[kind], length, nullable, sensitive))
        for kind, length in levels[1:]:
            check(nx_fields_element(handle, TYPE_CODES[kind], length))
    return handle


def type_text(levels):
    """A field's type in NSPL's spelling, with nested fixed-size lists as dimensions."""
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
    count = SIZE()
    check(nx_schema_field_count(schema, PART_ROWS, ctypes.byref(count)))
    fields = []
    for index in range(count.value):
        name, name_len = BYTES_P(), SIZE()
        kind, nullable, sensitive = ctypes.c_int32(), ctypes.c_bool(), ctypes.c_bool()
        check(
            nx_schema_field(
                schema, PART_ROWS, index, ctypes.byref(name), ctypes.byref(name_len),
                ctypes.byref(kind), ctypes.byref(nullable), ctypes.byref(sensitive),
            )
        )
        level_count = SIZE()
        check(nx_schema_field_levels(schema, PART_ROWS, index, ctypes.byref(level_count)))
        levels = []
        for level in range(level_count.value):
            level_kind, length = ctypes.c_int32(), ctypes.c_uint32()
            check(
                nx_schema_field_level(
                    schema, PART_ROWS, index, level, ctypes.byref(level_kind),
                    ctypes.byref(length),
                )
            )
            levels.append((TYPES[level_kind.value], length.value))
        if TYPES[kind.value] != levels[0][0]:
            raise RuntimeError("a field's first level differs from its type")
        fields.append(
            {
                "name": borrowed(name, name_len).decode(), "levels": levels,
                "nullable": nullable.value, "sensitive": sensitive.value,
            }
        )
    return fields


def reported_line(prefix, field):
    nullability = "nullable" if field["nullable"] else "required"
    sensitivity = "sensitive" if field["sensitive"] else "public"
    return f"{prefix} {field['name']} {type_text(field['levels'])} {nullability} {sensitivity}"


class Batch:
    """One reference to a batch, released when the object is collected."""

    def __init__(self, handle):
        self.handle = handle

    def __del__(self):
        nx_batch_release(self.handle)

    def retain(self):
        return Batch(nx_batch_retain(self.handle))

    @staticmethod
    def build(schema, rows):
        """Builds a batch of `rows` for `schema`, overwriting every buffer as soon as the binding
        has copied it."""
        builder = HANDLE()
        check(nx_batch_builder_new(schema, len(rows), ctypes.byref(builder)))
        try:
            for index, field in enumerate(reported_fields(schema)):
                column = host_column(field["name"], rows)
                if column["states"] is not None:
                    buffer = (ctypes.c_uint8 * len(column["states"])).from_buffer_copy(
                        column["states"]
                    )
                    check(nx_batch_builder_states(builder, index, buffer, len(buffer)))
                    ctypes.memset(buffer, SCRIBBLE, len(buffer))
                if column["offsets"] is not None:
                    offsets = (ctypes.c_uint64 * len(column["offsets"]))(*column["offsets"])
                    check(nx_batch_builder_offsets(builder, index, 0, offsets, len(offsets)))
                    ctypes.memset(offsets, 0xFF, ctypes.sizeof(offsets))
                if column["varlen"] is not None:
                    value_offsets, data = column["varlen"]
                    offsets = (ctypes.c_uint64 * len(value_offsets))(*value_offsets)
                    buffer = (ctypes.c_uint8 * max(len(data), 1)).from_buffer_copy(
                        data or b"\x00"
                    )
                    check(
                        nx_batch_builder_varlen(
                            builder, index, column["leaf"], offsets, len(offsets), buffer,
                            len(data),
                        )
                    )
                    ctypes.memset(offsets, 0xFF, ctypes.sizeof(offsets))
                    ctypes.memset(buffer, SCRIBBLE, len(buffer))
                else:
                    fixed = column["fixed"]
                    buffer = (ctypes.c_uint8 * max(len(fixed), 1)).from_buffer_copy(
                        fixed or b"\x00"
                    )
                    check(
                        nx_batch_builder_fixed(builder, index, column["leaf"], buffer, len(fixed))
                    )
                    ctypes.memset(buffer, SCRIBBLE, len(buffer))
            batch = HANDLE()
            check(nx_batch_builder_finish(builder, ctypes.byref(batch)))
            return Batch(batch)
        finally:
            nx_batch_builder_free(builder)

    def stream(self):
        """A memoryview of the batch's stream, borrowed without a copy; it keeps the batch alive."""
        ipc, ipc_len = BYTES_P(), SIZE()
        check(nx_batch_ipc(self.handle, ctypes.byref(ipc), ctypes.byref(ipc_len)))
        address = ctypes.cast(ipc, ctypes.c_void_p).value
        buffer = (ctypes.c_uint8 * ipc_len.value).from_address(address)
        buffer.owner = self
        return memoryview(buffer).cast("B")

    def cells(self, column, level):
        cells = SIZE()
        check(nx_batch_cells(self.handle, column, level, ctypes.byref(cells)))
        return cells.value

    def column(self, index, field):
        """Reads one column in one call per level of its type, and renders each row."""
        rows = self.cells(index, 0)
        states = (ctypes.c_uint8 * max(rows, 1))()
        check(nx_batch_states(self.handle, index, states, rows))
        levels = field["levels"]
        innermost = len(levels) - 1
        lists = []
        for level, (kind, length) in enumerate(levels[:innermost]):
            if kind == "LIST":
                count = self.cells(index, level) + 1
                offsets = (ctypes.c_uint64 * count)()
                check(nx_batch_offsets(self.handle, index, level, offsets, count))
                lists.append(list(offsets))
            else:
                lists.append(length)
        kind = levels[innermost][0]
        cells = self.cells(index, innermost)
        if kind in ("STRING", "BYTES"):
            offsets = (ctypes.c_uint64 * (cells + 1))()
            data_len = SIZE()
            check(
                nx_batch_varlen(
                    self.handle, index, innermost, offsets, cells + 1, None, 0,
                    ctypes.byref(data_len),
                )
            )
            data = (ctypes.c_uint8 * max(data_len.value, 1))()
            check(
                nx_batch_varlen(
                    self.handle, index, innermost, offsets, cells + 1, data, data_len.value,
                    ctypes.byref(data_len),
                )
            )
            raw = bytes(data)[: data_len.value]
            prefix = "str" if kind == "STRING" else "bytes"
            leaves = [f"{prefix}:{raw[offsets[cell]:offsets[cell + 1]].hex()}" for cell in range(cells)]
        else:
            element, render = FIXED[kind]
            values = (element * max(cells, 1))()
            if cells:
                check(nx_batch_fixed(self.handle, index, innermost, values, ctypes.sizeof(element) * cells))
            leaves = [render(values[cell]) for cell in range(cells)]

        def render_cell(level, cell):
            if level == len(lists):
                return leaves[cell]
            shape = lists[level]
            if isinstance(shape, list):
                start, end = shape[cell], shape[cell + 1]
            else:
                start, end = cell * shape, (cell + 1) * shape
            return "[" + ",".join(render_cell(level + 1, element) for element in range(start, end)) + "]"

        return [
            "null" if states[row] == CELL_NULL else render_cell(0, row) for row in range(rows)
        ]

    def rows(self, fields):
        columns = [self.column(index, field) for index, field in enumerate(fields)]
        count = nx_batch_row_count(self.handle)
        return [
            "ROW " + " ".join(f"{field['name']}={column[row]}" for field, column in zip(fields, columns))
            for row in range(count)
        ]

    def ids(self, fields):
        for index, field in enumerate(fields):
            if field["name"] == "id":
                return [int(value.split(":")[1]) for value in self.column(index, field)]
        raise RuntimeError("the batch has no id column")


class Delivery:
    """One reference to a delivery, released when the object is collected."""

    def __init__(self, handle):
        self.handle = handle

    def __del__(self):
        nx_delivery_release(self.handle)

    def retain(self):
        return Delivery(nx_delivery_retain(self.handle))

    def _bytes(self, read):
        data, data_len = BYTES_P(), SIZE()
        read(self.handle, ctypes.byref(data), ctypes.byref(data_len))
        return borrowed(data, data_len)

    def identity(self):
        return self._bytes(nx_delivery_identity)

    def reference(self):
        return self._bytes(nx_delivery_reference)

    def stream(self):
        return self._bytes(nx_delivery_ipc)

    def summary(self):
        relay = self._bytes(nx_delivery_source_relay).decode()
        fingerprint, fingerprint_len = BYTES_P(), SIZE()
        branched = nx_delivery_branch_fingerprint(
            self.handle, ctypes.byref(fingerprint), ctypes.byref(fingerprint_len)
        )
        branch = str(fingerprint_len.value) if branched else "none"
        return (
            f"DELIVERY relay={relay} members={nx_delivery_members(self.handle)} branch={branch} "
            f"identity={len(self.identity())} reference={len(self.reference())}"
        )

    def batch(self):
        batch = HANDLE()
        check(nx_delivery_batch(self.handle, ctypes.byref(batch)))
        return Batch(batch)

    def settle(self, settle, *reason):
        deadline = Cancel(WAIT_MILLIS)
        settlement = ctypes.c_int32()
        check(settle(self.handle, *reason, deadline.handle, ctypes.byref(settlement)))
        return SETTLEMENTS[settlement.value]


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
        producer = HANDLE()
        check(
            nx_session_open_ingestor(
                self.session.handle, *text(self.domain), *text(self.ingestor), fields, batches,
                asked, None, ctypes.byref(producer),
            )
        )
        return producer

    def open_consumer(self, fields):
        consumer = HANDLE()
        check(
            nx_session_subscribe_emitter(
                self.session.handle, *text(self.domain), *text(self.emitter), fields,
                CONSUMER_BATCHES, ENDPOINT_BYTES, None, ctypes.byref(consumer),
            )
        )
        return consumer

    @staticmethod
    def submit(producer, batch, cancel=None):
        submission = ctypes.c_uint64()
        check(
            nx_producer_submit(
                producer, batch.handle, cancel.handle if cancel else None,
                ctypes.byref(submission),
            )
        )
        return submission.value

    @staticmethod
    def outcome(producer, submission):
        deadline = Cancel(WAIT_MILLIS)
        outcome = HANDLE()
        check(nx_producer_rejoin(producer, submission, deadline.handle, ctypes.byref(outcome)))
        try:
            result = nx_submission_outcome_result(outcome)
            if result == SUBMISSION_COMPLETED:
                return "completed"
            cause = ctypes.c_int32()
            if result == SUBMISSION_NOT_ADMITTED:
                check(nx_submission_outcome_refusal(outcome, ctypes.byref(cause)))
                text_out = f"not_admitted {REFUSALS[cause.value]}"
                if cause.value == REFUSAL_INVALID_BATCH:
                    check(nx_submission_outcome_defect(outcome, ctypes.byref(cause)))
                    text_out += f" {DEFECTS[cause.value]}"
                return text_out
            if result == SUBMISSION_FAILED:
                check(nx_submission_outcome_failure(outcome, ctypes.byref(cause)))
                return f"processing_failed {FAILURES[cause.value]}"
            check(nx_submission_outcome_uncertainty(outcome, ctypes.byref(cause)))
            return f"outcome_unknown {UNCERTAINTIES[cause.value]}"
        finally:
            nx_submission_outcome_free(outcome)

    @staticmethod
    def next(consumer, cancel=None):
        deadline = cancel or Cancel(WAIT_MILLIS)
        delivery = HANDLE()
        check(nx_consumer_next(consumer, deadline.handle, ctypes.byref(delivery)))
        return Delivery(delivery)

    @staticmethod
    def policy(read, handle):
        window, outstanding = ctypes.c_int32(), ctypes.c_uint64()
        timeout, backoff, maximum = ctypes.c_uint64(), ctypes.c_uint64(), ctypes.c_uint64()
        read(
            handle, ctypes.byref(window), ctypes.byref(outstanding), ctypes.byref(timeout),
            ctypes.byref(backoff), ctypes.byref(maximum),
        )
        name = "sequential" if window.value == ACK_SEQUENTIAL else "parallel"
        return f"window={name}/{outstanding.value} ack_timeout={timeout.value}"

    @staticmethod
    def grant(read, handle):
        batches, granted = ctypes.c_uint32(), ctypes.c_uint64()
        max_rows, max_bytes = ctypes.c_uint32(), ctypes.c_uint64()
        read(
            handle, ctypes.byref(batches), ctypes.byref(granted), ctypes.byref(max_rows),
            ctypes.byref(max_bytes),
        )
        return batches.value, granted.value, max_rows.value, max_bytes.value

    def run(self):
        report = self.report
        input_fields = fields_handle(INPUT_FIELDS)
        output_fields = fields_handle(INPUT_FIELDS + [ECHO_FIELD])
        mismatched = fields_handle(
            [
                (name, levels, nullable, False if name == "secret" else sensitive)
                for name, levels, nullable, sensitive in INPUT_FIELDS
            ]
        )

        # An open whose expected fields differ from the endpoint's is refused exactly.
        for what in ("producer", "consumer"):
            producer_or_consumer = HANDLE()
            if what == "producer":
                error = nx_session_open_ingestor(
                    self.session.handle, *text(self.domain), *text(self.ingestor), mismatched,
                    PRODUCER_BATCHES, ENDPOINT_BYTES, None, ctypes.byref(producer_or_consumer),
                )
            else:
                error = nx_session_subscribe_emitter(
                    self.session.handle, *text(self.domain), *text(self.emitter), input_fields,
                    CONSUMER_BATCHES, ENDPOINT_BYTES, None, ctypes.byref(producer_or_consumer),
                )
            refusal = ctypes.c_int32()
            if (
                not error or nx_error_kind_of(error) != ERROR_REJECTED
                or not nx_error_open_refusal(error, ctypes.byref(refusal))
                or refusal.value != OPEN_SCHEMA_MISMATCH
            ):
                raise RuntimeError("an open with another schema was not refused exactly")
            nx_error_free(error)
            report(f"REFUSED {what} schema mismatch")
        nx_fields_free(mismatched)

        consumer = self.open_consumer(output_fields)
        producer = self.open_producer(input_fields, PRODUCER_BATCHES, ENDPOINT_BYTES)
        schema = HANDLE()
        check(nx_consumer_schema(consumer, ctypes.byref(schema)))
        output_schema = schema
        schema = HANDLE()
        check(nx_producer_schema(producer, ctypes.byref(schema)))
        input_schema = schema
        output = reported_fields(output_schema)
        inputs = reported_fields(input_schema)

        batches, granted, max_rows, max_bytes = self.grant(nx_consumer_grant, consumer)
        report(
            f"CONSUMER generation={nx_consumer_generation(consumer)} "
            f"state={STATES[nx_consumer_state(consumer)]} "
            f"{self.policy(nx_consumer_policy, consumer)} credit={batches}/{granted} "
            f"max={max_rows}/{max_bytes}"
        )
        for field in output:
            report(reported_line("OUTPUT FIELD", field))
        batches, granted, max_rows, max_bytes = self.grant(nx_producer_grant, producer)
        self.expect(0 < max_bytes <= granted and max_rows > 0, "the batch limits fit the grant")
        admission = "open" if nx_producer_admission(producer) == ADMISSION_OPEN else "suspended"
        report(
            f"PRODUCER generation={nx_producer_generation(producer)} "
            f"state={STATES[nx_producer_state(producer)]} admission={admission} "
            f"{self.policy(nx_producer_policy, producer)} credit={batches}/{granted}"
        )
        for field in inputs:
            report(reported_line("INPUT FIELD", field))
        report("OPENED")

        # A wait for output that nothing produces ends by its deadline, and one cancelled from
        # another thread by its token; the reads they leave behind are the consumer's.
        self.expect(
            failure_kind(lambda: self.next(consumer, Cancel(EXPIRING_MILLIS))).kind == ERROR_DEADLINE,
            "an expired read reports its deadline",
        )
        report("NEXT deadline")
        cancel = Cancel()
        outcome = {}
        waiter = threading.Thread(
            target=lambda: outcome.update(kind=failure_kind(lambda: self.next(consumer, cancel)).kind)
        )
        waiter.start()
        time.sleep(0.1)
        cancel.trigger()
        waiter.join()
        self.expect(outcome.get("kind") == ERROR_CANCELLED, "a cancelled read reports it")
        report("NEXT cancelled")

        # A batch built for another schema is refused before anything is sent, and the same
        # batch written as a stream by other tooling is refused by the server.
        rows = typed_rows()
        other = Batch.build(output_schema, rows)
        self.expect(
            failure_kind(lambda: self.submit(producer, other)).kind == ERROR_INVALID_ARGUMENT,
            "another schema is the host's argument",
        )
        report("SUBMIT invalid argument")
        foreign = bytes(other.stream())
        buffer = (ctypes.c_uint8 * len(foreign)).from_buffer_copy(foreign)
        submission = ctypes.c_uint64()
        check(nx_producer_submit_ipc(producer, buffer, len(foreign), None, ctypes.byref(submission)))
        ctypes.memset(buffer, SCRIBBLE, len(buffer))
        del other
        report(f"OUTCOME {self.outcome(producer, submission.value)}")

        # The typed batch: its outcome waits for the application's acknowledgement.
        first = self.submit(producer, Batch.build(input_schema, rows))
        report("SUBMITTED first")
        delivery = self.next(consumer)
        report(delivery.summary())
        submissions, resolved, count = (ctypes.c_uint64 * 4)(), (ctypes.c_bool * 4)(), SIZE()
        check(nx_producer_pending(producer, submissions, resolved, 4, ctypes.byref(count)))
        self.expect(
            count.value == 1 and submissions[0] == first and not resolved[0],
            "the only submission waits for its outcome",
        )

        def early():
            rejoined, expiring = HANDLE(), Cancel(EXPIRING_MILLIS)
            check(nx_producer_rejoin(producer, first, expiring.handle, ctypes.byref(rejoined)))

        self.expect(failure_kind(early).kind == ERROR_DEADLINE, "an unacknowledged batch has no outcome")
        report("PENDING first unresolved")
        delivered = delivery.batch()
        printed = delivered.rows(output)
        for line in printed:
            report(line)
        self.expect(
            bytes(delivered.stream()) == delivery.stream(),
            "the batch borrows the stream its delivery carried",
        )

        # A retried attempt comes back with the same identity and a new reference; the first
        # reference is stale from then on.
        report(f"RETRY {delivery.settle(nx_delivery_retry)}")
        again = self.next(consumer)
        self.expect(again.identity() == delivery.identity(), "a retry keeps the identity")
        self.expect(again.reference() != delivery.reference(), "a retry makes a new reference")
        report("REDELIVERED same identity new reference")
        report(f"ACK {delivery.settle(nx_delivery_ack)}")
        del delivery

        # A reference retained here outlives the first, released on another thread, and reads
        # and settles the same attempt.
        retained = again.retain()
        retained_batch = again.batch()
        view = retained_batch.stream()
        before = retained.stream()
        holder = [again]
        del again
        releaser = threading.Thread(target=holder.clear)
        releaser.start()
        releaser.join()
        gc.collect()
        self.expect(retained.stream() == before, "a retained delivery keeps its stream")
        self.expect(bytes(view) == before, "a borrowed view outlives the other references")
        self.expect(retained_batch.rows(output) == printed, "a retained batch reads the same")
        report(f"ACK {retained.settle(nx_delivery_ack)}")
        del retained, retained_batch, view
        report(f"OUTCOME {self.outcome(producer, first)}")

        # An application rejection finishes the batch through the emitter's message error policy.
        second = self.submit(producer, Batch.build(input_schema, [plain_row(10, "beta")]))
        report("SUBMITTED second")
        rejected = self.next(consumer)
        report(f"REJECT {rejected.settle(nx_delivery_reject, *text('application refused'))}")
        del rejected
        report(f"OUTCOME {self.outcome(producer, second)}")

        # Two outstanding batches use up the producer's credit, so a third waits for an outcome.
        fifth_batch = Batch.build(input_schema, [plain_row(24, "acme"), plain_row(25, "acme")])
        third = self.submit(
            producer, Batch.build(input_schema, [plain_row(20, "acme"), plain_row(21, "acme")])
        )
        report("SUBMITTED third")
        fourth = self.submit(
            producer, Batch.build(input_schema, [plain_row(22, "acme"), plain_row(23, "acme")])
        )
        report("SUBMITTED fourth")
        self.expect(
            failure_kind(lambda: self.submit(producer, fifth_batch, Cancel(EXPIRING_MILLIS))).kind
            == ERROR_DEADLINE,
            "a batch beyond the credit waits",
        )
        report("SUBMIT deadline")
        # A batch refused as busy is sent again after the ingestor's backoff, behind the batch
        # submitted after it, so the two outputs may arrive in either order; each keeps its rows.
        outputs = []
        for _ in range(2):
            output_delivery = self.next(consumer)
            outputs.append(output_delivery.batch().ids(output))
            report(f"ACK {output_delivery.settle(nx_delivery_ack)}")
        self.expect(sorted(outputs) == [[20, 21], [22, 23]], "a batch keeps its rows")
        report(f"OUTCOME {self.outcome(producer, third)}")
        report(f"OUTCOME {self.outcome(producer, fourth)}")
        fifth = self.submit(producer, fifth_batch)
        report("SUBMITTED fifth")
        fifth_output = self.next(consumer)
        self.expect(fifth_output.batch().ids(output) == [24, 25], "a batch keeps its rows")
        report(f"ACK {fifth_output.settle(nx_delivery_ack)}")
        report(f"OUTCOME {self.outcome(producer, fifth)}")

        # The scenario cuts the session while one delivery is held unacknowledged.
        extra = self.open_producer(input_fields, 1, 65536)
        report("PRODUCER extra opened")
        held = self.submit(producer, Batch.build(input_schema, [plain_row(30, "gamma")]))
        report("SUBMITTED held")
        held_delivery = self.next(consumer)
        held_identity = held_delivery.identity()
        report("HOLDING")
        self.expect(
            failure_kind(lambda: self.next(consumer)).kind == ERROR_INTERRUPTED,
            "a lost session interrupts the consumer",
        )
        report("NEXT interrupted")
        self.expect(
            failure_kind(lambda: held_delivery.settle(nx_delivery_ack)).kind == ERROR_REJECTED,
            "a delivery of a lost session expired",
        )
        report("ACK expired")
        del held_delivery
        report(f"OUTCOME {self.outcome(producer, held)}")
        waited_until = time.monotonic() + WAIT_MILLIS / 1000
        while nx_producer_state(extra) == ENDPOINT_ACTIVE:
            if time.monotonic() > waited_until:
                raise RuntimeError("the extra producer stayed active after its session ended")
            time.sleep(0.01)
        closing = Cancel(WAIT_MILLIS)
        check(nx_producer_close(extra, closing.handle))
        self.expect(nx_producer_state(extra) == ENDPOINT_CLOSED, "a producer closed on reconnect")
        report("CLOSED extra producer")
        report("WAITING restore")

        # The restored consumer receives the held batch again, and the restored producer
        # publishes.
        redelivered = self.next(consumer)
        self.expect(redelivered.identity() == held_identity, "the held batch keeps its identity")
        report("REDELIVERED held same identity")
        report(f"ACK {redelivered.settle(nx_delivery_ack)}")
        del redelivered
        sixth = self.submit(producer, Batch.build(input_schema, [plain_row(40, "gamma")]))
        report("SUBMITTED sixth")
        sixth_output = self.next(consumer)
        report(f"ACK {sixth_output.settle(nx_delivery_ack)}")
        del sixth_output
        report(f"OUTCOME {self.outcome(producer, sixth)}")
        report(
            f"STATE producer={STATES[nx_producer_state(producer)]} "
            f"extra={STATES[nx_producer_state(extra)]} "
            f"consumer={STATES[nx_consumer_state(consumer)]}"
        )
        check(nx_consumer_close(consumer, closing.handle))
        check(nx_producer_close(producer, closing.handle))
        self.expect(
            nx_consumer_state(consumer) == ENDPOINT_CLOSED
            and nx_producer_state(producer) == ENDPOINT_CLOSED,
            "closed handles read closed",
        )
        report("CLOSED completed")
        if self.failures:
            raise RuntimeError(f"the probe's checks failed: {'; '.join(self.failures)}")
        report("CHECKS ok")
        nx_producer_free(extra)
        nx_producer_free(producer)
        nx_consumer_free(consumer)
        nx_schema_free(input_schema)
        nx_schema_free(output_schema)
        nx_fields_free(input_fields)
        nx_fields_free(output_fields)
        self.session.close()
        report("PASS")


def main():
    environment = os.environ
    domain = environment["NERVIX_PROBE_DOMAIN"]
    session = Session(
        environment["NERVIX_PROBE_GRPC_URI"],
        domain,
        environment["NERVIX_PROBE_USERNAME"],
        environment["NERVIX_PROBE_PASSWORD"],
    )
    report = lambda line: print(line, flush=True)
    if sys.argv[1:] == ["clock"]:
        run_clock(session, domain, report)
        return
    if sys.argv[1:] == ["io"]:
        Endpoints(session, domain, report).run()
        return

    relay = environment["NERVIX_PROBE_RELAY"]
    subscription = environment["NERVIX_PROBE_SUBSCRIPTION"]
    expected_rows = int(environment["NERVIX_PROBE_ROWS"])

    report(f"OPERATION {session.execute(f'SHOW CREATE RELAY {relay};').disposition()}")
    report(session.execute("CREATE RELAY;").error_line())

    opened = session.execute(f"CREATE SUBSCRIPTION {subscription} TO {relay};")
    name, generation = opened.subscription()
    if name != subscription or generation == 0:
        raise RuntimeError("the subscription opened under another handle")
    fields, key_fields, branch_name = opened.schema()
    for field in fields:
        report(field_line("FIELD", field))
    if branch_name is not None:
        report(f"BRANCH {branch_name}")
    for field in key_fields:
        report(field_line("KEY", field))
    report("SUBSCRIBED")

    deadline = Cancel(120_000)
    retained, reported, seen = [], [], 0
    while seen < expected_rows:
        event = session.next_event(deadline)
        if event.kind() != EVENT_ROWS:
            raise RuntimeError("the subscription reported something other than rows first")
        if event.subscription() != (subscription, generation):
            raise RuntimeError("rows arrived for another subscription")
        lines = event.row_lines(fields, key_fields)
        for line in lines:
            report(line)
        seen += len(lines)
        reported.append(lines)
        # Keep a second reference and let the first go, so the rows must survive on it alone.
        retained.append(event.retain())
        del event

    if output_dir := environment.get("NERVIX_CLIENT_WIRE_BINDING_PROFILE_DIR"):
        profile_binding(retained[0], fields, output_dir)

    check_retention(retained, reported, fields, key_fields)
    check_cancellation(session)
    report("CHECKS ok")
    report(f"CLOSED {session.execute(f'DELETE SUBSCRIPTION {subscription};').disposition()}")
    session.close()
    report("PASS")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:  # noqa: BLE001 - the probe reports every failure the same way
        print(f"probe failed: {error!r}", file=sys.stderr)
        sys.exit(1)
