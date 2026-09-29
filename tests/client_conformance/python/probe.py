"""The CPython probe of the shared Rust binding, through ctypes.

It prints the same conformance report as every other probe. Bulk data stays borrowed: an event's
frame is a memoryview over the binding's buffer that keeps the event alive for as long as the view
exists, and every column is copied into a ctypes array in one call. ctypes releases the GIL for
every call, so a blocked wait never stops another Python thread from cancelling it. Run with the
`clock` argument, it attaches to the domain's clock instead, reads the state and the first tick of
the generation the scenario starts, and detaches.
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
CLOCK_KINDS = {1: "STATE", 2: "TICK", 3: "ENDED", 4: "INTERRUPTED"}
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
        """The committed clock of a paced state. The UTC anchor depends on when the scenario's
        START committed, so it is not read."""
        period, skew = ctypes.c_uint64(), ctypes.c_uint64()
        origin, rate = ctypes.c_int64(), ctypes.c_double()
        check(
            nx_clock_event_paced(
                self.handle, ctypes.byref(period), ctypes.byref(skew), ctypes.byref(origin), None,
                ctypes.byref(rate),
            )
        )
        return {
            "generation": self.generation(),
            "period": period.value,
            "skew": skew.value,
            "origin": origin.value,
            "rate": rate.value,
        }

    def tick(self):
        tick_id, boundary = ctypes.c_uint64(), ctypes.c_int64()
        authority_utc, serving_logical = ctypes.c_int64(), ctypes.c_int64()
        check(
            nx_clock_event_tick(
                self.handle, ctypes.byref(tick_id), ctypes.byref(boundary),
                ctypes.byref(authority_utc), ctypes.byref(serving_logical),
            )
        )
        return {
            "generation": self.generation(),
            "id": tick_id.value,
            "boundary": boundary.value,
            "serving_logical": serving_logical.value,
        }


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


def state_line(clock, domain):
    rate_bits = struct.unpack("<Q", struct.pack("<d", clock["rate"]))[0]
    return (
        f"STATE domain={domain} generation={clock['generation']} state=paced "
        f"period={clock['period']} skew={clock['skew']} origin={clock['origin']} "
        f"rate=f64:{rate_bits:016x}"
    )


def tick_line(event, domain, clock):
    """The report line of a tick, after holding it to the committed clock of its generation: its
    boundary is the logical origin plus one period for every id before it, and the serving node's
    reading never precedes the origin."""
    tick = event.tick()
    if tick["generation"] != clock["generation"]:
        raise RuntimeError("a tick belongs to another generation than the state before it")
    if tick["id"] == 0 or tick["boundary"] != clock["origin"] + (tick["id"] - 1) * clock["period"]:
        raise RuntimeError(
            "a tick's boundary is not the origin plus one period for every id before it"
        )
    if tick["serving_logical"] < clock["origin"]:
        raise RuntimeError("the serving node's logical reading precedes the logical origin")
    return f"TICK domain={domain} generation={tick['generation']} boundary=origin+(id-1)*period"


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


def run_clock(session, domain, report):
    """Attaches to the domain's clock, reads the state and the first tick of the generation the
    scenario starts, and detaches."""
    check_clock_cancellation(session, domain)
    report(f"ATTACHED {session.execute('ATTACH DOMAIN CLOCK;').disposition()}")

    deadline = Cancel(120_000)
    while True:
        state = session.next_clock_event(deadline, domain)
        if state.kind() != "STATE":
            raise RuntimeError(f"the clock reported {state.kind()} before the started state")
        # The serving node may report the started generation uninstalled until it holds the
        # committed mapping and an assigned clock authority.
        if state.state() == CLOCK_PACED:
            break
    clock = state.paced()
    reported_state = state_line(clock, domain)
    report(reported_state)

    while True:
        tick = session.next_clock_event(deadline, domain)
        if tick.kind() == "TICK":
            break
        if tick.kind() != "STATE":
            raise RuntimeError(f"the clock reported {tick.kind()} before its first tick")
        # The serving node reported the installation again; it is still the same generation.
        if tick.generation() != clock["generation"]:
            raise RuntimeError("the clock moved to another generation before its first tick")
    reported_tick = tick_line(tick, domain, clock)
    report(reported_tick)

    # Keep a second reference to each event and release the first on another thread, so the
    # events must read the same on the second alone.
    retained_state, retained_tick = state.retain(), tick.retain()
    firsts = [state, tick]
    del state, tick
    releaser = threading.Thread(target=lambda: (firsts.clear(), gc.collect()))
    releaser.start()
    releaser.join()
    if (
        state_line(retained_state.paced(), domain) != reported_state
        or tick_line(retained_tick, domain, clock) != reported_tick
    ):
        raise RuntimeError("a retained clock event reads differently than it did")

    report(f"DETACHED {session.execute('DETACH DOMAIN CLOCK;').disposition()}")
    report("CHECKS ok")
    session.close()
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
