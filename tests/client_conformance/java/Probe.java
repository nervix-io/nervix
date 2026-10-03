// The Java probe of the shared Rust binding, through the Foreign Function and Memory API.
//
// It prints the same conformance report as every other probe. An event's lifetime is an arena:
// the event handle and every borrowed view of its frame belong to that arena, the binding's
// release function runs when the arena closes, and a view read after that throws instead of
// reading freed memory. Arenas the garbage collector owns release their events when they become
// unreachable. Run it with `java --enable-native-access=ALL-UNNAMED Probe.java`; with the `clock`
// argument it attaches to the domain's running clock instead, reads the clock the attach reported
// before its first tick, follows the generation a STOP and START begin and the attachment restored
// after its session ends, and detaches. With the `io` argument it publishes typed batches through a
// client ingestor and reads, retries, rejects and acknowledges their output through a client
// emitter, across the session the scenario cuts.

import java.lang.foreign.AddressLayout;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class Probe {
    static final Linker LINKER = Linker.nativeLinker();
    static final SymbolLookup LIBRARY =
            SymbolLookup.libraryLookup(Path.of(System.getenv("NERVIX_CLIENT_LIBRARY")), Arena.global());
    static final AddressLayout POINTER = ValueLayout.ADDRESS;
    static final ValueLayout.OfLong SIZE = ValueLayout.JAVA_LONG;
    static final ValueLayout.OfInt INT = ValueLayout.JAVA_INT;
    static final ValueLayout.OfBoolean BOOL = ValueLayout.JAVA_BOOLEAN;

    static final int ERROR_DEADLINE = 6;
    static final int ERROR_CANCELLED = 7;
    static final int PART_ROWS = 1;
    static final int PART_BRANCH_KEY = 2;
    static final int CELL_VALUE = 1;
    static final int CELL_NULL = 2;
    static final int CELL_REDACTED = 3;
    static final int EVENT_ROWS = 1;
    static final String[] CLOCK_KINDS = {
        null, "STATE", "TICK", "ENDED", "INTERRUPTED", "RESTORATION_FAILED",
    };
    static final int CLOCK_PACED = 4;
    static final String[] DISPOSITIONS = {
        null, "completed", "failed", "not_leader", "transaction_detached",
        "transaction_taken_over", "outcome_unknown", "execution_reference_conflict",
        "execution_reference_expired", "preview_stale",
    };
    static final String[] TYPES = {
        null, "U8", "I8", "U16", "I16", "U32", "I32", "U64", "I64", "F32", "F64", "BOOL",
        "STRING", "BYTES", "DATETIME", "FIXED_LIST", "LIST",
    };

    static MethodHandle function(String name, MemoryLayout result, MemoryLayout... arguments) {
        MemorySegment symbol = LIBRARY.find(name).orElseThrow();
        FunctionDescriptor descriptor = result == null
                ? FunctionDescriptor.ofVoid(arguments)
                : FunctionDescriptor.of(result, arguments);
        return LINKER.downcallHandle(symbol, descriptor);
    }

    static final MethodHandle ERROR_KIND = function("nx_error_kind_of", INT, POINTER);
    static final MethodHandle ERROR_MESSAGE = function("nx_error_message", null, POINTER, POINTER, POINTER);
    static final MethodHandle ERROR_REFERENCE =
            function("nx_error_execution_reference", BOOL, POINTER, POINTER, POINTER);
    static final MethodHandle ERROR_FREE = function("nx_error_free", null, POINTER);
    static final MethodHandle CANCEL_NEW = function("nx_cancel_new", POINTER);
    static final MethodHandle CANCEL_WITH_DEADLINE =
            function("nx_cancel_with_deadline", POINTER, ValueLayout.JAVA_LONG, POINTER);
    static final MethodHandle CANCEL_TRIGGER = function("nx_cancel_trigger", null, POINTER);
    static final MethodHandle CANCEL_FREE = function("nx_cancel_free", null, POINTER);
    static final MethodHandle SESSION_CONNECT = function("nx_session_connect", POINTER,
            POINTER, SIZE, POINTER, SIZE, POINTER, SIZE, POINTER, SIZE, POINTER, POINTER);
    static final MethodHandle SESSION_FREE = function("nx_session_free", null, POINTER);
    static final MethodHandle SESSION_PREPARE =
            function("nx_session_prepare", POINTER, POINTER, POINTER, SIZE, POINTER, POINTER);
    static final MethodHandle EXECUTION_FREE = function("nx_execution_free", null, POINTER);
    static final MethodHandle SESSION_EXECUTE =
            function("nx_session_execute", POINTER, POINTER, POINTER, POINTER, POINTER);
    static final MethodHandle SESSION_NEXT_EVENT =
            function("nx_session_next_event", POINTER, POINTER, POINTER, POINTER);
    static final MethodHandle OUTCOME_DISPOSITION = function("nx_outcome_disposition", INT, POINTER);
    static final MethodHandle OUTCOME_DIAGNOSTIC_COUNT =
            function("nx_outcome_diagnostic_count", SIZE, POINTER);
    static final MethodHandle OUTCOME_DIAGNOSTIC = function("nx_outcome_diagnostic", POINTER,
            POINTER, SIZE, POINTER, POINTER, POINTER, POINTER, POINTER);
    static final MethodHandle OUTCOME_SUBSCRIPTION =
            function("nx_outcome_subscription", BOOL, POINTER, POINTER, POINTER, POINTER);
    static final MethodHandle OUTCOME_SCHEMA = function("nx_outcome_schema", POINTER, POINTER, POINTER);
    static final MethodHandle OUTCOME_FREE = function("nx_outcome_free", null, POINTER);
    static final MethodHandle SCHEMA_FIELD_COUNT =
            function("nx_schema_field_count", POINTER, POINTER, INT, POINTER);
    static final MethodHandle SCHEMA_FIELD = function("nx_schema_field", POINTER,
            POINTER, INT, SIZE, POINTER, POINTER, POINTER, POINTER, POINTER);
    static final MethodHandle SCHEMA_BRANCH = function("nx_schema_branch", BOOL, POINTER, POINTER, POINTER);
    static final MethodHandle SCHEMA_FREE = function("nx_schema_free", null, POINTER);
    static final MethodHandle EVENT_KIND = function("nx_event_kind_of", INT, POINTER);
    static final MethodHandle EVENT_SUBSCRIPTION =
            function("nx_event_subscription", null, POINTER, POINTER, POINTER, POINTER);
    static final MethodHandle EVENT_ROW_COUNT = function("nx_event_row_count", ValueLayout.JAVA_LONG, POINTER);
    static final MethodHandle EVENT_RETAIN = function("nx_event_retain", POINTER, POINTER);
    static final MethodHandle EVENT_RELEASE = function("nx_event_release", null, POINTER);
    static final MethodHandle EVENT_FRAME = function("nx_event_frame", POINTER, POINTER, POINTER, POINTER);
    static final MethodHandle EVENT_COLUMN_STATES =
            function("nx_event_column_states", POINTER, POINTER, INT, SIZE, POINTER, SIZE);
    static final MethodHandle EVENT_COLUMN_FIXED =
            function("nx_event_column_fixed", POINTER, POINTER, INT, SIZE, POINTER, SIZE);
    static final MethodHandle EVENT_COLUMN_VARLEN = function("nx_event_column_varlen", POINTER,
            POINTER, INT, SIZE, POINTER, SIZE, POINTER, SIZE, POINTER);
    static final MethodHandle EVENT_CELL_VARLEN =
            function("nx_event_cell_varlen", POINTER, POINTER, INT, SIZE, SIZE, POINTER, POINTER);
    static final MethodHandle SESSION_NEXT_CLOCK_EVENT =
            function("nx_session_next_clock_event", POINTER, POINTER, POINTER, POINTER);
    static final MethodHandle CLOCK_EVENT_KIND = function("nx_clock_event_kind_of", INT, POINTER);
    static final MethodHandle CLOCK_EVENT_DOMAIN =
            function("nx_clock_event_domain", null, POINTER, POINTER, POINTER);
    static final MethodHandle CLOCK_EVENT_GENERATION =
            function("nx_clock_event_generation", POINTER, POINTER, POINTER);
    static final MethodHandle CLOCK_EVENT_STATE = function("nx_clock_event_state", POINTER, POINTER, POINTER);
    static final MethodHandle CLOCK_EVENT_PACED = function("nx_clock_event_paced", POINTER,
            POINTER, POINTER, POINTER, POINTER, POINTER, POINTER);
    static final MethodHandle CLOCK_EVENT_TICK =
            function("nx_clock_event_tick", POINTER, POINTER, POINTER, POINTER, POINTER, POINTER);
    static final MethodHandle CLOCK_EVENT_RETAIN = function("nx_clock_event_retain", POINTER, POINTER);
    static final MethodHandle CLOCK_EVENT_RELEASE = function("nx_clock_event_release", null, POINTER);
    static final MethodHandle SESSION_DOMAIN_CLOCK =
            function("nx_session_domain_clock", POINTER, POINTER, POINTER, SIZE, POINTER);
    static final MethodHandle DOMAIN_CLOCK_GENERATION =
            function("nx_domain_clock_generation", ValueLayout.JAVA_LONG, POINTER);
    static final MethodHandle DOMAIN_CLOCK_STATE = function("nx_domain_clock_state", INT, POINTER);
    static final MethodHandle DOMAIN_CLOCK_PACED = function("nx_domain_clock_paced", POINTER,
            POINTER, POINTER, POINTER, POINTER, POINTER, POINTER);
    static final MethodHandle DOMAIN_CLOCK_TICK =
            function("nx_domain_clock_tick", BOOL, POINTER, POINTER, POINTER, POINTER, POINTER);
    static final MethodHandle DOMAIN_CLOCK_LOGICAL_TIME_AT = function("nx_domain_clock_logical_time_at",
            POINTER, POINTER, ValueLayout.JAVA_LONG, POINTER);
    static final MethodHandle DOMAIN_CLOCK_WALL_DURATION_UNTIL = function(
            "nx_domain_clock_wall_duration_until", POINTER, POINTER, ValueLayout.JAVA_LONG,
            ValueLayout.JAVA_LONG, POINTER);
    static final MethodHandle DOMAIN_CLOCK_ADMISSION_WINDOW = function("nx_domain_clock_admission_window",
            POINTER, POINTER, ValueLayout.JAVA_LONG, POINTER, POINTER, POINTER);
    static final MethodHandle DOMAIN_CLOCK_ADMITS = function("nx_domain_clock_admits", POINTER, POINTER,
            ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, POINTER);
    static final MethodHandle DOMAIN_CLOCK_RETAIN = function("nx_domain_clock_retain", POINTER, POINTER);
    static final MethodHandle DOMAIN_CLOCK_RELEASE = function("nx_domain_clock_release", null, POINTER);

    /** A failure the binding returned, with its kind and the execution reference it names. */
    static final class Failure extends RuntimeException {
        final int kind;
        final String reference;

        Failure(int kind, String message, String reference) {
            super("kind " + kind + ": " + message);
            this.kind = kind;
            this.reference = reference;
        }
    }

    static Object call(MethodHandle function, Object... arguments) {
        try {
            return function.invokeWithArguments(arguments);
        } catch (RuntimeException | Error error) {
            throw error;
        } catch (Throwable error) {
            throw new IllegalStateException(error);
        }
    }

    /** Copies bytes the binding lent for the duration of this call. */
    static byte[] borrowed(MemorySegment pointer, MemorySegment length) {
        long len = length.get(SIZE, 0);
        MemorySegment data = pointer.get(POINTER, 0);
        if (len == 0) {
            return new byte[0];
        }
        return data.reinterpret(len).toArray(ValueLayout.JAVA_BYTE);
    }

    static String utf8(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /** Throws the error a call returned, releasing it. */
    static void check(Object result) {
        MemorySegment error = (MemorySegment) result;
        if (error.equals(MemorySegment.NULL)) {
            return;
        }
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment message = arena.allocate(POINTER);
            MemorySegment messageLen = arena.allocate(SIZE);
            call(ERROR_MESSAGE, error, message, messageLen);
            MemorySegment reference = arena.allocate(POINTER);
            MemorySegment referenceLen = arena.allocate(SIZE);
            String named = null;
            if ((boolean) call(ERROR_REFERENCE, error, reference, referenceLen)) {
                named = utf8(borrowed(reference, referenceLen));
            }
            Failure failure = new Failure((int) call(ERROR_KIND, error),
                    utf8(borrowed(message, messageLen)), named);
            call(ERROR_FREE, error);
            throw failure;
        }
    }

    static MemorySegment text(Arena arena, String value) {
        return arena.allocateFrom(ValueLayout.JAVA_BYTE, value.getBytes(StandardCharsets.UTF_8));
    }

    static long length(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }

    /** A cancellation token, freed when its arena closes. */
    static MemorySegment cancel(Arena arena, Long deadlineMillis) {
        MemorySegment handle;
        if (deadlineMillis == null) {
            handle = (MemorySegment) call(CANCEL_NEW);
        } else {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment out = scratch.allocate(POINTER);
                check(call(CANCEL_WITH_DEADLINE, deadlineMillis, out));
                handle = out.get(POINTER, 0);
            }
        }
        return handle.reinterpret(arena, segment -> call(CANCEL_FREE, segment));
    }

    record Field(String name, String type, boolean nullable, boolean sensitive) {
        String line(String prefix) {
            return prefix + " " + name + " " + type + " " + (nullable ? "nullable" : "required") + " "
                    + (sensitive ? "sensitive" : "public");
        }
    }

    record Schema(List<Field> fields, List<Field> keyFields, String branch) {}

    static List<Field> fields(MemorySegment schema, int part) {
        List<Field> fields = new ArrayList<>();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment count = arena.allocate(SIZE);
            check(call(SCHEMA_FIELD_COUNT, schema, part, count));
            for (long index = 0; index < count.get(SIZE, 0); index++) {
                MemorySegment name = arena.allocate(POINTER);
                MemorySegment nameLen = arena.allocate(SIZE);
                MemorySegment type = arena.allocate(INT);
                MemorySegment nullable = arena.allocate(BOOL);
                MemorySegment sensitive = arena.allocate(BOOL);
                check(call(SCHEMA_FIELD, schema, part, index, name, nameLen, type, nullable, sensitive));
                fields.add(new Field(utf8(borrowed(name, nameLen)), TYPES[type.get(INT, 0)],
                        nullable.get(BOOL, 0), sensitive.get(BOOL, 0)));
            }
        }
        return fields;
    }

    /** One reference to an event, owned by an arena: closing the arena releases the reference. */
    static final class Event {
        final MemorySegment handle;
        final Arena arena;

        Event(MemorySegment raw, Arena arena) {
            this.arena = arena;
            this.handle = raw.reinterpret(arena, segment -> call(EVENT_RELEASE, segment));
        }

        Event retain(Arena into) {
            return new Event((MemorySegment) call(EVENT_RETAIN, handle), into);
        }

        int kind() {
            return (int) call(EVENT_KIND, handle);
        }

        long rowCount() {
            return (long) call(EVENT_ROW_COUNT, handle);
        }

        String subscription() {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment name = scratch.allocate(POINTER);
                MemorySegment nameLen = scratch.allocate(SIZE);
                MemorySegment generation = scratch.allocate(ValueLayout.JAVA_LONG);
                call(EVENT_SUBSCRIPTION, handle, name, nameLen, generation);
                return utf8(borrowed(name, nameLen)) + "#" + generation.get(ValueLayout.JAVA_LONG, 0);
            }
        }

        /** The frame, borrowed without a copy for exactly as long as this event's arena lives. */
        ByteBuffer frame() {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment frame = scratch.allocate(POINTER);
                MemorySegment frameLen = scratch.allocate(SIZE);
                check(call(EVENT_FRAME, handle, frame, frameLen));
                return frame.get(POINTER, 0).reinterpret(frameLen.get(SIZE, 0), arena, null)
                        .asByteBuffer().asReadOnlyBuffer();
            }
        }

        List<String> column(int part, int index, Field field, int cells) {
            List<String> rendered = new ArrayList<>();
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment states = scratch.allocate(cells);
                check(call(EVENT_COLUMN_STATES, handle, part, (long) index, states, (long) cells));
                int width = switch (field.type()) {
                    case "U8", "I8", "BOOL" -> 1;
                    case "U16", "I16" -> 2;
                    case "U32", "I32", "F32" -> 4;
                    case "U64", "I64", "F64", "DATETIME" -> 8;
                    default -> 0;
                };
                MemorySegment values = null;
                MemorySegment offsets = null;
                MemorySegment data = null;
                if (width > 0) {
                    values = scratch.allocate((long) cells * width, 8);
                    check(call(EVENT_COLUMN_FIXED, handle, part, (long) index, values, values.byteSize()));
                } else if (field.type().equals("STRING") || field.type().equals("BYTES")) {
                    MemorySegment dataLen = scratch.allocate(SIZE);
                    check(call(EVENT_COLUMN_VARLEN, handle, part, (long) index, MemorySegment.NULL, 0L,
                            MemorySegment.NULL, 0L, dataLen));
                    offsets = scratch.allocate(ValueLayout.JAVA_LONG, cells + 1);
                    data = scratch.allocate(Math.max(dataLen.get(SIZE, 0), 1));
                    check(call(EVENT_COLUMN_VARLEN, handle, part, (long) index, offsets, (long) cells + 1,
                            data, data.byteSize(), dataLen));
                } else {
                    throw new IllegalStateException("list columns are read from the frame");
                }
                for (int row = 0; row < cells; row++) {
                    int state = Byte.toUnsignedInt(states.get(ValueLayout.JAVA_BYTE, row));
                    if (state == CELL_NULL) {
                        rendered.add("null");
                        continue;
                    }
                    if (state == CELL_REDACTED) {
                        rendered.add("redacted");
                        continue;
                    }
                    if (state != CELL_VALUE) {
                        throw new IllegalStateException("unknown cell state " + state);
                    }
                    rendered.add(switch (field.type()) {
                        case "U8" -> "u8:" + Byte.toUnsignedInt(values.getAtIndex(ValueLayout.JAVA_BYTE, row));
                        case "I8" -> "i8:" + values.getAtIndex(ValueLayout.JAVA_BYTE, row);
                        case "U16" -> "u16:" + Short.toUnsignedInt(values.getAtIndex(ValueLayout.JAVA_SHORT, row));
                        case "I16" -> "i16:" + values.getAtIndex(ValueLayout.JAVA_SHORT, row);
                        case "U32" -> "u32:" + Integer.toUnsignedString(values.getAtIndex(INT, row));
                        case "I32" -> "i32:" + values.getAtIndex(INT, row);
                        case "U64" -> "u64:" + Long.toUnsignedString(values.getAtIndex(ValueLayout.JAVA_LONG, row));
                        case "I64" -> "i64:" + values.getAtIndex(ValueLayout.JAVA_LONG, row);
                        case "F32" -> String.format("f32:%08x", values.getAtIndex(INT, row));
                        case "F64" -> String.format("f64:%016x", values.getAtIndex(ValueLayout.JAVA_LONG, row));
                        case "BOOL" -> "bool:" + (values.getAtIndex(ValueLayout.JAVA_BYTE, row) != 0);
                        case "DATETIME" -> "datetime:" + values.getAtIndex(ValueLayout.JAVA_LONG, row);
                        default -> varlen(part, index, field, data, offsets, row);
                    });
                }
            }
            return rendered;
        }

        String varlen(int part, int index, Field field, MemorySegment data, MemorySegment offsets, int row) {
            long start = offsets.getAtIndex(ValueLayout.JAVA_LONG, row);
            long end = offsets.getAtIndex(ValueLayout.JAVA_LONG, row + 1);
            byte[] copied = data.asSlice(start, end - start).toArray(ValueLayout.JAVA_BYTE);
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment value = scratch.allocate(POINTER);
                MemorySegment valueLen = scratch.allocate(SIZE);
                check(call(EVENT_CELL_VARLEN, handle, part, (long) row, (long) index, value, valueLen));
                if (!Arrays.equals(borrowed(value, valueLen), copied)) {
                    throw new IllegalStateException(
                            "a copied value differs from the same value borrowed from the frame");
                }
            }
            return (field.type().equals("STRING") ? "str:" : "bytes:") + HexFormat.of().formatHex(copied);
        }

        List<String> render(int part, List<Field> fields, int cells) {
            List<StringBuilder> rows = new ArrayList<>();
            for (int row = 0; row < cells; row++) {
                rows.add(new StringBuilder());
            }
            for (int index = 0; index < fields.size(); index++) {
                Field field = fields.get(index);
                List<String> column = column(part, index, field, cells);
                for (int row = 0; row < cells; row++) {
                    StringBuilder line = rows.get(row);
                    if (line.length() > 0) {
                        line.append(' ');
                    }
                    line.append(field.name()).append('=').append(column.get(row));
                }
            }
            return rows.stream().map(StringBuilder::toString).toList();
        }

        List<String> rowLines(Schema schema) {
            String key = schema.keyFields().isEmpty() ? "" : render(PART_BRANCH_KEY, schema.keyFields(), 1).get(0);
            return render(PART_ROWS, schema.fields(), Math.toIntExact(rowCount())).stream()
                    .map(row -> "ROW [" + key + "] " + row)
                    .toList();
        }
    }

    /** The committed clock of a paced generation. */
    record PacedClock(long generation, long period, long skew, long origin, long anchor, double rate) {
        /**
         * The report line of the clock, with `prefix` naming where it was read. The UTC anchor
         * depends on when the scenario's START committed, so it is read but not reported.
         */
        String line(String prefix, String domain) {
            return prefix + " domain=" + domain + " generation=" + Long.toUnsignedString(generation)
                    + " state=paced period=" + Long.toUnsignedString(period) + " skew="
                    + Long.toUnsignedString(skew) + " origin=" + origin + " rate=f64:"
                    + String.format("%016x", Double.doubleToRawLongBits(rate));
        }

        /** An instant as the report names it: `origin` for the logical origin, the instant otherwise. */
        String relative(long instant) {
            return instant == origin ? "origin" : Long.toString(instant);
        }

        /**
         * The report line of the projections of `read`, which holds this clock, at its own UTC
         * anchor: the logical time there, the wait for the next tick center, the admission window,
         * and whether an event at the skew's edge and one nanosecond past it are admitted.
         */
        String projectionLine(DomainClock read, String domain) {
            long nextCenter = Math.addExact(origin, period);
            long edge = Math.addExact(origin, skew);
            long beyond = Math.addExact(edge, 1);
            long[] window = read.admissionWindow(anchor);
            if (window == null) {
                throw new IllegalStateException("a paced clock reports no admission window");
            }
            return "PROJECTION domain=" + domain + " generation=" + Long.toUnsignedString(generation)
                    + " anchor=" + relative(read.logicalTimeAt(anchor))
                    + " wait=" + Long.toUnsignedString(read.wallDurationUntil(anchor, nextCenter))
                    + " window=" + relative(window[0]) + ".." + relative(window[1])
                    + " skew=" + admission(read.admits(anchor, edge))
                    + " beyond=" + admission(read.admits(anchor, beyond));
        }

        static String admission(boolean admitted) {
            return admitted ? "admitted" : "refused";
        }

        /**
         * Holds a tick to this clock: the same generation, a boundary of the logical origin plus
         * one period for every id before it, and a serving node's reading that never precedes the
         * origin.
         */
        void checkTick(Tick tick) {
            if (tick.generation() != generation) {
                throw new IllegalStateException("a tick of one generation followed the state of another");
            }
            if (tick.id() == 0 || tick.boundary() != Math.addExact(origin, Math.multiplyExact(tick.id() - 1, period))) {
                throw new IllegalStateException(
                        "a tick's boundary is not the origin plus one period for every id before it");
            }
            if (tick.servingLogical() < origin) {
                throw new IllegalStateException("the serving node's logical reading precedes the logical origin");
            }
        }

        /** The report line of a tick this clock holds. */
        String tickLine(Tick tick, String domain) {
            checkTick(tick);
            return "TICK domain=" + domain + " generation=" + Long.toUnsignedString(tick.generation())
                    + " boundary=origin+(id-1)*period";
        }
    }

    /** The progress a tick event reports. */
    record Tick(long generation, long id, long boundary, long servingLogical) {}

    /** One reference to a domain clock event, owned by an arena: closing it releases the reference. */
    static final class ClockEvent {
        final MemorySegment handle;
        final Arena arena;

        ClockEvent(MemorySegment raw, Arena arena) {
            this.arena = arena;
            this.handle = raw.reinterpret(arena, segment -> call(CLOCK_EVENT_RELEASE, segment));
        }

        ClockEvent retain(Arena into) {
            return new ClockEvent((MemorySegment) call(CLOCK_EVENT_RETAIN, handle), into);
        }

        String kind() {
            return CLOCK_KINDS[(int) call(CLOCK_EVENT_KIND, handle)];
        }

        String domain() {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment name = scratch.allocate(POINTER);
                MemorySegment nameLen = scratch.allocate(SIZE);
                call(CLOCK_EVENT_DOMAIN, handle, name, nameLen);
                return utf8(borrowed(name, nameLen));
            }
        }

        long generation() {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment generation = scratch.allocate(ValueLayout.JAVA_LONG);
                check(call(CLOCK_EVENT_GENERATION, handle, generation));
                return generation.get(ValueLayout.JAVA_LONG, 0);
            }
        }

        int state() {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment state = scratch.allocate(INT);
                check(call(CLOCK_EVENT_STATE, handle, state));
                return state.get(INT, 0);
            }
        }

        PacedClock paced() {
            return pacedClock(CLOCK_EVENT_PACED, handle, generation());
        }

        /** The tick; the authority's UTC observation depends on when it was accepted and is not read. */
        Tick tick() {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment id = scratch.allocate(ValueLayout.JAVA_LONG);
                MemorySegment boundary = scratch.allocate(ValueLayout.JAVA_LONG);
                MemorySegment servingLogical = scratch.allocate(ValueLayout.JAVA_LONG);
                check(call(CLOCK_EVENT_TICK, handle, id, boundary, MemorySegment.NULL, servingLogical));
                return new Tick(generation(), id.get(ValueLayout.JAVA_LONG, 0),
                        boundary.get(ValueLayout.JAVA_LONG, 0), servingLogical.get(ValueLayout.JAVA_LONG, 0));
            }
        }
    }

    /** The committed clock `read` writes for `handle`, a clock event or a domain clock. */
    static PacedClock pacedClock(MethodHandle read, MemorySegment handle, long generation) {
        try (Arena scratch = Arena.ofConfined()) {
            MemorySegment period = scratch.allocate(ValueLayout.JAVA_LONG);
            MemorySegment skew = scratch.allocate(ValueLayout.JAVA_LONG);
            MemorySegment origin = scratch.allocate(ValueLayout.JAVA_LONG);
            MemorySegment anchor = scratch.allocate(ValueLayout.JAVA_LONG);
            MemorySegment rate = scratch.allocate(ValueLayout.JAVA_DOUBLE);
            check(call(read, handle, period, skew, origin, anchor, rate));
            return new PacedClock(generation, period.get(ValueLayout.JAVA_LONG, 0),
                    skew.get(ValueLayout.JAVA_LONG, 0), origin.get(ValueLayout.JAVA_LONG, 0),
                    anchor.get(ValueLayout.JAVA_LONG, 0), rate.get(ValueLayout.JAVA_DOUBLE, 0));
        }
    }

    /**
     * One reference to the clock the session held for a followed domain when the probe read it,
     * owned by an arena: closing it releases the reference.
     */
    static final class DomainClock {
        final MemorySegment handle;
        final Arena arena;

        DomainClock(MemorySegment raw, Arena arena) {
            this.arena = arena;
            this.handle = raw.reinterpret(arena, segment -> call(DOMAIN_CLOCK_RELEASE, segment));
        }

        DomainClock retain(Arena into) {
            return new DomainClock((MemorySegment) call(DOMAIN_CLOCK_RETAIN, handle), into);
        }

        long generation() {
            return (long) call(DOMAIN_CLOCK_GENERATION, handle);
        }

        int state() {
            return (int) call(DOMAIN_CLOCK_STATE, handle);
        }

        PacedClock paced() {
            return pacedClock(DOMAIN_CLOCK_PACED, handle, generation());
        }

        /** The id of the newest tick the read holds, when it holds one. */
        OptionalLong tickId() {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment id = scratch.allocate(ValueLayout.JAVA_LONG);
                boolean held = (boolean) call(DOMAIN_CLOCK_TICK, handle, id, MemorySegment.NULL,
                        MemorySegment.NULL, MemorySegment.NULL);
                return held ? OptionalLong.of(id.get(ValueLayout.JAVA_LONG, 0)) : OptionalLong.empty();
            }
        }

        long logicalTimeAt(long utc) {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment logical = scratch.allocate(ValueLayout.JAVA_LONG);
                check(call(DOMAIN_CLOCK_LOGICAL_TIME_AT, handle, utc, logical));
                return logical.get(ValueLayout.JAVA_LONG, 0);
            }
        }

        long wallDurationUntil(long utc, long target) {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment wait = scratch.allocate(ValueLayout.JAVA_LONG);
                check(call(DOMAIN_CLOCK_WALL_DURATION_UNTIL, handle, utc, target, wait));
                return wait.get(ValueLayout.JAVA_LONG, 0);
            }
        }

        /** The earliest and latest admitted tick centers, or null for a clock without a window. */
        long[] admissionWindow(long utc) {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment hasWindow = scratch.allocate(BOOL);
                MemorySegment earliest = scratch.allocate(ValueLayout.JAVA_LONG);
                MemorySegment latest = scratch.allocate(ValueLayout.JAVA_LONG);
                check(call(DOMAIN_CLOCK_ADMISSION_WINDOW, handle, utc, hasWindow, earliest, latest));
                if (!hasWindow.get(BOOL, 0)) {
                    return null;
                }
                return new long[] {earliest.get(ValueLayout.JAVA_LONG, 0), latest.get(ValueLayout.JAVA_LONG, 0)};
            }
        }

        boolean admits(long utc, long event) {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment admitted = scratch.allocate(BOOL);
                check(call(DOMAIN_CLOCK_ADMITS, handle, utc, event, admitted));
                return admitted.get(BOOL, 0);
            }
        }
    }

    record Outcome(MemorySegment handle) {
        String disposition() {
            return DISPOSITIONS[(int) call(OUTCOME_DISPOSITION, handle)];
        }
    }

    static final class Session implements AutoCloseable {
        final Arena arena = Arena.ofShared();
        final MemorySegment handle;

        Session(String server, String domain, String username, String password) {
            MemorySegment out = arena.allocate(POINTER);
            check(call(SESSION_CONNECT, text(arena, server), length(server), text(arena, domain),
                    length(domain), text(arena, username), length(username), text(arena, password),
                    length(password), MemorySegment.NULL, out));
            handle = out.get(POINTER, 0);
        }

        /** Runs one command; the outcome belongs to `into` and is freed when it closes. */
        Outcome execute(String query, MemorySegment cancel, Arena into) {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment execution = scratch.allocate(POINTER);
                check(call(SESSION_PREPARE, handle, text(scratch, query), length(query),
                        MemorySegment.NULL, execution));
                MemorySegment prepared = execution.get(POINTER, 0);
                try {
                    MemorySegment outcome = scratch.allocate(POINTER);
                    check(call(SESSION_EXECUTE, handle, prepared, cancel, outcome));
                    return new Outcome(outcome.get(POINTER, 0)
                            .reinterpret(into, segment -> call(OUTCOME_FREE, segment)));
                } finally {
                    call(EXECUTION_FREE, prepared);
                }
            }
        }

        Event nextEvent(MemorySegment cancel, Arena into) {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment event = scratch.allocate(POINTER);
                check(call(SESSION_NEXT_EVENT, handle, cancel, event));
                return new Event(event.get(POINTER, 0), into);
            }
        }

        /** The clock the session holds for `domain`, owned by `into`, or null when it follows none. */
        DomainClock domainClock(String domain, Arena into) {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment out = scratch.allocate(POINTER);
                check(call(SESSION_DOMAIN_CLOCK, handle, text(scratch, domain), length(domain), out));
                MemorySegment clock = out.get(POINTER, 0);
                if (clock.equals(MemorySegment.NULL)) {
                    return null;
                }
                return new DomainClock(clock, into);
            }
        }

        ClockEvent nextClockEvent(MemorySegment cancel, Arena into, String domain) {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment event = scratch.allocate(POINTER);
                check(call(SESSION_NEXT_CLOCK_EVENT, handle, cancel, event));
                ClockEvent clockEvent = new ClockEvent(event.get(POINTER, 0), into);
                if (!clockEvent.domain().equals(domain)) {
                    throw new IllegalStateException("a clock event arrived for another domain");
                }
                return clockEvent;
            }
        }

        @Override
        public void close() {
            call(SESSION_FREE, handle);
            arena.close();
        }
    }

    static Failure failure(Runnable action) {
        try {
            action.run();
        } catch (Failure failure) {
            return failure;
        }
        throw new IllegalStateException("a call succeeded where it had to fail");
    }

    static void checkRetention(List<Event> retained, List<List<String>> reported, Schema schema)
            throws InterruptedException {
        // Explicit lifetimes: a borrowed view is readable while its arena lives and throws after.
        List<ByteBuffer> views = new ArrayList<>();
        for (int index = 0; index < retained.size(); index++) {
            Event event = retained.get(index);
            ByteBuffer view = event.frame();
            views.add(view);
            if (view.get(4) != 'N' || view.get(7) != 'M') {
                throw new IllegalStateException("a retained frame is not a ServerMessage frame");
            }
            if (!event.rowLines(schema).equals(reported.get(index))) {
                throw new IllegalStateException("a retained event reads differently than it did");
            }
        }
        Thread closer = new Thread(() -> retained.forEach(event -> event.arena.close()));
        closer.start();
        closer.join();
        for (ByteBuffer view : views) {
            try {
                view.get(0);
                throw new IllegalStateException("a view of a released event was still readable");
            } catch (IllegalStateException released) {
                if (!released.getMessage().contains("closed")) {
                    throw released;
                }
            }
        }
    }

    /**
     * Garbage-collected lifetimes: a reference retained into an automatic arena is released by the
     * collector once nothing reaches it, on the collector's thread, while other references to the
     * same event stay readable on this one.
     */
    static void checkCollectedRelease(List<Event> retained, List<List<String>> reported, Schema schema) {
        List<WeakReference<Event>> collected = new ArrayList<>();
        for (int index = 0; index < retained.size(); index++) {
            Event automatic = retained.get(index).retain(Arena.ofAuto());
            if (!automatic.rowLines(schema).equals(reported.get(index))) {
                throw new IllegalStateException("an automatically retained event reads differently");
            }
            collected.add(new WeakReference<>(automatic));
        }
        for (int attempt = 0; attempt < 100 && collected.stream().anyMatch(ref -> ref.get() != null); attempt++) {
            System.gc();
            byte[][] churn = new byte[256][];
            for (int index = 0; index < churn.length; index++) {
                churn[index] = new byte[64 * 1024];
            }
        }
        if (collected.stream().anyMatch(ref -> ref.get() != null)) {
            throw new IllegalStateException("an unreachable automatically retained event was never collected");
        }
        for (int index = 0; index < retained.size(); index++) {
            if (!retained.get(index).rowLines(schema).equals(reported.get(index))) {
                throw new IllegalStateException("collecting one reference disturbed another");
            }
        }
    }

    static void checkCancellation(Session session) throws InterruptedException {
        try (Arena arena = Arena.ofShared()) {
            MemorySegment cancel = cancel(arena, null);
            AtomicReference<Failure> waited = new AtomicReference<>();
            Thread waiter = new Thread(() -> waited.set(failure(() -> {
                try (Arena events = Arena.ofConfined()) {
                    session.nextEvent(cancel, events);
                }
            })));
            waiter.start();
            Thread.sleep(100);
            call(CANCEL_TRIGGER, cancel);
            waiter.join(30_000);
            if (waiter.isAlive() || waited.get().kind != ERROR_CANCELLED) {
                throw new IllegalStateException("a cancelled wait did not report CANCELLED");
            }
            MemorySegment deadline = cancel(arena, 50L);
            Failure expired = failure(() -> {
                try (Arena events = Arena.ofConfined()) {
                    session.nextEvent(deadline, events);
                }
            });
            if (expired.kind != ERROR_DEADLINE) {
                throw new IllegalStateException("an expired wait did not report DEADLINE");
            }
            MemorySegment cancelled = cancel(arena, null);
            call(CANCEL_TRIGGER, cancelled);
            Failure command = failure(() -> {
                try (Arena outcomes = Arena.ofConfined()) {
                    session.execute("SHOW DOMAINS;", cancelled, outcomes);
                }
            });
            if (command.kind != ERROR_CANCELLED || command.reference == null || command.reference.isEmpty()) {
                throw new IllegalStateException(
                        "a cancelled command did not report CANCELLED with its execution reference");
            }
        }
    }

    static void report(String line) {
        System.out.println(line);
        System.out.flush();
    }

    /**
     * Cancels a clock wait from another thread, then lets a deadline end another. The session
     * follows no clock yet, so nothing but its token ends either wait.
     */
    static void checkClockCancellation(Session session, String domain) throws InterruptedException {
        try (Arena arena = Arena.ofShared()) {
            MemorySegment cancel = cancel(arena, null);
            AtomicReference<Failure> waited = new AtomicReference<>();
            Thread waiter = new Thread(() -> waited.set(failure(() -> {
                try (Arena events = Arena.ofConfined()) {
                    session.nextClockEvent(cancel, events, domain);
                }
            })));
            waiter.start();
            Thread.sleep(100);
            call(CANCEL_TRIGGER, cancel);
            waiter.join(30_000);
            if (waiter.isAlive() || waited.get().kind != ERROR_CANCELLED) {
                throw new IllegalStateException("a cancelled clock wait did not report CANCELLED");
            }
            MemorySegment deadline = cancel(arena, 50L);
            Failure expired = failure(() -> {
                try (Arena events = Arena.ofConfined()) {
                    session.nextClockEvent(deadline, events, domain);
                }
            });
            if (expired.kind != ERROR_DEADLINE) {
                throw new IllegalStateException("an expired clock wait did not report DEADLINE");
            }
        }
    }

    /**
     * What the probe has read about the domain's clock: the generation of the newest state, its
     * mapping while it is paced, and whether the session holding the attachment ended since. Every
     * event is held to what was read before it, and a read of the clock taken right after it is
     * held to be no older.
     */
    static final class FollowedClock {
        final Session session;
        final String domain;
        long generation;
        PacedClock paced;
        boolean interrupted;

        FollowedClock(Session session, String domain, PacedClock clock) {
            this.session = session;
            this.domain = domain;
            this.generation = clock.generation();
            this.paced = clock;
        }

        PacedClock clock() {
            if (paced == null) {
                throw new IllegalStateException("the followed clock is not paced");
            }
            return paced;
        }

        /** The next event about the domain, held to what the probe read before it. */
        ClockEvent next(MemorySegment deadline) {
            ClockEvent event = session.nextClockEvent(deadline, Arena.ofShared(), domain);
            switch (event.kind()) {
                case "STATE" -> observe(event);
                case "TICK" -> checkTick(event);
                case "INTERRUPTED" -> interrupted = true;
                case "ENDED" -> throw new IllegalStateException("the server ended the attachment");
                default -> {}
            }
            return event;
        }

        void observe(ClockEvent event) {
            long eventGeneration = event.generation();
            if (Long.compareUnsigned(eventGeneration, generation) < 0) {
                throw new IllegalStateException("a state went back to an earlier generation");
            }
            int state = event.state();
            PacedClock eventPaced = state == CLOCK_PACED ? event.paced() : null;
            try (Arena reads = Arena.ofConfined()) {
                DomainClock read = read(reads);
                if (Long.compareUnsigned(read.generation(), eventGeneration) < 0) {
                    throw new IllegalStateException("a read of the clock is older than the state the probe took");
                }
                if (read.generation() == eventGeneration) {
                    if (read.state() != state) {
                        throw new IllegalStateException("a read of the clock differs from the state of its generation");
                    }
                    if (eventPaced != null && !read.paced().equals(eventPaced)) {
                        throw new IllegalStateException("a read of the clock differs from the mapping of its generation");
                    }
                }
            }
            generation = eventGeneration;
            paced = eventPaced;
            interrupted = false;
        }

        void checkTick(ClockEvent event) {
            if (interrupted) {
                throw new IllegalStateException("a tick arrived before the restored attachment reported its clock");
            }
            Tick tick = event.tick();
            clock().checkTick(tick);
            try (Arena reads = Arena.ofConfined()) {
                DomainClock read = read(reads);
                if (Long.compareUnsigned(read.generation(), tick.generation()) < 0) {
                    throw new IllegalStateException("a read of the clock is older than the tick the probe took");
                }
                OptionalLong held = read.tickId();
                if (read.generation() == tick.generation() && held.isPresent()
                        && Long.compareUnsigned(held.getAsLong(), tick.id()) < 0) {
                    throw new IllegalStateException("a read of the clock holds an older tick than the probe took");
                }
            }
        }

        DomainClock read(Arena into) {
            DomainClock read = session.domainClock(domain, into);
            if (read == null) {
                throw new IllegalStateException("the session follows no clock of the domain after an event about it");
            }
            return read;
        }

        /**
         * The first tick of the followed generation. A state reporting that generation again is
         * taken on the way; one of another generation fails the probe.
         */
        ClockEvent firstTick(MemorySegment deadline) {
            long followed = generation;
            while (true) {
                ClockEvent event = next(deadline);
                String kind = event.kind();
                if (kind.equals("TICK")) {
                    return event;
                }
                event.arena.close();
                if (!kind.equals("STATE") || generation != followed) {
                    throw new IllegalStateException("the clock reported " + kind
                            + " before the first tick of the followed generation");
                }
            }
        }

        /**
         * The paced state of a generation after the followed one. The followed generation's ticks
         * and the states before the new paced one are taken on the way.
         */
        ClockEvent nextGeneration(MemorySegment deadline) {
            long previous = generation;
            while (true) {
                ClockEvent event = next(deadline);
                String kind = event.kind();
                if (kind.equals("STATE") && Long.compareUnsigned(generation, previous) > 0 && paced != null) {
                    return event;
                }
                event.arena.close();
                if (!kind.equals("TICK") && !kind.equals("STATE")) {
                    throw new IllegalStateException("the clock reported " + kind
                            + " before a generation after the followed one");
                }
            }
        }

        /** Waits for the interruption; the followed generation's ticks and states are taken on the way. */
        void interruption(MemorySegment deadline) {
            long followed = generation;
            while (true) {
                ClockEvent event = next(deadline);
                String kind = event.kind();
                event.arena.close();
                if (kind.equals("INTERRUPTED")) {
                    return;
                }
                if (!kind.equals("TICK") && (!kind.equals("STATE") || generation != followed)) {
                    throw new IllegalStateException("the clock reported " + kind + " before the interruption");
                }
            }
        }

        /**
         * The paced state the restored attachment reports. A refused restoration, which the session
         * repeats, and a clock reported uninstalled are taken on the way.
         */
        ClockEvent restored(MemorySegment deadline) {
            while (true) {
                ClockEvent event = next(deadline);
                if (event.kind().equals("STATE") && paced != null) {
                    return event;
                }
                event.arena.close();
            }
        }

        /** Reports the paced state a STATE event reports and the first tick after it. */
        void reportStateAndFirstTick(ClockEvent state, MemorySegment deadline) {
            report(state.paced().line("STATE", domain));
            state.arena.close();
            ClockEvent tick = firstTick(deadline);
            report(clock().tickLine(tick.tick(), domain));
            tick.arena.close();
        }
    }

    /**
     * Attaches to the domain's running clock and reads the clock the attach reported before its
     * first tick, then follows the generation the scenario's STOP and START begin and the attachment
     * restored after the scenario ends the session, and detaches.
     */
    static void runClock(Session session, String domain, Arena outcomes) throws InterruptedException {
        checkClockCancellation(session, domain);
        report("ATTACHED " + session.execute("ATTACH DOMAIN CLOCK;", MemorySegment.NULL, outcomes).disposition());
        try (Arena tokens = Arena.ofConfined()) {
            // The clock the attach reported, read before any event about the attachment.
            DomainClock read = session.domainClock(domain, Arena.ofShared());
            if (read == null) {
                throw new IllegalStateException("the session follows no clock after its attach completed");
            }
            if (read.state() != CLOCK_PACED) {
                throw new IllegalStateException("the attach reported a clock other than the running paced one");
            }
            PacedClock clock = read.paced();
            String reportedClock = clock.line("CLOCK", domain);
            report(reportedClock);
            report(clock.projectionLine(read, domain));

            FollowedClock followed = new FollowedClock(session, domain, clock);
            ClockEvent tick = followed.firstTick(cancel(tokens, 120_000L));
            String reportedTick = clock.tickLine(tick.tick(), domain);
            report(reportedTick);

            // Keep a second reference to the read and the tick and release the first ones on
            // another thread, so both must read the same on the second alone.
            try (Arena retained = Arena.ofConfined()) {
                DomainClock retainedRead = read.retain(retained);
                ClockEvent retainedTick = tick.retain(retained);
                Thread closer = new Thread(() -> {
                    read.arena.close();
                    tick.arena.close();
                });
                closer.start();
                closer.join();
                PacedClock clockAgain = retainedRead.paced();
                if (!clockAgain.line("CLOCK", domain).equals(reportedClock)
                        || !clockAgain.tickLine(retainedTick.tick(), domain).equals(reportedTick)) {
                    throw new IllegalStateException("a retained clock or tick reads differently than it did");
                }
            }

            // The scenario stops the domain and starts it again at another origin and rate.
            MemorySegment started = cancel(tokens, 120_000L);
            followed.reportStateAndFirstTick(followed.nextGeneration(started), started);

            // The scenario ends the session, and the binding attaches the clock again on the next one.
            MemorySegment restoring = cancel(tokens, 120_000L);
            followed.interruption(restoring);
            report("INTERRUPTED domain=" + domain);
            followed.reportStateAndFirstTick(followed.restored(restoring), restoring);
        }
        report("DETACHED " + session.execute("DETACH DOMAIN CLOCK;", MemorySegment.NULL, outcomes).disposition());
        report("CHECKS ok");
    }

    // ---- Producers and consumers ----------------------------------------------------------------

    static final ValueLayout.OfLong U64 = ValueLayout.JAVA_LONG;

    static final MethodHandle ERROR_OPEN_REFUSAL = function("nx_error_open_refusal", BOOL, POINTER, POINTER);
    static final MethodHandle FIELDS_NEW = function("nx_fields_new", POINTER);
    static final MethodHandle FIELDS_ADD = function("nx_fields_add", POINTER,
            POINTER, POINTER, SIZE, INT, INT, BOOL, BOOL);
    static final MethodHandle FIELDS_ELEMENT = function("nx_fields_element", POINTER, POINTER, INT, INT);
    static final MethodHandle FIELDS_FREE = function("nx_fields_free", null, POINTER);
    static final MethodHandle SCHEMA_FIELD_LEVELS =
            function("nx_schema_field_levels", POINTER, POINTER, INT, SIZE, POINTER);
    static final MethodHandle SCHEMA_FIELD_LEVEL =
            function("nx_schema_field_level", POINTER, POINTER, INT, SIZE, SIZE, POINTER, POINTER);
    static final MethodHandle OPEN_INGESTOR = function("nx_session_open_ingestor", POINTER,
            POINTER, POINTER, SIZE, POINTER, SIZE, POINTER, INT, U64, POINTER, POINTER);
    static final MethodHandle SUBSCRIBE_EMITTER = function("nx_session_subscribe_emitter", POINTER,
            POINTER, POINTER, SIZE, POINTER, SIZE, POINTER, INT, U64, POINTER, POINTER);
    static final MethodHandle PRODUCER_SCHEMA = function("nx_producer_schema", POINTER, POINTER, POINTER);
    static final MethodHandle PRODUCER_GENERATION = function("nx_producer_generation", U64, POINTER);
    static final MethodHandle PRODUCER_GRANT =
            function("nx_producer_grant", null, POINTER, POINTER, POINTER, POINTER, POINTER);
    static final MethodHandle PRODUCER_POLICY =
            function("nx_producer_policy", null, POINTER, POINTER, POINTER, POINTER, POINTER, POINTER);
    static final MethodHandle PRODUCER_ADMISSION = function("nx_producer_admission", INT, POINTER);
    static final MethodHandle PRODUCER_STATE = function("nx_producer_state", INT, POINTER);
    static final MethodHandle PRODUCER_SUBMIT =
            function("nx_producer_submit", POINTER, POINTER, POINTER, POINTER, POINTER);
    static final MethodHandle PRODUCER_SUBMIT_IPC =
            function("nx_producer_submit_ipc", POINTER, POINTER, POINTER, SIZE, POINTER, POINTER);
    static final MethodHandle PRODUCER_REJOIN =
            function("nx_producer_rejoin", POINTER, POINTER, U64, POINTER, POINTER);
    static final MethodHandle PRODUCER_PENDING =
            function("nx_producer_pending", POINTER, POINTER, POINTER, POINTER, SIZE, POINTER);
    static final MethodHandle PRODUCER_CLOSE = function("nx_producer_close", POINTER, POINTER, POINTER);
    static final MethodHandle PRODUCER_FREE = function("nx_producer_free", null, POINTER);
    static final MethodHandle OUTCOME_RESULT = function("nx_submission_outcome_result", INT, POINTER);
    static final MethodHandle OUTCOME_REFUSAL =
            function("nx_submission_outcome_refusal", POINTER, POINTER, POINTER);
    static final MethodHandle OUTCOME_DEFECT =
            function("nx_submission_outcome_defect", POINTER, POINTER, POINTER);
    static final MethodHandle OUTCOME_FAILURE =
            function("nx_submission_outcome_failure", POINTER, POINTER, POINTER);
    static final MethodHandle OUTCOME_UNCERTAINTY =
            function("nx_submission_outcome_uncertainty", POINTER, POINTER, POINTER);
    static final MethodHandle SUBMISSION_FREE = function("nx_submission_outcome_free", null, POINTER);
    static final MethodHandle CONSUMER_SCHEMA = function("nx_consumer_schema", POINTER, POINTER, POINTER);
    static final MethodHandle CONSUMER_GENERATION = function("nx_consumer_generation", U64, POINTER);
    static final MethodHandle CONSUMER_GRANT =
            function("nx_consumer_grant", null, POINTER, POINTER, POINTER, POINTER, POINTER);
    static final MethodHandle CONSUMER_POLICY =
            function("nx_consumer_policy", null, POINTER, POINTER, POINTER, POINTER, POINTER, POINTER);
    static final MethodHandle CONSUMER_STATE = function("nx_consumer_state", INT, POINTER);
    static final MethodHandle CONSUMER_NEXT = function("nx_consumer_next", POINTER, POINTER, POINTER, POINTER);
    static final MethodHandle CONSUMER_CLOSE = function("nx_consumer_close", POINTER, POINTER, POINTER);
    static final MethodHandle CONSUMER_FREE = function("nx_consumer_free", null, POINTER);
    static final MethodHandle DELIVERY_IDENTITY = function("nx_delivery_identity", null, POINTER, POINTER, POINTER);
    static final MethodHandle DELIVERY_REFERENCE =
            function("nx_delivery_reference", null, POINTER, POINTER, POINTER);
    static final MethodHandle DELIVERY_RELAY =
            function("nx_delivery_source_relay", null, POINTER, POINTER, POINTER);
    static final MethodHandle DELIVERY_FINGERPRINT =
            function("nx_delivery_branch_fingerprint", BOOL, POINTER, POINTER, POINTER);
    static final MethodHandle DELIVERY_MEMBERS = function("nx_delivery_members", INT, POINTER);
    static final MethodHandle DELIVERY_IPC = function("nx_delivery_ipc", null, POINTER, POINTER, POINTER);
    static final MethodHandle DELIVERY_BATCH = function("nx_delivery_batch", POINTER, POINTER, POINTER);
    static final MethodHandle DELIVERY_ACK = function("nx_delivery_ack", POINTER, POINTER, POINTER, POINTER);
    static final MethodHandle DELIVERY_RETRY = function("nx_delivery_retry", POINTER, POINTER, POINTER, POINTER);
    static final MethodHandle DELIVERY_REJECT =
            function("nx_delivery_reject", POINTER, POINTER, POINTER, SIZE, POINTER, POINTER);
    static final MethodHandle DELIVERY_RETAIN = function("nx_delivery_retain", POINTER, POINTER);
    static final MethodHandle DELIVERY_RELEASE = function("nx_delivery_release", null, POINTER);
    static final MethodHandle BATCH_ROW_COUNT = function("nx_batch_row_count", SIZE, POINTER);
    static final MethodHandle BATCH_IPC = function("nx_batch_ipc", POINTER, POINTER, POINTER, POINTER);
    static final MethodHandle BATCH_CELLS = function("nx_batch_cells", POINTER, POINTER, SIZE, SIZE, POINTER);
    static final MethodHandle BATCH_STATES = function("nx_batch_states", POINTER, POINTER, SIZE, POINTER, SIZE);
    static final MethodHandle BATCH_OFFSETS =
            function("nx_batch_offsets", POINTER, POINTER, SIZE, SIZE, POINTER, SIZE);
    static final MethodHandle BATCH_FIXED = function("nx_batch_fixed", POINTER, POINTER, SIZE, SIZE, POINTER, SIZE);
    static final MethodHandle BATCH_VARLEN = function("nx_batch_varlen", POINTER,
            POINTER, SIZE, SIZE, POINTER, SIZE, POINTER, SIZE, POINTER);
    static final MethodHandle BATCH_RETAIN = function("nx_batch_retain", POINTER, POINTER);
    static final MethodHandle BATCH_RELEASE = function("nx_batch_release", null, POINTER);
    static final MethodHandle BUILDER_NEW = function("nx_batch_builder_new", POINTER, POINTER, SIZE, POINTER);
    static final MethodHandle BUILDER_STATES =
            function("nx_batch_builder_states", POINTER, POINTER, SIZE, POINTER, SIZE);
    static final MethodHandle BUILDER_OFFSETS =
            function("nx_batch_builder_offsets", POINTER, POINTER, SIZE, SIZE, POINTER, SIZE);
    static final MethodHandle BUILDER_FIXED =
            function("nx_batch_builder_fixed", POINTER, POINTER, SIZE, SIZE, POINTER, SIZE);
    static final MethodHandle BUILDER_VARLEN = function("nx_batch_builder_varlen", POINTER,
            POINTER, SIZE, SIZE, POINTER, SIZE, POINTER, SIZE);
    static final MethodHandle BUILDER_FINISH = function("nx_batch_builder_finish", POINTER, POINTER, POINTER);
    static final MethodHandle BUILDER_FREE = function("nx_batch_builder_free", null, POINTER);

    static final long WAIT_MILLIS = 120_000L;
    static final long EXPIRING_MILLIS = 200L;
    static final int PRODUCER_BATCHES = 2;
    static final int CONSUMER_BATCHES = 4;
    static final long ENDPOINT_BYTES = 1_048_576L;
    static final byte SCRIBBLE = (byte) 0xaa;
    static final int ERROR_INVALID_ARGUMENT = 1;
    static final int ERROR_REJECTED = 5;
    static final int ERROR_INTERRUPTED = 12;
    static final int OPEN_SCHEMA_MISMATCH = 6;
    static final int ENDPOINT_ACTIVE = 1;
    static final int ENDPOINT_CLOSED = 5;
    static final String[] STATES = {null, "active", "interrupted", "restoring", "reopen_required", "closed"};
    static final String[] SETTLEMENTS = {
        null, "confirmed", "stale_reference", "wrong_consumer", "invalid_reason", "consumer_ended",
    };
    static final String[] REFUSALS = {
        null, "invalid_batch", "suspended", "busy", "draining", "producer_ended", "credit_exceeded",
    };
    static final String[] DEFECTS = {
        null, "malformed", "unexpected_message", "compressed", "schema_mismatch", "not_one_batch",
        "too_many_rows", "too_large", "invalid_data",
    };
    static final String[] FAILURES = {null, "ack_timed_out", "rejected"};
    static final String[] UNCERTAINTIES = {null, "interrupted", "owner_lost", "session_lost"};

    static int typeCode(String name) {
        return Arrays.asList(TYPES).indexOf(name);
    }

    /** One level of a field's type, as a host names it. */
    record Level(String type, int length) {}

    /** One field the probe expects an endpoint to have. */
    record FieldSpec(String name, List<Level> levels, boolean nullable, boolean sensitive) {}

    static FieldSpec scalar(String name, String type, boolean nullable, boolean sensitive) {
        return new FieldSpec(name, List.of(new Level(type, 0)), nullable, sensitive);
    }

    /** The input schema of the probe's ingestor, in declared order. */
    static List<FieldSpec> inputFields() {
        return List.of(
                scalar("id", "U32", false, false),
                scalar("tenant", "STRING", false, false),
                scalar("u8v", "U8", false, false),
                scalar("i8v", "I8", false, false),
                scalar("u16v", "U16", false, false),
                scalar("i16v", "I16", false, false),
                scalar("u32v", "U32", false, false),
                scalar("i32v", "I32", false, false),
                scalar("u64v", "U64", false, false),
                scalar("i64v", "I64", false, false),
                scalar("f32v", "F32", false, false),
                scalar("f64v", "F64", false, false),
                scalar("flag", "BOOL", false, false),
                scalar("text", "STRING", true, false),
                scalar("raw", "BYTES", true, false),
                scalar("at", "DATETIME", false, false),
                scalar("maybe", "I64", true, false),
                new FieldSpec("tags", List.of(new Level("LIST", 0), new Level("STRING", 0)), false, false),
                new FieldSpec("grid", List.of(new Level("FIXED_LIST", 2), new Level("FIXED_LIST", 2),
                        new Level("I16", 0)), false, false),
                new FieldSpec("spans", List.of(new Level("LIST", 0), new Level("FIXED_LIST", 2),
                        new Level("DATETIME", 0)), true, false),
                scalar("secret", "STRING", false, true));
    }

    /** One row of an input batch, as the probe's application holds it. */
    static final class Row {
        int id;
        String tenant;
        long u8v, i8v, u16v, i16v, u32v, i32v, u64v, i64v, f32v, f64v;
        boolean flag;
        byte[] text;
        byte[] raw;
        long at;
        Long maybe;
        List<String> tags = List.of();
        short[][] grid = {{0, 0}, {0, 0}};
        List<long[]> spans;
        String secret = "s";

        static Row plain(int id, String tenant) {
            Row row = new Row();
            row.id = id;
            row.tenant = tenant;
            return row;
        }
    }

    static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /** The typed rows of the first batch. */
    static List<Row> typedRows() {
        Row first = Row.plain(1, "acme");
        first.u8v = 255;
        first.i8v = 127;
        first.u16v = 65535;
        first.i16v = 32767;
        first.u32v = 4294967295L;
        first.i32v = Integer.MAX_VALUE;
        first.u64v = -1L;
        first.i64v = Long.MAX_VALUE;
        first.f32v = 0x7f7fffffL;
        first.f64v = 0x7fefffffffffffffL;
        first.flag = true;
        first.text = bytes("héllo 世界 🚀");
        first.raw = new byte[] {0x00, (byte) 0xff, (byte) 0xfe, (byte) 0x80, 0x00};
        first.at = Long.MAX_VALUE;
        first.tags = List.of("a", "", "héllo");
        first.grid = new short[][] {{1, -2}, {Short.MAX_VALUE, Short.MIN_VALUE}};
        first.spans = List.of(new long[] {Long.MIN_VALUE, Long.MAX_VALUE});
        first.secret = "s1";

        Row second = Row.plain(2, "acme");
        second.i8v = -128;
        second.i16v = -32768;
        second.i32v = Integer.MIN_VALUE;
        second.i64v = Long.MIN_VALUE;
        second.f32v = 0x80000000L;
        second.f64v = 0x0000000000000001L;
        second.text = new byte[] {'a', 0, 'b'};
        second.raw = new byte[0];
        second.at = Long.MIN_VALUE;
        second.maybe = 0L;
        second.secret = "s2";

        Row third = Row.plain(3, "acme");
        third.u8v = 1;
        third.i8v = -1;
        third.u16v = 1;
        third.i16v = -1;
        third.u32v = 1;
        third.i32v = -1;
        third.u64v = 9007199254740993L;
        third.i64v = -9007199254740993L;
        third.f32v = 0x3fc00000L;
        third.f64v = 0x3fb999999999999aL;
        third.flag = true;
        third.at = 1;
        third.maybe = 9007199254740992L;
        third.tags = List.of("x");
        third.grid = new short[][] {{-1, 1}, {2, -2}};
        third.spans = List.of();
        third.secret = "s3";
        return List.of(first, second, third);
    }

    /** A little-endian-agnostic writer of native-order values. */
    static final class Buffer {
        java.io.ByteArrayOutputStream data = new java.io.ByteArrayOutputStream();

        void put(long value, int width) {
            ByteBuffer buffer = ByteBuffer.allocate(8).order(java.nio.ByteOrder.nativeOrder());
            switch (width) {
                case 1 -> buffer.put((byte) value);
                case 2 -> buffer.putShort((short) value);
                case 4 -> buffer.putInt((int) value);
                default -> buffer.putLong(value);
            }
            data.write(buffer.array(), 0, width);
        }

        void put(byte[] bytes) {
            data.writeBytes(bytes);
        }

        byte[] toArray() {
            return data.toByteArray();
        }
    }

    /** What a host passes the binding for one column. */
    static final class HostColumn {
        byte[] states;
        long[] listOffsets;
        long leaf;
        boolean varlen;
        byte[] values = new byte[0];
        long[] valueOffsets;
    }

    static long[] offsets(List<Integer> lengths) {
        long[] offsets = new long[lengths.size() + 1];
        for (int index = 0; index < lengths.size(); index++) {
            offsets[index + 1] = offsets[index] + lengths.get(index);
        }
        return offsets;
    }

    /** The column `name` of `rows`, as a host lays it out. */
    static HostColumn hostColumn(String name, List<Row> rows) {
        HostColumn column = new HostColumn();
        Buffer values = new Buffer();
        List<Integer> valueLengths = new ArrayList<>();
        List<Integer> listLengths = new ArrayList<>();
        Buffer states = new Buffer();
        for (Row row : rows) {
            switch (name) {
                case "id", "echo" -> values.put(row.id, 4);
                case "u8v" -> values.put(row.u8v, 1);
                case "i8v" -> values.put(row.i8v, 1);
                case "u16v" -> values.put(row.u16v, 2);
                case "i16v" -> values.put(row.i16v, 2);
                case "u32v" -> values.put(row.u32v, 4);
                case "i32v" -> values.put(row.i32v, 4);
                case "u64v" -> values.put(row.u64v, 8);
                case "i64v" -> values.put(row.i64v, 8);
                case "f32v" -> values.put(row.f32v, 4);
                case "f64v" -> values.put(row.f64v, 8);
                case "flag" -> values.put(row.flag ? 1 : 0, 1);
                case "at" -> values.put(row.at, 8);
                case "tenant", "secret" -> {
                    byte[] value = bytes(name.equals("tenant") ? row.tenant : row.secret);
                    column.varlen = true;
                    values.put(value);
                    valueLengths.add(value.length);
                }
                case "text", "raw" -> {
                    byte[] value = name.equals("text") ? row.text : row.raw;
                    column.varlen = true;
                    states.put(value == null ? CELL_NULL : CELL_VALUE, 1);
                    byte[] present = value == null ? new byte[0] : value;
                    values.put(present);
                    valueLengths.add(present.length);
                }
                case "maybe" -> {
                    states.put(row.maybe == null ? CELL_NULL : CELL_VALUE, 1);
                    values.put(row.maybe == null ? 0 : row.maybe, 8);
                }
                case "tags" -> {
                    column.varlen = true;
                    column.leaf = 1;
                    listLengths.add(row.tags.size());
                    for (String tag : row.tags) {
                        values.put(bytes(tag));
                        valueLengths.add(bytes(tag).length);
                    }
                }
                case "grid" -> {
                    column.leaf = 2;
                    for (short[] pair : row.grid) {
                        for (short cell : pair) {
                            values.put(cell, 2);
                        }
                    }
                }
                case "spans" -> {
                    column.leaf = 2;
                    states.put(row.spans == null ? CELL_NULL : CELL_VALUE, 1);
                    List<long[]> spans = row.spans == null ? List.of() : row.spans;
                    listLengths.add(spans.size());
                    for (long[] span : spans) {
                        values.put(span[0], 8);
                        values.put(span[1], 8);
                    }
                }
                default -> throw new IllegalStateException("the probe holds no column named " + name);
            }
        }
        column.values = values.toArray();
        byte[] stateBytes = states.toArray();
        column.states = stateBytes.length == 0 ? null : stateBytes;
        column.listOffsets = name.equals("tags") || name.equals("spans") ? offsets(listLengths) : null;
        column.valueOffsets = column.varlen ? offsets(valueLengths) : null;
        return column;
    }

    static MemorySegment fieldsHandle(List<FieldSpec> specs) {
        MemorySegment fields = (MemorySegment) call(FIELDS_NEW);
        try (Arena scratch = Arena.ofConfined()) {
            for (FieldSpec spec : specs) {
                Level first = spec.levels().get(0);
                check(call(FIELDS_ADD, fields, text(scratch, spec.name()), length(spec.name()),
                        typeCode(first.type()), first.length(), spec.nullable(), spec.sensitive()));
                for (Level level : spec.levels().subList(1, spec.levels().size())) {
                    check(call(FIELDS_ELEMENT, fields, typeCode(level.type()), level.length()));
                }
            }
        }
        return fields;
    }

    /** One field of a schema the binding reported, with every level of its type. */
    record Reported(String name, List<Level> levels, boolean nullable, boolean sensitive) {
        static String typeText(List<Level> levels) {
            String kind = levels.get(0).type();
            if (kind.equals("LIST")) {
                return "VEC<" + typeText(levels.subList(1, levels.size())) + ">";
            }
            if (kind.equals("FIXED_LIST")) {
                List<String> dimensions = new ArrayList<>();
                int end = 0;
                while (end < levels.size() && levels.get(end).type().equals("FIXED_LIST")) {
                    dimensions.add(Integer.toString(levels.get(end).length()));
                    end++;
                }
                return "ARRAY<" + typeText(levels.subList(end, levels.size())) + ", "
                        + String.join(", ", dimensions) + ">";
            }
            return kind;
        }

        String line(String prefix) {
            return prefix + " " + name + " " + typeText(levels) + " " + (nullable ? "nullable" : "required")
                    + " " + (sensitive ? "sensitive" : "public");
        }
    }

    static List<Reported> reported(MemorySegment schema) {
        List<Reported> fields = new ArrayList<>();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment count = arena.allocate(SIZE);
            check(call(SCHEMA_FIELD_COUNT, schema, PART_ROWS, count));
            for (long index = 0; index < count.get(SIZE, 0); index++) {
                MemorySegment name = arena.allocate(POINTER);
                MemorySegment nameLen = arena.allocate(SIZE);
                MemorySegment type = arena.allocate(INT);
                MemorySegment nullable = arena.allocate(BOOL);
                MemorySegment sensitive = arena.allocate(BOOL);
                check(call(SCHEMA_FIELD, schema, PART_ROWS, index, name, nameLen, type, nullable, sensitive));
                MemorySegment levelCount = arena.allocate(SIZE);
                check(call(SCHEMA_FIELD_LEVELS, schema, PART_ROWS, index, levelCount));
                List<Level> levels = new ArrayList<>();
                for (long level = 0; level < levelCount.get(SIZE, 0); level++) {
                    MemorySegment levelType = arena.allocate(INT);
                    MemorySegment levelLength = arena.allocate(INT);
                    check(call(SCHEMA_FIELD_LEVEL, schema, PART_ROWS, index, level, levelType, levelLength));
                    levels.add(new Level(TYPES[levelType.get(INT, 0)], levelLength.get(INT, 0)));
                }
                if (!levels.get(0).type().equals(TYPES[type.get(INT, 0)])) {
                    throw new IllegalStateException("a field's first level differs from its type");
                }
                fields.add(new Reported(utf8(borrowed(name, nameLen)), levels, nullable.get(BOOL, 0),
                        sensitive.get(BOOL, 0)));
            }
        }
        return fields;
    }

    static MemorySegment segmentOf(Arena arena, byte[] bytes) {
        return arena.allocateFrom(ValueLayout.JAVA_BYTE, bytes.length == 0 ? new byte[1] : bytes);
    }

    static MemorySegment segmentOf(Arena arena, long[] values) {
        return arena.allocateFrom(ValueLayout.JAVA_LONG, values);
    }

    /** One list level of a read column: the offsets of a LIST level, or a FIXED_LIST's length. */
    record Shape(long[] offsets, int length) {}

    /** One reference to a batch, owned by an arena: closing the arena releases it. */
    record Batch(MemorySegment handle) {
        static Batch of(MemorySegment raw, Arena arena) {
            return new Batch(raw.reinterpret(arena, segment -> call(BATCH_RELEASE, segment)));
        }

        /** Builds a batch of `rows`, writing over every buffer once the binding has copied it. */
        static Batch build(MemorySegment schema, List<Row> rows, Arena into) {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment out = scratch.allocate(POINTER);
                check(call(BUILDER_NEW, schema, (long) rows.size(), out));
                MemorySegment builder = out.get(POINTER, 0);
                try {
                    List<Reported> fields = reported(schema);
                    for (int index = 0; index < fields.size(); index++) {
                        HostColumn column = hostColumn(fields.get(index).name(), rows);
                        if (column.states != null) {
                            MemorySegment states = segmentOf(scratch, column.states);
                            check(call(BUILDER_STATES, builder, (long) index, states, (long) column.states.length));
                            states.fill(SCRIBBLE);
                        }
                        if (column.listOffsets != null) {
                            MemorySegment offsets = segmentOf(scratch, column.listOffsets);
                            check(call(BUILDER_OFFSETS, builder, (long) index, 0L, offsets,
                                    (long) column.listOffsets.length));
                            offsets.fill((byte) 0xff);
                        }
                        MemorySegment values = segmentOf(scratch, column.values);
                        if (column.varlen) {
                            MemorySegment offsets = segmentOf(scratch, column.valueOffsets);
                            check(call(BUILDER_VARLEN, builder, (long) index, column.leaf, offsets,
                                    (long) column.valueOffsets.length, values, (long) column.values.length));
                            offsets.fill((byte) 0xff);
                        } else {
                            check(call(BUILDER_FIXED, builder, (long) index, column.leaf, values,
                                    (long) column.values.length));
                        }
                        values.fill(SCRIBBLE);
                    }
                    MemorySegment batch = scratch.allocate(POINTER);
                    check(call(BUILDER_FINISH, builder, batch));
                    return Batch.of(batch.get(POINTER, 0), into);
                } finally {
                    call(BUILDER_FREE, builder);
                }
            }
        }

        /** The stream, borrowed for exactly as long as this batch's arena lives. */
        byte[] stream() {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment ipc = scratch.allocate(POINTER);
                MemorySegment ipcLen = scratch.allocate(SIZE);
                check(call(BATCH_IPC, handle, ipc, ipcLen));
                return borrowed(ipc, ipcLen);
            }
        }

        long cells(Arena scratch, long column, long level) {
            MemorySegment cells = scratch.allocate(SIZE);
            check(call(BATCH_CELLS, handle, column, level, cells));
            return cells.get(SIZE, 0);
        }

        /** Reads one column in one call per level of its type and renders each row. */
        List<String> column(long index, Reported field) {
            try (Arena scratch = Arena.ofConfined()) {
                long rows = cells(scratch, index, 0);
                MemorySegment states = scratch.allocate(Math.max(rows, 1));
                check(call(BATCH_STATES, handle, index, states, rows));
                int innermost = field.levels().size() - 1;
                List<Shape> lists = new ArrayList<>();
                for (int level = 0; level < innermost; level++) {
                    Level shape = field.levels().get(level);
                    if (shape.type().equals("LIST")) {
                        long count = cells(scratch, index, level) + 1;
                        MemorySegment offsets = scratch.allocate(8 * count, 8);
                        check(call(BATCH_OFFSETS, handle, index, (long) level, offsets, count));
                        lists.add(new Shape(offsets.toArray(ValueLayout.JAVA_LONG), 0));
                    } else {
                        lists.add(new Shape(null, shape.length()));
                    }
                }
                String kind = field.levels().get(innermost).type();
                long cells = cells(scratch, index, innermost);
                List<String> leaves = new ArrayList<>();
                if (kind.equals("STRING") || kind.equals("BYTES")) {
                    MemorySegment offsets = scratch.allocate(8 * (cells + 1), 8);
                    MemorySegment dataLen = scratch.allocate(SIZE);
                    check(call(BATCH_VARLEN, handle, index, (long) innermost, offsets, cells + 1,
                            MemorySegment.NULL, 0L, dataLen));
                    long needed = dataLen.get(SIZE, 0);
                    MemorySegment data = scratch.allocate(Math.max(needed, 1));
                    check(call(BATCH_VARLEN, handle, index, (long) innermost, offsets, cells + 1, data,
                            needed, dataLen));
                    for (long cell = 0; cell < cells; cell++) {
                        long start = offsets.getAtIndex(ValueLayout.JAVA_LONG, cell);
                        long end = offsets.getAtIndex(ValueLayout.JAVA_LONG, cell + 1);
                        byte[] value = data.asSlice(start, end - start).toArray(ValueLayout.JAVA_BYTE);
                        leaves.add((kind.equals("STRING") ? "str:" : "bytes:") + HexFormat.of().formatHex(value));
                    }
                } else {
                    int width = switch (kind) {
                        case "U8", "I8", "BOOL" -> 1;
                        case "U16", "I16" -> 2;
                        case "U32", "I32", "F32" -> 4;
                        default -> 8;
                    };
                    MemorySegment values = scratch.allocate(Math.max(cells * width, 1), 8);
                    if (cells > 0) {
                        check(call(BATCH_FIXED, handle, index, (long) innermost, values, cells * width));
                    }
                    for (long cell = 0; cell < cells; cell++) {
                        long offset = cell * width;
                        leaves.add(switch (kind) {
                            case "U8" -> "u8:" + Byte.toUnsignedInt(values.get(ValueLayout.JAVA_BYTE, offset));
                            case "I8" -> "i8:" + values.get(ValueLayout.JAVA_BYTE, offset);
                            case "BOOL" -> "bool:" + (values.get(ValueLayout.JAVA_BYTE, offset) == 1);
                            case "U16" -> "u16:" + Short.toUnsignedInt(values.get(ValueLayout.JAVA_SHORT, offset));
                            case "I16" -> "i16:" + values.get(ValueLayout.JAVA_SHORT, offset);
                            case "U32" -> "u32:" + Integer.toUnsignedString(values.get(ValueLayout.JAVA_INT, offset));
                            case "I32" -> "i32:" + values.get(ValueLayout.JAVA_INT, offset);
                            case "F32" -> "f32:" + String.format("%08x", values.get(ValueLayout.JAVA_INT, offset));
                            case "U64" -> "u64:" + Long.toUnsignedString(values.get(ValueLayout.JAVA_LONG, offset));
                            case "I64" -> "i64:" + values.get(ValueLayout.JAVA_LONG, offset);
                            case "F64" -> "f64:" + String.format("%016x", values.get(ValueLayout.JAVA_LONG, offset));
                            case "DATETIME" -> "datetime:" + values.get(ValueLayout.JAVA_LONG, offset);
                            default -> throw new IllegalStateException("a leaf is not a scalar");
                        });
                    }
                }
                List<String> rendered = new ArrayList<>();
                for (long row = 0; row < rows; row++) {
                    rendered.add(states.get(ValueLayout.JAVA_BYTE, row) == CELL_NULL
                            ? "null"
                            : render(lists, leaves, 0, row));
                }
                return rendered;
            }
        }

        static String render(List<Shape> lists, List<String> leaves, int level, long cell) {
            if (level == lists.size()) {
                return leaves.get((int) cell);
            }
            Shape shape = lists.get(level);
            long start;
            long end;
            if (shape.offsets() == null) {
                start = cell * shape.length();
                end = start + shape.length();
            } else {
                start = shape.offsets()[(int) cell];
                end = shape.offsets()[(int) cell + 1];
            }
            List<String> elements = new ArrayList<>();
            for (long element = start; element < end; element++) {
                elements.add(render(lists, leaves, level + 1, element));
            }
            return "[" + String.join(",", elements) + "]";
        }

        List<String> rows(List<Reported> fields) {
            List<List<String>> columns = new ArrayList<>();
            for (int index = 0; index < fields.size(); index++) {
                columns.add(column(index, fields.get(index)));
            }
            long count = (long) call(BATCH_ROW_COUNT, handle);
            List<String> rows = new ArrayList<>();
            for (int row = 0; row < count; row++) {
                StringBuilder line = new StringBuilder("ROW");
                for (int column = 0; column < fields.size(); column++) {
                    line.append(' ').append(fields.get(column).name()).append('=')
                            .append(columns.get(column).get(row));
                }
                rows.add(line.toString());
            }
            return rows;
        }

        List<Long> ids(List<Reported> fields) {
            for (int index = 0; index < fields.size(); index++) {
                if (fields.get(index).name().equals("id")) {
                    List<Long> ids = new ArrayList<>();
                    for (String value : column(index, fields.get(index))) {
                        ids.add(Long.parseLong(value.substring("u32:".length())));
                    }
                    return ids;
                }
            }
            throw new IllegalStateException("the batch has no id column");
        }
    }

    /** One reference to a delivery, owned by an arena: closing the arena releases it. */
    record Delivery(MemorySegment handle) {
        static Delivery of(MemorySegment raw, Arena arena) {
            return new Delivery(raw.reinterpret(arena, segment -> call(DELIVERY_RELEASE, segment)));
        }

        Delivery retain(Arena into) {
            return Delivery.of((MemorySegment) call(DELIVERY_RETAIN, handle), into);
        }

        byte[] read(MethodHandle accessor) {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment data = scratch.allocate(POINTER);
                MemorySegment dataLen = scratch.allocate(SIZE);
                call(accessor, handle, data, dataLen);
                return borrowed(data, dataLen);
            }
        }

        String summary() {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment fingerprint = scratch.allocate(POINTER);
                MemorySegment fingerprintLen = scratch.allocate(SIZE);
                boolean branched = (boolean) call(DELIVERY_FINGERPRINT, handle, fingerprint, fingerprintLen);
                return "DELIVERY relay=" + utf8(read(DELIVERY_RELAY)) + " members="
                        + Integer.toUnsignedString((int) call(DELIVERY_MEMBERS, handle)) + " branch="
                        + (branched ? Long.toString(fingerprintLen.get(SIZE, 0)) : "none") + " identity="
                        + read(DELIVERY_IDENTITY).length + " reference=" + read(DELIVERY_REFERENCE).length;
            }
        }

        Batch batch(Arena into) {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment batch = scratch.allocate(POINTER);
                check(call(DELIVERY_BATCH, handle, batch));
                return Batch.of(batch.get(POINTER, 0), into);
            }
        }

        String settle(MethodHandle settle) {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment deadline = cancel(scratch, WAIT_MILLIS);
                MemorySegment settlement = scratch.allocate(INT);
                check(call(settle, handle, deadline, settlement));
                return SETTLEMENTS[settlement.get(INT, 0)];
            }
        }

        String reject(String reason) {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment deadline = cancel(scratch, WAIT_MILLIS);
                MemorySegment settlement = scratch.allocate(INT);
                check(call(DELIVERY_REJECT, handle, text(scratch, reason), length(reason), deadline, settlement));
                return SETTLEMENTS[settlement.get(INT, 0)];
            }
        }
    }

    static final class Endpoints {
        final Session session;
        final String domain;
        final String ingestor = System.getenv("NERVIX_PROBE_INGESTOR");
        final String emitter = System.getenv("NERVIX_PROBE_EMITTER");
        final List<String> failures = new ArrayList<>();

        Endpoints(Session session, String domain) {
            this.session = session;
            this.domain = domain;
        }

        void expect(boolean holds, String what) {
            if (!holds) {
                failures.add(what);
            }
        }

        Object openProducer(MemorySegment fields, int batches, long asked, MemorySegment out) {
            try (Arena scratch = Arena.ofConfined()) {
                return call(OPEN_INGESTOR, session.handle, text(scratch, domain), length(domain),
                        text(scratch, ingestor), length(ingestor), fields, batches, asked,
                        MemorySegment.NULL, out);
            }
        }

        Object openConsumer(MemorySegment fields, MemorySegment out) {
            try (Arena scratch = Arena.ofConfined()) {
                return call(SUBSCRIBE_EMITTER, session.handle, text(scratch, domain), length(domain),
                        text(scratch, emitter), length(emitter), fields, CONSUMER_BATCHES, ENDPOINT_BYTES,
                        MemorySegment.NULL, out);
            }
        }

        static void expectRefused(Object result, String what) {
            MemorySegment error = (MemorySegment) result;
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment refusal = scratch.allocate(INT);
                if (error.equals(MemorySegment.NULL) || (int) call(ERROR_KIND, error) != ERROR_REJECTED
                        || !(boolean) call(ERROR_OPEN_REFUSAL, error, refusal)
                        || refusal.get(INT, 0) != OPEN_SCHEMA_MISMATCH) {
                    throw new IllegalStateException("an open with another schema was not refused exactly");
                }
            }
            call(ERROR_FREE, error);
            report("REFUSED " + what + " schema mismatch");
        }

        static long submit(MemorySegment producer, Batch batch, MemorySegment cancel) {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment submission = scratch.allocate(U64);
                check(call(PRODUCER_SUBMIT, producer, batch.handle(), cancel, submission));
                return submission.get(U64, 0);
            }
        }

        /** Builds a batch of `rows` and submits it, releasing the batch once it is submitted. */
        static long submitRows(MemorySegment producer, MemorySegment schema, List<Row> rows) {
            try (Arena batches = Arena.ofConfined()) {
                return submit(producer, Batch.build(schema, rows, batches), MemorySegment.NULL);
            }
        }

        static String outcome(MemorySegment producer, long submission) {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment deadline = cancel(scratch, WAIT_MILLIS);
                MemorySegment out = scratch.allocate(POINTER);
                check(call(PRODUCER_REJOIN, producer, submission, deadline, out));
                MemorySegment outcome = out.get(POINTER, 0);
                try {
                    int result = (int) call(OUTCOME_RESULT, outcome);
                    MemorySegment cause = scratch.allocate(INT);
                    switch (result) {
                        case 2:
                            return "completed";
                        case 1: {
                            check(call(OUTCOME_REFUSAL, outcome, cause));
                            int refusal = cause.get(INT, 0);
                            String text = "not_admitted " + REFUSALS[refusal];
                            if (refusal == 1) {
                                check(call(OUTCOME_DEFECT, outcome, cause));
                                text += " " + DEFECTS[cause.get(INT, 0)];
                            }
                            return text;
                        }
                        case 3:
                            check(call(OUTCOME_FAILURE, outcome, cause));
                            return "processing_failed " + FAILURES[cause.get(INT, 0)];
                        default:
                            check(call(OUTCOME_UNCERTAINTY, outcome, cause));
                            return "outcome_unknown " + UNCERTAINTIES[cause.get(INT, 0)];
                    }
                } finally {
                    call(SUBMISSION_FREE, outcome);
                }
            }
        }

        static Delivery next(MemorySegment consumer, MemorySegment cancel, Arena into) {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment out = scratch.allocate(POINTER);
                check(call(CONSUMER_NEXT, consumer, cancel, out));
                return Delivery.of(out.get(POINTER, 0), into);
            }
        }

        static Delivery next(MemorySegment consumer, Arena into) {
            try (Arena scratch = Arena.ofConfined()) {
                return next(consumer, cancel(scratch, WAIT_MILLIS), into);
            }
        }

        static String policy(MethodHandle read, MemorySegment handle) {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment window = scratch.allocate(INT);
                MemorySegment outstanding = scratch.allocate(U64);
                MemorySegment timeout = scratch.allocate(U64);
                MemorySegment backoff = scratch.allocate(U64);
                MemorySegment maximum = scratch.allocate(U64);
                call(read, handle, window, outstanding, timeout, backoff, maximum);
                return "window=" + (window.get(INT, 0) == 1 ? "sequential" : "parallel") + "/"
                        + Long.toUnsignedString(outstanding.get(U64, 0)) + " ack_timeout="
                        + Long.toUnsignedString(timeout.get(U64, 0));
            }
        }

        static long[] grant(MethodHandle read, MemorySegment handle) {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment batches = scratch.allocate(INT);
                MemorySegment granted = scratch.allocate(U64);
                MemorySegment maxRows = scratch.allocate(INT);
                MemorySegment maxBytes = scratch.allocate(U64);
                call(read, handle, batches, granted, maxRows, maxBytes);
                return new long[] {
                    Integer.toUnsignedLong(batches.get(INT, 0)), granted.get(U64, 0),
                    Integer.toUnsignedLong(maxRows.get(INT, 0)), maxBytes.get(U64, 0),
                };
            }
        }

        static MemorySegment handleOut(Object result, MemorySegment out) {
            check(result);
            return out.get(POINTER, 0);
        }

        void run() throws InterruptedException {
            List<FieldSpec> outputSpecs = new ArrayList<>(inputFields());
            outputSpecs.add(scalar("echo", "U32", false, false));
            List<FieldSpec> mismatchedSpecs = new ArrayList<>();
            for (FieldSpec spec : inputFields()) {
                mismatchedSpecs.add(spec.name().equals("secret")
                        ? new FieldSpec(spec.name(), spec.levels(), spec.nullable(), false)
                        : spec);
            }
            MemorySegment inputFields = fieldsHandle(inputFields());
            MemorySegment outputFields = fieldsHandle(outputSpecs);
            MemorySegment mismatched = fieldsHandle(mismatchedSpecs);
            try (Arena handles = Arena.ofShared()) {
                MemorySegment out = handles.allocate(POINTER);
                // An open whose expected fields differ from the endpoint's is refused exactly.
                expectRefused(openProducer(mismatched, PRODUCER_BATCHES, ENDPOINT_BYTES, out), "producer");
                expectRefused(openConsumer(inputFields, out), "consumer");
                call(FIELDS_FREE, mismatched);

                MemorySegment consumer = handleOut(openConsumer(outputFields, out), out);
                MemorySegment producer = handleOut(openProducer(inputFields, PRODUCER_BATCHES, ENDPOINT_BYTES, out), out);
                MemorySegment outputSchema = handleOut(call(CONSUMER_SCHEMA, consumer, out), out);
                MemorySegment inputSchema = handleOut(call(PRODUCER_SCHEMA, producer, out), out);
                List<Reported> output = reported(outputSchema);
                List<Reported> input = reported(inputSchema);

                long[] consumerGrant = grant(CONSUMER_GRANT, consumer);
                report("CONSUMER generation=" + Long.toUnsignedString((long) call(CONSUMER_GENERATION, consumer))
                        + " state=" + STATES[(int) call(CONSUMER_STATE, consumer)] + " "
                        + policy(CONSUMER_POLICY, consumer) + " credit=" + consumerGrant[0] + "/"
                        + consumerGrant[1] + " max=" + consumerGrant[2] + "/" + consumerGrant[3]);
                for (Reported field : output) {
                    report(field.line("OUTPUT FIELD"));
                }
                long[] producerGrant = grant(PRODUCER_GRANT, producer);
                expect(producerGrant[2] > 0 && producerGrant[3] > 0 && producerGrant[3] <= producerGrant[1],
                        "the producer's batch limits fit its grant");
                report("PRODUCER generation=" + Long.toUnsignedString((long) call(PRODUCER_GENERATION, producer))
                        + " state=" + STATES[(int) call(PRODUCER_STATE, producer)] + " admission="
                        + ((int) call(PRODUCER_ADMISSION, producer) == 1 ? "open" : "suspended") + " "
                        + policy(PRODUCER_POLICY, producer) + " credit=" + producerGrant[0] + "/"
                        + producerGrant[1]);
                for (Reported field : input) {
                    report(field.line("INPUT FIELD"));
                }
                report("OPENED");

                // A wait for output that nothing produces ends by its deadline, and one cancelled
                // from another thread by its token; the reads they leave behind are the consumer's.
                try (Arena scratch = Arena.ofConfined()) {
                    MemorySegment expiring = cancel(scratch, EXPIRING_MILLIS);
                    expect(failure(() -> next(consumer, expiring, handles)).kind == ERROR_DEADLINE,
                            "an expired read reports its deadline");
                }
                report("NEXT deadline");
                try (Arena tokens = Arena.ofShared()) {
                    MemorySegment token = cancel(tokens, null);
                    AtomicReference<Failure> cancelled = new AtomicReference<>();
                    Thread reader = new Thread(() -> cancelled.set(failure(() -> next(consumer, token, handles))));
                    reader.start();
                    Thread.sleep(100);
                    call(CANCEL_TRIGGER, token);
                    reader.join();
                    expect(cancelled.get().kind == ERROR_CANCELLED, "a cancelled read reports its cancellation");
                }
                report("NEXT cancelled");

                // A batch built for another schema is refused before anything is sent, and the same
                // batch written as a stream by other tooling is refused by the server.
                List<Row> rows = typedRows();
                try (Arena batches = Arena.ofConfined()) {
                    Batch other = Batch.build(outputSchema, rows, batches);
                    expect(failure(() -> submit(producer, other, MemorySegment.NULL)).kind == ERROR_INVALID_ARGUMENT,
                            "another schema is the host's argument");
                    report("SUBMIT invalid argument");
                    byte[] foreign = other.stream();
                    MemorySegment copy = batches.allocateFrom(ValueLayout.JAVA_BYTE, foreign);
                    MemorySegment submission = batches.allocate(U64);
                    check(call(PRODUCER_SUBMIT_IPC, producer, copy, (long) foreign.length,
                            MemorySegment.NULL, submission));
                    copy.fill(SCRIBBLE);
                    report("OUTCOME " + outcome(producer, submission.get(U64, 0)));
                }

                // The typed batch: its outcome waits for the application's acknowledgement.
                long first = submitRows(producer, inputSchema, rows);
                report("SUBMITTED first");
                Arena deliveries = Arena.ofShared();
                Delivery delivery = next(consumer, deliveries);
                report(delivery.summary());
                try (Arena scratch = Arena.ofConfined()) {
                    MemorySegment ids = scratch.allocate(8 * 4, 8);
                    MemorySegment resolved = scratch.allocate(4);
                    MemorySegment count = scratch.allocate(SIZE);
                    check(call(PRODUCER_PENDING, producer, ids, resolved, 4L, count));
                    expect(count.get(SIZE, 0) == 1 && ids.get(U64, 0) == first
                            && !resolved.get(BOOL, 0), "the only submission waits for its outcome");
                    MemorySegment expiring = cancel(scratch, EXPIRING_MILLIS);
                    MemorySegment early = scratch.allocate(POINTER);
                    expect(failure(() -> check(call(PRODUCER_REJOIN, producer, first, expiring, early))).kind
                            == ERROR_DEADLINE, "an unacknowledged submission has no outcome");
                }
                report("PENDING first unresolved");
                Batch delivered = delivery.batch(deliveries);
                List<String> printed = delivered.rows(output);
                printed.forEach(Probe::report);
                expect(Arrays.equals(delivered.stream(), delivery.read(DELIVERY_IPC)),
                        "the batch borrows the stream its delivery carried");

                // A retried attempt comes back with the same identity and a new reference; the first
                // reference is stale from then on.
                report("RETRY " + delivery.settle(DELIVERY_RETRY));
                Arena firstReference = Arena.ofShared();
                Delivery again = next(consumer, firstReference);
                expect(Arrays.equals(again.read(DELIVERY_IDENTITY), delivery.read(DELIVERY_IDENTITY)),
                        "a retry keeps the identity");
                expect(!Arrays.equals(again.read(DELIVERY_REFERENCE), delivery.read(DELIVERY_REFERENCE)),
                        "a retry makes a new reference");
                report("REDELIVERED same identity new reference");
                report("ACK " + delivery.settle(DELIVERY_ACK));

                // A reference retained here outlives the first, released on another thread, and
                // reads and settles the same attempt.
                Delivery retained = again.retain(deliveries);
                Batch retainedBatch = again.batch(deliveries);
                byte[] before = retained.read(DELIVERY_IPC);
                Thread releaser = new Thread(firstReference::close);
                releaser.start();
                releaser.join();
                expect(Arrays.equals(retained.read(DELIVERY_IPC), before), "a retained delivery keeps its stream");
                expect(retainedBatch.rows(output).equals(printed), "a retained batch reads the same rows");
                report("ACK " + retained.settle(DELIVERY_ACK));
                deliveries.close();
                report("OUTCOME " + outcome(producer, first));

                // An application rejection finishes the batch through the emitter's message error
                // policy.
                try (Arena scratch = Arena.ofShared()) {
                    long second = submit(producer, Batch.build(inputSchema, List.of(Row.plain(10, "beta")), scratch),
                            MemorySegment.NULL);
                    report("SUBMITTED second");
                    Delivery rejected = next(consumer, scratch);
                    report("REJECT " + rejected.reject("application refused"));
                    report("OUTCOME " + outcome(producer, second));
                }

                // Two outstanding batches use up the producer's credit, so a third waits for an
                // outcome.
                try (Arena scratch = Arena.ofShared()) {
                    Batch fifthBatch = Batch.build(inputSchema, List.of(Row.plain(24, "acme"), Row.plain(25, "acme")), scratch);
                    long third = submit(producer, Batch.build(inputSchema,
                            List.of(Row.plain(20, "acme"), Row.plain(21, "acme")), scratch), MemorySegment.NULL);
                    report("SUBMITTED third");
                    long fourth = submit(producer, Batch.build(inputSchema,
                            List.of(Row.plain(22, "acme"), Row.plain(23, "acme")), scratch), MemorySegment.NULL);
                    report("SUBMITTED fourth");
                    MemorySegment expiring = cancel(scratch, EXPIRING_MILLIS);
                    expect(failure(() -> submit(producer, fifthBatch, expiring)).kind == ERROR_DEADLINE,
                            "a batch beyond the credit waits");
                    report("SUBMIT deadline");
                    // A batch refused as busy is sent again after the ingestor's backoff, behind
                    // the batch submitted after it, so the two outputs may arrive in either order;
                    // each keeps its own rows.
                    Set<List<Long>> outputs = new HashSet<>();
                    for (int index = 0; index < 2; index++) {
                        Delivery outputDelivery = next(consumer, scratch);
                        outputs.add(outputDelivery.batch(scratch).ids(output));
                        report("ACK " + outputDelivery.settle(DELIVERY_ACK));
                    }
                    expect(outputs.equals(Set.of(List.of(20L, 21L), List.of(22L, 23L))),
                            "a multiple-row batch keeps its rows");
                    report("OUTCOME " + outcome(producer, third));
                    report("OUTCOME " + outcome(producer, fourth));
                    long fifth = submit(producer, fifthBatch, MemorySegment.NULL);
                    report("SUBMITTED fifth");
                    Delivery fifthOutput = next(consumer, scratch);
                    expect(fifthOutput.batch(scratch).ids(output).equals(List.of(24L, 25L)),
                            "a multiple-row batch keeps its rows");
                    report("ACK " + fifthOutput.settle(DELIVERY_ACK));
                    report("OUTCOME " + outcome(producer, fifth));
                }

                // The scenario cuts the session while one delivery is held unacknowledged.
                MemorySegment extra = handleOut(openProducer(inputFields, 1, 65536L, out), out);
                report("PRODUCER extra opened");
                try (Arena scratch = Arena.ofShared()) {
                    long held = submit(producer, Batch.build(inputSchema, List.of(Row.plain(30, "gamma")), scratch),
                            MemorySegment.NULL);
                    report("SUBMITTED held");
                    Delivery heldDelivery = next(consumer, scratch);
                    byte[] heldIdentity = heldDelivery.read(DELIVERY_IDENTITY);
                    report("HOLDING");
                    expect(failure(() -> next(consumer, scratch)).kind == ERROR_INTERRUPTED,
                            "a lost session interrupts the consumer");
                    report("NEXT interrupted");
                    expect(failure(() -> heldDelivery.settle(DELIVERY_ACK)).kind == ERROR_REJECTED,
                            "a delivery of a lost session expired");
                    report("ACK expired");
                    report("OUTCOME " + outcome(producer, held));
                    long waitedUntil = System.nanoTime() + WAIT_MILLIS * 1_000_000L;
                    while ((int) call(PRODUCER_STATE, extra) == ENDPOINT_ACTIVE) {
                        if (System.nanoTime() > waitedUntil) {
                            throw new IllegalStateException("the extra producer stayed active after its session ended");
                        }
                        Thread.sleep(10);
                    }
                    check(call(PRODUCER_CLOSE, extra, cancel(scratch, WAIT_MILLIS)));
                    expect((int) call(PRODUCER_STATE, extra) == ENDPOINT_CLOSED,
                            "a producer closed during reconnect is closed");
                    report("CLOSED extra producer");
                    report("WAITING restore");

                    // The restored consumer receives the held batch again, and the restored
                    // producer publishes.
                    Delivery redelivered = next(consumer, scratch);
                    expect(Arrays.equals(redelivered.read(DELIVERY_IDENTITY), heldIdentity),
                            "the held batch keeps its identity");
                    report("REDELIVERED held same identity");
                    report("ACK " + redelivered.settle(DELIVERY_ACK));
                    long sixth = submit(producer, Batch.build(inputSchema, List.of(Row.plain(40, "gamma")), scratch),
                            MemorySegment.NULL);
                    report("SUBMITTED sixth");
                    Delivery sixthOutput = next(consumer, scratch);
                    report("ACK " + sixthOutput.settle(DELIVERY_ACK));
                    report("OUTCOME " + outcome(producer, sixth));
                    report("STATE producer=" + STATES[(int) call(PRODUCER_STATE, producer)] + " extra="
                            + STATES[(int) call(PRODUCER_STATE, extra)] + " consumer="
                            + STATES[(int) call(CONSUMER_STATE, consumer)]);
                    MemorySegment closing = cancel(scratch, WAIT_MILLIS);
                    check(call(CONSUMER_CLOSE, consumer, closing));
                    check(call(PRODUCER_CLOSE, producer, closing));
                    expect((int) call(CONSUMER_STATE, consumer) == ENDPOINT_CLOSED
                            && (int) call(PRODUCER_STATE, producer) == ENDPOINT_CLOSED,
                            "closed handles read closed");
                    report("CLOSED completed");
                }
                if (!failures.isEmpty()) {
                    throw new IllegalStateException("the probe's checks failed: " + String.join("; ", failures));
                }
                report("CHECKS ok");
                call(PRODUCER_FREE, extra);
                call(PRODUCER_FREE, producer);
                call(CONSUMER_FREE, consumer);
                call(SCHEMA_FREE, inputSchema);
                call(SCHEMA_FREE, outputSchema);
                call(FIELDS_FREE, inputFields);
                call(FIELDS_FREE, outputFields);
            }
        }
    }

    public static void main(String[] arguments) throws Exception {
        if (arguments.length > 0 && arguments[0].equals("io")) {
            String domain = System.getenv("NERVIX_PROBE_DOMAIN");
            try (Session session = new Session(System.getenv("NERVIX_PROBE_GRPC_URI"), domain,
                    System.getenv("NERVIX_PROBE_USERNAME"), System.getenv("NERVIX_PROBE_PASSWORD"))) {
                new Endpoints(session, domain).run();
            } catch (Throwable error) {
                System.err.println("probe failed: " + error);
                error.printStackTrace();
                System.exit(1);
            }
            report("PASS");
            return;
        }
        if (arguments.length > 0 && arguments[0].equals("clock")) {
            String domain = System.getenv("NERVIX_PROBE_DOMAIN");
            try (Session session = new Session(System.getenv("NERVIX_PROBE_GRPC_URI"), domain,
                    System.getenv("NERVIX_PROBE_USERNAME"), System.getenv("NERVIX_PROBE_PASSWORD"));
                    Arena outcomes = Arena.ofConfined()) {
                runClock(session, domain, outcomes);
            } catch (Throwable error) {
                System.err.println("probe failed: " + error);
                error.printStackTrace();
                System.exit(1);
            }
            report("PASS");
            return;
        }
        String relay = System.getenv("NERVIX_PROBE_RELAY");
        String subscription = System.getenv("NERVIX_PROBE_SUBSCRIPTION");
        int expectedRows = Integer.parseInt(System.getenv("NERVIX_PROBE_ROWS"));
        try (Session session = new Session(System.getenv("NERVIX_PROBE_GRPC_URI"),
                System.getenv("NERVIX_PROBE_DOMAIN"), System.getenv("NERVIX_PROBE_USERNAME"),
                System.getenv("NERVIX_PROBE_PASSWORD"));
                Arena outcomes = Arena.ofConfined()) {
            report("OPERATION " + session.execute("SHOW CREATE RELAY " + relay + ";", MemorySegment.NULL, outcomes)
                    .disposition());

            Outcome failed = session.execute("CREATE RELAY;", MemorySegment.NULL, outcomes);
            long diagnostics = (long) call(OUTCOME_DIAGNOSTIC_COUNT, failed.handle());
            String span = "none";
            if (diagnostics > 0) {
                try (Arena scratch = Arena.ofConfined()) {
                    MemorySegment message = scratch.allocate(POINTER);
                    MemorySegment messageLen = scratch.allocate(SIZE);
                    MemorySegment hasSpan = scratch.allocate(BOOL);
                    MemorySegment start = scratch.allocate(INT);
                    MemorySegment end = scratch.allocate(INT);
                    check(call(OUTCOME_DIAGNOSTIC, failed.handle(), 0L, message, messageLen, hasSpan, start, end));
                    if (hasSpan.get(BOOL, 0)) {
                        span = Integer.toUnsignedString(start.get(INT, 0)) + ".."
                                + Integer.toUnsignedString(end.get(INT, 0));
                    }
                }
            }
            report("ERROR " + failed.disposition() + " diagnostics=" + diagnostics + " span=" + span);

            Outcome opened = session.execute(
                    "CREATE SUBSCRIPTION " + subscription + " TO " + relay + ";", MemorySegment.NULL, outcomes);
            Schema schema;
            long generation;
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment name = scratch.allocate(POINTER);
                MemorySegment nameLen = scratch.allocate(SIZE);
                MemorySegment generationOut = scratch.allocate(ValueLayout.JAVA_LONG);
                if (!(boolean) call(OUTCOME_SUBSCRIPTION, opened.handle(), name, nameLen, generationOut)) {
                    throw new IllegalStateException("the subscribe command opened no subscription");
                }
                generation = generationOut.get(ValueLayout.JAVA_LONG, 0);
                MemorySegment schemaOut = scratch.allocate(POINTER);
                check(call(OUTCOME_SCHEMA, opened.handle(), schemaOut));
                MemorySegment handle = schemaOut.get(POINTER, 0);
                MemorySegment branch = scratch.allocate(POINTER);
                MemorySegment branchLen = scratch.allocate(SIZE);
                String branchName = (boolean) call(SCHEMA_BRANCH, handle, branch, branchLen)
                        ? utf8(borrowed(branch, branchLen))
                        : null;
                schema = new Schema(fields(handle, PART_ROWS), fields(handle, PART_BRANCH_KEY), branchName);
                call(SCHEMA_FREE, handle);
            }
            for (Field field : schema.fields()) {
                report(field.line("FIELD"));
            }
            if (schema.branch() != null) {
                report("BRANCH " + schema.branch());
            }
            for (Field field : schema.keyFields()) {
                report(field.line("KEY"));
            }
            report("SUBSCRIBED");

            List<Event> retained = new ArrayList<>();
            List<List<String>> reported = new ArrayList<>();
            try (Arena tokens = Arena.ofConfined()) {
                MemorySegment deadline = cancel(tokens, 120_000L);
                int seen = 0;
                while (seen < expectedRows) {
                    Arena first = Arena.ofConfined();
                    Event event = session.nextEvent(deadline, first);
                    if (event.kind() != EVENT_ROWS) {
                        throw new IllegalStateException("the subscription reported something other than rows first");
                    }
                    if (!event.subscription().equals(subscription + "#" + generation)) {
                        throw new IllegalStateException("rows arrived for another subscription");
                    }
                    List<String> lines = event.rowLines(schema);
                    lines.forEach(Probe::report);
                    seen += lines.size();
                    reported.add(lines);
                    // Keep a second reference in a shared arena and release the first now.
                    retained.add(event.retain(Arena.ofShared()));
                    first.close();
                }
            }
            checkCollectedRelease(retained, reported, schema);
            checkRetention(retained, reported, schema);
            checkCancellation(session);
            report("CHECKS ok");
            report("CLOSED " + session.execute("DELETE SUBSCRIPTION " + subscription + ";",
                    MemorySegment.NULL, outcomes).disposition());
        } catch (Throwable error) {
            System.err.println("probe failed: " + error);
            error.printStackTrace();
            System.exit(1);
        }
        report("PASS");
    }
}
