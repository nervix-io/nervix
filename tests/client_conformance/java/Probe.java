// The Java probe of the shared Rust binding, through the Foreign Function and Memory API.
//
// It prints the same conformance report as every other probe. An event's lifetime is an arena:
// the event handle and every borrowed view of its frame belong to that arena, the binding's
// release function runs when the arena closes, and a view read after that throws instead of
// reading freed memory. Arenas the garbage collector owns release their events when they become
// unreachable. Run it with `java --enable-native-access=ALL-UNNAMED Probe.java`; with the `clock`
// argument it attaches to the domain's running clock instead, reads the clock the attach reported
// before its first tick, follows the generation a STOP and START begin and the attachment restored
// after its session ends, and detaches.

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
import java.util.HexFormat;
import java.util.List;
import java.util.OptionalLong;
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

    public static void main(String[] arguments) throws Exception {
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
