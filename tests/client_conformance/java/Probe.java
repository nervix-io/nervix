// The Java probe of the shared Rust binding, through the Foreign Function and Memory API.
//
// It prints the same conformance report as every other probe. An event's lifetime is an arena:
// the event handle and every borrowed view of its frame belong to that arena, the binding's
// release function runs when the arena closes, and a view read after that throws instead of
// reading freed memory. Arenas the garbage collector owns release their events when they become
// unreachable. Run it with `java --enable-native-access=ALL-UNNAMED Probe.java`.

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

    public static void main(String[] arguments) throws Exception {
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
