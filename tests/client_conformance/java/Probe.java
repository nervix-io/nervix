// The Java 21 probe of the Nervix JNI library. Every blocking call uses the shared Rust client.
// Direct frame buffers retain their native event through slice/duplicate chains.
import io.nervix.client.Nervix;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public final class Probe {
    private Probe() {}
    static final int CELL_VALUE = 1, CELL_NULL = 2, CELL_REDACTED = 3;
    static final int CLOCK_PACED = 4;
    static final long WAIT_MILLIS = 120_000L;
    static final long EXPIRING_MILLIS = 200L;
    static final int PRODUCER_BATCHES = 2, CONSUMER_BATCHES = 4;
    static final long ENDPOINT_BYTES = 1_048_576L;
    static final byte SCRIBBLE = (byte) 0xaa;
    static String lower(Enum<?> value) { return value.name().toLowerCase(Locale.ROOT); }
    static String utf8(byte[] value) { return new String(value, StandardCharsets.UTF_8); }
    static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }
    static void report(String line) { System.out.println(line); System.out.flush(); }

    /** Only a test's explicit cleanup group; every library handle independently owns its Cleaner. */
    static final class Scope implements AutoCloseable {
        final List<AutoCloseable> handles = new ArrayList<>();
        synchronized <T extends AutoCloseable> T add(T handle) { handles.add(handle); return handle; }
        @Override public synchronized void close() {
            for (AutoCloseable handle : handles.reversed()) {
                try { handle.close(); } catch (Exception failure) { throw new IllegalStateException(failure); }
            }
            handles.clear();
        }
    }
    static Nervix.Cancel cancel(Scope scope, Long millis) {
        return scope.add(millis == null ? new Nervix.Cancel() : Nervix.Cancel.deadline(millis / 1000.0));
    }
    static Nervix.ClientException failure(Runnable call) {
        try { call.run(); } catch (Nervix.ClientException failure) { return failure; }
        throw new IllegalStateException("a call succeeded where it had to fail");
    }
    record Field(String name, String type, boolean nullable, boolean sensitive) {
        String line(String prefix) {
            return prefix + " " + name + " " + type + " " + (nullable ? "nullable" : "required") + " "
                + (sensitive ? "sensitive" : "public");
        }
    }
    record Schema(List<Field> fields, List<Field> keyFields, String branch) {}
    static List<Field> fields(Nervix.Schema schema, Nervix.Part part) {
        return schema.fields(part).stream().map(f -> new Field(f.name(), f.type().name(), f.nullable(), f.sensitive())).toList();
    }
    record Outcome(Nervix.Outcome handle) {
        String disposition() { return lower(handle.disposition()); }
    }
    static final class Session implements AutoCloseable {
        final Nervix.Session handle;
        Session(String server, String domain, String username, String password) {
            try (var options = new Nervix.ConnectOptions().auth(username, password)) {
                handle = Nervix.Session.connect(server, domain, options, null);
            }
        }
        Outcome execute(String query, Nervix.Cancel cancel, Scope scope) {
            // Prepare before passing an already-triggered token to execute: this verifies that
            // cancellation carries the exact prepared execution reference.
            try (var prepared = handle.prepare(query, null)) {
                return new Outcome(scope.add(handle.execute(prepared, cancel)));
            }
        }
        Event nextEvent(Nervix.Cancel cancel, Scope scope) { return new Event(handle.nextEvent(cancel), scope); }
        ClockEvent nextClockEvent(Nervix.Cancel cancel, Scope scope, String domain) {
            ClockEvent event = new ClockEvent(handle.nextClockEvent(cancel), scope);
            require(event.domain().equals(domain), "a clock event arrived for another domain"); return event;
        }
        DomainClock domainClock(String domain, Scope scope) {
            Nervix.DomainClock clock = handle.domainClock(domain);
            return clock == null ? null : new DomainClock(clock, scope);
        }
        @Override public void close() { handle.close(); }
    }
    static List<String> scalars(String kind, byte[] bytes, int count) {
        ByteBuffer data = ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder());
        List<String> values = new ArrayList<>();
        for (int index = 0; index < count; index++) values.add(switch (kind) {
            case "U8" -> "u8:" + Byte.toUnsignedInt(data.get());
            case "I8" -> "i8:" + data.get();
            case "U16" -> "u16:" + Short.toUnsignedInt(data.getShort());
            case "I16" -> "i16:" + data.getShort();
            case "U32" -> "u32:" + Integer.toUnsignedString(data.getInt());
            case "I32" -> "i32:" + data.getInt();
            case "U64" -> "u64:" + Long.toUnsignedString(data.getLong());
            case "I64" -> "i64:" + data.getLong();
            case "F32" -> String.format("f32:%08x", data.getInt());
            case "F64" -> String.format("f64:%016x", data.getLong());
            case "DATETIME" -> "datetime:" + data.getLong();
            case "BOOL" -> "bool:" + (data.get() != 0);
            default -> throw new IllegalStateException("a leaf is not a scalar");
        });
        return values;
    }
    static List<String> variable(String kind, Nervix.Varlen data) {
        List<String> values = new ArrayList<>();
        for (int index = 0; index + 1 < data.offsets().length; index++) {
            byte[] value = Arrays.copyOfRange(data.data(), Math.toIntExact(data.offsets()[index]), Math.toIntExact(data.offsets()[index + 1]));
            values.add((kind.equals("STRING") ? "str:" : "bytes:") + HexFormat.of().formatHex(value));
        }
        return values;
    }
    static final class Event {
        final Nervix.Event handle;
        final Scope scope;
        Event(Nervix.Event handle, Scope scope) { this.handle = scope.add(handle); this.scope = scope; }
        ByteBuffer frame() { return handle.frame(); }
        List<String> column(Nervix.Part part, int index, Field field, int cells) {
            byte[] states = handle.states(part, index);
            List<String> rendered;
            if (field.type().equals("STRING") || field.type().equals("BYTES")) {
                Nervix.Varlen copied = handle.varlen(part, index);
                rendered = variable(field.type(), copied);
                for (int row = 0; row < cells; row++) if (states[row] == CELL_VALUE) {
                    byte[] value = Arrays.copyOfRange(copied.data(), Math.toIntExact(copied.offsets()[row]), Math.toIntExact(copied.offsets()[row + 1]));
                    require(Arrays.equals(handle.cell(part, row, index), value), "a cell differs from its bulk copy");
                }
            } else rendered = scalars(field.type(), handle.fixed(part, index), cells);
            for (int row = 0; row < cells; row++) {
                switch (states[row]) {
                    case CELL_VALUE -> {}
                    case CELL_NULL -> rendered.set(row, "null");
                    case CELL_REDACTED -> rendered.set(row, "redacted");
                    default -> throw new IllegalStateException("unknown cell state");
                }
            }
            return rendered;
        }
        List<String> render(Nervix.Part part, List<Field> fields, int count) {
            List<StringBuilder> rows = new ArrayList<>();
            for (int row = 0; row < count; row++) rows.add(new StringBuilder());
            for (int index = 0; index < fields.size(); index++) {
                List<String> column = column(part, index, fields.get(index), count);
                for (int row = 0; row < count; row++) {
                    if (!rows.get(row).isEmpty()) rows.get(row).append(' ');
                    rows.get(row).append(fields.get(index).name()).append('=').append(column.get(row));
                }
            }
            return rows.stream().map(StringBuilder::toString).toList();
        }
        List<String> rowLines(Schema schema) {
            String key = schema.keyFields().isEmpty() ? "" : render(Nervix.Part.BRANCH, schema.keyFields(), 1).getFirst();
            return render(Nervix.Part.ROWS, schema.fields(), Math.toIntExact(handle.rows())).stream()
                .map(row -> "ROW [" + key + "] " + row).toList();
        }
    }
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

    record Tick(long generation, long id, long boundary, long servingLogical) {}
    static PacedClock paced(Nervix.PacedClock clock) {
        return new PacedClock(clock.generation(), clock.periodNanos(), clock.skewNanos(), clock.logicalOrigin(), clock.utcAnchor(), clock.rate());
    }
    static final class ClockEvent {
        final Nervix.ClockEvent handle;
        final Scope scope;
        ClockEvent(Nervix.ClockEvent handle, Scope scope) { this.handle = scope.add(handle); this.scope = scope; }
        String kind() { return handle.kind().name(); }
        String domain() { return handle.domain(); }
        long generation() { return handle.generation(); }
        int state() { return handle.state().code(); }
        PacedClock paced() { return Probe.paced(handle.paced()); }
        Tick tick() { Nervix.Tick t = handle.tick(); return new Tick(t.generation(), t.id(), t.logicalBoundary(), t.servingLogical()); }
    }
    static final class DomainClock {
        final Nervix.DomainClock handle;
        final Scope scope;
        DomainClock(Nervix.DomainClock handle, Scope scope) { this.handle = scope.add(handle); this.scope = scope; }
        long generation() { return handle.generation(); }
        int state() { return handle.state().code(); }
        PacedClock paced() { return Probe.paced(handle.paced()); }
        OptionalLong tickId() { Nervix.Tick tick = handle.tick(); return tick == null ? OptionalLong.empty() : OptionalLong.of(tick.id()); }
        long logicalTimeAt(long utc) { return handle.logicalTimeAt(utc); }
        long wallDurationUntil(long utc, long target) { return handle.wallNanosUntil(utc, target); }
        long[] admissionWindow(long utc) {
            Nervix.AdmissionWindow window = handle.admissionWindow(utc);
            return window == null ? null : new long[] {window.earliestCenter(), window.latestCenter()};
        }
        boolean admits(long utc, long event) { return handle.admits(utc, event); }
    }
    static void checkWaitCancellation(Session session, String domain, boolean clock) throws InterruptedException {
        try (var token = new Nervix.Cancel()) {
            var observed = new AtomicReference<Nervix.ClientException>();
            Runnable read = () -> {
                try (var events = new Scope()) {
                    if (clock) session.nextClockEvent(token, events, domain); else session.nextEvent(token, events);
                }
            };
            Thread waiter = new Thread(() -> observed.set(failure(read)));
            waiter.start(); Thread.sleep(100); token.trigger(); waiter.join(30_000);
            require(!waiter.isAlive() && observed.get() instanceof Nervix.CancelledException, "cancelled wait did not report cancellation");
        }
        try (var deadline = Nervix.Cancel.deadline(0.05); var events = new Scope()) {
            require(failure(() -> { if (clock) session.nextClockEvent(deadline, events, domain); else session.nextEvent(deadline, events); })
                instanceof Nervix.DeadlineException, "expired wait did not report deadline");
        }
    }
    static void checkClockCancellation(Session session, String domain) throws InterruptedException { checkWaitCancellation(session, domain, true); }
    static void checkCancellation(Session session) throws InterruptedException {
        checkWaitCancellation(session, null, false);
        try (var token = new Nervix.Cancel(); var outcomes = new Scope()) {
            token.trigger();
            var command = failure(() -> session.execute("SHOW DOMAINS;", token, outcomes));
            require(command instanceof Nervix.CancelledException && command.executionReference() != null && !command.executionReference().isEmpty(),
                "cancelled command did not carry its execution reference");
        }
    }
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
        ClockEvent next(Nervix.Cancel deadline) {
            ClockEvent event = session.nextClockEvent(deadline, new Scope(), domain);
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
            try (Scope reads = new Scope()) {
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
            try (Scope reads = new Scope()) {
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

        DomainClock read(Scope into) {
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
        ClockEvent firstTick(Nervix.Cancel deadline) {
            long followed = generation;
            while (true) {
                ClockEvent event = next(deadline);
                String kind = event.kind();
                if (kind.equals("TICK")) {
                    return event;
                }
                event.scope.close();
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
        ClockEvent nextGeneration(Nervix.Cancel deadline) {
            long previous = generation;
            while (true) {
                ClockEvent event = next(deadline);
                String kind = event.kind();
                if (kind.equals("STATE") && Long.compareUnsigned(generation, previous) > 0 && paced != null) {
                    return event;
                }
                event.scope.close();
                if (!kind.equals("TICK") && !kind.equals("STATE")) {
                    throw new IllegalStateException("the clock reported " + kind
                            + " before a generation after the followed one");
                }
            }
        }

        /** Waits for the interruption; the followed generation's ticks and states are taken on the way. */
        void interruption(Nervix.Cancel deadline) {
            long followed = generation;
            while (true) {
                ClockEvent event = next(deadline);
                String kind = event.kind();
                event.scope.close();
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
        ClockEvent restored(Nervix.Cancel deadline) {
            while (true) {
                ClockEvent event = next(deadline);
                if (event.kind().equals("STATE") && paced != null) {
                    return event;
                }
                event.scope.close();
            }
        }

        /** Reports the paced state a STATE event reports and the first tick after it. */
        void reportStateAndFirstTick(ClockEvent state, Nervix.Cancel deadline) {
            report(state.paced().line("STATE", domain));
            state.scope.close();
            ClockEvent tick = firstTick(deadline);
            report(clock().tickLine(tick.tick(), domain));
            tick.scope.close();
        }
    }

    /**
     * Attaches to the domain's running clock and reads the clock the attach reported before its
     * first tick, then follows the generation the scenario's STOP and START begin and the attachment
     * restored after the scenario ends the session, and detaches.
     */
    static void runClock(Session session, String domain, Scope outcomes) throws InterruptedException {
        checkClockCancellation(session, domain);
        report("ATTACHED " + session.execute("ATTACH DOMAIN CLOCK;", null, outcomes).disposition());
        try (Scope tokens = new Scope()) {
            // The clock the attach reported, read before any event about the attachment.
            DomainClock read = session.domainClock(domain, new Scope());
            if (read == null) {
                throw new IllegalStateException("the session follows no clock after its attach completed");
            }
            if (read.state() != CLOCK_PACED) {
                throw new IllegalStateException("the attach reported a clock other than the running paced one");
            }
            Nervix.DomainClock retainedRead = session.handle.domainClock(domain);
            require(retainedRead != null, "the attached clock has an independent snapshot");
            PacedClock clock = read.paced();
            String reportedClock = clock.line("CLOCK", domain);
            report(reportedClock);
            report(clock.projectionLine(read, domain));

            FollowedClock followed = new FollowedClock(session, domain, clock);
            ClockEvent tick = followed.firstTick(cancel(tokens, 120_000L));
            Tick retainedTick = tick.tick();
            String reportedTick = clock.tickLine(retainedTick, domain);
            report(reportedTick);

            // A clock snapshot and a Java tick value outlive the handles they were read beside.
            try (Nervix.DomainClock snapshot = retainedRead) {
                Thread closer = new Thread(() -> {
                    read.scope.close();
                    tick.scope.close();
                });
                closer.start();
                closer.join();
                PacedClock clockAgain = paced(snapshot.paced());
                if (!clockAgain.line("CLOCK", domain).equals(reportedClock)
                        || !clockAgain.tickLine(retainedTick, domain).equals(reportedTick)) {
                    throw new IllegalStateException("a retained clock or tick reads differently than it did");
                }
            }

            // The scenario stops the domain and starts it again at another origin and rate.
            Nervix.Cancel started = cancel(tokens, 120_000L);
            followed.reportStateAndFirstTick(followed.nextGeneration(started), started);

            // The scenario ends the session, and the binding attaches the clock again on the next one.
            Nervix.Cancel restoring = cancel(tokens, 120_000L);
            followed.interruption(restoring);
            report("INTERRUPTED domain=" + domain);
            followed.reportStateAndFirstTick(followed.restored(restoring), restoring);
        }
        report("DETACHED " + session.execute("DETACH DOMAIN CLOCK;", null, outcomes).disposition());
        report("CHECKS ok");
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

    static Nervix.Fields fieldsHandle(List<FieldSpec> specs) {
        var fields = new Nervix.Fields();
        try {
            for (FieldSpec spec : specs) {
                Level first = spec.levels().getFirst();
                fields.add(spec.name(), Nervix.FieldType.valueOf(first.type()), first.length(), spec.nullable(), spec.sensitive());
                for (Level level : spec.levels().subList(1, spec.levels().size())) fields.element(Nervix.FieldType.valueOf(level.type()), level.length());
            }
            return fields;
        } catch (Throwable failure) { fields.close(); throw failure; }
    }
    static List<Reported> reported(Nervix.Schema schema) {
        return schema.fields(Nervix.Part.ROWS).stream().map(f -> {
            List<Level> levels = f.levels().stream().map(l -> new Level(l.type().name(), Math.toIntExact(l.length()))).toList();
            require(levels.getFirst().type().equals(f.type().name()), "a field type differs from its first level");
            return new Reported(f.name(), levels, f.nullable(), f.sensitive());
        }).toList();
    }
    record Shape(long[] offsets, int length) {}
    record Batch(Nervix.Batch handle) implements AutoCloseable {
        static Batch build(Nervix.Schema schema, List<Row> rows) {
            try (var builder = new Nervix.BatchBuilder(schema, rows.size())) {
                List<Reported> fields = reported(schema);
                for (int index = 0; index < fields.size(); index++) {
                    HostColumn column = hostColumn(fields.get(index).name(), rows);
                    if (column.states != null) { builder.states(index, column.states); Arrays.fill(column.states, SCRIBBLE); }
                    if (column.listOffsets != null) { builder.offsets(index, 0, column.listOffsets); Arrays.fill(column.listOffsets, -1); }
                    if (column.varlen) {
                        builder.varlen(index, Math.toIntExact(column.leaf), column.valueOffsets, column.values);
                        Arrays.fill(column.valueOffsets, -1);
                    } else builder.fixed(index, Math.toIntExact(column.leaf), column.values);
                    Arrays.fill(column.values, SCRIBBLE);
                }
                return new Batch(builder.finish());
            }
        }
        byte[] stream() { return handle.ipc(); }
        List<String> column(int index, Reported field) {
            int rows = handle.cells(index, 0), inner = field.levels().size() - 1;
            byte[] states = handle.states(index);
            List<Shape> lists = new ArrayList<>();
            for (int level = 0; level < inner; level++) {
                Level shape = field.levels().get(level);
                lists.add(shape.type().equals("LIST") ? new Shape(handle.offsets(index, level), 0) : new Shape(null, shape.length()));
            }
            String kind = field.levels().get(inner).type();
            List<String> leaves = kind.equals("STRING") || kind.equals("BYTES") ? variable(kind, handle.varlen(index, inner))
                : scalars(kind, handle.fixed(index, inner), handle.cells(index, inner));
            List<String> values = new ArrayList<>();
            for (int row = 0; row < rows; row++) values.add(states[row] == CELL_NULL ? "null" : render(lists, leaves, 0, row));
            return values;
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
            long count = handle.rows();
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
        @Override public void close() { handle.close(); }
    }

    static String deliverySummary(Nervix.Delivery delivery) {
        byte[] branch = delivery.branchFingerprint();
        return "DELIVERY relay=" + delivery.sourceRelay() + " members=" + delivery.members()
            + " branch=" + (branch == null ? "none" : Integer.toString(branch.length))
            + " identity=" + delivery.identity().length + " reference=" + delivery.reference().length;
    }
    static Nervix.Delivery next(Nervix.Consumer consumer, Scope scope) {
        try (var deadline = Nervix.Cancel.deadline(WAIT_MILLIS / 1000.0)) { return scope.add(consumer.next(deadline)); }
    }
    static String ack(Nervix.Delivery delivery) {
        try (var deadline = Nervix.Cancel.deadline(WAIT_MILLIS / 1000.0)) { return lower(delivery.ack(deadline)); }
    }
    static String outcome(Nervix.Producer producer, long id) {
        try (var deadline = Nervix.Cancel.deadline(WAIT_MILLIS / 1000.0); var result = producer.rejoin(id, deadline)) {
            Nervix.SubmissionInfo info = result.info();
            return switch (info.result()) {
                case COMPLETED -> "completed";
                case NOT_ADMITTED -> "not_admitted " + lower(info.refusal()) + (info.defect() == null ? "" : " " + lower(info.defect()));
                case PROCESSING_FAILED -> "processing_failed " + lower(info.failure());
                case OUTCOME_UNKNOWN -> "outcome_unknown " + lower(info.uncertainty());
            };
        }
    }
    static long submitRows(Nervix.Producer producer, Nervix.Schema schema, List<Row> rows) {
        try (var batch = Batch.build(schema, rows)) { return producer.submit(batch.handle(), null); }
    }
    static String policy(Nervix.EndpointDescription d) {
        return "window=" + lower(d.window()) + "/" + Long.toUnsignedString(d.outstanding())
            + " ack_timeout=" + Long.toUnsignedString(d.ackTimeoutNanos());
    }
    static final class Endpoints {
        final Session session;
        final String domain;
        final String ingestor = System.getenv("NERVIX_PROBE_INGESTOR");
        final String emitter = System.getenv("NERVIX_PROBE_EMITTER");
        Endpoints(Session session, String domain) { this.session = session; this.domain = domain; }
        Nervix.Producer openProducer(Nervix.Fields fields, int batches, long bytes) {
            return session.handle.openProducer(domain, ingestor, fields, batches, bytes, null);
        }
        Nervix.Consumer openConsumer(Nervix.Fields fields) {
            return session.handle.openConsumer(domain, emitter, fields, CONSUMER_BATCHES, ENDPOINT_BYTES, null);
        }
        void refused(Runnable call, String endpoint) {
            Nervix.ClientException failure = failure(call);
            require(failure instanceof Nervix.RejectedException && failure.openRefusal() == Nervix.OpenRefusal.SCHEMA_MISMATCH,
                "an open with another schema was not refused exactly");
            report("REFUSED " + endpoint + " schema mismatch");
        }
        void run() throws InterruptedException {
            List<FieldSpec> outputSpecs = new ArrayList<>(inputFields());
            outputSpecs.add(scalar("echo", "U32", false, false));
            List<FieldSpec> mismatchSpecs = inputFields().stream().map(f -> f.name().equals("secret")
                ? new FieldSpec(f.name(), f.levels(), f.nullable(), false) : f).toList();
            try (var inputFields = fieldsHandle(inputFields()); var outputFields = fieldsHandle(outputSpecs);
                    var mismatch = fieldsHandle(mismatchSpecs); var handles = new Scope()) {
                refused(() -> handles.add(openProducer(mismatch, PRODUCER_BATCHES, ENDPOINT_BYTES)), "producer");
                refused(() -> handles.add(openConsumer(inputFields)), "consumer");
                var consumer = handles.add(openConsumer(outputFields));
                var producer = handles.add(openProducer(inputFields, PRODUCER_BATCHES, ENDPOINT_BYTES));
                var outputSchema = handles.add(consumer.schema());
                var inputSchema = handles.add(producer.schema());
                List<Reported> output = reported(outputSchema), input = reported(inputSchema);
                var cd = consumer.description();
                report("CONSUMER generation=" + Long.toUnsignedString(cd.generation()) + " state=" + lower(consumer.state())
                    + " " + policy(cd) + " credit=" + cd.batches() + "/" + cd.bytes() + " max=" + cd.maxBatchRows() + "/" + cd.maxBatchBytes());
                output.forEach(f -> report(f.line("OUTPUT FIELD")));
                var pd = producer.description();
                require(pd.maxBatchRows() > 0 && pd.maxBatchBytes() > 0 && pd.maxBatchBytes() <= pd.bytes(), "the producer batch limits fit its grant");
                report("PRODUCER generation=" + Long.toUnsignedString(pd.generation()) + " state=" + lower(producer.state())
                    + " admission=" + lower(producer.admission()) + " " + policy(pd) + " credit=" + pd.batches() + "/" + pd.bytes());
                input.forEach(f -> report(f.line("INPUT FIELD")));
                report("OPENED");
                try (var deadline = Nervix.Cancel.deadline(EXPIRING_MILLIS / 1000.0)) {
                    require(failure(() -> handles.add(consumer.next(deadline))) instanceof Nervix.DeadlineException, "an expired read reports its deadline");
                }
                report("NEXT deadline");
                try (var token = new Nervix.Cancel()) {
                    var observed = new AtomicReference<Nervix.ClientException>();
                    Thread reader = new Thread(() -> observed.set(failure(() -> handles.add(consumer.next(token)))));
                    reader.start(); Thread.sleep(100); token.trigger(); reader.join(30_000);
                    require(!reader.isAlive() && observed.get() instanceof Nervix.CancelledException, "a cancelled read reports cancellation");
                }
                report("NEXT cancelled");
                List<Row> rows = typedRows();
                try (var other = Batch.build(outputSchema, rows)) {
                    require(failure(() -> producer.submit(other.handle(), null)) instanceof Nervix.InvalidArgumentException, "another schema is the host argument");
                    report("SUBMIT invalid argument");
                    byte[] foreign = other.stream();
                    long id = producer.submitIpc(foreign, null); Arrays.fill(foreign, SCRIBBLE);
                    report("OUTCOME " + outcome(producer, id));
                }
                long first = submitRows(producer, inputSchema, rows);
                report("SUBMITTED first");
                try (var deliveries = new Scope()) {
                    var delivery = next(consumer, deliveries); report(deliverySummary(delivery));
                    List<Nervix.Pending> pending = producer.pending();
                    require(pending.size() == 1 && pending.getFirst().id() == first && pending.getFirst().outcome() == null, "the only submission waits for its outcome");
                    try (var deadline = Nervix.Cancel.deadline(EXPIRING_MILLIS / 1000.0)) {
                        require(failure(() -> handles.add(producer.rejoin(first, deadline))) instanceof Nervix.DeadlineException, "an unacknowledged submission has no outcome");
                    }
                    report("PENDING first unresolved");
                    var delivered = deliveries.add(new Batch(delivery.batch()));
                    List<String> printed = delivered.rows(output); printed.forEach(Probe::report);
                    require(Arrays.equals(delivered.stream(), delivery.ipc()), "the batch reads its delivery stream");
                    try (var deadline = Nervix.Cancel.deadline(WAIT_MILLIS / 1000.0)) { report("RETRY " + lower(delivery.retry(deadline))); }
                    var firstReference = new java.util.concurrent.atomic.AtomicReference<>(next(consumer, deliveries));
                    var again = firstReference.get();
                    require(Arrays.equals(again.identity(), delivery.identity()), "a retry keeps its identity");
                    require(!Arrays.equals(again.reference(), delivery.reference()), "a retry changes its reference");
                    report("REDELIVERED same identity new reference"); report("ACK " + ack(delivery));
                    var retained = again;
                    var retainedBatch = deliveries.add(new Batch(again.batch()));
                    byte[] before = retained.ipc();
                    Thread releaser = new Thread(() -> firstReference.set(null)); releaser.start(); releaser.join();
                    require(Arrays.equals(retained.ipc(), before), "a retained delivery keeps its stream");
                    require(retainedBatch.rows(output).equals(printed), "a retained batch reads the same rows");
                    report("ACK " + ack(retained));
                }
                report("OUTCOME " + outcome(producer, first));
                try (var scratch = new Scope()) {
                    long second = submitRows(producer, inputSchema, List.of(Row.plain(10, "beta"))); report("SUBMITTED second");
                    var rejected = next(consumer, scratch);
                    try (var deadline = Nervix.Cancel.deadline(WAIT_MILLIS / 1000.0)) { report("REJECT " + lower(rejected.reject("application refused", deadline))); }
                    report("OUTCOME " + outcome(producer, second));
                }
                try (var scratch = new Scope(); var fifthBatch = Batch.build(inputSchema, List.of(Row.plain(24, "acme"), Row.plain(25, "acme")))) {
                    long third = submitRows(producer, inputSchema, List.of(Row.plain(20, "acme"), Row.plain(21, "acme"))); report("SUBMITTED third");
                    long fourth = submitRows(producer, inputSchema, List.of(Row.plain(22, "acme"), Row.plain(23, "acme"))); report("SUBMITTED fourth");
                    try (var deadline = Nervix.Cancel.deadline(EXPIRING_MILLIS / 1000.0)) {
                        require(failure(() -> producer.submit(fifthBatch.handle(), deadline)) instanceof Nervix.DeadlineException, "a batch beyond credit waits");
                    }
                    report("SUBMIT deadline");
                    Set<List<Long>> outputs = new HashSet<>();
                    for (int index = 0; index < 2; index++) {
                        var delivery = next(consumer, scratch);
                        try (var batch = new Batch(delivery.batch())) { outputs.add(batch.ids(output)); }
                        report("ACK " + ack(delivery));
                    }
                    require(outputs.equals(Set.of(List.of(20L, 21L), List.of(22L, 23L))), "multiple-row batches keep their rows");
                    report("OUTCOME " + outcome(producer, third)); report("OUTCOME " + outcome(producer, fourth));
                    long fifth = producer.submit(fifthBatch.handle(), null); report("SUBMITTED fifth");
                    var fifthOutput = next(consumer, scratch);
                    try (var batch = new Batch(fifthOutput.batch())) { require(batch.ids(output).equals(List.of(24L, 25L)), "a multiple-row batch keeps its rows"); }
                    report("ACK " + ack(fifthOutput)); report("OUTCOME " + outcome(producer, fifth));
                }
                var extra = handles.add(openProducer(inputFields, 1, 65536)); report("PRODUCER extra opened");
                try (var scratch = new Scope()) {
                    long held = submitRows(producer, inputSchema, List.of(Row.plain(30, "gamma"))); report("SUBMITTED held");
                    var heldDelivery = next(consumer, scratch); byte[] identity = heldDelivery.identity(); report("HOLDING");
                    require(failure(() -> next(consumer, scratch)) instanceof Nervix.ConsumerInterruptedException, "a lost session interrupts the consumer");
                    report("NEXT interrupted");
                    require(failure(() -> ack(heldDelivery)) instanceof Nervix.RejectedException, "a lost-session delivery reference expired");
                    report("ACK expired"); report("OUTCOME " + outcome(producer, held));
                    long until = System.nanoTime() + WAIT_MILLIS * 1_000_000;
                    while (extra.state() == Nervix.EndpointState.ACTIVE) {
                        require(System.nanoTime() <= until, "the extra producer stayed active after the session ended"); Thread.sleep(10);
                    }
                    try (var deadline = Nervix.Cancel.deadline(WAIT_MILLIS / 1000.0)) { extra.end(deadline); }
                    require(extra.state() == Nervix.EndpointState.CLOSED, "a producer closed during reconnect is closed");
                    report("CLOSED extra producer"); report("WAITING restore");
                    var redelivered = next(consumer, scratch);
                    require(Arrays.equals(redelivered.identity(), identity), "the held batch keeps its identity");
                    report("REDELIVERED held same identity"); report("ACK " + ack(redelivered));
                    long sixth = submitRows(producer, inputSchema, List.of(Row.plain(40, "gamma"))); report("SUBMITTED sixth");
                    report("ACK " + ack(next(consumer, scratch))); report("OUTCOME " + outcome(producer, sixth));
                    report("STATE producer=" + lower(producer.state()) + " extra=" + lower(extra.state()) + " consumer=" + lower(consumer.state()));
                    try (var deadline = Nervix.Cancel.deadline(WAIT_MILLIS / 1000.0)) { consumer.end(deadline); producer.end(deadline); }
                    require(consumer.state() == Nervix.EndpointState.CLOSED && producer.state() == Nervix.EndpointState.CLOSED, "closed endpoints read closed");
                    report("CLOSED completed");
                }
                report("CHECKS ok");
            }
        }
    }
    record Cleanup(Object state, java.lang.reflect.Field pointer, WeakReference<Nervix.Handle> owner) {
        boolean released() {
            synchronized (state) {
                try { return owner.get() == null && pointer.getLong(state) == 0; }
                catch (IllegalAccessException failure) { throw new IllegalStateException(failure); }
            }
        }
    }
    static Cleanup cleanup(Nervix.Handle handle) {
        try {
            var field = Nervix.Handle.class.getDeclaredField("state"); field.setAccessible(true);
            Object state = field.get(handle);
            var pointer = state.getClass().getDeclaredField("pointer"); pointer.setAccessible(true);
            return new Cleanup(state, pointer, new WeakReference<>(handle));
        } catch (ReflectiveOperationException failure) { throw new IllegalStateException(failure); }
    }
    static Cleanup discardSchema(Event event, Schema expected) {
        Nervix.Schema automatic = event.handle.schema();
        require(fields(automatic, Nervix.Part.ROWS).equals(expected.fields())
                && fields(automatic, Nervix.Part.BRANCH).equals(expected.keyFields())
                && java.util.Objects.equals(automatic.branch(), expected.branch()), "a schema handle keeps its complete description");
        return cleanup(automatic);
    }
    static void checkCollectedRelease(List<Event> retained, List<List<String>> printed, Schema schema) throws InterruptedException {
        List<Cleanup> collected = new ArrayList<>();
        for (Event event : retained) collected.add(discardSchema(event, schema));
        for (int attempt = 0; attempt < 100 && collected.stream().anyMatch(ref -> !ref.released()); attempt++) {
            System.gc(); Thread.sleep(10); byte[][] churn = new byte[256][];
            for (int index = 0; index < churn.length; index++) churn[index] = new byte[64 * 1024];
        }
        require(collected.stream().allMatch(Cleanup::released), "Cleaner did not release an unreachable native schema after its session");
        for (int index = 0; index < retained.size(); index++) require(retained.get(index).rowLines(schema).equals(printed.get(index)), "collecting one reference disturbed another");
    }
    static void checkRetention(List<Event> retained, List<List<String>> printed, Schema schema) throws InterruptedException {
        List<ByteBuffer> slices = new ArrayList<>(); List<byte[]> copies = new ArrayList<>();
        for (int index = 0; index < retained.size(); index++) {
            Event event = retained.get(index); ByteBuffer view = event.frame();
            require(view.isDirect() && view.isReadOnly(), "a frame must be a read-only direct buffer");
            require(view.get(4) == 'N' && view.get(5) == 'X' && view.get(6) == 'E' && view.get(7) == 'L', "a retained frame is not a ServerMessage frame");
            require(event.rowLines(schema).equals(printed.get(index)), "a retained event reads differently");
            byte[] copy = new byte[view.remaining()]; view.duplicate().get(copy); copies.add(copy);
            slices.add(view.asReadOnlyBuffer().slice().duplicate());
        }
        Thread closer = new Thread(() -> retained.forEach(event -> event.scope.close())); closer.start(); closer.join();
        for (Event event : retained) require(failure(event.handle::rows) instanceof Nervix.ClosedException, "an explicitly closed handle remains closed");
        retained.clear(); System.gc();
        for (int index = 0; index < slices.size(); index++) {
            byte[] after = new byte[slices.get(index).remaining()]; slices.get(index).get(after);
            require(Arrays.equals(after, copies.get(index)), "a slice keeps its native frame alive after event release");
        }
    }
    /**
     * Opens a subscription, then reports how the session that replaces the lost one answers its
     * reopening: a refusal for now with its cause, message and wait, or the end of the subscription
     * with its reason.
     */
    static void runRestoration(Session session, Scope outcomes) {
        String relay = System.getenv("NERVIX_PROBE_RELAY"), subscription = System.getenv("NERVIX_PROBE_SUBSCRIPTION");
        Outcome opened = session.execute("CREATE SUBSCRIPTION " + subscription + " TO " + relay + ";", null, outcomes);
        require(opened.disposition().equals("completed"), "the subscription was not opened");
        report("SUBSCRIBED");
        try (var deadline = Nervix.Cancel.deadline(120)) {
            Event interrupted = session.nextEvent(deadline, outcomes);
            require(interrupted.handle.kind() == Nervix.EventKind.INTERRUPTED, "the lost session did not interrupt its subscription");
            report("INTERRUPTED");
            Event answer = session.nextEvent(deadline, outcomes);
            switch (answer.handle.kind()) {
                case ENDED -> report("ENDED reason=" + lower(answer.handle.endReason()));
                case RESTORATION_FAILED -> {
                    Nervix.RestorationFailure failure = answer.handle.restorationFailure();
                    require(failure.waitNanos() > 0, "the restoration retry wait was zero");
                    report("RESTORATION cause=" + lower(failure.cause()) + " message=" + failure.message() + " wait=positive");
                }
                default -> throw new IllegalStateException("the reopening was neither refused nor ended");
            }
        }
        report("CHECKS ok");
    }
    static void runRows(Session session, Scope outcomes) throws InterruptedException {
        String relay = System.getenv("NERVIX_PROBE_RELAY"), subscription = System.getenv("NERVIX_PROBE_SUBSCRIPTION");
        int expected = Integer.parseInt(System.getenv("NERVIX_PROBE_ROWS"));
        report("OPERATION " + session.execute("SHOW CREATE RELAY " + relay + ";", null, outcomes).disposition());
        Outcome failed = session.execute("CREATE RELAY;", null, outcomes);
        var diagnostics = failed.handle().diagnostics(); String span = "none";
        if (!diagnostics.isEmpty() && diagnostics.getFirst().span() != null) {
            long[] range = diagnostics.getFirst().span(); span = range[0] + ".." + range[1];
        }
        report("ERROR " + failed.disposition() + " diagnostics=" + diagnostics.size() + " span=" + span);
        Outcome opened = session.execute("CREATE SUBSCRIPTION " + subscription + " TO " + relay + ";", null, outcomes);
        Nervix.Subscription identity = opened.handle().subscription();
        require(identity != null, "the subscribe command opened no subscription");
        Schema schema;
        try (var nativeSchema = opened.handle().schema()) {
            schema = new Schema(fields(nativeSchema, Nervix.Part.ROWS), fields(nativeSchema, Nervix.Part.BRANCH), nativeSchema.branch());
        }
        schema.fields().forEach(f -> report(f.line("FIELD")));
        if (schema.branch() != null) report("BRANCH " + schema.branch());
        schema.keyFields().forEach(f -> report(f.line("KEY"))); report("SUBSCRIBED");
        List<Event> retained = new ArrayList<>(); List<List<String>> printed = new ArrayList<>();
        try (var deadline = Nervix.Cancel.deadline(120)) {
            int seen = 0;
            while (seen < expected) {
                Event event = session.nextEvent(deadline, new Scope());
                require(event.handle.kind() == Nervix.EventKind.ROWS, "the subscription reported something other than rows first");
                require(event.handle.subscription().equals(identity), "rows arrived for another subscription");
                List<String> lines = event.rowLines(schema); lines.forEach(Probe::report); seen += lines.size();
                printed.add(lines); retained.add(event);
            }
        }
        Outcome labelled = session.execute("SHOW LABELS ON RELAY " + relay + " FORMAT TEXT;", null, outcomes);
        try (var labels = labelled.handle().entityLabels()) {
            require(labels != null && " retained Δ ".equals(labels.labels().get("probe")), "typed entity labels lost exact metadata");
            require(relay.equals(labels.entityName()) && labels.revision() != null, "typed entity labels lost identity or revision");
        }
        checkCancellation(session);
        Outcome closed = session.execute("DELETE SUBSCRIPTION " + subscription + ";", null, outcomes);
        session.close();
        checkCollectedRelease(retained, printed, schema); checkRetention(retained, printed, schema);
        report("CHECKS ok"); report("CLOSED " + closed.disposition());
    }
    static void runUpload(Session session) throws java.io.IOException {
        try (var prepared = session.handle.prepare(System.getenv("NERVIX_PROBE_UPLOAD_QUERY"), null)) {
            for (String line : List.of("UPLOAD completed", "RETRY completed")) {
                try (var outcome = session.handle.execute(prepared, null)) {
                    require(outcome.disposition() == Nervix.Disposition.COMPLETED, "upload completion");
                    require(outcome.uploadFailure() == null, "successful upload has no refusal");
                    report(line);
                }
            }
            Files.writeString(Path.of(System.getenv("NERVIX_PROBE_UPLOAD_FILE")),
                              "different archive\n", StandardCharsets.UTF_8);
            try (var outcome = session.handle.execute(prepared, null)) {
                require(outcome.disposition() == Nervix.Disposition.FAILED, "conflicting upload fails");
                require(outcome.uploadFailure() == Nervix.UploadFailure.IDENTITY_CONFLICT,
                        "upload has its typed identity conflict");
                report("CONFLICT IdentityConflict");
            }
        }
    }

    static void runInspection(Session session) throws Exception {
        try (var outcome = session.handle.command("DESCRIBE LICENSE;", null); var license = outcome.stateInspection()) {
            require(license.domain() == null && license.entityName() == null, "cluster licensing scope");
            require(license.entityKind().equals("LICENSE") && license.counts().rows() == 3 && license.counts().branches() == 0, "typed licensing observation");
            report("INSPECTION license rows=3");
        }
        Nervix.Inspection inspection;
        try (var outcome = session.handle.command(
                "SHOW RELAY events MATERIALIZED STATE FOR UNBRANCHED LIMIT 1 FORMAT JSON;", null)) {
            inspection = outcome.stateInspection();
        }
        require(inspection.domain().equals(System.getenv("NERVIX_PROBE_DOMAIN")), "inspection domain");
        require(inspection.entityName().equals("events"), "inspection identity");
        require(inspection.counts().rows() == 0, "empty page counts");
        require(inspection.rejection() == null, "page is not a refusal");
        report("INSPECTION page rows=0");
        Nervix.Inspection refused;
        try (var outcome = session.handle.command("SHOW RELAY plain MATERIALIZED STATE FORMAT JSON;", null)) {
            refused = outcome.stateInspection();
        }
        require(refused.rejection() == Nervix.InspectionRejection.NOT_MATERIALIZED, "typed refusal");
        require(refused.counts() == null, "refusal has no page counts");
        report("INSPECTION refused not_materialized");
        String query = "SHOW RELAY populated MATERIALIZED STATE FOR ALL BRANCHES LIMIT 1";
        Nervix.Inspection first;
        try (var outcome = session.handle.command(query + " FORMAT JSON;", null)) { first = outcome.stateInspection(); }
        require(first.counts().rows() == 2 && first.counts().branches() == 2 && first.continuation() != null, "complete counts and continuation");
        var unit = first.units().getFirst();
        require(unit.kind() == Nervix.InspectionUnitKind.SINGLE && unit.index() == null, "unit identity");
        require(unit.owner().equals(unit.originNode()) && unit.originIncarnation() != 0 && unit.originFingerprint().length == 32 && unit.originStage() == Nervix.InspectionStage.LIVE, "read origin");
        var section = unit.sections().getFirst();
        var schema = section.schema();
        var fields = schema.fields(Nervix.Part.ROWS);
        require(schema.branch().equals("by_tenant") && schema.fields(Nervix.Part.BRANCH).getFirst().sensitive(), "branch sensitivity");
        require(fields.subList(0, 3).stream().map(Nervix.Field::name).toList().equals(List.of("tenant", "secret", "sequence")), "schema order");
        require(fields.get(0).sensitive() && fields.get(1).sensitive() && fields.get(2).type() == Nervix.FieldType.I64, "field metadata");
        require(section.omittedRows() == 1 && !section.watermarkLow().equals(section.watermarkHigh()), "section metadata");
        var frame = section.frames().getFirst();
        require(frame.rowCount() == 1, "verified frame rows");
        try (var frameSchema = frame.schema()) { require(frameSchema.branch().equals(schema.branch()), "frame schema"); }
        var buffer = frame.frame();
        byte[] expected = new byte[buffer.remaining()]; buffer.duplicate().get(expected);
        require(buffer.isReadOnly() && expected.length > 0, "verified read-only frame");
        String token = first.continuation().replace("'", "''");
        try (var outcome = session.handle.command(query + " AFTER '" + token + "' FORMAT JSON;", null);
                var second = outcome.stateInspection()) {
            require(second.continuation() == null && second.counts().rows() == 2, "continuation page");
            require(!first.json().equals(second.json()) && !first.json().contains("alpha-private") && !first.json().contains("first-private"), "distinct redacted pages");
            for (var nextUnit : second.units()) {
                try (nextUnit) {
                    for (var nextSection : nextUnit.sections()) {
                        try (nextSection) {
                            for (var nextFrame : nextSection.frames()) { try (nextFrame) { require(nextFrame.rowCount() == 1, "continuation frame rows"); } }
                        }
                    }
                }
            }
        }
        first.close(); unit.close(); section.close(); frame.close();
        report("INSPECTION branched rows=2 pages=2");
        session.close();
        System.gc();
        require(!inspection.json().isEmpty(), "retained inspection");
        require(refused.rejection() == Nervix.InspectionRejection.NOT_MATERIALIZED, "retained refusal");
        byte[] actual = new byte[buffer.remaining()]; buffer.duplicate().get(actual);
        require(java.util.Arrays.equals(actual, expected) && schema.fields(Nervix.Part.ROWS).get(2).name().equals("sequence"), "retained frame and schema");
        schema.close();
        inspection.close();
        refused.close();
        report("INSPECTION retained");
    }

    public static void main(String[] arguments) {
        String domain = System.getenv("NERVIX_PROBE_DOMAIN");
        try (var session = new Session(System.getenv("NERVIX_PROBE_GRPC_URI"), domain,
                System.getenv("NERVIX_PROBE_USERNAME"), System.getenv("NERVIX_PROBE_PASSWORD")); var outcomes = new Scope()) {
            if (arguments.length > 0 && arguments[0].equals("inspection")) runInspection(session);
            else if (arguments.length > 0 && arguments[0].equals("upload")) runUpload(session);
            else if (arguments.length > 0 && arguments[0].equals("io")) new Endpoints(session, domain).run();
            else if (arguments.length > 0 && arguments[0].equals("clock")) runClock(session, domain, outcomes);
            else if (arguments.length > 0 && arguments[0].equals("restoration")) runRestoration(session, outcomes);
            else runRows(session, outcomes);
            report("PASS");
        } catch (Throwable failure) {
            System.err.println("probe failed: " + failure); failure.printStackTrace(); System.exit(1);
        }
    }
}
