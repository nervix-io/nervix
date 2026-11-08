package io.nervix.client.suite.support;

import static org.junit.jupiter.api.Assertions.*;
import io.nervix.client.Nervix;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

/** The scenario's public target and columnar fixtures; no test changes the conformance probe. */
public final class Cluster {
    public static final long BYTES = 1 << 20;
    private static final AtomicInteger IDENTITIES = new AtomicInteger();
    public final String server = System.getenv("NERVIX_PROBE_GRPC_URI");
    public final String domain = System.getenv("NERVIX_PROBE_DOMAIN");
    public final String leader = System.getenv("NERVIX_SUITE_LEADER_GRPC_URI");
    public final String username = System.getenv("NERVIX_PROBE_USERNAME");
    public final String password = System.getenv("NERVIX_PROBE_PASSWORD");
    public final String[] seeds = System.getenv("NERVIX_SUITE_SEEDS").split(",");
    public final Path backups = Path.of(System.getenv("NERVIX_SUITE_BACKUP_DIRECTORY"));
    public final byte[] authority;

    public Cluster() throws IOException {
        authority = Files.readAllBytes(Path.of(System.getenv("NERVIX_SUITE_CA_CERTIFICATE")));
    }

    public Nervix.ConnectOptions options() {
        return new Nervix.ConnectOptions().auth(username, password).caCertificate(authority)
            .connectTimeout(30).requestTimeout(60).retryTimeout(120).backupWaitTimeout(300);
    }

    public Nervix.Session connect() {
        return connect(server, domain);
    }

    public Nervix.Session connect(String address, String selectedDomain) {
        try (var options = options(); var cancel = deadline()) {
            return Nervix.Session.connect(address, selectedDomain, options, cancel);
        }
    }

    public String unique(String prefix) {
        return prefix + "_" + IDENTITIES.incrementAndGet();
    }

    public static Nervix.Cancel deadline() {
        return Nervix.Cancel.deadline(120);
    }

    public static void completed(Nervix.Session session, String statement) {
        try (var cancel = deadline(); var outcome = session.command(statement, cancel)) {
            assertEquals(Nervix.Disposition.COMPLETED, outcome.disposition(), outcome.message());
        }
    }

    public static Nervix.Fields fields(boolean output) {
        var fields = new Nervix.Fields();
        fields.add("id", Nervix.FieldType.U32, 0, false, false)
            .add("tenant", Nervix.FieldType.STRING, 0, false, false)
            .add("amount", Nervix.FieldType.I64, 0, false, false)
            .add("ratio", Nervix.FieldType.F64, 0, false, false)
            .add("flag", Nervix.FieldType.BOOL, 0, false, false)
            .add("note", Nervix.FieldType.STRING, 0, true, false)
            .add("raw", Nervix.FieldType.BYTES, 0, true, false)
            .add("at", Nervix.FieldType.DATETIME, 0, false, false)
            .add("tags", Nervix.FieldType.LIST, 0, false, false).element(Nervix.FieldType.STRING, 0)
            .add("grid", Nervix.FieldType.FIXED_LIST, 2, false, false).element(Nervix.FieldType.I16, 0)
            .add("secret", Nervix.FieldType.STRING, 0, false, true);
        if (output) fields.add("echo", Nervix.FieldType.U32, 0, false, false);
        return fields;
    }

    public Nervix.Producer producer(Nervix.Session session, String name) {
        try (var fields = fields(false); var cancel = deadline()) {
            return session.openProducer(domain, name, fields, 2, BYTES, cancel);
        }
    }

    public Nervix.Consumer consumer(Nervix.Session session) {
        try (var fields = fields(true); var cancel = deadline()) {
            return session.openConsumer(domain, "typed_out", fields, 4, BYTES, cancel);
        }
    }

    public static byte[] number(long value, int bytes) {
        var buffer = ByteBuffer.allocate(bytes).order(ByteOrder.nativeOrder());
        if (bytes == 4) buffer.putInt(Math.toIntExact(value));
        else buffer.putLong(value);
        return buffer.array();
    }

    public static Nervix.Batch build(Nervix.Schema schema, int id) {
        try (var builder = new Nervix.BatchBuilder(schema, 1)) {
            builder.fixed(0, 0, number(id, 4));
            variable(builder, 1, 0, "acme".getBytes(StandardCharsets.UTF_8));
            builder.fixed(2, 0, number(Long.MIN_VALUE, 8));
            builder.fixed(3, 0, number(Double.doubleToRawLongBits(-0.0), 8));
            builder.fixed(4, 0, new byte[] {1});
            builder.states(5, new byte[] {(byte) Nervix.CellState.NULL.code()});
            builder.varlen(5, 0, new long[] {0, 0}, new byte[0]);
            variable(builder, 6, 0, new byte[] {0, 1, -1});
            builder.fixed(7, 0, number(1_893_456_000_000_000_000L, 8));
            builder.offsets(8, 0, new long[] {0, 2});
            builder.varlen(8, 1, new long[] {0, 1, 3}, "aβ".getBytes(StandardCharsets.UTF_8));
            builder.fixed(9, 1, ByteBuffer.allocate(4).order(ByteOrder.nativeOrder())
                .putShort(Short.MIN_VALUE).putShort(Short.MAX_VALUE).array());
            variable(builder, 10, 0, "hidden".getBytes(StandardCharsets.UTF_8));
            if (schema.fields(Nervix.Part.ROWS).size() == 12) builder.fixed(11, 0, number(id, 4));
            return builder.finish();
        }
    }

    private static void variable(Nervix.BatchBuilder builder, int column, int level, byte[] bytes) {
        builder.varlen(column, level, new long[] {0, bytes.length}, bytes);
    }

    public void publish(Nervix.Session session, int id) {
        try (var producer = producer(session, "watched_in"); var schema = producer.schema();
                var batch = build(schema, id); var cancel = deadline()) {
            long submitted = producer.submit(batch, cancel);
            try (var outcome = producer.rejoin(submitted, cancel)) {
                assertEquals(Nervix.SubmissionResult.COMPLETED, outcome.info().result(), outcome.info().message());
            }
        }
    }

    public static int id(Nervix.Batch batch) {
        return ByteBuffer.wrap(batch.fixed(0, 0)).order(ByteOrder.nativeOrder()).getInt();
    }

    public static void assertColumns(Nervix.Batch batch, int id) {
        assertEquals(1, batch.rows());
        assertEquals(id, id(batch));
        assertEquals(Long.MIN_VALUE, ByteBuffer.wrap(batch.fixed(2, 0)).order(ByteOrder.nativeOrder()).getLong());
        assertEquals(Double.doubleToRawLongBits(-0.0),
            ByteBuffer.wrap(batch.fixed(3, 0)).order(ByteOrder.nativeOrder()).getLong());
        assertArrayEquals(new byte[] {1}, batch.fixed(4, 0));
        assertArrayEquals(new byte[] {2}, batch.states(5));
        assertArrayEquals(new byte[] {0, 1, -1}, batch.varlen(6, 0).data());
        assertEquals(1_893_456_000_000_000_000L,
            ByteBuffer.wrap(batch.fixed(7, 0)).order(ByteOrder.nativeOrder()).getLong());
        assertEquals(2, batch.cells(8, 1));
        assertArrayEquals(new long[] {0, 2}, batch.offsets(8, 0));
        assertArrayEquals(new long[] {0, 1, 3}, batch.varlen(8, 1).offsets());
        assertArrayEquals("aβ".getBytes(StandardCharsets.UTF_8), batch.varlen(8, 1).data());
        assertArrayEquals(ByteBuffer.allocate(4).order(ByteOrder.nativeOrder())
            .putShort(Short.MIN_VALUE).putShort(Short.MAX_VALUE).array(), batch.fixed(9, 1));
    }

    public static void waitUntil(BooleanSupplier condition, String reason) {
        long expires = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        while (!condition.getAsBoolean()) {
            assertTrue(System.nanoTime() < expires, reason);
            Thread.yield();
        }
    }
}
