package io.nervix.client;

import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Checks the actual Java/JNI boundary, including cancellation from a second host thread. */
public final class BoundaryTests {
    private BoundaryTests() {}

    private static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    private static void columns() {
        try (var fields = new Nervix.Fields()) {
            fields.add("id", Nervix.FieldType.U64, 0, false, false);
            fields.add("text", Nervix.FieldType.STRING, 0, true, false);
            fields.add("tags", Nervix.FieldType.LIST, 0, false, false);
            fields.element(Nervix.FieldType.I16, 0);
            try (var schema = fields.schema(); var builder = new Nervix.BatchBuilder(schema, 2)) {
                byte[] ids = ByteBuffer.allocate(16).order(ByteOrder.nativeOrder())
                    .putLong(-1).putLong(0).array();
                builder.fixed(0, 0, ids);
                builder.states(1, new byte[] {1, 2});
                byte[] text = "hé\u0000".getBytes(java.nio.charset.StandardCharsets.UTF_8);
                builder.varlen(1, 0, new long[] {0, text.length, text.length}, text);
                builder.offsets(2, 0, new long[] {0, 2, 2});
                byte[] tags = ByteBuffer.allocate(4).order(ByteOrder.nativeOrder())
                    .putShort(Short.MIN_VALUE).putShort(Short.MAX_VALUE).array();
                builder.fixed(2, 1, tags);
                try (var batch = builder.finish(); var copy = Nervix.Batch.fromIpc(schema, 2, batch.ipc())) {
                    require(Arrays.equals(ids, copy.fixed(0, 0)), "unsigned bits");
                    require(Arrays.equals(text, copy.varlen(1, 0).data()), "UTF-8 bytes");
                    require(Arrays.equals(new byte[] {1, 2}, copy.states(1)), "null states");
                    require(Nervix.CellState.fromCode(copy.states(1)[1]) == Nervix.CellState.NULL, "typed state decoding");
                    require(Arrays.equals(new long[] {0, 2, 2}, copy.offsets(2, 0)), "list offsets");
                    require(Arrays.equals(tags, copy.fixed(2, 1)), "signed bounds");
                    require(schema.fields(Nervix.Part.ROWS).get(0).type() == Nervix.FieldType.U64,
                        "field descriptions");
                }
            }
        }
    }

    private static void cancellation() throws Exception {
        try (var listener = new ServerSocket(0)) {
            var cancel = new Nervix.Cancel();
            var entered = new CountDownLatch(1);
            var observed = new AtomicReference<Throwable>();
            var waiting = new Thread(() -> {
                entered.countDown();
                try (var session = Nervix.Session.connect("http://127.0.0.1:" + listener.getLocalPort(),
                        null, new Nervix.ConnectOptions(), cancel)) {
                    observed.set(new AssertionError("an unanswered session opened: " + session));
                } catch (Throwable failure) {
                    observed.set(failure);
                }
            });
            waiting.start();
            entered.await();
            listener.setSoTimeout(30_000);
            try (var accepted = listener.accept()) {
                require(accepted.isConnected(), "native connect reached the test listener");
                cancel.trigger();
                cancel.close();
                waiting.join(30_000);
                require(!waiting.isAlive(), "cancelled native call must return");
                require(observed.get() instanceof Nervix.CancelledException, "typed cancellation");
            }
        }
    }

    private static void cleanup() throws Exception {
        var fields = new Nervix.Fields();
        fields.add("value", Nervix.FieldType.U64, 0, false, false);
        var field = Nervix.Handle.class.getDeclaredField("state"); field.setAccessible(true);
        Object state = field.get(fields);
        var pointer = state.getClass().getDeclaredField("pointer"); pointer.setAccessible(true);
        var owner = new java.lang.ref.WeakReference<>(fields);
        fields = null;
        for (int attempt = 0; attempt < 100; attempt++) {
            System.gc(); Thread.sleep(10);
            synchronized (state) {
                if (owner.get() == null && pointer.getLong(state) == 0) return;
            }
        }
        throw new AssertionError("Cleaner did not release its unreachable native allocation");
    }

    /** A failed buffer-view allocation can release its lease while the root's Cleaner does too. */
    private static void concurrentLeaseCleanup() throws Exception {
        var lease = new AtomicReference<AutoCloseable>();
        var start = new CyclicBarrier(3);
        var finished = new CyclicBarrier(3);
        int cases = 5_000;
        try (var fields = new Nervix.Fields(); var workers = Executors.newFixedThreadPool(2)) {
            fields.add("value", Nervix.FieldType.U64, 0, false, false);
            java.util.concurrent.Callable<Void> cleanup = () -> {
                for (int index = 0; index < cases; index++) {
                    start.await(30, TimeUnit.SECONDS);
                    lease.get().close();
                    finished.await(30, TimeUnit.SECONDS);
                }
                return null;
            };
            var first = workers.submit(cleanup);
            var second = workers.submit(cleanup);
            for (int index = 0; index < cases; index++) {
                lease.set(fields.lease());
                start.await(30, TimeUnit.SECONDS);
                finished.await(30, TimeUnit.SECONDS);
            }
            first.get(30, TimeUnit.SECONDS);
            second.get(30, TimeUnit.SECONDS);
        }
    }

    public static void main(String[] arguments) throws Exception {
        columns();
        cleanup();
        concurrentLeaseCleanup();
        cancellation();
        System.out.println("Java binding boundary checks passed");
    }
}
