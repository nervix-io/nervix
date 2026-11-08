package io.nervix.client;

import static org.junit.jupiter.api.Assertions.*;
import java.net.ServerSocket;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Invalid values and bounded waits through the actual packaged JNI boundary. */
@Tag("boundary")
public final class ArgumentTests {
    @Test
    void invalidDurationsHaveTypedFailuresAndClosedHandlesRefuseCalls() {
        for (double seconds : new double[] {-1, Double.NaN, Double.POSITIVE_INFINITY}) {
            assertThrows(Nervix.InvalidArgumentException.class, () -> Nervix.Cancel.deadline(seconds));
            try (var options = new Nervix.ConnectOptions()) {
                assertThrows(Nervix.InvalidArgumentException.class, () -> options.connectTimeout(seconds));
                assertThrows(Nervix.InvalidArgumentException.class, () -> options.requestTimeout(seconds));
                assertThrows(Nervix.InvalidArgumentException.class, () -> options.retryTimeout(seconds));
                assertThrows(Nervix.InvalidArgumentException.class, () -> options.backupWaitTimeout(seconds));
            }
        }
        // ANCHOR: lifetimes
        var fields = new Nervix.Fields();
        fields.close();
        fields.close();
        var failure = assertThrows(Nervix.ClosedException.class, fields::schema);
        assertEquals(Nervix.FailureKind.CLOSED, failure.kind());
        // ANCHOR_END: lifetimes
    }

    @Test
    void aSilentListenerEndsAtTheConnectDeadline() throws Exception {
        try (var listener = new ServerSocket(0); var workers = Executors.newSingleThreadExecutor();
                var options = new Nervix.ConnectOptions().connectTimeout(0.1).retryTimeout(0.2);
                var cancellation = Nervix.Cancel.deadline(1)) {
            // ANCHOR: deadline
            var waiting = workers.submit(() -> assertThrows(Nervix.ClientException.class, () ->
                Nervix.Session.connect("http://127.0.0.1:" + listener.getLocalPort(), null, options, cancellation)));
            listener.setSoTimeout(30_000);
            try (var peer = listener.accept()) {
                assertTrue(peer.isConnected());
                var failure = waiting.get(30, TimeUnit.SECONDS);
                assertTrue(failure instanceof Nervix.ConnectException || failure instanceof Nervix.DeadlineException,
                    failure.toString());
                assertFalse(failure.getMessage().isBlank());
            }
            // ANCHOR_END: deadline
        }
    }

    @Test
    void fieldLevelsAndMalformedIpcAreRefusedAsTypedArguments() {
        try (var fields = new Nervix.Fields()) {
            assertThrows(Nervix.InvalidArgumentException.class, () -> fields.element(Nervix.FieldType.I16, 0));
            fields.add("number", Nervix.FieldType.I64, 0, false, false);
            try (var schema = fields.schema()) {
                assertThrows(Nervix.InvalidArgumentException.class,
                    () -> Nervix.Batch.fromIpc(schema, 1, new byte[] {0, 1, 2}));
            }
        }
    }
}
