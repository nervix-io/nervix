package io.nervix.client.suite;

import static org.junit.jupiter.api.Assertions.*;
import static io.nervix.client.suite.support.Cluster.*;
import io.nervix.client.Nervix;
import io.nervix.client.suite.support.Cluster;
import java.lang.ref.WeakReference;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/** Applications use the published jar against the scenario's one- or three-node cluster. */
@Tag("cluster")
@Order(0)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public final class ClusterTests {
    private Cluster cluster;
    private Nervix.Session session;

    @BeforeEach
    void connect() throws Exception {
        cluster = new Cluster();
        session = cluster.connect();
    }

    @AfterEach
    void close() {
        if (session != null) session.close();
    }

    @Test @Order(0)
    void packagedJarLoadsOnAFreshJvm() throws Exception {
        var jar = java.nio.file.Path.of(System.getenv("NERVIX_CLIENT_JVM_JAR"));
        var process = new ProcessBuilder(java.nio.file.Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-Xcheck:jni", "-cp", jar + java.io.File.pathSeparator + jar.getParent().resolve("tests"),
            "io.nervix.client.suite.support.PackagingProbe").inheritIO().start();
        try {
            assertTrue(process.waitFor(30, TimeUnit.SECONDS), "packaging JVM did not finish");
            assertEquals(0, process.exitValue());
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                assertTrue(process.waitFor(10, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void connectsWithEveryTlsRequirementAndTimeoutOption() {
        for (var requirement : List.of(Nervix.TlsRequirement.PREFERRED, Nervix.TlsRequirement.REQUIRED)) {
            // ANCHOR: connect
            try (var options = new Nervix.ConnectOptions()
                    .auth(cluster.username, cluster.password).tls(requirement)
                    .caCertificate(cluster.authority).seeds(cluster.seeds)
                    .connectTimeout(30).requestTimeout(60).retryTimeout(120).backupWaitTimeout(300);
                    var cancel = deadline()) {
                if (requirement == Nervix.TlsRequirement.REQUIRED && cluster.server.startsWith("http://")) {
                    assertThrows(Nervix.ConnectException.class,
                        () -> Nervix.Session.connect(cluster.server, cluster.domain, options, cancel));
                } else {
                    try (var connected = Nervix.Session.connect(cluster.server, cluster.domain, options, cancel)) {
                        completed(connected, "LIST DOMAINS;");
                    }
                }
            }
            // ANCHOR_END: connect
        }
        try (var options = cluster.options().tls(Nervix.TlsRequirement.REQUIRED); var cancel = deadline()) {
            assertThrows(Nervix.ConnectException.class, () -> Nervix.Session.connect(
                cluster.server.replace("https://", "http://"), cluster.domain, options, cancel));
        }
    }

    @Test
    void credentialsTrustAndUnavailableSeedsProduceTypedConnectFailures() throws Exception {
        try (var options = cluster.options().auth(cluster.username, "wrong password"); var cancel = deadline()) {
            var failure = assertThrows(Nervix.ConnectException.class,
                () -> Nervix.Session.connect(cluster.server, cluster.domain, options, cancel));
            assertEquals(Nervix.FailureKind.CONNECT, failure.kind());
            assertFalse(failure.getMessage().isBlank());
            assertFalse(failure.getMessage().contains("wrong password"));
        }
        try (var options = new Nervix.ConnectOptions().auth(cluster.username, cluster.password);
                var cancel = deadline()) {
            if (cluster.server.startsWith("https://")) {
                assertThrows(Nervix.ConnectException.class,
                    () -> Nervix.Session.connect(cluster.server, cluster.domain, options, cancel));
            } else {
                try (var connected = Nervix.Session.connect(cluster.server, cluster.domain, options, cancel)) {
                    completed(connected, "LIST DOMAINS;");
                }
            }
        }
        int refused;
        try (var reserved = new ServerSocket(0)) { refused = reserved.getLocalPort(); }
        String scheme = cluster.server.startsWith("https://") ? "https://" : "http://";
        String unreachable = scheme + "127.0.0.1:" + refused;
        // ANCHOR: seeds
        try (var options = cluster.options().seeds(cluster.seeds); var cancel = deadline();
                var seeded = Nervix.Session.connect(unreachable, cluster.domain, options, cancel)) {
            completed(seeded, "LIST DOMAINS;");
        }
        // ANCHOR_END: seeds
        try (var options = cluster.options().seeds(unreachable); var cancel = deadline()) {
            assertThrows(Nervix.ConnectException.class,
                () -> Nervix.Session.connect(unreachable, cluster.domain, options, cancel));
        }
        assertThrows(Nervix.ConnectException.class, () -> Nervix.Session.connect("http://127.0.0.1:1", null));
    }

    @Test
    void executesPreparedCommandsAndReadsDiagnostics() {
        String created = cluster.unique("prepared");
        // ANCHOR: execute
        try (var cancel = deadline(); var execution = session.prepare("CREATE SCHEMA " + created + " (id U32);", cancel);
                var outcome = session.execute(execution, cancel)) {
            assertEquals(Nervix.Disposition.COMPLETED, outcome.disposition());
            assertEquals(execution.reference(), outcome.executionReference());
            assertNotNull(outcome.message());
            assertTrue(outcome.diagnostics().isEmpty());
            assertNull(outcome.uploadFailure());
            assertNull(outcome.subscription());
            assertNull(outcome.schema());
            assertNull(outcome.backup());
            assertNull(outcome.restore());
        }
        // ANCHOR_END: execute
        // ANCHOR: diagnostics
        try (var cancel = deadline(); var execution = session.prepare("CREATE SCHEMA ;", cancel);
                var refused = session.execute(execution, cancel)) {
            assertEquals(Nervix.Disposition.FAILED, refused.disposition());
            assertFalse(refused.diagnostics().isEmpty());
            for (var diagnostic : refused.diagnostics()) {
                assertFalse(diagnostic.message().isBlank());
                assertEquals(2, diagnostic.span().length);
                assertTrue(diagnostic.span()[1] >= diagnostic.span()[0]);
            }
        }
        // ANCHOR_END: diagnostics
    }

    @Test
    void cancellationAndDeadlineKeepThePreparedIdentityForRecovery() {
        String schema = cluster.unique("recovered");
        // ANCHOR: recover_execution
        try (var execution = session.prepare("CREATE SCHEMA " + schema + " (id U32);", null);
                var cancelled = new Nervix.Cancel(); var deadline = deadline()) {
            cancelled.trigger();
            var failure = assertThrows(Nervix.CancelledException.class, () -> session.execute(execution, cancelled));
            assertEquals(execution.reference(), failure.executionReference());
            try (var outcome = session.execute(execution, deadline)) {
                assertEquals(Nervix.Disposition.COMPLETED, outcome.disposition(), outcome.message());
                assertEquals(execution.reference(), outcome.executionReference());
            }
            try (var repeated = session.execute(execution, deadline)) {
                assertEquals(Nervix.Disposition.COMPLETED, repeated.disposition());
            }
        }
        // ANCHOR_END: recover_execution
        try (var execution = session.prepare("LIST DOMAINS;", null); var expired = Nervix.Cancel.deadline(0)) {
            var failure = assertThrows(Nervix.DeadlineException.class, () -> session.execute(execution, expired));
            assertEquals(execution.reference(), failure.executionReference());
            try (var cancel = deadline(); var outcome = session.execute(execution, cancel)) {
                assertEquals(Nervix.Disposition.COMPLETED, outcome.disposition());
            }
        }
    }

    // Runs after every other test of the class: no statement removes a resource, and the backup test
    // archives this domain expecting it to hold no resource version.
    @Test @Order(Order.DEFAULT + 1)
    void anUploadCompletesAgainUnderItsIdentityAndRefusesAnotherArchive() throws Exception {
        String resource = cluster.unique("bundle");
        completed(session, "CREATE RESOURCE " + resource + ";");
        var directory = Files.createDirectory(cluster.backups.resolve(resource));
        var payload = directory.resolve("payload.txt");
        Files.writeString(payload, "first archive\n", StandardCharsets.UTF_8);
        // ANCHOR: upload
        try (var execution = session.prepare("UPLOAD RESOURCE " + resource + " VERSION '" + directory + "';", null)) {
            try (var cancel = deadline(); var uploaded = session.execute(execution, cancel)) {
                assertEquals(Nervix.Disposition.COMPLETED, uploaded.disposition(), uploaded.message());
                assertNull(uploaded.uploadFailure());
            }
            // The same execution sends the same archive under the same upload identity, so it completes
            // again instead of installing a second version.
            try (var cancel = deadline(); var again = session.execute(execution, cancel)) {
                assertEquals(Nervix.Disposition.COMPLETED, again.disposition(), again.message());
                assertNull(again.uploadFailure());
            }
            Files.writeString(payload, "different archive\n", StandardCharsets.UTF_8);
            try (var cancel = deadline(); var refused = session.execute(execution, cancel)) {
                assertEquals(Nervix.Disposition.FAILED, refused.disposition());
                assertEquals(Nervix.UploadFailure.IDENTITY_CONFLICT, refused.uploadFailure());
            }
        }
        // ANCHOR_END: upload
    }

    @Test
    void completesStatementsAndContinuesEveryPage() {
        String text = "CREATE SCH";
        // ANCHOR: complete
        try (var cancel = deadline(); var page = session.suggest(text, text.length(), 20, null, cancel)) {
            assertEquals(Nervix.CompletionStatus.READY, page.status());
            var schema = page.candidates().stream().filter(item -> item.value().equals("SCHEMA")).findFirst().orElseThrow();
            assertEquals(Nervix.CompletionKind.TEXT, schema.kind());
            assertEquals(text.length(), schema.end());
            String completed = text.substring(0, Math.toIntExact(schema.start())) + schema.replacement()
                + text.substring(Math.toIntExact(schema.end()));
            assertTrue(completed.startsWith("CREATE SCHEMA"));
        }
        // ANCHOR_END: complete
        var candidates = new ArrayList<String>();
        String continuation = null;
        do {
            try (var cancel = deadline(); var page = session.suggest("CREATE ", 7, 2, continuation, cancel)) {
                assertEquals(Nervix.CompletionStatus.READY, page.status());
                for (var candidate : page.candidates()) candidates.add(candidate.value());
                continuation = page.continuation();
            }
        } while (continuation != null);
        assertTrue(candidates.size() > 2);
        assertEquals(candidates.size(), new HashSet<>(candidates).size());
    }

    @Test
    void subscriptionsReadTypedColumnsNullsRedactionAndRetainedFrames() {
        String name = cluster.unique("seen");
        Nervix.Subscription subscription;
        // ANCHOR: subscribe
        try (var cancel = deadline(); var opened = session.command("CREATE SUBSCRIPTION " + name + " TO watched_rows;", cancel);
                var schema = opened.schema()) {
            assertEquals(Nervix.Disposition.COMPLETED, opened.disposition(), opened.message());
            subscription = opened.subscription();
            assertEquals(name, subscription.name());
            assertEquals("by_tenant", schema.branch());
            assertEquals("tenant", schema.fields(Nervix.Part.BRANCH).getFirst().name());
            var fields = schema.fields(Nervix.Part.ROWS);
            assertEquals(11, fields.size());
            assertEquals("id", fields.getFirst().name());
            assertEquals(Nervix.FieldType.U32, fields.getFirst().type());
            assertFalse(fields.getFirst().nullable());
            assertTrue(fields.get(10).sensitive());
            assertEquals(List.of(new Nervix.FieldLevel(Nervix.FieldType.LIST, 0),
                new Nervix.FieldLevel(Nervix.FieldType.STRING, 0)), fields.get(8).levels());
        }
        // ANCHOR_END: subscribe
        cluster.publish(session, 201);
        ByteBuffer held;
        byte[] original;
        // ANCHOR: read_rows
        try (var cancel = deadline(); var event = session.nextEvent(cancel); var schema = event.schema();
                var batch = event.batch()) {
            assertEquals(Nervix.EventKind.ROWS, event.kind());
            assertEquals(subscription, event.subscription());
            assertEquals(1, event.rows());
            assertEquals(11, schema.fields(Nervix.Part.ROWS).size());
            assertEquals(201, id(batch));
            assertArrayEquals(new byte[] {2}, event.states(Nervix.Part.ROWS, 5));
            assertArrayEquals(new byte[] {3}, event.states(Nervix.Part.ROWS, 10));
            assertEquals(201, ByteBuffer.wrap(event.fixed(Nervix.Part.ROWS, 0)).order(ByteOrder.nativeOrder()).getInt());
            assertArrayEquals("acme".getBytes(StandardCharsets.UTF_8), event.cell(Nervix.Part.ROWS, 0, 1));
            assertArrayEquals("acme".getBytes(StandardCharsets.UTF_8), event.varlen(Nervix.Part.ROWS, 1).data());
            assertArrayEquals("acme".getBytes(StandardCharsets.UTF_8), event.varlen(Nervix.Part.BRANCH, 0).data());
            assertNull(event.restorationFailure());
            assertThrows(Nervix.TypeMismatchException.class, event::endReason);
            held = event.frame().slice().duplicate().asReadOnlyBuffer();
            original = new byte[held.remaining()];
            held.duplicate().get(original);
        }
        // ANCHOR_END: read_rows
        // ANCHOR: frame_lifetime
        System.gc();
        assertTrue(held.isDirect());
        assertTrue(held.isReadOnly());
        byte[] retained = new byte[held.remaining()];
        held.duplicate().get(retained);
        assertArrayEquals(original, retained);
        assertThrows(java.nio.ReadOnlyBufferException.class, () -> held.put(0, (byte) 0));
        // ANCHOR_END: frame_lifetime
        completed(session, "DELETE SUBSCRIPTION " + name + ";");
        try (var cancel = deadline(); var reopened = session.command("CREATE SUBSCRIPTION " + name + " TO watched_rows;", cancel)) {
            assertTrue(Long.compareUnsigned(reopened.subscription().generation(), subscription.generation()) > 0);
        }
        completed(session, "DELETE SUBSCRIPTION " + name + ";");
    }

    @Test
    void producerAndConsumerCarryTheirDescriptionsAndExactColumnarValues() {
        // ANCHOR: application
        try (var producer = cluster.producer(session, "typed_in"); var consumer = cluster.consumer(session);
                var schema = producer.schema(); var batch = build(schema, 301); var cancel = deadline()) {
            // ANCHOR: open_producer
            assertEquals(Nervix.EndpointState.ACTIVE, producer.state());
            assertEquals(Nervix.Admission.OPEN, producer.admission());
            assertNull(producer.reopen());
            var description = producer.description();
            assertTrue(description.generation() > 0);
            assertEquals(2, description.batches());
            assertEquals(BYTES, description.bytes());
            assertTrue(description.maxBatchRows() > 0);
            assertTrue(description.maxBatchBytes() > 0);
            assertEquals(Nervix.WindowKind.SEQUENTIAL, description.window());
            assertEquals(1, description.outstanding());
            assertEquals(60_000_000_000L, description.ackTimeoutNanos());
            assertEquals(100_000_000L, description.retryBackoffNanos());
            assertEquals(1_000_000_000L, description.retryMaxBackoffNanos());
            assertEquals(32, description.contract().length);
            assertFalse(description.attachment().isBlank());
            // ANCHOR_END: open_producer
            assertEquals(Nervix.EndpointState.ACTIVE, consumer.state());
            assertNull(consumer.reopen());
            assertEquals(4, consumer.description().batches());
            try (var output = consumer.schema()) { assertEquals(12, output.fields(Nervix.Part.ROWS).size()); }
            // ANCHOR: submit
            long submitted = producer.submit(batch, cancel);
            var pending = producer.pending();
            assertEquals(1, pending.size());
            assertEquals(submitted, pending.getFirst().id());
            assertNull(pending.getFirst().outcome());
            // ANCHOR_END: submit
            // ANCHOR: consume
            try (var delivery = consumer.next(cancel); var received = delivery.batch()) {
                assertColumns(received, 301);
                assertEquals("typed_rows", delivery.sourceRelay());
                assertTrue(delivery.identity().length > 0);
                assertTrue(delivery.reference().length > 0);
                assertTrue(delivery.branchFingerprint().length > 0);
                assertEquals(1, delivery.members());
                assertTrue(delivery.executionNow() > 0);
                // ANCHOR: arrow_ipc
                try (var copied = Nervix.Batch.fromIpc(schema, 1, batch.ipc()); var copySchema = copied.schema()) {
                    assertColumns(copied, 301);
                    assertEquals(11, copySchema.fields(Nervix.Part.ROWS).size());
                    assertTrue(copied.ipcBuffer().isReadOnly());
                }
                // ANCHOR_END: arrow_ipc
                assertTrue(delivery.ipc().length > 0);
                assertTrue(delivery.ipcBuffer().slice().duplicate().isReadOnly());
                assertEquals(Nervix.Settlement.CONFIRMED, delivery.ack(cancel));
            }
            // ANCHOR_END: consume
            // ANCHOR: rejoin
            try (var completed = producer.rejoin(submitted, cancel)) {
                var info = completed.info();
                assertEquals(Nervix.SubmissionResult.COMPLETED, info.result(), info.message());
                assertEquals("", info.message());
                assertNull(info.refusal());
                assertNull(info.defect());
                assertNull(info.failure());
                assertNull(info.uncertainty());
            }
            // ANCHOR_END: rejoin
            assertTrue(producer.pending().isEmpty());
            consumer.end(cancel);
            producer.end(cancel);
        }
        // ANCHOR_END: application
    }

    @Test
    void retriesKeepDeliveryIdentityAndRejectsReportProcessingFailure() {
        try (var producer = cluster.producer(session, "typed_in"); var consumer = cluster.consumer(session);
                var schema = producer.schema(); var batch = build(schema, 401); var cancel = deadline()) {
            long submitted = producer.submitIpc(batch.ipc(), cancel);
            byte[] identity;
            byte[] reference;
            // ANCHOR: retry
            try (var first = consumer.next(cancel)) {
                identity = first.identity();
                reference = first.reference();
                assertEquals(Nervix.Settlement.CONFIRMED, first.retry(cancel));
            }
            try (var retried = consumer.next(cancel)) {
                assertArrayEquals(identity, retried.identity());
                assertFalse(java.util.Arrays.equals(reference, retried.reference()));
                assertEquals(Nervix.Settlement.CONFIRMED, retried.ack(cancel));
            }
            // ANCHOR_END: retry
            try (var outcome = producer.rejoin(submitted, cancel)) {
                assertEquals(Nervix.SubmissionResult.COMPLETED, outcome.info().result());
            }
            long rejected = producer.submit(batch, cancel);
            // ANCHOR: reject
            try (var delivery = consumer.next(cancel)) {
                assertEquals(Nervix.Settlement.CONFIRMED, delivery.reject("application refused the delivery", cancel));
            }
            try (var outcome = producer.rejoin(rejected, cancel)) {
                assertEquals(Nervix.SubmissionResult.PROCESSING_FAILED, outcome.info().result());
                assertEquals(Nervix.ProcessingFailure.REJECTED, outcome.info().failure());
            }
            // ANCHOR_END: reject
        }
    }

    @Test
    void releasesInterestWithoutCancellingEffectsAndRefusesForeignIpc() {
        try (var producer = cluster.producer(session, "typed_in"); var consumer = cluster.consumer(session);
                var schema = producer.schema(); var batch = build(schema, 501); var cancel = deadline()) {
            long submitted = producer.submit(batch, cancel);
            // ANCHOR: release
            assertNull(producer.release(submitted));
            assertTrue(producer.pending().isEmpty());
            try (var delivery = consumer.next(cancel)) {
                assertEquals(Nervix.Settlement.CONFIRMED, delivery.ack(cancel));
            }
            // ANCHOR_END: release
            try (var foreignSchema = consumer.schema(); var foreign = build(foreignSchema, 502)) {
                long refused = producer.submitIpc(foreign.ipc(), cancel);
                try (var outcome = producer.rejoin(refused, cancel)) {
                    assertEquals(Nervix.SubmissionResult.NOT_ADMITTED, outcome.info().result());
                    assertEquals(Nervix.SubmissionRefusal.INVALID_BATCH, outcome.info().refusal());
                    assertEquals(Nervix.BatchDefect.SCHEMA_MISMATCH, outcome.info().defect());
                }
            }
        }
    }

    @Test
    void opensRefusedByTheClusterKeepTheTypedRefusal() {
        try (var fields = fields(false); var cancel = deadline()) {
            // ANCHOR: errors
            var failure = assertThrows(Nervix.RejectedException.class, () ->
                session.openProducer(cluster.domain, "missing_ingestor", fields, 2, BYTES, cancel));
            assertEquals(Nervix.FailureKind.REJECTED, failure.kind());
            assertEquals(Nervix.OpenRefusal.ENDPOINT_NOT_FOUND, failure.openRefusal());
            assertFalse(failure.getMessage().isBlank());
            // ANCHOR_END: errors
            assertThrows(Nervix.InvalidArgumentException.class, () ->
                session.openProducer(cluster.domain, "typed_in", fields, 0, BYTES, cancel));
        }
    }

    @Test
    void aSharedSessionMakesProgressWhileAnotherThreadWaitsAndCancels() throws Exception {
        try (var workers = Executors.newFixedThreadPool(4); var token = new Nervix.Cancel()) {
            var entered = new CountDownLatch(1);
            // ANCHOR: threads
            var waiting = workers.submit(() -> {
                entered.countDown();
                return assertThrows(Nervix.CancelledException.class, () -> session.nextEvent(token));
            });
            try {
                assertTrue(entered.await(30, TimeUnit.SECONDS));
                var commands = new ArrayList<java.util.concurrent.Future<?>>();
                for (int thread = 0; thread < 3; thread++)
                    commands.add(workers.submit(() -> completed(session, "LIST DOMAINS;")));
                for (var command : commands) command.get(60, TimeUnit.SECONDS);
                token.trigger();
                assertEquals(Nervix.FailureKind.CANCELLED, waiting.get(30, TimeUnit.SECONDS).kind());
            } finally {
                token.trigger();
            }
            // ANCHOR_END: threads
        }
    }

    @Test
    void aBackupAndAnUncertainBackupRecoverUnderTheirPreparedReferences() throws Exception {
        var archive = cluster.backups.resolve(cluster.unique("archive") + ".nvxb");
        String copy = cluster.domain + "_" + cluster.unique("copy");
        // ANCHOR: backup
        try (var execution = session.prepare("BACKUP DOMAIN " + cluster.domain + " TO '" + archive + "';", null);
                var cancelled = new Nervix.Cancel(); var cancel = deadline()) {
            cancelled.trigger();
            var failure = assertThrows(Nervix.CancelledException.class, () -> session.execute(execution, cancelled));
            assertEquals(execution.reference(), failure.executionReference());
            try (var outcome = session.execute(execution, cancel)) {
                assertEquals(Nervix.Disposition.COMPLETED, outcome.disposition(), outcome.message());
                assertEquals(Files.size(archive), outcome.backup().bytes());
                assertEquals(32, outcome.backup().digest().length);
            }
        }
        // ANCHOR_END: backup
        // ANCHOR: restore
        try (var cancel = deadline(); var planned = session.command("RESTORE DOMAIN " + cluster.domain + " AS " + copy
                + " FROM '" + archive + "' DRY RUN;", cancel)) {
            assertEquals(Nervix.Disposition.COMPLETED, planned.disposition(), planned.message());
            assertTrue(planned.restore().dryRun());
            assertFalse(planned.restore().failed());
        }
        try (var cancel = deadline(); var restored = session.command("RESTORE DOMAIN " + cluster.domain + " AS " + copy
                + " FROM '" + archive + "';", cancel)) {
            assertEquals(Nervix.Disposition.COMPLETED, restored.disposition(), restored.message());
            var report = restored.restore();
            assertFalse(report.dryRun());
            assertFalse(report.failed());
            assertEquals(1, report.domains());
            assertTrue(report.models() > 0);
            assertEquals(0, report.resourceVersions());
        }
        // ANCHOR_END: restore
    }

    @Test
    void malformedLocalIpcIsAnArgumentFailureWithNoServerOperation() {
        try (var fields = new Nervix.Fields()) {
            fields.add("id", Nervix.FieldType.U32, 0, false, false);
            try (var schema = fields.schema()) {
                var failure = assertThrows(Nervix.InvalidArgumentException.class,
                    () -> Nervix.Batch.fromIpc(schema, 1, new byte[] {0, 1, 2}));
                assertEquals(Nervix.FailureKind.INVALID_ARGUMENT, failure.kind());
                assertNull(failure.executionReference());
                assertFalse(failure.getMessage().isBlank());
            }
        }
        completed(session, "LIST DOMAINS;");
    }

}
