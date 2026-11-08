package io.nervix.client.suite;

import static org.junit.jupiter.api.Assertions.*;
import static io.nervix.client.suite.support.Cluster.*;
import io.nervix.client.Nervix;
import io.nervix.client.suite.support.Cluster;
import io.nervix.client.suite.support.Relay;
import java.lang.ref.WeakReference;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Session loss, endpoint ownership, credit and contract changes through public host calls. */
@Tag("cluster")
@Order(2)
public final class RecoveryTests {
    @Test
    void aLostSessionRestoresSubscriptionsAndReportsARemovedRelay() throws Exception {
        var cluster = new Cluster();
        // A subscription to a newly created relay can redirect a follower before its view catches
        // up. Keep this loss fixture on the leader so both cuts end the session being observed.
        try (var administration = cluster.connect(); var relay = new Relay(cluster.leader);
                var watching = cluster.connect(relay.uri(), cluster.domain)) {
            String name = cluster.unique("restored");
            Nervix.Subscription initial;
            try (var cancel = deadline(); var outcome = watching.command("CREATE SUBSCRIPTION " + name + " TO watched_rows;", cancel)) {
                initial = outcome.subscription();
                assertNotNull(initial);
            }
            relay.cut();
            // ANCHOR: subscription_interrupted
            try (var cancel = deadline(); var interrupted = watching.nextEvent(cancel)) {
                assertEquals(Nervix.EventKind.INTERRUPTED, interrupted.kind());
                assertEquals(name, interrupted.subscription().name());
            }
            relay.restore();
            // ANCHOR_END: subscription_interrupted
            long expires = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
            boolean observed = false;
            while (!observed) {
                assertTrue(System.nanoTime() < expires, "restored subscription delivered no row");
                cluster.publish(administration, 601);
                try (var cancel = Nervix.Cancel.deadline(2); var event = watching.nextEvent(cancel)) {
                    assertEquals(Nervix.EventKind.ROWS, event.kind());
                    assertEquals(name, event.subscription().name());
                    // Generations are numbered within a server session; restoration can reuse the number.
                    assertEquals(initial.name(), event.subscription().name());
                    assertTrue(event.subscription().generation() > 0);
                    try (var batch = event.batch()) { assertEquals(601, id(batch)); }
                    observed = true;
                } catch (Nervix.DeadlineException restoring) {
                    // Reopening a subscription is asynchronous; only later rows can witness it.
                }
            }
            completed(watching, "DELETE SUBSCRIPTION " + name + ";");
            String doomed = cluster.unique("doomed");
            String ended = cluster.unique("ended");
            completed(administration, "CREATE RELAY " + doomed + " SCHEMA suite_row BRANCHED BY by_tenant;");
            completed(watching, "CREATE SUBSCRIPTION " + ended + " TO " + doomed + ";");
            relay.cut();
            try (var cancel = deadline(); var event = watching.nextEvent(cancel)) {
                assertEquals(Nervix.EventKind.INTERRUPTED, event.kind());
            }
            completed(administration, "DROP RELAY " + doomed + ";");
            relay.restore();
            // ANCHOR: subscription_ended
            try (var cancel = deadline(); var event = watching.nextEvent(cancel)) {
                assertEquals(Nervix.EventKind.ENDED, event.kind());
                assertEquals(Nervix.EndReason.RELAY_REMOVED, event.endReason());
                assertEquals(ended, event.subscription().name());
                assertNull(event.restorationFailure());
                assertThrows(Nervix.TypeMismatchException.class, event::schema);
                assertThrows(Nervix.TypeMismatchException.class, event::frame);
            }
            // ANCHOR_END: subscription_ended
        }
    }

    @Test
    void aLostPublishingSessionLeavesTheHeldSubmissionUnknownThenRestores() throws Exception {
        var cluster = new Cluster();
        try (var receiving = cluster.connect(); var consumer = cluster.consumer(receiving);
                var relay = new Relay(cluster.server); var publishing = cluster.connect(relay.uri(), cluster.domain);
                var producer = cluster.producer(publishing, "typed_in"); var schema = producer.schema();
                var batch = build(schema, 701); var cancel = deadline()) {
            long submitted = producer.submit(batch, cancel);
            try (var held = consumer.next(cancel)) {
                relay.cut();
                // ANCHOR: submission_unknown
                try (var unknown = producer.rejoin(submitted, cancel)) {
                    assertEquals(Nervix.SubmissionResult.OUTCOME_UNKNOWN, unknown.info().result());
                    assertEquals(Nervix.Uncertainty.SESSION_LOST, unknown.info().uncertainty());
                }
                // ANCHOR_END: submission_unknown
                assertEquals(Nervix.Settlement.CONFIRMED, held.ack(cancel));
            }
            relay.restore();
            try (var next = build(schema, 702)) {
                long again = producer.submit(next, cancel);
                try (var delivery = consumer.next(cancel); var received = delivery.batch()) {
                    assertEquals(702, id(received));
                    assertEquals(Nervix.Settlement.CONFIRMED, delivery.ack(cancel));
                }
                try (var outcome = producer.rejoin(again, cancel)) {
                    assertEquals(Nervix.SubmissionResult.COMPLETED, outcome.info().result());
                }
                assertEquals(Nervix.EndpointState.ACTIVE, producer.state());
            }
        }
    }

    @Test
    void aLostConsumerRestoresTheDeliveryIdentityAndFencesItsEarlierAttempt() throws Exception {
        var cluster = new Cluster();
        try (var publishing = cluster.connect(); var producer = cluster.producer(publishing, "typed_in");
                var schema = producer.schema(); var batch = build(schema, 801); var relay = new Relay(cluster.server);
                var consuming = cluster.connect(relay.uri(), cluster.domain); var consumer = cluster.consumer(consuming);
                var cancel = deadline()) {
            long submitted = producer.submit(batch, cancel);
            try (var held = consumer.next(cancel)) {
                byte[] identity = held.identity();
                relay.cut();
                // ANCHOR: consumer_interrupted
                assertThrows(Nervix.ConsumerInterruptedException.class, () -> consumer.next(cancel));
                assertThrows(Nervix.RejectedException.class, () -> held.ack(cancel));
                relay.restore();
                try (var redelivered = consumer.next(cancel)) {
                    assertArrayEquals(identity, redelivered.identity());
                    assertEquals(Nervix.Settlement.CONFIRMED, redelivered.ack(cancel));
                }
                // ANCHOR_END: consumer_interrupted
                assertEquals(Nervix.EndpointState.ACTIVE, consumer.state());
            }
            try (var outcome = producer.rejoin(submitted, cancel)) {
                assertEquals(Nervix.SubmissionResult.COMPLETED, outcome.info().result());
            }
            consumer.end(cancel);
            assertEquals(Nervix.EndpointState.CLOSED, consumer.state());
        }
    }

    @Test
    void creditWaitsWithoutSubmittingAndResolvedReleaseHandsOverTheOutcome() throws Exception {
        var cluster = new Cluster();
        try (var session = cluster.connect(); var fields = fields(false); var cancel = deadline();
                var producer = session.openProducer(cluster.domain, "typed_in", fields, 1, BYTES, cancel);
                var consumer = cluster.consumer(session); var schema = producer.schema();
                var first = build(schema, 901); var second = build(schema, 902)) {
            long submitted = producer.submit(first, cancel);
            // ANCHOR: credit
            try (var expires = Nervix.Cancel.deadline(0.2)) {
                assertThrows(Nervix.DeadlineException.class, () -> producer.submit(second, expires));
            }
            try (var delivery = consumer.next(cancel)) { assertEquals(Nervix.Settlement.CONFIRMED, delivery.ack(cancel)); }
            try (var result = producer.rejoin(submitted, cancel)) { assertEquals(Nervix.SubmissionResult.COMPLETED, result.info().result()); }
            long later = producer.submit(second, cancel);
            // ANCHOR_END: credit
            try (var delivery = consumer.next(cancel); var batch = delivery.batch()) {
                assertEquals(902, id(batch));
                assertEquals(Nervix.Settlement.CONFIRMED, delivery.ack(cancel));
            }
            waitUntil(() -> producer.pending().stream().allMatch(item -> item.outcome() != null), "terminal outcome did not arrive");
            try (var released = producer.release(later)) {
                assertNotNull(released);
                assertEquals(Nervix.SubmissionResult.COMPLETED, released.info().result());
            }
            assertTrue(producer.pending().isEmpty());
            try (var expires = Nervix.Cancel.deadline(0.1)) {
                assertThrows(Nervix.DeadlineException.class, () -> consumer.next(expires));
            }
            try (var workers = Executors.newSingleThreadExecutor(); var cancellation = new Nervix.Cancel()) {
                var waiting = workers.submit(() -> assertThrows(Nervix.CancelledException.class, () -> consumer.next(cancellation)));
                cancellation.trigger();
                assertEquals(Nervix.FailureKind.CANCELLED, waiting.get(30, TimeUnit.SECONDS).kind());
            }
        }
    }

    @Test
    void aProducerKeepsItsSessionAndDeliveryViewsKeepTheirNativeData() throws Exception {
        var cluster = new Cluster();
        var session = cluster.connect();
        var owner = new WeakReference<>(session);
        try (var producer = cluster.producer(session, "watched_in"); var schema = producer.schema();
                var batch = build(schema, 1001); var cancel = deadline()) {
            // ANCHOR: owner_lifetime
            session.close();
            session = null;
            waitUntil(() -> { System.gc(); return owner.get() == null; }, "session was not collected");
            long submitted = producer.submit(batch, cancel);
            try (var outcome = producer.rejoin(submitted, cancel)) {
                assertEquals(Nervix.SubmissionResult.COMPLETED, outcome.info().result());
            }
            // ANCHOR_END: owner_lifetime
        } finally {
            if (session != null) session.close();
        }
        java.nio.ByteBuffer held;
        byte[] original;
        try (var publishing = cluster.connect(); var producer = cluster.producer(publishing, "typed_in");
                var consumer = cluster.consumer(publishing); var schema = producer.schema(); var batch = build(schema, 1002);
                var cancel = deadline()) {
            long submitted = producer.submit(batch, cancel);
            try (var delivery = consumer.next(cancel)) {
                held = delivery.ipcBuffer().slice().duplicate().asReadOnlyBuffer();
                original = delivery.ipc();
                assertEquals(Nervix.Settlement.CONFIRMED, delivery.ack(cancel));
            }
            try (var outcome = producer.rejoin(submitted, cancel)) { assertEquals(Nervix.SubmissionResult.COMPLETED, outcome.info().result()); }
        }
        System.gc();
        byte[] retained = new byte[held.remaining()];
        held.get(retained);
        assertArrayEquals(original, retained);
    }

    @Test
    void changingAContractSuspendsAdmissionAndRequiresAnExplicitReopen() throws Exception {
        var cluster = new Cluster();
        String ingestor = cluster.unique("changing");
        String body = "FROM SCHEMA suite_row MODE ACK PARALLEL MAX 4 ACK TIMEOUT %s "
            + "RETRY POLICY BACKOFF 100ms MAX 1s ON QUIESCE SUSPEND";
        try (var session = cluster.connect()) {
            completed(session, "CREATE CLIENT PRODUCER INGESTOR " + ingestor + " " + body.formatted("60s")
                + " TIMESTAMP NOW TO typed_rows INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant "
                + "FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;");
            try (var producer = cluster.producer(session, ingestor); var consumer = cluster.consumer(session);
                    var schema = producer.schema(); var batch = build(schema, 1101); var cancel = deadline();
                    var workers = Executors.newSingleThreadExecutor()) {
                long submitted = producer.submit(batch, cancel);
                try (var delivery = consumer.next(cancel)) {
                    var altering = workers.submit(() -> completed(session, "ALTER CLIENT PRODUCER INGESTOR " + ingestor
                        + " SET " + body.formatted("30s") + ";"));
                    waitUntil(() -> producer.admission() == Nervix.Admission.SUSPENDED, "quiescence did not suspend admission");
                    assertEquals(Nervix.Settlement.CONFIRMED, delivery.ack(cancel));
                    altering.get(120, TimeUnit.SECONDS);
                }
                // The model alteration may retire the unresolved submission; either retained terminal
                // result is read before opening a producer with the current contract.
                if (!producer.pending().isEmpty()) try (var result = producer.rejoin(submitted, cancel)) {
                    assertTrue(result.info().result() == Nervix.SubmissionResult.COMPLETED
                        || result.info().result() == Nervix.SubmissionResult.OUTCOME_UNKNOWN);
                }
                // ANCHOR: reopen
                waitUntil(() -> producer.state() == Nervix.EndpointState.REOPEN_REQUIRED, "changed contract did not require reopen");
                assertEquals(Nervix.ReopenReason.CONTRACT_CHANGED, producer.reopen().reason());
                assertNull(producer.reopen().refusal());
                assertThrows(Nervix.ReopenRequiredException.class, () -> producer.submit(batch, cancel));
                try (var current = cluster.producer(session, ingestor)) {
                    assertEquals(Nervix.EndpointState.ACTIVE, current.state());
                    assertEquals(30_000_000_000L, current.description().ackTimeoutNanos());
                    current.end(cancel);
                }
                // ANCHOR_END: reopen
            }
        }
    }

    @Test
    void cancellationAfterAnObservedEffectRecoversTheSamePreparedExecution() throws Exception {
        var cluster = new Cluster();
        String relayName = cluster.unique("admitted");
        try (var administration = cluster.connect(); var relay = new Relay(cluster.leader);
                var executing = cluster.connect(relay.uri(), cluster.domain);
                var execution = executing.prepare("CREATE RELAY " + relayName + " SCHEMA suite_row BRANCHED BY by_tenant;", null);
                var cancellation = new Nervix.Cancel(); var workers = Executors.newSingleThreadExecutor()) {
            relay.pauseReplies();
            try {
                var running = workers.submit(() -> assertThrows(Nervix.CancelledException.class,
                    () -> executing.execute(execution, cancellation)));
                waitUntil(() -> {
                    try (var cancel = deadline(); var outcome = administration.command("SHOW CREATE RELAY " + relayName + ";", cancel)) {
                        return outcome.disposition() == Nervix.Disposition.COMPLETED;
                    }
                }, "the admitted command's relay was not observed");
                // ANCHOR: cancellation_after_admission
                cancellation.trigger();
                assertEquals(execution.reference(), running.get(30, TimeUnit.SECONDS).executionReference());
                relay.resumeReplies();
                try (var cancel = deadline(); var recovered = executing.execute(execution, cancel)) {
                    assertEquals(Nervix.Disposition.COMPLETED, recovered.disposition(), recovered.message());
                    assertEquals(execution.reference(), recovered.executionReference());
                }
                // ANCHOR_END: cancellation_after_admission
            } finally {
                cancellation.trigger();
                relay.resumeReplies();
            }
            completed(administration, "DROP RELAY " + relayName + ";");
        }
    }

}
