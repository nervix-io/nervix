package io.nervix.client.suite;

import static org.junit.jupiter.api.Assertions.*;
import static io.nervix.client.suite.support.Cluster.*;
import io.nervix.client.Nervix;
import io.nervix.client.suite.support.Cluster;
import io.nervix.client.suite.support.Actions;
import io.nervix.client.suite.support.Relay;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Clock snapshots, projection arithmetic, event generations and host wait cancellation. */
@Tag("cluster")
@Order(1)
public final class ClockTests {
    @Test
    void aRefusedClockRestorationReportsItsCauseAndRecovers() throws Exception {
        var cluster = new Cluster();
        try (var relay = new Relay(cluster.leader);
             var following = cluster.connect(relay.uri(), cluster.domain);
             var refusals = new Actions.ClockRefusals(); var cancel = deadline()) {
            completed(following, "ATTACH DOMAIN CLOCK;");
            long generation;
            try (var initial = following.domainClock(cluster.domain)) {
                assertNotNull(initial);
                assertEquals(Nervix.ClockState.UNPACED, initial.state());
                generation = initial.generation();
            }
            relay.cut();
            refusals.refuseNextClockAttachment();
            relay.restore();
            // ANCHOR: clock_restoration_failed
            try (var interrupted = following.nextClockEvent(cancel)) {
                assertEquals(Nervix.ClockEventKind.INTERRUPTED, interrupted.kind());
                assertEquals(cluster.domain, interrupted.domain());
            }
            try (var refused = following.nextClockEvent(cancel)) {
                assertEquals(Nervix.ClockEventKind.RESTORATION_FAILED, refused.kind());
                assertEquals(cluster.domain, refused.domain());
                var failure = refused.restorationFailure();
                assertNotNull(failure);
                assertEquals(Nervix.RestorationCause.IN_TRANSACTION, failure.cause());
                assertFalse(failure.message().isEmpty());
                assertTrue(failure.waitNanos() > 0);
            }
            // The library keeps the attachment desired and retries the refused attempt.
            boolean restored = false;
            while (!restored) try (var event = following.nextClockEvent(cancel)) {
                assertEquals(cluster.domain, event.domain());
                if (event.kind() == Nervix.ClockEventKind.STATE) {
                    assertEquals(Nervix.ClockState.UNPACED, event.state());
                    assertEquals(generation, event.generation());
                    restored = true;
                } else {
                    assertEquals(Nervix.ClockEventKind.RESTORATION_FAILED, event.kind());
                    assertEquals(Nervix.RestorationCause.IN_TRANSACTION, event.restorationFailure().cause());
                }
            }
            // ANCHOR_END: clock_restoration_failed
            completed(following, "DETACH DOMAIN CLOCK;");
        }
    }

    @Test
    void readsProjectsAndFollowsClockGenerationsThroughStopAndStart() throws Exception {
        var cluster = new Cluster();
        String domain = cluster.domain + "_" + cluster.unique("clock");
        try (var administration = cluster.connect()) {
            completed(administration, "CREATE PACED DOMAIN " + domain + " WITH PERIOD 100ms SKEW 10ms;");
            try (var session = cluster.connect(cluster.server, domain)) {
                completed(session, "START AT '2030-01-01T00:00:00Z' TIME RATE 2.0;");
                assertNull(session.domainClock(domain));
                // ANCHOR: attach_clock
                completed(session, "ATTACH DOMAIN CLOCK;");
                try (var clock = session.domainClock(domain)) {
                    assertEquals(domain, clock.domain());
                    assertEquals(1, clock.generation());
                    assertEquals(Nervix.ClockState.PACED, clock.state());
                    var mapping = clock.paced();
                    assertEquals(100_000_000, mapping.periodNanos());
                    assertEquals(10_000_000, mapping.skewNanos());
                    assertEquals(1_893_456_000_000_000_000L, mapping.logicalOrigin());
                    assertEquals(2.0, mapping.rate());
                    // ANCHOR_END: attach_clock
                    // ANCHOR: project_clock
                    assertEquals(mapping.logicalOrigin(), clock.logicalTimeAt(mapping.utcAnchor()));
                    assertEquals(50_000_000, clock.wallNanosUntil(mapping.utcAnchor(),
                        mapping.logicalOrigin() + mapping.periodNanos()));
                    var window = clock.admissionWindow(mapping.utcAnchor());
                    assertEquals(mapping.logicalOrigin(), window.earliestCenter());
                    assertEquals(mapping.logicalOrigin(), window.latestCenter());
                    assertTrue(clock.admits(mapping.utcAnchor(), mapping.logicalOrigin() + mapping.skewNanos()));
                    assertFalse(clock.admits(mapping.utcAnchor(), mapping.logicalOrigin() + mapping.skewNanos() + 1));
                    // ANCHOR_END: project_clock
                }
                // ANCHOR: clock_ticks
                try (var cancel = deadline()) {
                    boolean ticked = false;
                    while (!ticked) {
                        try (var event = session.nextClockEvent(cancel)) {
                            assertEquals(domain, event.domain());
                            assertEquals(1, event.generation());
                            assertNull(event.restorationFailure());
                            if (event.kind() == Nervix.ClockEventKind.STATE) {
                                assertEquals(Nervix.ClockState.PACED, event.state());
                                assertEquals(1, event.paced().generation());
                            } else {
                                assertEquals(Nervix.ClockEventKind.TICK, event.kind());
                                var tick = event.tick();
                                assertEquals(1, tick.generation());
                                assertEquals(1_893_456_000_000_000_000L + (tick.id() - 1) * 100_000_000,
                                    tick.logicalBoundary());
                                assertTrue(tick.servingLogical() >= 1_893_456_000_000_000_000L);
                                assertTrue(tick.authorityUtc() > 0);
                                assertThrows(Nervix.TypeMismatchException.class, event::state);
                                assertThrows(Nervix.TypeMismatchException.class, event::endReason);
                                ticked = true;
                            }
                        }
                    }
                }
                // ANCHOR_END: clock_ticks
                try (var clock = session.domainClock(domain)) { assertNotNull(clock.tick()); }
                completed(session, "STOP;");
                completed(session, "START AT '2031-01-01T00:00:00Z' TIME RATE 4.0;");
                // ANCHOR: clock_generations
                try (var cancel = deadline()) {
                    boolean installed = false;
                    while (!installed) {
                        try (var event = session.nextClockEvent(cancel)) {
                            if (event.kind() == Nervix.ClockEventKind.STATE && event.generation() == 2
                                    && event.state() == Nervix.ClockState.PACED) {
                                assertEquals(1_924_992_000_000_000_000L, event.paced().logicalOrigin());
                                assertEquals(4.0, event.paced().rate());
                                installed = true;
                            }
                        }
                    }
                }
                // ANCHOR_END: clock_generations
                completed(session, "STOP;");
                completed(session, "DETACH DOMAIN CLOCK;");
            }
        }
    }

    @Test
    void detachingEndsTheAttachmentAndClockWaitsCanBeCancelled() throws Exception {
        var cluster = new Cluster();
        try (var session = cluster.connect()) {
            completed(session, "ATTACH DOMAIN CLOCK;");
            try (var clock = session.domainClock(cluster.domain)) {
                assertEquals(Nervix.ClockState.UNPACED, clock.state());
                assertThrows(Nervix.TypeMismatchException.class, clock::paced);
                assertNull(clock.tick());
            }
            // ANCHOR: detach_clock
            completed(session, "DETACH DOMAIN CLOCK;");
            assertNull(session.domainClock(cluster.domain));
            // ANCHOR_END: detach_clock
        }
        try (var session = cluster.connect(); var expired = Nervix.Cancel.deadline(0.1)) {
            assertThrows(Nervix.DeadlineException.class, () -> session.nextClockEvent(expired));
            try (var workers = Executors.newSingleThreadExecutor(); var cancellation = new Nervix.Cancel()) {
                var waiting = workers.submit(() -> assertThrows(Nervix.CancelledException.class,
                    () -> session.nextClockEvent(cancellation)));
                cancellation.trigger();
                assertEquals(Nervix.FailureKind.CANCELLED, waiting.get(30, TimeUnit.SECONDS).kind());
            }
        }
    }

    @Test
    void aLostSessionRestoresItsClockAttachmentBeforeDeliveringTicks() throws Exception {
        var cluster = new Cluster();
        String domain = cluster.domain + "_" + cluster.unique("restored_clock");
        try (var administration = cluster.connect(); var relay = new io.nervix.client.suite.support.Relay(cluster.server)) {
            completed(administration, "CREATE PACED DOMAIN " + domain + " WITH PERIOD 100ms SKEW 10ms;");
            // START is leader-only and can redirect a follower session away from the byte relay.
            // Start through a separate session before creating the attachment whose route is cut.
            try (var starting = cluster.connect(cluster.server, domain)) {
                completed(starting, "START AT '2030-01-01T00:00:00Z' TIME RATE 2.0;");
            }
            try (var session = cluster.connect(relay.uri(), domain)) {
                completed(session, "ATTACH DOMAIN CLOCK;");
                try (var cancel = deadline()) {
                    boolean ticked = false;
                    while (!ticked) try (var event = session.nextClockEvent(cancel)) {
                        ticked = event.kind() == Nervix.ClockEventKind.TICK;
                    }
                    relay.cut();
                    // ANCHOR: clock_interrupted
                    boolean interrupted = false;
                    while (!interrupted) try (var event = session.nextClockEvent(cancel)) {
                        interrupted = event.kind() == Nervix.ClockEventKind.INTERRUPTED;
                    }
                    try (var held = session.domainClock(domain)) {
                        assertEquals(Nervix.ClockState.PACED, held.state());
                        assertEquals(1, held.generation());
                    }
                    relay.restore();
                    boolean restored = false;
                    while (!restored) try (var event = session.nextClockEvent(cancel)) {
                        if (event.kind() == Nervix.ClockEventKind.STATE) {
                            assertEquals(Nervix.ClockState.PACED, event.state());
                            assertEquals(1, event.generation());
                            restored = true;
                        }
                    }
                    // ANCHOR_END: clock_interrupted
                }
                completed(session, "DETACH DOMAIN CLOCK;");
                completed(session, "STOP;");
            }
        }
    }

}
