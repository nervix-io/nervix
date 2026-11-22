package io.nervix.client.suite;

import static org.junit.jupiter.api.Assertions.*;
import static io.nervix.client.suite.support.Cluster.*;
import io.nervix.client.Nervix;
import io.nervix.client.suite.support.Cluster;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Server notices a session reads by kind, the class and node of a logged error, and their waits. */
@Tag("cluster")
public final class NoticeTests {
    @Test
    void aLoggedJunctionErrorReachesTheSessionAsARuntimeNoticeWithItsClassAndNode() throws Exception {
        var cluster = new Cluster();
        String prefix = cluster.unique("noticed");
        String readings = prefix + "_readings", ratios = prefix + "_ratios";
        String ingestor = prefix + "_in", scorer = prefix + "_scorer";
        // A notice reaches the sessions open when it is published, so this one opens before the row is sent.
        try (var session = cluster.connect()) {
            completed(session, "CREATE RELAY " + readings + " SCHEMA suite_row UNBRANCHED;");
            completed(session, "CREATE RELAY " + ratios + " SCHEMA suite_row UNBRANCHED;");
            completed(session, "CREATE CLIENT PRODUCER INGESTOR " + ingestor + " FROM SCHEMA suite_row "
                + "MODE ACK SEQUENTIAL ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s ON QUIESCE SUSPEND "
                + "TIMESTAMP NOW TO " + readings + " INHERIT ALL UNBRANCHED FLUSH IMMEDIATE "
                + "ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;");
            // ANCHOR: logged_error
            // The divisor is always zero, so the junction fails every row with an `evaluation` error, and
            // its route's LOG policy publishes that error as a runtime notice to every session of every node.
            completed(session, "CREATE JUNCTION " + scorer + " FROM " + readings + " UNBRANCHED TO " + ratios
                + " INHERIT ALL SET amount = input.amount / (input.amount - input.amount) "
                + "FLUSH IMMEDIATE ON MESSAGE ERROR LOG;");
            try (var producer = cluster.producer(session, ingestor); var schema = producer.schema();
                    var batch = build(schema, 1301); var cancel = deadline()) {
                long submitted = producer.submit(batch, cancel);
                // LOG also negatively acknowledges the row, so its batch fails processing as rejected.
                try (var result = producer.rejoin(submitted, cancel)) {
                    var info = result.info();
                    assertEquals(Nervix.SubmissionResult.PROCESSING_FAILED, info.result(), info.message());
                    assertEquals(Nervix.ProcessingFailure.REJECTED, info.failure());
                    assertNull(info.refusal());
                    assertNull(info.defect());
                    assertNull(info.uncertainty());
                }
                producer.end(cancel);
            }
            // ANCHOR_END: logged_error
            // ANCHOR: read_notices
            try (var cancel = deadline()) {
                Nervix.Notice reported = null;
                while (reported == null) {
                    Nervix.Notice notice = session.nextNotice(cancel);
                    // A notice carries all three error fields or none; select by them, never by the text.
                    boolean classified = notice.errorClass() != null;
                    assertEquals(classified, notice.errorNodeKind() != null, notice.message());
                    assertEquals(classified, notice.errorNode() != null, notice.message());
                    if (scorer.equals(notice.errorNode())) reported = notice;
                }
                assertEquals(Nervix.NoticeKind.RUNTIME, reported.kind());
                assertEquals(Nervix.NoticeLevel.ERROR, reported.level());
                assertEquals("evaluation", reported.errorClass());
                assertEquals("junction", reported.errorNodeKind());
                String origin = "junction '" + scorer + "' message error in domain '" + cluster.domain + "'";
                assertTrue(reported.message().contains(origin), reported.message());
            }
            // ANCHOR_END: read_notices
            completed(session, "DROP JUNCTION " + scorer + ";");
            completed(session, "DROP INGESTOR " + ingestor + ";");
            completed(session, "DROP RELAY " + ratios + ";");
            completed(session, "DROP RELAY " + readings + ";");
        }
    }

    @Test
    void noticeWaitsEndAtTheirDeadlineOrCancellation() throws Exception {
        var cluster = new Cluster();
        try (var session = cluster.connect()) {
            // ANCHOR: notice_waits
            // One deadline bounds the whole loop: notices arriving meanwhile are read, then the wait ends.
            try (var briefly = Nervix.Cancel.deadline(0.2)) {
                assertThrows(Nervix.DeadlineException.class, () -> { while (true) session.nextNotice(briefly); });
            }
            // Another thread ends a reader's wait by triggering the token the reader passed.
            try (var workers = Executors.newSingleThreadExecutor(); var cancellation = new Nervix.Cancel()) {
                var reading = workers.submit(() -> assertThrows(Nervix.CancelledException.class,
                    () -> { while (true) session.nextNotice(cancellation); }));
                cancellation.trigger();
                assertEquals(Nervix.FailureKind.CANCELLED, reading.get(30, TimeUnit.SECONDS).kind());
            }
            // ANCHOR_END: notice_waits
        }
    }
}
