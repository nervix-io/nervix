package io.nervix.client;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** The declared Java vocabulary and records, with the host widths and value semantics. */
@Tag("boundary")
public final class VocabularyTests {
    @Test
    void everyEnumerationKeepsItsMembersAndCodes() {
        assertArrayEquals(new Nervix.CellState[] {
            Nervix.CellState.VALUE, Nervix.CellState.NULL, Nervix.CellState.REDACTED
        }, Nervix.CellState.values());
        assertEquals(1, Nervix.CellState.VALUE.code());
        assertEquals(2, Nervix.CellState.NULL.code());
        assertEquals(3, Nervix.CellState.REDACTED.code());

        assertArrayEquals(new Nervix.TlsRequirement[] {
            Nervix.TlsRequirement.PREFERRED, Nervix.TlsRequirement.REQUIRED
        }, Nervix.TlsRequirement.values());
        assertEquals(1, Nervix.TlsRequirement.PREFERRED.code());
        assertEquals(2, Nervix.TlsRequirement.REQUIRED.code());

        assertArrayEquals(new Nervix.FieldType[] {
            Nervix.FieldType.U8, Nervix.FieldType.I8, Nervix.FieldType.U16, Nervix.FieldType.I16, Nervix.FieldType.U32, Nervix.FieldType.I32, Nervix.FieldType.U64, Nervix.FieldType.I64, Nervix.FieldType.F32, Nervix.FieldType.F64, Nervix.FieldType.BOOL, Nervix.FieldType.STRING, Nervix.FieldType.BYTES, Nervix.FieldType.DATETIME, Nervix.FieldType.FIXED_LIST, Nervix.FieldType.LIST
        }, Nervix.FieldType.values());
        assertEquals(1, Nervix.FieldType.U8.code());
        assertEquals(2, Nervix.FieldType.I8.code());
        assertEquals(3, Nervix.FieldType.U16.code());
        assertEquals(4, Nervix.FieldType.I16.code());
        assertEquals(5, Nervix.FieldType.U32.code());
        assertEquals(6, Nervix.FieldType.I32.code());
        assertEquals(7, Nervix.FieldType.U64.code());
        assertEquals(8, Nervix.FieldType.I64.code());
        assertEquals(9, Nervix.FieldType.F32.code());
        assertEquals(10, Nervix.FieldType.F64.code());
        assertEquals(11, Nervix.FieldType.BOOL.code());
        assertEquals(12, Nervix.FieldType.STRING.code());
        assertEquals(13, Nervix.FieldType.BYTES.code());
        assertEquals(14, Nervix.FieldType.DATETIME.code());
        assertEquals(15, Nervix.FieldType.FIXED_LIST.code());
        assertEquals(16, Nervix.FieldType.LIST.code());

        assertArrayEquals(new Nervix.Part[] {
            Nervix.Part.ROWS, Nervix.Part.BRANCH
        }, Nervix.Part.values());
        assertEquals(1, Nervix.Part.ROWS.code());
        assertEquals(2, Nervix.Part.BRANCH.code());

        assertArrayEquals(new Nervix.Disposition[] {
            Nervix.Disposition.COMPLETED, Nervix.Disposition.FAILED, Nervix.Disposition.NOT_LEADER, Nervix.Disposition.TRANSACTION_DETACHED, Nervix.Disposition.TRANSACTION_TAKEN_OVER, Nervix.Disposition.OUTCOME_UNKNOWN, Nervix.Disposition.EXECUTION_REFERENCE_CONFLICT, Nervix.Disposition.EXECUTION_REFERENCE_EXPIRED, Nervix.Disposition.PREVIEW_STALE
        }, Nervix.Disposition.values());
        assertEquals(1, Nervix.Disposition.COMPLETED.code());
        assertEquals(2, Nervix.Disposition.FAILED.code());
        assertEquals(3, Nervix.Disposition.NOT_LEADER.code());
        assertEquals(4, Nervix.Disposition.TRANSACTION_DETACHED.code());
        assertEquals(5, Nervix.Disposition.TRANSACTION_TAKEN_OVER.code());
        assertEquals(6, Nervix.Disposition.OUTCOME_UNKNOWN.code());
        assertEquals(7, Nervix.Disposition.EXECUTION_REFERENCE_CONFLICT.code());
        assertEquals(8, Nervix.Disposition.EXECUTION_REFERENCE_EXPIRED.code());
        assertEquals(9, Nervix.Disposition.PREVIEW_STALE.code());

        assertArrayEquals(new Nervix.UploadFailure[] {
            Nervix.UploadFailure.INVALID_STREAM, Nervix.UploadFailure.RESOURCE_NOT_DECLARED, Nervix.UploadFailure.SIZE_MISMATCH, Nervix.UploadFailure.QUOTA_EXCEEDED, Nervix.UploadFailure.INSTALLATION_FAILED, Nervix.UploadFailure.IDENTITY_CONFLICT
        }, Nervix.UploadFailure.values());
        assertEquals(1, Nervix.UploadFailure.INVALID_STREAM.code());
        assertEquals(2, Nervix.UploadFailure.RESOURCE_NOT_DECLARED.code());
        assertEquals(3, Nervix.UploadFailure.SIZE_MISMATCH.code());
        assertEquals(4, Nervix.UploadFailure.QUOTA_EXCEEDED.code());
        assertEquals(5, Nervix.UploadFailure.INSTALLATION_FAILED.code());
        assertEquals(6, Nervix.UploadFailure.IDENTITY_CONFLICT.code());

        assertArrayEquals(new Nervix.EventKind[] {
            Nervix.EventKind.ROWS, Nervix.EventKind.DELIVERY_LOST, Nervix.EventKind.ROWS_SKIPPED, Nervix.EventKind.ENDED, Nervix.EventKind.INTERRUPTED, Nervix.EventKind.CONSUMER_OVERFLOW, Nervix.EventKind.RESTORATION_FAILED
        }, Nervix.EventKind.values());
        assertEquals(1, Nervix.EventKind.ROWS.code());
        assertEquals(2, Nervix.EventKind.DELIVERY_LOST.code());
        assertEquals(3, Nervix.EventKind.ROWS_SKIPPED.code());
        assertEquals(4, Nervix.EventKind.ENDED.code());
        assertEquals(5, Nervix.EventKind.INTERRUPTED.code());
        assertEquals(6, Nervix.EventKind.CONSUMER_OVERFLOW.code());
        assertEquals(7, Nervix.EventKind.RESTORATION_FAILED.code());

        assertArrayEquals(new Nervix.ClockEventKind[] {
            Nervix.ClockEventKind.STATE, Nervix.ClockEventKind.TICK, Nervix.ClockEventKind.ENDED, Nervix.ClockEventKind.INTERRUPTED, Nervix.ClockEventKind.RESTORATION_FAILED
        }, Nervix.ClockEventKind.values());
        assertEquals(1, Nervix.ClockEventKind.STATE.code());
        assertEquals(2, Nervix.ClockEventKind.TICK.code());
        assertEquals(3, Nervix.ClockEventKind.ENDED.code());
        assertEquals(4, Nervix.ClockEventKind.INTERRUPTED.code());
        assertEquals(5, Nervix.ClockEventKind.RESTORATION_FAILED.code());

        assertArrayEquals(new Nervix.ClockState[] {
            Nervix.ClockState.STOPPED, Nervix.ClockState.UNINSTALLED, Nervix.ClockState.UNPACED, Nervix.ClockState.PACED
        }, Nervix.ClockState.values());
        assertEquals(1, Nervix.ClockState.STOPPED.code());
        assertEquals(2, Nervix.ClockState.UNINSTALLED.code());
        assertEquals(3, Nervix.ClockState.UNPACED.code());
        assertEquals(4, Nervix.ClockState.PACED.code());

        assertArrayEquals(new Nervix.EndReason[] {
            Nervix.EndReason.RELAY_REMOVED, Nervix.EndReason.RELAY_CHANGED
        }, Nervix.EndReason.values());
        assertEquals(1, Nervix.EndReason.RELAY_REMOVED.code());
        assertEquals(2, Nervix.EndReason.RELAY_CHANGED.code());

        assertArrayEquals(new Nervix.RestorationCause[] {
            Nervix.RestorationCause.IN_TRANSACTION, Nervix.RestorationCause.INVALID_STATEMENT, Nervix.RestorationCause.NAME_IN_USE, Nervix.RestorationCause.INVALID_FILTER, Nervix.RestorationCause.UNAVAILABLE, Nervix.RestorationCause.REJECTED, Nervix.RestorationCause.FAILED
        }, Nervix.RestorationCause.values());
        assertEquals(1, Nervix.RestorationCause.IN_TRANSACTION.code());
        assertEquals(2, Nervix.RestorationCause.INVALID_STATEMENT.code());
        assertEquals(3, Nervix.RestorationCause.NAME_IN_USE.code());
        assertEquals(4, Nervix.RestorationCause.INVALID_FILTER.code());
        assertEquals(5, Nervix.RestorationCause.UNAVAILABLE.code());
        assertEquals(6, Nervix.RestorationCause.REJECTED.code());
        assertEquals(7, Nervix.RestorationCause.FAILED.code());

        assertArrayEquals(new Nervix.ClockEndReason[] {
            Nervix.ClockEndReason.DOMAIN_REMOVED
        }, Nervix.ClockEndReason.values());
        assertEquals(1, Nervix.ClockEndReason.DOMAIN_REMOVED.code());

        assertArrayEquals(new Nervix.EndpointState[] {
            Nervix.EndpointState.ACTIVE, Nervix.EndpointState.INTERRUPTED, Nervix.EndpointState.RESTORING, Nervix.EndpointState.REOPEN_REQUIRED, Nervix.EndpointState.CLOSED
        }, Nervix.EndpointState.values());
        assertEquals(1, Nervix.EndpointState.ACTIVE.code());
        assertEquals(2, Nervix.EndpointState.INTERRUPTED.code());
        assertEquals(3, Nervix.EndpointState.RESTORING.code());
        assertEquals(4, Nervix.EndpointState.REOPEN_REQUIRED.code());
        assertEquals(5, Nervix.EndpointState.CLOSED.code());

        assertArrayEquals(new Nervix.Admission[] {
            Nervix.Admission.OPEN, Nervix.Admission.SUSPENDED
        }, Nervix.Admission.values());
        assertEquals(1, Nervix.Admission.OPEN.code());
        assertEquals(2, Nervix.Admission.SUSPENDED.code());

        assertArrayEquals(new Nervix.WindowKind[] {
            Nervix.WindowKind.SEQUENTIAL, Nervix.WindowKind.PARALLEL
        }, Nervix.WindowKind.values());
        assertEquals(1, Nervix.WindowKind.SEQUENTIAL.code());
        assertEquals(2, Nervix.WindowKind.PARALLEL.code());

        assertArrayEquals(new Nervix.Settlement[] {
            Nervix.Settlement.CONFIRMED, Nervix.Settlement.STALE_REFERENCE, Nervix.Settlement.WRONG_CONSUMER, Nervix.Settlement.INVALID_REASON, Nervix.Settlement.CONSUMER_ENDED
        }, Nervix.Settlement.values());
        assertEquals(1, Nervix.Settlement.CONFIRMED.code());
        assertEquals(2, Nervix.Settlement.STALE_REFERENCE.code());
        assertEquals(3, Nervix.Settlement.WRONG_CONSUMER.code());
        assertEquals(4, Nervix.Settlement.INVALID_REASON.code());
        assertEquals(5, Nervix.Settlement.CONSUMER_ENDED.code());

        assertArrayEquals(new Nervix.OpenRefusal[] {
            Nervix.OpenRefusal.DOMAIN_NOT_FOUND, Nervix.OpenRefusal.DOMAIN_STOPPED, Nervix.OpenRefusal.ENDPOINT_NOT_FOUND, Nervix.OpenRefusal.NOT_CLIENT_ENDPOINT, Nervix.OpenRefusal.ENDPOINT_UNAVAILABLE, Nervix.OpenRefusal.SCHEMA_MISMATCH, Nervix.OpenRefusal.TOO_MANY_ENDPOINTS, Nervix.OpenRefusal.SESSION_CAPACITY_EXHAUSTED, Nervix.OpenRefusal.NODE_CAPACITY_EXHAUSTED, Nervix.OpenRefusal.INVALID_LIMITS, Nervix.OpenRefusal.IN_TRANSACTION
        }, Nervix.OpenRefusal.values());
        assertEquals(1, Nervix.OpenRefusal.DOMAIN_NOT_FOUND.code());
        assertEquals(2, Nervix.OpenRefusal.DOMAIN_STOPPED.code());
        assertEquals(3, Nervix.OpenRefusal.ENDPOINT_NOT_FOUND.code());
        assertEquals(4, Nervix.OpenRefusal.NOT_CLIENT_ENDPOINT.code());
        assertEquals(5, Nervix.OpenRefusal.ENDPOINT_UNAVAILABLE.code());
        assertEquals(6, Nervix.OpenRefusal.SCHEMA_MISMATCH.code());
        assertEquals(7, Nervix.OpenRefusal.TOO_MANY_ENDPOINTS.code());
        assertEquals(8, Nervix.OpenRefusal.SESSION_CAPACITY_EXHAUSTED.code());
        assertEquals(9, Nervix.OpenRefusal.NODE_CAPACITY_EXHAUSTED.code());
        assertEquals(10, Nervix.OpenRefusal.INVALID_LIMITS.code());
        assertEquals(11, Nervix.OpenRefusal.IN_TRANSACTION.code());

        assertArrayEquals(new Nervix.ReopenReason[] {
            Nervix.ReopenReason.DOMAIN_STOPPED, Nervix.ReopenReason.ENDPOINT_REMOVED, Nervix.ReopenReason.SCHEMA_CHANGED, Nervix.ReopenReason.CONTRACT_CHANGED, Nervix.ReopenReason.GENERATION_CHANGED, Nervix.ReopenReason.PROTOCOL_VIOLATED, Nervix.ReopenReason.REFUSED
        }, Nervix.ReopenReason.values());
        assertEquals(1, Nervix.ReopenReason.DOMAIN_STOPPED.code());
        assertEquals(2, Nervix.ReopenReason.ENDPOINT_REMOVED.code());
        assertEquals(3, Nervix.ReopenReason.SCHEMA_CHANGED.code());
        assertEquals(4, Nervix.ReopenReason.CONTRACT_CHANGED.code());
        assertEquals(5, Nervix.ReopenReason.GENERATION_CHANGED.code());
        assertEquals(6, Nervix.ReopenReason.PROTOCOL_VIOLATED.code());
        assertEquals(7, Nervix.ReopenReason.REFUSED.code());

        assertArrayEquals(new Nervix.SubmissionResult[] {
            Nervix.SubmissionResult.NOT_ADMITTED, Nervix.SubmissionResult.COMPLETED, Nervix.SubmissionResult.PROCESSING_FAILED, Nervix.SubmissionResult.OUTCOME_UNKNOWN
        }, Nervix.SubmissionResult.values());
        assertEquals(1, Nervix.SubmissionResult.NOT_ADMITTED.code());
        assertEquals(2, Nervix.SubmissionResult.COMPLETED.code());
        assertEquals(3, Nervix.SubmissionResult.PROCESSING_FAILED.code());
        assertEquals(4, Nervix.SubmissionResult.OUTCOME_UNKNOWN.code());

        assertArrayEquals(new Nervix.SubmissionRefusal[] {
            Nervix.SubmissionRefusal.INVALID_BATCH, Nervix.SubmissionRefusal.SUSPENDED, Nervix.SubmissionRefusal.BUSY, Nervix.SubmissionRefusal.DRAINING, Nervix.SubmissionRefusal.PRODUCER_ENDED, Nervix.SubmissionRefusal.CREDIT_EXCEEDED
        }, Nervix.SubmissionRefusal.values());
        assertEquals(1, Nervix.SubmissionRefusal.INVALID_BATCH.code());
        assertEquals(2, Nervix.SubmissionRefusal.SUSPENDED.code());
        assertEquals(3, Nervix.SubmissionRefusal.BUSY.code());
        assertEquals(4, Nervix.SubmissionRefusal.DRAINING.code());
        assertEquals(5, Nervix.SubmissionRefusal.PRODUCER_ENDED.code());
        assertEquals(6, Nervix.SubmissionRefusal.CREDIT_EXCEEDED.code());

        assertArrayEquals(new Nervix.BatchDefect[] {
            Nervix.BatchDefect.MALFORMED, Nervix.BatchDefect.UNEXPECTED_MESSAGE, Nervix.BatchDefect.COMPRESSED, Nervix.BatchDefect.SCHEMA_MISMATCH, Nervix.BatchDefect.NOT_ONE_BATCH, Nervix.BatchDefect.TOO_MANY_ROWS, Nervix.BatchDefect.TOO_LARGE, Nervix.BatchDefect.INVALID_DATA
        }, Nervix.BatchDefect.values());
        assertEquals(1, Nervix.BatchDefect.MALFORMED.code());
        assertEquals(2, Nervix.BatchDefect.UNEXPECTED_MESSAGE.code());
        assertEquals(3, Nervix.BatchDefect.COMPRESSED.code());
        assertEquals(4, Nervix.BatchDefect.SCHEMA_MISMATCH.code());
        assertEquals(5, Nervix.BatchDefect.NOT_ONE_BATCH.code());
        assertEquals(6, Nervix.BatchDefect.TOO_MANY_ROWS.code());
        assertEquals(7, Nervix.BatchDefect.TOO_LARGE.code());
        assertEquals(8, Nervix.BatchDefect.INVALID_DATA.code());

        assertArrayEquals(new Nervix.ProcessingFailure[] {
            Nervix.ProcessingFailure.ACK_TIMED_OUT, Nervix.ProcessingFailure.REJECTED
        }, Nervix.ProcessingFailure.values());
        assertEquals(1, Nervix.ProcessingFailure.ACK_TIMED_OUT.code());
        assertEquals(2, Nervix.ProcessingFailure.REJECTED.code());

        assertArrayEquals(new Nervix.Uncertainty[] {
            Nervix.Uncertainty.INTERRUPTED, Nervix.Uncertainty.OWNER_LOST, Nervix.Uncertainty.SESSION_LOST
        }, Nervix.Uncertainty.values());
        assertEquals(1, Nervix.Uncertainty.INTERRUPTED.code());
        assertEquals(2, Nervix.Uncertainty.OWNER_LOST.code());
        assertEquals(3, Nervix.Uncertainty.SESSION_LOST.code());

        assertArrayEquals(new Nervix.CompletionStatus[] {
            Nervix.CompletionStatus.READY, Nervix.CompletionStatus.MISSING_CONTEXT, Nervix.CompletionStatus.STALE_CONTEXT, Nervix.CompletionStatus.LOOKUP_FAILED
        }, Nervix.CompletionStatus.values());
        assertEquals(1, Nervix.CompletionStatus.READY.code());
        assertEquals(2, Nervix.CompletionStatus.MISSING_CONTEXT.code());
        assertEquals(3, Nervix.CompletionStatus.STALE_CONTEXT.code());
        assertEquals(4, Nervix.CompletionStatus.LOOKUP_FAILED.code());

        assertArrayEquals(new Nervix.CompletionKind[] {
            Nervix.CompletionKind.TEXT, Nervix.CompletionKind.LOCAL_DIRECTORY_LOOKUP
        }, Nervix.CompletionKind.values());
        assertEquals(1, Nervix.CompletionKind.TEXT.code());
        assertEquals(2, Nervix.CompletionKind.LOCAL_DIRECTORY_LOOKUP.code());

        assertArrayEquals(new Nervix.FailureKind[] {
            Nervix.FailureKind.INVALID_ARGUMENT, Nervix.FailureKind.CONNECT, Nervix.FailureKind.TRANSPORT, Nervix.FailureKind.UNCERTAIN, Nervix.FailureKind.REJECTED, Nervix.FailureKind.DEADLINE, Nervix.FailureKind.CANCELLED, Nervix.FailureKind.OVERFLOW, Nervix.FailureKind.PROTOCOL, Nervix.FailureKind.TYPE, Nervix.FailureKind.CLOSED, Nervix.FailureKind.INTERRUPTED, Nervix.FailureKind.REOPEN_REQUIRED
        }, Nervix.FailureKind.values());
        assertEquals(1, Nervix.FailureKind.INVALID_ARGUMENT.code());
        assertEquals(2, Nervix.FailureKind.CONNECT.code());
        assertEquals(3, Nervix.FailureKind.TRANSPORT.code());
        assertEquals(4, Nervix.FailureKind.UNCERTAIN.code());
        assertEquals(5, Nervix.FailureKind.REJECTED.code());
        assertEquals(6, Nervix.FailureKind.DEADLINE.code());
        assertEquals(7, Nervix.FailureKind.CANCELLED.code());
        assertEquals(8, Nervix.FailureKind.OVERFLOW.code());
        assertEquals(9, Nervix.FailureKind.PROTOCOL.code());
        assertEquals(10, Nervix.FailureKind.TYPE.code());
        assertEquals(11, Nervix.FailureKind.CLOSED.code());
        assertEquals(12, Nervix.FailureKind.INTERRUPTED.code());
        assertEquals(13, Nervix.FailureKind.REOPEN_REQUIRED.code());

    }

    @Test
    void recordsExposeTheirComponentsAndUseJavaValueEquality() {
        // ANCHOR: records
        var tick = new Nervix.Tick(2, 3, 400, 500, 600);
        assertEquals(new Nervix.Tick(2, 3, 400, 500, 600), tick);
        assertEquals(2, tick.generation());
        assertEquals(3, tick.id());
        assertEquals(400, tick.logicalBoundary());
        assertEquals(500, tick.authorityUtc());
        assertEquals(600, tick.servingLogical());
        assertEquals(tick.hashCode(), new Nervix.Tick(2, 3, 400, 500, 600).hashCode());
        assertTrue(tick.toString().startsWith("Tick[generation=2"));
        // ANCHOR_END: records
        var subscription = new Nervix.Subscription("rows", -1L);
        assertEquals("rows", subscription.name());
        assertEquals("18446744073709551615", Long.toUnsignedString(subscription.generation()));
        assertEquals(subscription, new Nervix.Subscription("rows", -1L));
        var level = new Nervix.FieldLevel(Nervix.FieldType.FIXED_LIST, 2);
        assertEquals(Nervix.FieldType.FIXED_LIST, level.type());
        assertEquals(2, level.length());
        var failure = new Nervix.RestorationFailure(Nervix.RestorationCause.UNAVAILABLE, "unavailable", 10);
        assertEquals(Nervix.RestorationCause.UNAVAILABLE, failure.cause());
        assertEquals("unavailable", failure.message());
        assertEquals(10, failure.waitNanos());
        var diagnostic = new Nervix.Diagnostic("expected a statement", new long[] {0, 1});
        assertEquals("expected a statement", diagnostic.message());
        assertArrayEquals(new long[] {0, 1}, diagnostic.span());
        var backup = new Nervix.Backup(15, new byte[] {1, 2});
        assertEquals(15, backup.bytes());
        assertArrayEquals(new byte[] {1, 2}, backup.digest());
        var restore = new Nervix.Restore(true, 2, 3, 4, false);
        assertTrue(restore.dryRun());
        assertEquals(2, restore.domains());
        assertEquals(3, restore.resourceVersions());
        assertEquals(4, restore.models());
        assertFalse(restore.failed());
        var mapping = new Nervix.PacedClock(1, 100, 10, 1000, 2000, 2.0);
        assertEquals(1, mapping.generation());
        assertEquals(100, mapping.periodNanos());
        assertEquals(10, mapping.skewNanos());
        assertEquals(1000, mapping.logicalOrigin());
        assertEquals(2000, mapping.utcAnchor());
        assertEquals(2.0, mapping.rate());
        var window = new Nervix.AdmissionWindow(10, 20);
        assertEquals(10, window.earliestCenter());
        assertEquals(20, window.latestCenter());
        var reopen = new Nervix.Reopen(Nervix.ReopenReason.REFUSED, Nervix.OpenRefusal.DOMAIN_STOPPED);
        assertEquals(Nervix.ReopenReason.REFUSED, reopen.reason());
        assertEquals(Nervix.OpenRefusal.DOMAIN_STOPPED, reopen.refusal());
        var bytes = new Nervix.Varlen(new long[] {0, 2}, new byte[] {0, 1});
        assertArrayEquals(new long[] {0, 2}, bytes.offsets());
        assertArrayEquals(new byte[] {0, 1}, bytes.data());
        var pending = new Nervix.Pending(42, null);
        assertEquals(42, pending.id());
        assertNull(pending.outcome());
    }

    @Test
    void everyTypedFailureIsAClientExceptionWithItsClassification() {
        {
            Nervix.ClientException failure = new Nervix.InvalidArgumentException("cause", "execution", Nervix.OpenRefusal.DOMAIN_STOPPED.code());
            assertEquals(Nervix.FailureKind.INVALID_ARGUMENT, failure.kind());
            assertEquals("cause", failure.getMessage());
            assertEquals("execution", failure.executionReference());
            assertEquals(Nervix.OpenRefusal.DOMAIN_STOPPED, failure.openRefusal());
        }
        {
            Nervix.ClientException failure = new Nervix.ConnectException("cause", "execution", Nervix.OpenRefusal.DOMAIN_STOPPED.code());
            assertEquals(Nervix.FailureKind.CONNECT, failure.kind());
            assertEquals("cause", failure.getMessage());
            assertEquals("execution", failure.executionReference());
            assertEquals(Nervix.OpenRefusal.DOMAIN_STOPPED, failure.openRefusal());
        }
        {
            Nervix.ClientException failure = new Nervix.TransportException("cause", "execution", Nervix.OpenRefusal.DOMAIN_STOPPED.code());
            assertEquals(Nervix.FailureKind.TRANSPORT, failure.kind());
            assertEquals("cause", failure.getMessage());
            assertEquals("execution", failure.executionReference());
            assertEquals(Nervix.OpenRefusal.DOMAIN_STOPPED, failure.openRefusal());
        }
        {
            Nervix.ClientException failure = new Nervix.UncertainOutcomeException("cause", "execution", Nervix.OpenRefusal.DOMAIN_STOPPED.code());
            assertEquals(Nervix.FailureKind.UNCERTAIN, failure.kind());
            assertEquals("cause", failure.getMessage());
            assertEquals("execution", failure.executionReference());
            assertEquals(Nervix.OpenRefusal.DOMAIN_STOPPED, failure.openRefusal());
        }
        {
            Nervix.ClientException failure = new Nervix.RejectedException("cause", "execution", Nervix.OpenRefusal.DOMAIN_STOPPED.code());
            assertEquals(Nervix.FailureKind.REJECTED, failure.kind());
            assertEquals("cause", failure.getMessage());
            assertEquals("execution", failure.executionReference());
            assertEquals(Nervix.OpenRefusal.DOMAIN_STOPPED, failure.openRefusal());
        }
        {
            Nervix.ClientException failure = new Nervix.DeadlineException("cause", "execution", Nervix.OpenRefusal.DOMAIN_STOPPED.code());
            assertEquals(Nervix.FailureKind.DEADLINE, failure.kind());
            assertEquals("cause", failure.getMessage());
            assertEquals("execution", failure.executionReference());
            assertEquals(Nervix.OpenRefusal.DOMAIN_STOPPED, failure.openRefusal());
        }
        {
            Nervix.ClientException failure = new Nervix.CancelledException("cause", "execution", Nervix.OpenRefusal.DOMAIN_STOPPED.code());
            assertEquals(Nervix.FailureKind.CANCELLED, failure.kind());
            assertEquals("cause", failure.getMessage());
            assertEquals("execution", failure.executionReference());
            assertEquals(Nervix.OpenRefusal.DOMAIN_STOPPED, failure.openRefusal());
        }
        {
            Nervix.ClientException failure = new Nervix.EventOverflowException("cause", "execution", Nervix.OpenRefusal.DOMAIN_STOPPED.code());
            assertEquals(Nervix.FailureKind.OVERFLOW, failure.kind());
            assertEquals("cause", failure.getMessage());
            assertEquals("execution", failure.executionReference());
            assertEquals(Nervix.OpenRefusal.DOMAIN_STOPPED, failure.openRefusal());
        }
        {
            Nervix.ClientException failure = new Nervix.ProtocolException("cause", "execution", Nervix.OpenRefusal.DOMAIN_STOPPED.code());
            assertEquals(Nervix.FailureKind.PROTOCOL, failure.kind());
            assertEquals("cause", failure.getMessage());
            assertEquals("execution", failure.executionReference());
            assertEquals(Nervix.OpenRefusal.DOMAIN_STOPPED, failure.openRefusal());
        }
        {
            Nervix.ClientException failure = new Nervix.TypeMismatchException("cause", "execution", Nervix.OpenRefusal.DOMAIN_STOPPED.code());
            assertEquals(Nervix.FailureKind.TYPE, failure.kind());
            assertEquals("cause", failure.getMessage());
            assertEquals("execution", failure.executionReference());
            assertEquals(Nervix.OpenRefusal.DOMAIN_STOPPED, failure.openRefusal());
        }
        {
            Nervix.ClientException failure = new Nervix.ClosedException("cause", "execution", Nervix.OpenRefusal.DOMAIN_STOPPED.code());
            assertEquals(Nervix.FailureKind.CLOSED, failure.kind());
            assertEquals("cause", failure.getMessage());
            assertEquals("execution", failure.executionReference());
            assertEquals(Nervix.OpenRefusal.DOMAIN_STOPPED, failure.openRefusal());
        }
        {
            Nervix.ClientException failure = new Nervix.ConsumerInterruptedException("cause", "execution", Nervix.OpenRefusal.DOMAIN_STOPPED.code());
            assertEquals(Nervix.FailureKind.INTERRUPTED, failure.kind());
            assertEquals("cause", failure.getMessage());
            assertEquals("execution", failure.executionReference());
            assertEquals(Nervix.OpenRefusal.DOMAIN_STOPPED, failure.openRefusal());
        }
        {
            Nervix.ClientException failure = new Nervix.ReopenRequiredException("cause", "execution", Nervix.OpenRefusal.DOMAIN_STOPPED.code());
            assertEquals(Nervix.FailureKind.REOPEN_REQUIRED, failure.kind());
            assertEquals("cause", failure.getMessage());
            assertEquals("execution", failure.executionReference());
            assertEquals(Nervix.OpenRefusal.DOMAIN_STOPPED, failure.openRefusal());
        }
    }
}
