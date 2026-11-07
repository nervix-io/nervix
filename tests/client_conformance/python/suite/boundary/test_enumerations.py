"""Every enumeration member keeps the stable value the host session runtime gives it, compares and
hashes as itself, and reads as its class and name; every failure kind has its exception class under
`ClientError`."""

from __future__ import annotations

from typing import SupportsInt

import nervix_client as nx

STABLE_VALUES: tuple[tuple[SupportsInt, int], ...] = (
    (nx.FailureKind.INVALID_ARGUMENT, 1),
    (nx.FailureKind.CONNECT, 2),
    (nx.FailureKind.TRANSPORT, 3),
    (nx.FailureKind.UNCERTAIN, 4),
    (nx.FailureKind.REJECTED, 5),
    (nx.FailureKind.DEADLINE, 6),
    (nx.FailureKind.CANCELLED, 7),
    (nx.FailureKind.OVERFLOW, 8),
    (nx.FailureKind.PROTOCOL, 9),
    (nx.FailureKind.TYPE, 10),
    (nx.FailureKind.CLOSED, 11),
    (nx.FailureKind.INTERRUPTED, 12),
    (nx.FailureKind.REOPEN_REQUIRED, 13),
    (nx.Disposition.COMPLETED, 1),
    (nx.Disposition.FAILED, 2),
    (nx.Disposition.NOT_LEADER, 3),
    (nx.Disposition.TRANSACTION_DETACHED, 4),
    (nx.Disposition.TRANSACTION_TAKEN_OVER, 5),
    (nx.Disposition.OUTCOME_UNKNOWN, 6),
    (nx.Disposition.EXECUTION_REFERENCE_CONFLICT, 7),
    (nx.Disposition.EXECUTION_REFERENCE_EXPIRED, 8),
    (nx.Disposition.PREVIEW_STALE, 9),
    (nx.EventKind.ROWS, 1),
    (nx.EventKind.DELIVERY_LOST, 2),
    (nx.EventKind.ROWS_SKIPPED, 3),
    (nx.EventKind.ENDED, 4),
    (nx.EventKind.INTERRUPTED, 5),
    (nx.EventKind.CONSUMER_OVERFLOW, 6),
    (nx.EventKind.RESTORATION_FAILED, 7),
    (nx.EndReason.RELAY_REMOVED, 1),
    (nx.EndReason.RELAY_CHANGED, 2),
    (nx.RestorationCause.IN_TRANSACTION, 1),
    (nx.RestorationCause.INVALID_STATEMENT, 2),
    (nx.RestorationCause.NAME_IN_USE, 3),
    (nx.RestorationCause.INVALID_FILTER, 4),
    (nx.RestorationCause.UNAVAILABLE, 5),
    (nx.RestorationCause.REJECTED, 6),
    (nx.RestorationCause.FAILED, 7),
    (nx.CellState.VALUE, 1),
    (nx.CellState.NULL, 2),
    (nx.CellState.REDACTED, 3),
    (nx.FieldType.U8, 1),
    (nx.FieldType.I8, 2),
    (nx.FieldType.U16, 3),
    (nx.FieldType.I16, 4),
    (nx.FieldType.U32, 5),
    (nx.FieldType.I32, 6),
    (nx.FieldType.U64, 7),
    (nx.FieldType.I64, 8),
    (nx.FieldType.F32, 9),
    (nx.FieldType.F64, 10),
    (nx.FieldType.BOOL, 11),
    (nx.FieldType.STRING, 12),
    (nx.FieldType.BYTES, 13),
    (nx.FieldType.DATETIME, 14),
    (nx.FieldType.FIXED_LIST, 15),
    (nx.FieldType.LIST, 16),
    (nx.Part.ROWS, 1),
    (nx.Part.BRANCH_KEY, 2),
    (nx.ClockEventKind.STATE, 1),
    (nx.ClockEventKind.TICK, 2),
    (nx.ClockEventKind.ENDED, 3),
    (nx.ClockEventKind.INTERRUPTED, 4),
    (nx.ClockEventKind.RESTORATION_FAILED, 5),
    (nx.ClockState.STOPPED, 1),
    (nx.ClockState.UNINSTALLED, 2),
    (nx.ClockState.UNPACED, 3),
    (nx.ClockState.PACED, 4),
    (nx.ClockEndReason.DOMAIN_REMOVED, 1),
    (nx.EndpointState.ACTIVE, 1),
    (nx.EndpointState.INTERRUPTED, 2),
    (nx.EndpointState.RESTORING, 3),
    (nx.EndpointState.REOPEN_REQUIRED, 4),
    (nx.EndpointState.CLOSED, 5),
    (nx.OpenRefusal.DOMAIN_NOT_FOUND, 1),
    (nx.OpenRefusal.DOMAIN_STOPPED, 2),
    (nx.OpenRefusal.ENDPOINT_NOT_FOUND, 3),
    (nx.OpenRefusal.NOT_CLIENT_ENDPOINT, 4),
    (nx.OpenRefusal.ENDPOINT_UNAVAILABLE, 5),
    (nx.OpenRefusal.SCHEMA_MISMATCH, 6),
    (nx.OpenRefusal.TOO_MANY_ENDPOINTS, 7),
    (nx.OpenRefusal.SESSION_CAPACITY_EXHAUSTED, 8),
    (nx.OpenRefusal.NODE_CAPACITY_EXHAUSTED, 9),
    (nx.OpenRefusal.INVALID_LIMITS, 10),
    (nx.OpenRefusal.IN_TRANSACTION, 11),
    (nx.ReopenReason.DOMAIN_STOPPED, 1),
    (nx.ReopenReason.ENDPOINT_REMOVED, 2),
    (nx.ReopenReason.SCHEMA_CHANGED, 3),
    (nx.ReopenReason.CONTRACT_CHANGED, 4),
    (nx.ReopenReason.GENERATION_CHANGED, 5),
    (nx.ReopenReason.PROTOCOL_VIOLATED, 6),
    (nx.ReopenReason.REFUSED, 7),
    (nx.WindowKind.SEQUENTIAL, 1),
    (nx.WindowKind.PARALLEL, 2),
    (nx.Admission.OPEN, 1),
    (nx.Admission.SUSPENDED, 2),
    (nx.Settlement.CONFIRMED, 1),
    (nx.Settlement.STALE_REFERENCE, 2),
    (nx.Settlement.WRONG_CONSUMER, 3),
    (nx.Settlement.INVALID_REASON, 4),
    (nx.Settlement.CONSUMER_ENDED, 5),
    (nx.SubmissionResult.NOT_ADMITTED, 1),
    (nx.SubmissionResult.COMPLETED, 2),
    (nx.SubmissionResult.PROCESSING_FAILED, 3),
    (nx.SubmissionResult.OUTCOME_UNKNOWN, 4),
    (nx.SubmissionRefusal.INVALID_BATCH, 1),
    (nx.SubmissionRefusal.SUSPENDED, 2),
    (nx.SubmissionRefusal.BUSY, 3),
    (nx.SubmissionRefusal.DRAINING, 4),
    (nx.SubmissionRefusal.PRODUCER_ENDED, 5),
    (nx.SubmissionRefusal.CREDIT_EXCEEDED, 6),
    (nx.BatchDefect.MALFORMED, 1),
    (nx.BatchDefect.UNEXPECTED_MESSAGE, 2),
    (nx.BatchDefect.COMPRESSED, 3),
    (nx.BatchDefect.SCHEMA_MISMATCH, 4),
    (nx.BatchDefect.NOT_ONE_BATCH, 5),
    (nx.BatchDefect.TOO_MANY_ROWS, 6),
    (nx.BatchDefect.TOO_LARGE, 7),
    (nx.BatchDefect.INVALID_DATA, 8),
    (nx.ProcessingFailure.ACK_TIMED_OUT, 1),
    (nx.ProcessingFailure.REJECTED, 2),
    (nx.Uncertainty.INTERRUPTED, 1),
    (nx.Uncertainty.OWNER_LOST, 2),
    (nx.Uncertainty.SESSION_LOST, 3),
    (nx.CompletionStatus.READY, 1),
    (nx.CompletionStatus.MISSING_CONTEXT, 2),
    (nx.CompletionStatus.STALE_CONTEXT, 3),
    (nx.CompletionStatus.LOOKUP_FAILED, 4),
    (nx.CompletionKind.TEXT, 1),
    (nx.CompletionKind.LOCAL_DIRECTORY_LOOKUP, 2),
    (nx.Tls.PREFERRED, 1),
    (nx.Tls.REQUIRED, 2),
)


def test_every_enumeration_member_keeps_its_stable_value() -> None:
    for member, value in STABLE_VALUES:
        assert int(member) == value, (member, value)
        assert member == value, (member, value)


def test_a_member_compares_and_hashes_as_itself_and_reads_as_its_name() -> None:
    # ANCHOR: enumerations
    kind = nx.FailureKind.CONNECT
    assert kind == nx.FailureKind.CONNECT
    assert kind != nx.FailureKind.TRANSPORT
    assert repr(kind) == "FailureKind.CONNECT"
    handled = {nx.FailureKind.CONNECT: "retry later", nx.FailureKind.REJECTED: "fix the request"}
    assert handled[kind] == "retry later"
    # ANCHOR_END: enumerations
    assert len({member for member, _ in STABLE_VALUES}) == len(STABLE_VALUES)


def test_every_failure_kind_has_its_exception_class_under_client_error() -> None:
    classes = (
        nx.InvalidArgumentError,
        nx.ConnectError,
        nx.TransportError,
        nx.UncertainOutcomeError,
        nx.RejectedError,
        nx.DeadlineError,
        nx.CancelledError,
        nx.EventOverflowError,
        nx.ProtocolError,
        nx.TypeMismatchError,
        nx.ClosedError,
        nx.ConsumerInterruptedError,
        nx.ReopenRequiredError,
    )
    assert issubclass(nx.ClientError, Exception)
    for exception in classes:
        assert issubclass(exception, nx.ClientError), exception
    assert len(set(classes)) == 13
