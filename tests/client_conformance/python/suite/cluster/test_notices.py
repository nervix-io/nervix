"""Server notices: a row a junction fails reaches the session as a runtime notice that carries the
error's class and the junction's kind and name beside its display text, a reader routes notices by
their kind, and a notice wait ends at its deadline and by cancellation. A test that provokes a
notice creates its own graph and drops it when it ends."""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator

import pytest

import nervix_client as nx
from support import WAIT_SECONDS, Cluster, Row, build, completed, deadline, input_fields


@pytest.fixture
def scoring(session: nx.Session, unique: str) -> Iterator[str]:
    """A client ingestor whose rows reach a junction that fails every row it reads and logs the
    failure, which the server publishes to every session as a runtime notice. Yields the prefix of
    the graph's names, and drops the graph when the test ends."""

    prefix = f"notices_{unique}"
    completed(session, f"CREATE RELAY {prefix}_readings SCHEMA suite_row UNBRANCHED;")
    completed(session, f"CREATE RELAY {prefix}_ratios SCHEMA suite_row UNBRANCHED;")
    # The divisor is always zero, so the assignment fails every row with an evaluation error.
    completed(
        session,
        f"CREATE JUNCTION {prefix}_scorer FROM {prefix}_readings UNBRANCHED "
        f"TO {prefix}_ratios INHERIT ALL SET amount = input.amount / (input.amount - input.amount) "
        "FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
    )
    completed(
        session,
        f"CREATE CLIENT PRODUCER INGESTOR {prefix}_in FROM SCHEMA suite_row "
        "MODE ACK SEQUENTIAL ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s ON QUIESCE SUSPEND "
        f"TIMESTAMP NOW TO {prefix}_readings INHERIT ALL UNBRANCHED FLUSH IMMEDIATE "
        "ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
    )
    yield prefix
    completed(session, f"DROP INGESTOR {prefix}_in;")
    completed(session, f"DROP JUNCTION {prefix}_scorer;")
    completed(session, f"DROP RELAY {prefix}_ratios;")
    completed(session, f"DROP RELAY {prefix}_readings;")


# ANCHOR: route_notices
def routed(notice: nx.Notice, log: list[str]) -> None:
    """Routes one notice by its kind; its text is only ever shown."""

    if notice.kind == nx.NoticeKind.RUNTIME and notice.error_class is not None:
        # An error of a graph node: select it by its class and the node that produced it.
        log.append(f"{notice.error_node_kind} {notice.error_node} failed: {notice.error_class}")
    elif notice.kind in (nx.NoticeKind.MEMBERSHIP, nx.NoticeKind.LEADERSHIP):
        log.append(f"cluster: {notice.message}")
    else:
        # Cluster parameters, control-plane and licensing notices, and runtime failures that are
        # no error of a graph node.
        log.append(f"{notice.kind!r} {notice.level!r}: {notice.message}")
# ANCHOR_END: route_notices


def test_a_failed_row_reaches_the_session_as_a_runtime_notice_with_its_class_and_node(
    cluster: Cluster, session: nx.Session, scoring: str
) -> None:
    domain = cluster.domain
    scorer = f"{scoring}_scorer"
    with session.open_ingestor(
        domain, f"{scoring}_in", input_fields(), batches=1, bytes=1 << 20, cancel=deadline()
    ) as producer:
        submission = producer.submit(build(producer.schema, [Row(301)]), cancel=deadline())
        # The junction failed the row, and its ON MESSAGE ERROR LOG logged the failure and refused
        # the row's acknowledgement, so the batch failed processing.
        outcome = producer.rejoin(submission, cancel=deadline())
        assert outcome.result == nx.SubmissionResult.PROCESSING_FAILED
        assert outcome.failure == nx.ProcessingFailure.REJECTED
        assert outcome.message
    log: list[str] = []
    # ANCHOR: read_notice
    cancel = nx.Cancel(deadline=120)
    while True:
        notice = session.next_notice(cancel=cancel)
        routed(notice, log)
        # Other notices, such as a membership change or another node's error, may come first.
        if notice.error_node == scorer:
            break
    assert notice.kind == nx.NoticeKind.RUNTIME
    assert notice.level == nx.NoticeLevel.ERROR
    assert notice.error_class == "evaluation"
    assert notice.error_node_kind == "junction"
    # ANCHOR_END: read_notice
    assert f"junction '{scorer}' message error in domain '{domain}'" in notice.message
    assert log[-1] == f"junction {scorer} failed: evaluation"


def test_a_notice_wait_ends_at_its_deadline_and_by_cancellation(session: nx.Session) -> None:
    # A notice the cluster published since the session opened may still be waiting, so the wait is
    # repeated until a deadline passes with none.
    ends = time.monotonic() + WAIT_SECONDS
    while True:
        try:
            session.next_notice(cancel=nx.Cancel(deadline=0.2))
        except nx.DeadlineError:
            break
        assert time.monotonic() < ends, f"notices kept arriving for {WAIT_SECONDS} seconds"
    cancel = nx.Cancel()
    failures: list[nx.ClientError] = []

    def wait() -> None:
        # A notice that arrives before the token is triggered is read; every read after it ends.
        while True:
            try:
                session.next_notice(cancel=cancel)
            except nx.ClientError as failure:
                failures.append(failure)
                return

    waiter = threading.Thread(target=wait)
    waiter.start()
    cancel.trigger()
    waiter.join(timeout=WAIT_SECONDS)
    assert not waiter.is_alive(), f"a cancelled notice wait went on for {WAIT_SECONDS} seconds"
    assert isinstance(failures[0], nx.CancelledError), failures
