"""Executing Plexus: prepared executions and their outcomes, diagnostics with spans, a command
cancelled before it was sent and one whose deadline passed, each recovered by executing the same
execution again, and one session shared by several threads."""

from __future__ import annotations

import threading

import pytest

import nervix_client as nx
from support import Cluster, completed, deadline, execute


def test_a_prepared_command_completes_under_its_execution_reference(
    session: nx.Session, unique: str
) -> None:
    # ANCHOR: execute
    execution = session.prepare(f"CREATE SCHEMA reading_{unique} ( value I64 );")
    outcome = session.execute(execution, cancel=nx.Cancel(deadline=60))
    assert outcome.disposition == nx.Disposition.COMPLETED
    assert outcome.execution_reference == execution.reference
    print(outcome.message)
    # ANCHOR_END: execute
    assert outcome.diagnostics == []
    # A command opens nothing, so what it would have opened reads as None.
    assert outcome.subscription is None
    assert outcome.schema is None
    assert outcome.backup is None
    assert outcome.restore is None


def test_a_failed_command_reports_its_diagnostics_with_their_spans(session: nx.Session) -> None:
    # ANCHOR: diagnostics
    outcome = session.execute(session.prepare("CREATE RELAY;"))
    assert outcome.disposition == nx.Disposition.FAILED
    diagnostic = outcome.diagnostics[0]
    assert diagnostic.span == (12, 12)
    print(diagnostic.message)
    # ANCHOR_END: diagnostics
    assert len(outcome.diagnostics) == 1
    assert diagnostic.message


def test_a_command_cancelled_before_it_was_sent_runs_once_when_executed_again(
    session: nx.Session, unique: str
) -> None:
    statement = f"CREATE SCHEMA cancelled_{unique} ( value I64 );"
    # ANCHOR: recover_cancelled
    execution = session.prepare(statement)
    cancel = nx.Cancel()
    cancel.trigger()
    with pytest.raises(nx.CancelledError) as cancelled:
        session.execute(execution, cancel=cancel)
    assert cancelled.value.execution_reference == execution.reference
    # Executing the same execution again runs the command once, or recovers its outcome.
    outcome = session.execute(execution)
    assert outcome.disposition == nx.Disposition.COMPLETED
    # ANCHOR_END: recover_cancelled
    # A fresh execution of the same statement is a second command, which finds the first's effect.
    assert execute(session, statement).disposition == nx.Disposition.FAILED


def test_a_command_whose_deadline_passed_is_recovered_by_executing_it_again(
    session: nx.Session, unique: str
) -> None:
    statement = f"CREATE SCHEMA expired_{unique} ( value I64 );"
    execution = session.prepare(statement)
    # The token's deadline passes before the call starts, so the call ends before anything is sent.
    with pytest.raises(nx.DeadlineError) as expired:
        session.execute(execution, cancel=nx.Cancel(deadline=0.000001))
    assert expired.value.execution_reference == execution.reference
    outcome = session.execute(execution, cancel=deadline())
    assert outcome.disposition == nx.Disposition.COMPLETED
    assert execute(session, statement).disposition == nx.Disposition.FAILED


def test_one_session_serves_several_threads_at_once(session: nx.Session, unique: str) -> None:
    # ANCHOR: threads
    outcomes: list[nx.Outcome] = []
    lock = threading.Lock()

    def create(index: int) -> None:
        execution = session.prepare(f"CREATE SCHEMA shared_{unique}_{index} ( value I64 );")
        outcome = session.execute(execution)
        with lock:
            outcomes.append(outcome)

    workers = [threading.Thread(target=create, args=(index,)) for index in range(8)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()
    assert all(outcome.disposition == nx.Disposition.COMPLETED for outcome in outcomes)
    # ANCHOR_END: threads
    assert len(outcomes) == 8


def test_a_wait_in_one_thread_leaves_another_thread_progressing(
    session: nx.Session, unique: str
) -> None:
    cancel = nx.Cancel()
    failures: list[nx.ClientError] = []
    started = threading.Event()

    def wait_for_an_event() -> None:
        started.set()
        try:
            session.next_event(cancel=cancel)
        except nx.ClientError as failure:
            failures.append(failure)

    waiter = threading.Thread(target=wait_for_an_event)
    waiter.start()
    started.wait()
    # The waiting thread released the GIL, so this one keeps using the same session.
    for index in range(3):
        completed(session, f"CREATE SCHEMA meanwhile_{unique}_{index} ( value I64 );")
    assert waiter.is_alive()
    cancel.trigger()
    waiter.join()
    assert isinstance(failures[0], nx.CancelledError)


def test_an_object_read_from_a_session_outlives_its_release(cluster: Cluster) -> None:
    with cluster.connect() as session:
        outcome = execute(session, "LIST DOMAINS;")
    with pytest.raises(nx.ClosedError):
        session.prepare("LIST DOMAINS;")
    assert outcome.disposition == nx.Disposition.COMPLETED
    assert cluster.domain in outcome.message
