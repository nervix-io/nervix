"""Cancellation tokens and deadlines: a token is triggered from any thread and ends every wait it
was passed to, and a deadline ends a wait on its own. A listener that accepts a connection and
never answers it holds a connect open until one of them ends it."""

from __future__ import annotations

import math
import threading

import pytest

import nervix_client as nx
from support import SilentListener, wait_until


def test_a_token_refuses_a_deadline_that_is_not_a_finite_number_of_seconds() -> None:
    for seconds in (-1.0, math.inf, math.nan):
        with pytest.raises(nx.InvalidArgumentError) as refused:
            nx.Cancel(deadline=seconds)
        assert refused.value.kind == nx.FailureKind.INVALID_ARGUMENT
        assert "deadline" in refused.value.message


def test_a_connect_ends_at_its_deadline_against_a_listener_that_never_answers(
    silent_listener: SilentListener,
) -> None:
    # ANCHOR: connect_deadline
    with pytest.raises(nx.DeadlineError) as expired:
        nx.Session(silent_listener.uri, cancel=nx.Cancel(deadline=0.5))
    assert expired.value.kind == nx.FailureKind.DEADLINE
    # ANCHOR_END: connect_deadline


def test_a_connect_is_cancelled_from_another_thread(silent_listener: SilentListener) -> None:
    # ANCHOR: cancel_from_another_thread
    cancel = nx.Cancel()
    failures: list[nx.ClientError] = []

    def connect() -> None:
        try:
            nx.Session(silent_listener.uri, cancel=cancel)
        except nx.ClientError as failure:
            failures.append(failure)

    connecting = threading.Thread(target=connect)
    connecting.start()
    # The connect waits with the GIL released, so this thread runs and cancels it.
    wait_until(lambda: bool(silent_listener.accepted), "the connect reaching the listener")
    cancel.trigger()
    connecting.join()
    assert isinstance(failures[0], nx.CancelledError)
    # ANCHOR_END: cancel_from_another_thread
    assert failures[0].kind == nx.FailureKind.CANCELLED


def test_one_token_ends_every_wait_it_was_passed_to(silent_listener: SilentListener) -> None:
    cancel = nx.Cancel()
    failures: list[nx.ClientError] = []
    lock = threading.Lock()

    def connect() -> None:
        try:
            nx.Session(silent_listener.uri, cancel=cancel)
        except nx.ClientError as failure:
            with lock:
                failures.append(failure)

    waiters = [threading.Thread(target=connect) for _ in range(4)]
    for waiter in waiters:
        waiter.start()
    wait_until(lambda: len(silent_listener.accepted) >= 4, "every connect reaching the listener")
    cancel.trigger()
    for waiter in waiters:
        waiter.join()
    assert len(failures) == 4
    assert all(isinstance(failure, nx.CancelledError) for failure in failures)

    # A token that was triggered before a call ends that call at once.
    with pytest.raises(nx.CancelledError):
        nx.Session(silent_listener.uri, cancel=cancel)
