"""Domain clocks: a paced clock attached while it runs and read before its first tick, its
projections, its generations across a STOP and START, a lost session, and detaching. Each test
creates and starts its own paced domain."""

from __future__ import annotations

import threading
from collections.abc import Iterator

import pytest

import nervix_client as nx
from actions import Harness
from support import Cluster, Relay, completed, deadline

ORIGIN_2030 = 1_893_456_000_000_000_000
ORIGIN_2031 = 1_924_992_000_000_000_000
PERIOD = 100_000_000
SKEW = 10_000_000


@pytest.fixture
def paced(cluster: Cluster, session: nx.Session, unique: str) -> Iterator[str]:
    """A paced domain started at the start of 2030 at twice the speed of UTC."""

    domain = f"{cluster.domain}_clock_{unique}"
    completed(session, f"CREATE PACED DOMAIN {domain} WITH PERIOD 100ms SKEW 10ms;")
    with cluster.connect(domain=domain) as starting:
        completed(starting, "START AT '2030-01-01T00:00:00Z' TIME RATE 2.0;")
    yield domain


def first_tick(session: nx.Session, domain: str, generation: int) -> nx.ClockEvent:
    """The first tick of `generation`; the states of that generation are taken on the way."""

    while True:
        event = session.next_clock_event(cancel=deadline())
        assert event.domain == domain
        if event.kind == nx.ClockEventKind.TICK and event.generation == generation:
            return event
        assert event.kind in (nx.ClockEventKind.STATE, nx.ClockEventKind.TICK), event.kind


def test_a_running_clock_is_read_before_its_first_tick_and_projected(
    cluster: Cluster, paced: str
) -> None:
    with cluster.connect(domain=paced) as session:
        assert session.domain_clock(paced) is None
        # ANCHOR: attach
        attached = session.execute(session.prepare("ATTACH DOMAIN CLOCK;"))
        assert attached.disposition == nx.Disposition.COMPLETED
        clock = session.domain_clock(paced)
        assert clock is not None
        assert clock.state == nx.ClockState.PACED
        mapping = clock.paced
        assert (mapping.period, mapping.skew, mapping.origin) == (PERIOD, SKEW, ORIGIN_2030)
        assert mapping.rate == 2.0
        # ANCHOR_END: attach
        assert clock.domain == paced
        assert clock.generation == mapping.generation == 1
        assert mapping == clock.paced
        assert repr(mapping).startswith("PacedClock(generation=1, period=100000000")
        # ANCHOR: project
        anchor = mapping.anchor
        assert clock.logical_time_at(anchor) == mapping.origin
        # At twice the speed of UTC, one period of logical time takes half a period of UTC.
        assert clock.wall_duration_until(anchor, mapping.origin + mapping.period) == PERIOD // 2
        assert clock.admission_window(anchor) == (mapping.origin, mapping.origin)
        assert clock.admits(anchor, mapping.origin + mapping.skew)
        assert not clock.admits(anchor, mapping.origin + mapping.skew + 1)
        # ANCHOR_END: project
        # ANCHOR: ticks
        event = first_tick(session, paced, clock.generation)
        tick = event.tick
        assert tick.generation == 1
        assert tick.boundary == mapping.origin + (tick.id - 1) * mapping.period
        assert tick.serving_logical >= mapping.origin
        assert tick.authority_utc >= mapping.anchor
        # ANCHOR_END: ticks
        assert repr(tick).startswith("Tick(generation=1, id=")
        assert tick == event.tick
        held = session.domain_clock(paced)
        assert held is not None and held.tick is not None and held.tick.id >= tick.id
        # A tick carries no state, mapping or end, which only their own kinds do.
        with pytest.raises(nx.TypeMismatchError):
            event.state
        with pytest.raises(nx.TypeMismatchError):
            event.paced
        with pytest.raises(nx.TypeMismatchError):
            event.end_reason
        assert event.restoration_failure is None
        completed(session, "DETACH DOMAIN CLOCK;")


def test_a_followed_clock_keeps_its_generations_apart_across_stop_and_start(
    cluster: Cluster, paced: str
) -> None:
    with cluster.connect(domain=paced) as session:
        completed(session, "ATTACH DOMAIN CLOCK;")
        first_tick(session, paced, 1)
        completed(session, "STOP;")
        completed(session, "START AT '2031-01-01T00:00:00Z' TIME RATE 4.0;")
        # ANCHOR: generations
        while True:
            event = session.next_clock_event(cancel=nx.Cancel(deadline=120))
            if event.kind == nx.ClockEventKind.STATE and event.state == nx.ClockState.PACED:
                if event.generation == 2:
                    break
        mapping = event.paced
        assert (mapping.origin, mapping.rate) == (ORIGIN_2031, 4.0)
        # ANCHOR_END: generations
        tick = first_tick(session, paced, 2).tick
        assert tick.boundary == mapping.origin + (tick.id - 1) * mapping.period
        completed(session, "DETACH DOMAIN CLOCK;")


def test_a_lost_session_interrupts_the_clock_until_its_attachment_is_restored(
    cluster: Cluster, relay: Relay, paced: str
) -> None:
    with cluster.connect(relay.uri, domain=paced) as session:
        completed(session, "ATTACH DOMAIN CLOCK;")
        first_tick(session, paced, 1)
        relay.cut()
        # ANCHOR: clock_interrupted
        while True:
            event = session.next_clock_event(cancel=nx.Cancel(deadline=120))
            if event.kind == nx.ClockEventKind.INTERRUPTED:
                break
        # ANCHOR_END: clock_interrupted
        held = session.domain_clock(paced)
        assert held is not None and held.state == nx.ClockState.PACED
        relay.restore()
        while True:
            event = session.next_clock_event(cancel=deadline())
            if event.kind == nx.ClockEventKind.STATE and event.state == nx.ClockState.PACED:
                break
        first_tick(session, paced, event.generation)
        completed(session, "DETACH DOMAIN CLOCK;")


def test_a_detached_clock_is_no_longer_followed(cluster: Cluster, paced: str) -> None:
    with cluster.connect(domain=paced) as session:
        completed(session, "ATTACH DOMAIN CLOCK;")
        assert session.domain_clock(paced) is not None
        # ANCHOR: detach
        detached = session.execute(session.prepare("DETACH DOMAIN CLOCK;"))
        assert detached.disposition == nx.Disposition.COMPLETED
        assert session.domain_clock(paced) is None
        # ANCHOR_END: detach


def test_a_refused_clock_restoration_reports_its_cause_and_recovers(
    cluster: Cluster, relay: Relay, clock_attachment_refusal: Harness
) -> None:
    with cluster.connect(relay.uri) as following:
        cancel = deadline()
        completed(following, "ATTACH DOMAIN CLOCK;")
        initial = following.domain_clock(cluster.domain)
        assert initial is not None
        assert initial.state == nx.ClockState.UNPACED
        relay.cut()
        clock_attachment_refusal.refuse_next_clock_attachment()
        relay.restore()
        # ANCHOR: clock_restoration_failed
        interrupted = following.next_clock_event(cancel=cancel)
        assert interrupted.kind == nx.ClockEventKind.INTERRUPTED
        assert interrupted.domain == cluster.domain
        refused = following.next_clock_event(cancel=cancel)
        assert refused.kind == nx.ClockEventKind.RESTORATION_FAILED
        assert refused.domain == cluster.domain
        failure = refused.restoration_failure
        assert failure is not None
        assert failure.cause == nx.RestorationCause.IN_TRANSACTION
        assert failure.message
        assert failure.retry_after_nanos > 0
        # A refused attempt keeps the attachment desired; the library retries it.
        while True:
            restored = following.next_clock_event(cancel=cancel)
            assert restored.domain == cluster.domain
            if restored.kind == nx.ClockEventKind.STATE:
                break
            assert restored.kind == nx.ClockEventKind.RESTORATION_FAILED
            retry = restored.restoration_failure
            assert retry is not None and retry.cause == nx.RestorationCause.IN_TRANSACTION
        assert restored.state == nx.ClockState.UNPACED
        assert restored.generation == initial.generation
        # ANCHOR_END: clock_restoration_failed
        completed(following, "DETACH DOMAIN CLOCK;")


def test_a_clock_wait_ends_at_its_deadline_and_by_cancellation(session: nx.Session) -> None:
    with pytest.raises(nx.DeadlineError):
        session.next_clock_event(cancel=nx.Cancel(deadline=0.2))
    cancel = nx.Cancel()
    failures: list[nx.ClientError] = []

    def wait() -> None:
        try:
            session.next_clock_event(cancel=cancel)
        except nx.ClientError as failure:
            failures.append(failure)

    waiter = threading.Thread(target=wait)
    waiter.start()
    cancel.trigger()
    waiter.join()
    assert isinstance(failures[0], nx.CancelledError)
