"""Subscriptions: rows read as typed values, level by level and as the frame, null and withheld
values, the events of a lost session and a refused restoration, the end of a subscription the
server ended, and deletion. Rows are published through the client ingestor `watched_in`, whose
relay `watched_rows` no emitter reads."""

from __future__ import annotations

import gc
import struct
import time
import weakref

import pytest

import nervix_client as nx
from actions import Harness
from support import (
    WAIT_SECONDS,
    Cluster,
    Relay,
    Row,
    build,
    completed,
    deadline,
    execute,
    input_fields,
    wait_until,
)


def publish(session: nx.Session, domain: str, rows: list[Row]) -> None:
    """Publishes `rows` through `watched_in` and waits until they completed."""

    with session.open_ingestor(
        domain, "watched_in", input_fields(), batches=4, bytes=1 << 20, cancel=deadline()
    ) as producer:
        submission = producer.submit(build(producer.schema, rows), cancel=deadline())
        outcome = producer.rejoin(submission, cancel=deadline())
        assert outcome.result == nx.SubmissionResult.COMPLETED, outcome.message


def next_rows(session: nx.Session) -> nx.Event:
    event = session.next_event(cancel=deadline())
    assert event.kind == nx.EventKind.ROWS, event.kind
    return event


def published_until_seen(
    publishing: nx.Session, watching: nx.Session, domain: str, row: Row
) -> nx.Event:
    """The first rows `watching` reads once it restored its subscription. A restored subscription
    is a new generation that sees only rows published after it opened, and no event tells when
    that was, so `row` is published again until rows arrive."""

    ends = time.monotonic() + WAIT_SECONDS
    while True:
        if time.monotonic() >= ends:
            raise AssertionError(
                f"the restored subscription delivered no rows within {WAIT_SECONDS} seconds"
            )
        publish(publishing, domain, [row])
        try:
            event = watching.next_event(cancel=nx.Cancel(deadline=10))
        except nx.DeadlineError:
            if time.monotonic() > ends:
                raise AssertionError(
                    f"the restored subscription delivered no rows within {WAIT_SECONDS} seconds"
                ) from None
            continue
        if event.kind == nx.EventKind.RESTORATION_FAILED:
            # Another seed can still owe its one refusal; each attempt stays observable.
            failure = event.restoration_failure
            assert failure is not None
            assert failure.cause == nx.RestorationCause.UNAVAILABLE
            assert failure.message and failure.retry_after_nanos > 0
            continue
        assert event.kind == nx.EventKind.ROWS, event.kind
        return event


def test_a_subscription_reads_typed_rows_with_null_and_withheld_values(
    cluster: Cluster, session: nx.Session, unique: str
) -> None:
    name = f"seen_{unique}"
    # ANCHOR: subscribe
    opened = session.execute(session.prepare(f"CREATE SUBSCRIPTION {name} TO watched_rows;"))
    assert opened.disposition == nx.Disposition.COMPLETED
    handle = opened.subscription
    schema = opened.schema
    assert handle is not None and schema is not None
    assert handle.name == name
    assert [field.name for field in schema.fields][:3] == ["id", "tenant", "amount"]
    assert schema.branch == "by_tenant"
    assert [field.name for field in schema.key_fields] == ["tenant"]
    # ANCHOR_END: subscribe
    publish(session, cluster.domain, [Row(1, "acme", amount=-7, note="first"), Row(2, "acme")])
    # ANCHOR: read_rows
    event = session.next_event(cancel=nx.Cancel(deadline=120))
    assert event.kind == nx.EventKind.ROWS
    assert event.subscription == handle
    assert event.row_count == 2
    ids = event.column(0)
    notes = event.column(5)
    assert ids == [1, 2]
    assert notes == ["first", None]
    assert event.states(5) == [nx.CellState.VALUE, nx.CellState.NULL]
    # A sensitive field is withheld from a subscriber: every cell is REDACTED.
    assert event.column(10) == [nx.REDACTED, nx.REDACTED]
    assert event.states(10) == [nx.CellState.REDACTED, nx.CellState.REDACTED]
    # The branch key is read from its own part.
    assert event.column(0, nx.Part.BRANCH_KEY) == ["acme"]
    # ANCHOR_END: read_rows
    assert event.schema.fields[0].name == "id"
    assert event.restoration_failure is None
    field = event.schema.fields[8]
    assert field.type == nx.FieldType.LIST
    assert [level.type for level in field.levels] == [nx.FieldType.LIST, nx.FieldType.STRING]
    assert not field.nullable and not field.sensitive
    assert event.schema.fields[5].nullable
    assert event.schema.fields[10].sensitive
    assert repr(field) == 'Field("tags")'
    assert repr(handle) == f'Subscription("{name}", {handle.generation})'
    assert hash(handle) == hash(event.subscription)
    completed(session, f"DELETE SUBSCRIPTION {name};")


def test_rows_read_in_bulk_as_values_offsets_and_the_frame(
    cluster: Cluster, session: nx.Session, unique: str
) -> None:
    name = f"bulk_{unique}"
    completed(session, f"CREATE SUBSCRIPTION {name} TO watched_rows;")
    publish(session, cluster.domain, [Row(3, "beta", amount=1 << 40, raw=b"\x00\xff"), Row(4, "beta")])
    event = next_rows(session)
    # ANCHOR: read_bulk
    amounts = struct.unpack("=2q", event.fixed(2))
    offsets, data = event.varlen(1)
    tenants = [data[offsets[row] : offsets[row + 1]].decode() for row in range(event.row_count)]
    frame = event.frame
    # ANCHOR_END: read_bulk
    assert amounts == (1 << 40, 0)
    assert tenants == ["beta", "beta"]
    assert bytes(memoryview(event)[4:8]) == b"NXSM"
    assert bytes(frame) == bytes(memoryview(event))
    completed(session, f"DELETE SUBSCRIPTION {name};")


def test_a_frame_view_keeps_its_event_alive(cluster: Cluster, session: nx.Session, unique: str) -> None:
    name = f"kept_{unique}"
    completed(session, f"CREATE SUBSCRIPTION {name} TO watched_rows;")
    publish(session, cluster.domain, [Row(5, "gamma")])
    event = next_rows(session)
    # ANCHOR: view_keeps_event
    view = event.frame
    snapshot = bytes(view)
    collected = weakref.ref(event)
    del event
    gc.collect()
    # The view keeps the event and the frame it borrows alive.
    assert bytes(view) == snapshot
    assert collected() is view.obj
    view.release()
    del view
    wait_until(lambda: gc.collect() >= 0 and collected() is None, "the event being collected")
    # ANCHOR_END: view_keeps_event
    completed(session, f"DELETE SUBSCRIPTION {name};")


def test_a_lost_session_interrupts_a_subscription_which_is_then_restored(
    cluster: Cluster, relay: Relay, session: nx.Session, unique: str
) -> None:
    name = f"restored_{unique}"
    with cluster.connect(relay.uri) as watching:
        completed(watching, f"CREATE SUBSCRIPTION {name} TO watched_rows;")
        publish(session, cluster.domain, [Row(6, "delta")])
        before = next_rows(watching)
        relay.cut()
        # ANCHOR: interrupted
        event = watching.next_event(cancel=nx.Cancel(deadline=120))
        assert event.kind == nx.EventKind.INTERRUPTED
        # ANCHOR_END: interrupted
        assert event.subscription.name == name
        relay.restore()
        # A command opens a new session through the relay, which opens the subscription again.
        completed(watching, "LIST DOMAINS;")
        after = published_until_seen(session, watching, cluster.domain, Row(7, "delta"))
        assert after.column(0) == [7]
        # A generation is numbered within its session, so the generation the new session opens can
        # carry the number the lost session gave the subscription; the name is what stays.
        assert after.subscription.name == before.subscription.name == name
        completed(watching, f"DELETE SUBSCRIPTION {name};")


def test_a_refused_subscription_restoration_reports_its_cause_and_recovers(
    cluster: Cluster, relay: Relay, subscription_opening_refusal: Harness,
    session: nx.Session, unique: str
) -> None:
    name = f"refused_{unique}"
    with cluster.connect(relay.uri) as watching:
        completed(watching, f"CREATE SUBSCRIPTION {name} TO watched_rows;")
        relay.cut()
        subscription_opening_refusal.refuse_next_subscription_opening()
        relay.restore()
        assert watching.next_event(cancel=deadline()).kind == nx.EventKind.INTERRUPTED
        # ANCHOR: restoration_failed
        event = watching.next_event(cancel=nx.Cancel(deadline=120))
        assert event.kind == nx.EventKind.RESTORATION_FAILED
        failure = event.restoration_failure
        assert failure is not None
        assert failure.cause == nx.RestorationCause.UNAVAILABLE
        assert failure.message
        assert failure.retry_after_nanos > 0
        # The subscription stays alive and a later attempt opens it again.
        after = published_until_seen(session, watching, cluster.domain, Row(8, "refused"))
        assert after.kind == nx.EventKind.ROWS
        assert after.column(0) == [8]
        # ANCHOR_END: restoration_failed
        assert event.subscription.name == after.subscription.name == name
        completed(watching, f"DELETE SUBSCRIPTION {name};")


def handled(event: nx.Event, log: list[str]) -> bool:
    """Handles one subscription event as a reader does, and tells whether the subscription goes
    on."""

    # ANCHOR: handle_events
    if event.kind == nx.EventKind.ROWS:
        log.append(f"{event.row_count} rows")
    elif event.kind == nx.EventKind.INTERRUPTED:
        log.append("interrupted")
    elif event.kind == nx.EventKind.RESTORATION_FAILED:
        # The new session refused for now to open the subscription again; it tries again later.
        failure = event.restoration_failure
        assert failure is not None
        log.append(f"refused: {failure.cause!r} {failure.message} {failure.retry_after_nanos}")
    elif event.kind == nx.EventKind.ENDED:
        # The generation ended for good: subscribe again to read the relay as it is now.
        log.append(f"ended: {event.end_reason!r}")
        return False
    else:
        # DELIVERY_LOST, ROWS_SKIPPED and CONSUMER_OVERFLOW count the rows the reader missed.
        log.append(f"missed {event.row_count} rows")
    return True
    # ANCHOR_END: handle_events


def test_a_relay_dropped_while_the_session_was_lost_ends_the_subscription(
    cluster: Cluster, relay: Relay, session: nx.Session, unique: str
) -> None:
    doomed = f"doomed_{unique}"
    name = f"removed_{unique}"
    completed(session, f"CREATE RELAY {doomed} SCHEMA suite_row BRANCHED BY by_tenant;")
    log: list[str] = []
    with cluster.connect(relay.uri) as watching:
        completed(watching, f"CREATE SUBSCRIPTION {name} TO {doomed};")
        relay.cut()
        assert handled(watching.next_event(cancel=deadline()), log)
        completed(session, f"DROP RELAY {doomed};")
        relay.restore()
        # ANCHOR: ended_while_lost
        event = watching.next_event(cancel=nx.Cancel(deadline=120))
        # The relay was dropped while the session was lost, so the new session ends the
        # subscription instead of opening it again.
        assert event.kind == nx.EventKind.ENDED
        assert event.end_reason == nx.EndReason.RELAY_REMOVED
        # ANCHOR_END: ended_while_lost
        assert event.subscription.name == name
        assert event.restoration_failure is None
        assert not handled(event, log)
    assert log == ["interrupted", "ended: EndReason.RELAY_REMOVED"]


def test_a_relay_redefined_while_the_session_was_lost_ends_the_subscription(
    cluster: Cluster, relay: Relay, session: nx.Session, unique: str
) -> None:
    redefined = f"redefined_{unique}"
    name = f"changed_{unique}"
    completed(session, f"CREATE RELAY {redefined} SCHEMA suite_row BRANCHED BY by_tenant;")
    with cluster.connect(relay.uri) as watching:
        completed(watching, f"CREATE SUBSCRIPTION {name} TO {redefined};")
        relay.cut()
        assert watching.next_event(cancel=deadline()).kind == nx.EventKind.INTERRUPTED
        # The relay comes back under its name, announcing another schema.
        completed(session, f"DROP RELAY {redefined};")
        completed(session, f"CREATE SCHEMA other_{unique} ( value I64 );")
        completed(session, f"CREATE RELAY {redefined} SCHEMA other_{unique} UNBRANCHED;")
        relay.restore()
        event = watching.next_event(cancel=deadline())
        assert event.kind == nx.EventKind.ENDED
        assert event.end_reason == nx.EndReason.RELAY_CHANGED
        assert event.subscription.name == name


def test_a_subscription_the_server_ended_reports_its_end(
    session: nx.Session, unique: str
) -> None:
    ending = f"ending_{unique}"
    name = f"ended_{unique}"
    completed(session, f"CREATE RELAY {ending} SCHEMA suite_row BRANCHED BY by_tenant;")
    completed(session, f"CREATE SUBSCRIPTION {name} TO {ending};")
    completed(session, f"DROP RELAY {ending};")
    # ANCHOR: ended
    event = session.next_event(cancel=nx.Cancel(deadline=120))
    assert event.kind == nx.EventKind.ENDED
    assert event.end_reason == nx.EndReason.RELAY_REMOVED
    # ANCHOR_END: ended
    assert event.subscription.name == name
    # An event that carries no rows has no schema or frame to read.
    with pytest.raises(nx.TypeMismatchError):
        event.schema
    with pytest.raises(nx.TypeMismatchError):
        event.rows()


def test_a_deleted_subscription_frees_its_name_for_a_new_generation(
    session: nx.Session, unique: str
) -> None:
    name = f"again_{unique}"
    first = completed(session, f"CREATE SUBSCRIPTION {name} TO watched_rows;").subscription
    # ANCHOR: delete
    deleted = session.execute(session.prepare(f"DELETE SUBSCRIPTION {name};"))
    assert deleted.disposition == nx.Disposition.COMPLETED
    # ANCHOR_END: delete
    second = completed(session, f"CREATE SUBSCRIPTION {name} TO watched_rows;").subscription
    assert first is not None and second is not None
    assert second.name == first.name
    assert second != first
    assert execute(session, f"DELETE SUBSCRIPTION {name};").disposition == nx.Disposition.COMPLETED
