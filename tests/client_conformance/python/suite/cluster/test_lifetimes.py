"""Lifetimes and memory: a `with` block releases a session, a producer, a consumer and a delivery;
a producer, consumer or delivery keeps its session running after the session is collected; and a
view of a delivery's stream keeps it alive past its release and its collection."""

from __future__ import annotations

import gc
import weakref
from collections.abc import Callable

import pytest

import nervix_client as nx
from support import Cluster, Row, build, deadline, input_fields, next_of, output_fields, wait_until


def collected(reference: Callable[[], object]) -> bool:
    """Whether the object `reference` refers to weakly was collected, after a collection."""

    gc.collect()
    return reference() is None


def test_a_with_block_releases_its_object_and_later_calls_raise_closed(cluster: Cluster) -> None:
    # ANCHOR: with_blocks
    with cluster.connect() as session:
        with session.open_ingestor(
            cluster.domain, "typed_in", input_fields(), batches=2, bytes=1 << 20
        ) as producer:
            assert producer.state == nx.EndpointState.ACTIVE
        # Leaving the block released the producer, without waiting for the server to close it.
        with pytest.raises(nx.ClosedError):
            producer.state
    with pytest.raises(nx.ClosedError):
        session.prepare("LIST DOMAINS;")
    # ANCHOR_END: with_blocks


def test_a_producer_keeps_its_session_running_after_the_session_is_collected(
    cluster: Cluster,
) -> None:
    session = cluster.connect()
    session_reference = weakref.ref(session)
    consumer = session.subscribe_emitter(
        cluster.domain, "typed_out", output_fields(), batches=4, bytes=1 << 20
    )
    # ANCHOR: outlives_session
    producer = session.open_ingestor(cluster.domain, "typed_in", input_fields(), batches=2, bytes=1 << 20)
    del session
    gc.collect()
    # The producer and the consumer still hold the session's runtime.
    submission = producer.submit(build(producer.schema, [Row(401)]))
    # ANCHOR_END: outlives_session
    wait_until(lambda: collected(session_reference), "the session object being collected")
    # The consumer and its delivery reach the cluster through the same session.
    delivery = next_of(consumer, [401])
    assert delivery.ack(cancel=deadline()) == nx.Settlement.CONFIRMED
    assert producer.rejoin(submission, cancel=deadline()).result == nx.SubmissionResult.COMPLETED
    references: list[Callable[[], object]] = [
        weakref.ref(delivery),
        weakref.ref(consumer),
        weakref.ref(producer),
    ]
    del delivery, consumer, producer
    wait_until(
        lambda: all(collected(reference) for reference in references),
        "the delivery, the consumer and the producer being collected",
    )


def test_a_view_of_a_delivery_outlives_the_delivery_and_its_release(cluster: Cluster) -> None:
    with cluster.connect() as session:
        with session.subscribe_emitter(
            cluster.domain, "typed_out", output_fields(), batches=4, bytes=1 << 20
        ) as consumer, session.open_ingestor(
            cluster.domain, "typed_in", input_fields(), batches=2, bytes=1 << 20
        ) as producer:
            submission = producer.submit(build(producer.schema, [Row(411)]))
            # ANCHOR: delivery_view
            with next_of(consumer, [411]) as delivery:
                stream = memoryview(delivery)
                snapshot = bytes(stream)
                assert snapshot == bytes(delivery.ipc)
                assert delivery.ack(cancel=deadline()) == nx.Settlement.CONFIRMED
            # The delivery is released, and the view still reads the stream it borrowed.
            with pytest.raises(nx.ClosedError):
                delivery.identity
            assert bytes(stream) == snapshot
            # ANCHOR_END: delivery_view
            reference = weakref.ref(delivery)
            del delivery
            gc.collect()
            assert reference() is not None
            stream.release()
            del stream
            wait_until(lambda: collected(reference), "the released delivery being collected")
            producer.rejoin(submission, cancel=deadline())
