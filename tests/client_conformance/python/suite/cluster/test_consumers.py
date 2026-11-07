"""Consumers of the client emitter `typed_out`: a delivery's identity, metadata and columns, read
level by level and as its stream; the ack, retry and reject settlements; deadlines; and a lost
session that interrupts a consumer which is then restored."""

from __future__ import annotations

import struct
import threading

import pytest

import nervix_client as nx
from support import Cluster, Relay, Row, build, deadline, input_fields, next_of, output_fields


def open_pair(session: nx.Session, domain: str) -> tuple[nx.Consumer, nx.Producer]:
    consumer = session.subscribe_emitter(
        domain, "typed_out", output_fields(), batches=4, bytes=1 << 20, cancel=deadline()
    )
    producer = session.open_ingestor(
        domain, "typed_in", input_fields(), batches=2, bytes=1 << 20, cancel=deadline()
    )
    return consumer, producer


def test_a_delivery_carries_its_identity_metadata_and_columns(
    cluster: Cluster, session: nx.Session
) -> None:
    consumer, producer = open_pair(session, cluster.domain)
    with consumer, producer:
        rows = [Row(201, "acme", tags=("a", "bc"), grid=(1, -2)), Row(202, "acme", note="n")]
        submission = producer.submit(build(producer.schema, rows))
        delivery = next_of(consumer, [201, 202])
        # ANCHOR: delivery
        print(delivery.source_relay, delivery.members, delivery.execution_now)
        identity, reference = delivery.identity, delivery.reference
        branch = delivery.branch_fingerprint
        batch = delivery.batch
        ids = batch.column(0)
        echoes = batch.column(11)
        # ANCHOR_END: delivery
        assert (delivery.source_relay, delivery.members) == ("typed_rows", 2)
        assert len(identity) == 16 and len(reference) == 16
        assert branch is not None and len(branch) == 32
        assert ids == [201, 202] and echoes == [201, 202]
        assert delivery.execution_now > 0
        # ANCHOR: delivery_levels
        assert batch.row_count == 2
        assert batch.states(5) == [nx.CellState.NULL, nx.CellState.VALUE]
        assert batch.cells(8, 1) == 2
        assert batch.offsets(8, 0) == [0, 2, 2]
        offsets, data = batch.varlen(8, 1)
        assert [data[offsets[index] : offsets[index + 1]] for index in range(2)] == [b"a", b"bc"]
        assert struct.unpack("=4h", batch.fixed(9, 1)) == (1, -2, 0, 0)
        # ANCHOR_END: delivery_levels
        assert batch.column(8) == [["a", "bc"], []]
        assert batch.column(10) == ["s", "s"]
        assert bytes(delivery.ipc) == bytes(memoryview(batch))
        assert batch.schema.fields[-1].name == "echo"
        assert consumer.schema.fields[-1].name == "echo"
        assert delivery.ack(cancel=deadline()) == nx.Settlement.CONFIRMED
        producer.rejoin(submission, cancel=deadline())


def test_a_consumer_describes_what_its_open_established(cluster: Cluster, session: nx.Session) -> None:
    with session.subscribe_emitter(
        cluster.domain, "typed_out", output_fields(), batches=4, bytes=1 << 20
    ) as consumer:
        assert consumer.state == nx.EndpointState.ACTIVE
        assert consumer.generation >= 1
        assert len(consumer.contract) == 32
        assert consumer.reopen is None
        grant = consumer.grant
        assert (grant.batches, grant.bytes) == (4, 1 << 20)
        assert grant.max_batch_rows == 16
        policy = consumer.policy
        assert (policy.window, policy.outstanding) == (nx.WindowKind.SEQUENTIAL, 1)
        assert policy.ack_timeout_nanos == 60_000_000_000


def test_a_retried_delivery_comes_back_with_its_identity_and_a_new_reference(
    cluster: Cluster, session: nx.Session
) -> None:
    consumer, producer = open_pair(session, cluster.domain)
    with consumer, producer:
        submission = producer.submit(build(producer.schema, [Row(211)]))
        first = next_of(consumer, [211])
        # ANCHOR: retry
        assert first.retry(cancel=deadline()) == nx.Settlement.CONFIRMED
        again = consumer.next(cancel=nx.Cancel(deadline=120))
        assert again.identity == first.identity
        assert again.reference != first.reference
        # The first attempt's reference is stale once its retry was confirmed.
        assert first.ack(cancel=deadline()) == nx.Settlement.STALE_REFERENCE
        assert again.ack(cancel=deadline()) == nx.Settlement.CONFIRMED
        # ANCHOR_END: retry
        assert producer.rejoin(submission, cancel=deadline()).result == nx.SubmissionResult.COMPLETED


def test_a_rejected_delivery_is_handled_by_the_emitters_error_policy(
    cluster: Cluster, session: nx.Session
) -> None:
    consumer, producer = open_pair(session, cluster.domain)
    with consumer, producer:
        submission = producer.submit(build(producer.schema, [Row(221)]))
        delivery = next_of(consumer, [221])
        # ANCHOR: reject
        settlement = delivery.reject("the order names no known account", cancel=deadline())
        assert settlement == nx.Settlement.CONFIRMED
        # ANCHOR_END: reject
        outcome = producer.rejoin(submission, cancel=deadline())
        assert outcome.failure == nx.ProcessingFailure.REJECTED


def test_a_consumer_wait_ends_at_its_deadline_and_by_cancellation(
    cluster: Cluster, session: nx.Session
) -> None:
    with session.subscribe_emitter(
        cluster.domain, "typed_out", output_fields(), batches=4, bytes=1 << 20
    ) as consumer:
        with pytest.raises(nx.DeadlineError):
            consumer.next(cancel=nx.Cancel(deadline=0.3))
        cancel = nx.Cancel()
        failures: list[nx.ClientError] = []

        def read() -> None:
            try:
                consumer.next(cancel=cancel)
            except nx.ClientError as failure:
                failures.append(failure)

        reader = threading.Thread(target=read)
        reader.start()
        cancel.trigger()
        reader.join()
        assert isinstance(failures[0], nx.CancelledError)


def test_a_lost_session_interrupts_the_consumer_which_is_then_restored(
    cluster: Cluster, relay: Relay, session: nx.Session
) -> None:
    with session.open_ingestor(
        cluster.domain, "typed_in", input_fields(), batches=2, bytes=1 << 20
    ) as producer, cluster.connect(relay.uri) as consuming:
        with consuming.subscribe_emitter(
            cluster.domain, "typed_out", output_fields(), batches=4, bytes=1 << 20
        ) as consumer:
            submission = producer.submit(build(producer.schema, [Row(231)]))
            held = next_of(consumer, [231])
            relay.cut()
            # ANCHOR: consumer_interrupted
            with pytest.raises(nx.ConsumerInterruptedError):
                consumer.next(cancel=nx.Cancel(deadline=120))
            # A delivery read before the gap can no longer be settled.
            with pytest.raises(nx.RejectedError):
                held.ack(cancel=nx.Cancel(deadline=120))
            # ANCHOR_END: consumer_interrupted
            relay.restore()
            redelivered = next_of(consumer, [231])
            assert redelivered.identity == held.identity
            assert redelivered.ack(cancel=deadline()) == nx.Settlement.CONFIRMED
            assert consumer.state == nx.EndpointState.ACTIVE
            producer.rejoin(submission, cancel=deadline())
            consumer.close(cancel=deadline())
            assert consumer.state == nx.EndpointState.CLOSED
