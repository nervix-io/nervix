"""Producers of the client ingestor `typed_in`: opening and refusals, the description an open
establishes, batches submitted built, as an IPC stream and as Arrow data, outcomes taken and let
go, credit, every outcome class, a lost session, and a reopen after the contract changed. Their
output reaches the client emitter `typed_out`, which a consumer acknowledges."""

from __future__ import annotations

import pytest

import nervix_client as nx
from support import (
    Cluster,
    Relay,
    Row,
    build,
    completed,
    deadline,
    fields_of,
    INPUT_FIELDS,
    input_fields,
    next_of,
    output_fields,
    wait_until,
)


def open_typed(session: nx.Session, domain: str, batches: int = 2) -> nx.Producer:
    return session.open_ingestor(
        domain, "typed_in", input_fields(), batches=batches, bytes=1 << 20, cancel=deadline()
    )


def open_consumer(session: nx.Session, domain: str) -> nx.Consumer:
    return session.subscribe_emitter(
        domain, "typed_out", output_fields(), batches=4, bytes=1 << 20, cancel=deadline()
    )


def test_a_producer_opens_with_the_description_its_open_established(
    cluster: Cluster, session: nx.Session
) -> None:
    domain = cluster.domain
    # ANCHOR: open_producer
    fields = nx.Fields()
    fields.add("id", nx.FieldType.U32)
    fields.add("tenant", nx.FieldType.STRING)
    fields.add("amount", nx.FieldType.I64)
    fields.add("ratio", nx.FieldType.F64)
    fields.add("flag", nx.FieldType.BOOL)
    fields.add("note", nx.FieldType.STRING, nullable=True)
    fields.add("raw", nx.FieldType.BYTES, nullable=True)
    fields.add("at", nx.FieldType.DATETIME)
    fields.add("tags", nx.FieldType.LIST)
    fields.element(nx.FieldType.STRING)
    fields.add("grid", nx.FieldType.FIXED_LIST, 2)
    fields.element(nx.FieldType.I16)
    fields.add("secret", nx.FieldType.STRING, sensitive=True)
    with session.open_ingestor(domain, "typed_in", fields, batches=2, bytes=1 << 20) as producer:
        assert producer.state == nx.EndpointState.ACTIVE
        assert producer.admission == nx.Admission.OPEN
        grant = producer.grant
        policy = producer.policy
        print(grant.batches, grant.bytes, grant.max_batch_rows, grant.max_batch_bytes)
        print(policy.window, policy.outstanding, policy.ack_timeout_nanos)
    # ANCHOR_END: open_producer
        assert grant == producer.grant
        assert (grant.batches, grant.bytes) == (2, 1 << 20)
        assert 0 < grant.max_batch_bytes <= grant.bytes and grant.max_batch_rows > 0
        assert policy == producer.policy
        assert (policy.window, policy.outstanding) == (nx.WindowKind.SEQUENTIAL, 1)
        assert policy.ack_timeout_nanos == 60_000_000_000
        assert policy.retry_backoff_nanos == 100_000_000
        assert policy.retry_max_backoff_nanos == 1_000_000_000
        assert producer.generation >= 1
        assert len(producer.contract) == 32
        assert producer.reopen is None
        assert [field.name for field in producer.schema.fields][-1] == "secret"


def test_an_open_the_cluster_cannot_serve_is_refused_with_its_refusal(
    cluster: Cluster, session: nx.Session
) -> None:
    other_fields = fields_of(
        [(name, levels, nullable, False) for name, levels, nullable, _ in INPUT_FIELDS]
    )
    # ANCHOR: open_refused
    with pytest.raises(nx.RejectedError) as refused:
        session.open_ingestor(cluster.domain, "typed_in", other_fields, batches=2, bytes=1 << 20)
    assert refused.value.open_refusal == nx.OpenRefusal.SCHEMA_MISMATCH
    # ANCHOR_END: open_refused
    with pytest.raises(nx.RejectedError) as missing:
        session.open_ingestor(cluster.domain, "no_such_in", input_fields(), batches=2, bytes=1024)
    assert missing.value.open_refusal == nx.OpenRefusal.ENDPOINT_NOT_FOUND
    with pytest.raises(nx.RejectedError) as no_domain:
        session.open_ingestor(f"{cluster.domain}_absent", "typed_in", input_fields(), batches=2, bytes=1024)
    assert no_domain.value.open_refusal == nx.OpenRefusal.DOMAIN_NOT_FOUND
    # A credit of nothing is the host's own argument, refused before anything is sent.
    with pytest.raises(nx.InvalidArgumentError):
        session.open_ingestor(cluster.domain, "typed_in", input_fields(), batches=0, bytes=1024)


def test_a_built_batch_completes_once_its_output_is_acknowledged(
    cluster: Cluster, session: nx.Session
) -> None:
    with open_consumer(session, cluster.domain) as consumer, open_typed(session, cluster.domain) as producer:
        # ANCHOR: submit_built
        batch = build(producer.schema, [Row(101, "acme", note="first"), Row(102, "acme")])
        submission = producer.submit(batch)
        pending = producer.pending()
        assert [held.id for held in pending] == [submission]
        assert not pending[0].resolved
        # ANCHOR_END: submit_built
        assert pending[0] == producer.pending()[0]
        delivery = next_of(consumer, [101, 102])
        assert delivery.ack(cancel=deadline()) == nx.Settlement.CONFIRMED
        # ANCHOR: rejoin
        outcome = producer.rejoin(submission, cancel=nx.Cancel(deadline=120))
        assert outcome.result == nx.SubmissionResult.COMPLETED
        # ANCHOR_END: rejoin
        assert outcome.message == ""
        assert producer.pending() == []


def test_a_batch_written_as_an_ipc_stream_completes(cluster: Cluster, session: nx.Session) -> None:
    with open_consumer(session, cluster.domain) as consumer, open_typed(session, cluster.domain) as producer:
        stream = bytes(build(producer.schema, [Row(111)]).ipc)
        # ANCHOR: submit_ipc
        submission = producer.submit(stream)
        # ANCHOR_END: submit_ipc
        next_of(consumer, [111]).ack(cancel=deadline())
        assert producer.rejoin(submission, cancel=deadline()).result == nx.SubmissionResult.COMPLETED


def test_every_submission_outcome_class_reads_its_cause(cluster: Cluster, session: nx.Session) -> None:
    with open_consumer(session, cluster.domain) as consumer, open_typed(session, cluster.domain) as producer:
        # A stream of another schema, written by other tooling, is refused by the server.
        foreign = bytes(build(consumer.schema, [Row(121)]).ipc)
        refused = producer.rejoin(producer.submit(foreign), cancel=deadline())
        # ANCHOR: outcome_classes
        if refused.result == nx.SubmissionResult.NOT_ADMITTED:
            print(refused.refusal, refused.message)
            if refused.refusal == nx.SubmissionRefusal.INVALID_BATCH:
                print(refused.defect)
        elif refused.result == nx.SubmissionResult.PROCESSING_FAILED:
            print(refused.failure)
        elif refused.result == nx.SubmissionResult.OUTCOME_UNKNOWN:
            print(refused.uncertainty)
        # ANCHOR_END: outcome_classes
        assert refused.result == nx.SubmissionResult.NOT_ADMITTED
        assert refused.refusal == nx.SubmissionRefusal.INVALID_BATCH
        assert refused.defect == nx.BatchDefect.SCHEMA_MISMATCH
        assert refused.message
        # A batch whose output the application rejects fails processing.
        rejected = producer.submit(build(producer.schema, [Row(122)]))
        next_of(consumer, [122]).reject("refused by the application", cancel=deadline())
        failed = producer.rejoin(rejected, cancel=deadline())
        assert failed.result == nx.SubmissionResult.PROCESSING_FAILED
        assert failed.failure == nx.ProcessingFailure.REJECTED
        # A cause an outcome's class does not carry is not read.
        with pytest.raises(nx.TypeMismatchError):
            failed.refusal
        with pytest.raises(nx.TypeMismatchError):
            failed.uncertainty
        with pytest.raises(nx.TypeMismatchError):
            refused.failure


def test_a_submission_let_go_is_no_longer_held(cluster: Cluster, session: nx.Session) -> None:
    with open_consumer(session, cluster.domain) as consumer, open_typed(session, cluster.domain) as producer:
        submission = producer.submit(build(producer.schema, [Row(131)]))
        # ANCHOR: release
        assert producer.release(submission) is None
        assert producer.pending() == []
        # ANCHOR_END: release
        with pytest.raises(nx.InvalidArgumentError):
            producer.rejoin(submission, cancel=deadline())
        next_of(consumer, [131]).ack(cancel=deadline())
        # A submission whose outcome is known hands it over when it is let go.
        known = producer.submit(build(producer.schema, [Row(132)]))
        next_of(consumer, [132]).ack(cancel=deadline())
        wait_until(lambda: all(held.resolved for held in producer.pending()), "the outcome arriving")
        released = producer.release(known)
        assert released is not None and released.result == nx.SubmissionResult.COMPLETED


def test_a_batch_beyond_the_credit_waits_for_an_outcome(cluster: Cluster, session: nx.Session) -> None:
    with open_consumer(session, cluster.domain) as consumer, open_typed(session, cluster.domain, batches=1) as producer:
        first = producer.submit(build(producer.schema, [Row(141)]))
        second = build(producer.schema, [Row(142)])
        # ANCHOR: credit
        with pytest.raises(nx.DeadlineError):
            producer.submit(second, cancel=nx.Cancel(deadline=0.3))
        # ANCHOR_END: credit
        next_of(consumer, [141]).ack(cancel=deadline())
        assert producer.rejoin(first, cancel=deadline()).result == nx.SubmissionResult.COMPLETED
        later = producer.submit(second, cancel=deadline())
        next_of(consumer, [142]).ack(cancel=deadline())
        assert producer.rejoin(later, cancel=deadline()).result == nx.SubmissionResult.COMPLETED


def test_a_lost_session_leaves_a_submitted_batch_unknown_and_restores_the_producer(
    cluster: Cluster, relay: Relay, session: nx.Session
) -> None:
    with open_consumer(session, cluster.domain) as consumer:
        with cluster.connect(relay.uri) as publishing, open_typed(publishing, cluster.domain) as producer:
            submission = producer.submit(build(producer.schema, [Row(151)]))
            held = next_of(consumer, [151])
            relay.cut()
            # ANCHOR: outcome_unknown
            outcome = producer.rejoin(submission, cancel=nx.Cancel(deadline=120))
            assert outcome.result == nx.SubmissionResult.OUTCOME_UNKNOWN
            assert outcome.uncertainty == nx.Uncertainty.SESSION_LOST
            # ANCHOR_END: outcome_unknown
            assert held.ack(cancel=deadline()) == nx.Settlement.CONFIRMED
            relay.restore()
            # The next submission opens a new session and restores the producer before it sends.
            again = producer.submit(build(producer.schema, [Row(152)]), cancel=deadline())
            assert producer.state == nx.EndpointState.ACTIVE
            next_of(consumer, [152]).ack(cancel=deadline())
            assert producer.rejoin(again, cancel=deadline()).result == nx.SubmissionResult.COMPLETED


def test_a_changed_contract_requires_the_producer_to_open_again(
    cluster: Cluster, session: nx.Session, unique: str
) -> None:
    ingestor = f"changing_{unique}"
    body = (
        "FROM SCHEMA suite_row MODE ACK PARALLEL MAX 4 ACK TIMEOUT {timeout} "
        "RETRY POLICY BACKOFF 100ms MAX 1s ON QUIESCE SUSPEND"
    )
    completed(
        session,
        f"CREATE CLIENT PRODUCER INGESTOR {ingestor} {body.format(timeout='60s')} TIMESTAMP NOW "
        "TO watched_rows INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant "
        "FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
    )
    producer = session.open_ingestor(cluster.domain, ingestor, input_fields(), batches=2, bytes=1 << 20)
    completed(session, f"ALTER CLIENT PRODUCER INGESTOR {ingestor} SET {body.format(timeout='30s')};")
    wait_until(
        lambda: producer.state == nx.EndpointState.REOPEN_REQUIRED, "the producer needing a reopen"
    )
    # ANCHOR: reopen
    reopen = producer.reopen
    assert reopen is not None and reopen.reason == nx.ReopenReason.CONTRACT_CHANGED
    with pytest.raises(nx.ReopenRequiredError):
        producer.submit(build(producer.schema, [Row(161)]))
    producer = session.open_ingestor(cluster.domain, ingestor, input_fields(), batches=2, bytes=1 << 20)
    # ANCHOR_END: reopen
    assert reopen.refusal is None
    assert reopen == reopen
    assert producer.state == nx.EndpointState.ACTIVE
    assert producer.policy.ack_timeout_nanos == 30_000_000_000
    producer.close(cancel=deadline())
    assert producer.state == nx.EndpointState.CLOSED
