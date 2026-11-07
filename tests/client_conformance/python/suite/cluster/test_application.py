"""A complete application on the package: one session, a consumer and a producer, a batch published
and its output handled, and the batch's outcome taken."""

from __future__ import annotations

import nervix_client as nx
from support import Cluster, Row, build, input_fields, output_fields


def test_an_application_publishes_a_batch_and_handles_its_output(cluster: Cluster) -> None:
    server, domain, authority = cluster.server, cluster.domain, cluster.ca_certificate
    username, password = cluster.username, cluster.password
    # ANCHOR: application
    with nx.Session(
        server, domain=domain, username=username, password=password, ca_certificate=authority
    ) as session:
        with session.subscribe_emitter(
            domain, "typed_out", output_fields(), batches=4, bytes=1 << 20
        ) as consumer, session.open_ingestor(
            domain, "typed_in", input_fields(), batches=2, bytes=1 << 20
        ) as producer:
            submission = producer.submit(build(producer.schema, [Row(501, "acme", amount=42)]))
            handled: set[bytes] = set()
            while True:
                delivery = consumer.next(cancel=nx.Cancel(deadline=120))
                with delivery:
                    if delivery.identity not in handled:
                        print(delivery.batch.column(0), delivery.batch.column(2))
                        handled.add(delivery.identity)
                    delivery.ack(cancel=nx.Cancel(deadline=120))
                    if delivery.batch.column(0) == [501]:
                        break
            outcome = producer.rejoin(submission, cancel=nx.Cancel(deadline=120))
            assert outcome.result == nx.SubmissionResult.COMPLETED
    # ANCHOR_END: application
