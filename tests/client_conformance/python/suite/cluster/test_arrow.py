"""Arrow interop through the Arrow PyCapsule interface: rows and deliveries read by pyarrow and by
polars without a copy, batches built by pyarrow submitted and imported, polars data cast to the
endpoint's exact types and submitted, and Arrow data of other types refused before anything is
sent."""

from __future__ import annotations

import ctypes

import polars
import pyarrow
import pytest

import nervix_client as nx
from support import Cluster, Row, build, completed, deadline, input_fields, next_of, output_fields

UTC = pyarrow.timestamp("ns", tz="+00:00")

# The Arrow schema of the endpoint graph's input schema `suite_row`: exactly the types the producer
# takes.
INPUT_SCHEMA = pyarrow.schema(
    [
        ("id", pyarrow.uint32()),
        ("tenant", pyarrow.string()),
        ("amount", pyarrow.int64()),
        ("ratio", pyarrow.float64()),
        ("flag", pyarrow.bool_()),
        ("note", pyarrow.string()),
        ("raw", pyarrow.binary()),
        ("at", UTC),
        ("tags", pyarrow.list_(pyarrow.string())),
        ("grid", pyarrow.list_(pyarrow.int16(), 2)),
        ("secret", pyarrow.string()),
    ]
)


def arrow_rows(identifiers: list[int]) -> pyarrow.RecordBatch:
    """A record batch of the endpoint graph's input schema, as pyarrow builds one."""

    count = len(identifiers)
    return pyarrow.record_batch(
        [
            pyarrow.array(identifiers, pyarrow.uint32()),
            pyarrow.array(["acme"] * count, pyarrow.string()),
            pyarrow.array([0] * count, pyarrow.int64()),
            pyarrow.array([0.5] * count, pyarrow.float64()),
            pyarrow.array([True] * count, pyarrow.bool_()),
            pyarrow.array([None] * count, pyarrow.string()),
            pyarrow.array([None] * count, pyarrow.binary()),
            pyarrow.array([0] * count, UTC),
            pyarrow.array([["p"]] * count, pyarrow.list_(pyarrow.string())),
            pyarrow.array([[1, 2]] * count, pyarrow.list_(pyarrow.int16(), 2)),
            pyarrow.array(["s"] * count, pyarrow.string()),
        ],
        schema=INPUT_SCHEMA,
    )


def test_a_batch_exports_itself_through_the_arrow_pycapsule_interface(
    cluster: Cluster, session: nx.Session
) -> None:
    with session.open_ingestor(cluster.domain, "typed_in", input_fields(), batches=2, bytes=1 << 20) as producer:
        batch = build(producer.schema, [Row(301, note="n"), Row(302)])
    is_valid = ctypes.pythonapi.PyCapsule_IsValid
    is_valid.restype = ctypes.c_int
    is_valid.argtypes = [ctypes.py_object, ctypes.c_char_p]
    assert is_valid(batch.__arrow_c_schema__(), b"arrow_schema")
    schema_capsule, array_capsule = batch.__arrow_c_array__()
    assert is_valid(schema_capsule, b"arrow_schema") and is_valid(array_capsule, b"arrow_array")
    assert is_valid(batch.__arrow_c_stream__(), b"arrow_array_stream")
    # ANCHOR: to_arrow
    table = pyarrow.record_batch(batch)
    frame = polars.DataFrame(batch)
    # ANCHOR_END: to_arrow
    assert table.column("id").to_pylist() == [301, 302]
    assert table.column("note").to_pylist() == ["n", None]
    assert frame["id"].to_list() == [301, 302]
    assert table.schema.field("at").type == UTC


def test_arrow_data_from_pyarrow_and_polars_is_submitted_and_imported(
    cluster: Cluster, session: nx.Session
) -> None:
    with session.subscribe_emitter(
        cluster.domain, "typed_out", output_fields(), batches=4, bytes=1 << 20
    ) as consumer, session.open_ingestor(
        cluster.domain, "typed_in", input_fields(), batches=2, bytes=1 << 20
    ) as producer:
        # ANCHOR: from_arrow
        brought = arrow_rows([311, 312])
        submission = producer.submit(brought)
        imported = nx.Batch.from_arrow(producer.schema, brought)
        assert imported.column(0) == [311, 312]
        # ANCHOR_END: from_arrow
        delivery = next_of(consumer, [311, 312])
        read = pyarrow.record_batch(delivery.batch)
        assert read.column("echo").to_pylist() == [311, 312]
        assert delivery.ack(cancel=deadline()) == nx.Settlement.CONFIRMED
        assert producer.rejoin(submission, cancel=deadline()).result == nx.SubmissionResult.COMPLETED
        frame = polars.from_arrow(arrow_rows([313]))
        # ANCHOR: from_polars
        # polars keeps strings, bytes and lists in Arrow types of its own, which the producer
        # refuses before anything is sent.
        with pytest.raises(nx.InvalidArgumentError):
            producer.submit(frame)
        # pyarrow reads the frame through the same interface and casts it to the endpoint's types.
        from_polars = producer.submit(pyarrow.table(frame).cast(INPUT_SCHEMA))
        # ANCHOR_END: from_polars
        next_of(consumer, [313]).ack(cancel=deadline())
        assert producer.rejoin(from_polars, cancel=deadline()).result == nx.SubmissionResult.COMPLETED
        # Arrow data of another schema is the host's argument, refused before anything is sent.
        with pytest.raises(nx.InvalidArgumentError):
            producer.submit(pyarrow.record_batch([pyarrow.array([1], pyarrow.uint32())], names=["id"]))


def test_rows_of_an_event_are_read_by_pyarrow_with_withheld_values_as_nulls(
    cluster: Cluster, session: nx.Session, unique: str
) -> None:
    name = f"arrow_{unique}"
    completed(session, f"CREATE SUBSCRIPTION {name} TO watched_rows;")
    with session.open_ingestor(cluster.domain, "watched_in", input_fields(), batches=2, bytes=1 << 20) as producer:
        producer.rejoin(producer.submit(build(producer.schema, [Row(321, "omega")])), cancel=deadline())
    event = session.next_event(cancel=deadline())
    assert event.kind == nx.EventKind.ROWS
    # ANCHOR: event_rows
    rows = pyarrow.record_batch(event.rows())
    # A withheld value is a null in Arrow; the event's states tell it from a real null.
    assert rows.column("secret").to_pylist() == [None]
    assert event.states(10) == [nx.CellState.REDACTED]
    # ANCHOR_END: event_rows
    assert rows.column("id").to_pylist() == [321]
    completed(session, f"DELETE SUBSCRIPTION {name};")
