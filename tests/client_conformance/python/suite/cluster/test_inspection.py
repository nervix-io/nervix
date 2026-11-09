"""Typed inspection metadata, refusal and independent schema/frame lifetimes."""

from __future__ import annotations

import gc
import json
import weakref

import nervix_client as nx
from support import Cluster, Row, build, completed, deadline, execute, input_fields


def test_inspection_metadata_and_verified_frames_survive_their_parents(
    cluster: Cluster, session: nx.Session, unique: str
) -> None:
    relay = f"inspected_{unique}"
    ingestor = f"inspect_in_{unique}"
    completed(session, f"CREATE RELAY {relay} SCHEMA suite_row UNBRANCHED WITH MATERIALIZED STATE LAST BY TIMESTAMP;")
    completed(session, f"""CREATE CLIENT PRODUCER INGESTOR {ingestor}
        FROM SCHEMA suite_row MODE ACK SEQUENTIAL ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND TIMESTAMP NOW TO {relay} INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;""")
    with session.open_ingestor(cluster.domain, ingestor, input_fields(), batches=2, bytes=1 << 20, cancel=deadline()) as producer:
        submission = producer.submit(build(producer.schema, [Row(71, secret="private-inspection")]), cancel=deadline())
        assert producer.rejoin(submission, cancel=deadline()).result == nx.SubmissionResult.COMPLETED
    reader = cluster.connect()
    # ANCHOR: inspection
    outcome = execute(reader, f"SHOW RELAY {relay} MATERIALIZED STATE FOR UNBRANCHED LIMIT 1 FORMAT JSON;")
    inspection = outcome.state_inspection
    assert isinstance(inspection, nx.Inspection)
    assert inspection.rejection is None
    assert isinstance(inspection.counts, nx.InspectionCounts)
    assert (inspection.counts.branches, inspection.counts.rows) == (1, 1)
    assert inspection.counts.bytes > 0 and inspection.unit_count == 1
    assert inspection.domain == cluster.domain and inspection.entity_kind == "RELAY"
    assert inspection.entity_name == relay and inspection.continuation is None
    envelope = json.loads(inspection.json)
    assert envelope["counts"] == {"branches": 1, "rows": 1, "bytes": inspection.counts.bytes}
    unit = inspection.units[0]
    assert isinstance(unit, nx.InspectionUnit)
    assert unit.kind == nx.InspectionUnitKind.SINGLE and unit.index is None
    assert unit.owner == unit.origin_node and unit.origin_incarnation > 0
    assert isinstance(unit.replicas, list) and len(unit.origin_fingerprint) == 32
    assert unit.origin_generation >= 0 and unit.origin_revision >= 0
    assert unit.origin_stage == nx.InspectionStage.LIVE
    assert unit.counts.rows == 1
    section = unit.sections[0]
    assert isinstance(section, nx.InspectionSection)
    assert section.name and section.omitted_rows == 0
    assert section.watermark_low is not None and section.watermark_high is not None
    assert section.watermark_low != section.watermark_high
    schema = section.schema
    assert schema.fields[0].name == "id" and schema.fields[10].sensitive
    frame = section.frames[0]
    assert isinstance(frame, nx.InspectionFrame) and frame.row_count == 1
    assert [field.name for field in frame.schema.fields] == [field.name for field in schema.fields]
    view = frame.frame
    assert view.readonly
    expected = bytes(view)
    assert bytes(memoryview(frame)) == expected
    refused = execute(reader, "SHOW RELAY typed_rows MATERIALIZED STATE FORMAT JSON;").state_inspection
    assert refused is not None and refused.rejection == nx.InspectionRejection.NOT_MATERIALIZED
    assert refused.counts is None and refused.unit_count is None
    assert refused.continuation is None and refused.units == []
    reader.__exit__(None, None, None)
    reference = weakref.ref(inspection)
    del outcome, inspection, unit, section, frame
    gc.collect()
    assert reference() is None
    assert bytes(view) == expected and schema.fields[0].name == "id"
    view.release()
    # ANCHOR_END: inspection
