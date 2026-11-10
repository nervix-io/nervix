# frozen_string_literal: true

module RubySuite
  class InspectionTest < ClusterTest
    def test_inspection_metadata_and_verified_frames_survive_their_parents
      relay = unique('inspected')
      ingestor = unique('inspect_in')
      completed("CREATE RELAY #{relay} SCHEMA suite_row UNBRANCHED WITH MATERIALIZED STATE LAST BY TIMESTAMP;")
      completed("CREATE CLIENT PRODUCER INGESTOR #{ingestor} FROM SCHEMA suite_row " \
                "MODE ACK SEQUENTIAL ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s " \
                "ON QUIESCE SUSPEND TIMESTAMP NOW TO #{relay} INHERIT ALL UNBRANCHED " \
                'FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;')
      writing = producer(session, ingestor)
      begin
        submission = writing.submit(build(writing.schema, 71), cancel: RubySuite.deadline)
        assert_equal NX::SubmissionResult::COMPLETED, writing.rejoin(submission, cancel: RubySuite.deadline).result
      ensure
        writing.close(cancel: RubySuite.deadline)
      end
      reader = cluster.connect
      begin
        # ANCHOR: inspection
        license_inspection = execute('DESCRIBE LICENSE;', reader).state_inspection
        assert_instance_of NX::Inspection, license_inspection
        assert_nil license_inspection.domain
        assert_nil license_inspection.entity_name
        assert_equal 'LICENSE', license_inspection.entity_kind
        assert_nil license_inspection.rejection
        assert_equal 3, license_inspection.counts.rows
        outcome = execute("SHOW RELAY #{relay} MATERIALIZED STATE FOR UNBRANCHED LIMIT 1 FORMAT JSON;", reader)
        assert_instance_of NX::Outcome, outcome
        inspection = outcome.state_inspection
        assert_instance_of NX::Inspection, inspection
        assert_nil inspection.rejection
        counts = inspection.counts
        assert_instance_of NX::InspectionCounts, counts
        assert_equal [0, 1], [counts.branches, counts.rows]
        assert_operator counts.bytes, :>, 0
        assert_equal 1, inspection.unit_count
        assert_equal [cluster.domain, 'RELAY', relay], [inspection.domain, inspection.entity_kind, inspection.entity_name]
        assert_nil inspection.continuation
        refute_empty inspection.json
        unit = inspection.units.fetch(0)
        assert_instance_of NX::InspectionUnit, unit
        assert_equal NX::InspectionUnitKind::SINGLE, unit.kind
        assert_nil unit.index
        assert_equal unit.owner, unit.origin_node
        assert_operator unit.origin_incarnation, :>, 0
        assert_instance_of Array, unit.replicas
        assert_equal 32, unit.origin_fingerprint.bytesize
        assert_operator unit.origin_generation, :>=, 0
        assert_operator unit.origin_revision, :>=, 0
        assert_equal NX::InspectionStage::LIVE, unit.origin_stage
        assert_equal 1, unit.counts.rows
        section = unit.sections.fetch(0)
        assert_instance_of NX::InspectionSection, section
        refute_empty section.name
        assert_equal 0, section.omitted_rows
        refute_nil section.watermark_low
        refute_nil section.watermark_high
        refute_equal section.watermark_low, section.watermark_high
        schema = section.schema
        assert_equal 'id', schema.fields.fetch(0).name
        assert schema.fields.fetch(10).sensitive
        frame = section.frames.fetch(0)
        assert_instance_of NX::InspectionFrame, frame
        assert_equal 1, frame.row_count
        assert_equal schema.fields.map(&:name), frame.schema.fields.map(&:name)
        buffer = frame.frame
        assert buffer.readonly?
        expected = buffer.get_string
        refused = execute('SHOW RELAY typed_rows MATERIALIZED STATE FORMAT JSON;', reader).state_inspection
        assert_equal NX::InspectionRejection::NOT_MATERIALIZED, refused.rejection
        assert_nil refused.counts
        assert_nil refused.unit_count
        assert_nil refused.continuation
        assert_empty refused.units
        reader.close
        outcome = inspection = unit = section = frame = nil
        GC.start
        GC.compact
        assert_equal expected, buffer.get_string
        assert_equal 'id', schema.fields.fetch(0).name
        # ANCHOR_END: inspection
      ensure
        reader.close
      end
    end
  end
end
