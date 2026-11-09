package io.nervix.client.suite;

import static org.junit.jupiter.api.Assertions.*;
import static io.nervix.client.suite.support.Cluster.*;
import io.nervix.client.Nervix;
import io.nervix.client.suite.support.Cluster;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Current inspection metadata and independently retained frame/schema handles. */
@Tag("cluster")
public final class InspectionTests {
    @Test
    void inspectionMetadataAndVerifiedFramesSurviveTheirParents() throws Exception {
        var cluster = new Cluster();
        String relay = cluster.unique("inspected"), ingestor = cluster.unique("inspect_in");
        var session = cluster.connect();
        try {
            completed(session, "CREATE RELAY " + relay + " SCHEMA suite_row UNBRANCHED WITH MATERIALIZED STATE LAST BY TIMESTAMP;");
            completed(session, "CREATE CLIENT PRODUCER INGESTOR " + ingestor + " FROM SCHEMA suite_row "
                + "MODE ACK SEQUENTIAL ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s ON QUIESCE SUSPEND "
                + "TIMESTAMP NOW TO " + relay + " INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;");
            try (var writing = cluster.producer(session, ingestor); var schema = writing.schema();
                    var batch = build(schema, 71); var cancel = deadline()) {
                long submission = writing.submit(batch, cancel);
                try (var result = writing.rejoin(submission, cancel)) {
                    assertEquals(Nervix.SubmissionResult.COMPLETED, result.info().result());
                }
            }
            // ANCHOR: inspection
            Nervix.Inspection inspection;
            try (var cancel = deadline(); var outcome = session.command(
                    "SHOW RELAY " + relay + " MATERIALIZED STATE FOR UNBRANCHED LIMIT 1 FORMAT JSON;", cancel)) {
                inspection = outcome.stateInspection();
            }
            assertNotNull(inspection);
            assertNull(inspection.rejection());
            Nervix.InspectionCounts counts = inspection.counts();
            assertEquals(1, counts.branches()); assertEquals(1, counts.rows()); assertTrue(counts.bytes() > 0);
            assertEquals(new Nervix.InspectionCounts(1, 1, counts.bytes()), counts);
            assertEquals(1L, inspection.unitCount()); assertNull(inspection.continuation());
            assertEquals(cluster.domain, inspection.domain()); assertEquals("RELAY", inspection.entityKind());
            assertEquals(relay, inspection.entityName()); assertFalse(inspection.json().isEmpty());
            Nervix.InspectionUnit unit = inspection.units().getFirst();
            inspection.close();
            assertEquals(Nervix.InspectionUnitKind.SINGLE, unit.kind()); assertNull(unit.index());
            assertEquals(unit.owner(), unit.originNode()); assertNotEquals(0, unit.originIncarnation());
            assertNotNull(unit.replicas()); assertEquals(32, unit.originFingerprint().length);
            assertNotEquals(0, unit.originGeneration()); assertNotEquals(0, unit.originRevision());
            assertEquals(Nervix.InspectionStage.LIVE, unit.originStage()); assertEquals(1, unit.counts().rows());
            Nervix.InspectionSection section = unit.sections().getFirst();
            unit.close();
            assertFalse(section.name().isEmpty()); assertEquals(0, section.omittedRows());
            assertNotNull(section.watermarkLow()); assertNotNull(section.watermarkHigh());
            assertNotEquals(section.watermarkLow(), section.watermarkHigh());
            var schema = section.schema();
            assertEquals("id", schema.fields(Nervix.Part.ROWS).getFirst().name());
            assertTrue(schema.fields(Nervix.Part.ROWS).get(10).sensitive());
            Nervix.InspectionFrame frame = section.frames().getFirst();
            section.close();
            assertEquals(1, frame.rowCount());
            try (var frameSchema = frame.schema()) {
                var expectedFields = schema.fields(Nervix.Part.ROWS);
                var actualFields = frameSchema.fields(Nervix.Part.ROWS);
                assertEquals(expectedFields.size(), actualFields.size());
                for (int index = 0; index < expectedFields.size(); index++) {
                    var expectedField = expectedFields.get(index); var actualField = actualFields.get(index);
                    assertEquals(expectedField.name(), actualField.name()); assertEquals(expectedField.levels(), actualField.levels());
                    assertEquals(expectedField.nullable(), actualField.nullable()); assertEquals(expectedField.sensitive(), actualField.sensitive());
                }
            }
            var buffer = frame.frame();
            byte[] expected = new byte[buffer.remaining()]; buffer.duplicate().get(expected);
            assertTrue(buffer.isReadOnly()); frame.close();
            try (var cancel = deadline(); var outcome = session.command("SHOW RELAY typed_rows MATERIALIZED STATE FORMAT JSON;", cancel);
                    var refused = outcome.stateInspection()) {
                assertEquals(Nervix.InspectionRejection.NOT_MATERIALIZED, refused.rejection());
                assertNull(refused.counts()); assertNull(refused.unitCount());
                assertNull(refused.continuation()); assertTrue(refused.units().isEmpty());
            }
            session.close(); System.gc();
            byte[] actual = new byte[buffer.remaining()]; buffer.duplicate().get(actual);
            assertArrayEquals(expected, actual);
            assertEquals("id", schema.fields(Nervix.Part.ROWS).getFirst().name()); schema.close();
            // ANCHOR_END: inspection
        } finally { session.close(); }
    }
}
