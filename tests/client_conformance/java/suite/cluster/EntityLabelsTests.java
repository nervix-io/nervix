package io.nervix.client.suite;

import static org.junit.jupiter.api.Assertions.*;
import static io.nervix.client.suite.support.Cluster.*;
import io.nervix.client.Nervix;
import io.nervix.client.suite.support.Cluster;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Exact metadata values and independently retained committed and staged views. */
@Tag("cluster")
public final class EntityLabelsTests {
    @Test
    void entityLabelsKeepExactValuesRevisionsAndStagedViews() throws Exception {
        var cluster = new Cluster();
        String name = cluster.unique("labelled");
        try (var session = cluster.connect()) {
            completed(session, "CREATE SCHEMA " + name + " ( value U32 ) LABELS { 'empty' = '', 'owner' = ' Δ ' };");
            // ANCHOR: entity_labels
            Nervix.EntityLabelsInspection labels;
            try (var cancel = deadline(); var outcome = session.command("SHOW LABELS ON SCHEMA " + name + " FORMAT TEXT;", cancel)) {
                labels = outcome.entityLabels();
            }
            try (labels) {
                assertEquals(cluster.domain, labels.domain());
                assertEquals("SCHEMA", labels.entityKind());
                assertEquals(name, labels.entityName());
                assertEquals(Map.of("empty", "", "owner", " Δ "), labels.labels());
                assertEquals(Nervix.EntityLabelViewKind.COMMITTED, labels.view());
                assertEquals(1, labels.view().code());
                assertEquals(2, Nervix.EntityLabelViewKind.STAGED.code());
                assertNotNull(labels.revision());
                assertNull(labels.transactionId()); assertNull(labels.position()); assertNull(labels.planningBasis());
                assertTrue(labels.policyRevision() > 0);
                assertEquals(64, labels.maxCount()); assertEquals(128, labels.maxKeyBytes());
                assertEquals(1024, labels.maxValueBytes()); assertEquals(16384, labels.maxBytes());
                completed(session, "BEGIN;");
                completed(session, "SET LABELS ON SCHEMA " + name + " { 'owner' = 'Platform', 'team/path' = 'east' };");
                Nervix.EntityLabelsInspection staged;
                try (var cancel = deadline(); var outcome = session.command("SHOW LABELS ON SCHEMA " + name + ";", cancel)) {
                    staged = outcome.entityLabels();
                }
                try (staged) {
                    assertEquals(Nervix.EntityLabelViewKind.STAGED, staged.view());
                    assertNull(staged.revision()); assertFalse(staged.transactionId().isEmpty());
                    assertEquals(1L, staged.position()); assertEquals(32, staged.planningBasis().length);
                    assertEquals(Map.of("empty", "", "owner", "Platform", "team/path", "east"), staged.labels());
                    completed(session, "REVERT;");
                    try (var cancel = deadline(); var outcome = session.command("SHOW LABELS ON SCHEMA " + name + " FORMAT JSON;", cancel);
                            var committed = outcome.entityLabels()) {
                        assertEquals(labels.labels(), committed.labels());
                        assertEquals(Nervix.EntityLabelViewKind.COMMITTED, committed.view());
                        assertTrue(Long.compareUnsigned(committed.revision(), labels.revision()) >= 0);
                    }
                    assertEquals("Platform", staged.labels().get("owner"));
                }
                assertEquals(" Δ ", labels.labels().get("owner"));
            }
            // ANCHOR_END: entity_labels
        }
    }
}
