"""The connector manual follows declarations and refuses unexplained differences."""

import copy
import contextlib
import io
import tempfile
import unittest
import subprocess
from pathlib import Path
from unittest import mock

from scripts import connector_matrix as matrix


def declarations():
    return {
        "ingest": [{
            "entity": "KAFKA TOPIC", "connector": "KAFKA", "reference": "KAFKA client",
            "locator": "TOPIC 'orders' (kafka_topic)", "options": [],
            "modes": ["NO ACK", "ACK SEQUENTIAL ACK TIMEOUT 5s"],
            "quiesce": ["SUSPEND"], "metadata": ["metadata.offset: I64 OPTIONAL"],
            "headers": True, "every_node": False, "position": "replayable",
            "confirmation": "answered", "template": "CREATE KAFKA TOPIC INGESTOR <name>\n  FROM <client> TOPIC 'orders'\n  MODE <mode> ON QUIESCE <quiesce>\n  DECODE USING <codec> TO <relay> <route>;",
        }],
        "emit": [{
            "entity": "KAFKA TOPIC", "connector": "KAFKA", "reference": "KAFKA client",
            "locator": "TOPIC orders (topic)", "options": [],
            "modes": ["NO_ACK RETRY POLICY BACKOFF 1s MAX 30s"],
            "headers": True, "batch": "optional", "batch_maximum": None,
            "body": "ENCODE USING <codec>", "batch_transformation": False,
            "envelope": [], "properties": [], "template": "CREATE EMITTER <name>\n  FROM <relay> TO KAFKA <client> TOPIC orders\n  MODE <mode> ENCODE USING <codec>\n  FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
        }],
        "clients": [{"kind": "KAFKA", "pool": "unpooled", "config_owner": "driver", "options": []}],
    }


def reasons():
    return {"KAFKA TOPIC": {
        "envelope": "The topic emitter currently writes payloads and headers only.",
        "properties": "Kafka has no AMQP or MQTT property block.",
        "mirror": "Consumption tracks log offsets; publishing waits for producer reports.",
    }}


class ConnectorMatrixTests(unittest.TestCase):
    def test_compiled_export_is_atomic_and_keeps_the_previous_complete_input_on_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "declarations.json"
            path.write_text("current complete input")
            with mock.patch.object(matrix.subprocess, "run", side_effect=subprocess.CalledProcessError(1, "cargo")):
                with self.assertRaises(subprocess.CalledProcessError):
                    matrix.export_declarations(path)
            self.assertEqual(path.read_text(), "current complete input")
            self.assertEqual(list(path.parent.iterdir()), [path])

            def compiled(command, *, cwd, stdout, check):
                self.assertEqual(command, ["cargo", "run", "--quiet", "--package", "nervix-models", "--example", "connector_capabilities"])
                self.assertEqual(cwd, matrix.ROOT)
                self.assertTrue(check)
                stdout.write(b'{"current": "declaration"}')

            with mock.patch.object(matrix.subprocess, "run", side_effect=compiled):
                matrix.export_declarations(path)
            self.assertEqual(path.read_text(), '{"current": "declaration"}')
            self.assertEqual(list(path.parent.iterdir()), [path])

    def test_renders_all_declared_fields_and_explains_absence(self):
        text = matrix.render_matrix(declarations(), reasons())
        for value in ["KAFKA TOPIC", "metadata.offset: I64 OPTIONAL", "replayable", "answered",
                      "kafka_topic", "ACK SEQUENTIAL ACK TIMEOUT 5s", "unpooled", "driver",
                      "producer reports", "no AMQP or MQTT"]:
            self.assertIn(value, text)

    def test_each_absence_and_mirror_requires_a_nonempty_reason(self):
        for field in reasons()["KAFKA TOPIC"]:
            with self.subTest(field=field):
                notes = reasons()
                del notes["KAFKA TOPIC"][field]
                with self.assertRaisesRegex(ValueError, field):
                    matrix.render_matrix(declarations(), notes)
        notes = reasons()
        notes["KAFKA TOPIC"]["mirror"] = "   "
        with self.assertRaisesRegex(ValueError, "mirror"):
            matrix.render_matrix(declarations(), notes)

    def test_rejects_stale_explanations_and_duplicate_entities(self):
        notes = reasons()
        notes["KAFKA TOPIC"]["metadata"] = "Not exposed."
        with self.assertRaisesRegex(ValueError, "metadata"):
            matrix.render_matrix(declarations(), notes)
        surface = declarations()
        surface["ingest"].append(copy.deepcopy(surface["ingest"][0]))
        with self.assertRaisesRegex(ValueError, "duplicate"):
            matrix.render_matrix(surface, reasons())

    def test_declared_change_invalidates_generated_output(self):
        surface = declarations()
        before = matrix.render_matrix(surface, reasons())
        surface["ingest"][0]["confirmation"] = "written"
        after = matrix.render_matrix(surface, reasons())
        self.assertNotEqual(before, after)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "matrix.md").write_text(before)
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertFalse(matrix.publish(root, {"matrix.md": after}, check=True))
            self.assertEqual((root / "matrix.md").read_text(), before)
            self.assertTrue(matrix.publish(root, {"matrix.md": after}, check=False))
            self.assertTrue(matrix.publish(root, {"matrix.md": after}, check=True))

    def test_generated_regions_preserve_prose_and_refuse_missing_or_duplicate_markers(self):
        text = "Before\n<!-- connector-matrix:sample:start -->\nstale\n<!-- connector-matrix:sample:end -->\nAfter\n"
        self.assertEqual(matrix.region(text, "sample", "current\n"), text.replace("stale", "current"))
        for invalid in ["", text + text, text.replace(":end", ":finish")]:
            with self.subTest(invalid=invalid):
                with self.assertRaisesRegex(ValueError, "sample"):
                    matrix.region(invalid, "sample", "current\n")

    def test_markdown_cells_preserve_pipes_and_newlines(self):
        self.assertEqual(matrix.cell("x | y\nz"), "x \\| y<br>z")
        self.assertEqual(matrix.cell("SCHEMA <schema> & VALUES"), "SCHEMA &lt;schema&gt; &amp; VALUES")

    def test_refuses_empty_exports_and_duplicate_clients(self):
        for family in ("ingest", "emit", "clients"):
            surface = declarations()
            surface[family] = []
            with self.assertRaisesRegex(ValueError, family):
                matrix.render_matrix(surface, reasons())
        surface = declarations()
        surface["clients"].append(surface["clients"][0])
        with self.assertRaisesRegex(ValueError, "duplicate client"):
            matrix.render_matrix(surface, reasons())

    def test_all_manual_regions_and_acknowledgement_membership_follow_the_export(self):
        surface = declarations()
        policy = {"position": {"replayable": "Positions can be replayed."},
                  "confirmation": {"answered": "The transport answered."}}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "docs/src").mkdir(parents=True)
            for path, names in {
                "ingestors.md": ["ingest-kafka-topic", "instancing"],
                "emitters.md": ["emit-kafka-topic"],
                "connector-contract.md": ["position", "confirmation"],
            }.items():
                content = "Retained prose\n" + "\n".join(
                    f"<!-- connector-matrix:{name}:start -->\nstale\n<!-- connector-matrix:{name}:end -->"
                    for name in names)
                (root / "docs/src" / path).write_text(content)
            output = matrix.documents(root, surface, reasons(), policy)
            self.assertEqual(len(output), 4)
            self.assertIn(surface["emit"][0]["template"], output["docs/src/emitters.md"])
            self.assertIn("`answered` | `KAFKA TOPIC`", output["docs/src/connector-contract.md"])
            self.assertIn("Retained prose", output["docs/src/ingestors.md"])
            policy["confirmation"]["queued"] = "Not declared by this export."
            with self.assertRaisesRegex(ValueError, "confirmation"):
                matrix.documents(root, surface, reasons(), policy)


if __name__ == "__main__":
    unittest.main()
