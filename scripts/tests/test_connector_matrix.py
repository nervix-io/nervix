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
            "headers": {"layout": "ordered_repeats", "names": "exact",
                        "layout_meaning": "every header in the order it was written",
                        "names_meaning": "a name matches only itself", "values": None},
            "every_node": False, "position": "replayable",
            "confirmation": "answered",
            "bounds": {
                "buffers": [{"holds": "messages prefetched per partition", "bound": "driver default: queued.min.messages 100000; holds the peer back when full"}],
                "waits": [{"wait": "connection setup", "bound": "driver default: socket.connection.setup.timeout.ms, 30 s"}],
                "driver_limits": ["receive.message.max.bytes 100000000"],
                "deadline_keys": [],
            },
            "template": "CREATE KAFKA TOPIC INGESTOR <name>\n  FROM <client> TOPIC 'orders'\n  MODE <mode> ON QUIESCE <quiesce>\n  DECODE USING <codec> TO <relay> <route>;",
        }],
        "emit": [{
            "entity": "KAFKA TOPIC", "connector": "KAFKA", "reference": "KAFKA client",
            "locator": "TOPIC orders (topic)", "options": [],
            "modes": ["NO_ACK RETRY POLICY BACKOFF 1s MAX 30s"],
            "headers": {"layout": "ordered_repeats", "names": "exact",
                        "layout_meaning": "every header in the order it was written",
                        "names_meaning": "a name matches only itself", "values": None},
            "batch": "optional", "batch_maximum": None, "frame_limits": None,
            "body": "ENCODE USING <codec>", "batch_transformation": False,
            "envelope": [], "properties": [], "value_contract": None,
            "bounds": {
                "buffers": [],
                "waits": [{"wait": "produce request", "bound": "`timeout_ms`; unbounded when omitted"}],
                "driver_limits": [],
                "deadline_keys": ["timeout_ms"],
            },
            "template": "CREATE EMITTER <name>\n  FROM <relay> TO KAFKA <client> TOPIC orders\n  MODE <mode> ENCODE USING <codec>\n  FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
        }],
        "clients": [{"kind": "KAFKA", "pool": "unpooled", "config_owner": "driver", "options": [],
                     "keys": [{"name": "sasl.password", "presence": "optional", "type": "string",
                               "owner": "driver; best effort", "confidential": True, "file": "none",
                               "readers": "every"}],
                     "namespaces": ["all driver property names"], "reserved": ["group.id"], "secret_keys": None,
                     "aliases": [], "stripped_prefix": None, "constraints": []}],
    }


def reasons():
    return {"KAFKA TOPIC": {
        "envelope": "The topic emitter currently writes payloads and headers only.",
        "properties": "Kafka has no AMQP or MQTT property block.",
        "mirror": "Consumption tracks log offsets; publishing waits for producer reports.",
    }}


class ConnectorMatrixTests(unittest.TestCase):
    def test_value_and_client_constraints_follow_the_compiled_declaration(self):
        surface = declarations()
        surface["emit"][0]["value_contract"] = {
            "fields": ["HISTOGRAM `count`: U64; required; the complete U64 range"],
            "attributes": "nested typed arrays; BYTES refused",
            "resource": "literal values only",
            "unsupported": ["OTLP/JSON: protobuf transports only"],
        }
        surface["clients"][0]["constraints"] = ["TLS files require https", "headers preserve percent escapes"]
        text = matrix.render_matrix(surface, reasons())
        for value in ["HISTOGRAM `count`: U64; required; the complete U64 range",
                      "nested typed arrays; BYTES refused", "literal values only",
                      "OTLP/JSON: protobuf transports only", "TLS files require https",
                      "headers preserve percent escapes"]:
            self.assertIn(value, text)
        surface["emit"][0]["value_contract"]["fields"] = ["HISTOGRAM `count`: U64; optional"]
        self.assertNotEqual(text, matrix.render_matrix(surface, reasons()))

    def test_driver_aliases_and_a_stripped_prefix_name_the_keys_a_client_refuses(self):
        surface = declarations()
        without = matrix.render_matrix(surface, reasons())
        self.assertNotIn("Refused driver name", without)
        surface["clients"][0]["aliases"] = [
            {"alias": "metadata.broker.list", "accepted": "bootstrap.servers"},
            {"alias": "request.required.acks", "accepted": "acks"},
        ]
        surface["clients"][0]["stripped_prefix"] = {"prefix": "topic.", "properties": ["topic.blacklist"]}
        text = matrix.render_matrix(surface, reasons())
        for value in ["| Refused driver name | Accepted name |",
                      "| `metadata.broker.list` | `bootstrap.servers` |",
                      "| `request.required.acks` | `acks` |",
                      "strips its `topic.` prefix from a key that names none of its own properties",
                      "whose names begin with it: `topic.blacklist`."]:
            self.assertIn(value, text)
        surface["clients"][0]["stripped_prefix"]["properties"] = []
        self.assertIn("whose names begin with it: none.", matrix.render_matrix(surface, reasons()))

    def test_keys_one_side_reads_name_the_entities_of_that_side(self):
        surface = declarations()
        key = surface["clients"][0]["keys"][0]
        surface["clients"][0]["keys"] = [
            {**key, "name": "framing", "readers": "emitters"},
            {**key, "name": "max_message_size", "readers": "ingestors"},
            {**key, "name": "protocol", "readers": "every"},
            {**key, "name": "retries", "readers": "emitters"},
            {**key, "name": "timeout_ms", "readers": "emitters"},
        ]
        text = matrix.render_matrix(surface, reasons())
        self.assertIn("Only its `KAFKA TOPIC` ingestors read `max_message_size`. "
                      "Only its `KAFKA TOPIC` emitters read `framing`, `retries` and `timeout_ms`. "
                      "Every entity that references the client reads its other keys.", text)
        self.assertNotIn("`protocol`.", text)

        surface["clients"][0]["keys"] = [{**key, "readers": "every"}]
        self.assertNotIn("Only its", matrix.render_matrix(surface, reasons()))

    def test_a_key_for_a_side_without_entities_is_refused(self):
        surface = declarations()
        surface["emit"][0]["connector"] = "PULSAR"
        surface["clients"][0]["keys"][0]["readers"] = "emitters"
        with self.assertRaisesRegex(ValueError, "KAFKA client: `sasl.password` is read only by emitters, "
                                                "but no KAFKA emitter references the client"):
            matrix.render_matrix(surface, reasons())
        surface = declarations()
        surface["clients"][0]["keys"][0]["readers"] = "everyone"
        with self.assertRaisesRegex(ValueError, "KAFKA client: `sasl.password` declares unknown readers 'everyone'"):
            matrix.render_matrix(surface, reasons())

    def test_declared_frame_limits_replace_the_absent_protocol_maximum(self):
        surface = declarations()
        text = matrix.render_matrix(surface, reasons())
        self.assertIn("No additional protocol maximum is declared", text)
        self.assertNotIn("Frame limits", text)

        surface["emit"][0]["frame_limits"] = [
            "udp: a payload of at most 65,507 bytes in one datagram",
            "non-transparent framing: a payload without LF",
        ]
        text = matrix.render_matrix(surface, reasons())
        self.assertIn("| Frame limits | udp: a payload of at most 65,507 bytes in one datagram"
                      "<br>non-transparent framing: a payload without LF |", text)
        self.assertIn("A batch is written as one frame within the frame limits", text)
        self.assertNotIn("No additional protocol maximum is declared", text)

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
                      "producer reports", "no AMQP or MQTT",
                      "sasl.password", "driver; best effort", "all driver property names", "Reserved keys: `group.id`",
                      "messages prefetched per partition: driver default: queued.min.messages 100000",
                      "receive.message.max.bytes 100000000", "produce request: `timeout_ms`",
                      "`STALLED PEER timeout_ms`", "holds no buffer between its transport",
                      "no client deadline key bounds a wait",
                      "`write_header`; `ordered_repeats`, exact names: every header in the order "
                      "it was written; a name matches only itself"]:
            self.assertIn(value, text)

    def test_header_models_group_each_side_and_absent_headers_need_a_reason(self):
        surface = declarations()
        surface["emit"][0]["headers"] = {
            "layout": "last_wins", "names": "lowercase",
            "layout_meaning": "one value per name", "names_meaning": "names are lowercased",
            "values": None}
        headers = matrix.header_models_table(surface)
        self.assertIn("| `last_wins`, lowercase names | None | `KAFKA TOPIC` | one value per name; "
                      "names are lowercased |", headers)
        self.assertIn("| `ordered_repeats`, exact names | `KAFKA TOPIC` | None |", headers)
        self.assertIn("headers", matrix.mirror(surface, "ingest", surface["ingest"][0], reasons()))
        surface["emit"][0]["headers"] = None
        self.assertIn("`KAFKA TOPIC` write no transport headers and refuse `write_header`.",
                      matrix.side_headers(surface, "emit"))
        with self.assertRaisesRegex(ValueError, "emit_headers"):
            matrix.render_matrix(surface, reasons())
        notes = reasons()
        notes["KAFKA TOPIC"]["emit_headers"] = "This emitter writes no transport headers."
        text = matrix.render_matrix(surface, notes)
        self.assertIn("Not offered: This emitter writes no transport headers.", text)

    def test_typed_header_values_name_what_carries_each_type_and_group_apart_from_text(self):
        surface = declarations()
        surface["ingest"][0]["headers"]["layout"] = "last_wins"
        surface["emit"][0]["headers"] = {
            "layout": "last_wins", "names": "exact",
            "layout_meaning": "every header in the order it was written",
            "names_meaning": "a name matches only itself",
            "values": "STRING as String; I64 as Number; BYTES as Binary"}
        headers = matrix.header_models_table(surface)
        self.assertIn("| `last_wins`, exact names | `KAFKA TOPIC` | None |", headers)
        self.assertIn("| `last_wins`, exact names; STRING as String; I64 as Number; BYTES as Binary "
                      "| None | `KAFKA TOPIC` | every header in the order it was written; a name "
                      "matches only itself; a value's exact type decides the type it travels as |",
                      headers)
        self.assertIn("| `KAFKA TOPIC` | `last_wins`, exact names; STRING as String; I64 as Number; "
                      "BYTES as Binary |", matrix.side_headers(surface, "emit"))

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
                "ingestors.md": ["ingest-kafka-topic", "instancing", "ingest-headers"],
                "emitters.md": ["emit-kafka-topic", "emit-headers"],
                "connector-contract.md": ["position", "confirmation", "headers"],
            }.items():
                content = "Retained prose\n" + "\n".join(
                    f"<!-- connector-matrix:{name}:start -->\nstale\n<!-- connector-matrix:{name}:end -->"
                    for name in names)
                (root / "docs/src" / path).write_text(content)
            output = matrix.documents(root, surface, reasons(), policy)
            self.assertEqual(len(output), 4)
            self.assertIn(surface["emit"][0]["template"], output["docs/src/emitters.md"])
            self.assertIn("`answered` | `KAFKA TOPIC`", output["docs/src/connector-contract.md"])
            self.assertIn("| `ordered_repeats`, exact names | `KAFKA TOPIC` | `KAFKA TOPIC` |",
                          output["docs/src/connector-contract.md"])
            self.assertIn("Retained prose", output["docs/src/ingestors.md"])
            self.assertIn("| `KAFKA TOPIC` | `ordered_repeats`, exact names | every header in the "
                          "order it was written; a name matches only itself |",
                          output["docs/src/emitters.md"])
            policy["confirmation"]["undeclared"] = "Not declared by this export."
            with self.assertRaisesRegex(ValueError, "confirmation"):
                matrix.documents(root, surface, reasons(), policy)


if __name__ == "__main__":
    unittest.main()
