"""Controls for the external binding client's outcome, effect and settlement evidence."""

import copy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from scripts.chaos import binding_client, binding_evidence, install_binding


def record(sequence):
    return {"event_id": f"run-{sequence}", "sequence": sequence, "content": f"row-{sequence}"}


def passing_events():
    events = []
    for sequence in range(1, 4):
        events.extend(
            [
                {"kind": "submitted", "sequence": sequence, "submission": sequence},
                {
                    "kind": "delivery",
                    "identity": f"identity-{sequence}",
                    "reference": f"reference-{sequence}",
                    "records": [record(sequence)],
                },
                {"kind": "effect", "identity": f"identity-{sequence}", "record": record(sequence)},
                {
                    "kind": "settlement",
                    "identity": f"identity-{sequence}",
                    "reference": f"reference-{sequence}",
                    "result": "confirmed",
                },
                {"kind": "outcome", "sequence": sequence, "submission": sequence, "result": "completed"},
            ]
        )
    events.append({"kind": "finished", "records": 3})
    return events


class BindingEvidenceTests(unittest.TestCase):
    def verify(self, events):
        return binding_evidence.verify([record(i) for i in range(1, 4)], events)

    def assert_fails(self, events, reason):
        verdict = self.verify(events)
        self.assertEqual(verdict["verdict"], "fail", verdict)
        self.assertTrue(any(reason in failure for failure in verdict["failures"]), verdict)

    def test_completed_submissions_have_one_exact_effect_and_confirmed_settlement(self):
        verdict = self.verify(passing_events())
        self.assertEqual(verdict["verdict"], "pass", verdict)
        self.assertEqual(verdict["completed"], 3)
        self.assertEqual(verdict["effects"], 3)
        self.assertEqual(verdict["confirmed_settlements"], 3)

    def test_an_unknown_submission_is_not_assumed_to_have_an_effect(self):
        events = [event for event in passing_events() if event.get("identity") != "identity-2"]
        next(event for event in events if event.get("kind") == "outcome" and event["sequence"] == 2)["result"] = "outcome_unknown"
        self.assertEqual(self.verify(events)["verdict"], "pass")

    def test_a_retry_with_the_same_identity_keeps_its_content_and_effect(self):
        events = passing_events()
        events.insert(2, {"kind": "settlement", "identity": "identity-1", "reference": "earlier", "result": "stale_reference"})
        events.insert(1, {"kind": "delivery", "identity": "identity-1", "reference": "earlier", "records": [record(1)]})
        self.assertEqual(self.verify(events)["verdict"], "pass")

    def test_rejected_processing_is_counted_without_claiming_completion_or_an_effect(self):
        events = [event for event in passing_events() if event.get("identity") != "identity-2"]
        outcome = next(event for event in events if event.get("kind") == "outcome" and event["sequence"] == 2)
        outcome.update(result="processing_failed", failure="rejected", message="acknowledgement rejected")
        verdict = self.verify(events)
        self.assertEqual(verdict["verdict"], "pass", verdict)
        self.assertEqual(verdict["processing_failed"], 1)
        self.assertEqual(verdict["completed"], 2)
        self.assertEqual(verdict["effects"], 2)

    def test_failed_processing_may_have_an_exact_partial_effect(self):
        events = passing_events()
        next(event for event in events if event.get("kind") == "outcome" and event["sequence"] == 2).update(
            result="processing_failed", failure="ack_timed_out", message="acknowledgement timed out")
        verdict = self.verify(events)
        self.assertEqual(verdict["verdict"], "pass", verdict)
        self.assertEqual(verdict["completed"], 2)
        self.assertEqual(verdict["effects"], 3)

    def test_failed_processing_needs_a_declared_typed_cause(self):
        events = passing_events()
        next(event for event in events if event.get("kind") == "outcome" and event["sequence"] == 2).update(
            result="processing_failed", failure="unrecognized", message="unrecognized")
        with self.assertRaisesRegex(ValueError, "processing failure"):
            self.verify(events)

    def test_a_completed_submission_cannot_lose_its_effect(self):
        events = [event for event in passing_events() if not (event["kind"] == "effect" and event["record"]["sequence"] == 2)]
        self.assert_fails(events, "completed record 2")

    def test_duplicate_effects_fail_even_when_both_acknowledgements_confirmed(self):
        events = passing_events()
        extra = copy.deepcopy(events[6:9])
        for event in extra:
            event["identity"] = "another-identity"
            if "reference" in event:
                event["reference"] = "another-reference"
        events[9:9] = extra
        self.assert_fails(events, "more than one effect")

    def test_changed_delivered_content_fails(self):
        events = passing_events()
        events[1]["records"][0]["content"] = "changed"
        self.assert_fails(events, "delivered content")

    def test_a_reference_must_be_settled_once(self):
        events = passing_events()
        events.insert(4, copy.deepcopy(events[3]))
        self.assert_fails(events, "settled more than once")

    def test_missing_settlement_evidence_fails(self):
        events = [event for event in passing_events() if not (event["kind"] == "settlement" and event["identity"] == "identity-2")]
        self.assert_fails(events, "no settlement")

    def test_completed_submission_does_not_invent_a_lost_ack_answer(self):
        events = passing_events()
        events[3]["result"] = "unknown"
        verdict = self.verify(events)
        self.assertEqual(verdict["verdict"], "pass", verdict)
        self.assertEqual(verdict["completed"], 3)
        self.assertEqual(verdict["confirmed_settlements"], 2)

    def test_a_submission_must_never_be_resubmitted_after_uncertainty(self):
        events = passing_events()
        events[4]["result"] = "outcome_unknown"
        events.insert(5, {"kind": "submitted", "sequence": 1, "submission": 4})
        self.assert_fails(events, "submitted more than once")

    def test_missing_outcome_and_incomplete_input_fail(self):
        events = [event for event in passing_events() if event["kind"] not in ("outcome", "finished")]
        self.assert_fails(events, "outcome")
        self.assert_fails(events, "finished")

    def test_vacuous_unknown_tail_is_not_a_recovery_pass(self):
        events = passing_events()
        for event in events:
            if event["kind"] == "outcome":
                event["result"] = "outcome_unknown"
        self.assert_fails(events, "final record")

    def test_unknown_ledger_events_are_not_ignored(self):
        events = passing_events()
        events.append({"kind": "unrecognized"})
        self.assert_fails(events, "unknown event")


class BindingPublisherTests(unittest.TestCase):
    def test_failed_and_unknown_results_advance_without_a_second_submission(self):
        outcomes = iter(["completed", "processing_failed", "outcome_unknown", "completed"])
        sent = []

        class Producer:
            def submit(self, batch, *, cancel):
                sent.append(batch)
                return len(sent)

            def rejoin(self, submission, *, cancel):
                class Result:
                    def __init__(self, name):
                        self.name = name

                    def __repr__(self):
                        return f"SubmissionResult.{self.name.upper()}"

                return SimpleNamespace(result=Result(next(outcomes)), failure=Result("rejected"),
                                       message="acknowledgement rejected")

        events = []
        binding_client.publish_records(
            Producer(), [record(i) for i in range(1, 5)],
            build=lambda row: row, cancel=lambda: None, emit=events.append,
            before_record=lambda sequence: None, interval=0,
        )
        self.assertEqual(sent, [record(i) for i in range(1, 5)])
        results = [event for event in events if event["kind"] == "outcome"]
        self.assertEqual([event["result"] for event in results],
                         ["completed", "processing_failed", "outcome_unknown", "completed"])
        self.assertEqual(results[1]["failure"], "rejected")
        self.assertEqual(results[1]["message"], "acknowledgement rejected")


class BindingConsumerTests(unittest.TestCase):
    def test_a_lost_ack_reply_does_not_repeat_the_ack_or_the_application_effect(self):
        class Value:
            def __init__(self, name):
                self.name = name

            def __repr__(self):
                return f"Result.{self.name}"

        class ClientError(Exception):
            kind = Value("TRANSPORT")

        nx = SimpleNamespace(
            Cancel=lambda **kwargs: None, ClientError=ClientError,
            DeadlineError=type("DeadlineError", (Exception,), {}),
            ConsumerInterruptedError=type("ConsumerInterruptedError", (Exception,), {}),
        )
        client = binding_client.BindingClient(nx, SimpleNamespace(timeout=10), [record(1)])
        events, acknowledgements = [], []
        client.ledger.emit = events.append

        class Delivery:
            identity = b"identity"
            members = 1
            batch = SimpleNamespace(row_count=1, column=lambda column: [list(record(1).values())[column]])

            def __init__(self, reference):
                self.reference = reference

            def ack(self, *, cancel):
                acknowledgements.append(self.reference)
                if self.reference == b"first":
                    raise ClientError("the ACK reply was lost")
                return Value("CONFIRMED")

        deliveries = iter([Delivery(b"first"), Delivery(b"retry")])

        class Consumer:
            def next(self, *, cancel):
                delivery = next(deliveries)
                if delivery.reference == b"retry":
                    client.published.set()
                return delivery

        client.consume(Consumer())
        self.assertEqual(acknowledgements, [b"first", b"retry"])
        self.assertEqual([event["record"] for event in events if event["kind"] == "effect"], [record(1)])
        self.assertEqual([event["result"] for event in events if event["kind"] == "settlement"],
                         ["unknown", "confirmed"])


class BindingArtifactTests(unittest.TestCase):
    def test_prepared_wheel_is_installed_offline_and_its_exact_digest_is_recorded(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            wheel = root / "nervix_client-fixture.whl"
            wheel.write_bytes(b"a prepared wheel fixture")
            destination = root / "installed"
            destination.mkdir()
            revision = "a" * 40
            with patch("venv.EnvBuilder.create") as create, patch("subprocess.run") as run:
                install_binding.install(root, destination, revision)
            create.assert_called_once_with(destination)
            self.assertEqual(run.call_count, 2)
            self.assertIn("--no-index", run.call_args_list[0].args[0])
            self.assertIn("--no-deps", run.call_args_list[0].args[0])
            self.assertEqual(json.loads((destination / "provenance.json").read_text()), {
                "revision": revision, "wheel": wheel.name,
                "wheel_sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
            })

    def test_missing_and_ambiguous_wheels_and_unidentified_source_are_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(ValueError, "source revision"):
                install_binding.install(root, root / "installed", "")
            with self.assertRaisesRegex(ValueError, "exactly one"):
                install_binding.install(root, root / "installed", "a" * 40)
            for name in ("first", "second"):
                (root / f"nervix_client-{name}.whl").touch()
            with self.assertRaisesRegex(ValueError, "exactly one"):
                install_binding.install(root, root / "installed", "a" * 40)

    def test_truncated_evidence_is_a_controller_error_with_no_product_verdict(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "input").write_text(json.dumps(record(1)) + "\n")
            (root / "ledger").write_text('{"kind":')
            result = subprocess.run([
                sys.executable, "scripts/chaos/binding_evidence.py", "--input", str(root / "input"),
                "--ledger", str(root / "ledger"), "--result", str(root / "result"),
            ], capture_output=True, text=True, check=False)
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertFalse((root / "result").exists())


if __name__ == "__main__":
    unittest.main()
