"""The Python suite's action adapter over real pipes, without a cluster or binding import."""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[2] / "tests/client_conformance/python/suite/actions.py"
SPEC = importlib.util.spec_from_file_location("suite_actions", SOURCE)
assert SPEC is not None and SPEC.loader is not None
actions = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(actions)


class ActionAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.input_read, self.input_write = os.pipe()
        self.output_read, self.output_write = os.pipe()
        for descriptor in (self.input_read, self.input_write, self.output_read, self.output_write):
            self.addCleanup(os.close, descriptor)
        self.harness = actions.Harness(
            input_fd=self.input_read, output_fd=self.output_write, within=10.0
        )

    def test_each_request_waits_for_its_own_numbered_confirmation(self) -> None:
        for sequence, action, perform in (
            (1, "refuse-next-subscription-opening", self.harness.refuse_next_subscription_opening),
            (2, "clear-subscription-opening-refusals", self.harness.clear_subscription_opening_refusals),
            (3, "refuse-next-clock-attachment", self.harness.refuse_next_clock_attachment),
            (4, "clear-clock-attachment-refusals", self.harness.clear_clock_attachment_refusals),
        ):
            os.write(
                self.input_write,
                f"NERVIX-SUITE-CONFIRMED {sequence} {action}\n".encode(),
            )
            perform()
            self.assertEqual(
                os.read(self.output_read, 256),
                f"\nNERVIX-SUITE-ACTION {sequence} {action}\n".encode(),
            )

    def test_a_foreign_confirmation_fails_instead_of_claiming_the_fault_was_armed(self) -> None:
        for reply in (
            b"NERVIX-SUITE-CONFIRMED 2 refuse-next-subscription-opening\n",
            b"NERVIX-SUITE-CONFIRMED 1 unknown-action\n",
            b"not a confirmation\n",
        ):
            with self.subTest(reply=reply):
                self.harness.sequence = 0
                os.write(self.input_write, reply)
                with self.assertRaisesRegex(AssertionError, "unexpected scenario action confirmation"):
                    self.harness.refuse_next_subscription_opening()

    def test_an_unconfirmed_action_ends_at_its_deadline(self) -> None:
        self.harness.within = 0.01
        with self.assertRaisesRegex(AssertionError, "did not confirm"):
            self.harness.refuse_next_subscription_opening()

    def test_a_confirmation_without_its_terminator_ends_at_its_deadline(self) -> None:
        self.harness.within = 0.01
        os.write(self.input_write, b"NERVIX-SUITE-CONFIRMED 1 refuse-next-subscription-opening")
        with self.assertRaisesRegex(AssertionError, "did not confirm"):
            self.harness.refuse_next_subscription_opening()

    def test_a_confirmation_cannot_grow_without_bound(self) -> None:
        os.write(self.input_write, b"x" * 257)
        with self.assertRaisesRegex(AssertionError, "exceeds 256 bytes"):
            self.harness.refuse_next_subscription_opening()

    def test_a_closed_channel_does_not_confirm_the_action(self) -> None:
        reader, writer = os.pipe()
        self.addCleanup(os.close, reader)
        os.close(writer)
        self.harness.input_fd = reader
        with self.assertRaisesRegex(AssertionError, "closed its action channel"):
            self.harness.refuse_next_subscription_opening()

    def test_the_runner_keeps_native_fd_capture_and_the_control_pipes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            test = Path(directory) / "test_capture.py"
            test.write_text(textwrap.dedent('''
                import os
                from actions import Harness

                def test_the_channel_survives_capture(suite_action_pipes, capfd, request):
                    assert request.config.getoption("capture") == "fd"
                    os.write(1, b"native stdout marker\\n")
                    os.write(2, b"native stderr marker\\n")
                    channel = Harness(
                        within=10,
                        input_fd=suite_action_pipes.input_fd,
                        output_fd=suite_action_pipes.output_fd,
                    )
                    channel.refuse_next_subscription_opening()
                    captured = capfd.readouterr()
                    assert captured.out == "native stdout marker\\n"
                    assert captured.err == "native stderr marker\\n"
            '''))
            environment = dict(os.environ)
            environment["PYTHONPATH"] = str(SOURCE.parent)
            environment["PYTHONDONTWRITEBYTECODE"] = "1"
            run = subprocess.run(
                [sys.executable, "-m", "cluster_runner", "--capture=fd", "-q", str(test)],
                cwd=directory,
                env=environment,
                input=b"NERVIX-SUITE-CONFIRMED 1 refuse-next-subscription-opening\n",
                capture_output=True,
                timeout=30,
                check=False,
            )
            self.assertEqual(run.returncode, 0, (run.stdout, run.stderr))
            self.assertIn(b"NERVIX-SUITE-ACTION 1 refuse-next-subscription-opening\n", run.stdout)


if __name__ == "__main__":
    unittest.main()
