"""The cleanup supervisor must preserve failed bodies and refuse incomplete node teardown."""

import json
from pathlib import Path
import tempfile
import unittest

from scripts.backup_cut_cleanup import FAILURE, FEATURE, CleanupCase, scenario_executable, verify


class CleanupVerdictTests(unittest.TestCase):
    def setUp(self) -> None:
        self.output = f"{FAILURE}\n6 scenarios (6 failed)\n"
        self.summary = "Attempts: 6 completed, 0 active; retries: 0.\nReservations: 0.0 slot-seconds beyond one slot per attempt."
        cases = [
            CleanupCase("Plexus", "capture", 1), CleanupCase("Plexus", "capture", 3),
            CleanupCase("command", "lease", 1), CleanupCase("command", "lease", 3),
            CleanupCase("CLI", "capture", 1), CleanupCase("CLI", "capture", 3),
        ]
        self.lines = []
        for line, case in enumerate(cases, start=1):
            self.lines.append(
                f'scenario finished: feature="{FEATURE}" '
                f'scenario="A failed {case.kind} backup body releases a {case.boundary} gate on {case.nodes} nodes" '
                f'line={line} body=step failed: {FAILURE} '
                f'teardown=stopped {case.nodes} of {case.nodes} node(s) in 1s of a 60s budget, 0 still stopping'
            )
        self.background = []
        for line in self.lines:
            if "failed Plexus" in line:
                ending = " kind=plexus"
            elif "failed command" in line:
                ending = " kind=command succeeded=true"
            else:
                ending = " kind=cli succeeded=true"
            self.background.append(
                line.replace("scenario finished:", "backup cleanup finished:").split(" line=")[0] + ending
            )

    def check(self, *, code=101, output=None, lifecycle=None, summary=None, background=None) -> None:
        background = self.background if background is None else background
        lifecycle = "\n".join(self.lines) if lifecycle is None else lifecycle
        verify(code, self.output if output is None else output,
               "\n".join(background) + "\n" + lifecycle,
               self.summary if summary is None else summary)

    def test_six_failed_bodies_and_complete_teardowns_qualify(self) -> None:
        self.check()

    def test_a_passing_or_timed_out_child_cannot_qualify(self) -> None:
        for code in (0, 1, 124, -9):
            with self.subTest(code=code), self.assertRaisesRegex(ValueError, "exit 101"):
                self.check(code=code)

    def test_missing_deliberate_failures_cannot_qualify(self) -> None:
        for output in ("6 scenarios (6 failed)", f"{FAILURE}\n6 scenarios (6 passed)"):
            with self.subTest(output=output), self.assertRaisesRegex(ValueError, "deliberate"):
                self.check(output=output)

    def test_active_or_retried_attempts_cannot_qualify(self) -> None:
        for summary in (self.summary.replace("0 active", "1 active"),
                        self.summary.replace("retries: 0.", "retries: 1.")):
            with self.subTest(summary=summary), self.assertRaisesRegex(ValueError, "attempts"):
                self.check(summary=summary)

    def test_missing_or_duplicate_topologies_cannot_qualify(self) -> None:
        with self.assertRaisesRegex(ValueError, "finished after hooks"):
            self.check(lifecycle="\n".join(self.lines[:3]))
        self.lines[3] = self.lines[2]
        with self.assertRaisesRegex(ValueError, "topology coverage"):
            self.check()

    def test_an_unfinished_node_cannot_qualify(self) -> None:
        self.lines[1] = self.lines[1].replace("stopped 3 of 3", "stopped 2 of 3").replace(
            "0 still stopping", "1 still stopping",
        )
        with self.assertRaisesRegex(ValueError, "stop every node"):
            self.check()

    def test_a_passed_body_cannot_qualify(self) -> None:
        self.lines[0] = self.lines[0].replace(f"step failed: {FAILURE}", "passed")
        with self.assertRaisesRegex(ValueError, "stop every node"):
            self.check()

    def test_a_reported_cleanup_fault_cannot_qualify(self) -> None:
        for fault in ("scenario cleanup unfinished:", "scenario teardown failed:"):
            with self.subTest(fault=fault), self.assertRaisesRegex(ValueError, "panicked node"):
                self.check(lifecycle="\n".join(self.lines) + "\n" + fault)

    def test_hook_or_parsing_errors_cannot_qualify(self) -> None:
        for fault in ("1 hook error(s)", "2 parsing error(s)"):
            with self.subTest(fault=fault), self.assertRaisesRegex(ValueError, "hook or parsing"):
                self.check(output=self.output + fault)

    def test_missing_or_duplicate_background_completions_cannot_qualify(self) -> None:
        for background in ([], self.background[:3], [self.background[0]] * 4):
            with self.subTest(background=background), self.assertRaisesRegex(ValueError, "every case"):
                self.check(background=background)

    def test_a_background_cleanup_failure_cannot_qualify(self) -> None:
        for fault in ("backup cleanup unfinished:", "backup cleanup failed:"):
            with self.subTest(fault=fault), self.assertRaisesRegex(ValueError, "background backup work"):
                self.check(lifecycle="\n".join(self.lines) + "\n" + fault)

    def test_build_manifest_identifies_exactly_one_scenarios_executable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "build.jsonl"
            artifact = {"reason": "compiler-artifact", "target": {"name": "scenarios"},
                        "executable": "/build/scenarios"}
            manifest.write_text(json.dumps(artifact) + "\n")
            self.assertEqual(scenario_executable(manifest), Path("/build/scenarios"))
            artifact["target"]["name"] = "other"
            manifest.write_text(json.dumps(artifact) + "\n")
            with self.assertRaisesRegex(ValueError, "one built scenarios"):
                scenario_executable(manifest)


if __name__ == "__main__":
    unittest.main()
