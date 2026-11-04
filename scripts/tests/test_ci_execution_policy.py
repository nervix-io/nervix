"""The temporary CI execution policy, checked without running a diagnostic or fuzz target."""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts.coverage_full import Provider, Selection
from scripts.tests.test_native_coverage import job_section, step_section


ROOT = Path(__file__).resolve().parents[2]


class ExecutionPolicyTests(unittest.TestCase):
    def selection(self, provider: Provider) -> Selection:
        return Selection(
            event="pull_request", labels=("deloxide", "fuzz", "extra-primitives", "shuttle"),
            revision="a" * 40, provider=provider,
            run="1" if provider is Provider.GITHUB else None,
            attempt=1 if provider is Provider.GITHUB else None,
            tests_required=True, bolero_required=True, jobs={},
        )

    def test_ci_requires_permitted_producers_even_when_banned_labels_are_present(self) -> None:
        selection = self.selection(Provider.GITHUB)
        required = {item.producer for item in selection.required()}
        self.assertEqual(required, {
            "ordinary", "test-primitives-shuttle", "test-primitives-loom",
            "test-primitives-turmoil", "test-deadlock-report", "test-shuttle",
        })
        self.assertFalse(selection.fuzz_requested())
        self.assertEqual(selection.required_jobs(), {"coverage", "extra-tests", "diagnostic-evidence", "shuttle", "bolero"})

    def test_local_collections_keep_their_declared_execution_modes(self) -> None:
        required = self.selection(Provider.LOCAL).required()
        self.assertTrue(any(item.mode == "deloxide-order" for item in required))
        self.assertTrue(any(item.mode == "fuzz" for item in required))

    def test_no_label_or_event_can_enable_the_banned_jobs(self) -> None:
        for filename, names in {
            "check.yaml": ("deloxide",),
            "bolero.yaml": ("bolero-fuzz", "bolero-fuzz-combined"),
            "docker-build.yaml": ("build-diagnostic", "chaos-smoke-diagnostic", "chaos-soak-diagnostic"),
        }.items():
            workflow = (ROOT / ".github/workflows" / filename).read_text()
            for name in names:
                with self.subTest(workflow=filename, job=name):
                    job = job_section(workflow, name)
                    self.assertIn("\n    if: ${{ false }}\n", job)
        bolero = (ROOT / ".github/workflows/bolero.yaml").read_text()
        gate = step_section(job_section(bolero, "bolero-gate"), "Require the requested ordinary properties and fuzzing")
        self.assertIn('FUZZ_REQUIRED: "false"', gate)
        chaos = (ROOT / ".github/workflows/chaos.yaml").read_text()
        self.assertIn("\n    if: inputs.image-kind == 'ordinary'\n", job_section(chaos, "plan"))

    def test_always_on_diagnostics_and_opt_in_primitives_select_only_permitted_execution(self) -> None:
        workflow = (ROOT / ".github/workflows/check.yaml").read_text()
        diagnostic = job_section(workflow, "diagnostic-evidence")
        self.assertIn("run: just coverage-native-extras test-deadlock-report --output target/deadlock.lcov\n", diagnostic)
        primitives = step_section(job_section(workflow, "extra-tests"), "Primitive boundary conformance")
        self.assertIn("test-primitives-ordinary test-primitives-shuttle test-primitives-loom test-primitives-turmoil", primitives)
        compile_checks = step_section(job_section(workflow, "extra-tests"), "Primitive boundary compile and browser checks")
        self.assertIn("run: just test-primitives-compile ''\n", compile_checks)

    def test_ci_primitive_documentation_recipe_preserves_ordinary_and_browser_checks(self) -> None:
        # Record the actual recipe's Cargo boundary, without compiling or executing any Rust test.
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            cargo = path / "cargo"
            cargo.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$CARGO_POLICY_RECORD"\n')
            cargo.chmod(0o755)
            record = path / "calls"
            for arguments, modes in (([""], ()), ([], ("deloxide", "deloxide-order", "deloxide-stress"))):
                with self.subTest(arguments=arguments):
                    record.unlink(missing_ok=True)
                    result = subprocess.run(
                        ["just", "test-primitives-compile", *arguments], cwd=ROOT,
                        env={**os.environ, "PATH": f"{path}:{os.environ['PATH']}", "CARGO_POLICY_RECORD": str(record)},
                        capture_output=True, text=True, timeout=30,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(record.read_text().splitlines(), [
                        "test --package nervix-primitives --features native --doc",
                        *[f"test --package nervix-primitives --features {mode} native --doc" for mode in modes],
                        "check --package nervix-primitives --lib --target wasm32-unknown-unknown",
                    ])


if __name__ == "__main__":
    unittest.main()
