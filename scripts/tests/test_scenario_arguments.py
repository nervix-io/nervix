"""Exercise the scenario recipes' argument and environment boundaries without building Rust."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
RECIPES = ("test-scenarios", "test-scenarios-reuse")


class ScenarioArgumentTests(unittest.TestCase):
    def test_both_recipes_keep_the_scenario_build_dependencies(self) -> None:
        dumped = subprocess.run(
            ["just", "--dump", "--dump-format", "json"],
            cwd=ROOT, capture_output=True, text=True, check=True, timeout=30,
        )
        recipes = json.loads(dumped.stdout)["recipes"]
        for recipe in RECIPES:
            with self.subTest(recipe=recipe):
                dependencies = recipes[recipe]["dependencies"]
                self.assertIn("tests-deps", [item["recipe"] for item in dependencies])

    def test_arguments_reach_cargo_intact_with_the_scenario_environment(self) -> None:
        selections = (
            (),
            (
                "--input", "tests/features/runtime/session_protocol.feature",
                "--name", "A leader that cannot confirm its lease",
                "--concurrency", "4", "--retry", "0",
            ),
            (
                "--input", "/features with spaces/{first,second}.feature",
                "--name", "^A leader that cannot confirm its lease$|a second scenario",
                "--concurrency", "4", "--retry", "0",
            ),
            (
                "", "single'quote", 'double"quote', "back\\slash", "$PATH",
                "$(printf substituted)", "`printf substituted`", "; false", "> output",
                "*", "two\nlines",
            ),
        )
        expected_library = subprocess.run(
            ["bash", "scripts/download_onnxruntime.sh", "--print-path"],
            cwd=ROOT, capture_output=True, text=True, check=True, timeout=30,
        ).stdout.strip()
        with tempfile.TemporaryDirectory(prefix="scenario arguments ") as directory:
            root = Path(directory)
            capture = root / "arguments.json"
            cargo = root / "cargo"
            cargo.write_text(
                f"#!{sys.executable}\n"
                "import json, os, pathlib, sys\n"
                "pathlib.Path(os.environ['SCENARIO_ARGUMENT_CAPTURE']).write_text(json.dumps({\n"
                "    'arguments': sys.argv[1:],\n"
                "    'library': os.environ.get('ORT_DYLIB_PATH'),\n"
                "    'containers': os.environ.get('NERVIX_TESTCONTAINERS_MODE'),\n"
                "}))\n"
            )
            cargo.chmod(0o755)
            environment = os.environ.copy()
            environment["PATH"] = str(root) + os.pathsep + environment["PATH"]
            environment["SCENARIO_ARGUMENT_CAPTURE"] = str(capture)
            environment.pop("NERVIX_TESTCONTAINERS_MODE", None)
            for recipe in RECIPES:
                for arguments in selections:
                    with self.subTest(recipe=recipe, arguments=arguments):
                        capture.unlink(missing_ok=True)
                        # Dependencies are asserted above; this executes the real recipe body with
                        # a recording Cargo boundary, so no compiler or external service runs.
                        completed = subprocess.run(
                            ["just", "--no-deps", recipe, *arguments],
                            cwd=ROOT, env=environment, capture_output=True, text=True, timeout=30,
                        )
                        self.assertEqual(completed.returncode, 0, completed.stderr)
                        observed = json.loads(capture.read_text())
                        self.assertEqual(observed["arguments"], [
                            "test", "--package", "nervix-scenarios", "--test", "scenarios", "--",
                            *arguments,
                        ])
                        self.assertEqual(observed["library"], expected_library)
                        expected_mode = "reusable" if recipe == "test-scenarios-reuse" else None
                        self.assertEqual(observed["containers"], expected_mode)


if __name__ == "__main__":
    unittest.main()
