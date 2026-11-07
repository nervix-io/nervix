"""Exercise the scenario recipes' argument and environment boundaries without building Rust."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
RECIPES = ("test-scenarios", "test-scenarios-reuse")
COVERAGE_RECIPES = ("coverage-scenarios", "coverage-scenarios-append")


class ScenarioArgumentTests(unittest.TestCase):
    def test_upload_recipes_select_their_public_cases_and_keep_failure(self) -> None:
        cases = (
            (
                "test-resource-upload-identity-scenarios",
                "tests/features/cluster/resource_describe.feature",
                None,
                "Upload retry reports one assigned version|An upload whose installing call loses its reply reports its one assigned version",
                4,
                {
                    "Upload retry reports one assigned version",
                    "An upload whose installing call loses its reply reports its one assigned version",
                },
            ),
            (
                "test-client-upload-conflict-conformance",
                "tests/features/runtime/client_conformance.feature",
                "@client_upload_failure",
                None,
                12,
                {"A <runtime> client distinguishes an upload identity conflict from its completed retry"},
            ),
        )
        with tempfile.TemporaryDirectory(prefix="upload identity arguments ") as directory:
            root = Path(directory)
            capture = root / "arguments.json"
            cargo = root / "cargo"
            cargo.write_text(
                f"#!{sys.executable}\n"
                "import json, os, pathlib, sys\n"
                "pathlib.Path(os.environ['SCENARIO_ARGUMENT_CAPTURE']).write_text(\n"
                "    json.dumps(sys.argv[1:]))\n"
                "sys.exit(37)\n"
            )
            cargo.chmod(0o755)
            environment = os.environ.copy()
            environment["PATH"] = str(root) + os.pathsep + environment["PATH"]
            environment["SCENARIO_ARGUMENT_CAPTURE"] = str(capture)
            from scripts.deloxide_lane import parse_scenarios

            for recipe, feature, tag, pattern, count, names in cases:
                with self.subTest(recipe=recipe):
                    completed = subprocess.run(
                        ["just", "--no-deps", recipe],
                        cwd=ROOT, env=environment, capture_output=True, text=True, timeout=30,
                    )
                    self.assertNotEqual(completed.returncode, 0)
                    self.assertIn("exit code 37", completed.stderr)
                    observed = json.loads(capture.read_text())
                    expected = [
                        "test", "--package", "nervix-scenarios", "--test", "scenarios", "--",
                        "--input", feature,
                    ]
                    if tag is not None:
                        expected.extend(("--tags", tag))
                    if pattern is not None:
                        expected.extend(("--name", pattern))
                    expected.extend(("--concurrency", "2", "--retry", "0"))
                    self.assertEqual(observed, expected)
                    self.assertFalse("--tags" in observed and "--name" in observed,
                                     "Cucumber's name and tag selectors are mutually exclusive")
                    text = (ROOT / feature).read_text()
                    if tag is None:
                        # A name selector ranges over the whole feature, including untagged cases.
                        text = "@recipe_inventory\n" + text
                        tag = "@recipe_inventory"
                    tagged = parse_scenarios(feature, text, frozenset({tag}))
                    selected = tagged
                    if pattern is not None:
                        selected = [scenario for scenario in tagged if re.search(pattern, scenario.name)]
                    self.assertEqual(sum(scenario.runs for scenario in selected), count)
                    self.assertEqual({scenario.name for scenario in selected}, names)

    def test_recipes_keep_the_scenario_build_dependencies(self) -> None:
        dumped = subprocess.run(
            ["just", "--dump", "--dump-format", "json"],
            cwd=ROOT, capture_output=True, text=True, check=True, timeout=30,
        )
        recipes = json.loads(dumped.stdout)["recipes"]
        for recipe in (*RECIPES, *COVERAGE_RECIPES):
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

    def test_coverage_named_filter_reaches_cargo_intact(self) -> None:
        self.assert_coverage_arguments((
            (
                "--input", "tests/features/tools/cli_session.feature",
                "--name", "CLI connects by hostname over https|CLI rejects a TLS certificate for a different DNS hostname",
                "--concurrency", "4", "--retry", "0",
            ),
        ))

    def test_restore_codec_recipe_executes_all_eight_cases_and_preserves_failures(self) -> None:
        self.assert_restore_recipe("test-restore-codec-replies", "@client_restore_codec_rejection", 8)

    def test_restore_history_recipe_executes_all_four_cases_and_preserves_failures(self) -> None:
        self.assert_restore_recipe("test-restore-attempt-history", "@client_restore_uncertainty", 4)

    def assert_restore_recipe(self, recipe_name: str, tag: str, count: int) -> None:
        dumped = subprocess.run(
            ["just", "--dump", "--dump-format", "json"],
            cwd=ROOT, capture_output=True, text=True, check=True, timeout=30,
        )
        recipe = json.loads(dumped.stdout)["recipes"][recipe_name]
        self.assertEqual([item["recipe"] for item in recipe["dependencies"]], [
            "build-deps", "build-test-cli", "build-deadlock-report",
        ])
        with tempfile.TemporaryDirectory(prefix="restore arguments ") as directory:
            root = Path(directory)
            capture = root / "arguments.json"
            cargo = root / "cargo"
            cargo.write_text(
                f"#!{sys.executable}\n"
                "import json, os, pathlib, sys\n"
                "pathlib.Path(os.environ['SCENARIO_ARGUMENT_CAPTURE']).write_text(\n"
                "    json.dumps(sys.argv[1:]))\n"
                "print(os.environ['SCENARIO_SUMMARY'])\n"
                "sys.exit(int(os.environ['SCENARIO_EXIT']))\n"
            )
            cargo.chmod(0o755)
            environment = os.environ.copy()
            # This recipe must also work on the book runner, which has no ripgrep. Keep the
            # real shell/core tools and Python while excluding undeclared host executables.
            for name in ("just", "bash", "sh", "mkdir", "mktemp", "tee", "uname"):
                executable = shutil.which(name)
                self.assertIsNotNone(executable, name)
                (root / name).symlink_to(executable)
            (root / "python3").symlink_to(sys.executable)
            environment["PATH"] = str(root)
            self.assertIsNone(shutil.which("rg", path=environment["PATH"]))
            environment["SCENARIO_ARGUMENT_CAPTURE"] = str(capture)
            arguments = ("--concurrency", "2", "--retry", "0", "two words", "$PATH")
            for summary, cargo_exit, succeeds in (
                (f"{count} scenarios ({count} passed)", 0, True),
                (f"\x1b[32m{count} scenarios ({count} passed)\x1b[0m", 0, True),
                ("0 scenarios", 0, False),
                (f"{count // 2} scenarios ({count // 2} passed)", 0, False),
                (f"{count} scenarios ({count} passed)\n4 scenarios (4 passed)", 0, False),
                (f"{count} scenarios ({count} failed)", 101, False),
            ):
                with self.subTest(summary=summary, cargo_exit=cargo_exit):
                    environment["SCENARIO_SUMMARY"] = summary
                    environment["SCENARIO_EXIT"] = str(cargo_exit)
                    completed = subprocess.run(
                        ["just", "--no-deps", "--set", "cargo_target_dir", str(root / "target"),
                         recipe_name, *arguments],
                        cwd=ROOT, env=environment, capture_output=True, text=True, timeout=30,
                    )
                    self.assertEqual(completed.returncode == 0, succeeds, completed.stderr)
                    self.assertEqual(json.loads(capture.read_text()), [
                        "test", "--package", "nervix-scenarios", "--test", "scenarios", "--",
                        "--tags", tag, *arguments,
                    ])

    def test_coverage_arguments_remain_literal(self) -> None:
        self.assert_coverage_arguments((
            (),
            (
                "--input", "/features with spaces/{first,second}.feature",
                "--name", "^A (named|second) scenario$|a second scenario",
                "--concurrency", "4", "--retry", "0",
            ),
            (
                "", "single'quote", 'double"quote', "back\\slash", "$PATH",
                "$(printf substituted)", "`printf substituted`", "; false", "> output",
                "*", "two\nlines",
            ),
        ))

    def assert_coverage_arguments(self, selections: tuple[tuple[str, ...], ...]) -> None:
        just = shutil.which("just")
        self.assertIsNotNone(just)
        expected_library = subprocess.run(
            ["bash", "scripts/download_onnxruntime.sh", "--print-path"],
            cwd=ROOT, capture_output=True, text=True, check=True, timeout=30,
        ).stdout.strip()
        tool_variables = (
            "ORT_DYLIB_PATH", "NERVIX_DEADLOCK_REPORT_TOOL", "NERVIX_TEST_CLI_PATH",
            "NERVIX_PACED_SIMULATION_PATH", "NERVIX_PACED_SIMULATION_PYTHON", "NERVIX_CLIENT_LIBRARY",
            "NERVIX_TESTCONTAINERS_MODE",
        )
        with tempfile.TemporaryDirectory(prefix="coverage arguments ") as directory:
            root = Path(directory)
            capture = root / "calls.jsonl"
            target = root / "build 'target"
            source_formatter = target / "debug/nervix-plexus-format"
            installed_formatter = target / "llvm-cov-target/debug/nervix-plexus-format"
            source_formatter.parent.mkdir(parents=True)
            installed_formatter.parent.mkdir(parents=True)
            source_formatter.write_bytes(b"fixture formatter\n")
            for tool in ("cargo", "just"):
                recorder = root / tool
                recorder.write_text(
                    f"#!{sys.executable}\n"
                    "import json, os, pathlib, sys\n"
                    f"variables = {tool_variables!r}\n"
                    "with open(os.environ['SCENARIO_ARGUMENT_CAPTURE'], 'a') as capture:\n"
                    "    capture.write(json.dumps({\n"
                    "        'tool': pathlib.Path(sys.argv[0]).name,\n"
                    "        'arguments': sys.argv[1:],\n"
                    "        'environment': {key: os.environ.get(key) for key in variables},\n"
                    "    }) + '\\n')\n"
                )
                recorder.chmod(0o755)
            environment = os.environ.copy()
            environment["PATH"] = str(root) + os.pathsep + environment["PATH"]
            environment["SCENARIO_ARGUMENT_CAPTURE"] = str(capture)
            for variable in tool_variables:
                environment.pop(variable, None)
            expected_environment = dict.fromkeys(tool_variables)
            expected_environment.update({
                "ORT_DYLIB_PATH": expected_library,
                "NERVIX_DEADLOCK_REPORT_TOOL": str(target / "llvm-cov-target/debug/nervix-deadlock-report"),
                "NERVIX_TEST_CLI_PATH": str(target / "llvm-cov-target/debug/nervix-cli"),
                "NERVIX_PACED_SIMULATION_PATH": str(target / "llvm-cov-target/debug/nervix-paced-simulation"),
                "NERVIX_PACED_SIMULATION_PYTHON": str(target / "paced-simulation/python/bin/python"),
                "NERVIX_CLIENT_LIBRARY": str(target / "llvm-cov-target/debug/libnervix_client_ffi.so"),
            })
            output = str(root / "report 'with spaces.lcov")
            for recipe in COVERAGE_RECIPES:
                for arguments in selections:
                    with self.subTest(recipe=recipe, arguments=arguments):
                        capture.unlink(missing_ok=True)
                        installed_formatter.unlink(missing_ok=True)
                        # Execute the production body and formatter install. Only nested builders
                        # and Cargo are recorders; no compiler or external service runs.
                        completed = subprocess.run(
                            [just, "--no-deps", "--set", "cargo_target_dir", str(target),
                             recipe, output, *arguments],
                            cwd=ROOT, env=environment, capture_output=True, text=True, timeout=30,
                        )
                        self.assertEqual(completed.returncode, 0, completed.stderr)
                        calls = [json.loads(line) for line in capture.read_text().splitlines()]
                        self.assertEqual([(call["tool"], call["arguments"]) for call in calls[:-1]], [
                            ("just", ["coverage-cli-binary"]),
                            ("just", ["coverage-deadlock-report-binary"]),
                            ("just", ["coverage-paced-simulation-binaries"]),
                        ])
                        for call in calls[:-1]:
                            self.assertEqual(call["environment"]["ORT_DYLIB_PATH"], expected_library)
                        cargo = calls[-1]
                        self.assertEqual(cargo["tool"], "cargo")
                        expected_arguments = ["llvm-cov"]
                        if recipe == "coverage-scenarios-append":
                            expected_arguments.append("--no-clean")
                        expected_arguments.extend([
                            "--package", "nervix-scenarios", "--test", "scenarios",
                            "--no-default-ignore-filename-regex", "--lcov", "--output-path", output,
                            "--", *arguments,
                        ])
                        self.assertEqual(cargo["arguments"], expected_arguments)
                        self.assertEqual(cargo["environment"], expected_environment)
                        self.assertEqual(installed_formatter.read_bytes(), source_formatter.read_bytes())
                        self.assertEqual(stat.S_IMODE(installed_formatter.stat().st_mode), 0o755)


if __name__ == "__main__":
    unittest.main()
