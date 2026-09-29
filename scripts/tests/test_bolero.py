"""Focused tests for the Bolero inventory and command runner."""

from __future__ import annotations

import dataclasses
import contextlib
import io
import pathlib
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from scripts import bolero


class InventoryTests(unittest.TestCase):
    def test_inventory_has_four_current_targets_and_exact_corpus_paths(self) -> None:
        inventory = bolero.load_inventory()
        self.assertEqual(len(inventory.targets), 4)
        self.assertEqual({target.package for target in inventory.targets},
                         {"nervix-nspl", "nervix-backup"})
        for target in inventory.targets:
            self.assertTrue(target.source.is_file())
            self.assertTrue(target.corpus.is_dir())
            self.assertEqual(target.test_target, "lib")
            self.assertEqual(target.corpus.parent.name, target.test.replace("::", "__"))
            self.assertGreater(target.random_iterations, 0)

    def test_invalid_case_budget_and_duplicate_id_fail(self) -> None:
        text = bolero.INVENTORY.read_text()
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "inventory.toml"
            path.write_text(text.replace("random_iterations = 256",
                                         "random_iterations = 0", 1))
            with self.assertRaisesRegex(bolero.BoleroError, "positive integer"):
                bolero.load_inventory(path)
            path.write_text(text.replace('id = "nspl-model"',
                                         'id = "nspl-expression"', 1))
            with self.assertRaisesRegex(bolero.BoleroError, "duplicate id"):
                bolero.load_inventory(path)

    def test_invalid_corpus_and_modeled_feature_fail(self) -> None:
        text = bolero.INVENTORY.read_text()
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "inventory.toml"
            path.write_text(text.replace(
                "statement__tests__bolero_expression_roundtrip_minimal_parentheses/corpus",
                "wrong/corpus", 1,
            ))
            with self.assertRaisesRegex(bolero.BoleroError, "actual work directory"):
                bolero.load_inventory(path)
            path.write_text(text.replace("features = []",
                                         'features = ["loom"]', 1))
            with self.assertRaisesRegex(bolero.BoleroError, "modeled feature"):
                bolero.load_inventory(path)

    def test_empty_selection_fails(self) -> None:
        with self.assertRaisesRegex(bolero.BoleroError, "no Bolero targets selected"):
            bolero.select(bolero.load_inventory(), "absent-target")


class DiscoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.inventory = bolero.load_inventory()
        self.manifests = bolero.package_manifests()

    def compiled(self, package: str, test_target: str) -> list[dict[str, str]]:
        return [
            {
                "package_name": target.package,
                "test_name": target.test,
                "work_dir": str(target.work_dir),
            }
            for target in self.inventory.targets
            if target.package == package and target.test_target == test_target
        ]

    def source_functions(self, manifest: pathlib.Path) -> dict[str, pathlib.Path]:
        package = next(name for name, path in self.manifests.items() if path == manifest)
        return {
            target.test.split("::")[-1]: target.source
            for target in self.inventory.targets
            if target.package == package
        }

    def test_compiled_discovery_accepts_exact_current_targets(self) -> None:
        with (
            mock.patch.object(bolero, "static_targets", side_effect=self.source_functions),
            mock.patch.object(bolero, "listed_tests",
                              side_effect=lambda package, test_target, ignored:
                              [] if ignored else
                              [item["test_name"] for item in self.compiled(package, test_target)]),
            mock.patch.object(bolero, "compiled_targets", side_effect=self.compiled),
        ):
            bolero.discover(self.inventory)

    def test_ignored_and_unregistered_targets_fail(self) -> None:
        with (
            mock.patch.object(bolero, "static_targets", side_effect=self.source_functions),
            mock.patch.object(bolero, "listed_tests",
                              side_effect=lambda package, test_target, ignored:
                              ["tests::bolero_ignored"] if ignored else
                              [item["test_name"] for item in self.compiled(package, test_target)]),
            mock.patch.object(bolero, "compiled_targets", side_effect=self.compiled),
        ):
            with self.assertRaisesRegex(bolero.BoleroError, "ignored"):
                bolero.discover(self.inventory)

        def unregistered(manifest: pathlib.Path) -> dict[str, pathlib.Path]:
            found = self.source_functions(manifest)
            if manifest == self.manifests["nervix-backup"]:
                found["bolero_new"] = manifest.parent / "src/lib.rs"
            return found

        with mock.patch.object(bolero, "static_targets", side_effect=unregistered):
            with self.assertRaisesRegex(bolero.BoleroError, "differ from inventory"):
                bolero.discover(self.inventory)

    def test_missing_package_declaration_fails(self) -> None:
        with mock.patch.object(bolero, "package_manifests", return_value={}):
            with self.assertRaisesRegex(bolero.BoleroError, "package mismatch"):
                bolero.discover(self.inventory)


class ExecutionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.inventory = bolero.load_inventory()
        self.target = self.inventory.targets[0]

    def test_command_propagates_engine_exit_and_timeout(self) -> None:
        with self.assertRaisesRegex(bolero.BoleroError, "exited 7"):
            bolero.command([sys.executable, "-c", "raise SystemExit(7)"])
        with tempfile.TemporaryDirectory() as directory:
            log = pathlib.Path(directory) / "timeout.log"
            with self.assertRaisesRegex(bolero.BoleroError, "timed out"):
                bolero.command([sys.executable, "-c",
                                "import time; time.sleep(5)"], timeout=1, log=log)
            self.assertIn("timeout after 1s", log.read_text())

    def test_ordinary_counts_require_random_and_corpus_cases(self) -> None:
        good = subprocess.CompletedProcess(
            [], 0, "corpus inputs: 2 | rng inputs: 256\n"
            "test result: ok. 1 passed; 0 failed\n", ""
        )
        with mock.patch.object(bolero, "command", return_value=good):
            bolero.test_targets(self.inventory, (self.target,))
        missing_cases = subprocess.CompletedProcess(
            [], 0, good.stdout.replace("rng inputs: 256", "rng inputs: 0"), ""
        )
        with mock.patch.object(bolero, "command", return_value=missing_cases):
            with self.assertRaisesRegex(bolero.BoleroError, "expected 256"):
                bolero.test_targets(self.inventory, (self.target,))

    def test_fuzz_copies_seed_corpus_and_requires_engine_completion(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            run = pathlib.Path(directory) / "run"
            run.mkdir()
            success = subprocess.CompletedProcess([], 0, "#100 DONE", "")
            with (
                mock.patch.object(bolero, "run_dir", return_value=run),
                mock.patch.object(bolero, "build_instrumented",
                                  return_value=bolero.ROOT / "fake-binary"),
                mock.patch.object(bolero, "verify_tool"),
                mock.patch.object(bolero, "metadata"),
                mock.patch.object(bolero, "command", return_value=success) as execute,
            ):
                bolero.fuzz_targets(self.inventory, (self.target,), 1)
            self.assertEqual(
                {file.name for file in (run / "corpus").iterdir()},
                {file.name for file in self.target.corpus.iterdir()},
            )
            flags = execute.call_args.kwargs["env"]["BOLERO_LIBFUZZER_ARGS"]
            self.assertIn(str(run / "corpus"), flags)
            self.assertIn(str(run / "crashes"), flags)
            self.assertIn("-max_total_time=1", flags)

        with tempfile.TemporaryDirectory() as directory:
            run = pathlib.Path(directory) / "run"
            run.mkdir()
            incomplete = subprocess.CompletedProcess([], 0, "no completion", "")
            with (
                mock.patch.object(bolero, "run_dir", return_value=run),
                mock.patch.object(bolero, "build_instrumented",
                                  return_value=bolero.ROOT / "fake-binary"),
                mock.patch.object(bolero, "verify_tool"),
                mock.patch.object(bolero, "metadata"),
                mock.patch.object(bolero, "command", return_value=incomplete),
            ):
                with self.assertRaisesRegex(bolero.BoleroError, "did not report completion"):
                    bolero.fuzz_targets(self.inventory, (self.target,), 1)

    def test_replay_stages_exact_bytes_and_disables_random_cases(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temporary = pathlib.Path(directory)
            target = dataclasses.replace(self.target, corpus=temporary / "corpus")
            input_file = temporary / "failure"
            input_file.write_bytes(b"\x42\x00")
            output = subprocess.CompletedProcess(
                [], 101, "test result: FAILED. 0 passed; 1 failed", ""
            )
            with mock.patch.object(bolero, "command", return_value=output) as execute:
                self.assertEqual(bolero.replay(target, input_file), 101)
            staged = list((temporary / "crashes").iterdir())
            self.assertEqual(len(staged), 1)
            self.assertEqual(staged[0].read_bytes(), input_file.read_bytes())
            self.assertEqual(execute.call_args.kwargs["env"]["BOLERO_RANDOM_ITERATIONS"],
                             "0")


class CliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.inventory = bolero.load_inventory()

    def invoke(self, *arguments: str) -> int:
        with (
            mock.patch.object(sys, "argv", ["bolero.py", *arguments]),
            mock.patch.object(bolero, "load_inventory", return_value=self.inventory),
            mock.patch.object(bolero, "discover"),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            return bolero.main()

    def test_list_and_validation_routes(self) -> None:
        self.assertEqual(self.invoke("validate"), 0)
        self.assertEqual(self.invoke("list"), 0)

    def test_fuzz_routes_select_targets_and_reject_zero_duration(self) -> None:
        with mock.patch.object(bolero, "fuzz_targets") as fuzz:
            self.assertEqual(self.invoke("fuzz", "nspl-expression", "3"), 0)
            self.assertEqual(fuzz.call_args.args[1], (self.inventory.targets[0],))
            self.assertEqual(fuzz.call_args.args[2], 3)
            self.assertEqual(self.invoke("fuzz-all"), 0)
            self.assertEqual(fuzz.call_args.args[1], self.inventory.targets)
            self.assertEqual(fuzz.call_args.args[2], 30)
            with self.assertRaisesRegex(bolero.BoleroError, "duration"):
                self.invoke("fuzz", "nspl-expression", "0")
            with self.assertRaisesRegex(bolero.BoleroError, "unknown Bolero target"):
                self.invoke("fuzz", "absent", "1")

    def test_replay_reduce_and_qualification_routes(self) -> None:
        with mock.patch.object(bolero, "qualify") as qualify:
            self.assertEqual(self.invoke("qualify"), 0)
            qualify.assert_called_once_with(self.inventory)
        with mock.patch.object(bolero, "replay", return_value=101) as replay:
            self.assertEqual(self.invoke("replay", "nspl-expression", "failure"), 101)
            self.assertEqual(replay.call_args.args[0], self.inventory.targets[0])
        with mock.patch.object(bolero, "reduce_failure") as reduce:
            self.assertEqual(self.invoke("reduce", "nspl-expression", "failure"), 0)
            reduce.assert_called_once()


if __name__ == "__main__":
    unittest.main()
