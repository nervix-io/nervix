"""Focused tests for the Bolero inventory and command runner."""

from __future__ import annotations

import dataclasses
import contextlib
import io
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from scripts import bolero, build_web_console


class ConsoleAssetTests(unittest.TestCase):
    def test_identical_assets_preserve_their_file_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source, destination = root / "staging", root / "dist"
            source.mkdir()
            (source / "index.html").write_bytes(b"current console")
            build_web_console.publish_assets(source, destination)
            current = (destination / "index.html").stat()
            source.mkdir(exist_ok=True)
            (source / "index.html").write_bytes(b"current console")
            build_web_console.publish_assets(source, destination)
            published = (destination / "index.html").stat()
            self.assertEqual(published.st_mtime_ns, current.st_mtime_ns)
            self.assertEqual(published.st_ino, current.st_ino)

    def test_published_assets_equal_the_complete_build_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source, destination = root / "staging", root / "dist"
            source.mkdir()
            destination.mkdir()
            (destination / "index.html").write_bytes(b"partial console")
            (destination / "unreferenced.js").write_bytes(b"unused")
            (source / "index.html").write_bytes(b"current console")
            (source / "nested").mkdir()
            (source / "nested/console.css").write_bytes(b"current style")
            expected = {path.relative_to(source): path.read_bytes()
                        for path in source.rglob("*") if path.is_file()}
            build_web_console.publish_assets(source, destination)
            actual = {path.relative_to(destination): path.read_bytes()
                      for path in destination.rglob("*") if path.is_file()}
            self.assertEqual(actual, expected)

    def test_build_publishes_success_and_preserves_assets_on_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            console = root / "crates/web-console"
            console.mkdir(parents=True)

            def build(args: list[str], **kwargs: object) -> None:
                self.assertEqual(args[:4], ["trunk", "build", "--release", "--dist"])
                self.assertEqual(kwargs["cwd"], console)
                self.assertNotIn("NO_COLOR", kwargs["env"])
                self.assertTrue(kwargs["check"])
                staging = pathlib.Path(args[4])
                staging.mkdir()
                (staging / "index.html").write_bytes(b"current console")

            with mock.patch.object(build_web_console, "ROOT", root), \
                    mock.patch.object(build_web_console.subprocess, "run", side_effect=build):
                build_web_console.main()
            with mock.patch.object(build_web_console, "ROOT", root), \
                    mock.patch.object(build_web_console.subprocess, "run",
                                      side_effect=subprocess.CalledProcessError(1, "trunk")):
                with self.assertRaises(subprocess.CalledProcessError):
                    build_web_console.main()
            self.assertEqual((console / "dist/index.html").read_bytes(), b"current console")


class InventoryTests(unittest.TestCase):
    def assert_invalid_inventory(self, text: str, message: str) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "inventory.toml"
            path.write_text(text)
            with self.assertRaisesRegex(bolero.BoleroError, message):
                bolero.load_inventory(path)

    def test_inventory_has_current_targets_and_exact_corpus_paths(self) -> None:
        inventory = bolero.load_inventory()
        self.assertEqual({target.id for target in inventory.targets}, {
            "task-status-transitions",
            "entity-freeze-transitions",
            "endpoint-route-table",
            "client-emitter-wire",
            "nspl-expression",
            "nspl-model",
            "nspl-archive-model",
            "backup-record-manifest",
            "client-processor-choice-request",
            "client-ffi-host-columns",
            "branch-membership",
            "typed-report", "typed-source-contract", "typed-site-union",
            "nspl-statement",
            "nspl-statement-text",
            "nspl-expression-text",
            "nspl-format-document",
            "nspl-format-text",
            "models-names",
            "models-name-validation",
            "models-timestamps",
            "models-timestamp-text",
            "models-domain-clock",
            "models-domain-clock-validation",
            "models-durations",
            "models-duration-text",
            "models-json-paths",
            "models-json-path-validation",
            "models-batch-limits",
            "models-batch-limit-validation",
            "models-identities",
            "models-identity-validation",
            "models-archived-models",
            "models-archived-counts",
            "consensus-archived-counts",
            "registry-archived-models",
            "runtime-window-archived-counts",
            "simd-constant-division",
            "simd-checked-lanes",
            "replica-progress",
            "replica-catch-up",
        })
        self.assertEqual({target.package for target in inventory.targets}, {
            "nervix-client-wire",
            "nervix-client-ffi",
            "nervix-nspl",
            "nervix-nspl-format",
            "nervix-models",
            "nervix-backup",
            "nervix-branch-instances",
            "nervix-consensus",
            "nervix-lint-report",
            "nervix-server",
            "nervix-simd-kernels",
            "nervix-checkpoint-replication",
        })
        for target in inventory.targets:
            self.assertTrue(target.source.is_file())
            self.assertTrue(target.corpus.is_dir())
            self.assertIn(target.test_target, {"lib", "test:representations"})
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
                "canonical_round_trip_tests__bolero_expression_roundtrip_minimal_parentheses/corpus",
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

    def test_tool_pins_and_target_identity_are_required(self) -> None:
        text = bolero.INVENTORY.read_text()
        for before, after, message in (
            ('cargo_bolero = "0.13.4"', 'cargo_bolero = "0.13"', "exact version"),
            ('nightly = "nightly-2026-09-17"', 'nightly = "nightly"', "dated toolchain"),
            ('sanitizer = "address"', 'sanitizer = "none"', "real sanitizer"),
            ('id = "nspl-expression"', 'id = "Invalid ID"', "invalid target id"),
            ('test_target = "lib"', 'test_target = "bin"', "invalid cargo test target"),
            ('invariant = "', 'invariant = "" # ', "invariant is required"),
        ):
            with self.subTest(message=message):
                self.assert_invalid_inventory(text.replace(before, after, 1), message)

    def test_paths_and_missing_source_are_rejected(self) -> None:
        text = bolero.INVENTORY.read_text()
        source = 'source = "crates/nspl/src/canonical_round_trip_tests.rs"'
        self.assert_invalid_inventory(
            text.replace(source, 'source = ""', 1),
            "repository-relative path",
        )
        self.assert_invalid_inventory(
            text.replace(source, 'source = "../canonical_round_trip_tests.rs"', 1),
            "normalized repository-relative path",
        )
        self.assert_invalid_inventory(
            text.replace(source, 'source = "crates/nspl/src/missing.rs"', 1),
            "missing source",
        )

    def test_inventory_sections_and_fields_are_closed(self) -> None:
        text = bolero.INVENTORY.read_text()
        self.assert_invalid_inventory(text + '\n[extra]\nvalue = 1\n',
                                      "only tool and target")
        self.assert_invalid_inventory(
            text.replace('campaign_fuzz_seconds = 300',
                         'campaign_fuzz_seconds = 300\nextra = 1', 1),
            "tool section",
        )
        self.assert_invalid_inventory(
            text.replace('domain_version = 1', 'unknown = 1', 1),
            "missing or unknown fields",
        )

    def test_target_names_and_empty_registry_are_rejected(self) -> None:
        text = bolero.INVENTORY.read_text()
        self.assert_invalid_inventory(
            text.replace('canonical_round_trip_tests::bolero_expression_roundtrip_minimal_parentheses',
                         'canonical_round_trip_tests::expression_roundtrip_minimal_parentheses', 1),
            "test name must start with bolero_",
        )
        self.assert_invalid_inventory(
            'target = []\n' + text.split('[[target]]')[0],
            "inventory contains no targets",
        )


class DiscoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.inventory = bolero.load_inventory()
        self.manifests = bolero.package_manifests()

    def compiled(self, package: str, test_target: str, manifest: pathlib.Path | None = None) -> list[dict[str, str]]:
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
                              side_effect=lambda package, test_target, ignored, manifest=None:
                              [] if ignored else
                              [item["test_name"] for item in self.compiled(package, test_target)]),
            mock.patch.object(bolero, "compiled_targets", side_effect=self.compiled),
        ):
            bolero.discover(self.inventory)

    def test_ignored_and_unregistered_targets_fail(self) -> None:
        with (
            mock.patch.object(bolero, "static_targets", side_effect=self.source_functions),
            mock.patch.object(bolero, "listed_tests",
                              side_effect=lambda package, test_target, ignored, manifest=None:
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

    def test_target_specific_bolero_dependency_is_discovered(self) -> None:
        self.assertTrue(bolero.declares_bolero({
            "target": {"cfg(unix)": {"dev-dependencies": {"bolero": "0.13.4"}}}
        }))

    def test_workspace_root_source_scan_keeps_package_ownership(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            manifest = root / "Cargo.toml"
            manifest.write_text("[package]\nname = 'host'\n")
            source = root / "src" / "owner.rs"
            source.parent.mkdir()
            source.write_text("fn bolero_host() { bolero::check!(); }")
            for relative in ("crates/connector", "tests/qualification"):
                package = root / relative
                package.mkdir(parents=True)
                (package / "Cargo.toml").write_text("[package]\nname = 'nested'\n")
                (package / "lib.rs").write_text("fn bolero_nested() { bolero::check!(); }")
            generated = root / "target" / "generated.rs"
            generated.parent.mkdir()
            generated.write_text("fn bolero_generated() { bolero::check!(); }")
            self.assertEqual(bolero.static_targets(manifest), {"bolero_host": source})

    def test_source_scan_rejects_unregistered_macro_shapes(self) -> None:
        cases = (
            ("bolero::check!();", "no owning function"),
            ("fn ordinary() { bolero::check!(); }", "must start with bolero_"),
            ("fn bolero_one() { check!(); }", "qualify bolero::check!"),
            ("fn bolero_one() { bolero::check!(); }\n"
             "fn bolero_one() { bolero::check!(); }", "duplicate Bolero function"),
        )
        for content, message in cases:
            with self.subTest(message=message), tempfile.TemporaryDirectory() as directory:
                manifest = pathlib.Path(directory) / "Cargo.toml"
                manifest.write_text("[package]\nname = 'scan'\n")
                (manifest.parent / "lib.rs").write_text(content)
                with self.assertRaisesRegex(bolero.BoleroError, message):
                    bolero.static_targets(manifest)

    def test_source_scan_stays_within_its_package(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            manifest = root / "Cargo.toml"
            manifest.write_text("[package]\nname = 'root'\n")
            source = root / "src/lib.rs"
            source.parent.mkdir()
            source.write_text("fn bolero_root() { bolero::check!(); }")
            nested = root / "crates/nested/Cargo.toml"
            nested.parent.mkdir(parents=True)
            nested.write_text("[package]\nname = 'nested'\n")
            nested_source = nested.parent / "src/lib.rs"
            nested_source.parent.mkdir()
            nested_source.write_text("fn bolero_nested() { bolero::check!(); }")
            generated = root / "target/generated.rs"
            generated.parent.mkdir()
            generated.write_text("fn bolero_generated() { bolero::check!(); }")
            self.assertEqual(bolero.static_targets(manifest), {"bolero_root": source})
            self.assertEqual(bolero.static_targets(nested), {"bolero_nested": nested_source})

    def test_compiled_selection_must_match_inventory_and_work_directory(self) -> None:
        def listed(package: str, test_target: str, ignored: bool, manifest: pathlib.Path | None = None) -> list[str]:
            return [] if ignored else [
                item["test_name"] for item in self.compiled(package, test_target)
            ]

        with (
            mock.patch.object(bolero, "static_targets", side_effect=self.source_functions),
            mock.patch.object(bolero, "listed_tests", side_effect=listed),
            mock.patch.object(bolero, "compiled_targets", side_effect=lambda package, test_target, manifest=None:
                              self.compiled(package, test_target)[:-1]),
        ):
            with self.assertRaisesRegex(bolero.BoleroError, "listed Bolero tests"):
                bolero.discover(self.inventory)

        def misplaced(package: str, test_target: str, manifest: pathlib.Path | None = None) -> list[dict[str, str]]:
            targets = self.compiled(package, test_target)
            if package == "nervix-backup":
                targets[0]["work_dir"] = "/tmp/wrong-bolero-work-directory"
            return targets

        with (
            mock.patch.object(bolero, "static_targets", side_effect=self.source_functions),
            mock.patch.object(bolero, "listed_tests", side_effect=listed),
            mock.patch.object(bolero, "compiled_targets", side_effect=misplaced),
        ):
            with self.assertRaisesRegex(bolero.BoleroError, "compiled work directory"):
                bolero.discover(self.inventory)


class ExecutionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.inventory = bolero.load_inventory()
        self.target = self.inventory.targets[0]
        self.cargo_metadata = {"packages": []}
        for package, manifest in bolero.package_manifests().items():
            targets = [{
                "name": package.replace("-", "_"), "kind": ["lib"],
                "src_path": str(manifest.parent / "src/lib.rs"),
            }]
            if package in {"nervix-server", "nervix-nspl-format"}:
                targets.append({
                    "name": package.replace("-", "_"), "kind": ["bin"],
                    "src_path": str(manifest.parent / "src/main.rs"),
                })
            if package == "nervix-models":
                targets.append({
                    "name": "representations", "kind": ["test"],
                    "src_path": str(manifest.parent / "tests/representations/main.rs"),
                })
            if package == "nervix-nspl-format":
                targets.append({
                    "name": "repository_files", "kind": ["test"],
                    "src_path": str(manifest.parent / "tests/repository_files.rs"),
                })
            self.cargo_metadata["packages"].append({
                "name": package, "manifest_path": str(manifest), "targets": targets,
            })

    def command_with_build(self, build: subprocess.CompletedProcess[str]):
        def execute(args: list[str], **kwargs) -> subprocess.CompletedProcess[str]:
            if args[:2] == ["cargo", "metadata"]:
                self.assertIn("--no-deps", args)
                self.assertIn("--manifest-path", args)
                return subprocess.CompletedProcess(args, 0, json.dumps(self.cargo_metadata), "")
            return build
        return execute

    def test_models_directory_integration_executable_is_selected(self) -> None:
        target = next(item for item in self.inventory.targets if item.id == "models-names")
        with tempfile.TemporaryDirectory() as directory:
            run = pathlib.Path(directory)
            library = run / "nervix_models-1111"
            integration = run / "representations-2222"
            library.touch()
            integration.touch()
            build = subprocess.CompletedProcess([], 0,
                f"Executable unittests src/lib.rs ({library})\n"
                f"Executable tests/representations/main.rs ({integration})\n", "")
            with mock.patch.object(bolero, "command", side_effect=self.command_with_build(build)):
                self.assertEqual(bolero.build_instrumented(self.inventory, target, run), integration)

    def test_cargo_library_name_types_and_explicit_root_select_the_binary(self) -> None:
        package = next(item for item in self.cargo_metadata["packages"]
                       if item["name"] == self.target.package)
        package_root = pathlib.Path(package["manifest_path"]).parent
        package["targets"] = [{
            "name": "custom-owner", "kind": ["rlib", "cdylib"],
            "src_path": str(package_root / "fixtures/owner.rs"),
        }]
        with tempfile.TemporaryDirectory() as directory:
            run = pathlib.Path(directory)
            library = run / "custom_owner-1111"
            unrelated = run / "custom_owner-2222"
            library.touch()
            unrelated.touch()
            build = subprocess.CompletedProcess([], 0,
                f"Executable unittests fixtures/owner.rs ({library})\n"
                f"Executable unittests src/main.rs ({unrelated})\n", "")
            with mock.patch.object(bolero, "command", side_effect=self.command_with_build(build)):
                self.assertEqual(bolero.build_instrumented(self.inventory, self.target, run), library)

    def test_library_and_integration_using_one_root_keep_their_target_identity(self) -> None:
        target = next(item for item in self.inventory.targets if item.id == "models-names")
        package = next(item for item in self.cargo_metadata["packages"]
                       if item["name"] == target.package)
        integration_target = next(item for item in package["targets"] if item["kind"] == ["test"])
        package["targets"][0] = {**integration_target, "kind": ["lib"]}
        with tempfile.TemporaryDirectory() as directory:
            run = pathlib.Path(directory)
            library = run / "representations-1111"
            integration = run / "representations-2222"
            library.touch()
            integration.touch()
            build = subprocess.CompletedProcess([], 0,
                f"Executable unittests tests/representations/main.rs ({library})\n"
                f"Executable tests/representations/main.rs ({integration})\n", "")
            with mock.patch.object(bolero, "command", side_effect=self.command_with_build(build)):
                self.assertEqual(bolero.build_instrumented(self.inventory, target, run), integration)
                second = run / "library"
                second.mkdir()
                self.assertEqual(
                    bolero.build_instrumented(self.inventory, dataclasses.replace(target, test_target="lib"), second),
                    library,
                )

    def test_cargo_package_and_target_must_be_present_and_unambiguous(self) -> None:
        target = next(item for item in self.inventory.targets if item.id == "models-names")
        original = json.dumps(self.cargo_metadata)
        for case in ("missing-package", "ambiguous-package", "missing-target", "ambiguous-target"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                self.cargo_metadata = json.loads(original)
                packages = self.cargo_metadata["packages"]
                package = next(item for item in packages if item["name"] == target.package)
                if case == "missing-package":
                    packages.remove(package)
                elif case == "ambiguous-package":
                    packages.append(package)
                elif case == "missing-target":
                    package["targets"] = [item for item in package["targets"] if item["kind"] != ["test"]]
                else:
                    package["targets"].append(next(item for item in package["targets"] if item["kind"] == ["test"]))
                build = subprocess.CompletedProcess([], 0, "", "")
                with mock.patch.object(bolero, "command", side_effect=self.command_with_build(build)) as calls:
                    with self.assertRaisesRegex(bolero.BoleroError, "expected one Cargo"):
                        bolero.build_instrumented(self.inventory, target, pathlib.Path(directory))
                    self.assertEqual(calls.call_count, 1)

    def test_server_library_executable_is_selected_among_package_targets(self) -> None:
        target = dataclasses.replace(self.target, package="nervix-server")
        with tempfile.TemporaryDirectory() as directory:
            run = pathlib.Path(directory)
            library = run / "nervix_server-1111"
            binary = run / "nervix_server-2222"
            for executable in (library, binary):
                executable.write_bytes(b"")
            output = (
                f"  Executable unittests src/lib.rs ({library})\n"
                f"  Executable unittests src/main.rs ({binary})\n"
            )
            build = subprocess.CompletedProcess([], 0, output, "")
            with mock.patch.object(bolero, "command", side_effect=self.command_with_build(build)):
                self.assertEqual(bolero.build_instrumented(self.inventory, target, run), library)

    def test_command_propagates_engine_exit_and_timeout(self) -> None:
        with self.assertRaisesRegex(bolero.BoleroError, "exited 7"):
            bolero.command([sys.executable, "-c", "raise SystemExit(7)"])
        with self.assertRaisesRegex(bolero.BoleroError, "timed out"):
            bolero.command([sys.executable, "-c", "import time; time.sleep(5)"],
                           timeout=1)
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

    def test_ordinary_run_rejects_missing_assertion_and_corpus(self) -> None:
        for output, message in (
            ("test result: ok. 0 passed; 0 failed", "exactly once"),
            ("test result: ok. 1 passed; 0 failed", "input counts"),
            ("test result: ok. 1 passed; 0 failed\n"
             "corpus inputs: 0 | rng inputs: 256", "corpus was not replayed"),
        ):
            with self.subTest(message=message):
                result = subprocess.CompletedProcess([], 0, output, "")
                with mock.patch.object(bolero, "command", return_value=result):
                    with self.assertRaisesRegex(bolero.BoleroError, message):
                        bolero.test_targets(self.inventory, (self.target,))

    def test_tool_version_and_instrumented_binary_are_required(self) -> None:
        mismatch = subprocess.CompletedProcess([], 0, "cargo-bolero 0.12.0", "")
        with mock.patch.object(bolero, "command", return_value=mismatch):
            with self.assertRaisesRegex(bolero.BoleroError, "required"):
                bolero.verify_tool(self.inventory)
        with tempfile.TemporaryDirectory() as directory:
            build = subprocess.CompletedProcess([], 0, "no executable", "")
            with mock.patch.object(bolero, "command", side_effect=self.command_with_build(build)):
                with self.assertRaisesRegex(bolero.BoleroError, "expected one instrumented"):
                    bolero.build_instrumented(self.inventory, self.target,
                                              pathlib.Path(directory))

    def test_library_executable_is_told_apart_from_a_binary_of_the_same_name(self) -> None:
        target = dataclasses.replace(self.target, package="nervix-nspl-format")
        with tempfile.TemporaryDirectory() as directory:
            run = pathlib.Path(directory)
            library = run / "nervix_nspl_format-1111"
            binary = run / "nervix_nspl_format-2222"
            integration = run / "repository_files-3333"
            for executable in (library, binary, integration):
                executable.write_bytes(b"")
            output = (
                f"  Executable unittests src/lib.rs ({library})\n"
                f"  Executable unittests src/main.rs ({binary})\n"
                f"  Executable tests/repository_files.rs ({integration})\n"
            )
            build = subprocess.CompletedProcess([], 0, output, "")
            with mock.patch.object(bolero, "command", side_effect=self.command_with_build(build)):
                self.assertEqual(bolero.build_instrumented(self.inventory, target, run), library)
                integration_target = dataclasses.replace(
                    target, test_target="test:repository_files"
                )
                again = run / "again"
                again.mkdir()
                self.assertEqual(
                    bolero.build_instrumented(self.inventory, integration_target, again),
                    integration,
                )

    def test_build_deadline_changes_only_the_compilation_budget(self) -> None:
        target = dataclasses.replace(self.target, package="nervix-nspl-format")
        with tempfile.TemporaryDirectory() as directory:
            run = pathlib.Path(directory)
            library = run / "nervix_nspl_format-1111"
            library.write_bytes(b"")
            output = f"  Executable unittests src/lib.rs ({library})\n"
            result = subprocess.CompletedProcess([], 0, output, "")
            with mock.patch.dict(os.environ, {"BOLERO_BUILD_TIMEOUT_SECONDS": "7200"}), \
                    mock.patch.object(bolero, "command", side_effect=self.command_with_build(result)) as build:
                self.assertEqual(bolero.build_instrumented(self.inventory, target, run), library)
                args = build.call_args.args[0]
                self.assertEqual(build.call_args.kwargs["timeout"], 7200)
                self.assertEqual(args[args.index("--timeout") + 1], "10s")
                self.assertEqual(args[args.index("--runs") + 1], "0")

    def test_build_deadline_refuses_invalid_bounds_before_starting_cargo(self) -> None:
        for budget in ("0", "-1", "invalid"):
            with self.subTest(budget=budget), tempfile.TemporaryDirectory() as directory, \
                    mock.patch.dict(os.environ, {"BOLERO_BUILD_TIMEOUT_SECONDS": budget}), \
                    mock.patch.object(bolero, "command") as build:
                with self.assertRaisesRegex(bolero.BoleroError, "positive integer"):
                    bolero.build_instrumented(self.inventory, self.target, pathlib.Path(directory))
                build.assert_not_called()

    def test_feature_and_integration_test_target_arguments(self) -> None:
        target = dataclasses.replace(self.target, test_target="test:property_suite",
                                     features=("fuzz-support",))
        self.assertIn("property_suite", bolero.cargo_test_args(target))
        self.assertIn("fuzz-support", bolero.cargo_test_args(target))
        self.assertIn("fuzz-support", bolero.bolero_args(self.inventory, target))

    def test_instrumented_library_is_selected_among_package_binaries(self) -> None:
        target = next(item for item in self.inventory.targets if item.id == "typed-report")
        with tempfile.TemporaryDirectory() as directory:
            temporary = pathlib.Path(directory)
            library = temporary / "nervix_lint_report-library"
            binary = temporary / "nervix_lint_report-binary"
            library.touch()
            binary.touch()
            build = subprocess.CompletedProcess([], 0,
                f"Executable unittests src/lib.rs ({library})\n"
                f"Executable unittests src/main.rs ({binary})\n", "")
            with mock.patch.object(bolero, "command", side_effect=self.command_with_build(build)):
                self.assertEqual(bolero.build_instrumented(self.inventory, target, temporary), library)

    def test_fuzz_build_failure_records_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with (
                mock.patch.object(bolero, "run_dir", return_value=pathlib.Path(directory)),
                mock.patch.object(bolero, "verify_tool"),
                mock.patch.object(bolero, "build_instrumented",
                                  side_effect=bolero.BoleroError("build failed")),
                mock.patch.object(bolero, "metadata") as metadata,
            ):
                with self.assertRaisesRegex(bolero.BoleroError, "build failed"):
                    bolero.fuzz_targets(self.inventory, (self.target,), 1)
                self.assertEqual(metadata.call_args.args[3], "failed build")

    def test_preparation_records_the_selected_binary_without_running_a_campaign(self) -> None:
        target = next(item for item in self.inventory.targets if item.id == "models-archived-counts")
        with tempfile.TemporaryDirectory() as directory:
            run = pathlib.Path(directory)
            binary = run / "representations-2222"
            binary.touch()
            build = subprocess.CompletedProcess([], 0,
                f"Executable tests/representations/main.rs ({binary})\n", "")
            execute_build = self.command_with_build(build)

            def execute(args, **kwargs):
                if args[:2] == ["git", "rev-parse"]:
                    return subprocess.CompletedProcess(args, 0, "revision\n", "")
                return execute_build(args, **kwargs)

            with (
                mock.patch.object(bolero, "run_dir", return_value=run),
                mock.patch.object(bolero, "verify_tool"),
                mock.patch.object(bolero, "command", side_effect=execute) as commands,
                mock.patch.object(bolero, "run_instrumented") as campaign,
                contextlib.redirect_stdout(io.StringIO()),
            ):
                bolero.prepare_target(self.inventory, target)
            campaign.assert_not_called()
            report = json.loads((run / "metadata.json").read_text())
            self.assertEqual(report["result"], "instrumented preparation")
            self.assertEqual(report["target"], target.id)
            self.assertEqual(report["test_target"], "test:representations")
            self.assertEqual(report["features"], list(target.features))
            args = next(call.args[0] for call in commands.call_args_list
                        if call.args[0][:3] == ["cargo", "bolero", "test"])
            self.assertEqual(args[args.index("--runs") + 1], "0")
            self.assertEqual(args[args.index("--toolchain") + 1], self.inventory.nightly)
            self.assertEqual(args[args.index("--sanitizer") + 1], self.inventory.sanitizer)

    def test_failed_preparation_keeps_evidence_and_propagates_the_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            run = pathlib.Path(directory)
            with (
                mock.patch.object(bolero, "run_dir", return_value=run),
                mock.patch.object(bolero, "verify_tool"),
                mock.patch.object(bolero, "build_instrumented",
                                  side_effect=bolero.BoleroError("build failed")),
                mock.patch.object(bolero, "command",
                                  return_value=subprocess.CompletedProcess([], 0, "revision\n", "")),
                mock.patch.object(bolero, "run_instrumented") as campaign,
            ):
                with self.assertRaisesRegex(bolero.BoleroError, "build failed"):
                    bolero.prepare_target(self.inventory, self.target)
            campaign.assert_not_called()
            report = json.loads((run / "metadata.json").read_text())
            self.assertEqual(report["result"], "failed preparation")
            self.assertEqual(report["target"], self.target.id)

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

    def test_replay_rejects_missing_large_or_unconfirmed_input(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temporary = pathlib.Path(directory)
            target = dataclasses.replace(self.target, corpus=temporary / "corpus")
            missing = temporary / "missing"
            with self.assertRaisesRegex(bolero.BoleroError, "does not exist"):
                bolero.replay(target, missing)
            failure = temporary / "failure"
            failure.write_bytes(b"x" * (target.max_input_bytes + 1))
            with self.assertRaisesRegex(bolero.BoleroError, "exceeds"):
                bolero.replay(target, failure)
            failure.write_bytes(b"\x42")
            for result, message in (
                (subprocess.CompletedProcess([], 101, "unrelated test failed", ""),
                 "outside the selected property"),
                (subprocess.CompletedProcess([], 0, "test result: ok. 1 passed", ""),
                 "saved input was not replayed"),
            ):
                with self.subTest(message=message), mock.patch.object(
                    bolero, "command", return_value=result
                ):
                    with self.assertRaisesRegex(bolero.BoleroError, message):
                        bolero.replay(target, failure)

    def test_reduction_requires_a_saved_crash_and_failing_minimum(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temporary = pathlib.Path(directory)
            failure = temporary / "failure"
            with self.assertRaisesRegex(bolero.BoleroError, "does not exist"):
                with mock.patch.object(bolero, "verify_tool"):
                    bolero.reduce_failure(self.inventory, self.target, failure)
            failure.write_bytes(b"x" * (self.target.max_input_bytes + 1))
            with self.assertRaisesRegex(bolero.BoleroError, "exceeds"):
                with mock.patch.object(bolero, "verify_tool"):
                    bolero.reduce_failure(self.inventory, self.target, failure)
            failure.write_bytes(b"\x42\x00")
            run = temporary / "run"
            run.mkdir()
            with (
                mock.patch.object(bolero, "verify_tool"),
                mock.patch.object(bolero, "run_dir", return_value=run),
                mock.patch.object(bolero, "build_instrumented",
                                  return_value=bolero.ROOT / "fake-binary"),
                mock.patch.object(bolero, "run_instrumented"),
                mock.patch.object(bolero, "metadata"),
            ):
                with self.assertRaisesRegex(bolero.BoleroError, "did not save"):
                    bolero.reduce_failure(self.inventory, self.target, failure)

            def save_minimum(*args: object, **kwargs: object) -> None:
                (run / "minimized").write_bytes(b"\x42")

            with (
                mock.patch.object(bolero, "verify_tool"),
                mock.patch.object(bolero, "run_dir", return_value=run),
                mock.patch.object(bolero, "build_instrumented",
                                  return_value=bolero.ROOT / "fake-binary"),
                mock.patch.object(bolero, "run_instrumented", side_effect=save_minimum),
                mock.patch.object(bolero, "replay", return_value=0),
                mock.patch.object(bolero, "metadata"),
            ):
                with self.assertRaisesRegex(bolero.BoleroError, "no longer fails"):
                    bolero.reduce_failure(self.inventory, self.target, failure)


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

    def test_preparation_requires_an_exact_registered_target(self) -> None:
        with mock.patch.object(bolero, "prepare_target") as prepare:
            self.assertEqual(self.invoke("prepare", "models-archived-counts"), 0)
            selected = next(target for target in self.inventory.targets
                            if target.id == "models-archived-counts")
            prepare.assert_called_once_with(self.inventory, selected)
            with self.assertRaisesRegex(bolero.BoleroError, "unknown Bolero target"):
                self.invoke("prepare", "models-archive")

    def test_fuzz_routes_select_targets_and_reject_zero_duration(self) -> None:
        with mock.patch.object(bolero, "fuzz_targets") as fuzz:
            self.assertEqual(self.invoke("fuzz", "nspl-expression", "3"), 0)
            selected = next(target for target in self.inventory.targets
                            if target.id == "nspl-expression")
            self.assertEqual(fuzz.call_args.args[1], (selected,))
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
            selected = next(target for target in self.inventory.targets
                            if target.id == "nspl-expression")
            self.assertEqual(replay.call_args.args[0], selected)
        with mock.patch.object(bolero, "reduce_failure") as reduce:
            self.assertEqual(self.invoke("reduce", "nspl-expression", "failure"), 0)
            reduce.assert_called_once()


if __name__ == "__main__":
    unittest.main()
