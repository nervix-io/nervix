from __future__ import annotations

import json
import copy
import dataclasses
import contextlib
import io
import os
import pathlib
import runpy
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from typing import Callable
from unittest import mock

from scripts import analysis_admission, typed_ratchet
from scripts.analysis_admission import GIB, Compiled, Policy
from scripts.typed_ratchet import AnalysisError, Configuration, ROOT, Runner, TOOLING, atomic_json, digest, summarize


class CompletionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = pathlib.Path(self.temporary.name)
        self.source = self.root / "src/lib.rs"
        self.source.parent.mkdir()
        self.source.write_text("pub fn empty() {}\n")
        self.artifact = self.root / "libexample-abcd.rmeta"
        self.artifact.write_bytes(b"metadata")
        self.reports = self.root / "reports"
        self.reports.mkdir()
        self.runner = Runner.__new__(Runner)
        self.runner.root = self.root
        self.runner.compiler = "qualified compiler"
        self.runner.identity = "current worktree fingerprint"
        self.runner.environment = {"NERVIX_LINT_MODE": "gate"}
        self.runner.admission = Policy(self.root / "queue", 4 * GIB, 60.0, {Compiled("example", "lib"): 16 * GIB})
        self.configuration = Configuration("ordinary", "Cargo.toml", ("example",))
        self.key = "example-id::example::lib"
        self.expected = {self.key: {"package": "example", "source": str(self.source), "kind": "lib"}}
        self.messages = [{
            "reason": "compiler-artifact", "package_id": "example-id",
            "target": {"name": "example", "kind": ["lib"], "src_path": str(self.source)},
            "filenames": [str(self.artifact)],
        }]
        self.report = {
            "complete": True, "identity": self.runner.identity,
            "compiler": self.runner.compiler, "configuration": "ordinary",
            "crate_name": "example", "crate_source": str(self.source), "findings": [],
        }
        self.path = self.reports / "example-abcd.json"
        atomic_json(self.path, self.report)

    def validate(self) -> dict:
        return self.runner.validate_reports(self.configuration, self.expected, self.messages, self.reports)

    def test_zero_requires_complete_declared_coverage(self) -> None:
        evidence = self.validate()
        self.assertTrue(evidence["complete"])
        self.assertEqual(set(evidence["reports"]), {self.key})
        self.assertEqual(evidence["reports"][self.key]["findings"], [])

    def test_missing_side_report_is_not_zero(self) -> None:
        self.path.unlink()
        with self.assertRaisesRegex(AnalysisError, "missing.*complete compiler report"):
            self.validate()

    def test_partial_stale_cross_worktree_or_mismatched_results_fail(self) -> None:
        for field, value in (
            ("complete", False), ("identity", "another worktree"),
            ("compiler", "another compiler"), ("configuration", "shuttle"),
            ("crate_name", "another_target"), ("crate_source", str(self.root / "different.rs")),
        ):
            with self.subTest(field=field):
                changed = {**self.report, field: value}
                atomic_json(self.path, changed)
                with self.assertRaises(AnalysisError):
                    self.validate()

    def test_missing_target_or_artifact_fails(self) -> None:
        self.messages.clear()
        with self.assertRaisesRegex(AnalysisError, "incomplete declared target coverage"):
            self.validate()

    def test_missing_metadata_fails(self) -> None:
        self.artifact.unlink()
        with self.assertRaisesRegex(AnalysisError, "missing Cargo artifact"):
            self.validate()





class RunnerIdentityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = pathlib.Path(self.temporary.name)
        self.target = self.root / "target"
        subprocess.run(["git", "init", "-q", "-b", "main"], cwd=self.root, check=True)
        for name, text in {"src/lib.rs": "pub fn owner() {}", "Cargo.toml": "[workspace]", "Cargo.lock": "lock", str(TOOLING / "report/src/rules.rs"): "rule code", str(TOOLING / "configurations.toml"): "[admission]\nreservation = []\nthreshold_gib = 4\nwait_limit_minutes = 30\n# declared", "scripts/typed_lint_wrapper.py": "wrapper", "target/typed-ratchet/driver/debug/nervix-lint-driver": "driver", "target/typed-ratchet/driver/debug/nervix-lint-report": "validator"}.items():
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
        (self.root / ".gitignore").write_text("target/\n")
        self.compiler = mock.patch("scripts.typed_ratchet.compiler_identity", return_value="qualified compiler")
        self.compiler.start()
        self.addCleanup(self.compiler.stop)
        self.original_command = typed_ratchet.command
        def command(args, *, cwd, env=None):
            if args[:4] == ["rustup", "run", typed_ratchet.TOOLCHAIN, "rustc"]:
                return str(self.root / "sysroot")
            return self.original_command(args, cwd=cwd, env=env)
        self.commands = mock.patch("scripts.typed_ratchet.command", side_effect=command)
        self.commands.start()
        self.addCleanup(self.commands.stop)

    def test_compiler_driver_rules_configuration_source_dependency_and_wrapper_invalidate(self) -> None:
        original = Runner(self.root, self.target)
        for name in ("src/lib.rs", "Cargo.toml", "Cargo.lock", str(TOOLING / "report/src/rules.rs"), str(TOOLING / "configurations.toml"), "scripts/typed_lint_wrapper.py", "target/typed-ratchet/driver/debug/nervix-lint-driver", "target/typed-ratchet/driver/debug/nervix-lint-report"):
            with self.subTest(name=name):
                path = self.root / name
                initial = path.read_bytes()
                path.write_bytes(initial + b" changed")
                self.assertNotEqual(Runner(self.root, self.target).identity, original.identity)
                path.write_bytes(initial)
        with mock.patch("scripts.typed_ratchet.compiler_identity", return_value="another compiler"):
            self.assertNotEqual(Runner(self.root, self.target).identity, original.identity)

    def test_cache_flags_and_local_cargo_configuration_invalidate(self) -> None:
        original = Runner(self.root, self.target)
        for name in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "KACHE_KEY_SALT", "RUSTC_WRAPPER", "RUSTC", "CARGO_BUILD_TARGET", "CARGO_BUILD_RUSTFLAGS", "CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS", "CARGO_PROFILE_DEV_DEBUG_ASSERTIONS"):
            with mock.patch.dict(os.environ, {name: "qualified value"}):
                self.assertNotEqual(Runner(self.root, self.target).identity, original.identity)
        path = self.root / ".cargo/config.toml"
        path.parent.mkdir()
        path.write_text("[build]\nrustc-wrapper='kache'\n")
        self.assertNotEqual(Runner(self.root, self.target).identity, original.identity)

    def test_missing_driver_and_an_existing_workspace_wrapper_fail(self) -> None:
        with mock.patch.dict(os.environ, {"RUSTC_WORKSPACE_WRAPPER": "another driver"}):
            with self.assertRaisesRegex(AnalysisError, "already set"):
                Runner(self.root, self.target)
        (self.target / "typed-ratchet/driver/debug/nervix-lint-driver").unlink()
        with self.assertRaisesRegex(AnalysisError, "missing compiler driver"):
            Runner(self.root, self.target)


class ConfigurationTests(unittest.TestCase):
    def test_diagnostic_configurations_include_the_lanes_testing_capability(self) -> None:
        diagnostic_features = {"deloxide", "deloxide-order", "deloxide-stress"}
        diagnostic_configurations = 0
        for configuration in typed_ratchet.load_configurations(ROOT):
            if diagnostic_features.intersection(configuration.features):
                diagnostic_configurations += 1
                with self.subTest(configuration=configuration.name):
                    self.assertIn("testing", configuration.features)
        self.assertEqual(diagnostic_configurations, 6)

    def test_arguments_keep_modes_targets_and_selected_packages_explicit(self) -> None:
        self.assertEqual(Configuration("selected", "Cargo.toml", ("one", "two"), ("lib", "bin"), ("native", "shuttle"), "native-target").cargo_arguments(), ["--manifest-path", "Cargo.toml", "--package", "one", "--package", "two", "--lib", "--bins", "--features", "native shuttle", "--target", "native-target"])
        self.assertIn("--workspace", Configuration("ordinary", "Cargo.toml", ()).cargo_arguments())

    def test_declared_modes_and_malformed_configuration(self) -> None:
        configurations = typed_ratchet.load_configurations(ROOT)
        self.assertEqual({item.name for item in configurations}, {"ordinary", "browser", "testing", "shuttle", "loom", "turmoil", "deloxide", "deloxide-order", "deloxide-stress", "deloxide-binaries", "deloxide-order-binaries", "deloxide-stress-binaries"})
        browser = next(item for item in configurations if item.name == "browser")
        self.assertEqual(browser.target, "wasm32-unknown-unknown")
        self.assertEqual(browser.kinds, ("lib", "bin"))
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            path = root / TOOLING / "configurations.toml"
            path.parent.mkdir(parents=True)
            valid = '[[configuration]]\nname="ordinary"\nmanifest="Cargo.toml"\npackages=[]\n'
            for text, error in ((valid + valid, "duplicate"), (valid + 'features=["loom", "shuttle"]\n', "incompatible"), (valid + 'features=["deloxide", "turmoil"]\n', "incompatible"), (valid + 'kinds=["test"]\n', "unsupported"), (valid + 'unknown=true\n', "unknown"), ('[[configuration]]\nname="absent"\n', "missing"), ('configuration=[]\n', "no declared")):
                path.write_text(text)
                with self.assertRaisesRegex(AnalysisError, error):
                    typed_ratchet.load_configurations(root)

    def test_the_declared_admission_reserves_the_server_library_in_the_hosts_queue(self) -> None:
        policy = typed_ratchet.load_admission(ROOT)
        self.assertEqual(policy.directory, analysis_admission.host_directory())
        self.assertGreater(policy.reservation(Compiled("nervix_server", "lib")), policy.threshold_bytes)
        self.assertGreater(policy.wait_limit_seconds, 0)

    def test_a_malformed_admission_declaration_names_its_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            path = root / TOOLING / "configurations.toml"
            path.parent.mkdir(parents=True)
            head = "[admission]\nthreshold_gib = 4\nwait_limit_minutes = 30\n"
            reservation = '[[admission.reservation]]\ncrate = "nervix_server"\ngib = 16\nkind = "lib"\n'
            path.write_text(head + reservation)
            self.assertEqual(typed_ratchet.load_admission(root).reservations, {Compiled("nervix_server", "lib"): 16 * GIB})
            path.write_text(head + "reservation = []\n")
            self.assertEqual(typed_ratchet.load_admission(root).reservations, {})
            for text, error in (
                ("", "needs exactly threshold_gib, wait_limit_minutes and reservation"),
                (head, "needs exactly threshold_gib"),
                (head + "reservation = []\nunknown = 1\n", "needs exactly threshold_gib"),
                ("[admission]\nthreshold_gib = 0\nwait_limit_minutes = 30\nreservation = []\n", "positive whole numbers"),
                ("[admission]\nthreshold_gib = true\nwait_limit_minutes = 30\nreservation = []\n", "positive whole numbers"),
                ("[admission]\nthreshold_gib = 4\nwait_limit_minutes = 0.5\nreservation = []\n", "positive whole numbers"),
                (head + 'reservation = "nervix_server"\n', "reservations are a list"),
                (head + "reservation = [1]\n", "needs exactly a crate, a kind and its gib"),
                (head + '[[admission.reservation]]\ncrate = "nervix_server"\nkind = "lib"\n', "needs exactly a crate, a kind and its gib"),
                (head + '[[admission.reservation]]\ncrate = ""\ngib = 16\nkind = "lib"\n', "names a crate and a lib or bin kind"),
                (head + '[[admission.reservation]]\ncrate = "nervix_server"\ngib = 16\nkind = "test"\n', "names a crate and a lib or bin kind"),
                (head + reservation + reservation, "duplicate admission reservation for nervix_server \\(lib\\)"),
                (head + '[[admission.reservation]]\ncrate = "nervix_server"\ngib = 4\nkind = "lib"\n', "is a whole number of GiB above the 4 GiB threshold"),
                (head + '[[admission.reservation]]\ncrate = "nervix_server"\ngib = 16.5\nkind = "lib"\n', "is a whole number of GiB above"),
            ):
                with self.subTest(text=text):
                    path.write_text(text)
                    with self.assertRaisesRegex(AnalysisError, "^tools/nervix-lint/configurations.toml: .*" + error):
                        typed_ratchet.load_admission(root)


class RunnerExecutionTests(CompletionTests):
    def setUp(self) -> None:
        super().setUp()
        self.runner.target = self.root / "target"
        self.runner.work = self.root / "evidence"
        self.runner.inputs = {"src/lib.rs": digest(self.source)}
        self.runner.environment = {**os.environ, "RUSTC_WRAPPER": "kache", "KACHE_KEY_SALT": "qualified"}
        self.runner.driver = self.root / "driver"
        self.runner.wrapper = self.root / "wrapper"
        self.runner.expected_targets = mock.Mock(return_value=self.expected)
        self.runner.clean_authored_artifacts = mock.Mock()

    def cargo_result(self, configuration, build, reports, salt):
        reports.mkdir(parents=True, exist_ok=True)
        atomic_json(reports / "example-abcd.json", self.report)
        (reports.parent / "cargo.jsonl").write_text("\n".join(json.dumps(message) for message in self.messages) + "\n")
        return self.messages

    def test_cargo_fresh_and_complete_cached_evidence_are_distinct(self) -> None:
        self.runner.cargo = mock.Mock(side_effect=self.cargo_result)
        with mock.patch("scripts.typed_ratchet.source_inputs", return_value=self.runner.inputs):
            first = self.runner.analyze(self.configuration)
            self.assertEqual(self.runner.analyze(self.configuration), first)
            self.assertEqual(self.runner.cargo.call_count, 1)
            self.assertEqual(self.runner.analyze(self.configuration, fresh=True), first)
            self.assertEqual(self.runner.cargo.call_count, 2)

    def test_missing_report_reestablishes_analysis_without_changing_wrapper(self) -> None:
        calls = []
        def cargo(configuration, build, reports, salt):
            calls.append((build, reports, salt))
            if len(calls) == 1:
                reports.mkdir(parents=True, exist_ok=True)
                return self.messages
            return self.cargo_result(configuration, build, reports, salt)
        self.runner.cargo = cargo
        with mock.patch("scripts.typed_ratchet.source_inputs", return_value=self.runner.inputs):
            evidence = self.runner.analyze(self.configuration)
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0][0], calls[1][0])
        self.assertNotEqual(calls[0][2], calls[1][2])
        self.assertEqual(self.runner.environment["RUSTC_WRAPPER"], "kache")
        self.runner.clean_authored_artifacts.assert_called_once()
        self.assertTrue(evidence["complete"])

    def test_repeated_missing_report_and_changed_inputs_fail_closed(self) -> None:
        self.runner.cargo = mock.Mock(return_value=self.messages)
        with self.assertRaisesRegex(AnalysisError, "missing.*report"):
            self.runner.analyze(self.configuration)
        self.runner.cargo = mock.Mock(side_effect=self.cargo_result)
        with mock.patch("scripts.typed_ratchet.source_inputs", return_value={"changed": "source"}):
            with self.assertRaisesRegex(AnalysisError, "changed during"):
                self.runner.analyze(self.configuration)
        self.assertFalse((self.runner.work / "ordinary/completion.json").exists())

    def test_tampered_completion_is_rejected_and_changed_artifacts_are_reanalyzed(self) -> None:
        self.runner.cargo = mock.Mock(side_effect=self.cargo_result)
        with mock.patch("scripts.typed_ratchet.source_inputs", return_value=self.runner.inputs):
            evidence = self.runner.analyze(self.configuration)
            completion = self.runner.work / "ordinary/completion.json"
            atomic_json(completion, {**evidence, "reports": {}})
            with self.assertRaisesRegex(AnalysisError, "disagrees"):
                self.runner.analyze(self.configuration)
            atomic_json(completion, evidence)
            self.artifact.write_bytes(b"different metadata")
            updated = self.runner.analyze(self.configuration)
            self.assertNotEqual(updated["files"], evidence["files"])

    def test_expected_roots_require_libraries_and_binaries_with_optional_workspace_dependencies(self) -> None:
        metadata = {"workspace_members": ["one", "two"], "packages": [{"id": name, "name": name, "targets": [{"name": name, "kind": ["lib"], "src_path": f"{name}/lib.rs"}, {"name": name + "-cli", "kind": ["bin"], "src_path": f"{name}/main.rs"}, {"name": "build-script-build", "kind": ["custom-build"], "src_path": f"{name}/build.rs"}]} for name in ["one", "two"]]}
        with mock.patch("scripts.typed_ratchet.command", return_value=json.dumps(metadata)):
            selected = Runner.expected_targets(self.runner, Configuration("selected", "Cargo.toml", ("one",), ("lib", "bin")))
            self.assertEqual(len(selected), 3)
            self.assertEqual(sum(item["required"] for item in selected.values()), 2)
            with self.assertRaisesRegex(AnalysisError, "unknown requested"):
                Runner.expected_targets(self.runner, Configuration("selected", "Cargo.toml", ("unknown",)))
        with mock.patch("scripts.typed_ratchet.command", return_value=json.dumps({"workspace_members": [], "packages": []})):
            with self.assertRaisesRegex(AnalysisError, "no declared targets"):
                Runner.expected_targets(self.runner, Configuration("none", "Cargo.toml", ()))

    def test_cargo_wrapper_nesting_and_build_completion(self) -> None:
        captured = []
        def popen(args, **kwargs):
            captured.append((args, kwargs["env"]))
            kwargs["stdout"].write(json.dumps({"reason": "build-finished", "success": True}) + "\n")
            return mock.Mock(wait=mock.Mock(return_value=0))
        with mock.patch("scripts.typed_ratchet.subprocess.Popen", side_effect=popen):
            messages = Runner.cargo(self.runner, self.configuration, self.root / "build", self.reports, "current namespace")
        self.assertEqual(messages[-1]["reason"], "build-finished")
        self.assertEqual(captured[0][1]["RUSTC_WRAPPER"], "kache")
        self.assertEqual(captured[0][1]["RUSTC_WORKSPACE_WRAPPER"], str(self.runner.wrapper))
        self.assertEqual(captured[0][1]["KACHE_KEY_SALT"], "current namespace")
        self.assertEqual(Policy.decode(captured[0][1]["NERVIX_LINT_ADMISSION"]), self.runner.admission)
        with mock.patch("scripts.typed_ratchet.subprocess.Popen", return_value=mock.Mock(wait=mock.Mock(return_value=1))):
            with self.assertRaisesRegex(AnalysisError, "did not finish"):
                Runner.cargo(self.runner, self.configuration, self.root / "build", self.reports, "namespace")

    def failed_cargo(self, messages: list[dict], terminations: list[dict]) -> str:
        def popen(args, **kwargs):
            for message in messages:
                kwargs["stdout"].write(json.dumps(message) + "\n")
            for termination in terminations:
                atomic_json(self.reports / "compilations" / f"{termination['crate_name']}{termination['extra_filename']}.json", termination)
            return mock.Mock(wait=mock.Mock(return_value=101))
        with mock.patch("scripts.typed_ratchet.subprocess.Popen", side_effect=popen):
            with self.assertRaisesRegex(AnalysisError, "compiler run failed or did not finish") as failure:
                Runner.cargo(self.runner, self.configuration, self.root / "build", self.reports, "namespace")
        return str(failure.exception)

    def test_a_failed_compilation_names_how_each_analysis_compiler_ended(self) -> None:
        finished = {"reason": "build-finished", "success": False}
        target = {"name": "example", "kind": ["lib"], "src_path": str(self.source)}
        error = {"reason": "compiler-message", "package_id": "example-id", "target": target, "message": {"level": "error", "message": "mismatched types", "rendered": "error: mismatched types", "code": None}}
        termination = {"crate_name": "example", "extra_filename": "-abcd", "crate_types": ["lib"], "source": "src/lib.rs", "peak_resident_bytes": 14 * 2**30, "started_unix_ns": 1, "seconds": 412.0, "oom_kills": 1, "admission": None}
        killed = self.failed_cargo([finished], [{**termination, "ending": {"kind": "signal", "signal": 9, "name": "SIGKILL"}}])
        self.assertRegex(killed, r"example \(lib\): the analysis compiler was terminated by signal 9 \(SIGKILL\) after 412s at a peak resident size of 14\.0 GiB; the kernel's out-of-memory killer recorded 1 kill in its cgroup while it ran; it wrote no compiler report")
        stopped = self.failed_cargo([finished], [{**termination, "crate_types": [], "oom_kills": 0, "ending": {"kind": "signal", "signal": 15, "name": None}}])
        self.assertIn("example (unknown): the analysis compiler was terminated by signal 15 (unnamed) after 412s at a peak resident size of 14.0 GiB; it wrote no compiler report", stopped)
        failed = self.failed_cargo([error, finished], [{**termination, "ending": {"kind": "exit", "status": 1}}])
        self.assertIn("example (lib): the analysis compiler failed with 1 compiler error (exit status 1)", failed)
        driver = self.failed_cargo([finished], [{**termination, "oom_kills": None, "ending": {"kind": "exit", "status": 1}}])
        self.assertIn("example (lib): the analysis compiler exited with status 1 without a compiler error, so the analysis driver itself failed", driver)
        self.assertNotIn("out-of-memory", driver)
        outside = self.failed_cargo([finished], [])
        self.assertIn("no analysis compilation ended unsuccessfully, so Cargo, a build script or the compiler wrapper failed outside them", outside)
        succeeded = self.failed_cargo([finished], [{**termination, "ending": {"kind": "exit", "status": 0}}])
        self.assertIn("no analysis compilation ended unsuccessfully", succeeded)

    def test_a_previous_runs_compilation_is_not_reported_again(self) -> None:
        stale = {"crate_name": "example", "extra_filename": "-abcd", "crate_types": ["lib"], "source": "src/lib.rs", "peak_resident_bytes": 1, "started_unix_ns": 1, "seconds": 1.0, "oom_kills": 0, "admission": None, "ending": {"kind": "signal", "signal": 9, "name": "SIGKILL"}}
        atomic_json(self.reports / "compilations/example-abcd.json", stale)
        outside = self.failed_cargo([{"reason": "build-finished", "success": False}], [])
        self.assertNotIn("SIGKILL", outside)
        self.assertFalse((self.reports / "compilations/example-abcd.json").exists())

    def test_clean_only_this_analysis_build(self) -> None:
        with mock.patch("scripts.typed_ratchet.command") as command:
            Runner.clean_authored_artifacts(self.runner, self.configuration, self.expected, self.root / "isolated")
        self.assertIn(str(self.root / "isolated"), command.call_args.args[0])
        self.assertIn("example", command.call_args.args[0])
        self.assertEqual(command.call_args.kwargs["env"]["RUSTC_WRAPPER"], "kache")

    def test_cross_target_reanalysis_cleans_the_analyzed_target(self) -> None:
        configuration = Configuration("browser", "Cargo.toml", ("example",), target="wasm32-unknown-unknown")
        with mock.patch("scripts.typed_ratchet.command") as command:
            Runner.clean_authored_artifacts(self.runner, configuration, self.expected, self.root / "isolated")
        arguments = command.call_args.args[0]
        self.assertIn("--target", arguments)
        self.assertEqual(arguments[arguments.index("--target") + 1], configuration.target)
        self.assertEqual(arguments[arguments.index("--target-dir") + 1], str(self.root / "isolated"))
        self.assertEqual(command.call_args.kwargs["env"]["RUSTC_WRAPPER"], "kache")

    def test_required_gate_rejects_warnings_and_inventory_records_them(self) -> None:
        messages = [{"reason": "compiler-message", "message": {"level": "warning", "code": {"code": "nervix::sync_acquisition"}}}]
        self.runner.environment["NERVIX_LINT_MODE"] = "gate"
        with self.assertRaisesRegex(AnalysisError, "unresolved Nervix diagnostic"):
            self.runner.check_diagnostics(messages)
        self.runner.environment["NERVIX_LINT_MODE"] = "inventory"
        self.runner.check_diagnostics(messages)
        self.runner.environment["NERVIX_LINT_MODE"] = "gate"
        self.runner.check_diagnostics([{"reason": "compiler-message", "message": {"code": None}}])


class CompilationMeasurementTests(CompletionTests):
    def setUp(self) -> None:
        super().setUp()
        for message in self.messages:
            message["fresh"] = False
        self.admission = {"reservation_bytes": 16 * GIB, "outcome": "fit", "waited_seconds": 93.0, "left_bytes": 40 * GIB, "holders": 1, "error": None}
        self.record = {"crate_name": "example", "extra_filename": "-abcd", "crate_types": ["lib"], "source": str(self.source), "ending": {"kind": "exit", "status": 0}, "peak_resident_bytes": 13 * GIB, "started_unix_ns": 5, "seconds": 119.0, "oom_kills": 0, "admission": self.admission}
        self.record_path = self.reports / "compilations/example-abcd.json"
        atomic_json(self.record_path, self.record)

    def measure(self) -> dict:
        return self.runner.measure_compilations(self.configuration, self.expected, self.messages, self.reports)

    def test_a_declared_compilation_within_its_reservation_is_measured(self) -> None:
        self.assertEqual(self.measure(), {self.key: {"crate_name": "example", "kind": "lib", "peak_resident_bytes": 13 * GIB, "started_unix_ns": 5, "seconds": 119.0, "admission": self.admission}})
        atomic_json(self.record_path, {**self.record, "peak_resident_bytes": 16 * GIB})
        self.assertEqual(self.measure()[self.key]["peak_resident_bytes"], 16 * GIB)

    def test_a_compilation_above_its_reservation_names_the_declaration(self) -> None:
        atomic_json(self.record_path, {**self.record, "peak_resident_bytes": 17 * GIB})
        with self.assertRaisesRegex(AnalysisError, r"^ordinary: example \(lib\) peaked at 17\.0 GiB, above its 16 GiB admission reservation; raise the reservation in tools/nervix-lint/configurations\.toml$"):
            self.measure()

    def test_an_undeclared_compilation_above_the_threshold_names_the_declaration(self) -> None:
        self.runner.admission = Policy(self.root / "queue", 4 * GIB, 60.0, {})
        atomic_json(self.record_path, {**self.record, "peak_resident_bytes": 4 * GIB, "admission": None})
        self.assertEqual(self.measure()[self.key]["peak_resident_bytes"], 4 * GIB)
        atomic_json(self.record_path, {**self.record, "peak_resident_bytes": 4 * GIB + 1, "admission": None})
        with self.assertRaisesRegex(AnalysisError, r"^ordinary: example \(lib\) peaked at 4\.0 GiB, above the 4 GiB admission threshold, without an admission reservation; declare one in tools/nervix-lint/configurations\.toml$"):
            self.measure()

    def test_fresh_targets_and_dependencies_outside_the_workspace_have_no_measurement(self) -> None:
        self.record_path.unlink()
        fresh = [{**message, "fresh": True} for message in self.messages]
        self.assertEqual(self.runner.measure_compilations(self.configuration, self.expected, fresh, self.reports), {})
        dependency = [{**message, "package_id": "registry-id"} for message in self.messages]
        self.assertEqual(self.runner.measure_compilations(self.configuration, self.expected, dependency, self.reports), {})

    def test_a_workspace_compilation_outside_the_expected_targets_is_held_to_the_declaration(self) -> None:
        # A binary configuration expects only binaries, but compiles the libraries they depend on.
        library = [{**message, "package_id": "library-id"} for message in self.messages]
        measured = self.runner.measure_compilations(self.configuration, self.expected, library, self.reports)
        self.assertEqual(list(measured), ["library-id::example::lib"])
        self.assertEqual(measured["library-id::example::lib"]["peak_resident_bytes"], 13 * GIB)
        atomic_json(self.record_path, {**self.record, "peak_resident_bytes": 17 * GIB})
        with self.assertRaisesRegex(AnalysisError, "above its 16 GiB admission reservation"):
            self.runner.measure_compilations(self.configuration, self.expected, library, self.reports)

    def test_a_missing_ambiguous_or_failed_record_fails(self) -> None:
        self.messages[0]["filenames"].append(str(self.root / "libexample-efgh.rlib"))
        atomic_json(self.reports / "compilations/example-efgh.json", self.record)
        with self.assertRaisesRegex(AnalysisError, "ordinary: example-id::example::lib: ambiguous compilation records"):
            self.measure()
        (self.reports / "compilations/example-efgh.json").unlink()
        atomic_json(self.record_path, {**self.record, "ending": {"kind": "signal", "signal": 9, "name": "SIGKILL"}})
        with self.assertRaisesRegex(AnalysisError, "compilation record names a failure"):
            self.measure()
        self.record_path.unlink()
        with self.assertRaisesRegex(AnalysisError, "ordinary: example-id::example::lib: missing compilation record"):
            self.measure()

    def test_analysis_evidence_keeps_the_measurements_and_prints_the_waits(self) -> None:
        self.runner.target = self.root / "target"
        self.runner.work = self.root / "evidence"
        self.runner.inputs = {"src/lib.rs": digest(self.source)}
        self.runner.environment = {**os.environ, "NERVIX_LINT_MODE": "gate"}
        self.runner.expected_targets = mock.Mock(return_value=self.expected)

        def cargo(configuration, build, reports, salt):
            reports.mkdir(parents=True, exist_ok=True)
            atomic_json(reports / "example-abcd.json", self.report)
            atomic_json(reports / "compilations/example-abcd.json", self.record)
            (reports.parent / "cargo.jsonl").write_text("\n".join(json.dumps(message) for message in self.messages) + "\n")
            return self.messages

        self.runner.cargo = cargo
        output = io.StringIO()
        with mock.patch("scripts.typed_ratchet.source_inputs", return_value=self.runner.inputs), contextlib.redirect_stdout(output):
            evidence = self.runner.analyze(self.configuration)
        self.assertEqual(evidence["compilations"][self.key]["peak_resident_bytes"], 13 * GIB)
        self.assertEqual(json.loads((self.runner.work / "ordinary/completion.json").read_text())["compilations"], evidence["compilations"])
        self.assertIn("typed ratchet: ordinary: the largest analysis compilation, example (lib), peaked at 13.0 GiB in 119 s", output.getvalue())
        self.assertIn("typed ratchet: ordinary: example (lib) waited 93 s for host memory admission (fit) of its 16 GiB reservation and peaked at 13.0 GiB", output.getvalue())

    def test_a_failed_measurement_discards_the_analysis_so_the_next_run_measures_again(self) -> None:
        self.runner.target = self.root / "target"
        self.runner.work = self.root / "evidence"
        self.runner.inputs = {"src/lib.rs": digest(self.source)}
        self.runner.expected_targets = mock.Mock(return_value=self.expected)
        self.runner.clean_authored_artifacts = mock.Mock()
        oversized = {**self.record, "peak_resident_bytes": 17 * GIB}

        def cargo(configuration, build, reports, salt):
            reports.mkdir(parents=True, exist_ok=True)
            atomic_json(reports / "example-abcd.json", self.report)
            atomic_json(reports / "compilations/example-abcd.json", oversized)
            (reports.parent / "cargo.jsonl").write_text("\n".join(json.dumps(message) for message in self.messages) + "\n")
            return self.messages

        self.runner.cargo = cargo
        with mock.patch("scripts.typed_ratchet.source_inputs", return_value=self.runner.inputs):
            with self.assertRaisesRegex(AnalysisError, "above its 16 GiB admission reservation"):
                self.runner.analyze(self.configuration)
        self.runner.clean_authored_artifacts.assert_called_once_with(self.configuration, self.expected, self.runner.target / "typed-ratchet/build/ordinary")
        self.assertFalse((self.runner.work / "ordinary/completion.json").exists())

    def test_measurement_lines_name_the_largest_compilation_and_admissions_that_waited_or_did_not_fit(self) -> None:
        self.assertEqual(typed_ratchet.measurement_lines(self.configuration, {}), [])
        small = {"crate_name": "small", "kind": "lib", "peak_resident_bytes": GIB, "started_unix_ns": 1, "seconds": 2.0, "admission": None}
        prompt = {**small, "crate_name": "prompt", "peak_resident_bytes": 2 * GIB, "started_unix_ns": 2, "admission": {**self.admission, "waited_seconds": 0.4}}
        alone = {**small, "crate_name": "alone", "peak_resident_bytes": 3 * GIB, "started_unix_ns": 4, "admission": {**self.admission, "outcome": "alone", "waited_seconds": 0.0}}
        waited = {**small, "crate_name": "waited", "peak_resident_bytes": 5 * GIB, "seconds": 120.0, "started_unix_ns": 3, "admission": self.admission}
        self.assertEqual(typed_ratchet.measurement_lines(self.configuration, {"a": small, "b": prompt, "c": alone, "d": waited}), [
            "typed ratchet: ordinary: the largest analysis compilation, waited (lib), peaked at 5.0 GiB in 120 s",
            "typed ratchet: ordinary: waited (lib) waited 93 s for host memory admission (fit) of its 16 GiB reservation and peaked at 5.0 GiB",
            "typed ratchet: ordinary: alone (lib) waited 0 s for host memory admission (alone) of its 16 GiB reservation and peaked at 3.0 GiB",
        ])


class MainTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = pathlib.Path(self.temporary.name)
        (self.root / TOOLING).mkdir(parents=True)
        self.runner = mock.Mock(root=self.root, target=self.root / "target", work=self.root / "evidence", compiler="qualified compiler", identity="qualified identity")
        self.runner.inputs = {"src/lib.rs": "current source"}
        self.runner.analyze.side_effect = lambda configuration, **kwargs: {"configuration": json.loads(typed_ratchet.encode(dataclasses.asdict(configuration))), "reports": {}, "complete": True}
        self.configurations = [Configuration("ordinary", "Cargo.toml", ())]
        for name, value in (("Runner", self.runner), ("load_configurations", self.configurations), ("summarize", []), ("source_inputs", self.runner.inputs)):
            patch = mock.patch("scripts.typed_ratchet." + name, return_value=value)
            patch.start()
            self.addCleanup(patch.stop)

    def run_main(self, *args: str) -> tuple[int, str]:
        output = io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
            status = typed_ratchet.main(["--root", str(self.root), *args])
        return status, output.getvalue()

    def test_inventory_and_diagnostic_gate_have_explicit_different_evidence(self) -> None:
        status, _ = self.run_main("--inventory")
        self.assertEqual(status, 0)
        path = self.runner.work / "inventory.json"
        inventory = json.loads(path.read_text())
        self.assertFalse(inventory["diagnostics_checked"])
        status, _ = self.run_main()
        self.assertEqual(status, 0)
        current = json.loads(path.read_text())
        self.assertTrue(current["diagnostics_checked"])

    def test_partial_unknown_matrix_and_analysis_failure_do_not_pass(self) -> None:
        for arguments, text in ((("--configuration", "ordinary"), "whole declared matrix"), (("--configuration", "missing", "--inventory"), "unknown requested")):
            status, output = self.run_main(*arguments)
            self.assertEqual(status, 1)
            self.assertIn(text, output)
        with mock.patch("scripts.typed_ratchet.Runner", side_effect=AnalysisError("missing compiler")):
            self.assertEqual(self.run_main("--inventory")[0], 1)

    def test_fresh_fixture_recompilation_and_environment_target_are_forwarded(self) -> None:
        with mock.patch.dict(os.environ, {"CARGO_TARGET_DIR": str(self.root / "selected-target")}):
            status, _ = self.run_main("--fixture-mode", "ordinary", "--inventory", "--recompile")
            self.assertEqual(status, 0)
            typed_ratchet.Runner.assert_called_with(self.root, self.root / "selected-target", inventory=True)
        self.runner.clean_authored_artifacts.assert_called_once()
        self.assertTrue(self.runner.analyze.call_args.kwargs["fresh"])
        self.assertEqual(self.runner.analyze.call_args.args[0].name, "fixture-ordinary")

    def test_turmoil_uses_a_separate_just_invocation_and_rejects_wrong_evidence(self) -> None:
        self.configurations[:] = [Configuration("turmoil", "Cargo.toml", (), features=("turmoil",))]
        separate = {"complete": True, "compiler": self.runner.compiler, "root": str(self.root), "diagnostics_checked": False, "source_identity": typed_ratchet.hashlib.sha256(typed_ratchet.encode(self.runner.inputs)).hexdigest(), "evidence": [{"configuration": {"name": "turmoil"}, "reports": {}}]}
        def nested(args, **kwargs):
            self.assertEqual(args[:2], ["just", "typed-ratchet-turmoil"])
            atomic_json(self.runner.work / "turmoil.json", separate)
            return mock.Mock(returncode=0)
        with mock.patch.dict(os.environ, {"RUSTFLAGS": ""}), mock.patch("scripts.typed_ratchet.subprocess.run", side_effect=nested):
            self.assertEqual(self.run_main("--inventory", "--fresh")[0], 0)
            for field, value in (("complete", False), ("compiler", "wrong compiler"), ("root", "wrong worktree"), ("diagnostics_checked", True), ("source_identity", "different inputs")):
                previous = separate[field]
                separate[field] = value
                self.assertEqual(self.run_main("--inventory")[0], 1)
                separate[field] = previous
            separate["evidence"][0]["configuration"]["name"] = "shuttle"
            self.assertEqual(self.run_main("--inventory")[0], 1)
        with mock.patch.dict(os.environ, {"RUSTFLAGS": ""}), mock.patch("scripts.typed_ratchet.subprocess.run", return_value=mock.Mock(returncode=1)):
            self.assertEqual(self.run_main("--fixture-mode", "turmoil", "--inventory", "--recompile")[0], 1)

    def test_turmoil_child_cannot_select_an_arbitrary_partial_gate(self) -> None:
        for flags in ((), ("--configuration", "ordinary"), ("--fixture-mode", "turmoil")):
            self.assertEqual(self.run_main("--turmoil-child", *flags)[0], 1)
        self.configurations[:] = [Configuration("turmoil", "Cargo.toml", (), features=("turmoil",))]
        with mock.patch.dict(os.environ, {"RUSTFLAGS": "--cfg tokio_unstable"}):
            self.assertEqual(self.run_main("--turmoil-child", "--configuration", "turmoil")[0], 0)
            result = json.loads((self.runner.work / "inventory.json").read_text())
            self.assertTrue(result["diagnostics_checked"])
            self.assertFalse(result["matrix_complete"])

    def test_configurations_analyze_beside_each_other_and_keep_their_declared_order(self) -> None:
        names = ["ordinary", "browser", "testing", "shuttle", "loom"]
        self.configurations[:] = [Configuration(name, "Cargo.toml", ()) for name in names]
        # The first three wait for each other, so the analysis passes only when they run at once.
        together = threading.Barrier(3, timeout=30)
        running = []
        most = []
        lock = threading.Lock()

        def analyze(configuration, **kwargs):
            with lock:
                running.append(configuration.name)
                most.append(len(running))
            if configuration.name in names[:3]:
                together.wait()
            # Later configurations end first, so the evidence's order is the declared one, not the
            # order the analyses end in.
            time.sleep(0.01 * (len(names) - names.index(configuration.name)))
            with lock:
                running.remove(configuration.name)
            return {"configuration": {"name": configuration.name}, "reports": {}, "complete": True}

        self.runner.analyze.side_effect = analyze
        status, output = self.run_main("--jobs", "3")
        self.assertEqual(status, 0, output)
        inventory = json.loads((self.runner.work / "inventory.json").read_text())
        self.assertEqual([entry["configuration"]["name"] for entry in inventory["evidence"]], names)
        self.assertEqual(max(most), 3)

    def test_a_failed_configuration_starts_no_other_and_fails_the_gate(self) -> None:
        names = ["ordinary", "browser", "testing"]
        self.configurations[:] = [Configuration(name, "Cargo.toml", ()) for name in names]
        started = []

        def analyze(configuration, **kwargs):
            started.append(configuration.name)
            if configuration.name == "ordinary":
                raise AnalysisError("ordinary: the server analysis was killed")
            return {"configuration": {"name": configuration.name}, "reports": {}, "complete": True}

        self.runner.analyze.side_effect = analyze
        status, output = self.run_main("--jobs", "1")
        self.assertEqual(status, 1)
        self.assertIn("ordinary: the server analysis was killed", output)
        self.assertEqual(started, ["ordinary"])

    def test_the_number_of_configurations_at_once_comes_from_the_environment_or_the_default(self) -> None:
        self.assertEqual(typed_ratchet.analysis_jobs({}), typed_ratchet.DEFAULT_ANALYSIS_JOBS)
        self.assertEqual(typed_ratchet.analysis_jobs({typed_ratchet.JOBS_VARIABLE: "6"}), 6)
        for value in ("0", "many"):
            with self.subTest(value=value), self.assertRaises(AnalysisError):
                typed_ratchet.analysis_jobs({typed_ratchet.JOBS_VARIABLE: value})
        with mock.patch.dict(os.environ, {typed_ratchet.JOBS_VARIABLE: "0"}):
            status, output = self.run_main()
        self.assertEqual(status, 1)
        self.assertIn(typed_ratchet.JOBS_VARIABLE, output)

    def test_source_changes_during_matrix_are_rejected(self) -> None:
        with mock.patch("scripts.typed_ratchet.source_inputs", return_value={"src/lib.rs": "changed source"}):
            self.assertEqual(self.run_main()[0], 1)

    def test_source_contract_and_expansion_origins_are_present(self) -> None:
        finding = {"receiver_type": "&DashMap<u32, u32>", "operation": "get", "acquisition": "shared", "owner": "example::batch", "context": {"context": "bounded", "reason": "bounded admission", "key": "branch identity", "bound": "one admitted batch"}, "span": {"line": 3, "column": 2}, "expansion": [{"macro_name": "external::forward", "call_site": "src/lib.rs:2"}]}
        site = {"site": {"path": "src/lib.rs", "start": 10}, "configurations": {"ordinary:example": [finding]}}
        with mock.patch("scripts.typed_ratchet.summarize", return_value=[site]):
            status, output = self.run_main("--show")
        self.assertEqual(status, 0)
        for text in ("src/lib.rs:3:3", "DashMap", "shared", "bounded admission", "branch identity", "one admitted batch", "ordinary:example", "external::forward"):
            self.assertIn(text, output)


class WorkspaceWrapperTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = pathlib.Path(self.temporary.name)
        self.reports = self.root / "reports"
        self.reports.mkdir()
        self.arguments = ["/rustc", "--crate-name", "example", "src/lib.rs", "--crate-type", "lib", "-C", "extra-filename=-abcd"]

    def driver(self, body: str) -> pathlib.Path:
        path = self.root / "driver"
        path.write_text(f"#!{sys.executable}\nimport os, signal, sys\n{body}\n")
        path.chmod(0o755)
        return path

    def environment(self, driver: pathlib.Path, policy: Policy | None = None) -> dict[str, str]:
        if policy is None:
            policy = Policy(self.root / "queue", 0, 60.0, {})
        return {"NERVIX_LINT_DRIVER": str(driver), "NERVIX_LINT_REPORTS": str(self.reports), "NERVIX_LINT_ADMISSION": policy.encode(), "NERVIX_LINT_CONFIGURATION": "testing", "NERVIX_LINT_ROOT": str(self.root)}

    def wrap(self, driver: pathlib.Path, inherited: tuple[int, ...] = (), policy: Policy | None = None) -> subprocess.CompletedProcess[str]:
        environment = {**os.environ, **self.environment(driver, policy)}
        environment.pop("NERVIX_NATIVE_COVERAGE_ATTEMPT", None)
        return subprocess.run([sys.executable, str(ROOT / "scripts/typed_lint_wrapper.py"), *self.arguments], env=environment, capture_output=True, text=True, pass_fds=inherited)

    def record(self) -> dict:
        return json.loads((self.reports / "compilations/example-abcd.json").read_text())

    @staticmethod
    def stop_driver(pid: int) -> None:
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)

    def lifetime(self) -> tuple[int, int]:
        """A pipe whose reading end a driver blocks on until the test ends and closes its writing end."""
        reading, writing = os.pipe()
        self.addCleanup(os.close, reading)
        self.addCleanup(os.close, writing)
        return reading, writing

    def start_wrapper(self, driver: pathlib.Path, lifetime: int) -> subprocess.Popen[bytes]:
        """The wrapper running `driver`, which inherits `lifetime` through it as Cargo's jobserver does."""
        environment = {**os.environ, **self.environment(driver)}
        environment.pop("NERVIX_NATIVE_COVERAGE_ATTEMPT", None)
        wrapper = subprocess.Popen([sys.executable, str(ROOT / "scripts/typed_lint_wrapper.py"), *self.arguments], env=environment, pass_fds=(lifetime,))
        self.addCleanup(wrapper.wait)
        # The wrapper is stopped before it is waited for, so no failure waits for its driver to end.
        self.addCleanup(lambda: wrapper.poll() is None and wrapper.kill())
        return wrapper

    def driver_pid(self, started: pathlib.Path, wrapper: subprocess.Popen[bytes], incomplete: Callable[[], None] = lambda: None) -> int:
        """The PID a running driver records in `started`, once its record is whole.

        Creating a file and writing it are two steps, so a record that exists can still be empty or
        partly written. The driver ends its record with a newline, which only a whole record has.
        `incomplete` runs every time the record is missing or not whole yet.
        """
        deadline = time.monotonic() + 60
        while True:
            if started.exists():
                record = started.read_text()
                if record.endswith("\n"):
                    return int(record)
            self.assertIsNone(wrapper.poll(), "the wrapper ended before its driver started")
            self.assertLess(time.monotonic(), deadline, "the driver never started")
            incomplete()
            time.sleep(0.01)

    def test_driver_cleanup_tolerates_exit_between_its_status_check_and_signal(self) -> None:
        with mock.patch("os.kill", side_effect=ProcessLookupError) as kill:
            self.stop_driver(12345)
        kill.assert_called_once_with(12345, signal.SIGKILL)

    def test_driver_cleanup_preserves_other_signal_failures(self) -> None:
        with mock.patch("os.kill", side_effect=PermissionError), self.assertRaises(PermissionError):
            self.stop_driver(12345)

    def test_wrapper_forwards_compiler_and_arguments_with_optional_coverage_recording(self) -> None:
        for coverage in ("", "/attempt"):
            with mock.patch.dict(os.environ, {"NERVIX_LINT_DRIVER": "/driver", "NERVIX_LINT_REPORTS": str(self.reports), "NERVIX_NATIVE_COVERAGE_ATTEMPT": coverage}), mock.patch("sys.argv", ["wrapper", "/rustc", "--crate-name", "example"]), mock.patch("subprocess.Popen", side_effect=RuntimeError("started")) as start:
                with self.assertRaisesRegex(RuntimeError, "started"):
                    runpy.run_module("scripts.typed_lint_wrapper", run_name="__main__")
            arguments = start.call_args.args[0]
            self.assertEqual(arguments[-4:], ["/driver", "/rustc", "--crate-name", "example"])
            self.assertEqual("exec" in arguments, bool(coverage))

    def test_the_driver_inherits_the_descriptors_the_wrapper_inherited(self) -> None:
        reading, writing = os.pipe()
        self.addCleanup(os.close, reading)
        self.addCleanup(os.close, writing)
        # Cargo's jobserver reaches the compiler as inherited descriptors named in CARGO_MAKEFLAGS.
        completed = self.wrap(self.driver(f"os.write({writing}, b'token')"), inherited=(writing,))
        self.assertEqual((completed.returncode, completed.stderr), (0, ""))
        self.assertEqual(os.read(reading, 5), b"token")

    @unittest.skipUnless(sys.platform.startswith("linux"), "a process's parent is read from Linux's /proc")
    def test_the_driver_is_ready_only_once_its_pid_record_is_whole(self) -> None:
        started = self.root / "started"
        reading, writing = self.lifetime()
        # The driver creates its record and writes its PID only once the test has seen the record without it.
        driver = self.driver(f"import pathlib\nrecord = pathlib.Path({str(started)!r}).open('w')\nos.read({reading}, 1)\nrecord.write(f'{{os.getpid()}}\\n')\nrecord.close()\nos.read({reading}, 1)")
        wrapper = self.start_wrapper(driver, reading)
        seen: list[str] = []

        def release_the_pid_once_the_record_exists() -> None:
            if started.exists() and not seen:
                seen.append(started.read_text())
                os.write(writing, b"1")

        pid = self.driver_pid(started, wrapper, release_the_pid_once_the_record_exists)
        self.assertEqual(seen, [""])
        parent = int(pathlib.Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[1])
        self.assertEqual(parent, wrapper.pid)

    @unittest.skipUnless(sys.platform.startswith("linux"), "the parent-death signal is Linux's")
    def test_the_driver_ends_when_a_compiler_wrapper_stops_its_wrapper(self) -> None:
        started = self.root / "started"
        # The test holds the writing end until it ends, so only the wrapper's death can end the driver first.
        reading, _writing = self.lifetime()
        driver = self.driver(f"import pathlib\npathlib.Path({str(started)!r}).write_text(f'{{os.getpid()}}\\n')\nos.read({reading}, 1)")
        wrapper = self.start_wrapper(driver, reading)
        pid = self.driver_pid(started, wrapper)

        def running() -> bool:
            # An ended driver can briefly remain a zombie until whoever adopted it reaps it.
            try:
                state = pathlib.Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0]
            except (OSError, IndexError):
                return False
            return state != "Z"

        self.addCleanup(lambda: running() and self.stop_driver(pid))
        wrapper.kill()
        wrapper.wait()
        deadline = time.monotonic() + 60
        while running():
            self.assertLess(time.monotonic(), deadline, "the driver outlived its stopped wrapper")
            time.sleep(0.01)
        self.assertEqual(list(self.reports.iterdir()), [])

    def test_a_driver_ended_by_a_signal_leaves_its_termination_and_the_same_signal(self) -> None:
        completed = self.wrap(self.driver("os.kill(os.getpid(), signal.SIGKILL)"))
        self.assertEqual(completed.returncode, -signal.SIGKILL)
        record = self.record()
        self.assertEqual(record["ending"], {"kind": "signal", "signal": signal.SIGKILL, "name": "SIGKILL"})
        self.assertEqual((record["crate_name"], record["extra_filename"], record["crate_types"], record["source"]), ("example", "-abcd", ["lib"], "src/lib.rs"))
        self.assertGreater(record["peak_resident_bytes"], 0)
        self.assertIn("example", completed.stderr)
        self.assertIn("signal 9 (SIGKILL)", completed.stderr)

    def run_main(self, driver: pathlib.Path) -> tuple[int, str]:
        from scripts import typed_lint_wrapper

        handlers = {number: signal.getsignal(number) for number in typed_lint_wrapper.FORWARDED}
        for number, handler in handlers.items():
            self.addCleanup(signal.signal, number, handler)
        output = io.StringIO()
        environment = {**self.environment(driver), "NERVIX_NATIVE_COVERAGE_ATTEMPT": ""}
        with mock.patch.dict(os.environ, environment), contextlib.redirect_stderr(output):
            status = typed_lint_wrapper.main(self.arguments)
        return status, output.getvalue()

    def test_main_records_each_drivers_ending_and_returns_its_status(self) -> None:
        self.assertEqual(self.run_main(self.driver("pass")), (0, ""))
        succeeded = self.record()
        self.assertEqual((succeeded["ending"], succeeded["admission"]), ({"kind": "exit", "status": 0}, None))
        self.assertGreater(succeeded["peak_resident_bytes"], 0)
        self.assertGreater(succeeded["started_unix_ns"], 0)
        status, output = self.run_main(self.driver("sys.exit(3)"))
        self.assertEqual((status, output), (3, ""))
        record = self.record()
        self.assertEqual(record["ending"], {"kind": "exit", "status": 3})
        self.assertEqual(record["crate_name"], "example")
        self.assertGreater(record["peak_resident_bytes"], 0)
        self.assertGreaterEqual(record["seconds"], 0)
        with mock.patch("scripts.typed_lint_wrapper.cgroup_oom_kills", return_value=None):
            self.assertEqual(self.run_main(self.driver("sys.exit(3)"))[0], 3)
        self.assertIsNone(self.record()["oom_kills"])

    def test_an_unwritable_record_keeps_the_drivers_ending(self) -> None:
        self.reports.rmdir()
        status, output = self.run_main(self.driver("sys.exit(3)"))
        self.assertEqual(status, 3)
        self.assertIn("nervix analysis: cannot write the compilation record of example:", output)

    def test_main_names_a_terminating_signal_and_ends_with_it(self) -> None:
        with mock.patch("scripts.typed_lint_wrapper.os.kill") as kill:
            status, output = self.run_main(self.driver("os.kill(os.getpid(), signal.SIGKILL)"))
        kill.assert_called_once_with(os.getpid(), signal.SIGKILL)
        self.assertEqual(status, 128 + signal.SIGKILL)
        self.assertIn("the compilation of example was terminated by signal 9 (SIGKILL); compilation record: " + str(self.reports / "compilations/example-abcd.json"), output)
        self.assertEqual(self.record()["ending"]["name"], "SIGKILL")

    def test_endings_name_signals_and_keep_unnamed_ones_by_number(self) -> None:
        from scripts import typed_lint_wrapper

        self.assertEqual(typed_lint_wrapper.ending(-signal.SIGTERM), {"kind": "signal", "signal": signal.SIGTERM, "name": "SIGTERM"})
        unnamed = next(number for number in range(1, 128) if number not in {item.value for item in signal.Signals})
        self.assertEqual(typed_lint_wrapper.ending(-unnamed), {"kind": "signal", "signal": unnamed, "name": None})
        with mock.patch("scripts.typed_lint_wrapper.os.kill") as kill:
            previous = signal.getsignal(signal.SIGTERM)
            self.addCleanup(signal.signal, signal.SIGTERM, previous)
            self.assertEqual(typed_lint_wrapper.end_like(signal.SIGTERM), 128 + signal.SIGTERM)
            self.assertEqual(signal.getsignal(signal.SIGTERM), signal.SIG_DFL)
        kill.assert_called_once_with(os.getpid(), signal.SIGTERM)

    def test_out_of_memory_kills_come_from_the_unified_cgroup_hierarchy(self) -> None:
        from scripts import typed_lint_wrapper

        membership = self.root / "cgroup"
        hierarchy = self.root / "hierarchy"
        events = hierarchy / "user.slice/build.scope/memory.events"
        events.parent.mkdir(parents=True)
        self.assertIsNone(typed_lint_wrapper.cgroup_oom_kills(membership, hierarchy))
        membership.write_text("12:memory:/user.slice/build.scope\n0::/user.slice/build.scope\n")
        self.assertIsNone(typed_lint_wrapper.cgroup_oom_kills(membership, hierarchy))
        events.write_text("low 0\nhigh 0\nmax 4\noom 1\noom_kill 2\noom_group_kill 0\n")
        self.assertEqual(typed_lint_wrapper.cgroup_oom_kills(membership, hierarchy), 2)
        events.write_text("oom_kill unknown\n")
        self.assertIsNone(typed_lint_wrapper.cgroup_oom_kills(membership, hierarchy))
        membership.write_text("12:memory:/user.slice/build.scope\n")
        self.assertIsNone(typed_lint_wrapper.cgroup_oom_kills(membership, hierarchy))

    def test_platform_specific_lifetime_and_resident_size(self) -> None:
        from scripts import typed_lint_wrapper

        self.assertTrue(callable(typed_lint_wrapper.ending_with(os.getpid())))
        usage = mock.Mock(ru_maxrss=4096)
        with mock.patch("scripts.typed_lint_wrapper.sys.platform", "darwin"), mock.patch("scripts.typed_lint_wrapper.resource.getrusage", return_value=usage):
            self.assertIsNone(typed_lint_wrapper.ending_with(os.getpid()))
            self.assertEqual(typed_lint_wrapper.peak_resident_bytes(), 4096)
        with mock.patch("scripts.typed_lint_wrapper.sys.platform", "linux"), mock.patch("scripts.typed_lint_wrapper.resource.getrusage", return_value=usage):
            self.assertEqual(typed_lint_wrapper.peak_resident_bytes(), 4096 * 1024)

    def test_the_compilation_comes_from_either_spelling_of_its_options(self) -> None:
        from scripts import typed_lint_wrapper

        separate = typed_lint_wrapper.compilation(self.arguments)
        joined = typed_lint_wrapper.compilation(["/rustc", "--crate-name=example", "src/lib.rs", "--crate-type=lib,rlib", "-Cextra-filename=-abcd", "--crate-type", "proc-macro"])
        self.assertEqual(separate, {"crate_name": "example", "crate_types": ["lib"], "extra_filename": "-abcd", "source": "src/lib.rs"})
        self.assertEqual(joined, {"crate_name": "example", "crate_types": ["lib", "rlib", "proc-macro"], "extra_filename": "-abcd", "source": "src/lib.rs"})
        self.assertEqual(typed_lint_wrapper.compilation(["/rustc", "--crate-name"])["crate_name"], None)

    def test_a_record_needs_a_report_directory_and_a_crate(self) -> None:
        from scripts import typed_lint_wrapper

        record = {**typed_lint_wrapper.compilation(["/rustc", "-vV"]), "ending": {"kind": "exit", "status": 1}}
        self.assertEqual((record["crate_name"], record["crate_types"], record["extra_filename"], record["source"]), (None, [], "", None))
        with mock.patch.dict(os.environ, {"NERVIX_LINT_REPORTS": str(self.reports)}):
            self.assertIsNone(typed_lint_wrapper.publish(record))
        with mock.patch.dict(os.environ, {"NERVIX_LINT_REPORTS": ""}):
            self.assertIsNone(typed_lint_wrapper.publish({**record, **typed_lint_wrapper.compilation(self.arguments)}))
        self.assertEqual(list(self.reports.iterdir()), [])

    def test_a_driver_leaves_its_exit_status_whether_it_failed_or_succeeded(self) -> None:
        completed = self.wrap(self.driver("print('Error: cannot publish the required compiler report', file=sys.stderr)\nsys.exit(1)"))
        self.assertEqual(completed.returncode, 1)
        self.assertEqual(self.record()["ending"], {"kind": "exit", "status": 1})
        self.assertIn("cannot publish the required compiler report", completed.stderr)
        completed = self.wrap(self.driver("print('{\"artifact\": \"example\"}', file=sys.stderr)"))
        self.assertEqual((completed.returncode, completed.stderr), (0, '{"artifact": "example"}\n'))
        self.assertEqual(self.record()["ending"], {"kind": "exit", "status": 0})
        self.assertEqual([path.name for path in (self.reports / "compilations").iterdir()], ["example-abcd.json"])

    def test_a_declared_compilation_holds_its_admitted_place_while_its_driver_runs(self) -> None:
        observed = self.root / "observed.json"
        queue = self.root / "queue"
        body = (
            "import json, pathlib, time\n"
            "deadline = time.monotonic() + 60\n"
            "while True:\n"
            f"    leases = [json.loads(path.read_text()) for path in pathlib.Path({str(queue / 'leases')!r}).glob('*.json')]\n"
            "    if [lease['driver_pid'] for lease in leases] == [os.getpid()] or time.monotonic() > deadline:\n"
            "        break\n"
            "    time.sleep(0.01)\n"
            f"pathlib.Path({str(observed)!r}).write_text(json.dumps(leases))\n"
        )
        policy = Policy(queue, 0, 60.0, {Compiled("example", "lib"): 2**20})
        completed = self.wrap(self.driver(body), policy=policy)
        self.assertEqual((completed.returncode, completed.stderr), (0, ""))
        leases = json.loads(observed.read_text())
        self.assertEqual(len(leases), 1)
        self.assertIsNotNone(leases[0]["admitted_ns"])
        self.assertEqual((leases[0]["crate_name"], leases[0]["kind"], leases[0]["configuration"], leases[0]["root"], leases[0]["reservation_bytes"]), ("example", "lib", "testing", str(self.root), 2**20))
        self.assertEqual(list((queue / "leases").iterdir()), [])
        admission = self.record()["admission"]
        self.assertEqual((admission["outcome"], admission["reservation_bytes"], admission["holders"], admission["error"]), ("fit", 2**20, 0, None))

    def test_main_holds_a_declared_compilations_place_until_its_driver_ends(self) -> None:
        queue = self.root / "queue"
        policy = Policy(queue, 0, 60.0, {Compiled("example", "lib"): 2**20})
        leases = queue / "leases"
        driver = self.driver(f"import json, pathlib\nleases = [json.loads(path.read_text()) for path in pathlib.Path({str(leases)!r}).glob('*.json')]\nsys.exit(0 if len(leases) == 1 and leases[0]['admitted_ns'] else 4)")
        output = io.StringIO()
        environment = {**self.environment(driver, policy), "NERVIX_NATIVE_COVERAGE_ATTEMPT": ""}
        with mock.patch.dict(os.environ, environment), contextlib.redirect_stderr(output):
            from scripts import typed_lint_wrapper

            handlers = {number: signal.getsignal(number) for number in typed_lint_wrapper.FORWARDED}
            for number, handler in handlers.items():
                self.addCleanup(signal.signal, number, handler)
            status = typed_lint_wrapper.main(self.arguments)
        self.assertEqual((status, output.getvalue()), (0, ""))
        self.assertEqual(list(leases.iterdir()), [])
        self.assertEqual(self.record()["admission"]["outcome"], "fit")

    def test_an_undeclared_compilation_takes_no_place(self) -> None:
        policy = Policy(self.root / "queue", 0, 60.0, {Compiled("example", "bin"): 2**20, Compiled("another", "lib"): 2**20})
        completed = self.wrap(self.driver("pass"), policy=policy)
        self.assertEqual(completed.returncode, 0)
        self.assertIsNone(self.record()["admission"])
        self.assertFalse((self.root / "queue").exists())

    def test_a_driver_that_cannot_start_releases_its_place(self) -> None:
        queue = self.root / "queue"
        policy = Policy(queue, 0, 60.0, {Compiled("example", "lib"): 2**20})
        completed = self.wrap(self.root / "missing-driver", policy=policy)
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("FileNotFoundError", completed.stderr)
        self.assertEqual(list((queue / "leases").iterdir()), [])


class CacheQualificationTests(unittest.TestCase):
    def test_second_worktree_can_already_contain_the_committed_tooling(self) -> None:
        from scripts.tests import qualify_typed_ratchet_cache as qualification

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            (root / TOOLING).mkdir(parents=True)
            (root / TOOLING / "Cargo.toml").write_text("current workspace")
            (root / TOOLING / "target").mkdir()
            (root / TOOLING / "target/cache.json").write_text("build artifact")
            for name in ("justfile", "Cargo.toml", "scripts/typed_ratchet.py", "scripts/typed_lint_wrapper.py", "scripts/ratchet.py"):
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            binaries = root / "target/typed-ratchet/driver/debug"
            binaries.mkdir(parents=True)
            for name in ("nervix-lint-driver", "nervix-lint-report"):
                (binaries / name).touch()

            def command(arguments, **kwargs):
                if arguments[:3] == ["git", "worktree", "add"]:
                    (pathlib.Path(arguments[4]) / "scripts").mkdir(parents=True)
                    destination = pathlib.Path(arguments[4]) / TOOLING
                    destination.mkdir(parents=True)
                    (destination / "Cargo.toml").write_text("committed workspace")
                if arguments[:2] == ["git", "diff"]:
                    return str(TOOLING / "Cargo.toml") + "\0"
                if arguments[:2] == ["git", "ls-files"]:
                    return ""

            def qualify(worktree, target):
                if worktree != root:
                    self.assertEqual((worktree / TOOLING / "Cargo.toml").read_text(), "current workspace")
                    self.assertFalse((worktree / TOOLING / "target").exists())
                    raise RuntimeError("workspace prepared")
                return {}

            with mock.patch.object(qualification, "ROOT", root), mock.patch.object(qualification, "command", side_effect=command), mock.patch.object(qualification, "qualify", side_effect=qualify), mock.patch.dict(os.environ, {"CARGO_TARGET_DIR": str(root / "target")}):
                with self.assertRaisesRegex(RuntimeError, "workspace prepared"):
                    qualification.main()
