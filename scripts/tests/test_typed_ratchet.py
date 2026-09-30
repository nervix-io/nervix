from __future__ import annotations

import json
import copy
import dataclasses
import contextlib
import io
import os
import pathlib
import runpy
import subprocess
import tempfile
import unittest
from unittest import mock

from scripts import typed_ratchet
from scripts.typed_ratchet import AnalysisError, Configuration, ROOT, Runner, TOOLING, atomic_json, digest, policy


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
        for name, text in {"src/lib.rs": "pub fn owner() {}", "Cargo.toml": "[workspace]", "Cargo.lock": "lock", str(TOOLING / "catalog.json"): "catalog", str(TOOLING / "configurations.toml"): "configuration", str(TOOLING / "scopes.json"): "review", "scripts/typed_lint_wrapper.py": "wrapper", "target/typed-ratchet/driver/debug/nervix-lint-driver": "driver", "target/typed-ratchet/driver/debug/nervix-lint-report": "validator"}.items():
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

    def test_compiler_driver_catalog_configuration_source_dependency_and_wrapper_invalidate(self) -> None:
        original = Runner(self.root, self.target)
        for name in ("src/lib.rs", "Cargo.toml", "Cargo.lock", str(TOOLING / "catalog.json"), str(TOOLING / "configurations.toml"), "scripts/typed_lint_wrapper.py", "target/typed-ratchet/driver/debug/nervix-lint-driver", "target/typed-ratchet/driver/debug/nervix-lint-report"):
            with self.subTest(name=name):
                path = self.root / name
                initial = path.read_bytes()
                path.write_bytes(initial + b" changed")
                self.assertNotEqual(Runner(self.root, self.target).identity, original.identity)
                path.write_bytes(initial)
        with mock.patch("scripts.typed_ratchet.compiler_identity", return_value="another compiler"):
            self.assertNotEqual(Runner(self.root, self.target).identity, original.identity)
        (self.root / TOOLING / "scopes.json").write_text("new review")
        self.assertEqual(Runner(self.root, self.target).identity, original.identity)

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
    def test_arguments_keep_modes_targets_and_selected_packages_explicit(self) -> None:
        self.assertEqual(Configuration("selected", "Cargo.toml", ("one", "two"), ("lib", "bin"), ("native", "shuttle"), "native-target").cargo_arguments(), ["--manifest-path", "Cargo.toml", "--package", "one", "--package", "two", "--lib", "--bins", "--features", "native shuttle", "--target", "native-target"])
        self.assertIn("--workspace", Configuration("ordinary", "Cargo.toml", ()).cargo_arguments())

    def test_declared_modes_and_malformed_configuration(self) -> None:
        configurations = typed_ratchet.load_configurations(ROOT)
        self.assertEqual({item.name for item in configurations}, {"ordinary", "testing", "shuttle", "loom", "turmoil"})
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            path = root / TOOLING / "configurations.toml"
            path.parent.mkdir(parents=True)
            valid = '[[configuration]]\nname="ordinary"\nmanifest="Cargo.toml"\npackages=[]\n'
            for text, error in ((valid + valid, "duplicate"), (valid + 'features=["loom", "shuttle"]\n', "incompatible"), (valid + 'kinds=["test"]\n', "unsupported"), (valid + 'unknown=true\n', "unknown"), ('[[configuration]]\nname="absent"\n', "missing"), ('configuration=[]\n', "no declared")):
                path.write_text(text)
                with self.assertRaisesRegex(AnalysisError, error):
                    typed_ratchet.load_configurations(root)


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
        with mock.patch("scripts.typed_ratchet.subprocess.Popen", return_value=mock.Mock(wait=mock.Mock(return_value=1))):
            with self.assertRaisesRegex(AnalysisError, "did not finish"):
                Runner.cargo(self.runner, self.configuration, self.root / "build", self.reports, "namespace")

    def test_clean_only_this_analysis_build(self) -> None:
        with mock.patch("scripts.typed_ratchet.command") as command:
            Runner.clean_authored_artifacts(self.runner, self.configuration, self.expected, self.root / "isolated")
        self.assertIn(str(self.root / "isolated"), command.call_args.args[0])
        self.assertIn("example", command.call_args.args[0])
        self.assertEqual(command.call_args.kwargs["env"]["RUSTC_WRAPPER"], "kache")


class MainTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = pathlib.Path(self.temporary.name)
        (self.root / TOOLING).mkdir(parents=True)
        (self.root / TOOLING / "scopes.json").write_text("[]")
        (self.root / TOOLING / "review-context.json").write_text('{"inputs":{}}')
        self.runner = mock.Mock(root=self.root, target=self.root / "target", work=self.root / "evidence", compiler="qualified compiler", identity="qualified identity")
        self.runner.analyze.side_effect = lambda configuration, **kwargs: {"configuration": json.loads(typed_ratchet.encode(dataclasses.asdict(configuration))), "reports": {}, "complete": True}
        self.configurations = [Configuration("ordinary", "Cargo.toml", ())]
        for name, value in (("Runner", self.runner), ("load_configurations", self.configurations), ("policy", [])):
            patch = mock.patch("scripts.typed_ratchet." + name, return_value=value)
            patch.start()
            self.addCleanup(patch.stop)

    def run_main(self, *args: str) -> tuple[int, str]:
        output = io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
            status = typed_ratchet.main(["--root", str(self.root), *args])
        return status, output.getvalue()

    def test_inventory_and_policy_have_explicit_different_evidence(self) -> None:
        status, _ = self.run_main("--inventory")
        self.assertEqual(status, 0)
        path = self.runner.work / "inventory.json"
        inventory = json.loads(path.read_text())
        self.assertFalse(inventory["policy_checked"])
        status, _ = self.run_main()
        self.assertEqual(status, 0)
        current = json.loads(path.read_text())
        self.assertTrue(current["policy_checked"])
        self.assertEqual(current["debt"], 0)
        self.assertEqual(current["policy_sha256"], digest(self.root / TOOLING / "scopes.json"))
        self.assertEqual(current["review_context_sha256"], digest(self.root / TOOLING / "review-context.json"))

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
            typed_ratchet.Runner.assert_called_with(self.root, self.root / "selected-target")
        self.runner.clean_authored_artifacts.assert_called_once()
        self.assertTrue(self.runner.analyze.call_args.kwargs["fresh"])
        self.assertEqual(self.runner.analyze.call_args.args[0].name, "fixture-ordinary")

    def test_turmoil_uses_a_separate_just_invocation_and_rejects_wrong_evidence(self) -> None:
        self.configurations[:] = [Configuration("turmoil", "Cargo.toml", (), features=("turmoil",))]
        separate = {"complete": True, "compiler": self.runner.compiler, "root": str(self.root), "evidence": [{"configuration": {"name": "turmoil"}, "reports": {}}]}
        def nested(args, **kwargs):
            self.assertEqual(args[:2], ["just", "typed-ratchet-turmoil"])
            atomic_json(self.runner.work / "turmoil.json", separate)
            return mock.Mock(returncode=0)
        with mock.patch.dict(os.environ, {"RUSTFLAGS": ""}), mock.patch("scripts.typed_ratchet.subprocess.run", side_effect=nested):
            self.assertEqual(self.run_main("--inventory", "--fresh")[0], 0)
            for field, value in (("complete", False), ("compiler", "wrong compiler"), ("root", "wrong worktree")):
                previous = separate[field]
                separate[field] = value
                self.assertEqual(self.run_main("--inventory")[0], 1)
                separate[field] = previous
            separate["evidence"][0]["configuration"]["name"] = "shuttle"
            self.assertEqual(self.run_main("--inventory")[0], 1)
        with mock.patch.dict(os.environ, {"RUSTFLAGS": ""}), mock.patch("scripts.typed_ratchet.subprocess.run", return_value=mock.Mock(returncode=1)):
            self.assertEqual(self.run_main("--fixture-mode", "turmoil", "--inventory", "--recompile")[0], 1)

    def test_human_review_and_expansion_origins_are_present(self) -> None:
        finding = {"receiver_type": "&DashMap<u32, u32>", "operation": "get", "acquisition": "shared", "span": {"line": 3, "column": 2}, "expansion": [{"macro_name": "external::forward", "call_site": "src/lib.rs:2"}]}
        site = {"acquisition": {"site": {"path": "src/lib.rs", "start": 10}, "configurations": {"ordinary:example": [finding]}}, "scope": {"owners": ["example::batch"], "frequency": "per batch", "rationale": "Retain its published map handle.", "disposition": {"class": "debt", "delivery": "owner repair"}}}
        with mock.patch("scripts.typed_ratchet.policy", return_value=[site]):
            status, output = self.run_main("--show")
        self.assertEqual(status, 0)
        for text in ("src/lib.rs:3:3", "DashMap", "shared", "per batch", "Retain its published map handle", "ordinary:example", "external::forward"):
            self.assertIn(text, output)
        site["scope"]["disposition"] = {"class": "bounded_protocol", "key": "branch identity", "bound": "one admitted batch"}
        rendered = typed_ratchet.render_finding(site)
        self.assertIn("key branch identity", rendered)
        self.assertIn("bound one admitted batch", rendered)


class WorkspaceWrapperTests(unittest.TestCase):
    def test_wrapper_forwards_compiler_and_arguments_with_optional_coverage_recording(self) -> None:
        for coverage in ("", "/attempt"):
            with mock.patch.dict(os.environ, {"NERVIX_LINT_DRIVER": "/driver", "NERVIX_NATIVE_COVERAGE_ATTEMPT": coverage}), mock.patch("sys.argv", ["wrapper", "/rustc", "--crate-name", "example"]), mock.patch("os.execv", side_effect=RuntimeError("executed")) as execute:
                with self.assertRaisesRegex(RuntimeError, "executed"):
                    runpy.run_module("scripts.typed_lint_wrapper", run_name="__main__")
            arguments = execute.call_args.args[1]
            self.assertEqual(arguments[-4:], ["/driver", "/rustc", "--crate-name", "example"])
            self.assertEqual("exec" in arguments, bool(coverage))


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

            def qualify(worktree, target):
                if worktree != root:
                    self.assertEqual((worktree / TOOLING / "Cargo.toml").read_text(), "current workspace")
                    self.assertFalse((worktree / TOOLING / "target").exists())
                    raise RuntimeError("workspace prepared")
                return {}

            with mock.patch.object(qualification, "ROOT", root), mock.patch.object(qualification, "command", side_effect=command), mock.patch.object(qualification, "qualify", side_effect=qualify), mock.patch.dict(os.environ, {"CARGO_TARGET_DIR": str(root / "target")}):
                with self.assertRaisesRegex(RuntimeError, "workspace prepared"):
                    qualification.main()


class CallerReviewTests(unittest.TestCase):
    def test_changed_caller_invalidates_an_unchanged_helpers_review(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            tooling = root / TOOLING
            tooling.mkdir(parents=True)
            inputs = {"src/helper.rs": "unchanged acquisition", "src/caller.rs": "lifecycle caller"}
            (tooling / "review-context.json").write_text(json.dumps({"inputs": inputs}))
            (tooling / "scopes.json").write_text("[]")
            runner = mock.Mock(root=root, target=root, inputs=dict(inputs))
            success = mock.Mock(returncode=0, stdout=b"[]")
            with mock.patch("scripts.typed_ratchet.subprocess.run", return_value=success):
                self.assertEqual(policy(runner, [], inventory=False), [])
                runner.inputs["src/caller.rs"] = "recurring batch caller"
                with self.assertRaisesRegex(AnalysisError, "stale review context.*src/caller.rs"):
                    policy(runner, [], inventory=False)
                for context in ([], {}, {"inputs": []}, {"inputs": {"src/caller.rs": 0}}):
                    (tooling / "review-context.json").write_text(json.dumps(context))
                    with self.assertRaisesRegex(AnalysisError, "invalid review context"):
                        policy(runner, [], inventory=False)
