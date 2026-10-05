"""Command/artifact scenarios for live fuzz coverage and incomplete campaign evidence."""

from __future__ import annotations

import dataclasses
import json
import os
import pathlib
import re
import subprocess
import tempfile
import textwrap
import unittest
from unittest import mock

from scripts import bolero, bolero_coverage as coverage, native_coverage as native
from scripts.tests.test_native_coverage import BUILD_ID, GNU, RAW_REPORT, ExportCommands, elf, note, toolchain


class ProfileScopeTests(unittest.TestCase):
    def test_build_instruments_source_and_preserves_the_configured_wrapper_and_flags(self) -> None:
        with mock.patch.dict(os.environ, {"RUSTFLAGS": "-C target-cpu=native", "RUSTC_WRAPPER": "kache"}, clear=True):
            environment = coverage.build_environment()
            self.assertEqual(environment["RUSTFLAGS"], "-C target-cpu=native -C instrument-coverage")
            self.assertEqual(environment["LLVM_PROFILE_FILE"], "/dev/null")
            self.assertEqual(os.environ["RUSTC_WRAPPER"], "kache")
            self.assertNotIn("RUSTC_WRAPPER", environment)

    def test_encoded_flags_cannot_override_sanitizer_and_source_instrumentation(self) -> None:
        for flags in ("", "-C\x1finstrument-coverage"):
            with self.subTest(flags=flags), mock.patch.dict(os.environ, {"CARGO_ENCODED_RUSTFLAGS": flags}):
                with self.assertRaisesRegex(native.RunnerError, "override.*sanitizer"):
                    coverage.build_environment()

    def test_discovery_replay_and_qualification_counters_do_not_enter_the_live_campaign(self) -> None:
        for previous in (None, "/product/profiles/%p-%m.profraw"):
            with self.subTest(previous=previous), mock.patch.dict(os.environ, {}, clear=True):
                if previous is not None:
                    os.environ["LLVM_PROFILE_FILE"] = previous
                with self.assertRaisesRegex(RuntimeError, "failure"), coverage.suppressed_profiles():
                    self.assertEqual(os.environ["LLVM_PROFILE_FILE"], "/dev/null")
                    raise RuntimeError("failure")
                self.assertEqual(os.environ.get("LLVM_PROFILE_FILE"), previous)


class CampaignTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = pathlib.Path(directory.name)
        (self.root / "tests").mkdir()
        (self.root / "tests/bolero-targets.toml").write_bytes(bolero.INVENTORY.read_bytes())
        (self.root / "Cargo.toml").write_text('[profile.fuzz]\nopt-level = 2\ndebug-assertions = true\noverflow-checks = true\n')
        inventory = bolero.load_inventory()
        selected = [target for target in inventory.targets if target.id in {"backup-record-manifest", "backup-runtime-state-records"}]
        targets = []
        for target in selected:
            paths = {field: self.root / getattr(target, field).relative_to(bolero.ROOT) for field in ("source", "corpus", "manifest")}
            targets.append(dataclasses.replace(target, **paths))
        self.inventory = dataclasses.replace(inventory, targets=tuple(targets))
        self.tools = self.root / "tools"
        self.tools.mkdir()
        for name in ("llvm-profdata", "llvm-cov"):
            (self.tools / name).touch()
        self.commands = ExportCommands(self.root, BUILD_ID.hex(), RAW_REPORT.format(root=self.root))
        packages = native.Packages.from_metadata({"packages": [{"name": "fixture", "manifest_path": str(self.root / "Cargo.toml")}]})
        patches = [
            mock.patch.object(native, "Commands", return_value=self.commands),
            mock.patch.object(native, "load_toolchain", return_value=toolchain(self.tools)),
            mock.patch.object(native, "load_packages", return_value=packages),
            mock.patch.object(native, "load_revision", return_value=native.Revision("revision", False)),
            mock.patch.dict(os.environ, {"BOLERO_EVENT_NAME": "pull_request", "BOLERO_LABELS": '["fuzz"]', "BOLERO_TESTED_SHA": "revision"}, clear=True),
        ]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)
        self.binary = self.root / "target/fuzz/build_a/x86_64-unknown-linux-gnu/fuzz/build/nervix-backup/1234/out/nervix_backup-1234"
        self.binary.parent.mkdir(parents=True)
        self.binary.write_bytes(elf([(note(GNU, 3, BUILD_ID, 4, 0), 4)]))
        self.fingerprint = self.binary.parent.parent / "fingerprint/test-lib-nervix_backup.json"
        self.fingerprint.parent.mkdir(parents=True)
        self.flags = ["--cfg", "fuzzing", "-Zsanitizer=address", "-Cllvm-args=-sanitizer-coverage-level=4", "-C", "instrument-coverage"]
        self.fingerprint.write_text(json.dumps({"rustflags": self.flags, "features": "[]"}))

    def campaign(self, selected=None) -> coverage.Campaign:
        return coverage.Campaign(self.root, self.inventory, self.inventory.targets if selected is None else selected, 30)

    def execute(self, measured: coverage.TargetCoverage) -> None:
        environment = measured.execute(self.binary, ["-max_total_time=30"])
        self.assertEqual(environment["LLVM_PROFILE_FILE"], str(measured.path / "profiles/%p-%m.profraw"))
        (measured.path / "profiles/7-9_0.profraw").write_bytes(b"counters")

    def record(self, path: pathlib.Path) -> dict:
        return json.loads((path / "completion.json").read_text())

    def test_selected_live_campaigns_publish_normalized_per_target_and_aggregate_reports(self) -> None:
        with self.campaign() as campaign:
            for target in self.inventory.targets:
                with campaign.target(target, self.root / "target/bolero/runs" / target.id) as measured:
                    self.assertEqual(self.record(measured.path)["verdict"], "running")
                    self.execute(measured)
                    measured.complete("#120 DONE", 30)
                record = self.record(measured.path)
                self.assertEqual(record["verdict"], "complete")
                self.assertEqual(record["cost"]["inputs_per_second"], 4)
                self.assertEqual(record["build"]["rustflags"], self.flags)
                self.assertEqual(record["build"]["profile_settings"]["opt-level"], 2)
                self.assertIn("SF:src/main.rs", (measured.path / "lcov.info").read_text())
            campaign.complete()
        record = self.record(campaign.path)
        self.assertEqual(record["verdict"], "complete")
        self.assertEqual(record["counts"], {"discovered": 2, "selected": 2, "executed": 2, "completed": 2})
        self.assertEqual(record["selection"], [target.id for target in self.inventory.targets])
        self.assertEqual(record["event"], "pull_request")
        self.assertEqual(record["labels"], ["fuzz"])
        self.assertEqual(record["tested_sha"], "revision")
        self.assertEqual(len(record["profiles"]), 2)
        self.assertEqual(len(record["execution"]["executions"]), 2)
        self.assertTrue((campaign.path / "export.log").is_file())
        self.assertEqual(record["sources"]["covered"], 3)

    def test_empty_duplicate_unregistered_and_invalid_budget_selections_fail_before_execution(self) -> None:
        target = self.inventory.targets[0]
        for selected in ((), (target, target), (dataclasses.replace(target, domain_version=99),)):
            with self.subTest(selected=selected), self.assertRaises(native.RunnerError):
                self.campaign(selected)
        with self.assertRaisesRegex(native.RunnerError, "positive integer"):
            coverage.Campaign(self.root, self.inventory, (target,), 0)

    def test_isolated_campaign_records_its_own_workspace_profile(self) -> None:
        manifest = self.root / "tools/nervix-lint/Cargo.toml"
        manifest.parent.mkdir(parents=True)
        manifest.write_text('[profile.fuzz]\nopt-level = 1\ndebug = 2\n')
        target = dataclasses.replace(self.inventory.targets[0], manifest=manifest)
        self.inventory = dataclasses.replace(self.inventory, targets=(target,))
        with self.campaign() as campaign:
            with campaign.target(target, self.root) as measured:
                self.execute(measured)
                measured.complete("#10 DONE", 1)
            campaign.complete()
        build = self.record(measured.path)["build"]
        self.assertEqual(build["profile_settings"], {"opt-level": 1, "debug": 2})
        self.assertEqual(build["profile_manifest"], "tools/nervix-lint/Cargo.toml")

    def test_absent_or_repeated_targets_cannot_complete(self) -> None:
        with self.assertRaisesRegex(native.RunnerError, "missing"), self.campaign() as campaign:
            campaign.complete()
        self.assertEqual(self.record(campaign.path)["verdict"], "failed")
        with self.assertRaisesRegex(native.RunnerError, "unexpected"), self.campaign() as campaign:
            with campaign.target(dataclasses.replace(self.inventory.targets[0], domain_version=99), self.root):
                pass
        with self.assertRaisesRegex(native.RunnerError, "repeated"), self.campaign() as campaign:
            with campaign.target(self.inventory.targets[0], self.root) as measured:
                self.execute(measured)
                measured.complete("#1 DONE", 1)
            with campaign.target(self.inventory.targets[0], self.root):
                pass

    def test_engine_failure_preserves_profiles_and_failed_completion_without_export(self) -> None:
        for failure in (RuntimeError("crash"), KeyboardInterrupt()):
            with self.subTest(failure=failure), self.assertRaises(type(failure)), self.campaign() as campaign:
                with campaign.target(self.inventory.targets[0], self.root) as measured:
                    self.execute(measured)
                    raise failure
            expected = "interrupted" if isinstance(failure, KeyboardInterrupt) else "failed"
            self.assertEqual(self.record(campaign.path)["verdict"], expected)
            self.assertEqual(self.record(measured.path)["verdict"], expected)
            self.assertTrue((measured.path / "profiles/7-9_0.profraw").is_file())
            self.assertFalse((campaign.path / "lcov.info").exists())

    def test_incomplete_and_zero_input_engine_exits_fail(self) -> None:
        for output in ("no completion", "#0 DONE"):
            with self.subTest(output=output), self.assertRaisesRegex(native.RunnerError, "no completed"), self.campaign() as campaign:
                with campaign.target(self.inventory.targets[0], self.root) as measured:
                    self.execute(measured)
                    measured.complete(output, 1)
            self.assertEqual(self.record(measured.path)["verdict"], "failed")

    def test_source_sanitizer_feedback_and_exact_executable_fingerprint_are_required(self) -> None:
        for flags in ([], self.flags[:4], [flag for flag in self.flags if not flag.startswith("-Z")], ["-C", "instrument-coverage", "-Zsanitizer=address"]):
            self.fingerprint.write_text(json.dumps({"rustflags": flags}))
            with self.subTest(flags=flags), self.assertRaisesRegex(native.RunnerError, "lacks"), self.campaign() as campaign:
                with campaign.target(self.inventory.targets[0], self.root) as measured:
                    measured.execute(self.binary, [])
        self.fingerprint.unlink()
        with self.assertRaisesRegex(native.RunnerError, "fingerprint"), self.campaign() as campaign:
            with campaign.target(self.inventory.targets[0], self.root) as measured:
                measured.execute(self.binary, [])

    def test_missing_profiles_wrong_binary_and_mixed_toolchain_profiles_fail_export(self) -> None:
        cases = ("no profile", "wrong binary", "unreadable profile", "gone executable")
        for case in cases:
            with self.subTest(case=case), self.assertRaises(native.RunnerError), self.campaign() as campaign:
                with campaign.target(self.inventory.targets[0], self.root) as measured:
                    self.execute(measured)
                    profile = measured.path / "profiles/7-9_0.profraw"
                    if case == "no profile":
                        profile.unlink()
                    elif case == "wrong binary":
                        self.commands.identifier = bytes(32).hex()
                    elif case == "gone executable":
                        self.binary.unlink()
                    else:
                        with mock.patch.object(self.commands, "capture", return_value=native.Captured(1, "", "raw profile version mismatch")):
                            measured.complete("#10 DONE", 1)
                    measured.complete("#10 DONE", 1)
            self.commands.identifier = BUILD_ID.hex()
            self.assertEqual(self.record(campaign.path)["verdict"], "failed")

    def test_changed_inventory_missing_completion_and_count_mismatches_fail_aggregate(self) -> None:
        for defect in ("inventory", "completion", "counts"):
            with self.subTest(defect=defect), self.assertRaises(native.RunnerError), self.campaign() as campaign:
                for target in self.inventory.targets:
                    with campaign.target(target, self.root) as measured:
                        self.execute(measured)
                        measured.complete("#10 DONE", 1)
                if defect == "inventory":
                    campaign.inventory_file.write_text("changed")
                elif defect == "completion":
                    measured.record.content["verdict"] = "running"
                    measured.record.write()
                else:
                    campaign.record.content["counts"]["executed"] = 0
                campaign.complete()
            self.assertEqual(self.record(campaign.path)["verdict"], "failed")

    def test_preparation_failures_and_ending_before_export_remain_failed(self) -> None:
        campaign = self.campaign()
        with mock.patch.object(native, "load_toolchain", side_effect=native.RunnerError("missing tools")), self.assertRaisesRegex(native.RunnerError, "missing tools"):
            with campaign:
                pass
        self.assertEqual(self.record(campaign.path)["verdict"], "failed")
        with self.campaign() as campaign:
            pass
        self.assertEqual(self.record(campaign.path)["verdict"], "failed")
        with self.assertRaisesRegex(native.RunnerError, "did not finish"), self.campaign() as campaign:
            with campaign.target(self.inventory.targets[0], self.root):
                pass


class WorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workflow = (bolero.ROOT / ".github/workflows/bolero.yaml").read_text()
        section = self.workflow.split("      - name: Require ordinary properties and requested fuzzing\n")[1]
        script = re.search(r"        run: \|\n((?:          .*\n|\n)*)", section)
        self.assertIsNotNone(script)
        self.script = textwrap.dedent(script[1])

    def test_complete_required_execution_and_deliberate_skips_retain_the_tested_snapshot(self) -> None:
        for event, labels, required, result in (
            ("pull_request", ["fuzz"], "true", "success"),
            ("pull_request", [], "false", "skipped"),
            ("schedule", ["fuzz"], "false", "skipped"),
            ("workflow_dispatch", [], "false", "skipped"),
        ):
            with self.subTest(event=event, labels=labels), tempfile.TemporaryDirectory() as directory:
                root = pathlib.Path(directory)
                environment = {**os.environ, "BOLERO_TESTED_SHA": "tested-revision", "GITHUB_RUN_ID": "100", "GITHUB_RUN_ATTEMPT": "2",
                               "BOLERO_EVENT_NAME": event, "BOLERO_LABELS": json.dumps(labels), "RANDOM_RESULT": "success",
                               "FUZZ_RESULT": result, "FUZZ_REQUIRED": required, "GITHUB_OUTPUT": str(root / "outputs")}
                executed = subprocess.run(["bash", "-e", "-c", self.script], cwd=root, env=environment, capture_output=True, text=True)
                self.assertEqual(executed.returncode, 0, executed.stderr)
                record = json.loads((root / "selection.json").read_text())
                self.assertEqual(record["tested_sha"], "tested-revision")
                self.assertEqual(record["event"], event)
                self.assertEqual(record["labels"], labels)
                self.assertEqual(record["sanitizer_required"], required == "true")
                self.assertEqual(record["sanitizer"], result)
                self.assertEqual(record["verdict"], "complete")
                self.assertIn(f"sanitizer-result={result}", (root / "outputs").read_text())

    def test_failed_cancelled_or_missing_required_jobs_do_not_publish_complete_selection(self) -> None:
        for random, fuzz, required in (("failure", "success", "true"), ("cancelled", "skipped", "false"),
                                       ("success", "failure", "true"), ("success", "cancelled", "true"), ("success", "skipped", "true"),
                                       ("success", "success", "false")):
            with self.subTest(random=random, fuzz=fuzz, required=required), tempfile.TemporaryDirectory() as directory:
                environment = {**os.environ, "RANDOM_RESULT": random, "FUZZ_RESULT": fuzz, "FUZZ_REQUIRED": required}
                result = subprocess.run(["bash", "-e", "-c", self.script], cwd=directory, env=environment, capture_output=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse((pathlib.Path(directory) / "selection.json").exists())

    def test_check_forwards_sha_event_and_labels_to_one_reusable_bolero_call(self) -> None:
        check = (bolero.ROOT / ".github/workflows/check.yaml").read_text()
        self.assertEqual(check.count("uses: ./.github/workflows/bolero.yaml"), 1)
        self.assertIn("tested-sha: ${{ github.sha }}", check)
        self.assertIn("event-name: ${{ github.event_name }}", check)
        self.assertIn("labels-json: ${{ toJSON(github.event.pull_request.labels.*.name) }}", check)
        self.assertIn("if: inputs.event-name == 'pull_request' && contains(fromJSON(inputs.labels-json || '[]'), 'fuzz')", self.workflow)
        self.assertIn("name: coverage-bolero-fuzz", self.workflow)
        self.assertIn("just coverage-bolero 30", self.workflow)


if __name__ == "__main__":
    unittest.main()
