"""Command/artifact scenarios for live fuzz coverage and incomplete campaign evidence."""

from __future__ import annotations

import dataclasses
import json
import os
import pathlib
import re
import shutil
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
            shutil.copytree(target.corpus, paths["corpus"])
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

    def campaign(self, selected=None, partition=None) -> coverage.Campaign:
        return coverage.Campaign(self.root, self.inventory, self.inventory.targets if selected is None else selected, 30, partition)

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
            coverage.Campaign(self.root, self.inventory, (target,), 0, None)

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

    def test_each_test_target_kind_reads_the_fingerprint_cargo_names_it_by(self) -> None:
        self.assertEqual(coverage.fingerprint_prefix("lib"), "test-lib-")
        self.assertEqual(coverage.fingerprint_prefix("bin:nervix-web-console"), "test-bin-")
        self.assertEqual(coverage.fingerprint_prefix("test:representations"), "test-integration-test-")

    def test_binary_unit_tests_read_their_own_build_fingerprint(self) -> None:
        target = dataclasses.replace(self.inventory.targets[0], test_target="bin:nervix-web-console")
        self.inventory = dataclasses.replace(self.inventory, targets=(target,))
        # The library's fingerprint beside the binary's executable is not the binary's own.
        with self.assertRaisesRegex(native.RunnerError, "fingerprint"), self.campaign() as campaign:
            with campaign.target(target, self.root) as measured:
                measured.execute(self.binary, [])
        own = self.fingerprint.with_name("test-bin-nervix-web-console.json")
        self.fingerprint.rename(own)
        with self.campaign() as campaign:
            with campaign.target(target, self.root) as measured:
                self.execute(measured)
                measured.complete("#10 DONE", 1)
            campaign.complete()
        build = self.record(measured.path)["build"]
        self.assertEqual(build["fingerprint"], str(own.relative_to(self.root)))
        self.assertEqual(self.record(campaign.path)["verdict"], "complete")

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

    def shard_campaigns(self, plan) -> pathlib.Path:
        """Run each shard of `plan` and copy its evidence the way CI downloads shard artifacts."""
        evidence = self.root / "evidence"
        for number, selected in enumerate(plan, start=1):
            partition = {"number": number, "count": len(plan), "plan_sha256": bolero.plan_digest(plan)}
            with self.campaign(selected, partition) as campaign:
                for target in selected:
                    with campaign.target(target, self.root / "target/bolero/runs" / target.id) as measured:
                        self.execute(measured)
                        measured.complete("#120 DONE", 30)
                campaign.complete()
            self.assertTrue(campaign.path.name.endswith(f"-shard-{number}-of-{len(plan)}"))
            self.assertEqual(self.record(campaign.path)["shard"], partition)
            shutil.copytree(campaign.path, evidence / f"shard-{number}" / campaign.path.name,
                            ignore=shutil.ignore_patterns("profiles"))
        return evidence

    def combine(self, evidence: pathlib.Path, plan, output: str = "combined") -> pathlib.Path:
        return coverage.combine(self.root, self.inventory, evidence, self.root / output,
                                lambda count: plan, bolero.plan_digest)

    def test_shards_of_one_plan_combine_into_one_complete_record_and_report(self) -> None:
        plan = tuple((target,) for target in self.inventory.targets)
        evidence = self.shard_campaigns(plan)
        output = self.combine(evidence, plan)
        record = self.record(output)
        self.assertEqual(record["verdict"], "complete")
        self.assertEqual(record["counts"], {"discovered": 2, "selected": 2, "executed": 2, "completed": 2})
        self.assertEqual(record["selection"], [target.id for target in self.inventory.targets])
        self.assertEqual([shard["number"] for shard in record["shards"]], [1, 2])
        self.assertEqual(record["plan_sha256"], bolero.plan_digest(plan))
        self.assertEqual(record["tested_sha"], "revision")
        for number, (target,) in enumerate(plan, start=1):
            self.assertEqual(record["targets"][target.id]["shard"], number)
            self.assertTrue((evidence / record["targets"][target.id]["completion"]).is_file())
        shard_report = (evidence / pathlib.Path(record["shards"][0]["record"]).parent / "lcov.info").read_text()
        merged = (output / "lcov.info").read_text()
        self.assertIn("DA:3,4", shard_report)
        self.assertIn("DA:3,8", merged)
        target = self.inventory.targets[0]
        completion = json.loads((evidence / record["targets"][target.id]["completion"]).read_text())
        self.assertEqual(completion["corpus"], coverage.corpus_identity(target.corpus))
        self.assertGreater(completion["corpus"]["inputs"], 0)

    def test_missing_failed_foreign_or_mismatched_shards_do_not_combine(self) -> None:
        plan = tuple((target,) for target in self.inventory.targets)
        evidence = self.shard_campaigns(plan)
        records = sorted(evidence.rglob("completion.json"))
        campaign_records = [path for path in records if "selection" in json.loads(path.read_text())]

        def edited(index: int, change) -> pathlib.Path:
            copy = self.root / f"edited-{index}-{len(list(self.root.glob('edited-*')))}"
            shutil.copytree(evidence, copy)
            path = copy / campaign_records[index].relative_to(evidence)
            content = json.loads(path.read_text())
            change(content)
            path.write_text(json.dumps(content))
            return copy

        def remove_second(content) -> None:
            raise AssertionError("unused")

        only_first = self.root / "only-first"
        shutil.copytree(evidence / "shard-1", only_first / "shard-1")
        cases = {
            "not shards 1 to 2": (only_first, plan),
            "did not complete": (edited(1, lambda content: content.update(verdict="failed")), plan),
            "another inventory": (edited(0, lambda content: content["inventory"].update(sha256="0" * 64)), plan),
            "different shard counts": (edited(0, lambda content: content["shard"].update(count=3)), plan),
            "selected": (edited(0, lambda content: content.update(selection=[])), plan),
            "counts": (edited(1, lambda content: content["counts"].update(completed=0)), plan),
            "another shard plan": (evidence, tuple(reversed(plan))),
            "different runs": (edited(1, lambda content: content.update(run={"provider": "github", "run": "2", "attempt": "1"})), plan),
            "tested": (edited(1, lambda content: content.update(tested_sha="other")), plan),
            "without a shard plan": (edited(0, lambda content: content.update(shard=None)), plan),
        }
        for index, (reason, (directory, shards)) in enumerate(cases.items()):
            with self.subTest(reason=reason):
                with self.assertRaisesRegex(native.RunnerError, reason):
                    self.combine(directory, shards, f"refused-{index}")
                self.assertEqual(self.record(self.root / f"refused-{index}")["verdict"], "failed")
        empty = self.root / "empty"
        empty.mkdir()
        with self.assertRaisesRegex(native.RunnerError, "no shard campaign record"):
            self.combine(empty, plan, "refused-empty")
        target_record = next(path for path in records if "selection" not in json.loads(path.read_text()))
        content = json.loads(target_record.read_text())
        content["verdict"] = "failed"
        target_record.write_text(json.dumps(content))
        with self.assertRaisesRegex(native.RunnerError, "no complete campaign record"):
            self.combine(evidence, plan, "refused-target")
        report = next(evidence.rglob("lcov.info"))
        report.unlink()
        with self.assertRaises(native.RunnerError):
            self.combine(evidence, plan, "refused-report")


class ReportMergeTests(unittest.TestCase):
    def merge(self, *reports: str) -> str:
        with tempfile.TemporaryDirectory() as directory:
            paths = []
            for index, text in enumerate(reports):
                path = pathlib.Path(directory) / f"{index}.info"
                path.write_text(text)
                paths.append(path)
            output = pathlib.Path(directory) / "merged.info"
            coverage.merge_lcov(paths, output)
            return output.read_text()

    def test_disjoint_executions_sum_their_counters_and_recount_summaries(self) -> None:
        first = ("TN:\nSF:src/a.rs\nFN:3,beta\nFN:1,alpha\nFNDA:2,alpha\nFNDA:0,beta\nFNF:2\nFNH:1\n"
                 "BRDA:4,0,0,-\nBRDA:4,0,1,1\nBRF:2\nBRH:1\nDA:1,2\nDA:3,0\nLF:2\nLH:1\nend_of_record\n")
        second = ("SF:src/a.rs\nFN:1,alpha\nFN:3,beta\nFNDA:1,alpha\nFNDA:5,beta\n"
                  "BRDA:4,0,0,3\nBRDA:4,0,1,-\nDA:1,1\nDA:3,5\nDA:7,0\nLF:3\nLH:2\nend_of_record\n"
                  "SF:src/b.rs\nDA:2,0\nend_of_record\n")
        self.assertEqual(self.merge(first, second), (
            "SF:src/a.rs\nFN:1,alpha\nFN:3,beta\nFNDA:3,alpha\nFNDA:5,beta\nFNF:2\nFNH:2\n"
            "BRDA:4,0,0,3\nBRDA:4,0,1,1\nBRF:2\nBRH:2\n"
            "DA:1,3\nDA:3,5\nDA:7,0\nLF:3\nLH:2\nend_of_record\n"
            "SF:src/b.rs\nFNF:0\nFNH:0\nDA:2,0\nLF:1\nLH:0\nend_of_record\n"
        ))
        self.assertIn("BRDA:4,0,0,-\n", self.merge("SF:x\nBRDA:4,0,0,-\nend_of_record\n"))

    def test_function_records_that_name_their_end_line_merge_by_name(self) -> None:
        first = ("SF:scripts/tool.py\nFN:3,4,Tool.run\nFN:7,9,Tool.stop\nFNDA:1,Tool.run\nFNDA:0,Tool.stop\n"
                 "FNF:2\nFNH:1\nDA:3,1\nDA:4,1\nDA:7,0\nLF:3\nLH:2\nend_of_record\n")
        second = "SF:scripts/tool.py\nFN:3,4,Tool.run\nFNDA:2,Tool.run\nDA:3,2\nDA:4,0\nend_of_record\n"
        self.assertEqual(self.merge(first, second), (
            "SF:scripts/tool.py\nFN:3,4,Tool.run\nFN:7,9,Tool.stop\nFNDA:3,Tool.run\nFNDA:0,Tool.stop\n"
            "FNF:2\nFNH:1\nDA:3,3\nDA:4,1\nDA:7,0\nLF:3\nLH:2\nend_of_record\n"
        ))

    def test_malformed_reports_are_refused(self) -> None:
        for text, reason in (
            ("DA:1,1\n", "outside a record"),
            ("SF:a\nDA:1,1\n", "no end_of_record"),
            ("SF:a\nSF:b\nend_of_record\n", "no end_of_record"),
            ("end_of_record\n", "outside a record"),
            ("SF:a\nXYZ:1\nend_of_record\n", "unsupported LCOV line"),
            ("SF:a\nFNDA:1,ghost\nend_of_record\n", "without a function line"),
        ):
            with self.subTest(text=text), self.assertRaisesRegex(native.RunnerError, reason):
                self.merge(text)
        with self.assertRaises(ValueError):
            self.merge("SF:a\nDA:x,1\nend_of_record\n")


class WorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workflow = (bolero.ROOT / ".github/workflows/bolero.yaml").read_text()
        section = self.workflow.split("      - name: Require the requested ordinary properties and fuzzing\n")[1]
        script = re.search(r"        run: \|\n((?:          .*\n|\n)*)", section)
        self.assertIsNotNone(script)
        self.script = textwrap.dedent(script[1])

    def test_documentation_only_validation_rerun_excludes_reusable_test_workflow(self) -> None:
        check = (bolero.ROOT / ".github/workflows/check.yaml").read_text()
        caller = check.split("\n  bolero:\n", 1)[1].split("\n  conventional-pr-title:", 1)[0]
        # The caller's selection survives a validation-only rerun. A cancelled
        # required producer must still fail the reusable workflow's own gate.
        self.assertIn("needs: [test-selection]", caller)
        self.assertIn("if: needs.test-selection.outputs.bolero-required == 'true'", caller)

    def test_complete_required_execution_and_deliberate_skips_retain_the_tested_snapshot(self) -> None:
        for event, labels, random_required, random, fuzz_required, fuzz in (
            ("pull_request", ["bolero-random", "fuzz"], "true", "success", "true", "success"),
            ("pull_request", ["fuzz"], "false", "skipped", "true", "success"),
            ("pull_request", ["bolero-random"], "true", "success", "false", "skipped"),
            ("pull_request", [], "false", "skipped", "false", "skipped"),
            ("workflow_dispatch", ["fuzz"], "true", "success", "false", "skipped"),
            ("workflow_dispatch", [], "true", "success", "false", "skipped"),
        ):
            with self.subTest(event=event, labels=labels), tempfile.TemporaryDirectory() as directory:
                root = pathlib.Path(directory)
                environment = {**os.environ, "BOLERO_TESTED_SHA": "tested-revision", "GITHUB_RUN_ID": "100", "GITHUB_RUN_ATTEMPT": "2",
                               "BOLERO_EVENT_NAME": event, "BOLERO_LABELS": json.dumps(labels), "RANDOM_RESULT": random,
                               "RANDOM_REQUIRED": random_required, "FUZZ_RESULT": fuzz, "COMBINED_RESULT": fuzz,
                               "FUZZ_REQUIRED": fuzz_required, "GITHUB_OUTPUT": str(root / "outputs")}
                executed = subprocess.run(["bash", "-e", "-c", self.script], cwd=root, env=environment, capture_output=True, text=True)
                self.assertEqual(executed.returncode, 0, executed.stderr)
                record = json.loads((root / "selection.json").read_text())
                self.assertEqual(record["tested_sha"], "tested-revision")
                self.assertEqual(record["event"], event)
                self.assertEqual(record["labels"], labels)
                self.assertEqual(record["randomized_required"], random_required == "true")
                self.assertEqual(record["randomized"], random)
                self.assertEqual(record["sanitizer_required"], fuzz_required == "true")
                self.assertEqual(record["sanitizer"], fuzz)
                self.assertEqual(record["sanitizer_combined"], fuzz)
                self.assertEqual(record["verdict"], "complete")
                outputs = (root / "outputs").read_text()
                self.assertIn(f"randomized-result={random}", outputs)
                self.assertIn(f"randomized-required={random_required}", outputs)
                self.assertIn(f"sanitizer-result={fuzz}", outputs)

    def test_failed_cancelled_or_missing_required_jobs_do_not_publish_complete_selection(self) -> None:
        for random_required, random, fuzz_required, fuzz, combined in (
            ("true", "failure", "true", "success", "success"), ("true", "cancelled", "false", "skipped", "skipped"),
            ("true", "skipped", "false", "skipped", "skipped"), ("true", "", "false", "skipped", "skipped"),
            ("false", "success", "false", "skipped", "skipped"), ("false", "failure", "false", "skipped", "skipped"),
            ("false", "cancelled", "false", "skipped", "skipped"), ("false", "", "false", "skipped", "skipped"),
            ("", "success", "false", "skipped", "skipped"),
            ("true", "success", "true", "failure", "skipped"), ("true", "success", "true", "cancelled", "skipped"),
            ("true", "success", "true", "skipped", "skipped"), ("true", "success", "false", "success", "success"),
            ("true", "success", "true", "success", "failure"), ("true", "success", "true", "success", "skipped"),
            ("true", "success", "false", "skipped", "success"),
            ("true", "success", "true", "", "success"), ("true", "success", "true", "success", ""),
            ("false", "skipped", "true", "failure", "skipped"), ("false", "skipped", "false", "success", "success"),
        ):
            with self.subTest(random_required=random_required, random=random, fuzz_required=fuzz_required, fuzz=fuzz,
                              combined=combined), tempfile.TemporaryDirectory() as directory:
                environment = {**os.environ, "RANDOM_RESULT": random, "RANDOM_REQUIRED": random_required, "FUZZ_RESULT": fuzz,
                               "COMBINED_RESULT": combined, "FUZZ_REQUIRED": fuzz_required}
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

    def job(self, name: str) -> str:
        sections = re.split(r"^  (?=[a-z][a-z-]*:\n)", self.workflow, flags=re.M)
        found = [section for section in sections if section.startswith(f"{name}:\n")]
        self.assertEqual(len(found), 1, name)
        return found[0]

    def test_a_pull_request_runs_the_randomized_job_only_when_bolero_random_requests_it(self) -> None:
        requested = ("(inputs.event-name || github.event_name) != 'pull_request' || "
                     "contains(fromJSON(inputs.labels-json || '[]'), 'bolero-random')")
        self.assertIn(f"    if: {requested}\n", self.job("bolero-random"))
        # The gate requires the job's success exactly where the job runs, and its deliberate skip elsewhere.
        gate = self.job("bolero-gate")
        self.assertIn(f"RANDOM_REQUIRED: ${{{{ {requested} }}}}\n", gate)
        self.assertIn("needs: [bolero-random, bolero-fuzz, bolero-fuzz-combined]", gate)

    def test_sanitizer_shards_run_the_plan_and_one_job_proves_the_whole_inventory_ran(self) -> None:
        label = "if: inputs.event-name == 'pull_request' && contains(fromJSON(inputs.labels-json || '[]'), 'fuzz')"
        shards = self.job("bolero-fuzz")
        self.assertIn(label, shards)
        self.assertIn("name: bolero-fuzz (${{ matrix.shard }} of ${{ strategy.job-total }})", shards)
        values = re.search(r"^\s+shard: \[(.+)\]$", shards, re.M)
        self.assertIsNotNone(values)
        count = len(values[1].split(","))
        self.assertEqual([int(value) for value in values[1].split(",")], list(range(1, count + 1)))
        self.assertIn("just validate-bolero", shards)
        self.assertIn("just prepare-bolero-shard ${{ matrix.shard }}/${{ strategy.job-total }}", shards)
        self.assertIn("just coverage-bolero 30 ${{ matrix.shard }}/${{ strategy.job-total }}", shards)
        # Only the server preparation is held to one Cargo build job.
        self.assertEqual(shards.count('CARGO_BUILD_JOBS: "1"'), 1)
        preparation = shards.split("- name: Prepare this shard's declared server harnesses")[1].split("- name:")[0]
        self.assertIn('CARGO_BUILD_JOBS: "1"', preparation)
        self.assertIn("python3 scripts/record_runner_load.py", shards)
        evidence = shards.split("name: bolero-fuzz-evidence-${{ matrix.shard }}")[1].split("- name:")[0]
        for excluded in ("**/profiles/**", "**/*.profdata", "**/targets/*/lcov.info"):
            self.assertIn(f"!target/native-coverage/bolero-fuzz/{excluded}", evidence)
        self.assertEqual(len(bolero.shard_plan(bolero.load_inventory(), count)), count)
        combined = self.job("bolero-fuzz-combined")
        self.assertIn(label, combined)
        self.assertIn("needs: [bolero-fuzz]", combined)
        # The combiner invokes repository recipes before any Rust build is required.
        self.assertIn("tool: just@latest", combined)
        self.assertIn("pattern: bolero-fuzz-evidence-*", combined)
        self.assertIn("just combine-bolero-fuzz target/bolero-fuzz-shards target/native-coverage/bolero-fuzz/combined", combined)
        self.assertIn("name: coverage-bolero-fuzz\n", combined)
        gate = self.job("bolero-gate")
        self.assertIn("needs: [bolero-random, bolero-fuzz, bolero-fuzz-combined]", gate)
        self.assertIn("COMBINED_RESULT: ${{ needs.bolero-fuzz-combined.result }}", gate)


if __name__ == "__main__":
    unittest.main()
