"""Resource probes retain truthful observations, stay bounded and clean up their own files."""
from __future__ import annotations

import json
import contextlib
import io
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

from scripts import runner_diagnostics as diagnostics
from scripts.tests.test_kache_ci import KACHE_JOBS, job_steps, workflow_jobs


class MetadataTests(unittest.TestCase):
    def test_missing_vm_frequency_controls_are_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(diagnostics.frequency_policy(Path(directory)), {
                "policies_khz": None, "boost": None,
                "intel_no_turbo": None, "intel_pstate_status": None,
            })

    def test_cgroup_record_keeps_the_restricting_parent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            child = root / "build/step"
            child.mkdir(parents=True)
            (root / "build/cpu.max").write_text("200000 100000\n")
            (child / "cpu.max").write_text("max 100000\n")
            membership = root / "membership"
            membership.write_text("0::/build/step\n")
            levels = diagnostics.cgroup_limits(root, membership)["visible_ancestors"]
            self.assertEqual(levels[str(child)]["cpu.max"], "max 100000")
            self.assertEqual(levels[str(root / "build")]["cpu.max"], "200000 100000")

    def test_a_missing_hardware_command_is_reported(self) -> None:
        record = diagnostics.command_record(["/does/not/exist"])
        self.assertIn("unavailable", record)
        self.assertNotIn("returncode", record)

    def test_container_cgroup_mount_keeps_its_visible_root_limits(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "cpu.max").write_text("100000 100000")
            membership = root / "membership"
            membership.write_text("0::/hidden/container\n")
            result = diagnostics.cgroup_limits(root, membership)
            self.assertEqual(result["visible_ancestors"][str(root)]["cpu.max"], "100000 100000")


class CalibrationTests(unittest.TestCase):
    def test_real_tools_retain_rates_identity_and_remove_the_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "reports"
            summary = root / "summary.md"
            with mock.patch("sys.argv", ["runner_diagnostics", "--label", "build", "--output",
                                         str(output), "--storage-path", str(root)]), \
                 mock.patch.dict(os.environ, {"GITHUB_STEP_SUMMARY": str(summary)}), \
                 contextlib.redirect_stdout(io.StringIO()):
                diagnostics.main()
            record = json.loads((output / "build.json").read_text())
            result = record["calibration"]
            self.assertIn("sysbench", result["tools"]["sysbench"]["stdout"])
            for name in ("cpu", "memory-sequential", "memory-random"):
                with self.subTest(name=name):
                    probe = result["measurements"][name]
                    self.assertEqual(probe.get("returncode"), 0, probe)
                    self.assertGreater(probe["wall_seconds"], 0)
                    self.assertGreater(probe["cpu_seconds"], 0)
                    self.assertIn("total number of events:", probe["stdout"])
            disk = result["measurements"]["disk"]
            self.assertEqual(disk.get("returncode"), 0, disk)
            jobs = json.loads(disk["stdout"])["jobs"]
            self.assertEqual([job["error"] for job in jobs], [0, 0, 0])
            self.assertEqual(jobs[0]["write"]["io_bytes"], 64 * 2**20)
            self.assertEqual(jobs[1]["read"]["io_bytes"], 64 * 2**20)
            self.assertEqual(jobs[2]["read"]["io_bytes"], 128 * 4096)
            self.assertEqual(summary.read_text(), (output / "build.md").read_text())
            self.assertIn("MiB/sec", summary.read_text())
            self.assertEqual(sorted(path.name for path in root.iterdir()), ["reports", "summary.md"])

    def test_tool_timeout_keeps_completed_output_and_reaps_its_process(self) -> None:
        import sys
        popen = subprocess.Popen

        def ready_process(*args, **kwargs):
            process = popen(*args, **kwargs)
            # Establish completed output before testing its retention across a timeout. Process
            # startup need not finish inside the probe's short stalled-tool budget on a busy host.
            ready = process.stderr.readline()
            if ready != "ready\n":
                process.kill()
                process.communicate()
                self.fail(f"the fixture did not publish its ready signal: {ready!r}")
            return process

        with mock.patch.object(diagnostics.subprocess, "Popen", side_effect=ready_process):
            probe = diagnostics.run_probe([sys.executable, "-c",
                "import sys, time; print('measurement before stall', flush=True); "
                "print('ready', file=sys.stderr, flush=True); time.sleep(30)"], 0.2)
        self.assertTrue(probe["timed_out"])
        self.assertLess(probe["returncode"], 0)
        self.assertIn("measurement before stall", probe["stdout"])

    def test_missing_tool_is_unavailable_instead_of_zero_throughput(self) -> None:
        result = diagnostics.run_probe(["/does/not/exist"], 1)
        self.assertIn("unavailable", result)
        self.assertNotIn("stdout", result)

    def test_failed_tool_keeps_its_status_and_raw_output(self) -> None:
        result = diagnostics.run_probe(["/bin/false"], 1)
        self.assertEqual(result["returncode"], 1)

    def test_exhausted_budget_starts_no_tool_and_cleans_up(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(diagnostics, "CALIBRATION_SECONDS", 0), \
                 mock.patch.object(diagnostics, "run_probe") as run:
                result = diagnostics.calibration(Path(directory))
            run.assert_not_called()
            self.assertTrue(all("unavailable" in item for item in result["measurements"].values()))
            self.assertEqual(list(Path(directory).iterdir()), [])


class WorkflowTests(unittest.TestCase):
    def test_every_rust_build_job_records_and_retains_diagnostics(self) -> None:
        for filename, names in KACHE_JOBS.items():
            jobs = workflow_jobs((Path(".github/workflows") / filename).read_text())
            if filename == "docker-build.yaml":
                names = names | {"build-arch", "build-diagnostic"}
            for name in names:
                with self.subTest(workflow=filename, job=name):
                    steps = job_steps(jobs[name])
                    probes = [step for step in steps if step.field("uses") ==
                              "./.github/actions/runner-diagnostics"]
                    self.assertTrue(probes)
                    labels = [step.mapping("with")["label"] for step in probes]
                    self.assertEqual(len(labels), len(set(labels)))
                    for step in probes:
                        following = steps[steps.index(step) + 1]
                        self.assertIsNotNone(following.field("run"))
        action = Path(".github/actions/runner-diagnostics/action.yml").read_text()
        self.assertIn("./.github/actions/ci-artifact-upload", action)
        self.assertIn('--storage-path "${GITHUB_WORKSPACE}"', action)
        builder = Path("Dockerfile.debian").read_text()
        self.assertIn("--output /tmp/runner-diagnostics --storage-path /app/target", builder)
        self.assertIn("sysbench", builder)


if __name__ == "__main__":
    unittest.main()
