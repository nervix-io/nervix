from __future__ import annotations

from pathlib import Path
import re
import runpy
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from scripts import install_go_ci_tool


CHECKSUM_STREAM_ERROR = (
    "go: github.com/reteps/dockerfmt@v0.5.4: verifying module: "
    "reading https://sum.golang.org/tile/8/0/x220/225: "
    "stream error: stream ID 3; INTERNAL_ERROR; received from peer\n"
)
MODULE_ARCHIVE_STREAM_ERROR = (
    'golang.org/x/text@v0.34.0: read '
    '"https://proxy.golang.org/golang.org/x/text/@v/v0.34.0.zip": '
    "stream error: stream ID 97; INTERNAL_ERROR; received from peer\n"
)


class GoCiToolTests(unittest.TestCase):
    def test_module_archive_transport_recovers_with_verification_and_evidence(self) -> None:
        attempts = [
            subprocess.CompletedProcess(["go"], 1, "", MODULE_ARCHIVE_STREAM_ERROR),
            subprocess.CompletedProcess(["go"], 0, "installed\n", ""),
        ]
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(install_go_ci_tool.subprocess, "run", side_effect=attempts) as run:
                with mock.patch.object(install_go_ci_tool.time, "sleep") as sleep:
                    status = install_go_ci_tool.install("ghokin", Path(directory))

            self.assertEqual(status, 0)
            self.assertEqual(run.call_count, 2)
            self.assertEqual(sleep.call_args.args, (2,))
            for call in run.call_args_list:
                self.assertEqual(call.args[0], ["go", "install", "github.com/antham/ghokin/v3@v3.9.0"])
                self.assertEqual(call.kwargs["env"]["GOSUMDB"], "sum.golang.org")
                self.assertEqual(call.kwargs["env"]["GONOSUMDB"], "")
                self.assertEqual(call.kwargs["env"]["GOPRIVATE"], "")
            self.assertIn(MODULE_ARCHIVE_STREAM_ERROR.strip(), (Path(directory) / "ghokin/attempt-01.log").read_text())
            self.assertIn("Exit status: 0", (Path(directory) / "ghokin/attempt-02.log").read_text())

    def test_checksum_transport_recovers_with_failed_attempt_retained(self) -> None:
        attempts = [
            subprocess.CompletedProcess(["go"], 1, "", CHECKSUM_STREAM_ERROR),
            subprocess.CompletedProcess(["go"], 0, "installed\n", ""),
        ]
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(install_go_ci_tool.subprocess, "run", side_effect=attempts) as run:
                with mock.patch.object(install_go_ci_tool.time, "sleep") as sleep:
                    status = install_go_ci_tool.install("dockerfmt", Path(directory))

            self.assertEqual(status, 0)
            self.assertEqual(run.call_count, 2)
            self.assertEqual(sleep.call_args.args, (2,))
            self.assertEqual(run.call_args.args[0], ["go", "install", "github.com/reteps/dockerfmt@v0.5.4"])
            self.assertEqual(run.call_args.kwargs["env"]["GOSUMDB"], "sum.golang.org")
            self.assertEqual(run.call_args.kwargs["env"]["GOPRIVATE"], "")
            self.assertEqual(run.call_args.kwargs["env"]["GOTOOLCHAIN"], "local")
            first = (Path(directory) / "dockerfmt/attempt-01.log").read_text()
            second = (Path(directory) / "dockerfmt/attempt-02.log").read_text()
            self.assertIn(CHECKSUM_STREAM_ERROR.strip(), first)
            self.assertIn("Retryable public download transport error: True", first)
            self.assertIn("Exit status: 0", second)

    def test_compiler_and_verification_failures_do_not_retry(self) -> None:
        failures = (
            "# github.com/antham/ghokin/v3\n./main.go:10:2: undefined: parser\n",
            "go: verifying module: checksum mismatch\n",
            CHECKSUM_STREAM_ERROR + "go: verifying module: checksum mismatch\n",
            MODULE_ARCHIVE_STREAM_ERROR + "go: verifying module: checksum mismatch\n",
            MODULE_ARCHIVE_STREAM_ERROR + "SECURITY ERROR\n",
            MODULE_ARCHIVE_STREAM_ERROR.replace("proxy.golang.org/", "example.com/"),
            "go: github.com/antham/ghokin/v3@v3.9.0: invalid version\n",
        )
        for failure in failures:
            with self.subTest(failure=failure):
                with tempfile.TemporaryDirectory() as directory:
                    answer = subprocess.CompletedProcess(["go"], 1, "", failure)
                    with mock.patch.object(install_go_ci_tool.subprocess, "run", return_value=answer) as run:
                        with mock.patch.object(install_go_ci_tool.time, "sleep") as sleep:
                            status = install_go_ci_tool.install("ghokin", Path(directory))
                    self.assertEqual(status, 1)
                    run.assert_called_once()
                    sleep.assert_not_called()
                    self.assertEqual(len(list((Path(directory) / "ghokin").glob("attempt-*.log"))), 1)

    def test_persistent_transport_failure_stops_after_three_attempts(self) -> None:
        for failure in (CHECKSUM_STREAM_ERROR, MODULE_ARCHIVE_STREAM_ERROR):
            with self.subTest(failure=failure):
                answer = subprocess.CompletedProcess(["go"], 1, "", failure)
                with tempfile.TemporaryDirectory() as directory:
                    with mock.patch.object(install_go_ci_tool.subprocess, "run", return_value=answer) as run:
                        with mock.patch.object(install_go_ci_tool.time, "sleep") as sleep:
                            status = install_go_ci_tool.install("dockerfmt", Path(directory))
                    self.assertEqual(status, 1)
                    self.assertEqual(run.call_count, 3)
                    self.assertEqual([call.args[0] for call in sleep.call_args_list], [2, 5])
                    self.assertEqual(len(list((Path(directory) / "dockerfmt").glob("attempt-*.log"))), 3)

    def test_timeout_is_bounded_and_keeps_partial_output(self) -> None:
        error = subprocess.TimeoutExpired(["go", "install"], 300, output=b"partial download\n")
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(install_go_ci_tool.subprocess, "run", side_effect=error) as run:
                with mock.patch.object(install_go_ci_tool.time, "sleep") as sleep:
                    status = install_go_ci_tool.install("dockerfmt", Path(directory))
            self.assertEqual(status, 124)
            run.assert_called_once()
            sleep.assert_not_called()
            log = (Path(directory) / "dockerfmt/attempt-01.log").read_text()
            self.assertIn("partial download", log)
            self.assertIn("attempt timed out", log)

    def test_missing_go_fails_immediately_with_a_retained_diagnostic(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(install_go_ci_tool.subprocess, "run", side_effect=FileNotFoundError("go")) as run:
                status = install_go_ci_tool.install("dockerfmt", Path(directory))
            self.assertEqual(status, 127)
            run.assert_called_once()
            self.assertIn("Could not launch go", (Path(directory) / "dockerfmt/attempt-01.log").read_text())

    def test_budget_stops_before_an_attempt_or_retry(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(install_go_ci_tool.time, "monotonic", side_effect=[0, 481]):
                with mock.patch.object(install_go_ci_tool.subprocess, "run") as run:
                    status = install_go_ci_tool.install("dockerfmt", Path(directory))
            self.assertEqual(status, 124)
            run.assert_not_called()

        answer = subprocess.CompletedProcess(["go"], 1, "", CHECKSUM_STREAM_ERROR)
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(install_go_ci_tool.time, "monotonic", side_effect=[0, 0, 0, 0, 479]):
                with mock.patch.object(install_go_ci_tool.subprocess, "run", return_value=answer) as run:
                    with mock.patch.object(install_go_ci_tool.time, "sleep") as sleep:
                        status = install_go_ci_tool.install("dockerfmt", Path(directory))
            self.assertEqual(status, 124)
            run.assert_called_once()
            sleep.assert_not_called()
            self.assertTrue((Path(directory) / "dockerfmt/attempt-01.log").is_file())

    def test_invalid_cli_and_tool_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(install_go_ci_tool.install("unlisted", Path(directory)), 2)
            self.assertEqual(list(Path(directory).iterdir()), [])
        with mock.patch.object(sys, "argv", ["scripts/install_go_ci_tool.py"]):
            with self.assertRaises(SystemExit) as exit_status:
                runpy.run_path("scripts/install_go_ci_tool.py", run_name="__main__")
            self.assertEqual(exit_status.exception.code, 2)

    def test_workflow_checkout_precedes_install_and_uploads_attempts(self) -> None:
        workflow = Path(".github/workflows/check.yaml").read_text()
        starts = list(re.finditer(r"^  ([a-z][a-z0-9-]*):\n", workflow, re.MULTILINE))
        jobs = {
            match.group(1): workflow[match.start() : starts[index + 1].start() if index + 1 < len(starts) else len(workflow)]
            for index, match in enumerate(starts)
        }
        self.assertNotIn("go install github.com/reteps/dockerfmt@latest", workflow)
        for name, tools in (("checks", ("dockerfmt", "ghokin")), ("tests", ("dockerfmt",)), ("scenarios-build", ("dockerfmt",))):
            with self.subTest(job=name):
                job = jobs[name]
                self.assertLess(job.index("- name: Checkout"), job.index("scripts/install_go_ci_tool.py"))
                self.assertIn('go-version: "1.26"', job)
                for tool in tools:
                    self.assertIn(f"scripts/install_go_ci_tool.py {tool}", job)
                self.assertIn("if [[ -f scripts/publish_kache_report.py ]]; then", job)
                self.assertIn("name: go-tool-bootstrap-${{ github.job }}", job)
                self.assertIn("path: ${{ runner.temp }}/go-tool-bootstrap", job)


if __name__ == "__main__":
    unittest.main()
