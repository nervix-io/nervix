#!/usr/bin/env python3

"""Tests of the test archives' bucket: path selection, the transfer, and the steps that use it."""

from __future__ import annotations

import contextlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import re
import tempfile
import threading
import unittest
from unittest import mock
from urllib.parse import parse_qs, unquote, urlsplit
from xml.sax.saxutils import escape

from scripts import ci_artifacts as artifacts

ROOT = Path(__file__).resolve().parents[2]
ACTIONS = ("./.github/actions/ci-artifact-upload", "./.github/actions/ci-artifact-download")
WORKFLOWS = sorted((ROOT / ".github/workflows").glob("*.yaml"))
COMPOSITES = sorted((ROOT / ".github/actions").glob("*/action.yml"))
VARIABLES = ("R2_ARTIFACTS_BUCKET", "R2_ARTIFACTS_ENDPOINT")
SECRETS = ("R2_ARTIFACTS_ACCESS_KEY_ID", "R2_ARTIFACTS_SECRET_ACCESS_KEY")


class SigningTests(unittest.TestCase):
    def test_a_request_is_signed_as_the_published_example_of_sigv4_for_s3(self) -> None:
        # The GET Object example of Amazon's "Signature Calculations for the Authorization Header".
        empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        header = artifacts.authorization(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "us-east-1",
            "GET",
            "/test.txt",
            "",
            {
                "Host": "examplebucket.s3.amazonaws.com",
                "Range": "bytes=0-9",
                "x-amz-content-sha256": empty,
                "x-amz-date": "20130524T000000Z",
            },
            empty,
        )
        self.assertEqual(
            header,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,"
            "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,"
            "Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
        )


class SelectionTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()

    def write(self, *paths: str) -> None:
        for path in paths:
            target = self.root / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(path, encoding="utf-8")

    def stored(self, *lines: str) -> tuple[str, list[str]]:
        selection = artifacts.Selection.of(lines, str(self.root))
        names = sorted(os.path.relpath(path, selection.root) for path in selection.files)
        return os.path.relpath(selection.root, self.root), names

    def test_one_file_sits_at_the_root_of_its_artifact(self) -> None:
        self.write("target/selection.json")
        self.assertEqual(self.stored("target/selection.json"), ("target", ["selection.json"]))

    def test_one_directory_is_the_root_of_its_artifact(self) -> None:
        self.write("tests/logs/a.log", "tests/logs/deep/b.log")
        self.assertEqual(self.stored("tests/logs"), ("tests/logs", ["a.log", "deep/b.log"]))

    def test_several_paths_keep_their_common_ancestor(self) -> None:
        self.write("lcov-workspace.info", "target/paced-simulation/python.lcov")
        self.assertEqual(
            self.stored("lcov-workspace.info", "target/paced-simulation/python.lcov"),
            (".", ["lcov-workspace.info", "target/paced-simulation/python.lcov"]),
        )

    def test_a_missing_path_still_sets_the_common_ancestor(self) -> None:
        self.write("work/kache-report/summary.md")
        self.assertEqual(
            self.stored("work/kache-report", "work/kache-build-report"),
            ("work", ["kache-report/summary.md"]),
        )

    def test_a_glob_is_rooted_at_its_literal_prefix(self) -> None:
        self.write("target/native-coverage/a/b/lcov.info", "target/native-coverage/a/other.txt")
        self.assertEqual(
            self.stored("target/native-coverage/**/lcov.info"),
            ("target/native-coverage", ["a/b/lcov.info"]),
        )

    def test_a_star_stays_within_one_segment(self) -> None:
        self.write("target/q/one/a.log", "target/q/one/deep/b.log", "target/q/c.log")
        self.assertEqual(self.stored("target/q/*/*.log"), ("target/q", ["one/a.log"]))

    def test_a_wildcard_in_a_directory_name_selects_below_it(self) -> None:
        self.write("target/build-a/lane/x.json", "target/build-b/lane/y.json", "target/other/z.json")
        self.assertEqual(
            self.stored("target/build-*/lane/"),
            ("target", ["build-a/lane/x.json", "build-b/lane/y.json"]),
        )

    def test_a_later_negation_removes_what_earlier_patterns_selected(self) -> None:
        self.write(
            "cover/fuzz/run/completion.json",
            "cover/fuzz/run/profiles/deep/a.profraw",
            "cover/fuzz/run/merged.profdata",
            "cover/fuzz/run/targets/one/lcov.info",
        )
        self.assertEqual(
            self.stored(
                "cover/fuzz/",
                "!cover/fuzz/**/profiles/**",
                "!cover/fuzz/**/*.profdata",
                "!cover/fuzz/**/targets/*/lcov.info",
            ),
            ("cover/fuzz", ["run/completion.json"]),
        )

    def test_a_pattern_after_a_negation_selects_again(self) -> None:
        self.write("logs/a.log", "logs/b.log")
        self.assertEqual(
            self.stored("logs", "!logs/*.log", "logs/b.log"),
            ("logs", ["b.log"]),
        )

    def test_hidden_files_and_directories_are_skipped(self) -> None:
        self.write("logs/seen.log", "logs/.hidden.log", "logs/.cache/entry.log")
        self.assertEqual(self.stored("logs"), ("logs", ["seen.log"]))

    def test_a_linked_file_is_stored_and_a_link_loop_ends(self) -> None:
        self.write("logs/seen.log")
        (self.root / "logs/link.log").symlink_to(self.root / "logs/seen.log")
        (self.root / "logs/loop").symlink_to(self.root / "logs", target_is_directory=True)
        self.assertEqual(self.stored("logs"), ("logs", ["link.log", "seen.log"]))

    def test_paths_without_a_positive_pattern_are_refused(self) -> None:
        with self.assertRaisesRegex(artifacts.ArtifactError, "at least one pattern"):
            artifacts.Selection.of(["!logs"], str(self.root))


class FakeBucket:
    """An S3 path-style bucket on a local port, whose failures a test schedules."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.requests: list[tuple[str, str]] = []
        self.failures: list[tuple[str, int]] = []
        self.corrupted: list[str] = []
        self.lock = threading.Lock()
        bucket = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *arguments: object) -> None:
                return None

            def key(self) -> str:
                return unquote(urlsplit(self.path).path).removeprefix("/artifacts/").removeprefix("/artifacts")

            def scheduled(self) -> int | None:
                with bucket.lock:
                    bucket.requests.append((self.command, self.key()))
                    for index, (fragment, status) in enumerate(bucket.failures):
                        if fragment in self.key():
                            del bucket.failures[index]
                            return status
                return None

            def answer(self, status: int, body: bytes = b"") -> None:
                if status == 403:
                    body = b"<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>"
                self.send_response(status)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_PUT(self) -> None:
                body = self.rfile.read(int(self.headers["Content-Length"]))
                status = self.scheduled()
                if status is not None:
                    self.answer(status)
                    return
                with bucket.lock:
                    bucket.objects[self.key()] = body
                self.answer(200)

            def do_GET(self) -> None:
                status = self.scheduled()
                if status is not None:
                    self.answer(status)
                    return
                query = parse_qs(urlsplit(self.path).query)
                if "list-type" in query:
                    self.answer(200, bucket.listing(query["prefix"][0], query.get("continuation-token")))
                    return
                with bucket.lock:
                    body = bucket.objects.get(self.key())
                    corrupted = any(item in self.key() for item in bucket.corrupted)
                    if corrupted:
                        bucket.corrupted.clear()
                if body is None:
                    self.answer(404)
                    return
                self.answer(200, b"damaged" + body[7:] if corrupted else body)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def listing(self, prefix: str, token: list[str] | None) -> bytes:
        """One name a page, so a listing has to follow its continuation tokens."""
        with self.lock:
            names = sorted({key[len(prefix) :].split("/")[0] for key in self.objects if key.startswith(prefix)})
        index = int(token[0]) if token else 0
        page = "".join(f"<CommonPrefixes><Prefix>{escape(prefix + name)}/</Prefix></CommonPrefixes>" for name in names[index : index + 1])
        following = f"<NextContinuationToken>{index + 1}</NextContinuationToken>" if index + 1 < len(names) else ""
        namespace = 'xmlns="http://s3.amazonaws.com/doc/2006-03-01/"'
        return f"<ListBucketResult {namespace}><Prefix>{prefix}</Prefix>{page}{following}</ListBucketResult>".encode()

    def client(self) -> artifacts.Bucket:
        return artifacts.Bucket(
            name="artifacts",
            endpoint=f"http://127.0.0.1:{self.server.server_address[1]}",
            access_key="key",
            secret_key="secret",
        )

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


class TransferTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.work = Path(directory.name).resolve()
        self.fake = FakeBucket()
        self.addCleanup(self.fake.close)
        self.bucket = self.fake.client()
        sleeping = mock.patch.object(artifacts.time, "sleep")
        self.sleeps = sleeping.start()
        self.addCleanup(sleeping.stop)
        temporary = mock.patch.dict(os.environ, {"RUNNER_TEMP": str(self.work)})
        temporary.start()
        self.addCleanup(temporary.stop)

    def tree(self, files: dict[str, bytes]) -> artifacts.Selection:
        source = self.work / "source"
        for name, data in files.items():
            target = source / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        return artifacts.Selection.of([str(source)], str(self.work))

    def extracted(self, directory: Path) -> dict[str, bytes]:
        found = (path for path in directory.rglob("*") if path.is_file())
        return {str(path.relative_to(directory)): path.read_bytes() for path in found}

    def quiet(self) -> contextlib.AbstractContextManager[object]:
        return contextlib.redirect_stdout(io.StringIO())

    def test_an_artifact_round_trips_across_parts(self) -> None:
        for level in (0, 1):
            with self.subTest(level=level):
                files = {"a.bin": os.urandom(2500), "deep/b.bin": os.urandom(1200), "empty": b""}
                with self.quiet():
                    artifacts.upload(
                        self.bucket, "7", f"pack-{level}", self.tree(files), level, part_bytes=1024
                    )
                    artifacts.download(self.bucket, "7", f"pack-{level}", "", self.work / f"out-{level}")
                manifest = json.loads(self.fake.objects[f"runs/7/pack-{level}/manifest.json"])
                self.assertGreater(len(manifest["parts"]), 2)
                self.assertTrue(all(part["length"] <= 1024 for part in manifest["parts"]))
                self.assertEqual(self.extracted(self.work / f"out-{level}"), files)

    def test_a_part_that_fails_is_sent_again_on_its_own(self) -> None:
        files = {"a.bin": os.urandom(5000)}
        self.fake.failures = [("part-1", 503), ("part-1", 500), ("part-2", 429)]
        with self.quiet():
            artifacts.upload(self.bucket, "7", "retried", self.tree(files), 0, part_bytes=2048)
            artifacts.download(self.bucket, "7", "retried", "", self.work / "out")
        self.assertEqual(self.extracted(self.work / "out"), files)
        sent = [key for method, key in self.fake.requests if method == "PUT" and key.endswith("part-0")]
        self.assertEqual(len(sent), 1, "a part that succeeded is not sent again")
        self.assertEqual([call.args[0] for call in self.sleeps.call_args_list].count(2), 2)

    def test_a_refused_request_is_not_retried(self) -> None:
        self.fake.failures = [("part-0", 403)]
        with self.quiet(), self.assertRaisesRegex(artifacts.ArtifactError, "answered 403 AccessDenied"):
            artifacts.upload(self.bucket, "7", "refused", self.tree({"a": b"1"}), 0)
        self.assertEqual(sum(1 for method, _ in self.fake.requests if method == "PUT"), 1)
        self.assertEqual(self.sleeps.call_count, 0)

    def test_a_store_that_stays_away_fails_after_its_attempts(self) -> None:
        self.fake.failures = [("part-0", 500)] * artifacts.TRANSFER_ATTEMPTS
        with self.quiet(), self.assertRaisesRegex(artifacts.ArtifactError, "failed 6 times"):
            artifacts.upload(self.bucket, "7", "gone", self.tree({"a": b"1"}), 0)
        self.assertEqual([call.args[0] for call in self.sleeps.call_args_list], [2, 4, 8, 16, 30])

    def test_a_damaged_part_is_fetched_again(self) -> None:
        files = {"a.bin": os.urandom(4000)}
        with self.quiet():
            artifacts.upload(self.bucket, "7", "checked", self.tree(files), 0, part_bytes=1024)
            self.fake.corrupted = ["part-1"]
            artifacts.download(self.bucket, "7", "checked", "", self.work / "out")
        self.assertEqual(self.extracted(self.work / "out"), files)

    def test_an_upload_without_its_manifest_cannot_be_read(self) -> None:
        self.fake.failures = [("manifest.json", 403)]
        with self.quiet(), self.assertRaises(artifacts.ArtifactError):
            artifacts.upload(self.bucket, "7", "partial", self.tree({"a": b"1"}), 0)
        with self.quiet(), self.assertRaisesRegex(artifacts.ArtifactError, "no artifact partial"):
            artifacts.download(self.bucket, "7", "partial", "", self.work / "named")
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            artifacts.download(self.bucket, "7", "", "part*", self.work / "matched")
        self.assertIn("never completed", output.getvalue())
        self.assertFalse((self.work / "matched").exists())

    def test_a_rerun_replaces_an_artifact_as_a_whole(self) -> None:
        with self.quiet():
            artifacts.upload(self.bucket, "7", "again", self.tree({"first": b"1", "shared": b"1"}), 0)
            (self.work / "source/first").unlink()
            artifacts.upload(self.bucket, "7", "again", self.tree({"shared": b"2"}), 0)
            artifacts.download(self.bucket, "7", "again", "", self.work / "out")
        self.assertEqual(self.extracted(self.work / "out"), {"shared": b"2"})

    def test_a_transfer_keeps_its_parts_below_their_own_prefix(self) -> None:
        files = {"a.bin": os.urandom(3000)}
        with self.quiet():
            artifacts.upload(self.bucket, "7", "suite", self.tree(files), 0, "transfer", part_bytes=1024)
            artifacts.download(self.bucket, "7", "suite", "", self.work / "named")
            artifacts.download(self.bucket, "7", "", "sui*", self.work / "matched")
        parts = [key for key in self.fake.objects if "/part-" in key]
        self.assertGreater(len(parts), 1)
        self.assertTrue(all(key.startswith("transfer/7/suite/") for key in parts), parts)
        self.assertIn("runs/7/suite/manifest.json", self.fake.objects)
        self.assertEqual(self.extracted(self.work / "named"), files)
        self.assertEqual(self.extracted(self.work / "matched/suite"), files)

    def test_an_artifact_whose_parts_expired_says_so(self) -> None:
        with self.quiet():
            artifacts.upload(self.bucket, "7", "suite", self.tree({"a": b"1"}), 0, "transfer")
        for key in [key for key in self.fake.objects if key.startswith("transfer/")]:
            del self.fake.objects[key]
        with self.quiet(), self.assertRaisesRegex(artifacts.ArtifactError, "suite of run 7 has expired"):
            artifacts.download(self.bucket, "7", "suite", "", self.work / "out")

    def test_a_pattern_extracts_each_match_under_its_name(self) -> None:
        with self.quiet():
            artifacts.upload(self.bucket, "7", "coverage-1", self.tree({"a": b"1"}), 0)
            artifacts.upload(self.bucket, "7", "coverage-2", self.tree({"a": b"2"}), 0)
            artifacts.upload(self.bucket, "7", "other", self.tree({"a": b"3"}), 0)
            artifacts.upload(self.bucket, "8", "coverage-9", self.tree({"a": b"4"}), 0)
            artifacts.download(self.bucket, "7", "", "coverage-*", self.work / "out")
        self.assertEqual(self.extracted(self.work / "out"), {"coverage-1/a": b"1", "coverage-2/a": b"2"})

    def test_a_pattern_that_matches_nothing_is_not_an_error(self) -> None:
        with self.quiet():
            artifacts.download(self.bucket, "7", "", "absent-*", self.work / "out")
        self.assertFalse((self.work / "out").exists())


class CommandTests(unittest.TestCase):
    def run_command(self, *arguments: str, environment: dict[str, str] | None = None) -> tuple[int, str]:
        output = io.StringIO()
        variables = {name: "" for name in artifacts.ENVIRONMENT.values()} | (environment or {})
        with mock.patch.dict(os.environ, variables), contextlib.redirect_stdout(output):
            status = artifacts.main(list(arguments))
        return status, output.getvalue()

    def test_nothing_to_upload_follows_the_jobs_choice(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            missing = f"{directory}/absent"
            base = ("upload", "--run", "7", "--name", "n", "--path", missing)
            self.assertEqual(self.run_command(*base, "--if-no-files-found", "ignore")[0], 0)
            status, output = self.run_command(*base)
            self.assertEqual(status, 0)
            self.assertIn("::warning", output)
            status, output = self.run_command(*base, "--if-no-files-found", "error")
            self.assertEqual(status, 1)
            self.assertIn("::error", output)

    def test_a_missing_credential_is_named_without_any_value(self) -> None:
        with tempfile.NamedTemporaryFile() as file:
            status, output = self.run_command(
                "upload", "--run", "7", "--name", "n", "--path", file.name,
                environment={"R2_ARTIFACTS_ACCESS_KEY_ID": "visible-key-id"},
            )
        self.assertEqual(status, 1)
        self.assertIn("R2_ARTIFACTS_SECRET_ACCESS_KEY", output)
        self.assertNotIn("visible-key-id", output)

    def test_a_setting_pasted_with_a_line_ending_is_trimmed(self) -> None:
        values = dict.fromkeys(artifacts.ENVIRONMENT.values(), "value\n")
        values["R2_ARTIFACTS_ENDPOINT"] = "https://account.example\n"
        bucket = artifacts.Bucket.from_environment(values)
        self.assertEqual((bucket.secret_key, bucket.endpoint), ("value", "https://account.example"))

    def test_an_endpoint_without_a_scheme_is_refused(self) -> None:
        values = dict.fromkeys(artifacts.ENVIRONMENT.values(), "value")
        with self.assertRaisesRegex(artifacts.ArtifactError, "must be a URL"):
            artifacts.Bucket.from_environment(values)

    def test_a_download_takes_one_selection(self) -> None:
        for arguments in (("--name", "a", "--pattern", "b"), ()):
            with self.subTest(arguments=arguments), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    artifacts.parse_arguments(["download", "--run", "7", *arguments])


def jobs_of(text: str) -> dict[str, str]:
    """The text of each job of a workflow, by name."""
    parts = re.split(r"^  ([A-Za-z0-9_-]+):\n", text.split("\njobs:\n", 1)[1], flags=re.M)
    return dict(zip(parts[1::2], parts[2::2], strict=True))


def steps_of(text: str) -> list[str]:
    """The text of each step of a job or a composite action."""
    return re.split(r"^\s+- (?=name:|uses:|run:|if:|id:|env:|with:)", text, flags=re.M)[1:]


def given_inputs(step: str) -> set[str]:
    """The inputs a step passes, which are the keys one level below its `with:`."""
    match = re.search(r"^( +)with:\n((?:\1  .*\n|\n)*)", step + "\n", flags=re.M)
    if match is None:
        return set()
    indent = len(match.group(1)) + 2
    return set(re.findall(rf"^ {{{indent}}}([a-z-]+):", match.group(2), flags=re.M))


class WorkflowTests(unittest.TestCase):
    """The rules a job has to follow for these actions to work at all."""

    def test_every_job_checks_out_the_actions_before_it_uses_them(self) -> None:
        used = 0
        for path in WORKFLOWS:
            for job, text in jobs_of(path.read_text(encoding="utf-8")).items():
                action = re.search(r"uses: \./\.github/actions/", text)
                if action is None:
                    continue
                used += sum(text.count(f"uses: {name}") for name in ACTIONS)
                checkout = text.find("uses: actions/checkout@")
                self.assertNotEqual(checkout, -1, f"{path.name} {job} has no checkout")
                self.assertLess(checkout, action.start(), f"{path.name} {job} checks out too late")
        self.assertEqual(used, 4)

    def test_the_test_archives_alone_pass_through_the_bucket(self) -> None:
        used = set()
        for path in WORKFLOWS:
            for job, text in jobs_of(path.read_text(encoding="utf-8")).items():
                for step in steps_of(text):
                    for action in ACTIONS:
                        if f"uses: {action}" in step:
                            name = re.search(r"^ +name: ([a-z-]+)$", step.split("with:", 1)[1], flags=re.M)
                            used.add((path.name, job, action, name.group(1)))
        self.assertEqual(used, {
            ("check.yaml", "scenarios-build", ACTIONS[0], "scenario-suite"),
            ("check.yaml", "scenarios-build", ACTIONS[0], "unit-tests"),
            ("check.yaml", "scenarios", ACTIONS[1], "scenario-suite"),
            ("check.yaml", "tests", ACTIONS[1], "unit-tests"),
        })

    def test_the_steps_that_use_the_bucket_receive_it_and_no_others_do(self) -> None:
        receiving = 0
        for path in WORKFLOWS:
            for step in steps_of(path.read_text(encoding="utf-8")):
                if "R2_ARTIFACTS_" not in step:
                    continue
                receiving += 1
                self.assertTrue(any(f"uses: {action}" in step for action in ACTIONS), step)
                for name in VARIABLES:
                    self.assertIn(f"{name}: ${{{{ vars.{name} }}}}\n", step)
                for name in SECRETS:
                    self.assertIn(f"{name}: ${{{{ secrets.{name} }}}}\n", step)
        self.assertEqual(receiving, 4)

    def test_a_step_passes_only_inputs_the_action_declares(self) -> None:
        declared = {}
        for action in ACTIONS:
            text = (ROOT / action / "action.yml").read_text(encoding="utf-8")
            section = text.split("inputs:\n", 1)[1].split("runs:\n", 1)[0]
            declared[action] = set(re.findall(r"^  ([a-z-]+):$", section, flags=re.M))
        for path in (*WORKFLOWS, *COMPOSITES):
            for step in steps_of(path.read_text(encoding="utf-8")):
                for action, inputs in declared.items():
                    if f"uses: {action}" in step:
                        self.assertLessEqual(given_inputs(step), inputs, f"{path.name}: {step}")


if __name__ == "__main__":
    unittest.main()
