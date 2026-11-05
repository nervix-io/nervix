#!/usr/bin/env python3

"""Tests of the test archives' bucket: path selection, the transfer, and the steps that use it."""

from __future__ import annotations

import contextlib
from dataclasses import dataclass
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import re
import signal
import socket
import tempfile
import threading
import time
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


class WatchTests(unittest.TestCase):
    """The pace a part's body keeps, at times the test sets, and the cut that ends a connection."""

    def test_a_body_that_falls_behind_its_pace_is_cut_off(self) -> None:
        limits = artifacts.Limits(slowest_bytes_per_second=100, slow_seconds=10)
        attempt = artifacts.Attempt()
        attempt.pacing = True
        attempt.watch(0.0, limits)
        attempt.moved = 600
        attempt.watch(9.0, limits)
        # A thousand bytes keep the pace of the ten seconds, and the next ten seconds start here.
        attempt.moved = 1000
        attempt.watch(9.5, limits)
        attempt.moved = 1999
        attempt.watch(19.0, limits)
        self.assertIsNone(attempt.cut)
        attempt.watch(19.6, limits)
        self.assertEqual(attempt.cut, "too slow: below 100 B/s for 10 s")

    def test_a_body_that_is_not_moving_keeps_no_pace(self) -> None:
        limits = artifacts.Limits(slowest_bytes_per_second=100, slow_seconds=10)
        attempt = artifacts.Attempt()
        attempt.watch(0.0, limits)
        attempt.watch(100.0, limits)
        self.assertIsNone(attempt.cut)
        # A body that starts moving starts its pace afresh.
        attempt.pacing = True
        attempt.watch(200.0, limits)
        attempt.watch(209.0, limits)
        self.assertIsNone(attempt.cut)

    def test_a_cut_ends_a_read_blocked_on_the_connection(self) -> None:
        near, far = socket.socketpair()
        self.addCleanup(near.close)
        self.addCleanup(far.close)
        attempt = artifacts.Attempt()
        attempt.connected(near)
        received: list[bytes] = []
        reader = threading.Thread(target=lambda: received.append(near.recv(1)))
        reader.start()
        attempt.cut_off("too slow")
        reader.join(timeout=10)
        self.assertFalse(reader.is_alive())
        self.assertEqual(received, [b""])
        self.assertEqual(attempt.cut, "too slow")

    def test_a_cut_ends_a_write_blocked_on_the_connection(self) -> None:
        near, far = socket.socketpair()
        self.addCleanup(near.close)
        self.addCleanup(far.close)
        attempt = artifacts.Attempt()
        attempt.connected(near)
        failures: list[OSError] = []

        def write() -> None:
            try:
                near.sendall(bytes(64 * 1024 * 1024))
            except OSError as error:
                failures.append(error)

        writer = threading.Thread(target=write)
        writer.start()
        attempt.cut_off("too slow")
        writer.join(timeout=10)
        self.assertFalse(writer.is_alive())
        self.assertEqual(len(failures), 1)

    def test_a_slow_body_cut_off_keeps_what_its_read_gathered_while_data_still_arrives(self) -> None:
        # A read gathering a block over TCP: Linux resets a connection that shut its write side when
        # more data arrives, and a buffered read that meets the reset loses the block it gathered.
        listener = socket.create_server(("127.0.0.1", 0))
        self.addCleanup(listener.close)
        near = socket.create_connection(listener.getsockname(), timeout=10)
        self.addCleanup(near.close)
        far, _ = listener.accept()
        self.addCleanup(far.close)
        attempt = artifacts.Attempt()
        attempt.connected(near)
        attempt.receiving = True
        far.sendall(b"a" * 1000)
        attempt.cut_off("too slow", slow=True)
        far.sendall(b"b")
        with near.makefile("rb") as body:
            gathered = body.read(4096)
        self.assertEqual(gathered[:1000], b"a" * 1000)

    def test_a_cut_after_its_connection_closed_reaches_no_other(self) -> None:
        near, far = socket.socketpair()
        self.addCleanup(far.close)
        attempt = artifacts.Attempt()
        attempt.connected(near)
        connection = mock.Mock(spec=["close"])
        connection.close.side_effect = near.close
        attempt.close(connection, None)
        # The descriptor it closed is free for the next connection to take.
        reused, peer = socket.socketpair()
        self.addCleanup(reused.close)
        self.addCleanup(peer.close)
        attempt.cut_off("too slow")
        reused.sendall(b"x")
        self.assertEqual(peer.recv(1), b"x")

    def test_an_attempt_cut_before_it_connects_is_cut_as_it_connects(self) -> None:
        near, far = socket.socketpair()
        self.addCleanup(near.close)
        self.addCleanup(far.close)
        attempt = artifacts.Attempt()
        attempt.cut_off("the transfer stopped")
        attempt.connected(near)
        self.assertEqual(near.recv(1), b"")

    def test_a_stopped_transfer_cuts_its_attempts_and_those_that_start_after(self) -> None:
        progress = artifacts.Progress("suite", "sent", [1, 1], artifacts.Limits())
        first = progress.begin(0)
        progress.stop()
        later = progress.begin(1)
        self.assertEqual((first.cut, later.cut), ("the transfer stopped", "the transfer stopped"))

    def test_an_upload_counts_a_block_once_the_next_is_asked_for_and_paces_until_sent(self) -> None:
        progress = artifacts.Progress("suite", "sent", [2500], artifacts.Limits())
        attempt = progress.begin(0)
        body = artifacts.MeteredBody(io.BytesIO(bytes(2500)), progress.part(0), attempt)
        seen = []
        for _ in range(4):
            block = body.read(1024)
            seen.append((len(block), attempt.moved, progress.moved[0], attempt.pacing))
        self.assertEqual(
            seen, [(1024, 0, 0, True), (1024, 1024, 1024, True), (452, 2048, 2048, True), (0, 2500, 2500, False)]
        )

class SplitTests(unittest.TestCase):
    """How what is left of a slow range is split, and how ranges that arrive out of order make the
    part, without a store."""

    def received(self, limits: artifacts.Limits, data: bytes = bytes(100)) -> artifacts.ReceivedPart:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        progress = artifacts.Progress("suite", "received", [len(data)], limits)
        spare = artifacts.SpareRequests(limits.spare_requests)
        self.addCleanup(spare.__exit__, None, None, None)
        part = artifacts.Part(len(data), hashlib.sha256(data).hexdigest())
        received = artifacts.ReceivedPart(Path(directory.name) / "part-0", part, progress.part(0), spare)
        self.addCleanup(received.__exit__, None, None, None)
        received.ranges_honored = True
        return received

    def test_a_slow_range_keeps_the_first_of_equal_pieces_and_hands_out_the_others(self) -> None:
        received = self.received(artifacts.Limits(split_ways=4, smallest_range_bytes=10, spare_requests=8))
        span = received.whole()
        span.next = 20
        fetched: list[tuple[int, int, int]] = []
        plan = received.split(span, lambda piece: fetched.append((piece.start, piece.end, piece.next)))
        self.assertIsNone(received.join())
        self.assertEqual(plan, "the 80 B left as 4 ranges at once")
        self.assertEqual((span.next, span.end, span.wanted()), (20, 40, "bytes=20-39"))
        self.assertEqual(sorted(fetched), [(40, 60, 40), (60, 80, 60), (80, 100, 80)])

    def test_spare_requests_bound_the_pieces(self) -> None:
        received = self.received(artifacts.Limits(split_ways=4, smallest_range_bytes=10, spare_requests=1))
        span = received.whole()
        span.next = 20
        fetched: list[tuple[int, int]] = []
        plan = received.split(span, lambda piece: fetched.append((piece.start, piece.end)))
        self.assertIsNone(received.join())
        self.assertEqual((plan, span.end, fetched), ("the 80 B left as 2 ranges at once", 60, [(60, 100)]))
        # The request the piece took is given back once it ends.
        self.assertEqual(received.split(span, lambda piece: None), "the 40 B left as 2 ranges at once")

    def test_a_range_too_small_to_split_or_a_part_in_its_most_ranges_is_left_as_it_is(self) -> None:
        small = self.received(artifacts.Limits(smallest_range_bytes=60))
        self.assertIsNone(small.split(small.whole(), lambda piece: None))
        crowded = self.received(artifacts.Limits(split_ways=4, smallest_range_bytes=10, most_ranges_per_part=2))
        span = crowded.whole()
        self.assertEqual(crowded.split(span, lambda piece: None), "the 100 B left as 2 ranges at once")
        self.assertIsNone(crowded.split(span, lambda piece: None))

    def test_a_store_that_ignored_a_range_has_nothing_split(self) -> None:
        received = self.received(artifacts.Limits(smallest_range_bytes=10))
        received.ranges_honored = False
        self.assertIsNone(received.split(received.whole(), lambda piece: None))

    def test_ranges_that_arrive_out_of_order_make_the_part_its_manifest_names(self) -> None:
        data = os.urandom(100)
        received = self.received(artifacts.Limits(), data)
        spans = [artifacts.Span(0, 40, 0), artifacts.Span(40, 70, 40), artifacts.Span(70, 100, 70)]
        for span in (spans[2], spans[0], spans[1]):
            received.write(span, data[span.start : span.end])
        self.assertTrue(received.verified())
        damaged = self.received(artifacts.Limits(), data)
        for span in (artifacts.Span(70, 100, 70), artifacts.Span(0, 70, 0)):
            damaged.write(span, bytes(span.end - span.start))
        self.assertFalse(damaged.verified())


class RetryTests(unittest.TestCase):
    """How many failures in a row a request may have, which its part's progress decides."""

    def run_retries(self, request, part: artifacts.PartProgress) -> str:
        with mock.patch.object(artifacts, "pause"), contextlib.redirect_stdout(io.StringIO()):
            return artifacts.with_retries("reading part-0", request, part.limits, part, kept=lambda: 0, moving=part.held)

    def test_a_range_starved_while_the_rest_of_its_part_moves_goes_on(self) -> None:
        progress = artifacts.Progress("suite", "received", [100], artifacts.Limits(backoff_jitter=0))
        part = progress.part(0)
        tries: list[artifacts.Attempt] = []

        def request(attempt: artifacts.Attempt) -> str:
            tries.append(attempt)
            if len(tries) <= 2 * artifacts.TRANSFER_ATTEMPTS:
                # Another range of the part moved while this one received nothing.
                part.add(1)
                raise artifacts.Transient("too slow")
            return "done"

        self.assertEqual(self.run_retries(request, part), "done")
        self.assertEqual(len(tries), 2 * artifacts.TRANSFER_ATTEMPTS + 1)

    def test_a_range_of_a_part_that_stopped_moving_fails_after_its_attempts(self) -> None:
        progress = artifacts.Progress("suite", "received", [100], artifacts.Limits(backoff_jitter=0))
        part = progress.part(0)

        def request(attempt: artifacts.Attempt) -> str:
            raise artifacts.Transient("too slow")

        with self.assertRaisesRegex(artifacts.ArtifactError, "failed 6 times in a row, last with too slow"):
            self.run_retries(request, part)


class BackoffTests(unittest.TestCase):
    def test_each_backoff_waits_longer_and_at_random_up_to_half_again(self) -> None:
        limits = artifacts.Limits()
        for failures, doubled in enumerate([2, 4, 8, 16, 30, 30], start=1):
            with mock.patch.object(artifacts.random, "uniform", return_value=1.25) as uniform:
                self.assertEqual(limits.backoff(failures), doubled * 1.25)
            uniform.assert_called_once_with(1, 1.5)


class ReportTests(unittest.TestCase):
    """What a transfer says about itself, at a time the test sets."""

    def test_a_report_counts_the_parts_and_names_the_slowest_one_moving(self) -> None:
        now = [100.0]
        progress = artifacts.Progress(
            "suite", "received", [1024, 2048, 4096, 1024], artifacts.Limits(), clock=lambda: now[0]
        )
        finished, second, third, _ = (progress.part(index) for index in range(4))
        attempts = [part.begin() for part in (finished, second, third)]
        finished.add(1024)
        finished.end(attempts[0])
        finished.complete()
        second.add(1024)
        third.add(512)
        now[0] = 110.0
        self.assertEqual(
            progress.report(),
            "suite: received 2.5 KiB of 8.0 KiB (31%) in 0:10, 256 B/s; parts: 1 done, 2 moving, "
            "0 waiting to retry, 1 queued; slowest part-2: 512 B of 4.0 KiB at 51 B/s",
        )
        second.add(1024)
        second.end(attempts[1])
        second.complete()
        # A part that starts again moved all it holds since the previous report.
        third.end(attempts[2])
        third.reset()
        third.add(300)
        now[0] = 115.0
        reports: list[str] = []
        with mock.patch.object(artifacts, "pause", side_effect=lambda stopping, seconds: reports.append(progress.report())):
            third.wait(2.0)
        self.assertEqual(
            reports,
            [
                "suite: received 3.3 KiB of 8.0 KiB (41%) in 0:15, 265 B/s; parts: 2 done, 0 moving, "
                "1 waiting to retry, 1 queued"
            ],
        )

    def test_a_part_asked_for_in_several_ranges_names_them(self) -> None:
        now = [0.0]
        progress = artifacts.Progress("suite", "received", [8192], artifacts.Limits(), clock=lambda: now[0])
        part = progress.part(0)
        for _ in range(3):
            part.begin()
        part.add(3000)
        now[0] = 10.0
        self.assertEqual(
            progress.report(),
            "suite: received 2.9 KiB of 8.0 KiB (37%) in 0:10, 300 B/s; parts: 0 done, 1 moving, "
            "0 waiting to retry, 0 queued; slowest part-0: 2.9 KiB of 8.0 KiB at 300 B/s in 3 ranges",
        )

    def test_a_long_transfer_counts_its_hours(self) -> None:
        now = [0.0]
        progress = artifacts.Progress("suite", "sent", [5 * 1024**3], artifacts.Limits(), clock=lambda: now[0])
        progress.part(0).begin()
        now[0] = 3725.0
        self.assertEqual(
            progress.report(),
            "suite: sent 0 B of 5.0 GiB (0%) in 1:02:05, 0 B/s; parts: 0 done, 1 moving, "
            "0 waiting to retry, 0 queued; slowest part-0: 0 B of 5.0 GiB at 0 B/s",
        )

    def test_a_part_that_started_since_the_previous_report_is_not_named_the_slowest(self) -> None:
        now = [0.0]
        progress = artifacts.Progress("suite", "sent", [4096, 4096], artifacts.Limits(), clock=lambda: now[0])
        steady = progress.part(0)
        steady.begin()
        steady.add(1024)
        now[0] = 10.0
        progress.report()
        steady.add(2000)
        now[0] = 15.0
        progress.part(1).begin()
        now[0] = 20.0
        self.assertEqual(
            progress.report(),
            "suite: sent 3.0 KiB of 8.0 KiB (37%) in 0:20, 200 B/s; parts: 0 done, 2 moving, "
            "0 waiting to retry, 0 queued; slowest part-0: 3.0 KiB of 4.0 KiB at 200 B/s",
        )

    def test_a_finished_transfer_ends_with_its_size_time_and_rate(self) -> None:
        now = [0.0]
        progress = artifacts.Progress("suite", "sent", [3 * 1024 * 1024], artifacts.Limits(), clock=lambda: now[0])
        progress.part(0).complete()
        now[0] = 75.0
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            progress.finish()
        self.assertEqual(output.getvalue(), "suite: sent 3.0 MiB in 1 parts in 1:15, 41.0 KiB/s\n")

    def test_a_transfer_with_failed_attempts_ends_with_a_warning(self) -> None:
        now = [0.0]
        progress = artifacts.Progress("suite", "sent", [3 * 1024 * 1024], artifacts.Limits(), clock=lambda: now[0])
        progress.part(0).failed()
        progress.part(0).failed()
        progress.part(0).complete()
        now[0] = 75.0
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            progress.finish()
        self.assertEqual(
            output.getvalue(),
            "::warning title=CI artifact::suite: sent 3.0 MiB in 1 parts in 1:15, 41.0 KiB/s, after 2 failed attempts\n",
        )


@dataclass(frozen=True)
class Cut:
    """Send this many bytes of the body, then close the connection."""

    after: int


@dataclass(frozen=True)
class Stall:
    """Send this many bytes of the body, then nothing for a while, then the rest."""

    after: int
    seconds: float


@dataclass(frozen=True)
class Trickle:
    """Send this many bytes of the body, then the rest one byte every 20 milliseconds: for the
    next request, or for the next that asks for a range starting below `below`."""

    after: int
    below: int | None = None


@dataclass(frozen=True)
class Silence:
    """Take the request, and close the connection after a while without answering it."""

    seconds: float


@dataclass(frozen=True)
class SlowRead:
    """Take a body 64 KiB every 50 milliseconds, and store nothing of it."""


class FakeBucket:
    """An S3 path-style bucket on a local port, whose failures a test schedules: statuses, and
    answers that end early, stall, crawl or never come."""

    def __init__(self, receive_buffer: int | None = None) -> None:
        self.objects: dict[str, bytes] = {}
        self.requests: list[tuple[str, str]] = []
        # The Range header of every GET of an object, by key.
        self.ranges: list[tuple[str, str | None]] = []
        self.failures: list[tuple[str, int]] = []
        self.faults: list[tuple[str, Cut | Stall | Trickle | Silence | SlowRead]] = []
        self.corrupted: list[str] = []
        self.ranges_ignored = False
        self.answer_delay = 0.0
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

            def fault(self, start: int = 0) -> Cut | Stall | Trickle | Silence | SlowRead | None:
                with bucket.lock:
                    for index, (fragment, fault) in enumerate(bucket.faults):
                        below = getattr(fault, "below", None)
                        if fragment in self.key() and (below is None or start < below):
                            del bucket.faults[index]
                            return fault
                return None

            def answer(self, status: int, body: bytes = b"") -> None:
                if status == 403:
                    body = b"<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>"
                try:
                    self.send_response(status)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except OSError:
                    # The client cut its connection off while this answer waited.
                    return

            def deliver(self, body: bytes, fault: Cut | Stall | Trickle | None) -> None:
                """End the headers and write a body as a fault says, and stop quietly once the client
                hangs up. The bytes a fault lets through go in the same write as the headers, so a
                client holds them as soon as it holds the answer, however late this thread runs."""
                if fault is None:
                    prefix = body
                else:
                    prefix = body[: fault.after]
                try:
                    # What end_headers does, with the body's first bytes joined to the headers.
                    self._headers_buffer.append(b"\r\n")
                    self._headers_buffer.append(prefix)
                    self.flush_headers()
                    if fault is None:
                        return
                    if isinstance(fault, Stall):
                        time.sleep(fault.seconds)
                        self.wfile.write(body[fault.after :])
                    elif isinstance(fault, Trickle):
                        for index in range(fault.after, len(body)):
                            time.sleep(0.02)
                            self.wfile.write(body[index : index + 1])
                except OSError:
                    return

            def crawl(self, length: int) -> None:
                """Take a body slowly until the client gives up on it."""
                taken = 0
                try:
                    while taken < length:
                        chunk = self.rfile.read(min(64 * 1024, length - taken))
                        if not chunk:
                            return
                        taken += len(chunk)
                        time.sleep(0.05)
                except OSError:
                    return

            def do_PUT(self) -> None:
                status = self.scheduled()
                fault = self.fault()
                length = int(self.headers["Content-Length"])
                if isinstance(fault, SlowRead):
                    self.crawl(length)
                    return
                try:
                    body = self.rfile.read(length)
                except OSError:
                    return
                # A client that gave up on a body stored nothing.
                if len(body) < length:
                    return
                if status is not None:
                    self.answer(status)
                    return
                if isinstance(fault, Silence):
                    time.sleep(fault.seconds)
                    return
                time.sleep(bucket.answer_delay)
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
                    bucket.ranges.append((self.key(), self.headers.get("Range")))
                wanted = bucket.requested_range(self.headers.get("Range"))
                fault = self.fault(0 if wanted is None else wanted[0])
                if isinstance(fault, Silence):
                    time.sleep(fault.seconds)
                    return
                with bucket.lock:
                    body = bucket.objects.get(self.key())
                    corrupted = any(item in self.key() for item in bucket.corrupted)
                    if corrupted:
                        bucket.corrupted.clear()
                if body is None:
                    self.answer(404)
                    return
                if corrupted:
                    body = b"damaged" + body[7:]
                if wanted is None:
                    self.send_response(200)
                elif wanted[0] > wanted[1]:
                    self.answer(416)
                    return
                else:
                    first, last = wanted
                    self.send_response(206)
                    self.send_header("Content-Range", f"bytes {first}-{last}/{len(body)}")
                    body = body[first : last + 1]
                self.send_header("Content-Length", str(len(body)))
                self.deliver(body, fault)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        if receive_buffer is not None:
            # The connections it accepts take this buffer, so a client sending to a bucket that
            # reads slowly blocks once its own send buffer is full, whatever the host's tuning.
            self.server.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, receive_buffer)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def requested_range(self, header: str | None) -> tuple[int, int] | None:
        """The first and last byte a `bytes=N-M` range asks for, or None for the whole object, which
        a request without a range and a store that ignores ranges both receive."""
        if header is None or self.ranges_ignored:
            return None
        match = re.fullmatch(r"bytes=(\d+)-(\d+)", header)
        if match is None:
            raise AssertionError(f"a range the client never asks for: {header}")
        return int(match.group(1)), int(match.group(2))

    def listing(self, prefix: str, token: list[str] | None) -> bytes:
        """One name a page, so a listing has to follow its continuation tokens."""
        with self.lock:
            names = sorted({key[len(prefix) :].split("/")[0] for key in self.objects if key.startswith(prefix)})
        index = int(token[0]) if token else 0
        page = "".join(f"<CommonPrefixes><Prefix>{escape(prefix + name)}/</Prefix></CommonPrefixes>" for name in names[index : index + 1])
        following = f"<NextContinuationToken>{index + 1}</NextContinuationToken>" if index + 1 < len(names) else ""
        namespace = 'xmlns="http://s3.amazonaws.com/doc/2006-03-01/"'
        return f"<ListBucketResult {namespace}><Prefix>{prefix}</Prefix>{page}{following}</ListBucketResult>".encode()

    def client(self, limits: artifacts.Limits | None = None) -> artifacts.Bucket:
        """A client of this bucket, whose backoffs are not random unless the test's limits say so."""
        return artifacts.Bucket(
            name="artifacts",
            endpoint=f"http://127.0.0.1:{self.server.server_address[1]}",
            access_key="key",
            secret_key="secret",
            limits=artifacts.Limits(backoff_jitter=0) if limits is None else limits,
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
        pausing = mock.patch.object(artifacts, "pause")
        self.pauses = pausing.start()
        self.addCleanup(pausing.stop)
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

    def delays(self) -> list[float]:
        return [call.args[1] for call in self.pauses.call_args_list]

    def ranges_of(self, part: str) -> list[str | None]:
        return [header for key, header in self.fake.ranges if key.endswith(f"/{part}")]

    def stored(self, name: str, files: dict[str, bytes]) -> None:
        """Upload an artifact of three parts: 4096, 4096 and 2048 bytes of a padded tar stream."""
        with self.quiet():
            artifacts.upload(self.bucket, "7", name, self.tree(files), 0, part_bytes=4096)

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

    def test_a_transfer_says_what_it_moves_and_how_fast(self) -> None:
        files = {"a.bin": os.urandom(5000)}
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            artifacts.upload(self.bucket, "7", "told", self.tree(files), 0, part_bytes=4096)
            artifacts.download(self.bucket, "7", "told", "", self.work / "out")
        lines = output.getvalue().splitlines()
        self.assertEqual(lines[0], "told: 1 files, 10240 bytes in 3 parts")
        self.assertRegex(lines[1], r"^told: sent 10\.0 KiB in 3 parts in 0:00, .+/s$")
        self.assertEqual(lines[2], "told: 10240 bytes in 3 parts")
        self.assertRegex(lines[3], r"^told: received 10\.0 KiB in 3 parts in 0:00, .+/s$")
        self.assertEqual(lines[4], f"told: extracted into {self.work / 'out'}")

    def test_moving_parts_report_how_far_they_got(self) -> None:
        files = {"a.bin": os.urandom(5000)}
        self.stored("watched", files)
        self.fake.faults = [("part-0", Stall(after=1000, seconds=0.5))]
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            bucket = self.fake.client(artifacts.Limits(report_seconds=0.05, backoff_jitter=0))
            artifacts.download(bucket, "7", "watched", "", self.work / "out")
        self.assertEqual(self.extracted(self.work / "out"), files)
        # A download counts what arrived a block or a moment at a time, so a stalled part's bytes
        # are those it held when it last counted them.
        stalled = re.compile(
            r"^watched: received .+ of 10\.0 KiB \(\d+%\) in 0:00, 0 B/s; parts: 2 done, 1 moving, "
            r"0 waiting to retry, 0 queued; slowest part-0: \d+ B of 4\.0 KiB at 0 B/s$"
        )
        reports = [line for line in output.getvalue().splitlines() if stalled.match(line)]
        self.assertTrue(reports, output.getvalue())

    def test_a_part_that_fails_is_sent_again_on_its_own(self) -> None:
        files = {"a.bin": os.urandom(5000)}
        self.fake.failures = [("part-1", 503), ("part-1", 500), ("part-2", 429)]
        with self.quiet():
            artifacts.upload(self.bucket, "7", "retried", self.tree(files), 0, part_bytes=2048)
            artifacts.download(self.bucket, "7", "retried", "", self.work / "out")
        self.assertEqual(self.extracted(self.work / "out"), files)
        sent = [key for method, key in self.fake.requests if method == "PUT" and key.endswith("part-0")]
        self.assertEqual(len(sent), 1, "a part that succeeded is not sent again")
        self.assertEqual(sorted(self.delays()), [2, 2, 4])

    def test_a_refused_request_is_not_retried(self) -> None:
        self.fake.failures = [("part-0", 403)]
        with self.quiet(), self.assertRaisesRegex(artifacts.ArtifactError, "answered 403 AccessDenied"):
            artifacts.upload(self.bucket, "7", "refused", self.tree({"a": b"1"}), 0)
        self.assertEqual(sum(1 for method, _ in self.fake.requests if method == "PUT"), 1)
        self.assertEqual(self.pauses.call_count, 0)

    def test_a_store_that_stays_away_fails_after_its_attempts(self) -> None:
        self.fake.failures = [("part-0", 500)] * artifacts.TRANSFER_ATTEMPTS
        with self.quiet(), self.assertRaisesRegex(artifacts.ArtifactError, "failed 6 times in a row, last with status 500"):
            artifacts.upload(self.bucket, "7", "gone", self.tree({"a": b"1"}), 0)
        self.assertEqual(self.delays(), [2, 4, 8, 16, 30])

    def test_a_part_that_fails_stops_the_others_at_once(self) -> None:
        self.fake.failures = [("part-0", 403)]
        self.fake.answer_delay = 5.0
        files = {"a.bin": os.urandom(12000)}
        started = time.monotonic()
        with self.quiet(), self.assertRaisesRegex(artifacts.ArtifactError, "answered 403"):
            artifacts.upload(self.bucket, "7", "stopped", self.tree(files), 0, part_bytes=1024)
        self.assertLess(time.monotonic() - started, 3.0, "the parts waiting for an answer are cut off")
        sent = {int(key.rsplit("-", 1)[1]) for method, key in self.fake.requests if method == "PUT"}
        self.assertIn(0, sent)
        self.assertLessEqual(max(sent), artifacts.CONCURRENT_TRANSFERS - 1, "no part starts after one failed")
        self.assertNotIn("runs/7/stopped/manifest.json", self.fake.objects)

    def test_a_part_that_is_cut_off_resumes_where_it_stopped(self) -> None:
        files = {"a.bin": os.urandom(5000)}
        self.stored("resumed", files)
        self.fake.faults = [("part-0", Cut(after=1500))]
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            artifacts.download(self.bucket, "7", "resumed", "", self.work / "out")
        self.assertEqual(self.extracted(self.work / "out"), files)
        self.assertEqual(self.ranges_of("part-0"), ["bytes=0-4095", "bytes=1500-4095"])
        self.assertIn(
            "part-0 from 0 B: attempt 1 failed with the body ended at 1500 of 4096 bytes; resuming at 1.5 KiB in 2.0 s",
            output.getvalue(),
        )

    def test_attempts_that_keep_bytes_do_not_use_up_the_retries(self) -> None:
        files = {"a.bin": os.urandom(5000)}
        self.stored("patient", files)
        cuts = artifacts.TRANSFER_ATTEMPTS + 2
        self.fake.faults = [("part-0", Cut(after=100))] * cuts
        with self.quiet():
            artifacts.download(self.bucket, "7", "patient", "", self.work / "out")
        self.assertEqual(self.extracted(self.work / "out"), files)
        self.assertEqual(self.ranges_of("part-0"), [f"bytes={100 * index}-4095" for index in range(cuts + 1)])
        self.assertEqual(self.delays(), [2] * cuts)

    def test_a_store_that_ignores_ranges_sends_the_part_from_its_start(self) -> None:
        files = {"a.bin": os.urandom(5000)}
        self.stored("whole", files)
        self.fake.ranges_ignored = True
        self.fake.faults = [("part-0", Cut(after=1500))]
        with self.quiet():
            artifacts.download(self.bucket, "7", "whole", "", self.work / "out")
        self.assertEqual(self.extracted(self.work / "out"), files)
        self.assertEqual(self.ranges_of("part-0"), ["bytes=0-4095", "bytes=1500-4095"])

    def test_a_part_that_crawls_is_cut_off_and_resumed(self) -> None:
        files = {"a.bin": os.urandom(5000)}
        self.stored("crawled", files)
        self.fake.faults = [("part-0", Trickle(after=1000))]
        limits = artifacts.Limits(
            slowest_bytes_per_second=64 * 1024, slow_seconds=0.5, watch_seconds=0.02, backoff_jitter=0
        )
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            artifacts.download(self.fake.client(limits), "7", "crawled", "", self.work / "out")
        self.assertEqual(self.extracted(self.work / "out"), files)
        self.assertRegex(output.getvalue(), r"part-0 from 0 B: attempt 1 failed with too slow: below 64\.0 KiB/s for \d+ s; resuming at ")
        resumed = self.ranges_of("part-0")[1]
        self.assertGreaterEqual(int(resumed.removeprefix("bytes=").split("-")[0]), 1000)

    def test_an_upload_that_crawls_is_cut_off_and_sent_again(self) -> None:
        fake = FakeBucket(receive_buffer=8192)
        self.addCleanup(fake.close)
        fake.faults = [("part-0", SlowRead())]
        limits = artifacts.Limits(
            slowest_bytes_per_second=8 * 1024 * 1024, slow_seconds=0.5, watch_seconds=0.05, backoff_jitter=0
        )
        files = {"a.bin": bytes(64 * 1024 * 1024)}
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            artifacts.upload(fake.client(limits), "7", "crawled", self.tree(files), 0)
            artifacts.download(fake.client(), "7", "crawled", "", self.work / "out")
        self.assertEqual(self.extracted(self.work / "out"), files)
        self.assertRegex(
            output.getvalue(),
            r"storing \S+/part-0: attempt 1 failed with too slow: below 8\.0 MiB/s for \d+ s; retrying in 2\.0 s",
        )

    def test_a_slow_part_is_asked_for_in_new_ranges_at_once(self) -> None:
        files = {"a.bin": os.urandom(60000)}
        with self.quiet():
            artifacts.upload(self.bucket, "7", "split", self.tree(files), 0)
        length = json.loads(self.fake.objects["runs/7/split/manifest.json"])["parts"][0]["length"]
        self.fake.faults = [("part-0", Trickle(after=1000))]
        limits = artifacts.Limits(slow_seconds=0.5, watch_seconds=0.02, smallest_range_bytes=4096, backoff_jitter=0)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            artifacts.download(self.fake.client(limits), "7", "split", "", self.work / "out")
        self.assertEqual(self.extracted(self.work / "out"), files)
        self.assertRegex(
            output.getvalue(),
            r"reading \S+/part-0 from 0 B: attempt 1 failed with too slow: below 256\.0 KiB/s for \d+ s; "
            r"asking for the [\d.]+ KiB left as 4 ranges at once",
        )
        first, *later = self.ranges_of("part-0")
        self.assertEqual(first, f"bytes=0-{length - 1}")
        starts = sorted(int(header.removeprefix("bytes=").split("-")[0]) for header in later)
        ends = sorted(int(header.split("-")[1]) for header in later)
        self.assertGreaterEqual(len(later), 4)
        self.assertGreaterEqual(starts[0], 1000, "no range asks again for bytes that arrived")
        self.assertGreaterEqual(len(set(ends)), 4, "every range split off is a request of its own")
        self.assertEqual(ends[-1], length - 1)

    def test_a_range_that_stays_slow_is_split_again(self) -> None:
        files = {"a.bin": os.urandom(60000)}
        with self.quiet():
            artifacts.upload(self.bucket, "7", "again", self.tree(files), 0)
        # The part's first request crawls, and so does the first range split off it, which starts
        # where the crawl stopped; ranges further on move at once.
        self.fake.faults = [("part-0", Trickle(after=1000, below=8192))] * 2
        limits = artifacts.Limits(slow_seconds=0.5, watch_seconds=0.02, smallest_range_bytes=2048, backoff_jitter=0)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            artifacts.download(self.fake.client(limits), "7", "again", "", self.work / "out")
        self.assertEqual(self.extracted(self.work / "out"), files)
        self.assertEqual(output.getvalue().count("ranges at once"), 2, output.getvalue())

    def test_a_slow_part_without_spare_requests_is_asked_for_again_as_it_is(self) -> None:
        files = {"a.bin": os.urandom(60000)}
        with self.quiet():
            artifacts.upload(self.bucket, "7", "spare", self.tree(files), 0)
        self.fake.faults = [("part-0", Trickle(after=1000))]
        limits = artifacts.Limits(
            slow_seconds=0.5, watch_seconds=0.02, smallest_range_bytes=4096, spare_requests=0, backoff_jitter=0
        )
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            artifacts.download(self.fake.client(limits), "7", "spare", "", self.work / "out")
        self.assertEqual(self.extracted(self.work / "out"), files)
        self.assertNotIn("ranges at once", output.getvalue())
        self.assertRegex(output.getvalue(), r"part-0 from 0 B: attempt 1 failed with too slow: .*; resuming at ")

    def test_a_body_that_stalls_is_cut_off_and_keeps_what_arrived(self) -> None:
        files = {"a.bin": os.urandom(5000)}
        self.stored("stalled", files)
        self.fake.faults = [("part-0", Stall(after=1000, seconds=5))]
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            limits = artifacts.Limits(slow_seconds=0.5, watch_seconds=0.02, backoff_jitter=0)
            artifacts.download(self.fake.client(limits), "7", "stalled", "", self.work / "out")
        self.assertEqual(self.extracted(self.work / "out"), files)
        self.assertEqual(self.ranges_of("part-0"), ["bytes=0-4095", "bytes=1000-4095"])
        self.assertRegex(
            output.getvalue(),
            r"part-0 from 0 B: attempt 1 failed with too slow: below 256\.0 KiB/s for \d+ s; resuming at 1000 B in 2\.0 s",
        )

    def test_a_body_that_stalls_past_the_idle_limit_is_tried_again(self) -> None:
        files = {"a.bin": os.urandom(5000)}
        self.stored("idle", files)
        self.fake.faults = [("part-0", Stall(after=1000, seconds=5))]
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            bucket = self.fake.client(artifacts.Limits(idle_seconds=0.5, backoff_jitter=0))
            artifacts.download(bucket, "7", "idle", "", self.work / "out")
        self.assertEqual(self.extracted(self.work / "out"), files)
        self.assertIn("part-0 from 0 B: attempt 1 failed with nothing arrived for 0.5 s; ", output.getvalue())

    def test_an_answer_that_never_comes_is_asked_for_again(self) -> None:
        files = {"a.bin": os.urandom(5000)}
        bucket = self.fake.client(artifacts.Limits(idle_seconds=0.5, backoff_jitter=0))
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.fake.faults = [("part-1", Silence(seconds=2))]
            artifacts.upload(bucket, "7", "unanswered", self.tree(files), 0, part_bytes=4096)
            self.fake.faults = [("part-0", Silence(seconds=2))]
            artifacts.download(bucket, "7", "unanswered", "", self.work / "out")
        self.assertEqual(self.extracted(self.work / "out"), files)
        retried = re.findall(r"(storing|reading) \S+/part-(\d)(?: from 0 B)?: attempt 1 failed with nothing moved for 0\.5 s; retrying", output.getvalue())
        self.assertEqual(retried, [("storing", "1"), ("reading", "0")])

    def test_an_interrupted_transfer_reports_how_far_it_got(self) -> None:
        output = io.StringIO()
        with mock.patch.object(artifacts, "wait", side_effect=KeyboardInterrupt), contextlib.redirect_stdout(output):
            with self.assertRaises(KeyboardInterrupt):
                artifacts.upload(self.bucket, "7", "cancelled", self.tree({"a": b"1"}), 0)
        self.assertRegex(
            output.getvalue(),
            r"::error title=CI artifact::interrupted: cancelled: sent .+ of 10\.0 KiB \(\d+%\) in 0:00, .+; parts: ",
        )
        self.assertNotIn("runs/7/cancelled/manifest.json", self.fake.objects)

    def test_a_damaged_part_is_fetched_again_from_its_start(self) -> None:
        files = {"a.bin": os.urandom(4000)}
        with self.quiet():
            artifacts.upload(self.bucket, "7", "checked", self.tree(files), 0, part_bytes=1024)
            self.fake.corrupted = ["part-1"]
            artifacts.download(self.bucket, "7", "checked", "", self.work / "out")
        self.assertEqual(self.extracted(self.work / "out"), files)
        self.assertEqual(self.ranges_of("part-1"), ["bytes=0-1023", "bytes=0-1023"])

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

    def test_a_terminating_signal_interrupts_a_command_as_a_cancellation_does(self) -> None:
        before = signal.getsignal(signal.SIGTERM)
        handlers = []
        with mock.patch.object(artifacts, "run_upload", side_effect=lambda _: handlers.append(signal.getsignal(signal.SIGTERM))):
            status, _ = self.run_command("upload", "--run", "7", "--name", "n", "--path", "unused")
        self.assertEqual(status, 0)
        self.assertEqual(handlers, [signal.default_int_handler])
        self.assertIs(signal.getsignal(signal.SIGTERM), before)

    def test_an_interrupted_command_exits_as_interrupted(self) -> None:
        with mock.patch.object(artifacts, "run_upload", side_effect=KeyboardInterrupt):
            status, _ = self.run_command("upload", "--run", "7", "--name", "n", "--path", "unused")
        self.assertEqual(status, 130)


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

    def test_the_actions_make_the_script_their_steps_process(self) -> None:
        # The signals that stop a cancelled or timed out step reach its own process, which has to
        # be the script for an interrupted transfer to report how far it got.
        for action in ACTIONS:
            text = (ROOT / action / "action.yml").read_text(encoding="utf-8")
            run = text.split("run: >-\n", 1)[1]
            self.assertTrue(run.lstrip().startswith('exec python3 "${GITHUB_WORKSPACE}/scripts/ci_artifacts.py" '), action)


if __name__ == "__main__":
    unittest.main()
