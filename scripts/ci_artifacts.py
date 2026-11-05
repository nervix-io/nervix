#!/usr/bin/env python3

"""Keep a CI run's test archives in a Cloudflare R2 bucket.

CI's jobs exchange files through GitHub's artifact service, with one exception: the archives
`scenarios-build` hands to the scenario shards and the `tests` job. A test job has nothing to run
without its archive, so they pass through a bucket that fails independently of GitHub.
`.github/actions/ci-artifact-upload` and `.github/actions/ci-artifact-download` call this script
for those steps, and take the inputs of GitHub's upload and download actions.

`upload --name NAME --path PATHS` packs the files PATHS selects into a tar stream, gzipped unless
`--compression-level 0` says the files are compressed already, cut into parts of 128 MiB. Each part
is sent and retried on its own, a few at a time, so a failure costs one part rather than the
artifact. A manifest naming every part's size and SHA-256 is written last, so an artifact either
has a manifest and every part or does not exist: a job cancelled mid-upload leaves nothing a reader
can find. Parts sit below an id of their own upload, so a rerun's upload replaces the artifact at
the moment its manifest lands, and a reader never mixes parts of two uploads.

`download --name NAME --path DIR` extracts one artifact into DIR and fails if it does not exist.
`download --pattern GLOB --path DIR` extracts every artifact whose name matches into DIR/<name>, and
a pattern that matches nothing is not an error, because a job that did not run uploaded nothing.
Each request for a part asks for the bytes after those it already holds, so an attempt that was cut
off resumes where it stopped. Every part is checked against its manifest entry once it is whole,
and fetched again from its start if it differs.

Every wait of a transfer is bounded, and a transfer says how it is going. A connection that does
not open within 20 s, one that moves no byte for 60 s, and a body that moves less than 256 KiB/s
over 30 s are cut off and tried again: R2 sometimes serves one object at a few KiB/s for many
minutes, to every reader at once, before it drops them, while a new request usually moves at full
speed. Each retry waits longer than the last, from 2 s doubling to 30 s, lengthened by up to half at
random so that parts failing together are not retried together. Six failures in a row fail the
transfer, but a download attempt that kept bytes starts a new count, so a part that keeps moving,
however slowly, is bounded only by the job's time limit. While parts move, a line every ten seconds
reports the bytes moved, the current rate, how many parts are done, moving, waiting to retry or
queued, and the slowest part moving. When one part fails the others stop, and a transfer that is
interrupted, as a job's cancellation or time limit does, reports how far it got as its error.

PATHS is one pattern a line, as the replaced action read them, because what a job reads back
depends on these rules. A pattern is relative to the working directory. `**` matches any depth, and
a directory selects everything below it. A later pattern starting with `!` removes what earlier ones
selected. A file or directory whose name starts with a dot is skipped. Each entry is stored relative
to one root: the common ancestor of the patterns' literal prefixes, or the single file's directory
when one literal path selected one file, or the one literal prefix.

A manifest lives at `runs/<run id>/<name>/manifest.json`, and its parts at
`<prefix>/<run id>/<name>/<upload id>/part-N`. A run id is shared by every attempt of a run, so an
artifact survives a rerun of other jobs, and a job that runs again replaces its own. Retention is
the bucket's lifecycle rules, which match key prefixes: `--retention standard` keeps parts below
`runs/`, and `--retention transfer` below `transfer/`, for a large artifact only later jobs of the
run read. The manifest names the prefix, so a reader needs no option, and a reader of an artifact
whose parts expired is told so. The credentials are the environment's R2_ARTIFACTS_* variables, and
developing-nervix.md lists them.
"""

from __future__ import annotations

import argparse
import collections
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait
import contextlib
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
import enum
import fnmatch
import gzip
import hashlib
import hmac
import http.client
import json
import os
from pathlib import Path
import random
import re
import signal
import sys
import tarfile
import tempfile
import threading
import time
from types import TracebackType
from typing import BinaryIO, TypeVar
from urllib.parse import quote, urlsplit
import uuid
from xml.etree import ElementTree

PART_BYTES = 128 * 1024 * 1024
CONCURRENT_TRANSFERS = 8
# A body moves a block at a time: an upload reads its part file this much at once, and a download
# takes at most this much from one read of its connection. A body's pace is checked block by block.
BLOCK_BYTES = 256 * 1024
# How long opening a connection may take, its TLS handshake included.
CONNECT_SECONDS = 20
# How long a connection may move no byte at all, waiting for an answer or inside a body.
IDLE_SECONDS = 60
# A body that moves less than SLOWEST_BYTES_PER_SECOND over SLOW_SECONDS is cut off, and its part
# tried again. A runner's healthy connection to R2 moves 1 to 12 MiB/s, but R2 sometimes serves one
# object at a few KiB/s for many minutes, to every reader at once, before it drops them.
SLOWEST_BYTES_PER_SECOND = 256 * 1024
SLOW_SECONDS = 30
# Six failures in a row wait 2, 4, 8, 16 and 30 seconds between them, each lengthened by up to half
# at random so that parts failing together are not retried together: at least a minute of a store
# being away.
TRANSFER_ATTEMPTS = 6
FIRST_BACKOFF_SECONDS = 2
LONGEST_BACKOFF_SECONDS = 30
BACKOFF_JITTER = 0.5
# How often a transfer of parts reports how far they got.
REPORT_SECONDS = 10
GZIP_DEFAULT_LEVEL = 1
# The key prefix of an upload's parts, which the bucket's lifecycle rules match.
PART_PREFIXES = {"standard": "runs", "transfer": "transfer"}
# R2 has one region, and SigV4 names it `auto`.
REGION = "auto"
SERVICE = "s3"
# The payload is covered by TLS and by the manifest's SHA-256, so it is not hashed a second time.
UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD"
TRANSIENT_STATUSES = frozenset({408, 429})
ENVIRONMENT = {
    "name": "R2_ARTIFACTS_BUCKET",
    "endpoint": "R2_ARTIFACTS_ENDPOINT",
    "access_key": "R2_ARTIFACTS_ACCESS_KEY_ID",
    "secret_key": "R2_ARTIFACTS_SECRET_ACCESS_KEY",
}
# Lines come from every thread moving a part, and each one is printed whole.
OUTPUT = threading.Lock()

T = TypeVar("T")


class ArtifactError(Exception):
    """An artifact could not be stored or read, for a reason a retry does not change."""


class Missing(Exception):
    """The bucket holds no such object."""


class Transient(Exception):
    """A transfer failed in a way the next attempt may not."""


class Stopped(Exception):
    """A part stops moving, because another part of its transfer failed or the transfer was
    interrupted."""


@dataclass(frozen=True)
class Limits:
    """How long a request may wait or crawl, how it is retried, and how often a transfer reports."""

    connect_seconds: float = CONNECT_SECONDS
    idle_seconds: float = IDLE_SECONDS
    slowest_bytes_per_second: int = SLOWEST_BYTES_PER_SECOND
    slow_seconds: float = SLOW_SECONDS
    attempts: int = TRANSFER_ATTEMPTS
    first_backoff_seconds: float = FIRST_BACKOFF_SECONDS
    longest_backoff_seconds: float = LONGEST_BACKOFF_SECONDS
    backoff_jitter: float = BACKOFF_JITTER
    report_seconds: float = REPORT_SECONDS

    def backoff(self, failures: int) -> float:
        """The wait after `failures` failed attempts in a row."""
        doubled = self.first_backoff_seconds * 2 ** (failures - 1)
        return min(doubled, self.longest_backoff_seconds) * random.uniform(1, 1 + self.backoff_jitter)


def say(line: str) -> None:
    """Print one line of the log, whole, whichever thread prints it."""
    with OUTPUT:
        print(line, flush=True)


def size(count: float) -> str:
    """A byte count in binary units, as a report prints it."""
    if count < 1024:
        return f"{count:.0f} B"
    scaled = count / 1024
    for unit in ("KiB", "MiB", "GiB"):
        if scaled < 1024:
            return f"{scaled:.1f} {unit}"
        scaled /= 1024
    return f"{scaled:.1f} TiB"


def duration(seconds: float) -> str:
    """An elapsed time as minutes and seconds, and hours when it has any."""
    minutes, whole_seconds = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}:{minutes:02}:{whole_seconds:02}"
    return f"{minutes}:{whole_seconds:02}"


def describe(error: BaseException) -> str:
    """The cause of a failed attempt, as its log line names it."""
    if isinstance(error, Transient):
        return str(error)
    if str(error):
        return f"{type(error).__name__}: {error}"
    return type(error).__name__


class Phase(enum.Enum):
    """Where one part of a transfer stands."""

    QUEUED = "queued"
    MOVING = "moving"
    WAITING = "waiting to retry"
    DONE = "done"


class Progress:
    """How far the parts of one artifact got. The threads moving the parts update it, and the
    thread that started them reports it."""

    def __init__(
        self,
        name: str,
        verb: str,
        lengths: Sequence[int],
        limits: Limits,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.name = name
        self.verb = verb
        self.lengths = tuple(lengths)
        self.total = sum(self.lengths)
        self.limits = limits
        self.clock = clock
        self.stopping = threading.Event()
        self.lock = threading.Lock()
        self.moved = [0] * len(self.lengths)
        self.phases = [Phase.QUEUED] * len(self.lengths)
        self.failed_attempts = 0
        self.started = clock()
        # What the previous report saw, which only the reporting thread reads and writes.
        self.reported_at = self.started
        self.reported = [0] * len(self.lengths)

    def part(self, index: int) -> PartProgress:
        return PartProgress(self, index)

    def enter(self, index: int, phase: Phase) -> None:
        with self.lock:
            self.phases[index] = phase
            if phase is Phase.WAITING:
                self.failed_attempts += 1
            if phase is Phase.DONE:
                self.moved[index] = self.lengths[index]

    def add(self, index: int, count: int) -> None:
        with self.lock:
            self.moved[index] += count

    def reset(self, index: int) -> None:
        with self.lock:
            self.moved[index] = 0

    @staticmethod
    def rate(moved: int, reported: int, interval: float) -> float:
        """How fast a part moved since the previous report. A part that started again since then
        moved all it holds within the interval."""
        if interval <= 0:
            return 0.0
        if moved < reported:
            return moved / interval
        return (moved - reported) / interval

    def report(self) -> str:
        """How far the parts got since the transfer started, how fast they moved since the previous
        report, and which moving part is the slowest."""
        now = self.clock()
        with self.lock:
            moved = list(self.moved)
            phases = list(self.phases)
        interval = now - self.reported_at
        rates = [self.rate(current, previous, interval) for current, previous in zip(moved, self.reported, strict=True)]
        self.reported_at = now
        self.reported = moved
        held = sum(moved)
        share = held / self.total if self.total else 1.0
        counts = collections.Counter(phases)
        line = (
            f"{self.name}: {self.verb} {size(held)} of {size(self.total)} ({share:.0%}) in "
            f"{duration(now - self.started)}, {size(sum(rates))}/s; parts: {counts[Phase.DONE]} done, "
            f"{counts[Phase.MOVING]} moving, {counts[Phase.WAITING]} waiting to retry, "
            f"{counts[Phase.QUEUED]} queued"
        )
        moving = [index for index, phase in enumerate(phases) if phase is Phase.MOVING]
        if not moving:
            return line
        slowest = min(moving, key=rates.__getitem__)
        return (
            f"{line}; slowest part-{slowest}: {size(moved[slowest])} of {size(self.lengths[slowest])} "
            f"at {size(rates[slowest])}/s"
        )

    def finish(self) -> None:
        """Print the line a finished transfer ends with. A transfer some of whose attempts failed
        says so as a warning, so that a store that slowed a job down shows in the run's summary."""
        elapsed = self.clock() - self.started
        speed = self.total / elapsed if elapsed > 0 else 0.0
        line = (
            f"{self.name}: {self.verb} {size(self.total)} in {len(self.lengths)} parts in "
            f"{duration(elapsed)}, {size(speed)}/s"
        )
        if self.failed_attempts:
            say(f"::warning title=CI artifact::{line}, after {self.failed_attempts} failed attempts")
            return
        say(line)


@dataclass(frozen=True)
class PartProgress:
    """One part's handle on the progress of its transfer."""

    progress: Progress
    index: int

    @property
    def stopping(self) -> threading.Event:
        return self.progress.stopping

    def enter(self, phase: Phase) -> None:
        self.progress.enter(self.index, phase)

    def add(self, count: int) -> None:
        self.progress.add(self.index, count)

    def reset(self) -> None:
        self.progress.reset(self.index)


def pause(stopping: threading.Event, seconds: float) -> None:
    """Wait out a backoff, unless the transfer stops first."""
    if stopping.wait(seconds):
        raise Stopped()


def with_retries(
    what: str,
    operation: Callable[[], T],
    limits: Limits,
    part: PartProgress | None = None,
    kept: Callable[[], int] | None = None,
) -> T:
    """Run a request until it succeeds, fails for a reason a retry does not change, or fails
    `limits.attempts` times in a row. `kept` counts the bytes the attempts have kept for the next
    one to continue from, and an attempt that kept more starts a new count of failures in a row."""
    stopping = threading.Event() if part is None else part.stopping
    failures = 0
    attempt = 0
    while True:
        if stopping.is_set():
            raise Stopped()
        attempt += 1
        before = 0 if kept is None else kept()
        if part is not None:
            part.enter(Phase.MOVING)
        try:
            return operation()
        except (OSError, http.client.HTTPException, Transient) as error:
            if stopping.is_set():
                raise Stopped() from error
            after = 0 if kept is None else kept()
            if after > before:
                failures = 1
            else:
                failures += 1
            cause = describe(error)
            if failures >= limits.attempts:
                raise ArtifactError(f"{what} failed {failures} times in a row, last with {cause}") from error
            delay = limits.backoff(failures)
            if after:
                step = f"resuming at {size(after)}"
            else:
                step = "retrying"
            say(f"{what}: attempt {attempt} failed with {cause}; {step} in {delay:.1f} s")
            if part is not None:
                part.enter(Phase.WAITING)
            pause(stopping, delay)


class Meter:
    """The pace of one attempt's body, which must move `slowest_bytes_per_second` for each of every
    `slow_seconds`. An attempt whose body falls behind is cut off as too slow."""

    def __init__(
        self, limits: Limits, stopping: threading.Event, clock: Callable[[], float] = time.monotonic
    ) -> None:
        self.limits = limits
        self.stopping = stopping
        self.clock = clock
        self.quota = limits.slowest_bytes_per_second * limits.slow_seconds
        self.since = clock()
        self.moved = 0

    def count(self, moved: int) -> None:
        """Count a block that moved. The attempt stops when its transfer is stopping, and when the
        bytes moved since the body last met its pace fall short of it for `slow_seconds`."""
        if self.stopping.is_set():
            raise Stopped()
        now = self.clock()
        self.moved += moved
        if self.moved >= self.quota:
            self.since = now
            self.moved = 0
            return
        elapsed = now - self.since
        if elapsed >= self.limits.slow_seconds:
            raise Transient(
                f"too slow: {size(self.moved)} in {elapsed:.0f} s, "
                f"below {size(self.limits.slowest_bytes_per_second)}/s"
            )


class MeteredBody:
    """A part file as http.client reads it to send it, a block at a time. A block counts as sent
    when http.client asks for the next one, which it does once it has sent the last."""

    def __init__(
        self,
        file: BinaryIO,
        part: PartProgress,
        limits: Limits,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.file = file
        self.part = part
        self.limits = limits
        self.clock = clock
        self.meter: Meter | None = None
        self.unsent = 0

    def read(self, amount: int = -1) -> bytes:
        if self.meter is None:
            self.meter = Meter(self.limits, self.part.stopping, self.clock)
        if self.unsent:
            self.part.add(self.unsent)
            self.meter.count(self.unsent)
        block = self.file.read(amount)
        self.unsent = len(block)
        return block


def error_code(body: bytes) -> str:
    """The code an S3 error document names, which tells a refused token from a bad signature."""
    try:
        document = ElementTree.fromstring(body)
    except ElementTree.ParseError:
        return "without an error document"
    return document.findtext("Code") or "without an error code"


def signing_key(secret_key: str, date: str, region: str) -> bytes:
    key = f"AWS4{secret_key}".encode()
    for part in (date, region, SERVICE, "aws4_request"):
        key = hmac.new(key, part.encode(), hashlib.sha256).digest()
    return key


def authorization(
    access_key: str,
    secret_key: str,
    region: str,
    method: str,
    path: str,
    query: str,
    headers: Mapping[str, str],
    payload: str,
) -> str:
    """The SigV4 Authorization header of a request. `headers` are the signed ones, which include
    `x-amz-date`; `query` is already in canonical order and encoding."""
    names = sorted(name.lower() for name in headers)
    lowered = {name.lower(): value.strip() for name, value in headers.items()}
    canonical_headers = "".join(f"{name}:{lowered[name]}\n" for name in names)
    signed_headers = ";".join(names)
    canonical = "\n".join((method, path, query, canonical_headers, signed_headers, payload))
    stamp = lowered["x-amz-date"]
    date = stamp[:8]
    scope = f"{date}/{region}/{SERVICE}/aws4_request"
    digest = hashlib.sha256(canonical.encode()).hexdigest()
    to_sign = "\n".join(("AWS4-HMAC-SHA256", stamp, scope, digest))
    signature = hmac.new(signing_key(secret_key, date, region), to_sign.encode(), hashlib.sha256)
    return (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{scope},"
        f"SignedHeaders={signed_headers},Signature={signature.hexdigest()}"
    )


@dataclass(frozen=True)
class Part:
    length: int
    sha256: str


class ReceivedPart:
    """One part as its attempts receive it. Each attempt asks for the bytes after those kept, so one
    that was cut off resumes where it stopped. The part's manifest entry judges it once it is whole,
    and a part that does not match is received again from its start."""

    def __init__(self, path: Path, part: Part, progress: PartProgress) -> None:
        self.part = part
        self.progress = progress
        self.output = path.open("wb", buffering=BLOCK_BYTES)
        self.digest = hashlib.sha256()
        self.received = 0

    def __enter__(self) -> ReceivedPart:
        return self

    def __exit__(
        self,
        kind: type[BaseException] | None,
        error: BaseException | None,
        trace: TracebackType | None,
    ) -> None:
        self.output.close()

    def kept(self) -> int:
        return self.received

    def complete(self) -> bool:
        """Whether the part holds every byte its manifest entry counts, which an attempt cut off
        after its last block leaves for the next one to check."""
        return self.received >= self.part.length

    def wanted(self) -> str:
        """The byte range the next attempt asks for: everything after the bytes kept."""
        return f"bytes={self.received}-"

    def accept(self, response: http.client.HTTPResponse) -> None:
        """Take a response's body as the rest of the part, or as the whole part when the store
        answered with all of it."""
        if response.status != 206:
            self.restart()
        wanted = self.part.length - self.received
        if response.length is not None and response.length != wanted:
            self.restart()
            raise Transient(f"{response.length} bytes on offer where {wanted} were asked for")

    def write(self, block: bytes) -> None:
        self.output.write(block)
        self.digest.update(block)
        self.received += len(block)
        self.progress.add(len(block))

    def finish(self) -> None:
        """Check the whole part against its manifest entry."""
        if self.received < self.part.length:
            raise Transient(f"the body ended at {self.received} of {self.part.length} bytes")
        if self.received == self.part.length and self.digest.hexdigest() == self.part.sha256:
            self.output.flush()
            return
        received = self.received
        self.restart()
        raise Transient(f"{received} bytes that do not match the manifest")

    def restart(self) -> None:
        self.output.seek(0)
        self.output.truncate()
        self.digest = hashlib.sha256()
        self.received = 0
        self.progress.reset()


@dataclass(frozen=True)
class Bucket:
    """The artifacts' bucket, addressed by path below its account's endpoint."""

    name: str
    endpoint: str
    access_key: str
    secret_key: str
    limits: Limits = field(default_factory=Limits)

    @classmethod
    def from_environment(cls, environment: Mapping[str, str]) -> Bucket:
        # A pasted setting easily keeps a line ending, which changes every signature.
        values = {field: environment.get(name, "").strip() for field, name in ENVIRONMENT.items()}
        missing = [name for field, name in ENVIRONMENT.items() if not values[field]]
        if missing:
            raise ArtifactError(f"{', '.join(missing)} must be set to reach the artifacts' bucket")
        if not urlsplit(values["endpoint"]).netloc:
            raise ArtifactError("R2_ARTIFACTS_ENDPOINT must be a URL, starting with https://")
        return cls(**values)

    @contextlib.contextmanager
    def exchange(
        self,
        method: str,
        key: str,
        query: str = "",
        body: bytes | BinaryIO | MeteredBody | None = None,
        length: int = 0,
        headers: Mapping[str, str] | None = None,
    ) -> Iterator[http.client.HTTPResponse]:
        """One signed request, whose response is open inside the block; `headers` are signed with
        it. A status a retry can change raises Transient, and so do a connection that does not open
        in time and one that moves nothing for too long; an absent object raises Missing."""
        endpoint = urlsplit(self.endpoint)
        path = "/" + quote(f"{self.name}/{key}" if key else self.name, safe="/-_.~")
        signed = {
            "host": endpoint.netloc,
            "x-amz-content-sha256": UNSIGNED_PAYLOAD,
            "x-amz-date": datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"),
        }
        if headers:
            signed.update(headers)
        request_headers = dict(signed)
        request_headers["Authorization"] = authorization(
            self.access_key, self.secret_key, REGION, method, path, query, signed, UNSIGNED_PAYLOAD
        )
        if method == "PUT":
            request_headers["Content-Length"] = str(length)
        kind = http.client.HTTPSConnection if endpoint.scheme == "https" else http.client.HTTPConnection
        connection = kind(endpoint.netloc, timeout=self.limits.connect_seconds, blocksize=BLOCK_BYTES)
        try:
            try:
                connection.connect()
            except TimeoutError as error:
                raise Transient(f"no connection within {self.limits.connect_seconds:g} s") from error
            connection.sock.settimeout(self.limits.idle_seconds)
            target = f"{path}?{query}" if query else path
            try:
                connection.request(method, target, body=body, headers=request_headers)
                response = connection.getresponse()
            except TimeoutError as error:
                raise Transient(f"nothing moved for {self.limits.idle_seconds:g} s") from error
            if response.status == 404:
                raise Missing(key)
            if response.status in TRANSIENT_STATUSES or response.status >= 500:
                raise Transient(f"status {response.status}")
            if response.status >= 300:
                reason = error_code(response.read())
                raise ArtifactError(f"{method} of {key or 'the bucket'} answered {response.status} {reason}")
            yield response
        finally:
            connection.close()

    def put_bytes(self, key: str, data: bytes) -> None:
        def attempt() -> None:
            with self.exchange("PUT", key, body=data, length=len(data)) as response:
                response.read()

        with_retries(f"storing {key}", attempt, self.limits)

    def put_file(self, key: str, path: Path, length: int, progress: PartProgress) -> None:
        """Send one part, from its start on every attempt."""

        def attempt() -> None:
            progress.reset()
            with path.open("rb") as file:
                body = MeteredBody(file, progress, self.limits)
                with self.exchange("PUT", key, body=body, length=length) as response:
                    response.read()

        with_retries(f"storing {key}", attempt, self.limits, progress)
        progress.enter(Phase.DONE)

    def get_bytes(self, key: str) -> bytes:
        def attempt() -> bytes:
            with self.exchange("GET", key) as response:
                return response.read()

        return with_retries(f"reading {key}", attempt, self.limits)

    def get_file(self, key: str, path: Path, part: Part, progress: PartProgress) -> None:
        """Fetch one part, resuming an attempt that was cut off where it stopped, and fetching the
        part again from its start if it does not match its manifest entry."""
        with ReceivedPart(path, part, progress) as received:

            def attempt() -> None:
                if not received.complete():
                    with self.exchange("GET", key, headers={"range": received.wanted()}) as response:
                        received.accept(response)
                        meter = Meter(self.limits, progress.stopping)
                        try:
                            while block := response.read1(BLOCK_BYTES):
                                received.write(block)
                                meter.count(len(block))
                        except TimeoutError as error:
                            raise Transient(f"nothing arrived for {self.limits.idle_seconds:g} s") from error
                received.finish()

            with_retries(f"reading {key}", attempt, self.limits, progress, received.kept)
        progress.enter(Phase.DONE)

    def list_page(self, query: str) -> bytes:
        def attempt() -> bytes:
            with self.exchange("GET", "", query) as response:
                return response.read()

        return with_retries("listing the run's artifacts", attempt, self.limits)

    def artifact_names(self, run: str) -> list[str]:
        """The artifacts of a run: every name below its prefix, page by page."""
        prefix = f"runs/{run}/"
        names: list[str] = []
        token = ""
        while True:
            parameters = {"delimiter": "/", "list-type": "2", "prefix": prefix}
            if token:
                parameters["continuation-token"] = token
            encoded = [f"{name}={quote(parameters[name], safe='-_.~')}" for name in sorted(parameters)]
            query = "&".join(encoded)
            page = ElementTree.fromstring(self.list_page(query))
            token = ""
            for element in page.iter():
                tag = element.tag.rsplit("}", 1)[-1]
                if tag == "NextContinuationToken":
                    token = element.text or ""
                elif tag == "Prefix" and element.text and element.text != prefix:
                    names.append(element.text.removeprefix(prefix).rstrip("/"))
            if not token:
                return names


def move_parts(progress: Progress, move: Callable[[PartProgress], None]) -> None:
    """Move every part, CONCURRENT_TRANSFERS at a time, and report how far they got every
    `report_seconds` until the last one is done. The first part to fail stops the others: none
    starts after it, and those moving stop at their next block or backoff."""

    def guarded(part: PartProgress) -> None:
        try:
            move(part)
        except Stopped:
            return
        except BaseException:
            progress.stopping.set()
            raise

    pool = ThreadPoolExecutor(CONCURRENT_TRANSFERS)
    try:
        pending = {pool.submit(guarded, progress.part(index)) for index in range(len(progress.lengths))}
        while pending:
            finished, pending = wait(pending, timeout=progress.limits.report_seconds, return_when=FIRST_EXCEPTION)
            for future in finished:
                future.result()
            if pending:
                say(progress.report())
    except KeyboardInterrupt:
        progress.stopping.set()
        # How far a cancelled or timed out job got, in its run's summary.
        say(f"::error title=CI artifact::interrupted: {progress.report()}")
        raise
    except BaseException:
        progress.stopping.set()
        say(f"{progress.report()}; stopping the other parts")
        raise
    finally:
        pool.shutdown(wait=True, cancel_futures=True)


def segment_regex(segment: str) -> str:
    """The regular expression of one path segment of a pattern."""
    parts: list[str] = []
    index = 0
    while index < len(segment):
        character = segment[index]
        closing = segment.find("]", index + 1) if character == "[" else -1
        if character == "*":
            parts.append("[^/]*")
        elif character == "?":
            parts.append("[^/]")
        elif closing != -1:
            parts.append(segment[index : closing + 1])
            index = closing
        else:
            parts.append(re.escape(character))
        index += 1
    return "".join(parts)


@dataclass(frozen=True)
class Pattern:
    """One line of an artifact's paths."""

    negated: bool
    search_path: str
    expression: re.Pattern[str]

    @classmethod
    def parse(cls, line: str, working_directory: str) -> Pattern:
        negated = line.startswith("!")
        text = line[1:] if negated else line
        segments = os.path.normpath(os.path.join(working_directory, text)).split("/")
        literal: list[str] = []
        for segment in segments:
            if "*" in segment or "?" in segment or "[" in segment:
                break
            literal.append(segment)
        regex = ""
        for index, segment in enumerate(segments):
            last = index == len(segments) - 1
            if segment == "**":
                regex += ".*" if last else "(?:[^/]*/)*"
            else:
                regex += segment_regex(segment) + ("" if last else "/")
        # A pattern that names a directory selects everything below it.
        expression = re.compile(rf"{regex}(?:/.*)?\Z", re.DOTALL)
        return cls(negated=negated, search_path="/".join(literal) or "/", expression=expression)


def files_below(search_path: str) -> Iterator[str]:
    """Every file at or below a path, following links once, without hidden names."""
    if os.path.basename(search_path).startswith("."):
        return
    if os.path.isfile(search_path):
        yield search_path
        return
    visited: set[str] = set()
    for directory, directories, files in os.walk(search_path, followlinks=True):
        real = os.path.realpath(directory)
        if real in visited:
            directories.clear()
            continue
        visited.add(real)
        directories[:] = [name for name in directories if not name.startswith(".")]
        for name in files:
            path = os.path.join(directory, name)
            if not name.startswith(".") and os.path.isfile(path):
                yield path


@dataclass(frozen=True)
class Selection:
    """The files an artifact holds, and the directory their stored paths are relative to."""

    root: str
    files: tuple[str, ...]

    @classmethod
    def of(cls, lines: Sequence[str], working_directory: str) -> Selection:
        patterns = [Pattern.parse(line, working_directory) for line in lines]
        literals = {pattern.search_path for pattern in patterns if not pattern.negated}
        if not literals:
            raise ArtifactError("an artifact's paths must include at least one pattern")
        outermost = sorted(
            path
            for path in literals
            if not any(path != other and path.startswith(f"{other}/") for other in literals)
        )
        selected: set[str] = set()
        for search_path in outermost:
            for path in files_below(search_path):
                matched = False
                for pattern in patterns:
                    if pattern.expression.match(path):
                        matched = not pattern.negated
                if matched:
                    selected.add(path)
        files = tuple(sorted(selected))
        if len(outermost) > 1:
            root = os.path.commonpath(outermost)
        elif len(files) == 1 and files[0] == outermost[0]:
            root = os.path.dirname(files[0])
        else:
            root = outermost[0]
        return cls(root=root, files=files)


class PartWriter:
    """Cuts one written stream into files of at most `part_bytes`."""

    def __init__(self, directory: Path, part_bytes: int) -> None:
        self.directory = directory
        self.part_bytes = part_bytes
        self.parts: list[Part] = []
        self.output: BinaryIO | None = None
        self.digest = hashlib.sha256()
        self.written = 0

    def path(self, index: int) -> Path:
        return self.directory / f"part-{index}"

    def write(self, data: bytes) -> int:
        view = memoryview(data)
        while view:
            if self.output is None:
                self.output = self.path(len(self.parts)).open("wb")
                self.digest = hashlib.sha256()
                self.written = 0
            room = self.part_bytes - self.written
            block = view[:room]
            self.output.write(block)
            self.digest.update(block)
            self.written += len(block)
            view = view[room:]
            if self.written == self.part_bytes:
                self.seal()
        return len(data)

    def flush(self) -> None:
        return None

    def seal(self) -> None:
        if self.output is not None:
            self.output.close()
            self.output = None
            self.parts.append(Part(length=self.written, sha256=self.digest.hexdigest()))


class PartReader:
    """Reads part files in order as one stream."""

    def __init__(self, paths: Sequence[Path]) -> None:
        self.paths = iter(paths)
        self.current: BinaryIO | None = None

    def read(self, size: int = -1) -> bytes:
        while True:
            if self.current is None:
                path = next(self.paths, None)
                if path is None:
                    return b""
                self.current = path.open("rb")
            block = self.current.read(size)
            if block:
                return block
            self.close()

    def close(self) -> None:
        if self.current is not None:
            self.current.close()
            self.current = None


def write_archive(selection: Selection, stream: gzip.GzipFile | PartWriter) -> int:
    """Write the selection as a tar stream, and return how many files it holds."""
    packed = 0
    with tarfile.open(fileobj=stream, mode="w|", dereference=True) as archive:
        for path in selection.files:
            try:
                archive.add(path, arcname=os.path.relpath(path, selection.root), recursive=False)
            except FileNotFoundError:
                say(f"::warning title=CI artifact::{path} went away before it was packed")
                continue
            packed += 1
    return packed


def pack(selection: Selection, writer: PartWriter, compression_level: int) -> int:
    """Pack the selection into the writer's parts, compressed unless the level is zero."""
    if compression_level == 0:
        packed = write_archive(selection, writer)
    else:
        with gzip.GzipFile(fileobj=writer, mode="wb", compresslevel=compression_level, mtime=0) as stream:
            packed = write_archive(selection, stream)
    writer.seal()
    return packed


def transfer_directory() -> tempfile.TemporaryDirectory[str]:
    return tempfile.TemporaryDirectory(prefix="ci-artifact-", dir=os.environ.get("RUNNER_TEMP"))


def manifest_key(run: str, name: str) -> str:
    return f"runs/{run}/{name}/manifest.json"


def upload(
    bucket: Bucket,
    run: str,
    name: str,
    selection: Selection,
    compression_level: int,
    retention: str = "standard",
    part_bytes: int = PART_BYTES,
) -> None:
    prefix = PART_PREFIXES[retention]
    with transfer_directory() as directory:
        writer = PartWriter(Path(directory), part_bytes)
        files = pack(selection, writer, compression_level)
        lengths = [part.length for part in writer.parts]
        say(f"{name}: {files} files, {sum(lengths)} bytes in {len(lengths)} parts")
        upload_id = uuid.uuid4().hex
        progress = Progress(name, "sent", lengths, bucket.limits)

        def send(part: PartProgress) -> None:
            key = f"{prefix}/{run}/{name}/{upload_id}/part-{part.index}"
            bucket.put_file(key, writer.path(part.index), lengths[part.index], part)

        move_parts(progress, send)
        manifest = {"prefix": prefix, "upload": upload_id, "parts": [asdict(part) for part in writer.parts]}
        bucket.put_bytes(manifest_key(run, name), json.dumps(manifest).encode())
    progress.finish()


def fetch(bucket: Bucket, run: str, name: str, destination: Path) -> None:
    manifest = json.loads(bucket.get_bytes(manifest_key(run, name)))
    parts = [Part(**part) for part in manifest["parts"]]
    prefix = manifest["prefix"]
    upload_id = manifest["upload"]
    destination.mkdir(parents=True, exist_ok=True)
    progress = Progress(name, "received", [part.length for part in parts], bucket.limits)
    say(f"{name}: {progress.total} bytes in {len(parts)} parts")
    with transfer_directory() as directory:
        paths = [Path(directory) / f"part-{index}" for index in range(len(parts))]

        def receive(part: PartProgress) -> None:
            key = f"{prefix}/{run}/{name}/{upload_id}/part-{part.index}"
            bucket.get_file(key, paths[part.index], parts[part.index], part)

        try:
            move_parts(progress, receive)
        except Missing as error:
            raise ArtifactError(f"{name} of run {run} has expired: its parts are gone") from error
        progress.finish()
        with contextlib.closing(PartReader(paths)) as reader:
            with tarfile.open(fileobj=reader, mode="r|*") as archive:
                archive.extractall(destination, filter="data")
    say(f"{name}: extracted into {destination}")


def download(bucket: Bucket, run: str, name: str, pattern: str, destination: Path) -> None:
    if name:
        try:
            fetch(bucket, run, name, destination)
        except Missing as error:
            raise ArtifactError(f"run {run} has no artifact {name}") from error
        return
    matching = [item for item in bucket.artifact_names(run) if fnmatch.fnmatchcase(item, pattern)]
    if not matching:
        say(f"run {run} has no artifact matching {pattern}")
    for item in matching:
        try:
            fetch(bucket, run, item, destination / item)
        except Missing:
            # Parts without a manifest are an upload that never finished: no artifact at all.
            say(f"::warning title=CI artifact::{item} of run {run} was never completed")


def parse_arguments(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    send = commands.add_parser("upload")
    send.add_argument("--run", default=os.environ.get("GITHUB_RUN_ID", ""))
    send.add_argument("--name", required=True)
    send.add_argument("--path", required=True, help="one pattern a line")
    send.add_argument("--if-no-files-found", choices=("warn", "error", "ignore"), default="warn")
    send.add_argument("--compression-level", type=int, choices=range(10), default=GZIP_DEFAULT_LEVEL)
    send.add_argument("--retention", choices=tuple(PART_PREFIXES), default="standard")
    receive = commands.add_parser("download")
    receive.add_argument("--run", default=os.environ.get("GITHUB_RUN_ID", ""))
    receive.add_argument("--name", default="")
    receive.add_argument("--pattern", default="")
    receive.add_argument("--path", default=".")
    arguments = parser.parse_args(argv)
    if arguments.command == "download" and bool(arguments.name) == bool(arguments.pattern):
        parser.error("download takes exactly one of --name and --pattern")
    return arguments


def run_upload(arguments: argparse.Namespace) -> None:
    lines = [line.strip() for line in arguments.path.splitlines() if line.strip()]
    selection = Selection.of(lines, os.getcwd())
    if selection.files:
        upload(
            Bucket.from_environment(os.environ),
            arguments.run,
            arguments.name,
            selection,
            arguments.compression_level,
            arguments.retention,
        )
        return
    message = f"{arguments.name}: no files match {' '.join(lines)}"
    if arguments.if_no_files_found == "error":
        raise ArtifactError(message)
    if arguments.if_no_files_found == "warn":
        say(f"::warning title=CI artifact::{message}")
    else:
        say(message)


def main(argv: Sequence[str] | None = None) -> int:
    arguments = parse_arguments(argv)
    # A job that is cancelled or runs out of time signals its step with SIGINT and then SIGTERM.
    # Both interrupt a transfer, which reports how far it got.
    previous = signal.signal(signal.SIGTERM, signal.default_int_handler)
    try:
        if not arguments.run:
            raise ArtifactError("GITHUB_RUN_ID or --run must name the run")
        if arguments.command == "upload":
            run_upload(arguments)
        else:
            bucket = Bucket.from_environment(os.environ)
            download(bucket, arguments.run, arguments.name, arguments.pattern, Path(arguments.path))
    except ArtifactError as error:
        say(f"::error title=CI artifact::{error}")
        return 1
    except KeyboardInterrupt:
        return 130
    finally:
        signal.signal(signal.SIGTERM, previous)
    return 0


if __name__ == "__main__":
    sys.exit(main())
