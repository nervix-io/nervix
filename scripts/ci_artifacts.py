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
Every part is checked against its manifest entry and fetched again if it differs.

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
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
import contextlib
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import fnmatch
import gzip
import hashlib
import hmac
import http.client
import json
import os
from pathlib import Path
import re
import sys
import tarfile
import tempfile
import time
from typing import BinaryIO, TypeVar
from urllib.parse import quote, urlsplit
import uuid
from xml.etree import ElementTree

PART_BYTES = 128 * 1024 * 1024
CONCURRENT_TRANSFERS = 8
# Six attempts wait 2, 4, 8, 16 and 30 seconds between them: a minute of a store being away.
TRANSFER_ATTEMPTS = 6
LONGEST_BACKOFF_SECONDS = 30
# How long a connection may make no progress at all. A transfer that progresses has no limit here;
# the job's own timeout bounds it.
SOCKET_SECONDS = 60
SOCKET_BLOCK_BYTES = 1024 * 1024
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

T = TypeVar("T")


class ArtifactError(Exception):
    """An artifact could not be stored or read, for a reason a retry does not change."""


class Missing(Exception):
    """The bucket holds no such object."""


class Transient(Exception):
    """A transfer failed in a way the next attempt may not."""


def with_retries(what: str, operation: Callable[[], T]) -> T:
    """Run a transfer until it succeeds or its attempts run out."""
    for attempt in range(1, TRANSFER_ATTEMPTS + 1):
        try:
            return operation()
        except (OSError, http.client.HTTPException, Transient) as error:
            if attempt == TRANSFER_ATTEMPTS:
                raise ArtifactError(f"{what} failed {attempt} times, last with {error}") from error
            delay = min(2**attempt, LONGEST_BACKOFF_SECONDS)
            print(f"{what}: attempt {attempt} failed with {error}; retrying in {delay} s", flush=True)
            time.sleep(delay)
    raise AssertionError("the last attempt returns or raises")


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
class Bucket:
    """The artifacts' bucket, addressed by path below its account's endpoint."""

    name: str
    endpoint: str
    access_key: str
    secret_key: str

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
        body: bytes | BinaryIO | None = None,
        length: int = 0,
    ) -> Iterator[http.client.HTTPResponse]:
        """One signed request, whose response is open inside the block. A status a retry can
        change raises Transient, and an absent object raises Missing."""
        endpoint = urlsplit(self.endpoint)
        path = "/" + quote(f"{self.name}/{key}" if key else self.name, safe="/-_.~")
        signed = {
            "host": endpoint.netloc,
            "x-amz-content-sha256": UNSIGNED_PAYLOAD,
            "x-amz-date": datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"),
        }
        headers = dict(signed)
        headers["Authorization"] = authorization(
            self.access_key, self.secret_key, REGION, method, path, query, signed, UNSIGNED_PAYLOAD
        )
        if method == "PUT":
            headers["Content-Length"] = str(length)
        kind = http.client.HTTPSConnection if endpoint.scheme == "https" else http.client.HTTPConnection
        connection = kind(endpoint.netloc, timeout=SOCKET_SECONDS, blocksize=SOCKET_BLOCK_BYTES)
        try:
            target = f"{path}?{query}" if query else path
            connection.request(method, target, body=body, headers=headers)
            response = connection.getresponse()
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

        with_retries(f"storing {key}", attempt)

    def put_file(self, key: str, path: Path, length: int) -> None:
        def attempt() -> None:
            with path.open("rb") as body, self.exchange("PUT", key, body=body, length=length) as response:
                response.read()

        with_retries(f"storing {key}", attempt)

    def get_bytes(self, key: str) -> bytes:
        def attempt() -> bytes:
            with self.exchange("GET", key) as response:
                return response.read()

        return with_retries(f"reading {key}", attempt)

    def get_file(self, key: str, path: Path, length: int, sha256: str) -> None:
        """Fetch one part, and fetch it again if it does not match its manifest entry."""

        def attempt() -> None:
            digest = hashlib.sha256()
            received = 0
            with self.exchange("GET", key) as response, path.open("wb") as output:
                while block := response.read(SOCKET_BLOCK_BYTES):
                    output.write(block)
                    digest.update(block)
                    received += len(block)
            if received != length or digest.hexdigest() != sha256:
                raise Transient(f"{received} bytes that do not match the manifest")

        with_retries(f"reading {key}", attempt)

    def list_page(self, query: str) -> bytes:
        def attempt() -> bytes:
            with self.exchange("GET", "", query) as response:
                return response.read()

        return with_retries("listing the run's artifacts", attempt)

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


@dataclass(frozen=True)
class Part:
    length: int
    sha256: str


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
                print(f"::warning title=CI artifact::{path} went away before it was packed", flush=True)
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
        upload_id = uuid.uuid4().hex

        def send(index: int) -> None:
            part = writer.parts[index]
            key = f"{prefix}/{run}/{name}/{upload_id}/part-{index}"
            bucket.put_file(key, writer.path(index), part.length)

        with ThreadPoolExecutor(CONCURRENT_TRANSFERS) as transfers:
            list(transfers.map(send, range(len(writer.parts))))
        manifest = {"prefix": prefix, "upload": upload_id, "parts": [asdict(part) for part in writer.parts]}
        bucket.put_bytes(manifest_key(run, name), json.dumps(manifest).encode())
    total = sum(part.length for part in writer.parts)
    print(f"{name}: {files} files, {total} bytes in {len(writer.parts)} parts")


def fetch(bucket: Bucket, run: str, name: str, destination: Path) -> None:
    manifest = json.loads(bucket.get_bytes(manifest_key(run, name)))
    parts = [Part(**part) for part in manifest["parts"]]
    prefix = manifest["prefix"]
    upload_id = manifest["upload"]
    destination.mkdir(parents=True, exist_ok=True)
    with transfer_directory() as directory:
        paths = [Path(directory) / f"part-{index}" for index in range(len(parts))]

        def receive(index: int) -> None:
            key = f"{prefix}/{run}/{name}/{upload_id}/part-{index}"
            bucket.get_file(key, paths[index], parts[index].length, parts[index].sha256)

        try:
            with ThreadPoolExecutor(CONCURRENT_TRANSFERS) as transfers:
                list(transfers.map(receive, range(len(parts))))
        except Missing as error:
            raise ArtifactError(f"{name} of run {run} has expired: its parts are gone") from error
        with contextlib.closing(PartReader(paths)) as reader:
            with tarfile.open(fileobj=reader, mode="r|*") as archive:
                archive.extractall(destination, filter="data")
    print(f"{name}: extracted into {destination}")


def download(bucket: Bucket, run: str, name: str, pattern: str, destination: Path) -> None:
    if name:
        try:
            fetch(bucket, run, name, destination)
        except Missing as error:
            raise ArtifactError(f"run {run} has no artifact {name}") from error
        return
    matching = [item for item in bucket.artifact_names(run) if fnmatch.fnmatchcase(item, pattern)]
    if not matching:
        print(f"run {run} has no artifact matching {pattern}")
    for item in matching:
        try:
            fetch(bucket, run, item, destination / item)
        except Missing:
            # Parts without a manifest are an upload that never finished: no artifact at all.
            print(f"::warning title=CI artifact::{item} of run {run} was never completed", flush=True)


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
        print(f"::warning title=CI artifact::{message}", flush=True)
    else:
        print(message)


def main(argv: Sequence[str] | None = None) -> int:
    arguments = parse_arguments(argv)
    try:
        if not arguments.run:
            raise ArtifactError("GITHUB_RUN_ID or --run must name the run")
        if arguments.command == "upload":
            run_upload(arguments)
        else:
            bucket = Bucket.from_environment(os.environ)
            download(bucket, arguments.run, arguments.name, arguments.pattern, Path(arguments.path))
    except ArtifactError as error:
        print(f"::error title=CI artifact::{error}", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
