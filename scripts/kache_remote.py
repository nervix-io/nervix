#!/usr/bin/env python3
"""The shared kache remote of a CI job: its configuration, its daemon and its publication.

`configure PATH` writes the file every kache process reads: the remote, and the budget of the local
store, a quarter of the volume that holds the cache directory.

`start` configures that remote and store for the current GitHub Actions job, prepares the store's
disk, exports the environment later steps need, starts the job's daemon and fails unless the daemon
runs with that remote.

`publish --job-status STATUS REPORT_DIR` publishes what the job built within a bounded time and
records the outcome beside the job's kache report.

Every answer this script reads from kache is one of its documented machine interfaces: the
versioned `--json` documents and the OTLP counters of `kache telemetry write`. The "Cache
Publication" section of docs/src/integration-test-lifecycle.md owns the contract.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Iterator, Mapping, Sequence
import contextlib
from dataclasses import asdict, dataclass, field
import enum
import json
import os
from pathlib import Path
import pwd
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from types import FrameType
from typing import Any
from urllib.parse import urlsplit

# Every job reads the base prefix first. Kache lets a pull-request job publish only below a prefix
# of its own, which successive pull-request runs share.
BASE_PREFIX = "artifacts"
PULL_REQUEST_PREFIX = "artifacts-pr"

# Publication of a job whose work succeeded waits at most this long, kache commands included. A
# scenario suite that passes ends inside the 5-minute reserve its job keeps after the suite budget,
# and this bound leaves that reserve the uploads before publication and the post steps after it.
PUBLICATION_BUDGET_SECONDS = 120.0
UPLOAD_POLL_SECONDS = 2.0

# A daemon that has just started may not answer at once, and on a loaded runner it can take a while:
# it gets a minute to answer, polled every two seconds. A daemon that never answers is started once
# more, after its log is shown, before the job fails: on 2026-10-30 one Boat runner, slow enough that
# `kache doctor` took 47 seconds, never reached a daemon its first start reported as started.
VERIFY_SECONDS = 60.0
VERIFY_RETRY_SECONDS = 2.0
DAEMON_STARTS = 2
DAEMON_COMMAND_SECONDS = 30.0
# How much of each daemon log a failed start shows.
DAEMON_LOG_LINES = 40
DAEMON_LOG_BYTES = 64 * 1024

KACHE_DOCUMENT_SCHEMA_VERSION = 1
TELEMETRY_SCHEMA_VERSION = "1"
OUTPUT_TAIL_LINES = 20

REMOTE_SETTINGS = (
    "KACHE_S3_BUCKET",
    "KACHE_S3_REGION",
    "KACHE_S3_ENDPOINT",
    "KACHE_S3_ACCESS_KEY",
    "KACHE_S3_SECRET_KEY",
)
# Settings that would describe a second copy of the remote beside the configuration file. Kache
# keeps them out of a daemon it spawns, and the commands this script runs leave them out too, so
# every kache process reads the one remote the file names.
AMBIENT_REMOTE_SETTINGS = ("KACHE_S3_BUCKET", "KACHE_S3_REGION", "KACHE_S3_ENDPOINT", "KACHE_S3_PREFIX")
BUCKET_NAME = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")
REGION_NAME = re.compile(r"[a-z0-9][a-z0-9-]*")
JOB_STATUSES = ("success", "failure", "cancelled")

# The share of its volume a job's local store may hold, and the least it may hold. Kache's own
# default is 5% of the volume, capped at 100 GiB: 15 GiB on a Boat runner, below the 33 GiB the
# `checks` job's store reached, so the store ran over its budget, where kache's sweeps evict from it
# while the job still builds. A quarter of the volume holds that with room to spare, and still
# scales with a runner's disk. The floor is kache's own.
STORE_SHARE_PERCENT = 25
STORE_FLOOR_GIB = 5
GIB = 1024**3


class ConfigurationError(Exception):
    """The job's remote settings are missing or malformed."""


class KacheError(Exception):
    """A kache command failed, or answered outside its documented contract."""


class StartError(Exception):
    """The job's daemon did not start with the configured remote."""


class Cancelled(Exception):
    """The step running publication received a termination signal."""

    def __init__(self, signal_number: int) -> None:
        super().__init__(signal.Signals(signal_number).name)
        self.signal_number = signal_number


def toml_string(value: str) -> str:
    """A TOML basic string. JSON's escapes for quotes, backslashes and control characters are
    TOML's too, so the encoded value cannot end the string or start another key."""
    return json.dumps(value)


@dataclass(frozen=True)
class Remote:
    """The S3 remote a job's kache daemon uses. Credentials never enter the configuration file:
    they reach the daemon and every other kache process through the environment."""

    bucket: str
    region: str
    endpoint: str
    access_key: str
    secret_key: str

    @classmethod
    def from_environment(cls, environment: Mapping[str, str]) -> Remote:
        for name in REMOTE_SETTINGS:
            if not environment.get(name):
                raise ConfigurationError(f"{name} is required")
        bucket = environment["KACHE_S3_BUCKET"]
        if not BUCKET_NAME.fullmatch(bucket):
            raise ConfigurationError("KACHE_S3_BUCKET is not a valid S3 bucket name")
        region = environment["KACHE_S3_REGION"]
        if not REGION_NAME.fullmatch(region):
            raise ConfigurationError("KACHE_S3_REGION is not a valid S3 region")
        endpoint = cls.service_endpoint(environment["KACHE_S3_ENDPOINT"])
        return cls(
            bucket=bucket,
            region=region,
            endpoint=endpoint,
            access_key=environment["KACHE_S3_ACCESS_KEY"],
            secret_key=environment["KACHE_S3_SECRET_KEY"],
        )

    @staticmethod
    def service_endpoint(endpoint: str) -> str:
        """The endpoint as an HTTP(S) service URL. A bucket path would make kache look for its keys
        below the bucket's name, so the remote would appear empty."""
        refusal = ConfigurationError(
            "KACHE_S3_ENDPOINT must be an HTTP(S) service URL without credentials, a bucket path, "
            "query, or fragment"
        )
        for character in endpoint:
            if character.isspace() or ord(character) < 32:
                raise refusal
        try:
            parsed = urlsplit(endpoint)
            # Reading the port validates it.
            parsed.port
        except ValueError as error:
            raise refusal from error
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise refusal
        if parsed.username is not None or parsed.password is not None:
            raise refusal
        if parsed.path not in ("", "/") or parsed.query or parsed.fragment:
            raise refusal
        return endpoint.rstrip("/")

    @property
    def description(self) -> str:
        """How kache names this remote."""
        return f"s3://{self.bucket}/{BASE_PREFIX}"

    def configuration(self) -> str:
        """The remote's table of the configuration file."""
        lines = [
            "[cache.remote]",
            'type = "s3"',
            f"bucket = {toml_string(self.bucket)}",
            f"region = {toml_string(self.region)}",
            f"endpoint = {toml_string(self.endpoint)}",
            f"prefix = {toml_string(BASE_PREFIX)}",
            f"pull_request_prefix = {toml_string(PULL_REQUEST_PREFIX)}",
        ]
        return "\n".join(lines) + "\n"



def cache_directory(environment: Mapping[str, str]) -> Path:
    """Where kache keeps its store: KACHE_CACHE_DIR, else kache's default below the user's cache
    directory, which an absolute XDG_CACHE_HOME names, else `.cache` in the home directory: HOME,
    or the user's passwd entry when HOME is unset."""
    configured = environment.get("KACHE_CACHE_DIR")
    if configured:
        return Path(configured).expanduser()
    cache_home = environment.get("XDG_CACHE_HOME")
    if cache_home and Path(cache_home).is_absolute():
        return Path(cache_home) / "kache"
    home = environment.get("HOME")
    if not home:
        home = pwd.getpwuid(os.getuid()).pw_dir
    return Path(home) / ".cache" / "kache"


@dataclass(frozen=True)
class LocalStore:
    """The budget of a job's local kache store: a share of the volume that holds it."""

    max_size_gib: int

    @classmethod
    def for_volume(cls, volume_bytes: int) -> LocalStore:
        """The volume's share, rounded to the nearest GiB as kache rounds its own, and at least the
        floor."""
        share = volume_bytes * STORE_SHARE_PERCENT // 100
        rounded = (share + GIB // 2) // GIB
        return cls(max_size_gib=max(rounded, STORE_FLOOR_GIB))

    @classmethod
    def from_environment(cls, environment: Mapping[str, str]) -> LocalStore:
        """The budget of the store in the job's cache directory, measured on its volume."""
        directory = cache_directory(environment)
        # Before kache's first build the directory may not exist yet; it will be created on the
        # volume of the nearest directory above it, and the filesystem root always exists.
        existing = directory
        while not existing.exists():
            existing = existing.parent
        return cls.for_volume(shutil.disk_usage(existing).total)

    def configuration(self) -> str:
        """The store's table of the configuration file."""
        return f"[cache]\nlocal_max_size = {toml_string(f'{self.max_size_gib}GiB')}\n"


# A single-use runner's store needs no crash consistency: the machine and everything on it end with
# the job. On Boat, kache's downloads spent most of their time waiting for the lock of its SQLite
# index, whose holders wait on the disk, so a job keeps the index in memory and lets the other
# writes on the store's volume skip the disk's write barrier.
MEMORY_DIRECTORY = Path("/dev/shm")
MEMORY_INDEX = "kache-index"
# The index and the files SQLite keeps beside it in write-ahead-log mode. SQLite resolves a
# symlinked database and keeps those files next to its target.
INDEX_FILES = ("index.db", "index.db-wal", "index.db-shm")
# The least free memory the index may move to.
MEMORY_INDEX_FREE_BYTES = GIB
SYSTEM_COMMAND_SECONDS = 30.0
# What remounting the store's ext4 volume adds: no access-time writes, and no write barrier, so an
# fsync returns once the device has the data instead of after its cache is flushed too.
VOLUME_OPTIONS = ("noatime", "barrier=0")


def existing_ancestor(path: Path) -> Path:
    """`path`, or the nearest directory above it that exists; the filesystem root always does."""
    existing = path
    while not existing.exists():
        existing = existing.parent
    return existing


def run_system(arguments: Sequence[str], environment: Mapping[str, str]) -> subprocess.CompletedProcess[str] | None:
    """A system command's result, or None when it could not run or did not finish in time."""
    try:
        return subprocess.run(
            list(arguments),
            env=dict(environment),
            capture_output=True,
            text=True,
            timeout=SYSTEM_COMMAND_SECONDS,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None


@dataclass(frozen=True)
class Mount:
    """The mounted filesystem that holds a path, as findmnt reports it."""

    target: str
    filesystem: str
    options: tuple[str, ...]

    @classmethod
    def holding(cls, path: Path, environment: Mapping[str, str]) -> Mount | None:
        """None when findmnt cannot say."""
        completed = run_system(
            [
                "findmnt", "--raw", "--noheadings", "--output", "TARGET,FSTYPE,OPTIONS",
                "--target", str(existing_ancestor(path)),
            ],
            environment,
        )
        if completed is None or completed.returncode != 0:
            return None
        fields = completed.stdout.split()
        if len(fields) != 3:
            return None
        return cls(target=fields[0], filesystem=fields[1], options=tuple(fields[2].split(",")))


def keep_index_in_memory(cache_dir: Path, memory: Path) -> str:
    """Move kache's index into `memory` behind a symlink. Only before any kache process opens it."""
    index = cache_dir / INDEX_FILES[0]
    if index.is_symlink():
        return f"kache's index already lives at {os.readlink(index)}."
    directory = memory / MEMORY_INDEX
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)
    for name in INDEX_FILES:
        source = cache_dir / name
        if source.exists():
            shutil.move(source, directory / name)
    index.symlink_to(directory / INDEX_FILES[0])
    return f"kache's index and its write-ahead log live in memory, in {directory}."


def lighten_volume(path: Path, environment: Mapping[str, str]) -> str:
    """Remount the ext4 volume that holds `path` without access times and write barriers."""
    mount = Mount.holding(path, environment)
    if mount is None:
        return "The store's volume was not remounted: findmnt could not describe it."
    if mount.filesystem != "ext4":
        return f"The store's volume, {mount.target}, is {mount.filesystem}, so it was not remounted."
    options = ",".join(("remount", *VOLUME_OPTIONS))
    completed = run_system(["sudo", "--non-interactive", "mount", "-o", options, mount.target], environment)
    if completed is None:
        return f"Remounting the store's volume, {mount.target}, did not run; it keeps its options."
    if completed.returncode != 0:
        return (
            f"Remounting the store's volume, {mount.target}, exited with status "
            f"{completed.returncode}: {completed.stderr.strip()}; it keeps its options."
        )
    remounted = Mount.holding(path, environment)
    if remounted is None:
        return f"The store's volume, {mount.target}, is remounted; findmnt could not read its options."
    return f"The store's volume, {mount.target}, is remounted with {','.join(remounted.options)}."


def prepare_store(environment: Mapping[str, str], memory: Path = MEMORY_DIRECTORY) -> list[str]:
    """Keep the store's index in memory and lighten its volume's writes, before the job's daemon
    opens the index. Neither is required: a runner where one does not apply keeps kache's default,
    and the lines returned say what was done."""
    cache_dir = cache_directory(environment)
    lines = []
    memory_mount = Mount.holding(memory, environment)
    if memory_mount is None or memory_mount.filesystem != "tmpfs":
        lines.append(f"kache's index stays on disk: {memory} is not a tmpfs.")
    elif shutil.disk_usage(memory).free < MEMORY_INDEX_FREE_BYTES:
        lines.append(f"kache's index stays on disk: {memory} has less than 1 GiB free.")
    else:
        lines.append(keep_index_in_memory(cache_dir, memory))
    lines.append(lighten_volume(cache_dir, environment))
    return lines


@dataclass(frozen=True)
class Configuration:
    """The file every kache process of a job reads, through KACHE_CONFIG: its local store's budget
    and its remote."""

    store: LocalStore
    remote: Remote

    def text(self) -> str:
        return self.store.configuration() + "\n" + self.remote.configuration()

    def write(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            output.write(self.text())


def kache_environment(environment: Mapping[str, str]) -> dict[str, str]:
    """The environment of a kache command this script runs."""
    selected = dict(environment)
    for name in AMBIENT_REMOTE_SETTINGS:
        selected.pop(name, None)
    return selected


def required(environment: Mapping[str, str], name: str) -> str:
    value = environment.get(name)
    if not value:
        raise ConfigurationError(f"{name} is required")
    return value


def append_job_environment(path: Path, variables: Mapping[str, str]) -> None:
    """Export variables to the job's later steps through GitHub's environment file."""
    lines = []
    for name, value in variables.items():
        if "\n" in value or "\r" in value:
            raise ConfigurationError(f"{name} must be a single line to be exported to later steps")
        lines.append(f"{name}={value}\n")
    with path.open("a", encoding="utf-8") as output:
        output.writelines(lines)


def captured_text(output: str | bytes | None) -> str:
    """Output a timed-out command left behind: the POSIX runner keeps it as bytes even in text
    mode."""
    if output is None:
        return ""
    if isinstance(output, bytes):
        return output.decode(errors="replace")
    return output


def run_kache(arguments: Sequence[str], environment: Mapping[str, str], timeout: float) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["kache", *arguments],
        env=dict(environment),
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def kache_document(arguments: Sequence[str], environment: Mapping[str, str], timeout: float) -> dict[str, Any]:
    """A kache command's versioned JSON document, which must report success."""
    rendered = " ".join(["kache", "--json", *arguments])
    try:
        completed = run_kache(["--json", *arguments], environment, timeout)
    except subprocess.TimeoutExpired as error:
        raise KacheError(f"`{rendered}` did not answer within {timeout:.0f} s") from error
    try:
        document = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise KacheError(
            f"`{rendered}` exited with status {completed.returncode} without a JSON document: "
            f"{completed.stderr.strip()}"
        ) from error
    if not isinstance(document, dict) or document.get("schema_version") != KACHE_DOCUMENT_SCHEMA_VERSION:
        raise KacheError(f"`{rendered}` answered outside JSON schema {KACHE_DOCUMENT_SCHEMA_VERSION}")
    if document.get("success") is not True:
        message = document.get("error", {}).get("message", "no error message")
        raise KacheError(f"`{rendered}` failed: {message}")
    return document


@dataclass(frozen=True)
class DaemonState:
    """What the job's kache daemon reports about itself.

    remote is the remote the job's configuration names. remote_readonly says whether the daemon
    skips uploads, which kache decides when the daemon starts; None means it did not say."""

    running: bool
    config_path: str | None
    remote: str | None
    remote_readonly: bool | None

    @classmethod
    def read(cls, environment: Mapping[str, str], timeout: float) -> DaemonState:
        status = kache_document(["daemon", "status"], environment, timeout)
        config_path = status.get("daemon_config_path")
        # `kache stats` starts a daemon when none answers, so it is asked only about a running one.
        if status.get("daemon_running") is not True:
            return cls(running=False, config_path=config_path, remote=None, remote_readonly=None)
        stats = kache_document(["stats"], environment, timeout)
        return cls(
            running=stats.get("daemon_connected") is True,
            config_path=config_path,
            remote=stats.get("remote"),
            remote_readonly=stats.get("daemon_remote_readonly"),
        )

    def refusal(self, remote: Remote, configuration: Path) -> str | None:
        """Why this daemon is not running with the job's remote, or None when it is."""
        if not self.running:
            return "the daemon is not running"
        if self.config_path is None or Path(self.config_path).resolve() != configuration.resolve():
            return f"the daemon reads configuration {self.config_path}, not {configuration}"
        if self.remote != remote.description:
            return f"the configuration names remote {self.remote}, not {remote.description}"
        return None


@dataclass(frozen=True)
class DaemonCheck:
    """What the job's daemon answered when start last asked, and why it is not the job's daemon,
    if it is not."""

    daemon: DaemonState | None
    refusal: str | None


def await_daemon(
    environment: Mapping[str, str], remote: Remote, configuration: Path, clock: Clock, verify_seconds: float
) -> DaemonCheck:
    """Poll the daemon until it runs with the job's remote or `verify_seconds` pass."""
    deadline = clock.now() + verify_seconds
    while True:
        try:
            daemon = DaemonState.read(environment, DAEMON_COMMAND_SECONDS)
            check = DaemonCheck(daemon=daemon, refusal=daemon.refusal(remote, configuration))
        except KacheError as error:
            check = DaemonCheck(daemon=None, refusal=str(error))
        if check.refusal is None or clock.now() >= deadline:
            return check
        clock.sleep(VERIFY_RETRY_SECONDS)


def start_daemon(environment: Mapping[str, str]) -> None:
    try:
        started = run_kache(["daemon", "start"], environment, DAEMON_COMMAND_SECONDS)
    except subprocess.TimeoutExpired as error:
        raise StartError(f"`kache daemon start` did not return within {DAEMON_COMMAND_SECONDS:.0f} s") from error
    print((started.stdout + started.stderr).strip(), flush=True)
    if started.returncode != 0:
        raise StartError(f"`kache daemon start` exited with status {started.returncode}")


def daemon_logs(environment: Mapping[str, str]) -> str:
    """The end of every log in the job's kache runtime directory, where a daemon that cannot start
    writes why."""
    runtime = environment.get("KACHE_RUNTIME_DIR")
    if not runtime:
        return "KACHE_RUNTIME_DIR is not set, so no kache daemon log can be shown."
    logs = sorted(Path(runtime).glob("*.log"))
    if not logs:
        return f"{runtime} holds no kache daemon log."
    sections = []
    for log in logs:
        with log.open("rb") as stream:
            stream.seek(0, os.SEEK_END)
            stream.seek(max(stream.tell() - DAEMON_LOG_BYTES, 0))
            tail = stream.read().decode(errors="replace").splitlines()[-DAEMON_LOG_LINES:]
        sections.append(f"The end of {log}:\n" + "\n".join(tail))
    return "\n".join(sections)


def start(
    environment: Mapping[str, str],
    clock: Clock | None = None,
    verify_seconds: float = VERIFY_SECONDS,
    prepare: Callable[[Mapping[str, str]], list[str]] | None = None,
) -> DaemonState:
    """Configure the job's remote, prepare its store, export them to later steps, and start the
    job's daemon with them."""
    clock = clock or Clock()
    prepare = prepare or prepare_store
    remote = Remote.from_environment(environment)
    store = LocalStore.from_environment(environment)
    manifest_key = required(environment, "KACHE_MANIFEST_KEY")
    configuration = Path(required(environment, "RUNNER_TEMP")) / "kache-remote.toml"
    job_environment = Path(required(environment, "GITHUB_ENV"))
    Configuration(store=store, remote=remote).write(configuration)
    exported = {
        "KACHE_CONFIG": str(configuration),
        "KACHE_S3_ACCESS_KEY": remote.access_key,
        "KACHE_S3_SECRET_KEY": remote.secret_key,
        "KACHE_MANIFEST_KEY": manifest_key,
        # The namespace selects the content-addressed shards the daemon prefetches and the
        # manifest publishes; one per manifest key keeps every build variant's shards apart.
        "KACHE_NAMESPACE": manifest_key,
    }
    append_job_environment(job_environment, exported)
    daemon_environment = kache_environment(environment) | exported
    for line in prepare(environment):
        print(line, flush=True)

    for attempt in range(1, DAEMON_STARTS + 1):
        start_daemon(daemon_environment)
        check = await_daemon(daemon_environment, remote, configuration, clock, verify_seconds)
        if check.refusal is None and check.daemon is not None:
            daemon = check.daemon
            break
        failure = f"the kache daemon is not running with {remote.description}: {check.refusal}"
        logs = daemon_logs(environment)
        if attempt == DAEMON_STARTS:
            raise StartError(f"{failure}\n{logs}")
        print(f"::warning title=kache daemon start {attempt} of {DAEMON_STARTS}::{failure}", flush=True)
        print(logs, flush=True)
    writes = "skips uploads" if daemon.remote_readonly else "publishes uploads"
    print(
        f"kache daemon runs with {remote.description} and a {store.max_size_gib} GiB local store "
        f"from {configuration}, and {writes}"
    )
    return daemon


class Outcome(enum.StrEnum):
    """How a job's publication ended.

    published: the manifest is saved and the daemon reports every upload of the job finished.
    incomplete: publication ran, but the manifest or an upload is unconfirmed at its end.
    read-only: kache made the job's remote read-only, so the job has nothing to publish.
    not-run: the job's work failed or was cancelled, so publication did not wait for anything.
    cancelled: the step was terminated while publication ran.
    unavailable: the job has no kache command or no daemon that answers.
    """

    PUBLISHED = "published"
    INCOMPLETE = "incomplete"
    READ_ONLY = "read-only"
    NOT_RUN = "not-run"
    CANCELLED = "cancelled"
    UNAVAILABLE = "unavailable"

    @property
    def warns(self) -> bool:
        return self in (Outcome.INCOMPLETE, Outcome.CANCELLED, Outcome.UNAVAILABLE)


@dataclass(frozen=True)
class Uploads:
    """The job daemon's upload counters, from `kache telemetry write`.

    pending counts uploads queued or in flight. The daemon retries a retryable failure until it
    succeeds, so an upload leaves pending only finished or failed for good. suppressed counts
    attempts deferred while the remote's write breaker was open, whose uploads stay pending.
    """

    pending: int
    completed: int
    failed: int
    skipped: int
    suppressed: int
    bytes_uploaded: int
    remote_degraded: bool

    @classmethod
    def from_telemetry(cls, directory: Path) -> Uploads:
        schema = (directory / "schema_version").read_text(encoding="utf-8").strip()
        if schema != TELEMETRY_SCHEMA_VERSION:
            raise KacheError(f"kache telemetry schema {schema!r}, expected {TELEMETRY_SCHEMA_VERSION!r}")
        document = json.loads((directory / "metrics.otlp.json").read_text(encoding="utf-8"))
        metrics = {}
        for resource in document["resourceMetrics"]:
            for scope in resource["scopeMetrics"]:
                for metric in scope["metrics"]:
                    metrics[metric["name"]] = metric
        results = cls.points(metrics, "kache.cache.uploads", "sum", "kache.cache.result")
        directions = cls.points(metrics, "kache.cache.bytes", "sum", "kache.cache.direction")
        return cls(
            pending=cls.points(metrics, "kache.cache.uploads.pending", "gauge", None)[""],
            completed=results["completed"],
            failed=results["failed"],
            skipped=results["skipped"],
            suppressed=results["suppressed"],
            bytes_uploaded=directions["upload"],
            remote_degraded=cls.points(metrics, "kache.cache.remote.degraded", "gauge", None)[""] != 0,
        )

    @staticmethod
    def points(metrics: Mapping[str, dict[str, Any]], name: str, kind: str, attribute: str | None) -> dict[str, int]:
        """A metric's integer points, keyed by the value of `attribute`, or by "" without one."""
        metric = metrics.get(name)
        if metric is None:
            raise KacheError(f"kache telemetry has no {name} metric")
        values = {}
        for point in metric[kind]["dataPoints"]:
            key = ""
            if attribute is not None:
                for candidate in point["attributes"]:
                    if candidate["key"] == attribute:
                        key = candidate["value"]["stringValue"]
            # OTLP's JSON encoding writes 64-bit integers as strings.
            values[key] = int(point["asInt"])
        return values


@dataclass(frozen=True)
class CommandResult:
    """One kache command publication ran: its exit status, or None when it did not finish within
    the budget, and the end of its output."""

    command: str
    status: int | None
    seconds: float
    output_tail: list[str]

    @property
    def succeeded(self) -> bool:
        return self.status == 0


class Clock:
    def now(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


class Termination:
    """The SIGINT or SIGTERM that ends the publication step, and where publication raises it.

    Python runs a signal handler between any two bytecodes, so a handler that raised at once could
    land inside library cleanup. Python 3.12.3's `shutil.rmtree`, interrupted just after it closed a
    temporary directory's descriptor, closed it again, and that failure replaced the cancellation
    before it was recorded; later versions leave the directory behind instead. So the handler only
    records the first signal, and raises it only inside a wait for a kache command or for the next
    poll. There it raises at once and only once, so the wait's own cleanup, such as reaping the
    command it killed, runs to its end. A signal received outside a wait is raised when the next wait
    begins or the current phase ends, so the record names the phase in which the step received it.
    """

    def __init__(self) -> None:
        self.signal_number: int | None = None
        self.interrupts_wait = False

    def install(self) -> None:
        """Make this the step's handler of both termination signals."""
        signal.signal(signal.SIGINT, self.handle)
        signal.signal(signal.SIGTERM, self.handle)

    def ignore(self) -> None:
        """Ignore both termination signals once the record is written. Python's exit restores the
        default handlers, so a later signal would otherwise end the step with its own status."""
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)

    def handle(self, signal_number: int, _frame: FrameType | None) -> None:
        """Record the step's first termination signal, and raise it if a wait is running."""
        if self.signal_number is None:
            self.signal_number = signal_number
        if self.interrupts_wait:
            self.interrupts_wait = False
            raise Cancelled(self.signal_number)

    def check(self) -> None:
        """Raise the termination the step received, if it received one."""
        if self.signal_number is not None:
            raise Cancelled(self.signal_number)

    @contextlib.contextmanager
    def interruptible(self) -> Iterator[None]:
        """A wait that the step's termination ends at once, including one received before it."""
        # The wait becomes interruptible before it checks, so a signal between the two is raised.
        self.interrupts_wait = True
        try:
            self.check()
            yield
        finally:
            self.interrupts_wait = False


@dataclass
class Publication:
    """The record of one job's publication, written whatever its outcome."""

    job: str
    job_status: str
    manifest_key: str | None
    budget_seconds: float
    outcome: Outcome | None = None
    reasons: list[str] = field(default_factory=list)
    phase: str = "starting"
    manifest: CommandResult | None = None
    uploads_at_start: Uploads | None = None
    uploads_at_end: Uploads | None = None
    upload_wait_seconds: float | None = None
    elapsed_seconds: float = 0.0

    def finish(self, outcome: Outcome, reasons: Sequence[str]) -> None:
        self.outcome = outcome
        self.reasons.extend(reasons)

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2) + "\n"

    def markdown(self) -> str:
        lines = [f"### Kache publication: {self.outcome}", ""]
        lines.append(f"Job `{self.job}` ended its work with status `{self.job_status}`.")
        lines.append(f"Publication took {self.elapsed_seconds:.1f} s of its {self.budget_seconds:.0f} s budget.")
        if self.manifest is not None:
            if self.manifest.status is None:
                state = "did not finish within the budget"
            else:
                state = f"exited with status {self.manifest.status}"
            said = self.manifest.output_tail[-1] if self.manifest.output_tail else "no output"
            lines.append(
                f"Manifest `{self.manifest_key}`: `kache save-manifest` {state} after "
                f"{self.manifest.seconds:.1f} s: {said}"
            )
        if self.uploads_at_end is not None:
            uploads = self.uploads_at_end
            lines.append(
                f"Uploads: {uploads.completed} completed, {uploads.skipped} already present, "
                f"{uploads.failed} failed, {uploads.pending} pending; {uploads.bytes_uploaded} bytes uploaded."
            )
        for reason in self.reasons:
            lines.append(f"- {reason}")
        return "\n".join(lines) + "\n"


class Publisher:
    """Publishes one job's cache within its budget: saves the build manifest, then waits until the
    job's daemon reports no upload queued or in flight."""

    def __init__(
        self,
        environment: Mapping[str, str],
        clock: Clock,
        termination: Termination,
        budget_seconds: float,
        poll_seconds: float,
    ) -> None:
        self.environment = kache_environment(environment)
        self.clock = clock
        self.termination = termination
        self.budget_seconds = budget_seconds
        self.poll_seconds = poll_seconds
        self.started = clock.now()
        self.deadline = self.started + budget_seconds

    def remaining(self) -> float:
        return self.deadline - self.clock.now()

    def command_timeout(self) -> float:
        """A daemon query's timeout: the rest of the budget, but at least one second, so a query
        that confirms the end of the budget can still be asked."""
        return min(max(self.remaining(), 1.0), DAEMON_COMMAND_SECONDS)

    def publish(self, publication: Publication) -> None:
        if publication.job_status == "cancelled":
            # The kache action's post step, whose daemon stop drains queued uploads, runs only
            # after a job that succeeded or failed, and a job that reaches its time limit is
            # cancelled.
            publication.finish(
                Outcome.NOT_RUN,
                ["The job's work ended with status `cancelled`. The kache action's post step does not run for a cancelled job, so uploads still queued end with the machine."],
            )
            return
        if publication.job_status != "success":
            publication.finish(
                Outcome.NOT_RUN,
                [f"The job's work ended with status `{publication.job_status}`; uploads still queued are left to the daemon's shutdown drain."],
            )
            return
        if shutil.which("kache", path=self.environment.get("PATH")) is None:
            publication.finish(Outcome.UNAVAILABLE, ["This job has no kache command."])
            return
        if publication.manifest_key is None:
            publication.finish(Outcome.UNAVAILABLE, ["KACHE_MANIFEST_KEY is not set, so the job never started its kache remote."])
            return

        # Without its daemon, kache reports counters from the store alone, where nothing is ever
        # pending, so every count below is confirmed only while the daemon answers.
        self.begin(publication, "asking the daemon")
        try:
            daemon = self.daemon()
        except KacheError as error:
            publication.finish(Outcome.UNAVAILABLE, [f"The job's kache daemon could not be asked: {error}."])
            return
        if not daemon.running:
            publication.finish(Outcome.UNAVAILABLE, ["The job's kache daemon is not running."])
            return
        if daemon.remote_readonly:
            publication.finish(
                Outcome.READ_ONLY,
                ["Kache made this job's remote read-only, so the job publishes no manifest and no uploads."],
            )
            return
        try:
            publication.uploads_at_start = self.uploads()
        except KacheError as error:
            publication.finish(Outcome.UNAVAILABLE, [f"The daemon's uploads could not be read: {error}."])
            return

        self.begin(publication, "saving the manifest")
        namespace = self.environment.get("KACHE_NAMESPACE") or publication.manifest_key
        publication.manifest = self.command(
            ["save-manifest", "--manifest-key", publication.manifest_key, "--namespace", namespace]
        )

        self.begin(publication, "waiting for uploads")
        waiting_since = self.clock.now()
        reasons = []
        uploads = publication.uploads_at_start
        while self.remaining() > 0:
            try:
                uploads = self.uploads()
            except KacheError as error:
                reasons.append(f"The daemon's uploads could not be read: {error}.")
                break
            if uploads.pending == 0:
                break
            remaining = self.remaining()
            if remaining <= 0:
                break
            with self.termination.interruptible():
                self.clock.sleep(min(self.poll_seconds, remaining))
        publication.upload_wait_seconds = self.clock.now() - waiting_since
        publication.uploads_at_end = uploads
        try:
            still_running = self.daemon().running
        except KacheError:
            still_running = False
        if not still_running:
            reasons.append("The daemon stopped answering during the wait, so its upload counters are unconfirmed.")

        if not publication.manifest.succeeded:
            if publication.manifest.status is None:
                reasons.append("The manifest was not saved within the budget.")
            else:
                reasons.append(f"Saving the manifest exited with status {publication.manifest.status}.")
        if uploads.pending:
            reasons.append(
                f"{uploads.pending} uploads were still queued or in flight when the budget ended; "
                "the daemon's shutdown drain takes them over."
            )
        if uploads.failed:
            reasons.append(f"{uploads.failed} uploads failed for good during the job.")
        if reasons:
            publication.finish(Outcome.INCOMPLETE, reasons)
        else:
            publication.finish(Outcome.PUBLISHED, [])

    def begin(self, publication: Publication, phase: str) -> None:
        """Move publication to its next phase. A termination received since the last wait is raised
        first, so the record names the phase in which the step received it."""
        self.termination.check()
        publication.phase = phase

    def daemon(self) -> DaemonState:
        """What the job's daemon reports, asked in a wait the step's termination interrupts."""
        with self.termination.interruptible():
            return DaemonState.read(self.environment, self.command_timeout())

    def command(self, arguments: Sequence[str]) -> CommandResult:
        began = self.clock.now()
        rendered = " ".join(["kache", *arguments])
        remaining = self.remaining()
        if remaining <= 0:
            return CommandResult(rendered, None, 0.0, [])
        try:
            with self.termination.interruptible():
                completed = run_kache(arguments, self.environment, remaining)
        except subprocess.TimeoutExpired as expired:
            output = captured_text(expired.stdout) + captured_text(expired.stderr)
            return CommandResult(rendered, None, self.clock.now() - began, output.splitlines()[-OUTPUT_TAIL_LINES:])
        output = completed.stdout + completed.stderr
        return CommandResult(rendered, completed.returncode, self.clock.now() - began, output.splitlines()[-OUTPUT_TAIL_LINES:])

    def uploads(self) -> Uploads:
        timeout = self.command_timeout()
        # Only the command is a wait: a termination never interrupts the directory's removal.
        with tempfile.TemporaryDirectory(prefix="kache-telemetry-") as directory:
            try:
                with self.termination.interruptible():
                    written = run_kache(["telemetry", "write", directory], self.environment, timeout)
            except subprocess.TimeoutExpired as error:
                raise KacheError(f"`kache telemetry write` did not answer within {timeout:.0f} s") from error
            if written.returncode != 0:
                raise KacheError(
                    f"`kache telemetry write` exited with status {written.returncode}: {written.stderr.strip()}"
                )
            try:
                return Uploads.from_telemetry(Path(directory))
            except (OSError, ValueError, KeyError, TypeError) as error:
                raise KacheError(f"unreadable kache telemetry: {error!r}") from error

    def elapsed(self) -> float:
        return self.clock.now() - self.started


def publish(
    report_directory: Path,
    job_status: str,
    environment: Mapping[str, str],
    termination: Termination,
    clock: Clock | None = None,
    budget_seconds: float = PUBLICATION_BUDGET_SECONDS,
    poll_seconds: float = UPLOAD_POLL_SECONDS,
) -> Publication:
    """Publish the job's cache and write its record. A step termination is recorded too, then
    raised again so the step ends as the runner asked."""
    publisher = Publisher(environment, clock or Clock(), termination, budget_seconds, poll_seconds)
    publication = Publication(
        # An image build has no GitHub job; its manifest key names it.
        job=environment.get("GITHUB_JOB") or environment.get("KACHE_MANIFEST_KEY") or "local build",
        job_status=job_status,
        manifest_key=environment.get("KACHE_MANIFEST_KEY") or None,
        budget_seconds=budget_seconds,
    )
    try:
        publisher.publish(publication)
        # A termination received after the last wait cancels publication too.
        termination.check()
    except Cancelled as error:
        publication.finish(Outcome.CANCELLED, [f"The step received {error} while {publication.phase}."])
    publication.elapsed_seconds = publisher.elapsed()
    record(publication, report_directory, environment)
    # Raised again, as is a termination received while the record was written.
    termination.check()
    return publication


def record(publication: Publication, report_directory: Path, environment: Mapping[str, str]) -> None:
    report_directory.mkdir(parents=True, exist_ok=True)
    (report_directory / "publication.json").write_text(publication.to_json(), encoding="utf-8")
    rendered = publication.markdown()
    (report_directory / "publication.md").write_text(rendered, encoding="utf-8")
    summary = environment.get("GITHUB_STEP_SUMMARY")
    if summary:
        with Path(summary).open("a", encoding="utf-8") as output:
            output.write(rendered + "\n")
    print(rendered)
    if publication.outcome is not None and publication.outcome.warns:
        reasons = " ".join(publication.reasons)
        print(f"::warning title=kache publication {publication.outcome}::{reasons}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    configure_parser = commands.add_parser("configure", help="write the configuration file of kache's remote and store")
    configure_parser.add_argument("path", type=Path)
    commands.add_parser("start", help="configure the job's remote and start its kache daemon")
    publish_parser = commands.add_parser("publish", help="publish the job's cache within its budget")
    publish_parser.add_argument("--job-status", required=True, choices=JOB_STATUSES)
    publish_parser.add_argument("report_directory", type=Path)
    arguments = parser.parse_args(argv)

    environment = dict(os.environ)
    try:
        if arguments.command == "configure":
            remote = Remote.from_environment(environment)
            store = LocalStore.from_environment(environment)
            Configuration(store=store, remote=remote).write(arguments.path)
        elif arguments.command == "start":
            start(environment)
        else:
            termination = Termination()
            termination.install()
            try:
                publish(arguments.report_directory, arguments.job_status, environment, termination)
            finally:
                termination.ignore()
            # A termination received after publication's last check still ends the step with its
            # status, and from here on none can replace it.
            termination.check()
    except ConfigurationError as error:
        print(f"{parser.prog} {arguments.command}: {error}", file=sys.stderr)
        return 2
    except StartError as error:
        print(f"{parser.prog} {arguments.command}: {error}", file=sys.stderr)
        return 1
    except Cancelled as cancelled:
        return 128 + cancelled.signal_number
    return 0


if __name__ == "__main__":
    sys.exit(main())
