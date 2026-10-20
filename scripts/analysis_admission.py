"""Admit the analysis compilations that need the most memory against the memory they will use.

Cargo runs every analysis compilation through the typed ratchet's workspace wrapper beneath the
configured kache, which runs such a wrapper chain directly, outside its own memory-weighted
scheduler. Nothing else admits an analysis compilation against the host's memory, while one
validation analyzes the server library in about ten configurations and validations in several
worktrees of one host run at the same time.

The runner hands every compilation the declared admission policy. A compilation the policy gives a
reservation takes a place in one queue that all of the user's worktrees on the host share before
its driver starts, and holds it until the driver ends. It waits while an older waiter that still
decides is ahead of it. It is then admitted when its reservation fits in what each memory domain
it runs in has left after the growth still to come of every compilation admitted in that domain:
the host's available memory and each ancestor cgroup with a memory limit. It is also admitted when
no other compilation is admitted, so with too little memory the declared compilations run one at a
time, and at the policy's wait limit, which bounds every wait. Each admission returns a record of
how it was admitted, how long it waited and what it waited for.

A place is an exclusive advisory lock on the compilation's lease file, so the kernel releases the
place of a wrapper that ends without releasing it and the next decision deletes its lease. Every
decision holds one queue lock, so two waiters never decide on the same memory.
"""

from __future__ import annotations

import dataclasses
import errno
import fcntl
import json
import os
import pathlib
import stat
import sys
import time
import typing
import uuid
from contextlib import contextmanager
from typing import Any, Callable, Iterator, Sequence, TextIO

GIB = 2**30
# A waiter decides again this often, so an admitted compilation's place passes on within it.
POLL_SECONDS = 1.0
# A waiter that has not decided for this many polls keeps its lease but stops holding back the
# waiters behind it, so a stopped wrapper cannot stop the queue.
RESPONSIVE_POLLS = 10
RESPONSIVE_NS = int(RESPONSIVE_POLLS * POLL_SECONDS * 10**9)
# A waiter repeats what it waits for this often.
REPORT_SECONDS = 60.0
# The queue lock is held only while deciding; a waiter that finds it held tries again this often.
LOCK_RETRY_SECONDS = 0.01

LIBRARY_KINDS = {"lib", "rlib", "cdylib", "staticlib", "proc-macro"}


def target_kind(kinds: list[str]) -> str:
    """Cargo's target kinds as the analysis names them: every library form is one `lib`."""
    if set(kinds) & LIBRARY_KINDS:
        return "lib"
    return kinds[0]


def host_directory() -> pathlib.Path:
    """The queue every worktree of this user on this host shares, whatever its temporary directory."""
    return pathlib.Path("/tmp") / f"nervix-analysis-admission-{os.getuid()}"


@dataclasses.dataclass(frozen=True)
class Compiled:
    """A compilation as the policy names it: its crate and its target kind."""

    crate_name: str
    kind: str


@dataclasses.dataclass(frozen=True)
class Policy:
    """The declared admission policy, as the runner hands it to every analysis compilation."""

    directory: pathlib.Path
    threshold_bytes: int
    wait_limit_seconds: float
    reservations: dict[Compiled, int]

    def reservation(self, compiled: Compiled) -> int | None:
        return self.reservations.get(compiled)

    def encode(self) -> str:
        reservations = []
        for compiled in sorted(self.reservations, key=lambda item: (item.crate_name, item.kind)):
            reservations.append({"crate": compiled.crate_name, "kind": compiled.kind, "bytes": self.reservations[compiled]})
        return json.dumps({
            "directory": str(self.directory),
            "threshold_bytes": self.threshold_bytes,
            "wait_limit_seconds": self.wait_limit_seconds,
            "reservations": reservations,
        }, sort_keys=True)

    @classmethod
    def decode(cls, text: str) -> Policy:
        value = json.loads(text)
        reservations = {}
        for item in value["reservations"]:
            reservations[Compiled(item["crate"], item["kind"])] = item["bytes"]
        return cls(
            directory=pathlib.Path(value["directory"]),
            threshold_bytes=value["threshold_bytes"],
            wait_limit_seconds=value["wait_limit_seconds"],
            reservations=reservations,
        )


@dataclasses.dataclass
class Lease:
    """One declared compilation's place in the queue, as its lease file holds it.

    A lease without an admission time is waiting. The times are the host's Unix nanoseconds, so
    the waiters of every worktree compare them.
    """

    identifier: str
    pid: int
    crate_name: str
    kind: str
    configuration: str
    root: str
    reservation_bytes: int
    cgroup: str | None
    enqueued_ns: int
    polled_ns: int
    admitted_ns: int | None
    driver_pid: int | None

    def encode(self) -> bytes:
        return json.dumps(dataclasses.asdict(self), sort_keys=True).encode()

    @classmethod
    def decode(cls, data: bytes) -> Lease:
        value = json.loads(data)
        if not isinstance(value, dict):
            raise ValueError("a lease is not an object")
        fields = typing.get_type_hints(cls)
        if set(value) != set(fields):
            raise ValueError("a lease names other fields than the current lease")
        for name, kind in fields.items():
            item = value[name]
            # A boolean is an int to Python, never to a lease.
            if isinstance(item, bool) or not isinstance(item, kind):
                raise ValueError(f"a lease's {name} is not {kind}")
        return cls(**value)

    def queued_before(self, other: Lease) -> bool:
        return (self.enqueued_ns, self.identifier) < (other.enqueued_ns, other.identifier)

    def describe(self) -> str:
        return f"{self.crate_name} ({self.kind}) in the {self.configuration} analysis of {self.root}, pid {self.pid}"


@dataclasses.dataclass(frozen=True)
class Domain:
    """Memory a compilation runs within: the host's, or a cgroup's below its limit."""

    cgroup: str | None
    available_bytes: int

    def contains(self, lease: Lease) -> bool:
        if self.cgroup is None:
            return True
        if lease.cgroup is None:
            return False
        return lease.cgroup == self.cgroup or lease.cgroup.startswith(self.cgroup + "/")


def host_available_bytes(meminfo: pathlib.Path = pathlib.Path("/proc/meminfo")) -> int | None:
    """The kernel's estimate of the memory the host can give new work without swapping."""
    try:
        text = meminfo.read_text()
    except OSError:
        return None
    for line in text.splitlines():
        name, _, value = line.partition(":")
        if name != "MemAvailable":
            continue
        amount, _, unit = value.strip().partition(" ")
        if unit == "kB" and amount.isdigit():
            return int(amount) * 1024
    return None


def own_cgroup(membership: pathlib.Path = pathlib.Path("/proc/self/cgroup")) -> str | None:
    """This process's cgroup in the unified cgroup v2 hierarchy."""
    try:
        text = membership.read_text()
    except OSError:
        return None
    for line in text.splitlines():
        hierarchy, separator, group = line.partition("::")
        if hierarchy == "0" and separator:
            return group
    return None


def stat_value(text: str, name: str) -> int | None:
    for line in text.splitlines():
        key, _, value = line.partition(" ")
        if key == name and value.isdigit():
            return int(value)
    return None


def cgroup_domains(cgroup: str, hierarchy: pathlib.Path = pathlib.Path("/sys/fs/cgroup")) -> list[Domain]:
    """Each cgroup from `cgroup` up to the root that limits its memory, with what it has left.

    A cgroup's inactive file pages are reclaimed before it reaches its limit, so they count as
    left, as the host's available memory counts them.
    """
    domains = []
    path = pathlib.PurePosixPath(cgroup)
    for current in [path, *path.parents]:
        # The root cgroup has no memory limit of its own; the host's memory is its limit.
        if current == current.parent:
            break
        directory = hierarchy / str(current).lstrip("/")
        try:
            limit = (directory / "memory.max").read_text().strip()
        except OSError:
            continue
        if not limit.isdigit():
            continue
        try:
            used = int((directory / "memory.current").read_text().strip())
            reclaimable = stat_value((directory / "memory.stat").read_text(), "inactive_file")
        except (OSError, ValueError):
            continue
        if reclaimable is None:
            reclaimable = 0
        left = int(limit) - used + reclaimable
        domains.append(Domain(str(current), max(0, min(left, int(limit)))))
    return domains


def memory_domains() -> list[Domain]:
    """The memory domains this process runs within; none where the platform does not tell."""
    domains = []
    available = host_available_bytes()
    if available is not None:
        domains.append(Domain(None, available))
    cgroup = own_cgroup()
    if cgroup is not None:
        domains.extend(cgroup_domains(cgroup))
    return domains


def resident_bytes(pid: int, proc: pathlib.Path = pathlib.Path("/proc")) -> int | None:
    """A running process's resident size; None once it has ended."""
    try:
        status = (proc / str(pid) / "status").read_text()
    except OSError:
        return None
    for line in status.splitlines():
        if not line.startswith("VmRSS:"):
            continue
        fields = line.split()
        if len(fields) == 3 and fields[1].isdigit() and fields[2] == "kB":
            return int(fields[1]) * 1024
    # A process that has ended but is not yet reaped has no resident size.
    return None


def outstanding_bytes(lease: Lease, resident: Callable[[int], int | None]) -> int:
    """The growth an admitted compilation may still add to the memory it uses now."""
    if lease.driver_pid is None:
        return lease.reservation_bytes
    used = resident(lease.driver_pid)
    if used is None:
        # Its driver has ended and its wrapper is about to release its place.
        return 0
    return max(0, lease.reservation_bytes - used)


@dataclasses.dataclass(frozen=True)
class Decision:
    """One waiter's decision against the queue and the memory its domains have left."""

    admitted: bool
    fits: bool
    ahead: int
    holders: tuple[Lease, ...]
    # The least any domain has left after the growth still to come of the compilations admitted
    # in it; None when no domain tells its memory.
    left_bytes: int | None


def decide(waiter: Lease, leases: Sequence[Lease], domains: Sequence[Domain], resident: Callable[[int], int | None], now_ns: int) -> Decision:
    """Whether `waiter` is admitted now, among the live `leases` of the queue."""
    holders = []
    ahead = 0
    for lease in leases:
        if lease.identifier == waiter.identifier:
            continue
        if lease.admitted_ns is not None:
            holders.append(lease)
            continue
        if not lease.queued_before(waiter):
            continue
        if now_ns - lease.polled_ns > RESPONSIVE_NS:
            continue
        ahead += 1
    left = None
    for domain in domains:
        growth = 0
        for holder in holders:
            if domain.contains(holder):
                growth += outstanding_bytes(holder, resident)
        domain_left = domain.available_bytes - growth
        if left is None or domain_left < left:
            left = domain_left
    fits = left is not None and waiter.reservation_bytes <= left
    admitted = ahead == 0 and (fits or not holders)
    return Decision(admitted=admitted, fits=fits, ahead=ahead, holders=tuple(holders), left_bytes=left)


class QueueLocked(Exception):
    """The queue lock stayed held until a waiter's deadline."""


class Queue:
    """The host-wide queue in one directory: its decision lock and its leases."""

    def __init__(self, directory: pathlib.Path) -> None:
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        information = directory.lstat()
        if not stat.S_ISDIR(information.st_mode) or information.st_uid != os.getuid():
            raise OSError(errno.EPERM, "the admission queue is not a directory this user owns", str(directory))
        self.leases = directory / "leases"
        self.leases.mkdir(mode=0o700, exist_ok=True)
        self.lock = os.open(directory / "queue.lock", os.O_RDWR | os.O_CREAT, 0o600)

    def close(self) -> None:
        os.close(self.lock)

    @contextmanager
    def deciding(self, deadline: float) -> Iterator[None]:
        """Hold the queue lock; raise QueueLocked when it is still held at the monotonic `deadline`."""
        while True:
            try:
                fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise QueueLocked from None
                time.sleep(LOCK_RETRY_SECONDS)
        try:
            yield
        finally:
            fcntl.flock(self.lock, fcntl.LOCK_UN)

    def enter(self, lease: Lease) -> tuple[int, pathlib.Path]:
        """Lock a new lease file and place it in the queue; return its descriptor and path."""
        path = self.leases / f"{lease.identifier}.json"
        # A lease enters the queue already locked, so no decision can take it for stale.
        temporary = self.leases / f".{lease.identifier}.tmp"
        descriptor = os.open(temporary, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            write(descriptor, lease)
            os.rename(temporary, path)
        except OSError:
            temporary.unlink(missing_ok=True)
            os.close(descriptor)
            raise
        return descriptor, path

    def live(self, report: TextIO) -> list[Lease]:
        """The leases whose holders still hold them; the others are deleted.

        Called with the queue lock held, so no lease changes while it is read.
        """
        leases = []
        for path in sorted(self.leases.glob("*.json")):
            try:
                descriptor = os.open(path, os.O_RDONLY)
            except FileNotFoundError:
                continue
            try:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    data = os.pread(descriptor, os.fstat(descriptor).st_size, 0)
                    try:
                        leases.append(Lease.decode(data))
                    except ValueError as error:
                        # Another worktree's tooling can hold a lease of another shape. Its
                        # compilation stays in the memory the host reports, only not in the queue.
                        print(f"nervix analysis: skipping the admission lease {path}: {error}", file=report, flush=True)
                    continue
                # Its holder ended without releasing it, and the kernel released its lock.
                path.unlink(missing_ok=True)
            finally:
                os.close(descriptor)
        return leases


def write(descriptor: int, lease: Lease) -> None:
    data = lease.encode()
    os.pwrite(descriptor, data, 0)
    os.ftruncate(descriptor, len(data))


def gib(amount: int) -> str:
    return f"{amount / GIB:.1f} GiB"


@dataclasses.dataclass
class Place:
    """An admitted compilation's lease, held until its driver ends."""

    queue: Queue
    lease: Lease
    descriptor: int
    path: pathlib.Path

    def name_driver(self, driver_pid: int) -> None:
        self.lease.driver_pid = driver_pid
        try:
            with self.queue.deciding(time.monotonic() + POLL_SECONDS):
                write(self.descriptor, self.lease)
        except QueueLocked:
            # Later decisions keep counting the whole reservation, so they only wait longer.
            pass

    def release(self) -> None:
        # A decision that read the lease before it disappears counts it once more; one that opens
        # it after its lock is gone deletes it as stale. Neither needs the queue lock.
        self.path.unlink(missing_ok=True)
        os.close(self.descriptor)
        self.queue.close()


class Admission:
    """How a declared compilation was admitted, and its place in the queue until it is released."""

    def __init__(self, record: dict[str, Any], place: Place | None) -> None:
        self.record = record
        self.place = place

    def started(self, driver_pid: int) -> None:
        """Name the driver, so later decisions count only the growth still to come of it."""
        if self.place is not None:
            self.place.name_driver(driver_pid)

    def release(self) -> None:
        if self.place is not None:
            self.place.release()


def admit(policy: Policy, compiled: Compiled, reservation: int, *, configuration: str, root: str, report: TextIO = sys.stderr) -> Admission:
    """Wait at most the policy's wait limit for `compiled`'s admission.

    A queue that cannot be used admits nothing: the compilation starts at once without a place, and
    its record and report name the failure.
    """
    started = time.monotonic()
    try:
        return wait(policy, compiled, reservation, configuration=configuration, root=root, report=report, started=started)
    except OSError as error:
        waited = round(time.monotonic() - started, 1)
        print(f"nervix analysis: {compiled.crate_name} ({compiled.kind}) in the {configuration} analysis starts without host memory admission after {waited:.0f} s: its queue in {policy.directory} cannot be used: {error}", file=report, flush=True)
        record = {"reservation_bytes": reservation, "outcome": "unavailable", "waited_seconds": waited, "left_bytes": None, "holders": 0, "error": str(error)}
        return Admission(record, None)


def wait(policy: Policy, compiled: Compiled, reservation: int, *, configuration: str, root: str, report: TextIO, started: float) -> Admission:
    queue = Queue(policy.directory)
    now_ns = time.time_ns()
    lease = Lease(
        identifier=uuid.uuid4().hex, pid=os.getpid(), crate_name=compiled.crate_name, kind=compiled.kind,
        configuration=configuration, root=root, reservation_bytes=reservation, cgroup=own_cgroup(),
        enqueued_ns=now_ns, polled_ns=now_ns, admitted_ns=None, driver_pid=None,
    )
    try:
        descriptor, path = queue.enter(lease)
    except OSError:
        queue.close()
        raise
    place = Place(queue, lease, descriptor, path)
    try:
        record = decide_until_admitted(place, policy, report, started)
    except OSError:
        place.release()
        raise
    return Admission(record, place)


def decide_until_admitted(place: Place, policy: Policy, report: TextIO, started: float) -> dict[str, Any]:
    """Decide every poll until the place is admitted or the wait limit passes; return the record."""
    lease = place.lease
    deadline = started + policy.wait_limit_seconds
    reported = None
    outcome = None
    while outcome is None:
        try:
            with place.queue.deciding(deadline):
                now_ns = time.time_ns()
                lease.polled_ns = now_ns
                decision = decide(lease, place.queue.live(report), memory_domains(), resident_bytes, now_ns)
                if decision.admitted and decision.fits:
                    outcome = "fit"
                elif decision.admitted:
                    outcome = "alone"
                elif time.monotonic() >= deadline:
                    outcome = "wait_limit"
                if outcome is not None:
                    lease.admitted_ns = now_ns
                write(place.descriptor, lease)
        except QueueLocked:
            # The holder of the queue lock stopped while deciding. This compilation starts at its
            # wait limit, and its lease counts it as admitted once the lock returns.
            decision = Decision(admitted=False, fits=False, ahead=0, holders=(), left_bytes=None)
            outcome = "queue_locked"
            lease.admitted_ns = time.time_ns()
            write(place.descriptor, lease)
        if outcome is not None:
            break
        if reported is None or time.monotonic() - reported >= REPORT_SECONDS:
            print(waiting(lease, decision, time.monotonic() - started, policy), file=report, flush=True)
            reported = time.monotonic()
        time.sleep(POLL_SECONDS)
    waited = round(time.monotonic() - started, 1)
    if reported is not None or outcome != "fit":
        print(admitted(lease, decision, outcome, waited), file=report, flush=True)
    return {
        "reservation_bytes": lease.reservation_bytes,
        "outcome": outcome,
        "waited_seconds": waited,
        "left_bytes": decision.left_bytes,
        "holders": len(decision.holders),
        "error": None,
    }


def waiting(lease: Lease, decision: Decision, waited: float, policy: Policy) -> str:
    text = f"nervix analysis: {lease.crate_name} ({lease.kind}) in the {lease.configuration} analysis waits for host memory admission of its {gib(lease.reservation_bytes)} reservation"
    if decision.ahead:
        plural = "" if decision.ahead == 1 else "s"
        text += f" behind {decision.ahead} older waiting compilation{plural}"
    if decision.holders:
        holders = "; ".join(holder.describe() for holder in decision.holders)
        text += f"; admitted: {holders}"
    if decision.left_bytes is not None:
        text += f"; {gib(decision.left_bytes)} left"
    return text + f"; waited {waited:.0f} s of at most {policy.wait_limit_seconds:.0f} s"


def admitted(lease: Lease, decision: Decision, outcome: str, waited: float) -> str:
    text = f"nervix analysis: {lease.crate_name} ({lease.kind}) in the {lease.configuration} analysis "
    reservation = gib(lease.reservation_bytes)
    if outcome == "queue_locked":
        return text + f"starts after {waited:.0f} s, its wait limit, without admission: the host's admission queue stayed locked"
    if outcome == "wait_limit":
        text += f"starts at its wait limit after {waited:.0f} s, although its {reservation} reservation does not fit"
    elif outcome == "alone":
        text += f"is admitted after {waited:.0f} s with no other analysis compilation admitted, although its {reservation} reservation does not fit"
    else:
        text += f"is admitted after waiting {waited:.0f} s; its {reservation} reservation fits"
    if decision.left_bytes is None:
        return text + "; this platform does not report its memory, so declared compilations run one at a time"
    plural = "" if len(decision.holders) == 1 else "s"
    return text + f" in the {gib(decision.left_bytes)} left beside {len(decision.holders)} admitted compilation{plural}"
