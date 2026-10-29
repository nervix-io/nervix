from __future__ import annotations

import dataclasses
import io
import json
import os
import pathlib
import random
import subprocess
import sys
import tempfile
import time
import unittest
from typing import Callable
from unittest import mock

from scripts import analysis_admission
from scripts.analysis_admission import Compiled, Decision, Domain, GIB, Lease, Policy, Queue, decide

ROOT = pathlib.Path(__file__).resolve().parents[2]


def lease(identifier: str, *, reservation: int = 8 * GIB, enqueued: int = 1_000, polled: int | None = None, admitted: int | None = None, driver: int | None = None, cgroup: str | None = "/user.slice/one.scope") -> Lease:
    if polled is None:
        polled = enqueued
    return Lease(
        identifier=identifier, pid=100, crate_name="nervix_server", kind="lib", configuration="testing",
        root="/work/ws-1", reservation_bytes=reservation, cgroup=cgroup, enqueued_ns=enqueued,
        polled_ns=polled, admitted_ns=admitted, driver_pid=driver,
    )


def unknown(_pid: int) -> int | None:
    return None


class NamingTests(unittest.TestCase):
    def test_every_library_form_is_one_lib_and_other_kinds_keep_their_name(self) -> None:
        for kinds in (["lib"], ["rlib"], ["cdylib", "rlib"], ["staticlib"], ["proc-macro"]):
            self.assertEqual(analysis_admission.target_kind(kinds), "lib")
        self.assertEqual(analysis_admission.target_kind(["bin"]), "bin")
        self.assertEqual(analysis_admission.target_kind(["custom-build"]), "custom-build")

    def test_the_queue_is_one_directory_per_user_whatever_the_temporary_directory(self) -> None:
        with mock.patch.dict(os.environ, {"TMPDIR": "/private/session"}):
            directory = analysis_admission.host_directory()
        self.assertEqual(directory, pathlib.Path("/tmp") / f"nervix-analysis-admission-{os.getuid()}")


class RepresentationTests(unittest.TestCase):
    def test_a_policy_survives_the_environment_unchanged(self) -> None:
        policy = Policy(pathlib.Path("/tmp/queue"), 4 * GIB, 1800.0, {Compiled("nervix_server", "lib"): 16 * GIB, Compiled("nervix_server", "bin"): 5 * GIB, Compiled("alpha", "lib"): 6 * GIB})
        self.assertEqual(Policy.decode(policy.encode()), policy)
        self.assertEqual(policy.reservation(Compiled("nervix_server", "bin")), 5 * GIB)
        self.assertIsNone(policy.reservation(Compiled("nervix_models", "lib")))
        empty = Policy(pathlib.Path("/tmp/queue"), GIB, 0.5, {})
        self.assertEqual(Policy.decode(empty.encode()), empty)

    def test_a_lease_survives_its_file_unchanged(self) -> None:
        for current in (lease("a"), lease("b", admitted=2_000, driver=4_321, cgroup=None)):
            self.assertEqual(Lease.decode(current.encode()), current)

    def test_every_bounded_policy_and_lease_survive_their_encodings(self) -> None:
        generator = random.Random(86)
        alphabet = "abc_-/ é漢\\\"\n"

        def text(minimum: int = 0) -> str:
            return "".join(generator.choice(alphabet) for _ in range(generator.randint(minimum, 12)))

        def optional(value: object) -> object:
            return None if generator.random() < 0.3 else value

        for _ in range(500):
            reservations = {}
            for _ in range(generator.randint(0, 5)):
                reservations[Compiled(text(1), generator.choice(["lib", "bin"]))] = generator.randint(1, 2**45)
            policy = Policy(pathlib.Path("/" + text()), generator.randint(1, 2**40), generator.choice([0.25, 60.0, 1800.0, 2.0**30]), reservations)
            self.assertEqual(Policy.decode(policy.encode()), policy)
            current = Lease(
                identifier=text(1), pid=generator.randint(1, 2**22), crate_name=text(1), kind=generator.choice(["lib", "bin"]),
                configuration=text(), root=text(), reservation_bytes=generator.randint(1, 2**45), cgroup=optional(text()),
                enqueued_ns=generator.randint(0, 2**63 - 1), polled_ns=generator.randint(0, 2**63 - 1),
                admitted_ns=optional(generator.randint(0, 2**63 - 1)), driver_pid=optional(generator.randint(1, 2**22)),
            )
            self.assertEqual(Lease.decode(current.encode()), current)

    def test_a_lease_of_another_shape_is_rejected(self) -> None:
        value = json.loads(lease("a").encode())
        for changed in (
            {key: item for key, item in value.items() if key != "driver_pid"},
            {**value, "state": "waiting"},
            {**value, "pid": "100"},
            {**value, "reservation_bytes": True},
            {**value, "crate_name": None},
            {**value, "cgroup": 3},
        ):
            with self.subTest(changed=changed):
                with self.assertRaises(ValueError):
                    Lease.decode(json.dumps(changed).encode())
        with self.assertRaises(ValueError):
            Lease.decode(b"[]")
        with self.assertRaises(ValueError):
            Lease.decode(b"{")


class MemoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = pathlib.Path(self.temporary.name)

    def test_the_hosts_available_memory_comes_from_meminfo(self) -> None:
        meminfo = self.root / "meminfo"
        self.assertIsNone(analysis_admission.host_available_bytes(meminfo))
        meminfo.write_text("MemTotal:       126812684 kB\nMemFree:        25165824 kB\nMemAvailable:   96468992 kB\n")
        self.assertEqual(analysis_admission.host_available_bytes(meminfo), 96468992 * 1024)
        meminfo.write_text("MemTotal:       126812684 kB\nMemAvailable:   unknown kB\n")
        self.assertIsNone(analysis_admission.host_available_bytes(meminfo))
        meminfo.write_text("MemTotal:       126812684 kB\n")
        self.assertIsNone(analysis_admission.host_available_bytes(meminfo))

    def test_the_own_cgroup_comes_from_the_unified_hierarchy(self) -> None:
        membership = self.root / "cgroup"
        self.assertIsNone(analysis_admission.own_cgroup(membership))
        membership.write_text("12:memory:/legacy\n")
        self.assertIsNone(analysis_admission.own_cgroup(membership))
        membership.write_text("12:memory:/legacy\n0::/user.slice/run-1.scope\n")
        self.assertEqual(analysis_admission.own_cgroup(membership), "/user.slice/run-1.scope")

    def limit(self, path: str, limit: str, used: int | None = None, inactive: int | None = None) -> None:
        directory = self.root / "hierarchy" / path.lstrip("/")
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "memory.max").write_text(limit + "\n")
        if used is not None:
            (directory / "memory.current").write_text(f"{used}\n")
        if inactive is not None:
            (directory / "memory.stat").write_text(f"anon 1\ninactive_file {inactive}\nactive_file 7\n")

    def test_every_limited_ancestor_cgroup_is_a_domain_with_what_it_has_left(self) -> None:
        hierarchy = self.root / "hierarchy"
        self.limit("/", "max")
        self.limit("/user.slice", "max", 1, 0)
        self.limit("/user.slice/build.slice", str(24 * GIB), 10 * GIB, 2 * GIB)
        self.limit("/user.slice/build.slice/run.scope", str(20 * GIB), 15 * GIB, 0)
        domains = analysis_admission.cgroup_domains("/user.slice/build.slice/run.scope", hierarchy)
        self.assertEqual(domains, [
            Domain("/user.slice/build.slice/run.scope", 5 * GIB),
            Domain("/user.slice/build.slice", 16 * GIB),
        ])

    def test_unreadable_or_unlimited_cgroups_are_not_domains_and_left_memory_is_bounded(self) -> None:
        hierarchy = self.root / "hierarchy"
        self.limit("/a", str(8 * GIB), 9 * GIB, 0)
        self.limit("/a/b", str(4 * GIB))
        self.limit("/a/b/c", "garbage", 1, 1)
        self.limit("/a/b/c/d", str(2 * GIB), 0, 5 * GIB)
        self.assertEqual(analysis_admission.cgroup_domains("/a/b/c/d/e", hierarchy), [
            Domain("/a/b/c/d", 2 * GIB),
            Domain("/a", 0),
        ])
        self.limit("/x", str(GIB), 0)
        (hierarchy / "x/memory.stat").write_text("anon 1\n")
        self.assertEqual(analysis_admission.cgroup_domains("/x", hierarchy), [Domain("/x", GIB)])

    def test_the_domains_are_the_host_and_its_limited_cgroups(self) -> None:
        with mock.patch("scripts.analysis_admission.host_available_bytes", return_value=40 * GIB), mock.patch("scripts.analysis_admission.own_cgroup", return_value="/run.scope"), mock.patch("scripts.analysis_admission.cgroup_domains", return_value=[Domain("/run.scope", GIB)]) as cgroups:
            self.assertEqual(analysis_admission.memory_domains(), [Domain(None, 40 * GIB), Domain("/run.scope", GIB)])
        cgroups.assert_called_once_with("/run.scope")
        with mock.patch("scripts.analysis_admission.host_available_bytes", return_value=None), mock.patch("scripts.analysis_admission.own_cgroup", return_value=None):
            self.assertEqual(analysis_admission.memory_domains(), [])

    def test_a_running_drivers_resident_size_comes_from_its_status(self) -> None:
        proc = self.root / "proc"
        self.assertIsNone(analysis_admission.resident_bytes(7, proc))
        (proc / "7").mkdir(parents=True)
        (proc / "7/status").write_text("Name:\tdriver\nState:\tZ (zombie)\n")
        self.assertIsNone(analysis_admission.resident_bytes(7, proc))
        (proc / "7/status").write_text("Name:\tdriver\nVmHWM:\t 9000 kB\nVmRSS:\t 4096 kB\n")
        self.assertEqual(analysis_admission.resident_bytes(7, proc), 4096 * 1024)
        self.assertGreater(analysis_admission.resident_bytes(os.getpid()), 0)

    def test_an_admitted_compilation_still_owes_its_growth(self) -> None:
        self.assertEqual(analysis_admission.outstanding_bytes(lease("a", admitted=1), unknown), 8 * GIB)
        started = lease("a", admitted=1, driver=7)
        self.assertEqual(analysis_admission.outstanding_bytes(started, lambda pid: 3 * GIB), 5 * GIB)
        self.assertEqual(analysis_admission.outstanding_bytes(started, lambda pid: 9 * GIB), 0)
        self.assertEqual(analysis_admission.outstanding_bytes(started, unknown), 0)


class DecisionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.waiter = lease("w", enqueued=5_000, polled=9_000)

    def decide(self, leases: list[Lease], domains: list[Domain], now: int = 10_000) -> Decision:
        return decide(self.waiter, [self.waiter, *leases], domains, lambda pid: 2 * GIB, now)

    def test_a_reservation_that_fits_beside_the_admitted_growth_is_admitted(self) -> None:
        holder = lease("h", admitted=1, driver=11)
        fits = self.decide([holder], [Domain(None, 14 * GIB)])
        self.assertEqual(fits, Decision(admitted=True, fits=True, ahead=0, holders=(holder,), left_bytes=8 * GIB))
        short = self.decide([holder], [Domain(None, 13 * GIB)])
        self.assertEqual((short.admitted, short.fits, short.left_bytes), (False, False, 7 * GIB))

    def test_a_compilation_with_nothing_admitted_is_admitted_whether_or_not_it_fits(self) -> None:
        self.assertEqual(self.decide([], [Domain(None, 2 * GIB)]), Decision(admitted=True, fits=False, ahead=0, holders=(), left_bytes=2 * GIB))
        self.assertEqual(self.decide([], []), Decision(admitted=True, fits=False, ahead=0, holders=(), left_bytes=None))
        unknown_memory = self.decide([lease("h", admitted=1)], [])
        self.assertEqual((unknown_memory.admitted, unknown_memory.left_bytes), (False, None))

    def test_the_tightest_domain_decides_and_counts_only_the_growth_inside_it(self) -> None:
        inside = lease("in", admitted=1, cgroup="/build.slice/run.scope")
        outside = lease("out", admitted=1, cgroup="/user.slice/other.scope")
        undeclared = lease("none", admitted=1, cgroup=None)
        similar = lease("similar", admitted=1, cgroup="/build.slice-2/run.scope")
        domains = [Domain(None, 100 * GIB), Domain("/build.slice", 20 * GIB)]
        decision = self.decide([inside, outside, undeclared, similar], domains)
        self.assertEqual(decision.left_bytes, 12 * GIB)
        self.assertTrue(decision.admitted)
        tight_host = self.decide([inside, outside, undeclared, similar], [Domain(None, 39 * GIB), Domain("/build.slice", 20 * GIB)])
        self.assertEqual((tight_host.admitted, tight_host.left_bytes), (False, 7 * GIB))

    def test_older_waiters_that_still_decide_go_first(self) -> None:
        older = lease("older", enqueued=4_000, polled=9_500)
        stalled = lease("stalled", enqueued=3_000, polled=10_000 - analysis_admission.RESPONSIVE_NS - 1)
        same_time = lease("a", enqueued=5_000, polled=9_500)
        later_name = lease("x", enqueued=5_000, polled=9_500)
        younger = lease("younger", enqueued=6_000, polled=9_500)
        domains = [Domain(None, 100 * GIB)]
        self.assertEqual(self.decide([older], domains).ahead, 1)
        self.assertFalse(self.decide([older], domains).admitted)
        self.assertEqual(self.decide([stalled, later_name, younger], domains), Decision(admitted=True, fits=True, ahead=0, holders=(), left_bytes=100 * GIB))
        self.assertEqual(self.decide([same_time], domains).ahead, 1)
        self.assertEqual(self.decide([older, same_time], domains).ahead, 2)


@dataclasses.dataclass
class Due:
    """An action a clock runs once, when a sleep first reaches its time."""

    elapsed_ns: int
    action: Callable[[], None]


class AdmissionClock:
    """The clocks an admission reads, which only its own sleeps and the test move.

    An admission measures its wait on the monotonic clock and stamps its lease with the host's Unix
    nanoseconds. Here both move together, the Unix one from `unix_ns`, so a recorded wait is exactly
    the time this clock passed, however long the host took to run the admission. A sleep passes at
    once, then runs each action that has come due before the admission decides again.
    """

    def __init__(self, unix_ns: int) -> None:
        self.unix_ns = unix_ns
        self.elapsed_ns = 0
        self.due: list[Due] = []

    def monotonic(self) -> float:
        return self.elapsed_ns / 10**9

    def time_ns(self) -> int:
        return self.unix_ns + self.elapsed_ns

    def advance(self, seconds: float) -> None:
        self.elapsed_ns += round(seconds * 10**9)

    def sleep(self, seconds: float) -> None:
        self.advance(seconds)
        while self.due and self.due[0].elapsed_ns <= self.elapsed_ns:
            item = self.due.pop(0)
            item.action()

    def at(self, seconds: float, action: Callable[[], None]) -> None:
        self.due.append(Due(round(seconds * 10**9), action))
        self.due.sort(key=lambda item: item.elapsed_ns)


class QueueTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = pathlib.Path(self.temporary.name) / "queue"
        self.report = io.StringIO()

    def queue(self) -> Queue:
        queue = Queue(self.directory)
        self.addCleanup(queue.close)
        return queue

    def child(self, body: str) -> subprocess.Popen[str]:
        script = "import os, pathlib, sys, time\nfrom scripts import analysis_admission\nfrom scripts.analysis_admission import Lease, Queue\n" + body
        process = subprocess.Popen([sys.executable, "-c", script, str(self.directory)], cwd=ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        self.addCleanup(process.wait)
        self.addCleanup(lambda: process.poll() is None and process.kill())
        self.addCleanup(process.stdout.close)
        self.addCleanup(process.stdin.close)
        self.assertEqual(process.stdout.readline(), "ready\n")
        return process

    def holder(self, *, reservation: int, admitted: bool = True) -> subprocess.Popen[str]:
        """A process holding one lease until its standard input closes."""
        current = lease("holder", reservation=reservation, enqueued=0, admitted=0 if admitted else None)
        body = (
            f"queue = Queue(pathlib.Path(sys.argv[1]))\ncurrent = Lease.decode({current.encode()!r})\n"
            "current.enqueued_ns = current.polled_ns = time.time_ns()\n"
            "if current.admitted_ns is not None:\n    current.admitted_ns = current.enqueued_ns\n"
            "descriptor, path = queue.enter(current)\n"
            "print('ready', flush=True)\nsys.stdin.read()\npath.unlink()\n"
        )
        return self.child(body)

    @staticmethod
    def end(holder: subprocess.Popen[str]) -> None:
        """Let a holder release its lease, and wait until it has."""
        holder.stdin.close()
        holder.wait(timeout=60)

    def clock_after(self, identifier: str) -> AdmissionClock:
        """A clock that starts just after the last poll of the lease `identifier`."""
        polled = Lease.decode((self.directory / "leases" / f"{identifier}.json").read_bytes()).polled_ns
        return AdmissionClock(polled + 1)

    def test_a_held_lease_is_live_and_a_released_one_leaves_the_queue(self) -> None:
        holder = self.holder(reservation=3 * GIB)
        queue = self.queue()
        with queue.deciding(time.monotonic() + 5):
            live = queue.live(self.report)
        self.assertEqual([(item.identifier, item.reservation_bytes) for item in live], [("holder", 3 * GIB)])
        holder.stdin.close()
        holder.wait()
        with queue.deciding(time.monotonic() + 5):
            self.assertEqual(queue.live(self.report), [])
        self.assertEqual(list((self.directory / "leases").iterdir()), [])

    def test_the_lease_of_a_holder_that_ended_without_releasing_it_is_deleted(self) -> None:
        holder = self.holder(reservation=3 * GIB)
        holder.kill()
        holder.wait()
        self.assertEqual(len(list((self.directory / "leases").glob("*.json"))), 1)
        queue = self.queue()
        with queue.deciding(time.monotonic() + 5):
            self.assertEqual(queue.live(self.report), [])
        self.assertEqual(list((self.directory / "leases").iterdir()), [])

    def test_a_held_lease_of_another_shape_is_skipped_and_reported(self) -> None:
        body = (
            "queue = Queue(pathlib.Path(sys.argv[1]))\n"
            "import fcntl\n"
            "path = queue.leases / 'other.json'\n"
            "descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)\n"
            "fcntl.flock(descriptor, fcntl.LOCK_EX)\n"
            "os.write(descriptor, b'{\"state\": \"admitted\"}')\n"
            "print('ready', flush=True)\nsys.stdin.read()\n"
        )
        self.child(body)
        queue = self.queue()
        with queue.deciding(time.monotonic() + 5):
            self.assertEqual(queue.live(self.report), [])
        self.assertIn("skipping the admission lease", self.report.getvalue())
        self.assertIn("other.json", self.report.getvalue())

    def test_a_lease_released_while_the_queue_is_read_is_not_live(self) -> None:
        holder = self.holder(reservation=3 * GIB)
        queue = self.queue()
        opened = os.open

        def released_first(path, flags, *arguments):
            if pathlib.Path(path).name == "holder.json":
                holder.stdin.close()
                holder.wait()
            return opened(path, flags, *arguments)

        with queue.deciding(time.monotonic() + 5), mock.patch("scripts.analysis_admission.os.open", side_effect=released_first):
            self.assertEqual(queue.live(self.report), [])

    def test_a_queue_must_be_this_users_directory(self) -> None:
        self.directory.parent.mkdir(parents=True, exist_ok=True)
        elsewhere = self.directory.parent / "elsewhere"
        elsewhere.mkdir()
        self.directory.symlink_to(elsewhere)
        with self.assertRaisesRegex(OSError, "not a directory this user owns"):
            Queue(self.directory)

    def test_a_lock_held_past_the_deadline_is_reported_to_the_waiter(self) -> None:
        self.child("import fcntl\nqueue = Queue(pathlib.Path(sys.argv[1]))\nfcntl.flock(queue.lock, fcntl.LOCK_EX)\nprint('ready', flush=True)\nsys.stdin.read()\n")
        queue = self.queue()
        with self.assertRaises(analysis_admission.QueueLocked):
            with queue.deciding(time.monotonic() + 0.05):
                pass

    def admit(self, reservation: int, *, wait_limit: float = 30.0, available: int = 10 * GIB, released: bool = True, clock: AdmissionClock | None = None) -> analysis_admission.Admission:
        """Admit `reservation` on `clock`, a new one unless given, so every wait it records is exact."""
        if clock is None:
            clock = AdmissionClock(time.time_ns())
        policy = Policy(self.directory, GIB, wait_limit, {Compiled("nervix_server", "lib"): reservation})
        constants = {"POLL_SECONDS": 0.02, "REPORT_SECONDS": 0.2, "RESPONSIVE_NS": int(0.2 * 10**9)}
        with mock.patch.multiple("scripts.analysis_admission", **constants), mock.patch("scripts.analysis_admission.memory_domains", return_value=[Domain(None, available)]), mock.patch("scripts.analysis_admission.time", clock):
            admission = analysis_admission.admit(policy, Compiled("nervix_server", "lib"), reservation, configuration="testing", root="/work/ws-1", report=self.report)
        if released:
            self.addCleanup(admission.release)
        return admission

    def test_a_fitting_compilation_is_admitted_at_once_and_silently(self) -> None:
        admission = self.admit(4 * GIB, released=False)
        self.assertEqual(admission.record, {"reservation_bytes": 4 * GIB, "outcome": "fit", "waited_seconds": 0.0, "left_bytes": 10 * GIB, "holders": 0, "error": None})
        self.assertEqual(self.report.getvalue(), "")
        stored = Lease.decode(admission.place.path.read_bytes())
        self.assertIsNotNone(stored.admitted_ns)
        self.assertEqual((stored.crate_name, stored.configuration, stored.root, stored.pid), ("nervix_server", "testing", "/work/ws-1", os.getpid()))
        admission.started(4321)
        self.assertEqual(Lease.decode(admission.place.path.read_bytes()).driver_pid, 4321)
        admission.release()
        self.assertFalse(admission.place.path.exists())
        self.assertEqual(list((self.directory / "leases").iterdir()), [])

    def test_an_immediate_fit_waits_exactly_the_setup_its_clock_measured(self) -> None:
        # A loaded host can take long to set the queue up before an immediate fit. The record's
        # wait is the time on the admission's clock, which counts that setup, not the host's own.
        clock = AdmissionClock(time.time_ns())
        enter = Queue.enter

        def slow_setup(queue: Queue, entered: Lease) -> tuple[int, pathlib.Path]:
            time.sleep(0.2)
            clock.advance(0.8)
            return enter(queue, entered)

        with mock.patch.object(Queue, "enter", autospec=True, side_effect=slow_setup):
            admission = self.admit(4 * GIB, clock=clock)
        self.assertEqual(admission.record, {"reservation_bytes": 4 * GIB, "outcome": "fit", "waited_seconds": 0.8, "left_bytes": 10 * GIB, "holders": 0, "error": None})
        self.assertEqual(self.report.getvalue(), "")

    def test_a_compilation_that_does_not_fit_waits_for_an_admitted_one_to_end(self) -> None:
        holder = self.holder(reservation=8 * GIB)
        clock = AdmissionClock(time.time_ns())
        clock.at(0.5, lambda: self.end(holder))
        admission = self.admit(8 * GIB, clock=clock)
        self.assertEqual(admission.record, {"reservation_bytes": 8 * GIB, "outcome": "fit", "waited_seconds": 0.5, "left_bytes": 10 * GIB, "holders": 0, "error": None})
        output = self.report.getvalue()
        self.assertIn("nervix_server (lib) in the testing analysis waits for host memory admission of its 8.0 GiB reservation; admitted: nervix_server (lib) in the testing analysis of /work/ws-1, pid", output)
        self.assertIn("2.0 GiB left; waited 0 s of at most 30 s", output)
        self.assertIn("is admitted after waiting", output)
        self.assertIn("its 8.0 GiB reservation fits in the 10.0 GiB left beside 0 admitted compilations", output)

    def test_a_younger_compilation_waits_behind_an_older_waiter(self) -> None:
        older = self.holder(reservation=GIB, admitted=False)
        # The older waiter last decided just before the younger one's clock starts, and it leaves
        # within the responsive window that follows, so it holds the younger one back until then.
        clock = self.clock_after("holder")
        clock.at(0.1, lambda: self.end(older))
        admission = self.admit(GIB, clock=clock)
        self.assertEqual(admission.record, {"reservation_bytes": GIB, "outcome": "fit", "waited_seconds": 0.1, "left_bytes": 10 * GIB, "holders": 0, "error": None})
        self.assertIn("behind 1 older waiting compilation", self.report.getvalue())

    def test_a_stopped_older_waiter_does_not_hold_back_the_queue(self) -> None:
        self.holder(reservation=GIB, admitted=False)
        # The older waiter never decides again, so it stops holding the younger one back once the
        # responsive window after its last poll has passed.
        admission = self.admit(GIB, clock=self.clock_after("holder"))
        self.assertEqual(admission.record, {"reservation_bytes": GIB, "outcome": "fit", "waited_seconds": 0.2, "left_bytes": 10 * GIB, "holders": 0, "error": None})
        self.assertIn("behind 1 older waiting compilation", self.report.getvalue())

    def test_a_compilation_alone_is_admitted_although_it_does_not_fit(self) -> None:
        admission = self.admit(16 * GIB)
        self.assertEqual(admission.record, {"reservation_bytes": 16 * GIB, "outcome": "alone", "waited_seconds": 0.0, "left_bytes": 10 * GIB, "holders": 0, "error": None})
        self.assertIn("is admitted after 0 s with no other analysis compilation admitted, although its 16.0 GiB reservation does not fit in the 10.0 GiB left beside 0 admitted compilations", self.report.getvalue())

    def test_without_reported_memory_declared_compilations_run_one_at_a_time(self) -> None:
        policy = Policy(self.directory, GIB, 30.0, {Compiled("nervix_server", "lib"): GIB})
        with mock.patch("scripts.analysis_admission.memory_domains", return_value=[]):
            admission = analysis_admission.admit(policy, Compiled("nervix_server", "lib"), GIB, configuration="testing", root="/work/ws-1", report=self.report)
        self.addCleanup(admission.release)
        self.assertEqual((admission.record["outcome"], admission.record["left_bytes"]), ("alone", None))
        self.assertIn("although its 1.0 GiB reservation does not fit; this platform does not report its memory, so declared compilations run one at a time", self.report.getvalue())

    def test_the_wait_limit_bounds_every_wait(self) -> None:
        self.holder(reservation=8 * GIB)
        admission = self.admit(8 * GIB, wait_limit=0.3)
        self.assertEqual(admission.record, {"reservation_bytes": 8 * GIB, "outcome": "wait_limit", "waited_seconds": 0.3, "left_bytes": 2 * GIB, "holders": 1, "error": None})
        self.assertIn("starts at its wait limit after 0 s, although its 8.0 GiB reservation does not fit in the 2.0 GiB left beside 1 admitted compilation", self.report.getvalue())

    def test_a_queue_lock_held_through_the_wait_limit_starts_the_compilation(self) -> None:
        self.child("import fcntl\nqueue = Queue(pathlib.Path(sys.argv[1]))\nfcntl.flock(queue.lock, fcntl.LOCK_EX)\nprint('ready', flush=True)\nsys.stdin.read()\n")
        admission = self.admit(GIB, wait_limit=0.2)
        self.assertEqual(admission.record, {"reservation_bytes": GIB, "outcome": "queue_locked", "waited_seconds": 0.2, "left_bytes": None, "holders": 0, "error": None})
        self.assertIn("without admission: the host's admission queue stayed locked", self.report.getvalue())
        self.assertIsNotNone(Lease.decode(admission.place.path.read_bytes()).admitted_ns)

    def test_a_driver_named_while_the_queue_is_locked_keeps_the_whole_reservation(self) -> None:
        admission = self.admit(GIB)
        self.child("import fcntl\nqueue = Queue(pathlib.Path(sys.argv[1]))\nfcntl.flock(queue.lock, fcntl.LOCK_EX)\nprint('ready', flush=True)\nsys.stdin.read()\n")
        with mock.patch("scripts.analysis_admission.POLL_SECONDS", 0.05):
            admission.started(99)
        self.assertIsNone(Lease.decode(admission.place.path.read_bytes()).driver_pid)

    def test_a_queue_that_cannot_be_used_starts_the_compilation_without_a_place(self) -> None:
        self.directory.parent.mkdir(parents=True, exist_ok=True)
        elsewhere = self.directory.parent / "elsewhere"
        elsewhere.mkdir()
        self.directory.symlink_to(elsewhere)
        policy = Policy(self.directory, GIB, 1.0, {})
        with mock.patch("scripts.analysis_admission.time", AdmissionClock(time.time_ns())):
            admission = analysis_admission.admit(policy, Compiled("nervix_server", "lib"), GIB, configuration="testing", root="/work/ws-1", report=self.report)
        self.assertIsNone(admission.place)
        self.assertEqual(admission.record, {"reservation_bytes": GIB, "outcome": "unavailable", "waited_seconds": 0.0, "left_bytes": None, "holders": 0, "error": f"[Errno 1] the admission queue is not a directory this user owns: '{self.directory}'"})
        self.assertIn(f"nervix_server (lib) in the testing analysis starts without host memory admission after 0 s: its queue in {self.directory} cannot be used:", self.report.getvalue())
        admission.started(1)
        admission.release()
        self.assertEqual(list(elsewhere.iterdir()), [])

    def test_a_lease_that_cannot_enter_the_queue_leaves_nothing_behind(self) -> None:
        policy = Policy(self.directory, GIB, 1.0, {})
        with mock.patch("scripts.analysis_admission.os.rename", side_effect=OSError(28, "No space left on device")):
            admission = analysis_admission.admit(policy, Compiled("nervix_server", "lib"), GIB, configuration="testing", root="/work/ws-1", report=self.report)
        self.assertEqual((admission.place, admission.record["outcome"], admission.record["error"]), (None, "unavailable", "[Errno 28] No space left on device"))
        self.assertEqual(list((self.directory / "leases").iterdir()), [])

    def test_a_queue_that_fails_while_waiting_releases_its_place(self) -> None:
        self.holder(reservation=8 * GIB)
        with mock.patch("scripts.analysis_admission.Queue.live", side_effect=OSError(5, "Input/output error")):
            admission = self.admit(8 * GIB)
        self.assertEqual((admission.record["outcome"], admission.record["error"]), ("unavailable", "[Errno 5] Input/output error"))
        self.assertEqual([path.name for path in (self.directory / "leases").iterdir()], ["holder.json"])


if __name__ == "__main__":
    unittest.main()
