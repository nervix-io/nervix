#!/usr/bin/env python3
"""Run the analysis driver as Cargo's workspace compiler and record how each compilation ended.

Cargo's configured compiler wrapper reports a compiler ended by a signal with the same exit status
as a compile error, so the driver runs as this process's child instead of replacing it. A
compilation the runner's admission policy gives a reservation first waits for its host memory
admission, which it holds until the driver ends. Every compilation leaves a compilation record in
the `compilations` directory beside the compiler reports: its exit status or signal, its start,
duration and peak resident size, the out-of-memory kills the kernel counted in this cgroup while it
ran, and its admission. The driver still ends with this process, as it did when it replaced it, so
a compiler wrapper that stops this process also stops the driver.
"""

from __future__ import annotations

import ctypes
import json
import os
import pathlib
import resource
import signal
import subprocess
import sys
import time
from typing import Any, Callable

# Cargo runs this file as a program, which puts only its own directory on the import path.
ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import analysis_admission

PR_SET_PDEATHSIG = 1
FORWARDED = (signal.SIGHUP, signal.SIGINT, signal.SIGQUIT, signal.SIGTERM)


def command(arguments: list[str]) -> list[str]:
    """The driver invocation, under the native coverage recorder when an attempt selects it."""
    driver = [os.environ["NERVIX_LINT_DRIVER"], *arguments]
    if os.environ.get("NERVIX_NATIVE_COVERAGE_ATTEMPT"):
        recorder = pathlib.Path(__file__).with_name("native_coverage.py")
        return [sys.executable, str(recorder), "exec", *driver]
    return driver


def cgroup_oom_kills(
    membership_path: pathlib.Path = pathlib.Path("/proc/self/cgroup"),
    hierarchy_root: pathlib.Path = pathlib.Path("/sys/fs/cgroup"),
) -> int | None:
    """The kernel's count of out-of-memory kills in this process's cgroup, where cgroup v2 has it."""
    try:
        membership = membership_path.read_text()
    except OSError:
        return None
    for line in membership.splitlines():
        hierarchy, separator, group = line.partition("::")
        if hierarchy != "0" or not separator:
            continue
        try:
            events = (hierarchy_root / group.lstrip("/") / "memory.events").read_text()
        except OSError:
            return None
        for event in events.splitlines():
            name, _, count = event.partition(" ")
            if name == "oom_kill" and count.isdigit():
                return int(count)
    return None


def ending_with(parent: int) -> Callable[[], None] | None:
    """Make the child receive SIGKILL when its parent, this wrapper, ends."""
    if not sys.platform.startswith("linux"):
        return None
    prctl = ctypes.CDLL(None, use_errno=True).prctl

    def end_with_parent() -> None:
        if prctl(PR_SET_PDEATHSIG, int(signal.SIGKILL), 0, 0, 0) != 0:
            raise OSError(ctypes.get_errno(), "cannot tie the analysis driver to its wrapper")
        # The parent may have ended before the request took effect.
        if os.getppid() != parent:
            os.kill(os.getpid(), signal.SIGKILL)

    return end_with_parent


def option_values(arguments: list[str], option: str) -> list[str]:
    """Every value of a compiler option, in each spelling the compiler reads: `--long value`,
    `--long=value`, `-X value` and `-Xvalue`."""
    values = []
    for index, argument in enumerate(arguments):
        if argument == option and index + 1 < len(arguments):
            values.append(arguments[index + 1])
        elif option.startswith("--") and argument.startswith(option + "="):
            values.append(argument.removeprefix(option + "="))
        elif not option.startswith("--") and argument.startswith(option) and argument != option:
            values.append(argument.removeprefix(option))
    return values


def compilation(arguments: list[str]) -> dict[str, Any]:
    """The compiled crate, from the same arguments the driver reads."""
    crate_names = option_values(arguments, "--crate-name")
    crate_types = []
    for value in option_values(arguments, "--crate-type"):
        crate_types.extend(value.split(","))
    extra_filename = ""
    for value in option_values(arguments, "-C"):
        if value.startswith("extra-filename="):
            extra_filename = value.removeprefix("extra-filename=")
    sources = [argument for argument in arguments if argument.endswith(".rs")]
    return {
        "crate_name": crate_names[0] if crate_names else None,
        "crate_types": crate_types,
        "extra_filename": extra_filename,
        "source": sources[0] if sources else None,
    }


def ending(status: int) -> dict[str, Any]:
    if status >= 0:
        return {"kind": "exit", "status": status}
    number = -status
    try:
        name = signal.Signals(number).name
    except ValueError:
        name = None
    return {"kind": "signal", "signal": number, "name": name}


def peak_resident_bytes() -> int:
    """The largest resident size of the waited-for child; Linux reports kibibytes, macOS bytes."""
    peak = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    if sys.platform == "darwin":
        return peak
    return peak * 1024


def publish(record: dict[str, Any]) -> pathlib.Path | None:
    reports = os.environ.get("NERVIX_LINT_REPORTS")
    if not reports or not record["crate_name"]:
        return None
    # A compiler report and its compilation record share the artifact's stem, so the records
    # live in a directory of their own.
    directory = pathlib.Path(reports) / "compilations"
    directory.mkdir(exist_ok=True)
    destination = directory / f"{record['crate_name']}{record['extra_filename']}.json"
    temporary = destination.with_suffix(f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(record, sort_keys=True) + "\n")
    temporary.replace(destination)
    return destination


def end_like(number: int) -> int:
    """End this process with the driver's signal, so the compiler wrapper sees what Cargo would."""
    if number not in (signal.SIGKILL, signal.SIGSTOP):
        signal.signal(number, signal.SIG_DFL)
    os.kill(os.getpid(), number)
    # A signal whose default action does not end a process falls back to the shell convention.
    return 128 + number


def admission(compiled: dict[str, Any]) -> analysis_admission.Admission | None:
    """Wait for the host memory admission of a compilation the runner's policy gives a reservation."""
    if not compiled["crate_name"] or not compiled["crate_types"]:
        return None
    policy = analysis_admission.Policy.decode(os.environ["NERVIX_LINT_ADMISSION"])
    named = analysis_admission.Compiled(compiled["crate_name"], analysis_admission.target_kind(compiled["crate_types"]))
    reservation = policy.reservation(named)
    if reservation is None:
        return None
    return analysis_admission.admit(
        policy, named, reservation,
        configuration=os.environ["NERVIX_LINT_CONFIGURATION"], root=os.environ["NERVIX_LINT_ROOT"],
    )


def main(arguments: list[str]) -> int:
    compiled = compilation(arguments)
    admitted = admission(compiled)
    kills_before = cgroup_oom_kills()
    started_unix_ns = time.time_ns()
    started = time.monotonic()
    try:
        # The driver inherits every descriptor this process inherited, as it did when it replaced
        # it: Cargo's jobserver is a pair of them.
        process = subprocess.Popen(command(arguments), close_fds=False, preexec_fn=ending_with(os.getpid()))
        for number in FORWARDED:
            signal.signal(number, lambda received, _frame: process.send_signal(received))
        if admitted is not None:
            admitted.started(process.pid)
        status = process.wait()
    finally:
        if admitted is not None:
            admitted.release()
    kills_after = cgroup_oom_kills()
    oom_kills = None
    if kills_before is not None and kills_after is not None:
        oom_kills = kills_after - kills_before
    admission_record = None
    if admitted is not None:
        admission_record = admitted.record
    record = {
        **compiled,
        "ending": ending(status),
        "peak_resident_bytes": peak_resident_bytes(),
        "started_unix_ns": started_unix_ns,
        "seconds": round(time.monotonic() - started, 1),
        "oom_kills": oom_kills,
        "admission": admission_record,
    }
    try:
        destination = publish(record)
    except OSError as error:
        # The driver's own ending still reaches Cargo below; only its record is missing.
        print(f"nervix analysis: cannot write the compilation record of {record['crate_name']}: {error}", file=sys.stderr, flush=True)
        destination = None
    if status >= 0:
        return status
    signal_name = record["ending"]["name"] or "unnamed"
    print(f"nervix analysis: the compilation of {record['crate_name']} was terminated by signal {-status} ({signal_name}); compilation record: {destination}", file=sys.stderr, flush=True)
    return end_like(-status)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
