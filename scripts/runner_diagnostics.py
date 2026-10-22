#!/usr/bin/env python3
"""Record build-runner identity and short CPU, memory and filesystem calibrations.

All measurements are diagnostic. Missing VM counters and unsupported direct I/O stay unavailable;
the probes neither qualify the runner nor attribute a slowdown to a particular hardware cause.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import re
import resource
import shutil
import signal
import subprocess
import tempfile
import time

from scripts.record_runner_load import frequency_sample

CALIBRATION_SECONDS = 10
TOOL_SECONDS = 3
DISK_BYTES = 64 * 2**20


def read_available(path: Path) -> str | None:
    try:
        return path.read_text().strip()
    except OSError:
        return None


def command_record(command: list[str]) -> dict:
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=1)
        return {"command": command, "returncode": result.returncode,
                "stdout": result.stdout.strip(), "stderr": result.stderr.strip()}
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"command": command, "unavailable": str(error)}


def frequency_policy(root: Path = Path("/sys/devices/system/cpu")) -> dict:
    policies = {}
    for policy in sorted((root / "cpufreq").glob("policy*")):
        policies[policy.name] = {
            name: read_available(policy / name) for name in (
                "affected_cpus", "scaling_driver", "scaling_governor", "scaling_min_freq",
                "scaling_max_freq", "scaling_cur_freq", "cpuinfo_min_freq", "cpuinfo_max_freq",
            )
        }
    return {"policies_khz": policies or None,
            "boost": read_available(root / "cpufreq/boost"),
            "intel_no_turbo": read_available(root / "intel_pstate/no_turbo"),
            "intel_pstate_status": read_available(root / "intel_pstate/status")}


def cpu_identity(cpuinfo: Path = Path("/proc/cpuinfo")) -> dict[str, list[str]]:
    fields: dict[str, set[str]] = {}
    wanted = {"model name", "vendor_id", "cpu family", "model", "stepping", "microcode",
              "Hardware", "CPU implementer", "CPU architecture", "CPU variant", "CPU part",
              "CPU revision"}
    for line in (read_available(cpuinfo) or "").splitlines():
        key, _, value = line.partition(":")
        if key.strip() in wanted:
            fields.setdefault(key.strip(), set()).add(value.strip())
    return {key: sorted(values) for key, values in fields.items()}


def cgroup_limits(root: Path = Path("/sys/fs/cgroup"),
                  membership: Path = Path("/proc/self/cgroup")) -> dict:
    relative = None
    for line in (read_available(membership) or "").splitlines():
        if line.startswith("0::"):
            relative = line[3:].lstrip("/")
    current = root / relative if relative is not None else root
    if not current.is_dir():
        current = root  # A container may already mount its cgroup as the visible root.
    levels = {}
    while current.is_relative_to(root):
        levels[str(current)] = {name: read_available(current / name) for name in (
            "cpu.max", "cpu.stat", "cpuset.cpus.effective", "memory.max", "memory.high", "io.max",
        )}
        if current == root:
            break
        current = current.parent
    return {"membership": relative, "visible_ancestors": levels}


def snapshot(storage: Path) -> dict:
    return {
        "utc": datetime.now(timezone.utc).isoformat(),
        "platform": platform.platform(), "architecture": platform.machine(),
        "logical_cpus": os.cpu_count(), "allowed_cpus": sorted(os.sched_getaffinity(0)),
        "cpu_identity": cpu_identity(),
        "cpu": command_record(["lscpu", "--json"]),
        "frequency": frequency_sample(), "frequency_policy": frequency_policy(),
        "memory_kib": read_available(Path("/proc/meminfo")),
        "cgroup": cgroup_limits(),
        "pressure": {name: read_available(Path("/proc/pressure") / name)
                     for name in ("cpu", "memory", "io")},
        "filesystem": command_record(["findmnt", "--json", "--target", str(storage),
                                      "--output", "TARGET,SOURCE,FSTYPE,OPTIONS"]),
        "block_devices": command_record(["lsblk", "--json", "--bytes", "--exclude", "7", "--output",
                                         "NAME,TYPE,SIZE,MODEL,ROTA,SCHED,TRAN,MOUNTPOINTS"]),
        "free_bytes": os.statvfs(storage).f_bavail * os.statvfs(storage).f_frsize,
    }


def run_probe(command: list[str], timeout: float) -> dict:
    """Bound a tool's whole process group, retaining raw output and process CPU cost."""
    began = time.monotonic()
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    record = {"command": command, "timeout_seconds": timeout}
    try:
        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              text=True, start_new_session=True) as process:
            try:
                stdout, stderr = process.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                stdout, stderr = process.communicate()
                record["timed_out"] = True
            except BaseException:
                os.killpg(process.pid, signal.SIGKILL)
                process.communicate()
                raise
            record.update(returncode=process.returncode, stdout=stdout, stderr=stderr)
    except OSError as error:
        record["unavailable"] = str(error)
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    record["wall_seconds"] = time.monotonic() - began
    record["cpu_seconds"] = after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime
    return record


def calibration(storage: Path) -> dict:
    cpu = min(os.sched_getaffinity(0))
    pin = ["taskset", "--cpu-list", str(cpu)] if shutil.which("taskset") else []
    record = {"tools": {name: command_record([name, "--version"])
                        for name in ("sysbench", "/usr/bin/fio")},
              "cpu": cpu if pin else None, "execution_limit_seconds": CALIBRATION_SECONDS,
              "measurements": {}}
    began = time.monotonic()
    with tempfile.TemporaryDirectory(prefix=".runner-probe-", dir=storage) as directory:
        job = Path(directory) / "probe.fio"
        # No raw device and no global cache invalidation. A byte bound keeps fast disks short;
        # the supervisor's wall bound covers slow disks and unsupported direct I/O.
        job.write_text(f"""[global]
filename=io.bin
ioengine=psync
iodepth=1
numjobs=1
thread=1
size={DISK_BYTES}
direct=1
invalidate=0
exitall_on_error=1
randrepeat=1
refill_buffers=1

[sequential-write]
rw=write
bs={2**20}
end_fsync=1

[sequential-read]
stonewall
rw=read
bs={2**20}
allow_file_create=0

[random-read]
stonewall
rw=randread
bs=4096
io_size={128 * 4096}
allow_file_create=0
""")
        common = ["sysbench", "--threads=1", "--time=1", "--events=0", "--rand-seed=1", "--rand-type=uniform"]
        memory = ["--memory-block-size=64M", "--memory-total-size=1T", "--memory-oper=read"]
        commands = {
            "cpu": [*common, "cpu", "--cpu-max-prime=20000", "run"],
            "memory-sequential": [*common, "memory", *memory, "--memory-access-mode=seq", "run"],
            "memory-random": [*common, "memory", *memory, "--memory-access-mode=rnd", "run"],
            "disk": ["/usr/bin/fio", "--output-format=json", "--eta=never",
                     f"--directory={directory}", str(job)],
        }
        record["fio_job"] = job.read_text()
        for label, command in commands.items():
            remaining = CALIBRATION_SECONDS - (time.monotonic() - began)
            if remaining <= 0:
                record["measurements"][label] = {"unavailable": "calibration wall budget exhausted"}
                continue
            limit = min(4 if label == "disk" else TOOL_SECONDS, remaining)
            record["measurements"][label] = run_probe([*pin, *command], limit)
    return record


def markdown(record: dict) -> str:
    before = record["before"]
    frequency = before["frequency"]
    cpu_name = ", ".join(before["cpu_identity"].get("model name", [])) or "unavailable"
    lines = [f"## Runner diagnostics: {record['label']}", "",
             f"UTC: {before['utc']}; duration: {record['elapsed_seconds']:.2f} s.",
             f"CPU: {cpu_name}; architecture: {before['architecture']}; "
             f"allowed CPUs: {before['allowed_cpus']}; "
             f"guest-reported MHz min/mean/max: {frequency['min_mhz']} / "
             f"{frequency['mean_mhz']} / {frequency['max_mhz']}.",
             f"Filesystem: `{record['storage_path']}`; available: {before['free_bytes'] / 2**30:.1f} GiB.",
             "", "| Probe | Status | Wall seconds | Process CPU seconds |",
             "| --- | --- | ---: | ---: |"]
    for name, probe in record["calibration"]["measurements"].items():
        if "unavailable" in probe:
            status = f"unavailable: {probe['unavailable']}"
        elif probe.get("timed_out"):
            status = "wall guard elapsed"
        else:
            status = f"exit {probe['returncode']}"
        lines.append(f"| {name} | {status} | {probe.get('wall_seconds', 'unavailable')} | "
                     f"{probe.get('cpu_seconds', 'unavailable')} |")
    lines += ["", "sysbench uses one worker for one second of CPU, sequential memory reads and "
              "random memory reads across a 64 MiB block. Its memory latency is per block event, "
              "not individual hardware load latency. fio requests direct I/O, one worker and queue "
              "depth one: 64 MiB of writes with end_fsync, 64 MiB of sequential reads and 128 "
              "random 4 KiB reads. Each tool has a short process-group guard within a ten-second "
              "calibration budget. Temporary files are removed. Unsupported direct I/O is retained "
              "as an error, with no buffered substitute or global cache drops.",
              "", "Compare matching tool versions, arguments and architecture. These short probes "
              "are not sustained benchmarks or physical SSD latency measurements. Guest MHz and "
              "cpufreq values do not measure effective physical clocks. Missing counters stay "
              "unavailable; steal, pressure and throughput alone do not identify a hardware cause."]
    for name, probe in record["calibration"]["measurements"].items():
        lines += ["", f"### {name}: tool output", "", "```text",
                  probe.get("stdout", "unavailable"), probe.get("stderr", ""), "```"]
    lines += ["", "<details><summary>Hardware, limits and raw records</summary>", "", "```json",
              json.dumps(record, indent=2), "```", "", "</details>", ""]
    return "\n".join(lines)



def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--storage-path", type=Path, default=Path.cwd())
    arguments = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9-]+", arguments.label):
        parser.error("label must contain lowercase letters, digits or hyphens")
    began = time.monotonic()
    record = {"label": arguments.label, "storage_path": str(arguments.storage_path.resolve()),
              "identity": {key: os.environ.get(key) for key in (
                  "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_SHA", "GITHUB_JOB", "RUNNER_NAME",
                  "RUNNER_OS", "RUNNER_ARCH", "ImageOS", "ImageVersion",
                  "BUILDPLATFORM", "TARGETPLATFORM", "TARGETARCH", "NERVIX_DIAGNOSTIC_SELECTION",
              )}, "before": snapshot(arguments.storage_path),
              "calibration": calibration(arguments.storage_path),
              "after": {"utc": datetime.now(timezone.utc).isoformat(),
                        "frequency": frequency_sample(), "cgroup": cgroup_limits()}}
    record["elapsed_seconds"] = time.monotonic() - began
    arguments.output.mkdir(parents=True, exist_ok=True)
    (arguments.output / f"{arguments.label}.json").write_text(json.dumps(record, indent=2) + "\n")
    summary = markdown(record)
    (arguments.output / f"{arguments.label}.md").write_text(summary)
    print(summary)
    if path := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(path).open("a") as stream:
            stream.write(summary)


if __name__ == "__main__":
    main()
