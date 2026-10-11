#!/usr/bin/env python3
"""Sample Linux runner CPU use, steal time and memory while a CI job builds and tests."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
import signal
import sys
import threading
import time

# A sample every five seconds keeps an hour-long job's record small and still shows a build's peak.
DEFAULT_INTERVAL_SECONDS = 5.0


def cpu_counters() -> tuple[int, int, int]:
    fields = Path("/proc/stat").read_text().splitlines()[0].split()
    # Linux already includes guest time in user and nice; counting those fields again would make
    # utilization and steal percentages smaller than the time the runner actually received.
    counts = [int(value) for value in fields[1:9]]
    total = sum(counts)
    idle = counts[3] + counts[4]
    steal = counts[7]
    return total, idle, steal


@dataclass(frozen=True)
class Memory:
    """Runner memory in MiB. Used memory is what the kernel cannot give a new allocation without
    reclaiming it, so page cache that can be dropped does not count."""

    total_mib: float
    used_mib: float
    swap_used_mib: float


def memory_sample(meminfo: str) -> Memory:
    kibibytes = {}
    for line in meminfo.splitlines():
        name, _, value = line.partition(":")
        parts = value.split()
        if parts:
            kibibytes[name] = int(parts[0])
    total = kibibytes["MemTotal"]
    used = total - kibibytes["MemAvailable"]
    swap_used = kibibytes.get("SwapTotal", 0) - kibibytes.get("SwapFree", 0)
    return Memory(total / 1024, used / 1024, swap_used / 1024)


def record(csv_path: Path, summary_path: Path, interval: float, stopped: threading.Event) -> None:
    """Sample the runner every `interval` seconds until `stopped`, then write the summary."""
    began = time.monotonic()
    previous = cpu_counters()
    total_mib = memory_sample(Path("/proc/meminfo").read_text()).total_mib
    rows: list[tuple[float, float, float, float, float]] = []
    with csv_path.open("w", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(
            ("elapsed_seconds", "cpu_utilization_pct", "steal_pct", "memory_used_mib", "swap_used_mib")
        )
        while not stopped.wait(interval):
            current = cpu_counters()
            memory = memory_sample(Path("/proc/meminfo").read_text())
            ticks = current[0] - previous[0]
            if ticks > 0:
                stolen_ticks = current[2] - previous[2]
                utilization = 100 * (ticks - (current[1] - previous[1]) - stolen_ticks) / ticks
                steal = 100 * stolen_ticks / ticks
                row = (
                    round(time.monotonic() - began, 1),
                    round(utilization, 2),
                    round(steal, 2),
                    round(memory.used_mib, 1),
                    round(memory.swap_used_mib, 1),
                )
                rows.append(row)
                writer.writerow(row)
                output.flush()
            previous = current

    count = len(rows)
    metrics = []
    for name, column, unit in (
        ("CPU utilization", 1, "%"),
        ("CPU steal time", 2, "%"),
        ("Memory used", 3, " MiB"),
        ("Swap used", 4, " MiB"),
    ):
        values = [row[column] for row in rows]
        mean = sum(values) / count if count else 0
        peak = max(values, default=0)
        metrics.append(f"| {name} | {mean:.1f}{unit} | {peak:.1f}{unit} |\n")
    summary_path.write_text(
        "## Runner load\n\n"
        f"Duration: {time.monotonic() - began:.1f} s; samples: {count} ({interval:g} s cadence); "
        f"memory: {total_mib:.0f} MiB.\n\n"
        "| Metric | Mean | Peak |\n| --- | ---: | ---: |\n"
        + "".join(metrics)
    )


def main() -> None:
    csv_path, summary_path = (Path(argument) for argument in sys.argv[1:3])
    interval = float(sys.argv[3]) if len(sys.argv) > 3 else DEFAULT_INTERVAL_SECONDS
    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda _signal, _frame: stopped.set())
    signal.signal(signal.SIGINT, lambda _signal, _frame: stopped.set())
    record(csv_path, summary_path, interval, stopped)


if __name__ == "__main__":
    main()
