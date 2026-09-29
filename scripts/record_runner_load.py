#!/usr/bin/env python3
"""Sample Linux runner CPU use and steal time while a CI job builds and tests."""

from __future__ import annotations

import csv
from pathlib import Path
import signal
import sys
import threading
import time


def cpu_counters() -> tuple[int, int, int]:
    fields = Path("/proc/stat").read_text().splitlines()[0].split()
    # Linux already includes guest time in user and nice; counting those fields again would make
    # utilization and steal percentages smaller than the time the runner actually received.
    counts = [int(value) for value in fields[1:9]]
    total = sum(counts)
    idle = counts[3] + counts[4]
    steal = counts[7]
    return total, idle, steal


def main() -> None:
    csv_path, summary_path = (Path(argument) for argument in sys.argv[1:3])
    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda _signal, _frame: stopped.set())
    signal.signal(signal.SIGINT, lambda _signal, _frame: stopped.set())
    began = time.monotonic()
    previous = cpu_counters()
    rows: list[tuple[float, float, float]] = []
    with csv_path.open("w", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(("elapsed_seconds", "cpu_utilization_pct", "steal_pct"))
        while not stopped.wait(5):
            current = cpu_counters()
            ticks = current[0] - previous[0]
            if ticks > 0:
                stolen_ticks = current[2] - previous[2]
                utilization = 100 * (ticks - (current[1] - previous[1]) - stolen_ticks) / ticks
                steal = 100 * stolen_ticks / ticks
                row = (round(time.monotonic() - began, 1), round(utilization, 2), round(steal, 2))
                rows.append(row)
                writer.writerow(row)
                output.flush()
            previous = current

    count = len(rows)
    mean_cpu = sum(row[1] for row in rows) / count if count else 0
    mean_steal = sum(row[2] for row in rows) / count if count else 0
    peak_cpu = max((row[1] for row in rows), default=0)
    peak_steal = max((row[2] for row in rows), default=0)
    summary_path.write_text(
        "## Runner load\n\n"
        f"Duration: {time.monotonic() - began:.1f} s; samples: {count} (5 s cadence).\n\n"
        "| Metric | Mean | Peak |\n| --- | ---: | ---: |\n"
        f"| CPU utilization | {mean_cpu:.1f}% | {peak_cpu:.1f}% |\n"
        f"| CPU steal time | {mean_steal:.1f}% | {peak_steal:.1f}% |\n"
    )


if __name__ == "__main__":
    main()
