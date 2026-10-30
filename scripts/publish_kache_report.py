#!/usr/bin/env python3
"""Record kache health, where a job's compile time went, and the causes of the most expensive misses.

A job reports after its build, when it builds before it runs, and again at its end, whatever its
outcome. The report says how long kache saw compilation run and splits that time into compiling
cache misses, serving cache hits and kache's own work: computing cache keys, waiting for a cache
permit, and storing and restoring outputs. Together with the job's steps, which show whether its
build or its run took the time, a slow job reads as slow compilation, a cache that missed, a cache
whose hits were slow, or slow execution.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import subprocess
from collections.abc import Mapping, Sequence
from typing import Any

PHASES = ("build", "job")
# The report's window. No kache-backed job may run longer, and each job has its machine to itself,
# so the window holds the whole job and nothing from before it.
REPORT_WINDOW_HOURS = 6


def run(*args: str) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(args, text=True, capture_output=True, check=False)
    except OSError as error:
        return subprocess.CompletedProcess(args, 127, "", str(error))


def heading(job: str, phase: str) -> str:
    """The report's heading: the job, and whether the report covers its build or the whole job."""
    if phase == "build":
        return f"## Kache: {job}, after the build"
    return f"## Kache: {job}"


def minutes(milliseconds: float) -> str:
    return f"{milliseconds / 60_000:.1f} min"


def seconds(milliseconds: float) -> str:
    return f"{milliseconds / 1000:.1f} s"


def clock_time(timestamp: str) -> str | None:
    """The hour and minute of one of kache's RFC 3339 timestamps, which it writes in UTC."""
    match = re.search(r"T(\d{2}:\d{2})", timestamp)
    return match.group(1) if match else None


@dataclass(frozen=True)
class CompileTime:
    """Where the compile time kache observed went, from its JSON report."""

    window_ms: float
    started: str | None
    ended: str | None
    cacheable: int
    compiled: int
    compile_ms: float
    local_hits: int
    prefetch_hits: int
    remote_hits: int
    hit_ms: float
    average_hit_ms: float
    key_ms: float
    average_key_ms: float
    permit_wait_ms: float
    store_ms: float
    restore_ms: float
    suggestions: tuple[str, ...]

    @classmethod
    def from_report(cls, report: Mapping[str, Any]) -> CompileTime | None:
        """None when kache recorded no compilation in the report's window."""
        timeline = report.get("timeline") or {}
        summary = report.get("summary") or {}
        timing = report.get("timing") or {}
        if not timeline.get("event_count"):
            return None
        return cls(
            window_ms=float(timeline.get("duration_ms", 0)),
            started=clock_time(str(timeline.get("start_time", ""))),
            ended=clock_time(str(timeline.get("end_time", ""))),
            cacheable=int(summary.get("total_crates", timeline.get("cacheable_count", 0))),
            compiled=int(summary.get("misses", 0)),
            compile_ms=float(timing.get("miss_compile_time_ms", 0)),
            local_hits=int(summary.get("local_hits", 0)),
            prefetch_hits=int(summary.get("prefetch_hits", 0)),
            remote_hits=int(summary.get("remote_hits", 0)),
            hit_ms=float(timing.get("hit_time_ms", 0)),
            average_hit_ms=float(timing.get("avg_hit_ms", 0)),
            key_ms=float(timing.get("total_key_ms", 0)),
            average_key_ms=float(timing.get("avg_key_ms", 0)),
            permit_wait_ms=float(timing.get("total_permit_wait_ms", 0)),
            store_ms=float(timing.get("total_store_ms", 0)),
            restore_ms=float(timing.get("total_restore_ms", 0)),
            suggestions=tuple(str(suggestion) for suggestion in report.get("suggestions") or ()),
        )

    def hits(self) -> int:
        return self.local_hits + self.prefetch_hits + self.remote_hits

    def lines(self) -> list[str]:
        window = f"a {minutes(self.window_ms)} window"
        if self.started and self.ended:
            window = f"{window} from {self.started} to {self.ended} UTC"
        compiled_each = seconds(self.compile_ms / self.compiled) if self.compiled else ""
        lines = [
            "### Where the compile time went",
            "",
            f"Kache saw {self.cacheable} cacheable crates in {window}. Each total adds up every "
            "parallel rustc, so together they exceed the window.",
            "",
            "| Part | Crates | Total | Each |",
            "| --- | ---: | ---: | ---: |",
            f"| Compiled after a cache miss | {self.compiled} | {minutes(self.compile_ms)} | {compiled_each} |",
            f"| Served from the cache: {self.local_hits} local, {self.prefetch_hits} prefetched, "
            f"{self.remote_hits} remote | {self.hits()} | {minutes(self.hit_ms)} | {seconds(self.average_hit_ms)} |",
            f"| Computing cache keys | | {minutes(self.key_ms)} | {seconds(self.average_key_ms)} |",
            f"| Waiting for a cache permit | | {minutes(self.permit_wait_ms)} | |",
            f"| Storing compiled outputs | | {minutes(self.store_ms)} | |",
            f"| Restoring cached outputs | | {minutes(self.restore_ms)} | |",
        ]
        if self.suggestions:
            lines.extend(["", "Kache's reading of the window:", ""])
            lines.extend(f"- {suggestion}" for suggestion in self.suggestions)
        return lines


def report_lines(report: Mapping[str, Any]) -> list[str]:
    """The summary lines of kache's JSON report, before the costliest misses' diagnoses."""
    summary = report.get("summary") or {}
    lines = [
        "",
        f"Hits: {summary.get('local_hits', 0)} local, {summary.get('prefetch_hits', 0)} prefetched, {summary.get('remote_hits', 0)} remote. Misses: {summary.get('misses', 0)}; passthroughs: {summary.get('passthroughs', 0)}.",
        f"Hit rate: {summary.get('hit_rate_pct', 0)}%; estimated time saved: {summary.get('time_saved_ms', 0)} ms.",
    ]
    for store in (report.get("storage") or {}).get("stores", []):
        used = store.get("bytes", 0)
        limit = store.get("max_size", 0)
        lines.append(f"Store: {used / 1024**3:.1f} GiB of {limit / 1024**3:.1f} GiB configured.")
    lines.append("")
    compile_time = CompileTime.from_report(report)
    if compile_time is None:
        lines.append("Kache recorded no compilation in this report's window.")
    else:
        lines.extend(compile_time.lines())
    return lines


def main(arguments: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("destination", type=Path)
    parser.add_argument(
        "--phase",
        choices=PHASES,
        default="job",
        help="whether the report covers the job's build or the whole job",
    )
    options = parser.parse_args(arguments)
    destination = options.destination
    destination.mkdir(parents=True, exist_ok=True)
    root = os.environ.get("GITHUB_WORKSPACE", os.getcwd())
    doctor = run("kache", "doctor")
    (destination / "doctor.txt").write_text(doctor.stdout + doctor.stderr)

    report = run(
        "kache", "report", "--format", "json", "--since", f"{REPORT_WINDOW_HOURS}h", "--top", "20", "--root", root
    )
    (destination / "report.json").write_text(report.stdout)
    markdown = [heading(os.environ.get("GITHUB_JOB", "local build"), options.phase), ""]
    markdown.append(f"Doctor exit status: `{doctor.returncode}`. Problems are diagnostic only.")
    if doctor.stdout or doctor.stderr:
        markdown.extend(["", "<details><summary>Doctor output</summary>", "", "```text", (doctor.stdout + doctor.stderr).strip(), "```", "", "</details>"])

    if report.returncode:
        markdown.extend(["", f"Report failed with status `{report.returncode}`: {report.stderr.strip()}"])
    else:
        data = json.loads(report.stdout)
        markdown.extend(report_lines(data))
        misses = []
        named = set()
        for miss in data.get("top_misses", []):
            crate = miss.get("crate_name")
            if crate and crate not in named:
                misses.append(miss)
                named.add(crate)
            if len(misses) == 5:
                break
        if misses:
            markdown.extend(["", "### Top misses", ""])
        for miss in misses:
            crate = miss["crate_name"]
            diagnosis = run("kache", "why-miss", crate)
            (destination / f"why-miss-{crate}.txt").write_text(
                diagnosis.stdout + diagnosis.stderr
            )
            markdown.extend(
                [
                    f"#### `{crate}` ({miss.get('compile_time_ms', 0)} ms compile)",
                    "",
                    "```text",
                    (diagnosis.stdout + diagnosis.stderr).strip() or "No diagnosis returned.",
                    "```",
                    "",
                ]
            )

    rendered = "\n".join(markdown) + "\n"
    (destination / "summary.md").write_text(rendered)
    if summary_path := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(summary_path).open("a") as summary_file:
            summary_file.write(rendered)
    print(rendered)


if __name__ == "__main__":
    main()
