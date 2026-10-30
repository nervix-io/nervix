#!/usr/bin/env python3
"""Record kache health, where a job's compile time went, and the causes of the most expensive misses.

A job reports after its build, when it builds before it runs, and again at its end, whatever its
outcome. The report covers every build on the job's machine, which no other job shares. It gives
the window in which kache saw compilation and the time kache tracked on cacheable crates: the hits
it served, the misses it compiled, and how that time splits without overlap into compiling the
misses and kache's own work, such as computing cache keys, waiting for a cache permit, and restoring
and storing outputs. It adds what moved to and from the remote cache: downloads with their sizes
and times, and uploads. Together with the job's steps, which show whether its build or its run took
the time, a slow job reads as slow compilation, a cache that missed, a cache whose hits were slow,
or slow execution.
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
# so the window holds the whole job and nothing from before it. For the same reason the report
# filters no build tree: kache leaves the remote transfers out of a report filtered by one.
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


def gibibytes(size: float) -> str:
    return f"{size / 1024**3:.1f} GiB"


def clock_time(timestamp: str) -> str | None:
    """The hour and minute of one of kache's RFC 3339 timestamps, which it writes in UTC."""
    match = re.search(r"T(\d{2}:\d{2})", timestamp)
    return match.group(1) if match else None


@dataclass(frozen=True)
class CompileTime:
    """Where the compile time kache tracked went, from its JSON report.

    Kache tracks the time its wrapper spends on every cacheable crate: a hit's time is kache's own
    work, and a miss's time is the compilation plus kache's own work around it. Its phases split
    that tracked time without overlap.
    """

    window_ms: float
    started: str | None
    ended: str | None
    cacheable: int
    local_hits: int
    prefetch_hits: int
    remote_hits: int
    compiled: int
    hit_ms: float
    average_hit_ms: float
    miss_ms: float
    average_miss_ms: float
    compile_ms: float
    key_ms: float
    dep_info_ms: float
    dep_info_runs: int
    flight_wait_ms: float
    permit_wait_ms: float
    wait_ms: float
    restore_ms: float
    store_ms: float
    lookup_ms: float
    startup_ms: float
    unattributed_ms: float
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
            cacheable=int(summary.get("total_crates", 0)),
            local_hits=int(summary.get("local_hits", 0)),
            prefetch_hits=int(summary.get("prefetch_hits", 0)),
            remote_hits=int(summary.get("remote_hits", 0)),
            # Kache counts a crate it compiled beside an identical compilation as a duplicate.
            compiled=int(summary.get("misses", 0)) + int(summary.get("dups", 0)),
            hit_ms=float(timing.get("hit_time_ms", 0)),
            average_hit_ms=float(timing.get("avg_hit_ms", 0)),
            miss_ms=float(timing.get("miss_time_ms", 0)),
            average_miss_ms=float(timing.get("avg_miss_ms", 0)),
            compile_ms=float(timing.get("miss_compile_time_ms", 0)),
            key_ms=float(timing.get("total_key_ms", 0)),
            dep_info_ms=float(timing.get("total_dep_info_ms", 0)),
            dep_info_runs=int(timing.get("dep_info_runs", 0)),
            flight_wait_ms=float(timing.get("total_flight_wait_ms", 0)),
            permit_wait_ms=float(timing.get("total_permit_wait_ms", 0)),
            wait_ms=float(timing.get("total_wait_ms", 0)),
            restore_ms=float(timing.get("total_restore_ms", 0)),
            store_ms=float(timing.get("total_store_ms", 0)),
            lookup_ms=float(timing.get("total_lookup_ms", 0)),
            startup_ms=float(timing.get("total_startup_ms", 0)),
            unattributed_ms=float(timing.get("total_unattributed_ms", 0)),
            suggestions=tuple(str(suggestion) for suggestion in report.get("suggestions") or ()),
        )

    def hits(self) -> int:
        return self.local_hits + self.prefetch_hits + self.remote_hits

    def tracked_ms(self) -> float:
        return self.hit_ms + self.miss_ms

    def share(self, milliseconds: float) -> str:
        tracked = self.tracked_ms()
        if not tracked:
            return ""
        return f"{milliseconds / tracked * 100:.1f}%"

    def phase(self, label: str, milliseconds: float) -> str:
        return f"| {label} | {minutes(milliseconds)} | {self.share(milliseconds)} |"

    def lines(self) -> list[str]:
        window = f"a {minutes(self.window_ms)} window"
        if self.started and self.ended:
            window = f"{window} from {self.started} to {self.ended} UTC"
        lines = [
            "### Where the compile time went",
            "",
            f"Kache saw {self.cacheable} cacheable crates in {window}. It tracked "
            f"{minutes(self.tracked_ms())} on them, adding up every parallel rustc, so it can "
            "exceed the window.",
            "",
            "| Crates | Count | Tracked time | Each |",
            "| --- | ---: | ---: | ---: |",
            f"| Served from the cache: {self.local_hits} local, {self.prefetch_hits} prefetched, "
            f"{self.remote_hits} remote | {self.hits()} | {minutes(self.hit_ms)} | {seconds(self.average_hit_ms)} |",
            f"| Compiled after a cache miss | {self.compiled} | {minutes(self.miss_ms)} | {seconds(self.average_miss_ms)} |",
            "",
            "| Where the tracked time went | Time | Share |",
            "| --- | ---: | ---: |",
            self.phase("Compiling the misses", self.compile_ms),
            self.phase("Computing cache keys", self.key_ms),
            self.phase(f"of which the dep-info pre-pass, {self.dep_info_runs} runs", self.dep_info_ms),
            self.phase(
                f"Waiting: {minutes(self.permit_wait_ms)} for a cache permit, "
                f"{minutes(self.flight_wait_ms)} for the same crate compiling in another process",
                self.wait_ms,
            ),
            self.phase("Restoring cached outputs", self.restore_ms),
            self.phase("Storing compiled outputs", self.store_ms),
            self.phase("Looking up the cache", self.lookup_ms),
            self.phase("Starting the wrapper", self.startup_ms),
            self.phase("Attributed to no phase", self.unattributed_ms),
        ]
        if self.suggestions:
            lines.extend(["", "Kache's reading of the window:", ""])
            lines.extend(f"- {suggestion}" for suggestion in self.suggestions)
        return lines


@dataclass(frozen=True)
class RemoteTransfer:
    """What moved between the job's store and the remote cache, from kache's JSON report."""

    downloads: int
    failed_downloads: int
    downloaded_bytes: int
    average_download_ms: float
    p95_download_ms: float
    max_download_ms: float
    slot_wait_ms: float
    most_concurrent_downloads: int
    dominant_phase: str | None
    dominant_phase_percent: float
    uploads: int
    failed_uploads: int
    uploaded_bytes: int

    @classmethod
    def from_report(cls, report: Mapping[str, Any]) -> RemoteTransfer | None:
        """None when kache recorded no transfer in the report's window."""
        network = report.get("network")
        if not network:
            return None
        return cls(
            downloads=int(network.get("downloads_ok", 0)),
            failed_downloads=int(network.get("downloads_failed", 0)),
            downloaded_bytes=int(network.get("bytes_down", 0)),
            average_download_ms=float(network.get("avg_download_ms", 0)),
            p95_download_ms=float(network.get("p95_download_ms", 0)),
            max_download_ms=float(network.get("max_download_ms", 0)),
            slot_wait_ms=float(network.get("total_semaphore_wait_ms", 0)),
            most_concurrent_downloads=int(network.get("max_concurrent_downloads", 0)),
            dominant_phase=network.get("dominant_download_phase") or None,
            dominant_phase_percent=float(network.get("dominant_download_phase_pct", 0)),
            uploads=int(network.get("uploads_ok", 0)),
            failed_uploads=int(network.get("uploads_failed", 0)),
            uploaded_bytes=int(network.get("bytes_up", 0)),
        )

    def lines(self) -> list[str]:
        download_times = (
            f"{seconds(self.average_download_ms)} on average, {seconds(self.p95_download_ms)} at the "
            f"95th percentile, {seconds(self.max_download_ms)} at most"
        )
        lines = [
            "### Remote transfer",
            "",
            "| Direction | Objects | Failed | Size | Time each |",
            "| --- | ---: | ---: | ---: | --- |",
            f"| Downloaded | {self.downloads} | {self.failed_downloads} | {gibibytes(self.downloaded_bytes)} | {download_times} |",
            f"| Uploaded | {self.uploads} | {self.failed_uploads} | {gibibytes(self.uploaded_bytes)} | |",
            "",
            f"Downloads waited {minutes(self.slot_wait_ms)} in all for a transfer slot, with at most "
            f"{self.most_concurrent_downloads} at once.",
        ]
        if self.dominant_phase:
            lines[-1] += (
                f" Most of their time went to {self.dominant_phase}: "
                f"{self.dominant_phase_percent:.1f}%."
            )
        return lines



def report_lines(report: Mapping[str, Any]) -> list[str]:
    """The summary lines of kache's JSON report, before the costliest misses' diagnoses."""
    summary = report.get("summary") or {}
    lines = [
        "",
        f"Hits: {summary.get('local_hits', 0)} local, {summary.get('prefetch_hits', 0)} prefetched, {summary.get('remote_hits', 0)} remote. Misses: {summary.get('misses', 0)}; passthroughs: {summary.get('passthroughs', 0)}.",
        f"Hit rate: {summary.get('hit_rate_pct', 0)}%; compile time the hits saved, by kache's estimate: {minutes(float(summary.get('time_saved_ms', 0)))}.",
    ]
    for store in (report.get("storage") or {}).get("stores", []):
        used = store.get("bytes", 0)
        limit = store.get("max_size", 0)
        lines.append(f"Store: {gibibytes(used)} of {gibibytes(limit)} configured.")
    lines.append("")
    compile_time = CompileTime.from_report(report)
    if compile_time is None:
        lines.append("Kache recorded no compilation in this report's window.")
    else:
        lines.extend(compile_time.lines())
    transfer = RemoteTransfer.from_report(report)
    if transfer is not None:
        lines.append("")
        lines.extend(transfer.lines())
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
    doctor = run("kache", "doctor")
    (destination / "doctor.txt").write_text(doctor.stdout + doctor.stderr)

    report = run("kache", "report", "--format", "json", "--since", f"{REPORT_WINDOW_HOURS}h", "--top", "20")
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
