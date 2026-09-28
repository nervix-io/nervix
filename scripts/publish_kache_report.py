#!/usr/bin/env python3
"""Record kache health and the causes of the most expensive misses for one CI job."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys


def run(*args: str) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(args, text=True, capture_output=True, check=False)
    except OSError as error:
        return subprocess.CompletedProcess(args, 127, "", str(error))


def main() -> None:
    destination = Path(sys.argv[1])
    destination.mkdir(parents=True, exist_ok=True)
    root = os.environ.get("GITHUB_WORKSPACE", os.getcwd())
    doctor = run("kache", "doctor")
    (destination / "doctor.txt").write_text(doctor.stdout + doctor.stderr)

    report = run("kache", "report", "--format", "json", "--since", "3h", "--top", "20", "--root", root)
    (destination / "report.json").write_text(report.stdout)
    markdown = [f"## Kache: {os.environ.get('GITHUB_JOB', 'local build')}", ""]
    markdown.append(f"Doctor exit status: `{doctor.returncode}`. Problems are diagnostic only.")
    if doctor.stdout or doctor.stderr:
        markdown.extend(["", "<details><summary>Doctor output</summary>", "", "```text", (doctor.stdout + doctor.stderr).strip(), "```", "", "</details>"])

    if report.returncode:
        markdown.extend(["", f"Report failed with status `{report.returncode}`: {report.stderr.strip()}"])
    else:
        data = json.loads(report.stdout)
        summary = data.get("summary", {})
        markdown.extend(
            [
                "",
                f"Hits: {summary.get('local_hits', 0)} local, {summary.get('prefetch_hits', 0)} prefetched, {summary.get('remote_hits', 0)} remote. Misses: {summary.get('misses', 0)}; passthroughs: {summary.get('passthroughs', 0)}.",
                f"Hit rate: {summary.get('hit_rate_pct', 0)}%; estimated time saved: {summary.get('time_saved_ms', 0)} ms.",
            ]
        )
        stores = data.get("storage", {}).get("stores", [])
        for store in stores:
            used = store.get("bytes", 0)
            limit = store.get("max_size", 0)
            markdown.append(f"Store: {used / 1024**3:.1f} GiB of {limit / 1024**3:.1f} GiB configured.")
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
