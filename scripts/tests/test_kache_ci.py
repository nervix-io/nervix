from __future__ import annotations

import contextlib
from dataclasses import dataclass
import io
import json
import os
from pathlib import Path
import pwd
import re
import shlex
import shutil
import signal
import sqlite3
import stat
import subprocess
import sys
import tempfile
import textwrap
import time
import tomllib
import unittest
from unittest import mock

from scripts import kache_remote, publish_kache_report
from scripts.kache_remote import (
    ConfigurationError,
    DaemonState,
    LocalStore,
    Outcome,
    Publication,
    StartError,
)

WORKFLOWS = (
    Path(".github/workflows/check.yaml"),
    Path(".github/workflows/docker-build.yaml"),
    Path(".github/workflows/bolero.yaml"),
)
CACHE_RECOVERY_BYTES = 64 * 1024**3
# Every job that builds Rust through kache, by workflow.
KACHE_JOBS = {
    "check.yaml": {
        "checks",
        "diagnostic-evidence",
        "deloxide",
        "tests",
        "scenarios-build",
        "coverage",
        "client-conformance",
        "extra-tests",
        "shuttle",
        "loom",
        "loom-qualification",
        "turmoil",
    },
    "docker-build.yaml": {"benchmark", "build-book"},
    "bolero.yaml": {"bolero-random", "bolero-fuzz"},
}
# The jobs that build before they run, with the cache published and reported between the two, so the
# run's outcome cannot lose what the build filled and each phase's time is visible on its own.
BUILD_PHASE_JOBS = {
    "check.yaml": {"tests", "scenarios-build", "client-conformance", "shuttle", "loom", "turmoil", "deloxide"},
    "docker-build.yaml": set(),
    "bolero.yaml": {"bolero-random", "bolero-fuzz"},
}
BUILD_PUBLISH_COMMAND = 'python3 scripts/kache_remote.py publish --job-status "${JOB_STATUS}" "${RUNNER_TEMP}/kache-build-report"'
BUILD_REPORT_COMMAND = 'python3 scripts/publish_kache_report.py --phase build "${RUNNER_TEMP}/kache-build-report"'
# A kache-backed job reads the bucket in its runner's region: Boat's machines run in Europe, and
# GitHub's hosted runners and Blacksmith's runners in the US.
EU_REMOTE = {
    "KACHE_S3_BUCKET": "${{ secrets.KACHE_EU_BUCKET }}",
    "KACHE_S3_REGION": "${{ vars.KACHE_BUCKET_REGION }}",
    "KACHE_S3_ENDPOINT": "${{ secrets.KACHE_EU_BUCKET_ENDPOINT }}",
    "KACHE_S3_ACCESS_KEY": "${{ secrets.KACHE_EU_BUCKET_ACCESS_KEY_ID }}",
    "KACHE_S3_SECRET_KEY": "${{ secrets.KACHE_EU_BUCKET_SECRET_ACCESS_KEY }}",
}
US_REMOTE = {
    "KACHE_S3_BUCKET": "${{ vars.KACHE_BUCKET }}",
    "KACHE_S3_REGION": "${{ vars.KACHE_BUCKET_REGION }}",
    "KACHE_S3_ENDPOINT": "${{ vars.KACHE_BUCKET_ENDPOINT }}",
    "KACHE_S3_ACCESS_KEY": "${{ secrets.KACHE_BUCKET_ACCESS_KEY_ID }}",
    "KACHE_S3_SECRET_KEY": "${{ secrets.KACHE_BUCKET_SECRET_ACCESS_KEY }}",
}
REMOTE_ENVIRONMENT = {
    "KACHE_S3_BUCKET": "kache-ci",
    "KACHE_S3_REGION": "auto",
    "KACHE_S3_ENDPOINT": "https://account-id.r2.cloudflarestorage.com",
    "KACHE_S3_ACCESS_KEY": "test-access-key",
    "KACHE_S3_SECRET_KEY": "test-secret-key",
}
# The publication step's script. A job that failed before its checkout has no script to run.
PUBLISH_SCRIPT = (
    "if [[ -f scripts/kache_remote.py ]]; then\n"
    '  python3 scripts/kache_remote.py publish --job-status "${JOB_STATUS}" "${RUNNER_TEMP}/kache-report"\n'
    "else\n"
    '  echo "::notice::Kache publication did not run because checkout did not provide its script"\n'
    "fi"
)


@dataclass(frozen=True)
class Step:
    name: str
    text: str

    def mapping(self, key: str) -> dict[str, str]:
        """The step's `key:` block as a flat mapping of its entries."""
        match = re.search(rf"^        {key}:\n((?:          .+\n)+)", self.text, re.MULTILINE)
        if match is None:
            return {}
        entries = {}
        for line in match.group(1).splitlines():
            name, _, value = line.strip().partition(": ")
            entries[name] = value
        return entries

    def field(self, key: str) -> str | None:
        match = re.search(rf"^        {key}: (.+)$", self.text, re.MULTILINE)
        return None if match is None else match.group(1)

    def block(self, key: str) -> str | None:
        """The step's `key: |` literal block without its indentation."""
        match = re.search(rf"^        {key}: \|\n((?:          .*\n)+)", self.text, re.MULTILINE)
        if match is None:
            return None
        return textwrap.dedent(match.group(1)).rstrip("\n")


def workflow_jobs(workflow: str) -> dict[str, str]:
    body = workflow.split("\njobs:\n", maxsplit=1)[1]
    starts = list(re.finditer(r"^  ([a-z][a-z0-9-]*):\n", body, re.MULTILINE))
    jobs = {}
    for index, start in enumerate(starts):
        end = starts[index + 1].start() if index + 1 < len(starts) else len(body)
        jobs[start.group(1)] = body[start.start() : end]
    return jobs


def job_steps(job: str) -> list[Step]:
    starts = list(re.finditer(r"^      - ", job, re.MULTILINE))
    steps = []
    for index, start in enumerate(starts):
        end = starts[index + 1].start() if index + 1 < len(starts) else len(job)
        text = job[start.start() : end]
        name = re.search(r"^      (?:- |  )name: (.+)$", text, re.MULTILINE)
        steps.append(Step(name.group(1) if name else "", text))
    return steps


def workflow_env(workflow: str) -> str:
    return workflow.split("\njobs:\n", maxsplit=1)[0]


def regional_remote(job: str) -> dict[str, str]:
    """The settings of the cache bucket in the region of the job's runner."""
    runner = re.search(r"^    runs-on: (\S+)$", job, re.MULTILINE)
    if runner is None:
        raise AssertionError("a kache-backed job names its runner")
    label = runner.group(1)
    if label.startswith("boat-"):
        return EU_REMOTE
    if label.startswith(("ubuntu-", "blacksmith-")):
        return US_REMOTE
    raise AssertionError(f"no cache bucket serves the region of runner {label}")


class KacheWorkflowTests(unittest.TestCase):
    def test_every_kache_job_starts_its_remote_and_publishes_within_the_contract(self) -> None:
        manifest_keys = []
        for path in WORKFLOWS:
            workflow = path.read_text()
            self.assertIn('KACHE_VERSION: "1.2.2"', workflow_env(workflow))
            self.assertIn('KACHE_REMOTE_KEY_LISTING: "false"', workflow_env(workflow))
            self.assertIn(
                f'KACHE_AUTO_RECOVER_MIN_FREE_BYTES: "{CACHE_RECOVERY_BYTES}"', workflow_env(workflow)
            )
            self.assertNotIn("sync --push", workflow)
            self.assertNotIn("configure_kache_remote", workflow)
            self.assertNotIn("actions/cache@", workflow)
            jobs = workflow_jobs(workflow)
            kache_jobs = {name for name, job in jobs.items() if "kunobi-ninja/kache-action" in job}
            self.assertEqual(kache_jobs, KACHE_JOBS[path.name], path)
            for name in sorted(kache_jobs):
                with self.subTest(workflow=path.name, job=name):
                    steps = job_steps(jobs[name])
                    names = [step.name for step in steps]
                    setup = names.index("Setup kache")
                    self.assertIn("uses: kunobi-ninja/kache-action@v1", steps[setup].text)
                    # Without S3 inputs the action neither saves a manifest nor pushes, and it
                    # still stops the job's daemon at the end of the job.
                    self.assertEqual(
                        steps[setup].mapping("with"),
                        {
                            "version": "${{ env.KACHE_VERSION }}",
                            "github-cache": '"false"',
                            "cache-executables": '"true"',
                            "pr-comment": '"false"',
                        },
                    )

                    start = steps[setup + 1]
                    self.assertEqual(start.name, "Start kache remote")
                    self.assertEqual(start.field("run"), "python3 scripts/kache_remote.py start")
                    environment = start.mapping("env")
                    manifest_key = environment.pop("KACHE_MANIFEST_KEY")
                    self.assertTrue(manifest_key.endswith("-${{ runner.os }}-${{ runner.arch }}"), manifest_key)
                    manifest_keys.append(manifest_key.replace("${{ github.job }}", name))
                    self.assertEqual(environment, regional_remote(jobs[name]))
                    for earlier in steps[:setup]:
                        self.assertNotIn("KACHE_", earlier.text, earlier.name)

                    # The report's window holds the whole job, its build included.
                    limit = re.search(r"^    timeout-minutes: (\d+)$", jobs[name], re.MULTILINE)
                    self.assertIsNotNone(limit)
                    self.assertLessEqual(int(limit.group(1)), publish_kache_report.REPORT_WINDOW_HOURS * 60)

                    self.assertEqual(names.count("Publish kache cache"), 1)
                    publish_index = names.index("Publish kache cache")
                    publish = steps[publish_index]
                    self.assertEqual(publish.field("if"), "always()")
                    self.assertEqual(publish.field("timeout-minutes"), "4")
                    self.assertEqual(publish.mapping("env"), {"JOB_STATUS": "${{ job.status }}"})
                    self.assertEqual(publish.block("run"), PUBLISH_SCRIPT)
                    # Every kache-backed job reports, whatever its outcome: the report and its
                    # artifact carry the publication record and where the compile time went.
                    self.assertEqual(names[publish_index + 1], "Publish kache report")
                    self.assertEqual(names[publish_index + 2], "Upload kache report")
                    for reporting in (steps[publish_index + 1], steps[publish_index + 2]):
                        self.assertEqual(reporting.field("if"), "always()", reporting.name)

                    after_build = [index for index, step in enumerate(names) if step == "Publish kache cache after the build"]
                    if name not in BUILD_PHASE_JOBS[path.name]:
                        self.assertEqual(after_build, [])
                        continue
                    self.assertEqual(len(after_build), 1)
                    build_publish = steps[after_build[0]]
                    # Between the build and the run: after the remote starts, before the job's end.
                    self.assertLess(setup + 1, after_build[0])
                    self.assertLess(after_build[0], publish_index)
                    # Only a build that succeeded publishes; a failed one leaves the end's record.
                    self.assertIsNone(build_publish.field("if"))
                    self.assertEqual(build_publish.field("timeout-minutes"), "4")
                    self.assertEqual(build_publish.mapping("env"), {"JOB_STATUS": "${{ job.status }}"})
                    self.assertEqual(build_publish.field("run"), BUILD_PUBLISH_COMMAND)
                    build_report = steps[after_build[0] + 1]
                    self.assertEqual(build_report.name, "Publish kache report after the build")
                    self.assertEqual(build_report.field("run"), BUILD_REPORT_COMMAND)
                    self.assertIn("            ${{ runner.temp }}/kache-build-report\n", steps[publish_index + 2].text)
        self.assertEqual(len(manifest_keys), len(set(manifest_keys)), manifest_keys)

    def test_a_job_takes_the_bucket_of_its_runners_region(self) -> None:
        self.assertEqual(regional_remote("  tests:\n    runs-on: boat-xlarge\n"), EU_REMOTE)
        self.assertEqual(regional_remote("  book:\n    runs-on: ubuntu-24.04\n"), US_REMOTE)
        self.assertEqual(regional_remote("  tests:\n    runs-on: blacksmith-16vcpu-ubuntu-2404\n"), US_REMOTE)
        with self.assertRaisesRegex(AssertionError, "no cache bucket serves the region of runner self-hosted"):
            regional_remote("  tests:\n    runs-on: self-hosted\n")
        with self.assertRaisesRegex(AssertionError, "names its runner"):
            regional_remote("  tests:\n    runs-on: ${{ matrix.runner }}\n")

    def test_the_publication_step_records_a_failed_job_and_tolerates_a_missing_checkout(self) -> None:
        scenarios = job_steps(workflow_jobs(Path(".github/workflows/check.yaml").read_text())["scenarios-build"])
        script = next(step for step in scenarios if step.name == "Publish kache cache").block("run")
        self.assertEqual(script, PUBLISH_SCRIPT)
        with tempfile.TemporaryDirectory() as directory:
            runner_temp = Path(directory) / "runner-temp"
            runner_temp.mkdir()
            empty_workspace = Path(directory) / "workspace"
            empty_workspace.mkdir()

            # A job that failed before its checkout has no script, so the step only adds a notice.
            failed = run_step(script, empty_workspace, {"JOB_STATUS": "failure", "RUNNER_TEMP": str(runner_temp)})
            self.assertEqual(failed.returncode, 0, failed.stderr)
            self.assertEqual(
                failed.stdout,
                "::notice::Kache publication did not run because checkout did not provide its script\n",
            )
            self.assertFalse((runner_temp / "kache-report").exists())

            # A checked-out job whose work was cancelled records that publication did not run.
            cancelled = run_step(script, Path.cwd(), {"JOB_STATUS": "cancelled", "RUNNER_TEMP": str(runner_temp)})
            self.assertEqual(cancelled.returncode, 0, cancelled.stderr)
            recorded = json.loads((runner_temp / "kache-report" / "publication.json").read_text())
            self.assertEqual(recorded["outcome"], "not-run")
            self.assertEqual(recorded["job_status"], "cancelled")

    def test_the_report_records_health_and_the_costliest_misses(self) -> None:
        report_script = Path("scripts/publish_kache_report.py").read_text()
        self.assertIn('run("kache", "doctor")', report_script)
        self.assertIn('run("kache", "explain", crate)', report_script)
        self.assertIn('"kache", "report", "--format", "json", "--since", f"{REPORT_WINDOW_HOURS}h"', report_script)
        # Kache leaves the remote transfers out of a report filtered by a build tree.
        self.assertNotIn('"--root"', report_script)

    def test_the_report_splits_compile_time_into_compilation_hits_and_kache_work(self) -> None:
        report = {
            "timeline": {
                "event_count": 10,
                "duration_ms": 120_000,
                # 2026-10-29T21:43:35.902Z and 2026-10-29T21:45:35.100Z.
                "start_unix_ms": 1_793_310_215_902,
                "end_unix_ms": 1_793_310_335_100,
            },
            "summary": {
                "total_crates": 9,
                "misses": 2,
                "dups": 1,
                "local_hits": 3,
                "prefetch_hits": 1,
                "remote_hits": 2,
                "time_saved_ms": 90_000,
            },
            # Kache's phases split its tracked time, hits plus misses, without overlap.
            "timing": {
                "hit_time_ms": 60_000,
                "avg_hit_ms": 10_000,
                "miss_time_ms": 60_000,
                "avg_miss_ms": 20_000,
                "miss_compile_time_ms": 42_000,
                "total_key_ms": 48_000,
                "total_dep_info_ms": 6_000,
                "dep_info_runs": 4,
                "total_wait_ms": 18_000,
                "total_flight_wait_ms": 6_000,
                "total_permit_wait_ms": 12_000,
                "total_restore_ms": 6_000,
                "total_store_ms": 3_000,
                "total_lookup_ms": 600,
                "total_startup_ms": 1_200,
                "total_unattributed_ms": 1_200,
            },
            "suggestions": ["Average cache hit overhead is 1000ms — check disk I/O or consider faster storage"],
        }
        lines = publish_kache_report.report_lines(report)
        self.assertIn("Hit rate: 0%; compile time the hits saved, by kache's estimate: 1.5 min.", lines)
        self.assertIn(
            "Kache saw 9 cacheable crates in a 2.0 min window from 21:43 to 21:45 UTC. It tracked "
            "2.0 min on them, adding up every parallel rustc, so it can exceed the window.",
            lines,
        )
        self.assertIn("| Served from the cache: 3 local, 1 prefetched, 2 remote | 6 | 1.0 min | 10.0 s |", lines)
        self.assertIn("| Compiled after a cache miss | 3 | 1.0 min | 20.0 s |", lines)
        self.assertIn("| Compiling the misses | 0.7 min | 35.0% |", lines)
        self.assertIn("| Computing cache keys | 0.8 min | 40.0% |", lines)
        self.assertIn("| of which the dep-info pre-pass, 4 runs | 0.1 min | 5.0% |", lines)
        self.assertIn(
            "| Waiting: 0.2 min for a cache permit, 0.1 min for the same crate compiling in another "
            "process | 0.3 min | 15.0% |",
            lines,
        )
        self.assertIn("| Restoring cached outputs | 0.1 min | 5.0% |", lines)
        self.assertIn("| Storing compiled outputs | 0.1 min | 2.5% |", lines)
        self.assertIn("| Looking up the cache | 0.0 min | 0.5% |", lines)
        self.assertIn("| Starting the wrapper | 0.0 min | 1.0% |", lines)
        self.assertIn("| Attributed to no phase | 0.0 min | 1.0% |", lines)
        self.assertIn("- Average cache hit overhead is 1000ms — check disk I/O or consider faster storage", lines)

    def test_the_report_names_what_moved_to_and_from_the_remote(self) -> None:
        network = {
            "downloads_ok": 1537,
            "downloads_failed": 2,
            "bytes_down": 3 * 1024**3,
            "avg_download_ms": 2_950,
            "p95_download_ms": 7_100,
            "max_download_ms": 20_300,
            "total_semaphore_wait_ms": 90_000,
            "max_concurrent_downloads": 16,
            "dominant_download_phase": "body",
            "dominant_download_phase_pct": 71.25,
            "uploads_ok": 911,
            "uploads_failed": 0,
            "bytes_up": 2 * 1024**3,
        }
        lines = publish_kache_report.report_lines({"network": network})
        self.assertIn("### Remote transfer", lines)
        self.assertIn(
            "| Downloaded | 1537 | 2 | 3.0 GiB | 3.0 s on average, 7.1 s at the 95th percentile, 20.3 s at most |",
            lines,
        )
        self.assertIn("| Uploaded | 911 | 0 | 2.0 GiB | |", lines)
        self.assertIn(
            "Downloads waited 1.5 min in all for a transfer slot, with at most 16 at once. Most of "
            "their time went to body: 71.2%.",
            lines,
        )
        self.assertNotIn("### Remote transfer", publish_kache_report.report_lines({"network": None}))

    def test_a_report_without_compilation_says_so_instead_of_a_breakdown(self) -> None:
        lines = publish_kache_report.report_lines({"summary": {}, "timeline": {"event_count": 0}})
        self.assertIn("Kache recorded no compilation in this report's window.", lines)
        self.assertNotIn("### Where the compile time went", lines)
        self.assertIn("Kache recorded no compilation in this report's window.", publish_kache_report.report_lines({}))

    def test_the_report_redacts_the_remote_the_workflow_keeps_secret(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            configuration = Path(directory) / "kache-remote.toml"
            remote = kache_remote.Remote.from_environment(REMOTE_ENVIRONMENT)
            kache_remote.Configuration(store=LocalStore(max_size_gib=70), remote=remote).write(configuration)
            redaction = publish_kache_report.Redaction.from_environment({"KACHE_CONFIG": str(configuration)})
        doctor = (
            "Remote           ✓   s3://kache-ci/artifacts\n"
            "Remote access    ✓   https://account-id.r2.cloudflarestorage.com reachable\n"
            "Host             account-id.r2.cloudflarestorage.com\n"
        )
        self.assertEqual(
            redaction.apply(doctor),
            "Remote           ✓   s3://***/artifacts\nRemote access    ✓   *** reachable\nHost             ***\n",
        )
        # Without a configuration kache reads none either, so no remote is named.
        self.assertEqual(publish_kache_report.Redaction.from_environment({}).apply(doctor), doctor)

    def test_the_report_says_where_the_index_lives(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory) / "cache"
            cache.mkdir()
            (cache / "index.db").write_text("")
            on_disk = publish_kache_report.index_location({"KACHE_CACHE_DIR": str(cache)})
            (cache / "index.db").unlink()
            (cache / "index.db").symlink_to("/dev/shm/kache-index/index.db")
            in_memory = publish_kache_report.index_location({"KACHE_CACHE_DIR": str(cache)})
        self.assertEqual(on_disk, "Index: on disk, in the cache directory.")
        self.assertEqual(in_memory, "Index: in memory, at /dev/shm/kache-index/index.db.")
        self.assertIsNone(publish_kache_report.index_location({}))

    def test_a_build_report_names_its_phase(self) -> None:
        self.assertEqual(publish_kache_report.heading("tests", "build"), "## Kache: tests, after the build")
        self.assertEqual(publish_kache_report.heading("tests", "job"), "## Kache: tests")

    def test_image_builds_configure_and_publish_through_the_same_contract(self) -> None:
        workflow = Path(".github/workflows/docker-build.yaml").read_text()
        dockerfile = Path("Dockerfile.debian").read_text()
        justfile = Path("justfile").read_text()

        self.assertIn("uses: docker/setup-buildx-action@v3", workflow)
        jobs = workflow_jobs(workflow)
        for build in ("build-arch", "build-diagnostic"):
            for name, value in regional_remote(jobs[build]).items():
                with self.subTest(job=build, setting=name):
                    self.assertIn(f"{name}: {value}", jobs[build])
        for name in EU_REMOTE:
            self.assertIn(f'--build-arg "{name}=${{{name}}}"', justfile)
            self.assertIn(f'"${{{name}:?{name} is required}}"', justfile)
            self.assertIn(f"ARG {name}\n", dockerfile)
        self.assertIn('--build-arg "KACHE_VERSION=${KACHE_VERSION:-1.2.2}"', justfile)

        self.assertIn("ARG KACHE_VERSION=1.2.2", dockerfile)
        self.assertIn("COPY scripts/kache_remote.py ./scripts/kache_remote.py", dockerfile)
        self.assertIn("COPY scripts/publish_kache_report.py ./scripts/publish_kache_report.py", dockerfile)
        self.assertIn("ENV RUSTC_WRAPPER=kache", dockerfile)
        self.assertIn("ENV KACHE_CACHE_EXECUTABLES=true", dockerfile)
        self.assertIn(f"ENV KACHE_AUTO_RECOVER_MIN_FREE_BYTES={CACHE_RECOVERY_BYTES}", dockerfile)
        self.assertIn("ENV KACHE_REMOTE_KEY_LISTING=false", dockerfile)
        configure = dockerfile.index('python3 scripts/kache_remote.py configure "${KACHE_CONFIG}"')
        daemon = dockerfile.index("kache daemon start")
        self.assertLess(configure, daemon)
        # The store's index lives in the build step's tmpfs while the daemon runs, and returns to the
        # cache mount, which outlives the step, once the daemon stops.
        self.assertIn("--mount=type=tmpfs,target=/kache-index", dockerfile)
        prepare = dockerfile.index("python3 scripts/kache_remote.py prepare-index --memory /kache-index")
        self.assertLess(configure, prepare)
        self.assertLess(prepare, daemon)
        trap = re.search(r"trap '([^']*)' EXIT", dockerfile)
        assert trap is not None
        self.assertLess(
            trap.group(1).index("kache daemon stop"),
            trap.group(1).index("python3 scripts/kache_remote.py restore-index"),
        )
        jobs = workflow_jobs(workflow)
        for build in ("build-arch", "build-diagnostic"):
            with self.subTest(job=build):
                names = [step.name for step in job_steps(jobs[build])]
                lighten = names.index("Lighten the build volume")
                self.assertLess(names.index("Wait for Docker"), lighten)
                self.assertLess(lighten, next(index for index, name in enumerate(names) if name.startswith("Build and push")))
        # The build stage ends with the publication, after every artifact it built.
        build_stage = dockerfile.split("\nFROM ", maxsplit=2)[1]
        self.assertTrue(
            build_stage.rstrip().endswith(
                "&& python3 scripts/kache_remote.py publish --job-status success /tmp/kache-report"
            ),
            build_stage[-300:],
        )
        self.assertNotIn("sync --push", dockerfile)
        self.assertNotIn("kache save-manifest", dockerfile)
        self.assertNotIn("configure_kache_remote", dockerfile)

    def test_the_image_builder_takes_kache_and_trunk_from_their_pinned_release_archives(self) -> None:
        image_build = ImageBuild.read()
        for tool, argument, repository, libc in (
            ("kache", "KACHE", "kunobi-ninja/kache", "musl"),
            ("trunk", "TRUNK", "trunk-rs/trunk", "gnu"),
        ):
            image_build.assert_pinned(self, f"{argument}_VERSION", r"\d+\.\d+\.\d+")
            for build_architecture in ("AMD64", "ARM64"):
                checksum = f"{argument}_SHA256_{build_architecture}"
                image_build.assert_pinned(self, checksum, "[0-9a-f]{64}")
                self.assertIn(f'{tool}_sha256="${{{checksum}}}"', image_build.builder_stage)
            self.assertIn(
                f'"https://github.com/{repository}/releases/download/v${{{argument}_VERSION}}/'
                f'{tool}-${{tool_arch}}-unknown-linux-{libc}.tar.gz"',
                image_build.builder_stage,
            )
            # The archive is checked before anything is taken out of it.
            check = image_build.builder_stage.index(f'echo "${{{tool}_sha256}}  /tmp/{tool}.tar.gz" | sha256sum --check -')
            extraction = image_build.builder_stage.index(f"tar -xzf /tmp/{tool}.tar.gz")
            self.assertLess(check, extraction)
            self.assertIn(f'test "$({tool} --version)" = "{tool} ${{{argument}_VERSION}}"', image_build.builder_stage)
        self.assertIn("amd64) tool_arch=x86_64;", image_build.builder_stage)
        self.assertIn("arm64) tool_arch=aarch64;", image_build.builder_stage)

    def test_the_image_builder_compiles_its_other_tools_from_their_published_lockfiles(self) -> None:
        # Unlocked, a tool resolves the newest compatible release of each dependency, which can
        # stop it compiling: kache 1.2.1 did once kache-shims 1.2.2 was published.
        image_build = ImageBuild.read()
        instructions = image_build.builder_stage.replace("\\\n", " ").splitlines()
        installs = [instruction for instruction in instructions if instruction.startswith("RUN cargo install")]
        self.assertEqual(len(installs), 1, installs)
        install_command = installs[0].removeprefix("RUN ").split("&&")[0]
        arguments = shlex.split(install_command)
        self.assertEqual(arguments[:3], ["cargo", "install", "--locked"])

        pins = {}
        for package in arguments[3:]:
            name, _, version = package.partition("@")
            pin = re.fullmatch(r"\$\{(?P<argument>[A-Z_]+_VERSION)\}", version)
            self.assertIsNotNone(pin, package)
            pins[name] = pin["argument"]
        self.assertEqual(pins, {"cargo-auditable": "CARGO_AUDITABLE_VERSION", "cargo-sonic": "CARGO_SONIC_VERSION"})
        for argument in pins.values():
            image_build.assert_pinned(self, argument, r"\d+\.\d+\.\d+")


@dataclass(frozen=True)
class ImageBuild:
    """`Dockerfile.debian`'s global build arguments and its builder stage."""

    global_arguments: str
    builder_stage: str

    @classmethod
    def read(cls) -> ImageBuild:
        dockerfile = Path("Dockerfile.debian").read_text()
        global_arguments, stages = dockerfile.split("\nFROM ", maxsplit=1)
        builder_stage = stages.split("\nFROM ", maxsplit=1)[0]
        return cls(global_arguments=global_arguments, builder_stage=builder_stage)

    def assert_pinned(self, test: unittest.TestCase, argument: str, value: str) -> None:
        """`argument` defaults to a value matching `value` and reaches the builder stage."""
        test.assertRegex(self.global_arguments, rf"(?m)^ARG {argument}={value}$")
        test.assertIn(f"\nARG {argument}\n", self.builder_stage)


def isolated(environment: dict[str, str]) -> dict[str, str]:
    """This process's environment without a CI job's GitHub and kache settings, then
    `environment`, so a command under test never writes to the job running the tests."""
    inherited = {}
    for name, value in os.environ.items():
        if not name.startswith(("GITHUB_", "KACHE_", "RUNNER_")):
            inherited[name] = value
    return inherited | environment


def run_step(script: str, working_directory: Path, environment: dict[str, str]) -> subprocess.CompletedProcess[str]:
    """Run a workflow step's script as GitHub runs a `run` step without a `shell`: `bash -e`."""
    return subprocess.run(
        ["bash", "-e", "-c", script],
        capture_output=True,
        cwd=working_directory,
        env=isolated(environment),
        text=True,
        check=False,
    )


def run_cli(arguments: list[str], environment: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "scripts/kache_remote.py", *arguments],
        capture_output=True,
        env=isolated(environment),
        text=True,
        check=False,
    )


def quarter_of_volume_gib(path: Path) -> int:
    """A quarter of the volume holding `path`, in whole GiB rounded half up, and at least 5."""
    volume = shutil.disk_usage(path).total
    quarter_mib = volume // 4 // 1024**2
    return max((quarter_mib + 512) // 1024, 5)


class RemoteConfigurationTests(unittest.TestCase):
    def test_configure_writes_the_remote_and_the_store_budget_without_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "nested" / "kache.toml"
            # The cache directory does not exist yet: its budget comes from the volume it will be on.
            cache = Path(directory) / "home" / ".cache" / "kache"
            result = run_cli(["configure", str(config)], REMOTE_ENVIRONMENT | {"KACHE_CACHE_DIR": str(cache)})
            self.assertEqual(result.returncode, 0, result.stderr)
            parsed = tomllib.loads(config.read_text())
            mode = stat.S_IMODE(config.stat().st_mode)
            budget = quarter_of_volume_gib(Path(directory))

        self.assertEqual(
            parsed,
            {
                "cache": {
                    "local_max_size": f"{budget}GiB",
                    "remote": {
                        "type": "s3",
                        "bucket": "kache-ci",
                        "region": "auto",
                        "endpoint": "https://account-id.r2.cloudflarestorage.com",
                        "prefix": "artifacts",
                        "pull_request_prefix": "artifacts-pr",
                    }
                }
            },
        )
        self.assertEqual(mode, 0o600)

    def test_the_store_may_hold_a_quarter_of_its_volume_and_at_least_kaches_floor(self) -> None:
        gib = 1024**3
        self.assertEqual(LocalStore.for_volume(300 * 10**9), LocalStore(max_size_gib=70))
        self.assertEqual(LocalStore.for_volume(4 * 42 * gib + 4 * gib // 2), LocalStore(max_size_gib=43))
        self.assertEqual(LocalStore.for_volume(4 * 42 * gib + 4 * gib // 2 - 4), LocalStore(max_size_gib=42))
        self.assertEqual(LocalStore.for_volume(16 * gib), LocalStore(max_size_gib=5))
        self.assertEqual(LocalStore(max_size_gib=70).configuration(), '[cache]\nlocal_max_size = "70GiB"\n')

    def test_the_store_is_measured_where_kache_keeps_it(self) -> None:
        self.assertEqual(
            kache_remote.cache_directory({"KACHE_CACHE_DIR": "/cache/kache", "XDG_CACHE_HOME": "/xdg", "HOME": "/home/user"}),
            Path("/cache/kache"),
        )
        self.assertEqual(
            kache_remote.cache_directory({"XDG_CACHE_HOME": "/xdg", "HOME": "/home/user"}), Path("/xdg/kache")
        )
        # As in kache, a relative XDG_CACHE_HOME names no cache directory.
        self.assertEqual(
            kache_remote.cache_directory({"XDG_CACHE_HOME": "xdg", "HOME": "/home/user"}),
            Path("/home/user/.cache/kache"),
        )
        # Without HOME, kache takes the home directory from the user's passwd entry.
        self.assertEqual(
            kache_remote.cache_directory({}), Path(pwd.getpwuid(os.getuid()).pw_dir) / ".cache" / "kache"
        )

    def test_every_remote_setting_is_required(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "kache.toml"
            for missing in REMOTE_ENVIRONMENT:
                with self.subTest(missing=missing):
                    environment = dict(REMOTE_ENVIRONMENT) | {"KACHE_CACHE_DIR": directory}
                    environment.pop(missing)
                    result = run_cli(["configure", str(config)], environment)
                    self.assertEqual(result.returncode, 2)
                    self.assertIn(f"{missing} is required", result.stderr)
            self.assertFalse(config.exists())

    def test_endpoint_is_a_service_url(self) -> None:
        for endpoint in (
            "https://account-id.r2.cloudflarestorage.com/kache-ci",
            "account-id.r2.cloudflarestorage.com",
            "ftp://account-id.r2.cloudflarestorage.com",
            "https://user:password@example.com",
            "https://example.com/?region=auto",
            "https://example.com/#fragment",
            "https://example.com:not-a-port",
            'https://example.com/\n[other]\nvalue = "injected"',
            "https://example.com /",
        ):
            with self.subTest(endpoint=endpoint), self.assertRaises(ConfigurationError) as raised:
                kache_remote.Remote.from_environment(REMOTE_ENVIRONMENT | {"KACHE_S3_ENDPOINT": endpoint})
            self.assertIn("KACHE_S3_ENDPOINT", str(raised.exception))

    def test_a_valid_endpoint_keeps_its_port_without_a_trailing_slash(self) -> None:
        remote = kache_remote.Remote.from_environment(REMOTE_ENVIRONMENT | {"KACHE_S3_ENDPOINT": "http://127.0.0.1:9000/"})
        self.assertEqual(remote.endpoint, "http://127.0.0.1:9000")

    def test_a_quote_in_a_value_cannot_leave_its_string(self) -> None:
        remote = kache_remote.Remote.from_environment(REMOTE_ENVIRONMENT | {"KACHE_S3_ENDPOINT": 'https://example.com"'})
        parsed = tomllib.loads(remote.configuration())
        self.assertEqual(parsed["cache"]["remote"]["endpoint"], 'https://example.com"')
        self.assertEqual(set(parsed["cache"]["remote"]), {"type", "bucket", "region", "endpoint", "prefix", "pull_request_prefix"})

    def test_bucket_and_region_are_validated(self) -> None:
        for name, value, message in (
            ("KACHE_S3_BUCKET", "Kache_CI", "not a valid S3 bucket name"),
            ("KACHE_S3_BUCKET", "k", "not a valid S3 bucket name"),
            ("KACHE_S3_REGION", "us east", "not a valid S3 region"),
        ):
            with self.subTest(name=name, value=value), self.assertRaises(ConfigurationError) as raised:
                kache_remote.Remote.from_environment(REMOTE_ENVIRONMENT | {name: value})
            self.assertIn(message, str(raised.exception))


FAKE_KACHE = r'''
import json
import os
from pathlib import Path
import sys
import time

behavior_path = Path(os.environ["FAKE_KACHE_BEHAVIOR"])
behavior = json.loads(behavior_path.read_text())
arguments = sys.argv[1:]
observed = ("KACHE_CONFIG", "KACHE_S3_BUCKET", "KACHE_S3_PREFIX", "KACHE_S3_ACCESS_KEY", "KACHE_MANIFEST_KEY", "KACHE_NAMESPACE")
with open(os.environ["FAKE_KACHE_LOG"], "a") as log:
    log.write(json.dumps({"argv": arguments, "env": {name: os.environ.get(name) for name in observed}}) + "\n")


def take(name, default):
    """The next value of a sequence the behavior gives; its last value repeats."""
    values = behavior.get(name)
    if values is None:
        return default
    value = values[0]
    if len(values) > 1:
        behavior[name] = values[1:]
        behavior_path.write_text(json.dumps(behavior))
    return value


def document(command, body):
    print(json.dumps({"schema_version": behavior.get("schema_version", 1), "command": command, "success": True, **body}))


if arguments == ["daemon", "start"]:
    print("daemon started")
    sys.exit(behavior.get("daemon_start_status", 0))
if arguments == ["--json", "daemon", "status"]:
    time.sleep(behavior.get("status_seconds", 0))
    text = take("status_text", None)
    if text is not None:
        print(text)
        sys.exit(3)
    failure = behavior.get("status_failure")
    if failure is not None:
        print(json.dumps({"schema_version": 1, "command": "daemon-status", "success": False,
                          "error": {"code": "command_failed", "message": failure, "causes": []}}))
        sys.exit(1)
    config_path = behavior.get("config_path", os.environ.get("KACHE_CONFIG"))
    document("daemon-status", {"daemon_running": take("running", True), "daemon_config_path": config_path})
    sys.exit(0)
if arguments == ["--json", "stats"]:
    document("stats", {
        "daemon_connected": True,
        "remote": behavior.get("remote", "s3://kache-ci/artifacts"),
        "daemon_remote_readonly": behavior.get("readonly", False),
    })
    sys.exit(0)
if arguments[:2] == ["telemetry", "write"]:
    time.sleep(behavior.get("telemetry_seconds", 0))
    status = take("telemetry_status", 0)
    if status:
        print("telemetry failed", file=sys.stderr)
        sys.exit(status)
    directory = Path(arguments[2])
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "schema_version").write_text(behavior.get("telemetry_schema", "1") + "\n")

    def point(value, key=None, attribute=None):
        attributes = [] if key is None else [{"key": key, "value": {"stringValue": attribute}}]
        return {"asInt": str(value), "timeUnixNano": "1", "attributes": attributes}

    results = [point(behavior.get(result, 0), "kache.cache.result", result) for result in ("completed", "failed", "skipped", "suppressed")]
    metrics = [
        {"name": "kache.cache.uploads.pending", "unit": "{upload}", "gauge": {"dataPoints": [point(take("pending", 0))]}},
        {"name": "kache.cache.remote.degraded", "unit": "1", "gauge": {"dataPoints": [point(behavior.get("degraded", 0))]}},
        {"name": "kache.cache.uploads", "unit": "{upload}", "sum": {"dataPoints": results}},
        {"name": "kache.cache.bytes", "unit": "By", "sum": {"dataPoints": [
            point(behavior.get("bytes", 0), "kache.cache.direction", "upload"),
            point(0, "kache.cache.direction", "download"),
        ]}},
    ]
    metrics = [metric for metric in metrics if metric["name"] != behavior.get("telemetry_without")]
    document_body = {"resourceMetrics": [{"resource": {"attributes": []}, "scopeMetrics": [{"scope": {"name": "kache.cache"}, "metrics": metrics}]}]}
    rendered = "{not json" if behavior.get("telemetry_corrupt") else json.dumps(document_body)
    (directory / "metrics.otlp.json").write_text(rendered)
    sys.exit(0)
if arguments[0] == "save-manifest":
    time.sleep(behavior.get("manifest_seconds", 0))
    print(behavior.get("manifest_output", "Saved manifest: 12 entries for 'job-Linux-X64'"))
    sys.exit(behavior.get("manifest_status", 0))
sys.exit(f"unexpected kache arguments: {arguments}")
'''


# The system commands the store preparation runs, faked so that no test reads the host's mounts or
# remounts its volume. `findmnt` answers from FAKE_MOUNTS, a list of [path prefix, mount] pairs whose
# mount may give the options it shows once `sudo` has remounted it; without FAKE_MOUNTS every path
# is on an overlay filesystem, which the preparation leaves alone. `sudo` records its arguments in
# FAKE_SUDO_LOG and exits with FAKE_SUDO_STATUS, refusing by default.
FAKE_FINDMNT = r'''
import json, os, sys
target = sys.argv[sys.argv.index("--target") + 1]
mounts = json.loads(os.environ.get("FAKE_MOUNTS", '[["/", {"target": "/", "fstype": "overlay", "options": "rw"}]]'))
remounted = os.path.exists(os.environ.get("FAKE_REMOUNTED", "/nonexistent"))
for prefix, mount in mounts:
    if target.startswith(prefix):
        options = mount.get("remounted", mount["options"]) if remounted else mount["options"]
        print(mount["target"], mount["fstype"], options)
        sys.exit(0)
sys.exit(1)
'''
FAKE_SUDO = r'''
import json, os, sys
log = os.environ.get("FAKE_SUDO_LOG")
if log:
    with open(log, "a") as output:
        output.write(json.dumps(sys.argv[1:]) + "\n")
status = int(os.environ.get("FAKE_SUDO_STATUS", "1"))
if status:
    print("mount: only root can do that", file=sys.stderr)
    sys.exit(status)
remounted = os.environ.get("FAKE_REMOUNTED")
if remounted:
    open(remounted, "w").close()
'''


def install_fake(directory: Path, name: str, body: str) -> None:
    executable = directory / name
    executable.write_text(f"#!{sys.executable}\n{body}")
    executable.chmod(0o755)


class FakeKache:
    """A `kache` executable whose answers a test sets, recording every invocation, beside harmless
    `findmnt` and `sudo` commands for the store preparation."""

    def __init__(self, directory: Path, behavior: dict) -> None:
        self.bin = directory / "bin"
        self.bin.mkdir()
        install_fake(self.bin, "kache", FAKE_KACHE)
        install_fake(self.bin, "findmnt", FAKE_FINDMNT)
        install_fake(self.bin, "sudo", FAKE_SUDO)
        self.behavior = directory / "behavior.json"
        self.behavior.write_text(json.dumps(behavior))
        self.log = directory / "kache.log"
        self.log.touch()

    def environment(self, **extra: str) -> dict[str, str]:
        return {
            "PATH": f"{self.bin}{os.pathsep}{os.environ['PATH']}",
            "FAKE_KACHE_BEHAVIOR": str(self.behavior),
            "FAKE_KACHE_LOG": str(self.log),
        } | extra

    def calls(self) -> list[dict]:
        return [json.loads(line) for line in self.log.read_text().splitlines()]

    def commands(self) -> list[list[str]]:
        return [call["argv"][:3] if call["argv"][:2] == ["--json", "daemon"] else call["argv"][:2] for call in self.calls()]


class SteppingClock(kache_remote.Clock):
    """A clock that moves only when publication sleeps."""

    def __init__(self) -> None:
        self.time = 1000.0
        self.sleeps: list[float] = []

    def now(self) -> float:
        return self.time

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.time += seconds


class PublicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: subprocess.run(["rm", "-rf", str(self.directory)], check=True))
        self.report = self.directory / "kache-report"
        self.summary = self.directory / "summary.md"

    def publish(self, behavior: dict, job_status: str = "success", budget: float = 10.0, **extra: str) -> tuple[Publication, FakeKache, str]:
        kache = FakeKache(self.directory, behavior)
        environment = kache.environment(
            GITHUB_JOB="scenarios",
            GITHUB_STEP_SUMMARY=str(self.summary),
            KACHE_MANIFEST_KEY="scenarios-Linux-X64",
            KACHE_NAMESPACE="scenarios-Linux-X64",
            # A second copy of the remote's description never reaches a kache command.
            KACHE_S3_BUCKET="ambient-bucket",
            KACHE_S3_PREFIX="ambient-prefix",
        ) | extra
        self.clock = SteppingClock()
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            publication = kache_remote.publish(
                self.report, job_status, environment, kache_remote.Termination(), self.clock, budget, 2.0
            )
        return publication, kache, output.getvalue()

    def recorded(self) -> dict:
        return json.loads((self.report / "publication.json").read_text())

    def test_a_successful_job_saves_its_manifest_and_waits_for_every_upload(self) -> None:
        publication, kache, output = self.publish({"pending": [5, 3, 0], "completed": 41, "skipped": 7, "bytes": 1024})

        self.assertEqual(publication.outcome, Outcome.PUBLISHED)
        self.assertEqual(publication.reasons, [])
        self.assertEqual(publication.manifest.status, 0)
        self.assertEqual(publication.uploads_at_start.pending, 5)
        self.assertEqual(publication.uploads_at_end.pending, 0)
        self.assertEqual(publication.uploads_at_end.completed, 41)
        self.assertEqual(self.clock.sleeps, [2.0])
        self.assertEqual(
            kache.commands(),
            [
                ["--json", "daemon", "status"],
                ["--json", "stats"],
                ["telemetry", "write"],
                ["save-manifest", "--manifest-key"],
                ["telemetry", "write"],
                ["telemetry", "write"],
                ["--json", "daemon", "status"],
                ["--json", "stats"],
            ],
        )
        manifest = kache.calls()[3]["argv"]
        self.assertEqual(manifest, ["save-manifest", "--manifest-key", "scenarios-Linux-X64", "--namespace", "scenarios-Linux-X64"])
        for call in kache.calls():
            self.assertIsNone(call["env"]["KACHE_S3_BUCKET"])
            self.assertIsNone(call["env"]["KACHE_S3_PREFIX"])
        self.assertEqual(self.recorded()["outcome"], "published")
        self.assertIn("### Kache publication: published", self.summary.read_text())
        self.assertIn("Saved manifest: 12 entries", (self.report / "publication.md").read_text())
        self.assertNotIn("::warning", output)

    def test_the_wait_ends_with_the_budget_and_names_the_uploads_left_to_the_drain(self) -> None:
        publication, kache, output = self.publish({"pending": [3]}, budget=10.0)

        self.assertEqual(publication.outcome, Outcome.INCOMPLETE)
        self.assertEqual(self.clock.sleeps, [2.0, 2.0, 2.0, 2.0, 2.0])
        self.assertEqual(publication.upload_wait_seconds, 10.0)
        self.assertEqual(publication.uploads_at_end.pending, 3)
        self.assertEqual(
            publication.reasons,
            ["3 uploads were still queued or in flight when the budget ended; the daemon's shutdown drain takes them over."],
        )
        self.assertEqual(kache.commands().count(["telemetry", "write"]), 6)
        self.assertIn("::warning title=kache publication incomplete::3 uploads", output)
        self.assertEqual(self.recorded()["outcome"], "incomplete")

    def test_a_manifest_that_outlasts_the_budget_is_reported_unsaved(self) -> None:
        publication, _kache, _output = self.publish({"manifest_seconds": 5}, budget=1.0)

        self.assertEqual(publication.outcome, Outcome.INCOMPLETE)
        self.assertIsNone(publication.manifest.status)
        self.assertIn("The manifest was not saved within the budget.", publication.reasons)

    def test_a_failed_manifest_and_failed_uploads_leave_publication_incomplete(self) -> None:
        publication, _kache, _output = self.publish({"manifest_status": 1, "manifest_output": "remote refused", "failed": 2})

        self.assertEqual(publication.outcome, Outcome.INCOMPLETE)
        self.assertEqual(
            publication.reasons,
            ["Saving the manifest exited with status 1.", "2 uploads failed for good during the job."],
        )
        self.assertEqual(publication.manifest.output_tail, ["remote refused"])

    def test_a_job_whose_work_failed_or_was_cancelled_waits_for_nothing(self) -> None:
        reasons = {
            "failure": "The job's work ended with status `failure`; uploads still queued are left to the daemon's shutdown drain.",
            "cancelled": "The job's work ended with status `cancelled`. The kache action's post step does not run for a cancelled job, so uploads still queued end with the machine.",
        }
        for status, reason in reasons.items():
            with self.subTest(status=status):
                publication, kache, output = self.publish({}, job_status=status)
                self.assertEqual(publication.outcome, Outcome.NOT_RUN)
                self.assertEqual(publication.reasons, [reason])
                self.assertEqual(kache.calls(), [])
                self.assertEqual(self.clock.sleeps, [])
                self.assertNotIn("::warning", output)
                self.assertEqual(self.recorded()["job_status"], status)
                kache.log.unlink()
                subprocess.run(["rm", "-rf", str(kache.bin)], check=True)

    def test_a_read_only_remote_has_nothing_to_publish(self) -> None:
        publication, kache, output = self.publish({"readonly": True})

        self.assertEqual(publication.outcome, Outcome.READ_ONLY)
        self.assertEqual(kache.commands(), [["--json", "daemon", "status"], ["--json", "stats"]])
        self.assertNotIn("::warning", output)

    def test_a_stopped_daemon_is_not_started_again_to_answer(self) -> None:
        publication, kache, output = self.publish({"running": [False]})

        self.assertEqual(publication.outcome, Outcome.UNAVAILABLE)
        self.assertEqual(publication.reasons, ["The job's kache daemon is not running."])
        # `kache stats` would start a daemon, so a stopped one is never asked through it.
        self.assertEqual(kache.commands(), [["--json", "daemon", "status"]])
        self.assertIn("::warning title=kache publication unavailable::", output)

    def test_a_daemon_that_stops_during_the_wait_leaves_its_counters_unconfirmed(self) -> None:
        publication, _kache, _output = self.publish({"running": [True, False], "pending": [1, 0]})

        self.assertEqual(publication.outcome, Outcome.INCOMPLETE)
        self.assertEqual(
            publication.reasons,
            ["The daemon stopped answering during the wait, so its upload counters are unconfirmed."],
        )

    def test_unreadable_uploads_are_unavailable_before_and_incomplete_during_the_wait(self) -> None:
        publication, _kache, _output = self.publish({"telemetry_status": [1]})
        self.assertEqual(publication.outcome, Outcome.UNAVAILABLE)
        self.assertIn("`kache telemetry write` exited with status 1", publication.reasons[0])

        subprocess.run(["rm", "-rf", str(self.directory / "bin")], check=True)
        publication, _kache, _output = self.publish({"telemetry_status": [0, 1], "pending": [2]})
        self.assertEqual(publication.outcome, Outcome.INCOMPLETE)
        self.assertIn("The daemon's uploads could not be read", publication.reasons[0])

    def test_telemetry_of_another_schema_is_refused(self) -> None:
        publication, _kache, _output = self.publish({"telemetry_schema": "2"})

        self.assertEqual(publication.outcome, Outcome.UNAVAILABLE)
        self.assertIn("kache telemetry schema '2', expected '1'", publication.reasons[0])

    def test_a_document_of_another_schema_is_refused(self) -> None:
        publication, _kache, _output = self.publish({"schema_version": 2})

        self.assertEqual(publication.outcome, Outcome.UNAVAILABLE)
        self.assertIn("answered outside JSON schema 1", publication.reasons[0])

    def test_a_job_without_kache_or_its_remote_is_unavailable(self) -> None:
        publication, _kache, _output = self.publish({}, PATH="/nonexistent")
        self.assertEqual(publication.outcome, Outcome.UNAVAILABLE)
        self.assertEqual(publication.reasons, ["This job has no kache command."])

        subprocess.run(["rm", "-rf", str(self.directory / "bin")], check=True)
        publication, kache, _output = self.publish({}, KACHE_MANIFEST_KEY="")
        self.assertEqual(publication.outcome, Outcome.UNAVAILABLE)
        self.assertIn("KACHE_MANIFEST_KEY is not set", publication.reasons[0])
        self.assertEqual(kache.calls(), [])

    def test_kache_answers_outside_their_contract_are_refusals(self) -> None:
        for behavior, budget, outcome, reason in (
            ({"status_text": ["Daemon: running"]}, 10.0, Outcome.UNAVAILABLE, "exited with status 3 without a JSON document"),
            ({"status_failure": "socket refused"}, 10.0, Outcome.UNAVAILABLE, "`kache --json daemon status` failed: socket refused"),
            ({"status_seconds": 3}, 0.5, Outcome.UNAVAILABLE, "`kache --json daemon status` did not answer within 1 s"),
            ({"telemetry_seconds": 3}, 0.5, Outcome.UNAVAILABLE, "`kache telemetry write` did not answer within 1 s"),
            ({"telemetry_without": "kache.cache.uploads.pending"}, 10.0, Outcome.UNAVAILABLE, "kache telemetry has no kache.cache.uploads.pending metric"),
            ({"telemetry_corrupt": True}, 10.0, Outcome.UNAVAILABLE, "unreadable kache telemetry"),
            ({"status_text": [None, "garbage"]}, 10.0, Outcome.INCOMPLETE, "The daemon stopped answering during the wait"),
            ({}, 0.0, Outcome.INCOMPLETE, "The manifest was not saved within the budget."),
        ):
            with self.subTest(behavior=behavior, budget=budget):
                subprocess.run(["rm", "-rf", str(self.directory / "bin")], check=True)
                publication, _kache, _output = self.publish(behavior, budget=budget)
                self.assertEqual(publication.outcome, outcome)
                self.assertTrue(any(reason in recorded for recorded in publication.reasons), publication.reasons)

    def test_output_a_timed_out_command_left_is_text(self) -> None:
        self.assertEqual(kache_remote.captured_text(None), "")
        self.assertEqual(kache_remote.captured_text("partial"), "partial")
        self.assertEqual(kache_remote.captured_text(b"bytes \xff"), "bytes \ufffd")

    def test_an_image_build_is_named_by_its_manifest_key(self) -> None:
        kache = FakeKache(self.directory, {})
        environment = kache.environment(KACHE_MANIFEST_KEY="docker-amd64", KACHE_NAMESPACE="docker-amd64")
        with contextlib.redirect_stdout(io.StringIO()):
            publication = kache_remote.publish(
                self.report, "success", environment, kache_remote.Termination(), SteppingClock(), 10.0, 2.0
            )
        self.assertEqual(publication.job, "docker-amd64")
        self.assertEqual(publication.outcome, Outcome.PUBLISHED)

    def test_a_termination_during_the_wait_is_recorded_before_the_step_ends(self) -> None:
        kache = FakeKache(self.directory, {"pending": [4]})
        environment = isolated(kache.environment(
            GITHUB_JOB="scenarios",
            KACHE_MANIFEST_KEY="scenarios-Linux-X64",
            KACHE_NAMESPACE="scenarios-Linux-X64",
        ))
        process = subprocess.Popen(
            [sys.executable, "scripts/kache_remote.py", "publish", "--job-status", "success", str(self.report)],
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        # The publication is waiting once it has asked for the uploads after saving the manifest.
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            commands = kache.commands()
            if ["save-manifest", "--manifest-key"] in commands and commands[-1] == ["telemetry", "write"]:
                break
            time.sleep(0.05)
        else:
            process.kill()
            self.fail(f"publication never started waiting: {kache.commands()}")
        process.send_signal(signal.SIGTERM)
        _stdout, stderr = process.communicate(timeout=60)

        self.assertEqual(process.returncode, 128 + signal.SIGTERM, stderr)
        recorded = self.recorded()
        self.assertEqual(recorded["outcome"], "cancelled")
        self.assertEqual(recorded["reasons"], ["The step received SIGTERM while waiting for uploads."])

    def test_a_termination_while_telemetry_is_removed_is_recorded_and_the_removal_completes(self) -> None:
        # Python 3.12.3's rmtree, interrupted just after it closes a directory's descriptor, closes
        # it again in a finally block, and that EBADF replaced the cancellation of Check run
        # 40423033091 before its record was written; later releases leave the directory behind
        # instead. The signal lands at that point, once while the daemon is asked and once while the
        # wait polls.
        handlers = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
        self.addCleanup(lambda: [signal.signal(number, handler) for number, handler in handlers.items()])
        real_close = os.close
        for removal, phase in ((1, "asking the daemon"), (2, "waiting for uploads")):
            with self.subTest(phase=phase):
                subprocess.run(["rm", "-rf", str(self.directory / "bin")], check=True)
                kache = FakeKache(self.directory, {"pending": [4]})
                temporary = self.directory / f"tmp-{removal}"
                temporary.mkdir()
                telemetry_closes = []

                def is_telemetry_directory(descriptor: int) -> bool:
                    opened = os.fstat(descriptor)
                    for created in temporary.glob("kache-telemetry-*"):
                        if os.path.samestat(opened, created.stat()):
                            return True
                    return False

                def close(descriptor: int) -> None:
                    # A close after the signal is not examined: Python 3.12.3 then closes the same
                    # descriptor again, and that close must fail as it did in CI.
                    closes_telemetry = len(telemetry_closes) < removal and is_telemetry_directory(descriptor)
                    real_close(descriptor)
                    if closes_telemetry:
                        telemetry_closes.append(descriptor)
                        if len(telemetry_closes) == removal:
                            signal.raise_signal(signal.SIGTERM)

                environment = kache.environment(
                    GITHUB_JOB="scenarios",
                    KACHE_MANIFEST_KEY="scenarios-Linux-X64",
                    KACHE_NAMESPACE="scenarios-Linux-X64",
                )
                with (
                    mock.patch.dict(os.environ, environment, clear=True),
                    mock.patch.object(tempfile, "tempdir", str(temporary)),
                    mock.patch.object(os, "close", close),
                    contextlib.redirect_stdout(io.StringIO()),
                ):
                    status = kache_remote.main(["publish", "--job-status", "success", str(self.report)])

                self.assertEqual(len(telemetry_closes), removal)
                self.assertEqual(status, 128 + signal.SIGTERM)
                recorded = self.recorded()
                self.assertEqual(recorded["outcome"], "cancelled")
                self.assertEqual(recorded["reasons"], [f"The step received SIGTERM while {phase}."])
                self.assertIn("### Kache publication: cancelled", (self.report / "publication.md").read_text())
                self.assertEqual(sorted(temporary.iterdir()), [])
                # No later signal can replace that status while the interpreter exits.
                self.assertEqual(signal.getsignal(signal.SIGINT), signal.SIG_IGN)
                self.assertEqual(signal.getsignal(signal.SIGTERM), signal.SIG_IGN)

    def test_a_termination_is_recorded_and_raised_again(self) -> None:
        termination = kache_remote.Termination()

        class TerminatedWhileWaiting(SteppingClock):
            def sleep(self, seconds: float) -> None:
                termination.handle(signal.SIGINT, None)

        kache = FakeKache(self.directory, {"pending": [2]})
        environment = kache.environment(KACHE_MANIFEST_KEY="tests-Linux-X64", GITHUB_JOB="tests")
        with contextlib.redirect_stdout(io.StringIO()) as output, self.assertRaises(kache_remote.Cancelled) as raised:
            kache_remote.publish(self.report, "success", environment, termination, TerminatedWhileWaiting(), 10.0, 2.0)

        self.assertEqual(raised.exception.signal_number, signal.SIGINT)
        self.assertEqual(self.recorded()["reasons"], ["The step received SIGINT while waiting for uploads."])
        self.assertIn("::warning title=kache publication cancelled::", output.getvalue())

    def test_a_termination_before_publication_begins_runs_no_kache_command(self) -> None:
        termination = kache_remote.Termination()
        termination.handle(signal.SIGTERM, None)
        kache = FakeKache(self.directory, {})
        environment = kache.environment(KACHE_MANIFEST_KEY="tests-Linux-X64", GITHUB_JOB="tests")
        with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(kache_remote.Cancelled) as raised:
            kache_remote.publish(self.report, "success", environment, termination, SteppingClock(), 10.0, 2.0)

        self.assertEqual(raised.exception.signal_number, signal.SIGTERM)
        self.assertEqual(self.recorded()["outcome"], "cancelled")
        self.assertEqual(self.recorded()["reasons"], ["The step received SIGTERM while starting."])
        self.assertEqual(kache.calls(), [])

    def test_a_termination_interrupts_only_a_wait_and_keeps_its_first_signal(self) -> None:
        # Outside a wait a signal is only recorded, and a later one does not replace it.
        termination = kache_remote.Termination()
        termination.handle(signal.SIGINT, None)
        termination.handle(signal.SIGTERM, None)
        waited = []
        with self.assertRaises(kache_remote.Cancelled) as raised, termination.interruptible():
            waited.append(True)
        self.assertEqual(raised.exception.signal_number, signal.SIGINT)
        self.assertEqual(waited, [])

        # Inside a wait the signal is raised at once and only once, so the wait's cleanup runs to its
        # end whatever signal follows.
        termination = kache_remote.Termination()
        cleaned = []
        with self.assertRaises(kache_remote.Cancelled) as raised, termination.interruptible():
            try:
                termination.handle(signal.SIGTERM, None)
            finally:
                termination.handle(signal.SIGINT, None)
                cleaned.append(True)
        self.assertEqual(raised.exception.signal_number, signal.SIGTERM)
        self.assertEqual(cleaned, [True])
        with self.assertRaises(kache_remote.Cancelled) as raised:
            termination.check()
        self.assertEqual(raised.exception.signal_number, signal.SIGTERM)

    def test_main_publishes_with_the_step_handlers_installed(self) -> None:
        handlers = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
        self.addCleanup(lambda: [signal.signal(number, handler) for number, handler in handlers.items()])
        kache = FakeKache(self.directory, {})
        environment = kache.environment(GITHUB_STEP_SUMMARY=str(self.summary))
        real_publish = kache_remote.publish
        published = []

        def publish(report: Path, job_status: str, environment: dict[str, str], termination: kache_remote.Termination) -> Publication:
            # Both signals reach the termination that publication raises.
            for number in (signal.SIGINT, signal.SIGTERM):
                handler = signal.getsignal(number)
                self.assertIs(handler.__self__, termination)
                self.assertIs(handler.__func__, kache_remote.Termination.handle)
            published.append(report)
            return real_publish(report, job_status, environment, termination)

        with (
            mock.patch.dict(os.environ, environment, clear=True),
            mock.patch.object(kache_remote, "publish", publish),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            status = kache_remote.main(["publish", "--job-status", "failure", str(self.report)])

        self.assertEqual(status, 0)
        self.assertEqual(published, [self.report])
        # Once the record is written, no signal can change the step's status.
        self.assertEqual(signal.getsignal(signal.SIGINT), signal.SIG_IGN)
        self.assertEqual(signal.getsignal(signal.SIGTERM), signal.SIG_IGN)
        self.assertEqual(self.recorded()["outcome"], "not-run")
        self.assertEqual(kache.calls(), [])

    def test_a_termination_after_publication_still_ends_the_step_with_its_status(self) -> None:
        handlers = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
        self.addCleanup(lambda: [signal.signal(number, handler) for number, handler in handlers.items()])
        kache = FakeKache(self.directory, {})
        real_publish = kache_remote.publish

        def publish(report: Path, job_status: str, environment: dict[str, str], termination: kache_remote.Termination) -> Publication:
            publication = real_publish(report, job_status, environment, termination)
            # The signal lands after publication's last check, before the step stops listening.
            signal.raise_signal(signal.SIGTERM)
            return publication

        with (
            mock.patch.dict(os.environ, kache.environment(), clear=True),
            mock.patch.object(kache_remote, "publish", publish),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            status = kache_remote.main(["publish", "--job-status", "failure", str(self.report)])

        self.assertEqual(status, 128 + signal.SIGTERM)
        self.assertEqual(self.recorded()["outcome"], "not-run")


class StorePreparationTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.cache = self.root / "home" / ".cache" / "kache"
        self.memory = self.root / "shm"
        self.memory.mkdir()
        self.bin = self.root / "bin"
        self.bin.mkdir()
        install_fake(self.bin, "findmnt", FAKE_FINDMNT)
        install_fake(self.bin, "sudo", FAKE_SUDO)
        self.sudo_log = self.root / "sudo.log"
        self.remounted = self.root / "remounted"

    def environment(self, mounts: list, sudo_status: int = 0) -> dict[str, str]:
        return {
            "PATH": f"{self.bin}{os.pathsep}{os.environ['PATH']}",
            "KACHE_CACHE_DIR": str(self.cache),
            "FAKE_MOUNTS": json.dumps(mounts),
            "FAKE_SUDO_LOG": str(self.sudo_log),
            "FAKE_SUDO_STATUS": str(sudo_status),
            "FAKE_REMOUNTED": str(self.remounted),
        }

    def sudo_calls(self) -> list[list[str]]:
        if not self.sudo_log.exists():
            return []
        return [json.loads(line) for line in self.sudo_log.read_text().splitlines()]

    def test_the_index_moves_into_memory_with_the_files_sqlite_keeps_beside_it(self) -> None:
        self.cache.mkdir(parents=True)
        for name, content in (("index.db", "pages"), ("index.db-wal", "log"), ("index.db-shm", "shared")):
            (self.cache / name).write_text(content)
        line = kache_remote.keep_index_in_memory(self.cache, self.memory)
        directory = self.memory / "kache-index"
        self.assertEqual(line, f"kache's index and its write-ahead log live in memory, in {directory}.")
        self.assertTrue((self.cache / "index.db").is_symlink())
        self.assertEqual(os.readlink(self.cache / "index.db"), str(directory / "index.db"))
        self.assertEqual(sorted(path.name for path in self.cache.iterdir()), ["index.db"])
        self.assertEqual((directory / "index.db-wal").read_text(), "log")
        self.assertEqual((directory / "index.db-shm").read_text(), "shared")
        self.assertEqual((self.cache / "index.db").read_text(), "pages")
        self.assertEqual(
            kache_remote.keep_index_in_memory(self.cache, self.memory),
            f"kache's index already lives at {directory / 'index.db'}.",
        )

    def test_sqlite_keeps_a_symlinked_index_and_its_log_in_memory(self) -> None:
        kache_remote.keep_index_in_memory(self.cache, self.memory)
        database = sqlite3.connect(self.cache / "index.db")
        self.addCleanup(database.close)
        database.execute("PRAGMA journal_mode=WAL")
        database.execute("CREATE TABLE entries (cache_key TEXT PRIMARY KEY)")
        database.execute("INSERT INTO entries VALUES ('key')")
        database.commit()
        directory = self.memory / "kache-index"
        self.assertEqual(sorted(path.name for path in directory.iterdir()), ["index.db", "index.db-shm", "index.db-wal"])
        self.assertEqual(sorted(path.name for path in self.cache.iterdir()), ["index.db"])

    def test_the_store_moves_its_index_into_a_tmpfs_and_remounts_an_ext4_volume(self) -> None:
        mounts = [
            [str(self.memory), {"target": "/dev/shm", "fstype": "tmpfs", "options": "rw,nosuid,nodev"}],
            ["/", {"target": "/", "fstype": "ext4", "options": "rw,relatime", "remounted": "rw,noatime,nobarrier"}],
        ]
        lines = kache_remote.prepare_store(self.environment(mounts), memory=self.memory)
        self.assertEqual(
            lines,
            [
                f"kache's index and its write-ahead log live in memory, in {self.memory / 'kache-index'}.",
                "The store's volume, /, is remounted with rw,noatime,nobarrier.",
            ],
        )
        self.assertEqual(self.sudo_calls(), [["--non-interactive", "mount", "-o", "remount,noatime,barrier=0", "/"]])

    def test_a_store_without_a_tmpfs_or_an_ext4_volume_keeps_kaches_defaults(self) -> None:
        mounts = [["/", {"target": "/", "fstype": "xfs", "options": "rw,relatime"}]]
        lines = kache_remote.prepare_store(self.environment(mounts), memory=self.memory)
        self.assertEqual(
            lines,
            [
                f"kache's index stays on disk: {self.memory} is not a tmpfs.",
                "The store's volume, /, is xfs, so it was not remounted.",
            ],
        )
        self.assertFalse((self.cache / "index.db").exists())
        self.assertEqual(self.sudo_calls(), [])

    def test_a_refused_remount_keeps_the_volume_as_it_was(self) -> None:
        mounts = [["/", {"target": "/", "fstype": "ext4", "options": "rw,relatime"}]]
        line = kache_remote.lighten_volume(self.cache, self.environment(mounts, sudo_status=1))
        self.assertEqual(
            line,
            "Remounting the store's volume, /, exited with status 1: mount: only root can do that; it keeps its options.",
        )

    def test_the_index_returns_to_the_cache_directory_once_no_daemon_has_it_open(self) -> None:
        self.cache.mkdir(parents=True)
        (self.cache / "index.db").write_text("pages")
        (self.cache / "index.db-wal").write_text("log")
        kache_remote.keep_index_in_memory(self.cache, self.memory)
        self.assertEqual(kache_remote.restore_index(self.cache), f"kache's index is back in {self.cache}.")
        self.assertFalse((self.cache / "index.db").is_symlink())
        self.assertEqual((self.cache / "index.db").read_text(), "pages")
        self.assertEqual((self.cache / "index.db-wal").read_text(), "log")
        self.assertEqual(list((self.memory / "kache-index").iterdir()), [])
        self.assertEqual(kache_remote.restore_index(self.cache), "kache's index is already in the cache directory.")

    def test_an_index_whose_memory_is_gone_leaves_kache_to_start_a_new_one(self) -> None:
        self.cache.mkdir(parents=True)
        (self.cache / "index.db").symlink_to(self.memory / "gone" / "index.db")
        line = kache_remote.restore_index(self.cache)
        self.assertEqual(
            line, f"kache's index in memory, {self.memory / 'gone' / 'index.db'}, is gone; kache starts a new one in {self.cache}."
        )
        self.assertFalse((self.cache / "index.db").exists() or (self.cache / "index.db").is_symlink())

    def test_the_command_line_prepares_restores_and_lightens(self) -> None:
        mounts = [
            [str(self.memory), {"target": "/kache-index", "fstype": "tmpfs", "options": "rw"}],
            ["/", {"target": "/", "fstype": "ext4", "options": "rw,relatime", "remounted": "rw,noatime,nobarrier"}],
        ]
        environment = self.environment(mounts)
        prepared = run_cli(["prepare-index", "--memory", str(self.memory)], environment)
        self.assertEqual(prepared.returncode, 0, prepared.stderr)
        self.assertIn("live in memory", prepared.stdout)
        self.assertTrue((self.cache / "index.db").is_symlink())
        # The daemon creates the index where the symlink points.
        (self.memory / "kache-index" / "index.db").write_text("pages")
        restored = run_cli(["restore-index"], environment)
        self.assertEqual(restored.returncode, 0, restored.stderr)
        self.assertEqual(restored.stdout.strip(), f"kache's index is back in {self.cache}.")
        lightened = run_cli(["lighten-volume", str(self.root)], environment)
        self.assertEqual(lightened.returncode, 0, lightened.stderr)
        self.assertEqual(lightened.stdout.strip(), "The store's volume, /, is remounted with rw,noatime,nobarrier.")

    def test_a_volume_findmnt_cannot_describe_is_left_alone(self) -> None:
        line = kache_remote.lighten_volume(self.cache, self.environment([]))
        self.assertEqual(line, "The store's volume was not remounted: findmnt could not describe it.")
        self.assertEqual(self.sudo_calls(), [])


class StartTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: subprocess.run(["rm", "-rf", str(self.directory)], check=True))
        self.runner_temp = self.directory / "runner-temp"
        self.runner_temp.mkdir()
        self.github_env = self.directory / "github-env"
        self.github_env.touch()

    def environment(self, kache: FakeKache, **extra: str) -> dict[str, str]:
        return kache.environment(
            RUNNER_TEMP=str(self.runner_temp),
            GITHUB_ENV=str(self.github_env),
            KACHE_MANIFEST_KEY="deloxide-deloxide-order-Linux-X64",
            KACHE_S3_PREFIX="ambient-prefix",
            KACHE_CACHE_DIR=str(self.directory / "kache-cache"),
        ) | REMOTE_ENVIRONMENT | extra

    def start(self, behavior: dict, **extra: str) -> tuple[DaemonState, FakeKache, SteppingClock]:
        kache = FakeKache(self.directory, behavior)
        clock = SteppingClock()
        # Four seconds of polling, three polls, keep the tests of a daemon that never answers quick.
        with contextlib.redirect_stdout(io.StringIO()):
            daemon = kache_remote.start(self.environment(kache, **extra), clock, verify_seconds=4.0)
        return daemon, kache, clock

    def runtime_with_log(self, text: str) -> Path:
        runtime = self.directory / "kache-runtime"
        runtime.mkdir(exist_ok=True)
        (runtime / "daemon-pr-ff050e90.log").write_text(text)
        return runtime

    def test_the_daemon_starts_with_the_configured_remote_and_later_steps_inherit_it(self) -> None:
        daemon, kache, clock = self.start({})

        configuration = self.runner_temp / "kache-remote.toml"
        self.assertEqual(daemon, DaemonState(True, str(configuration), "s3://kache-ci/artifacts", False))
        self.assertEqual(clock.sleeps, [])
        self.assertEqual(
            self.github_env.read_text().splitlines(),
            [
                f"KACHE_CONFIG={configuration}",
                "KACHE_S3_ACCESS_KEY=test-access-key",
                "KACHE_S3_SECRET_KEY=test-secret-key",
                "KACHE_MANIFEST_KEY=deloxide-deloxide-order-Linux-X64",
                "KACHE_NAMESPACE=deloxide-deloxide-order-Linux-X64",
            ],
        )
        written = tomllib.loads(configuration.read_text())["cache"]
        self.assertEqual(written["remote"]["bucket"], "kache-ci")
        self.assertEqual(written["local_max_size"], f"{quarter_of_volume_gib(self.directory)}GiB")
        started = kache.calls()[0]
        self.assertEqual(started["argv"], ["daemon", "start"])
        self.assertEqual(
            started["env"],
            {
                "KACHE_CONFIG": str(configuration),
                "KACHE_S3_BUCKET": None,
                "KACHE_S3_PREFIX": None,
                "KACHE_S3_ACCESS_KEY": "test-access-key",
                "KACHE_MANIFEST_KEY": "deloxide-deloxide-order-Linux-X64",
                "KACHE_NAMESPACE": "deloxide-deloxide-order-Linux-X64",
            },
        )

    def test_a_daemon_without_the_remote_fails_the_job_after_two_starts_with_its_log(self) -> None:
        runtime = self.runtime_with_log("starting\nerror: the store index is locked\n")
        for behavior, refusal in (
            ({"config_path": "/elsewhere/kache.toml"}, "the daemon reads configuration /elsewhere/kache.toml"),
            ({"remote": "s3://other/artifacts"}, "the configuration names remote s3://other/artifacts"),
            ({"running": [False]}, "the daemon is not running"),
            ({"schema_version": 2}, "answered outside JSON schema 1"),
        ):
            with self.subTest(refusal=refusal):
                subprocess.run(["rm", "-rf", str(self.directory / "bin")], check=True)
                (self.directory / "kache.log").unlink(missing_ok=True)
                self.github_env.write_text("")
                with self.assertRaises(StartError) as raised:
                    self.start(behavior, KACHE_RUNTIME_DIR=str(runtime))
                self.assertIn(refusal, str(raised.exception))
                self.assertIn("s3://kache-ci/artifacts", str(raised.exception))
                self.assertIn(f"The end of {runtime}/daemon-pr-ff050e90.log:\nstarting\nerror: the store index is locked", str(raised.exception))
                started = [
                    call["argv"] for call in map(json.loads, (self.directory / "kache.log").read_text().splitlines())
                    if call["argv"] == ["daemon", "start"]
                ]
                self.assertEqual(len(started), kache_remote.DAEMON_STARTS)

    def test_a_daemon_that_answers_only_after_its_second_start_is_accepted_with_a_warning(self) -> None:
        runtime = self.runtime_with_log("error: socket bind timed out\n")
        kache = FakeKache(self.directory, {"running": [False, False, False, True]})
        clock = SteppingClock()
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            daemon = kache_remote.start(
                self.environment(kache, KACHE_RUNTIME_DIR=str(runtime)), clock, verify_seconds=4.0
            )
        self.assertTrue(daemon.running)
        self.assertEqual(kache.commands().count(["daemon", "start"]), 2)
        self.assertIn(
            "::warning title=kache daemon start 1 of 2::the kache daemon is not running with "
            "s3://kache-ci/artifacts: the daemon is not running",
            output.getvalue(),
        )
        self.assertIn("error: socket bind timed out", output.getvalue())

    def test_the_store_is_prepared_before_any_daemon_opens_its_index(self) -> None:
        kache = FakeKache(self.directory, {})
        seen: list[list[list[str]]] = []

        def prepare(environment: Mapping[str, str]) -> list[str]:
            seen.append(kache.commands())
            return ["kache's index and its write-ahead log live in memory."]

        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            kache_remote.start(self.environment(kache), SteppingClock(), verify_seconds=4.0, prepare=prepare)
        self.assertEqual(seen, [[]])
        self.assertIn("kache's index and its write-ahead log live in memory.", output.getvalue())

    def test_daemon_logs_show_the_end_of_each_log_or_say_there_is_none(self) -> None:
        runtime = self.runtime_with_log("".join(f"line {number}\n" for number in range(100)))
        shown = kache_remote.daemon_logs({"KACHE_RUNTIME_DIR": str(runtime)})
        self.assertTrue(shown.startswith(f"The end of {runtime}/daemon-pr-ff050e90.log:\nline 60\n"), shown[:120])
        self.assertTrue(shown.endswith("line 99"))
        empty = self.directory / "empty-runtime"
        empty.mkdir()
        self.assertEqual(kache_remote.daemon_logs({"KACHE_RUNTIME_DIR": str(empty)}), f"{empty} holds no kache daemon log.")
        self.assertEqual(
            kache_remote.daemon_logs({}), "KACHE_RUNTIME_DIR is not set, so no kache daemon log can be shown."
        )

    def test_a_daemon_that_answers_on_the_second_poll_is_accepted(self) -> None:
        _daemon, kache, clock = self.start({"running": [False, True]})

        self.assertEqual(clock.sleeps, [kache_remote.VERIFY_RETRY_SECONDS])
        self.assertEqual(kache.commands().count(["--json", "daemon", "status"]), 2)

    def test_a_daemon_that_does_not_start_fails_the_job(self) -> None:
        with self.assertRaises(StartError) as raised:
            self.start({"daemon_start_status": 3})
        self.assertIn("`kache daemon start` exited with status 3", str(raised.exception))

    def test_a_job_without_a_runner_directory_is_refused(self) -> None:
        with self.assertRaises(ConfigurationError) as raised:
            self.start({}, RUNNER_TEMP="")
        self.assertIn("RUNNER_TEMP is required", str(raised.exception))

    def test_a_value_later_steps_cannot_inherit_is_refused(self) -> None:
        with self.assertRaises(ConfigurationError) as raised:
            self.start({}, KACHE_S3_SECRET_KEY="first-line\nKACHE_CONFIG=/injected")
        self.assertIn("KACHE_S3_SECRET_KEY must be a single line", str(raised.exception))

    def test_main_maps_each_command_to_its_exit_status(self) -> None:
        kache = FakeKache(self.directory, {"daemon_start_status": 1})
        configuration = self.directory / "image-build.toml"
        # An image build keeps kache's default cache directory below its HOME.
        image_build = REMOTE_ENVIRONMENT | {"HOME": str(self.directory / "root")}
        missing_bucket = dict(image_build)
        missing_bucket.pop("KACHE_S3_BUCKET")
        for environment, arguments, status, message in (
            (image_build, ["configure", str(configuration)], 0, ""),
            (missing_bucket, ["configure", str(configuration)], 2, "configure: KACHE_S3_BUCKET is required"),
            (self.environment(kache), ["start"], 1, "`kache daemon start` exited with status 1"),
        ):
            with self.subTest(arguments=arguments, status=status):
                errors = io.StringIO()
                with (
                    mock.patch.dict(os.environ, environment, clear=True),
                    contextlib.redirect_stdout(io.StringIO()),
                    contextlib.redirect_stderr(errors),
                ):
                    self.assertEqual(kache_remote.main(arguments), status)
                self.assertIn(message, errors.getvalue())
        written = tomllib.loads(configuration.read_text())["cache"]
        self.assertEqual(written["remote"]["prefix"], "artifacts")
        self.assertEqual(written["local_max_size"], f"{quarter_of_volume_gib(self.directory)}GiB")

    def test_the_command_line_reports_configuration_and_start_failures(self) -> None:
        kache = FakeKache(self.directory, {"daemon_start_status": 1})
        environment = self.environment(kache)
        environment.pop("KACHE_S3_BUCKET")
        result = run_cli(["start"], environment)
        self.assertEqual(result.returncode, 2)
        self.assertIn("start: KACHE_S3_BUCKET is required", result.stderr)

        result = run_cli(["start"], self.environment(kache))
        self.assertEqual(result.returncode, 1)
        self.assertIn("`kache daemon start` exited with status 1", result.stderr)


if __name__ == "__main__":
    unittest.main()
