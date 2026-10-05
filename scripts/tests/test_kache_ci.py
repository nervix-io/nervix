from __future__ import annotations

import os
import subprocess
import tempfile
import tomllib
import unittest
from pathlib import Path


class KacheCiTests(unittest.TestCase):
    def test_native_rust_jobs_use_s3_backed_kache_without_actions_cache(self) -> None:
        check_workflow = Path(".github/workflows/check.yaml").read_text()
        docker_workflow = Path(".github/workflows/docker-build.yaml").read_text()

        self.assertNotIn("actions/cache@", check_workflow)
        self.assertNotIn("actions/cache@", docker_workflow)
        self.assertNotIn("enable-cache: true", docker_workflow)
        for workflow in (check_workflow, docker_workflow):
            self.assertIn('KACHE_VERSION: "0.28.1"', workflow)
            self.assertIn('KACHE_REMOTE_KEY_LISTING: "false"', workflow)

        import re

        def job_sections(workflow: str) -> dict[str, str]:
            starts = list(re.finditer(r"^  ([a-z][a-z-]*):\n", workflow, re.MULTILINE))
            return {
                match.group(1): workflow[match.start() : starts[index + 1].start() if index + 1 < len(starts) else len(workflow)]
                for index, match in enumerate(starts)
            }

        jobs = job_sections(check_workflow) | job_sections(docker_workflow)
        kache_jobs = (
            "checks",
            "tests",
            "scenarios",
            "client-conformance",
            "extra-tests",
            "shuttle",
            "loom",
            "turmoil",
            "benchmark",
            "build-book",
        )
        for name in kache_jobs:
            with self.subTest(job=name):
                job = jobs[name]
                configure_index = job.index("bash scripts/configure_kache_remote.sh")
                setup_index = job.index("uses: kunobi-ninja/kache-action@v1")
                self.assertLess(configure_index, setup_index)
                self.assertIn('echo "KACHE_CONFIG=${config}" >> "${GITHUB_ENV}"', job)
                self.assertIn('github-cache: "false"', job)
                self.assertIn('sync: "false"', job)
                self.assertIn('cache-executables: "true"', job)
                self.assertIn('s3-prefix: "artifacts"', job)
                self.assertIn("s3-bucket: ${{ vars.KACHE_BUCKET }}", job)
                self.assertIn("s3-region: ${{ vars.KACHE_BUCKET_REGION }}", job)
                self.assertIn("KACHE_S3_ENDPOINT: ${{ vars.KACHE_BUCKET_ENDPOINT }}", job)
                self.assertIn("s3-endpoint: ${{ vars.KACHE_BUCKET_ENDPOINT }}", job)
                self.assertIn("KACHE_S3_ACCESS_KEY: ${{ secrets.KACHE_BUCKET_ACCESS_KEY_ID }}", job)
                self.assertIn("KACHE_S3_SECRET_KEY: ${{ secrets.KACHE_BUCKET_SECRET_ACCESS_KEY }}", job)
                self.assertIn("s3-access-key-id: ${{ secrets.KACHE_BUCKET_ACCESS_KEY_ID }}", job)
                self.assertIn("s3-secret-access-key: ${{ secrets.KACHE_BUCKET_SECRET_ACCESS_KEY }}", job)
                self.assertIn("python3 scripts/publish_kache_report.py", job)
                self.assertIn("Upload kache report", job)
                self.assertIn('max-size: "1TiB"', job)

        self.assertNotIn("KACHE_GC_EVICT_SHARED", docker_workflow)

        report_script = Path("scripts/publish_kache_report.py").read_text()
        self.assertIn('run("kache", "doctor")', report_script)
        self.assertIn('run("kache", "why-miss", crate)', report_script)
        self.assertIn('run("kache", "report", "--format", "json"', report_script)

    def test_remote_config_is_file_backed_for_the_daemon(self) -> None:
        script = Path("scripts/configure_kache_remote.sh")
        self.assertTrue(script.is_file())

        with tempfile.TemporaryDirectory() as temporary_directory:
            config = Path(temporary_directory) / "kache.toml"
            required_environment = {
                "KACHE_S3_BUCKET": "kache-ci",
                "KACHE_S3_REGION": "auto",
                "KACHE_S3_ENDPOINT": "https://account-id.r2.cloudflarestorage.com",
                "KACHE_S3_ACCESS_KEY": "test-access-key",
                "KACHE_S3_SECRET_KEY": "test-secret-key",
            }
            environment = os.environ | required_environment
            subprocess.run(
                ["bash", str(script), str(config)],
                check=True,
                env=environment,
            )

            parsed = tomllib.loads(config.read_text())

        self.assertEqual(
            parsed,
            {
                "cache": {
                    "remote": {
                        "type": "s3",
                        "bucket": "kache-ci",
                        "region": "auto",
                        "endpoint": "https://account-id.r2.cloudflarestorage.com",
                        "pull_request_prefix": "artifacts-pr",
                    }
                }
            },
        )

        for missing_variable in required_environment:
            with self.subTest(missing_variable=missing_variable):
                incomplete_environment = environment.copy()
                incomplete_environment.pop(missing_variable)
                result = subprocess.run(
                    ["bash", str(script), str(config)],
                    capture_output=True,
                    check=False,
                    env=incomplete_environment,
                    text=True,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(f"{missing_variable} is required", result.stderr)

    def test_docker_build_requires_the_s3_remote(self) -> None:
        workflow = Path(".github/workflows/docker-build.yaml").read_text()
        dockerfile = Path("Dockerfile.debian").read_text()
        justfile = Path("justfile").read_text()

        self.assertNotIn("useblacksmith/setup-docker-builder", workflow)
        self.assertIn("uses: docker/setup-buildx-action@v3", workflow)
        self.assertIn("KACHE_S3_BUCKET: ${{ vars.KACHE_BUCKET }}", workflow)
        self.assertIn("KACHE_S3_REGION: ${{ vars.KACHE_BUCKET_REGION }}", workflow)
        self.assertIn("KACHE_S3_ENDPOINT: ${{ vars.KACHE_BUCKET_ENDPOINT }}", workflow)
        self.assertIn(
            "KACHE_S3_ACCESS_KEY: ${{ secrets.KACHE_BUCKET_ACCESS_KEY_ID }}", workflow
        )
        self.assertIn(
            "KACHE_S3_SECRET_KEY: ${{ secrets.KACHE_BUCKET_SECRET_ACCESS_KEY }}", workflow
        )

        self.assertIn('--build-arg "KACHE_S3_ACCESS_KEY=', justfile)
        self.assertIn('--build-arg "KACHE_S3_SECRET_KEY=', justfile)
        self.assertIn('--build-arg "KACHE_S3_ENDPOINT=${KACHE_S3_ENDPOINT}"', justfile)
        self.assertIn(
            ': "${KACHE_S3_ENDPOINT:?KACHE_S3_ENDPOINT is required}"', justfile
        )
        self.assertIn(
            ': "${KACHE_S3_REGION:?KACHE_S3_REGION is required}"', justfile
        )
        self.assertNotIn('if [[ -n "${KACHE_S3_ACCESS_KEY:-}"', justfile)
        self.assertNotIn("${KACHE_S3_REGION:-us-east-1}", justfile)
        self.assertIn("ENV RUSTC_WRAPPER=kache", dockerfile)
        self.assertIn("ARG KACHE_VERSION=0.28.1", dockerfile)
        self.assertIn("ENV KACHE_CACHE_EXECUTABLES=true", dockerfile)
        self.assertIn("ENV KACHE_MAX_SIZE=1TiB", dockerfile)
        self.assertIn("ENV KACHE_REMOTE_KEY_LISTING=false", dockerfile)
        self.assertIn("ARG KACHE_S3_ENDPOINT", dockerfile)
        self.assertIn(
            ': "${KACHE_S3_ENDPOINT:?KACHE_S3_ENDPOINT is required}"', dockerfile
        )
        configure_index = dockerfile.index("bash scripts/configure_kache_remote.sh")
        daemon_index = dockerfile.index("kache daemon start")
        self.assertLess(configure_index, daemon_index)
        self.assertIn("export KACHE_CONFIG=", dockerfile)
        self.assertNotIn("ARG KACHE_S3_REGION=", dockerfile)
        self.assertNotIn('if [ -n "${KACHE_S3_ACCESS_KEY', dockerfile)
        self.assertIn("kache daemon start", dockerfile)
        self.assertIn("kache stats", dockerfile)
        self.assertIn("kache save-manifest", dockerfile)
        self.assertIn("kache sync --push", dockerfile)

    def test_remote_endpoint_requires_an_account_url(self) -> None:
        environment = os.environ | {
            "KACHE_S3_BUCKET": "kache-ci",
            "KACHE_S3_REGION": "auto",
            "KACHE_S3_ACCESS_KEY": "test-access-key",
            "KACHE_S3_SECRET_KEY": "test-secret-key",
        }
        with tempfile.TemporaryDirectory() as temporary_directory:
            config = Path(temporary_directory) / "kache.toml"
            for endpoint in (
                "https://account-id.r2.cloudflarestorage.com/kache-ci",
                "account-id.r2.cloudflarestorage.com",
                "https://user:password@example.com",
                'https://example.com/\n[other]\nvalue = "injected"',
            ):
                with self.subTest(endpoint=endpoint):
                    result = subprocess.run(
                        ["bash", "scripts/configure_kache_remote.sh", str(config)],
                        capture_output=True,
                        env=environment | {"KACHE_S3_ENDPOINT": endpoint},
                        text=True,
                    )
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("KACHE_S3_ENDPOINT", result.stderr)


if __name__ == "__main__":
    unittest.main()
