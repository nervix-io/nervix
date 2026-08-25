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

        expected_jobs = (
            check_workflow.split("\n  checks:", maxsplit=1)[1].split(
                "\n  tests:", maxsplit=1
            )[0],
            check_workflow.split("\n  tests:", maxsplit=1)[1],
            docker_workflow.split("\n  benchmark:", maxsplit=1)[1].split(
                "\n  benchmark-comment:", maxsplit=1
            )[0],
            docker_workflow.split("\n  build-book:", maxsplit=1)[1],
        )

        for job in expected_jobs:
            configure_index = job.index("bash scripts/configure_kache_remote.sh")
            setup_index = job.index("uses: kunobi-ninja/kache-action@v1")
            self.assertLess(configure_index, setup_index)
            self.assertNotIn("if: vars.KACHE_S3_BUCKET != ''", job)
            self.assertIn('echo "KACHE_CONFIG=${config}" >> "${GITHUB_ENV}"', job)
            self.assertIn("uses: kunobi-ninja/kache-action@v1", job)
            self.assertIn("github-cache: \"false\"", job)
            self.assertIn(
                "continue-on-error: true\n        if: always()\n        run: kache stats",
                job,
            )
            self.assertIn("s3-bucket: ${{ vars.KACHE_S3_BUCKET }}", job)
            self.assertIn("s3-region: ${{ vars.KACHE_S3_REGION }}", job)
            self.assertIn(
                "KACHE_S3_ACCESS_KEY: ${{ secrets.AWS_ACCESS_KEY_ID }}", job
            )
            self.assertIn(
                "KACHE_S3_SECRET_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}", job
            )
            self.assertIn(
                "s3-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}", job
            )
            self.assertIn(
                "s3-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}", job
            )

    def test_remote_config_is_file_backed_for_the_daemon(self) -> None:
        script = Path("scripts/configure_kache_remote.sh")
        self.assertTrue(script.is_file())

        with tempfile.TemporaryDirectory() as temporary_directory:
            config = Path(temporary_directory) / "kache.toml"
            required_environment = {
                "KACHE_S3_BUCKET": "nervix-ci-kache-us-west-1",
                "KACHE_S3_REGION": "us-west-1",
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
                        "bucket": "nervix-ci-kache-us-west-1",
                        "region": "us-west-1",
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
        self.assertIn("KACHE_S3_BUCKET: ${{ vars.KACHE_S3_BUCKET }}", workflow)
        self.assertIn("KACHE_S3_REGION: ${{ vars.KACHE_S3_REGION }}", workflow)
        self.assertIn(
            "KACHE_S3_ACCESS_KEY: ${{ secrets.AWS_ACCESS_KEY_ID }}", workflow
        )
        self.assertIn(
            "KACHE_S3_SECRET_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}", workflow
        )

        self.assertIn('--build-arg "KACHE_S3_ACCESS_KEY=', justfile)
        self.assertIn('--build-arg "KACHE_S3_SECRET_KEY=', justfile)
        self.assertIn(
            ': "${KACHE_S3_REGION:?KACHE_S3_REGION is required}"', justfile
        )
        self.assertNotIn('if [[ -n "${KACHE_S3_ACCESS_KEY:-}"', justfile)
        self.assertNotIn("${KACHE_S3_REGION:-us-east-1}", justfile)
        self.assertIn("ENV RUSTC_WRAPPER=kache", dockerfile)
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


if __name__ == "__main__":
    unittest.main()
