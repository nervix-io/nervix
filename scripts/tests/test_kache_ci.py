from __future__ import annotations

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
            self.assertIn("uses: kunobi-ninja/kache-action@v1", job)
            self.assertIn("github-cache: \"false\"", job)
            self.assertIn(
                "continue-on-error: true\n        if: always()\n        run: kache stats",
                job,
            )
            self.assertIn("s3-bucket: ${{ vars.KACHE_S3_BUCKET }}", job)
            self.assertIn(
                "s3-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}", job
            )
            self.assertIn(
                "s3-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}", job
            )

    def test_docker_build_uses_kache_daemon_and_optional_s3_remote(self) -> None:
        workflow = Path(".github/workflows/docker-build.yaml").read_text()
        dockerfile = Path("Dockerfile.debian").read_text()
        justfile = Path("justfile").read_text()

        self.assertNotIn("useblacksmith/setup-docker-builder", workflow)
        self.assertIn("uses: docker/setup-buildx-action@v3", workflow)
        self.assertIn("KACHE_S3_BUCKET: ${{ vars.KACHE_S3_BUCKET }}", workflow)
        self.assertIn(
            "KACHE_S3_ACCESS_KEY: ${{ secrets.AWS_ACCESS_KEY_ID }}", workflow
        )
        self.assertIn(
            "KACHE_S3_SECRET_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}", workflow
        )

        self.assertIn('--build-arg "KACHE_S3_ACCESS_KEY=', justfile)
        self.assertIn('--build-arg "KACHE_S3_SECRET_KEY=', justfile)
        self.assertIn("ENV RUSTC_WRAPPER=kache", dockerfile)
        self.assertIn("kache daemon start", dockerfile)
        self.assertIn("kache stats", dockerfile)
        self.assertIn("kache save-manifest", dockerfile)
        self.assertIn("kache sync --push", dockerfile)


if __name__ == "__main__":
    unittest.main()
