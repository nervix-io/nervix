from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import tomllib
import unittest

from scripts.build_onnxruntime import BuildSpec, RuntimeBuild, native_platform
from scripts.tests.test_build_onnxruntime import construct_package, fixture_repository


ROOT = Path(__file__).resolve().parents[2]


@unittest.skipUnless(shutil.which("just") and shutil.which("uv"), "requires just and uv")
class ArtifactRecipeTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.repo = fixture_repository(self.root / "repository")
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.log = self.root / "publication.jsonl"
        uv = self.bin / "uv"
        uv.write_text("""#!/usr/bin/env python3
import json, os, pathlib, sys
sys.path.insert(0, str(pathlib.Path.cwd()))
from scripts import build_onnxruntime as builder
assert sys.argv[1:6] == ['run', '--locked', 'python', '-m', 'scripts.build_onnxruntime']
class Cache:
    def restore(self, spec, destination):
        return None
class Publisher:
    bucket = 'fixture'
    def key(self, spec):
        return 'onnxruntime/' + spec.object_key
    def publish(self, build):
        archive = pathlib.Path(os.environ['RECIPE_ARCHIVE'])
        build.write_archive(str(archive))
        import hashlib
        assert hashlib.sha256(archive.read_bytes()).hexdigest() == build.spec.artifact_checksum
        with pathlib.Path(os.environ['RECIPE_PUBLICATION']).open('a') as stream:
            stream.write(json.dumps({'key': self.key(build.spec), 'package': str(build.package_dir)}) + '\\n')
if os.environ.get('RECIPE_PUBLIC_URL'):
    builder.R2_PUBLIC_URL = os.environ['RECIPE_PUBLIC_URL']
else:
    builder.R2Cache = Cache
builder.R2Publisher.configured = lambda: Publisher()
def compile_artifact(self, destination):
    (destination / 'lib').mkdir()
    (destination / 'include').mkdir()
    (destination / 'lib/libonnxruntime.a').write_bytes(b'!<arch>\\nfixture')
    (destination / 'include/onnxruntime_c_api.h').write_text('fixture header')
    (destination / 'LICENSE').write_text('fixture license')
    (destination / 'ThirdPartyNotices.txt').write_text('fixture notices')
    if self.spec.cuda_enabled:
        runtime = destination / 'runtime/lib'
        runtime.mkdir(parents=True)
        for name in ('libonnxruntime_providers_shared.so', 'libonnxruntime_providers_cuda.so',
                     'libcudnn_graph.so.9', 'libnvrtc-builtins.so.13.2'):
            (runtime / name).write_bytes(b'fixture library')
    with pathlib.Path(os.environ['RECIPE_COMPILATION']).open('a') as stream:
        stream.write(self.spec.platform + '\\n')
builder.RuntimeBuild._build = compile_artifact
sys.argv = ['onnxruntime', *sys.argv[6:]]
sys.exit(builder.main())
""")
        uv.chmod(0o755)
        docker = self.bin / "docker"
        docker.write_text("#!/bin/sh\nexit 99\n")
        docker.chmod(0o755)
        self.environment = os.environ.copy()
        for name in ("CI", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"):
            self.environment.pop(name, None)
        self.environment.update({
            "PATH": f"{self.bin}:{self.environment['PATH']}",
            "NERVIX_ONNXRUNTIME_DIR": str(self.root / "stage"),
            "RECIPE_PUBLICATION": str(self.log), "RECIPE_ARCHIVE": str(self.root / "package.tar.gz"),
            "RECIPE_COMPILATION": str(self.root / "compilation.log"),
            "CC": "unavailable-clang", "CXX": "unavailable-clang++",
            "KACHE_S3_BUCKET": "fixture", "KACHE_S3_REGION": "fixture",
            "KACHE_S3_ENDPOINT": "https://fixture.r2.cloudflarestorage.com",
            "KACHE_S3_ACCESS_KEY": "fixture", "KACHE_S3_SECRET_KEY": "fixture",
        })

    def test_development_and_ci_require_published_artifacts_before_building_the_product(self) -> None:
        recipes = [
            ["build-server"], ["test"], ["docker-build-debian"], ["test-admission-runtime"],
            ["test-runtime"], ["test-capability-docs"], ["test-endpoint-intake"], ["bench-endpoint-routing"],
            ["bench-admitted-work"], ["bench-state-replication"], ["bench-task-handles"],
            ["coverage-task-handles"], ["coverage-runtime"], ["bench-smoke"], ["ratchet"],
            ["coverage-scenarios", str(self.root / "scenarios.lcov")],
            ["coverage-scenarios-append", str(self.root / "scenarios.lcov")],
            ["test-package-test", "nervix-server", "scenarios"],
        ]
        for ci in ("", "true"):
            self.environment["CI"] = ci
            for recipe in recipes:
                with self.subTest(ci=ci, recipe=recipe):
                    result = subprocess.run(["just", *recipe], cwd=self.repo, env=self.environment,
                                            capture_output=True, text=True, timeout=30)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("normal development requires a published R2 artifact", result.stderr)
                    self.assertIn("ask a maintainer", result.stderr)
        self.assertFalse(Path(self.environment["RECIPE_COMPILATION"]).exists())

    def test_fetch_reuses_a_pinned_artifact_without_host_compilers(self) -> None:
        build = RuntimeBuild(BuildSpec.create("native", repo=self.repo), self.root / "stage")
        build.package_dir.mkdir(parents=True)
        construct_package(build.package_dir, platform=build.spec.platform)
        build._seal(build.package_dir)
        build.checksum()
        for ci in ("", "true"):
            self.environment["CI"] = ci
            result = subprocess.run(["just", "fetch-onnxruntime"], cwd=self.repo, env=self.environment,
                                    capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn(str(build.package_dir / "lib"), result.stdout)
        self.assertFalse(Path(self.environment["RECIPE_COMPILATION"]).exists())

    def test_fetch_downloads_public_artifacts_and_reuses_them_without_credentials(self) -> None:
        spec = BuildSpec.create("native", repo=self.repo)
        producer = RuntimeBuild(spec, self.root / "producer")
        producer.package_dir.mkdir(parents=True)
        construct_package(producer.package_dir, platform=spec.platform)
        producer._seal(producer.package_dir)
        producer.checksum()
        payload = producer.archive().read_bytes()
        requests = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                requests.append((self.path, self.headers.get("User-Agent")))
                self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args: object) -> None:
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        worker = threading.Thread(target=server.serve_forever)
        worker.start()
        self.environment["RECIPE_PUBLIC_URL"] = f"http://127.0.0.1:{server.server_port}"
        try:
            for ci in ("", "true"):
                self.environment["CI"] = ci
                stage = self.root / ("ci-download" if ci else "development-download")
                self.environment["NERVIX_ONNXRUNTIME_DIR"] = str(stage)
                for _ in range(2):
                    result = subprocess.run(["just", "fetch-onnxruntime"], cwd=self.repo, env=self.environment,
                                            capture_output=True, text=True, timeout=30)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    build = RuntimeBuild(spec, stage)
                    build.validate_package()
                    self.assertEqual(result.stdout.strip(), str(build.package_dir / "lib"))
        finally:
            server.shutdown()
            worker.join(timeout=10)
            server.server_close()
        self.assertEqual(requests, [(f"/onnxruntime/{spec.object_key}", "nervix-onnxruntime-artifacts")] * 2)
        self.assertFalse(Path(self.environment["RECIPE_COMPILATION"]).exists())

    def test_maintainer_publication_builds_and_pins_the_artifact_through_dependencies(self) -> None:
        result = subprocess.run(["just", "publish-onnxruntime"], cwd=self.repo, env=self.environment,
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        build = RuntimeBuild(BuildSpec.create("native", repo=self.repo), self.root / "stage")
        build.prepare()
        self.assertEqual(Path(self.environment["RECIPE_COMPILATION"]).read_text(), build.spec.platform + "\n")
        self.assertIn("published ONNX Runtime to R2", result.stdout)

    def test_manual_publish_uses_the_completed_package_and_is_disabled_in_ci(self) -> None:
        build = RuntimeBuild(BuildSpec.create("native", repo=self.repo), self.root / "stage")
        build.package_dir.mkdir(parents=True)
        construct_package(build.package_dir, platform=build.spec.platform)
        build._seal(build.package_dir)
        recipe = ["just", "publish-onnxruntime", "native"]
        result = subprocess.run(recipe, cwd=self.repo, env=self.environment,
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(f"published ONNX Runtime to R2: fixture/onnxruntime/{build.spec.object_key}", result.stdout)
        pins = tomllib.loads((self.repo / "scripts/onnxruntime/checksums.toml").read_text())
        self.assertEqual(pins[build.spec.platform]["fingerprint"], build.spec.fingerprint)
        self.assertEqual(pins[build.spec.platform]["sha256"], build.spec.artifact_checksum)
        records = self.log.read_text().splitlines()
        self.assertEqual(json.loads(records[0]), {
            "key": f"onnxruntime/{build.spec.object_key}", "package": str(build.package_dir),
        })
        self.environment["CI"] = "true"
        blocked = subprocess.run(recipe, cwd=self.repo, env=self.environment,
                                 capture_output=True, text=True, timeout=30)
        self.assertNotEqual(blocked.returncode, 0)
        self.assertIn("manual operation and is disabled in CI", blocked.stderr)
        self.assertEqual(self.log.read_text().splitlines(), records)

    def test_ci_missing_version_fails_before_the_product_image_build(self) -> None:
        self.environment["CI"] = "true"
        result = subprocess.run(["just", "docker-build-debian"], cwd=self.repo, env=self.environment,
                                capture_output=True, text=True, timeout=30)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unavailable in local storage and R2", result.stderr)
        self.assertIn("just publish-onnxruntime linux/amd64", result.stderr)
        self.assertFalse(self.log.exists())

    @unittest.skipUnless(native_platform().startswith("linux/"), "native CUDA verification requires Linux")
    def test_cuda_verification_prepares_the_cached_package_and_requires_gpu_execution(self) -> None:
        build = RuntimeBuild(BuildSpec.create("native", repo=self.repo), self.root / "stage")
        build.package_dir.mkdir(parents=True)
        construct_package(build.package_dir, platform=build.spec.platform)
        build._seal(build.package_dir)
        build.checksum()
        compiler = self.bin / "clang"
        compiler.write_text("""#!/usr/bin/env python3
import os, pathlib, sys
if sys.argv[1:] == ['--version']:
    print('clang version 22.1.8')
else:
    assert sys.argv[-2] == '-o', sys.argv
    executable = pathlib.Path(sys.argv[-1])
    executable.write_text('''#!/usr/bin/env python3
import os, pathlib, sys
assert sys.argv[-1] == 'cuda', sys.argv
runtime = pathlib.Path(os.environ['LD_LIBRARY_PATH'])
assert (pathlib.Path(sys.argv[0]).parent / 'libonnxruntime_providers_cuda.so').resolve() == runtime / 'libonnxruntime_providers_cuda.so'
pathlib.Path(os.environ['RECIPE_PUBLICATION']).write_text('GPU verification requested')
''')
    executable.chmod(0o755)
""")
        compiler.chmod(0o755)
        archiver = self.bin / "llvm-ar"
        archiver.write_text("#!/bin/sh\nprintf 'LLVM version 22.1.8\\n'\n")
        archiver.chmod(0o755)
        self.environment.update({"CC": str(compiler), "CXX": str(compiler),
                                 "CUDACXX": "unavailable-nvcc", "CUDA_HOME": "/unavailable-cuda",
                                 "CUDNN_HOME": "/unavailable-cudnn"})
        result = subprocess.run(["just", "verify-onnxruntime"], cwd=self.repo,
                                env=self.environment, capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("verified ONNX Runtime 1.24.2 linux/amd64", result.stdout)
        self.assertEqual(self.log.read_text(), "GPU verification requested")
        build.validate_package()


if __name__ == "__main__":
    unittest.main()
