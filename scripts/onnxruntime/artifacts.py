"""Own complete manual builds, publication, and normal artifact preparation.

Build tool downloads and command ordering live here. The runtime producer owns compilation
and artifact identity; changing cache locations or tool installation does not invalidate it.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import shutil
import sys
import tempfile

from scripts.build_onnxruntime import BuildSpec, R2Publisher, RuntimeBuild, lock, run
from scripts.onnxruntime.bootstrap import Bootstrap
from scripts.onnxruntime.toolchain import BuildError


def cache_root() -> Path:
    return Path(os.environ.get("NERVIX_ONNXRUNTIME_DIR") or Path.home() / ".cache/nervix-build/onnxruntime")


class ManagedRuntimeBuild(RuntimeBuild):
    force_rebuild = False

    def build_source(self, *, force: bool = False) -> Path:
        if not force:
            return super().build_source()
        if os.environ.get("CI") == "true":
            raise BuildError("ONNX Runtime source compilation is a manual operation and is disabled in CI")
        with lock(self.stage_root / "locks" / f"{self.spec.fingerprint}.lock"):
            self.package_dir.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.TemporaryDirectory(dir=self.package_dir.parent) as temporary:
                destination = Path(temporary) / "package"
                destination.mkdir()
                self.force_rebuild = True
                try:
                    self._build(destination)
                finally:
                    self.force_rebuild = False
                self._seal(destination)
                backup = Path(temporary) / "previous-package"
                if self.package_dir.exists():
                    self.package_dir.rename(backup)
                try:
                    destination.rename(self.package_dir)
                    (self.stage_root / "verified" / f"{self.spec.fingerprint}.json").unlink(missing_ok=True)
                except BaseException:
                    if self.package_dir.exists():
                        shutil.rmtree(self.package_dir)
                    if backup.exists():
                        backup.rename(self.package_dir)
                    raise
            return self.package_dir / "lib"

    def _build(self, destination: Path) -> None:
        bootstrap = Bootstrap(self.stage_root, self.spec.platform)
        with bootstrap.environment():
            super()._build(destination)
        self.provenance["downloaded_tools"] = bootstrap.installer.receipts

    def compile(self, destination: Path) -> None:
        # RuntimeBuild holds the compiler lock here. Keep sources and fetched dependencies while
        # removing Ninja's outputs so a forced build recompiles them with the selected compilers.
        if self.force_rebuild and (self.build_dir / "build.ninja").is_file():
            run(["cmake", "--build", str(self.build_dir), "--target", "clean"])
        super().compile(destination)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["build", "fetch", "pin", "publish", "path", "verify"])
    parser.add_argument("--platform", default="native")
    parser.add_argument("--stage", type=Path, default=cache_root())
    parser.add_argument("--jobs", type=int, default=min(os.cpu_count() or 1, 4))
    parser.add_argument("--unchecked", action="store_true")
    parser.add_argument("--force", action="store_true", help="recompile a completed package; reuse cached sources and SDKs")
    arguments = parser.parse_args()
    if arguments.jobs < 1:
        parser.error("--jobs must be positive")
    if arguments.force and arguments.operation != "build":
        parser.error("--force is only valid for build")
    try:
        spec = BuildSpec.create(arguments.platform)
        build = ManagedRuntimeBuild(spec, arguments.stage, arguments.jobs)
        if arguments.operation == "path" and arguments.unchecked:
            print(build.package_dir / "lib")
            return 0
        with lock(build.stage_root / "locks" / f"workflow-{spec.fingerprint}.lock"):
            if arguments.operation == "build":
                path = build.build_source(force=arguments.force)
                checksum = None
                if (build.stage_root / "verified" / f"{spec.fingerprint}.json").is_file():
                    try:
                        build._validate_verification()
                        checksum = spec.artifact_checksum
                    except BuildError:
                        pass
                if checksum is None:
                    checksum = build.checksum()
                print(f"pinned SHA-256 {checksum} for {spec.platform} in scripts/onnxruntime/checksums.toml", file=sys.stderr)
                print(path)
            elif arguments.operation == "fetch":
                print(build.prepare())
            elif arguments.operation == "pin":
                if os.environ.get("CI") == "true":
                    raise BuildError("pinning artifact checksums is a manual operation and is disabled in CI")
                if not build.package_dir.exists():
                    raise BuildError(f"no completed ONNX Runtime package to pin for {spec.configuration['version']} "
                                     f"{spec.platform}; build it with just build-onnxruntime {spec.platform}")
                checksum = build.checksum()
                print(f'["{spec.platform}"]')
                print(f'fingerprint = "{spec.fingerprint}"')
                print(f'sha256 = "{checksum}"')
                print("updated scripts/onnxruntime/checksums.toml", file=sys.stderr)
            elif arguments.operation == "publish":
                if os.environ.get("CI") == "true":
                    raise BuildError("publishing ONNX Runtime is a manual operation and is disabled in CI")
                if not build.package_dir.exists():
                    raise BuildError(f"no completed ONNX Runtime package to publish; build it with just build-onnxruntime {spec.platform}")
                build.validate_package()
                build._validate_verification()
                remote = R2Publisher.configured()
                build.publish(remote)
                print(f"published ONNX Runtime to R2: {remote.bucket}/{remote.key(spec)}")
            elif arguments.operation == "verify":
                build.verify()
                print(f"verified ONNX Runtime {spec.configuration['version']} {spec.platform}")
            else:
                build.validate_package()
                print(build.package_dir / "lib")
    except (BuildError, OSError) as error:
        print(f"ONNX Runtime preparation failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
