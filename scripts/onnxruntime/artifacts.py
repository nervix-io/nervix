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

from scripts.build_onnxruntime import BuildSpec, R2Publisher, RuntimeBuild, lock
from scripts.onnxruntime.toolchain import BuildError


def cache_root() -> Path:
    return Path(os.environ.get("NERVIX_ONNXRUNTIME_DIR") or Path.home() / ".cache/nervix-build/onnxruntime")


def build_spec(platform: str = "native", repo: Path | None = None, *, variant: str = "portable") -> BuildSpec:
    spec = BuildSpec.create(platform, **({"repo": repo} if repo else {}), variant=variant)
    if spec.platform.startswith("darwin/"):
        from scripts.onnxruntime.macos import artifact_spec
        return artifact_spec(spec)
    return spec


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
        if self.spec.cuda_enabled:
            from scripts.onnxruntime.containers import DockerBuilder
            DockerBuilder(self).compile(destination, force=self.force_rebuild)
            return
        from scripts.onnxruntime.macos import DockerBuilder
        DockerBuilder(self).compile(destination, force=self.force_rebuild)

    def checksum(self) -> str:
        if os.environ.get("CI") == "true":
            raise BuildError("pinning artifact checksums is a manual operation and is disabled in CI")
        if not self.spec.cuda_enabled:
            from scripts.onnxruntime.macos import require_qualification
            require_qualification(self)
        return super().checksum()

    def verify(self) -> None:
        if self.spec.cuda_enabled:
            return super().verify()
        self.validate_package()
        self._validate_verification()
        from scripts.onnxruntime.macos import qualification_path, qualify_native
        qualify_native(self, self.archive(), qualification_path(self))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["build", "fetch", "pin", "publish", "path", "verify", "qualify"])
    parser.add_argument("--platform", default="native")
    parser.add_argument("--variant", choices=("portable", "docker"), default="portable")
    parser.add_argument("--stage", type=Path, default=cache_root())
    parser.add_argument("--jobs", type=int, default=min(os.cpu_count() or 1, 4))
    parser.add_argument("--unchecked", action="store_true")
    parser.add_argument("--force", action="store_true", help="rebuild in an empty compiler tree; reuse source and SDK downloads")
    parser.add_argument("--archive", type=Path, help="completed macOS candidate archive for native qualification")
    parser.add_argument("--qualification", type=Path, help="native macOS inference receipt to write or import")
    arguments = parser.parse_args()
    if arguments.jobs < 1:
        parser.error("--jobs must be positive")
    if arguments.force and arguments.operation != "build":
        parser.error("--force is only valid for build")
    try:
        spec = build_spec(arguments.platform, variant=arguments.variant)
        build = ManagedRuntimeBuild(spec, arguments.stage, arguments.jobs)
        if arguments.operation == "path" and arguments.unchecked:
            print(build.package_dir / "lib")
            return 0
        with lock(build.stage_root / "locks" / f"workflow-{spec.fingerprint}.lock"):
            if arguments.operation == "qualify":
                if spec.cuda_enabled or arguments.archive is None or arguments.qualification is None:
                    raise BuildError("qualify requires darwin/arm64, --archive and --qualification")
                from scripts.onnxruntime.macos import qualify_native
                qualify_native(build, arguments.archive, arguments.qualification)
                print(arguments.qualification)
                return 0
            if arguments.qualification is not None:
                if spec.cuda_enabled or arguments.operation not in ("pin", "publish"):
                    raise BuildError("import --qualification only for macOS pinning or publication")
                from scripts.onnxruntime.macos import qualification_path, require_qualification
                destination = qualification_path(build)
                destination.parent.mkdir(parents=True, exist_ok=True)
                if arguments.qualification.resolve() != destination.resolve():
                    shutil.copyfile(arguments.qualification, destination)
                require_qualification(build)
            if arguments.operation == "build":
                path = build.build_source(force=arguments.force)
                if not spec.cuda_enabled:
                    print(f"compiled macOS candidate {build.archive()}; qualify it on ARM64 macOS before pinning", file=sys.stderr)
                    print(path)
                    return 0
                checksum = None
                if (build.stage_root / "verified" / f"{spec.fingerprint}.json").is_file():
                    try:
                        build._validate_verification()
                        checksum = spec.artifact_checksum
                    except BuildError:
                        pass
                if checksum is None:
                    checksum = build.checksum()
                print(f"pinned SHA-256 {checksum} for {spec.artifact_id} in scripts/onnxruntime/checksums.toml", file=sys.stderr)
                print(path)
            elif arguments.operation == "fetch":
                print(build.prepare())
            elif arguments.operation == "pin":
                if os.environ.get("CI") == "true":
                    raise BuildError("pinning artifact checksums is a manual operation and is disabled in CI")
                if not build.package_dir.exists():
                    raise BuildError(f"no completed ONNX Runtime package to pin for {spec.configuration['version']} "
                                     f"{spec.artifact_id}; build it with just build-onnxruntime {spec.platform} "
                                     f"--variant {spec.variant}")
                checksum = build.checksum()
                print(f'["{spec.artifact_id}"]')
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
                if not spec.cuda_enabled:
                    from scripts.onnxruntime.macos import require_qualification
                    require_qualification(build)
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
