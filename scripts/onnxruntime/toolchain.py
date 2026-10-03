"""Resolve LLVM tools for explicit maintainer builds and verification, never artifact fetching."""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
import subprocess


class BuildError(RuntimeError):
    pass


def native_platform() -> str:
    machine = platform.machine().lower()
    machine = {"x86_64": "amd64", "aarch64": "arm64"}.get(machine, machine)
    return f"{platform.system().lower()}/{machine}"


def executable(name: str) -> str:
    path = shutil.which(name)
    if path is None:
        raise BuildError(f"required host build tool is unavailable: {name}")
    return path


def output(command: list[str]) -> str:
    try:
        return subprocess.check_output(command, text=True).strip()
    except (OSError, subprocess.CalledProcessError) as error:
        raise BuildError(f"host tool failed: {command[0]}: {error}") from error


@dataclass(frozen=True)
class Compiler:
    executable: str
    launcher: list[str]
    arguments: list[str]
    version: str

    @classmethod
    def discover(cls, variable: str, default: str, language: str, *, llvm: bool = True) -> Compiler:
        parts = shlex.split(os.environ.get(variable) or default)
        if not parts:
            raise BuildError(f"{variable} must name a host compiler")
        index = next((index for index, part in enumerate(parts)
                      if re.fullmatch(r"(?:clang(?:\+\+)?|gcc|g\+\+|cc|c\+\+|nvcc)(?:-\d+(?:\.\d+)*)?",
                                      Path(part).name)), len(parts) - 1)
        compiler = executable(parts[index])
        version = output([compiler, "--version"])
        if llvm and "clang" not in version.lower():
            raise BuildError(f"{variable} must select LLVM Clang: {compiler}")
        launcher = parts[:index]
        if not launcher:
            launcher = (os.environ.get(f"CMAKE_{language}_COMPILER_LAUNCHER") or "").split(";")
            launcher = [part for part in launcher if part]
        if launcher:
            launcher = [executable(launcher[0]), *launcher[1:]]
        return cls(compiler, launcher, parts[index + 1:], version)

    @property
    def command(self) -> list[str]:
        return [*self.launcher, self.executable, *self.arguments]

    def definitions(self, language: str) -> list[str]:
        definitions = [f"CMAKE_{language}_COMPILER={self.executable}",
                       f"CMAKE_{language}_COMPILER_LAUNCHER={';'.join(self.launcher)}"]
        if language != "CUDA":
            definitions.append(f"CMAKE_{language}_COMPILER_ARG1={shlex.join(self.arguments)}")
        return definitions


@dataclass(frozen=True)
class LinuxTarget:
    platform: str
    sysroot: Path
    gcc_toolchain: Path
    emulator: str | None

    @classmethod
    def discover(cls, target: str, host: str) -> LinuxTarget:
        configured = os.environ.get("ONNXRUNTIME_SYSROOT")
        if not configured:
            raise BuildError("Linux source builds require the target runtime sysroot; "
                             "use just build-onnxruntime or set ONNXRUNTIME_SYSROOT")
        sysroot = Path(configured).resolve()
        if not sysroot.is_dir():
            raise BuildError("ONNXRUNTIME_SYSROOT must name an existing Linux target sysroot")
        gcc_toolchain = Path(os.environ.get("ONNXRUNTIME_GCC_TOOLCHAIN") or sysroot / "usr").resolve()
        if not gcc_toolchain.is_dir():
            raise BuildError("ONNXRUNTIME_GCC_TOOLCHAIN must name the installation containing "
                             "target libstdc++ headers and libgcc")
        emulator = executable(os.environ.get("ONNXRUNTIME_QEMU") or "qemu-aarch64") if target != host else None
        executable("ld.lld")
        return cls(target, sysroot, gcc_toolchain, emulator)

    @property
    def triple(self) -> str:
        return "x86_64-linux-gnu" if self.platform == "linux/amd64" else "aarch64-linux-gnu"

    @property
    def runtime_root(self) -> Path:
        cross = self.sysroot / "usr/aarch64-linux-gnu"
        if self.emulator and (cross / "lib/ld-linux-aarch64.so.1").is_file():
            return cross
        return self.sysroot

    @property
    def compiler_arguments(self) -> list[str]:
        return [f"--target={self.triple}", f"--sysroot={self.sysroot}",
                f"--gcc-toolchain={self.gcc_toolchain}"]

    @property
    def library_directories(self) -> list[Path]:
        root = self.runtime_root
        return [root / "lib" / self.triple, root / "usr/lib" / self.triple,
                root / "lib64", root / "usr/lib64", root / "lib", root / "usr/lib"]

    @property
    def loader(self) -> Path:
        name = "ld-linux-x86-64.so.2" if self.platform == "linux/amd64" else "ld-linux-aarch64.so.1"
        loader = next((directory / name for directory in self.library_directories
                       if (directory / name).is_file()), None)
        if loader is None:
            raise BuildError(f"Linux target sysroot is missing its runtime loader: {name}")
        return loader

    def compiler(self, compiler: Compiler) -> Compiler:
        return replace(compiler, arguments=[*compiler.arguments, *self.compiler_arguments])

    def definitions(self, cuda_home: Path, cudnn_home: Path, cuda_host: Compiler) -> list[str]:
        cuda_target = "targets/x86_64-linux" if self.platform == "linux/amd64" else "targets/sbsa-linux"
        roots = [self.sysroot, cuda_home / cuda_target, cudnn_home]
        definitions = [f"CMAKE_SYSROOT={self.sysroot}",
                f"CMAKE_FIND_ROOT_PATH={';'.join(str(path) for path in roots)}",
                "CMAKE_FIND_ROOT_PATH_MODE_PROGRAM=NEVER", "CMAKE_FIND_ROOT_PATH_MODE_LIBRARY=ONLY",
                "CMAKE_FIND_ROOT_PATH_MODE_INCLUDE=ONLY", "CMAKE_FIND_ROOT_PATH_MODE_PACKAGE=ONLY",
                "CMAKE_LINKER_TYPE=nervix_lld", "CMAKE_C_USING_LINKER_nervix_lld=-fuse-ld=lld",
                "CMAKE_CXX_USING_LINKER_nervix_lld=-fuse-ld=lld",
                # NVIDIA's CMake rules invoke the host compiler directly for the final link.
                "CMAKE_CUDA_USING_LINKER_nervix_lld=" + shlex.join([*cuda_host.arguments, "-fuse-ld=lld"])]
        if self.emulator:
            definitions += ["CMAKE_SYSTEM_NAME=Linux", "CMAKE_SYSTEM_PROCESSOR=aarch64",
                            f"CMAKE_CROSSCOMPILING_EMULATOR={self.emulator};-L;{self.runtime_root}"]
        return definitions


@dataclass(frozen=True)
class HostToolchain:
    cc: Compiler
    cxx: Compiler
    archiver: str
    aggregate: list[str]
    sdk: str | None
    cuda: Compiler | None
    cuda_host: Compiler | None
    cuda_home: Path | None
    cudnn_home: Path | None
    linux_target: LinuxTarget | None
    metadata: dict

    @property
    def cross(self) -> LinuxTarget | None:
        return self.linux_target if self.linux_target and self.linux_target.emulator else None

    def static_runtime(self) -> list[Path]:
        """Carry the producing C++ runtime into the archive instead of requiring its host version."""
        if self.linux_target is None:
            return []
        libraries = []
        for name in ("libstdc++.a", "libgcc.a", "libgcc_eh.a"):
            library = Path(output([*self.cxx.command, f"-print-file-name={name}"])).resolve()
            if not library.is_file() or not library.is_relative_to(self.linux_target.sysroot):
                raise BuildError(f"static C++ runtime must come from the target SDK: {name}: {library}")
            libraries.append(library)
        return libraries

    @classmethod
    def discover(cls, target: str, *, require_cuda: bool = True) -> HostToolchain:
        host = native_platform()
        if os.environ.get("CI") == "true":
            raise BuildError("ONNX Runtime source compilation is disabled in CI")
        if target != host:
            if (host, target) != ("linux/amd64", "linux/arm64"):
                raise BuildError(f"unsupported ONNX Runtime cross-compilation: {host} to {target}; "
                                 "use a matching host or restore its artifact from R2")
        linux_target = (LinuxTarget.discover(target, host) if target.startswith("linux/") and
                        (require_cuda or target != host or os.environ.get("ONNXRUNTIME_SYSROOT")) else None)
        sdk = None
        if target.startswith("darwin/"):
            cc_default = output(["xcrun", "--sdk", "macosx", "--find", "clang"])
            cxx_default = output(["xcrun", "--sdk", "macosx", "--find", "clang++"])
            sdk = output(["xcrun", "--sdk", "macosx", "--show-sdk-path"])
        else:
            cc_default, cxx_default = "clang", "clang++"
        cc = Compiler.discover("CC", cc_default, "C")
        cxx = Compiler.discover("CXX", cxx_default, "CXX")
        if linux_target:
            cc, cxx = linux_target.compiler(cc), linux_target.compiler(cxx)
        if sdk:
            archiver = executable(os.environ.get("AR") or output(["xcrun", "--find", "ar"]))
            aggregate = [output(["xcrun", "--find", "libtool"]), "-static"]
        else:
            suffix = re.search(r"(-\d+)$", Path(cc.executable).name)
            name = "llvm-ar" + (suffix.group(1) if suffix else "")
            archiver = executable(os.environ.get("AR") or str(Path(cc.executable).with_name(name)))
            if "llvm" not in output([archiver, "--version"]).lower():
                raise BuildError(f"AR must select the LLVM archiver: {archiver}")
            aggregate = [archiver, "qcLs"]
        cuda = cuda_host = None
        cuda_home = cudnn_home = None
        if require_cuda and target.startswith("linux/"):
            root = os.environ.get("CUDA_HOME")
            default = str(Path(root) / "bin/nvcc") if root else "nvcc"
            cuda = Compiler.discover("CUDACXX", default, "CUDA", llvm=False)
            if re.search(r"release 13\.", cuda.version) is None:
                raise BuildError("Linux packages require the host CUDA 13 toolkit")
            cuda_host = Compiler.discover("CUDAHOSTCXX", shlex.join(cxx.command), "CXX")
            cuda_home = Path(root).resolve() if root else Path(cuda.executable).resolve().parents[1]
            cudnn_home = Path(os.environ.get("CUDNN_HOME") or "/usr").resolve()
            if target != host:
                if not (cuda_home / "targets/sbsa-linux/include/cuda_runtime.h").is_file():
                    raise BuildError("arm64 CUDA cross-compilation requires the CUDA 13 SBSA SDK "
                                     "under CUDA_HOME/targets/sbsa-linux")
                if not os.environ.get("CUDNN_HOME"):
                    raise BuildError("CUDNN_HOME must select an arm64 cuDNN 9 SDK for cross-compilation")
                cuda = replace(cuda, arguments=[*cuda.arguments, "--target-directory=sbsa-linux"])
            # CXX already carries the target flags when it supplies the default compiler.
            if linux_target and os.environ.get("CUDAHOSTCXX"):
                cuda_host = linux_target.compiler(cuda_host)
        metadata = {
            "platform": host, "cc": asdict(cc), "cxx": asdict(cxx), "archiver": archiver,
            "aggregate": aggregate, "sdk": sdk,
            "cmake": output(["cmake", "--version"]), "ninja": output(["ninja", "--version"]),
            "flags": {name: os.environ.get(name, "") for name in (
                "CFLAGS", "CXXFLAGS", "CPPFLAGS", "LDFLAGS", "ASMFLAGS", "CUDAFLAGS",
            )},
        }
        if sdk:
            metadata["sdk_version"] = output(["xcrun", "--sdk", "macosx", "--show-sdk-version"])
        if image := os.environ.get("ONNXRUNTIME_BUILDER_IMAGE"):
            metadata["builder_image"] = image
        if cuda:
            metadata["cuda"] = asdict(cuda)
            metadata["cuda_host"] = asdict(cuda_host)
            metadata["cuda_home"] = str(cuda_home)
            metadata["cudnn_home"] = str(cudnn_home)
        if linux_target:
            version = re.search(r"version (\d+)\.(\d+)", metadata["cmake"])
            if version is None or tuple(map(int, version.groups())) < (3, 29):
                raise BuildError("Linux source builds require CMake 3.29 or newer for LLVM linker selection")
            metadata["linux_target"] = {"target": target, "sysroot": str(linux_target.sysroot),
                                        "gcc_toolchain": str(linux_target.gcc_toolchain)}
            if linux_target.emulator:
                metadata["linux_target"].update(emulator=linux_target.emulator,
                                               emulator_version=output([linux_target.emulator, "--version"]))
        return cls(cc, cxx, archiver, aggregate, sdk, cuda, cuda_host, cuda_home, cudnn_home, linux_target, metadata)
