"""Prepare missing host build tools and target SDKs in the shared ONNX build cache.

Downloads are checksum pinned, locked, and installed atomically. LLVM must be installed on the
host; explicit compiler and SDK configuration is preserved.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import tarfile
import tempfile
from typing import Callable, Iterator
from urllib.error import URLError
from urllib.request import Request, urlopen
import zipfile

from scripts.build_onnxruntime import atomic_text, file_digest, lock
from scripts.onnxruntime.toolchain import BuildError, Compiler, native_platform, output


DOWNLOADS = Path(__file__).with_name("downloads.json")


def local_llvm(name: str) -> str:
    program = next((path for major in range(23, 19, -1)
                    if (path := shutil.which(f"{name}-{major}"))), None) or shutil.which(name)
    if program is None:
        raise BuildError(f"install local LLVM with {name} or {name}-21 on PATH; LLVM tools are not downloaded")
    return program


def merge_tree(source: Path, destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    for path in source.iterdir():
        target = destination / path.name
        if path.is_dir() and not path.is_symlink() and target.is_dir():
            merge_tree(path, target)
        else:
            if target.exists() or target.is_symlink():
                target.unlink()
            path.rename(target)


def extract_archive(archive: Path, destination: Path) -> None:
    with archive.open("rb") as stream:
        is_deb = stream.read(8) == b"!<arch>\n"
        if is_deb:
            while header := stream.read(60):
                if len(header) != 60 or header[-2:] != b"`\n":
                    raise BuildError("invalid Debian archive header")
                size = int(header[48:58].strip())
                name = header[:16].decode().strip().rstrip("/")
                if name.startswith("data.tar"):
                    with tempfile.TemporaryFile(dir=destination.parent) as data:
                        remaining = size
                        while remaining:
                            chunk = stream.read(min(remaining, 1024 * 1024))
                            if not chunk:
                                raise BuildError("truncated Debian archive")
                            data.write(chunk)
                            remaining -= len(chunk)
                        data.seek(0)
                        with tarfile.open(fileobj=data) as contents:
                            contents.extractall(destination, filter="data")
                    return
                stream.seek(size + size % 2, 1)
            raise BuildError("Debian archive has no data tarball")
    if zipfile.is_zipfile(archive):
        with zipfile.ZipFile(archive) as contents:
            for member in contents.infolist():
                target = destination / member.filename
                if not target.resolve().is_relative_to(destination.resolve()):
                    raise BuildError("ZIP archive path escapes its installation root")
                contents.extract(member, destination)
                if target.is_file():
                    target.chmod((member.external_attr >> 16) & 0o777 or 0o755)
    else:
        with tarfile.open(archive) as contents:
            contents.extractall(destination, filter="data")


class Installer:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.receipts: dict[str, dict] = {}

    def download(self, asset: dict) -> Path:
        checksum = asset["sha256"]
        destination = self.root / "downloads" / checksum
        with lock(self.root / "locks" / f"download-{checksum}.lock"):
            if destination.is_file() and file_digest(destination) == checksum:
                return destination
            destination.parent.mkdir(parents=True, exist_ok=True)
            try:
                with tempfile.TemporaryDirectory(dir=destination.parent) as temporary:
                    staged = Path(temporary) / "download"
                    from tqdm import tqdm

                    with urlopen(Request(asset["url"], headers={"User-Agent": "nervix-build"}), timeout=120) as response:
                        size = int(response.headers.get("Content-Length", "0")) or None
                        with staged.open("wb") as stream, tqdm(total=size, unit="B", unit_scale=True,
                                                               desc=asset["url"].rsplit("/", 1)[-1]) as progress:
                            while chunk := response.read(1024 * 1024):
                                stream.write(chunk)
                                progress.update(len(chunk))
                    if file_digest(staged) != checksum:
                        raise BuildError(f"build tool download checksum mismatch: {asset['url']}")
                    staged.replace(destination)
            except (OSError, URLError) as error:
                raise BuildError(f"build tool download failed: {asset['url']}: {error}") from error
        return destination

    def install(self, name: str, entries: list[dict], required: list[str],
                transform: Callable[[Path], None] | None = None) -> Path:
        identity = {"entries": entries, "required": required}
        digest = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        destination = self.root / f"{name}-{digest[:16]}"
        with lock(self.root / "locks" / f"install-{digest}.lock"):
            try:
                receipt = json.loads((destination / "installation.json").read_text())
                if receipt == identity and all((destination / path).is_file() for path in required):
                    self.receipts[name] = identity
                    return destination
            except (OSError, json.JSONDecodeError):
                pass
            with tempfile.TemporaryDirectory(dir=self.root) as temporary:
                staged = Path(temporary) / "installation"
                staged.mkdir()
                licenses = []
                for entry in entries:
                    archive = self.download(entry)
                    with tempfile.TemporaryDirectory(dir=temporary) as extracted:
                        source = Path(extracted)
                        try:
                            extract_archive(archive, source)
                        except (OSError, ValueError, tarfile.TarError, zipfile.BadZipFile) as error:
                            raise BuildError(f"cannot unpack build tool: {entry['url']}: {error}") from error
                        if entry["strip_root"]:
                            children = list(source.iterdir())
                            if len(children) != 1 or not children[0].is_dir():
                                raise BuildError(f"build tool archive must contain one directory: {entry['url']}")
                            source = children[0]
                        for filename in ("LICENSE", "LICENSE.txt", "EULA.txt"):
                            license_file = source / filename
                            if license_file.is_file():
                                licenses.append(entry["url"] + "\n" + license_file.read_text())
                        merge_tree(source, staged / entry.get("destination", "."))
                if transform:
                    transform(staged)
                missing = [path for path in required if not (staged / path).is_file()]
                if missing:
                    raise BuildError(f"downloaded {name} is missing required build tools or SDK files: {', '.join(missing)}")
                if licenses:
                    (staged / "LICENSE.txt").unlink(missing_ok=True)
                    (staged / "LICENSE.txt").write_text("\n\n".join(licenses))
                atomic_text(staged / "installation.json", json.dumps(identity, sort_keys=True) + "\n")
                if destination.exists():
                    shutil.rmtree(destination)
                staged.rename(destination)
            self.receipts[name] = identity
        return destination


class Bootstrap:
    def __init__(self, stage_root: Path, target: str) -> None:
        self.target = target
        self.host = native_platform()
        self.installer = Installer(stage_root / "tools")
        self.catalog = json.loads(DOWNLOADS.read_text())

    def add_path(self, directory: Path) -> None:
        os.environ["PATH"] = str(directory) + os.pathsep + os.environ.get("PATH", "")

    def alias(self, name: str, program: str) -> None:
        digest = hashlib.sha256(f"{name}:{program}".encode()).hexdigest()
        directory = self.installer.root / "aliases" / digest[:16]
        with lock(self.installer.root / "locks" / f"aliases-{digest}.lock"):
            directory.mkdir(parents=True, exist_ok=True)
            alias = directory / name
            if not alias.is_symlink() or alias.resolve() != Path(program).resolve():
                alias.unlink(missing_ok=True)
                alias.symlink_to(program)
        self.add_path(directory)

    def objdump(self) -> None:
        if self.target.startswith("linux/") and not shutil.which("objdump"):
            self.alias("objdump", local_llvm("llvm-objdump"))

    def program(self, name: str, minimum: tuple[int, int] | None = None) -> None:
        available = shutil.which(name)
        if available:
            version = output([available, "--version"])
            match = re.search(r"(\d+)\.(\d+)", version)
            if minimum is None or match and tuple(map(int, match.groups())) >= minimum:
                return
        binary = "CMake.app/Contents/bin/cmake" if name == "cmake" and self.host.startswith("darwin/") else name
        if name == "cmake" and not self.host.startswith("darwin/"):
            binary = "bin/cmake"
        directory = self.installer.install(name, [self.catalog[name][self.host]], [binary])
        self.add_path((directory / binary).parent)

    def compiler(self, variable: str, default: str, language: str, *, llvm: bool = True) -> Compiler:
        compiler = replace(Compiler.discover(variable, default, language, llvm=llvm), launcher=[])
        os.environ[variable] = shlex.join(compiler.command)
        return compiler

    def compilers(self) -> Compiler:
        if self.host.startswith("darwin/"):
            if not shutil.which("xcrun"):
                raise BuildError("macOS source builds require the Xcode command line tools and SDK")
            defaults = [output(["xcrun", "--sdk", "macosx", "--find", name]) for name in ("clang", "clang++")]
        else:
            defaults = []
            for variable, name in (("CC", "clang"), ("CXX", "clang++")):
                if os.environ.get(variable):
                    defaults.append(name)
                    continue
                defaults.append(local_llvm(name))
        cc = self.compiler("CC", defaults[0], "C")
        cxx = self.compiler("CXX", defaults[1], "CXX")
        if not self.host.startswith("darwin/") and not os.environ.get("AR"):
            suffix = re.search(r"(-\d+)$", Path(cc.executable).name)
            sibling = Path(cc.executable).with_name("llvm-ar" + (suffix.group(1) if suffix else ""))
            major = re.search(r"clang version (\d+)", cc.version)
            available = str(sibling) if sibling.is_file() else shutil.which("llvm-ar")
            if not available and major:
                available = shutil.which(f"llvm-ar-{major.group(1)}")
            os.environ["AR"] = available or local_llvm("llvm-ar")
        return cxx

    def cross(self) -> None:
        if self.target == self.host:
            return
        if (self.host, self.target) != ("linux/amd64", "linux/arm64"):
            raise BuildError(f"unsupported ONNX Runtime cross-compilation: {self.host} to {self.target}")
        if not shutil.which("ld.lld"):
            self.alias("ld.lld", local_llvm("ld.lld"))
        if not os.environ.get("ONNXRUNTIME_SYSROOT"):
            if Path("/usr/aarch64-linux-gnu/include/stdio.h").is_file():
                os.environ["ONNXRUNTIME_SYSROOT"] = "/"
            else:
                root = self.installer.install("arm64-sysroot", self.catalog["arm64_sysroot"],
                                              ["usr/aarch64-linux-gnu/include/stdio.h",
                                               "usr/aarch64-linux-gnu/lib/ld-linux-aarch64.so.1",
                                               "usr/lib/gcc-cross/aarch64-linux-gnu/14/libgcc.a"])
                os.environ["ONNXRUNTIME_SYSROOT"] = str(root)
                if not os.environ.get("ONNXRUNTIME_GCC_TOOLCHAIN"):
                    os.environ["ONNXRUNTIME_GCC_TOOLCHAIN"] = str(root / "usr")
        if not os.environ.get("ONNXRUNTIME_QEMU"):
            available = shutil.which("qemu-aarch64")
            if not available:
                root = self.installer.install("qemu", [self.catalog["qemu"][self.host]], ["usr/bin/qemu-aarch64"])
                available = str(root / "usr/bin/qemu-aarch64")
            os.environ["ONNXRUNTIME_QEMU"] = available

    def cuda_host(self, cxx: Compiler, maximum: int) -> None:
        if os.environ.get("CUDAHOSTCXX"):
            self.compiler("CUDAHOSTCXX", "", "CXX")
            return
        major = re.search(r"clang version (\d+)", cxx.version)
        if major and int(major.group(1)) <= maximum:
            os.environ["CUDAHOSTCXX"] = shlex.join([cxx.executable, *cxx.arguments])
            return
        compiler = shutil.which(f"clang++-{maximum}")
        if compiler is None:
            raise BuildError(f"install local clang-{maximum} with clang++-{maximum} on PATH for CUDA host compilation, "
                             "or set CUDAHOSTCXX to a supported local LLVM compiler")
        os.environ["CUDAHOSTCXX"] = shlex.join([compiler, *cxx.arguments])

    def cuda(self, cxx: Compiler) -> None:
        if not self.target.startswith("linux/"):
            return
        cuda_root = os.environ.get("CUDA_HOME")
        maximum = 21
        if not cuda_root and not os.environ.get("CUDACXX"):
            compiler = shutil.which("nvcc")
            if compiler and re.search(r"release 13\.", output([compiler, "--version"])):
                root = Path(compiler).resolve().parents[1]
                if self.target == self.host or (root / "targets/sbsa-linux/include/cuda_runtime.h").is_file():
                    cuda_root = str(root)
        if cuda_root or os.environ.get("CUDACXX"):
            default = str(Path(cuda_root) / "bin/nvcc") if cuda_root else "nvcc"
            compiler = self.compiler("CUDACXX", default, "CUDA", llvm=False)
            release = re.search(r"release 13\.(\d+)", compiler.version)
            if release is None:
                raise BuildError("Linux packages require a CUDA 13 toolkit")
            maximum = 20 if int(release.group(1)) < 2 else 21
            cuda_root = cuda_root or str(Path(compiler.executable).resolve().parents[1])
            if self.target != self.host and not (Path(cuda_root) / "targets/sbsa-linux/include/cuda_runtime.h").is_file():
                raise BuildError("the configured CUDA toolkit needs the arm64 SDK under targets/sbsa-linux")
        self.cuda_host(cxx, maximum)
        if not cuda_root and not os.environ.get("CUDACXX"):
            target_dir = "targets/sbsa-linux" if self.target == "linux/arm64" else "targets/x86_64-linux"
            entries = [self.catalog["cuda"][component][self.host] for component in ("cuda_nvcc", "libnvvm")]
            entries.append(self.catalog["cuda"]["cuda_crt"][self.target])
            entries += [{**assets[self.target], "destination": target_dir}
                        for component, assets in self.catalog["cuda"].items()
                        if component not in ("cuda_nvcc", "libnvvm", "cuda_crt")]

            def layout(root: Path) -> None:
                include = root / "include"
                if include.is_dir():
                    merge_tree(include, root / target_dir / "include")
                    include.rmdir()
                include.symlink_to(target_dir + "/include", target_is_directory=True)
                (root / "lib64").symlink_to(target_dir + "/lib", target_is_directory=True)

            # ONNX's CUDA provider includes SDK headers from ordinary C++ as well as NVCC
            # sources. A usable compiler and libcudart alone do not make this SDK complete.
            required = ["bin/nvcc", "nvvm/bin/cicc", "nvvm/libdevice/libdevice.10.bc"]
            required += [f"{target_dir}/include/{header}" for header in (
                "cuda.h", "cuda_runtime.h", "cuda_fp16.h", "cuda_bf16.h", "cuda_fp8.h", "cuda_fp4.h",
                "crt/host_config.h", "cccl/cuda/std/utility", "cublas_v2.h", "cublasLt.h", "cusparse.h",
                "curand.h", "curand_kernel.h", "cufft.h", "cufftXt.h", "nvrtc.h", "nvJitLink.h",
            )]
            required += [f"{target_dir}/lib/{library}" for library in (
                "libcudart.so", "libcublas.so", "libcublasLt.so", "libcusparse.so", "libcurand.so", "libcufft.so",
                "libnvrtc.so", "libnvrtc-builtins.so", "libnvJitLink.so", "libculibos.a", "stubs/libcuda.so",
            )]
            root = self.installer.install(f"cuda-{self.host.replace('/', '-')}-to-{self.target.replace('/', '-')}",
                                          entries, required, layout)
            cuda_root = str(root)
        if cuda_root:
            os.environ["CUDA_HOME"] = cuda_root
            if not os.environ.get("CUDACXX"):
                os.environ["CUDACXX"] = str(Path(cuda_root) / "bin/nvcc")
        if not os.environ.get("CUDNN_HOME"):
            system_libraries = ("lib/libcudnn.so.9", "lib64/libcudnn.so.9",
                                "lib/x86_64-linux-gnu/libcudnn.so.9", "lib/aarch64-linux-gnu/libcudnn.so.9")
            if self.target == self.host and Path("/usr/include/cudnn.h").is_file() and any(
                (Path("/usr") / name).is_file() for name in system_libraries
            ):
                os.environ["CUDNN_HOME"] = "/usr"
            else:
                root = self.installer.install("cudnn-" + self.target.replace("/", "-"),
                                              [self.catalog["cudnn"][self.target]],
                                              ["include/cudnn.h", "lib/libcudnn.so.9"])
                os.environ["CUDNN_HOME"] = str(root)

    @contextmanager
    def environment(self) -> Iterator[None]:
        if os.environ.get("CI") == "true":
            raise BuildError("ONNX Runtime source compilation is disabled in CI")
        if self.host not in self.catalog["cmake"]:
            raise BuildError(f"unsupported ONNX Runtime build host: {self.host}")
        if self.target != self.host and (self.host, self.target) != ("linux/amd64", "linux/arm64"):
            raise BuildError(f"unsupported ONNX Runtime cross-compilation: {self.host} to {self.target}")
        original = dict(os.environ)
        launchers = tuple(f"CMAKE_{language}_COMPILER_LAUNCHER" for language in ("C", "CXX", "CUDA", "ASM"))
        try:
            # Source builds invoke LLVM and NVCC directly. Compiler-cache setup belongs to the
            # host; never pass a wrapper prefix through CUDAHOSTCXX to CMake's NVCC probe.
            for name in launchers:
                os.environ[name] = ""
            if not shutil.which("git"):
                raise BuildError("ONNX Runtime source builds require Git on the host")
            cxx = self.compilers()
            self.program("cmake", (3, 29))
            self.program("ninja")
            self.objdump()
            self.cross()
            self.cuda(cxx)
            yield
        finally:
            for name in ("PATH", "CC", "CXX", "AR", "CUDA_HOME", "CUDNN_HOME", "CUDACXX", "CUDAHOSTCXX",
                         "ONNXRUNTIME_SYSROOT", "ONNXRUNTIME_GCC_TOOLCHAIN", "ONNXRUNTIME_QEMU", *launchers):
                if name in original:
                    os.environ[name] = original[name]
                else:
                    os.environ.pop(name, None)
