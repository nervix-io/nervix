from __future__ import annotations

from dataclasses import replace
import os
from pathlib import Path
from shutil import which
import subprocess
import sys
import tempfile
from textwrap import dedent
import unittest
from unittest.mock import Mock, patch

from scripts.build_onnxruntime import BuildSpec, RuntimeBuild
from scripts.onnxruntime.toolchain import BuildError, Compiler, HostToolchain, LinuxTarget
from scripts.tests.test_build_onnxruntime import construct_cuda_runtime


ROOT = Path(__file__).resolve().parents[2]
CLANG_CXX = which("clang++-23") or which("clang++-22") or which("clang++-21") or which("clang++")


def tool_output(command: list[str], **kwargs: object) -> str:
    if command[0] == "xcrun":
        if command[-1] == "--show-sdk-path":
            return "/Xcode/SDK"
        if command[-1] == "--show-sdk-version":
            return "15.0"
        return f"/Xcode/bin/{command[-1]}"
    name = Path(command[0]).name
    if name.startswith("clang"):
        return "clang version 22.1.0"
    if name.startswith("llvm-ar"):
        return "LLVM version 22.1.0"
    if name == "nvcc":
        return "Cuda compilation tools, release 13.1, V13.1.80"
    if name == "gcc":
        return "gcc version 14.0"
    return f"{name} version 4.2.3"


class NativeToolchainTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.sysroot = Path(temporary.name) / "sysroot"
        (self.sysroot / "usr").mkdir(parents=True)
        for mocked in (
            patch.dict(os.environ, {name: "" for name in (
                "CI", "CC", "CXX", "AR", "CUDA_HOME", "CUDACXX", "CUDAHOSTCXX", "CUDNN_HOME",
                "CMAKE_C_COMPILER_LAUNCHER", "CMAKE_CXX_COMPILER_LAUNCHER", "CMAKE_CUDA_COMPILER_LAUNCHER",
                "ONNXRUNTIME_SYSROOT", "ONNXRUNTIME_GCC_TOOLCHAIN", "ONNXRUNTIME_QEMU", "ONNXRUNTIME_BUILDER_IMAGE",
            )}),
            patch("scripts.onnxruntime.toolchain.shutil.which",
                  side_effect=lambda name: name if name.startswith("/") else f"/host/bin/{name}"),
            patch("scripts.onnxruntime.toolchain.subprocess.check_output", side_effect=tool_output),
            patch("scripts.onnxruntime.toolchain.platform.system", return_value="Linux"),
            patch("scripts.onnxruntime.toolchain.platform.machine", return_value="x86_64"),
        ):
            mocked.start()
            self.addCleanup(mocked.stop)
        self.enterContext(patch.dict(os.environ, {"ONNXRUNTIME_SYSROOT": str(self.sysroot),
                                                 "ONNXRUNTIME_GCC_TOOLCHAIN": str(self.sysroot / "usr")}))

    def target_arguments(self) -> list[str]:
        return ["--target=x86_64-linux-gnu", f"--sysroot={self.sysroot}",
                f"--gcc-toolchain={self.sysroot / 'usr'}"]

    def test_runtime_loader_uses_the_selected_target_with_cross_packages_installed(self) -> None:
        cross = self.sysroot / "usr/aarch64-linux-gnu"
        for directory, name in ((self.sysroot / "lib/x86_64-linux-gnu", "ld-linux-x86-64.so.2"),
                                (self.sysroot / "lib/aarch64-linux-gnu", "ld-linux-aarch64.so.1"),
                                (cross / "lib", "ld-linux-aarch64.so.1")):
            directory.mkdir(parents=True)
            (directory / name).write_bytes(b"target loader")
        for platform, emulator, expected in (("linux/amd64", None, self.sysroot),
                                             ("linux/arm64", None, self.sysroot),
                                             ("linux/arm64", "qemu-aarch64", cross)):
            with self.subTest(platform=platform, emulator=emulator):
                target = LinuxTarget(platform, self.sysroot, self.sysroot / "usr", emulator)
                self.assertEqual(target.runtime_root, expected)
                self.assertTrue(target.loader.is_relative_to(expected))

    def test_static_cpp_runtime_is_taken_from_the_target_sdk(self) -> None:
        tools = HostToolchain.discover("linux/amd64")
        archives = [self.sysroot / "usr/lib" / name for name in ("libstdc++.a", "libgcc.a", "libgcc_eh.a")]
        for path in archives:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"!<arch>\n")
        with patch("scripts.onnxruntime.toolchain.output", side_effect=[str(path) for path in archives]):
            self.assertEqual(tools.static_runtime(), archives)
        with patch("scripts.onnxruntime.toolchain.output", return_value="/host/libstdc++.a"):
            with self.assertRaisesRegex(BuildError, "target SDK"):
                tools.static_runtime()

    def test_host_compiler_commands_preserve_the_kache_launcher(self) -> None:
        with patch.dict(os.environ, {"CC": "kache clang-22", "CXX": "kache clang++-22 -fno-omit-frame-pointer"}):
            tools = HostToolchain.discover("linux/amd64")
        self.assertEqual(tools.cc.command, ["/host/bin/kache", "/host/bin/clang-22", *self.target_arguments()])
        self.assertEqual(tools.cxx.command, ["/host/bin/kache", "/host/bin/clang++-22", "-fno-omit-frame-pointer",
                                           *self.target_arguments()])
        self.assertIn("CMAKE_CXX_COMPILER_LAUNCHER=/host/bin/kache", tools.cxx.definitions("CXX"))
        self.assertIn("CMAKE_CXX_COMPILER_ARG1=" + " ".join(["-fno-omit-frame-pointer", *self.target_arguments()]),
                      tools.cxx.definitions("CXX"))
        self.assertEqual(tools.aggregate, ["/host/bin/llvm-ar-22", "qcLs"])

    def test_cmake_launcher_configuration_is_used_for_an_unwrapped_compiler(self) -> None:
        with patch.dict(os.environ, {"CMAKE_CXX_COMPILER_LAUNCHER": "kache;--local"}):
            compiler = Compiler.discover("CXX", "clang++", "CXX")
        self.assertEqual(compiler.command, ["/host/bin/kache", "--local", "/host/bin/clang++"])

    def test_compiler_selection_requires_llvm(self) -> None:
        with patch.dict(os.environ, {"CC": "gcc"}):
            with self.assertRaisesRegex(BuildError, "CC must select LLVM Clang"):
                HostToolchain.discover("linux/amd64")

    def test_native_linux_compilers_use_the_target_runtime_sysroot(self) -> None:
        for machine, target, triple in (("x86_64", "linux/amd64", "x86_64-linux-gnu"),
                                        ("aarch64", "linux/arm64", "aarch64-linux-gnu")):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                sysroot, gcc = root / "sysroot", root / "gcc"
                sysroot.mkdir()
                gcc.mkdir()
                environment = {"ONNXRUNTIME_SYSROOT": str(sysroot), "ONNXRUNTIME_GCC_TOOLCHAIN": str(gcc),
                               "CC": "clang-23", "CXX": "clang++-23", "CUDAHOSTCXX": "clang++-21"}
                with patch.dict(os.environ, environment), \
                        patch("scripts.onnxruntime.toolchain.platform.machine", return_value=machine):
                    tools = HostToolchain.discover(target)
                for compiler in (tools.cc, tools.cxx, tools.cuda_host):
                    self.assertIn(f"--sysroot={sysroot}", compiler.arguments)
                    self.assertIn(f"--gcc-toolchain={gcc}", compiler.arguments)
                    self.assertIn(f"--target={triple}", compiler.arguments)
                build = RuntimeBuild(BuildSpec.create(target, repo=ROOT), root)
                build.tools = tools
                with patch("scripts.build_onnxruntime.run", side_effect=BuildError("configuration captured")) as command:
                    with self.assertRaisesRegex(BuildError, "configuration captured"):
                        build.compile(root / "package")
                arguments = command.call_args.args[0]
                self.assertIn(f"-DCMAKE_SYSROOT={sysroot}", arguments)
                for kind in ("SHARED", "MODULE"):
                    self.assertIn(f"-DCMAKE_{kind}_LINKER_FLAGS=-static-libstdc++ -static-libgcc "
                                  "-Wl,--exclude-libs,ALL", arguments)
                cuda_flags = next(arg for arg in arguments if arg.startswith("-DCMAKE_CUDA_FLAGS="))
                self.assertIn(f"-Xcompiler=--sysroot={sysroot}", cuda_flags)

    def cross_sdk(self, root: Path) -> dict[str, str]:
        sysroot = root / "sysroot"
        gcc = root / "gcc"
        cuda = root / "cuda"
        cudnn = root / "cudnn"
        for path in (sysroot, gcc, cuda / "targets/sbsa-linux/include", cudnn / "lib"):
            path.mkdir(parents=True)
        (cuda / "targets/sbsa-linux/include/cuda_runtime.h").write_text("CUDA header")
        driver = cuda / "targets/sbsa-linux/lib/stubs/libcuda.so"
        driver.parent.mkdir(parents=True)
        driver.write_bytes(b"target driver stub")
        return {"ONNXRUNTIME_SYSROOT": str(sysroot), "ONNXRUNTIME_GCC_TOOLCHAIN": str(gcc),
                "CUDA_HOME": str(cuda), "CUDNN_HOME": str(cudnn)}

    def test_arm64_cross_compilation_preserves_launchers_and_targets_every_compiler(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            environment = self.cross_sdk(Path(temporary))
            environment.update({"CC": "kache clang-22", "CXX": "kache clang++-22",
                                "CUDACXX": "kache nvcc", "CUDAHOSTCXX": "clang++-21 -fno-omit-frame-pointer"})
            with patch.dict(os.environ, environment):
                tools = HostToolchain.discover("linux/arm64")
            for compiler in (tools.cc, tools.cxx, tools.cuda_host):
                self.assertIn("--target=aarch64-linux-gnu", compiler.arguments)
                self.assertIn(f"--sysroot={environment['ONNXRUNTIME_SYSROOT']}", compiler.arguments)
                self.assertIn(f"--gcc-toolchain={environment['ONNXRUNTIME_GCC_TOOLCHAIN']}", compiler.arguments)
            self.assertEqual(tools.cc.launcher, ["/host/bin/kache"])
            self.assertEqual(tools.cxx.launcher, ["/host/bin/kache"])
            self.assertEqual(tools.cuda.launcher, ["/host/bin/kache"])
            self.assertIn("--target-directory=sbsa-linux", tools.cuda.arguments)
            self.assertIn("-fno-omit-frame-pointer", tools.cuda_host.arguments)
            self.assertEqual(tools.metadata["platform"], "linux/amd64")
            self.assertEqual(tools.metadata["linux_target"]["target"], "linux/arm64")

    def test_arm64_cross_compile_configures_cmake_for_target_libraries_and_host_tools(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            environment = self.cross_sdk(Path(temporary))
            with patch.dict(os.environ, environment):
                tools = HostToolchain.discover("linux/arm64")
            build = RuntimeBuild(BuildSpec.create("linux/arm64", repo=ROOT), Path(temporary))
            build.tools = tools
            with patch("scripts.build_onnxruntime.run", side_effect=BuildError("configuration captured")) as command:
                with self.assertRaisesRegex(BuildError, "configuration captured"):
                    build.compile(Path(temporary) / "package")
            arguments = command.call_args.args[0]
            for definition in ("CMAKE_SYSTEM_NAME=Linux", "CMAKE_SYSTEM_PROCESSOR=aarch64",
                               "CMAKE_FIND_ROOT_PATH_MODE_PROGRAM=NEVER", "CMAKE_FIND_ROOT_PATH_MODE_LIBRARY=ONLY",
                               "CMAKE_FIND_ROOT_PATH_MODE_INCLUDE=ONLY"):
                self.assertIn(f"-D{definition}", arguments)
            self.assertIn(f"-DCMAKE_SYSROOT={environment['ONNXRUNTIME_SYSROOT']}", arguments)
            assembler = next(argument for argument in arguments if argument.startswith("-DCMAKE_ASM_COMPILER_ARG1="))
            self.assertIn("--target=aarch64-linux-gnu", assembler)
            self.assertIn("-DCMAKE_ASM_COMPILER_LAUNCHER=" + ";".join(tools.cc.launcher), arguments)
            self.assertIn("-DCMAKE_LINKER_TYPE=nervix_lld", arguments)
            cuda_link = next(argument for argument in arguments if argument.startswith("-DCMAKE_CUDA_USING_LINKER_nervix_lld="))
            self.assertIn("--target=aarch64-linux-gnu", cuda_link)
            self.assertIn("-fuse-ld=lld", cuda_link)
            cuda_flags = next(argument for argument in arguments if argument.startswith("-DCMAKE_CUDA_FLAGS="))
            self.assertIn("--target-directory=sbsa-linux", cuda_flags)
            self.assertIn("-Xcompiler=--target=aarch64-linux-gnu", cuda_flags)

    def test_arm64_cross_smoke_links_for_arm64_and_runs_through_qemu(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            environment = self.cross_sdk(Path(temporary))
            with patch.dict(os.environ, environment):
                tools = HostToolchain.discover("linux/arm64")
            build = RuntimeBuild(BuildSpec.create("linux/arm64", repo=ROOT), Path(temporary))
            destination = Path(temporary) / "package"
            construct_cuda_runtime(destination)
            with patch("scripts.build_onnxruntime.run") as command:
                build._smoke(destination, tools)
            compile = command.call_args_list[-2].args[0]
            self.assertIn("--target=aarch64-linux-gnu", compile)
            self.assertIn("-fuse-ld=lld", compile)
            execution = command.call_args.args[0]
            self.assertEqual(execution[:3], ["/host/bin/qemu-aarch64", "-L", environment["ONNXRUNTIME_SYSROOT"]])
            self.assertIn(f"LD_LIBRARY_PATH={destination}/runtime/lib", execution[4])
            self.assertEqual(execution[-1], "cuda-load")

    @unittest.skipUnless(sys.platform == "linux" and CLANG_CXX, "requires Linux and Clang")
    def test_cross_provider_loading_resolves_only_the_sdk_driver_stub(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            environment = self.cross_sdk(root)
            with patch.dict(os.environ, environment):
                tools = HostToolchain.discover("linux/arm64")
            build = RuntimeBuild(BuildSpec.create("linux/arm64", repo=ROOT), root)
            destination = root / "package"
            construct_cuda_runtime(destination)
            driver = Path(environment["CUDA_HOME"]) / "targets/sbsa-linux/lib/stubs/libcuda.so"
            (driver.parent / "libcudart.so").write_text("another SDK stub")
            driver_source = root / "driver.cc"
            driver_source.write_text('extern "C" int nervix_driver_fixture() { return 47; }')
            provider_source = root / "provider.cc"
            provider_source.write_text('extern "C" int nervix_driver_fixture();\n'
                                       'extern "C" int probe() { return nervix_driver_fixture(); }')
            probe_source = root / "probe.cc"
            probe_source.write_text(dedent("""\
                #include <dlfcn.h>
                #include <cstdio>
                int main(int argc, char** argv) {
                  void* provider = dlopen(argv[1], RTLD_NOW);
                  if (!provider) { std::fprintf(stderr, "%s\\n", dlerror()); return 1; }
                  auto probe = reinterpret_cast<int (*)()>(dlsym(provider, "probe"));
                  return probe && probe() == 47 ? 0 : 2;
                }
            """))
            provider = destination / "runtime/lib/libonnxruntime_providers_cuda.so"
            probe = root / "loader-probe"
            for command in (
                [CLANG_CXX, "-shared", "-fPIC", str(driver_source), "-Wl,-soname,libcuda.so.1", "-o", str(driver)],
                [CLANG_CXX, "-shared", "-fPIC", str(provider_source), str(driver), "-o", str(provider)],
                [CLANG_CXX, str(probe_source), "-ldl", "-o", str(probe)],
            ):
                subprocess.run(command, check=True, capture_output=True, text=True)

            def run_smoke(command: list[str], **kwargs: object) -> None:
                if command[0] != tools.cross.emulator:
                    return
                self.assertEqual(command[-1], "cuda-load")
                # Exercise the guest loader environment using native ELF fixtures, without
                # requiring QEMU or an installed target SDK in the tooling test suite.
                guest = kwargs["env"].copy()
                guest["LD_LIBRARY_PATH"] = command[4].split("=", 1)[1]
                result = subprocess.run([str(probe), str(provider)], env=guest, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                directories = [Path(path) for path in guest["LD_LIBRARY_PATH"].split(os.pathsep)]
                resolved = next(path / "libcuda.so.1" for path in directories if (path / "libcuda.so.1").is_file())
                self.assertEqual(resolved.resolve(), driver)
                self.assertEqual([path.name for path in resolved.parent.iterdir()], ["libcuda.so.1"])
                self.assertEqual(kwargs["env"].get("LD_LIBRARY_PATH"), os.environ.get("LD_LIBRARY_PATH"))

            with patch("scripts.build_onnxruntime.run", side_effect=run_smoke):
                build._smoke(destination, tools)
            self.assertEqual({path.name for path in (destination / "runtime/lib").iterdir()},
                             {"libonnxruntime_providers_shared.so", "libonnxruntime_providers_cuda.so",
                              "libcudnn_graph.so.9", "libnvrtc-builtins.so.13.2"})

    def test_cross_sdk_requires_an_existing_sysroot(self) -> None:
        with patch.dict(os.environ, {"ONNXRUNTIME_SYSROOT": "/unavailable-arm64-sysroot"}):
            with patch("scripts.onnxruntime.toolchain.Compiler.discover") as compiler:
                with self.assertRaisesRegex(BuildError, "ONNXRUNTIME_SYSROOT"):
                    HostToolchain.discover("linux/arm64")
        compiler.assert_not_called()

    def test_cross_sdk_requires_sbsa_cuda_headers(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            environment = self.cross_sdk(Path(temporary))
            (Path(environment["CUDA_HOME"]) / "targets/sbsa-linux/include/cuda_runtime.h").unlink()
            with patch.dict(os.environ, environment):
                with self.assertRaisesRegex(BuildError, "CUDA 13 SBSA SDK"):
                    HostToolchain.discover("linux/arm64")

    def test_cross_sdk_requires_the_arm64_cudnn_root(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            environment = self.cross_sdk(Path(temporary))
            environment["CUDNN_HOME"] = ""
            with patch.dict(os.environ, environment):
                with self.assertRaisesRegex(BuildError, "CUDNN_HOME must select an arm64"):
                    HostToolchain.discover("linux/arm64")

    def test_cross_cmake_requires_llvm_linker_selection_support(self) -> None:
        def version(command: list[str], **kwargs: object) -> str:
            return "cmake version 3.28.0" if command[0] == "cmake" else tool_output(command)

        with tempfile.TemporaryDirectory() as temporary:
            with patch.dict(os.environ, self.cross_sdk(Path(temporary))):
                with patch("scripts.onnxruntime.toolchain.subprocess.check_output", side_effect=version):
                    with self.assertRaisesRegex(BuildError, "CMake 3.29"):
                        HostToolchain.discover("linux/arm64")

    def test_cross_sdk_supports_debian_cross_package_loader_layout(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            environment = self.cross_sdk(Path(temporary))
            loader = Path(environment["ONNXRUNTIME_SYSROOT"]) / "usr/aarch64-linux-gnu/lib/ld-linux-aarch64.so.1"
            loader.parent.mkdir(parents=True)
            loader.write_bytes(b"target loader")
            with patch.dict(os.environ, environment):
                tools = HostToolchain.discover("linux/arm64")
            self.assertEqual(tools.cross.runtime_root, loader.parent.parent)
            self.assertIn(f"CMAKE_CROSSCOMPILING_EMULATOR=/host/bin/qemu-aarch64;-L;{loader.parent.parent}",
                          tools.cross.definitions(tools.cuda_home, tools.cudnn_home, tools.cuda_host))

    def test_ci_does_not_resolve_a_compilation_toolchain(self) -> None:
        with patch.dict(os.environ, {"CI": "true"}):
            with patch("scripts.onnxruntime.toolchain.Compiler.discover") as compiler:
                for target in ("linux/amd64", "linux/arm64"):
                    with self.subTest(target=target), self.assertRaisesRegex(BuildError, "compilation is disabled in CI"):
                        HostToolchain.discover(target)
        compiler.assert_not_called()

    def test_macos_uses_the_apple_sdk_and_host_archive_tools(self) -> None:
        with patch("scripts.onnxruntime.toolchain.platform.system", return_value="Darwin"):
            with patch("scripts.onnxruntime.toolchain.platform.machine", return_value="arm64"):
                tools = HostToolchain.discover("darwin/arm64")
        self.assertEqual(tools.cc.executable, "/Xcode/bin/clang")
        self.assertEqual(tools.cxx.executable, "/Xcode/bin/clang++")
        self.assertEqual(tools.sdk, "/Xcode/SDK")
        self.assertEqual(tools.aggregate, ["/Xcode/bin/libtool", "-static"])
        self.assertEqual(tools.metadata["sdk_version"], "15.0")

    def test_cuda_uses_the_host_toolkit_and_llvm_host_compiler(self) -> None:
        with patch.dict(os.environ, {"CC": "kache clang-22", "CXX": "kache clang++-22",
                                   "CUDA_HOME": "/native/cuda", "CUDNN_HOME": "/native/cudnn",
                                   "CUDACXX": "kache /native/cuda/bin/nvcc", "CUDAHOSTCXX": "clang++-20"}):
            tools = HostToolchain.discover("linux/amd64")
        self.assertEqual(tools.cuda.command, ["/host/bin/kache", "/native/cuda/bin/nvcc"])
        self.assertEqual(tools.cuda_host.executable, "/host/bin/clang++-20")
        self.assertEqual(tools.cuda_home, Path("/native/cuda"))
        self.assertEqual(tools.cudnn_home, Path("/native/cudnn"))

    def test_linux_arm64_uses_the_cuda_toolkit(self) -> None:
        with patch("scripts.onnxruntime.toolchain.platform.machine", return_value="aarch64"):
            with patch.dict(os.environ, {"CUDA_HOME": "/native/cuda", "CUDAHOSTCXX": "clang++-21"}):
                tools = HostToolchain.discover("linux/arm64")
        self.assertEqual(tools.cuda.executable, "/native/cuda/bin/nvcc")
        self.assertEqual(tools.cuda_host.executable, "/host/bin/clang++-21")

    def test_artifact_key_is_shared_across_host_toolchains(self) -> None:
        with patch.dict(os.environ, {"CC": "kache clang-22", "CXX": "kache clang++-22"}):
            first = BuildSpec.create("linux/amd64", repo=ROOT)
        with patch.dict(os.environ, {"CC": "clang-23", "CXX": "clang++-23"}):
            second = BuildSpec.create("linux/amd64", repo=ROOT)
        self.assertEqual(first.object_key, second.object_key)

    def test_changed_container_packages_have_separate_compiler_trees_with_the_same_clang_version(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directories = []
            versions = []
            for image in ("sha256:" + "a" * 64, "sha256:" + "b" * 64):
                with patch.dict(os.environ, {"ONNXRUNTIME_BUILDER_IMAGE": image}):
                    tools = HostToolchain.discover("linux/amd64")
                build = RuntimeBuild(BuildSpec.create("linux/amd64"), Path(temporary))
                with patch("scripts.build_onnxruntime.HostToolchain.discover", return_value=tools), \
                        patch.object(build, "_source"), patch.object(build, "compile"):
                    build._build(Path(temporary) / "package")
                directories.append(build.build_dir)
                versions.append(tools.cc.version)
            self.assertEqual(versions[0], versions[1])
            self.assertNotEqual(directories[0], directories[1])

    @unittest.skipUnless(which("cmake") and which("ninja") and CLANG_CXX,
                         "requires CMake, Ninja, and installed Clang")
    def test_abseil_headers_exclude_host_features_when_nvcc_preprocesses_them(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            abseil = root / "abseil"
            traits = abseil / "absl/meta/type_traits.h"
            nullability = abseil / "absl/base/nullability.h"
            attributes = abseil / "absl/base/attributes.h"
            traits.parent.mkdir(parents=True)
            nullability.parent.mkdir(parents=True)
            # Exercise the pinned dependency's feature gates without downloading dependencies.
            traits.write_text(dedent("""\
                #pragma once
                #define ABSL_HAVE_BUILTIN(x) __has_builtin(x)
                #if ABSL_HAVE_BUILTIN(__builtin_is_cpp_trivially_relocatable)
                inline constexpr bool relocation_builtin = true;
                #else
                inline constexpr bool relocation_builtin = false;
                #endif
                """))
            nullability.write_text(dedent("""\
                #pragma once
                #define ABSL_HAVE_FEATURE(x) __has_feature(x)
                #if defined(__clang__) && !defined(__OBJC__) && ABSL_HAVE_FEATURE(nullability_on_classes)
                #define absl_nonnull _Nonnull
                #define absl_nullable _Nullable
                #define absl_nullability_unknown _Null_unspecified
                #else
                #define absl_nonnull
                #define absl_nullable
                #define absl_nullability_unknown
                #endif
                #if ABSL_HAVE_FEATURE(nullability_on_classes)
                #define ABSL_NULLABILITY_COMPATIBLE _Nullable
                #else
                #define ABSL_NULLABILITY_COMPATIBLE
                #endif
                """))
            attributes.write_text(dedent("""\
                #pragma once
                #define ABSL_HAVE_CPP_ATTRIBUTE(x) __has_cpp_attribute(x)
                #define ABSL_HAVE_ATTRIBUTE(x) __has_attribute(x)
                #if ABSL_HAVE_CPP_ATTRIBUTE(clang::lifetimebound)
                #define ABSL_ATTRIBUTE_LIFETIME_BOUND [[clang::lifetimebound]]
                #elif ABSL_HAVE_CPP_ATTRIBUTE(msvc::lifetimebound)
                #define ABSL_ATTRIBUTE_LIFETIME_BOUND [[msvc::lifetimebound]]
                #elif ABSL_HAVE_ATTRIBUTE(lifetimebound)
                #define ABSL_ATTRIBUTE_LIFETIME_BOUND __attribute__((lifetimebound))
                #else
                #define ABSL_ATTRIBUTE_LIFETIME_BOUND
                #endif
                """))
            original_headers = {path: path.read_bytes() for path in (traits, nullability, attributes)}
            runtime = root / "onnxruntime"
            linear_attention = runtime / "contrib_ops/cuda/bert/linear_attention_impl.cu"
            linear_attention.parent.mkdir(parents=True)
            linear_attention.write_text(dedent("""\
                using Status = int;
                void launch() {
                  auto launch_col = [&](auto dk_tag) -> Status { return 0; };
                  auto launch_decode = [&](auto dk_tag) -> Status { return 0; };
                  auto launch_fixed = [&](auto dk_tag, auto dv_tag) -> Status { return 0; };
                }
                """))
            (root / "empty.cc").write_text("int value = 0;\n")
            (root / "CMakeLists.txt").write_text(dedent(f"""\
                cmake_minimum_required(VERSION 3.29)
                project(nervix_cuda_headers LANGUAGES CXX)
                set(ONNXRUNTIME_ROOT "{runtime}")
                set(abseil_cpp_SOURCE_DIR "{abseil}")
                add_library(onnxruntime STATIC empty.cc)
                add_library(onnxruntime_providers_shared SHARED empty.cc)
                add_library(onnxruntime_providers_cuda SHARED empty.cc "{linear_attention}")
                target_include_directories(onnxruntime_providers_cuda PRIVATE "{abseil}")
                foreach(name flash_attention sm90_tma sm120_tma llm)
                  add_library(onnxruntime_providers_cuda_${{name}} OBJECT empty.cc)
                  target_include_directories(onnxruntime_providers_cuda_${{name}} PRIVATE "{abseil}")
                  target_link_libraries(onnxruntime_providers_cuda PRIVATE onnxruntime_providers_cuda_${{name}})
                endforeach()
                function(record_cuda_includes)
                  get_property(targets DIRECTORY PROPERTY BUILDSYSTEM_TARGETS)
                  foreach(target IN LISTS targets)
                    if(target MATCHES "^onnxruntime_providers_cuda($|_)")
                      get_target_property(includes ${{target}} INCLUDE_DIRECTORIES)
                      string(REPLACE ";" "\n" includes "${{includes}}")
                      file(WRITE "${{CMAKE_BINARY_DIR}}/${{target}}-includes.txt" "${{includes}}")
                    endif()
                  endforeach()
                endfunction()
                cmake_language(DEFER CALL record_cuda_includes)
                """))
            build = root / "build"
            result = subprocess.run([
                "cmake", "-S", str(root), "-B", str(build), "-G", "Ninja",
                f"-DCMAKE_CXX_COMPILER={CLANG_CXX}", "-Donnxruntime_USE_CUDA=ON",
                f"-DCMAKE_PROJECT_TOP_LEVEL_INCLUDES={ROOT}/scripts/onnxruntime/aggregate.cmake",
            ], capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            source = root / "probe.cc"
            source.write_text(dedent("""\
                #include "absl/meta/type_traits.h"
                #include "absl/base/nullability.h"
                #include "absl/base/attributes.h"
                #include <string_view>
                #define STRINGIFY_TOKEN(x) #x
                #define STRINGIFY(x) STRINGIFY_TOKEN(x)
                #if defined(__NVCC__)
                static_assert(!relocation_builtin);
                static_assert(std::string_view(STRINGIFY(ABSL_NULLABILITY_COMPATIBLE)).empty());
                static_assert(std::string_view(STRINGIFY(absl_nonnull)).empty());
                static_assert(std::string_view(STRINGIFY(absl_nullable)).empty());
                static_assert(std::string_view(STRINGIFY(absl_nullability_unknown)).empty());
                static_assert(std::string_view(STRINGIFY(ABSL_ATTRIBUTE_LIFETIME_BOUND)).empty());
                #else
                static_assert(relocation_builtin == __has_builtin(__builtin_is_cpp_trivially_relocatable));
                #if __has_feature(nullability_on_classes)
                static_assert(std::string_view(STRINGIFY(ABSL_NULLABILITY_COMPATIBLE)) == "_Nullable");
                static_assert(std::string_view(STRINGIFY(absl_nonnull)) == "_Nonnull");
                static_assert(std::string_view(STRINGIFY(absl_nullable)) == "_Nullable");
                static_assert(std::string_view(STRINGIFY(absl_nullability_unknown)) == "_Null_unspecified");
                #endif
                #if __has_cpp_attribute(clang::lifetimebound)
                static_assert(std::string_view(STRINGIFY(ABSL_ATTRIBUTE_LIFETIME_BOUND)) == "[[clang::lifetimebound]]");
                #endif
                #endif
                """))
            includes_files = sorted(build.glob("*-includes.txt"))
            self.assertEqual(len(includes_files), 5)
            for includes_file in includes_files:
                for mode in ("host", "nvcc"):
                    with self.subTest(target=includes_file.stem, mode=mode):
                        command = [CLANG_CXX, "-std=c++20", "-fsyntax-only"]
                        # Evaluate CUDA-only include entries for the reduced NVCC preprocessing probe.
                        for directory in includes_file.read_text().splitlines():
                            if directory.startswith("$<$<COMPILE_LANGUAGE:CUDA>:"):
                                if mode == "host":
                                    continue
                                directory = directory.removeprefix("$<$<COMPILE_LANGUAGE:CUDA>:").removesuffix(">")
                            command += ["-I", directory]
                        if mode == "nvcc":
                            command.append("-D__NVCC__=1")
                        result = subprocess.run([*command, str(source)], capture_output=True, text=True, timeout=30)
                        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            for path, content in original_headers.items():
                self.assertEqual(path.read_bytes(), content)

    @unittest.skipUnless(which("cmake") and which("ninja") and CLANG_CXX,
                         "requires CMake, Ninja, and installed Clang")
    def test_cuda_linear_attention_launches_use_the_specialized_device_stubs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            runtime = root / "onnxruntime"
            source = runtime / "contrib_ops/cuda/bert/linear_attention_impl.cu"
            source.parent.mkdir(parents=True)
            # Reduced NVCC host output: its stub specializations follow the launcher instantiation.
            source.write_text(dedent("""\
                #include <type_traits>
                struct Status { int value; };
                namespace {
                template<typename T, int DK> void device_stub(T*& value) {}
                template<typename T, int DK> void kernel(T* value) { device_stub<T, DK>(value); }
                }
                template<typename T> Status launch(T* value) {
                  auto launch_col = [&](auto dk_tag) -> Status {
                    kernel<T, decltype(dk_tag)::value>(value);
                    return Status{64};
                  };
                  auto launch_decode = [&](auto dk_tag) -> Status {
                    kernel<T, decltype(dk_tag)::value>(value);
                    return Status{128};
                  };
                  auto launch_fixed = [&](auto dk_tag, auto dv_tag) -> Status {
                    kernel<T, decltype(dk_tag)::value + decltype(dv_tag)::value>(value);
                    return Status{256};
                  };
                  auto col = launch_col(std::integral_constant<int, 64>{});
                  auto decode = launch_decode(std::integral_constant<int, 128>{});
                  auto fixed = launch_fixed(std::integral_constant<int, 128>{}, std::integral_constant<int, 128>{});
                  return Status{col.value + decode.value + fixed.value};
                }
                template Status launch<float>(float*);
                namespace {
                template<> void device_stub<float, 64>(float*& value) { *value += 64; }
                template<> void device_stub<float, 128>(float*& value) { *value += 128; }
                template<> void device_stub<float, 256>(float*& value) { *value += 256; }
                }
                int main() {
                  float value = 0;
                  Status result = launch(&value);
                  return value == 448 && result.value == 448 ? 0 : 1;
                }
                """))
            original_source = source.read_bytes()
            abseil = root / "abseil"
            for name, guard in (
                ("meta/type_traits.h", "#if ABSL_HAVE_BUILTIN(__builtin_is_cpp_trivially_relocatable)"),
                ("base/nullability.h", "#if ABSL_HAVE_FEATURE(nullability_on_classes)"),
                ("base/attributes.h", "#if ABSL_HAVE_CPP_ATTRIBUTE(clang::lifetimebound)"),
            ):
                header = abseil / "absl" / name
                header.parent.mkdir(parents=True, exist_ok=True)
                header.write_text(guard + "\n#endif\n")
            (root / "empty.cc").write_text("int value = 0;\n")
            (root / "CMakeLists.txt").write_text(dedent(f"""\
                cmake_minimum_required(VERSION 3.29)
                project(nervix_cuda_launches LANGUAGES CXX)
                set(ONNXRUNTIME_ROOT "{runtime}")
                set(abseil_cpp_SOURCE_DIR "{abseil}")
                add_library(onnxruntime STATIC empty.cc)
                add_library(onnxruntime_providers_shared SHARED empty.cc)
                add_library(onnxruntime_providers_cuda SHARED empty.cc "{source}")
                file(GENERATE OUTPUT "${{CMAKE_BINARY_DIR}}/cuda-sources.txt"
                  CONTENT "$<JOIN:$<TARGET_PROPERTY:onnxruntime_providers_cuda,SOURCES>,\n>")
                """))
            build = root / "build"
            result = subprocess.run([
                "cmake", "-S", str(root), "-B", str(build), "-G", "Ninja",
                f"-DCMAKE_CXX_COMPILER={CLANG_CXX}", "-Donnxruntime_USE_CUDA=ON",
                f"-DCMAKE_PROJECT_TOP_LEVEL_INCLUDES={ROOT}/scripts/onnxruntime/aggregate.cmake",
            ], capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            sources = (build / "cuda-sources.txt").read_text().splitlines()
            self.assertIn("empty.cc", sources)
            cuda_sources = [path for path in sources if path.endswith(".cu")]
            self.assertEqual(len(cuda_sources), 1)
            executable = root / "probe"
            result = subprocess.run([
                CLANG_CXX, "-std=c++20", "-x", "c++", cuda_sources[0], "-o", str(executable),
            ], capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            result = subprocess.run([str(executable)], capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(source.read_bytes(), original_source)

    @unittest.skipUnless(which("cmake") and which("ninja") and CLANG_CXX,
                         "requires CMake, Ninja, and installed Clang")
    def test_xqa_fp8_conversion_preserves_both_lanes_and_the_scalar_tail(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            runtime = root / "onnxruntime"
            xqa = runtime / "contrib_ops/cuda/bert/xqa"
            xqa.mkdir(parents=True)
            (xqa.parent / "linear_attention_impl.cu").write_text(dedent("""\
                using Status = int;
                void launch() {
                  auto launch_col = [&](auto dk_tag) -> Status { return 0; };
                  auto launch_decode = [&](auto dk_tag) -> Status { return 0; };
                  auto launch_fixed = [&](auto dk_tag, auto dv_tag) -> Status { return 0; };
                }
                """))
            (xqa / "types.h").write_text(dedent("""\
                #include <cstdint>
                struct float2 { float x, y; };
                struct __nv_fp8_e4m3 { float value; explicit operator float() const { return value; } };
                struct __nv_fp8x2_e4m3 {
                  __nv_fp8_e4m3 x, y;
                  explicit operator float2() const { return {x.value, y.value}; }
                };
                template<class T, uint32_t size> struct Vec {
                  T values[size];
                  T& operator[](uint32_t i) { return values[i]; }
                  T const& operator[](uint32_t i) const { return values[i]; }
                };
                """))
            (xqa / "utils.cuh").write_text(dedent("""\
                template<uint32_t size> Vec<float, size> convert(Vec<__nv_fp8_e4m3, size> const& src) {
                  Vec<float, size> dst;
                  for (uint32_t i = 0; i < size - 1; i += 2) {
                    reinterpret_cast<float2&>(dst[i]) = float2(reinterpret_cast<__nv_fp8x2_e4m3 const&>(src[i]));
                  }
                  if constexpr (size % 2 != 0) { dst[size - 1] = float(src[size - 1]); }
                  return dst;
                }
                """))
            (xqa / "mhaUtils.cuh").write_text('#include "utils.cuh"\n')
            source = xqa / "xqa_loader_bf16_128.cu"
            source.write_text(dedent("""\
                #include "types.h"
                // NVCC rewrites functional aggregate casts to brace initialization in host output.
                #define float2(...) float2{__VA_ARGS__}
                #include "mhaUtils.cuh"
                int main() {
                  Vec<__nv_fp8_e4m3, 3> odd{{{1.25f}, {-2.5f}, {7.0f}}};
                  Vec<__nv_fp8_e4m3, 2> even{odd[0], odd[1]};
                  auto odd_result = convert(odd);
                  auto even_result = convert(even);
                  return odd_result[0] == 1.25f && odd_result[1] == -2.5f && odd_result[2] == 7.0f &&
                    even_result[0] == 1.25f && even_result[1] == -2.5f ? 0 : 1;
                }
                """))
            original_files = {path: path.read_bytes() for path in xqa.iterdir()}
            abseil = root / "abseil"
            for name, guard in (
                ("meta/type_traits.h", "#if ABSL_HAVE_BUILTIN(__builtin_is_cpp_trivially_relocatable)"),
                ("base/nullability.h", "#if ABSL_HAVE_FEATURE(nullability_on_classes)"),
                ("base/attributes.h", "#if ABSL_HAVE_CPP_ATTRIBUTE(clang::lifetimebound)"),
            ):
                header = abseil / "absl" / name
                header.parent.mkdir(parents=True, exist_ok=True)
                header.write_text(guard + "\n#endif\n")
            (root / "empty.cc").write_text("int value = 0;\n")
            (root / "CMakeLists.txt").write_text(dedent(f"""\
                cmake_minimum_required(VERSION 3.29)
                project(nervix_xqa_conversion LANGUAGES CXX)
                set(ONNXRUNTIME_ROOT "{runtime}")
                set(abseil_cpp_SOURCE_DIR "{abseil}")
                add_library(onnxruntime STATIC empty.cc)
                add_library(onnxruntime_providers_shared SHARED empty.cc)
                add_library(onnxruntime_providers_cuda SHARED empty.cc
                  "{xqa.parent / 'linear_attention_impl.cu'}" "{source}")
                file(GENERATE OUTPUT "${{CMAKE_BINARY_DIR}}/cuda-sources.txt"
                  CONTENT "$<JOIN:$<TARGET_PROPERTY:onnxruntime_providers_cuda,SOURCES>,\n>")
                """))
            build = root / "build"
            result = subprocess.run([
                "cmake", "-S", str(root), "-B", str(build), "-G", "Ninja",
                f"-DCMAKE_CXX_COMPILER={CLANG_CXX}", "-Donnxruntime_USE_CUDA=ON",
                f"-DCMAKE_PROJECT_TOP_LEVEL_INCLUDES={ROOT}/scripts/onnxruntime/aggregate.cmake",
            ], capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            sources = (build / "cuda-sources.txt").read_text().splitlines()
            selected = next(path for path in sources if Path(path).name == source.name)
            executable = root / "probe"
            result = subprocess.run([
                CLANG_CXX, "-std=c++20", "-x", "c++", selected, "-o", str(executable),
            ], capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            result = subprocess.run([str(executable)], capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual({path: path.read_bytes() for path in xqa.iterdir()}, original_files)

    @unittest.skipUnless(which("cmake") and which("ninja") and CLANG_CXX,
                         "requires CMake, Ninja, and installed Clang")
    def test_release_build_reports_warnings_and_rejects_compiler_errors(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            build = RuntimeBuild(BuildSpec.create("linux/amd64", repo=ROOT), Path(temporary))
            with patch.dict(os.environ, {"ONNXRUNTIME_SYSROOT": "/", "ONNXRUNTIME_GCC_TOOLCHAIN": "/usr"}):
                tools = HostToolchain.discover("linux/amd64")
            build.tools = replace(tools, cxx=replace(tools.cxx, executable=CLANG_CXX))
            source = build.source_dir / "cmake"
            source.mkdir(parents=True)
            (source / "CMakeLists.txt").write_text(
                "cmake_minimum_required(VERSION 3.29)\n"
                "project(nervix_warning_probe LANGUAGES CXX)\n"
                "add_library(vendor OBJECT warning.cc)\n"
                "set_property(TARGET vendor PROPERTY COMPILE_WARNING_AS_ERROR ON)\n"
            )
            warning = source / "warning.cc"
            warning.write_text('#warning "nervix_vendor_warning"\nint value = 1;\n')
            with patch("scripts.build_onnxruntime.run", side_effect=BuildError("configuration captured")) as command:
                with self.assertRaisesRegex(BuildError, "configuration captured"):
                    build.compile(Path(temporary) / "package")
            # This fixture owns its target graph; retain the producer's other configure options.
            configure = [argument for argument in command.call_args.args[0]
                         if not argument.startswith("-DCMAKE_PROJECT_TOP_LEVEL_INCLUDES=")]
            result = subprocess.run(configure, capture_output=True, text=True, timeout=60)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            compile = ["cmake", "--build", str(build.build_dir)]
            result = subprocess.run(compile, capture_output=True, text=True, timeout=60)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("nervix_vendor_warning", result.stdout + result.stderr)
            warning.write_text('#error "nervix_compile_error"\n')
            result = subprocess.run(compile, capture_output=True, text=True, timeout=60)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("nervix_compile_error", result.stdout + result.stderr)

    def test_cuda_host_sources_are_checked_before_compiling_gpu_kernels(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            build = RuntimeBuild(BuildSpec.create("linux/amd64", repo=ROOT), Path(temporary))
            build.tools = HostToolchain.discover("linux/amd64")
            host_object = "CMakeFiles/onnxruntime_providers_cuda.dir/attention.cc.o"
            device_object = "CMakeFiles/onnxruntime_providers_cuda.dir/attention.cu.o"
            targets = f"{host_object}: CXX_COMPILER\n{device_object}: CUDA_COMPILER\n"

            def compile_command(command: list[str], **kwargs: object) -> None:
                if command[:2] == ["cmake", "--build"]:
                    self.assertIn(host_object, command)
                    raise BuildError("host compilation failed")

            with patch("scripts.build_onnxruntime.subprocess.check_output", return_value=targets) as query:
                with patch("scripts.build_onnxruntime.run", side_effect=compile_command) as compile:
                    with self.assertRaisesRegex(BuildError, "host compilation failed"):
                        build.compile(Path(temporary) / "package")
            query.assert_called_once_with(["ninja", "-C", str(build.build_dir), "-t", "targets", "all"], text=True)
            self.assertEqual(compile.call_count, 2)

    def test_native_build_records_provenance_and_uses_the_selected_cmake_compilers(self) -> None:
        for target, system, machine in (("linux/amd64", "Linux", "x86_64"),
                                        ("linux/arm64", "Linux", "aarch64"),
                                        ("darwin/arm64", "Darwin", "arm64")):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as temporary:
                build = RuntimeBuild(BuildSpec.create(target, repo=ROOT), Path(temporary))
                source = build.source_dir / "include/onnxruntime/core/session"
                source.mkdir(parents=True)
                (source / "onnxruntime_c_api.h").write_text("fixture header")
                for name in ("LICENSE", "ThirdPartyNotices.txt"):
                    (build.source_dir / name).write_text("fixture notice")
                destination = Path(temporary) / "package"
                destination.mkdir()
                runtime_libraries = ([Path("/fixture/libstdc++.a"), Path("/fixture/libgcc.a"),
                                      Path("/fixture/libgcc_eh.a")] if system == "Linux" else [])

                def run_command(command: list[str], **kwargs: object) -> None:
                    if command[:2] == ["cmake", "--build"]:
                        (build.build_dir / "nervix-archives.txt").write_text("/fixture/a library.a\n")
                    elif command[0] == "/host/bin/llvm-ar":
                        self.assertEqual(command[1:3], ["qcLs", str(destination / "lib/libonnxruntime.a")])
                        self.assertEqual(command[3:], ["/fixture/a library.a", *map(str, runtime_libraries)])
                        (destination / "lib/libonnxruntime.a").write_bytes(b"!<arch>\nfixture")
                    elif command[0] == "/Xcode/bin/libtool":
                        self.assertEqual(command[1:4], ["-static", "-o", str(destination / "lib/libonnxruntime.a")])
                        (destination / "lib/libonnxruntime.a").write_bytes(b"!<arch>\nfixture")

                with patch("scripts.onnxruntime.toolchain.platform.system", return_value=system):
                    with patch("scripts.onnxruntime.toolchain.platform.machine", return_value=machine):
                        with patch.object(build, "_source"), patch.object(build, "_smoke") as smoke, \
                                patch.object(HostToolchain, "static_runtime", return_value=runtime_libraries):
                            with patch.object(build, "_cuda_host_sources"), patch.object(
                                build, "_cuda_runtime", side_effect=lambda destination, tools: construct_cuda_runtime(destination)
                            ), patch("scripts.build_onnxruntime.run", side_effect=run_command) as command:
                                build._build(destination)
                                build._seal(destination)
                build.validate_package(destination)
                cmake = command.call_args_list[0].args[0]
                self.assertIn(f"-DCMAKE_C_COMPILER={build.tools.cc.executable}", cmake)
                self.assertIn(f"-DCMAKE_CXX_COMPILER={build.tools.cxx.executable}", cmake)
                self.assertEqual(build.provenance["cc"]["version"], "clang version 22.1.0")
                if system == "Darwin":
                    self.assertIn("-DCMAKE_OSX_SYSROOT=/Xcode/SDK", cmake)
                    self.assertIn("-DCMAKE_OSX_DEPLOYMENT_TARGET=14.0", cmake)
                else:
                    self.assertIn("-Donnxruntime_USE_CUDA=ON", cmake)
                    self.assertIn(f"-DCMAKE_CUDA_COMPILER={build.tools.cuda.executable}", cmake)
                smoke.assert_called_once_with(destination, build.tools)


if __name__ == "__main__":
    unittest.main()
