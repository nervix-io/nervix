# Cargo Install From GitHub

From a [clone of the Nervix repository](developing-nervix.md#clone-the-repository), install the
server, interactive CLI, and NSPL formatter:

```bash
just install
```

The recipe's dependencies fetch the published static ONNX Runtime artifact from R2 and prepare the embedded web
console, then build and install the binaries into Cargo's binary directory, `$HOME/.cargo/bin` by
default. Ensure that directory is on your `PATH`. An existing validated ONNX Runtime artifact is
reused. An unavailable artifact or checksum pin fails installation; ONNX Runtime compilation is
a separate maintainer task.

Cargo build output stays in the repository's `target` directory, or `CARGO_TARGET_DIR` when set,
so later installations reuse the compiled dependencies.

The build requires Rust 1.98 or newer with the `wasm32-unknown-unknown` target, Trunk, `just`,
Python 3.12 or newer, `uv`, Git, and the native build tools required by Nervix's dependencies.
ONNX Runtime downloads use the public R2 development URL without credentials; validated local
artifacts are reused without another download. CUDA and cuDNN SDKs are unnecessary for this installation.
Supported server targets are Linux x86_64, Linux arm64, and macOS arm64. The initial build can
take several minutes. See
[ONNX Runtime Artifact Cache](developing-nervix.md#onnx-runtime-artifact-cache) for stage and
R2 cache configuration. The installed server contains the ONNX Runtime core.

Confirm that the commands are available:

```bash
nervix-server --help
nervix-cli --help
nervix-nspl-format --help
```

## Pin A Git Revision

The clone initially selects the current default branch. For a reproducible installation, select a
reviewed commit in your clone and run `just install`. To upgrade, select the desired revision and
run `just install --force`.

To remove the binaries:

```bash
just uninstall
```
