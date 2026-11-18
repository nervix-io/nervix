# ONNX Runtime 1.30.0

The Linux CPU archives contain ONNX Runtime built from Microsoft commit
`f2c39fe2f838cf35ce7da92824f5a5e3ee6e88a7`. The installer uses the same package names,
library paths, and versioned symlinks as the standalone release. Both archives include the
upstream license, privacy notice, third-party notices, headers, and install metadata.

The x64 archive repacks the locally installed standalone package. The ARM64 archive pairs its
common SDK files with the ARM64 runtime and shared provider libraries from the previously
built Nervix ARM64 image. Both architectures' native libraries match the corresponding files
in [Microsoft.ML.OnnxRuntime 1.30.0](https://www.nuget.org/packages/Microsoft.ML.OnnxRuntime/1.30.0)
byte for byte. The headers and notices shared with that package also match after normalizing
line endings.

| Archive | SHA-256 |
| --- | --- |
| `onnxruntime-linux-x64-1.30.0.tgz` | `d608986105245f46078cfedebd07aa1fe5ea8ec1d968b6271a5ee03befde273a` |
| `onnxruntime-linux-aarch64-1.30.0.tgz` | `b47c99f0646cfa2eca1842d76bfcd753eb4db99d5a9688d9477e354a2781ed17` |

When updating the pin, replace both archives with the corresponding
[upstream Linux CPU packages](https://github.com/microsoft/onnxruntime/releases) and update
their hashes and provenance here. `just test-onnxruntime` checks cold installation for both
architectures and the other platform selectors.
