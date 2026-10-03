# Developing Nervix

This section is for working on Nervix itself. Everything here assumes a clone of the Nervix
repository and uses its `just` recipes; none of it is required to use Nervix.

## Clone The Repository

```bash
git clone https://github.com/nervix-io/nervix
cd nervix
```

## Prerequisites

Nervix is developed on Linux x86_64, Linux aarch64, and macOS arm64. The server statically links the
pinned ONNX Runtime core. Linux amd64 and arm64 each have a portable GNU/Linux variant and a
variant for the product's pinned Debian Docker base. Both include CUDA 13 and CPU execution.
macOS arm64 uses CPU execution because CUDA is unavailable there. Native server recipes fetch
the portable variant; Debian image recipes fetch the matching Docker variant. Portable Linux
artifacts require glibc 2.28 or newer and include their C++ runtime dependencies. NVIDIA's packaged
CUDA libraries use glibc; native musl systems such as Alpine require a separate runtime contract.

Install:

- Rust via `rustup`, including the `wasm32-unknown-unknown` target
- Trunk for the embedded web console
- `just` (latest release)
- Python 3.12 or newer and `uv`
- Git and the LLVM build tools used by Nervix's dependencies
- Xcode command line tools on macOS
- Docker for product images and tests that use external services
- `zellij`

## ONNX Runtime Artifact Cache

Normal development downloads ONNX Runtime builds published in R2 by maintainers. Building ONNX
Runtime from source is a separate maintainer task, never a prerequisite for building Nervix.
Server builds, installation, tests, and Debian image builds depend on `fetch-onnxruntime`, which
reuses a verified local package or downloads the required artifact over public R2 HTTPS without
credentials. Run the recipe for the task you want; its dependencies select the target and set
`ORT_LIB_PATH`.

Normal recipes build Nervix's own programs and assets and fetch the published ONNX Runtime
artifact. Their dependencies make the runtime available before compiling code that links it.

| Task | Command |
| --- | --- |
| Build the server | `just build-server` |
| Install the server, CLI, and formatter | `just install` |
| Run the test suite | `just test` |
| Fetch the published runtime artifact | `just fetch-onnxruntime` |
| Verify native Linux CUDA inference | `just verify-onnxruntime` |

Fetching requires a checksum pin in `scripts/onnxruntime/checksums.toml`. It validates the completed
local package, then tries R2 when that package is absent or invalid. An unavailable version,
platform, or checksum pin fails with instructions for a maintainer to publish the artifact. This policy applies to development
and CI: fetching never downloads ONNX Runtime sources or invokes its compiler. Ordinary development
requires neither an ONNX Runtime build toolchain nor CUDA or cuDNN SDKs.

The package contains `libonnxruntime.a`, headers, licenses, and a manifest with a SHA-256 checksum
for every file. Downloads verify the archive against the checked-in pin before extraction, then
verify its manifest, target, and complete build identity before installation. Local reuse also
requires the pinned checksum and package verification receipt.

Artifact downloads show a progress bar on stderr with bytes transferred and transfer speed.
When the server provides the archive size, the bar also shows percentage complete and estimated
time remaining. Downloads without a size show a running byte count. Stdout remains the prepared
library path for commands that consume it.

The recipes use the shared stage root `~/.cache/nervix-build/onnxruntime`; `NERVIX_ONNXRUNTIME_DIR`
overrides it. Every workspace reuses the same verified package when its build identity and
checksum pin match, without another download. Completed packages are installed atomically under
`packages`. Version, source revision, target, build scripts,
and build configuration determine the artifact identity. Cache lookup does not require compilers,
CMake, Ninja, or an SDK. The package manifest records the producing host's compiler commands,
versions, launchers, flags, SDK, and container image identity. Manual maintainer builds share sources
by revision and retain compiler intermediates using the compiler identity, source revision, target,
and artifact variant. Package validation changes reuse
unchanged compiler outputs; CMake and Ninja track changes to compilation settings and dependencies.
Builds sharing compiler intermediates also share a file lock.
Repeated fetches reuse the validated package without network requests or compilation. Concurrent
invocations share a file lock. Interrupted maintainer builds retain compiler intermediates for a retry.

### Maintainer Source Builds

`just build-onnxruntime [platform]` performs the complete manual build for local use. It reuses a
completed package first. When a build is needed, it downloads the pinned sources, prepares
the pinned compiler container and target SDK, compiles with LLVM, verifies the package, and records
its SHA-256 in
`scripts/onnxruntime/checksums.toml`. A workflow lock covers the build and pinning, and packages are
installed atomically after validation. Repeating the command skips downloads, compilation, and
compression for an already verified package. Normal recipes can immediately reuse the local result.

`just pin-onnxruntime [platform]` records the fingerprint and archive SHA-256 for a completed local
artifact in `scripts/onnxruntime/checksums.toml` and prints its TOML entry. It validates the package,
reuses its cached archive or creates one when needed, and updates only the selected platform and variant's pin.
It requires a completed package matching the current manifest and build identity. Pinning needs no
compiler or R2 credentials and performs no downloads, source compilation, or publication.

`just build-onnxruntime native --force` recompiles a completed package and updates its local
checksum pin. Use `just build-onnxruntime linux/arm64 --force` for the arm64 cross-build, or pass
`--force` to `just build-artifacts [platform]`. A forced build cleans the selected compiler tree's
entire contents while retaining the pinned source checkout and downloaded SDKs outside that tree. The existing
package stays available until its replacement builds and validates successfully. A compilation
failure leaves the existing verified package and checksum pin usable.

`just build-artifacts [platform]` is the manual aggregate for external artifacts and depends on
`build-onnxruntime`. No normal development or CI recipe depends on either build task. These commands
require no R2 credentials; `just publish-onnxruntime` uploads a completed package separately.
Source compilation, checksum pinning, and publication are disabled in CI.

Linux source builds require Docker and the repository's Python/uv environment on the host. They
run inside a Debian builder whose base image digest is pinned in
`scripts/onnxruntime/downloads.json`. Apt installs current packages from the normal Debian and
LLVM repositories, including Ninja, QEMU, LLVM 23, and LLVM 21. CMake, CUDA, and cuDNN use
checksum-pinned archives. Host compiler settings and compiler caches do not enter the build
container. LLVM 23 compiles the C++ sources; CUDA uses LLVM 21 as its supported host compiler. Both compiler
provenance and the producing container image are recorded in each package manifest. Package
revision numbers are recorded after installation; apt selects available updates within the
chosen Debian release and LLVM major versions.
Compiler intermediates are separated by the actual builder image ID, so an image rebuilt with
updated packages gets its own compiler tree even when Clang's version string stays the same.

Linux has two artifact variants for each architecture:

| Variant | Runtime target | Selection |
| --- | --- | --- |
| `docker` | The exact Debian base image used by `Dockerfile.debian` | Debian image recipes select it automatically |
| `portable` | GNU/Linux with glibc 2.28 or newer | Native development and installation select it by default |

The portable variant compiles against private headers and libraries from digest-pinned
[PyPA manylinux 2.28 images](https://github.com/pypa/manylinux), based on AlmaLinux 8 with GCC 14.
This baseline follows the pinned [CUDA 13.2 Linux support table](https://docs.nvidia.com/cuda/archive/13.2.0/cuda-installation-guide-linux/index.html#system-requirements),
which includes RHEL 8 with glibc 2.28 for amd64 and arm64 SBSA.
Its static archive contains the C++ runtime, and CUDA providers link their C++ runtime statically.
Packaging rejects shared libraries or the linked smoke executable that require a newer glibc than
the selected variant. CUDA vendor libraries keep their own documented driver and hardware
requirements. These are GNU/glibc artifacts; Alpine's native musl runtime requires a separate CUDA
support contract.

Build and publish both variants explicitly:

```sh
just build-onnxruntime linux/amd64 --variant portable --force
just build-onnxruntime linux/arm64 --variant portable --force
just build-onnxruntime linux/amd64 --variant docker --force
just build-onnxruntime linux/arm64 --variant docker --force
just publish-onnxruntime linux/amd64 --variant portable
just publish-onnxruntime linux/arm64 --variant portable
just publish-onnxruntime linux/amd64 --variant docker
just publish-onnxruntime linux/arm64 --variant docker
```

`--variant` also selects artifacts for fetching, pinning, and verification. Each platform and
variant has its own checksum pin and R2 object. Changing the Docker base updates the Docker
artifact identity and keeps the application and dependency bases aligned. The shared stage root
retains source and SDK downloads; `--force` deletes the selected compiler tree before compilation.

Builds support matching Linux amd64 and Linux arm64 hosts, and cross-compilation from Linux amd64
to Linux arm64. Cross builds use QEMU for CPU inference and CUDA provider loading. Provider loading
uses a private CUDA driver stub when no GPU inference is requested; that stub is never packaged.
Native GPU verification still requires a matching host and NVIDIA GPU. macOS arm64 source builds
use locally installed LLVM and the Xcode command line tools and SDK.
A C API program links the archive and executes a generated ONNX model before installation. Linux
builds also initialize and load the packaged CUDA provider before running CPU inference. This
check can run without a GPU.

The pinned runtime forces warnings to errors on its core and CUDA targets. Release builds pass
CMake's `--compile-no-warning-as-error` switch so newer LLVM diagnostics remain visible warnings.
Actual compiler errors still fail the build. This setting participates in the artifact identity.
Each NVCC invocation uses one compiler thread so `--jobs` bounds concurrent CUDA frontends,
including the memory-intensive MoE and LLM kernels.
CUDA compilation uses generated Abseil headers that exclude Clang's relocation builtin,
class nullability annotations, and lifetime annotations from NVCC compilations. These headers
apply to the CUDA provider and its architecture-specific object libraries, including FlashAttention,
SM90/SM120 TMA, and LLM targets. Ordinary Clang compilation retains those features. The fetched
dependencies remain unchanged, and the corrections are part of the build identity.

The CUDA provider also compiles a generated copy of `linear_attention_impl.cu` whose three
generic launch lambdas deduce their `Status` return types. This prevents Clang from instantiating
NVCC's host wrappers before their generated specializations are declared. Kernel bodies and
launch parameters are unchanged; the fetched runtime source remains unchanged.

XQA sources and headers are also copied into the build tree so their packed FP8-to-`float2`
conversion can explicitly select CUDA's conversion operator. This prevents NVCC from emitting
invalid aggregate initialization in the host code while preserving both packed values.

Every Linux package is built with CUDA 13 enabled. Linux amd64 and
[arm64 SBSA](https://docs.nvidia.com/cuda/archive/13.2.0/cuda-installation-guide-linux/index.html#system-requirements)
source builds prepare checksum-pinned CUDA 13.2 and cuDNN 9.20 SDKs inside the compiler container.
The builder uses LLVM 23 for ordinary C++ compilation and LLVM 21, the supported CUDA host
compiler, for NVCC. Compiler launchers remain empty. The CUDA provider's host C++ sources are
checked before compiling its GPU kernels, so a host compiler error fails before the kernel build.
The package includes provider shared libraries, their CUDA user-space dependency closure,
and the cuDNN and NVRTC libraries loaded dynamically. Packaging reads ELF dependencies without
executing the target loader, resolves them against the selected SDK roots, and verifies that every
library matches the target architecture. Shared libraries and the linked C API smoke executable
are checked against the selected variant's glibc baseline. Target SDK inputs participate in the
artifact identity.

### Cross-Compile Linux Arm64

For maintainers on Linux amd64, `just build-onnxruntime linux/arm64` builds the portable arm64
CUDA artifact inside the pinned amd64 compiler container. Add `--variant docker` to build against
the application's Debian runtime baseline. The container prepares LLVM LLD, QEMU, host-native
NVCC, and arm64 CUDA and cuDNN SDKs. The portable variant uses its private manylinux 2.28 SDK;
the Docker variant uses Debian target packages from the same release and repositories as the
application image. Clang receives the arm64 target, sysroot, and target C++ runtime flags.

CMake searches the target SDKs for headers and libraries and the compiler container for build
programs. ONNX Runtime fetches its pinned host-native `protoc` for code generation. CUDA
compilation explicitly selects the SBSA SDK, and provider linking uses LLVM 21 and LLD.

Before installation, cross-builds link an arm64 C API smoke program and run CPU inference and CUDA
provider loading under QEMU. The guest loader receives the SBSA SDK's `libcuda` driver stub in a
private smoke directory to resolve driver symbols without executing GPU work. Other CUDA libraries
come from the package. The driver stub is excluded from the artifact and the host QEMU process's
library paths; deployments use their installed NVIDIA driver. Full GPU verification uses
`just verify-onnxruntime linux/arm64` on an arm64 Linux host with an NVIDIA GPU and driver.
QEMU cannot verify GPU execution.
Native and cross-builds use the same target artifact identity; their compiler intermediates remain
separate by toolchain identity. Cached artifacts require none of these cross-build tools or SDKs.

`just verify-onnxruntime` prepares the required package through its dependency. On Linux, it
runs matrix multiplication and a cuDNN convolution with CUDA and CPU fallback disabled.
It requires an accessible NVIDIA GPU,
a compatible driver, and the native LLVM build tools. Verification of a cached package uses its
packaged runtime libraries and requires neither NVCC nor a CUDA or cuDNN SDK. Preparation and
publication reuse validated artifacts; CI only restores the published package. Ordinary CPU
inference uses the same Linux artifact and does not require an NVIDIA GPU or driver. macOS
verification runs the CPU model.

### Shared R2 Cache

Development and CI download artifacts over public HTTPS from
`https://pub-4668ad14d0814ca58c09a124f6bd96f3.r2.dev`. Downloads require no R2 credentials.
Objects use `onnxruntime/<version>/<platform>/<variant>/<build-identity>.tar.gz`, for example
`onnxruntime/1.28.2/linux/arm64/portable/<build-identity>.tar.gz`. Preparation checks the downloaded archive
before extracting and installs a verified package atomically. Missing objects, HTTP errors,
network failures, and checksum failures stop preparation without starting a source build.

`scripts/onnxruntime/checksums.toml` pins each platform and variant's build identity and archive SHA-256.
Downloaded bytes must match that pin; R2 object metadata does not establish trust. A missing pin
prevents artifact fetching in both development and CI. Explicit local builds also record their
checksum in this file for local reuse. The checksum file is excluded from the build identity, so pinning
an artifact does not invalidate the compiled runtime or change its R2 key.

Cached downloads retain a verification receipt tied to the pin and package manifest. Preparation
checks that receipt and every packaged file without another download. Changing the pin invalidates
a cached download. Archives normalize timestamps, owner information, permissions, and gzip headers,
so repackaging unchanged contents produces the same checksum. Completed upload archives are cached
under the stage directory to avoid repeated compression.

### Manual Publication

Publication uploads to the fixed `nervix-artifacts` bucket in Cloudflare account
`93f53d256da23587269279513de3fc80` through its authenticated S3 API. Only local maintainer uploads
need R2 credentials with object write access:

| Environment variable | Value |
| --- | --- |
| `R2_ACCESS_KEY_ID` | R2 S3 API access key |
| `R2_SECRET_ACCESS_KEY` | R2 S3 API secret |

`just publish-onnxruntime [platform]` uploads the completed, locally pinned package from the shared
cache. It validates that package against its checksum pin and uploads the exact verified archive.
Publication requires an existing package and performs no source build or tool setup.
Uploads display a progress bar on standard error with the percentage, transferred bytes, transfer
speed, and estimated time remaining. Multipart upload workers share the same progress bar.

| Package | Command |
| --- | --- |
| Prepare tools, build, and pin the current host's package for local use | `just build-onnxruntime` |
| Prepare tools, build, and pin Linux arm64 CUDA from Linux amd64 for local use | `just build-onnxruntime linux/arm64` |
| Rebuild and repin a completed package in its pinned environment | `just build-onnxruntime native --force` |
| Manually build external artifacts for local use | `just build-artifacts` |
| Upload the completed current host's package | `just publish-onnxruntime` |
| Upload the completed Linux arm64 package | `just publish-onnxruntime linux/arm64` |

These commands are manual maintainer operations. CI only fetches published artifacts. An upload
failure retains the completed local package and archive, so rerunning publication retries without compiling
or compressing again. After publishing, include the updated `scripts/onnxruntime/checksums.toml`
in the same commit as the corresponding runtime version or build-input change. A package kept
for local use requires no publication; its checksum pin can remain a local change. Pinning and
publication are disabled in CI; consumers read the checked-in pins.

A verified local artifact is reused without network requests or build tools. Source builds require
no R2 credentials; only publication configures the authenticated upload client. GitHub Actions
downloads through the public URL and requires no R2 secrets. The compiler's kache cache retains
its existing S3 configuration independently.

### Build The Debian Image

```bash
just docker-build-debian nervix:debian linux/amd64
```

For arm64, use `just docker-build-debian nervix:debian-arm64 linux/arm64`.

The recipe's dependencies fetch the published CUDA 13 artifact for amd64 or arm64,
then supply that validated package as a named Docker build context. Product image builds start
after the artifact is available and validated. A missing artifact fails before Docker starts and
must be published by a maintainer. Image builds in development and CI never compile ONNX Runtime.
Configure the existing `KACHE_S3_*` variables for the Rust compiler cache as well. GitHub Actions
invokes this same recipe and dependency graph.

Use `just test-onnxruntime-tooling` for cache, interruption, integrity, concurrency, and image recipe
coverage. Both `just validate` and `just validate-ci` include this suite.

## Start The Server

The server crate and executable are both named `nervix-server`. The `just server` recipe creates a
development CA and a `default`/`node-1` identity, then runs that dedicated server binary.

```bash
NERVIX_INIT_DEFAULT_USER_PASSWORD='nervix' just server
```

Fresh clusters require an initial password for the `default` user. Set
`NERVIX_INIT_DEFAULT_USER_PASSWORD` or pass `--init-default-user-password <password>` on the first
startup. The leader stores the `default` user's Argon2 password hash in the strongly consistent
control plane only when that user does not already exist. After the default user has been created,
remove the environment variable or flag from normal startup. Later users can be created through NSPL:

```nspl
CREATE USER my_username WITH PASSWORD 'my_secure_password';
```

Clustered startup example:

```bash
just server -- \
  --addr 127.0.0.1:47391 \
  --http-listen-addr 0.0.0.0:8080 \
  --https-listen-addr 0.0.0.0:8443 \
  --grpc-advertise-addr 10.0.0.10:47391 \
  --cluster-id production \
  --node-id node-1 \
  --interconnect-listen-addr 0.0.0.0:47395 \
  --interconnect-advertise-addr node-1.internal.example:47395 \
  --interconnect-tls-ca /etc/nervix/interconnect/ca.pem \
  --interconnect-tls-cert /etc/nervix/interconnect/node-1.pem \
  --interconnect-tls-key /etc/nervix/interconnect/node-1-key.pem \
  --cluster-bootstrap-host node-2.internal.example:47395
```

Nervix uses separate listener addresses for plain and TLS server-side traffic:

- `--http-listen-addr` for HTTP and WS
- `--https-listen-addr` for HTTPS and WSS

All internal node-to-node traffic uses one authenticated HTTP/2 listener. The CA, certificate, and
private-key options are mandatory. Each node certificate must support both TLS server and client
authentication, contain the advertised DNS name or IP address, and contain exactly one identity URI
of the form `nervix://cluster/<cluster-id>/node/<node-id>`. Peers with a different cluster identity
or a certificate identity that disagrees with their protocol identity are rejected. Gossip, Raft,
resource transfer, and relay traffic use independent connection pools on this listener; bounded
rkyv is the internal control encoding, while relay batches remain Arrow IPC.
The server monitors all three interconnect PEM files. It reloads a changed bundle only after two
consecutive reads agree and the complete replacement passes certificate, identity, and lifetime
validation; an incomplete or invalid update leaves the active bundle in service and is retried.

OpenTelemetry trace export is optional and uses the existing `tracing` instrumentation:

- `--otel-enabled` or `NERVIX_OTEL_ENABLED=true` enables OTLP trace export
- `--otel-otlp-endpoint` or `NERVIX_OTEL_OTLP_ENDPOINT` sets the OTLP gRPC collector endpoint, defaulting to `http://127.0.0.1:4317`
- `--otel-service-name` or `NERVIX_OTEL_SERVICE_NAME` sets the OpenTelemetry service name, defaulting to `nervix`
- `--otel-trace-sample-ratio` or `NERVIX_OTEL_TRACE_SAMPLE_RATIO` sets parent-based trace sampling from `0.0` through `1.0`

The node's trace exporter shares its configured name resolver. Startup, timeout and transport
behavior are described in [Node Trace Export](./name-resolution.md#node-trace-export).

The `just deps` stack includes Quickwit and Jaeger for local trace storage and viewing. Quickwit receives OTLP traces on host port `4317`, and the Jaeger dashboard at `http://127.0.0.1:16686` is configured to query Quickwit as its trace backend.

The observability listener exposes health and graph metrics:

- `/livez` reports process liveness
- `/readyz` reports readiness once a leader is known
- `/metrics` reports raw Prometheus text metrics for graph nodes and relays

`DESCRIBE` commands include the same graph-node and relay metric labels, plus local derived values such as counter rates and histogram percentiles. See [Metrics And Observability](metrics-and-observability.md) for the metric families, labels, and rate semantics.

## Local Multi-Node Setup

```bash
just cluster-dashboard
```

The zellij dashboard seeds the `default` user once with the local password `nervix` and exports the
same password for the interactive client pane. Override it by setting `NERVIX_PASSWORD` or
`NERVIX_INIT_DEFAULT_USER_PASSWORD` before running the dashboard. If the local dashboard state was
already initialized with a different password, run `just reset-local-dashboard-state` before starting
fresh.

## Start The Interactive Client

The separate interactive client crate and executable are both named `nervix-cli`. Its options and
interactive behaviour are documented in [Command Line Client](client-tools-cli.md).

```bash
just client
```

The client connects as `default` unless `--username` or `NERVIX_USERNAME` is set. Pass
`--password`, set `NERVIX_PASSWORD`, or let the client prompt interactively.

Direct subscription example:

```bash
just client subscribe notifications
```

## Start Local Broker Dependencies

```bash
just deps
```

The local dependency stack includes broker and service containers used by the documented examples,
including Kafka, Pulsar, RabbitMQ, Redis, MQTT, ClickHouse, Postgres, MySQL, MongoDB, RustFS,
Prometheus, Quickwit, Jaeger, and a Sentry-compatible Bugsink service.

The local Sentry-compatible service is available at `http://127.0.0.1:18090`. Sign in with
`admin@example.org` / `admin`, create a project, and copy its DSN into a `TYPE SENTRY` client used
by a Sentry emitter. These credentials and the Compose `SECRET_KEY` are development-only.

RustFS provides the local Rust-written S3-compatible target for Iceberg emitters:

- S3 endpoint: `http://127.0.0.1:9900`
- console: `http://127.0.0.1:9901`
- access key: `rustfsadmin`
- secret key: `rustfsadmin`
- bucket: `nervix-iceberg`

The compose stack also starts `fake-gcs` for GCS API emulation and `azurite` for Azure Blob API emulation:

- GCS endpoint: `http://127.0.0.1:4443`
- GCS bucket: `nervix-iceberg`
- Azure Blob endpoint: `http://127.0.0.1:10000/devstoreaccount1`
- Azure Blob container: `nervix-iceberg`
- Azure Blob development account: `devstoreaccount1`

The current Iceberg OpenDAL adapter honors custom GCS service endpoints, so `fake-gcs` can be used for local GCS tests. Azure Blob support is exposed through the adapter's ADLS/Blob URL forms (`wasb://` and `wasbs://`); the pinned adapter derives its endpoint from the storage URL and does not yet honor Azurite's path-style local endpoint, so Azurite is available in compose for blob-client work but Iceberg Azure local integration needs an adapter patch or upstream endpoint support.

Iceberg emitters stage local batch files under `/tmp` by default before committing them to blob storage. Use `--temp-dir` or `NERVIX_TEMP_DIR` to place runtime temporary files elsewhere.

## Prometheus Local Check

```bash
curl --get 'http://127.0.0.1:9090/api/v1/query' \
  --data-urlencode 'query=label_replace(vector(42.5), "source", "local", "", "")'
```

## OpenTelemetry Local Check

Start Nervix with `--otel-enabled` and keep the default OTLP endpoint when `just deps` is running. Open `http://127.0.0.1:16686`, select the `nervix` service, and search for traces. Quickwit is also available at `http://127.0.0.1:7280`.

## Connector Crates

[Connector Crates And The Connector Contract](./connector-contract.md) is the architecture
reference for ownership, source and sink lifecycle, and the cross-layer checklist for adding an
integration. The notes here identify the repository locations and validation commands.

The shared contract is in `crates/connector`, and each external integration is in
`crates/connectors/<name>`. The server's `src/runtime/ingestors` and emitter modules compose the
typed plans with those crates. Keep driver dependencies in their integration crates; the server
test harness may name a driver in `[dev-dependencies]` to provision or inspect an external system.
The node's own endpoint source lives in `src/runtime/ingestors/endpoint.rs` because it has no
external driver. Follow the architecture chapter's [integration checklist](./connector-contract.md#adding-an-integration)
when extending this layout.

## Validation And Tests

For repository-wide validation:

```bash
just validate
```

CI installs the latest `just` release. `just validate`, `just validate-ci`, `just lint` and
`just cargo-clippy` default to four concurrent recipe bodies across the `[parallel]` groups.
Pass a job count, such as `just validate 2`, or use `just --jobs 2 validate` to select another
limit. `JUST_JOBS` also supplies the default. Validation runs the product and compiler tooling
Clippy recipes in one invocation, so they share the same limit.

Each package and configuration is a separate parallel dependency of one shared Clippy recipe.
The `[parallel]` groups map package lists onto that recipe, including the packages checked with
`--all-features` and the compiler tooling packages. Feature, target, profile and compiler selections
stay separate, so mutually exclusive execution modes never share a Cargo invocation.

Clippy uses `clippy/<package>/<configuration_hash>` beneath `CARGO_TARGET_DIR`, or the repository's
`target` directory when that variable is unset. The hash includes the Cargo arguments and compiler,
so each configuration has its own build directory lock. `just cargo-clippy-package <package>
[arguments ...]` runs a focused check of all targets through the same recipe. The compiler driver
builds keep their separate `typed-ratchet/driver` directory. All builds retain the configured kache
wrapper. Cargo's compiler job limit applies separately within each invocation. On CI, where nothing
reads a build directory after its lint and the runner's disk cannot hold every configuration's at
once, a target that passed deletes its own; a failed target keeps its directory, and a local run
keeps them all for the next one.

`just validate-nspl-docs` scans `docs/src` and parses every exact `nspl` code fence directly with
the parser crate. Use `nspl,ignore` only for loose grammar synopses and statement fragments that
are intentionally not complete NSPL scripts; the fence remains identified as NSPL in the rendered
documentation.

For test runs:

```bash
just test
```

For representation properties and sanitizer fuzzing, see
[Property Testing And Fuzzing](./property-testing-and-fuzzing.md). The focused entry points are
`just test-bolero [filter]`, `just fuzz-list` and
`just fuzz <target> [duration]`. These commands and the dedicated Bolero workflow own target
discovery; repository validation does not run Bolero checks. `just help` lists the recipes.

Native CI builds for validation, unit tests, scenarios, client conformance, Turmoil, Bolero and
extra checks use Clang 23 with Wild. Cargo's `x86_64-unknown-linux-gnu` linker points to
`scripts/ci_linker.sh`, which selects Wild independently of `RUSTFLAGS` and
`CARGO_ENCODED_RUSTFLAGS` supplied by coverage, simulation or sanitizer tooling. Browser targets
use their own linker. `just validate-workflows` checks the Actions workflow definitions.

Connector crates have a focused recipe. `just test-connectors` runs the unit tests of the
`nervix-connector` contract crate and of every `nervix-connector-*` integration crate. Arguments are
passed to the test binaries, so a test name filter narrows the run:

```bash
just test-connectors <filter>
```

### Compiler synchronization gate

`just ratchet`, `just validate` and `just validate-ci` run the same source-driven compiler
synchronization gate in the isolated `tools/nervix-lint` workspace. Product builds
remain stable. Tooling, analysis and product artifacts occupy separate target directories.
Prepare the ordinary server build prerequisites, including `just build-web-console`.

```bash
just ratchet
just typed-ratchet --show
just typed-ratchet --configuration shuttle --inventory
just test-typed-ratchet
just test-typed-ratchet-docs
just qualify-typed-ratchet-cache
```

Declare `#[cfg_attr(nervix_lint, nervix::context(...))]` at the source's owning execution boundary.
Choose recurring, lifecycle, observer, outside or bounded; every kind requires a reason, and
bounded protocols also require their identity key and bound. Trait/type/module inheritance and
local call relationships supply defaults. Hot callers of lifecycle-only helpers, unknown dispatch,
invalid annotations and recurring acquisitions produce ordinary Rust diagnostics. Use a narrow
`nervix_primitives::expect_lint!` with a meaningful reason and owning repair task for retained debt.
An expectation covering multiple operations or no operation fails. See
[Data-Plane Concurrency](data-plane-concurrency.md#source-contracts) for the complete authoring
contract, inheritance, supported call effects and limitations.

The JSON output records resolved APIs, spans, owner variants, expansions, source contracts,
compiled configurations and generated/external exclusions. It is generated evidence under
`target/`, not an approval database. Partial selections are inventories; the required diagnostic
gate needs the complete matrix and complete current compiler/Cargo evidence, including zero.

The workspace wrapper nests beneath configured kache. Completion identities cover current source,
annotations, rule code, compiler, driver, validator, dependencies, Cargo configuration and flags.
Cargo-fresh artifacts reuse matching complete side reports. Missing reports establish analysis
using `KACHE_KEY_SALT` and rebuild only isolated authored artifacts. `--fresh` reruns Cargo;
`--recompile` discards those authored artifacts to execute the callback. Every path preserves
`RUSTC_WRAPPER`. Changing a caller, callee, annotation or source location needs only a new run.

Cache qualification exercises fresh and Cargo-fresh analysis, an actual kache dependency hit,
changed source/annotation/rule/dependency/configuration inputs, missing and interrupted output,
fixture doctests twice, and cross-worktree rejection in two isolated worktrees. Kache 0.28 runs
workspace-wrapper chains directly while caching ordinary dependencies; complete evidence is
validated independently of that detail. Compiler semantic fixtures qualify resolved lints and
cross-crate metadata.

### Capability doctests

`just test-capability-docs` checks runtime state handles, logical and physical deadlines,
subscription predicates, and the UDF execution context with `compile_fail` doctests beside their
APIs. Paired compiling examples check that the imports and current signatures remain valid. These
checks cover the specific rejected operations in each snippet. They run in `just test` and CI;
the runtime examples require the `testing` feature, which the recipe enables.

### Deterministic concurrency checks

Run every registered Shuttle check of the execution, interconnect, Rust client and server crates
with:

```bash
just test-shuttle
```

Each check runs in its own process under the exploration it declares, then again under the
uncontrolled-nondeterminism detector. `crates/model-harness/shuttle-inventory.toml` registers every
check by package and test name, and the run fails when a registered check is missing, ignored or did
not complete its exploration, and when a `shuttle_` test is not registered; add a new check to its
package's list, and rename a renamed one there, in the same change. The command reports how many
checks it discovered, selected, executed and saw complete. A substring selects a focused check or
protocol family in every package, and a substring that selects nothing fails:

```bash
just test-shuttle force_flush
```

On failure, the runner keeps the schedule Shuttle persisted, the run's output and its metadata below
`target/shuttle-failures/<package>/<fully-qualified-test-name>/`. Pass the schedule file to the
replay recipe; its parent directories identify the exact package and check:

```bash
just test-shuttle-replay target/shuttle-failures/<package>/<fully-qualified-test-name>/<schedule-file>
```

Keep the schedule with the failure report while fixing the owning protocol, then run the focused
check and the full suite. `just test-shuttle-replay-check` proves persistence and replay without
changing a protocol: it fails one check deliberately after its invariant held, through
`SHUTTLE_FORCE_FAILURE=1`, and requires the one schedule it persisted to reproduce the failure in a
fresh process. Use `SHUTTLE_REPORT_STEPS=1` to inspect the highest explored step count when setting
a check's iteration and step budgets. [Data-Plane Concurrency](./data-plane-concurrency.md) defines
what these checks model, their limits, and the invariant held by each protocol.

### Memory-ordering models

Explore every registered Loom model of a production owner to exhaustion, each in its own process:

```bash
just test-loom
```

A substring of a model's test name or invariant selects it, and a filter that selects nothing
fails:

```bash
just test-loom cancellation.publication
```

The run fails when a registered invariant is missing, ignored or did not complete its exploration,
and when a `loom_*` test is not registered in `crates/model-harness/loom-inventory.toml`. A failed
model leaves its Loom checkpoint, output and `metadata.json` below
`target/loom-failures/<package>/<test>/`; replay it with location tracking and tracing enabled:

```bash
just test-loom-replay target/loom-failures/<package>/<test>
```

`just test-loom-qualification` applies each registered weakening to a copy of the working tree and
requires its model to fail.

### Primitive boundary and execution modes

Every execution-sensitive primitive comes from `nervix-primitives`, and every mode is its own build.
`just validate` runs the checks that keep it that way: `just validate-primitive-boundary` for import
origin, permissions, manifests, global cfgs, the analysis cfg and release binaries, over every
authored source including the isolated analysis workspace,
`just validate-execution-mode-dependencies` for the ordinary and portable dependency graphs of the
workspace and of every package, and `just validate-execution-mode-conflicts` for the diagnostics of
combined modes, of a mode or the native capability requested for the browser's target, of the
diagnostic mode requested without the native capability, and of a product binary built in a mode it
does not declare. `just lint` lints each mode in its own build, including the Shuttle builds and
checks in `just cargo-clippy-shuttle`, the Loom builds in `just cargo-clippy-loom` and the
diagnostic builds in `just cargo-clippy-deloxide`.

`just test-primitives` builds `nervix-primitives` once per execution mode and runs its conformance
checks: which backend each mode selects, the same contract scripts of every family against the
ordinary libraries and against the Shuttle adapters, the Shuttle checks that a publication between
a read and a waiter's registration is reached, and the check that a Loom build takes the ordinary
libraries for the families Loom does not model. The timer checks measure every timer on a paused
clock in ordinary execution and show that Shuttle's timers are scheduling points whose timeouts a
check triggers, the Turmoil check shows that sockets, name lookup, timers and admitted CPU jobs
belong to the simulated host that uses them, and the Deloxide checks show that the tracked locks keep
the contracts of the locks they replace, including their non-blocking `Debug` and the exact count of
waiters a notification wakes. Run it after changing an adapter or the families a mode provides:

```bash
just test-primitives
```

It runs `just test-primitives-ordinary`, the checks in ordinary native execution, then
`just test-primitives-modeled`, the checks under each other mode's backend, then
`just test-primitives-compile`, the checks that compile rather than run; each also runs alone.
[Data-Plane Concurrency](./data-plane-concurrency.md) defines the primitive boundary, what each
mode observes, and every model's claim.

### Deadlock diagnostics

A diagnostic node is the server built in the `deloxide` mode: every thread-blocking lock is tracked
by a deadlock detector, and the first active deadlock it reports is described on standard error,
recorded as evidence and ends the process with status `3`. Build one, in its own target directory so
it never replaces the ordinary binary, and run it with an evidence directory that already exists:

```bash
just build-diagnostic-server
target/deloxide/debug/nervix-server --deadlock-evidence /var/tmp/nervix-deadlocks ...
```

Run the diagnostic mode's checks with:

```bash
just test-deloxide
```

It builds under `target/deloxide` and runs the deadlock probes, each workload in a disposable
process that must report its real cycle or end cleanly, then the `@deadlock_diagnostics` scenarios
on in-process nodes and real diagnostic server processes of one and three nodes, without retries. It
fails when an invocation executed no check or a scenario did not run and pass, keeps every
invocation's output and the scenario binary's evidence under `target/deloxide/test-deloxide`, and
exits with `124` once its budget, 2,400 seconds by default, expires. Run it after any change that adds
or alters blocking synchronization, a lock's acquisition order, or a lifecycle or ownership path that
uses tracked locks, and record what it covered and what it cannot see.
[Data-Plane Concurrency](./data-plane-concurrency.md#diagnostic-deadlock-detection) describes the
detector, its evidence and the locks it does not track.

### Deterministic network simulation

Run the interconnect's seeded Turmoil simulation, its library checks, and every scenario over its
committed seeds with:

```bash
just test-turmoil
```

Each seed runs twice in fresh processes and both runs must record the same semantic trace. To
iterate on one simulation test, pass its name to the simulation target alone:

```bash
just test-turmoil-simulation transport::relay::relay_reconciliation_and_cancellation_survive_lost_replies --exact
```

A failed seed leaves a JSON record below `target/turmoil-failures/` and prints the command that
replays it in a fresh process with exactly its recorded inputs:

```bash
just test-turmoil-replay target/turmoil-failures/<test>/<case>-seed-<seed>.json
```

The suite reports how many tests each of its invocations and the whole suite discovered, selected,
executed and completed, and keeps each invocation's output under `target/turmoil-suite`.
`tests/turmoil-inventory.toml` registers the tests each invocation runs and leaves ignored: the
suite fails when a registered test did not run or ran ignored, when a simulation test ran
unregistered, and when an invocation executed no test, and a `just test-turmoil-simulation`
selection that matches nothing fails too. `just test-turmoil-sweep` runs every scenario over
sixty-four seeds from 1000 in place of its committed seeds, and `just test-turmoil-replay-check`
proves the record and replay path end to end. [Deterministic Interconnect
Simulation](./interconnect-simulation.md) defines what the simulation controls, its fault model and
limits, the scenario matrix, and how to investigate a failure.

### Source coverage of the native extra checks

The extra checks that execute Nervix code natively in ordinary mode, the compiler synchronization
fixtures, benchmark bodies, primitive boundary conformance and NSPL completion walk, also measure which source
lines they execute. Run them the way CI does:

```bash
just coverage-native-extras
```

Name producers to run only those, one or more of `test-typed-ratchet`, `bench-smoke`, `test-primitives` and
`nspl-completion-walk`:

```bash
just coverage-native-extras nspl-completion-walk
```

A producer runs its check exactly as `just <producer>` does and fails when the check fails.
`rust-toolchain.toml` includes the `llvm-tools` component because only the compiler's own LLVM
tools read its profiles. What the check needs first, such as the web console the server benches
link, is built normally. Before instrumentation, `bench-smoke` fetches the verified ONNX Runtime
artifact and builds the web console. The recipe that executes Nervix code then runs in the
environment `cargo llvm-cov show-env --sh
--no-rustc-wrapper` describes: every crate is compiled with source coverage instrumentation into
`target/native-coverage-build`, and the configured kache wrapper stays in place. The parts of a
check that only compile, target the browser or run under a model checker stay uninstrumented, and
Miri, mutation testing and the Loom weakening qualification never run under this command.

Every run collects into a directory of its own,
`target/native-coverage/<producer>/<mode>/<toolchain>/<attempt>/`. The attempt is the GitHub
Actions run and attempt in CI, and the time and process locally. No run reuses, reads or cleans
another run's directory, so no run can count another's counters. The directory holds:

| Entry | Content |
| --- | --- |
| `lcov.info` | The LCOV report of the repository's sources, with the absolute paths `llvm-cov` writes |
| `completion.json` | The completion record: the verdict and everything it covers |
| `executions.jsonl` | Every executable Cargo ran, with its arguments and build ID |
| `export.log` | The diagnostics of `llvm-profdata` and `llvm-cov`, each bounded |
| `profiles/` and `merged.profdata` | The raw counters and their merge; they stay on the machine |

Cargo runs each instrumented executable through the collector as its runner, which records the
execution and points the counters of the executable, and of every process it starts, at `profiles/`,
one file per process and module. Build scripts and procedural macros execute only when a crate is
compiled rather than served from the cache, so their ordinary build-time counters are discarded.
The compiler fixture is itself an executed check: its loaded procedural macro objects are retained
by build ID, including those in nested Cargo output directories. The producer uses its pinned
compiler's LLVM tools. The report is exported with the toolchain's own
`llvm-profdata` and `llvm-cov` from exactly the executables that ran and the children they started.
It keeps the files inside the repository and leaves out dependencies, harness code below a `tests`,
`examples` or `benches` directory, and code generated below a Cargo target directory.

`completion.json` names the producer and mode, the tested commit and whether the working tree was
modified, the CI run and attempt, the compiler and its LLVM version, the instrumentation flags, the
recipes that make up the check, the executions and child executables selected, the profile files
with the binary IDs they name, the warnings `llvm-cov` printed, and the executable and covered lines
of each package. Its `verdict` reads `running` from the moment the directory exists. It becomes
`complete` only once the whole check passed and the report was written, and `failed` or
`interrupted` otherwise, with a `failure` naming the stage, `prepare`, `instrument`, `run`, `export`
or `finish`, and what went wrong. A collection fails when the recipe ran no executable, when an
executable it ran is gone or was rebuilt before the export, when one wrote no profile, or when a
profile is unreadable or names an executable that is no longer there. Every attempt keeps its
profiles, so a failed one keeps its evidence; remove `target/native-coverage` when the collections
are no longer needed.

Every profile is matched to the executable that wrote it before the export, so a warning from
`llvm-cov` never means a stale profile. When several executables share a function that only some of
them run, such as an inlined dependency function, the copies that never ran can carry a different
hash, and `llvm-cov` warns that they have mismatched data and reads the function from the
executables that ran it.

CI's extra-tests job runs the four checks through this command and uploads the `lcov.info`,
`completion.json`, `executions.jsonl` and `export.log` of every collection as the
`coverage-native-extras` artifact, whatever the verdict. The benchmark bodies run instrumented, so
their Criterion test mode shows that each body executes and measures nothing. The collector itself,
including real instrumented runs of a fixture crate and its refusal of every incomplete collection,
is tested with:

```bash
just test-native-coverage
```

The unit and scenario coverage recipes export `lcov-workspace.info` with every workspace package
selected. CI merges those reports with the native extra reports, checks complexity against that
complete report, and uploads it to Codecov. The separate `lcov.info` report selects the server,
CLI and console for focused inspection; it does not replace the workspace export.
Run the same complexity check locally with `just check-coverage <merged-workspace-report>`.

### The scenario suite's execution budget

The Cucumber suite bounds its own run. A step, a teardown diagnostic or a node stop that never
returns ends the whole run at the budget rather than leaving the process alive until CI cancels the
job, which kills it mid-scenario with no record of what each scenario was doing. When the budget
expires the suite prints every scenario still active with its attempt, its phase, how long it has
been in that phase and the cluster nodes it holds, asks every live node to stop, waits one bounded
cleanup window for them, and exits with status `124`. A passing run still exits `0` and a failing
one still panics, so a wedged suite is told apart from a failing one by the exit status alone.

The default budget leaves the workflow job time for the work that precedes the suite and for the
artifact upload that follows a timeout. Give a run a budget of its own with `--suite-budget` or the
`NERVIX_TEST_SUITE_BUDGET` environment variable, which is how the timeout path is exercised without
waiting out the suite's own budget:

```bash
just test-scenarios --input tests/features/cluster/rejoin.feature --suite-budget 5s
```

The focused regressions that hold the harness's startup, status, teardown and watchdog budgets run
in seconds:

```bash
just test-harness-liveness
```

[Integration Test Lifecycle](./integration-test-lifecycle.md) defines every harness deadline, the
phases a scenario reports, and how each failure reaches CI output.

## Building The Documentation

`just book <version>` renders this book, and `just book-pdf <version>` additionally produces
`nervix.pdf` through pandoc and XeLaTeX.

Both recipes require the exact mdBook release pinned by `MDBOOK_VERSION` in
`scripts/build_book.py`, and they fail with that version in the message when a different one is
installed. CI resolves the same constant, so a local build always matches what is published. The
pin is exact because the rendered theme targets mdBook's internal element IDs and because the PDF
relies on mdBook rewriting print-page links into in-document anchors — on an older release every
cross-chapter link in the PDF would point back at the website instead.

Install the pinned release with:

```bash
cargo install mdbook --version <MDBOOK_VERSION> --locked
```

Building the PDF also needs `pandoc` and a XeLaTeX installation on `PATH`.
