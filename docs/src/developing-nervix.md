# Developing Nervix

*Plexus, the Nervix graph definition language.*

This section is for working on Nervix itself. Everything here assumes a clone of the Nervix
repository and uses its `just` recipes; none of it is required to use Nervix.

## Clone The Repository

```bash
git clone https://github.com/nervix-io/nervix
cd nervix
```

## Prerequisites

Nervix is developed on Linux x86_64, Linux aarch64, and macOS arm64. The `just` recipes fetch an
ONNX Runtime build for the host and stop on any other.

The server uses `ort` 2.0.0-rc.15 with C API level 28. The downloader pins
ONNX Runtime 1.30.0 and installs it under `.nervix-deps/onnxruntime`. Download the CPU package or,
on Linux x86_64, the CUDA 13 package:

```bash
just download-onnxruntime
just download-onnxruntime gpu_cuda13
```

The optional second argument selects an upstream platform: `host` (the default), `linux-x64`,
`linux-aarch64`, or `osx-arm64`. For example, `just download-onnxruntime cpu linux-aarch64`
downloads for Linux ARM64 from another supported development host. CUDA packages are available
only for `linux-x64`. Version and destination are fixed; update the runtime pin together with the
selected `ort` API level.

Install:

- Rust via `rustup`
- `just` (latest release)
- `zellij`

`just build-web-console` builds the console the server embeds. trunk uses a `wasm-bindgen` CLI
from `PATH` when its version equals the one `Cargo.lock` locks, and otherwise downloads it from
GitHub without retrying. So the build installs that exact version with `cargo install --locked`
into `target/tools/wasm-bindgen-<version>`, through the configured compiler wrapper, puts it first
on `PATH` and runs `trunk build --offline`. Cargo checks the crate against the registry index and
retries its own transient network failures. Offline, trunk stops at once when the CLI is missing or
reports another version. The image's `builder` stage runs the same installer,
`python3 scripts/wasm_bindgen.py install`, before its own offline trunk build.

## Build Container Images

```bash
just docker-build-debian
just docker-build-cuda
```

Both recipes require the repository's remote compiler-cache configuration: `KACHE_S3_BUCKET`,
`KACHE_S3_REGION`, `KACHE_S3_ENDPOINT`, `KACHE_S3_ACCESS_KEY`, and `KACHE_S3_SECRET_KEY`. They build
and load a local image by default; the existing tag, platform, push, and BuildKit cache arguments
are available for publishing builds.

The `builder` stage installs its build tools at the exact versions that `ARG`s in
`Dockerfile.debian` pin, and every image build runs that step. kache and trunk come from their
projects' release archives for the build architecture, fetched from the release download URL with
no GitHub API call, and each archive is checked against its pinned SHA-256 before anything is taken
out of it. Both projects build their releases with fat LTO in one codegen unit, which would take
minutes to compile. cargo-auditable and cargo-sonic compile with `cargo install --locked` against
the lockfile each crate published, so a later compatible release of one of their dependencies can
neither change them nor stop them compiling. Moving a tool to another version changes its version
pin and, for an archive, the checksum of each build architecture; an archive that does not match
its checksum fails the build. The kache pin is the `KACHE_VERSION` that CI's kache-backed jobs
install, which the recipes pass to the build.

`Dockerfile.debian` has two complete image targets: `cpu` (the default) and `cuda`. The recipes
select them with `--target cpu` and `--target cuda`, respectively. Both targets copy the same
binaries and Sonic bundle from the shared `builder` stage using `COPY --link`, so those artifact
layers are shared across the images. Each target has its own runtime base and ONNX Runtime
download stage; building the CPU target requires only the CPU stages.

Both images pin their builder, downloader, and runtime base to `debian:trixie-20260918-slim`,
with its multi-platform digest fixed in `Dockerfile.debian`. The Debian image contains the CPU
runtime and supports AMD64 and ARM64. The CUDA image is AMD64 only. It installs pinned CUDA
13.2.2 and cuDNN 9.25.1 packages from NVIDIA's Debian 13 repository in one filesystem layer
directly above the Debian base. The next layer runs `apt-get update` and `apt-get upgrade` before
installing Nervix's dependencies. CUDA packages are held at their pinned versions during that
upgrade. Nervix's user, ONNX Runtime libraries, and binaries follow, so application changes reuse
the large CUDA layer. Both images obtain ONNX Runtime through the same pinned downloader.

The CPU layer order is Debian → Debian updates → Nervix; the GPU layer order is
Debian → CUDA/cuDNN → Debian updates → Nervix. The CUDA layer is built by Nervix and reused
across its image builds.

`just test-onnxruntime` checks package selection and recipe targets. `just test-onnxruntime-docker`
builds both final targets with fixture binaries replacing the compiler stage. It verifies shared
binary layers, Debian upgrades immediately after the selected base, preserved CUDA pins, and
runtime shared-library dependencies without requiring remote compiler-cache credentials. The
host NVIDIA driver is provided at container startup by the NVIDIA Container Toolkit.

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
remove the environment variable or flag from normal startup. Later users can be created through Plexus:

```plexus
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

The dashboard runs three nodes on loopback addresses. `node-1` bootstraps the cluster, and `node-2`
and `node-3` join it through node-1's interconnect listener on `127.0.0.1:47395`. A node certificate
carries exactly one node identity, so `just generate-dev-tls` issues each node its own interconnect
certificate from the development CA: `tls/dev/node.pem` for `node-1`, and `tls/dev/node-2.pem` and
`tls/dev/node-3.pem` for the other two. A run that finds the `node-2` or `node-3` certificate
missing, or not issued by the current CA, issues it again without replacing the CA or `node-1`'s
identity. Started from inside a zellij session, the dashboard opens a zellij session of its own in
the current pane, because the panes of the outer session would not see the binaries and passwords
it exports.

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
`admin@example.org` / `admin`, create a project, and copy its DSN into a `SENTRY CLIENT` used
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
reference for ownership, ingestor and emitter lifecycle, and the cross-layer checklist for adding an
integration. The notes here identify the repository locations and validation commands.

The shared contract is in `crates/connector`, and each external integration is in
`crates/connectors/<name>`. The server's `src/runtime/ingestors` and emitter modules compose the
typed plans with those crates. Keep driver dependencies in their integration crates; the server
test harness may name a driver in `[dev-dependencies]` to provision or inspect an external system.
The node's own endpoint ingestor lives in `src/runtime/ingestors/endpoint.rs` because it has no
external driver. Follow the architecture chapter's [integration checklist](./connector-contract.md#adding-an-integration)
when extending this layout.

## Validation And Tests

For repository-wide validation:

```bash
just validate
```

The `checks`, `tests`, and `scenarios` CI jobs install the Go formatters through
`scripts/install_go_ci_tool.py`: dockerfmt is pinned to v0.5.4 and ghokin to v3.9.0. The installer
uses Go 1.26 in each job, keeps the public Go checksum database enabled, and disallows an implicit
toolchain download. It retries only a transport failure while reading that database, at most three
attempts within eight minutes. A checksum mismatch, compiler failure, or invalid module version
fails on the first attempt. Each attempt is retained in the job's
`go-tool-bootstrap-<job>` artifact. Run `just test-go-ci-tool` to exercise the classifier and
workflow wiring without network access.

CI installs the latest `just` release. `just validate`, `just validate-ci`, `just lint` and
`just cargo-clippy` default to four concurrent recipe bodies across the `[parallel]` groups.
Pass a job count, such as `just validate 2`, or use `just --jobs 2 validate` to select another
limit. `JUST_JOBS` also supplies the default. Validation runs the product and compiler tooling
Clippy recipes in one invocation, so they share the same limit.

Each package and configuration is a separate parallel dependency of one shared Clippy recipe.
The `[parallel]` groups map package lists onto that recipe, including the packages checked with
`--all-features` and the compiler tooling packages. Feature, target, profile and compiler selections
stay separate, so mutually exclusive execution modes never share a Cargo invocation.

Clippy builds in four directories, `clippy/0` to `clippy/3` beneath `CARGO_TARGET_DIR`, or the
repository's `target` directory when that variable is unset. The SHA-256 of a configuration's
package, Cargo arguments and toolchain, modulo four, selects its directory, so each run of a
configuration returns to the directory that holds its artifacts. The configurations in a directory
share the dependencies they build, build scripts included, and Cargo keeps the artifacts of
different features, profiles, targets and flags, and of the stable and nightly compilers, apart
there. Configurations in the same directory take turns on its build directory lock, so two of them
selected at once run one after the other. Nothing deletes these directories, locally or on CI: the
four of them bound what a lint leaves on disk. `just cargo-clippy-package <package> [arguments ...]`
runs a focused check of all targets through the same recipe. The compiler driver builds keep their
separate `typed-ratchet/driver` directory. All builds retain the configured kache wrapper. Cargo's
compiler job limit applies separately within each invocation.

`just validate-plexus-docs` scans `docs/src` and parses every exact `plexus` code fence directly with
the parser crate. Use `plexus,ignore` only for loose grammar synopses and statement fragments that
are intentionally not complete Plexus scripts; the fence remains identified as Plexus in the rendered
documentation.

For test runs:

```bash
just test
```

For representation properties and sanitizer fuzzing, see
[Property Testing And Fuzzing](./property-testing-and-fuzzing.md). The focused entry points are
`just test-bolero [filter]`, `just fuzz-list` and
`just fuzz <target> [duration]`. These commands and the dedicated Bolero workflow own target
discovery; repository validation does not run Bolero checks. In CI, that workflow runs the
sanitizer campaigns, the preferred Bolero evidence, for a pull request labeled `fuzz`, and the
randomized properties for one labeled `bolero-random`, which a pull request carries only when its
task requires that run. `just help` lists the recipes.

Native CI builds for validation, unit tests, scenarios, client conformance, Turmoil, Shuttle,
Loom, Bolero and extra checks use Clang 23 with Wild. Cargo's `x86_64-unknown-linux-gnu` linker
points to `scripts/ci_linker.sh`, which selects Wild independently of `RUSTFLAGS` and
`CARGO_ENCODED_RUSTFLAGS` supplied by coverage, simulation or sanitizer tooling. Browser targets
use their own linker. `just validate-workflows` checks the Actions workflow definitions.

Connector crates have a focused recipe. `just test-connectors` runs the unit tests of the
`nervix-connector` contract crate and of every `nervix-connector-*` integration crate. Arguments are
passed to the test binaries, so a test name filter narrows the run:

```bash
just test-connectors <filter>
```

### CI runners

CI jobs run on [Boat](https://boat.dev) machines, which the boat CI GitHub App starts for every job
whose `runs-on` names a Boat size. Each job gets a fresh x86_64 Ubuntu 24.04 virtual machine with
Docker, deleted when the job ends. `.github/actionlint.yaml` declares the Boat labels to
`just validate-workflows`. A job's size follows the memory and disk its builds use rather than its
CPUs, since a Boat machine has only 2 GB of memory per CPU:

- `boat-xlarge`, with 16 CPUs, 32 GB and 251 GB of disk, runs validation, the unit tests, the
  scenarios, client conformance, the extra checks, Shuttle, Loom, the Deloxide lane, Bolero, the
  arm64 and diagnostic image builds and the benchmarks. In Check runs of October 2026 the Loom jobs
  used about 20 GiB of memory, the scenarios up to 28 GiB and the extra checks about 95 GiB of disk.
- `boat-large`, with 8 CPUs, 16 GB and 111 GB, runs the coverage reports, Turmoil, the book, the
  amd64 image build and deployment check, and the chaos shards.
- `boat-small`, with 2 CPUs and 4 GB, runs the jobs that only select, plan, gate or publish.

Boat has no arm64 machine, so the arm64 deployment check runs on GitHub's hosted `ubuntu-24.04-arm`
runner. A Boat machine sets the `en_US.UTF-8` locale, whose collation orders `sort` output
differently from the `C.UTF-8` of GitHub's runner image, so every workflow sets `LC_ALL=C.UTF-8`.
Its Docker daemon can start a few seconds after a job begins, so a job whose first steps use Docker
waits for it. Its own Node.js comes from nvm, which a job's shell may not load, so a job that runs
npm pins Node.js with `actions/setup-node`.

### Compiler architecture gate

`just ratchet`, `just validate` and `just validate-ci` run the same source-driven compiler
architecture gate in the isolated `tools/nervix-lint` workspace. Product builds
remain stable. Tooling, analysis and product artifacts occupy separate target directories.
Install the declared toolchains with `just toolchains-install`, including the pinned analysis
compiler's `wasm32-unknown-unknown` standard library for browser analysis. Prepare the ordinary
server build prerequisites, including `just build-web-console`.
When compiler reports need reanalysis, cleanup selects the configuration's target as well as its
isolated build directory, so browser artifacts cannot remain fresh without their current reports.

```bash
just ratchet
just typed-ratchet --show
just typed-ratchet --configuration shuttle --inventory
just test-typed-ratchet
just test-typed-ratchet-docs
just qualify-typed-ratchet-cache
just lint-audit
```

`just lint-audit` reads the same complete evidence and reports review candidates for code shaped
around an inaccurate lint, each attributed to the commits and debt counts that introduced it.
[Architecture Rules And The Debt Ratchet](architecture-enforcement.md#auditing-lint-workarounds)
states the candidates, the attribution and the review contract.

The complexity verdict is the ratchet's `functions_over_complexity_30` count: the functions that
the pinned cargo-crap scores above cyclomatic complexity 30. `just ratchet` builds that version
first with `just install-cargo-crap`, into `target/tools/cargo-crap-<version>/` through the
configured wrapper, and refuses a binary of another version. A new function above 30 raises the
count and fails `just validate` and CI's `checks` job, which name it among the listed sites:

```bash
just install-cargo-crap
just ratchet --show functions_over_complexity_30
```

Decompose the function rather than moving the number. The
[function complexity](architecture-enforcement.md#function-complexity) section states the
analyzer's scope, its limits and every failure that refuses an incomplete analysis.

Declare `#[cfg_attr(nervix_lint, nervix::context(...))]` at the source's owning execution boundary.
Choose recurring, lifecycle, observer, outside or bounded; every kind requires a reason, and
bounded protocols also require their identity key and bound. Trait/type/module inheritance and
local call relationships supply defaults. Hot callers of lifecycle-only helpers, unknown dispatch,
invalid annotations and recurring acquisitions produce ordinary Rust diagnostics. Use a narrow
`nervix_primitives::expect_lint!` with a meaningful reason and owning repair task for retained debt.
An expectation covering multiple operations or no operation fails. See
[Data-Plane Concurrency](data-plane-concurrency.md#source-contracts) for the complete authoring
contract, inheritance, supported call effects and limitations.

The same gate checks contextual failure contracts, explicit outcome handling and canonical panic
APIs through `nervix::bare_error_signature`, `nervix::discarded_outcome` and `nervix::bare_panic`.
Aliases, associated outputs, generic calls, inferred closures and boxed future/stream returns
are resolved by the compiler. Ordinary values and custom same-named methods keep their own
contracts. Classify a semantic outcome or modeled library contract with a reason-bearing
`nervix::error_boundary(outcome|library, reason = "...")` on its exact error type or return
contract. These classifications do not inherit module defaults. Standard-error hooks retain a
complete report carrier. [Errors And Diagnostics](errors-and-diagnostics.md) owns the classification
rules and the current carriers. `just validate-typed-errors` separately guards textual
`Result<_, String>` source, including inactive forms.

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
fixture doctests twice, and cross-worktree rejection in two isolated worktrees. Kache runs
workspace-wrapper chains other than Clippy's directly while caching ordinary dependencies; complete
evidence is validated independently of that detail. Compiler semantic fixtures qualify resolved
lints and cross-crate metadata.

The same gate enforces source-owned architectural layers, configuration Model consumption,
language references, awaited cluster access and declared identity/installation state. Declare
`nervix::layer` at documented ownership boundaries, `nervix::input(plans, ...)` on connector engines,
and exact `value`, `identity` and `state` roles at their semantic owners. The
[architecture contracts](./data-plane-concurrency.md#resolved-architecture-boundaries) and
[typed-state contracts](./typed-states.md#source-owned-identity-and-installation-contracts) state the
supported scope. `just test-typed-ratchet-contracts <unittest selector...>` runs focused pass/fail
compiler fixtures after preparing `just test-typed-ratchet-compiler`. `just test-ratchet-units`
also qualifies language provenance before resolution; `python3 -m scripts.check_layer_provenance`
prints current source/manifest violations. Calibration logs and complete matrix/coverage outputs
belong on the owning task, not in the source tree. Compilation under a modeled mode qualifies
recognition and supplies no concurrency execution evidence.

### Typed errors and reports

`just validate` rejects a `Result<_, String>` in product code and a signature that returns a Nervix
failure without an `error-stack` report. Run the distinct source check, the complete compiler gate,
its semantic fixtures and the structural scanner tests with:

```bash
just validate-typed-errors
just ratchet
just test-typed-ratchet-contracts
just test-ratchet-units
```

[Errors And Diagnostics](./errors-and-diagnostics.md#adding-a-fallible-operation) holds the
checklist for a new fallible operation: which layer owns the failure, what its context carries,
where the report ends, how it is rendered and which evidence holds it.

### Capability doctests

[Execution Primitives](./execution-primitives.md#verification-contracts) distinguishes API
doctests, Cargo graph and target probes, compiler fixtures, adapter conformance and actual
production-owner model execution. Keep each verdict with the configuration and claim it checks.

`just test-capability-docs` checks runtime state handles, logical and physical deadlines,
subscription and VM predicates, the UDF execution context, consensus capabilities, DNS error
carriers, recovery outcomes and SIMD byte classes with `compile_fail` doctests beside their APIs.
Paired compiling examples check that the imports and current signatures remain valid. These
checks cover the specific rejected operations in each snippet. They run in `just test` and CI;
the runtime examples require the `testing` feature, which the recipe enables.

### Deterministic concurrency checks

Run every registered Shuttle check of the execution, interconnect, Rust client and server crates
with:

```bash
just test-shuttle
```

Each check runs in its own process under the exploration it declares, then again under the
uncontrolled-nondeterminism detector. Checks run beside each other, one per CPU the command may use,
so restrict its CPUs, for example with `taskset -c 0-3 just test-shuttle`, to share a busy machine.
`crates/model-harness/shuttle-inventory.toml` registers every check by package and test name, and
the run fails when a registered check is missing, ignored or did not complete its exploration, and
when a `shuttle_` test is not registered; add a new check to its package's list, and rename a
renamed one there, in the same change. The command reports how many checks it discovered, selected,
executed and saw complete. A substring selects a focused check or protocol family in every package,
and a substring that selects nothing fails:

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

CI runs the checks and the replay check only for a pull request labeled `shuttle`. Label a pull
request that changes a protocol a Shuttle check holds.

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
`target/loom-failures/<package>/<invariant>/`; replay it with location tracking and tracing enabled:

```bash
just test-loom-replay target/loom-failures/<package>/<invariant>
```

`just test-loom-qualification` applies each registered weakening to a copy of the working tree and
requires its model to fail. The copy lives below `target/loom-qualification-build/` and builds into a
target directory of its own there, so a weakened build never stands in for the working tree's.
Every file the copy takes or restores gets a fresh modification time, so each weakening rebuilds only
what it changed, and qualifications that register the same weakening share one weakened build. The
server's models embed the built web console, which the copy links rather than copies; the Loom
recipes build it first. The copied sources are removed when the qualification ends, and the target
directory stays, so a later run reuses the dependencies it built. The copy compiles incrementally,
because its weakened builds serve that run alone, so a weakening recompiles what it changed rather
than the whole crate. `just test-loom-qualification NUMBER/COUNT` qualifies one shard of the distinct
weakenings, the ones at the inventory positions that leave `NUMBER - 1` when divided by `COUNT`; the
default, `1/1`, qualifies them all.

CI runs the models and their qualification only for a pull request labeled `loom`. Label a pull
request that changes an ordering a Loom model holds.

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

#### Integrated primitive qualification

At a release candidate revision, run the ordinary suite and the complete primitive, Shuttle,
Loom and Turmoil inventories. Keep the replay and weakening checks separate from successful model
exploration: they show that recorded failures reproduce and that the asserted ordering matters.
Run the applicable public Cucumber scenarios at the same revision. An empty selection, incomplete
inventory or timed-out exploration is a failed qualification, even if another mode passed.

The external smoke and soak suites use a fresh ordinary image built from that revision. The suite
resolves the image once and gives every scenario the same image identity; its `suite.json` and each
run's manifest prove which image, fault action, accepted events and recovery outcome were observed.
For pull-request CI, select both `chaos` and `chaos-soak` when qualifying the complete matrix; the
labels run the suites against that pull request's own image. A nightly run of a different commit
does not qualify the candidate. A recorded replay reproduces the external action plan, not the
operating system's thread schedule.

Measure the ordinary, uninstrumented build on the same host and profile as its baseline:

```bash
just bench-primitives
just bench-relay-interaction
just bench-admitted-work
just bench-task-handles
```

`bench-primitives` times atomic access, publication loads and stores, an uncontended blocking
lock, and shared-reference cloning. The relay and admitted-work suites include their production
owners; `bench-task-handles` retains raw timing and allocation samples. Record the revisions,
host/load, toolchain, exact commands, raw results and comparison on the ClickUp task. Keep model
evidence, real-process Chaos evidence and performance measurements distinct; none establishes the
claim of another method. Generated qualification reports and logs belong on the task rather than
in the repository. A pull request labeled `benchmark` runs the product comparison and the ordinary
primitive probe on its benchmark worker and retains the probe output with the benchmark artifacts.

### Deadlock diagnostics

Diagnostic builds select `deloxide` for active cycles, `deloxide-order` for active and historical
order analysis, or `deloxide-stress` for active cycles under Deloxide's bounded scheduling
disturbance, which delays one in twenty of the acquisitions a thread makes while it holds a tracked
lock by 20µs to 200µs. Build and run one with an existing local evidence
directory:

```bash
just build-diagnostic-server deloxide-order
target/deloxide/debug/nervix-server --deadlock-evidence /var/tmp/nervix-deadlocks ...
```

The order build's `--deadlock-active-only` option disables checking at runtime but keeps compiled
instrumentation and its cost. Ordinary product performance is measured separately. An active cycle
exits `3`; a potential cycle retains evidence and continues; lost evidence or failed diagnostics
exit `4`.

Run every selection of the diagnostic lane after any change that adds or alters thread-blocking
synchronization, the order of tracked acquisitions, or a shutdown, drain, restore, cancellation,
handoff or other lifecycle path that uses tracked locks:

```bash
just test-deloxide
just test-deloxide-order
just test-deloxide-stress
just test-deloxide-stress-restore
just deadlock-report inspect target/deloxide/test-deloxide/deloxide-order/run.XXXX/evidence/scenarios/deadlock-PID-TIME.rkyv --source potential
```

The lane runs every workload `tests/deloxide-inventory.toml` registers for its selection: the
disposable-process probes, the tracked locks' conformance checks, the diagnostic owner tests and the
tagged one- and three-node scenarios, without retries. The two stress selections compile one
build: `deloxide-stress` runs the probes, conformance checks, owner tests, lifecycle scenarios and
paced-driver scenarios under the disturbance, and `deloxide-stress-restore` its capture, restore
and checkpoint-transfer scenarios, one at a time in a lane of their own. A scenario that joins the
active-only selection also joins the stress selection of its family, tagged `@deloxide_stress` or
`@deloxide_stress_restore`, under a `stress.` identity. It fails when a registered workload is
missing or ignored, when a probe, conformance check, owner test or tagged scenario is not
registered, when a process deadlocks, fails its diagnostics, is killed by a signal, outlives its
bound or leaves a process behind, and when evidence is missing, partly written or does not qualify.
An active deadlock exits `3`, a diagnostic failure `4`, a timeout or the expired budget `124`, and
every other failure `1`. A potential cycle fails the lane until its order is corrected; the lane
keeps no list of approved cycles.

Each run keeps a fresh attempt under `target/deloxide/test-deloxide/<selection>/` with every log,
probe artifact, evidence file and finding description, and `lane.json` with the exact command,
environment, bound and ending of every launch. Replay one recorded launch in a fresh attempt, and
prove the lane's supervision on deliberately failing processes:

```bash
just test-deloxide-replay target/deloxide/test-deloxide/deloxide/run.XXXX/lane.json scenarios
just test-deloxide-qualification deloxide-order
```

A new probe, conformance check, owner test or tagged scenario joins the inventory with a stable
identity, the invariant it owns, its selections and the coverage it declares; a renamed one is
renamed there in the same change. A source file that starts acquiring tracked blocking locks needs
an `[[owner]]` record naming the workloads that reach them or the path the lane does not reach and
why: `just validate-deloxide-applicability`, part of `just validate`, holds the records to the
compiler's acquisition catalog. For a pull request labeled `deloxide`, which every change the
Deloxide rule applies to carries, CI's `deloxide` job runs every selection through the native
coverage collector, as `just coverage-native-extras test-deloxide test-deloxide-order
test-deloxide-stress test-deloxide-stress-restore` does locally,
and keeps their attempts, completion records and diagnostic coverage reports as artifacts. The
ordinary report tool also exports selected artifacts, records explicit proofs in reviewed copies and
qualifies whole-process evidence. `just coverage-deadlock` measures the diagnostic implementation
and the ordinary report command in separate instrumented builds through the native collector.
For applicable work, record the tested revision and inventory digest, each selection's complete
workload accounting and artifact path, and the registered workloads that reach the changed owner
on the task and pull request. A green job from an earlier revision cannot qualify a changed
owner. For async-only or otherwise untracked changes, name the other checks that cover the changed
path; for documentation-only work, record why no diagnostic execution applies. Use the toolchain
files' declared `llvm-tools` for the compiler that produced each coverage profile, retaining the
configured kache wrapper. Registered Bolero properties run as ordinary tests in the ordinary
suites; sanitizer CI, the preferred Bolero evidence, runs only with `fuzz`, the randomized Bolero
job only with `bolero-random`, and a skipped run supplies no execution evidence. API
rejections use paired current `compile_fail` and compiling doctests in normal tests and CI.
[Data-Plane Concurrency](./data-plane-concurrency.md#diagnostic-deadlock-detection) has the lane's
classes, its record, proof requirements, current graph limits and the paths detection does not
cover, and [Integration Test Lifecycle](./integration-test-lifecycle.md#diagnostic-lane) its bounds.

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

CI runs the suite and the replay check only for a pull request labeled `turmoil`. Label a pull
request that changes the interconnect or a seam the simulation plugs into.

### External cluster chaos

`just chaos` runs black-box cluster scenarios against an already built Nervix image through Docker
Compose, Pumba and the image's packaged CLI. It compiles nothing and needs no Rust toolchain. Two
suites run selections of those scenarios, each as its own `just chaos run` command:

```bash
just chaos suite list
just chaos suite smoke --image ghcr.io/nervix-io/nervix@sha256:<digest>
just chaos suite soak --image ghcr.io/nervix-io/nervix@sha256:<digest>
```

The smoke suite runs a baseline, a leader crash with its restart and a partition with its recovery;
the soak suite runs every delivered scenario with fixed and rotating mixed-instability seeds. CI
runs the smoke suite against a pull request's own images only for a pull request labeled `chaos`,
and the soak suite for one labeled `chaos-soak`. The nightly build of `main` runs the soak suite,
and a manual run of the `Chaos` workflow takes a suite, an immutable image digest and its kind.
Label a pull request that changes the chaos runner, or the membership, failover, recovery or
delivery behavior its scenarios verify, with `chaos`.

Each suite runs twice on CI: against the release image and against the Deloxide diagnostic image
of the same revision, which `just docker-build-diagnostic` builds and whose label makes every run
judge the deadlock evidence of its nodes. Locally, `just build-chaos-diagnostic-image` overlays a
release-mode diagnostic server of this checkout on an available image:

```bash
just build-chaos-diagnostic-image nervix:diagnostic-local ghcr.io/nervix-io/nervix:debian-latest deloxide-order
just chaos suite smoke --image nervix:diagnostic-local --image-kind deloxide-order
```

[External Cluster Chaos](./cluster-chaos.md) describes the runner, every scenario and its verdicts,
the suites, diagnostic images, their budgets and artifacts, and how to reproduce or replay a failed
run.

### Release evidence

`tests/release-evidence.toml` declares the evidence a revision needs before it qualifies for
release: the checks every pull request runs, the release images, and each label-gated method with
its label and checks, Bolero's randomized run and its sanitizer fuzzing, Loom and its weakening
qualification, Shuttle, Turmoil, every Deloxide lane selection, client conformance, and the smoke
and soak suites against both the release image and the diagnostic image of the revision.

```bash
just release-evidence <pull-request>
```

The gate reads the pull request's head revision, its labels and every check run GitHub recorded on
that revision. It refuses the revision when a method's label is missing, so CI never ran it; when
the latest run of a required check, or of any shard of a sharded one, is missing, still running,
skipped, cancelled, timed out or failed. The register also lists every gap an owner of tracked
locks declares in `tests/deloxide-inventory.toml`, the path the diagnostic lane does not reach and
why. A declared gap is a reviewed declaration the Deloxide rule permits, so it does not refuse the
revision: `just validate-deloxide-applicability`, part of the required `checks` job, holds the
records to the compiler's acquisition catalog, and the register shows each declared limit of the
diagnostic evidence to whoever qualifies the release. A rerun of a failed job supersedes the run it
repeats; a passing rerun of a nondeterministic failure still needs the failure's own investigation,
which the gate cannot see. The register of every check's
verdict, run and time goes to `target/release-evidence/<revision>/` and belongs on the task that
qualifies the revision, not in the repository. `just test-release-evidence` exercises the gate on
recorded check runs and reads the Deloxide lane and release image matrices from the workflows, so
it fails when one of their jobs is missing from the inventory. A renamed CI job, or a value added to
the matrix of one the inventory names, is updated in the inventory in the same change.

### SIMD kernel development

[SIMD Kernels](./simd-kernels.md) owns the typed-buffer admission rule, caller catalog, dispatch
and build-target matrix, result guarantees, and recorded performance evidence. New kernels need
generated-instruction inspection, complete scalar/level differential checks including forced
fallback, and a measured caller. Use its [qualification recipes](./simd-kernels.md#qualification)
and [measurement recipes](./simd-kernels.md#measurement-and-recorded-evidence), retaining kache.
Registered Bolero properties run as ordinary tests in the ordinary suites; sanitizer CI, the
preferred Bolero evidence, requires the `fuzz` label, the randomized Bolero job the `bolero-random`
label, and a skip supplies no execution or coverage evidence. API-rejection checks use paired
current `compile_fail` and compiling doctests. Publish new benchmark and validation artifacts on
the corresponding ClickUp task; keep durable architectural conclusions in the chapter.

### Coverage reports and complexity

[Coverage Reports And The Complexity Gate](./coverage-reports.md) owns the ordinary producer
catalog, native collection, source mapping, report integrity, advisory patch coverage and CRAP,
the full workspace union, and their relationship to the deterministic complexity ratchet. Start
with its
[local reproduction commands](./coverage-reports.md#the-ordinary-workspace-report) or
[native collection commands](./coverage-reports.md#native-collection).
When adding execution, follow [producer registration](./coverage-reports.md#adding-or-changing-a-producer)
and keep [independent verification verdicts](./coverage-reports.md#verification-outside-collected-counters)
separate from collected counters. A complete report qualifies its recorded source and selection;
later targets need their own completed execution.

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

Both recipes accept stable mdBook patch releases in the major/minor series selected by
`MDBOOK_VERSION` in `scripts/build_book.py`: the current `0.5.3` pin accepts `0.5.x`, including
`0.5.4`. A different major or minor version, or a malformed version, fails with the supported series
in the message. CI installs the exact pinned release for reproducible publication. The series
requirement protects the theme's element IDs and mdBook's rewriting of print-page links into
in-document anchors, which the PDF uses for cross-chapter links.

Install the pinned release with:

```bash
cargo install mdbook --version <MDBOOK_VERSION> --locked
```

Building the PDF also needs `pandoc` and a XeLaTeX installation on `PATH`.

In CI, the Docker Build workflow's `build-book` job runs `just publish-book`: it renders the book,
its PDF and the Plexus agent index, uploads them under an immutable prefix, repoints the `snapshot`
alias that docs.nervix.io serves and comments the preview's links on the pull request. It runs for
a manual run of the workflow and, for a pull request from a branch of this repository, only when
the pull request carries the `book` label. The book needs no image, so adding the label builds it
at once, and each later push to the labeled pull request builds it again; a newer build of one pull
request replaces an older one before it publishes. Label a pull request `book` only when its task
requires the rendered book, such as a change to the book's build, theme, structure, screenshots or
generated chapters, or a preview a reviewer needs. Every pull request's `checks` job runs
`just test-docs`, the script tests and docs worker tests that `just book` runs first, so a pull
request without the label still checks the book's structure and its tooling.
