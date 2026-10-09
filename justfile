set minimum-version := "1.56.0"
set unstable
set lists

export RUSTUP_AUTO_INSTALL := "0"

# CI builds do not need incremental compilation artifacts.
export CARGO_INCREMENTAL := if env("CI", []) != [] { "0" } else { env("CARGO_INCREMENTAL", []) }

rust_toolchain_version := shell("toml get -r rust-toolchain.toml toolchain.channel")
rustflags := env('RUSTFLAGS', '')
build_mode := "debug"
release_flag := if build_mode == "release" { "--release" } else { "" }
cargo_target_dir := env("CARGO_TARGET_DIR", justfile_directory() + "/target")
turmoil_failures := cargo_target_dir + "/turmoil-failures"
default_jobs := num_jobs() || "4"

# Show the documented recipes, including the Bolero property and fuzz commands.
help:
    just --list

# Start one bounded invocation and retain the caller's repository variable overrides.
[private]
run-with-jobs recipe jobs:
    {{ quote(just_executable()) }} --justfile {{ quote(justfile()) }} --jobs {{ quote(jobs) }} \
        rust_toolchain_version={{ quote(rust_toolchain_version) }} \
        rustflags={{ quote(rustflags) }} \
        build_mode={{ quote(build_mode) }} \
        release_flag={{ quote(release_flag) }} \
        cargo_target_dir={{ quote(cargo_target_dir) }} \
        turmoil_failures={{ quote(turmoil_failures) }} \
        {{ quote(recipe) }}

# Install the qualified CLI version with only the libFuzzer engine.
install-cargo-bolero:
    cargo install --locked --version 0.13.4 --no-default-features --features libfuzzer cargo-bolero

# Server library properties embed the same real console assets as the product.
bolero-deps: build-web-console

# Prepare one declared sanitizer target through the shared build path, without a campaign.
prepare-bolero target: bolero-deps
    BOLERO_BUILD_TIMEOUT_SECONDS=7200 python3 -m scripts.bolero prepare {{ quote(target) }}

# The registry and window properties share this declared server library and feature set.
prepare-archive-counts-fuzz: (prepare-bolero "registry-archived-models")

# Run all registered properties with bounded randomized cases and source-adjacent corpus replay.
test-bolero filter="": bolero-deps
    python3 -m scripts.bolero test {{ quote(filter) }}

# List every compiled, registered Bolero target after checking the inventory.
fuzz-list: bolero-deps
    python3 -m scripts.bolero list

# Run one target through sanitizer-backed libFuzzer. Duration is in seconds.
fuzz target duration="30": bolero-deps
    python3 -m scripts.bolero fuzz {{ quote(target) }} {{ quote(duration) }}

# Run every target through sanitizer-backed libFuzzer. Duration is per target in seconds.
fuzz-all duration="30": bolero-deps
    python3 -m scripts.bolero fuzz-all {{ quote(duration) }}

# Replay the exact saved input through its ordinary property assertion.
fuzz-replay target failure: bolero-deps
    python3 -m scripts.bolero replay {{ quote(target) }} {{ quote(failure) }}

# Minimize a saved failure with libFuzzer and verify the minimized input still fails.
fuzz-reduce target failure: bolero-deps
    python3 -m scripts.bolero reduce {{ quote(target) }} {{ quote(failure) }}

# Compare the inventory, package declarations, test harness and compiled Bolero targets.
validate-bolero: bolero-deps
    python3 -m scripts.bolero validate

# Check the dedicated Bolero workflow with the pinned Actions linter.
validate-bolero-workflow: (validate-workflows ".github/workflows/bolero.yaml")

# Check selected Actions workflows, or all workflows when no paths are supplied.
validate-workflows *workflows:
    go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12 {{ workflows }}

# Qualify nonzero failures, saved crashes, minimization, exact replay and case timeouts.
qualify-bolero: bolero-deps
    python3 -m scripts.bolero qualify

# Exercise the inventory and runner's validation and failure paths.
test-bolero-runner: build-web-console
    python3 -m unittest scripts.tests.test_bolero scripts.tests.test_bolero_coverage

# Measure runner edits without launching unrelated product fuzz campaigns.
coverage-bolero-runner:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p target/bolero
    coverage=(uvx --from coverage==7.11.0 coverage)
    "${coverage[@]}" run --data-file target/bolero/runner.coverage --branch --source=scripts.bolero,scripts.bolero_coverage,scripts.build_web_console,scripts.tests.test_bolero,scripts.tests.test_bolero_coverage -m unittest scripts.tests.test_bolero scripts.tests.test_bolero_coverage
    "${coverage[@]}" lcov --data-file target/bolero/runner.coverage -o target/bolero/python-runner.lcov

# Collect distinct Python runner and live Rust source reports, then qualify failure retention.
# The duration is per product target; CI passes 30 on PRs labeled `fuzz`.
coverage-bolero duration="2": bolero-deps
    #!/usr/bin/env bash
    set -euo pipefail
    coverage=(uvx --from coverage==7.11.0 coverage)
    "${coverage[@]}" erase
    "${coverage[@]}" run --branch --source=scripts.bolero,scripts.bolero_coverage,scripts.build_web_console,scripts.tests.test_bolero,scripts.tests.test_bolero_coverage -m unittest scripts.tests.test_bolero scripts.tests.test_bolero_coverage
    "${coverage[@]}" run --branch -a --source=scripts.bolero,scripts.bolero_coverage,scripts.build_web_console,scripts.tests.test_bolero,scripts.tests.test_bolero_coverage -m scripts.bolero fuzz-all {{ quote(duration) }}
    "${coverage[@]}" run --branch -a --source=scripts.bolero,scripts.bolero_coverage,scripts.build_web_console,scripts.tests.test_bolero,scripts.tests.test_bolero_coverage -m scripts.bolero qualify
    mkdir -p target/bolero
    "${coverage[@]}" lcov -o target/bolero/python.lcov
    "${coverage[@]}" report --fail-under=80

# Qualify the restore installation properties while measuring the runner during focused iteration.
coverage-bolero-restore duration="30": build-web-console
    #!/usr/bin/env bash
    set -euo pipefail
    coverage=(uvx --from coverage==7.11.0 coverage)
    "${coverage[@]}" erase
    "${coverage[@]}" run --branch --source=scripts.bolero -m unittest scripts.tests.test_bolero scripts.tests.test_bolero_coverage
    "${coverage[@]}" run --branch -a -m scripts.bolero test restore-installation
    for target in restore-installation-wire restore-installation-storage; do
        "${coverage[@]}" run --branch -a -m scripts.bolero fuzz "${target}" {{ quote(duration) }}
    done
    "${coverage[@]}" run --branch -a -m scripts.bolero qualify
    mkdir -p target/bolero
    "${coverage[@]}" lcov -o target/bolero/python.lcov
    "${coverage[@]}" report --fail-under=80

build-deps: generate-test-onnx download-onnxruntime build-web-console wasm-processor-guests

tests-deps: build-deps build-nspl-format build-test-cli build-paced-simulation build-deadlock-report

build-deadlock-report:
    CARGO_TARGET_DIR={{ cargo_target_dir }} cargo build --package nervix-deadlock --features report-tool --bin nervix-deadlock-report

deadlock-report *args:
    cargo run --quiet --package nervix-deadlock --features report-tool --bin nervix-deadlock-report -- {{ args }}

build-test-cli:
    CARGO_TARGET_DIR={{ cargo_target_dir }} cargo build --package nervix-cli --bin nervix-cli

# Build the runnable paced simulation's Rust driver, and the shared C binding its Python driver
# loads, where the scenarios run them from.
build-paced-simulation:
    CARGO_TARGET_DIR={{ cargo_target_dir }} cargo build --package nervix-paced-simulation \
        --package nervix-client-ffi

# Run the unit tests of both paced simulation drivers.
test-paced-simulation *args: test-paced-simulation-python
    cargo test --package nervix-paced-simulation -- {{ args }}

test-paced-simulation-python:
    python3 -m unittest discover -s examples/paced-simulation/python -p 'test_*.py'

# Deterministic application-open coverage, including refusal and deadline outcomes.
coverage-paced-simulation-python:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p {{ quote(cargo_target_dir + "/paced-simulation") }}
    export COVERAGE_FILE="{{ cargo_target_dir }}/paced-simulation/python.coverage"
    uvx --from coverage==7.11.0 coverage run --branch \
        --source=examples/paced-simulation/python,scripts/paced_simulation_coverage \
        -m unittest discover -s examples/paced-simulation/python -p 'test_*.py'
    uvx --from coverage==7.11.0 coverage lcov -o "{{ cargo_target_dir }}/paced-simulation/python.lcov"

# Run the paced simulation's Rust driver against a running node, for example
# `just paced-simulation --server http://127.0.0.1:47391 --ticks 100`.
paced-simulation *args:
    cargo run --release --package nervix-paced-simulation -- {{ args }}

# Run the paced simulation's Python driver through the shared C binding, with the same arguments
# as the Rust driver.
paced-simulation-python *args:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build --release --package nervix-client-ffi
    export NERVIX_CLIENT_LIBRARY={{ quote(cargo_target_dir + "/release/libnervix_client_ffi.so") }}
    python3 examples/paced-simulation/python/paced_simulation.py {{ args }}

test: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    # Execution-mode features replace primitives and are valid only inside their runner, and the
    # modes cannot be enabled together. Test the packages that own a mode with ordinary primitives
    # here; `test-shuttle`, `test-loom`, `test-turmoil`, `test-deloxide` and `test-primitives`
    # exercise the modes.
    mode_packages=(
        nervix-client-core
        'nervix-connector*'
        nervix-consensus
        nervix-deadlock
        nervix-execution
        nervix-interconnect
        nervix-model-harness
        nervix-paced-simulation
        nervix-primitives
        nervix-server
        nervix-wasm
    )
    workspace_exclusions=()
    for package in "${mode_packages[@]}"; do
        workspace_exclusions+=(--exclude "${package}")
    done
    cargo test --all-targets --all-features --workspace "${workspace_exclusions[@]}"
    cargo test --all-targets --features testing --package nervix-server
    cargo test --all-targets \
        --package nervix-client-core \
        --package 'nervix-connector*' \
        --package nervix-consensus \
        --package nervix-deadlock \
        --package nervix-execution \
        --package nervix-interconnect \
        --package nervix-model-harness \
        --package nervix-paced-simulation \
        --package nervix-wasm
    cargo test --all-targets --features native --package nervix-primitives
    just test-capability-docs
    just test-turmoil

# Check capability examples with rustdoc, including runtime exports enabled only for tests.
test-capability-docs: build-web-console
    cargo test --package nervix-server --package nervix-roto --features nervix-server/testing --doc

test-scenarios *args: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --features testing --test scenarios -- {{ args }}

# Replay a compiled scenario binary, including a saved pre-fix reproducer, without rebuilding it.
test-scenarios-binary binary *args: download-onnxruntime
    ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)" {{ quote(binary) }} {{ args }}

# Focused kernel tests: every SIMD level the host supports and the forced scalar fallback,
# beside the crate's doctests.
test-simd-kernels *args:
    cargo test --package nervix-simd-kernels -- {{ args }}

# Focused columnar admission kernel and vocabulary tests.
test-admission-kernels *args:
    cargo test --package nervix-simd-kernels --lib -- {{ args }}
    cargo test --package nervix-models --lib -- {{ args }}

bench-window-admission *args:
    cargo bench --package nervix-simd-kernels --bench window_admission -- {{ args }}

# Measure the checked integer SIMD kernels beside the lane loop they replaced, over one 1,024-lane
# run each. Extra arguments are forwarded to Criterion.
bench-checked-lanes *args:
    cargo bench --package nervix-simd-kernels --bench checked_lanes -- {{ args }}

# Compare constant division at every width with scalar reciprocal and checked lane loops.
bench-constant-division *args:
    cargo bench --package nervix-simd-kernels --bench constant_division -- {{ args }}

# Compare the byte-class scans and the XML character check with the scalar loops they replaced.
bench-byte-classes *args:
    cargo bench --package nervix-simd-kernels --bench byte_classes -- {{ args }}

bench-byte-classes-x86-64-v3 *args:
    CARGO_TARGET_DIR="{{ cargo_target_dir }}/simd-kernels-x86-64-v3" RUSTFLAGS="-C target-cpu=x86-64-v3" cargo bench --package nervix-simd-kernels --bench byte_classes -- {{ args }}

# Measure RFC 6587 stream framing over a mebibyte of frames read in a connection's 8 KiB reads.
bench-syslog-framing *args:
    cargo bench --package nervix-connector-syslog --bench stream_framing --features benchmarks -- {{ args }}

bench-constant-division-x86-64-v3 *args:
    CARGO_TARGET_DIR="{{ cargo_target_dir }}/simd-kernels-x86-64-v3" RUSTFLAGS="-C target-cpu=x86-64-v3" cargo bench --package nervix-simd-kernels --bench constant_division -- {{ args }}

# The same measurement built for the x86-64-v3 payload the Docker image ships, in its own target
# directory: the lane loop compiles for AVX2 as the payload's does, and the kernels still select
# their level from the CPU at run time.
bench-checked-lanes-x86-64-v3 *args:
    CARGO_TARGET_DIR="{{ cargo_target_dir }}/simd-kernels-x86-64-v3" RUSTFLAGS="-C target-cpu=x86-64-v3" cargo bench --package nervix-simd-kernels --bench checked_lanes -- {{ args }}

test-admission-runtime *args: download-onnxruntime
    ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)" cargo test --package nervix-server --features testing --lib -- {{ args }}

# Focused ordinary-mode regressions for runtime owners.
test-runtime *args: build-web-console wasm-processor-guests download-onnxruntime
    ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)" cargo test --package nervix-server --features testing --lib -- {{ args }}

# Measure established row publication and immutable captures with bounded allocation evidence.
bench-materialized-state: build-web-console wasm-processor-guests download-onnxruntime bench-materialized-state-bodies

bench-materialized-state-bodies:
    ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)" cargo test --package nervix-server --features testing --lib materialized_owner_cost -- --ignored --nocapture

# Measure the endpoint's actual request routing and per-thread allocations on the same host.
bench-endpoint-routing: build-web-console download-onnxruntime
    ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)" cargo test --package nervix-server --features testing,benchmarks --lib endpoint_routing_cost -- --ignored --nocapture

test-endpoint-intake *args: build-web-console download-onnxruntime
    ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)" cargo test --package nervix-server --features testing --lib -- {{ args }}

test-web-console:
    CARGO_TARGET_DIR={{ cargo_target_dir }} cargo test --package nervix-web-console --bin nervix-web-console

test-harness-liveness *args: tests-deps
    cargo test --features testing --test harness_liveness -- {{ args }}

test-scenarios-reuse *args: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    export NERVIX_TESTCONTAINERS_MODE=reusable
    cargo test --features testing --test scenarios -- {{ args }}

# Measure the public client protocol against a release nervix-server process.
# The JSON report and raw Prometheus scrape record the workload, toolchain, hardware and limits.
client-wire-baseline samples="100" upload_samples="5" payload_bytes="1024" output="target/client-wire-baseline": build-web-console download-onnxruntime
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo build --release --package nervix-server --bin nervix-server
    export NERVIX_CLIENT_WIRE_BASELINE_SERVER={{ quote(cargo_target_dir + "/release/nervix-server") }}
    export NERVIX_CLIENT_WIRE_BASELINE_SERVER_PROFILE=release
    export NERVIX_CLIENT_WIRE_BASELINE_SAMPLES={{ quote(samples) }}
    export NERVIX_CLIENT_WIRE_BASELINE_UPLOAD_SAMPLES={{ quote(upload_samples) }}
    export NERVIX_CLIENT_WIRE_BASELINE_PAYLOAD_BYTES={{ quote(payload_bytes) }}
    export NERVIX_CLIENT_WIRE_BASELINE_OUTPUT={{ quote(output) }}
    cargo test --features testing --test scenarios -- \
        --input tests/features/runtime/client_wire_baseline.feature \
        --tags @client_wire_baseline \
        --concurrency 1 \
        --retry 0

# Capture the web console images the book publishes. The capture tool starts a real nervix-server,
# seeds it with nervix-cli, and drives the console in a browser, so the images are build output
# rather than repository content and the book stages them from target/.
docs-screenshots: build-web-console
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build --package nervix-server --bin nervix-server --package nervix-cli --bin nervix-cli
    npm --prefix scripts/console-screenshots ci
    node scripts/console-screenshots/capture.mjs \
      --server "{{ cargo_target_dir }}/debug/nervix-server" \
      --cli "{{ cargo_target_dir }}/debug/nervix-cli" \
      --output "{{ cargo_target_dir }}/docs-screenshots"

test-lib *args: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --features testing --lib -- {{ args }}

# Run the Arrow-to-Row correctness cases and print the typed encoding's allocation evidence.
test-subscription-rows: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --features benchmarks,testing --lib subscription_row:: -- --nocapture

# Type-check one workspace package and all of its targets without building binaries. Extra
# arguments are forwarded to Cargo, so a server check can add `--features testing`.
check-package package *args:
    cargo check --package {{ package }} --all-targets {{ args }}

# Type-check only the library target of one package, leaving its tests, benches and binaries out.
check-package-lib package *args:
    cargo check --package {{ package }} --lib {{ args }}

# Run the unit tests of one workspace package whose tests need no server test dependencies.
test-package-lib package *args:
    cargo test --package {{ package }} --lib -- {{ args }}

# Run one integration test target of one workspace package whose tests need no server test
# dependencies, such as the vocabulary's representation properties.
test-package-test package test *args:
    cargo test --package {{ package }} --test {{ test }} -- {{ args }}

# Run the unit tests in the binary targets of one workspace package, such as the web console's
# view logic in its `main.rs`.
test-package-bins package *args:
    cargo test --package {{ package }} --bins -- {{ args }}

# Run the bounded-execution unit tests, which live in the nervix-execution crate rather than the
# server lib.
test-execution *args:
    cargo test --package nervix-execution --lib -- {{ args }}

# Run the primitive boundary's conformance checks once per execution mode. Each mode runs the same
# contract scripts of every family against its own backend and checks that it selected that backend,
# so an operation a backend lacks or answers differently fails here. The ordinary build with the
# `test-util` capability measures every timer on a paused clock. The Shuttle build also shows that
# its adapters let the scheduler reach a publication between a read and a waiter's registration,
# that its timers are scheduling points whose timeouts a check triggers, and that a socket fails a
# check; the Turmoil build that sockets, name lookup, timers and admitted CPU jobs belong to the
# simulated host that uses them; and the Loom build that it takes the ordinary libraries for the
# families Loom does not model. The documentation tests show that a runtime attribute refuses a
# crate path. Every mode is its own build, because Cargo would unify the features of one.
# The portable surface is also built for the browser target.
test-primitives: test-primitives-ordinary test-primitives-modeled test-primitives-compile

# The conformance checks in ordinary native execution: the portable surface alone, then with the
# native families, then with the `test-util` capability, which measures every timer on a paused
# clock. `coverage-native-extras test-primitives` runs these under instrumentation.
test-primitives-ordinary:
    cargo test --package nervix-primitives --lib
    cargo test --package nervix-primitives --features native --lib
    cargo test --package nervix-primitives --features 'native test-util' --lib

# The conformance checks under each other execution mode's backend, each mode its own build: the
# model checkers, the simulator, and the diagnostic mode's tracked locks.
test-primitives-modeled: test-primitives-shuttle test-primitives-loom test-primitives-turmoil test-primitives-deloxide

test-primitives-shuttle:
    cargo test --package nervix-primitives --features 'shuttle native' --lib

test-primitives-loom:
    cargo test --package nervix-primitives --features 'loom native' --lib

test-primitives-turmoil:
    cargo test --package nervix-primitives --features 'turmoil native' --lib

test-primitives-deloxide:
    cargo test --package nervix-primitives --features 'deloxide native' --lib
    cargo test --package nervix-primitives --features 'deloxide-order native' --lib

# The conformance checks that compile rather than run: the documentation tests that a runtime
# attribute refuses a crate path, that a product binary has the forms each mode builds, and that the
# diagnostic mode's tracked locks refuse the operations they cannot track; and the portable
# surface's browser build.
test-primitives-compile:
    cargo test --package nervix-primitives --features native --doc
    cargo test --package nervix-primitives --features 'deloxide native' --doc
    cargo test --package nervix-primitives --features 'deloxide-order native' --doc
    cargo check --package nervix-primitives --lib --target wasm32-unknown-unknown

# Run the Deloxide diagnostic lane in its active-only selection: every workload
# tests/deloxide-inventory.toml registers for the `deloxide` build, after the prerequisites. The
# lane builds each invocation for the selection under target/deloxide, so the diagnostic server
# binary never replaces the ordinary one, and refuses a registered workload that is missing or
# ignored and a probe, conformance check, owner test or tagged scenario that is not registered. The
# probes of nervix-deadlock run every workload in a disposable child that must report its cycle,
# record its evidence and end as its contract says; the tracked locks' conformance checks of
# nervix-primitives and the diagnostic owner tests each install their detector in a fresh process;
# and the scenario binary runs the `@deadlock_diagnostics`, `@restore_installation`,
# `@client_ingestor_alter_drain`, `@deadlock_reports`, `@memory_pressure_pause`,
# `@client_io_03_consumer_restore`, `@client_io_03_generation`, `@remote_ack_owners`, `@udf_column_builder`,
# `@vhost_tls_rebinding` and `@inferencer_branch_batches` scenarios and then the
# `@paced_simulation_reopen` scenarios with the diagnostic Rust paced driver, without retries, on
# in-process nodes and real diagnostic server processes of one and three nodes. Every process runs
# in a session of its own within its bound and the inventory's budget, and the lane fails on an
# active deadlock (status 3), a diagnostic failure (4), a signal, a timeout or an expired budget
# (124), a leftover process, incomplete accounting, and missing, partly written or nonqualifying
# evidence. A fresh attempt under target/deloxide/test-deloxide/deloxide keeps every log, artifact,
# evidence file and finding description, and lane.json with the exact commands, bounds and
# outcomes. Under the
# native coverage collector the same lane builds instrumented and records its completion there.
test-deloxide: tests-deps test-deloxide-workloads

# The same lane in the `deloxide-order` selection, whose builds add historical lock-order
# instrumentation and run the order-only probes.
test-deloxide-order: tests-deps test-deloxide-order-workloads

# The part of each selection's lane that executes Nervix code. It has no dependencies, so the native
# coverage collector runs exactly the lane inside its instrumentation.
test-deloxide-workloads:
    python3 -m scripts.deloxide_lane --target-dir {{ quote(cargo_target_dir) }} run deloxide

test-deloxide-order-workloads:
    python3 -m scripts.deloxide_lane --target-dir {{ quote(cargo_target_dir) }} run deloxide-order

# Prove the lane's supervision on real failing processes: probe workloads that deadlock, fail their
# diagnostics, hang on an untracked wait, abort, retain an unreviewed potential cycle or overflow
# their retention must each fail the lane with its own class, keep their output and evidence, and
# leave no process behind; a clean control must pass; and the recorded deadlock must replay. The
# order-only cases run in the `deloxide-order` selection.
test-deloxide-qualification selection="deloxide-order": build-deadlock-report
    python3 -m scripts.deloxide_lane --target-dir {{ quote(cargo_target_dir) }} qualify {{ quote(selection) }}

# Run one launch a lane attempt recorded again, with exactly its command, environment and bound, in a
# fresh attempt: `just test-deloxide-replay target/deloxide/test-deloxide/deloxide/run.XXXX/lane.json scenarios`.
# Schedules and timing are not recorded, so a replay can take another interleaving.
test-deloxide-replay record launch:
    python3 -m scripts.deloxide_lane --target-dir {{ quote(cargo_target_dir) }} replay {{ quote(record) }} {{ quote(launch) }}

# Hold every source file that acquires a tracked blocking lock in a diagnostic build to its owner
# record in tests/deloxide-inventory.toml, using the compiler catalog `just ratchet` writes.
validate-deloxide-applicability: ratchet
    python3 -m scripts.deloxide_lane applicability --catalog {{ quote(cargo_target_dir + "/typed-ratchet/gate.json") }}

# Focused disposable-process reproducer and diagnostic checks, preserving configured kache.
test-deadlock-probes features="deloxide" *args:
    CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/deloxide") }} cargo test --package nervix-deadlock --features {{ quote(features) }} --test active_cycles {{ args }}

# The packages whose `shuttle_` checks `test-shuttle` explores, as the Shuttle inventory lists them,
# and whose test builds `shuttle-clippy-targets` lints. scripts/tests/test_shuttle_checks.py holds
# this list to the inventory.
shuttle_test_packages := ["nervix-execution", "nervix-interconnect", "nervix-client-core", "nervix-consensus", "nervix-server", "nervix-deadlock"]

# Explore every registered Shuttle check of a production owner, each in its own process: under the
# exploration it declares, then under the uncontrolled-nondeterminism detector. The inventory in
# crates/model-harness/shuttle-inventory.toml registers each check by package and test; the whole
# run fails when a registered check is missing, ignored or did not complete its exploration, or when
# a check is unregistered. A non-empty `filter` runs the checks whose full names contain it, in
# every package, and fails when it selects none at all. A failed check leaves its persisted
# schedule, output and metadata under target/shuttle-failures for `test-shuttle-replay`. The server's
# checks need the build dependencies this recipe prepares.
test-shuttle filter="": build-web-console wasm-processor-guests download-onnxruntime (test-shuttle-checks filter)

[private]
test-shuttle-checks filter="":
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    python3 -m unittest --quiet scripts.tests.test_shuttle_checks
    evidence="${NERVIX_MODEL_EVIDENCE:-}"
    report=()
    if [[ -n "$evidence" ]]; then report=(--report "$evidence"); fi
    python3 -m scripts.shuttle_checks --target-dir {{ quote(cargo_target_dir) }} run {{ quote(filter) }} "${report[@]}"

# Replay a schedule `test-shuttle` persisted under target/shuttle-failures in a fresh process. Its
# parent directories name the exact package and check, so the schedule cannot run against another
# invariant.
test-shuttle-replay schedule: build-web-console wasm-processor-guests download-onnxruntime
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    python3 -m scripts.shuttle_checks --target-dir {{ quote(cargo_target_dir) }} replay {{ quote(schedule) }}

# Prove schedule persistence and replay end to end: fail one execution check deliberately after its
# invariant held, require exactly one persisted schedule, and require it to reproduce the failure in
# a fresh process.
test-shuttle-replay-check:
    python3 -m scripts.shuttle_checks --target-dir {{ quote(cargo_target_dir) }} replay-check

# Explore every registered Loom model of a production owner to exhaustion, each in its own process
# and with Loom's primitives selected through its package's `loom` feature. The inventory in
# crates/model-harness/loom-inventory.toml registers each model by the invariant it checks; the
# whole run fails when a registered invariant is missing, ignored or incomplete, or when a model is
# unregistered. A non-empty `filter` runs the models whose test name or invariant contains it and
# fails when it selects none. A failed model leaves its Loom checkpoint, output and metadata under
# target/loom-failures for `test-loom-replay`. The server's models need the web console its library
# embeds.
test-loom filter="": build-web-console (test-loom-models filter)

[private]
test-loom-models filter="":
    #!/usr/bin/env bash
    set -euo pipefail
    python3 -m unittest --quiet scripts.tests.test_loom_models
    evidence="${NERVIX_MODEL_EVIDENCE:-}"
    report=()
    if [[ -n "$evidence" ]]; then report=(--report "$evidence"); fi
    python3 -m scripts.loom_models --target-dir {{ quote(cargo_target_dir) }} run {{ quote(filter) }} "${report[@]}"

# Measure the native runner's inventory, completion and failure artifact paths.
coverage-loom-runner:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p target/loom-coverage
    coverage=(uvx --from coverage==7.11.0 coverage)
    "${coverage[@]}" run --data-file target/loom-coverage/runner.coverage --branch --source=scripts.loom_models,scripts.tests.test_loom_models -m unittest scripts.tests.test_loom_models
    "${coverage[@]}" lcov --data-file target/loom-coverage/runner.coverage -o target/loom-coverage/python-runner.lcov

# Replay a failure `test-loom` recorded: Loom resumes from the checkpoint of the failed execution,
# with location tracking and tracing enabled, so that execution runs first.
test-loom-replay failure: build-web-console
    python3 -m scripts.loom_models --target-dir {{ quote(cargo_target_dir) }} replay {{ quote(failure) }}

# Show that each qualified Loom model detects its ordering or stack capacity fault. Every weakening is
# applied to a copy of the working tree, the model must fail with its registered message, and the
# checkpoint of that failure must replay it. `shard`, NUMBER/COUNT, qualifies one part of the
# distinct weakenings, so CI can split their rebuilds across jobs; the default qualifies them all.
test-loom-qualification shard="1/1": build-web-console
    python3 -m scripts.loom_models --target-dir {{ quote(cargo_target_dir) }} qualify --shard {{ quote(shard) }}

# Run the Turmoil suite: the primitive boundary's simulated-host checks, the execution and library
# simulation checks, then every interconnect scenario over its committed regression seeds. Tokio's
# unstable runtime knobs seed per-host scheduling and turn unhandled task panics into runtime
# failures; the cfg is scoped to this test mode, and ordinary and Shuttle builds keep their flags.
# After the build, the tests run inside a real-time budget of `budget_seconds` and end with status
# 124 when it expires. A failed scenario leaves a failure record under target/turmoil-failures for
# `test-turmoil-replay`. The suite reports how many tests each invocation and the whole suite
# discovered, selected, executed and completed. tests/turmoil-inventory.toml registers the tests
# each invocation runs and leaves ignored, and the suite fails when a registered test did not run or
# ran ignored, when an invariant ran unregistered, and when an invocation executed no test. Each
# invocation's output stays under target/turmoil-suite.
test-turmoil budget_seconds="480":
    #!/usr/bin/env bash
    set -euo pipefail
    python3 -m unittest --quiet scripts.tests.test_libtest_accounting
    turmoil_rustflags="--cfg tokio_unstable ${RUSTFLAGS:-}"
    export NERVIX_TURMOIL_FAILURES={{ quote(turmoil_failures) }}
    RUSTFLAGS="${turmoil_rustflags}" cargo test --no-run \
        --package nervix-primitives --features 'turmoil native' --lib
    RUSTFLAGS="${turmoil_rustflags}" cargo test --no-run \
        --package nervix-execution --features turmoil --lib
    RUSTFLAGS="${turmoil_rustflags}" cargo test --no-run \
        --package nervix-interconnect --features turmoil --lib --test simulation
    logs={{ quote(cargo_target_dir + "/turmoil-suite") }}
    rm -rf "${logs}"
    mkdir -p "${logs}"
    deadline=$((SECONDS + {{ budget_seconds }}))
    within_budget() {
        local log="$1"
        shift
        local remaining=$((deadline - SECONDS))
        local status=0
        if ((remaining > 0)); then
            set +e
            RUSTFLAGS="${turmoil_rustflags}" timeout --kill-after=30 "${remaining}" "$@" 2>&1 \
                | tee "${logs}/${log}.log"
            status="${PIPESTATUS[0]}"
            set -e
        else
            status=124
        fi
        if ((status == 124)); then
            echo "the Turmoil suite exceeded its {{ budget_seconds }}s real-time budget;" \
                "in-progress records under ${NERVIX_TURMOIL_FAILURES} name the unfinished runs" >&2
        fi
        return "${status}"
    }
    within_budget primitives \
        cargo test --package nervix-primitives --features 'turmoil native' --lib -- \
            simulated_host turmoil_mode --test-threads=1
    within_budget execution \
        cargo test --package nervix-execution --features turmoil --lib -- --test-threads=1
    within_budget interconnect-library \
        cargo test --package nervix-interconnect --features turmoil --lib -- \
            wire::simulation_checks authentication::simulation_tests --test-threads=1
    within_budget interconnect-simulation \
        cargo test --package nervix-interconnect --features turmoil --test simulation -- \
            --test-threads=1
    python3 -m scripts.libtest_accounting turmoil --inventory tests/turmoil-inventory.toml \
        "${logs}/primitives.log" "${logs}/execution.log" "${logs}/interconnect-library.log" \
        "${logs}/interconnect-simulation.log"

# Run the interconnect's Turmoil simulation scenarios. Extra arguments filter or configure the test
# binary, so one test can run without the execution and library checks. A selection that executes
# no test fails.
test-turmoil-simulation *args:
    #!/usr/bin/env bash
    set -euo pipefail
    export RUSTFLAGS="--cfg tokio_unstable ${RUSTFLAGS:-}"
    export NERVIX_TURMOIL_FAILURES={{ quote(turmoil_failures) }}
    logs="$(mktemp -d)"
    trap 'rm -rf "${logs}"' EXIT
    set +e
    cargo test --package nervix-interconnect --features turmoil --test simulation -- \
        --test-threads=1 {{ args }} 2>&1 | tee "${logs}/simulation.log"
    status="${PIPESTATUS[0]}"
    set -e
    if ((status != 0)); then
        exit "${status}"
    fi
    python3 -m scripts.libtest_accounting turmoil-simulation "${logs}/simulation.log"

# Replay one Turmoil failure record in a fresh process with exactly its recorded inputs: the seed,
# epoch, topology, network parameters, bounds and any injected failure. The record names the
# package, test target and test, so nothing else runs. The replay reports how the recorded build
# differs from this one, compares its outcome and semantic trace with the record, and fails when
# the recorded failure reproduces.
test-turmoil-replay record:
    #!/usr/bin/env bash
    set -euo pipefail
    record="$(realpath -e {{ quote(record) }})"
    mapfile -t selection < <(
        python3 -c 'import json, sys; scenario = json.load(open(sys.argv[1]))["scenario"]; print(scenario["package"], scenario["target"], scenario["test"], sep="\n")' "${record}"
    )
    if ((${#selection[@]} != 3)); then
        echo "${record} is not a Turmoil failure record" >&2
        exit 1
    fi
    output="$(mktemp)"
    trap 'rm -f "${output}"' EXIT
    status=0
    NERVIX_TURMOIL_REPLAY="${record}" RUSTFLAGS="--cfg tokio_unstable ${RUSTFLAGS:-}" \
        cargo test --package "${selection[0]}" --features turmoil --test "${selection[1]}" -- \
            "${selection[2]}" --exact --include-ignored --test-threads=1 --nocapture 2>&1 \
        | tee "${output}" \
        || status=$?
    if ! grep -Fq 'turmoil replay of' "${output}"; then
        echo "no scenario in ${selection[2]} matches ${record}; it may come from another revision" >&2
        exit 1
    fi
    exit "${status}"

# Prove the replay path end to end: inject a harness failure into one scenario, require exactly one
# failure record, and reproduce it in a fresh process through `test-turmoil-replay`.
test-turmoil-replay-check:
    #!/usr/bin/env bash
    set -euo pipefail
    check="$(mktemp -d)"
    trap 'rm -rf "${check}"' EXIT
    records="${check}/records"
    if NERVIX_TURMOIL_FAILURES="${records}" NERVIX_TURMOIL_INJECT_FAILURE=5s \
        RUSTFLAGS="--cfg tokio_unstable ${RUSTFLAGS:-}" \
        cargo test --package nervix-interconnect --features turmoil --test simulation -- \
            transport::network_disruption_respects_deadlines_and_repairs_authenticated_service \
            --exact --test-threads=1 >"${check}/injected.log" 2>&1; then
        cat "${check}/injected.log"
        echo "the injected harness failure did not fail its scenario" >&2
        exit 1
    fi
    mapfile -t found < <(find "${records}" -name '*.json')
    if ((${#found[@]} != 1)); then
        cat "${check}/injected.log"
        echo "expected one failure record, found ${#found[@]}" >&2
        exit 1
    fi
    if just test-turmoil-replay "${found[0]}" >"${check}/replay.log" 2>&1; then
        cat "${check}/replay.log"
        echo "the replay of an injected failure passed" >&2
        exit 1
    fi
    if ! grep -Fq 'turmoil replay: reproduced the recorded outcome and trace' \
        "${check}/replay.log"; then
        cat "${check}/replay.log"
        echo "the replay did not reproduce the recorded failure" >&2
        exit 1
    fi
    grep -F 'turmoil replay' "${check}/replay.log"

# Explore every interconnect scenario over `count` consecutive seeds from `first` instead of its
# committed regression seeds, running each seed twice, inside a real-time budget of
# `budget_seconds`; the recipe ends with status 124 when the budget expires. A failure leaves a
# record like any other run. A seed that exposes a defect joins its scenario's committed seeds
# with the fix. A sweep that executes no scenario fails.
test-turmoil-sweep first="1000" count="64" budget_seconds="1500":
    #!/usr/bin/env bash
    set -euo pipefail
    turmoil_rustflags="--cfg tokio_unstable ${RUSTFLAGS:-}"
    export NERVIX_TURMOIL_FAILURES={{ quote(turmoil_failures) }}
    first={{ quote(first) }}
    end=$((first + {{ count }}))
    RUSTFLAGS="${turmoil_rustflags}" cargo test --no-run \
        --package nervix-interconnect --features turmoil --test simulation
    logs="$(mktemp -d)"
    trap 'rm -rf "${logs}"' EXIT
    set +e
    NERVIX_TURMOIL_SWEEP="${first}..${end}" RUSTFLAGS="${turmoil_rustflags}" \
        timeout --kill-after=30 {{ budget_seconds }} \
        cargo test --package nervix-interconnect --features turmoil --test simulation -- \
            --test-threads=1 2>&1 | tee "${logs}/sweep.log"
    status="${PIPESTATUS[0]}"
    set -e
    if ((status == 124)); then
        echo "the seed sweep exceeded its {{ budget_seconds }}s real-time budget;" \
            "in-progress records under ${NERVIX_TURMOIL_FAILURES} name the unfinished runs" >&2
    fi
    if ((status != 0)); then
        exit "${status}"
    fi
    python3 -m scripts.libtest_accounting turmoil-sweep "${logs}/sweep.log"

# Run the expression VM unit tests, which live in the nervix-vm crate rather than the server lib.
test-vm *args:
    cargo test --package nervix-vm --lib -- {{ args }}

# Run the WASM host, guest SDK and ABI protocol unit tests, which live in their own crates rather
# than the server lib. The host tests drive the bundled Rust and Go reference guests.
test-wasm *args: wasm-processor-guests
    cargo test --package nervix-wasm --package nervix-wasm-sdk --package nervix-wasm-protocol --lib -- {{ args }}
    cargo test --package nervix-wasm-protocol --test representations -- {{ args }}

# Protocol boundary checks do not need compiled processor guests.
test-wasm-protocol *args:
    cargo test --package nervix-wasm-protocol -- {{ args }}

# Run the consensus unit tests, which live in the nervix-consensus crate rather than the server lib.
test-consensus *args:
    cargo test --package nervix-consensus --lib -- {{ args }}

# Run vocabulary unit tests and representation properties.
test-models *args:
    cargo test --package nervix-models --all-targets -- {{ args }}

# Run the interconnect unit tests, which live in the nervix-interconnect crate rather than the
# server lib.
test-interconnect *args:
    cargo test --package nervix-interconnect --lib -- {{ args }}

# Run the resolver's unit tests and its focused protocol tests, which ask local DNS authorities the
# tests start themselves.
test-dns *args:
    cargo test --package nervix-dns --all-targets -- {{ args }}

# Run the connector unit tests, which live in the nervix-connector contract crate and in every
# nervix-connector-* integration crate rather than the server lib.
test-connectors *args:
    cargo test --package 'nervix-connector*' --lib -- {{ args }}

# Run the session wire codec tests and the gRPC and WebSocket sessions that carry its frames.
test-client-wire *args:
    cargo test --package nervix-client-wire --all-features --all-targets -- {{ args }}

# Run native client session unit tests without enabling the modeled Shuttle build.
test-client-core *args:
    cargo test --package nervix-client-core --features arrow,autocomplete --lib -- {{ args }}

# Rewrite the client wire conformance corpus from the encoder's current output. Review the
# regenerated `corpus.report` before committing it: every client implementation is held to it.
update-client-wire-corpus:
    NERVIX_UPDATE_CLIENT_WIRE_CORPUS=1 cargo test --package nervix-client-wire --lib -- \
        tests::conformance

# Build what the cross-language client probes run: the shared Rust binding, the C and C++ probes
# linked against it, the Go probe with its generated FlatBuffers code, and the TypeScript probe
# bundled with its generated code for Node.js and Bun. Every generated file lands under the
# artifacts directory, never in the source tree.
build-client-conformance:
    #!/usr/bin/env bash
    set -euo pipefail
    artifacts={{ quote(cargo_target_dir + "/client-conformance") }}
    library_dir={{ quote(cargo_target_dir + "/debug") }}
    schema="${PWD}/crates/client-wire/schema/session.fbs"
    mkdir -p "${artifacts}"
    cargo build --package nervix-client-ffi
    cc -std=c11 -Wall -Wextra -Werror -pthread -I crates/client-ffi/include \
        tests/client_conformance/c/probe.c \
        -L "${library_dir}" -lnervix_client_ffi -Wl,-rpath,"${library_dir}" \
        -o "${artifacts}/c-probe"
    c++ -std=c++17 -Wall -Wextra -Werror -pthread -I crates/client-ffi/include \
        tests/client_conformance/cpp/probe.cpp \
        -L "${library_dir}" -lnervix_client_ffi -Wl,-rpath,"${library_dir}" \
        -o "${artifacts}/cpp-probe"
    rm -rf "${artifacts}/go" && mkdir -p "${artifacts}/go"
    cp tests/client_conformance/go/go.mod tests/client_conformance/go/go.sum \
        tests/client_conformance/go/*.go "${artifacts}/go/"
    flatc --go -o "${artifacts}/go" "${schema}"
    (cd "${artifacts}/go" && go build -o "${artifacts}/go-probe" .)
    rm -rf "${artifacts}/node-src" "${artifacts}/node" && mkdir -p "${artifacts}/node-src"
    cp tests/client_conformance/node/package.json tests/client_conformance/node/package-lock.json \
        tests/client_conformance/node/probe.ts "${artifacts}/node-src/"
    flatc --ts -o "${artifacts}/node-src/generated" "${schema}"
    (cd "${artifacts}/node-src" && npm ci --no-audit --no-fund && \
        ./node_modules/.bin/esbuild probe.ts --bundle --platform=node --format=esm \
            --target=es2022 --outfile="${artifacts}/node/probe.mjs")

# Run the cross-language client probes against in-process clusters. Every probe prints the same
# report, which the scenario compares with its one expected report. Select runtimes with a tag
# expression, for example `just test-client-conformance '@client_probe_python or @client_probe_java'`.
test-client-conformance tags="@client_conformance_toolchain" *args: tests-deps build-client-conformance
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    export NERVIX_CLIENT_CONFORMANCE_DIR={{ quote(cargo_target_dir + "/client-conformance") }}
    export NERVIX_CLIENT_LIBRARY={{ quote(cargo_target_dir + "/debug/libnervix_client_ffi.so") }}
    cargo test --features testing --test scenarios -- \
        --input tests/features/runtime/client_conformance.feature \
        --tags {{ quote(tags) }} {{ args }}

test-runtime-state-capabilities: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --features testing --package nervix-server --doc runtime::

# Validate the small unsafe boundary used by deduplicator expiration tracking.
test-expiry-map:
    cargo test --package nervix-expiry-map

# Bolero reads its checked-in corpus from the source tree, which Miri's isolation forbids, so the
# Miri run lifts isolation and bounds the property's random cases beside that corpus.
test-expiry-map-miri:
    MIRIFLAGS="-Zmiri-disable-isolation" BOLERO_RANDOM_ITERATIONS=4 BOLERO_RANDOM_MAX_LEN=512 \
        cargo +nightly miri test --package nervix-expiry-map

test-expiry-map-mutants:
    cargo mutants --package nervix-expiry-map --timeout 120

# Walk the NSPL completion graph and fail on any branch that cannot be completed by accepting the
# suggestions the parser itself offers. Kept out of `just test` because it saturates every core for
# tens of seconds; CI runs it once the main tests have passed.
nspl-completion-walk *args:
    cargo test -p nervix-nspl --test completion_walk -- {{ args }}

# The same walk with deduplication relaxed, reaching branches the gating budget stops short of.
# Reports without failing: this is for finding new ones, not for gating.
nspl-completion-walk-deep *args:
    cargo test -p nervix-nspl --test completion_walk -- \
      --report-only --signature-window 8 --max-states 1000000 {{ args }}

test-coverage: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    # Merge ordinary-mode coverage for the packages that own an execution mode into the workspace
    # profile without running modeled primitives outside their runner. Model checks are not
    # product coverage, so no Shuttle, Loom, Turmoil or diagnostic build contributes to it.
    mode_packages=(
        nervix-client-core
        'nervix-connector*'
        nervix-consensus
        nervix-deadlock
        nervix-execution
        nervix-interconnect
        nervix-model-harness
        nervix-paced-simulation
        nervix-primitives
        nervix-server
        nervix-wasm
    )
    workspace_exclusions=()
    for package in "${mode_packages[@]}"; do
        workspace_exclusions+=(--exclude "${package}")
    done
    cargo llvm-cov clean --workspace
    cargo llvm-cov --no-report --all-targets --all-features --workspace \
        "${workspace_exclusions[@]}"
    # These server targets cover every server test except the scenario suite, which runs in its
    # own job.
    cargo llvm-cov --no-report --lib --bins --benches --test harness_liveness \
        --features testing --package nervix-server
    cargo llvm-cov --no-report --all-targets \
        --package nervix-client-core \
        --package 'nervix-connector*' \
        --package nervix-consensus \
        --package nervix-deadlock \
        --package nervix-execution \
        --package nervix-interconnect \
        --package nervix-model-harness \
        --package nervix-paced-simulation \
        --package nervix-wasm
    cargo llvm-cov --no-report --all-targets --features native --package nervix-primitives
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov --output-path lcov-workspace.info
    cargo llvm-cov report --package nervix-cli --package nervix-web-console \
        --package nervix-server --lcov --output-path lcov.info

test-scenarios-coverage: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo llvm-cov clean --workspace
    just coverage-cli-binary
    just coverage-deadlock-report-binary
    export NERVIX_DEADLOCK_REPORT_TOOL={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-deadlock-report") }}
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-cli") }}
    just coverage-paced-simulation-binaries
    export NERVIX_PACED_SIMULATION_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-paced-simulation") }}
    export NERVIX_CLIENT_LIBRARY={{ quote(cargo_target_dir + "/llvm-cov-target/debug/libnervix_client_ffi.so") }}
    install -m 755 {{ quote(cargo_target_dir + "/debug/nervix-nspl-format") }} \
        {{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-nspl-format") }}
    collection_dir="$(mktemp -d {{ quote(cargo_target_dir + "/paced-simulation-coverage.XXXXXX") }})"
    export COVERAGE_FILE="${collection_dir}/python.coverage"
    export PYTHONPATH="{{ justfile_directory() }}/scripts/paced_simulation_coverage${PYTHONPATH:+:${PYTHONPATH}}"
    export COVERAGE_PROCESS_START="{{ justfile_directory() }}/scripts/paced_simulation_coverage/coverage.ini"
    uv run --no-project --with coverage==7.11.0 cargo llvm-cov --no-report \
        --features testing --package nervix-server --test scenarios
    uvx --from coverage==7.11.0 coverage combine --data-file "${collection_dir}/python.coverage"
    mkdir -p {{ quote(cargo_target_dir + "/paced-simulation") }}
    uvx --from coverage==7.11.0 coverage lcov --data-file "${collection_dir}/python.coverage" \
        -o "{{ cargo_target_dir }}/paced-simulation/python.lcov"
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov --output-path lcov-workspace.info
    cargo llvm-cov report --package nervix-cli --package nervix-web-console \
        --package nervix-server --lcov --output-path lcov.info

# Remove collected profiles and instrumented workspace artifacts before collecting a new revision.
# Append recipes retain artifacts, so a source move can otherwise leave stale line mappings beside
# the current ones in a report.
coverage-clean-workspace:
    cargo llvm-cov clean --workspace

# Rewrite lcov.info from the profiles the last coverage recipe collected, over the sources of every
# workspace package, so crate lines the server's tests executed are measured as CI measures them.
coverage-report-workspace *args:
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov --output-path lcov.info {{ args }}

# Check the same merged workspace coverage and complexity limit locally as CI does.
check-coverage report="lcov-workspace.info":
    cargo crap --lcov {{ quote(report) }} --min 30 --threshold 30

# Measure added executable lines; --pr <number> updates an advisory GitHub PR comment.
coverage-patch base="origin/main" report="lcov-workspace.info" *args:
    python3 -m scripts.patch_coverage --base {{ quote(base) }} --report {{ quote(report) }} --output {{ quote(cargo_target_dir + "/patch-coverage.md") }} {{ args }}

# Exercise line accounting, Git source changes and advisory comment publication.
test-patch-coverage:
    python3 -m unittest scripts.tests.test_patch_coverage

# Retain the reporter's own measured line coverage alongside other Python tooling reports.
coverage-patch-runner:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p "{{ cargo_target_dir }}/patch-coverage"
    coverage=(uvx --from coverage==7.11.0 coverage)
    "${coverage[@]}" run --data-file "{{ cargo_target_dir }}/patch-coverage/python.coverage" --branch --source=scripts.patch_coverage,scripts.tests.test_patch_coverage -m unittest scripts.tests.test_patch_coverage
    "${coverage[@]}" lcov --data-file "{{ cargo_target_dir }}/patch-coverage/python.coverage" -o "{{ cargo_target_dir }}/patch-coverage/python.lcov"

# Measure changed server and CLI lines against the server's unit tests and selected Cucumber
# features while iterating. The scenarios run the public CLI, so it is built instrumented and handed
# to them exactly as `test-coverage` does. That recipe remains the CI gate for workspace coverage
# and CRAP.
test-coverage-feature +features: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo llvm-cov clean --workspace
    just coverage-cli-binary
    just coverage-deadlock-report-binary
    export NERVIX_DEADLOCK_REPORT_TOOL={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-deadlock-report") }}
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-cli") }}
    install -m 755 {{ quote(cargo_target_dir + "/debug/nervix-nspl-format") }} \
        {{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-nspl-format") }}
    cargo llvm-cov --no-report --features testing --package nervix-server --lib
    for feature in {{ features }}; do
        cargo llvm-cov --no-report --features testing --package nervix-server \
            --test scenarios -- --input "${feature}" --concurrency 1
    done
    cargo llvm-cov report --package nervix-cli --package nervix-server --lcov \
        --output-path lcov.info

# Add client and vocabulary tests to an existing coverage profile without clearing server and
# public-scenario coverage collected by `test-coverage-feature`.
test-coverage-client-packages:
    cargo llvm-cov --no-report --all-targets \
        --package nervix-client-core --package nervix-client-wire \
        --package nervix-models --package nervix-cli --package nervix-web-console

# Add focused backup, wire, state, and primitive tests to a backup feature's coverage profile.
test-coverage-backup-packages:
    cargo llvm-cov --no-report --lib \
        --package nervix-backup --package nervix-nspl --package nervix-models \
        --package nervix-client-wire --package nervix-consensus --package nervix-interconnect \
        --package nervix-wasm-protocol --package nervix-wasm-sdk --package nervix-primitives

# Extend an existing instrumented profile with focused public regressions after a correction.
test-coverage-scenario-filter feature filter:
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-cli") }}
    cargo llvm-cov --no-report --features testing --package nervix-server \
        --test scenarios -- --input {{ quote(feature) }} --name {{ quote(filter) }} \
        --concurrency 1 --retry 0

# Measure browser and CLI binary tests together with their public session scenarios.
test-coverage-clients: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo llvm-cov clean --workspace
    cargo llvm-cov --no-report --bins \
        --package nervix-web-console --package nervix-cli
    just coverage-cli-binary
    just coverage-deadlock-report-binary
    export NERVIX_DEADLOCK_REPORT_TOOL={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-deadlock-report") }}
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-cli") }}
    install -m 755 {{ quote(cargo_target_dir + "/debug/nervix-nspl-format") }} \
        {{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-nspl-format") }}
    cargo llvm-cov --no-report --features testing --package nervix-server --lib
    for feature in \
        tests/features/web-console/connection_status.feature \
        tests/features/web-console/nspl_repl.feature \
        tests/features/web-console/domain_clock.feature \
        tests/features/tools/cli_session.feature; do
        cargo llvm-cov --no-report --features testing --package nervix-server \
            --test scenarios -- --input "${feature}" --concurrency 1
    done
    just coverage-clients-report

coverage-clients-report:
    cargo llvm-cov report --package nervix-cli --package nervix-web-console \
        --package nervix-server --lcov --output-path lcov.info

# Refresh the native console's coverage after a focused change without rebuilding the server
# browser scenario harness; its recorded browser and CLI coverage remains in the same target.
coverage-web-console-unit:
    cargo llvm-cov --no-report --bins --package nervix-web-console
    just coverage-clients-report

# Build the standalone CLI with the same coverage flags as its binary unit tests. Cargo's
# all-targets test pass alone leaves only the test executable, which public scenarios do not run.
coverage-cli-binary:
    #!/usr/bin/env bash
    set -euo pipefail
    source <(cargo llvm-cov show-env --sh 2>/dev/null)
    CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/llvm-cov-target") }} \
        cargo build --package nervix-cli --bin nervix-cli

# The local diagnostic report workflow uses this ordinary instrumented executable.
coverage-deadlock-report-binary:
    #!/usr/bin/env bash
    set -euo pipefail
    source <(cargo llvm-cov show-env --sh --no-rustc-wrapper 2>/dev/null)
    CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/llvm-cov-target") }} \
        cargo build --package nervix-deadlock --features report-tool --bin nervix-deadlock-report

# Build the paced simulation's Rust driver, and the shared C binding its Python driver loads, with
# the same coverage flags, so the public scenarios that run both drivers count the lines they reach.
coverage-paced-simulation-binaries:
    #!/usr/bin/env bash
    set -euo pipefail
    source <(cargo llvm-cov show-env --sh 2>/dev/null)
    CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/llvm-cov-target") }} \
        cargo build --package nervix-paced-simulation --package nervix-client-ffi

coverage-clients-units:
    cargo llvm-cov --no-report --bins \
        --package nervix-web-console --package nervix-cli
    just coverage-clients-report

# Measure the visual create patch across its Model, language, wire, browser, and server owners,
# including the public browser scenarios that exercise the attached transaction prefix and the
# subscription tab lifecycle.
coverage-visual-create output="target/visual-create.lcov": tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo llvm-cov clean --workspace
    cargo llvm-cov --no-report --lib \
        --package nervix-models --package nervix-client-wire --package nervix-nspl
    cargo llvm-cov --no-report --bin nervix-web-console --package nervix-web-console
    cargo llvm-cov --no-report --features testing --package nervix-server --lib
    for feature in visual_create_schema visual_create_relay visual_create_codec visual_create_client_endpoint visual_create_lookup_udf visual_create_ingestor visual_create_processors; do
        cargo llvm-cov --no-report --features testing --package nervix-server \
            --test scenarios -- --input "tests/features/web-console/${feature}.feature" \
            --concurrency 1 --retry 0
    done
    just coverage-visual-create-report {{ quote(output) }}

# Write the LCOV report of the profiles `coverage-visual-create` collected, over the packages the
# visual create forms span.
coverage-visual-create-report output="target/visual-create.lcov":
    cargo llvm-cov report --lcov --output-path {{ quote(output) }} \
        --package nervix-models --package nervix-client-wire --package nervix-nspl \
        --package nervix-web-console --package nervix-server

# Write the line coverage of the unit tests of the packages named in `args`, such as
# `--package nervix-vm --package nervix-nspl`, as LCOV to `output`. It checks the patch coverage of
# a change to those packages without the scenario suite that `test-coverage` runs.
coverage-lib output *args:
    cargo llvm-cov --lib --lcov --output-path {{ output }} {{ args }}

# Complete public archive representation and restore/re-export coverage, including test sources.
coverage-backup-archives output="target/backup-archives.lcov": tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo llvm-cov clean --workspace
    just coverage-cli-binary
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-cli") }}
    cargo llvm-cov --no-report --lib --package nervix-backup
    cargo llvm-cov --no-report --lib --package nervix-nspl -- backup
    cargo llvm-cov --no-report --features testing --package nervix-server --test scenarios -- \
        --input tests/features/cluster/backup.feature \
        --name 'A quiesced backup restores two WASM branches and Kafka domain offsets' \
        --concurrency 2 --retry 0
    cargo llvm-cov --no-report --features testing --package nervix-server --lib -- materialized_snapshot
    cargo llvm-cov --no-report --features testing --package nervix-server --lib -- backup
    cargo llvm-cov --no-report --features testing --package nervix-server --lib -- runtime::state_store::checkpoint_reader
    cargo llvm-cov --no-report --features testing --package nervix-server --test scenarios -- \
        --input tests/features/cluster/backup_materialized.feature \
        --name 'A resumed materialized cut preserves interleaved branches and generators through restart|Materialized generations larger than the bulk budget resume on every assigned owner and replica' \
        --concurrency 2 --retry 0
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} \
        --package nervix-backup --package nervix-nspl --package nervix-server

# Deduplicator and window archive coverage: their records and Arrow groups, capture, restore
# conversion into staged pieces and installation, segmented persistence of large windows, the
# framing check their Arrow sections open through, and the public scenarios that resume, re-export
# and skip them, including state above the bulk budget and a conversion refused for room.
coverage-backup-branch-state output="target/backup-branch-state.lcov": tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo llvm-cov clean --workspace
    just coverage-cli-binary
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-cli") }}
    cargo llvm-cov --no-report --lib --package nervix-backup
    cargo llvm-cov --no-report --lib --package nervix-models -- window_model_digest
    cargo llvm-cov --no-report --lib --package nervix-expiry-map
    cargo llvm-cov --no-report --features testing --package nervix-server --lib -- \
        deduplicator window_ backup restore branch_state runtime_ack snapshot_staging \
        materialized_snapshot checkpoint_stream arrow_body client_batch
    cargo llvm-cov --no-report --features testing --package nervix-server --test scenarios -- \
        --input 'tests/features/cluster/backup_branch_state*.feature' --concurrency 2 --retry 0
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} \
        --package nervix-backup --package nervix-models --package nervix-server --package nervix-cli \
        --package nervix-expiry-map

# Append current checkpoint-reader and backup ownership tests to retained archive profiles.
coverage-backup-archives-state-append output="target/backup-archives.lcov":
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo llvm-cov --no-report --lib --package nervix-backup
    cargo llvm-cov --no-report --features testing --package nervix-server --lib -- materialized_snapshot
    cargo llvm-cov --no-report --features testing --package nervix-server --lib -- backup
    cargo llvm-cov --no-report --features testing --package nervix-server --lib -- runtime::state_store::checkpoint_reader
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} \
        --package nervix-backup --package nervix-nspl --package nervix-server

# Ordinary representation coverage includes the protocol's integration-test target and the
# native host, SDK, archive descriptors and current checkpoint storage codecs.
coverage-wasm output="target/wasm-representations.lcov": build-web-console wasm-processor-guests download-onnxruntime
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo llvm-cov clean --workspace
    cargo llvm-cov --no-report --lib --package nervix-wasm --package nervix-wasm-sdk
    cargo llvm-cov --no-report --lib --tests --package nervix-wasm-protocol
    cargo llvm-cov --no-report --lib --package nervix-backup -- wasm_properties
    cargo llvm-cov --no-report --features testing --lib --package nervix-server -- wasm_properties
    cargo llvm-cov --no-report --features testing --package nervix-server --test scenarios -- \
        --input tests/features/runtime/wasm_processor.feature --name '^Malformed.*output' \
        --concurrency 1 --retry 0
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} \
        --package nervix-arbitrary --package nervix-wasm-protocol --package nervix-wasm-sdk \
        --package nervix-wasm --package nervix-backup --package nervix-server

# Measure the count adapter through vocabulary properties, production storage codecs, language
# archives and window snapshots. These ordinary tests need no external services or containers.
coverage-archive-counts output="target/archive-counts.lcov": tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo llvm-cov clean --workspace
    cargo llvm-cov --no-report --package nervix-models --test representations
    cargo llvm-cov --no-report --package nervix-consensus --lib
    cargo llvm-cov --no-report --package nervix-interconnect --lib
    cargo llvm-cov --no-report --package nervix-nspl --lib -- bolero_
    cargo llvm-cov --no-report --features testing --package nervix-server --lib -- \
        registry::storage::tests
    cargo llvm-cov --no-report --features testing --package nervix-server --lib -- \
        runtime::window_state::tests
    cargo llvm-cov --no-report --features testing --package nervix-server --test scenarios -- \
        --input tests/features/runtime/session_subscription_options.feature \
        --name Blocking.sampled.session --retry 0
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} \
        --package nervix-models --package nervix-arbitrary --package nervix-consensus \
        --package nervix-interconnect --package nervix-server

# Retain collected profiles and export the vocabulary and consensus test lines CI also reports.
coverage-archive-counts-tests-append output="target/archive-counts-tests.lcov":
    cargo llvm-cov --no-report --package nervix-models --test representations
    cargo llvm-cov --no-report --package nervix-consensus --lib
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }}

# Retain the current package profiles while collecting both server archive owners.
coverage-archive-counts-server-append output="target/archive-counts.lcov": download-onnxruntime
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo llvm-cov --no-clean --features testing --package nervix-server --lib -- \
        registry::storage::tests
    cargo llvm-cov --no-clean --features testing --package nervix-server --lib -- \
        runtime::window_state::tests
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} \
        --package nervix-models --package nervix-arbitrary --package nervix-consensus \
        --package nervix-interconnect --package nervix-server

# Measure the server owner regressions with the same dependencies and environment as test-runtime.
coverage-runtime output="target/task-handles-runtime.lcov" *args: build-web-console wasm-processor-guests download-onnxruntime
    ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)" cargo llvm-cov --package nervix-server --features testing --lib --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} {{ args }}

# Ordinary coverage for delivery correlations, membership publication and authenticated relay owners.
coverage-remote-owners output="target/remote-owners.lcov": build-web-console wasm-processor-guests download-onnxruntime
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    just coverage-clean-workspace
    stages="$(mktemp -d {{ quote(cargo_target_dir + "/remote-owner-coverage.XXXXXX") }})"
    cargo llvm-cov --no-report --package nervix-server --features testing --lib -- remote_
    # Later invocations retain the first profile; --no-report and --no-clean cannot be combined.
    cargo llvm-cov --no-clean --lcov --output-path "${stages}/cluster.lcov" \
        --package nervix-server --features testing --lib -- cluster::tests
    cargo llvm-cov --no-clean --lcov --output-path "${stages}/interconnect.lcov" \
        --package nervix-interconnect --lib
    cargo llvm-cov --no-clean --lcov --output-path "${stages}/ack-cost.lcov" \
        --package nervix-server --features testing --lib -- remote_ack_owner_cost --ignored --nocapture
    cargo llvm-cov --no-clean --lcov --output-path "${stages}/frame-cost.lcov" \
        --package nervix-interconnect --lib -- remote_relay_frame_cost --ignored --nocapture
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }}

# Measure selected public scenarios with the same arguments as `test-scenarios`.
coverage-scenarios output *args: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    just coverage-cli-binary
    just coverage-deadlock-report-binary
    export NERVIX_DEADLOCK_REPORT_TOOL={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-deadlock-report") }}
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-cli") }}
    just coverage-paced-simulation-binaries
    export NERVIX_PACED_SIMULATION_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-paced-simulation") }}
    export NERVIX_CLIENT_LIBRARY={{ quote(cargo_target_dir + "/llvm-cov-target/debug/libnervix_client_ffi.so") }}
    install -m 755 {{ quote(cargo_target_dir + "/debug/nervix-nspl-format") }} \
        {{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-nspl-format") }}
    cargo llvm-cov --features testing --test scenarios --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} -- {{ args }}

# Add selected scenarios to the current coverage profiles without rebuilding unchanged artifacts.
coverage-scenarios-append output *args: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    just coverage-cli-binary
    just coverage-deadlock-report-binary
    export NERVIX_DEADLOCK_REPORT_TOOL={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-deadlock-report") }}
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-cli") }}
    just coverage-paced-simulation-binaries
    export NERVIX_PACED_SIMULATION_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-paced-simulation") }}
    export NERVIX_CLIENT_LIBRARY={{ quote(cargo_target_dir + "/llvm-cov-target/debug/libnervix_client_ffi.so") }}
    install -m 755 {{ quote(cargo_target_dir + "/debug/nervix-nspl-format") }} \
        {{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-nspl-format") }}
    cargo llvm-cov --no-clean --features testing --test scenarios --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} -- {{ args }}

# Measure both published paced drivers through their public scenarios, including Python threads
# and Rust unit tests. The Python report is written beside `output` with a `.python.lcov` suffix.
# A compiled scenario binary can run the instrumented drivers without rebuilding the server.
coverage-paced-simulation output="target/paced-simulation.lcov" scenario_binary="" *args: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    collection_dir="$(mktemp -d {{ quote(cargo_target_dir + "/paced-simulation-coverage.XXXXXX") }})"
    export COVERAGE_FILE="${collection_dir}/python.coverage"
    export PYTHONPATH="{{ justfile_directory() }}/scripts/paced_simulation_coverage${PYTHONPATH:+:${PYTHONPATH}}"
    export COVERAGE_PROCESS_START="{{ justfile_directory() }}/scripts/paced_simulation_coverage/coverage.ini"
    if [[ -n {{ quote(scenario_binary) }} ]]; then
        just coverage-paced-simulation-binaries
        (
            source <(CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/llvm-cov-target") }} \
                cargo llvm-cov show-env --sh 2>/dev/null)
            export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/debug/nervix-cli") }}
            export NERVIX_PACED_SIMULATION_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-paced-simulation") }}
            export NERVIX_CLIENT_LIBRARY={{ quote(cargo_target_dir + "/llvm-cov-target/debug/libnervix_client_ffi.so") }}
            install -m 755 {{ quote(cargo_target_dir + "/debug/nervix-nspl-format") }} \
                {{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-nspl-format") }}
            uv run --no-project --with coverage==7.11.0 just test-scenarios-binary {{ quote(scenario_binary) }} \
                --input tests/features/runtime/paced_simulation.feature {{ args }}
        )
    else
        uv run --no-project --with coverage==7.11.0 just coverage-scenarios {{ quote(output) }} \
            --input tests/features/runtime/paced_simulation.feature {{ args }}
    fi
    uvx --from coverage==7.11.0 coverage combine --data-file "${collection_dir}/python.coverage"
    uvx --from coverage==7.11.0 coverage lcov --data-file "${collection_dir}/python.coverage" \
        -o {{ quote(output + ".python.lcov") }}
    just coverage-paced-simulation-report {{ quote(output) }}

# Add the driver's unit coverage to already collected public-driver profiles and export them.
coverage-paced-simulation-report output="target/paced-simulation.lcov":
    cargo llvm-cov --no-clean --package nervix-paced-simulation --lib --lcov --output-path {{ quote(output) }}
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov \
        --output-path {{ quote(output) }}

# Measure the Redis DNS connector, its shared TLS/DNS code, and public source/sink scenarios.
coverage-redis output="target/redis-dns.lcov": tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo llvm-cov clean --workspace
    cargo llvm-cov --no-report --lib \
        --package nervix-dns \
        --package nervix-connector \
        --package nervix-connector-redis
    cargo llvm-cov --no-report --lib --package nervix-server -- redis_
    cargo llvm-cov --no-report --lib --package nervix-server -- sources_that_resolve_names_report_missing_node_dns_as_start_failure
    just coverage-cli-binary
    just coverage-deadlock-report-binary
    export NERVIX_DEADLOCK_REPORT_TOOL={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-deadlock-report") }}
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-cli") }}
    cargo llvm-cov --no-report --features testing --package nervix-server --test scenarios -- \
        --input tests/features/runtime/redis_dns_resolution.feature --name Redis --retry 0 --concurrency 1
    just coverage-redis-report {{ quote(output) }}

coverage-redis-report output="target/redis-dns.lcov":
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} \
        --package nervix-server \
        --package nervix-dns \
        --package nervix-connector \
        --package nervix-connector-redis

coverage-redis-units-append output="target/redis-dns.lcov":
    cargo llvm-cov --no-clean --lib --package nervix-connector-redis
    just coverage-redis-report {{ quote(output) }}

coverage-redis-server-units-append output="target/redis-dns.lcov":
    cargo llvm-cov --no-clean --lib --package nervix-server -- redis_
    cargo llvm-cov --no-clean --lib --package nervix-server -- sources_that_resolve_names_report_missing_node_dns_as_start_failure
    just coverage-redis-report {{ quote(output) }}

# Collect the changed DNS client units and their public one-/three-node paths into one LCOV
# profile so patch coverage can be checked before opening the PR.
coverage-dns-clients output="target/dns-clients.lcov": tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo llvm-cov clean --workspace
    cargo llvm-cov --no-report --all-targets \
        --package nervix-dns \
        --package nervix-client-core \
        --package nervix-client-ffi \
        --package nervix-cli \
        --package nervix-connector \
        --package nervix-connector-http \
        --package nervix-connector-prometheus \
        --package nervix-connector-sentry \
        --package nervix-connector-otel \
        --package nervix-connector-iceberg \
        --package nervix-connector-rabbitmq \
        --package nervix-connector-redis \
        --package nervix-connector-mqtt \
        --package nervix-connector-syslog \
        --package nervix-connector-websockets \
        --package nervix-connector-clickhouse \
        --package nervix-connector-sqs \
        --package nervix-interconnect
    just coverage-cli-binary
    just coverage-deadlock-report-binary
    export NERVIX_DEADLOCK_REPORT_TOOL={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-deadlock-report") }}
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-cli") }}
    cargo llvm-cov --no-report --features testing --package nervix-server --lib
    run_scenario() {
        cargo llvm-cov --no-report --features testing --package nervix-server --test scenarios -- \
            --input "$1" --name "$2" --retry 0 --concurrency 1
    }
    run_scenario tests/features/runtime/http_client_ingestion.feature 'DNS.*fixture'
    run_scenario tests/features/runtime/prometheus_ingestion.feature 'Prometheus.*delivers'
    run_scenario tests/features/runtime/sentry_emission.feature 'Sentry.*publishes'
    run_scenario tests/features/runtime/otel_emission.feature 'OTEL.*trace'
    run_scenario tests/features/runtime/otel_emission.feature 'OTEL.*metric.*HTTP'
    run_scenario tests/features/runtime/client_wire_qualification.feature 'Subscription.restoration'
    run_scenario tests/features/tools/cli_session.feature 'CLI.connects.by.hostname'
    run_scenario tests/features/tools/cli_session.feature 'CLI.rejects.a.TLS'
    run_scenario tests/features/runtime/iceberg_emission.feature 'DNS.*fixture|Iceberg.*holds.*ACK'
    run_scenario tests/features/runtime/rabbitmq_dns_resolution.feature 'RabbitMQ|AMQPS'
    run_scenario tests/features/runtime/redis_dns_resolution.feature 'Redis'
    run_scenario tests/features/runtime/mqtt_dns_resolution.feature 'MQTT'
    run_scenario tests/features/runtime/syslog_dns_resolution.feature 'Syslog'
    run_scenario tests/features/runtime/websocket_client_ingestion.feature 'Websocket client ingestor connects'
    run_scenario tests/features/runtime/websocket_client_tls_resource_mounts.feature 'Websocket client keeps'
    run_scenario tests/features/runtime/websocket_dns_resolution.feature 'WebSocket clients reconnect'
    run_scenario tests/features/runtime/clickhouse_dns_resolution.feature 'ClickHouse'
    run_scenario tests/features/runtime/sqs_dns_resolution.feature 'SQS'
    just coverage-dns-clients-report {{ quote(output) }}

# Export the profiles collected by `coverage-dns-clients` without rebuilding its test binaries.
coverage-dns-clients-report output="target/dns-clients.lcov":
    cargo llvm-cov report --lcov --output-path {{ quote(output) }} \
        --package nervix-server \
        --package nervix-dns \
        --package nervix-client-core \
        --package nervix-client-ffi \
        --package nervix-cli \
        --package nervix-connector \
        --package nervix-connector-http \
        --package nervix-connector-prometheus \
        --package nervix-connector-sentry \
        --package nervix-connector-otel \
        --package nervix-connector-iceberg \
        --package nervix-connector-rabbitmq \
        --package nervix-connector-redis \
        --package nervix-connector-mqtt \
        --package nervix-connector-syslog \
        --package nervix-connector-websockets \
        --package nervix-connector-clickhouse \
        --package nervix-connector-sqs \
        --package nervix-interconnect

# Measure the Shuttle-only test paths, which production-mode workspace coverage cannot compile.
# The canonical runner owns packages, per-check exploration, paired nondeterminism and replay.
coverage-shuttle output filter="":
    python3 -m scripts.native_coverage --target-dir {{ quote(cargo_target_dir) }} run test-shuttle --output {{ quote(output) }} --filter {{ quote(filter) }}

# Run the canonical current-source Loom inventory; weakening qualification remains independent.
coverage-loom output filter="":
    python3 -m scripts.native_coverage --target-dir {{ quote(cargo_target_dir) }} run test-loom --output {{ quote(output) }} --filter {{ quote(filter) }}

# Write line coverage for binary unit tests, such as the CLI's main target.
coverage-bins output *args:
    cargo llvm-cov --bins --lcov --output-path {{ output }} {{ args }}

# Exercise the CLI binary through the public transaction, clock, and REPL reconnect scenarios with
# LLVM coverage.
coverage-cli-process output="target/cli-process.lcov":
    #!/usr/bin/env bash
    set -euo pipefail
    coverage_dir="{{ cargo_target_dir }}/cli-process"
    mkdir -p "$coverage_dir"
    CARGO_TARGET_DIR="$coverage_dir" RUSTFLAGS="-C instrument-coverage ${RUSTFLAGS:-}" \
        cargo build --package nervix-cli --bin nervix-cli
    rm -f "$coverage_dir"/cli-*.profraw
    LLVM_PROFILE_FILE="$coverage_dir/cli-%p-%m.profraw" \
        NERVIX_TEST_CLI_PATH="$coverage_dir/debug/nervix-cli" \
        just test-scenarios --input tests/features/runtime/nspl_transactions.feature --name CLI
    LLVM_PROFILE_FILE="$coverage_dir/cli-%p-%m.profraw" \
        NERVIX_TEST_CLI_PATH="$coverage_dir/debug/nervix-cli" \
        just test-scenarios --input tests/features/tools/cli_session.feature --name clock
    LLVM_PROFILE_FILE="$coverage_dir/cli-%p-%m.profraw" \
        NERVIX_TEST_CLI_PATH="$coverage_dir/debug/nervix-cli" \
        just test-scenarios --input tests/features/tools/cli_session.feature --name keeps.printing
    llvm_bin="$(rustc --print sysroot)/lib/rustlib/$(rustc -vV | awk '/^host:/{print $2}')/bin"
    "$llvm_bin/llvm-profdata" merge -sparse "$coverage_dir"/*.profraw -o "$coverage_dir/merged.profdata"
    "$llvm_bin/llvm-cov" export "$coverage_dir/debug/nervix-cli" \
        --instr-profile="$coverage_dir/merged.profdata" --format=lcov > {{ quote(output) }}

# Measure the Turmoil runner's changed lines without running the full scenario suite.
coverage-turmoil output:
    #!/usr/bin/env bash
    set -euo pipefail
    export RUSTFLAGS="--cfg tokio_unstable ${RUSTFLAGS:-}"
    cargo llvm-cov --no-report \
        --package nervix-primitives --features 'turmoil native' --lib -- \
        simulated_host turmoil_mode --test-threads=1
    cargo llvm-cov --no-report \
        --package nervix-execution --features turmoil --lib
    cargo llvm-cov --no-report \
        --package nervix-interconnect --features turmoil --lib -- \
        wire::simulation_checks authentication::simulation_tests --test-threads=1
    cargo llvm-cov --no-report \
        --package nervix-interconnect --features turmoil --test simulation -- --test-threads=1
    cargo llvm-cov report --no-default-ignore-filename-regex \
        --lcov --output-path {{ quote(output) }}

# Run the eligible native extras in their declared modes, with the canonical Shuttle and Loom
# runners and each primitive conformance mode. `test-primitives` selects all native conformance.
# Prerequisites build outside instrumentation. Compile/browser checks and weakening qualification
# run independently. Each producer
# writes lcov.info, completion.json, executions.jsonl and export.log to a fresh
# target/native-coverage/<producer>/<mode>/<toolchain>/<attempt>/, and CI runs one per step.
# Run all: `just coverage-native-extras`; one: `just coverage-native-extras bench-smoke`.
coverage-native-extras *producers:
    python3 -m scripts.native_coverage --target-dir {{ quote(cargo_target_dir) }} run {{ producers }}

# Exercise the native coverage collector: its producer inventory, source policy, selection and
# failure handling, then instrumented runs of a fixture crate through the real toolchain.
test-native-coverage:
    NERVIX_NATIVE_COVERAGE_TOOLCHAIN_TESTS=required python3 -m unittest --quiet scripts.tests.test_native_coverage scripts.tests.test_model_coverage

# Exercise the Deloxide lane runner: its inventory, discovery, supervision of real processes,
# accounting, record, replay, qualification classes, applicability check and CI contract.
test-deloxide-lane-runner:
    python3 -m unittest --quiet scripts.tests.test_deloxide_lane

# Line coverage of collection and canonical runner changes, with actual command/artifact fixtures,
# including the Deloxide lane's supervision of real processes.
coverage-model-runner:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p target/model-coverage
    coverage=(uvx --from coverage==7.11.0 coverage)
    export COVERAGE_FILE="{{ cargo_target_dir }}/model-coverage/python.coverage"
    NERVIX_NATIVE_COVERAGE_TOOLCHAIN_TESTS=required "${coverage[@]}" run --branch --source=scripts.native_coverage,scripts.coverage_workspace_wrapper,scripts.model_evidence,scripts.shuttle_checks,scripts.loom_models,scripts.deloxide_lane -m unittest scripts.tests.test_native_coverage scripts.tests.test_model_coverage scripts.tests.test_shuttle_checks scripts.tests.test_loom_models scripts.tests.test_deloxide_lane
    "${coverage[@]}" lcov -o "{{ cargo_target_dir }}/model-coverage/python.lcov"
    "${coverage[@]}" report

# Run every Criterion suite with the release profile. Extra arguments are forwarded to Criterion.
# The server benches link the console the server serves, so the console is built first rather than
# left to whatever ran before them.
bench *args: build-web-console
    cargo bench --package nervix-server --bench relay_interaction --features benchmarks -- {{ args }}
    cargo bench --package nervix-branch-instances --bench owned_branches -- {{ args }}
    cargo bench --package nervix-server --bench subscription_row_encoding --features benchmarks -- {{ args }}
    cargo bench --package nervix-server --bench wasm_checkpoint --features benchmarks -- {{ args }}
    cargo bench --package nervix-server --bench state_replication --features benchmarks -- {{ args }}
    cargo bench --package nervix-columnar-json --bench json_encode -- {{ args }}
    cargo bench --package nervix-vm --bench vm -- {{ args }}

# Exercise every Criterion body once without spending CI's smoke-test budget on release codegen.
bench-smoke: build-web-console wasm-processor-guests download-onnxruntime bench-smoke-bodies

# The Criterion bodies `bench-smoke` exercises, without the console build that precedes them there.
# `coverage-native-extras` builds the console outside its instrumentation and then runs these in it.
bench-smoke-bodies:
    cargo bench --profile dev --package nervix-server --bench relay_interaction --features benchmarks -- --test
    cargo bench --profile dev --package nervix-server --bench admitted_work --features benchmarks -- --test
    cargo bench --profile dev --package nervix-branch-instances --bench owned_branches -- --test
    cargo bench --profile dev --package nervix-server --bench subscription_row_encoding --features benchmarks -- --test
    cargo bench --profile dev --package nervix-server --bench wasm_checkpoint --features benchmarks -- --test
    cargo bench --profile dev --package nervix-server --bench task_handles --features benchmarks -- target/task-handles-smoke.json
    cargo bench --profile dev --package nervix-server --bench state_replication --features benchmarks -- --test
    cargo bench --profile dev --package nervix-columnar-json --bench json_encode -- --test
    cargo bench --profile dev --package nervix-simd-kernels --bench constant_division -- --test
    cargo bench --profile dev --package nervix-simd-kernels --bench byte_classes -- --test
    cargo bench --profile dev --package nervix-connector-syslog --bench stream_framing --features benchmarks -- --test
    cargo bench --profile dev --package nervix-vm --bench vm -- --test
    just bench-retained-channels-bodies
    just bench-remote-owners-bodies
    just bench-materialized-state-bodies

# Measure the data-plane work a node admits through its bounded executor, as the runtime submits it:
# one branched input prepared into its branch batches, and an emitter batch encoded through a JAQ
# transformation. Extra arguments are forwarded to Criterion.
bench-admitted-work *args: build-web-console
    cargo bench --package nervix-server --bench admitted_work --features benchmarks -- {{ args }}

# Exercise the admission benchmark's current emitter context while measuring its line coverage.
coverage-admitted-work output="target/admitted-work.lcov": build-web-console
    cargo llvm-cov --bench admitted_work --features benchmarks --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} -- --test

# Run only the relay-interaction Criterion suite, including the delivery a node input records for
# one batch at 1, 64, and 1,024 rows. Extra arguments are forwarded to Criterion.
bench-relay-interaction *args: build-web-console
    cargo bench --package nervix-server --bench relay_interaction --features benchmarks -- {{ args }}

# Measure the branch owner a relay owner task holds: batches for established branches, which
# publish nothing, and branch churn, which creates, evicts and publishes once per batch.
bench-branch-instances *args:
    cargo bench --package nervix-branch-instances --bench owned_branches -- {{ args }}

# Build the SIMD kernel crate's optimized unit-test binary for the x86-64-v3 payload the Docker
# image ships, in its own target directory, so the generated instructions of each dispatch level can
# be inspected with objdump without the host's native CPU tuning.
build-simd-kernels-x86-64-v3:
    CARGO_TARGET_DIR="{{ cargo_target_dir }}/simd-kernels-x86-64-v3" RUSTFLAGS="-C target-cpu=x86-64-v3" cargo test --release --package nervix-simd-kernels --lib --no-run

# Measure one batch of schemaful JSON rows, including the escape classification made once per
# Arrow batch. The suite compares the column writer against serde's per-row reference encoding.
bench-json-encode *args:
    cargo bench --package nervix-columnar-json --bench json_encode -- {{ args }}

# Measure direct Arrow-to-Row subscription encoding. The suite reports encoded bytes before
# Criterion measures CPU; its unit probe measures allocations.
bench-subscription-rows *args:
    cargo bench --package nervix-server --bench subscription_row_encoding --features benchmarks -- {{ args }}

# Write raw component timing and retained-allocation samples for Arrow-to-Row delivery.
client-wire-cost output="target/client-wire-cost.json":
    cargo bench --package nervix-server --bench client_wire_cost --features benchmarks -- {{ quote(output) }}

# Run the component benchmark with coverage instrumentation for changed benchmark lines.
coverage-client-wire-cost output="target/client-wire-cost.lcov" report="target/client-wire-cost-coverage.json":
    cargo llvm-cov --bench client_wire_cost --features benchmarks --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} -- {{ quote(report) }}

# Check the typed Arrow workload's selection, null, redaction, branch and frame-limit assertions.
test-client-wire-bench-fixture:
    cargo test --package nervix-server --features benchmarks --lib subscription_row::benchmark::tests -- --nocapture

# Capture raw timings through the exported Rust binding's C ABI on a 100-row frame.
client-wire-binding-cost output="target/client-wire-binding-cost.json":
    NERVIX_CLIENT_WIRE_BINDING_COST_OUTPUT="$(realpath -m {{ quote(output) }})" cargo test --package nervix-client-ffi --lib tests::profiles_bulk_binding_access_and_retain_release -- --exact --nocapture

# Measure CPython ctypes and GC overhead on the same live rows used by conformance.
client-wire-binding-host-cost output_dir="target/client-wire-binding-host":
    NERVIX_CLIENT_WIRE_BINDING_PROFILE_DIR="$(realpath -m {{ quote(output_dir) }})" just test-client-conformance '@client_probe_python' --concurrency 1 --retry 0

# Compare the same native command workload over plaintext and TLS with one-node test clusters.
client-wire-tls-cost output_dir="target/client-wire-tls-cost":
    NERVIX_CLIENT_WIRE_TLS_OUTPUT_DIR={{ quote(output_dir) }} just test-scenarios --input tests/features/runtime/client_wire_tls_cost.feature --tags @client_wire_tls_cost --concurrency 1 --retry 0

# Measure runtime-state replication on the owner and on a replica: a Kafka offset commit its one
# replica acknowledges, and the branch lifecycle check a replica makes before it installs each
# branch checkpoint, for lifecycles of 16, 128 and 1,024 branches.
bench-state-replication *args: build-web-console
    cargo bench --package nervix-server --bench state_replication --features benchmarks -- {{ args }}

# Measure durable WASM guest-state checkpoints against unsynchronized writes of the same states. The
# store lives under the crate target directory, so the synchronization cost is that of its storage.
bench-wasm-checkpoint *args:
    cargo bench --package nervix-server --bench wasm_checkpoint --features benchmarks -- {{ args }}

# Run only the expression VM Criterion suite. Extra arguments are forwarded to Criterion, so a
# group filter and `--save-baseline` or `--baseline` compare VM kernels without the relay suite.
bench-vm *args:
    cargo bench --package nervix-vm --bench vm -- {{ args }}

# Build the VM Criterion binary for the x86-64-v3 payload the Docker image ships, in its own target
# directory, so the checked lanes and their failure packing can be inspected with objdump without
# the host's native CPU tuning.
build-vm-bench-x86-64-v3:
    CARGO_TARGET_DIR="{{ cargo_target_dir }}/vm-x86-64-v3" RUSTFLAGS="-C target-cpu=x86-64-v3" cargo bench --package nervix-vm --bench vm --no-run

# Run the same VM Criterion harness with one-shot allocation and output-size probes. The
# instrumentation is compiled only for this recipe; use `bench-vm` for timing comparisons.
bench-vm-alloc *args:
    cargo bench --package nervix-vm --bench vm --features benchmark-allocations -- {{ args }}

# Build the reusable harness and forward its CLI arguments. This is enough for container subjects
# such as Vector; local Nervix has a dedicated recipe below because it also builds the server.
benchmark *args:
    cargo build --release --package nervix-benchmark --bins
    "{{ cargo_target_dir }}/release/nervix-benchmark" {{ args }}

# Focused validation for the benchmark framework without building product binaries.
test-benchmark-framework *args:
    cargo test --package nervix-benchmark {{ args }}

# Build the pinned Flink image with its matching Kafka SQL connector.
benchmark-flink-image:
    docker build --file "{{ justfile_directory() }}/benches/flink/Dockerfile" \
        --tag nervix-benchmark-flink:2.0.1 \
        "{{ justfile_directory() }}/benches/flink"

# Build and benchmark the current local Nervix checkout.
benchmark-nervix-local benchmark_name="kafka-filter-map" *args: build-web-console
    cargo build --release \
        --package nervix-server --bin nervix-server \
        --package nervix-benchmark --bins
    "{{ cargo_target_dir }}/release/nervix-benchmark" run {{ quote(benchmark_name) }} \
        --implementation nervix --nervix-mode local \
        --server-binary "{{ cargo_target_dir }}/release/nervix-server" {{ args }}

# Run a tagged Nervix server image; the harness configures it through the client-core API.
benchmark-nervix-image image benchmark_name="kafka-filter-map" *args:
    cargo build --release --package nervix-benchmark --bins
    "{{ cargo_target_dir }}/release/nervix-benchmark" run {{ quote(benchmark_name) }} \
        --implementation nervix --nervix-mode image --nervix-image {{ quote(image) }} {{ args }}

# Build once, then run every declared workload implementation sequentially with local Nervix.
benchmark-all-local *args: build-web-console benchmark-flink-image
    cargo build --release \
        --package nervix-server --bin nervix-server \
        --package nervix-benchmark --bins
    "{{ cargo_target_dir }}/release/nervix-benchmark" run-all \
        --nervix-mode local \
        --server-binary "{{ cargo_target_dir }}/release/nervix-server" {{ args }}

# Local same-hardware A/B: build the baseline ref and the current tree once each into cached
# binaries under target/ab/, then interleave runs per arm so machine drift cancels out. This is
# how performance claims are established; CI benchmark comments are only a same-run smoke signal.
benchmark-ab baseline_ref runs="3" benchmark_name="kafka-filter-map" *args: build-web-console
    #!/usr/bin/env bash
    set -euo pipefail
    baseline_commit="$(git rev-parse --verify --end-of-options {{ quote(baseline_ref) }}'^{commit}')" || {
        echo "error:" {{ quote(baseline_ref) }} "does not resolve to a commit" >&2
        exit 1
    }
    ab_root="{{ cargo_target_dir }}/ab"
    case "${ab_root}" in /*) ;; *) ab_root="${PWD}/${ab_root}" ;; esac
    baseline_binary="${ab_root}/${baseline_commit}/nervix-server"
    if [[ ! -x "${baseline_binary}" ]]; then
        worktree="${ab_root}/worktree"
        git worktree prune
        if [[ -e "${worktree}" ]]; then
            git worktree remove --force "${worktree}" || { rm -rf "${worktree}"; git worktree prune; }
        fi
        git worktree add --detach "${worktree}" "${baseline_commit}"
        (cd "${worktree}" \
            && export CARGO_TARGET_DIR="${ab_root}/build" \
            && just build-web-console \
            && cargo build --release --package nervix-server --bin nervix-server)
        install -D "${ab_root}/build/release/nervix-server" "${baseline_binary}"
        git worktree remove --force "${worktree}"
    fi
    cargo build --release \
        --package nervix-server --bin nervix-server \
        --package nervix-benchmark --bins
    install -D "{{ cargo_target_dir }}/release/nervix-server" "${ab_root}/candidate/nervix-server"
    "{{ cargo_target_dir }}/release/nervix-benchmark" run-ab {{ quote(benchmark_name) }} \
        --baseline-binary "${baseline_binary}" \
        --candidate-binary "${ab_root}/candidate/nervix-server" \
        --baseline-label {{ quote(baseline_ref) }}" @ ${baseline_commit:0:12}" \
        --candidate-label "working tree" \
        --runs {{ quote(runs) }} {{ args }}

# Build only the benchmark harness, then run it against an already-built Nervix image. The harness
# configures the server directly through client-core and never rebuilds a product binary.
benchmark-ci nervix_image artifacts_root *args: benchmark-flink-image
    #!/usr/bin/env bash
    set -euo pipefail
    test -S /var/run/docker.sock
    docker image inspect {{ quote(nervix_image) }} >/dev/null
    cargo build --release --package nervix-benchmark --bins
    "{{ cargo_target_dir }}/release/nervix-benchmark" \
        --repository-root "{{ justfile_directory() }}" \
        run-all --nervix-mode image --nervix-image {{ quote(nervix_image) }} \
        --artifacts-root {{ quote(artifacts_root) }} {{ args }}

cargo-fmt:
    cargo +nightly fmt

taplo-format:
    taplo format

[parallel]
fmt: cargo-fmt fmt-typed-ratchet taplo-format dockerfmt gherkin-fmt nspl-fmt autoinherit

cargo-fmt-check:
    cargo +nightly fmt --check

taplo-format-check:
    taplo format --check

[parallel]
fmt-check: cargo-fmt-check fmt-check-typed-ratchet taplo-format-check dockerfmt-check gherkin-fmt-check nspl-fmt-check autoinherit-check

gherkin-fmt:
    ghokin fmt replace tests/features

gherkin-fmt-check:
    ghokin check tests/features

# Format every NSPL file in the repository.
nspl-fmt:
    cargo run -q --package nervix-nspl-format -- .

# Report every NSPL file in the repository that is not formatted.
nspl-fmt-check:
    cargo run -q --package nervix-nspl-format -- --check .

build-nspl-format:
    cargo build --package nervix-nspl-format

autoinherit:
    cargo autoinherit

autoinherit-check:
    #!/bin/bash
    set -e
    git diff --exit-code 2>/dev/null >/dev/null  || echo "skip autoinherit on dirty working tree" && exit 0
    cargo autoinherit
    git diff --exit-code

# Each package/configuration is a separate parallel dependency. The one Cargo recipe below
# hashes its arguments and toolchain into an isolated directory, and `just --jobs N` bounds it.
clippy_all_features_packages := [
    "nervix-approx-into",
    "nervix-arbitrary",
    "nervix-backup",
    "nervix-benchmark",
    "nervix-bounded-write",
    "nervix-branch-instances",
    "nervix-checkpoint-replication",
    "nervix-cli",
    "nervix-client-ffi",
    "nervix-client-wire",
    "nervix-columnar-json",
    "nervix-dataflow-graph",
    "nervix-dns",
    "nervix-expiry-map",
    "nervix-jaq",
    "nervix-models",
    "nervix-nspl",
    "nervix-nspl-format",
    "nervix-primitives-macros",
    "nervix-recovery",
    "nervix-roto",
    "nervix-simd-kernels",
    "nervix-test-environment",
    "nervix-vm",
    "nervix-wasm-protocol",
    "nervix-wasm-sdk",
    "nervix-web-console",
]

clippy_connector_packages := [
    "nervix-connector",
    "nervix-connector-clickhouse",
    "nervix-connector-http",
    "nervix-connector-iceberg",
    "nervix-connector-kafka",
    "nervix-connector-mongodb",
    "nervix-connector-mqtt",
    "nervix-connector-mysql",
    "nervix-connector-nats",
    "nervix-connector-otel",
    "nervix-connector-postgres",
    "nervix-connector-prometheus",
    "nervix-connector-pulsar",
    "nervix-connector-rabbitmq",
    "nervix-connector-redis",
    "nervix-connector-sentry",
    "nervix-connector-sqs",
    "nervix-connector-syslog",
    "nervix-connector-websockets",
    "nervix-connector-zeromq",
]

clippy_shuttle_packages := [
    "nervix-client-core",
    "nervix-connector",
    "nervix-connector-clickhouse",
    "nervix-connector-http",
    "nervix-connector-iceberg",
    "nervix-connector-mongodb",
    "nervix-connector-mqtt",
    "nervix-connector-mysql",
    "nervix-connector-nats",
    "nervix-connector-otel",
    "nervix-connector-postgres",
    "nervix-connector-pulsar",
    "nervix-connector-rabbitmq",
    "nervix-connector-redis",
    "nervix-connector-sentry",
    "nervix-connector-sqs",
    "nervix-connector-syslog",
    "nervix-connector-zeromq",
    "nervix-execution",
    "nervix-interconnect",
    "nervix-wasm",
]

[private, parallel]
clippy-targets: ordinary-clippy-targets shuttle-clippy-targets turmoil-clippy-targets loom-clippy-targets deloxide-clippy-targets

[private, parallel]
ordinary-clippy-targets: \
    *(clippy-target *clippy_all_features_packages ["--all-features", "--all-targets"]) \
    *(clippy-target *clippy_connector_packages ["--all-targets"]) \
    *(clippy-target *["nervix-execution", "nervix-interconnect", "nervix-model-harness", "nervix-wasm"] ["--all-targets"]) \
    (clippy-target "nervix-server" ["--all-targets", "--features", "benchmarks testing"]) \
    (clippy-target "nervix-client-core" ["--all-targets", "--features", "autocomplete"]) \
    (clippy-target "nervix-paced-simulation" ["--all-targets"]) \
    (clippy-target "nervix-consensus" ["--all-targets", "--features", "testing"]) \
    (clippy-target "nervix-primitives" ["--all-targets", "--features", "native"]) \
    (clippy-target "nervix-primitives" ["--all-targets", "--features", "native test-util"]) \
    (clippy-target "nervix-deadlock" ["--all-targets", "--features", "report-tool"]) \
    *(clippy-target *["nervix-cli", "nervix-server", "nervix-nspl-format", "nervix-web-console"] ["--all-targets"]) \
    (clippy-target "nervix-client-wire" ["--target", "wasm32-unknown-unknown"])

# Lint every Shuttle build: each library under the mode, the primitive boundary, and each
# package `test-shuttle` explores in test mode. The full validation matrix runs the same targets.
cargo-clippy-shuttle jobs=default_jobs: (run-with-jobs "shuttle-clippy-targets" jobs)

# The `--profile test` targets lint each package `test-shuttle` explores as the runner builds it,
# with its checks compiled. The library targets never compile them, so a warning in a check would
# otherwise pass validation and the Shuttle job alike.
[private, parallel]
shuttle-clippy-targets: \
    *(clippy-target *clippy_shuttle_packages ["--lib", "--features", "shuttle"]) \
    *(clippy-target *["nervix-connector-kafka", "nervix-consensus", "nervix-server"] ["--lib", "--features", "shuttle testing"]) \
    *(clippy-target *["nervix-connector-prometheus", "nervix-connector-websockets"] ["--lib", "--features", "nervix-connector/shuttle nervix-primitives/shuttle"]) \
    (clippy-target "nervix-primitives" ["--all-targets", "--features", "shuttle native"]) \
    *(clippy-target *shuttle_test_packages ["--lib", "--profile", "test", "--features", "shuttle"])

[private, parallel]
turmoil-clippy-targets: \
    *(clippy-target *["nervix-execution", "nervix-interconnect"] ["--all-targets", "--features", "turmoil"]) \
    (clippy-target "nervix-primitives" ["--all-targets", "--features", "turmoil native"])

# Lint the Loom models and harness, the primitive boundary, and each library as it ships and
# in test mode. The same parallel dependencies run in the full validation matrix.
cargo-clippy-loom jobs=default_jobs: (run-with-jobs "loom-clippy-targets" jobs)

[private, parallel]
loom-clippy-targets: \
    *(clippy-target *["nervix-execution", "nervix-model-harness", "nervix-interconnect"] ["--all-targets", "--features", "loom"]) \
    (clippy-target "nervix-primitives" ["--all-targets", "--features", "loom native"]) \
    *(clippy-target *["nervix-consensus", "nervix-server"] ["--lib", "--features", "loom"]) \
    (clippy-target "nervix-server" ["--lib", "--profile", "test", "--features", "loom"]) \
    (clippy-target "nervix-consensus" ["--lib", "--profile", "test", "--features", "loom testing"])

# Lint the diagnostic mode: the tracked locks and the detector, the deadlock diagnostics with their
# probes, and the server as a diagnostic node, alone and with its tests and scenario binary.
cargo-clippy-deloxide jobs=default_jobs: (run-with-jobs "deloxide-clippy-targets" jobs)

[private, parallel]
deloxide-clippy-targets: \
    (clippy-target "nervix-primitives" ["--all-targets", "--features", "deloxide native"]) \
    (clippy-target "nervix-deadlock" ["--all-targets", "--features", "deloxide"]) \
    (clippy-target "nervix-paced-simulation" ["--all-targets", "--features", "deloxide"]) \
    (clippy-target "nervix-server" ["--lib", "--bins", "--features", "deloxide"]) \
    (clippy-target "nervix-server" ["--all-targets", "--features", "deloxide testing"]) \
    (clippy-target "nervix-primitives" ["--all-targets", "--features", "deloxide-order native"]) \
    (clippy-target "nervix-deadlock" ["--all-targets", "--features", "deloxide-order"]) \
    (clippy-target "nervix-paced-simulation" ["--all-targets", "--features", "deloxide-order"]) \
    (clippy-target "nervix-server" ["--lib", "--bins", "--features", "deloxide-order"]) \
    (clippy-target "nervix-server" ["--all-targets", "--features", "deloxide-order testing"])

# The shared Clippy command accepts one package and its Cargo arguments. Target, feature,
# profile and toolchain differences identify separate build directories. Keep kache configured.
# On CI nothing reads a build directory after its lint and the runner's disk cannot hold them all,
# so a target that passed deletes its own.
[private]
clippy-target package args toolchain="":
    CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/clippy/" + package + "/" + sha256(show([args, toolchain]))) }} RUSTFLAGS={{ quote("-Dwarnings " + rustflags) }} cargo {{ if toolchain == "" { "" } else { quote("+" + toolchain) } }} clippy --package {{ quote(package) }} {{ quote(args) }} -q
    @{{ if env("CI", "") == "true" { "rm -rf " + quote(cargo_target_dir + "/clippy/" + package + "/" + sha256(show([args, toolchain]))) } else { "true" } }}

# Lint one package and all of its targets; extra arguments retain their boundaries.
cargo-clippy-package package *args: (clippy-target package ["--all-targets", args])

# Lint every product Clippy target, with four concurrent processes by default.
cargo-clippy jobs=default_jobs: (run-with-jobs "clippy-targets" jobs)

[private, parallel]
lint-inner: clippy-targets typed-ratchet-clippy-targets

# Lint the product and compiler tooling in one bounded invocation.
lint jobs=default_jobs: (run-with-jobs "lint-targets" jobs)

[private]
lint-targets: build-web-console lint-inner

audit:
    cargo audit

# Count the architecture debt and fail when a count is above its baseline in debt-baseline.json.
ratchet *args: typed-ratchet-build
    python3 -m scripts.ratchet {{ args }}

# Focused regression checks for the debt gate and its shared Rust source scanner.
test-ratchet-units:
    python3 -m unittest scripts.tests.test_ratchet

typed-ratchet-build:
    CARGO_TARGET_DIR={{ cargo_target_dir }}/typed-ratchet/driver cargo +nightly-2026-09-17 build --manifest-path tools/nervix-lint/Cargo.toml --package nervix-lint-driver --package nervix-lint-report

typed-ratchet *args: typed-ratchet-build
    python3 -m scripts.typed_ratchet {{ args }}

typed-ratchet-turmoil *args:
    RUSTFLAGS="--cfg tokio_unstable {{ rustflags }}" python3 -m scripts.typed_ratchet {{ args }}

test-typed-ratchet-compiler:
    just typed-ratchet --fixture-mode ordinary --recompile --inventory --output {{ quote(cargo_target_dir) }}/typed-ratchet/fixture-ordinary.json
    python3 -m unittest scripts.tests.test_typed_ratchet scripts.tests.compiler_fixture_checks.CompilerFixtureTests

test-typed-ratchet-contracts: typed-ratchet-build
    python3 -m unittest scripts.tests.compiler_contract_checks

test-typed-ratchet: test-typed-ratchet-ordinary test-typed-ratchet-product-docs test-typed-ratchet-modeled

test-typed-ratchet-ordinary:
    just test-typed-ratchet-reports
    just test-typed-ratchet-compiler
    just test-typed-ratchet-contracts
    just test-typed-ratchet-docs

test-typed-ratchet-modeled:
    just typed-ratchet --fixture-mode shuttle --inventory --output {{ quote(cargo_target_dir) }}/typed-ratchet/fixture-shuttle.json
    just typed-ratchet --fixture-mode loom --inventory --output {{ quote(cargo_target_dir) }}/typed-ratchet/fixture-loom.json
    just typed-ratchet --fixture-mode turmoil --inventory --output {{ quote(cargo_target_dir) }}/typed-ratchet/fixture-turmoil.json
    just typed-ratchet --fixture-mode deloxide --inventory --output {{ quote(cargo_target_dir) }}/typed-ratchet/fixture-deloxide.json
    python3 -m unittest scripts.tests.compiler_fixture_checks.ModeledFixtureTests

test-typed-ratchet-docs:
    #!/usr/bin/env bash
    set -euo pipefail
    doctest_flags="${RUSTDOCFLAGS:-} -Z unstable-options --persist-doctests {{ cargo_target_dir }}/typed-ratchet/doctests"
    if [[ -n "${NERVIX_NATIVE_COVERAGE_ATTEMPT:-}" ]]; then
        doctest_flags+=" -C instrument-coverage"
    fi
    RUSTDOCFLAGS="$doctest_flags" CARGO_TARGET_DIR={{ cargo_target_dir }}/typed-ratchet/docs \
        cargo +nightly-2026-09-17 test --manifest-path tools/nervix-lint/fixtures/Cargo.toml --doc

# Current API examples compile alongside their compile_fail counterparts on the product toolchain.
test-typed-ratchet-product-docs:
    cargo +1.99 test --package nervix-primitives --doc expect_lint
    cargo +1.99 test --package nervix-vm --doc FunctionInjector
    RUSTUP_TOOLCHAIN=1.99 just test-runtime-state-capabilities

qualify-typed-ratchet-cache: typed-ratchet-build
    python3 -m scripts.tests.qualify_typed_ratchet_cache

test-typed-ratchet-reports:
    CARGO_TARGET_DIR={{ cargo_target_dir }}/typed-ratchet/driver cargo +nightly-2026-09-17 test --manifest-path tools/nervix-lint/Cargo.toml --package nervix-lint-report --lib

fmt-typed-ratchet:
    cargo +nightly-2026-09-17 fmt --manifest-path tools/nervix-lint/Cargo.toml --all
    cargo +nightly-2026-09-17 fmt --manifest-path tools/nervix-lint/fixtures/Cargo.toml --all

fmt-check-typed-ratchet:
    cargo +nightly-2026-09-17 fmt --manifest-path tools/nervix-lint/Cargo.toml --all --check
    cargo +nightly-2026-09-17 fmt --manifest-path tools/nervix-lint/fixtures/Cargo.toml --all --check

# Tooling packages use the same bounded scheduler and command, with their pinned compiler.
lint-typed-ratchet jobs=default_jobs: (run-with-jobs "typed-ratchet-clippy-targets" jobs)

[private]
typed-ratchet-clippy-targets: typed-ratchet-clippy-packages

[private, parallel]
typed-ratchet-clippy-packages: *(clippy-target *["nervix-lint-driver", "nervix-lint-report"] ["--manifest-path", "tools/nervix-lint/Cargo.toml", "--all-targets"] "nightly-2026-09-17")

coverage-typed-ratchet-python: test-typed-ratchet-compiler test-typed-ratchet-modeled
    #!/usr/bin/env bash
    set -euo pipefail
    coverage=(uvx --from coverage==7.11.0 coverage)
    export COVERAGE_FILE="{{ cargo_target_dir }}/typed-ratchet/python.coverage"
    "${coverage[@]}" run --branch --source=scripts.typed_ratchet,scripts.typed_lint_wrapper,scripts.ratchet,scripts.native_coverage,scripts.bolero,scripts.bolero_coverage,scripts.build_web_console,scripts.tests.test_bolero,scripts.tests.test_bolero_coverage -m unittest scripts.tests.test_typed_ratchet scripts.tests.compiler_fixture_checks scripts.tests.test_ratchet scripts.tests.test_native_coverage scripts.tests.test_bolero scripts.tests.test_bolero_coverage
    "${coverage[@]}" lcov -o "{{ cargo_target_dir }}/typed-ratchet/python.lcov"
    "${coverage[@]}" report

# Format and validate, with four concurrent recipe bodies by default.
validate jobs=default_jobs: (run-with-jobs "validate-targets" jobs)

[private]
validate-targets: fmt lint-targets validate-skill validate-nspl-docs validate-clock-boundaries validate-typed-errors validate-primitive-boundary validate-execution-mode-dependencies validate-execution-mode-conflicts validate-dns-dependencies ratchet validate-deloxide-applicability

# Check each connector as a consumer root. Cargo tree limits feature unification to that root;
# the full workspace build alone can hide a missing resolver feature in a leaf connector.
validate-dns-dependencies:
    #!/usr/bin/env bash
    set -euo pipefail
    for package in nervix-connector-http nervix-connector-prometheus nervix-connector-sentry nervix-connector-otel nervix-connector-iceberg; do
        graph="$(cargo tree --package "${package}" --edges normal --format '{p} {f}' --prefix none)"
        if ! rg -q '^reqwest v0\.13\.[0-9]+ .*hickory-dns' <<< "${graph}"; then
            echo "${package} lacks Reqwest 0.13 Hickory DNS" >&2
            exit 1
        fi
        if [[ "${package}" == nervix-connector-iceberg ]] && \
            ! rg -q '^reqwest v0\.12\.[0-9]+ .*hickory-dns' <<< "${graph}"; then
            echo "${package} lacks Reqwest 0.12 Hickory DNS" >&2
            exit 1
        fi
        if [[ "${package}" == nervix-connector-iceberg ]] && \
            rg -q '^reqwest v0\.12\.[0-9]+ [^ ]*__rustls-ring' <<< "${graph}"; then
            echo "${package} selected Reqwest 0.12's Ring TLS provider" >&2
            exit 1
        fi
    done
    # RabbitMQ resolves through the node's Hickory resolver and hands Lapin the transport it
    # established. Neither the connector on its own nor the server may select the process-wide
    # Hickory resolver behind Lapin's own feature, and Lapin's TLS stays on AWS-LC.
    for package in nervix-connector-rabbitmq nervix-server; do
        graph="$(cargo tree --package "${package}" --edges normal --format '{p} {f}' --prefix none)"
        if ! rg -q '^nervix-dns v' <<< "${graph}" || \
            ! rg -q '^hickory-resolver v0\.26\.[0-9]+ .*tokio' <<< "${graph}"; then
            echo "${package} lacks the node resolver RabbitMQ connects through" >&2
            exit 1
        fi
        if rg -q '^(lapin|amq-protocol|amq-protocol-tcp|async-rs) v[^ ]+ .*hickory-dns' <<< "${graph}"; then
            echo "${package} selected Lapin's process-wide Hickory resolver" >&2
            exit 1
        fi
        if ! rg -q '^tcp-stream v[^ ]+ .*rustls--aws_lc_rs' <<< "${graph}" || \
            rg -q '^tcp-stream v[^ ]+ .*rustls--ring' <<< "${graph}"; then
            echo "${package} does not select AWS-LC for RabbitMQ TLS" >&2
            exit 1
        fi
    done
    # Syslog, WebSocket, Redis and MQTT transports resolve through the node resolver, even when each
    # connector is built without the server's feature graph.
    for package in nervix-connector-syslog nervix-connector-websockets nervix-connector-redis nervix-connector-mqtt; do
        graph="$(cargo tree --package "${package}" --edges normal --format '{p} {f}' --prefix none)"
        if ! rg -q '^nervix-dns v' <<< "${graph}" || \
            ! rg -q '^hickory-resolver v0\.26\.[0-9]+ .*tokio' <<< "${graph}"; then
            echo "${package} lacks the node resolver for its outbound connections" >&2
            exit 1
        fi
    done
    graph="$(cargo tree --package nervix-connector-redis --edges normal --format '{p} {f}' --prefix none)"
    if ! rg -q '^redis v1\.[0-9]+\.[0-9]+ .*tokio-rustls-comp' <<< "${graph}" || \
        rg -q '^rustls v[^ ]+ (.*,)?ring(,|$)' <<< "${graph}"; then
        echo "nervix-connector-redis lacks Redis's AWS-LC TLS path" >&2
        exit 1
    fi
    # MQTT dials each resolved address with rumqttc's own per-address dialer from rumqttc-core,
    # and the driver still completes TLS on the stream, on AWS-LC alone.
    graph="$(cargo tree --package nervix-connector-mqtt --edges normal --format '{p} {f}' --prefix none)"
    if ! rg -q '^rumqttc-v5-next v[^ ]+ (.*,)?use-rustls-aws-lc(,|$)' <<< "${graph}" || \
        rg -q '^rustls v[^ ]+ (.*,)?ring(,|$)' <<< "${graph}"; then
        echo "nervix-connector-mqtt lacks rumqttc's AWS-LC TLS path" >&2
        exit 1
    fi
    # ClickHouse and SQS hand the node resolver to their drivers' own DNS hooks, Hyper's connector
    # and Smithy's HTTP client, even when built without the server's feature graph, and complete
    # TLS with AWS-LC alone.
    for package in nervix-connector-clickhouse nervix-connector-sqs; do
        graph="$(cargo tree --package "${package}" --edges normal --format '{p} {f}' --prefix none)"
        if ! rg -q '^nervix-dns v' <<< "${graph}" || \
            ! rg -q '^hickory-resolver v0\.26\.[0-9]+ .*tokio' <<< "${graph}"; then
            echo "${package} lacks the node resolver for its outbound connections" >&2
            exit 1
        fi
        if rg -q '^rustls v[^ ]+ (.*,)?ring(,|$)' <<< "${graph}"; then
            echo "${package} selected Rustls's Ring provider" >&2
            exit 1
        fi
    done
    graph="$(cargo tree --package nervix-connector-sqs --edges normal --format '{p} {f}' --prefix none)"
    if ! rg -q '^aws-smithy-http-client v[^ ]+ (.*,)?rustls-aws-lc(,|$)' <<< "${graph}" || \
        rg -q '^aws-smithy-http-client v[^ ]+ (.*,)?(rustls-ring|legacy-rustls-ring|s2n-tls)(,|$)' <<< "${graph}"; then
        echo "nervix-connector-sqs does not select AWS-LC alone for its Smithy HTTP client" >&2
        exit 1
    fi
    # MongoDB keeps its driver's Hickory SRV and TXT discovery; the driver still resolves the
    # addresses it connects to through Tokio.
    graph="$(cargo tree --package nervix-connector-mongodb --edges normal --format '{p} {f}' --prefix none)"
    if ! rg -q '^mongodb v3\.[0-9]+\.[0-9]+ .*dns-resolver' <<< "${graph}"; then
        echo "nervix-connector-mongodb lacks MongoDB's Hickory SRV and TXT discovery" >&2
        exit 1
    fi

# Check formatting and validate with the same concurrency default as local validation.
validate-ci jobs=default_jobs: (run-with-jobs "validate-ci-targets" jobs)

[private]
validate-ci-targets: fmt-check lint-targets validate-skill validate-nspl-docs validate-clock-boundaries validate-typed-errors validate-primitive-boundary validate-execution-mode-dependencies validate-execution-mode-conflicts validate-dns-dependencies ratchet validate-deloxide-applicability

# Hold every governed primitive to nervix-primitives and every mode feature to its owner. The check
# rejects a direct, renamed, grouped, qualified, glob, alias or macro path to another backend's
# primitives, shared ownership and the `futures` crates' synchronization included; a manifest that
# renames a governed crate; a mode selected by a bare `cfg` or a global `--cfg`; the analysis cfg
# selecting code or set by a build; a selected atomic held by a static or constructed in a const
# context; a family Loom does not model in Loom model code; an unmodeled primitive without its
# permission; a stale or misplaced permission; a `loom` dependency outside its owner and harness;
# and a mode feature that is not forwarded. Guest code compiled into user WASM guests is outside
# the source rules, and so is what a build wrote into a Cargo build directory. The check's own
# tests run first, so a rule that stopped rejecting its bypass fails here too.
validate-primitive-boundary:
    python3 -m unittest --quiet scripts.tests.test_check_primitive_boundary
    python3 -m scripts.check_primitive_boundary

# Measure the primitive source validator and its fixtures, including exact thread owner declarations.
coverage-primitive-boundary:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p "{{ cargo_target_dir }}/primitive-boundary"
    coverage=(uvx --from coverage==7.11.0 coverage)
    export COVERAGE_FILE="{{ cargo_target_dir }}/primitive-boundary/python.coverage"
    "${coverage[@]}" run --branch --source=scripts.check_primitive_boundary,scripts.tests.test_check_primitive_boundary -m unittest scripts.tests.test_check_primitive_boundary
    "${coverage[@]}" run --branch -a --source=scripts.check_primitive_boundary -m scripts.check_primitive_boundary
    "${coverage[@]}" lcov -o "{{ cargo_target_dir }}/primitive-boundary/python.lcov"
    "${coverage[@]}" json -o "{{ cargo_target_dir }}/primitive-boundary/python.json"
    "${coverage[@]}" report

# Keep every model checker, simulator and modeled wrapper out of every ordinary dependency graph, and
# the portable graphs portable. The workspace and every package built on its own, the way a consumer
# builds it, with default features and without them, contain no Loom, Shuttle, Shuttle wrapper or
# Turmoil, and enable no execution mode or paused-clock capability of the primitive boundary. The
# vocabulary, and the browser console and wire crate for the browser's target, contain no async
# runtime or network library and never the boundary's `native` capability. The check's own tests run
# first.
validate-execution-mode-dependencies:
    python3 -m unittest --quiet scripts.tests.test_check_mode_dependencies
    python3 -m scripts.check_mode_dependencies

# Keep one precise diagnostic when an execution mode is selected where it cannot run.
# nervix-primitives owns the rejection, so every pair of `loom`, `shuttle`, `turmoil` and
# `deloxide`, and all four, fail there with the modes named, including when separate dependencies
# enable them. A mode or the `native` capability requested for the browser's target fails with the
# boundary's own diagnostic as the first error, before any dependency that cannot build there, and
# so does the diagnostic mode requested without the `native` capability it tracks. A product
# binary built with a modeled mode fails where it declares itself one, naming the binary and the
# mode, and one built with the diagnostic mode fails there unless it declares a diagnostic form.
validate-execution-mode-conflicts:
    #!/usr/bin/env bash
    set -euo pipefail
    # The diagnostics are read as text, so Cargo must not color them even where the caller asks.
    export CARGO_TERM_COLOR=never
    diagnostics="$(mktemp)"
    trap 'rm -f "${diagnostics}"' EXIT
    expect_failure() {
        local description="$1"
        shift
        if "$@" >"${diagnostics}" 2>&1; then
            echo "${description}: the build unexpectedly compiled" >&2
            exit 1
        fi
    }
    expect_first_error() {
        local description="$1" message="$2"
        local first
        first="$(grep -m 1 '^error' "${diagnostics}" || true)"
        if [[ "${first}" != *"${message}"* ]]; then
            cat "${diagnostics}" >&2
            echo "${description}: the first error is not \`${message}\`" >&2
            exit 1
        fi
    }
    expect_conflict() {
        local package="$1" features="$2"
        shift 2
        expect_failure "${package} with ${features}" \
            cargo check --package "${package}" --features "${features}" --lib
        for pair in "$@"; do
            if ! grep -Fq "the ${pair} execution modes cannot be enabled together" "${diagnostics}"; then
                cat "${diagnostics}" >&2
                echo "${package} with ${features}: no diagnostic naming ${pair}" >&2
                exit 1
            fi
        done
    }
    expect_conflict nervix-primitives 'loom shuttle' '`loom` and `shuttle`'
    expect_conflict nervix-primitives 'loom turmoil' '`loom` and `turmoil`'
    expect_conflict nervix-primitives 'shuttle turmoil' '`shuttle` and `turmoil`'
    expect_conflict nervix-primitives 'loom deloxide native' '`loom` and `deloxide`'
    expect_conflict nervix-primitives 'shuttle deloxide native' '`shuttle` and `deloxide`'
    expect_conflict nervix-primitives 'turmoil deloxide native' '`turmoil` and `deloxide`'
    expect_conflict nervix-primitives 'loom shuttle turmoil deloxide native' \
        '`loom` and `shuttle`' '`loom` and `turmoil`' '`shuttle` and `turmoil`' \
        '`loom` and `deloxide`' '`shuttle` and `deloxide`' '`turmoil` and `deloxide`'
    # Two packages each select one mode; Cargo unifies both onto the owner.
    expect_conflict nervix-interconnect 'shuttle nervix-execution/turmoil' \
        '`shuttle` and `turmoil`'
    expect_conflict nervix-execution 'loom nervix-primitives/shuttle' '`loom` and `shuttle`'
    expect_conflict nervix-deadlock 'deloxide nervix-primitives/shuttle' '`shuttle` and `deloxide`'
    expect_failure "nervix-primitives with deloxide alone" \
        cargo check --package nervix-primitives --features deloxide --lib
    expect_first_error "nervix-primitives with deloxide alone" \
        'the `deloxide` diagnostic mode tracks the thread-blocking locks of the `native` capability'
    for mode in loom shuttle turmoil deloxide; do
        expect_failure "nervix-primitives with ${mode} for the browser" \
            cargo check --package nervix-primitives --features "${mode}" --lib \
                --target wasm32-unknown-unknown
        expect_first_error "nervix-primitives with ${mode} for the browser" \
            'execution modes run on native targets only'
    done
    expect_failure "nervix-primitives with native for the browser" \
        cargo check --package nervix-primitives --features native --lib \
            --target wasm32-unknown-unknown
    expect_first_error "nervix-primitives with native for the browser" \
        'the `native` capability provides operating-system threads'
    for mode in loom shuttle turmoil; do
        expect_failure "nervix-nspl-format with ${mode}" \
            cargo check --package nervix-nspl-format --bin nervix-nspl-format \
                --features "nervix-primitives/${mode}"
        expect_first_error "nervix-nspl-format with ${mode}" \
            "nervix-nspl-format is a product binary and builds only for ordinary execution; a build that selects the \`${mode}\` execution mode is a test artifact"
    done
    expect_failure "nervix-nspl-format with deloxide" \
        cargo check --package nervix-nspl-format --bin nervix-nspl-format \
            --features 'nervix-primitives/deloxide nervix-primitives/native'
    expect_first_error "nervix-nspl-format with deloxide" \
        "nervix-nspl-format is a product binary without a diagnostic form; a build that selects the \`deloxide\` diagnostic mode builds only a binary that declares one"
    echo "every mode conflict, browser-target request, diagnostic mode without its capability and product binary without its form fails with its diagnostic"

validate-clock-boundaries:
    python3 scripts/check_clock_boundaries.py

# Reject `Result<_, String>` in product code. A typed error is a rule, not a count, so there is no
# baseline to raise: any occurrence fails and names the rule.
validate-typed-errors:
    python3 -m scripts.check_typed_errors

toolchains-install:
    rustup toolchain install
    rustup toolchain install nightly-2026-09-17 --profile minimal --component rustc-dev --component rust-src --component rustfmt --component clippy --component llvm-tools

# Run every test target of the NSPL language and its formatter: unit, integration, completion-walk
# unit and documentation tests. The walk itself is a separate gate, `nspl-completion-walk`.
test-nspl *args:
    cargo test --package nervix-nspl --package nervix-nspl-format --all-targets -- {{ args }}

# Verify the backup and restore completion clauses and their public suggestion ordering.
test-nspl-backup-completion:
    cargo test --package nervix-nspl --lib backup::tests::completion_offers_each_

# Parse every runnable NSPL block in the documentation directly through the parser crate. Syntax
# synopses and statement fragments remain NSPL-labelled but opt out explicitly with `nspl,ignore`.
validate-nspl-docs:
    cargo test -p nervix-nspl --test documentation -- --nocapture

# Check the shared R2 configuration used by native CI jobs and Docker builds.
test-kache-ci:
    uv run --locked python -m unittest scripts.tests.test_kache_ci

test-docs:
    uv run --locked python -m unittest discover -s scripts/tests -p "test_*.py"
    node --test cloudflare/docs-worker/src/index.test.js

# Screenshots are recaptured from the current console and the nervix-cli reference chapter is
# rendered from the current binary, so a published book can never describe an older build.
book version="0.1.0-dev": test-docs docs-screenshots
    python scripts/build_book.py --version {{ version }}

validate-skill:
    env GH_PROMPT_DISABLED=1 gh skill publish .agents/skills --dry-run

book-pdf version="0.1.0-dev" output="":
    #!/usr/bin/env bash
    set -euo pipefail
    just book "{{ version }}"
    if ! command -v pandoc >/dev/null 2>&1; then
        echo "pandoc is required for book-pdf" >&2
        exit 1
    fi
    if [[ -n "{{ output }}" ]]; then
        output_path="{{ output }}"
    else
        output_path="docs/book/nervix.pdf"
    fi
    tmp_html="$(mktemp --suffix=.html)"
    tmp_title="$(mktemp --suffix=.tex)"
    trap 'rm -f "${tmp_html}" "${tmp_title}"' EXIT
    python3 scripts/render_pdf_title.py \
        --template docs/theme/nervix-pdf-title.tex \
        --version "{{ version }}" \
        --output "${tmp_title}"
    python3 scripts/prepare_pdf_html.py \
        --print-html docs/book/print.html \
        --summary docs/src/SUMMARY.md \
        --output "${tmp_html}"
    pandoc \
        --from=html \
        --to=pdf \
        --pdf-engine=xelatex \
        --top-level-division=chapter \
        --variable=documentclass:report \
        --include-in-header=docs/theme/nervix-pdf-header.tex \
        --include-before-body="${tmp_title}" \
        --variable=graphics \
        --variable=colorlinks \
        --variable=linkcolor:NervixLink \
        --variable=urlcolor:NervixLink \
        --variable=filecolor:NervixLink \
        --variable=citecolor:NervixLink \
        --variable=toccolor:black \
        --toc \
        --toc-depth=2 \
        --variable=geometry:margin=0.8in \
        --variable=linestretch:1.08 \
        --variable=mainfont:"Noto Serif" \
        --variable=sansfont:"Noto Sans" \
        --variable=monofont:"Noto Sans Mono" \
        --variable=CJKmainfont:"Noto Serif CJK JP" \
        --variable=CJKsansfont:"Noto Sans CJK JP" \
        --variable=CJKmonofont:"Noto Sans Mono CJK JP" \
        --resource-path=docs/book \
        --output="${output_path}" \
        "${tmp_html}"
    echo "${output_path}"

book-upload prefix="snapshot" bucket="nervix-docs":
    uv run --locked python scripts/upload_book_to_r2.py --bucket {{ bucket }} --prefix {{ prefix }}

publish-dir source target zone_id alias="snapshot" bucket="nervix-docs":
    uv run --locked python scripts/publish_docs_alias.py --source {{ source }} --target {{ target }} --alias {{ alias }} --bucket {{ bucket }} --zone-id {{ zone_id }}

purge-cache zone_id:
    python scripts/purge_cloudflare_cache.py --zone-id {{ zone_id }}

worker-deploy zone_id="":
    #!/usr/bin/env bash
    set -euo pipefail
    npx --yes wrangler deploy --config cloudflare/docs-worker/wrangler.jsonc
    if [[ -n "{{ zone_id }}" ]]; then
        python scripts/purge_cloudflare_cache.py --zone-id {{ zone_id }}
    fi

publish-book target zone_id alias="snapshot" bucket="nervix-docs":
    just book-pdf {{ target }} docs/book/nervix.pdf
    just publish-dir docs/book {{ target }} {{ zone_id }} {{ alias }} {{ bucket }}
    just worker-deploy {{ zone_id }}

deps:
    just generate-dev-tls
    docker compose up -d --build --wait --wait-timeout 90
    docker compose run --rm fake-gcs-init
    docker compose run --rm azurite-init

deps-down:
    docker compose down --remove-orphans --volumes

# Run black-box cluster scenarios against an explicitly supplied, already-built Nervix image.
# The script replaces the recipe's shell, so the TERM that just passes on when it is cancelled
# reaches the controller, whose exit trap heals, captures evidence and cleans up.
chaos *args:
    exec bash scripts/chaos/chaos.sh {{ args }}

# Exercise the actual rate-probe owner with delayed connects, partial bytes and unreachable peers.
test-chaos-rate-probe:
    exec bash scripts/chaos/tests/rate-probe-self-test.sh

# Requires kcov; retains shell Cobertura coverage for the probe and its regression controls.
coverage-chaos-rate-probe:
    #!/usr/bin/env bash
    set -euo pipefail
    # kcov's Bash descriptor exchange cannot handle an inherited limit near INT_MAX.
    descriptor_limit="$(ulimit -n)"
    if [[ "${descriptor_limit}" == unlimited ]] || ((descriptor_limit > 65536)); then
        ulimit -n 65536
    fi
    kcov --clean --include-path={{ quote(invocation_directory() / "scripts/chaos/link-degradation.sh") }},{{ quote(invocation_directory() / "scripts/chaos/tests/rate-probe-self-test.sh") }} {{ quote(cargo_target_dir / "chaos-rate-probe-coverage") }} scripts/chaos/tests/rate-probe-self-test.sh

# Regenerate the prebuilt WASM guest the stateful chaos scenarios upload, after a guest ABI change.
# The chaos runner itself never builds guest code; `cargo test --package nervix-wasm` checks that
# the checked-in module still equals what this writes.
chaos-wasm-fixture:
    cargo test --package nervix-wasm --test chaos_branch_counter -- --ignored write_the_branch_counter_module

server *args: build-deps generate-dev-tls
    NERVIX_NODE_ID="${NERVIX_NODE_ID:-node-1}" \
    NERVIX_INTERCONNECT_TLS_CA="${NERVIX_INTERCONNECT_TLS_CA:-tls/dev/ca.pem}" \
    NERVIX_INTERCONNECT_TLS_CERT="${NERVIX_INTERCONNECT_TLS_CERT:-tls/dev/node.pem}" \
    NERVIX_INTERCONNECT_TLS_KEY="${NERVIX_INTERCONNECT_TLS_KEY:-tls/dev/node-key.pem}" \
    cargo run --package nervix-server --bin nervix-server -- {{ args }}

client *args: build-deps
    cargo run --package nervix-cli -- {{ args }}

build-web-console:
    python3 scripts/build_web_console.py

build-server:
    CARGO_TARGET_DIR={{ cargo_target_dir }}/server cargo build {{ release_flag }} --package nervix-server --bin nervix-server

# Reuse an already available packaged image's runtime libraries for a local Chaos candidate.
# The two binaries are built from this checkout in the ordinary target directory, then stripped
# into a small, temporary Docker context so the source tree is never sent to the builder.
build-chaos-local-image tag="nervix:backup-local" base="nervix:chaos-current":
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build --package nervix-server --bin nervix-server --package nervix-cli --bin nervix-cli
    stage="$(mktemp -d {{ quote(cargo_target_dir + "/backup-chaos-image.XXXXXX") }})"
    trap 'rm -rf "${stage}"' EXIT
    install -m 755 {{ quote(cargo_target_dir + "/debug/nervix-server") }} "${stage}/nervix-server"
    install -m 755 {{ quote(cargo_target_dir + "/debug/nervix-cli") }} "${stage}/nervix-cli"
    llvm-strip-23 "${stage}/nervix-server" "${stage}/nervix-cli"
    cat >"${stage}/Dockerfile" <<'EOF'
    ARG BASE
    FROM ${BASE}
    COPY nervix-server /usr/local/bin/nervix-server
    COPY nervix-cli /usr/local/bin/nervix-cli
    EOF
    docker build --build-arg BASE={{ quote(base) }} --tag {{ quote(tag) }} "${stage}"

# Build a diagnostic node: nervix-server in the `deloxide` mode, whose tracked locks report an active
# deadlock with evidence and end the process. Its own target directory keeps it from replacing the
# ordinary binary; it is a diagnostic artifact, never a release product.
build-diagnostic-server selection="deloxide":
    CARGO_TARGET_DIR={{ cargo_target_dir }}/deloxide cargo build {{ release_flag }} --package nervix-server --bin nervix-server --features {{ quote(selection) }}

build-cli:
    CARGO_TARGET_DIR={{ cargo_target_dir }}/cli cargo build {{ release_flag }} --package nervix-cli --bin nervix-cli

[parallel]
build-apps: build-cli build-server

build-all: generate-dev-tls build-deps build-apps

wasm-processor-rust-guest:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build \
        --manifest-path examples/wasm-processors/rust-guest/Cargo.toml \
        --target wasm32-unknown-unknown \
        --release
    test -s examples/wasm-processors/rust-guest/target/wasm32-unknown-unknown/release/nervix_wasm_processor_rust_guest.wasm

# DB-IP publishes one City Lite build per month, so the current month is tried first and the
# previous one is the fallback for the days before a new build appears.
download-datalake-dbip:
    #!/usr/bin/env bash
    set -euo pipefail
    destination="examples/datalake/geo-wasm-guest/dbip-city-lite.mmdb.gz"
    if [[ -s "${destination}" ]]; then
        exit 0
    fi
    year="$(date -u +%Y)"
    month="$(date -u +%m)"
    months=()
    for _ in 1 2; do
        months+=("${year}-${month}")
        if [[ "${month}" == "01" ]]; then
            year="$((10#${year} - 1))"
            month="12"
        else
            month="$(printf '%02d' "$((10#${month} - 1))")"
        fi
    done
    mkdir -p "$(dirname "${destination}")"
    tmp="$(mktemp "${destination}.tmp.XXXXXX")"
    trap 'rm -f "${tmp}"' EXIT
    for release in "${months[@]}"; do
        url="https://download.db-ip.com/free/dbip-city-lite-${release}.mmdb.gz"
        if curl -L --proto '=https' --tlsv1.2 -sSf "${url}" -o "${tmp}"; then
            echo "downloaded DB-IP City Lite ${release}"
            mv "${tmp}" "${destination}"
            exit 0
        fi
    done
    echo "no DB-IP City Lite build available for ${months[*]}" >&2
    exit 1

wasm-datalake-geo-guest: download-datalake-dbip
    #!/usr/bin/env bash
    set -euo pipefail
    artifact="examples/datalake/geo-wasm-guest/target/wasm32-unknown-unknown/release/nervix_datalake_geo_wasm_guest.wasm"
    resource_dir="examples/datalake/geo-wasm-guest/resource"
    cargo build \
        --manifest-path examples/datalake/geo-wasm-guest/Cargo.toml \
        --target wasm32-unknown-unknown \
        --release
    test -s "${artifact}"
    rm -rf "${resource_dir}"
    mkdir -p "${resource_dir}"
    cp "${artifact}" "${resource_dir}/"
    test "$(find "${resource_dir}" -type f | wc -l)" -eq 1

provision-datalake-iceberg:
    docker compose run --rm -e ICEBERG_INIT_HOLD=false datalake-iceberg-init

duckdb-datalake:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! command -v duckdb >/dev/null 2>&1; then
        echo "duckdb is required to query the datalake Iceberg tables" >&2
        exit 127
    fi
    duckdb :memory: -init examples/datalake/duckdb_iceberg.sql

wasm-processor-go-guest:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! command -v tinygo >/dev/null 2>&1; then
        echo "tinygo is required to build the native non-WASI Go WASM guest" >&2
        echo "standard Go only supports js/wasm and wasip1/wasm outputs" >&2
        exit 127
    fi
    cd examples/wasm-processors/go-guest
    # TinyGo compiles against the standard library of the Go toolchain that `go` selects, and
    # automatic selection never moves below a newer host Go, so the go.mod toolchain is forced.
    toolchain="$(sed -n 's/^toolchain //p' go.mod)"
    test -n "${toolchain}"
    GOTOOLCHAIN="${toolchain}" tinygo build \
        -target=wasm-unknown \
        -scheduler=none \
        -opt=z \
        -panic=trap \
        -no-debug \
        -o nervix_wasm_processor_go_guest.wasm \
        .
    test -s nervix_wasm_processor_go_guest.wasm

[parallel]
wasm-processor-guests: wasm-processor-rust-guest wasm-processor-go-guest

generate-dev-tls:
    #!/usr/bin/env bash
    set -euo pipefail
    bash scripts/generate_dev_tls.sh

generate-test-onnx output="tests/fixtures/onnx/simple_score.onnx" alternate_output="tests/fixtures/onnx/alternate_score.onnx" batch_output="tests/fixtures/onnx/batch_score.onnx" f64_output="tests/fixtures/onnx/f64_score.onnx" matrix_output="tests/fixtures/onnx/matrix_identity.onnx" dynamic_batch_output="tests/fixtures/onnx/dynamic_batch_score.onnx" scalar_output="tests/fixtures/onnx/scalar_identity.onnx":
    python3 scripts/train_simple_onnx.py --output {{ output }} --alternate-output {{ alternate_output }} --batch-output {{ batch_output }} --f64-output {{ f64_output }} --matrix-output {{ matrix_output }} --dynamic-batch-output {{ dynamic_batch_output }} --scalar-output {{ scalar_output }}

# Download a CPU or CUDA 13 runtime; `host` selects the current machine's platform.
download-onnxruntime flavor="cpu" platform="host":
    bash scripts/download_onnxruntime.sh --flavor {{ quote(flavor) }} --platform {{ quote(platform) }}

# Check runtime package selection and the CPU/CUDA image build targets.
test-onnxruntime:
    uv run --locked python -m unittest scripts.tests.test_onnxruntime

# Build both final targets with fixture binaries and check layer sharing and runtime dependencies.
test-onnxruntime-docker:
    #!/usr/bin/env bash
    set -euo pipefail
    just docker-prepare-qemu linux/amd64
    mkdir -p target
    fixture_context="$(mktemp -d "${PWD}/target/onnxruntime-docker.XXXXXX")"
    trap 'rm -rf "${fixture_context}"' EXIT
    mkdir -p "${fixture_context}/artifacts/nervix-server.bundle"
    for binary in nervix-server nervix-cli nervix-nspl-format; do
        cat > "${fixture_context}/artifacts/${binary}" <<'SH'
    #!/bin/sh
    basename "$0"
    SH
        chmod +x "${fixture_context}/artifacts/${binary}"
    done
    printf 'Sonic bundle fixture\n' > "${fixture_context}/artifacts/nervix-server.bundle/manifest"
    for variant in cpu cuda; do
        base=debian-base
        if [[ "${variant}" == cuda ]]; then
            base=cuda-base
        fi
        docker buildx build -f Dockerfile.debian --target "${base}" --platform linux/amd64 \
            --load --tag "nervix:onnxruntime-${variant}-base-check" .
        image="nervix:onnxruntime-${variant}-check"
        docker buildx build -f Dockerfile.debian --target "${variant}" --platform linux/amd64 \
            --build-context "builder=${fixture_context}" --load --tag "${image}" .
        test "$(docker run --rm "${image}")" = nervix-server
        docker run --rm --entrypoint /bin/sh "${image}" -c '
            set -eu
            if test -f /usr/local/lib/libonnxruntime_providers_cuda.so; then
                ldconfig -p | grep -F libcudnn.so.9
            fi
            for library in /usr/local/lib/libonnxruntime*.so /usr/lib/x86_64-linux-gnu/libcudnn*.so.9; do
                test -f "${library}" || continue
                dependencies="$(ldd "${library}")"
                printf "%s\n%s\n" "${library}" "${dependencies}"
                # The NVIDIA container runtime injects the host driver when --gpus is used.
                missing="$(printf "%s\n" "${dependencies}" | awk '\''/not found/ && $1 != "libcuda.so.1"'\'')"
                test -z "${missing}"
            done
        '
    done
    uv run --locked python - <<'PY'
    import json
    import subprocess

    def layers(image):
        result = subprocess.check_output(["docker", "image", "inspect", image], text=True)
        return json.loads(result)[0]["RootFS"]["Layers"]

    def filesystem_history(image):
        result = subprocess.check_output([
            "docker", "image", "history", "--no-trunc", "--human=false", "--format", "json", image,
        ], text=True)
        rows = [json.loads(line) for line in result.splitlines()]
        return [row["CreatedBy"] for row in reversed(rows) if int(row["Size"]) > 0]

    debian = layers("nervix:onnxruntime-cpu-base-check")
    cuda = layers("nervix:onnxruntime-cuda-base-check")
    if cuda[:len(debian)] != debian or len(cuda) != len(debian) + 1:
        raise SystemExit("CUDA must add exactly one filesystem layer directly above Debian")
    for variant, base in (("cpu", debian), ("cuda", cuda)):
        image = f"nervix:onnxruntime-{variant}-check"
        runtime = layers(image)
        if runtime[:len(base)] != base or len(runtime) <= len(base):
            raise SystemExit(f"{variant}: Nervix runtime layers must follow the selected base")
        first_runtime_layer = " ".join(filesystem_history(image)[len(base)].split())
        command = first_runtime_layer.partition(" /bin/sh -c ")[2]
        if not command.startswith("apt-get update && apt-get upgrade -y"):
            raise SystemExit(f"{variant}: Debian updates must immediately follow the selected base")

    if layers("nervix:onnxruntime-cpu-check")[-4:] != layers("nervix:onnxruntime-cuda-check")[-4:]:
        raise SystemExit("CPU and CUDA targets must share the Nervix binary and bundle layers")

    cuda_base = "nervix:onnxruntime-cuda-base-check"
    held = subprocess.check_output([
        "docker", "run", "--rm", "--entrypoint", "apt-mark", cuda_base, "showhold",
    ], text=True).splitlines()
    if not held:
        raise SystemExit("CUDA packages must be held at their pinned versions")

    def cuda_versions(image):
        return subprocess.check_output([
            "docker", "run", "--rm", "--entrypoint", "dpkg-query", image,
            "-W", "-f=${Package}=${Version}\\n", *held,
        ], text=True)
    if cuda_versions(cuda_base) != cuda_versions("nervix:onnxruntime-cuda-check"):
        raise SystemExit("Debian upgrades must preserve the pinned CUDA package versions")
    print("Verified CPU/CUDA final targets, shared Nervix layers, Debian updates, and CUDA pins")
    PY

reset-local-dashboard-state:
    #!/usr/bin/env bash
    set -euo pipefail
    rm -rf .nervix-db

cluster-dashboard: build-all
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p .nervix-db/node1 .nervix-db/node2 .nervix-db/node3
    dashboard_password="${NERVIX_PASSWORD:-${NERVIX_INIT_DEFAULT_USER_PASSWORD:-nervix}}"
    export NERVIX_PASSWORD="${dashboard_password}"
    export NERVIX_INIT_DEFAULT_USER_PASSWORD="${NERVIX_INIT_DEFAULT_USER_PASSWORD:-${dashboard_password}}"
    target_dir="${CARGO_TARGET_DIR:-target}"
    case "${target_dir}" in
        /*) ;;
        *) target_dir="${PWD}/${target_dir}" ;;
    esac
    cli_bin_dir="${target_dir}/cli/{{ build_mode }}"
    server_bin_dir="${target_dir}/server/{{ build_mode }}"
    export PATH="${cli_bin_dir}:${server_bin_dir}:${PATH}"
    exec zellij --layout .zellij/layouts/local-3-nodes.kdl

dockerfmt:
    #!/usr/bin/env bash
    set -euo pipefail
    for file in Dockerfile*; do
        tmp="$(mktemp)"
        dockerfmt < "${file}" > "${tmp}"
        mv "${tmp}" "${file}"
    done

dockerfmt-check:
    #!/usr/bin/env bash
    set -euo pipefail
    failed=0
    for file in Dockerfile*; do
        tmp="$(mktemp)"
        dockerfmt < "${file}" > "${tmp}"
        if ! cmp -s "${file}" "${tmp}"; then
            echo "dockerfmt check failed for ${file}"
            diff -u "${file}" "${tmp}" || true
            failed=1
        fi
        rm -f "${tmp}"
    done
    exit "${failed}"

docker-prepare-qemu platform="linux/amd64":
    #!/usr/bin/env bash
    set -euo pipefail
    normalized_platform="{{ platform }}"
    if [[ "${normalized_platform}" == "linux/aarch64" ]]; then
        normalized_platform="linux/arm64"
    fi
    host_arch="$(uname -m)"
    case "${host_arch}" in
        x86_64) host_platform="linux/amd64" ;;
        aarch64|arm64) host_platform="linux/arm64" ;;
        *) host_platform="" ;;
    esac
    if [[ -n "${host_platform}" && "${normalized_platform}" != "${host_platform}" ]]; then
        docker run --privileged --rm tonistiigi/binfmt --install all
    fi

# Build the CPU image for amd64 or arm64.
docker-build-debian llvm_version="23" tag="nervix:debian" platform="linux/amd64" push="false" cache_from="" cache_to="":
    just docker-build-linux cpu {{ quote(llvm_version) }} \
        {{ quote(tag) }} {{ quote(platform) }} {{ quote(push) }} {{ quote(cache_from) }} {{ quote(cache_to) }}

# Build Debian trixie -> CUDA 13/cuDNN 9 -> Nervix, reusing CUDA across application builds.
docker-build-cuda llvm_version="23" tag="nervix:cuda" platform="linux/amd64" push="false" cache_from="" cache_to="":
    just docker-build-linux cuda {{ quote(llvm_version) }} \
        {{ quote(tag) }} {{ quote(platform) }} {{ quote(push) }} {{ quote(cache_from) }} {{ quote(cache_to) }}

[private]
docker-build-linux image_target llvm_version tag platform push cache_from cache_to:
    #!/usr/bin/env bash
    set -euo pipefail
    normalized_platform={{ quote(platform) }}
    if [[ "${normalized_platform}" == "linux/aarch64" ]]; then
        normalized_platform="linux/arm64"
    fi
    if [[ {{ quote(image_target) }} == "cuda" && "${normalized_platform}" != "linux/amd64" ]]; then
        echo "CUDA images require linux/amd64; ONNX Runtime publishes no GPU archive for ${normalized_platform}" >&2
        exit 1
    fi
    just docker-prepare-qemu "${normalized_platform}"
    output_flag="--load"
    if [[ {{ quote(push) }} == "true" ]]; then
        output_flag="--push"
    fi
    cache_flags=()
    if [[ -n {{ quote(cache_from) }} ]]; then
        cache_flags+=(--cache-from={{ quote(cache_from) }})
    fi
    if [[ -n {{ quote(cache_to) }} ]]; then
        cache_flags+=(--cache-to={{ quote(cache_to) }})
    fi
    : "${KACHE_S3_BUCKET:?KACHE_S3_BUCKET is required}"
    : "${KACHE_S3_REGION:?KACHE_S3_REGION is required}"
    : "${KACHE_S3_ENDPOINT:?KACHE_S3_ENDPOINT is required}"
    : "${KACHE_S3_ACCESS_KEY:?KACHE_S3_ACCESS_KEY is required}"
    : "${KACHE_S3_SECRET_KEY:?KACHE_S3_SECRET_KEY is required}"
    docker buildx build \
        -f Dockerfile.debian \
        --target {{ quote(image_target) }} \
        --progress=plain \
        --platform "${normalized_platform}" \
        --build-arg "KACHE_VERSION=${KACHE_VERSION:-0.28.1}" \
        --build-arg RUST_VERSION={{ rust_toolchain_version }} \
        --build-arg LLVM_VERSION={{ quote(llvm_version) }} \
        --build-arg "KACHE_S3_BUCKET=${KACHE_S3_BUCKET}" \
        --build-arg "KACHE_S3_REGION=${KACHE_S3_REGION}" \
        --build-arg "KACHE_S3_ENDPOINT=${KACHE_S3_ENDPOINT}" \
        --build-arg "KACHE_S3_ACCESS_KEY=${KACHE_S3_ACCESS_KEY}" \
        --build-arg "KACHE_S3_SECRET_KEY=${KACHE_S3_SECRET_KEY}" \
        "${cache_flags[@]}" \
        -t {{ quote(tag) }} \
        "${output_flag}" \
        .

docker-build-alpine alpine_version="3.23" llvm_version="21" tag="nervix:alpine" platform="linux/amd64" push="false" cache_from="" cache_to="":
    #!/usr/bin/env bash
    set -euo pipefail
    normalized_platform="{{ platform }}"
    if [[ "${normalized_platform}" == "linux/aarch64" ]]; then
        normalized_platform="linux/arm64"
    fi
    just docker-prepare-qemu "${normalized_platform}"
    output_flag="--load"
    if [[ "{{ push }}" == "true" ]]; then
        output_flag="--push"
    fi
    cache_from_flag=""
    if [[ -n "{{ cache_from }}" ]]; then
        cache_from_flag="--cache-from={{ cache_from }}"
    fi
    cache_to_flag=""
    if [[ -n "{{ cache_to }}" ]]; then
        cache_to_flag="--cache-to={{ cache_to }}"
    fi
    docker buildx build \
        -f Dockerfile.alpine \
        --progress=plain \
        --platform "${normalized_platform}" \
        --build-arg RUST_VERSION={{ rust_toolchain_version }} \
        --build-arg ALPINE_VERSION={{ alpine_version }} \
        --build-arg LLVM_VERSION={{ llvm_version }} \
        ${cache_from_flag} \
        ${cache_to_flag} \
        -t {{ tag }} \
        "${output_flag}" \
        .

kube-deps:
    bash scripts/kube_deps.sh

kube-deps-down:
    bash scripts/kube_deps_down.sh

kube-app:
    bash scripts/kube_app.sh

kube-app-down:
    bash scripts/kube_app_down.sh

kube-cli:
    bash scripts/kube_cli.sh

kube-cli-command command:
    bash scripts/kube_cli.sh --command "{{ command }}"

# Record timing and allocations for every repaired recurring task dependency.
bench-task-handles output="target/task-handles.json": build-web-console
    cargo bench --package nervix-server --bench task_handles --features benchmarks -- {{ quote(output) }}

coverage-task-handles output="target/task-handles.lcov" report="target/task-handles-coverage.json":
    cargo llvm-cov --bench task_handles --features benchmarks --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} -- {{ quote(report) }}

# Inspect a benchmark harness from an existing successful instrumented run without rebuilding it.
coverage-task-handles-export executable profile output="target/task-handles-benchmark.lcov":
    #!/usr/bin/env bash
    set -euo pipefail
    llvm_bin="$(rustc --print sysroot)/lib/rustlib/$(rustc -vV | sed -n 's/^host: //p')/bin"
    "${llvm_bin}/llvm-profdata" merge -sparse {{ quote(profile) }} -o target/task-handles-benchmark.profdata
    "${llvm_bin}/llvm-cov" export {{ quote(executable) }} --instr-profile=target/task-handles-benchmark.profdata --format=lcov > {{ quote(output) }}

# Same-host retained relay selection, branch churn and authenticated pool leasing measurements.
bench-retained-channels: build-web-console wasm-processor-guests download-onnxruntime bench-retained-channels-bodies

# Native bodies also exercised by the coverage-producing benchmark smoke check.
bench-retained-channels-bodies:
    ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)" cargo test --package nervix-server --features testing --lib relay_channel_cost -- --ignored --nocapture
    cargo test --package nervix-interconnect --lib established_pool_cost -- --ignored --nocapture

# Delivery correlation and authenticated frame costs, including retained state and reclamation.
bench-remote-owners: build-web-console wasm-processor-guests download-onnxruntime bench-remote-owners-bodies

[private]
bench-remote-owners-bodies:
    ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)" cargo test --package nervix-server --features testing --lib remote_ack_owner_cost -- --ignored --nocapture
    cargo test --package nervix-interconnect --lib remote_relay_frame_cost -- --ignored --nocapture

# Native diagnostic owners and probes, composed separately from the ordinary report command.
test-deadlock-evidence-order:
    cargo test --package nervix-deadlock --features deloxide-order --lib
    cargo test --package nervix-primitives --features 'native deloxide-order' --lib
    cargo test --package nervix-deadlock --features deloxide-order --test active_cycles

test-deadlock-report:
    cargo test --package nervix-deadlock --features report-tool --test report_cli

# Collect both focused checks in their own mode's build and retain canonical completion evidence.
# The combined report is labeled diagnostic-evidence, outside ordinary coverage and the CRAP gate.
coverage-deadlock output="target/deadlock.lcov":
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p {{ quote(cargo_target_dir + "/diagnostic-evidence") }}
    reports="$(mktemp -d {{ quote(cargo_target_dir + "/diagnostic-evidence/coverage.XXXXXX") }})"
    python3 -m scripts.native_coverage --target-dir {{ quote(cargo_target_dir) }} run test-deadlock-evidence-order --output "${reports}/order.lcov"
    python3 -m scripts.native_coverage --target-dir {{ quote(cargo_target_dir) }} run test-deadlock-report --output "${reports}/report.lcov"
    cat "${reports}/order.lcov" "${reports}/report.lcov" > "${reports}/combined.lcov"
    mkdir -p "$(dirname {{ quote(output) }})"
    mv "${reports}/combined.lcov" {{ quote(output) }}
