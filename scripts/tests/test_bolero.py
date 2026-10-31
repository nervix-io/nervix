"""Focused tests for the Bolero inventory and command runner."""

from __future__ import annotations

import dataclasses
import contextlib
import hashlib
import io
import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from scripts import bolero, bolero_rustc, build_web_console


class SourceLocationBuildTests(unittest.TestCase):
    def test_location_parser_keeps_sanitizer_and_engine_instrumentation(self) -> None:
        for gate in (["--cfg", "fuzzing"], ["--cfg=fuzzing"]):
            arguments = ["--crate-name", "proc_macro2", "--cfg", 'feature="span-locations"',
                         *gate, "--cfg", "fuzzing_libfuzzer", "-Zsanitizer=address",
                         "-Cinstrument-coverage", "--cfg", 'feature="proc-macro"']
            actual = bolero_rustc.compiler_arguments(arguments)
            self.assertEqual(actual, ["--crate-name", "proc_macro2", "--cfg",
                                     'feature="span-locations"', "--cfg", "fuzzing_libfuzzer",
                                     "-Zsanitizer=address", "-Cinstrument-coverage", "--cfg",
                                     'feature="proc-macro"'])

    def test_other_dependency_and_harness_configurations_are_unchanged(self) -> None:
        for arguments in (["--crate-name", "nervix_lint_report", "--cfg", "fuzzing"],
                          ["--crate-name", "proc_macro2", "--cfg", "fuzzing"],
                          ["-vV"],
                          ["--crate-name", "proc_macro2", '--cfg=feature="span-locations"', "--cfg"]):
            self.assertEqual(bolero_rustc.compiler_arguments(arguments), arguments)

    def test_compiler_adapter_executes_the_selected_compiler(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            compiler = pathlib.Path(directory) / "selected-rustc"
            compiler.write_text(f"#!{sys.executable}\nimport json,sys\nprint(json.dumps(sys.argv[1:]))\n")
            compiler.chmod(0o700)
            result = subprocess.run([sys.executable, str(bolero.ROOT / "scripts/bolero_rustc.py"),
                "--crate-name", "proc_macro2", '--cfg=feature="span-locations"',
                "--cfg=fuzzing", "--cfg=fuzzing_libfuzzer", "-Zsanitizer=address"],
                env={**os.environ, "NERVIX_BOLERO_RUSTC": str(compiler)},
                capture_output=True, text=True, check=True)
            self.assertEqual(json.loads(result.stdout), ["--crate-name", "proc_macro2",
                '--cfg=feature="span-locations"', "--cfg=fuzzing_libfuzzer", "-Zsanitizer=address"])


class ConsoleAssetTests(unittest.TestCase):
    def test_identical_assets_preserve_their_file_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source, destination = root / "staging", root / "dist"
            source.mkdir()
            (source / "index.html").write_bytes(b"current console")
            build_web_console.publish_assets(source, destination)
            current = (destination / "index.html").stat()
            source.mkdir(exist_ok=True)
            (source / "index.html").write_bytes(b"current console")
            build_web_console.publish_assets(source, destination)
            published = (destination / "index.html").stat()
            self.assertEqual(published.st_mtime_ns, current.st_mtime_ns)
            self.assertEqual(published.st_ino, current.st_ino)

    def test_published_assets_equal_the_complete_build_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source, destination = root / "staging", root / "dist"
            source.mkdir()
            destination.mkdir()
            (destination / "index.html").write_bytes(b"partial console")
            (destination / "unreferenced.js").write_bytes(b"unused")
            (source / "index.html").write_bytes(b"current console")
            (source / "nested").mkdir()
            (source / "nested/console.css").write_bytes(b"current style")
            expected = {path.relative_to(source): path.read_bytes()
                        for path in source.rglob("*") if path.is_file()}
            build_web_console.publish_assets(source, destination)
            actual = {path.relative_to(destination): path.read_bytes()
                      for path in destination.rglob("*") if path.is_file()}
            self.assertEqual(actual, expected)

    def test_build_publishes_success_and_preserves_assets_on_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            console = root / "crates/web-console"
            console.mkdir(parents=True)
            tools = root / "target/tools/wasm-bindgen-0.2.129/bin"

            def build(args: list[str], **kwargs: object) -> None:
                # Offline, trunk takes the wasm-bindgen first on PATH and never downloads one.
                self.assertEqual(args[:5], ["trunk", "build", "--release", "--offline", "--dist"])
                self.assertEqual(kwargs["cwd"], console)
                self.assertNotIn("NO_COLOR", kwargs["env"])
                self.assertEqual(kwargs["env"]["PATH"].split(os.pathsep)[0], str(tools))
                self.assertTrue(kwargs["check"])
                staging = pathlib.Path(args[5])
                staging.mkdir()
                (staging / "index.html").write_bytes(b"current console")

            with mock.patch.object(build_web_console, "ROOT", root), \
                    mock.patch.object(build_web_console.wasm_bindgen, "install", return_value=tools) as install, \
                    mock.patch.object(build_web_console.subprocess, "run", side_effect=build):
                build_web_console.main()
            install.assert_called_once_with(root, build_web_console.wasm_bindgen.default_target())
            with mock.patch.object(build_web_console, "ROOT", root), \
                    mock.patch.object(build_web_console.wasm_bindgen, "install", return_value=tools), \
                    mock.patch.object(build_web_console.subprocess, "run",
                                      side_effect=subprocess.CalledProcessError(1, "trunk")):
                with self.assertRaises(subprocess.CalledProcessError):
                    build_web_console.main()
            self.assertEqual((console / "dist/index.html").read_bytes(), b"current console")

    def test_a_failed_tool_install_stops_the_build_before_trunk_runs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            (root / "crates/web-console/dist").mkdir(parents=True)
            (root / "crates/web-console/dist/index.html").write_bytes(b"current console")
            refusal = build_web_console.wasm_bindgen.ToolError("installing wasm-bindgen-cli 0.2.129 failed with exit 101")
            with mock.patch.object(build_web_console, "ROOT", root), \
                    mock.patch.object(build_web_console.wasm_bindgen, "install", side_effect=refusal), \
                    mock.patch.object(build_web_console.subprocess, "run") as run:
                with self.assertRaisesRegex(build_web_console.wasm_bindgen.ToolError, "failed with exit 101"):
                    build_web_console.main()
            run.assert_not_called()
            self.assertEqual((root / "crates/web-console/dist/index.html").read_bytes(), b"current console")


class InventoryTests(unittest.TestCase):
    def assert_invalid_inventory(self, text: str, message: str) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "inventory.toml"
            path.write_text(text)
            with self.assertRaisesRegex(bolero.BoleroError, message):
                bolero.load_inventory(path)

    def test_inventory_has_current_targets_and_exact_corpus_paths(self) -> None:
        inventory = bolero.load_inventory()
        self.assertEqual({target.id for target in inventory.targets}, {
            "clickhouse-client-tls-pairing",
            "client-config-schema",
            "connector-entity-presentation",
            "console-ingestor-source-drafts",
            "materialized-publication-lifetimes",
            "task-status-transitions",
            "entity-freeze-transitions",
            "endpoint-route-table",
            "endpoint-acknowledgement-body",
            "relay-channel-lifetimes",
            "interconnect-slot-lifetimes",
            "runtime-checkpoint-description-wire",
            "runtime-state-replication-wire",
            "runtime-state-replication-malformed",
            "runtime-checkpoint-listing-owner",
            "runtime-checkpoint-listing-malformed",
            "remote-correlation-lifetimes",
            "relay-admission-owners",
            "relay-channel-watermark-turnover",
            "client-emitter-wire",
            "client-requests",
            "client-replies",
            "client-events",
            "client-streams",
            "client-upload-archives",
            "client-reply-transfer",
            "client-row-views",
            "client-arbitrary-frames",
            "client-frame-corruption",
            "client-discriminators",
            "client-arrow-rows",
            "client-arrow-selection",
            "client-binding-rows",
            "plexus-expression",
            "plexus-model",
            "plexus-archive-model",
            "backup-record-manifest",
            "backup-runtime-state-records",
            "backup-streamed-state-records",
            "backup-complete-archives",
            "backup-malformed-records",
            "backup-corrupt-archives",
            "backup-materialized-identities",
            "backup-materialized-columns",
            "backup-branch-state-descriptors",
            "backup-deduplicator-keys",
            "backup-window-groups",
            "client-processor-choice-request",
            "client-ffi-host-columns",
            "client-ffi-field-descriptors",
            "client-ffi-field-descriptors-malformed",
            "branch-membership",
            "restore-installation-wire",
            "backup-capture-wire",
            "backup-capture-wire-malformed",
            "backup-section-mapping",
            "stopping-node-drain-wire",
            "cluster-peer-clock-probes",
            "restore-installation-storage",
            "restore-staging-kept-measurement",
            "typed-report", "typed-source-contract", "typed-site-union", "typed-source-syntax", "typed-hidden-tunables",
            "plexus-statement",
            "plexus-statement-text",
            "plexus-emitter-envelopes",
            "plexus-expression-text",
            "plexus-format-document",
            "plexus-format-text",
            "models-names",
            "models-name-validation",
            "models-external-names",
            "models-external-name-validation",
            "models-connector-declarations",
            "websocket-retained-metadata",
            "ingest-metadata-projection",
            "websocket-client-config",
            "websocket-upgrade-headers",
            "models-timestamps",
            "models-timestamp-text",
            "models-domain-clock",
            "models-domain-clock-validation",
            "models-durations",
            "models-duration-text",
            "models-header-model-round-trip",
            "models-iceberg-declarations",
            "models-emitter-envelopes",
            "models-json-paths",
            "models-json-path-validation",
            "models-batch-limits",
            "models-batch-limit-validation",
            "models-identities",
            "models-identity-validation",
            "models-prerequisite-records",
            "models-admission-verdicts",
            "models-archived-models",
            "scheduled-units",
            "execution-revision-units",
            "models-archived-counts",
            "consensus-archived-counts",
            "registry-archived-models",
            "runtime-window-archived-counts",
            "simd-constant-division",
            "simd-checked-lanes",
            "simd-byte-classes",
            "simd-xml-chars",
            "syslog-stream-framing",
            "rabbitmq-client-address",
            "syslog-structured-data",
            "replica-progress",
            "replica-catch-up",
            "state-replication-routing",
            "paced-simulation-records",
            "deadlock-evidence",
            "deadlock-order-normalization",
            "deadlock-order-bounds",
            "deadlock-evidence-malformed",
            "deadlock-evidence-bounds",
            "deadlock-evidence-summary",
            "wasm-protocol-malformed",
            "wasm-branch-metadata",
            "wasm-envelopes",
            "wasm-snapshots",
            "wasm-abi-codes",
            "wasm-host-schema",
            "wasm-host-envelopes",
            "wasm-sdk-snapshots",
            "wasm-sdk-rejections",
            "wasm-sdk-input",
            "wasm-sdk-output",
            "wasm-state-descriptors",
            "wasm-state-descriptors-malformed",
            "wasm-protocol-corruption",
            "wasm-protocol-tags",
            "wasm-stored-checkpoints",
            "wasm-stored-checkpoints-malformed",
            "restore-native-lifecycle",
            "restore-native-kafka",
            "backup-native-lifecycle",
            "backup-native-kafka",
            "runtime-kafka-offset-captures",
            "runtime-kafka-offset-transfer-headers",
            "runtime-kafka-offset-transfer-headers-malformed",
            "consensus-state-records",
            "consensus-state-corruption",
            "consensus-raft-records",
            "consensus-raft-records-malformed",
            "registry-stored-models",
            "registry-stored-models-corruption",
            "runtime-state-keys",
            "runtime-state-keys-malformed",
            "runtime-state-chunk-keys-malformed",
            "runtime-state-staging-namespaces",
            "runtime-state-staging-namespaces-malformed",
            "runtime-state-identity-records",
            "runtime-state-identity-records-malformed",
            "runtime-state-snapshots",
            "runtime-state-snapshots-malformed",
            "runtime-state-store-checkpoints",
            "runtime-window-snapshots",
            "runtime-window-snapshots-malformed",
            "runtime-window-sealed-checkpoints",
            "runtime-arrow-bodies",
            "runtime-arrow-bodies-malformed",
            "resource-store-layout",
            "client-producer-batches",
            "client-producer-batches-malformed",
            "codec-schemaful-rows",
            "codec-schemaful-payload-groups",
            "codec-json-non-finite-floats",
            "codec-transforming-numbers",
            "jaq-native-numbers",
            "jaq-yaml-string-values",
            "node-settings-command-line",
            "remote-relay-rows",
            "remote-relay-rows-malformed",
            "relay-wire-messages",
            "bulk-request-envelope-wire",
            "receive-admission-outcomes-wire",
            "relay-wire-messages-malformed",
            "client-producer-link-frames",
            "client-producer-link-frames-malformed",
            "client-consumer-stream-frames",
            "client-consumer-stream-frames-malformed",
            "client-emitter-deliveries",
            "wasm-generated-pools",
            "wasm-generated-pools-malformed",
            "expiry-map-operations",
            "vm-decimal-rounding-floats",
            "vm-decimal-rounding-integers",
            "approx-into-rounding",
            "approx-into-exact",
            "simd-json-escapes",
            "simd-paced-admission",
            "simd-elapsed-histograms",
            "simd-flag-packing",
            "simd-window-runs",
            "simd-window-moments",
            "columnar-json-rows",
            "bounded-write-limits",
            "connector-row-request-division",
            "connector-confirmation-window",
            "pulsar-unconfirmed-receipts",
            "emitter-batch-subdivision",
            "emitter-batch-packing",
            "codec-bounded-encodings",
            "emitter-row-preparations",
            "emitter-retained-payloads",
            "sqs-batch-requests",
            "iceberg-nested-datetime-staging",
            "iceberg-table-projection",
            "iceberg-staged-file-encoding",
            "sentry-prepared-envelopes",
            "sentry-event-models",
            "http-emitter-requests",
            "mqtt-publish-packets",
            "postgres-insert-measures",
            "mysql-insert-measures",
            "mongodb-write-answers",
            "mongodb-bson-mapping",
            "websocket-signaling-protobuf-values",
            "websocket-acknowledgement-values",
            "websocket-signaling-protobuf-malformed",
            "otel-hex-ids",
            "models-header-pairs",
            "models-otel-mappings",
            "console-pending-backups",
            "console-pending-backups-malformed",
            "console-restore-drafts",
            "console-client-drafts",
            "runtime-window-sketch-panes",
            "kafka-partition-offsets",
            "ingestor-describe-envelope",
            "dns-operation-budget",
        })
        self.assertEqual({target.package for target in inventory.targets}, {
            "nervix-client-core",
            "nervix-client-wire",
            "nervix-client-ffi",
            "nervix-server",
            "nervix-plexus",
            "nervix-plexus-format",
            "nervix-models",
            "nervix-backup",
            "nervix-branch-instances",
            "nervix-interconnect",
            "nervix-deadlock",
            "nervix-consensus",
            "nervix-lint-report",
            "nervix-server",
            "nervix-simd-kernels",
            "nervix-checkpoint-replication",
            "nervix-connector-syslog",
            "nervix-connector-rabbitmq",
            "nervix-connector-pulsar",
            "nervix-connector-kafka",
            "nervix-paced-simulation",
            "nervix-wasm-protocol",
            "nervix-wasm",
            "nervix-wasm-sdk",
            "nervix-jaq",
            "nervix-expiry-map",
            "nervix-vm",
            "nervix-approx-into",
            "nervix-columnar-json",
            "nervix-bounded-write",
            "nervix-connector",
            "nervix-connector-sqs",
            "nervix-connector-iceberg",
            "nervix-connector-mqtt",
            "nervix-connector-postgres",
            "nervix-connector-mysql",
            "nervix-connector-mongodb",
            "nervix-connector-websockets",
            "nervix-connector-otel",
            "nervix-connector-sentry",
            "nervix-connector-http",
            "nervix-web-console",
            "nervix-dns",
        })
        for target in inventory.targets:
            self.assertTrue(target.source.is_file())
            self.assertTrue(target.corpus.is_dir())
            self.assertIn(
                target.test_target,
                {"lib", "test:representations", "test:connectors", "bin:nervix-web-console"},
            )
            self.assertEqual(target.corpus.parent.name, target.test.replace("::", "__"))
            self.assertGreater(target.random_iterations, 0)

    def test_invalid_case_budget_and_duplicate_id_fail(self) -> None:
        text = bolero.INVENTORY.read_text()
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "inventory.toml"
            path.write_text(text.replace("random_iterations = 256",
                                         "random_iterations = 0", 1))
            with self.assertRaisesRegex(bolero.BoleroError, "positive integer"):
                bolero.load_inventory(path)
            path.write_text(text.replace('id = "plexus-model"',
                                         'id = "plexus-expression"', 1))
            with self.assertRaisesRegex(bolero.BoleroError, "duplicate id"):
                bolero.load_inventory(path)

    def test_invalid_corpus_and_modeled_feature_fail(self) -> None:
        text = bolero.INVENTORY.read_text()
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "inventory.toml"
            path.write_text(text.replace(
                "canonical_round_trip_tests__bolero_expression_roundtrip_minimal_parentheses/corpus",
                "wrong/corpus", 1,
            ))
            with self.assertRaisesRegex(bolero.BoleroError, "actual work directory"):
                bolero.load_inventory(path)
            path.write_text(text.replace("features = []",
                                         'features = ["loom"]', 1))
            with self.assertRaisesRegex(bolero.BoleroError, "modeled feature"):
                bolero.load_inventory(path)
            path.write_text(text.replace("features = []",
                                         'features = ["deloxide"]', 1))
            with self.assertRaisesRegex(bolero.BoleroError, "modeled feature"):
                bolero.load_inventory(path)

    def test_empty_selection_fails(self) -> None:
        with self.assertRaisesRegex(bolero.BoleroError, "no Bolero targets selected"):
            bolero.select(bolero.load_inventory(), "absent-target")

    def test_tool_pins_and_target_identity_are_required(self) -> None:
        text = bolero.INVENTORY.read_text()
        for before, after, message in (
            ('cargo_bolero = "0.13.4"', 'cargo_bolero = "0.13"', "exact version"),
            ('nightly = "nightly-2026-09-17"', 'nightly = "nightly"', "dated toolchain"),
            ('sanitizer = "address"', 'sanitizer = "none"', "real sanitizer"),
            ('id = "plexus-expression"', 'id = "Invalid ID"', "invalid target id"),
            ('test_target = "lib"', 'test_target = "bin"', "invalid cargo test target"),
            ('test_target = "lib"', 'test_target = "bin:"', "invalid cargo test target"),
            ('test_target = "lib"', 'test_target = "example:owner"', "invalid cargo test target"),
            ('invariant = "', 'invariant = "" # ', "invariant is required"),
        ):
            with self.subTest(message=message):
                self.assert_invalid_inventory(text.replace(before, after, 1), message)

    def test_paths_and_missing_source_are_rejected(self) -> None:
        text = bolero.INVENTORY.read_text()
        source = 'source = "crates/plexus/src/canonical_round_trip_tests.rs"'
        self.assert_invalid_inventory(
            text.replace(source, 'source = ""', 1),
            "repository-relative path",
        )
        self.assert_invalid_inventory(
            text.replace(source, 'source = "../canonical_round_trip_tests.rs"', 1),
            "normalized repository-relative path",
        )
        self.assert_invalid_inventory(
            text.replace(source, 'source = "crates/plexus/src/missing.rs"', 1),
            "missing source",
        )

    def test_inventory_sections_and_fields_are_closed(self) -> None:
        text = bolero.INVENTORY.read_text()
        self.assert_invalid_inventory(text + '\n[extra]\nvalue = 1\n',
                                      "only tool and target")
        self.assert_invalid_inventory(
            text.replace('pr_fuzz_seconds = 30', 'pr_fuzz_seconds = 30\nextra = 1', 1),
            "tool section",
        )

    def test_prepared_targets_name_distinct_registered_harnesses(self) -> None:
        inventory = bolero.load_inventory()
        self.assertEqual(inventory.prepared_targets, ("registry-archived-models", "client-arrow-rows"))
        self.assertEqual(
            {harness.describe() for harness in inventory.prepared_harnesses()},
            {"nervix-server lib (no features)", "nervix-server lib (testing)"},
        )
        text = bolero.INVENTORY.read_text()
        prepared = 'prepared_targets = ["registry-archived-models", "client-arrow-rows"]'
        for value, message in (
            ('"registry-archived-models"', "must list registered target ids"),
            ('["registry-archived-models", 1]', "must list registered target ids"),
            ('["absent"]', "unregistered targets"),
            ('["registry-archived-models", "relay-channel-lifetimes"]', "one harness more than once"),
        ):
            with self.subTest(value=value):
                self.assert_invalid_inventory(
                    text.replace(prepared, f"prepared_targets = {value}", 1), message
                )
        self.assert_invalid_inventory(
            text.replace('domain_version = 1', 'unknown = 1', 1),
            "missing or unknown fields",
        )

    def test_target_names_and_empty_registry_are_rejected(self) -> None:
        text = bolero.INVENTORY.read_text()
        self.assert_invalid_inventory(
            text.replace('canonical_round_trip_tests::bolero_expression_roundtrip_minimal_parentheses',
                         'canonical_round_trip_tests::expression_roundtrip_minimal_parentheses', 1),
            "test name must start with bolero_",
        )
        self.assert_invalid_inventory(
            'target = []\n' + text.split('[[target]]')[0],
            "inventory contains no targets",
        )


class DiscoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.inventory = bolero.load_inventory()
        self.manifests = bolero.package_manifests()

    def compiled(self, package: str, test_target: str, *, features=(), manifest: pathlib.Path | None = None) -> list[dict[str, str]]:
        return [
            {
                "package_name": target.package,
                "test_name": target.test,
                "work_dir": str(target.work_dir),
            }
            for target in self.inventory.targets
            if target.package == package and target.test_target == test_target
        ]

    def source_functions(self, manifest: pathlib.Path) -> dict[str, pathlib.Path]:
        package = next(name for name, path in self.manifests.items() if path == manifest)
        return {
            target.test.split("::")[-1]: target.source
            for target in self.inventory.targets
            if target.package == package
        }

    def test_compiled_discovery_accepts_exact_current_targets(self) -> None:
        with (
            mock.patch.object(bolero, "static_targets", side_effect=self.source_functions),
            mock.patch.object(bolero, "listed_tests",
                              side_effect=lambda package, test_target, ignored, features, manifest=None:
                              [] if ignored else
                              [item["test_name"] for item in self.compiled(package, test_target)]),
            mock.patch.object(bolero, "compiled_targets", side_effect=self.compiled) as compiled,
        ):
            bolero.discover(self.inventory)
            compiled.assert_any_call("nervix-server", "lib", features=("testing",),
                                     manifest=self.manifests["nervix-server"])
            compiled.assert_any_call("nervix-lint-report", "lib", features=(),
                                     manifest=self.manifests["nervix-lint-report"])

    def test_ignored_and_unregistered_targets_fail(self) -> None:
        with (
            mock.patch.object(bolero, "static_targets", side_effect=self.source_functions),
            mock.patch.object(bolero, "listed_tests",
                              side_effect=lambda package, test_target, ignored, features, manifest=None:
                              ["tests::bolero_ignored"] if ignored else
                              [item["test_name"] for item in self.compiled(package, test_target)]),
            mock.patch.object(bolero, "compiled_targets", side_effect=self.compiled),
        ):
            with self.assertRaisesRegex(bolero.BoleroError, "ignored"):
                bolero.discover(self.inventory)

        def unregistered(manifest: pathlib.Path) -> dict[str, pathlib.Path]:
            found = self.source_functions(manifest)
            if manifest == self.manifests["nervix-backup"]:
                found["bolero_new"] = manifest.parent / "src/lib.rs"
            return found

        with mock.patch.object(bolero, "static_targets", side_effect=unregistered):
            with self.assertRaisesRegex(bolero.BoleroError, "differ from inventory"):
                bolero.discover(self.inventory)

    def test_missing_package_declaration_fails(self) -> None:
        with mock.patch.object(bolero, "package_manifests", return_value={}):
            with self.assertRaisesRegex(bolero.BoleroError, "package mismatch"):
                bolero.discover(self.inventory)

    def test_target_specific_bolero_dependency_is_discovered(self) -> None:
        self.assertTrue(bolero.declares_bolero({
            "target": {"cfg(unix)": {"dev-dependencies": {"bolero": "0.13.4"}}}
        }))

    def test_workspace_root_source_scan_keeps_package_ownership(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            manifest = root / "Cargo.toml"
            manifest.write_text("[package]\nname = 'host'\n")
            source = root / "src" / "owner.rs"
            source.parent.mkdir()
            source.write_text("fn bolero_host() { bolero::check!(); }")
            for relative in ("crates/connector", "tests/qualification"):
                package = root / relative
                package.mkdir(parents=True)
                (package / "Cargo.toml").write_text("[package]\nname = 'nested'\n")
                (package / "lib.rs").write_text("fn bolero_nested() { bolero::check!(); }")
            generated = root / "target" / "generated.rs"
            generated.parent.mkdir()
            generated.write_text("fn bolero_generated() { bolero::check!(); }")
            self.assertEqual(bolero.static_targets(manifest), {"bolero_host": source})

    def test_source_scan_respects_nested_package_ownership(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            manifest = root / "Cargo.toml"
            manifest.write_text("[package]\nname = 'server'\n")
            source = root / "src/lib.rs"
            source.parent.mkdir()
            source.write_text("fn bolero_server() { bolero::check!(); }")
            child = root / "crates/wire"
            child.mkdir(parents=True)
            child_manifest = child / "Cargo.toml"
            child_manifest.write_text("[package]\nname = 'wire'\n")
            child_source = child / "src/lib.rs"
            child_source.parent.mkdir()
            child_source.write_text("fn bolero_wire() { bolero::check!(); }")
            qualification = root / "tests/qualification"
            qualification.mkdir(parents=True)
            (qualification / "Cargo.toml").write_text("[package]\nname = 'qualification'\n")
            (qualification / "lib.rs").write_text(
                "fn bolero_qualification() { bolero::check!(); }")
            self.assertEqual(bolero.static_targets(manifest), {"bolero_server": source})
            self.assertEqual(bolero.static_targets(child_manifest), {"bolero_wire": child_source})

    def test_source_scan_keeps_authored_modules_and_excludes_build_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            manifest = root / "Cargo.toml"
            manifest.write_text("[package]\nname = 'server'\n")
            source = root / "src/target/lib.rs"
            source.parent.mkdir(parents=True)
            source.write_text("fn bolero_target_module() { bolero::check!(); }")
            for output in [root / "target", root / "configured-output"]:
                output.mkdir()
                (output / "generated.rs").write_text(
                    "fn bolero_generated() { bolero::check!(); }")
            with mock.patch.dict("os.environ", {"CARGO_TARGET_DIR": str(root / "configured-output")}):
                self.assertEqual(bolero.static_targets(manifest), {"bolero_target_module": source})

    def test_compiled_discovery_uses_declared_features(self) -> None:
        result = subprocess.CompletedProcess([], 0, "", "")
        for features in [(), ("testing",)]:
            with mock.patch.object(bolero, "command", return_value=result) as command:
                bolero.listed_tests("nervix-server", "lib", ignored=False, features=features)
                bolero.compiled_targets("nervix-server", "lib", features=features)
            self.assertEqual(len(command.call_args_list), 2)
            for call in command.call_args_list:
                args = call.args[0]
                self.assertEqual(args[:5], ["cargo", "test", "--package", "nervix-server", "--lib"])
                if features:
                    self.assertIn("--features", args)
                    self.assertEqual(args[args.index("--features") + 1], "testing")
                else:
                    self.assertNotIn("--features", args)

    def test_source_scan_rejects_unregistered_macro_shapes(self) -> None:
        cases = (
            ("bolero::check!();", "no owning function"),
            ("fn ordinary() { bolero::check!(); }", "must start with bolero_"),
            ("fn bolero_one() { fn helper() { bolero::check!(); } }",
             "property helper must start with bolero_"),
            ("fn bolero_one() { check!(); }", "qualify bolero::check!"),
            ("fn bolero_one() { bolero::check!(); }\n"
             "fn bolero_one() { bolero::check!(); }", "duplicate Bolero function"),
        )
        for content, message in cases:
            with self.subTest(message=message), tempfile.TemporaryDirectory() as directory:
                manifest = pathlib.Path(directory) / "Cargo.toml"
                manifest.write_text("[package]\nname = 'scan'\n")
                (manifest.parent / "lib.rs").write_text(content)
                with self.assertRaisesRegex(bolero.BoleroError, message):
                    bolero.static_targets(manifest)

    def test_source_scan_names_the_function_whose_body_holds_the_macro(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest = pathlib.Path(directory) / "Cargo.toml"
            manifest.write_text("[package]\nname = 'scan'\n")
            source = manifest.parent / "lib.rs"
            source.write_text(
                "fn bolero_nested_helper() {\n"
                "    fn helper(bytes: &[u8]) -> [u8; 2] { let _ = \"}\"; [0; 2] }\n"
                "    // fn commented() {\n"
                "    bolero::check!().for_each(|bytes: &[u8]| { helper(bytes); });\n"
                "}\n"
                "fn bolero_declaration() {\n"
                "    trait Declared { fn declared(&self); }\n"
                "    { bolero::check!(); }\n"
                "}\n")
            self.assertEqual(
                bolero.static_targets(manifest),
                {"bolero_nested_helper": source, "bolero_declaration": source},
            )

    def test_source_scan_obeys_package_ownership_and_excludes_generated_trees(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            manifest = root / "Cargo.toml"
            manifest.write_text("[package]\nname = 'root'\n")
            source = root / "src" / "lib.rs"
            source.parent.mkdir()
            source.write_text("fn bolero_root() { bolero::check!(); }")
            (source.parent / "notes.txt").write_text("check!();")
            member = root / "crates" / "member"
            member.mkdir(parents=True)
            member_manifest = member / "Cargo.toml"
            member_manifest.write_text("[package]\nname = 'member'\n")
            member_source = member / "lib.rs"
            member_source.write_text("fn bolero_member() { bolero::check!(); }")
            for generated in ("target", "node_modules", ".cache", "__fuzz__"):
                tree = root / generated
                tree.mkdir()
                (tree / "generated.rs").write_text("check!();")
            self.assertEqual(bolero.static_targets(manifest), {"bolero_root": source})
            self.assertEqual(bolero.static_targets(member_manifest),
                             {"bolero_member": member_source})

    def test_compiled_selection_must_match_inventory_and_work_directory(self) -> None:
        def listed(package: str, test_target: str, ignored: bool, features, manifest: pathlib.Path | None = None) -> list[str]:
            return [] if ignored else [
                item["test_name"] for item in self.compiled(package, test_target)
            ]

        with (
            mock.patch.object(bolero, "static_targets", side_effect=self.source_functions),
            mock.patch.object(bolero, "listed_tests", side_effect=listed),
            mock.patch.object(bolero, "compiled_targets", side_effect=lambda package, test_target, features, manifest=None:
                              self.compiled(package, test_target)[:-1]),
        ):
            with self.assertRaisesRegex(bolero.BoleroError, "listed Bolero tests"):
                bolero.discover(self.inventory)

        def misplaced(package: str, test_target: str, *, features, manifest: pathlib.Path | None = None) -> list[dict[str, str]]:
            targets = self.compiled(package, test_target)
            if package == "nervix-backup":
                targets[0]["work_dir"] = "/tmp/wrong-bolero-work-directory"
            return targets

        with (
            mock.patch.object(bolero, "static_targets", side_effect=self.source_functions),
            mock.patch.object(bolero, "listed_tests", side_effect=listed),
            mock.patch.object(bolero, "compiled_targets", side_effect=misplaced),
        ):
            with self.assertRaisesRegex(bolero.BoleroError, "compiled work directory"):
                bolero.discover(self.inventory)


class ExecutionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.inventory = bolero.load_inventory()
        self.target = self.inventory.targets[0]
        self.cargo_metadata = {"packages": []}
        for package, manifest in bolero.package_manifests().items():
            targets = [{
                "name": package.replace("-", "_"), "kind": ["lib"],
                "src_path": str(manifest.parent / "src/lib.rs"),
            }]
            if package in {"nervix-server", "nervix-plexus-format"}:
                targets.append({
                    "name": package.replace("-", "_"), "kind": ["bin"],
                    "src_path": str(manifest.parent / "src/main.rs"),
                })
            if package == "nervix-web-console":
                targets.append({
                    "name": package, "kind": ["bin"],
                    "src_path": str(manifest.parent / "src/main.rs"),
                })
            if package == "nervix-models":
                targets.append({
                    "name": "representations", "kind": ["test"],
                    "src_path": str(manifest.parent / "tests/representations/main.rs"),
                })
            if package == "nervix-plexus-format":
                targets.append({
                    "name": "repository_files", "kind": ["test"],
                    "src_path": str(manifest.parent / "tests/repository_files.rs"),
                })
            self.cargo_metadata["packages"].append({
                "name": package, "manifest_path": str(manifest), "targets": targets,
            })

    def command_with_build(self, build: subprocess.CompletedProcess[str]):
        def execute(args: list[str], **kwargs) -> subprocess.CompletedProcess[str]:
            if args[:2] == ["cargo", "metadata"]:
                self.assertIn("--no-deps", args)
                self.assertIn("--manifest-path", args)
                return subprocess.CompletedProcess(args, 0, json.dumps(self.cargo_metadata), "")
            return build
        return execute

    def test_models_directory_integration_executable_is_selected(self) -> None:
        target = next(item for item in self.inventory.targets if item.id == "models-names")
        with tempfile.TemporaryDirectory() as directory:
            run = pathlib.Path(directory)
            library = run / "nervix_models-1111"
            integration = run / "representations-2222"
            library.touch()
            integration.touch()
            build = subprocess.CompletedProcess([], 0,
                f"Executable unittests src/lib.rs ({library})\n"
                f"Executable tests/representations/main.rs ({integration})\n", "")
            with mock.patch.object(bolero, "command", side_effect=self.command_with_build(build)):
                self.assertEqual(bolero.build_instrumented(self.inventory, target, run), integration)

    def test_cargo_library_name_types_and_explicit_root_select_the_binary(self) -> None:
        package = next(item for item in self.cargo_metadata["packages"]
                       if item["name"] == self.target.package)
        package_root = pathlib.Path(package["manifest_path"]).parent
        package["targets"] = [{
            "name": "custom-owner", "kind": ["rlib", "cdylib"],
            "src_path": str(package_root / "fixtures/owner.rs"),
        }]
        with tempfile.TemporaryDirectory() as directory:
            run = pathlib.Path(directory)
            library = run / "custom_owner-1111"
            unrelated = run / "custom_owner-2222"
            library.touch()
            unrelated.touch()
            build = subprocess.CompletedProcess([], 0,
                f"Executable unittests fixtures/owner.rs ({library})\n"
                f"Executable unittests src/main.rs ({unrelated})\n", "")
            with mock.patch.object(bolero, "command", side_effect=self.command_with_build(build)):
                self.assertEqual(bolero.build_instrumented(self.inventory, self.target, run), library)

    def test_library_and_integration_using_one_root_keep_their_target_identity(self) -> None:
        target = next(item for item in self.inventory.targets if item.id == "models-names")
        package = next(item for item in self.cargo_metadata["packages"]
                       if item["name"] == target.package)
        integration_target = next(item for item in package["targets"] if item["kind"] == ["test"])
        package["targets"][0] = {**integration_target, "kind": ["lib"]}
        with tempfile.TemporaryDirectory() as directory:
            run = pathlib.Path(directory)
            library = run / "representations-1111"
            integration = run / "representations-2222"
            library.touch()
            integration.touch()
            build = subprocess.CompletedProcess([], 0,
                f"Executable unittests tests/representations/main.rs ({library})\n"
                f"Executable tests/representations/main.rs ({integration})\n", "")
            with mock.patch.object(bolero, "command", side_effect=self.command_with_build(build)):
                self.assertEqual(bolero.build_instrumented(self.inventory, target, run), integration)
                second = run / "library"
                second.mkdir()
                self.assertEqual(
                    bolero.build_instrumented(self.inventory, dataclasses.replace(target, test_target="lib"), second),
                    library,
                )

    def test_cargo_package_and_target_must_be_present_and_unambiguous(self) -> None:
        target = next(item for item in self.inventory.targets if item.id == "models-names")
        original = json.dumps(self.cargo_metadata)
        for case in ("missing-package", "ambiguous-package", "missing-target", "ambiguous-target"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                self.cargo_metadata = json.loads(original)
                packages = self.cargo_metadata["packages"]
                package = next(item for item in packages if item["name"] == target.package)
                if case == "missing-package":
                    packages.remove(package)
                elif case == "ambiguous-package":
                    packages.append(package)
                elif case == "missing-target":
                    package["targets"] = [item for item in package["targets"] if item["kind"] != ["test"]]
                else:
                    package["targets"].append(next(item for item in package["targets"] if item["kind"] == ["test"]))
                build = subprocess.CompletedProcess([], 0, "", "")
                with mock.patch.object(bolero, "command", side_effect=self.command_with_build(build)) as calls:
                    with self.assertRaisesRegex(bolero.BoleroError, "expected one Cargo"):
                        bolero.build_instrumented(self.inventory, target, pathlib.Path(directory))
                    self.assertEqual(calls.call_count, 1)

    def test_server_library_executable_is_selected_among_package_targets(self) -> None:
        target = dataclasses.replace(self.target, package="nervix-server")
        with tempfile.TemporaryDirectory() as directory:
            run = pathlib.Path(directory)
            library = run / "nervix_server-1111"
            binary = run / "nervix_server-2222"
            for executable in (library, binary):
                executable.write_bytes(b"")
            output = (
                f"  Executable unittests src/lib.rs ({library})\n"
                f"  Executable unittests src/main.rs ({binary})\n"
            )
            build = subprocess.CompletedProcess([], 0, output, "")
            with mock.patch.object(bolero, "command", side_effect=self.command_with_build(build)):
                self.assertEqual(bolero.build_instrumented(self.inventory, target, run), library)

    def test_command_propagates_engine_exit_and_timeout(self) -> None:
        with self.assertRaisesRegex(bolero.BoleroError, "exited 7"):
            bolero.command([sys.executable, "-c", "raise SystemExit(7)"])
        with self.assertRaisesRegex(bolero.BoleroError, "timed out"):
            bolero.command([sys.executable, "-c", "import time; time.sleep(5)"],
                           timeout=1)
        with tempfile.TemporaryDirectory() as directory:
            log = pathlib.Path(directory) / "timeout.log"
            with self.assertRaisesRegex(bolero.BoleroError, "timed out"):
                bolero.command([sys.executable, "-c",
                                "import time; time.sleep(5)"], timeout=1, log=log)
            self.assertIn("timeout after 1s", log.read_text())

    def ordinary_result(self, target: bolero.Target) -> subprocess.CompletedProcess[str]:
        corpus_count = sum(
            seed.is_file() and not seed.name.startswith(".")
            for seed in target.corpus.iterdir()
        )
        return subprocess.CompletedProcess(
            [], 0, f"corpus inputs: {corpus_count} | rng inputs: {target.random_iterations}\n"
            "test result: ok. 1 passed; 0 failed\n", ""
        )

    def test_ordinary_counts_require_random_and_corpus_cases(self) -> None:
        good = self.ordinary_result(self.target)
        iterations = self.target.random_iterations
        with mock.patch.object(bolero, "command", return_value=good):
            bolero.test_targets(self.inventory, (self.target,))
        missing_cases = subprocess.CompletedProcess(
            [], 0, good.stdout.replace(f"rng inputs: {iterations}", "rng inputs: 0"), ""
        )
        with mock.patch.object(bolero, "command", return_value=missing_cases):
            with self.assertRaisesRegex(bolero.BoleroError, f"expected {iterations}"):
                bolero.test_targets(self.inventory, (self.target,))

    def test_ordinary_prepares_each_exact_harness_before_bounded_execution(self) -> None:
        target = dataclasses.replace(self.target, package="nervix-server", features=())
        repeated = dataclasses.replace(target, id="another-target", test="tests::bolero_another")
        with_features = dataclasses.replace(target, id="feature-target", features=("testing",))
        good = self.ordinary_result(target)
        with mock.patch.object(bolero, "command", return_value=good) as command:
            bolero.test_targets(self.inventory, (target, repeated, with_features))
        builds = [call for call in command.call_args_list if "--no-run" in call.args[0]]
        self.assertEqual(len(builds), 2)
        self.assertEqual(builds[0].args[0], bolero.cargo_test_args(target) + ["--no-run"])
        self.assertEqual(builds[1].args[0], bolero.cargo_test_args(with_features) + ["--no-run"])
        self.assertTrue(all(call.kwargs["timeout"] == 1800 for call in builds))
        cases = [call for call in command.call_args_list if "--exact" in call.args[0]]
        self.assertEqual(len(cases), 3)
        self.assertTrue(all(call.kwargs["timeout"] == 240 for call in cases))
        self.assertEqual(command.call_args_list[0], builds[0])
        self.assertEqual(command.call_args_list[3], builds[1])

    def test_failed_ordinary_preparation_stops_before_any_property_execution(self) -> None:
        calls = []

        def prepare_fails(args, **kwargs):
            calls.append(args)
            if "--no-run" in args:
                raise bolero.BoleroError("ordinary build refused")
            return subprocess.CompletedProcess(
                [], 0, "corpus inputs: 2 | rng inputs: 256\n"
                "test result: ok. 1 passed; 0 failed\n", ""
            )

        with mock.patch.object(bolero, "command", side_effect=prepare_fails):
            with self.assertRaisesRegex(bolero.BoleroError, "ordinary build refused"):
                bolero.test_targets(self.inventory, (self.target,))
        self.assertEqual(len(calls), 1)
        self.assertIn("--no-run", calls[0])

    def test_ordinary_run_rejects_missing_assertion_and_corpus(self) -> None:
        for output, message in (
            ("test result: ok. 0 passed; 0 failed", "exactly once"),
            ("test result: ok. 1 passed; 0 failed", "input counts"),
            ("test result: ok. 1 passed; 0 failed\n"
             "corpus inputs: 0 | rng inputs: 256", "corpus was not replayed"),
        ):
            with self.subTest(message=message):
                result = subprocess.CompletedProcess([], 0, output, "")
                with mock.patch.object(bolero, "command", return_value=result):
                    with self.assertRaisesRegex(bolero.BoleroError, message):
                        bolero.test_targets(self.inventory, (self.target,))

    def test_tool_version_and_instrumented_binary_are_required(self) -> None:
        mismatch = subprocess.CompletedProcess([], 0, "cargo-bolero 0.12.0", "")
        with mock.patch.object(bolero, "command", return_value=mismatch):
            with self.assertRaisesRegex(bolero.BoleroError, "required"):
                bolero.verify_tool(self.inventory)
        with tempfile.TemporaryDirectory() as directory:
            build = subprocess.CompletedProcess([], 0, "no executable", "")
            with mock.patch.object(bolero.shutil, "which", return_value=None):
                with self.assertRaisesRegex(bolero.BoleroError, "rustup is required"):
                    bolero.scoped_build_env(self.target, pathlib.Path(directory))
            with mock.patch.object(bolero, "command", side_effect=self.command_with_build(build)):
                with self.assertRaisesRegex(bolero.BoleroError, "expected one instrumented"):
                    bolero.build_instrumented(self.inventory, self.target,
                                              pathlib.Path(directory))

    def test_instrumented_build_returns_the_registered_harness(self) -> None:
        target = dataclasses.replace(self.target, package="nervix-plexus-format")
        with tempfile.TemporaryDirectory() as directory:
            run = pathlib.Path(directory)
            library = run / "nervix_plexus_format-1111"
            integration = run / "repository_files-3333"
            for executable in (library, integration):
                executable.write_bytes(b"")
            output = f"  Executable unittests src/lib.rs ({library})\n"
            build = subprocess.CompletedProcess([], 0, output, "")
            with mock.patch.object(bolero, "command", side_effect=self.command_with_build(build)) as invocation:
                self.assertEqual(bolero.build_instrumented(self.inventory, target, run), library)
                self.assertEqual(invocation.call_args.kwargs["env"]["PATH"].split(os.pathsep)[0],
                                 str(run / "scope-bin"))
                self.assertEqual(invocation.call_args.kwargs["timeout"], 1800)
                integration_target = dataclasses.replace(
                    target, test_target="test:repository_files"
                )
                again = run / "again"
                again.mkdir()
                invocation.side_effect = self.command_with_build(subprocess.CompletedProcess(
                    [], 0, f"  Executable tests/repository_files.rs ({integration})\n", ""))
                self.assertEqual(
                    bolero.build_instrumented(self.inventory, integration_target, again, timeout=7200),
                    integration,
                )
                self.assertEqual(invocation.call_args.kwargs["timeout"], 7200)

    def test_source_instrumentation_preserves_the_sanitizer_build_contract(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            run = pathlib.Path(directory)
            binary = run / "nervix_backup-1234"
            binary.touch()
            target = next(item for item in self.inventory.targets if item.id == "backup-record-manifest")
            build = subprocess.CompletedProcess([], 0, f"Executable unittests src/lib.rs ({binary})\n", "")
            with mock.patch.object(bolero, "command", side_effect=self.command_with_build(build)) as commands:
                self.assertEqual(bolero.build_instrumented(self.inventory, target, run), binary)
            call = commands.call_args
            self.assertIn("-C instrument-coverage", call.kwargs.get("env", {}).get("RUSTFLAGS", ""))
            self.assertEqual(call.kwargs["env"]["LLVM_PROFILE_FILE"], "/dev/null")
            args = call.args[0]
            self.assertEqual(args[args.index("--sanitizer") + 1], self.inventory.sanitizer)
            self.assertEqual(args[args.index("--profile") + 1], "fuzz")

    def test_build_deadline_changes_only_the_compilation_budget(self) -> None:
        target = dataclasses.replace(self.target, package="nervix-plexus-format")
        with tempfile.TemporaryDirectory() as directory:
            run = pathlib.Path(directory)
            library = run / "nervix_plexus_format-1111"
            library.write_bytes(b"")
            output = f"  Executable unittests src/lib.rs ({library})\n"
            result = subprocess.CompletedProcess([], 0, output, "")
            with mock.patch.dict(os.environ, {"BOLERO_BUILD_TIMEOUT_SECONDS": "7200"}), \
                    mock.patch.object(bolero, "command", side_effect=self.command_with_build(result)) as build:
                self.assertEqual(bolero.build_instrumented(self.inventory, target, run), library)
                args = build.call_args.args[0]
                self.assertEqual(build.call_args.kwargs["timeout"], 7200)
                self.assertEqual(args[args.index("--timeout") + 1], "10s")
                self.assertEqual(args[args.index("--runs") + 1], "0")

    def test_build_deadline_refuses_invalid_bounds_before_starting_cargo(self) -> None:
        for budget in ("0", "-1", "invalid"):
            with self.subTest(budget=budget), tempfile.TemporaryDirectory() as directory, \
                    mock.patch.dict(os.environ, {"BOLERO_BUILD_TIMEOUT_SECONDS": budget}), \
                    mock.patch.object(bolero, "command") as build:
                with self.assertRaisesRegex(bolero.BoleroError, "positive integer"):
                    bolero.build_instrumented(self.inventory, self.target, pathlib.Path(directory))
                build.assert_not_called()

    def test_feature_and_integration_test_target_arguments(self) -> None:
        target = dataclasses.replace(self.target, test_target="test:property_suite",
                                     features=("fuzz-support",))
        self.assertIn("property_suite", bolero.cargo_test_args(target))
        self.assertIn("fuzz-support", bolero.cargo_test_args(target))
        self.assertIn("fuzz-support", bolero.bolero_args(self.inventory, target))
        self.assertIn("--engine-args=-print_funcs=0", bolero.bolero_args(self.inventory, target))

    def test_each_test_target_kind_has_its_own_cargo_selector(self) -> None:
        for test_target, selector in [("lib", ["--lib"]),
                                       ("test:property_suite", ["--test", "property_suite"]),
                                       ("bin:nervix-web-console", ["--bin", "nervix-web-console"])]:
            with self.subTest(test_target=test_target):
                args = bolero.cargo_test_args(dataclasses.replace(self.target, test_target=test_target))
                self.assertEqual(args[-len(selector):], selector)

    def test_binary_unit_tests_are_selected_beside_the_library_of_the_same_name(self) -> None:
        target = next(item for item in self.inventory.targets if item.id == "console-pending-backups")
        with tempfile.TemporaryDirectory() as directory:
            run = pathlib.Path(directory)
            library = run / "nervix_web_console-1111"
            binary = run / "nervix_web_console-2222"
            library.touch()
            binary.touch()
            build = subprocess.CompletedProcess([], 0,
                f"Executable unittests src/lib.rs ({library})\n"
                f"Executable unittests src/main.rs ({binary})\n", "")
            with mock.patch.object(bolero, "command", side_effect=self.command_with_build(build)):
                self.assertEqual(bolero.build_instrumented(self.inventory, target, run), binary)
                second = run / "library"
                second.mkdir()
                self.assertEqual(
                    bolero.build_instrumented(self.inventory, dataclasses.replace(target, test_target="lib"), second),
                    library,
                )

    def test_a_binary_target_names_one_binary_of_its_package(self) -> None:
        target = next(item for item in self.inventory.targets if item.id == "console-pending-backups")
        for test_target in ("bin:nervix-web-console-renamed", "test:nervix-web-console"):
            with self.subTest(test_target=test_target), tempfile.TemporaryDirectory() as directory:
                build = subprocess.CompletedProcess([], 0, "", "")
                with mock.patch.object(bolero, "command", side_effect=self.command_with_build(build)):
                    with self.assertRaisesRegex(bolero.BoleroError, "expected one Cargo"):
                        bolero.build_instrumented(
                            self.inventory, dataclasses.replace(target, test_target=test_target),
                            pathlib.Path(directory),
                        )

    def test_instrumented_cargo_scope_keeps_the_pinned_tool_and_wrapper(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            tool = root / "actual-rustup"
            tool.write_text(
                f"#!{sys.executable}\n"
                "import json,os,sys\n"
                "print(json.dumps({'args':sys.argv[1:],'wrapper':os.environ.get('RUSTC_WRAPPER'),"
                "'build_wrapper':os.environ.get('CARGO_BUILD_RUSTC_WRAPPER')}))\n")
            tool.chmod(0o700)
            for test_target, selector in [("lib", ["--lib"]),
                                           ("test:representations", ["--test", "representations"]),
                                           ("bin:nervix-web-console", ["--bin", "nervix-web-console"])]:
                target = dataclasses.replace(self.target, test_target=test_target)
                run = root / test_target.replace(":", "_")
                run.mkdir()
                with mock.patch.object(bolero.shutil, "which", return_value=str(tool)):
                    env = bolero.scoped_build_env(target, run)
                for command, args in [("cargo", ["test", "--profile", "fuzz", "bolero_case"]),
                                       ("rustc", ["-vV"])]:
                    invocation = ["run", self.inventory.nightly, command, *args]
                    result = subprocess.run(
                        [str(pathlib.Path(env["PATH"].split(os.pathsep)[0]) / "rustup"), *invocation],
                        env={**os.environ, **env}, capture_output=True, text=True, check=True)
                    observed = json.loads(result.stdout)
                    expected = [*invocation[:4], *selector, *invocation[4:]] if command == "cargo" else invocation
                    self.assertEqual(observed["args"], expected)
                    self.assertEqual(observed["wrapper"], os.environ.get("RUSTC_WRAPPER"))
                    self.assertEqual(observed["build_wrapper"], os.environ.get("CARGO_BUILD_RUSTC_WRAPPER"))

    def test_instrumented_library_is_selected_among_package_binaries(self) -> None:
        target = next(item for item in self.inventory.targets if item.id == "typed-report")
        with tempfile.TemporaryDirectory() as directory:
            temporary = pathlib.Path(directory)
            library = temporary / "nervix_lint_report-library"
            binary = temporary / "nervix_lint_report-binary"
            library.touch()
            binary.touch()
            build = subprocess.CompletedProcess([], 0,
                f"Executable unittests src/lib.rs ({library})\n"
                f"Executable unittests src/main.rs ({binary})\n", "")
            with mock.patch.object(bolero, "command", side_effect=self.command_with_build(build)):
                self.assertEqual(bolero.build_instrumented(self.inventory, target, temporary), library)

    def test_fuzz_build_failure_records_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with (
                mock.patch.object(bolero, "run_dir", return_value=pathlib.Path(directory)),
                mock.patch.object(bolero, "verify_tool"),
                mock.patch.object(bolero.coverage, "Campaign"),
                mock.patch.object(bolero, "build_instrumented",
                                  side_effect=bolero.BoleroError("build failed")),
                mock.patch.object(bolero, "metadata") as metadata,
            ):
                with self.assertRaisesRegex(bolero.BoleroError, "build failed"):
                    bolero.fuzz_targets(self.inventory, (self.target,), 1)
                self.assertEqual(metadata.call_args.args[3], "failed build")

    def test_preparation_records_only_the_registered_build_and_keeps_campaign_checks(self) -> None:
        for failure in (False, True):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                run = pathlib.Path(directory)
                with (
                    mock.patch.object(bolero, "run_dir", return_value=run),
                    mock.patch.object(bolero, "verify_tool"),
                    mock.patch.object(bolero, "build_instrumented",
                                      side_effect=bolero.BoleroError("build failed") if failure else None,
                                      return_value=run / "binary") as build,
                    mock.patch.object(bolero, "run_instrumented") as execute,
                    mock.patch.object(bolero, "metadata") as metadata,
                ):
                    if failure:
                        with self.assertRaisesRegex(bolero.BoleroError, "build failed"):
                            bolero.prepare_target(self.inventory, self.target)
                    else:
                        bolero.prepare_target(self.inventory, self.target)
                    build.assert_called_once_with(self.inventory, self.target, run)
                    execute.assert_not_called()
                    self.assertEqual(metadata.call_args.args[3],
                                     "failed preparation" if failure else "instrumented preparation")
                    self.assertFalse((run / "corpus").exists())
    def test_preparation_records_the_selected_binary_without_running_a_campaign(self) -> None:
        target = next(item for item in self.inventory.targets if item.id == "models-archived-counts")
        with tempfile.TemporaryDirectory() as directory:
            run = pathlib.Path(directory)
            binary = run / "representations-2222"
            binary.touch()
            build = subprocess.CompletedProcess([], 0,
                f"Executable tests/representations/main.rs ({binary})\n", "")
            execute_build = self.command_with_build(build)

            def execute(args, **kwargs):
                if args[:2] == ["git", "rev-parse"]:
                    return subprocess.CompletedProcess(args, 0, "revision\n", "")
                return execute_build(args, **kwargs)

            with (
                mock.patch.object(bolero, "run_dir", return_value=run),
                mock.patch.object(bolero, "verify_tool"),
                mock.patch.object(bolero, "command", side_effect=execute) as commands,
                mock.patch.object(bolero, "run_instrumented") as campaign,
                contextlib.redirect_stdout(io.StringIO()),
            ):
                bolero.prepare_target(self.inventory, target)
            campaign.assert_not_called()
            report = json.loads((run / "metadata.json").read_text())
            self.assertEqual(report["result"], "instrumented preparation")
            self.assertEqual(report["target"], target.id)
            self.assertEqual(report["test_target"], "test:representations")
            self.assertEqual(report["features"], list(target.features))
            args = next(call.args[0] for call in commands.call_args_list
                        if call.args[0][:3] == ["cargo", "bolero", "test"])
            self.assertEqual(args[args.index("--runs") + 1], "0")
            self.assertEqual(args[args.index("--toolchain") + 1], self.inventory.nightly)
            self.assertEqual(args[args.index("--sanitizer") + 1], self.inventory.sanitizer)

    def test_failed_preparation_keeps_evidence_and_propagates_the_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            run = pathlib.Path(directory)
            with (
                mock.patch.object(bolero, "run_dir", return_value=run),
                mock.patch.object(bolero, "verify_tool"),
                mock.patch.object(bolero, "build_instrumented",
                                  side_effect=bolero.BoleroError("build failed")),
                mock.patch.object(bolero, "command",
                                  return_value=subprocess.CompletedProcess([], 0, "revision\n", "")),
                mock.patch.object(bolero, "run_instrumented") as campaign,
            ):
                with self.assertRaisesRegex(bolero.BoleroError, "build failed"):
                    bolero.prepare_target(self.inventory, self.target)
            campaign.assert_not_called()
            report = json.loads((run / "metadata.json").read_text())
            self.assertEqual(report["result"], "failed preparation")
            self.assertEqual(report["target"], self.target.id)

    def test_fuzz_copies_seed_corpus_and_requires_engine_completion(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            run = pathlib.Path(directory) / "run"
            run.mkdir()
            success = subprocess.CompletedProcess([], 0, "#100 DONE", "")
            with (
                mock.patch.object(bolero, "run_dir", return_value=run),
                mock.patch.object(bolero, "build_instrumented",
                                  return_value=bolero.ROOT / "fake-binary"),
                mock.patch.object(bolero, "verify_tool"),
                mock.patch.object(bolero.coverage, "Campaign"),
                mock.patch.object(bolero, "metadata"),
                mock.patch.object(bolero, "command", return_value=success) as execute,
            ):
                bolero.fuzz_targets(self.inventory, (self.target,), 1)
            self.assertEqual(
                {file.name for file in (run / "corpus").iterdir()},
                {file.name for file in self.target.corpus.iterdir()},
            )
            flags = execute.call_args.kwargs["env"]["BOLERO_LIBFUZZER_ARGS"]
            self.assertIn(str(run / "corpus"), flags)
            self.assertIn(str(run / "crashes"), flags)
            self.assertIn("-max_total_time=1", flags)
            self.assertIn("-print_funcs=0", flags.split())
            self.assertEqual(execute.call_args.kwargs["timeout"], 31)

        with tempfile.TemporaryDirectory() as directory:
            run = pathlib.Path(directory) / "run"
            run.mkdir()
            incomplete = subprocess.CompletedProcess([], 0, "no completion", "")
            with (
                mock.patch.object(bolero, "run_dir", return_value=run),
                mock.patch.object(bolero, "build_instrumented",
                                  return_value=bolero.ROOT / "fake-binary"),
                mock.patch.object(bolero, "verify_tool"),
                mock.patch.object(bolero.coverage, "Campaign"),
                mock.patch.object(bolero, "metadata"),
                mock.patch.object(bolero, "command", return_value=incomplete),
            ):
                with self.assertRaisesRegex(bolero.BoleroError, "did not report completion"):
                    bolero.fuzz_targets(self.inventory, (self.target,), 1)

    def test_campaign_keeps_symbolizer_settings_and_records_progress_flags(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            run = pathlib.Path(directory)
            binary = run / "fuzz-binary"
            binary.write_text(
                f"#!{sys.executable}\n"
                "import json,os\n"
                "print(json.dumps({name:os.environ[name] for name in "
                "['ASAN_OPTIONS','ASAN_SYMBOLIZER_PATH','BOLERO_LIBFUZZER_ARGS']}))\n"
                "print('#100 DONE')\n"
            )
            binary.chmod(0o700)
            settings = {
                "ASAN_OPTIONS": "symbolize=1:detect_leaks=0",
                "ASAN_SYMBOLIZER_PATH": "/configured/llvm-symbolizer",
            }
            with (
                mock.patch.dict(os.environ, settings),
                mock.patch.object(bolero, "run_dir", return_value=run),
                mock.patch.object(bolero, "build_instrumented", return_value=binary),
                mock.patch.object(bolero, "verify_tool"),
                mock.patch.object(bolero.coverage, "Campaign"),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                bolero.fuzz_targets(self.inventory, (self.target,), 2)
            observed = json.loads((run / "fuzz.log").read_text().splitlines()[0])
            for name, value in settings.items():
                self.assertEqual(observed[name], value)
            flags = observed["BOLERO_LIBFUZZER_ARGS"].split()
            self.assertIn("-print_funcs=0", flags)
            self.assertIn("-max_total_time=2", flags)
            self.assertIn(f"-timeout={self.target.case_timeout_seconds}", flags)
            report = json.loads((run / "metadata.json").read_text())
            self.assertEqual(report["result"], "passed")
            self.assertEqual(report["command"][0],
                             "BOLERO_LIBFUZZER_ARGS=" + observed["BOLERO_LIBFUZZER_ARGS"])

    def qualification_world(self, root: pathlib.Path):
        """Run directories under a temporary repository root, with numbered attempts."""
        runs = root / "target/bolero/runs"
        attempts = iter(range(1000))

        def run_dir(target: bolero.Target) -> pathlib.Path:
            path = runs / target.id / str(next(attempts))
            path.mkdir(parents=True)
            return path

        return runs, run_dir

    def qualification_cargo(self, sanitizer_report: str):
        """Plays cargo-bolero for the qualification package and cargo's test listing."""

        def execute(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
            if "--crashes-dir" in args:
                crashes = pathlib.Path(args[args.index("--crashes-dir") + 1])
                if args[-1].endswith("fails_on_marker"):
                    (crashes / "crash-input").write_bytes(b"\x42\x00")
                if args[-1].endswith("memory_error_on_marker"):
                    (crashes / "crash-input").write_bytes(b"\x33")
                    pathlib.Path(kwargs["log"]).write_text(sanitizer_report)
            return subprocess.CompletedProcess(args, 1, "timeout or intentional failure", "")

        def functions(manifest: pathlib.Path) -> list[tuple[str, bool]]:
            source = (manifest.parent / "src/lib.rs").read_text()
            found = []
            for name in re.findall(r"fn (bolero_\w+)\(", source):
                found.append((name, f"#[ignore]\n    fn {name}(" in source))
            return found

        def listed(package, test_target, *, ignored, features, manifest=None):
            return [f"tests::{name}" for name, skipped in functions(manifest) if skipped == ignored]

        def compiled(package, test_target, *, features, manifest=None):
            return [
                {"package_name": package, "test_name": f"tests::{name}",
                 "work_dir": str(manifest.parent / "src/__fuzz__" / f"tests__{name}")}
                for name, skipped in functions(manifest) if not skipped
            ]

        return execute, listed, compiled

    def test_qualification_records_every_way_a_run_must_fail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            runs, run_dir = self.qualification_world(root)
            execute, listed, compiled = self.qualification_cargo(
                "==7==ERROR: AddressSanitizer: heap-buffer-overflow on address 0x1\n"
            )

            def reduce(inventory, target, failure):
                path = run_dir(target)
                minimized = path / "minimized"
                minimized.write_bytes(b"\x42")
                record = {"target": target.id, "saved_inputs": {
                    str(minimized.relative_to(root)): hashlib.sha256(b"\x42").hexdigest()}}
                (path / "metadata.json").write_text(json.dumps(record))
                return minimized

            with (
                mock.patch.object(bolero, "ROOT", root),
                mock.patch.object(bolero, "RUNS", runs),
                mock.patch.object(bolero, "run_dir", side_effect=run_dir),
                mock.patch.object(bolero, "verify_tool"),
                mock.patch.object(bolero, "command", side_effect=execute) as commands,
                mock.patch.object(bolero, "listed_tests", side_effect=listed),
                mock.patch.object(bolero, "compiled_targets", side_effect=compiled),
                mock.patch.object(bolero, "reduce_failure", side_effect=reduce),
                mock.patch.object(bolero, "replay", return_value=101),
                mock.patch.object(bolero, "replay_sanitizer", return_value=1) as sanitizer_replay,
                mock.patch.object(bolero, "metadata"),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                bolero.qualify(self.inventory)
            record = json.loads(next(runs.glob("qualification-assertion/0/qualification.json")).read_text())
            self.assertEqual(record["verdict"], "qualified")
            self.assertEqual([case["case"] for case in record["cases"]], [
                "property-mismatch", "minimization-and-replay", "corrupt-replay-input",
                "unreadable-replay-record", "sanitizer-finding", "sanitizer-replay", "timeout",
                "engine-failure", "missing-engine-target", "discovery-control", "discovery-ignored",
                "discovery-unregistered", "discovery-missing", "empty-selection", "empty-shard",
                "empty-campaign",
            ])
            cases = {case["case"]: case for case in record["cases"]}
            self.assertIn("AddressSanitizer: heap-buffer-overflow", cases["sanitizer-finding"]["observed"])
            self.assertIn("ignored Bolero targets", cases["discovery-ignored"]["observed"])
            self.assertIn("has digest", cases["corrupt-replay-input"]["observed"])
            self.assertTrue(all(case["requirement"] for case in record["cases"]))
            self.assertEqual(sanitizer_replay.call_args.args[1].id, "qualification-sanitizer")
            self.assertEqual(len(commands.call_args_list), 5)
            for call in commands.call_args_list:
                self.assertIn("--engine-args=-print_funcs=0", call.args[0])
                expected_timeout = 10 if call.args[0][-1].endswith("times_out_on_marker") else 1800
                self.assertEqual(call.kwargs["timeout"], expected_timeout)
            generated = runs / "qualification-assertion/0/discovery-ignored/src/lib.rs"
            self.assertIn("#[ignore]\n    fn bolero_ignored()", generated.read_text())
            self.assertTrue((generated.parent.parent / "Cargo.lock").is_file())

    def test_qualification_fails_when_a_memory_error_goes_unreported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            runs, run_dir = self.qualification_world(root)
            execute, listed, compiled = self.qualification_cargo("no report\n")

            def reduce(inventory, target, failure):
                path = run_dir(target)
                minimized = path / "minimized"
                minimized.write_bytes(b"\x42")
                (path / "metadata.json").write_text(json.dumps({"target": target.id, "saved_inputs": {
                    str(minimized.relative_to(root)): hashlib.sha256(b"\x42").hexdigest()}}))
                return minimized

            with (
                mock.patch.object(bolero, "ROOT", root),
                mock.patch.object(bolero, "RUNS", runs),
                mock.patch.object(bolero, "run_dir", side_effect=run_dir),
                mock.patch.object(bolero, "verify_tool"),
                mock.patch.object(bolero, "command", side_effect=execute),
                mock.patch.object(bolero, "reduce_failure", side_effect=reduce),
                mock.patch.object(bolero, "replay", return_value=101),
                mock.patch.object(bolero, "metadata") as metadata,
                contextlib.redirect_stdout(io.StringIO()),
            ):
                with self.assertRaisesRegex(bolero.BoleroError, "memory error did not fail"):
                    bolero.qualify(self.inventory)
            record = json.loads(next(runs.glob("qualification-assertion/0/qualification.json")).read_text())
            self.assertEqual(record["verdict"], "failed")
            results = [call.args[3] for call in metadata.call_args_list]
            self.assertIn("missing sanitizer finding", results)
            self.assertEqual(results[-1], "failed qualification")

    def test_a_qualification_case_must_fail_for_its_own_reason(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            qualification = bolero.Qualification(pathlib.Path(directory))
            with self.assertRaisesRegex(bolero.BoleroError, "but it succeeded"):
                qualification.expect_refusal("case", "it must fail", "reason", lambda: None)

            def other() -> None:
                raise bolero.BoleroError("another failure")

            with self.assertRaisesRegex(bolero.BoleroError, "failed for another reason"):
                qualification.expect_refusal("case", "it must fail", "reason", other)
            qualification.record("kept", "a requirement", "observed", pathlib.Path("/outside"))
            qualification.write("failed")
            record = json.loads((pathlib.Path(directory) / "qualification.json").read_text())
            self.assertEqual(record["cases"], [{"case": "kept", "requirement": "a requirement",
                                                "observed": "observed", "evidence": ["/outside"]}])

    def test_sanitizer_replay_runs_the_exact_input_once_in_the_instrumented_build(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temporary = pathlib.Path(directory)
            failure = temporary / "failure"
            failure.write_bytes(b"\x33")
            for status, outcome in ((1, "instrumented replay reproduced"), (0, "instrumented replay not reproduced")):
                run = temporary / f"run-{status}"
                run.mkdir()
                result = subprocess.CompletedProcess([], status, "ERROR: AddressSanitizer", "")
                with (
                    self.subTest(status=status),
                    mock.patch.object(bolero, "verify_tool"),
                    mock.patch.object(bolero, "run_dir", return_value=run),
                    mock.patch.object(bolero, "build_instrumented", return_value=bolero.ROOT / "fake-binary"),
                    mock.patch.object(bolero, "run_instrumented", return_value=result) as instrumented,
                    mock.patch.object(bolero, "metadata") as metadata,
                    contextlib.redirect_stdout(io.StringIO()),
                ):
                    self.assertEqual(bolero.replay_sanitizer(self.inventory, self.target, failure), status)
                flags = instrumented.call_args.args[2]
                self.assertEqual(flags[0], str(run / "input"))
                self.assertIn(f"-artifact_prefix={run}/", flags)
                self.assertIn("-print_funcs=0", flags)
                self.assertEqual((run / "input").read_bytes(), b"\x33")
                self.assertEqual(metadata.call_args.args[3], outcome)
                self.assertEqual(metadata.call_args.args[5]["replayed_input_sha256"],
                                 hashlib.sha256(b"\x33").hexdigest())
            run = temporary / "run-failed-build"
            run.mkdir()
            with (
                mock.patch.object(bolero, "verify_tool"),
                mock.patch.object(bolero, "run_dir", return_value=run),
                mock.patch.object(bolero, "build_instrumented", side_effect=bolero.BoleroError("build")),
                mock.patch.object(bolero, "metadata") as metadata,
            ):
                with self.assertRaisesRegex(bolero.BoleroError, "build"):
                    bolero.replay_sanitizer(self.inventory, self.target, failure)
            self.assertEqual(metadata.call_args.args[3], "failed")

    def test_a_saved_input_must_match_the_record_of_its_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            runs = root / "target/bolero/runs"
            run = runs / self.target.id / "1"
            (run / "crashes").mkdir(parents=True)
            saved = run / "crashes" / "crash-1"
            saved.write_bytes(b"\x01\x02")
            name = str(saved.relative_to(root))
            record = run / "metadata.json"
            outside = root / "elsewhere"
            outside.write_bytes(b"\x01")
            with mock.patch.object(bolero, "ROOT", root), mock.patch.object(bolero, "RUNS", runs):
                bolero.verify_replay_record(self.target, outside)
                for content, reason in (
                    (None, "cannot be read"),
                    ("{", "cannot be read"),
                    (json.dumps({"target": "another"}), "not a record of"),
                    (json.dumps({"target": self.target.id, "saved_inputs": {}}), "does not hold"),
                    (json.dumps({"target": self.target.id, "saved_inputs": {name: "0" * 64}}), "has digest"),
                ):
                    with self.subTest(reason=reason):
                        if content is None:
                            record.unlink(missing_ok=True)
                        else:
                            record.write_text(content)
                        with self.assertRaisesRegex(bolero.BoleroError, "corrupt replay record: .*" + reason):
                            bolero.verify_replay_record(self.target, saved)
                record.write_text(json.dumps({"target": self.target.id, "saved_inputs": {
                    name: hashlib.sha256(b"\x01\x02").hexdigest()}}))
                bolero.verify_replay_record(self.target, saved)
                with self.assertRaisesRegex(bolero.BoleroError, "was saved by"):
                    bolero.verify_replay_record(self.inventory.targets[1], saved)
                loose = runs / self.target.id / "loose"
                loose.write_bytes(b"\x01")
                with self.assertRaisesRegex(bolero.BoleroError, "not inside a run directory"):
                    bolero.verify_replay_record(self.target, loose)

    def test_replay_stages_exact_bytes_and_disables_random_cases(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temporary = pathlib.Path(directory)
            target = dataclasses.replace(self.target, corpus=temporary / "corpus")
            input_file = temporary / "failure"
            input_file.write_bytes(b"\x42\x00")
            output = subprocess.CompletedProcess(
                [], 101, "test result: FAILED. 0 passed; 1 failed", ""
            )
            with mock.patch.object(bolero, "command", return_value=output) as execute:
                self.assertEqual(bolero.replay(target, input_file), 101)
            staged = list((temporary / "crashes").iterdir())
            self.assertEqual(len(staged), 1)
            self.assertEqual(staged[0].read_bytes(), input_file.read_bytes())
            self.assertEqual(execute.call_args.kwargs["env"]["BOLERO_RANDOM_ITERATIONS"],
                             "0")

    def test_replay_accepts_corpus_only_completion(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temporary = pathlib.Path(directory)
            target = dataclasses.replace(self.target, corpus=temporary / "corpus")
            failure = temporary / "failure"
            failure.write_bytes(b"\x42")
            output = subprocess.CompletedProcess(
                [], 0, "run time: 1ms | corpus inputs: 1\n"
                "test result: ok. 1 passed; 0 failed\n", ""
            )
            with mock.patch.object(bolero, "command", return_value=output):
                self.assertEqual(bolero.replay(target, failure), 0)

    def test_replay_rejects_missing_large_or_unconfirmed_input(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temporary = pathlib.Path(directory)
            target = dataclasses.replace(self.target, corpus=temporary / "corpus")
            missing = temporary / "missing"
            with self.assertRaisesRegex(bolero.BoleroError, "does not exist"):
                bolero.replay(target, missing)
            failure = temporary / "failure"
            failure.write_bytes(b"x" * (target.max_input_bytes + 1))
            with self.assertRaisesRegex(bolero.BoleroError, "exceeds"):
                bolero.replay(target, failure)
            failure.write_bytes(b"\x42")
            for result, message in (
                (subprocess.CompletedProcess([], 101, "unrelated test failed", ""),
                 "outside the selected property"),
                (subprocess.CompletedProcess([], 0, "test result: ok. 1 passed", ""),
                 "saved input was not replayed"),
                (subprocess.CompletedProcess([], 0, "corpus inputs: 1 | rng inputs: 1\n"
                                             "test result: ok. 1 passed", ""),
                 "saved input was not replayed"),
            ):
                with self.subTest(message=message), mock.patch.object(
                    bolero, "command", return_value=result
                ):
                    with self.assertRaisesRegex(bolero.BoleroError, message):
                        bolero.replay(target, failure)

    def test_reduction_requires_a_saved_crash_and_failing_minimum(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temporary = pathlib.Path(directory)
            failure = temporary / "failure"
            with self.assertRaisesRegex(bolero.BoleroError, "does not exist"):
                with mock.patch.object(bolero, "verify_tool"):
                    bolero.reduce_failure(self.inventory, self.target, failure)
            failure.write_bytes(b"x" * (self.target.max_input_bytes + 1))
            with self.assertRaisesRegex(bolero.BoleroError, "exceeds"):
                with mock.patch.object(bolero, "verify_tool"):
                    bolero.reduce_failure(self.inventory, self.target, failure)
            failure.write_bytes(b"\x42\x00")
            run = temporary / "run"
            run.mkdir()
            with (
                mock.patch.object(bolero, "verify_tool"),
                mock.patch.object(bolero, "run_dir", return_value=run),
                mock.patch.object(bolero, "build_instrumented",
                                  return_value=bolero.ROOT / "fake-binary"),
                mock.patch.object(bolero, "run_instrumented") as reduce,
                mock.patch.object(bolero, "metadata"),
            ):
                with self.assertRaisesRegex(bolero.BoleroError, "did not save"):
                    bolero.reduce_failure(self.inventory, self.target, failure)
                self.assertIn("-print_funcs=0", reduce.call_args.args[2])
                self.assertEqual(reduce.call_args.kwargs["timeout"], 1800)

            def minimizing(verified_status: int):
                def run(binary, target, flags, **kwargs):
                    if "-minimize_crash=1" in flags:
                        (run_path / "minimized").write_bytes(b"\x42")
                        return subprocess.CompletedProcess([], 0, "", "")
                    self.assertEqual(flags[0], str(run_path / "minimized"))
                    self.assertIn(f"-artifact_prefix={run_path}/", flags)
                    return subprocess.CompletedProcess([], verified_status, "", "")
                return run

            run_path = run
            with (
                mock.patch.object(bolero, "verify_tool"),
                mock.patch.object(bolero, "run_dir", return_value=run),
                mock.patch.object(bolero, "build_instrumented",
                                  return_value=bolero.ROOT / "fake-binary"),
                mock.patch.object(bolero, "run_instrumented", side_effect=minimizing(0)),
                mock.patch.object(bolero, "replay", return_value=101),
                mock.patch.object(bolero, "metadata") as metadata,
            ):
                with self.assertRaisesRegex(bolero.BoleroError, "no longer fails the instrumented target"):
                    bolero.reduce_failure(self.inventory, self.target, failure)
            self.assertEqual(metadata.call_args.args[3], "failed")
            # A sanitizer finding still fails the instrumented target while ordinary replay passes;
            # an assertion failure fails both. Either way the minimum is kept, with the ordinary result.
            for ordinary, recorded in ((0, "passes"), (101, "fails")):
                with (
                    self.subTest(ordinary=ordinary),
                    mock.patch.object(bolero, "verify_tool"),
                    mock.patch.object(bolero, "run_dir", return_value=run),
                    mock.patch.object(bolero, "build_instrumented",
                                      return_value=bolero.ROOT / "fake-binary"),
                    mock.patch.object(bolero, "run_instrumented", side_effect=minimizing(1)),
                    mock.patch.object(bolero, "replay", return_value=ordinary),
                    mock.patch.object(bolero, "metadata") as metadata,
                    contextlib.redirect_stdout(io.StringIO()),
                ):
                    self.assertEqual(bolero.reduce_failure(self.inventory, self.target, failure),
                                     run / "minimized")
                self.assertEqual(metadata.call_args.args[3], "minimized")
                self.assertEqual(metadata.call_args.args[5], {"ordinary_replay": recorded})


class ShardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.inventory = bolero.load_inventory()
        self.harnesses = len(bolero.harness_estimates(self.inventory))

    def test_shard_numbers_name_one_part_of_a_count(self) -> None:
        self.assertEqual(bolero.Shard.parse("2/3"), bolero.Shard(2, 3))
        self.assertEqual(bolero.Shard(2, 3).describe(), {"number": 2, "count": 3})
        for text in ("3", "a/b", "0/3", "4/3", "1/0", "-1/3", "1/3/4"):
            with self.subTest(text=text), self.assertRaises(bolero.BoleroError):
                bolero.Shard.parse(text)

    def test_every_plan_runs_each_target_once_and_keeps_each_harness_whole(self) -> None:
        identities = [target.id for target in self.inventory.targets]
        for count in range(1, self.harnesses + 1):
            with self.subTest(count=count):
                plan = bolero.shard_plan(self.inventory, count)
                self.assertEqual(len(plan), count)
                self.assertTrue(all(plan))
                selected = [target.id for shard in plan for target in shard]
                self.assertEqual(sorted(selected), sorted(identities))
                owners: dict[bolero.Harness, int] = {}
                for number, shard in enumerate(plan):
                    for target in shard:
                        self.assertEqual(owners.setdefault(target.harness, number), number)
                    # Each shard keeps the inventory's order, so its records list targets stably.
                    self.assertEqual([target.id for target in shard],
                                     [target_id for target_id in identities if target_id in {t.id for t in shard}])
                self.assertEqual(bolero.shard_plan(self.inventory, count), plan)
                self.assertEqual(bolero.plan_digest(bolero.shard_plan(self.inventory, count)), bolero.plan_digest(plan))

    def test_prepared_server_harnesses_lead_separate_shards(self) -> None:
        plan = bolero.shard_plan(self.inventory, 3)
        described = bolero.describe_plan(self.inventory, 3)
        self.assertEqual(described["plan_sha256"], bolero.plan_digest(plan))
        self.assertEqual([shard["prepared_targets"] for shard in described["shards"]],
                         [["registry-archived-models"], ["client-arrow-rows"], []])
        self.assertEqual([shard["targets"] for shard in described["shards"]],
                         [[target.id for target in shard] for shard in plan])
        estimates = [shard["estimated_seconds"] for shard in described["shards"]]
        # Longest-first dealing keeps every shard within one harness of the others.
        largest = max(estimate.seconds for estimate in bolero.harness_estimates(self.inventory))
        self.assertLessEqual(max(estimates) - min(estimates), largest)
        self.assertNotEqual(bolero.plan_digest(plan), bolero.plan_digest(tuple(reversed(plan))))

    def test_a_plan_that_would_leave_a_shard_empty_is_refused(self) -> None:
        with self.assertRaisesRegex(bolero.BoleroError, "would select no target"):
            bolero.shard_plan(self.inventory, self.harnesses + 1)
        with self.assertRaisesRegex(bolero.BoleroError, "positive integer"):
            bolero.shard_plan(self.inventory, 0)

    def test_shard_preparation_builds_only_its_own_prepared_harnesses(self) -> None:
        for number, expected in ((1, ["registry-archived-models"]), (2, ["client-arrow-rows"]), (3, [])):
            with (
                self.subTest(number=number),
                mock.patch.object(bolero, "prepare_target") as prepare,
                contextlib.redirect_stdout(io.StringIO()) as output,
            ):
                bolero.prepare_shard(self.inventory, bolero.Shard(number, 3))
            self.assertEqual([call.args[1].id for call in prepare.call_args_list], expected)
            if not expected:
                self.assertIn("builds no prepared harness", output.getvalue())


class CliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.inventory = bolero.load_inventory()

    def invoke(self, *arguments: str) -> int:
        with (
            mock.patch.object(sys, "argv", ["bolero.py", *arguments]),
            mock.patch.object(bolero, "load_inventory", return_value=self.inventory),
            mock.patch.object(bolero, "discover"),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            return bolero.main()

    def test_list_and_validation_routes(self) -> None:
        self.assertEqual(self.invoke("validate"), 0)
        self.assertEqual(self.invoke("list"), 0)

    def test_preparation_selects_one_current_target_and_rejects_unknown_targets(self) -> None:
        with mock.patch.object(bolero, "prepare_target") as prepare:
            self.assertEqual(self.invoke("prepare", "client-arrow-rows"), 0)
            selected = next(target for target in self.inventory.targets
                            if target.id == "client-arrow-rows")
            prepare.assert_called_once_with(self.inventory, selected)
            with self.assertRaisesRegex(bolero.BoleroError, "unknown Bolero target"):
                self.invoke("prepare", "absent")
    def test_preparation_requires_an_exact_registered_target(self) -> None:
        with mock.patch.object(bolero, "prepare_target") as prepare:
            self.assertEqual(self.invoke("prepare", "models-archived-counts"), 0)
            selected = next(target for target in self.inventory.targets
                            if target.id == "models-archived-counts")
            prepare.assert_called_once_with(self.inventory, selected)
            with self.assertRaisesRegex(bolero.BoleroError, "unknown Bolero target"):
                self.invoke("prepare", "models-archive")

    def test_fuzz_routes_select_targets_and_reject_zero_duration(self) -> None:
        with mock.patch.object(bolero, "fuzz_targets") as fuzz:
            self.assertEqual(self.invoke("fuzz", "plexus-expression", "3"), 0)
            selected = next(target for target in self.inventory.targets
                            if target.id == "plexus-expression")
            self.assertEqual(fuzz.call_args.args[1], (selected,))
            self.assertEqual(fuzz.call_args.args[2], 3)
            self.assertEqual(self.invoke("fuzz-all"), 0)
            self.assertEqual(fuzz.call_args.args[1], self.inventory.targets)
            self.assertEqual(fuzz.call_args.args[2], 30)
            with self.assertRaisesRegex(bolero.BoleroError, "duration"):
                self.invoke("fuzz", "plexus-expression", "0")
            with self.assertRaisesRegex(bolero.BoleroError, "unknown Bolero target"):
                self.invoke("fuzz", "absent", "1")

    def test_replay_reduce_and_qualification_routes(self) -> None:
        with mock.patch.object(bolero, "qualify") as qualify:
            self.assertEqual(self.invoke("qualify"), 0)
            qualify.assert_called_once_with(self.inventory)
        with mock.patch.object(bolero, "replay", return_value=101) as replay:
            self.assertEqual(self.invoke("replay", "plexus-expression", "failure"), 101)
            selected = next(target for target in self.inventory.targets
                            if target.id == "plexus-expression")
            self.assertEqual(replay.call_args.args[0], selected)
        with mock.patch.object(bolero, "reduce_failure") as reduce:
            self.assertEqual(self.invoke("reduce", "plexus-expression", "failure"), 0)
            reduce.assert_called_once()

    def test_planning_and_combining_need_no_compiled_discovery(self) -> None:
        with (
            mock.patch.object(sys, "argv", ["bolero.py", "plan", "3"]),
            mock.patch.object(bolero, "discover") as discover,
            contextlib.redirect_stdout(io.StringIO()) as output,
        ):
            self.assertEqual(bolero.main(), 0)
        discover.assert_not_called()
        self.assertEqual(json.loads(output.getvalue())["count"], 3)
        with (
            mock.patch.object(sys, "argv", ["bolero.py", "combine", "shards", "combined"]),
            mock.patch.object(bolero, "discover") as discover,
            mock.patch.object(bolero.coverage, "combine", return_value=pathlib.Path("combined")) as combine,
            contextlib.redirect_stdout(io.StringIO()),
        ):
            self.assertEqual(bolero.main(), 0)
        discover.assert_not_called()
        root, inventory, evidence, output_path, plan_for, digest = combine.call_args.args
        self.assertEqual((root, evidence, output_path), (bolero.ROOT, pathlib.Path("shards"), pathlib.Path("combined")))
        self.assertEqual(plan_for(3), bolero.shard_plan(inventory, 3))
        self.assertIs(digest, bolero.plan_digest)

    def test_shard_routes_prepare_and_run_exactly_their_part(self) -> None:
        with mock.patch.object(bolero, "prepare_shard") as prepare:
            self.assertEqual(self.invoke("prepare-shard", "2/3"), 0)
            prepare.assert_called_once_with(self.inventory, bolero.Shard(2, 3))
        with mock.patch.object(bolero, "fuzz_targets") as fuzz:
            self.assertEqual(self.invoke("fuzz-all", "5", "--shard", "2/3"), 0)
            self.assertEqual(fuzz.call_args.args, (self.inventory, bolero.shard_plan(self.inventory, 3)[1],
                                                   5, bolero.Shard(2, 3)))
            self.assertEqual(self.invoke("fuzz-all"), 0)
            self.assertEqual(fuzz.call_args.args, (self.inventory, self.inventory.targets, 30, bolero.Shard(1, 1)))
            with self.assertRaisesRegex(bolero.BoleroError, "NUMBER/COUNT"):
                self.invoke("fuzz-all", "5", "--shard", "two")

    def test_sanitizer_replay_route_and_corrupt_records(self) -> None:
        with mock.patch.object(bolero, "replay_sanitizer", return_value=1) as replay_sanitizer:
            self.assertEqual(self.invoke("replay-sanitizer", "plexus-expression", "failure"), 1)
            self.assertEqual(replay_sanitizer.call_args.args[1].id, "plexus-expression")
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            runs = root / "target/bolero/runs"
            saved = runs / "plexus-expression/1/crashes/crash-1"
            saved.parent.mkdir(parents=True)
            saved.write_bytes(b"\x01")
            with (
                mock.patch.object(bolero, "ROOT", root),
                mock.patch.object(bolero, "RUNS", runs),
                mock.patch.object(bolero, "replay") as replay,
            ):
                with self.assertRaisesRegex(bolero.BoleroError, "corrupt replay record"):
                    self.invoke("replay", "plexus-expression", str(saved))
                replay.assert_not_called()


if __name__ == "__main__":
    unittest.main()
