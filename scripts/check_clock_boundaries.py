#!/usr/bin/env python3
"""Reject clock APIs and imports that cross their owning architecture boundary."""

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def product_source(path: Path) -> str:
    source = path.read_text(encoding="utf-8")
    for marker in ("#[cfg(test)]\nmod tests", "#[cfg(test)]\r\nmod tests"):
        if marker in source:
            return source.split(marker, 1)[0]
    return source


def reject(violations: list[str], path: Path, source: str, needle: str, reason: str) -> None:
    if needle not in source:
        return
    relative = path.relative_to(ROOT)
    for line_number, line in enumerate(source.splitlines(), start=1):
        if needle in line:
            violations.append(f"{relative}:{line_number}: {reason}: {line.strip()}")


def main() -> int:
    violations: list[str] = []

    wall_time_owners = {
        ROOT / "crates/interconnect/src/connection.rs",
        ROOT / "crates/models/src/timestamp.rs",
        ROOT / "src/application/domain_clock.rs",
        ROOT / "src/cluster.rs",
        ROOT / "src/metrics.rs",
        ROOT / "src/runtime/physical_time.rs",
        ROOT / "src/runtime_schema/syslog.rs",
    }
    product_roots = [ROOT / "src", ROOT / "crates"]
    excluded_components = {"benchmark", "test-environment", "tests"}
    wall_time_needles = (
        "Timestamp::now(",
        "Utc::now(",
        "SystemTime::now(",
        "OffsetDateTime::now_utc(",
        "Local::now(",
    )
    for source_root in product_roots:
        for path in sorted(source_root.rglob("*.rs")):
            relative = path.relative_to(ROOT)
            if excluded_components.intersection(relative.parts):
                continue
            if path.name.startswith("test_") or path.name.endswith("_tests.rs"):
                continue
            if path in wall_time_owners:
                continue
            source = product_source(path)
            for needle in wall_time_needles:
                reject(
                    violations,
                    path,
                    source,
                    needle,
                    "direct actual-UTC reads are limited to declared operational and external owners",
                )

    engine_sources = [
        *sorted((ROOT / "crates/nervix-vm/src").rglob("*.rs")),
        *sorted((ROOT / "crates/nervix-roto/src").rglob("*.rs")),
        *sorted((ROOT / "crates/nervix-wasm/src").rglob("*.rs")),
    ]
    for path in engine_sources:
        source = path.read_text(encoding="utf-8")
        for needle in ("Timestamp::now(", "Utc::now(", "SystemTime::now("):
            reject(
                violations,
                path,
                source,
                needle,
                "an engine must receive logical time from its caller",
            )

    runtime_root = ROOT / "src/runtime"
    actual_utc_owner = runtime_root / "physical_time.rs"
    actual_utc_consumers = {
        runtime_root / "domain_clock.rs",
        runtime_root / "endpoint.rs",
        runtime_root / "emitters/mod.rs",
        runtime_root / "emitters/iceberg.rs",
        runtime_root / "emitters/otel.rs",
        runtime_root / "emitters/sentry.rs",
        runtime_root / "ingestors/http.rs",
        runtime_root / "ingestors/kafka.rs",
        runtime_root / "ingestors/mqtt.rs",
        runtime_root / "ingestors/nats.rs",
        runtime_root / "ingestors/prometheus.rs",
        runtime_root / "ingestors/pulsar.rs",
        runtime_root / "ingestors/rabbitmq.rs",
        runtime_root / "ingestors/redis_pubsub.rs",
        runtime_root / "ingestors/sqs.rs",
        runtime_root / "ingestors/syslog.rs",
        runtime_root / "ingestors/websockets.rs",
        runtime_root / "ingestors/zeromq.rs",
    }
    physical_capability_owners = {
        runtime_root / "branch_buffering.rs",
        runtime_root / "domain_clock.rs",
        runtime_root / "emitters/mod.rs",
        actual_utc_owner,
    }
    physical_time_source = product_source(actual_utc_owner)
    if "pub(super) const fn new() -> Self" not in physical_time_source:
        violations.append(
            "src/runtime/physical_time.rs: physical deadline construction must remain private "
            "to its declared runtime owners"
        )

    ownership_contract_sources = {
        ROOT / "crates/models/src/domain_clock.rs",
        ROOT / "crates/models/src/statement.rs",
        ROOT / "crates/nervix-roto/src/lib.rs",
        ROOT / "crates/nervix-vm/src/runtime.rs",
        ROOT / "crates/nervix-wasm/src/lib.rs",
        ROOT / "crates/nspl/src/domain/mod.rs",
        ROOT / "crates/nspl/src/generator/mod.rs",
        ROOT / "crates/nspl/src/ingestor/mod.rs",
        ROOT / "crates/nspl/src/parser_support.rs",
        ROOT / "crates/nspl/src/statement.rs",
        ROOT / "src/application/domain_clock.rs",
        ROOT / "src/runtime/domain_clock.rs",
        ROOT / "src/runtime/ingestion_time.rs",
        ROOT / "src/runtime/ingestor_quiesce.rs",
        ROOT / "src/runtime/physical_time.rs",
        ROOT / "src/runtime/relay_interaction.rs",
        ROOT / "src/runtime_schema/syslog.rs",
        *actual_utc_consumers,
    }
    for path in sorted(ownership_contract_sources):
        header = "\n".join(path.read_text(encoding="utf-8").splitlines()[:20])
        required_parts = ("//!", "Layer:", "- **Owns.**", "- **Depends on.**", "- **Must not know.**")
        if not header.startswith("//!") or any(part not in header for part in required_parts):
            relative = path.relative_to(ROOT)
            violations.append(f"{relative}: missing the required layer ownership contract")

    for path in sorted(runtime_root.rglob("*.rs")):
        source = product_source(path)
        if path != actual_utc_owner:
            for needle in ("Timestamp::now(", "Utc::now(", "SystemTime::now("):
                reject(
                    violations,
                    path,
                    source,
                    needle,
                    "actual UTC is owned by runtime/physical_time.rs",
                )
        reject(
            violations,
            path,
            source,
            "current_timestamp(",
            "data-plane code must name the actual-UTC boundary or use a bound logical clock",
        )
        if path not in actual_utc_consumers and path != actual_utc_owner:
            reject(
                violations,
                path,
                source,
                "actual_utc_now",
                "actual UTC may enter only a declared projection or external-observation owner",
            )
        if path not in physical_capability_owners:
            reject(
                violations,
                path,
                source,
                "PhysicalDeadlineCapability::new",
                "physical deadline construction is limited to declared operational owners",
            )

    vm_runtime = ROOT / "crates/nervix-vm/src/runtime.rs"
    vm_source = product_source(vm_runtime)
    for needle in ("fn inject(", "fn inject_with_errors("):
        reject(
            violations,
            vm_runtime,
            vm_source,
            needle,
            "function injection requires an explicit execution context",
        )

    wasm_host = ROOT / "crates/nervix-wasm/src/lib.rs"
    wasm_source = product_source(wasm_host)
    for needle in (
        "pub trait DomainClock",
        "pub struct FixedDomainClock",
        "pub async fn init(",
        "pub async fn current_domain_time(",
        "pub async fn process_batch(",
        "pub async fn process_envelope(",
        "pub async fn on_timeout(",
        "pub async fn flush(",
        "pub async fn save_state(",
        "pub async fn load_state(",
        "pub async fn reset_state(",
    ):
        reject(
            violations,
            wasm_host,
            wasm_source,
            needle,
            "every public WASM guest call requires WasmExecutionContext",
        )

    wasm_guest_operations = (
        "instantiate_branch",
        "instantiate_branch_with_emitter",
        "instantiate_branch_inner",
        "init",
        "init_in_context",
        "current_domain_time",
        "current_domain_time_in_context",
        "process_batch_in_context",
        "process_envelope",
        "process_envelope_in_context",
        "on_timeout",
        "on_timeout_in_context",
        "flush",
        "flush_in_context",
        "save_state",
        "save_state_in_context",
        "load_state",
        "load_state_in_context",
        "reset_state",
        "reset_state_in_context",
    )
    for operation in wasm_guest_operations:
        signature = re.compile(
            rf"(?:pub\s+)?async\s+fn\s+{operation}\s*\((.*?)\)\s*->",
            re.DOTALL,
        )
        for match in signature.finditer(wasm_source):
            if "WasmExecutionContext" in match.group(1):
                continue
            line_number = wasm_source.count("\n", 0, match.start()) + 1
            violations.append(
                f"{wasm_host.relative_to(ROOT)}:{line_number}: "
                f"WASM guest operation '{operation}' requires no explicit execution context"
            )

    if violations:
        print("clock architecture boundary violations:")
        for violation in violations:
            print(f"  {violation}")
        return 1
    print("clock architecture boundaries are valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
