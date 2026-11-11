//! The pure execution-capacity decisions and the concrete executor they produce.
//!
//! Layer: test harness.
//! - **Owns.** CPU-share, queue, operation and complete memory-accounting regression evidence.
//! - **Depends on.** Production execution setting declarations and the executor.
//! - **Must not know.** Detection, command-line parsing or graph execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests capability-derived execution decisions")
)]

use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};

use ubyte::ByteUnit;

use crate::{ExecutionConfigError, ExecutionOverrides, ExecutionSettings, Executor, MemoryClass};

fn resolve(overrides: ExecutionOverrides, cpus: u64) -> ExecutionSettings {
    ExecutionSettings::resolve(
        overrides,
        NonZeroU64::new(cpus),
        NonZeroU64::new(ByteUnit::Gibibyte(1).as_u64()),
    )
    .expect("the bounded configuration fits one GiB")
}

#[test]
fn cpu_classes_share_the_cpu_claim_and_keep_independent_minimum_service() {
    for cpus in [1, 2, 3, 4, 5, 8, 16, 256] {
        let config = resolve(ExecutionOverrides::default(), cpus).config();
        let workers = config.workers;
        let total = [
            workers.control_cpu,
            workers.credentials_cpu,
            workers.data_cpu,
            workers.extension_cpu,
            workers.bulk_cpu,
        ]
        .into_iter()
        .try_fold(0usize, |total, workers| total.checked_add(workers.get()))
        .expect("test CPU sums are bounded by 256");
        assert_eq!(
            u64::try_from(total).expect("test CPU sum fits"),
            cpus.max(5)
        );
        assert_eq!(workers.control_cpu.get(), 1);
        assert_eq!(workers.credentials_cpu.get(), 1);
        assert!(workers.data_cpu.get() > 0);
        assert!(workers.extension_cpu.get() > 0);
        assert!(workers.bulk_cpu.get() > 0);
    }
}

#[test]
fn explicit_cpu_and_queue_capacities_are_used_and_remaining_shares_are_derived() {
    let config = resolve(
        ExecutionOverrides {
            data_cpu_workers: NonZeroUsize::new(3),
            data_cpu_pending_jobs: NonZeroUsize::new(7),
            ..Default::default()
        },
        8,
    )
    .config();
    assert_eq!(config.workers.data_cpu.get(), 3);
    assert_eq!(config.workers.extension_cpu.get(), 2);
    assert_eq!(config.workers.bulk_cpu.get(), 1);
    let executor = Executor::new(config).expect("all operation bounds fit");
    let snapshot = executor.snapshot();
    let (_, data) = snapshot
        .worker_classes()
        .into_iter()
        .find(|(class, _)| *class == crate::WorkerClassName::Cpu(crate::CpuClass::Data))
        .expect("the data worker class is reported");
    assert_eq!((data.workers, data.queue_capacity), (3, 7));
    assert_eq!(executor.snapshot().data_cpu.queue_capacity, 7);
    assert_eq!(executor.snapshot().extension_cpu.queue_capacity, 128);
}

#[test]
fn explicit_cpu_overallocation_is_a_typed_refusal() {
    let error = ExecutionSettings::resolve(
        ExecutionOverrides {
            data_cpu_workers: NonZeroUsize::new(7),
            ..Default::default()
        },
        NonZeroU64::new(8),
        None,
    )
    .expect_err("the independent classes cannot fit the CPU claim");
    assert_eq!(
        error.current_context(),
        &ExecutionConfigError::CpuBudgetExceeded {
            workers: 11,
            cpus: 8
        }
    );
}

#[test]
fn one_gibibyte_holds_every_independent_pool_and_the_current_credential_profile() {
    let config = resolve(ExecutionOverrides::default(), 8).config();
    assert_eq!(config.budgets.relay, ByteUnit::Mebibyte(64));
    assert_eq!(config.budgets.state_read, config.budgets.relay);
    assert_eq!(config.budgets.state_read_cache, config.budgets.relay);
    assert_eq!(config.budgets.credentials, ByteUnit::Mebibyte(19));
    assert_eq!(config.limits.credential_working_bytes, ByteUnit::Byte(64));
    assert_eq!(config.budgets.total(), Some(ByteUnit::Mebibyte(555)));
    let pair = config
        .limits
        .relay_operation_bytes()
        .expect("bounded operation")
        .checked_mul(2)
        .expect("bounded pair");
    assert!(pair <= config.budgets.relay.as_u64());
    let executor = Executor::new(config).expect("one GiB has room for all declared operations");
    let reported_total = executor
        .snapshot()
        .memory_classes()
        .into_iter()
        .try_fold(0u64, |sum, (_, pool)| sum.checked_add(pool.capacity_bytes))
        .expect("the configured total fits u64");
    assert_eq!(Some(ByteUnit::Byte(reported_total)), config.budgets.total());
    assert_eq!(
        executor.memory(MemoryClass::StateRead).capacity_bytes,
        config.budgets.state_read.as_u64()
    );
    assert_eq!(
        executor.memory(MemoryClass::StateReadCache).capacity_bytes,
        config.budgets.state_read_cache.as_u64()
    );
}

#[test]
fn all_configured_pools_count_toward_the_memory_claim() {
    let error = ExecutionSettings::resolve(
        ExecutionOverrides {
            state_read_cache_memory_bytes: Some(ByteUnit::Gibibyte(1)),
            ..Default::default()
        },
        NonZeroU64::new(8),
        NonZeroU64::new(ByteUnit::Gibibyte(1).as_u64()),
    )
    .expect_err("the independent cache pool counts too");
    assert!(matches!(
        error.current_context(),
        ExecutionConfigError::MemoryBudgetExceeded { .. }
    ));
}

#[test]
fn a_replication_window_that_cannot_leave_room_for_its_answer_is_refused() {
    let error = ExecutionSettings::resolve(
        ExecutionOverrides {
            resident_replication_batches: NonZeroU32::new(u32::MAX),
            ..Default::default()
        },
        NonZeroU64::new(8),
        None,
    )
    .expect_err("the window cannot exhaust the command budget");
    assert!(matches!(
        error.current_context(),
        ExecutionConfigError::BudgetBelowOperation {
            class: "commands",
            ..
        }
    ));
}

#[test]
fn an_explicit_operation_limit_is_never_silently_shrunk_to_fit_a_budget() {
    let error = ExecutionSettings::resolve(
        ExecutionOverrides {
            relay_encoded_bytes: Some(ByteUnit::Mebibyte(64)),
            ..Default::default()
        },
        NonZeroU64::new(8),
        NonZeroU64::new(ByteUnit::Gibibyte(1).as_u64()),
    )
    .expect_err("the explicit operation needs more relay memory");
    assert!(matches!(
        error.current_context(),
        ExecutionConfigError::BudgetBelowOperation { class: "relay", .. }
    ));
}

#[test]
fn every_memory_fraction_and_operation_default_is_resolved_before_construction() {
    let config = resolve(ExecutionOverrides::default(), 8).config();
    for (actual, mebibytes) in [
        (config.budgets.management, 8),
        (config.budgets.commands, 32),
        (config.budgets.relay, 64),
        (config.budgets.state_read, 64),
        (config.budgets.state_read_cache, 64),
        (config.budgets.bulk, 32),
        (config.budgets.bulk_window, 16),
        (config.budgets.restore_metadata, 128),
        (config.budgets.credentials, 19),
    ] {
        assert_eq!(actual, ByteUnit::Mebibyte(mebibytes));
    }
    assert_eq!(
        config.limits.relay_encoded_bytes.as_u64(),
        ByteUnit::Mebibyte(64).as_u64() / 6
    );
    assert_eq!(
        config.limits.relay_decoded_bytes,
        config.limits.relay_encoded_bytes
    );
    assert_eq!(
        config.limits.relay_scratch_bytes.as_u64(),
        ByteUnit::Mebibyte(64).as_u64() / 12
    );
    assert_eq!(config.limits.bulk_chunk_bytes, ByteUnit::Kibibyte(64));
    assert_eq!(config.limits.resident_replication_batches.get(), 4);
    let largest = ExecutionSettings::resolve(
        ExecutionOverrides::default(),
        NonZeroU64::new(256),
        NonZeroU64::new(u64::MAX),
    )
    .expect("derived pools cap at their permit representation")
    .config();
    assert_eq!(
        largest.budgets.restore_metadata.as_u64(),
        u64::from(u32::MAX)
    );
    assert_eq!(largest.workers.pending_jobs.data_cpu.get(), 1024);
}

#[test]
fn unknown_capacity_has_explicit_conservative_defaults_without_a_measured_claim() {
    let config = ExecutionSettings::resolve(ExecutionOverrides::default(), None, None)
        .expect("unknown capacity still has valid declared defaults")
        .config();
    assert_eq!(config.workers.data_cpu.get(), 1);
    assert_eq!(config.workers.filesystem_storage.get(), 2);
    assert_eq!(config.budgets.total(), Some(ByteUnit::Mebibyte(2851)));
    assert_eq!(config.limits.relay_encoded_bytes, ByteUnit::Mebibyte(32));
}

#[test]
fn scalar_refusals_precede_capacity_detection() {
    for (overrides, expected) in [
        (
            ExecutionOverrides {
                management_memory_bytes: Some(ByteUnit::Byte(0)),
                ..Default::default()
            },
            "execution-management-memory-bytes",
        ),
        (
            ExecutionOverrides {
                state_read_cache_memory_bytes: Some(ByteUnit::Gibibyte(4)),
                ..Default::default()
            },
            "execution-state-read-cache-memory-bytes",
        ),
        (
            ExecutionOverrides {
                bulk_cpu_pending_jobs: NonZeroUsize::new(usize::MAX),
                ..Default::default()
            },
            "execution-bulk-cpu-pending-jobs",
        ),
    ] {
        let error = overrides
            .validate()
            .expect_err("invalid explicit scalar cannot reach detection");
        assert!(
            error.current_context().to_string().contains(expected),
            "{error:?}"
        );
    }
}
