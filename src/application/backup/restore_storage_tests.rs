//! Tests of the bulk admission the restore checkpoint maintenance sweep takes for its working set.

use futures_util::FutureExt as _;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{ExecutionConfig, Executor, MemoryBudgets, MemoryClass, OperationLimits};
use nervix_recovery::Discarded as _;
use ubyte::ByteUnit;

use crate::{
    application::test_fixtures::{TestService, build_test_service_with_executor},
    runtime::RESTORE_STATE_WORKING_BYTES,
};

/// What a materialized snapshot fetch charges the bulk class for its encoded request, and again
/// for the envelope that frames it.
const REQUEST_CHARGE: u64 = 4 * 1024;

/// A sweep whose working set does not fit leaves the bulk class to the work already holding it.
/// A place in the class's queue takes every byte the class releases until it is satisfied, so a
/// fetch that holds part of the free room while it charges its next allocation could never finish:
/// that charge would wait behind the sweep, and the sweep for the room the fetch holds.
#[nervix_primitives::test]
async fn a_sweep_without_room_for_its_working_set_takes_no_place_in_the_bulk_queue() {
    let executor = Executor::default();
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service_with_executor(false, executor.clone()).await;
    let capacity = executor.memory(MemoryClass::Bulk).capacity_bytes;
    let contended = capacity
        .checked_sub(RESTORE_STATE_WORKING_BYTES)
        .assured("the default bulk budget holds the restore working set");
    // Other bulk work leaves exactly the sweep's working set free, and a fetch takes part of it for
    // its encoded request before it charges the envelope.
    let contention = executor
        .try_reserve(MemoryClass::Bulk, contended)
        .assured("the test occupies the untouched bulk budget");
    let request = executor
        .try_reserve(MemoryClass::Bulk, REQUEST_CHARGE)
        .assured("the encoded request fits the room the contention leaves");

    // The sweep stays alive across the envelope's charge, as the node's maintenance task keeps it.
    let sweep = service.sweep_restore_checkpoint_staging();
    let mut sweep = std::pin::pin!(sweep);
    let swept = sweep.as_mut().now_or_never();
    let envelope = executor
        .reserve(MemoryClass::Bulk, REQUEST_CHARGE)
        .now_or_never();
    assert!(
        envelope.is_some(),
        "the fetch's envelope must be charged from the room left beside its request instead of \
         waiting behind a sweep queued for the whole working set"
    );
    assert!(
        swept.is_some(),
        "a sweep without room for its working set must end rather than wait for it"
    );
    let envelope = envelope
        .verified("the assertion above found the envelope charged")
        .assured("the free room holds the envelope");
    let held = contended
        .checked_add(request.bytes())
        .assured("the test's charges stay within the bulk budget");
    let held = held
        .checked_add(envelope.bytes())
        .assured("the test's charges stay within the bulk budget");
    assert_eq!(
        executor.memory(MemoryClass::Bulk).reserved_bytes,
        held,
        "a deferred sweep holds no part of the bulk class"
    );
    assert_eq!(
        completed_sweeps(&service),
        0,
        "a deferred sweep reclaims nothing"
    );
    drop((envelope, request, contention));
    assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, 0);
    std::fs::remove_dir_all(&path).discarded("the throwaway test database may already be gone");
}

/// A sweep whose working set fits completes its reclamation and returns the working set.
#[nervix_primitives::test]
async fn a_sweep_with_room_reclaims_under_its_working_set_and_returns_it() {
    let executor = Executor::default();
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service_with_executor(false, executor.clone()).await;
    let before = executor.memory(MemoryClass::Bulk);

    service.sweep_restore_checkpoint_staging().await;

    assert_eq!(completed_sweeps(&service), 1);
    let after = executor.memory(MemoryClass::Bulk);
    assert!(
        after.granted > before.granted,
        "the sweep charged its working set"
    );
    assert_eq!(after.refused, before.refused);
    assert_eq!(after.reserved_bytes, before.reserved_bytes);
    std::fs::remove_dir_all(&path).discarded("the throwaway test database may already be gone");
}

/// A working set larger than the whole bulk class is refused at once, as the class refuses every
/// charge it could never hold, rather than deferred to rounds that no room could ever admit.
#[nervix_primitives::test]
async fn a_sweep_whose_working_set_exceeds_the_bulk_budget_is_refused() {
    let executor = Executor::new(ExecutionConfig {
        budgets: MemoryBudgets {
            bulk: ByteUnit::Mebibyte(1),
            ..MemoryBudgets::default()
        },
        limits: OperationLimits {
            snapshot_section_bytes: ByteUnit::Kibibyte(64),
            ..OperationLimits::default()
        },
        ..ExecutionConfig::default()
    })
    .assured("a bulk budget above two small sections, their header and a chunk is consistent");
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service_with_executor(false, executor.clone()).await;
    let before = executor.memory(MemoryClass::Bulk);
    assert!(
        before.capacity_bytes < RESTORE_STATE_WORKING_BYTES,
        "the test's bulk class is smaller than the sweep's working set"
    );

    service.sweep_restore_checkpoint_staging().await;

    let after = executor.memory(MemoryClass::Bulk);
    let refused = before
        .refused
        .checked_add(1)
        .assured("a node that just started has refused few bulk charges");
    assert_eq!(
        after.refused, refused,
        "the class refuses a working set it could never hold"
    );
    assert_eq!(after.granted, before.granted);
    assert_eq!(after.reserved_bytes, before.reserved_bytes);
    assert_eq!(
        completed_sweeps(&service),
        0,
        "a refused sweep reclaims nothing"
    );
    std::fs::remove_dir_all(&path).discarded("the throwaway test database may already be gone");
}

/// The maintenance sweeps the service's node reports as completed.
fn completed_sweeps(service: &crate::application::session_service::SessionServiceImpl) -> u64 {
    let exposition = service.inner.runtime.metrics().prometheus_text();
    let sample = exposition
        .lines()
        .find_map(|line| line.strip_prefix("nervix_restore_staging_sweeps_total "))
        .assured("every node reports its completed restore maintenance sweeps");
    sample.parse().assured("a counter sample is a whole number")
}
