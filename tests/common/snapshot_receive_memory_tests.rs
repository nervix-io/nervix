//! Controlled reservation-lifetime regressions for the snapshot probe's actual memory owner.
//!
//! Layer: test harness.
//! - **Owns.** Fixed receive room across earlier and later reservation releases.
//! - **Depends on.** The shared probe helper and the real executor's memory admission.
//! - **Must not know.** Scenario scheduling, checkpoint representations or server maps.

use meticulous::ResultExt as _;
use nervix_execution::{AdmissionError, Executor, MemoryClass};

use crate::snapshot_receive_memory::try_hold_snapshot_receive_memory;

const ROOM: u64 = 4096;

#[nervix_primitives::test]
async fn a_prior_charge_must_finish_before_the_probe_can_hold_its_fixed_share() {
    let executor = Executor::default();
    let capacity = executor.memory(MemoryClass::Bulk).capacity_bytes;
    let prior = executor
        .try_reserve(MemoryClass::Bulk, 8192)
        .assured("the fixture's prior request fits the budget");
    let refusal = match try_hold_snapshot_receive_memory(&executor, ROOM) {
        Err(refusal) => refusal,
        Ok(held) => {
            drop(prior);
            let free = capacity - executor.memory(MemoryClass::Bulk).reserved_bytes;
            panic!(
                "the probe held {} bytes rather than its {}-byte fixed share; releasing the prior \
                 charge opened {free} bytes of room instead of {ROOM}",
                held.bytes(),
                capacity - ROOM
            );
        }
    };
    assert!(matches!(
        refusal.current_context(),
        AdmissionError::BudgetExhausted { .. }
    ));
    assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, 8192);
    drop(prior);
    let held = try_hold_snapshot_receive_memory(&executor, ROOM)
        .assured("releasing the prior request admits the whole fixed share");
    assert_eq!(held.bytes(), capacity - ROOM);
    assert_eq!(
        executor.memory(MemoryClass::Bulk).reserved_bytes,
        capacity - ROOM
    );
    drop(held);
    assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, 0);
}

#[nervix_primitives::test]
async fn releasing_another_request_never_increases_the_probes_free_room() {
    let executor = Executor::default();
    let capacity = executor.memory(MemoryClass::Bulk).capacity_bytes;
    let held = try_hold_snapshot_receive_memory(&executor, ROOM)
        .assured("an idle executor admits the probe's hold");
    let request = executor
        .try_reserve(MemoryClass::Bulk, ROOM)
        .assured("one request can occupy the remaining room");
    assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, capacity);
    drop(request);
    assert_eq!(
        executor.memory(MemoryClass::Bulk).reserved_bytes,
        capacity - ROOM
    );
    let Err(refusal) = executor.try_reserve(MemoryClass::Bulk, ROOM + 1) else {
        panic!("the fixed hold must refuse a larger request after the other release");
    };
    assert!(matches!(
        refusal.current_context(),
        AdmissionError::BudgetExhausted { .. }
    ));
    drop(held);
    assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, 0);
}
