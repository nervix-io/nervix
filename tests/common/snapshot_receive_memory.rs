//! The memory reservation held by the public snapshot receive probe.
//!
//! Layer: test harness.
//! - **Owns.** The probe's bulk reservation and its remaining receive allowance.
//! - **Depends on.** The executor's nonblocking admission and owned reservations.
//! - **Must not know.** Snapshot encoding, node lifecycle or transport scheduling.

use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_execution::{AdmissionError, Executor, MemoryClass, Reservation};

pub(crate) fn try_hold_snapshot_receive_memory(
    executor: &Executor,
    room: u64,
) -> Result<Reservation, Report<AdmissionError>> {
    let memory = executor.memory(MemoryClass::Bulk);
    let held = memory
        .capacity_bytes
        .checked_sub(room)
        .assured("the probe's allowance fits the node's bulk capacity");
    executor.try_reserve(MemoryClass::Bulk, held)
}
