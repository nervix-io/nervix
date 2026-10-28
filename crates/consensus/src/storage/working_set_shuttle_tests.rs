//! Scheduling checks of how consensus storage admits its Commands and Bulk working sets.
//!
//! Layer: test harness.
//! - **Owns.** The invariant that a working set waiting for the store's turn leaves the rest of the
//!   memory class to its other work, and that the next working set never waits in the class for
//!   bytes the previous one still holds.
//! - **Depends on.** The production working-set admission, executor and Shuttle runner.
//! - **Must not know.** Fjall I/O, peer transport or application lifecycle policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "explores the production admission of consensus storage working sets"
    )
)]

use meticulous::ResultExt as _;
use nervix_execution::Executor;
use nervix_model_harness::shuttle::check_random_and_pct;
use nervix_primitives::sync::Arc;

use super::{StorageCharge, WorkingSetAdmission};

/// What a Kafka source's control call charges the Commands class.
const SOURCE_CONTROL_BYTES: u64 = 176;

#[test]
fn shuttle_a_working_set_waiting_for_its_turn_leaves_room_for_source_control() {
    check_working_set_progress(StorageCharge::Commands, SOURCE_CONTROL_BYTES);
}

#[test]
fn shuttle_a_bulk_working_set_waiting_for_its_turn_leaves_room_for_snapshot_progress() {
    check_working_set_progress(StorageCharge::Bulk, 4096);
}

fn check_working_set_progress(charge: StorageCharge, progress_bytes: u64) {
    check_random_and_pct(move || {
        nervix_primitives::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .assured("the modeled runtime needs no external resources")
            .block_on(async {
                let executor = Executor::default();
                let admission = Arc::new(
                    WorkingSetAdmission::new(&executor)
                        .assured("the default executor holds the storage working set"),
                );
                let held = admission
                    .admit(&executor, charge)
                    .await
                    .assured("the first working set fits the untouched class");

                let waiting_admission = Arc::clone(&admission);
                let waiting_executor = executor.clone();
                let waiting = nervix_primitives::task::spawn(async move {
                    waiting_admission
                        .admit(&waiting_executor, charge)
                        .await
                        .assured("the second working set is admitted once the first releases")
                });
                let control_executor = executor.clone();
                let control = nervix_primitives::task::spawn(async move {
                    control_executor
                        .try_reserve(charge.memory_class(), progress_bytes)
                        .assured(
                            "a working set waiting for its turn leaves room for the class's other \
                             work",
                        )
                });
                nervix_primitives::task::yield_now().await;
                // The held working set ends while the other two may still be running. Its bytes
                // are back before its turn passes, so the next working set is admitted without
                // waiting in the class, and the control call finds room at every point.
                drop(held);
                let next = waiting
                    .await
                    .assured("the waiting admission finishes once the held working set ends");
                let control = control
                    .await
                    .assured("the control call finishes without waiting");
                drop(control);
                drop(next);
                assert_eq!(executor.memory(charge.memory_class()).reserved_bytes, 0);
            });
    });
}
