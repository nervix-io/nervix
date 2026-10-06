use std::{
    io::Write as _,
    num::{NonZeroU32, NonZeroUsize},
};

use meticulous::OptionExt as _;
use ubyte::ByteUnit;

use crate::{
    AdmissionError, BudgetedBuffer, ExecutionConfig, ExecutionConfigError, Executor, MemoryBudgets,
    MemoryClass, OperationLimits, WorkerCounts,
};

fn one() -> NonZeroUsize {
    NonZeroUsize::MIN
}

fn small_executor() -> Executor {
    Executor::new(ExecutionConfig {
        workers: WorkerCounts {
            control_cpu: one(),
            credentials_cpu: one(),
            data_cpu: one(),
            extension_cpu: one(),
            bulk_cpu: one(),
            consensus_storage: one(),
            filesystem_storage: one(),
            pending_jobs: one(),
        },
        budgets: MemoryBudgets::default(),
        limits: OperationLimits::default(),
    })
    .expect("the default budgets hold the default operation limits")
}

/// One ordered consensus worker with a wait queue deep enough to hold every job a test submits
/// before the first one is allowed to finish. `small_executor` deliberately allows one waiter, so
/// it cannot express a queue.
#[cfg(feature = "shuttle")]
fn queued_executor(pending_jobs: usize) -> Executor {
    Executor::new(ExecutionConfig {
        workers: WorkerCounts {
            pending_jobs: NonZeroUsize::new(pending_jobs).expect("a test queue holds at least one"),
            ..WorkerCounts {
                control_cpu: one(),
                credentials_cpu: one(),
                data_cpu: one(),
                extension_cpu: one(),
                bulk_cpu: one(),
                consensus_storage: one(),
                filesystem_storage: one(),
                pending_jobs: one(),
            }
        },
        budgets: MemoryBudgets::default(),
        limits: OperationLimits::default(),
    })
    .expect("the default budgets hold the default operation limits")
}

#[nervix_primitives::test]
async fn default_limits_validate_together() {
    let executor = Executor::new(ExecutionConfig::default()).expect("defaults are consistent");
    let snapshot = executor.snapshot();
    assert_eq!(
        snapshot.relay_memory.capacity_bytes,
        ByteUnit::Mebibyte(192).as_u64()
    );
    assert_eq!(snapshot.control_cpu.workers, 1);
    assert_eq!(snapshot.consensus_storage.workers, 1);
    assert_eq!(snapshot.filesystem_storage.workers, 2);
}

#[nervix_primitives::test]
async fn retained_restore_metadata_is_admitted_independently_of_bulk_buffers() {
    use meticulous::ResultExt as _;

    let executor = small_executor();
    let snapshot = executor.snapshot();
    assert_eq!(
        snapshot.bulk_memory.capacity_bytes,
        ByteUnit::Mebibyte(32).as_u64()
    );
    assert_eq!(
        snapshot.restore_metadata_memory.capacity_bytes,
        ByteUnit::Gibibyte(2).as_u64()
    );
    let bulk = executor
        .try_reserve(MemoryClass::Bulk, snapshot.bulk_memory.capacity_bytes)
        .assured("the complete bulk class is available");
    let metadata = executor
        .try_reserve(
            MemoryClass::RestoreMetadata,
            snapshot.restore_metadata_memory.capacity_bytes,
        )
        .assured("retained metadata has independent admission");
    let error = executor
        .try_reserve(MemoryClass::RestoreMetadata, 1)
        .err()
        .assured("the occupied metadata class refuses more memory");
    assert_eq!(
        *error.current_context(),
        AdmissionError::BudgetExhausted {
            class: "restore_metadata",
            requested: 1
        }
    );
    assert_eq!(MemoryClass::RestoreMetadata.as_str(), "restore_metadata");
    drop(metadata);
    assert_eq!(
        executor.snapshot().restore_metadata_memory.reserved_bytes,
        0
    );
    assert_eq!(
        executor.snapshot().bulk_memory.reserved_bytes,
        snapshot.bulk_memory.capacity_bytes
    );
    let oversized = executor
        .reserve(
            MemoryClass::RestoreMetadata,
            snapshot.restore_metadata_memory.capacity_bytes + 1,
        )
        .await
        .err()
        .assured("an oversized preparation is refused immediately");
    assert_eq!(
        *oversized.current_context(),
        AdmissionError::ExceedsBudget {
            class: "restore_metadata",
            requested: snapshot.restore_metadata_memory.capacity_bytes + 1,
            capacity: snapshot.restore_metadata_memory.capacity_bytes,
        }
    );
    drop(bulk);
}

#[nervix_primitives::test]
async fn a_relay_budget_below_two_maximum_operations_fails_to_start() {
    let error = Executor::new(ExecutionConfig {
        budgets: MemoryBudgets {
            relay: ByteUnit::Mebibyte(96),
            ..MemoryBudgets::default()
        },
        ..ExecutionConfig::default()
    })
    .expect_err("a relay budget that cannot hold two maximum operations is rejected");
    assert_eq!(
        *error.current_context(),
        ExecutionConfigError::BudgetBelowOperation {
            class: "relay",
            operation: "pair of relay operations",
            budget: ByteUnit::Mebibyte(96).as_u64(),
            required: ByteUnit::Mebibyte(160).as_u64(),
        }
    );
}

#[nervix_primitives::test]
async fn a_management_budget_below_one_event_fails_to_start() {
    let error = Executor::new(ExecutionConfig {
        budgets: MemoryBudgets {
            management: ByteUnit::Kibibyte(32),
            ..MemoryBudgets::default()
        },
        ..ExecutionConfig::default()
    })
    .expect_err("a management budget below one event is rejected");
    assert_eq!(
        *error.current_context(),
        ExecutionConfigError::BudgetBelowOperation {
            class: "management",
            operation: "management event",
            budget: ByteUnit::Kibibyte(32).as_u64(),
            required: ByteUnit::Kibibyte(64).as_u64(),
        }
    );
}

#[nervix_primitives::test]
async fn a_credentials_budget_below_one_password_hash_fails_to_start() {
    let error = Executor::new(ExecutionConfig {
        budgets: MemoryBudgets {
            credentials: ByteUnit::Mebibyte(16),
            ..MemoryBudgets::default()
        },
        ..ExecutionConfig::default()
    })
    .expect_err("a credentials budget below one password hash is rejected");
    assert_eq!(
        *error.current_context(),
        ExecutionConfigError::BudgetBelowOperation {
            class: "credentials",
            operation: "password hash",
            budget: ByteUnit::Mebibyte(16).as_u64(),
            required: ByteUnit::Mebibyte(19).as_u64(),
        }
    );
}

/// A follower charges every decoded replication batch until its Raft core answers it, and encodes
/// that answer against the same class. A budget that cannot hold the whole resident window beside
/// one batch being encoded would let a full window leave no room to produce the answer that
/// releases it, so the node refuses to start instead of deadlocking on its first catch-up.
#[nervix_primitives::test]
async fn a_commands_budget_below_the_resident_replication_window_fails_to_start() {
    let error = Executor::new(ExecutionConfig {
        budgets: MemoryBudgets {
            commands: ByteUnit::Mebibyte(16),
            ..MemoryBudgets::default()
        },
        limits: OperationLimits {
            resident_replication_batches: NonZeroU32::new(8).assured("eight is nonzero"),
            ..OperationLimits::default()
        },
        ..ExecutionConfig::default()
    })
    .expect_err("a commands budget below the resident replication window is rejected");
    assert_eq!(
        *error.current_context(),
        ExecutionConfigError::BudgetBelowOperation {
            class: "commands",
            operation: "resident replication batches beside one being encoded",
            budget: ByteUnit::Mebibyte(16).as_u64(),
            required: ByteUnit::Mebibyte(18).as_u64(),
        }
    );
}

#[nervix_primitives::test]
async fn a_commands_budget_below_one_normalized_state_write_fails_to_start() {
    let error = Executor::new(ExecutionConfig {
        budgets: MemoryBudgets {
            commands: ByteUnit::Mebibyte(15),
            ..MemoryBudgets::default()
        },
        ..ExecutionConfig::default()
    })
    .expect_err("a commands budget below one normalized state write is rejected");
    assert_eq!(
        *error.current_context(),
        ExecutionConfigError::BudgetBelowOperation {
            class: "commands",
            operation: "normalized command state storage",
            budget: ByteUnit::Mebibyte(15).as_u64(),
            required: ByteUnit::Mebibyte(16).as_u64(),
        }
    );
}

#[nervix_primitives::test]
async fn a_reservation_holds_its_class_until_it_is_dropped() {
    let executor = small_executor();
    let capacity = executor.snapshot().bulk_memory.capacity_bytes;
    let reservation = executor
        .try_reserve(MemoryClass::Bulk, capacity)
        .expect("the whole class is available");
    assert_eq!(executor.snapshot().bulk_memory.reserved_bytes, capacity);
    assert_eq!(
        *executor
            .try_reserve(MemoryClass::Bulk, 1)
            .expect_err("a full class refuses another byte")
            .current_context(),
        AdmissionError::BudgetExhausted {
            class: "bulk",
            requested: 1,
        }
    );
    drop(reservation);
    assert_eq!(executor.snapshot().bulk_memory.reserved_bytes, 0);
}

#[nervix_primitives::test]
async fn an_operation_larger_than_its_class_is_refused_rather_than_queued() {
    let executor = small_executor();
    let capacity = executor.snapshot().management_memory.capacity_bytes;
    let error = executor
        .reserve(MemoryClass::Management, capacity + 1)
        .await
        .expect_err("an operation the class can never hold is refused");
    assert_eq!(
        *error.current_context(),
        AdmissionError::ExceedsBudget {
            class: "management",
            requested: capacity + 1,
            capacity,
        }
    );
}

#[cfg(feature = "shuttle")]
mod shuttle_checks {
    use std::{future::Future, task::Poll};

    use error_stack::Report;
    use meticulous::ResultExt as _;
    use nervix_model_harness::shuttle::check_random_and_pct;
    use nervix_primitives::sync::StdArc;

    use super::*;
    use crate::{CpuClass, ExecutionError, QueueAdmission, StorageClass};

    struct ReservationProbe {
        started: nervix_primitives::sync::oneshot::Receiver<()>,
        release: nervix_primitives::sync::oneshot::Sender<()>,
        task: nervix_primitives::task::JoinHandle<()>,
    }

    fn assert_live_reservations_fit(executor: &Executor) {
        let snapshot = executor.snapshot();
        for (class, budget) in [
            ("management", snapshot.management_memory),
            ("commands", snapshot.commands_memory),
            ("relay", snapshot.relay_memory),
            ("bulk", snapshot.bulk_memory),
            ("restore_metadata", snapshot.restore_metadata_memory),
            ("credentials", snapshot.credentials_memory),
        ] {
            assert!(
                budget.reserved_bytes <= budget.capacity_bytes,
                "{class} has {} live reservation bytes but capacity is {}",
                budget.reserved_bytes,
                budget.capacity_bytes
            );
        }
    }

    fn assert_every_permit_returned(executor: &Executor) {
        let snapshot = executor.snapshot();
        for (class, workers) in [
            ("control_cpu", snapshot.control_cpu),
            ("credentials_cpu", snapshot.credentials_cpu),
            ("data_cpu", snapshot.data_cpu),
            ("extension_cpu", snapshot.extension_cpu),
            ("bulk_cpu", snapshot.bulk_cpu),
            ("consensus_storage", snapshot.consensus_storage),
            ("filesystem_storage", snapshot.filesystem_storage),
        ] {
            assert_eq!(
                workers.running, 0,
                "{class} must return every worker permit"
            );
            assert_eq!(workers.pending, 0, "{class} must return every queue permit");
        }
        for (class, budget) in [
            ("management", snapshot.management_memory),
            ("commands", snapshot.commands_memory),
            ("relay", snapshot.relay_memory),
            ("bulk", snapshot.bulk_memory),
            ("restore_metadata", snapshot.restore_metadata_memory),
            ("credentials", snapshot.credentials_memory),
        ] {
            assert_eq!(
                budget.reserved_bytes, 0,
                "{class} must return every memory permit"
            );
        }
    }

    async fn announce_after_first_pending<F>(
        future: F,
        pending: nervix_primitives::sync::oneshot::Sender<()>,
    ) -> F::Output
    where
        F: Future,
    {
        let mut future = std::pin::pin!(future);
        std::future::poll_fn(|context| match future.as_mut().poll(context) {
            Poll::Ready(_) => {
                panic!("the worker was expected to remain occupied while this job queued")
            }
            Poll::Pending => Poll::Ready(()),
        })
        .await;
        pending
            .send(())
            .assured("the test waits for the exact admission point before it can finish");
        future.await
    }

    fn saturated_class_invariant() {
        shuttle::future::block_on(async {
            let executor = small_executor();
            let relay_capacity = executor.snapshot().relay_memory.capacity_bytes;
            let relay = executor
                .try_reserve(MemoryClass::Relay, relay_capacity)
                .assured("the untouched relay class starts with every permit available");
            assert_live_reservations_fit(&executor);

            let mut probes = Vec::new();
            for class in [
                MemoryClass::Management,
                MemoryClass::Commands,
                MemoryClass::Bulk,
            ] {
                let (started, has_started) = nervix_primitives::sync::oneshot::channel();
                let (release, released) = nervix_primitives::sync::oneshot::channel();
                let probe_executor = executor.clone();
                let task = nervix_primitives::task::spawn(async move {
                    let reservation = probe_executor
                        .try_reserve(class, 1024)
                        .assured("a saturated relay class cannot consume another class's permits");
                    assert_live_reservations_fit(&probe_executor);
                    started
                        .send(())
                        .assured("the test holds the receiver until every class is charged");
                    released
                        .await
                        .assured("the test releases every held class before it exits");
                    drop(reservation);
                });
                probes.push(ReservationProbe {
                    started: has_started,
                    release,
                    task,
                });
            }

            for probe in &mut probes {
                nervix_primitives::task::consume_budget().await;
                (&mut probe.started)
                    .await
                    .assured("each independent memory class reports its live reservation");
                assert_live_reservations_fit(&executor);
            }

            let snapshot = executor.snapshot();
            assert_eq!(snapshot.relay_memory.reserved_bytes, relay_capacity);
            assert_eq!(snapshot.management_memory.reserved_bytes, 1024);
            assert_eq!(snapshot.commands_memory.reserved_bytes, 1024);
            assert_eq!(snapshot.bulk_memory.reserved_bytes, 1024);

            let Err(error) = executor.try_reserve(MemoryClass::Relay, 1) else {
                panic!("a saturated relay class must refuse another byte");
            };
            assert_eq!(
                *error.current_context(),
                AdmissionError::BudgetExhausted {
                    class: "relay",
                    requested: 1,
                }
            );
            assert_live_reservations_fit(&executor);

            for probe in probes {
                nervix_primitives::task::consume_budget().await;
                probe
                    .release
                    .send(())
                    .assured("the reservation task remains blocked on its release signal");
                probe
                    .task
                    .await
                    .assured("an independent reservation task does not panic");
                assert_live_reservations_fit(&executor);
            }
            drop(relay);
            assert_every_permit_returned(&executor);
        });
    }

    #[test]
    fn shuttle_saturated_class_keeps_live_reservations_within_each_class_capacity() {
        check_random_and_pct(saturated_class_invariant);
    }

    fn occupied_bulk_execution_invariant() {
        shuttle::future::block_on(async {
            let executor = small_executor();
            let (started, has_started) = nervix_primitives::sync::oneshot::channel();
            let (release, released) = nervix_primitives::sync::oneshot::channel();
            let bulk_reservation = executor
                .try_reserve(MemoryClass::Bulk, 1024)
                .assured("the untouched bulk class starts with room");
            let bulk_executor = executor.clone();
            let bulk = nervix_primitives::task::spawn(async move {
                bulk_executor
                    .run_cpu(
                        CpuClass::Bulk,
                        bulk_reservation,
                        move |_charge, _cancellation| {
                            started
                                .send(())
                                .assured("the test awaits the occupied bulk worker");
                            released
                                .blocking_recv()
                                .assured("the test releases the bulk worker before it exits");
                        },
                    )
                    .await
                    .assured("the admitted bulk job runs");
            });
            has_started
                .await
                .assured("the bulk job reports after taking its worker permit");

            let snapshot = executor.snapshot();
            assert_eq!(snapshot.bulk_cpu.running, 1);
            assert_eq!(snapshot.bulk_cpu.pending, 0);
            assert_live_reservations_fit(&executor);

            let control_reservation = executor
                .try_reserve(MemoryClass::Management, 1024)
                .assured("the bulk job cannot consume management memory");
            let value = executor
                .run_cpu(CpuClass::Control, control_reservation, |_, _| 7_u32)
                .await
                .assured("the occupied bulk class cannot consume the control worker");
            assert_eq!(value, 7);
            assert_eq!(executor.snapshot().bulk_cpu.running, 1);
            assert_live_reservations_fit(&executor);

            release
                .send(())
                .assured("the bulk job remains blocked until control work finishes");
            bulk.await
                .assured("the occupied bulk job exits after its release");
            assert_every_permit_returned(&executor);
        });
    }

    #[test]
    fn shuttle_occupied_bulk_execution_leaves_control_execution_untouched() {
        check_random_and_pct(occupied_bulk_execution_invariant);
    }

    fn queued_job_drop_invariant() {
        shuttle::future::block_on(async {
            let executor = small_executor();
            let (started, has_started) = nervix_primitives::sync::oneshot::channel();
            let (release, released) = nervix_primitives::sync::oneshot::channel();
            let occupying_bytes = 1024;
            let occupying = executor
                .try_reserve(MemoryClass::Relay, occupying_bytes)
                .assured("the untouched relay class starts with room");
            let occupied = executor.clone();
            let running = nervix_primitives::task::spawn(async move {
                occupied
                    .run_cpu(CpuClass::Data, occupying, move |_charge, _| {
                        started
                            .send(())
                            .assured("the test awaits the occupying data job");
                        released
                            .blocking_recv()
                            .assured("the test releases the occupying job before it exits");
                    })
                    .await
                    .assured("the occupying data job runs");
            });
            has_started
                .await
                .assured("the occupying job reports after taking its worker permit");

            let queued_bytes = 4096;
            let queued = executor
                .try_reserve(MemoryClass::Relay, queued_bytes)
                .assured("the relay class has room for the queued job");
            let (admitted, is_admitted) = nervix_primitives::sync::oneshot::channel();
            let waiting = executor.clone();
            let cancelled = nervix_primitives::task::spawn(async move {
                announce_after_first_pending(
                    waiting.run_cpu(CpuClass::Data, queued, |_, _| ()),
                    admitted,
                )
                .await
            });
            is_admitted
                .await
                .assured("the queued job reports after taking its queue permit");

            let queued_snapshot = executor.snapshot();
            assert_eq!(queued_snapshot.data_cpu.running, 1);
            assert_eq!(queued_snapshot.data_cpu.pending, 1);
            assert_eq!(
                queued_snapshot.relay_memory.reserved_bytes,
                occupying_bytes + queued_bytes
            );
            assert_live_reservations_fit(&executor);

            cancelled.abort();
            let cancelled_result = cancelled.await;
            assert!(
                cancelled_result.is_err(),
                "the queued job must be cancelled before it can take the occupied worker"
            );

            let cancelled_snapshot = executor.snapshot();
            assert_eq!(cancelled_snapshot.data_cpu.running, 1);
            assert_eq!(cancelled_snapshot.data_cpu.pending, 0);
            assert_eq!(
                cancelled_snapshot.relay_memory.reserved_bytes,
                occupying_bytes
            );
            assert_live_reservations_fit(&executor);

            release
                .send(())
                .assured("the occupying job remains blocked while the queued job is dropped");
            running
                .await
                .assured("the occupying job exits after its release");
            assert_every_permit_returned(&executor);
        });
    }

    #[test]
    fn shuttle_queued_job_drop_releases_its_reservation_and_exact_queue_slot() {
        check_random_and_pct(queued_job_drop_invariant);
    }

    fn running_job_cancellation_invariant() {
        shuttle::future::block_on(async {
            let executor = small_executor();
            let (started, has_started) = nervix_primitives::sync::oneshot::channel();
            let (continue_job, may_continue) = nervix_primitives::sync::oneshot::channel();
            let (exited, has_exited) = nervix_primitives::sync::oneshot::channel();
            let reservation = executor
                .try_reserve(MemoryClass::Relay, 8192)
                .assured("the untouched relay class starts with room");
            let running = executor.clone();
            let waiting_snapshot = executor.clone();
            let task = nervix_primitives::task::spawn(async move {
                running
                    .run_cpu(CpuClass::Data, reservation, move |_charge, cancellation| {
                        started
                            .send(())
                            .assured("the test awaits the running job before cancelling it");
                        may_continue
                            .blocking_recv()
                            .assured("the test lets the running job inspect cancellation");
                        assert!(
                            cancellation.check().is_err(),
                            "the running job must observe cancellation after its waiter is dropped"
                        );
                        exited
                            .send(waiting_snapshot.snapshot().relay_memory.reserved_bytes)
                            .assured("the test waits for the job's charged exit point");
                    })
                    .await
                    .assured("the running job itself exits normally after observing cancellation");
            });
            has_started
                .await
                .assured("the job reports after taking its worker permit");

            let running_snapshot = executor.snapshot();
            assert_eq!(running_snapshot.data_cpu.running, 1);
            assert_eq!(running_snapshot.data_cpu.pending, 0);
            assert_eq!(running_snapshot.relay_memory.reserved_bytes, 8192);
            assert_live_reservations_fit(&executor);

            task.abort();
            let cancelled_result = task.await;
            assert!(
                cancelled_result.is_err(),
                "dropping the running job's waiter must cancel that waiter"
            );
            let cancelled_snapshot = executor.snapshot();
            assert_eq!(cancelled_snapshot.data_cpu.running, 1);
            assert_eq!(cancelled_snapshot.data_cpu.pending, 0);
            assert_eq!(cancelled_snapshot.relay_memory.reserved_bytes, 8192);

            continue_job
                .send(())
                .assured("the running job remains alive after its waiter is cancelled");
            let charged_at_exit = has_exited
                .await
                .assured("the running job reports the charge it sees at exit");
            assert_eq!(
                charged_at_exit, 8192,
                "a running job keeps its charge until it actually exits"
            );

            let capacity = executor.snapshot().relay_memory.capacity_bytes;
            let every_memory_permit = executor
                .reserve(MemoryClass::Relay, capacity)
                .await
                .assured("the cancelled job eventually returns its whole reservation");
            executor
                .run_cpu(CpuClass::Data, every_memory_permit, |_, _| ())
                .await
                .assured("the cancelled job eventually returns its worker permit");
            assert_every_permit_returned(&executor);
        });
    }

    #[test]
    fn shuttle_running_job_observes_cancellation_and_keeps_its_charge_until_exit() {
        check_random_and_pct(running_job_cancellation_invariant);
    }

    fn full_wait_queue_invariant() {
        shuttle::future::block_on(async {
            let executor = small_executor();
            let (started, has_started) = nervix_primitives::sync::oneshot::channel();
            let (release, released) = nervix_primitives::sync::oneshot::channel();
            let occupying = executor
                .try_reserve(MemoryClass::Relay, 1024)
                .assured("the untouched relay class starts with room");
            let occupied = executor.clone();
            let running = nervix_primitives::task::spawn(async move {
                occupied
                    .run_cpu(CpuClass::Data, occupying, move |_charge, _| {
                        started
                            .send(())
                            .assured("the test awaits the occupying data job");
                        released
                            .blocking_recv()
                            .assured("the test releases the occupying job before it exits");
                    })
                    .await
                    .assured("the occupying data job runs");
            });
            has_started
                .await
                .assured("the occupying job reports after taking its worker permit");

            let queued = executor
                .try_reserve(MemoryClass::Relay, 1024)
                .assured("the relay class has room for the queued job");
            let (admitted, is_admitted) = nervix_primitives::sync::oneshot::channel();
            let waiting = executor.clone();
            let pending = nervix_primitives::task::spawn(async move {
                announce_after_first_pending(
                    waiting.run_cpu(CpuClass::Data, queued, |_, _| ()),
                    admitted,
                )
                .await
            });
            is_admitted
                .await
                .assured("the queued job reports after taking the only queue permit");

            let full_snapshot = executor.snapshot();
            assert_eq!(full_snapshot.data_cpu.running, 1);
            assert_eq!(full_snapshot.data_cpu.pending, 1);
            assert_live_reservations_fit(&executor);

            let refused = executor
                .try_reserve(MemoryClass::Relay, 1024)
                .assured("memory remains independent from worker queue admission");
            let error = executor
                .run_cpu(CpuClass::Data, refused, |_, _| ())
                .await
                .expect_err("the single wait slot is already taken");
            assert!(
                matches!(
                    error.current_context(),
                    crate::ExecutionError::QueueFull { class, pending }
                        if *class == "data_cpu" && *pending == 1
                ),
                "expected typed queue backpressure, got {error}"
            );

            let refused_snapshot = executor.snapshot();
            assert_eq!(refused_snapshot.data_cpu.running, 1);
            assert_eq!(refused_snapshot.data_cpu.pending, 1);
            assert_eq!(refused_snapshot.data_cpu.refused, 1);
            assert_live_reservations_fit(&executor);

            release
                .send(())
                .assured("the occupying job remains blocked until backpressure is observed");
            running
                .await
                .assured("the occupying job exits after its release");
            pending
                .await
                .assured("the queued job is joined")
                .assured("the queued job runs once the worker permit returns");
            assert_every_permit_returned(&executor);
        });
    }

    #[test]
    fn shuttle_full_wait_queue_is_exact_typed_backpressure() {
        check_random_and_pct(full_wait_queue_invariant);
    }

    /// The only data worker and the only place in its wait queue, each held by a job until the
    /// check lets the worker's job exit.
    struct FullDataQueue {
        release: nervix_primitives::sync::oneshot::Sender<()>,
        running: nervix_primitives::task::JoinHandle<()>,
        queued: nervix_primitives::task::JoinHandle<Result<(), Report<ExecutionError>>>,
    }

    impl FullDataQueue {
        async fn fill(executor: &Executor) -> Self {
            let (started, has_started) = nervix_primitives::sync::oneshot::channel();
            let (release, released) = nervix_primitives::sync::oneshot::channel();
            let occupying = executor
                .try_reserve(MemoryClass::Relay, 1024)
                .assured("the untouched relay class starts with room");
            let occupied = executor.clone();
            let running = nervix_primitives::task::spawn(async move {
                occupied
                    .run_cpu(CpuClass::Data, occupying, move |_charge, _| {
                        started
                            .send(())
                            .assured("the check awaits the occupying data job");
                        released
                            .blocking_recv()
                            .assured("the check releases the occupying job before it exits");
                    })
                    .await
                    .assured("the occupying data job runs");
            });
            has_started
                .await
                .assured("the occupying job reports after taking its worker permit");

            let queued_charge = executor
                .try_reserve(MemoryClass::Relay, 1024)
                .assured("the relay class has room for the queued job");
            let (admitted, is_admitted) = nervix_primitives::sync::oneshot::channel();
            let queue_holder = executor.clone();
            let queued = nervix_primitives::task::spawn(async move {
                announce_after_first_pending(
                    queue_holder.run_cpu(CpuClass::Data, queued_charge, |_, _| ()),
                    admitted,
                )
                .await
            });
            is_admitted
                .await
                .assured("the queued job reports after taking the only queue permit");
            Self {
                release,
                running,
                queued,
            }
        }

        /// Let the worker's job exit, so the queued job takes the worker and frees its place, and
        /// wait for both.
        async fn release(self) {
            self.release
                .send(())
                .assured("the occupying job stays blocked until the check releases it");
            self.running
                .await
                .assured("the occupying job exits after its release");
            self.queued
                .await
                .assured("the queued job is joined")
                .assured("the queued job runs once the worker permit returns");
        }
    }

    /// A job waiting for a place, and the report its first poll sends once the full queue holds it.
    struct WaitingJob {
        registered: nervix_primitives::sync::oneshot::Receiver<()>,
        task: nervix_primitives::task::JoinHandle<Result<(), Report<ExecutionError>>>,
    }

    impl WaitingJob {
        /// Spawn a data job that waits for a place and records `label` in `order` when it runs.
        fn spawn(
            executor: &Executor,
            order: &StdArc<nervix_primitives::sync::blocking::Mutex<Vec<&'static str>>>,
            label: &'static str,
        ) -> Self {
            let charge = executor
                .try_reserve(MemoryClass::Relay, 1024)
                .assured("the relay class has room for a waiting job");
            let (registered, is_registered) = nervix_primitives::sync::oneshot::channel();
            let waiter = executor.clone();
            let job_order = StdArc::clone(order);
            let task = nervix_primitives::task::spawn(async move {
                announce_after_first_pending(
                    waiter.run_cpu_with(
                        CpuClass::Data,
                        QueueAdmission::WaitForPlace,
                        charge,
                        move |_, _| job_order.lock().push(label),
                    ),
                    registered,
                )
                .await
            });
            Self {
                registered: is_registered,
                task,
            }
        }
    }

    fn waiting_admission_order_invariant() {
        shuttle::future::block_on(async {
            let executor = small_executor();
            let order = StdArc::new(nervix_primitives::sync::blocking::Mutex::new(Vec::new()));
            let full = FullDataQueue::fill(&executor).await;

            let WaitingJob {
                registered,
                task: waiting,
            } = WaitingJob::spawn(&executor, &order, "waiting");
            registered
                .await
                .assured("the waiting job reports once the full queue holds it");
            let held = executor.snapshot();
            assert_eq!(held.data_cpu.pending, 1, "a waiting job takes no place");
            assert_eq!(held.data_cpu.refused, 0, "a waiting job is not refused");
            assert_eq!(held.relay_memory.reserved_bytes, 3 * 1024);

            // A job refused when full asks after the waiting one, racing the release that frees a
            // place.
            let late_charge = executor
                .try_reserve(MemoryClass::Relay, 1024)
                .assured("the relay class has room for the late job");
            let late_executor = executor.clone();
            let late_order = StdArc::clone(&order);
            let late = nervix_primitives::task::spawn(async move {
                late_executor
                    .run_cpu(CpuClass::Data, late_charge, move |_, _| {
                        late_order.lock().push("late");
                    })
                    .await
            });
            full.release().await;
            waiting
                .await
                .assured("the waiting job is joined")
                .assured("the waiting job runs once a place frees");
            if let Err(error) = late.await.assured("the late job is joined") {
                assert!(
                    matches!(error.current_context(), ExecutionError::QueueFull { .. }),
                    "a late job is only ever refused for a full queue, got {error}"
                );
            }

            let order = order.lock().clone();
            assert_eq!(
                order.first(),
                Some(&"waiting"),
                "the freed place goes to the job that waited for it: {order:?}"
            );
            assert_every_permit_returned(&executor);
        });
    }

    /// A job waiting for a place takes the first place the full queue frees, ahead of a job refused
    /// when full that asks after it, and runs first.
    #[test]
    fn shuttle_a_job_waiting_for_a_place_takes_the_first_freed_place() {
        check_random_and_pct(waiting_admission_order_invariant);
    }

    fn dropped_waiting_admission_invariant() {
        shuttle::future::block_on(async {
            let executor = small_executor();
            let order = StdArc::new(nervix_primitives::sync::blocking::Mutex::new(Vec::new()));
            let full = FullDataQueue::fill(&executor).await;

            let WaitingJob {
                registered,
                task: dropped,
            } = WaitingJob::spawn(&executor, &order, "dropped");
            registered
                .await
                .assured("the first waiting job reports once the full queue holds it");
            let WaitingJob {
                registered: successor_registered,
                task: successor,
            } = WaitingJob::spawn(&executor, &order, "successor");
            successor_registered
                .await
                .assured("the second waiting job reports once the full queue holds it");
            assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 4 * 1024);

            dropped.abort();
            assert!(
                dropped.await.is_err(),
                "dropping a waiting job ends it before it takes a place"
            );
            let abandoned = executor.snapshot();
            assert_eq!(
                abandoned.relay_memory.reserved_bytes,
                3 * 1024,
                "a waiting job that is dropped returns its charge"
            );
            assert_eq!(abandoned.data_cpu.pending, 1);

            full.release().await;
            successor
                .await
                .assured("the second waiting job is joined")
                .assured("the place the dropped job gave up goes to the job after it");
            assert_eq!(*order.lock(), vec!["successor"]);
            assert_every_permit_returned(&executor);
        });
    }

    /// A job waiting for a place that its caller drops gives up its place in line and returns its
    /// charge, and the place freed next goes to the job waiting after it.
    #[test]
    fn shuttle_a_dropped_job_waiting_for_a_place_gives_up_its_place_and_charge() {
        check_random_and_pct(dropped_waiting_admission_invariant);
    }

    fn consensus_admission_order_invariant() {
        shuttle::future::block_on(async {
            let executor = queued_executor(16);
            let order = StdArc::new(nervix_primitives::sync::blocking::Mutex::new(Vec::new()));
            let (release, released) = nervix_primitives::sync::oneshot::channel::<()>();
            let mut held = Some(released);
            let mut submissions = Vec::new();

            for index in 0..8_usize {
                nervix_primitives::task::consume_budget().await;
                let reservation = executor
                    .try_reserve(MemoryClass::Commands, 1024)
                    .assured("the commands class has room for every ordered job");
                let submitted = executor.clone();
                let job_order = StdArc::clone(&order);
                let gate = held.take();
                let (admitted, is_admitted) = nervix_primitives::sync::oneshot::channel();
                submissions.push(nervix_primitives::task::spawn(async move {
                    announce_after_first_pending(
                        submitted.run_storage(
                            StorageClass::Consensus,
                            reservation,
                            move |_charge, _| {
                                if let Some(gate) = gate {
                                    gate.blocking_recv()
                                        .assured("the test releases the first admitted job");
                                }
                                job_order.lock().push(index);
                            },
                        ),
                        admitted,
                    )
                    .await
                }));
                is_admitted
                    .await
                    .assured("each job reports after entering consensus admission");

                let submitted_so_far = index
                    .checked_add(1)
                    .assured("the test submits a fixed eight jobs");
                let snapshot = executor.snapshot();
                assert_eq!(
                    snapshot.consensus_storage.admitted,
                    u64::try_from(submitted_so_far).assured("eight jobs fit in u64")
                );
                assert_eq!(snapshot.consensus_storage.pending, index);
                assert_live_reservations_fit(&executor);
            }

            let queued_snapshot = executor.snapshot();
            assert_eq!(queued_snapshot.consensus_storage.running, 1);
            assert_eq!(queued_snapshot.consensus_storage.pending, 7);
            release
                .send(())
                .assured("the first consensus job remains held while its followers queue");

            for submission in submissions {
                nervix_primitives::task::consume_budget().await;
                submission
                    .await
                    .assured("every consensus submission is joined")
                    .assured("every admitted consensus job runs");
            }

            assert_eq!(*order.lock(), (0..8_usize).collect::<Vec<_>>());
            assert_every_permit_returned(&executor);
        });
    }

    #[test]
    fn shuttle_consensus_storage_preserves_admission_order_and_returns_every_permit() {
        check_random_and_pct(consensus_admission_order_invariant);
    }
}

#[nervix_primitives::test]
async fn an_incremental_writer_fails_at_its_budget_boundary() {
    let executor = small_executor();
    let capacity = executor.snapshot().bulk_memory.capacity_bytes;
    let reservation = executor
        .try_reserve(MemoryClass::Bulk, 64 * 1024)
        .expect("the bulk class is empty");
    let mut buffer = BudgetedBuffer::new(reservation);
    let chunk = vec![7_u8; 64 * 1024];
    let mut written = 0_u64;
    let error = loop {
        match buffer.write_all(&chunk) {
            Ok(()) => {
                written += 64 * 1024;
                assert!(
                    written <= capacity,
                    "the writer grew past the whole bulk class"
                );
            }
            Err(error) => break error,
        }
    };
    assert_eq!(
        u64::try_from(buffer.len()).expect("the written length fits"),
        written
    );
    assert!(
        written < capacity + 64 * 1024,
        "the writer stopped at its budget"
    );
    assert!(
        error
            .to_string()
            .contains(&executor.limits().relay_encoded_bytes.as_u64().to_string())
            || error.to_string().contains(&capacity.to_string()),
        "the failure names the boundary the writer stopped at: {error}"
    );
}

#[nervix_primitives::test]
async fn a_budgeted_buffer_returns_its_bytes_with_the_charge_that_backs_them() {
    let executor = small_executor();
    let reservation = executor
        .try_reserve(MemoryClass::Commands, 4096)
        .expect("the commands class is empty");
    let mut buffer = BudgetedBuffer::new(reservation);
    buffer.write_all(b"nervix").expect("a small write fits");
    let (bytes, reservation) = buffer.into_parts();
    assert_eq!(bytes, b"nervix");
    assert_eq!(
        executor.snapshot().commands_memory.reserved_bytes,
        6,
        "taking the buffer apart narrows the charge to what it holds"
    );
    assert_eq!(
        bytes.capacity(),
        bytes.len(),
        "and narrows the allocation with it, so the charge still measures the memory"
    );
    drop(reservation);
    assert_eq!(executor.snapshot().commands_memory.reserved_bytes, 0);
}

#[nervix_primitives::test]
async fn an_operation_limit_stops_a_writer_before_its_class_does() {
    let executor = small_executor();
    let reservation = executor
        .try_reserve(MemoryClass::Relay, 64 * 1024)
        .expect("the relay class is empty");
    let mut buffer = crate::BudgetedBuffer::with_limit(reservation, 100 * 1024);
    let chunk = vec![3_u8; 64 * 1024];
    buffer.write_all(&chunk).expect("the first chunk fits");
    let error = buffer
        .write_all(&chunk)
        .expect_err("the second chunk crosses the operation limit");
    assert_eq!(
        buffer.len(),
        64 * 1024,
        "the writer kept only what it wrote"
    );
    assert!(
        error.to_string().contains("102400"),
        "the failure names the operation limit: {error}"
    );
    assert!(
        executor.snapshot().relay_memory.reserved_bytes < 100 * 1024,
        "the writer never charged past the operation limit"
    );
}

/// A writer that reserves room to grow into must not keep that room once it is done. A frame the
/// size of a heartbeat holds its own bytes, not the granule its writer started with.
#[nervix_primitives::test]
async fn freezing_a_buffer_returns_the_room_the_writer_did_not_use() {
    let executor = small_executor();
    let reservation = executor
        .try_reserve(MemoryClass::Management, 4096)
        .expect("the management class is empty");
    let mut buffer = BudgetedBuffer::with_limit(reservation, 64 * 1024);
    buffer
        .write_all(&[7_u8; 5])
        .expect("a heartbeat-sized write fits");
    assert_eq!(executor.snapshot().management_memory.reserved_bytes, 4096);

    let frozen = crate::ChargedBytes::from_buffer(buffer);

    assert_eq!(frozen.len(), 5);
    assert_eq!(
        executor.snapshot().management_memory.reserved_bytes,
        5,
        "the charge narrows to the bytes the writer actually produced"
    );
    drop(frozen);
    assert_eq!(executor.snapshot().management_memory.reserved_bytes, 0);
}

/// Shrinking is what keeps a class usable under churn: a thousand small frames must not exhaust a
/// budget sized for the operations it actually has to hold.
#[nervix_primitives::test]
async fn many_small_frames_do_not_exhaust_the_class_that_backs_them() {
    let executor = small_executor();
    let capacity = executor.snapshot().management_memory.capacity_bytes;
    let mut frozen = Vec::new();
    for _ in 0..2048 {
        let reservation = executor
            .try_reserve(MemoryClass::Management, 4096)
            .expect("a small frame is always admitted while the class holds its own size");
        let mut buffer = BudgetedBuffer::with_limit(reservation, 64 * 1024);
        buffer.write_all(&[1_u8; 50]).expect("a small write fits");
        frozen.push(crate::ChargedBytes::from_buffer(buffer));
    }
    let reserved = executor.snapshot().management_memory.reserved_bytes;
    assert_eq!(reserved, 2048 * 50);
    assert!(
        reserved < capacity / 4,
        "two thousand small frames leave the class with room for the work that must not be blocked"
    );
    executor
        .try_reserve(MemoryClass::Management, 4096)
        .expect("a connection handshake still gets its reservation");
}
