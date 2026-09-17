use std::{io::Write as _, num::NonZeroUsize};

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
            data_cpu: one(),
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
                data_cpu: one(),
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

#[tokio::test]
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

#[tokio::test]
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

#[tokio::test]
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

/// A follower charges every decoded replication batch until its Raft core answers it, and encodes
/// that answer against the same class. A budget that cannot hold the whole resident window beside
/// one batch being encoded would let a full window leave no room to produce the answer that
/// releases it, so the node refuses to start instead of deadlocking on its first catch-up.
#[tokio::test]
async fn a_commands_budget_below_the_resident_replication_window_fails_to_start() {
    let error = Executor::new(ExecutionConfig {
        budgets: MemoryBudgets {
            commands: ByteUnit::Mebibyte(8),
            ..MemoryBudgets::default()
        },
        ..ExecutionConfig::default()
    })
    .expect_err("a commands budget below the resident replication window is rejected");
    assert_eq!(
        *error.current_context(),
        ExecutionConfigError::BudgetBelowOperation {
            class: "commands",
            operation: "resident replication batches beside one being encoded",
            budget: ByteUnit::Mebibyte(8).as_u64(),
            required: ByteUnit::Mebibyte(10).as_u64(),
        }
    );
}

#[tokio::test]
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

#[tokio::test]
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
    use std::{future::Future, path::PathBuf, sync::Arc as StdArc, task::Poll};

    use meticulous::{OptionExt as _, ResultExt as _};
    use shuttle::{
        Config, FailurePersistence, Runner,
        scheduler::{PctScheduler, RandomScheduler},
    };

    use super::*;
    use crate::{CpuClass, StorageClass};

    const RANDOM_ITERATIONS: usize = 100;
    const PCT_ITERATIONS: usize = 100;
    const PCT_DEPTH: usize = 3;

    struct ReservationProbe {
        started: tokio::sync::oneshot::Receiver<()>,
        release: tokio::sync::oneshot::Sender<()>,
        task: tokio::task::JoinHandle<()>,
    }

    fn check_invariant(invariant: fn()) {
        if let Some(schedule) = std::env::var_os("SHUTTLE_TRACE_FILE") {
            shuttle::replay_from_file(invariant, schedule);
            return;
        }

        let Some(trace_directory) = std::env::var_os("SHUTTLE_TRACE_DIR") else {
            shuttle::check_random(invariant, RANDOM_ITERATIONS);
            shuttle::check_pct(invariant, PCT_ITERATIONS, PCT_DEPTH);
            return;
        };

        let trace_directory = PathBuf::from(trace_directory);
        if let Err(error) = std::fs::create_dir_all(&trace_directory) {
            panic!(
                "cannot create Shuttle failure directory {}: {error}",
                trace_directory.display()
            );
        }

        let mut config = Config::new();
        config.failure_persistence = FailurePersistence::File(Some(trace_directory));

        Runner::new(RandomScheduler::new(RANDOM_ITERATIONS), config.clone()).run(invariant);
        Runner::new(PctScheduler::new(PCT_DEPTH, PCT_ITERATIONS), config).run(invariant);
    }

    fn assert_live_reservations_fit(executor: &Executor) {
        let snapshot = executor.snapshot();
        for (class, budget) in [
            ("management", snapshot.management_memory),
            ("commands", snapshot.commands_memory),
            ("relay", snapshot.relay_memory),
            ("bulk", snapshot.bulk_memory),
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
            ("data_cpu", snapshot.data_cpu),
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
        ] {
            assert_eq!(
                budget.reserved_bytes, 0,
                "{class} must return every memory permit"
            );
        }
    }

    async fn announce_after_first_pending<F>(
        future: F,
        pending: tokio::sync::oneshot::Sender<()>,
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
                let (started, has_started) = tokio::sync::oneshot::channel();
                let (release, released) = tokio::sync::oneshot::channel();
                let probe_executor = executor.clone();
                let task = tokio::spawn(async move {
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
                tokio::task::consume_budget().await;
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
                tokio::task::consume_budget().await;
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
        check_invariant(saturated_class_invariant);
    }

    fn occupied_bulk_execution_invariant() {
        shuttle::future::block_on(async {
            let executor = small_executor();
            let (started, has_started) = tokio::sync::oneshot::channel();
            let (release, released) = tokio::sync::oneshot::channel();
            let bulk_reservation = executor
                .try_reserve(MemoryClass::Bulk, 1024)
                .assured("the untouched bulk class starts with room");
            let bulk_executor = executor.clone();
            let bulk = tokio::spawn(async move {
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
        check_invariant(occupied_bulk_execution_invariant);
    }

    fn queued_job_drop_invariant() {
        shuttle::future::block_on(async {
            let executor = small_executor();
            let (started, has_started) = tokio::sync::oneshot::channel();
            let (release, released) = tokio::sync::oneshot::channel();
            let occupying_bytes = 1024;
            let occupying = executor
                .try_reserve(MemoryClass::Relay, occupying_bytes)
                .assured("the untouched relay class starts with room");
            let occupied = executor.clone();
            let running = tokio::spawn(async move {
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
            let (admitted, is_admitted) = tokio::sync::oneshot::channel();
            let waiting = executor.clone();
            let cancelled = tokio::spawn(async move {
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
        check_invariant(queued_job_drop_invariant);
    }

    fn running_job_cancellation_invariant() {
        shuttle::future::block_on(async {
            let executor = small_executor();
            let (started, has_started) = tokio::sync::oneshot::channel();
            let (continue_job, may_continue) = tokio::sync::oneshot::channel();
            let (exited, has_exited) = tokio::sync::oneshot::channel();
            let reservation = executor
                .try_reserve(MemoryClass::Relay, 8192)
                .assured("the untouched relay class starts with room");
            let running = executor.clone();
            let waiting_snapshot = executor.clone();
            let task = tokio::spawn(async move {
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
        check_invariant(running_job_cancellation_invariant);
    }

    fn full_wait_queue_invariant() {
        shuttle::future::block_on(async {
            let executor = small_executor();
            let (started, has_started) = tokio::sync::oneshot::channel();
            let (release, released) = tokio::sync::oneshot::channel();
            let occupying = executor
                .try_reserve(MemoryClass::Relay, 1024)
                .assured("the untouched relay class starts with room");
            let occupied = executor.clone();
            let running = tokio::spawn(async move {
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
            let (admitted, is_admitted) = tokio::sync::oneshot::channel();
            let waiting = executor.clone();
            let pending = tokio::spawn(async move {
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
        check_invariant(full_wait_queue_invariant);
    }

    fn consensus_admission_order_invariant() {
        shuttle::future::block_on(async {
            let executor = queued_executor(16);
            let order = StdArc::new(parking_lot::Mutex::new(Vec::new()));
            let (release, released) = tokio::sync::oneshot::channel::<()>();
            let mut held = Some(released);
            let mut submissions = Vec::new();

            for index in 0..8_usize {
                tokio::task::consume_budget().await;
                let reservation = executor
                    .try_reserve(MemoryClass::Commands, 1024)
                    .assured("the commands class has room for every ordered job");
                let submitted = executor.clone();
                let job_order = StdArc::clone(&order);
                let gate = held.take();
                let (admitted, is_admitted) = tokio::sync::oneshot::channel();
                submissions.push(tokio::spawn(async move {
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
                tokio::task::consume_budget().await;
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
        check_invariant(consensus_admission_order_invariant);
    }
}

#[tokio::test]
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

#[tokio::test]
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

#[tokio::test]
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
#[tokio::test]
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
#[tokio::test]
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
