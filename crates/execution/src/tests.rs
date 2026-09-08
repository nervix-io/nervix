use std::{
    io::Write as _,
    num::NonZeroUsize,
    sync::{
        Arc as StdArc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use ubyte::ByteUnit;

use crate::{
    AdmissionError, BudgetedBuffer, CpuClass, ExecutionConfig, ExecutionConfigError, Executor,
    MemoryBudgets, MemoryClass, OperationLimits, StorageClass, WorkerCounts,
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

#[test]
fn default_limits_validate_together() {
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

#[test]
fn a_relay_budget_below_two_maximum_operations_fails_to_start() {
    let error = Executor::new(ExecutionConfig {
        budgets: MemoryBudgets {
            relay: ByteUnit::Mebibyte(96),
            ..MemoryBudgets::default()
        },
        ..ExecutionConfig::default()
    })
    .expect_err("a relay budget that cannot hold two maximum operations is rejected");
    assert_eq!(
        error,
        ExecutionConfigError::BudgetBelowOperation {
            class: "relay",
            operation: "pair of relay operations",
            budget: ByteUnit::Mebibyte(96).as_u64(),
            required: ByteUnit::Mebibyte(160).as_u64(),
        }
    );
}

#[test]
fn a_management_budget_below_one_event_fails_to_start() {
    let error = Executor::new(ExecutionConfig {
        budgets: MemoryBudgets {
            management: ByteUnit::Kibibyte(32),
            ..MemoryBudgets::default()
        },
        ..ExecutionConfig::default()
    })
    .expect_err("a management budget below one event is rejected");
    assert_eq!(
        error,
        ExecutionConfigError::BudgetBelowOperation {
            class: "management",
            operation: "management event",
            budget: ByteUnit::Kibibyte(32).as_u64(),
            required: ByteUnit::Kibibyte(64).as_u64(),
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
        executor.try_reserve(MemoryClass::Bulk, 1).err(),
        Some(AdmissionError::BudgetExhausted {
            class: "bulk",
            requested: 1,
        })
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
        error,
        AdmissionError::ExceedsBudget {
            class: "management",
            requested: capacity + 1,
            capacity,
        }
    );
}

#[tokio::test]
async fn a_saturated_class_leaves_the_other_classes_untouched() {
    let executor = small_executor();
    let relay = executor.snapshot().relay_memory.capacity_bytes;
    let _held = executor
        .try_reserve(MemoryClass::Relay, relay)
        .expect("the whole relay class is available");
    for class in [
        MemoryClass::Management,
        MemoryClass::Commands,
        MemoryClass::Bulk,
    ] {
        executor
            .try_reserve(class, 1024)
            .unwrap_or_else(|error| panic!("{class:?} must not borrow from relay work: {error}"));
    }
}

#[tokio::test]
async fn occupied_bulk_execution_does_not_delay_control_execution() {
    let executor = small_executor();
    let (release_bulk, bulk_released) = tokio::sync::oneshot::channel::<()>();
    let bulk_started = StdArc::new(tokio::sync::Notify::new());
    let started = StdArc::clone(&bulk_started);
    let bulk_reservation = executor
        .try_reserve(MemoryClass::Bulk, 1024)
        .expect("the bulk class is empty");
    let bulk_executor = executor.clone();
    let bulk = tokio::spawn(async move {
        bulk_executor
            .run_cpu(CpuClass::Bulk, bulk_reservation, move |_cancellation| {
                started.notify_waiters();
                // The bulk worker is deliberately occupied for the whole of the control request.
                let _ = bulk_released.blocking_recv();
            })
            .await
            .expect("the bulk job runs")
    });
    // Wait for the single bulk worker to actually be occupied before measuring the control class.
    while executor.snapshot().bulk_cpu.running == 0 {
        tokio::task::yield_now().await;
    }

    let control_reservation = executor
        .try_reserve(MemoryClass::Management, 1024)
        .expect("management capacity is reserved");
    let control = tokio::time::timeout(
        Duration::from_secs(5),
        executor.run_cpu(CpuClass::Control, control_reservation, |_| 7_u32),
    )
    .await
    .expect("control execution completes while bulk execution is occupied")
    .expect("the control job runs");
    assert_eq!(control, 7);
    assert_eq!(executor.snapshot().bulk_cpu.running, 1);

    release_bulk
        .send(())
        .expect("the bulk job is still waiting");
    bulk.await.expect("the bulk job finishes");
}

#[tokio::test]
async fn a_job_dropped_while_queued_releases_its_reservation() {
    let executor = small_executor();
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let occupying = executor
        .try_reserve(MemoryClass::Relay, 1024)
        .expect("the relay class is empty");
    let occupied = executor.clone();
    let running = tokio::spawn(async move {
        occupied
            .run_cpu(CpuClass::Data, occupying, move |_| {
                let _ = released.blocking_recv();
            })
            .await
            .expect("the occupying job runs")
    });
    while executor.snapshot().data_cpu.running == 0 {
        tokio::task::yield_now().await;
    }

    let queued_bytes = 4096;
    let queued = executor
        .try_reserve(MemoryClass::Relay, queued_bytes)
        .expect("the relay class has room");
    let reserved_before = executor.snapshot().relay_memory.reserved_bytes;
    let waiting = executor.clone();
    let cancelled = tokio::spawn(async move {
        waiting
            .run_cpu(CpuClass::Data, queued, |_| ())
            .await
            .expect("the queued job either runs or is dropped")
    });
    while executor.snapshot().data_cpu.pending == 0 {
        tokio::task::yield_now().await;
    }
    cancelled.abort();
    let _ = cancelled.await;
    while executor.snapshot().relay_memory.reserved_bytes == reserved_before {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        executor.snapshot().relay_memory.reserved_bytes,
        reserved_before
            .checked_sub(queued_bytes)
            .expect("the queued job's charge was part of the total"),
    );

    release
        .send(())
        .expect("the occupying job is still waiting");
    running.await.expect("the occupying job finishes");
}

#[tokio::test]
async fn a_running_job_observes_cancellation_and_keeps_its_charge_until_it_exits() {
    let executor = small_executor();
    let observed = StdArc::new(AtomicUsize::new(0));
    let job_observed = StdArc::clone(&observed);
    let (exited, has_exited) = tokio::sync::oneshot::channel::<u64>();
    let reservation = executor
        .try_reserve(MemoryClass::Relay, 8192)
        .expect("the relay class is empty");
    let running = executor.clone();
    let waiting_snapshot = executor.clone();
    let task = tokio::spawn(async move {
        running
            .run_cpu(CpuClass::Data, reservation, move |cancellation| {
                job_observed.store(1, Ordering::Release);
                while cancellation.check().is_ok() {
                    std::thread::yield_now();
                }
                job_observed.store(2, Ordering::Release);
                let _ = exited.send(waiting_snapshot.snapshot().relay_memory.reserved_bytes);
            })
            .await
            .expect("the job runs")
    });
    while observed.load(Ordering::Acquire) == 0 {
        tokio::task::yield_now().await;
    }
    task.abort();
    let charged_at_exit = has_exited.await.expect("the job reports as it exits");
    assert_eq!(
        charged_at_exit, 8192,
        "a running job keeps its charge until it actually exits"
    );
    while executor.snapshot().relay_memory.reserved_bytes != 0 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn a_full_wait_queue_is_typed_backpressure_rather_than_unbounded_growth() {
    let executor = small_executor();
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let occupying = executor
        .try_reserve(MemoryClass::Relay, 1024)
        .expect("the relay class is empty");
    let occupied = executor.clone();
    let running = tokio::spawn(async move {
        occupied
            .run_cpu(CpuClass::Data, occupying, move |_| {
                let _ = released.blocking_recv();
            })
            .await
            .expect("the occupying job runs")
    });
    while executor.snapshot().data_cpu.running == 0 {
        tokio::task::yield_now().await;
    }

    let queued = executor
        .try_reserve(MemoryClass::Relay, 1024)
        .expect("the relay class has room");
    let waiting = executor.clone();
    let pending =
        tokio::spawn(async move { waiting.run_cpu(CpuClass::Data, queued, |_| ()).await });
    while executor.snapshot().data_cpu.pending == 0 {
        tokio::task::yield_now().await;
    }

    let refused = executor
        .try_reserve(MemoryClass::Relay, 1024)
        .expect("the relay class has room");
    let error = executor
        .run_cpu(CpuClass::Data, refused, |_| ())
        .await
        .expect_err("the single wait slot is already taken");
    assert!(
        matches!(error, crate::ExecutionError::QueueFull { class, pending } if class == "data_cpu" && pending == 1),
        "expected typed queue backpressure, got {error}"
    );

    release
        .send(())
        .expect("the occupying job is still waiting");
    running.await.expect("the occupying job finishes");
    pending
        .await
        .expect("the queued job is joined")
        .expect("the queued job runs once a worker frees up");
}

#[tokio::test]
async fn consensus_storage_runs_its_jobs_in_admission_order() {
    let executor = small_executor();
    let order = StdArc::new(parking_lot::Mutex::new(Vec::new()));
    let mut handles = Vec::new();
    for index in 0..8_usize {
        let reservation = executor
            .try_reserve(MemoryClass::Commands, 1024)
            .expect("the commands class has room");
        let executor = executor.clone();
        let order = StdArc::clone(&order);
        handles.push(
            executor
                .run_storage(StorageClass::Consensus, reservation, move |_| {
                    order.lock().push(index);
                })
                .await,
        );
    }
    for handle in handles {
        handle.expect("every ordered storage job runs");
    }
    assert_eq!(*order.lock(), (0..8).collect::<Vec<_>>());
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
    assert!(executor.snapshot().commands_memory.reserved_bytes >= 4096);
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
