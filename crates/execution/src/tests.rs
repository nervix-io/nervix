use std::{
    io::Write as _,
    num::{NonZeroU32, NonZeroUsize},
};

use meticulous::OptionExt as _;
use ubyte::ByteUnit;

use crate::{
    ADMISSION_TOKEN_BYTES, AdmissionError, BudgetedBuffer, BufferGrowthError, BufferLimitExceeded,
    ExecutionConfig, ExecutionConfigError, Executor, MemoryBudgets, MemoryClass, OperationLimits,
    StorageClass, WorkerCounts,
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
            connector_storage: crate::ConnectorStorageWorkers::resolve(Some(one()))
                .expect("one worker fits the primitive permit bound"),
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
    assert_eq!(snapshot.connector_storage.workers, 2);
}

#[test]
fn request_input_capacity_is_validated_before_pools_are_constructed() {
    for budget in [0, u64::from(u32::MAX) + 1] {
        let mut config = ExecutionConfig::default();
        config.budgets.intake = ByteUnit::Byte(budget);
        let Err(error) = Executor::new(config) else {
            panic!("an unenforceable intake budget must refuse executor construction");
        };
        match (budget, error.current_context()) {
            (0, ExecutionConfigError::EmptyBudget { class: "intake" }) => {}
            (
                requested,
                ExecutionConfigError::BudgetTooLarge {
                    class: "intake",
                    budget,
                },
            ) if *budget == requested => {}
            _ => panic!("the refusal must identify the intake budget: {error:#}"),
        }
    }
}

/// Each configured pool and both equally sized derived read pools count in the total.
#[test]
fn the_memory_total_counts_every_class_once() {
    let budgets = MemoryBudgets {
        intake: ByteUnit::Byte(128),
        management: ByteUnit::Byte(1),
        commands: ByteUnit::Byte(2),
        relay: ByteUnit::Byte(4),
        state_read: ByteUnit::Byte(4),
        state_read_cache: ByteUnit::Byte(4),
        bulk: ByteUnit::Byte(8),
        bulk_window: ByteUnit::Byte(16),
        restore_metadata: ByteUnit::Byte(32),
        credentials: ByteUnit::Byte(64),
    };

    assert_eq!(budgets.total(), Some(ByteUnit::Byte(263)));
}

/// Budgets whose sum does not fit a byte count have no total, rather than one that wrapped.
#[test]
fn memory_budgets_beyond_a_byte_count_have_no_total() {
    let budgets = MemoryBudgets {
        relay: ByteUnit::Byte(u64::MAX),
        ..MemoryBudgets::default()
    };

    assert_eq!(budgets.total(), None);
}

/// The startup preflight refuses a memory limit below the total, so the total is what the executor
/// can reserve: the capacity of every memory class together, 2,851 MiB by default.
#[nervix_primitives::test]
async fn the_default_memory_total_is_what_the_executor_can_reserve() {
    use meticulous::ResultExt as _;

    let config = ExecutionConfig::default();
    let executor = Executor::new(config).assured("the default budgets hold the default limits");
    let mut reservable = 0_u64;
    for class in [
        MemoryClass::Intake,
        MemoryClass::Management,
        MemoryClass::Commands,
        MemoryClass::Relay,
        MemoryClass::StateRead,
        MemoryClass::StateReadCache,
        MemoryClass::Bulk,
        MemoryClass::BulkWindow,
        MemoryClass::RestoreMetadata,
        MemoryClass::Credentials,
    ] {
        reservable = reservable
            .checked_add(executor.memory(class).capacity_bytes)
            .assured("ten budgets that each fit a u32 permit count add up within a u64");
    }

    assert_eq!(config.budgets.total(), Some(ByteUnit::Byte(reservable)));
    assert_eq!(reservable, ByteUnit::Mebibyte(2851).as_u64());
}

#[nervix_primitives::test]
async fn active_reads_retained_reads_and_live_relays_have_independent_capacity() {
    let executor = small_executor();
    let capacity = executor.memory(MemoryClass::Relay).capacity_bytes;
    assert_eq!(
        executor.memory(MemoryClass::StateRead).capacity_bytes,
        capacity
    );
    let active = executor
        .try_reserve(MemoryClass::StateRead, capacity)
        .expect("active source views can fill their own pool");
    assert_eq!(
        executor.memory(MemoryClass::StateReadCache).capacity_bytes,
        capacity
    );
    let cached = executor
        .try_reserve(MemoryClass::StateReadCache, capacity)
        .expect("the cache can fill its own pool");
    let exhausted = executor
        .try_reserve(MemoryClass::StateReadCache, 1)
        .expect_err("optional retention refuses a full pool without waiting");
    assert!(matches!(
        exhausted.current_context(),
        AdmissionError::BudgetExhausted { .. }
    ));
    let live = executor
        .try_reserve(MemoryClass::Relay, capacity)
        .expect("a full cache leaves all live relay capacity available");
    drop(cached);
    assert_eq!(
        executor.memory(MemoryClass::StateReadCache).reserved_bytes,
        0
    );
    assert_eq!(executor.memory(MemoryClass::Relay).reserved_bytes, capacity);
    drop(active);
    assert_eq!(executor.memory(MemoryClass::StateRead).reserved_bytes, 0);
    drop(live);
    assert_eq!(executor.memory(MemoryClass::Relay).reserved_bytes, 0);
}

#[test]
fn charged_bytes_keep_their_own_size_and_the_room_their_only_holder_keeps() {
    let executor = Executor::default();
    let operation = executor
        .try_reserve(MemoryClass::Relay, 10_000)
        .expect("an empty relay budget takes a small operation");
    let mut body = crate::ChargedBytes::from_owned(vec![7; 100], operation);
    let shared = body.clone();

    body.keep_charge_for(250);
    assert_eq!(
        executor.memory(MemoryClass::Relay).reserved_bytes,
        10_000,
        "an allocation another handle shares keeps its whole charge"
    );

    drop(shared);
    body.keep_charge_for(250);
    assert_eq!(executor.memory(MemoryClass::Relay).reserved_bytes, 350);
    assert_eq!(&*body, &[7; 100][..]);

    body.keep_charge_for(u64::MAX);
    body.keep_charge_for(1000);
    assert_eq!(
        executor.memory(MemoryClass::Relay).reserved_bytes,
        350,
        "keeping more room than the charge holds returns nothing and takes nothing"
    );
    drop(body);
    assert_eq!(executor.memory(MemoryClass::Relay).reserved_bytes, 0);
}

#[test]
fn connector_worker_capacity_is_a_validated_node_capability() {
    use nervix_models::{NodeCapabilities, SettingClass as _, SettingDefinition as _};

    use crate::ConnectorStorageWorkers;

    let default = ConnectorStorageWorkers::resolve(None).expect("the default capacity is valid");
    let explicit = ConnectorStorageWorkers::resolve(Some(NonZeroUsize::MIN))
        .expect("one worker fits the primitive permit bound");
    let copied = explicit;
    assert_eq!(copied, explicit);
    assert_eq!(default.value().get(), 2);
    assert_eq!(explicit.value().get(), 1);
    assert_eq!(
        ConnectorStorageWorkers::NAME,
        "execution-connector-storage-workers"
    );
    assert_eq!(
        <ConnectorStorageWorkers as nervix_models::SettingDefinition>::Class::NAME,
        NodeCapabilities::NAME
    );
    let error = ConnectorStorageWorkers::resolve(Some(NonZeroUsize::MAX))
        .expect_err("a capability claim must fit the primitive's permit representation");
    assert_eq!(
        error.current_context(),
        &ExecutionConfigError::WorkerCountTooLarge {
            class: "connector_storage",
            workers: usize::MAX,
            maximum: nervix_primitives::sync::Semaphore::MAX_PERMITS,
        }
    );
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

/// An open bulk stream holds its receive window while its reader's chunks are charged to bulk
/// buffers. With every window of the node held, a chunk still finds room, and with every bulk
/// buffer held, a waiting stream still gets the window another stream returns.
#[nervix_primitives::test]
async fn bulk_receive_windows_are_admitted_independently_of_bulk_buffers() {
    use meticulous::ResultExt as _;

    let executor = small_executor();
    let snapshot = executor.snapshot();
    assert_eq!(
        snapshot.bulk_window_memory.capacity_bytes,
        ByteUnit::Mebibyte(16).as_u64()
    );
    assert_eq!(MemoryClass::BulkWindow.as_str(), "bulk_window");
    let windows = executor
        .try_reserve(
            MemoryClass::BulkWindow,
            snapshot.bulk_window_memory.capacity_bytes,
        )
        .assured("every window of the class is available");
    let chunk = executor
        .try_reserve(MemoryClass::Bulk, snapshot.bulk_memory.capacity_bytes)
        .assured("bulk buffers have independent admission");
    let error = executor
        .try_reserve(MemoryClass::BulkWindow, 1)
        .err()
        .assured("the occupied window class refuses another window");
    assert_eq!(
        *error.current_context(),
        AdmissionError::BudgetExhausted {
            class: "bulk_window",
            requested: 1
        }
    );
    drop(windows);
    assert_eq!(executor.snapshot().bulk_window_memory.reserved_bytes, 0);
    assert_eq!(
        executor.snapshot().bulk_memory.reserved_bytes,
        snapshot.bulk_memory.capacity_bytes
    );
    let window = executor
        .reserve(MemoryClass::BulkWindow, ByteUnit::Mebibyte(1).as_u64())
        .await
        .assured("a returned window admits the next stream although bulk buffers are full");
    drop(window);
    drop(chunk);
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
async fn an_empty_bulk_window_budget_fails_to_start() {
    let error = Executor::new(ExecutionConfig {
        budgets: MemoryBudgets {
            bulk_window: ByteUnit::Byte(0),
            ..MemoryBudgets::default()
        },
        ..ExecutionConfig::default()
    })
    .expect_err("a budget that backs no bulk stream's window is rejected");
    assert_eq!(
        *error.current_context(),
        ExecutionConfigError::EmptyBudget {
            class: "bulk_window"
        }
    );
}

/// Every consensus vote and metadata read or write reserves four management events, 272 KiB with
/// the default limits. A management budget that holds one event but not that working set would let
/// the node open its stores and then fail every one of those operations, so the executor refuses
/// it first.
#[nervix_primitives::test]
async fn a_management_budget_below_one_consensus_vote_fails_to_start() {
    let error = Executor::new(ExecutionConfig {
        budgets: MemoryBudgets {
            management: ByteUnit::Kibibyte(128),
            ..MemoryBudgets::default()
        },
        ..ExecutionConfig::default()
    })
    .expect_err("a management budget below one consensus vote is rejected");
    assert_eq!(
        *error.current_context(),
        ExecutionConfigError::BudgetBelowOperation {
            class: "management",
            operation: "consensus vote or metadata write",
            budget: ByteUnit::Kibibyte(128).as_u64(),
            required: ByteUnit::Kibibyte(272).as_u64(),
        }
    );
}

/// A management budget of exactly one consensus vote's working set is the smallest one accepted,
/// and its whole class holds that working set.
#[nervix_primitives::test]
async fn a_management_budget_of_one_consensus_vote_starts_and_holds_it() {
    let limits = OperationLimits::default();
    let working_set = limits
        .management_storage_working_bytes()
        .expect("the default management event times the storage multiplier fits a u64");
    assert_eq!(working_set, ByteUnit::Kibibyte(272).as_u64());
    let executor = Executor::new(ExecutionConfig {
        budgets: MemoryBudgets {
            management: ByteUnit::Byte(working_set),
            ..MemoryBudgets::default()
        },
        limits,
        ..ExecutionConfig::default()
    })
    .expect("a management budget of one consensus vote is accepted");
    let reservation = executor
        .try_reserve(MemoryClass::Management, working_set)
        .expect("the whole management class holds one consensus vote");
    assert_eq!(
        executor.snapshot().management_memory.reserved_bytes,
        working_set
    );
    drop(reservation);
    assert_eq!(executor.snapshot().management_memory.reserved_bytes, 0);
}

/// A management event so large that its storage working set has no byte count is a contradiction
/// the executor reports, not a reservation it wraps around.
#[nervix_primitives::test]
async fn a_management_storage_working_set_beyond_a_byte_count_fails_to_start() {
    let error = Executor::new(ExecutionConfig {
        limits: OperationLimits {
            management_event_bytes: ByteUnit::Byte(u64::MAX),
            ..OperationLimits::default()
        },
        ..ExecutionConfig::default()
    })
    .expect_err("an unaddressable management working set is rejected");
    assert_eq!(
        *error.current_context(),
        ExecutionConfigError::UnaddressableManagementStorage
    );
    assert_eq!(
        error.current_context().to_string(),
        "the configured management storage limits do not add up to an addressable size"
    );
}

#[nervix_primitives::test]
async fn a_credentials_budget_below_one_password_hash_fails_to_start() {
    let error = Executor::new(ExecutionConfig {
        budgets: MemoryBudgets {
            credentials: ByteUnit::Byte(63),
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
            budget: ByteUnit::Byte(63).as_u64(),
            required: ByteUnit::Byte(64).as_u64(),
        }
    );
}

/// A follower charges every decoded replication batch until its Raft core answers it, and the
/// durable append of those entries takes one command state storage working set from the same class.
/// A budget that cannot hold the whole resident window beside that working set would let a full
/// window leave no room for the append that releases it, so the node refuses to start instead of
/// deadlocking on its first catch-up.
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
            operation: "resident replication batches beside one command state storage working set",
            budget: ByteUnit::Mebibyte(16).as_u64(),
            required: ByteUnit::Mebibyte(32).as_u64(),
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
            operation: "resident replication batches beside one command state storage working set",
            budget: ByteUnit::Mebibyte(15).as_u64(),
            required: ByteUnit::Mebibyte(24).as_u64(),
        }
    );
}

/// A commands budget that holds a follower's full resident window, or one command state storage
/// working set, but not both, lets a follower whose window is full wait for room only its own
/// window releases, so the node refuses to start.
#[nervix_primitives::test]
async fn a_commands_budget_holding_the_window_or_one_state_write_but_not_both_fails_to_start() {
    let error = Executor::new(ExecutionConfig {
        budgets: MemoryBudgets {
            commands: ByteUnit::Mebibyte(20),
            ..MemoryBudgets::default()
        },
        ..ExecutionConfig::default()
    })
    .expect_err("a commands budget below the window beside one state write is rejected");
    assert_eq!(
        *error.current_context(),
        ExecutionConfigError::BudgetBelowOperation {
            class: "commands",
            operation: "resident replication batches beside one command state storage working set",
            budget: ByteUnit::Mebibyte(20).as_u64(),
            required: ByteUnit::Mebibyte(24).as_u64(),
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

/// A job that carries state for its caller hands it back when the caller withdraws it before a
/// worker takes it, keeping the state and the charge the caller gave it, and hands it back with its
/// outcome when it runs.
#[nervix_primitives::test]
async fn a_carrying_job_hands_its_state_back_when_withdrawn_and_when_it_returns() {
    let executor = small_executor();
    let held = crate::workers::HeldWorker::new(executor.cpu_pool(crate::CpuClass::Data));
    let mut home: Option<Vec<u8>> = None;
    let charge = executor
        .reserve(MemoryClass::Relay, 1024)
        .await
        .expect("the operation fits");
    {
        let mut waiting = Box::pin(executor.run_cpu_carrying(
            crate::CpuClass::Data,
            charge,
            vec![7_u8],
            &mut home,
            |mut state, _charge, _| {
                state.push(8);
                (state, 1)
            },
        ));
        let first_poll = std::future::poll_fn(|context| {
            std::task::Poll::Ready(std::future::Future::poll(waiting.as_mut(), context))
        })
        .await;
        assert!(
            first_poll.is_pending(),
            "the carrying job waits for the busy worker instead of being refused"
        );
        assert_eq!(executor.snapshot().data_cpu.waiting, 1);
        assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 1024);
    }
    assert_eq!(
        home,
        Some(vec![7]),
        "a job withdrawn before a worker took it hands its state back unchanged"
    );
    assert_eq!(executor.snapshot().data_cpu.waiting, 0);
    assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 0);
    drop(held);

    let state = home
        .take()
        .expect("the withdrawn job handed its state back");
    let charge = executor
        .reserve(MemoryClass::Relay, 1024)
        .await
        .expect("the returned charge is reusable");
    let value = executor
        .run_cpu_carrying(
            crate::CpuClass::Data,
            charge,
            state,
            &mut home,
            |mut state, _charge, _| {
                state.push(8);
                (state, 2)
            },
        )
        .await
        .expect("the carrying job runs on the free worker");
    assert_eq!(value, 2);
    assert_eq!(
        home,
        Some(vec![7, 8]),
        "the job hands the state it carried back with its outcome"
    );
    let snapshot = executor.snapshot();
    assert_eq!(snapshot.relay_memory.reserved_bytes, 0);
    assert_eq!(snapshot.data_cpu.waiting, 0);
    assert_eq!(snapshot.data_cpu.running, 0);
    assert_eq!(snapshot.data_cpu.admitted, 2);
    assert_eq!(
        snapshot.data_cpu.completed, 1,
        "the withdrawn job never reached a worker"
    );
}

/// A carrying job that finds every worker busy waits its turn behind the jobs that asked before it,
/// holding no worker meanwhile, and the worker that frees takes those jobs first. The pool's owner
/// hold keeps the worker busy without parking a thread.
#[nervix_primitives::test]
async fn a_carrying_job_waits_its_turn_without_holding_a_worker() {
    let executor = small_executor();
    let held = crate::workers::HeldWorker::new(executor.cpu_pool(crate::CpuClass::Data));
    let (completed, mut observed) = nervix_primitives::sync::mpsc::unbounded_channel();
    let first_charge = executor
        .try_reserve(MemoryClass::Relay, 1)
        .expect("the untouched relay class has room for the first job");
    let first_submitter = executor.clone();
    let first_completed = completed.clone();
    let first = nervix_primitives::task::spawn(async move {
        first_submitter
            .run_cpu(crate::CpuClass::Data, first_charge, move |_charge, _| {
                first_completed
                    .send("job")
                    .expect("the test holds the completion receiver");
            })
            .await
    });
    while executor.snapshot().data_cpu.waiting < 1 {
        nervix_primitives::task::consume_budget().await;
        nervix_primitives::task::yield_now().await;
    }
    let carried_charge = executor
        .try_reserve(MemoryClass::Relay, 1)
        .expect("the relay class has room for the carrying job");
    let mut home: Option<&'static str> = None;
    let mut carrying = Box::pin(executor.run_cpu_carrying(
        crate::CpuClass::Data,
        carried_charge,
        "session",
        &mut home,
        move |state, _charge, _| {
            completed
                .send("carried")
                .expect("the test holds the completion receiver");
            (state, ())
        },
    ));
    let first_poll = std::future::poll_fn(|context| {
        std::task::Poll::Ready(std::future::Future::poll(carrying.as_mut(), context))
    })
    .await;
    assert!(
        first_poll.is_pending(),
        "a carrying job waits for the busy worker instead of being refused"
    );
    let busy = executor.snapshot().data_cpu;
    assert_eq!(busy.waiting, 2);
    assert_eq!(busy.running, 1, "a waiting carrying job holds no worker");
    drop(held);

    carrying
        .await
        .expect("the carrying job runs once the job before it ran");
    first
        .await
        .expect("the first job is joined")
        .expect("the first job ran before the carrying job");
    assert_eq!(home, Some("session"));
    assert_eq!(observed.recv().await, Some("job"));
    assert_eq!(observed.recv().await, Some("carried"));
    let snapshot = executor.snapshot();
    assert_eq!(snapshot.data_cpu.waiting, 0);
    assert_eq!(snapshot.data_cpu.running, 0);
    assert_eq!(snapshot.data_cpu.completed, 2);
    assert_eq!(snapshot.relay_memory.reserved_bytes, 0);
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

/// A job that writes `input` into a budgeted buffer under its charge, and reports the charge it
/// needs when its class has no room for the growth, as every growing writer does.
// Its users are the growing job's ordinary check and its Shuttle check; a Turmoil build without
// Shuttle compiles neither.
#[cfg(any(not(feature = "turmoil"), feature = "shuttle"))]
fn write_growing(
    input: Vec<u8>,
    charge: crate::Reservation,
) -> crate::Attempt<crate::ChargedBytes, Vec<u8>> {
    let mut buffer = BudgetedBuffer::with_limit(charge, 1024 * 1024);
    if buffer.write_all(&input).is_ok() {
        return crate::Attempt::Finished(crate::ChargedBytes::from_buffer(buffer));
    }
    let needed = buffer
        .shortfall()
        .expect("the only refusal this writer meets is a class without room");
    crate::Attempt::Short { input, needed }
}

/// A growing job whose class has no room for its growth gives back everything it holds before it
/// waits for the charge it needs: a charge queued behind its attempt is granted the room the
/// attempt held, and the job runs again under its larger charge once room frees.
// The first attempt parks its worker's thread on a blocking channel until a charge is queued
// behind it. Turmoil's simulated CPU strategy runs every job on the one simulated host thread,
// which that park would stop, so this check runs in the ordinary build only.
#[cfg(not(feature = "turmoil"))]
#[nervix_primitives::test]
async fn a_growing_job_that_ran_short_gives_back_its_charge_waits_and_runs_again() {
    use nervix_primitives::sync::{
        StdArc,
        atomic::{AtomicUsize, Ordering},
    };

    let executor = small_executor();
    let capacity = executor.memory(MemoryClass::Bulk).capacity_bytes;
    let first_charge = 4096;
    let held = executor
        .try_reserve(
            MemoryClass::Bulk,
            capacity
                .checked_sub(first_charge)
                .expect("the bulk class is larger than one first charge"),
        )
        .expect("the empty bulk class holds all but one first charge");
    let attempts = StdArc::new(AtomicUsize::new(0));
    let counted = StdArc::clone(&attempts);
    let (started, attempt_started) = nervix_primitives::sync::oneshot::channel();
    let (proceed, attempt_proceeds) = nervix_primitives::sync::blocking::mpsc::channel::<()>();
    let mut gate = Some((started, attempt_proceeds));
    let growing = executor.clone();
    let task = nervix_primitives::task::spawn(async move {
        growing
            .run_cpu_growing(
                crate::CpuClass::Bulk,
                MemoryClass::Bulk,
                first_charge,
                vec![7_u8; 64 * 1024],
                move |input, charge, _cancellation| {
                    counted.fetch_add(1, Ordering::AcqRel);
                    // The first attempt holds its charge until a charge is queued behind it.
                    if let Some((started, proceeds)) = gate.take() {
                        started
                            .send(())
                            .expect("the test waits for the first attempt");
                        proceeds
                            .recv()
                            .expect("the test lets the first attempt go on");
                    }
                    write_growing(input, charge)
                },
            )
            .await
    });
    attempt_started
        .await
        .expect("the first attempt runs under the one free charge");
    let mut waiting = Box::pin(executor.reserve(MemoryClass::Bulk, first_charge));
    let first_poll = std::future::poll_fn(|context| {
        std::task::Poll::Ready(std::future::Future::poll(waiting.as_mut(), context))
    })
    .await;
    assert!(
        first_poll.is_pending(),
        "the class has no room while the attempt holds its charge"
    );
    proceed
        .send(())
        .expect("the first attempt waits for the test");
    let granted = nervix_primitives::time::timeout(std::time::Duration::from_secs(60), waiting)
        .await
        .expect("a job that ran short gives back its charge rather than waiting while it holds it")
        .expect("the waiter's charge is no larger than the class");
    assert!(
        !task.is_finished(),
        "a job that ran short waits for its larger charge instead of failing"
    );
    assert_eq!(attempts.load(Ordering::Acquire), 1);
    drop(held);
    drop(granted);
    let written = task
        .await
        .expect("the growing job's task is joined")
        .expect("the job runs again once its class has room");
    assert_eq!(&*written, vec![7_u8; 64 * 1024].as_slice());
    assert_eq!(attempts.load(Ordering::Acquire), 2);
    drop(written);
    assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, 0);
}

/// A job whose need is larger than its whole class fails rather than waiting for room that could
/// never exist.
#[nervix_primitives::test]
async fn a_growing_job_that_needs_more_than_its_class_fails() {
    let executor = small_executor();
    let capacity = executor.memory(MemoryClass::Bulk).capacity_bytes;
    let failure = executor
        .run_cpu_growing(
            crate::CpuClass::Bulk,
            MemoryClass::Bulk,
            4096,
            (),
            move |(), _charge, _cancellation| crate::Attempt::<(), ()>::Short {
                input: (),
                needed: capacity + 1,
            },
        )
        .await
        .expect_err("a need beyond the class cannot be granted");
    assert!(matches!(
        failure.current_context(),
        crate::ExecutionFailure::Admission
    ));
    assert!(matches!(
        failure.downcast_ref::<AdmissionError>(),
        Some(AdmissionError::ExceedsBudget { .. })
    ));
    assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, 0);
}

/// An operation waits once for its allowance, while it holds nothing, and draws every charge of
/// the class from it without waiting: a charge the allowance has no room for fails at once with
/// the share the whole operation needs.
#[nervix_primitives::test]
async fn an_allowance_draws_without_waiting_and_reports_the_operation_s_whole_need() {
    let executor = small_executor();
    let allowance = executor
        .reserve_allowance(MemoryClass::Bulk, 8192)
        .await
        .expect("the empty bulk class grants the share");
    assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, 8192);
    let first = allowance
        .reserve(MemoryClass::Bulk, 4096)
        .await
        .expect("the share has room for the first charge");
    let second = allowance
        .try_reserve(MemoryClass::Bulk, 4096)
        .expect("the share has room for the second charge");
    let refused = {
        let mut refusing = std::pin::pin!(allowance.reserve(MemoryClass::Bulk, 1));
        let polled = std::future::poll_fn(|context| {
            std::task::Poll::Ready(std::future::Future::poll(refusing.as_mut(), context))
        })
        .await;
        let std::task::Poll::Ready(result) = polled else {
            panic!("a charge within an allowance never waits");
        };
        result.expect_err("the full share has no room for another byte")
    };
    assert_eq!(crate::Shortfall::of(&refused), Some(8193));
    assert_eq!(
        executor.memory(MemoryClass::Bulk).reserved_bytes,
        8192,
        "draws come out of the share, not out of the class"
    );
    // Other classes are the executor's own.
    let other = allowance
        .reserve(MemoryClass::Relay, 4096)
        .await
        .expect("the relay class has room");
    assert_eq!(executor.memory(MemoryClass::Relay).reserved_bytes, 4096);
    drop(other);
    drop(first);
    drop(second);
    drop(allowance);
    assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, 0);
}

/// An operation that ran short of its share runs again within the share it needed, even when it
/// reported the shortfall as an answer of its own rather than as a failure.
#[nervix_primitives::test]
async fn an_operation_short_of_its_share_runs_again_within_the_share_it_needed() {
    let executor = small_executor();
    let mut attempts = Vec::new();
    let answer = executor
        .within_allowance(MemoryClass::Bulk, 4096, |bulk| {
            attempts.push(bulk.memory(MemoryClass::Bulk).reserved_bytes);
            async move {
                // The operation turns a charge its share has no room for into a degraded answer.
                let degraded = bulk.reserve(MemoryClass::Bulk, 16_384).await.is_err();
                Ok::<_, error_stack::Report<AdmissionError>>(!degraded)
            }
        })
        .await
        .expect("the class holds the larger share")
        .expect("the operation never fails");
    assert!(
        answer,
        "the attempt within the share it needed answers in full"
    );
    assert_eq!(
        attempts,
        vec![4096, 16_384],
        "the operation runs again within the share it was found to need"
    );
    assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, 0);
}

/// An operation that grows one step at a time runs again only a few times: each share it runs
/// again within at least doubles, rather than covering only the step where it ran short, which
/// would end the next attempt one step later and run the operation once per step.
#[nervix_primitives::test]
async fn an_operation_growing_step_by_step_runs_again_a_logarithmic_number_of_times() {
    const STEP: u64 = 1024;
    const STEPS: u64 = 64;
    let executor = small_executor();
    let mut attempts = 0_u32;
    let drawn = executor
        .within_allowance(MemoryClass::Bulk, STEP, |bulk| {
            attempts += 1;
            async move {
                let mut charges = Vec::new();
                for _ in 0..STEPS {
                    charges.push(bulk.reserve(MemoryClass::Bulk, STEP).await?);
                }
                Ok::<_, error_stack::Report<AdmissionError>>(charges.len())
            }
        })
        .await
        .expect("the class holds the whole operation")
        .expect("the operation finishes within the share it needs");
    assert_eq!(u64::try_from(drawn).expect("a step count fits u64"), STEPS);
    // Shares of 1, 2, 4, 8, 16, 32 and 64 steps.
    assert_eq!(
        attempts, 7,
        "the share doubles from one attempt to the next"
    );
    assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, 0);
}

/// A charge drawn from an allowance keeps the allowance's share of the class held for as long as
/// it lives, and growing it past the share reports the share the operation needs.
#[nervix_primitives::test]
async fn a_charge_drawn_from_an_allowance_keeps_its_share_and_reports_growth_beyond_it() {
    let executor = small_executor();
    let allowance = executor
        .reserve_allowance(MemoryClass::Bulk, 8192)
        .await
        .expect("the empty bulk class grants the share");
    let mut drawn = allowance
        .reserve(MemoryClass::Bulk, 4096)
        .await
        .expect("the share has room");
    drop(allowance);
    assert_eq!(
        executor.memory(MemoryClass::Bulk).reserved_bytes,
        8192,
        "the share stays held while a charge drawn from it lives"
    );
    drawn
        .grow_to(8192)
        .expect("the share has room to grow the charge to its whole size");
    let refused = drawn
        .grow_to(12_288)
        .expect_err("the share has no room to grow further");
    assert_eq!(crate::Shortfall::of(&refused), Some(12_288));
    drop(drawn);
    assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, 0);
}

/// An optional extra an operation goes without, a growth its share has no room for now, never
/// makes the operation run again: its own answer without the extra stands.
#[nervix_primitives::test]
async fn an_optional_growth_its_share_has_no_room_for_never_runs_the_operation_again() {
    let executor = small_executor();
    let mut attempts = 0_u32;
    let went_without = executor
        .within_allowance(MemoryClass::Bulk, 8192, |bulk| {
            attempts += 1;
            async move {
                let mut charge = bulk.reserve(MemoryClass::Bulk, 4096).await?;
                charge
                    .try_grow_to(8192)
                    .expect("the share has room for the first extra");
                let refused = charge.try_grow_to(12_288).is_err();
                Ok::<_, error_stack::Report<AdmissionError>>(refused)
            }
        })
        .await
        .expect("the class grants the share")
        .expect("the operation answers");
    assert!(went_without, "the share has no room for the second extra");
    assert_eq!(
        attempts, 1,
        "an optional growth the share has no room for runs nothing again"
    );
    assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, 0);
}

/// A charge in a class without room for it outside any allowance still waits, and a growth
/// refusal there reports the charge's own target.
#[nervix_primitives::test]
async fn growth_outside_an_allowance_reports_its_own_target() {
    let executor = small_executor();
    let capacity = executor.memory(MemoryClass::Bulk).capacity_bytes;
    let _held = executor
        .try_reserve(MemoryClass::Bulk, capacity - 4096)
        .expect("the empty class holds all but one charge");
    let mut charge = executor
        .try_reserve(MemoryClass::Bulk, 4096)
        .expect("the class has room for one charge");
    let refused = charge
        .grow_to(8192)
        .expect_err("the full class has no room to grow");
    assert_eq!(crate::Shortfall::of(&refused), Some(8192));
}

/// An operation counted under a larger charge than it needs counts its own bytes once, keeps the
/// room it started with until it ends, and gives the rest back when it narrows.
#[nervix_primitives::test]
async fn a_growing_charge_counts_its_own_bytes_and_keeps_its_starting_room() {
    let executor = small_executor();
    let start = executor
        .reserve(MemoryClass::Bulk, 8192)
        .await
        .expect("the empty class grants the starting charge");
    let mut charge = crate::GrowingCharge::new(start);
    charge.grow_by(4096).expect("the starting charge covers it");
    charge.grow_by(4096).expect("the starting charge covers it");
    assert_eq!(charge.required(), 8192);
    assert_eq!(
        executor.memory(MemoryClass::Bulk).reserved_bytes,
        8192,
        "bytes the starting charge covers are not charged again"
    );
    charge.release(6144);
    assert_eq!(charge.required(), 2048);
    assert_eq!(
        executor.memory(MemoryClass::Bulk).reserved_bytes,
        8192,
        "the charge keeps the room it started with until it ends"
    );
    charge.grow_by(8192).expect("the class has room to grow");
    assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, 10_240);
    let narrowed = charge.into_reservation();
    assert_eq!(narrowed.bytes(), 10_240);
    drop(narrowed);
    assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, 0);
}

/// Busy workers are backpressure: a storage job that finds the class's one worker busy waits for
/// it, however many jobs already wait, and runs once it frees, whatever the job does.
#[nervix_primitives::test]
async fn a_storage_job_that_finds_every_worker_busy_waits_for_one_and_runs_once_it_frees() {
    let executor = small_executor();
    let (started, has_started) = nervix_primitives::sync::oneshot::channel();
    let (release, released) = nervix_primitives::sync::oneshot::channel::<()>();
    let occupying = executor
        .try_reserve(MemoryClass::Bulk, ADMISSION_TOKEN_BYTES)
        .expect("the untouched bulk class has room");
    let occupied = executor.clone();
    let running = nervix_primitives::task::spawn(async move {
        occupied
            .run_storage(StorageClass::Filesystem, occupying, move |_charge, _| {
                started
                    .send(())
                    .expect("the test waits for the occupying job");
                released
                    .blocking_recv()
                    .expect("the test releases the occupying job before it exits");
            })
            .await
            .expect("the occupying job runs");
    });
    has_started
        .await
        .expect("the occupying job reports once it holds the one worker");
    let first_charge = executor
        .try_reserve(MemoryClass::Bulk, ADMISSION_TOKEN_BYTES)
        .expect("the bulk class has room for the first waiting job");
    let first_submitter = executor.clone();
    let first = nervix_primitives::task::spawn(async move {
        first_submitter
            .run_storage(StorageClass::Filesystem, first_charge, |_charge, _| ())
            .await
    });
    while executor.snapshot().filesystem_storage.waiting < 1 {
        nervix_primitives::task::consume_budget().await;
        nervix_primitives::task::yield_now().await;
    }

    let second_charge = executor
        .try_reserve(MemoryClass::Bulk, ADMISSION_TOKEN_BYTES)
        .expect("the bulk class has room for the second waiting job");
    let mut second = Box::pin(executor.run_storage(
        StorageClass::Filesystem,
        second_charge,
        |_charge, _| "waited",
    ));
    let first_poll = std::future::poll_fn(|context| {
        std::task::Poll::Ready(std::future::Future::poll(second.as_mut(), context))
    })
    .await;
    assert!(
        first_poll.is_pending(),
        "a job behind a waiting job waits too instead of being refused"
    );
    assert_eq!(
        executor.snapshot().filesystem_storage.waiting,
        2,
        "every job that found the worker busy counts as waiting"
    );
    release
        .send(())
        .expect("the occupying job holds its worker until the test releases it");

    running.await.expect("the occupying job is joined");
    first
        .await
        .expect("the first waiting job is joined")
        .expect("the first waiting job runs once the worker frees");
    let waited = second
        .await
        .expect("the second waiting job runs once the worker takes it");
    assert_eq!(waited, "waited");
    let snapshot = executor.snapshot();
    assert_eq!(
        snapshot.filesystem_storage.waiting, 0,
        "a job a worker took no longer counts as waiting"
    );
    assert_eq!(snapshot.filesystem_storage.completed, 3);
    assert_eq!(snapshot.filesystem_storage.running, 0);
    assert_eq!(snapshot.bulk_memory.reserved_bytes, 0);
}

/// The same for a CPU class: a job that finds the class's one worker busy waits for it, behind
/// every job that asked before it, and runs once it frees. The pool's owner hold keeps the worker
/// busy without parking a thread, so the check also completes on Turmoil's simulated scheduler.
#[nervix_primitives::test]
async fn a_cpu_job_that_finds_every_worker_busy_waits_for_one_and_runs_once_it_frees() {
    let executor = small_executor();
    let held = crate::workers::HeldWorker::new(executor.cpu_pool(crate::CpuClass::Data));
    let first_charge = executor
        .try_reserve(MemoryClass::Relay, 1)
        .expect("the untouched relay class has room for the first waiting job");
    let first_submitter = executor.clone();
    let first = nervix_primitives::task::spawn(async move {
        first_submitter
            .run_cpu(crate::CpuClass::Data, first_charge, |_charge, _| ())
            .await
    });
    while executor.snapshot().data_cpu.waiting < 1 {
        nervix_primitives::task::consume_budget().await;
        nervix_primitives::task::yield_now().await;
    }

    let second_charge = executor
        .try_reserve(MemoryClass::Relay, 1)
        .expect("the relay class has room for the second waiting job");
    let mut second =
        Box::pin(executor.run_cpu(crate::CpuClass::Data, second_charge, |_charge, _| 7));
    let first_poll = std::future::poll_fn(|context| {
        std::task::Poll::Ready(std::future::Future::poll(second.as_mut(), context))
    })
    .await;
    assert!(
        first_poll.is_pending(),
        "a job behind a waiting job waits too instead of being refused"
    );
    assert_eq!(executor.snapshot().data_cpu.waiting, 2);
    drop(held);

    first
        .await
        .expect("the first waiting job is joined")
        .expect("the first waiting job runs once the worker frees");
    let value = second
        .await
        .expect("the second waiting job runs once the worker takes it");
    assert_eq!(value, 7);
    let snapshot = executor.snapshot();
    assert_eq!(
        snapshot.data_cpu.completed, 2,
        "both waiting jobs ran; the owner hold is no job"
    );
    assert_eq!(snapshot.data_cpu.waiting, 0);
    assert_eq!(snapshot.relay_memory.reserved_bytes, 0);
}

/// A snapshot read by class name returns that class's own counts, for every storage class and
/// through the name a CPU or storage class widens into.
#[nervix_primitives::test]
async fn a_snapshot_reads_each_worker_class_by_its_name() {
    let executor = small_executor();
    let submitted = [
        StorageClass::Consensus,
        StorageClass::Filesystem,
        StorageClass::Filesystem,
        StorageClass::Connector,
        StorageClass::Connector,
        StorageClass::Connector,
    ];
    for class in submitted {
        nervix_primitives::task::consume_budget().await;
        let charge = executor
            .try_reserve(MemoryClass::Bulk, ADMISSION_TOKEN_BYTES)
            .expect("the bulk class has room for one job at a time");
        executor
            .run_storage(class, charge, |_charge, _| ())
            .await
            .expect("an idle storage class runs the job");
    }

    let snapshot = executor.snapshot();
    let admitted_by_class = [
        (StorageClass::Consensus, 1),
        (StorageClass::Filesystem, 2),
        (StorageClass::Connector, 3),
    ];
    for (class, admitted) in admitted_by_class {
        let read = snapshot.storage_class(class);
        assert_eq!(
            read.admitted, admitted,
            "{class:?} counts only its own jobs"
        );
        assert_eq!(
            snapshot.worker_class(crate::WorkerClassName::from(class)),
            read,
            "the widened name of {class:?} reads the same class"
        );
    }
    assert_eq!(
        snapshot.storage_class(StorageClass::Consensus),
        snapshot.consensus_storage
    );
    assert_eq!(
        snapshot.storage_class(StorageClass::Filesystem),
        snapshot.filesystem_storage
    );
    assert_eq!(
        snapshot.storage_class(StorageClass::Connector),
        snapshot.connector_storage
    );
    assert_eq!(
        snapshot.worker_class(crate::WorkerClassName::from(crate::CpuClass::Bulk)),
        snapshot.bulk_cpu
    );
}

async fn connector_storage_isolation() {
    let executor = small_executor();
    let (started, has_started) = nervix_primitives::sync::oneshot::channel();
    let (release, released) = nervix_primitives::sync::oneshot::channel();
    let charge = executor
        .try_reserve(MemoryClass::Commands, 1)
        .expect("the connector job can reserve its envelope");
    let occupied = executor.clone();
    let running = nervix_primitives::task::spawn(async move {
        occupied
            .run_storage(StorageClass::Connector, charge, move |_, _| {
                started.send(()).expect("the test awaits worker admission");
                released
                    .blocking_recv()
                    .expect("the test releases the worker");
            })
            .await
            .expect("the admitted connector job completes");
    });
    has_started
        .await
        .expect("the connector reports its admission");
    let snapshot = executor.snapshot();
    assert_eq!(snapshot.connector_storage.running, 1);
    assert_eq!(snapshot.filesystem_storage.running, 0);
    assert_eq!(snapshot.consensus_storage.running, 0);
    for class in [StorageClass::Filesystem, StorageClass::Consensus] {
        let charge = executor
            .try_reserve(MemoryClass::Commands, 1)
            .expect("each independent job can reserve its envelope");
        executor
            .run_storage(class, charge, |_, _| ())
            .await
            .expect("an occupied connector worker cannot hold other storage classes");
        assert_eq!(executor.snapshot().connector_storage.running, 1);
    }
    release
        .send(())
        .expect("the connector remains held until both jobs finish");
    running.await.expect("the connector supervisor is joined");
    let snapshot = executor.snapshot();
    assert_eq!(snapshot.connector_storage.running, 0);
    assert_eq!(snapshot.connector_storage.completed, 1);
    assert_eq!(snapshot.filesystem_storage.completed, 1);
    assert_eq!(snapshot.consensus_storage.completed, 1);
    assert_eq!(snapshot.commands_memory.reserved_bytes, 0);
}

#[nervix_primitives::test]
async fn a_blocked_connector_leaves_filesystem_and_consensus_workers_available() {
    connector_storage_isolation().await;
}

#[cfg(feature = "shuttle")]
mod shuttle_checks {
    use std::{future::Future, task::Poll};

    use error_stack::Report;
    use meticulous::ResultExt as _;
    use nervix_model_harness::shuttle::check_random_and_pct;
    use nervix_primitives::sync::StdArc;

    use super::*;
    use crate::{CpuClass, ExecutionError, StorageClass};

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
            ("state_read", snapshot.state_read_memory),
            ("state_read_cache", snapshot.state_read_cache_memory),
            ("bulk", snapshot.bulk_memory),
            ("bulk_window", snapshot.bulk_window_memory),
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
            ("connector_storage", snapshot.connector_storage),
        ] {
            assert_eq!(
                workers.running, 0,
                "{class} must return every worker permit"
            );
            assert_eq!(
                workers.waiting, 0,
                "{class} must count no job as waiting once a worker took each one or it gave up"
            );
        }
        for (class, budget) in [
            ("management", snapshot.management_memory),
            ("commands", snapshot.commands_memory),
            ("relay", snapshot.relay_memory),
            ("state_read", snapshot.state_read_memory),
            ("state_read_cache", snapshot.state_read_cache_memory),
            ("bulk", snapshot.bulk_memory),
            ("bulk_window", snapshot.bulk_window_memory),
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
                panic!("the worker was expected to remain occupied while this job waited")
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
                MemoryClass::Intake,
                MemoryClass::Management,
                MemoryClass::Commands,
                MemoryClass::StateRead,
                MemoryClass::StateReadCache,
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
            assert_eq!(snapshot.state_read_memory.reserved_bytes, 1024);
            assert_eq!(snapshot.state_read_cache_memory.reserved_bytes, 1024);
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
            assert_eq!(snapshot.bulk_cpu.waiting, 0);
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

    #[test]
    fn shuttle_occupied_connector_leaves_filesystem_and_consensus_execution_untouched() {
        check_random_and_pct(|| {
            shuttle::future::block_on(super::connector_storage_isolation());
        });
    }

    fn waiting_job_drop_invariant() {
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
                    .run_cpu(crate::CpuClass::Data, occupying, move |_charge, _| {
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

            let waiting_bytes = 4096;
            let waiting_charge = executor
                .try_reserve(MemoryClass::Relay, waiting_bytes)
                .assured("the relay class has room for the waiting job");
            let (admitted, is_admitted) = nervix_primitives::sync::oneshot::channel();
            let waiting = executor.clone();
            let cancelled = nervix_primitives::task::spawn(async move {
                announce_after_first_pending(
                    waiting.run_cpu(CpuClass::Data, waiting_charge, |_, _| ()),
                    admitted,
                )
                .await
            });
            is_admitted
                .await
                .assured("the waiting job reports once it waits for the busy worker");

            let waiting_snapshot = executor.snapshot();
            assert_eq!(waiting_snapshot.data_cpu.running, 1);
            assert_eq!(waiting_snapshot.data_cpu.waiting, 1);
            assert_eq!(
                waiting_snapshot.relay_memory.reserved_bytes,
                occupying_bytes + waiting_bytes
            );
            assert_live_reservations_fit(&executor);

            cancelled.abort();
            let cancelled_result = cancelled.await;
            assert!(
                cancelled_result.is_err(),
                "the waiting job must be cancelled before it can take the occupied worker"
            );

            let cancelled_snapshot = executor.snapshot();
            assert_eq!(cancelled_snapshot.data_cpu.running, 1);
            assert_eq!(cancelled_snapshot.data_cpu.waiting, 0);
            assert_eq!(
                cancelled_snapshot.relay_memory.reserved_bytes,
                occupying_bytes
            );
            assert_live_reservations_fit(&executor);

            release
                .send(())
                .assured("the occupying job remains blocked while the waiting job is dropped");
            running
                .await
                .assured("the occupying job exits after its release");
            assert_every_permit_returned(&executor);
        });
    }

    #[test]
    fn shuttle_a_dropped_waiting_job_releases_its_reservation_and_exact_entry() {
        check_random_and_pct(waiting_job_drop_invariant);
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
            assert_eq!(running_snapshot.data_cpu.waiting, 0);
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
            assert_eq!(cancelled_snapshot.data_cpu.waiting, 0);
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

    fn busy_worker_invariant() {
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
                    .run_cpu(crate::CpuClass::Data, occupying, move |_charge, _| {
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

            let first_charge = executor
                .try_reserve(MemoryClass::Relay, 1024)
                .assured("the relay class has room for the first waiting job");
            let (admitted, is_admitted) = nervix_primitives::sync::oneshot::channel();
            let waiting = executor.clone();
            let first = nervix_primitives::task::spawn(async move {
                announce_after_first_pending(
                    waiting.run_cpu(CpuClass::Data, first_charge, |_, _| ()),
                    admitted,
                )
                .await
            });
            is_admitted
                .await
                .assured("the first job reports once it waits for the busy worker");

            let busy_snapshot = executor.snapshot();
            assert_eq!(busy_snapshot.data_cpu.running, 1);
            assert_eq!(busy_snapshot.data_cpu.waiting, 1);
            assert_live_reservations_fit(&executor);

            let next_charge = executor
                .try_reserve(MemoryClass::Relay, 1024)
                .assured("memory admission stays independent of busy workers");
            let (next_registered, is_next_registered) = nervix_primitives::sync::oneshot::channel();
            let next_executor = executor.clone();
            let next = nervix_primitives::task::spawn(async move {
                announce_after_first_pending(
                    next_executor.run_cpu(CpuClass::Data, next_charge, |_, _| ()),
                    next_registered,
                )
                .await
            });
            is_next_registered
                .await
                .assured("the next job reports once it waits behind the first");

            let held_snapshot = executor.snapshot();
            assert_eq!(held_snapshot.data_cpu.running, 1);
            assert_eq!(
                held_snapshot.data_cpu.waiting, 2,
                "every job that finds the worker busy waits, however many already do"
            );
            assert_live_reservations_fit(&executor);

            release
                .send(())
                .assured("the occupying job remains blocked until the next job waits");
            running
                .await
                .assured("the occupying job exits after its release");
            first
                .await
                .assured("the first waiting job is joined")
                .assured("the first waiting job runs once the worker frees");
            next.await
                .assured("the next job is joined")
                .assured("the next job runs once the worker takes it");
            assert_every_permit_returned(&executor);
        });
    }

    /// Busy workers are backpressure: a job that finds every worker busy waits for one instead of
    /// being refused, however many jobs already wait, and runs once a worker takes it.
    #[test]
    fn shuttle_a_job_that_finds_every_worker_busy_waits_until_one_frees() {
        check_random_and_pct(busy_worker_invariant);
    }

    /// The only data worker, held by a job until the check lets that job exit.
    struct BusyDataWorker {
        release: nervix_primitives::sync::oneshot::Sender<()>,
        running: nervix_primitives::task::JoinHandle<()>,
    }

    impl BusyDataWorker {
        async fn fill(executor: &Executor) -> Self {
            let (started, has_started) = nervix_primitives::sync::oneshot::channel();
            let (release, released) = nervix_primitives::sync::oneshot::channel();
            let occupying = executor
                .try_reserve(MemoryClass::Relay, 1024)
                .assured("the untouched relay class starts with room");
            let occupied = executor.clone();
            let running = nervix_primitives::task::spawn(async move {
                occupied
                    .run_cpu(crate::CpuClass::Data, occupying, move |_charge, _| {
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
            Self { release, running }
        }

        /// Let the worker's job exit, so the worker takes the waiting jobs in the order they asked,
        /// and wait for that job.
        async fn release(self) {
            self.release
                .send(())
                .assured("the occupying job stays blocked until the check releases it");
            self.running
                .await
                .assured("the occupying job exits after its release");
        }
    }

    /// A job waiting for the busy worker, and the report its first poll sends once it waits.
    struct WaitingJob {
        registered: nervix_primitives::sync::oneshot::Receiver<()>,
        task: nervix_primitives::task::JoinHandle<Result<(), Report<ExecutionError>>>,
    }

    impl WaitingJob {
        /// Spawn a data job that waits for the worker and records `label` in `order` when it runs.
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
                    waiter.run_cpu(CpuClass::Data, charge, move |_, _| {
                        job_order.lock().push(label)
                    }),
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
            let busy = BusyDataWorker::fill(&executor).await;

            let WaitingJob {
                registered,
                task: waiting,
            } = WaitingJob::spawn(&executor, &order, "waiting");
            registered
                .await
                .assured("the waiting job reports once it waits for the busy worker");
            let held = executor.snapshot();
            assert_eq!(
                held.data_cpu.waiting, 1,
                "the job waits for the busy worker"
            );
            assert_eq!(held.relay_memory.reserved_bytes, 2 * 1024);

            // A later job asks after the waiting one, racing the release that frees the worker.
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
            busy.release().await;
            waiting
                .await
                .assured("the waiting job is joined")
                .assured("the waiting job runs once the worker frees");
            late.await
                .assured("the late job is joined")
                .assured("the late job waits for the worker too and runs");

            let order = order.lock().clone();
            assert_eq!(
                order.first(),
                Some(&"waiting"),
                "the freed worker takes the job that waited for it first: {order:?}"
            );
            assert_every_permit_returned(&executor);
        });
    }

    /// A job waiting for the busy worker takes it once it frees, ahead of a job that asks after it,
    /// and runs first.
    #[test]
    fn shuttle_a_waiting_job_takes_the_freed_worker_ahead_of_a_later_job() {
        check_random_and_pct(waiting_admission_order_invariant);
    }

    fn dropped_waiting_admission_invariant() {
        shuttle::future::block_on(async {
            let executor = small_executor();
            let order = StdArc::new(nervix_primitives::sync::blocking::Mutex::new(Vec::new()));
            let busy = BusyDataWorker::fill(&executor).await;

            let WaitingJob {
                registered,
                task: dropped,
            } = WaitingJob::spawn(&executor, &order, "dropped");
            registered
                .await
                .assured("the first waiting job reports once it waits for the busy worker");
            let WaitingJob {
                registered: successor_registered,
                task: successor,
            } = WaitingJob::spawn(&executor, &order, "successor");
            successor_registered
                .await
                .assured("the second waiting job reports once it waits for the busy worker");
            assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 3 * 1024);
            assert_eq!(
                executor.snapshot().data_cpu.waiting,
                2,
                "both jobs that found the worker busy count as waiting"
            );

            dropped.abort();
            assert!(
                dropped.await.is_err(),
                "dropping a waiting job ends it before a worker takes it"
            );
            let abandoned = executor.snapshot();
            assert_eq!(
                abandoned.relay_memory.reserved_bytes,
                2 * 1024,
                "a waiting job that is dropped returns its charge"
            );
            assert_eq!(
                abandoned.data_cpu.waiting, 1,
                "a dropped wait stops counting while the job after it still waits"
            );

            busy.release().await;
            successor
                .await
                .assured("the second waiting job is joined")
                .assured("the turn the dropped job gave up goes to the job after it");
            assert_eq!(*order.lock(), vec!["successor"]);
            assert_every_permit_returned(&executor);
        });
    }

    /// A waiting job that its caller drops gives up its turn and returns its charge, and the worker
    /// freed next takes the job waiting after it.
    #[test]
    fn shuttle_a_dropped_waiting_job_gives_its_turn_to_the_job_after_it() {
        check_random_and_pct(dropped_waiting_admission_invariant);
    }

    fn growing_job_holds_nothing_while_waiting_invariant() {
        shuttle::future::block_on(async {
            let executor = small_executor();
            let capacity = executor.memory(MemoryClass::Bulk).capacity_bytes;
            let first_charge = 4096;
            // The holder leaves exactly one first charge free, and lets go only once the waiter
            // below has its charge.
            let holder = executor
                .try_reserve(
                    MemoryClass::Bulk,
                    capacity
                        .checked_sub(first_charge)
                        .assured("the bulk class is larger than one first charge"),
                )
                .assured("the empty bulk class holds all but one first charge");
            let (started, attempt_started) = nervix_primitives::sync::oneshot::channel();
            let (proceed, attempt_proceeds) =
                nervix_primitives::sync::blocking::mpsc::channel::<()>();
            let mut gate = Some((started, attempt_proceeds));
            let growing = executor.clone();
            let job = nervix_primitives::task::spawn(async move {
                growing
                    .run_cpu_growing(
                        CpuClass::Bulk,
                        MemoryClass::Bulk,
                        first_charge,
                        vec![7_u8; 64 * 1024],
                        move |input, charge, _cancellation| {
                            // The first attempt holds its charge until a charge is queued behind
                            // it.
                            if let Some((started, proceeds)) = gate.take() {
                                started.send(()).assured(
                                    "the test waits for the first attempt while it holds its \
                                     charge",
                                );
                                proceeds
                                    .recv()
                                    .assured("the test lets the first attempt go on");
                            }
                            write_growing(input, charge)
                        },
                    )
                    .await
            });
            attempt_started
                .await
                .assured("the first attempt runs under the one free charge");
            // This waiter queues for the very charge the first attempt holds. A job that waited for
            // its growth while holding that charge would leave the waiter, and so the holder and the
            // job itself, waiting forever.
            let mut waiting = std::pin::pin!(executor.reserve(MemoryClass::Bulk, first_charge));
            let first_poll =
                std::future::poll_fn(|context| Poll::Ready(waiting.as_mut().poll(context))).await;
            assert!(
                first_poll.is_pending(),
                "the class has no room while the attempt holds its charge"
            );
            proceed
                .send(())
                .assured("the first attempt waits for the test");
            let granted = waiting
                .await
                .assured("the waiter is granted the charge the short attempt gave back");
            assert_live_reservations_fit(&executor);
            drop(holder);
            drop(granted);
            let written = job
                .await
                .assured("the growing job is joined")
                .assured("the growing job runs again once its class has room");
            assert_eq!(written.len(), 64 * 1024);
            drop(written);
            assert_every_permit_returned(&executor);
        });
    }

    fn reported_wait_invariant() {
        shuttle::future::block_on(async {
            let executor = small_executor();
            let busy = BusyDataWorker::fill(&executor).await;
            let waits = crate::CapacityWaits::new();
            let reporting = executor.reporting_waits_to(&waits);

            // The owner watches from a task of its own, so its watch races the submission that
            // begins the wait.
            let (began, has_begun) = nervix_primitives::sync::oneshot::channel();
            let watched = waits.clone();
            let watcher = nervix_primitives::task::spawn(async move {
                watched.began().await;
                began
                    .send(())
                    .assured("the check awaits the owner seeing the wait begin");
                watched.ended().await;
            });
            let charge = executor
                .try_reserve(MemoryClass::Relay, 1024)
                .assured("the relay class has room for the waiting job");
            let submitted = nervix_primitives::task::spawn(async move {
                reporting
                    .run_cpu(CpuClass::Data, charge, |_, _| "ran")
                    .await
            });
            has_begun
                .await
                .assured("the owner sees its job begin waiting while the worker is busy");
            assert!(
                waits.is_waiting(),
                "the wait lasts until the worker takes the job"
            );

            // The freed worker races the owner's watch for the end of the wait.
            busy.release().await;
            let ran = submitted
                .await
                .assured("the waiting job is joined")
                .assured("a waiting job runs once the worker takes it");
            assert_eq!(ran, "ran");
            watcher
                .await
                .assured("the owner sees the wait end once the freed worker takes its job");
            assert!(!waits.is_waiting());
            assert_every_permit_returned(&executor);
        });
    }

    /// An owner watching its work through `CapacityWaits` sees its job begin waiting for a worker of
    /// a busy class and sees the wait end once a worker takes it, whichever of its watch and the
    /// job's wait comes first.
    #[test]
    fn shuttle_an_owner_sees_its_job_begin_and_end_waiting_for_a_worker() {
        check_random_and_pct(reported_wait_invariant);
    }

    fn withdrawn_carrying_job_racing_its_worker_invariant() {
        shuttle::future::block_on(async {
            let executor = small_executor();
            let order = StdArc::new(nervix_primitives::sync::blocking::Mutex::new(Vec::new()));
            let busy = BusyDataWorker::fill(&executor).await;
            let charge = executor
                .try_reserve(MemoryClass::Relay, 1024)
                .assured("the relay class has room for the carrying job");
            let mut home: Option<&'static str> = None;
            {
                let job_order = StdArc::clone(&order);
                let mut carrying = Box::pin(executor.run_cpu_carrying(
                    CpuClass::Data,
                    charge,
                    "session",
                    &mut home,
                    move |state, _charge, _| {
                        job_order.lock().push("carried");
                        (state, ())
                    },
                ));
                assert!(
                    std::future::poll_fn(|cx| Poll::Ready(carrying.as_mut().poll(cx)))
                        .await
                        .is_pending(),
                    "the carrying job waits for the busy worker"
                );
                // The freed worker races the caller withdrawing the waiting job.
                let release = nervix_primitives::task::spawn(busy.release());
                nervix_primitives::task::yield_now().await;
                drop(carrying);
                release
                    .await
                    .assured("the occupying job exits after its release");
            }
            while {
                let data = executor.snapshot().data_cpu;
                data.running != 0 || data.waiting != 0
            } {
                nervix_primitives::task::consume_budget().await;
                nervix_primitives::task::yield_now().await;
            }
            let ran = order.lock().contains(&"carried");
            assert!(
                ran != home.is_some(),
                "the withdrawn job either ran, taking its state with it, or handed the state \
                 back, never both and never neither: ran {ran}, home {home:?}"
            );
            assert_every_permit_returned(&executor);
        });
    }

    /// A caller withdrawing a carrying job races the worker that frees to take it: either the
    /// worker took the job first and it runs, or the withdrawal took the job back first and its
    /// state is the caller's again. Every charge and worker returns either way.
    #[test]
    fn shuttle_a_withdrawn_carrying_job_racing_its_worker_runs_or_hands_its_state_back() {
        check_random_and_pct(withdrawn_carrying_job_racing_its_worker_invariant);
    }

    /// A growing job that runs short gives its charge back before it waits for the larger one, so
    /// a charge queued behind it is granted the room it held: it never waits for room while holding
    /// any.
    #[test]
    fn shuttle_a_growing_job_that_ran_short_waits_for_its_charge_holding_nothing() {
        check_random_and_pct(growing_job_holds_nothing_while_waiting_invariant);
    }

    fn consensus_admission_order_invariant() {
        shuttle::future::block_on(async {
            let executor = small_executor();
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
                assert_eq!(snapshot.consensus_storage.waiting, index);
                assert_live_reservations_fit(&executor);
            }

            let waiting_snapshot = executor.snapshot();
            assert_eq!(waiting_snapshot.consensus_storage.running, 1);
            assert_eq!(waiting_snapshot.consensus_storage.waiting, 7);
            release
                .send(())
                .assured("the first consensus job remains held while its followers wait");

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

    #[test]
    fn shuttle_submission_racing_worker_exit_never_strands_an_admitted_job() {
        check_random_and_pct(|| {
            shuttle::future::block_on(async {
                let executor = small_executor();
                let mut submissions = Vec::new();
                for value in 0..3 {
                    nervix_primitives::task::consume_budget().await;
                    let submitted = executor.clone();
                    let charge = executor
                        .try_reserve(MemoryClass::Management, 1)
                        .assured("three bytes fit in the management budget");
                    submissions.push(nervix_primitives::task::spawn(async move {
                        submitted
                            .run_cpu(CpuClass::Control, charge, move |_, _| value)
                            .await
                    }));
                }
                for (value, submission) in submissions.into_iter().enumerate() {
                    nervix_primitives::task::consume_budget().await;
                    assert_eq!(
                        submission
                            .await
                            .assured("the submitter completes")
                            .assured("an admitted job is never stranded"),
                        value
                    );
                }
                assert_eq!(executor.snapshot().control_cpu.completed, 3);
                assert_every_permit_returned(&executor);
            });
        });
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

/// A reader that must answer that its class is full, rather than wait for room, tells a class
/// without free room from a buffer at its operation's limit, and keeps nothing it was refused.
#[nervix_primitives::test]
async fn a_typed_append_tells_a_full_class_from_an_operation_limit() {
    let executor = small_executor();
    let reservation = executor
        .try_reserve(MemoryClass::Relay, 4096)
        .expect("the relay class is empty");
    let mut buffer = BudgetedBuffer::with_limit(reservation, 100 * 1024);
    buffer
        .try_extend_from_slice(&[5_u8; 4096])
        .expect("the first chunk fits its charge");
    let relay_memory = executor.snapshot().relay_memory;
    let room = relay_memory
        .capacity_bytes
        .checked_sub(relay_memory.reserved_bytes)
        .expect("a class never reserves more than its capacity");
    let occupied = executor
        .try_reserve(MemoryClass::Relay, room)
        .expect("the rest of the relay class is free");

    let refusal = buffer
        .try_extend_from_slice(&[5_u8; 4096])
        .expect_err("a full class backs no growth");
    assert_eq!(refusal.current_context(), &BufferGrowthError::NotCharged);
    assert!(
        matches!(
            refusal.downcast_ref::<AdmissionError>(),
            Some(AdmissionError::BudgetExhausted { .. })
        ),
        "the class's own refusal is the cause: {refusal:?}"
    );
    assert_eq!(buffer.len(), 4096, "a refused append keeps nothing");
    let error = buffer
        .write_all(&[5_u8; 4096])
        .expect_err("a full class backs no growth through io::Write either");
    assert!(
        error.to_string().contains("has no room for another"),
        "an io::Write writer reports the class's own refusal: {error}"
    );
    assert_eq!(buffer.len(), 4096, "a refused write keeps nothing");

    drop(occupied);
    buffer
        .try_extend_from_slice(&[5_u8; 4096])
        .expect("the class backs the growth once it has room");
    let refusal = buffer
        .try_extend_from_slice(&[5_u8; 100 * 1024])
        .expect_err("the operation limit stops the buffer");
    assert_eq!(
        refusal.current_context(),
        &BufferGrowthError::LimitExceeded(BufferLimitExceeded {
            limit: 100 * 1024,
            required: 108 * 1024,
        })
    );
    assert_eq!(buffer.len(), 8192, "a refused append keeps nothing");
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

#[nervix_primitives::test]
async fn bulk_read_batches_leave_admission_for_two_live_buffers() {
    let tuned = ExecutionConfig {
        budgets: MemoryBudgets {
            bulk: ByteUnit::Mebibyte(8),
            ..MemoryBudgets::default()
        },
        limits: OperationLimits {
            bulk_chunk_bytes: ByteUnit::Mebibyte(4),
            snapshot_section_bytes: ByteUnit::Mebibyte(1),
            ..OperationLimits::default()
        },
        ..ExecutionConfig::default()
    };
    let mut odd = tuned;
    odd.budgets.bulk = ByteUnit::Byte(ByteUnit::Mebibyte(8).as_u64() - 1);
    for (config, expected) in [
        (ExecutionConfig::default(), ByteUnit::Mebibyte(1).as_u64()),
        (tuned, ByteUnit::Mebibyte(4).as_u64()),
        (odd, ByteUnit::Mebibyte(4).as_u64() - 1),
    ] {
        let executor = Executor::new(config).expect("the complete tuned configuration is valid");
        let bytes = executor.bulk_read_batch_bytes(NonZeroU32::new(16).expect("positive quanta"));
        assert_eq!(bytes, expected);
        let first = executor
            .try_reserve(MemoryClass::Bulk, bytes)
            .expect("the first actual read buffer fits");
        let second = executor
            .try_reserve(MemoryClass::Bulk, bytes)
            .expect("the next read can coexist with the preceding transport buffer");
        assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, 2 * bytes);
        drop((first, second));
        assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, 0);
    }
}
