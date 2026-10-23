//! Admission and deadlines for the Kafka ingestor's synchronous driver control calls.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The executor admission and native request budget of an ingestor or partition inspector,
//!   and submission of a consumer-group commit whose answer may outlive its caller.
//! - **Depends on.** The connector's typed operations, the bounded executor and librdkafka.
//! - **Must not know.** Graphs, placement, registry state or host-owned domain offsets.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns admitted Kafka driver control I/O")
)]

use std::time::Duration;

use arch_into::ArchInto as _;
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{Cancellation, ExecutionError, Executor, MemoryClass, StorageClass};
use nervix_primitives::{
    sync::{Semaphore, StdArc},
    task::JoinHandle,
    time::{Instant, timeout_at},
};
use rdkafka::{config::ClientConfig, consumer::StreamConsumer, error::KafkaResult};

use super::{GroupCommitRequest, KafkaTopicIngestorError, retirement::KafkaConsumerHandle};

#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the per-consumer ordering invariant admits one control operation, so a \
                  cancelled native mutation cannot be overtaken"
    )
)]
const CONSUMER_CONTROL_TURNS: usize = 1;

/// One commit's exact job and eventual coordinator answer, retained by its ingestor instance.
pub(super) type CommitTask = JoinHandle<Result<KafkaResult<()>, Report<ExecutionError>>>;

pub(super) struct KafkaControl {
    executor: Executor,
    timeout: Duration,
    /// A cancelled caller cannot let a later assignment or seek overtake its native mutation.
    turn: StdArc<Semaphore>,
}

impl KafkaControl {
    pub(super) fn new(
        config: &ClientConfig,
        executor: Executor,
    ) -> Result<Self, Report<KafkaTopicIngestorError>> {
        let native = config
            .create_native_config()
            .change_context(KafkaTopicIngestorError::Initialize)?;
        let value = native
            .get("socket.timeout.ms")
            .change_context(KafkaTopicIngestorError::Initialize)?;
        let milliseconds = value.parse::<u32>().assured(
            "librdkafka validates socket.timeout.ms as an integer from 10 to 300000 and renders \
             decimal digits",
        );
        Ok(Self {
            executor,
            timeout: Duration::from_millis(u64::from(milliseconds)),
            turn: StdArc::new(Semaphore::new(CONSUMER_CONTROL_TURNS)),
        })
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the admitted job invokes its caller's synchronous Kafka operation"
        )
    )]
    pub(super) async fn run<T, F>(
        &self,
        consumer: &KafkaConsumerHandle,
        operation: F,
    ) -> Result<T, Report<KafkaTopicIngestorError>>
    where
        T: Send + 'static,
        F: FnOnce(
                &StreamConsumer,
                &ControlBudget<'_>,
            ) -> Result<T, Report<KafkaTopicIngestorError>>
            + Send
            + 'static,
    {
        let deadline = Instant::now().checked_add(self.timeout).assured(
            "librdkafka limits the control-call budget to five minutes, which fits the monotonic \
             clock",
        );
        // Charge the job envelope. librdkafka owns its native request and response buffers;
        // their limits remain driver bounds, separate from Nervix's transient byte budgets.
        let charge = self
            .executor
            .try_reserve(MemoryClass::Commands, std::mem::size_of::<F>().arch_into())
            .change_context(KafkaTopicIngestorError::ControlAdmission)?;
        let consumer = consumer.clone();
        let bound = self.timeout;
        let turn = StdArc::clone(&self.turn);
        let running = async move {
            let turn = turn.acquire_owned().await.assured(
                "the control owner never closes its semaphore and this waiter owns a handle",
            );
            self.executor
                .run_storage(
                    StorageClass::Connector,
                    charge,
                    move |_charge, cancellation| {
                        let _turn = turn;
                        let budget = ControlBudget {
                            deadline,
                            bound,
                            cancellation,
                        };
                        budget.remaining()?;
                        operation(&consumer, &budget)
                    },
                )
                .await
        };
        timeout_at(deadline, running)
            .await
            .map_err(|_| Report::new(KafkaTopicIngestorError::ControlTimeout { timeout: bound }))?
            .change_context(KafkaTopicIngestorError::ControlExecution)?
    }

    /// The supervisor owns admission and the answer until the synchronous commit actually ends.
    /// Dropping its JoinHandle does not cancel it or release its worker prematurely.
    pub(super) fn start_commit(
        &self,
        consumer: &KafkaConsumerHandle,
        request: GroupCommitRequest,
    ) -> Result<CommitTask, Report<KafkaTopicIngestorError>> {
        let charge = self
            .executor
            .try_reserve(
                MemoryClass::Commands,
                std::mem::size_of::<GroupCommitRequest>().arch_into(),
            )
            .change_context(KafkaTopicIngestorError::ControlAdmission)?;
        let consumer = consumer.clone();
        let executor = self.executor.clone();
        Ok(nervix_primitives::task::spawn(async move {
            executor
                .run_storage(StorageClass::Connector, charge, move |_charge, _| {
                    request.commit(&consumer)
                })
                .await
        }))
    }
}

pub(super) struct ControlBudget<'a> {
    deadline: Instant,
    bound: Duration,
    cancellation: &'a Cancellation,
}

impl ControlBudget<'_> {
    /// Check between partitions, and give each native wait only what remains of the operation.
    pub(super) fn remaining(&self) -> Result<Duration, Report<KafkaTopicIngestorError>> {
        self.cancellation
            .check()
            .change_context(KafkaTopicIngestorError::ControlCancelled)?;
        if let Some(remaining) = self.deadline.checked_duration_since(Instant::now())
            && !remaining.is_zero()
        {
            return Ok(remaining);
        }
        Err(Report::new(KafkaTopicIngestorError::ControlTimeout {
            timeout: self.bound,
        }))
    }
}
