//! Retirement of the Kafka source's consumers: their native destruction, off the async workers.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The consumer a source or partition inspector shares with the driver jobs it admits,
//!   and the retirement that destroys it on the connector storage workers once the last of them
//!   lets it go.
//! - **Depends on.** The bounded executor, the primitive boundary, `error-stack` and librdkafka.
//! - **Must not know.** Graphs, placement, registry state or the host that drives the source.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the native destruction of Kafka consumers")
)]

use std::ops::Deref;

use arch_into::ArchInto as _;
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_execution::{Executor, MemoryClass, QueueAdmission, Reservation, StorageClass};
use nervix_primitives::sync::Arc;
use nervix_recovery::Reported as _;
use rdkafka::{config::ClientConfig, consumer::StreamConsumer};

use super::KafkaSourceError;

/// A handle to a librdkafka consumer, which the source or partition inspector that created it
/// shares with the driver jobs it admits.
///
/// Destroying a consumer is native work that waits. rust-rdkafka closes a consumer that has a
/// group, polling it in steps of 100 milliseconds until it is closed, and a consumer that joined
/// its group waits there for its coordinator to answer the LeaveGroup request, which librdkafka
/// gives five seconds. librdkafka then joins its threads. Whoever lets the last handle go therefore
/// hands the consumer to its retirement rather than destroying it on its own thread.
#[derive(Clone)]
pub(super) struct KafkaConsumerHandle {
    shared: Arc<SharedConsumer>,
}

/// The consumer every handle shares. Dropping it, which only the last handle's drop does, retires
/// the consumer.
struct SharedConsumer {
    /// Present for as long as a handle exists; the drop that retires the consumer takes it.
    owned: Option<OwnedConsumer>,
}

/// A consumer and what its retirement runs under.
struct OwnedConsumer {
    driver: StreamConsumer,
    executor: Executor,
    /// The commands charge of the job that destroys the consumer, taken when the consumer was
    /// created, so retiring it asks the node for nothing it could refuse.
    retirement: Reservation,
}

impl KafkaConsumerHandle {
    /// Creates the consumer `config` describes, taking the charge of the job that will destroy it
    /// first.
    pub(super) fn create(
        config: &ClientConfig,
        executor: Executor,
    ) -> Result<Self, Report<KafkaSourceError>> {
        // The retirement job carries the consumer and nothing else.
        let retirement = executor
            .try_reserve(
                MemoryClass::Commands,
                std::mem::size_of::<StreamConsumer>().arch_into(),
            )
            .change_context(KafkaSourceError::RetirementAdmission)?;
        let driver = config
            .create()
            .map_err(|source| Report::new(source).change_context(KafkaSourceError::Initialize))?;
        Ok(Self {
            shared: Arc::new(SharedConsumer {
                owned: Some(OwnedConsumer {
                    driver,
                    executor,
                    retirement,
                }),
            }),
        })
    }
}

impl Deref for KafkaConsumerHandle {
    type Target = StreamConsumer;

    fn deref(&self) -> &StreamConsumer {
        let owned = self
            .shared
            .owned
            .as_ref()
            .assured("a consumer is retired only once no handle to it is left");
        &owned.driver
    }
}

impl Drop for SharedConsumer {
    fn drop(&mut self) {
        let owned = self
            .owned
            .take()
            .assured("the shared consumer is dropped once, and only that drop takes the consumer");
        owned.retire();
    }
}

impl OwnedConsumer {
    /// Hands the consumer to a supervisor that waits for a place among the connector storage
    /// workers and destroys it there.
    ///
    /// Spawning does not wait, so the holder that let the last handle go, an async worker
    /// included, carries on at once. The supervisor owns the consumer until it is destroyed: no
    /// caller waits for it and nothing cancels it, because the destruction cannot stop part way.
    /// A full queue holds the job until a place frees rather than refusing it, since nothing could
    /// present the consumer again, and the charge the job runs under was taken when the consumer
    /// was created. Once a stopping runtime drops the supervisor, a consumer still waiting for a
    /// worker is destroyed by that stop.
    fn retire(self) {
        let Self {
            driver,
            executor,
            retirement,
        } = self;
        nervix_primitives::task::spawn(async move {
            let destroyed = executor
                .run_storage_with(
                    StorageClass::Connector,
                    QueueAdmission::WaitForPlace,
                    retirement,
                    // Destruction cannot stop part way, so the job never checks its cancellation.
                    move |_retirement, _cancellation| drop(driver),
                )
                .await;
            destroyed.reported("destroying a retired Kafka consumer");
        });
    }
}
