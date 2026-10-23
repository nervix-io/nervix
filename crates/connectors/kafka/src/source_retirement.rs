//! Retirement of the Kafka source's consumers: their native destruction, off the async workers.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The consumer a source or partition inspector shares with the driver jobs it admits,
//!   created once the charge of the job that will destroy it is taken.
//! - **Depends on.** The crate's retirement of native clients, the bounded executor, `error-stack`
//!   and librdkafka.
//! - **Must not know.** Graphs, placement, registry state or the host that drives the source.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the native destruction of Kafka consumers")
)]

use error_stack::{Report, ResultExt as _};
use nervix_execution::Executor;
use rdkafka::config::ClientConfig;

use super::{
    KafkaSourceError,
    rejection::{KafkaConsumer, KafkaConsumerContext},
};
use crate::retirement::{NativeClient, RetirementCharge, Retiring};

/// A handle to a librdkafka consumer, which the source or partition inspector that created it
/// shares with the driver jobs it admits.
pub(super) type KafkaConsumerHandle = Retiring<KafkaConsumer>;

/// Destroying a consumer is native work that waits. rust-rdkafka closes a consumer that has a
/// group, polling it in steps of 100 milliseconds until it is closed, and a consumer that joined
/// its group waits there for its coordinator to answer the LeaveGroup request, which librdkafka
/// gives five seconds. librdkafka then joins its threads.
impl NativeClient for KafkaConsumer {
    const DESTRUCTION: &'static str = "destroying a retired Kafka consumer";
}

impl Retiring<KafkaConsumer> {
    /// Creates the consumer `config` describes, taking the charge of the job that will destroy it
    /// first.
    pub(super) fn create(
        config: &ClientConfig,
        executor: Executor,
    ) -> Result<Self, Report<KafkaSourceError>> {
        let retirement = RetirementCharge::reserve(executor)
            .change_context(KafkaSourceError::RetirementAdmission)?;
        let driver = config
            .create_with_context(KafkaConsumerContext::default())
            .map_err(|source| Report::new(source).change_context(KafkaSourceError::Initialize))?;
        Ok(retirement.hold(driver))
    }
}
