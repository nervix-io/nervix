//! The RabbitMQ queue emitter, which publishes to one queue through the broker's default exchange.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The queue emitter's entity identity and the default-exchange destination it hands
//!   the shared publisher.
//! - **Depends on.** The connector contract, vocabulary values, the node resolver and the crate's
//!   publisher.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "the host supplies a validated connector plan")
)]
#![cfg_attr(
    nervix_lint,
    nervix::host_services(
        dns,
        reason = "the RabbitMQ queue emitter resolves its broker host through its node's resolver"
    )
)]

use async_trait::async_trait;
use error_stack::ResultExt as _;
use nervix_connector::{
    BrokerPublishingMode, EmitterFinishResult, EmitterHost, EmitterLifecycle, EmitterOpenError,
    EmitterRecord, EmitterRecordId, EmitterStartResult, PerRecordOutcome, RecordEmitter,
};
use nervix_models::{ClientConfigEntry, RabbitMqQueueName};
use nervix_primitives::time::Instant;

use crate::publisher::{RABBITMQ, RabbitMqDestination, RabbitMqPublisher, RabbitMqPublisherConfig};

/// What one RabbitMQ queue emitter publishes through: its client entries, its queue and its
/// publishing mode.
///
/// The node resolver its broker host resolves through reaches the emitter through its host handle.
pub struct RabbitMqQueueEmitterConfig {
    pub config: Vec<ClientConfigEntry>,
    pub queue: RabbitMqQueueName,
    pub mode: BrokerPublishingMode,
}

/// An emitter that publishes every record to one queue: to the broker's default exchange, with the
/// queue's name as the routing key that routes it there.
pub struct RabbitMqQueueEmitter {
    publisher: RabbitMqPublisher,
}

impl RabbitMqQueueEmitter {
    pub const KIND: nervix_models::EmitterEntityKind =
        nervix_models::EmitterEntityKind::RabbitMq(nervix_models::RabbitMqEmitterEntity::Queue);

    /// Prepares configuration and TLS material without opening a connection. Resume acquires the
    /// channel and checks that the queue exists within the client's `connect_timeout_ms`.
    async fn prepare(
        config: RabbitMqQueueEmitterConfig,
        host: EmitterHost,
    ) -> EmitterStartResult<Self> {
        // A node without its resolver cannot open the emitter at all, so that is reported before
        // anything its configuration says.
        let dns = host.dns().change_context(EmitterOpenError::Initialize {
            connector: RABBITMQ,
        })?;
        let RabbitMqQueueEmitterConfig {
            config,
            queue,
            mode,
        } = config;
        let publisher = RabbitMqPublisher::open(
            RabbitMqPublisherConfig {
                config,
                destination: RabbitMqDestination::Queue(queue),
                mode,
            },
            dns,
        )?;
        Ok(Self { publisher })
    }
}

#[async_trait]
impl EmitterLifecycle for RabbitMqQueueEmitter {
    fn kind(&self) -> nervix_models::EmitterEntityKind {
        Self::KIND
    }

    fn needs_resume(&mut self) -> bool {
        self.publisher.needs_resume()
    }

    fn suspend(&mut self) -> nervix_connector::EmitterLifecycleStep<'_, ()> {
        self.publisher.suspend();
        nervix_connector::EmitterLifecycleStep::ready(Ok(()))
    }

    fn resume(
        &mut self,
    ) -> nervix_connector::EmitterLifecycleStep<'_, nervix_connector::EmitterResume> {
        if !self.needs_resume() {
            return nervix_connector::EmitterLifecycleStep::ready(Ok(
                nervix_connector::EmitterResume::Ready,
            ));
        }
        nervix_connector::EmitterLifecycleStep::run(async move { self.publisher.resume().await })
    }

    async fn finish(&mut self, _deadline: Instant) -> EmitterFinishResult {
        self.publisher.finish()
    }
}

#[async_trait]
impl RecordEmitter for RabbitMqQueueEmitter {
    async fn publish(&mut self, records: Vec<EmitterRecord>) -> PerRecordOutcome<EmitterRecordId> {
        self.publisher.publish(records).await
    }
}

impl nervix_connector::EmitterConnector for RabbitMqQueueEmitter {
    const KIND: nervix_models::EmitterEntityKind = Self::KIND;
    type Plan = RabbitMqQueueEmitterConfig;
    async fn open(plan: Self::Plan, host: EmitterHost) -> EmitterStartResult<Self> {
        Self::prepare(plan, host).await
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use nervix_connector::{EmitterConnector as _, HostServiceError};

    use super::*;

    #[nervix_primitives::test]
    async fn an_emitter_on_a_node_without_its_resolver_fails_to_open_and_names_the_resolver() {
        let opened = RabbitMqQueueEmitter::open(
            RabbitMqQueueEmitterConfig {
                // The entries name no broker, which alone is a configuration failure the host
                // gives up on: the node's missing resolver is reported before it.
                config: Vec::new(),
                queue: RabbitMqQueueName::parse("orders").expect("a fixed queue name is valid"),
                mode: BrokerPublishingMode::NoAck,
            },
            nervix_connector_fake::FakeEmitterHost::default().host(),
        )
        .await;
        let error = match opened {
            Ok(_) => panic!("an emitter that dials its broker by name needs its node's resolver"),
            Err(error) => error,
        };

        // The failure is the node's, not the configuration's, so the host opens the emitter again.
        assert_eq!(
            *error.current_context(),
            EmitterOpenError::Initialize {
                connector: RABBITMQ
            }
        );
        assert_eq!(
            error
                .frames()
                .find_map(|frame| frame.downcast_ref::<HostServiceError>()),
            Some(&HostServiceError::DnsNotInstalled)
        );
    }
}
