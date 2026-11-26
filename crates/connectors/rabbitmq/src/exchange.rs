//! The RabbitMQ exchange emitter, which publishes to a declared exchange under the routing key each
//! record computes.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The exchange emitter's entity identity and the exchange destination it hands the
//!   shared publisher, which checks the exchange exists when it opens and routes each message by
//!   its record's evaluated routing key.
//! - **Depends on.** The connector contract, vocabulary values, the node resolver and the crate's
//!   publisher.
//! - **Must not know.** The routing key's expression, runtime batches, relays, branches,
//!   schedules, registry state, or another connector implementation.

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
        reason = "the RabbitMQ exchange emitter resolves its broker host through its node's \
                  resolver"
    )
)]

use async_trait::async_trait;
use error_stack::ResultExt as _;
use nervix_connector::{
    BrokerPublishingMode, EmitterFinishResult, EmitterHost, EmitterLifecycle, EmitterOpenError,
    EmitterRecord, EmitterRecordId, EmitterStartResult, PerRecordOutcome, RecordEmitter,
};
use nervix_models::{ClientConfigEntry, RabbitMqExchangeName};
use nervix_primitives::time::Instant;

use crate::publisher::{RABBITMQ, RabbitMqDestination, RabbitMqPublisher, RabbitMqPublisherConfig};

/// What one RabbitMQ exchange emitter publishes through: its client entries, its exchange and its
/// publishing mode.
///
/// The node resolver its broker host resolves through reaches the emitter through its host handle.
pub struct RabbitMqExchangeEmitterConfig {
    pub config: Vec<ClientConfigEntry>,
    pub exchange: RabbitMqExchangeName,
    pub mode: BrokerPublishingMode,
}

/// An emitter that publishes every record to one exchange, under the routing key the host evaluated
/// for the record, which the exchange routes by.
pub struct RabbitMqExchangeEmitter {
    publisher: RabbitMqPublisher,
}

impl RabbitMqExchangeEmitter {
    pub const KIND: nervix_models::EmitterEntityKind =
        nervix_models::EmitterEntityKind::RabbitMq(nervix_models::RabbitMqEmitterEntity::Exchange);

    /// Prepares configuration and TLS material without opening a connection. Resume acquires the
    /// channel and checks that the exchange exists within the client's `connect_timeout_ms`. A
    /// missing exchange is the contract's missing entity, which resume retries until it is declared.
    async fn prepare(
        config: RabbitMqExchangeEmitterConfig,
        host: EmitterHost,
    ) -> EmitterStartResult<Self> {
        // A node without its resolver cannot open the emitter at all, so that is reported before
        // anything its configuration says.
        let dns = host.dns().change_context(EmitterOpenError::Initialize {
            connector: RABBITMQ,
        })?;
        let RabbitMqExchangeEmitterConfig {
            config,
            exchange,
            mode,
        } = config;
        let publisher = RabbitMqPublisher::open(
            RabbitMqPublisherConfig {
                config,
                destination: RabbitMqDestination::Exchange(exchange),
                mode,
            },
            dns,
        )?;
        Ok(Self { publisher })
    }
}

#[async_trait]
impl EmitterLifecycle for RabbitMqExchangeEmitter {
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
impl RecordEmitter for RabbitMqExchangeEmitter {
    async fn publish(&mut self, records: Vec<EmitterRecord>) -> PerRecordOutcome<EmitterRecordId> {
        self.publisher.publish(records).await
    }
}

impl nervix_connector::EmitterConnector for RabbitMqExchangeEmitter {
    const KIND: nervix_models::EmitterEntityKind = Self::KIND;
    type Plan = RabbitMqExchangeEmitterConfig;
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
        let opened = RabbitMqExchangeEmitter::open(
            RabbitMqExchangeEmitterConfig {
                // The entries name no broker, which alone is a configuration failure the host
                // gives up on: the node's missing resolver is reported before it.
                config: Vec::new(),
                exchange: RabbitMqExchangeName::parse("events")
                    .expect("a fixed exchange name is valid"),
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
