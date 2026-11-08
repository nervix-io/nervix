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

use async_trait::async_trait;
use nervix_connector::{
    BrokerPublishingMode, EmitterFinishResult, EmitterHost, EmitterLifecycle, EmitterRecord,
    EmitterRecordId, EmitterStartResult, PerRecordOutcome, RecordEmitter,
};
use nervix_dns::DnsResolver;
use nervix_models::{ClientConfigEntry, RabbitMqExchangeName};
use nervix_primitives::time::Instant;

use crate::publisher::{RabbitMqDestination, RabbitMqPublisher, RabbitMqPublisherConfig};

/// What one RabbitMQ exchange emitter publishes through: its client entries, the node resolver its
/// broker host resolves through, its exchange and its publishing mode.
pub struct RabbitMqExchangeEmitterConfig {
    pub config: Vec<ClientConfigEntry>,
    pub dns: DnsResolver,
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
        _host: EmitterHost,
    ) -> EmitterStartResult<Self> {
        let RabbitMqExchangeEmitterConfig {
            config,
            dns,
            exchange,
            mode,
        } = config;
        let publisher = RabbitMqPublisher::open(RabbitMqPublisherConfig {
            config,
            dns,
            destination: RabbitMqDestination::Exchange(exchange),
            mode,
        })?;
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
