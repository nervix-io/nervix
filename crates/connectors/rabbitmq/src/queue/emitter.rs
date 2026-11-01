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

use async_trait::async_trait;
use nervix_connector::{
    BrokerPublishingMode, EmitterFinishResult, EmitterHost, EmitterLifecycle, EmitterRecord,
    EmitterRecordId, EmitterStartResult, PerRecordOutcome, RecordEmitter,
};
use nervix_dns::DnsResolver;
use nervix_models::{ClientConfigEntry, RabbitMqQueueName};
use nervix_primitives::time::Instant;

use crate::publisher::{RabbitMqDestination, RabbitMqPublisher, RabbitMqPublisherConfig};

/// What one RabbitMQ queue emitter publishes through: its client entries, the node resolver its
/// broker host resolves through, its queue and its publishing mode.
pub struct RabbitMqQueueEmitterConfig {
    pub config: Vec<ClientConfigEntry>,
    pub dns: DnsResolver,
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

    /// Connects, opens the channel the emitter publishes through and checks that its queue exists,
    /// every step within the client's `connect_timeout_ms`.
    pub async fn new(
        config: RabbitMqQueueEmitterConfig,
        _host: EmitterHost,
    ) -> EmitterStartResult<Self> {
        let RabbitMqQueueEmitterConfig {
            config,
            dns,
            queue,
            mode,
        } = config;
        let publisher = RabbitMqPublisher::open(RabbitMqPublisherConfig {
            config,
            dns,
            destination: RabbitMqDestination::Queue(queue),
            mode,
        })
        .await?;
        Ok(Self { publisher })
    }
}

#[async_trait]
impl EmitterLifecycle for RabbitMqQueueEmitter {
    fn kind(&self) -> nervix_models::EmitterEntityKind {
        Self::KIND
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
