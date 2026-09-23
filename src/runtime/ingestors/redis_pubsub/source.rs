//! Redis Pub/Sub source transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The dedicated subscription connection each source instance reads a channel
//!   through, opened from the same client construction the crate's command pool uses.
//! - **Depends on.** The connector contract, typed client configuration entries, `error-stack`,
//!   Tokio and `redis`.
//! - **Must not know.** Runtime collectors, relays, branches, schedules, registry state, or NSPL.

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use futures_util::StreamExt as _;
use nervix_connector::{
    BrokerSourceConnector, IngestMessageHeaders, IngestMetadataRow, NoIngestHeaders, SourceBatch,
    SourceBatchRequest, SourceConnector, SourceError, SourceMessage, SourceResult, SourceResume,
    client_config_value,
};
use nervix_connector_redis::redis_client;
use nervix_models::{ChannelName, ClientConfigEntry};
use redis::{Msg, aio::PubSubStream};
use thiserror::Error;

const REDIS: &str = "redis";

/// Why a Redis Pub/Sub source could not open its subscription.
#[derive(Debug, Error)]
pub enum RedisPubSubSourceError {
    #[error("invalid Redis client configuration")]
    ClientConfig,
    #[error("failed to build Redis client")]
    BuildClient,
    #[error("failed to connect Redis subscription")]
    Connect,
    #[error("failed to subscribe to Redis channel '{channel}'")]
    Subscribe { channel: String },
    #[error("Redis subscription closed")]
    SubscriptionClosed,
}

type RedisPubSubSourceResult<T> = Result<T, Report<RedisPubSubSourceError>>;

/// What one Redis Pub/Sub source subscribes to: its resolved client entries, the address they
/// name, and the channel.
#[derive(Debug, Clone)]
pub struct RedisPubSubSourcePlan {
    addr: String,
    config: Vec<ClientConfigEntry>,
    channel: ChannelName,
}

impl RedisPubSubSourcePlan {
    pub fn new(
        config: Vec<ClientConfigEntry>,
        channel: ChannelName,
    ) -> RedisPubSubSourceResult<Self> {
        let addr = client_config_value(&config, "addr", "Redis")
            .change_context(RedisPubSubSourceError::ClientConfig)?;
        Ok(Self {
            addr,
            config,
            channel,
        })
    }

    async fn subscribe(&self) -> RedisPubSubSourceResult<PubSubStream> {
        let client = redis_client(&self.addr, &self.config)
            .change_context(RedisPubSubSourceError::BuildClient)?;
        let mut pubsub = client.get_async_pubsub().await.map_err(|source| {
            Report::new(RedisPubSubSourceError::Connect).attach_printable(source.to_string())
        })?;
        pubsub
            .subscribe(self.channel.as_str())
            .await
            .map_err(|source| {
                Report::new(RedisPubSubSourceError::Subscribe {
                    channel: self.channel.as_str().to_string(),
                })
                .attach_printable(source.to_string())
            })?;
        Ok(pubsub.into_on_message())
    }
}

/// One received channel message. Redis Pub/Sub carries no headers.
pub struct RedisPubSubSourceMessage {
    message: Msg,
    position: (),
    headers: NoIngestHeaders,
}

impl SourceMessage for RedisPubSubSourceMessage {
    type Position = ();

    fn payload(&self) -> &[u8] {
        self.message.get_payload_bytes()
    }

    fn position(&self) -> &Self::Position {
        &self.position
    }

    fn headers(&self) -> &dyn IngestMessageHeaders {
        &self.headers
    }

    fn metadata(&self) -> IngestMetadataRow<'_> {
        IngestMetadataRow::Headers {
            headers: &self.headers,
        }
    }
}

/// One Redis Pub/Sub source instance: its subscription stream, absent while reconnecting.
pub struct RedisPubSubSource {
    plan: RedisPubSubSourcePlan,
    stream: Option<PubSubStream>,
}

#[async_trait]
impl SourceConnector for RedisPubSubSource {
    type Plan = RedisPubSubSourcePlan;

    async fn open(plan: &Self::Plan, _instance_index: u64) -> SourceResult<Self> {
        Ok(Self {
            plan: plan.clone(),
            stream: None,
        })
    }

    fn needs_resume(&mut self) -> bool {
        self.stream.is_none()
    }

    async fn suspend(&mut self) -> SourceResult<()> {
        self.stream = None;
        Ok(())
    }

    async fn resume(&mut self) -> SourceResult<SourceResume> {
        if self.stream.is_none() {
            let stream = self
                .plan
                .subscribe()
                .await
                .change_context(SourceError::Resume { connector: REDIS })?;
            self.stream = Some(stream);
        }
        Ok(SourceResume::Ready)
    }

    async fn close(&mut self) -> SourceResult<()> {
        self.stream = None;
        Ok(())
    }
}

#[async_trait]
impl BrokerSourceConnector for RedisPubSubSource {
    type Message = RedisPubSubSourceMessage;
    type Position = ();

    async fn next_batch(
        &mut self,
        _request: SourceBatchRequest,
    ) -> SourceResult<SourceBatch<Self::Message>> {
        let Some(stream) = self.stream.as_mut() else {
            return Ok(SourceBatch::ResumeRequired);
        };
        let Some(message) = stream.next().await else {
            self.stream = None;
            return Err(Report::new(RedisPubSubSourceError::SubscriptionClosed)
                .change_context(SourceError::Read { connector: REDIS }));
        };
        Ok(SourceBatch::Messages(vec![RedisPubSubSourceMessage {
            message,
            position: (),
            headers: NoIngestHeaders,
        }]))
    }

    async fn acknowledge(&mut self, _positions: &[Self::Position]) -> SourceResult<()> {
        Ok(())
    }

    async fn reject(&mut self, _positions: &[Self::Position]) -> SourceResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_plan_requires_the_address_before_it_subscribes() {
        let channel = ChannelName::parse("events").expect("a plain channel name parses");
        let error = RedisPubSubSourcePlan::new(Vec::new(), channel)
            .expect_err("a missing Redis address must fail");
        assert!(matches!(
            error.current_context(),
            RedisPubSubSourceError::ClientConfig
        ));
    }
}
