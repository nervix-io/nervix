//! The Redis connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The Redis client Model with its connection pool, the Pub/Sub source and sink
//!   Models with their delivery mode, the channel name a source subscribes to with the rule it
//!   follows, and what each entity declares.
//! - **Depends on.** The shared source and sink declarations and the names a body references.
//! - **Must not know.** The Redis driver, how a connection is opened, or how a channel is
//!   subscribed or published to.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::{
    bounds::{
        BufferBound, BufferCapacity, BufferOverflow, DEFAULT_CONNECT_TIMEOUT, DeadlineKey,
        DeclaredBounds, DeclaredBuffer, DeclaredWait, OmittedDeadline, WaitBound,
    },
    configured_client::configured_client,
    entity_union::{connector_sinks, connector_sources},
    external_name::{ExternalNameError, external_names, require_length},
};
use crate::{
    AcknowledgementConfirmation, ChannelName, ClientName, ConnectorKind, DeclaredDelivery,
    DeclaredQuiesce, EmitSinkCapabilities, EmitterBatchLegality, EmitterBodyForm,
    EmitterPublishingModeKind, IngestAcknowledgement, IngestQuiesceMode, IngestSourceCapabilities,
    LocatorDeclaration, LocatorName, PositionClass, SinkBody, SinkClientReference, SinkEntity,
    SinkReference, SinkReferenceKind, SourceBody, SourceEntity, SourceMetadataScope,
    SourceReference, SourceReferenceKind,
};

configured_client!(Redis: pooled, config NervixWithAddressQuery, CreateClientRedis => RedisConfigEntry);

/// The longest channel Nervix subscribes to, in bytes.
const REDIS_CHANNEL_MAX: usize = 1024;

external_names! {
    /// A Redis Pub/Sub channel: 1 to 1,024 bytes. A Redis channel is binary-safe, so any text is a
    /// channel, and a `*` is the character itself: a pattern subscription is another entity.
    pub struct RedisChannelName => redis_channel_rule, label "redis_channel", example "orders";
}

fn redis_channel_rule(text: &str) -> Result<(), ExternalNameError> {
    require_length(text, REDIS_CHANNEL_MAX)
}

/// One Pub/Sub channel a Redis client subscribes to.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct RedisPubSubSource {
    pub client: ClientName,
    pub channel: RedisChannelName,
    pub mode: RedisPubSubIngestMode,
    pub quiesce: IngestQuiesceMode,
}

/// What the connector of a REDIS PUBSUB ingestor holds and how long it waits.
const REDIS_SOURCE_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[DeclaredBuffer {
        holds: "messages read ahead of intake",
        bound: BufferBound::Bounded {
            capacity: BufferCapacity::Setting("subscription_buffer_messages"),
            overflow: BufferOverflow::Backpressure,
        },
    }],
    waits: &[
        DeclaredWait {
            wait: "subscription: name resolution, address attempts, TLS, Redis setup and SUBSCRIBE",
            bound: WaitBound::Key {
                key: DeadlineKey::ConnectTimeout,
                omitted: OmittedDeadline::Default(DEFAULT_CONNECT_TIMEOUT),
            },
        },
        DeclaredWait {
            wait: "idle subscription activity, including its PING probe",
            bound: WaitBound::Key {
                key: DeadlineKey::IdleTimeout,
                omitted: OmittedDeadline::Required,
            },
        },
    ],
    driver_limits: &[],
};

/// What the connector of a REDIS PUBSUB emitter holds and how long it waits.
const REDIS_SINK_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[DeclaredBuffer {
        holds: "connections the command pool holds",
        bound: BufferBound::Bounded {
            capacity: BufferCapacity::Setting("POOL SIZE MAX"),
            overflow: BufferOverflow::Backpressure,
        },
    }],
    waits: &[
        DeclaredWait {
            wait: "checking out a connection, a new one's name resolution, connection, TLS and \
                   Redis setup included",
            bound: WaitBound::Key {
                key: DeadlineKey::ConnectTimeout,
                omitted: OmittedDeadline::Default(DEFAULT_CONNECT_TIMEOUT),
            },
        },
        DeclaredWait {
            wait: "the answer to a PUBLISH",
            bound: WaitBound::Driver("500 ms"),
        },
    ],
    driver_limits: &[],
};

impl SourceEntity for RedisPubSubSource {
    const CAPABILITIES: IngestSourceCapabilities = IngestSourceCapabilities {
        headers: None,
        metadata: SourceMetadataScope::Headers,
        executes_on_every_cluster_node: false,
        reference: SourceReferenceKind::Client(ConnectorKind::Redis),
        position_class: PositionClass::None,
        acknowledgement_confirmation: AcknowledgementConfirmation::None,
        delivery: &[DeclaredDelivery::NO_ACK],
        quiesce: &[DeclaredQuiesce::BUFFER, DeclaredQuiesce::DROP],
        locator: Some(LocatorDeclaration {
            keywords: "CHANNEL",
            name: LocatorName::External(RedisChannelName::DECLARATION),
        }),
        options: &[],
        bounds: REDIS_SOURCE_BOUNDS,
    };
}

impl SourceBody for RedisPubSubSource {
    fn reference(&self) -> SourceReference<'_> {
        SourceReference::Client(&self.client)
    }

    fn quiesce(&self) -> &IngestQuiesceMode {
        &self.quiesce
    }

    fn quiesce_mut(&mut self) -> &mut IngestQuiesceMode {
        &mut self.quiesce
    }

    fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        self.mode.acknowledgement()
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum RedisPubSubIngestMode {
    NoAck,
}

impl RedisPubSubIngestMode {
    pub const fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        match self {
            Self::NoAck => IngestAcknowledgement::Unacknowledged,
        }
    }
}

connector_sources! {
    connector Redis = "REDIS";
    /// What a Redis source reads.
    pub enum RedisSource;
    /// The alterations of the ingestors that read a Redis entity.
    pub enum RedisIngestorAlteration;
    /// The entities a Redis source reads.
    pub enum RedisSourceEntity {
        PubSub(RedisPubSubSource) = "PUBSUB", "Redis Pub/Sub";
    }
}

/// One Pub/Sub channel a Redis client publishes to.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct RedisPubSubSink {
    pub client: ClientName,
    pub channel: ChannelName,
}

impl SinkEntity for RedisPubSubSink {
    const CAPABILITIES: EmitSinkCapabilities = EmitSinkCapabilities {
        headers: None,
        reference: SinkReferenceKind::Client(ConnectorKind::Redis),
        body: EmitterBodyForm::Codec,
        batch: EmitterBatchLegality::Optional { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: &[EmitterPublishingModeKind::NoAck],
        locator: Some(LocatorDeclaration {
            keywords: "CHANNEL",
            name: LocatorName::Unquoted {
                label: "channel_name",
            },
        }),
        options: &[],
        envelope: &[],
        properties: &[],
        bounds: REDIS_SINK_BOUNDS,
    };
}

impl SinkBody for RedisPubSubSink {
    fn reference(&self) -> SinkReference<'_> {
        SinkReference::Client(SinkClientReference {
            client: &self.client,
            connector: ConnectorKind::Redis,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }
}

connector_sinks! {
    connector Redis = "REDIS";
    /// What a Redis sink writes.
    pub enum RedisSink;
    /// The entities a Redis sink writes.
    pub enum RedisSinkEntity {
        PubSub(RedisPubSubSink) = "PUBSUB";
    }
}
