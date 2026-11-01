//! The Redis connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The Redis client Model with its connection pool, the Pub/Sub source and emitter
//!   Models with their delivery mode, the channel or channel pattern a source subscribes to and the
//!   channel an emitter publishes to with the rules they follow, the metadata a delivered message
//!   carries, and what each entity declares.
//!   Client declarations also own public configuration keys, types and confidentiality.
//! - **Depends on.** The shared ingestor and emitter declarations and the names a body references.
//! - **Must not know.** The Redis driver, how a connection is opened, or how a channel is
//!   subscribed or published to.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::fmt;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::{
    bounds::{
        BufferBound, BufferCapacity, BufferOverflow, DEFAULT_CONNECT_TIMEOUT, DeadlineKey,
        DeclaredBounds, DeclaredBuffer, DeclaredWait, OmittedDeadline, WaitBound,
    },
    configured_client::configured_client,
    entity_union::{connector_emitters, connector_ingestors},
    external_name::{ExternalNameError, external_names, require_length},
};
use crate::{
    AcknowledgementConfirmation, ClientName, ConnectorKind, DeclaredDelivery, DeclaredQuiesce,
    EmitterBatchLegality, EmitterBodyForm, EmitterClientReference, EmitterDeclaration,
    EmitterEntityBody, EmitterEntityCapabilities, EmitterReference, EmitterReferenceKind,
    IngestAcknowledgement, IngestQuiesceMode, IngestorDeclaration, IngestorEntityBody,
    IngestorEntityCapabilities, IngestorMetadataDeclaration, IngestorMetadataField,
    IngestorMetadataType, IngestorReference, IngestorReferenceKind, LocatorDeclaration,
    LocatorName, PositionClass,
};

configured_client!(Redis: pooled, config CLIENT_CONFIG, CreateClientRedis => RedisConfigEntry);

/// The current keys this connector reads, shared by validation, completion and inspection.
const CLIENT_CONFIG: crate::ClientConfigSchema = {
    use crate::{ClientConfigKey as K, ClientConfigSchema, ClientConfigValue as V};
    ClientConfigSchema::new(
        &[
            K::required("addr", V::DriverAddress),
            K::CONNECT_TIMEOUT,
            K::IDLE_TIMEOUT,
            K::defaulted("pool_idle_timeout_ms", V::Milliseconds, "600000"),
            K::defaulted("pool_max_lifetime_ms", V::Milliseconds, "1800000"),
            K::optional("subscription_buffer_messages", V::PositiveInteger),
            K::TLS_CA,
            K::TLS_CERT,
            K::TLS_KEY,
        ],
        &[],
        &[],
    )
};

/// The longest channel or channel pattern Nervix subscribes or publishes to, in bytes.
const REDIS_CHANNEL_MAX: usize = 1024;

external_names! {
    /// A Redis Pub/Sub channel: 1 to 1,024 bytes. A Redis channel is binary-safe, so any text is a
    /// channel, and a `*` is the character itself. The same rule serves both directions; neither
    /// trims, lowercases or interprets the channel as a subscription pattern.
    pub struct RedisChannelName => redis_channel_rule, label "redis_channel", example "orders";
    /// A Redis Pub/Sub channel pattern: 1 to 1,024 bytes that Redis matches, byte by byte, against
    /// the channel each message is published to. `*` matches any run of bytes, `?` any one byte,
    /// `[...]` one byte of a set or range, `[^...]` one byte outside it, and `\` makes the next byte
    /// literal. Redis accepts any text as a pattern, so its rule is the channel's length bound, and
    /// Nervix sends it exactly as written.
    pub struct RedisChannelPattern => redis_channel_rule,
        label "redis_channel_pattern", example "orders.*";
}

fn redis_channel_rule(text: &str) -> Result<(), ExternalNameError> {
    require_length(text, REDIS_CHANNEL_MAX)
}

/// `CHANNEL '<channel>'`: the one channel of that exact name, which both directions locate.
const CHANNEL_LOCATOR: LocatorDeclaration = LocatorDeclaration {
    keywords: "CHANNEL",
    name: LocatorName::External(RedisChannelName::DECLARATION),
};

/// `PATTERN '<pattern>'`: every channel a pattern matches, which only a subscription locates.
const PATTERN_LOCATOR: LocatorDeclaration = LocatorDeclaration {
    keywords: "PATTERN",
    name: LocatorName::External(RedisChannelPattern::DECLARATION),
};

/// What a REDIS PUBSUB ingestor subscribes to.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum RedisSubscriptionTarget {
    /// The one channel of this exact name, through Redis's `SUBSCRIBE`.
    Channel(RedisChannelName),
    /// Every channel this pattern matches, through Redis's `PSUBSCRIBE`.
    Pattern(RedisChannelPattern),
}

impl RedisSubscriptionTarget {
    /// The declared locator a statement writes this subscription with.
    pub fn locator(&self) -> LocatorDeclaration {
        match self {
            Self::Channel(_) => CHANNEL_LOCATOR,
            Self::Pattern(_) => PATTERN_LOCATOR,
        }
    }

    /// The channel or the pattern, exactly as written.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Channel(channel) => channel.as_str(),
            Self::Pattern(pattern) => pattern.as_str(),
        }
    }
}

/// A subscription reads as what Redis subscribes to: `channel 'orders'` or `pattern 'orders.*'`.
impl fmt::Display for RedisSubscriptionTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Channel(channel) => write!(formatter, "channel '{channel}'"),
            Self::Pattern(pattern) => write!(formatter, "pattern '{pattern}'"),
        }
    }
}

/// One Pub/Sub subscription a Redis client holds: one channel, or every channel a pattern matches.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct RedisPubSubIngestor {
    pub client: ClientName,
    pub subscription: RedisSubscriptionTarget,
    pub mode: RedisPubSubIngestMode,
    pub quiesce: IngestQuiesceMode,
}

/// The metadata of every message a subscription delivers, in the order its source hands it over:
/// the channel the message was published to, and the pattern that matched it, which a channel
/// subscription leaves null.
const REDIS_PUBSUB_METADATA: IngestorMetadataDeclaration = IngestorMetadataDeclaration::new(&[
    IngestorMetadataField::new("channel", IngestorMetadataType::String),
    IngestorMetadataField::new("pattern", IngestorMetadataType::String),
]);

/// What the connector of a REDIS PUBSUB ingestor holds and how long it waits.
const REDIS_INGESTOR_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[DeclaredBuffer {
        holds: "messages read ahead of intake",
        bound: BufferBound::Bounded {
            capacity: BufferCapacity::Setting("subscription_buffer_messages"),
            overflow: BufferOverflow::Backpressure,
        },
    }],
    waits: &[
        DeclaredWait {
            wait: "subscription: name resolution, address attempts, TLS, Redis setup and \
                   SUBSCRIBE or PSUBSCRIBE",
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
const REDIS_EMITTER_BOUNDS: DeclaredBounds = DeclaredBounds {
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

impl IngestorDeclaration for RedisPubSubIngestor {
    const CAPABILITIES: IngestorEntityCapabilities = IngestorEntityCapabilities {
        // Redis Pub/Sub carries the channel, the pattern that matched it and the payload, without
        // transport headers.
        headers: None,
        metadata: REDIS_PUBSUB_METADATA,
        executes_on_every_cluster_node: false,
        reference: IngestorReferenceKind::Client(ConnectorKind::Redis),
        position_class: PositionClass::None,
        acknowledgement_confirmation: AcknowledgementConfirmation::None,
        delivery: &[DeclaredDelivery::NO_ACK],
        // Redis has no retained Pub/Sub backlog. Intake must keep reading and buffer or drop.
        quiesce: &[DeclaredQuiesce::BUFFER, DeclaredQuiesce::DROP],
        locator: Some(CHANNEL_LOCATOR),
        options: &[],
        bounds: REDIS_INGESTOR_BOUNDS,
    };

    const ALTERNATIVE_LOCATORS: &'static [LocatorDeclaration] = &[PATTERN_LOCATOR];
}

impl IngestorEntityBody for RedisPubSubIngestor {
    fn reference(&self) -> IngestorReference<'_> {
        IngestorReference::Client(&self.client)
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

connector_ingestors! {
    connector Redis = "REDIS";
    /// What a Redis source reads.
    pub enum RedisIngestor;
    /// The alterations of the ingestors that read a Redis entity.
    pub enum RedisIngestorAlteration;
    /// The entities a Redis source reads.
    pub enum RedisIngestorEntity {
        PubSub(RedisPubSubIngestor) = "PUBSUB", "Redis Pub/Sub";
    }
}

/// One Pub/Sub channel a Redis client publishes to.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct RedisPubSubEmitter {
    pub mode: RedisPubSubPublishingMode,
    pub client: ClientName,
    pub channel: RedisChannelName,
}

impl EmitterDeclaration for RedisPubSubEmitter {
    const CAPABILITIES: EmitterEntityCapabilities = EmitterEntityCapabilities {
        // PUBLISH accepts a channel and payload, without transport headers or an envelope.
        headers: None,
        reference: EmitterReferenceKind::Client(ConnectorKind::Redis),
        body: EmitterBodyForm::Codec,
        // The server enforces proto-max-bulk-len; this transport learns no size ceiling.
        batch: EmitterBatchLegality::Optional { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: RedisPubSubPublishingMode::KINDS,
        locator: Some(CHANNEL_LOCATOR),
        options: &[],
        envelope: &[],
        properties: &[],
        bounds: REDIS_EMITTER_BOUNDS,
    };
}

impl EmitterEntityBody for RedisPubSubEmitter {
    fn reference(&self) -> EmitterReference<'_> {
        EmitterReference::Client(EmitterClientReference {
            client: &self.client,
            connector: ConnectorKind::Redis,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }
}

connector_emitters! {
    connector Redis = "REDIS";
    /// What a Redis emitter writes.
    pub enum RedisEmitter;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum RedisPubSubEmitterMode;
    /// The entities a Redis emitter writes.
    pub enum RedisEmitterEntity {
        PubSub(RedisPubSubEmitter, RedisPubSubPublishingMode) = "PUBSUB";
    }
}

super::publishing::emitter_modes! {
    pub enum RedisPubSubPublishingMode {
        NoAck { retry_policy: crate::RetryPolicy } => NoAck,
    }
}

#[cfg(test)]
#[path = "redis_tests.rs"]
mod tests;
