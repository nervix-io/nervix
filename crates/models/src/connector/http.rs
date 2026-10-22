//! The HTTP connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The HTTP client Model, the polled request source with its delivery mode, the source
//!   that reads an HTTP endpoint this cluster serves, the request sink Model, and what each entity
//!   declares.
//! - **Depends on.** The shared source and sink declarations, expressions, the domain clock's
//!   period and the names a body references.
//! - **Must not know.** The HTTP client library, its configuration keys, how a request is built,
//!   sent or classified, or how an endpoint is published, routed to or served.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::{
    bounds::{
        BufferBound, BufferCapacity, BufferOverflow, DeadlineKey, DeclaredBounds, DeclaredBuffer,
        DeclaredWait, OmittedDeadline, WaitBound,
    },
    configured_client::configured_client,
    entity_union::{connector_sinks, connector_sources},
};
use crate::{
    AcknowledgementConfirmation, BatchUnavailability, ClientName, ConnectorKind, DeclaredDelivery,
    DeclaredQuiesce, DomainClockPeriod, EmitSinkCapabilities, EmitterBatchLegality,
    EmitterBodyForm, EmitterPublishingModeKind, EndpointIngestMode, EndpointName, EndpointType,
    Expression, HeaderLayout, HeaderModel, IngestAcknowledgement, IngestQuiesceMode,
    IngestSourceCapabilities, OptionDeclaration, OptionForm, OptionPresence, OptionValue,
    PositionClass, SinkBody, SinkClientReference, SinkEntity, SinkReference, SinkReferenceKind,
    SourceBody, SourceEntity, SourceMetadataScope, SourceReference, SourceReferenceKind,
};

configured_client!(Http: unpooled, config Nervix, CreateClientHttp => HttpConfigEntry);

/// The request an HTTP client sends on a cadence of domain time, whose response is the payload.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct HttpRequestSource {
    pub client: ClientName,
    pub every: DomainClockPeriod,
    pub mode: HttpRequestIngestMode,
    pub quiesce: IngestQuiesceMode,
}

/// What the connector of a HTTP REQUEST ingestor holds and how long it waits.
const HTTP_REQUEST_SOURCE_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[DeclaredBuffer {
        holds: "the response body",
        bound: BufferBound::Unbounded,
    }],
    waits: &[DeclaredWait {
        wait: "a request: name resolution, connection, TLS and the response",
        bound: WaitBound::Key {
            key: DeadlineKey::Timeout,
            omitted: OmittedDeadline::Unbounded,
        },
    }],
    driver_limits: &[
        "Response heads above 100 headers or 417,792 bytes are refused.",
        "A request follows at most 10 redirects.",
    ],
};

/// What the connector of a HTTP ENDPOINT holds and how long it waits.
const HTTP_ENDPOINT_SOURCE_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[
        DeclaredBuffer {
            holds: "a request body",
            bound: BufferBound::Unbounded,
        },
        DeclaredBuffer {
            holds: "unresolved requests under MODE ACK, per node and ingestor",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Setting("MODE ACK MAX"),
                overflow: BufferOverflow::RefuseRequest,
            },
        },
    ],
    waits: &[
        DeclaredWait {
            wait: "a client's request head and body",
            bound: WaitBound::Unbounded,
        },
        DeclaredWait {
            wait: "graph acknowledgement under MODE ACK",
            bound: WaitBound::Clause("ACK TIMEOUT, or the exact intake lifetime ending"),
        },
    ],
    driver_limits: &["Request heads above 100 headers are refused."],
};

/// What the connector of a HTTP REQUEST emitter holds and how long it waits.
const HTTP_REQUEST_SINK_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[],
    waits: &[DeclaredWait {
        wait: "a request: name resolution within half of it, each address's connection within an \
               equal share of what is left, TLS, writing it and its final response headers",
        bound: WaitBound::Key {
            key: DeadlineKey::Timeout,
            omitted: OmittedDeadline::Required,
        },
    }],
    driver_limits: &["A response head above 74,240 bytes or 128 fields fails the attempt."],
};

impl SourceEntity for HttpRequestSource {
    const CAPABILITIES: IngestSourceCapabilities = IngestSourceCapabilities {
        // An HTTP response's fields, as the client's header map keeps them: names in lowercase, every
        // value of a repeated field together under its name.
        headers: Some(HeaderModel::lowercase(HeaderLayout::RepeatsByName)),
        metadata: SourceMetadataScope::Headers,
        executes_on_every_cluster_node: false,
        reference: SourceReferenceKind::Client(ConnectorKind::Http),
        position_class: PositionClass::None,
        acknowledgement_confirmation: AcknowledgementConfirmation::None,
        delivery: &[DeclaredDelivery::NO_ACK],
        quiesce: &[DeclaredQuiesce::SUSPEND, DeclaredQuiesce::BUFFER],
        locator: None,
        options: &[OptionDeclaration::EVERY],
        bounds: HTTP_REQUEST_SOURCE_BOUNDS,
    };
}

impl SourceBody for HttpRequestSource {
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

/// How the responses to a polled request are delivered. The next request reads the endpoint
/// again, so no response waits on an acknowledgement.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum HttpRequestIngestMode {
    NoAck,
}

impl HttpRequestIngestMode {
    pub const fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        match self {
            Self::NoAck => IngestAcknowledgement::Unacknowledged,
        }
    }
}

/// One endpoint of `TYPE HTTP` this cluster serves, whose requests are the payloads. Every node of
/// the cluster serves it.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct HttpEndpointSource {
    pub endpoint: EndpointName,
    pub mode: EndpointIngestMode,
    pub quiesce: IngestQuiesceMode,
}

impl SourceEntity for HttpEndpointSource {
    const CAPABILITIES: IngestSourceCapabilities = IngestSourceCapabilities {
        // An HTTP request's fields, as the server's header map keeps them: names in lowercase, every
        // value of a repeated field together under its name.
        headers: Some(HeaderModel::lowercase(HeaderLayout::RepeatsByName)),
        metadata: SourceMetadataScope::Headers,
        executes_on_every_cluster_node: true,
        reference: SourceReferenceKind::Endpoint(EndpointType::Http),
        position_class: PositionClass::None,
        acknowledgement_confirmation: AcknowledgementConfirmation::None,
        delivery: &[DeclaredDelivery::NO_ACK, DeclaredDelivery::ACK_REQUEST],
        quiesce: &[DeclaredQuiesce::REJECT, DeclaredQuiesce::ENDPOINT_BUFFER],
        locator: None,
        options: &[],
        bounds: HTTP_ENDPOINT_SOURCE_BOUNDS,
    };
}

impl SourceBody for HttpEndpointSource {
    fn reference(&self) -> SourceReference<'_> {
        SourceReference::Endpoint(&self.endpoint)
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

connector_sources! {
    connector Http = "HTTP";
    /// What an HTTP source reads.
    pub enum HttpSource;
    /// The alterations of the ingestors that read an HTTP entity.
    pub enum HttpIngestorAlteration;
    /// The entities an HTTP source reads.
    pub enum HttpSourceEntity {
        Request(HttpRequestSource) = "REQUEST", "HTTP polling";
        Endpoint(HttpEndpointSource) = "ENDPOINT", "HTTP endpoint";
    }
}

/// The request an HTTP client sends for every published payload, with the method and path each
/// one computes.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct HttpRequestSink {
    pub client: ClientName,
    pub method: Expression,
    pub path: Expression,
}

impl SinkEntity for HttpRequestSink {
    const CAPABILITIES: EmitSinkCapabilities = EmitSinkCapabilities {
        // A request's application fields: names in lowercase, a later write of a name replacing the
        // earlier one, written in ascending order of name.
        headers: Some(HeaderModel::lowercase(HeaderLayout::SortedByName)),
        reference: SinkReferenceKind::Client(ConnectorKind::Http),
        body: EmitterBodyForm::CodecOrWithoutBody,
        batch: EmitterBatchLegality::Unavailable {
            reason: BatchUnavailability::OneRequestPerRecord,
        },
        requires_batch_transformation: false,
        publishing_modes: &[EmitterPublishingModeKind::RequestAck],
        locator: None,
        options: &[
            OptionDeclaration {
                keywords: "METHOD",
                presence: OptionPresence::Required,
                form: OptionForm::Value {
                    value: OptionValue::Expression,
                    example: "METHOD 'POST'",
                },
            },
            OptionDeclaration {
                keywords: "PATH",
                presence: OptionPresence::Required,
                form: OptionForm::Value {
                    value: OptionValue::Expression,
                    example: "PATH '/events'",
                },
            },
        ],
        envelope: &[],
        properties: &[],
        bounds: HTTP_REQUEST_SINK_BOUNDS,
    };
}

impl SinkBody for HttpRequestSink {
    fn reference(&self) -> SinkReference<'_> {
        SinkReference::Client(SinkClientReference {
            client: &self.client,
            connector: ConnectorKind::Http,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }
}

connector_sinks! {
    connector Http = "HTTP";
    /// What an HTTP sink writes.
    pub enum HttpSink;
    /// The entities an HTTP sink writes.
    pub enum HttpSinkEntity {
        Request(HttpRequestSink) = "REQUEST";
    }
}
