//! The HTTP connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The HTTP client Model and transport declaration, the polled request source with its delivery mode, the source
//!   that reads an HTTP endpoint this cluster serves, the request emitter Model, and what each entity
//!   declares.
//!   Client declarations also own public configuration keys, types and confidentiality.
//! - **Depends on.** The shared source and emitter declarations, expressions, the domain clock's
//!   period and the names a body references.
//! - **Must not know.** The HTTP client library, how a request is built,
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
    entity_union::{connector_emitters, connector_ingestors},
};
use crate::{
    AcknowledgementConfirmation, BatchUnavailability, ByteSizeUnit, ClientName, ConnectorKind,
    DeclaredDelivery, DeclaredQuiesce, DomainClockPeriod, EmitterBatchLegality, EmitterBodyForm,
    EmitterClientReference, EmitterDeclaration, EmitterEntityBody, EmitterEntityCapabilities,
    EmitterReference, EmitterReferenceKind, EndpointIngestMode, EndpointName, EndpointType,
    Expression, HeaderLayout, HeaderModel, IngestAcknowledgement, IngestQuiesceMode,
    IngestorDeclaration, IngestorEntityBody, IngestorEntityCapabilities, IngestorMetadataScope,
    IngestorReference, IngestorReferenceKind, OptionDeclaration, OptionForm, OptionPresence,
    OptionValue, PayloadSizeLimit, PositionClass,
};

configured_client!(Http: unpooled, config CLIENT_CONFIG, CreateClientHttp => HttpConfigEntry);

/// The HTTP client's declared transport rules. The CONFIG schema below names the settings that
/// opt into redirects or a proxy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpTransportContract {
    pub timeout: DeadlineKey,
    pub redirect_limit_without_opt_in: usize,
    pub redirect_limit_with_opt_in: usize,
    pub reads_environment_proxy: bool,
    pub redacts_targets_and_headers: bool,
    pub tls_identity_requires_pair: bool,
}

impl CreateClientHttp {
    pub const TRANSPORT: HttpTransportContract = HttpTransportContract {
        timeout: DeadlineKey::Timeout,
        redirect_limit_without_opt_in: 0,
        redirect_limit_with_opt_in: 10,
        reads_environment_proxy: false,
        redacts_targets_and_headers: true,
        tls_identity_requires_pair: true,
    };
}

/// The current keys this connector reads, shared by validation, completion and inspection.
const CLIENT_CONFIG: crate::ClientConfigSchema = {
    use crate::{ClientConfigKey as K, ClientConfigSchema, ClientConfigValue as V};
    ClientConfigSchema::new(
        &[
            K::required("endpoint", V::Url(&["http", "https"])),
            K::optional("follow_redirects", V::Boolean),
            K::optional("proxy", V::Url(&["http", "https"])),
            K::required("timeout_ms", V::Milliseconds),
            K::TLS_CA,
            K::TLS_CERT,
            K::TLS_KEY,
        ],
        &[],
        &[],
    )
};

/// The most bytes a polling request reads when its statement omits `MAX BODY SIZE`.
pub const HTTP_DEFAULT_MAX_BODY_SIZE: PayloadSizeLimit =
    match PayloadSizeLimit::new(nonzero_ext::nonzero!(16u64), ByteSizeUnit::MiB) {
        Some(limit) => limit,
        None => panic!("16 MiB fits in a 64-bit byte count"),
    };

/// One literal request header, in the polling statement's written order.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct HttpRequestHeader {
    pub name: String,
    pub value: String,
}

/// The request an HTTP client sends on a cadence of domain time, whose response is the payload.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct HttpRequestIngestor {
    pub client: ClientName,
    pub method: String,
    pub headers: Vec<HttpRequestHeader>,
    pub max_body_size: Option<PayloadSizeLimit>,
    pub every: DomainClockPeriod,
    pub mode: HttpRequestIngestMode,
    pub quiesce: IngestQuiesceMode,
}

impl HttpRequestIngestor {
    pub fn max_body_size_in_effect(&self) -> PayloadSizeLimit {
        self.max_body_size.unwrap_or(HTTP_DEFAULT_MAX_BODY_SIZE)
    }
}

/// What the connector of a HTTP REQUEST ingestor holds and how long it waits.
const HTTP_REQUEST_INGESTOR_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[DeclaredBuffer {
        holds: "the response body",
        bound: BufferBound::Bounded {
            capacity: BufferCapacity::Setting("MAX BODY SIZE"),
            overflow: BufferOverflow::FailPoll,
        },
    }],
    waits: &[DeclaredWait {
        wait: "a request and the redirects it follows: name resolution, connection, TLS and each \
               response",
        bound: WaitBound::Key {
            key: DeadlineKey::Timeout,
            omitted: OmittedDeadline::Required,
        },
    }],
    driver_limits: &[
        "HTTP/1.1 response heads above 100 fields or 417,792 bytes, and HTTP/2 header lists of \
         16,384 bytes or more as HTTP/2 counts them, are refused.",
        "Redirects and proxies require the client's explicit opt-in.",
    ],
};

/// What the connector of a HTTP ENDPOINT holds and how long it waits.
const HTTP_ENDPOINT_INGESTOR_BOUNDS: DeclaredBounds = DeclaredBounds {
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
const HTTP_REQUEST_EMITTER_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[],
    waits: &[DeclaredWait {
        wait: "a request and the redirects it follows: for each, name resolution within half of \
               what is left, each address's connection within an equal share of what is left \
               after it, the proxy's TLS and tunnel, TLS, writing it and its final response \
               headers",
        bound: WaitBound::Key {
            key: DeadlineKey::Timeout,
            omitted: OmittedDeadline::Required,
        },
    }],
    driver_limits: &[
        "HTTP/1.1 response heads above 100 fields or 417,792 bytes, and HTTP/2 header lists of \
         16,384 bytes or more as HTTP/2 counts them, are refused.",
        "Redirects and proxies require the client's explicit opt-in.",
    ],
};

impl IngestorDeclaration for HttpRequestIngestor {
    const CAPABILITIES: IngestorEntityCapabilities = IngestorEntityCapabilities {
        // An HTTP response's fields, as the client's header map keeps them: names in lowercase, every
        // value of a repeated field together under its name.
        headers: Some(HeaderModel::lowercase(HeaderLayout::RepeatsByName)),
        metadata: IngestorMetadataScope::Http,
        executes_on_every_cluster_node: false,
        reference: IngestorReferenceKind::Client(ConnectorKind::Http),
        position_class: PositionClass::None,
        acknowledgement_confirmation: AcknowledgementConfirmation::None,
        delivery: &[DeclaredDelivery::NO_ACK],
        quiesce: &[DeclaredQuiesce::SUSPEND, DeclaredQuiesce::BUFFER],
        locator: None,
        options: &[
            OptionDeclaration {
                keywords: "METHOD",
                presence: OptionPresence::Required,
                form: OptionForm::Value {
                    value: OptionValue::Text,
                    example: "METHOD 'GET'",
                },
            },
            OptionDeclaration {
                keywords: "HEADERS",
                presence: OptionPresence::Optional { default: None },
                form: OptionForm::Value {
                    value: OptionValue::LiteralMapping,
                    example: "HEADERS { 'accept' = 'application/json' }",
                },
            },
            OptionDeclaration {
                keywords: "MAX BODY SIZE",
                presence: OptionPresence::Optional {
                    default: Some("16MiB"),
                },
                form: OptionForm::Value {
                    value: OptionValue::Size,
                    example: "MAX BODY SIZE 1MiB",
                },
            },
            OptionDeclaration::EVERY,
        ],
        bounds: HTTP_REQUEST_INGESTOR_BOUNDS,
    };
}

impl IngestorEntityBody for HttpRequestIngestor {
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
pub struct HttpEndpointIngestor {
    pub endpoint: EndpointName,
    pub mode: EndpointIngestMode,
    pub quiesce: IngestQuiesceMode,
}

impl IngestorDeclaration for HttpEndpointIngestor {
    const CAPABILITIES: IngestorEntityCapabilities = IngestorEntityCapabilities {
        // An HTTP request's fields, as the server's header map keeps them: names in lowercase, every
        // value of a repeated field together under its name.
        headers: Some(HeaderModel::lowercase(HeaderLayout::RepeatsByName)),
        metadata: IngestorMetadataScope::Headers,
        executes_on_every_cluster_node: true,
        reference: IngestorReferenceKind::Endpoint(EndpointType::Http),
        position_class: PositionClass::None,
        acknowledgement_confirmation: AcknowledgementConfirmation::None,
        delivery: &[DeclaredDelivery::NO_ACK, DeclaredDelivery::ACK_REQUEST],
        quiesce: &[DeclaredQuiesce::REJECT, DeclaredQuiesce::ENDPOINT_BUFFER],
        locator: None,
        options: &[],
        bounds: HTTP_ENDPOINT_INGESTOR_BOUNDS,
    };
}

impl IngestorEntityBody for HttpEndpointIngestor {
    fn reference(&self) -> IngestorReference<'_> {
        IngestorReference::Endpoint(&self.endpoint)
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

connector_ingestors! {
    connector Http = "HTTP";
    /// What an HTTP source reads.
    pub enum HttpIngestor;
    /// The alterations of the ingestors that read an HTTP entity.
    pub enum HttpRequestIngestorAlteration;
    /// The entities an HTTP source reads.
    pub enum HttpIngestorEntity {
        Request(HttpRequestIngestor) = "REQUEST", "HTTP polling";
        Endpoint(HttpEndpointIngestor) = "ENDPOINT", "HTTP endpoint";
    }
}

/// The request an HTTP client sends for every published payload, with the method and path each
/// one computes.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct HttpRequestEmitter {
    pub mode: HttpRequestPublishingMode,
    pub client: ClientName,
    pub method: Expression,
    pub path: Expression,
}

impl EmitterDeclaration for HttpRequestEmitter {
    const CAPABILITIES: EmitterEntityCapabilities = EmitterEntityCapabilities {
        // A request's application fields: names in lowercase, a later write of a name replacing the
        // earlier one, written in ascending order of name.
        headers: Some(HeaderModel::lowercase(HeaderLayout::SortedByName)),
        reference: EmitterReferenceKind::Client(ConnectorKind::Http),
        body: EmitterBodyForm::CodecOrWithoutBody,
        batch: EmitterBatchLegality::Unavailable {
            reason: BatchUnavailability::OneRequestPerRecord,
        },
        requires_batch_transformation: false,
        publishing_modes: HttpRequestPublishingMode::KINDS,
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
        bounds: HTTP_REQUEST_EMITTER_BOUNDS,
    };
}

impl EmitterEntityBody for HttpRequestEmitter {
    fn reference(&self) -> EmitterReference<'_> {
        EmitterReference::Client(EmitterClientReference {
            client: &self.client,
            connector: ConnectorKind::Http,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }
}

connector_emitters! {
    connector Http = "HTTP";
    /// What an HTTP emitter writes.
    pub enum HttpEmitter;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum HttpRequestEmitterMode;
    /// The entities an HTTP emitter writes.
    pub enum HttpEmitterEntity {
        Request(HttpRequestEmitter, HttpRequestPublishingMode) = "REQUEST";
    }
}

super::publishing::emitter_modes! {
    pub enum HttpRequestPublishingMode {
        Ack { retry_policy: crate::RetryPolicy } => RequestAck,
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;

    use super::*;

    #[test]
    fn polling_declares_literal_request_fields_and_its_response_bound() {
        let [method, headers, body_size, every] = HttpRequestIngestor::CAPABILITIES.options else {
            panic!("HTTP polling declares four request options");
        };
        assert_eq!(method.keywords, "METHOD");
        assert_eq!(method.presence, OptionPresence::Required);
        assert!(matches!(
            method.form,
            OptionForm::Value {
                value: OptionValue::Text,
                ..
            }
        ));
        assert_eq!(headers.keywords, "HEADERS");
        assert_eq!(headers.presence, OptionPresence::Optional { default: None });
        assert!(matches!(
            headers.form,
            OptionForm::Value {
                value: OptionValue::LiteralMapping,
                ..
            }
        ));
        assert_eq!(body_size.keywords, "MAX BODY SIZE");
        let OptionPresence::Optional {
            default: Some(default),
        } = body_size.presence
        else {
            panic!("HTTP polling declares a default response bound");
        };
        assert_eq!(
            default
                .parse::<PayloadSizeLimit>()
                .assured("the declared default is a size literal"),
            HTTP_DEFAULT_MAX_BODY_SIZE
        );
        assert_eq!(every.keywords, "EVERY");
    }
}
