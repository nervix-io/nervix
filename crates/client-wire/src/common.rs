//! Values every message family shares: request identity, diagnostics, leader endpoints and entity
//! references.

use std::{fmt, num::NonZeroU64};

use error_stack::Report;
use flatbuffers::{ForwardsUOffset, Vector, WIPOffset};
use nervix_models::{ClusterNodeName, ModelKind, ModelName, NodeRef};
use thiserror::Error;
use url::Url;

use crate::{
    codec::{DecodeError, Decoder, EncodeError, Encoder, wire_enum},
    wire,
};

/// The correlation identity of one request within a session.
///
/// It is unrelated to a command's durable execution reference: it only pairs replies with the
/// request that is waiting for them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestId(NonZeroU64);

impl RequestId {
    pub const fn new(id: NonZeroU64) -> Self {
        Self(id)
    }

    pub const fn get(self) -> NonZeroU64 {
        self.0
    }

    pub(crate) const fn wire(self) -> u64 {
        self.0.get()
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        field: &'static str,
        value: u64,
    ) -> Result<Self, Report<DecodeError>> {
        let id = decoder.non_zero(field, value)?;
        Ok(Self(id))
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Why a protocol value refused to be constructed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WireValueError {
    #[error("source span {start}..{end} ends before it starts")]
    ReversedSourceSpan { start: u32, end: u32 },
    #[error("cursor {cursor} is not a character boundary of a {length}-byte input")]
    CursorOffCharBoundary { cursor: usize, length: usize },
    #[error("{applied} applied operations exceed {accepted} accepted operations")]
    AppliedOperationsExceedAccepted { applied: usize, accepted: usize },
}

/// A byte range of the NSPL source text a request carried.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SourceSpan {
    start: u32,
    end: u32,
}

impl SourceSpan {
    pub fn new(start: u32, end: u32) -> Result<Self, Report<WireValueError>> {
        if end < start {
            return Err(Report::new(WireValueError::ReversedSourceSpan {
                start,
                end,
            }));
        }
        Ok(Self { start, end })
    }

    pub const fn start(self) -> u32 {
        self.start
    }

    pub const fn end(self) -> u32 {
        self.end
    }

    fn decode(span: &wire::SourceSpan) -> Result<Self, Report<DecodeError>> {
        match Self::new(span.start(), span.end()) {
            Ok(span) => Ok(span),
            Err(error) => Err(error.change_context(DecodeError::InvalidValue {
                field: "Diagnostic.span",
                kind: "source span",
            })),
        }
    }
}

/// One problem found while serving a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic {
    pub message: String,
    /// Where the problem is in the request's source text, when it has a location.
    pub span: Option<SourceSpan>,
}

impl Diagnostic {
    pub(crate) fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::Diagnostic<'fbb>>, Report<EncodeError>> {
        let message = encoder.text("Diagnostic.message", &self.message)?;
        let span = self
            .span
            .map(|span| wire::SourceSpan::new(span.start, span.end));
        Ok(wire::Diagnostic::create(
            encoder.fbb(),
            &wire::DiagnosticArgs {
                message: Some(message),
                span: span.as_ref(),
            },
        ))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        table: wire::Diagnostic<'_>,
    ) -> Result<Self, Report<DecodeError>> {
        let message = decoder.text("Diagnostic.message", table.message())?;
        let span = match table.span() {
            Some(span) => Some(SourceSpan::decode(span)?),
            None => None,
        };
        Ok(Self { message, span })
    }

    pub(crate) fn encode_all<'fbb>(
        encoder: &mut Encoder<'fbb>,
        field: &'static str,
        diagnostics: &[Self],
    ) -> Result<WIPOffset<Vector<'fbb, ForwardsUOffset<wire::Diagnostic<'fbb>>>>, Report<EncodeError>>
    {
        encoder.table_vector(field, diagnostics, Self::encode)
    }

    pub(crate) fn decode_all<'a>(
        decoder: Decoder<'_>,
        field: &'static str,
        diagnostics: Vector<'a, ForwardsUOffset<wire::Diagnostic<'a>>>,
    ) -> Result<Vec<Self>, Report<DecodeError>> {
        decoder.table_vector(field, diagnostics, |diagnostic| {
            Self::decode(decoder, diagnostic)
        })
    }
}

/// How clients reach the cluster leader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderEndpoints {
    pub node: ClusterNodeName,
    /// The leader's gRPC session URI, when it advertises one.
    pub grpc_uri: Option<Url>,
    /// The leader's web console URI, when it advertises one.
    pub web_console_uri: Option<Url>,
}

impl LeaderEndpoints {
    pub(crate) fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::LeaderEndpoints<'fbb>>, Report<EncodeError>> {
        let node = encoder.text("LeaderEndpoints.node", self.node.as_str())?;
        let grpc_uri = match &self.grpc_uri {
            Some(uri) => Some(encoder.text("LeaderEndpoints.grpc_uri", uri.as_str())?),
            None => None,
        };
        let web_console_uri = match &self.web_console_uri {
            Some(uri) => Some(encoder.text("LeaderEndpoints.web_console_uri", uri.as_str())?),
            None => None,
        };
        Ok(wire::LeaderEndpoints::create(
            encoder.fbb(),
            &wire::LeaderEndpointsArgs {
                node: Some(node),
                grpc_uri,
                web_console_uri,
            },
        ))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        table: wire::LeaderEndpoints<'_>,
    ) -> Result<Self, Report<DecodeError>> {
        let node = decoder.name("LeaderEndpoints.node", table.node())?;
        let grpc_uri = decode_uri(decoder, "LeaderEndpoints.grpc_uri", table.grpc_uri())?;
        let web_console_uri = decode_uri(
            decoder,
            "LeaderEndpoints.web_console_uri",
            table.web_console_uri(),
        )?;
        Ok(Self {
            node,
            grpc_uri,
            web_console_uri,
        })
    }
}

fn decode_uri(
    decoder: Decoder<'_>,
    field: &'static str,
    value: Option<&str>,
) -> Result<Option<Url>, Report<DecodeError>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = decoder.check_text(field, value)?;
    match Url::parse(value) {
        Ok(uri) => Ok(Some(uri)),
        Err(error) => {
            Err(Report::new(error).change_context(DecodeError::InvalidValue { field, kind: "URI" }))
        }
    }
}

/// The request needs the cluster leader, and the serving node is not the leader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderRedirect {
    /// The current leader, or `None` while no leader is known.
    pub leader: Option<LeaderEndpoints>,
}

impl LeaderRedirect {
    pub(crate) fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::LeaderRedirect<'fbb>>, Report<EncodeError>> {
        let leader = match &self.leader {
            Some(leader) => Some(leader.encode(encoder)?),
            None => None,
        };
        Ok(wire::LeaderRedirect::create(
            encoder.fbb(),
            &wire::LeaderRedirectArgs { leader },
        ))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        table: wire::LeaderRedirect<'_>,
    ) -> Result<Self, Report<DecodeError>> {
        let leader = match table.leader() {
            Some(leader) => Some(LeaderEndpoints::decode(decoder, leader)?),
            None => None,
        };
        Ok(Self { leader })
    }
}

/// Whether an outcome was produced now or recovered from the durable record of an earlier attempt
/// with the same identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OutcomeOrigin {
    Executed,
    Recovered,
}

wire_enum!(ALL_OUTCOME_ORIGINS: OutcomeOrigin => wire::OutcomeOrigin { Executed, Recovered });

wire_enum!(ALL_MODEL_KINDS: ModelKind => wire::ModelKind {
    Schema,
    WireJsonSchema,
    WireCborSchema,
    WireAvroSchema,
    Codec,
    Client,
    Vhost,
    Branch,
    Endpoint,
    SignalingProtocol,
    Generator,
    Inferencer,
    WasmProcessor,
    Ingestor,
    Reingestor,
    Relay,
    Lookup,
    Junction,
    Deduplicator,
    Correlator,
    Reorderer,
    WindowProcessor,
    Emitter,
    Placement,
    Udf,
});

/// Encodes one entity reference.
pub(crate) fn encode_node_ref<'fbb>(
    encoder: &mut Encoder<'fbb>,
    node: &NodeRef,
) -> Result<WIPOffset<wire::NodeRef<'fbb>>, Report<EncodeError>> {
    let name = encoder.text("NodeRef.name", node.identifier.as_str())?;
    Ok(wire::NodeRef::create(
        encoder.fbb(),
        &wire::NodeRefArgs {
            kind: Some(node.kind.into()),
            name: Some(name),
        },
    ))
}

/// Decodes one entity reference.
pub(crate) fn decode_node_ref(
    decoder: Decoder<'_>,
    table: wire::NodeRef<'_>,
) -> Result<NodeRef, Report<DecodeError>> {
    let kind: ModelKind = decoder.required_enumeration("NodeRef.kind", table.kind())?;
    let identifier: ModelName = decoder.name("NodeRef.name", table.name())?;
    Ok(NodeRef::new(kind, identifier))
}
