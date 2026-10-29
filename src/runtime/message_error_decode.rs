//! The record of a source payload its ingestor's codec definitively refused, and how the ingestor's
//! general error policy reports it.
//!
//! Layer: data plane.
//! - **Owns.** Turning one codec refusal of a source payload into its decode rejection, naming the
//!   payload's transport position from its typed metadata, and reporting the rejection under the
//!   ingestor's `ON GENERAL ERROR` policy.
//! - **Depends on.** The codec's report, the source's typed ingest metadata, the message-error
//!   vocabulary and the node's runtime event bus.
//! - **Must not know.** The payload's bytes, the source's driver, or how the host settles the
//!   payload's position on its transport.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use std::{fmt, net::SocketAddr};

use nervix_connector::IngestMetadataRow;
use uuid::Uuid;

use super::*;

/// Where on its transport a refused payload was read, as its source's typed metadata names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TransportPosition {
    /// The topic, partition and offset of a Kafka record.
    Kafka {
        topic: String,
        partition: i32,
        offset: i64,
    },
    /// The peer a syslog frame came from.
    Syslog { peer: SocketAddr },
}

impl TransportPosition {
    /// The position `metadata` names. A source whose metadata holds only headers names none: a
    /// header value is what its sender wrote, never a position.
    fn of(metadata: &IngestMetadataRow<'_>) -> Option<Self> {
        match metadata {
            IngestMetadataRow::Kafka {
                topic,
                partition,
                offset,
                ..
            } => Some(Self::Kafka {
                topic: (*topic).to_string(),
                partition: *partition,
                offset: *offset,
            }),
            IngestMetadataRow::Syslog { peer_addr } => Some(Self::Syslog { peer: *peer_addr }),
            IngestMetadataRow::Headers { .. }
            | IngestMetadataRow::Http { .. }
            | IngestMetadataRow::Websockets { .. } => None,
        }
    }
}

impl fmt::Display for TransportPosition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Kafka {
                topic,
                partition,
                offset,
            } => write!(
                formatter,
                "Kafka topic '{topic}' partition {partition} offset {offset}"
            ),
            Self::Syslog { peer } => write!(formatter, "syslog peer {peer}"),
        }
    }
}

/// A source payload its ingestor's codec definitively refused, as the message error that remains of
/// it once the ingestor consumed it.
///
/// Presenting the payload again would only repeat the refusal, so the payload is consumed rather
/// than retried. Every decode rejection has the code [`Self::CODE`] and the operation
/// [`Self::OPERATION`]. It never holds the payload: its message is the codec's own diagnostic, which
/// names the codec and the field at fault and never a payload value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodeRejection {
    /// The rejection's stable reference, a UUIDv7.
    pub(crate) reference: Uuid,
    pub(crate) codec: CodecName,
    /// Where the source read the payload, when its metadata names a position.
    pub(crate) position: Option<TransportPosition>,
    /// The fields the codec named at fault.
    pub(crate) fields: SortedSet<FieldPath>,
    /// The codec's diagnostic, its whole chain rendered once.
    pub(crate) message: String,
    pub(crate) occurred_at: Timestamp,
}

impl DecodeRejection {
    /// The payload does not satisfy its codec's contract.
    pub(crate) const CODE: MessageErrorCode = MessageErrorCode::Validation;
    pub(crate) const OPERATION: MessageErrorOperation = MessageErrorOperation::Decode;

    /// The rejection of the payload a source read at `metadata` and `codec` refused with `report`,
    /// when the source took it in at `occurred_at`.
    pub(super) fn new(
        codec: &CompiledCodec,
        metadata: &IngestMetadataRow<'_>,
        report: &Report<CodecError>,
        occurred_at: Timestamp,
    ) -> Self {
        let mut fields = Vec::new();
        collect_refused_fields(report, &mut fields);
        Self {
            reference: Uuid::now_v7(),
            codec: CodecName::from(&codec.name),
            position: TransportPosition::of(metadata),
            fields: SortedSet::from_unsorted(fields),
            message: format!("{report:#}"),
            occurred_at,
        }
    }
}

impl fmt::Display for DecodeRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} at {}: codec '{}' refused the payload",
            self.reference,
            self.occurred_at.to_rfc3339(),
            self.codec.as_str()
        )?;
        if let Some(position) = &self.position {
            write!(formatter, " from {position}")?;
        }
        write!(formatter, ": {}", self.message)
    }
}

/// Collects the fields the codec contexts of `report` name at fault, including those of the message
/// an unfolding payload failed in, which its context keeps as a report of its own.
fn collect_refused_fields(report: &Report<CodecError>, fields: &mut Vec<FieldPath>) {
    for frame in report.frames() {
        let Some(context) = frame.downcast_ref::<CodecError>() else {
            continue;
        };
        if let Some(field) = context.field() {
            fields.push(FieldPath::new(field));
        }
        if let CodecError::Unfold { cause, .. } = context {
            collect_refused_fields(cause, fields);
        }
    }
}

/// What one ingestor's intake on this node consumes the payloads its codec refuses with, resolved
/// once when the intake starts: the ingestor's general error policy, the node's count of such
/// payloads and the status `DESCRIBE INGESTOR` reads the last of them from.
#[derive(Clone)]
pub(super) struct DecodeRejections {
    policy: GeneralErrorPolicy,
    count: prometheus::IntCounter,
    status: Arc<IngestorStatus>,
}

impl DecodeRejections {
    pub(super) fn new(
        policy: GeneralErrorPolicy,
        count: prometheus::IntCounter,
        status: Arc<IngestorStatus>,
    ) -> Self {
        Self {
            policy,
            count,
            status,
        }
    }

    /// Consumes the payload `rejection` stands for: reports it under the policy of `ingestor`,
    /// counts it, and publishes it as the ingestor's last decode rejection on this node.
    pub(super) fn consume(
        &self,
        runtime: &Runtime,
        domain: &DomainName,
        ingestor: &IngestorName,
        rejection: DecodeRejection,
    ) {
        runtime.report_decode_rejection(domain, ingestor, &self.policy, &rejection);
        self.count.inc();
        self.status.record_decode_rejection(rejection);
    }
}

impl Runtime {
    /// Reports `rejection` under the `ON GENERAL ERROR` policy of `ingestor`, whose codec refused
    /// the payload.
    ///
    /// No policy fails the payload, because presenting it again would repeat the refusal: its
    /// source consumes it whatever the policy says. `IGNORE` reports nothing, and `LOG` publishes
    /// the rejection as a runtime event and logs its fields. Either way the record stays visible
    /// in `DESCRIBE INGESTOR`, which the source host publishes it to.
    pub(in crate::runtime) fn report_decode_rejection(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
        policy: &GeneralErrorPolicy,
        rejection: &DecodeRejection,
    ) {
        match policy {
            GeneralErrorPolicy::Ignore => {
                debug!(
                    domain = domain.as_str(),
                    ingestor = ingestor.as_str(),
                    error_reference = %rejection.reference,
                    codec = rejection.codec.as_str(),
                    "consumed a payload its codec refused under an IGNORE general error policy"
                );
            }
            GeneralErrorPolicy::Log => {
                self.inner.events.report_error(format!(
                    "ingestor '{}' general error in domain '{}': decode rejection {rejection}",
                    ingestor.as_str(),
                    domain.as_str(),
                ));
                warn!(
                    domain = domain.as_str(),
                    node_kind = ModelKind::Ingestor.as_str(),
                    node = ingestor.as_str(),
                    error_reference = %rejection.reference,
                    error_code = DecodeRejection::CODE.as_ref(),
                    error_operation = DecodeRejection::OPERATION.as_ref(),
                    codec = rejection.codec.as_str(),
                    rejection = %rejection,
                    "runtime node consumed a payload its codec refused"
                );
            }
        }
    }
}

#[cfg(test)]
#[path = "message_error_decode_tests.rs"]
mod tests;
