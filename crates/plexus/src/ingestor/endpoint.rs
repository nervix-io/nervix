//! The ingestor grammar of the endpoints a cluster serves.
//!
//! Layer: language.
//!
//! - **Owns.** The body an endpoint intake writes, whichever protocol its endpoint serves: the
//!   endpoint it reads, its delivery mode and its quiesce mode.
//! - **Depends on.** The shared ingestor clauses and the endpoint vocabulary.
//! - **Must not know.** How an endpoint is published, routed to or served, or which protocol it
//!   speaks; the HTTP and WebSockets grammars name that in their heads.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{
    EndpointIngestMode, EndpointName, IngestDeliveryKind, IngestQuiesceMode, IngestorEntityModel,
};

use super::delivery::{endpoint_quiesce_clause, no_ack, rate_limits_in};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, ack_timeout, endpoint_ref, kw, nonzero_u64_value},
};

/// What the body of an endpoint intake states.
#[derive(Debug, Clone)]
pub(super) struct EndpointIntake {
    pub(super) endpoint: EndpointName,
    pub(super) mode: EndpointIngestMode,
    pub(super) quiesce: IngestQuiesceMode,
}

/// The endpoint's delivery mode and the quiesce clause that mode can honor.
/// `ACK MAX <n> ACK TIMEOUT <d>`: up to `n` requests wait on their records' acknowledgements.
fn acknowledged_mode<'src>()
-> impl Parser<'src, &'src [Token], EndpointIngestMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Ack)
        .ignore_then(kw(Identifier::Max))
        .ignore_then(
            nonzero_u64_value("request_window", "endpoint MAX must be greater than 0").try_map(
                |max, span| {
                    std::num::NonZeroU16::try_from(max)
                        .map_err(|_| Rich::custom(span, "endpoint MAX must be at most 65535"))
                },
            ),
        )
        .then(ack_timeout())
        .map(|(max, timeout)| EndpointIngestMode::Ack { max, timeout })
}

/// The delivery modes of an endpoint: `NO ACK` or `ACK MAX <n> ACK TIMEOUT <d>`, read beside no
/// quiesce mode, so whether the two pair is checked on the complete ingestor.
pub(super) fn mode<'src>()
-> impl Parser<'src, &'src [Token], EndpointIngestMode, extra::Err<ParseError<'src>>> + Clone {
    choice((no_ack().to(EndpointIngestMode::NoAck), acknowledged_mode()))
}

/// The body of the endpoint intake of the entity `S`: its endpoint, its delivery mode, and the
/// quiesce clause that mode can honor. Clients send whenever they choose, so a `RATE LIMIT` after
/// either mode is refused as its entity declares.
pub(super) fn intake<'src, S: IngestorEntityModel>()
-> impl Parser<'src, &'src [Token], EndpointIntake, extra::Err<ParseError<'src>>> + Clone {
    let declared = S::KIND.capabilities().quiesce;
    let unacknowledged = no_ack()
        .to(EndpointIngestMode::NoAck)
        .then_ignore(rate_limits_in::<S>(IngestDeliveryKind::NoAck))
        .then(endpoint_quiesce_clause(declared, IngestDeliveryKind::NoAck));
    let acknowledged = acknowledged_mode()
        .then_ignore(rate_limits_in::<S>(IngestDeliveryKind::AckRequest))
        .then(endpoint_quiesce_clause(
            declared,
            IngestDeliveryKind::AckRequest,
        ));
    endpoint_ref()
        .then_ignore(kw(Identifier::Mode))
        .then(choice((unacknowledged, acknowledged)))
        .map(|(endpoint, (mode, quiesce))| EndpointIntake {
            endpoint,
            mode,
            quiesce,
        })
        .boxed()
}

#[cfg(test)]
#[path = "endpoint_ack_tests.rs"]
mod tests;
