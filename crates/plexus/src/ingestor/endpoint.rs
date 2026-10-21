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
use nervix_models::{EndpointIngestMode, EndpointName, IngestQuiesceMode};

use super::delivery::{endpoint_ack_quiesce_clause, endpoint_quiesce_clause, no_ack};
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
pub(super) fn intake<'src>()
-> impl Parser<'src, &'src [Token], EndpointIntake, extra::Err<ParseError<'src>>> + Clone {
    let unacknowledged = no_ack()
        .to(EndpointIngestMode::NoAck)
        .then(endpoint_quiesce_clause());
    let acknowledged = kw(Identifier::Ack)
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
        .then(endpoint_ack_quiesce_clause());
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
