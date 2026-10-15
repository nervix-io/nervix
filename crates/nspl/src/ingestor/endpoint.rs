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

use super::delivery::{endpoint_quiesce_clause, no_ack};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, endpoint_ref, kw},
};

/// What the body of an endpoint intake states.
#[derive(Debug, Clone)]
pub(super) struct EndpointIntake {
    pub(super) endpoint: EndpointName,
    pub(super) mode: EndpointIngestMode,
    pub(super) quiesce: IngestQuiesceMode,
}

/// `FROM <endpoint> MODE NO ACK ON QUIESCE REJECT RETRY AFTER <d>|BUFFER MAX SIZE <size>`.
pub(super) fn intake<'src>()
-> impl Parser<'src, &'src [Token], EndpointIntake, extra::Err<ParseError<'src>>> + Clone {
    endpoint_ref()
        .then_ignore(kw(Identifier::Mode))
        .then(no_ack().to(EndpointIngestMode::NoAck))
        .then(endpoint_quiesce_clause())
        .map(|((endpoint, mode), quiesce)| EndpointIntake {
            endpoint,
            mode,
            quiesce,
        })
        .boxed()
}
