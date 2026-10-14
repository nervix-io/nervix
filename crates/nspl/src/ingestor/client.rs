//! The ingestor grammar of application sessions as a source.
//!
//! Layer: language.
//!
//! - **Owns.** The body of a client ingestor source: the schema producers submit and the
//!   acknowledging mode they submit under.
//! - **Depends on.** The shared ingestor clauses and the client source vocabulary.
//! - **Must not know.** How a session attaches or a batch is admitted.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{ClientIngestMode, ClientProducerSource};

use super::delivery::suspend_only_quiesce_clause;
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, ack_timeout, ack_window, kw, retry_policy, schema_ref},
};

/// The acknowledging mode of a client source: its window, its ACK timeout and the retry policy a
/// producer applies to a batch refused before admission.
pub(super) fn client_mode_parser<'src>()
-> impl Parser<'src, &'src [Token], ClientIngestMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Ack)
        .ignore_then(ack_window())
        .then(ack_timeout())
        .then(retry_policy())
        .map(|((window, ack_timeout), retry_policy)| ClientIngestMode {
            window,
            ack_timeout,
            retry_policy,
        })
}

/// `CLIENT SCHEMA <schema> MODE ... ON QUIESCE SUSPEND`: typed batches application clients
/// submit through their sessions. Suspension is the only quiesce mode it honors, and it decodes
/// nothing, so no `DECODE USING` follows.
pub(super) fn client_ingest_source_parser<'src>()
-> impl Parser<'src, &'src [Token], ClientProducerSource, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Client)
        .ignore_then(kw(Identifier::Schema))
        .ignore_then(schema_ref())
        .then_ignore(kw(Identifier::Mode))
        .then(client_mode_parser())
        .then_ignore(suspend_only_quiesce_clause())
        .map(|(schema, mode)| ClientProducerSource { schema, mode })
}
