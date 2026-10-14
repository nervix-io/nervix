//! The NATS connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of a NATS ingestor source: its client, subject, queue group, instances,
//!   delivery mode and quiesce mode.
//! - **Depends on.** The shared ingestor clauses and the NATS vocabulary.
//! - **Must not know.** The NATS driver or how a queue subscription is held at run time.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{IngestSource, NatsIngestMode, NatsSource, NatsSubjectSource};

use super::delivery::{instance_count, push_quiesce_clause};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, client_ref, kw, kw_phrase2, nats_queue_group_ref, sequential_ack_window,
        subject_ref,
    },
};

pub(super) fn nats_mode_parser<'src>()
-> impl Parser<'src, &'src [Token], NatsIngestMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::NoAck)
        .ignore_then(sequential_ack_window())
        .to(NatsIngestMode::NoAckSequential)
}

pub(super) fn nats_ingest_source_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestSource, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Nats)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Subject))
        .then(subject_ref())
        .then_ignore(kw_phrase2(Identifier::Queue, Identifier::Group))
        .then(nats_queue_group_ref())
        .then_ignore(kw(Identifier::Instances))
        .then(instance_count())
        .then_ignore(kw(Identifier::Mode))
        .then(nats_mode_parser())
        .then(push_quiesce_clause())
        .map(
            |(((((client, subject), queue_group), instances), mode), quiesce)| {
                IngestSource::Nats(NatsSource::Subject(NatsSubjectSource {
                    client,
                    subject,
                    queue_group,
                    instances,
                    mode,
                    quiesce,
                }))
            },
        )
}
