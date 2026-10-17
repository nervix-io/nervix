//! The NATS connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of a `NATS SUBJECT` ingestor: its client, subject, queue group, instances,
//!   delivery mode and quiesce mode.
//! - **Depends on.** The shared ingestor clauses and the NATS vocabulary.
//! - **Must not know.** The NATS driver or how a queue subscription is held at run time.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{NatsIngestMode, NatsQueueGroupName, NatsSubjectName, NatsSubjectSource};

use super::delivery::{instance_count, no_ack, push_quiesce_clause};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, external_name, kw, kw_phrase2},
};

/// `FROM <client> SUBJECT '<subject>' QUEUE GROUP '<group>' INSTANCES <n> MODE NO ACK ON QUIESCE
/// BUFFER …|DROP`.
pub(super) fn subject_body<'src>()
-> impl Parser<'src, &'src [Token], NatsSubjectSource, extra::Err<ParseError<'src>>> + Clone {
    client_ref()
        .then_ignore(kw(Identifier::Subject))
        .then(external_name::<NatsSubjectName>())
        .then_ignore(kw_phrase2(Identifier::Queue, Identifier::Group))
        .then(external_name::<NatsQueueGroupName>())
        .then_ignore(kw(Identifier::Instances))
        .then(instance_count())
        .then_ignore(kw(Identifier::Mode))
        .then(no_ack().to(NatsIngestMode::NoAck))
        .then(push_quiesce_clause())
        .map(
            |(((((client, subject), queue_group), instances), mode), quiesce)| NatsSubjectSource {
                client,
                subject,
                queue_group,
                instances,
                mode,
                quiesce,
            },
        )
        .boxed()
}
