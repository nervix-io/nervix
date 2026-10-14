//! The SQS connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of an SQS ingestor source: its client, queue, instances, delivery mode and
//!   quiesce mode.
//! - **Depends on.** The shared ingestor clauses and the SQS vocabulary.
//! - **Must not know.** The SQS SDK or how a received message is deleted at run time.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use std::num::NonZeroU64;

use chumsky::prelude::*;
use nervix_models::{IngestSource, SqsIngestMode, SqsQueueSource, SqsSource};

use super::delivery::{instance_count, suspend_only_quiesce_clause};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ack_timeout, client_ref, kw, queue_ref, retry_policy, sequential_ack_window,
    },
};

pub(super) fn sqs_mode_parser<'src>()
-> impl Parser<'src, &'src [Token], SqsIngestMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Ack)
        .ignore_then(sequential_ack_window())
        .ignore_then(ack_timeout())
        .then(retry_policy())
        .map(|(timeout, retry_policy)| SqsIngestMode::AckSequential {
            timeout,
            retry_policy,
        })
}

pub(super) fn sqs_ingest_source_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestSource, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Sqs)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Queue))
        .then(queue_ref())
        .then(
            kw(Identifier::Instances)
                .ignore_then(instance_count())
                .or_not()
                .map(|instances| instances.unwrap_or(NonZeroU64::MIN)),
        )
        .then_ignore(kw(Identifier::Mode))
        .then(sqs_mode_parser())
        .then(suspend_only_quiesce_clause())
        .map(|((((client, queue), instances), mode), quiesce)| {
            IngestSource::Sqs(SqsSource::Queue(SqsQueueSource {
                client,
                queue,
                instances,
                mode,
                quiesce,
            }))
        })
}
