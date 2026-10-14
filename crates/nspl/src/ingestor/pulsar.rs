//! The Pulsar connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of a Pulsar ingestor source: its client, topic, subscription,
//!   instances, delivery mode and quiesce mode.
//! - **Depends on.** The shared ingestor clauses, the Kafka delivery modes a Pulsar subscription shares,
//!   and the Pulsar vocabulary.
//! - **Must not know.** The Pulsar driver or how a subscription is acknowledged at run time.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use std::num::NonZeroU64;

use chumsky::prelude::*;
use nervix_models::{IngestSource, PulsarIngestMode, PulsarSource, PulsarTopicSource};

use super::{
    delivery::{instance_count, suspend_only_quiesce_clause},
    kafka::mode_parser,
};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, kw, subscription_ref, topic_ref},
};

pub(super) fn pulsar_mode_parser<'src>()
-> impl Parser<'src, &'src [Token], PulsarIngestMode, extra::Err<ParseError<'src>>> + Clone {
    mode_parser()
}

pub(super) fn pulsar_ingest_source_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestSource, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Pulsar)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Topic))
        .then(topic_ref())
        .then_ignore(kw(Identifier::Subscription))
        .then(subscription_ref())
        .then(
            kw(Identifier::Instances)
                .ignore_then(instance_count())
                .or_not()
                .map(|instances| instances.unwrap_or(NonZeroU64::MIN)),
        )
        .then_ignore(kw(Identifier::Mode))
        .then(pulsar_mode_parser())
        .then(suspend_only_quiesce_clause())
        .map(
            |(((((client, topic), subscription), instances), mode), quiesce)| {
                IngestSource::Pulsar(PulsarSource::Topic(PulsarTopicSource {
                    client,
                    topic,
                    subscription,
                    instances,
                    mode,
                    quiesce,
                }))
            },
        )
}
