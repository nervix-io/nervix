//! The Kafka connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of a Kafka ingestor source: its client, topic, offset ownership,
//!   instances, delivery mode and quiesce mode.
//! - **Depends on.** The shared ingestor clauses and the Kafka vocabulary.
//! - **Must not know.** The Kafka driver, consumer groups at run time or offset storage.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use std::num::NonZeroU64;

use chumsky::prelude::*;
use nervix_models::{IngestSource, KafkaIngestMode, KafkaOffsetMode, KafkaSource, KafkaTopicSource};

use super::delivery::{batch_timeout_parser, instance_count, suspend_only_quiesce_clause};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ack_timeout, client_ref, consumer_group_ref, kw, kw_phrase2,
        parallel_ack_window, retry_policy, sequential_ack_window, topic_ref,
    },
};

pub(super) fn mode_parser<'src>()
-> impl Parser<'src, &'src [Token], KafkaIngestMode, extra::Err<ParseError<'src>>> + Clone {
    let ack_timeout = ack_timeout();
    let batch_timeout = batch_timeout_parser();
    let retry_policy = retry_policy();
    choice((
        kw(Identifier::Ack)
            .ignore_then(parallel_ack_window())
            .then(batch_timeout)
            .then(ack_timeout.clone())
            .then(retry_policy.clone())
            .map(
                |(((max, batch_timeout), timeout), retry_policy)| KafkaIngestMode::AckParallel {
                    max,
                    batch_timeout,
                    timeout,
                    retry_policy,
                },
            ),
        kw(Identifier::Ack)
            .ignore_then(sequential_ack_window())
            .ignore_then(ack_timeout)
            .then(retry_policy)
            .map(|(timeout, retry_policy)| KafkaIngestMode::AckSequential {
                timeout,
                retry_policy,
            }),
        kw(Identifier::NoAck)
            .ignore_then(kw(Identifier::Parallel))
            .to(KafkaIngestMode::NoAckParallel),
    ))
}

pub(super) fn kafka_ingest_source_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestSource, extra::Err<ParseError<'src>>> + Clone {
    let offset_mode = kw(Identifier::Offset)
        .ignore_then(kw(Identifier::By))
        .ignore_then(choice((
            kw_phrase2(Identifier::Consumer, Identifier::Group)
                .ignore_then(consumer_group_ref())
                .map(KafkaOffsetMode::ConsumerGroup),
            kw(Identifier::Domain).to(KafkaOffsetMode::Domain),
        )));
    kw(Identifier::Kafka)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Topic))
        .then(topic_ref())
        .then(offset_mode)
        .then(
            kw(Identifier::Instances)
                .ignore_then(instance_count())
                .or_not()
                .map(|instances| instances.unwrap_or(NonZeroU64::MIN)),
        )
        .then_ignore(kw(Identifier::Mode))
        .then(mode_parser())
        .then(suspend_only_quiesce_clause())
        .map(
            |(((((client, topic), offset_mode), instances), mode), quiesce)| {
                IngestSource::Kafka(KafkaSource::Topic(KafkaTopicSource {
                    client,
                    topic,
                    offset_mode,
                    instances,
                    mode,
                    quiesce,
                }))
            },
        )
}
