//! The Kafka connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of a `KAFKA TOPIC` ingestor: its client, topic, offset ownership,
//!   instances, delivery mode and quiesce mode, and the delivery modes a Kafka consumer offers.
//! - **Depends on.** The shared ingestor clauses and the Kafka vocabulary.
//! - **Must not know.** The Kafka driver, consumer groups at run time or offset storage.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{
    KafkaConsumerGroupName, KafkaIngestMode, KafkaOffsetMode, KafkaTopicIngestor, KafkaTopicName,
};

use super::delivery::{
    ParallelAck, SequentialAck, ack_parallel, ack_sequential, no_ack, optional_instances,
    suspend_only_quiesce_clause,
};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, external_name, kw, kw_phrase2},
};

/// `FROM <client> TOPIC '<topic>' OFFSET BY DOMAIN|CONSUMER GROUP '<group>' [INSTANCES <n>] MODE …
/// ON QUIESCE SUSPEND`.
pub(super) fn topic_body<'src>()
-> impl Parser<'src, &'src [Token], KafkaTopicIngestor, extra::Err<ParseError<'src>>> + Clone {
    let offset_mode = kw(Identifier::Offset)
        .ignore_then(kw(Identifier::By))
        .ignore_then(choice((
            kw_phrase2(Identifier::Consumer, Identifier::Group)
                .ignore_then(external_name::<KafkaConsumerGroupName>())
                .map(KafkaOffsetMode::ConsumerGroup),
            kw(Identifier::Domain).to(KafkaOffsetMode::Domain),
        )));
    client_ref()
        .then_ignore(kw(Identifier::Topic))
        .then(external_name::<KafkaTopicName>())
        .then(offset_mode)
        .then(optional_instances())
        .then_ignore(kw(Identifier::Mode))
        .then(mode())
        .then(suspend_only_quiesce_clause())
        .map(
            |(((((client, topic), offset_mode), instances), mode), quiesce)| KafkaTopicIngestor {
                client,
                topic,
                offset_mode,
                instances,
                mode,
                quiesce,
            },
        )
        .boxed()
}

/// The delivery modes of a Kafka consumer: `NO ACK`, `ACK SEQUENTIAL …` or `ACK PARALLEL …`.
fn mode<'src>()
-> impl Parser<'src, &'src [Token], KafkaIngestMode, extra::Err<ParseError<'src>>> + Clone {
    choice((
        ack_parallel().map(
            |ParallelAck {
                 max,
                 batch_timeout,
                 timeout,
                 retry_policy,
             }| KafkaIngestMode::AckParallel {
                max,
                batch_timeout,
                timeout,
                retry_policy,
            },
        ),
        ack_sequential().map(
            |SequentialAck {
                 timeout,
                 retry_policy,
             }| KafkaIngestMode::AckSequential {
                timeout,
                retry_policy,
            },
        ),
        no_ack().to(KafkaIngestMode::NoAck),
    ))
}
