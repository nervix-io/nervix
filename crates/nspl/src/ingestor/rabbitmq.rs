//! The RabbitMQ connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of a `RABBITMQ QUEUE` ingestor: its client, queue, instances, delivery
//!   mode and quiesce mode.
//! - **Depends on.** The shared ingestor clauses and the RabbitMQ vocabulary.
//! - **Must not know.** The AMQP driver or how a delivery is acknowledged at run time.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{RabbitMqIngestMode, RabbitMqQueueName, RabbitMqQueueSource};

use super::delivery::{
    SequentialAck, ack_sequential, optional_instances, suspend_only_quiesce_clause,
};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, external_name, kw},
};

/// `FROM <client> QUEUE '<queue>' [INSTANCES <n>] MODE ACK SEQUENTIAL … ON QUIESCE SUSPEND`.
pub(super) fn queue_body<'src>()
-> impl Parser<'src, &'src [Token], RabbitMqQueueSource, extra::Err<ParseError<'src>>> + Clone {
    let mode = ack_sequential().map(
        |SequentialAck {
             timeout,
             retry_policy,
         }| RabbitMqIngestMode::AckSequential {
            timeout,
            retry_policy,
        },
    );
    client_ref()
        .then_ignore(kw(Identifier::Queue))
        .then(external_name("rabbitmq_queue", RabbitMqQueueName::parse))
        .then(optional_instances())
        .then_ignore(kw(Identifier::Mode))
        .then(mode)
        .then(suspend_only_quiesce_clause())
        .map(
            |((((client, queue), instances), mode), quiesce)| RabbitMqQueueSource {
                client,
                queue,
                instances,
                mode,
                quiesce,
            },
        )
        .boxed()
}
