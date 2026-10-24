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
use nervix_models::{RabbitMqIngestMode, RabbitMqQueueIngestor, RabbitMqQueueName};

use super::delivery::{
    ParallelAck, SequentialAck, ack_parallel, ack_sequential, no_ack, optional_instances,
    suspend_only_quiesce_clause,
};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, external_name, kw},
};

/// `FROM <client> QUEUE '<queue>' [INSTANCES <n>] MODE … ON QUIESCE SUSPEND`.
pub(super) fn queue_body<'src>()
-> impl Parser<'src, &'src [Token], RabbitMqQueueIngestor, extra::Err<ParseError<'src>>> + Clone {
    let sequential = ack_sequential().map(
        |SequentialAck {
             timeout,
             retry_policy,
         }| RabbitMqIngestMode::AckSequential {
            timeout,
            retry_policy,
        },
    );
    let parallel = ack_parallel().try_map(
        |ParallelAck {
             max,
             batch_timeout,
             timeout,
             retry_policy,
         },
         span| {
            let max = max.try_into().map_err(|_| {
                Rich::custom(
                    span,
                    "RabbitMQ ACK PARALLEL MAX must be between 1 and 65535",
                )
            })?;
            Ok(RabbitMqIngestMode::AckParallel {
                max,
                batch_timeout,
                timeout,
                retry_policy,
            })
        },
    );
    let mode = choice((parallel, sequential, no_ack().to(RabbitMqIngestMode::NoAck)));
    client_ref()
        .then_ignore(kw(Identifier::Queue))
        .then(external_name::<RabbitMqQueueName>())
        .then(optional_instances())
        .then_ignore(kw(Identifier::Mode))
        .then(mode)
        .then(suspend_only_quiesce_clause())
        .map(
            |((((client, queue), instances), mode), quiesce)| RabbitMqQueueIngestor {
                client,
                queue,
                instances,
                mode,
                quiesce,
            },
        )
        .boxed()
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests composed RabbitMQ modes")
)]
mod tests {
    use std::num::NonZeroU16;

    use meticulous::OptionExt as _;
    use nervix_models::{IngestorEntity, IngestorInput, Model, RabbitMqIngestor, RetryPolicy};

    use super::*;
    use crate::conformance_support::{assert_offers_exactly, created_model, refused};

    fn statement(mode: &str) -> String {
        format!(
            "CREATE RABBITMQ QUEUE INGESTOR events FROM broker QUEUE 'orders' MODE {mode} ON \
             QUIESCE SUSPEND DECODE USING codec TO events UNBRANCHED FLUSH IMMEDIATE ON MESSAGE \
             ERROR LOG ON GENERAL ERROR LOG;"
        )
    }

    #[test]
    fn every_consumer_mode_preserves_its_complete_acknowledgement_contract() {
        let retry_policy = RetryPolicy {
            backoff: "1s".into(),
            max_backoff: "30s".into(),
        };
        let cases = [
            ("NO ACK".to_string(), RabbitMqIngestMode::NoAck),
            (
                "ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s".to_string(),
                RabbitMqIngestMode::AckSequential {
                    timeout: "5s".into(),
                    retry_policy: retry_policy.clone(),
                },
            ),
            (
                "ACK PARALLEL MAX 65535 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF \
                 1s MAX 30s"
                    .to_string(),
                RabbitMqIngestMode::AckParallel {
                    max: NonZeroU16::MAX,
                    batch_timeout: "100ms".into(),
                    timeout: "5s".into(),
                    retry_policy,
                },
            ),
        ];
        for (text, expected) in cases {
            let model = created_model(&statement(&text)).assured("each complete mode is supported");
            let Model::Ingestor(ingestor) = model else {
                panic!("the statement creates an ingestor")
            };
            let IngestorInput::Transport(input) = ingestor.input else {
                panic!("RabbitMQ is a transport")
            };
            let IngestorEntity::RabbitMq(RabbitMqIngestor::Queue(source)) = input.source else {
                panic!("the declared source is a RabbitMQ queue")
            };
            assert_eq!(source.mode, expected);
        }
    }

    #[test]
    fn parallel_mode_requires_a_prefetch_that_amqp_can_carry_and_every_duration() {
        for mode in [
            "ACK PARALLEL MAX 0 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s",
            "ACK PARALLEL MAX 65536 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s \
             MAX 30s",
            "ACK PARALLEL MAX 4 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s",
            "ACK PARALLEL MAX 4 BATCH TIMEOUT 100ms RETRY POLICY BACKOFF 1s MAX 30s",
            "ACK PARALLEL MAX 4 BATCH TIMEOUT 100ms ACK TIMEOUT 5s",
        ] {
            assert!(refused(&statement(mode)), "{mode}");
        }
    }

    #[test]
    fn completion_composes_consumer_modes_without_another_entitys_clauses() {
        let prefix = "CREATE RABBITMQ QUEUE INGESTOR events FROM broker QUEUE 'orders' MODE ";
        assert_offers_exactly(prefix, &["NO ACK", "ACK SEQUENTIAL", "ACK PARALLEL"]);
        assert_offers_exactly(&format!("{prefix}ACK "), &["SEQUENTIAL", "PARALLEL"]);
        assert_offers_exactly(&format!("{prefix}ACK PARALLEL "), &["MAX"]);
        assert_offers_exactly(&format!("{prefix}NO ACK "), &["ON QUIESCE"]);
    }
}
