//! The Redis connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of a `REDIS PUBSUB` ingestor: its client, channel, delivery mode and
//!   quiesce mode.
//! - **Depends on.** The shared ingestor clauses and the Redis vocabulary.
//! - **Must not know.** The Redis driver or how a subscription is held at run time.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{RedisChannelName, RedisPubSubIngestMode, RedisPubSubIngestor};

use super::delivery::{no_ack, push_quiesce_clause};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, external_name, kw},
};

/// `FROM <client> CHANNEL '<channel>' MODE NO ACK ON QUIESCE BUFFER …|DROP`.
pub(super) fn pubsub_body<'src>()
-> impl Parser<'src, &'src [Token], RedisPubSubIngestor, extra::Err<ParseError<'src>>> + Clone {
    client_ref()
        .then_ignore(kw(Identifier::Channel))
        .then(external_name::<RedisChannelName>())
        .then_ignore(kw(Identifier::Mode))
        .then(no_ack().to(RedisPubSubIngestMode::NoAck))
        .then(push_quiesce_clause())
        .map(|(((client, channel), mode), quiesce)| RedisPubSubIngestor {
            client,
            channel,
            mode,
            quiesce,
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use nervix_models::{IngestorEntity, IngestorInput, Model, RedisIngestor};
    use rstest::rstest;

    use crate::conformance_support::{assert_offers_exactly, created_model, refused};

    #[rstest]
    #[case("Orders:EU/*")]
    #[case("通道:Événements")]
    fn channel_literals_reach_the_ingestor_model_unchanged(#[case] channel: &str) {
        let statement = format!(
            "CREATE REDIS PUBSUB INGESTOR intake FROM redis_main CHANNEL '{channel}' MODE NO ACK \
             ON QUIESCE DROP DECODE USING event_codec TO events INHERIT ALL UNBRANCHED FLUSH \
             IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        );
        let Some(Model::Ingestor(ingestor)) = created_model(&statement) else {
            panic!("a literal channel is accepted by the composed grammar: {statement}");
        };
        let IngestorInput::Transport(input) = ingestor.input else {
            panic!("the REDIS PUBSUB head names a transport");
        };
        let IngestorEntity::Redis(RedisIngestor::PubSub(source)) = input.source else {
            panic!("the REDIS PUBSUB head constructs the Redis channel Model");
        };
        assert_eq!(source.channel.as_str(), channel);
    }

    #[test]
    fn channel_completion_and_modes_follow_the_pubsub_declaration() {
        assert_offers_exactly(
            "CREATE REDIS PUBSUB INGESTOR intake FROM redis_main CHANNEL ",
            &["redis_channel"],
        );
        assert_offers_exactly(
            "CREATE REDIS PUBSUB INGESTOR intake FROM redis_main CHANNEL 'Orders:EU/*' MODE ",
            &["NO ACK"],
        );
        assert_offers_exactly(
            "CREATE REDIS PUBSUB INGESTOR intake FROM redis_main CHANNEL 'Orders:EU/*' MODE NO \
             ACK ON QUIESCE ",
            &["BUFFER", "DROP"],
        );
    }

    #[test]
    fn the_composed_grammar_applies_the_channel_byte_bound() {
        for channel in [String::new(), "é".repeat(513)] {
            let statement = format!(
                "CREATE REDIS PUBSUB INGESTOR intake FROM redis_main CHANNEL '{channel}' MODE NO \
                 ACK ON QUIESCE DROP DECODE USING event_codec TO events INHERIT ALL UNBRANCHED \
                 FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
            );
            assert!(refused(&statement), "channel is outside its declared bound");
        }
    }
}
