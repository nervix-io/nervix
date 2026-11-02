//! The Redis connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of a `REDIS PUBSUB` ingestor: its client, the channel or channel pattern
//!   it subscribes to, its delivery mode and its quiesce mode.
//! - **Depends on.** The shared ingestor clauses and the Redis vocabulary.
//! - **Must not know.** The Redis driver or how a subscription is held at run time.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{
    RedisChannelName, RedisChannelPattern, RedisPubSubIngestMode, RedisPubSubIngestor,
    RedisSubscriptionTarget,
};

use super::delivery::{no_ack, push_quiesce_clause};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, external_name, kw},
};

/// `FROM <client> CHANNEL '<channel>' | PATTERN '<pattern>' MODE NO ACK ON QUIESCE BUFFER …|DROP`.
pub(super) fn pubsub_body<'src>()
-> impl Parser<'src, &'src [Token], RedisPubSubIngestor, extra::Err<ParseError<'src>>> + Clone {
    client_ref()
        .then(subscription())
        .then_ignore(kw(Identifier::Mode))
        .then(no_ack().to(RedisPubSubIngestMode::NoAck))
        .then(push_quiesce_clause())
        .map(
            |(((client, subscription), mode), quiesce)| RedisPubSubIngestor {
                client,
                subscription,
                mode,
                quiesce,
            },
        )
        .boxed()
}

/// `CHANNEL '<channel>'` or `PATTERN '<pattern>'`: the one locator a body subscribes by.
fn subscription<'src>()
-> impl Parser<'src, &'src [Token], RedisSubscriptionTarget, extra::Err<ParseError<'src>>> + Clone {
    let channel = kw(Identifier::Channel)
        .ignore_then(external_name::<RedisChannelName>())
        .map(RedisSubscriptionTarget::Channel);
    let pattern = kw(Identifier::Pattern)
        .ignore_then(external_name::<RedisChannelPattern>())
        .map(RedisSubscriptionTarget::Pattern);
    choice((channel, pattern)).boxed()
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        IngestorEntity, IngestorInput, Model, RedisIngestor, RedisPubSubIngestor,
        RedisSubscriptionTarget,
    };
    use rstest::rstest;

    use crate::conformance_support::{assert_offers_exactly, created_model, refused};

    /// The source a statement whose body subscribes by `locator` constructs.
    fn created_source(locator: &str) -> RedisPubSubIngestor {
        let statement = format!(
            "CREATE REDIS PUBSUB INGESTOR intake FROM redis_main {locator} MODE NO ACK ON QUIESCE \
             DROP DECODE USING event_codec TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON \
             MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        );
        let Some(Model::Ingestor(ingestor)) = created_model(&statement) else {
            panic!("the composed grammar accepts the locator: {statement}");
        };
        let IngestorInput::Transport(input) = ingestor.input else {
            panic!("the REDIS PUBSUB head names a transport");
        };
        let IngestorEntity::Redis(RedisIngestor::PubSub(source)) = input.source else {
            panic!("the REDIS PUBSUB head constructs the Redis Pub/Sub Model");
        };
        source
    }

    #[rstest]
    #[case("Orders:EU/*")]
    #[case("通道:Événements")]
    fn channel_literals_reach_the_ingestor_model_unchanged(#[case] channel: &str) {
        let source = created_source(&format!("CHANNEL '{channel}'"));
        let RedisSubscriptionTarget::Channel(subscribed) = source.subscription else {
            panic!("CHANNEL subscribes to one channel");
        };
        assert_eq!(subscribed.as_str(), channel);
    }

    #[rstest]
    #[case("orders.*")]
    #[case("Orders:[EU]?/[^x]*")]
    #[case("literal\\*star")]
    #[case("通道:*")]
    fn pattern_literals_reach_the_ingestor_model_unchanged(#[case] pattern: &str) {
        let source = created_source(&format!("PATTERN '{pattern}'"));
        let RedisSubscriptionTarget::Pattern(subscribed) = source.subscription else {
            panic!("PATTERN subscribes to the channels a pattern matches");
        };
        assert_eq!(subscribed.as_str(), pattern);
    }

    #[test]
    fn completion_offers_both_locators_then_the_pubsub_declaration() {
        assert_offers_exactly(
            "CREATE REDIS PUBSUB INGESTOR intake FROM redis_main ",
            &["CHANNEL", "PATTERN"],
        );
        assert_offers_exactly(
            "CREATE REDIS PUBSUB INGESTOR intake FROM redis_main CHANNEL ",
            &["redis_channel"],
        );
        assert_offers_exactly(
            "CREATE REDIS PUBSUB INGESTOR intake FROM redis_main PATTERN ",
            &["redis_channel_pattern"],
        );
        for locator in ["CHANNEL 'Orders:EU/*'", "PATTERN 'orders.*'"] {
            assert_offers_exactly(
                &format!("CREATE REDIS PUBSUB INGESTOR intake FROM redis_main {locator} "),
                &["MODE"],
            );
            assert_offers_exactly(
                &format!("CREATE REDIS PUBSUB INGESTOR intake FROM redis_main {locator} MODE "),
                &["NO ACK"],
            );
            assert_offers_exactly(
                &format!(
                    "CREATE REDIS PUBSUB INGESTOR intake FROM redis_main {locator} MODE NO ACK ON \
                     QUIESCE "
                ),
                &["BUFFER", "DROP"],
            );
        }
    }

    #[rstest]
    #[case("CHANNEL")]
    #[case("PATTERN")]
    fn the_composed_grammar_applies_the_byte_bound_to_either_locator(#[case] keywords: &str) {
        for name in [String::new(), "é".repeat(513)] {
            let statement = format!(
                "CREATE REDIS PUBSUB INGESTOR intake FROM redis_main {keywords} '{name}' MODE NO \
                 ACK ON QUIESCE DROP DECODE USING event_codec TO events INHERIT ALL UNBRANCHED \
                 FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
            );
            assert!(
                refused(&statement),
                "{keywords} is outside its declared bound"
            );
        }
    }

    #[rstest]
    #[case("CHANNEL 'orders' PATTERN 'orders.*'")]
    #[case("PATTERN 'orders.*' CHANNEL 'orders'")]
    #[case("")]
    fn a_body_subscribes_by_exactly_one_locator(#[case] locators: &str) {
        let statement = format!(
            "CREATE REDIS PUBSUB INGESTOR intake FROM redis_main {locators} MODE NO ACK ON \
             QUIESCE DROP DECODE USING event_codec TO events INHERIT ALL UNBRANCHED FLUSH \
             IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        );
        assert!(
            refused(&statement),
            "a body writes one locator: {statement}"
        );
    }
}
