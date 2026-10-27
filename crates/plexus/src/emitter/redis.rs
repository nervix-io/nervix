//! REDIS emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and typed publication grammar of REDIS emitters.
//! - **Depends on.** Shared emitter clauses and this connector's declared entity Models.
//! - **Must not know.** Connector drivers, execution planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn mode<'src>()
-> impl Parser<'src, &'src [Token], RedisPubSubPublishingMode, extra::Err<ParseError<'src>>> + Clone
{
    unconfirmed_mode!(RedisPubSubPublishingMode)
}

pub(super) fn pubsub_body<'src>(
    context: EmitterContext,
) -> impl Parser<
    'src,
    &'src [Token],
    ParsedDestination<RedisPubSubEmitter>,
    extra::Err<ParseError<'src>>,
> + Clone {
    encoded_destination(
        context,
        client_ref()
            .then_ignore(kw(Identifier::Channel))
            .then(external_name::<RedisChannelName>())
            .then_ignore(kw(Identifier::Mode))
            .then(mode())
            .map(|((client, channel), mode)| RedisPubSubEmitter {
                client,
                channel,
                mode,
            }),
    )
}

#[cfg(test)]
mod tests {
    use nervix_models::{EmitterEntity, Model, RedisEmitter};
    use rstest::rstest;

    use crate::conformance_support::{assert_offers_exactly, created_model, refused};

    #[rstest]
    #[case("Orders:EU/*")]
    #[case("通道:Événements")]
    fn channel_literals_reach_the_emitter_model_unchanged(#[case] channel: &str) {
        let statement = format!(
            "CREATE REDIS PUBSUB EMITTER publishing FROM events TO redis_main CHANNEL '{channel}' \
             MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING event_codec INHERIT ALL \
             FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        );
        let Some(Model::Emitter(emitter)) = created_model(&statement) else {
            panic!("a literal channel is accepted by the composed grammar: {statement}");
        };
        let EmitterEntity::Redis(RedisEmitter::PubSub(destination)) = *emitter.connector else {
            panic!("the REDIS PUBSUB head constructs the Redis channel Model");
        };
        assert_eq!(destination.channel.as_str(), channel);
    }

    #[test]
    fn channel_completion_and_modes_follow_the_pubsub_declaration() {
        assert_offers_exactly(
            "CREATE REDIS PUBSUB EMITTER publishing FROM events TO redis_main CHANNEL ",
            &["redis_channel"],
        );
        assert_offers_exactly(
            "CREATE REDIS PUBSUB EMITTER publishing FROM events TO redis_main CHANNEL \
             'Orders:EU/*' MODE ",
            &["NO ACK"],
        );
    }

    #[test]
    fn the_composed_grammar_applies_the_channel_byte_bound() {
        for channel in [String::new(), "é".repeat(513)] {
            let statement = format!(
                "CREATE REDIS PUBSUB EMITTER publishing FROM events TO redis_main CHANNEL \
                 '{channel}' MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING \
                 event_codec INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR \
                 LOG;"
            );
            assert!(refused(&statement), "channel is outside its declared bound");
        }
    }
}
