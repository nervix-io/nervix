//! The Redis connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of a Redis Pub/Sub ingestor source: its client, channel, delivery mode and
//!   quiesce mode.
//! - **Depends on.** The shared ingestor clauses and the Redis vocabulary.
//! - **Must not know.** The Redis driver or how a subscription is held at run time.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{IngestSource, RedisPubSubIngestMode, RedisPubSubSource, RedisSource};

use super::delivery::push_quiesce_clause;
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, channel_ref, client_ref, kw, sequential_ack_window},
};

pub(super) fn redis_pubsub_mode_parser<'src>()
-> impl Parser<'src, &'src [Token], RedisPubSubIngestMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::NoAck)
        .ignore_then(sequential_ack_window())
        .to(RedisPubSubIngestMode::NoAckSequential)
}

pub(super) fn redis_pubsub_ingest_source_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestSource, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Redis)
        .ignore_then(kw(Identifier::Pubsub))
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Channel))
        .then(channel_ref())
        .then_ignore(kw(Identifier::Mode))
        .then(redis_pubsub_mode_parser())
        .then(push_quiesce_clause())
        .map(|(((client, channel), mode), quiesce)| {
            IngestSource::Redis(RedisSource::PubSub(RedisPubSubSource {
                client,
                channel,
                mode,
                quiesce,
            }))
        })
}
