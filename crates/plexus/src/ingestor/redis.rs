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
use nervix_models::{RedisChannelName, RedisPubSubIngestMode, RedisPubSubSource};

use super::delivery::{no_ack, push_quiesce_clause};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, external_name, kw},
};

/// `FROM <client> CHANNEL '<channel>' MODE NO ACK ON QUIESCE BUFFER …|DROP`.
pub(super) fn pubsub_body<'src>()
-> impl Parser<'src, &'src [Token], RedisPubSubSource, extra::Err<ParseError<'src>>> + Clone {
    client_ref()
        .then_ignore(kw(Identifier::Channel))
        .then(external_name::<RedisChannelName>())
        .then_ignore(kw(Identifier::Mode))
        .then(no_ack().to(RedisPubSubIngestMode::NoAck))
        .then(push_quiesce_clause())
        .map(|(((client, channel), mode), quiesce)| RedisPubSubSource {
            client,
            channel,
            mode,
            quiesce,
        })
        .boxed()
}
