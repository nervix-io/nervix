//! The Pulsar connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of a `PULSAR TOPIC` ingestor: its client, topic, subscription, instances,
//!   delivery mode and quiesce mode.
//! - **Depends on.** The shared ingestor clauses, the delivery modes a Pulsar subscription shares
//!   with a Kafka consumer, and the Pulsar vocabulary.
//! - **Must not know.** The Pulsar driver or how a subscription is acknowledged at run time.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{PulsarSubscriptionName, PulsarTopicName, PulsarTopicSource};

use super::{
    delivery::{optional_instances, suspend_only_quiesce_clause},
    kafka,
};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, external_name, kw},
};

/// `FROM <client> TOPIC '<topic>' SUBSCRIPTION '<subscription>' [INSTANCES <n>] MODE … ON QUIESCE
/// SUSPEND`.
pub(super) fn topic_body<'src>()
-> impl Parser<'src, &'src [Token], PulsarTopicSource, extra::Err<ParseError<'src>>> + Clone {
    client_ref()
        .then_ignore(kw(Identifier::Topic))
        .then(external_name::<PulsarTopicName>())
        .then_ignore(kw(Identifier::Subscription))
        .then(external_name::<PulsarSubscriptionName>())
        .then(optional_instances())
        .then_ignore(kw(Identifier::Mode))
        .then(kafka::mode())
        .then(suspend_only_quiesce_clause())
        .map(
            |(((((client, topic), subscription), instances), mode), quiesce)| PulsarTopicSource {
                client,
                topic,
                subscription,
                instances,
                mode,
                quiesce,
            },
        )
        .boxed()
}
