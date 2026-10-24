//! The Pulsar connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of a `PULSAR TOPIC` ingestor: its client, topic, subscription and who
//!   owns it, instances, delivery mode and quiesce mode.
//! - **Depends on.** The shared ingestor clauses, the delivery modes a Pulsar subscription shares
//!   with a Kafka consumer, and the Pulsar vocabulary.
//! - **Must not know.** The Pulsar driver or how a subscription is acknowledged, verified or
//!   removed at run time.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{
    PulsarSubscription, PulsarSubscriptionName, PulsarSubscriptionOwnership, PulsarTopicIngestor,
    PulsarTopicName,
};

use super::{
    delivery::{optional_instances, suspend_only_quiesce_clause},
    kafka,
};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, external_name, kw, kw_phrase3},
};

/// `FROM <client> TOPIC '<topic>' SUBSCRIPTION '<subscription>' OWNED BY INGESTOR|PROVISIONED
/// [INSTANCES <n>] MODE … ON QUIESCE SUSPEND`.
pub(super) fn topic_body<'src>()
-> impl Parser<'src, &'src [Token], PulsarTopicIngestor, extra::Err<ParseError<'src>>> + Clone {
    client_ref()
        .then_ignore(kw(Identifier::Topic))
        .then(external_name::<PulsarTopicName>())
        .then(subscription())
        .then(optional_instances())
        .then_ignore(kw(Identifier::Mode))
        .then(kafka::mode())
        .then(suspend_only_quiesce_clause())
        .map(
            |(((((client, topic), subscription), instances), mode), quiesce)| PulsarTopicIngestor {
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

/// `SUBSCRIPTION '<subscription>' OWNED BY INGESTOR|PROVISIONED`: the subscription's name, then
/// who owns it, which every statement declares.
fn subscription<'src>()
-> impl Parser<'src, &'src [Token], PulsarSubscription, extra::Err<ParseError<'src>>> + Clone {
    let ownership = choice((
        kw_phrase3(Identifier::Owned, Identifier::By, Identifier::Ingestor)
            .to(PulsarSubscriptionOwnership::Ingestor),
        kw(Identifier::Provisioned).to(PulsarSubscriptionOwnership::Provisioned),
    ));
    kw(Identifier::Subscription)
        .ignore_then(external_name::<PulsarSubscriptionName>())
        .then(ownership)
        .map(|(name, ownership)| PulsarSubscription { name, ownership })
        .boxed()
}
