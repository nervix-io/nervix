//! The clauses ingestor source bodies share: the delivery modes, the consumer count, the batch
//! timeout of a parallel acknowledgement window, the start position, and every `ON QUIESCE` body.
//!
//! Layer: language.
//!
//! - **Owns.** The one spelling of each delivery mode an ingestor writes — `NO ACK`,
//!   `ACK SEQUENTIAL …` and `ACK PARALLEL MAX n …` — and the grammar of `INSTANCES`,
//!   `BATCH TIMEOUT`, `START AT`, the `RATE LIMIT` clauses that follow a delivery mode and the
//!   quiesce bodies, which each connector's grammar composes into the subset its entity accepts.
//! - **Depends on.** Shared Plexus grammar primitives and the delivery and quiesce vocabulary.
//! - **Must not know.** Which connector accepts which clause; the connector modules choose.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use std::num::NonZeroU64;

use chumsky::{Boxed, prelude::*};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    DeclaredQuiesce, EndpointPauseResponse, IngestAcknowledgement, IngestDeliveryKind,
    IngestQuiesceKind, IngestQuiesceMode, IngestQuiesceOverflow, IngestStartPosition,
    IngestorEntityModel, IngestorInstances, RateLimit, RateLimits, RateQuantity, RetryPolicy,
};
use strum::IntoEnumIterator as _;

use crate::{
    lexer::{Identifier, Token, Word},
    parser_support::{
        ParseError, ack_timeout, byte_size_lit, duration_lit, embedded_expression,
        ingestor_retry_policy, kw, kw_phrase2, kw_phrase3, nonzero_u64_value, parallel_ack_window,
        sequential_ack_window, unknown_word,
    },
};

/// `NO ACK`: no message waits on an acknowledgement. One composed phrase, so completion offers it
/// as one item.
pub(super) fn no_ack<'src>()
-> impl Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone {
    kw_phrase2(Identifier::No, Identifier::Ack)
}

/// What `ACK SEQUENTIAL ACK TIMEOUT <d> RETRY POLICY …` states: one message at a time waits on its
/// acknowledgement.
#[derive(Debug, Clone)]
pub(super) struct SequentialAck {
    pub(super) timeout: String,
    pub(super) retry_policy: RetryPolicy,
}

pub(super) fn ack_sequential<'src>()
-> impl Parser<'src, &'src [Token], SequentialAck, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Ack)
        .ignore_then(sequential_ack_window())
        .ignore_then(ack_timeout())
        .then(ingestor_retry_policy())
        .map(|(timeout, retry_policy)| SequentialAck {
            timeout,
            retry_policy,
        })
}

/// What `ACK PARALLEL MAX <n> BATCH TIMEOUT <d> ACK TIMEOUT <d> RETRY POLICY …` states: up to `max`
/// messages wait on their acknowledgements together.
#[derive(Debug, Clone)]
pub(super) struct ParallelAck<W = NonZeroU64> {
    pub(super) max: W,
    pub(super) batch_timeout: String,
    pub(super) timeout: String,
    pub(super) retry_policy: RetryPolicy,
}

pub(super) fn ack_parallel<'src>()
-> impl Parser<'src, &'src [Token], ParallelAck, extra::Err<ParseError<'src>>> + Clone {
    ack_parallel_within(parallel_ack_window())
}

/// `ACK PARALLEL …` whose `PARALLEL MAX <n>` window `window` reads, for an entity that bounds its
/// window itself.
pub(super) fn ack_parallel_within<'src, W: 'src>(
    window: impl Parser<'src, &'src [Token], W, extra::Err<ParseError<'src>>> + Clone + 'src,
) -> impl Parser<'src, &'src [Token], ParallelAck<W>, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Ack)
        .ignore_then(window)
        .then(batch_timeout_parser())
        .then(ack_timeout())
        .then(ingestor_retry_policy())
        .map(
            |(((max, batch_timeout), timeout), retry_policy)| ParallelAck {
                max,
                batch_timeout,
                timeout,
                retry_policy,
            },
        )
}

/// The instancing body the entity declares after `INSTANCES`.
pub(super) fn instances<'src, S: IngestorEntityModel>()
-> impl Parser<'src, &'src [Token], IngestorInstances, extra::Err<ParseError<'src>>> + Clone {
    let count = nonzero_u64_value("instance_count", "instances must be greater than 0")
        .map(IngestorInstances::Count);
    let capability = S::KIND.capabilities().instancing;
    let grammar = match capability {
        nervix_models::IngestorInstancingCapability::PerTransportPartition => choice((
            kw_phrase3(
                Identifier::Per,
                Identifier::Transport,
                Identifier::Partition,
            )
            .to(IngestorInstances::PerTransportPartition),
            count,
        ))
        .boxed(),
        nervix_models::IngestorInstancingCapability::Instances => count.boxed(),
        nervix_models::IngestorInstancingCapability::Single => empty()
            .try_map(|_, span| {
                Err(Rich::custom(
                    span,
                    "this entity has no instance declaration",
                ))
            })
            .boxed(),
    };
    grammar
        .try_map(move |instances, span| {
            if capability.permits(instances) {
                Ok(instances)
            } else {
                Err(Rich::custom(
                    span,
                    format!("{} does not support this INSTANCES form", S::KIND),
                ))
            }
        })
        .boxed()
}

/// `INSTANCES <n>` where a source may leave it out, and one instance when it does.
pub(super) fn optional_instances<'src, S: IngestorEntityModel>()
-> impl Parser<'src, &'src [Token], IngestorInstances, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Instances)
        .ignore_then(instances::<S>())
        .or_not()
        .map(|instances| instances.unwrap_or(IngestorInstances::ONE))
}

/// `START AT EARLIEST|LATEST`: where a source starts when its broker holds no position it can use.
pub(super) fn start_position<'src>()
-> impl Parser<'src, &'src [Token], IngestStartPosition, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase2(Identifier::Start, Identifier::At).ignore_then(choice((
        kw(Identifier::Earliest).to(IngestStartPosition::Earliest),
        kw(Identifier::Latest).to(IngestStartPosition::Latest),
    )))
}

pub(super) fn batch_timeout_parser<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Batch)
        .ignore_then(kw(Identifier::Timeout))
        .ignore_then(duration_lit())
}

fn positive_quiesce_buffer_size<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    byte_size_lit().try_map(|value, span| {
        let bytes = value
            .parse::<ubyte::ByteUnit>()
            .verified("byte_size_lit only yields text the ByteUnit parser accepts")
            .as_u64();
        if bytes == 0 {
            Err(Rich::custom(
                span,
                "quiesce BUFFER MAX SIZE must be greater than 0",
            ))
        } else {
            Ok(value)
        }
    })
}

pub(super) fn quiesce_buffer_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestQuiesceMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Buffer)
        .ignore_then(kw_phrase2(Identifier::Max, Identifier::Size))
        .ignore_then(positive_quiesce_buffer_size())
        .then_ignore(kw_phrase2(Identifier::On, Identifier::Overflow))
        .then(choice((
            kw_phrase2(Identifier::Drop, Identifier::Oldest).to(IngestQuiesceOverflow::DropOldest),
            kw_phrase2(Identifier::Drop, Identifier::Newest).to(IngestQuiesceOverflow::DropNewest),
        )))
        .map(|(max_size, overflow)| IngestQuiesceMode::Buffer { max_size, overflow })
}

fn endpoint_quiesce_buffer_parser<'src>(
    response: EndpointPauseResponse,
) -> impl Parser<'src, &'src [Token], IngestQuiesceMode, extra::Err<ParseError<'src>>> + Clone {
    let response_phrase = match response {
        EndpointPauseResponse::Immediate => {
            kw_phrase2(Identifier::Respond, Identifier::Immediate).boxed()
        }
        EndpointPauseResponse::AfterResume => {
            kw_phrase3(Identifier::Respond, Identifier::After, Identifier::Resume).boxed()
        }
    };
    kw(Identifier::Buffer)
        .ignore_then(kw_phrase2(Identifier::Max, Identifier::Size))
        .ignore_then(positive_quiesce_buffer_size())
        .then_ignore(response_phrase)
        .map(move |max_size| IngestQuiesceMode::EndpointBuffer { max_size, response })
}

pub(super) fn quiesce_suspend_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestQuiesceMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Suspend).to(IngestQuiesceMode::Suspend)
}

pub(super) fn quiesce_drop_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestQuiesceMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Drop).to(IngestQuiesceMode::Drop)
}

fn quiesce_reject_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestQuiesceMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Reject)
        .ignore_then(kw_phrase2(Identifier::Retry, Identifier::After))
        .ignore_then(duration_lit())
        .map(|retry_after| IngestQuiesceMode::Reject { retry_after })
}

fn quiesce_clause<'src, P>(
    body: P,
) -> impl Parser<'src, &'src [Token], IngestQuiesceMode, extra::Err<ParseError<'src>>> + Clone
where
    P: Parser<'src, &'src [Token], IngestQuiesceMode, extra::Err<ParseError<'src>>> + Clone,
{
    kw_phrase2(Identifier::On, Identifier::Quiesce).ignore_then(body)
}

pub(super) fn suspend_only_quiesce_clause<'src>()
-> impl Parser<'src, &'src [Token], IngestQuiesceMode, extra::Err<ParseError<'src>>> + Clone {
    quiesce_clause(quiesce_suspend_parser())
}

pub(super) fn push_quiesce_clause<'src>()
-> impl Parser<'src, &'src [Token], IngestQuiesceMode, extra::Err<ParseError<'src>>> + Clone {
    quiesce_clause(choice((quiesce_buffer_parser(), quiesce_drop_parser())))
}

pub(super) fn flexible_quiesce_clause<'src>()
-> impl Parser<'src, &'src [Token], IngestQuiesceMode, extra::Err<ParseError<'src>>> + Clone {
    quiesce_clause(choice((
        quiesce_suspend_parser(),
        quiesce_buffer_parser(),
        quiesce_drop_parser(),
    )))
}

pub(super) fn polling_quiesce_clause<'src>()
-> impl Parser<'src, &'src [Token], IngestQuiesceMode, extra::Err<ParseError<'src>>> + Clone {
    quiesce_clause(choice((quiesce_suspend_parser(), quiesce_buffer_parser())))
}

pub(super) fn endpoint_quiesce_clause<'src>(
    declared: &'static [DeclaredQuiesce],
    delivery: IngestDeliveryKind,
) -> impl Parser<'src, &'src [Token], IngestQuiesceMode, extra::Err<ParseError<'src>>> + Clone {
    let declared = declared
        .iter()
        .filter(|clause| clause.allows_delivery(delivery));
    quiesce_clause(declared_quiesce_body(declared))
}

/// The quiesce body `SET QUIESCE` writes for an entity that declares `declared`: each mode the
/// entity declares and no other. Whether the rest of the body keeps a mode's requirements is
/// checked on the complete ingestor, once every operation applied.
pub(super) fn declared_quiesce_body<'src>(
    declared: impl IntoIterator<Item = &'static DeclaredQuiesce>,
) -> Boxed<'src, 'src, &'src [Token], IngestQuiesceMode, extra::Err<ParseError<'src>>> {
    let mut bodies = Vec::new();
    for quiesce in declared {
        let body = match quiesce.kind {
            IngestQuiesceKind::Suspend => quiesce_suspend_parser().boxed(),
            IngestQuiesceKind::Buffer => quiesce_buffer_parser().boxed(),
            IngestQuiesceKind::Drop => quiesce_drop_parser().boxed(),
            IngestQuiesceKind::Reject => quiesce_reject_parser().boxed(),
            IngestQuiesceKind::EndpointBufferImmediate => {
                endpoint_quiesce_buffer_parser(EndpointPauseResponse::Immediate).boxed()
            }
            IngestQuiesceKind::EndpointBufferAfterResume => {
                endpoint_quiesce_buffer_parser(EndpointPauseResponse::AfterResume).boxed()
            }
        };
        bodies.push(body);
    }
    choice(bodies).boxed()
}

/// A count a `RATE LIMIT` clause writes for its rate or its burst: a whole number such as `100`,
/// or a byte size such as `16MiB`. Its unit is never a keyword, so the `PER` or `BURST` after a bare
/// number is not read as one.
fn rate_quantity<'src>(
    label: &'static str,
) -> impl Parser<'src, &'src [Token], RateQuantity, extra::Err<ParseError<'src>>> + Clone {
    select! { Token::NumberLiteral(number) => number }
        .labelled(label)
        .then(unknown_word().or_not())
        .try_map(|(number, unit), span| {
            let text = match unit {
                Some(unit) => format!("{number}{unit}"),
                None => number,
            };
            RateQuantity::parse(&text)
                .map_err(|refusal| Rich::custom(span, refusal.current_context().to_string()))
        })
        .boxed()
}

/// `RATE LIMIT <count> [OF <measure>] PER <period> BURST <burst>`: one allowance a payload pays.
fn rate_limit_clause<'src>()
-> impl Parser<'src, &'src [Token], RateLimit, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase2(Identifier::Rate, Identifier::Limit)
        .ignore_then(rate_quantity("rate_limit_count"))
        .then(
            kw(Identifier::Of)
                .ignore_then(embedded_expression("rate_limit_measure"))
                .or_not(),
        )
        .then_ignore(kw(Identifier::Per))
        .then(duration_lit())
        .then_ignore(kw(Identifier::Burst))
        .then(rate_quantity("rate_limit_burst"))
        .map(|(((count, measure), period), burst)| RateLimit {
            count,
            measure,
            period,
            burst,
        })
        .boxed()
}

/// Every `RATE LIMIT` clause `SET RATE LIMIT` writes: at least one, in order.
pub(super) fn rate_limit_sequence<'src>()
-> impl Parser<'src, &'src [Token], RateLimits, extra::Err<ParseError<'src>>> + Clone {
    rate_limit_clause()
        .repeated()
        .at_least(1)
        .collect::<Vec<_>>()
        .map(RateLimits::from)
        .boxed()
}

/// A `RATE LIMIT` written where its entity cannot hold its source back in the delivery mode the
/// statement names. Its first keyword is read without a label, so a parse that reaches it refuses
/// the clause by name, and completion, which offers only labelled expectations, never offers it.
pub(super) fn refused_rate_limit<'src, R>(
    refusal: R,
) -> impl Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone
where
    R: std::fmt::Display + Clone + 'src,
{
    select! { Token::Word(Word::KnownWord { iden: Identifier::Rate, .. }) => () }
        .try_map(move |(), span| Err(Rich::custom(span, refusal.to_string())))
}

/// What may follow `MODE <mode>` in the body of an entity `S` whose statement names the delivery
/// mode `delivery`: any number of `RATE LIMIT` clauses where `S` declares the mode holds its source
/// back, and otherwise none, refusing one written by name.
pub(super) fn rate_limits_in<'src, S: IngestorEntityModel>(
    delivery: IngestDeliveryKind,
) -> Boxed<'src, 'src, &'src [Token], RateLimits, extra::Err<ParseError<'src>>> {
    match S::KIND.capabilities().rate_limit.refusal(S::KIND, delivery) {
        None => rate_limit_clause()
            .repeated()
            .collect::<Vec<_>>()
            .map(RateLimits::from)
            .boxed(),
        Some(refusal) => refused_rate_limit(refusal)
            .or_not()
            .map(|_| RateLimits::NONE)
            .boxed(),
    }
}

/// The delivery mode a body names, which decides what may follow it.
pub(super) trait NamedDelivery: Clone {
    fn delivery_kind(&self) -> IngestDeliveryKind;
}

impl NamedDelivery for IngestAcknowledgement<'_> {
    fn delivery_kind(&self) -> IngestDeliveryKind {
        self.kind()
    }
}

/// The parse extras of the clauses after a delivery mode `M` that a body has just read.
pub(super) type AfterDelivery<'src, M> = extra::Full<ParseError<'src>, (), M>;

/// What may follow the delivery mode the context holds in the body of an entity `S`: what
/// [`rate_limits_in`] reads after that mode. Each declared mode's parser is built once, and the
/// mode read selects among them.
pub(super) fn rate_limits_after<'src, S, M>()
-> impl Parser<'src, &'src [Token], RateLimits, AfterDelivery<'src, M>> + Clone
where
    S: IngestorEntityModel,
    M: NamedDelivery + 'src,
{
    let by_delivery: Vec<_> = IngestDeliveryKind::iter()
        .map(|delivery| (delivery, rate_limits_in::<S>(delivery)))
        .collect();
    custom::<_, _, _, AfterDelivery<'src, M>>(move |input| {
        let delivery = input.ctx().delivery_kind();
        // Four delivery kinds exist, one parser for each, so the scan is bounded by them.
        let (_, parser) = by_delivery
            .iter()
            .find(|(kind, _)| *kind == delivery)
            .assured("a parser was built above for every delivery kind there is");
        input.parse(map_ctx::<
            _,
            _,
            _,
            AfterDelivery<'src, M>,
            extra::Err<ParseError<'src>>,
            _,
        >(|_| (), parser.clone()))
    })
}

#[cfg(test)]
#[path = "rate_limit_tests.rs"]
mod rate_limit_tests;
