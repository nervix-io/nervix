//! SQS QUEUE emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and typed publication grammar of SQS QUEUE emitters.
//! - **Depends on.** Shared emitter clauses and this connector's declared entity Models.
//! - **Must not know.** Connector drivers, execution planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn sqs_fifo_group_expression<'src>()
-> impl Parser<'src, &'src [Token], nervix_models::Expression, extra::Err<ParseError<'src>>> + Clone
{
    embedded_expression("fifo_group_expression")
}
pub(super) fn sqs_fifo_group_clause<'src>()
-> impl Parser<'src, &'src [Token], SqsFifoGroup, extra::Err<ParseError<'src>>> + Clone {
    // The expression is tried first: an embedded rejection raised at the token where `FROM BRANCH`
    // had already failed would take that failure's span.
    kw_phrase2(Identifier::Fifo, Identifier::Group)
        .ignore_then(choice((
            sqs_fifo_group_expression().map(SqsFifoGroup::Expression),
            kw_phrase2(Identifier::From, Identifier::Branch).to(SqsFifoGroup::FromBranch),
        )))
        .boxed()
}
pub(super) fn mode<'src>()
-> impl Parser<'src, &'src [Token], SqsQueuePublishingMode, extra::Err<ParseError<'src>>> + Clone {
    choice((
        kw(Identifier::Single)
            .ignore_then(retry_policy())
            .map(|retry_policy| SqsQueuePublishingMode::Single { retry_policy }),
        kw(Identifier::Batch)
            .ignore_then(retry_policy())
            .map(|retry_policy| SqsQueuePublishingMode::Batch { retry_policy }),
    ))
    .boxed()
}
pub(super) fn queue_body<'src>(
    context: EmitterContext,
) -> impl Parser<'src, &'src [Token], ParsedDestination<SqsQueueEmitter>, extra::Err<ParseError<'src>>>
+ Clone {
    encoded_destination(
        context,
        client_ref()
            .then_ignore(kw(Identifier::Queue))
            .then(external_name::<SqsQueueName>())
            .then(sqs_fifo_group_clause().or_not())
            .then_ignore(kw(Identifier::Mode))
            .then(mode())
            .map(|(((client, queue), fifo_group), mode)| SqsQueueEmitter {
                client,
                queue,
                fifo_group,
                mode,
            }),
    )
}
