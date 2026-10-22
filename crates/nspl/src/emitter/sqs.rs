//! SQS emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and connector-specific clauses of SQS emitters.
//! - **Depends on.** Shared emitter grammar, lexical primitives and the connector vocabulary.
//! - **Must not know.** Connector drivers, graph planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn sqs_publishing_mode<'src>()
-> impl Parser<'src, &'src [Token], EmitterPublishingMode, extra::Err<ParseError<'src>>> + Clone {
    choice((
        kw(Identifier::Single)
            .ignore_then(retry_policy())
            .map(|retry_policy| EmitterPublishingMode::SqsSingle { retry_policy }),
        kw(Identifier::Batch)
            .ignore_then(retry_policy())
            .map(|retry_policy| EmitterPublishingMode::SqsBatch { retry_policy }),
    ))
    .boxed()
}

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

pub(super) fn sqs_queue_name<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    let atom = choice((select! { Token::NumberLiteral(value) => value }, word_raw()));
    let first = atom.clone().labelled("queue_name");
    let continuation = atom.labelled("queue_name");
    let base = first
        .then(
            tok(Token::Hyphen)
                .ignore_then(continuation)
                .repeated()
                .collect::<Vec<_>>(),
        )
        .map(|(first, rest)| {
            let mut queue = first;
            for part in rest {
                queue.push('-');
                queue.push_str(&part);
            }
            queue
        });

    base.then(tok(Token::Dot).ignore_then(kw(Identifier::Fifo)).or_not())
        .map(|(mut queue, fifo)| {
            if fifo.is_some() {
                queue.push_str(".fifo");
            }
            queue
        })
        .boxed()
}

pub(super) fn sqs_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Sqs)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Queue))
        .then(sqs_queue_name())
        .then(sqs_fifo_group_clause().or_not())
        .map(|((client, queue), fifo_group)| {
            EmitSink::Sqs(SqsSink::Queue(SqsQueueSink {
                client,
                queue,
                fifo_group,
            }))
        })
}
