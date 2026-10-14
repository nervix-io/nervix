//! The clauses ingestor source bodies share: the consumer count, the batch timeout of a parallel
//! acknowledgement window, and every `ON QUIESCE` body.
//!
//! Layer: language.
//!
//! - **Owns.** The grammar of `INSTANCES`, `BATCH TIMEOUT` and the quiesce bodies, which each
//!   connector's grammar composes into the subset its entity accepts.
//! - **Depends on.** Shared NSPL grammar primitives and the quiesce vocabulary.
//! - **Must not know.** Which connector accepts which clause; the connector modules choose.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use std::num::NonZeroU64;

use chumsky::prelude::*;
use meticulous::ResultExt as _;
use nervix_models::{IngestQuiesceMode, IngestQuiesceOverflow};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, boxed_choice, byte_size_lit, duration_lit, kw, kw_phrase2, nonzero_u64_value,
    },
};

/// The count after `INSTANCES`.
pub(super) fn instance_count<'src>()
-> impl Parser<'src, &'src [Token], NonZeroU64, extra::Err<ParseError<'src>>> + Clone {
    nonzero_u64_value("instance_count", "instances must be greater than 0")
}

pub(super) fn batch_timeout_parser<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Batch)
        .ignore_then(kw(Identifier::Timeout))
        .ignore_then(duration_lit())
}

pub(super) fn positive_quiesce_buffer_size<'src>()
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

pub(super) fn endpoint_quiesce_buffer_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestQuiesceMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Buffer)
        .ignore_then(kw_phrase2(Identifier::Max, Identifier::Size))
        .ignore_then(positive_quiesce_buffer_size())
        .map(|max_size| IngestQuiesceMode::EndpointBuffer { max_size })
}

pub(super) fn quiesce_suspend_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestQuiesceMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Suspend).to(IngestQuiesceMode::Suspend)
}

pub(super) fn quiesce_drop_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestQuiesceMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Drop).to(IngestQuiesceMode::Drop)
}

pub(super) fn quiesce_reject_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestQuiesceMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Reject)
        .ignore_then(kw_phrase2(Identifier::Retry, Identifier::After))
        .ignore_then(duration_lit())
        .map(|retry_after| IngestQuiesceMode::Reject { retry_after })
}

pub(super) fn quiesce_clause<'src, P>(
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

pub(super) fn endpoint_quiesce_clause<'src>()
-> impl Parser<'src, &'src [Token], IngestQuiesceMode, extra::Err<ParseError<'src>>> + Clone {
    quiesce_clause(choice((
        quiesce_reject_parser(),
        endpoint_quiesce_buffer_parser(),
    )))
}

pub(super) fn alter_quiesce_body<'src>()
-> impl Parser<'src, &'src [Token], IngestQuiesceMode, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
        quiesce_suspend_parser(),
        quiesce_buffer_parser(),
        quiesce_drop_parser(),
        quiesce_reject_parser(),
        endpoint_quiesce_buffer_parser(),
    )
}
