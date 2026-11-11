//! Shared domain entity metadata grammar.
//!
//! Layer: language.
//! - **Owns.** The common trailing creation clause and typed SET, DROP and SHOW LABELS statements.
//! - **Depends on.** Shared tokens, typed entity references and metadata vocabulary constructors.
//! - **Must not know.** Registry state, committed limits, sessions or execution behavior.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns composed metadata grammar")
)]

#[cfg(test)]
mod tests;

use chumsky::prelude::*;
use nervix_models::{
    CreateStatement, EntityLabelKeys, EntityLabelMutation, EntityLabels, EntityMetadata,
    LabelAssignment, LabelKey, MutateEntityLabels, ShowLabels, Statement,
};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, boxed_choice, inspection_format, kw, kw_phrase2, model_ref, string_lit, tok,
    },
};

fn label_key<'src>()
-> impl Parser<'src, &'src [Token], LabelKey, extra::Err<ParseError<'src>>> + Clone {
    string_lit()
        .try_map(|value, span| {
            LabelKey::try_from(value).map_err(|error| Rich::custom(span, format!("{error:#}")))
        })
        .labelled("label_key")
        .boxed()
}

fn labels<'src>()
-> impl Parser<'src, &'src [Token], EntityLabels, extra::Err<ParseError<'src>>> + Clone {
    label_key()
        .then_ignore(tok(Token::Eq))
        .then(string_lit())
        .map(|(key, value)| LabelAssignment { key, value })
        .separated_by(tok(Token::Comma))
        .allow_trailing()
        .collect::<Vec<_>>()
        .delimited_by(tok(Token::LBrace), tok(Token::RBrace))
        .try_map(|entries, span| {
            EntityLabels::try_from(entries)
                .map_err(|error| Rich::custom(span, format!("{error:#}")))
        })
        .boxed()
}

/// Exactly one optional metadata clause precedes the optional statement terminator.
pub(crate) fn finish_creation<'src, T, P>(
    parser: P,
) -> impl Parser<'src, &'src [Token], CreateStatement<T>, extra::Err<ParseError<'src>>> + Clone
where
    T: EntityMetadata + Clone + 'src,
    P: Parser<'src, &'src [Token], CreateStatement<T>, extra::Err<ParseError<'src>>> + Clone + 'src,
{
    parser
        .then(kw(Identifier::Labels).ignore_then(labels()).or_not())
        .map(|(mut create, labels)| {
            if let Some(labels) = labels {
                *create.body.labels_mut() = labels;
            }
            create
        })
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub(crate) fn entity_labels_parser<'src>()
-> impl Parser<'src, &'src [Token], Statement, extra::Err<ParseError<'src>>> + Clone {
    let keys = label_key()
        .separated_by(tok(Token::Comma))
        .allow_trailing()
        .collect::<Vec<_>>()
        .delimited_by(tok(Token::LBrace), tok(Token::RBrace))
        .try_map(|keys, span| {
            EntityLabelKeys::try_from(keys)
                .map_err(|error| Rich::custom(span, format!("{error:#}")))
        });
    boxed_choice!(
        kw(Identifier::Set)
            .ignore_then(kw_phrase2(Identifier::Labels, Identifier::On))
            .ignore_then(model_ref())
            .then(labels())
            .map(
                |(entity, labels)| Statement::MutateEntityLabels(MutateEntityLabels {
                    entity,
                    mutation: EntityLabelMutation::Set(labels)
                })
            ),
        kw(Identifier::Drop)
            .ignore_then(kw_phrase2(Identifier::Labels, Identifier::On))
            .ignore_then(model_ref())
            .then(keys)
            .map(
                |(entity, keys)| Statement::MutateEntityLabels(MutateEntityLabels {
                    entity,
                    mutation: EntityLabelMutation::Drop(keys)
                })
            ),
        kw(Identifier::Show)
            .ignore_then(kw_phrase2(Identifier::Labels, Identifier::On))
            .ignore_then(model_ref())
            .then(
                kw(Identifier::Format)
                    .ignore_then(inspection_format())
                    .or_not()
            )
            .map(|(entity, format)| Statement::ShowLabels(ShowLabels {
                entity,
                format: format.unwrap_or_default()
            })),
    )
    .then_ignore(tok(Token::Semicolon).or_not())
    .boxed()
}
