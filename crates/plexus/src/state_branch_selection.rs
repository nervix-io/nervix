//! Shared literal-field grammar for state branch selectors.
//!
//! Layer: language.
//! - **Owns.** Literal branch fields and duplicate-field refusal for state commands.
//! - **Depends on.** Shared Plexus tokens and branch-selection vocabulary.
//! - **Must not know.** Runtime branches, reset effects or inspection pages.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the boundary declared above")
)]

use std::collections::BTreeSet;

use chumsky::prelude::*;
use nervix_models::{Float64Literal, Literal, StateBranchField};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, field_ref, kw, string_lit, tok},
};

fn branch_literal<'src>()
-> impl Parser<'src, &'src [Token], Literal, extra::Err<ParseError<'src>>> + Clone {
    let number = tok(Token::Hyphen)
        .or_not()
        .then(select! { Token::NumberLiteral(raw) => raw })
        .labelled("number_literal")
        .try_map(|(negative, raw), span| {
            let signed = if negative.is_some() {
                format!("-{raw}")
            } else {
                raw
            };
            if signed.contains(['.', 'e', 'E']) {
                let value = signed
                    .parse::<f64>()
                    .map_err(|error| Rich::custom(span, error.to_string()))?;
                Ok(Literal::F64(Float64Literal::new(value)))
            } else {
                let value = signed
                    .parse::<i64>()
                    .map_err(|error| Rich::custom(span, error.to_string()))?;
                Ok(Literal::I64(value))
            }
        });
    choice((
        string_lit().map(Literal::String),
        number,
        kw(Identifier::True).to(Literal::Bool(true)),
        kw(Identifier::False).to(Literal::Bool(false)),
    ))
    .boxed()
}

pub(crate) fn branch_fields<'src>()
-> impl Parser<'src, &'src [Token], Vec<StateBranchField>, extra::Err<ParseError<'src>>> + Clone {
    let field = field_ref()
        .then_ignore(tok(Token::Eq))
        .then(branch_literal())
        .map(|(name, value)| StateBranchField { name, value });
    tok(Token::LBrace)
        .ignore_then(
            field
                .separated_by(tok(Token::Comma))
                .at_least(1)
                .collect::<Vec<_>>(),
        )
        .then_ignore(tok(Token::RBrace))
        .try_map(|fields, span| {
            let mut names = BTreeSet::new();
            for field in &fields {
                if !names.insert(&field.name) {
                    return Err(Rich::custom(
                        span,
                        format!("duplicate branch field '{}'", field.name.as_str()),
                    ));
                }
            }
            Ok(fields)
        })
        .boxed()
}
