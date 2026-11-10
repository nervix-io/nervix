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
use nervix_models::{Float64Literal, Literal, StateBranchField, StateBranchSelection};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, field_ref, kw, kw_phrase2, string_lit, tok},
};

pub(crate) fn branch_selection<'src>()
-> impl Parser<'src, &'src [Token], StateBranchSelection, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::For).ignore_then(choice((
        kw(Identifier::Unbranched).to(StateBranchSelection::Unbranched),
        kw_phrase2(Identifier::All, Identifier::Branches).to(StateBranchSelection::AllBranches),
        kw_phrase2(Identifier::Branch, Identifier::Key)
            .ignore_then(branch_fields())
            .map(StateBranchSelection::Branch),
    )))
}

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
                        format!("duplicate branch key field '{}'", field.name.as_str()),
                    ));
                }
            }
            Ok(fields)
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use crate::statement::{parse_statement, suggest_statement};

    const HEADS: [&str; 6] = [
        "RESET WASM PROCESSOR counter STATE",
        "SHOW RELAY events MATERIALIZED STATE",
        "SHOW REORDERER held STATE",
        "SHOW CORRELATOR held STATE",
        "SHOW DEDUPLICATOR held STATE",
        "SHOW WINDOW PROCESSOR held STATE",
    ];

    #[test]
    fn every_state_command_completes_the_composed_branch_key_phrase() {
        for head in HEADS {
            let input = format!("{head} FOR ");
            let mut suggestions = suggest_statement(&input, input.len());
            suggestions.sort();
            assert_eq!(
                suggestions,
                ["ALL BRANCHES", "BRANCH KEY", "UNBRANCHED"],
                "{input}"
            );

            let input = format!("{head} FOR BRANCH ");
            assert_eq!(suggest_statement(&input, input.len()), ["KEY"], "{input}");
        }
    }

    #[test]
    fn every_state_command_requires_a_complete_literal_key() {
        for head in HEADS {
            for key in [
                "{}",
                "{ tenant = 'alpha', tenant = 'beta' }",
                "{ tenant = null }",
                "{ tenant = input.tenant }",
                "{ tenant = 'alpha' } FOR ALL BRANCHES",
            ] {
                let input = format!("{head} FOR BRANCH KEY {key};");
                assert!(parse_statement(&input).is_err(), "{input}");
            }
        }
    }
}
