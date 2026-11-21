//! `SHOW NODES` syntax.
//!
//! Layer: language.
//! - **Owns.** The statement head and its ordered selection and format clauses.
//! - **Depends on.** Shared grammar primitives and the semantic observation Model.
//! - **Must not know.** How a selector runs or where node facts come from.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "parses node inspection")
)]

use chumsky::prelude::*;
use nervix_models::ShowNodes;

use crate::{
    lexer::{Identifier, Token, Word},
    parser_support::{ParseError, inspection_format, kw, kw_phrase, tok, where_expression},
};

const HEAD: [Identifier; 2] = [Identifier::Show, Identifier::Nodes];

pub(super) fn has_head(tokens: &[Token]) -> bool {
    matches!(
        tokens,
        [Token::Word(Word::KnownWord { iden: first, .. }),
         Token::Word(Word::KnownWord { iden: second, .. }), ..] if [*first, *second] == HEAD
    )
}

pub(super) fn show_nodes_parser<'src>()
-> impl Parser<'src, &'src [Token], ShowNodes, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase(HEAD)
        .ignore_then(where_expression().or_not())
        .then(
            kw(Identifier::Format)
                .ignore_then(inspection_format())
                .or_not(),
        )
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|(selector, format)| ShowNodes {
            selector,
            format: format.unwrap_or_default(),
        })
}
