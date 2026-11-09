//! The clauses every state inspection statement ends with.
//!
//! Layer: language.
//! - **Owns.** The ordered `FOR` branch selection, `LIMIT` with its optional `AFTER` continuation,
//!   and `FORMAT` clauses a `SHOW ... STATE` statement accepts after its head.
//! - **Depends on.** Shared Plexus tokens, the branch-selection grammar and inspection vocabulary.
//! - **Must not know.** Which entity a statement inspects, runtime state or inspection pages.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{InspectionFormat, StateBranchSelection, StateInspectionPageRequest};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, inspection_format, kw, nonzero_u64_value, string_lit, tok},
};

/// What a state inspection statement asks for after it names the inspected entity.
pub(crate) struct StateInspectionClauses {
    pub(crate) selection: Option<StateBranchSelection>,
    pub(crate) page: Option<StateInspectionPageRequest>,
    pub(crate) format: InspectionFormat,
}

pub(crate) fn state_inspection_clauses<'src>()
-> impl Parser<'src, &'src [Token], StateInspectionClauses, extra::Err<ParseError<'src>>> + Clone {
    let page = kw(Identifier::Limit)
        .ignore_then(nonzero_u64_value("row_limit", "LIMIT must be positive"))
        .then(kw(Identifier::After).ignore_then(string_lit()).or_not())
        .map(|(limit, after)| StateInspectionPageRequest { limit, after });
    crate::state_branch_selection::branch_selection()
        .or_not()
        .then(page.or_not())
        .then(
            kw(Identifier::Format)
                .ignore_then(inspection_format())
                .or_not(),
        )
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|((selection, page), format)| StateInspectionClauses {
            selection,
            page,
            format: format.unwrap_or(InspectionFormat::Text),
        })
}
