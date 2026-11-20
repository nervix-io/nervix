//! The clause that limits how long an operation an operator requests may wait.
//!
//! Layer: language.
//!
//! - **Owns.** Parsing `ENFORCE AFTER <duration>` and `CANCEL AFTER <duration>`, the one clause
//!   every statement that can wait shares, and the words completion offers for it.
//! - **Depends on.** Shared Plexus tokens, the shared duration literal and the duration reader.
//! - **Must not know.** What an operation waits for, how it is enforced or called off, or any
//!   clock.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use std::time::Duration;

use chumsky::prelude::*;
use nervix_models::WaitLimit;

use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, duration_lit, kw_phrase2},
};

/// `ENFORCE AFTER <duration>` or `CANCEL AFTER <duration>`.
pub(crate) fn wait_limit_clause<'src>()
-> impl Parser<'src, &'src [Token], WaitLimit, extra::Err<ParseError<'src>>> + Clone {
    let enforce = kw_phrase2(Identifier::Enforce, Identifier::After)
        .ignore_then(wait_limit_duration())
        .map(|after| WaitLimit::Enforce { after });
    let cancel = kw_phrase2(Identifier::Cancel, Identifier::After)
        .ignore_then(wait_limit_duration())
        .map(|after| WaitLimit::Cancel { after });
    choice((enforce, cancel))
}

/// The duration of a wait limit. A zero duration is valid: it enforces or cancels at once.
fn wait_limit_duration<'src>()
-> impl Parser<'src, &'src [Token], Duration, extra::Err<ParseError<'src>>> + Clone {
    duration_lit().try_map(|raw, span| match nervix_models::parse_duration_text(&raw) {
        Ok(duration) => Ok(duration),
        Err(report) => Err(Rich::custom(
            span,
            format!("invalid wait limit duration: {}", report.current_context()),
        )),
    })
}

/// The clauses completion offers where a statement can still take a wait limit, in sorted order.
pub(crate) const WAIT_LIMIT_CLAUSES: [&str; 2] = ["CANCEL AFTER", "ENFORCE AFTER"];
