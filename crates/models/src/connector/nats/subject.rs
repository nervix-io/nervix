//! The NATS subject contract used by the connector's typed external names.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** Parsing a NATS 2.11 subscription pattern into typed token kinds, the stricter
//!   Nervix character and 1,024-byte limits, and the literal-only publication check.
//! - **Depends on.** The NATS vocabulary's shared whitespace refusal and typed external-name errors.
//! - **Must not know.** The NATS driver, network subscriptions, Plexus grammar, or graph execution.
//!
//! nats-server 2.11's `IsValidSubject` and `Sublist.Insert` define dot-separated tokens, `*` as
//! one whole token and `>` as a terminal whole token. They also permit some characters within a
//! literal token that Nervix has always refused, including embedded wildcard characters and
//! controls. The restriction here is the complete public Nervix subject-name contract, not a
//! claim that the server imposes these additional limits. The 1,024-byte limit is Nervix's bound;
//! nats-server does not define a fixed maximum subject length.

use super::{ExternalNameError, refuse_whitespace};
use crate::connector::external_name::require_length;

const MAX_SUBJECT_BYTES: usize = 1024;

/// A validated subscription pattern. Its text remains borrowed and in the operator's spelling.
pub(super) struct SubscriptionPattern<'a> {
    source: &'a str,
}

/// One dot-separated NATS token; literal spelling is retained without allocation.
#[derive(Debug, PartialEq, Eq)]
enum SubjectToken<'a> {
    Literal(&'a str),
    One,
    Tail,
}

impl<'a> SubjectToken<'a> {
    fn parse(token: &'a str) -> Result<Self, ExternalNameError> {
        if token.is_empty() {
            return Err(ExternalNameError::EmptyLevel { separator: '.' });
        }
        if token.len() > 1
            && let Some(wildcard) = token.chars().find(|ch| matches!(ch, '*' | '>'))
        {
            return Err(ExternalNameError::WildcardInsideLevel { wildcard });
        }
        Ok(match token {
            "*" => Self::One,
            ">" => Self::Tail,
            _ => Self::Literal(token),
        })
    }
}

impl<'a> SubscriptionPattern<'a> {
    pub(super) fn parse(source: &'a str) -> Result<Self, ExternalNameError> {
        require_length(source, MAX_SUBJECT_BYTES)?;
        refuse_whitespace(source)?;

        let pattern = Self { source };
        {
            let mut tokens = pattern.tokens().peekable();
            while let Some(token) = tokens.next() {
                if matches!(token?, SubjectToken::Tail) && tokens.peek().is_some() {
                    return Err(ExternalNameError::WildcardBeforeLastLevel { wildcard: '>' });
                }
            }
        }
        Ok(pattern)
    }

    /// Publication names are a literal subset of valid subscription patterns. Check this after
    /// the entire pattern so malformed-pattern refusals keep their existing precedence.
    pub(super) fn require_literal(self) -> Result<(), ExternalNameError> {
        if let Some(ch) = self.source.chars().find(|ch| matches!(ch, '*' | '>')) {
            return Err(ExternalNameError::InvalidChar { ch });
        }
        Ok(())
    }

    fn tokens(&self) -> impl Iterator<Item = Result<SubjectToken<'_>, ExternalNameError>> + '_ {
        self.source.split('.').map(SubjectToken::parse)
    }
}
