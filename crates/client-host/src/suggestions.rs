//! One bounded completion page.
//!
//! Layer: edges.
//!
//! - **Owns.** The completion statuses and candidate kinds a host reads, and one page's status,
//!   candidates, source edits and continuation.
//! - **Depends on.** The Rust session client's completion outcome.
//! - **Must not know.** How grammar candidates or transaction snapshots are computed.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use nervix_client_core::{AutocompleteOutcome, SuggestionKind, SuggestionStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum CompletionStatus {
    Ready = 1,
    MissingContext = 2,
    StaleContext = 3,
    LookupFailed = 4,
}

impl From<SuggestionStatus> for CompletionStatus {
    fn from(status: SuggestionStatus) -> Self {
        match status {
            SuggestionStatus::Ready => Self::Ready,
            SuggestionStatus::MissingContext => Self::MissingContext,
            SuggestionStatus::StaleContext => Self::StaleContext,
            SuggestionStatus::LookupFailed => Self::LookupFailed,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum CompletionKind {
    Text = 1,
    LocalDirectoryLookup = 2,
}

impl From<SuggestionKind> for CompletionKind {
    fn from(kind: SuggestionKind) -> Self {
        match kind {
            SuggestionKind::Text => Self::Text,
            SuggestionKind::LocalDirectoryLookup => Self::LocalDirectoryLookup,
        }
    }
}

/// One page of completion candidates.
pub struct Suggestions {
    outcome: AutocompleteOutcome,
}

impl Suggestions {
    pub fn new(outcome: AutocompleteOutcome) -> Self {
        Self { outcome }
    }

    /// The page as the session received it.
    pub fn outcome(&self) -> &AutocompleteOutcome {
        &self.outcome
    }
}
