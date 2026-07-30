//! The completion surface under test, and the accept/reject oracle derived from parse diagnostics.

use nervix_nspl::schema::{Diagnostic, ParseFromSourceError};

/// Which composed grammar the walk explores.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Grammar {
    /// The server statement grammar.
    Server,
    /// The client superset, which is what the server autocomplete endpoint calls.
    Client,
}

impl Grammar {
    /// Suggestions offered at a fresh word boundary after `input`.
    ///
    /// Completion prefix-filters on the word under the cursor, so asking with the cursor straight
    /// after a finished word only offers that word back. The trailing space is what asks "what can
    /// come next"; the server reaches the same position by cutting the partial word out of the
    /// input before handing it to the grammar.
    pub fn suggest(self, input: &str) -> Vec<String> {
        let probe = format!("{input} ");
        self.suggest_at(&probe, probe.len())
    }

    /// Suggestions offered once `typed` has been partially entered after `input`.
    pub fn suggest_typed(self, input: &str, typed: &str) -> Vec<String> {
        let probe = format!("{input} {typed}");
        self.suggest_at(&probe, probe.len())
    }

    fn suggest_at(self, input: &str, cursor: usize) -> Vec<String> {
        match self {
            Self::Server => nervix_nspl::statement::suggest_statement(input, cursor),
            Self::Client => nervix_nspl::client_statement::suggest_client_statement(input, cursor),
        }
    }

    /// Parse `input` and classify how far the parser got.
    pub fn parse(self, input: &str) -> ParseOutcome {
        ParseOutcome::classify(self.parse_error(input).as_ref(), input.len())
    }

    /// Every diagnostic the parser produced, not just the furthest. Only the furthest decides the
    /// verdict, but seeing the rest is what explains why a position offers what it offers.
    pub fn diagnostics(self, input: &str) -> Vec<Diagnostic> {
        match self.parse_error(input) {
            Some(ParseFromSourceError::Lex { diagnostics, .. })
            | Some(ParseFromSourceError::Parse { diagnostics, .. }) => diagnostics,
            None => Vec::new(),
        }
    }

    fn parse_error(self, input: &str) -> Option<ParseFromSourceError> {
        match self {
            Self::Server => nervix_nspl::statement::parse_statement(input).err(),
            Self::Client => nervix_nspl::client_statement::parse_client_statement(input).err(),
        }
    }
}

/// How the parser responded to a walked input.
///
/// The walk only ever extends states that were accepted or incomplete, so a rejection in a child
/// state is always caused by the text that was just appended. That makes the position of the
/// diagnostic irrelevant to the verdict: what matters is only whether the parser reached the end of
/// the input. `Rich::custom` errors from the `vm_program` splice point at the whole captured run
/// rather than at the offending token, so anything finer would misread them anyway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseOutcome {
    /// The input is a complete statement.
    Accepted,
    /// Every alternative ran out of input: what was applied was consumed and more is expected.
    Incomplete { message: String },
    /// The parser stopped before the end of the input.
    Rejected { at: usize, message: String },
    /// The input no longer lexes.
    LexFailure { at: usize, message: String },
}

impl ParseOutcome {
    fn classify(error: Option<&ParseFromSourceError>, input_len: usize) -> Self {
        let Some(error) = error else {
            return Self::Accepted;
        };

        let (diagnostics, lexical) = match error {
            ParseFromSourceError::Lex { diagnostics, .. } => (diagnostics, true),
            ParseFromSourceError::Parse { diagnostics, .. } => (diagnostics, false),
        };

        // Only the furthest-progress error matters: earlier ones are alternatives that backtracked.
        // This mirrors how `suggestions_from_errors` picks the expectation set it reports.
        let Some(furthest) = diagnostics
            .iter()
            .max_by_key(|diagnostic| (diagnostic.span.start, diagnostic.span.end))
        else {
            return Self::Rejected {
                at: input_len,
                message: "parser failed without reporting a diagnostic".to_string(),
            };
        };

        if lexical {
            return Self::LexFailure {
                at: furthest.span.start,
                message: furthest.message.clone(),
            };
        }

        if furthest.span.start >= input_len {
            // The message names what the parser was still hoping for, which is exactly what has to
            // be missing from the suggestions when a state turns out to be a dead end.
            Self::Incomplete {
                message: furthest.message.clone(),
            }
        } else {
            Self::Rejected {
                at: furthest.span.start,
                message: furthest.message.clone(),
            }
        }
    }

    /// The diagnostic text, if the outcome carries one.
    pub fn message(&self) -> Option<&str> {
        match self {
            Self::Accepted => None,
            Self::Incomplete { message }
            | Self::Rejected { message, .. }
            | Self::LexFailure { message, .. } => Some(message),
        }
    }
}
