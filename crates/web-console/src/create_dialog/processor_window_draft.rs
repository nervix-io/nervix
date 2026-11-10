//! The cadence and state allowance of a visual window processor until they become its Model.
//!
//! Layer: edges.
//!
//! - **Owns.** The browser text of a window's `WIDTH` and `STEP` bounds and of its optional
//!   `MAX STATE SIZE`, the aggregate its input-field picker calls, and their conversion to the
//!   current window Models.
//! - **Depends on.** Typed vocabulary and the byte-size reading the language applies to
//!   `MAX STATE SIZE`.
//! - **Must not know.** Window panes, deadlines or state accounting, and registry validation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::num::NonZeroU64;

use error_stack::Report;
use nervix_models::{BuiltinFunctionName, WindowBound, WindowStateLimit};
use strum::Display;

use super::processor_draft::ProcessorDraftError;

/// The window bound a refusal names, spelled as Plexus writes its clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
pub(super) enum WindowBoundClause {
    #[strum(serialize = "WIDTH")]
    Width,
    #[strum(serialize = "STEP")]
    Step,
}

/// One window bound as typed: a message count, a duration, or one of each. An empty text is a
/// kind the bound does not measure.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct WindowBoundDraft {
    pub(super) messages: String,
    pub(super) duration: String,
}

impl WindowBoundDraft {
    fn build(
        &self,
        clause: WindowBoundClause,
    ) -> error_stack::Result<WindowBound, ProcessorDraftError> {
        let messages = self.messages.trim();
        let messages = if messages.is_empty() {
            None
        } else {
            let count = messages.parse::<u64>().map_err(|cause| {
                Report::new(cause).change_context(ProcessorDraftError::WindowMessages { clause })
            })?;
            Some(count)
        };
        let duration = self.duration.trim();
        let duration = if duration.is_empty() {
            None
        } else {
            Some(duration.to_string())
        };
        let bound = WindowBound { messages, duration };
        if bound.is_empty() {
            return Err(Report::new(ProcessorDraftError::WindowBound { clause }));
        }
        Ok(bound)
    }
}

/// What only a window processor declares, kept as typed until the draft builds.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct WindowDraft {
    pub(super) width: WindowBoundDraft,
    pub(super) step: WindowBoundDraft,
    /// The `MAX STATE SIZE` text, while the window caps the state of each branch.
    pub(super) state_size: Option<String>,
    /// The aggregate the window input-field picker calls on each field it inserts.
    pub(super) aggregate: String,
}

/// The checked bounds and state allowance of a window processor.
pub(super) struct WindowParts {
    pub(super) width: WindowBound,
    pub(super) step: WindowBound,
    pub(super) state_limit: WindowStateLimit,
}

impl WindowDraft {
    /// The window's bounds and state allowance, or the first missing or unreadable one in form
    /// order. How a step relates to its width is the language's rule, which the server applies.
    pub(super) fn build(&self) -> error_stack::Result<WindowParts, ProcessorDraftError> {
        let width = self.width.build(WindowBoundClause::Width)?;
        let step = self.step.build(WindowBoundClause::Step)?;
        let state_limit = match &self.state_size {
            None => WindowStateLimit::Unbounded,
            Some(size) => WindowStateLimit::MaxBytes(state_size_bytes(size)?),
        };
        Ok(WindowParts {
            width,
            step,
            state_limit,
        })
    }

    pub(super) fn bound(&self, clause: WindowBoundClause) -> &WindowBoundDraft {
        match clause {
            WindowBoundClause::Width => &self.width,
            WindowBoundClause::Step => &self.step,
        }
    }

    pub(super) fn bound_mut(&mut self, clause: WindowBoundClause) -> &mut WindowBoundDraft {
        match clause {
            WindowBoundClause::Width => &mut self.width,
            WindowBoundClause::Step => &mut self.step,
        }
    }

    /// The function the input-field picker calls, while the aggregate text names one.
    pub(super) fn aggregate_function(&self) -> Option<BuiltinFunctionName> {
        BuiltinFunctionName::parse(self.aggregate.trim()).ok()
    }
}

/// The byte count `MAX STATE SIZE` caps a branch's state at, read as the language reads the
/// clause's byte-size literal.
fn state_size_bytes(size: &str) -> error_stack::Result<NonZeroU64, ProcessorDraftError> {
    let bytes = match size.trim().parse::<ubyte::ByteUnit>() {
        Ok(bytes) => bytes.as_u64(),
        // The byte-size parser is `no_std`: its refusal is no error type a report can hold, so
        // the report carries it as a printable attachment.
        Err(cause) => {
            return Err(Report::new(ProcessorDraftError::StateSize).attach_printable(cause));
        }
    };
    NonZeroU64::new(bytes).ok_or_else(|| Report::new(ProcessorDraftError::StateSizeZero))
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};

    use super::*;

    fn bound(messages: &str, duration: &str) -> WindowBoundDraft {
        WindowBoundDraft {
            messages: messages.into(),
            duration: duration.into(),
        }
    }

    fn refusal(draft: &WindowDraft) -> ProcessorDraftError {
        draft
            .build()
            .err()
            .assured("the window draft is incomplete")
            .current_context()
            .clone()
    }

    #[test]
    fn each_bound_counts_messages_measures_a_duration_or_does_both_as_typed() {
        let draft = WindowDraft {
            width: bound(" 100 ", " 10s "),
            step: bound("", "1s"),
            ..WindowDraft::default()
        };
        let parts = draft.build().assured("both bounds are complete");
        assert_eq!(
            parts.width,
            WindowBound {
                messages: Some(100),
                duration: Some("10s".into()),
            }
        );
        assert_eq!(parts.step, WindowBound::of_duration("1s".into()));
        assert_eq!(parts.state_limit, WindowStateLimit::Unbounded);

        let draft = WindowDraft {
            width: bound("18446744073709551615", ""),
            step: bound("1", ""),
            ..WindowDraft::default()
        };
        let parts = draft.build().assured("message bounds are complete");
        assert_eq!(parts.width, WindowBound::of_messages(u64::MAX));
        assert_eq!(parts.step, WindowBound::of_messages(1));
    }

    #[test]
    fn a_bound_that_measures_nothing_or_counts_no_number_is_refused_by_its_clause() {
        let mut draft = WindowDraft::default();
        assert_eq!(
            refusal(&draft),
            ProcessorDraftError::WindowBound {
                clause: WindowBoundClause::Width
            }
        );
        draft.width = bound("3", "");
        assert_eq!(
            refusal(&draft),
            ProcessorDraftError::WindowBound {
                clause: WindowBoundClause::Step
            }
        );
        draft.step = bound("three", "");
        assert_eq!(
            refusal(&draft),
            ProcessorDraftError::WindowMessages {
                clause: WindowBoundClause::Step
            }
        );
        draft.step = bound("-1", "");
        assert_eq!(
            refusal(&draft),
            ProcessorDraftError::WindowMessages {
                clause: WindowBoundClause::Step
            }
        );
        draft.width = bound("18446744073709551616", "");
        draft.step = bound("1", "");
        assert_eq!(
            refusal(&draft),
            ProcessorDraftError::WindowMessages {
                clause: WindowBoundClause::Width
            }
        );
    }

    #[test]
    fn the_state_size_is_read_as_the_language_reads_a_byte_size_and_must_be_positive() {
        let mut draft = WindowDraft {
            width: bound("", "1m"),
            step: bound("", "1m"),
            state_size: Some(" 1MiB ".into()),
            ..WindowDraft::default()
        };
        assert_eq!(
            draft
                .build()
                .assured("the state size is a byte size")
                .state_limit,
            WindowStateLimit::MaxBytes(NonZeroU64::new(1_048_576).assured("1 MiB is positive"))
        );
        draft.state_size = Some("18446744073709551615B".into());
        assert_eq!(
            draft
                .build()
                .assured("the largest byte count is a byte size")
                .state_limit,
            WindowStateLimit::MaxBytes(NonZeroU64::MAX)
        );
        // The language reads a number and its unit as two tokens of one literal, so a space
        // between them names the same size.
        draft.state_size = Some("1 MiB".into());
        assert_eq!(
            draft
                .build()
                .assured("a spaced byte size is a byte size")
                .state_limit,
            WindowStateLimit::MaxBytes(NonZeroU64::new(1_048_576).assured("1 MiB is positive"))
        );
        draft.state_size = Some("0B".into());
        assert_eq!(refusal(&draft), ProcessorDraftError::StateSizeZero);
        for unreadable in ["", "big", "1.5B", "1 M iB"] {
            draft.state_size = Some(unreadable.into());
            assert_eq!(
                refusal(&draft),
                ProcessorDraftError::StateSize,
                "{unreadable:?}"
            );
        }
        draft.state_size = None;
        assert_eq!(
            draft
                .build()
                .assured("an uncapped window is complete")
                .state_limit,
            WindowStateLimit::Unbounded
        );
    }

    #[test]
    fn the_input_field_picker_calls_the_aggregate_its_text_names() {
        let mut draft = WindowDraft::default();
        assert_eq!(draft.aggregate_function(), None);
        draft.aggregate = " COUNT ".into();
        assert_eq!(
            draft
                .aggregate_function()
                .assured("the text names a function")
                .as_str(),
            "count"
        );
        draft.aggregate = "count(".into();
        assert_eq!(draft.aggregate_function(), None);
    }
}
