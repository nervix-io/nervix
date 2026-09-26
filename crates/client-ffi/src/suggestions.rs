//! One bounded completion page: `nx_suggestions`.
//!
//! Layer: edges.
//! - **Owns.** C access to completion status, candidates, source edits, and continuation.
//! - **Depends on.** The Rust session client and the C ABI's handle and failure conventions.
//! - **Must not know.** How grammar candidates or transaction snapshots are computed.

use nervix_client_core::{AutocompleteOutcome, SuggestionKind, SuggestionStatus};

use crate::{
    abi,
    cancel::Cancel,
    failure::{Failure, FailureKind},
    session::Session,
};

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

/// One page owned by the caller until released.
pub struct Suggestions {
    outcome: AutocompleteOutcome,
}

/// # Safety
///
/// `session` is a live session, non-null text arguments address their lengths, `cancel` is a live
/// token when non-null, and `out` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_session_suggest(
    session: *const Session,
    input: *const u8,
    input_len: usize,
    cursor: usize,
    page_size: u16,
    continuation: *const u8,
    continuation_len: usize,
    cancel: *const Cancel,
    out: *mut *mut Suggestions,
) -> *mut Failure {
    // SAFETY: the header requires all non-null input pointers to be live and all outputs writable.
    abi::outcome(unsafe {
        write_suggest(
            session,
            input,
            input_len,
            cursor,
            page_size,
            continuation,
            continuation_len,
            cancel,
            out,
        )
    })
}

/// # Safety
///
/// As [`nx_session_suggest`].
#[expect(
    clippy::too_many_arguments,
    reason = "C strings each carry a pointer and byte length"
)]
unsafe fn write_suggest(
    session: *const Session,
    input: *const u8,
    input_len: usize,
    cursor: usize,
    page_size: u16,
    continuation: *const u8,
    continuation_len: usize,
    cancel: *const Cancel,
    out: *mut *mut Suggestions,
) -> Result<(), Failure> {
    abi::require_out(out, "out")?;
    // SAFETY: the caller upholds the header's pointer and lifetime contract.
    let (session, input, continuation, cancel) = unsafe {
        (
            abi::handle(session, "session")?,
            abi::text(input, input_len, "input")?,
            abi::optional_text(continuation, continuation_len, "continuation")?,
            cancel.as_ref(),
        )
    };
    let outcome = session.suggest_page(input, cursor, page_size, continuation, cancel)?;
    // SAFETY: the caller guarantees the out pointer is writable.
    unsafe { abi::write(out, abi::into_handle(Suggestions { outcome })) };
    Ok(())
}

/// # Safety
///
/// `suggestions` is a live completion page returned by this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_suggestions_status(
    suggestions: *const Suggestions,
) -> CompletionStatus {
    // SAFETY: the header requires a live page.
    unsafe { abi::accessor(suggestions) }.outcome.status.into()
}

/// # Safety
///
/// `suggestions` is a live completion page returned by this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_suggestions_count(suggestions: *const Suggestions) -> usize {
    // SAFETY: the header requires a live page.
    unsafe { abi::accessor(suggestions) }
        .outcome
        .suggestions
        .len()
}

/// # Safety
///
/// `suggestions` is a live completion page and non-null output pointers are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_suggestions_at(
    suggestions: *const Suggestions,
    index: usize,
    kind: *mut CompletionKind,
    value: *mut *const u8,
    value_len: *mut usize,
    start: *mut u32,
    end: *mut u32,
    replacement: *mut *const u8,
    replacement_len: *mut usize,
) -> *mut Failure {
    // SAFETY: the header requires a live page and writable non-null outputs.
    abi::outcome(unsafe {
        write_suggestion(
            suggestions,
            index,
            kind,
            value,
            value_len,
            start,
            end,
            replacement,
            replacement_len,
        )
    })
}

/// # Safety
///
/// As [`nx_suggestions_at`].
#[expect(
    clippy::too_many_arguments,
    reason = "the C ABI returns each borrowed string separately"
)]
unsafe fn write_suggestion(
    suggestions: *const Suggestions,
    index: usize,
    kind: *mut CompletionKind,
    value: *mut *const u8,
    value_len: *mut usize,
    start: *mut u32,
    end: *mut u32,
    replacement: *mut *const u8,
    replacement_len: *mut usize,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live page.
    let suggestions = unsafe { abi::handle(suggestions, "suggestions") }?;
    let Some(suggestion) = suggestions.outcome.suggestions.get(index) else {
        return Err(Failure::new(
            FailureKind::InvalidArgument,
            format!("suggestion index {index} is out of range"),
        ));
    };
    // SAFETY: the caller guarantees all non-null output pointers are writable.
    unsafe {
        abi::write(kind, suggestion.kind.into());
        abi::write_bytes(value, value_len, suggestion.value.as_bytes());
        abi::write(start, suggestion.edit.start);
        abi::write(end, suggestion.edit.end);
        abi::write_bytes(
            replacement,
            replacement_len,
            suggestion.edit.replacement.as_bytes(),
        );
    }
    Ok(())
}

/// # Safety
///
/// `suggestions` is a live completion page and non-null output pointers are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_suggestions_continuation(
    suggestions: *const Suggestions,
    continuation: *mut *const u8,
    continuation_len: *mut usize,
) -> bool {
    // SAFETY: the header requires a live page.
    let suggestions = unsafe { abi::accessor(suggestions) };
    let Some(next) = suggestions.outcome.continuation.as_ref() else {
        return false;
    };
    // SAFETY: the header requires writable non-null outputs.
    unsafe { abi::write_bytes(continuation, continuation_len, next.as_bytes()) };
    true
}

/// # Safety
///
/// A non-null page was returned by this library and has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_suggestions_free(suggestions: *mut Suggestions) {
    // SAFETY: the header requires one release for an owned page.
    unsafe { abi::release(suggestions) };
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use nervix_client_core::{AutocompleteSuggestion, TextEdit};

    use super::*;

    #[test]
    fn c_accessors_keep_page_status_edit_and_continuation() {
        let page = abi::into_handle(Suggestions {
            outcome: AutocompleteOutcome {
                status: SuggestionStatus::Ready,
                suggestions: vec![AutocompleteSuggestion {
                    value: "CLUSTER".to_string(),
                    kind: SuggestionKind::Text,
                    edit: TextEdit {
                        start: 5,
                        end: 11,
                        replacement: "CLUSTER".to_string(),
                    },
                }],
                continuation: Some("next-page".to_string()),
            },
        });
        // SAFETY: this test owns the live page and writes into its local outputs.
        unsafe {
            assert_eq!(nx_suggestions_status(page), CompletionStatus::Ready);
            assert_eq!(nx_suggestions_count(page), 1);
            let mut kind = CompletionKind::LocalDirectoryLookup;
            let mut value = ptr::null();
            let mut value_len = 0;
            let mut start = 0;
            let mut end = 0;
            let mut replacement = ptr::null();
            let mut replacement_len = 0;
            let failure = nx_suggestions_at(
                page,
                0,
                &mut kind,
                &mut value,
                &mut value_len,
                &mut start,
                &mut end,
                &mut replacement,
                &mut replacement_len,
            );
            assert!(failure.is_null());
            assert_eq!(kind, CompletionKind::Text);
            assert_eq!(std::slice::from_raw_parts(value, value_len), b"CLUSTER");
            assert_eq!((start, end), (5, 11));
            assert_eq!(
                std::slice::from_raw_parts(replacement, replacement_len),
                b"CLUSTER"
            );
            let mut continuation = ptr::null();
            let mut continuation_len = 0;
            assert!(nx_suggestions_continuation(
                page,
                &mut continuation,
                &mut continuation_len
            ));
            assert_eq!(
                std::slice::from_raw_parts(continuation, continuation_len),
                b"next-page"
            );
            nx_suggestions_free(page);
        }
    }

    #[test]
    fn c_accessors_report_empty_statuses_and_an_out_of_range_index() {
        let statuses = [
            (
                SuggestionStatus::MissingContext,
                CompletionStatus::MissingContext,
            ),
            (
                SuggestionStatus::StaleContext,
                CompletionStatus::StaleContext,
            ),
            (
                SuggestionStatus::LookupFailed,
                CompletionStatus::LookupFailed,
            ),
        ];
        for (status, expected) in statuses {
            let page = abi::into_handle(Suggestions {
                outcome: AutocompleteOutcome {
                    status,
                    suggestions: Vec::new(),
                    continuation: None,
                },
            });
            // SAFETY: this test owns the live page and writes into its local outputs.
            unsafe {
                assert_eq!(nx_suggestions_status(page), expected);
                assert_eq!(nx_suggestions_count(page), 0);
                let mut continuation = ptr::null();
                let mut continuation_len = 0;
                assert!(!nx_suggestions_continuation(
                    page,
                    &mut continuation,
                    &mut continuation_len,
                ));
                let failure = nx_suggestions_at(
                    page,
                    0,
                    ptr::null_mut(),
                    ptr::null_mut(),
                    ptr::null_mut(),
                    ptr::null_mut(),
                    ptr::null_mut(),
                    ptr::null_mut(),
                    ptr::null_mut(),
                );
                assert!(!failure.is_null());
                crate::failure::nx_error_free(failure);
                nx_suggestions_free(page);
            }
        }
    }

    #[test]
    fn c_completion_request_requires_a_result_pointer_before_a_session() {
        // SAFETY: null inputs are explicitly checked before they are dereferenced.
        unsafe {
            let missing_out = nx_session_suggest(
                ptr::null(),
                ptr::null(),
                0,
                0,
                1,
                ptr::null(),
                0,
                ptr::null(),
                ptr::null_mut(),
            );
            assert!(!missing_out.is_null());
            crate::failure::nx_error_free(missing_out);

            let mut result = ptr::null_mut();
            let missing_session = nx_session_suggest(
                ptr::null(),
                b"USE ".as_ptr(),
                4,
                4,
                1,
                ptr::null(),
                0,
                ptr::null(),
                &mut result,
            );
            assert!(!missing_session.is_null());
            assert!(result.is_null());
            crate::failure::nx_error_free(missing_session);
        }
    }
}
