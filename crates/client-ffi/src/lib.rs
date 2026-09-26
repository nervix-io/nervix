//! The shared Rust binding of the Nervix client session: the C ABI that Python, JVM, Ruby, C and
//! C++ hosts load instead of reimplementing the session.
//!
//! `include/nervix_client.h` is the contract. Every host drives the same [`Client`] state
//! machine the Rust client uses, so correlation, redirects, reconnection, execution identity and
//! subscription restoration are decided once, here, and never reinterpreted by a host.
//!
//! Layer: edges.
//!
//! - **Owns.** The C ABI: the handles a host holds, their ownership and release, blocking calls
//!   with cancellation and deadlines, the typed failure a host reads, and bulk column access to
//!   the rows of a verified frame.
//! - **Depends on.** The Rust session client and its wire contract, the vocabulary it names, and
//!   Tokio to run the session on threads the library owns.
//! - **Must not know.** The server, the parser, Arrow, or anything about a host language beyond
//!   the C ABI.
//!
//! [`Client`]: nervix_client_core::Client

mod abi;
mod cancel;
mod event;
mod failure;
mod outcome;
mod schema;
mod session;
mod suggestions;

pub use cancel::{
    Cancel, nx_cancel_free, nx_cancel_new, nx_cancel_trigger, nx_cancel_with_deadline,
};
pub use event::{
    CellState, Event, EventKind, nx_event_cell_varlen, nx_event_column_fixed,
    nx_event_column_states, nx_event_column_varlen, nx_event_frame, nx_event_kind_of,
    nx_event_release, nx_event_retain, nx_event_row_count, nx_event_schema, nx_event_subscription,
};
pub use failure::{
    Failure, FailureKind, nx_error_execution_reference, nx_error_free, nx_error_kind_of,
    nx_error_message,
};
pub use outcome::{
    Disposition, Outcome, nx_outcome_diagnostic, nx_outcome_diagnostic_count,
    nx_outcome_disposition, nx_outcome_execution_reference, nx_outcome_free, nx_outcome_message,
    nx_outcome_schema, nx_outcome_subscription,
};
pub use schema::{
    FieldType, Part, Schema, nx_schema_branch, nx_schema_field, nx_schema_field_count,
    nx_schema_free,
};
pub use session::{
    Execution, Session, nx_execution_free, nx_execution_reference, nx_session_connect,
    nx_session_execute, nx_session_free, nx_session_next_event, nx_session_prepare,
};
pub use suggestions::{
    CompletionKind, CompletionStatus, Suggestions, nx_session_suggest, nx_suggestions_at,
    nx_suggestions_continuation, nx_suggestions_count, nx_suggestions_free, nx_suggestions_status,
};

#[cfg(test)]
mod tests;
