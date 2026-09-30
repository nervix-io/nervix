//! The shared Rust binding of the Nervix client session: the C ABI that Python, JVM, Ruby, C and
//! C++ hosts load instead of reimplementing the session.
//!
//! `include/nervix_client.h` is the contract. Every host drives the same [`Client`] state
//! machine the Rust client uses, so correlation, redirects, reconnection, execution identity,
//! subscription restoration, domain clock re-attachment, and the restoration, credit, outcomes
//! and settlements of producers and consumers are decided once, here, and never reinterpreted by
//! a host.
//!
//! Layer: edges.
//!
//! - **Owns.** The C ABI: the handles a host holds, their ownership and release, blocking calls
//!   with cancellation and deadlines, the typed failure a host reads, bulk column access to the
//!   rows of a verified frame, the typed fields of a domain clock event, the clock of a followed
//!   domain with the arithmetic that projects it, producers and consumers of client endpoints with
//!   their typed outcomes and settlements, and building and reading their Arrow batches one
//!   column level at a time.
//! - **Depends on.** The Rust session client and its wire contract, the vocabulary it names,
//!   Arrow's arrays for the batches of client endpoints, and Tokio to run the session on threads
//!   the library owns.
//! - **Must not know.** The server, the parser, how an Arrow IPC stream is encoded or decoded,
//!   which the Rust client does, or anything about a host language beyond the C ABI. Row
//!   subscriptions read verified frames and never Arrow.
//!
//! [`Client`]: nervix_client_core::Client

mod abi;
mod batch;
mod batch_builder;
mod cancel;
mod clock_event;
mod consumer;
mod delivery;
mod domain_clock;
mod endpoint;
mod event;
mod failure;
mod fields;
mod outcome;
mod producer;
mod schema;
mod session;
mod submission;
mod suggestions;

pub use batch::{
    Batch, nx_batch_cells, nx_batch_fixed, nx_batch_ipc, nx_batch_offsets, nx_batch_release,
    nx_batch_retain, nx_batch_row_count, nx_batch_schema, nx_batch_states, nx_batch_varlen,
};
pub use batch_builder::{
    BatchBuilder, nx_batch_builder_finish, nx_batch_builder_fixed, nx_batch_builder_free,
    nx_batch_builder_new, nx_batch_builder_offsets, nx_batch_builder_states,
    nx_batch_builder_varlen,
};
pub use cancel::{
    Cancel, nx_cancel_free, nx_cancel_new, nx_cancel_trigger, nx_cancel_with_deadline,
};
pub use clock_event::{
    ClockEndReason, ClockEvent, ClockEventKind, ClockState, nx_clock_event_domain,
    nx_clock_event_end_reason, nx_clock_event_generation, nx_clock_event_kind_of,
    nx_clock_event_paced, nx_clock_event_release, nx_clock_event_retain, nx_clock_event_state,
    nx_clock_event_tick,
};
pub use consumer::{
    Consumer, nx_consumer_close, nx_consumer_contract, nx_consumer_free, nx_consumer_generation,
    nx_consumer_grant, nx_consumer_next, nx_consumer_policy, nx_consumer_reopen_reason,
    nx_consumer_schema, nx_consumer_state, nx_session_subscribe_emitter,
};
pub use delivery::{
    Delivery, Settlement, nx_delivery_ack, nx_delivery_batch, nx_delivery_branch_fingerprint,
    nx_delivery_execution_now, nx_delivery_identity, nx_delivery_ipc, nx_delivery_members,
    nx_delivery_reference, nx_delivery_reject, nx_delivery_release, nx_delivery_retain,
    nx_delivery_retry, nx_delivery_source_relay,
};
pub use domain_clock::{
    DomainClock, nx_domain_clock_admission_window, nx_domain_clock_admits, nx_domain_clock_domain,
    nx_domain_clock_generation, nx_domain_clock_logical_time_at, nx_domain_clock_paced,
    nx_domain_clock_release, nx_domain_clock_retain, nx_domain_clock_state, nx_domain_clock_tick,
    nx_domain_clock_wall_duration_until,
};
pub use endpoint::{EndpointState, OpenRefusal, Reopen, ReopenReason, Window, WindowKind};
pub use event::{
    CellState, Event, EventKind, nx_event_cell_varlen, nx_event_column_fixed,
    nx_event_column_states, nx_event_column_varlen, nx_event_frame, nx_event_kind_of,
    nx_event_release, nx_event_retain, nx_event_row_count, nx_event_schema, nx_event_subscription,
};
pub use failure::{
    Failure, FailureKind, nx_error_execution_reference, nx_error_free, nx_error_kind_of,
    nx_error_message, nx_error_open_refusal,
};
pub use fields::{Fields, nx_fields_add, nx_fields_element, nx_fields_free, nx_fields_new};
pub use outcome::{
    Disposition, Outcome, nx_outcome_backup, nx_outcome_diagnostic, nx_outcome_diagnostic_count,
    nx_outcome_disposition, nx_outcome_execution_reference, nx_outcome_free, nx_outcome_message,
    nx_outcome_restore, nx_outcome_schema, nx_outcome_subscription,
};
pub use producer::{
    Admission, Producer, nx_producer_admission, nx_producer_close, nx_producer_contract,
    nx_producer_free, nx_producer_generation, nx_producer_grant, nx_producer_pending,
    nx_producer_policy, nx_producer_rejoin, nx_producer_release, nx_producer_reopen_reason,
    nx_producer_schema, nx_producer_state, nx_producer_submit, nx_producer_submit_ipc,
    nx_session_open_ingestor,
};
pub use schema::{
    FieldType, Part, Schema, nx_schema_branch, nx_schema_field, nx_schema_field_count,
    nx_schema_field_level, nx_schema_field_levels, nx_schema_free,
};
pub use session::{
    Execution, Session, nx_execution_free, nx_execution_reference, nx_session_connect,
    nx_session_domain_clock, nx_session_execute, nx_session_free, nx_session_next_clock_event,
    nx_session_next_event, nx_session_prepare,
};
pub use submission::{
    BatchDefect, ProcessingFailure, SubmissionOutcome, SubmissionRefusal, SubmissionResult,
    Uncertainty, nx_submission_outcome_defect, nx_submission_outcome_failure,
    nx_submission_outcome_free, nx_submission_outcome_message, nx_submission_outcome_refusal,
    nx_submission_outcome_result, nx_submission_outcome_uncertainty,
};
pub use suggestions::{
    CompletionKind, CompletionStatus, Suggestions, nx_session_suggest, nx_suggestions_at,
    nx_suggestions_continuation, nx_suggestions_count, nx_suggestions_free, nx_suggestions_status,
};

#[cfg(test)]
mod tests;
