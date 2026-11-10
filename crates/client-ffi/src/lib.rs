//! The C ABI of the Nervix client session for C and C++ hosts.
//!
//! `include/nervix_client.h` is the contract. Every call drives the host session runtime that
//! every binding shares, [`nervix_client_host`], and through it the same [`Client`] state machine
//! the Rust client uses, so correlation, redirects, reconnection, execution identity,
//! subscription restoration, domain clock re-attachment, and the restoration, credit, outcomes and
//! settlements of producers and consumers are decided once and never reinterpreted by a host.
//!
//! Layer: edges.
//!
//! - **Owns.** The C ABI: the functions the header declares, the handles a host holds, their
//!   ownership and release, reading what a host passes in and writing what it reads back.
//! - **Depends on.** The shared host session runtime, the Rust client vocabulary the header
//!   names, and the C calling convention.
//! - **Must not know.** The session, its runtime, how a failure is classified, or how a batch is
//!   built or read, which the shared runtime decides; the server, the parser, or anything about a
//!   host language beyond the C ABI.
//!
//! [`Client`]: nervix_client_core::Client

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

mod abi;
mod batch;
mod batch_builder;
mod cancel;
mod clock_event;
mod connect_options;
mod consumer;
mod delivery;
mod domain_clock;
mod entity_labels;
mod event;
mod failure;
mod fields;
mod inspection;
mod outcome;
mod producer;
mod schema;
mod session;
mod submission;
mod suggestions;

pub use batch::{
    nx_batch_cells, nx_batch_fixed, nx_batch_ipc, nx_batch_offsets, nx_batch_release,
    nx_batch_retain, nx_batch_row_count, nx_batch_schema, nx_batch_states, nx_batch_varlen,
};
pub use batch_builder::{
    nx_batch_builder_finish, nx_batch_builder_fixed, nx_batch_builder_free, nx_batch_builder_new,
    nx_batch_builder_offsets, nx_batch_builder_states, nx_batch_builder_varlen,
};
pub use cancel::{nx_cancel_free, nx_cancel_new, nx_cancel_trigger, nx_cancel_with_deadline};
pub use clock_event::{
    nx_clock_event_domain, nx_clock_event_end_reason, nx_clock_event_generation,
    nx_clock_event_kind_of, nx_clock_event_paced, nx_clock_event_release,
    nx_clock_event_restoration_cause, nx_clock_event_restoration_message,
    nx_clock_event_restoration_retry_after, nx_clock_event_retain, nx_clock_event_state,
    nx_clock_event_tick,
};
pub use connect_options::{
    ConnectionOptions, nx_connect_options_add_seed, nx_connect_options_basic_auth,
    nx_connect_options_ca_certificate, nx_connect_options_free, nx_connect_options_new,
    nx_connect_options_timeout, nx_connect_options_tls_requirement,
};
pub use consumer::{
    nx_consumer_close, nx_consumer_contract, nx_consumer_free, nx_consumer_generation,
    nx_consumer_grant, nx_consumer_next, nx_consumer_policy, nx_consumer_reopen_reason,
    nx_consumer_schema, nx_consumer_state, nx_session_subscribe_emitter,
};
pub use delivery::{
    nx_delivery_ack, nx_delivery_batch, nx_delivery_branch_fingerprint, nx_delivery_execution_now,
    nx_delivery_identity, nx_delivery_ipc, nx_delivery_members, nx_delivery_reference,
    nx_delivery_reject, nx_delivery_release, nx_delivery_retain, nx_delivery_retry,
    nx_delivery_source_relay,
};
pub use domain_clock::{
    nx_domain_clock_admission_window, nx_domain_clock_admits, nx_domain_clock_domain,
    nx_domain_clock_generation, nx_domain_clock_logical_time_at, nx_domain_clock_paced,
    nx_domain_clock_release, nx_domain_clock_retain, nx_domain_clock_state, nx_domain_clock_tick,
    nx_domain_clock_wall_duration_until,
};
pub use event::{
    nx_event_cell_varlen, nx_event_column_fixed, nx_event_column_states, nx_event_column_varlen,
    nx_event_end_reason, nx_event_frame, nx_event_kind_of, nx_event_release,
    nx_event_restoration_cause, nx_event_restoration_message, nx_event_restoration_retry_after,
    nx_event_retain, nx_event_row_count, nx_event_schema, nx_event_subscription,
};
pub use failure::{
    nx_error_execution_reference, nx_error_free, nx_error_kind_of, nx_error_message,
    nx_error_open_refusal,
};
pub use fields::{nx_fields_add, nx_fields_element, nx_fields_free, nx_fields_new};
pub use nervix_client_host::{
    Admission, Batch, BatchBuilder, BatchDefect, Cancel, CellState, ClockEndReason, ClockEvent,
    ClockEventKind, ClockState, CompletionKind, CompletionStatus, Consumer, Delivery, Disposition,
    DomainClock, EndReason, EndpointState, Event, EventKind, Execution, Failure, FailureKind,
    FieldType, Fields, OpenRefusal, Outcome, Part, ProcessingFailure, Producer, Reopen,
    ReopenReason, RestorationCause, Schema, Session, Settlement, SubmissionOutcome,
    SubmissionRefusal, SubmissionResult, Suggestions, Uncertainty, UploadFailure, Window,
    WindowKind,
};
pub use outcome::{
    nx_outcome_backup, nx_outcome_diagnostic, nx_outcome_diagnostic_count, nx_outcome_disposition,
    nx_outcome_execution_reference, nx_outcome_free, nx_outcome_message, nx_outcome_restore,
    nx_outcome_schema, nx_outcome_subscription, nx_outcome_upload_failure,
};
pub use producer::{
    nx_producer_admission, nx_producer_close, nx_producer_contract, nx_producer_free,
    nx_producer_generation, nx_producer_grant, nx_producer_pending, nx_producer_policy,
    nx_producer_rejoin, nx_producer_release, nx_producer_reopen_reason, nx_producer_schema,
    nx_producer_state, nx_producer_submit, nx_producer_submit_ipc, nx_session_open_ingestor,
};
pub use schema::{
    nx_schema_branch, nx_schema_field, nx_schema_field_count, nx_schema_field_level,
    nx_schema_field_levels, nx_schema_free,
};
pub use session::{
    nx_execution_free, nx_execution_reference, nx_session_connect, nx_session_domain_clock,
    nx_session_execute, nx_session_free, nx_session_next_clock_event, nx_session_next_event,
    nx_session_prepare,
};
pub use submission::{
    nx_submission_outcome_defect, nx_submission_outcome_failure, nx_submission_outcome_free,
    nx_submission_outcome_message, nx_submission_outcome_refusal, nx_submission_outcome_result,
    nx_submission_outcome_uncertainty,
};
pub use suggestions::{
    nx_session_suggest, nx_suggestions_at, nx_suggestions_continuation, nx_suggestions_count,
    nx_suggestions_free, nx_suggestions_status,
};

#[cfg(test)]
mod tests;

pub use entity_labels::*;
pub use inspection::{
    nx_inspection_counts, nx_inspection_free, nx_inspection_json, nx_inspection_row_frame,
    nx_inspection_schema, nx_outcome_inspection,
};
pub use nervix_client_host::{EntityLabelViewKind, EntityLabelsInspection, Inspection};
