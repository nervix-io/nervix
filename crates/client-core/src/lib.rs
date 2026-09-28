//! A client session against a Nervix server.
//!
//! Layer: edges.
//!
//! - **Owns.** Connecting to the session service, TLS selection, the dispatcher that pairs every
//!   reply of a session exchange with the request it answers, submitting statements, transaction
//!   state, completion suggestions, subscription streams, the domain clocks the session follows and
//!   resource upload.
//! - **Depends on.** The session wire contract, the language layer — an edge may name the parser,
//!   and this one does so for client-side parsing and completion — and the vocabulary.
//! - **Must not know.** The registry, the runtime, or anything else inside the server. Everything
//!   it learns arrives over the session protocol.

#[cfg(feature = "shuttle")]
extern crate shuttle_tokio as tokio;
#[cfg(feature = "shuttle")]
extern crate shuttle_tokio_stream as tokio_stream;

mod client;
mod connection;
mod domain_clock;
mod error;
mod events;
mod exchange;
mod outcome;
mod subscriptions;
mod upload;

pub use client::{Client, ExecutionHandle};
pub use connection::{ConnectDns, ConnectOptions, TlsRequirement};
pub use domain_clock::{
    AttachedDomainClock, DomainClockEvent, DomainClockInterruption, DomainClockReadError,
};
pub use error::{ClientError, EventStreamKind, RequestKind};
use error_stack::ResultExt as _;
pub use events::{
    AutocompleteOutcome, AutocompleteSuggestion, ServerEvent, SubscriptionEvent,
    SubscriptionRequest, SubscriptionRowsEvent,
};
pub use nervix_client_wire as wire;
pub use nervix_client_wire::{
    Choice, ChoiceLookupRequest, ChoiceOutcome, ChoicePresentation, ChoiceSelection, ChoiceStatus,
    ChoiceTarget, ChoiceValue, CommandDisposition, Diagnostic, DomainClockAttachDisposition,
    DomainClockAttachOutcome, DomainClockAttachmentEndReason, DomainClockAttachmentEnded,
    DomainClockDetachDisposition, DomainClockDetachOutcome, DomainClockObserved, DomainClockTicked,
    DomainInfo, DomainPaceChoice, ExecutionReferenceConflict, LeaderEndpoints, LeaderRedirect,
    Leadership, NoticeLevel, OutcomeOrigin, RowConformanceError, RowSchema, SourceSpan,
    StatementDisposition, StatementOutcome, SubscriptionDeliveryLost, SubscriptionEnded,
    SubscriptionHandle, SubscriptionOpened, SubscriptionRows, SubscriptionRowsSkipped,
    SuggestionKind, SuggestionStatus, TextEdit, UnknownOutcomeCause, UploadFailure,
};
pub use nervix_models::{
    CommandExecutionReference, DomainAdmissionWindow, DomainClockObservation,
    DomainClockObservedState, DomainClockTickObservation, DomainName, ImpactPlanningBasis,
    PacedDomainClock, ResourceUploadIdentity, SubscriptionDeliveryBehavior, Timestamp,
    TransactionImpactReport, TransactionInspection, TransactionLifecycle,
    TransactionOperationAdmission, TransactionOperationNumber, TransactionPosition,
    TransactionPreviewIdentity, TransactionStatus,
};
pub use outcome::{CommandOutcome, ResourceUploadOutcome};
pub use subscriptions::{SubscriptionInterruption, SubscriptionLifecycle};
use thiserror::Error;

/// A statement batch that could not be split, over the language's report of why it was rejected.
#[derive(Debug, Error)]
#[error("failed to parse the statement batch")]
pub struct QuerySplitError;

/// Splits a batch into the exact NSPL source slices that should be submitted separately.
pub fn split_query_statements(query: &str) -> error_stack::Result<Vec<&str>, QuerySplitError> {
    let statements = nervix_nspl::client_statement::parse_client_statement_sources(query)
        .change_context(QuerySplitError)?;
    Ok(statements
        .iter()
        .map(|statement| statement.source(query))
        .collect())
}

#[cfg(test)]
mod tests;
