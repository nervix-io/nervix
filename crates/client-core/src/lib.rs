//! A client session against a Nervix server.
//!
//! Layer: edges.
//!
//! - **Owns.** Connecting to the session service, TLS selection, the dispatcher that pairs every
//!   reply of a session exchange with the request it answers, submitting statements, transaction
//!   state, completion suggestions, subscription streams and resource upload.
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
mod error;
mod events;
mod exchange;
mod outcome;
mod upload;

pub use client::{Client, ExecutionHandle};
pub use connection::{ConnectOptions, TlsRequirement};
pub use error::{ClientError, EventStreamKind, RequestKind};
pub use events::{
    AutocompleteSuggestion, ServerEvent, SubscriptionEvent, SubscriptionRequest,
    SubscriptionRowsEvent,
};
pub use nervix_client_wire as wire;
pub use nervix_client_wire::{
    CommandDisposition, Diagnostic, DomainInfo, ExecutionReferenceConflict, LeaderEndpoints,
    LeaderRedirect, Leadership, NoticeLevel, OutcomeOrigin, RowConformanceError, RowSchema,
    SourceSpan, StatementDisposition, StatementOutcome, SubscriptionDeliveryLost,
    SubscriptionEnded, SubscriptionHandle, SubscriptionOpened, SubscriptionRows,
    SubscriptionRowsSkipped, SuggestionKind, UnknownOutcomeCause, UploadFailure,
};
pub use nervix_models::{
    CommandExecutionReference, DomainName, ImpactPlanningBasis, ResourceUploadIdentity,
    SubscriptionDeliveryBehavior, TransactionImpactReport, TransactionInspection,
    TransactionLifecycle, TransactionOperationAdmission, TransactionOperationNumber,
    TransactionPosition, TransactionPreviewIdentity, TransactionStatus,
};
pub use outcome::{CommandOutcome, ResourceUploadOutcome};
use thiserror::Error;

#[derive(Debug, Error)]
#[error("failed to parse the statement batch: {message}")]
pub struct QuerySplitError {
    message: String,
}

/// Splits a batch into the exact NSPL source slices that should be submitted separately.
pub fn split_query_statements(query: &str) -> error_stack::Result<Vec<&str>, QuerySplitError> {
    nervix_nspl::client_statement::parse_client_statement_sources(query)
        .map(|statements| {
            statements
                .iter()
                .map(|statement| statement.source(query))
                .collect()
        })
        .map_err(|error| {
            error_stack::Report::new(QuerySplitError {
                message: error.to_string(),
            })
        })
}

#[cfg(test)]
mod tests;
