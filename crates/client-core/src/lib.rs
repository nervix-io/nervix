//! A client session against a Nervix server.
//!
//! Layer: edges.
//!
//! - **Owns.** Connecting to the session service, TLS selection, the dispatcher that pairs every
//!   reply of a session exchange with the request it answers, submitting statements, transaction
//!   state, completion suggestions, subscription streams, the domain clocks the session follows,
//!   the producers it opens against client ingestors, resource upload, backup download and
//!   restore.
//! - **Depends on.** The session wire contract, the language layer — an edge may name the parser,
//!   and this one does so for client-side parsing and completion — and the vocabulary.
//! - **Must not know.** The registry, the runtime, or anything else inside the server. Everything
//!   it learns arrives over the session protocol.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "native client session and attachment ownership belongs to the client edge"
    )
)]

mod backup;
mod client;
mod connection;
mod consumer;
mod domain_clock;
mod error;
mod events;
mod exchange;
mod outcome;
mod producer;
mod restoration;
mod restore;
#[cfg(all(test, feature = "shuttle"))]
mod shuttle_test;
mod subscriptions;
mod upload;

pub use backup::BackupDownloadError;
pub use client::{Client, ExecutionHandle};
pub use connection::{ConnectDns, ConnectOptions, TlsRequirement};
pub use consumer::{ConsumerConnection, ConsumerReopenReason, EmitterConsumer, EmitterDelivery};
pub use domain_clock::{
    AttachedDomainClock, DomainClockEvent, DomainClockInterruption, DomainClockReadError,
    DomainClockRestorationFailure,
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
    DomainInfo, DomainPaceChoice, EmitterSettlement, ExecutionReferenceConflict, LeaderEndpoints,
    LeaderRedirect, Leadership, NoticeLevel, OutcomeOrigin, ProducerId, RestoreUploadFailure,
    RowConformanceError, RowSchema, SourceSpan, StatementDisposition, StatementOutcome,
    SubscriptionDeliveryLost, SubscriptionEnded, SubscriptionHandle, SubscriptionOpened,
    SubscriptionRows, SubscriptionRowsSkipped, SuggestionKind, SuggestionStatus, TextEdit,
    UnknownOutcomeCause, UploadFailure,
};
pub use nervix_models::{
    AckWindow, ArchiveDigest, BackupArchiveSummary, BackupDomainSummary, BackupResources,
    ClientAttachmentId, ClientBatchDefect, ClientConsumerLimits, ClientEndpointContract,
    ClientProcessingFailure, ClientProducerAdmission, ClientProducerDescription,
    ClientProducerEndReason, ClientProducerGrant, ClientProducerLimits, ClientProducerPolicy,
    ClientProducerRefusal, ClientSubmissionRefusal, CommandExecutionReference,
    DomainAdmissionWindow, DomainClockObservation, DomainClockObservedState,
    DomainClockTickObservation, DomainName, EmitterName, ExistingUserPolicy, ImpactPlanningBasis,
    IngestorName, PacedDomainClock, ResourceUploadIdentity, Restore, RestoreArchive, RestoreMode,
    RestoreReport, RestoreScope, RestoreStep, RestoreStepOutcome, RestoreStepReport,
    RestoredDomain, RestoredUsers, SchemaField, SubscriptionDeliveryBehavior, Timestamp,
    TransactionImpactReport, TransactionInspection, TransactionLifecycle,
    TransactionOperationAdmission, TransactionOperationNumber, TransactionPosition,
    TransactionPreviewIdentity, TransactionStatus,
};
pub use outcome::{CommandOutcome, ResourceUploadOutcome};
pub use producer::{
    PendingSubmission, Producer, ProducerBatch, ProducerConnection, ProducerEnd, ProducerError,
    ProducerOutcome, ProducerReopenReason, SubmissionId, SubmissionUncertainty,
};
pub use subscriptions::{
    SubscriptionInterruption, SubscriptionLifecycle, SubscriptionRestorationFailure,
};
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
