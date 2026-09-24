//! The outcome of a statement, whichever request served it, and what an outcome asks of the
//! client before the request behind it is complete.
//!
//! - **Owns.** The outcome a caller receives for every statement, the conversions from each reply
//!   that can answer a statement, and the routing a reply calls for.
//! - **Depends on.** The wire contract's outcomes and the vocabulary's transaction state.
//! - **Must not know.** How a request is sent, retried or redirected.

use std::num::NonZeroU64;

use meticulous::ResultExt as _;
use nervix_client_wire::{
    self as wire, AttachDisposition, AttachOutcome, CommandDisposition, Diagnostic,
    InspectionOutcome, LeaderRedirect, OutcomeOrigin, SourceSpan, StatementOutcome,
    SubscribeDisposition, SubscribeOutcome, SubscriptionOpened, UnsubscribeDisposition,
    UnsubscribeOutcome, UploadDisposition, UploadFailure, UploadReply,
};
use nervix_models::{
    CommandExecutionReference, ResourceUploadIdentity, TransactionInspection,
    TransactionOperationAdmission, TransactionPreviewIdentity, TransactionStatus,
};
use url::Url;

/// What became of a statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutcome {
    /// Stable identity of the logical server command across redirects and reconnects. `None` for
    /// a statement the client serves itself — `USE`, `LIST DOMAINS` and `UPLOAD RESOURCE` — and
    /// for a subscription request.
    pub execution_reference: Option<CommandExecutionReference>,
    /// Whether the outcome was produced now or recovered from an earlier attempt of the same
    /// command. `None` when no command request produced it.
    pub origin: Option<OutcomeOrigin>,
    pub disposition: CommandDisposition,
    pub message: String,
    pub diagnostics: Vec<Diagnostic>,
    /// The outcome of each statement of a multi-statement command, in written order. Empty for a
    /// single statement.
    pub statements: Vec<StatementOutcome>,
    /// The transaction this session was bound to while serving the command.
    pub transaction: Option<TransactionStatus>,
    /// The preview an accepted append made current, which a later COMMIT fences against.
    pub transaction_admission: Option<TransactionOperationAdmission>,
    /// The transaction a `DESCRIBE TRANSACTION` read, whichever format rendered its message. It
    /// may name a transaction other than `transaction`, which stays this session's own binding.
    pub inspection: Option<Box<TransactionInspection>>,
    /// Present when the statement opened a subscription.
    pub subscription: Option<Box<SubscriptionOpened>>,
    pub resource_upload: Option<ResourceUploadOutcome>,
}

/// What became of a resource upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceUploadOutcome {
    pub identity: ResourceUploadIdentity,
    /// The installed version, or the version assigned before a failure, when there is one.
    pub version: Option<NonZeroU64>,
    /// Whether the installation happened now or was recovered from an earlier attempt with the
    /// same identity, when the resource was installed.
    pub origin: Option<OutcomeOrigin>,
    /// Why the server failed the upload, when it did.
    pub failure: Option<UploadFailure>,
}

impl CommandOutcome {
    /// Whether the statement completed.
    pub fn succeeded(&self) -> bool {
        matches!(self.disposition, CommandDisposition::Completed { .. })
    }

    /// Whether the statement found the entity it creates already present and changed nothing.
    pub fn already_existed(&self) -> bool {
        matches!(
            self.disposition,
            CommandDisposition::Completed {
                already_existed: true
            }
        )
    }

    /// An outcome of a statement the client served itself.
    fn local(disposition: CommandDisposition, message: String) -> Self {
        Self {
            execution_reference: None,
            origin: None,
            disposition,
            message,
            diagnostics: Vec::new(),
            statements: Vec::new(),
            transaction: None,
            transaction_admission: None,
            inspection: None,
            subscription: None,
            resource_upload: None,
        }
    }

    pub(crate) fn completed_locally(message: String) -> Self {
        Self::local(
            CommandDisposition::Completed {
                already_existed: false,
            },
            message,
        )
    }

    pub(crate) fn failed_locally(message: String) -> Self {
        Self::local(CommandDisposition::Failed, message)
    }

    /// The outcome of an upload whose reply the client checked against the identity it sent.
    pub(crate) fn from_upload(reply: UploadReply, identity: ResourceUploadIdentity) -> Self {
        let mut resource_upload = ResourceUploadOutcome {
            identity,
            version: None,
            origin: None,
            failure: None,
        };
        let disposition = match reply.disposition {
            UploadDisposition::Installed {
                version, origin, ..
            } => {
                resource_upload.version = Some(version);
                resource_upload.origin = Some(origin);
                CommandDisposition::Completed {
                    already_existed: false,
                }
            }
            UploadDisposition::Failed {
                failure,
                assigned_version,
                ..
            } => {
                resource_upload.version = assigned_version;
                resource_upload.failure = Some(failure);
                CommandDisposition::Failed
            }
            UploadDisposition::NotLeader(redirect) => CommandDisposition::NotLeader(redirect),
        };
        let mut outcome = Self::local(disposition, reply.message);
        outcome.diagnostics = reply.diagnostics;
        outcome.resource_upload = Some(resource_upload);
        outcome
    }

    /// Moves the diagnostics of a statement that was sent on its own, and that starts `offset`
    /// bytes into the query the caller passed, so their spans address that query.
    ///
    /// A span the query's offsets cannot express keeps its message and loses its location.
    pub(crate) fn locate_diagnostics_in_query(&mut self, offset: usize) {
        for diagnostic in &mut self.diagnostics {
            let Some(span) = diagnostic.span else {
                continue;
            };
            diagnostic.span = shifted_span(span, offset);
        }
    }

    /// What the outcome asks of the client before the command behind it is complete.
    pub(crate) fn routing(&self) -> Routing<'_> {
        match &self.disposition {
            CommandDisposition::NotLeader(redirect) => Routing::for_redirect(redirect),
            CommandDisposition::TransactionDetached { .. } => Routing::Detached,
            CommandDisposition::OutcomeUnknown(_) => Routing::AwaitOutcome,
            CommandDisposition::Completed { .. }
            | CommandDisposition::Failed
            | CommandDisposition::TransactionTakenOver { .. }
            | CommandDisposition::ExecutionReferenceConflict(_)
            | CommandDisposition::ExecutionReferenceExpired
            | CommandDisposition::PreviewStale { .. } => Routing::Complete,
        }
    }

    /// The preview a later COMMIT should fence against, as this outcome reports it.
    ///
    /// An accepted append makes its own preview current. A refused commit reports the preview
    /// that now describes the transaction, so the caller can decide again against the transaction
    /// as it actually is instead of staying fenced against a revision it already knows is gone.
    pub(crate) fn commit_basis(&self) -> Option<&TransactionPreviewIdentity> {
        if let Some(admission) = &self.transaction_admission {
            return Some(&admission.preview);
        }
        if let CommandDisposition::PreviewStale { current, .. } = &self.disposition {
            return Some(current);
        }
        None
    }
}

impl From<wire::CommandOutcome> for CommandOutcome {
    fn from(outcome: wire::CommandOutcome) -> Self {
        Self {
            execution_reference: Some(outcome.execution_reference),
            origin: Some(outcome.origin),
            disposition: outcome.disposition,
            message: outcome.message,
            diagnostics: outcome.diagnostics,
            statements: outcome.statements,
            transaction: outcome.transaction,
            transaction_admission: outcome.transaction_admission,
            inspection: outcome.inspection,
            subscription: None,
            resource_upload: None,
        }
    }
}

impl From<AttachOutcome> for CommandOutcome {
    /// A finished transaction stays unattached: its final status is reported beside a failed
    /// disposition, so the caller learns how it ended without holding it.
    fn from(outcome: AttachOutcome) -> Self {
        let mut attached = Self::local(CommandDisposition::Failed, outcome.message);
        attached.diagnostics = outcome.diagnostics;
        match outcome.disposition {
            AttachDisposition::Attached(status) => {
                attached.disposition = CommandDisposition::Completed {
                    already_existed: false,
                };
                attached.transaction = Some(status);
            }
            AttachDisposition::AlreadyFinished(status) => attached.transaction = Some(status),
            AttachDisposition::Failed => {}
            AttachDisposition::NotLeader(redirect) => {
                attached.disposition = CommandDisposition::NotLeader(redirect);
            }
        }
        attached
    }
}

impl From<SubscribeOutcome> for CommandOutcome {
    fn from(outcome: SubscribeOutcome) -> Self {
        let mut subscribed = Self::local(CommandDisposition::Failed, outcome.message);
        subscribed.diagnostics = outcome.diagnostics;
        if let SubscribeDisposition::Opened(opened) = outcome.disposition {
            subscribed.disposition = CommandDisposition::Completed {
                already_existed: false,
            };
            subscribed.subscription = Some(opened);
        }
        subscribed
    }
}

impl From<UnsubscribeOutcome> for CommandOutcome {
    fn from(outcome: UnsubscribeOutcome) -> Self {
        let disposition = match outcome.disposition {
            UnsubscribeDisposition::Deleted(_) => CommandDisposition::Completed {
                already_existed: false,
            },
            UnsubscribeDisposition::Failed => CommandDisposition::Failed,
        };
        let mut unsubscribed = Self::local(disposition, outcome.message);
        unsubscribed.diagnostics = outcome.diagnostics;
        unsubscribed
    }
}

/// `span` moved `offset` bytes later, or `None` when an end of the moved span passes the largest
/// offset a span holds.
fn shifted_span(span: SourceSpan, offset: usize) -> Option<SourceSpan> {
    let offset = u32::try_from(offset).ok()?;
    let start = span.start().checked_add(offset)?;
    let end = span.end().checked_add(offset)?;
    let shifted = SourceSpan::new(start, end).assured(
        "a span starts at or before its end, and moving both ends by one offset keeps that order",
    );
    Some(shifted)
}

/// What a reply asks of the client before the request it answers is complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Routing<'a> {
    /// The reply is the request's outcome.
    Complete,
    /// The leader serves the request at this URI: move the session there and send it again.
    Redirect(&'a Url),
    /// No leader is known, or it advertises no session URI: wait for the election to settle and
    /// send the request again.
    AwaitElection,
    /// The node serving the session holds no binding for its transaction. Attaching the
    /// transaction again restores the binding there, after which the command can run.
    Detached,
    /// The command was durably admitted and its outcome is not known yet. Sending it again with
    /// the same execution reference recovers the outcome.
    AwaitOutcome,
}

impl<'a> Routing<'a> {
    pub(crate) fn for_redirect(redirect: &'a LeaderRedirect) -> Self {
        let Some(leader) = &redirect.leader else {
            return Self::AwaitElection;
        };
        match &leader.grpc_uri {
            Some(uri) => Self::Redirect(uri),
            None => Self::AwaitElection,
        }
    }

    pub(crate) fn for_attach(outcome: &'a AttachOutcome) -> Self {
        match &outcome.disposition {
            AttachDisposition::NotLeader(redirect) => Self::for_redirect(redirect),
            AttachDisposition::Attached(_)
            | AttachDisposition::AlreadyFinished(_)
            | AttachDisposition::Failed => Self::Complete,
        }
    }

    pub(crate) fn for_inspection(outcome: &'a InspectionOutcome) -> Self {
        match outcome {
            InspectionOutcome::NotLeader(redirect) => Self::for_redirect(redirect),
            InspectionOutcome::Inspected(_) | InspectionOutcome::Rejected { .. } => Self::Complete,
        }
    }
}
