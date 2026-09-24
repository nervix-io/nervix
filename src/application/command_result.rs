//! The outcome of one session command, as the control plane decides it.
//!
//! Layer: control plane.
//!
//! - **Owns.** A command's typed disposition, its message and diagnostics, the outcomes of the
//!   statements of a multi-statement command, and the transaction binding, admitted operation and
//!   inspection read a command reports.
//! - **Depends on.** The vocabulary for transaction status, admission, preview identity and
//!   inspection, cluster node names and service URLs, and consensus for the ways a reused
//!   execution reference can conflict and for the diagnostics its durable records keep.
//! - **Must not know.** How an outcome travels to a client, or how any transport encodes it.

use std::ops::Range;

use arch_into::ArchInto as _;
use nervix_consensus::{
    CommandExecutionDiagnostic, CommandExecutionRequestConflict, DiagnosticSpan,
    TransactionDiagnostic,
};
use nervix_models::{
    ClusterNodeName, NodeServiceUrl, TransactionInspection, TransactionOperationAdmission,
    TransactionPreviewIdentity, TransactionStatus,
};

/// What became of a command, or of one statement of a multi-statement command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::application) enum CommandDisposition {
    Completed {
        /// The statement found the entity it creates already present and changed nothing.
        already_existed: bool,
    },
    /// The command failed definitively; the message and diagnostics say why.
    Failed,
    /// The command needs the cluster leader, and this node is not the leader. Nothing was
    /// admitted.
    NotLeader(LeaderRedirect),
    /// The session names a transaction this leader holds no binding for.
    TransactionDetached { transaction_id: String },
    /// Another session attached the transaction this session was bound to.
    TransactionTakenOver { transaction_id: String },
    /// The command was durably admitted, and its outcome is not known yet. A retry with the same
    /// execution reference recovers it.
    OutcomeUnknown(OutcomeUnknownCause),
    /// The execution reference already identifies a different command.
    ExecutionReferenceConflict(CommandExecutionRequestConflict),
    /// The execution reference aged out of execution history. Its outcome cannot be recovered, and
    /// the command was not executed again.
    ExecutionReferenceExpired,
    /// The COMMIT expected a preview that no longer describes the transaction. Nothing applied,
    /// and the transaction stays open.
    PreviewStale {
        expected: TransactionPreviewIdentity,
        current: TransactionPreviewIdentity,
    },
}

/// Why an admitted command's outcome is not known yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::application) enum OutcomeUnknownCause {
    /// Leadership moved after the command was durably admitted, or while its admission was being
    /// decided.
    LeadershipLost,
    /// The command is durably admitted and still applying.
    StillApplying,
    /// The command finished, but its outcome is not yet authoritative on every live node.
    NotYetAuthoritative,
}

/// Where a request that needs the leader should go instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::application) struct LeaderRedirect {
    /// The current leader, or `None` while no leader is known, for example during an election.
    pub(in crate::application) leader: Option<LeaderLocation>,
}

/// The cluster leader and the endpoints it advertises to clients.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::application) struct LeaderLocation {
    pub(in crate::application) node: ClusterNodeName,
    /// Absent while discovery has not established the leader's session endpoint.
    pub(in crate::application) grpc_url: Option<NodeServiceUrl>,
    /// Absent while discovery has not established the leader's console endpoint.
    pub(in crate::application) web_console_url: Option<NodeServiceUrl>,
}

/// One problem found while serving a command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::application) struct CommandDiagnostic {
    pub(in crate::application) message: String,
    /// The byte range of the command's source text the problem is in, or `None` when it has no
    /// location there.
    pub(in crate::application) span: Option<Range<usize>>,
}

impl CommandDiagnostic {
    /// A diagnostic that points at no part of the source.
    pub(in crate::application) fn unlocated(message: String) -> Self {
        Self {
            message,
            span: None,
        }
    }
}

/// Whether a command's result was produced by this request or recovered from the durable record
/// of an earlier request with the same execution reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::application) enum CommandOrigin {
    Executed,
    Recovered,
}

/// A command's result and where it came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::application) struct CommandResponse {
    pub(in crate::application) result: CommandResult,
    pub(in crate::application) origin: CommandOrigin,
}

impl CommandResponse {
    /// A result this request produced.
    pub(in crate::application) fn executed(result: CommandResult) -> Self {
        Self {
            result,
            origin: CommandOrigin::Executed,
        }
    }
}

/// The outcome of one session command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::application) struct CommandResult {
    pub(in crate::application) disposition: CommandDisposition,
    pub(in crate::application) message: String,
    pub(in crate::application) diagnostics: Vec<CommandDiagnostic>,
    /// The outcome of each statement of a multi-statement command, in written order. Empty for a
    /// single statement.
    pub(in crate::application) statements: Vec<CommandResult>,
    /// The transaction this session was bound to while serving the command.
    pub(in crate::application) transaction: Option<TransactionStatus>,
    /// The operation the command accepted into the transaction, with the preview it made current.
    pub(in crate::application) transaction_admission: Option<TransactionOperationAdmission>,
    /// The read of a command that inspected a transaction. It may name a transaction other than
    /// `transaction`, which keeps describing this session's own binding.
    pub(in crate::application) inspection: Option<Box<TransactionInspection>>,
}

impl CommandResult {
    /// A result with `disposition` and `message` that reports nothing else.
    pub(in crate::application) fn new(disposition: CommandDisposition, message: String) -> Self {
        Self {
            disposition,
            message,
            diagnostics: Vec::new(),
            statements: Vec::new(),
            transaction: None,
            transaction_admission: None,
            inspection: None,
        }
    }

    /// Whether the command completed, whatever it found already present.
    pub(in crate::application) fn succeeded(&self) -> bool {
        matches!(self.disposition, CommandDisposition::Completed { .. })
    }

    /// Whether the command completed by finding what it creates already present.
    pub(in crate::application) fn found_existing(&self) -> bool {
        matches!(
            self.disposition,
            CommandDisposition::Completed {
                already_existed: true
            }
        )
    }

    /// Whether the command needs the leader and was not admitted here.
    pub(in crate::application) fn is_not_leader(&self) -> bool {
        matches!(self.disposition, CommandDisposition::NotLeader(_))
    }

    /// Replaces the disposition and message with a definitive failure, keeping what else the
    /// result reports.
    pub(in crate::application) fn fail(&mut self, message: String) {
        self.disposition = CommandDisposition::Failed;
        self.message = message;
    }
}

impl CommandDiagnostic {
    /// The span as a durable record keeps it. Every source a session executes arrived in a frame
    /// the session limits hold below four gibibytes, so a span beyond that has no location a
    /// record, or a client, could name, and is kept without one.
    fn durable_span(&self) -> Option<DiagnosticSpan> {
        let span = self.span.as_ref()?;
        let start = u32::try_from(span.start).ok()?;
        let end = u32::try_from(span.end).ok()?;
        Some(DiagnosticSpan { start, end })
    }

    fn from_durable(message: &str, span: Option<DiagnosticSpan>) -> Self {
        let span = span.map(|span| span.start.arch_into()..span.end.arch_into());
        Self {
            message: message.to_string(),
            span,
        }
    }
}

impl From<&CommandDiagnostic> for TransactionDiagnostic {
    fn from(diagnostic: &CommandDiagnostic) -> Self {
        Self {
            message: diagnostic.message.clone(),
            span: diagnostic.durable_span(),
        }
    }
}

impl From<&TransactionDiagnostic> for CommandDiagnostic {
    fn from(diagnostic: &TransactionDiagnostic) -> Self {
        Self::from_durable(&diagnostic.message, diagnostic.span)
    }
}

impl From<&CommandDiagnostic> for CommandExecutionDiagnostic {
    fn from(diagnostic: &CommandDiagnostic) -> Self {
        Self {
            message: diagnostic.message.clone(),
            span: diagnostic.durable_span(),
        }
    }
}

impl From<&CommandExecutionDiagnostic> for CommandDiagnostic {
    fn from(diagnostic: &CommandExecutionDiagnostic) -> Self {
        Self::from_durable(&diagnostic.message, diagnostic.span)
    }
}
