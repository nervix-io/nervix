//! The session's typed replies, built from the results the control plane decides.
//!
//! Layer: edges.
//!
//! - **Owns.** Converting command results, transaction attachments and inspections into the
//!   outcomes the client wire contract carries.
//! - **Depends on.** The control-plane result types and the client wire contract.
//! - **Must not know.** How a reply is framed, correlated or sent.

use std::ops::Range;

use nervix_client_wire::{
    AttachDisposition, AttachOutcome, CommandDisposition as WireCommandDisposition, CommandOutcome,
    Diagnostic, ExecutionReferenceConflict, LeaderEndpoints, LeaderRedirect as WireLeaderRedirect,
    OutcomeOrigin, SourceSpan, StatementDisposition, StatementOutcome, UnknownOutcomeCause,
};
use nervix_consensus::CommandExecutionRequestConflict;
use nervix_models::CommandExecutionReference;

use crate::application::{
    command_result::{
        CommandDiagnostic, CommandDisposition, CommandOrigin, CommandResponse, CommandResult,
        LeaderLocation, LeaderRedirect, OutcomeUnknownCause,
    },
    transaction::TransactionAttachment,
};

/// The outcome a command request is answered with.
pub(super) fn command_outcome(
    execution_reference: CommandExecutionReference,
    response: CommandResponse,
) -> CommandOutcome {
    let CommandResponse { result, origin } = response;
    let CommandResult {
        disposition,
        message,
        diagnostics,
        statements,
        transaction,
        transaction_admission,
        inspection,
        wasm_state,
        resource,
    } = result;
    let origin = match origin {
        CommandOrigin::Executed => OutcomeOrigin::Executed,
        CommandOrigin::Recovered => OutcomeOrigin::Recovered,
    };
    CommandOutcome {
        execution_reference,
        origin,
        disposition: command_disposition(disposition),
        message,
        diagnostics: wire_diagnostics(diagnostics),
        statements: statements.into_iter().map(statement_outcome).collect(),
        transaction,
        transaction_admission,
        inspection,
        wasm_state,
        resource,
    }
}

fn command_disposition(disposition: CommandDisposition) -> WireCommandDisposition {
    match disposition {
        CommandDisposition::Completed { already_existed } => {
            WireCommandDisposition::Completed { already_existed }
        }
        CommandDisposition::Failed => WireCommandDisposition::Failed,
        CommandDisposition::NotLeader(redirect) => {
            WireCommandDisposition::NotLeader(leader_redirect(redirect))
        }
        CommandDisposition::TransactionDetached { transaction_id } => {
            WireCommandDisposition::TransactionDetached { transaction_id }
        }
        CommandDisposition::TransactionTakenOver { transaction_id } => {
            WireCommandDisposition::TransactionTakenOver { transaction_id }
        }
        CommandDisposition::OutcomeUnknown(cause) => {
            WireCommandDisposition::OutcomeUnknown(unknown_outcome_cause(cause))
        }
        CommandDisposition::ExecutionReferenceConflict(conflict) => {
            WireCommandDisposition::ExecutionReferenceConflict(reference_conflict(conflict))
        }
        CommandDisposition::ExecutionReferenceExpired => {
            WireCommandDisposition::ExecutionReferenceExpired
        }
        CommandDisposition::PreviewStale { expected, current } => {
            WireCommandDisposition::PreviewStale { expected, current }
        }
    }
}

/// One statement of a multi-statement command. A statement completes, fails, or is redirected;
/// any other disposition it ended with is a failure of that statement, and the command's own
/// disposition says the rest.
fn statement_outcome(result: CommandResult) -> StatementOutcome {
    let disposition = match result.disposition {
        CommandDisposition::Completed { already_existed } => {
            StatementDisposition::Completed { already_existed }
        }
        CommandDisposition::NotLeader(redirect) => {
            StatementDisposition::NotLeader(leader_redirect(redirect))
        }
        CommandDisposition::Failed
        | CommandDisposition::TransactionDetached { .. }
        | CommandDisposition::TransactionTakenOver { .. }
        | CommandDisposition::OutcomeUnknown(_)
        | CommandDisposition::ExecutionReferenceConflict(_)
        | CommandDisposition::ExecutionReferenceExpired
        | CommandDisposition::PreviewStale { .. } => StatementDisposition::Failed,
    };
    StatementOutcome {
        disposition,
        message: result.message,
        diagnostics: wire_diagnostics(result.diagnostics),
    }
}

fn unknown_outcome_cause(cause: OutcomeUnknownCause) -> UnknownOutcomeCause {
    match cause {
        OutcomeUnknownCause::LeadershipLost => UnknownOutcomeCause::LeadershipLost,
        OutcomeUnknownCause::StillApplying => UnknownOutcomeCause::StillApplying,
        OutcomeUnknownCause::NotYetAuthoritative => UnknownOutcomeCause::NotYetAuthoritative,
    }
}

fn reference_conflict(conflict: CommandExecutionRequestConflict) -> ExecutionReferenceConflict {
    match conflict {
        CommandExecutionRequestConflict::Content => ExecutionReferenceConflict::Content,
        CommandExecutionRequestConflict::Domain => ExecutionReferenceConflict::Domain,
        CommandExecutionRequestConflict::Owner => ExecutionReferenceConflict::Owner,
        CommandExecutionRequestConflict::Position => ExecutionReferenceConflict::Position,
    }
}

pub(super) fn leader_redirect(redirect: LeaderRedirect) -> WireLeaderRedirect {
    let leader = redirect.leader.map(leader_endpoints);
    WireLeaderRedirect { leader }
}

pub(super) fn leader_endpoints(location: LeaderLocation) -> LeaderEndpoints {
    let grpc_uri = location.grpc_url.map(|url| url.to_url());
    let web_console_uri = location.web_console_url.map(|url| url.to_url());
    LeaderEndpoints {
        node: location.node,
        grpc_uri,
        web_console_uri,
    }
}

pub(super) fn wire_diagnostics(diagnostics: Vec<CommandDiagnostic>) -> Vec<Diagnostic> {
    diagnostics.into_iter().map(wire_diagnostic).collect()
}

fn wire_diagnostic(diagnostic: CommandDiagnostic) -> Diagnostic {
    let span = match diagnostic.span {
        Some(span) => wire_span(span),
        None => None,
    };
    Diagnostic {
        message: diagnostic.message,
        span,
    }
}

/// The span as the wire carries it. Every source a session executes arrived in a frame the
/// session limits hold below four gibibytes, so a span beyond that, or one that runs backwards,
/// names no location in the request and is sent without one.
fn wire_span(span: Range<usize>) -> Option<SourceSpan> {
    let start = u32::try_from(span.start).ok()?;
    let end = u32::try_from(span.end).ok()?;
    SourceSpan::new(start, end).ok()
}

/// The outcome an attach request is answered with.
pub(super) fn attach_outcome(attachment: TransactionAttachment) -> AttachOutcome {
    match attachment {
        TransactionAttachment::Attached {
            transaction,
            message,
        } => AttachOutcome {
            disposition: AttachDisposition::Attached(transaction),
            message,
            diagnostics: Vec::new(),
        },
        TransactionAttachment::AlreadyFinished {
            transaction,
            message,
            diagnostics,
        } => AttachOutcome {
            disposition: AttachDisposition::AlreadyFinished(transaction),
            message,
            diagnostics: wire_diagnostics(diagnostics),
        },
        TransactionAttachment::Refused(result) => {
            let disposition = match result.disposition {
                CommandDisposition::NotLeader(redirect) => {
                    AttachDisposition::NotLeader(leader_redirect(redirect))
                }
                _ => AttachDisposition::Failed,
            };
            AttachOutcome {
                disposition,
                message: result.message,
                diagnostics: wire_diagnostics(result.diagnostics),
            }
        }
    }
}
