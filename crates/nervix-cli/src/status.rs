//! How a run of the CLI ends, and the exit status that reports it.
//!
//! Layer: edges.
//!
//! - **Owns.** The CLI's exit statuses: what each says about the statements a run sent, which one
//!   each disposition of an outcome calls for, and how the endings of several statements combine.
//! - **Depends on.** The dispositions of the client core's outcomes.
//! - **Must not know.** How a statement is sent, printed or recovered.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::process::{ExitCode, Termination};

use nervix_client_core::{CommandDisposition, StatementDisposition};

/// How a statement, or a whole run of the CLI, ended. Each variant has an exit status of its own,
/// and a later variant is worse than an earlier one, so the worst ending of several is their
/// maximum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum RunEnd {
    /// Everything completed. Exit status `0`.
    Completed,
    /// A statement failed, was refused, or could not be sent, and changed nothing that is not
    /// known. Exit status `1`.
    Failed,
    /// Ctrl-C cancelled a command before the server admitted it, so it changed nothing. Exit
    /// status `130`, the status of a process its interrupt ended.
    Cancelled,
    /// An outcome did not arrive: a command or an upload may have taken effect, or a completed
    /// backup's archive was not downloaded. The CLI printed the identity that recovers it. Exit
    /// status `3`.
    Uncertain,
}

impl RunEnd {
    /// How a command whose outcome has `disposition` ended.
    pub(super) fn of_command(disposition: &CommandDisposition) -> Self {
        match disposition {
            CommandDisposition::Completed { .. } => Self::Completed,
            CommandDisposition::OutcomeUnknown(_) => Self::Uncertain,
            CommandDisposition::Failed
            | CommandDisposition::NotLeader(_)
            | CommandDisposition::TransactionDetached { .. }
            | CommandDisposition::TransactionTakenOver { .. }
            | CommandDisposition::ExecutionReferenceConflict(_)
            | CommandDisposition::ExecutionReferenceExpired
            | CommandDisposition::PreviewStale { .. } => Self::Failed,
        }
    }

    /// How one statement of a command, whose outcome has `disposition`, ended.
    pub(super) fn of_statement(disposition: &StatementDisposition) -> Self {
        match disposition {
            StatementDisposition::Completed { .. } => Self::Completed,
            StatementDisposition::Failed | StatementDisposition::NotLeader(_) => Self::Failed,
        }
    }
}

impl Termination for RunEnd {
    fn report(self) -> ExitCode {
        match self {
            Self::Completed => ExitCode::SUCCESS,
            Self::Failed => ExitCode::FAILURE,
            Self::Uncertain => ExitCode::from(3),
            Self::Cancelled => ExitCode::from(130),
        }
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use nervix_client_core::{ExecutionReferenceConflict, LeaderRedirect, UnknownOutcomeCause};
    use nervix_models::{ImpactPlanningBasis, TransactionPosition, TransactionPreviewIdentity};

    use super::*;

    fn preview(position: usize) -> TransactionPreviewIdentity {
        TransactionPreviewIdentity {
            transaction_id: "tx".to_string(),
            position: TransactionPosition::new(position),
            planning_basis: ImpactPlanningBasis::new([7; 32]),
        }
    }

    #[test]
    fn every_disposition_takes_the_status_of_what_it_says_about_the_command() {
        assert_eq!(
            RunEnd::of_command(&CommandDisposition::Completed {
                already_existed: true
            }),
            RunEnd::Completed
        );
        assert_eq!(
            RunEnd::of_command(&CommandDisposition::OutcomeUnknown(
                UnknownOutcomeCause::StillApplying
            )),
            RunEnd::Uncertain
        );
        for failed in [
            CommandDisposition::Failed,
            CommandDisposition::NotLeader(LeaderRedirect { leader: None }),
            CommandDisposition::TransactionDetached {
                transaction_id: "tx".to_string(),
            },
            CommandDisposition::TransactionTakenOver {
                transaction_id: "tx".to_string(),
            },
            CommandDisposition::ExecutionReferenceConflict(ExecutionReferenceConflict::Content),
            CommandDisposition::ExecutionReferenceExpired,
            CommandDisposition::PreviewStale {
                expected: preview(1),
                current: preview(2),
            },
        ] {
            assert_eq!(RunEnd::of_command(&failed), RunEnd::Failed, "{failed:?}");
        }
        assert_eq!(
            RunEnd::of_statement(&StatementDisposition::Completed {
                already_existed: false
            }),
            RunEnd::Completed
        );
        assert_eq!(
            RunEnd::of_statement(&StatementDisposition::Failed),
            RunEnd::Failed
        );
    }

    #[test]
    fn several_endings_combine_into_the_worst_of_them() {
        assert_eq!(RunEnd::Completed.max(RunEnd::Failed), RunEnd::Failed);
        assert_eq!(RunEnd::Failed.max(RunEnd::Uncertain), RunEnd::Uncertain);
        assert_eq!(RunEnd::Uncertain.max(RunEnd::Completed), RunEnd::Uncertain);
    }

    #[test]
    fn each_ending_exits_with_its_documented_status() {
        assert_eq!(RunEnd::Completed.report(), ExitCode::SUCCESS);
        assert_eq!(RunEnd::Failed.report(), ExitCode::FAILURE);
        assert_eq!(RunEnd::Uncertain.report(), ExitCode::from(3));
        assert_eq!(RunEnd::Cancelled.report(), ExitCode::from(130));
    }
}
