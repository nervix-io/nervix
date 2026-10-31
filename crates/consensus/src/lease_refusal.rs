//! A test harness's refusal of a command execution's write, made the way a leader whose lease
//! lapsed refuses a write.
//!
//! Layer: engines and infrastructure; arming is exposed only to test harnesses.
//! - **Owns.** One armed refusal of a command execution's admission or finalization proposal, and
//!   whether a proposal met it.
//! - **Depends on.** The vocabulary's execution references and asynchronous synchronization.
//! - **Must not know.** How the refusal is classified, answered or recovered.

use crate::ConsensusCommand;

#[cfg(not(any(test, feature = "testing")))]
#[derive(Clone, Default)]
pub(crate) struct LeaseRefusal(());

#[cfg(not(any(test, feature = "testing")))]
impl LeaseRefusal {
    pub(crate) async fn take(&self, _: &ConsensusCommand) -> bool {
        false
    }
}

#[cfg(any(test, feature = "testing"))]
pub use enabled::{CommandWrite, LeaseRefusal};

#[cfg(any(test, feature = "testing"))]
mod enabled {
    use nervix_models::CommandExecutionReference;
    use nervix_primitives::sync::{Arc, Mutex};

    use super::*;

    /// The proposal of a command execution that a refusal is armed for.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum CommandWrite {
        /// The proposal that admits the execution.
        Admission,
        /// The proposal that records the execution's outcome.
        Finalization,
    }

    /// The refusal a test harness armed for one command execution's proposal on this node.
    #[derive(Clone, Debug, Default)]
    pub struct LeaseRefusal {
        armed: Arc<Mutex<Option<ArmedRefusal>>>,
    }

    #[derive(Debug)]
    struct ArmedRefusal {
        reference: CommandExecutionReference,
        write: CommandWrite,
        delivered: bool,
    }

    impl LeaseRefusal {
        /// Arms the refusal of `reference`'s next `write`, replacing any refusal armed before.
        pub(crate) async fn arm(&self, reference: CommandExecutionReference, write: CommandWrite) {
            let mut armed = self.armed.lock().await;
            *armed = Some(ArmedRefusal {
                reference,
                write,
                delivered: false,
            });
        }

        /// Whether a proposal of `reference`'s `write` met the refusal armed for it.
        pub(crate) async fn delivered(
            &self,
            reference: &CommandExecutionReference,
            write: CommandWrite,
        ) -> bool {
            let armed = self.armed.lock().await;
            let Some(armed) = armed.as_ref() else {
                return false;
            };
            armed.reference == *reference && armed.write == write && armed.delivered
        }

        /// Whether `command` is the proposal the armed refusal is for and has not met yet. It meets
        /// the refusal once; every later proposal goes ahead.
        pub(crate) async fn take(&self, command: &ConsensusCommand) -> bool {
            let (reference, write) = match command {
                ConsensusCommand::AdmitCommandExecution { execution, .. } => {
                    (&execution.reference, CommandWrite::Admission)
                }
                ConsensusCommand::FinishCommandExecution { reference, .. } => {
                    (reference, CommandWrite::Finalization)
                }
                _ => return false,
            };
            let mut armed = self.armed.lock().await;
            let Some(armed) = armed.as_mut() else {
                return false;
            };
            if armed.delivered || armed.reference != *reference || armed.write != write {
                return false;
            }
            armed.delivered = true;
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::{CommandExecutionReference, DomainName, Timestamp, UserName};

    use super::{CommandWrite, LeaseRefusal};
    use crate::{CommandExecutionDisposition, CommandExecutionResult, ConsensusCommand};

    fn reference(index: u64) -> CommandExecutionReference {
        CommandExecutionReference::parse(format!("018bcfe5-6800-7000-8000-{index:012x}"))
            .assured("the test reference is a UUIDv7")
    }

    fn owner() -> UserName {
        UserName::parse("operator").assured("the test user name follows the name rule")
    }

    fn finalization(reference: CommandExecutionReference) -> ConsensusCommand {
        ConsensusCommand::FinishCommandExecution {
            reference,
            owner: owner(),
            request_digest: [1; 32],
            at: Timestamp::from_unix_nanos(1),
            result: Box::new(CommandExecutionResult {
                disposition: CommandExecutionDisposition::Completed {
                    already_existed: false,
                },
                message: "finished".to_string(),
                diagnostics: Vec::new(),
                statements: Vec::new(),
                transaction: None,
                transaction_admission: None,
                backup: None,
                restore: None,
            }),
        }
    }

    #[nervix_primitives::test]
    async fn an_armed_refusal_meets_only_its_own_proposal_and_only_once() {
        let refusal = LeaseRefusal::default();
        let armed = reference(1);
        assert!(
            !refusal.take(&finalization(armed.clone())).await,
            "a proposal goes ahead while nothing is armed"
        );

        refusal.arm(armed.clone(), CommandWrite::Finalization).await;
        let release = ConsensusCommand::ReleaseCommandDomainMutation {
            reference: armed.clone(),
            owner: owner(),
            request_digest: [1; 32],
            domain: DomainName::try_from("tenant").assured("the test domain is valid"),
        };
        assert!(
            !refusal.take(&release).await,
            "another proposal of the armed execution goes ahead"
        );
        assert!(
            !refusal.take(&finalization(reference(2))).await,
            "the finalization of another execution goes ahead"
        );
        assert!(!refusal.delivered(&armed, CommandWrite::Finalization).await);

        assert!(refusal.take(&finalization(armed.clone())).await);
        assert!(
            !refusal.take(&finalization(armed.clone())).await,
            "the armed proposal meets the refusal once"
        );
        assert!(refusal.delivered(&armed, CommandWrite::Finalization).await);
        assert!(!refusal.delivered(&armed, CommandWrite::Admission).await);
    }
}
