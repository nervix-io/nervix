//! A test harness's refusal of a consensus write, made the way a leader whose lease
//! lapsed refuses a write.
//!
//! Layer: engines and infrastructure; arming is exposed only to test harnesses.
//! - **Owns.** One armed refusal of a command execution write, a license activation or a domain
//!   stop's ending proposal, and whether a proposal met it.
//! - **Depends on.** The vocabulary's execution references and domain names, and asynchronous
//!   synchronization.
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
pub(crate) use enabled::RefusalTarget;
#[cfg(any(test, feature = "testing"))]
pub use enabled::{CommandWrite, LeaseRefusal};

#[cfg(any(test, feature = "testing"))]
mod enabled {
    use nervix_models::{CommandExecutionReference, DomainName};
    use nervix_primitives::sync::{Arc, Mutex};

    use super::*;

    /// The proposal of a command execution that a refusal is armed for.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum CommandWrite {
        /// The proposal that admits the execution.
        Admission,
        /// The proposal that records the execution's outcome.
        Finalization,
        /// The proposal that releases one domain's mutation lease after its cut.
        DomainMutationRelease,
        /// The conditional proposal that releases an orphaned model-alteration pause.
        PauseRecovery,
    }

    #[derive(Debug, PartialEq, Eq)]
    pub(crate) enum RefusalTarget {
        Command {
            reference: CommandExecutionReference,
            write: CommandWrite,
        },
        LicenseActivation,
        /// The proposal that ends one domain's stop.
        DomainStopEnding { domain: DomainName },
    }

    impl RefusalTarget {
        fn for_proposal(command: &ConsensusCommand) -> Option<Self> {
            let (reference, write) = match command {
                ConsensusCommand::AdmitCommandExecution { execution, .. } => {
                    (&execution.reference, CommandWrite::Admission)
                }
                ConsensusCommand::FinishCommandExecution { reference, .. } => {
                    (reference, CommandWrite::Finalization)
                }
                ConsensusCommand::ReleaseCommandDomainMutation { reference, .. } => {
                    (reference, CommandWrite::DomainMutationRelease)
                }
                ConsensusCommand::RecoverOrphanedModelPause { mutation, .. } => {
                    let crate::DomainMutationOwner::Command(reference) = mutation.owner() else {
                        return None;
                    };
                    (reference, CommandWrite::PauseRecovery)
                }
                ConsensusCommand::ActivateLicense { .. } => return Some(Self::LicenseActivation),
                ConsensusCommand::EndDomainStop { domain_id, .. } => {
                    return Some(Self::DomainStopEnding {
                        domain: domain_id.clone(),
                    });
                }
                _ => return None,
            };
            Some(Self::Command {
                reference: reference.clone(),
                write,
            })
        }
    }

    /// The refusal a test harness armed for one proposal on this node.
    #[derive(Clone, Debug, Default)]
    pub struct LeaseRefusal {
        armed: Arc<Mutex<Option<ArmedRefusal>>>,
    }

    #[derive(Debug)]
    struct ArmedRefusal {
        target: RefusalTarget,
        delivered: bool,
    }

    impl LeaseRefusal {
        /// Arms the next proposal matching `target`, replacing any refusal armed before.
        pub(crate) async fn arm(&self, target: RefusalTarget) {
            let mut armed = self.armed.lock().await;
            *armed = Some(ArmedRefusal {
                target,
                delivered: false,
            });
        }

        /// Whether a proposal matching `target` met the refusal armed for it.
        pub(crate) async fn delivered(&self, target: &RefusalTarget) -> bool {
            let armed = self.armed.lock().await;
            let Some(armed) = armed.as_ref() else {
                return false;
            };
            armed.target == *target && armed.delivered
        }

        /// Whether `command` is the proposal the armed refusal is for and has not met yet. It meets
        /// the refusal once; every later proposal goes ahead.
        pub(crate) async fn take(&self, command: &ConsensusCommand) -> bool {
            let Some(target) = RefusalTarget::for_proposal(command) else {
                return false;
            };
            let mut armed = self.armed.lock().await;
            let Some(armed) = armed.as_mut() else {
                return false;
            };
            if armed.delivered || armed.target != target {
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
    use nervix_models::{CommandExecutionReference, DomainName, StopEnding, Timestamp, UserName};

    use super::{CommandWrite, LeaseRefusal, RefusalTarget};
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

        let finalization_refusal = || RefusalTarget::Command {
            reference: armed.clone(),
            write: CommandWrite::Finalization,
        };
        refusal.arm(finalization_refusal()).await;
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
        assert!(!refusal.delivered(&finalization_refusal()).await);

        assert!(refusal.take(&finalization(armed.clone())).await);
        assert!(
            !refusal.take(&finalization(armed.clone())).await,
            "the armed proposal meets the refusal once"
        );
        assert!(refusal.delivered(&finalization_refusal()).await);
        let admission = RefusalTarget::Command {
            reference: armed,
            write: CommandWrite::Admission,
        };
        assert!(!refusal.delivered(&admission).await);
    }

    #[nervix_primitives::test]
    async fn a_domain_mutation_release_refusal_leaves_the_next_attempt_available() {
        let refusal = LeaseRefusal::default();
        let armed = reference(1);
        let release_refusal = || RefusalTarget::Command {
            reference: armed.clone(),
            write: CommandWrite::DomainMutationRelease,
        };
        refusal.arm(release_refusal()).await;
        let release = ConsensusCommand::ReleaseCommandDomainMutation {
            reference: armed.clone(),
            owner: owner(),
            request_digest: [1; 32],
            domain: DomainName::try_from("tenant").assured("the test domain is valid"),
        };
        assert!(refusal.take(&release).await);
        assert!(refusal.delivered(&release_refusal()).await);
        assert!(!refusal.take(&release).await);
        assert!(!refusal.take(&finalization(armed)).await);
    }

    #[nervix_primitives::test]
    async fn a_domain_stop_ending_refusal_meets_that_domains_next_ending_once() {
        let refusal = LeaseRefusal::default();
        let domain = |name: &str| DomainName::try_from(name).assured("the test domain is valid");
        let ending = |name: &str| ConsensusCommand::EndDomainStop {
            domain_id: domain(name),
            began_at_revision: 7,
            ending: StopEnding::Completed,
        };
        let ending_refusal = || RefusalTarget::DomainStopEnding {
            domain: domain("tenant"),
        };
        refusal.arm(ending_refusal()).await;
        assert!(
            !refusal.take(&ending("other")).await,
            "another domain's ending goes ahead"
        );
        assert!(!refusal.delivered(&ending_refusal()).await);
        assert!(refusal.take(&ending("tenant")).await);
        assert!(refusal.delivered(&ending_refusal()).await);
        assert!(
            !refusal.take(&ending("tenant")).await,
            "the armed ending meets the refusal once, so the owner's next attempt goes ahead"
        );
    }
}
