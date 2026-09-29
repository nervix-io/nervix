//! Running a restore's steps in order, from the first one not recorded.
//!
//! Layer: control plane.
//!
//! - **Owns.** The order a restore applies its steps in, skipping every step an earlier attempt
//!   recorded, stopping at the first failure, and telling a failure that leaves the restore for
//!   the next leader from one that ends it.
//! - **Depends on.** Where a step takes effect, through [`RestoreSteps`].
//! - **Must not know.** What a step changes, or how it is recorded.
//!
//! A step's record is what makes it happen once: an attempt skips the steps recorded when it
//! began, and a step another attempt recorded since is recorded again without taking effect,
//! because recording is where the effect is decided. So attempts that race, or a new leader that
//! resumes after an old one's proposal still committed, never apply one step's effect twice.

use std::{collections::BTreeSet, future::Future};

use nervix_models::{RestoreStep, RestoreStepOutcome, RestoreStepReport};

use crate::application::command_result::CommandResult;

/// Why a step of a restore did not apply.
pub(in crate::application) enum StepFailure {
    /// Leadership moved. The restore stays applying for the next leader, and this is the redirect
    /// to answer with.
    LeadershipLost(Box<CommandResult>),
    /// The step failed definitively, for this reason. The steps before it stay applied.
    Failed(String),
}

/// Where a restore's steps take effect.
pub(in crate::application) trait RestoreSteps {
    /// Applies `step` and records it; a step already recorded is recorded again without effect.
    fn apply(&self, step: &RestoreStep) -> impl Future<Output = Result<(), StepFailure>> + Send;
}

/// How running a restore's steps ended.
pub(in crate::application) enum RestoreRunEnd {
    /// Every step is applied.
    Completed,
    /// Leadership moved; the restore stays applying.
    LeadershipLost(Box<CommandResult>),
    /// A step failed definitively, for this reason.
    Failed { step: RestoreStep, reason: String },
}

/// What became of every step, and how the run ended.
pub(in crate::application) struct RestoreRun {
    pub(in crate::application) steps: Vec<RestoreStepReport>,
    pub(in crate::application) end: RestoreRunEnd,
}

/// Applies every step of `steps` not in `recorded`, in order, through `effects`.
pub(in crate::application) async fn run_restore_steps<S: RestoreSteps>(
    steps: &[RestoreStep],
    recorded: &BTreeSet<RestoreStep>,
    effects: &S,
) -> RestoreRun {
    let mut reports = Vec::with_capacity(steps.len());
    for step in steps {
        let outcome = if recorded.contains(step) {
            RestoreStepOutcome::Applied
        } else {
            RestoreStepOutcome::NotAttempted
        };
        reports.push(RestoreStepReport {
            step: step.clone(),
            outcome,
        });
    }
    for report in &mut reports {
        tokio::task::consume_budget().await;
        if report.outcome == RestoreStepOutcome::Applied {
            continue;
        }
        match effects.apply(&report.step).await {
            Ok(()) => report.outcome = RestoreStepOutcome::Applied,
            Err(StepFailure::LeadershipLost(redirect)) => {
                return RestoreRun {
                    steps: reports,
                    end: RestoreRunEnd::LeadershipLost(redirect),
                };
            }
            Err(StepFailure::Failed(reason)) => {
                report.outcome = RestoreStepOutcome::Failed;
                let step = report.step.clone();
                return RestoreRun {
                    steps: reports,
                    end: RestoreRunEnd::Failed { step, reason },
                };
            }
        }
    }
    RestoreRun {
        steps: reports,
        end: RestoreRunEnd::Completed,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use meticulous::ResultExt as _;
    use nervix_models::DomainName;
    use parking_lot::Mutex;

    use super::*;
    use crate::application::model_mutation::command_error;

    fn domain(name: &str) -> DomainName {
        DomainName::parse(name).assured("the test domain is valid")
    }

    fn steps() -> Vec<RestoreStep> {
        vec![
            RestoreStep::Users,
            RestoreStep::CreateDomain(domain("prod")),
            RestoreStep::ImportResources(domain("prod")),
            RestoreStep::ApplyModels(domain("prod")),
        ]
    }

    /// Records every step it applies, and fails the one it is told to.
    struct Recording {
        applied: Mutex<Vec<RestoreStep>>,
        failing: Option<(RestoreStep, bool)>,
        attempts: AtomicUsize,
    }

    impl RestoreSteps for Recording {
        async fn apply(&self, step: &RestoreStep) -> Result<(), StepFailure> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            if let Some((failing, leadership)) = &self.failing
                && failing == step
            {
                if *leadership {
                    return Err(StepFailure::LeadershipLost(Box::new(command_error(
                        "leadership moved".to_string(),
                    ))));
                }
                return Err(StepFailure::Failed("the step failed".to_string()));
            }
            self.applied.lock().push(step.clone());
            Ok(())
        }
    }

    fn recording(failing: Option<(RestoreStep, bool)>) -> Recording {
        Recording {
            applied: Mutex::new(Vec::new()),
            failing,
            attempts: AtomicUsize::new(0),
        }
    }

    fn outcomes(run: &RestoreRun) -> Vec<RestoreStepOutcome> {
        run.steps.iter().map(|report| report.outcome).collect()
    }

    #[tokio::test]
    async fn a_run_applies_only_the_steps_not_recorded_in_order() {
        let effects = recording(None);
        let recorded = BTreeSet::from([RestoreStep::Users]);
        let run = run_restore_steps(&steps(), &recorded, &effects).await;
        assert!(matches!(run.end, RestoreRunEnd::Completed));
        assert_eq!(*effects.applied.lock(), steps()[1..]);
        assert_eq!(outcomes(&run), [RestoreStepOutcome::Applied; 4]);
    }

    #[tokio::test]
    async fn a_failed_step_ends_the_run_and_leaves_the_rest_unattempted() {
        let failing = RestoreStep::ImportResources(domain("prod"));
        let effects = recording(Some((failing.clone(), false)));
        let run = run_restore_steps(&steps(), &BTreeSet::new(), &effects).await;
        let RestoreRunEnd::Failed { step, reason } = run.end else {
            panic!("the run fails at the failing step");
        };
        assert_eq!(step, failing);
        assert_eq!(reason, "the step failed");
        assert_eq!(
            outcomes(&RestoreRun {
                steps: run.steps,
                end: RestoreRunEnd::Completed,
            }),
            [
                RestoreStepOutcome::Applied,
                RestoreStepOutcome::Applied,
                RestoreStepOutcome::Failed,
                RestoreStepOutcome::NotAttempted,
            ]
        );
        assert_eq!(effects.attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn lost_leadership_leaves_the_restore_for_the_next_leader() {
        let effects = recording(Some((RestoreStep::CreateDomain(domain("prod")), true)));
        let run = run_restore_steps(&steps(), &BTreeSet::new(), &effects).await;
        assert!(matches!(run.end, RestoreRunEnd::LeadershipLost(_)));
        assert_eq!(*effects.applied.lock(), [RestoreStep::Users]);
    }
}
