//! Recording the actual impact of one transaction execution step as its owner observes it.
//!
//! Layer: control plane.
//!
//! - **Owns.** Ordered actual quiescence transitions and the mutable application impact snapshot.
//! - **Depends on.** The transaction impact vocabulary.
//! - **Must not know.** Consensus persistence, runtime gate mechanics or presentation.

use meticulous::OptionExt as _;
use nervix_models::{
    ActualExecutionStepImpact, ActualQuiescence, ExecutionStepImpactReport, ImpactDiagnostic,
    ImpactDiagnosticKind, ImpactEffects, PauseRequirement, QuiesceLevel, QuiescenceOutcome,
    RebuildImpact, TransactionOperationNumber,
};
use parking_lot::Mutex;

#[derive(Clone, Copy)]
pub(in crate::application) struct QuiescenceAttempt(usize);

pub(in crate::application) struct TransactionStepImpactRecorder {
    operation: TransactionOperationNumber,
    actual: Mutex<ActualExecutionStepImpact>,
}

impl TransactionStepImpactRecorder {
    pub(in crate::application) fn new(step: &ExecutionStepImpactReport) -> Self {
        Self {
            operation: step.operations().first(),
            actual: Mutex::new(step.actual().clone()),
        }
    }

    pub(in crate::application) fn from_actual(
        operation: TransactionOperationNumber,
        actual: ActualExecutionStepImpact,
    ) -> Self {
        Self {
            operation,
            actual: Mutex::new(actual),
        }
    }

    pub(in crate::application) fn snapshot(&self) -> ActualExecutionStepImpact {
        self.actual.lock().clone()
    }

    pub(in crate::application) fn apply_to(
        &self,
        mut step: ExecutionStepImpactReport,
    ) -> ExecutionStepImpactReport {
        *step.actual_mut() = self.snapshot();
        step
    }

    pub(in crate::application) fn request(
        &self,
        requirement: PauseRequirement,
    ) -> QuiescenceAttempt {
        let mut actual = self.actual.lock();
        let index = actual.quiescence.len();
        actual.quiescence.push(ActualQuiescence {
            requirement,
            outcomes: vec![QuiescenceOutcome::Requested],
        });
        QuiescenceAttempt(index)
    }

    pub(in crate::application) fn confirm(&self, attempt: QuiescenceAttempt) {
        self.record(attempt, QuiescenceOutcome::Confirmed);
    }

    pub(in crate::application) fn fail(
        &self,
        attempt: QuiescenceAttempt,
        kind: ImpactDiagnosticKind,
        message: impl Into<String>,
    ) {
        self.record(
            attempt,
            QuiescenceOutcome::Failed {
                diagnostic: self.diagnostic(kind, message),
            },
        );
    }

    pub(in crate::application) fn uncertain(
        &self,
        attempt: QuiescenceAttempt,
        kind: ImpactDiagnosticKind,
        message: impl Into<String>,
    ) {
        self.record(
            attempt,
            QuiescenceOutcome::Uncertain {
                diagnostic: self.diagnostic(kind, message),
            },
        );
    }

    pub(in crate::application) fn release(&self, attempt: QuiescenceAttempt) {
        self.record(attempt, QuiescenceOutcome::Released);
    }

    pub(in crate::application) fn begin_application(&self, effects: ImpactEffects) {
        let mut actual = self.actual.lock();
        actual.outcome = nervix_models::ExecutionStepOutcome::Applying;
        actual.effects = effects;
    }

    pub(in crate::application) fn record_recovery_expansion(
        &self,
        requirement: PauseRequirement,
        rebuilds: impl IntoIterator<Item = RebuildImpact>,
    ) {
        let attempt = self.request(requirement);
        self.confirm(attempt);
        self.actual.lock().effects.rebuilds.extend(rebuilds);
        self.release(attempt);
    }

    pub(in crate::application) fn pending_domain_attempt(&self) -> Option<QuiescenceAttempt> {
        self.pending_attempts(QuiesceLevel::DomainPause)
            .into_iter()
            .next_back()
    }

    pub(in crate::application) fn pending_entity_attempts(&self) -> Vec<QuiescenceAttempt> {
        self.pending_attempts(QuiesceLevel::EntityPause)
    }

    fn pending_attempts(&self, level: QuiesceLevel) -> Vec<QuiescenceAttempt> {
        self.actual
            .lock()
            .quiescence
            .iter()
            .enumerate()
            .filter(|(_, engagement)| {
                if engagement.requirement.level() != level {
                    return false;
                }
                let engaged = engagement.outcomes.iter().any(|outcome| {
                    matches!(
                        outcome,
                        QuiescenceOutcome::Confirmed | QuiescenceOutcome::Uncertain { .. }
                    )
                });
                let released = engagement
                    .outcomes
                    .iter()
                    .any(|outcome| matches!(outcome, QuiescenceOutcome::Released));
                engaged && !released
            })
            .map(|(index, _)| QuiescenceAttempt(index))
            .collect()
    }

    fn record(&self, attempt: QuiescenceAttempt, outcome: QuiescenceOutcome) {
        self.actual
            .lock()
            .quiescence
            .get_mut(attempt.0)
            .verified("a quiescence attempt is issued by this recorder before it is updated")
            .outcomes
            .push(outcome);
    }

    fn diagnostic(
        &self,
        kind: ImpactDiagnosticKind,
        message: impl Into<String>,
    ) -> ImpactDiagnostic {
        ImpactDiagnostic {
            kind,
            operation: Some(self.operation),
            message: message.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::{
        ActualExecutionStepImpact, DomainName, ExecutionStepImpactReport, ImpactDiagnosticKind,
        ImpactReportCompleteness, PauseRequirement, PlannedExecutionStepImpact, QuiesceLevel,
        QuiescenceOutcome, TransactionOperationRange,
    };

    use super::TransactionStepImpactRecorder;

    fn step() -> ExecutionStepImpactReport {
        let operations = TransactionOperationRange::from_index_and_count(0, 1)
            .assured("the first test operation is addressable");
        ExecutionStepImpactReport::new(
            operations,
            PlannedExecutionStepImpact {
                completeness: ImpactReportCompleteness::Complete,
                pause: PauseRequirement::Domain {
                    domain: DomainName::parse("tenant")
                        .assured("the test domain is an identifier-shaped literal"),
                },
                effects: Default::default(),
            },
            ActualExecutionStepImpact::unattempted(),
        )
    }

    #[test]
    fn recorder_preserves_ordered_engagement_and_release_outcomes() {
        let step = step();
        let recorder = TransactionStepImpactRecorder::new(&step);
        let attempt = recorder.request(step.planned().pause.clone());
        recorder.confirm(attempt);
        recorder.fail(
            attempt,
            ImpactDiagnosticKind::Quiescence,
            "the domain drain reached its deadline",
        );
        recorder.release(attempt);

        let actual = recorder.snapshot();
        assert_eq!(actual.quiesce_level(), QuiesceLevel::DomainPause);
        assert!(matches!(
            actual.quiescence[0].outcomes.as_slice(),
            [
                QuiescenceOutcome::Requested,
                QuiescenceOutcome::Confirmed,
                QuiescenceOutcome::Failed { .. },
                QuiescenceOutcome::Released
            ]
        ));
    }

    #[test]
    fn recorder_keeps_a_pre_engagement_failure_dynamic() {
        let step = step();
        let recorder = TransactionStepImpactRecorder::new(&step);
        let attempt = recorder.request(step.planned().pause.clone());
        recorder.fail(
            attempt,
            ImpactDiagnosticKind::Quiescence,
            "the pause proposal was rejected",
        );

        assert_eq!(recorder.snapshot().quiesce_level(), QuiesceLevel::Dynamic);
    }
}
