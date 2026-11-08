//! Deterministic validation of a verified decision against committed typed inputs.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Service/binding, revision, timing and acknowledged-transition acceptance.
//! - **Depends on.** Verified documents, condition declarations and caller-supplied state.
//! - **Must not know.** Consensus mutation, graphs, networking or the physical clock.

use std::collections::BTreeSet;

use error_stack::{Report, ResultExt as _};
use thiserror::Error;
use url::Url;
use uuid::Uuid;

use crate::{
    conditions::{
        ActivationKind, CLUSTER_DOMAINS, CLUSTER_NODES, Catalog, ConditionShape, FREE_POLICY, Scope,
    },
    document::DocumentLimits,
    documents::{Activation, Decision, DecisionStatus, Transition},
    keys::KeyId,
    trust::{DecisionVerification, TrustState, VerifiedDecision},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecisionOperation<'a> {
    Apply,
    CompleteClusterKeyRotation { pending_key: &'a KeyId },
}

pub struct DecisionContext<'a> {
    pub service: &'a Url,
    pub binding: &'a Activation,
    pub operation: DecisionOperation<'a>,
    pub request_id: Uuid,
    pub committed: Option<&'a Decision>,
    pub known_conditions_revisions: &'a BTreeSet<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BindingField {
    Activation,
    Cluster,
    Kind,
    Key,
    Generation,
    Expiry,
    Creation,
    Request,
    License,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RevisionField {
    Decision,
    Conditions,
    IssuedAt,
    ReusedDecision,
    ReusedConditions,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimingField {
    RefreshOrder,
    ValidUntil,
    Term,
    Ephemeral,
    Creation,
}

#[derive(Debug, Error)]
pub enum DecisionRejection {
    #[error("the decision envelope or signing authorization is invalid")]
    Envelope,
    #[error("decision protocol {received} is unsupported")]
    Protocol { received: u8 },
    #[error("the decision names a different licensing service")]
    Service,
    #[error("the decision does not match its committed binding: {field:?}")]
    Binding { field: BindingField },
    #[error("the decision violates monotone revision identity: {field:?}")]
    Revision { field: RevisionField },
    #[error("the decision timing is invalid: {field:?}")]
    Timing { field: TimingField },
    #[error("the decision conditions, status or acknowledged transitions are invalid")]
    Content,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecisionDisposition {
    Apply { unknown_conditions: usize },
    AlreadyApplied,
}

pub struct ValidatedDecision {
    verified: VerifiedDecision,
    disposition: DecisionDisposition,
}

impl ValidatedDecision {
    pub fn verified(&self) -> &VerifiedDecision {
        &self.verified
    }
    pub const fn disposition(&self) -> DecisionDisposition {
        self.disposition
    }
}

impl TrustState {
    pub fn validate_decision(
        &self,
        compact: &str,
        verification: DecisionVerification,
        context: DecisionContext<'_>,
        limits: DocumentLimits,
    ) -> Result<ValidatedDecision, Report<DecisionRejection>> {
        let verified = self
            .verify_decision(compact, verification, limits)
            .change_context(DecisionRejection::Envelope)?;
        let disposition = verified.validate(context)?;
        Ok(ValidatedDecision {
            verified,
            disposition,
        })
    }
}

impl VerifiedDecision {
    pub fn validate(
        &self,
        context: DecisionContext<'_>,
    ) -> Result<DecisionDisposition, Report<DecisionRejection>> {
        let decision = self.decision();
        if decision.protocol != 1 {
            return Err(Report::new(DecisionRejection::Protocol {
                received: decision.protocol,
            }));
        }
        if &decision.service != context.service {
            return Err(Report::new(DecisionRejection::Service));
        }
        decision.validate_binding(&context)?;
        decision.validate_timing()?;
        let stopped = decision.status == DecisionStatus::Expired;
        let unknown_conditions = Catalog::validate_parts(
            &decision.conditions.usage,
            &decision.conditions.operating,
            true,
            true,
            stopped,
        )
        .change_context(DecisionRejection::Content)?;
        decision.validate_content(&context)?;
        if let Some(previous) = context.committed {
            if decision.decision_revision < previous.decision_revision {
                return Err(Report::new(DecisionRejection::Revision {
                    field: RevisionField::Decision,
                }));
            }
            if decision.conditions.conditions_revision < previous.conditions.conditions_revision {
                return Err(Report::new(DecisionRejection::Revision {
                    field: RevisionField::Conditions,
                }));
            }
            if decision.conditions.conditions_revision == previous.conditions.conditions_revision
                && (decision.conditions.usage != previous.conditions.usage
                    || decision.conditions.operating != previous.conditions.operating)
            {
                return Err(Report::new(DecisionRejection::Revision {
                    field: RevisionField::ReusedConditions,
                }));
            }
            if decision.issued_at < previous.issued_at {
                return Err(Report::new(DecisionRejection::Revision {
                    field: RevisionField::IssuedAt,
                }));
            }
            if decision.decision_revision == previous.decision_revision {
                if decision != previous {
                    return Err(Report::new(DecisionRejection::Revision {
                        field: RevisionField::ReusedDecision,
                    }));
                }
                return Ok(DecisionDisposition::AlreadyApplied);
            }
        }
        Ok(DecisionDisposition::Apply { unknown_conditions })
    }
}

impl Decision {
    fn validate_binding(
        &self,
        context: &DecisionContext<'_>,
    ) -> Result<(), Report<DecisionRejection>> {
        let expected = context.binding;
        let actual = &self.activation;
        let fields = [
            (
                actual.activation_id == expected.activation_id,
                BindingField::Activation,
            ),
            (
                actual.cluster_uid == expected.cluster_uid,
                BindingField::Cluster,
            ),
            (actual.kind == expected.kind, BindingField::Kind),
            (
                actual.created_at == expected.created_at,
                BindingField::Creation,
            ),
            (
                actual.ephemeral_expires_at == expected.ephemeral_expires_at,
                BindingField::Expiry,
            ),
            (
                self.in_reply_to == context.request_id,
                BindingField::Request,
            ),
        ];
        for (matches, field) in fields {
            if !matches {
                return Err(Report::new(DecisionRejection::Binding { field }));
            }
        }
        let (key, generation) = match context.operation {
            DecisionOperation::Apply => (&expected.cluster_key_kid, Some(expected.generation)),
            DecisionOperation::CompleteClusterKeyRotation { pending_key } => {
                (pending_key, expected.generation.checked_add(1))
            }
        };
        if &actual.cluster_key_kid != key {
            return Err(Report::new(DecisionRejection::Binding {
                field: BindingField::Key,
            }));
        }
        if Some(actual.generation) != generation {
            return Err(Report::new(DecisionRejection::Binding {
                field: BindingField::Generation,
            }));
        }
        if let Some(previous) = context.committed
            && (self.license.license_id != previous.license.license_id
                || self.license.organization_id != previous.license.organization_id)
        {
            return Err(Report::new(DecisionRejection::Binding {
                field: BindingField::License,
            }));
        }
        Ok(())
    }

    fn validate_timing(&self) -> Result<(), Report<DecisionRejection>> {
        if self.issued_at > self.timing.refresh_after
            || self.timing.refresh_after > self.timing.overdue_at
        {
            return Err(Report::new(DecisionRejection::Timing {
                field: TimingField::RefreshOrder,
            }));
        }
        if let Some(until) = self.timing.valid_until
            && until < self.timing.overdue_at
        {
            return Err(Report::new(DecisionRejection::Timing {
                field: TimingField::ValidUntil,
            }));
        }
        if self.license.term_starts_at > self.license.term_ends_at
            || self.timing.term_ends_at != self.license.term_ends_at
        {
            return Err(Report::new(DecisionRejection::Timing {
                field: TimingField::Term,
            }));
        }
        if self.activation.created_at > self.issued_at {
            return Err(Report::new(DecisionRejection::Timing {
                field: TimingField::Creation,
            }));
        }
        let expiry = self.activation.ephemeral_expires_at;
        if self.timing.ephemeral_expires_at != expiry {
            return Err(Report::new(DecisionRejection::Timing {
                field: TimingField::Ephemeral,
            }));
        }
        match (self.activation.kind, expiry) {
            (ActivationKind::Persistent, None) => {}
            (ActivationKind::Ephemeral, Some(expiry)) if expiry >= self.activation.created_at => {}
            _ => {
                return Err(Report::new(DecisionRejection::Timing {
                    field: TimingField::Ephemeral,
                }));
            }
        }
        Ok(())
    }

    fn validate_content(
        &self,
        context: &DecisionContext<'_>,
    ) -> Result<(), Report<DecisionRejection>> {
        self.validate_status()?;
        let mut identities = BTreeSet::new();
        for transition in &self.transitions {
            if !identities.insert(transition.transition_id) {
                return Err(Report::new(DecisionRejection::Content));
            }
            transition.validate(
                self.conditions.conditions_revision,
                context.known_conditions_revisions,
            )?;
        }
        Ok(())
    }

    fn validate_status(&self) -> Result<(), Report<DecisionRejection>> {
        if self.status == DecisionStatus::Active
            && (self.issued_at < self.license.term_starts_at
                || self.issued_at >= self.license.term_ends_at)
        {
            return Err(Report::new(DecisionRejection::Content));
        }
        if self.status == DecisionStatus::TermEnded && self.issued_at < self.license.term_ends_at {
            return Err(Report::new(DecisionRejection::Content));
        }
        if self.status == DecisionStatus::Expired {
            match self.activation.ephemeral_expires_at {
                Some(expiry) if self.issued_at >= expiry => {}
                _ => return Err(Report::new(DecisionRejection::Content)),
            }
        } else if self.status != DecisionStatus::Active {
            let free = FREE_POLICY.usage();
            for name in [CLUSTER_NODES, CLUSTER_DOMAINS] {
                let expected = free
                    .quota(name)
                    .change_context(DecisionRejection::Content)?;
                if self
                    .conditions
                    .usage
                    .quota(name)
                    .change_context(DecisionRejection::Content)?
                    != expected
                {
                    return Err(Report::new(DecisionRejection::Content));
                }
            }
        }
        if let Some(expiry) = self.activation.ephemeral_expires_at
            && self.issued_at >= expiry
            && self.status != DecisionStatus::Expired
        {
            return Err(Report::new(DecisionRejection::Content));
        }
        Ok(())
    }
}

impl Transition {
    fn validate(
        &self,
        current_revision: u64,
        known_revisions: &BTreeSet<u64>,
    ) -> Result<(), Report<DecisionRejection>> {
        if self.deadline < self.started_at || self.affected.is_empty() {
            return Err(Report::new(DecisionRejection::Content));
        }
        for revision in [self.from_conditions_revision, self.to_conditions_revision] {
            if revision != current_revision && !known_revisions.contains(&revision) {
                return Err(Report::new(DecisionRejection::Content));
            }
        }
        let mut conditions = BTreeSet::new();
        for affected in &self.affected {
            let Some(definition) = Catalog::definition(&affected.condition) else {
                return Err(Report::new(DecisionRejection::Content));
            };
            if !matches!(
                definition.shape,
                ConditionShape::Quota {
                    scope: Scope::Cluster,
                    affects_existing: true
                }
            ) || !conditions.insert(&affected.condition)
                || !affected.target.is_reduction_from(affected.previous)
                || affected.target.admits(affected.usage)
            {
                return Err(Report::new(DecisionRejection::Content));
            }
        }
        Ok(())
    }
}
