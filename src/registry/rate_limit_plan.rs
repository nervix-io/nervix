//! The allowances an ingestor's `RATE LIMIT` clauses decide, and the charge a payload pays in each.
//!
//! Layer: decisions.
//!
//! - **Owns.** Converting each written clause once into the allowance it declares, within the
//!   bounds the allowance engine enforces, and into the charge one payload pays in it: one unit,
//!   a payload fact read directly, or the value of the clause's measure, lowered here into the
//!   program the host compiles. `CREATE` and `ALTER` refuse a clause through the same conversion
//!   the plan makes.
//! - **Depends on.** The rate-limit vocabulary, the allowance engine's quota, the payload
//!   declaration and the VM frontend.
//! - **Must not know.** How a host holds its source back, its clock, or any connector.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns the decisions boundary declared above")
)]

use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    Assignment, AssignmentTarget, Expression, FieldName, FieldScope, IngestorInput,
    IngestorPayloadField, RateLimit, RouteConstruction,
};
use nervix_rate_limit::{Quota, QuotaError};
use nervix_vm::{
    SemanticScopePolicy, lower_route_construction,
    program::{Program, SpannedNode},
};
use thiserror::Error;

/// Why a written `RATE LIMIT` clause declares no allowance the engine can keep.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "converting a written clause into its allowance is a pure refusal"
    )
)]
pub(crate) enum RateLimitClauseIssue {
    #[error("its count is not a positive whole number or byte size")]
    Count,
    #[error("its BURST is not a positive whole number or byte size")]
    Burst,
    #[error("its period is not a duration longer than 0")]
    Period,
    #[error("{0}")]
    Quota(QuotaError),
}

/// What one payload costs in one allowance.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RateLimitCharge {
    /// Every payload costs one unit.
    Payload,
    /// A payload costs the value of one of its payload facts, read without a program.
    PayloadFact(IngestorPayloadField),
    /// A payload costs what its measure's program computes from the payload, its metadata and its
    /// headers.
    Measure(LoweredMeasure),
}

impl RateLimitCharge {
    /// Whether the charge is one unit per payload, so that the allowance alone bounds how many
    /// payloads may be taken at once.
    pub(crate) fn counts_payloads(&self) -> bool {
        match self {
            Self::Payload => true,
            Self::PayloadFact(_) | Self::Measure(_) => false,
        }
    }
}

/// One clause's allowance and the charge a payload pays in it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RateLimitClausePlan {
    pub(crate) quota: Quota,
    pub(crate) charge: RateLimitCharge,
}

/// Every `RATE LIMIT` clause of one ingestor, in the order its body writes them. A payload is
/// admitted only once every clause can pay for it.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct IngestorRateLimitPlan {
    clauses: Vec<RateLimitClausePlan>,
}

impl IngestorRateLimitPlan {
    /// The allowances and charges of every clause `input` writes, or the first clause that
    /// declares none, by its position from 1.
    pub(crate) fn decide(input: &IngestorInput) -> Result<Self, Report<RateLimitPlanError>> {
        let mut clauses = Vec::with_capacity(input.rate_limits().clauses().len());
        for (index, clause) in input.rate_limits().clauses().iter().enumerate() {
            let position = index
                .checked_add(1)
                .assured("a body's clauses are far fewer than usize::MAX");
            let quota = clause_quota(clause)
                .change_context(RateLimitPlanError::Quota { clause: position })?;
            let charge = match &clause.measure {
                None => RateLimitCharge::Payload,
                Some(measure) => match direct_payload_fact(input, measure) {
                    Some(fact) => RateLimitCharge::PayloadFact(fact),
                    None => RateLimitCharge::Measure(
                        LoweredMeasure::lower(measure)
                            .change_context(RateLimitPlanError::Measure { clause: position })?,
                    ),
                },
            };
            clauses.push(RateLimitClausePlan { quota, charge });
        }
        Ok(Self { clauses })
    }

    pub(crate) fn clauses(&self) -> &[RateLimitClausePlan] {
        &self.clauses
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.clauses.is_empty()
    }
}

/// Why an ingestor's clauses could not be planned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(crate) enum RateLimitPlanError {
    #[error("RATE LIMIT clause {clause} declares no allowance")]
    Quota { clause: usize },
    #[error("the measure of RATE LIMIT clause {clause} cannot be lowered")]
    Measure { clause: usize },
}

/// The allowance `clause` declares, within the bounds the engine keeps: a positive count no larger
/// than the period in nanoseconds, a positive period, and a burst a governor quota holds.
pub(crate) fn clause_quota(clause: &RateLimit) -> Result<Quota, Report<RateLimitClauseIssue>> {
    let count = clause
        .count
        .units()
        .change_context(RateLimitClauseIssue::Count)?;
    let burst = clause
        .burst
        .units()
        .change_context(RateLimitClauseIssue::Burst)?;
    let period = clause
        .period()
        .change_context(RateLimitClauseIssue::Period)?;
    match Quota::new(count.get(), period, burst.get()) {
        Ok(quota) => Ok(quota),
        Err(refusal) => {
            let issue = RateLimitClauseIssue::Quota(*refusal.current_context());
            Err(refusal.change_context(issue))
        }
    }
}

/// The payload fact `measure` reads when it is that fact alone, such as `payload.bytes`, which
/// the host reads without a program.
fn direct_payload_fact(
    input: &IngestorInput,
    measure: &Expression,
) -> Option<IngestorPayloadField> {
    let Expression::Field(field) = measure else {
        return None;
    };
    if field.scope != FieldScope::Payload {
        return None;
    }
    input
        .entity_kind()
        .capabilities()
        .payload
        .field(field.field.as_str())
}

/// A `RATE LIMIT` measure lowered into a program that writes one value per payload into the field
/// [`Self::FIELD`] of the namespace [`Self::NAMESPACE`], reading nothing but the payload, its
/// metadata and its headers.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LoweredMeasure {
    program: SpannedNode<Program>,
}

impl LoweredMeasure {
    /// The namespace a measure writes its value to, which no other scope names.
    pub(crate) const NAMESPACE: &str = "measure";
    /// The one field the measure writes.
    pub(crate) const FIELD: &str = "value";

    /// Lowers `measure` as the one assignment of its program. A bare field read has no scope to
    /// resolve against, so it is refused here.
    pub(crate) fn lower(measure: &Expression) -> nervix_vm::FrontendResult<Self> {
        let target =
            FieldName::parse(Self::FIELD).assured("the measure's field is a valid field name");
        let program = lower_route_construction(
            &RouteConstruction {
                assignments: vec![Assignment {
                    target: AssignmentTarget::bare(target),
                    value: measure.clone(),
                }],
                ..RouteConstruction::default()
            },
            SemanticScopePolicy::write_only(Self::NAMESPACE),
        )?;
        Ok(Self { program })
    }

    pub(crate) fn program(&self) -> &SpannedNode<Program> {
        &self.program
    }
}
