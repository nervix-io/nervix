//! The right side, correlation and timeout policies of a visual correlator until they become its
//! Model.
//!
//! Layer: edges.
//!
//! - **Owns.** A correlator's `RIGHT FROM` list, its `CORRELATE WHERE` text, its explicit match
//!   policy, its `MAX TIME` text and each side's timeout action, and their conversion to current
//!   correlator Models.
//! - **Depends on.** Typed vocabulary, expression parsing, the shared input list draft and
//!   selected browser references.
//! - **Must not know.** Correlation buffers, timeout deadlines, or registry validation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use error_stack::{Report, ResultExt as _};
use nervix_models::{
    CorrelationTimeoutAction, CorrelationTimeoutPolicy, CorrelatorMatchPolicy, Expression,
    FieldName, FieldReference, FieldScope, ModelKind, NodeRef, ProcessorInputs, RelayName,
};
use nervix_plexus::parse_expression;
use strum::Display;

use super::{
    SelectedReference,
    inserted_reference::InsertedReference,
    processor_draft::ProcessorDraftError,
    processor_input_draft::{InputList, InputListDraft},
};

/// One side of a correlation, named as its records' scope is spelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
pub(super) enum CorrelationSide {
    #[strum(serialize = "left")]
    Left,
    #[strum(serialize = "right")]
    Right,
}

impl CorrelationSide {
    /// The scope an expression reads this side's record in.
    pub(super) fn scope(self) -> FieldScope {
        match self {
            Self::Left => FieldScope::Left,
            Self::Right => FieldScope::Right,
        }
    }

    /// A reference to `field` of this side's record.
    pub(super) fn reference(self, field: FieldName) -> InsertedReference {
        InsertedReference::Field(FieldReference::scoped(self.scope(), field))
    }
}

/// What a correlator does with one side's record no record of the other side matched in time.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) enum TimeoutDraft {
    #[default]
    Unselected,
    Drop,
    SendTo(Option<SelectedReference<RelayName>>),
}

impl TimeoutDraft {
    pub(super) fn choose_drop(&mut self) {
        *self = Self::Drop;
    }

    /// Chooses `SEND TO`, keeping the relay already chosen for it.
    pub(super) fn choose_send(&mut self) {
        if !matches!(self, Self::SendTo(_)) {
            *self = Self::SendTo(None);
        }
    }

    pub(super) fn select_relay(&mut self, node: &NodeRef) {
        if node.kind == ModelKind::Relay
            && let Self::SendTo(relay) = self
        {
            *relay = Some(SelectedReference::chosen(RelayName::from(&node.identifier)));
        }
    }

    pub(super) fn current_relay(&self) -> Option<&RelayName> {
        let Self::SendTo(Some(relay)) = self else {
            return None;
        };
        relay.current_name()
    }

    pub(super) fn invalidate(&mut self) {
        if let Self::SendTo(Some(relay)) = self {
            relay.invalidate();
        }
    }

    fn build(
        &self,
        side: CorrelationSide,
    ) -> error_stack::Result<CorrelationTimeoutAction, ProcessorDraftError> {
        match self {
            Self::Unselected => Err(Report::new(ProcessorDraftError::Timeout { side })),
            Self::Drop => Ok(CorrelationTimeoutAction::Drop),
            Self::SendTo(None) => Err(Report::new(ProcessorDraftError::TimeoutRelay { side })),
            Self::SendTo(Some(relay)) => {
                let relay = relay.current_name().ok_or_else(|| {
                    Report::new(ProcessorDraftError::TimeoutRelayChanged { side })
                })?;
                Ok(CorrelationTimeoutAction::SendTo {
                    relay: relay.clone(),
                })
            }
        }
    }
}

/// What only a correlator declares, kept as typed until the draft builds. Its left side is the
/// processor draft's primary input list.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct CorrelatorDraft {
    pub(super) right: InputListDraft,
    pub(super) correlate_where: String,
    pub(super) match_policy: Option<CorrelatorMatchPolicy>,
    pub(super) max_time: String,
    pub(super) left_timeout: TimeoutDraft,
    pub(super) right_timeout: TimeoutDraft,
}

/// The checked right side, correlation and timeout policies of a correlator.
pub(super) struct CorrelationParts {
    pub(super) right: ProcessorInputs,
    pub(super) correlate_where: Expression,
    pub(super) match_policy: CorrelatorMatchPolicy,
    pub(super) max_time: String,
    pub(super) timeout_policy: CorrelationTimeoutPolicy,
}

impl CorrelatorDraft {
    pub(super) fn timeout(&self, side: CorrelationSide) -> &TimeoutDraft {
        match side {
            CorrelationSide::Left => &self.left_timeout,
            CorrelationSide::Right => &self.right_timeout,
        }
    }

    pub(super) fn timeout_mut(&mut self, side: CorrelationSide) -> &mut TimeoutDraft {
        match side {
            CorrelationSide::Left => &mut self.left_timeout,
            CorrelationSide::Right => &mut self.right_timeout,
        }
    }

    /// Selects `node` for the active right input. A changed first right relay also requires the
    /// right timeout relay again, which is offered by that relay's schema and branch.
    pub(super) fn select_right_input(&mut self, node: &NodeRef) {
        if self.right.select(node) {
            self.right_timeout.invalidate();
        }
    }

    /// Requires every relay this part of the draft selected to be selected again.
    pub(super) fn invalidate_relays(&mut self) {
        self.right.invalidate();
        self.left_timeout.invalidate();
        self.right_timeout.invalidate();
    }

    /// Appends a reference to `field` of `side`'s record to the `CORRELATE WHERE` text.
    pub(super) fn insert_correlation_reference(&mut self, side: CorrelationSide, field: FieldName) {
        side.reference(field).append_to(&mut self.correlate_where);
    }

    /// The right side, correlation and timeouts, or the first missing or unreadable one in form
    /// order.
    pub(super) fn build(&self) -> error_stack::Result<CorrelationParts, ProcessorDraftError> {
        let right = self.right.build(InputList::Right)?;
        let correlate_where = self.correlate_where.trim();
        if correlate_where.is_empty() {
            return Err(Report::new(ProcessorDraftError::CorrelateWhere));
        }
        let correlate_where = parse_expression(correlate_where)
            .change_context(ProcessorDraftError::CorrelateWhere)?;
        let match_policy = self
            .match_policy
            .ok_or_else(|| Report::new(ProcessorDraftError::MatchPolicy))?;
        let max_time = self.max_time.trim();
        if max_time.is_empty() {
            return Err(Report::new(ProcessorDraftError::MaxTime));
        }
        let left = self.left_timeout.build(CorrelationSide::Left)?;
        let right_timeout = self.right_timeout.build(CorrelationSide::Right)?;
        Ok(CorrelationParts {
            right,
            correlate_where,
            match_policy,
            max_time: max_time.to_string(),
            timeout_policy: CorrelationTimeoutPolicy {
                left,
                right: right_timeout,
            },
        })
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{BinaryOperator, ModelName};

    use super::*;

    fn node(kind: ModelKind, name: &str) -> NodeRef {
        NodeRef::new(kind, ModelName::parse(name).assured("test name is valid"))
    }

    fn field(name: &str) -> FieldName {
        FieldName::parse(name).assured("test field is valid")
    }

    fn complete() -> CorrelatorDraft {
        let mut draft = CorrelatorDraft::default();
        draft.select_right_input(&node(ModelKind::Relay, "payments"));
        draft.correlate_where = "left.id = right.id".into();
        draft.match_policy = Some(CorrelatorMatchPolicy::Earliest);
        draft.max_time = " 5s ".into();
        draft.left_timeout.choose_drop();
        draft.right_timeout.choose_drop();
        draft
    }

    fn refusal(draft: &CorrelatorDraft) -> ProcessorDraftError {
        draft
            .build()
            .err()
            .assured("the correlator draft is incomplete")
            .current_context()
            .clone()
    }

    #[test]
    fn a_complete_correlation_keeps_its_right_side_policies_and_exact_time() {
        let mut draft = complete();
        draft.right.add();
        draft.select_right_input(&node(ModelKind::Relay, "refunds"));
        draft.right.relays[1].where_clause = "right.amount > 0".into();
        draft.right_timeout.choose_send();
        draft
            .right_timeout
            .select_relay(&node(ModelKind::Relay, "unmatched"));
        let parts = draft.build().assured("the correlation is complete");
        assert_eq!(
            parts
                .right
                .from
                .iter()
                .map(RelayName::as_str)
                .collect::<Vec<_>>(),
            ["payments", "refunds"]
        );
        assert_eq!(parts.right.r#where.len(), 1);
        assert_eq!(
            parts.correlate_where,
            Expression::Binary {
                operator: BinaryOperator::Equal,
                left: Box::new(Expression::Field(FieldReference::scoped(
                    FieldScope::Left,
                    field("id"),
                ))),
                right: Box::new(Expression::Field(FieldReference::scoped(
                    FieldScope::Right,
                    field("id"),
                ))),
            }
        );
        assert_eq!(parts.match_policy, CorrelatorMatchPolicy::Earliest);
        assert_eq!(parts.max_time, "5s");
        assert_eq!(
            parts.timeout_policy,
            CorrelationTimeoutPolicy {
                left: CorrelationTimeoutAction::Drop,
                right: CorrelationTimeoutAction::SendTo {
                    relay: RelayName::parse("unmatched").assured("valid relay"),
                },
            }
        );
    }

    #[test]
    fn every_correlation_choice_is_required_in_form_order() {
        let mut draft = CorrelatorDraft::default();
        assert!(matches!(
            refusal(&draft),
            ProcessorDraftError::Input {
                list: InputList::Right,
                index: 1
            }
        ));
        draft.select_right_input(&node(ModelKind::Relay, "payments"));
        assert_eq!(refusal(&draft), ProcessorDraftError::CorrelateWhere);
        draft.correlate_where = "left.id =".into();
        assert_eq!(refusal(&draft), ProcessorDraftError::CorrelateWhere);
        draft.correlate_where = "left.id = right.id".into();
        assert_eq!(refusal(&draft), ProcessorDraftError::MatchPolicy);
        draft.match_policy = Some(CorrelatorMatchPolicy::Latest);
        assert_eq!(refusal(&draft), ProcessorDraftError::MaxTime);
        draft.max_time = "1m".into();
        assert_eq!(
            refusal(&draft),
            ProcessorDraftError::Timeout {
                side: CorrelationSide::Left
            }
        );
        draft.left_timeout.choose_send();
        assert_eq!(
            refusal(&draft),
            ProcessorDraftError::TimeoutRelay {
                side: CorrelationSide::Left
            }
        );
        draft
            .left_timeout
            .select_relay(&node(ModelKind::Relay, "unmatched"));
        assert_eq!(
            refusal(&draft),
            ProcessorDraftError::Timeout {
                side: CorrelationSide::Right
            }
        );
        draft.right_timeout.choose_drop();
        assert!(draft.build().is_ok());
    }

    #[test]
    fn a_changed_first_right_relay_requires_later_right_inputs_and_its_timeout_relay_again() {
        let mut draft = complete();
        draft.right.add();
        draft.select_right_input(&node(ModelKind::Relay, "refunds"));
        draft.right_timeout.choose_send();
        draft
            .right_timeout
            .select_relay(&node(ModelKind::Relay, "unmatched"));
        draft.right.active = 1;
        draft.select_right_input(&node(ModelKind::Relay, "chargebacks"));
        assert_eq!(
            draft
                .right_timeout
                .current_relay()
                .assured("a later right input keeps the timeout relay")
                .as_str(),
            "unmatched"
        );
        draft.right.active = 0;
        draft.select_right_input(&node(ModelKind::Relay, "settlements"));
        assert_eq!(draft.right.relays[1].current_relay(), None);
        assert_eq!(draft.right_timeout.current_relay(), None);
        assert!(matches!(
            refusal(&draft),
            ProcessorDraftError::InputChanged {
                list: InputList::Right,
                index: 2
            }
        ));
        draft.right.active = 1;
        draft.select_right_input(&node(ModelKind::Relay, "chargebacks"));
        assert_eq!(
            refusal(&draft),
            ProcessorDraftError::TimeoutRelayChanged {
                side: CorrelationSide::Right
            }
        );
    }

    #[test]
    fn correlation_references_enter_their_explicit_side_scopes() {
        let mut draft = CorrelatorDraft::default();
        draft.insert_correlation_reference(CorrelationSide::Left, field("order_id"));
        assert_eq!(draft.correlate_where, "left.order_id");
        draft.correlate_where.push_str(" =");
        draft.insert_correlation_reference(CorrelationSide::Right, field("order_id"));
        assert_eq!(draft.correlate_where, "left.order_id = right.order_id");
    }

    #[test]
    fn timeout_refusals_name_their_side() {
        assert_eq!(
            ProcessorDraftError::Timeout {
                side: CorrelationSide::Left
            }
            .to_string(),
            "Choose the left timeout action: DROP or SEND TO"
        );
        assert_eq!(
            ProcessorDraftError::TimeoutRelay {
                side: CorrelationSide::Right
            }
            .to_string(),
            "Choose the right timeout relay"
        );
    }
}
