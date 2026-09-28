//! Browser drafts for session subscriptions.
//!
//! Layer: edges.
//!
//! - **Owns.** Incomplete subscription editor state — its name, relay reference, filter, delivery,
//!   and sampling — the typed field references it inserts into the filter, and its one-way
//!   conversion into the current subscription Model.
//! - **Depends on.** Vocabulary Models and names, the NSPL expression and sample-rate grammar, and
//!   the create dialog's selected references.
//! - **Must not know.** Subscription tabs, session transport, or how references are looked up.

use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_models::{
    CreateSubscription, Expression, FieldName, FieldReference, FieldScope, ModelKind, NodeRef,
    RelayName, SubscriptionDeliveryBehavior, SubscriptionName, expression_to_nspl,
};
use nervix_nspl::{parse_expression, subscribe::parse_batch_sample_rate};
use thiserror::Error;

use super::SelectedReference;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SubscriptionDraft {
    pub(super) name: String,
    pub(super) relay: Option<SelectedReference<RelayName>>,
    /// The `WHERE` predicate as typed. It reads the subscribed relay's record, which field
    /// references name through `input`.
    pub(super) filter: String,
    pub(super) delivery: SubscriptionDeliveryBehavior,
    pub(super) sampled: bool,
    pub(super) sample_rate: String,
}

/// What the filter reads as while it is edited.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum FilterReading {
    /// No filter: every record of the relay is selected.
    Everything,
    /// The canonical form of the expression the filter parses into.
    Expression(String),
    /// The parser's reason for rejecting the filter.
    Invalid(String),
}

impl SubscriptionDraft {
    /// A new draft named `name`, reading no relay yet. It delivers with `BLOCKING`, the delivery a
    /// statement that names none uses, and neither filters nor samples.
    pub(super) fn named(name: SubscriptionName) -> Self {
        Self {
            name: name.to_string(),
            relay: None,
            filter: String::new(),
            delivery: SubscriptionDeliveryBehavior::Blocking,
            sampled: false,
            sample_rate: String::new(),
        }
    }

    /// A new draft that reads `relay`, as an action on that relay starts one.
    pub(super) fn for_relay(name: SubscriptionName, relay: RelayName) -> Self {
        Self {
            relay: Some(SelectedReference::chosen(relay)),
            ..Self::named(name)
        }
    }

    pub(super) fn select_relay(&mut self, node: &NodeRef) {
        if node.kind == ModelKind::Relay {
            self.relay = Some(SelectedReference::chosen(RelayName::from(&node.identifier)));
        }
    }

    pub(super) fn selects_relay(&self, node: &NodeRef) -> bool {
        match &self.relay {
            Some(relay) => relay.selects(ModelKind::Relay, node),
            None => false,
        }
    }

    /// The relay whose fields the filter reads, while it still belongs to the draft's domain.
    pub(super) fn current_relay(&self) -> Option<&RelayName> {
        let relay = self.relay.as_ref()?;
        relay.current_name()
    }

    /// Keeps the selected relay visible after the draft moved to another domain, but requires
    /// selecting it again.
    pub(super) fn invalidate_references(&mut self) {
        if let Some(relay) = &mut self.relay {
            relay.invalidate();
        }
    }

    /// Appends a reference to `field` of the relay record, spelled the way the canonical renderer
    /// spells an `input` field.
    pub(super) fn insert_field_reference(&mut self, field: FieldName) {
        let reference = Expression::Field(FieldReference::scoped(FieldScope::Input, field));
        let rendered = expression_to_nspl(&reference)
            .assured("a field reference holds no float literal, the only expression that fails");
        if !self.filter.trim().is_empty() && !self.filter.ends_with(char::is_whitespace) {
            self.filter.push(' ');
        }
        self.filter.push_str(&rendered);
    }

    pub(super) fn filter_reading(&self) -> FilterReading {
        let filter = self.filter.trim();
        if filter.is_empty() {
            return FilterReading::Everything;
        }
        let expression = match parse_expression(filter) {
            Ok(expression) => expression,
            Err(error) => return FilterReading::Invalid(error.current_context().to_string()),
        };
        match expression_to_nspl(&expression) {
            Ok(canonical) => FilterReading::Expression(canonical),
            Err(error) => FilterReading::Invalid(error.current_context().to_string()),
        }
    }

    pub(super) fn build(&self) -> error_stack::Result<CreateSubscription, SubscriptionDraftError> {
        let name = SubscriptionName::parse(self.name.trim())
            .map_err(|_| Report::new(SubscriptionDraftError::Name))?;
        let Some(relay) = &self.relay else {
            return Err(Report::new(SubscriptionDraftError::RelayRequired));
        };
        let Some(relay) = relay.current_name() else {
            return Err(Report::new(SubscriptionDraftError::RelayChanged));
        };
        let where_clause = self.where_clause()?;
        let batch_sample_rate = self.batch_sample_rate()?;
        Ok(CreateSubscription {
            name,
            relay: relay.clone(),
            delivery_behavior: self.delivery,
            batch_sample_rate,
            where_clause,
        })
    }

    fn where_clause(&self) -> error_stack::Result<Option<Expression>, SubscriptionDraftError> {
        let filter = self.filter.trim();
        if filter.is_empty() {
            return Ok(None);
        }
        let expression = parse_expression(filter).change_context(SubscriptionDraftError::Filter)?;
        Ok(Some(expression))
    }

    /// The rate `BATCH SAMPLE RATE` would carry, checked by the rule of that clause.
    fn batch_sample_rate(&self) -> error_stack::Result<Option<String>, SubscriptionDraftError> {
        if !self.sampled {
            return Ok(None);
        }
        let rate = parse_batch_sample_rate(self.sample_rate.trim())
            .change_context(SubscriptionDraftError::SampleRate)?;
        Ok(Some(rate))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(super) enum SubscriptionDraftError {
    #[error("Subscription name is invalid")]
    Name,
    #[error("Choose a relay to subscribe to")]
    RelayRequired,
    #[error("The selected relay belongs to a changed context; select it again")]
    RelayChanged,
    #[error("Filter is not a valid expression")]
    Filter,
    #[error("Sample rate must be a number from 0 to 1")]
    SampleRate,
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{
        FieldName, ModelKind, ModelName, NodeRef, RelayName, SubscriptionDeliveryBehavior,
        SubscriptionName,
    };
    use nervix_nspl::client_statement::{ClientStatement, parse_client_statement};
    use strum::IntoEnumIterator as _;

    use super::{FilterReading, SubscriptionDraft, SubscriptionDraftError};

    fn name(value: &str) -> SubscriptionName {
        SubscriptionName::parse(value).assured("the test subscription names are valid")
    }

    fn relay(value: &str) -> RelayName {
        RelayName::parse(value).assured("the test relay names are valid")
    }

    fn node(kind: ModelKind, value: &str) -> NodeRef {
        NodeRef::new(
            kind,
            ModelName::parse(value).assured("the test names are valid"),
        )
    }

    fn draft_error(draft: &SubscriptionDraft) -> SubscriptionDraftError {
        draft
            .build()
            .err()
            .verified("the draft in this assertion has a required value missing or invalid")
            .current_context()
            .clone()
    }

    fn canonical(draft: &SubscriptionDraft) -> String {
        let statement =
            ClientStatement::CreateSubscription(draft.build().assured("the draft is complete"));
        let source = statement
            .to_canonical_nspl()
            .assured("a subscription statement renders");
        assert_eq!(
            parse_client_statement(&source).assured("the canonical statement parses"),
            statement
        );
        source
    }

    #[test]
    fn every_delivery_sampling_and_filter_option_renders_the_subscription_model() {
        let mut draft = SubscriptionDraft::for_relay(name("acme"), relay("notifications"));
        assert_eq!(
            canonical(&draft),
            "CREATE SUBSCRIPTION acme TO notifications;"
        );
        draft.insert_field_reference(FieldName::parse("tenant").assured("valid field"));
        assert_eq!(draft.filter, "input.tenant");
        draft.filter.push_str(" = 'acme'");
        draft.insert_field_reference(FieldName::parse("active").assured("valid field"));
        assert_eq!(draft.filter, "input.tenant = 'acme' input.active");
        draft.filter = "input.tenant = 'acme' AND ".to_string();
        draft.insert_field_reference(FieldName::parse("active").assured("valid field"));
        assert_eq!(draft.filter, "input.tenant = 'acme' AND input.active");
        draft.sampled = true;
        draft.sample_rate = " 0.25 ".to_string();
        for delivery in SubscriptionDeliveryBehavior::iter() {
            draft.delivery = delivery;
            let expected = match delivery {
                SubscriptionDeliveryBehavior::Blocking => {
                    "CREATE SUBSCRIPTION acme TO notifications BATCH SAMPLE RATE 0.25 WHERE \
                     input.tenant = 'acme' AND input.active;"
                }
                SubscriptionDeliveryBehavior::Dropping => {
                    "CREATE SUBSCRIPTION acme TO notifications DROPPING BATCH SAMPLE RATE 0.25 \
                     WHERE input.tenant = 'acme' AND input.active;"
                }
            };
            assert_eq!(canonical(&draft), expected);
        }
    }

    #[test]
    fn invalid_names_references_filters_and_rates_stay_in_the_draft() {
        let mut draft = SubscriptionDraft::named(name("typed"));
        assert_eq!(draft.delivery, SubscriptionDeliveryBehavior::Blocking);
        assert_eq!(draft_error(&draft), SubscriptionDraftError::RelayRequired);
        draft.select_relay(&node(ModelKind::Schema, "notifications"));
        assert_eq!(draft_error(&draft), SubscriptionDraftError::RelayRequired);
        draft.select_relay(&node(ModelKind::Relay, "notifications"));
        assert!(draft.selects_relay(&node(ModelKind::Relay, "notifications")));
        draft.name = "not a name".to_string();
        assert_eq!(draft_error(&draft), SubscriptionDraftError::Name);
        draft.name = "typed".to_string();
        draft.filter = "input.user_id = = 1".to_string();
        assert_eq!(draft_error(&draft), SubscriptionDraftError::Filter);
        assert!(matches!(draft.filter_reading(), FilterReading::Invalid(_)));
        draft.filter = "input.user_id=1".to_string();
        assert_eq!(
            draft.filter_reading(),
            FilterReading::Expression("input.user_id = 1".to_string())
        );
        draft.sampled = true;
        for rate in ["", "1.5", "half"] {
            draft.sample_rate = rate.to_string();
            assert_eq!(draft_error(&draft), SubscriptionDraftError::SampleRate);
        }
        draft.sampled = false;
        assert!(draft.build().is_ok());
        draft.filter = "   ".to_string();
        assert_eq!(draft.filter_reading(), FilterReading::Everything);
    }

    #[test]
    fn a_changed_domain_keeps_the_relay_visible_until_it_is_selected_again() {
        let mut draft = SubscriptionDraft::for_relay(name("tab"), relay("notifications"));
        assert_eq!(draft.current_relay(), Some(&relay("notifications")));
        draft.invalidate_references();
        assert_eq!(draft.current_relay(), None);
        assert!(!draft.selects_relay(&node(ModelKind::Relay, "notifications")));
        assert_eq!(draft_error(&draft), SubscriptionDraftError::RelayChanged);
        assert_eq!(
            draft
                .relay
                .as_ref()
                .assured("the relay stays selected")
                .name(),
            &relay("notifications")
        );
        draft.select_relay(&node(ModelKind::Relay, "notifications"));
        assert!(draft.build().is_ok());
        let mut empty = SubscriptionDraft::named(name("empty"));
        empty.invalidate_references();
        assert_eq!(empty.current_relay(), None);
    }
}
