//! The materialized relay and cadence of a visual generator until they become its Model.
//!
//! Layer: edges.
//!
//! - **Owns.** The browser selection of a generator's materialized relay, the text of its `EACH`
//!   period, and their conversion to the generator Model's source and domain-clock cadence.
//! - **Depends on.** Typed relay names, the domain-clock period the language reads `EACH` as, and
//!   selected browser references.
//! - **Must not know.** Generator scheduling, materialized-state reads or registry validation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use error_stack::Report;
use nervix_models::{DomainClockPeriod, ModelKind, NodeRef, RelayName};

use super::{SelectedReference, processor_draft::ProcessorDraftError};

/// What only a generator declares: the materialized relay it reads and the domain-clock period
/// it generates on, kept as selected and typed until the draft builds.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct GeneratorDraft {
    pub(super) relay: Option<SelectedReference<RelayName>>,
    /// The `EACH` text, read as the language reads a domain-clock period.
    pub(super) each: String,
}

/// The checked materialized relay and cadence of a generator.
pub(super) struct GeneratorParts {
    pub(super) relay: RelayName,
    pub(super) each: DomainClockPeriod,
}

impl GeneratorDraft {
    /// Selects `node` as the generator's materialized relay, and reports whether the selection
    /// changed: the routes were offered by the previous relay's branch, so a change requires them
    /// again.
    pub(super) fn select_relay(&mut self, node: &NodeRef) -> bool {
        if node.kind != ModelKind::Relay {
            return false;
        }
        let selected = RelayName::from(&node.identifier);
        let changed = self.current_relay() != Some(&selected);
        self.relay = Some(SelectedReference::chosen(selected));
        changed
    }

    /// The selected materialized relay, while it still belongs to the draft's context.
    pub(super) fn current_relay(&self) -> Option<&RelayName> {
        match &self.relay {
            Some(relay) => relay.current_name(),
            None => None,
        }
    }

    /// Whether the relay was selected in a context the draft has since left.
    pub(super) fn relay_changed(&self) -> bool {
        match &self.relay {
            Some(relay) => !relay.is_current(),
            None => false,
        }
    }

    pub(super) fn invalidate(&mut self) {
        if let Some(relay) = &mut self.relay {
            relay.invalidate();
        }
    }

    /// The generator's materialized relay and period, or the first missing or unreadable one in
    /// form order. Whether the relay holds materialized state on the generator's branch is the
    /// registry's rule, which the server applies.
    pub(super) fn build(&self) -> error_stack::Result<GeneratorParts, ProcessorDraftError> {
        let relay = match &self.relay {
            Some(relay) => relay
                .current_name()
                .ok_or_else(|| Report::new(ProcessorDraftError::GeneratorRelayChanged))?
                .clone(),
            None => return Err(Report::new(ProcessorDraftError::GeneratorRelay)),
        };
        let each = self.each.trim();
        if each.is_empty() {
            return Err(Report::new(ProcessorDraftError::Each));
        }
        let each = each
            .parse::<DomainClockPeriod>()
            .map_err(|cause| Report::new(cause).change_context(ProcessorDraftError::Each))?;
        Ok(GeneratorParts { relay, each })
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::ModelName;

    use super::*;

    fn relay(name: &str) -> NodeRef {
        NodeRef::new(
            ModelKind::Relay,
            ModelName::parse(name).assured("test name is valid"),
        )
    }

    fn refusal(draft: &GeneratorDraft) -> ProcessorDraftError {
        draft
            .build()
            .err()
            .assured("the generator draft is incomplete")
            .current_context()
            .clone()
    }

    #[test]
    fn a_generator_needs_a_current_materialized_relay_and_a_readable_period() {
        let mut draft = GeneratorDraft::default();
        assert_eq!(refusal(&draft), ProcessorDraftError::GeneratorRelay);
        assert!(draft.select_relay(&relay("notifications")));
        assert!(!draft.select_relay(&relay("notifications")));
        assert_eq!(refusal(&draft), ProcessorDraftError::Each);
        draft.each = "soon".into();
        assert_eq!(refusal(&draft), ProcessorDraftError::Each);
        draft.each = "0s".into();
        assert_eq!(refusal(&draft), ProcessorDraftError::Each);
        draft.each = " 250ms ".into();
        let parts = draft.build().assured("the relay and period are complete");
        assert_eq!(parts.relay.as_str(), "notifications");
        assert_eq!(
            parts.each,
            "250ms"
                .parse::<DomainClockPeriod>()
                .assured("a positive period parses")
        );

        draft.invalidate();
        assert!(draft.current_relay().is_none());
        assert_eq!(refusal(&draft), ProcessorDraftError::GeneratorRelayChanged);
        assert!(draft.select_relay(&relay("notifications")));
        // A selection of another kind leaves the relay as it is.
        assert!(!draft.select_relay(&NodeRef::new(
            ModelKind::Branch,
            ModelName::parse("by_tenant").assured("test name is valid"),
        )));
        assert_eq!(
            draft
                .current_relay()
                .verified("the relay was selected again")
                .as_str(),
            "notifications"
        );
    }
}
