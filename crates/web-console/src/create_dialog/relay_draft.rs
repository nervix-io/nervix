//! Browser drafts for relay creation.
//!
//! Layer: edges.
//!
//! - **Owns.** Incomplete relay editor state — its schema and branch references, the branching
//!   decision, capacity, and materialized state — and its one-way conversion into the current
//!   relay Model.
//! - **Depends on.** Vocabulary Models and names, and the create dialog's selected references.
//! - **Must not know.** Registry state, session transport, or how references are looked up.

use std::num::NonZeroUsize;

use error_stack::Report;
use nervix_models::{
    BranchName, CreateRelay, MaterializedRelayState, ModelKind, NodeRef, RelayBranching, RelayName,
    SchemaName, default_relay_buffer,
};
use thiserror::Error;

use super::SelectedReference;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RelayDraft {
    pub(super) name: String,
    pub(super) if_not_exists: bool,
    pub(super) schema: Option<SelectedReference<SchemaName>>,
    pub(super) branching: BranchingDraft,
    pub(super) capacity: String,
    /// No materialized state is a relay's own valid configuration, not a missing choice.
    pub(super) materialized_state: Option<MaterializedRelayState>,
}

impl Default for RelayDraft {
    /// A new draft starts from the relay buffer the language uses when `CAPACITY` is omitted.
    fn default() -> Self {
        Self {
            name: String::new(),
            if_not_exists: false,
            schema: None,
            branching: BranchingDraft::Unselected,
            capacity: default_relay_buffer().to_string(),
            materialized_state: None,
        }
    }
}

/// How a relay's records are branched, as far as the operator has decided. Choosing nothing and
/// choosing unbranched execution are different states, so a relay never becomes unbranched by
/// omission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum BranchingDraft {
    Unselected,
    Unbranched,
    /// `BRANCHED BY`, together with the branch its picker selected once there is one.
    BranchedBy(Option<SelectedReference<BranchName>>),
}

impl BranchingDraft {
    pub(super) fn is_unbranched(&self) -> bool {
        match self {
            Self::Unbranched => true,
            Self::Unselected | Self::BranchedBy(_) => false,
        }
    }

    pub(super) fn is_branched(&self) -> bool {
        match self {
            Self::BranchedBy(_) => true,
            Self::Unselected | Self::Unbranched => false,
        }
    }

    /// The branch the picker selected, whether or not it still belongs to the draft's domain.
    pub(super) fn selected_branch(&self) -> Option<&SelectedReference<BranchName>> {
        match self {
            Self::BranchedBy(branch) => branch.as_ref(),
            Self::Unselected | Self::Unbranched => None,
        }
    }
}

impl RelayDraft {
    pub(super) fn select_schema(&mut self, node: &NodeRef) {
        if node.kind == ModelKind::Schema {
            self.schema = Some(SelectedReference::chosen(SchemaName::from(
                &node.identifier,
            )));
        }
    }

    pub(super) fn selects_schema(&self, node: &NodeRef) -> bool {
        match &self.schema {
            Some(schema) => schema.selects(ModelKind::Schema, node),
            None => false,
        }
    }

    pub(super) fn choose_unbranched(&mut self) {
        self.branching = BranchingDraft::Unbranched;
    }

    /// Chooses `BRANCHED BY`. Choosing it again keeps the branch already selected.
    pub(super) fn choose_branched(&mut self) {
        if let BranchingDraft::BranchedBy(_) = self.branching {
            return;
        }
        self.branching = BranchingDraft::BranchedBy(None);
    }

    pub(super) fn select_branch(&mut self, node: &NodeRef) {
        if node.kind == ModelKind::Branch {
            let branch = SelectedReference::chosen(BranchName::from(&node.identifier));
            self.branching = BranchingDraft::BranchedBy(Some(branch));
        }
    }

    pub(super) fn selects_branch(&self, node: &NodeRef) -> bool {
        match self.branching.selected_branch() {
            Some(branch) => branch.selects(ModelKind::Branch, node),
            None => false,
        }
    }

    /// Keeps the selected schema and branch visible after the draft moved to another domain, but
    /// requires selecting each of them again.
    pub(super) fn invalidate_references(&mut self) {
        if let Some(schema) = &mut self.schema {
            schema.invalidate();
        }
        if let BranchingDraft::BranchedBy(Some(branch)) = &mut self.branching {
            branch.invalidate();
        }
    }

    pub(super) fn build(&self) -> error_stack::Result<CreateRelay, RelayDraftError> {
        let name = RelayName::parse(self.name.trim())
            .map_err(|_| Report::new(RelayDraftError::RelayName))?;
        let Some(schema) = &self.schema else {
            return Err(Report::new(RelayDraftError::SchemaRequired));
        };
        let Some(schema) = schema.current_name() else {
            return Err(Report::new(RelayDraftError::SchemaChanged));
        };
        let branching = match &self.branching {
            BranchingDraft::Unselected => {
                return Err(Report::new(RelayDraftError::BranchingRequired));
            }
            BranchingDraft::Unbranched => RelayBranching::unbranched(),
            BranchingDraft::BranchedBy(None) => {
                return Err(Report::new(RelayDraftError::BranchRequired));
            }
            BranchingDraft::BranchedBy(Some(branch)) => {
                let Some(branch) = branch.current_name() else {
                    return Err(Report::new(RelayDraftError::BranchChanged));
                };
                RelayBranching::branched_by(branch.clone())
            }
        };
        let buffer = self
            .capacity
            .trim()
            .parse::<NonZeroUsize>()
            .map_err(|_| Report::new(RelayDraftError::Capacity))?;
        Ok(CreateRelay {
            name,
            schema: schema.clone(),
            buffer,
            branching,
            materialized_state: self.materialized_state.clone(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(super) enum RelayDraftError {
    #[error("Relay name is invalid")]
    RelayName,
    #[error("Choose a schema for the relay")]
    SchemaRequired,
    #[error("The selected schema belongs to a changed context; select it again")]
    SchemaChanged,
    #[error("Choose UNBRANCHED or a branch")]
    BranchingRequired,
    #[error("Choose a branch for BRANCHED BY")]
    BranchRequired,
    #[error("The selected branch belongs to a changed context; select it again")]
    BranchChanged,
    #[error("Capacity must be a positive integer")]
    Capacity,
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{
        BranchName, CreateStatement, MaterializedRelayState, Model, ModelKind, ModelName, NodeRef,
        RelayBranching, RequestedResourceVersion, SchemaName, Statement,
    };
    use nervix_nspl::client_statement::{ClientStatement, parse_client_statement};
    use strum::IntoEnumIterator as _;

    use super::{BranchingDraft, RelayDraft, RelayDraftError};

    fn node(kind: ModelKind, name: &str) -> NodeRef {
        NodeRef::new(
            kind,
            ModelName::parse(name).assured("the test names are valid"),
        )
    }

    fn draft_error(draft: &RelayDraft) -> RelayDraftError {
        draft
            .build()
            .err()
            .verified("the draft in this assertion has a required value missing or invalid")
            .current_context()
            .clone()
    }

    fn assert_canonical_round_trip(model: Model, if_not_exists: bool) -> String {
        let model: Model<RequestedResourceVersion> = model.into();
        let statement = Statement::Create(CreateStatement::new(Box::new(model), if_not_exists));
        let source = statement
            .to_canonical_nspl()
            .assured("every completed relay has a canonical command");
        let parsed = parse_client_statement(&source)
            .assured("the canonical command must parse back to the same Model");
        assert_eq!(parsed, ClientStatement::Server(statement));
        source
    }

    #[test]
    fn every_branching_and_materialized_state_becomes_the_exact_relay_model() {
        let mut draft = RelayDraft {
            name: "orders".to_string(),
            ..RelayDraft::default()
        };
        assert_eq!(draft.capacity, "1");
        draft.select_schema(&node(ModelKind::Schema, "order_record"));
        draft.choose_branched();
        draft.select_branch(&node(ModelKind::Branch, "by_tenant"));
        draft.choose_branched();
        draft.capacity = " 4 ".to_string();
        for state in MaterializedRelayState::iter() {
            draft.materialized_state = Some(state.clone());
            let relay = draft.build().assured("the branched draft is complete");
            assert_eq!(
                relay.branching,
                RelayBranching::branched_by(
                    BranchName::parse("by_tenant").assured("valid branch name")
                )
            );
            assert_eq!(relay.buffer.get(), 4);
            assert_eq!(relay.materialized_state, Some(state));
            let source = assert_canonical_round_trip(Model::Relay(relay), true);
            assert_eq!(
                source,
                "CREATE IF NOT EXISTS RELAY orders SCHEMA order_record BRANCHED BY by_tenant \
                 CAPACITY 4 WITH MATERIALIZED STATE LAST BY TIMESTAMP;"
            );
        }

        draft.choose_unbranched();
        draft.materialized_state = None;
        let relay = draft.build().assured("the unbranched draft is complete");
        assert_eq!(
            relay.buffer,
            NonZeroUsize::new(4).assured("four is nonzero")
        );
        assert_eq!(
            assert_canonical_round_trip(Model::Relay(relay), false),
            "CREATE RELAY orders SCHEMA order_record UNBRANCHED CAPACITY 4;"
        );
    }

    #[test]
    fn unselected_branching_and_missing_references_remain_explicit_errors() {
        let mut draft = RelayDraft {
            name: "not a relay".to_string(),
            ..RelayDraft::default()
        };
        assert_eq!(draft_error(&draft), RelayDraftError::RelayName);
        draft.name = "orders".to_string();
        assert_eq!(draft_error(&draft), RelayDraftError::SchemaRequired);
        draft.select_schema(&node(ModelKind::Relay, "order_record"));
        assert_eq!(draft_error(&draft), RelayDraftError::SchemaRequired);
        draft.select_schema(&node(ModelKind::Schema, "order_record"));
        assert_eq!(draft.branching, BranchingDraft::Unselected);
        assert_eq!(draft_error(&draft), RelayDraftError::BranchingRequired);
        draft.choose_branched();
        assert_eq!(draft_error(&draft), RelayDraftError::BranchRequired);
        draft.select_branch(&node(ModelKind::Schema, "by_tenant"));
        assert_eq!(draft_error(&draft), RelayDraftError::BranchRequired);
        draft.select_branch(&node(ModelKind::Branch, "by_tenant"));
        draft.capacity = "0".to_string();
        assert_eq!(draft_error(&draft), RelayDraftError::Capacity);
        draft.capacity = "many".to_string();
        assert_eq!(draft_error(&draft), RelayDraftError::Capacity);
    }

    #[test]
    fn a_changed_domain_keeps_both_references_visible_until_they_are_selected_again() {
        let schema = node(ModelKind::Schema, "order_record");
        let branch = node(ModelKind::Branch, "by_tenant");
        let mut draft = RelayDraft {
            name: "orders".to_string(),
            ..RelayDraft::default()
        };
        draft.select_schema(&schema);
        draft.select_branch(&branch);
        assert!(draft.selects_schema(&schema));
        assert!(draft.selects_branch(&branch));
        assert!(draft.branching.is_branched());
        assert!(!draft.branching.is_unbranched());

        draft.invalidate_references();
        assert!(!draft.selects_schema(&schema));
        assert!(!draft.selects_branch(&branch));
        assert_eq!(
            draft
                .schema
                .as_ref()
                .map(|reference| reference.name().as_str()),
            Some("order_record")
        );
        assert_eq!(draft_error(&draft), RelayDraftError::SchemaChanged);
        draft.select_schema(&schema);
        assert_eq!(draft_error(&draft), RelayDraftError::BranchChanged);
        draft.select_branch(&branch);
        assert!(draft.build().is_ok());

        draft.choose_unbranched();
        assert!(!draft.selects_branch(&branch));
        assert_eq!(draft.branching.selected_branch(), None);
        draft.invalidate_references();
        assert_eq!(draft_error(&draft), RelayDraftError::SchemaChanged);
        assert_eq!(
            SchemaName::parse("order_record").assured("valid schema name"),
            *draft
                .schema
                .as_ref()
                .assured("the schema stays selected")
                .name()
        );
    }
}
