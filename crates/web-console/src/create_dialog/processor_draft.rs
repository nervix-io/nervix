//! Incomplete visual junction and reingestor state until it becomes a semantic Model.
//!
//! Layer: edges.
//!
//! - **Owns.** Ordered browser drafts for processor inputs, state dependencies, routes, and
//!   branch-boundary choices, and their conversion to current semantic Models.
//! - **Depends on.** Typed vocabulary, expression parsing, and shared route drafts.
//! - **Must not know.** Registry state, runtime execution, or session transport.

use std::collections::BTreeSet;

use error_stack::{Report, ResultExt as _};
use nervix_models::{
    AckMode, BranchName, BranchSelection, CreateJunction, CreateReingestor, InputCollectPolicy,
    JunctionName, MaterializedStateDependency, MaterializedStatePolicy, ModelKind, NodeRef,
    ProcessorInputWhere, ProcessorInputs, ProcessorOutputs, ReingestorName, RelayName,
};
use nervix_nspl::parse_expression;
use thiserror::Error;

use super::{
    SelectedReference,
    ingestor_route_draft::{AssignmentDraft, IngestRouteDraft, IngestRouteDraftError},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProcessorFamily {
    Junction,
    Reingestor,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct InputDraft {
    pub(super) relay: Option<SelectedReference<RelayName>>,
    pub(super) where_clause: String,
}

impl InputDraft {
    fn select_relay(&mut self, node: &NodeRef) {
        if node.kind == ModelKind::Relay {
            self.relay = Some(SelectedReference::chosen(RelayName::from(&node.identifier)));
        }
    }

    pub(super) fn current_relay(&self) -> Option<&RelayName> {
        self.relay
            .as_ref()
            .and_then(SelectedReference::current_name)
    }

    fn invalidate(&mut self) {
        if let Some(relay) = &mut self.relay {
            relay.invalidate();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) enum JunctionBranchDraft {
    #[default]
    Unselected,
    Unbranched,
    Branched(Option<SelectedReference<BranchName>>),
}

impl JunctionBranchDraft {
    pub(super) fn choose_unbranched(&mut self) {
        *self = Self::Unbranched;
    }

    pub(super) fn choose_branched(&mut self) {
        if !matches!(self, Self::Branched(_)) {
            *self = Self::Branched(None);
        }
    }

    pub(super) fn select_branch(&mut self, node: &NodeRef) {
        if node.kind == ModelKind::Branch
            && let Self::Branched(branch) = self
        {
            *branch = Some(SelectedReference::chosen(BranchName::from(
                &node.identifier,
            )));
        }
    }

    pub(super) fn current_branch(&self) -> Option<&BranchName> {
        let Self::Branched(Some(branch)) = self else {
            return None;
        };
        branch.current_name()
    }

    fn invalidate(&mut self) {
        if let Self::Branched(Some(branch)) = self {
            branch.invalidate();
        }
    }

    fn build(&self) -> error_stack::Result<BranchSelection, ProcessorDraftError> {
        match self {
            Self::Unselected => Err(Report::new(ProcessorDraftError::Branching)),
            Self::Unbranched => Ok(BranchSelection::Unbranched),
            Self::Branched(None) => Err(Report::new(ProcessorDraftError::Branch)),
            Self::Branched(Some(branch)) => Ok(BranchSelection::BranchedBy {
                branch: branch
                    .current_name()
                    .ok_or_else(|| Report::new(ProcessorDraftError::BranchChanged))?
                    .clone(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) enum StatePolicyDraft {
    #[default]
    Unselected,
    RequiredSkip,
    RequiredWait,
    Default(Vec<AssignmentDraft>),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct StateDependencyDraft {
    pub(super) relay: Option<SelectedReference<RelayName>>,
    pub(super) policy: StatePolicyDraft,
}

impl StateDependencyDraft {
    pub(super) fn select_relay(&mut self, node: &NodeRef) {
        if node.kind == ModelKind::Relay {
            let selected = RelayName::from(&node.identifier);
            if self.current_relay() != Some(&selected)
                && let StatePolicyDraft::Default(assignments) = &mut self.policy
            {
                for assignment in assignments {
                    assignment.invalidate();
                }
            }
            self.relay = Some(SelectedReference::chosen(selected));
        }
    }

    pub(super) fn current_relay(&self) -> Option<&RelayName> {
        self.relay
            .as_ref()
            .and_then(SelectedReference::current_name)
    }

    fn invalidate(&mut self) {
        if let Some(relay) = &mut self.relay {
            relay.invalidate();
        }
        if let StatePolicyDraft::Default(assignments) = &mut self.policy {
            for assignment in assignments {
                assignment.invalidate();
            }
        }
    }

    fn build(
        &self,
        index: usize,
    ) -> error_stack::Result<MaterializedStateDependency, ProcessorDraftError> {
        let relay = match &self.relay {
            Some(relay) => relay
                .current_name()
                .ok_or_else(|| Report::new(ProcessorDraftError::StateRelayChanged { index }))?,
            None => return Err(Report::new(ProcessorDraftError::StateRelay { index })),
        };
        let policy = match &self.policy {
            StatePolicyDraft::Unselected => {
                return Err(Report::new(ProcessorDraftError::StatePolicy { index }));
            }
            StatePolicyDraft::RequiredSkip => MaterializedStatePolicy::RequiredSkip,
            StatePolicyDraft::RequiredWait => MaterializedStatePolicy::RequiredWait,
            StatePolicyDraft::Default(assignments) => {
                let mut built = Vec::new();
                for (assignment_index, assignment) in assignments.iter().enumerate() {
                    built.push(assignment.build().map_err(|error| {
                        let cause = error.current_context().clone();
                        error.change_context(ProcessorDraftError::StateDefault {
                            index,
                            assignment: assignment_index + 1,
                            cause,
                        })
                    })?);
                }
                MaterializedStatePolicy::Default(built)
            }
        };
        Ok(MaterializedStateDependency {
            relay: relay.clone(),
            policy,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ProcessorDraft {
    pub(super) name: String,
    pub(super) if_not_exists: bool,
    pub(super) mode: AckMode,
    pub(super) inputs: Vec<InputDraft>,
    pub(super) active_input: usize,
    pub(super) collect: Option<InputCollectPolicy>,
    pub(super) filter: String,
    pub(super) branching: JunctionBranchDraft,
    pub(super) state: Vec<StateDependencyDraft>,
    pub(super) active_state: usize,
    pub(super) routes: Vec<IngestRouteDraft>,
    pub(super) active_route: usize,
    pub(super) family: ProcessorFamily,
}

impl ProcessorDraft {
    pub(super) fn new(family: ProcessorFamily) -> Self {
        Self {
            name: String::new(),
            if_not_exists: false,
            mode: AckMode::Attached,
            inputs: vec![InputDraft::default()],
            active_input: 0,
            collect: None,
            filter: String::new(),
            branching: JunctionBranchDraft::Unselected,
            state: Vec::new(),
            active_state: 0,
            routes: vec![IngestRouteDraft::default()],
            active_route: 0,
            family,
        }
    }

    pub(super) fn active_input(&self) -> Option<&InputDraft> {
        self.inputs.get(self.active_input)
    }

    pub(super) fn active_input_mut(&mut self) -> Option<&mut InputDraft> {
        self.inputs.get_mut(self.active_input)
    }

    pub(super) fn current_first_input(&self) -> Option<&RelayName> {
        self.inputs.first().and_then(InputDraft::current_relay)
    }

    pub(super) fn select_input(&mut self, node: &NodeRef) {
        if node.kind != ModelKind::Relay {
            return;
        }
        let previous = self.current_first_input().cloned();
        let active = self.active_input;
        if let Some(input) = self.active_input_mut() {
            input.select_relay(node);
        }
        if active == 0 && self.current_first_input() != previous.as_ref() {
            for input in self.inputs.iter_mut().skip(1) {
                input.invalidate();
            }
            for state in &mut self.state {
                state.invalidate();
            }
            for route in &mut self.routes {
                route.invalidate_references();
            }
        }
    }

    pub(super) fn select_branch(&mut self, node: &NodeRef) {
        let previous = self.branching.current_branch().cloned();
        self.branching.select_branch(node);
        if self.branching.current_branch() != previous.as_ref() {
            self.invalidate_relays();
        }
    }

    pub(super) fn choose_unbranched(&mut self) {
        if self.branching != JunctionBranchDraft::Unbranched {
            self.branching.choose_unbranched();
            self.invalidate_relays();
        }
    }

    pub(super) fn choose_branched(&mut self) {
        if !matches!(self.branching, JunctionBranchDraft::Branched(_)) {
            self.branching.choose_branched();
            self.invalidate_relays();
        }
    }

    fn invalidate_relays(&mut self) {
        for input in &mut self.inputs {
            input.invalidate();
        }
        for state in &mut self.state {
            state.invalidate();
        }
        for route in &mut self.routes {
            route.invalidate_references();
        }
    }

    pub(super) fn invalidate_references(&mut self) {
        self.branching.invalidate();
        self.invalidate_relays();
    }

    pub(super) fn active_state(&self) -> Option<&StateDependencyDraft> {
        self.state.get(self.active_state)
    }

    pub(super) fn active_state_mut(&mut self) -> Option<&mut StateDependencyDraft> {
        self.state.get_mut(self.active_state)
    }

    pub(super) fn active_route(&self) -> Option<&IngestRouteDraft> {
        self.routes.get(self.active_route)
    }

    pub(super) fn active_route_mut(&mut self) -> Option<&mut IngestRouteDraft> {
        self.routes.get_mut(self.active_route)
    }

    pub(super) fn add_input(&mut self) {
        self.inputs.push(InputDraft::default());
        self.active_input = self.inputs.len() - 1;
    }

    pub(super) fn remove_input(&mut self) {
        if self.inputs.len() > 1 {
            self.inputs.remove(self.active_input);
            self.active_input = self.active_input.min(self.inputs.len() - 1);
        }
    }

    pub(super) fn move_input(&mut self, up: bool) {
        let other = if up {
            self.active_input.checked_sub(1)
        } else if self.active_input + 1 < self.inputs.len() {
            Some(self.active_input + 1)
        } else {
            None
        };
        if let Some(other) = other {
            self.inputs.swap(self.active_input, other);
            self.active_input = other;
        }
    }

    pub(super) fn add_state(&mut self) {
        self.state.push(StateDependencyDraft::default());
        self.active_state = self.state.len() - 1;
    }

    pub(super) fn remove_state(&mut self) {
        if !self.state.is_empty() {
            self.state.remove(self.active_state);
            self.active_state = if self.state.is_empty() {
                0
            } else {
                self.active_state.min(self.state.len() - 1)
            };
        }
    }

    pub(super) fn move_state(&mut self, up: bool) {
        let other = if up {
            self.active_state.checked_sub(1)
        } else if self.active_state + 1 < self.state.len() {
            Some(self.active_state + 1)
        } else {
            None
        };
        if let Some(other) = other {
            self.state.swap(self.active_state, other);
            self.active_state = other;
        }
    }

    pub(super) fn add_route(&mut self) {
        self.routes.push(IngestRouteDraft::default());
        self.active_route = self.routes.len() - 1;
    }

    pub(super) fn remove_route(&mut self) {
        if self.routes.len() > 1 {
            self.routes.remove(self.active_route);
            self.active_route = self.active_route.min(self.routes.len() - 1);
        }
    }

    pub(super) fn move_route(&mut self, up: bool) {
        let other = if up {
            self.active_route.checked_sub(1)
        } else if self.active_route + 1 < self.routes.len() {
            Some(self.active_route + 1)
        } else {
            None
        };
        if let Some(other) = other {
            self.routes.swap(self.active_route, other);
            self.active_route = other;
        }
    }

    pub(super) fn build_junction(
        &self,
    ) -> error_stack::Result<CreateJunction, ProcessorDraftError> {
        let name = JunctionName::parse(self.name.trim())
            .map_err(|_| Report::new(ProcessorDraftError::JunctionName))?;
        let branching = self.branching.build()?;
        let parts = self.build_parts()?;
        Ok(CreateJunction {
            name,
            from: parts.inputs,
            output_routes: parts.routes,
            branched_by: branching,
            mode: self.mode,
            filter_where: parts.filter,
            materialized_state: parts.state,
        })
    }

    pub(super) fn build_reingestor(
        &self,
    ) -> error_stack::Result<CreateReingestor, ProcessorDraftError> {
        let name = ReingestorName::parse(self.name.trim())
            .map_err(|_| Report::new(ProcessorDraftError::ReingestorName))?;
        let parts = self.build_parts()?;
        Ok(CreateReingestor {
            name,
            from: parts.inputs,
            output_routes: parts.routes,
            mode: self.mode,
            filter_where: parts.filter,
            materialized_state: parts.state,
        })
    }

    fn build_parts(&self) -> error_stack::Result<ProcessorParts, ProcessorDraftError> {
        if self.inputs.is_empty() {
            return Err(Report::new(ProcessorDraftError::Inputs));
        }
        let mut from = Vec::new();
        let mut input_where = Vec::new();
        let mut seen_inputs = BTreeSet::new();
        for (index, input) in self.inputs.iter().enumerate() {
            let number = index + 1;
            let relay = match &input.relay {
                Some(relay) => relay.current_name().ok_or_else(|| {
                    Report::new(ProcessorDraftError::InputChanged { index: number })
                })?,
                None => return Err(Report::new(ProcessorDraftError::Input { index: number })),
            };
            if !seen_inputs.insert(relay.clone()) {
                return Err(Report::new(ProcessorDraftError::DuplicateInput {
                    relay: relay.clone(),
                }));
            }
            from.push(relay.clone());
            if !input.where_clause.trim().is_empty() {
                input_where.push(ProcessorInputWhere {
                    relay: relay.clone(),
                    where_clause: parse_expression(input.where_clause.trim())
                        .change_context(ProcessorDraftError::InputWhere { index: number })?,
                });
            }
        }
        let mut inputs = ProcessorInputs::new(from, input_where);
        if let Some(collect) = &self.collect {
            if collect.collect_for.trim().is_empty() {
                return Err(Report::new(ProcessorDraftError::CollectFor));
            }
            inputs.collect_policy = Some(InputCollectPolicy {
                collect_for: collect.collect_for.trim().to_string(),
                max_batch_size: match collect.max_batch_size.as_deref() {
                    Some(size) if !size.trim().is_empty() => Some(size.trim().to_string()),
                    _ => None,
                },
            });
        }
        let filter = if self.filter.trim().is_empty() {
            None
        } else {
            Some(parse_expression(self.filter.trim()).change_context(ProcessorDraftError::Filter)?)
        };
        let mut state = Vec::new();
        let mut seen_state = BTreeSet::new();
        for (index, dependency) in self.state.iter().enumerate() {
            let built = dependency.build(index + 1)?;
            if !seen_state.insert(built.relay.clone()) {
                return Err(Report::new(ProcessorDraftError::DuplicateState {
                    relay: built.relay,
                }));
            }
            state.push(built);
        }
        if self.routes.is_empty() {
            return Err(Report::new(ProcessorDraftError::Routes));
        }
        let mut routes = Vec::new();
        for (index, route) in self.routes.iter().enumerate() {
            let built = match self.family {
                ProcessorFamily::Junction => route.build_preserving(),
                ProcessorFamily::Reingestor => route.build_reingestor(),
            };
            routes.push(built.map_err(|error| {
                let cause = error.current_context().clone();
                error.change_context(ProcessorDraftError::Route {
                    index: index + 1,
                    cause,
                })
            })?);
        }
        Ok(ProcessorParts {
            inputs,
            routes: ProcessorOutputs::new(routes),
            filter,
            state,
        })
    }
}

struct ProcessorParts {
    inputs: ProcessorInputs,
    routes: ProcessorOutputs,
    filter: Option<nervix_models::Expression>,
    state: Vec<MaterializedStateDependency>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(super) enum ProcessorDraftError {
    #[error("Junction name is invalid")]
    JunctionName,
    #[error("Reingestor name is invalid")]
    ReingestorName,
    #[error("Add at least one input relay")]
    Inputs,
    #[error("Input {index}: choose a relay")]
    Input { index: usize },
    #[error("Input {index}: the relay belongs to a changed context; select it again")]
    InputChanged { index: usize },
    #[error("Input relay `{relay}` is selected more than once")]
    DuplicateInput { relay: RelayName },
    #[error("Input {index}: enter a valid WHERE expression")]
    InputWhere { index: usize },
    #[error("Enter the COLLECT FOR duration")]
    CollectFor,
    #[error("Enter a valid node FILTER WHERE expression")]
    Filter,
    #[error("Choose UNBRANCHED or a named branch for this junction")]
    Branching,
    #[error("Choose the junction's named branch")]
    Branch,
    #[error("The selected branch belongs to a changed context; select it again")]
    BranchChanged,
    #[error("Materialized dependency {index}: choose a relay")]
    StateRelay { index: usize },
    #[error(
        "Materialized dependency {index}: the relay belongs to a changed context; select it again"
    )]
    StateRelayChanged { index: usize },
    #[error("Materialized dependency {index}: choose REQUIRED SKIP, REQUIRED WAIT, or DEFAULT")]
    StatePolicy { index: usize },
    #[error("Materialized relay `{relay}` is declared more than once")]
    DuplicateState { relay: RelayName },
    #[error("Materialized dependency {index} DEFAULT assignment {assignment}: {cause}")]
    StateDefault {
        index: usize,
        assignment: usize,
        cause: IngestRouteDraftError,
    },
    #[error("Add at least one output route")]
    Routes,
    #[error("Route {index}: {cause}")]
    Route {
        index: usize,
        cause: IngestRouteDraftError,
    },
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{
        BranchName, CreateStatement, FieldName, MaterializedStatePolicy, Model, ModelName,
        OutputBranch, RequestedResourceVersion, Statement,
    };
    use nervix_nspl::statement::parse_statement;

    use super::*;
    use crate::create_dialog::ingestor_route_draft::{
        FlushDraft, InheritDraft, MessageErrorDraft, RouteBranchDraft,
    };

    fn node(kind: ModelKind, name: &str) -> NodeRef {
        NodeRef::new(kind, ModelName::parse(name).assured("test name is valid"))
    }

    fn complete(family: ProcessorFamily) -> ProcessorDraft {
        let mut draft = ProcessorDraft::new(family);
        draft.name = "route_events".into();
        draft.select_input(&node(ModelKind::Relay, "incoming"));
        draft.routes[0].select_relay(&node(ModelKind::Relay, "outgoing"));
        draft.routes[0].inherit = InheritDraft::All;
        draft.routes[0].flush = FlushDraft::Immediate;
        draft.routes[0].message_error = MessageErrorDraft::Log;
        if family == ProcessorFamily::Junction {
            draft.choose_unbranched();
            draft.select_input(&node(ModelKind::Relay, "incoming"));
            draft.routes[0].select_relay(&node(ModelKind::Relay, "outgoing"));
        } else {
            draft.routes[0].branch.choose_preserve();
        }
        draft
    }

    #[test]
    fn junction_keeps_input_predicate_state_route_and_assignment_order() {
        let mut draft = complete(ProcessorFamily::Junction);
        draft.inputs[0].where_clause = "input.message != ''".into();
        draft.add_input();
        draft.select_input(&node(ModelKind::Relay, "incoming_b"));
        draft.inputs[1].where_clause = "input.message != 'x'".into();
        draft.collect = Some(InputCollectPolicy {
            collect_for: "25ms".into(),
            max_batch_size: Some("1MiB".into()),
        });
        draft.add_state();
        draft
            .active_state_mut()
            .assured("state exists")
            .select_relay(&node(ModelKind::Relay, "profile"));
        draft.active_state_mut().assured("state exists").policy = StatePolicyDraft::RequiredWait;
        draft.routes[0].add_assignment(FieldName::parse("result").assured("field is valid"));
        draft.routes[0].assignments[0].expression = "input.message".into();
        draft.add_route();
        draft.routes[1].select_relay(&node(ModelKind::Relay, "audit"));
        draft.routes[1].inherit = InheritDraft::All;
        draft.routes[1].flush = FlushDraft::Immediate;
        draft.routes[1].message_error = MessageErrorDraft::Ignore;
        let built = draft.build_junction().assured("complete junction builds");
        assert_eq!(
            built
                .from
                .from
                .iter()
                .map(RelayName::as_str)
                .collect::<Vec<_>>(),
            ["incoming", "incoming_b"]
        );
        assert_eq!(built.from.r#where.len(), 2);
        assert_eq!(
            built
                .from
                .collect_policy
                .assured("collection exists")
                .collect_for,
            "25ms"
        );
        assert_eq!(
            built
                .output_routes
                .routes
                .iter()
                .map(|route| route.relay.as_str())
                .collect::<Vec<_>>(),
            ["outgoing", "audit"]
        );
        assert_eq!(
            built.output_routes.routes[0].construction.assignments.len(),
            1
        );
        assert_eq!(built.materialized_state[0].relay.as_str(), "profile");
        assert_eq!(
            built.materialized_state[0].policy,
            MaterializedStatePolicy::RequiredWait
        );
    }

    #[test]
    fn processor_draft_round_trip_keeps_all_ordered_sections() {
        let mut draft = complete(ProcessorFamily::Junction);
        draft.add_input();
        draft.select_input(&node(ModelKind::Relay, "alternate"));
        draft.move_input(true);

        draft.add_state();
        let first = draft.active_state_mut().assured("state exists");
        first.select_relay(&node(ModelKind::Relay, "snapshot_a"));
        first.policy = StatePolicyDraft::RequiredSkip;
        draft.add_state();
        let second = draft.active_state_mut().assured("state exists");
        second.select_relay(&node(ModelKind::Relay, "snapshot_b"));
        second.policy = StatePolicyDraft::RequiredWait;
        draft.move_state(true);

        draft.routes[0].add_assignment(FieldName::parse("first").assured("valid field"));
        draft.routes[0].assignments[0].expression = "input.message".into();
        draft.routes[0].add_assignment(FieldName::parse("second").assured("valid field"));
        draft.routes[0].assignments[1].expression = "output.first".into();
        draft.add_route();
        draft.routes[1].select_relay(&node(ModelKind::Relay, "audit"));
        draft.routes[1].inherit = InheritDraft::All;
        draft.routes[1].flush = FlushDraft::Immediate;
        draft.routes[1].message_error = MessageErrorDraft::Log;
        draft.move_route(true);

        let built = draft.build_junction().assured("complete junction builds");
        assert_eq!(
            built
                .from
                .from
                .iter()
                .map(RelayName::as_str)
                .collect::<Vec<_>>(),
            ["alternate", "incoming"]
        );
        assert_eq!(
            built
                .materialized_state
                .iter()
                .map(|item| item.relay.as_str())
                .collect::<Vec<_>>(),
            ["snapshot_b", "snapshot_a"]
        );
        assert_eq!(
            built
                .output_routes
                .routes
                .iter()
                .map(|route| route.relay.as_str())
                .collect::<Vec<_>>(),
            ["audit", "outgoing"]
        );
        assert_eq!(
            built.output_routes.routes[1]
                .construction
                .assignments
                .iter()
                .map(|item| item.target.field.as_str())
                .collect::<Vec<_>>(),
            ["first", "second"]
        );

        let statement = Statement::Create(CreateStatement::new(
            Box::new(Model::<RequestedResourceVersion>::Junction(built)),
            false,
        ));
        let canonical = statement.to_canonical_nspl().assured("junction renders");
        assert_eq!(
            parse_statement(&canonical).assured("canonical junction parses"),
            statement
        );
    }

    #[test]
    fn reingestor_routes_preserve_or_repartition_independently() {
        let mut draft = complete(ProcessorFamily::Reingestor);
        draft.add_route();
        draft.routes[1].select_relay(&node(ModelKind::Relay, "branched"));
        draft.routes[1].inherit = InheritDraft::All;
        draft.routes[1].branch.choose_branched();
        draft.routes[1]
            .branch
            .select_branch(&node(ModelKind::Branch, "by_tenant"));
        draft.routes[1]
            .branch
            .add_assignment(FieldName::parse("tenant").assured("field is valid"));
        if let RouteBranchDraft::Branched { assignments, .. } = &mut draft.routes[1].branch {
            assignments[0].expression = "input.tenant".into();
        }
        draft.routes[1].flush = FlushDraft::Immediate;
        draft.routes[1].message_error = MessageErrorDraft::Log;
        let built = draft
            .build_reingestor()
            .assured("complete reingestor builds");
        assert_eq!(built.output_routes.routes[0].branch, None);
        assert!(
            matches!(&built.output_routes.routes[1].branch, Some(OutputBranch::BranchedBy { branch, assignments }) if branch == &BranchName::parse("by_tenant").assured("valid branch") && assignments.len() == 1)
        );
    }

    #[test]
    fn stale_and_duplicate_references_stay_invalid() {
        let mut draft = complete(ProcessorFamily::Reingestor);
        draft.add_input();
        draft.select_input(&node(ModelKind::Relay, "incoming"));
        assert!(matches!(
            draft.build_reingestor().unwrap_err().current_context(),
            ProcessorDraftError::DuplicateInput { .. }
        ));
        draft.select_input(&node(ModelKind::Relay, "other"));
        draft.invalidate_references();
        assert!(matches!(
            draft.build_reingestor().unwrap_err().current_context(),
            ProcessorDraftError::InputChanged { .. }
        ));
    }

    #[test]
    fn changing_the_first_input_invalidates_dependent_references() {
        let mut draft = complete(ProcessorFamily::Reingestor);
        draft.add_input();
        draft.select_input(&node(ModelKind::Relay, "second"));
        draft.add_state();
        let state = draft.active_state_mut().assured("state exists");
        state.select_relay(&node(ModelKind::Relay, "profile"));
        state.policy = StatePolicyDraft::RequiredSkip;

        draft.active_input = 0;
        draft.select_input(&node(ModelKind::Relay, "replacement"));
        assert!(draft.inputs[1].current_relay().is_none());
        assert!(draft.state[0].current_relay().is_none());
        assert!(draft.routes[0].current_relay().is_none());
        assert!(matches!(
            draft.build_reingestor().unwrap_err().current_context(),
            ProcessorDraftError::InputChanged { index: 2 }
        ));
    }

    #[test]
    fn junction_branch_and_state_policy_are_explicit() {
        let mut draft = ProcessorDraft::new(ProcessorFamily::Junction);
        draft.name = "route_events".into();
        assert!(matches!(
            draft.build_junction().unwrap_err().current_context(),
            ProcessorDraftError::Branching
        ));
        draft.choose_branched();
        assert!(matches!(
            draft.build_junction().unwrap_err().current_context(),
            ProcessorDraftError::Branch
        ));
        draft.select_branch(&node(ModelKind::Branch, "by_tenant"));
        assert!(matches!(
            draft.build_junction().unwrap_err().current_context(),
            ProcessorDraftError::Input { index: 1 }
        ));

        let mut complete = complete(ProcessorFamily::Junction);
        complete.add_state();
        complete
            .active_state_mut()
            .assured("state exists")
            .select_relay(&node(ModelKind::Relay, "profile"));
        assert!(matches!(
            complete.build_junction().unwrap_err().current_context(),
            ProcessorDraftError::StatePolicy { index: 1 }
        ));
        complete.active_state_mut().assured("state exists").policy =
            StatePolicyDraft::Default(vec![AssignmentDraft {
                field: Some(SelectedReference::chosen(
                    FieldName::parse("message").assured("valid field"),
                )),
                expression: "'fallback'".into(),
            }]);
        let built = complete.build_junction().assured("default state builds");
        assert!(
            matches!(built.materialized_state[0].policy, MaterializedStatePolicy::Default(ref rows) if rows.len() == 1)
        );

        complete.add_state();
        let state = complete.active_state_mut().assured("state exists");
        state.select_relay(&node(ModelKind::Relay, "profile"));
        state.policy = StatePolicyDraft::RequiredWait;
        assert!(matches!(
            complete.build_junction().unwrap_err().current_context(),
            ProcessorDraftError::DuplicateState { .. }
        ));
        complete
            .active_state_mut()
            .assured("state exists")
            .select_relay(&node(ModelKind::Relay, "other_profile"));
        complete.routes[0].flush = FlushDraft::Unselected;
        assert!(matches!(
            complete.build_junction().unwrap_err().current_context(),
            ProcessorDraftError::Route { index: 1, .. }
        ));
    }

    #[test]
    fn reordering_later_inputs_and_reselecting_state_keeps_independent_choices() {
        let mut draft = complete(ProcessorFamily::Reingestor);
        draft.add_input();
        draft.select_input(&node(ModelKind::Relay, "second"));
        draft.add_input();
        draft.select_input(&node(ModelKind::Relay, "third"));
        draft.add_state();
        let state = draft.active_state_mut().assured("state exists");
        state.select_relay(&node(ModelKind::Relay, "profile"));
        state.policy = StatePolicyDraft::Default(vec![AssignmentDraft::selected(
            FieldName::parse("message").assured("valid field"),
        )]);

        draft.move_input(true);
        assert_eq!(
            draft
                .current_first_input()
                .assured("first input exists")
                .as_str(),
            "incoming"
        );
        assert_eq!(
            draft
                .inputs
                .iter()
                .map(|input| input.current_relay().assured("input exists").as_str())
                .collect::<Vec<_>>(),
            ["incoming", "third", "second"]
        );
        assert_eq!(
            draft
                .active_route()
                .and_then(IngestRouteDraft::current_relay)
                .assured("route relay exists")
                .as_str(),
            "outgoing"
        );
        let state = draft.active_state_mut().assured("state exists");
        state.select_relay(&node(ModelKind::Relay, "profile"));
        let StatePolicyDraft::Default(assignments) = &state.policy else {
            panic!("default policy remains selected");
        };
        assert!(
            assignments[0]
                .field
                .as_ref()
                .assured("field exists")
                .is_current()
        );
        draft.active_input = 0;
        draft.move_input(false);
        assert_eq!(
            draft
                .current_first_input()
                .assured("first input exists")
                .as_str(),
            "third"
        );
        assert_eq!(
            draft
                .active_route()
                .and_then(IngestRouteDraft::current_relay)
                .assured("route relay exists")
                .as_str(),
            "outgoing"
        );
        assert_eq!(
            draft
                .active_state()
                .and_then(StateDependencyDraft::current_relay)
                .assured("state relay exists")
                .as_str(),
            "profile"
        );
    }
}
