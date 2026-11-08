//! Incomplete visual junction, deduplicator, reorderer and reingestor state until it becomes a
//! semantic Model.
//!
//! Layer: edges.
//!
//! - **Owns.** Ordered browser drafts for processor inputs, key expressions, state dependencies,
//!   routes, and branch-boundary choices, and their conversion to current semantic Models.
//! - **Depends on.** Typed vocabulary, expression parsing, and shared route drafts.
//! - **Must not know.** Registry state, runtime execution, or session transport.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::collections::BTreeSet;

use error_stack::{Report, ResultExt as _};
use nervix_models::{
    AckMode, BranchName, BranchSelection, CreateDeduplicator, CreateJunction, CreateReingestor,
    CreateReorderer, DeduplicatorName, Expression, InputCollectPolicy, JunctionName,
    MaterializedStateDependency, MaterializedStatePolicy, Model, ModelKind, NodeRef,
    ProcessorInputWhere, ProcessorInputs, ProcessorOutputs, ReingestorName, RelayName,
    ReordererName,
};
use nervix_plexus::parse_expression;
use strum::Display;
use thiserror::Error;

use super::{
    SelectedReference,
    ingestor_route_draft::{AssignmentDraft, IngestRouteDraft, IngestRouteDraftError},
};

/// The processor kinds the shared visual processor form creates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
#[strum(serialize_all = "lowercase")]
pub(super) enum ProcessorFamily {
    Junction,
    Deduplicator,
    Reorderer,
    Reingestor,
}

impl ProcessorFamily {
    /// Whether one node-wide `BRANCHED BY` or `UNBRANCHED` declaration governs every input and
    /// route. A reingestor instead preserves, drops or constructs the branch route by route.
    pub(super) fn declares_node_branch(self) -> bool {
        match self {
            Self::Junction | Self::Deduplicator | Self::Reorderer => true,
            Self::Reingestor => false,
        }
    }

    /// The ordered expression clause the family writes before its `MAX TIME`, when it keys or
    /// orders its branch-local state by one.
    pub(super) fn key_clause(self) -> Option<KeyClause> {
        match self {
            Self::Deduplicator => Some(KeyClause::DeduplicateOn),
            Self::Reorderer => Some(KeyClause::By),
            Self::Junction | Self::Reingestor => None,
        }
    }

    pub(super) fn name_label(self) -> &'static str {
        match self {
            Self::Junction => "Junction name",
            Self::Deduplicator => "Deduplicator name",
            Self::Reorderer => "Reorderer name",
            Self::Reingestor => "Reingestor name",
        }
    }

    /// The label of the node-wide branch choice, for the families that declare one.
    pub(super) fn node_branch_label(self) -> Option<&'static str> {
        match self {
            Self::Junction => Some("Junction branch"),
            Self::Deduplicator => Some("Deduplicator branch"),
            Self::Reorderer => Some("Reorderer branch"),
            Self::Reingestor => None,
        }
    }
}

/// The ordered expression clause of a keyed processor, spelled as Plexus writes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
pub(super) enum KeyClause {
    #[strum(serialize = "DEDUPLICATE ON")]
    DeduplicateOn,
    #[strum(serialize = "BY")]
    By,
}

/// The ordered key or ordering expressions and the holding time of a deduplicator or reorderer,
/// kept exactly as typed until the draft builds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct KeyedDraft {
    pub(super) expressions: Vec<String>,
    pub(super) active: usize,
    pub(super) max_time: String,
}

impl Default for KeyedDraft {
    /// One empty expression to type into, as a keyed processor needs at least one.
    fn default() -> Self {
        Self {
            expressions: vec![String::new()],
            active: 0,
            max_time: String::new(),
        }
    }
}

impl KeyedDraft {
    pub(super) fn active_expression(&self) -> Option<&String> {
        self.expressions.get(self.active)
    }

    pub(super) fn active_expression_mut(&mut self) -> Option<&mut String> {
        self.expressions.get_mut(self.active)
    }

    pub(super) fn add(&mut self) {
        self.expressions.push(String::new());
        self.active = self.expressions.len() - 1;
    }

    /// Removes the active expression unless it is the only one.
    pub(super) fn remove_active(&mut self) {
        if self.expressions.len() > 1 {
            self.expressions.remove(self.active);
            self.active = self.active.min(self.expressions.len() - 1);
        }
    }

    pub(super) fn move_active(&mut self, up: bool) {
        let other = if up {
            self.active.checked_sub(1)
        } else if self.active + 1 < self.expressions.len() {
            Some(self.active + 1)
        } else {
            None
        };
        if let Some(other) = other {
            self.expressions.swap(self.active, other);
            self.active = other;
        }
    }

    fn build(&self, clause: KeyClause) -> error_stack::Result<KeyedParts, ProcessorDraftError> {
        if self.expressions.is_empty() {
            return Err(Report::new(ProcessorDraftError::Keys { clause }));
        }
        let mut expressions = Vec::with_capacity(self.expressions.len());
        for (index, text) in self.expressions.iter().enumerate() {
            let expression = parse_expression(text.trim()).change_context(
                ProcessorDraftError::KeyExpression {
                    clause,
                    index: index + 1,
                },
            )?;
            expressions.push(expression);
        }
        let max_time = self.max_time.trim();
        if max_time.is_empty() {
            return Err(Report::new(ProcessorDraftError::MaxTime));
        }
        Ok(KeyedParts {
            expressions,
            max_time: max_time.to_string(),
        })
    }
}

/// The checked expressions and holding time of a keyed processor.
struct KeyedParts {
    expressions: Vec<Expression>,
    max_time: String,
}

/// The kind of processor a draft creates, with what only that kind declares.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ProcessorStage {
    Junction,
    Deduplicator(KeyedDraft),
    Reorderer(KeyedDraft),
    Reingestor,
}

impl ProcessorStage {
    fn new(family: ProcessorFamily) -> Self {
        match family {
            ProcessorFamily::Junction => Self::Junction,
            ProcessorFamily::Deduplicator => Self::Deduplicator(KeyedDraft::default()),
            ProcessorFamily::Reorderer => Self::Reorderer(KeyedDraft::default()),
            ProcessorFamily::Reingestor => Self::Reingestor,
        }
    }

    fn family(&self) -> ProcessorFamily {
        match self {
            Self::Junction => ProcessorFamily::Junction,
            Self::Deduplicator(_) => ProcessorFamily::Deduplicator,
            Self::Reorderer(_) => ProcessorFamily::Reorderer,
            Self::Reingestor => ProcessorFamily::Reingestor,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct InputDraft {
    pub(super) relay: Option<SelectedReference<RelayName>>,
    pub(super) where_clause: String,
}

impl InputDraft {
    pub(super) fn select_relay(&mut self, node: &NodeRef) {
        if node.kind == ModelKind::Relay {
            self.relay = Some(SelectedReference::chosen(RelayName::from(&node.identifier)));
        }
    }

    pub(super) fn current_relay(&self) -> Option<&RelayName> {
        self.relay
            .as_ref()
            .and_then(SelectedReference::current_name)
    }

    pub(super) fn invalidate(&mut self) {
        if let Some(relay) = &mut self.relay {
            relay.invalidate();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) enum NodeBranchDraft {
    #[default]
    Unselected,
    Unbranched,
    Branched(Option<SelectedReference<BranchName>>),
}

impl NodeBranchDraft {
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

    fn build(
        &self,
        family: ProcessorFamily,
    ) -> error_stack::Result<BranchSelection, ProcessorDraftError> {
        match self {
            Self::Unselected => Err(Report::new(ProcessorDraftError::Branching { family })),
            Self::Unbranched => Ok(BranchSelection::Unbranched),
            Self::Branched(None) => Err(Report::new(ProcessorDraftError::Branch { family })),
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

    pub(super) fn invalidate(&mut self) {
        if let Some(relay) = &mut self.relay {
            relay.invalidate();
        }
        if let StatePolicyDraft::Default(assignments) = &mut self.policy {
            for assignment in assignments {
                assignment.invalidate();
            }
        }
    }

    pub(super) fn build(
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
                    built.push({
                        let mapping_input = assignment.build();
                        match mapping_input {
                            Ok(mapped_value) => Ok(mapped_value),
                            Err(error) => Err({
                                let cause = error.current_context().clone();
                                error.change_context(ProcessorDraftError::StateDefault {
                                    index,
                                    assignment: assignment_index + 1,
                                    cause,
                                })
                            }),
                        }
                    }?);
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
    pub(super) branching: NodeBranchDraft,
    pub(super) state: Vec<StateDependencyDraft>,
    pub(super) active_state: usize,
    pub(super) routes: Vec<IngestRouteDraft>,
    pub(super) active_route: usize,
    stage: ProcessorStage,
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
            branching: NodeBranchDraft::Unselected,
            state: Vec::new(),
            active_state: 0,
            routes: vec![IngestRouteDraft::default()],
            active_route: 0,
            stage: ProcessorStage::new(family),
        }
    }

    pub(super) fn family(&self) -> ProcessorFamily {
        self.stage.family()
    }

    /// The ordered expressions and holding time of a deduplicator or reorderer draft.
    pub(super) fn keyed(&self) -> Option<&KeyedDraft> {
        match &self.stage {
            ProcessorStage::Deduplicator(keyed) | ProcessorStage::Reorderer(keyed) => Some(keyed),
            ProcessorStage::Junction | ProcessorStage::Reingestor => None,
        }
    }

    pub(super) fn keyed_mut(&mut self) -> Option<&mut KeyedDraft> {
        match &mut self.stage {
            ProcessorStage::Deduplicator(keyed) | ProcessorStage::Reorderer(keyed) => Some(keyed),
            ProcessorStage::Junction | ProcessorStage::Reingestor => None,
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
        if self.branching != NodeBranchDraft::Unbranched {
            self.branching.choose_unbranched();
            self.invalidate_relays();
        }
    }

    pub(super) fn choose_branched(&mut self) {
        if !matches!(self.branching, NodeBranchDraft::Branched(_)) {
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

    /// The complete semantic Model of the processor kind this draft creates, or the first missing
    /// or invalid choice in form order.
    pub(super) fn build(&self) -> error_stack::Result<Model, ProcessorDraftError> {
        match &self.stage {
            ProcessorStage::Junction => Ok(Model::Junction(self.build_junction()?)),
            ProcessorStage::Deduplicator(keyed) => {
                Ok(Model::Deduplicator(self.build_deduplicator(keyed)?))
            }
            ProcessorStage::Reorderer(keyed) => Ok(Model::Reorderer(self.build_reorderer(keyed)?)),
            ProcessorStage::Reingestor => Ok(Model::Reingestor(self.build_reingestor()?)),
        }
    }

    fn build_junction(&self) -> error_stack::Result<CreateJunction, ProcessorDraftError> {
        let name = JunctionName::parse(self.name.trim())
            .map_err(|cause| cause.change_context(ProcessorDraftError::JunctionName))?;
        let branching = self.branching.build(self.family())?;
        let input = self.build_input()?;
        let output = self.build_output()?;
        Ok(CreateJunction {
            name,
            from: input.inputs,
            output_routes: output.routes,
            branched_by: branching,
            mode: self.mode,
            filter_where: input.filter,
            materialized_state: output.state,
        })
    }

    fn build_deduplicator(
        &self,
        keyed: &KeyedDraft,
    ) -> error_stack::Result<CreateDeduplicator, ProcessorDraftError> {
        let name = DeduplicatorName::parse(self.name.trim())
            .map_err(|cause| cause.change_context(ProcessorDraftError::DeduplicatorName))?;
        let branching = self.branching.build(self.family())?;
        let input = self.build_input()?;
        let keys = keyed.build(KeyClause::DeduplicateOn)?;
        let output = self.build_output()?;
        Ok(CreateDeduplicator {
            name,
            from: input.inputs,
            output_routes: output.routes,
            branched_by: branching,
            deduplicate_on: keys.expressions,
            max_time: keys.max_time,
            mode: self.mode,
            filter_where: input.filter,
            materialized_state: output.state,
        })
    }

    fn build_reorderer(
        &self,
        keyed: &KeyedDraft,
    ) -> error_stack::Result<CreateReorderer, ProcessorDraftError> {
        let name = ReordererName::parse(self.name.trim())
            .map_err(|cause| cause.change_context(ProcessorDraftError::ReordererName))?;
        let branching = self.branching.build(self.family())?;
        let input = self.build_input()?;
        let keys = keyed.build(KeyClause::By)?;
        let output = self.build_output()?;
        Ok(CreateReorderer {
            name,
            from: input.inputs,
            output_routes: output.routes,
            branched_by: branching,
            order_by: keys.expressions,
            max_time: keys.max_time,
            mode: self.mode,
            filter_where: input.filter,
            materialized_state: output.state,
        })
    }

    fn build_reingestor(&self) -> error_stack::Result<CreateReingestor, ProcessorDraftError> {
        let name = ReingestorName::parse(self.name.trim())
            .map_err(|cause| cause.change_context(ProcessorDraftError::ReingestorName))?;
        let input = self.build_input()?;
        let output = self.build_output()?;
        Ok(CreateReingestor {
            name,
            from: input.inputs,
            output_routes: output.routes,
            mode: self.mode,
            filter_where: input.filter,
            materialized_state: output.state,
        })
    }

    /// The ordered inputs with their predicates and collection, and the node filter.
    fn build_input(&self) -> error_stack::Result<ProcessorInputParts, ProcessorDraftError> {
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
        Ok(ProcessorInputParts { inputs, filter })
    }

    /// The ordered materialized dependencies and output routes.
    fn build_output(&self) -> error_stack::Result<ProcessorOutputParts, ProcessorDraftError> {
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
        let preserves_node_branch = self.family().declares_node_branch();
        let mut routes = Vec::new();
        for (index, route) in self.routes.iter().enumerate() {
            let built = if preserves_node_branch {
                route.build_preserving()
            } else {
                route.build_reingestor()
            };
            routes.push({
                let mapping_input = built;
                match mapping_input {
                    Ok(mapped_value) => Ok(mapped_value),
                    Err(error) => Err({
                        let cause = error.current_context().clone();
                        error.change_context(ProcessorDraftError::Route {
                            index: index + 1,
                            cause,
                        })
                    }),
                }
            }?);
        }
        Ok(ProcessorOutputParts {
            state,
            routes: ProcessorOutputs::new(routes),
        })
    }
}

/// The checked inputs and node filter every processor family declares.
struct ProcessorInputParts {
    inputs: ProcessorInputs,
    filter: Option<Expression>,
}

/// The checked materialized dependencies and output routes every processor family declares.
struct ProcessorOutputParts {
    state: Vec<MaterializedStateDependency>,
    routes: ProcessorOutputs,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(super) enum ProcessorDraftError {
    #[error("Junction name is invalid")]
    JunctionName,
    #[error("Deduplicator name is invalid")]
    DeduplicatorName,
    #[error("Reorderer name is invalid")]
    ReordererName,
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
    #[error("Choose UNBRANCHED or a named branch for this {family}")]
    Branching { family: ProcessorFamily },
    #[error("Choose the {family}'s named branch")]
    Branch { family: ProcessorFamily },
    #[error("Add at least one {clause} expression")]
    Keys { clause: KeyClause },
    #[error("{clause} expression {index}: enter a valid expression")]
    KeyExpression { clause: KeyClause, index: usize },
    #[error("Enter the MAX TIME duration")]
    MaxTime,
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
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{
        BinaryOperator, BranchName, CreateStatement, Expression, FieldName, FieldReference,
        FieldScope, Literal, MaterializedStatePolicy, Model, ModelName, OutputBranch,
        RequestedResourceVersion, Statement,
    };
    use nervix_plexus::statement::parse_statement;

    use super::*;
    use crate::create_dialog::ingestor_route_draft::{
        FlushDraft, InheritDraft, MessageErrorDraft, RouteBranchDraft,
    };

    fn node(kind: ModelKind, name: &str) -> NodeRef {
        NodeRef::new(kind, ModelName::parse(name).assured("test name is valid"))
    }

    const FAMILIES: [ProcessorFamily; 4] = [
        ProcessorFamily::Junction,
        ProcessorFamily::Deduplicator,
        ProcessorFamily::Reorderer,
        ProcessorFamily::Reingestor,
    ];

    fn complete(family: ProcessorFamily) -> ProcessorDraft {
        let mut draft = ProcessorDraft::new(family);
        draft.name = "route_events".into();
        draft.select_input(&node(ModelKind::Relay, "incoming"));
        draft.routes[0].select_relay(&node(ModelKind::Relay, "outgoing"));
        draft.routes[0].inherit = InheritDraft::All;
        draft.routes[0].flush = FlushDraft::Immediate;
        draft.routes[0].message_error = MessageErrorDraft::Log;
        if family.declares_node_branch() {
            draft.choose_unbranched();
            draft.select_input(&node(ModelKind::Relay, "incoming"));
            draft.routes[0].select_relay(&node(ModelKind::Relay, "outgoing"));
        } else {
            draft.routes[0].branch.choose_preserve();
        }
        if let Some(keyed) = draft.keyed_mut() {
            keyed.expressions = vec!["input.message".into()];
            keyed.max_time = "10m".into();
        }
        draft
    }

    fn statement(draft: &ProcessorDraft) -> Statement {
        let model: Model<RequestedResourceVersion> =
            draft.build().assured("the draft is complete").into();
        Statement::Create(CreateStatement::new(Box::new(model), draft.if_not_exists))
    }

    #[test]
    fn processor_previews_render_input_acknowledgement_after_collection() {
        for family in FAMILIES {
            for mode in [AckMode::Attached, AckMode::Detached] {
                for collect in [false, true] {
                    let mut draft = complete(family);
                    if family == ProcessorFamily::Reingestor {
                        draft.routes[0].branch.choose_unbranched();
                    }
                    draft.mode = mode;
                    let collection = if collect {
                        draft.collect = Some(InputCollectPolicy {
                            collect_for: "25ms".into(),
                            max_batch_size: Some("1MiB".into()),
                        });
                        " COLLECT FOR 25ms MAX BATCH SIZE 1MiB"
                    } else {
                        ""
                    };
                    let statement = statement(&draft);
                    let canonical = statement.to_canonical_plexus().assured("the draft renders");
                    let input_contract =
                        format!("\n  FROM incoming{collection}\n  {}\n", mode.as_ref());
                    assert!(canonical.contains(&input_contract), "{canonical}");
                    assert_eq!(
                        parse_statement(&canonical).assured("the preview is executable Plexus"),
                        statement
                    );
                }
            }
        }
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
        let canonical = statement.to_canonical_plexus().assured("junction renders");
        assert_eq!(
            parse_statement(&canonical).assured("canonical junction parses"),
            statement
        );
    }

    #[test]
    fn input_predicates_and_filters_read_a_backslash_in_a_string_literal_verbatim() {
        let mut draft = complete(ProcessorFamily::Junction);
        draft.inputs[0].where_clause = r"input.message = 'a\nb'".into();
        draft.filter = r#"input.message != "c\'d""#.into();
        let built = draft.build_junction().assured("complete junction builds");
        let message = Expression::Field(FieldReference::scoped(
            FieldScope::Input,
            FieldName::parse("message").assured("valid field"),
        ));
        assert_eq!(
            built.from.r#where[0].where_clause,
            Expression::Binary {
                operator: BinaryOperator::Equal,
                left: Box::new(message.clone()),
                right: Box::new(Expression::Literal(Literal::String(r"a\nb".to_string()))),
            }
        );
        assert_eq!(
            built.filter_where,
            Some(Expression::Binary {
                operator: BinaryOperator::NotEqual,
                left: Box::new(message),
                right: Box::new(Expression::Literal(Literal::String(r"c\'d".to_string()))),
            })
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
        assert_eq!(
            draft.build_junction().unwrap_err().current_context(),
            &ProcessorDraftError::Branching {
                family: ProcessorFamily::Junction
            }
        );
        draft.choose_branched();
        assert_eq!(
            draft.build_junction().unwrap_err().current_context(),
            &ProcessorDraftError::Branch {
                family: ProcessorFamily::Junction
            }
        );
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
                ..AssignmentDraft::default()
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

    #[test]
    fn each_family_builds_its_own_model_kind_and_only_keyed_families_hold_keys() {
        for family in FAMILIES {
            let draft = complete(family);
            let expected = match family {
                ProcessorFamily::Junction => ModelKind::Junction,
                ProcessorFamily::Deduplicator => ModelKind::Deduplicator,
                ProcessorFamily::Reorderer => ModelKind::Reorderer,
                ProcessorFamily::Reingestor => ModelKind::Reingestor,
            };
            assert_eq!(draft.family(), family);
            assert_eq!(
                draft.build().assured("the draft is complete").kind(),
                expected
            );
            assert_eq!(draft.keyed().is_some(), family.key_clause().is_some());
        }
    }

    #[test]
    fn keyed_processors_keep_expression_order_and_holding_time() {
        for family in [ProcessorFamily::Deduplicator, ProcessorFamily::Reorderer] {
            let clause = family
                .key_clause()
                .assured("deduplicators and reorderers write a key clause");
            let mut draft = complete(family);
            let keyed = draft.keyed_mut().assured("a keyed family holds its keys");
            keyed.expressions = vec!["input.tenant".into()];
            keyed.add();
            *keyed
                .active_expression_mut()
                .assured("the added expression is active") = " lower(input.message) ".into();
            keyed.add();
            *keyed
                .active_expression_mut()
                .assured("the added expression is active") = "input.sequence".into();
            keyed.move_active(true);
            keyed.max_time = " 30s ".into();
            assert_eq!(keyed.active, 1);

            let statement = statement(&draft);
            let canonical = statement
                .to_canonical_plexus()
                .assured("keyed processors render");
            let keys = format!(
                "\n  {clause} input.tenant, input.sequence, lower(input.message)\n  MAX TIME \
                 30s\n  UNBRANCHED\n"
            );
            assert!(canonical.contains(&keys), "{canonical}");
            assert_eq!(
                parse_statement(&canonical).assured("the preview is executable Plexus"),
                statement
            );
        }
    }

    #[test]
    fn keyed_processors_require_valid_expressions_and_a_holding_time() {
        for family in [ProcessorFamily::Deduplicator, ProcessorFamily::Reorderer] {
            let clause = family
                .key_clause()
                .assured("deduplicators and reorderers write a key clause");
            let mut draft = complete(family);
            let refusal = |draft: &ProcessorDraft| {
                draft
                    .build()
                    .err()
                    .assured("the draft is incomplete")
                    .current_context()
                    .clone()
            };

            let keyed = draft.keyed_mut().assured("a keyed family holds its keys");
            keyed.expressions = vec!["input.tenant".into(), "  ".into()];
            assert_eq!(
                refusal(&draft),
                ProcessorDraftError::KeyExpression { clause, index: 2 }
            );
            let keyed = draft.keyed_mut().assured("a keyed family holds its keys");
            keyed.expressions[1] = "input.tenant +".into();
            assert_eq!(
                refusal(&draft),
                ProcessorDraftError::KeyExpression { clause, index: 2 }
            );
            let keyed = draft.keyed_mut().assured("a keyed family holds its keys");
            keyed.expressions[1] = "input.sequence".into();
            keyed.max_time = "  ".into();
            assert_eq!(refusal(&draft), ProcessorDraftError::MaxTime);
            let keyed = draft.keyed_mut().assured("a keyed family holds its keys");
            keyed.expressions.clear();
            assert_eq!(refusal(&draft), ProcessorDraftError::Keys { clause });
        }
    }

    #[test]
    fn the_last_key_expression_stays_editable_and_moves_stay_in_bounds() {
        let mut keyed = KeyedDraft::default();
        keyed.remove_active();
        assert_eq!(keyed.expressions, vec![String::new()]);
        keyed.move_active(true);
        keyed.move_active(false);
        assert_eq!(keyed.active, 0);
        keyed.add();
        keyed.add();
        assert_eq!(keyed.active, 2);
        keyed.move_active(false);
        assert_eq!(keyed.active, 2);
        *keyed
            .active_expression_mut()
            .assured("the last expression is active") = "input.last".into();
        keyed.active = 0;
        keyed.move_active(false);
        assert_eq!(keyed.active, 1);
        assert_eq!(keyed.active_expression().map(String::as_str), Some(""));
        keyed.active = 2;
        keyed.remove_active();
        assert_eq!(keyed.active, 1);
        assert_eq!(keyed.expressions.len(), 2);
    }

    #[test]
    fn a_changed_scope_keeps_typed_keys_and_requires_references_again() {
        let mut draft = complete(ProcessorFamily::Deduplicator);
        draft.invalidate_references();
        let keyed = draft.keyed().assured("a deduplicator holds its keys");
        assert_eq!(keyed.expressions, vec!["input.message".to_string()]);
        assert_eq!(keyed.max_time, "10m");
        assert_eq!(
            draft
                .build()
                .err()
                .assured("its input belongs to the earlier scope")
                .current_context(),
            &ProcessorDraftError::InputChanged { index: 1 }
        );
    }

    #[test]
    fn processor_draft_errors_name_their_family_and_clause() {
        assert_eq!(
            ProcessorDraftError::Branching {
                family: ProcessorFamily::Junction
            }
            .to_string(),
            "Choose UNBRANCHED or a named branch for this junction"
        );
        assert_eq!(
            ProcessorDraftError::Branching {
                family: ProcessorFamily::Deduplicator
            }
            .to_string(),
            "Choose UNBRANCHED or a named branch for this deduplicator"
        );
        assert_eq!(
            ProcessorDraftError::Branch {
                family: ProcessorFamily::Reorderer
            }
            .to_string(),
            "Choose the reorderer's named branch"
        );
        assert_eq!(
            ProcessorDraftError::KeyExpression {
                clause: KeyClause::DeduplicateOn,
                index: 1
            }
            .to_string(),
            "DEDUPLICATE ON expression 1: enter a valid expression"
        );
        assert_eq!(
            ProcessorDraftError::Keys {
                clause: KeyClause::By
            }
            .to_string(),
            "Add at least one BY expression"
        );
    }
}
