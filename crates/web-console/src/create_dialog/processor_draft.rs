//! Incomplete visual junction, deduplicator, reorderer, window processor, correlator and
//! reingestor state until it becomes a semantic Model.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser drafts for processor key expressions, node branches, state dependencies,
//!   routes, and branch-boundary choices, composed with their ordered input lists and family
//!   stages, the scoped references their pickers insert, and their conversion to current semantic
//!   Models.
//! - **Depends on.** Typed vocabulary, expression parsing, and the shared input list, window,
//!   correlator and route drafts.
//! - **Must not know.** Registry state, runtime execution, or session transport.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::{collections::BTreeSet, num::NonZeroU64};

use error_stack::{Report, ResultExt as _};
use nervix_models::{
    AckMode, BranchName, BranchSelection, CorrelatorName, CreateCorrelator, CreateDeduplicator,
    CreateJunction, CreateReingestor, CreateReorderer, CreateWindowProcessor, DeduplicatorName,
    Expression, FieldName, FieldReference, FieldScope, JunctionName, MaterializedStateDependency,
    MaterializedStatePolicy, Model, ModelKind, NodeRef, ProcessorInputs, ProcessorOutputs,
    ReingestorName, RelayName, ReordererName, WindowProcessorName,
};
use nervix_plexus::parse_expression;
use strum::Display;
use thiserror::Error;

use super::{
    SelectedReference,
    ingestor_route_draft::{
        AssignmentDraft, IngestRouteDraft, IngestRouteDraftError, SetOnlyEmission,
    },
    inserted_reference::InsertedReference,
    processor_correlator_draft::{CorrelationSide, CorrelatorDraft},
    processor_input_draft::{InputList, InputListDraft},
    processor_window_draft::{WindowBoundClause, WindowDraft},
};

/// The processor kinds the shared visual processor form creates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
#[strum(serialize_all = "lowercase")]
pub(super) enum ProcessorFamily {
    Junction,
    Deduplicator,
    Reorderer,
    #[strum(serialize = "window processor")]
    Window,
    Correlator,
    Reingestor,
}

impl ProcessorFamily {
    /// Whether one node-wide `BRANCHED BY` or `UNBRANCHED` declaration governs every input and
    /// route. A reingestor instead preserves, drops or constructs the branch route by route.
    pub(super) fn declares_node_branch(self) -> bool {
        match self {
            Self::Junction
            | Self::Deduplicator
            | Self::Reorderer
            | Self::Window
            | Self::Correlator => true,
            Self::Reingestor => false,
        }
    }

    /// The ordered expression clause the family writes before its `MAX TIME`, when it keys or
    /// orders its branch-local state by one.
    pub(super) fn key_clause(self) -> Option<KeyClause> {
        match self {
            Self::Deduplicator => Some(KeyClause::DeduplicateOn),
            Self::Reorderer => Some(KeyClause::By),
            Self::Junction | Self::Window | Self::Correlator | Self::Reingestor => None,
        }
    }

    pub(super) fn name_label(self) -> &'static str {
        match self {
            Self::Junction => "Junction name",
            Self::Deduplicator => "Deduplicator name",
            Self::Reorderer => "Reorderer name",
            Self::Window => "Window processor name",
            Self::Correlator => "Correlator name",
            Self::Reingestor => "Reingestor name",
        }
    }

    /// The label of the node-wide branch choice, for the families that declare one.
    pub(super) fn node_branch_label(self) -> Option<&'static str> {
        match self {
            Self::Junction => Some("Junction branch"),
            Self::Deduplicator => Some("Deduplicator branch"),
            Self::Reorderer => Some("Reorderer branch"),
            Self::Window => Some("Window processor branch"),
            Self::Correlator => Some("Correlator branch"),
            Self::Reingestor => None,
        }
    }

    /// The input list the draft's primary relays form: a correlator's left side, and the `FROM`
    /// list of every other family.
    pub(super) fn primary_input_list(self) -> InputList {
        match self {
            Self::Correlator => InputList::Left,
            Self::Junction
            | Self::Deduplicator
            | Self::Reorderer
            | Self::Window
            | Self::Reingestor => InputList::From,
        }
    }

    /// Whether the family also reads a `RIGHT FROM` list, which only a correlator does.
    pub(super) fn reads_right_side(self) -> bool {
        match self {
            Self::Correlator => true,
            Self::Junction
            | Self::Deduplicator
            | Self::Reorderer
            | Self::Window
            | Self::Reingestor => false,
        }
    }

    /// Whether the family declares a node-wide `FILTER WHERE`. A correlator filters each side on
    /// its own.
    pub(super) fn filters_input(self) -> bool {
        !self.reads_right_side()
    }

    pub(super) fn route_contract(self) -> RouteContract {
        match self {
            Self::Junction | Self::Deduplicator | Self::Reorderer | Self::Reingestor => {
                RouteContract::Transforming
            }
            Self::Window => RouteContract::WindowSetOnly,
            Self::Correlator => RouteContract::FlushedSetOnly,
        }
    }
}

/// What a processor family's routes construct, and when they emit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RouteContract {
    /// Ordered `INHERIT`, `SET`, `WHERE` and `INVOKE`, emitted on the route's flush policy.
    Transforming,
    /// `SET` and `WHERE` alone, emitted on the route's flush policy.
    FlushedSetOnly,
    /// `SET` and `WHERE` alone, emitted on the window's `WIDTH` and `STEP`.
    WindowSetOnly,
}

impl RouteContract {
    /// Whether a route inherits fields and invokes functions besides its `SET` and `WHERE`.
    pub(super) fn transforms(self) -> bool {
        match self {
            Self::Transforming => true,
            Self::FlushedSetOnly | Self::WindowSetOnly => false,
        }
    }

    /// Whether a route declares its own flush policy.
    pub(super) fn flushes(self) -> bool {
        match self {
            Self::Transforming | Self::FlushedSetOnly => true,
            Self::WindowSetOnly => false,
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
    Window(WindowDraft),
    Correlator(CorrelatorDraft),
    Reingestor,
}

impl ProcessorStage {
    fn new(family: ProcessorFamily) -> Self {
        match family {
            ProcessorFamily::Junction => Self::Junction,
            ProcessorFamily::Deduplicator => Self::Deduplicator(KeyedDraft::default()),
            ProcessorFamily::Reorderer => Self::Reorderer(KeyedDraft::default()),
            ProcessorFamily::Window => Self::Window(WindowDraft::default()),
            ProcessorFamily::Correlator => Self::Correlator(CorrelatorDraft::default()),
            ProcessorFamily::Reingestor => Self::Reingestor,
        }
    }

    fn family(&self) -> ProcessorFamily {
        match self {
            Self::Junction => ProcessorFamily::Junction,
            Self::Deduplicator(_) => ProcessorFamily::Deduplicator,
            Self::Reorderer(_) => ProcessorFamily::Reorderer,
            Self::Window(_) => ProcessorFamily::Window,
            Self::Correlator(_) => ProcessorFamily::Correlator,
            Self::Reingestor => ProcessorFamily::Reingestor,
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
    pub(super) from: InputListDraft,
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
            from: InputListDraft::default(),
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
            ProcessorStage::Junction
            | ProcessorStage::Window(_)
            | ProcessorStage::Correlator(_)
            | ProcessorStage::Reingestor => None,
        }
    }

    pub(super) fn keyed_mut(&mut self) -> Option<&mut KeyedDraft> {
        match &mut self.stage {
            ProcessorStage::Deduplicator(keyed) | ProcessorStage::Reorderer(keyed) => Some(keyed),
            ProcessorStage::Junction
            | ProcessorStage::Window(_)
            | ProcessorStage::Correlator(_)
            | ProcessorStage::Reingestor => None,
        }
    }

    /// The cadence and state allowance of a window processor draft.
    pub(super) fn window(&self) -> Option<&WindowDraft> {
        let ProcessorStage::Window(window) = &self.stage else {
            return None;
        };
        Some(window)
    }

    pub(super) fn window_mut(&mut self) -> Option<&mut WindowDraft> {
        let ProcessorStage::Window(window) = &mut self.stage else {
            return None;
        };
        Some(window)
    }

    /// The right side, correlation and timeouts of a correlator draft.
    pub(super) fn correlator(&self) -> Option<&CorrelatorDraft> {
        let ProcessorStage::Correlator(correlator) = &self.stage else {
            return None;
        };
        Some(correlator)
    }

    pub(super) fn correlator_mut(&mut self) -> Option<&mut CorrelatorDraft> {
        let ProcessorStage::Correlator(correlator) = &mut self.stage else {
            return None;
        };
        Some(correlator)
    }

    /// The draft's `list`: its primary list, or a correlator's right side.
    pub(super) fn input_list(&self, list: InputList) -> Option<&InputListDraft> {
        match list {
            InputList::Right => self.correlator().map(|correlator| &correlator.right),
            InputList::From | InputList::Left => Some(&self.from),
        }
    }

    pub(super) fn input_list_mut(&mut self, list: InputList) -> Option<&mut InputListDraft> {
        match list {
            InputList::Right => self
                .correlator_mut()
                .map(|correlator| &mut correlator.right),
            InputList::From | InputList::Left => Some(&mut self.from),
        }
    }

    pub(super) fn current_first_input(&self) -> Option<&RelayName> {
        self.from.current_first()
    }

    /// Selects `node` for the active input. The state, the routes and a correlator's left
    /// timeout relay are offered by the first input's schema or branch, so a changed first input
    /// requires them again.
    pub(super) fn select_input(&mut self, node: &NodeRef) {
        if self.from.select(node) {
            for state in &mut self.state {
                state.invalidate();
            }
            for route in &mut self.routes {
                route.invalidate_references();
            }
            if let Some(correlator) = self.correlator_mut() {
                correlator.left_timeout.invalidate();
            }
        }
    }

    pub(super) fn select_right_input(&mut self, node: &NodeRef) {
        if let Some(correlator) = self.correlator_mut() {
            correlator.select_right_input(node);
        }
    }

    pub(super) fn select_timeout_relay(&mut self, side: CorrelationSide, node: &NodeRef) {
        if let Some(correlator) = self.correlator_mut() {
            correlator.timeout_mut(side).select_relay(node);
        }
    }

    /// Whether the active route has an output assignment a picker can insert a reference into.
    pub(super) fn has_output_assignment(&self) -> bool {
        match self.active_route() {
            Some(route) => !route.assignments.is_empty(),
            None => false,
        }
    }

    fn insert_into_last_assignment(&mut self, reference: &InsertedReference) {
        let Some(route) = self.active_route_mut() else {
            return;
        };
        if let Some(assignment) = route.assignments.last_mut() {
            reference.append_to(&mut assignment.expression);
        }
    }

    /// Appends the window's aggregate over `field` of the input record to the last assignment of
    /// the active route, so the field is read only as the aggregate's argument.
    pub(super) fn insert_aggregate_argument(&mut self, field: FieldName) {
        let Some(function) = self.window().and_then(WindowDraft::aggregate_function) else {
            return;
        };
        let reference = InsertedReference::Call {
            function,
            argument: FieldReference::scoped(FieldScope::Input, field),
        };
        self.insert_into_last_assignment(&reference);
    }

    /// Appends `field` of `side`'s record to the last assignment of the active route.
    pub(super) fn insert_route_reference(&mut self, side: CorrelationSide, field: FieldName) {
        self.insert_into_last_assignment(&side.reference(field));
    }

    /// Appends `field` of `side`'s record to a correlator's `CORRELATE WHERE`.
    pub(super) fn insert_correlation_reference(&mut self, side: CorrelationSide, field: FieldName) {
        if let Some(correlator) = self.correlator_mut() {
            correlator.insert_correlation_reference(side, field);
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
        self.from.invalidate();
        for state in &mut self.state {
            state.invalidate();
        }
        for route in &mut self.routes {
            route.invalidate_references();
        }
        if let Some(correlator) = self.correlator_mut() {
            correlator.invalidate_relays();
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
            ProcessorStage::Window(window) => {
                Ok(Model::WindowProcessor(self.build_window(window)?))
            }
            ProcessorStage::Correlator(correlator) => {
                Ok(Model::Correlator(self.build_correlator(correlator)?))
            }
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

    fn build_window(
        &self,
        window: &WindowDraft,
    ) -> error_stack::Result<CreateWindowProcessor, ProcessorDraftError> {
        let name = WindowProcessorName::parse(self.name.trim())
            .map_err(|cause| cause.change_context(ProcessorDraftError::WindowProcessorName))?;
        let branching = self.branching.build(self.family())?;
        let input = self.build_input()?;
        let cadence = window.build()?;
        let output = self.build_output()?;
        Ok(CreateWindowProcessor {
            name,
            from: input.inputs,
            output_routes: output.routes,
            branched_by: branching,
            width: cadence.width,
            step: cadence.step,
            state_limit: cadence.state_limit,
            mode: self.mode,
            filter_where: input.filter,
            materialized_state: output.state,
        })
    }

    /// A correlator, whose left side is the draft's primary list. A correlator declares no
    /// node-wide filter, so its form offers none and the draft reads none.
    fn build_correlator(
        &self,
        correlator: &CorrelatorDraft,
    ) -> error_stack::Result<CreateCorrelator, ProcessorDraftError> {
        let name = CorrelatorName::parse(self.name.trim())
            .map_err(|cause| cause.change_context(ProcessorDraftError::CorrelatorName))?;
        let branching = self.branching.build(self.family())?;
        let left = self.from.build(InputList::Left)?;
        let correlation = correlator.build()?;
        let output = self.build_output()?;
        Ok(CreateCorrelator {
            name,
            left,
            right: correlation.right,
            output_routes: output.routes,
            branched_by: branching,
            correlate_where: correlation.correlate_where,
            match_policy: correlation.match_policy,
            max_time: correlation.max_time,
            timeout_policy: correlation.timeout_policy,
            mode: self.mode,
            filter_where: None,
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
        let inputs = self.from.build(InputList::From)?;
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
        let family = self.family();
        let mut routes = Vec::new();
        for (index, route) in self.routes.iter().enumerate() {
            let built = match family.route_contract() {
                RouteContract::Transforming if family.declares_node_branch() => {
                    route.build_preserving()
                }
                RouteContract::Transforming => route.build_reingestor(),
                RouteContract::FlushedSetOnly => route.build_set_only(SetOnlyEmission::Flushed),
                RouteContract::WindowSetOnly => {
                    route.build_set_only(SetOnlyEmission::WindowCadence)
                }
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

/// A positive byte count, read as the language reads a byte-size literal: `unreadable` when the
/// text is no byte size, and `zero` when it is a size of no bytes.
pub(super) fn positive_byte_size(
    text: &str,
    unreadable: ProcessorDraftError,
    zero: ProcessorDraftError,
) -> error_stack::Result<NonZeroU64, ProcessorDraftError> {
    let bytes = match text.trim().parse::<ubyte::ByteUnit>() {
        Ok(bytes) => bytes.as_u64(),
        // The byte-size parser is `no_std`: its refusal is no error type a report can hold, so
        // the report carries it as a printable attachment.
        Err(cause) => {
            return Err(Report::new(unreadable).attach_printable(cause));
        }
    };
    NonZeroU64::new(bytes).ok_or_else(|| Report::new(zero))
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
    #[error("Window processor name is invalid")]
    WindowProcessorName,
    #[error("Correlator name is invalid")]
    CorrelatorName,
    #[error("Add at least one {} relay", .list.relays())]
    Inputs { list: InputList },
    #[error("{} {index}: choose a relay", .list.input())]
    Input { list: InputList, index: usize },
    #[error(
        "{} {index}: the relay belongs to a changed context; select it again",
        .list.input()
    )]
    InputChanged { list: InputList, index: usize },
    #[error("{} relay `{relay}` is selected more than once", .list.input())]
    DuplicateInput { list: InputList, relay: RelayName },
    #[error("{} {index}: enter a valid WHERE expression", .list.input())]
    InputWhere { list: InputList, index: usize },
    #[error("Enter the {} duration", .list.collection())]
    CollectFor { list: InputList },
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
    #[error("Enter {clause} as a message count, a duration, or both")]
    WindowBound { clause: WindowBoundClause },
    #[error("Enter the {clause} message count as a whole number")]
    WindowMessages { clause: WindowBoundClause },
    #[error("Enter MAX STATE SIZE as a byte size such as 1MiB")]
    StateSize,
    #[error("MAX STATE SIZE must be greater than zero")]
    StateSizeZero,
    #[error("Enter a valid CORRELATE WHERE expression")]
    CorrelateWhere,
    #[error("Choose MATCH EARLIEST or MATCH LATEST")]
    MatchPolicy,
    #[error("Choose the {side} timeout action: DROP or SEND TO")]
    Timeout { side: CorrelationSide },
    #[error("Choose the {side} timeout relay")]
    TimeoutRelay { side: CorrelationSide },
    #[error("The {side} timeout relay belongs to a changed context; select it again")]
    TimeoutRelayChanged { side: CorrelationSide },
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
        BinaryOperator, BranchName, CorrelatorMatchPolicy, CreateStatement, Expression, FieldName,
        FieldReference, FieldScope, InputCollectPolicy, Literal, MaterializedStatePolicy, Model,
        ModelName, OutputBranch, RequestedResourceVersion, Statement,
    };
    use nervix_plexus::statement::parse_statement;

    use super::*;
    use crate::create_dialog::{
        ingestor_route_draft::{FlushDraft, InheritDraft, MessageErrorDraft, RouteBranchDraft},
        processor_window_draft::WindowBoundDraft,
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
                        draft.from.collect = Some(InputCollectPolicy {
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
        draft.from.relays[0].where_clause = "input.message != ''".into();
        draft.from.add();
        draft.select_input(&node(ModelKind::Relay, "incoming_b"));
        draft.from.relays[1].where_clause = "input.message != 'x'".into();
        draft.from.collect = Some(InputCollectPolicy {
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
        draft.from.add();
        draft.select_input(&node(ModelKind::Relay, "alternate"));
        draft.from.move_active(true);

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
        draft.from.relays[0].where_clause = r"input.message = 'a\nb'".into();
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
        draft.from.add();
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
        draft.from.add();
        draft.select_input(&node(ModelKind::Relay, "second"));
        draft.add_state();
        let state = draft.active_state_mut().assured("state exists");
        state.select_relay(&node(ModelKind::Relay, "profile"));
        state.policy = StatePolicyDraft::RequiredSkip;

        draft.from.active = 0;
        draft.select_input(&node(ModelKind::Relay, "replacement"));
        assert!(draft.from.relays[1].current_relay().is_none());
        assert!(draft.state[0].current_relay().is_none());
        assert!(draft.routes[0].current_relay().is_none());
        assert!(matches!(
            draft.build_reingestor().unwrap_err().current_context(),
            ProcessorDraftError::InputChanged {
                list: InputList::From,
                index: 2
            }
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
            ProcessorDraftError::Input {
                list: InputList::From,
                index: 1
            }
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
        draft.from.add();
        draft.select_input(&node(ModelKind::Relay, "second"));
        draft.from.add();
        draft.select_input(&node(ModelKind::Relay, "third"));
        draft.add_state();
        let state = draft.active_state_mut().assured("state exists");
        state.select_relay(&node(ModelKind::Relay, "profile"));
        state.policy = StatePolicyDraft::Default(vec![AssignmentDraft::selected(
            FieldName::parse("message").assured("valid field"),
        )]);

        draft.from.move_active(true);
        assert_eq!(
            draft
                .current_first_input()
                .assured("first input exists")
                .as_str(),
            "incoming"
        );
        assert_eq!(
            draft
                .from
                .relays
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
        draft.from.active = 0;
        draft.from.move_active(false);
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
                ProcessorFamily::Window => ModelKind::WindowProcessor,
                ProcessorFamily::Correlator => ModelKind::Correlator,
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
            &ProcessorDraftError::InputChanged {
                list: InputList::From,
                index: 1
            }
        );
    }

    fn window_bound(messages: &str, duration: &str) -> WindowBoundDraft {
        WindowBoundDraft {
            messages: messages.into(),
            duration: duration.into(),
        }
    }

    /// A complete window over `incoming` writing one aggregate to `outgoing`.
    fn complete_window() -> ProcessorDraft {
        let mut draft = ProcessorDraft::new(ProcessorFamily::Window);
        draft.name = "latency_windows".into();
        draft.choose_unbranched();
        draft.select_input(&node(ModelKind::Relay, "incoming"));
        let window = draft
            .window_mut()
            .assured("a window draft holds its cadence");
        window.width = window_bound("3", "");
        window.step = window_bound("3", "");
        let route = draft.active_route_mut().assured("the route exists");
        route.select_relay(&node(ModelKind::Relay, "outgoing"));
        route.add_assignment(FieldName::parse("samples").assured("valid field"));
        route.assignments[0].expression = "COUNT(input.latency)".into();
        route.message_error = MessageErrorDraft::Log;
        draft
    }

    /// A complete correlator of `orders` and `payments` writing one field to `paid`.
    fn complete_correlator() -> ProcessorDraft {
        let mut draft = ProcessorDraft::new(ProcessorFamily::Correlator);
        draft.name = "paid_orders".into();
        draft.choose_unbranched();
        draft.select_input(&node(ModelKind::Relay, "orders"));
        draft.select_right_input(&node(ModelKind::Relay, "payments"));
        let correlator = draft
            .correlator_mut()
            .assured("a correlator draft holds its correlation");
        correlator.correlate_where = "left.order_id = right.order_id".into();
        correlator.match_policy = Some(CorrelatorMatchPolicy::Earliest);
        correlator.max_time = "5m".into();
        correlator.left_timeout.choose_drop();
        correlator.right_timeout.choose_drop();
        let route = draft.active_route_mut().assured("the route exists");
        route.select_relay(&node(ModelKind::Relay, "paid"));
        route.add_assignment(FieldName::parse("amount").assured("valid field"));
        route.assignments[0].expression = "right.amount".into();
        route.flush = FlushDraft::Immediate;
        route.message_error = MessageErrorDraft::Log;
        draft
    }

    #[test]
    fn window_previews_keep_every_bound_shape_and_state_limit_and_reparse() {
        for (width, step, state_size, clauses) in [
            (
                window_bound("3", ""),
                window_bound("3", ""),
                None,
                "\n  WIDTH 3 MESSAGES\n  STEP 3 MESSAGES\n  UNBRANCHED\n",
            ),
            (
                window_bound("", "5m"),
                window_bound("", "1m"),
                Some("1MiB"),
                "\n  WIDTH 5m DURATION\n  STEP 1m DURATION\n  MAX STATE SIZE 1048576B\n  \
                 UNBRANCHED\n",
            ),
            (
                window_bound("100", "10s"),
                window_bound("10", ""),
                None,
                "\n  WIDTH 100 MESSAGES 10s DURATION\n  STEP 10 MESSAGES\n  UNBRANCHED\n",
            ),
            (
                window_bound("100", "10s"),
                window_bound("10", "1s"),
                Some("4096B"),
                "\n  WIDTH 100 MESSAGES 10s DURATION\n  STEP 10 MESSAGES 1s DURATION\n  MAX STATE \
                 SIZE 4096B\n  UNBRANCHED\n",
            ),
        ] {
            let mut draft = complete_window();
            let window = draft
                .window_mut()
                .assured("a window draft holds its cadence");
            window.width = width;
            window.step = step;
            window.state_size = state_size.map(str::to_string);
            let statement = statement(&draft);
            let canonical = statement.to_canonical_plexus().assured("windows render");
            assert!(canonical.contains(clauses), "{canonical}");
            assert_eq!(
                parse_statement(&canonical).assured("the preview is executable Plexus"),
                statement
            );
        }
    }

    #[test]
    fn window_routes_are_set_only_and_emit_on_the_window_cadence() {
        let mut draft = complete_window();
        draft.filter = "input.latency >= 0".into();
        let route = draft.active_route_mut().assured("the route exists");
        route.add_assignment(FieldName::parse("spread").assured("valid field"));
        route.assignments[1].expression = "MAX(input.latency) - MIN(input.latency)".into();
        route.where_clause = "output.samples > 0".into();
        // A window route reads neither a flush policy nor an inheritance its form never shows.
        route.flush = FlushDraft::Unselected;
        route.inherit = InheritDraft::Unselected;
        draft.add_route();
        let error_route = draft
            .active_route_mut()
            .assured("the added route is active");
        error_route.select_relay(&node(ModelKind::Relay, "audit"));
        error_route.add_assignment(FieldName::parse("samples").assured("valid field"));
        error_route.assignments[0].expression = "COUNT(input.latency) + 1".into();
        error_route.message_error = MessageErrorDraft::SendTo {
            relay: None,
            assignments: Vec::new(),
        };
        error_route
            .message_error
            .select_relay(&node(ModelKind::Relay, "errors"));
        if let MessageErrorDraft::SendTo { assignments, .. } = &mut error_route.message_error {
            assignments.push(AssignmentDraft::selected(
                FieldName::parse("reason").assured("valid field"),
            ));
            assignments[0].expression = "error.message".into();
        }
        let Model::WindowProcessor(built) = draft.build().assured("the window is complete") else {
            panic!("a window draft builds a window processor");
        };
        assert_eq!(built.output_routes.routes.len(), 2);
        for route in &built.output_routes.routes {
            assert_eq!(route.flush_policy, None);
            assert_eq!(route.branch, None);
            assert_eq!(route.construction.inherit, None);
            assert!(route.construction.invocations.is_empty());
        }
        assert!(built.filter_where.is_some());
        let statement = statement(&draft);
        let canonical = statement.to_canonical_plexus().assured("windows render");
        assert!(
            canonical.contains(
                "\n    SET samples = count(input.latency),\n        spread = max(input.latency) - \
                 min(input.latency)\n    WHERE output.samples > 0\n    ON MESSAGE ERROR LOG\n"
            ),
            "{canonical}"
        );
        assert!(
            canonical.contains("ON MESSAGE ERROR SEND TO errors"),
            "{canonical}"
        );
        assert_eq!(
            parse_statement(&canonical).assured("the preview is executable Plexus"),
            statement
        );

        let route = draft.active_route_mut().assured("the route exists");
        route.assignments.clear();
        assert!(matches!(
            draft
                .build()
                .err()
                .assured("a route without SET is incomplete")
                .current_context(),
            ProcessorDraftError::Route {
                index: 2,
                cause: IngestRouteDraftError::SetAssignments
            }
        ));
    }

    #[test]
    fn correlator_previews_keep_both_sides_every_timeout_and_match_policy_and_reparse() {
        for (policy, left, right, clause) in [
            (
                CorrelatorMatchPolicy::Earliest,
                None,
                None,
                "MATCH EARLIEST\n  MAX TIME 5m\n  ON CORRELATION TIMEOUT DROP, DROP\n",
            ),
            (
                CorrelatorMatchPolicy::Latest,
                Some("unpaid"),
                None,
                "MATCH LATEST\n  MAX TIME 5m\n  ON CORRELATION TIMEOUT SEND TO unpaid, DROP\n",
            ),
            (
                CorrelatorMatchPolicy::Earliest,
                None,
                Some("orphaned"),
                "MATCH EARLIEST\n  MAX TIME 5m\n  ON CORRELATION TIMEOUT DROP, SEND TO orphaned\n",
            ),
            (
                CorrelatorMatchPolicy::Latest,
                Some("unpaid"),
                Some("orphaned"),
                "MATCH LATEST\n  MAX TIME 5m\n  ON CORRELATION TIMEOUT SEND TO unpaid, SEND TO \
                 orphaned\n",
            ),
        ] {
            let mut draft = complete_correlator();
            let correlator = draft
                .correlator_mut()
                .assured("a correlator draft holds its correlation");
            correlator.match_policy = Some(policy);
            for (side, relay) in [
                (CorrelationSide::Left, left),
                (CorrelationSide::Right, right),
            ] {
                if let Some(relay) = relay {
                    let timeout = correlator.timeout_mut(side);
                    timeout.choose_send();
                    timeout.select_relay(&node(ModelKind::Relay, relay));
                }
            }
            let statement = statement(&draft);
            let canonical = statement
                .to_canonical_plexus()
                .assured("correlators render");
            assert!(canonical.contains(clause), "{canonical}");
            assert_eq!(
                parse_statement(&canonical).assured("the preview is executable Plexus"),
                statement
            );
        }
    }

    #[test]
    fn correlator_sides_keep_their_own_relays_predicates_and_collection() {
        let mut draft = complete_correlator();
        draft.from.relays[0].where_clause = "left.active".into();
        draft.from.collect = Some(InputCollectPolicy {
            collect_for: "10ms".into(),
            max_batch_size: None,
        });
        draft.from.add();
        draft.select_input(&node(ModelKind::Relay, "order_backfill"));
        let correlator = draft
            .correlator_mut()
            .assured("a correlator draft holds its correlation");
        correlator.right.relays[0].where_clause = "right.amount > 0".into();
        correlator.right.collect = Some(InputCollectPolicy {
            collect_for: "20ms".into(),
            max_batch_size: Some("1MiB".into()),
        });
        draft.mode = AckMode::Detached;
        let route = draft.active_route_mut().assured("the route exists");
        route.flush = FlushDraft::Each {
            interval: "100ms".into(),
            max_batch_size: "1MiB".into(),
        };
        route.where_clause = "output.amount > 0".into();
        let statement = statement(&draft);
        let canonical = statement
            .to_canonical_plexus()
            .assured("correlators render");
        assert!(
            canonical.contains(
                "\n  LEFT FROM orders WHERE left.active, order_backfill COLLECT FOR 10ms\n  RIGHT \
                 FROM payments WHERE right.amount > 0 COLLECT FOR 20ms MAX BATCH SIZE 1MiB\n  \
                 DETACHED\n  CORRELATE WHERE left.order_id = right.order_id\n"
            ),
            "{canonical}"
        );
        assert!(
            canonical.contains(
                "\n    SET amount = right.amount\n    WHERE output.amount > 0\n    FLUSH EACH \
                 100ms MAX BATCH SIZE 1MiB\n    ON MESSAGE ERROR LOG;"
            ),
            "{canonical}"
        );
        assert_eq!(
            parse_statement(&canonical).assured("the preview is executable Plexus"),
            statement
        );
        let Model::Correlator(built) = draft.build().assured("the correlator is complete") else {
            panic!("a correlator draft builds a correlator");
        };
        assert_eq!(built.filter_where, None);
        assert_eq!(built.output_routes.routes[0].construction.inherit, None);
    }

    #[test]
    fn a_correlator_refuses_each_side_in_its_own_terms() {
        let mut draft = complete_correlator();
        draft.from.add();
        let refusal = |draft: &ProcessorDraft| {
            draft
                .build()
                .err()
                .assured("the correlator is incomplete")
                .current_context()
                .clone()
        };
        assert_eq!(
            refusal(&draft),
            ProcessorDraftError::Input {
                list: InputList::Left,
                index: 2
            }
        );
        draft.from.remove_active();
        let correlator = draft
            .correlator_mut()
            .assured("a correlator draft holds its correlation");
        correlator.right.add();
        assert_eq!(
            refusal(&draft),
            ProcessorDraftError::Input {
                list: InputList::Right,
                index: 2
            }
        );
        assert_eq!(
            refusal(&draft).to_string(),
            "RIGHT FROM input 2: choose a relay"
        );
        let correlator = draft
            .correlator_mut()
            .assured("a correlator draft holds its correlation");
        correlator.right.remove_active();
        correlator.right.collect = Some(InputCollectPolicy {
            collect_for: " ".into(),
            max_batch_size: None,
        });
        assert_eq!(
            refusal(&draft).to_string(),
            "Enter the RIGHT FROM COLLECT FOR duration"
        );
        let correlator = draft
            .correlator_mut()
            .assured("a correlator draft holds its correlation");
        correlator.right.collect = None;
        let route = draft.active_route_mut().assured("the route exists");
        route.flush = FlushDraft::Unselected;
        assert!(matches!(
            refusal(&draft),
            ProcessorDraftError::Route {
                index: 1,
                cause: IngestRouteDraftError::Flush
            }
        ));
    }

    #[test]
    fn a_changed_first_left_relay_requires_the_left_timeout_relay_again() {
        let mut draft = complete_correlator();
        let correlator = draft
            .correlator_mut()
            .assured("a correlator draft holds its correlation");
        correlator.left_timeout.choose_send();
        correlator
            .left_timeout
            .select_relay(&node(ModelKind::Relay, "unpaid"));
        draft.select_input(&node(ModelKind::Relay, "orders"));
        assert!(
            draft.build().is_ok(),
            "reselecting the same relay keeps the timeout relay"
        );
        draft.select_input(&node(ModelKind::Relay, "returns"));
        assert_eq!(
            draft
                .build()
                .err()
                .assured("the left timeout relay belongs to the earlier relay")
                .current_context(),
            &ProcessorDraftError::TimeoutRelayChanged {
                side: CorrelationSide::Left
            }
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
