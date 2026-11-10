//! Browser-local emitter construction until every required choice is complete.
//!
//! Layer: edges.
//! - **Owns.** Ordered emitter inputs, construction, state, batching and required error policies.
//! - **Depends on.** Current semantic Models, expression parsing and shared field drafts.
//! - **Must not know.** Registry snapshots, emitter drivers or command recovery.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the visual emitter draft")
)]

use std::collections::BTreeSet;

use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_models::{
    AckMode, CodecName, CreateEmitter, EmitterBatchLegality, EmitterBatchPolicy, EmitterBody,
    EmitterBodyForm, EmitterName, ErrorPolicies, GeneralErrorPolicy, InputCollectPolicy, ModelKind,
    NodeRef, ProcessorInputWhere, ProcessorInputs, RouteConstruction,
};
use nervix_plexus::parse_expression;
use thiserror::Error;

use super::{
    SelectedReference,
    emitter_destination_draft::EmitterDestinationDraft,
    emitter_envelope_draft::EnvelopeDraft,
    ingestor_route_draft::{
        AssignmentDraft, FlushDraft, InheritDraft, InvocationDraft, MessageErrorDraft,
    },
    processor_draft::StateDependencyDraft,
    processor_input_draft::InputDraft,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BodyChoice {
    Codec,
    WithoutBody,
    Values,
    Client,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct BatchDraft {
    pub(super) enabled: bool,
    pub(super) messages: String,
    pub(super) size: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct EmitterDraft {
    pub(super) name: String,
    pub(super) if_not_exists: bool,
    pub(super) destination: EmitterDestinationDraft,
    pub(super) inputs: Vec<InputDraft>,
    pub(super) active_input: usize,
    pub(super) collect: Option<InputCollectPolicy>,
    pub(super) mode: AckMode,
    pub(super) body: Option<BodyChoice>,
    pub(super) codec: Option<SelectedReference<CodecName>>,
    pub(super) state: Vec<StateDependencyDraft>,
    pub(super) active_state: usize,
    pub(super) inherit: InheritDraft,
    pub(super) assignments: Vec<AssignmentDraft>,
    pub(super) predicate: String,
    pub(super) invocations: Vec<InvocationDraft>,
    pub(super) envelope: EnvelopeDraft,
    pub(super) batch: BatchDraft,
    pub(super) flush: FlushDraft,
    pub(super) message_error: MessageErrorDraft,
    pub(super) general_error: Option<GeneralErrorPolicy>,
}

impl Default for EmitterDraft {
    fn default() -> Self {
        Self {
            name: String::new(),
            if_not_exists: false,
            destination: EmitterDestinationDraft::default(),
            inputs: vec![InputDraft::default()],
            active_input: 0,
            collect: None,
            mode: AckMode::Attached,
            body: None,
            codec: None,
            state: Vec::new(),
            active_state: 0,
            inherit: InheritDraft::Unselected,
            assignments: Vec::new(),
            predicate: String::new(),
            invocations: Vec::new(),
            envelope: EnvelopeDraft::default(),
            batch: BatchDraft::default(),
            flush: FlushDraft::Unselected,
            message_error: MessageErrorDraft::Unselected,
            general_error: None,
        }
    }
}

impl EmitterDraft {
    pub(super) fn choose_kind(&mut self, kind: nervix_models::EmitterEntityKind) {
        if self.destination.kind == Some(kind) {
            return;
        }
        self.destination = EmitterDestinationDraft {
            kind: Some(kind),
            ..EmitterDestinationDraft::default()
        };
        self.codec = None;
        self.body = match kind.capabilities().body {
            EmitterBodyForm::Codec => Some(BodyChoice::Codec),
            EmitterBodyForm::CodecOrWithoutBody => None,
            EmitterBodyForm::Values => Some(BodyChoice::Values),
            EmitterBodyForm::Native => Some(BodyChoice::Client),
        };
        self.assignments.clear();
        self.invocations.clear();
        self.envelope = EnvelopeDraft::default();
        self.batch = BatchDraft {
            enabled: matches!(
                kind.capabilities().batch,
                EmitterBatchLegality::Required { .. }
            ),
            ..BatchDraft::default()
        };
        self.inherit = if matches!(self.body, Some(BodyChoice::Values)) {
            InheritDraft::None
        } else {
            InheritDraft::Unselected
        };
    }

    pub(super) fn choose_body(&mut self, body: BodyChoice) {
        if self.body == Some(body) {
            return;
        }
        self.body = Some(body);
        self.assignments.clear();
        self.inherit = if body == BodyChoice::WithoutBody {
            InheritDraft::None
        } else {
            InheritDraft::Unselected
        };
    }

    pub(super) fn select_codec(&mut self, node: &NodeRef) {
        if node.kind != ModelKind::Codec {
            return;
        }
        let selected = CodecName::from(&node.identifier);
        if self.current_codec() != Some(&selected) {
            for assignment in &mut self.assignments {
                assignment.invalidate();
            }
        }
        self.codec = Some(SelectedReference::chosen(selected));
    }

    pub(super) fn current_codec(&self) -> Option<&CodecName> {
        self.codec
            .as_ref()
            .and_then(SelectedReference::current_name)
    }

    pub(super) fn select_input(&mut self, node: &NodeRef) {
        if node.kind != ModelKind::Relay {
            return;
        }
        if let Some(input) = self.inputs.get_mut(self.active_input) {
            if input
                .current_relay()
                .is_some_and(|relay| relay.as_str() == node.identifier.as_str())
            {
                return;
            }
            input.select_relay(node);
        }
        if self.active_input == 0 {
            for input in self.inputs.iter_mut().skip(1) {
                input.invalidate();
            }
        }
        for state in &mut self.state {
            state.invalidate();
        }
        self.message_error.invalidate();
        self.inherit.invalidate();
    }

    pub(super) fn add_input(&mut self) {
        self.inputs.push(InputDraft::default());
        self.active_input = self
            .inputs
            .len()
            .checked_sub(1)
            .assured("the input was just appended");
    }

    pub(super) fn remove_input(&mut self) {
        if self.inputs.len() > 1 && self.active_input < self.inputs.len() {
            self.inputs.remove(self.active_input);
            self.active_input = self.active_input.min(
                self.inputs
                    .len()
                    .checked_sub(1)
                    .assured("one input remains"),
            );
            for state in &mut self.state {
                state.invalidate();
            }
            self.message_error.invalidate();
        }
    }

    pub(super) fn move_input(&mut self, up: bool) {
        let other = if up {
            self.active_input.checked_sub(1)
        } else {
            self.active_input.checked_add(1)
        };
        if let Some(other) = other
            && other < self.inputs.len()
            && self.active_input < self.inputs.len()
        {
            self.inputs.swap(self.active_input, other);
            self.active_input = other;
        }
    }

    pub(super) fn invalidate_references(&mut self) {
        self.destination.invalidate_references();
        if let Some(codec) = &mut self.codec {
            codec.invalidate();
        }
        for input in &mut self.inputs {
            input.invalidate();
        }
        for state in &mut self.state {
            state.invalidate();
        }
        self.inherit.invalidate();
        for assignment in &mut self.assignments {
            assignment.invalidate();
        }
        self.message_error.invalidate();
    }

    fn build_inputs(&self) -> error_stack::Result<ProcessorInputs, EmitterDraftError> {
        if self.inputs.is_empty() {
            return Err(Report::new(EmitterDraftError::Missing("input relay")));
        }
        let mut from = Vec::with_capacity(self.inputs.len());
        let mut predicates = Vec::new();
        let mut seen = BTreeSet::new();
        for input in &self.inputs {
            let selected = input
                .relay
                .as_ref()
                .ok_or_else(|| Report::new(EmitterDraftError::Missing("input relay")))?;
            let relay = selected
                .current_name()
                .ok_or_else(|| Report::new(EmitterDraftError::Changed("input relay")))?;
            if !seen.insert(relay.clone()) {
                return Err(Report::new(EmitterDraftError::DuplicateInput));
            }
            from.push(relay.clone());
            if !input.where_clause.trim().is_empty() {
                predicates.push(ProcessorInputWhere {
                    relay: relay.clone(),
                    where_clause: parse_expression(input.where_clause.trim())
                        .change_context(EmitterDraftError::Invalid("input predicate"))?,
                });
            }
        }
        let mut inputs = ProcessorInputs::new(from, predicates);
        if let Some(collect) = &self.collect {
            required(&collect.collect_for, "collection duration")?;
            inputs.collect_policy = Some(collect.clone());
        }
        Ok(inputs)
    }

    fn build_construction(
        &self,
        body: &EmitterBody,
    ) -> error_stack::Result<RouteConstruction, EmitterDraftError> {
        let mut assignments = Vec::with_capacity(self.assignments.len());
        for assignment in &self.assignments {
            assignments.push(
                assignment
                    .build()
                    .change_context(EmitterDraftError::Invalid("output assignment"))?,
            );
        }
        let mut invocations = Vec::with_capacity(self.invocations.len());
        for invocation in &self.invocations {
            invocations.push(
                invocation
                    .build()
                    .change_context(EmitterDraftError::Invalid("invocation"))?,
            );
        }
        let inherit = self
            .inherit
            .build()
            .change_context(EmitterDraftError::Invalid("inheritance"))?;
        if matches!(body, EmitterBody::WithoutBody | EmitterBody::Values)
            && (inherit.is_some() || !assignments.is_empty())
        {
            return Err(Report::new(EmitterDraftError::Invalid(
                "construction for this body",
            )));
        }
        let predicate = if self.predicate.trim().is_empty() {
            None
        } else {
            Some(
                parse_expression(self.predicate.trim())
                    .change_context(EmitterDraftError::Invalid("output predicate"))?,
            )
        };
        Ok(RouteConstruction {
            inherit,
            assignments,
            where_clause: predicate,
            invocations,
        })
    }

    pub(super) fn build(&self) -> error_stack::Result<CreateEmitter, EmitterDraftError> {
        let name = EmitterName::parse(self.name.trim())
            .change_context(EmitterDraftError::Invalid("emitter name"))?;
        let connector = self.destination.build()?;
        let body = match self.body {
            Some(BodyChoice::Codec) => {
                let codec = self
                    .codec
                    .as_ref()
                    .ok_or_else(|| Report::new(EmitterDraftError::Missing("encoding codec")))?;
                EmitterBody::Codec {
                    codec: codec
                        .current_name()
                        .ok_or_else(|| Report::new(EmitterDraftError::Changed("encoding codec")))?
                        .clone(),
                }
            }
            Some(BodyChoice::WithoutBody) => EmitterBody::WithoutBody,
            Some(BodyChoice::Values) => EmitterBody::Values,
            Some(BodyChoice::Client) => EmitterBody::Client,
            None => return Err(Report::new(EmitterDraftError::Missing("emitter body"))),
        };
        if !connector.capabilities().body.admits(&body) {
            return Err(Report::new(EmitterDraftError::Invalid("emitter body")));
        }
        let construction = self.build_construction(&body)?;
        let mut materialized_state = Vec::with_capacity(self.state.len());
        let mut seen_state = BTreeSet::new();
        for (index, dependency) in self.state.iter().enumerate() {
            let number = index
                .checked_add(1)
                .assured("an allocated vector is smaller than usize::MAX");
            let dependency = dependency
                .build(number)
                .change_context(EmitterDraftError::Invalid("materialized dependency"))?;
            if !seen_state.insert(dependency.relay.clone()) {
                return Err(Report::new(EmitterDraftError::DuplicateState));
            }
            materialized_state.push(dependency);
        }
        let batch = if self.batch.enabled {
            Some(EmitterBatchPolicy {
                max_messages: nervix_models::BatchMessageLimit::try_from(
                    self.batch
                        .messages
                        .parse::<u64>()
                        .change_context(EmitterDraftError::Invalid("batch message limit"))?,
                )
                .change_context(EmitterDraftError::Invalid("batch message limit"))?,
                max_size: self
                    .batch
                    .size
                    .parse::<nervix_models::PayloadSizeLimit>()
                    .change_context(EmitterDraftError::Invalid("batch byte limit"))?,
            })
        } else {
            None
        };
        let envelope = self.envelope.build(connector.kind())?;
        let emitter = CreateEmitter {
            labels: Default::default(),
            name,
            from: self.build_inputs()?,
            body,
            envelope: Box::new(envelope),
            connector: Box::new(connector),
            batch,
            flush_policy: self
                .flush
                .build()
                .change_context(EmitterDraftError::Invalid("flush policy"))?,
            error_policies: ErrorPolicies {
                message: self
                    .message_error
                    .build()
                    .change_context(EmitterDraftError::Invalid("message error policy"))?,
                general: self.general_error.clone().ok_or_else(|| {
                    Report::new(EmitterDraftError::Missing("general error policy"))
                })?,
            },
            mode: self.mode,
            construction,
            materialized_state,
        };
        emitter
            .validate_batch()
            .change_context(EmitterDraftError::Invalid("batch policy"))?;
        Ok(emitter)
    }
}

pub(super) fn required(
    value: &str,
    field: &'static str,
) -> error_stack::Result<String, EmitterDraftError> {
    if value.is_empty() {
        return Err(Report::new(EmitterDraftError::Missing(field)));
    }
    Ok(value.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(super) enum EmitterDraftError {
    #[error("Choose or enter {0}")]
    Missing(&'static str),
    #[error("Enter a valid {0}")]
    Invalid(&'static str),
    #[error("The selected {0} belongs to a changed context; select it again")]
    Changed(&'static str),
    #[error("Choose each input relay only once")]
    DuplicateInput,
    #[error("Choose each materialized dependency only once")]
    DuplicateState,
}
