//! Typed dependency questions for the emitter form's independent controls.
//!
//! Layer: edges.
//! - **Owns.** Emitter choice context, typed selections and visible lookup controls.
//! - **Depends on.** Emitter drafts and the public choice request contract.
//! - **Must not know.** Registry storage, network framing or emitter execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns emitter browser choice context")
)]

use leptos::prelude::*;
use nervix_client_wire::{ChoiceSelection, ChoiceTarget, ChoiceValue};
use nervix_models::{EmitterBodyForm, ModelKind, NodeRef};

use super::{
    ChoiceControl, ChoiceQuery, CreateSignals,
    emitter_draft::BodyChoice,
    ingestor_route_draft::{AssignmentDraft, InheritDraft, MessageErrorDraft},
    processor_draft::StatePolicyDraft,
};

impl CreateSignals {
    #[cfg_attr(
        nervix_lint,
        nervix::error_boundary(
            outcome,
            reason = "an incomplete browser choice exposes only its prerequisite prompt"
        )
    )]
    fn emitter_model_question(
        self,
        target: ChoiceTarget,
        node: NodeRef,
    ) -> Result<ChoiceQuery, &'static str> {
        let mut query = self.domain_question(
            target,
            "Select a domain before choosing an emitter reference",
        )?;
        query.dependencies.push(ChoiceSelection {
            value: ChoiceValue::Model(node),
        });
        query.page_size = 100;
        Ok(query)
    }

    #[cfg_attr(
        nervix_lint,
        nervix::error_boundary(
            outcome,
            reason = "an incomplete browser choice exposes only its prerequisite prompt"
        )
    )]
    pub(super) fn emitter_choice_query(
        self,
        control: ChoiceControl,
    ) -> Result<ChoiceQuery, &'static str> {
        let draft = self.emitter.get_untracked();
        match control {
            ChoiceControl::EmitterReference => {
                let kind = draft
                    .destination
                    .kind
                    .ok_or("Choose the emitter entity before its reference")?;
                self.domain_question(
                    ChoiceTarget::for_emitter_reference(kind),
                    "Select a domain before choosing an emitter reference",
                )
            }
            ChoiceControl::EmitterCatalog => self.domain_question(
                ChoiceTarget::EmitterIcebergCatalog,
                "Select a domain before choosing a catalog",
            ),
            ChoiceControl::EmitterCodec => self.domain_question(
                ChoiceTarget::EmitterCodec,
                "Select a domain before choosing an encoding codec",
            ),
            ChoiceControl::EmitterInputRelay => {
                if draft.active_input == 0 {
                    return self.domain_question(
                        ChoiceTarget::Relay,
                        "Select a domain before choosing an input",
                    );
                }
                let input = draft
                    .inputs
                    .first()
                    .ok_or("Add the first input before another input")?;
                let input = input
                    .current_relay()
                    .ok_or("Choose the first input before adding another input")?;
                self.emitter_model_question(
                    ChoiceTarget::EmitterCompatibleInputRelay,
                    NodeRef::new(ModelKind::Relay, input),
                )
            }
            ChoiceControl::EmitterInputField => {
                let input = draft
                    .inputs
                    .first()
                    .ok_or("Add an input before its fields")?;
                let input = input
                    .current_relay()
                    .ok_or("Choose an input before its fields")?;
                self.emitter_model_question(
                    ChoiceTarget::RelayField,
                    NodeRef::new(ModelKind::Relay, input),
                )
            }
            ChoiceControl::EmitterOutputField => {
                if draft.body == Some(BodyChoice::Client) {
                    let reference = draft
                        .destination
                        .reference
                        .as_ref()
                        .ok_or("Choose the output schema before its fields")?;
                    let schema = reference
                        .current_name()
                        .ok_or("Reselect the changed output schema before its fields")?;
                    self.emitter_model_question(
                        ChoiceTarget::SchemaField,
                        NodeRef::new(ModelKind::Schema, schema),
                    )
                } else {
                    let codec = draft
                        .current_codec()
                        .ok_or("Choose the encoding codec before output fields")?;
                    self.emitter_model_question(
                        ChoiceTarget::CodecField,
                        NodeRef::new(ModelKind::Codec, codec),
                    )
                }
            }
            ChoiceControl::EmitterStateRelay | ChoiceControl::EmitterErrorRelay => {
                let target = if control == ChoiceControl::EmitterStateRelay {
                    ChoiceTarget::EmitterMaterializedRelay
                } else {
                    ChoiceTarget::EmitterErrorRelay
                };
                let mut query =
                    self.domain_question(target, "Select a domain before choosing a relay")?;
                for input in &draft.inputs {
                    let relay = input
                        .current_relay()
                        .ok_or("Choose every input before state or error relays")?;
                    query.dependencies.push(ChoiceSelection {
                        value: ChoiceValue::Model(NodeRef::new(ModelKind::Relay, relay)),
                    });
                }
                Ok(query)
            }
            ChoiceControl::EmitterStateField => {
                let dependency = draft
                    .state
                    .get(draft.active_state)
                    .ok_or("Add a materialized dependency before its fields")?;
                let relay = dependency
                    .current_relay()
                    .ok_or("Choose a materialized relay before its default fields")?;
                self.emitter_model_question(
                    ChoiceTarget::RelayField,
                    NodeRef::new(ModelKind::Relay, relay),
                )
            }
            ChoiceControl::EmitterErrorField => {
                let relay = draft
                    .message_error
                    .current_relay()
                    .ok_or("Choose an error relay before its fields")?;
                self.emitter_model_question(
                    ChoiceTarget::RelayField,
                    NodeRef::new(ModelKind::Relay, relay),
                )
            }
            _ => Err("Open an emitter form first"),
        }
    }

    pub(super) fn emitter_open_controls(self) -> Vec<ChoiceControl> {
        let draft = self.emitter.get_untracked();
        let mut controls = vec![ChoiceControl::EmitterInputRelay];
        if let Some(kind) = draft.destination.kind {
            controls.push(ChoiceControl::EmitterReference);
            if kind.capabilities().option("CATALOG").is_some() {
                controls.push(ChoiceControl::EmitterCatalog);
            }
            if matches!(
                kind.capabilities().body,
                EmitterBodyForm::Codec | EmitterBodyForm::CodecOrWithoutBody
            ) && draft.body == Some(BodyChoice::Codec)
            {
                controls.push(ChoiceControl::EmitterCodec);
            }
        }
        if matches!(
            draft.inherit,
            InheritDraft::AllExcept(_) | InheritDraft::Fields(_)
        ) {
            controls.push(ChoiceControl::EmitterInputField);
        }
        if matches!(draft.body, Some(BodyChoice::Codec | BodyChoice::Client)) {
            controls.push(ChoiceControl::EmitterOutputField);
        }
        if !draft.state.is_empty() {
            controls.push(ChoiceControl::EmitterStateRelay);
        }
        if draft
            .state
            .get(draft.active_state)
            .is_some_and(|dependency| matches!(dependency.policy, StatePolicyDraft::Default(_)))
        {
            controls.push(ChoiceControl::EmitterStateField);
        }
        if let MessageErrorDraft::SendTo { .. } = draft.message_error {
            controls.extend([
                ChoiceControl::EmitterErrorRelay,
                ChoiceControl::EmitterErrorField,
            ]);
        }
        controls
    }

    pub(super) fn emitter_selected(self, control: ChoiceControl, value: &ChoiceValue) -> bool {
        let ChoiceValue::Model(node) = value else {
            return false;
        };
        let draft = self.emitter.get();
        match control {
            ChoiceControl::EmitterReference => {
                draft
                    .destination
                    .reference
                    .as_ref()
                    .is_some_and(|reference| {
                        reference.is_current() && reference.name() == &node.identifier
                    })
            }
            ChoiceControl::EmitterCatalog => draft
                .destination
                .catalog
                .as_ref()
                .is_some_and(|reference| reference.selects(ModelKind::Client, node)),
            ChoiceControl::EmitterInputRelay => match draft.inputs.get(draft.active_input) {
                Some(input) => input
                    .relay
                    .as_ref()
                    .is_some_and(|reference| reference.selects(ModelKind::Relay, node)),
                None => false,
            },
            ChoiceControl::EmitterCodec => draft
                .codec
                .as_ref()
                .is_some_and(|reference| reference.selects(ModelKind::Codec, node)),
            ChoiceControl::EmitterStateRelay => match draft.state.get(draft.active_state) {
                Some(dependency) => dependency
                    .relay
                    .as_ref()
                    .is_some_and(|reference| reference.selects(ModelKind::Relay, node)),
                None => false,
            },
            ChoiceControl::EmitterErrorRelay => {
                draft.message_error.current_relay().is_some_and(|relay| {
                    node.kind == ModelKind::Relay && relay.as_str() == node.identifier.as_str()
                })
            }
            _ => false,
        }
    }

    pub(super) fn select_emitter_choice(self, control: ChoiceControl, value: ChoiceValue) {
        self.emitter.update(|draft| match (control, value) {
            (ChoiceControl::EmitterReference, ChoiceValue::Model(node)) => {
                let before = draft.destination.reference.clone();
                draft.destination.select_reference(&node);
                if before != draft.destination.reference && draft.body == Some(BodyChoice::Client) {
                    for assignment in &mut draft.assignments {
                        assignment.invalidate();
                    }
                }
            }
            (ChoiceControl::EmitterCatalog, ChoiceValue::Model(node)) => {
                draft.destination.select_catalog(&node)
            }
            (ChoiceControl::EmitterInputRelay, ChoiceValue::Model(node)) => {
                draft.select_input(&node)
            }
            (ChoiceControl::EmitterCodec, ChoiceValue::Model(node)) => draft.select_codec(&node),
            (ChoiceControl::EmitterInputField, ChoiceValue::Field(field)) => {
                draft.inherit.add_field(field)
            }
            (ChoiceControl::EmitterOutputField, ChoiceValue::Field(field)) => {
                draft.assignments.push(AssignmentDraft::selected(field))
            }
            (ChoiceControl::EmitterStateRelay, ChoiceValue::Model(node)) => {
                if let Some(dependency) = draft.state.get_mut(draft.active_state) {
                    dependency.select_relay(&node);
                }
            }
            (ChoiceControl::EmitterStateField, ChoiceValue::Field(field)) => {
                if let Some(dependency) = draft.state.get_mut(draft.active_state)
                    && let StatePolicyDraft::Default(assignments) = &mut dependency.policy
                {
                    assignments.push(AssignmentDraft::selected(field));
                }
            }
            (ChoiceControl::EmitterErrorRelay, ChoiceValue::Model(node)) => {
                draft.message_error.select_relay(&node)
            }
            (ChoiceControl::EmitterErrorField, ChoiceValue::Field(field)) => {
                if let MessageErrorDraft::SendTo { assignments, .. } = &mut draft.message_error {
                    assignments.push(AssignmentDraft::selected(field));
                }
            }
            _ => {}
        });
    }
}
