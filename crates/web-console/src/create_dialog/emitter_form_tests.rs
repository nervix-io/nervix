//! Rendering and typed-choice contracts for the emitter form.
//!
//! Layer: test harness, outside product layer order.
//! - **Owns.** Checks that every current declaration is editable and dependency changes fence choices.
//! - **Depends on.** Leptos rendering, the current form and the public choice vocabulary.
//! - **Must not know.** Runtime publishing or external transport clients.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns visual emitter form tests")
)]

use leptos::prelude::*;
use meticulous::ResultExt as _;
use nervix_client_wire::{ChoiceTarget, ChoiceValue};
use nervix_models::{
    DomainName, EmitterEntityKind, FieldName, HttpEmitterEntity, ModelKind, ModelName, NodeRef,
};

use super::{
    ChoiceControl, CreateDialog, CreateDialogProps, CreateKind, CreateSignals,
    emitter_draft::BodyChoice, ingestor_route_draft::InheritDraft, select_choice,
};

fn node(kind: ModelKind, name: &str) -> NodeRef {
    NodeRef::new(
        kind,
        ModelName::parse(name).assured("the fixture names are valid"),
    )
}

#[test]
fn every_declared_emitter_renders_its_entity_options_body_modes_and_policies() {
    super::super::initialize_test_executor();
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        let domain = DomainName::parse("orders").assured("valid domain");
        let active_domain = RwSignal::new(Some(domain.clone()));
        let connection_state = RwSignal::new(super::super::ConsoleConnectionState::Waiting);
        let generation = RwSignal::new(1);
        let request_tx = RwSignal::new(None);
        signals.open(CreateKind::Emitter, Some(domain), "global-create-button");
        for kind in EmitterEntityKind::all() {
            signals.emitter.update(|draft| draft.choose_kind(kind));
            let dialog = CreateDialog(
                CreateDialogProps::builder()
                    .signals(signals)
                    .active_domain(active_domain)
                    .connection_state(connection_state)
                    .session_generation(generation)
                    .request_tx(request_tx)
                    .submit(|_, _, _| {})
                    .build(),
            );
            any_spawner::Executor::poll_local();
            let rendered = dialog.to_html();
            assert!(
                !rendered.contains("This declared entity option has no visual editor"),
                "{}",
                kind.phrase()
            );
            for control in [
                "create-emitter-input",
                "create-emitter-attachment",
                "create-emitter-collect",
                "create-emitter-state",
                "create-emitter-construction",
                "create-emitter-flush",
                "create-emitter-message-error",
                "create-emitter-general-error",
                "create-emitter-retry-backoff",
                "create-emitter-retry-max",
            ] {
                assert!(rendered.contains(control), "{}: {control}", kind.phrase());
            }
            for option in kind.capabilities().options {
                assert!(
                    rendered.contains(option.keywords),
                    "{}: {}",
                    kind.phrase(),
                    option.keywords
                );
            }
            for mode in kind.capabilities().publishing_modes {
                assert!(
                    rendered.contains(&format!("data-mode=\"{}\"", mode.kind.label())),
                    "{}: {}",
                    kind.phrase(),
                    mode.kind.label()
                );
            }
            if kind.capabilities().batch.is_available() {
                assert!(
                    rendered.contains("create-emitter-batch-enabled"),
                    "{}",
                    kind.phrase()
                );
            }
            if kind.capabilities().writes_headers() {
                assert!(
                    rendered.contains("create-emitter-add-invocation"),
                    "{}",
                    kind.phrase()
                );
            }
        }
    });
}

#[test]
fn emitter_questions_keep_output_schema_inputs_and_all_branch_dependencies_separate() {
    super::super::initialize_test_executor();
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        let domain = DomainName::parse("orders").assured("valid domain");
        signals.open(CreateKind::Emitter, Some(domain), "global-create-button");
        signals
            .emitter
            .update(|draft| draft.choose_kind(EmitterEntityKind::Http(HttpEmitterEntity::Request)));
        select_choice(
            signals,
            ChoiceControl::EmitterReference,
            ChoiceValue::Model(node(ModelKind::Client, "api")),
        );
        select_choice(
            signals,
            ChoiceControl::EmitterInputRelay,
            ChoiceValue::Model(node(ModelKind::Relay, "first")),
        );
        signals.emitter.update(|draft| draft.add_input());
        let inputs = signals
            .choice_query(ChoiceControl::EmitterInputRelay)
            .assured("additional inputs have a first input dependency");
        assert_eq!(inputs.target, ChoiceTarget::EmitterCompatibleInputRelay);
        select_choice(
            signals,
            ChoiceControl::EmitterInputRelay,
            ChoiceValue::Model(node(ModelKind::Relay, "second")),
        );
        let state = signals
            .choice_query(ChoiceControl::EmitterStateRelay)
            .assured("state sees every input");
        assert_eq!(state.dependencies.len(), 3);
        assert_eq!(state.target, ChoiceTarget::EmitterMaterializedRelay);
        let codec = signals
            .choice_query(ChoiceControl::EmitterCodec)
            .assured("output encoding is independent of the input schema");
        assert_eq!(codec.dependencies.len(), 1);
        select_choice(
            signals,
            ChoiceControl::EmitterCodec,
            ChoiceValue::Model(node(ModelKind::Codec, "encoder")),
        );
        select_choice(
            signals,
            ChoiceControl::EmitterOutputField,
            ChoiceValue::Field(FieldName::parse("body").assured("valid field")),
        );
        signals.emitter.update(|draft| {
            draft.inherit = InheritDraft::Fields(Vec::new());
            draft
                .inherit
                .add_field(FieldName::parse("secret").assured("valid field"));
            draft.inherit.set_leak(0, true);
        });
        assert_eq!(
            signals.emitter.get_untracked().inherit.leak_at(0),
            Some(true)
        );
        select_choice(
            signals,
            ChoiceControl::EmitterCodec,
            ChoiceValue::Model(node(ModelKind::Codec, "other_encoder")),
        );
        assert!(
            signals.emitter.get_untracked().assignments[0]
                .field
                .as_ref()
                .is_some_and(|field| !field.is_current())
        );
        signals
            .emitter
            .update(|draft| draft.choose_body(BodyChoice::WithoutBody));
        assert!(signals.emitter.get_untracked().assignments.is_empty());
        assert_eq!(signals.emitter.get_untracked().inherit, InheritDraft::None);
        signals.emitter.update(|draft| {
            draft.active_input = 0;
            draft.select_input(&node(ModelKind::Relay, "replacement"));
        });
        assert!(
            signals.emitter.get_untracked().inputs[1]
                .current_relay()
                .is_none()
        );
    });
}
