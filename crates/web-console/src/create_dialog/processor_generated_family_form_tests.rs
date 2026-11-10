//! Native rendering, typed-choice and submission checks for the generator, inferencer and WASM
//! processor forms.
//!
//! Layer: test harness, outside product layer order.
//! - **Owns.** Focused checks of the rendered generator, inferencer and WASM processor controls,
//!   the typed questions their materialized-relay, binding and field pickers ask, the references
//!   those pickers insert, their canonical submissions, and their invalidation by a changed
//!   domain.
//! - **Depends on.** Processor drafts, typed choices and the create dialog component.
//! - **Must not know.** Browser transport, registry validation or runtime processing.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]

use leptos::prelude::*;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{ChoiceSelection, ChoiceTarget, ChoiceValue};
use nervix_models::{
    DomainName, FieldName, GeneralErrorPolicy, ModelKind, ModelName, NodeRef,
    RequestedResourceVersion, ResourceName, WasmRejectedStatePolicy,
};
use nervix_plexus::client_statement::{ClientStatement, parse_client_statement};

use super::{
    ChoiceControl, CreateDialog, CreateDialogProps, CreateDispatch, CreateKind, CreateSignals,
    ingestor_route_draft::{FlushDraft, MessageErrorDraft},
    processor_draft::ProcessorDraftError,
    processor_inferencer_draft::{DimensionDraft, StateInitialDraft},
    resource_pin_draft::ResourcePinError,
    select_choice, selected_choice,
};

fn node(kind: ModelKind, name: &str) -> NodeRef {
    NodeRef::new(kind, ModelName::parse(name).assured("test name is valid"))
}

fn field(name: &str) -> FieldName {
    FieldName::parse(name).assured("test field is valid")
}

fn resource(name: &str) -> ResourceName {
    ResourceName::parse(name).assured("test resource is valid")
}

fn models() -> DomainName {
    DomainName::parse("models").assured("valid domain")
}

fn render(signals: CreateSignals) -> String {
    let dialog = CreateDialog(
        CreateDialogProps::builder()
            .signals(signals)
            .active_domain(RwSignal::new(Some(models())))
            .connection_state(RwSignal::new(super::super::ConsoleConnectionState::Waiting))
            .session_generation(RwSignal::new(1))
            .request_tx(RwSignal::new(None))
            .submit(super::ignore_test_submission)
            .build(),
    );
    any_spawner::Executor::poll_local();
    dialog.to_html()
}

/// The typed question `control` asks, as the session receives it.
fn question(signals: CreateSignals, control: ChoiceControl) -> (ChoiceTarget, Vec<ChoiceValue>) {
    let query = signals
        .choice_query(control)
        .assured("the control has its prerequisites");
    let dependencies = query
        .dependencies
        .into_iter()
        .map(|dependency: ChoiceSelection| dependency.value)
        .collect();
    (query.target, dependencies)
}

fn missing(signals: CreateSignals, control: ChoiceControl) -> &'static str {
    signals
        .choice_query(control)
        .err()
        .assured("the control lacks a prerequisite")
}

fn assert_renders(rendered: &str, form: &str, present: &[&str], absent: &[&str]) {
    for control in present {
        assert!(rendered.contains(control), "{form}: {control} must render");
    }
    for control in absent {
        assert!(
            !rendered.contains(control),
            "{form}: {control} must not render"
        );
    }
}

#[test]
fn generated_family_forms_render_their_own_sections_and_omit_what_they_do_not_declare() {
    super::super::initialize_test_executor();
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        signals.open(
            CreateKind::Generator,
            Some(models()),
            "global-create-button",
        );
        signals.generator.update(|draft| draft.choose_branched());
        let generator = render(signals);
        assert_renders(
            &generator,
            "generator",
            &[
                "create-generator-source",
                "create-processor-branching",
                "create-processor-branch",
                "create-generator-relay",
                "create-generator-each",
                "create-processor-route-relay",
                "create-processor-output-fields",
                "create-generator-state-fields",
                "create-processor-branch-key-fields",
                "create-processor-flush",
                "create-processor-message-error",
            ],
            &[
                "create-processor-input-list",
                "create-processor-mode",
                "create-processor-filter",
                "create-processor-state-list",
                "create-processor-inherit",
                "create-processor-invocations",
                "create-processor-binding",
                "create-wasm-guest-fields",
            ],
        );

        signals.open(
            CreateKind::Inferencer,
            Some(models()),
            "global-create-button",
        );
        signals.inferencer.update(|draft| {
            draft.choose_unbranched();
            if let Some(inferencer) = draft.inferencer_mut() {
                inferencer.state.add();
                if let Some(binding) = inferencer.state.active_mut() {
                    binding.initial = StateInitialDraft::Constant(String::new());
                }
            }
        });
        let inferencer = render(signals);
        assert_renders(
            &inferencer,
            "inferencer",
            &[
                "create-processor-input-list",
                "create-processor-mode",
                "create-processor-filter",
                "create-processor-binding",
                "create-processor-resource",
                "create-processor-resource-version",
                "create-processor-file",
                "create-inferencer-tensors",
                "create-inferencer-execution-mode",
                "create-inferencer-input-name",
                "create-inferencer-input-shape",
                "create-tensor-dimension-size",
                "create-tensor-add-dimension",
                "create-inferencer-input-expression",
                "create-inferencer-input-fields",
                "create-inferencer-state-input",
                "create-inferencer-state-output",
                "create-inferencer-state-shape",
                "create-inferencer-state-initial",
                "create-inferencer-state-constant",
                "create-inferencer-output-name",
                "create-inferencer-output-shape",
                "create-processor-state-list",
                "create-processor-flush",
                "create-inferencer-generated",
            ],
            &[
                "create-processor-inherit",
                "create-processor-invocations",
                "create-generator-relay",
                "create-wasm-max-fuel",
                "create-processor-branch-key-fields",
            ],
        );

        signals.open(
            CreateKind::WasmProcessor,
            Some(models()),
            "global-create-button",
        );
        signals.wasm.update(|draft| draft.choose_branched());
        let wasm = render(signals);
        assert_renders(
            &wasm,
            "WASM processor",
            &[
                "create-processor-input-list",
                "create-processor-mode",
                "create-processor-filter",
                "create-processor-binding",
                "create-wasm-limits",
                "create-wasm-max-fuel",
                "create-wasm-max-memory",
                "create-processor-state-list",
                "create-wasm-guest-fields",
                "create-processor-branch-key-fields",
                "create-wasm-policies",
                "create-wasm-rejected-state",
                "create-wasm-rejected-state-consequence",
                "create-wasm-global-error",
                "keeps the snapshot",
            ],
            &[
                "create-processor-flush",
                "create-processor-inherit",
                "create-processor-invocations",
                "create-inferencer-tensors",
                "create-generator-relay",
            ],
        );
    });
}

#[test]
fn a_generator_asks_for_materialized_relays_on_exactly_its_branch_and_routes_by_that_relay() {
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        signals.open(
            CreateKind::Generator,
            Some(models()),
            "global-create-button",
        );
        assert_eq!(
            missing(signals, ChoiceControl::GeneratorRelay),
            "Choose the generator branch before its materialized relay"
        );
        signals.generator.update(|draft| draft.choose_unbranched());
        assert_eq!(
            question(signals, ChoiceControl::GeneratorRelay),
            (
                ChoiceTarget::GeneratorMaterializedRelay,
                vec![ChoiceValue::Domain(models())]
            )
        );
        signals.generator.update(|draft| draft.choose_branched());
        assert_eq!(
            missing(signals, ChoiceControl::GeneratorRelay),
            "Choose the named generator branch before its materialized relay"
        );
        select_choice(
            signals,
            ChoiceControl::ProcessorBranch,
            ChoiceValue::Model(node(ModelKind::Branch, "by_tenant")),
        );
        assert_eq!(
            question(signals, ChoiceControl::GeneratorRelay),
            (
                ChoiceTarget::GeneratorMaterializedRelay,
                vec![
                    ChoiceValue::Domain(models()),
                    ChoiceValue::Model(node(ModelKind::Branch, "by_tenant")),
                ]
            )
        );
        // The routes keep the materialized relay's branch, so they wait for that relay.
        assert_eq!(
            missing(signals, ChoiceControl::ProcessorRouteRelay),
            "Choose the materialized relay before its output relay"
        );
        assert_eq!(
            missing(signals, ChoiceControl::ProcessorInputRelay),
            "A generator reads the materialized state of one relay, not inputs"
        );
        let notifications = ChoiceValue::Model(node(ModelKind::Relay, "notifications"));
        select_choice(
            signals,
            ChoiceControl::GeneratorRelay,
            notifications.clone(),
        );
        assert!(selected_choice(
            signals,
            ChoiceControl::GeneratorRelay,
            &notifications
        ));
        assert!(!selected_choice(
            signals,
            ChoiceControl::GeneratorRelay,
            &ChoiceValue::Model(node(ModelKind::Relay, "other"))
        ));
        assert_eq!(
            question(signals, ChoiceControl::ProcessorRouteRelay),
            (
                ChoiceTarget::ProcessorInputBranchRelay,
                vec![ChoiceValue::Domain(models()), notifications.clone()]
            )
        );
        assert_eq!(
            question(signals, ChoiceControl::ProcessorErrorRelay),
            (
                ChoiceTarget::ProcessorInputBranchRelay,
                vec![ChoiceValue::Domain(models()), notifications.clone()]
            )
        );

        // A route's state and branch-key pickers insert into its last output assignment.
        assert_eq!(
            missing(signals, ChoiceControl::GeneratorStateField),
            "Add an output assignment before a field to assign"
        );
        select_choice(
            signals,
            ChoiceControl::ProcessorRouteRelay,
            ChoiceValue::Model(node(ModelKind::Relay, "generated")),
        );
        select_choice(
            signals,
            ChoiceControl::ProcessorOutputField,
            ChoiceValue::Field(field("tenant")),
        );
        assert_eq!(
            question(signals, ChoiceControl::ProcessorBranchKeyField),
            (
                ChoiceTarget::BranchField,
                vec![
                    ChoiceValue::Domain(models()),
                    ChoiceValue::Model(node(ModelKind::Branch, "by_tenant")),
                ]
            )
        );
        select_choice(
            signals,
            ChoiceControl::ProcessorBranchKeyField,
            ChoiceValue::Field(field("tenant")),
        );
        select_choice(
            signals,
            ChoiceControl::ProcessorOutputField,
            ChoiceValue::Field(field("total")),
        );
        assert_eq!(
            question(signals, ChoiceControl::GeneratorStateField),
            (
                ChoiceTarget::RelayField,
                vec![ChoiceValue::Domain(models()), notifications]
            )
        );
        select_choice(
            signals,
            ChoiceControl::GeneratorStateField,
            ChoiceValue::Field(field("amount")),
        );
        let draft = signals.generator.get_untracked();
        let route = draft.active_route().assured("the route exists");
        assert_eq!(route.assignments[0].expression, "branch.tenant");
        assert_eq!(
            route.assignments[1].expression,
            "relay_state.notifications.amount"
        );
        assert!(
            !signals
                .processor_open_controls()
                .contains(&ChoiceControl::ProcessorInputRelay)
        );

        // Another materialized relay requires the routes it offered again.
        select_choice(
            signals,
            ChoiceControl::GeneratorRelay,
            ChoiceValue::Model(node(ModelKind::Relay, "other_state")),
        );
        let draft = signals.generator.get_untracked();
        assert!(
            draft
                .active_route()
                .assured("the route exists")
                .current_relay()
                .is_none()
        );
    });
}

#[test]
fn inferencer_and_wasm_pickers_ask_for_their_binding_and_the_fields_their_routes_may_read() {
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        signals.open(
            CreateKind::Inferencer,
            Some(models()),
            "global-create-button",
        );
        assert_eq!(
            question(signals, ChoiceControl::ProcessorResource),
            (ChoiceTarget::Resource, vec![ChoiceValue::Domain(models())])
        );
        assert_eq!(
            missing(signals, ChoiceControl::ProcessorResourceVersion),
            "Select a resource to list its completed versions"
        );
        let bundle = ChoiceValue::Resource(resource("scoring"));
        select_choice(signals, ChoiceControl::ProcessorResource, bundle.clone());
        assert!(selected_choice(
            signals,
            ChoiceControl::ProcessorResource,
            &bundle
        ));
        assert_eq!(
            question(signals, ChoiceControl::ProcessorResourceVersion),
            (
                ChoiceTarget::CompletedResourceVersion,
                vec![ChoiceValue::Domain(models()), bundle.clone()]
            )
        );
        let latest = ChoiceValue::ResourceVersion(RequestedResourceVersion::Latest);
        select_choice(
            signals,
            ChoiceControl::ProcessorResourceVersion,
            latest.clone(),
        );
        assert!(selected_choice(
            signals,
            ChoiceControl::ProcessorResourceVersion,
            &latest
        ));
        assert!(!selected_choice(
            signals,
            ChoiceControl::ProcessorResourceVersion,
            &ChoiceValue::ResourceVersion(RequestedResourceVersion::Number(1))
        ));

        assert_eq!(
            missing(signals, ChoiceControl::InferencerInputField),
            "Choose an input relay before its fields"
        );
        // Its input relays are offered by the node-wide branch, which comes first.
        assert_eq!(
            missing(signals, ChoiceControl::ProcessorInputRelay),
            "Choose the inferencer branch before its input relays"
        );
        signals.inferencer.update(|draft| draft.choose_branched());
        assert_eq!(
            missing(signals, ChoiceControl::ProcessorInputRelay),
            "Choose the named inferencer branch before its input relays"
        );
        signals.inferencer.update(|draft| draft.choose_unbranched());
        select_choice(
            signals,
            ChoiceControl::ProcessorInputRelay,
            ChoiceValue::Model(node(ModelKind::Relay, "features")),
        );
        assert_eq!(
            question(signals, ChoiceControl::InferencerInputField),
            (
                ChoiceTarget::RelayField,
                vec![
                    ChoiceValue::Domain(models()),
                    ChoiceValue::Model(node(ModelKind::Relay, "features")),
                ]
            )
        );
        select_choice(
            signals,
            ChoiceControl::InferencerInputField,
            ChoiceValue::Field(field("vector")),
        );
        // An unbranched inferencer has no branch key to read.
        assert_eq!(
            missing(signals, ChoiceControl::ProcessorBranchKeyField),
            "Choose the named branch before its key fields"
        );
        signals.inferencer.update(|draft| {
            if let Some(route) = draft.active_route_mut() {
                route.add_assignment(field("score"));
            }
        });
        signals
            .inferencer
            .update(|draft| draft.insert_generated_reference(field("score")));
        let draft = signals.inferencer.get_untracked();
        assert_eq!(
            draft
                .inferencer()
                .assured("an inferencer draft holds its model")
                .inputs
                .active()
                .assured("one input exists")
                .expression,
            "input.vector"
        );
        assert_eq!(
            draft.active_route().assured("the route exists").assignments[0].expression,
            "score"
        );

        signals.open(
            CreateKind::WasmProcessor,
            Some(models()),
            "global-create-button",
        );
        assert_eq!(
            missing(signals, ChoiceControl::WasmGuestField),
            "Choose the output relay before the guest fields aligned to it"
        );
        assert_eq!(
            missing(signals, ChoiceControl::ProcessorInputRelay),
            "Choose the WASM processor branch before its input relays"
        );
        signals.wasm.update(|draft| draft.choose_branched());
        assert_eq!(
            missing(signals, ChoiceControl::ProcessorInputRelay),
            "Choose the named WASM processor branch before its input relays"
        );
        signals.wasm.update(|draft| draft.choose_unbranched());
        select_choice(
            signals,
            ChoiceControl::ProcessorInputRelay,
            ChoiceValue::Model(node(ModelKind::Relay, "events")),
        );
        select_choice(
            signals,
            ChoiceControl::ProcessorRouteRelay,
            ChoiceValue::Model(node(ModelKind::Relay, "tagged")),
        );
        assert_eq!(
            missing(signals, ChoiceControl::WasmGuestField),
            "Add an output assignment before a field to assign"
        );
        select_choice(
            signals,
            ChoiceControl::ProcessorOutputField,
            ChoiceValue::Field(field("bucket")),
        );
        assert_eq!(
            question(signals, ChoiceControl::WasmGuestField),
            (
                ChoiceTarget::RelayField,
                vec![
                    ChoiceValue::Domain(models()),
                    ChoiceValue::Model(node(ModelKind::Relay, "tagged")),
                ]
            )
        );
        select_choice(
            signals,
            ChoiceControl::WasmGuestField,
            ChoiceValue::Field(field("bucket")),
        );
        let draft = signals.wasm.get_untracked();
        assert_eq!(
            draft.active_route().assured("the route exists").assignments[0].expression,
            "bucket"
        );
        // Each form keeps its own binding.
        assert!(!selected_choice(
            signals,
            ChoiceControl::ProcessorResource,
            &bundle
        ));
    });
}

fn submitted_statement(signals: CreateSignals) -> String {
    let submission = signals.submission().assured("the draft is complete");
    let CreateDispatch::Command(command) = &submission.dispatch else {
        panic!("a processor form dispatches a command");
    };
    assert_eq!(command.domain, Some(models()));
    let parsed =
        parse_client_statement(&command.query).assured("the submitted statement is executable");
    assert!(matches!(parsed, ClientStatement::Server(_)));
    submission.presentation
}

#[test]
fn generated_family_submissions_render_their_canonical_statements() {
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        signals.open(CreateKind::Generator, Some(models()), "global-create-button");
        signals.generator.update(|draft| {
            draft.name = "synth".into();
            draft.choose_unbranched();
            draft.select_generator_relay(&node(ModelKind::Relay, "notifications"));
            if let Some(generator) = draft.generator_mut() {
                generator.each = "250ms".into();
            }
            if let Some(route) = draft.active_route_mut() {
                route.select_relay(&node(ModelKind::Relay, "generated"));
                route.add_assignment(field("total"));
                route.assignments[0].expression = "relay_state.notifications.amount".into();
                route.flush = FlushDraft::Immediate;
                route.message_error = MessageErrorDraft::Log;
            }
        });
        let generator = submitted_statement(signals);
        assert!(
            generator.contains(
                "CREATE GENERATOR synth\n  USING MATERIALIZED STATE notifications\n  EACH 250ms\n  UNBRANCHED"
            ),
            "{generator}"
        );

        signals.open(CreateKind::Inferencer, Some(models()), "global-create-button");
        signals.inferencer.update(|draft| {
            draft.name = "running_total".into();
            draft.choose_unbranched();
            draft.select_input(&node(ModelKind::Relay, "features"));
            if let Some(inferencer) = draft.inferencer_mut() {
                inferencer.binding.pin.select_resource(resource("scoring"));
                inferencer
                    .binding
                    .pin
                    .select_version(RequestedResourceVersion::Latest);
                inferencer.binding.file = "models/running_sum.onnx".into();
                if let Some(mapping) = inferencer.inputs.active_mut() {
                    mapping.tensor = "features".into();
                    mapping.shape.dimensions = vec![DimensionDraft::Size("2".into())];
                    mapping.expression = "input.vector".into();
                }
                inferencer.state.add();
                if let Some(binding) = inferencer.state.active_mut() {
                    binding.input = "state_in".into();
                    binding.output = "state_out".into();
                    binding.shape.dimensions = vec![DimensionDraft::Size("2".into())];
                    binding.initial = StateInitialDraft::Zeros;
                }
                if let Some(declaration) = inferencer.outputs.active_mut() {
                    declaration.tensor = "score".into();
                    declaration.shape.dimensions = vec![DimensionDraft::Size("1".into())];
                }
            }
            if let Some(route) = draft.active_route_mut() {
                route.select_relay(&node(ModelKind::Relay, "scores"));
                route.add_assignment(field("score"));
                route.assignments[0].expression = "score".into();
                route.flush = FlushDraft::Immediate;
                route.message_error = MessageErrorDraft::Log;
            }
        });
        let inferencer = submitted_statement(signals);
        for clause in [
            "USING RESOURCE scoring VERSION LATEST",
            "FILE 'models/running_sum.onnx'",
            "'features' DENSE TENSOR<F32>[2] = input.vector",
            "'state_in' FROM 'state_out' DENSE TENSOR<F32>[2] INITIAL ZEROS",
            "'score' DENSE TENSOR<F32>[1]",
        ] {
            assert!(inferencer.contains(clause), "{clause}: {inferencer}");
        }

        signals.open(
            CreateKind::WasmProcessor,
            Some(models()),
            "global-create-button",
        );
        signals.wasm.update(|draft| {
            draft.name = "normalize".into();
            draft.choose_unbranched();
            draft.select_input(&node(ModelKind::Relay, "events"));
            if let Some(wasm) = draft.wasm_mut() {
                wasm.binding.pin.select_resource(resource("guests"));
                wasm.binding
                    .pin
                    .select_version(RequestedResourceVersion::Number(2));
                wasm.binding.file = "processors/filter_even.wasm".into();
                wasm.max_fuel = "1000000000".into();
                wasm.max_memory = "64MiB".into();
                wasm.rejected_state = WasmRejectedStatePolicy::Reset;
                wasm.global_error = Some(GeneralErrorPolicy::Log);
            }
            if let Some(route) = draft.active_route_mut() {
                route.select_relay(&node(ModelKind::Relay, "tagged"));
                route.add_assignment(field("value"));
                route.assignments[0].expression = "value".into();
                route.message_error = MessageErrorDraft::Log;
            }
        });
        let wasm = submitted_statement(signals);
        for clause in [
            "USING RESOURCE guests VERSION 2",
            "FILE 'processors/filter_even.wasm'",
            "MAX FUEL 1000000000",
            "MAX MEMORY 64MiB",
            "ON REJECTED STATE RESET",
            "ON GLOBAL ERROR LOG",
        ] {
            assert!(wasm.contains(clause), "{clause}: {wasm}");
        }
        assert!(!wasm.contains("FLUSH"), "{wasm}");
    });
}

fn refusal(signals: CreateSignals, kind: CreateKind) -> ProcessorDraftError {
    let family = kind
        .processor_family()
        .assured("the kind has a processor form");
    signals
        .processor(family)
        .get_untracked()
        .build()
        .err()
        .assured("the draft is incomplete")
        .current_context()
        .clone()
}

#[test]
fn a_changed_domain_requires_the_materialized_relay_and_the_binding_again() {
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        signals.open(
            CreateKind::Generator,
            Some(models()),
            "global-create-button",
        );
        signals.generator.update(|draft| {
            draft.name = "synth".into();
            draft.choose_unbranched();
            draft.select_generator_relay(&node(ModelKind::Relay, "notifications"));
        });
        signals.change_scope(Some(DomainName::parse("other").assured("valid domain")));
        assert_eq!(
            refusal(signals, CreateKind::Generator),
            ProcessorDraftError::GeneratorRelayChanged
        );

        signals.open(
            CreateKind::Inferencer,
            Some(models()),
            "global-create-button",
        );
        signals.inferencer.update(|draft| {
            draft.name = "score".into();
            draft.choose_unbranched();
            draft.select_input(&node(ModelKind::Relay, "features"));
            if let Some(binding) = draft.binding_mut() {
                binding.pin.select_resource(resource("scoring"));
                binding
                    .pin
                    .select_version(RequestedResourceVersion::Number(1));
                binding.file = "models/simple_score.onnx".into();
            }
        });
        signals.change_scope(Some(DomainName::parse("other").assured("valid domain")));
        signals.inferencer.update(|draft| {
            draft.select_input(&node(ModelKind::Relay, "features"));
        });
        assert_eq!(
            refusal(signals, CreateKind::Inferencer),
            ProcessorDraftError::Binding(ResourcePinError::ResourceChanged)
        );
        let draft = signals.inferencer.get_untracked();
        assert_eq!(
            draft
                .binding()
                .assured("an inferencer binds a resource")
                .file,
            "models/simple_score.onnx"
        );
    });
}
