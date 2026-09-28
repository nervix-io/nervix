//! Native rendering checks for the codec and signaling browser forms.
//!
//! Layer: edges.
//!
//! - **Owns.** Focused checks of the rendered current visual create controls.
//! - **Depends on.** Browser drafts, typed choices, and the dialog component.
//! - **Must not know.** Browser transport or runtime codec and handshake execution.

use leptos::prelude::*;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{ChoiceTarget, ChoiceValue};
use nervix_models::{
    DomainName, ModelKind, ModelName, NodeRef, RequestedResourceVersion, ResourceName,
};

use super::{
    ChoiceControl, CreateDialog, CreateDialogProps, CreateKind, CreateSignals,
    codec_draft::{CodecFormatDraft, CodecFormatKind},
    open_form_controls,
    resource_binding_draft::ConfigEntryDraft,
    select_choice, selected_choice,
    signaling_draft::{SignalingFormatDraft, SignalingFormatKind, SignalingStepDraft},
};

fn node(kind: ModelKind, name: &str) -> NodeRef {
    NodeRef::new(
        kind,
        ModelName::parse(name).assured("test reference name is valid"),
    )
}

#[test]
fn codec_and_signaling_forms_render_every_current_editor_family() {
    super::super::initialize_test_executor();
    Owner::new().with(|| {
        let scope = DomainName::parse("orders").assured("test domain is valid");
        let active_domain = RwSignal::new(Some(scope.clone()));
        let connection_state = RwSignal::new(super::super::ConsoleConnectionState::Waiting);
        let generation = RwSignal::new(1);
        let request_tx = RwSignal::new(None);
        let signals = CreateSignals::new();
        let render = || {
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
            dialog.to_html()
        };

        signals.open(
            CreateKind::Codec,
            Some(scope.clone()),
            "global-create-button",
        );
        signals.codec.update(|draft| {
            draft.name = "wire_codec".to_string();
            draft.select_schema(&node(ModelKind::Schema, "payload"));
            draft.set_format(CodecFormatKind::WireJson);
            draft.select_wire_schema(&node(ModelKind::WireJsonSchema, "payload_wire"));
        });
        let wire = render();
        assert!(wire.contains("Create codec"));
        assert!(wire.contains("FROM WIRE JSON SCHEMA payload_wire"));
        assert!(wire.contains("create-codec-wire-schema"));

        signals.codec.update(|draft| {
            draft.set_format(CodecFormatKind::JaqJson);
            let transforms = draft
                .transformations_mut()
                .verified("JAQ format has transformations");
            transforms.ingestion = Some(".".to_string());
            transforms.emitting = Some("{message: .message}".to_string());
            transforms.emitting_batch = Some("{records: .}".to_string());
        });
        let jaq = render();
        assert!(jaq.contains("create-codec-ingestion-program"));
        assert!(jaq.contains("create-codec-emitting-program"));
        assert!(jaq.contains("create-codec-batch-program"));
        assert!(jaq.contains("ON EMITTING BATCH"));

        signals.codec.update(|draft| {
            draft.set_format(CodecFormatKind::Protobuf);
            if let Some(CodecFormatDraft::Protobuf {
                binding,
                message,
                transformations,
                ..
            }) = &mut draft.format
            {
                binding.select_resource(
                    ResourceName::parse("proto_bundle").assured("test resource is valid"),
                );
                binding.select_version(RequestedResourceVersion::Latest);
                binding.file = "payload.proto".to_string();
                binding.config.push(ConfigEntryDraft {
                    key: "optimize_for".to_string(),
                    value: "SPEED".to_string(),
                });
                *message = "pkg.Payload".to_string();
                transformations.ingestion = Some(".".to_string());
            }
        });
        let protobuf = render();
        assert!(protobuf.contains("create-protobuf-resource"));
        assert!(protobuf.contains("Requested version: LATEST"));
        assert!(protobuf.contains("create-config-key"));
        assert!(protobuf.contains("USING RESOURCE proto_bundle VERSION LATEST"));

        signals.open(
            CreateKind::SignalingProtocol,
            Some(scope),
            "global-create-button",
        );
        signals.signaling.update(|draft| {
            draft.name = "handshake".to_string();
            draft.set_format(SignalingFormatKind::Json);
            draft.steps.push(SignalingStepDraft::Send {
                programs: vec!["{id: 1}".to_string()],
            });
            draft.steps.push(SignalingStepDraft::Wait {
                matchers: vec![".id == 1".to_string()],
                capture: Some("{token: .token}".to_string()),
                fail_matchers: vec![".error".to_string()],
                accept_data: true,
            });
            draft.fail_matchers.push(".fatal".to_string());
            draft.timeout = "5s".to_string();
        });
        let signaling = render();
        assert!(signaling.contains("Create signaling protocol"));
        assert!(signaling.contains("create-signaling-send-program"));
        assert!(signaling.contains("create-signaling-wait-matcher"));
        assert!(signaling.contains("create-signaling-capture"));
        assert!(signaling.contains("CAPTURE"));

        signals.signaling.update(|draft| {
            draft.set_format(SignalingFormatKind::Protobuf);
            if let Some(SignalingFormatDraft::Protobuf {
                binding,
                send_message,
                wait_message,
            }) = &mut draft.format
            {
                binding.select_resource(
                    ResourceName::parse("proto_bundle").assured("test resource is valid"),
                );
                binding.select_version(RequestedResourceVersion::Number(2));
                *send_message = "pkg.Send".to_string();
                *wait_message = "pkg.Wait".to_string();
            }
        });
        let protocol_protobuf = render();
        assert!(protocol_protobuf.contains("create-signaling-send-message"));
        assert!(protocol_protobuf.contains("Requested version: 2"));
        assert!(protocol_protobuf.contains("SEND MESSAGE 'pkg.Send' WAIT MESSAGE 'pkg.Wait'"));
    });
}

#[test]
fn typed_resource_and_wire_schema_choices_select_only_the_matching_form() {
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        signals
            .codec
            .update(|draft| draft.set_format(CodecFormatKind::WireJson));
        let wire = ChoiceValue::Model(node(ModelKind::WireJsonSchema, "payload_wire"));
        select_choice(signals, ChoiceControl::CodecWireSchema, wire.clone());
        assert!(selected_choice(
            signals,
            ChoiceControl::CodecWireSchema,
            &wire
        ));
        let cbor = ChoiceValue::Model(node(ModelKind::WireCborSchema, "payload_wire"));
        assert!(!selected_choice(
            signals,
            ChoiceControl::CodecWireSchema,
            &cbor
        ));

        signals
            .codec
            .update(|draft| draft.set_format(CodecFormatKind::Protobuf));
        let resource =
            ChoiceValue::Resource(ResourceName::parse("proto_bundle").assured("valid resource"));
        select_choice(signals, ChoiceControl::CodecResource, resource.clone());
        assert!(selected_choice(
            signals,
            ChoiceControl::CodecResource,
            &resource
        ));
        let version = ChoiceValue::ResourceVersion(RequestedResourceVersion::Latest);
        select_choice(signals, ChoiceControl::CodecVersion, version.clone());
        assert!(selected_choice(
            signals,
            ChoiceControl::CodecVersion,
            &version
        ));

        signals
            .signaling
            .update(|draft| draft.set_format(SignalingFormatKind::Protobuf));
        select_choice(signals, ChoiceControl::SignalingResource, resource.clone());
        assert!(selected_choice(
            signals,
            ChoiceControl::SignalingResource,
            &resource
        ));
        select_choice(signals, ChoiceControl::SignalingVersion, version.clone());
        assert!(selected_choice(
            signals,
            ChoiceControl::SignalingVersion,
            &version
        ));
        assert!(!selected_choice(
            signals,
            ChoiceControl::SignalingVersion,
            &resource
        ));
    });
}

#[test]
fn changing_domain_invalidates_codec_and_signaling_references() {
    Owner::new().with(|| {
        let orders = DomainName::parse("orders").assured("test domain is valid");
        let other = DomainName::parse("other").assured("test domain is valid");
        let signals = CreateSignals::new();

        signals.open(CreateKind::Codec, Some(orders.clone()), "global-create-button");
        signals.codec.update(|draft| {
            draft.name = "selected_codec".to_string();
            draft.select_schema(&node(ModelKind::Schema, "payload"));
            draft.set_format(CodecFormatKind::WireJson);
            draft.select_wire_schema(&node(ModelKind::WireJsonSchema, "payload_wire"));
        });
        signals.change_scope(Some(other.clone()));
        let codec = signals.codec.get_untracked();
        assert!(codec.schema.as_ref().is_some_and(|selected| !selected.is_current()));
        assert!(matches!(codec.format, Some(CodecFormatDraft::Wire { schema: Some(ref selected), .. }) if !selected.is_current()));

        signals.open(CreateKind::SignalingProtocol, Some(orders), "global-create-button");
        signals.signaling.update(|draft| {
            draft.set_format(SignalingFormatKind::Protobuf);
            let binding = draft.binding_mut().verified("Protobuf signaling has a binding");
            binding.select_resource(ResourceName::parse("proto_bundle").assured("valid resource"));
            binding.select_version(RequestedResourceVersion::Latest);
        });
        signals.change_scope(Some(other));
        let signaling = signals.signaling.get_untracked();
        let binding = signaling.binding().verified("Protobuf signaling has a binding");
        assert!(binding.resource.as_ref().is_some_and(|selected| !selected.is_current()));
        assert!(binding.version.as_ref().is_some_and(|selected| !selected.is_current()));
    });
}

#[test]
fn codec_and_signaling_choice_queries_follow_format_and_resource_dependencies() {
    Owner::new().with(|| {
        let scope = DomainName::parse("orders").assured("test domain is valid");
        let resource = ResourceName::parse("proto_bundle").assured("test resource is valid");
        let signals = CreateSignals::new();
        signals.open(CreateKind::Codec, None, "global-create-button");
        assert_eq!(
            signals.choice_query(ChoiceControl::CodecVersion).err(),
            Some("Select a domain before choosing a resource version")
        );
        signals.change_scope(Some(scope.clone()));
        assert_eq!(
            signals
                .choice_query(ChoiceControl::CodecSchema)
                .assured("codec schema lookup is scoped")
                .target,
            ChoiceTarget::Schema
        );
        for (format, target) in [
            (CodecFormatKind::WireJson, ChoiceTarget::WireJsonSchema),
            (CodecFormatKind::WireCbor, ChoiceTarget::WireCborSchema),
            (CodecFormatKind::WireAvro, ChoiceTarget::WireAvroSchema),
        ] {
            signals.codec.update(|draft| draft.set_format(format));
            let query = signals
                .choice_query(ChoiceControl::CodecWireSchema)
                .assured("wire lookup has a format");
            assert_eq!(query.target, target);
            assert_eq!(
                query.dependencies[0].value,
                ChoiceValue::Domain(scope.clone())
            );
        }
        signals
            .codec
            .update(|draft| draft.set_format(CodecFormatKind::Protobuf));
        assert_eq!(
            signals
                .choice_query(ChoiceControl::CodecResource)
                .assured("resource lookup is scoped")
                .target,
            ChoiceTarget::Resource
        );
        assert_eq!(
            signals.choice_query(ChoiceControl::CodecVersion).err(),
            Some("Select a resource to list its completed versions")
        );
        signals.codec.update(|draft| {
            draft
                .binding_mut()
                .verified("codec is Protobuf")
                .select_resource(resource.clone())
        });
        let version = signals
            .choice_query(ChoiceControl::CodecVersion)
            .assured("codec resource is selected");
        assert_eq!(version.target, ChoiceTarget::CompletedResourceVersion);
        assert_eq!(
            version
                .dependencies
                .iter()
                .map(|selection| selection.value.clone())
                .collect::<Vec<_>>(),
            [
                ChoiceValue::Domain(scope.clone()),
                ChoiceValue::Resource(resource.clone()),
            ]
        );
        assert_eq!(
            open_form_controls(signals, CreateKind::Codec),
            [
                ChoiceControl::CodecSchema,
                ChoiceControl::CodecResource,
                ChoiceControl::CodecVersion,
            ]
        );

        signals.open(
            CreateKind::SignalingProtocol,
            Some(scope.clone()),
            "global-create-button",
        );
        signals
            .signaling
            .update(|draft| draft.set_format(SignalingFormatKind::Protobuf));
        assert_eq!(
            signals
                .choice_query(ChoiceControl::SignalingResource)
                .assured("signaling resource lookup is scoped")
                .target,
            ChoiceTarget::Resource
        );
        assert_eq!(
            signals.choice_query(ChoiceControl::SignalingVersion).err(),
            Some("Select a resource to list its completed versions")
        );
        signals.signaling.update(|draft| {
            draft
                .binding_mut()
                .verified("signaling is Protobuf")
                .select_resource(resource.clone())
        });
        let version = signals
            .choice_query(ChoiceControl::SignalingVersion)
            .assured("signaling resource is selected");
        assert_eq!(version.target, ChoiceTarget::CompletedResourceVersion);
        assert_eq!(
            version.dependencies[1].value,
            ChoiceValue::Resource(resource)
        );
        assert_eq!(
            open_form_controls(signals, CreateKind::SignalingProtocol),
            [
                ChoiceControl::SignalingResource,
                ChoiceControl::SignalingVersion,
            ]
        );
        assert_eq!(
            ChoiceControl::SignalingVersion.form(),
            CreateKind::SignalingProtocol
        );
        assert_eq!(ChoiceControl::CodecVersion.form(), CreateKind::Codec);
    });
}
