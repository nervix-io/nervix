//! Native rendering and typed-choice checks for the visual create forms.
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
    DomainName, FieldName, ModelKind, ModelName, NodeRef, ParseAsType, RequestedResourceVersion,
    ResourceName,
};

use super::{
    ChoiceControl, CreateDialog, CreateDialogProps, CreateDispatch, CreateKind, CreateSignals,
    client_draft::{ClientConfigDraft, ClientTransport},
    codec_draft::{CodecFormatDraft, CodecFormatKind},
    open_form_controls,
    resource_binding_draft::ConfigEntryDraft,
    schema_draft::CollectionLayer,
    select_choice, selected_choice,
    signaling_draft::{SignalingFormatDraft, SignalingFormatKind, SignalingStepDraft},
    udf_draft::UdfArgumentDraft,
};

#[test]
fn hash_map_and_roto_udf_forms_render_current_model_controls() {
    super::super::initialize_test_executor();
    Owner::new().with(|| {
        let domain = DomainName::parse("orders").assured("valid domain");
        let signals = CreateSignals::new();
        let active_domain = RwSignal::new(Some(domain.clone()));
        let connection_state = RwSignal::new(super::super::ConsoleConnectionState::Waiting);
        let generation = RwSignal::new(1);
        let request_tx = RwSignal::new(None);
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
            CreateKind::HashMap,
            Some(domain.clone()),
            "global-create-button",
        );
        signals.hash_map.update(|draft| {
            draft.name = "by_id".to_string();
            draft.path = "lookup.jsonl".to_string();
            draft
                .pin
                .select_resource(ResourceName::parse("bundle").assured("valid resource"));
            draft.pin.select_version(RequestedResourceVersion::Latest);
            draft.select_codec(&node(ModelKind::Codec, "entry_codec"));
            draft.select_key(FieldName::parse("id").assured("valid field"));
        });
        let lookup = render();
        for control in [
            "create-hash-resource",
            "create-hash-version",
            "create-hash-codec",
            "create-hash-key",
            "create-hash-path",
        ] {
            assert!(lookup.contains(control), "{control} must render");
        }
        assert!(lookup.contains("KEY id"));
        assert!(lookup.contains("VERSION LATEST"));
        assert!(lookup.contains("Selected key: id"));
        assert_eq!(open_form_controls(signals, CreateKind::HashMap).len(), 4);

        signals.open(CreateKind::Udf, Some(domain), "global-create-button");
        signals.udf.update(|draft| {
            draft.name = "reflect".to_string();
            let mut argument = UdfArgumentDraft {
                name: "value".to_string(),
                ..UdfArgumentDraft::default()
            };
            argument.ty.scalar = Some(ParseAsType::I64);
            draft.arguments.push(argument);
            draft.returns.scalar = Some(ParseAsType::I64);
            draft.code = "fn reflect(value: I64Column) -> I64Column { value }".to_string();
        });
        let udf = render();
        for control in [
            "create-udf-language",
            "create-udf-add-argument",
            "create-udf-argument-name",
            "create-udf-argument-type",
            "create-udf-return-type",
            "create-udf-volatile",
            "create-udf-code",
        ] {
            assert!(udf.contains(control), "{control} must render");
        }
        assert!(udf.contains("ARGS (value I64)"));
        assert!(udf.contains("RETURNS I64"));
        assert!(udf.contains("fn reflect(value: I64Column)"));
        assert!(open_form_controls(signals, CreateKind::Udf).is_empty());

        signals.udf.update(|draft| {
            draft.arguments[0].ty.layers.push(CollectionLayer::Vector);
            draft.returns.layers.push(CollectionLayer::Array {
                length: "4".to_string(),
            });
        });
        let collections = render();
        assert!(collections.contains("Vector"));
        assert!(collections.contains("Fixed array"));
        assert!(collections.contains("create-udf-array-length"));
        assert!(collections.contains("RETURNS ARRAY&lt;I64, 4&gt;"));
    });
}

#[test]
fn hash_map_choices_bind_the_key_to_the_selected_codec_and_domain() {
    Owner::new().with(|| {
        let scope = DomainName::parse("orders").assured("valid domain");
        let elsewhere = DomainName::parse("elsewhere").assured("valid domain");
        let resource =
            ChoiceValue::Resource(ResourceName::parse("bundle").assured("valid resource"));
        let version = ChoiceValue::ResourceVersion(RequestedResourceVersion::Latest);
        let codec = ChoiceValue::Model(node(ModelKind::Codec, "entry_codec"));
        let key = ChoiceValue::Field(FieldName::parse("id").assured("valid field"));
        let signals = CreateSignals::new();
        for control in open_form_controls(signals, CreateKind::HashMap) {
            assert_eq!(control.form(), CreateKind::HashMap);
        }
        assert_eq!(CreateKind::HashMap.wire_format(), None);
        assert_eq!(CreateKind::Udf.wire_format(), None);

        signals.open(
            CreateKind::HashMap,
            Some(scope.clone()),
            "global-create-button",
        );
        assert_eq!(
            signals.choice_query(ChoiceControl::HashVersion).err(),
            Some("Select a resource to list its completed versions")
        );
        assert_eq!(
            signals.choice_query(ChoiceControl::HashKey).err(),
            Some("Select a codec to list the fields of its output schema")
        );
        select_choice(signals, ChoiceControl::HashResource, resource.clone());
        select_choice(signals, ChoiceControl::HashVersion, version.clone());
        select_choice(signals, ChoiceControl::HashCodec, codec.clone());
        select_choice(signals, ChoiceControl::HashKey, key.clone());
        for (control, value) in [
            (ChoiceControl::HashResource, &resource),
            (ChoiceControl::HashVersion, &version),
            (ChoiceControl::HashCodec, &codec),
            (ChoiceControl::HashKey, &key),
        ] {
            assert!(selected_choice(signals, control, value));
        }
        let versions = signals
            .choice_query(ChoiceControl::HashVersion)
            .assured("version choices depend on the selected resource");
        assert_eq!(versions.target, ChoiceTarget::CompletedResourceVersion);
        assert_eq!(versions.dependencies[1].value, resource);
        let codecs = signals
            .choice_query(ChoiceControl::HashCodec)
            .assured("codec choices are scoped to the domain");
        assert_eq!(codecs.target, ChoiceTarget::Codec);
        assert_eq!(
            codecs.dependencies[0].value,
            ChoiceValue::Domain(scope.clone())
        );
        let keys = signals
            .choice_query(ChoiceControl::HashKey)
            .assured("key choices depend on the selected codec");
        assert_eq!(keys.target, ChoiceTarget::CodecField);
        assert_eq!(keys.dependencies[0].value, ChoiceValue::Domain(scope));
        assert_eq!(keys.dependencies[1].value, codec);

        signals.change_scope(Some(elsewhere));
        for (control, value) in [
            (ChoiceControl::HashResource, &resource),
            (ChoiceControl::HashVersion, &version),
            (ChoiceControl::HashCodec, &keys.dependencies[1].value),
            (ChoiceControl::HashKey, &key),
        ] {
            assert!(!selected_choice(signals, control, value));
        }
    });
}

#[test]
fn ingestor_form_renders_every_source_and_keeps_its_typed_references_in_scope() {
    use nervix_models::IngestSourceKind;

    super::super::initialize_test_executor();
    Owner::new().with(|| {
        let scope = DomainName::parse("orders").assured("valid domain");
        let signals = CreateSignals::new();
        signals.open(
            CreateKind::Ingestor,
            Some(scope.clone()),
            "global-create-button",
        );
        let dialog = CreateDialog(
            CreateDialogProps::builder()
                .signals(signals)
                .active_domain(RwSignal::new(Some(scope.clone())))
                .connection_state(RwSignal::new(super::super::ConsoleConnectionState::Waiting))
                .session_generation(RwSignal::new(1))
                .request_tx(RwSignal::new(None))
                .submit(|_, _, _| {})
                .build(),
        );
        any_spawner::Executor::poll_local();
        let html = dialog.to_html();
        for kind in IngestSourceKind::ALL {
            assert!(html.contains(&format!("data-source=\"{}\"", kind.form_key())));
        }
        for control in [
            "create-ingestor-codec",
            "create-ingestor-routes",
            "create-ingestor-branching",
            "create-ingestor-relay",
            "create-ingestor-flush",
            "create-ingestor-message-error",
            "create-ingestor-general-error",
        ] {
            assert!(html.contains(control), "{control} must render");
        }
        signals
            .ingestor
            .update(|draft| draft.source.choose_kind(IngestSourceKind::Endpoint));
        let source = ChoiceValue::Model(node(ModelKind::Endpoint, "incoming"));
        select_choice(signals, ChoiceControl::IngestSourceRef, source.clone());
        assert!(selected_choice(
            signals,
            ChoiceControl::IngestSourceRef,
            &source
        ));
        assert_eq!(
            signals
                .choice_query(ChoiceControl::IngestSourceRef)
                .assured("endpoint lookup has a captured domain")
                .target,
            ChoiceTarget::IngestEndpointSource,
        );
        signals.ingestor.update(|draft| {
            draft.routes[0].branch.choose_branched();
            draft.routes[0]
                .branch
                .select_branch(&node(ModelKind::Branch, "by_tenant"));
        });
        assert_eq!(
            signals
                .choice_query(ChoiceControl::IngestRouteRelay)
                .assured("named branch limits the output relay")
                .target,
            ChoiceTarget::IngestBranchedRelay,
        );
        assert_eq!(
            signals
                .choice_query(ChoiceControl::IngestErrorRelay)
                .assured("ingestor errors remain unbranched")
                .target,
            ChoiceTarget::IngestUnbranchedRelay,
        );
        signals.change_scope(Some(
            DomainName::parse("new_orders").assured("valid domain"),
        ));
        assert!(!selected_choice(
            signals,
            ChoiceControl::IngestSourceRef,
            &source
        ));
    });
}

#[test]
fn ingestor_choices_follow_the_selected_source_codec_branch_and_relays() {
    use nervix_models::IngestSourceKind;

    Owner::new().with(|| {
        let domain = DomainName::parse("orders").assured("valid domain");
        let signals = CreateSignals::new();
        signals.open(CreateKind::Ingestor, Some(domain.clone()), "global-create-button");
        assert_eq!(
            signals.choice_query(ChoiceControl::IngestSourceRef).err(),
            Some("Choose a source type before its client or endpoint")
        );
        for kind in IngestSourceKind::ALL {
            signals.ingestor.update(|draft| draft.source.choose_kind(kind));
            let query = signals
                .choice_query(ChoiceControl::IngestSourceRef)
                .assured("every selected source asks a typed question");
            assert_eq!(query.target, ChoiceTarget::for_ingest_source(kind));
            assert_eq!(query.dependencies[0].value, ChoiceValue::Domain(domain.clone()));
            let reference_kind = if kind == IngestSourceKind::Endpoint {
                ModelKind::Endpoint
            } else {
                ModelKind::Client
            };
            let source = ChoiceValue::Model(node(reference_kind, "source"));
            select_choice(signals, ChoiceControl::IngestSourceRef, source.clone());
            assert!(selected_choice(signals, ChoiceControl::IngestSourceRef, &source));
        }

        let codec = ChoiceValue::Model(node(ModelKind::Codec, "decode"));
        select_choice(signals, ChoiceControl::IngestCodec, codec.clone());
        assert!(selected_choice(signals, ChoiceControl::IngestCodec, &codec));
        assert_eq!(
            signals.choice_query(ChoiceControl::IngestCodec).assured("domain captured").target,
            ChoiceTarget::IngestCodec
        );
        signals.ingestor.update(|draft| {
            draft.timestamp = super::ingestor_draft::TimestampDraft::At(None);
            draft.routes[0].inherit.choose("fields");
        });
        for control in [ChoiceControl::IngestTimestampField, ChoiceControl::IngestInputField] {
            let query = signals.choice_query(control).assured("codec selected");
            assert_eq!(query.target, ChoiceTarget::CodecField);
            assert_eq!(query.dependencies[1].value, codec);
        }
        let occurred_at = ChoiceValue::Field(FieldName::parse("occurred_at").assured("valid field"));
        select_choice(signals, ChoiceControl::IngestTimestampField, occurred_at);
        let inherited = ChoiceValue::Field(FieldName::parse("message").assured("valid field"));
        select_choice(signals, ChoiceControl::IngestInputField, inherited);
        assert!(matches!(
            signals.ingestor.get().routes[0].inherit,
            super::ingestor_route_draft::InheritDraft::Fields(ref fields) if fields.len() == 1
        ));

        assert_eq!(
            signals.choice_query(ChoiceControl::IngestRouteRelay).err(),
            Some("Choose the route branch before its relay")
        );
        signals.ingestor.update(|draft| draft.choose_route_branched());
        assert_eq!(
            signals.choice_query(ChoiceControl::IngestRouteRelay).err(),
            Some("Choose the named branch before its relay")
        );
        let branch = ChoiceValue::Model(node(ModelKind::Branch, "by_tenant"));
        select_choice(signals, ChoiceControl::IngestRouteBranch, branch.clone());
        assert!(selected_choice(signals, ChoiceControl::IngestRouteBranch, &branch));
        assert_eq!(
            signals.choice_query(ChoiceControl::IngestBranchField).assured("branch selected").target,
            ChoiceTarget::BranchField
        );
        let route_query = signals.choice_query(ChoiceControl::IngestRouteRelay).assured("branch selected");
        assert_eq!(route_query.target, ChoiceTarget::IngestBranchedRelay);
        assert_eq!(route_query.dependencies[1].value, branch);
        select_choice(
            signals,
            ChoiceControl::IngestBranchField,
            ChoiceValue::Field(FieldName::parse("tenant").assured("valid field")),
        );
        let output = ChoiceValue::Model(node(ModelKind::Relay, "output"));
        select_choice(signals, ChoiceControl::IngestRouteRelay, output.clone());
        assert!(selected_choice(signals, ChoiceControl::IngestRouteRelay, &output));
        assert_eq!(
            signals.choice_query(ChoiceControl::IngestOutputField).assured("output relay selected").dependencies[1].value,
            output
        );
        select_choice(
            signals,
            ChoiceControl::IngestOutputField,
            ChoiceValue::Field(FieldName::parse("message").assured("valid field")),
        );

        signals.ingestor.update(|draft| {
            draft.routes[0].message_error = super::ingestor_route_draft::MessageErrorDraft::SendTo {
                relay: None,
                assignments: Vec::new(),
            };
        });
        assert_eq!(
            signals.choice_query(ChoiceControl::IngestErrorRelay).assured("domain captured").target,
            ChoiceTarget::IngestUnbranchedRelay
        );
        let errors = ChoiceValue::Model(node(ModelKind::Relay, "errors"));
        select_choice(signals, ChoiceControl::IngestErrorRelay, errors.clone());
        assert!(selected_choice(signals, ChoiceControl::IngestErrorRelay, &errors));
        assert_eq!(
            signals.choice_query(ChoiceControl::IngestErrorField).assured("error relay selected").dependencies[1].value,
            errors
        );
        select_choice(
            signals,
            ChoiceControl::IngestErrorField,
            ChoiceValue::Field(FieldName::parse("reason").assured("valid field")),
        );
        assert!(matches!(
            signals.ingestor.get().routes[0].message_error,
            super::ingestor_route_draft::MessageErrorDraft::SendTo { ref assignments, .. } if assignments.len() == 1
        ));
        assert!(open_form_controls(signals, CreateKind::Ingestor)
            .contains(&ChoiceControl::IngestErrorField));
    });
}

#[test]
fn ingestor_editor_shows_source_specific_fields_and_route_construction_sections() {
    use nervix_models::IngestSourceKind;

    super::super::initialize_test_executor();
    Owner::new().with(|| {
        let scope = DomainName::parse("orders").assured("valid domain");
        let signals = CreateSignals::new();
        let active_domain = RwSignal::new(Some(scope.clone()));
        let connection_state = RwSignal::new(super::super::ConsoleConnectionState::Waiting);
        let generation = RwSignal::new(1);
        let request_tx = RwSignal::new(None);
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
        signals.open(CreateKind::Ingestor, Some(scope), "global-create-button");
        for (kind, field) in [
            (IngestSourceKind::Http, "create-ingestor-every"),
            (IngestSourceKind::Kafka, "create-ingestor-offset"),
            (IngestSourceKind::Pulsar, "create-ingestor-subscription"),
            (IngestSourceKind::Mqtt, "create-ingestor-mqtt-session"),
            (IngestSourceKind::Nats, "create-ingestor-queue-group"),
            (IngestSourceKind::RabbitMq, "create-ingestor-queue"),
            (IngestSourceKind::RedisPubSub, "create-ingestor-channel"),
            (IngestSourceKind::Prometheus, "create-ingestor-query"),
            (IngestSourceKind::ZeroMq, "create-ingestor-quiesce"),
            (IngestSourceKind::Sqs, "create-ingestor-queue"),
            (IngestSourceKind::Endpoint, "create-ingestor-timestamp"),
            (IngestSourceKind::Websockets, "create-ingestor-quiesce"),
            (IngestSourceKind::Syslog, "create-ingestor-quiesce"),
        ] {
            signals
                .ingestor
                .update(|draft| draft.source.choose_kind(kind));
            assert!(
                render().contains(field),
                "{} must show {field}",
                kind.form_label()
            );
        }
        signals.ingestor.update(|draft| {
            draft.source.choose_kind(IngestSourceKind::Mqtt);
            draft.source.delivery = Some(super::ingestor_source_draft::DeliveryChoice::AckParallel);
            draft.source.quiesce = Some(super::ingestor_source_draft::QuiesceChoice::Buffer);
            let route = draft.active_route_mut().assured("first route exists");
            route.branch.choose_branched();
            route.inherit.choose("fields");
            route.flush = super::ingestor_route_draft::FlushDraft::Each {
                interval: "1s".into(),
                max_batch_size: "1MiB".into(),
            };
            route.message_error = super::ingestor_route_draft::MessageErrorDraft::SendTo {
                relay: None,
                assignments: Vec::new(),
            };
            route
                .invocations
                .push(super::ingestor_route_draft::InvocationDraft {
                    function: "lower".into(),
                    arguments: vec!["message.value".into()],
                });
        });
        let html = render();
        for section in [
            "create-ingestor-ack-max",
            "create-ingestor-overflow",
            "create-ingestor-branch-fields",
            "create-ingestor-input-fields",
            "create-ingestor-flush-interval",
            "create-ingestor-error-relay",
            "create-ingestor-invocation-argument",
        ] {
            assert!(html.contains(section), "{section} must render");
        }
    });
}

#[test]
fn client_vhost_and_endpoint_forms_render_every_transport_and_typed_reference_control() {
    super::super::initialize_test_executor();
    Owner::new().with(|| {
        let scope = DomainName::parse("orders").assured("valid domain");
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
            CreateKind::Client,
            Some(scope.clone()),
            "global-create-button",
        );
        let client = render();
        for transport in ClientTransport::ALL {
            assert!(client.contains(&format!("data-type=\"{}\"", transport.key())));
        }
        signals.client.update(|draft| {
            draft.set_transport(ClientTransport::Redis);
            draft.mount_enabled = true;
        });
        let pooled = render();
        assert!(pooled.contains("create-client-pool-min"));
        assert!(pooled.contains("create-client-pool-max"));
        assert!(pooled.contains("create-client-resource"));
        assert!(pooled.contains("create-client-version"));
        signals.client.update(|draft| {
            draft.config.push(ClientConfigDraft {
                key: "password".to_string(),
                value: "hidden-value".to_string(),
                secret: false,
            });
        });
        let configured = render();
        assert!(configured.contains("create-client-config-entry"));
        assert!(configured.contains("type=\"password\""));
        assert!(configured.contains("create-client-config-secret"));
        signals
            .client
            .update(|draft| draft.set_transport(ClientTransport::Websockets));
        assert!(render().contains("create-client-signaling"));
        assert!(!render().contains("create-client-config-entry"));
        signals.client.update(|draft| {
            draft.select_signaling(&node(ModelKind::SignalingProtocol, "handshake"));
        });
        assert!(render().contains("create-client-signaling-clear"));

        signals.open(
            CreateKind::Vhost,
            Some(scope.clone()),
            "global-create-button",
        );
        assert!(open_form_controls(signals, CreateKind::Vhost).is_empty());
        signals.vhost.update(|draft| draft.tls_enabled = true);
        let vhost = render();
        assert!(vhost.contains("create-vhost-hostname"));
        assert!(vhost.contains("create-vhost-resource"));
        assert!(vhost.contains("create-vhost-version"));

        signals.open(CreateKind::Endpoint, Some(scope), "global-create-button");
        signals
            .endpoint
            .update(|draft| draft.select_type(nervix_models::EndpointType::Websockets));
        let endpoint = render();
        assert!(endpoint.contains("create-endpoint-vhost"));
        assert!(endpoint.contains("create-endpoint-signaling"));
        signals.endpoint.update(|draft| {
            draft.select_signaling(&node(ModelKind::SignalingProtocol, "handshake"));
        });
        assert!(render().contains("create-endpoint-signaling-clear"));
    });
}

#[test]
fn client_vhost_and_endpoint_submissions_use_current_models_and_mask_secrets() {
    Owner::new().with(|| {
        let scope = DomainName::parse("orders").assured("valid domain");
        let signals = CreateSignals::new();

        signals.open(
            CreateKind::Client,
            Some(scope.clone()),
            "global-create-button",
        );
        signals.client.update(|draft| {
            draft.name = "external_http".to_string();
            draft.set_transport(ClientTransport::Http);
            draft.config.push(ClientConfigDraft {
                key: "password".to_string(),
                value: "private-value".to_string(),
                secret: false,
            });
        });
        let client = signals.submission().assured("client draft is complete");
        assert!(!client.presentation.contains("private-value"));
        let CreateDispatch::Command(command) = client.dispatch else {
            panic!("client creation must use the command dispatcher");
        };
        assert!(command.query.contains("private-value"));
        assert_eq!(command.domain, Some(scope.clone()));

        signals.open(
            CreateKind::Vhost,
            Some(scope.clone()),
            "global-create-button",
        );
        signals.vhost.update(|draft| {
            draft.name = "external_edge".to_string();
            draft.hostnames = vec!["api.example.com".to_string()];
        });
        let vhost = signals.submission().assured("VHOST draft is complete");
        assert!(
            vhost
                .presentation
                .contains("CREATE VHOST external_edge api.example.com")
        );

        signals.open(CreateKind::Endpoint, Some(scope), "global-create-button");
        signals.endpoint.update(|draft| {
            draft.name = "external_path".to_string();
            draft.select_vhost(&node(ModelKind::Vhost, "external_edge"));
            draft.path = "/external".to_string();
            draft.select_type(nervix_models::EndpointType::Http);
        });
        let endpoint = signals.submission().assured("endpoint draft is complete");
        assert!(
            endpoint.presentation.contains(
                "CREATE ENDPOINT external_path ON external_edge PATH '/external' TYPE HTTP"
            )
        );
    });
}

#[test]
fn client_vhost_and_endpoint_choices_keep_their_typed_dependencies() {
    Owner::new().with(|| {
        let scope = DomainName::parse("orders").assured("valid domain");
        let elsewhere = DomainName::parse("elsewhere").assured("valid domain");
        let resource =
            ChoiceValue::Resource(ResourceName::parse("bundle").assured("valid resource"));
        let version = ChoiceValue::ResourceVersion(RequestedResourceVersion::Number(2));
        let protocol = ChoiceValue::Model(node(ModelKind::SignalingProtocol, "handshake"));
        let vhost = ChoiceValue::Model(node(ModelKind::Vhost, "edge"));
        let signals = CreateSignals::new();
        for (control, kind) in [
            (ChoiceControl::ClientResource, CreateKind::Client),
            (ChoiceControl::ClientVersion, CreateKind::Client),
            (ChoiceControl::ClientSignaling, CreateKind::Client),
            (ChoiceControl::VhostResource, CreateKind::Vhost),
            (ChoiceControl::VhostVersion, CreateKind::Vhost),
            (ChoiceControl::EndpointVhost, CreateKind::Endpoint),
            (ChoiceControl::EndpointSignaling, CreateKind::Endpoint),
        ] {
            assert_eq!(control.form(), kind);
        }
        assert_eq!(CreateKind::Endpoint.wire_format(), None);

        signals.open(
            CreateKind::Client,
            Some(scope.clone()),
            "global-create-button",
        );
        signals.client.update(|draft| {
            draft.set_transport(ClientTransport::Websockets);
            draft.mount_enabled = true;
        });
        assert_eq!(
            signals.choice_query(ChoiceControl::ClientVersion).err(),
            Some("Select a resource to list its completed versions")
        );
        select_choice(signals, ChoiceControl::ClientResource, resource.clone());
        select_choice(signals, ChoiceControl::ClientVersion, version.clone());
        select_choice(signals, ChoiceControl::ClientSignaling, protocol.clone());
        for (control, value) in [
            (ChoiceControl::ClientResource, &resource),
            (ChoiceControl::ClientVersion, &version),
            (ChoiceControl::ClientSignaling, &protocol),
        ] {
            assert!(selected_choice(signals, control, value));
        }
        assert_eq!(
            signals
                .choice_query(ChoiceControl::ClientSignaling)
                .assured("signaling choices are scoped")
                .target,
            ChoiceTarget::SignalingProtocol
        );
        assert_eq!(
            signals
                .choice_query(ChoiceControl::ClientVersion)
                .assured("selected resource is a typed dependency")
                .dependencies[1]
                .value,
            resource
        );
        assert_eq!(
            open_form_controls(signals, CreateKind::Client),
            [
                ChoiceControl::ClientResource,
                ChoiceControl::ClientVersion,
                ChoiceControl::ClientSignaling,
            ]
        );
        signals.change_scope(Some(elsewhere.clone()));
        assert!(!selected_choice(
            signals,
            ChoiceControl::ClientResource,
            &resource
        ));
        assert!(!selected_choice(
            signals,
            ChoiceControl::ClientVersion,
            &version
        ));
        assert!(!selected_choice(
            signals,
            ChoiceControl::ClientSignaling,
            &protocol
        ));

        signals.open(
            CreateKind::Vhost,
            Some(scope.clone()),
            "global-create-button",
        );
        signals.vhost.update(|draft| draft.tls_enabled = true);
        select_choice(signals, ChoiceControl::VhostResource, resource.clone());
        select_choice(signals, ChoiceControl::VhostVersion, version.clone());
        assert!(selected_choice(
            signals,
            ChoiceControl::VhostResource,
            &resource
        ));
        assert!(selected_choice(
            signals,
            ChoiceControl::VhostVersion,
            &version
        ));
        assert_eq!(
            signals
                .choice_query(ChoiceControl::VhostVersion)
                .assured("TLS resource is a typed dependency")
                .dependencies[1]
                .value,
            resource
        );
        assert_eq!(
            open_form_controls(signals, CreateKind::Vhost),
            [ChoiceControl::VhostResource, ChoiceControl::VhostVersion]
        );
        signals.change_scope(Some(elsewhere.clone()));
        assert!(!selected_choice(
            signals,
            ChoiceControl::VhostResource,
            &resource
        ));
        assert!(!selected_choice(
            signals,
            ChoiceControl::VhostVersion,
            &version
        ));

        signals.open(
            CreateKind::Endpoint,
            Some(scope.clone()),
            "global-create-button",
        );
        signals.endpoint.update(|draft| {
            draft.select_type(nervix_models::EndpointType::Websockets);
        });
        select_choice(signals, ChoiceControl::EndpointVhost, vhost.clone());
        select_choice(signals, ChoiceControl::EndpointSignaling, protocol.clone());
        assert!(selected_choice(
            signals,
            ChoiceControl::EndpointVhost,
            &vhost
        ));
        assert!(selected_choice(
            signals,
            ChoiceControl::EndpointSignaling,
            &protocol
        ));
        assert_eq!(
            signals
                .choice_query(ChoiceControl::EndpointVhost)
                .assured("VHOST choices are scoped")
                .target,
            ChoiceTarget::Vhost
        );
        assert_eq!(
            signals
                .choice_query(ChoiceControl::EndpointSignaling)
                .assured("signaling choices are scoped")
                .target,
            ChoiceTarget::SignalingProtocol
        );
        assert_eq!(
            open_form_controls(signals, CreateKind::Endpoint),
            [
                ChoiceControl::EndpointVhost,
                ChoiceControl::EndpointSignaling
            ]
        );
        signals.change_scope(Some(elsewhere));
        assert!(!selected_choice(
            signals,
            ChoiceControl::EndpointVhost,
            &vhost
        ));
        assert!(!selected_choice(
            signals,
            ChoiceControl::EndpointSignaling,
            &protocol
        ));
    });
}

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
