//! The domain surfaces installed before node-local execution starts.
//!
//! Layer: decisions.
//! - **Owns.** Resolving schemas, wire formats, relay branch retention, VHOSTs, endpoints and
//!   signaling references into one typed domain activation decision.
//! - **Depends on.** Validated schedule Models, names and the pure schema compiler.
//! - **Must not know.** Runtime tasks, listeners, resource stores or cluster placement.

use std::{collections::BTreeMap, num::NonZeroUsize, time::Duration};

use arch_into::ArchInto as _;
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_models::{
    CodecEncodingRule, CodecJaqFormat, CodecJaqTransformations, CodecName, CodecProtobufConfig,
    CreateAvroWireSchema, CreateCborWireSchema, CreateJsonWireSchema, DomainName, EndpointName,
    EndpointType, Model, RelayName, ResolvedBranching, ResolvedCodecWireFormat, ResourceId,
    ScheduledNodes, SchemaName, SignalingProtocolName, SignalingProtocolOnConnect,
    SignalingWireFormat, VhostName, WireSchemaLookup, WireSchemaName, parse_duration_text,
};
use thiserror::Error;
use triomphe::Arc;

use crate::runtime_schema::{CompiledSchema, compile_schema};

#[derive(Debug, Error)]
pub(crate) enum DomainActivationPlanError {
    #[error("relay '{relay}' references missing schema '{schema}'")]
    MissingRelaySchema {
        relay: RelayName,
        schema: SchemaName,
    },
    #[error("relay '{relay}' has no resolved branch declaration")]
    MissingRelayBranching { relay: RelayName },
    #[error("relay '{relay}' references missing branch '{branch}'")]
    MissingRelayBranch {
        relay: RelayName,
        branch: nervix_models::BranchName,
    },
    #[error("relay '{relay}' has invalid TTL on branch '{branch}'")]
    InvalidRelayBranchTtl {
        relay: RelayName,
        branch: nervix_models::BranchName,
    },
    #[error("codec '{codec}' references missing schema '{schema}'")]
    MissingCodecSchema {
        codec: CodecName,
        schema: SchemaName,
    },
    #[error("codec '{codec}' references missing wire schema '{wire_schema}'")]
    MissingCodecWireSchema {
        codec: CodecName,
        wire_schema: WireSchemaName,
    },
    #[error("endpoint '{endpoint}' references missing VHOST '{vhost}'")]
    MissingEndpointVhost {
        endpoint: EndpointName,
        vhost: VhostName,
    },
    #[error("endpoint '{endpoint}' references missing signaling protocol '{protocol}'")]
    MissingEndpointSignalingProtocol {
        endpoint: EndpointName,
        protocol: SignalingProtocolName,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PlannedRelayRetention {
    pub(crate) branch_ttl: Option<Duration>,
    pub(crate) branch_capacity: Option<NonZeroUsize>,
}

#[derive(Debug, Clone)]
pub(crate) struct PlannedRelay {
    pub(crate) name: RelayName,
    pub(crate) schema: Arc<CompiledSchema>,
    pub(crate) capacity: NonZeroUsize,
    pub(crate) branching: ResolvedBranching,
    pub(crate) retention: PlannedRelayRetention,
    pub(crate) materialized: bool,
}

#[derive(Debug, Clone)]
pub(crate) enum PlannedCodecWireFormat {
    Json(CreateJsonWireSchema),
    Cbor(CreateCborWireSchema),
    Avro(CreateAvroWireSchema),
    Syslog,
    JaqNative {
        format: CodecJaqFormat,
        transformations: CodecJaqTransformations,
    },
    Protobuf(CodecProtobufConfig),
}

impl PlannedCodecWireFormat {
    pub(crate) fn resolved(&self) -> ResolvedCodecWireFormat<'_> {
        match self {
            Self::Json(schema) => ResolvedCodecWireFormat::Json(schema),
            Self::Cbor(schema) => ResolvedCodecWireFormat::Cbor(schema),
            Self::Avro(schema) => ResolvedCodecWireFormat::Avro(schema),
            Self::Syslog => ResolvedCodecWireFormat::Syslog,
            Self::JaqNative {
                format,
                transformations,
            } => ResolvedCodecWireFormat::JaqNative {
                format: *format,
                transformations,
            },
            Self::Protobuf(config) => ResolvedCodecWireFormat::Protobuf(config),
        }
    }
}

impl From<ResolvedCodecWireFormat<'_>> for PlannedCodecWireFormat {
    fn from(format: ResolvedCodecWireFormat<'_>) -> Self {
        match format {
            ResolvedCodecWireFormat::Json(schema) => Self::Json(schema.clone()),
            ResolvedCodecWireFormat::Cbor(schema) => Self::Cbor(schema.clone()),
            ResolvedCodecWireFormat::Avro(schema) => Self::Avro(schema.clone()),
            ResolvedCodecWireFormat::Syslog => Self::Syslog,
            ResolvedCodecWireFormat::JaqNative {
                format,
                transformations,
            } => Self::JaqNative {
                format,
                transformations: transformations.clone(),
            },
            ResolvedCodecWireFormat::Protobuf(config) => Self::Protobuf(config.clone()),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PlannedCodec {
    pub(crate) name: CodecName,
    pub(crate) schema: Arc<CompiledSchema>,
    pub(crate) wire_format: PlannedCodecWireFormat,
    pub(crate) encoding_rules: Vec<CodecEncodingRule>,
}

#[derive(Debug, Clone)]
pub(crate) struct PlannedSignalingProtocol {
    pub(crate) name: SignalingProtocolName,
    pub(crate) format: SignalingWireFormat,
    pub(crate) on_connect: SignalingProtocolOnConnect,
}

#[derive(Debug, Clone)]
pub(crate) struct PlannedVhost {
    pub(crate) name: VhostName,
    pub(crate) hostnames: Vec<String>,
    pub(crate) tls: Option<ResourceId>,
}

#[derive(Debug, Clone)]
pub(crate) struct PlannedEndpoint {
    pub(crate) name: EndpointName,
    pub(crate) path: String,
    pub(crate) hostnames: Vec<String>,
    pub(crate) endpoint_type: EndpointType,
    pub(crate) signaling_protocol: Option<SignalingProtocolName>,
}

/// One pure decision for all surfaces a domain installs on a live node. The same plan is valid
/// for running and passive execution; the caller decides whether an endpoint admits traffic.
#[derive(Debug, Default)]
pub(crate) struct DomainActivationPlan {
    pub(crate) schemas: BTreeMap<SchemaName, Arc<CompiledSchema>>,
    pub(crate) codecs: BTreeMap<CodecName, PlannedCodec>,
    pub(crate) relays: BTreeMap<RelayName, PlannedRelay>,
    pub(crate) vhosts: BTreeMap<VhostName, PlannedVhost>,
    pub(crate) endpoints: BTreeMap<EndpointName, PlannedEndpoint>,
    pub(crate) signaling_protocols: BTreeMap<SignalingProtocolName, PlannedSignalingProtocol>,
}

impl DomainActivationPlan {
    pub(crate) fn from_scheduled_nodes(
        domain: &DomainName,
        nodes: &ScheduledNodes,
    ) -> error_stack::Result<Self, DomainActivationPlanError> {
        let mut plan = Self::default();
        let mut wire_schemas = WireSchemas::default();
        let mut branches = BTreeMap::new();

        for node in nodes.values() {
            match node.config.as_ref() {
                Model::Schema(schema) => {
                    plan.schemas
                        .insert(schema.name.clone(), Arc::new(compile_schema(schema)));
                }
                Model::WireJsonSchema(schema) => wire_schemas.insert_json(schema.clone()),
                Model::WireCborSchema(schema) => wire_schemas.insert_cbor(schema.clone()),
                Model::WireAvroSchema(schema) => wire_schemas.insert_avro(schema.clone()),
                Model::Branch(branch) => {
                    branches.insert(branch.name.clone(), branch);
                }
                Model::Vhost(vhost) => {
                    let tls = vhost.tls.as_ref().map(|tls| {
                        ResourceId::new(domain.clone(), tls.resource.clone(), tls.version)
                    });
                    plan.vhosts.insert(
                        vhost.name.clone(),
                        PlannedVhost {
                            name: vhost.name.clone(),
                            hostnames: vhost.hostnames.clone(),
                            tls,
                        },
                    );
                }
                Model::SignalingProtocol(protocol) => {
                    plan.signaling_protocols.insert(
                        protocol.name.clone(),
                        PlannedSignalingProtocol {
                            name: protocol.name.clone(),
                            format: protocol.format.clone(),
                            on_connect: protocol.on_connect.clone(),
                        },
                    );
                }
                _ => {}
            }
        }

        for node in nodes.values() {
            match node.config.as_ref() {
                Model::Relay(relay) => {
                    let schema = plan.schemas.get(&relay.schema).cloned().ok_or_else(|| {
                        Report::new(DomainActivationPlanError::MissingRelaySchema {
                            relay: relay.name.clone(),
                            schema: relay.schema.clone(),
                        })
                    })?;
                    let branching = node.resolved_branching.clone().ok_or_else(|| {
                        Report::new(DomainActivationPlanError::MissingRelayBranching {
                            relay: relay.name.clone(),
                        })
                    })?;
                    let retention = match branching.branch() {
                        None => PlannedRelayRetention::default(),
                        Some(branch_name) => {
                            let branch = branches.get(branch_name).ok_or_else(|| {
                                Report::new(DomainActivationPlanError::MissingRelayBranch {
                                    relay: relay.name.clone(),
                                    branch: branch_name.clone(),
                                })
                            })?;
                            let branch_ttl =
                                parse_duration_text(&branch.ttl).change_context_lazy(|| {
                                    DomainActivationPlanError::InvalidRelayBranchTtl {
                                        relay: relay.name.clone(),
                                        branch: branch_name.clone(),
                                    }
                                })?;
                            let branch_capacity = branch.eviction.as_ref().map(|eviction| {
                                NonZeroUsize::new(eviction.max_instances().get().arch_into())
                                    .assured(
                                        "non-zero configured branch counts fit the supported \
                                         pointer width",
                                    )
                            });
                            PlannedRelayRetention {
                                branch_ttl: Some(branch_ttl),
                                branch_capacity,
                            }
                        }
                    };
                    plan.relays.insert(
                        relay.name.clone(),
                        PlannedRelay {
                            name: relay.name.clone(),
                            schema,
                            capacity: relay.buffer,
                            branching,
                            retention,
                            materialized: relay.materialized_state.is_some(),
                        },
                    );
                }
                Model::Codec(codec) => {
                    let schema = plan.schemas.get(&codec.schema).cloned().ok_or_else(|| {
                        Report::new(DomainActivationPlanError::MissingCodecSchema {
                            codec: codec.name.clone(),
                            schema: codec.schema.clone(),
                        })
                    })?;
                    let wire_format = codec.wire_format.resolve(&wire_schemas).map_err(|name| {
                        Report::new(DomainActivationPlanError::MissingCodecWireSchema {
                            codec: codec.name.clone(),
                            wire_schema: name,
                        })
                    })?;
                    plan.codecs.insert(
                        codec.name.clone(),
                        PlannedCodec {
                            name: codec.name.clone(),
                            schema,
                            wire_format: wire_format.into(),
                            encoding_rules: codec.encoding_rules.clone(),
                        },
                    );
                }
                Model::Endpoint(endpoint) => {
                    let vhost = plan.vhosts.get(&endpoint.on_vhost).ok_or_else(|| {
                        Report::new(DomainActivationPlanError::MissingEndpointVhost {
                            endpoint: endpoint.name.clone(),
                            vhost: endpoint.on_vhost.clone(),
                        })
                    })?;
                    if let Some(protocol) = endpoint.signaling_protocol.as_ref()
                        && !plan.signaling_protocols.contains_key(protocol)
                    {
                        return Err(Report::new(
                            DomainActivationPlanError::MissingEndpointSignalingProtocol {
                                endpoint: endpoint.name.clone(),
                                protocol: protocol.clone(),
                            },
                        ));
                    }
                    plan.endpoints.insert(
                        endpoint.name.clone(),
                        PlannedEndpoint {
                            name: endpoint.name.clone(),
                            path: endpoint.path.clone(),
                            hostnames: vhost
                                .hostnames
                                .iter()
                                .map(|host| host.to_ascii_lowercase())
                                .collect(),
                            endpoint_type: endpoint.endpoint_type,
                            signaling_protocol: endpoint.signaling_protocol.clone(),
                        },
                    );
                }
                _ => {}
            }
        }
        Ok(plan)
    }
}

#[derive(Default)]
struct WireSchemas {
    json: BTreeMap<WireSchemaName, CreateJsonWireSchema>,
    cbor: BTreeMap<WireSchemaName, CreateCborWireSchema>,
    avro: BTreeMap<WireSchemaName, CreateAvroWireSchema>,
}

impl WireSchemas {
    fn insert_json(&mut self, schema: CreateJsonWireSchema) {
        self.json.insert(schema.name.clone(), schema);
    }

    fn insert_cbor(&mut self, schema: CreateCborWireSchema) {
        self.cbor.insert(schema.name.clone(), schema);
    }

    fn insert_avro(&mut self, schema: CreateAvroWireSchema) {
        self.avro.insert(schema.name.clone(), schema);
    }
}

impl WireSchemaLookup for WireSchemas {
    type Error = WireSchemaName;

    fn json_wire_schema(
        &self,
        name: &WireSchemaName,
    ) -> Result<&CreateJsonWireSchema, Self::Error> {
        self.json.get(name).ok_or_else(|| name.clone())
    }

    fn cbor_wire_schema(
        &self,
        name: &WireSchemaName,
    ) -> Result<&CreateCborWireSchema, Self::Error> {
        self.cbor.get(name).ok_or_else(|| name.clone())
    }

    fn avro_wire_schema(
        &self,
        name: &WireSchemaName,
    ) -> Result<&CreateAvroWireSchema, Self::Error> {
        self.avro.get(name).ok_or_else(|| name.clone())
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        AvroType, BranchEviction, BranchName, CodecJaqFormat, CodecJaqTransformations,
        CodecProtobufConfig, CodecWireFormat, CreateBranch, CreateCodec, CreateEndpoint,
        CreateVhost, CreateWireSchema, FieldName, JsonType, Model, ModelKind, ModelName, NodeRef,
        RelayBranching, ResolvedBranching, ScheduledNode, ScheduledNodes, SchemaFingerprint,
        SignalingWireFormat, VhostTlsResource, WireSchemaField,
    };
    use nonzero_ext::nonzero;

    use super::*;
    use crate::registry::test_fixtures::{
        TOO_LONG_DURATION_TEXT, named, relay, schema, signaling_protocol, wire_schema,
    };

    fn nodes(models: Vec<Model>) -> ScheduledNodes {
        models
            .into_iter()
            .map(|model| {
                let branching = if let Model::Relay(_) = &model {
                    Some(ResolvedBranching::unbranched())
                } else {
                    None
                };
                let node = ScheduledNode::new(model, SchemaFingerprint::from_digest([1; 32]))
                    .with_resolved_branching(branching);
                (node.identity(), node)
            })
            .collect()
    }

    fn codec(name: &str, wire_format: CodecWireFormat) -> Model {
        Model::Codec(CreateCodec {
            name: named(name),
            wire_format,
            schema: named("payload"),
            encoding_rules: Vec::new(),
        })
    }

    #[test]
    fn plans_all_codec_formats_and_resolves_relay_and_endpoint_contracts() {
        let domain = named("orders");
        let branch_name: BranchName = named("by_tenant");
        let branch_schema = match schema("branch_schema") {
            Model::Schema(schema) => schema,
            _ => unreachable!("the schema fixture constructs a schema"),
        };
        let mut branched = match relay("branched", "payload") {
            Model::Relay(relay) => relay,
            _ => unreachable!("the relay fixture constructs a relay"),
        };
        branched.buffer = nonzero!(17usize);
        branched.branching = RelayBranching::branched_by(branch_name.clone());
        branched.materialized_state = Some(nervix_models::MaterializedRelayState::LastByTimestamp);
        let mut scheduled = nodes(vec![
            schema("payload"),
            Model::Schema(branch_schema.clone()),
            Model::Branch(CreateBranch {
                name: branch_name.clone(),
                schema: branch_schema.name.clone(),
                ttl: "7m".to_string(),
                eviction: Some(BranchEviction::Lru {
                    max_instances: nonzero!(19u64),
                }),
            }),
            relay("plain", "payload"),
            Model::Relay(branched),
            wire_schema("json_wire"),
            Model::WireCborSchema(CreateWireSchema {
                name: named("cbor_wire"),
                strictness: Default::default(),
                fields: vec![WireSchemaField {
                    name: named::<FieldName>("value"),
                    ty: JsonType::String,
                    optional: false,
                }],
            }),
            Model::WireAvroSchema(CreateWireSchema {
                name: named("avro_wire"),
                strictness: Default::default(),
                fields: vec![WireSchemaField {
                    name: named::<FieldName>("value"),
                    ty: AvroType::String,
                    optional: false,
                }],
            }),
            codec(
                "json",
                CodecWireFormat::Json {
                    wire_schema: named("json_wire"),
                },
            ),
            codec(
                "cbor",
                CodecWireFormat::Cbor {
                    wire_schema: named("cbor_wire"),
                },
            ),
            codec(
                "avro",
                CodecWireFormat::Avro {
                    wire_schema: named("avro_wire"),
                },
            ),
            codec("syslog", CodecWireFormat::Syslog),
            codec(
                "jaq",
                CodecWireFormat::JaqNative {
                    format: CodecJaqFormat::Json,
                    transformations: CodecJaqTransformations {
                        on_ingestion: Some(".".to_string()),
                        ..Default::default()
                    },
                },
            ),
            codec(
                "protobuf",
                CodecWireFormat::Protobuf(CodecProtobufConfig {
                    resource: named("proto_bundle"),
                    resource_version: 3,
                    config: Vec::new(),
                    message: "example.Payload".to_string(),
                    batch_message: None,
                    transformations: CodecJaqTransformations {
                        on_ingestion: Some(".".to_string()),
                        ..Default::default()
                    },
                }),
            ),
            Model::Vhost(CreateVhost {
                name: named("edge"),
                hostnames: vec!["API.EXAMPLE.COM".to_string()],
                tls: Some(VhostTlsResource {
                    resource: named("tls_bundle"),
                    version: 7,
                }),
            }),
            Model::SignalingProtocol(signaling_protocol(
                SignalingWireFormat::Json,
                &["{id: 1}"],
                &[".id == 1"],
                &[],
            )),
            Model::Endpoint(CreateEndpoint {
                name: named("receive"),
                on_vhost: named("edge"),
                path: "/ingest".to_string(),
                endpoint_type: EndpointType::Websockets,
                signaling_protocol: Some(named("handshake")),
            }),
        ]);
        let branched_node = scheduled
            .get_mut(&NodeRef::new(
                ModelKind::Relay,
                named::<ModelName>("branched"),
            ))
            .expect("the fixture has a branched relay");
        branched_node.resolved_branching =
            Some(ResolvedBranching::branched(branch_name, branch_schema));

        let plan = DomainActivationPlan::from_scheduled_nodes(&domain, &scheduled)
            .expect("all domain references resolve");
        assert_eq!(plan.codecs.len(), 6);
        assert!(matches!(
            plan.codecs["json"].wire_format,
            PlannedCodecWireFormat::Json(_)
        ));
        assert!(matches!(
            plan.codecs["cbor"].wire_format,
            PlannedCodecWireFormat::Cbor(_)
        ));
        assert!(matches!(
            plan.codecs["avro"].wire_format,
            PlannedCodecWireFormat::Avro(_)
        ));
        assert!(matches!(
            plan.codecs["protobuf"].wire_format,
            PlannedCodecWireFormat::Protobuf(_)
        ));
        assert!(matches!(
            plan.codecs["syslog"].wire_format,
            PlannedCodecWireFormat::Syslog
        ));
        assert!(matches!(
            plan.codecs["jaq"].wire_format,
            PlannedCodecWireFormat::JaqNative { .. }
        ));
        assert_eq!(
            plan.relays["plain"].retention,
            PlannedRelayRetention::default()
        );
        let branched = &plan.relays["branched"];
        assert_eq!(branched.capacity, nonzero!(17usize));
        assert_eq!(
            branched.retention.branch_ttl,
            Some(Duration::from_secs(420))
        );
        assert_eq!(branched.retention.branch_capacity, Some(nonzero!(19usize)));
        assert!(branched.materialized);
        assert_eq!(
            plan.endpoints["receive"].hostnames,
            vec!["api.example.com".to_string()]
        );
        assert_eq!(
            plan.endpoints["receive"].signaling_protocol,
            Some(named("handshake"))
        );
        assert_eq!(
            plan.vhosts["edge"].tls,
            Some(ResourceId::new(domain, named("tls_bundle"), 7))
        );
    }

    #[test]
    fn missing_references_fail_at_the_plan_boundary() {
        let domain = named("orders");
        let relay_nodes = nodes(vec![relay("events", "missing")]);
        let error = DomainActivationPlan::from_scheduled_nodes(&domain, &relay_nodes)
            .expect_err("the relay schema is required");
        assert!(matches!(
            error.current_context(),
            DomainActivationPlanError::MissingRelaySchema { .. }
        ));

        let mut unresolved_relay = nodes(vec![schema("payload"), relay("events", "payload")]);
        unresolved_relay
            .get_mut(&NodeRef::new(
                ModelKind::Relay,
                named::<ModelName>("events"),
            ))
            .expect("the fixture has a relay")
            .resolved_branching = None;
        let error = DomainActivationPlan::from_scheduled_nodes(&domain, &unresolved_relay)
            .expect_err("the relay branch declaration must be resolved");
        assert!(matches!(
            error.current_context(),
            DomainActivationPlanError::MissingRelayBranching { .. }
        ));

        let branch_name: BranchName = named("by_tenant");
        let branch_schema = match schema("branch_schema") {
            Model::Schema(schema) => schema,
            _ => unreachable!("the schema fixture constructs a schema"),
        };
        let mut branched_relay = nodes(vec![schema("payload"), relay("events", "payload")]);
        branched_relay
            .get_mut(&NodeRef::new(
                ModelKind::Relay,
                named::<ModelName>("events"),
            ))
            .expect("the fixture has a relay")
            .resolved_branching = Some(ResolvedBranching::branched(
            branch_name.clone(),
            branch_schema.clone(),
        ));
        let error = DomainActivationPlan::from_scheduled_nodes(&domain, &branched_relay)
            .expect_err("the named branch must exist");
        assert!(matches!(
            error.current_context(),
            DomainActivationPlanError::MissingRelayBranch { .. }
        ));

        for (ttl, why) in [
            ("invalid", "expected number at 0"),
            (
                TOO_LONG_DURATION_TEXT,
                "it is longer than a duration can be",
            ),
        ] {
            branched_relay.extend(nodes(vec![Model::Branch(CreateBranch {
                name: branch_name.clone(),
                schema: branch_schema.name.clone(),
                ttl: ttl.to_string(),
                eviction: None,
            })]));
            let error = DomainActivationPlan::from_scheduled_nodes(&domain, &branched_relay)
                .expect_err("the branch TTL must be a duration");
            assert!(matches!(
                error.current_context(),
                DomainActivationPlanError::InvalidRelayBranchTtl { .. }
            ));
            assert_eq!(
                error
                    .downcast_ref::<nervix_models::DurationTextError>()
                    .map(ToString::to_string),
                Some(why.to_string())
            );
        }

        let codec_nodes = nodes(vec![codec("decode", CodecWireFormat::Syslog)]);
        let error = DomainActivationPlan::from_scheduled_nodes(&domain, &codec_nodes)
            .expect_err("the codec schema is required");
        assert!(matches!(
            error.current_context(),
            DomainActivationPlanError::MissingCodecSchema { .. }
        ));

        let codec_nodes = nodes(vec![
            schema("payload"),
            codec(
                "decode",
                CodecWireFormat::Json {
                    wire_schema: named("missing"),
                },
            ),
        ]);
        let error = DomainActivationPlan::from_scheduled_nodes(&domain, &codec_nodes)
            .expect_err("the wire schema is required");
        assert!(matches!(
            error.current_context(),
            DomainActivationPlanError::MissingCodecWireSchema { .. }
        ));

        let endpoint_nodes = nodes(vec![Model::Endpoint(CreateEndpoint {
            name: named("receive"),
            on_vhost: named("missing"),
            path: "/ingest".to_string(),
            endpoint_type: EndpointType::Http,
            signaling_protocol: None,
        })]);
        let error = DomainActivationPlan::from_scheduled_nodes(&domain, &endpoint_nodes)
            .expect_err("the endpoint VHOST is required");
        assert!(matches!(
            error.current_context(),
            DomainActivationPlanError::MissingEndpointVhost { .. }
        ));

        let endpoint_nodes = nodes(vec![
            Model::Vhost(CreateVhost {
                name: named("edge"),
                hostnames: vec!["example.com".to_string()],
                tls: None,
            }),
            Model::Endpoint(CreateEndpoint {
                name: named("receive"),
                on_vhost: named("edge"),
                path: "/ingest".to_string(),
                endpoint_type: EndpointType::Websockets,
                signaling_protocol: Some(named("missing")),
            }),
        ]);
        let error = DomainActivationPlan::from_scheduled_nodes(&domain, &endpoint_nodes)
            .expect_err("the endpoint signaling protocol is required");
        assert!(matches!(
            error.current_context(),
            DomainActivationPlanError::MissingEndpointSignalingProtocol { .. }
        ));
    }
}
