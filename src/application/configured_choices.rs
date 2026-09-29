//! Typed choices that name what one domain configures.
//!
//! Layer: edges.
//!
//! - **Owns.** Resolving a structured control's question about one domain's configuration — its
//!   schemas, branches, relays, source references, decoding codecs, and record fields — into
//!   ordered typed choices, their presentation, and the digest of the definitions a page cursor binds.
//! - **Depends on.** Vocabulary Models, names, and node references, and the session choice
//!   contract.
//! - **Must not know.** How the configuration snapshot was read or queued, sessions, transports,
//!   or paging.

use std::collections::BTreeSet;

use meticulous::ResultExt as _;
use nervix_client_wire::{
    Choice, ChoiceLookupRequest, ChoicePresentation, ChoiceSelection, ChoiceStatus, ChoiceTarget,
    ChoiceValue,
};
use nervix_models::{
    BranchName, CanonicalNsplError, CreateBranch, CreateRelay, CreateSchema, DomainName,
    IngestSourceKind, Model, ModelIndex, ModelKind, NodeRef, RelayBranching,
    RequestedResourceVersion, ResourceName, ResourceVersionStatus, SchemaField,
};

use super::session_service::hash_choice_text;

/// A question a structured control asks about one domain's configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::application) enum ConfiguredQuestion {
    Schemas,
    Branches,
    Relays,
    Codecs,
    Vhosts,
    SignalingProtocols,
    WireJsonSchemas,
    WireCborSchemas,
    WireAvroSchemas,
    Resources,
    CompletedResourceVersions(ResourceName),
    /// The fields of the records the named relay carries.
    RelayFields(NodeRef),
    /// Fields of the selected codec's output schema.
    CodecFields(NodeRef),
    IngestSource(IngestSourceKind),
    IngestCodecs,
    IngestRelays(Option<BranchName>),
    BranchFields(NodeRef),
}

/// The domain a lookup reads, and what it asks of that domain's configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::application) struct ConfiguredQuery<'a> {
    pub(in crate::application) domain: &'a DomainName,
    pub(in crate::application) question: ConfiguredQuestion,
}

impl<'a> ConfiguredQuery<'a> {
    /// The query `request` makes, when its target names configuration and its dependencies are
    /// exactly the ones that target needs: a domain, followed for field choices by their relay or
    /// codec.
    pub(in crate::application) fn of(request: &'a ChoiceLookupRequest) -> Option<Self> {
        if let Some(kind) = request.target().ingest_source_kind() {
            let [
                ChoiceSelection {
                    value: ChoiceValue::Domain(domain),
                },
            ] = request.dependencies()
            else {
                return None;
            };
            return Some(Self {
                domain,
                question: ConfiguredQuestion::IngestSource(kind),
            });
        }
        let question = match request.target() {
            ChoiceTarget::Schema => ConfiguredQuestion::Schemas,
            ChoiceTarget::Branch => ConfiguredQuestion::Branches,
            ChoiceTarget::Relay => ConfiguredQuestion::Relays,
            ChoiceTarget::Codec => ConfiguredQuestion::Codecs,
            ChoiceTarget::Vhost => ConfiguredQuestion::Vhosts,
            ChoiceTarget::SignalingProtocol => ConfiguredQuestion::SignalingProtocols,
            ChoiceTarget::WireJsonSchema => ConfiguredQuestion::WireJsonSchemas,
            ChoiceTarget::WireCborSchema => ConfiguredQuestion::WireCborSchemas,
            ChoiceTarget::WireAvroSchema => ConfiguredQuestion::WireAvroSchemas,
            ChoiceTarget::Resource => ConfiguredQuestion::Resources,
            ChoiceTarget::CompletedResourceVersion => {
                return Self::resource_versions(request.dependencies());
            }
            ChoiceTarget::RelayField => return Self::relay_fields(request.dependencies()),
            ChoiceTarget::CodecField => return Self::codec_fields(request.dependencies()),
            ChoiceTarget::IngestCodec => ConfiguredQuestion::IngestCodecs,
            ChoiceTarget::IngestUnbranchedRelay => ConfiguredQuestion::IngestRelays(None),
            ChoiceTarget::IngestBranchedRelay => {
                return Self::ingest_branched_relays(request.dependencies());
            }
            ChoiceTarget::BranchField => return Self::branch_fields(request.dependencies()),
            ChoiceTarget::DomainPace | ChoiceTarget::PlacementPolicy => return None,
            ChoiceTarget::IngestHttpSource
            | ChoiceTarget::IngestKafkaSource
            | ChoiceTarget::IngestPulsarSource
            | ChoiceTarget::IngestMqttSource
            | ChoiceTarget::IngestNatsSource
            | ChoiceTarget::IngestRabbitMqSource
            | ChoiceTarget::IngestRedisPubSubSource
            | ChoiceTarget::IngestPrometheusSource
            | ChoiceTarget::IngestZeroMqSource
            | ChoiceTarget::IngestSqsSource
            | ChoiceTarget::IngestEndpointSource
            | ChoiceTarget::IngestWebsocketsSource
            | ChoiceTarget::IngestSyslogSource => return None,
        };
        let [
            ChoiceSelection {
                value: ChoiceValue::Domain(domain),
            },
        ] = request.dependencies()
        else {
            return None;
        };
        Some(Self { domain, question })
    }

    fn resource_versions(dependencies: &'a [ChoiceSelection]) -> Option<Self> {
        let [
            ChoiceSelection {
                value: ChoiceValue::Domain(domain),
            },
            ChoiceSelection {
                value: ChoiceValue::Resource(resource),
            },
        ] = dependencies
        else {
            return None;
        };
        Some(Self {
            domain,
            question: ConfiguredQuestion::CompletedResourceVersions(resource.clone()),
        })
    }

    fn relay_fields(dependencies: &'a [ChoiceSelection]) -> Option<Self> {
        let [
            ChoiceSelection {
                value: ChoiceValue::Domain(domain),
            },
            ChoiceSelection {
                value: ChoiceValue::Model(relay),
            },
        ] = dependencies
        else {
            return None;
        };
        if relay.kind != ModelKind::Relay {
            return None;
        }
        Some(Self {
            domain,
            question: ConfiguredQuestion::RelayFields(relay.clone()),
        })
    }

    fn codec_fields(dependencies: &'a [ChoiceSelection]) -> Option<Self> {
        let [
            ChoiceSelection {
                value: ChoiceValue::Domain(domain),
            },
            ChoiceSelection {
                value: ChoiceValue::Model(codec),
            },
        ] = dependencies
        else {
            return None;
        };
        if codec.kind != ModelKind::Codec {
            return None;
        }
        Some(Self {
            domain,
            question: ConfiguredQuestion::CodecFields(codec.clone()),
        })
    }

    fn ingest_branched_relays(dependencies: &'a [ChoiceSelection]) -> Option<Self> {
        let [
            ChoiceSelection {
                value: ChoiceValue::Domain(domain),
            },
            ChoiceSelection {
                value: ChoiceValue::Model(branch),
            },
        ] = dependencies
        else {
            return None;
        };
        if branch.kind != ModelKind::Branch {
            return None;
        }
        Some(Self {
            domain,
            question: ConfiguredQuestion::IngestRelays(Some(BranchName::from(&branch.identifier))),
        })
    }

    fn branch_fields(dependencies: &'a [ChoiceSelection]) -> Option<Self> {
        let [
            ChoiceSelection {
                value: ChoiceValue::Domain(domain),
            },
            ChoiceSelection {
                value: ChoiceValue::Model(branch),
            },
        ] = dependencies
        else {
            return None;
        };
        if branch.kind != ModelKind::Branch {
            return None;
        }
        Some(Self {
            domain,
            question: ConfiguredQuestion::BranchFields(branch.clone()),
        })
    }
}

/// The choices a question resolved to, and a digest of every definition that decided them.
#[derive(Debug)]
pub(in crate::application) struct ResolvedChoices {
    pub(in crate::application) choices: Vec<Choice>,
    /// Binds a page cursor to the definitions behind the choices, so a changed definition makes a
    /// continued page stale even when every label stays the same.
    pub(in crate::application) content_digest: String,
}

/// One domain's configuration as a structured control sees it: the stored Models with the
/// requesting session's attached transaction prefix applied.
pub(in crate::application) struct ConfiguredChoices {
    models: ModelIndex<RequestedResourceVersion>,
    resources: ResourceVersionStatus,
    queued_resources: BTreeSet<ResourceName>,
    domain: DomainName,
}

impl ConfiguredChoices {
    pub(in crate::application) fn new(
        domain: DomainName,
        models: Vec<Model<RequestedResourceVersion>>,
        resources: ResourceVersionStatus,
        queued_resources: Vec<String>,
    ) -> Self {
        Self {
            models: models.into_iter().collect(),
            resources,
            queued_resources: queued_resources
                .into_iter()
                .map(|name| {
                    ResourceName::parse(&name).assured(
                        "queued resource suggestions are rendered from typed ResourceName values",
                    )
                })
                .collect(),
            domain,
        }
    }

    /// Resolves `question`, keeping the candidates whose label contains `search` in any case.
    ///
    /// Models are ordered by name. Record fields keep the order their schema declares them in,
    /// because that order is part of the schema.
    pub(in crate::application) fn resolve(
        &self,
        question: &ConfiguredQuestion,
        search: &str,
    ) -> Result<ResolvedChoices, ChoiceStatus> {
        let search = search.to_lowercase();
        match question {
            ConfiguredQuestion::Schemas
            | ConfiguredQuestion::Branches
            | ConfiguredQuestion::Relays
            | ConfiguredQuestion::Codecs
            | ConfiguredQuestion::Vhosts
            | ConfiguredQuestion::SignalingProtocols
            | ConfiguredQuestion::WireJsonSchemas
            | ConfiguredQuestion::WireCborSchemas
            | ConfiguredQuestion::WireAvroSchemas => self.models(question, &search),
            ConfiguredQuestion::IngestSource(_)
            | ConfiguredQuestion::IngestCodecs
            | ConfiguredQuestion::IngestRelays(_) => self.models(question, &search),
            ConfiguredQuestion::RelayFields(relay) => self.relay_fields(relay, &search),
            ConfiguredQuestion::CodecFields(codec) => self.codec_fields(codec, &search),
            ConfiguredQuestion::BranchFields(branch) => self.branch_fields(branch, &search),
            ConfiguredQuestion::Resources => self.resources(&search),
            ConfiguredQuestion::CompletedResourceVersions(resource) => {
                self.completed_versions(resource, &search)
            }
        }
    }

    fn models(
        &self,
        question: &ConfiguredQuestion,
        search: &str,
    ) -> Result<ResolvedChoices, ChoiceStatus> {
        let mut candidates = Vec::new();
        // A listing of one kind reads every model of the domain; nothing is looked up by scanning.
        for model in self.models.models() {
            let candidate = match (question, model) {
                (ConfiguredQuestion::Schemas, Model::Schema(schema)) => {
                    ModelCandidate::schema(schema)?
                }
                (ConfiguredQuestion::Branches, Model::Branch(branch)) => {
                    ModelCandidate::branch(branch)?
                }
                (ConfiguredQuestion::Relays, Model::Relay(relay)) => ModelCandidate::relay(relay)?,
                (ConfiguredQuestion::Codecs, Model::Codec(codec)) => ModelCandidate::new(
                    NodeRef::new(ModelKind::Codec, &codec.name),
                    format!("output schema {}", codec.schema),
                    "Codec",
                    Model::<RequestedResourceVersion>::Codec(codec.clone()).to_canonical_nspl(),
                )?,
                (ConfiguredQuestion::IngestCodecs, Model::Codec(codec))
                    if codec.wire_format.supports_decoding() =>
                {
                    ModelCandidate::new(
                        NodeRef::new(ModelKind::Codec, &codec.name),
                        format!("decodes into schema {}", codec.schema),
                        "Decoding codec",
                        Model::<RequestedResourceVersion>::Codec(codec.clone()).to_canonical_nspl(),
                    )?
                }
                (
                    ConfiguredQuestion::IngestSource(IngestSourceKind::Endpoint),
                    Model::Endpoint(endpoint),
                ) => ModelCandidate::new(
                    NodeRef::new(ModelKind::Endpoint, &endpoint.name),
                    format!("{:?} endpoint", endpoint.endpoint_type),
                    "Endpoint source",
                    model.to_canonical_nspl(),
                )?,
                (ConfiguredQuestion::IngestSource(kind), model)
                    if model.kind() == ModelKind::Client
                        && model.client_type_label() == kind.client_type_label() =>
                {
                    ModelCandidate::new(
                        model.node_ref(),
                        format!("{} client", kind.form_label()),
                        "Client source",
                        model.to_canonical_nspl(),
                    )?
                }
                (ConfiguredQuestion::IngestRelays(branch), Model::Relay(relay))
                    if relay.branching.branch() == branch.as_ref() =>
                {
                    ModelCandidate::relay(relay)?
                }
                (ConfiguredQuestion::Vhosts, Model::Vhost(vhost)) => ModelCandidate::new(
                    NodeRef::new(ModelKind::Vhost, &vhost.name),
                    format!("{} hostnames", vhost.hostnames.len()),
                    "VHOST",
                    Model::<RequestedResourceVersion>::Vhost(vhost.clone()).to_canonical_nspl(),
                )?,
                (ConfiguredQuestion::SignalingProtocols, Model::SignalingProtocol(protocol)) => {
                    ModelCandidate::new(
                        NodeRef::new(ModelKind::SignalingProtocol, &protocol.name),
                        "WebSocket handshake".to_string(),
                        "Signaling protocol",
                        Model::<RequestedResourceVersion>::SignalingProtocol(protocol.clone())
                            .to_canonical_nspl(),
                    )?
                }
                (ConfiguredQuestion::WireJsonSchemas, Model::WireJsonSchema(schema)) => {
                    ModelCandidate::new(
                        NodeRef::new(ModelKind::WireJsonSchema, &schema.name),
                        format!("{} fields", schema.fields.len()),
                        "Wire JSON schema",
                        Model::<RequestedResourceVersion>::WireJsonSchema(schema.clone())
                            .to_canonical_nspl(),
                    )?
                }
                (ConfiguredQuestion::WireCborSchemas, Model::WireCborSchema(schema)) => {
                    ModelCandidate::new(
                        NodeRef::new(ModelKind::WireCborSchema, &schema.name),
                        format!("{} fields", schema.fields.len()),
                        "Wire CBOR schema",
                        Model::<RequestedResourceVersion>::WireCborSchema(schema.clone())
                            .to_canonical_nspl(),
                    )?
                }
                (ConfiguredQuestion::WireAvroSchemas, Model::WireAvroSchema(schema)) => {
                    ModelCandidate::new(
                        NodeRef::new(ModelKind::WireAvroSchema, &schema.name),
                        format!("{} fields", schema.fields.len()),
                        "Wire AVRO schema",
                        Model::<RequestedResourceVersion>::WireAvroSchema(schema.clone())
                            .to_canonical_nspl(),
                    )?
                }
                _ => continue,
            };
            if candidate.matches(search) {
                candidates.push(candidate);
            }
        }
        candidates.sort_by(|left, right| {
            left.choice
                .presentation
                .label
                .cmp(&right.choice.presentation.label)
        });
        let mut digest = blake3::Hasher::new();
        let mut choices = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            hash_choice_text(&mut digest, &candidate.definition);
            choices.push(candidate.choice);
        }
        Ok(ResolvedChoices {
            choices,
            content_digest: digest.finalize().to_hex().to_string(),
        })
    }

    fn resources(&self, search: &str) -> Result<ResolvedChoices, ChoiceStatus> {
        let mut names = BTreeSet::new();
        for counter in &self.resources.next_version_by_resource {
            if counter.domain == self.domain {
                names.insert(counter.identifier.clone());
            }
        }
        for queued in &self.queued_resources {
            names.insert(queued.clone());
        }
        let mut choices = Vec::new();
        let mut digest = blake3::Hasher::new();
        for name in names {
            if !name.as_str().to_lowercase().contains(search) {
                continue;
            }
            hash_choice_text(&mut digest, name.as_str());
            let completed_versions = self
                .resources
                .uploads
                .completed_versions_of(&self.domain, &name)
                .count();
            hash_choice_text(&mut digest, &completed_versions.to_string());
            choices.push(Choice {
                value: ChoiceValue::Resource(name.clone()),
                presentation: ChoicePresentation {
                    label: name.to_string(),
                    detail: Some(format!("{completed_versions} completed versions")),
                    group: Some("Resource".to_string()),
                },
            });
        }
        Ok(ResolvedChoices {
            choices,
            content_digest: digest.finalize().to_hex().to_string(),
        })
    }

    fn completed_versions(
        &self,
        resource: &ResourceName,
        search: &str,
    ) -> Result<ResolvedChoices, ChoiceStatus> {
        let in_catalog = self
            .resources
            .next_version_by_resource
            .binary_search_by(|counter| {
                counter
                    .domain
                    .cmp(&self.domain)
                    .then(counter.identifier.cmp(resource))
            })
            .is_ok();
        let staged = self.queued_resources.contains(resource);
        if !in_catalog && !staged {
            return Err(ChoiceStatus::MissingContext);
        }
        let versions = self
            .resources
            .uploads
            .completed_versions_of(&self.domain, resource)
            .map(|id| id.version)
            .collect::<Vec<_>>();
        let mut choices = Vec::new();
        if !versions.is_empty() && "latest".contains(search) {
            choices.push(Choice {
                value: ChoiceValue::ResourceVersion(RequestedResourceVersion::Latest),
                presentation: ChoicePresentation {
                    label: "LATEST".to_string(),
                    detail: Some(
                        "Highest completed version when the statement is applied".to_string(),
                    ),
                    group: Some("Resource version".to_string()),
                },
            });
        }
        for version in versions {
            if !version.to_string().contains(search) {
                continue;
            }
            choices.push(Choice {
                value: ChoiceValue::ResourceVersion(RequestedResourceVersion::Number(version)),
                presentation: ChoicePresentation {
                    label: version.to_string(),
                    detail: Some("Completed version".to_string()),
                    group: Some("Resource version".to_string()),
                },
            });
        }
        let mut digest = blake3::Hasher::new();
        hash_choice_text(&mut digest, resource.as_str());
        for choice in &choices {
            hash_choice_text(&mut digest, &choice.presentation.label);
        }
        Ok(ResolvedChoices {
            choices,
            content_digest: digest.finalize().to_hex().to_string(),
        })
    }

    fn relay_fields(&self, relay: &NodeRef, search: &str) -> Result<ResolvedChoices, ChoiceStatus> {
        // A relay dropped since the form selected it no longer gives the question its context.
        let Some(Model::Relay(relay)) = self.models.get(relay) else {
            return Err(ChoiceStatus::MissingContext);
        };
        let schema = NodeRef::new(ModelKind::Schema, &relay.schema);
        // A relay staged in a transaction can name a schema its prefix does not declare, and then
        // there are no fields to read.
        let Some(Model::Schema(schema)) = self.models.get(&schema) else {
            return Err(ChoiceStatus::LookupFailed);
        };
        // Every configured Model renders; one that cannot is a lookup that failed.
        let relay_definition = relay
            .to_canonical_nspl()
            .map_err(|_| ChoiceStatus::LookupFailed)?;
        let schema_definition = schema
            .to_canonical_nspl()
            .map_err(|_| ChoiceStatus::LookupFailed)?;
        let mut digest = blake3::Hasher::new();
        hash_choice_text(&mut digest, &relay_definition);
        hash_choice_text(&mut digest, &schema_definition);
        let mut choices = Vec::new();
        for field in &schema.fields {
            let choice = Self::field_choice(field, "Relay field");
            if choice.presentation.label.to_lowercase().contains(search) {
                choices.push(choice);
            }
        }
        Ok(ResolvedChoices {
            choices,
            content_digest: digest.finalize().to_hex().to_string(),
        })
    }

    fn codec_fields(&self, codec: &NodeRef, search: &str) -> Result<ResolvedChoices, ChoiceStatus> {
        let Some(Model::Codec(codec)) = self.models.get(codec) else {
            return Err(ChoiceStatus::MissingContext);
        };
        let schema_ref = NodeRef::new(ModelKind::Schema, &codec.schema);
        let Some(Model::Schema(schema)) = self.models.get(&schema_ref) else {
            return Err(ChoiceStatus::LookupFailed);
        };
        let codec_definition = codec
            .to_canonical_nspl()
            .map_err(|_| ChoiceStatus::LookupFailed)?;
        let schema_definition = schema
            .to_canonical_nspl()
            .map_err(|_| ChoiceStatus::LookupFailed)?;
        let mut digest = blake3::Hasher::new();
        hash_choice_text(&mut digest, &codec_definition);
        hash_choice_text(&mut digest, &schema_definition);
        let choices = schema
            .fields
            .iter()
            .map(|field| Self::field_choice(field, "Codec field"))
            .filter(|choice| choice.presentation.label.to_lowercase().contains(search))
            .collect();
        Ok(ResolvedChoices {
            choices,
            content_digest: digest.finalize().to_hex().to_string(),
        })
    }

    fn branch_fields(
        &self,
        branch: &NodeRef,
        search: &str,
    ) -> Result<ResolvedChoices, ChoiceStatus> {
        let Some(Model::Branch(branch)) = self.models.get(branch) else {
            return Err(ChoiceStatus::MissingContext);
        };
        let schema_ref = NodeRef::new(ModelKind::Schema, &branch.schema);
        let Some(Model::Schema(schema)) = self.models.get(&schema_ref) else {
            return Err(ChoiceStatus::LookupFailed);
        };
        let mut digest = blake3::Hasher::new();
        hash_choice_text(
            &mut digest,
            &branch
                .to_canonical_nspl()
                .map_err(|_| ChoiceStatus::LookupFailed)?,
        );
        hash_choice_text(
            &mut digest,
            &schema
                .to_canonical_nspl()
                .map_err(|_| ChoiceStatus::LookupFailed)?,
        );
        let choices = schema
            .fields
            .iter()
            .map(|field| Self::field_choice(field, "Branch field"))
            .filter(|choice| choice.presentation.label.to_lowercase().contains(search))
            .collect();
        Ok(ResolvedChoices {
            choices,
            content_digest: digest.finalize().to_hex().to_string(),
        })
    }

    /// A field offered by name, presented with its exact type and modifiers as its schema declares
    /// them.
    fn field_choice(field: &SchemaField, group: &str) -> Choice {
        let mut detail = field.ty.to_string();
        if field.optional {
            detail.push_str(" OPTIONAL");
        }
        if field.sensitive {
            detail.push_str(" SENSITIVE");
        }
        Choice {
            value: ChoiceValue::Field(field.name.clone()),
            presentation: ChoicePresentation {
                label: field.name.to_string(),
                detail: Some(detail),
                group: Some(group.to_string()),
            },
        }
    }
}

/// One offered model and the canonical definition behind it.
struct ModelCandidate {
    choice: Choice,
    definition: String,
}

impl ModelCandidate {
    /// A model offered by its name, with the definition that renders it. Every configured Model
    /// renders; one that cannot is a lookup that failed.
    fn new(
        node: NodeRef,
        detail: String,
        group: &str,
        definition: error_stack::Result<String, CanonicalNsplError>,
    ) -> Result<Self, ChoiceStatus> {
        let definition = definition.map_err(|_| ChoiceStatus::LookupFailed)?;
        Ok(Self {
            choice: Choice {
                presentation: ChoicePresentation {
                    label: node.identifier.to_string(),
                    detail: Some(detail),
                    group: Some(group.to_string()),
                },
                value: ChoiceValue::Model(node),
            },
            definition,
        })
    }

    fn schema(schema: &CreateSchema) -> Result<Self, ChoiceStatus> {
        let detail = format!("{} fields", schema.fields.len());
        let node = NodeRef::new(ModelKind::Schema, &schema.name);
        Self::new(node, detail, "Schema", schema.to_canonical_nspl())
    }

    fn branch(branch: &CreateBranch) -> Result<Self, ChoiceStatus> {
        let mut detail = format!("key schema {} · TTL {}", branch.schema, branch.ttl);
        if let Some(eviction) = &branch.eviction {
            detail.push_str(&format!(
                " · at most {} instances",
                eviction.max_instances()
            ));
        }
        let node = NodeRef::new(ModelKind::Branch, &branch.name);
        Self::new(node, detail, "Branch", branch.to_canonical_nspl())
    }

    fn relay(relay: &CreateRelay) -> Result<Self, ChoiceStatus> {
        let branching = match &relay.branching {
            RelayBranching::Unbranched => "unbranched".to_string(),
            RelayBranching::BranchedBy { branch } => format!("branched by {branch}"),
        };
        let mut detail = format!("schema {} · {branching}", relay.schema);
        if relay.materialized_state.is_some() {
            detail.push_str(" · materialized");
        }
        let node = NodeRef::new(ModelKind::Relay, &relay.name);
        Self::new(node, detail, "Relay", relay.to_canonical_nspl())
    }

    fn matches(&self, search: &str) -> bool {
        self.choice
            .presentation
            .label
            .to_lowercase()
            .contains(search)
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};

    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_client_wire::{
        ChoiceLookupRequest, ChoiceSelection, ChoiceStatus, ChoiceTarget, ChoiceValue,
    };
    use nervix_models::{
        AvroType, BranchEviction, BranchName, CodecName, CodecWireFormat, CreateBranch,
        CreateCodec, CreateRelay, CreateSchema, CreateSignalingProtocol, CreateVhost,
        CreateWireSchema, DomainName, FieldName, JsonType, MaterializedRelayState, Model,
        ModelKind, ModelName, NodeRef, ParseAsType, RelayBranching, RelayName,
        RequestedResourceVersion, ResourceName, ResourceUpload, ResourceUploadIdentity,
        ResourceUploadKey, ResourceUploadState, ResourceUploads, ResourceVersionCounter,
        ResourceVersionStatus, SchemaField, SchemaName, SignalingProtocolName,
        SignalingProtocolOnConnect, SignalingStep, SignalingWireFormat, UserName, VhostName,
        WireSchemaField, WireSchemaName, WireSchemaStrictness,
    };
    use sorted_vec::SortedVec;

    use super::{ConfiguredChoices, ConfiguredQuery, ConfiguredQuestion};

    fn field(name: &str, ty: ParseAsType, optional: bool, sensitive: bool) -> SchemaField {
        SchemaField {
            name: FieldName::parse(name).assured("the test field names are valid"),
            ty,
            optional,
            sensitive,
        }
    }

    fn schema(name: &str, fields: Vec<SchemaField>) -> Model<RequestedResourceVersion> {
        Model::Schema(CreateSchema {
            name: SchemaName::parse(name).assured("the test schema names are valid"),
            fields,
        })
    }

    fn relay(
        name: &str,
        schema: &str,
        branching: RelayBranching,
        materialized_state: Option<MaterializedRelayState>,
    ) -> Model<RequestedResourceVersion> {
        Model::Relay(CreateRelay {
            name: RelayName::parse(name).assured("the test relay names are valid"),
            schema: SchemaName::parse(schema).assured("the test schema names are valid"),
            buffer: NonZeroUsize::MIN,
            branching,
            materialized_state,
        })
    }

    fn branch(name: &str, eviction: Option<BranchEviction>) -> Model<RequestedResourceVersion> {
        Model::Branch(CreateBranch {
            name: BranchName::parse(name).assured("the test branch names are valid"),
            schema: SchemaName::parse("tenant_key").assured("the test schema name is valid"),
            ttl: "5m".to_string(),
            eviction,
        })
    }

    fn configuration() -> ConfiguredChoices {
        let tenant = BranchName::parse("by_tenant").assured("the test branch name is valid");
        ConfiguredChoices::new(
            domain(),
            vec![
                schema(
                    "tenant_key",
                    vec![field("tenant", ParseAsType::String, false, false)],
                ),
                schema(
                    "order_record",
                    vec![
                        field("tenant", ParseAsType::String, false, false),
                        field("amount", ParseAsType::I64, true, true),
                        field("placed_at", ParseAsType::Datetime, false, false),
                    ],
                ),
                branch("by_tenant", None),
                branch(
                    "bounded_tenants",
                    Some(BranchEviction::Lru {
                        max_instances: NonZeroU64::new(3).assured("three is nonzero"),
                    }),
                ),
                relay(
                    "orders",
                    "order_record",
                    RelayBranching::branched_by(tenant),
                    Some(MaterializedRelayState::LastByTimestamp),
                ),
                relay("audit", "order_record", RelayBranching::unbranched(), None),
                relay(
                    "dangling",
                    "missing_record",
                    RelayBranching::unbranched(),
                    None,
                ),
            ],
            ResourceVersionStatus::default(),
            Vec::new(),
        )
    }

    fn domain() -> DomainName {
        DomainName::parse("tenant").assured("the test domain name is valid")
    }

    fn relay_node(name: &str) -> NodeRef {
        NodeRef::new(
            ModelKind::Relay,
            ModelName::parse(name).assured("the test relay names are valid"),
        )
    }

    fn labels(question: &ConfiguredQuestion, search: &str) -> Vec<String> {
        configuration()
            .resolve(question, search)
            .assured("the test configuration answers every listed question")
            .choices
            .into_iter()
            .map(|choice| choice.presentation.label)
            .collect()
    }

    #[test]
    fn ingestor_questions_keep_source_kind_branch_and_field_dependencies_typed() {
        use nervix_models::IngestSourceKind;

        let in_domain = vec![ChoiceSelection {
            value: ChoiceValue::Domain(domain()),
        }];
        for kind in IngestSourceKind::ALL {
            let request = ChoiceLookupRequest::new(
                ChoiceTarget::for_ingest_source(kind),
                in_domain.clone(),
                String::new(),
            );
            assert_eq!(
                ConfiguredQuery::of(&request).map(|query| query.question),
                Some(ConfiguredQuestion::IngestSource(kind)),
            );
        }
        assert_eq!(
            labels(&ConfiguredQuestion::IngestRelays(None), ""),
            ["audit", "dangling"]
        );
        let branch = BranchName::parse("by_tenant").assured("valid branch");
        assert_eq!(
            labels(&ConfiguredQuestion::IngestRelays(Some(branch.clone())), ""),
            ["orders"]
        );
        let branch_ref = NodeRef::new(ModelKind::Branch, &branch);
        assert_eq!(
            labels(&ConfiguredQuestion::BranchFields(branch_ref.clone()), ""),
            ["tenant"]
        );
        let branched = ChoiceLookupRequest::new(
            ChoiceTarget::IngestBranchedRelay,
            vec![
                ChoiceSelection {
                    value: ChoiceValue::Domain(domain()),
                },
                ChoiceSelection {
                    value: ChoiceValue::Model(branch_ref.clone()),
                },
            ],
            String::new(),
        );
        assert_eq!(
            ConfiguredQuery::of(&branched).map(|query| query.question),
            Some(ConfiguredQuestion::IngestRelays(Some(branch))),
        );
        let key_fields = ChoiceLookupRequest::new(
            ChoiceTarget::BranchField,
            branched.dependencies().to_vec(),
            String::new(),
        );
        assert_eq!(
            ConfiguredQuery::of(&key_fields).map(|query| query.question),
            Some(ConfiguredQuestion::BranchFields(branch_ref)),
        );
    }

    #[test]
    fn ingestor_source_and_codec_choices_offer_only_matching_capabilities() {
        use nervix_models::{
            ClientName, CodecJaqFormat, CodecJaqTransformations, CreateClientHttp,
            CreateClientKafka, CreateEndpoint, EndpointName, EndpointType, IngestSourceKind,
        };

        let models = vec![
            Model::ClientHttp(CreateClientHttp {
                name: ClientName::parse("http_input").assured("valid client"),
                mount: None,
                config: Vec::new(),
            }),
            Model::ClientKafka(CreateClientKafka {
                name: ClientName::parse("kafka_input").assured("valid client"),
                mount: None,
                config: Vec::new(),
            }),
            Model::Endpoint(CreateEndpoint {
                name: EndpointName::parse("listener").assured("valid endpoint"),
                on_vhost: VhostName::parse("host").assured("valid VHOST"),
                path: "/in".into(),
                endpoint_type: EndpointType::Http,
                signaling_protocol: None,
            }),
            Model::Codec(CreateCodec {
                name: CodecName::parse("decode").assured("valid codec"),
                wire_format: CodecWireFormat::JaqNative {
                    format: CodecJaqFormat::Json,
                    transformations: CodecJaqTransformations {
                        on_ingestion: Some(".".into()),
                        ..CodecJaqTransformations::default()
                    },
                },
                schema: SchemaName::parse("records").assured("valid schema"),
                encoding_rules: Vec::new(),
            }),
            Model::Codec(CreateCodec {
                name: CodecName::parse("encode_only").assured("valid codec"),
                wire_format: CodecWireFormat::JaqNative {
                    format: CodecJaqFormat::Json,
                    transformations: CodecJaqTransformations {
                        on_emitting: Some(".".into()),
                        ..CodecJaqTransformations::default()
                    },
                },
                schema: SchemaName::parse("records").assured("valid schema"),
                encoding_rules: Vec::new(),
            }),
        ];
        let choices = ConfiguredChoices::new(
            domain(),
            models,
            ResourceVersionStatus::default(),
            Vec::new(),
        );
        let labels = |question| {
            choices
                .resolve(&question, "")
                .assured("configured choices resolve")
                .choices
                .into_iter()
                .map(|choice| choice.presentation.label)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            labels(ConfiguredQuestion::IngestSource(IngestSourceKind::Http)),
            ["http_input"]
        );
        assert_eq!(
            labels(ConfiguredQuestion::IngestSource(IngestSourceKind::Kafka)),
            ["kafka_input"]
        );
        assert_eq!(
            labels(ConfiguredQuestion::IngestSource(IngestSourceKind::Endpoint)),
            ["listener"]
        );
        assert_eq!(labels(ConfiguredQuestion::IngestCodecs), ["decode"]);
    }

    #[test]
    fn a_configured_question_needs_exactly_the_typed_dependencies_of_its_target() {
        let in_domain = vec![ChoiceSelection {
            value: ChoiceValue::Domain(domain()),
        }];
        for (target, question) in [
            (ChoiceTarget::Schema, ConfiguredQuestion::Schemas),
            (ChoiceTarget::Branch, ConfiguredQuestion::Branches),
            (ChoiceTarget::Relay, ConfiguredQuestion::Relays),
        ] {
            let request = ChoiceLookupRequest::new(target, in_domain.clone(), String::new());
            let query = ConfiguredQuery::of(&request).assured("a domain is the only dependency");
            assert_eq!(query.domain, &domain());
            assert_eq!(query.question, question);
            let bare = ChoiceLookupRequest::new(target, Vec::new(), String::new());
            assert_eq!(ConfiguredQuery::of(&bare), None);
        }

        let with_relay = |node: NodeRef| {
            ChoiceLookupRequest::new(
                ChoiceTarget::RelayField,
                vec![
                    ChoiceSelection {
                        value: ChoiceValue::Domain(domain()),
                    },
                    ChoiceSelection {
                        value: ChoiceValue::Model(node),
                    },
                ],
                String::new(),
            )
        };
        let request = with_relay(relay_node("orders"));
        assert_eq!(
            ConfiguredQuery::of(&request)
                .assured("a domain and then a relay are the field dependencies")
                .question,
            ConfiguredQuestion::RelayFields(relay_node("orders"))
        );
        let schema_node = NodeRef::new(
            ModelKind::Schema,
            ModelName::parse("order_record").assured("valid model name"),
        );
        assert_eq!(ConfiguredQuery::of(&with_relay(schema_node)), None);
        let domain_only =
            ChoiceLookupRequest::new(ChoiceTarget::RelayField, in_domain.clone(), String::new());
        assert_eq!(ConfiguredQuery::of(&domain_only), None);
        let enumerated = ChoiceLookupRequest::new(ChoiceTarget::DomainPace, in_domain, "".into());
        assert_eq!(ConfiguredQuery::of(&enumerated), None);
    }

    #[test]
    fn models_of_each_kind_are_ordered_searched_and_described() {
        assert_eq!(
            labels(&ConfiguredQuestion::Schemas, ""),
            ["order_record", "tenant_key"]
        );
        assert_eq!(
            labels(&ConfiguredQuestion::Branches, ""),
            ["bounded_tenants", "by_tenant"]
        );
        assert_eq!(
            labels(&ConfiguredQuestion::Relays, ""),
            ["audit", "dangling", "orders"]
        );
        assert_eq!(labels(&ConfiguredQuestion::Relays, "ORD"), ["orders"]);
        assert!(labels(&ConfiguredQuestion::Branches, "nothing").is_empty());

        let relays = configuration()
            .resolve(&ConfiguredQuestion::Relays, "")
            .assured("relays resolve");
        let orders = &relays.choices[2];
        assert_eq!(orders.value, ChoiceValue::Model(relay_node("orders")));
        assert_eq!(
            orders.presentation.detail.as_deref(),
            Some("schema order_record · branched by by_tenant · materialized")
        );
        assert_eq!(
            relays.choices[0].presentation.detail.as_deref(),
            Some("schema order_record · unbranched")
        );
        let branches = configuration()
            .resolve(&ConfiguredQuestion::Branches, "")
            .assured("branches resolve");
        assert_eq!(
            branches.choices[0].presentation.detail.as_deref(),
            Some("key schema tenant_key · TTL 5m · at most 3 instances")
        );
        let schemas = configuration()
            .resolve(&ConfiguredQuestion::Schemas, "")
            .assured("schemas resolve");
        assert_eq!(
            schemas.choices[0].presentation.detail.as_deref(),
            Some("3 fields")
        );
    }

    #[test]
    fn codec_and_its_output_fields_use_the_same_domain_snapshot() {
        let domain = domain();
        let schema = CreateSchema {
            name: SchemaName::parse("lookup_entry").assured("valid schema"),
            fields: vec![
                SchemaField {
                    name: FieldName::parse("id").assured("valid field"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: FieldName::parse("value").assured("valid field"),
                    ty: ParseAsType::I64,
                    optional: true,
                    sensitive: true,
                },
            ],
        };
        let codec = CreateCodec::<RequestedResourceVersion> {
            name: CodecName::parse("lookup_codec").assured("valid codec"),
            wire_format: CodecWireFormat::Json {
                wire_schema: WireSchemaName::parse("lookup_wire").assured("valid wire schema"),
            },
            schema: schema.name.clone(),
            encoding_rules: Vec::new(),
        };
        let codec_ref = NodeRef::new(ModelKind::Codec, &codec.name);
        let configured = ConfiguredChoices::new(
            domain.clone(),
            vec![Model::Schema(schema), Model::Codec(codec)],
            ResourceVersionStatus::default(),
            Vec::new(),
        );
        let codec_request = ChoiceLookupRequest::new(
            ChoiceTarget::Codec,
            vec![ChoiceSelection {
                value: ChoiceValue::Domain(domain.clone()),
            }],
            String::new(),
        );
        let codec_query = ConfiguredQuery::of(&codec_request).assured("codec asks in one domain");
        let codecs = configured
            .resolve(&codec_query.question, "LOOKUP")
            .assured("codec resolves");
        assert_eq!(codecs.choices.len(), 1);
        assert_eq!(
            codecs.choices[0].value,
            ChoiceValue::Model(codec_ref.clone())
        );
        let fields_request = ChoiceLookupRequest::new(
            ChoiceTarget::CodecField,
            vec![
                ChoiceSelection {
                    value: ChoiceValue::Domain(domain.clone()),
                },
                ChoiceSelection {
                    value: ChoiceValue::Model(codec_ref.clone()),
                },
            ],
            String::new(),
        );
        let fields_query = ConfiguredQuery::of(&fields_request)
            .assured("codec fields require domain and typed codec");
        let fields = configured
            .resolve(&fields_query.question, "")
            .assured("codec output schema resolves");
        assert_eq!(fields.choices[0].presentation.label, "id");
        assert_eq!(
            fields.choices[1].presentation.detail.as_deref(),
            Some("I64 OPTIONAL SENSITIVE")
        );
        assert_eq!(
            configured
                .resolve(
                    &ConfiguredQuestion::CodecFields(NodeRef::new(
                        ModelKind::Codec,
                        ModelName::parse("missing").assured("valid name"),
                    )),
                    ""
                )
                .err(),
            Some(ChoiceStatus::MissingContext)
        );
        let wrong_kind = ChoiceLookupRequest::new(
            ChoiceTarget::CodecField,
            vec![
                ChoiceSelection {
                    value: ChoiceValue::Domain(domain),
                },
                ChoiceSelection {
                    value: ChoiceValue::Model(NodeRef::new(
                        ModelKind::Relay,
                        ModelName::parse("lookup_codec").assured("valid name"),
                    )),
                },
            ],
            String::new(),
        );
        assert_eq!(ConfiguredQuery::of(&wrong_kind), None);
    }

    #[test]
    fn vhost_and_signaling_choices_use_typed_domain_references() {
        let domain = domain();
        let configured = ConfiguredChoices::new(
            domain.clone(),
            vec![
                Model::Vhost(CreateVhost::<RequestedResourceVersion> {
                    name: VhostName::parse("edge").assured("valid VHOST"),
                    hostnames: vec!["api.example.com".to_string()],
                    tls: None,
                }),
                Model::SignalingProtocol(CreateSignalingProtocol::<RequestedResourceVersion> {
                    name: SignalingProtocolName::parse("handshake").assured("valid protocol"),
                    format: SignalingWireFormat::Json,
                    on_connect: SignalingProtocolOnConnect {
                        accept_data: false,
                        steps: vec![SignalingStep::Send(vec!["{hello: true}".to_string()])],
                        fail_matchers: Vec::new(),
                        timeout: "5s".to_string(),
                    },
                }),
            ],
            ResourceVersionStatus::default(),
            Vec::new(),
        );
        for (target, question, kind, name) in [
            (
                ChoiceTarget::Vhost,
                ConfiguredQuestion::Vhosts,
                ModelKind::Vhost,
                "edge",
            ),
            (
                ChoiceTarget::SignalingProtocol,
                ConfiguredQuestion::SignalingProtocols,
                ModelKind::SignalingProtocol,
                "handshake",
            ),
        ] {
            let request = ChoiceLookupRequest::new(
                target,
                vec![ChoiceSelection {
                    value: ChoiceValue::Domain(domain.clone()),
                }],
                String::new(),
            );
            assert_eq!(
                ConfiguredQuery::of(&request)
                    .assured("domain dependency is typed")
                    .question,
                question
            );
            let choices = configured
                .resolve(&question, "")
                .assured("models resolve")
                .choices;
            assert_eq!(choices.len(), 1);
            assert_eq!(
                choices[0].value,
                ChoiceValue::Model(NodeRef::new(
                    kind,
                    ModelName::parse(name).assured("valid model name"),
                ))
            );
            assert_eq!(choices[0].presentation.label, name);
            assert_eq!(
                ConfiguredQuery::of(&ChoiceLookupRequest::new(target, Vec::new(), String::new())),
                None
            );
        }
    }

    #[test]
    fn relay_fields_keep_their_declared_order_types_and_modifiers() {
        let resolved = configuration()
            .resolve(&ConfiguredQuestion::RelayFields(relay_node("orders")), "")
            .assured("the relay and its schema exist");
        let fields = resolved
            .choices
            .iter()
            .map(|choice| {
                (
                    choice.presentation.label.as_str(),
                    choice.presentation.detail.as_deref(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            fields,
            [
                ("tenant", Some("STRING")),
                ("amount", Some("I64 OPTIONAL SENSITIVE")),
                ("placed_at", Some("DATETIME")),
            ]
        );
        assert_eq!(
            resolved.choices[1].value,
            ChoiceValue::Field(FieldName::parse("amount").assured("valid field name"))
        );
        let searched = configuration()
            .resolve(&ConfiguredQuestion::RelayFields(relay_node("orders")), "AM")
            .assured("the relay and its schema exist");
        assert_eq!(searched.choices.len(), 1);
    }

    #[test]
    fn a_missing_relay_or_schema_is_distinct_from_an_empty_answer() {
        let choices = configuration();
        assert_eq!(
            choices
                .resolve(&ConfiguredQuestion::RelayFields(relay_node("ghost")), "")
                .err(),
            Some(ChoiceStatus::MissingContext)
        );
        assert_eq!(
            choices
                .resolve(&ConfiguredQuestion::RelayFields(relay_node("dangling")), "")
                .err(),
            Some(ChoiceStatus::LookupFailed)
        );
        let empty = choices
            .resolve(
                &ConfiguredQuestion::RelayFields(relay_node("orders")),
                "zzz",
            )
            .assured("no match is an ordinary empty answer");
        assert!(empty.choices.is_empty());
    }

    #[test]
    fn the_content_digest_follows_the_definitions_behind_the_choices() {
        let digest = |models: Vec<Model<RequestedResourceVersion>>, question| {
            ConfiguredChoices::new(
                domain(),
                models,
                ResourceVersionStatus::default(),
                Vec::new(),
            )
            .resolve(&question, "")
            .assured("the test configuration resolves")
            .content_digest
        };
        let definitions = |ty| {
            vec![
                schema("order_record", vec![field("amount", ty, false, false)]),
                relay("orders", "order_record", RelayBranching::unbranched(), None),
            ]
        };
        let fields = || ConfiguredQuestion::RelayFields(relay_node("orders"));
        assert_eq!(
            digest(definitions(ParseAsType::I64), fields()),
            digest(definitions(ParseAsType::I64), fields())
        );
        assert_ne!(
            digest(definitions(ParseAsType::I64), fields()),
            digest(definitions(ParseAsType::U64), fields())
        );
        assert_ne!(
            digest(definitions(ParseAsType::I64), ConfiguredQuestion::Schemas),
            digest(definitions(ParseAsType::U64), ConfiguredQuestion::Schemas)
        );
    }

    #[test]
    fn exact_wire_schema_queries_do_not_mix_same_named_formats() {
        let name = WireSchemaName::parse("payload_wire").assured("valid wire schema name");
        let models = vec![
            Model::WireJsonSchema(CreateWireSchema {
                name: name.clone(),
                strictness: WireSchemaStrictness::Strict,
                fields: vec![WireSchemaField {
                    name: FieldName::parse("message").assured("valid field"),
                    ty: JsonType::String,
                    optional: false,
                }],
            }),
            Model::WireCborSchema(CreateWireSchema {
                name: name.clone(),
                strictness: WireSchemaStrictness::Loose,
                fields: vec![WireSchemaField {
                    name: FieldName::parse("message").assured("valid field"),
                    ty: JsonType::String,
                    optional: false,
                }],
            }),
            Model::WireAvroSchema(CreateWireSchema {
                name,
                strictness: WireSchemaStrictness::Strict,
                fields: vec![WireSchemaField {
                    name: FieldName::parse("message").assured("valid field"),
                    ty: AvroType::String,
                    optional: false,
                }],
            }),
        ];
        let resolver = ConfiguredChoices::new(
            domain(),
            models,
            ResourceVersionStatus::default(),
            Vec::new(),
        );
        for (target, question, kind) in [
            (
                ChoiceTarget::WireJsonSchema,
                ConfiguredQuestion::WireJsonSchemas,
                ModelKind::WireJsonSchema,
            ),
            (
                ChoiceTarget::WireCborSchema,
                ConfiguredQuestion::WireCborSchemas,
                ModelKind::WireCborSchema,
            ),
            (
                ChoiceTarget::WireAvroSchema,
                ConfiguredQuestion::WireAvroSchemas,
                ModelKind::WireAvroSchema,
            ),
        ] {
            let request = ChoiceLookupRequest::new(
                target,
                vec![ChoiceSelection {
                    value: ChoiceValue::Domain(domain()),
                }],
                String::new(),
            );
            assert_eq!(
                ConfiguredQuery::of(&request)
                    .assured("domain query")
                    .question,
                question
            );
            let choices = resolver
                .resolve(&question, "PAYLOAD")
                .assured("wire schema resolves")
                .choices;
            assert_eq!(choices.len(), 1);
            assert_eq!(
                choices[0].value,
                ChoiceValue::Model(NodeRef::new(
                    kind,
                    ModelName::parse("payload_wire").assured("valid model name")
                ))
            );
        }
    }

    #[test]
    fn resource_choices_include_staged_catalogs_but_only_completed_versions() {
        let resource = ResourceName::parse("proto_bundle").assured("valid resource");
        let key = |identity: &str| {
            ResourceUploadKey::new(
                UserName::parse("operator").assured("valid user"),
                domain(),
                resource.clone(),
                ResourceUploadIdentity::parse(identity).assured("valid upload identity"),
            )
        };
        let resources = ResourceVersionStatus {
            next_version_by_resource: SortedVec::from_unsorted(vec![ResourceVersionCounter {
                domain: domain(),
                identifier: resource.clone(),
                next_version: 4,
            }]),
            uploads: ResourceUploads::try_from_uploads([
                ResourceUpload {
                    key: key("completed"),
                    version: 1,
                    state: ResourceUploadState::Completed {
                        root_checksum: "one".to_string(),
                        outcome_revision: 1,
                    },
                },
                ResourceUpload {
                    key: key("applying"),
                    version: 2,
                    state: ResourceUploadState::Applying {
                        root_checksum: "two".to_string(),
                    },
                },
                ResourceUpload {
                    key: key("failed"),
                    version: 3,
                    state: ResourceUploadState::Failed {
                        root_checksum: "three".to_string(),
                        outcome_revision: 2,
                        reason: "failed".to_string(),
                    },
                },
            ])
            .assured("upload identities are unique"),
            ..ResourceVersionStatus::default()
        };
        let resolver = ConfiguredChoices::new(
            domain(),
            Vec::new(),
            resources,
            vec!["staged_bundle".to_string()],
        );
        let names = resolver
            .resolve(&ConfiguredQuestion::Resources, "bundle")
            .assured("resource query resolves")
            .choices
            .into_iter()
            .map(|choice| choice.presentation.label)
            .collect::<Vec<_>>();
        assert_eq!(names, ["proto_bundle", "staged_bundle"]);
        let versions = resolver
            .resolve(
                &ConfiguredQuestion::CompletedResourceVersions(resource.clone()),
                "",
            )
            .assured("completed version query resolves")
            .choices;
        assert_eq!(
            versions
                .iter()
                .map(|choice| &choice.value)
                .collect::<Vec<_>>(),
            [
                &ChoiceValue::ResourceVersion(RequestedResourceVersion::Latest),
                &ChoiceValue::ResourceVersion(RequestedResourceVersion::Number(1)),
            ]
        );
        assert_eq!(
            resolver
                .resolve(
                    &ConfiguredQuestion::CompletedResourceVersions(resource),
                    "2"
                )
                .assured("unfinished version is filtered")
                .choices
                .len(),
            0
        );
    }
}
