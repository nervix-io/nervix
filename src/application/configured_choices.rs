//! Typed choices that name what one domain configures.
//!
//! Layer: edges.
//!
//! - **Owns.** Resolving a structured control's question about one domain's configuration — its
//!   internal schemas, branches, relays, and the fields a relay's records carry — into ordered
//!   typed choices, their presentation, and the digest of the definitions a page cursor binds.
//! - **Depends on.** Vocabulary Models, names, and node references, and the session choice
//!   contract.
//! - **Must not know.** How the configuration snapshot was read or queued, sessions, transports,
//!   or paging.

use nervix_client_wire::{
    Choice, ChoiceLookupRequest, ChoicePresentation, ChoiceSelection, ChoiceStatus, ChoiceTarget,
    ChoiceValue,
};
use nervix_models::{
    CanonicalNsplError, CreateBranch, CreateRelay, CreateSchema, DomainName, Model, ModelIndex,
    ModelKind, NodeRef, RelayBranching, RequestedResourceVersion, SchemaField,
};

use super::session_service::hash_choice_text;

/// A question a structured control asks about one domain's configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::application) enum ConfiguredQuestion {
    Schemas,
    Branches,
    Relays,
    /// The fields of the records the named relay carries.
    RelayFields(NodeRef),
}

/// The domain a lookup reads, and what it asks of that domain's configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::application) struct ConfiguredQuery<'a> {
    pub(in crate::application) domain: &'a DomainName,
    pub(in crate::application) question: ConfiguredQuestion,
}

impl<'a> ConfiguredQuery<'a> {
    /// The query `request` makes, when its target names configuration and its dependencies are
    /// exactly the ones that target needs: a domain, followed for relay fields by that relay.
    pub(in crate::application) fn of(request: &'a ChoiceLookupRequest) -> Option<Self> {
        let question = match request.target() {
            ChoiceTarget::Schema => ConfiguredQuestion::Schemas,
            ChoiceTarget::Branch => ConfiguredQuestion::Branches,
            ChoiceTarget::Relay => ConfiguredQuestion::Relays,
            ChoiceTarget::RelayField => return Self::relay_fields(request.dependencies()),
            ChoiceTarget::DomainPace | ChoiceTarget::PlacementPolicy => return None,
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
}

impl ConfiguredChoices {
    pub(in crate::application) fn new(models: Vec<Model<RequestedResourceVersion>>) -> Self {
        Self {
            models: models.into_iter().collect(),
        }
    }

    /// Resolves `question`, keeping the candidates whose label contains `search` in any case.
    ///
    /// Models are ordered by name. A relay's fields keep the order its schema declares them in,
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
            | ConfiguredQuestion::Relays => self.models(question, &search),
            ConfiguredQuestion::RelayFields(relay) => self.relay_fields(relay, &search),
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
            let choice = Self::field_choice(field);
            if choice.presentation.label.to_lowercase().contains(search) {
                choices.push(choice);
            }
        }
        Ok(ResolvedChoices {
            choices,
            content_digest: digest.finalize().to_hex().to_string(),
        })
    }

    /// A field offered by name, presented with its exact type and modifiers as its schema declares
    /// them.
    fn field_choice(field: &SchemaField) -> Choice {
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
                group: Some("Relay field".to_string()),
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
        BranchEviction, BranchName, CreateBranch, CreateRelay, CreateSchema, DomainName, FieldName,
        MaterializedRelayState, Model, ModelKind, ModelName, NodeRef, ParseAsType, RelayBranching,
        RelayName, RequestedResourceVersion, SchemaField, SchemaName,
    };

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
        ConfiguredChoices::new(vec![
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
        ])
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
            ConfiguredChoices::new(models)
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
}
