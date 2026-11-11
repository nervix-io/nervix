//! Semantic choices for visual emitter construction.
//!
//! Layer: edges.
//! - **Owns.** Emitter reference and catalog choices, and the typed questions an emitter form asks
//!   about its codec and relays.
//! - **Depends on.** The immutable configured-choice snapshot, vocabulary declarations and the
//!   vocabulary's codec and relay questions.
//! - **Must not know.** Browser drafts, transport state or emitter execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns emitter structured choices")
)]

use nervix_client_wire::{
    ChoiceLookupRequest, ChoiceSelection, ChoiceStatus, ChoiceTarget, ChoiceValue,
};
use nervix_models::{
    CodecDirection, EmitterEntityKind, IcebergEmitterEntity, Model, ModelKind, OptionForm,
    OptionValue, RelayName, RelayQuestion,
};

use super::{ConfiguredChoices, ConfiguredQuery, ConfiguredQuestion, ModelCandidate};

impl<'a> ConfiguredQuery<'a> {
    pub(super) fn emitter(request: &'a ChoiceLookupRequest) -> Option<Self> {
        let [
            ChoiceSelection {
                value: ChoiceValue::Domain(domain),
            },
            dependencies @ ..,
        ] = request.dependencies()
        else {
            return None;
        };
        if let Some(kind) = request.target().emitter_reference_entity() {
            if !dependencies.is_empty() {
                return None;
            }
            return Some(Self {
                domain,
                question: ConfiguredQuestion::EmitterEntity(kind),
            });
        }
        let question = match request.target() {
            ChoiceTarget::EmitterIcebergCatalog if dependencies.is_empty() => {
                ConfiguredQuestion::EmitterCatalog
            }
            ChoiceTarget::EmitterCodec if dependencies.is_empty() => {
                ConfiguredQuestion::DirectedCodecs(CodecDirection::Encoding)
            }
            ChoiceTarget::EmitterCompatibleInputRelay => {
                let [
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
                ConfiguredQuestion::CompatibleRelays(RelayQuestion::EmitterInput {
                    first: RelayName::from(&relay.identifier),
                })
            }
            ChoiceTarget::SchemaField => {
                let [
                    ChoiceSelection {
                        value: ChoiceValue::Model(schema),
                    },
                ] = dependencies
                else {
                    return None;
                };
                if schema.kind != ModelKind::Schema {
                    return None;
                }
                ConfiguredQuestion::SchemaFields(schema.clone())
            }
            ChoiceTarget::EmitterMaterializedRelay | ChoiceTarget::EmitterErrorRelay => {
                if dependencies.is_empty() {
                    return None;
                }
                let mut inputs = Vec::with_capacity(dependencies.len());
                for dependency in dependencies {
                    let ChoiceValue::Model(input) = &dependency.value else {
                        return None;
                    };
                    if input.kind != ModelKind::Relay {
                        return None;
                    }
                    inputs.push(RelayName::from(&input.identifier));
                }
                if request.target() == ChoiceTarget::EmitterMaterializedRelay {
                    ConfiguredQuestion::CompatibleRelays(RelayQuestion::EmitterState { inputs })
                } else {
                    ConfiguredQuestion::CompatibleRelays(RelayQuestion::EmitterError { inputs })
                }
            }
            _ => return None,
        };
        Some(Self { domain, question })
    }
}

impl ConfiguredChoices {
    #[cfg_attr(
        nervix_lint,
        nervix::error_boundary(
            outcome,
            reason = "configured definition rendering projects to the typed choice status"
        )
    )]
    pub(super) fn emitter_candidate(
        &self,
        question: &ConfiguredQuestion,
        model: &Model<nervix_models::RequestedResourceVersion>,
    ) -> Result<Option<ModelCandidate>, ChoiceStatus> {
        let candidate = match (question, model) {
            (ConfiguredQuestion::EmitterEntity(kind), model)
                if model.connector_kind().is_some_and(|connector| {
                    kind.capabilities().reference.admits_client(connector)
                }) =>
            {
                ModelCandidate::new(
                    model.node_ref(),
                    kind.phrase().to_string(),
                    "Emitter client",
                    model.to_public_plexus(),
                )?
            }
            (ConfiguredQuestion::EmitterCatalog, model)
                if model.connector_kind().is_some_and(|connector| {
                    Some(connector) == Self::emitter_catalog_connector()
                }) =>
            {
                ModelCandidate::new(
                    model.node_ref(),
                    "Catalog client".to_string(),
                    "Emitter catalog",
                    model.to_public_plexus(),
                )?
            }
            _ => return Ok(None),
        };
        Ok(Some(candidate))
    }

    fn emitter_catalog_connector() -> Option<nervix_models::ConnectorKind> {
        let capabilities = EmitterEntityKind::Iceberg(IcebergEmitterEntity::Table).capabilities();
        let option = capabilities.option("CATALOG")?;
        match option.form {
            OptionForm::Value {
                value: OptionValue::Client(connector),
                ..
            } => Some(connector),
            _ => None,
        }
    }
}
