//! Semantic choices for visual emitter construction.
//!
//! Layer: edges.
//! - **Owns.** Emitter reference, encoding, input and branch-compatible dependency questions.
//! - **Depends on.** The immutable configured-choice snapshot and vocabulary declarations.
//! - **Must not know.** Browser drafts, transport state or emitter execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns emitter structured choices")
)]

use nervix_client_wire::{
    ChoiceLookupRequest, ChoiceSelection, ChoiceStatus, ChoiceTarget, ChoiceValue,
};
use nervix_models::{
    CreateRelay, EmitterEntityKind, IcebergEmitterEntity, Model, ModelKind, OptionForm, OptionValue,
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
                ConfiguredQuestion::EmitterCodecs
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
                ConfiguredQuestion::EmitterCompatibleInputs(relay.clone())
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
                    inputs.push(input.clone());
                }
                if request.target() == ChoiceTarget::EmitterMaterializedRelay {
                    ConfiguredQuestion::EmitterMaterializedRelays(inputs)
                } else {
                    ConfiguredQuestion::EmitterErrorRelays(inputs)
                }
            }
            _ => return None,
        };
        Some(Self { domain, question })
    }
}

pub(super) struct EmitterChoiceContext<'a> {
    pub(super) inputs: Vec<&'a CreateRelay>,
    common_branch: bool,
}

impl ConfiguredChoices {
    #[cfg_attr(
        nervix_lint,
        nervix::error_boundary(
            outcome,
            reason = "missing configured references project to the typed choice status"
        )
    )]
    pub(super) fn emitter_context(
        &self,
        question: &ConfiguredQuestion,
    ) -> Result<EmitterChoiceContext<'_>, ChoiceStatus> {
        let references = match question {
            ConfiguredQuestion::EmitterCompatibleInputs(input) => std::slice::from_ref(input),
            ConfiguredQuestion::EmitterMaterializedRelays(inputs)
            | ConfiguredQuestion::EmitterErrorRelays(inputs) => inputs.as_slice(),
            _ => &[],
        };
        let mut inputs: Vec<&CreateRelay> = Vec::with_capacity(references.len());
        let mut common_branch = true;
        for reference in references {
            let Some(Model::Relay(input)) = self.models.get(reference) else {
                return Err(ChoiceStatus::MissingContext);
            };
            if let Some(first) = inputs.first()
                && first.branching != input.branching
            {
                common_branch = false;
            }
            inputs.push(input);
        }
        Ok(EmitterChoiceContext {
            inputs,
            common_branch,
        })
    }

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
        context: &EmitterChoiceContext<'_>,
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
            (ConfiguredQuestion::EmitterCodecs, Model::Codec(codec))
                if codec.wire_format.supports_encoding() =>
            {
                ModelCandidate::new(
                    model.node_ref(),
                    format!("encodes schema {}", codec.schema),
                    "Encoding codec",
                    model.to_canonical_plexus(),
                )?
            }
            (ConfiguredQuestion::EmitterCompatibleInputs(_), Model::Relay(relay))
                if context
                    .inputs
                    .first()
                    .is_some_and(|input| input.schema == relay.schema) =>
            {
                ModelCandidate::relay(relay)?
            }
            (ConfiguredQuestion::EmitterMaterializedRelays(_), Model::Relay(relay))
                if relay.materialized_state.is_some() && context.accepts_branch(relay) =>
            {
                ModelCandidate::relay(relay)?
            }
            (ConfiguredQuestion::EmitterErrorRelays(_), Model::Relay(relay))
                if context.accepts_branch(relay) =>
            {
                ModelCandidate::relay(relay)?
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

impl EmitterChoiceContext<'_> {
    fn accepts_branch(&self, candidate: &CreateRelay) -> bool {
        self.common_branch
            && self
                .inputs
                .first()
                .is_some_and(|input| input.branching == candidate.branching)
    }
}
