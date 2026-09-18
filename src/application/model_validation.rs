//! What a model must satisfy before the cluster is asked to store it.
//!
//! Layer: control plane.
//!
//! - **Owns.** Binding checks for changed models, planned domain UDFs, and the ONNX metadata an
//!   inferencer declares.
//! - **Depends on.** The registry's active graph, the resource store and the VM's type inference.
//! - **Must not know.** How a validated model is scheduled or executed.

use ahash::{HashMap, HashSet};
use error_stack::ResultExt as _;
use meticulous::OptionExt as _;
use nervix_models::{
    CreateInferencer, CreateLookup, DomainName, DomainPace, InferencerTensorDimension,
    InferencerTensorSchema, LookupName, Model, ModelKind, ResourceId, ResourceName,
};
use ort::{
    session::Session,
    value::{TensorElementType, ValueType},
};
use tokio::time::Duration;

use super::{
    session_service::SessionServiceImpl,
    tls::{ensure_file_exists, load_vhost_tls_materials},
};

struct OnnxModelMetadata {
    inputs: HashMap<String, OnnxTensorMetadata>,
    outputs: HashMap<String, OnnxTensorMetadata>,
}

struct OnnxTensorMetadata {
    value_type: ValueType,
}

#[derive(Debug, thiserror::Error)]
enum LookupBindingValidationError {
    #[error(
        "failed to resolve path '{path}' in resource '{resource}@{version}' for HASH MAP \
         '{lookup}' in domain '{domain}'"
    )]
    ResolveContentPath {
        domain: DomainName,
        lookup: LookupName,
        resource: ResourceName,
        version: u64,
        path: String,
    },
    #[error(
        "resource file '{path}' in '{resource}@{version}' for HASH MAP '{lookup}' in domain \
         '{domain}' is unavailable"
    )]
    FileUnavailable {
        domain: DomainName,
        lookup: LookupName,
        resource: ResourceName,
        version: u64,
        path: String,
    },
}

impl OnnxModelMetadata {
    fn validate_binding_names(&self, processor: &CreateInferencer) -> Result<(), String> {
        self.validate_direction_binding_names(
            processor,
            "input",
            processor
                .inputs
                .iter()
                .map(|mapping| mapping.tensor.as_str()),
            &self.inputs,
        )?;
        self.validate_direction_binding_names(
            processor,
            "output",
            processor
                .output_schema
                .iter()
                .map(|declaration| declaration.tensor.as_str()),
            &self.outputs,
        )
    }

    fn validate_direction_binding_names<'a>(
        &self,
        processor: &CreateInferencer,
        direction: &str,
        tensors: impl IntoIterator<Item = &'a str>,
        model_tensors: &HashMap<String, OnnxTensorMetadata>,
    ) -> Result<(), String> {
        let mut declared = HashSet::default();
        for tensor in tensors {
            if !declared.insert(tensor) {
                return Err(format!(
                    "inferencer '{}' has duplicate {} binding for ONNX tensor '{}'",
                    processor.name.as_str(),
                    direction,
                    tensor
                ));
            }
            if !model_tensors.contains_key(tensor) {
                return Err(format!(
                    "inferencer '{}' missing ONNX {} tensor '{}'",
                    processor.name.as_str(),
                    direction,
                    tensor
                ));
            }
        }
        let mut missing_bindings = model_tensors
            .keys()
            .filter(|tensor| !declared.contains(tensor.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        missing_bindings.sort();
        if let Some(tensor) = missing_bindings.first() {
            return Err(format!(
                "inferencer '{}' missing {} binding for ONNX {} tensor '{}'",
                processor.name.as_str(),
                if direction == "input" {
                    "INPUTS"
                } else {
                    "OUTPUT SCHEMA"
                },
                direction,
                tensor
            ));
        }
        Ok(())
    }
}

impl OnnxTensorMetadata {
    fn validate_declared_schema(
        &self,
        processor: &CreateInferencer,
        direction: &str,
        tensor: &str,
        schema: &InferencerTensorSchema,
    ) -> Result<(), String> {
        let ValueType::Tensor { ty, shape, .. } = &self.value_type else {
            return Err(format!(
                "inferencer '{}' {} tensor '{}' expected dense ONNX tensor, got {}",
                processor.name.as_str(),
                direction,
                tensor,
                self.value_type
            ));
        };
        if *ty != TensorElementType::Float32 {
            return Err(format!(
                "inferencer '{}' {} tensor '{}' has incompatible element type: ONNX {} vs \
                 declared F32",
                processor.name.as_str(),
                direction,
                tensor,
                ty
            ));
        }
        let incompatible_shape = shape.len() != schema.dimensions.len()
            || shape
                .iter()
                .zip(&schema.dimensions)
                .any(|(actual, declared)| match declared {
                    InferencerTensorDimension::Fixed(declared) => {
                        *actual >= 0 && *actual != i64::from(declared.get())
                    }
                    InferencerTensorDimension::Dynamic => *actual >= 0,
                    InferencerTensorDimension::Batch => *actual >= 0,
                });
        if incompatible_shape {
            return Err(format!(
                "inferencer '{}' {} tensor '{}' has incompatible shape: ONNX {:?} vs declared {:?}",
                processor.name.as_str(),
                direction,
                tensor,
                shape.as_ref(),
                schema.dimensions
            ));
        }
        Ok(())
    }
}

impl SessionServiceImpl {
    /// Validates the bindings a planned batch would activate: everything that has to reach outside
    /// the registry (domain pace, resource storage, ONNX metadata) and therefore cannot live in
    /// `DomainState::build`, which follower synchronization and startup replay also run.
    ///
    /// This runs over the candidate models the batch produces rather than over the statements that
    /// produced them, so `CREATE` and every present and future `ALTER` share one boundary.
    pub(in crate::application) async fn validate_changed_model_bindings(
        &self,
        domain: &DomainName,
        pace: DomainPace,
        planned: &crate::registry::PlannedMutations,
    ) -> Result<(), String> {
        for model in planned.changed_models() {
            tokio::task::consume_budget().await;
            match model {
                Model::Ingestor(ingestor) => {
                    if pace.is_paced() && ingestor.timestamp_source.is_none() {
                        return Err(format!(
                            "paced domain '{}' requires ingestor '{}' to declare TIMESTAMP NOW or \
                             TIMESTAMP AT <field>",
                            domain.as_str(),
                            ingestor.name.as_str()
                        ));
                    }
                }
                Model::Vhost(vhost) => {
                    // Planning already resolved the pinned version against the completed versions
                    // of the domain; this proves that version's material loads.
                    if let Some(tls) = vhost.tls.as_ref() {
                        let id = ResourceId::new(domain.clone(), tls.resource.clone(), tls.version);
                        load_vhost_tls_materials(&self.inner.resource_store, &id)
                            .await
                            .map_err(|error| {
                                format!(
                                    "invalid TLS resource for VHOST '{}': {error:#}",
                                    vhost.name.as_str()
                                )
                            })?;
                    }
                }
                Model::Lookup(lookup) => {
                    self.validate_lookup_binding(domain, lookup)
                        .await
                        .map_err(|error| {
                            format!("invalid HASH MAP '{}': {error}", lookup.name.as_str())
                        })?;
                }
                Model::Inferencer(processor) => {
                    self.validate_inferencer_binding(domain, processor)
                        .await
                        .map_err(|error| {
                            format!("invalid INFERENCER '{}': {error}", processor.name.as_str())
                        })?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Compiles the UDF family the batch would leave active. A UDF body is only usable once every
    /// sibling it may call compiles with it, so this takes the whole candidate set rather than the
    /// changed members.
    pub(in crate::application) async fn prepare_planned_domain_udfs(
        &self,
        planned: &crate::registry::PlannedMutations,
    ) -> Result<Option<crate::runtime::CompiledDomainUdfs>, String> {
        let changed_udfs = planned
            .changed_models()
            .into_iter()
            .filter_map(|model| match model {
                Model::Udf(udf) => Some(udf.name.as_str().to_string()),
                _ => None,
            })
            .collect::<Vec<_>>();
        if changed_udfs.is_empty() {
            return Ok(None);
        }
        let domain_udfs = planned
            .candidate_models_of_kind(ModelKind::Udf)
            .into_iter()
            .filter_map(|model| match model {
                Model::Udf(udf) => Some(udf.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        self.inner
            .runtime
            .prepare_domain_udfs(domain_udfs)
            .await
            .map(Some)
            .map_err(|error| format!("invalid UDF '{}': {error}", changed_udfs.join(", ")))
    }

    async fn validate_lookup_binding(
        &self,
        domain: &DomainName,
        lookup: &CreateLookup,
    ) -> error_stack::Result<(), LookupBindingValidationError> {
        let id = ResourceId::new(
            domain.clone(),
            lookup.resource.clone(),
            lookup.resource_version,
        );
        let path = self
            .inner
            .resource_store
            .resolve_content_path(&id, &lookup.path)
            .change_context(LookupBindingValidationError::ResolveContentPath {
                domain: domain.clone(),
                lookup: lookup.name.clone(),
                resource: lookup.resource.clone(),
                version: lookup.resource_version,
                path: lookup.path.clone(),
            })?;
        ensure_file_exists(&path, "lookup").await.change_context(
            LookupBindingValidationError::FileUnavailable {
                domain: domain.clone(),
                lookup: lookup.name.clone(),
                resource: lookup.resource.clone(),
                version: lookup.resource_version,
                path: lookup.path.clone(),
            },
        )
    }

    async fn validate_inferencer_binding(
        &self,
        domain: &DomainName,
        processor: &CreateInferencer,
    ) -> Result<(), String> {
        processor.execution_mode().map_err(|error| {
            format!(
                "inferencer '{}' has invalid tensor schemas: {}",
                processor.name.as_str(),
                error
            )
        })?;
        let id = ResourceId::new(
            domain.clone(),
            processor.resource.clone(),
            processor.resource_version,
        );
        let path = self
            .inner
            .resource_store
            .resolve_content_path(&id, &processor.file)
            .map_err(|error| error.to_string())?;
        ensure_file_exists(&path, "ONNX model")
            .await
            .map_err(|error| error.to_string())?;
        if path.extension().and_then(|extension| extension.to_str()) != Some("onnx") {
            return Err(format!(
                "model file '{}' must have .onnx extension",
                processor.file
            ));
        }
        self.validate_inferencer_model_metadata(processor, &path)
            .await?;
        Ok(())
    }

    async fn validate_inferencer_model_metadata(
        &self,
        processor: &CreateInferencer,
        path: &std::path::Path,
    ) -> Result<(), String> {
        let path = path.to_path_buf();
        let processor_name = processor.name.as_str().to_string();
        let processor_file = processor.file.clone();
        let model_metadata = tokio::time::timeout(
            Duration::from_secs(30),
            tokio::task::spawn_blocking(move || {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    Self::inspect_onnx_model_metadata(&processor_name, &processor_file, &path)
                }))
                .map_err(|panic| {
                    let reason = if let Some(message) = panic.downcast_ref::<&str>() {
                        *message
                    } else if let Some(message) = panic.downcast_ref::<String>() {
                        message.as_str()
                    } else {
                        "unknown panic"
                    };
                    format!(
                        "failed to inspect ONNX model '{}' for inferencer '{}': {}",
                        processor_file, processor_name, reason
                    )
                })?
            }),
        )
        .await
        .map_err(|_| {
            format!(
                "timed out inspecting ONNX model '{}' for inferencer '{}'",
                processor.file,
                processor.name.as_str()
            )
        })?
        .map_err(|error| {
            format!(
                "failed to inspect ONNX model '{}' for inferencer '{}': {}",
                processor.file,
                processor.name.as_str(),
                error
            )
        })??;

        model_metadata.validate_binding_names(processor)?;

        for mapping in &processor.inputs {
            let model_type = model_metadata.inputs.get(&mapping.tensor).verified(
                "validate_binding_names above rejected any mapping this metadata does not carry",
            );
            model_type.validate_declared_schema(
                processor,
                "input",
                &mapping.tensor,
                &mapping.schema,
            )?;
        }

        for declaration in &processor.output_schema {
            let model_type = model_metadata.outputs.get(&declaration.tensor).verified(
                "validate_binding_names above rejected any mapping this metadata does not carry",
            );
            model_type.validate_declared_schema(
                processor,
                "output",
                &declaration.tensor,
                &declaration.schema,
            )?;
        }

        Ok(())
    }

    fn inspect_onnx_model_metadata(
        processor_name: &str,
        processor_file: &str,
        path: &std::path::Path,
    ) -> Result<OnnxModelMetadata, String> {
        let mut builder = Session::builder().map_err(|error| {
            format!(
                "failed to initialize ONNX session builder for inferencer '{}': {}",
                processor_name, error
            )
        })?;
        let session = builder.commit_from_file(path).map_err(|error| {
            format!(
                "failed to inspect ONNX model '{}' for inferencer '{}': {}",
                processor_file, processor_name, error
            )
        })?;
        let model_inputs = session
            .inputs()
            .iter()
            .map(|input| {
                (
                    input.name().to_string(),
                    OnnxTensorMetadata {
                        value_type: input.dtype().clone(),
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        let model_outputs = session
            .outputs()
            .iter()
            .map(|output| {
                (
                    output.name().to_string(),
                    OnnxTensorMetadata {
                        value_type: output.dtype().clone(),
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        Ok(OnnxModelMetadata {
            inputs: model_inputs,
            outputs: model_outputs,
        })
    }
}
