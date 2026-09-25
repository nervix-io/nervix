//! What a model must satisfy before the cluster is asked to store it.
//!
//! Layer: control plane.
//!
//! - **Owns.** Binding checks for changed models, planned domain UDFs, and the ONNX metadata an
//!   inferencer declares.
//! - **Depends on.** The registry's active graph, the resource store and the VM's type inference.
//! - **Must not know.** How a validated model is scheduled or executed.

use ahash::{HashMap, HashSet};
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_models::{
    CreateInferencer, CreateLookup, DomainName, DomainPace, InferencerName,
    InferencerTensorDimension, InferencerTensorSchema, InferencerTensorSchemaError, IngestorName,
    LookupName, Model, ModelKind, ResourceId, ResourceName, UdfName, VhostName, WasmProcessorName,
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

#[derive(Debug, Clone, Copy, strum::Display)]
#[strum(serialize_all = "lowercase")]
enum TensorDirection {
    Input,
    Output,
}

impl TensorDirection {
    fn binding_keyword(self) -> &'static str {
        match self {
            Self::Input => "INPUTS",
            Self::Output => "OUTPUT SCHEMA",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(in crate::application) enum ModelBindingValidationError {
    #[error(
        "paced domain '{domain}' requires ingestor '{node}' to declare TIMESTAMP NOW or TIMESTAMP \
         AT <field>"
    )]
    MissingIngestorTimestamp {
        domain: DomainName,
        node: IngestorName,
    },
    #[error("invalid TLS resource for VHOST '{node}' in domain '{}' from '{}@{}'", .resource.domain, .resource.identifier, .resource.version)]
    VhostTls {
        node: VhostName,
        resource: ResourceId,
    },
    #[error("invalid HASH MAP '{node}' in domain '{domain}'")]
    Lookup {
        domain: DomainName,
        node: LookupName,
    },
    #[error("INFERENCER '{node}' binding validation failed in domain '{domain}'")]
    Inferencer {
        domain: DomainName,
        node: InferencerName,
    },
    #[error("invalid WASM PROCESSOR '{node}' in domain '{domain}'")]
    WasmProcessor {
        domain: DomainName,
        node: WasmProcessorName,
    },
    #[error("invalid UDF bindings for {nodes:?}")]
    UdfPreparation { nodes: Vec<UdfName> },
}

#[derive(Debug, thiserror::Error)]
enum InferencerBindingValidationError {
    #[error("inferencer '{node}' has invalid tensor schemas: {source}")]
    TensorSchemas {
        node: InferencerName,
        source: InferencerTensorSchemaError,
    },
    #[error(
        "inferencer '{node}' cannot resolve model resource '{resource}@{version}' file '{file}'"
    )]
    ResolveModelFile {
        node: InferencerName,
        resource: ResourceName,
        version: u64,
        file: String,
    },
    #[error(
        "inferencer '{node}' model resource '{resource}@{version}' file '{file}' is unavailable"
    )]
    ModelFileUnavailable {
        node: InferencerName,
        resource: ResourceName,
        version: u64,
        file: String,
    },
    #[error("model file '{file}' for inferencer '{node}' must have .onnx extension")]
    InvalidExtension { node: InferencerName, file: String },
    #[error("timed out inspecting ONNX model '{file}' for inferencer '{node}'")]
    InspectionTimeout { node: InferencerName, file: String },
    #[error(
        "failed to inspect ONNX model '{file}' for inferencer '{node}': inspection task failed"
    )]
    InspectionTask { node: InferencerName, file: String },
    #[error("failed to inspect ONNX model '{file}' for inferencer '{node}': inspection panicked")]
    InspectionPanic { node: InferencerName, file: String },
    #[error("failed to initialize ONNX session builder for inferencer '{node}': {source}")]
    SessionBuilder {
        node: InferencerName,
        source: ort::Error,
    },
    #[error("failed to inspect ONNX model '{file}' for inferencer '{node}': {source}")]
    SessionModel {
        node: InferencerName,
        file: String,
        source: ort::Error,
    },
    #[error("inferencer '{node}' has duplicate {direction} binding for ONNX tensor '{tensor}'")]
    DuplicateTensor {
        node: InferencerName,
        direction: TensorDirection,
        tensor: String,
    },
    #[error("inferencer '{node}' missing ONNX {direction} tensor '{tensor}'")]
    MissingTensor {
        node: InferencerName,
        direction: TensorDirection,
        tensor: String,
    },
    #[error("inferencer '{node}' missing {} binding for ONNX {direction} tensor '{tensor}'", .direction.binding_keyword())]
    MissingBinding {
        node: InferencerName,
        direction: TensorDirection,
        tensor: String,
    },
    #[error(
        "inferencer '{node}' {direction} tensor '{tensor}' expected dense ONNX tensor, got \
         {actual}"
    )]
    NotDenseTensor {
        node: InferencerName,
        direction: TensorDirection,
        tensor: String,
        actual: ValueType,
    },
    #[error(
        "inferencer '{node}' {direction} tensor '{tensor}' has incompatible element type: ONNX \
         {actual} vs declared F32"
    )]
    ElementType {
        node: InferencerName,
        direction: TensorDirection,
        tensor: String,
        actual: TensorElementType,
    },
    #[error(
        "inferencer '{node}' {direction} tensor '{tensor}' has incompatible shape: ONNX \
         {actual:?} vs declared {declared:?}"
    )]
    Shape {
        node: InferencerName,
        direction: TensorDirection,
        tensor: String,
        actual: Vec<i64>,
        declared: Vec<InferencerTensorDimension>,
    },
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
    fn validate_binding_names(
        &self,
        processor: &CreateInferencer,
    ) -> error_stack::Result<(), InferencerBindingValidationError> {
        self.validate_direction_binding_names(
            processor,
            TensorDirection::Input,
            processor
                .inputs
                .iter()
                .map(|mapping| mapping.tensor.as_str()),
            &self.inputs,
        )?;
        self.validate_direction_binding_names(
            processor,
            TensorDirection::Output,
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
        direction: TensorDirection,
        tensors: impl IntoIterator<Item = &'a str>,
        model_tensors: &HashMap<String, OnnxTensorMetadata>,
    ) -> error_stack::Result<(), InferencerBindingValidationError> {
        let mut declared = HashSet::default();
        for tensor in tensors {
            if !declared.insert(tensor) {
                return Err(Report::new(
                    InferencerBindingValidationError::DuplicateTensor {
                        node: processor.name.clone(),
                        direction,
                        tensor: tensor.to_string(),
                    },
                ));
            }
            if !model_tensors.contains_key(tensor) {
                return Err(Report::new(
                    InferencerBindingValidationError::MissingTensor {
                        node: processor.name.clone(),
                        direction,
                        tensor: tensor.to_string(),
                    },
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
            return Err(Report::new(
                InferencerBindingValidationError::MissingBinding {
                    node: processor.name.clone(),
                    direction,
                    tensor: tensor.clone(),
                },
            ));
        }
        Ok(())
    }
}

impl OnnxTensorMetadata {
    fn validate_declared_schema(
        &self,
        processor: &CreateInferencer,
        direction: TensorDirection,
        tensor: &str,
        schema: &InferencerTensorSchema,
    ) -> error_stack::Result<(), InferencerBindingValidationError> {
        let ValueType::Tensor { ty, shape, .. } = &self.value_type else {
            return Err(Report::new(
                InferencerBindingValidationError::NotDenseTensor {
                    node: processor.name.clone(),
                    direction,
                    tensor: tensor.to_string(),
                    actual: self.value_type.clone(),
                },
            ));
        };
        if *ty != TensorElementType::Float32 {
            return Err(Report::new(InferencerBindingValidationError::ElementType {
                node: processor.name.clone(),
                direction,
                tensor: tensor.to_string(),
                actual: *ty,
            }));
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
            return Err(Report::new(InferencerBindingValidationError::Shape {
                node: processor.name.clone(),
                direction,
                tensor: tensor.to_string(),
                actual: shape.iter().copied().collect(),
                declared: schema.dimensions.clone(),
            }));
        }
        Ok(())
    }
}

impl SessionServiceImpl {
    /// Validates the bindings a planned batch would activate: everything that has to reach outside
    /// the registry (domain pace, resource storage, ONNX metadata, WASM module compilation) and
    /// therefore cannot live in `DomainState::build`, which follower synchronization and startup
    /// replay also run.
    ///
    /// This runs over the candidate models the batch produces rather than over the statements that
    /// produced them, so `CREATE` and every present and future `ALTER` share one boundary.
    pub(in crate::application) async fn validate_changed_model_bindings(
        &self,
        domain: &DomainName,
        pace: DomainPace,
        planned: &crate::registry::PlannedMutations,
    ) -> error_stack::Result<(), ModelBindingValidationError> {
        for model in planned.changed_models() {
            tokio::task::consume_budget().await;
            match model {
                Model::Ingestor(ingestor) => {
                    if pace.is_paced() && ingestor.timestamp_source.is_none() {
                        return Err(Report::new(
                            ModelBindingValidationError::MissingIngestorTimestamp {
                                domain: domain.clone(),
                                node: ingestor.name.clone(),
                            },
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
                            .change_context(ModelBindingValidationError::VhostTls {
                                node: vhost.name.clone(),
                                resource: id,
                            })?;
                    }
                }
                Model::Lookup(lookup) => {
                    self.validate_lookup_binding(domain, lookup)
                        .await
                        .change_context(ModelBindingValidationError::Lookup {
                            domain: domain.clone(),
                            node: lookup.name.clone(),
                        })?;
                }
                Model::Inferencer(processor) => {
                    self.validate_inferencer_binding(domain, processor)
                        .await
                        .change_context(ModelBindingValidationError::Inferencer {
                            domain: domain.clone(),
                            node: processor.name.clone(),
                        })?;
                }
                Model::WasmProcessor(processor) => {
                    // Activating a changed module binding also starts a new guest-state lifetime,
                    // so a module that cannot be compiled has to reject the batch here, while the
                    // previous binding and its saved state are still the current ones.
                    self.inner
                        .runtime
                        .prepare_candidate_wasm_module(domain, processor)
                        .await
                        .change_context(ModelBindingValidationError::WasmProcessor {
                            domain: domain.clone(),
                            node: processor.name.clone(),
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
    ) -> error_stack::Result<Option<crate::runtime::CompiledDomainUdfs>, ModelBindingValidationError>
    {
        let changed_udfs = planned
            .changed_models()
            .into_iter()
            .filter_map(|model| match model {
                Model::Udf(udf) => Some(udf.name.clone()),
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
        let prepared = self
            .inner
            .runtime
            .prepare_domain_udfs(domain_udfs)
            .await
            .change_context(ModelBindingValidationError::UdfPreparation {
                nodes: changed_udfs,
            })?;
        Ok(Some(prepared))
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
    ) -> error_stack::Result<(), InferencerBindingValidationError> {
        processor.execution_mode().map_err(|source| {
            Report::new(InferencerBindingValidationError::TensorSchemas {
                node: processor.name.clone(),
                source,
            })
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
            .change_context(InferencerBindingValidationError::ResolveModelFile {
                node: processor.name.clone(),
                resource: processor.resource.clone(),
                version: processor.resource_version,
                file: processor.file.clone(),
            })?;
        ensure_file_exists(&path, "ONNX model")
            .await
            .change_context(InferencerBindingValidationError::ModelFileUnavailable {
                node: processor.name.clone(),
                resource: processor.resource.clone(),
                version: processor.resource_version,
                file: processor.file.clone(),
            })?;
        if path.extension().and_then(|extension| extension.to_str()) != Some("onnx") {
            return Err(Report::new(
                InferencerBindingValidationError::InvalidExtension {
                    node: processor.name.clone(),
                    file: processor.file.clone(),
                },
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
    ) -> error_stack::Result<(), InferencerBindingValidationError> {
        let path = path.to_path_buf();
        let processor_name = processor.name.clone();
        let processor_file = processor.file.clone();
        let model_metadata = tokio::time::timeout(
            Duration::from_secs(30),
            tokio::task::spawn_blocking(move || {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    Self::inspect_onnx_model_metadata(&processor_name, &processor_file, &path)
                }))
                .map_err(|_| {
                    Report::new(InferencerBindingValidationError::InspectionPanic {
                        node: processor_name.clone(),
                        file: processor_file.clone(),
                    })
                })?
            }),
        )
        .await
        .map_err(|_| {
            Report::new(InferencerBindingValidationError::InspectionTimeout {
                node: processor.name.clone(),
                file: processor.file.clone(),
            })
        })?
        .change_context(InferencerBindingValidationError::InspectionTask {
            node: processor.name.clone(),
            file: processor.file.clone(),
        })??;

        model_metadata.validate_binding_names(processor)?;

        for mapping in &processor.inputs {
            let model_type = model_metadata.inputs.get(&mapping.tensor).verified(
                "validate_binding_names above rejected any mapping this metadata does not carry",
            );
            model_type.validate_declared_schema(
                processor,
                TensorDirection::Input,
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
                TensorDirection::Output,
                &declaration.tensor,
                &declaration.schema,
            )?;
        }

        Ok(())
    }

    fn inspect_onnx_model_metadata(
        processor_name: &InferencerName,
        processor_file: &str,
        path: &std::path::Path,
    ) -> error_stack::Result<OnnxModelMetadata, InferencerBindingValidationError> {
        let mut builder = Session::builder().map_err(|source| {
            Report::new(InferencerBindingValidationError::SessionBuilder {
                node: processor_name.clone(),
                source,
            })
        })?;
        let session = builder.commit_from_file(path).map_err(|source| {
            Report::new(InferencerBindingValidationError::SessionModel {
                node: processor_name.clone(),
                file: processor_file.to_string(),
                source,
            })
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

#[cfg(test)]
mod tests {
    use nervix_models::{
        AckMode, BranchSelection, InferencerTensorElementType, InferencerTensorRepresentation,
        ProcessorInputs, ProcessorOutputs,
    };
    use nonzero_ext::nonzero;

    use super::{super::test_fixtures::named, *};

    fn inferencer() -> CreateInferencer {
        CreateInferencer {
            name: named("scoring"),
            from: ProcessorInputs::new(Vec::new(), Vec::new()),
            output_routes: ProcessorOutputs::new(Vec::new()),
            branched_by: BranchSelection::unbranched(),
            resource: named("model"),
            resource_version: 1,
            file: "model.onnx".to_string(),
            inputs: Vec::new(),
            output_schema: Vec::new(),
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        }
    }

    #[test]
    fn tensor_validation_preserves_the_node_direction_and_tensor_contract() {
        let processor = inferencer();
        let schema = InferencerTensorSchema {
            representation: InferencerTensorRepresentation::Dense,
            element_type: InferencerTensorElementType::F32,
            dimensions: vec![InferencerTensorDimension::Fixed(nonzero!(2u32))],
        };
        let dense = ValueType::Tensor {
            ty: TensorElementType::Float32,
            shape: vec![2i64].into(),
            dimension_symbols: [String::new()].into_iter().collect(),
        };
        let sequence = OnnxTensorMetadata {
            value_type: ValueType::Sequence(Box::new(dense.clone())),
        };
        let error = sequence
            .validate_declared_schema(&processor, TensorDirection::Input, "features", &schema)
            .expect_err("a sequence is not a dense tensor");
        assert!(
            matches!(error.current_context(), InferencerBindingValidationError::NotDenseTensor {
            node, direction: TensorDirection::Input, tensor, actual: ValueType::Sequence(_),
        } if node == &processor.name && tensor == "features")
        );

        let metadata = OnnxModelMetadata {
            inputs: HashMap::from_iter([(
                "features".to_string(),
                OnnxTensorMetadata { value_type: dense },
            )]),
            outputs: HashMap::default(),
        };
        let error = metadata
            .validate_direction_binding_names(
                &processor,
                TensorDirection::Input,
                ["features", "features"],
                &metadata.inputs,
            )
            .expect_err("a tensor cannot be mapped twice");
        assert!(
            matches!(error.current_context(), InferencerBindingValidationError::DuplicateTensor {
            node, direction: TensorDirection::Input, tensor,
        } if node == &processor.name && tensor == "features")
        );
        let error = metadata
            .validate_direction_binding_names(
                &processor,
                TensorDirection::Input,
                ["missing"],
                &metadata.inputs,
            )
            .expect_err("a binding must name a model tensor");
        assert!(
            matches!(error.current_context(), InferencerBindingValidationError::MissingTensor { tensor, .. } if tensor == "missing")
        );
        let error = metadata
            .validate_direction_binding_names(
                &processor,
                TensorDirection::Output,
                [],
                &metadata.inputs,
            )
            .expect_err("every model tensor needs a binding");
        assert!(
            matches!(error.current_context(), InferencerBindingValidationError::MissingBinding { direction: TensorDirection::Output, tensor, .. } if tensor == "features")
        );
    }

    #[test]
    fn unusable_onnx_content_preserves_the_inspection_cause() {
        let directory = tempfile::tempdir().expect("the fixture directory is created");
        let path = directory.path().join("model.onnx");
        std::fs::write(&path, b"not an ONNX model").expect("the fixture is written");
        let processor = inferencer();
        let error = SessionServiceImpl::inspect_onnx_model_metadata(
            &processor.name,
            &processor.file,
            &path,
        )
        .err()
        .expect("malformed ONNX content must fail inspection");
        assert!(
            matches!(error.current_context(), InferencerBindingValidationError::SessionModel { node, file, .. } if node == &processor.name && file == "model.onnx")
        );
    }
}
