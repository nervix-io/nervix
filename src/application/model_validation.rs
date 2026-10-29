//! What a model must satisfy before the cluster is asked to store it.
//!
//! Layer: control plane.
//!
//! - **Owns.** Binding checks for changed models, planned domain UDFs, and the ONNX metadata an
//!   inferencer declares.
//! - **Depends on.** The registry's active graph, the resource store and the VM's type inference.
//! - **Must not know.** How a validated model is scheduled or executed.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::time::Duration;

use ahash::{HashMap, HashSet};
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{
    Cancellation, Cancelled, CpuClass, ExecutionError, MemoryClass, QueueAdmission,
};
use nervix_models::{
    CreateInferencer, CreateLookup, DomainName, DomainPace, InferencerName,
    InferencerTensorDimension, InferencerTensorSchema, IngestorName, LookupName, Model, ModelKind,
    ResourceId, ResourceName, UdfName, VhostName, WasmProcessorName,
};
use ort::{
    session::Session,
    value::{TensorElementType, ValueType},
};

use super::{
    session_service::SessionServiceImpl,
    tls::{ensure_file_exists, load_vhost_tls_materials},
};

mod client_config;
pub(in crate::application) use client_config::client_validation_message;

/// How long opening one ONNX model to read its tensor metadata may take before validation gives up.
const MODEL_INSPECTION_TIMEOUT: Duration = Duration::from_secs(30);

/// The bounded buffer used to read the configured native image sequentially. ONNX Runtime owns
/// its other allocations; this charge covers the image buffer and admits the bulk job.
const MODEL_INSPECTION_RESERVATION_BYTES: u64 = 16 * 1024;

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
    #[error("invalid client '{node}' in domain '{domain}'")]
    Client {
        domain: DomainName,
        node: nervix_models::ClientName,
    },
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
    #[error("inferencer '{node}' has invalid tensor schemas")]
    TensorSchemas { node: InferencerName },
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
    #[error("ONNX model inspection was cancelled for inferencer '{node}'")]
    InspectionCancelled { node: InferencerName },
    #[error(
        "failed to inspect ONNX model '{file}' for inferencer '{node}': the node's bounded \
         execution did not take the inspection"
    )]
    InspectionTask { node: InferencerName, file: String },
    #[error("failed to inspect ONNX model '{file}' for inferencer '{node}': inspection panicked")]
    InspectionPanic { node: InferencerName, file: String },
    #[error("failed to initialize ONNX session builder for inferencer '{node}'")]
    SessionBuilder { node: InferencerName },
    #[error("failed to inspect ONNX model '{file}' for inferencer '{node}'")]
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
        client_admission: QueueAdmission,
    ) -> error_stack::Result<(), ModelBindingValidationError> {
        for model in planned.changed_models() {
            nervix_primitives::task::consume_budget().await;
            if let Some(client) = model.configured_client() {
                self.validate_client_configuration(domain, client, client_admission)
                    .await?;
            }
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
                        .prepare_wasm_module(&crate::registry::WasmModulePlan::from_model(
                            domain, processor,
                        ))
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
                Model::Udf(udf) => Some(crate::registry::udf_program(udf)),
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
        processor.execution_mode().change_context(
            InferencerBindingValidationError::TensorSchemas {
                node: processor.name.clone(),
            },
        )?;
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
        let model_metadata = match nervix_primitives::time::timeout(
            MODEL_INSPECTION_TIMEOUT,
            self.inspect_inferencer_model_metadata(processor, path),
        )
        .await
        {
            Ok(metadata) => metadata?,
            Err(_) => {
                let snapshot = self.inner.runtime.executor().snapshot();
                tracing::debug!(
                    node = %processor.name,
                    model_file = %processor.file,
                    workers = snapshot.bulk_cpu.workers,
                    running = snapshot.bulk_cpu.running,
                    pending = snapshot.bulk_cpu.pending,
                    reserved_bytes = snapshot.bulk_memory.reserved_bytes,
                    capacity_bytes = snapshot.bulk_memory.capacity_bytes,
                    "ONNX metadata inspection deadline expired"
                );
                return Err(Report::new(
                    InferencerBindingValidationError::InspectionTimeout {
                        node: processor.name.clone(),
                        file: processor.file.clone(),
                    },
                ));
            }
        };

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

    async fn inspect_inferencer_model_metadata(
        &self,
        processor: &CreateInferencer,
        path: &std::path::Path,
    ) -> error_stack::Result<OnnxModelMetadata, InferencerBindingValidationError> {
        let path = path.to_path_buf();
        let processor_name = processor.name.clone();
        let processor_file = processor.file.clone();
        let executor = self.inner.runtime.executor();
        tracing::debug!(
            node = %processor.name,
            model_file = %processor.file,
            "ONNX metadata inspection waiting for bulk memory"
        );
        // Native initialization and the model's load, optimization and release stay on this
        // admitted worker. The reservation covers the bounded image buffer; ORT owns its heap.
        let reservation = executor
            .reserve(MemoryClass::Bulk, MODEL_INSPECTION_RESERVATION_BYTES)
            .await
            .change_context_lazy(|| InferencerBindingValidationError::InspectionTask {
                node: processor.name.clone(),
                file: processor.file.clone(),
            })?;
        tracing::debug!(
            node = %processor.name,
            model_file = %processor.file,
            "ONNX metadata inspection submitted to bulk execution"
        );
        match executor
            .run_cpu(CpuClass::Bulk, reservation, move |_charge, cancellation| {
                tracing::debug!(
                    node = %processor_name,
                    model_file = %processor_file,
                    "ONNX metadata inspection worker started"
                );
                let configured_image = std::env::var_os("ORT_DYLIB_PATH");
                tracing::debug!(
                    node = %processor_name,
                    model_file = %processor_file,
                    "ONNX metadata native image preparing"
                );
                Self::prepare_onnx_metadata_image(configured_image.as_deref(), cancellation)
                    .change_context_lazy(|| {
                        InferencerBindingValidationError::InspectionCancelled {
                            node: processor_name.clone(),
                        }
                    })?;
                tracing::debug!(
                    node = %processor_name,
                    model_file = %processor_file,
                    "ONNX metadata native image preparation ended"
                );
                Self::inspect_onnx_model_metadata(&processor_name, &processor_file, &path)
            })
            .await
        {
            Ok(metadata) => metadata,
            Err(error) => {
                let failure = match error.current_context() {
                    ExecutionError::JobPanicked { .. } => {
                        InferencerBindingValidationError::InspectionPanic {
                            node: processor.name.clone(),
                            file: processor.file.clone(),
                        }
                    }
                    ExecutionError::QueueFull { .. } | ExecutionError::PoolClosed { .. } => {
                        InferencerBindingValidationError::InspectionTask {
                            node: processor.name.clone(),
                            file: processor.file.clone(),
                        }
                    }
                };
                Err(error.change_context(failure))
            }
        }
    }

    /// Reading a shared library on demand follows native call order, which can fault unrelated
    /// code pages one at a time during global and kernel-registry initialization. Read an explicit
    /// image sequentially on the same admitted worker before entering that initialization.
    ///
    /// This is advisory I/O. The external loader still selects the image and owns load errors,
    /// including relative names and an image already loaded by another caller. No process-global
    /// initializer or cache is added here, and preparation stays inside the inspection's deadline.
    fn prepare_onnx_metadata_image(
        configured: Option<&std::ffi::OsStr>,
        cancellation: &Cancellation,
    ) -> Result<(), Cancelled> {
        cancellation.check()?;
        let Some(configured) = configured else {
            return cancellation.check();
        };
        let path = std::path::Path::new(configured);
        if !path.is_absolute() {
            return cancellation.check();
        }
        let mut image = match std::fs::File::open(path) {
            Ok(image) => image,
            Err(error) => {
                tracing::debug!(
                    ?error,
                    "ONNX native image read hint could not open its file"
                );
                return cancellation.check();
            }
        };
        if !matches!(image.metadata(), Ok(metadata) if metadata.is_file()) {
            return cancellation.check();
        }
        Self::read_onnx_metadata_image(&mut image, cancellation)?;
        Ok(())
    }

    fn read_onnx_metadata_image(
        image: &mut impl std::io::Read,
        cancellation: &Cancellation,
    ) -> Result<(), Cancelled> {
        cancellation.check()?;
        let mut buffer = vec![
            0;
            usize::try_from(MODEL_INSPECTION_RESERVATION_BYTES).assured(
                "the fixed inspection buffer fits every supported target"
            )
        ];
        loop {
            cancellation.check()?;
            match image.read(&mut buffer) {
                Ok(0) => break,
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => {
                    tracing::debug!(?error, "ONNX native image read hint ended before EOF");
                    break;
                }
            }
        }
        cancellation.check()
    }

    fn inspect_onnx_model_metadata(
        processor_name: &InferencerName,
        processor_file: &str,
        path: &std::path::Path,
    ) -> error_stack::Result<OnnxModelMetadata, InferencerBindingValidationError> {
        let session = Self::load_onnx_metadata_session(processor_name, processor_file, path)?;
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
        tracing::debug!(
            node = %processor_name,
            model_file = %processor_file,
            "ONNX metadata session releasing"
        );
        drop(session);
        tracing::debug!(
            node = %processor_name,
            model_file = %processor_file,
            "ONNX metadata session released"
        );
        Ok(OnnxModelMetadata {
            inputs: model_inputs,
            outputs: model_outputs,
        })
    }

    fn load_onnx_metadata_session(
        processor_name: &InferencerName,
        processor_file: &str,
        path: &std::path::Path,
    ) -> error_stack::Result<Session, InferencerBindingValidationError> {
        tracing::debug!(
            node = %processor_name,
            model_file = %processor_file,
            "ONNX metadata session builder initializing"
        );
        let environment = crate::runtime::onnx_environment().change_context_lazy(|| {
            InferencerBindingValidationError::SessionBuilder {
                node: processor_name.clone(),
            }
        })?;
        let builder = Session::builder(environment).change_context_lazy(|| {
            InferencerBindingValidationError::SessionBuilder {
                node: processor_name.clone(),
            }
        })?;
        // Reading metadata executes no inference. Keep optimization on the admitted bulk
        // worker too: ONNX's defaults create a physical-core-sized pool for every inspection.
        let builder = builder.with_intra_threads(1).map_err(|source| {
            Report::<ort::Error>::new(source.into()).change_context(
                InferencerBindingValidationError::SessionBuilder {
                    node: processor_name.clone(),
                },
            )
        })?;
        let mut builder = builder.with_inter_threads(1).map_err(|source| {
            Report::<ort::Error>::new(source.into()).change_context(
                InferencerBindingValidationError::SessionBuilder {
                    node: processor_name.clone(),
                },
            )
        })?;
        tracing::debug!(
            node = %processor_name,
            model_file = %processor_file,
            "ONNX metadata session loading"
        );
        let session = builder.commit_from_file(path).map_err(|source| {
            Report::new(InferencerBindingValidationError::SessionModel {
                node: processor_name.clone(),
                file: processor_file.to_string(),
                source,
            })
        })?;
        tracing::debug!(
            node = %processor_name,
            model_file = %processor_file,
            "ONNX metadata session loaded"
        );
        Ok(session)
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

    #[cfg(all(
        feature = "testing",
        feature = "deloxide",
        not(any(feature = "shuttle", feature = "loom"))
    ))]
    #[test]
    fn deloxide_onnx_metadata_inspection() {
        run_metadata_inspection(released_bulk_queue_model_preflights());
        metadata_image_reads_use_a_bounded_buffer_and_leave_load_errors_to_the_loader();
        cancelling_image_preparation_stops_before_the_next_read_and_releases_its_charge();
        image_preparation_preserves_the_external_loaders_path_and_error_ownership();
        a_model_inspection_bounds_its_wait_for_bulk_memory();
    }

    #[cfg(not(any(feature = "shuttle", feature = "loom")))]
    #[nervix_primitives::test]
    async fn metadata_image_reads_use_a_bounded_buffer_and_leave_load_errors_to_the_loader() {
        struct Image {
            bytes: std::io::Cursor<Vec<u8>>,
            requests: Vec<usize>,
            interrupted: bool,
            fail: bool,
        }
        impl std::io::Read for Image {
            fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
                self.requests.push(buffer.len());
                if !self.interrupted {
                    self.interrupted = true;
                    return Err(std::io::ErrorKind::Interrupted.into());
                }
                if self.fail {
                    return Err(std::io::ErrorKind::PermissionDenied.into());
                }
                std::io::Read::read(&mut self.bytes, buffer)
            }
        }
        let size = usize::try_from(MODEL_INSPECTION_RESERVATION_BYTES)
            .assured("the inspection buffer fits this target");
        for fail in [false, true] {
            let executor = nervix_execution::Executor::default();
            let charge = executor
                .try_reserve(MemoryClass::Bulk, MODEL_INSPECTION_RESERVATION_BYTES)
                .assured("one image buffer fits the bulk budget");
            let image = executor
                .run_cpu(CpuClass::Bulk, charge, move |_charge, cancellation| {
                    let mut image = Image {
                        bytes: std::io::Cursor::new(vec![9; size * 2 + 1]),
                        requests: Vec::new(),
                        interrupted: false,
                        fail,
                    };
                    SessionServiceImpl::read_onnx_metadata_image(&mut image, cancellation)
                        .assured("a read hint does not replace native loader error ownership");
                    image
                })
                .await
                .assured("the image read fits the bulk worker");
            assert!(!image.requests.is_empty());
            assert!(
                image
                    .requests
                    .iter()
                    .all(|request| *request > 0 && *request <= size)
            );
            assert_eq!(
                image.bytes.position(),
                if fail {
                    0
                } else {
                    (size * 2 + 1)
                        .try_into()
                        .assured("bounded fixture length fits u64")
                }
            );
            assert_eq!(executor.snapshot().bulk_memory.reserved_bytes, 0);
        }
    }

    #[cfg(not(any(feature = "shuttle", feature = "loom")))]
    #[nervix_primitives::test]
    async fn cancelling_image_preparation_stops_before_the_next_read_and_releases_its_charge() {
        use nervix_primitives::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
            blocking::mpsc,
            oneshot,
        };
        enum ReadState {
            Held {
                entered: oneshot::Sender<()>,
                release: mpsc::Receiver<()>,
            },
            Released,
        }
        struct Image {
            state: ReadState,
            reads: Arc<AtomicUsize>,
        }
        impl std::io::Read for Image {
            fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
                self.reads.fetch_add(1, Ordering::Relaxed);
                match std::mem::replace(&mut self.state, ReadState::Released) {
                    ReadState::Held { entered, release } => {
                        entered
                            .send(())
                            .assured("the cancelling caller awaits worker entry");
                        release
                            .recv()
                            .assured("the cancelling caller releases this read");
                    }
                    ReadState::Released => panic!("a cancelled preparation attempted another read"),
                }
                Ok(buffer.len())
            }
        }
        let executor = nervix_execution::Executor::default();
        let charge = executor
            .try_reserve(MemoryClass::Bulk, MODEL_INSPECTION_RESERVATION_BYTES)
            .assured("one image buffer fits the bulk budget");
        let reads = Arc::new(AtomicUsize::new(0));
        let image_reads = reads.clone();
        let (entered, reading) = oneshot::channel();
        let (release, released) = mpsc::channel();
        let (finished, cancellation) = oneshot::channel();
        let worker = executor.clone();
        let awaiting = nervix_primitives::task::spawn(async move {
            worker
                .run_cpu(CpuClass::Bulk, charge, move |_charge, signal| {
                    let mut image = Image {
                        state: ReadState::Held {
                            entered,
                            release: released,
                        },
                        reads: image_reads,
                    };
                    let outcome = SessionServiceImpl::read_onnx_metadata_image(&mut image, signal);
                    finished
                        .send(outcome)
                        .assured("the cancelling caller observes the worker's outcome");
                })
                .await
        });
        reading
            .await
            .assured("the preparation entered its first bounded read");
        assert_eq!(
            executor.snapshot().bulk_memory.reserved_bytes,
            MODEL_INSPECTION_RESERVATION_BYTES
        );
        awaiting.abort();
        assert!(
            awaiting
                .await
                .expect_err("the awaiting job was aborted")
                .is_cancelled()
        );
        assert_eq!(
            executor.snapshot().bulk_memory.reserved_bytes,
            MODEL_INSPECTION_RESERVATION_BYTES
        );
        release
            .send(())
            .assured("the admitted worker still owns its read");
        assert_eq!(
            cancellation
                .await
                .assured("the worker reported its cancellation"),
            Err(Cancelled)
        );
        assert_eq!(reads.load(Ordering::Relaxed), 1);
        nervix_primitives::time::timeout(Duration::from_secs(30), async {
            loop {
                nervix_primitives::task::consume_budget().await;
                let snapshot = executor.snapshot();
                if snapshot.bulk_cpu.completed == 1
                    && snapshot.bulk_cpu.running == 0
                    && snapshot.bulk_memory.reserved_bytes == 0
                {
                    break;
                }
                nervix_primitives::task::yield_now().await;
            }
        })
        .await
        .expect("the cancelled read returns its worker and charge");
        assert_eq!(executor.snapshot().bulk_cpu.running, 0);
        assert_eq!(executor.snapshot().bulk_cpu.pending, 0);
        assert_eq!(executor.snapshot().bulk_memory.reserved_bytes, 0);
    }

    #[cfg(all(feature = "testing", not(any(feature = "shuttle", feature = "loom"))))]
    #[nervix_primitives::test]
    async fn a_model_inspection_bounds_its_wait_for_bulk_memory() {
        let executor = crate::runtime::single_worker_executor();
        let crate::application::test_fixtures::TestService {
            service,
            registry: _registry,
            path,
        } = crate::application::test_fixtures::build_test_service_with_executor(
            false,
            executor.clone(),
        )
        .await;
        let before = executor.snapshot();
        let capacity = before.bulk_memory.capacity_bytes;
        let held = executor
            .try_reserve(MemoryClass::Bulk, capacity)
            .assured("the test occupies the untouched bulk budget");
        let processor = inferencer();
        let model = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/onnx/matrix_identity.onnx");
        let result = nervix_primitives::time::timeout(
            MODEL_INSPECTION_TIMEOUT + Duration::from_secs(2),
            service.validate_inferencer_model_metadata(&processor, &model),
        )
        .await
        .expect("the inspection's own deadline must bound memory admission");
        let failure = result.expect_err("the full memory budget cannot admit an inspection");
        assert!(
            matches!(failure.current_context(), InferencerBindingValidationError::InspectionTimeout { node, .. } if node == &processor.name),
            "{failure:?}"
        );
        assert_eq!(
            executor.snapshot().bulk_cpu.admitted,
            before.bulk_cpu.admitted
        );
        assert_eq!(executor.snapshot().bulk_cpu.pending, 0);
        assert_eq!(executor.snapshot().bulk_memory.reserved_bytes, capacity);
        drop(held);
        assert_eq!(executor.snapshot().bulk_memory.reserved_bytes, 0);
        let _ = std::fs::remove_dir_all(&path);
    }

    #[cfg(not(any(feature = "shuttle", feature = "loom")))]
    #[nervix_primitives::test]
    async fn image_preparation_preserves_the_external_loaders_path_and_error_ownership() {
        let directory = tempfile::tempdir().assured("the image hint fixture has a directory");
        let image = directory.path().join("native-image");
        std::fs::write(&image, vec![1; 20_000]).assured("the fixture writes its private image");
        for configured in [
            None,
            Some(std::path::PathBuf::from("native-image")),
            Some(directory.path().join("unavailable-image")),
            Some(directory.path().to_path_buf()),
            Some(image.clone()),
        ] {
            let executor = nervix_execution::Executor::default();
            let charge = executor
                .try_reserve(MemoryClass::Bulk, MODEL_INSPECTION_RESERVATION_BYTES)
                .assured("one preparation buffer fits the bulk budget");
            executor
                .run_cpu(CpuClass::Bulk, charge, move |_charge, cancellation| {
                    SessionServiceImpl::prepare_onnx_metadata_image(
                        configured.as_deref().map(std::path::Path::as_os_str),
                        cancellation,
                    )
                    .assured("the loader owns path selection and image load failures");
                })
                .await
                .assured("the preparation job completes");
            assert_eq!(executor.snapshot().bulk_memory.reserved_bytes, 0);
        }
    }

    #[cfg(all(feature = "testing", not(any(feature = "shuttle", feature = "loom"))))]
    fn run_metadata_inspection(inspection: impl Future<Output = ()>) {
        #[cfg(feature = "deloxide")]
        {
            let directory = std::env::var_os("NERVIX_DEADLOCK_EVIDENCE")
                .map(nervix_deadlock::EvidenceDirectory::new);
            nervix_deadlock::DiagnosticRun::start_selected(
                directory,
                nervix_primitives::deadlock::DiagnosticSelection::for_build(cfg!(
                    feature = "deloxide-order"
                )),
            )
            .assured("the isolated diagnostic process starts before its runtime and tracked locks");
        }
        let runtime = nervix_primitives::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .assured("the isolated inspection test can start its runtime");
        runtime.block_on(inspection);
    }

    #[cfg(all(feature = "testing", not(any(feature = "shuttle", feature = "loom"))))]
    fn isolated_metadata_inspection(scenario: &str, inspection: impl Future<Output = ()>) {
        run_metadata_inspection(async {
            const CHILD: &str = "NERVIX_ONNX_INSPECTION_SCENARIO";
            const DIRECTORY: &str = "NERVIX_ONNX_INSPECTION_DIRECTORY";
            if std::env::var(CHILD).as_deref() != Ok(scenario) {
                let executable = std::env::current_exe().assured("the test has an executable");
                let directory = tempfile::Builder::new()
                    .prefix("onnx-inspection-")
                    .tempdir_in(executable.parent().assured("the executable has a parent"))
                    .assured("the parent can retain the inspection's private evidence")
                    .keep();
                let stdout = directory.join("stdout.log");
                let mut command = tokio::process::Command::new(executable);
                command
                    .args(["--exact", scenario, "--test-threads=1", "--nocapture"])
                    .env(CHILD, scenario)
                    .env(DIRECTORY, &directory)
                    .env(
                        "RUST_LOG",
                        "info,nervix=info,registry=info,openraft::core::heartbeat::worker=error,\
                         openraft::replication=error,\
                         openraft::engine::handler::replication_handler=error,\
                         nervix_interconnect=debug,\
                         nervix_server::application::model_validation=debug",
                    )
                    .stdout(std::fs::File::create(&stdout).assured("the parent can retain stdout"))
                    .stderr(std::process::Stdio::inherit())
                    .kill_on_drop(true);
                eprintln!("inspection evidence: {}", directory.display());
                let mut child = command.spawn().assured("the isolated inspection can start");
                let status =
                    match nervix_primitives::time::timeout(Duration::from_secs(90), child.wait())
                        .await
                    {
                        Ok(status) => status.assured("the parent can observe its inspection child"),
                        Err(_) => {
                            child
                                .kill()
                                .await
                                .assured("the parent kills and reaps its owned child");
                            panic!(
                                "inspection supervision expired; evidence: {}",
                                directory.display()
                            );
                        }
                    };
                let trace = directory.join("inspection.log");
                match std::fs::read_to_string(&trace) {
                    Ok(trace) => eprint!("{trace}"),
                    Err(error) => {
                        eprintln!("inspection trace {} unavailable: {error}", trace.display())
                    }
                }
                assert!(
                    status.success(),
                    "inspection child {status}; evidence: {}",
                    directory.display()
                );
                let output =
                    std::fs::read_to_string(&stdout).assured("the parent retained child stdout");
                assert!(
                    output.contains("test result: ok. 1 passed; 0 failed"),
                    "the isolated inspection must execute: {output}"
                );
                return;
            }
            let directory = std::env::var_os(DIRECTORY)
                .assured("the parent supplies its child's evidence directory");
            let trace = std::path::PathBuf::from(directory).join("inspection.log");
            crate::application::tracing_setup::init_tracing_to_file(&trace)
                .assured("the child installs the production file subscriber");
            inspection.await;
        });
    }

    #[cfg(all(feature = "testing", not(any(feature = "shuttle", feature = "loom"))))]
    #[test]
    fn a_released_full_bulk_queue_admits_concurrent_fresh_model_preflights() {
        isolated_metadata_inspection(
            "application::model_validation::tests::a_released_full_bulk_queue_admits_concurrent_fresh_model_preflights",
            released_bulk_queue_model_preflights(),
        );
    }

    #[cfg(all(feature = "testing", not(any(feature = "shuttle", feature = "loom"))))]
    async fn released_bulk_queue_model_preflights() {
        let executor = nervix_execution::Executor::default();
        let crate::application::test_fixtures::TestService {
            service,
            registry: _registry,
            path,
        } = crate::application::test_fixtures::build_test_service_with_executor(
            false,
            executor.clone(),
        )
        .await;
        let matrix = InferencerTensorSchema {
            representation: InferencerTensorRepresentation::Dense,
            element_type: InferencerTensorElementType::F32,
            dimensions: vec![
                InferencerTensorDimension::Fixed(nonzero!(2u32)),
                InferencerTensorDimension::Fixed(nonzero!(3u32)),
            ],
        };
        let processor = CreateInferencer {
            file: "matrix_identity.onnx".to_string(),
            inputs: vec![nervix_models::InferencerTensorMapping {
                tensor: "matrix".to_string(),
                schema: matrix.clone(),
                expression: nervix_models::Expression::Literal(nervix_models::Literal::Null),
            }],
            output_schema: vec![nervix_models::InferencerTensorDeclaration {
                tensor: "transformed".to_string(),
                schema: matrix,
            }],
            ..inferencer()
        };
        let model = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/onnx/matrix_identity.onnx");
        let before = executor.snapshot().bulk_cpu;
        let mut releases = Vec::new();
        let mut holders = nervix_primitives::task::JoinSet::new();
        for _ in 0..before.workers {
            nervix_primitives::task::consume_budget().await;
            let worker = executor.clone();
            let reservation = worker
                .try_reserve(MemoryClass::Bulk, 0)
                .assured("occupancy requires no memory charge");
            let (entered, entry) = nervix_primitives::sync::oneshot::channel();
            let (release, held) = nervix_primitives::sync::blocking::mpsc::channel::<()>();
            holders.spawn(async move {
                worker
                    .run_cpu(CpuClass::Bulk, reservation, move |_, _| {
                        entered
                            .send(())
                            .assured("the test observes each occupied worker");
                        held.recv()
                            .expect_err("releasing occupancy disconnects its gate");
                    })
                    .await
                    .assured("each holder has one admitted bulk worker");
            });
            entry.await.assured("the holder reached its actual worker");
            releases.push(release);
        }
        let mut empty_jobs = nervix_primitives::task::JoinSet::new();
        for _ in 0..before.queue_capacity {
            nervix_primitives::task::consume_budget().await;
            let worker = executor.clone();
            let reservation = worker
                .try_reserve(MemoryClass::Bulk, 0)
                .assured("an empty queued job requires no memory charge");
            empty_jobs.spawn(async move {
                worker
                    .run_cpu(CpuClass::Bulk, reservation, |_, _| ())
                    .await
                    .assured("each empty job has one bounded queue place");
            });
        }
        while executor.snapshot().bulk_cpu.pending != before.queue_capacity {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::task::yield_now().await;
        }
        let refusal = service
            .validate_inferencer_model_metadata(&processor, &model)
            .await
            .expect_err("the full queue refuses an uninspected model");
        assert!(matches!(
            refusal.current_context(),
            InferencerBindingValidationError::InspectionTask { .. }
        ));
        assert!(matches!(
            refusal.downcast_ref::<ExecutionError>(),
            Some(ExecutionError::QueueFull { .. })
        ));
        drop(releases);
        let pending_limit = before
            .queue_capacity
            .checked_sub(4)
            .assured("the default queue holds four preflights");
        while executor.snapshot().bulk_cpu.pending > pending_limit {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::task::yield_now().await;
        }
        let mut preflights = nervix_primitives::task::JoinSet::new();
        for job in 0..4 {
            let service = service.clone();
            let processor = CreateInferencer {
                name: named(&format!("queued_matrix_{job}")),
                ..processor.clone()
            };
            let model = model.clone();
            preflights.spawn(async move {
                service
                    .validate_inferencer_model_metadata(&processor, &model)
                    .await
                    .assured("the released queue admits a matching model within its product bound");
            });
        }
        while let Some(preflight) = preflights.join_next().await {
            nervix_primitives::task::consume_budget().await;
            preflight.assured("the admitted production preflight did not panic");
        }
        while let Some(empty) = empty_jobs.join_next().await {
            nervix_primitives::task::consume_budget().await;
            empty.assured("the queued empty job completed");
        }
        while let Some(holder) = holders.join_next().await {
            nervix_primitives::task::consume_budget().await;
            holder.assured("the released holder completed");
        }
        let completed = before
            .workers
            .checked_add(before.queue_capacity)
            .and_then(|jobs| jobs.checked_add(4))
            .assured("the bounded default workers, queue and preflights fit usize");
        let completed =
            u64::try_from(completed).assured("the bounded job count fits the execution counters");
        let after = executor.snapshot();
        assert_eq!(after.bulk_cpu.admitted - before.admitted, completed);
        assert_eq!(after.bulk_cpu.completed - before.completed, completed);
        assert_eq!(after.bulk_cpu.refused - before.refused, 1);
        assert_eq!(after.bulk_cpu.running, 0);
        assert_eq!(after.bulk_cpu.pending, 0);
        assert_eq!(after.bulk_memory.reserved_bytes, 0);
        std::fs::remove_dir_all(path).assured("the completed service fixture can be removed");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn metadata_inspection_uses_only_its_admitted_worker() {
        const CHILD: &str = "NERVIX_ONNX_INSPECTION_THREAD_BOUND_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let executable =
                std::env::current_exe().assured("the test harness has a running executable");
            let output = std::process::Command::new(executable)
                .args([
                    "--exact",
                    "application::model_validation::tests::metadata_inspection_uses_only_its_admitted_worker",
                    "--test-threads=1",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .output()
                .assured("the current test executable starts its isolated owner regression");
            assert!(
                output.status.success(),
                "isolated ONNX thread bound failed: {}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                String::from_utf8_lossy(&output.stdout)
                    .contains("test result: ok. 1 passed; 0 failed"),
                "the isolated owner regression must actually execute: {}",
                String::from_utf8_lossy(&output.stdout)
            );
            return;
        }

        // A fresh process prevents another parallel test's runtime or database threads from
        // changing this count. Create the process's ONNX environment before the observation; no
        // session has been loaded and no ONNX operator pool has been created yet.
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(std::io::sink)
            .try_init()
            .assured("the isolated owner regression has installed no other tracing subscriber");
        crate::runtime::onnx_environment()
            .assured("ONNX Runtime creates the process's environment");
        let before = std::fs::read_dir("/proc/self/task")
            .assured("Linux exposes the current process's tasks")
            .count();
        let processor = inferencer();
        let model = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/onnx/batch_score.onnx");
        let session = SessionServiceImpl::load_onnx_metadata_session(
            &processor.name,
            &processor.file,
            &model,
        )
        .assured("the current ONNX fixture is valid");
        let during = std::fs::read_dir("/proc/self/task")
            .assured("Linux exposes the current process's tasks")
            .count();
        assert_eq!(session.inputs().len(), 2);
        assert_eq!(session.outputs().len(), 1);
        assert_eq!(
            during, before,
            "metadata inspection started CPU workers outside its admitted executor job"
        );
    }

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
        let InferencerBindingValidationError::SessionModel { source: cause, .. } =
            error.current_context()
        else {
            panic!("the inspection failure is the current context: {error:?}");
        };
        let cause = cause.to_string();
        let rendered = format!("{error:#}");
        assert!(
            rendered.starts_with(&format!(
                "failed to inspect ONNX model 'model.onnx' for inferencer '{}': {cause}",
                processor.name
            )),
            "{rendered}"
        );
        assert_eq!(rendered.matches(&cause).count(), 1, "{rendered}");
    }

    #[nervix_primitives::test]
    async fn a_model_is_inspected_on_the_bulk_workers() {
        let crate::application::test_fixtures::TestService {
            service,
            registry: _registry,
            path,
        } = crate::application::test_fixtures::build_test_service(false).await;
        let executor = service.inner.runtime.executor().clone();
        let matrix = InferencerTensorSchema {
            representation: InferencerTensorRepresentation::Dense,
            element_type: InferencerTensorElementType::F32,
            dimensions: vec![
                InferencerTensorDimension::Fixed(nonzero!(2u32)),
                InferencerTensorDimension::Fixed(nonzero!(3u32)),
            ],
        };
        let processor = CreateInferencer {
            inputs: vec![nervix_models::InferencerTensorMapping {
                tensor: "matrix".to_string(),
                schema: matrix.clone(),
                expression: nervix_models::Expression::Literal(nervix_models::Literal::Null),
            }],
            output_schema: vec![nervix_models::InferencerTensorDeclaration {
                tensor: "transformed".to_string(),
                schema: matrix,
            }],
            ..inferencer()
        };
        let model = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/onnx/matrix_identity.onnx");
        let before = executor.snapshot().bulk_cpu;

        service
            .validate_inferencer_model_metadata(&processor, &model)
            .await
            .expect("the identity model's tensors match the declared binding");

        let after = executor.snapshot().bulk_cpu;
        assert_eq!(after.admitted - before.admitted, 1);
        assert_eq!(after.completed - before.completed, 1);
        let _ = std::fs::remove_dir_all(&path);
    }

    #[cfg(feature = "testing")]
    #[nervix_primitives::test]
    async fn a_queued_model_inspection_times_out_and_releases_its_admission() {
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(std::io::sink)
            .finish();
        let _tracing = tracing::subscriber::set_default(subscriber);
        let executor = crate::runtime::single_worker_executor();
        let crate::application::test_fixtures::TestService {
            service,
            registry: _registry,
            path,
        } = crate::application::test_fixtures::build_test_service_with_executor(
            false,
            executor.clone(),
        )
        .await;
        let reservation = executor
            .try_reserve(MemoryClass::Bulk, 0)
            .assured("an empty occupying job requires no memory");
        let (entered, entry) = nervix_primitives::sync::oneshot::channel();
        let (release, held) = nervix_primitives::sync::blocking::mpsc::channel();
        let occupied_executor = executor.clone();
        let occupying = nervix_primitives::task::spawn(async move {
            occupied_executor
                .run_cpu(CpuClass::Bulk, reservation, move |_, _| {
                    entered
                        .send(())
                        .assured("the test retains its worker-entry receiver");
                    held.recv()
                        .assured("the test releases its occupied worker after the assertion");
                })
                .await
                .assured("the only occupying job fits the single-worker class");
        });
        entry
            .await
            .assured("the occupying job publishes its actual worker entry");
        let processor = inferencer();
        let model = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/onnx/matrix_identity.onnx");

        let failure = service
            .validate_inferencer_model_metadata(&processor, &model)
            .await
            .expect_err("the occupied worker cannot inspect a queued model within its bound");

        assert!(
            matches!(failure.current_context(), InferencerBindingValidationError::InspectionTimeout { node, .. } if node == &processor.name),
            "{failure:?}"
        );
        let timed_out = executor.snapshot();
        assert_eq!(timed_out.bulk_cpu.running, 1);
        assert_eq!(timed_out.bulk_cpu.pending, 0);
        assert_eq!(timed_out.bulk_memory.reserved_bytes, 0);
        release
            .send(())
            .assured("the occupied worker retains its release receiver");
        occupying
            .await
            .assured("the released occupying job completes");
        let _ = std::fs::remove_dir_all(&path);
    }

    #[cfg(feature = "testing")]
    #[nervix_primitives::test]
    async fn a_model_the_bulk_workers_cannot_take_now_is_refused_as_an_inspection_task() {
        let executor = crate::runtime::single_worker_executor();
        let crate::application::test_fixtures::TestService {
            service,
            registry: _registry,
            path,
        } = crate::application::test_fixtures::build_test_service_with_executor(
            false,
            executor.clone(),
        )
        .await;
        let processor = inferencer();
        let model = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/onnx/matrix_identity.onnx");
        let filled = crate::runtime::FilledCpuClass::fill(&executor, CpuClass::Bulk).await;

        let refused = service
            .validate_inferencer_model_metadata(&processor, &model)
            .await
            .expect_err("a full bulk class refuses the inspection");

        assert!(
            matches!(
                refused.current_context(),
                InferencerBindingValidationError::InspectionTask { node, .. }
                    if node == &processor.name
            ),
            "{refused:?}"
        );
        assert!(refused.contains::<ExecutionError>(), "{refused:?}");
        filled.release().await;
        let _ = std::fs::remove_dir_all(&path);
    }
}
