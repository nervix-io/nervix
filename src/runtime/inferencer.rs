//! ONNX inference at the data-plane boundary.
//!
//! Layer: data plane.
//! Owns: the one inference implementation every inferencer runs, with or without `STATE`: native
//! sessions, tensor conversion at the Arrow boundary, branch-owned explicit state, safe
//! diagnostics, and the startup probe of what the process's ONNX Runtime library reports about
//! itself.
//! Depends on: typed tensor plans, Arrow batches, the bounded executor and ONNX Runtime.
//! Must not know: semantic Models, graph validation or placement decisions.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "inferencer preparation resolves the retained compiled session before branch \
                  processing"
    )
)]

use std::path::PathBuf;

use arch_into::ArchInto as _;
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_execution::{Cancellation, CpuClass, Executor, MemoryClass};
use nervix_models::{
    CudaProviderRegistration, Detected, ExecutionProviders, InferencerTensorDimension,
    InferencerTensorSchema, OnnxRuntime, OnnxRuntimeDetection, PropertyText, UndetectedReason,
};
use nervix_primitives::{sync::StdArc, unmodeled::sync::OnceLock};
use ort::{environment::Environment, session::Session};

use crate::runtime_schema::RuntimeValue;

mod columns;
mod session;
mod stateful;
pub(super) use session::{InferenceRun, InferenceSession, MessageRefusal, PreparedInference};
pub(super) use stateful::{CarriedState, NativeGeneration};

/// Every way ONNX inference fails, from loading a model to reshaping one tensor axis. A model's
/// refusal keeps only ONNX Runtime's status: its native text can quote tensor values.
#[derive(Debug, thiserror::Error)]
pub(super) enum InferencerError {
    #[error("failed to initialize ONNX session")]
    InitializeSession,
    #[error("failed to load ONNX model '{}'", .path.display())]
    LoadModel { path: PathBuf },
    #[error("the node's bounded execution did not load the ONNX model")]
    LoadExecution,
    #[error("the node's bounded execution did not run the ONNX model")]
    RunExecution,
    #[error("the node's bounded execution did not prepare the INITIAL tensors")]
    InitialExecution,
    #[error("the caller stopped waiting between two ONNX model invocations")]
    Cancelled,
    #[error("the branch's ONNX session was not loaded")]
    SessionUnavailable,
    #[error("the ONNX model refused the batched call with status {status:?}")]
    Refused { status: ort::ErrorCode },
    #[error("model port '{port}' is unavailable")]
    Port { port: String },
    #[error("a batched model reads a STATE port")]
    BatchWithState,
    #[error("failed to prepare INITIAL tensors")]
    Initial,
    #[error("published state tensor {index} is unavailable or has an invalid type")]
    StateTensor { index: usize },
    #[error(
        "the declared state holds {bytes} bytes, exceeding one configured snapshot section \
         ({limit} bytes)"
    )]
    StateSize { bytes: u64, limit: u64 },
    #[error("the inference working set exceeds the configured relay operation limit")]
    WorkingSet,
    #[error("input tensor '{port}' is unavailable")]
    InputPort { port: String },
    #[error("input tensor '{port}' contains null values")]
    NullInput { port: String },
    #[error("input tensor '{port}' has an invalid Arrow type {actual:?}")]
    InputType {
        port: String,
        actual: arrow_schema::DataType,
    },
    #[error("input tensor '{port}' has an invalid shape")]
    InputShape { port: String },
    #[error("failed to build a native input tensor")]
    BuildInput,
    #[error("model output '{port}' has an invalid tensor type or shape")]
    OutputTensor { port: String },
    #[error("ONNX output tensor returned a negative shape {shape:?}")]
    NegativeOutputShape { shape: Vec<i64> },
    #[error("output tensor has an invalid Arrow type {actual:?}")]
    OutputType { actual: arrow_schema::DataType },
    #[error("output tensor size exceeds the supported size")]
    OutputSize,
    #[error("failed to build the output Arrow arrays")]
    OutputArray,
    #[error("failed to build the output Arrow batch")]
    OutputBatch,
    #[error("batched tensor schema has no dimensions")]
    BatchedSchemaWithoutDimensions,
    #[error("batched tensor schema has no BATCH axis")]
    MissingBatchAxis,
    #[error("tensor slice rank {actual} does not match declared rank {expected}")]
    SliceRankMismatch { actual: usize, expected: usize },
    #[error("tensor shape {shape:?} does not match declared dimensions {dimensions:?}")]
    ShapeDimensionCountMismatch {
        shape: Vec<usize>,
        dimensions: Vec<InferencerTensorDimension>,
    },
    #[error("tensor shape {shape:?} has dimension {actual}, expected {expected}")]
    ShapeDimensionMismatch {
        shape: Vec<usize>,
        actual: usize,
        expected: std::num::NonZeroU32,
    },
    #[error("tensor shape {shape:?} has batch dimension {actual}, expected {batch_size}")]
    ShapeBatchMismatch {
        shape: Vec<usize>,
        actual: usize,
        batch_size: usize,
    },
    #[error("batched tensor outer element count overflowed")]
    OuterCountOverflow,
    #[error("batched tensor inner element count overflowed")]
    InnerCountOverflow,
    #[error("batched tensor slice element count overflowed")]
    SliceCountOverflow,
    #[error("batched tensor slice contains {actual} values, expected {expected}")]
    SliceValueCount { actual: usize, expected: usize },
    #[error("batched tensor joined element count overflowed")]
    JoinedCountOverflow,
    #[error("batched tensor holds {actual} values, expected {expected}")]
    BatchedValueCount { actual: usize, expected: usize },
    #[error("tensor element requires F32, got {value:?}")]
    ElementRequiresF32 { value: RuntimeValue },
    #[error("tensor ARRAY axis contains {actual} values, expected {expected}")]
    FixedAxisLength {
        actual: usize,
        expected: std::num::NonZeroU32,
    },
    #[error("fixed tensor axis requires ARRAY, got {value:?}")]
    FixedAxisRequiresArray { value: RuntimeValue },
    #[error("dynamic tensor axis requires VEC, got {value:?}")]
    DynamicAxisRequiresVec { value: RuntimeValue },
    #[error("dense tensor is ragged: child shapes {expected:?} and {actual:?} differ")]
    RaggedChildShapes {
        expected: Vec<usize>,
        actual: Vec<usize>,
    },
    #[error("cannot infer an inner DYNAMIC axis from an empty outer vector")]
    InnerDynamicFromEmpty,
    #[error("tensor contains {actual} values, expected {expected} for shape {shape:?}")]
    ValueCountForShape {
        actual: usize,
        expected: usize,
        shape: Vec<usize>,
    },
}

/// The process's ONNX Runtime environment, or ONNX Runtime's refusal to create it.
///
/// ONNX Runtime creates every session in an environment and allows a process one, refusing another
/// even after the first is dropped. Every node the process runs and every model inspection share
/// this one, created on first use and kept until the process exits, so its outcome is final. A
/// panic while it is created, such as a native library that does not load, leaves no outcome, and
/// the next use tries again.
static ONNX_ENVIRONMENT: OnceLock<Result<Environment, ort::Error>> = OnceLock::new();

/// The environment this process creates its ONNX sessions in.
pub(crate) fn onnx_environment() -> error_stack::Result<&'static Environment, &'static ort::Error> {
    match ONNX_ENVIRONMENT.get_or_init(|| {
        ort::init()
            .with_logger(StdArc::new(report_native_diagnostic))
            .build()
    }) {
        Ok(environment) => Ok(environment),
        Err(refusal) => Err(Report::new(refusal)),
    }
}

/// What the ONNX Runtime library this process loads reports about itself, read once at startup on
/// the bulk workers, where a model load runs: its version, the execution providers it reports, and
/// whether the CUDA execution provider registers in a session builder that is dropped afterwards,
/// which loads the provider library and the CUDA runtime the way a session would. The probe loads
/// the library `ort` would load through `ort`'s fallible loader first, so a node without it reports
/// that instead of panicking, then uses the process's one environment and creates no other.
pub(crate) async fn probe_onnx_runtime(
    executor: &Executor,
) -> error_stack::Result<OnnxRuntimeDetection, OnnxRuntimeProbeError> {
    let reservation = executor
        .reserve(MemoryClass::Bulk, SESSION_LOAD_RESERVATION_BYTES)
        .await
        .change_context(OnnxRuntimeProbeError::Execution)?;
    executor
        .run_cpu(CpuClass::Bulk, reservation, |_charge, cancellation| {
            OnnxRuntimeProbe::run(cancellation)
        })
        .await
        .change_context(OnnxRuntimeProbeError::Execution)
}

/// Why the startup probe of the ONNX Runtime answered nothing.
#[derive(Debug, thiserror::Error)]
pub(crate) enum OnnxRuntimeProbeError {
    #[error("the node's bounded execution did not run the ONNX Runtime probe")]
    Execution,
}

/// The steps of the startup probe, each reading the library this process loaded.
struct OnnxRuntimeProbe;

impl OnnxRuntimeProbe {
    fn run(cancellation: &Cancellation) -> OnnxRuntimeDetection {
        let library = Self::library_path(std::env::var("ORT_DYLIB_PATH"));
        if let Err(refusal) = ort::init_from(&library) {
            tracing::info!(
                library = %library.display(),
                error = %refusal,
                "the ONNX Runtime library did not load"
            );
            return OnnxRuntimeDetection::Undetected(UndetectedReason::LibraryUnavailable);
        }
        let environment = match onnx_environment() {
            Ok(environment) => environment,
            Err(refusal) => {
                tracing::info!(
                    error = %format_args!("{refusal:#}"),
                    "the ONNX Runtime environment was not created"
                );
                return OnnxRuntimeDetection::Undetected(UndetectedReason::LibraryUnavailable);
            }
        };
        let version = Self::version();
        let providers = Self::providers();
        if cancellation.check().is_err() {
            return OnnxRuntimeDetection::Undetected(UndetectedReason::DeadlineExpired);
        }
        let cuda = Self::cuda(environment);
        OnnxRuntimeDetection::Loaded(OnnxRuntime {
            version,
            providers,
            cuda,
        })
    }

    /// The library `ort` loads when the process first uses it: the one `ORT_DYLIB_PATH` names, and
    /// otherwise the platform's name for it, which the dynamic loader resolves. Loading it here
    /// first leaves the same library loaded for the environment.
    fn library_path(configured: Result<String, std::env::VarError>) -> PathBuf {
        match configured {
            Ok(path) if !path.is_empty() => PathBuf::from(path),
            Ok(_) | Err(_) => PathBuf::from(format!(
                "{}onnxruntime{}",
                std::env::consts::DLL_PREFIX,
                std::env::consts::DLL_SUFFIX
            )),
        }
    }

    /// The version the loaded library reports through `OrtGetApiBase()->GetVersionString()`. `ort`
    /// loads the library itself and exposes only its API table, so the probe finds the image that
    /// holds that table and reads the version from the same image.
    #[cfg(unix)]
    fn version() -> Detected<PropertyText> {
        use std::ffi::{CStr, c_void};

        let api: &'static ort::sys::OrtApi = ort::api();
        let anchor = std::ptr::from_ref(api).cast::<c_void>();
        let mut image = std::mem::MaybeUninit::<libc::Dl_info>::zeroed();
        // SAFETY: `anchor` points into the API table of the library `ort` loaded, which stays
        // loaded for the process's life; dladdr only fills `image` from the loader's records.
        let found = unsafe { libc::dladdr(anchor, image.as_mut_ptr()) };
        if found == 0 {
            return Detected::Undetected(UndetectedReason::NotReported);
        }
        // SAFETY: a non-zero answer means dladdr filled `image`, which was zeroed before.
        let image = unsafe { image.assume_init() };
        if image.dli_fname.is_null() {
            return Detected::Undetected(UndetectedReason::NotReported);
        }
        // SAFETY: RTLD_NOLOAD loads nothing: it returns a new reference to the image the process
        // already holds, named by the path dladdr reported, or null.
        let handle = unsafe { libc::dlopen(image.dli_fname, libc::RTLD_NOLOAD | libc::RTLD_LAZY) };
        if handle.is_null() {
            return Detected::Undetected(UndetectedReason::NotReported);
        }
        // SAFETY: the handle names the loaded library; dlsym only reads its symbol table.
        let symbol = unsafe { libc::dlsym(handle, c"OrtGetApiBase".as_ptr()) };
        let version = if symbol.is_null() {
            Detected::Undetected(UndetectedReason::NotReported)
        } else {
            // SAFETY: `OrtGetApiBase` takes nothing and returns the library's static
            // `OrtApiBase`, as the C API header every release ships declares it.
            let get_api_base = unsafe {
                std::mem::transmute::<
                    *mut c_void,
                    unsafe extern "system" fn() -> *const ort::sys::OrtApiBase,
                >(symbol)
            };
            // SAFETY: calling the library's own entry point, which has no preconditions.
            let base = unsafe { get_api_base() };
            if base.is_null() {
                Detected::Undetected(UndetectedReason::NotReported)
            } else {
                // SAFETY: `base` is the library's static API base; GetVersionString returns a
                // static NUL-terminated string the caller must not free.
                let text = unsafe { ((*base).GetVersionString)() };
                if text.is_null() {
                    Detected::Undetected(UndetectedReason::NotReported)
                } else {
                    // SAFETY: a non-null answer is a NUL-terminated static string.
                    let text = unsafe { CStr::from_ptr(text) };
                    match text.to_str() {
                        Ok(text) => Self::text(text),
                        Err(_) => Detected::Undetected(UndetectedReason::QueryFailed),
                    }
                }
            }
        };
        // SAFETY: releases only the reference the dlopen above took; `ort` keeps its own.
        unsafe { libc::dlclose(handle) };
        version
    }

    #[cfg(not(unix))]
    fn version() -> Detected<PropertyText> {
        Detected::Undetected(UndetectedReason::NotReported)
    }

    /// The execution providers the loaded library reports, in the order it reports them.
    fn providers() -> Detected<ExecutionProviders> {
        use std::ffi::{CStr, c_char, c_int};

        let api = ort::api();
        let mut names: *mut *mut c_char = std::ptr::null_mut();
        let mut count: c_int = 0;
        // SAFETY: GetAvailableProviders writes an array of `count` NUL-terminated names it
        // allocated, or answers a status the caller owns.
        let status = unsafe { (api.GetAvailableProviders)(&mut names, &mut count) };
        if !status.0.is_null() {
            // SAFETY: a non-null status belongs to the caller, released exactly once.
            unsafe { (api.ReleaseStatus)(status.0) };
            return Detected::Undetected(UndetectedReason::QueryFailed);
        }
        if names.is_null() {
            return Detected::Undetected(UndetectedReason::NotReported);
        }
        let mut refusal = None;
        let listed = match usize::try_from(count) {
            Ok(listed) => listed,
            Err(_) => {
                refusal = Some(UndetectedReason::QueryFailed);
                0
            }
        };
        let mut providers = Vec::new();
        for index in 0..listed {
            // SAFETY: `names` holds `count` valid NUL-terminated names until it is released below.
            let name = unsafe { CStr::from_ptr(*names.add(index)) };
            let Ok(name) = name.to_str() else {
                refusal = Some(UndetectedReason::QueryFailed);
                continue;
            };
            match Self::text(name) {
                Detected::Value(name) => providers.push(name),
                Detected::Undetected(reason) => refusal = Some(reason),
            }
        }
        // SAFETY: releases exactly the array GetAvailableProviders returned, with its count.
        let released = unsafe { (api.ReleaseAvailableProviders)(names, count) };
        if !released.0.is_null() {
            // SAFETY: a non-null status belongs to the caller, released exactly once.
            unsafe { (api.ReleaseStatus)(released.0) };
        }
        if let Some(reason) = refusal {
            return Detected::Undetected(reason);
        }
        match ExecutionProviders::try_from(providers) {
            Ok(providers) => Detected::Value(providers),
            Err(_) => Detected::Undetected(UndetectedReason::ExceedsRecordBound),
        }
    }

    /// One attempt to register the CUDA execution provider in a session builder of the process's
    /// environment. The builder is dropped without a session; what registration loaded stays
    /// loaded, as it would for a session.
    fn cuda(environment: &'static Environment) -> CudaProviderRegistration {
        use ort::ep::ExecutionProvider as _;

        match ort::ep::CUDA::is_available() {
            Ok(true) => {}
            Ok(false) => return CudaProviderRegistration::NotInLibrary,
            Err(error) => {
                tracing::info!(%error, "the ONNX Runtime did not list its execution providers");
                return CudaProviderRegistration::RegistrationFailed;
            }
        }
        let mut builder = match Session::builder(environment) {
            Ok(builder) => builder,
            Err(error) => {
                tracing::info!(%error, "the ONNX Runtime did not create a session builder");
                return CudaProviderRegistration::RegistrationFailed;
            }
        };
        match ort::ep::CUDA::default().register(&mut builder) {
            Ok(()) => CudaProviderRegistration::Registered,
            Err(error) => {
                tracing::info!(%error, "the CUDA execution provider did not register");
                CudaProviderRegistration::RegistrationFailed
            }
        }
    }

    /// Detected text, nothing when the library reported none, or the bound it breaks.
    fn text(text: &str) -> Detected<PropertyText> {
        match PropertyText::new(text.trim()) {
            Ok(text) => Detected::Value(text),
            Err(nervix_models::PropertyValueError::Empty) => {
                Detected::Undetected(UndetectedReason::NotReported)
            }
            Err(_) => Detected::Undetected(UndetectedReason::ExceedsRecordBound),
        }
    }
}

/// Projects native diagnostics at their reporting boundary: native text can contain tensor values.
/// Severity, the tracing target and the native code site remain available to operators.
fn report_native_diagnostic(
    level: ort::logging::LogLevel,
    _category: &str,
    id: &str,
    site: &str,
    _message: &str,
) {
    let span = tracing::span!(
        target: "ort::logging",
        tracing::Level::TRACE,
        "ort",
        id,
        location = site
    );
    macro_rules! report {
        ($severity:expr) => {
            tracing::event!(
                target: "ort::logging",
                parent: &span,
                $severity,
                native_level = ?level,
                native_site = site,
                "native ONNX diagnostic"
            )
        };
    }
    match level {
        ort::logging::LogLevel::Verbose => report!(tracing::Level::TRACE),
        ort::logging::LogLevel::Info => report!(tracing::Level::INFO),
        ort::logging::LogLevel::Warning => report!(tracing::Level::WARN),
        ort::logging::LogLevel::Error | ort::logging::LogLevel::Fatal => {
            report!(tracing::Level::ERROR);
        }
    }
}

/// What loading a model is charged. ONNX Runtime allocates a session's memory itself and keeps it
/// for as long as the inferencer holds the session, which no transient budget can stand for, so
/// the charge only admits the load onto the bulk workers.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "one byte is an admission token for one inference session load job"
    )
)]
const SESSION_LOAD_RESERVATION_BYTES: u64 = 1;

struct RuntimeTensorSlice {
    shape: Vec<usize>,
    values: Vec<f32>,
}

/// Tensor-shape rules of a declared tensor schema: reading a configuration constant as a tensor,
/// and checking concrete shapes against the declared axes.
trait RuntimeTensorSchema {
    fn tensor_from_runtime_value(
        &self,
        value: &RuntimeValue,
    ) -> error_stack::Result<RuntimeTensorSlice, InferencerError>;
    fn batch_shape(
        &self,
        slice_shape: &[usize],
        batch_size: usize,
    ) -> error_stack::Result<Vec<usize>, InferencerError>;
    fn validate_concrete_shape(
        &self,
        shape: &[usize],
        batch_size: usize,
        include_batch: bool,
    ) -> error_stack::Result<(), InferencerError>;
    fn tensor_from_dimensions(
        &self,
        dimensions: &[InferencerTensorDimension],
        value: &RuntimeValue,
    ) -> error_stack::Result<RuntimeTensorSlice, InferencerError>;
    fn shape_from_dimensions_without_values(
        &self,
        dimensions: &[InferencerTensorDimension],
    ) -> error_stack::Result<Vec<usize>, InferencerError>;
}

impl RuntimeTensorSchema for InferencerTensorSchema {
    fn tensor_from_runtime_value(
        &self,
        value: &RuntimeValue,
    ) -> error_stack::Result<RuntimeTensorSlice, InferencerError> {
        self.tensor_from_dimensions(&self.dimensions, value)
    }

    fn batch_shape(
        &self,
        slice_shape: &[usize],
        batch_size: usize,
    ) -> error_stack::Result<Vec<usize>, InferencerError> {
        let expected_slice_rank = self
            .dimensions
            .len()
            .checked_sub(1)
            .ok_or_else(|| Report::new(InferencerError::BatchedSchemaWithoutDimensions))?;
        if slice_shape.len() != expected_slice_rank {
            return Err(Report::new(InferencerError::SliceRankMismatch {
                actual: slice_shape.len(),
                expected: expected_slice_rank,
            }));
        }
        let mut shape = Vec::with_capacity(self.dimensions.len());
        let mut slice_dimensions = slice_shape.iter();
        for dimension in &self.dimensions {
            if let InferencerTensorDimension::Batch = dimension {
                shape.push(batch_size);
            } else {
                shape.push(*slice_dimensions.next().verified(
                    "the rank check above rejected a slice shape with too few dimensions",
                ));
            }
        }
        self.validate_concrete_shape(&shape, batch_size, true)?;
        Ok(shape)
    }

    fn validate_concrete_shape(
        &self,
        shape: &[usize],
        batch_size: usize,
        include_batch: bool,
    ) -> error_stack::Result<(), InferencerError> {
        let dimensions = self
            .dimensions
            .iter()
            .filter(|dimension| include_batch || !dimension.is_batch());
        if shape.len() != dimensions.clone().count() {
            return Err(Report::new(InferencerError::ShapeDimensionCountMismatch {
                shape: shape.to_vec(),
                dimensions: self.dimensions.clone(),
            }));
        }
        for (actual, declared) in shape.iter().zip(dimensions) {
            match declared {
                InferencerTensorDimension::Fixed(expected)
                    if *actual != expected.get().arch_into() =>
                {
                    return Err(Report::new(InferencerError::ShapeDimensionMismatch {
                        shape: shape.to_vec(),
                        actual: *actual,
                        expected: *expected,
                    }));
                }
                InferencerTensorDimension::Batch if *actual != batch_size => {
                    return Err(Report::new(InferencerError::ShapeBatchMismatch {
                        shape: shape.to_vec(),
                        actual: *actual,
                        batch_size,
                    }));
                }
                InferencerTensorDimension::Fixed(_)
                | InferencerTensorDimension::Dynamic
                | InferencerTensorDimension::Batch => {}
            }
        }
        Ok(())
    }

    fn tensor_from_dimensions(
        &self,
        dimensions: &[InferencerTensorDimension],
        value: &RuntimeValue,
    ) -> error_stack::Result<RuntimeTensorSlice, InferencerError> {
        let Some((dimension, remaining)) = dimensions.split_first() else {
            let RuntimeValue::F32(value) = value else {
                return Err(Report::new(InferencerError::ElementRequiresF32 {
                    value: value.clone(),
                }));
            };
            return Ok(RuntimeTensorSlice {
                shape: Vec::new(),
                values: vec![value.into_inner()],
            });
        };
        if let InferencerTensorDimension::Batch = dimension {
            return self.tensor_from_dimensions(remaining, value);
        }
        let (values, size) = match (dimension, value) {
            (InferencerTensorDimension::Fixed(expected), RuntimeValue::Array(values))
                if values.len() == expected.get().arch_into() =>
            {
                (values, expected.get().arch_into())
            }
            (InferencerTensorDimension::Fixed(expected), RuntimeValue::Array(values)) => {
                return Err(Report::new(InferencerError::FixedAxisLength {
                    actual: values.len(),
                    expected: *expected,
                }));
            }
            (InferencerTensorDimension::Dynamic, RuntimeValue::Vec(values)) => {
                (values, values.len())
            }
            (InferencerTensorDimension::Fixed(_), value) => {
                return Err(Report::new(InferencerError::FixedAxisRequiresArray {
                    value: value.clone(),
                }));
            }
            (InferencerTensorDimension::Dynamic, value) => {
                return Err(Report::new(InferencerError::DynamicAxisRequiresVec {
                    value: value.clone(),
                }));
            }
            (InferencerTensorDimension::Batch, _) => unreachable!(),
        };
        let mut child_shape: Option<Vec<usize>> = None;
        let mut flattened = Vec::new();
        for child in values {
            let child = self.tensor_from_dimensions(remaining, child)?;
            if let Some(expected) = &child_shape
                && expected != &child.shape
            {
                return Err(Report::new(InferencerError::RaggedChildShapes {
                    expected: expected.clone(),
                    actual: child.shape.clone(),
                }));
            }
            child_shape.get_or_insert_with(|| child.shape.clone());
            flattened.extend(child.values);
        }
        let child_shape = match child_shape {
            Some(shape) => shape,
            None => self.shape_from_dimensions_without_values(remaining)?,
        };
        // This level's dimension, ahead of its children's.
        let leading = [size];
        let dimensions = child_shape
            .len()
            .checked_add(leading.len())
            .assured("a shape holds at most isize::MAX dimensions, so one more fits usize");
        let mut shape = Vec::with_capacity(dimensions);
        shape.extend(leading);
        shape.extend(child_shape);
        Ok(RuntimeTensorSlice {
            shape,
            values: flattened,
        })
    }

    fn shape_from_dimensions_without_values(
        &self,
        dimensions: &[InferencerTensorDimension],
    ) -> error_stack::Result<Vec<usize>, InferencerError> {
        let mut shape = Vec::new();
        for dimension in dimensions {
            match dimension {
                InferencerTensorDimension::Fixed(size) => shape.push(size.get().arch_into()),
                InferencerTensorDimension::Dynamic => {
                    return Err(Report::new(InferencerError::InnerDynamicFromEmpty));
                }
                InferencerTensorDimension::Batch => {}
            }
        }
        Ok(shape)
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use nervix_execution::Executor;
    use nervix_models::{
        InferencerTensorDimension, InferencerTensorElementType, InferencerTensorMapping,
        InferencerTensorRepresentation, InferencerTensorSchema, ParseAsType,
    };
    use nonzero_ext::nonzero;
    use ordered_float::OrderedFloat;

    use super::{OnnxRuntimeProbe, RuntimeTensorSchema};
    use crate::{
        runtime::{
            CompiledInferencerInputProgram,
            test_fixtures::{expression, named, test_schema},
        },
        runtime_schema::RuntimeValue,
    };

    #[test]
    fn the_probe_loads_the_library_ort_would_load() {
        use std::{env::VarError, ffi::OsString, path::PathBuf};

        let platform_name = PathBuf::from(format!(
            "{}onnxruntime{}",
            std::env::consts::DLL_PREFIX,
            std::env::consts::DLL_SUFFIX
        ));
        assert_eq!(
            OnnxRuntimeProbe::library_path(Ok("/opt/ort/lib/libonnxruntime.so".to_string())),
            PathBuf::from("/opt/ort/lib/libonnxruntime.so")
        );
        assert_eq!(
            OnnxRuntimeProbe::library_path(Ok(String::new())),
            platform_name
        );
        assert_eq!(
            OnnxRuntimeProbe::library_path(Err(VarError::NotPresent)),
            platform_name
        );
        assert_eq!(
            OnnxRuntimeProbe::library_path(Err(VarError::NotUnicode(OsString::from("x")))),
            platform_name
        );
    }

    #[nervix_primitives::test]
    async fn the_probe_reports_what_the_loaded_library_reports() {
        use nervix_models::{
            CudaProviderRegistration, Detected, OnnxRuntimeDetection, PropertyText,
        };

        let executor = Executor::default();
        let detection = super::probe_onnx_runtime(&executor)
            .await
            .expect("the bulk workers run the probe");
        let OnnxRuntimeDetection::Loaded(runtime) = &detection else {
            panic!("the test process loads ONNX Runtime from ORT_DYLIB_PATH: {detection:?}");
        };
        let Detected::Value(version) = &runtime.version else {
            panic!(
                "the loaded library reports its version: {:?}",
                runtime.version
            );
        };
        assert!(version.as_str().starts_with("1."), "{version}");
        let Detected::Value(providers) = &runtime.providers else {
            panic!(
                "the loaded library lists its providers: {:?}",
                runtime.providers
            );
        };
        let names = providers
            .as_slice()
            .iter()
            .map(PropertyText::as_str)
            .collect::<Vec<_>>();
        assert!(names.contains(&"CPUExecutionProvider"), "{names:?}");
        if !names.contains(&"CUDAExecutionProvider") {
            assert_eq!(runtime.cuda, CudaProviderRegistration::NotInLibrary);
        }
    }

    #[test]
    fn a_constant_tensor_keeps_its_nested_array_shape_and_row_major_values() {
        let schema = dense(vec![
            InferencerTensorDimension::Fixed(nonzero!(2u32)),
            InferencerTensorDimension::Fixed(nonzero!(3u32)),
        ]);
        let value = RuntimeValue::Array(vec![
            RuntimeValue::Array(vec![
                RuntimeValue::F32(OrderedFloat(1.0)),
                RuntimeValue::F32(OrderedFloat(2.0)),
                RuntimeValue::F32(OrderedFloat(3.0)),
            ]),
            RuntimeValue::Array(vec![
                RuntimeValue::F32(OrderedFloat(4.0)),
                RuntimeValue::F32(OrderedFloat(5.0)),
                RuntimeValue::F32(OrderedFloat(6.0)),
            ]),
        ]);

        let tensor = schema.tensor_from_runtime_value(&value).unwrap();

        assert_eq!(tensor.shape, vec![2, 3]);
        assert_eq!(tensor.values, vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
    }

    #[test]
    fn dynamic_dense_tensor_rejects_ragged_vectors() {
        let schema = dense(vec![
            InferencerTensorDimension::Dynamic,
            InferencerTensorDimension::Dynamic,
        ]);
        let value = RuntimeValue::Vec(vec![
            RuntimeValue::Vec(vec![RuntimeValue::F32(OrderedFloat(1.0))]),
            RuntimeValue::Vec(vec![
                RuntimeValue::F32(OrderedFloat(2.0)),
                RuntimeValue::F32(OrderedFloat(3.0)),
            ]),
        ]);

        assert!(schema.tensor_from_runtime_value(&value).is_err());
    }

    fn dense(dimensions: Vec<InferencerTensorDimension>) -> InferencerTensorSchema {
        InferencerTensorSchema {
            representation: InferencerTensorRepresentation::Dense,
            element_type: InferencerTensorElementType::F32,
            dimensions,
        }
    }

    /// The message a failed operation reports to the processor that called it.
    fn failure<T, C: error_stack::Context>(result: error_stack::Result<T, C>) -> String {
        let Err(error) = result else {
            panic!("the operation must fail");
        };
        error.current_context().to_string()
    }

    #[test]
    fn tensor_elements_and_axes_reject_values_the_schema_does_not_describe() {
        let scalar = dense(Vec::new());
        assert_eq!(
            failure(scalar.tensor_from_runtime_value(&RuntimeValue::I64(1))),
            "tensor element requires F32, got I64(1)"
        );

        let pair = dense(vec![InferencerTensorDimension::Fixed(nonzero!(2u32))]);
        let three = RuntimeValue::Array(vec![RuntimeValue::F32(OrderedFloat(1.0)); 3]);
        assert_eq!(
            failure(pair.tensor_from_runtime_value(&three)),
            "tensor ARRAY axis contains 3 values, expected 2"
        );
        assert_eq!(
            failure(pair.tensor_from_runtime_value(&RuntimeValue::I64(1))),
            "fixed tensor axis requires ARRAY, got I64(1)"
        );

        let dynamic = dense(vec![InferencerTensorDimension::Dynamic]);
        assert_eq!(
            failure(dynamic.tensor_from_runtime_value(&RuntimeValue::I64(1))),
            "dynamic tensor axis requires VEC, got I64(1)"
        );

        let nested = dense(vec![
            InferencerTensorDimension::Dynamic,
            InferencerTensorDimension::Dynamic,
        ]);
        assert_eq!(
            failure(nested.tensor_from_runtime_value(&RuntimeValue::Vec(Vec::new()))),
            "cannot infer an inner DYNAMIC axis from an empty outer vector"
        );
    }

    #[test]
    fn batched_tensor_shapes_reject_mismatched_ranks_and_dimensions() {
        let schema = dense(vec![
            InferencerTensorDimension::Batch,
            InferencerTensorDimension::Fixed(nonzero!(2u32)),
        ]);
        assert_eq!(
            failure(schema.batch_shape(&[2, 1], 3)),
            "tensor slice rank 2 does not match declared rank 1"
        );
        assert_eq!(
            failure(schema.validate_concrete_shape(&[3], 3, true)),
            "tensor shape [3] does not match declared dimensions [Batch, Fixed(2)]"
        );
        assert_eq!(
            failure(schema.validate_concrete_shape(&[3, 4], 3, true)),
            "tensor shape [3, 4] has dimension 4, expected 2"
        );
        assert_eq!(
            failure(schema.validate_concrete_shape(&[4, 2], 3, true)),
            "tensor shape [4, 2] has batch dimension 4, expected 3"
        );
        assert_eq!(
            schema
                .batch_shape(&[2], 3)
                .expect("a pair joins along the leading BATCH axis"),
            vec![3, 2]
        );
    }

    #[test]
    fn inferencer_inputs_reject_tensor_names_that_are_not_fields() {
        let mapping = InferencerTensorMapping {
            tensor: "not a field".to_string(),
            schema: dense(Vec::new()),
            expression: expression("input.score"),
        };
        let input_schema = test_schema(&[("score", ParseAsType::F32)]);
        assert_eq!(
            failure(CompiledInferencerInputProgram::compile(
                &named("score_events"),
                std::slice::from_ref(&mapping),
                &input_schema,
                None,
                nervix_vm::VmSettings::default(),
            )),
            "inferencer 'score_events' tensor name 'not a field' is not a valid field"
        );
    }
}
