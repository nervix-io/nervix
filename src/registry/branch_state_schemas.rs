//! The Arrow shapes a domain's deduplicator keyspaces and windows take in a backup archive.
//!
//! Layer: decisions.
//!
//! - **Owns.** Resolving, from a domain's models, each deduplicator's typed key columns and each
//!   window processor's input rows, aggregate argument columns, aggregate structures and model
//!   digest, with the compilers the runtime uses.
//! - **Depends on.** The vocabulary, the VM's expression and window compilers, and the registry's
//!   schema bindings.
//! - **Must not know.** Archive encoding, runtime state, or how a section is captured or installed.

use std::{collections::BTreeMap, num::NonZeroUsize};

use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use error_stack::{Report, ResultExt as _};
use nervix_backup::{BranchLifecycleRecord, WindowStateDescriptor};
use nervix_models::{
    CreateDeduplicator, CreateWindowProcessor, DomainName, Model, ModelIndex, ModelKind, ModelName,
    WindowModelDigest,
};
use nervix_primitives::sync::StdArc;
use nervix_vm::window::{
    CompiledWindowDemand, WindowAggregateStorageKind, argument_snapshot_schema,
    lower_window_assignments,
};

use crate::registry::{
    error::RegistryError,
    validation::{
        branching::relay_declared_branch_schema,
        processor::{ModelValidationContext, deduplicator_key_types},
        window_route::compile_window_route,
        wire::schema_for_ack_model,
    },
};

/// What one aggregate structure of a window carries beside the rows it retains.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WindowAccumulatorShape {
    /// The structure is rebuilt entirely from the retained rows.
    Retained,
    /// A linear histogram over `buckets` buckets, which also counts stepped rows until their
    /// delay expires.
    LinearHistogram { buckets: NonZeroUsize },
}

/// Everything an archived window must agree with before its state is installed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WindowStateSchemas {
    pub(crate) model: WindowModelDigest,
    /// The window's input relay schema, which its retained rows are laid out by.
    pub(crate) input: StdArc<ArrowSchema>,
    /// One nullable column per argument of every aggregate demand, in demand order.
    pub(crate) arguments: StdArc<ArrowSchema>,
    /// Each aggregate structure, in demand order.
    pub(crate) accumulators: Vec<WindowAccumulatorShape>,
}

/// Why an archived window does not continue in the restored domain, whose state then starts empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WindowStateSkip {
    /// The restored window processor is not the model the window's rows were admitted under.
    ModelChanged,
    /// The restored branch lifecycle does not hold the branch lifetime the window belongs to.
    IncarnationChanged,
}

impl WindowStateSkip {
    /// The reason a restore warning gives for the skipped window.
    pub(crate) fn reason(self) -> &'static str {
        match self {
            Self::ModelChanged => {
                "the archived window model differs from the restored window processor"
            }
            Self::IncarnationChanged => {
                "the archived window branch incarnation differs from the restored branch lifecycle"
            }
        }
    }
}

impl WindowStateSchemas {
    /// Why `window` cannot continue under these restored shapes, given the branch lifecycles the
    /// restore installs, or nothing when it continues.
    pub(crate) fn skip_of<'a>(
        &self,
        window: &WindowStateDescriptor,
        lifecycles: impl IntoIterator<Item = &'a BranchLifecycleRecord>,
    ) -> Option<WindowStateSkip> {
        if window.model != self.model {
            return Some(WindowStateSkip::ModelChanged);
        }
        for lifecycle in lifecycles {
            if lifecycle.owner_kind != ModelKind::WindowProcessor
                || lifecycle.entity != window.entity
            {
                continue;
            }
            // A lifecycle lists each branch of its processor once, which bounds this walk.
            for branch in &lifecycle.branches {
                if branch.key == window.branch {
                    if branch.incarnation == window.incarnation {
                        return None;
                    }
                    return Some(WindowStateSkip::IncarnationChanged);
                }
            }
        }
        Some(WindowStateSkip::IncarnationChanged)
    }
}

/// The archive shapes of one domain's deduplicator and window processor state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct BranchStateSchemas {
    deduplicators: BTreeMap<ModelName, StdArc<ArrowSchema>>,
    windows: BTreeMap<ModelName, WindowStateSchemas>,
}

impl BranchStateSchemas {
    /// Resolves the shapes of every deduplicator and window processor `models` configures.
    pub(crate) fn resolve(
        domain: &DomainName,
        models: &ModelIndex,
    ) -> Result<Self, Report<RegistryError>> {
        let mut schemas = Self::default();
        for model in models.models() {
            match model {
                Model::Deduplicator(deduplicator) => {
                    let identifier = ModelName::from(&deduplicator.name);
                    let keys = deduplicator_key_schema(domain, &identifier, models, deduplicator)?;
                    schemas.deduplicators.insert(identifier, keys);
                }
                Model::WindowProcessor(window) => {
                    let identifier = ModelName::from(&window.name);
                    let window_schemas = window_state_schemas(domain, &identifier, models, window)?;
                    schemas.windows.insert(identifier, window_schemas);
                }
                _ => {}
            }
        }
        Ok(schemas)
    }

    /// The typed key columns and `seen_at` column of deduplicator `entity`'s archived groups.
    pub(crate) fn deduplicator(&self, entity: &ModelName) -> Option<&StdArc<ArrowSchema>> {
        self.deduplicators.get(entity)
    }

    /// What window processor `entity`'s archived state must agree with.
    pub(crate) fn window(&self, entity: &ModelName) -> Option<&WindowStateSchemas> {
        self.windows.get(entity)
    }
}

/// The Arrow type every archived `seen_at` value takes: Nervix's datetime in UTC nanoseconds.
fn seen_at_data_type() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into()))
}

fn deduplicator_key_schema(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    deduplicator: &CreateDeduplicator,
) -> Result<StdArc<ArrowSchema>, Report<RegistryError>> {
    let Some(input) = deduplicator.from.relays().first() else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "deduplicator input requires at least one input relay".to_string(),
        }));
    };
    let input_schema = schema_for_ack_model(domain, identifier, models, input)?;
    let key_types = deduplicator_key_types(domain, identifier, models, deduplicator, input_schema)?;
    let mut fields = Vec::with_capacity(key_types.len() + 1);
    for (index, key) in key_types.into_iter().enumerate() {
        // A key part records a null for any value its type does not normalize, so every key
        // column admits nulls whatever its expression's nullability.
        fields.push(Field::new(
            nervix_backup::deduplicator_key_column(index),
            key.data_type,
            true,
        ));
    }
    fields.push(Field::new(
        nervix_backup::DEDUPLICATOR_SEEN_AT_COLUMN,
        seen_at_data_type(),
        false,
    ));
    Ok(StdArc::new(ArrowSchema::new(fields)))
}

fn window_state_schemas(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    window: &CreateWindowProcessor,
) -> Result<WindowStateSchemas, Report<RegistryError>> {
    let invalid = |reason: String| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason,
        })
    };
    let Some(input_relay) = window.from.relays().first() else {
        return Err(invalid(
            "window processor requires at least one input relay".to_string(),
        ));
    };
    let input_schema = schema_for_ack_model(domain, identifier, models, input_relay)?;
    let branch_schema = relay_declared_branch_schema(domain, identifier, models, input_relay)?;
    let mut demands: Vec<CompiledWindowDemand> = Vec::new();
    for output in window.output_routes.outputs() {
        let output_schema = schema_for_ack_model(domain, identifier, models, &output.relay)?;
        let aggregate = lower_window_assignments(&output.construction).map_err(|reason| {
            invalid(format!(
                "window output '{}' is invalid: {reason}",
                output.relay
            ))
        })?;
        let compiled = compile_window_route(
            ModelValidationContext {
                domain,
                identifier,
                models,
            },
            output,
            &aggregate.inner,
            output_schema,
            input_schema,
            branch_schema,
        )?;
        demands.extend(compiled.demands);
    }
    let mut accumulators = Vec::with_capacity(demands.len());
    for demand in &demands {
        let shape = match (demand.storage, &demand.linear_histogram) {
            (WindowAggregateStorageKind::Histogram, Some(histogram)) => {
                WindowAccumulatorShape::LinearHistogram {
                    buckets: histogram.buckets,
                }
            }
            (WindowAggregateStorageKind::Histogram, None) => {
                return Err(invalid(
                    "a window histogram demand compiled without its bucket layout".to_string(),
                ));
            }
            _ => WindowAccumulatorShape::Retained,
        };
        accumulators.push(shape);
    }
    let model = window
        .model_digest()
        .change_context_lazy(|| RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "the window processor does not render as canonical NSPL".to_string(),
        })?;
    Ok(WindowStateSchemas {
        model,
        input: crate::runtime_schema::compile_schema(input_schema).arrow_schema(),
        arguments: StdArc::new(argument_snapshot_schema(&demands)),
        accumulators,
    })
}
