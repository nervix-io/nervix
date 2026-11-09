//! Layer: decisions.
//! Owns: the typed inference identity and state shapes an archived branch must match.
//! Depends on: the validated schedule, existing model identity and snapshot footprint decisions.
//! Must not know: tensor encoding, state publications, storage, locks or installation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        decisions,
        reason = "projects validated inference archive requirements"
    )
)]

use nervix_models::{InferencerModelFingerprint, InferencerTensorDimension, Model, ScheduledNode};

pub(crate) struct InferencerArchivePlan {
    pub(crate) model: InferencerModelFingerprint,
    shapes: Vec<Vec<u64>>,
    pub(crate) bytes_per_branch: u64,
    pub(crate) snapshot_bytes: u64,
}

impl InferencerArchivePlan {
    /// Project shape metadata without copying INITIAL expressions or allocating tensor values.
    pub(crate) fn of_scheduled(node: &ScheduledNode) -> Option<Self> {
        let Model::Inferencer(model) = node.config.as_ref() else {
            return None;
        };
        let identity = super::execution_revision::inferencer_model_fingerprint(node)?;
        let mut shapes = Vec::with_capacity(model.state.len());
        let mut layout = Vec::with_capacity(model.state.len());
        let mut bytes_per_branch = 0u64;
        for binding in &model.state {
            let shape = binding
                .schema
                .dimensions
                .iter()
                .map(|dimension| match dimension {
                    InferencerTensorDimension::Fixed(axis) => Some(u64::from(axis.get())),
                    InferencerTensorDimension::Dynamic | InferencerTensorDimension::Batch => None,
                })
                .collect::<Option<Vec<_>>>()?;
            let elements = u64::try_from(binding.schema.fixed_element_count()?).ok()?;
            bytes_per_branch = bytes_per_branch.checked_add(elements.checked_mul(4)?)?;
            layout.push((u64::try_from(shape.len()).ok()?, elements));
            shapes.push(shape);
        }
        Some(Self {
            model: identity,
            shapes,
            bytes_per_branch,
            snapshot_bytes: crate::inferencer_state_snapshot::footprint(layout).ok()?,
        })
    }

    pub(crate) fn shapes(&self) -> &[Vec<u64>] {
        &self.shapes
    }
}
