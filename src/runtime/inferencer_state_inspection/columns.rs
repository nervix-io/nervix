//! Layer: data plane.
//! Owns: direct Arrow construction of inference headers, tensor shapes and bit-exact F32 values.
//! Depends on: immutable native tensor views, typed state plans and admitted page memory.
//! Must not know: Models, Plexus, Row encoding, transport or mutable inference sessions.

use super::*;

/// Start without row/value buffers; admitted page construction owns their growth.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "Arrow columns start at zero row/value capacity so page admission, rather than \
                  Arrow's eager default, controls their growth"
    )
)]
const EMPTY_COLUMN_CAPACITY: usize = 0;

pub(super) struct Headers {
    fingerprint: StringBuilder,
    incarnation: UInt64Builder,
    revision: UInt64Builder,
    persisted: UInt64Builder,
    bytes: UInt64Builder,
    confirmed: UInt64Builder,
    replicas: UInt64Builder,
    stage: StringBuilder,
}

impl Default for Headers {
    fn default() -> Self {
        Self {
            fingerprint: StringBuilder::with_capacity(EMPTY_COLUMN_CAPACITY, EMPTY_COLUMN_CAPACITY),
            incarnation: UInt64Builder::with_capacity(EMPTY_COLUMN_CAPACITY),
            revision: UInt64Builder::with_capacity(EMPTY_COLUMN_CAPACITY),
            persisted: UInt64Builder::with_capacity(EMPTY_COLUMN_CAPACITY),
            bytes: UInt64Builder::with_capacity(EMPTY_COLUMN_CAPACITY),
            confirmed: UInt64Builder::with_capacity(EMPTY_COLUMN_CAPACITY),
            replicas: UInt64Builder::with_capacity(EMPTY_COLUMN_CAPACITY),
            stage: StringBuilder::with_capacity(EMPTY_COLUMN_CAPACITY, EMPTY_COLUMN_CAPACITY),
        }
    }
}

impl Headers {
    pub(super) fn append(&mut self, branch: &CapturedBranch, bytes: u64) -> ReadResult<()> {
        match &branch.key {
            Some(key) => self.fingerprint.append_value(key.fingerprint().to_string()),
            None => self.fingerprint.append_null(),
        }
        self.incarnation.append_value(branch.value()?.incarnation);
        self.revision.append_value(branch.generation.revision);
        self.persisted.append_value(branch.persisted);
        self.bytes.append_value(bytes);
        self.confirmed.append_value(branch.confirmed);
        self.replicas.append_value(branch.replicas);
        self.stage.append_value(branch.stage().as_ref());
        Ok(())
    }

    pub(super) fn finish(mut self, total: u64) -> ProcessorStateSectionColumns {
        ProcessorStateSectionColumns {
            section: ProcessorStateSection::Header,
            rows: total,
            payload: vec![
                StdArc::new(self.fingerprint.finish()),
                StdArc::new(self.incarnation.finish()),
                StdArc::new(self.revision.finish()),
                StdArc::new(self.persisted.finish()),
                StdArc::new(self.bytes.finish()),
                StdArc::new(self.confirmed.finish()),
                StdArc::new(self.replicas.finish()),
                StdArc::new(self.stage.finish()),
            ],
            appended: Vec::new(),
            key_fields: Vec::new(),
        }
    }
}

pub(super) struct Tensors {
    index: u32,
    input: StringBuilder,
    output: StringBuilder,
    shape: ListBuilder<UInt64Builder>,
    values: ListBuilder<Float32Builder>,
}

impl Tensors {
    pub(super) fn new(index: usize) -> ReadResult<Self> {
        Ok(Self {
            index: u32::try_from(index).change_context(ProcessorStatePageError::Columns)?,
            input: StringBuilder::with_capacity(EMPTY_COLUMN_CAPACITY, EMPTY_COLUMN_CAPACITY),
            output: StringBuilder::with_capacity(EMPTY_COLUMN_CAPACITY, EMPTY_COLUMN_CAPACITY),
            shape: ListBuilder::with_capacity(
                UInt64Builder::with_capacity(EMPTY_COLUMN_CAPACITY),
                EMPTY_COLUMN_CAPACITY,
            )
            .with_field(StdArc::new(arrow_schema::Field::new(
                "item",
                arrow_schema::DataType::UInt64,
                false,
            ))),
            values: ListBuilder::with_capacity(
                Float32Builder::with_capacity(EMPTY_COLUMN_CAPACITY),
                EMPTY_COLUMN_CAPACITY,
            )
            .with_field(StdArc::new(arrow_schema::Field::new(
                "item",
                arrow_schema::DataType::Float32,
                false,
            ))),
        })
    }

    pub(super) fn append(
        &mut self,
        generation: &InferencerGeneration,
        plan: &InferencerStatePortPlan,
        cancellation: &Cancellation,
    ) -> ReadResult<()> {
        let index = usize::try_from(self.index).change_context(ProcessorStatePageError::Columns)?;
        let (shape, values) = generation
            .generation
            .tensor(index)
            .change_context(ProcessorStatePageError::Columns)?;
        if values.len() != plan.elements || shape.len() != plan.schema.dimensions.len() {
            return Err(Report::new(ProcessorStatePageError::Columns));
        }
        self.input.append_value(&plan.input);
        self.output.append_value(&plan.output);
        for axis in shape {
            cancellation
                .check()
                .change_context(ProcessorStatePageError::Admission)?;
            self.shape.values().append_value(
                u64::try_from(*axis).change_context(ProcessorStatePageError::Columns)?,
            );
        }
        self.shape.append(true);
        self.values.values().append_slice(values);
        self.values.append(true);
        Ok(())
    }

    pub(super) fn finish(mut self, total: u64) -> ProcessorStateSectionColumns {
        ProcessorStateSectionColumns {
            section: ProcessorStateSection::Tensor(self.index),
            rows: total,
            payload: vec![
                StdArc::new(self.input.finish()),
                StdArc::new(self.output.finish()),
                StdArc::new(self.shape.finish()),
                StdArc::new(self.values.finish()),
            ],
            appended: Vec::new(),
            key_fields: Vec::new(),
        }
    }
}
