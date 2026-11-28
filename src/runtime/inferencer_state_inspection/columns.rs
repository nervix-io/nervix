//! Layer: data plane.
//! Owns: direct Arrow construction of inference headers, tensor shapes and bit-exact F32 values.
//! Depends on: immutable native tensor views, typed state plans and admitted page memory.
//! Must not know: Models, Plexus, Row encoding, transport or mutable inference sessions.

use arrow_array::{ArrayRef, Float32Array, ListArray, StringArray, UInt64Array};

use super::{
    super::processor_state_inspection::memory::{aligned, assert_buffers, slots},
    *,
};

fn overflow() -> Report<ProcessorStatePageError> {
    Report::new(ProcessorStatePageError::Admission)
}

fn offsets(rows: usize, columns: usize) -> ReadResult<u64> {
    let entries = rows.checked_add(1).ok_or_else(overflow)?;
    let entries = entries.checked_mul(columns).ok_or_else(overflow)?;
    slots::<i32>(entries)
}

#[derive(Clone, Copy, Default)]
pub(super) struct HeaderCapacity {
    pub(super) rows: usize,
    fingerprints: usize,
    stages: usize,
    nullable: bool,
}

impl HeaderCapacity {
    pub(super) fn with_branch(self, branch: &CapturedBranch) -> ReadResult<Self> {
        let rows = self.rows.checked_add(1).ok_or_else(overflow)?;
        let fingerprint = match &branch.key {
            Some(key) => key.fingerprint().fingerprint().len() * 2,
            None => 0,
        };
        let fingerprints = self
            .fingerprints
            .checked_add(fingerprint)
            .ok_or_else(overflow)?;
        let stages = self
            .stages
            .checked_add(PersistenceStage::ReplicaConfirmed.as_ref().len())
            .ok_or_else(overflow)?;
        Ok(Self {
            rows,
            fingerprints,
            stages,
            nullable: self.nullable || branch.key.is_none(),
        })
    }

    pub(super) fn buffers(self) -> ReadResult<u64> {
        let texts = self
            .fingerprints
            .checked_add(self.stages)
            .ok_or_else(overflow)?;
        let counters = aligned(slots::<u64>(self.rows)?)?
            .checked_mul(6)
            .ok_or_else(overflow)?;
        let validity = if self.nullable {
            aligned(slots::<u8>(self.rows.div_ceil(8))?)?
        } else {
            0
        };
        let bytes = checked_add(slots::<u8>(texts)?, offsets(self.rows, 2)?)?;
        checked_add(checked_add(bytes, counters)?, validity)
    }

    pub(super) fn allocation(self) -> ReadResult<u64> {
        let columns = checked_add(slots::<StringArray>(2)?, slots::<UInt64Array>(6)?)?;
        let columns = checked_add(columns, slots::<ArrayRef>(8)?)?;
        checked_add(self.buffers()?, columns)
    }
}

pub(super) struct TensorCapacity {
    rows: usize,
    input: usize,
    output: usize,
    axes: usize,
    elements: usize,
}

impl TensorCapacity {
    pub(super) fn new(plan: &InferencerStatePortPlan, rows: usize) -> ReadResult<Self> {
        let elements = rows.checked_mul(plan.elements).ok_or_else(overflow)?;
        let axes = rows
            .checked_mul(plan.schema.dimensions.len())
            .ok_or_else(overflow)?;
        let input = rows.checked_mul(plan.input.len()).ok_or_else(overflow)?;
        let output = rows.checked_mul(plan.output.len()).ok_or_else(overflow)?;
        Ok(Self {
            rows,
            input,
            output,
            axes,
            elements,
        })
    }

    fn texts(&self) -> ReadResult<u64> {
        let bytes = self.input.checked_add(self.output).ok_or_else(overflow)?;
        slots::<u8>(bytes)
    }

    pub(super) fn body(&self) -> ReadResult<u64> {
        let values = checked_add(slots::<u64>(self.axes)?, slots::<f32>(self.elements)?)?;
        checked_add(checked_add(values, self.texts()?)?, offsets(self.rows, 4)?)
    }

    pub(super) fn buffers(&self) -> ReadResult<u64> {
        let axes = aligned(slots::<u64>(self.axes)?)?;
        let values = aligned(slots::<f32>(self.elements)?)?;
        checked_add(
            checked_add(axes, values)?,
            checked_add(self.texts()?, offsets(self.rows, 4)?)?,
        )
    }

    pub(super) fn objects() -> ReadResult<u64> {
        let columns = checked_add(slots::<StringArray>(2)?, slots::<ListArray>(2)?)?;
        let values = checked_add(slots::<UInt64Array>(1)?, slots::<Float32Array>(1)?)?;
        let fields = checked_add(
            slots::<arrow_schema::Field>(2)?,
            slots::<u8>(2 * "item".len())?,
        )?;
        checked_add(
            checked_add(columns, values)?,
            checked_add(fields, slots::<ArrayRef>(4)?)?,
        )
    }
}

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

impl Headers {
    pub(super) fn new(capacity: HeaderCapacity) -> Self {
        let rows = capacity.rows;
        Self {
            fingerprint: StringBuilder::with_capacity(rows, capacity.fingerprints),
            incarnation: UInt64Builder::with_capacity(rows),
            revision: UInt64Builder::with_capacity(rows),
            persisted: UInt64Builder::with_capacity(rows),
            bytes: UInt64Builder::with_capacity(rows),
            confirmed: UInt64Builder::with_capacity(rows),
            replicas: UInt64Builder::with_capacity(rows),
            stage: StringBuilder::with_capacity(rows, capacity.stages),
        }
    }

    pub(super) fn append(&mut self, retained: &RetainedBranch, bytes: u64) -> ReadResult<()> {
        let branch = &retained.branch;
        match &branch.key {
            Some(key) => self.fingerprint.append_value(key.fingerprint().to_string()),
            None => self.fingerprint.append_null(),
        }
        self.incarnation.append_value(retained.value()?.incarnation);
        self.revision.append_value(branch.revision);
        self.persisted.append_value(branch.persisted);
        self.bytes.append_value(bytes);
        self.confirmed.append_value(branch.confirmed);
        self.replicas.append_value(branch.replicas);
        self.stage.append_value(branch.stage().as_ref());
        Ok(())
    }

    /// The bytes `branch`'s header row adds to the body that carries it: its fingerprint and stage
    /// text with their offsets, and its six counters.
    pub(super) fn row_bytes(branch: &CapturedBranch) -> u64 {
        let fingerprint = match &branch.key {
            Some(key) => key.fingerprint().fingerprint().len() * 2,
            None => 0,
        };
        let texts = fingerprint
            .checked_add(PersistenceStage::ReplicaConfirmed.as_ref().len())
            .assured("a header row's text is tiny");
        let offsets = std::mem::size_of::<i32>()
            .checked_mul(2)
            .assured("two text offsets are tiny");
        let counters = std::mem::size_of::<u64>()
            .checked_mul(6)
            .assured("six counters are tiny");
        let bytes = texts.checked_add(offsets).assured("a header row is tiny");
        let bytes = bytes.checked_add(counters).assured("a header row is tiny");
        u64::try_from(bytes).assured("a header row's size fits u64")
    }

    pub(super) fn finish(
        mut self,
        total: u64,
        capacity: HeaderCapacity,
    ) -> ReadResult<ProcessorStateSectionColumns> {
        let section = ProcessorStateSectionColumns {
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
        };
        assert_buffers(std::slice::from_ref(&section), capacity.buffers()?);
        Ok(section)
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
    pub(super) fn new(index: usize, capacity: &TensorCapacity) -> ReadResult<Self> {
        let rows = capacity.rows;
        Ok(Self {
            index: u32::try_from(index).change_context(ProcessorStatePageError::Columns)?,
            input: StringBuilder::with_capacity(rows, capacity.input),
            output: StringBuilder::with_capacity(rows, capacity.output),
            shape: ListBuilder::with_capacity(UInt64Builder::with_capacity(capacity.axes), rows)
                .with_field(StdArc::new(arrow_schema::Field::new(
                    "item",
                    arrow_schema::DataType::UInt64,
                    false,
                ))),
            values: ListBuilder::with_capacity(
                Float32Builder::with_capacity(capacity.elements),
                rows,
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

    pub(super) fn finish(
        mut self,
        total: u64,
        capacity: &TensorCapacity,
    ) -> ReadResult<ProcessorStateSectionColumns> {
        let section = ProcessorStateSectionColumns {
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
        };
        assert_buffers(std::slice::from_ref(&section), capacity.buffers()?);
        Ok(section)
    }
}
