//! Layer: engines and infrastructure.
//! Owns: the inferencer archive descriptor and ordered little-endian F32 tensor sections.
//! Depends on: vocabulary identities, archive record framing and standard byte I/O.
//! Must not know: native snapshots, ONNX execution, scheduling or runtime placement.

use std::io::{Read, Write};

use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_models::{
    BranchKeyFingerprint, DomainName, InferencerModelFingerprint, ModelName, SchemaFingerprint,
    Unit,
};
use rkyv::{Archive, Deserialize, Serialize};

use crate::{
    ArchiveReadError, ArchiveRecord, ArchiveWriteError, RecordKind, SectionDigest, StateField,
    section::{decode_record, encode_record},
    state::{parse_domain, parse_entity, validate_branch},
};

/// A single branch's published tensors. `state.bin` concatenates tensors in declaration order,
/// in row-major order, preserving every F32 bit, including signed zero and NaN payloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InferencerStateDescriptor {
    pub domain: DomainName,
    pub entity: ModelName,
    pub unit: Unit,
    pub schema: SchemaFingerprint,
    pub model: InferencerModelFingerprint,
    pub branch_fingerprint: Option<BranchKeyFingerprint>,
    pub branch: nervix_models::BranchKeyFields<StateField>,
    pub incarnation: u64,
    pub revision: u64,
    pub shapes: Vec<Vec<u64>>,
    pub byte_length: u64,
    pub digest: SectionDigest,
}

#[derive(Archive, Serialize, Deserialize)]
struct DescriptorWire {
    domain: String,
    entity: String,
    unit: crate::wire::UnitWire,
    schema: [u8; 32],
    model: [u8; 32],
    branch_fingerprint: Option<[u8; 32]>,
    branch: nervix_models::BranchKeyFields<StateField>,
    incarnation: u64,
    revision: u64,
    shapes: Vec<Vec<u64>>,
    byte_length: u64,
    digest: [u8; 32],
}

impl ArchiveRecord for InferencerStateDescriptor {
    const KIND: RecordKind = RecordKind::InferencerStateDescriptor;
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "the header fences the one supported inferencer descriptor shape"
        )
    )]
    const VERSION: u16 = 2;

    fn encode(&self) -> Result<Vec<u8>, Report<ArchiveWriteError>> {
        encode_record(
            Self::KIND,
            Self::VERSION,
            &DescriptorWire {
                domain: self.domain.to_string(),
                entity: self.entity.to_string(),
                unit: self.unit.into(),
                schema: *self.schema.as_digest(),
                model: *self.model.as_digest(),
                branch_fingerprint: self.branch_fingerprint.map(|v| *v.fingerprint()),
                branch: self.branch.clone(),
                incarnation: self.incarnation,
                revision: self.revision,
                shapes: self.shapes.clone(),
                byte_length: self.byte_length,
                digest: *self.digest.as_bytes(),
            },
        )
    }

    fn decode(path: &str, bytes: &[u8]) -> Result<Self, Report<ArchiveReadError>> {
        let wire: DescriptorWire = decode_record(path, Self::KIND, Self::VERSION, bytes)?;
        let descriptor = Self {
            domain: parse_domain(path, &wire.domain)?,
            entity: parse_entity(path, &wire.entity)?,
            unit: wire.unit.into(),
            schema: SchemaFingerprint::from_digest(wire.schema),
            model: InferencerModelFingerprint::from_digest(wire.model),
            branch_fingerprint: wire.branch_fingerprint.map(BranchKeyFingerprint::new),
            branch: wire.branch,
            incarnation: wire.incarnation,
            revision: wire.revision,
            shapes: wire.shapes,
            byte_length: wire.byte_length,
            digest: SectionDigest::from_bytes(wire.digest),
        };
        descriptor.validate(path)?;
        Ok(descriptor)
    }
}

impl InferencerStateDescriptor {
    /// Validates metadata and derives the exact raw length without allocating any tensor values.
    pub fn validate(&self, path: &str) -> Result<(), Report<ArchiveReadError>> {
        validate_branch(path, &self.branch)?;
        if self.branch.is_branched() != self.branch_fingerprint.is_some() {
            return Err(invalid(path, "inferencer branch identity"));
        }
        if self.incarnation == 0 {
            return Err(invalid(path, "inferencer incarnation"));
        }
        if self.shapes.is_empty() {
            return Err(invalid(path, "inferencer tensor shapes"));
        }
        let length = self.shapes.iter().try_fold(0u64, |total, shape| {
            total.checked_add(tensor_elements(shape)?.checked_mul(4)?)
        });
        if length != Some(self.byte_length) {
            return Err(invalid(path, "inferencer tensor byte length"));
        }
        Ok(())
    }
}

fn tensor_elements(shape: &[u64]) -> Option<u64> {
    shape.iter().try_fold(1u64, |count, axis| {
        if *axis == 0 {
            None
        } else {
            count.checked_mul(*axis)
        }
    })
}

/// Writes one tensor's exact bits through fixed-size encoding blocks, without a numeric conversion.
pub fn write_tensor_values(writer: &mut dyn Write, values: &[f32]) -> std::io::Result<()> {
    let mut block = [0u8; TENSOR_VALUE_BLOCK_BYTES];
    for values in values.chunks(TENSOR_VALUE_BLOCK_BYTES / 4) {
        let length = values
            .len()
            .checked_mul(4)
            .verified("a tensor encoding block holds at most 16,384 F32 values");
        let encoded = &mut block[..length];
        for (slot, value) in encoded.as_chunks_mut::<4>().0.iter_mut().zip(values) {
            slot.copy_from_slice(&value.to_bits().to_le_bytes());
        }
        writer.write_all(encoded)?;
    }
    Ok(())
}

#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "a raw tensor encoding block bounds stack memory and the writer's cancellation \
                  unit"
    )
)]
const TENSOR_VALUE_BLOCK_BYTES: usize = 64 * 1024;

/// Reads exactly the descriptor's values. The caller admits `byte_length` before calling; the
/// explicit limit refuses hostile sizes before allocating. The section reader verifies its digest.
pub fn read_tensor_values(
    path: &str,
    reader: &mut dyn Read,
    descriptor: &InferencerStateDescriptor,
    limit: u64,
) -> Result<Vec<Vec<f32>>, Report<ArchiveReadError>> {
    descriptor.validate(path)?;
    if descriptor.byte_length > limit {
        return Err(Report::new(ArchiveReadError::SectionTooLarge {
            path: path.to_string(),
            length: descriptor.byte_length,
            limit,
        }));
    }
    let mut tensors = Vec::with_capacity(descriptor.shapes.len());
    for shape in &descriptor.shapes {
        let count =
            tensor_elements(shape).ok_or_else(|| invalid(path, "inferencer tensor size"))?;
        let Ok(count) = usize::try_from(count) else {
            return Err(invalid(path, "inferencer tensor size"));
        };
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            let mut bytes = [0u8; 4];
            reader
                .read_exact(&mut bytes)
                .change_context(ArchiveReadError::Read)?;
            values.push(f32::from_bits(u32::from_le_bytes(bytes)));
        }
        tensors.push(values);
    }
    let mut extra = [0u8; 1];
    if reader
        .read(&mut extra)
        .change_context(ArchiveReadError::Read)?
        != 0
    {
        return Err(invalid(path, "inferencer tensor trailing bytes"));
    }
    Ok(tensors)
}

fn invalid(path: &str, field: &'static str) -> Report<ArchiveReadError> {
    Report::new(ArchiveReadError::InvalidValue {
        path: path.to_string(),
        field,
    })
}
