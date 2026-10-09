//! Archive-owned records for the three runtime state kinds carried by a domain backup.
//!
//! The record header identifies each kind before bytecheck touches its rkyv payload. Names and
//! branch-key fields are validated when the wire shape becomes a public record. A WASM guest save
//! remains a separate raw section named by its descriptor.

use error_stack::Report;
use nervix_models::{
    BranchKeyFingerprint, DomainName, FieldName, ModelKind, ModelName, RemoteRuntimeElementValue,
    RemoteRuntimeField, RemoteRuntimeValue, SchemaFingerprint, Timestamp, WasmStateGeneration,
};

use crate::{
    error::{ArchiveReadError, ArchiveWriteError},
    section::{ArchiveRecord, RecordKind, decode_record, encode_record},
    stream::{StreamedBranchLifecycle, StreamedKafkaOffsets},
    wire::{
        BranchLifecycleWire, KafkaOffsetsWire, StateField, StateValue, WasmStateDescriptorWire,
    },
};

const STATE_RECORD_VERSION: u16 = 1;

/// One branch's committed WASM guest save. The raw blob is a separate section at the descriptor's
/// corresponding guest-blob path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmStateDescriptor {
    pub domain: DomainName,
    pub entity: ModelName,
    pub schema: SchemaFingerprint,
    pub branch_fingerprint: Option<BranchKeyFingerprint>,
    pub branch: Option<Vec<StateField>>,
    pub generation: WasmStateGeneration,
    pub revision: u64,
}

/// The next offset for one Kafka source partition at the cut.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaPartitionOffset {
    pub topic: String,
    pub partition: i32,
    pub next_offset: i64,
}

/// A Kafka domain-offset checkpoint. Offsets are in topic/partition order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaOffsetsRecord {
    pub domain: DomainName,
    pub entity: ModelName,
    pub schema: SchemaFingerprint,
    pub revision: u64,
    pub offsets: Vec<KafkaPartitionOffset>,
}

/// One branch, in the LRU order the owner checkpointed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchLifecycleEntry {
    pub key: Option<Vec<StateField>>,
    pub last_ingestion: Timestamp,
    pub incarnation: u64,
}

/// The lifecycle of one processor's branches at the cut.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchLifecycleRecord {
    pub domain: DomainName,
    pub owner_kind: ModelKind,
    pub entity: ModelName,
    pub schema: SchemaFingerprint,
    pub revision: u64,
    pub branches: Vec<BranchLifecycleEntry>,
}

impl ArchiveRecord for WasmStateDescriptor {
    const KIND: RecordKind = RecordKind::WasmStateDescriptor;
    const VERSION: u16 = STATE_RECORD_VERSION;

    fn encode(&self) -> Result<Vec<u8>, Report<ArchiveWriteError>> {
        encode_record(
            Self::KIND,
            Self::VERSION,
            &WasmStateDescriptorWire {
                domain: self.domain.as_str().to_string(),
                entity: self.entity.as_str().to_string(),
                schema: *self.schema.as_digest(),
                branch_fingerprint: self
                    .branch_fingerprint
                    .as_ref()
                    .map(|key| *key.fingerprint()),
                branch: self.branch.clone(),
                generation: self.generation.into(),
                revision: self.revision,
            },
        )
    }

    fn decode(path: &str, bytes: &[u8]) -> Result<Self, Report<ArchiveReadError>> {
        let wire: WasmStateDescriptorWire = decode_record(path, Self::KIND, Self::VERSION, bytes)?;
        validate_branch(path, &wire.branch)?;
        if wire.branch.is_some() != wire.branch_fingerprint.is_some() {
            return Err(invalid(path, "WASM branch fingerprint"));
        }
        let generation = WasmStateGeneration::try_from(wire.generation)
            .map_err(|_| invalid(path, "WASM state generation"))?;
        Ok(Self {
            domain: parse_domain(path, &wire.domain)?,
            entity: parse_entity(path, &wire.entity)?,
            schema: SchemaFingerprint::from_digest(wire.schema),
            branch_fingerprint: wire.branch_fingerprint.map(BranchKeyFingerprint::new),
            branch: wire.branch,
            generation,
            revision: wire.revision,
        })
    }
}

impl ArchiveRecord for KafkaOffsetsRecord {
    const KIND: RecordKind = RecordKind::KafkaOffsets;
    const VERSION: u16 = STATE_RECORD_VERSION;

    fn encode(&self) -> Result<Vec<u8>, Report<ArchiveWriteError>> {
        StreamedKafkaOffsets {
            domain: self.domain.clone(),
            entity: self.entity.clone(),
            schema: self.schema,
            revision: self.revision,
            offsets: self.offsets.iter().cloned(),
        }
        .encode()
    }

    fn decode(path: &str, bytes: &[u8]) -> Result<Self, Report<ArchiveReadError>> {
        let wire: KafkaOffsetsWire = decode_record(path, Self::KIND, Self::VERSION, bytes)?;
        let mut previous = None;
        for offset in &wire.offsets {
            if offset.topic.is_empty() || offset.partition < 0 || offset.next_offset < 0 {
                return Err(invalid(path, "Kafka partition offset"));
            }
            let current = (&offset.topic, offset.partition);
            if previous.is_some_and(|previous| previous >= current) {
                return Err(invalid(path, "Kafka offset order"));
            }
            previous = Some(current);
        }
        Ok(Self {
            domain: parse_domain(path, &wire.domain)?,
            entity: parse_entity(path, &wire.entity)?,
            schema: SchemaFingerprint::from_digest(wire.schema),
            revision: wire.revision,
            offsets: wire
                .offsets
                .into_iter()
                .map(|offset| KafkaPartitionOffset {
                    topic: offset.topic,
                    partition: offset.partition,
                    next_offset: offset.next_offset,
                })
                .collect(),
        })
    }
}

impl ArchiveRecord for BranchLifecycleRecord {
    const KIND: RecordKind = RecordKind::BranchLifecycle;
    const VERSION: u16 = STATE_RECORD_VERSION;

    fn encode(&self) -> Result<Vec<u8>, Report<ArchiveWriteError>> {
        StreamedBranchLifecycle {
            domain: self.domain.clone(),
            owner_kind: self.owner_kind,
            entity: self.entity.clone(),
            schema: self.schema,
            revision: self.revision,
            branches: self.branches.iter().cloned(),
        }
        .encode()
    }

    fn decode(path: &str, bytes: &[u8]) -> Result<Self, Report<ArchiveReadError>> {
        let wire: BranchLifecycleWire = decode_record(path, Self::KIND, Self::VERSION, bytes)?;
        for branch in &wire.branches {
            validate_branch(path, &branch.key)?;
            if branch.incarnation == 0 {
                return Err(invalid(path, "branch incarnation"));
            }
        }
        Ok(Self {
            domain: parse_domain(path, &wire.domain)?,
            owner_kind: wire
                .owner_kind
                .parse()
                .map_err(|_| invalid(path, "state owner kind"))?,
            entity: parse_entity(path, &wire.entity)?,
            schema: SchemaFingerprint::from_digest(wire.schema),
            revision: wire.revision,
            branches: wire
                .branches
                .into_iter()
                .map(|branch| BranchLifecycleEntry {
                    key: branch.key,
                    last_ingestion: Timestamp::from_unix_nanos(branch.last_ingestion_unix_nanos),
                    incarnation: branch.incarnation,
                })
                .collect(),
        })
    }
}

fn parse_domain(path: &str, raw: &str) -> Result<DomainName, Report<ArchiveReadError>> {
    DomainName::parse(raw).map_err(|_| invalid(path, "domain name"))
}

fn parse_entity(path: &str, raw: &str) -> Result<ModelName, Report<ArchiveReadError>> {
    ModelName::parse(raw).map_err(|_| invalid(path, "state entity name"))
}

pub(crate) fn validate_branch(
    path: &str,
    key: &Option<Vec<StateField>>,
) -> Result<(), Report<ArchiveReadError>> {
    let Some(fields) = key else {
        return Ok(());
    };
    if fields.is_empty() {
        return Err(invalid(path, "branch key"));
    }
    let mut previous = None;
    for field in fields {
        FieldName::parse(&field.name).map_err(|_| invalid(path, "branch field name"))?;
        if previous.is_some_and(|name: &str| name >= field.name.as_str()) {
            return Err(invalid(path, "branch field order"));
        }
        previous = Some(field.name.as_str());
    }
    Ok(())
}

fn invalid(path: &str, field: &'static str) -> Report<ArchiveReadError> {
    Report::new(ArchiveReadError::InvalidValue {
        path: path.to_string(),
        field,
    })
}

impl StateField {
    /// Converts a typed runtime branch field into the archive's independent value shape.
    pub fn from_remote(field: RemoteRuntimeField) -> Self {
        Self {
            name: field.name,
            value: StateValue::from_remote(field.value),
        }
    }

    pub fn into_remote(self) -> RemoteRuntimeField {
        RemoteRuntimeField {
            name: self.name,
            value: self.value.into_remote(),
        }
    }
}

impl StateValue {
    fn from_remote(value: RemoteRuntimeValue) -> Self {
        match value {
            RemoteRuntimeValue::U8(v) => Self::U8(v),
            RemoteRuntimeValue::I8(v) => Self::I8(v),
            RemoteRuntimeValue::U16(v) => Self::U16(v),
            RemoteRuntimeValue::I16(v) => Self::I16(v),
            RemoteRuntimeValue::U32(v) => Self::U32(v),
            RemoteRuntimeValue::I32(v) => Self::I32(v),
            RemoteRuntimeValue::U64(v) => Self::U64(v),
            RemoteRuntimeValue::I64(v) => Self::I64(v),
            RemoteRuntimeValue::Bool(v) => Self::Bool(v),
            RemoteRuntimeValue::String(v) => Self::String(v),
            RemoteRuntimeValue::Datetime(v) => Self::Datetime(v),
            RemoteRuntimeValue::F32(v) => Self::F32Bits(v.to_bits()),
            RemoteRuntimeValue::F64(v) => Self::F64Bits(v.to_bits()),
            RemoteRuntimeValue::Array(v) => {
                Self::Array(v.into_iter().map(Self::from_remote_element).collect())
            }
            RemoteRuntimeValue::Vec(v) => {
                Self::Vec(v.into_iter().map(Self::from_remote_element).collect())
            }
        }
    }

    fn from_remote_element(value: RemoteRuntimeElementValue) -> Self {
        match value {
            RemoteRuntimeElementValue::U8(v) => Self::U8(v),
            RemoteRuntimeElementValue::I8(v) => Self::I8(v),
            RemoteRuntimeElementValue::U16(v) => Self::U16(v),
            RemoteRuntimeElementValue::I16(v) => Self::I16(v),
            RemoteRuntimeElementValue::U32(v) => Self::U32(v),
            RemoteRuntimeElementValue::I32(v) => Self::I32(v),
            RemoteRuntimeElementValue::U64(v) => Self::U64(v),
            RemoteRuntimeElementValue::I64(v) => Self::I64(v),
            RemoteRuntimeElementValue::Bool(v) => Self::Bool(v),
            RemoteRuntimeElementValue::String(v) => Self::String(v),
            RemoteRuntimeElementValue::Datetime(v) => Self::Datetime(v),
            RemoteRuntimeElementValue::F32(v) => Self::F32Bits(v.to_bits()),
            RemoteRuntimeElementValue::F64(v) => Self::F64Bits(v.to_bits()),
            RemoteRuntimeElementValue::Array(v) => {
                Self::Array(v.into_iter().map(Self::from_remote_element).collect())
            }
            RemoteRuntimeElementValue::Vec(v) => {
                Self::Vec(v.into_iter().map(Self::from_remote_element).collect())
            }
        }
    }

    fn into_remote(self) -> RemoteRuntimeValue {
        match self {
            Self::U8(v) => RemoteRuntimeValue::U8(v),
            Self::I8(v) => RemoteRuntimeValue::I8(v),
            Self::U16(v) => RemoteRuntimeValue::U16(v),
            Self::I16(v) => RemoteRuntimeValue::I16(v),
            Self::U32(v) => RemoteRuntimeValue::U32(v),
            Self::I32(v) => RemoteRuntimeValue::I32(v),
            Self::U64(v) => RemoteRuntimeValue::U64(v),
            Self::I64(v) => RemoteRuntimeValue::I64(v),
            Self::Bool(v) => RemoteRuntimeValue::Bool(v),
            Self::String(v) => RemoteRuntimeValue::String(v),
            Self::Datetime(v) => RemoteRuntimeValue::Datetime(v),
            Self::F32Bits(v) => RemoteRuntimeValue::F32(f32::from_bits(v)),
            Self::F64Bits(v) => RemoteRuntimeValue::F64(f64::from_bits(v)),
            Self::Array(v) => {
                RemoteRuntimeValue::Array(v.into_iter().map(Self::into_remote_element).collect())
            }
            Self::Vec(v) => {
                RemoteRuntimeValue::Vec(v.into_iter().map(Self::into_remote_element).collect())
            }
        }
    }

    fn into_remote_element(self) -> RemoteRuntimeElementValue {
        match self {
            Self::U8(v) => RemoteRuntimeElementValue::U8(v),
            Self::I8(v) => RemoteRuntimeElementValue::I8(v),
            Self::U16(v) => RemoteRuntimeElementValue::U16(v),
            Self::I16(v) => RemoteRuntimeElementValue::I16(v),
            Self::U32(v) => RemoteRuntimeElementValue::U32(v),
            Self::I32(v) => RemoteRuntimeElementValue::I32(v),
            Self::U64(v) => RemoteRuntimeElementValue::U64(v),
            Self::I64(v) => RemoteRuntimeElementValue::I64(v),
            Self::Bool(v) => RemoteRuntimeElementValue::Bool(v),
            Self::String(v) => RemoteRuntimeElementValue::String(v),
            Self::Datetime(v) => RemoteRuntimeElementValue::Datetime(v),
            Self::F32Bits(v) => RemoteRuntimeElementValue::F32(f32::from_bits(v)),
            Self::F64Bits(v) => RemoteRuntimeElementValue::F64(f64::from_bits(v)),
            Self::Array(v) => RemoteRuntimeElementValue::Array(
                v.into_iter().map(Self::into_remote_element).collect(),
            ),
            Self::Vec(v) => RemoteRuntimeElementValue::Vec(
                v.into_iter().map(Self::into_remote_element).collect(),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;

    use super::*;

    fn domain() -> DomainName {
        DomainName::parse("payments").assured("the domain is an accepted literal")
    }

    fn entity() -> ModelName {
        ModelName::parse("source").assured("the entity is an accepted literal")
    }

    #[test]
    fn runtime_state_records_round_trip_with_typed_branch_values() {
        let branch = vec![StateField {
            name: "tenant".to_string(),
            value: StateValue::String("north".to_string()),
        }];
        let descriptor = WasmStateDescriptor {
            domain: domain(),
            entity: entity(),
            schema: SchemaFingerprint::from_digest([3; 32]),
            branch_fingerprint: Some(BranchKeyFingerprint::of_canonical_text(
                "{\"tenant\":\"north\"}",
            )),
            branch: Some(branch.clone()),
            generation: WasmStateGeneration::FIRST,
            revision: 19,
        };
        let bytes = descriptor.encode().assured("a state descriptor encodes");
        assert_eq!(
            WasmStateDescriptor::decode("descriptor.rkyv", &bytes)
                .assured("the descriptor verifies"),
            descriptor
        );

        let offsets = KafkaOffsetsRecord {
            domain: domain(),
            entity: entity(),
            schema: SchemaFingerprint::from_digest([3; 32]),
            revision: 19,
            offsets: vec![
                KafkaPartitionOffset {
                    topic: "orders".to_string(),
                    partition: 0,
                    next_offset: 7,
                },
                KafkaPartitionOffset {
                    topic: "orders".to_string(),
                    partition: 1,
                    next_offset: 11,
                },
            ],
        };
        let bytes = offsets.encode().assured("an offset record encodes");
        assert_eq!(
            KafkaOffsetsRecord::decode("offsets.rkyv", &bytes).assured("the offsets verify"),
            offsets
        );

        let lifecycle = BranchLifecycleRecord {
            domain: domain(),
            owner_kind: ModelKind::WasmProcessor,
            entity: entity(),
            schema: SchemaFingerprint::from_digest([3; 32]),
            revision: 19,
            branches: vec![BranchLifecycleEntry {
                key: Some(branch),
                last_ingestion: Timestamp::from_unix_nanos(123),
                incarnation: 2,
            }],
        };
        let bytes = lifecycle.encode().assured("a branch lifecycle encodes");
        assert_eq!(
            BranchLifecycleRecord::decode("branches.rkyv", &bytes)
                .assured("the lifecycle verifies"),
            lifecycle
        );
    }

    #[test]
    fn runtime_state_records_validate_order_and_incarnation() {
        let offsets = KafkaOffsetsRecord {
            domain: domain(),
            entity: entity(),
            schema: SchemaFingerprint::from_digest([3; 32]),
            revision: 1,
            offsets: vec![
                KafkaPartitionOffset {
                    topic: "orders".to_string(),
                    partition: 2,
                    next_offset: 1,
                },
                KafkaPartitionOffset {
                    topic: "orders".to_string(),
                    partition: 1,
                    next_offset: 1,
                },
            ],
        };
        let bytes = offsets.encode().assured("the record wire encodes");
        assert!(KafkaOffsetsRecord::decode("offsets.rkyv", &bytes).is_err());

        let lifecycle = BranchLifecycleRecord {
            domain: domain(),
            owner_kind: ModelKind::WasmProcessor,
            entity: entity(),
            schema: SchemaFingerprint::from_digest([3; 32]),
            revision: 1,
            branches: vec![BranchLifecycleEntry {
                key: None,
                last_ingestion: Timestamp::from_unix_nanos(0),
                incarnation: 0,
            }],
        };
        let bytes = lifecycle.encode().assured("the record wire encodes");
        assert!(BranchLifecycleRecord::decode("branches.rkyv", &bytes).is_err());
    }
}
