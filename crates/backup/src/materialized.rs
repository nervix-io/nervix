//! Public materialized relay descriptors and bounded record-identity sections.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Archive-owned materialized metadata, typed branch keys and watermarks; Arrow
//!   columns remain separate sections described by their archive offsets.
//! - **Depends on.** The archive record envelope and vocabulary identities.
//! - **Must not know.** Native checkpoint encoding, runtime ownership or restore placement.

use error_stack::Report;
use nervix_models::{DomainName, ModelName, RemoteRuntimeRecordMetadata, SchemaFingerprint};
use rkyv::{Archive, Deserialize, Serialize};

use crate::{
    ArchiveReadError, ArchiveRecord, ArchiveWriteError, RecordKind, StateField,
    section::{decode_record, encode_record},
    state::validate_branch,
};

/// Conversion holds one identity section and one Arrow section at a time.
pub const MATERIALIZED_IDENTITIES_BYTES: u64 = 1024 * 1024;
pub const MATERIALIZED_COLUMNS_BYTES: u64 = 8 * 1024 * 1024;

/// One immutable generation of a materialized relay. Each group has an identity record and an
/// Arrow IPC stream using the relay's exact schema, including field sensitivity metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedRelayDescriptor {
    pub domain: DomainName,
    pub entity: ModelName,
    pub schema: SchemaFingerprint,
    pub revision: u64,
    pub fence: u64,
    pub branch_generation: u64,
    pub record_count: u64,
    pub groups: u32,
}

/// Scalar identity beside one Arrow row; no payload is represented as row values here.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
pub struct MaterializedRecordIdentity {
    pub branch: Option<Vec<StateField>>,
    pub watermarks: RemoteRuntimeRecordMetadata,
}

/// One bounded group's identities in Arrow row order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedIdentitiesRecord {
    pub domain: DomainName,
    pub entity: ModelName,
    pub group: u32,
    pub identities: Vec<MaterializedRecordIdentity>,
}

#[derive(Archive, Serialize, Deserialize)]
struct DescriptorWire {
    domain: String,
    entity: String,
    schema: [u8; 32],
    revision: u64,
    fence: u64,
    branch_generation: u64,
    record_count: u64,
    groups: u32,
}

#[derive(Archive, Serialize, Deserialize)]
struct IdentitiesWire {
    domain: String,
    entity: String,
    group: u32,
    identities: Vec<MaterializedRecordIdentity>,
}

impl ArchiveRecord for MaterializedRelayDescriptor {
    const KIND: RecordKind = RecordKind::MaterializedRelayDescriptor;
    const VERSION: u16 = 1;

    fn encode(&self) -> Result<Vec<u8>, Report<ArchiveWriteError>> {
        encode_record(
            Self::KIND,
            Self::VERSION,
            &DescriptorWire {
                domain: self.domain.as_str().to_string(),
                entity: self.entity.as_str().to_string(),
                schema: *self.schema.as_digest(),
                revision: self.revision,
                fence: self.fence,
                branch_generation: self.branch_generation,
                record_count: self.record_count,
                groups: self.groups,
            },
        )
    }

    fn decode(path: &str, bytes: &[u8]) -> Result<Self, Report<ArchiveReadError>> {
        let wire: DescriptorWire = decode_record(path, Self::KIND, Self::VERSION, bytes)?;
        if (wire.groups == 0) != (wire.record_count == 0)
            || u64::from(wire.groups) > wire.record_count
        {
            return Err(invalid(path, "materialized group count"));
        }
        Ok(Self {
            domain: DomainName::parse(&wire.domain).map_err(|_| invalid(path, "domain"))?,
            entity: ModelName::parse(&wire.entity).map_err(|_| invalid(path, "entity"))?,
            schema: SchemaFingerprint::from_digest(wire.schema),
            revision: wire.revision,
            fence: wire.fence,
            branch_generation: wire.branch_generation,
            record_count: wire.record_count,
            groups: wire.groups,
        })
    }
}

impl ArchiveRecord for MaterializedIdentitiesRecord {
    const KIND: RecordKind = RecordKind::MaterializedIdentities;
    const VERSION: u16 = 1;

    fn encode(&self) -> Result<Vec<u8>, Report<ArchiveWriteError>> {
        let bytes = encode_record(
            Self::KIND,
            Self::VERSION,
            &IdentitiesWire {
                domain: self.domain.as_str().to_string(),
                entity: self.entity.as_str().to_string(),
                group: self.group,
                identities: self.identities.clone(),
            },
        )?;
        if !u64::try_from(bytes.len()).is_ok_and(|length| length <= MATERIALIZED_IDENTITIES_BYTES) {
            return Err(Report::new(ArchiveWriteError::RecordTooLarge {
                kind: Self::KIND,
                length: MATERIALIZED_IDENTITIES_BYTES + 1,
                limit: MATERIALIZED_IDENTITIES_BYTES,
            }));
        }
        Ok(bytes)
    }

    fn decode(path: &str, bytes: &[u8]) -> Result<Self, Report<ArchiveReadError>> {
        if !u64::try_from(bytes.len()).is_ok_and(|length| length <= MATERIALIZED_IDENTITIES_BYTES) {
            return Err(invalid(path, "materialized identities size"));
        }
        let wire: IdentitiesWire = decode_record(path, Self::KIND, Self::VERSION, bytes)?;
        if wire.identities.is_empty() {
            return Err(invalid(path, "materialized identities"));
        }
        for identity in &wire.identities {
            validate_branch(path, &identity.branch)?;
            if identity.watermarks.ingested_at_low_watermark
                > identity.watermarks.ingested_at_high_watermark
            {
                return Err(invalid(path, "materialized watermarks"));
            }
        }
        Ok(Self {
            domain: DomainName::parse(&wire.domain).map_err(|_| invalid(path, "domain"))?,
            entity: ModelName::parse(&wire.entity).map_err(|_| invalid(path, "entity"))?,
            group: wire.group,
            identities: wire.identities,
        })
    }
}

fn invalid(path: &str, field: &'static str) -> Report<ArchiveReadError> {
    Report::new(ArchiveReadError::InvalidValue {
        path: path.to_string(),
        field,
    })
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_arbitrary::{Arbitrary, Domain};
    use nervix_models::Timestamp;

    use super::*;

    #[test]
    fn bolero_materialized_descriptors_and_identities_preserve_the_complete_generation() {
        bolero::check!()
            .with_iterations(256)
            .with_max_len(2048)
            .for_each(|bytes| {
                let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
                let descriptor = MaterializedRelayDescriptor {
                    domain: arbitrary.name(),
                    entity: arbitrary.name(),
                    schema: SchemaFingerprint::from_digest(std::array::from_fn(|_| {
                        arbitrary.entropy().byte()
                    })),
                    revision: arbitrary.entropy().any_u64(),
                    fence: arbitrary.entropy().any_u64(),
                    branch_generation: arbitrary.entropy().any_u64(),
                    record_count: 1,
                    groups: 1,
                };
                let low = arbitrary.entropy().any_i64();
                let high = arbitrary.entropy().any_i64();
                let branch = vec![StateField {
                    name: "tenant".into(),
                    value: crate::StateValue::Array(vec![
                        crate::StateValue::String(arbitrary.string()),
                        crate::StateValue::Vec(vec![
                            crate::StateValue::U8(arbitrary.entropy().byte()),
                            crate::StateValue::F64Bits(arbitrary.entropy().any_u64()),
                        ]),
                    ]),
                }];
                let branches = if arbitrary.entropy().byte() % 2 == 0 {
                    vec![None]
                } else {
                    let mut other = branch.clone();
                    other[0].value = crate::StateValue::Array(vec![
                        crate::StateValue::String("another branch".into()),
                        crate::StateValue::Vec(vec![crate::StateValue::U8(1)]),
                    ]);
                    vec![Some(branch), Some(other)]
                };
                let descriptor = MaterializedRelayDescriptor {
                    record_count: u64::try_from(branches.len()).assured("bounded identities fit"),
                    ..descriptor
                };
                let record = MaterializedIdentitiesRecord {
                    domain: descriptor.domain.clone(),
                    entity: descriptor.entity.clone(),
                    group: 0,
                    identities: branches
                        .into_iter()
                        .map(|branch| MaterializedRecordIdentity {
                            branch,
                            watermarks: RemoteRuntimeRecordMetadata {
                                ingested_at_low_watermark: Timestamp::from_unix_nanos(
                                    low.min(high),
                                ),
                                ingested_at_high_watermark: Timestamp::from_unix_nanos(
                                    low.max(high),
                                ),
                            },
                        })
                        .collect(),
                };
                let encoded = descriptor
                    .encode()
                    .assured("the bounded generation encodes");
                assert_eq!(
                    MaterializedRelayDescriptor::decode("descriptor.rkyv", &encoded)
                        .assured("the current descriptor validates"),
                    descriptor
                );
                let encoded = record.encode().assured("the bounded identities encode");
                let decoded = MaterializedIdentitiesRecord::decode("identities.rkyv", &encoded)
                    .assured("the current identities validate");
                assert_eq!(decoded, record);
                assert_eq!(
                    decoded.encode().assured("the decoded identities encode"),
                    encoded
                );
                for identity in decoded.identities {
                    for field in identity.branch.into_iter().flatten() {
                        assert_eq!(StateField::from_remote(field.clone().into_remote()), field);
                    }
                }
            });
    }

    #[test]
    fn materialized_headers_reject_wrong_kinds_and_versions_before_payload_validation() {
        for kind in [
            MaterializedRelayDescriptor::KIND,
            MaterializedIdentitiesRecord::KIND,
        ] {
            for (tag, version) in [(kind.tag(), 2_u16), (u16::MAX, 1)] {
                let mut bytes = crate::section::RECORD_MAGIC.to_vec();
                bytes.extend_from_slice(&tag.to_le_bytes());
                bytes.extend_from_slice(&version.to_le_bytes());
                bytes.push(255);
                let error = if kind == MaterializedRelayDescriptor::KIND {
                    MaterializedRelayDescriptor::decode("descriptor.rkyv", &bytes).err()
                } else {
                    MaterializedIdentitiesRecord::decode("identities.rkyv", &bytes).err()
                };
                assert!(matches!(
                    error.map(|error| error.current_context().clone()),
                    Some(
                        ArchiveReadError::ForeignRecordKind { .. }
                            | ArchiveReadError::UnsupportedRecordVersion { .. }
                    )
                ));
            }
        }
    }

    #[test]
    fn materialized_record_counts_and_branch_keys_are_validated() {
        let descriptor = DescriptorWire {
            domain: "tenant".into(),
            entity: "state".into(),
            schema: [1; 32],
            revision: 5,
            fence: 7,
            branch_generation: 2,
            record_count: 0,
            groups: 1,
        };
        let bytes = encode_record(MaterializedRelayDescriptor::KIND, 1, &descriptor)
            .assured("the current malformed count encodes");
        assert!(MaterializedRelayDescriptor::decode("descriptor.rkyv", &bytes).is_err());
        for branch in [
            Some(Vec::new()),
            Some(vec![StateField {
                name: String::new(),
                value: crate::StateValue::U8(1),
            }]),
        ] {
            let record = IdentitiesWire {
                domain: "tenant".into(),
                entity: "state".into(),
                group: 0,
                identities: vec![MaterializedRecordIdentity {
                    branch,
                    watermarks: RemoteRuntimeRecordMetadata {
                        ingested_at_low_watermark: Timestamp::from_unix_nanos(0),
                        ingested_at_high_watermark: Timestamp::from_unix_nanos(1),
                    },
                }],
            };
            let bytes = encode_record(MaterializedIdentitiesRecord::KIND, 1, &record)
                .assured("the bounded current malformed identity encodes");
            assert!(MaterializedIdentitiesRecord::decode("identities.rkyv", &bytes).is_err());
        }
    }
}
